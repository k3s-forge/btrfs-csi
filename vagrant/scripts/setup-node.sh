#!/bin/bash
set -e

ROLE=$1
IP=$2
NODE_NAME=$(hostname)

echo "=== Setting up $NODE_NAME (role: $ROLE, ip: $IP) ==="

# Update system
apt-get update
apt-get install -y \
    btrfs-progs \
    curl \
    wget \
    jq \
    gnupg \
    lsb-release \
    apt-transport-https \
    ca-certificates \
    software-properties-common

# Install Docker
curl -fsSL https://get.docker.com | sh
usermod -aG docker vagrant

# Install Rust
sudo -u vagrant bash -c 'curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y'
echo 'source $HOME/.cargo/env' >> /home/vagrant/.bashrc

# Install Nomad
wget -O- https://releases.hashicorp.com/nomad/1.7.5/nomad_1.7.5_linux_amd64.zip | gunzip > /usr/local/bin/nomad
chmod +x /usr/local/bin/nomad

# Install Consul (required for Nomad)
wget -O- https://releases.hashicorp.com/consul/1.17.1/consul_1.17.1_linux_amd64.zip | gunzip > /usr/local/bin/consul
chmod +x /usr/local/bin/consul

# Create btrfs data partition
truncate -s 20G /mnt/btrfs-data.img
mkfs.btrfs -f /mnt/btrfs-data.img
mkdir -p /mnt/data /mnt/snapshots
mount -o loop /mnt/btrfs-data.img /mnt/data
mkdir -p /mnt/data/volumes /mnt/data/snapshots
mount -o loop /mnt/btrfs-data.img /mnt/snapshots

# Add to fstab for persistence
echo "/mnt/btrfs-data.img /mnt/data btrfs loop 0 0" >> /etc/fstab

# Create directories for Nomad
mkdir -p /etc/nomad.d /opt/nomad/data

# Copy CSI binary (will be built later)
cp /home/vagrant/btrfs-csi/target/release/btrfs-csi /usr/local/bin/ 2>/dev/null || true

# Setup systemd service for btrfs-csi
cat > /etc/systemd/system/btrfs-csi.service <<EOF
[Unit]
Description=Btrfs CSI Driver
After=network.target

[Service]
Type=simple
ExecStart=/usr/local/bin/btrfs-csi --config /etc/btrfs-csi/config.toml --endpoint 0.0.0.0:9201
Restart=always
RestartSec=5
User=vagrant
Group=vagrant

[Install]
WantedBy=multi-user.target
EOF

# Create CSI config
mkdir -p /etc/btrfs-csi
cat > /etc/btrfs-csi/config.toml <<EOF
[node]
node_id = "$NODE_NAME"
listen_addr = "0.0.0.0"
listen_port = 9200
zone = "dc1"
auth_key = "$(openssl rand -hex 32)"
seed_nodes = ["192.168.56.11:9200"]

[replication]
default_replica_count = 2
default_interval = 10
data_dir = "/mnt/data"
snapshot_dir = "/mnt/snapshots"
enable_incremental = true

[maintenance]
enabled = true
balance_threshold = 0.7
EOF

# Configure Nomad
cat > /etc/nomad.d/nomad.hcl <<EOF
datacenter = "dc1"
data_dir = "/opt/nomad/data"

bind_addr = "$IP"

server {
  enabled = true
  bootstrap_expect = 3
}

client {
  enabled = true
  network_interface = "eth1"
  
  host_volume "btrfs-data" {
    path = "/mnt/data"
    read_only = false
  }
  
  host_volume "btrfs-snapshots" {
    path = "/mnt/snapshots"
    read_only = false
  }
}

plugin "docker" {
  config {
    allow_privileged = true
  }
}

consul {
  address = "127.0.0.1:8500"
}
EOF

# Start Consul
consul agent -dev -bind=$IP -client=0.0.0.0 &

# Wait for Consul
sleep 5

# Start Nomad
nomad agent -config=/etc/nomad.d/ &

echo "=== Setup complete for $NODE_NAME ==="
echo "Nomad UI: http://$IP:4646"
echo "CSI endpoint: $IP:9201"
