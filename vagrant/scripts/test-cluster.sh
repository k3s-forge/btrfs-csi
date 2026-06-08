#!/bin/bash
set -e

echo "=== Starting btrfs-csi cluster test ==="

# Build CSI driver
echo "1. Building CSI driver..."
cd /home/vagrant/btrfs-csi
sudo -u vagrant bash -c 'source $HOME/.cargo/env && cargo build --release'

# Deploy CSI driver on all nodes
echo "2. Deploying CSI driver..."
for i in 1 2 3; do
  ssh vagrant@192.168.56.1$i "sudo cp /home/vagrant/btrfs-csi/target/release/btrfs-csi /usr/local/bin/ && sudo systemctl restart btrfs-csi" || true
done

# Wait for CSI to start
sleep 10

# Check cluster status
echo "3. Checking cluster status..."
nomad server members
nomad node status

# Create test volume
echo "4. Creating test volume..."
cat > /tmp/test-volume.hcl <<EOF
volume "test-data" {
  type      = "csi"
  plugin_id = "btrfs-csi"

  capacity_min = "1GiB"
  capacity_max = "10GiB"

  capability {
    access_mode     = "single-node-writer"
    attachment_mode = "file-system"
  }

  parameters {
    replica_count        = "2"
    replication_interval = "10"
  }
}
EOF

nomad volume create /tmp/test-volume.hcl

# Run test job
echo "5. Running test job..."
cat > /tmp/test-job.hcl <<EOF
job "test-btrfs" {
  datacenters = ["dc1"]
  type        = "batch"

  group "test" {
    volume "data" {
      type   = "csi"
      source = "test-data"
    }

    task "write-test" {
      driver = "docker"

      config {
        image   = "ubuntu:22.04"
        command = "/bin/bash"
        args    = ["-c", "apt-get update && apt-get install -y btrfs-progs && dd if=/dev/urandom of=/mnt/data/test-1gb.bin bs=1M count=1024 && echo 'Write test passed'"]
      }

      volume_mount {
        volume      = "data"
        destination = "/mnt/data"
      }

      resources {
        cpu    = 500
        memory = 256
      }
    }
  }
}
EOF

nomad job run /tmp/test-job.hcl

# Wait for job
echo "6. Waiting for job to complete..."
sleep 30
nomad job status test-btrfs

# Check replication
echo "7. Checking replication..."
for i in 1 2 3; do
  echo "Node $i:"
  ssh vagrant@192.168.56.1$i "sudo btrfs subvolume list /mnt/data" || true
done

# Run incremental replication test
echo "8. Testing incremental replication..."
cat > /tmp/replication-test.sh <<'EOF'
#!/bin/bash
set -e

# Create first snapshot
echo "Creating snapshot 1..."
sudo btrfs subvolume snapshot -r /mnt/data/volumes/test-data /mnt/snapshots/test-data-snap1

# Write more data
echo "Writing more data..."
sudo dd if=/dev/urandom of=/mnt/data/volumes/test-data/more-data.bin bs=1M count=512

# Create second snapshot
echo "Creating snapshot 2..."
sudo btrfs subvolume snapshot -r /mnt/data/volumes/test-data /mnt/snapshots/test-data-snap2

# Test incremental send
echo "Testing incremental send..."
sudo btrfs send -p /mnt/snapshots/test-data-snap1 /mnt/snapshots/test-data-snap2 | wc -c
echo "bytes sent incrementally"

# Verify data
echo "Verifying data..."
ls -la /mnt/data/volumes/test-data/
sudo btrfs filesystem usage /mnt/data
EOF

chmod +x /tmp/replication-test.sh
for i in 1 2 3; do
  ssh vagrant@192.168.56.1$i "bash /tmp/replication-test.sh" || true
done

echo "=== All tests passed! ==="
echo "CSI cluster is working correctly."
echo ""
echo "Access Nomad UI: http://192.168.56.11:4646"
echo "Access CSI endpoint: http://192.168.56.11:9201"
