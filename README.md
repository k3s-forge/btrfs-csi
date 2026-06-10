# Btrfs CSI Driver

[![Build](https://github.com/k3s-forge/btrfs-csi/actions/workflows/build.yml/badge.svg)](https://github.com/k3s-forge/btrfs-csi/actions/workflows/build.yml)

CSI driver for Btrfs with async replication, gossip discovery, and encrypted transport. Written in Rust (~3MB resident). Zero external dependencies — no Consul, no SSH, no state store.

## Features

- **CSI 24/24 RPCs** — full Identity, Controller, Node spec coverage
- **Async replication** — gossip-based peer discovery, btrfs send/receive, incremental snapshots
- **Stateless architecture** — volume metadata stored as xattr on subvolumes, no JSON files or external DB
- **Encrypted transport** — XChaCha20-Poly1305 (24-byte random nonce, pure ChaCha20, no AES-NI)
- **HMAC-SHA256 authentication** — timestamped tokens, connection-level auth
- **Volume profiles** — `default`, `database` (WAL + periodic checkpoint), `log` (NOCOW + compression)
- **Automated maintenance** — progressive balance throttling, scrub scheduling, snapshot cleanup with retention
- **CSI Topology** — zone-based replica placement, `topology.gocsi.io/zone`
- **Graceful shutdown** — SIGTERM sends NodeLeave to all peers
- **Crash recovery** — stale mount cleanup on startup
- **Multi-platform** — linux/amd64 + linux/arm64 binaries

## Install

```bash
curl -LO https://github.com/k3s-forge/btrfs-csi/releases/download/v0.2.0/btrfs-csi-linux-amd64
chmod +x btrfs-csi-linux-amd64
sudo mv btrfs-csi-linux-amd64 /usr/local/bin/btrfs-csi
```

Or build from source:

```bash
cargo build --release
```

## Quick Start

Generate a 64-char hex auth key:

```bash
openssl rand -hex 32
```

```toml
# /etc/btrfs-csi/config.toml
node_id = "node-a"
listen_addr = "0.0.0.0"
listen_port = 9200
replication_port = 9300
zone = "dc1"
auth_key = "your-64-char-hex-from-openssl"
seed_nodes = ["node-b:9200", "node-c:9200"]
gossip_interval = 10
heartbeat_interval = 30
node_timeout = 90

[replication]
default_replica_count = 2
default_interval = 300
max_concurrent = 4
data_dir = "/mnt/btrfs/data"
snapshot_dir = "/mnt/btrfs/snapshots"
enable_incremental = true

[replication.database]
enabled = true
sqlite_wal_mode = true
checkpoint_interval = 30
enable_nocow = false

[maintenance]
enabled = true
balance_schedule = "0 2 * * *"
balance_threshold = 0.7
scrub_schedule = "0 3 * * 0"
snapshot_cleanup_schedule = "0 4 * * *"

[maintenance.snapshot_retention]
daily = 7
weekly = 4
monthly = 3
```

```bash
btrfs-csi \
  --config /etc/btrfs-csi/config.toml \
  --endpoint unix:///var/run/csi/btrfs-csi.sock \
  --log-level info
```

## CSI Spec — 24/24 RPCs

**Identity:** GetPluginInfo, GetPluginCapabilities, Probe

**Controller:** CreateVolume, DeleteVolume, ControllerPublishVolume, ControllerUnpublishVolume, ValidateVolumeCapabilities, ListVolumes, GetCapacity, ControllerGetCapabilities, CreateSnapshot, DeleteSnapshot, ListSnapshots, ControllerExpandVolume, ControllerGetVolume

**Node:** NodeStageVolume, NodeUnstageVolume, NodePublishVolume, NodeUnpublishVolume, NodeGetVolumeStats, NodeGetCapabilities, NodeGetInfo, NodeExpandVolume

## Volume Profiles

Set via `parameters.profile` in CreateVolumeRequest:

| Profile | NOCOW | Compression | Sync Writes | Use Case |
|---------|-------|-------------|-------------|----------|
| `default` | off | zstd:3 | off | General purpose |
| `database` | off | zstd:3 | on (O_SYNC) | SQLite, PostgreSQL |
| `log` | on | none | off | Logs, caches |

## Security

- **HMAC-SHA256** — connection-level authentication with timestamp replay protection (30s window)
- **XChaCha20-Poly1305** — payload encryption with 24-byte random nonce per message; pure software, no AES-NI
- **Empty auth_key** — driver refuses to start
- **Volume name validation** — rejects path traversal, null bytes, non-alphanumeric characters

## Nomad

```hcl
# Register plugin (system job on all nodes)
job "btrfs-csi" {
  type = "system"
  group "csi" {
    task "plugin" {
      driver = "exec"
      csi_plugin {
        id        = "btrfs-csi"
        type      = "monolith"
        mount_dir = "/var/run/csi"
      }
      config {
        command = "/usr/local/bin/btrfs-csi"
        args = [
          "--config", "/etc/btrfs-csi/config.toml",
          "--endpoint", "unix:///var/run/csi/csi.sock",
        ]
      }
    }
  }
}

# Create replicated volume
volume "my-data" {
  type            = "csi"
  plugin_id       = "btrfs-csi"
  capacity_min    = "10GiB"
  capability {
    access_mode     = "single-node-writer"
    attachment_mode = "file-system"
  }
  parameters {
    replica_count = "3"
    profile       = "database"
  }
}
```

## License

AGPL-3.0
