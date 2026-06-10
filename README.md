# Btrfs CSI Driver

[![Build](https://github.com/k3s-forge/btrfs-csi/actions/workflows/build.yml/badge.svg)](https://github.com/k3s-forge/btrfs-csi/actions/workflows/build.yml)
[![Release](https://img.shields.io/github/v/release/k3s-forge/btrfs-csi?label=v0.2.0)](https://github.com/k3s-forge/btrfs-csi/releases/tag/v0.2.0)

CSI driver for Btrfs with async replication, gossip discovery, quorum-based write safety, adaptive crypto, load-aware maintenance, and multi-tenant protection. Written in Rust (~3MB resident). Zero external dependencies — no Consul, no SSH, no state store.

## Features

- **CSI 24/24 RPCs** — full Identity, Controller, Node spec coverage
- **Async replication** — gossip-based peer discovery, btrfs send/receive, incremental snapshots
- **Quorum + Epoch/Vector Clock** — majority vote required for write access, minority partition auto-readonly, conflict detection via vector clock divergence
- **Adaptive Crypto Engine** — runtime AES-NI detection via raw-cpuid; AES-256-GCM on x86_64 with AES-NI, XChaCha20-Poly1305 fallback on ARM/low-end
- **Load-Aware Maintenance** — I/O priority IDLE (ioprio_set), dynamic window checks (/proc/loadavg + /sys/block/*/stat), targeted balance by fragmentation analysis
- **Multi-tenant Protection** — btrfs quota `--simple` (squota) + qgroup limits, idmapped mounts via mount_setattr (Linux 5.12+)
- **Stateless architecture** — volume metadata stored as xattr on subvolumes, no JSON files or external DB
- **Encrypted transport** — XChaCha20-Poly1305 (24-byte random nonce) / AES-256-GCM (12-byte nonce) adaptive cipher
- **HMAC-SHA256 authentication** — timestamped tokens, connection-level auth
- **Volume profiles** — `default`, `database` (WAL + periodic checkpoint), `log` (NOCOW + compression)
- **Automated maintenance** — progressive balance throttling, scrub scheduling, snapshot cleanup with retention
- **CSI Topology** — zone-based replica placement, `topology.gocsi.io/zone`
- **Graceful shutdown** — SIGTERM sends NodeLeave to all peers
- **Crash recovery** — stale mount cleanup on startup
- **Multi-platform** — linux/amd64 + linux/arm64 binaries

## Install

```bash
# linux/amd64
curl -LO https://github.com/k3s-forge/btrfs-csi/releases/download/v0.2.0/btrfs-csi-linux-amd64
# linux/arm64
# curl -LO https://github.com/k3s-forge/btrfs-csi/releases/download/v0.2.0/btrfs-csi-linux-arm64
chmod +x btrfs-csi-linux-amd64
sudo mv btrfs-csi-linux-amd64 /usr/local/bin/btrfs-csi
```

Verify checksums (published with release):
```bash
sha256sum -c btrfs-csi-linux-amd64.sha256
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
- **Adaptive cipher** — AES-256-GCM (12-byte nonce, AES-NI on x86_64) / XChaCha20-Poly1305 (24-byte nonce, software) — runtime CPUID detection
- **Empty auth_key** — driver refuses to start; non-32-byte keys normalized via SHA-256
- **Volume name validation** — rejects path traversal, null bytes, non-alphanumeric characters
- **Quorum lease** — majority vote required for write; minority partition forced read-only
- **Conflict detection** — vector clock divergence triggers ConflictDetected alerts

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

# Create replicated volume with v0.2.0 features
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
    # v0.2.0: custom replica location & cycle
    replica_zone  = "dc1,dc2"
    sync_interval = "300"
    # v0.2.0: quorum & conflict settings
    require_quorum = "true"
    readonly_on_minority = "true"
  }
}
```

## License

AGPL-3.0
