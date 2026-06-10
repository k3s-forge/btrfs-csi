# Btrfs CSI Driver

[![Build](https://github.com/k3s-forge/btrfs-csi/actions/workflows/build.yml/badge.svg)](https://github.com/k3s-forge/btrfs-csi/actions/workflows/build.yml)
[![Test](https://github.com/k3s-forge/btrfs-csi/actions/workflows/test.yml/badge.svg)](https://github.com/k3s-forge/btrfs-csi/actions/workflows/test.yml)
[![Release](https://github.com/k3s-forge/btrfs-csi/actions/workflows/release.yml/badge.svg)](https://github.com/k3s-forge/btrfs-csi/actions/workflows/release.yml)

A Container Storage Interface (CSI) driver for Btrfs with built-in async replication, gossip-based discovery, and HMAC-authenticated TCP transport. Designed for Nomad clusters with no external dependencies.

## Features

- **Full CSI Spec**: All 24 RPCs implemented (Identity: 3, Controller: 13, Node: 8)
- **Btrfs-native Storage**: Leverages subvolumes, snapshots, and `btrfs send/receive`
- **Async Replication**: Configurable intervals, incremental sends with blake3 checksums
- **Gossip Discovery**: Zero external dependencies — no Consul, no external state store
- **HMAC Authentication**: SHA-256 HMAC with replay protection on all inter-node communication
- **Topology-aware**: CSI topology zones for cross-datacenter replication
- **Database Profiles**: SQLite WAL-mode + periodic checkpoint, nodatacow for DB volumes
- **Automated Maintenance**: Scheduled balance (progressive throttling), scrub, snapshot cleanup with retention
- **Graceful Shutdown**: NodeLeave broadcast on SIGTERM/Ctrl+C
- **Crash Recovery**: Cleans stale mounts on startup
- **Disk Persistence**: Idempotent across restarts (`volumes.json`/`snapshots.json`)
- **Concurrent Replication**: Bounded by `max_concurrent` semaphore

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                 Nomad Cluster                       │
│  ┌───────────────────────────────────────────────┐  │
│  │  CSI Driver (System Job, runs on every node)  │  │
│  │                                                │  │
│  │  ┌─────────────┐  ┌───────────────────┐       │  │
│  │  │ Identity     │  │ Controller        │       │  │
│  │  │ (3 RPCs)     │  │ (13 RPCs)        │       │  │
│  │  └─────────────┘  └───────────────────┘       │  │
│  │                                                │  │
│  │  ┌─────────────┐  ┌───────────────────┐       │  │
│  │  │ Node        │  │ Gossip +          │       │  │
│  │  │ (8 RPCs)    │  │ Replication Engine │       │  │
│  │  └─────────────┘  └───────────────────┘       │  │
│  │                                                │  │
│  │  ┌───────────────────┐  ┌───────────────────┐  │  │
│  │  │ Btrfs Operations  │  │ Maintenance       │  │  │
│  │  │ (subvol/snap)     │  │ (balance/scrub)   │  │  │
│  │  └───────────────────┘  └───────────────────┘  │  │
│  └───────────────────────────────────────────────┘  │
│                                                     │
│  Node A ◄─── HMAC/TCP ───► Node B                    │
│         ◄─── HMAC/TCP ───► Node C                    │
└─────────────────────────────────────────────────────┘
```

## Quick Start

### Pre-built Binaries

Download from [GitHub Releases](https://github.com/k3s-forge/btrfs-csi/releases):

```bash
# amd64
curl -LO https://github.com/k3s-forge/btrfs-csi/releases/download/v0.1.1/btrfs-csi-linux-amd64
chmod +x btrfs-csi-linux-amd64

# arm64
curl -LO https://github.com/k3s-forge/btrfs-csi/releases/download/v0.1.1/btrfs-csi-linux-arm64
chmod +x btrfs-csi-linux-arm64
```

### Build from Source

```bash
git clone https://github.com/k3s-forge/btrfs-csi.git
cd btrfs-csi
cargo build --release
# binary at: target/release/btrfs-csi
```

### Configuration

```toml
[node]
node_id = "node-a"
listen_addr = "0.0.0.0"
listen_port = 9200
replication_port = 9300
zone = "dc1"
auth_key = "CHANGE-ME-REQUIRED-32-BYTES-HEX"     # Required: shared secret across cluster
seed_nodes = ["node-b:9200", "node-c:9200"]

[replication]
default_replica_count = 2
default_interval = 30
data_dir = "/mnt/btrfs/data"
snapshot_dir = "/mnt/btrfs/snapshots"
enable_incremental = true
max_concurrent = 4

[replication.database]
sqlite_wal_mode = true
checkpoint_interval = 30

[maintenance]
enabled = true
balance_schedule = "0 2 * * *"
balance_threshold = 0.7
scrub_schedule = "0 3 * * 0"

[maintenance.snapshot_retention]
daily = 7
weekly = 4
monthly = 3

[volume_profiles.default]
compression = "zstd"
snapshot_retention = 7

[volume_profiles.database]
nodatacow = true
snapshot_retention = 14

[volume_profiles.log]
nodatacow = true
sync_writes = true
snapshot_retention = 3
```

### Deploy to Nomad

```hcl
# deploy/nomad/btrfs-csi.hcl
job "btrfs-csi" {
  type = "system"
  group "csi" {
    task "plugin" {
      driver = "exec"
      config {
        command = "local/btrfs-csi"
        args = [
          "--config", "local/config.toml",
          "--node-id", "${attr.unique.hostname}",
          "--endpoint", "unix:///var/run/csi/btrfs-csi.sock",
          "--zone", "dc1",
        ]
      }
      csi_plugin {
        id        = "btrfs-csi"
        type      = "controller"
        mount_dir = "/var/run/csi"
      }
      resources {
        cpu    = 100
        memory = 64
      }
    }
  }
}
```

## CSI Specification Coverage

| Service       | RPC                          | Status |
|---------------|------------------------------|--------|
| Identity      | GetPluginInfo                | ✅     |
| Identity      | GetPluginCapabilities        | ✅     |
| Identity      | Probe                        | ✅     |
| Controller    | CreateVolume                 | ✅     |
| Controller    | DeleteVolume                 | ✅     |
| Controller    | ControllerPublishVolume      | ✅     |
| Controller    | ControllerUnpublishVolume    | ✅     |
| Controller    | ValidateVolumeCapabilities   | ✅     |
| Controller    | ListVolumes                  | ✅     |
| Controller    | GetCapacity                  | ✅     |
| Controller    | ControllerGetCapabilities    | ✅     |
| Controller    | CreateSnapshot               | ✅     |
| Controller    | DeleteSnapshot               | ✅     |
| Controller    | ListSnapshots                | ✅     |
| Controller    | ControllerExpandVolume       | ✅     |
| Controller    | ControllerGetVolume          | ✅     |
| Node          | NodeStageVolume              | ✅     |
| Node          | NodeUnstageVolume            | ✅     |
| Node          | NodePublishVolume            | ✅     |
| Node          | NodeUnpublishVolume          | ✅     |
| Node          | NodeGetVolumeStats           | ✅     |
| Node          | NodeGetCapabilities          | ✅     |
| Node          | NodeGetInfo                  | ✅     |
| Node          | NodeExpandVolume             | ✅     |

## Nomad Volume Usage

```hcl
# Create a volume
volume "my-data" {
  type = "csi"
  plugin_id = "btrfs-csi"
  capacity_min = "10GiB"
  capacity_max = "100GiB"
  capability {
    access_mode = "single-node-writer"
    attachment_mode = "file-system"
  }
  parameters {
    replica_count = "3"
    replica_zones = "dc1,dc2,dc3"
    replication_interval = "30"
    profile = "database"
  }
}

# Use in a job
job "app" {
  group "web" {
    volume "data" {
      type = "csi"
      source = "my-data"
    }
    task "app" {
      driver = "docker"
      volume_mount {
        volume = "data"
        destination = "/data"
      }
    }
  }
}
```

## Governance

### No External Dependencies

- **No Consul**: Node discovery uses gossip protocol over TCP
- **No SSH**: Replication uses direct TCP connections with HMAC auth
- **No State Store**: All state is derived from the Btrfs filesystem; in-memory state is persisted to `volumes.json`/`snapshots.json` for idempotency
- **Minimal Footprint**: Written in Rust (~3MB memory per node vs ~15MB for Go equivalents)

### Security

- HMAC-SHA256 authentication on all inter-node connections
- Timestamp-based replay protection (15s window)
- Volume name validation (alphanumeric + `._-`, max 255 chars, no path traversal)
- Empty `auth_key` causes startup failure — no insecure mode
- `--auth-key` CLI argument for production deployment (avoids secrets in config files)

## Development

### Project Structure

```
btrfs-csi/
├── crates/
│   ├── btrfs-csi/         # CSI gRPC server binary (tonic)
│   ├── btrfs-exchange/    # Gossip, replication, scheduler, volume manager
│   ├── btrfs-ops/         # Btrfs command wrappers (subvol, snapshot, usage)
│   └── btrfs-protocol/    # TCP transport + HMAC auth + message types
├── config/config.toml     # Example configuration
├── deploy/nomad/          # Nomad job definitions
├── proto/csi.proto        # CSI specification (v1.9)
└── vagrant/               # Test environment
```

### Commands

```bash
cargo build --release      # Build production binary
cargo test --workspace     # Run all tests
cargo clippy               # Lint
cargo fmt                  # Format
```

### Test Workflows

CI automatically runs: unit tests, btrfs integration tests, cluster read/write tests, replication end-to-end tests, and Nomad CSI deployment tests.

## License

AGPL-3.0 — see [LICENSE](LICENSE) for full text. Copyright btrfs-csi contributors.