# Btrfs CSI Driver

[![Build](https://github.com/k3s-forge/btrfs-csi/actions/workflows/build.yml/badge.svg)](https://github.com/k3s-forge/btrfs-csi/actions/workflows/build.yml)

CSI driver for Btrfs with async replication, gossip discovery, HMAC auth. Zero external deps — no Consul, no SSH, no state store. Rust, ~3MB/node.

## Install

```bash
# Download from GitHub Releases
curl -LO https://github.com/k3s-forge/btrfs-csi/releases/download/v0.1.1/btrfs-csi-linux-amd64
chmod +x btrfs-csi-linux-amd64

# Or build from source
cargo build --release
```

## Quick Start

```toml
# config.toml
[node]
auth_key = "CHANGE-ME-32-BYTES-HEX"          # required
seed_nodes = ["node-b:9200", "node-c:9200"]
zone = "dc1"

[replication]
default_replica_count = 2
data_dir = "/mnt/btrfs/data"
snapshot_dir = "/mnt/btrfs/snapshots"
```

```bash
btrfs-csi \
  --config config.toml \
  --node-id $(hostname) \
  --endpoint unix:///var/run/csi/btrfs-csi.sock
```

## CSI Spec — 24/24 RPCs

Identity: GetPluginInfo, GetPluginCapabilities, Probe
Controller: CreateVolume, DeleteVolume, ControllerPublishVolume, ControllerUnpublishVolume, ValidateVolumeCapabilities, ListVolumes, GetCapacity, ControllerGetCapabilities, CreateSnapshot, DeleteSnapshot, ListSnapshots, ControllerExpandVolume, ControllerGetVolume
Node: NodeStageVolume, NodeUnstageVolume, NodePublishVolume, NodeUnpublishVolume, NodeGetVolumeStats, NodeGetCapabilities, NodeGetInfo, NodeExpandVolume

## Nomad

```hcl
# Register plugin
job "btrfs-csi" {
  type = "system"
  group "csi" {
    task "plugin" {
      driver = "exec"
      csi_plugin { id = "btrfs-csi" type = "controller" mount_dir = "/var/run/csi" }
      config { command = "local/btrfs-csi" }
    }
  }
}

# Create volume
volume "my-data" {
  type = "csi"
  plugin_id = "btrfs-csi"
  capacity_min = "10GiB"
  capability { access_mode = "single-node-writer" attachment_mode = "file-system" }
  parameters { replica_count = "3" profile = "database" }
}
```

## License

AGPL-3.0