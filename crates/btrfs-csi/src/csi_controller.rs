use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::{Request, Response, Status};

use btrfs_exchange::gossip::GossipService;
use btrfs_exchange::replicator::Replicator;

use crate::csi::controller_server::Controller;
use crate::csi::*;

pub struct CsiController {
    node_id: String,
    zone: String,
    data_dir: String,
    gossip: Arc<GossipService>,
    replicator: Arc<Replicator>,
    volumes: Arc<RwLock<HashMap<String, VolumeInfo>>>,
    snapshots: Arc<RwLock<HashMap<String, SnapshotInfo>>>,
}

#[derive(Clone, Debug)]
struct VolumeInfo {
    id: String,
    name: String,
    size: u64,
    node_id: String,
    zone: String,
    status: String,
}

#[derive(Clone, Debug)]
struct SnapshotInfo {
    id: String,
    source_volume_id: String,
    name: String,
    size: u64,
    creation_time: i64,
}

impl CsiController {
    pub fn new(
        node_id: String,
        zone: String,
        data_dir: String,
        gossip: Arc<GossipService>,
        replicator: Arc<Replicator>,
    ) -> Self {
        Self {
            node_id,
            zone,
            data_dir,
            gossip,
            replicator,
            volumes: Arc::new(RwLock::new(HashMap::new())),
            snapshots: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

#[tonic::async_trait]
impl Controller for CsiController {
    async fn create_volume(
        &self,
        request: Request<CreateVolumeRequest>,
    ) -> Result<Response<CreateVolumeResponse>, Status> {
        let req = request.into_inner();
        let volume_name = &req.name;

        tracing::info!("CSI CreateVolume: name={}", volume_name);

        // Determine capacity
        let capacity = req
            .capacity_range
            .as_ref()
            .map(|r| r.required_bytes as u64)
            .unwrap_or(1024 * 1024 * 1024); // Default 1GB

        // Check if volume already exists
        {
            let volumes = self.volumes.read().await;
            if let Some(existing) = volumes.values().find(|v| v.name == volume_name) {
                if existing.size >= capacity {
                    tracing::info!("Volume {} already exists, returning it", volume_name);
                    return Ok(Response::new(CreateVolumeResponse {
                        volume: Some(Volume {
                            volume_id: existing.id.clone(),
                            capacity_bytes: existing.size as i64,
                            volume_capabilities: req.volume_capabilities.clone(),
                            volume_context: HashMap::new(),
                            parameters: HashMap::new(),
                            content_source_volume_id: String::new(),
                            content_source_snapshot_id: String::new(),
                            accessible_topology: vec![],
                        }),
                    }));
                }
            }
        }

        // Create btrfs subvolume
        let subvol_path = format!("{}/{}", self.data_dir, volume_name);
        let output = tokio::process::Command::new("btrfs")
            .args(["subvolume", "create", &subvol_path])
            .output()
            .await
            .map_err(|e| Status::internal(format!("Failed to execute btrfs: {}", e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(Status::internal(format!(
                "Failed to create subvolume: {}",
                stderr
            )));
        }

        let volume_id = format!("vol-{}", uuid::Uuid::new_v4());

        let vol_info = VolumeInfo {
            id: volume_id.clone(),
            name: volume_name.clone(),
            size: capacity,
            node_id: self.node_id.clone(),
            zone: self.zone.clone(),
            status: "available".to_string(),
        };

        {
            let mut volumes = self.volumes.write().await;
            volumes.insert(volume_id.clone(), vol_info);
        }

        tracing::info!("Volume {} created successfully (id={})", volume_name, volume_id);

        Ok(Response::new(CreateVolumeResponse {
            volume: Some(Volume {
                volume_id,
                capacity_bytes: capacity as i64,
                volume_capabilities: req.volume_capabilities.clone(),
                volume_context: HashMap::new(),
                parameters: HashMap::new(),
                content_source_volume_id: String::new(),
                content_source_snapshot_id: String::new(),
                accessible_topology: vec![],
            }),
        }))
    }

    async fn delete_volume(
        &self,
        request: Request<DeleteVolumeRequest>,
    ) -> Result<Response<DeleteVolumeResponse>, Status> {
        let req = request.into_inner();
        tracing::info!("CSI DeleteVolume: volume_id={}", req.volume_id);

        // Get volume info
        let vol_info = {
            let volumes = self.volumes.read().await;
            volumes.get(&req.volume_id).cloned()
        };

        let vol_info = vol_info.ok_or_else(|| {
            Status::not_found(format!("Volume {} not found", req.volume_id))
        })?;

        // Delete btrfs subvolume
        let subvol_path = format!("{}/{}", self.data_dir, vol_info.name);
        let output = tokio::process::Command::new("btrfs")
            .args(["subvolume", "delete", &subvol_path])
            .output()
            .await
            .map_err(|e| Status::internal(format!("Failed to execute btrfs: {}", e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            // If it's a snapshot, try delete anyway
            if !stderr.contains("not a subvolume") {
                return Err(Status::internal(format!(
                    "Failed to delete subvolume: {}",
                    stderr
                )));
            }
        }

        {
            let mut volumes = self.volumes.write().await;
            volumes.remove(&req.volume_id);
        }

        tracing::info!("Volume {} deleted", req.volume_id);

        Ok(Response::new(DeleteVolumeResponse {}))
    }

    async fn controller_publish_volume(
        &self,
        request: Request<ControllerPublishVolumeRequest>,
    ) -> Result<Response<ControllerPublishVolumeResponse>, Status> {
        let req = request.into_inner();
        tracing::info!(
            "CSI ControllerPublishVolume: volume_id={}, node_id={}",
            req.volume_id,
            req.node_id
        );

        // For btrfs, "publish" means ensuring the volume is available on the node
        // Since btrfs volumes are local, we just return the path as publish context
        let vol_info = {
            let volumes = self.volumes.read().await;
            volumes.get(&req.volume_id).cloned()
        };

        let vol_info = vol_info.ok_or_else(|| {
            Status::not_found(format!("Volume {} not found", req.volume_id))
        })?;

        let publish_context = serde_json::to_string(&serde_json::json!({
            "path": format!("{}/{}", self.data_dir, vol_info.name),
            "node_id": self.node_id,
        }))
        .unwrap_or_default();

        Ok(Response::new(ControllerPublishVolumeResponse {
            publish_context,
        }))
    }

    async fn controller_unpublish_volume(
        &self,
        request: Request<ControllerUnpublishVolumeRequest>,
    ) -> Result<Response<ControllerUnpublishVolumeResponse>, Status> {
        let req = request.into_inner();
        tracing::info!(
            "CSI ControllerUnpublishVolume: volume_id={}, node_id={}",
            req.volume_id,
            req.node_id
        );

        Ok(Response::new(ControllerUnpublishVolumeResponse {}))
    }

    async fn validate_volume_capabilities(
        &self,
        request: Request<ValidateVolumeCapabilitiesRequest>,
    ) -> Result<Response<ValidateVolumeCapabilitiesResponse>, Status> {
        let req = request.into_inner();
        tracing::info!("CSI ValidateVolumeCapabilities");

        // Check volume exists
        {
            let volumes = self.volumes.read().await;
            if volumes.get(&req.volume_id).is_none() {
                return Err(Status::not_found(format!(
                    "Volume {} not found",
                    req.volume_id
                )));
            }
        }

        // We support everything btrfs supports
        Ok(Response::new(ValidateVolumeCapabilitiesResponse {
            confirmed: Some(
                validate_volume_capabilities_response::Confirmed {
                    volume_capabilities: req.volume_capabilities.clone(),
                    parameters: HashMap::new(),
                },
            ),
            message: String::new(),
        }))
    }

    async fn list_volumes(
        &self,
        request: Request<ListVolumesRequest>,
    ) -> Result<Response<ListVolumesResponse>, Status> {
        let req = request.into_inner();
        tracing::debug!("CSI ListVolumes");

        let volumes = self.volumes.read().await;
        let mut entries: Vec<list_volumes_response::Entry> = volumes
            .values()
            .map(|v| list_volumes_response::Entry {
                volume: Some(Volume {
                    volume_id: v.id.clone(),
                    capacity_bytes: v.size as i64,
                    volume_capabilities: None,
                    volume_context: HashMap::new(),
                    parameters: HashMap::new(),
                    content_source_volume_id: String::new(),
                    content_source_snapshot_id: String::new(),
                    accessible_topology: vec![],
                }),
                status: Some(VolumeStatus {
                    volume_id: v.id.clone(),
                    node_id: vec![v.node_id.clone()],
                    accessible_topology: vec![],
                }),
            })
            .collect();

        // Simple pagination
        let start = req
            .starting_token
            .parse::<usize>()
            .unwrap_or(0);
        let max = req.max_entries as usize;

        if max > 0 && entries.len() > start + max {
            entries = entries.into_iter().skip(start).take(max).collect();
            let next_token = (start + max).to_string();
            Ok(Response::new(ListVolumesResponse {
                entries,
                next_token,
            }))
        } else {
            if start > 0 {
                entries = entries.into_iter().skip(start).collect();
            }
            Ok(Response::new(ListVolumesResponse {
                entries,
                next_token: String::new(),
            }))
        }
    }

    async fn get_capacity(
        &self,
        request: Request<GetCapacityRequest>,
    ) -> Result<Response<GetCapacityResponse>, Status> {
        tracing::debug!("CSI GetCapacity");

        // Get filesystem usage via btrfs command
        let output = tokio::process::Command::new("btrfs")
            .args(["filesystem", "usage", "-b", &self.data_dir])
            .output()
            .await
            .map_err(|e| Status::internal(format!("Failed to get filesystem usage: {}", e)))?;

        let stdout = String::from_utf8_lossy(&output.stdout);
        let available = parse_btrfs_free_space(&stdout);

        Ok(Response::new(GetCapacityResponse {
            available_capacity: available as i64,
        }))
    }

    async fn create_snapshot(
        &self,
        request: Request<CreateSnapshotRequest>,
    ) -> Result<Response<CreateSnapshotResponse>, Status> {
        let req = request.into_inner();
        tracing::info!(
            "CSI CreateSnapshot: source_volume_id={}, name={}",
            req.source_volume_id,
            req.name
        );

        // Get source volume
        let vol_info = {
            let volumes = self.volumes.read().await;
            volumes.get(&req.source_volume_id).cloned()
        };

        let vol_info = vol_info.ok_or_else(|| {
            Status::not_found(format!("Source volume {} not found", req.source_volume_id))
        })?;

        // Create btrfs snapshot
        let source_path = format!("{}/{}", self.data_dir, vol_info.name);
        let snap_path = format!("{}/{}", self.data_dir, req.name);

        let output = tokio::process::Command::new("btrfs")
            .args([
                "subvolume",
                "snapshot",
                "-r",
                &source_path,
                &snap_path,
            ])
            .output()
            .await
            .map_err(|e| Status::internal(format!("Failed to execute btrfs: {}", e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(Status::internal(format!(
                "Failed to create snapshot: {}",
                stderr
            )));
        }

        let snapshot_id = format!("snap-{}", uuid::Uuid::new_v4());
        let creation_time = chrono::Utc::now().timestamp();

        let snap_info = SnapshotInfo {
            id: snapshot_id.clone(),
            source_volume_id: req.source_volume_id.clone(),
            name: req.name.clone(),
            size: vol_info.size,
            creation_time,
        };

        {
            let mut snapshots = self.snapshots.write().await;
            snapshots.insert(snapshot_id.clone(), snap_info);
        }

        tracing::info!("Snapshot {} created (id={})", req.name, snapshot_id);

        Ok(Response::new(CreateSnapshotResponse {
            snapshot: Some(crate::csi::Snapshot {
                snapshot_id,
                source_volume_id: req.source_volume_id,
                creation_time,
                size_bytes: vol_info.size as i64,
                snapshot_context: HashMap::new(),
            }),
        }))
    }

    async fn delete_snapshot(
        &self,
        request: Request<DeleteSnapshotRequest>,
    ) -> Result<Response<DeleteSnapshotResponse>, Status> {
        let req = request.into_inner();
        tracing::info!("CSI DeleteSnapshot: snapshot_id={}", req.snapshot_id);

        let snap_info = {
            let snapshots = self.snapshots.read().await;
            snapshots.get(&req.snapshot_id).cloned()
        };

        let snap_info = snap_info.ok_or_else(|| {
            Status::not_found(format!("Snapshot {} not found", req.snapshot_id))
        })?;

        // Delete btrfs snapshot
        let snap_path = format!("{}/{}", self.data_dir, snap_info.name);
        let output = tokio::process::Command::new("btrfs")
            .args(["subvolume", "delete", &snap_path])
            .output()
            .await
            .map_err(|e| Status::internal(format!("Failed to execute btrfs: {}", e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            tracing::warn!("Failed to delete snapshot: {}", stderr);
        }

        {
            let mut snapshots = self.snapshots.write().await;
            snapshots.remove(&req.snapshot_id);
        }

        Ok(Response::new(DeleteSnapshotResponse {}))
    }

    async fn list_snapshots(
        &self,
        request: Request<ListSnapshotsRequest>,
    ) -> Result<Response<ListSnapshotsResponse>, Status> {
        tracing::debug!("CSI ListSnapshots");

        let snapshots = self.snapshots.read().await;
        let entries: Vec<list_snapshots_response::Entry> = snapshots
            .values()
            .map(|s| list_snapshots_response::Entry {
                snapshot: Some(crate::csi::Snapshot {
                    snapshot_id: s.id.clone(),
                    source_volume_id: s.source_volume_id.clone(),
                    creation_time: s.creation_time,
                    size_bytes: s.size as i64,
                    snapshot_context: HashMap::new(),
                }),
            })
            .collect();

        Ok(Response::new(ListSnapshotsResponse {
            entries,
            next_token: String::new(),
        }))
    }

    async fn controller_expand_volume(
        &self,
        request: Request<ControllerExpandVolumeRequest>,
    ) -> Result<Response<ControllerExpandVolumeResponse>, Status> {
        let req = request.into_inner();
        tracing::info!("CSI ControllerExpandVolume: volume_id={}", req.volume_id);

        // Btrfs supports online resize, report that node expansion is required
        let new_capacity = req
            .capacity_range
            .as_ref()
            .map(|r| r.required_bytes)
            .unwrap_or(0);

        Ok(Response::new(ControllerExpandVolumeResponse {
            capacity_bytes: new_capacity,
            node_expansion_required: true,
        }))
    }

    async fn controller_get_volume(
        &self,
        request: Request<ControllerGetVolumeRequest>,
    ) -> Result<Response<ControllerGetVolumeResponse>, Status> {
        let req = request.into_inner();

        let volumes = self.volumes.read().await;
        match volumes.get(&req.volume_id) {
            Some(v) => Ok(Response::new(ControllerGetVolumeResponse {
                volume: Some(Volume {
                    volume_id: v.id.clone(),
                    capacity_bytes: v.size as i64,
                    volume_capabilities: None,
                    volume_context: HashMap::new(),
                    parameters: HashMap::new(),
                    content_source_volume_id: String::new(),
                    content_source_snapshot_id: String::new(),
                    accessible_topology: vec![],
                }),
                status: Some(VolumeStatus {
                    volume_id: v.id.clone(),
                    node_id: vec![v.node_id.clone()],
                    accessible_topology: vec![],
                }),
            })),
            None => Err(Status::not_found(format!(
                "Volume {} not found",
                req.volume_id
            ))),
        }
    }
}

fn parse_btrfs_free_space(output: &str) -> u64 {
    for line in output.lines() {
        if line.contains("Free (estimated):") {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() >= 3 {
                if let Ok(val) = parts[2].parse::<u64>() {
                    return val;
                }
            }
        }
        if line.contains("Free:") {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() >= 2 {
                if let Ok(val) = parts[1].parse::<u64>() {
                    return val;
                }
            }
        }
    }
    0
}
