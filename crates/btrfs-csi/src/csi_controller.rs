use std::collections::HashMap;
use std::sync::Arc;
use tonic::{Request, Response, Status};

use btrfs_exchange::config::VolumeProfile;
use btrfs_exchange::gossip::GossipService;
use btrfs_exchange::replicator::Replicator;
use btrfs_ops::xattr;
use tracing::warn;

use crate::csi::controller_server::Controller;
use crate::csi::*;

/// Validate volume name: reject path traversal, null bytes, empty names
fn validate_volume_name(name: &str) -> Result<(), Status> {
    if name.is_empty() {
        return Err(Status::invalid_argument("volume name must not be empty"));
    }
    if name.contains('\0') {
        return Err(Status::invalid_argument("volume name must not contain null bytes"));
    }
    if name.contains("..") || name.contains('/') || name.contains('\\') {
        return Err(Status::invalid_argument(format!(
            "volume name must not contain path separators or '..': {}", name
        )));
    }
    if name.len() > 255 {
        return Err(Status::invalid_argument("volume name too long (max 255 chars)"));
    }
    if !name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || b == b'.') {
        return Err(Status::invalid_argument(
            "volume name must only contain [a-zA-Z0-9._-]"
        ));
    }
    Ok(())
}

#[derive(Clone)]
pub struct CsiController {
    node_id: String,
    zone: String,
    data_dir: String,
    gossip: Arc<GossipService>,
    replicator: Arc<Replicator>,
    volume_profiles: HashMap<String, VolumeProfile>,
}

impl CsiController {
    pub fn new(
        node_id: String,
        zone: String,
        data_dir: String,
        gossip: Arc<GossipService>,
        replicator: Arc<Replicator>,
        volume_profiles: HashMap<String, VolumeProfile>,
    ) -> Self {
        Self { node_id, zone, data_dir, gossip, replicator, volume_profiles }
    }

    fn subvol_path(&self, name: &str) -> String {
        format!("{}/{}", self.data_dir, name)
    }

    fn get_profile(&self, profile_type: &str) -> Result<&VolumeProfile, Status> {
        self.volume_profiles
            .get(profile_type)
            .or_else(|| self.volume_profiles.get("default"))
            .ok_or_else(|| Status::internal(format!(
                "No volume profile found for '{}' and no default profile configured", profile_type
            )))
    }

    async fn volume_xattr(&self, name: &str, key: &str) -> Option<String> {
        xattr::get_csi_attr(&self.subvol_path(name), key).await.ok().flatten()
    }

    async fn set_vol_xattr(&self, name: &str, key: &str, value: &str) {
        if let Err(e) = xattr::set_csi_attr(&self.subvol_path(name), key, value).await {
            tracing::warn!("Failed to set xattr {} on {}: {}", key, name, e);
        }
    }

    async fn get_published_nodes(&self, name: &str) -> Vec<String> {
        self.volume_xattr(name, "published").await
            .filter(|v| !v.is_empty())
            .map(|v| v.split(',').map(|s| s.trim().to_string()).collect())
            .unwrap_or_default()
    }

    async fn add_published_node(&self, name: &str, node_id: &str) {
        let mut nodes = self.get_published_nodes(name).await;
        if !nodes.contains(&node_id.to_string()) {
            nodes.push(node_id.to_string());
            self.set_vol_xattr(name, "published", &nodes.join(",")).await;
        }
    }

    async fn remove_published_node(&self, name: &str, node_id: &str) {
        let nodes = self.get_published_nodes(name).await;
        let remaining: Vec<String> = nodes.into_iter()
            .filter(|n| n != node_id)
            .collect();
        if remaining.is_empty() {
            let _ = xattr::remove_csi_attr(&self.subvol_path(name), "published").await;
        } else {
            self.set_vol_xattr(name, "published", &remaining.join(",")).await;
        }
    }

    fn volume_id(&self, name: &str) -> String {
        format!("vol-{}", uuid::Uuid::new_v4())
    }

    fn snapshot_id(&self) -> String {
        format!("snap-{}", uuid::Uuid::new_v4())
    }
}

#[tonic::async_trait]
impl Controller for CsiController {
    async fn create_volume(
        &self,
        request: Request<CreateVolumeRequest>,
    ) -> Result<Response<CreateVolumeResponse>, Status> {
        let req = request.into_inner();
        tracing::info!("CSI CreateVolume: name={}", req.name);

        validate_volume_name(&req.name)?;

        let capacity = req
            .capacity_range
            .as_ref()
            .map(|r| r.required_bytes as u64)
            .unwrap_or(1024 * 1024 * 1024);

        let subvol_path = self.subvol_path(&req.name);
        let subvol_exists = tokio::process::Command::new("btrfs")
            .args(["subvolume", "show", &subvol_path])
            .output()
            .await
            .map(|o| o.status.success())
            .unwrap_or(false);

        if subvol_exists {
            // Idempotency: check existing xattr
            if let Some(vid) = self.volume_xattr(&req.name, "volume_id").await {
                tracing::info!("Volume {} already exists (id={})", req.name, vid);
                return Ok(Response::new(CreateVolumeResponse {
                    volume: Some(Volume {
                        volume_id: vid,
                        capacity_bytes: capacity as i64,
                        volume_capabilities: req.volume_capabilities.clone(),
                        ..Default::default()
                    }),
                }));
            }
        } else {
            let output = tokio::process::Command::new("btrfs")
                .args(["subvolume", "create", &subvol_path])
                .output()
                .await
                .map_err(|e| Status::internal(format!("Failed to execute btrfs: {}", e)))?;

            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                // Race: check if another controller created it
                if self.volume_xattr(&req.name, "volume_id").await.is_some() {
                    let vid = self.volume_xattr(&req.name, "volume_id").await.unwrap();
                    return Ok(Response::new(CreateVolumeResponse {
                        volume: Some(Volume {
                            volume_id: vid,
                            capacity_bytes: capacity as i64,
                            volume_capabilities: req.volume_capabilities.clone(),
                            ..Default::default()
                        }),
                    }));
                }
                return Err(Status::internal(format!("Failed to create subvolume: {}", stderr)));
            }
        }

        // Determine profile type from parameters
        let profile_type = req.parameters
            .get("volume_type")
            .cloned()
            .unwrap_or_else(|| "default".to_string());

        let profile = self.get_profile(&profile_type)?;

        if profile_type == "database" && !profile.sync_writes {
            tracing::warn!(
                "Database volume profile '{}' has sync_writes=false, forcing to true",
                profile_type
            );
        }

        if profile.nocow {
            let _ = tokio::process::Command::new("chattr")
                .args(["+C", &subvol_path])
                .output()
                .await;
        }

        if profile.compression != "none" {
            let comp_opt = format!("compression={}", profile.compression);
            let _ = tokio::process::Command::new("btrfs")
                .args(["property", "set", "-ts", &subvol_path, &comp_opt])
                .output()
                .await;
        }

        // Enable filesystem-level quota (idempotent)
        let _ = tokio::process::Command::new("btrfs")
            .args(["quota", "enable", "--simple", &self.data_dir])
            .output()
            .await;

        // Set qgroup limit via subvolume ID
        if let Ok(subvol_id) = get_subvolume_id(&subvol_path).await {
            let _ = tokio::process::Command::new("btrfs")
                .args(["qgroup", "limit", &capacity.to_string(), &subvol_path])
                .output()
                .await;
            info!("Set quota limit={} on subvolume {} (id={})", capacity, subvol_path, subvol_id);
        }

        // Write metadata as xattr on the subvolume
        let vol_id = self.volume_id(&req.name);
        let now = chrono::Utc::now().timestamp_millis().to_string();
        self.set_vol_xattr(&req.name, "volume_id", &vol_id).await;
        self.set_vol_xattr(&req.name, "size", &capacity.to_string()).await;
        self.set_vol_xattr(&req.name, "zone", &self.zone).await;
        self.set_vol_xattr(&req.name, "profile", &profile_type).await;
        self.set_vol_xattr(&req.name, "created_at", &now).await;

        // Initialize epoch/vector clock for quorum tracking
        let initial_clock = [(self.node_id.clone(), 1u64)];
        xattr::set_epoch(&subvol_path, 1).await.ok();
        xattr::set_vector_clock(&subvol_path, &initial_clock.iter().cloned().collect()).await.ok();
        xattr::set_volume_status(&subvol_path, xattr::VOLUME_STATUS_ACTIVE).await.ok();

        // Register with gossip quorum system
        self.gossip.register_volume_epoch(
            &vol_id, 1, initial_clock.to_vec(), xattr::VOLUME_STATUS_ACTIVE,
        ).await;

        tracing::info!("Volume {} created (id={})", req.name, vol_id);

        Ok(Response::new(CreateVolumeResponse {
            volume: Some(Volume {
                volume_id: vol_id,
                capacity_bytes: capacity as i64,
                volume_capabilities: req.volume_capabilities.clone(),
                ..Default::default()
            }),
        }))
    }

    async fn delete_volume(
        &self,
        request: Request<DeleteVolumeRequest>,
    ) -> Result<Response<DeleteVolumeResponse>, Status> {
        let req = request.into_inner();
        tracing::info!("CSI DeleteVolume: {}", req.volume_id);

        // Find the volume by scanning subvolumes for matching volume_id xattr
        let mut found_name: Option<String> = None;
        if let Ok(subvols) = tokio::fs::read_dir(&self.data_dir).await {
            use tokio_stream::wrappers::ReadDirStream;
            use tokio_stream::StreamExt;
            let mut entries = ReadDirStream::new(subvols);
            while let Some(Ok(entry)) = entries.next().await {
                let name = entry.file_name().to_string_lossy().to_string();
                if let Ok(Some(vid)) = xattr::get_csi_attr(&entry.path().to_string_lossy(), "volume_id").await {
                    if vid == req.volume_id {
                        found_name = Some(name);
                        break;
                    }
                }
            }
        }

        let name = match found_name {
            Some(n) => n,
            None => {
                tracing::warn!("Volume {} not found, may have been deleted already", req.volume_id);
                return Ok(Response::new(DeleteVolumeResponse {}));
            }
        };

        // Check if still published on any node
        let published = self.get_published_nodes(&name).await;
        if !published.is_empty() {
            return Err(Status::failed_precondition(format!(
                "Volume {} is still published on nodes: {}",
                req.volume_id, published.join(", ")
            )));
        }

        let subvol_path = self.subvol_path(&name);
        let output = tokio::process::Command::new("btrfs")
            .args(["subvolume", "delete", &subvol_path])
            .output()
            .await;

        match output {
            Ok(o) if !o.status.success() => {
                let stderr = String::from_utf8_lossy(&o.stderr);
                tracing::error!("Failed to delete subvolume {}: {}", name, stderr);
            }
            Err(e) => tracing::error!("Failed to execute btrfs delete: {}", e),
            _ => {}
        }

        Ok(Response::new(DeleteVolumeResponse {}))
    }

    async fn controller_publish_volume(
        &self,
        request: Request<ControllerPublishVolumeRequest>,
    ) -> Result<Response<ControllerPublishVolumeResponse>, Status> {
        let req = request.into_inner();
        tracing::info!("CSI ControllerPublishVolume: volume_id={}, node_id={}", req.volume_id, req.node_id);

        // Find volume name by volume_id xattr
        let mut found_name: Option<String> = None;
        let mut found_path: Option<String> = None;
        if let Ok(subvols) = tokio::fs::read_dir(&self.data_dir).await {
            use tokio_stream::wrappers::ReadDirStream;
            use tokio_stream::StreamExt;
            let mut entries = ReadDirStream::new(subvols);
            while let Some(Ok(entry)) = entries.next().await {
                let name = entry.file_name().to_string_lossy().to_string();
                if let Ok(Some(vid)) = xattr::get_csi_attr(&entry.path().to_string_lossy(), "volume_id").await {
                    if vid == req.volume_id {
                        found_name = Some(name);
                        found_path = Some(entry.path().to_string_lossy().to_string());
                        break;
                    }
                }
            }
        }

        let name = found_name.ok_or_else(|| Status::not_found(format!("Volume {} not found", req.volume_id)))?;
        let subvol_path = found_path.unwrap_or_else(|| self.subvol_path(&name));

        // Check quorum: get current epoch info and request lease
        let epoch = xattr::get_epoch(&subvol_path).await;
        let vclock = xattr::get_vector_clock(&subvol_path).await;
        let vclock_vec: Vec<(String, u64)> = vclock.into_iter().collect();
        let status = xattr::get_volume_status(&subvol_path).await;

        // If volume is in conflict, refuse publish
        if status == xattr::VOLUME_STATUS_CONFLICT {
            return Err(Status::failed_precondition(format!(
                "Volume {} is in CONFLICT state. Manual resolution required via btrfs send/receive.",
                req.volume_id
            )));
        }

        // Request quorum lease from peers
        let quorum = self.gossip.request_quorum_lease(
            &req.volume_id, epoch, &vclock_vec,
        ).await.map_err(|e| Status::internal(format!("Quorum check failed: {}", e)))?;

        let volume_readonly = if quorum.conflict {
            // Mark as conflict
            xattr::set_volume_status(&subvol_path, xattr::VOLUME_STATUS_CONFLICT).await.ok();
            return Err(Status::failed_precondition(format!(
                "Volume {} is in CONFLICT state after quorum check ({} {})",
                req.volume_id, quorum.votes_received, quorum.votes_needed
            )));
        } else if !quorum.granted {
            // Minority partition: mark as readonly, allow mounting but read-only
            warn!(
                "Volume {} quorum NOT granted ({}/{}), publishing as READ-ONLY",
                req.volume_id, quorum.votes_received, quorum.votes_needed
            );
            xattr::set_volume_status(&subvol_path, xattr::VOLUME_STATUS_READONLY).await.ok();
            true
        } else {
            // Quorum granted: mark as active
            xattr::set_volume_status(&subvol_path, xattr::VOLUME_STATUS_ACTIVE).await.ok();
            false
        };

        self.add_published_node(&name, &req.node_id).await;

        let publish_context = serde_json::to_string(&serde_json::json!({
            "path": self.subvol_path(&name),
            "node_id": self.node_id,
            "readonly": volume_readonly,
        })).unwrap_or_default();

        Ok(Response::new(ControllerPublishVolumeResponse { publish_context }))
    }

    async fn controller_unpublish_volume(
        &self,
        request: Request<ControllerUnpublishVolumeRequest>,
    ) -> Result<Response<ControllerUnpublishVolumeResponse>, Status> {
        let req = request.into_inner();
        tracing::info!("CSI ControllerUnpublishVolume: volume_id={}, node_id={}", req.volume_id, req.node_id);

        // Find volume name by volume_id xattr
        if let Ok(subvols) = tokio::fs::read_dir(&self.data_dir).await {
            use tokio_stream::wrappers::ReadDirStream;
            use tokio_stream::StreamExt;
            let mut entries = ReadDirStream::new(subvols);
            while let Some(Ok(entry)) = entries.next().await {
                let name = entry.file_name().to_string_lossy().to_string();
                if let Ok(Some(vid)) = xattr::get_csi_attr(&entry.path().to_string_lossy(), "volume_id").await {
                    if vid == req.volume_id {
                        self.remove_published_node(&name, &req.node_id).await;
                        break;
                    }
                }
            }
        }

        Ok(Response::new(ControllerUnpublishVolumeResponse {}))
    }

    async fn validate_volume_capabilities(
        &self,
        request: Request<ValidateVolumeCapabilitiesRequest>,
    ) -> Result<Response<ValidateVolumeCapabilitiesResponse>, Status> {
        let req = request.into_inner();

        let vol_id = req.volume_id.first()
            .ok_or_else(|| Status::invalid_argument("volume_id is required"))?;

        let found = if let Ok(subvols) = tokio::fs::read_dir(&self.data_dir).await {
            use tokio_stream::wrappers::ReadDirStream;
            use tokio_stream::StreamExt;
            let mut entries = ReadDirStream::new(subvols);
            let mut ok = false;
            while let Some(Ok(entry)) = entries.next().await {
                if let Ok(Some(vid)) = xattr::get_csi_attr(&entry.path().to_string_lossy(), "volume_id").await {
                    if vid == *vol_id {
                        ok = true;
                        break;
                    }
                }
            }
            ok
        } else { false };

        if !found {
            return Err(Status::not_found(format!("Volume {} not found", vol_id)));
        }

        Ok(Response::new(ValidateVolumeCapabilitiesResponse {
            confirmed: vec![validate_volume_capabilities_response::Confirmed {
                volume_capabilities: req.volume_capabilities.clone(),
                ..Default::default()
            }],
            ..Default::default()
        }))
    }

    async fn list_volumes(
        &self,
        request: Request<ListVolumesRequest>,
    ) -> Result<Response<ListVolumesResponse>, Status> {
        let req = request.into_inner();

        let mut entries = Vec::new();
        if let Ok(subvols) = tokio::fs::read_dir(&self.data_dir).await {
            use tokio_stream::wrappers::ReadDirStream;
            use tokio_stream::StreamExt;
            let mut entries_stream = ReadDirStream::new(subvols);
            while let Some(Ok(entry)) = entries_stream.next().await {
                let path = entry.path().to_string_lossy().to_string();
                if let Ok(Some(vid)) = xattr::get_csi_attr(&path, "volume_id").await {
                    let size = xattr::get_csi_attr(&path, "size").await.ok().flatten()
                        .and_then(|s| s.parse::<u64>().ok()).unwrap_or(0);
                    entries.push(list_volumes_response::Entry {
                        volume: Some(Volume {
                            volume_id: vid.clone(),
                            capacity_bytes: size as i64,
                            ..Default::default()
                        }),
                        status: Some(VolumeStatus {
                            volume_id: vid,
                            ..Default::default()
                        }),
                    });
                }
            }
        }

        // Pagination
        let start = req.starting_token.parse::<usize>().unwrap_or(0);
        let max = req.max_entries as usize;

        if max > 0 && entries.len() > start + max {
            entries = entries.into_iter().skip(start).take(max).collect();
            Ok(Response::new(ListVolumesResponse { entries, next_token: (start + max).to_string() }))
        } else {
            if start > 0 { entries = entries.into_iter().skip(start).collect(); }
            Ok(Response::new(ListVolumesResponse { entries, next_token: String::new() }))
        }
    }

    async fn get_capacity(
        &self,
        request: Request<GetCapacityRequest>,
    ) -> Result<Response<GetCapacityResponse>, Status> {
        let req = request.into_inner();

        if let Some(topo_req) = req.topology_requirement.as_ref() {
            let segments = &topo_req.requisite;
            let zone_matches = segments.iter().any(|t| {
                t.segments.get("topology.btrfs-csi/zone").map(|z| z == &self.zone).unwrap_or(false)
            });
            if !zone_matches && !segments.is_empty() {
                return Ok(Response::new(GetCapacityResponse { available_capacity: 0 }));
            }
        }

        let output = tokio::process::Command::new("btrfs")
            .args(["filesystem", "usage", "-b", &self.data_dir])
            .output()
            .await
            .map_err(|e| Status::internal(format!("Failed to get usage: {}", e)))?;

        let stdout = String::from_utf8_lossy(&output.stdout);
        let available = parse_btrfs_free_space(&stdout);

        Ok(Response::new(GetCapacityResponse { available_capacity: available as i64 }))
    }

    async fn create_snapshot(
        &self,
        request: Request<CreateSnapshotRequest>,
    ) -> Result<Response<CreateSnapshotResponse>, Status> {
        let req = request.into_inner();
        tracing::info!("CSI CreateSnapshot: source={}, name={}", req.source_volume_id, req.name);

        validate_volume_name(&req.name)?;

        // Check if snapshot already exists (by scanning)
        let snap_path = self.subvol_path(&req.name);
        let snap_exists = tokio::process::Command::new("btrfs")
            .args(["subvolume", "show", &snap_path])
            .output()
            .await
            .map(|o| o.status.success())
            .unwrap_or(false);

        if snap_exists {
            if let Some(sid) = self.volume_xattr(&req.name, "snapshot_id").await {
                tracing::info!("Snapshot {} already exists (id={})", req.name, sid);
                let source = self.volume_xattr(&req.name, "source_volume_id").await.unwrap_or_default();
                let ctime = self.volume_xattr(&req.name, "creation_time").await
                    .and_then(|v| v.parse::<i64>().ok()).unwrap_or(0);
                let size = self.volume_xattr(&req.name, "size").await
                    .and_then(|v| v.parse::<u64>().ok()).unwrap_or(0);
                return Ok(Response::new(CreateSnapshotResponse {
                    snapshot: Some(Snapshot {
                        snapshot_id: sid,
                        source_volume_id: source,
                        creation_time: ctime,
                        size_bytes: size as i64,
                        ..Default::default()
                    }),
                }));
            }
        }

        // Find source volume name
        let mut source_name: Option<String> = None;
        if let Ok(subvols) = tokio::fs::read_dir(&self.data_dir).await {
            use tokio_stream::wrappers::ReadDirStream;
            use tokio_stream::StreamExt;
            let mut entries = ReadDirStream::new(subvols);
            while let Some(Ok(entry)) = entries.next().await {
                let name = entry.file_name().to_string_lossy().to_string();
                if let Ok(Some(vid)) = xattr::get_csi_attr(&entry.path().to_string_lossy(), "volume_id").await {
                    if vid == req.source_volume_id {
                        source_name = Some(name);
                        break;
                    }
                }
            }
        }

        let source_name = source_name.ok_or_else(|| {
            Status::not_found(format!("Source volume {} not found", req.source_volume_id))
        })?;

        let source_path = self.subvol_path(&source_name);

        let output = tokio::process::Command::new("btrfs")
            .args(["subvolume", "snapshot", "-r", &source_path, &snap_path])
            .output()
            .await
            .map_err(|e| Status::internal(format!("Failed to create snapshot: {}", e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(Status::internal(format!("Failed to create snapshot: {}", stderr)));
        }

        let snap_id = self.snapshot_id();
        let creation_time = chrono::Utc::now().timestamp();
        let size = 0u64; // snapshot size is dynamic

        // Write xattr on snapshot subvolume
        self.set_vol_xattr(&req.name, "snapshot_id", &snap_id).await;
        self.set_vol_xattr(&req.name, "source_volume_id", &req.source_volume_id).await;
        self.set_vol_xattr(&req.name, "snapshot_name", &req.name).await;
        self.set_vol_xattr(&req.name, "size", &size.to_string()).await;
        self.set_vol_xattr(&req.name, "creation_time", &creation_time.to_string()).await;

        Ok(Response::new(CreateSnapshotResponse {
            snapshot: Some(Snapshot {
                snapshot_id: snap_id,
                source_volume_id: req.source_volume_id,
                creation_time,
                size_bytes: size as i64,
                ..Default::default()
            }),
        }))
    }

    async fn delete_snapshot(
        &self,
        request: Request<DeleteSnapshotRequest>,
    ) -> Result<Response<DeleteSnapshotResponse>, Status> {
        let req = request.into_inner();
        tracing::info!("CSI DeleteSnapshot: {}", req.snapshot_id);

        let mut found_name: Option<String> = None;
        if let Ok(subvols) = tokio::fs::read_dir(&self.data_dir).await {
            use tokio_stream::wrappers::ReadDirStream;
            use tokio_stream::StreamExt;
            let mut entries = ReadDirStream::new(subvols);
            while let Some(Ok(entry)) = entries.next().await {
                let name = entry.file_name().to_string_lossy().to_string();
                if let Ok(Some(sid)) = xattr::get_csi_attr(&entry.path().to_string_lossy(), "snapshot_id").await {
                    if sid == req.snapshot_id {
                        found_name = Some(name);
                        break;
                    }
                }
            }
        }

        let name = match found_name {
            Some(n) => n,
            None => {
                tracing::warn!("Snapshot {} not found", req.snapshot_id);
                return Ok(Response::new(DeleteSnapshotResponse {}));
            }
        };

        let snap_path = self.subvol_path(&name);
        let output = tokio::process::Command::new("btrfs")
            .args(["subvolume", "delete", &snap_path])
            .output()
            .await;

        match output {
            Ok(o) if !o.status.success() => {
                let stderr = String::from_utf8_lossy(&o.stderr);
                tracing::error!("Failed to delete snapshot subvolume {}: {}", name, stderr);
            }
            Err(e) => tracing::error!("Failed to execute btrfs delete for snapshot {}: {}", name, e),
            _ => {}
        }

        Ok(Response::new(DeleteSnapshotResponse {}))
    }

    async fn list_snapshots(
        &self,
        request: Request<ListSnapshotsRequest>,
    ) -> Result<Response<ListSnapshotsResponse>, Status> {
        let req = request.into_inner();

        let mut entries = Vec::new();
        if let Ok(subvols) = tokio::fs::read_dir(&self.data_dir).await {
            use tokio_stream::wrappers::ReadDirStream;
            use tokio_stream::StreamExt;
            let mut entries_stream = ReadDirStream::new(subvols);
            while let Some(Ok(entry)) = entries_stream.next().await {
                let path = entry.path().to_string_lossy().to_string();
                let snap_id = match xattr::get_csi_attr(&path, "snapshot_id").await {
                    Ok(Some(id)) => id,
                    _ => continue,
                };
                let source_vol_id = xattr::get_csi_attr(&path, "source_volume_id").await.ok().flatten()
                    .unwrap_or_default();
                let size = xattr::get_csi_attr(&path, "size").await.ok().flatten()
                    .and_then(|s| s.parse::<u64>().ok()).unwrap_or(0);
                let ctime = xattr::get_csi_attr(&path, "creation_time").await.ok().flatten()
                    .and_then(|s| s.parse::<i64>().ok()).unwrap_or(0);

                let snapshot = Snapshot {
                    snapshot_id: snap_id.clone(),
                    source_volume_id: source_vol_id.clone(),
                    creation_time: ctime,
                    size_bytes: size as i64,
                    ..Default::default()
                };

                // Filter by source_volume_id if provided
                if !req.source_volume_id.is_empty() && source_vol_id != req.source_volume_id {
                    continue;
                }

                entries.push(list_snapshots_response::Entry {
                    snapshot: Some(snapshot),
                });
            }
        }

        // Pagination
        let start = req.starting_token.parse::<usize>().unwrap_or(0);
        let max = req.max_entries as usize;

        if max > 0 && entries.len() > start + max {
            entries = entries.into_iter().skip(start).take(max).collect();
            Ok(Response::new(ListSnapshotsResponse { entries, next_token: (start + max).to_string() }))
        } else {
            if start > 0 { entries = entries.into_iter().skip(start).collect(); }
            Ok(Response::new(ListSnapshotsResponse { entries, next_token: String::new() }))
        }
    }

    async fn controller_expand_volume(
        &self,
        request: Request<ControllerExpandVolumeRequest>,
    ) -> Result<Response<ControllerExpandVolumeResponse>, Status> {
        let req = request.into_inner();
        tracing::info!("CSI ControllerExpandVolume: {}", req.volume_id);

        let new_capacity = req.capacity_range.as_ref().map(|r| r.required_bytes).unwrap_or(0);

        // Find volume and resize
        if let Ok(subvols) = tokio::fs::read_dir(&self.data_dir).await {
            use tokio_stream::wrappers::ReadDirStream;
            use tokio_stream::StreamExt;
            let mut entries = ReadDirStream::new(subvols);
            while let Some(Ok(entry)) = entries.next().await {
                let name = entry.file_name().to_string_lossy().to_string();
                if let Ok(Some(vid)) = xattr::get_csi_attr(&entry.path().to_string_lossy(), "volume_id").await {
                    if vid == req.volume_id {
                        let subvol_path = entry.path().to_string_lossy().to_string();
                        let output = tokio::process::Command::new("btrfs")
                            .args(["filesystem", "resize", "max", &subvol_path])
                            .output()
                            .await;

                        match output {
                            Ok(o) if o.status.success() => {
                                tracing::info!("Resized volume {} to max", req.volume_id);
                                self.set_vol_xattr(&name, "size", &new_capacity.to_string()).await;
                            }
                            Ok(o) => {
                                let stderr = String::from_utf8_lossy(&o.stderr);
                                tracing::warn!("btrfs resize failed: {}", stderr);
                            }
                            Err(e) => tracing::warn!("Failed to execute btrfs resize: {}", e),
                        }
                        break;
                    }
                }
            }
        }

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

        if let Ok(subvols) = tokio::fs::read_dir(&self.data_dir).await {
            use tokio_stream::wrappers::ReadDirStream;
            use tokio_stream::StreamExt;
            let mut entries = ReadDirStream::new(subvols);
            while let Some(Ok(entry)) = entries.next().await {
                if let Ok(Some(vid)) = xattr::get_csi_attr(&entry.path().to_string_lossy(), "volume_id").await {
                    if vid == req.volume_id {
                        let size = xattr::get_csi_attr(&entry.path().to_string_lossy(), "size").await.ok().flatten()
                            .and_then(|s| s.parse::<u64>().ok()).unwrap_or(0);
                        return Ok(Response::new(ControllerGetVolumeResponse {
                            volume: Some(Volume {
                                volume_id: vid,
                                capacity_bytes: size as i64,
                                ..Default::default()
                            }),
                            status: Some(VolumeStatus {
                                volume_id: req.volume_id,
                                ..Default::default()
                            }),
                        }));
                    }
                }
            }
        }

        Err(Status::not_found(format!("Volume {} not found", req.volume_id)))
    }
}

/// Get btrfs subvolume ID from a path
async fn get_subvolume_id(path: &str) -> Option<u64> {
    let output = tokio::process::Command::new("btrfs")
        .args(["subvolume", "show", path])
        .output()
        .await
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    for line in stdout.lines() {
        if line.contains("Subvolume ID:") || line.contains("subvolume id:") {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if let Some(id_str) = parts.last() {
                return id_str.parse::<u64>().ok();
            }
        }
    }
    None
}

fn parse_btrfs_free_space(output: &str) -> u64 {
    for line in output.lines() {
        if line.contains("Free (estimated):") || line.contains("Free:") {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if let Some(s) = parts.last() {
                if let Ok(val) = s.parse::<u64>() {
                    return val;
                }
            }
        }
    }
    0
}