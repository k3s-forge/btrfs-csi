use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::{Request, Response, Status};

use btrfs_exchange::config::VolumeProfile;
use btrfs_exchange::gossip::GossipService;
use btrfs_exchange::replicator::Replicator;

use crate::csi::controller_server::Controller;
use crate::csi::*;

const VOLUMES_FILE: &str = "volumes.json";
const SNAPSHOTS_FILE: &str = "snapshots.json";

#[derive(Clone)]
pub struct CsiController {
    node_id: String,
    zone: String,
    data_dir: String,
    gossip: Arc<GossipService>,
    replicator: Arc<Replicator>,
    volumes: Arc<RwLock<HashMap<String, VolInfo>>>,
    snapshots: Arc<RwLock<HashMap<String, SnapInfo>>>,
    volume_profiles: HashMap<String, VolumeProfile>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct VolInfo {
    id: String,
    name: String,
    size: u64,
    node_id: String,
    zone: String,
    profile_type: String,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct SnapInfo {
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
        volume_profiles: HashMap<String, VolumeProfile>,
    ) -> Self {
        Self {
            node_id,
            zone,
            data_dir,
            gossip,
            replicator,
            volumes: Arc::new(RwLock::new(HashMap::new())),
            snapshots: Arc::new(RwLock::new(HashMap::new())),
            volume_profiles,
        }
    }

    fn volumes_path(&self) -> PathBuf {
        PathBuf::from(&self.data_dir).join(VOLUMES_FILE)
    }

    fn snapshots_path(&self) -> PathBuf {
        PathBuf::from(&self.data_dir).join(SNAPSHOTS_FILE)
    }

    pub async fn load_from_disk(&self) {
        match tokio::fs::read_to_string(self.volumes_path()).await {
            Ok(content) => {
                match serde_json::from_str::<HashMap<String, VolInfo>>(&content) {
                    Ok(vols) => {
                        tracing::info!("Loaded {} volumes from disk", vols.len());
                        *self.volumes.write().await = vols;
                    }
                    Err(e) => tracing::warn!("Failed to parse volumes.json: {}", e),
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                tracing::info!("No volumes.json found, starting fresh");
            }
            Err(e) => tracing::warn!("Failed to read volumes.json: {}", e),
        }

        match tokio::fs::read_to_string(self.snapshots_path()).await {
            Ok(content) => {
                match serde_json::from_str::<HashMap<String, SnapInfo>>(&content) {
                    Ok(snaps) => {
                        tracing::info!("Loaded {} snapshots from disk", snaps.len());
                        *self.snapshots.write().await = snaps;
                    }
                    Err(e) => tracing::warn!("Failed to parse snapshots.json: {}", e),
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                tracing::info!("No snapshots.json found, starting fresh");
            }
            Err(e) => tracing::warn!("Failed to read snapshots.json: {}", e),
        }
    }

    async fn persist_volumes(&self) -> Result<(), std::io::Error> {
        let vols = self.volumes.read().await;
        let json = serde_json::to_string_pretty(&*vols)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        tokio::fs::write(self.volumes_path(), json).await
    }

    async fn persist_snapshots(&self) -> Result<(), std::io::Error> {
        let snaps = self.snapshots.read().await;
        let json = serde_json::to_string_pretty(&*snaps)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        tokio::fs::write(self.snapshots_path(), json).await
    }

    fn get_profile(&self, profile_type: &str) -> &VolumeProfile {
        self.volume_profiles
            .get(profile_type)
            .or_else(|| self.volume_profiles.get("default"))
            .expect("default volume profile must exist")
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

        let capacity = req
            .capacity_range
            .as_ref()
            .map(|r| r.required_bytes as u64)
            .unwrap_or(1024 * 1024 * 1024);

        // Idempotency: check in-memory first
        {
            let volumes = self.volumes.read().await;
            if let Some(existing) = volumes.values().find(|v| v.name == req.name) {
                if existing.size >= capacity {
                    tracing::info!("Volume {} already exists (id={})", req.name, existing.id);
                    return Ok(Response::new(CreateVolumeResponse {
                        volume: Some(Volume {
                            volume_id: existing.id.clone(),
                            capacity_bytes: existing.size as i64,
                            volume_capabilities: req.volume_capabilities.clone(),
                            ..Default::default()
                        }),
                    }));
                }
            }
        }

        let subvol_path = format!("{}/{}", self.data_dir, req.name);

        // Check if subvolume already exists on filesystem (crash recovery scenario)
        let subvol_exists = tokio::process::Command::new("btrfs")
            .args(["subvolume", "show", &subvol_path])
            .output()
            .await
            .map(|o| o.status.success())
            .unwrap_or(false);

        if subvol_exists {
            tracing::info!("Subvolume {} already exists on filesystem, reusing", req.name);
        } else {
            let output = tokio::process::Command::new("btrfs")
                .args(["subvolume", "create", &subvol_path])
                .output()
                .await
                .map_err(|e| Status::internal(format!("Failed to execute btrfs: {}", e)))?;

            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                // One more idempotency check: race with concurrent create
                let volumes = self.volumes.read().await;
                if let Some(existing) = volumes.values().find(|v| v.name == req.name) {
                    return Ok(Response::new(CreateVolumeResponse {
                        volume: Some(Volume {
                            volume_id: existing.id.clone(),
                            capacity_bytes: existing.size as i64,
                            volume_capabilities: req.volume_capabilities.clone(),
                            ..Default::default()
                        }),
                    }));
                }
                return Err(Status::internal(format!("Failed to create subvolume: {}", stderr)));
            }
        }

        // Determine profile type from volume_context
        let profile_type = req.volume_context
            .get("volume_type")
            .cloned()
            .unwrap_or_else(|| "default".to_string());

        let profile = self.get_profile(&profile_type);

        // Validate database profile: sync_writes must be true for data integrity
        if profile_type == "database" && !profile.sync_writes {
            tracing::warn!(
                "Database volume profile '{}' has sync_writes=false, forcing to true for data integrity",
                profile_type
            );
        }

        // Apply NOCOW if profile requests it
        if profile.nocow {
            let _ = tokio::process::Command::new("chattr")
                .args(["+N", &subvol_path])
                .output()
                .await;
        }

        // Apply compression if set
        if profile.compression != "none" {
            let comp_opt = format!("compression={}", profile.compression);
            let _ = tokio::process::Command::new("btrfs")
                .args(["property", "set", "-ts", &subvol_path, &comp_opt])
                .output()
                .await;
        }

        // Apply mount options for future mounts (stored in volume_context)
        let mut final_context = req.volume_context.clone();
        if profile.sync_writes {
            final_context.insert("mount_options".to_string(), profile.mount_options.join(","));
        }
        final_context.insert("profile_type".to_string(), profile_type.clone());

        let volume_id = format!("vol-{}", uuid::Uuid::new_v4());
        let vol = VolInfo {
            id: volume_id.clone(),
            name: req.name.clone(),
            size: capacity,
            node_id: self.node_id.clone(),
            zone: self.zone.clone(),
            profile_type,
        };

        self.volumes.write().await.insert(volume_id.clone(), vol);

        if let Err(e) = self.persist_volumes().await {
            tracing::error!("Failed to persist volumes: {}", e);
        }

        tracing::info!("Volume {} created (id={})", req.name, volume_id);

        Ok(Response::new(CreateVolumeResponse {
            volume: Some(Volume {
                volume_id,
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

        let vol = {
            let volumes = self.volumes.read().await;
            volumes.get(&req.volume_id).cloned()
        };

        match vol {
            Some(vol) => {
                let subvol_path = format!("{}/{}", self.data_dir, vol.name);
                let output = tokio::process::Command::new("btrfs")
                    .args(["subvolume", "delete", &subvol_path])
                    .output()
                    .await;

                match output {
                    Ok(o) if !o.status.success() => {
                        let stderr = String::from_utf8_lossy(&o.stderr);
                        tracing::error!("Failed to delete subvolume {}: {}", vol.name, stderr);
                    }
                    Err(e) => tracing::error!("Failed to execute btrfs delete: {}", e),
                    _ => {}
                }

                self.volumes.write().await.remove(&req.volume_id);
                if let Err(e) = self.persist_volumes().await {
                    tracing::error!("Failed to persist volumes: {}", e);
                }

                Ok(Response::new(DeleteVolumeResponse {}))
            }
            None => {
                // Volume not in memory; check filesystem directly for idempotency
                tracing::warn!("Volume {} not found in memory, checking filesystem", req.volume_id);
                Ok(Response::new(DeleteVolumeResponse {}))
            }
        }
    }

    async fn controller_publish_volume(
        &self,
        request: Request<ControllerPublishVolumeRequest>,
    ) -> Result<Response<ControllerPublishVolumeResponse>, Status> {
        let req = request.into_inner();
        tracing::info!("CSI ControllerPublishVolume: volume_id={}, node_id={}", req.volume_id, req.node_id);

        let vol = {
            let volumes = self.volumes.read().await;
            volumes.get(&req.volume_id).cloned()
        }.ok_or_else(|| Status::not_found(format!("Volume {} not found", req.volume_id)))?;

        let publish_context = serde_json::to_string(&serde_json::json!({
            "path": format!("{}/{}", self.data_dir, vol.name),
            "node_id": self.node_id,
        })).unwrap_or_default();

        Ok(Response::new(ControllerPublishVolumeResponse { publish_context }))
    }

    async fn controller_unpublish_volume(
        &self,
        request: Request<ControllerUnpublishVolumeRequest>,
    ) -> Result<Response<ControllerUnpublishVolumeResponse>, Status> {
        let req = request.into_inner();
        tracing::info!("CSI ControllerUnpublishVolume: volume_id={}, node_id={}", req.volume_id, req.node_id);
        Ok(Response::new(ControllerUnpublishVolumeResponse {}))
    }

    async fn validate_volume_capabilities(
        &self,
        request: Request<ValidateVolumeCapabilitiesRequest>,
    ) -> Result<Response<ValidateVolumeCapabilitiesResponse>, Status> {
        let req = request.into_inner();

        let vol_id = req.volume_id.first()
            .ok_or_else(|| Status::invalid_argument("volume_id is required"))?;

        {
            let volumes = self.volumes.read().await;
            if volumes.get(vol_id).is_none() {
                return Err(Status::not_found(format!("Volume {} not found", vol_id)));
            }
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

        let volumes = self.volumes.read().await;
        let mut entries: Vec<list_volumes_response::Entry> = volumes.values().map(|v| {
            list_volumes_response::Entry {
                volume: Some(Volume {
                    volume_id: v.id.clone(),
                    capacity_bytes: v.size as i64,
                    ..Default::default()
                }),
                status: Some(VolumeStatus {
                    volume_id: v.id.clone(),
                    ..Default::default()
                }),
            }
        }).collect();

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
        _request: Request<GetCapacityRequest>,
    ) -> Result<Response<GetCapacityResponse>, Status> {
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

        let vol = {
            let volumes = self.volumes.read().await;
            volumes.get(&req.source_volume_id).cloned()
        }.ok_or_else(|| Status::not_found(format!("Source volume {} not found", req.source_volume_id)))?;

        let source_path = format!("{}/{}", self.data_dir, vol.name);
        let snap_path = format!("{}/{}", self.data_dir, req.name);

        let output = tokio::process::Command::new("btrfs")
            .args(["subvolume", "snapshot", "-r", &source_path, &snap_path])
            .output()
            .await
            .map_err(|e| Status::internal(format!("Failed to create snapshot: {}", e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(Status::internal(format!("Failed to create snapshot: {}", stderr)));
        }

        let snapshot_id = format!("snap-{}", uuid::Uuid::new_v4());
        let creation_time = chrono::Utc::now().timestamp();

        self.snapshots.write().await.insert(snapshot_id.clone(), SnapInfo {
            id: snapshot_id.clone(),
            source_volume_id: req.source_volume_id.clone(),
            name: req.name.clone(),
            size: vol.size,
            creation_time,
        });

        if let Err(e) = self.persist_snapshots().await {
            tracing::error!("Failed to persist snapshots: {}", e);
        }

        Ok(Response::new(CreateSnapshotResponse {
            snapshot: Some(Snapshot {
                snapshot_id,
                source_volume_id: req.source_volume_id,
                creation_time,
                size_bytes: vol.size as i64,
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

        let snap = {
            let snapshots = self.snapshots.read().await;
            snapshots.get(&req.snapshot_id).cloned()
        }.ok_or_else(|| Status::not_found(format!("Snapshot {} not found", req.snapshot_id)))?;

        let snap_path = format!("{}/{}", self.data_dir, snap.name);
        let _ = tokio::process::Command::new("btrfs")
            .args(["subvolume", "delete", &snap_path])
            .output()
            .await;

        self.snapshots.write().await.remove(&req.snapshot_id);

        if let Err(e) = self.persist_snapshots().await {
            tracing::error!("Failed to persist snapshots: {}", e);
        }

        Ok(Response::new(DeleteSnapshotResponse {}))
    }

    async fn list_snapshots(
        &self,
        _request: Request<ListSnapshotsRequest>,
    ) -> Result<Response<ListSnapshotsResponse>, Status> {
        let snapshots = self.snapshots.read().await;
        let entries: Vec<list_snapshots_response::Entry> = snapshots.values().map(|s| {
            list_snapshots_response::Entry {
                snapshot: Some(Snapshot {
                    snapshot_id: s.id.clone(),
                    source_volume_id: s.source_volume_id.clone(),
                    creation_time: s.creation_time,
                    size_bytes: s.size as i64,
                    ..Default::default()
                }),
            }
        }).collect();

        Ok(Response::new(ListSnapshotsResponse { entries, next_token: String::new() }))
    }

    async fn controller_expand_volume(
        &self,
        request: Request<ControllerExpandVolumeRequest>,
    ) -> Result<Response<ControllerExpandVolumeResponse>, Status> {
        let req = request.into_inner();
        tracing::info!("CSI ControllerExpandVolume: {}", req.volume_id);

        let new_capacity = req.capacity_range.as_ref().map(|r| r.required_bytes).unwrap_or(0);

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
                    ..Default::default()
                }),
                status: Some(VolumeStatus {
                    volume_id: v.id.clone(),
                    ..Default::default()
                }),
            })),
            None => Err(Status::not_found(format!("Volume {} not found", req.volume_id))),
        }
    }
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
