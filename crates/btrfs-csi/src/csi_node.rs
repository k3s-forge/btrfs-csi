use std::collections::HashMap;
use std::path::Path;
use tonic::{Request, Response, Status};

use crate::csi::node_server::Node;
use crate::csi::*;
use crate::csi::node_get_volume_stats_response as ngvsr;

#[derive(Clone)]
pub struct CsiNode {
    node_id: String,
    zone: String,
    data_dir: String,
}

impl CsiNode {
    pub fn new(node_id: String, zone: String, data_dir: String) -> Self {
        Self { node_id, zone, data_dir }
    }
}

#[tonic::async_trait]
impl Node for CsiNode {
    async fn node_stage_volume(
        &self,
        request: Request<NodeStageVolumeRequest>,
    ) -> Result<Response<NodeStageVolumeResponse>, Status> {
        let req = request.into_inner();
        tracing::info!("CSI NodeStageVolume: volume_id={}, staging_path={}", req.volume_id, req.staging_target_path);

        tokio::fs::create_dir_all(Path::new(&req.staging_target_path))
            .await
            .map_err(|e| Status::internal(format!("Failed to create staging dir: {}", e)))?;

        let volume_path = extract_volume_path(&req.volume_context, &self.data_dir);
        if volume_path.is_empty() {
            return Err(Status::invalid_argument(
                "volume_context must contain 'path' or 'volume_name' for staging"
            ));
        }

        let output = tokio::process::Command::new("mount")
            .args(["--bind", &volume_path, req.staging_target_path.as_str()])
            .output()
            .await
            .map_err(|e| Status::internal(format!("Failed to bind mount: {}", e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(Status::internal(format!("Bind mount failed: {}", stderr)));
        }

        Ok(Response::new(NodeStageVolumeResponse {}))
    }

    async fn node_unstage_volume(
        &self,
        request: Request<NodeUnstageVolumeRequest>,
    ) -> Result<Response<NodeUnstageVolumeResponse>, Status> {
        let req = request.into_inner();
        tracing::info!("CSI NodeUnstageVolume: {}", req.volume_id);

        let _ = tokio::process::Command::new("umount")
            .arg(&req.staging_target_path)
            .output()
            .await;

        let _ = tokio::fs::remove_dir_all(&req.staging_target_path).await;

        Ok(Response::new(NodeUnstageVolumeResponse {}))
    }

    async fn node_publish_volume(
        &self,
        request: Request<NodePublishVolumeRequest>,
    ) -> Result<Response<NodePublishVolumeResponse>, Status> {
        let req = request.into_inner();
        tracing::info!("CSI NodePublishVolume: volume_id={}, target_path={}", req.volume_id, req.target_path);

        tokio::fs::create_dir_all(Path::new(&req.target_path))
            .await
            .map_err(|e| Status::internal(format!("Failed to create target dir: {}", e)))?;

        let volume_path = extract_volume_path(&req.volume_context, &self.data_dir);
        if volume_path.is_empty() {
            return Err(Status::invalid_argument(
                "volume_context must contain 'path' or 'volume_name' for staging"
            ));
        }

        let output = tokio::process::Command::new("mount")
            .args(["--bind", &volume_path, req.target_path.as_str()])
            .output()
            .await
            .map_err(|e| Status::internal(format!("Failed to bind mount: {}", e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(Status::internal(format!("Bind mount failed: {}", stderr)));
        }

        Ok(Response::new(NodePublishVolumeResponse {}))
    }

    async fn node_unpublish_volume(
        &self,
        request: Request<NodeUnpublishVolumeRequest>,
    ) -> Result<Response<NodeUnpublishVolumeResponse>, Status> {
        let req = request.into_inner();
        tracing::info!("CSI NodeUnpublishVolume: {}", req.volume_id);

        match tokio::process::Command::new("umount")
            .arg(&req.target_path)
            .output()
            .await
        {
            Ok(o) if !o.status.success() => {
                let stderr = String::from_utf8_lossy(&o.stderr);
                tracing::warn!("umount {} failed: {}", req.target_path, stderr);
            }
            Err(e) => tracing::warn!("Failed to execute umount: {}", e),
            _ => {}
        }

        if let Err(e) = tokio::fs::remove_dir_all(&req.target_path).await {
            tracing::warn!("Failed to remove target dir {}: {}", req.target_path, e);
        }

        Ok(Response::new(NodeUnpublishVolumeResponse {}))
    }

    async fn node_get_volume_stats(
        &self,
        request: Request<NodeGetVolumeStatsRequest>,
    ) -> Result<Response<NodeGetVolumeStatsResponse>, Status> {
        let req = request.into_inner();
        tracing::debug!("CSI NodeGetVolumeStats: volume_id={}", req.volume_id);

        let output = tokio::process::Command::new("btrfs")
            .args(["filesystem", "usage", "-b", &self.data_dir])
            .output()
            .await
            .map_err(|e| Status::internal(format!("Failed to get stats: {}", e)))?;

        let stdout = String::from_utf8_lossy(&output.stdout);
        let (total, used) = parse_btrfs_usage(&stdout);

        // Also query inode usage via df -i
        let inode_output = tokio::process::Command::new("df")
            .args(["-i", &self.data_dir])
            .output()
            .await
            .unwrap_or_else(|_| std::process::Output::default());
        let (total_inodes, used_inodes) = parse_inode_usage(&String::from_utf8_lossy(&inode_output.stdout));

        Ok(Response::new(NodeGetVolumeStatsResponse {
            usage: Some(ngvsr::VolumeUsage {
                available: (total - used) as i64,
                total: total as i64,
                used: used as i64,
                unit: ngvsr::volume_usage::Unit::Bytes.into(),
            }),
            volume_attributes: Some(ngvsr::VolumeAttributes {
                attributes: [
                    ("inode_total".to_string(), total_inodes.to_string()),
                    ("inode_used".to_string(), used_inodes.to_string()),
                    ("inode_available".to_string(), (total_inodes.saturating_sub(used_inodes)).to_string()),
                ].into_iter().collect(),
            }),
            ..Default::default()
        }))
    }

    async fn node_expand_volume(
        &self,
        request: Request<NodeExpandVolumeRequest>,
    ) -> Result<Response<NodeExpandVolumeResponse>, Status> {
        let req = request.into_inner();
        tracing::info!("CSI NodeExpandVolume: {}", req.volume_id);

        let new_capacity = req.capacity_range.as_ref().map(|r| r.required_bytes).unwrap_or(0);

        // Resize btrfs filesystem to max
        let output = tokio::process::Command::new("btrfs")
            .args(["filesystem", "resize", "max", &self.data_dir])
            .output()
            .await;

        match output {
            Ok(o) if o.status.success() => {
                tracing::info!("Resized filesystem to max on {}", self.data_dir);
            }
            Ok(o) => {
                let stderr = String::from_utf8_lossy(&o.stderr);
                tracing::warn!("btrfs resize failed: {}", stderr);
            }
            Err(e) => tracing::warn!("Failed to execute btrfs resize: {}", e),
        }

        Ok(Response::new(NodeExpandVolumeResponse { capacity_bytes: new_capacity }))
    }

    async fn node_get_capabilities(
        &self,
        _request: Request<NodeGetCapabilitiesRequest>,
    ) -> Result<Response<NodeGetCapabilitiesResponse>, Status> {
        use crate::csi::node_get_capabilities_response as ngcr;

        Ok(Response::new(NodeGetCapabilitiesResponse {
            capabilities: vec![ngcr::NodeCapability {
                r#type: Some(ngcr::node_capability::Type::Service(ngcr::Service {
                    r#type: ngcr::service::Type::StageUnstageVolume.into(),
                })),
            }],
        }))
    }

    async fn node_get_info(
        &self,
        _request: Request<NodeGetInfoRequest>,
    ) -> Result<Response<NodeGetInfoResponse>, Status> {
        let mut segments = HashMap::new();
        segments.insert("topology.btrfs-csi/zone".to_string(), self.zone.clone());

        Ok(Response::new(NodeGetInfoResponse {
            node_id: self.node_id.clone(),
            accessible_topology: Some(Topology { segments }),
        }))
    }
}

fn extract_volume_path(volume_context: &HashMap<String, String>, data_dir: &str) -> String {
    if let Some(path) = volume_context.get("path") {
        return path.clone();
    }
    if let Some(name) = volume_context.get("volume_name") {
        return format!("{}/{}", data_dir, name);
    }
    String::new()
}

fn parse_btrfs_usage(output: &str) -> (u64, u64) {
    let mut total = 0u64;
    let mut used = 0u64;
    for line in output.lines() {
        if line.contains("Device allocated:") || line.contains("Used:") {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if let Some(s) = parts.get(1) {
                if let Ok(val) = s.parse::<u64>() {
                    if line.contains("Device allocated") { total = val; }
                    else { used = val; }
                }
            }
        }
    }
    (total, used)
}

fn parse_inode_usage(output: &str) -> (u64, u64) {
    // Parse df -i output: Filesystem Inodes IUsed IFree IUse% MountedOn
    for line in output.lines().skip(1) {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() >= 4 {
            if let (Ok(total), Ok(used)) = (parts[1].parse::<u64>(), parts[2].parse::<u64>()) {
                return (total, used);
            }
        }
    }
    (0, 0)
}
