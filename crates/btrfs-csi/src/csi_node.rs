use std::collections::HashMap;
use std::path::Path;
use tonic::{Request, Response, Status};

use crate::csi::node_server::Node;
use crate::csi::*;

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

        let output = tokio::process::Command::new("mount")
            .args(["--bind", &volume_path, req.staging_target_path.as_str()])
            .output()
            .await
            .map_err(|e| Status::internal(format!("Failed to bind mount: {}", e)))?;

        if !output.status.success() {
            tracing::warn!("Bind mount: {}", String::from_utf8_lossy(&output.stderr));
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

        let output = tokio::process::Command::new("mount")
            .args(["--bind", &volume_path, req.target_path.as_str()])
            .output()
            .await
            .map_err(|e| Status::internal(format!("Failed to bind mount: {}", e)))?;

        if !output.status.success() {
            tracing::warn!("Bind mount: {}", String::from_utf8_lossy(&output.stderr));
        }

        Ok(Response::new(NodePublishVolumeResponse {}))
    }

    async fn node_unpublish_volume(
        &self,
        request: Request<NodeUnpublishVolumeRequest>,
    ) -> Result<Response<NodeUnpublishVolumeResponse>, Status> {
        let req = request.into_inner();
        tracing::info!("CSI NodeUnpublishVolume: {}", req.volume_id);

        let _ = tokio::process::Command::new("umount")
            .arg(&req.target_path)
            .output()
            .await;

        let _ = tokio::fs::remove_dir_all(&req.target_path).await;

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

        Ok(Response::new(NodeGetVolumeStatsResponse {
            usage: vec![node_get_volume_stats_response::VolumeUsage {
                available: (total - used) as i64,
                total: total as i64,
                used: used as i64,
                r#unit: node_get_volume_stats_response::volume_usage::Unit::Bytes as i32,
            }],
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

        Ok(Response::new(NodeExpandVolumeResponse { capacity_bytes: new_capacity }))
    }

    async fn node_get_capabilities(
        &self,
        _request: Request<NodeGetCapabilitiesRequest>,
    ) -> Result<Response<NodeGetCapabilitiesResponse>, Status> {
        use crate::csi::node_get_capabilities_response::node_capability;

        Ok(Response::new(NodeGetCapabilitiesResponse {
            capabilities: vec![node_capability::NodeCapability {
                r#type: Some(node_capability::Type::Service(node_capability::Service {
                    r#type: node_capability::service::Type::StageUnstageVolume as i32,
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
    data_dir.to_string()
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
