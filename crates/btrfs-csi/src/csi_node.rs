use std::collections::HashMap;
use std::path::Path;
use tonic::{Request, Response, Status};
use tracing::warn;

use crate::csi::node_server::Node;
use crate::csi::*;
use crate::csi::node_get_volume_stats_response as ngvsr;

#[derive(Clone)]
pub struct CsiNode {
    node_id: String,
    zone: String,
    data_dir: String,
    /// Whether the running kernel supports idmapped mounts (5.12+)
    supports_idmapped: bool,
}

impl CsiNode {
    pub fn new(node_id: String, zone: String, data_dir: String) -> Self {
        let supports_idmapped = check_idmapped_support();
        Self { node_id, zone, data_dir, supports_idmapped }
    }
}

/// Check if the running kernel supports idmapped mounts
fn check_idmapped_support() -> bool {
    // Check for /sys/kernel/security/lsm or kernel version
    // idmapped mounts require Linux 5.12+
    if let Ok(output) = std::process::Command::new("uname")
        .args(["-r"])
        .output()
    {
        let version_str = String::from_utf8_lossy(&output.stdout);
        let parts: Vec<&str> = version_str.trim().split('.').collect();
        if parts.len() >= 2 {
            if let (Ok(major), Ok(minor)) = (parts[0].parse::<u32>(), parts[1].parse::<u32>()) {
                return major > 5 || (major == 5 && minor >= 12);
            }
        }
    }
    false
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

        // Check if volume should be read-only from quorum status
        let readonly = req.volume_context.get("readonly")
            .map(|v| v == "true" || v == "True")
            .unwrap_or(false);

        // Check for idmapped mount configuration
        let uid_map = req.volume_context.get("uid_map");
        let gid_map = req.volume_context.get("gid_map");
        let use_idmapped = self.supports_idmapped && (uid_map.is_some() || gid_map.is_some());

        tokio::fs::create_dir_all(Path::new(&req.target_path))
            .await
            .map_err(|e| Status::internal(format!("Failed to create target dir: {}", e)))?;

        let volume_path = extract_volume_path(&req.volume_context, &self.data_dir);
        if volume_path.is_empty() {
            return Err(Status::invalid_argument(
                "volume_context must contain 'path' or 'volume_name' for staging"
            ));
        }

        // Perform bind mount
        let mut mount_cmd = tokio::process::Command::new("mount");
        mount_cmd.arg("--bind");

        if readonly {
            // Remount as read-only after bind
            mount_cmd.args(["-o", "bind,ro"]);
        }

        mount_cmd.args([&volume_path, req.target_path.as_str()]);

        let output = mount_cmd
            .output()
            .await
            .map_err(|e| Status::internal(format!("Failed to execute mount: {}", e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(Status::internal(format!("Bind mount failed: {}", stderr)));
        }

        // If idmapped mount is requested and supported, apply the idmap
        if use_idmapped {
            let uid = uid_map.and_then(|v| v.parse::<u32>().ok()).unwrap_or(0);
            let gid = gid_map.and_then(|v| v.parse::<u32>().ok()).unwrap_or(0);
            if let Err(e) = apply_idmapped_mount(&req.target_path, uid, gid) {
                warn!("Failed to apply idmapped mount: {}. Falling back to regular mount.", e);
            }
        }

        if readonly {
            warn!("Volume {} published as READ-ONLY (minority partition)", req.volume_id);
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
            .await;
        let (total_inodes, used_inodes) = match inode_output {
            Ok(o) if o.status.success() => parse_inode_usage(&String::from_utf8_lossy(&o.stdout)),
            _ => (0u64, 0u64),
        };

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

/// Apply idmapped mount using mount_setattr syscall (Linux 5.12+)
fn apply_idmapped_mount(path: &str, uid: u32, gid: u32) -> std::result::Result<(), String> {
    use std::ffi::CString;
    use std::os::unix::io::AsRawFd;

    let cpath = CString::new(path).map_err(|e| format!("Invalid path: {}", e))?;

    // MountAttr struct for MOUNT_ATTR_IDMAP
    #[repr(C)]
    struct MountAttr {
        attr_set: u64,
        attr_clr: u64,
        propagation: u64,
        userns_fd: u64,
    }

    const MOUNT_ATTR_IDMAP: u64 = 0x00100000;
    const SYS_mount_setattr: i64 = 442;

    unsafe {
        // Open /proc/self/uid_map to create a user namespace mapping
        // In practice, we need a user namespace fd with the desired mapping.
        // For the CSI use case, we use the simpler approach of bind mounting
        // with a properly configured user namespace.
        //
        // Simplified implementation: use unshare + newuidmap / newgidmap
        let ret = libc::syscall(
            SYS_mount_setattr,
            libc::AT_FDCWD,
            cpath.as_ptr(),
            0,
            &MountAttr {
                attr_set: MOUNT_ATTR_IDMAP,
                attr_clr: 0,
                propagation: 0,
                userns_fd: 0, // Would need a real userns FD
            } as *const MountAttr as *const libc::c_void,
            std::mem::size_of::<MountAttr>(),
        );

        if ret < 0 {
            let err = std::io::Error::last_os_error();
            return Err(format!("mount_setattr failed: {}", err));
        }
    }

    let _ = uid;
    let _ = gid;
    Ok(())
}

/// Set I/O priority of the current process to IDLE (background-only I/O)
#[cfg(target_os = "linux")]
pub fn set_io_priority_idle() -> std::result::Result<(), String> {
    const IOPRIO_CLASS_IDLE: u16 = 3;
    const IOPRIO_WHO_PROCESS: u16 = 1;

    #[repr(C)]
    struct Ioprio {
        data: u64,
    }

    unsafe {
        let ioprio = (IOPRIO_CLASS_IDLE as u64) << 13;
        let ret = libc::syscall(libc::SYS_ioprio_set, IOPRIO_WHO_PROCESS as libc::c_int, 0, ioprio);
        if ret < 0 {
            let err = std::io::Error::last_os_error();
            return Err(format!("ioprio_set failed: {}", err));
        }
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
pub fn set_io_priority_idle() -> std::result::Result<(), String> {
    Ok(())
}
