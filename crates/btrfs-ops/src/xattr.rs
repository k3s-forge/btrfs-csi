use anyhow::{Context, Result};
use std::collections::HashMap;

const CSI_XATTR_PREFIX: &str = "user.csi.";

/// Epoch/Quorum xattr key names
pub const XATTR_EPOCH: &str = "epoch";
pub const XATTR_VECTOR_CLOCK: &str = "vector_clock";
pub const XATTR_LAST_SYNCED_FROM: &str = "last_synced_from";
pub const XATTR_VOLUME_STATUS: &str = "volume_status";
pub const XATTR_REPLICA_COUNT: &str = "replica_count";

/// Volume status values
pub const VOLUME_STATUS_ACTIVE: &str = "active";
pub const VOLUME_STATUS_READONLY: &str = "readonly";
pub const VOLUME_STATUS_CONFLICT: &str = "conflict";

/// Get epoch for a subvolume, returning 0 if not set
pub async fn get_epoch(path: &str) -> u64 {
    get_csi_attr(path, XATTR_EPOCH).await
        .ok().flatten()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(0)
}

/// Set epoch on a subvolume
pub async fn set_epoch(path: &str, epoch: u64) -> Result<()> {
    set_csi_attr(path, XATTR_EPOCH, &epoch.to_string()).await
}

/// Increment epoch atomically (read + 1 + write)
pub async fn increment_epoch(path: &str) -> Result<u64> {
    let current = get_epoch(path).await;
    let next = current + 1;
    set_epoch(path, next).await?;
    Ok(next)
}

/// Parse vector clock from "node1:3,node2:7" format into HashMap
pub fn parse_vector_clock(val: &str) -> HashMap<String, u64> {
    let mut clock = HashMap::new();
    for pair in val.split(',') {
        let pair = pair.trim();
        if let Some((node, epoch)) = pair.split_once(':') {
            if let Ok(e) = epoch.trim().parse::<u64>() {
                clock.insert(node.trim().to_string(), e);
            }
        }
    }
    clock
}

/// Serialize vector clock HashMap into "node1:3,node2:7" format
pub fn format_vector_clock(clock: &HashMap<String, u64>) -> String {
    let mut pairs: Vec<String> = clock.iter()
        .map(|(k, v)| format!("{}:{}", k, v))
        .collect();
    pairs.sort();
    pairs.join(",")
}

/// Get vector clock for a subvolume
pub async fn get_vector_clock(path: &str) -> HashMap<String, u64> {
    get_csi_attr(path, XATTR_VECTOR_CLOCK).await
        .ok().flatten()
        .map(|v| parse_vector_clock(&v))
        .unwrap_or_default()
}

/// Set vector clock on a subvolume
pub async fn set_vector_clock(path: &str, clock: &HashMap<String, u64>) -> Result<()> {
    set_csi_attr(path, XATTR_VECTOR_CLOCK, &format_vector_clock(clock)).await
}

/// Merge two vector clocks (take max per-node entry)
pub fn merge_vector_clocks(
    local: &HashMap<String, u64>,
    remote: &HashMap<String, u64>,
) -> HashMap<String, u64> {
    let mut merged = local.clone();
    for (node, epoch) in remote {
        let entry = merged.entry(node.clone()).or_insert(0);
        *entry = (*entry).max(*epoch);
    }
    merged
}

/// Detect if two vector clocks are concurrent (true conflict).
///
/// Two vector clocks conflict if neither one happens-before the other:
///   !(local <= remote) && !(remote <= local)
///
/// Where A <= B iff for every node k, A[k] <= B[k].
pub fn has_conflict(
    local: &HashMap<String, u64>,
    remote: &HashMap<String, u64>,
) -> bool {
    if local.is_empty() || remote.is_empty() {
        return false;
    }
    let local_le_remote = vector_clock_le(local, remote);
    let remote_le_local = vector_clock_le(remote, local);
    !local_le_remote && !remote_le_local
}

/// Check if vector clock a happens-before or equals vector clock b.
/// Returns true if a[k] <= b[k] for every node k present in a.
fn vector_clock_le(a: &HashMap<String, u64>, b: &HashMap<String, u64>) -> bool {
    for (node, epoch_a) in a {
        let epoch_b = b.get(node).copied().unwrap_or(0);
        if *epoch_a > epoch_b {
            return false;
        }
    }
    true
}

/// Get volume status from xattr
pub async fn get_volume_status(path: &str) -> String {
    get_csi_attr(path, XATTR_VOLUME_STATUS).await
        .ok().flatten()
        .unwrap_or_else(|| VOLUME_STATUS_ACTIVE.to_string())
}

/// Set volume status on a subvolume
pub async fn set_volume_status(path: &str, status: &str) -> Result<()> {
    set_csi_attr(path, XATTR_VOLUME_STATUS, status).await
}

/// Set a CSI xattr on a btrfs subvolume path
pub async fn set_csi_attr(path: &str, key: &str, value: &str) -> Result<()> {
    let full_key = format!("{}{}", CSI_XATTR_PREFIX, key);
    tokio::process::Command::new("setfattr")
        .args(["-n", &full_key, "-v", value, "--", path])
        .output()
        .await
        .context("failed to execute setfattr")?;
    Ok(())
}

/// Get a CSI xattr value from a subvolume path
pub async fn get_csi_attr(path: &str, key: &str) -> Result<Option<String>> {
    let full_key = format!("{}{}", CSI_XATTR_PREFIX, key);
    let output = tokio::process::Command::new("getfattr")
        .args(["-n", &full_key, "--only-values", "--", path])
        .output()
        .await
        .context("failed to execute getfattr")?;

    if output.status.success() {
        let value = String::from_utf8_lossy(&output.stdout).trim().to_string();
        Ok(Some(value))
    } else {
        // ENODATA (no such attribute) or file not found
        Ok(None)
    }
}

/// Remove a CSI xattr from a subvolume path
pub async fn remove_csi_attr(path: &str, key: &str) -> Result<()> {
    let full_key = format!("{}{}", CSI_XATTR_PREFIX, key);
    let _ = tokio::process::Command::new("setfattr")
        .args(["-x", &full_key, "--", path])
        .output()
        .await;
    Ok(())
}

/// Get all CSI xattrs from a subvolume path
pub async fn get_all_csi_attrs(path: &str) -> Result<HashMap<String, String>> {
    let output = tokio::process::Command::new("getfattr")
        .args(["-d", "--", path])
        .output()
        .await
        .context("failed to execute getfattr")?;

    let mut attrs = HashMap::new();
    if !output.status.success() {
        return Ok(attrs);
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    for line in stdout.lines() {
        // Skip the "# file: ..." header line
        if line.starts_with('#') {
            continue;
        }
        // getfattr -d outputs: user.csi.key="value"
        // Use split_once with '=' but handle value quoting correctly
        if let Some(eq_pos) = line.find('=') {
            let key_part = &line[..eq_pos];
            let val_part = &line[eq_pos + 1..];
            let k = key_part.trim().strip_prefix(CSI_XATTR_PREFIX).unwrap_or(key_part.trim());
            // Trim surrounding quotes and any trailing content
            let v = val_part.trim().trim_matches('"').to_string();
            attrs.insert(k.to_string(), v);
        }
    }
    Ok(attrs)
}

/// Parse u64 from xattr value safely
pub fn parse_u64_attr(val: Option<&String>) -> u64 {
    val.and_then(|v| v.parse::<u64>().ok()).unwrap_or(0)
}

/// Parse i64 from xattr value safely
pub fn parse_i64_attr(val: Option<&String>) -> i64 {
    val.and_then(|v| v.parse::<i64>().ok()).unwrap_or(0)
}

/// Get replica count for a volume (0 = no replication)
pub async fn get_replica_count(path: &str) -> u32 {
    get_csi_attr(path, XATTR_REPLICA_COUNT).await
        .ok().flatten()
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(0)
}

/// Set replica count on a subvolume
pub async fn set_replica_count(path: &str, count: u32) -> Result<()> {
    set_csi_attr(path, XATTR_REPLICA_COUNT, &count.to_string()).await
}