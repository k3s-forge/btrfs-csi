use anyhow::{Context, Result};
use std::collections::HashMap;

const CSI_XATTR_PREFIX: &str = "user.csi.";

/// Set a CSI xattr on a btrfs subvolume path
pub async fn set_csi_attr(path: &str, key: &str, value: &str) -> Result<()> {
    let full_key = format!("{}{}", CSI_XATTR_PREFIX, key);
    tokio::process::Command::new("setfattr")
        .args(["-n", &full_key, "-v", value, path])
        .output()
        .await
        .context("failed to execute setfattr")?;
    Ok(())
}

/// Get a CSI xattr value from a subvolume path
pub async fn get_csi_attr(path: &str, key: &str) -> Result<Option<String>> {
    let full_key = format!("{}{}", CSI_XATTR_PREFIX, key);
    let output = tokio::process::Command::new("getfattr")
        .args(["-n", &full_key, "--only-values", path])
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
        .args(["-x", &full_key, path])
        .output()
        .await;
    Ok(())
}

/// Get all CSI xattrs from a subvolume path
pub async fn get_all_csi_attrs(path: &str) -> Result<HashMap<String, String>> {
    let output = tokio::process::Command::new("getfattr")
        .args(["-d", "--only-values", path])
        .output()
        .await
        .context("failed to execute getfattr")?;

    let mut attrs = HashMap::new();
    if !output.status.success() {
        return Ok(attrs);
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    for line in stdout.lines() {
        if let Some((key, value)) = line.split_once('=') {
            let k = key.trim().strip_prefix(CSI_XATTR_PREFIX).unwrap_or(key.trim());
            attrs.insert(k.to_string(), value.trim().to_string());
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