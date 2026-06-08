use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use tracing::{debug, info};

use super::commands::BtrfsCommand;

/// Information about a btrfs subvolume
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Subvolume {
    pub id: u64,
    pub path: String,
    pub name: String,
    pub size: u64,
    pub uuid: String,
    pub parent_id: Option<u64>,
    pub created_at: Option<String>,
}

/// Manager for btrfs subvolume operations
pub struct SubvolumeManager {
    base_path: String,
}

impl SubvolumeManager {
    /// Create a new subvolume manager
    pub fn new(base_path: &str) -> Self {
        Self {
            base_path: base_path.to_string(),
        }
    }

    /// Create a new subvolume
    pub async fn create(&self, name: &str) -> Result<Subvolume> {
        let path = format!("{}/{}", self.base_path, name);

        info!("Creating subvolume: {}", path);

        BtrfsCommand::run("subvolume", &["create", &path])
            .await
            .context("Failed to create subvolume")?;

        // Get subvolume info
        self.get_info(name).await
    }

    /// Delete a subvolume
    pub async fn delete(&self, name: &str) -> Result<()> {
        let path = format!("{}/{}", self.base_path, name);

        info!("Deleting subvolume: {}", path);

        BtrfsCommand::run("subvolume", &["delete", &path])
            .await
            .context("Failed to delete subvolume")
    }

    /// Get subvolume information
    pub async fn get_info(&self, name: &str) -> Result<Subvolume> {
        let path = format!("{}/{}", self.base_path, name);

        let output = BtrfsCommand::run("subvolume", &["show", &path]).await?;

        // Parse output (simplified)
        let mut id = 0;
        let mut uuid = String::new();
        let mut size = 0;

        for line in output.lines() {
            if line.contains("Subvolume ID:") {
                id = line
                    .split(':')
                    .nth(1)
                    .unwrap_or("0")
                    .trim()
                    .parse()
                    .unwrap_or(0);
            }
            if line.contains("UUID:") {
                uuid = line.split(':').nth(1).unwrap_or("").trim().to_string();
            }
            if line.contains("Size:") {
                // Parse size (handle GiB, MiB, etc.)
                size = parse_size(line);
            }
        }

        Ok(Subvolume {
            id,
            path: path.clone(),
            name: name.to_string(),
            size,
            uuid,
            parent_id: None,
            created_at: None,
        })
    }

    /// List all subvolumes
    pub async fn list(&self) -> Result<Vec<Subvolume>> {
        let output = BtrfsCommand::run("subvolume", &["list", &self.base_path]).await?;

        let mut subvolumes = Vec::new();

        for line in output.lines() {
            // Parse: ID <id> gen <gen> top level <parent> path <path>
            if let Some(subvol) = parse_subvolume_line(line) {
                subvolumes.push(subvol);
            }
        }

        Ok(subvolumes)
    }

    /// Check if a subvolume exists
    pub async fn exists(&self, name: &str) -> bool {
        let path = format!("{}/{}", self.base_path, name);
        tokio::fs::metadata(&path).await.is_ok()
    }

    /// Get subvolume size
    pub async fn get_size(&self, name: &str) -> Result<u64> {
        let subvol = self.get_info(name).await?;
        Ok(subvol.size)
    }

    /// Set NOCOW attribute for database volumes
    pub async fn set_nocow(&self, name: &str) -> Result<()> {
        let path = format!("{}/{}", self.base_path, name);

        info!("Setting NOCOW for: {}", path);

        // Use chattr to set NOCOW
        tokio::process::Command::new("chattr")
            .args(&["+C", &path])
            .output()
            .await
            .context("Failed to set NOCOW attribute")?;

        // Also set via btrfs property
        BtrfsCommand::run("property", &["set", &path, "nocow", "true"])
            .await
            .context("Failed to set btrfs nocow property")?;

        Ok(())
    }
}

/// Parse size string (e.g., "10.00GiB") to bytes
fn parse_size(s: &str) -> u64 {
    let parts: Vec<&str> = s.split_whitespace().collect();
    if parts.len() < 2 {
        return 0;
    }

    let value: f64 = parts[0].parse().unwrap_or(0.0);
    let unit = parts[1].to_lowercase();

    match unit.as_str() {
        "b" | "bytes" => value as u64,
        "kb" | "kib" => (value * 1024.0) as u64,
        "mb" | "mib" => (value * 1024.0 * 1024.0) as u64,
        "gb" | "gib" => (value * 1024.0 * 1024.0 * 1024.0) as u64,
        "tb" | "tib" => (value * 1024.0 * 1024.0 * 1024.0 * 1024.0) as u64,
        _ => 0,
    }
}

/// Parse subvolume list line
fn parse_subvolume_line(line: &str) -> Option<Subvolume> {
    // Format: ID <id> gen <gen> top level <parent> path <path>
    let parts: Vec<&str> = line.split_whitespace().collect();

    if parts.len() < 7 || parts[0] != "ID" {
        return None;
    }

    let id: u64 = parts[1].parse().ok()?;
    let path = parts[6..].join(" ");

    Some(Subvolume {
        id,
        path: path.clone(),
        name: path.split('/').last().unwrap_or("").to_string(),
        size: 0,
        uuid: String::new(),
        parent_id: Some(5),
        created_at: None,
    })
}
