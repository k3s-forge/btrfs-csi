use anyhow::{Context, Result};
use chrono::{DateTime, Datelike, Utc};
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use super::commands::BtrfsCommand;

/// Information about a btrfs snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    pub id: u64,
    pub name: String,
    pub path: String,
    pub subvolume_id: u64,
    pub created_at: DateTime<Utc>,
    pub size: u64,
}

/// Manager for btrfs snapshot operations
pub struct SnapshotManager {
    base_path: String,
}

impl SnapshotManager {
    /// Create a new snapshot manager
    pub fn new(base_path: &str) -> Self {
        Self {
            base_path: base_path.to_string(),
        }
    }

    /// Create a read-only snapshot
    pub async fn create(&self, subvolume_name: &str, snapshot_name: &str) -> Result<Snapshot> {
        let subvol_path = format!("{}/{}", self.base_path, subvolume_name);
        let snap_path = format!("{}/{}", self.base_path, snapshot_name);

        info!("Creating snapshot: {} -> {}", subvol_path, snap_path);

        BtrfsCommand::run(
            "subvolume",
            &["snapshot", "-r", &subvol_path, &snap_path],
        )
        .await
        .context("Failed to create snapshot")?;

        // Get snapshot info
        self.get_info(snapshot_name).await
    }

    /// Create a writable snapshot
    pub async fn create_writable(
        &self,
        subvolume_name: &str,
        snapshot_name: &str,
    ) -> Result<Snapshot> {
        let subvol_path = format!("{}/{}", self.base_path, subvolume_name);
        let snap_path = format!("{}/{}", self.base_path, snapshot_name);

        info!(
            "Creating writable snapshot: {} -> {}",
            subvol_path, snap_path
        );

        BtrfsCommand::run("subvolume", &["snapshot", &subvol_path, &snap_path])
            .await
            .context("Failed to create writable snapshot")?;

        self.get_info(snapshot_name).await
    }

    /// Delete a snapshot
    pub async fn delete(&self, snapshot_name: &str) -> Result<()> {
        let snap_path = format!("{}/{}", self.base_path, snapshot_name);

        info!("Deleting snapshot: {}", snap_path);

        BtrfsCommand::run("subvolume", &["delete", &snap_path])
            .await
            .context("Failed to delete snapshot")?;
        Ok(())
    }

    /// Get snapshot information
    pub async fn get_info(&self, snapshot_name: &str) -> Result<Snapshot> {
        let snap_path = format!("{}/{}", self.base_path, snapshot_name);

        let output = BtrfsCommand::run("subvolume", &["show", &snap_path]).await?;

        // Parse output
        let mut id = 0;
        let mut subvolume_id = 0;
        let mut created_at = Utc::now();

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
            if line.contains("Parent ID:") {
                subvolume_id = line
                    .split(':')
                    .nth(1)
                    .unwrap_or("0")
                    .trim()
                    .parse()
                    .unwrap_or(0);
            }
            if line.contains("Creation time:") {
                // Parse creation time
                if let Some(time_str) = line.split(':').nth(1) {
                    created_at = DateTime::parse_from_str(time_str.trim(), "%Y-%m-%d %H:%M:%S %z")
                        .map(|dt| dt.with_timezone(&Utc))
                        .unwrap_or_else(|_| Utc::now());
                }
            }
        }

        Ok(Snapshot {
            id,
            name: snapshot_name.to_string(),
            path: snap_path,
            subvolume_id,
            created_at,
            size: 0,
        })
    }

    /// List all snapshots
    pub async fn list(&self) -> Result<Vec<Snapshot>> {
        let output = BtrfsCommand::run("subvolume", &["list", "-s", &self.base_path]).await?;

        let mut snapshots = Vec::new();

        for line in output.lines() {
            if let Some(snap) = parse_snapshot_line(line) {
                snapshots.push(snap);
            }
        }

        Ok(snapshots)
    }

    /// Delete old snapshots based on retention policy
    pub async fn cleanup(
        &self,
        retain_daily: u32,
        retain_weekly: u32,
        retain_monthly: u32,
    ) -> Result<u32> {
        let snapshots = self.list().await?;
        let mut deleted = 0;

        // Sort by creation time (oldest first)
        let mut sorted_snapshots = snapshots;
        sorted_snapshots.sort_by(|a, b| a.created_at.cmp(&b.created_at));

        let now = Utc::now();

        for snap in &sorted_snapshots {
            let age = now - snap.created_at;

            // Keep recent snapshots
            if age.num_days() < retain_daily as i64 {
                continue;
            }

            // Keep weekly snapshots
            if age.num_days() < retain_weekly as i64 * 7
                && snap.created_at.weekday() == chrono::Weekday::Sun
            {
                continue;
            }

            // Keep monthly snapshots
            if age.num_days() < retain_monthly as i64 * 30 && snap.created_at.day() == 1 {
                continue;
            }

            // Delete this snapshot
            warn!("Deleting old snapshot: {}", snap.name);
            self.delete(&snap.name).await?;
            deleted += 1;
        }

        Ok(deleted)
    }

    /// Create incremental snapshot for replication
    pub async fn create_for_replication(
        &self,
        subvolume_name: &str,
    ) -> Result<(Snapshot, Option<Snapshot>)> {
        let timestamp = Utc::now().format("%Y%m%d_%H%M%S");
        let snap_name = format!("{}_snap_{}", subvolume_name, timestamp);

        // Create new snapshot
        let snap = self.create(subvolume_name, &snap_name).await?;

        // Find previous snapshot for incremental send
        let snapshots = self.list().await?;
        let previous = snapshots
            .iter()
            .filter(|s| s.name.starts_with(&format!("{}_snap_", subvolume_name)))
            .filter(|s| s.id != snap.id)
            .max_by_key(|s| s.created_at)
            .cloned();

        Ok((snap, previous))
    }
}

/// Parse snapshot list line
fn parse_snapshot_line(line: &str) -> Option<Snapshot> {
    // Format similar to subvolume list
    let parts: Vec<&str> = line.split_whitespace().collect();

    if parts.len() < 7 || parts[0] != "ID" {
        return None;
    }

    let id: u64 = parts[1].parse().ok()?;
    let path = parts[6..].join(" ");
    let name = path.split('/').last().unwrap_or("").to_string();

    Some(Snapshot {
        id,
        name,
        path,
        subvolume_id: 0,
        created_at: Utc::now(),
        size: 0,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_snapshot_line() {
        let line = "ID 258 gen 100 top level 5 path snapshots/snap_20240101";
        let snap = parse_snapshot_line(line).unwrap();
        assert_eq!(snap.id, 258);
        assert_eq!(snap.name, "snap_20240101");
    }
}
