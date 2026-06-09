use anyhow::Result;
use btrfs_ops::usage::UsageManager;
use std::sync::Arc;
use tracing::{debug, error, info, warn};

use crate::config::ExchangeConfig;
use crate::replicator::Replicator;

/// Maintenance scheduler for btrfs operations
pub struct ReplicaScheduler {
    config: ExchangeConfig,
    replicator: Arc<Replicator>,
    usage_manager: UsageManager,
}

impl ReplicaScheduler {
    pub fn new(config: ExchangeConfig, replicator: Arc<Replicator>) -> Self {
        let usage_manager = UsageManager::new(&config.replication.data_dir);
        Self { config, replicator, usage_manager }
    }

    pub async fn start(&self) -> Result<()> {
        info!("Starting maintenance scheduler");

        let config = self.config.clone();
        let usage_manager = self.usage_manager.clone();
        tokio::spawn(async move { Self::balance_check_loop(config, usage_manager).await; });

        let config = self.config.clone();
        let usage_manager = self.usage_manager.clone();
        tokio::spawn(async move { Self::scrub_check_loop(config, usage_manager).await; });

        let config = self.config.clone();
        let replicator = self.replicator.clone();
        tokio::spawn(async move { Self::snapshot_cleanup_loop(config, replicator).await; });

        Ok(())
    }

    async fn balance_check_loop(config: ExchangeConfig, usage_manager: UsageManager) {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(3600));
        loop {
            interval.tick().await;
            if !config.maintenance.enabled { continue; }

            match usage_manager.needs_balance(config.maintenance.balance_threshold).await {
                Ok(true) => {
                    info!("Filesystem needs balance, starting with IO-throttled settings");
                    Self::run_balance(&config).await;
                }
                Ok(false) => debug!("Filesystem balance is OK"),
                Err(e) => warn!("Failed to check balance status: {}", e),
            }
        }
    }

    async fn run_balance(config: &ExchangeConfig) {
        let data_dir = &config.replication.data_dir;
        // Start with conservative dusage/musage to limit IO impact
        // Gradually increase if balance doesn't complete in time
        let thresholds = [(25, 25), (50, 50), (75, 75), (100, 100)];

        for (dusage, musage) in thresholds {
            info!("Running balance with -dusage={} -musage={}", dusage, musage);
            let output = tokio::process::Command::new("btrfs")
                .args([
                    "balance", "start",
                    &format!("-dusage={}", dusage),
                    &format!("-musage={}", musage),
                    data_dir,
                ])
                .output()
                .await;

            match output {
                Ok(o) if o.status.success() => {
                    info!("Balance completed at -dusage={} -musage={}", dusage, musage);
                    break;
                }
                Ok(o) => {
                    let stderr = String::from_utf8_lossy(&o.stderr);
                    if stderr.contains("no balance found") || stderr.contains("Nothing to do") {
                        debug!("No more chunks to balance");
                        break;
                    }
                    warn!("Balance at -dusage={} failed: {}", dusage, stderr);
                }
                Err(e) => {
                    error!("Failed to execute btrfs balance: {}", e);
                    break;
                }
            }
        }
    }

    async fn scrub_check_loop(config: ExchangeConfig, usage_manager: UsageManager) {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(86400));
        loop {
            interval.tick().await;
            if !config.maintenance.enabled { continue; }

            match usage_manager.needs_scrub(None).await {
                Ok(true) => {
                    info!("Filesystem needs scrub, starting");
                    Self::run_scrub(&config).await;
                }
                Ok(false) => debug!("Scrub is not needed"),
                Err(e) => warn!("Failed to check scrub status: {}", e),
            }
        }
    }

    async fn run_scrub(config: &ExchangeConfig) {
        let data_dir = &config.replication.data_dir;
        info!("Starting scrub on {}", data_dir);

        let output = tokio::process::Command::new("btrfs")
            .args(["scrub", "start", "-Bd", data_dir])
            .output()
            .await;

        match output {
            Ok(o) => {
                let stdout = String::from_utf8_lossy(&o.stdout);
                let stderr = String::from_utf8_lossy(&o.stderr);
                if o.status.success() {
                    info!("Scrub completed: {}", stdout.trim());
                } else {
                    warn!("Scrub failed (exit={}): {}", o.status, stderr);
                }
            }
            Err(e) => error!("Failed to execute btrfs scrub: {}", e),
        }
    }

    async fn snapshot_cleanup_loop(config: ExchangeConfig, _replicator: Arc<Replicator>) {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(86400));
        loop {
            interval.tick().await;
            if !config.maintenance.enabled { continue; }

            let retention = &config.maintenance.snapshot_retention;
            let snap_dir = &config.replication.snapshot_dir;

            match tokio::process::Command::new("btrfs")
                .args(["subvolume", "list", "-s", snap_dir])
                .output()
                .await
            {
                Ok(o) if o.status.success() => {
                    let stdout = String::from_utf8_lossy(&o.stdout);
                    let snap_count = stdout.lines().count();

                    if snap_count > (retention.daily + retention.weekly + retention.monthly) as usize {
                        info!(
                            "Snapshot cleanup: {} snapshots, retention daily={} weekly={} monthly={}",
                            snap_count, retention.daily, retention.weekly, retention.monthly
                        );

                        // Delete old snapshots directly via btrfs subvolume delete
                        // Parse snapshot list and delete oldest beyond retention
                        let mut snapshots: Vec<(String, String)> = stdout.lines().filter_map(|line| {
                            let parts: Vec<&str> = line.split_whitespace().collect();
                            if parts.len() >= 7 && parts[0] == "ID" {
                                let path = parts[6..].join(" ");
                                let name = path.split('/').last()?.to_string();
                                Some((name, path))
                            } else {
                                None
                            }
                        }).collect();

                        // Sort by name (timestamp in name) - oldest first
                        snapshots.sort_by(|a, b| a.0.cmp(&b.0));

                        let max_keep = (retention.daily + retention.weekly + retention.monthly) as usize;
                        let to_delete = snapshots.len().saturating_sub(max_keep);

                        for (name, _) in snapshots.iter().take(to_delete) {
                            let snap_path = format!("{}/{}", snap_dir, name);
                            info!("Deleting old snapshot: {}", snap_path);
                            let _ = tokio::process::Command::new("btrfs")
                                .args(["subvolume", "delete", &snap_path])
                                .output()
                                .await;
                        }

                        if to_delete > 0 {
                            info!("Cleaned up {} old snapshots", to_delete);
                        }
                    } else {
                        debug!("Snapshot count {} within retention limit", snap_count);
                    }
                }
                Ok(o) => {
                    let stderr = String::from_utf8_lossy(&o.stderr);
                    warn!("Failed to list snapshots: {}", stderr);
                }
                Err(e) => error!("Failed to execute btrfs subvolume list: {}", e),
            }
        }
    }
}

impl Clone for ReplicaScheduler {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            replicator: self.replicator.clone(),
            usage_manager: self.usage_manager.clone(),
        }
    }
}
