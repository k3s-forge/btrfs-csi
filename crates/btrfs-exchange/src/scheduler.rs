use anyhow::Result;
use btrfs_ops::io_priority;
use btrfs_ops::usage::UsageManager;
use std::sync::Arc;
use tracing::{debug, error, info, warn};

use crate::config::ExchangeConfig;
use crate::replicator::Replicator;

/// Load threshold for 1-minute average (0.0 = disabled)
const CPU_LOAD_THRESHOLD: f64 = 4.0;
/// I/O busy threshold in ms (0.0 = disabled)
const IO_BUSY_THRESHOLD: f64 = 1000.0;
/// Retry delay in seconds when system is busy
const BUSY_RETRY_SECS: u64 = 900; // 15 minutes

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

    /// Wait for a maintenance window when system load is low
    async fn wait_for_maintenance_window(config: &ExchangeConfig) {
        let data_dir = &config.replication.data_dir;
        let mut retries = 0;
        let max_retries = 4;

        while retries < max_retries {
            let cpu_busy = io_priority::is_system_busy(CPU_LOAD_THRESHOLD);
            let disk_busy = io_priority::is_disk_busy(data_dir, IO_BUSY_THRESHOLD);

            if !cpu_busy && !disk_busy {
                return;
            }

            retries += 1;
            let load = io_priority::get_load_average();
            warn!(
                "System busy (load={:.2}, disk_busy={}), deferring maintenance (attempt {}/{})",
                load, disk_busy, retries, max_retries
            );

            tokio::time::sleep(tokio::time::Duration::from_secs(BUSY_RETRY_SECS)).await;
        }

        info!("Proceeding with maintenance despite system load (max retries reached)");
    }

    /// Run a command with IDLE I/O priority
    async fn run_with_idle_io<F, Fut>(f: F)
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = ()>,
    {
        // Set current process I/O priority to IDLE
        if let Err(e) = io_priority::set_io_priority_idle() {
            warn!("Failed to set I/O priority to IDLE: {}", e);
        }
        f().await;
    }

    /// Scan fragmentation using btrfs fi df and return block groups needing balance
    async fn find_fragmented_block_groups(data_dir: &str) -> Vec<(u32, u32)> {
        // Parse btrfs fi df for data and metadata usage percentages
        let output = tokio::process::Command::new("btrfs")
            .args(["filesystem", "df", data_dir])
            .output()
            .await;

        let stdout = match output {
            Ok(o) if o.status.success() => String::from_utf8_lossy(&o.stdout).to_string(),
            _ => return vec![(25, 25), (50, 50)], // Fallback to conservative thresholds
        };

        let mut targets = Vec::new();
        for line in stdout.lines() {
            let usage = if line.starts_with("Data,") || line.starts_with("Data.") {
                extract_usage_pct(line)
            } else if line.starts_with("Metadata,") || line.starts_with("Metadata.") {
                extract_usage_pct(line)
            } else {
                None
            };
            if let Some(pct) = usage {
                if pct < 50 {
                    targets.push((pct as u32, 0u32));
                }
            }
        }

        if targets.is_empty() {
            // Conservative defaults
            targets.push((25, 25));
            targets.push((50, 50));
        }
        targets.sort();
        targets
    }

    async fn balance_check_loop(config: ExchangeConfig, usage_manager: UsageManager) {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(3600));
        loop {
            interval.tick().await;
            if !config.maintenance.enabled { continue; }

            match usage_manager.needs_balance(config.maintenance.balance_threshold).await {
                Ok(true) => {
                    Self::wait_for_maintenance_window(&config).await;
                    Self::run_with_idle_io(|| async {
                        Self::run_balance(&config).await;
                    }).await;
                }
                Ok(false) => debug!("Filesystem balance is OK"),
                Err(e) => warn!("Failed to check balance status: {}", e),
            }
        }
    }

    async fn run_balance(config: &ExchangeConfig) {
        let data_dir = &config.replication.data_dir;

        // Targeted balance: only balance fragmented block groups
        let targets = Self::find_fragmented_block_groups(data_dir).await;

        for (dusage, musage) in &targets {
            let mut args = vec!["balance", "start", "-dusage", &dusage.to_string(), "-musage", &musage.to_string(), data_dir];
            let _ = &args;

            info!("Targeted balance: -dusage={} -musage={}", dusage, musage);
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
                }
                Ok(o) => {
                    let stderr = String::from_utf8_lossy(&o.stderr);
                    if stderr.contains("no balance found") || stderr.contains("Nothing to do") {
                        debug!("No more chunks to balance at -dusage={}", dusage);
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
                    Self::wait_for_maintenance_window(&config).await;
                    Self::run_with_idle_io(|| async {
                        Self::run_scrub(&config).await;
                    }).await;
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

                        snapshots.sort_by(|a, b| a.0.cmp(&b.0));

                        let max_keep = (retention.daily + retention.weekly + retention.monthly) as usize;
                        let to_delete = snapshots.len().saturating_sub(max_keep);

                        if to_delete > 0 {
                            Self::wait_for_maintenance_window(&config).await;
                            Self::run_with_idle_io(|| async {
                                for (name, _) in snapshots.iter().take(to_delete) {
                                    let snap_path = format!("{}/{}", snap_dir, name);
                                    info!("Deleting old snapshot: {}", snap_path);
                                    let _ = tokio::process::Command::new("btrfs")
                                        .args(["subvolume", "delete", &snap_path])
                                        .output()
                                        .await;
                                }
                                info!("Cleaned up {} old snapshots", to_delete);
                            }).await;
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

/// Extract usage percentage from a "btrfs filesystem df" line
fn extract_usage_pct(line: &str) -> Option<u64> {
    // Example: "Data, single: total=1.00GiB, used=256.00MiB"
    if let Some(used_part) = line.split(',').nth(1) {
        if let Some(pct_str) = used_part.trim().strip_prefix("used=") {
            if let Some((num_str, unit)) = split_number_unit(pct_str) {
                if let Ok(num) = num_str.parse::<f64>() {
                    let total_str = line.split(',').next()?;
                    if let Some(total_val) = total_str.split('=').nth(1) {
                        if let Some((total_num, total_unit)) = split_number_unit(total_val) {
                            if let Ok(total) = total_num.parse::<f64>() {
                                let used_bytes = convert_to_bytes(num, unit);
                                let total_bytes = convert_to_bytes(total, total_unit);
                                if total_bytes > 0.0 {
                                    return Some(((used_bytes / total_bytes) * 100.0) as u64);
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    None
}

fn split_number_unit(s: &str) -> Option<(String, &str)> {
    let s = s.trim();
    let unit_start = s.rfind(|c: char| c.is_ascii_digit()).map(|i| i + 1).unwrap_or(0);
    if unit_start >= s.len() {
        return Some((s.to_string(), "B"));
    }
    let (num, unit) = s.split_at(unit_start);
    Some((num.to_string(), unit))
}

fn convert_to_bytes(value: f64, unit: &str) -> f64 {
    match unit {
        "B" => value,
        "KiB" | "KB" => value * 1024.0,
        "MiB" | "MB" => value * 1024.0 * 1024.0,
        "GiB" | "GB" => value * 1024.0 * 1024.0 * 1024.0,
        "TiB" | "TB" => value * 1024.0 * 1024.0 * 1024.0 * 1024.0,
        _ => value,
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
