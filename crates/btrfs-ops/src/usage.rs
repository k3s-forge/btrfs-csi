use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use tracing::debug;

use super::commands::BtrfsCommand;

/// Filesystem usage information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilesystemUsage {
    pub total: u64,
    pub used: u64,
    pub free: u64,
    pub data_size: u64,
    pub metadata_size: u64,
    pub system_size: u64,
    pub fragmentation_ratio: f64,
    pub device_count: u32,
}

/// Device usage information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeviceUsage {
    pub path: String,
    pub size: u64,
    pub used: u64,
    pub free: u64,
    pub usage_percent: f64,
    pub imbalance: u64,
}

/// Manager for btrfs filesystem usage
#[derive(Clone)]
pub struct UsageManager {
    pub base_path: String,
}

impl UsageManager {
    /// Create a new usage manager
    pub fn new(base_path: &str) -> Self {
        Self {
            base_path: base_path.to_string(),
        }
    }

    /// Get filesystem usage information
    pub async fn get_usage(&self) -> Result<FilesystemUsage> {
        let output = BtrfsCommand::run("filesystem", &["usage", "-b", &self.base_path]).await?;

        parse_usage_output(&output)
    }

    /// Get device usage information
    pub async fn get_device_usage(&self) -> Result<Vec<DeviceUsage>> {
        let output = BtrfsCommand::run("filesystem", &["usage", &self.base_path]).await?;

        parse_device_usage(&output)
    }

    /// Get free space
    pub async fn get_free_space(&self) -> Result<u64> {
        let usage = self.get_usage().await?;
        Ok(usage.free)
    }

    /// Check if filesystem needs balance
    pub async fn needs_balance(&self, threshold: f64) -> Result<bool> {
        let devices = self.get_device_usage().await?;

        for device in &devices {
            if device.usage_percent > threshold * 100.0 {
                debug!(
                    "Device {} usage {}% exceeds threshold {}%",
                    device.path,
                    device.usage_percent,
                    threshold * 100.0
                );
                return Ok(true);
            }
        }

        Ok(false)
    }

    /// Check if filesystem needs scrub
    pub async fn needs_scrub(&self, last_scrub: Option<i64>) -> Result<bool> {
        // Check if scrub hasn't run in the last week
        if let Some(last) = last_scrub {
            let now = chrono::Utc::now().timestamp_millis();
            let one_week_ms = 7 * 24 * 60 * 60 * 1000;

            if now - last < one_week_ms {
                return Ok(false);
            }
        }

        Ok(true)
    }

    /// Get fragmentation ratio
    pub async fn get_fragmentation(&self) -> Result<f64> {
        let usage = self.get_usage().await?;
        Ok(usage.fragmentation_ratio)
    }
}

/// Parse filesystem usage output
fn parse_usage_output(output: &str) -> Result<FilesystemUsage> {
    let mut total = 0;
    let mut used = 0;
    let mut free = 0;
    let mut data_size = 0;
    let mut metadata_size = 0;
    let mut system_size = 0;

    for line in output.lines() {
        let line = line.trim();

        if line.contains("Overall:") {
            // Parse: Overall:    size: 100.00GiB    used: 50.00GiB
            if let Some(colon_pos) = line.find(':') {
                let value_part = &line[colon_pos + 1..];
                let parts: Vec<&str> = value_part.split_whitespace().collect();
                if parts.len() >= 2 {
                    // Parse size
                    if parts[0] == "size" {
                        total = parse_size_value(parts[1]);
                    } else if parts[0] == "used" {
                        used = parse_size_value(parts[1]);
                    }
                }
            }
        }

        if line.contains("Data:") {
            data_size = parse_size_from_line(line);
        }

        if line.contains("Metadata:") {
            metadata_size = parse_size_from_line(line);
        }

        if line.contains("System:") {
            system_size = parse_size_from_line(line);
        }
    }

    Ok(FilesystemUsage {
        total,
        used,
        free: total - used,
        data_size,
        metadata_size,
        system_size,
        fragmentation_ratio: 0.0, // TODO: Calculate
        device_count: 1,
    })
}

/// Parse device usage output
fn parse_device_usage(output: &str) -> Result<Vec<DeviceUsage>> {
    let mut devices = Vec::new();

    for line in output.lines() {
        if line.contains("/dev/") {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() >= 4 {
                let path = parts[0].to_string();
                let size = parse_size_value(parts[1]);
                let used = parse_size_value(parts[2]);
                let free = size - used;
                let usage_percent = if size > 0 {
                    (used as f64 / size as f64) * 100.0
                } else {
                    0.0
                };

                devices.push(DeviceUsage {
                    path,
                    size,
                    used,
                    free,
                    usage_percent,
                    imbalance: 0,
                });
            }
        }
    }

    Ok(devices)
}

/// Parse size value (e.g., "10.00GiB")
fn parse_size_value(s: &str) -> u64 {
    let s = s.trim().to_lowercase();

    if s.ends_with("tib") || s.ends_with("tb") {
        let value: f64 = s.trim_end_matches("tib").trim_end_matches("tb").trim().parse().unwrap_or(0.0);
        (value * 1024.0 * 1024.0 * 1024.0 * 1024.0) as u64
    } else if s.ends_with("gib") || s.ends_with("gb") {
        let value: f64 = s.trim_end_matches("gib").trim_end_matches("gb").trim().parse().unwrap_or(0.0);
        (value * 1024.0 * 1024.0 * 1024.0) as u64
    } else if s.ends_with("mib") || s.ends_with("mb") {
        let value: f64 = s.trim_end_matches("mib").trim_end_matches("mb").trim().parse().unwrap_or(0.0);
        (value * 1024.0 * 1024.0) as u64
    } else if s.ends_with("kib") || s.ends_with("kb") {
        let value: f64 = s.trim_end_matches("kib").trim_end_matches("kb").trim().parse().unwrap_or(0.0);
        (value * 1024.0) as u64
    } else {
        0
    }
}

/// Parse size from line (e.g., "Data: size 10.00GiB")
fn parse_size_from_line(line: &str) -> u64 {
    let parts: Vec<&str> = line.split_whitespace().collect();
    for (i, part) in parts.iter().enumerate() {
        if *part == "size" && i + 1 < parts.len() {
            return parse_size_value(parts[i + 1]);
        }
    }
    0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_size_value() {
        assert_eq!(parse_size_value("10.00GiB"), 10 * 1024 * 1024 * 1024);
        assert_eq!(parse_size_value("512MiB"), 512 * 1024 * 1024);
        assert_eq!(parse_size_value("1.5TiB"), (1.5 * 1024.0 * 1024.0 * 1024.0 * 1024.0) as u64);
    }
}
