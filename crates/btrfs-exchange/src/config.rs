use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DurationSeconds};
use std::time::Duration;
use toml;

/// Exchange engine configuration
#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExchangeConfig {
    /// Node identifier (auto-generated if empty)
    pub node_id: String,

    /// Node address for incoming connections
    pub listen_addr: String,

    /// Port for transport
    pub listen_port: u16,

    /// Zone/region for topology
    pub zone: String,

    /// Shared secret for HMAC authentication
    pub auth_key: String,

    /// Seed nodes for initial cluster join
    pub seed_nodes: Vec<String>,

    /// Gossip interval
    #[serde_as(as = "DurationSeconds<String>")]
    pub gossip_interval: Duration,

    /// Heartbeat interval
    #[serde_as(as = "DurationSeconds<String>")]
    pub heartbeat_interval: Duration,

    /// Node timeout (mark as failed)
    #[serde_as(as = "DurationSeconds<String>")]
    pub node_timeout: Duration,

    /// Replication settings
    pub replication: ReplicationConfig,

    /// Maintenance settings
    pub maintenance: MaintenanceConfig,
}

/// Replication configuration
#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationConfig {
    /// Default replica count
    pub default_replica_count: u32,

    /// Default replication interval
    #[serde_as(as = "DurationSeconds<String>")]
    pub default_interval: Duration,

    /// Maximum concurrent replications
    pub max_concurrent: u32,

    /// Data directory for snapshots
    pub data_dir: String,

    /// Snapshot directory
    pub snapshot_dir: String,

    /// Enable incremental replication
    pub enable_incremental: bool,

    /// Database optimization settings
    pub database: DatabaseConfig,
}

/// Database-specific configuration
#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseConfig {
    /// Enable database-aware replication
    pub enabled: bool,

    /// SQLite WAL mode
    pub sqlite_wal_mode: bool,

    /// WAL checkpoint interval
    #[serde_as(as = "DurationSeconds<String>")]
    pub checkpoint_interval: Duration,

    /// Enable NOCOW for database volumes
    pub enable_nocow: bool,
}

/// Maintenance configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaintenanceConfig {
    /// Enable automatic maintenance
    pub enabled: bool,

    /// Balance schedule (cron expression)
    pub balance_schedule: String,

    /// Balance threshold (0.0 - 1.0)
    pub balance_threshold: f64,

    /// Scrub schedule (cron expression)
    pub scrub_schedule: String,

    /// Snapshot cleanup schedule
    pub snapshot_cleanup_schedule: String,

    /// Snapshot retention
    pub snapshot_retention: SnapshotRetention,
}

/// Snapshot retention policy
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotRetention {
    /// Daily snapshots to keep
    pub daily: u32,

    /// Weekly snapshots to keep
    pub weekly: u32,

    /// Monthly snapshots to keep
    pub monthly: u32,
}

impl Default for ExchangeConfig {
    fn default() -> Self {
        Self {
            node_id: uuid::Uuid::new_v4().to_string(),
            listen_addr: "0.0.0.0".to_string(),
            listen_port: 9200,
            zone: "default".to_string(),
            auth_key: String::new(),
            seed_nodes: Vec::new(),
            gossip_interval: Duration::from_secs(10),
            heartbeat_interval: Duration::from_secs(30),
            node_timeout: Duration::from_secs(90),
            replication: ReplicationConfig::default(),
            maintenance: MaintenanceConfig::default(),
        }
    }
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            default_replica_count: 2,
            default_interval: Duration::from_secs(30),
            max_concurrent: 4,
            data_dir: "/mnt/data".to_string(),
            snapshot_dir: "/mnt/snapshots".to_string(),
            enable_incremental: true,
            database: DatabaseConfig::default(),
        }
    }
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            sqlite_wal_mode: true,
            checkpoint_interval: Duration::from_secs(30),
            enable_nocow: false,
        }
    }
}

impl Default for MaintenanceConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            balance_schedule: "0 2 * * *".to_string(), // Daily at 2 AM
            balance_threshold: 0.7,
            scrub_schedule: "0 3 * * 0".to_string(), // Weekly on Sunday at 3 AM
            snapshot_cleanup_schedule: "0 4 * * *".to_string(), // Daily at 4 AM
            snapshot_retention: SnapshotRetention {
                daily: 7,
                weekly: 4,
                monthly: 3,
            },
        }
    }
}

impl ExchangeConfig {
    /// Load configuration from file
    pub fn load(path: &str) -> anyhow::Result<Self> {
        let content = std::fs::read_to_string(path)?;
        let config: Self = toml::from_str(&content)?;
        Ok(config)
    }

    /// Save configuration to file
    pub fn save(&self, path: &str) -> anyhow::Result<()> {
        let content = toml::to_string_pretty(self)?;
        std::fs::write(path, content)?;
        Ok(())
    }
}
