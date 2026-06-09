use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;
use toml;

/// Exchange engine configuration
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

    /// Gossip interval in seconds
    #[serde(default = "default_gossip_interval")]
    pub gossip_interval: u64,

    /// Heartbeat interval in seconds
    #[serde(default = "default_heartbeat_interval")]
    pub heartbeat_interval: u64,

    /// Node timeout in seconds (mark as failed)
    #[serde(default = "default_node_timeout")]
    pub node_timeout: u64,

    /// Replication settings
    pub replication: ReplicationConfig,

    /// Maintenance settings
    pub maintenance: MaintenanceConfig,
}

fn default_gossip_interval() -> u64 { 10 }
fn default_heartbeat_interval() -> u64 { 30 }
fn default_node_timeout() -> u64 { 90 }

impl ExchangeConfig {
    /// Get gossip interval as Duration
    pub fn gossip_interval_duration(&self) -> Duration {
        Duration::from_secs(self.gossip_interval)
    }

    /// Get heartbeat interval as Duration
    pub fn heartbeat_interval_duration(&self) -> Duration {
        Duration::from_secs(self.heartbeat_interval)
    }

    /// Get node timeout as Duration
    pub fn node_timeout_duration(&self) -> Duration {
        Duration::from_secs(self.node_timeout)
    }
}

/// Volume profile for different workload types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeProfile {
    /// Profile name
    pub name: String,

    /// Enable NOCOW (for databases, usually false to keep COW for snapshots)
    pub nocow: bool,

    /// Compression algorithm (none, zstd, lzo, zlib)
    pub compression: String,

    /// Compression level (1-15 for zstd)
    pub compression_level: u8,

    /// Enable sync writes for data integrity
    pub sync_writes: bool,

    /// Enable quota enforcement
    pub quota_enforced: bool,

    /// Subvolume creation flags
    #[serde(default)]
    pub create_flags: Vec<String>,

    /// Mount options for this volume type
    #[serde(default)]
    pub mount_options: Vec<String>,
}

impl Default for VolumeProfile {
    fn default() -> Self {
        Self {
            name: "default".to_string(),
            nocow: false,
            compression: "zstd".to_string(),
            compression_level: 1,
            sync_writes: false,
            quota_enforced: true,
            create_flags: vec![],
            mount_options: vec!["noatime".to_string()],
        }
    }
}

/// Database-specific volume profile (COW enabled, sync writes)
impl VolumeProfile {
    pub fn database() -> Self {
        Self {
            name: "database".to_string(),
            nocow: false,  // Keep COW for snapshots/consistency
            compression: "zstd".to_string(),
            compression_level: 1,
            sync_writes: true,
            quota_enforced: true,
            create_flags: vec![],
            mount_options: vec!["noatime,sync".to_string()],
        }
    }

    pub fn log() -> Self {
        Self {
            name: "log".to_string(),
            nocow: true,   // NOCOW for append-heavy workloads
            compression: "lzo".to_string(),
            compression_level: 1,
            sync_writes: false,
            quota_enforced: true,
            create_flags: vec![],
            mount_options: vec!["noatime".to_string()],
        }
    }
}

/// Replication configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationConfig {
    /// Default replica count
    pub default_replica_count: u32,

    /// Default replication interval in seconds
    #[serde(default = "default_replication_interval")]
    pub default_interval: u64,

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

    /// Volume profiles by type
    #[serde(default)]
    pub volume_profiles: HashMap<String, VolumeProfile>,
}

fn default_replication_interval() -> u64 { 30 }

impl ReplicationConfig {
    /// Get default interval as Duration
    pub fn default_interval_duration(&self) -> Duration {
        Duration::from_secs(self.default_interval)
    }
}

/// Database-specific configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseConfig {
    /// Enable database-aware replication
    pub enabled: bool,

    /// SQLite WAL mode
    pub sqlite_wal_mode: bool,

    /// WAL checkpoint interval in seconds
    #[serde(default = "default_checkpoint_interval")]
    pub checkpoint_interval: u64,

    /// Enable NOCOW for database volumes
    pub enable_nocow: bool,
}

fn default_checkpoint_interval() -> u64 { 30 }

impl DatabaseConfig {
    /// Get checkpoint interval as Duration
    pub fn checkpoint_interval_duration(&self) -> Duration {
        Duration::from_secs(self.checkpoint_interval)
    }
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
            gossip_interval: 10,
            heartbeat_interval: 30,
            node_timeout: 90,
            replication: ReplicationConfig::default(),
            maintenance: MaintenanceConfig::default(),
        }
    }
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        let mut volume_profiles = HashMap::new();
        volume_profiles.insert("default".to_string(), VolumeProfile::default());
        volume_profiles.insert("database".to_string(), VolumeProfile::database());
        volume_profiles.insert("log".to_string(), VolumeProfile::log());

        Self {
            default_replica_count: 2,
            default_interval: 30,
            max_concurrent: 4,
            data_dir: "/mnt/data".to_string(),
            snapshot_dir: "/mnt/snapshots".to_string(),
            enable_incremental: true,
            database: DatabaseConfig::default(),
            volume_profiles,
        }
    }
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            sqlite_wal_mode: true,
            checkpoint_interval: 30,
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
