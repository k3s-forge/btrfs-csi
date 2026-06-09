use anyhow::{Context, Result};
use btrfs_ops::subvolume::SubvolumeManager;
use btrfs_protocol::message::VolumeInfo;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{error, info, warn};

use crate::config::ExchangeConfig;
use crate::gossip::GossipService;
use crate::replicator::Replicator;

/// Volume manager for handling volume lifecycle
pub struct VolumeManager {
    config: ExchangeConfig,
    gossip: Arc<GossipService>,
    replicator: Arc<Replicator>,
    subvol_manager: SubvolumeManager,
    volumes: Arc<RwLock<HashMap<String, VolumeInfo>>>,
}

impl VolumeManager {
    /// Create a new volume manager
    pub fn new(
        config: ExchangeConfig,
        gossip: Arc<GossipService>,
        replicator: Arc<Replicator>,
    ) -> Self {
        let subvol_manager = SubvolumeManager::new(&config.replication.data_dir);

        Self {
            config,
            gossip,
            replicator,
            subvol_manager,
            volumes: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Start the volume manager
    pub async fn start(&self) -> Result<()> {
        info!("Starting volume manager");

        // Load existing volumes
        self.load_volumes().await?;

        // Start volume sync
        let config = self.config.clone();
        let gossip = self.gossip.clone();
        let volumes = self.volumes.clone();

        tokio::spawn(async move {
            Self::volume_sync_loop(config, gossip, volumes).await;
        });

        Ok(())
    }

    /// Load existing volumes from filesystem
    async fn load_volumes(&self) -> Result<()> {
        let subvolumes = self.subvol_manager.list().await?;

        let mut volumes = self.volumes.write().await;

        for subvol in subvolumes {
            let vol_info = VolumeInfo {
                id: subvol.name.clone(),
                name: subvol.name.clone(),
                size: subvol.size,
                node_id: self.config.node_id.clone(),
                zone: self.config.zone.clone(),
                status: "active".to_string(),
                created_at: chrono::Utc::now().timestamp_millis(),
            };

            volumes.insert(subvol.name, vol_info);
        }

        info!("Loaded {} volumes", volumes.len());
        Ok(())
    }

    /// Volume sync loop
    async fn volume_sync_loop(
        config: ExchangeConfig,
        gossip: Arc<GossipService>,
        volumes: Arc<RwLock<HashMap<String, VolumeInfo>>>,
    ) {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(60));

        loop {
            interval.tick().await;

            let volumes_snapshot = volumes.read().await.clone();
            let volume_list: Vec<VolumeInfo> = volumes_snapshot.into_values().collect();
            gossip.update_volumes(volume_list).await;
        }
    }

    /// Create a new volume
    pub async fn create_volume(
        &self,
        name: &str,
        size: u64,
        replica_count: u32,
    ) -> Result<VolumeInfo> {
        info!("Creating volume: {} (size: {})", name, size);

        // Create subvolume
        let _subvol = self
            .subvol_manager
            .create(name)
            .await
            .context("Failed to create subvolume")?;

        let vol_info = VolumeInfo {
            id: name.to_string(),
            name: name.to_string(),
            size,
            node_id: self.config.node_id.clone(),
            zone: self.config.zone.clone(),
            status: "active".to_string(),
            created_at: chrono::Utc::now().timestamp_millis(),
        };

        // Store volume
        {
            let mut volumes = self.volumes.write().await;
            volumes.insert(name.to_string(), vol_info.clone());
        }

        // Register for replication
        if replica_count > 0 {
            self.replicator
                .register_volume(name, Vec::new())
                .await?;
        }

        Ok(vol_info)
    }

    /// Delete a volume
    pub async fn delete_volume(&self, name: &str) -> Result<()> {
        info!("Deleting volume: {}", name);

        // Unregister from replication
        self.replicator.unregister_volume(name).await?;

        // Delete subvolume
        self.subvol_manager
            .delete(name)
            .await
            .context("Failed to delete subvolume")?;

        // Remove from cache
        {
            let mut volumes = self.volumes.write().await;
            volumes.remove(name);
        }

        Ok(())
    }

    /// Get volume information
    pub async fn get_volume(&self, name: &str) -> Option<VolumeInfo> {
        self.volumes.read().await.get(name).cloned()
    }

    /// List all volumes
    pub async fn list_volumes(&self) -> Vec<VolumeInfo> {
        self.volumes.read().await.values().cloned().collect()
    }

    /// Check if volume exists
    pub async fn volume_exists(&self, name: &str) -> bool {
        self.volumes.read().await.contains_key(name)
    }

    /// Get volume count
    pub async fn volume_count(&self) -> usize {
        self.volumes.read().await.len()
    }

    /// Get total volume size
    pub async fn total_size(&self) -> u64 {
        self.volumes
            .read()
            .await
            .values()
            .map(|v| v.size)
            .sum()
    }

    /// Update volume status
    pub async fn update_status(&self, name: &str, status: &str) -> Result<()> {
        let mut volumes = self.volumes.write().await;
        if let Some(vol) = volumes.get_mut(name) {
            vol.status = status.to_string();
        }
        Ok(())
    }
}
