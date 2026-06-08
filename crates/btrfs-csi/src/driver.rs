use anyhow::Result;
use btrfs_exchange::config::ExchangeConfig;
use btrfs_exchange::gossip::GossipService;
use btrfs_exchange::replicator::Replicator;
use btrfs_exchange::scheduler::ReplicaScheduler;
use btrfs_exchange::volume_manager::VolumeManager;
use btrfs_protocol::message::VolumeInfo;
use std::sync::Arc;
use tracing::info;

/// Main CSI driver implementation
pub struct BtrfsCsiDriver {
    config: ExchangeConfig,
    gossip: Arc<GossipService>,
    replicator: Arc<Replicator>,
    volume_manager: Arc<VolumeManager>,
    scheduler: ReplicaScheduler,
}

impl BtrfsCsiDriver {
    /// Create a new CSI driver
    pub async fn new(config: ExchangeConfig) -> Result<Self> {
        info!("Creating Btrfs CSI Driver");

        // Create gossip service
        let gossip = Arc::new(GossipService::new(config.clone()));

        // Create replicator
        let replicator = Arc::new(Replicator::new(config.clone(), gossip.clone()));

        // Create volume manager
        let volume_manager = Arc::new(VolumeManager::new(
            config.clone(),
            gossip.clone(),
            replicator.clone(),
        ));

        // Create scheduler
        let scheduler = ReplicaScheduler::new(config.clone(), replicator.clone());

        Ok(Self {
            config,
            gossip,
            replicator,
            volume_manager,
            scheduler,
        })
    }

    /// Start the driver
    pub async fn start(&self) -> Result<()> {
        info!("Starting Btrfs CSI Driver");

        // Start gossip service
        self.gossip.start().await?;

        // Join cluster if seed nodes are configured
        if !self.config.seed_nodes.is_empty() {
            self.gossip.join_cluster().await?;
        }

        // Start replicator
        self.replicator.start().await?;

        // Start volume manager
        self.volume_manager.start().await?;

        // Start scheduler
        self.scheduler.start().await?;

        info!("Btrfs CSI Driver started successfully");
        Ok(())
    }

    /// Create a new volume
    pub async fn create_volume(
        &self,
        name: &str,
        size: u64,
        replica_count: u32,
    ) -> Result<VolumeInfo> {
        self.volume_manager
            .create_volume(name, size, replica_count)
            .await
    }

    /// Delete a volume
    pub async fn delete_volume(&self, name: &str) -> Result<()> {
        self.volume_manager.delete_volume(name).await
    }

    /// Get volume information
    pub async fn get_volume(&self, name: &str) -> Option<VolumeInfo> {
        self.volume_manager.get_volume(name).await
    }

    /// List all volumes
    pub async fn list_volumes(&self) -> Vec<VolumeInfo> {
        self.volume_manager.list_volumes().await
    }

    /// Check if volume exists
    pub async fn volume_exists(&self, name: &str) -> bool {
        self.volume_manager.volume_exists(name).await
    }

    /// Get node ID
    pub fn node_id(&self) -> &str {
        &self.config.node_id
    }

    /// Get zone
    pub fn zone(&self) -> &str {
        &self.config.zone
    }
}
