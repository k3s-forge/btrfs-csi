use anyhow::{Context, Result};
use btrfs_ops::snapshot::{Snapshot, SnapshotManager};
use btrfs_ops::subvolume::SubvolumeManager;
use btrfs_protocol::message::{Message, MessageType, SendCompleteResponse, SendStartRequest};
use btrfs_protocol::transport::TcpTransport;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{error, info, warn};

use crate::config::ExchangeConfig;
use crate::gossip::GossipService;

/// Replication state for a volume
#[derive(Debug, Clone)]
pub struct ReplicationState {
    pub volume_id: String,
    pub last_snapshot: Option<String>,
    pub last_sync: Option<i64>,
    pub status: ReplicationStatus,
    pub target_nodes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ReplicationStatus {
    Idle,
    Syncing,
    Error(String),
    Completed,
}

/// Replicator for handling volume replication
pub struct Replicator {
    config: ExchangeConfig,
    gossip: Arc<GossipService>,
    transport: TcpTransport,
    subvol_manager: SubvolumeManager,
    snap_manager: SnapshotManager,
    states: Arc<RwLock<HashMap<String, ReplicationState>>>,
}

impl Replicator {
    /// Create a new replicator
    pub fn new(config: ExchangeConfig, gossip: Arc<GossipService>) -> Self {
        let transport = TcpTransport::new(config.auth_key.as_bytes());
        let subvol_manager = SubvolumeManager::new(&config.replication.data_dir);
        let snap_manager = SnapshotManager::new(&config.replication.snapshot_dir);

        Self {
            config,
            gossip,
            transport,
            subvol_manager,
            snap_manager,
            states: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Start the replicator
    pub async fn start(&self) -> Result<()> {
        info!("Starting replicator");

        // Start replication scheduler
        let config = self.config.clone();
        let gossip = self.gossip.clone();
        let replicator = self.clone();

        tokio::spawn(async move {
            Self::replication_loop(config, gossip, replicator).await;
        });

        Ok(())
    }

    /// Replication main loop
    async fn replication_loop(
        config: ExchangeConfig,
        gossip: Arc<GossipService>,
        replicator: Replicator,
    ) {
        let mut interval = tokio::time::interval(config.replication.default_interval_duration());

        loop {
            interval.tick().await;

            // Get volumes that need replication
            let volumes = replicator.get_volumes_for_replication().await;

            for vol in volumes {
                if let Err(e) = replicator.replicate_volume(&vol).await {
                    error!("Failed to replicate volume {}: {}", vol, e);
                }
            }
        }
    }

    /// Get volumes that need replication
    async fn get_volumes_for_replication(&self) -> Vec<String> {
        let states = self.states.read().await;
        let now = chrono::Utc::now().timestamp_millis();
        let interval = self.config.replication.default_interval * 1000; // Convert seconds to milliseconds

        states
            .iter()
            .filter(|(_, state)| {
                state.status != ReplicationStatus::Syncing
                    && state
                        .last_sync
                        .map(|last| now - last > interval)
                        .unwrap_or(true)
            })
            .map(|(vol_id, _)| vol_id.clone())
            .collect()
    }

    /// Replicate a single volume
    pub async fn replicate_volume(&self, volume_id: &str) -> Result<()> {
        info!("Replicating volume: {}", volume_id);

        // Update state to syncing
        {
            let mut states = self.states.write().await;
            let state = states
                .entry(volume_id.to_string())
                .or_insert_with(|| ReplicationState {
                    volume_id: volume_id.to_string(),
                    last_snapshot: None,
                    last_sync: None,
                    status: ReplicationStatus::Idle,
                    target_nodes: Vec::new(),
                });
            state.status = ReplicationStatus::Syncing;
        }

        // Create snapshot for replication
        let (snapshot, previous) = self
            .snap_manager
            .create_for_replication(volume_id)
            .await
            .context("Failed to create snapshot")?;

        // Get target nodes
        let targets = self.select_replication_targets(volume_id).await;

        // Send to each target
        let mut success = true;
        for target in &targets {
            match self
                .send_to_target(&snapshot, &previous, target)
                .await
            {
                Ok(()) => {
                    info!("Successfully replicated {} to {}", volume_id, target.id);
                }
                Err(e) => {
                    error!("Failed to replicate {} to {}: {}", volume_id, target.id, e);
                    success = false;
                }
            }
        }

        // Update state
        {
            let mut states = self.states.write().await;
            if let Some(state) = states.get_mut(volume_id) {
                state.last_snapshot = Some(snapshot.name);
                state.last_sync = Some(chrono::Utc::now().timestamp_millis());
                state.status = if success {
                    ReplicationStatus::Completed
                } else {
                    ReplicationStatus::Error("Some replications failed".to_string())
                };
            }
        }

        Ok(())
    }

    /// Select replication targets
    async fn select_replication_targets(
        &self,
        volume_id: &str,
    ) -> Vec<btrfs_protocol::message::NodeInfo> {
        let states = self.states.read().await;
        let exclude = states
            .get(volume_id)
            .map(|s| s.target_nodes.clone())
            .unwrap_or_default();

        let count = self.config.replication.default_replica_count as usize;

        self.gossip
            .select_replica_targets(&exclude, count)
            .await
    }

    /// Send volume to target node
    async fn send_to_target(
        &self,
        snapshot: &Snapshot,
        previous: &Option<Snapshot>,
        target: &btrfs_protocol::message::NodeInfo,
    ) -> Result<()> {
        let addr: SocketAddr = target.addr.parse().context("Invalid target address")?;

        let mut conn = self.transport.connect(addr).await?;

        // Send start message
        let start_req = SendStartRequest {
            volume_id: snapshot.name.clone(),
            snapshot_name: snapshot.path.clone(),
            is_incremental: previous.is_some(),
            parent_snapshot: previous.as_ref().map(|p| p.path.clone()),
        };

        let payload = serde_json::to_vec(&start_req)?;
        let msg = Message::new(MessageType::SendStart, payload);
        conn.send_message(&msg).await?;

        // Wait for acknowledgment
        let ack = conn.recv_message().await?;
        if ack.msg_type != MessageType::SendStart {
            return Err(anyhow::anyhow!("Unexpected response: {:?}", ack.msg_type));
        }

        // TODO: Stream btrfs send data
        // For now, just send completion
        let complete_msg = Message::new(
            MessageType::SendComplete,
            serde_json::to_vec(&SendCompleteResponse {
                volume_id: snapshot.name.clone(),
                success: true,
                error: None,
            })?,
        );
        conn.send_message(&complete_msg).await?;

        Ok(())
    }

    /// Register a volume for replication
    pub async fn register_volume(
        &self,
        volume_id: &str,
        target_nodes: Vec<String>,
    ) -> Result<()> {
        let mut states = self.states.write().await;
        states.insert(
            volume_id.to_string(),
            ReplicationState {
                volume_id: volume_id.to_string(),
                last_snapshot: None,
                last_sync: None,
                status: ReplicationStatus::Idle,
                target_nodes,
            },
        );
        Ok(())
    }

    /// Unregister a volume
    pub async fn unregister_volume(&self, volume_id: &str) -> Result<()> {
        let mut states = self.states.write().await;
        states.remove(volume_id);
        Ok(())
    }

    /// Get replication status
    pub async fn get_status(&self, volume_id: &str) -> Option<ReplicationState> {
        self.states.read().await.get(volume_id).cloned()
    }

    /// Get all replication states
    pub async fn get_all_states(&self) -> HashMap<String, ReplicationState> {
        self.states.read().await.clone()
    }
}

impl Clone for Replicator {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            gossip: self.gossip.clone(),
            transport: TcpTransport::new(self.config.auth_key.as_bytes()),
            subvol_manager: SubvolumeManager::new(&self.config.replication.data_dir),
            snap_manager: SnapshotManager::new(&self.config.replication.snapshot_dir),
            states: self.states.clone(),
        }
    }
}
