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

const MAX_RETRIES: u32 = 5;
const BASE_BACKOFF_MS: u64 = 1000;
const MAX_BACKOFF_MS: u64 = 60000;

/// Replication state for a volume
#[derive(Debug, Clone)]
pub struct ReplicationState {
    pub volume_id: String,
    pub last_snapshot: Option<String>,
    pub last_sync: Option<i64>,
    pub status: ReplicationStatus,
    pub target_nodes: Vec<String>,
    pub retry_count: u32,
    pub last_error: Option<String>,
    pub failed_nodes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ReplicationStatus {
    Idle,
    Syncing,
    Error(String),
    Completed,
    Retrying,
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

            // Get volumes that need replication (idle, completed, or retrying)
            let volumes = replicator.get_volumes_for_replication().await;

            for vol in volumes {
                // Check backoff for retrying volumes
                let should_retry = {
                    let states = replicator.states.read().await;
                    if let Some(state) = states.get(&vol) {
                        if state.status == ReplicationStatus::Retrying {
                            let backoff = std::cmp::min(
                                BASE_BACKOFF_MS * 2u64.pow(state.retry_count - 1),
                                MAX_BACKOFF_MS,
                            );
                            let now = chrono::Utc::now().timestamp_millis();
                            let last = state.last_sync.unwrap_or(0);
                            now - last >= backoff as i64
                        } else {
                            true
                        }
                    } else {
                        true
                    }
                };

                if should_retry {
                    if let Err(e) = replicator.replicate_volume(&vol).await {
                        error!("Failed to replicate volume {}: {}", vol, e);
                    }
                }
            }
        }
    }

    /// Get volumes that need replication
    async fn get_volumes_for_replication(&self) -> Vec<String> {
        let states = self.states.read().await;
        let now = chrono::Utc::now().timestamp_millis();
        let interval = (self.config.replication.default_interval * 1000) as i64;

        states
            .iter()
            .filter(|(_, state)| {
                match state.status {
                    ReplicationStatus::Syncing => false,
                    ReplicationStatus::Retrying => true,
                    ReplicationStatus::Idle | ReplicationStatus::Completed => {
                        state.last_sync.map(|last| now - last > interval).unwrap_or(true)
                    }
                    ReplicationStatus::Error(_) => {
                        // Retry after 5x the normal interval
                        state.last_sync.map(|last| now - last > interval * 5).unwrap_or(true)
                    }
                }
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
                    retry_count: 0,
                    last_error: None,
                    failed_nodes: Vec::new(),
                });
            state.status = ReplicationStatus::Syncing;
        }

        // Create snapshot for replication
        let (snapshot, previous) = self
            .snap_manager
            .create_for_replication(volume_id)
            .await
            .context("Failed to create snapshot")?;

        // Get target nodes, excluding previously failed nodes
        let targets = self.select_replication_targets(volume_id).await;

        if targets.is_empty() {
            warn!("No replication targets available for volume {}", volume_id);
            let mut states = self.states.write().await;
            if let Some(state) = states.get_mut(volume_id) {
                state.status = ReplicationStatus::Error("No targets available".to_string());
                state.last_error = Some("No replication targets available".to_string());
            }
            return Ok(());
        }

        // Send to each target with retry
        let mut success_count = 0u32;
        let mut new_failed_nodes = Vec::new();

        for target in &targets {
            match self.send_with_retry(&snapshot, &previous, target, volume_id).await {
                Ok(()) => {
                    info!("Successfully replicated {} to {}", volume_id, target.id);
                    success_count += 1;
                }
                Err(e) => {
                    error!("Failed to replicate {} to {} after retries: {}", volume_id, target.id, e);
                    new_failed_nodes.push(target.id.clone());
                }
            }
        }

        // Update state
        {
            let mut states = self.states.write().await;
            if let Some(state) = states.get_mut(volume_id) {
                state.last_snapshot = Some(snapshot.name);
                state.last_sync = Some(chrono::Utc::now().timestamp_millis());

                // Track permanently failed nodes
                for node in &new_failed_nodes {
                    if !state.failed_nodes.contains(node) {
                        state.failed_nodes.push(node.clone());
                    }
                }

                if success_count == targets.len() as u32 {
                    state.status = ReplicationStatus::Completed;
                    state.retry_count = 0;
                    state.last_error = None;
                } else if success_count > 0 {
                    state.status = ReplicationStatus::Completed;
                    state.last_error = Some(format!(
                        "Partial success: {}/{} targets",
                        success_count, targets.len()
                    ));
                } else {
                    state.retry_count += 1;
                    state.status = if state.retry_count >= MAX_RETRIES {
                        ReplicationStatus::Error("Max retries exceeded".to_string())
                    } else {
                        ReplicationStatus::Retrying
                    };
                    state.last_error = Some(format!(
                        "All {} targets failed (attempt {}/{})",
                        targets.len(), state.retry_count, MAX_RETRIES
                    ));
                }
            }
        }

        Ok(())
    }

    /// Send with exponential backoff retry
    async fn send_with_retry(
        &self,
        snapshot: &Snapshot,
        previous: &Option<Snapshot>,
        target: &btrfs_protocol::message::NodeInfo,
        volume_id: &str,
    ) -> Result<()> {
        let mut last_err = None;

        for attempt in 0..=MAX_RETRIES {
            match self.send_to_target(snapshot, previous, target).await {
                Ok(()) => return Ok(()),
                Err(e) => {
                    last_err = Some(e.to_string());
                    if attempt < MAX_RETRIES {
                        let backoff = std::cmp::min(
                            BASE_BACKOFF_MS * 2u64.pow(attempt),
                            MAX_BACKOFF_MS,
                        );
                        warn!(
                            "Replication to {} failed (attempt {}/{}), retrying in {}ms: {}",
                            target.id, attempt + 1, MAX_RETRIES, backoff, e
                        );
                        tokio::time::sleep(tokio::time::Duration::from_millis(backoff)).await;
                    }
                }
            }
        }

        Err(anyhow::anyhow!(
            "Failed after {} retries: {}",
            MAX_RETRIES,
            last_err.unwrap_or_default()
        ))
    }

    /// Select replication targets
    async fn select_replication_targets(
        &self,
        volume_id: &str,
    ) -> Vec<btrfs_protocol::message::NodeInfo> {
        let states = self.states.read().await;
        let state = states.get(volume_id);

        // Exclude previously used targets AND permanently failed nodes
        let mut exclude: Vec<String> = state
            .map(|s| s.target_nodes.clone())
            .unwrap_or_default();

        if let Some(s) = state {
            for node in &s.failed_nodes {
                if !exclude.contains(node) {
                    exclude.push(node.clone());
                }
            }
        }

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

        // Execute btrfs send and stream data
        let mut cmd = tokio::process::Command::new("btrfs");
        cmd.args(["send"]);

        if let Some(parent) = previous {
            cmd.args(["-p", &parent.path]);
        }

        cmd.arg(&snapshot.path);

        let mut child = cmd
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .context("Failed to spawn btrfs send")?;

        // Take stderr before async read to avoid borrow issues
        let mut stderr_output = child.stderr.take();

        let stdout = child.stdout.take().context("Failed to capture stdout")?;
        let mut reader = tokio::io::BufReader::new(stdout);
        let mut buffer = vec![0u8; 64 * 1024]; // 64KB chunks

        use tokio::io::AsyncReadExt;
        loop {
            let n = reader.read(&mut buffer).await?;
            if n == 0 {
                break;
            }

            let chunk = &buffer[..n];
            let send_msg = Message::new(
                MessageType::SendData,
                chunk.to_vec(),
            );
            conn.send_message(&send_msg).await?;
        }

        // Wait for btrfs send to complete
        let status = child.wait().await?;
        if !status.success() {
            // Read stderr asynchronously
            let mut stderr_buf = Vec::new();
            if let Some(ref mut s) = stderr_output {
                let _ = s.read_to_end(&mut stderr_buf).await;
            }
            let stderr_msg = String::from_utf8_lossy(&stderr_buf);
            return Err(anyhow::anyhow!("btrfs send failed: {}", stderr_msg));
        }

        // Send completion
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
                retry_count: 0,
                last_error: None,
                failed_nodes: Vec::new(),
            },
        );
        Ok(())
    }

    /// Clear failed node tracking for a volume (e.g., after node comes back)
    pub async fn clear_failed_nodes(&self, volume_id: &str, node_id: &str) {
        let mut states = self.states.write().await;
        if let Some(state) = states.get_mut(volume_id) {
            state.failed_nodes.retain(|n| n != node_id);
            // Reset error status if we were stuck
            if state.status == ReplicationStatus::Error("Max retries exceeded".to_string()) {
                state.status = ReplicationStatus::Idle;
                state.retry_count = 0;
            }
        }
    }

    /// Get volumes that were targeting a specific node (for re-replication on node failure)
    pub async fn get_volumes_for_node(&self, node_id: &str) -> Vec<String> {
        let states = self.states.read().await;
        states
            .iter()
            .filter(|(_, state)| state.target_nodes.contains(&node_id.to_string()))
            .map(|(vol_id, _)| vol_id.clone())
            .collect()
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
