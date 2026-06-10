use anyhow::{anyhow, Result};
use btrfs_protocol::message::{
    ConflictInfo, DeleteVolumeRequest, EpochInfo, HeartbeatPayload, Message, MessageType,
    NodeInfo, QuorumVoteRequest, QuorumVoteResponse, VolumeUnpublishRequest,
};
use btrfs_protocol::transport::TcpTransport;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::{RwLock, Semaphore};
use tracing::{debug, error, info, warn};

use crate::config::ExchangeConfig;

const MAX_CONCURRENT_CONNECTIONS: usize = 512;
const RECV_TIMEOUT_SECS: u64 = 30;

/// Callback type for node failure events
pub type NodeFailureCallback = Arc<dyn Fn(String) + Send + Sync>;

/// Callback type for conflict events
pub type ConflictCallback = Arc<dyn Fn(ConflictInfo) + Send + Sync>;

/// Gossip service for node discovery and state synchronization
pub struct GossipService {
    config: ExchangeConfig,
    transport: TcpTransport,
    peers: Arc<RwLock<HashMap<String, NodeInfo>>>,
    on_node_failure: Arc<RwLock<Vec<NodeFailureCallback>>>,
    on_conflict: Arc<RwLock<Vec<ConflictCallback>>>,
    /// Track epoch/status of volumes known to this node
    volume_epochs: Arc<RwLock<HashMap<String, EpochInfo>>>,
}

impl GossipService {
    /// Create a new gossip service
    pub fn new(config: ExchangeConfig) -> Self {
        let transport = TcpTransport::new(config.auth_key.as_bytes());

        Self {
            config,
            transport,
            peers: Arc::new(RwLock::new(HashMap::new())),
            on_node_failure: Arc::new(RwLock::new(Vec::new())),
            on_conflict: Arc::new(RwLock::new(Vec::new())),
            volume_epochs: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Register a callback for node failure events
    pub async fn on_node_failure(&self, callback: NodeFailureCallback) {
        self.on_node_failure.write().await.push(callback);
    }

    /// Register a callback for conflict events
    pub async fn on_conflict_detected(&self, callback: ConflictCallback) {
        self.on_conflict.write().await.push(callback);
    }

    /// Start the gossip service
    pub async fn start(&self) -> Result<()> {
        info!(
            "Starting gossip service on {}:{}",
            self.config.listen_addr, self.config.listen_port
        );

        // Start listener for incoming gossip messages
        let config = self.config.clone();
        let transport = TcpTransport::new(config.auth_key.as_bytes());
        let peers = self.peers.clone();
        let on_node_failure = self.on_node_failure.clone();
        let on_conflict = self.on_conflict.clone();
        let volume_epochs = self.volume_epochs.clone();
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();

        tokio::spawn(async move {
            Self::listener_loop(config, transport, peers, on_node_failure, on_conflict, volume_epochs, ready_tx).await;
        });

        // Wait for listener to confirm bind success (propagate error if it failed)
        ready_rx.await??;

        info!("Gossip listener bound successfully");

        // Start heartbeat sender
        let config = self.config.clone();
        let transport = TcpTransport::new(config.auth_key.as_bytes());
        let peers = self.peers.clone();

        tokio::spawn(async move {
            Self::heartbeat_loop(config, transport, peers).await;
        });

        // Start peer cleanup
        let config = self.config.clone();
        let peers = self.peers.clone();
        let on_node_failure = self.on_node_failure.clone();

        tokio::spawn(async move {
            Self::peer_cleanup_loop(config, peers, on_node_failure).await;
        });

        Ok(())
    }

    /// Join the cluster by connecting to seed nodes
    pub async fn join_cluster(&self) -> Result<()> {
        for seed in &self.config.seed_nodes {
            let addr: SocketAddr = seed.parse()?;
            info!("Joining cluster via seed node: {}", addr);

            match self.transport.connect(addr).await {
                Ok(mut conn) => {
                    // Send node join message
                    let payload = serde_json::to_vec(&self.create_heartbeat_payload())?;
                    let msg = Message::new(MessageType::NodeJoin, payload);
                    conn.send_message(&msg).await?;

                    // Wait for response
                    let response = conn.recv_message().await?;
                    match response.msg_type {
                        MessageType::NodeList => {
                            let nodes: Vec<NodeInfo> = serde_json::from_slice(&response.payload)?;
                            for node in nodes {
                                self.peers
                                    .write()
                                    .await
                                    .insert(node.id.clone(), node);
                            }
                            info!("Joined cluster successfully");
                            break;
                        }
                        _ => {
                            warn!("Unexpected response from seed node");
                        }
                    }
                }
                Err(e) => {
                    warn!("Failed to connect to seed node {}: {}", addr, e);
                }
            }
        }

        Ok(())
    }

    /// TCP listener for incoming gossip messages (heartbeats, join, leave)
    async fn listener_loop(
        config: ExchangeConfig,
        transport: TcpTransport,
        peers: Arc<RwLock<HashMap<String, NodeInfo>>>,
        on_node_failure: Arc<RwLock<Vec<NodeFailureCallback>>>,
        on_conflict: Arc<RwLock<Vec<ConflictCallback>>>,
        volume_epochs: Arc<RwLock<HashMap<String, EpochInfo>>>,
        ready_tx: tokio::sync::oneshot::Sender<anyhow::Result<()>>,
    ) {
        let addr: SocketAddr = match format!("{}:{}", config.listen_addr, config.listen_port).parse() {
            Ok(a) => a,
            Err(e) => {
                error!("Invalid gossip listen address: {}", e);
                let _ = ready_tx.send(Err(anyhow::anyhow!("Invalid gossip listen address: {}", e)));
                return;
            }
        };

        let listener = match transport.listen(addr).await {
            Ok(l) => {
                let _ = ready_tx.send(Ok(()));
                l
            }
            Err(e) => {
                error!("Failed to bind gossip listener on {}: {}", addr, e);
                let _ = ready_tx.send(Err(anyhow::anyhow!("Failed to bind gossip listener on {}: {}", addr, e)));
                return;
            }
        };

        info!("Gossip listener ready on {}", addr);

        let connection_semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_CONNECTIONS));

        loop {
            // Acquire permit before accepting to limit concurrent connections
            let permit = connection_semaphore.clone().acquire_owned().await;
            let Ok(permit) = permit else { return };

            match transport.accept(&listener).await {
                Ok(mut conn) => {
                    let config = config.clone();
                    let peers = peers.clone();
                    let on_node_failure = on_node_failure.clone();
                    let on_conflict = on_conflict.clone();
                    let volume_epochs = volume_epochs.clone();

                    tokio::spawn(async move {
                        if let Err(e) = Self::handle_gossip_message(
                            &config, &mut conn, &peers, &on_node_failure, &on_conflict, &volume_epochs,
                        ).await {
                            debug!("Gossip message handler error: {}", e);
                        }
                        drop(permit);
                    });
                }
                Err(e) => {
                    drop(permit);
                    warn!("Failed to accept gossip connection: {}", e);
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                }
            }
        }
    }

    /// Handle a single incoming gossip message
    async fn handle_gossip_message(
        config: &ExchangeConfig,
        conn: &mut btrfs_protocol::transport::TransportConnection,
        peers: &Arc<RwLock<HashMap<String, NodeInfo>>>,
        on_node_failure: &Arc<RwLock<Vec<NodeFailureCallback>>>,
        on_conflict: &Arc<RwLock<Vec<ConflictCallback>>>,
        volume_epochs: &Arc<RwLock<HashMap<String, EpochInfo>>>,
    ) -> anyhow::Result<()> {
        let msg = conn.recv_message().await.map_err(|e| anyhow::anyhow!("{}", e))?;

        match msg.msg_type {
            MessageType::Heartbeat => {
                let heartbeat: HeartbeatPayload = serde_json::from_slice(&msg.payload)?;
                let now = chrono::Utc::now().timestamp_millis();

                let node_info = NodeInfo {
                    id: heartbeat.node_id.clone(),
                    addr: heartbeat.addr,
                    zone: heartbeat.zone,
                    role: heartbeat.role,
                    free_space: heartbeat.free_space,
                    last_seen: now,
                };

                peers.write().await.insert(heartbeat.node_id.clone(), node_info);

                // Reply with HeartbeatAck
                let ack = Message::new(MessageType::HeartbeatAck, vec![]);
                conn.send_message(&ack).await.map_err(|e| anyhow::anyhow!("{}", e))?;
            }
            MessageType::NodeJoin => {
                let heartbeat: HeartbeatPayload = serde_json::from_slice(&msg.payload)?;
                let now = chrono::Utc::now().timestamp_millis();

                info!("Node joining: {} ({})", heartbeat.node_id, heartbeat.addr);

                let node_info = NodeInfo {
                    id: heartbeat.node_id.clone(),
                    addr: heartbeat.addr,
                    zone: heartbeat.zone,
                    role: heartbeat.role,
                    free_space: heartbeat.free_space,
                    last_seen: now,
                };

                peers.write().await.insert(heartbeat.node_id.clone(), node_info);

                // Respond with current peer list
                let current_peers: Vec<NodeInfo> = peers.read().await.values().cloned().collect();
                let payload = serde_json::to_vec(&current_peers)?;
                let response = Message::new(MessageType::NodeList, payload);
                conn.send_message(&response).await.map_err(|e| anyhow::anyhow!("{}", e))?;
            }
            MessageType::NodeLeave => {
                let heartbeat: HeartbeatPayload = serde_json::from_slice(&msg.payload)?;
                info!("Node leaving: {}", heartbeat.node_id);

                peers.write().await.remove(&heartbeat.node_id);

                let callbacks = on_node_failure.read().await;
                for callback in callbacks.iter() {
                    callback(heartbeat.node_id.clone());
                }
            }
            MessageType::HeartbeatAck => {
                // No-op, just acknowledge
            }
            MessageType::QuorumVote => {
                let vote_req: QuorumVoteRequest = serde_json::from_slice(&msg.payload)?;
                debug!(
                    "Quorum vote request for {} from {} (epoch={})",
                    vote_req.volume_id, vote_req.requester_node, vote_req.epoch
                );

                // Check our local epoch info for conflict detection
                let local = volume_epochs.read().await;
                let local_info = local.get(&vote_req.volume_id);
                let (peer_epoch, peer_vc, conflict) = if let Some(info) = local_info {
                    let local_vc: HashMap<String, u64> = info.vector_clock.iter().cloned().collect();
                    let req_vc: HashMap<String, u64> = vote_req.vector_clock.iter().cloned().collect();
                    let has_conflict = btrfs_ops::xattr::has_conflict(&local_vc, &req_vc);
                    (info.epoch, info.vector_clock.clone(), has_conflict)
                } else {
                    // We don't know this volume; vote granted (epoch=0 means clean)
                    (0u64, vec![], false)
                };
                drop(local);

                let response = QuorumVoteResponse {
                    volume_id: vote_req.volume_id.clone(),
                    peer_epoch,
                    peer_vector_clock: peer_vc,
                    vote_granted: !conflict || peer_epoch == 0,
                    peer_node: config.node_id.clone(),
                    conflict,
                };

                let payload = serde_json::to_vec(&response)?;
                let ack = Message::new(MessageType::QuorumVoteResponse, payload);
                conn.send_message(&ack).await.map_err(|e| anyhow::anyhow!("{}", e))?;

                if conflict {
                    warn!(
                        "CONFLICT DETECTED for volume {} between {} and local node",
                        vote_req.volume_id, vote_req.requester_node
                    );
                }
            }
            MessageType::QuorumVoteResponse => {
                // Responses are handled synchronously in request_quorum_lease,
                // but we may receive unsolicited ones; ignore.
            }
            MessageType::ConflictDetected => {
                let conflict_info: ConflictInfo = serde_json::from_slice(&msg.payload)?;
                error!(
                    "Volume {} in CONFLICT between {} and {}",
                    conflict_info.volume_id, conflict_info.node_a, conflict_info.node_b
                );

                // Update local epoch status to conflict
                let mut epochs = volume_epochs.write().await;
                if let Some(info) = epochs.get_mut(&conflict_info.volume_id) {
                    info.status = "conflict".to_string();
                }

                let callbacks = on_conflict.read().await;
                for cb in callbacks.iter() {
                    cb(conflict_info.clone());
                }
            }
            MessageType::DeleteVolume => {
                let del_req: DeleteVolumeRequest = serde_json::from_slice(&msg.payload)?;
                info!("Received volume delete notification for {}", del_req.volume_id);

                // Remove from volume epoch tracking
                volume_epochs.write().await.remove(&del_req.volume_id);

                // Delete received replica data from snapshot_dir
                let snap_dir = format!("{}/{}", config.replication.snapshot_dir, del_req.volume_id);
                let _ = tokio::fs::remove_dir_all(&snap_dir).await;
                info!("Cleaned up replica data for {} at {}", del_req.volume_id, snap_dir);

                // Send acknowledgment
                let ack = Message::new(MessageType::DeleteVolumeAck, vec![]);
                let _ = conn.send_message(&ack).await;
            }
            MessageType::VolumeUnpublish => {
                let unpublish_req: VolumeUnpublishRequest =
                    serde_json::from_slice(&msg.payload)?;
                info!(
                    "Received volume unpublish notification: volume={}, node={}",
                    unpublish_req.volume_id, unpublish_req.node_id
                );
                // Remove the node from published list for this volume
                // (The volume remains; only the node's publication is removed)
                let _ = conn
                    .send_message(&Message::new(MessageType::VolumeUnpublishAck, vec![]))
                    .await;
            }
            _ => {
                debug!("Unexpected gossip message type: {:?}", msg.msg_type);
            }
        }

        Ok(())
    }

    /// Broadcast heartbeat to all peers
    async fn heartbeat_loop(
        config: ExchangeConfig,
        transport: TcpTransport,
        peers: Arc<RwLock<HashMap<String, NodeInfo>>>,
    ) {
        let mut interval = tokio::time::interval(config.heartbeat_interval_duration());

        loop {
            interval.tick().await;

            let payload = {
                let heartbeat = HeartbeatPayload {
                    node_id: config.node_id.clone(),
                    addr: format!("{}:{}", config.listen_addr, config.listen_port),
                    zone: config.zone.clone(),
                    role: "replica".to_string(),
                    free_space: get_free_space(&config.replication.data_dir),
                };
                serde_json::to_vec(&heartbeat).unwrap_or_default()
            };

            let peers_snapshot = peers.read().await.clone();

            for (node_id, node_info) in &peers_snapshot {
                if node_id == &config.node_id {
                    continue;
                }

                let addr: SocketAddr = match node_info.addr.parse() {
                    Ok(addr) => addr,
                    Err(e) => {
                        warn!("Invalid address for node {}: {}", node_id, e);
                        continue;
                    }
                };

                match transport.connect(addr).await {
                    Ok(mut conn) => {
                        let msg = Message::new(MessageType::Heartbeat, payload.clone());
                        if let Err(e) = conn.send_message(&msg).await {
                            warn!("Failed to send heartbeat to {}: {}", node_id, e);
                        }
                    }
                    Err(e) => {
                        debug!("Failed to connect to {}: {}", node_id, e);
                    }
                }
            }
        }
    }

    /// Cleanup stale peers
    async fn peer_cleanup_loop(
        config: ExchangeConfig,
        peers: Arc<RwLock<HashMap<String, NodeInfo>>>,
        on_node_failure: Arc<RwLock<Vec<NodeFailureCallback>>>,
    ) {
        let mut interval = tokio::time::interval(config.node_timeout_duration());

        loop {
            interval.tick().await;

            let now = chrono::Utc::now().timestamp_millis();
            let timeout = (config.node_timeout * 1000) as i64;

            let mut peers = peers.write().await;
            let stale_nodes: Vec<String> = peers
                .iter()
                .filter(|(_, node)| now - node.last_seen > timeout)
                .map(|(id, _)| id.clone())
                .collect();

            for node_id in stale_nodes {
                warn!("Removing stale node: {}", node_id);
                peers.remove(&node_id);

                // Notify listeners about node failure
                let callbacks = on_node_failure.read().await;
                for callback in callbacks.iter() {
                    callback(node_id.clone());
                }
            }
        }
    }

    /// Create heartbeat payload
    fn create_heartbeat_payload(&self) -> HeartbeatPayload {
        HeartbeatPayload {
            node_id: self.config.node_id.clone(),
            addr: format!("{}:{}", self.config.listen_addr, self.config.listen_port),
            zone: self.config.zone.clone(),
            role: "replica".to_string(),
            free_space: get_free_space(&self.config.replication.data_dir),
        }
    }

    /// Get all peers
    pub async fn get_peers(&self) -> HashMap<String, NodeInfo> {
        self.peers.read().await.clone()
    }

    /// Get peer count
    pub async fn peer_count(&self) -> usize {
        self.peers.read().await.len()
    }

    /// Select replica targets for a volume
    pub async fn select_replica_targets(
        &self,
        exclude_nodes: &[String],
        count: usize,
    ) -> Vec<NodeInfo> {
        let peers = self.peers.read().await;

        let mut candidates: Vec<NodeInfo> = peers
            .values()
            .filter(|node| {
                node.id != self.config.node_id && !exclude_nodes.contains(&node.id)
            })
            .cloned()
            .collect();

        // Sort by free space (descending)
        candidates.sort_by(|a, b| b.free_space.cmp(&a.free_space));

        candidates.into_iter().take(count).collect()
    }

    /// Select targets by zone
    pub async fn select_targets_by_zone(
        &self,
        zones: &[String],
        exclude_nodes: &[String],
    ) -> Vec<NodeInfo> {
        let peers = self.peers.read().await;
        let mut selected = Vec::new();

        for zone in zones {
            let zone_peers: Vec<NodeInfo> = peers
                .values()
                .filter(|node| {
                    node.zone == *zone
                        && node.id != self.config.node_id
                        && !exclude_nodes.contains(&node.id)
                })
                .cloned()
                .collect();

            if let Some(best) = zone_peers.iter().max_by_key(|n| n.free_space) {
                selected.push(best.clone());
            }
        }

        selected
    }

    /// Send NodeLeave to all peers
    pub async fn leave_cluster(&self) {
        let payload = serde_json::to_vec(&self.create_heartbeat_payload()).unwrap_or_default();
        let peers_snapshot = self.peers.read().await.clone();

        for (_, node_info) in &peers_snapshot {
            if let Ok(addr) = node_info.addr.parse::<SocketAddr>() {
                if let Ok(mut conn) = self.transport.connect(addr).await {
                    let msg = Message::new(MessageType::NodeLeave, payload.clone());
                    let _ = conn.send_message(&msg).await;
                }
            }
        }
    }

    /// Register a volume's epoch info for quorum tracking
    pub async fn register_volume_epoch(
        &self,
        volume_id: &str,
        epoch: u64,
        vector_clock: Vec<(String, u64)>,
        status: &str,
    ) {
        let mut epochs = self.volume_epochs.write().await;
        epochs.insert(volume_id.to_string(), EpochInfo {
            volume_id: volume_id.to_string(),
            epoch,
            vector_clock,
            status: status.to_string(),
            last_synced_from: None,
        });
    }

    /// Update epoch for a volume after successful sync
    pub async fn update_volume_epoch(
        &self,
        volume_id: &str,
        epoch: u64,
        last_synced_from: &str,
    ) {
        let mut epochs = self.volume_epochs.write().await;
        if let Some(info) = epochs.get_mut(volume_id) {
            info.epoch = epoch;
            info.last_synced_from = Some(last_synced_from.to_string());
            info.status = "active".to_string();
        }
    }

    /// Request quorum lease from peers to allow write access to a volume.
    /// Returns true if majority (N/2 + 1) confirm the volume is not conflicted.
    pub async fn request_quorum_lease(
        &self,
        volume_id: &str,
        epoch: u64,
        vector_clock: &[(String, u64)],
    ) -> Result<QuorumLeaseResult> {
        let peers = self.peers.read().await;
        let total_peers = peers.len();
        if total_peers == 0 {
            // Solo node: always grant
            return Ok(QuorumLeaseResult {
                granted: true,
                votes_received: 1,
                votes_needed: 1,
                total_nodes: 1,
                conflict: false,
                conflict_info: None,
            });
        }

        let required = total_peers / 2 + 1;
        let mut votes_granted = 1u32; // Self-vote
        let mut conflict = false;
        let mut conflict_info: Option<ConflictInfo> = None;

        let vote_req = QuorumVoteRequest {
            volume_id: volume_id.to_string(),
            epoch,
            vector_clock: vector_clock.to_vec(),
            requester_node: self.config.node_id.clone(),
        };
        let payload = serde_json::to_vec(&vote_req)?;

        for (node_id, node_info) in peers.iter() {
            if *node_id == self.config.node_id {
                continue;
            }

            let addr: SocketAddr = match node_info.addr.parse() {
                Ok(a) => a,
                Err(_) => continue,
            };

            if let Ok(mut conn) = self.transport.connect(addr).await {
                let msg = Message::new(MessageType::QuorumVote, payload.clone());
                if conn.send_message(&msg).await.is_ok() {
                    if let Ok(response) = conn.recv_message().await {
                        if response.msg_type == MessageType::QuorumVoteResponse {
                            if let Ok(vote) = serde_json::from_slice::<QuorumVoteResponse>(&response.payload) {
                                if vote.conflict {
                                    conflict = true;
                                    conflict_info = Some(ConflictInfo {
                                        volume_id: volume_id.to_string(),
                                        node_a: self.config.node_id.clone(),
                                        node_b: vote.peer_node.clone(),
                                        epoch_a: epoch,
                                        epoch_b: vote.peer_epoch,
                                        vector_clock_a: vector_clock.to_vec(),
                                        vector_clock_b: vote.peer_vector_clock,
                                    });
                                } else if vote.vote_granted {
                                    votes_granted += 1;
                                }
                            }
                        }
                    }
                }
            }
        }

        if conflict {
            // Fire conflict callbacks
            if let Some(ref ci) = conflict_info {
                let callbacks = self.on_conflict.read().await;
                for cb in callbacks.iter() {
                    cb(ci.clone());
                }
            }
            return Ok(QuorumLeaseResult {
                granted: false,
                votes_received: votes_granted,
                votes_needed: required as u32,
                total_nodes: (total_peers + 1) as u32,
                conflict: true,
                conflict_info,
            });
        }

        Ok(QuorumLeaseResult {
            granted: votes_granted >= required as u32,
            votes_received: votes_granted,
            votes_needed: required as u32,
            total_nodes: (total_peers + 1) as u32,
            conflict: false,
            conflict_info: None,
        })
    }

    /// Get the quorum status for a volume
    pub async fn get_volume_epoch_info(&self, volume_id: &str) -> Option<EpochInfo> {
        self.volume_epochs.read().await.get(volume_id).cloned()
    }

    /// Notify peers that a volume is being unpublished from this node (migration)
    pub async fn notify_volume_unpublish(&self, volume_id: &str, node_id: &str) {
        info!("Notifying peers of volume unpublish: volume={}, node={}", volume_id, node_id);
        let unpublish_req = VolumeUnpublishRequest {
            volume_id: volume_id.to_string(),
            node_id: node_id.to_string(),
        };
        let payload = serde_json::to_vec(&unpublish_req).unwrap_or_default();
        let peers = self.peers.read().await.clone();
        for (peer_id, peer_info) in &peers {
            if *peer_id == self.config.node_id {
                continue;
            }
            if let Ok(addr) = peer_info.addr.parse::<SocketAddr>() {
                if let Ok(mut conn) = self.transport.connect(addr).await {
                    let msg = Message::new(MessageType::VolumeUnpublish, payload.clone());
                    let _ = conn.send_message(&msg).await;
                }
            }
        }
    }

    /// Notify all peers to delete their replica data for a volume
    pub async fn notify_volume_delete(&self, volume_id: &str) -> usize {
        info!("Notifying peers to delete volume: {}", volume_id);
        let del_req = DeleteVolumeRequest {
            volume_id: volume_id.to_string(),
        };
        let payload = serde_json::to_vec(&del_req).unwrap_or_default();
        let msg = Message::new(MessageType::DeleteVolume, payload);

        let peers = self.peers.read().await.clone();
        let mut notified = 0usize;
        for (peer_id, peer_info) in &peers {
            if *peer_id == self.config.node_id {
                continue;
            }
            if let Ok(addr) = peer_info.addr.parse::<SocketAddr>() {
                if let Ok(mut conn) = self.transport.connect(addr).await {
                    let _ = conn.send_message(&msg).await;
                    notified += 1;
                }
            }
        }
        info!("Notified {} peers about volume delete: {}", notified, volume_id);
        notified
    }
}

/// Result of a quorum lease request
#[derive(Debug, Clone)]
pub struct QuorumLeaseResult {
    pub granted: bool,
    pub votes_received: u32,
    pub votes_needed: u32,
    pub total_nodes: u32,
    pub conflict: bool,
    pub conflict_info: Option<ConflictInfo>,
}

/// Get free space for a path using btrfs filesystem usage
fn get_free_space(path: &str) -> u64 {
    std::process::Command::new("btrfs")
        .args(["filesystem", "usage", "-b", path])
        .output()
        .ok()
        .and_then(|o| {
            let stdout = String::from_utf8_lossy(&o.stdout);
            for line in stdout.lines() {
                if line.contains("Free (estimated):") || line.contains("Free:") {
                    let parts: Vec<&str> = line.split_whitespace().collect();
                    if let Some(s) = parts.last() {
                        if let Ok(val) = s.parse::<u64>() {
                            return Some(val);
                        }
                    }
                }
            }
            None
        })
        .unwrap_or(0)
}
