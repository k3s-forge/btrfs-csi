use anyhow::Result;
use btrfs_protocol::message::{HeartbeatPayload, Message, MessageType, NodeInfo, VolumeInfo};
use btrfs_protocol::transport::TcpTransport;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

use crate::config::ExchangeConfig;

/// Callback type for node failure events
pub type NodeFailureCallback = Arc<dyn Fn(String) + Send + Sync>;

/// Gossip service for node discovery and state synchronization
pub struct GossipService {
    config: ExchangeConfig,
    transport: TcpTransport,
    peers: Arc<RwLock<HashMap<String, NodeInfo>>>,
    local_volumes: Arc<RwLock<Vec<VolumeInfo>>>,
    on_node_failure: Arc<RwLock<Vec<NodeFailureCallback>>>,
}

impl GossipService {
    /// Create a new gossip service
    pub fn new(config: ExchangeConfig) -> Self {
        let transport = TcpTransport::new(config.auth_key.as_bytes());

        Self {
            config,
            transport,
            peers: Arc::new(RwLock::new(HashMap::new())),
            local_volumes: Arc::new(RwLock::new(Vec::new())),
            on_node_failure: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Register a callback for node failure events
    pub async fn on_node_failure(&self, callback: NodeFailureCallback) {
        self.on_node_failure.write().await.push(callback);
    }

    /// Start the gossip service
    pub async fn start(&self) -> Result<()> {
        info!(
            "Starting gossip service on {}:{}",
            self.config.listen_addr, self.config.listen_port
        );

        // Start TCP listener for incoming gossip messages
        let config = self.config.clone();
        let transport = TcpTransport::new(config.auth_key.as_bytes());
        let peers = self.peers.clone();
        let on_node_failure = self.on_node_failure.clone();
        let local_volumes = self.local_volumes.clone();

        tokio::spawn(async move {
            Self::listener_loop(config, transport, peers, on_node_failure, local_volumes).await;
        });

        // Start heartbeat sender
        let config = self.config.clone();
        let transport = TcpTransport::new(config.auth_key.as_bytes());
        let peers = self.peers.clone();
        let local_volumes = self.local_volumes.clone();

        tokio::spawn(async move {
            Self::heartbeat_loop(config, transport, peers, local_volumes).await;
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
        local_volumes: Arc<RwLock<Vec<VolumeInfo>>>,
    ) {
        let addr: SocketAddr = match format!("{}:{}", config.listen_addr, config.listen_port).parse() {
            Ok(a) => a,
            Err(e) => {
                error!("Invalid gossip listen address: {}", e);
                return;
            }
        };

        let listener = match transport.listen(addr).await {
            Ok(l) => l,
            Err(e) => {
                error!("Failed to bind gossip listener on {}: {}", addr, e);
                return;
            }
        };

        info!("Gossip listener ready on {}", addr);

        loop {
            match transport.accept(&listener).await {
                Ok(mut conn) => {
                    let config = config.clone();
                    let peers = peers.clone();
                    let on_node_failure = on_node_failure.clone();
                    let local_volumes = local_volumes.clone();

                    tokio::spawn(async move {
                        if let Err(e) = Self::handle_gossip_message(
                            &config, &mut conn, &peers, &on_node_failure, &local_volumes,
                        ).await {
                            debug!("Gossip message handler error: {}", e);
                        }
                    });
                }
                Err(e) => {
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
        local_volumes: &Arc<RwLock<Vec<VolumeInfo>>>,
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
        local_volumes: Arc<RwLock<Vec<VolumeInfo>>>,
    ) {
        let mut interval = tokio::time::interval(config.heartbeat_interval_duration());

        loop {
            interval.tick().await;

            let payload = {
                let volumes = local_volumes.read().await;
                let heartbeat = HeartbeatPayload {
                    node_id: config.node_id.clone(),
                    addr: format!("{}:{}", config.listen_addr, config.listen_port),
                    zone: config.zone.clone(),
                    role: "replica".to_string(),
                    free_space: get_free_space(&config.replication.data_dir),
                    volumes: volumes.clone(),
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
            volumes: Vec::new(),
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

    /// Update local volumes
    pub async fn update_volumes(&self, volumes: Vec<VolumeInfo>) {
        *self.local_volumes.write().await = volumes;
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
