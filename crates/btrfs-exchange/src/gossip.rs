use anyhow::Result;
use btrfs_protocol::message::{HeartbeatPayload, Message, MessageType, NodeInfo, VolumeInfo};
use btrfs_protocol::transport::TcpTransport;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

use crate::config::ExchangeConfig;

/// Gossip service for node discovery and state synchronization
pub struct GossipService {
    config: ExchangeConfig,
    transport: TcpTransport,
    peers: Arc<RwLock<HashMap<String, NodeInfo>>>,
    local_volumes: Arc<RwLock<Vec<VolumeInfo>>>,
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
        }
    }

    /// Start the gossip service
    pub async fn start(&self) -> Result<()> {
        info!(
            "Starting gossip service on {}:{}",
            self.config.listen_addr, self.config.listen_port
        );

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

        tokio::spawn(async move {
            Self::peer_cleanup_loop(config, peers).await;
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

    /// Broadcast heartbeat to all peers
    async fn heartbeat_loop(
        config: ExchangeConfig,
        transport: TcpTransport,
        peers: Arc<RwLock<HashMap<String, NodeInfo>>>,
        local_volumes: Arc<RwLock<Vec<VolumeInfo>>>,
    ) {
        let mut interval = tokio::time::interval(config.heartbeat_interval);

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
    ) {
        let mut interval = tokio::time::interval(config.node_timeout);

        loop {
            interval.tick().await;

            let now = chrono::Utc::now().timestamp_millis();
            let timeout = config.node_timeout.as_millis() as i64;

            let mut peers = peers.write().await;
            let stale_nodes: Vec<String> = peers
                .iter()
                .filter(|(_, node)| now - node.last_seen > timeout)
                .map(|(id, _)| id.clone())
                .collect();

            for node_id in stale_nodes {
                warn!("Removing stale node: {}", node_id);
                peers.remove(&node_id);
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
    }
}

/// Get free space for a path
fn get_free_space(path: &str) -> u64 {
    std::fs::metadata(path)
        .ok()
        .and_then(|_| {
            // This is a simplified version
            // In production, use statvfs
            Some(1024 * 1024 * 1024) // 1 GB placeholder
        })
        .unwrap_or(0)
}
