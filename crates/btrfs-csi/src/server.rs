use anyhow::Result;
use btrfs_protocol::message::{Message, MessageType};
use btrfs_protocol::transport::{TcpTransport, TransportConnection};
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tracing::{error, info, warn};

use crate::driver::BtrfsCsiDriver;

/// CSI server handling incoming requests
pub struct CsiServer {
    endpoint: String,
    driver: BtrfsCsiDriver,
}

impl CsiServer {
    /// Create a new CSI server
    pub fn new(endpoint: String, driver: BtrfsCsiDriver) -> Self {
        Self { endpoint, driver }
    }

    /// Start serving requests
    pub async fn serve(&self) -> Result<()> {
        info!("CSI server starting on {}", self.endpoint);

        // Parse endpoint - support both TCP and unix socket formats
        let addr: SocketAddr = if self.endpoint.starts_with("unix://") {
            // For unix sockets, extract the path and log a warning
            let path = self.endpoint.trim_start_matches("unix://");
            warn!(
                "Unix socket requested ({}), falling back to TCP on port 9201",
                path
            );
            "0.0.0.0:9201".parse()?
        } else if self.endpoint.contains(':') {
            // TCP endpoint like "0.0.0.0:9201"
            self.endpoint.parse()?
        } else {
            // Just a port number
            format!("0.0.0.0:{}", self.endpoint).parse()?
        };

        // Create transport
        let transport = TcpTransport::new(self.driver.config.auth_key.as_bytes());

        // Listen for connections
        let listener = transport.listen(addr).await?;

        info!("CSI server listening on {}", addr);

        // Accept connections
        loop {
            match transport.accept(&listener).await {
                Ok(conn) => {
                    let driver = self.driver.clone();
                    tokio::spawn(async move {
                        if let Err(e) = Self::handle_connection(conn, driver).await {
                            error!("Connection handler error: {}", e);
                        }
                    });
                }
                Err(e) => {
                    error!("Failed to accept connection: {}", e);
                }
            }
        }
    }

    /// Handle a single connection
    async fn handle_connection(
        mut conn: TransportConnection,
        driver: BtrfsCsiDriver,
    ) -> Result<()> {
        info!("New CSI connection from {}", conn.peer_addr()?);

        loop {
            match conn.recv_message().await {
                Ok(msg) => {
                    let response = Self::handle_message(msg, &driver).await?;
                    if let Some(resp) = response {
                        conn.send_message(&resp).await?;
                    }
                }
                Err(btrfs_protocol::transport::TransportError::ConnectionClosed) => {
                    info!("Connection closed");
                    break;
                }
                Err(e) => {
                    error!("Error receiving message: {}", e);
                    break;
                }
            }
        }

        Ok(())
    }

    /// Handle a single message
    async fn handle_message(
        msg: Message,
        driver: &BtrfsCsiDriver,
    ) -> Result<Option<Message>> {
        match msg.msg_type {
            MessageType::CreateVolume => {
                let req: CreateVolumeRequest = serde_json::from_slice(&msg.payload)?;
                let vol = driver
                    .create_volume(&req.name, req.size, req.replica_count)
                    .await?;

                let resp = CreateVolumeResponse {
                    volume_id: vol.id,
                    node_id: vol.node_id,
                    path: format!("{}/{}", driver.config.replication.data_dir, vol.name),
                };

                let payload = serde_json::to_vec(&resp)?;
                Ok(Some(Message::new(MessageType::CreateVolumeAck, payload)))
            }
            MessageType::DeleteVolume => {
                let req: DeleteVolumeRequest = serde_json::from_slice(&msg.payload)?;
                driver.delete_volume(&req.volume_id).await?;

                let resp = DeleteVolumeResponse {
                    success: true,
                    error: None,
                };

                let payload = serde_json::to_vec(&resp)?;
                Ok(Some(Message::new(MessageType::DeleteVolumeAck, payload)))
            }
            MessageType::GetVolumeInfo => {
                let req: GetVolumeInfoRequest = serde_json::from_slice(&msg.payload)?;

                match driver.get_volume(&req.volume_id).await {
                    Some(vol) => {
                        let payload = serde_json::to_vec(&vol)?;
                        Ok(Some(Message::new(MessageType::GetVolumeInfoAck, payload)))
                    }
                    None => {
                        let resp = GetVolumeInfoResponse {
                            error: "Volume not found".to_string(),
                        };
                        let payload = serde_json::to_vec(&resp)?;
                        Ok(Some(Message::new(MessageType::GetVolumeInfoAck, payload)))
                    }
                }
            }
            MessageType::VolumeList => {
                let volumes = driver.list_volumes().await;
                let payload = serde_json::to_vec(&volumes)?;
                Ok(Some(Message::new(MessageType::VolumeListAck, payload)))
            }
            _ => {
                warn!("Unhandled message type: {:?}", msg.msg_type);
                Ok(None)
            }
        }
    }
}

// Request/Response types

#[derive(serde::Deserialize)]
struct CreateVolumeRequest {
    name: String,
    size: u64,
    replica_count: u32,
}

#[derive(serde::Serialize)]
struct CreateVolumeResponse {
    volume_id: String,
    node_id: String,
    path: String,
}

#[derive(serde::Deserialize)]
struct DeleteVolumeRequest {
    volume_id: String,
}

#[derive(serde::Serialize)]
struct DeleteVolumeResponse {
    success: bool,
    error: Option<String>,
}

#[derive(serde::Deserialize)]
struct GetVolumeInfoRequest {
    volume_id: String,
}

#[derive(serde::Serialize)]
struct GetVolumeInfoResponse {
    error: String,
}

// Clone implementation for driver
impl Clone for BtrfsCsiDriver {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            gossip: self.gossip.clone(),
            replicator: self.replicator.clone(),
            volume_manager: self.volume_manager.clone(),
            scheduler: self.scheduler.clone(),
        }
    }
}
