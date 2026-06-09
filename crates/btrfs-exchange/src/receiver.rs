use anyhow::{Context, Result};
use btrfs_protocol::message::{Message, MessageType, SendCompleteResponse, SendStartRequest};
use btrfs_protocol::transport::{TcpTransport, TransportConnection};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tracing::{error, info, warn};

use crate::config::ExchangeConfig;

/// Replication receiver: accepts incoming btrfs send streams
pub struct ReplicationReceiver {
    config: ExchangeConfig,
    transport: TcpTransport,
}

impl ReplicationReceiver {
    pub fn new(config: ExchangeConfig) -> Self {
        let transport = TcpTransport::new(config.auth_key.as_bytes());
        Self { config, transport }
    }

    /// Start listening for replication connections
    pub async fn start(&self) -> Result<()> {
        let addr: SocketAddr = format!(
            "{}:{}",
            self.config.listen_addr, self.config.replication_port
        )
        .parse()
        .context("Invalid listen address")?;

        let listener = self.transport.listen(addr).await?;
        info!("Replication receiver listening on {}", addr);

        loop {
            match self.transport.accept(&listener).await {
                Ok(conn) => {
                    let config = self.config.clone();
                    tokio::spawn(async move {
                        if let Err(e) = Self::handle_connection(config, conn).await {
                            error!("Replication connection error: {}", e);
                        }
                    });
                }
                Err(e) => {
                    warn!("Failed to accept connection: {}", e);
                    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                }
            }
        }
    }

    /// Handle a single replication connection
    async fn handle_connection(
        config: ExchangeConfig,
        mut conn: TransportConnection,
    ) -> Result<()> {
        let peer = conn.peer_addr()?;
        info!("Handling replication from {}", peer);

        // Wait for SendStart
        let start_msg = conn.recv_message().await?;
        if start_msg.msg_type != MessageType::SendStart {
            return Err(anyhow::anyhow!(
                "Expected SendStart, got {:?}",
                start_msg.msg_type
            ));
        }

        let start_req: SendStartRequest = serde_json::from_slice(&start_msg.payload)?;
        info!(
            "Receiving volume: {} (incremental={})",
            start_req.volume_id, start_req.is_incremental
        );

        // Determine receive path (use snapshot_dir for received data)
        let receive_path = format!("{}/{}", config.replication.snapshot_dir, start_req.volume_id);

        // Start btrfs receive
        let mut cmd = tokio::process::Command::new("btrfs");
        cmd.args(["receive", &receive_path]);

        let mut child = cmd
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .context("Failed to spawn btrfs receive")?;

        let stdin = child.stdin.take().context("Failed to capture stdin")?;
        let mut stdin_writer = tokio::io::BufWriter::new(stdin);

        // Send acknowledgment
        let ack = Message::new(MessageType::SendStart, vec![]);
        conn.send_message(&ack).await?;

        // Receive data chunks and pipe to btrfs receive
        let mut total_bytes: u64 = 0;
        let mut chunk_count: u64 = 0;
        let mut hasher = blake3::Hasher::new();

        loop {
            match conn.recv_message().await {
                Ok(msg) => match msg.msg_type {
                    MessageType::SendData => {
                        hasher.update(&msg.payload);
                        stdin_writer.write_all(&msg.payload).await?;
                        total_bytes += msg.payload.len() as u64;
                        chunk_count += 1;

                        if chunk_count % 100 == 0 {
                            info!(
                                "Receiving {}: {} bytes ({} chunks)",
                                start_req.volume_id, total_bytes, chunk_count
                            );
                        }
                    }
                    MessageType::SendComplete => {
                        info!(
                            "Send complete for {}: {} bytes ({} chunks)",
                            start_req.volume_id, total_bytes, chunk_count
                        );
                        break;
                    }
                    MessageType::SendError => {
                        let error_msg = String::from_utf8_lossy(&msg.payload);
                        error!("Send error for {}: {}", start_req.volume_id, error_msg);
                        drop(stdin_writer);
                        let _ = child.kill().await;
                        return Err(anyhow::anyhow!("Remote send error: {}", error_msg));
                    }
                    _ => {
                        warn!("Unexpected message type: {:?}", msg.msg_type);
                    }
                },
                Err(e) => {
                    error!("Connection lost during receive: {}", e);
                    drop(stdin_writer);
                    let _ = child.kill().await;
                    return Err(e.into());
                }
            }
        }

        // Close stdin to signal EOF to btrfs receive
        drop(stdin_writer);

        // Wait for btrfs receive to finish
        let status = child.wait().await?;
        if !status.success() {
            let mut stderr_buf = Vec::new();
            if let Some(mut s) = child.stderr.take() {
                use tokio::io::AsyncReadExt;
                let _ = s.read_to_end(&mut stderr_buf).await;
            }
            let stderr = String::from_utf8_lossy(&stderr_buf);
            return Err(anyhow::anyhow!("btrfs receive failed: {}", stderr));
        }

        // Compute checksum for verification
        let checksum = hasher.finalize().to_hex().to_string();
        info!(
            "Volume {} received successfully ({} bytes, checksum={})",
            start_req.volume_id, total_bytes, checksum
        );

        // Send completion with checksum
        let complete = SendCompleteResponse {
            volume_id: start_req.volume_id,
            success: true,
            error: None,
            checksum: Some(checksum),
        };
        let complete_msg = Message::new(
            MessageType::SendComplete,
            serde_json::to_vec(&complete)?,
        );
        conn.send_message(&complete_msg).await?;

        Ok(())
    }
}
