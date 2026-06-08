use bytes::{Bytes, BytesMut};
use std::net::SocketAddr;
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{error, info, warn};

use crate::auth::HmacAuth;
use crate::message::{Message, MessageType};

/// Errors that can occur during transport operations
#[derive(Debug, Error)]
pub enum TransportError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Connection closed")]
    ConnectionClosed,

    #[error("Authentication failed: {0}")]
    AuthFailed(String),

    #[error("Protocol error: {0}")]
    Protocol(String),

    #[error("Message decode error: {0}")]
    DecodeError(String),
}

/// Result type for transport operations
pub type Result<T> = std::result::Result<T, TransportError>;

/// TCP transport with HMAC authentication
pub struct TcpTransport {
    auth: HmacAuth,
}

impl TcpTransport {
    /// Create a new transport with the given authentication key
    pub fn new(key: &[u8]) -> Self {
        Self {
            auth: HmacAuth::new(key, 30),
        }
    }

    /// Connect to a remote node and authenticate
    pub async fn connect(&self, addr: SocketAddr) -> Result<TransportConnection> {
        let stream = TcpStream::connect(addr).await?;
        let mut conn = TransportConnection::new(stream);

        // Send authentication
        let (timestamp, signature) = self.auth.generate_token();
        let auth_payload = HmacAuth::serialize_auth_payload(timestamp, &signature);

        let msg = Message::new(MessageType::Auth, auth_payload);
        conn.send_message(&msg).await?;

        // Wait for auth response
        let response = conn.recv_message().await?;
        match response.msg_type {
            MessageType::AuthOk => Ok(conn),
            MessageType::AuthFailed => {
                let error_msg = String::from_utf8(response.payload)
                    .unwrap_or_else(|_| "Unknown error".to_string());
                Err(TransportError::AuthFailed(error_msg))
            }
            _ => Err(TransportError::Protocol(
                "Unexpected response to auth".to_string(),
            )),
        }
    }

    /// Start listening for incoming connections
    pub async fn listen(&self, addr: SocketAddr) -> Result<TcpListener> {
        let listener = TcpListener::bind(addr).await?;
        info!("Listening on {}", addr);
        Ok(listener)
    }

    /// Handle an incoming connection with authentication
    pub async fn accept(&self, listener: &TcpListener) -> Result<TransportConnection> {
        let (stream, addr) = listener.accept().await?;
        info!("Accepted connection from {}", addr);

        let mut conn = TransportConnection::new(stream);

        // Wait for auth
        let msg = conn.recv_message().await?;
        match msg.msg_type {
            MessageType::Auth => {
                let (timestamp, signature) =
                    HmacAuth::deserialize_auth_payload(&msg.payload)
                        .map_err(|e| TransportError::AuthFailed(e.to_string()))?;

                match self.auth.validate_token(timestamp, &signature) {
                    Ok(()) => {
                        let response = Message::new(MessageType::AuthOk, vec![]);
                        conn.send_message(&response).await?;
                        Ok(conn)
                    }
                    Err(e) => {
                        let response = Message::new(
                            MessageType::AuthFailed,
                            e.to_string().into_bytes(),
                        );
                        conn.send_message(&response).await?;
                        Err(TransportError::AuthFailed(e.to_string()))
                    }
                }
            }
            _ => Err(TransportError::Protocol(
                "Expected auth message".to_string(),
            )),
        }
    }
}

/// A verified transport connection
pub struct TransportConnection {
    stream: TcpStream,
    read_buf: BytesMut,
}

impl TransportConnection {
    fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            read_buf: BytesMut::with_capacity(64 * 1024),
        }
    }

    /// Send a message
    pub async fn send_message(&mut self, msg: &Message) -> Result<()> {
        let data = msg.encode();
        self.stream.write_all(&data).await?;
        self.stream.flush().await?;
        Ok(())
    }

    /// Receive a message
    pub async fn recv_message(&mut self) -> Result<Message> {
        loop {
            // Try to decode from buffer
            if let Some(msg) = Message::decode(&mut self.read_buf)? {
                return Ok(msg);
            }

            // Read more data
            let n = self
                .stream
                .read_buf(&mut self.read_buf)
                .await
                .map_err(TransportError::Io)?;

            if n == 0 {
                return Err(TransportError::ConnectionClosed);
            }
        }
    }

    /// Get the peer address
    pub fn peer_addr(&self) -> Result<SocketAddr> {
        self.stream.peer_addr().map_err(TransportError::Io)
    }

    /// Close the connection
    pub async fn close(&mut self) -> Result<()> {
        self.stream.shutdown().await?;
        Ok(())
    }
}
