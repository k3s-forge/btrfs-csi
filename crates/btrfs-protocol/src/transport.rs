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

/// AES-256-GCM payload encryptor
struct PayloadCipher {
    cipher: aes_gcm::Aes256Gcm,
}

impl PayloadCipher {
    fn new(auth_key: &[u8]) -> Self {
        use sha2::Digest;
        let hash = sha2::Sha256::digest(auth_key);
        let key = aes_gcm::Key::<aes_gcm::Aes256Gcm>::from_slice(&hash);
        Self { cipher: aes_gcm::Aes256Gcm::new(key) }
    }

    fn encrypt(&self, plaintext: &[u8]) -> Vec<u8> {
        use aes_gcm::aead::Aead;
        let nonce_bytes: [u8; 12] = rand::random();
        let nonce = aes_gcm::Nonce::from_slice(&nonce_bytes);
        let ciphertext = self.cipher.encrypt(nonce, plaintext)
            .expect("AES-GCM encryption should not fail");
        let mut out = Vec::with_capacity(12 + ciphertext.len());
        out.extend_from_slice(&nonce_bytes);
        out.extend_from_slice(&ciphertext);
        out
    }

    fn decrypt(&self, data: &[u8]) -> Option<Vec<u8>> {
        use aes_gcm::aead::Aead;
        if data.len() < 12 + 16 {
            return None;
        }
        let nonce = aes_gcm::Nonce::from_slice(&data[..12]);
        self.cipher.decrypt(nonce, &data[12..]).ok()
    }
}

/// TCP transport with HMAC authentication and AES-256-GCM payload encryption
pub struct TcpTransport {
    auth: HmacAuth,
    auth_key: Vec<u8>,
}

impl TcpTransport {
    /// Create a new transport with the given authentication key
    pub fn new(key: &[u8]) -> Self {
        Self {
            auth: HmacAuth::new(key, 30),
            auth_key: key.to_vec(),
        }
    }

    /// Connect to a remote node and authenticate
    pub async fn connect(&self, addr: SocketAddr) -> Result<TransportConnection> {
        let stream = TcpStream::connect(addr).await?;
        let mut conn = TransportConnection::new(stream);

        // Send authentication (cleartext)
        let (timestamp, signature) = self.auth.generate_token();
        let auth_payload = HmacAuth::serialize_auth_payload(timestamp, &signature);

        let msg = Message::new(MessageType::Auth, auth_payload);
        conn.send_message(&msg).await?;

        // Wait for auth response (cleartext)
        let response = conn.recv_message().await?;
        match response.msg_type {
            MessageType::AuthOk => {
                // Enable encryption for all subsequent messages
                conn.cipher = Some(PayloadCipher::new(&self.auth_key));
                Ok(conn)
            }
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

        // Wait for auth (cleartext)
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
                        // Enable encryption for all subsequent messages
                        conn.cipher = Some(PayloadCipher::new(&self.auth_key));
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

/// A verified, encrypted transport connection
pub struct TransportConnection {
    stream: TcpStream,
    read_buf: BytesMut,
    cipher: Option<PayloadCipher>,
}

impl TransportConnection {
    fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            read_buf: BytesMut::with_capacity(64 * 1024),
            cipher: None,
        }
    }

    /// Send a message (payload encrypted if cipher is active)
    pub async fn send_message(&mut self, msg: &Message) -> Result<()> {
        let payload = match &self.cipher {
            Some(cipher) => cipher.encrypt(&msg.payload),
            None => msg.payload.clone(),
        };
        let wire_msg = Message {
            msg_type: msg.msg_type,
            payload,
            timestamp: msg.timestamp,
        };
        let data = wire_msg.encode();
        self.stream.write_all(&data).await?;
        self.stream.flush().await?;
        Ok(())
    }

    /// Receive a message (payload decrypted if cipher is active)
    pub async fn recv_message(&mut self) -> Result<Message> {
        loop {
            if let Some(wire) = Message::decode(&mut self.read_buf)? {
                let payload = match &self.cipher {
                    Some(cipher) => cipher.decrypt(&wire.payload)
                        .ok_or_else(|| TransportError::Protocol(
                            "AES-GCM decryption failed".to_string()
                        ))?,
                    None => wire.payload,
                };
                return Ok(Message {
                    msg_type: wire.msg_type,
                    payload,
                    timestamp: wire.timestamp,
                });
            }

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