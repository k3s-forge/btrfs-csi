use bytes::{Bytes, BytesMut};
use std::net::SocketAddr;
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::info;

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

/// Runtime-detected cipher kind for adaptive crypto
#[derive(Debug, Clone, Copy, PartialEq)]
enum CipherKind {
    /// AES-256-GCM — hardware accelerated via AES-NI on x86_64
    Aes256Gcm,
    /// XChaCha20-Poly1305 — pure software, no hardware acceleration needed
    XChaCha20,
}

/// Detect AES-NI support at runtime using CPUID
fn detect_hardware_aes() -> bool {
    #[cfg(target_arch = "x86_64")]
    {
        let cpuid = raw_cpuid::CpuId::new();
        if let Some(features) = cpuid.get_feature_info() {
            return features.has_aesni();
        }
        false
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        false // ARM etc. use XChaCha20 by default
    }
}

/// Adaptive payload cipher: runtime-selected between AES-256-GCM and XChaCha20-Poly1305.
/// Prepend nonce (12 or 24 bytes) to ciphertext for transmission.
enum PayloadCipher {
    Aes256Gcm {
        cipher: aes_gcm::Aes256Gcm,
    },
    XChaCha20 {
        cipher: chacha20poly1305::XChaCha20Poly1305,
    },
}

impl PayloadCipher {
    fn new(raw_key: &[u8]) -> Self {
        if detect_hardware_aes() {
            tracing::info!("AES-NI detected: using AES-256-GCM (hardware accelerated)");
            use aes_gcm::KeyInit;
            let key = aes_gcm::Key::<aes_gcm::Aes256Gcm>::from_slice(raw_key);
            PayloadCipher::Aes256Gcm {
                cipher: aes_gcm::Aes256Gcm::new(key),
            }
        } else {
            tracing::info!("No AES-NI: using XChaCha20-Poly1305 (pure software)");
            use chacha20poly1305::aead::KeyInit;
            let key = chacha20poly1305::Key::from_slice(raw_key);
            PayloadCipher::XChaCha20 {
                cipher: chacha20poly1305::XChaCha20Poly1305::new(key),
            }
        }
    }

    fn kind(&self) -> CipherKind {
        match self {
            PayloadCipher::Aes256Gcm { .. } => CipherKind::Aes256Gcm,
            PayloadCipher::XChaCha20 { .. } => CipherKind::XChaCha20,
        }
    }

    fn nonce_len(&self) -> usize {
        match self {
            PayloadCipher::Aes256Gcm { .. } => 12,
            PayloadCipher::XChaCha20 { .. } => 24,
        }
    }

    fn encrypt(&self, plaintext: &[u8]) -> Vec<u8> {
        match self {
            PayloadCipher::Aes256Gcm { cipher } => {
                use aes_gcm::aead::{Aead, generic_array::GenericArray};
                let nonce_bytes: [u8; 12] = rand::random();
                let nonce = GenericArray::from_slice(&nonce_bytes);
                let ciphertext = cipher.encrypt(nonce, plaintext)
                    .expect("AES-256-GCM encryption should not fail");
                let mut out = Vec::with_capacity(12 + ciphertext.len());
                out.extend_from_slice(&nonce_bytes);
                out.extend_from_slice(&ciphertext);
                out
            }
            PayloadCipher::XChaCha20 { cipher } => {
                use chacha20poly1305::aead::Aead;
                let nonce_bytes: [u8; 24] = rand::random();
                let nonce = chacha20poly1305::XNonce::from_slice(&nonce_bytes);
                let ciphertext = cipher.encrypt(nonce, plaintext)
                    .expect("XChaCha20-Poly1305 encryption should not fail");
                let mut out = Vec::with_capacity(24 + ciphertext.len());
                out.extend_from_slice(&nonce_bytes);
                out.extend_from_slice(&ciphertext);
                out
            }
        }
    }

    fn decrypt(&self, data: &[u8]) -> Option<Vec<u8>> {
        let nonce_len = self.nonce_len();
        if data.len() < nonce_len + 16 {
            return None;
        }
        match self {
            PayloadCipher::Aes256Gcm { cipher } => {
                use aes_gcm::aead::{Aead, generic_array::GenericArray};
                let nonce = GenericArray::from_slice(&data[..nonce_len]);
                cipher.decrypt(nonce, &data[nonce_len..]).ok()
            }
            PayloadCipher::XChaCha20 { cipher } => {
                use chacha20poly1305::aead::Aead;
                let nonce = chacha20poly1305::XNonce::from_slice(&data[..nonce_len]);
                cipher.decrypt(nonce, &data[nonce_len..]).ok()
            }
        }
    }
}

/// If the key isn't 64 hex chars or 32 raw bytes, hash it to 32 bytes.
fn normalize_key(key: &[u8]) -> Vec<u8> {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(key);
    hasher.finalize().to_vec()
}

/// TCP transport with HMAC authentication and XChaCha20-Poly1305 payload encryption
pub struct TcpTransport {
    auth: HmacAuth,
    auth_key: Vec<u8>,
}

impl TcpTransport {
    /// Create a new transport with the given authentication key
    ///
    /// Accepts either a 64-char hex string (from `openssl rand -hex 32`) or exactly 32 raw bytes.
    /// For any other length, hashes the key with SHA-256 to produce a 32-byte key.
    pub fn new(key: &[u8]) -> Self {
        let raw_key = if key.len() == 64 {
            hex::decode(key).unwrap_or_else(|_| {
                // Not valid hex; treat as raw bytes, hash to 32 bytes if needed
                normalize_key(key)
            })
        } else if key.len() == 32 {
            key.to_vec()
        } else {
            normalize_key(key)
        };
        Self {
            auth: HmacAuth::new(&raw_key, 30),
            auth_key: raw_key,
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
                            "Payload decryption failed".to_string()
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