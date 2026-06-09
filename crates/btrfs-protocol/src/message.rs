use bytes::{Buf, BufMut, BytesMut};
use serde::{Deserialize, Serialize};
use std::io;

/// Protocol magic bytes: "BTRF"
const MAGIC: [u8; 4] = [0x42, 0x54, 0x52, 0x46];

/// Current protocol version
const PROTOCOL_VERSION: u16 = 1;

/// Message type identifiers
#[repr(u16)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MessageType {
    // Authentication
    Auth = 0x0001,
    AuthOk = 0x0002,
    AuthFailed = 0x0003,

    // Heartbeat
    Heartbeat = 0x0100,
    HeartbeatAck = 0x0101,

    // Volume operations
    CreateVolume = 0x0200,
    CreateVolumeAck = 0x0201,
    DeleteVolume = 0x0202,
    DeleteVolumeAck = 0x0203,
    GetVolumeInfo = 0x0204,
    GetVolumeInfoAck = 0x0205,

    // Replication
    SendStart = 0x0300,
    SendData = 0x0301,
    SendComplete = 0x0302,
    SendError = 0x0303,
    SendIncrementalStart = 0x0304,

    // Node discovery
    NodeJoin = 0x0400,
    NodeLeave = 0x0401,
    NodeList = 0x0402,
    NodeListAck = 0x0403,

    // State sync
    StateSync = 0x0500,
    StateSyncAck = 0x0501,
    VolumeList = 0x0502,
    VolumeListAck = 0x0503,
}

/// Network message format
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub msg_type: MessageType,
    pub payload: Vec<u8>,
    pub timestamp: i64,
}

impl Message {
    pub fn new(msg_type: MessageType, payload: Vec<u8>) -> Self {
        Self {
            msg_type,
            payload,
            timestamp: chrono::Utc::now().timestamp_millis(),
        }
    }

    /// Serialize message to bytes
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();

        // Magic (4 bytes)
        buf.put_slice(&MAGIC);

        // Version (2 bytes)
        buf.put_u16(PROTOCOL_VERSION);

        // Message type (2 bytes)
        buf.put_u16(self.msg_type as u16);

        // Payload length (4 bytes)
        buf.put_u32(self.payload.len() as u32);

        // Timestamp (8 bytes)
        buf.put_i64(self.timestamp);

        // Payload
        buf.put_slice(&self.payload);

        buf.freeze()
    }

    /// Deserialize message from bytes
    pub fn decode(buf: &mut BytesMut) -> io::Result<Option<Self>> {
        // Need at least header size
        if buf.len() < 20 {
            return Ok(None);
        }

        // Check magic
        if buf[0..4] != MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Invalid magic bytes",
            ));
        }

        let version = u16::from_be_bytes([buf[4], buf[5]]);
        if version != PROTOCOL_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Unsupported protocol version: {}", version),
            ));
        }

        let msg_type = u16::from_be_bytes([buf[6], buf[7]]);
        let payload_len = u32::from_be_bytes([buf[8], buf[9], buf[10], buf[11]]) as usize;
        let timestamp = i64::from_be_bytes([
            buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18], buf[19],
        ]);

        // Check if we have the full message
        if buf.len() < 20 + payload_len {
            return Ok(None);
        }

        // Extract payload
        let payload = buf[20..20 + payload_len].to_vec();
        buf.advance(20 + payload_len);

        let msg_type = match msg_type {
            0x0001 => MessageType::Auth,
            0x0002 => MessageType::AuthOk,
            0x0003 => MessageType::AuthFailed,
            0x0100 => MessageType::Heartbeat,
            0x0101 => MessageType::HeartbeatAck,
            0x0200 => MessageType::CreateVolume,
            0x0201 => MessageType::CreateVolumeAck,
            0x0202 => MessageType::DeleteVolume,
            0x0203 => MessageType::DeleteVolumeAck,
            0x0204 => MessageType::GetVolumeInfo,
            0x0205 => MessageType::GetVolumeInfoAck,
            0x0300 => MessageType::SendStart,
            0x0301 => MessageType::SendData,
            0x0302 => MessageType::SendComplete,
            0x0303 => MessageType::SendError,
            0x0304 => MessageType::SendIncrementalStart,
            0x0400 => MessageType::NodeJoin,
            0x0401 => MessageType::NodeLeave,
            0x0402 => MessageType::NodeList,
            0x0403 => MessageType::NodeListAck,
            0x0500 => MessageType::StateSync,
            0x0501 => MessageType::StateSyncAck,
            0x0502 => MessageType::VolumeList,
            0x0503 => MessageType::VolumeListAck,
            _ => return Err(io::Error::new(io::ErrorKind::InvalidData, "Unknown message type")),
        };

        Ok(Some(Message {
            msg_type,
            payload,
            timestamp,
        }))
    }
}

// Volume-related message types

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateVolumeRequest {
    pub name: String,
    pub size: u64,
    pub replica_count: u32,
    pub replica_zones: Vec<String>,
    pub replication_interval: u64, // seconds
    pub volume_type: String,       // "general", "database"
    pub database_type: Option<String>, // "sqlite", "postgresql", etc.
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateVolumeResponse {
    pub volume_id: String,
    pub node_id: String,
    pub path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteVolumeRequest {
    pub volume_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeInfo {
    pub id: String,
    pub name: String,
    pub size: u64,
    pub node_id: String,
    pub zone: String,
    pub status: String,
    pub created_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    pub id: String,
    pub addr: String,
    pub zone: String,
    pub role: String, // "primary", "replica"
    pub free_space: u64,
    pub last_seen: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatPayload {
    pub node_id: String,
    pub addr: String,
    pub zone: String,
    pub role: String,
    pub free_space: u64,
    pub volumes: Vec<VolumeInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SendStartRequest {
    pub volume_id: String,
    pub snapshot_name: String,
    pub is_incremental: bool,
    pub parent_snapshot: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SendCompleteResponse {
    pub volume_id: String,
    pub success: bool,
    pub error: Option<String>,
    pub checksum: Option<String>,
}
