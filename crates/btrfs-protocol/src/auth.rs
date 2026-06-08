use hmac::{Hmac, Mac};
use sha2::Sha256;
use thiserror::Error;

type HmacSha256 = Hmac<Sha256>;

/// Errors that can occur during authentication
#[derive(Debug, Error)]
pub enum AuthError {
    #[error("Invalid HMAC signature")]
    InvalidSignature,

    #[error("Timestamp expired")]
    TimestampExpired,

    #[error("Invalid timestamp")]
    InvalidTimestamp,

    #[error("Key generation failed")]
    KeyGenerationFailed,
}

/// HMAC-based authentication for cluster communication
pub struct HmacAuth {
    key: Vec<u8>,
    max_age_secs: i64,
}

impl HmacAuth {
    /// Create a new authenticator with the given key
    pub fn new(key: &[u8], max_age_secs: i64) -> Self {
        Self {
            key: key.to_vec(),
            max_age_secs,
        }
    }

    /// Generate authentication token
    /// Returns: (timestamp, hmac_signature)
    pub fn generate_token(&self) -> (i64, Vec<u8>) {
        let timestamp = chrono::Utc::now().timestamp_millis();
        let signature = self.compute_hmac(timestamp);
        (timestamp, signature)
    }

    /// Validate authentication token
    pub fn validate_token(&self, timestamp: i64, signature: &[u8]) -> Result<(), AuthError> {
        // Check timestamp freshness
        let now = chrono::Utc::now().timestamp_millis();
        let age = (now - timestamp).abs() / 1000;

        if age > self.max_age_secs {
            return Err(AuthError::TimestampExpired);
        }

        // Verify HMAC
        let expected = self.compute_hmac(timestamp);
        if signature != expected.as_slice() {
            return Err(AuthError::InvalidSignature);
        }

        Ok(())
    }

    /// Compute HMAC signature for a given timestamp
    fn compute_hmac(&self, timestamp: i64) -> Vec<u8> {
        let mut mac =
            HmacSha256::new_from_slice(&self.key).expect("HMAC can take key of any size");
        mac.update(&timestamp.to_be_bytes());
        mac.finalize().into_bytes().to_vec()
    }

    /// Generate a random key for cluster authentication
    pub fn generate_key() -> Result<Vec<u8>, AuthError> {
        use rand::RngCore;
        let mut key = vec![0u8; 32];
        rand::thread_rng().fill_bytes(&mut key);
        Ok(key)
    }

    /// Serialize auth payload: [timestamp: 8 bytes][hmac: 32 bytes]
    pub fn serialize_auth_payload(timestamp: i64, hmac: &[u8]) -> Vec<u8> {
        let mut payload = Vec::with_capacity(40);
        payload.extend_from_slice(&timestamp.to_be_bytes());
        payload.extend_from_slice(hmac);
        payload
    }

    /// Deserialize auth payload
    pub fn deserialize_auth_payload(data: &[u8]) -> Result<(i64, Vec<u8>), AuthError> {
        if data.len() < 40 {
            return Err(AuthError::InvalidSignature);
        }

        let timestamp = i64::from_be_bytes(data[0..8].try_into().unwrap());
        let hmac = data[8..40].to_vec();

        Ok((timestamp, hmac))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_and_validate_token() {
        let key = HmacAuth::generate_key().unwrap();
        let auth = HmacAuth::new(&key, 30);

        let (timestamp, signature) = auth.generate_token();
        assert!(auth.validate_token(timestamp, &signature).is_ok());
    }

    #[test]
    fn test_invalid_signature() {
        let key = HmacAuth::generate_key().unwrap();
        let auth = HmacAuth::new(&key, 30);

        let (timestamp, _) = auth.generate_token();
        let wrong_signature = vec![0u8; 32];

        assert!(auth.validate_token(timestamp, &wrong_signature).is_err());
    }

    #[test]
    fn test_expired_timestamp() {
        let key = HmacAuth::generate_key().unwrap();
        let auth = HmacAuth::new(&key, 1); // 1 second max age

        let (timestamp, signature) = auth.generate_token();
        // Simulate old timestamp
        let old_timestamp = timestamp - 5000; // 5 seconds ago
        assert!(auth.validate_token(old_timestamp, &signature).is_err());
    }
}
