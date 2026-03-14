//! Cryptographic identity management.

use std::path::Path;

use crate::error::BoloError;
use crate::types::BoloNodeId;

/// A node's cryptographic identity (ed25519 keypair).
pub struct Identity {
    secret_key: iroh::SecretKey,
}

impl Identity {
    /// Generate a new random identity.
    pub fn generate() -> Self {
        Self {
            secret_key: iroh::SecretKey::generate(&mut rand::rng()),
        }
    }

    /// Get the public node ID.
    pub fn node_id(&self) -> BoloNodeId {
        BoloNodeId(self.secret_key.public())
    }

    /// Get a reference to the secret key.
    pub fn secret_key(&self) -> &iroh::SecretKey {
        &self.secret_key
    }

    /// Sign data with this identity's secret key.
    pub fn sign(&self, data: &[u8]) -> iroh::Signature {
        self.secret_key.sign(data)
    }

    /// Verify a signature against a public key.
    pub fn verify(
        public_key: &iroh::PublicKey,
        data: &[u8],
        signature: &iroh::Signature,
    ) -> Result<(), BoloError> {
        public_key
            .verify(data, signature)
            .map_err(|_| BoloError::PeerUnreachable("signature verification failed".to_string()))
    }

    /// Load identity from a key file (hex-encoded secret key).
    pub fn load(path: &Path) -> Result<Self, BoloError> {
        if !path.exists() {
            return Err(BoloError::IdentityNotFound(format!(
                "key file not found: {}",
                path.display()
            )));
        }
        let contents = std::fs::read_to_string(path)?;
        let bytes = hex_decode(contents.trim())?;
        let byte_array: [u8; 32] = bytes.try_into().map_err(|_| {
            BoloError::IdentityNotFound(
                "key file must contain exactly 32 bytes (64 hex chars)".into(),
            )
        })?;
        let secret_key = iroh::SecretKey::from_bytes(&byte_array);
        Ok(Self { secret_key })
    }

    /// Save identity to a key file (hex-encoded secret key).
    pub fn save(&self, path: &Path) -> Result<(), BoloError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(path, hex_encode(&self.secret_key.to_bytes()))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
        }
        Ok(())
    }

    /// Load identity from the standard config directory location.
    pub fn load_from_config_dir(config_dir: &Path) -> Result<Self, BoloError> {
        let config_path = config_dir.join("config.toml");
        let config = if config_path.exists() {
            crate::BoloConfig::load(Some(&config_path))?
        } else {
            crate::BoloConfig::default()
        };
        let key_path = config_dir.join(&config.identity.key_file);
        Self::load(&key_path)
    }
}

/// Encode bytes as a hex string.
pub fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Decode a hex string into bytes.
pub fn hex_decode(s: &str) -> Result<Vec<u8>, BoloError> {
    if s.len() % 2 != 0 {
        return Err(BoloError::InvalidPath("odd-length hex string".to_string()));
    }
    (0..s.len())
        .step_by(2)
        .map(|i| {
            u8::from_str_radix(&s[i..i + 2], 16)
                .map_err(|e| BoloError::InvalidPath(format!("invalid hex: {e}")))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_and_sign_verify() {
        let identity = Identity::generate();
        let data = b"hello bolo";
        let sig = identity.sign(data);
        let pubkey = identity.node_id().0;
        assert!(Identity::verify(&pubkey, data, &sig).is_ok());
    }

    #[test]
    fn generate_and_sign_verify_bad_data() {
        let identity = Identity::generate();
        let sig = identity.sign(b"hello");
        let pubkey = identity.node_id().0;
        assert!(Identity::verify(&pubkey, b"wrong", &sig).is_err());
    }

    #[test]
    fn save_and_load_roundtrip() {
        let identity = Identity::generate();
        let dir = std::env::temp_dir().join(format!("bolo-test-{}", rand::random::<u64>()));
        let key_path = dir.join("test.key");

        identity.save(&key_path).unwrap();
        let loaded = Identity::load(&key_path).unwrap();

        assert_eq!(identity.node_id(), loaded.node_id());

        // Clean up
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn hex_roundtrip() {
        let bytes = [0xde, 0xad, 0xbe, 0xef];
        let encoded = hex_encode(&bytes);
        assert_eq!(encoded, "deadbeef");
        let decoded = hex_decode(&encoded).unwrap();
        assert_eq!(decoded, bytes);
    }
}
