//! Cryptographic primitives for channel encryption.
//!
//! Provides symmetric encryption (XChaCha20-Poly1305 AEAD) and key derivation
//! for encrypting gossip payloads, CRDT snapshots, and blob contents.
//!
//! # Wire format
//!
//! Sealed messages are: `nonce (24 bytes) || ciphertext || tag (16 bytes)`.
//! Each call to [`seal`] generates a fresh random nonce.

use chacha20poly1305::aead::{Aead, KeyInit};
use chacha20poly1305::{XChaCha20Poly1305, XNonce};

use crate::error::BoloError;

/// XChaCha20 nonce size in bytes.
const NONCE_SIZE: usize = 24;

/// Poly1305 authentication tag size in bytes.
const TAG_SIZE: usize = 16;

/// A symmetric key for encrypting channel/topic payloads.
///
/// All members of a gossip channel share the same `ChannelKey`.
/// Messages are encrypted with XChaCha20-Poly1305 AEAD using a
/// random 24-byte nonce per message (negligible collision probability).
#[derive(Clone)]
pub struct ChannelKey([u8; 32]);

impl ChannelKey {
    /// Generate a random channel key.
    pub fn generate() -> Self {
        let mut bytes = [0u8; 32];
        rand::Rng::fill(&mut rand::rng(), &mut bytes);
        Self(bytes)
    }

    /// Derive a channel key deterministically from input key material and a context string.
    ///
    /// Uses blake3's key derivation mode for domain separation.
    /// Different contexts produce different keys from the same input.
    pub fn derive(ikm: &[u8; 32], context: &str) -> Self {
        let key = blake3::derive_key(context, ikm);
        Self(key)
    }

    /// Construct from raw bytes.
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Access the raw key bytes.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Encrypt plaintext using this key.
    ///
    /// Returns `nonce (24 bytes) || ciphertext || tag (16 bytes)`.
    pub fn seal(&self, plaintext: &[u8]) -> Result<Vec<u8>, BoloError> {
        seal(plaintext, self)
    }

    /// Decrypt ciphertext using this key.
    ///
    /// Expects the format produced by [`seal`]: `nonce || ciphertext || tag`.
    pub fn open(&self, ciphertext: &[u8]) -> Result<Vec<u8>, BoloError> {
        open(ciphertext, self)
    }
}

impl std::fmt::Debug for ChannelKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("ChannelKey").field(&"[REDACTED]").finish()
    }
}

/// A shared secret derived from a key exchange.
///
/// Use [`SharedSecret::derive_channel_key`] to derive a [`ChannelKey`]
/// for a specific channel or topic.
pub struct SharedSecret([u8; 32]);

impl SharedSecret {
    /// Construct from raw bytes (e.g., from a DH exchange result).
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Access the raw secret bytes.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Derive a channel key from this shared secret and a channel/topic name.
    ///
    /// Different channel names produce different keys from the same shared secret,
    /// providing domain separation.
    pub fn derive_channel_key(&self, channel_name: &str) -> ChannelKey {
        ChannelKey::derive(&self.0, &format!("bolo/channel-key/{channel_name}"))
    }
}

impl std::fmt::Debug for SharedSecret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("SharedSecret").field(&"[REDACTED]").finish()
    }
}

/// Encrypt plaintext with the given channel key.
///
/// Uses XChaCha20-Poly1305 AEAD with a random 24-byte nonce.
/// Returns: `nonce (24 bytes) || ciphertext || tag (16 bytes)`.
pub fn seal(plaintext: &[u8], key: &ChannelKey) -> Result<Vec<u8>, BoloError> {
    let cipher = XChaCha20Poly1305::new_from_slice(&key.0)
        .map_err(|e| BoloError::ConfigError(format!("invalid channel key: {e}")))?;

    let mut nonce_bytes = [0u8; NONCE_SIZE];
    rand::Rng::fill(&mut rand::rng(), &mut nonce_bytes);
    let nonce = XNonce::from_slice(&nonce_bytes);

    let ciphertext = cipher
        .encrypt(nonce, plaintext)
        .map_err(|e| BoloError::ConfigError(format!("encryption failed: {e}")))?;

    let mut result = Vec::with_capacity(NONCE_SIZE + ciphertext.len());
    result.extend_from_slice(&nonce_bytes);
    result.extend_from_slice(&ciphertext);
    Ok(result)
}

/// Decrypt ciphertext with the given channel key.
///
/// Expects the format produced by [`seal`]: `nonce (24 bytes) || ciphertext || tag (16 bytes)`.
pub fn open(sealed: &[u8], key: &ChannelKey) -> Result<Vec<u8>, BoloError> {
    if sealed.len() < NONCE_SIZE + TAG_SIZE {
        return Err(BoloError::ConfigError(
            "ciphertext too short (need at least nonce + tag)".into(),
        ));
    }
    let (nonce_bytes, ct) = sealed.split_at(NONCE_SIZE);
    let nonce = XNonce::from_slice(nonce_bytes);
    let cipher = XChaCha20Poly1305::new_from_slice(&key.0)
        .map_err(|e| BoloError::ConfigError(format!("invalid channel key: {e}")))?;
    cipher.decrypt(nonce, ct).map_err(|_| {
        BoloError::ConfigError("decryption failed: invalid key or tampered ciphertext".into())
    })
}

/// Derive a per-topic gossip key from a mesh-wide shared secret.
///
/// Each gossip topic gets its own symmetric key via blake3 domain separation.
/// The `topic_context` should uniquely identify the gossip topic
/// (e.g., `"chat/dev"`, `"bolo/ci"`, `"bolo/doc/my-doc"`).
pub fn derive_gossip_key(mesh_secret: &[u8; 32], topic_context: &str) -> ChannelKey {
    ChannelKey::derive(mesh_secret, &format!("bolo/gossip/{topic_context}"))
}

/// Encrypt payload if a key is provided, otherwise return plaintext bytes.
pub fn maybe_seal(payload: &[u8], key: Option<&ChannelKey>) -> Result<Vec<u8>, BoloError> {
    match key {
        Some(k) => k.seal(payload),
        None => Ok(payload.to_vec()),
    }
}

/// Decrypt payload if a key is provided.
///
/// If decryption fails (e.g., message from a node without the key, or
/// a plaintext message during migration), falls back to treating the
/// payload as plaintext.
pub fn maybe_open(payload: &[u8], key: Option<&ChannelKey>) -> Vec<u8> {
    match key {
        Some(k) => k.open(payload).unwrap_or_else(|_| payload.to_vec()),
        None => payload.to_vec(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seal_open_roundtrip() {
        let key = ChannelKey::generate();
        let plaintext = b"hello, encrypted world!";
        let ciphertext = seal(plaintext, &key).unwrap();
        let decrypted = open(&ciphertext, &key).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn seal_produces_different_ciphertexts() {
        let key = ChannelKey::generate();
        let plaintext = b"same message";
        let ct1 = seal(plaintext, &key).unwrap();
        let ct2 = seal(plaintext, &key).unwrap();
        // Different random nonces produce different ciphertext
        assert_ne!(ct1, ct2);
        // But both decrypt to the same plaintext
        assert_eq!(open(&ct1, &key).unwrap(), plaintext);
        assert_eq!(open(&ct2, &key).unwrap(), plaintext);
    }

    #[test]
    fn wrong_key_fails() {
        let key1 = ChannelKey::generate();
        let key2 = ChannelKey::generate();
        let ciphertext = seal(b"secret", &key1).unwrap();
        assert!(open(&ciphertext, &key2).is_err());
    }

    #[test]
    fn tampered_ciphertext_fails() {
        let key = ChannelKey::generate();
        let mut ciphertext = seal(b"secret", &key).unwrap();
        // Flip a byte in the ciphertext body (after nonce)
        let last = ciphertext.len() - 1;
        ciphertext[last] ^= 0xff;
        assert!(open(&ciphertext, &key).is_err());
    }

    #[test]
    fn too_short_ciphertext_fails() {
        let key = ChannelKey::generate();
        assert!(open(&[0u8; 10], &key).is_err());
    }

    #[test]
    fn empty_plaintext_roundtrip() {
        let key = ChannelKey::generate();
        let ciphertext = seal(b"", &key).unwrap();
        let decrypted = open(&ciphertext, &key).unwrap();
        assert!(decrypted.is_empty());
    }

    #[test]
    fn derive_key_deterministic() {
        let ikm = [42u8; 32];
        let k1 = ChannelKey::derive(&ikm, "test-context");
        let k2 = ChannelKey::derive(&ikm, "test-context");
        assert_eq!(k1.as_bytes(), k2.as_bytes());
    }

    #[test]
    fn derive_key_different_context_differs() {
        let ikm = [42u8; 32];
        let k1 = ChannelKey::derive(&ikm, "context-a");
        let k2 = ChannelKey::derive(&ikm, "context-b");
        assert_ne!(k1.as_bytes(), k2.as_bytes());
    }

    #[test]
    fn shared_secret_derive_channel_key() {
        let secret = SharedSecret::from_bytes([7u8; 32]);
        let k1 = secret.derive_channel_key("chat/dev");
        let k2 = secret.derive_channel_key("chat/ops");
        // Different channel names produce different keys
        assert_ne!(k1.as_bytes(), k2.as_bytes());
        // Same channel name produces same key
        let k3 = secret.derive_channel_key("chat/dev");
        assert_eq!(k1.as_bytes(), k3.as_bytes());
    }

    #[test]
    fn channel_key_method_aliases() {
        let key = ChannelKey::generate();
        let plaintext = b"method test";
        let ciphertext = key.seal(plaintext).unwrap();
        let decrypted = key.open(&ciphertext).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn large_plaintext_roundtrip() {
        let key = ChannelKey::generate();
        let plaintext = vec![0xABu8; 100_000]; // 100KB
        let ciphertext = seal(&plaintext, &key).unwrap();
        let decrypted = open(&ciphertext, &key).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn ciphertext_overhead_is_nonce_plus_tag() {
        let key = ChannelKey::generate();
        let plaintext = b"measure overhead";
        let ciphertext = seal(plaintext, &key).unwrap();
        // overhead = 24 (nonce) + 16 (tag) = 40 bytes
        assert_eq!(ciphertext.len(), plaintext.len() + NONCE_SIZE + TAG_SIZE);
    }

    #[test]
    fn derive_gossip_key_deterministic() {
        let secret = [99u8; 32];
        let k1 = derive_gossip_key(&secret, "chat/dev");
        let k2 = derive_gossip_key(&secret, "chat/dev");
        assert_eq!(k1.as_bytes(), k2.as_bytes());
    }

    #[test]
    fn derive_gossip_key_different_topics_differ() {
        let secret = [99u8; 32];
        let k1 = derive_gossip_key(&secret, "chat/dev");
        let k2 = derive_gossip_key(&secret, "bolo/ci");
        assert_ne!(k1.as_bytes(), k2.as_bytes());
    }

    #[test]
    fn maybe_seal_with_key_encrypts() {
        let key = ChannelKey::generate();
        let plaintext = b"secret message";
        let sealed = maybe_seal(plaintext, Some(&key)).unwrap();
        assert_ne!(sealed, plaintext);
        let opened = maybe_open(&sealed, Some(&key));
        assert_eq!(opened, plaintext);
    }

    #[test]
    fn maybe_seal_without_key_passes_through() {
        let plaintext = b"no encryption";
        let result = maybe_seal(plaintext, None).unwrap();
        assert_eq!(result, plaintext);
        let opened = maybe_open(&result, None);
        assert_eq!(opened, plaintext);
    }

    #[test]
    fn maybe_open_falls_back_to_plaintext_on_wrong_key() {
        let plaintext = b"plaintext message";
        let wrong_key = ChannelKey::generate();
        // Treat plaintext bytes as if they were received without encryption
        let result = maybe_open(plaintext, Some(&wrong_key));
        // Falls back to raw bytes since decryption fails
        assert_eq!(result, plaintext);
    }
}
