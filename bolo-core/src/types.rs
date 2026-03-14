//! Core types shared across all bolo crates.
//!
//! Transport-agnostic types live in `bolo-types` and are re-exported here.
//! This module adds iroh-dependent types that wrap the transport-agnostic ones.

use std::fmt;

use serde::{Deserialize, Serialize};

// Re-export all transport-agnostic types so existing `use bolo_core::X` works unchanged.
pub use bolo_types::{
    ConnectionStatus, ContentHash, DocumentPath, NodeId, Timestamp, TopicId, TypeError,
};

/// A node's public identity on the mesh.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BoloNodeId(pub iroh::PublicKey);

impl fmt::Display for BoloNodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<iroh::PublicKey> for BoloNodeId {
    fn from(key: iroh::PublicKey) -> Self {
        Self(key)
    }
}

impl From<BoloNodeId> for NodeId {
    fn from(id: BoloNodeId) -> Self {
        NodeId(*id.0.as_bytes())
    }
}

/// A node's secret key.
///
/// Debug output is redacted to prevent accidental key leakage in logs.
pub struct BoloSecretKey(pub iroh::SecretKey);

impl fmt::Debug for BoloSecretKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("BoloSecretKey").field(&"[REDACTED]").finish()
    }
}

impl From<iroh::SecretKey> for BoloSecretKey {
    fn from(key: iroh::SecretKey) -> Self {
        Self(key)
    }
}

/// Content-addressed blob hash.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BlobHash(pub iroh_blobs::Hash);

impl fmt::Display for BlobHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<iroh_blobs::Hash> for BlobHash {
    fn from(hash: iroh_blobs::Hash) -> Self {
        Self(hash)
    }
}

impl From<BlobHash> for ContentHash {
    fn from(hash: BlobHash) -> Self {
        ContentHash(*hash.0.as_bytes())
    }
}

/// Information about a connected peer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerInfo {
    pub node_id: BoloNodeId,
    pub last_seen: Timestamp,
    pub connection: ConnectionStatus,
}

/// A ticket for sharing a document with a peer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShareTicket {
    pub node_id: BoloNodeId,
    pub path: DocumentPath,
    pub topic: TopicId,
}
