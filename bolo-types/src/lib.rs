#![deny(unsafe_code)]

//! Transport-agnostic types shared across all bolo crates.
//!
//! This crate contains types that do NOT depend on iroh, iroh-blobs,
//! iroh-gossip, or tokio. Lightweight clients (MQTT bridge, future
//! mobile/WASM) can depend on this crate without pulling in the
//! full QUIC stack.

use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Error type for bolo-types validation.
#[derive(Debug, Error)]
pub enum TypeError {
    #[error("invalid path: {0}")]
    InvalidPath(String),
}

/// A generic 32-byte node identity (transport-agnostic).
///
/// Use this in portable contexts. In iroh-based code, convert to/from
/// `BoloNodeId` via the `From`/`Into` implementations in `bolo-core`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeId(pub [u8; 32]);

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", hex::encode(&self.0))
    }
}

/// A generic 32-byte content hash (transport-agnostic).
///
/// Use this in portable contexts. In iroh-based code, convert to/from
/// `BlobHash` via the `From`/`Into` implementations in `bolo-core`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ContentHash(pub [u8; 32]);

impl fmt::Display for ContentHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", hex::encode(&self.0))
    }
}

/// A gossip topic identifier derived via blake3 from a human-readable name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TopicId(pub [u8; 32]);

impl TopicId {
    /// Derive a topic ID from a human-readable name.
    pub fn from_name(name: &str) -> Self {
        let hash = blake3::hash(name.as_bytes());
        Self(*hash.as_bytes())
    }
}

impl fmt::Display for TopicId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", hex::encode(&self.0))
    }
}

/// Millisecond-precision UTC timestamp.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Timestamp(pub u64);

impl Timestamp {
    /// Current time.
    pub fn now() -> Self {
        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before UNIX epoch")
            .as_millis() as u64;
        Self(millis)
    }

    /// Human-friendly relative display (e.g. "3m ago").
    pub fn relative(&self) -> String {
        let now = Self::now().0;
        if now <= self.0 {
            return "just now".to_string();
        }
        let diff_secs = (now - self.0) / 1000;
        if diff_secs < 60 {
            format!("{diff_secs}s ago")
        } else if diff_secs < 3600 {
            format!("{}m ago", diff_secs / 60)
        } else if diff_secs < 86400 {
            format!("{}h ago", diff_secs / 3600)
        } else {
            format!("{}d ago", diff_secs / 86400)
        }
    }
}

/// A validated document path (forward-slash separated, no leading slash).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DocumentPath(String);

impl DocumentPath {
    /// Create a new document path, validating the format.
    pub fn new(path: &str) -> Result<Self, TypeError> {
        if path.is_empty() {
            return Err(TypeError::InvalidPath(
                "document path cannot be empty".into(),
            ));
        }
        if path.starts_with('/') {
            return Err(TypeError::InvalidPath(
                "document path must not start with /".into(),
            ));
        }
        if path.contains("//") {
            return Err(TypeError::InvalidPath(
                "document path must not contain //".into(),
            ));
        }
        Ok(Self(path.to_string()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for DocumentPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Connection state for a peer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConnectionStatus {
    Connected,
    Disconnected,
    Connecting,
}

impl fmt::Display for ConnectionStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Connected => write!(f, "connected"),
            Self::Disconnected => write!(f, "disconnected"),
            Self::Connecting => write!(f, "connecting"),
        }
    }
}

/// Hex encoding utility (avoids adding a `hex` crate dependency).
pub(crate) mod hex {
    pub fn encode(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }
}
