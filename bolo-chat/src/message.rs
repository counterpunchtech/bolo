use serde::{Deserialize, Serialize};

/// A signed chat message.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatMessage {
    /// Unique message ID (blake3 hash of channel+sender+timestamp+content, 12 hex chars).
    pub id: String,
    /// Channel this message belongs to.
    pub channel: String,
    /// Sender node ID (hex-encoded public key).
    pub sender: String,
    /// Unix timestamp in milliseconds.
    pub timestamp: u64,
    /// Message text content.
    pub content: String,
    /// Optional parent message ID for threaded replies.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent: Option<String>,
    /// Optional attached blob hash.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blob: Option<String>,
    /// Hex-encoded ed25519 signature over the canonical message bytes.
    pub signature: String,
}

impl ChatMessage {
    /// Compute the canonical bytes for signing: channel + sender + timestamp + content.
    pub fn signing_bytes(channel: &str, sender: &str, timestamp: u64, content: &str) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(channel.as_bytes());
        buf.extend_from_slice(b":");
        buf.extend_from_slice(sender.as_bytes());
        buf.extend_from_slice(b":");
        buf.extend_from_slice(timestamp.to_le_bytes().as_ref());
        buf.extend_from_slice(b":");
        buf.extend_from_slice(content.as_bytes());
        buf
    }

    /// Generate the message ID from its contents.
    pub fn compute_id(channel: &str, sender: &str, timestamp: u64, content: &str) -> String {
        let input = format!("{channel}:{sender}:{timestamp}:{content}");
        blake3::hash(input.as_bytes()).to_hex()[..12].to_string()
    }
}

/// Wire format for gossip: wraps a ChatMessage for network transmission.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatWireMessage {
    /// The chat message.
    pub msg: ChatMessage,
}

/// Tagged gossip message — extends the wire protocol with history sync.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ChatGossipMessage {
    /// A regular chat message.
    #[serde(rename = "message")]
    Message { msg: ChatMessage },
    /// Request message history from peers since a given timestamp.
    #[serde(rename = "history_request")]
    HistoryRequest {
        channel: String,
        since_timestamp: u64,
        /// Random nonce to avoid PlumTree dedup.
        nonce: u64,
    },
    /// Response with missed messages.
    #[serde(rename = "history_response")]
    HistoryResponse {
        channel: String,
        messages: Vec<ChatMessage>,
        /// Random nonce to avoid PlumTree dedup.
        nonce: u64,
    },
}
