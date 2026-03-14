//! CI gossip protocol for distributed build coordination.
//!
//! All CI coordination happens over a single gossip topic `bolo/ci`.
//! Peers broadcast task announcements, claims, and results.

use serde::{Deserialize, Serialize};

use crate::types::{BuildResult, BuildTask};

/// Messages exchanged over gossip for CI coordination.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CiMessage {
    /// A new build task broadcast to the mesh.
    TaskCreated {
        task: BuildTask,
        author: String,
        timestamp: u64,
    },
    /// A peer announces it will build a task.
    Claim {
        task_id: String,
        peer: String,
        timestamp: u64,
    },
    /// Build result broadcast back to mesh.
    Result {
        result: BuildResult,
        author: String,
        timestamp: u64,
    },
}

impl CiMessage {
    /// Serialize to bytes for gossip broadcast.
    pub fn to_bytes(&self) -> Result<Vec<u8>, bolo_core::BoloError> {
        serde_json::to_vec(self)
            .map_err(|e| bolo_core::BoloError::Serialization(format!("failed to serialize: {e}")))
    }

    /// Deserialize from gossip message bytes.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, bolo_core::BoloError> {
        serde_json::from_slice(bytes)
            .map_err(|e| bolo_core::BoloError::Serialization(format!("failed to deserialize: {e}")))
    }
}

/// Derive the gossip topic ID for CI coordination.
pub fn ci_topic_id() -> iroh_gossip::TopicId {
    let topic = bolo_core::TopicId::from_name("bolo/ci");
    iroh_gossip::TopicId::from_bytes(topic.0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::*;

    #[test]
    fn message_roundtrip() {
        let task = BuildTask {
            id: "abc123".to_string(),
            task_type: TaskType::Test,
            source_tree: "deadbeef".to_string(),
            config_hash: None,
            rust_version: None,
            targets: vec!["aarch64".to_string()],
            status: BuildStatus::Pending,
            verification: Verification::default(),
            triggered_by: "test".to_string(),
            created_at: 1000,
            updated_at: 1000,
        };
        let msg = CiMessage::TaskCreated {
            task,
            author: "node-a".to_string(),
            timestamp: 1000,
        };
        let bytes = msg.to_bytes().unwrap();
        let decoded = CiMessage::from_bytes(&bytes).unwrap();
        match decoded {
            CiMessage::TaskCreated { task, author, .. } => {
                assert_eq!(task.id, "abc123");
                assert_eq!(author, "node-a");
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn topic_id_deterministic() {
        let id1 = ci_topic_id();
        let id2 = ci_topic_id();
        assert_eq!(id1, id2);
    }
}
