//! Document sync protocol over gossip.
//!
//! Documents are synced between peers using gossip topics. Each document has a dedicated
//! gossip topic derived from its path. Updates are broadcast as serialized messages
//! containing Loro incremental updates or full snapshots.

use serde::{Deserialize, Serialize};

use crate::store::DocStore;

/// Messages exchanged over gossip for document sync.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DocSyncMessage {
    /// An incremental update to a document (Loro update bytes).
    Update {
        /// Document path
        path: String,
        /// Loro update bytes (encoded via `ExportMode::updates_till`)
        data: Vec<u8>,
        /// Author node ID (public key string)
        author: String,
        /// Timestamp in milliseconds since epoch
        timestamp: u64,
        /// Random nonce to prevent PlumTree message deduplication.
        #[serde(default)]
        nonce: u64,
    },
    /// A full snapshot of a document.
    Snapshot {
        /// Document path
        path: String,
        /// Loro snapshot bytes
        data: Vec<u8>,
        /// Author node ID
        author: String,
        /// Timestamp in milliseconds since epoch
        timestamp: u64,
        /// Random nonce to prevent PlumTree message deduplication.
        #[serde(default)]
        nonce: u64,
    },
    /// Request the current state of a document.
    SyncRequest {
        /// Document path
        path: String,
        /// Requester's node ID
        peer: String,
        /// Random nonce to prevent PlumTree message deduplication.
        #[serde(default)]
        nonce: u64,
    },
}

impl DocSyncMessage {
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

    /// Get the document path from any message variant.
    pub fn path(&self) -> &str {
        match self {
            Self::Update { path, .. } => path,
            Self::Snapshot { path, .. } => path,
            Self::SyncRequest { path, .. } => path,
        }
    }
}

/// Apply a sync message to the local document store.
///
/// Returns `true` if the document was modified.
pub fn apply_sync_message(
    store: &DocStore,
    msg: &DocSyncMessage,
) -> Result<bool, bolo_core::BoloError> {
    match msg {
        DocSyncMessage::Update { path, data, .. } => {
            if !store.exists(path) {
                // Create the doc if it doesn't exist, then apply the update
                let doc = loro::LoroDoc::new();
                doc.import(data).map_err(|e| {
                    bolo_core::BoloError::Serialization(format!("failed to import update: {e}"))
                })?;
                doc.commit();
                store.save(path, &doc)?;
                return Ok(true);
            }
            let doc = store.load(path)?;
            doc.import(data).map_err(|e| {
                bolo_core::BoloError::Serialization(format!("failed to import update: {e}"))
            })?;
            doc.commit();
            store.save(path, &doc)?;
            Ok(true)
        }
        DocSyncMessage::Snapshot { path, data, .. } => {
            if store.exists(path) {
                // Merge: load local doc and import the remote snapshot bytes
                let local_doc = store.load(path)?;
                local_doc.import(data).map_err(|e| {
                    bolo_core::BoloError::Serialization(format!("merge error: {e}"))
                })?;
                local_doc.commit();
                store.save(path, &local_doc)?;
            } else {
                // No local doc — create from snapshot
                let doc = loro::LoroDoc::from_snapshot(data).map_err(|e| {
                    bolo_core::BoloError::Serialization(format!("invalid snapshot: {e}"))
                })?;
                store.save(path, &doc)?;
            }
            Ok(true)
        }
        DocSyncMessage::SyncRequest { .. } => {
            // Sync requests are handled by the caller (daemon loop)
            Ok(false)
        }
    }
}

/// Derive a gossip topic ID from a document path.
pub fn doc_topic_id(path: &str) -> iroh_gossip::TopicId {
    let topic = bolo_core::TopicId::from_name(&format!("bolo/doc/{path}"));
    iroh_gossip::TopicId::from_bytes(topic.0)
}

/// Well-known gossip topic for announcing new documents.
/// When a node creates a new doc, it broadcasts the doc path on this topic.
/// Peers receiving the message auto-subscribe to the new doc's topic.
pub fn doc_discovery_topic_id() -> iroh_gossip::TopicId {
    let topic = bolo_core::TopicId::from_name("bolo/doc-discovery");
    iroh_gossip::TopicId::from_bytes(topic.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_roundtrip() {
        let msg = DocSyncMessage::Update {
            path: "specs/vision".to_string(),
            data: vec![1, 2, 3, 4],
            author: "test-node".to_string(),
            timestamp: 1234567890,
            nonce: 0,
        };
        let bytes = msg.to_bytes().unwrap();
        let decoded = DocSyncMessage::from_bytes(&bytes).unwrap();
        assert_eq!(decoded.path(), "specs/vision");
        if let DocSyncMessage::Update { data, .. } = decoded {
            assert_eq!(data, vec![1, 2, 3, 4]);
        } else {
            panic!("wrong variant");
        }
    }

    #[test]
    fn loro_snapshot_merge_works() {
        // Verify that importing a snapshot into an existing doc merges correctly
        let doc_a = loro::LoroDoc::new();
        let text = doc_a.get_text("content");
        text.insert(0, "Hello").unwrap();
        doc_a.commit();

        // Import A's snapshot into B
        let snap_a = doc_a.export(loro::ExportMode::Snapshot).unwrap();
        let doc_b = loro::LoroDoc::from_snapshot(&snap_a).unwrap();

        // B makes an edit
        let text_b = doc_b.get_text("content");
        text_b.insert(5, " World").unwrap();
        doc_b.commit();

        // Export B's snapshot, import into A
        let snap_b = doc_b.export(loro::ExportMode::Snapshot).unwrap();
        doc_a.import(&snap_b).unwrap();
        doc_a.commit();
        assert_eq!(doc_a.get_text("content").to_string(), "Hello World");
    }

    #[test]
    fn apply_snapshot_merge() {
        let tmp = tempfile::tempdir().unwrap();
        let store = DocStore::open(tmp.path()).unwrap();

        // Create doc A
        let doc_a = loro::LoroDoc::new();
        doc_a.get_text("content").insert(0, "Hello").unwrap();
        doc_a.commit();
        store.save("test", &doc_a).unwrap();

        // Create doc B from A's snapshot, edit it
        let snap_a = doc_a.export(loro::ExportMode::Snapshot).unwrap();
        let doc_b = loro::LoroDoc::from_snapshot(&snap_a).unwrap();
        doc_b.get_text("content").insert(5, " World").unwrap();
        doc_b.commit();

        // Apply B's snapshot to the store (which has A's version)
        let snap_b = doc_b.export(loro::ExportMode::Snapshot).unwrap();
        let msg = DocSyncMessage::Snapshot {
            path: "test".to_string(),
            data: snap_b,
            author: "node-b".to_string(),
            timestamp: 1000,
            nonce: 0,
        };
        let applied = apply_sync_message(&store, &msg).unwrap();
        assert!(applied);

        // Verify merge
        let merged = store.load("test").unwrap();
        assert_eq!(merged.get_text("content").to_string(), "Hello World");
    }

    #[test]
    fn topic_id_deterministic() {
        let id1 = doc_topic_id("specs/vision");
        let id2 = doc_topic_id("specs/vision");
        assert_eq!(id1, id2);

        let id3 = doc_topic_id("specs/roadmap");
        assert_ne!(id1, id3);
    }
}
