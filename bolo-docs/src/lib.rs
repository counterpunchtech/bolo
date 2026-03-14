#![deny(unsafe_code)]

//! CRDT document operations for bolo.

pub mod store;
pub mod sync;

pub use bolo_core::{BoloError, DocumentPath, ShareTicket, TopicId};
pub use loro;
pub use store::DocStore;
pub use sync::{apply_sync_message, doc_discovery_topic_id, doc_topic_id, DocSyncMessage};
