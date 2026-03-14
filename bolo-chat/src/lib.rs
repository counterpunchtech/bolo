#![deny(unsafe_code)]

//! Bolo chat — gossip-backed team communication channels.
//!
//! Messages are signed with ed25519, stored locally as JSON files,
//! and propagated to peers via iroh-gossip topics.

mod message;
mod store;

pub use message::{ChatGossipMessage, ChatMessage, ChatWireMessage};
pub use store::ChatStore;
