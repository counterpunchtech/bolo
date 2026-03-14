#![deny(unsafe_code)]

//! Core types, errors, and configuration shared across all bolo crates.

pub mod capabilities;
pub mod config;
pub mod crypto;
pub mod error;
pub mod identity;
pub mod ipc;
pub mod node;
pub mod peers;
pub mod state;
pub mod types;

pub use capabilities::{MeshCapabilities, NodeCapabilities};
pub use config::{BoloConfig, GcConfig, StorageConfig};
pub use error::BoloError;
pub use identity::Identity;
pub use node::BoloNode;
pub use peers::TrustList;
pub use state::DaemonState;
pub use types::*;
