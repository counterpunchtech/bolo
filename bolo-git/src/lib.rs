#![deny(unsafe_code)]

//! Git-to-mesh bridge for bolo.

pub mod bridge;
pub mod review;
pub mod types;

pub use bridge::GitBridge;
pub use review::ReviewStore;
pub use types::*;
