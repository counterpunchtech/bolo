//! Bolo error types.

use thiserror::Error;

/// Exit code constants for CLI.
pub mod exit_codes {
    pub const SUCCESS: i32 = 0;
    pub const GENERAL_ERROR: i32 = 1;
    pub const CONFIG_ERROR: i32 = 2;
    pub const NODE_NOT_RUNNING: i32 = 3;
    pub const IDENTITY_ERROR: i32 = 4;
    pub const PEER_ERROR: i32 = 5;
    pub const BLOB_ERROR: i32 = 6;
    pub const DOCUMENT_ERROR: i32 = 7;
    pub const TIMEOUT: i32 = 8;
}

/// Core error type for all bolo operations.
#[derive(Debug, Error)]
pub enum BoloError {
    #[error("node is not running")]
    NodeNotRunning,

    #[error("identity not found: {0}")]
    IdentityNotFound(String),

    #[error("peer unreachable: {0}")]
    PeerUnreachable(String),

    #[error("blob not found: {0}")]
    BlobNotFound(String),

    #[error("document not found: {0}")]
    DocumentNotFound(String),

    #[error("configuration error: {0}")]
    ConfigError(String),

    #[error("invalid path: {0}")]
    InvalidPath(String),

    #[error("timeout after {0}ms")]
    Timeout(u64),

    #[error("serialization error: {0}")]
    Serialization(String),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Type(#[from] bolo_types::TypeError),
}

impl BoloError {
    /// Return the exit code for this error.
    pub fn exit_code(&self) -> i32 {
        match self {
            Self::NodeNotRunning => exit_codes::NODE_NOT_RUNNING,
            Self::IdentityNotFound(_) => exit_codes::IDENTITY_ERROR,
            Self::PeerUnreachable(_) => exit_codes::PEER_ERROR,
            Self::BlobNotFound(_) => exit_codes::BLOB_ERROR,
            Self::DocumentNotFound(_) => exit_codes::DOCUMENT_ERROR,
            Self::ConfigError(_) => exit_codes::CONFIG_ERROR,
            Self::InvalidPath(_) => exit_codes::GENERAL_ERROR,
            Self::Timeout(_) => exit_codes::TIMEOUT,
            Self::Serialization(_) => exit_codes::GENERAL_ERROR,
            Self::Io(_) => exit_codes::GENERAL_ERROR,
            Self::Type(_) => exit_codes::GENERAL_ERROR,
        }
    }
}
