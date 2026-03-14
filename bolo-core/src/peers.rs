//! Peer trust list management.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::BoloError;

/// A local list of trusted peer node IDs.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TrustList {
    pub trusted: Vec<String>,
}

impl TrustList {
    /// Path to the trust list file.
    fn path(config_dir: &Path) -> PathBuf {
        config_dir.join("trusted_peers.json")
    }

    /// Load the trust list, returning an empty list if the file doesn't exist.
    pub fn load(config_dir: &Path) -> Result<Self, BoloError> {
        let path = Self::path(config_dir);
        if !path.exists() {
            return Ok(Self::default());
        }
        let contents = std::fs::read_to_string(&path)?;
        serde_json::from_str(&contents).map_err(|e| BoloError::Serialization(e.to_string()))
    }

    /// Save the trust list.
    pub fn save(&self, config_dir: &Path) -> Result<(), BoloError> {
        let path = Self::path(config_dir);
        let json = serde_json::to_string_pretty(self)
            .map_err(|e| BoloError::Serialization(e.to_string()))?;
        std::fs::write(&path, json)?;
        Ok(())
    }

    /// Add a peer to the trusted set. Returns true if newly added.
    pub fn add(&mut self, node_id: &str) -> bool {
        if self.trusted.iter().any(|id| id == node_id) {
            return false;
        }
        self.trusted.push(node_id.to_string());
        true
    }

    /// Remove a peer from the trusted set. Returns true if it was present.
    pub fn remove(&mut self, node_id: &str) -> bool {
        let before = self.trusted.len();
        self.trusted.retain(|id| id != node_id);
        self.trusted.len() < before
    }

    /// Check if a peer is trusted.
    pub fn contains(&self, node_id: &str) -> bool {
        self.trusted.iter().any(|id| id == node_id)
    }
}
