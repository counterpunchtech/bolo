//! Daemon state file for CLI communication.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::BoloError;

/// Persistent state written by the daemon for CLI tools to read.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DaemonState {
    pub pid: u32,
    pub node_id: String,
    pub start_time_ms: u64,
    pub data_dir: String,
}

impl DaemonState {
    /// Path to the state file within a config directory.
    pub fn path(config_dir: &Path) -> PathBuf {
        config_dir.join("daemon.json")
    }

    /// Write state to the config directory.
    pub fn save(&self, config_dir: &Path) -> Result<(), BoloError> {
        let path = Self::path(config_dir);
        let json = serde_json::to_string_pretty(self)
            .map_err(|e| BoloError::Serialization(e.to_string()))?;
        std::fs::write(&path, json)?;
        Ok(())
    }

    /// Load state from the config directory.
    pub fn load(config_dir: &Path) -> Result<Self, BoloError> {
        let path = Self::path(config_dir);
        if !path.exists() {
            return Err(BoloError::NodeNotRunning);
        }
        let contents = std::fs::read_to_string(&path)?;
        serde_json::from_str(&contents).map_err(|e| BoloError::Serialization(e.to_string()))
    }

    /// Remove the state file.
    pub fn remove(config_dir: &Path) -> Result<(), BoloError> {
        let path = Self::path(config_dir);
        if path.exists() {
            std::fs::remove_file(&path)?;
        }
        Ok(())
    }

    /// Check if the daemon process is still alive.
    pub fn is_alive(&self) -> bool {
        std::process::Command::new("kill")
            .args(["-0", &self.pid.to_string()])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .map(|s| s.success())
            .unwrap_or(false)
    }
}
