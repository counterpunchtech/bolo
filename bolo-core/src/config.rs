//! Configuration loading and saving.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::BoloError;

/// Top-level bolo configuration (TOML format).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BoloConfig {
    #[serde(default)]
    pub daemon: DaemonConfig,
    #[serde(default)]
    pub relay: RelayConfig,
    #[serde(default)]
    pub identity: IdentityConfig,
    #[serde(default)]
    pub crypto: CryptoConfig,
    #[serde(default)]
    pub deploy: DeployConfig,
    #[serde(default)]
    pub storage: StorageConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DaemonConfig {
    #[serde(default = "default_daemon_port")]
    pub port: u16,
}

impl Default for DaemonConfig {
    fn default() -> Self {
        Self {
            port: default_daemon_port(),
        }
    }
}

fn default_daemon_port() -> u16 {
    4919
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RelayConfig {
    #[serde(default)]
    pub urls: Vec<String>,
    #[serde(default)]
    pub serve: bool,
    #[serde(default = "default_relay_port")]
    pub port: u16,
}

impl Default for RelayConfig {
    fn default() -> Self {
        Self {
            urls: Vec::new(),
            serve: false,
            port: default_relay_port(),
        }
    }
}

fn default_relay_port() -> u16 {
    4920
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdentityConfig {
    #[serde(default = "default_key_file")]
    pub key_file: String,
}

impl Default for IdentityConfig {
    fn default() -> Self {
        Self {
            key_file: default_key_file(),
        }
    }
}

fn default_key_file() -> String {
    "identity.key".to_string()
}

/// Gossip encryption configuration.
///
/// When `mesh_secret` is set (64 hex chars = 32 bytes), all gossip payloads
/// are encrypted with per-topic keys derived from this shared secret.
/// Nodes without the secret can still relay ciphertext (zero-knowledge relays)
/// but cannot read message contents.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CryptoConfig {
    /// Hex-encoded 32-byte shared secret for mesh-wide gossip encryption.
    /// If absent, gossip payloads are sent as plaintext.
    #[serde(default)]
    pub mesh_secret: Option<String>,
}

impl CryptoConfig {
    /// Parse the hex-encoded mesh secret into raw bytes.
    /// Returns `None` if no secret is configured, errors if the hex is invalid.
    pub fn mesh_secret_bytes(&self) -> Result<Option<[u8; 32]>, crate::error::BoloError> {
        match &self.mesh_secret {
            None => Ok(None),
            Some(hex_str) => {
                let hex_str = hex_str.trim();
                if hex_str.len() != 64 {
                    return Err(crate::error::BoloError::ConfigError(format!(
                        "mesh_secret must be 64 hex chars (32 bytes), got {}",
                        hex_str.len()
                    )));
                }
                let mut bytes = [0u8; 32];
                for (i, chunk) in hex_str.as_bytes().chunks(2).enumerate() {
                    let hi = hex_nibble(chunk[0]).ok_or_else(|| {
                        crate::error::BoloError::ConfigError("invalid hex in mesh_secret".into())
                    })?;
                    let lo = hex_nibble(chunk[1]).ok_or_else(|| {
                        crate::error::BoloError::ConfigError("invalid hex in mesh_secret".into())
                    })?;
                    bytes[i] = (hi << 4) | lo;
                }
                Ok(Some(bytes))
            }
        }
    }
}

/// Storage budgets and garbage collection configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// Maximum blob storage in bytes (default: 10 GB).
    #[serde(default = "default_max_blob_bytes")]
    pub max_blob_bytes: u64,
    /// Maximum number of CRDT docs before LRU eviction (default: 10000).
    #[serde(default = "default_max_docs")]
    pub max_docs: usize,
    /// Maximum chat messages per channel before pruning (default: 10000).
    #[serde(default = "default_chat_history")]
    pub chat_history_per_channel: usize,
    /// Maximum CI results per task before pruning (default: 50).
    #[serde(default = "default_ci_retain")]
    pub ci_results_retain: usize,
    /// Garbage collection settings.
    #[serde(default)]
    pub gc: GcConfig,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            max_blob_bytes: default_max_blob_bytes(),
            max_docs: default_max_docs(),
            chat_history_per_channel: default_chat_history(),
            ci_results_retain: default_ci_retain(),
            gc: GcConfig::default(),
        }
    }
}

fn default_max_blob_bytes() -> u64 {
    10_737_418_240 // 10 GB
}
fn default_max_docs() -> usize {
    10_000
}
fn default_chat_history() -> usize {
    10_000
}
fn default_ci_retain() -> usize {
    50
}

/// Garbage collection scheduling.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GcConfig {
    /// Run GC on daemon start and periodically (default: true).
    #[serde(default = "default_gc_auto")]
    pub auto: bool,
    /// Periodic GC interval in hours (default: 24).
    #[serde(default = "default_gc_interval")]
    pub interval_hours: u64,
}

impl Default for GcConfig {
    fn default() -> Self {
        Self {
            auto: default_gc_auto(),
            interval_hours: default_gc_interval(),
        }
    }
}

fn default_gc_auto() -> bool {
    true
}
fn default_gc_interval() -> u64 {
    24
}

/// Configuration for hot deploy.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DeployConfig {
    /// Systemd service name to restart on deploy (default: "bolo").
    #[serde(default)]
    pub service_name: Option<String>,
}

fn hex_nibble(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

impl BoloConfig {
    /// Resolve the configuration directory (~/.config/bolo or platform equivalent).
    pub fn resolve_config_dir() -> Result<PathBuf, BoloError> {
        dirs::config_dir()
            .map(|d| d.join("bolo"))
            .ok_or_else(|| BoloError::ConfigError("cannot determine config directory".into()))
    }

    /// Load config from the given path, or the default location.
    pub fn load(path: Option<&Path>) -> Result<Self, BoloError> {
        let config_path = match path {
            Some(p) => p.to_path_buf(),
            None => Self::resolve_config_dir()?.join("config.toml"),
        };

        if !config_path.exists() {
            return Ok(Self::default());
        }

        let contents = std::fs::read_to_string(&config_path)?;
        toml::from_str(&contents).map_err(|e| BoloError::ConfigError(format!("parse error: {e}")))
    }

    /// Save config to the given path, or the default location.
    pub fn save(&self, path: Option<&Path>) -> Result<(), BoloError> {
        let config_path = match path {
            Some(p) => p.to_path_buf(),
            None => Self::resolve_config_dir()?.join("config.toml"),
        };

        if let Some(parent) = config_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let contents = toml::to_string_pretty(self)
            .map_err(|e| BoloError::Serialization(format!("serialize error: {e}")))?;
        std::fs::write(&config_path, contents)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_round_trip() {
        let config = BoloConfig {
            daemon: DaemonConfig { port: 5000 },
            relay: RelayConfig {
                urls: vec!["https://relay.example.com".to_string()],
                serve: true,
                port: 5001,
            },
            identity: IdentityConfig {
                key_file: "my.key".to_string(),
            },
            crypto: CryptoConfig::default(),
            deploy: DeployConfig::default(),
            storage: StorageConfig::default(),
        };

        let toml_str = toml::to_string_pretty(&config).unwrap();
        let parsed: BoloConfig = toml::from_str(&toml_str).unwrap();

        assert_eq!(parsed.daemon.port, 5000);
        assert_eq!(parsed.relay.urls.len(), 1);
        assert!(parsed.relay.serve);
        assert_eq!(parsed.relay.port, 5001);
        assert_eq!(parsed.identity.key_file, "my.key");
    }

    #[test]
    fn storage_config_defaults() {
        let config = StorageConfig::default();
        assert_eq!(config.max_blob_bytes, 10_737_418_240);
        assert_eq!(config.max_docs, 10_000);
        assert_eq!(config.chat_history_per_channel, 10_000);
        assert_eq!(config.ci_results_retain, 50);
        assert!(config.gc.auto);
        assert_eq!(config.gc.interval_hours, 24);
    }

    #[test]
    fn storage_config_round_trip_toml() {
        let toml_str = r#"
[storage]
max_blob_bytes = 5_368_709_120
max_docs = 500
chat_history_per_channel = 1000
ci_results_retain = 10

[storage.gc]
auto = false
interval_hours = 12
"#;
        let config: BoloConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.storage.max_blob_bytes, 5_368_709_120);
        assert_eq!(config.storage.max_docs, 500);
        assert_eq!(config.storage.chat_history_per_channel, 1000);
        assert_eq!(config.storage.ci_results_retain, 10);
        assert!(!config.storage.gc.auto);
        assert_eq!(config.storage.gc.interval_hours, 12);
    }

    #[test]
    fn crypto_config_defaults_to_none() {
        let config = CryptoConfig::default();
        assert!(config.mesh_secret.is_none());
        assert!(config.mesh_secret_bytes().unwrap().is_none());
    }

    #[test]
    fn crypto_config_parses_hex_secret() {
        let config = CryptoConfig {
            mesh_secret: Some("aa".repeat(32)),
        };
        let bytes = config.mesh_secret_bytes().unwrap().unwrap();
        assert_eq!(bytes, [0xaa; 32]);
    }

    #[test]
    fn crypto_config_rejects_short_secret() {
        let config = CryptoConfig {
            mesh_secret: Some("aabb".to_string()),
        };
        assert!(config.mesh_secret_bytes().is_err());
    }

    #[test]
    fn crypto_config_round_trip_toml() {
        let toml_str = r#"
[crypto]
mesh_secret = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
"#;
        let config: BoloConfig = toml::from_str(toml_str).unwrap();
        assert!(config.crypto.mesh_secret.is_some());
        let bytes = config.crypto.mesh_secret_bytes().unwrap().unwrap();
        assert_eq!(bytes[0], 0x01);
        assert_eq!(bytes[1], 0x23);
    }
}
