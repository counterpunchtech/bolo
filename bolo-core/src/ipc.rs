//! Daemon IPC protocol — JSON-RPC 2.0 over Unix domain socket.
//!
//! The daemon listens on `{config_dir}/daemon.sock`. CLI commands connect,
//! send a single JSON request line, read a single JSON response line, and disconnect.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

use crate::error::BoloError;

/// Path to the daemon Unix socket.
pub fn socket_path(config_dir: &Path) -> PathBuf {
    config_dir.join("daemon.sock")
}

/// A JSON-RPC 2.0 request.
#[derive(Debug, Serialize, Deserialize)]
pub struct IpcRequest {
    pub jsonrpc: String,
    pub method: String,
    #[serde(default)]
    pub params: serde_json::Value,
    pub id: u64,
}

impl IpcRequest {
    pub fn new(method: &str, params: serde_json::Value) -> Self {
        Self {
            jsonrpc: "2.0".into(),
            method: method.into(),
            params,
            id: 1,
        }
    }
}

/// A JSON-RPC 2.0 response.
#[derive(Debug, Serialize, Deserialize)]
pub struct IpcResponse {
    pub jsonrpc: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<IpcError>,
    pub id: u64,
}

impl IpcResponse {
    pub fn success(id: u64, result: serde_json::Value) -> Self {
        Self {
            jsonrpc: "2.0".into(),
            result: Some(result),
            error: None,
            id,
        }
    }

    pub fn error(id: u64, code: i32, message: String) -> Self {
        Self {
            jsonrpc: "2.0".into(),
            result: None,
            error: Some(IpcError { code, message }),
            id,
        }
    }
}

/// JSON-RPC error object.
#[derive(Debug, Serialize, Deserialize)]
pub struct IpcError {
    pub code: i32,
    pub message: String,
}

/// Thin client for talking to the daemon over its Unix socket.
/// Each `call()` opens a fresh connection since the daemon handles
/// one request per connection.
pub struct DaemonClient {
    socket_path: PathBuf,
}

impl DaemonClient {
    /// Connect to the daemon socket in the given config directory.
    /// Validates that the socket is connectable by making a test connection.
    pub async fn connect(config_dir: &Path) -> Result<Self, BoloError> {
        let path = socket_path(config_dir);
        // Verify the daemon is running by making a test connection
        UnixStream::connect(&path).await.map_err(|e| {
            BoloError::ConfigError(format!(
                "failed to connect to daemon at {}: {e}",
                path.display()
            ))
        })?;
        Ok(Self { socket_path: path })
    }

    /// Send a request and read the response.
    /// Opens a fresh connection for each call.
    pub async fn call(
        &mut self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<serde_json::Value, BoloError> {
        let stream = UnixStream::connect(&self.socket_path)
            .await
            .map_err(|e| BoloError::ConfigError(format!("failed to connect to daemon: {e}")))?;
        let mut stream = BufReader::new(stream);

        let req = IpcRequest::new(method, params);
        let mut line =
            serde_json::to_string(&req).map_err(|e| BoloError::Serialization(e.to_string()))?;
        line.push('\n');

        stream
            .get_mut()
            .write_all(line.as_bytes())
            .await
            .map_err(|e| BoloError::ConfigError(format!("failed to send IPC request: {e}")))?;

        let mut response_line = String::new();
        stream
            .read_line(&mut response_line)
            .await
            .map_err(|e| BoloError::ConfigError(format!("failed to read IPC response: {e}")))?;

        let resp: IpcResponse = serde_json::from_str(&response_line)
            .map_err(|e| BoloError::Serialization(format!("invalid IPC response: {e}")))?;

        if let Some(err) = resp.error {
            return Err(BoloError::ConfigError(err.message));
        }

        Ok(resp.result.unwrap_or(serde_json::Value::Null))
    }
}
