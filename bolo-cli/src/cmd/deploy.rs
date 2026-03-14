//! Deploy command handlers — hot deploy binaries across the mesh.

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};

use super::daemon::resolve_config_dir;

/// State file tracking last build for deploy.
const DEPLOY_STATE_FILE: &str = "deploy_state.json";

/// Default cross-compile target for aarch64 Linux (Jetson, RPi, etc).
const DEFAULT_TARGET: &str = "aarch64-unknown-linux-gnu";

#[derive(Debug, Serialize, Deserialize)]
struct DeployState {
    blob_hash: String,
    target: String,
    binary_size: u64,
    built_at: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct DeployMessage {
    /// "deploy" — message type tag
    r#type: String,
    blob_hash: String,
    target: String,
    binary_size: u64,
    sender: String,
    timestamp: u64,
}

/// `bolo deploy build` — cross-compile and stage binary as a blob.
pub async fn build(target: Option<&str>, config_flag: Option<&str>, json: bool) -> Result<()> {
    let config_dir = resolve_config_dir(config_flag)?;
    let target = target.unwrap_or(DEFAULT_TARGET);

    if !json {
        println!("Cross-compiling for {target}...");
    }

    // Run cargo build
    let status = std::process::Command::new("cargo")
        .args([
            "build",
            "-p",
            "bolo-cli",
            "--release",
            "--target",
            target,
            "--features",
            "git2/vendored-openssl",
        ])
        .status()
        .context("failed to run cargo build")?;

    if !status.success() {
        bail!("cargo build failed with exit code: {}", status);
    }

    // Find the built binary
    let binary_path = format!("target/{target}/release/bolo");
    if !std::path::Path::new(&binary_path).exists() {
        bail!("expected binary not found at {binary_path}");
    }

    let binary_size = std::fs::metadata(&binary_path)?.len();

    if !json {
        println!(
            "Build complete: {binary_path} ({:.1} MB)",
            binary_size as f64 / 1_048_576.0
        );
        println!("Adding to blob store...");
    }

    // Add to blob store via daemon IPC
    let mut client = bolo_core::ipc::DaemonClient::connect(&config_dir)
        .await
        .context("Daemon not running. Start with `bolo daemon start`.")?;

    let abs_path = std::path::Path::new(&binary_path)
        .canonicalize()
        .context("failed to resolve binary path")?;

    let result = client
        .call(
            "blob.put",
            serde_json::json!({ "file": abs_path.to_string_lossy() }),
        )
        .await
        .context("failed to add binary to blob store")?;

    let blob_hash = result
        .get("hash")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("blob.put returned no hash"))?
        .to_string();

    // Save deploy state
    let state = DeployState {
        blob_hash: blob_hash.clone(),
        target: target.to_string(),
        binary_size,
        built_at: bolo_core::Timestamp::now().0,
    };
    let state_path = config_dir.join(DEPLOY_STATE_FILE);
    std::fs::write(&state_path, serde_json::to_string_pretty(&state)?)?;

    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "blob_hash": blob_hash,
                "target": target,
                "binary_size": binary_size,
            }))?
        );
    } else {
        println!("Staged: {blob_hash}");
        println!("Run `bolo deploy push <peer>` to deploy.");
    }

    Ok(())
}

/// `bolo deploy push` — send the staged binary to a peer via gossip.
pub async fn push(peer: Option<&str>, config_flag: Option<&str>, json: bool) -> Result<()> {
    let config_dir = resolve_config_dir(config_flag)?;
    let identity = bolo_core::Identity::load_from_config_dir(&config_dir)
        .context("failed to load identity")?;
    let node_id = identity.node_id().to_string();

    // Load deploy state
    let state_path = config_dir.join(DEPLOY_STATE_FILE);
    if !state_path.exists() {
        bail!("No staged build. Run `bolo deploy build` first.");
    }
    let state: DeployState = serde_json::from_str(&std::fs::read_to_string(&state_path)?)
        .context("corrupt deploy state")?;

    let msg = DeployMessage {
        r#type: "deploy".to_string(),
        blob_hash: state.blob_hash.clone(),
        target: state.target.clone(),
        binary_size: state.binary_size,
        sender: node_id.clone(),
        timestamp: bolo_core::Timestamp::now().0,
    };

    let payload = serde_json::to_string(&msg)?;

    // Send via daemon IPC deploy.push — waits for peer to connect before broadcasting
    let mut client = bolo_core::ipc::DaemonClient::connect(&config_dir)
        .await
        .context("Daemon not running.")?;

    if !json {
        println!("Waiting for peer connection...");
    }

    let mut params = serde_json::json!({
        "message": payload,
    });
    if let Some(peer_id) = peer {
        params["peer"] = serde_json::json!(peer_id);
    }

    client
        .call("deploy.push", params)
        .await
        .context("failed to send deploy message")?;

    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "sent": true,
                "blob_hash": state.blob_hash,
                "target": state.target,
                "peer": peer,
            }))?
        );
    } else {
        println!("Deploy message sent!");
        println!("  Blob:   {}", state.blob_hash);
        println!("  Target: {}", state.target);
        if let Some(p) = peer {
            println!("  Peer:   {p}");
        }
    }

    Ok(())
}

/// `bolo deploy status` — show last build and deploy state.
pub fn status(config_flag: Option<&str>, json: bool) -> Result<()> {
    let config_dir = resolve_config_dir(config_flag)?;
    let state_path = config_dir.join(DEPLOY_STATE_FILE);

    if !state_path.exists() {
        if json {
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({ "staged": false }))?
            );
        } else {
            println!("No staged build. Run `bolo deploy build` first.");
        }
        return Ok(());
    }

    let state: DeployState = serde_json::from_str(&std::fs::read_to_string(&state_path)?)
        .context("corrupt deploy state")?;

    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "staged": true,
                "blob_hash": state.blob_hash,
                "target": state.target,
                "binary_size": state.binary_size,
                "built_at": state.built_at,
            }))?
        );
    } else {
        println!("Staged build:");
        println!("  Blob:   {}", state.blob_hash);
        println!("  Target: {}", state.target);
        println!("  Size:   {:.1} MB", state.binary_size as f64 / 1_048_576.0);
    }

    Ok(())
}
