//! Peer command handlers.

use anyhow::{bail, Context, Result};
use bolo_core::TrustList;

use super::daemon::resolve_config_dir;

/// `bolo peer trust <node_id>` — add a peer to the trusted set.
pub fn trust(node_id: &str, config_flag: Option<&str>, json: bool) -> Result<()> {
    let config_dir = resolve_config_dir(config_flag)?;
    let mut list = TrustList::load(&config_dir).context("failed to load trust list")?;
    let added = list.add(node_id);

    if added {
        list.save(&config_dir)
            .context("failed to save trust list")?;
    }

    if json {
        let out = serde_json::json!({ "trusted": added, "node_id": node_id });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else if added {
        println!("Trusted peer: {node_id}");
    } else {
        println!("Peer already trusted: {node_id}");
    }

    Ok(())
}

/// `bolo peer untrust <node_id>` — remove a peer from the trusted set.
pub fn untrust(node_id: &str, config_flag: Option<&str>, json: bool) -> Result<()> {
    let config_dir = resolve_config_dir(config_flag)?;
    let mut list = TrustList::load(&config_dir).context("failed to load trust list")?;
    let removed = list.remove(node_id);

    if removed {
        list.save(&config_dir)
            .context("failed to save trust list")?;
    }

    if json {
        let out = serde_json::json!({ "untrusted": removed, "node_id": node_id });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else if removed {
        println!("Untrusted peer: {node_id}");
    } else {
        println!("Peer not in trust list: {node_id}");
    }

    Ok(())
}

/// `bolo peer ls` — list trusted peers.
pub fn ls(config_flag: Option<&str>, json: bool) -> Result<()> {
    let config_dir = resolve_config_dir(config_flag)?;
    let list = TrustList::load(&config_dir).context("failed to load trust list")?;

    if json {
        let out = serde_json::json!({ "trusted": list.trusted });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else if list.trusted.is_empty() {
        println!("No trusted peers.");
    } else {
        for id in &list.trusted {
            println!("{id}");
        }
        println!("\n{} peer(s)", list.trusted.len());
    }

    Ok(())
}

/// `bolo peer add <node_id>` — connect to a peer via daemon IPC.
pub async fn add(node_id: &str, config_flag: Option<&str>, json: bool) -> Result<()> {
    let config_dir = resolve_config_dir(config_flag)?;
    let mut client = bolo_core::ipc::DaemonClient::connect(&config_dir)
        .await
        .context(
            "Cannot add peer: daemon is not running.\n\
             Start the daemon with `bolo daemon start`, then retry.",
        )?;

    let result = client
        .call("peer.add", serde_json::json!({ "node_id": node_id }))
        .await
        .context("peer add failed")?;

    if json {
        println!("{}", serde_json::to_string_pretty(&result)?);
    } else {
        let conn = result
            .get("connection")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");
        println!("Added peer {node_id} ({conn})");
    }

    Ok(())
}

/// `bolo peer rm <node_id>` — disconnect from a peer (requires daemon).
pub fn rm(node_id: &str, _config_flag: Option<&str>, _json: bool) -> Result<()> {
    let _ = node_id;
    bail!(
        "Cannot remove peer: requires a running daemon with active connections.\n\
         Start the daemon with `bolo daemon start`, then retry."
    );
}

/// `bolo peer discover <topic>` — find peers by topic (requires daemon).
pub fn discover(topic: &str, _config_flag: Option<&str>, _json: bool) -> Result<()> {
    let _ = topic;
    bail!(
        "Cannot discover peers: requires a running daemon with mesh connectivity.\n\
         Start the daemon with `bolo daemon start`, then retry."
    );
}

/// `bolo peer bench <node_id>` — measure throughput to a peer.
pub async fn bench(
    node_id: &str,
    size_mb: u64,
    config_flag: Option<&str>,
    json: bool,
) -> Result<()> {
    let config_dir = resolve_config_dir(config_flag)?;
    let mut client = bolo_core::ipc::DaemonClient::connect(&config_dir)
        .await
        .context(
            "Cannot bench peer: daemon is not running.\n\
             Start the daemon with `bolo daemon start`, then retry.",
        )?;

    if !json {
        println!("Benchmarking {node_id} with {size_mb}MB payload...");
    }

    let result = client
        .call(
            "peer.bench",
            serde_json::json!({ "node_id": node_id, "size_mb": size_mb }),
        )
        .await
        .context("peer bench failed")?;

    if json {
        println!("{}", serde_json::to_string_pretty(&result)?);
    } else {
        let throughput = result
            .get("throughput_mbps")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);
        let duration_ms = result
            .get("duration_ms")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        let size_bytes = result
            .get("size_bytes")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        println!("  Size:       {:.1} MB", size_bytes as f64 / 1_048_576.0);
        println!("  Duration:   {}ms", duration_ms);
        println!("  Throughput: {:.2} MB/s", throughput);
    }

    Ok(())
}

/// `bolo peer logs <node_id>` — view logs from a remote peer.
pub async fn logs(
    node_id: &str,
    lines: usize,
    config_flag: Option<&str>,
    json: bool,
) -> Result<()> {
    let config_dir = resolve_config_dir(config_flag)?;
    let mut client = bolo_core::ipc::DaemonClient::connect(&config_dir)
        .await
        .context(
            "Cannot fetch peer logs: daemon is not running.\n\
             Start the daemon with `bolo daemon start`, then retry.",
        )?;

    if !json {
        println!("Requesting logs from {node_id}...");
    }

    let result = client
        .call(
            "peer.logs",
            serde_json::json!({ "node_id": node_id, "lines": lines }),
        )
        .await
        .context("peer.logs failed")?;

    if json {
        println!("{}", serde_json::to_string_pretty(&result)?);
    } else {
        let node = result
            .get("node_id")
            .and_then(|v| v.as_str())
            .unwrap_or(node_id);
        let log_lines = result
            .get("lines")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        println!("Logs from {node} ({} lines):", log_lines.len());
        println!();
        for line in &log_lines {
            if let Some(s) = line.as_str() {
                println!("{s}");
            }
        }
    }

    Ok(())
}

/// `bolo peer ping <node_id>` — measure latency to a peer.
pub async fn ping(node_id: &str, config_flag: Option<&str>, json: bool) -> Result<()> {
    let config_dir = resolve_config_dir(config_flag)?;

    let identity = bolo_core::Identity::load_from_config_dir(&config_dir)
        .context("failed to load identity — have you run `bolo daemon init`?")?;

    let secret_key = identity.secret_key().clone();
    let endpoint = iroh::Endpoint::builder()
        .secret_key(secret_key)
        .bind()
        .await
        .context("failed to bind endpoint")?;

    endpoint.online().await;

    let remote: iroh::PublicKey = node_id.parse().context("invalid node ID")?;

    let start = std::time::Instant::now();
    let conn = endpoint
        .connect(remote, iroh_blobs::ALPN)
        .await
        .context("failed to connect to peer")?;
    let rtt = start.elapsed();

    // Check connection type before closing
    let conn_type = if let Some(info) = endpoint.remote_info(remote).await {
        let has_direct = info.addrs().any(|a| a.addr().is_ip());
        let has_relay = info.addrs().any(|a| a.addr().is_relay());
        match (has_direct, has_relay) {
            (true, _) => "direct",
            (false, true) => "relay",
            _ => "unknown",
        }
    } else {
        "unknown"
    };

    conn.close(0u32.into(), b"ping");
    endpoint.close().await;

    if json {
        let out = serde_json::json!({
            "node_id": node_id,
            "rtt_ms": rtt.as_millis(),
            "connection": conn_type,
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Ping {node_id}: {}ms ({})", rtt.as_millis(), conn_type);
    }

    Ok(())
}
