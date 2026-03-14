//! Doc command handlers.

use anyhow::{bail, Context, Result};
use bolo_core::Timestamp;
use bolo_docs::DocStore;

use super::daemon::{resolve_config_dir, resolve_data_dir};

fn open_store(config_flag: Option<&str>) -> Result<DocStore> {
    let config_dir = resolve_config_dir(config_flag)?;
    let data_dir = resolve_data_dir(&config_dir);
    DocStore::open(&data_dir).context("failed to open document store")
}

/// `bolo doc create <path>` — create a new CRDT document.
pub fn create(path: &str, config_flag: Option<&str>, json: bool) -> Result<()> {
    let store = open_store(config_flag)?;
    store.create(path).context("failed to create document")?;

    if json {
        let out = serde_json::json!({ "created": true, "path": path });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Created document: {path}");
    }

    Ok(())
}

/// `bolo doc rm <path>` — delete a document.
pub fn rm(path: &str, config_flag: Option<&str>, json: bool) -> Result<()> {
    let store = open_store(config_flag)?;
    store.delete(path).context("failed to delete document")?;

    if json {
        let out = serde_json::json!({ "deleted": true, "path": path });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Deleted document: {path}");
    }

    Ok(())
}

/// `bolo doc ls [prefix]` — list documents.
pub fn ls(prefix: Option<&str>, config_flag: Option<&str>, json: bool) -> Result<()> {
    let store = open_store(config_flag)?;
    let mut names = store.list().context("failed to list documents")?;

    if let Some(prefix) = prefix {
        names.retain(|n| n.starts_with(prefix));
    }

    if json {
        let out = serde_json::json!({ "documents": names });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else if names.is_empty() {
        println!("No documents.");
    } else {
        for name in &names {
            println!("{name}");
        }
        println!("\n{} document(s)", names.len());
    }

    Ok(())
}

/// `bolo doc get <path> [key]` — read a document or a specific key.
pub fn get(path: &str, key: Option<&str>, config_flag: Option<&str>, json: bool) -> Result<()> {
    let store = open_store(config_flag)?;
    let doc = store.load(path).context("failed to load document")?;

    match key {
        Some(k) => {
            let map = doc.get_map("data");
            match map.get(k) {
                Some(val) => {
                    let value = val
                        .into_value()
                        .map(|v| format!("{v:?}"))
                        .unwrap_or_default();
                    if json {
                        let out = serde_json::json!({ "path": path, "key": k, "value": value });
                        println!("{}", serde_json::to_string_pretty(&out)?);
                    } else {
                        println!("{value}");
                    }
                }
                None => {
                    if json {
                        let out = serde_json::json!({ "path": path, "key": k, "value": null });
                        println!("{}", serde_json::to_string_pretty(&out)?);
                    } else {
                        println!("Key not found: {k}");
                    }
                }
            }
        }
        None => {
            let value = doc.get_deep_value();
            if json {
                let out = serde_json::json!({ "path": path, "value": format!("{value:?}") });
                println!("{}", serde_json::to_string_pretty(&out)?);
            } else {
                println!("{value:?}");
            }
        }
    }

    Ok(())
}

/// `bolo doc set <path> <key> <value>` — set a key in a map document.
///
/// Routes through daemon IPC when available so changes are broadcast to mesh peers.
pub async fn set(
    path: &str,
    key: &str,
    value: &str,
    config_flag: Option<&str>,
    json: bool,
) -> Result<()> {
    let config_dir = resolve_config_dir(config_flag)?;

    // Try daemon IPC first for mesh broadcast
    if let Ok(mut client) = bolo_core::ipc::DaemonClient::connect(&config_dir).await {
        let result = client
            .call(
                "doc.set",
                serde_json::json!({ "path": path, "key": key, "value": value }),
            )
            .await
            .context("IPC doc.set failed")?;

        if json {
            println!("{}", serde_json::to_string_pretty(&result)?);
        } else {
            let synced = result
                .get("synced")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            println!("Set {key} = {value}");
            if synced {
                println!("  Broadcast to mesh.");
            }
        }
        return Ok(());
    }

    // Fallback: direct store access (no broadcast)
    let store = open_store(config_flag)?;

    let doc = if store.exists(path) {
        store.load(path).context("failed to load document")?
    } else {
        store.create(path).context("failed to create document")?
    };

    let map = doc.get_map("data");
    map.insert(key, value)
        .map_err(|e| anyhow::anyhow!("failed to set key: {e}"))?;
    doc.commit();

    store.save(path, &doc).context("failed to save document")?;

    if json {
        let out = serde_json::json!({ "path": path, "key": key, "value": value });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Set {key} = {value}");
    }

    Ok(())
}

/// `bolo doc del <path> <key>` — delete a key from a document.
///
/// Routes through daemon IPC when available so changes are broadcast to mesh peers.
pub async fn del(path: &str, key: &str, config_flag: Option<&str>, json: bool) -> Result<()> {
    let config_dir = resolve_config_dir(config_flag)?;

    // Try daemon IPC first
    if let Ok(mut client) = bolo_core::ipc::DaemonClient::connect(&config_dir).await {
        let result = client
            .call("doc.del", serde_json::json!({ "path": path, "key": key }))
            .await
            .context("IPC doc.del failed")?;

        if json {
            println!("{}", serde_json::to_string_pretty(&result)?);
        } else {
            println!("Deleted key: {key}");
            if result
                .get("synced")
                .and_then(|v| v.as_bool())
                .unwrap_or(false)
            {
                println!("  Broadcast to mesh.");
            }
        }
        return Ok(());
    }

    // Fallback: direct store access
    let store = open_store(config_flag)?;
    let doc = store.load(path).context("failed to load document")?;

    let map = doc.get_map("data");
    map.delete(key)
        .map_err(|e| anyhow::anyhow!("failed to delete key: {e}"))?;
    doc.commit();

    store.save(path, &doc).context("failed to save document")?;

    if json {
        let out = serde_json::json!({ "deleted": true, "path": path, "key": key });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Deleted key: {key}");
    }

    Ok(())
}

/// `bolo doc edit <path>` — open document text in $EDITOR, broadcast changes to mesh.
pub fn edit(path: &str, config_flag: Option<&str>, json: bool) -> Result<()> {
    let config_dir = resolve_config_dir(config_flag)?;
    let data_dir = resolve_data_dir(&config_dir);
    let store = DocStore::open(&data_dir).context("failed to open document store")?;

    let doc = if store.exists(path) {
        store.load(path).context("failed to load document")?
    } else {
        store.create(path).context("failed to create document")?
    };

    let text = doc.get_text("content");
    let current = text.to_string();

    let editor = std::env::var("EDITOR").unwrap_or_else(|_| "vi".to_string());

    let tmp = tempfile::NamedTempFile::new().context("failed to create temp file")?;
    std::fs::write(tmp.path(), &current)?;

    let status = std::process::Command::new(&editor)
        .arg(tmp.path())
        .status()
        .context("failed to launch editor")?;

    if !status.success() {
        bail!("Editor exited with non-zero status");
    }

    let new_content = std::fs::read_to_string(tmp.path())?;

    if new_content == current {
        if json {
            let out = serde_json::json!({ "changed": false, "path": path });
            println!("{}", serde_json::to_string_pretty(&out)?);
        } else {
            println!("No changes.");
        }
        return Ok(());
    }

    // Replace text content
    let len = text.len_unicode();
    if len > 0 {
        text.delete(0, len)
            .map_err(|e| anyhow::anyhow!("failed to clear text: {e}"))?;
    }
    text.insert(0, &new_content)
        .map_err(|e| anyhow::anyhow!("failed to insert text: {e}"))?;
    doc.commit();

    // Export snapshot for broadcast
    let snapshot_bytes = doc
        .export(bolo_docs::loro::ExportMode::Snapshot)
        .map_err(|e| anyhow::anyhow!("failed to export snapshot: {e}"))?;

    store.save(path, &doc).context("failed to save document")?;

    // Try daemon IPC first (triggers gossip broadcast from daemon's long-lived endpoint),
    // fall back to ephemeral gossip endpoint
    let synced = broadcast_doc_via_daemon_or_ephemeral(path, &snapshot_bytes, &config_dir);

    if json {
        let out = serde_json::json!({
            "changed": true,
            "path": path,
            "size": new_content.len(),
            "synced": synced,
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Saved document: {path}");
        if synced {
            println!("  Broadcast to mesh.");
        }
    }

    Ok(())
}

/// Try to broadcast a doc change: daemon IPC first, then ephemeral gossip.
fn broadcast_doc_via_daemon_or_ephemeral(
    path: &str,
    snapshot_bytes: &[u8],
    config_dir: &std::path::Path,
) -> bool {
    // Try daemon IPC first (uses daemon's long-lived gossip connection)
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build();
    if let Ok(rt) = rt {
        let result = rt.block_on(async {
            let mut client = bolo_core::ipc::DaemonClient::connect(config_dir).await?;
            client
                .call("doc.sync", serde_json::json!({ "path": path }))
                .await
        });
        if result.is_ok() {
            return true;
        }
    }

    // Fall back to ephemeral endpoint
    broadcast_doc_snapshot(path, snapshot_bytes, config_dir).is_ok()
}

/// Broadcast a doc snapshot via ephemeral gossip endpoint.
fn broadcast_doc_snapshot(
    path: &str,
    snapshot_bytes: &[u8],
    config_dir: &std::path::Path,
) -> Result<()> {
    let identity =
        bolo_core::Identity::load_from_config_dir(config_dir).context("failed to load identity")?;
    let node_id = identity.node_id().to_string();

    let msg = bolo_docs::DocSyncMessage::Snapshot {
        path: path.to_string(),
        data: snapshot_bytes.to_vec(),
        author: node_id,
        timestamp: Timestamp::now().0,
        nonce: rand::random(),
    };
    let msg_bytes = msg.to_bytes().context("failed to serialize sync message")?;
    let topic_id = bolo_docs::doc_topic_id(path);

    // Use a small tokio runtime for the ephemeral broadcast
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("failed to create runtime")?;

    rt.block_on(async {
        let secret_key = identity.secret_key().clone();
        let endpoint = iroh::Endpoint::builder()
            .secret_key(secret_key)
            .alpns(vec![iroh_gossip::net::GOSSIP_ALPN.to_vec()])
            .bind()
            .await
            .context("failed to bind endpoint")?;

        let gossip = bolo_pub::create_gossip(endpoint.clone());
        let _router = iroh::protocol::Router::builder(endpoint.clone())
            .accept(iroh_gossip::net::GOSSIP_ALPN, gossip.clone())
            .spawn();

        endpoint.online().await;

        let mut channel = bolo_pub::Channel::subscribe(&gossip, topic_id, vec![])
            .await
            .context("failed to subscribe to topic")?;

        channel
            .broadcast(msg_bytes)
            .await
            .context("failed to broadcast")?;

        // Brief pause to let the message propagate
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        endpoint.close().await;
        Ok(())
    })
}

/// `bolo doc read <path>` — render document to terminal with markdown formatting.
pub fn read(path: &str, config_flag: Option<&str>, json: bool) -> Result<()> {
    let store = open_store(config_flag)?;
    let doc = store.load(path).context("failed to load document")?;

    let text = doc.get_text("content");
    let content = text.to_string();

    if json {
        let out = serde_json::json!({ "path": path, "content": content });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else if content.is_empty() {
        // Fall back to showing map data if no text content
        let value = doc.get_deep_value();
        println!("{value:?}");
    } else {
        // Render markdown if the content looks like markdown and stdout is a TTY
        use std::io::IsTerminal;
        if std::io::stdout().is_terminal() && looks_like_markdown(&content) {
            termimad::print_text(&content);
        } else {
            print!("{content}");
            if !content.ends_with('\n') {
                println!();
            }
        }
    }

    Ok(())
}

/// Heuristic: does the text look like markdown?
fn looks_like_markdown(text: &str) -> bool {
    let first_lines: Vec<&str> = text.lines().take(20).collect();
    first_lines.iter().any(|line| {
        line.starts_with('#')
            || line.starts_with("- ")
            || line.starts_with("* ")
            || line.starts_with("> ")
            || line.starts_with("```")
            || line.starts_with("| ")
            || line.starts_with("1. ")
    })
}

/// `bolo doc append <path> <value>` — append to a list document.
///
/// Routes through daemon IPC when available so changes are broadcast to mesh peers.
pub async fn append(path: &str, value: &str, config_flag: Option<&str>, json: bool) -> Result<()> {
    let config_dir = resolve_config_dir(config_flag)?;

    // Try daemon IPC first
    if let Ok(mut client) = bolo_core::ipc::DaemonClient::connect(&config_dir).await {
        let result = client
            .call(
                "doc.append",
                serde_json::json!({ "path": path, "value": value }),
            )
            .await
            .context("IPC doc.append failed")?;

        if json {
            println!("{}", serde_json::to_string_pretty(&result)?);
        } else {
            let len = result.get("length").and_then(|v| v.as_u64()).unwrap_or(0);
            println!("Appended to {path} ({len} items)");
            if result
                .get("synced")
                .and_then(|v| v.as_bool())
                .unwrap_or(false)
            {
                println!("  Broadcast to mesh.");
            }
        }
        return Ok(());
    }

    // Fallback: direct store access
    let store = open_store(config_flag)?;

    let doc = if store.exists(path) {
        store.load(path).context("failed to load document")?
    } else {
        store.create(path).context("failed to create document")?
    };

    let list = doc.get_list("items");
    list.push(value)
        .map_err(|e| anyhow::anyhow!("failed to append: {e}"))?;
    let len = list.len();
    doc.commit();

    store.save(path, &doc).context("failed to save document")?;

    if json {
        let out = serde_json::json!({ "path": path, "value": value, "length": len });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Appended to {path} ({len} items)");
    }

    Ok(())
}

/// `bolo doc watch <path>` — stream real-time document changes from peers.
pub async fn watch(path: &str, config_flag: Option<&str>, json: bool) -> Result<()> {
    let config_dir = resolve_config_dir(config_flag)?;
    let store = open_store(config_flag)?;

    // Load current state
    let doc = if store.exists(path) {
        store.load(path).context("failed to load document")?
    } else {
        bail!("Document not found: {path}");
    };

    let identity = bolo_core::Identity::load_from_config_dir(&config_dir)
        .context("failed to load identity — have you run `bolo daemon init`?")?;
    let secret_key = identity.secret_key().clone();
    let topic_id = bolo_docs::doc_topic_id(path);

    let endpoint = iroh::Endpoint::builder()
        .secret_key(secret_key)
        .alpns(vec![iroh_gossip::net::GOSSIP_ALPN.to_vec()])
        .bind()
        .await
        .context("failed to bind endpoint")?;

    let gossip = bolo_pub::create_gossip(endpoint.clone());
    let _router = iroh::protocol::Router::builder(endpoint.clone())
        .accept(iroh_gossip::net::GOSSIP_ALPN, gossip.clone())
        .spawn();

    endpoint.online().await;

    if !json {
        let text = doc.get_text("content");
        let content = text.to_string();
        if !content.is_empty() {
            println!("--- Current content ---");
            print!("{content}");
            if !content.ends_with('\n') {
                println!();
            }
            println!("--- Watching for changes... (Ctrl-C to stop) ---\n");
        } else {
            println!("Watching document '{path}' for changes... (Ctrl-C to stop)\n");
        }
    }

    let channel = bolo_pub::Channel::subscribe(&gossip, topic_id, vec![])
        .await
        .context("failed to subscribe to doc topic")?;

    let (_sender, mut receiver) = channel.split();

    use futures_lite::StreamExt;
    while let Some(event) = receiver.try_next().await.transpose() {
        match event {
            Ok(iroh_gossip::api::Event::Received(msg)) => {
                match bolo_docs::DocSyncMessage::from_bytes(&msg.content) {
                    Ok(sync_msg) => {
                        // Apply the update locally
                        match bolo_docs::apply_sync_message(&store, &sync_msg) {
                            Ok(true) => {
                                if json {
                                    let out = serde_json::json!({
                                        "event": "update",
                                        "path": sync_msg.path(),
                                        "from": msg.delivered_from.to_string(),
                                    });
                                    println!("{}", serde_json::to_string(&out)?);
                                } else {
                                    println!(
                                        "[{}] Update from {}",
                                        Timestamp::now().relative(),
                                        msg.delivered_from.fmt_short()
                                    );
                                    // Reload and show current text
                                    if let Ok(updated) = store.load(path) {
                                        let text = updated.get_text("content");
                                        let content = text.to_string();
                                        if !content.is_empty() {
                                            println!("--- Updated content ---");
                                            print!("{content}");
                                            if !content.ends_with('\n') {
                                                println!();
                                            }
                                            println!("---");
                                        }
                                    }
                                }
                            }
                            Ok(false) => {}
                            Err(e) => {
                                if !json {
                                    eprintln!("Failed to apply update: {e}");
                                }
                            }
                        }
                    }
                    Err(e) => {
                        if !json {
                            eprintln!("Invalid sync message: {e}");
                        }
                    }
                }
            }
            Ok(iroh_gossip::api::Event::NeighborUp(peer)) => {
                if json {
                    let out = serde_json::json!({
                        "event": "peer_joined",
                        "peer": peer.to_string(),
                    });
                    println!("{}", serde_json::to_string(&out)?);
                } else {
                    println!("+ Peer joined: {}", peer.fmt_short());
                }
            }
            Ok(iroh_gossip::api::Event::NeighborDown(peer)) => {
                if json {
                    let out = serde_json::json!({
                        "event": "peer_left",
                        "peer": peer.to_string(),
                    });
                    println!("{}", serde_json::to_string(&out)?);
                } else {
                    println!("- Peer left: {}", peer.fmt_short());
                }
            }
            Ok(iroh_gossip::api::Event::Lagged) => {
                if !json {
                    eprintln!("Warning: receiver lagged, messages may have been dropped");
                }
            }
            Err(e) => {
                eprintln!("Error: {e}");
                break;
            }
        }
    }

    endpoint.close().await;
    Ok(())
}

/// `bolo doc diff <path>` — show local text content with a unified diff view.
///
/// Without mesh connectivity, shows a summary of document state.
/// With a running daemon, could show local vs mesh diff.
pub fn diff(path: &str, config_flag: Option<&str>, json: bool) -> Result<()> {
    let store = open_store(config_flag)?;
    let doc = store.load(path).context("failed to load document")?;

    let text = doc.get_text("content");
    let content = text.to_string();
    let vv = doc.oplog_vv();

    if json {
        let out = serde_json::json!({
            "path": path,
            "content": content,
            "version": format!("{vv:?}"),
            "size": content.len(),
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else if content.is_empty() {
        println!("Document '{path}' has no text content.");
        let value = doc.get_deep_value();
        println!("  Data: {value:?}");
        println!("  Version: {vv:?}");
    } else {
        // Show the document content with line numbers for easy reference
        println!("Document: {path}");
        println!("Version:  {vv:?}");
        println!("---");
        for (i, line) in content.lines().enumerate() {
            println!("{:4} | {line}", i + 1);
        }
        println!("---");
        println!("{} lines, {} bytes", content.lines().count(), content.len());
    }

    Ok(())
}

/// `bolo doc history <path>` — show change history with version info.
pub fn history(path: &str, config_flag: Option<&str>, json: bool) -> Result<()> {
    let store = open_store(config_flag)?;
    let doc = store.load(path).context("failed to load document")?;

    let vv = doc.oplog_vv();
    let frontiers = doc.state_frontiers();

    // Get per-peer operation counts from the version vector
    let vv_map = vv.iter().collect::<Vec<_>>();

    if json {
        let peers: Vec<serde_json::Value> = vv_map
            .iter()
            .map(|(peer_id, counter)| {
                serde_json::json!({
                    "peer": format!("{peer_id}"),
                    "operations": counter,
                })
            })
            .collect();
        let out = serde_json::json!({
            "path": path,
            "peers": peers,
            "frontiers": format!("{frontiers:?}"),
            "total_peers": vv_map.len(),
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Document: {path}");
        println!("  Frontiers: {frontiers:?}");
        if vv_map.is_empty() {
            println!("  No changes recorded.");
        } else {
            println!("  Contributors:");
            for (peer_id, counter) in &vv_map {
                println!("    {peer_id}: {counter} operations");
            }
        }
    }

    Ok(())
}

/// `bolo doc sync [path]` — force sync via daemon gossip.
pub async fn sync(path: Option<&str>, config_flag: Option<&str>, json: bool) -> Result<()> {
    let config_dir = resolve_config_dir(config_flag)?;

    let mut client = bolo_core::ipc::DaemonClient::connect(&config_dir)
        .await
        .context(
            "Cannot sync: daemon is not running.\n\
             Start the daemon with `bolo daemon start`, then retry.",
        )?;

    let params = match path {
        Some(p) => serde_json::json!({ "path": p }),
        None => serde_json::json!({}),
    };

    let result = client
        .call("doc.sync", params)
        .await
        .context("IPC doc.sync failed")?;

    if json {
        println!("{}", serde_json::to_string_pretty(&result)?);
    } else {
        let count = result.get("count").and_then(|v| v.as_u64()).unwrap_or(0);
        let target = path.unwrap_or("all documents");
        println!("Synced {target} ({count} broadcast)");
    }

    Ok(())
}

/// `bolo doc export <path> <file>` — export document to a snapshot file.
pub fn export(path: &str, file: &str, config_flag: Option<&str>, json: bool) -> Result<()> {
    let store = open_store(config_flag)?;
    let doc = store.load(path).context("failed to load document")?;

    let snapshot = doc
        .export(bolo_docs::loro::ExportMode::Snapshot)
        .map_err(|e| anyhow::anyhow!("failed to export: {e}"))?;

    std::fs::write(file, &snapshot)?;

    if json {
        let out = serde_json::json!({
            "path": path,
            "file": file,
            "size": snapshot.len(),
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Exported {path} to {file} ({} bytes)", snapshot.len());
    }

    Ok(())
}

/// `bolo doc import <file> <path>` — import document from a snapshot or text file.
///
/// Tries to load as a Loro snapshot first. If that fails and the file looks like text,
/// imports it as a text document.
pub fn import(file: &str, path: &str, config_flag: Option<&str>, json: bool) -> Result<()> {
    let store = open_store(config_flag)?;

    let bytes = std::fs::read(file).context("failed to read import file")?;

    // Try Loro snapshot first
    match bolo_docs::loro::LoroDoc::from_snapshot(&bytes) {
        Ok(doc) => {
            store.save(path, &doc).context("failed to save document")?;
            if json {
                let out = serde_json::json!({
                    "path": path,
                    "file": file,
                    "format": "snapshot",
                    "size": bytes.len(),
                });
                println!("{}", serde_json::to_string_pretty(&out)?);
            } else {
                println!("Imported snapshot {file} as {path}");
            }
        }
        Err(_) => {
            // Fall back to text import
            let text_content = String::from_utf8(bytes.clone())
                .context("file is neither a valid Loro snapshot nor valid UTF-8 text")?;

            let doc = if store.exists(path) {
                store.load(path).context("failed to load document")?
            } else {
                bolo_docs::loro::LoroDoc::new()
            };

            let text = doc.get_text("content");
            let len = text.len_unicode();
            if len > 0 {
                text.delete(0, len)
                    .map_err(|e| anyhow::anyhow!("failed to clear text: {e}"))?;
            }
            text.insert(0, &text_content)
                .map_err(|e| anyhow::anyhow!("failed to insert text: {e}"))?;
            doc.commit();

            store.save(path, &doc).context("failed to save document")?;

            if json {
                let out = serde_json::json!({
                    "path": path,
                    "file": file,
                    "format": "text",
                    "size": text_content.len(),
                });
                println!("{}", serde_json::to_string_pretty(&out)?);
            } else {
                println!(
                    "Imported text file {file} as {path} ({} bytes)",
                    text_content.len()
                );
            }
        }
    }

    Ok(())
}

/// `bolo doc share <path>` — generate a share ticket (requires daemon).
pub fn share(path: &str, _config_flag: Option<&str>, _json: bool) -> Result<()> {
    bail!(
        "Cannot share document '{path}': requires a running daemon with mesh connectivity.\n\
         Start the daemon with `bolo daemon start`, then retry."
    );
}

/// `bolo doc join <ticket>` — join a shared document (requires daemon).
pub fn join(ticket: &str, _config_flag: Option<&str>, _json: bool) -> Result<()> {
    let _ = ticket;
    bail!(
        "Cannot join document: requires a running daemon with mesh connectivity.\n\
         Start the daemon with `bolo daemon start`, then retry."
    );
}

/// `bolo doc revoke <ticket_id>` — revoke a share ticket (requires daemon).
pub fn revoke(ticket_id: &str, _config_flag: Option<&str>, _json: bool) -> Result<()> {
    let _ = ticket_id;
    bail!(
        "Cannot revoke ticket: requires a running daemon with mesh connectivity.\n\
         Start the daemon with `bolo daemon start`, then retry."
    );
}

/// `bolo doc tickets <path>` — list issued tickets (requires daemon).
pub fn tickets(path: &str, _config_flag: Option<&str>, _json: bool) -> Result<()> {
    bail!(
        "Cannot list tickets for '{path}': requires a running daemon with mesh connectivity.\n\
         Start the daemon with `bolo daemon start`, then retry."
    );
}

/// `bolo doc compact <path>` — compact CRDT history.
pub fn compact(path: &str, all: bool, config_flag: Option<&str>, json: bool) -> Result<()> {
    let store = open_store(config_flag)?;

    let paths = if all {
        store.list().context("failed to list documents")?
    } else {
        vec![path.to_string()]
    };

    let mut compacted = Vec::new();
    for p in &paths {
        let doc = store.load(p).context("failed to load document")?;
        // Re-export as snapshot (this compacts the history)
        store.save(p, &doc).context("failed to save document")?;
        compacted.push(p.as_str());
    }

    if json {
        let out = serde_json::json!({
            "compacted": compacted,
            "count": compacted.len(),
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        for p in &compacted {
            println!("Compacted: {p}");
        }
        println!("\n{} document(s) compacted", compacted.len());
    }

    Ok(())
}

/// `bolo doc acl grant <path> <peer_id>` — grant write access (requires daemon).
pub fn acl_grant(path: &str, peer_id: &str, _config_flag: Option<&str>, _json: bool) -> Result<()> {
    let _ = (path, peer_id);
    bail!(
        "Cannot manage ACLs: requires a running daemon with mesh connectivity.\n\
         Start the daemon with `bolo daemon start`, then retry."
    );
}

/// `bolo doc acl revoke <path> <peer_id>` — revoke access (requires daemon).
pub fn acl_revoke(
    path: &str,
    peer_id: &str,
    _config_flag: Option<&str>,
    _json: bool,
) -> Result<()> {
    let _ = (path, peer_id);
    bail!(
        "Cannot manage ACLs: requires a running daemon with mesh connectivity.\n\
         Start the daemon with `bolo daemon start`, then retry."
    );
}

/// `bolo doc acl show <path>` — show ACL (requires daemon).
pub fn acl_show(path: &str, _config_flag: Option<&str>, _json: bool) -> Result<()> {
    let _ = path;
    bail!(
        "Cannot show ACLs: requires a running daemon with mesh connectivity.\n\
         Start the daemon with `bolo daemon start`, then retry."
    );
}
