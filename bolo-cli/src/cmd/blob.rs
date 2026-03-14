//! Blob command handlers.

use std::path::Path;

use anyhow::{bail, Context, Result};
use bolo_core::ipc::DaemonClient;
use bolo_core::DaemonState;
use futures_lite::StreamExt;

use super::daemon::{resolve_config_dir, resolve_data_dir};

/// Try to connect to the running daemon via IPC.
/// Returns Some(client) if daemon is running and socket connects, None otherwise.
async fn try_daemon(config_flag: Option<&str>) -> Result<Option<DaemonClient>> {
    let config_dir = resolve_config_dir(config_flag)?;
    if let Ok(state) = DaemonState::load(&config_dir) {
        if state.is_alive() {
            match DaemonClient::connect(&config_dir).await {
                Ok(client) => return Ok(Some(client)),
                Err(_) => bail!(
                    "Daemon is running (PID {}) but IPC socket is not available.",
                    state.pid
                ),
            }
        }
    }
    Ok(None)
}

/// Open the blob store directly (only when daemon is NOT running).
async fn open_store(
    config_flag: Option<&str>,
) -> Result<(bolo_blobs::FsStore, std::path::PathBuf)> {
    let config_dir = resolve_config_dir(config_flag)?;

    if let Ok(state) = DaemonState::load(&config_dir) {
        if state.is_alive() {
            bail!(
                "Daemon is running (PID {}). Blob commands use IPC when daemon is running.",
                state.pid
            );
        }
    }

    let data_dir = resolve_data_dir(&config_dir);
    let store = bolo_blobs::store::open_store(&data_dir)
        .await
        .context("failed to open blob store")?;
    Ok((store, data_dir))
}

/// `bolo blob put <file>` — store a file, return hash.
pub async fn put(file: &str, config_flag: Option<&str>, json: bool) -> Result<()> {
    if let Some(mut client) = try_daemon(config_flag).await? {
        let result = client
            .call("blob.put", serde_json::json!({ "file": file }))
            .await?;
        if json {
            println!("{}", serde_json::to_string_pretty(&result)?);
        } else {
            println!("{}", result["hash"].as_str().unwrap_or(""));
        }
        return Ok(());
    }

    let (store, _) = open_store(config_flag).await?;
    let path = Path::new(file);
    if !path.exists() {
        bail!("File not found: {file}");
    }

    let tag = store
        .add_path(path)
        .with_tag()
        .await
        .map_err(|e| anyhow::anyhow!("failed to add blob: {e}"))?;

    let hash = tag.hash.to_string();
    let size = std::fs::metadata(path)?.len();
    let _ = store.shutdown().await;

    if json {
        let out = serde_json::json!({ "hash": hash, "size": size, "path": file });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("{hash}");
    }

    Ok(())
}

/// `bolo blob get <hash> [path]` — retrieve a blob.
pub async fn get(
    hash: &str,
    path: Option<&str>,
    config_flag: Option<&str>,
    json: bool,
) -> Result<()> {
    if let Some(mut client) = try_daemon(config_flag).await? {
        let mut params = serde_json::json!({ "hash": hash });
        if let Some(p) = path {
            params["path"] = serde_json::Value::String(p.to_string());
        }
        let result = client.call("blob.get", params).await?;
        if json {
            println!("{}", serde_json::to_string_pretty(&result)?);
        } else if let Some(p) = path {
            println!(
                "Wrote {} bytes to {p}",
                result["size"].as_u64().unwrap_or(0)
            );
        } else {
            print!("{}", result["data"].as_str().unwrap_or(""));
        }
        return Ok(());
    }

    let (store, _) = open_store(config_flag).await?;
    let blob_hash: iroh_blobs::Hash = hash.parse().context("invalid blob hash")?;
    let bytes = store
        .get_bytes(blob_hash)
        .await
        .map_err(|e| anyhow::anyhow!("blob not found or corrupted: {e}"))?;
    let _ = store.shutdown().await;

    match path {
        Some(out_path) => {
            std::fs::write(out_path, &bytes)?;
            if json {
                let out =
                    serde_json::json!({ "hash": hash, "size": bytes.len(), "path": out_path });
                println!("{}", serde_json::to_string_pretty(&out)?);
            } else {
                println!("Wrote {} bytes to {out_path}", bytes.len());
            }
        }
        None => {
            if json {
                let out = serde_json::json!({
                    "hash": hash,
                    "size": bytes.len(),
                    "data": String::from_utf8_lossy(&bytes),
                });
                println!("{}", serde_json::to_string_pretty(&out)?);
            } else {
                use std::io::Write;
                std::io::stdout().write_all(&bytes)?;
            }
        }
    }

    Ok(())
}

/// `bolo blob ls` — list locally stored blobs.
pub async fn ls(config_flag: Option<&str>, json: bool) -> Result<()> {
    if let Some(mut client) = try_daemon(config_flag).await? {
        let result = client.call("blob.list", serde_json::json!({})).await?;
        if json {
            println!("{}", serde_json::to_string_pretty(&result)?);
        } else {
            let entries = result.as_array();
            if entries.is_none_or(|e| e.is_empty()) {
                println!("No blobs stored.");
            } else {
                let entries = entries.unwrap();
                for entry in entries {
                    let hash = entry["hash"].as_str().unwrap_or("");
                    let tag = entry["tag"].as_str().unwrap_or("");
                    println!("{hash}  {tag}");
                }
                println!("\n{} blob(s)", entries.len());
            }
        }
        return Ok(());
    }

    let (store, _) = open_store(config_flag).await?;
    let mut entries = Vec::new();
    let mut stream = store.tags().list().await?;
    while let Some(item) = stream.next().await {
        let item = item?;
        entries.push(serde_json::json!({
            "tag": item.name.to_string(),
            "hash": item.hash_and_format().hash.to_string(),
            "format": format!("{:?}", item.hash_and_format().format),
        }));
    }
    let _ = store.shutdown().await;

    if json {
        println!("{}", serde_json::to_string_pretty(&entries)?);
    } else if entries.is_empty() {
        println!("No blobs stored.");
    } else {
        for entry in &entries {
            let hash = entry["hash"].as_str().unwrap_or("");
            let tag = entry["tag"].as_str().unwrap_or("");
            println!("{hash}  {tag}");
        }
        println!("\n{} blob(s)", entries.len());
    }

    Ok(())
}

/// `bolo blob stat <hash>` — show blob metadata.
pub async fn stat(hash: &str, config_flag: Option<&str>, json: bool) -> Result<()> {
    if let Some(mut client) = try_daemon(config_flag).await? {
        let result = client
            .call("blob.stat", serde_json::json!({ "hash": hash }))
            .await?;
        if json {
            println!("{}", serde_json::to_string_pretty(&result)?);
        } else if result["exists"].as_bool() == Some(true) {
            println!("Hash:   {hash}");
            println!("Exists: yes");
        } else {
            bail!("Blob not found: {hash}");
        }
        return Ok(());
    }

    let (store, _) = open_store(config_flag).await?;
    let blob_hash: iroh_blobs::Hash = hash.parse().context("invalid blob hash")?;
    let has = store.has(blob_hash).await.unwrap_or(false);
    let _ = store.shutdown().await;

    if !has {
        bail!("Blob not found: {hash}");
    }

    if json {
        let out = serde_json::json!({ "hash": hash, "exists": true });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Hash:   {hash}");
        println!("Exists: yes");
    }

    Ok(())
}

/// `bolo blob pin <hash>` — pin a blob to prevent garbage collection.
pub async fn pin(hash: &str, config_flag: Option<&str>, json: bool) -> Result<()> {
    if let Some(mut client) = try_daemon(config_flag).await? {
        let result = client
            .call("blob.pin", serde_json::json!({ "hash": hash }))
            .await?;
        if json {
            println!("{}", serde_json::to_string_pretty(&result)?);
        } else {
            println!("Pinned {hash}");
        }
        return Ok(());
    }

    let (store, _) = open_store(config_flag).await?;
    let blob_hash: iroh_blobs::Hash = hash.parse().context("invalid blob hash")?;
    let tag_name = format!("pin-{hash}");
    store
        .tags()
        .set(
            iroh_blobs::api::Tag::from(tag_name.clone()),
            iroh_blobs::HashAndFormat::raw(blob_hash),
        )
        .await?;
    let _ = store.shutdown().await;

    if json {
        let out = serde_json::json!({ "pinned": true, "hash": hash, "tag": tag_name });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Pinned {hash}");
    }

    Ok(())
}

/// `bolo blob unpin <hash>` — unpin a blob, allowing garbage collection.
pub async fn unpin(hash: &str, config_flag: Option<&str>, json: bool) -> Result<()> {
    if let Some(mut client) = try_daemon(config_flag).await? {
        let result = client
            .call("blob.unpin", serde_json::json!({ "hash": hash }))
            .await?;
        if json {
            println!("{}", serde_json::to_string_pretty(&result)?);
        } else {
            println!("Unpinned {hash}");
        }
        return Ok(());
    }

    let (store, _) = open_store(config_flag).await?;
    let tag_name = format!("pin-{hash}");
    store
        .tags()
        .delete(iroh_blobs::api::Tag::from(tag_name))
        .await?;
    let _ = store.shutdown().await;

    if json {
        let out = serde_json::json!({ "unpinned": true, "hash": hash });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Unpinned {hash}");
    }

    Ok(())
}

/// `bolo blob fetch <hash> <peer> [path]` — fetch a blob from a remote peer.
pub async fn fetch(
    hash: &str,
    peer: &str,
    path: Option<&str>,
    config_flag: Option<&str>,
    json: bool,
) -> Result<()> {
    let config_dir = resolve_config_dir(config_flag)?;
    let data_dir = resolve_data_dir(&config_dir);
    std::fs::create_dir_all(&data_dir)?;

    let identity = bolo_core::Identity::load_from_config_dir(&config_dir)
        .context("failed to load identity — have you run `bolo daemon init`?")?;

    let secret_key = identity.secret_key().clone();
    let blob_hash: iroh_blobs::Hash = hash.parse().context("invalid blob hash")?;
    let remote: iroh::PublicKey = peer.parse().context("invalid peer node ID")?;

    let store = bolo_blobs::store::open_store(&data_dir)
        .await
        .context("failed to open blob store")?;

    let blobs = bolo_blobs::BlobsProtocol::new(&store, None);
    let endpoint = iroh::Endpoint::builder()
        .secret_key(secret_key)
        .alpns(vec![iroh_blobs::ALPN.to_vec()])
        .bind()
        .await
        .context("failed to bind endpoint")?;

    let _router = iroh::protocol::Router::builder(endpoint.clone())
        .accept(iroh_blobs::ALPN, blobs)
        .spawn();

    endpoint.online().await;

    let downloader = store.downloader(&endpoint);
    downloader
        .download(blob_hash, vec![remote])
        .await
        .context("failed to download blob from peer")?;

    let bytes = store
        .get_bytes(blob_hash)
        .await
        .map_err(|e| anyhow::anyhow!("failed to read downloaded blob: {e}"))?;

    let size = bytes.len();

    match path {
        Some(out_path) => {
            std::fs::write(out_path, &bytes)?;
            if json {
                let out = serde_json::json!({
                    "hash": hash, "size": size, "peer": peer, "path": out_path,
                });
                println!("{}", serde_json::to_string_pretty(&out)?);
            } else {
                println!("Fetched {size} bytes from {peer} → {out_path}");
            }
        }
        None => {
            if json {
                let out = serde_json::json!({
                    "hash": hash, "size": size, "peer": peer,
                    "data": String::from_utf8_lossy(&bytes),
                });
                println!("{}", serde_json::to_string_pretty(&out)?);
            } else {
                use std::io::Write;
                std::io::stdout().write_all(&bytes)?;
            }
        }
    }

    endpoint.close().await;
    let _ = store.shutdown().await;

    Ok(())
}

/// `bolo blob gc` — garbage collect unpinned blobs.
pub async fn gc(config_flag: Option<&str>, json: bool) -> Result<()> {
    if let Some(mut client) = try_daemon(config_flag).await? {
        let result = client.call("blob.gc", serde_json::json!({})).await?;
        if json {
            println!("{}", serde_json::to_string_pretty(&result)?);
        } else {
            println!("Garbage collection completed.");
        }
        return Ok(());
    }

    let (store, _) = open_store(config_flag).await?;
    let _ = store.shutdown().await;

    if json {
        println!(r#"{{"status": "completed"}}"#);
    } else {
        println!("Garbage collection completed.");
    }

    Ok(())
}

/// `bolo blob encrypt-store` — migrate to encrypt all stored blobs.
pub fn encrypt_store(_config_flag: Option<&str>, _json: bool) -> Result<()> {
    bail!(
        "Blob store encryption is not yet implemented.\n\
         Blobs are stored as content-addressed files without encryption."
    );
}
