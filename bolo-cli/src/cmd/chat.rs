//! Chat command handlers.

use anyhow::{Context, Result};
use bolo_chat::{ChatGossipMessage, ChatMessage, ChatStore, ChatWireMessage};
use bolo_core::crypto::{derive_gossip_key, maybe_open, maybe_seal, ChannelKey};
use bolo_core::BoloConfig;

use super::daemon::resolve_config_dir;

fn open_chat_store(config_flag: Option<&str>) -> Result<ChatStore> {
    let config_dir = resolve_config_dir(config_flag)?;
    let data_dir = super::daemon::resolve_data_dir(&config_dir);
    ChatStore::open(&data_dir).context("failed to open chat store")
}

/// Load the chat gossip key from config, if mesh_secret is set.
fn load_chat_key(config_flag: Option<&str>, channel: &str) -> Result<Option<ChannelKey>> {
    let config_dir = resolve_config_dir(config_flag)?;
    let config = BoloConfig::load(Some(&config_dir.join("config.toml")))?;
    let mesh_secret = config.crypto.mesh_secret_bytes()?;
    Ok(mesh_secret.map(|s| derive_gossip_key(&s, &format!("chat/{channel}"))))
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// `bolo chat join <channel>` — join a chat channel.
pub async fn join(channel: &str, config_flag: Option<&str>, json: bool) -> Result<()> {
    let store = open_chat_store(config_flag)?;
    store.join_channel(channel)?;

    // If daemon is running, subscribe to the gossip topic via IPC
    let config_dir = resolve_config_dir(config_flag)?;
    if let Ok(mut client) = bolo_core::ipc::DaemonClient::connect(&config_dir).await {
        let _ = client
            .call("chat.join", serde_json::json!({ "channel": channel }))
            .await;
    }

    if json {
        let out = serde_json::json!({ "joined": true, "channel": channel });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Joined #{channel}");
    }
    Ok(())
}

/// `bolo chat leave <channel>` — leave a chat channel.
pub fn leave(channel: &str, config_flag: Option<&str>, json: bool) -> Result<()> {
    let store = open_chat_store(config_flag)?;
    store.leave_channel(channel)?;

    if json {
        let out = serde_json::json!({ "left": true, "channel": channel });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Left #{channel}");
    }
    Ok(())
}

/// `bolo chat ls` — list joined channels.
pub fn ls(config_flag: Option<&str>, json: bool) -> Result<()> {
    let store = open_chat_store(config_flag)?;
    let channels = store.list_channels()?;

    if json {
        let out = serde_json::json!({ "channels": channels });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else if channels.is_empty() {
        println!("No joined channels.");
    } else {
        for ch in &channels {
            println!("  #{ch}");
        }
        println!("\n{} channel(s)", channels.len());
    }
    Ok(())
}

/// `bolo chat send <channel> <message>` — send a signed message.
pub async fn send(
    channel: &str,
    content: &str,
    parent: Option<&str>,
    blob: Option<&str>,
    peers: &[String],
    config_flag: Option<&str>,
    json: bool,
) -> Result<()> {
    let config_dir = resolve_config_dir(config_flag)?;
    let identity = bolo_core::Identity::load_from_config_dir(&config_dir)
        .context("failed to load identity — have you run `bolo daemon init`?")?;
    let sender = identity.node_id().to_string();
    let timestamp = now_ms();

    // Sign the message
    let signing_bytes = ChatMessage::signing_bytes(channel, &sender, timestamp, content);
    let signature = identity.sign(&signing_bytes);
    let signature_hex = hex::encode(&signature.to_bytes());

    let id = ChatMessage::compute_id(channel, &sender, timestamp, content);
    let msg = ChatMessage {
        id: id.clone(),
        channel: channel.to_string(),
        sender: sender.clone(),
        timestamp,
        content: content.to_string(),
        parent: parent.map(|s| s.to_string()),
        blob: blob.map(|s| s.to_string()),
        signature: signature_hex,
    };

    // Store locally
    let store = open_chat_store(config_flag)?;
    store.join_channel(channel)?; // Auto-join on send
    store.append(&msg)?;

    // Broadcast via daemon IPC if available, else direct gossip
    if let Ok(mut client) = bolo_core::ipc::DaemonClient::connect(&config_dir).await {
        let wire = ChatWireMessage { msg: msg.clone() };
        let wire_json = serde_json::to_string(&wire).context("failed to serialize wire message")?;
        let peer_values: Vec<serde_json::Value> = peers
            .iter()
            .map(|p| serde_json::Value::String(p.clone()))
            .collect();
        let _ = client
            .call(
                "chat.send",
                serde_json::json!({
                    "channel": channel,
                    "wire_message": wire_json,
                    "peers": peer_values,
                }),
            )
            .await;
    } else {
        // Direct gossip fallback
        let chat_key = load_chat_key(config_flag, channel)?;
        broadcast_direct(channel, &msg, peers, &config_dir, chat_key.as_ref()).await?;
    }

    if json {
        println!("{}", serde_json::to_string_pretty(&msg)?);
    } else {
        println!("[{sender_short}] {content}", sender_short = &sender[..8]);
    }
    Ok(())
}

/// `bolo chat history <channel>` — view message history.
pub fn history(channel: &str, limit: usize, config_flag: Option<&str>, json: bool) -> Result<()> {
    let store = open_chat_store(config_flag)?;
    let messages = store.history(channel, limit)?;

    if json {
        println!("{}", serde_json::to_string_pretty(&messages)?);
    } else if messages.is_empty() {
        println!("No messages in #{channel}.");
    } else {
        for msg in &messages {
            let time = format_timestamp(msg.timestamp);
            let sender_short = if msg.sender.len() >= 8 {
                &msg.sender[..8]
            } else {
                &msg.sender
            };
            if let Some(ref parent) = msg.parent {
                println!("  [{time}] {sender_short} (re: {parent}): {}", msg.content);
            } else {
                println!("  [{time}] {sender_short}: {}", msg.content);
            }
            if let Some(ref blob) = msg.blob {
                println!("           📎 {blob}");
            }
        }
        println!("\n{} message(s) in #{channel}", messages.len());
    }
    Ok(())
}

/// `bolo chat watch <channel>` — stream new messages in real-time.
pub async fn watch(
    channel: &str,
    peers: &[String],
    config_flag: Option<&str>,
    json: bool,
) -> Result<()> {
    let config_dir = resolve_config_dir(config_flag)?;
    let identity = bolo_core::Identity::load_from_config_dir(&config_dir)
        .context("failed to load identity — have you run `bolo daemon init`?")?;

    let store = open_chat_store(config_flag)?;
    store.join_channel(channel)?;

    let chat_key = load_chat_key(config_flag, channel)?;
    let secret_key = identity.secret_key().clone();
    let topic_id = chat_topic_id(channel);

    let endpoint = iroh::Endpoint::builder()
        .secret_key(secret_key)
        .alpns(vec![iroh_gossip::net::GOSSIP_ALPN.to_vec()])
        .bind()
        .await
        .context("failed to bind endpoint")?;

    // Enable mDNS
    let mdns = iroh::address_lookup::MdnsAddressLookup::builder()
        .build(endpoint.id())
        .map_err(|e| anyhow::anyhow!("failed to start mDNS: {e}"))?;
    endpoint.address_lookup().add(mdns);

    let gossip = bolo_pub::create_gossip(endpoint.clone());
    let _router = iroh::protocol::Router::builder(endpoint.clone())
        .accept(iroh_gossip::net::GOSSIP_ALPN, gossip.clone())
        .spawn();

    endpoint.online().await;

    if !json {
        println!("Watching #{channel}... (Ctrl-C to stop)\n");
    }

    let bootstrap: Vec<iroh::EndpointId> = peers.iter().filter_map(|p| p.parse().ok()).collect();

    let channel_handle = if bootstrap.is_empty() {
        bolo_pub::Channel::subscribe(&gossip, topic_id, bootstrap)
            .await
            .context("failed to subscribe to chat topic")?
    } else {
        bolo_pub::Channel::join(&gossip, topic_id, bootstrap)
            .await
            .context("failed to join chat topic")?
    };

    let (_sender, mut receiver) = channel_handle.split();

    use futures_lite::StreamExt;
    while let Some(event) = receiver.try_next().await.transpose() {
        match event {
            Ok(iroh_gossip::api::Event::Received(gossip_msg)) => {
                let content = maybe_open(&gossip_msg.content, chat_key.as_ref());

                // Extract message(s) from tagged or legacy format
                let mut incoming_msgs: Vec<ChatMessage> = Vec::new();
                if let Ok(gossip_message) = serde_json::from_slice::<ChatGossipMessage>(&content) {
                    match gossip_message {
                        ChatGossipMessage::Message { msg } => {
                            incoming_msgs.push(msg);
                        }
                        ChatGossipMessage::HistoryResponse { messages, .. } => {
                            incoming_msgs.extend(messages);
                        }
                        ChatGossipMessage::HistoryRequest { .. } => {
                            // Ignore requests in watch mode
                        }
                    }
                } else if let Ok(wire) = serde_json::from_slice::<ChatWireMessage>(&content) {
                    incoming_msgs.push(wire.msg);
                }

                for msg in incoming_msgs {
                    if !store.has_message(channel, &msg.id) {
                        store.append(&msg)?;
                    }

                    if json {
                        println!("{}", serde_json::to_string(&msg)?);
                    } else {
                        let time = format_timestamp(msg.timestamp);
                        let sender_short = if msg.sender.len() >= 8 {
                            &msg.sender[..8]
                        } else {
                            &msg.sender
                        };
                        println!("[{time}] {sender_short}: {}", msg.content);
                    }
                }
            }
            Ok(iroh_gossip::api::Event::NeighborUp(peer)) => {
                if !json {
                    println!("+ {} joined", peer.fmt_short());
                }
            }
            Ok(iroh_gossip::api::Event::NeighborDown(peer)) => {
                if !json {
                    println!("- {} left", peer.fmt_short());
                }
            }
            Ok(iroh_gossip::api::Event::Lagged) => {
                if !json {
                    eprintln!("Warning: receiver lagged");
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

/// `bolo chat sync [channel]` — request missed messages from peers.
pub async fn sync(channel: Option<&str>, config_flag: Option<&str>, json: bool) -> Result<()> {
    let config_dir = resolve_config_dir(config_flag)?;
    let mut client = bolo_core::ipc::DaemonClient::connect(&config_dir)
        .await
        .context("failed to connect to daemon — is it running?")?;

    let params = if let Some(ch) = channel {
        serde_json::json!({ "channel": ch })
    } else {
        serde_json::json!({})
    };

    let result = client.call("chat.sync", params).await?;

    if json {
        println!("{}", serde_json::to_string_pretty(&result)?);
    } else {
        let synced = result
            .get("synced_channels")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        let channels = result
            .get("channels")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            })
            .unwrap_or_default();
        println!("Sync requested for {synced} channel(s): {channels}");
        println!("History responses will arrive via gossip.");
    }
    Ok(())
}

/// Broadcast a message directly via an ephemeral gossip endpoint.
async fn broadcast_direct(
    channel: &str,
    msg: &ChatMessage,
    peers: &[String],
    config_dir: &std::path::Path,
    chat_key: Option<&ChannelKey>,
) -> Result<()> {
    let identity =
        bolo_core::Identity::load_from_config_dir(config_dir).context("failed to load identity")?;

    let endpoint = iroh::Endpoint::builder()
        .secret_key(identity.secret_key().clone())
        .alpns(vec![iroh_gossip::net::GOSSIP_ALPN.to_vec()])
        .bind()
        .await
        .context("failed to bind endpoint")?;

    let mdns = iroh::address_lookup::MdnsAddressLookup::builder()
        .build(endpoint.id())
        .map_err(|e| anyhow::anyhow!("failed to start mDNS: {e}"))?;
    endpoint.address_lookup().add(mdns);

    let gossip = bolo_pub::create_gossip(endpoint.clone());
    let _router = iroh::protocol::Router::builder(endpoint.clone())
        .accept(iroh_gossip::net::GOSSIP_ALPN, gossip.clone())
        .spawn();

    endpoint.online().await;

    let topic_id = chat_topic_id(channel);
    let bootstrap: Vec<iroh::EndpointId> = peers.iter().filter_map(|p| p.parse().ok()).collect();

    let gossip_msg = ChatGossipMessage::Message { msg: msg.clone() };
    let msg_bytes = serde_json::to_vec(&gossip_msg).context("serialize message")?;
    let payload = maybe_seal(&msg_bytes, chat_key).context("encryption failed")?;

    let topic_handle = if bootstrap.is_empty() {
        gossip
            .subscribe(topic_id, bootstrap)
            .await
            .context("failed to join chat topic")?
    } else {
        gossip
            .subscribe_and_join(topic_id, bootstrap)
            .await
            .context("failed to join chat topic")?
    };

    let (sender, _receiver) = topic_handle.split();
    sender
        .broadcast(bytes::Bytes::from(payload))
        .await
        .context("failed to broadcast")?;

    // Give gossip time to deliver before closing
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    endpoint.close().await;
    Ok(())
}

/// Compute the gossip topic ID for a chat channel.
fn chat_topic_id(channel: &str) -> iroh_gossip::TopicId {
    let topic = bolo_core::TopicId::from_name(&format!("chat/{channel}"));
    iroh_gossip::TopicId::from_bytes(topic.0)
}

/// Format a millisecond timestamp to a readable time string.
fn format_timestamp(ts_ms: u64) -> String {
    let secs = ts_ms / 1000;
    let hours = (secs / 3600) % 24;
    let mins = (secs / 60) % 60;
    let secs = secs % 60;
    format!("{hours:02}:{mins:02}:{secs:02}")
}

/// Hex encoding helper (no extra dependency needed).
mod hex {
    pub fn encode(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }
}
