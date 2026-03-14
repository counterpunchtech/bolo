//! Pub/sub command handlers.

use anyhow::{bail, Context, Result};
use bolo_core::TopicId;
use iroh::address_lookup::MdnsAddressLookup;
use iroh_gossip::api::Event;
use iroh_gossip::net::GOSSIP_ALPN;

use super::daemon::resolve_config_dir;

fn make_gossip_topic_id(topic: &str) -> iroh_gossip::TopicId {
    let topic_id = TopicId::from_name(topic);
    iroh_gossip::TopicId::from_bytes(topic_id.0)
}

fn parse_bootstrap_peers(peers: &[String]) -> Vec<iroh::EndpointId> {
    peers.iter().filter_map(|p| p.parse().ok()).collect()
}

/// Build an endpoint with mDNS discovery enabled.
async fn build_endpoint(secret_key: iroh::SecretKey) -> Result<iroh::Endpoint> {
    let endpoint = iroh::Endpoint::builder()
        .secret_key(secret_key)
        .alpns(vec![GOSSIP_ALPN.to_vec()])
        .bind()
        .await
        .context("failed to bind endpoint")?;

    // Enable mDNS for LAN auto-discovery
    let mdns = MdnsAddressLookup::builder()
        .build(endpoint.id())
        .map_err(|e| anyhow::anyhow!("failed to start mDNS: {e}"))?;
    endpoint.address_lookup().add(mdns);

    Ok(endpoint)
}

/// `bolo pub send <topic> <message>` — publish a message to a topic.
///
/// Tries IPC to daemon first. Falls back to ephemeral endpoint with mDNS.
pub async fn send(
    topic: &str,
    message: &str,
    peers: &[String],
    config_flag: Option<&str>,
    json: bool,
) -> Result<()> {
    let config_dir = resolve_config_dir(config_flag)?;

    // Try IPC to daemon first
    if let Ok(mut client) = bolo_core::ipc::DaemonClient::connect(&config_dir).await {
        let peer_values: Vec<serde_json::Value> = peers
            .iter()
            .map(|p| serde_json::Value::String(p.clone()))
            .collect();
        let result = client
            .call(
                "pub.send",
                serde_json::json!({
                    "topic": topic,
                    "message": message,
                    "peers": peer_values,
                }),
            )
            .await
            .context("IPC pub.send failed")?;

        if json {
            println!("{}", serde_json::to_string_pretty(&result)?);
        } else {
            println!("Sent {} bytes to topic: {topic}", message.len());
        }
        return Ok(());
    }

    // Fallback: ephemeral endpoint with mDNS
    let identity = bolo_core::Identity::load_from_config_dir(&config_dir)
        .context("failed to load identity — have you run `bolo daemon init`?")?;

    let endpoint = build_endpoint(identity.secret_key().clone()).await?;
    let gossip_topic_id = make_gossip_topic_id(topic);

    let gossip = bolo_pub::create_gossip(endpoint.clone());
    let _router = iroh::protocol::Router::builder(endpoint.clone())
        .accept(GOSSIP_ALPN, gossip.clone())
        .spawn();

    endpoint.online().await;

    let bootstrap = parse_bootstrap_peers(peers);
    let mut channel = bolo_pub::Channel::subscribe(&gossip, gossip_topic_id, bootstrap)
        .await
        .context("failed to subscribe to topic")?;

    channel
        .broadcast(message.as_bytes().to_vec())
        .await
        .context("failed to broadcast message")?;

    endpoint.close().await;

    if json {
        let out = serde_json::json!({
            "sent": true,
            "topic": topic,
            "size": message.len(),
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Sent {} bytes to topic: {topic}", message.len());
    }

    Ok(())
}

/// `bolo pub sub <topic>` — subscribe to a topic and stream messages.
///
/// Uses mDNS for LAN peer discovery. Pass `--peer <node_id>` for explicit bootstrap.
pub async fn sub(
    topic: &str,
    peers: &[String],
    config_flag: Option<&str>,
    json: bool,
) -> Result<()> {
    let config_dir = resolve_config_dir(config_flag)?;
    let identity = bolo_core::Identity::load_from_config_dir(&config_dir)
        .context("failed to load identity — have you run `bolo daemon init`?")?;

    let endpoint = build_endpoint(identity.secret_key().clone()).await?;
    let gossip_topic_id = make_gossip_topic_id(topic);

    let gossip = bolo_pub::create_gossip(endpoint.clone());
    let _router = iroh::protocol::Router::builder(endpoint.clone())
        .accept(GOSSIP_ALPN, gossip.clone())
        .spawn();

    endpoint.online().await;

    if !json {
        println!("Subscribed to topic: {topic}");
        println!("Waiting for messages... (Ctrl-C to stop)\n");
    }

    let bootstrap = parse_bootstrap_peers(peers);
    let channel = bolo_pub::Channel::subscribe(&gossip, gossip_topic_id, bootstrap)
        .await
        .context("failed to subscribe to topic")?;

    let (_sender, mut receiver) = channel.split();

    use futures_lite::StreamExt;
    while let Some(event) = receiver.try_next().await.transpose() {
        match event {
            Ok(Event::Received(msg)) => {
                let text = String::from_utf8_lossy(&msg.content);
                if json {
                    let out = serde_json::json!({
                        "from": msg.delivered_from.to_string(),
                        "message": text,
                        "size": msg.content.len(),
                    });
                    println!("{}", serde_json::to_string(&out)?);
                } else {
                    println!("[{}] {text}", msg.delivered_from.fmt_short());
                }
            }
            Ok(Event::NeighborUp(peer)) => {
                if !json {
                    println!("+ Peer joined: {}", peer.fmt_short());
                }
            }
            Ok(Event::NeighborDown(peer)) => {
                if !json {
                    println!("- Peer left: {}", peer.fmt_short());
                }
            }
            Ok(Event::Lagged) => {
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

/// `bolo pub ls` — list active topics (via daemon IPC).
pub async fn ls(config_flag: Option<&str>, json: bool) -> Result<()> {
    let config_dir = super::daemon::resolve_config_dir(config_flag)?;
    let mut client = bolo_core::ipc::DaemonClient::connect(&config_dir)
        .await
        .context(
            "Cannot list subscriptions: daemon is not running.\n\
             Start the daemon with `bolo daemon start`, then retry.",
        )?;

    let result = client
        .call("pub.topics", serde_json::json!({}))
        .await
        .context("IPC call failed")?;

    if json {
        println!("{}", serde_json::to_string_pretty(&result)?);
    } else {
        let topics = result["topics"].as_array();
        if topics.is_none_or(|t| t.is_empty()) {
            println!("No active topics.");
        } else {
            for t in topics.unwrap() {
                println!("  {}", t.as_str().unwrap_or(""));
            }
        }
    }

    Ok(())
}

/// `bolo pub peers <topic>` — list peers on a topic (requires daemon).
pub fn peers(topic: &str, _config_flag: Option<&str>, _json: bool) -> Result<()> {
    let _ = topic;
    bail!(
        "Cannot list topic peers: requires a running daemon with active gossip.\n\
         Start the daemon with `bolo daemon start`, then retry."
    );
}

/// `bolo pub unsub <topic>` — unsubscribe from a topic (requires daemon).
pub fn unsub(topic: &str, _config_flag: Option<&str>, _json: bool) -> Result<()> {
    let _ = topic;
    bail!(
        "Cannot unsubscribe: requires a running daemon with active gossip.\n\
         Start the daemon with `bolo daemon start`, then retry."
    );
}
