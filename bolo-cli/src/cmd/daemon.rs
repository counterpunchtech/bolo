//! Daemon command handlers.

use std::path::PathBuf;

use anyhow::{bail, Context, Result};
use bolo_ci::{ci_topic_id, CiMessage, CiStore};
use bolo_core::crypto::{derive_gossip_key, maybe_open, maybe_seal, ChannelKey};
use bolo_core::{BoloConfig, BoloNode, DaemonState, Identity, Timestamp};
use bolo_docs::{
    apply_sync_message, doc_discovery_topic_id, doc_topic_id, DocStore, DocSyncMessage,
};

const VERSION: &str = env!("BOLO_BUILD_VERSION");

fn print_splash() {
    println!(
        r#"
  ██████╗  ██████╗ ██╗      ██████╗
  ██╔══██╗██╔═══██╗██║     ██╔═══██╗
  ██████╔╝██║   ██║██║     ██║   ██║
  ██╔══██╗██║   ██║██║     ██║   ██║
  ██████╔╝╚██████╔╝███████╗╚██████╔╝
  ╚═════╝  ╚═════╝ ╚══════╝ ╚═════╝  v{}
  P2P mesh network
"#,
        VERSION
    );
}

/// Resolve the config directory from the --config flag or default.
pub fn resolve_config_dir(config_flag: Option<&str>) -> Result<PathBuf> {
    match config_flag {
        Some(p) => {
            let path = PathBuf::from(p);
            if path.extension().is_some() {
                Ok(path.parent().context("invalid config path")?.to_path_buf())
            } else {
                Ok(path)
            }
        }
        None => Ok(BoloConfig::resolve_config_dir()?),
    }
}

/// Resolve the data directory (config_dir/data).
pub fn resolve_data_dir(config_dir: &std::path::Path) -> PathBuf {
    config_dir.join("data")
}

/// `bolo daemon init` — create keypair and config directory.
pub fn init(config_flag: Option<&str>, json: bool) -> Result<()> {
    let config_dir = resolve_config_dir(config_flag)?;
    std::fs::create_dir_all(&config_dir)?;

    let config_path = config_dir.join("config.toml");
    let config = if config_path.exists() {
        BoloConfig::load(Some(&config_path))?
    } else {
        BoloConfig::default()
    };

    let key_path = config_dir.join(&config.identity.key_file);
    if key_path.exists() {
        bail!(
            "Identity already exists at {}. Remove it first to reinitialize.",
            key_path.display()
        );
    }

    let identity = Identity::generate();
    identity.save(&key_path)?;

    if !config_path.exists() {
        config.save(Some(&config_path))?;
    }

    let node_id = identity.node_id().to_string();

    if json {
        let out = serde_json::json!({
            "node_id": node_id,
            "config_dir": config_dir.to_string_lossy(),
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Initialized bolo node");
        println!("  Node ID: {node_id}");
        println!("  Config:  {}", config_dir.display());
    }

    Ok(())
}

/// `bolo daemon install` — install as a system service (launchd on macOS, systemd on Linux).
///
/// This ensures the daemon auto-starts on boot and auto-restarts after deploys.
pub fn install(config_flag: Option<&str>, json: bool) -> Result<()> {
    let exe = std::env::current_exe().context("failed to resolve bolo binary path")?;
    let exe_path = exe.to_string_lossy();

    #[cfg(target_os = "macos")]
    {
        let home = std::env::var("HOME").context("HOME not set")?;
        let plist_dir = PathBuf::from(&home).join("Library/LaunchAgents");
        std::fs::create_dir_all(&plist_dir)?;
        let plist_path = plist_dir.join("com.bolo.daemon.plist");

        let mut args = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.bolo.daemon</string>
    <key>ProgramArguments</key>
    <array>
        <string>{exe_path}</string>
        <string>daemon</string>
        <string>start</string>"#
        );
        if let Some(cfg) = config_flag {
            args.push_str(&format!(
                r#"
        <string>--config</string>
        <string>{cfg}</string>"#
            ));
        }
        args.push_str(
            r#"
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <dict>
        <key>SuccessfulExit</key>
        <false/>
    </dict>
    <key>ThrottleInterval</key>
    <integer>5</integer>
    <key>StandardOutPath</key>
    <string>/tmp/bolo-daemon.stdout.log</string>
    <key>StandardErrorPath</key>
    <string>/tmp/bolo-daemon.stderr.log</string>
    <key>EnvironmentVariables</key>
    <dict>
        <key>BOLO_LOG</key>
        <string>info</string>
    </dict>
</dict>
</plist>"#,
        );

        std::fs::write(&plist_path, &args).context("failed to write launchd plist")?;

        // Load the service
        let status = std::process::Command::new("launchctl")
            .args(["load", "-w"])
            .arg(&plist_path)
            .status()
            .context("failed to run launchctl load")?;

        if json {
            let out = serde_json::json!({
                "installed": true,
                "service": "com.bolo.daemon",
                "plist": plist_path.to_string_lossy(),
                "binary": exe_path,
                "loaded": status.success(),
            });
            println!("{}", serde_json::to_string_pretty(&out)?);
        } else {
            println!("Installed launchd service: com.bolo.daemon");
            println!("  Plist:  {}", plist_path.display());
            println!("  Binary: {exe_path}");
            if status.success() {
                println!("  Status: loaded (will start on boot and restart on exit)");
            } else {
                println!("  Status: written but launchctl load failed — try manually");
            }
        }
    }

    #[cfg(target_os = "linux")]
    {
        let home = std::env::var("HOME").context("HOME not set")?;
        let unit_dir = PathBuf::from(&home).join(".config/systemd/user");
        std::fs::create_dir_all(&unit_dir)?;
        let unit_path = unit_dir.join("bolo-daemon.service");

        let mut exec_start = format!("{exe_path} daemon start");
        if let Some(cfg) = config_flag {
            exec_start.push_str(&format!(" --config {cfg}"));
        }

        let unit = format!(
            r#"[Unit]
Description=Bolo P2P mesh daemon
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart={exec_start}
Restart=on-failure
RestartSec=5
Environment=BOLO_LOG=info

[Install]
WantedBy=default.target
"#
        );

        std::fs::write(&unit_path, &unit).context("failed to write systemd unit")?;

        // Enable and start
        let reload = std::process::Command::new("systemctl")
            .args(["--user", "daemon-reload"])
            .status();
        let enable = std::process::Command::new("systemctl")
            .args(["--user", "enable", "bolo-daemon.service"])
            .status();
        let start = std::process::Command::new("systemctl")
            .args(["--user", "start", "bolo-daemon.service"])
            .status();

        if json {
            let out = serde_json::json!({
                "installed": true,
                "service": "bolo-daemon.service",
                "unit": unit_path.to_string_lossy(),
                "binary": exe_path,
                "enabled": enable.map(|s| s.success()).unwrap_or(false),
                "started": start.map(|s| s.success()).unwrap_or(false),
            });
            println!("{}", serde_json::to_string_pretty(&out)?);
        } else {
            println!("Installed systemd user service: bolo-daemon.service");
            println!("  Unit:   {}", unit_path.display());
            println!("  Binary: {exe_path}");
            let _ = reload;
            if enable.map(|s| s.success()).unwrap_or(false) {
                println!("  Status: enabled (will start on login and restart on failure)");
            } else {
                println!("  Status: written but systemctl enable failed — try manually");
            }
        }
    }

    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    bail!("daemon install is only supported on macOS (launchd) and Linux (systemd)");

    Ok(())
}

/// `bolo daemon uninstall` — remove the system service.
pub fn uninstall(config_flag: Option<&str>, json: bool) -> Result<()> {
    let _ = config_flag;

    #[cfg(target_os = "macos")]
    {
        let home = std::env::var("HOME").context("HOME not set")?;
        let plist_path = PathBuf::from(&home).join("Library/LaunchAgents/com.bolo.daemon.plist");

        if plist_path.exists() {
            // Unload first
            let _ = std::process::Command::new("launchctl")
                .args(["unload"])
                .arg(&plist_path)
                .status();
            std::fs::remove_file(&plist_path).context("failed to remove plist")?;
        }

        if json {
            let out = serde_json::json!({ "uninstalled": true, "service": "com.bolo.daemon" });
            println!("{}", serde_json::to_string_pretty(&out)?);
        } else {
            println!("Uninstalled launchd service: com.bolo.daemon");
        }
    }

    #[cfg(target_os = "linux")]
    {
        let home = std::env::var("HOME").context("HOME not set")?;
        let unit_path = PathBuf::from(&home).join(".config/systemd/user/bolo-daemon.service");

        let _ = std::process::Command::new("systemctl")
            .args(["--user", "stop", "bolo-daemon.service"])
            .status();
        let _ = std::process::Command::new("systemctl")
            .args(["--user", "disable", "bolo-daemon.service"])
            .status();

        if unit_path.exists() {
            std::fs::remove_file(&unit_path).context("failed to remove unit file")?;
        }

        let _ = std::process::Command::new("systemctl")
            .args(["--user", "daemon-reload"])
            .status();

        if json {
            let out = serde_json::json!({ "uninstalled": true, "service": "bolo-daemon.service" });
            println!("{}", serde_json::to_string_pretty(&out)?);
        } else {
            println!("Uninstalled systemd user service: bolo-daemon.service");
        }
    }

    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    bail!("daemon uninstall is only supported on macOS (launchd) and Linux (systemd)");

    Ok(())
}

/// `bolo daemon start` — start the node.
///
/// With `--detach`, re-execs itself as a background process and returns immediately.
/// Without `--detach`, runs in the foreground and blocks until Ctrl-C/SIGTERM.
pub async fn start(detach: bool, config_flag: Option<&str>, json: bool) -> Result<()> {
    if detach {
        return start_detached(config_flag, json);
    }

    start_foreground(config_flag, json).await
}

/// Spawn a detached background daemon process using the current executable.
fn start_detached(config_flag: Option<&str>, json: bool) -> Result<()> {
    let exe = std::env::current_exe().context("failed to resolve bolo binary path")?;

    let mut args = vec!["daemon".to_string(), "start".to_string()];
    if json {
        args.insert(0, "--json".to_string());
    }
    if let Some(cfg) = config_flag {
        args.insert(0, cfg.to_string());
        args.insert(0, "--config".to_string());
    }

    let child = std::process::Command::new(&exe)
        .args(&args)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .context("failed to spawn detached daemon")?;

    let pid = child.id();

    // Wait briefly for the daemon to write its state file so we can confirm it started
    let config_dir = resolve_config_dir(config_flag)?;
    let mut started = false;
    for _ in 0..30 {
        std::thread::sleep(std::time::Duration::from_millis(200));
        if let Ok(state) = DaemonState::load(&config_dir) {
            if state.is_alive() {
                if json {
                    let out = serde_json::json!({
                        "status": "running",
                        "node_id": state.node_id,
                        "pid": state.pid,
                        "detached": true,
                        "data_dir": state.data_dir,
                    });
                    println!("{}", serde_json::to_string_pretty(&out)?);
                } else {
                    print_splash();
                    println!("  Node ID: {}", state.node_id);
                    println!("  PID:     {}", state.pid);
                    println!("  Data:    {}", state.data_dir);
                    println!("  Mode:    detached");
                }
                started = true;
                break;
            }
        }
    }

    if !started {
        bail!(
            "Daemon process spawned (PID {pid}) but did not become ready within 6 seconds. \
             Check logs with BOLO_LOG=debug."
        );
    }

    Ok(())
}

/// Run the daemon in the foreground (blocks until signal).
async fn start_foreground(config_flag: Option<&str>, json: bool) -> Result<()> {
    let config_dir = resolve_config_dir(config_flag)?;
    let data_dir = resolve_data_dir(&config_dir);
    std::fs::create_dir_all(&data_dir)?;

    // Check if already running
    if let Ok(state) = DaemonState::load(&config_dir) {
        if state.is_alive() {
            bail!("Daemon is already running (PID {})", state.pid);
        }
        // Stale state file — clean it up
        DaemonState::remove(&config_dir)?;
    }

    // Clean up stale socket file
    let sock_path = bolo_core::ipc::socket_path(&config_dir);
    if sock_path.exists() {
        std::fs::remove_file(&sock_path)?;
    }

    // Load config and identity
    let config = BoloConfig::load(Some(&config_dir.join("config.toml")))?;
    let mesh_secret = config
        .crypto
        .mesh_secret_bytes()
        .context("invalid mesh_secret in config")?;
    if mesh_secret.is_some() && !json {
        println!("Gossip encryption: enabled (mesh_secret configured)");
    }

    let identity = Identity::load_from_config_dir(&config_dir)
        .context("failed to load identity — have you run `bolo daemon init`?")?;
    let node_id = identity.node_id().to_string();
    let secret_key = identity.secret_key().clone();

    // Open blob store
    let store = bolo_blobs::store::open_store(&data_dir).await?;
    let store = std::sync::Arc::new(store);

    // Create blobs protocol handler
    let blobs = bolo_blobs::BlobsProtocol::new(&store, None);

    // Spawn node (includes gossip protocol)
    let node = BoloNode::spawn(secret_key, blobs).await?;

    // Wait for relay connectivity
    node.endpoint().online().await;
    let addr = node.endpoint().addr();

    // Open doc store and subscribe to all existing documents
    let doc_store = std::sync::Arc::new(
        bolo_docs::DocStore::open(&data_dir).context("failed to open document store")?,
    );
    let doc_count = spawn_doc_sync_loop(
        node.gossip().clone(),
        doc_store.clone(),
        node_id.clone(),
        mesh_secret,
    )
    .await?;

    // Open chat store
    let chat_store = std::sync::Arc::new(
        bolo_chat::ChatStore::open(&data_dir).context("failed to open chat store")?,
    );

    // Start persistent chat sync loop — listens on all joined channels
    let chat_channels_count = chat_store.list_channels().unwrap_or_default().len();
    spawn_chat_sync_loop(
        node.gossip().clone(),
        chat_store.clone(),
        node_id.clone(),
        mesh_secret,
    )
    .await?;

    // Spawn the doc discovery listener — peers announce new doc paths on this topic,
    // and we auto-subscribe to each new doc's gossip topic.
    spawn_doc_discovery_listener(
        node.gossip().clone(),
        doc_store.clone(),
        node_id.clone(),
        mesh_secret,
    )
    .await?;

    // Open CI store and start CI sync loop
    let ci_store =
        std::sync::Arc::new(CiStore::open(&data_dir).context("failed to open CI store")?);
    spawn_ci_sync_loop(
        node.gossip().clone(),
        ci_store.clone(),
        node_id.clone(),
        data_dir.clone(),
        store.clone(),
        doc_store.clone(),
        mesh_secret,
    )
    .await?;

    // Shared peer list — used by IPC and deploy listener
    let known_peers = std::sync::Arc::new(tokio::sync::Mutex::new(Vec::<iroh::PublicKey>::new()));

    // Start deploy listener
    spawn_deploy_listener(
        node.gossip().clone(),
        store.clone(),
        node.endpoint().clone(),
        node_id.clone(),
        config_dir.clone(),
        data_dir.clone(),
        mesh_secret,
        known_peers.clone(),
    )
    .await?;

    // Start logs listener (responds to incoming log requests from peers)
    spawn_logs_listener(
        node.gossip().clone(),
        node_id.clone(),
        mesh_secret,
        known_peers.clone(),
        config_dir.clone(),
    )
    .await?;

    // Shared state for mesh-status gossip
    let mesh_status_sender = std::sync::Arc::new(tokio::sync::Mutex::new(None));
    let (mesh_response_tx, _) = tokio::sync::broadcast::channel(16);

    // Start mesh status listener (responds to incoming capability requests from peers
    // and provides shared sender for IPC handler to broadcast through)
    spawn_mesh_status_listener(
        node.gossip().clone(),
        node_id.clone(),
        mesh_secret,
        known_peers.clone(),
        mesh_status_sender.clone(),
        mesh_response_tx.clone(),
    )
    .await?;

    // Start bench listener
    spawn_bench_listener(
        node.gossip().clone(),
        store.clone(),
        node.endpoint().clone(),
        node_id.clone(),
        mesh_secret,
        known_peers.clone(),
    )
    .await?;

    // Run GC on startup if configured
    if config.storage.gc.auto {
        match crate::gc::run_gc(&data_dir, &config.storage) {
            Ok(report) => {
                let total =
                    report.chat_messages_pruned + report.ci_tasks_pruned + report.docs_evicted;
                if total > 0 && !json {
                    println!(
                        "  GC:      pruned {} chat, {} CI tasks, {} docs",
                        report.chat_messages_pruned, report.ci_tasks_pruned, report.docs_evicted
                    );
                }
            }
            Err(e) => {
                tracing::warn!("GC on startup failed: {e}");
            }
        }
    }

    // Schedule periodic GC
    {
        let gc_data_dir = data_dir.clone();
        let gc_config = config.storage.clone();
        let interval_hours = config.storage.gc.interval_hours;
        if config.storage.gc.auto && interval_hours > 0 {
            tokio::spawn(async move {
                let mut interval =
                    tokio::time::interval(std::time::Duration::from_secs(interval_hours * 3600));
                interval.tick().await; // skip the immediate first tick
                loop {
                    interval.tick().await;
                    match crate::gc::run_gc(&gc_data_dir, &gc_config) {
                        Ok(report) => {
                            let total = report.chat_messages_pruned
                                + report.ci_tasks_pruned
                                + report.docs_evicted;
                            if total > 0 {
                                tracing::info!(
                                    chat = report.chat_messages_pruned,
                                    ci = report.ci_tasks_pruned,
                                    docs = report.docs_evicted,
                                    "periodic GC completed"
                                );
                            }
                        }
                        Err(e) => {
                            tracing::warn!("periodic GC failed: {e}");
                        }
                    }
                }
            });
        }
    }

    // Start IPC server
    let ipc_ctx = IpcContext {
        store: store.clone(),
        endpoint: node.endpoint().clone(),
        gossip: node.gossip().clone(),
        doc_store: doc_store.clone(),
        chat_store: chat_store.clone(),
        ci_store: ci_store.clone(),
        node_id: node_id.clone(),
        known_peers,
        mesh_secret,
        config_dir: config_dir.clone(),
        mesh_status_sender: mesh_status_sender.clone(),
        mesh_response_tx: mesh_response_tx.clone(),
        doc_senders: std::sync::Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
    };
    spawn_ipc_server(&config_dir, ipc_ctx.clone())?;

    // Auto-connect to persisted peers from trust list
    let trust_list = bolo_core::TrustList::load(&config_dir).unwrap_or_default();
    let mut reconnected = 0usize;
    for peer_id_str in &trust_list.trusted {
        if let Ok(remote) = peer_id_str.parse::<iroh::PublicKey>() {
            // Same logic as ipc_peer_add: subscribe to all gossip topics
            let ci_topic = ci_topic_id();
            let _ = ipc_ctx.gossip.subscribe(ci_topic, vec![remote]).await;
            let discovery_topic = doc_discovery_topic_id();
            let _ = ipc_ctx
                .gossip
                .subscribe(discovery_topic, vec![remote])
                .await;
            let deploy_topic =
                iroh_gossip::TopicId::from_bytes(bolo_core::TopicId::from_name("bolo/deploy").0);
            let _ = ipc_ctx.gossip.subscribe(deploy_topic, vec![remote]).await;
            let _ = ipc_ctx
                .gossip
                .subscribe(bench_topic_id(), vec![remote])
                .await;
            let _ = ipc_ctx
                .gossip
                .subscribe(mesh_status_topic_id(), vec![remote])
                .await;
            let _ = ipc_ctx
                .gossip
                .subscribe(logs_topic_id(), vec![remote])
                .await;
            // Join all doc topics with this peer
            if let Ok(doc_names) = ipc_ctx.doc_store.list() {
                for name in &doc_names {
                    let topic_id = doc_topic_id(name);
                    let _ = ipc_ctx.gossip.subscribe(topic_id, vec![remote]).await;
                }
            }
            // Join all chat channel topics with this peer
            if let Ok(channels) = ipc_ctx.chat_store.list_channels() {
                for ch in &channels {
                    let topic_id = chat_gossip_topic_id(ch);
                    let _ = ipc_ctx.gossip.subscribe(topic_id, vec![remote]).await;
                }
            }
            // Add to known_peers
            {
                let mut peers = ipc_ctx.known_peers.lock().await;
                if !peers.contains(&remote) {
                    peers.push(remote);
                }
            }
            // Log connection type for each reconnected peer
            let conn_type = describe_connection(&ipc_ctx.endpoint, remote).await;
            tracing::info!(peer = %peer_id_str, connection = %conn_type, "peer reconnected");
            reconnected += 1;
        }
    }
    if reconnected > 0 && !json {
        println!("  Peers:   {reconnected} reconnecting");
    }

    // Write state file
    let state = DaemonState {
        pid: std::process::id(),
        node_id: node_id.clone(),
        start_time_ms: Timestamp::now().0,
        data_dir: data_dir.to_string_lossy().to_string(),
    };
    state.save(&config_dir)?;

    if json {
        let out = serde_json::json!({
            "status": "running",
            "node_id": node_id,
            "pid": state.pid,
            "address": format!("{addr:?}"),
            "docs_syncing": doc_count,
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        print_splash();
        println!("  Node ID: {node_id}");
        println!("  PID:     {}", state.pid);
        println!("  Address: {addr:?}");
        println!("  Data:    {}", data_dir.display());
        if doc_count > 0 {
            println!("  Docs:    {doc_count} syncing");
        }
        if chat_channels_count > 0 {
            println!("  Chat:    {chat_channels_count} channels syncing");
        }
        println!("\nPress Ctrl-C to stop.");
    }

    // Wait for shutdown signal
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut sigterm = signal(SignalKind::terminate())?;
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {},
            _ = sigterm.recv() => {},
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await?;
    }

    if !json {
        println!("\nShutting down...");
    }

    // Clean shutdown
    std::fs::remove_file(&sock_path).ok();
    node.shutdown().await?;
    DaemonState::remove(&config_dir)?;

    if !json {
        println!("Daemon stopped.");
    }

    Ok(())
}

// --- IPC server ---

/// Shared context for handling IPC requests.
#[derive(Clone)]
struct IpcContext {
    store: std::sync::Arc<bolo_blobs::FsStore>,
    endpoint: iroh::Endpoint,
    gossip: iroh_gossip::Gossip,
    doc_store: std::sync::Arc<DocStore>,
    chat_store: std::sync::Arc<bolo_chat::ChatStore>,
    ci_store: std::sync::Arc<CiStore>,
    node_id: String,
    /// Peers added via `peer.add` — used as bootstrap nodes for gossip topic subscriptions.
    known_peers: std::sync::Arc<tokio::sync::Mutex<Vec<iroh::PublicKey>>>,
    /// Mesh-wide shared secret for gossip encryption (None = plaintext).
    mesh_secret: Option<[u8; 32]>,
    /// Config directory — used for persisting peer list.
    config_dir: std::path::PathBuf,
    /// Shared sender for the persistent mesh-status gossip subscription (used by background listener).
    #[allow(dead_code)]
    mesh_status_sender: std::sync::Arc<tokio::sync::Mutex<Option<iroh_gossip::api::GossipSender>>>,
    /// Broadcast channel for mesh-status capability responses from peers.
    mesh_response_tx: tokio::sync::broadcast::Sender<bolo_core::capabilities::NodeCapabilities>,
    /// Shared doc topic senders — registered by `spawn_doc_sync_for_topic`, used by `broadcast_doc_to_gossip`.
    doc_senders: std::sync::Arc<
        tokio::sync::Mutex<
            std::collections::HashMap<String, std::sync::Arc<iroh_gossip::api::GossipSender>>,
        >,
    >,
}

impl IpcContext {
    /// Derive a per-topic gossip encryption key. Returns None if no mesh secret configured.
    fn gossip_key(&self, topic_context: &str) -> Option<ChannelKey> {
        self.mesh_secret
            .as_ref()
            .map(|s| derive_gossip_key(s, topic_context))
    }
}

/// Bind the Unix socket and spawn the accept loop.
fn spawn_ipc_server(config_dir: &std::path::Path, ctx: IpcContext) -> Result<()> {
    let sock_path = bolo_core::ipc::socket_path(config_dir);
    let listener =
        tokio::net::UnixListener::bind(&sock_path).context("failed to bind IPC socket")?;

    tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((stream, _)) => {
                    let ctx = ctx.clone();
                    tokio::spawn(handle_ipc_connection(stream, ctx));
                }
                Err(e) => {
                    tracing::warn!(error = %e, "IPC accept error");
                }
            }
        }
    });

    Ok(())
}

/// Handle a single IPC connection: read one request line, dispatch, write one response line.
async fn handle_ipc_connection(stream: tokio::net::UnixStream, ctx: IpcContext) {
    use bolo_core::ipc::{IpcRequest, IpcResponse};
    use tokio::io::{AsyncBufReadExt, BufReader};

    let mut reader = BufReader::new(stream);
    let mut line = String::new();

    if reader.read_line(&mut line).await.is_err() {
        return;
    }

    let req: IpcRequest = match serde_json::from_str(line.trim()) {
        Ok(r) => r,
        Err(e) => {
            let resp = IpcResponse::error(0, -32700, format!("parse error: {e}"));
            let _ = write_response(&mut reader, &resp).await;
            return;
        }
    };

    let resp = dispatch_ipc(&ctx, &req).await;
    let _ = write_response(&mut reader, &resp).await;
}

async fn write_response(
    reader: &mut tokio::io::BufReader<tokio::net::UnixStream>,
    resp: &bolo_core::ipc::IpcResponse,
) -> std::io::Result<()> {
    use tokio::io::AsyncWriteExt;
    let mut json = serde_json::to_string(resp).unwrap_or_default();
    json.push('\n');
    reader.get_mut().write_all(json.as_bytes()).await?;
    reader.get_mut().flush().await
}

/// Route an IPC method to the appropriate handler.
async fn dispatch_ipc(
    ctx: &IpcContext,
    req: &bolo_core::ipc::IpcRequest,
) -> bolo_core::ipc::IpcResponse {
    use bolo_core::ipc::IpcResponse;

    let result = match req.method.as_str() {
        "blob.list" => ipc_blob_list(ctx).await,
        "blob.put" => ipc_blob_put(ctx, &req.params).await,
        "blob.get" => ipc_blob_get(ctx, &req.params).await,
        "blob.stat" => ipc_blob_stat(ctx, &req.params).await,
        "blob.pin" => ipc_blob_pin(ctx, &req.params).await,
        "blob.unpin" => ipc_blob_unpin(ctx, &req.params).await,
        "blob.gc" => ipc_blob_gc(ctx).await,
        "pub.topics" => ipc_pub_topics(ctx).await,
        "pub.send" => ipc_pub_send(ctx, &req.params).await,
        "peer.add" => ipc_peer_add(ctx, &req.params).await,
        "peer.list" => ipc_peer_list(ctx).await,
        "peer.bench" => ipc_peer_bench(ctx, &req.params).await,
        "doc.create" => ipc_doc_create(ctx, &req.params).await,
        "doc.set" => ipc_doc_set(ctx, &req.params).await,
        "doc.set_many" => ipc_doc_set_many(ctx, &req.params).await,
        "doc.get" => ipc_doc_get(ctx, &req.params).await,
        "doc.del" => ipc_doc_del(ctx, &req.params).await,
        "doc.list" => ipc_doc_list(ctx, &req.params).await,
        "doc.read" => ipc_doc_read(ctx, &req.params).await,
        "doc.append" => ipc_doc_append(ctx, &req.params).await,
        "doc.sync" => ipc_doc_sync(ctx, &req.params).await,
        "ci.run" => ipc_ci_run(ctx, &req.params).await,
        "ci.status" => ipc_ci_status(ctx).await,
        "ci.results" => ipc_ci_results(ctx, &req.params).await,
        "chat.join" => ipc_chat_join(ctx, &req.params).await,
        "chat.send" => ipc_chat_send(ctx, &req.params).await,
        "chat.history" => ipc_chat_history(ctx, &req.params).await,
        "chat.channels" => ipc_chat_channels(ctx).await,
        "chat.sync" => ipc_chat_sync(ctx, &req.params).await,
        "task.create" => ipc_task_create(ctx, &req.params).await,
        "task.list" => ipc_task_list(ctx).await,
        "task.show" => ipc_task_show(ctx, &req.params).await,
        "task.update" => ipc_task_update(ctx, &req.params).await,
        "task.delete" => ipc_task_delete(ctx, &req.params).await,
        "task.claim" => ipc_task_claim(ctx, &req.params).await,
        "task.release" => ipc_task_release(ctx, &req.params).await,
        "review.comment" => ipc_review_comment(ctx, &req.params).await,
        "review.approve" => ipc_review_approve(ctx, &req.params).await,
        "review.reject" => ipc_review_reject(ctx, &req.params).await,
        "review.show" => ipc_review_show(ctx, &req.params).await,
        "review.ls" => ipc_review_ls(ctx).await,
        "deploy.push" => ipc_deploy_push(ctx, &req.params).await,
        "mesh.status" => ipc_mesh_status(ctx, &req.params).await,
        "daemon.logs" => ipc_daemon_logs(ctx, &req.params).await,
        "peer.logs" => ipc_peer_logs(ctx, &req.params).await,
        _ => Err(format!("unknown method: {}", req.method)),
    };

    match result {
        Ok(val) => IpcResponse::success(req.id, val),
        Err(msg) => IpcResponse::error(req.id, -1, msg),
    }
}

async fn ipc_blob_list(ctx: &IpcContext) -> Result<serde_json::Value, String> {
    use futures_lite::StreamExt;
    let mut entries = Vec::new();
    let mut stream = ctx.store.tags().list().await.map_err(|e| e.to_string())?;
    while let Some(item) = stream.next().await {
        let item = item.map_err(|e| e.to_string())?;
        entries.push(serde_json::json!({
            "tag": item.name.to_string(),
            "hash": item.hash_and_format().hash.to_string(),
            "format": format!("{:?}", item.hash_and_format().format),
        }));
    }
    Ok(serde_json::json!(entries))
}

async fn ipc_blob_put(
    ctx: &IpcContext,
    params: &serde_json::Value,
) -> Result<serde_json::Value, String> {
    let file = params
        .get("file")
        .and_then(|v| v.as_str())
        .ok_or("missing 'file' param")?;
    let path = std::path::Path::new(file);
    if !path.exists() {
        return Err(format!("file not found: {file}"));
    }
    let tag = ctx
        .store
        .add_path(path)
        .with_tag()
        .await
        .map_err(|e| format!("failed to add blob: {e}"))?;
    let hash = tag.hash.to_string();
    let size = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
    Ok(serde_json::json!({ "hash": hash, "size": size, "path": file }))
}

async fn ipc_blob_get(
    ctx: &IpcContext,
    params: &serde_json::Value,
) -> Result<serde_json::Value, String> {
    let hash_str = params
        .get("hash")
        .and_then(|v| v.as_str())
        .ok_or("missing 'hash' param")?;
    let blob_hash: iroh_blobs::Hash = hash_str.parse().map_err(|_| "invalid blob hash")?;

    // Try local first
    let bytes = match ctx.store.get_bytes(blob_hash).await {
        Ok(b) => b,
        Err(_) => {
            // Not local — try fetching from known peers
            let peers = ctx.known_peers.lock().await.clone();
            if peers.is_empty() {
                return Err(format!(
                    "blob not found locally and no peers to fetch from: {hash_str}"
                ));
            }
            let downloader = ctx.store.downloader(&ctx.endpoint);
            downloader
                .download(blob_hash, peers)
                .await
                .map_err(|e| format!("failed to fetch blob from peers: {e}"))?;
            ctx.store
                .get_bytes(blob_hash)
                .await
                .map_err(|e| format!("blob fetch succeeded but read failed: {e}"))?
        }
    };

    if let Some(out_path) = params.get("path").and_then(|v| v.as_str()) {
        std::fs::write(out_path, &bytes).map_err(|e| e.to_string())?;
        Ok(serde_json::json!({ "hash": hash_str, "size": bytes.len(), "path": out_path }))
    } else {
        Ok(serde_json::json!({
            "hash": hash_str,
            "size": bytes.len(),
            "data": String::from_utf8_lossy(&bytes),
        }))
    }
}

async fn ipc_blob_stat(
    ctx: &IpcContext,
    params: &serde_json::Value,
) -> Result<serde_json::Value, String> {
    let hash_str = params
        .get("hash")
        .and_then(|v| v.as_str())
        .ok_or("missing 'hash' param")?;
    let blob_hash: iroh_blobs::Hash = hash_str.parse().map_err(|_| "invalid blob hash")?;
    let exists = ctx.store.has(blob_hash).await.unwrap_or(false);
    Ok(serde_json::json!({ "hash": hash_str, "exists": exists }))
}

async fn ipc_blob_pin(
    ctx: &IpcContext,
    params: &serde_json::Value,
) -> Result<serde_json::Value, String> {
    let hash_str = params
        .get("hash")
        .and_then(|v| v.as_str())
        .ok_or("missing 'hash' param")?;
    let blob_hash: iroh_blobs::Hash = hash_str.parse().map_err(|_| "invalid blob hash")?;
    let tag_name = format!("pin-{hash_str}");
    ctx.store
        .tags()
        .set(
            iroh_blobs::api::Tag::from(tag_name.clone()),
            iroh_blobs::HashAndFormat::raw(blob_hash),
        )
        .await
        .map_err(|e| e.to_string())?;
    Ok(serde_json::json!({ "pinned": true, "hash": hash_str, "tag": tag_name }))
}

async fn ipc_blob_unpin(
    ctx: &IpcContext,
    params: &serde_json::Value,
) -> Result<serde_json::Value, String> {
    let hash_str = params
        .get("hash")
        .and_then(|v| v.as_str())
        .ok_or("missing 'hash' param")?;
    let tag_name = format!("pin-{hash_str}");
    ctx.store
        .tags()
        .delete(iroh_blobs::api::Tag::from(tag_name))
        .await
        .map_err(|e| e.to_string())?;
    Ok(serde_json::json!({ "unpinned": true, "hash": hash_str }))
}

async fn ipc_blob_gc(ctx: &IpcContext) -> Result<serde_json::Value, String> {
    let config =
        BoloConfig::load(Some(&ctx.config_dir.join("config.toml"))).map_err(|e| e.to_string())?;
    let data_dir = resolve_data_dir(&ctx.config_dir);
    let report = crate::gc::run_gc(&data_dir, &config.storage).map_err(|e| e.to_string())?;
    Ok(serde_json::json!({
        "status": "completed",
        "chat_messages_pruned": report.chat_messages_pruned,
        "ci_tasks_pruned": report.ci_tasks_pruned,
        "docs_evicted": report.docs_evicted,
        "docs_evicted_names": report.docs_evicted_names,
    }))
}

async fn ipc_pub_topics(ctx: &IpcContext) -> Result<serde_json::Value, String> {
    // Query the doc store for syncing documents — these are the active gossip topics
    let docs = ctx.doc_store.list().map_err(|e| e.to_string())?;
    let topics: Vec<String> = docs.into_iter().collect();
    Ok(serde_json::json!({ "topics": topics }))
}

async fn ipc_pub_send(
    ctx: &IpcContext,
    params: &serde_json::Value,
) -> Result<serde_json::Value, String> {
    let topic = params
        .get("topic")
        .and_then(|v| v.as_str())
        .ok_or("missing 'topic' param")?;
    let message = params
        .get("message")
        .and_then(|v| v.as_str())
        .ok_or("missing 'message' param")?;

    let topic_id = bolo_core::TopicId::from_name(topic);
    let gossip_topic_id = iroh_gossip::TopicId::from_bytes(topic_id.0);

    // Get bootstrap peers from params if provided
    let bootstrap: Vec<iroh::EndpointId> = if let Some(peers) = params.get("peers") {
        peers
            .as_array()
            .unwrap_or(&vec![])
            .iter()
            .filter_map(|v| v.as_str()?.parse().ok())
            .collect()
    } else {
        vec![]
    };

    let topic_handle = ctx
        .gossip
        .subscribe(gossip_topic_id, bootstrap)
        .await
        .map_err(|e| format!("failed to join topic: {e}"))?;

    let key = ctx.gossip_key(topic);
    let payload = maybe_seal(message.as_bytes(), key.as_ref())
        .map_err(|e| format!("encryption failed: {e}"))?;

    let (sender, _receiver) = topic_handle.split();
    sender
        .broadcast(bytes::Bytes::from(payload))
        .await
        .map_err(|e| format!("failed to broadcast: {e}"))?;

    Ok(serde_json::json!({
        "sent": true,
        "topic": topic,
        "size": message.len(),
        "encrypted": key.is_some(),
    }))
}

async fn ipc_peer_add(
    ctx: &IpcContext,
    params: &serde_json::Value,
) -> Result<serde_json::Value, String> {
    let node_id_str = params
        .get("node_id")
        .and_then(|v| v.as_str())
        .ok_or("missing 'node_id' param")?;

    let remote: iroh::PublicKey = node_id_str
        .parse()
        .map_err(|_| format!("invalid node ID: {node_id_str}"))?;

    // Store the peer for gossip topic subscriptions
    {
        let mut peers = ctx.known_peers.lock().await;
        if !peers.contains(&remote) {
            peers.push(remote);
        }
    }

    // Persist to trust list so peer reconnects on restart
    {
        let mut trust_list = bolo_core::TrustList::load(&ctx.config_dir).unwrap_or_default();
        if trust_list.add(node_id_str) {
            trust_list.save(&ctx.config_dir).ok();
        }
    }

    // Subscribe to the CI topic with the new peer — this establishes the gossip
    // connection without the connect/close dance that poisons gossip's internal state.
    let ci_topic = ci_topic_id();
    ctx.gossip
        .subscribe(ci_topic, vec![remote])
        .await
        .map_err(|e| format!("failed to subscribe to CI topic with peer: {e}"))?;

    // Subscribe to the doc discovery topic with the new peer so we learn about
    // documents created on remote nodes.
    let discovery_topic = doc_discovery_topic_id();
    ctx.gossip
        .subscribe(discovery_topic, vec![remote])
        .await
        .map_err(|e| format!("failed to subscribe to doc discovery topic with peer: {e}"))?;

    // Subscribe to the deploy topic with the new peer so we receive deploy messages.
    let deploy_topic =
        iroh_gossip::TopicId::from_bytes(bolo_core::TopicId::from_name("bolo/deploy").0);
    ctx.gossip
        .subscribe(deploy_topic, vec![remote])
        .await
        .map_err(|e| format!("failed to subscribe to deploy topic with peer: {e}"))?;

    // Subscribe to the bench topic with the new peer.
    ctx.gossip
        .subscribe(bench_topic_id(), vec![remote])
        .await
        .map_err(|e| format!("failed to subscribe to bench topic with peer: {e}"))?;

    // Subscribe to mesh-status and logs topics with the new peer.
    ctx.gossip
        .subscribe(mesh_status_topic_id(), vec![remote])
        .await
        .map_err(|e| format!("failed to subscribe to mesh-status topic with peer: {e}"))?;
    ctx.gossip
        .subscribe(logs_topic_id(), vec![remote])
        .await
        .map_err(|e| format!("failed to subscribe to logs topic with peer: {e}"))?;

    // Join all existing doc topics with the new peer so gossip messages flow
    if let Ok(doc_names) = ctx.doc_store.list() {
        for name in &doc_names {
            let topic_id = doc_topic_id(name);
            if let Err(e) = ctx.gossip.subscribe(topic_id, vec![remote]).await {
                tracing::warn!(doc = %name, error = %e, "failed to add peer to doc topic");
            }
        }
    }

    // Join all chat channel topics with the new peer
    if let Ok(channels) = ctx.chat_store.list_channels() {
        for ch in &channels {
            let topic_id = chat_gossip_topic_id(ch);
            if let Err(e) = ctx.gossip.subscribe(topic_id, vec![remote]).await {
                tracing::warn!(channel = %ch, error = %e, "failed to add peer to chat topic");
            }
        }
    }

    // Log connection type for observability
    let conn_type = describe_connection(&ctx.endpoint, remote).await;
    tracing::info!(peer = %node_id_str, connection = %conn_type, "peer added");

    Ok(serde_json::json!({
        "added": true,
        "node_id": node_id_str,
        "connection": conn_type,
    }))
}

async fn ipc_peer_list(ctx: &IpcContext) -> Result<serde_json::Value, String> {
    let peers = ctx.known_peers.lock().await.clone();
    let mut peer_infos = Vec::new();
    for peer in &peers {
        let conn_type = describe_connection(&ctx.endpoint, *peer).await;
        peer_infos.push(serde_json::json!({
            "node_id": peer.to_string(),
            "connection": conn_type,
        }));
    }
    Ok(serde_json::json!({
        "node_id": ctx.node_id,
        "endpoint_id": ctx.endpoint.id().to_string(),
        "peers": peer_infos,
    }))
}

/// Describe how we're connected to a peer: "direct (ip:port)", "relay (url)", or "not connected".
async fn describe_connection(endpoint: &iroh::Endpoint, peer: iroh::PublicKey) -> String {
    if let Some(info) = endpoint.remote_info(peer).await {
        let mut direct = Vec::new();
        let mut relay = Vec::new();
        for addr_info in info.addrs() {
            let addr = addr_info.addr();
            if addr.is_ip() {
                // Format as just the socket addr without the Ip() wrapper
                direct.push(format!("{addr:?}"));
            } else if addr.is_relay() {
                relay.push(format!("{addr:?}"));
            }
        }
        let has_direct = !direct.is_empty();
        let has_relay = !relay.is_empty();
        match (has_direct, has_relay) {
            (true, true) => format!("direct + relay ({} addrs)", direct.len() + relay.len()),
            (true, false) => format!("direct ({} addrs)", direct.len()),
            (false, true) => format!("relay only ({} addrs)", relay.len()),
            (false, false) => "no known addrs".to_string(),
        }
    } else {
        "not connected".to_string()
    }
}

// --- Doc IPC handlers ---

/// Broadcast a doc snapshot to the gossip topic for the given path.
/// Reuses cached gossip senders for reliable delivery across multiple broadcasts.
async fn broadcast_doc_to_gossip(
    ctx: &IpcContext,
    path: &str,
    doc: &bolo_docs::loro::LoroDoc,
) -> Result<(), String> {
    let snapshot = doc
        .export(bolo_docs::loro::ExportMode::Snapshot)
        .map_err(|e| format!("failed to export snapshot: {e}"))?;

    let msg = DocSyncMessage::Snapshot {
        path: path.to_string(),
        data: snapshot,
        author: ctx.node_id.clone(),
        timestamp: Timestamp::now().0,
        nonce: rand::random(),
    };
    let msg_bytes = msg.to_bytes().map_err(|e| format!("serialize: {e}"))?;
    let key = ctx.gossip_key(&format!("bolo/doc/{path}"));
    let payload =
        maybe_seal(&msg_bytes, key.as_ref()).map_err(|e| format!("encryption failed: {e}"))?;

    // Use the registered sender from spawn_doc_sync_for_topic if available,
    // otherwise fall back to creating a new subscription (for first-time broadcasts).
    let registered_sender = {
        let senders = ctx.doc_senders.lock().await;
        senders.get(path).cloned()
    };

    let sender: std::sync::Arc<iroh_gossip::api::GossipSender> =
        if let Some(sender) = registered_sender {
            sender
        } else {
            let topic_id = doc_topic_id(path);
            let peers = ctx.known_peers.lock().await.clone();
            let topic_handle = ctx
                .gossip
                .subscribe(topic_id, peers)
                .await
                .map_err(|e| format!("failed to subscribe to doc topic: {e}"))?;
            let (sender, _receiver) = topic_handle.split();
            std::sync::Arc::new(sender)
        };

    sender
        .broadcast(bytes::Bytes::from(payload))
        .await
        .map_err(|e| format!("failed to broadcast doc update: {e}"))?;

    Ok(())
}

/// Announce a new document on the well-known discovery topic so that peers
/// automatically subscribe to its gossip topic.
///
/// The announcement includes the full doc snapshot so peers receive the initial
/// data on the already-established discovery topic (newly-created per-doc topics
/// may not yet have a working gossip mesh).
async fn announce_doc_on_discovery(ctx: &IpcContext, doc_path: &str) -> Result<(), String> {
    let discovery_topic = doc_discovery_topic_id();
    let peers = ctx.known_peers.lock().await.clone();
    let topic_handle = ctx
        .gossip
        .subscribe(discovery_topic, peers)
        .await
        .map_err(|e| format!("failed to subscribe to discovery topic: {e}"))?;

    // Build a wire message: nonce(8 bytes LE) | path_len(4 bytes LE) | path_bytes | snapshot_bytes
    // Nonce prevents PlumTree deduplication for repeated announcements.
    let mut payload = Vec::new();
    let nonce: u64 = rand::random();
    payload.extend_from_slice(&nonce.to_le_bytes());
    let path_bytes = doc_path.as_bytes();
    payload.extend_from_slice(&(path_bytes.len() as u32).to_le_bytes());
    payload.extend_from_slice(path_bytes);

    // Append snapshot if the doc exists
    if ctx.doc_store.exists(doc_path) {
        if let Ok(doc) = ctx.doc_store.load(doc_path) {
            if let Ok(snapshot) = doc.export(bolo_docs::loro::ExportMode::Snapshot) {
                payload.extend_from_slice(&snapshot);
            }
        }
    }

    let key = ctx.gossip_key("bolo/doc-discovery");
    let sealed =
        maybe_seal(&payload, key.as_ref()).map_err(|e| format!("encryption failed: {e}"))?;

    let (sender, _receiver) = topic_handle.split();
    sender
        .broadcast(bytes::Bytes::from(sealed))
        .await
        .map_err(|e| format!("failed to broadcast doc announcement: {e}"))?;

    tracing::info!(doc = %doc_path, "announced new document on discovery topic");
    Ok(())
}

async fn ipc_doc_create(
    ctx: &IpcContext,
    params: &serde_json::Value,
) -> Result<serde_json::Value, String> {
    let path = params
        .get("path")
        .and_then(|v| v.as_str())
        .ok_or("missing 'path' param")?;

    ctx.doc_store
        .create(path)
        .map_err(|e| format!("failed to create document: {e}"))?;

    // Subscribe to the gossip topic for this new doc so we receive updates
    let peers = ctx.known_peers.lock().await.clone();
    let sender = spawn_doc_sync_for_topic(
        &ctx.gossip,
        &ctx.doc_store,
        &ctx.node_id,
        path,
        peers,
        ctx.mesh_secret,
    )
    .await
    .map_err(|e| format!("failed to subscribe to doc topic: {e}"))?;
    ctx.doc_senders
        .lock()
        .await
        .insert(path.to_string(), sender);

    // Announce new doc on discovery topic so peers auto-subscribe
    let _ = announce_doc_on_discovery(ctx, path).await;

    Ok(serde_json::json!({ "created": true, "path": path }))
}

async fn ipc_doc_set(
    ctx: &IpcContext,
    params: &serde_json::Value,
) -> Result<serde_json::Value, String> {
    let path = params
        .get("path")
        .and_then(|v| v.as_str())
        .ok_or("missing 'path' param")?;
    let key = params
        .get("key")
        .and_then(|v| v.as_str())
        .ok_or("missing 'key' param")?;
    let value = params
        .get("value")
        .and_then(|v| v.as_str())
        .ok_or("missing 'value' param")?;

    let is_new = !ctx.doc_store.exists(path);
    let doc = if !is_new {
        ctx.doc_store
            .load(path)
            .map_err(|e| format!("failed to load: {e}"))?
    } else {
        ctx.doc_store
            .create(path)
            .map_err(|e| format!("failed to create: {e}"))?
    };

    // If this is a newly created doc, subscribe to its gossip topic for receiving updates
    if is_new {
        let peers = ctx.known_peers.lock().await.clone();
        if let Ok(sender) = spawn_doc_sync_for_topic(
            &ctx.gossip,
            &ctx.doc_store,
            &ctx.node_id,
            path,
            peers,
            ctx.mesh_secret,
        )
        .await
        {
            ctx.doc_senders
                .lock()
                .await
                .insert(path.to_string(), sender);
        }
    }

    let map = doc.get_map("data");
    map.insert(key, value)
        .map_err(|e| format!("failed to set key: {e}"))?;
    doc.commit();

    ctx.doc_store
        .save(path, &doc)
        .map_err(|e| format!("failed to save: {e}"))?;

    // Announce AFTER save so the discovery message includes the full snapshot
    if is_new {
        let _ = announce_doc_on_discovery(ctx, path).await;
    }

    // Broadcast to peers
    broadcast_doc_to_gossip(ctx, path, &doc).await?;

    Ok(serde_json::json!({ "path": path, "key": key, "value": value, "synced": true }))
}

/// Set multiple keys at once in a document, with a single commit and broadcast.
async fn ipc_doc_set_many(
    ctx: &IpcContext,
    params: &serde_json::Value,
) -> Result<serde_json::Value, String> {
    let path = params
        .get("path")
        .and_then(|v| v.as_str())
        .ok_or("missing 'path' param")?;
    let entries = params
        .get("entries")
        .and_then(|v| v.as_object())
        .ok_or("missing 'entries' param (must be object)")?;

    let is_new = !ctx.doc_store.exists(path);
    let doc = if !is_new {
        ctx.doc_store
            .load(path)
            .map_err(|e| format!("failed to load: {e}"))?
    } else {
        ctx.doc_store
            .create(path)
            .map_err(|e| format!("failed to create: {e}"))?
    };

    if is_new {
        let peers = ctx.known_peers.lock().await.clone();
        if let Ok(sender) = spawn_doc_sync_for_topic(
            &ctx.gossip,
            &ctx.doc_store,
            &ctx.node_id,
            path,
            peers,
            ctx.mesh_secret,
        )
        .await
        {
            ctx.doc_senders
                .lock()
                .await
                .insert(path.to_string(), sender);
        }
    }

    let map = doc.get_map("data");
    let mut count = 0usize;
    for (key, value) in entries {
        if let Some(val_str) = value.as_str() {
            map.insert(key, val_str)
                .map_err(|e| format!("failed to set key {key}: {e}"))?;
            count += 1;
        }
    }
    doc.commit();

    ctx.doc_store
        .save(path, &doc)
        .map_err(|e| format!("failed to save: {e}"))?;

    // Announce AFTER save so the discovery message includes the full snapshot
    if is_new {
        let _ = announce_doc_on_discovery(ctx, path).await;
    }

    broadcast_doc_to_gossip(ctx, path, &doc).await?;

    Ok(serde_json::json!({ "path": path, "count": count, "synced": true }))
}

async fn ipc_doc_get(
    ctx: &IpcContext,
    params: &serde_json::Value,
) -> Result<serde_json::Value, String> {
    let path = params
        .get("path")
        .and_then(|v| v.as_str())
        .ok_or("missing 'path' param")?;

    if !ctx.doc_store.exists(path) {
        return Err(format!("document not found: {path}"));
    }

    let doc = ctx
        .doc_store
        .load(path)
        .map_err(|e| format!("failed to load: {e}"))?;

    match params.get("key").and_then(|v| v.as_str()) {
        Some(key) => {
            let map = doc.get_map("data");
            let value = map
                .get(key)
                .and_then(|v| v.into_value().ok())
                .map(|v| format!("{v:?}"));
            Ok(serde_json::json!({ "path": path, "key": key, "value": value }))
        }
        None => {
            let value = doc.get_deep_value();
            Ok(serde_json::json!({ "path": path, "value": format!("{value:?}") }))
        }
    }
}

async fn ipc_doc_del(
    ctx: &IpcContext,
    params: &serde_json::Value,
) -> Result<serde_json::Value, String> {
    let path = params
        .get("path")
        .and_then(|v| v.as_str())
        .ok_or("missing 'path' param")?;
    let key = params
        .get("key")
        .and_then(|v| v.as_str())
        .ok_or("missing 'key' param")?;

    let doc = ctx
        .doc_store
        .load(path)
        .map_err(|e| format!("failed to load: {e}"))?;

    let map = doc.get_map("data");
    map.delete(key)
        .map_err(|e| format!("failed to delete key: {e}"))?;
    doc.commit();

    ctx.doc_store
        .save(path, &doc)
        .map_err(|e| format!("failed to save: {e}"))?;

    // Broadcast to peers
    broadcast_doc_to_gossip(ctx, path, &doc).await?;

    Ok(serde_json::json!({ "deleted": true, "path": path, "key": key, "synced": true }))
}

async fn ipc_doc_list(
    ctx: &IpcContext,
    params: &serde_json::Value,
) -> Result<serde_json::Value, String> {
    let mut names = ctx.doc_store.list().map_err(|e| e.to_string())?;

    if let Some(prefix) = params.get("prefix").and_then(|v| v.as_str()) {
        names.retain(|n| n.starts_with(prefix));
    }

    Ok(serde_json::json!({ "documents": names }))
}

async fn ipc_doc_read(
    ctx: &IpcContext,
    params: &serde_json::Value,
) -> Result<serde_json::Value, String> {
    let path = params
        .get("path")
        .and_then(|v| v.as_str())
        .ok_or("missing 'path' param")?;

    if !ctx.doc_store.exists(path) {
        return Err(format!("document not found: {path}"));
    }

    let doc = ctx
        .doc_store
        .load(path)
        .map_err(|e| format!("failed to load: {e}"))?;

    let text = doc.get_text("content");
    let content = text.to_string();

    Ok(serde_json::json!({ "path": path, "content": content }))
}

async fn ipc_doc_append(
    ctx: &IpcContext,
    params: &serde_json::Value,
) -> Result<serde_json::Value, String> {
    let path = params
        .get("path")
        .and_then(|v| v.as_str())
        .ok_or("missing 'path' param")?;
    let value = params
        .get("value")
        .and_then(|v| v.as_str())
        .ok_or("missing 'value' param")?;

    let doc = if ctx.doc_store.exists(path) {
        ctx.doc_store
            .load(path)
            .map_err(|e| format!("failed to load: {e}"))?
    } else {
        ctx.doc_store
            .create(path)
            .map_err(|e| format!("failed to create: {e}"))?
    };

    let list = doc.get_list("items");
    list.push(value)
        .map_err(|e| format!("failed to append: {e}"))?;
    let len = list.len();
    doc.commit();

    ctx.doc_store
        .save(path, &doc)
        .map_err(|e| format!("failed to save: {e}"))?;

    // Broadcast to peers
    broadcast_doc_to_gossip(ctx, path, &doc).await?;

    Ok(serde_json::json!({ "path": path, "value": value, "length": len, "synced": true }))
}

async fn ipc_doc_sync(
    ctx: &IpcContext,
    params: &serde_json::Value,
) -> Result<serde_json::Value, String> {
    let target_path = params.get("path").and_then(|v| v.as_str());

    let paths = match target_path {
        Some(p) => {
            if !ctx.doc_store.exists(p) {
                return Err(format!("document not found: {p}"));
            }
            vec![p.to_string()]
        }
        None => ctx.doc_store.list().map_err(|e| e.to_string())?,
    };

    let mut synced = Vec::new();
    for path in &paths {
        let doc = ctx
            .doc_store
            .load(path)
            .map_err(|e| format!("failed to load {path}: {e}"))?;

        if let Err(e) = broadcast_doc_to_gossip(ctx, path, &doc).await {
            tracing::warn!(doc = %path, error = %e, "failed to broadcast doc sync");
        } else {
            synced.push(path.as_str());
        }
    }

    Ok(serde_json::json!({ "synced": synced, "count": synced.len() }))
}

// --- Chat IPC handlers ---

async fn ipc_chat_join(
    ctx: &IpcContext,
    params: &serde_json::Value,
) -> Result<serde_json::Value, String> {
    let channel = params
        .get("channel")
        .and_then(|v| v.as_str())
        .ok_or("missing 'channel' param")?;

    ctx.chat_store
        .join_channel(channel)
        .map_err(|e| format!("failed to join channel: {e}"))?;

    // Spawn a persistent sync loop for this channel so it receives messages
    // and handles history requests even without an active `chat watch`.
    let peers = ctx.known_peers.lock().await.clone();
    spawn_chat_channel_listener(
        ctx.gossip.clone(),
        ctx.chat_store.clone(),
        channel.to_string(),
        ctx.mesh_secret,
        peers,
    )
    .await
    .map_err(|e| format!("failed to start chat sync for channel: {e}"))?;

    Ok(serde_json::json!({ "joined": true, "channel": channel }))
}

async fn ipc_chat_send(
    ctx: &IpcContext,
    params: &serde_json::Value,
) -> Result<serde_json::Value, String> {
    let channel = params
        .get("channel")
        .and_then(|v| v.as_str())
        .ok_or("missing 'channel' param")?;
    let wire_json = params
        .get("wire_message")
        .and_then(|v| v.as_str())
        .ok_or("missing 'wire_message' param")?;

    // Parse the wire message and store it
    let wire: bolo_chat::ChatWireMessage =
        serde_json::from_str(wire_json).map_err(|e| format!("invalid wire message: {e}"))?;

    if !ctx.chat_store.has_message(channel, &wire.msg.id) {
        ctx.chat_store
            .append(&wire.msg)
            .map_err(|e| format!("failed to store message: {e}"))?;
    }

    // Broadcast via gossip
    let topic_id = chat_gossip_topic_id(channel);
    let bootstrap: Vec<iroh::EndpointId> = if let Some(peers) = params.get("peers") {
        peers
            .as_array()
            .unwrap_or(&vec![])
            .iter()
            .filter_map(|v| v.as_str()?.parse().ok())
            .collect()
    } else {
        vec![]
    };

    let topic_handle = ctx
        .gossip
        .subscribe(topic_id, bootstrap)
        .await
        .map_err(|e| format!("failed to join chat topic: {e}"))?;

    // Broadcast in tagged format (ChatGossipMessage::Message) for history sync compat
    let gossip_msg = bolo_chat::ChatGossipMessage::Message {
        msg: wire.msg.clone(),
    };
    let msg_bytes =
        serde_json::to_vec(&gossip_msg).map_err(|e| format!("serialize message: {e}"))?;
    let key = ctx.gossip_key(&format!("chat/{channel}"));
    let payload =
        maybe_seal(&msg_bytes, key.as_ref()).map_err(|e| format!("encryption failed: {e}"))?;

    let (sender, _receiver) = topic_handle.split();
    sender
        .broadcast(bytes::Bytes::from(payload))
        .await
        .map_err(|e| format!("failed to broadcast: {e}"))?;

    Ok(serde_json::json!({
        "sent": true,
        "channel": channel,
        "id": wire.msg.id,
        "encrypted": key.is_some(),
    }))
}

async fn ipc_chat_history(
    ctx: &IpcContext,
    params: &serde_json::Value,
) -> Result<serde_json::Value, String> {
    let channel = params
        .get("channel")
        .and_then(|v| v.as_str())
        .ok_or("missing 'channel' param")?;
    let limit = params.get("limit").and_then(|v| v.as_u64()).unwrap_or(50) as usize;

    let messages = ctx
        .chat_store
        .history(channel, limit)
        .map_err(|e| format!("failed to get history: {e}"))?;

    Ok(serde_json::json!({ "messages": messages, "channel": channel }))
}

async fn ipc_chat_channels(ctx: &IpcContext) -> Result<serde_json::Value, String> {
    let channels = ctx
        .chat_store
        .list_channels()
        .map_err(|e| format!("failed to list channels: {e}"))?;

    Ok(serde_json::json!({ "channels": channels }))
}

/// Manual history sync: broadcast a history request on specified channel (or all channels).
async fn ipc_chat_sync(
    ctx: &IpcContext,
    params: &serde_json::Value,
) -> Result<serde_json::Value, String> {
    use bolo_chat::ChatGossipMessage;

    let channel = params.get("channel").and_then(|v| v.as_str());

    let channels: Vec<String> = if let Some(ch) = channel {
        vec![ch.to_string()]
    } else {
        ctx.chat_store
            .list_channels()
            .map_err(|e| format!("failed to list channels: {e}"))?
    };

    let mut synced_channels = 0;
    for ch in &channels {
        let topic_id = chat_gossip_topic_id(ch);
        let peers = ctx.known_peers.lock().await.clone();
        let topic_handle = ctx
            .gossip
            .subscribe(topic_id, peers)
            .await
            .map_err(|e| format!("failed to subscribe to chat topic: {e}"))?;

        let since = ctx
            .chat_store
            .latest_timestamp(ch)
            .unwrap_or(None)
            .unwrap_or(0);

        let request = ChatGossipMessage::HistoryRequest {
            channel: ch.clone(),
            since_timestamp: since,
            nonce: rand::random(),
        };
        let req_bytes =
            serde_json::to_vec(&request).map_err(|e| format!("serialize request: {e}"))?;
        let key = ctx.gossip_key(&format!("chat/{ch}"));
        let payload =
            maybe_seal(&req_bytes, key.as_ref()).map_err(|e| format!("encryption failed: {e}"))?;

        let (sender, _receiver) = topic_handle.split();
        sender
            .broadcast(bytes::Bytes::from(payload))
            .await
            .map_err(|e| format!("failed to broadcast sync request: {e}"))?;
        synced_channels += 1;
    }

    Ok(serde_json::json!({
        "synced_channels": synced_channels,
        "channels": channels,
    }))
}

// --- Task IPC handlers (CRDT-backed via DocStore at `tasks/board`) ---

const TASK_DOC_PATH: &str = "tasks/board";

/// Helper: load all tasks from the CRDT doc. Returns empty vec if doc doesn't exist.
fn load_tasks_from_doc(ctx: &IpcContext) -> Result<Vec<bolo_task::Task>, String> {
    if !ctx.doc_store.exists(TASK_DOC_PATH) {
        return Ok(Vec::new());
    }
    let doc = ctx
        .doc_store
        .load(TASK_DOC_PATH)
        .map_err(|e| format!("failed to load task doc: {e}"))?;
    let map = doc.get_map("data");

    // Use get_value() → serde_json for robust iteration after CRDT merge.
    // The LoroMap::for_each callback may not see all merged entries reliably.
    let map_value = map.get_value();
    let json_value =
        serde_json::to_value(&map_value).map_err(|e| format!("failed to serialize map: {e}"))?;

    let mut tasks = Vec::new();
    if let serde_json::Value::Object(obj) = json_value {
        for (_key, val) in obj {
            if let serde_json::Value::String(task_json) = val {
                if let Ok(task) = serde_json::from_str::<bolo_task::Task>(&task_json) {
                    tasks.push(task);
                }
            }
        }
    }
    tasks.sort_by(|a, b| {
        a.priority
            .cmp(&b.priority)
            .then(a.created_at.cmp(&b.created_at))
    });
    Ok(tasks)
}

/// Helper: save a task into the CRDT doc and broadcast.
/// Loads all existing tasks, adds/updates the given task, saves everything
/// in a single LoroDoc mutation (single peer ID), and broadcasts.
async fn save_task_to_doc(ctx: &IpcContext, task: &bolo_task::Task) -> Result<(), String> {
    let is_new = !ctx.doc_store.exists(TASK_DOC_PATH);
    let doc = if is_new {
        ctx.doc_store
            .create(TASK_DOC_PATH)
            .map_err(|e| format!("failed to create task doc: {e}"))?
    } else {
        ctx.doc_store
            .load(TASK_DOC_PATH)
            .map_err(|e| format!("failed to load task doc: {e}"))?
    };

    if is_new {
        let peers = ctx.known_peers.lock().await.clone();
        if let Ok(sender) = spawn_doc_sync_for_topic(
            &ctx.gossip,
            &ctx.doc_store,
            &ctx.node_id,
            TASK_DOC_PATH,
            peers,
            ctx.mesh_secret,
        )
        .await
        {
            ctx.doc_senders
                .lock()
                .await
                .insert(TASK_DOC_PATH.to_string(), sender);
        }
    }

    // Re-write ALL existing tasks + the new/updated task in a single mutation.
    // This ensures all map entries use the same peer ID, avoiding CRDT merge issues
    // from rapid load-modify-save cycles with different peer IDs.
    let mut all_tasks = load_tasks_from_doc(ctx)?;
    // Replace or add the task
    if let Some(existing) = all_tasks.iter_mut().find(|t| t.id == task.id) {
        *existing = task.clone();
    } else {
        all_tasks.push(task.clone());
    }

    let map = doc.get_map("data");
    for t in &all_tasks {
        let json = serde_json::to_string(t).map_err(|e| format!("serialize task: {e}"))?;
        map.insert(&t.id, json.as_str())
            .map_err(|e| format!("failed to set task: {e}"))?;
    }
    doc.commit();

    ctx.doc_store
        .save(TASK_DOC_PATH, &doc)
        .map_err(|e| format!("failed to save task doc: {e}"))?;

    if is_new {
        let _ = announce_doc_on_discovery(ctx, TASK_DOC_PATH).await;
    }
    broadcast_doc_to_gossip(ctx, TASK_DOC_PATH, &doc).await?;
    Ok(())
}

async fn ipc_task_create(
    ctx: &IpcContext,
    params: &serde_json::Value,
) -> Result<serde_json::Value, String> {
    let title = params
        .get("title")
        .and_then(|v| v.as_str())
        .ok_or("missing 'title' param")?;
    let priority = params.get("priority").and_then(|v| v.as_str());
    let assignee = params.get("assignee").and_then(|v| v.as_str());
    let status = params.get("status").and_then(|v| v.as_str());

    let now = Timestamp::now().0;
    let id_input = format!("{title}:{}:{now}", ctx.node_id);
    let id = blake3::hash(id_input.as_bytes()).to_hex()[..12].to_string();

    let task = bolo_task::Task {
        id,
        title: title.to_string(),
        status: match status {
            Some("ready") => bolo_task::TaskStatus::Ready,
            Some("in-progress") => bolo_task::TaskStatus::InProgress,
            Some("review") => bolo_task::TaskStatus::Review,
            Some("done") => bolo_task::TaskStatus::Done,
            _ => bolo_task::TaskStatus::Backlog,
        },
        assignee: assignee.map(|s| s.to_string()),
        priority: bolo_task::Priority::from_str_or_default(priority),
        spec_doc: params
            .get("spec_doc")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        dependencies: Vec::new(),
        commits: Vec::new(),
        ci_results: Vec::new(),
        review_doc: None,
        created_by: ctx.node_id.clone(),
        created_at: now,
        updated_at: now,
        claimed_by: None,
        claimed_at: None,
    };

    save_task_to_doc(ctx, &task).await?;

    serde_json::to_value(&task).map_err(|e| format!("serialize: {e}"))
}

async fn ipc_task_list(ctx: &IpcContext) -> Result<serde_json::Value, String> {
    let tasks = load_tasks_from_doc(ctx)?;
    let board = bolo_task::Board::from_tasks(tasks);
    serde_json::to_value(&board).map_err(|e| format!("serialize board: {e}"))
}

async fn ipc_task_show(
    ctx: &IpcContext,
    params: &serde_json::Value,
) -> Result<serde_json::Value, String> {
    let id = params
        .get("id")
        .and_then(|v| v.as_str())
        .ok_or("missing 'id' param")?;

    let tasks = load_tasks_from_doc(ctx)?;
    let task = tasks
        .into_iter()
        .find(|t| t.id == id)
        .ok_or_else(|| format!("task not found: {id}"))?;

    serde_json::to_value(&task).map_err(|e| format!("serialize: {e}"))
}

async fn ipc_task_update(
    ctx: &IpcContext,
    params: &serde_json::Value,
) -> Result<serde_json::Value, String> {
    let id = params
        .get("id")
        .and_then(|v| v.as_str())
        .ok_or("missing 'id' param")?;

    let tasks = load_tasks_from_doc(ctx)?;
    let mut task = tasks
        .into_iter()
        .find(|t| t.id == id)
        .ok_or_else(|| format!("task not found: {id}"))?;

    if let Some(status) = params.get("status").and_then(|v| v.as_str()) {
        task.status = match status {
            "backlog" => bolo_task::TaskStatus::Backlog,
            "ready" => bolo_task::TaskStatus::Ready,
            "in-progress" => bolo_task::TaskStatus::InProgress,
            "review" => bolo_task::TaskStatus::Review,
            "done" => bolo_task::TaskStatus::Done,
            other => return Err(format!("invalid status: {other}")),
        };
    }
    if let Some(priority) = params.get("priority").and_then(|v| v.as_str()) {
        task.priority = bolo_task::Priority::from_str_or_default(Some(priority));
    }
    if let Some(assignee) = params.get("assignee").and_then(|v| v.as_str()) {
        task.assignee = Some(assignee.to_string());
    }
    if let Some(title) = params.get("title").and_then(|v| v.as_str()) {
        task.title = title.to_string();
    }
    // Handle link fields in update too (assign, spec, commit, ci_result)
    if let Some(spec) = params.get("spec_doc").and_then(|v| v.as_str()) {
        task.spec_doc = Some(spec.to_string());
    }
    if let Some(commit) = params.get("commit").and_then(|v| v.as_str()) {
        if !task.commits.contains(&commit.to_string()) {
            task.commits.push(commit.to_string());
        }
    }
    if let Some(ci_result) = params.get("ci_result").and_then(|v| v.as_str()) {
        if !task.ci_results.contains(&ci_result.to_string()) {
            task.ci_results.push(ci_result.to_string());
        }
    }

    task.updated_at = Timestamp::now().0;
    save_task_to_doc(ctx, &task).await?;

    serde_json::to_value(&task).map_err(|e| format!("serialize: {e}"))
}

async fn ipc_task_delete(
    ctx: &IpcContext,
    params: &serde_json::Value,
) -> Result<serde_json::Value, String> {
    let id = params
        .get("id")
        .and_then(|v| v.as_str())
        .ok_or("missing 'id' param")?;

    if !ctx.doc_store.exists(TASK_DOC_PATH) {
        return Err(format!("task not found: {id}"));
    }
    let doc = ctx
        .doc_store
        .load(TASK_DOC_PATH)
        .map_err(|e| format!("failed to load task doc: {e}"))?;

    let map = doc.get_map("data");
    map.delete(id)
        .map_err(|e| format!("failed to delete task: {e}"))?;
    doc.commit();

    ctx.doc_store
        .save(TASK_DOC_PATH, &doc)
        .map_err(|e| format!("failed to save: {e}"))?;

    broadcast_doc_to_gossip(ctx, TASK_DOC_PATH, &doc).await?;

    Ok(serde_json::json!({ "deleted": true, "id": id }))
}

async fn ipc_task_claim(
    ctx: &IpcContext,
    params: &serde_json::Value,
) -> Result<serde_json::Value, String> {
    let id = params
        .get("id")
        .and_then(|v| v.as_str())
        .ok_or("missing 'id' param")?;
    let agent = params
        .get("agent")
        .and_then(|v| v.as_str())
        .unwrap_or(&ctx.node_id);
    let ttl_ms = params
        .get("ttl_ms")
        .and_then(|v| v.as_u64())
        .unwrap_or(bolo_task::DEFAULT_CLAIM_TTL_MS);

    let tasks = load_tasks_from_doc(ctx)?;
    let mut task = tasks
        .into_iter()
        .find(|t| t.id == id)
        .ok_or_else(|| format!("task not found: {id}"))?;

    let now = Timestamp::now().0;

    // Check if already claimed by someone else (and not expired)
    if let Some(current) = task.active_claimer(now, ttl_ms) {
        if current != agent {
            return Ok(serde_json::json!({
                "claimed": false,
                "conflict": true,
                "current_claimer": current,
                "task_id": id,
            }));
        }
        // Same agent re-claiming — refresh the heartbeat
    }

    task.claimed_by = Some(agent.to_string());
    task.claimed_at = Some(now);
    task.updated_at = now;

    // Auto-transition from ready/backlog to in-progress on claim
    if matches!(
        task.status,
        bolo_task::TaskStatus::Ready | bolo_task::TaskStatus::Backlog
    ) {
        task.status = bolo_task::TaskStatus::InProgress;
    }

    save_task_to_doc(ctx, &task).await?;

    Ok(serde_json::json!({
        "claimed": true,
        "task_id": id,
        "agent": agent,
        "status": task.status.to_string(),
        "expires_at": now + ttl_ms,
    }))
}

async fn ipc_task_release(
    ctx: &IpcContext,
    params: &serde_json::Value,
) -> Result<serde_json::Value, String> {
    let id = params
        .get("id")
        .and_then(|v| v.as_str())
        .ok_or("missing 'id' param")?;
    let agent = params
        .get("agent")
        .and_then(|v| v.as_str())
        .unwrap_or(&ctx.node_id);

    let tasks = load_tasks_from_doc(ctx)?;
    let mut task = tasks
        .into_iter()
        .find(|t| t.id == id)
        .ok_or_else(|| format!("task not found: {id}"))?;

    // Only the claimer (or anyone if no claim) can release
    if let Some(ref current) = task.claimed_by {
        if current != agent {
            return Err(format!("task {id} is claimed by {current}, not {agent}"));
        }
    }

    task.claimed_by = None;
    task.claimed_at = None;
    task.updated_at = Timestamp::now().0;

    save_task_to_doc(ctx, &task).await?;

    Ok(serde_json::json!({
        "released": true,
        "task_id": id,
    }))
}

// --- Review IPC handlers (CRDT-backed via Loro doc) ---

const REVIEW_DOC_PATH: &str = "reviews/all";

fn load_reviews_from_doc(ctx: &IpcContext) -> Result<Vec<bolo_git::ReviewComment>, String> {
    if !ctx.doc_store.exists(REVIEW_DOC_PATH) {
        return Ok(Vec::new());
    }
    let doc = ctx
        .doc_store
        .load(REVIEW_DOC_PATH)
        .map_err(|e| format!("failed to load review doc: {e}"))?;

    let map = doc.get_map("data");
    let map_value = map.get_value();
    let json_value =
        serde_json::to_value(&map_value).map_err(|e| format!("failed to serialize map: {e}"))?;

    let mut reviews = Vec::new();
    if let serde_json::Value::Object(obj) = json_value {
        for (_key, val) in obj {
            if let Some(json_str) = val.as_str() {
                if let Ok(review) = serde_json::from_str::<bolo_git::ReviewComment>(json_str) {
                    reviews.push(review);
                }
            }
        }
    }
    reviews.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));
    Ok(reviews)
}

async fn save_review_to_doc(
    ctx: &IpcContext,
    review: &bolo_git::ReviewComment,
) -> Result<(), String> {
    let is_new = !ctx.doc_store.exists(REVIEW_DOC_PATH);
    let doc = if is_new {
        ctx.doc_store
            .create(REVIEW_DOC_PATH)
            .map_err(|e| format!("failed to create review doc: {e}"))?
    } else {
        ctx.doc_store
            .load(REVIEW_DOC_PATH)
            .map_err(|e| format!("failed to load review doc: {e}"))?
    };

    if is_new {
        let peers = ctx.known_peers.lock().await.clone();
        if let Ok(sender) = spawn_doc_sync_for_topic(
            &ctx.gossip,
            &ctx.doc_store,
            &ctx.node_id,
            REVIEW_DOC_PATH,
            peers,
            ctx.mesh_secret,
        )
        .await
        {
            ctx.doc_senders
                .lock()
                .await
                .insert(REVIEW_DOC_PATH.to_string(), sender);
        }
    }

    let map = doc.get_map("data");
    let json = serde_json::to_string(review).map_err(|e| format!("serialize review: {e}"))?;
    map.insert(&review.id, json.as_str())
        .map_err(|e| format!("failed to set review: {e}"))?;
    doc.commit();

    ctx.doc_store
        .save(REVIEW_DOC_PATH, &doc)
        .map_err(|e| format!("failed to save review doc: {e}"))?;

    if is_new {
        let _ = announce_doc_on_discovery(ctx, REVIEW_DOC_PATH).await;
    }
    broadcast_doc_to_gossip(ctx, REVIEW_DOC_PATH, &doc).await?;
    Ok(())
}

async fn ipc_review_comment(
    ctx: &IpcContext,
    params: &serde_json::Value,
) -> Result<serde_json::Value, String> {
    let commit = params
        .get("commit")
        .and_then(|v| v.as_str())
        .ok_or("missing 'commit' param")?;
    let body = params
        .get("body")
        .and_then(|v| v.as_str())
        .ok_or("missing 'body' param")?;
    let file_path = params.get("file").and_then(|v| v.as_str());
    let line = params
        .get("line")
        .and_then(|v| v.as_u64())
        .map(|v| v as u32);
    let author = params
        .get("author")
        .and_then(|v| v.as_str())
        .unwrap_or(&ctx.node_id);

    let timestamp = Timestamp::now().0;
    let id_input = format!("comment:{commit}:{body}:{timestamp}");
    let id = blake3::hash(id_input.as_bytes()).to_hex().to_string();

    let review = bolo_git::ReviewComment {
        id: id.clone(),
        author: author.to_string(),
        commit_oid: commit.to_string(),
        file_path: file_path.map(|s| s.to_string()),
        line,
        body: body.to_string(),
        timestamp,
        status: bolo_git::ReviewStatus::Pending,
        signature: None,
    };

    save_review_to_doc(ctx, &review).await?;

    Ok(serde_json::json!({ "added": true, "id": id, "commit": commit }))
}

async fn ipc_review_approve(
    ctx: &IpcContext,
    params: &serde_json::Value,
) -> Result<serde_json::Value, String> {
    let commit = params
        .get("commit")
        .and_then(|v| v.as_str())
        .ok_or("missing 'commit' param")?;
    let author = params
        .get("author")
        .and_then(|v| v.as_str())
        .unwrap_or(&ctx.node_id);
    let signature = params.get("signature").and_then(|v| v.as_str());

    let timestamp = Timestamp::now().0;
    let id_input = format!("approve:{commit}:{author}:{timestamp}");
    let id = blake3::hash(id_input.as_bytes()).to_hex().to_string();

    let review = bolo_git::ReviewComment {
        id: id.clone(),
        author: author.to_string(),
        commit_oid: commit.to_string(),
        file_path: None,
        line: None,
        body: "Approved".to_string(),
        timestamp,
        status: bolo_git::ReviewStatus::Approved,
        signature: signature.map(|s| s.to_string()),
    };

    save_review_to_doc(ctx, &review).await?;

    Ok(serde_json::json!({
        "approved": true,
        "id": id,
        "commit": commit,
        "signature": signature,
    }))
}

async fn ipc_review_reject(
    ctx: &IpcContext,
    params: &serde_json::Value,
) -> Result<serde_json::Value, String> {
    let commit = params
        .get("commit")
        .and_then(|v| v.as_str())
        .ok_or("missing 'commit' param")?;
    let message = params
        .get("message")
        .and_then(|v| v.as_str())
        .unwrap_or("Changes requested");
    let author = params
        .get("author")
        .and_then(|v| v.as_str())
        .unwrap_or(&ctx.node_id);
    let signature = params.get("signature").and_then(|v| v.as_str());

    let timestamp = Timestamp::now().0;
    let id_input = format!("reject:{commit}:{author}:{timestamp}");
    let id = blake3::hash(id_input.as_bytes()).to_hex().to_string();

    let review = bolo_git::ReviewComment {
        id: id.clone(),
        author: author.to_string(),
        commit_oid: commit.to_string(),
        file_path: None,
        line: None,
        body: message.to_string(),
        timestamp,
        status: bolo_git::ReviewStatus::ChangesRequested,
        signature: signature.map(|s| s.to_string()),
    };

    save_review_to_doc(ctx, &review).await?;

    Ok(serde_json::json!({
        "rejected": true,
        "id": id,
        "commit": commit,
        "message": message,
        "signature": signature,
    }))
}

async fn ipc_review_show(
    ctx: &IpcContext,
    params: &serde_json::Value,
) -> Result<serde_json::Value, String> {
    let commit = params
        .get("commit")
        .and_then(|v| v.as_str())
        .ok_or("missing 'commit' param")?;

    let reviews = load_reviews_from_doc(ctx)?;
    let matching: Vec<_> = reviews
        .into_iter()
        .filter(|r| r.commit_oid == commit || r.commit_oid.starts_with(commit))
        .collect();

    serde_json::to_value(&matching).map_err(|e| format!("serialize: {e}"))
}

async fn ipc_review_ls(ctx: &IpcContext) -> Result<serde_json::Value, String> {
    let reviews = load_reviews_from_doc(ctx)?;

    // Group by commit, find those without an approval
    let mut by_commit: std::collections::HashMap<String, Vec<&bolo_git::ReviewComment>> =
        std::collections::HashMap::new();
    for r in &reviews {
        by_commit.entry(r.commit_oid.clone()).or_default().push(r);
    }

    let mut pending = Vec::new();
    for (commit, comments) in &by_commit {
        let has_approval = comments
            .iter()
            .any(|c| matches!(c.status, bolo_git::ReviewStatus::Approved));
        if !has_approval {
            pending.push(serde_json::json!({
                "commit": commit,
                "comments": comments,
            }));
        }
    }

    Ok(serde_json::json!({ "pending": pending }))
}

async fn ipc_deploy_push(
    ctx: &IpcContext,
    params: &serde_json::Value,
) -> Result<serde_json::Value, String> {
    use futures_lite::StreamExt;

    let message = params
        .get("message")
        .and_then(|v| v.as_str())
        .ok_or("missing 'message' param")?;

    let peer_id: Option<iroh::PublicKey> = params
        .get("peer")
        .and_then(|v| v.as_str())
        .map(|s| s.parse())
        .transpose()
        .map_err(|_| "invalid peer node ID")?;

    let topic_str = "bolo/deploy";
    let topic_id = iroh_gossip::TopicId::from_bytes(bolo_core::TopicId::from_name(topic_str).0);

    // Build bootstrap list: specified peer + all known peers
    let mut bootstrap: Vec<iroh::PublicKey> = ctx.known_peers.lock().await.clone();
    if let Some(p) = peer_id {
        if !bootstrap.contains(&p) {
            bootstrap.push(p);
        }
    }

    let topic_handle = ctx
        .gossip
        .subscribe(topic_id, bootstrap)
        .await
        .map_err(|e| format!("failed to subscribe to deploy topic: {e}"))?;

    let (sender, mut receiver) = topic_handle.split();

    // Wait for at least one neighbor to join before broadcasting
    let wait_result = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while let Some(Ok(event)) = receiver.next().await {
            if matches!(event, iroh_gossip::api::Event::NeighborUp(_)) {
                return true;
            }
        }
        false
    })
    .await;

    if wait_result != Ok(true) {
        return Err("no peers joined deploy topic within 10s".into());
    }

    let key = ctx.gossip_key(topic_str);
    let payload = maybe_seal(message.as_bytes(), key.as_ref())
        .map_err(|e| format!("encryption failed: {e}"))?;

    sender
        .broadcast(bytes::Bytes::from(payload))
        .await
        .map_err(|e| format!("failed to broadcast deploy message: {e}"))?;

    Ok(serde_json::json!({ "sent": true }))
}

fn chat_gossip_topic_id(channel: &str) -> iroh_gossip::TopicId {
    let topic = bolo_core::TopicId::from_name(&format!("chat/{channel}"));
    iroh_gossip::TopicId::from_bytes(topic.0)
}

// --- CI IPC handlers ---

async fn ipc_ci_run(
    ctx: &IpcContext,
    params: &serde_json::Value,
) -> Result<serde_json::Value, String> {
    let task_type_str = params
        .get("task_type")
        .and_then(|v| v.as_str())
        .unwrap_or("full");

    let task_type = match task_type_str {
        "build" => bolo_ci::TaskType::Build,
        "test" => bolo_ci::TaskType::Test,
        "check" => bolo_ci::TaskType::Check,
        "clippy" => bolo_ci::TaskType::Clippy,
        "fmt" => bolo_ci::TaskType::Fmt,
        "full" => bolo_ci::TaskType::Full,
        other => return Err(format!("unknown task type: {other}")),
    };

    let source_tree = params
        .get("source_tree")
        .and_then(|v| v.as_str())
        .unwrap_or("local")
        .to_string();

    let now = Timestamp::now().0;
    let task = bolo_ci::BuildTask {
        id: String::new(),
        task_type,
        source_tree,
        config_hash: None,
        rust_version: None,
        targets: vec![std::env::consts::ARCH.to_string()],
        status: bolo_ci::BuildStatus::Pending,
        verification: bolo_ci::Verification::default(),
        triggered_by: ctx.node_id.clone(),
        created_at: now,
        updated_at: now,
    };

    let task = ctx
        .ci_store
        .create_task(task)
        .map_err(|e| format!("failed to create task: {e}"))?;

    // Broadcast task to mesh
    let msg = CiMessage::TaskCreated {
        task: task.clone(),
        author: ctx.node_id.clone(),
        timestamp: now,
    };
    let msg_bytes = msg.to_bytes().map_err(|e| format!("serialize: {e}"))?;
    let key = ctx.gossip_key("bolo/ci");
    let payload =
        maybe_seal(&msg_bytes, key.as_ref()).map_err(|e| format!("encryption failed: {e}"))?;

    let peers = ctx.known_peers.lock().await.clone();
    let topic_handle = ctx
        .gossip
        .subscribe(ci_topic_id(), peers)
        .await
        .map_err(|e| format!("failed to subscribe to CI topic: {e}"))?;
    let (sender, _receiver) = topic_handle.split();
    sender
        .broadcast(bytes::Bytes::from(payload))
        .await
        .map_err(|e| format!("failed to broadcast CI task: {e}"))?;

    Ok(serde_json::json!({
        "task_id": task.id,
        "status": "pending",
        "broadcast": true,
    }))
}

async fn ipc_ci_status(ctx: &IpcContext) -> Result<serde_json::Value, String> {
    let tasks = ctx.ci_store.list_tasks().map_err(|e| e.to_string())?;
    let entries: Vec<serde_json::Value> = tasks
        .iter()
        .map(|t| {
            serde_json::json!({
                "id": t.id,
                "type": format!("{:?}", t.task_type),
                "source_tree": t.source_tree,
                "status": format!("{:?}", t.status),
                "triggered_by": t.triggered_by,
                "created_at": t.created_at,
            })
        })
        .collect();
    Ok(serde_json::json!({ "tasks": entries }))
}

async fn ipc_ci_results(
    ctx: &IpcContext,
    params: &serde_json::Value,
) -> Result<serde_json::Value, String> {
    let task_id = params
        .get("task_id")
        .and_then(|v| v.as_str())
        .ok_or("missing 'task_id' param")?;

    let results = ctx
        .ci_store
        .load_results(task_id)
        .map_err(|e| e.to_string())?;

    Ok(serde_json::json!({ "task_id": task_id, "results": results }))
}

/// Subscribe to the CI gossip topic and handle incoming build tasks.
async fn spawn_ci_sync_loop(
    gossip: iroh_gossip::Gossip,
    ci_store: std::sync::Arc<CiStore>,
    node_id: String,
    data_dir: PathBuf,
    blob_store: std::sync::Arc<bolo_blobs::FsStore>,
    doc_store: std::sync::Arc<DocStore>,
    mesh_secret: Option<[u8; 32]>,
) -> Result<()> {
    let topic = gossip
        .subscribe(ci_topic_id(), vec![])
        .await
        .context("failed to subscribe to CI topic")?;

    let ci_key = mesh_secret
        .as_ref()
        .map(|s| derive_gossip_key(s, "bolo/ci"));

    let (sender, mut receiver) = topic.split();

    tokio::spawn(async move {
        use futures_lite::StreamExt;
        while let Some(event) = receiver.try_next().await.transpose() {
            match event {
                Ok(iroh_gossip::api::Event::Received(msg)) => {
                    let content = maybe_open(&msg.content, ci_key.as_ref());
                    match CiMessage::from_bytes(&content) {
                        Ok(CiMessage::TaskCreated { task, author, .. }) => {
                            // Don't run our own tasks
                            if author == node_id {
                                tracing::debug!(task_id = %task.id, "ignoring own CI task");
                                continue;
                            }

                            tracing::info!(
                                task_id = %task.id,
                                from = %msg.delivered_from.fmt_short(),
                                "received CI task from mesh"
                            );

                            // Save task locally
                            let mut local_task = task.clone();
                            if ci_store.load_task(&task.id).is_err() {
                                // Use create_task which generates ID, but task already has one
                                // Save directly by updating
                                local_task.status = bolo_ci::BuildStatus::Running {
                                    peer: node_id.clone(),
                                };
                                local_task.updated_at = Timestamp::now().0;
                                let _ = ci_store.create_task(local_task.clone());
                            }

                            // Broadcast claim
                            let claim = CiMessage::Claim {
                                task_id: task.id.clone(),
                                peer: node_id.clone(),
                                timestamp: Timestamp::now().0,
                            };
                            if let Ok(claim_bytes) = claim.to_bytes() {
                                if let Ok(sealed) = maybe_seal(&claim_bytes, ci_key.as_ref()) {
                                    let _ = sender.broadcast(bytes::Bytes::from(sealed)).await;
                                }
                            }

                            // Run the build
                            let work_dir = data_dir.join("ci").join("workspace");
                            std::fs::create_dir_all(&work_dir).ok();

                            // Find a directory with Cargo.toml to build in.
                            // Walk from CWD upward to find the workspace root.
                            let build_dir = if work_dir.join("Cargo.toml").exists() {
                                work_dir.clone()
                            } else {
                                let cwd = std::env::current_dir().unwrap_or(work_dir.clone());
                                let mut dir = cwd.as_path();
                                loop {
                                    if dir.join("Cargo.toml").exists() {
                                        // Prefer the workspace root (has [workspace] section)
                                        let content =
                                            std::fs::read_to_string(dir.join("Cargo.toml"))
                                                .unwrap_or_default();
                                        if content.contains("[workspace]") {
                                            break dir.to_path_buf();
                                        }
                                    }
                                    match dir.parent() {
                                        Some(p) => dir = p,
                                        None => break cwd,
                                    }
                                }
                            };

                            let mut result = bolo_ci::runner::run_task(&task, &build_dir).await;
                            // Set the peer field to our actual node ID (runner defaults to "local")
                            result.peer = node_id.clone();

                            // If build succeeded, store the binary as a blob and publish release
                            if result.passed
                                && matches!(
                                    task.task_type,
                                    bolo_ci::TaskType::Build | bolo_ci::TaskType::Full
                                )
                            {
                                let platform =
                                    format!("{}/{}", std::env::consts::OS, std::env::consts::ARCH);
                                let binary_path = build_dir.join("target/debug/bolo");
                                if binary_path.exists() {
                                    match blob_store.add_path(&binary_path).with_tag().await {
                                        Ok(tag) => {
                                            let hash = tag.hash.to_string();
                                            tracing::info!(
                                                hash = %hash,
                                                platform = %platform,
                                                "stored build artifact as blob"
                                            );
                                            // Update releases/latest doc
                                            let doc = if doc_store.exists("releases/latest") {
                                                doc_store.load("releases/latest").ok()
                                            } else {
                                                doc_store.create("releases/latest").ok()
                                            };
                                            if let Some(doc) = doc {
                                                let map = doc.get_map("data");
                                                let _ = map.insert(&platform, hash.as_str());
                                                doc.commit();
                                                let _ = doc_store.save("releases/latest", &doc);
                                                tracing::info!(
                                                    platform = %platform,
                                                    "published release to releases/latest"
                                                );
                                            }
                                            let size = std::fs::metadata(&binary_path)
                                                .map(|m| m.len())
                                                .unwrap_or(0);
                                            result.artifacts.push(bolo_ci::ArtifactRef {
                                                name: format!("bolo-{platform}"),
                                                hash,
                                                size,
                                            });
                                        }
                                        Err(e) => {
                                            tracing::warn!(
                                                error = %e,
                                                "failed to store build artifact as blob"
                                            );
                                        }
                                    }
                                }
                            }

                            // Save result
                            let _ = ci_store.save_result(&result);

                            // Update task status
                            let now = Timestamp::now().0;
                            local_task.status = if result.passed {
                                bolo_ci::BuildStatus::Passed {
                                    peer: node_id.clone(),
                                    duration_ms: result.duration_ms,
                                }
                            } else {
                                bolo_ci::BuildStatus::Failed {
                                    peer: node_id.clone(),
                                    duration_ms: result.duration_ms,
                                    error: result.summary.clone(),
                                }
                            };
                            local_task.updated_at = now;
                            let _ = ci_store.update_task(&local_task);

                            // Broadcast result
                            let result_msg = CiMessage::Result {
                                result,
                                author: node_id.clone(),
                                timestamp: now,
                            };
                            if let Ok(result_bytes) = result_msg.to_bytes() {
                                if let Ok(sealed) = maybe_seal(&result_bytes, ci_key.as_ref()) {
                                    let _ = sender.broadcast(bytes::Bytes::from(sealed)).await;
                                }
                            }

                            tracing::info!(
                                task_id = %task.id,
                                status = ?local_task.status,
                                "CI task completed"
                            );
                        }
                        Ok(CiMessage::Claim {
                            task_id,
                            peer,
                            timestamp,
                        }) => {
                            tracing::debug!(
                                task_id = %task_id,
                                peer = %peer,
                                "peer claimed CI task"
                            );
                            // Update local task status if we have it
                            if let Ok(mut task) = ci_store.load_task(&task_id) {
                                if matches!(task.status, bolo_ci::BuildStatus::Pending) {
                                    task.status = bolo_ci::BuildStatus::Running { peer };
                                    task.updated_at = timestamp;
                                    let _ = ci_store.update_task(&task);
                                }
                            }
                        }
                        Ok(CiMessage::Result { result, author, .. }) => {
                            tracing::info!(
                                task_id = %result.task_id,
                                peer = %author,
                                passed = result.passed,
                                "received CI result from mesh"
                            );
                            // Save result from remote peer
                            let _ = ci_store.save_result(&result);

                            // Update task status
                            if let Ok(mut task) = ci_store.load_task(&result.task_id) {
                                task.status = if result.passed {
                                    bolo_ci::BuildStatus::Passed {
                                        peer: result.peer.clone(),
                                        duration_ms: result.duration_ms,
                                    }
                                } else {
                                    bolo_ci::BuildStatus::Failed {
                                        peer: result.peer.clone(),
                                        duration_ms: result.duration_ms,
                                        error: result.summary.clone(),
                                    }
                                };
                                task.updated_at = Timestamp::now().0;
                                let _ = ci_store.update_task(&task);
                            }
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, "failed to parse CI gossip message");
                        }
                    }
                }
                Ok(iroh_gossip::api::Event::NeighborUp(peer)) => {
                    tracing::info!(peer = %peer.fmt_short(), "peer joined CI topic");
                }
                Ok(iroh_gossip::api::Event::NeighborDown(peer)) => {
                    tracing::debug!(peer = %peer.fmt_short(), "peer left CI topic");
                }
                Ok(iroh_gossip::api::Event::Lagged) => {
                    tracing::warn!("CI gossip receiver lagged");
                }
                Err(e) => {
                    tracing::error!(error = %e, "CI gossip receiver error");
                    break;
                }
            }
        }
    });

    Ok(())
}

/// `bolo daemon stop` — stop a running daemon.
pub fn stop(config_flag: Option<&str>, json: bool) -> Result<()> {
    let config_dir = resolve_config_dir(config_flag)?;
    let state = DaemonState::load(&config_dir).context("daemon is not running")?;

    if !state.is_alive() {
        DaemonState::remove(&config_dir)?;
        if json {
            println!(r#"{{"status": "not_running"}}"#);
        } else {
            println!("Daemon is not running (stale state file cleaned up).");
        }
        return Ok(());
    }

    // Send SIGTERM
    let status = std::process::Command::new("kill")
        .arg(state.pid.to_string())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()?;

    if !status.success() {
        bail!("Failed to send stop signal to PID {}", state.pid);
    }

    // Wait briefly for process to exit
    for _ in 0..20 {
        if !state.is_alive() {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }

    if state.is_alive() {
        bail!(
            "Daemon PID {} did not exit within 2 seconds. Try `kill -9 {}`.",
            state.pid,
            state.pid
        );
    }

    // Clean up state file and socket (daemon should have done this, but just in case)
    DaemonState::remove(&config_dir)?;
    let sock_path = bolo_core::ipc::socket_path(&config_dir);
    std::fs::remove_file(&sock_path).ok();

    if json {
        let out = serde_json::json!({"status": "stopped", "pid": state.pid});
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Daemon stopped (PID {}).", state.pid);
    }

    Ok(())
}

/// `bolo daemon status` — show daemon status.
pub fn status(config_flag: Option<&str>, json: bool) -> Result<()> {
    let config_dir = resolve_config_dir(config_flag)?;

    match DaemonState::load(&config_dir) {
        Ok(state) => {
            let alive = state.is_alive();
            if !alive {
                DaemonState::remove(&config_dir)?;
            }

            let uptime = if alive {
                let now = Timestamp::now().0;
                let diff_secs = (now.saturating_sub(state.start_time_ms)) / 1000;
                format_uptime(diff_secs)
            } else {
                "N/A".to_string()
            };

            // Gather storage stats
            let data_dir_path = std::path::PathBuf::from(&state.data_dir);
            let storage = gather_storage_stats(&data_dir_path);

            if json {
                let out = serde_json::json!({
                    "running": alive,
                    "pid": state.pid,
                    "node_id": state.node_id,
                    "uptime_secs": if alive {
                        (Timestamp::now().0.saturating_sub(state.start_time_ms)) / 1000
                    } else { 0 },
                    "data_dir": state.data_dir,
                    "storage": storage,
                });
                println!("{}", serde_json::to_string_pretty(&out)?);
            } else if alive {
                println!("Daemon is running");
                println!("  Node ID: {}", state.node_id);
                println!("  PID:     {}", state.pid);
                println!("  Uptime:  {uptime}");
                println!("  Data:    {}", state.data_dir);
                if let Some(s) = &storage {
                    println!("  Storage:");
                    println!(
                        "    Blobs:  {}",
                        format_bytes(s["blobs_bytes"].as_u64().unwrap_or(0))
                    );
                    println!("    Docs:   {}", s["doc_count"].as_u64().unwrap_or(0));
                    println!(
                        "    Chats:  {} channels",
                        s["chat_channels"].as_u64().unwrap_or(0)
                    );
                    println!("    CI:     {} tasks", s["ci_tasks"].as_u64().unwrap_or(0));
                }
            } else {
                println!("Daemon is not running (stale state cleaned up).");
            }
        }
        Err(_) => {
            if json {
                println!(r#"{{"running": false}}"#);
            } else {
                println!("Daemon is not running.");
            }
        }
    }

    Ok(())
}

/// `bolo daemon logs` — view daemon log file.
pub fn logs(lines: usize, config_flag: Option<&str>, json: bool) -> Result<()> {
    let config_dir = resolve_config_dir(config_flag)?;
    let data_dir = resolve_data_dir(&config_dir);
    let log_path = data_dir.join("daemon.log");

    if !log_path.exists() {
        if json {
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "lines": [],
                    "error": "no log file found"
                }))?
            );
        } else {
            println!("No daemon log file found at {}", log_path.display());
            println!("Start the daemon in foreground to generate logs.");
        }
        return Ok(());
    }

    let log_lines = tail_file(&log_path, lines)?;

    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "lines": log_lines,
                "count": log_lines.len(),
                "path": log_path.to_string_lossy(),
            }))?
        );
    } else {
        for line in &log_lines {
            println!("{line}");
        }
    }

    Ok(())
}

/// `bolo logs` — stream logs from local daemon, a specific peer, or the entire mesh.
pub async fn stream_logs(
    follow: bool,
    lines: usize,
    peer: Option<&str>,
    mesh: bool,
    config_flag: Option<&str>,
) -> Result<()> {
    let config_dir = resolve_config_dir(config_flag)?;

    if mesh {
        // Stream from all peers + local
        stream_mesh_logs(follow, lines, config_flag).await
    } else if let Some(peer_id) = peer {
        // Stream from a specific remote peer
        stream_peer_logs(follow, lines, peer_id, config_flag).await
    } else {
        // Stream local daemon logs
        stream_local_logs(follow, lines, &config_dir)
    }
}

fn stream_local_logs(follow: bool, lines: usize, config_dir: &std::path::Path) -> Result<()> {
    let data_dir = resolve_data_dir(config_dir);
    let log_path = data_dir.join("daemon.log");

    if !log_path.exists() {
        bail!(
            "No log file at {}. Is the daemon running?\n  Start with: bolo daemon start",
            log_path.display()
        );
    }

    // Print initial lines
    let initial = tail_file(&log_path, lines)?;
    for line in &initial {
        println!("{line}");
    }

    if !follow {
        return Ok(());
    }

    // Follow mode: poll for new lines
    let mut last_len = std::fs::metadata(&log_path).map(|m| m.len()).unwrap_or(0);

    eprintln!("--- following {} (Ctrl+C to stop) ---", log_path.display());

    loop {
        std::thread::sleep(std::time::Duration::from_millis(200));

        let current_len = std::fs::metadata(&log_path).map(|m| m.len()).unwrap_or(0);

        if current_len > last_len {
            // Read only the new bytes
            let mut file = std::fs::File::open(&log_path).context("failed to open log file")?;
            std::io::Seek::seek(&mut file, std::io::SeekFrom::Start(last_len))?;
            let mut new_data = String::new();
            std::io::Read::read_to_string(&mut file, &mut new_data)?;
            for line in new_data.lines() {
                if !line.is_empty() {
                    println!("{line}");
                }
            }
            last_len = current_len;
        } else if current_len < last_len {
            // Log file was truncated (rotated), re-read from start
            last_len = 0;
        }
    }
}

async fn stream_peer_logs(
    follow: bool,
    lines: usize,
    peer_id: &str,
    config_flag: Option<&str>,
) -> Result<()> {
    let config_dir = resolve_config_dir(config_flag)?;
    let mut client = bolo_core::ipc::DaemonClient::connect(&config_dir)
        .await
        .context("daemon not running — start with: bolo daemon start")?;

    let short = &peer_id[..peer_id.len().min(16)];

    loop {
        let result = client
            .call(
                "peer.logs",
                serde_json::json!({ "node_id": peer_id, "lines": lines }),
            )
            .await?;

        if let Some(log_lines) = result["lines"].as_array() {
            for line in log_lines {
                if let Some(s) = line.as_str() {
                    println!("[{short}] {s}");
                }
            }
        } else if let Some(err) = result["error"].as_str() {
            bail!("peer.logs error: {err}");
        }

        if !follow {
            return Ok(());
        }

        // Poll interval for remote logs
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }
}

async fn stream_mesh_logs(follow: bool, lines: usize, config_flag: Option<&str>) -> Result<()> {
    let config_dir = resolve_config_dir(config_flag)?;
    let data_dir = resolve_data_dir(&config_dir);
    let log_path = data_dir.join("daemon.log");

    let mut client = bolo_core::ipc::DaemonClient::connect(&config_dir)
        .await
        .context("daemon not running — start with: bolo daemon start")?;

    // Get peer list
    let peers_result = client.call("peer.list", serde_json::json!({})).await?;
    let peer_ids: Vec<String> = peers_result
        .as_array()
        .unwrap_or(&vec![])
        .iter()
        .filter_map(|p| p["node_id"].as_str().map(|s| s.to_string()))
        .collect();

    // Get local node ID
    let local_id = if let Ok(state) = DaemonState::load(&config_dir) {
        state.node_id[..state.node_id.len().min(16)].to_string()
    } else {
        "local".to_string()
    };

    // Print initial local logs
    if log_path.exists() {
        let initial = tail_file(&log_path, lines)?;
        for line in &initial {
            println!("[{local_id}] {line}");
        }
    }

    // Fetch initial logs from each peer
    for peer_id in &peer_ids {
        let short = &peer_id[..peer_id.len().min(16)];
        if let Ok(result) = client
            .call(
                "peer.logs",
                serde_json::json!({ "node_id": peer_id, "lines": lines }),
            )
            .await
        {
            if let Some(log_lines) = result["lines"].as_array() {
                for line in log_lines {
                    if let Some(s) = line.as_str() {
                        println!("[{short}] {s}");
                    }
                }
            }
        }
    }

    if !follow {
        return Ok(());
    }

    eprintln!(
        "--- following local + {} peer(s) (Ctrl+C to stop) ---",
        peer_ids.len()
    );

    let mut last_len = std::fs::metadata(&log_path).map(|m| m.len()).unwrap_or(0);

    loop {
        // Check local log for new lines
        let current_len = std::fs::metadata(&log_path).map(|m| m.len()).unwrap_or(0);
        if current_len > last_len {
            let mut file = std::fs::File::open(&log_path)?;
            std::io::Seek::seek(&mut file, std::io::SeekFrom::Start(last_len))?;
            let mut new_data = String::new();
            std::io::Read::read_to_string(&mut file, &mut new_data)?;
            for line in new_data.lines() {
                if !line.is_empty() {
                    println!("[{local_id}] {line}");
                }
            }
            last_len = current_len;
        }

        // Poll each peer for new logs (last 10 lines, dedup by content)
        for peer_id in &peer_ids {
            let short = &peer_id[..peer_id.len().min(16)];
            if let Ok(result) = client
                .call(
                    "peer.logs",
                    serde_json::json!({ "node_id": peer_id, "lines": 10 }),
                )
                .await
            {
                if let Some(log_lines) = result["lines"].as_array() {
                    for line in log_lines {
                        if let Some(s) = line.as_str() {
                            println!("[{short}] {s}");
                        }
                    }
                }
            }
        }

        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }
}

/// Read the last N lines from a file.
fn tail_file(path: &std::path::Path, n: usize) -> Result<Vec<String>> {
    let content = std::fs::read_to_string(path).context("failed to read log file")?;
    let all_lines: Vec<&str> = content.lines().collect();
    let start = all_lines.len().saturating_sub(n);
    Ok(all_lines[start..].iter().map(|s| s.to_string()).collect())
}

/// `bolo daemon export <path>` — export full node state to a tar archive.
pub fn export(path: &str, config_flag: Option<&str>, json: bool) -> Result<()> {
    let config_dir = resolve_config_dir(config_flag)?;

    // Check daemon is not running
    if let Ok(state) = DaemonState::load(&config_dir) {
        if state.is_alive() {
            bail!(
                "Daemon is running (PID {}). Stop it with `bolo daemon stop` before exporting.",
                state.pid
            );
        }
    }

    if !config_dir.exists() {
        bail!("Config directory not found: {}", config_dir.display());
    }

    // Create a tar.gz archive of the config directory
    let out_file =
        std::fs::File::create(path).with_context(|| format!("failed to create {path}"))?;
    let enc = flate2::write::GzEncoder::new(out_file, flate2::Compression::default());
    let mut tar = tar::Builder::new(enc);
    tar.append_dir_all("bolo", &config_dir)
        .context("failed to write archive")?;
    tar.finish().context("failed to finalize archive")?;

    let size = std::fs::metadata(path)?.len();

    if json {
        let out = serde_json::json!({
            "exported": true,
            "path": path,
            "size": size,
            "source": config_dir.to_string_lossy(),
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Exported node state to {path} ({size} bytes)");
    }

    Ok(())
}

/// `bolo daemon import <path>` — import node state from a tar archive.
pub fn import(path: &str, force: bool, config_flag: Option<&str>, json: bool) -> Result<()> {
    let config_dir = resolve_config_dir(config_flag)?;

    // Check daemon is not running
    if let Ok(state) = DaemonState::load(&config_dir) {
        if state.is_alive() {
            bail!(
                "Daemon is running (PID {}). Stop it with `bolo daemon stop` before importing.",
                state.pid
            );
        }
    }

    if config_dir.exists() && !force {
        bail!(
            "Config directory already exists: {}. Use --force to overwrite.",
            config_dir.display()
        );
    }

    let in_file = std::fs::File::open(path).with_context(|| format!("failed to open {path}"))?;
    let dec = flate2::read::GzDecoder::new(in_file);
    let mut tar = tar::Archive::new(dec);

    // Extract to a temp dir first, then move
    let parent = config_dir.parent().context("invalid config dir")?;
    std::fs::create_dir_all(parent)?;

    // Clear existing if force
    if config_dir.exists() && force {
        std::fs::remove_dir_all(&config_dir)?;
    }
    std::fs::create_dir_all(&config_dir)?;

    // Extract — the archive has a "bolo/" prefix
    for entry in tar.entries().context("failed to read archive")? {
        let mut entry = entry?;
        let entry_path = entry.path()?.into_owned();

        // Strip the "bolo/" prefix
        let relative = if let Ok(stripped) = entry_path.strip_prefix("bolo") {
            stripped.to_path_buf()
        } else {
            entry_path
        };

        if relative.as_os_str().is_empty() {
            continue;
        }

        let dest = config_dir.join(&relative);
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent)?;
        }

        if entry.header().entry_type().is_dir() {
            std::fs::create_dir_all(&dest)?;
        } else {
            let mut out = std::fs::File::create(&dest)?;
            std::io::copy(&mut entry, &mut out)?;
        }
    }

    if json {
        let out = serde_json::json!({
            "imported": true,
            "path": path,
            "config_dir": config_dir.to_string_lossy(),
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Imported node state from {path}");
        println!("  Config: {}", config_dir.display());
    }

    Ok(())
}

/// Subscribe to the well-known doc discovery topic and handle incoming announcements.
///
/// When a peer creates a new document, it broadcasts the doc path on this topic.
/// We respond by subscribing to the new doc's gossip topic so updates flow automatically.
async fn spawn_doc_discovery_listener(
    gossip: iroh_gossip::Gossip,
    doc_store: std::sync::Arc<DocStore>,
    node_id: String,
    mesh_secret: Option<[u8; 32]>,
) -> Result<()> {
    let discovery_topic = doc_discovery_topic_id();
    let topic = gossip
        .subscribe(discovery_topic, vec![])
        .await
        .context("failed to subscribe to doc discovery topic")?;

    let discovery_key = mesh_secret
        .as_ref()
        .map(|s| derive_gossip_key(s, "bolo/doc-discovery"));

    let (_sender, mut receiver) = topic.split();
    let gossip_clone = gossip.clone();

    tokio::spawn(async move {
        use futures_lite::StreamExt;
        while let Some(event) = receiver.try_next().await.transpose() {
            match event {
                Ok(iroh_gossip::api::Event::Received(msg)) => {
                    // Decrypt if mesh_secret is configured
                    let decrypted = maybe_open(&msg.content, discovery_key.as_ref());
                    // Parse wire format: nonce(8 bytes LE) | path_len(4 bytes LE) | path_bytes | snapshot_bytes
                    // For backward compat, also accept old format without nonce (< 12 byte header)
                    let content = &decrypted[..];
                    let offset = if content.len() >= 12 {
                        // New format: skip 8-byte nonce
                        8
                    } else {
                        // Legacy format: no nonce
                        0
                    };
                    if content.len() < offset + 4 {
                        tracing::warn!("discovery message too short");
                        continue;
                    }
                    let path_len = u32::from_le_bytes([
                        content[offset],
                        content[offset + 1],
                        content[offset + 2],
                        content[offset + 3],
                    ]) as usize;
                    if content.len() < offset + 4 + path_len {
                        tracing::warn!("discovery message truncated");
                        continue;
                    }
                    let doc_path =
                        match std::str::from_utf8(&content[offset + 4..offset + 4 + path_len]) {
                            Ok(s) => s.to_string(),
                            Err(_) => {
                                tracing::warn!("invalid UTF-8 in doc discovery path");
                                continue;
                            }
                        };
                    let snapshot_data = &content[offset + 4 + path_len..];

                    tracing::info!(
                        doc = %doc_path,
                        from = %msg.delivered_from.fmt_short(),
                        snapshot_bytes = snapshot_data.len(),
                        "discovered new document from peer"
                    );

                    // Apply the snapshot if data was included
                    if !snapshot_data.is_empty() {
                        let sync_msg = DocSyncMessage::Snapshot {
                            path: doc_path.clone(),
                            data: snapshot_data.to_vec(),
                            author: msg.delivered_from.fmt_short().to_string(),
                            timestamp: Timestamp::now().0,
                            nonce: rand::random(),
                        };
                        match apply_sync_message(&doc_store, &sync_msg) {
                            Ok(true) => {
                                tracing::info!(
                                    doc = %doc_path,
                                    "applied discovered doc snapshot"
                                );
                            }
                            Ok(false) => {}
                            Err(e) => {
                                tracing::warn!(
                                    doc = %doc_path,
                                    error = %e,
                                    "failed to apply discovered doc snapshot"
                                );
                            }
                        }
                    }

                    // Subscribe to the doc's gossip topic with the announcing peer
                    // as a bootstrap peer so future updates flow.
                    if let Err(e) = spawn_doc_sync_for_topic(
                        &gossip_clone,
                        &doc_store,
                        &node_id,
                        &doc_path,
                        vec![msg.delivered_from],
                        mesh_secret,
                    )
                    .await
                    {
                        tracing::warn!(
                            doc = %doc_path,
                            error = %e,
                            "failed to subscribe to discovered doc topic"
                        );
                    }
                }
                Ok(iroh_gossip::api::Event::NeighborUp(peer)) => {
                    tracing::debug!(peer = %peer.fmt_short(), "peer joined doc discovery topic");
                }
                Ok(iroh_gossip::api::Event::NeighborDown(peer)) => {
                    tracing::debug!(peer = %peer.fmt_short(), "peer left doc discovery topic");
                }
                Ok(iroh_gossip::api::Event::Lagged) => {
                    tracing::warn!("doc discovery gossip receiver lagged");
                }
                Err(e) => {
                    tracing::error!(error = %e, "doc discovery gossip error");
                    break;
                }
            }
        }
    });

    Ok(())
}

/// Subscribe to a single document's gossip topic and spawn a background sync task.
async fn spawn_doc_sync_for_topic(
    gossip: &iroh_gossip::Gossip,
    doc_store: &std::sync::Arc<DocStore>,
    node_id: &str,
    doc_name: &str,
    bootstrap_peers: Vec<iroh::PublicKey>,
    mesh_secret: Option<[u8; 32]>,
) -> Result<std::sync::Arc<iroh_gossip::api::GossipSender>> {
    let topic_id = doc_topic_id(doc_name);
    let topic = gossip
        .subscribe(topic_id, bootstrap_peers)
        .await
        .context("failed to subscribe to doc topic")?;

    let doc_key = mesh_secret
        .as_ref()
        .map(|s| derive_gossip_key(s, &format!("bolo/doc/{doc_name}")));

    let (sender, mut receiver) = topic.split();
    let sender = std::sync::Arc::new(sender);
    let sender_ret = sender.clone();
    let store = doc_store.clone();
    let my_node_id = node_id.to_string();
    let doc_name = doc_name.to_string();

    tokio::spawn(async move {
        use futures_lite::StreamExt;
        while let Some(event) = receiver.try_next().await.transpose() {
            match event {
                Ok(iroh_gossip::api::Event::Received(msg)) => {
                    let content = maybe_open(&msg.content, doc_key.as_ref());
                    match DocSyncMessage::from_bytes(&content) {
                        Ok(DocSyncMessage::SyncRequest { path, .. }) => {
                            // Respond with our local snapshot
                            if store.exists(&path) {
                                if let Ok(doc) = store.load(&path) {
                                    if let Ok(snapshot) =
                                        doc.export(bolo_docs::loro::ExportMode::Snapshot)
                                    {
                                        let resp = DocSyncMessage::Snapshot {
                                            path,
                                            data: snapshot,
                                            author: my_node_id.clone(),
                                            timestamp: Timestamp::now().0,
                                            nonce: rand::random(),
                                        };
                                        if let Ok(resp_bytes) = resp.to_bytes() {
                                            if let Ok(sealed) =
                                                maybe_seal(&resp_bytes, doc_key.as_ref())
                                            {
                                                let _ = sender
                                                    .broadcast(bytes::Bytes::from(sealed))
                                                    .await;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        Ok(sync_msg) => match apply_sync_message(&store, &sync_msg) {
                            Ok(true) => {
                                tracing::info!(
                                    doc = %doc_name,
                                    from = %msg.delivered_from.fmt_short(),
                                    "applied doc sync update"
                                );
                            }
                            Ok(false) => {}
                            Err(e) => {
                                tracing::warn!(
                                    doc = %doc_name,
                                    error = %e,
                                    "failed to apply doc sync message"
                                );
                            }
                        },
                        Err(e) => {
                            tracing::warn!(
                                doc = %doc_name,
                                error = %e,
                                "failed to parse doc sync message"
                            );
                        }
                    }
                }
                Ok(iroh_gossip::api::Event::NeighborUp(peer)) => {
                    tracing::info!(doc = %doc_name, peer = %peer.fmt_short(), "peer joined doc topic");
                    // Send our current snapshot to the new peer
                    if store.exists(&doc_name) {
                        if let Ok(doc) = store.load(&doc_name) {
                            if let Ok(snapshot) = doc.export(bolo_docs::loro::ExportMode::Snapshot)
                            {
                                let msg = DocSyncMessage::Snapshot {
                                    path: doc_name.clone(),
                                    data: snapshot,
                                    author: my_node_id.clone(),
                                    timestamp: Timestamp::now().0,
                                    nonce: rand::random(),
                                };
                                if let Ok(msg_bytes) = msg.to_bytes() {
                                    if let Ok(sealed) = maybe_seal(&msg_bytes, doc_key.as_ref()) {
                                        let _ = sender.broadcast(bytes::Bytes::from(sealed)).await;
                                    }
                                }
                            }
                        }
                    }
                }
                Ok(iroh_gossip::api::Event::NeighborDown(peer)) => {
                    tracing::debug!(doc = %doc_name, peer = %peer.fmt_short(), "peer left doc topic");
                }
                Ok(iroh_gossip::api::Event::Lagged) => {
                    tracing::warn!(doc = %doc_name, "gossip receiver lagged");
                }
                Err(e) => {
                    tracing::error!(doc = %doc_name, error = %e, "gossip receiver error");
                    break;
                }
            }
        }
    });

    Ok(sender_ret)
}

/// Subscribe to gossip topics for all local documents and spawn background sync tasks.
///
/// Returns the number of documents being synced.
async fn spawn_doc_sync_loop(
    gossip: iroh_gossip::Gossip,
    doc_store: std::sync::Arc<DocStore>,
    node_id: String,
    mesh_secret: Option<[u8; 32]>,
) -> Result<usize> {
    let doc_names = doc_store.list().context("failed to list documents")?;
    let count = doc_names.len();

    for name in doc_names {
        spawn_doc_sync_for_topic(&gossip, &doc_store, &node_id, &name, vec![], mesh_secret).await?;
    }

    Ok(count)
}

/// `bolo daemon upgrade` — fetch latest bolo binary from the mesh and replace self.
pub async fn upgrade(platform: Option<&str>, config_flag: Option<&str>, json: bool) -> Result<()> {
    let config_dir = resolve_config_dir(config_flag)?;
    let platform_key = platform
        .map(|s| s.to_string())
        .unwrap_or_else(|| format!("{}/{}", std::env::consts::OS, std::env::consts::ARCH));

    let mut client = bolo_core::ipc::DaemonClient::connect(&config_dir)
        .await
        .context(
            "Cannot upgrade: daemon is not running.\n\
             Start the daemon with `bolo daemon start`, then retry.",
        )?;

    // Read the releases/latest doc
    let result = client
        .call(
            "doc.get",
            serde_json::json!({ "path": "releases/latest", "key": platform_key }),
        )
        .await
        .context("failed to read releases/latest doc")?;

    let blob_hash = result
        .get("value")
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            anyhow::anyhow!(
                "No release found for platform '{platform_key}' in releases/latest doc.\n\
                 Publish a release first with: bolo ci run --type build"
            )
        })?;

    // Strip surrounding quotes if the value was stored as a debug-formatted string
    let blob_hash = blob_hash
        .trim_matches('"')
        .trim_start_matches("String(\"")
        .trim_end_matches("\")");

    if !json {
        println!("Found release for {platform_key}: {blob_hash}");
        println!("Fetching binary from mesh...");
    }

    // Fetch the blob to a temp file
    let tmp_path = std::env::temp_dir().join(format!("bolo-upgrade-{blob_hash}"));
    let result = client
        .call(
            "blob.get",
            serde_json::json!({ "hash": blob_hash, "path": tmp_path.to_string_lossy() }),
        )
        .await
        .context("failed to fetch binary blob")?;

    let size = result.get("size").and_then(|v| v.as_u64()).unwrap_or(0);

    // Set executable permission
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&tmp_path, std::fs::Permissions::from_mode(0o755))?;
    }

    // Replace the current binary
    let current_exe = std::env::current_exe().context("failed to resolve current binary")?;

    if !json {
        println!("Downloaded {size} bytes.");
        println!("Stopping daemon...");
    }

    // Stop daemon
    drop(client);
    stop(config_flag, false).ok();

    // Replace binary
    let backup_path = current_exe.with_extension("bak");
    std::fs::rename(&current_exe, &backup_path).context("failed to backup current binary")?;
    if let Err(e) = std::fs::copy(&tmp_path, &current_exe) {
        // Restore backup on failure
        std::fs::rename(&backup_path, &current_exe).ok();
        bail!("failed to install new binary: {e}");
    }

    // Set executable permission on installed binary
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&current_exe, std::fs::Permissions::from_mode(0o755))?;
    }

    // Clean up
    std::fs::remove_file(&tmp_path).ok();
    std::fs::remove_file(&backup_path).ok();

    if json {
        let out = serde_json::json!({
            "upgraded": true,
            "platform": platform_key,
            "hash": blob_hash,
            "size": size,
            "binary": current_exe.to_string_lossy(),
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Upgraded bolo binary ({size} bytes).");
        println!("Restart daemon with: bolo daemon start");
    }

    Ok(())
}

// --- Chat sync loop ---

/// Spawn a persistent gossip listener for a single chat channel.
/// Handles incoming messages, history requests, and history responses.
/// On `NeighborUp`, automatically requests missed messages from the new peer.
async fn spawn_chat_channel_listener(
    gossip: iroh_gossip::Gossip,
    chat_store: std::sync::Arc<bolo_chat::ChatStore>,
    channel: String,
    mesh_secret: Option<[u8; 32]>,
    bootstrap_peers: Vec<iroh::PublicKey>,
) -> Result<()> {
    use bolo_chat::ChatGossipMessage;
    use futures_lite::StreamExt;

    let topic_id = chat_gossip_topic_id(&channel);
    let topic_handle = gossip
        .subscribe(topic_id, bootstrap_peers)
        .await
        .context("failed to subscribe to chat topic")?;

    let (sender, mut receiver) = topic_handle.split();

    let key = mesh_secret
        .as_ref()
        .map(|s| derive_gossip_key(s, &format!("chat/{channel}")));
    let sender = std::sync::Arc::new(sender);
    let store = chat_store;

    // If we have bootstrap peers, immediately request history
    let init_sender = sender.clone();
    let init_store = store.clone();
    let init_key = key.clone();
    let init_channel = channel.clone();
    tokio::spawn(async move {
        // Small delay to let gossip connections establish
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        let since = init_store
            .latest_timestamp(&init_channel)
            .unwrap_or(None)
            .unwrap_or(0);
        let request = ChatGossipMessage::HistoryRequest {
            channel: init_channel.clone(),
            since_timestamp: since,
            nonce: rand::random(),
        };
        if let Ok(req_bytes) = serde_json::to_vec(&request) {
            let payload = maybe_seal(&req_bytes, init_key.as_ref()).unwrap_or(req_bytes);
            let _ = init_sender.broadcast(bytes::Bytes::from(payload)).await;
            tracing::debug!(
                channel = %init_channel,
                since,
                "chat: initial history request sent"
            );
        }
    });

    tokio::spawn(async move {
        while let Some(Ok(event)) = receiver.next().await {
            match event {
                iroh_gossip::api::Event::Received(gossip_msg) => {
                    let content = maybe_open(&gossip_msg.content, key.as_ref());

                    // Try new tagged format first, fall back to legacy ChatWireMessage
                    if let Ok(gossip_message) =
                        serde_json::from_slice::<ChatGossipMessage>(&content)
                    {
                        match gossip_message {
                            ChatGossipMessage::Message { msg } => {
                                if !store.has_message(&channel, &msg.id) {
                                    let _ = store.append(&msg);
                                    tracing::debug!(
                                        channel = %channel,
                                        id = %msg.id,
                                        "chat: stored incoming message"
                                    );
                                }
                            }
                            ChatGossipMessage::HistoryRequest {
                                channel: req_channel,
                                since_timestamp,
                                ..
                            } => {
                                if req_channel != channel {
                                    continue;
                                }
                                // Respond with messages since the requested timestamp
                                if let Ok(msgs) = store.messages_since(&channel, since_timestamp) {
                                    if msgs.is_empty() {
                                        continue;
                                    }
                                    let resp = ChatGossipMessage::HistoryResponse {
                                        channel: channel.clone(),
                                        messages: msgs,
                                        nonce: rand::random(),
                                    };
                                    if let Ok(resp_bytes) = serde_json::to_vec(&resp) {
                                        let payload = maybe_seal(&resp_bytes, key.as_ref())
                                            .unwrap_or(resp_bytes);
                                        let _ = sender.broadcast(bytes::Bytes::from(payload)).await;
                                        tracing::debug!(
                                            channel = %channel,
                                            since = since_timestamp,
                                            "chat: sent history response"
                                        );
                                    }
                                }
                            }
                            ChatGossipMessage::HistoryResponse {
                                channel: resp_channel,
                                messages,
                                ..
                            } => {
                                if resp_channel != channel {
                                    continue;
                                }
                                let mut synced = 0usize;
                                for msg in messages {
                                    if !store.has_message(&channel, &msg.id) {
                                        let _ = store.append(&msg);
                                        synced += 1;
                                    }
                                }
                                if synced > 0 {
                                    tracing::info!(
                                        channel = %channel,
                                        count = synced,
                                        "chat: synced missed messages"
                                    );
                                }
                            }
                        }
                    } else if let Ok(wire) =
                        serde_json::from_slice::<bolo_chat::ChatWireMessage>(&content)
                    {
                        // Legacy format — just store the message
                        if !store.has_message(&channel, &wire.msg.id) {
                            let _ = store.append(&wire.msg);
                        }
                    }
                }
                iroh_gossip::api::Event::NeighborUp(peer) => {
                    tracing::info!(
                        channel = %channel,
                        peer = %peer.fmt_short(),
                        "chat: peer joined, requesting history"
                    );
                    // Request history since our latest message
                    let since = store
                        .latest_timestamp(&channel)
                        .unwrap_or(None)
                        .unwrap_or(0);
                    let request = ChatGossipMessage::HistoryRequest {
                        channel: channel.clone(),
                        since_timestamp: since,
                        nonce: rand::random(),
                    };
                    if let Ok(req_bytes) = serde_json::to_vec(&request) {
                        let payload = maybe_seal(&req_bytes, key.as_ref()).unwrap_or(req_bytes);
                        let _ = sender.broadcast(bytes::Bytes::from(payload)).await;
                    }
                }
                _ => {}
            }
        }
    });

    Ok(())
}

/// Persistent chat gossip listener — subscribes to all joined channels,
/// receives incoming messages, and handles history sync on peer join.
async fn spawn_chat_sync_loop(
    gossip: iroh_gossip::Gossip,
    chat_store: std::sync::Arc<bolo_chat::ChatStore>,
    _node_id: String,
    mesh_secret: Option<[u8; 32]>,
) -> Result<()> {
    let channels = chat_store.list_channels().unwrap_or_default();
    if channels.is_empty() {
        return Ok(());
    }

    for channel in channels {
        spawn_chat_channel_listener(
            gossip.clone(),
            chat_store.clone(),
            channel,
            mesh_secret,
            vec![],
        )
        .await?;
    }

    Ok(())
}

/// When a deploy message arrives:
/// 1. Fetch the blob (binary) from the sender
/// 2. Stage it to a temp path and verify it's executable
/// 3. Write a shell script that replaces the binary and restarts the systemd service
/// 4. Execute the script and exit the daemon
#[allow(clippy::too_many_arguments)]
async fn spawn_deploy_listener(
    gossip: iroh_gossip::Gossip,
    store: std::sync::Arc<bolo_blobs::FsStore>,
    endpoint: iroh::Endpoint,
    _node_id: String,
    _config_dir: std::path::PathBuf,
    data_dir: std::path::PathBuf,
    mesh_secret: Option<[u8; 32]>,
    known_peers: std::sync::Arc<tokio::sync::Mutex<Vec<iroh::PublicKey>>>,
) -> Result<()> {
    use futures_lite::StreamExt;

    let topic_str = "bolo/deploy";
    let topic_id = iroh_gossip::TopicId::from_bytes(bolo_core::TopicId::from_name(topic_str).0);

    // Use known peers as bootstrap so we can receive deploy messages
    let bootstrap = known_peers.lock().await.clone();
    let topic_handle = gossip
        .subscribe(topic_id, bootstrap)
        .await
        .context("failed to subscribe to deploy topic")?;

    let (_sender, mut receiver) = topic_handle.split();

    tokio::spawn(async move {
        let key = mesh_secret
            .as_ref()
            .map(|s| derive_gossip_key(s, topic_str));

        while let Some(Ok(event)) = receiver.next().await {
            let payload = match &event {
                iroh_gossip::api::Event::Received(msg) => &msg.content[..],
                _ => continue,
            };

            let decrypted = maybe_open(payload, key.as_ref());
            let msg_str = match std::str::from_utf8(&decrypted) {
                Ok(s) => s,
                Err(_) => continue,
            };

            let msg: serde_json::Value = match serde_json::from_str(msg_str) {
                Ok(v) => v,
                Err(_) => continue,
            };

            // Only process deploy messages
            if msg.get("type").and_then(|v| v.as_str()) != Some("deploy") {
                continue;
            }

            let blob_hash = match msg.get("blob_hash").and_then(|v| v.as_str()) {
                Some(h) => h.to_string(),
                None => continue,
            };
            let sender = msg
                .get("sender")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown");
            let target = msg
                .get("target")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown");

            tracing::info!(
                sender = sender,
                blob_hash = %blob_hash,
                target = target,
                "[deploy] Received deploy message"
            );

            // Check if this is for our platform
            let our_platform = format!("{}/{}", std::env::consts::OS, std::env::consts::ARCH);
            let target_platform = target
                .replace("aarch64-unknown-linux-gnu", "linux/aarch64")
                .replace("x86_64-unknown-linux-gnu", "linux/x86_64")
                .replace("aarch64-apple-darwin", "macos/aarch64")
                .replace("x86_64-apple-darwin", "macos/x86_64");
            if target_platform != our_platform && target != our_platform {
                tracing::info!(
                    target = target,
                    ours = our_platform,
                    "[deploy] Skipping — platform mismatch"
                );
                continue;
            }

            // Fetch the blob
            let parsed_hash: iroh_blobs::Hash = match blob_hash.parse() {
                Ok(h) => h,
                Err(_) => {
                    tracing::warn!(hash = %blob_hash, "[deploy] Invalid blob hash");
                    continue;
                }
            };

            tracing::info!("[deploy] Fetching binary blob...");

            // Try local first, then download from peers
            let bytes = match store.get_bytes(parsed_hash).await {
                Ok(b) => b,
                Err(_) => {
                    // Try to download from the sender
                    let downloader = store.downloader(&endpoint);
                    if let Ok(sender_key) = sender.parse::<iroh::PublicKey>() {
                        match downloader.download(parsed_hash, vec![sender_key]).await {
                            Ok(_) => match store.get_bytes(parsed_hash).await {
                                Ok(b) => b,
                                Err(e) => {
                                    tracing::error!(
                                        error = %e,
                                        "[deploy] Blob fetch ok but read failed"
                                    );
                                    continue;
                                }
                            },
                            Err(e) => {
                                tracing::error!(error = %e, "[deploy] Failed to fetch blob");
                                continue;
                            }
                        }
                    } else {
                        tracing::error!("[deploy] Cannot parse sender as peer key");
                        continue;
                    }
                }
            };

            tracing::info!(size = bytes.len(), "[deploy] Binary fetched, staging...");

            // Stage the binary
            let staged_path = data_dir.join("deploy-staged-binary");
            if let Err(e) = std::fs::write(&staged_path, &bytes) {
                tracing::error!(error = %e, "[deploy] Failed to write staged binary");
                continue;
            }

            // Set executable
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                if let Err(e) =
                    std::fs::set_permissions(&staged_path, std::fs::Permissions::from_mode(0o755))
                {
                    tracing::error!(error = %e, "[deploy] Failed to set permissions");
                    continue;
                }
            }

            // Determine where to install
            let current_exe = match std::env::current_exe() {
                Ok(p) => p,
                Err(e) => {
                    tracing::error!(error = %e, "[deploy] Cannot resolve current binary");
                    continue;
                }
            };

            // Replace the binary in-place (on Linux, the running binary's inode is
            // held open so we can safely overwrite the path).
            tracing::info!(
                binary = %current_exe.display(),
                "[deploy] Replacing binary and exiting for restart"
            );

            // On Linux, can't overwrite a running binary ("Text file busy").
            // Remove the file first (kernel keeps the inode until process exits),
            // then write the new binary to the same path.
            std::fs::remove_file(&current_exe).ok();
            if let Err(e) = std::fs::copy(&staged_path, &current_exe) {
                tracing::error!(error = %e, "[deploy] Failed to replace binary");
                continue;
            }
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(&current_exe, std::fs::Permissions::from_mode(0o755)).ok();
            }

            // Clean up staged binary
            std::fs::remove_file(&staged_path).ok();

            tracing::info!("[deploy] Binary replaced. Exiting — systemd will restart.");

            // Exit with code 1 so systemd's Restart=on-failure kicks in
            std::process::exit(1);
        }
    });

    Ok(())
}

// --- Bench ---

const BENCH_TOPIC: &str = "bolo/bench";

fn bench_topic_id() -> iroh_gossip::TopicId {
    iroh_gossip::TopicId::from_bytes(bolo_core::TopicId::from_name(BENCH_TOPIC).0)
}

/// IPC handler for `peer.bench` — creates a test blob and asks the peer to fetch it.
async fn ipc_peer_bench(
    ctx: &IpcContext,
    params: &serde_json::Value,
) -> Result<serde_json::Value, String> {
    use futures_lite::StreamExt;

    let node_id_str = params
        .get("node_id")
        .and_then(|v| v.as_str())
        .ok_or("missing 'node_id' param")?;
    let size_mb = params.get("size_mb").and_then(|v| v.as_u64()).unwrap_or(10);

    let remote: iroh::PublicKey = node_id_str.parse().map_err(|_| "invalid node ID")?;

    // Generate random test data
    let size_bytes = size_mb * 1_048_576;
    let mut data = vec![0u8; size_bytes as usize];
    // Use a simple PRNG seeded with current time for unique blobs each run
    let mut seed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    for byte in data.iter_mut() {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        *byte = (seed >> 33) as u8;
    }

    // Write to temp file and add as blob
    let tmp = std::env::temp_dir().join(format!("bolo-bench-{}.bin", std::process::id()));
    std::fs::write(&tmp, &data).map_err(|e| format!("failed to write test data: {e}"))?;

    let tag = ctx
        .store
        .add_path(&tmp)
        .with_tag()
        .await
        .map_err(|e| format!("failed to add bench blob: {e}"))?;
    let blob_hash = tag.hash.to_string();

    // Clean up temp file
    let _ = std::fs::remove_file(&tmp);

    // Subscribe to bench topic with the target peer
    let mut bootstrap: Vec<iroh::PublicKey> = ctx.known_peers.lock().await.clone();
    if !bootstrap.contains(&remote) {
        bootstrap.push(remote);
    }

    let topic_handle = ctx
        .gossip
        .subscribe(bench_topic_id(), bootstrap)
        .await
        .map_err(|e| format!("failed to subscribe to bench topic: {e}"))?;

    let (sender, mut receiver) = topic_handle.split();

    // Wait for neighbor before sending
    let neighbor_up = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while let Some(Ok(event)) = receiver.next().await {
            if matches!(event, iroh_gossip::api::Event::NeighborUp(_)) {
                return true;
            }
        }
        false
    })
    .await;

    if neighbor_up != Ok(true) {
        return Err("no peers joined bench topic within 10s".into());
    }

    // Send bench request
    let request = serde_json::json!({
        "type": "bench_request",
        "blob_hash": blob_hash,
        "size_bytes": size_bytes,
        "sender": ctx.node_id,
    });

    let key = ctx.gossip_key(BENCH_TOPIC);
    let payload = maybe_seal(request.to_string().as_bytes(), key.as_ref())
        .map_err(|e| format!("encryption failed: {e}"))?;

    sender
        .broadcast(bytes::Bytes::from(payload))
        .await
        .map_err(|e| format!("failed to send bench request: {e}"))?;

    // Wait for bench result (timeout 60s for large payloads)
    let result = tokio::time::timeout(std::time::Duration::from_secs(60), async {
        while let Some(Ok(event)) = receiver.next().await {
            let payload = match &event {
                iroh_gossip::api::Event::Received(msg) => &msg.content[..],
                _ => continue,
            };

            let decrypted = maybe_open(payload, key.as_ref());
            let msg_str = match std::str::from_utf8(&decrypted) {
                Ok(s) => s,
                Err(_) => continue,
            };

            let msg: serde_json::Value = match serde_json::from_str(msg_str) {
                Ok(v) => v,
                Err(_) => continue,
            };

            if msg.get("type").and_then(|v| v.as_str()) == Some("bench_result") {
                return Some(msg);
            }
        }
        None
    })
    .await;

    match result {
        Ok(Some(msg)) => Ok(msg),
        Ok(None) => Err("bench topic closed without result".into()),
        Err(_) => Err("bench timed out waiting for result (60s)".into()),
    }
}

/// Background listener that responds to bench requests from peers.
async fn spawn_bench_listener(
    gossip: iroh_gossip::Gossip,
    store: std::sync::Arc<bolo_blobs::FsStore>,
    endpoint: iroh::Endpoint,
    node_id: String,
    mesh_secret: Option<[u8; 32]>,
    known_peers: std::sync::Arc<tokio::sync::Mutex<Vec<iroh::PublicKey>>>,
) -> Result<()> {
    use futures_lite::StreamExt;

    let bootstrap = known_peers.lock().await.clone();
    let topic_handle = gossip
        .subscribe(bench_topic_id(), bootstrap)
        .await
        .context("failed to subscribe to bench topic")?;

    let (sender, mut receiver) = topic_handle.split();

    tokio::spawn(async move {
        let key = mesh_secret
            .as_ref()
            .map(|s| derive_gossip_key(s, BENCH_TOPIC));

        while let Some(Ok(event)) = receiver.next().await {
            let payload = match &event {
                iroh_gossip::api::Event::Received(msg) => &msg.content[..],
                _ => continue,
            };

            let decrypted = maybe_open(payload, key.as_ref());
            let msg_str = match std::str::from_utf8(&decrypted) {
                Ok(s) => s,
                Err(_) => continue,
            };

            let msg: serde_json::Value = match serde_json::from_str(msg_str) {
                Ok(v) => v,
                Err(_) => continue,
            };

            if msg.get("type").and_then(|v| v.as_str()) != Some("bench_request") {
                continue;
            }

            let blob_hash_str = match msg.get("blob_hash").and_then(|v| v.as_str()) {
                Some(h) => h.to_string(),
                None => continue,
            };
            let size_bytes = msg.get("size_bytes").and_then(|v| v.as_u64()).unwrap_or(0);
            let requester = msg
                .get("sender")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown")
                .to_string();

            tracing::info!(
                sender = %requester,
                blob_hash = %blob_hash_str,
                size = size_bytes,
                "[bench] Received bench request"
            );

            let parsed_hash: iroh_blobs::Hash = match blob_hash_str.parse() {
                Ok(h) => h,
                Err(_) => continue,
            };

            // Parse sender key for blob download
            let sender_key: iroh::PublicKey = match requester.parse() {
                Ok(k) => k,
                Err(_) => {
                    tracing::warn!("[bench] Invalid sender key: {requester}");
                    continue;
                }
            };

            // Fetch the blob from the sender and measure time
            let start = std::time::Instant::now();
            let downloader = store.downloader(&endpoint);
            let fetch_result = downloader.download(parsed_hash, vec![sender_key]).await;
            let duration = start.elapsed();
            let duration_ms = duration.as_millis() as u64;

            if let Err(e) = fetch_result {
                tracing::warn!(error = %e, "[bench] Failed to fetch bench blob");
                // Send error result so initiator doesn't hang
                let err_result = serde_json::json!({
                    "type": "bench_result",
                    "node_id": node_id,
                    "error": format!("blob fetch failed: {e}"),
                    "size_bytes": size_bytes,
                    "duration_ms": duration_ms,
                    "throughput_mbps": 0.0,
                });
                let err_payload = maybe_seal(err_result.to_string().as_bytes(), key.as_ref())
                    .unwrap_or_else(|_| err_result.to_string().into_bytes());
                let _ = sender.broadcast(bytes::Bytes::from(err_payload)).await;
                continue;
            }

            let throughput_mbps = if duration_ms > 0 {
                (size_bytes as f64 / 1_048_576.0) / (duration_ms as f64 / 1000.0)
            } else {
                0.0
            };

            tracing::info!(
                duration_ms = duration_ms,
                throughput = format!("{throughput_mbps:.2} MB/s"),
                "[bench] Bench complete"
            );

            // Send result back
            let result = serde_json::json!({
                "type": "bench_result",
                "node_id": node_id,
                "blob_hash": blob_hash_str,
                "size_bytes": size_bytes,
                "duration_ms": duration_ms,
                "throughput_mbps": (throughput_mbps * 100.0).round() / 100.0,
            });

            let result_payload = maybe_seal(result.to_string().as_bytes(), key.as_ref())
                .unwrap_or_else(|_| result.to_string().into_bytes());

            if let Err(e) = sender.broadcast(bytes::Bytes::from(result_payload)).await {
                tracing::warn!(error = %e, "[bench] Failed to send bench result");
            }
        }
    });

    Ok(())
}

// --- Mesh status ---

const MESH_STATUS_TOPIC: &str = "bolo/mesh-status";

fn mesh_status_topic_id() -> iroh_gossip::TopicId {
    iroh_gossip::TopicId::from_bytes(bolo_core::TopicId::from_name(MESH_STATUS_TOPIC).0)
}

/// IPC handler for `mesh.status` — discover local + peer capabilities via gossip.
///
/// Uses `subscribe_and_join` to create a fresh subscription that waits for confirmed
/// peer connections, then broadcasts the request and collects responses. Also subscribes
/// to the background listener's broadcast channel for responses that arrive there.
async fn ipc_mesh_status(
    ctx: &IpcContext,
    params: &serde_json::Value,
) -> Result<serde_json::Value, String> {
    let timeout_secs = params
        .get("timeout_secs")
        .and_then(|v| v.as_u64())
        .unwrap_or(5);

    let local = bolo_core::NodeCapabilities::discover(&ctx.node_id, VERSION);
    let mut nodes = vec![local];

    let peers = ctx.known_peers.lock().await.clone();
    let peer_count = peers.len() + 1;

    if peers.is_empty() {
        let mesh = bolo_core::MeshCapabilities::aggregate(nodes, peer_count);
        return serde_json::to_value(&mesh).map_err(|e| e.to_string());
    }

    // Create a fresh subscription for this query. This opens new QUIC connections
    // to all known peers. When dropped, the topic stays alive (background listener +
    // per-peer subscriptions keep still_needed() true) so connections aren't torn down.
    let topic_handle = ctx
        .gossip
        .subscribe(mesh_status_topic_id(), peers.clone())
        .await
        .map_err(|e| format!("failed to subscribe to mesh-status topic: {e}"))?;
    let (query_sender, _query_receiver) = topic_handle.split();

    // Subscribe to the response broadcast channel BEFORE sending
    let mut response_rx = ctx.mesh_response_tx.subscribe();

    let key = ctx.gossip_key(MESH_STATUS_TOPIC);
    let nonce: u64 = rand::random();
    let request = serde_json::json!({
        "type": "capabilities_request",
        "sender": ctx.node_id,
        "nonce": nonce,
    });
    let req_bytes = maybe_seal(request.to_string().as_bytes(), key.as_ref())
        .map_err(|e| format!("encryption failed: {e}"))?;

    query_sender
        .broadcast(bytes::Bytes::from(req_bytes.clone()))
        .await
        .map_err(|e| format!("failed to broadcast capabilities request: {e}"))?;

    let expected_peers = peer_count - 1;

    // Collect responses from the background listener's broadcast channel.
    // Responses arrive there because iroh delivers messages to ALL subscribers.
    let _ = tokio::time::timeout(std::time::Duration::from_secs(timeout_secs), async {
        let mut next_retry = tokio::time::Instant::now() + std::time::Duration::from_secs(2);

        loop {
            tokio::select! {
                result = response_rx.recv() => {
                    match result {
                        Ok(node_caps) => {
                            if !nodes.iter().any(|n| n.node_id == node_caps.node_id) {
                                nodes.push(node_caps);
                            }
                            if nodes.len() > expected_peers {
                                break;
                            }
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                    }
                }
                _ = tokio::time::sleep_until(next_retry) => {
                    // Use fresh nonce so PlumTree doesn't deduplicate the retry
                    let retry_nonce: u64 = rand::random();
                    let retry_req = serde_json::json!({
                        "type": "capabilities_request",
                        "sender": ctx.node_id,
                        "nonce": retry_nonce,
                    });
                    if let Ok(retry_bytes) = maybe_seal(
                        retry_req.to_string().as_bytes(), key.as_ref()
                    ) {
                        let _ = query_sender
                            .broadcast(bytes::Bytes::from(retry_bytes))
                            .await;
                    }
                    next_retry =
                        tokio::time::Instant::now() + std::time::Duration::from_secs(2);
                }
            }
        }
    })
    .await;

    let mesh = bolo_core::MeshCapabilities::aggregate(nodes, peer_count);
    serde_json::to_value(&mesh).map_err(|e| e.to_string())
}

/// Background listener that responds to incoming mesh-status capability requests
/// and provides a shared sender for `ipc_mesh_status` to broadcast through.
/// Uses the persistent subscription so gossip connections stay alive between queries.
async fn spawn_mesh_status_listener(
    gossip: iroh_gossip::Gossip,
    node_id: String,
    mesh_secret: Option<[u8; 32]>,
    known_peers: std::sync::Arc<tokio::sync::Mutex<Vec<iroh::PublicKey>>>,
    shared_sender: std::sync::Arc<tokio::sync::Mutex<Option<iroh_gossip::api::GossipSender>>>,
    response_tx: tokio::sync::broadcast::Sender<bolo_core::capabilities::NodeCapabilities>,
) -> Result<()> {
    tokio::spawn(async move {
        let key = mesh_secret
            .as_ref()
            .map(|s| derive_gossip_key(s, MESH_STATUS_TOPIC));

        loop {
            let bootstrap = known_peers.lock().await.clone();
            // Use subscribe (not subscribe_and_join) since bootstrap may be empty
            // or peers may not be reachable yet. Per-peer subscriptions from the
            // reconnection code establish the actual connections.
            let topic_handle = match gossip.subscribe(mesh_status_topic_id(), bootstrap).await {
                Ok(h) => h,
                Err(e) => {
                    tracing::warn!(error = %e, "[mesh-status] Failed to subscribe, retrying in 2s");
                    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                    continue;
                }
            };

            use futures_lite::StreamExt;
            let (sender, mut receiver) = topic_handle.split();

            *shared_sender.lock().await = Some(sender.clone());
            tracing::info!("[mesh-status] Listener subscribed and sender ready");

            while let Some(event) = receiver.next().await {
                let event = match event {
                    Ok(e) => e,
                    Err(e) => {
                        tracing::warn!(error = %e, "[mesh-status] Stream error, reconnecting");
                        break;
                    }
                };
                let payload = match &event {
                    iroh_gossip::api::Event::Received(msg) => &msg.content[..],
                    _ => continue,
                };

                let decrypted = maybe_open(payload, key.as_ref());
                let msg_str = match std::str::from_utf8(&decrypted) {
                    Ok(s) => s,
                    Err(_) => continue,
                };
                let msg: serde_json::Value = match serde_json::from_str(msg_str) {
                    Ok(v) => v,
                    Err(_) => continue,
                };

                match msg.get("type").and_then(|v| v.as_str()) {
                    Some("capabilities_request") => {
                        tracing::info!(
                            "[mesh-status] Received capabilities request, responding..."
                        );
                        let caps = bolo_core::NodeCapabilities::discover(&node_id, VERSION);
                        let resp_nonce: u64 = rand::random();
                        let response = serde_json::json!({
                            "type": "capabilities_response",
                            "capabilities": caps,
                            "nonce": resp_nonce,
                        });
                        let resp_payload =
                            maybe_seal(response.to_string().as_bytes(), key.as_ref())
                                .unwrap_or_else(|_| response.to_string().into_bytes());
                        if let Err(e) = sender.broadcast(bytes::Bytes::from(resp_payload)).await {
                            tracing::warn!(error = %e, "[mesh-status] Failed to send response");
                        }
                    }
                    Some("capabilities_response") => {
                        if let Some(caps) = msg.get("capabilities") {
                            if let Ok(node_caps) = serde_json::from_value::<
                                bolo_core::capabilities::NodeCapabilities,
                            >(caps.clone())
                            {
                                let _ = response_tx.send(node_caps);
                            }
                        }
                    }
                    _ => {}
                }
            }

            *shared_sender.lock().await = None;
            tracing::warn!("[mesh-status] Stream ended, reconnecting in 2s");
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        }
    });

    Ok(())
}

// --- Logs ---

const LOGS_TOPIC: &str = "bolo/logs";
const MAX_LOG_LINES: usize = 500;

fn logs_topic_id() -> iroh_gossip::TopicId {
    iroh_gossip::TopicId::from_bytes(bolo_core::TopicId::from_name(LOGS_TOPIC).0)
}

/// IPC handler for `daemon.logs` — read the local log file.
async fn ipc_daemon_logs(
    ctx: &IpcContext,
    params: &serde_json::Value,
) -> Result<serde_json::Value, String> {
    let lines = params.get("lines").and_then(|v| v.as_u64()).unwrap_or(100) as usize;
    let lines = lines.min(MAX_LOG_LINES);

    let data_dir = resolve_data_dir(&ctx.config_dir);
    let log_path = data_dir.join("daemon.log");

    if !log_path.exists() {
        return Ok(serde_json::json!({
            "node_id": ctx.node_id,
            "lines": [],
            "error": "no log file found",
        }));
    }

    let log_lines = tail_file(&log_path, lines).map_err(|e| e.to_string())?;
    Ok(serde_json::json!({
        "node_id": ctx.node_id,
        "lines": log_lines,
        "count": log_lines.len(),
    }))
}

/// IPC handler for `peer.logs` — request logs from a remote peer via gossip.
async fn ipc_peer_logs(
    ctx: &IpcContext,
    params: &serde_json::Value,
) -> Result<serde_json::Value, String> {
    use futures_lite::StreamExt;

    let node_id_str = params
        .get("node_id")
        .and_then(|v| v.as_str())
        .ok_or("missing 'node_id' param")?;
    let lines = params.get("lines").and_then(|v| v.as_u64()).unwrap_or(100) as usize;
    let lines = lines.min(MAX_LOG_LINES);

    let mut bootstrap: Vec<iroh::PublicKey> = ctx.known_peers.lock().await.clone();
    let remote: iroh::PublicKey = node_id_str.parse().map_err(|_| "invalid node ID")?;
    if !bootstrap.contains(&remote) {
        bootstrap.push(remote);
    }

    let topic = ctx
        .gossip
        .subscribe_and_join(logs_topic_id(), bootstrap)
        .await
        .map_err(|e| format!("failed to join logs topic: {e}"))?;

    let (sender, mut receiver) = topic.split();
    let key = ctx.gossip_key(LOGS_TOPIC);

    let request = serde_json::json!({
        "type": "logs_request",
        "sender": ctx.node_id,
        "target": node_id_str,
        "lines": lines,
    });
    let payload = maybe_seal(request.to_string().as_bytes(), key.as_ref())
        .map_err(|e| format!("encryption failed: {e}"))?;
    sender
        .broadcast(bytes::Bytes::from(payload))
        .await
        .map_err(|e| format!("failed to broadcast logs request: {e}"))?;

    let result = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while let Some(Ok(event)) = receiver.next().await {
            let payload = match &event {
                iroh_gossip::api::Event::Received(msg) => &msg.content[..],
                _ => continue,
            };

            let decrypted = maybe_open(payload, key.as_ref());
            let msg_str = match std::str::from_utf8(&decrypted) {
                Ok(s) => s,
                Err(_) => continue,
            };

            let msg: serde_json::Value = match serde_json::from_str(msg_str) {
                Ok(v) => v,
                Err(_) => continue,
            };

            if msg.get("type").and_then(|v| v.as_str()) == Some("logs_response")
                && msg.get("node_id").and_then(|v| v.as_str()) == Some(node_id_str)
            {
                return Some(msg);
            }
        }
        None
    })
    .await;

    match result {
        Ok(Some(msg)) => Ok(msg),
        Ok(None) => Err("logs topic closed without response".into()),
        Err(_) => Err("timed out waiting for logs response (10s)".into()),
    }
}

/// Background listener that responds to log requests from peers
/// and handles IPC-initiated log queries. Reconnects on gossip errors.
async fn spawn_logs_listener(
    gossip: iroh_gossip::Gossip,
    node_id: String,
    mesh_secret: Option<[u8; 32]>,
    known_peers: std::sync::Arc<tokio::sync::Mutex<Vec<iroh::PublicKey>>>,
    config_dir: std::path::PathBuf,
) -> Result<()> {
    tokio::spawn(async move {
        let key = mesh_secret
            .as_ref()
            .map(|s| derive_gossip_key(s, LOGS_TOPIC));

        loop {
            let bootstrap = known_peers.lock().await.clone();
            let topic_handle = match gossip.subscribe(logs_topic_id(), bootstrap).await {
                Ok(h) => h,
                Err(e) => {
                    tracing::warn!(error = %e, "[logs] Failed to subscribe, retrying in 2s");
                    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                    continue;
                }
            };

            use futures_lite::StreamExt;
            let (sender, mut receiver) = topic_handle.split();

            while let Some(event) = receiver.next().await {
                let event = match event {
                    Ok(e) => e,
                    Err(e) => {
                        tracing::warn!(error = %e, "[logs] Stream error, reconnecting");
                        break;
                    }
                };
                let payload = match &event {
                    iroh_gossip::api::Event::Received(msg) => &msg.content[..],
                    _ => continue,
                };

                let decrypted = maybe_open(payload, key.as_ref());
                let msg_str = match std::str::from_utf8(&decrypted) {
                    Ok(s) => s,
                    Err(_) => continue,
                };
                let msg: serde_json::Value = match serde_json::from_str(msg_str) {
                    Ok(v) => v,
                    Err(_) => continue,
                };

                if msg.get("type").and_then(|v| v.as_str()) != Some("logs_request") {
                    continue;
                }

                if let Some(target) = msg.get("target").and_then(|v| v.as_str()) {
                    if target != node_id {
                        continue;
                    }
                }

                let requested_lines =
                    msg.get("lines").and_then(|v| v.as_u64()).unwrap_or(100) as usize;
                let requested_lines = requested_lines.min(MAX_LOG_LINES);

                tracing::info!("[logs] Received log request, responding...");

                let data_dir = resolve_data_dir(&config_dir);
                let log_path = data_dir.join("daemon.log");

                let log_lines = if log_path.exists() {
                    tail_file(&log_path, requested_lines).unwrap_or_default()
                } else {
                    vec!["(no log file found)".to_string()]
                };

                let response = serde_json::json!({
                    "type": "logs_response",
                    "node_id": node_id,
                    "lines": log_lines,
                    "count": log_lines.len(),
                });

                let resp_payload = maybe_seal(response.to_string().as_bytes(), key.as_ref())
                    .unwrap_or_else(|_| response.to_string().into_bytes());

                if let Err(e) = sender.broadcast(bytes::Bytes::from(resp_payload)).await {
                    tracing::warn!(
                        error = %e,
                        "[logs] Failed to send logs response"
                    );
                }
            }

            tracing::warn!("[logs] Stream ended, reconnecting in 2s");
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        }
    });

    Ok(())
}

fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = 1024 * KB;
    const GB: u64 = 1024 * MB;
    if bytes >= GB {
        format!("{:.1} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.1} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.1} KB", bytes as f64 / KB as f64)
    } else {
        format!("{bytes} B")
    }
}

fn gather_storage_stats(data_dir: &std::path::Path) -> Option<serde_json::Value> {
    let blobs_dir = data_dir.join("blobs");
    let blobs_bytes = dir_size(&blobs_dir).unwrap_or(0);

    let doc_count = bolo_docs::DocStore::open(data_dir)
        .ok()
        .and_then(|s| s.count().ok())
        .unwrap_or(0);

    let chat_channels = bolo_chat::ChatStore::open(data_dir)
        .ok()
        .and_then(|s| s.list_channels().ok())
        .map(|c| c.len())
        .unwrap_or(0);

    let ci_tasks = bolo_ci::CiStore::open(data_dir)
        .ok()
        .and_then(|s| s.list_tasks().ok())
        .map(|t| t.len())
        .unwrap_or(0);

    Some(serde_json::json!({
        "blobs_bytes": blobs_bytes,
        "blobs_human": format_bytes(blobs_bytes),
        "doc_count": doc_count,
        "chat_channels": chat_channels,
        "ci_tasks": ci_tasks,
    }))
}

fn dir_size(path: &std::path::Path) -> std::io::Result<u64> {
    let mut total = 0u64;
    if !path.exists() {
        return Ok(0);
    }
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let meta = entry.metadata()?;
        if meta.is_dir() {
            total += dir_size(&entry.path())?;
        } else {
            total += meta.len();
        }
    }
    Ok(total)
}

fn format_uptime(secs: u64) -> String {
    if secs < 60 {
        format!("{secs}s")
    } else if secs < 3600 {
        format!("{}m {}s", secs / 60, secs % 60)
    } else if secs < 86400 {
        format!("{}h {}m", secs / 3600, (secs % 3600) / 60)
    } else {
        format!("{}d {}h", secs / 86400, (secs % 86400) / 3600)
    }
}
