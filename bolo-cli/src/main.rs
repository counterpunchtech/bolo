#![deny(unsafe_code)]

//! Bolo CLI — P2P mesh platform.

mod cmd;
mod gc;
mod quality;

use clap::{Parser, Subcommand};

/// Bolo: a P2P mesh platform.
#[derive(Parser)]
#[command(name = "bolo", version = env!("BOLO_BUILD_VERSION"), about)]
struct Cli {
    /// Output as JSON
    #[arg(long, global = true)]
    json: bool,

    /// Suppress non-essential output
    #[arg(long, global = true)]
    quiet: bool,

    /// Increase log verbosity
    #[arg(long, global = true)]
    verbose: bool,

    /// Path to config file
    #[arg(long, global = true)]
    config: Option<String>,

    /// Timeout in seconds for operations
    #[arg(long, global = true)]
    timeout: Option<u64>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Daemon lifecycle management
    Daemon {
        #[command(subcommand)]
        command: DaemonCmd,
    },
    /// Identity and key management
    Id {
        #[command(subcommand)]
        command: IdCmd,
    },
    /// Peer connections and discovery
    Peer {
        #[command(subcommand)]
        command: PeerCmd,
    },
    /// Relay server management
    Relay {
        #[command(subcommand)]
        command: RelayCmd,
    },
    /// Content-addressed blob storage
    Blob {
        #[command(subcommand)]
        command: BlobCmd,
    },
    /// CRDT document operations
    Doc {
        #[command(subcommand)]
        command: DocCmd,
    },
    /// Pub/sub messaging
    Pub {
        #[command(subcommand)]
        command: PubCmd,
    },
    /// Git mesh sync
    Git {
        #[command(subcommand)]
        command: GitCmd,
    },
    /// Code review
    Review {
        #[command(subcommand)]
        command: ReviewCmd,
    },
    /// Distributed CI/CD
    Ci {
        #[command(subcommand)]
        command: CiCmd,
    },
    /// Team chat
    Chat {
        #[command(subcommand)]
        command: ChatCmd,
    },
    /// Task management
    Task {
        #[command(subcommand)]
        command: TaskCmd,
    },
    /// Hot deploy binaries to mesh peers
    Deploy {
        #[command(subcommand)]
        command: DeployCmd,
    },
    /// Mesh-wide status and capability discovery
    Mesh {
        #[command(subcommand)]
        command: MeshCmd,
    },
    /// MCP server for LLM tool access
    Mcp {
        #[command(subcommand)]
        command: McpCmd,
    },
    /// Stream logs from local daemon, a specific peer, or the entire mesh
    Logs {
        /// Follow (tail) mode — stream new log entries in real time
        #[arg(short, long)]
        follow: bool,
        /// Number of lines to show initially (default: 50)
        #[arg(short = 'n', long, default_value = "50")]
        lines: usize,
        /// Stream logs from a specific peer (node ID or prefix)
        #[arg(long)]
        peer: Option<String>,
        /// Stream logs from all mesh peers
        #[arg(long)]
        mesh: bool,
    },
    /// Run project quality checks
    Quality {
        /// Run fast checks only (fmt + clippy + machete)
        #[arg(long)]
        fast: bool,
        /// Run full checks (fast + test + deny + audit + doc)
        #[arg(long)]
        full: bool,
        /// Auto-fix issues where possible
        #[arg(long)]
        fix: bool,
    },
}

// --- Daemon subcommands ---

#[derive(Subcommand)]
enum DaemonCmd {
    /// Create key pair and config directory
    Init,
    /// Start the daemon
    Start {
        /// Run in background (detach from terminal)
        #[arg(long)]
        detach: bool,
    },
    /// Stop the daemon
    Stop,
    /// Show node status
    Status,
    /// Export full node state
    Export {
        /// Output path
        path: String,
    },
    /// Import node state
    Import {
        /// Input path
        path: String,
        /// Overwrite existing state
        #[arg(long)]
        force: bool,
    },
    /// Install as system service (launchd on macOS, systemd on Linux)
    Install,
    /// Remove system service
    Uninstall,
    /// Upgrade bolo binary from the mesh
    Upgrade {
        /// Target platform (default: auto-detect)
        #[arg(long)]
        platform: Option<String>,
    },
    /// View daemon logs
    Logs {
        /// Number of lines to show (default: 100)
        #[arg(short = 'n', long, default_value = "100")]
        lines: usize,
    },
}

// --- Id subcommands ---

#[derive(Subcommand)]
enum IdCmd {
    /// Print node ID
    Show,
    /// Export key pair to file
    Export,
    /// Import key pair from file
    Import {
        /// Key file path
        file: String,
    },
    /// Sign arbitrary data
    Sign {
        /// Data to sign
        data: String,
    },
    /// Verify a signature
    Verify {
        /// Data that was signed
        data: String,
        /// Signature
        sig: String,
        /// Peer node ID
        peer: String,
    },
}

// --- Peer subcommands ---

#[derive(Subcommand)]
enum PeerCmd {
    /// Connect to a peer
    Add {
        /// Node ID to connect to
        node_id: String,
    },
    /// Disconnect from a peer
    Rm {
        /// Node ID to disconnect
        node_id: String,
    },
    /// List connected peers
    Ls,
    /// Find peers by topic
    Discover {
        /// Topic name
        topic: String,
    },
    /// Measure latency to a peer
    Ping {
        /// Node ID to ping
        node_id: String,
    },
    /// Measure throughput to a peer
    Bench {
        /// Node ID to benchmark
        node_id: String,
        /// Test payload size in MB (default: 10)
        #[arg(long, default_value = "10")]
        size: u64,
    },
    /// View logs from a remote peer
    Logs {
        /// Node ID of the peer
        node_id: String,
        /// Number of lines to show (default: 100)
        #[arg(short = 'n', long, default_value = "100")]
        lines: usize,
    },
    /// Add peer to trusted set
    Trust {
        /// Node ID to trust
        node_id: String,
    },
    /// Remove peer from trusted set
    Untrust {
        /// Node ID to untrust
        node_id: String,
    },
}

// --- Relay subcommands ---

#[derive(Subcommand)]
enum RelayCmd {
    /// Run relay server
    Start,
    /// Stop relay server
    Stop,
    /// Show relay stats
    Status,
    /// List configured relays
    Ls,
    /// Add a relay
    Add {
        /// Relay URL
        url: String,
    },
    /// Remove a relay
    Rm {
        /// Relay URL
        url: String,
    },
    /// Measure latency to all relays
    Ping,
    /// Find community relays
    Discover,
}

// --- Blob subcommands ---

#[derive(Subcommand)]
enum BlobCmd {
    /// Store a file, return hash
    Put {
        /// File path or - for stdin
        file: String,
    },
    /// Retrieve a blob
    Get {
        /// Blob hash
        hash: String,
        /// Output path
        path: Option<String>,
    },
    /// List locally stored blobs
    Ls,
    /// Show blob metadata
    Stat {
        /// Blob hash
        hash: String,
    },
    /// Pin a blob (prevent GC)
    Pin {
        /// Blob hash
        hash: String,
    },
    /// Unpin a blob
    Unpin {
        /// Blob hash
        hash: String,
    },
    /// Fetch a blob from a remote peer
    Fetch {
        /// Blob hash
        hash: String,
        /// Remote peer node ID
        peer: String,
        /// Output path (optional, writes to stdout if omitted)
        path: Option<String>,
    },
    /// Garbage collect unpinned blobs
    Gc,
    /// Migrate to encrypt all stored blobs
    EncryptStore,
}

// --- Doc subcommands ---

#[derive(Subcommand)]
enum DocCmd {
    /// Create a CRDT document
    Create {
        /// Document path
        path: String,
    },
    /// Delete a document
    Rm {
        /// Document path
        path: String,
    },
    /// List documents
    Ls {
        /// Optional path prefix filter
        prefix: Option<String>,
    },
    /// Read a document or key
    Get {
        /// Document path
        path: String,
        /// Optional key
        key: Option<String>,
    },
    /// Set a key in a map document
    Set {
        /// Document path
        path: String,
        /// Key
        key: String,
        /// Value
        value: String,
    },
    /// Delete a key from a document
    Del {
        /// Document path
        path: String,
        /// Key
        key: String,
    },
    /// Open document in $EDITOR
    Edit {
        /// Document path
        path: String,
    },
    /// Render document to terminal
    Read {
        /// Document path
        path: String,
    },
    /// Append value to a list document
    Append {
        /// Document path
        path: String,
        /// Value to append
        value: String,
    },
    /// Stream document changes
    Watch {
        /// Document path
        path: String,
    },
    /// Show local vs mesh diff
    Diff {
        /// Document path
        path: String,
    },
    /// Show change history
    History {
        /// Document path
        path: String,
    },
    /// Force sync a document
    Sync {
        /// Optional document path (sync all if omitted)
        path: Option<String>,
    },
    /// Export document to file
    Export {
        /// Document path
        path: String,
        /// Output file
        file: String,
    },
    /// Import document from file
    Import {
        /// Input file
        file: String,
        /// Document path
        path: String,
    },
    /// Generate a share ticket
    Share {
        /// Document path
        path: String,
    },
    /// Join a shared document
    Join {
        /// Share ticket
        ticket: String,
    },
    /// Revoke a share ticket
    Revoke {
        /// Ticket ID
        ticket_id: String,
    },
    /// List issued tickets for a document
    Tickets {
        /// Document path
        path: String,
    },
    /// Compact CRDT history
    Compact {
        /// Document path
        path: String,
        /// Compact all documents
        #[arg(long)]
        all: bool,
    },
    /// Access control management
    Acl {
        #[command(subcommand)]
        command: AclCmd,
    },
}

#[derive(Subcommand)]
enum AclCmd {
    /// Grant write access to a peer
    Grant {
        /// Document path
        path: String,
        /// Peer node ID
        peer_id: String,
    },
    /// Revoke access from a peer
    Revoke {
        /// Document path
        path: String,
        /// Peer node ID
        peer_id: String,
    },
    /// Show ACL for a document
    Show {
        /// Document path
        path: String,
    },
}

// --- Pub subcommands ---

#[derive(Subcommand)]
enum PubCmd {
    /// Publish a message to a topic
    Send {
        /// Topic name
        topic: String,
        /// Message or - for stdin
        message: String,
        /// Bootstrap peer node IDs
        #[arg(long = "peer", short = 'p')]
        peers: Vec<String>,
    },
    /// Subscribe to a topic and stream messages
    Sub {
        /// Topic name
        topic: String,
        /// Bootstrap peer node IDs
        #[arg(long = "peer", short = 'p')]
        peers: Vec<String>,
    },
    /// List active subscriptions
    Ls,
    /// List peers on a topic
    Peers {
        /// Topic name
        topic: String,
    },
    /// Unsubscribe from a topic
    Unsub {
        /// Topic name
        topic: String,
    },
}

// --- Git subcommands ---

#[derive(Subcommand)]
enum GitCmd {
    /// Show git repo sync state
    Status {
        /// Repository path (default: current directory)
        #[arg(long)]
        path: Option<String>,
    },
    /// List git objects in HEAD tree
    Objects {
        /// Repository path (default: current directory)
        #[arg(long)]
        path: Option<String>,
    },
    /// Show recent commits
    Log {
        /// Repository path (default: current directory)
        #[arg(long)]
        path: Option<String>,
        /// Number of commits to show
        #[arg(short = 'n', long, default_value = "10")]
        count: usize,
    },
    /// List all refs
    Refs {
        /// Repository path (default: current directory)
        #[arg(long)]
        path: Option<String>,
    },
    /// Sync local git objects to mesh
    Push {
        /// Repository path (default: current directory)
        #[arg(long)]
        path: Option<String>,
    },
    /// Pull git objects from mesh peers
    Pull {
        /// Repository path (default: current directory)
        #[arg(long)]
        path: Option<String>,
    },
    /// Clone a repo from the mesh swarm
    Clone {
        /// Peer/repo identifier (e.g. <peer-id>/<repo>)
        url: String,
        /// Destination path
        dest: Option<String>,
    },
}

// --- Review subcommands ---

#[derive(Subcommand)]
enum ReviewCmd {
    /// Show review comments for a commit
    Show {
        /// Commit OID
        commit: String,
    },
    /// Add a review comment
    Comment {
        /// Commit OID
        commit: String,
        /// Comment body
        #[arg(short, long)]
        message: String,
        /// Attach to a specific file
        #[arg(long)]
        file: Option<String>,
        /// Attach to a specific line
        #[arg(long)]
        line: Option<u32>,
    },
    /// Sign an approval
    Approve {
        /// Commit OID
        commit: String,
    },
    /// Sign a rejection
    Reject {
        /// Commit OID
        commit: String,
        /// Rejection reason
        #[arg(short, long)]
        message: Option<String>,
    },
    /// List pending reviews
    Ls,
}

// --- MCP subcommands ---

// --- CI subcommands ---

#[derive(Subcommand)]
enum CiCmd {
    /// Trigger a CI build
    Run {
        /// Task type: build, test, check, clippy, fmt, full (default: full)
        #[arg(long, short = 't')]
        task_type: Option<String>,
        /// Repository/workspace path
        #[arg(long)]
        path: Option<String>,
    },
    /// Show CI build status
    Status,
    /// View build results
    Results {
        /// Task hash
        task_id: String,
    },
}

// --- Chat subcommands ---

#[derive(Subcommand)]
enum ChatCmd {
    /// Join a chat channel
    Join {
        /// Channel name
        channel: String,
    },
    /// Leave a chat channel
    Leave {
        /// Channel name
        channel: String,
    },
    /// List joined channels
    Ls,
    /// Send a message to a channel
    Send {
        /// Channel name
        channel: String,
        /// Message text
        message: String,
        /// Reply to a message ID (thread)
        #[arg(long)]
        reply: Option<String>,
        /// Attach a blob hash
        #[arg(long)]
        blob: Option<String>,
        /// Bootstrap peer node IDs
        #[arg(long = "peer", short = 'p')]
        peers: Vec<String>,
    },
    /// View message history
    History {
        /// Channel name
        channel: String,
        /// Number of messages to show (default: 50)
        #[arg(short = 'n', long, default_value = "50")]
        limit: usize,
    },
    /// Watch for new messages in real-time
    Watch {
        /// Channel name
        channel: String,
        /// Bootstrap peer node IDs
        #[arg(long = "peer", short = 'p')]
        peers: Vec<String>,
    },
    /// Sync missed messages from peers
    Sync {
        /// Channel name (sync all channels if omitted)
        channel: Option<String>,
    },
}

// --- Task subcommands ---

#[derive(Subcommand)]
enum TaskCmd {
    /// Create a new task
    Create {
        /// Task title
        title: String,
        /// Priority: critical, high, medium, low
        #[arg(short, long)]
        priority: Option<String>,
        /// Assign to a peer
        #[arg(short, long)]
        assignee: Option<String>,
    },
    /// Assign a task to a peer
    Assign {
        /// Task ID
        id: String,
        /// Peer node ID or name
        peer: String,
    },
    /// Show task board
    List {
        /// Filter by status column
        #[arg(long)]
        status: Option<String>,
    },
    /// Show task details
    Show {
        /// Task ID
        id: String,
    },
    /// Update task status or priority
    Update {
        /// Task ID
        id: String,
        /// New status
        #[arg(short, long)]
        status: Option<String>,
        /// New priority
        #[arg(short, long)]
        priority: Option<String>,
    },
    /// Link artifacts to a task
    Link {
        /// Task ID
        id: String,
        /// Link spec document
        #[arg(long)]
        spec: Option<String>,
        /// Link commit hash
        #[arg(long)]
        commit: Option<String>,
        /// Link CI result
        #[arg(long)]
        ci_result: Option<String>,
    },
    /// Delete a task
    Delete {
        /// Task ID
        id: String,
    },
    /// Claim a task for this agent (atomic claim-or-fail)
    Claim {
        /// Task ID
        id: String,
        /// Claim TTL in seconds (default: 300 = 5 min, re-claim to refresh)
        #[arg(long, default_value = "300")]
        ttl: u64,
    },
    /// Release a claimed task
    Release {
        /// Task ID
        id: String,
    },
}

// --- MCP subcommands ---

#[derive(Subcommand)]
enum McpCmd {
    /// Start the MCP server (stdio transport)
    Start {
        /// Tool namespaces to expose (comma-separated, e.g. "daemon,doc,git")
        #[arg(long)]
        tools: Option<String>,
    },
    /// Show MCP server info and available tools
    Status,
}

// --- Deploy subcommands ---

#[derive(Subcommand)]
enum DeployCmd {
    /// Cross-compile and stage binary as a blob
    Build {
        /// Rust target triple (default: aarch64-unknown-linux-gnu)
        #[arg(long)]
        target: Option<String>,
    },
    /// Push staged binary to mesh peers
    Push {
        /// Target peer node ID (optional, broadcasts to all if omitted)
        peer: Option<String>,
    },
    /// Show staged build info
    Status,
}

// --- Mesh subcommands ---

#[derive(Subcommand)]
enum MeshCmd {
    /// Show aggregate mesh capabilities (CPU, RAM, storage, GPUs)
    Status {
        /// Discovery timeout in seconds (default: 5)
        #[arg(long, default_value = "5")]
        timeout: u64,
    },
}

fn init_tracing(verbose: bool, log_file: Option<std::path::PathBuf>) {
    use tracing_subscriber::{fmt, prelude::*, EnvFilter};

    let default = if verbose { "debug" } else { "warn" };
    let filter = EnvFilter::try_from_env("BOLO_LOG").unwrap_or_else(|_| EnvFilter::new(default));

    let stderr_layer = fmt::layer().with_target(false).with_writer(std::io::stderr);

    if let Some(path) = log_file {
        if let Ok(file) = std::fs::File::create(&path) {
            let file_layer = fmt::layer()
                .with_target(false)
                .with_ansi(false)
                .with_writer(std::sync::Mutex::new(file));
            tracing_subscriber::registry()
                .with(filter)
                .with(stderr_layer)
                .with(file_layer)
                .init();
            return;
        }
    }

    tracing_subscriber::registry()
        .with(filter)
        .with(stderr_layer)
        .init();
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    // For any daemon start (foreground or detached child), write to daemon.log
    let log_file = if matches!(
        &cli.command,
        Commands::Daemon {
            command: DaemonCmd::Start { .. }
        }
    ) {
        let config_dir = cmd::daemon::resolve_config_dir(cli.config.as_deref())?;
        let data_dir = cmd::daemon::resolve_data_dir(&config_dir);
        std::fs::create_dir_all(&data_dir)?;
        Some(data_dir.join("daemon.log"))
    } else {
        None
    };
    init_tracing(cli.verbose, log_file);

    match cli.command {
        Commands::Daemon { command } => match command {
            DaemonCmd::Init => cmd::daemon::init(cli.config.as_deref(), cli.json)?,
            DaemonCmd::Start { detach } => {
                cmd::daemon::start(detach, cli.config.as_deref(), cli.json).await?
            }
            DaemonCmd::Stop => cmd::daemon::stop(cli.config.as_deref(), cli.json)?,
            DaemonCmd::Status => cmd::daemon::status(cli.config.as_deref(), cli.json)?,
            DaemonCmd::Export { path } => {
                cmd::daemon::export(&path, cli.config.as_deref(), cli.json)?
            }
            DaemonCmd::Import { path, force } => {
                cmd::daemon::import(&path, force, cli.config.as_deref(), cli.json)?
            }
            DaemonCmd::Install => cmd::daemon::install(cli.config.as_deref(), cli.json)?,
            DaemonCmd::Uninstall => cmd::daemon::uninstall(cli.config.as_deref(), cli.json)?,
            DaemonCmd::Upgrade { platform } => {
                cmd::daemon::upgrade(platform.as_deref(), cli.config.as_deref(), cli.json).await?
            }
            DaemonCmd::Logs { lines } => cmd::daemon::logs(lines, cli.config.as_deref(), cli.json)?,
        },
        Commands::Id { command } => match command {
            IdCmd::Show => cmd::id::show(cli.config.as_deref(), cli.json)?,
            IdCmd::Export => cmd::id::export(cli.config.as_deref(), cli.json)?,
            IdCmd::Import { file } => cmd::id::import(&file, cli.config.as_deref(), cli.json)?,
            IdCmd::Sign { data } => cmd::id::sign(&data, cli.config.as_deref(), cli.json)?,
            IdCmd::Verify { data, sig, peer } => cmd::id::verify(&data, &sig, &peer, cli.json)?,
        },
        Commands::Peer { command } => match command {
            PeerCmd::Add { node_id } => {
                cmd::peer::add(&node_id, cli.config.as_deref(), cli.json).await?
            }
            PeerCmd::Rm { node_id } => cmd::peer::rm(&node_id, cli.config.as_deref(), cli.json)?,
            PeerCmd::Ls => cmd::peer::ls(cli.config.as_deref(), cli.json)?,
            PeerCmd::Discover { topic } => {
                cmd::peer::discover(&topic, cli.config.as_deref(), cli.json)?
            }
            PeerCmd::Ping { node_id } => {
                cmd::peer::ping(&node_id, cli.config.as_deref(), cli.json).await?
            }
            PeerCmd::Bench { node_id, size } => {
                cmd::peer::bench(&node_id, size, cli.config.as_deref(), cli.json).await?
            }
            PeerCmd::Logs { node_id, lines } => {
                cmd::peer::logs(&node_id, lines, cli.config.as_deref(), cli.json).await?
            }
            PeerCmd::Trust { node_id } => {
                cmd::peer::trust(&node_id, cli.config.as_deref(), cli.json)?
            }
            PeerCmd::Untrust { node_id } => {
                cmd::peer::untrust(&node_id, cli.config.as_deref(), cli.json)?
            }
        },
        Commands::Relay { command } => match command {
            RelayCmd::Start => cmd::relay::start(cli.config.as_deref(), cli.json)?,
            RelayCmd::Stop => cmd::relay::stop(cli.config.as_deref(), cli.json)?,
            RelayCmd::Status => cmd::relay::status(cli.config.as_deref(), cli.json)?,
            RelayCmd::Ls => cmd::relay::ls(cli.config.as_deref(), cli.json)?,
            RelayCmd::Add { url } => cmd::relay::add(&url, cli.config.as_deref(), cli.json)?,
            RelayCmd::Rm { url } => cmd::relay::rm(&url, cli.config.as_deref(), cli.json)?,
            RelayCmd::Ping => cmd::relay::ping(cli.config.as_deref(), cli.json)?,
            RelayCmd::Discover => cmd::relay::discover(cli.config.as_deref(), cli.json)?,
        },
        Commands::Blob { command } => match command {
            BlobCmd::Put { file } => cmd::blob::put(&file, cli.config.as_deref(), cli.json).await?,
            BlobCmd::Get { hash, path } => {
                cmd::blob::get(&hash, path.as_deref(), cli.config.as_deref(), cli.json).await?
            }
            BlobCmd::Ls => cmd::blob::ls(cli.config.as_deref(), cli.json).await?,
            BlobCmd::Stat { hash } => {
                cmd::blob::stat(&hash, cli.config.as_deref(), cli.json).await?
            }
            BlobCmd::Pin { hash } => cmd::blob::pin(&hash, cli.config.as_deref(), cli.json).await?,
            BlobCmd::Unpin { hash } => {
                cmd::blob::unpin(&hash, cli.config.as_deref(), cli.json).await?
            }
            BlobCmd::Fetch { hash, peer, path } => {
                cmd::blob::fetch(
                    &hash,
                    &peer,
                    path.as_deref(),
                    cli.config.as_deref(),
                    cli.json,
                )
                .await?
            }
            BlobCmd::Gc => cmd::blob::gc(cli.config.as_deref(), cli.json).await?,
            BlobCmd::EncryptStore => cmd::blob::encrypt_store(cli.config.as_deref(), cli.json)?,
        },
        Commands::Doc { command } => match command {
            DocCmd::Create { path } => cmd::doc::create(&path, cli.config.as_deref(), cli.json)?,
            DocCmd::Rm { path } => cmd::doc::rm(&path, cli.config.as_deref(), cli.json)?,
            DocCmd::Ls { prefix } => {
                cmd::doc::ls(prefix.as_deref(), cli.config.as_deref(), cli.json)?
            }
            DocCmd::Get { path, key } => {
                cmd::doc::get(&path, key.as_deref(), cli.config.as_deref(), cli.json)?
            }
            DocCmd::Set { path, key, value } => {
                cmd::doc::set(&path, &key, &value, cli.config.as_deref(), cli.json).await?
            }
            DocCmd::Del { path, key } => {
                cmd::doc::del(&path, &key, cli.config.as_deref(), cli.json).await?
            }
            DocCmd::Edit { path } => cmd::doc::edit(&path, cli.config.as_deref(), cli.json)?,
            DocCmd::Read { path } => cmd::doc::read(&path, cli.config.as_deref(), cli.json)?,
            DocCmd::Append { path, value } => {
                cmd::doc::append(&path, &value, cli.config.as_deref(), cli.json).await?
            }
            DocCmd::Watch { path } => {
                cmd::doc::watch(&path, cli.config.as_deref(), cli.json).await?
            }
            DocCmd::Diff { path } => cmd::doc::diff(&path, cli.config.as_deref(), cli.json)?,
            DocCmd::History { path } => cmd::doc::history(&path, cli.config.as_deref(), cli.json)?,
            DocCmd::Sync { path } => {
                cmd::doc::sync(path.as_deref(), cli.config.as_deref(), cli.json).await?
            }
            DocCmd::Export { path, file } => {
                cmd::doc::export(&path, &file, cli.config.as_deref(), cli.json)?
            }
            DocCmd::Import { file, path } => {
                cmd::doc::import(&file, &path, cli.config.as_deref(), cli.json)?
            }
            DocCmd::Share { path } => cmd::doc::share(&path, cli.config.as_deref(), cli.json)?,
            DocCmd::Join { ticket } => cmd::doc::join(&ticket, cli.config.as_deref(), cli.json)?,
            DocCmd::Revoke { ticket_id } => {
                cmd::doc::revoke(&ticket_id, cli.config.as_deref(), cli.json)?
            }
            DocCmd::Tickets { path } => cmd::doc::tickets(&path, cli.config.as_deref(), cli.json)?,
            DocCmd::Compact { path, all } => {
                cmd::doc::compact(&path, all, cli.config.as_deref(), cli.json)?
            }
            DocCmd::Acl { command } => match command {
                AclCmd::Grant { path, peer_id } => {
                    cmd::doc::acl_grant(&path, &peer_id, cli.config.as_deref(), cli.json)?
                }
                AclCmd::Revoke { path, peer_id } => {
                    cmd::doc::acl_revoke(&path, &peer_id, cli.config.as_deref(), cli.json)?
                }
                AclCmd::Show { path } => {
                    cmd::doc::acl_show(&path, cli.config.as_deref(), cli.json)?
                }
            },
        },
        Commands::Pub { command } => match command {
            PubCmd::Send {
                topic,
                message,
                peers,
            } => {
                cmd::pub_sub::send(&topic, &message, &peers, cli.config.as_deref(), cli.json)
                    .await?
            }
            PubCmd::Sub { topic, peers } => {
                cmd::pub_sub::sub(&topic, &peers, cli.config.as_deref(), cli.json).await?
            }
            PubCmd::Ls => cmd::pub_sub::ls(cli.config.as_deref(), cli.json).await?,
            PubCmd::Peers { topic } => {
                cmd::pub_sub::peers(&topic, cli.config.as_deref(), cli.json)?
            }
            PubCmd::Unsub { topic } => {
                cmd::pub_sub::unsub(&topic, cli.config.as_deref(), cli.json)?
            }
        },
        Commands::Git { command } => match command {
            GitCmd::Status { path } => {
                cmd::git::git_status(path.as_deref(), cli.config.as_deref(), cli.json)?
            }
            GitCmd::Objects { path } => {
                cmd::git::git_objects(path.as_deref(), cli.config.as_deref(), cli.json)?
            }
            GitCmd::Log { path, count } => {
                cmd::git::git_log(path.as_deref(), count, cli.config.as_deref(), cli.json)?
            }
            GitCmd::Refs { path } => {
                cmd::git::git_refs(path.as_deref(), cli.config.as_deref(), cli.json)?
            }
            GitCmd::Push { path } => {
                cmd::git::git_push(path.as_deref(), cli.config.as_deref(), cli.json).await?
            }
            GitCmd::Pull { path } => {
                cmd::git::git_pull(path.as_deref(), cli.config.as_deref(), cli.json).await?
            }
            GitCmd::Clone { url, dest } => {
                cmd::git::git_clone(&url, dest.as_deref(), cli.config.as_deref(), cli.json).await?
            }
        },
        Commands::Review { command } => match command {
            ReviewCmd::Show { commit } => {
                cmd::git::review_show(&commit, cli.config.as_deref(), cli.json).await?
            }
            ReviewCmd::Comment {
                commit,
                message,
                file,
                line,
            } => {
                cmd::git::review_comment(
                    &commit,
                    &message,
                    file.as_deref(),
                    line,
                    cli.config.as_deref(),
                    cli.json,
                )
                .await?
            }
            ReviewCmd::Approve { commit } => {
                cmd::git::review_approve(&commit, cli.config.as_deref(), cli.json).await?
            }
            ReviewCmd::Reject { commit, message } => {
                cmd::git::review_reject(
                    &commit,
                    message.as_deref(),
                    cli.config.as_deref(),
                    cli.json,
                )
                .await?
            }
            ReviewCmd::Ls => cmd::git::review_ls(cli.config.as_deref(), cli.json).await?,
        },
        Commands::Ci { command } => match command {
            CiCmd::Run { task_type, path } => {
                cmd::ci::run(
                    task_type.as_deref(),
                    path.as_deref(),
                    cli.config.as_deref(),
                    cli.json,
                )
                .await?
            }
            CiCmd::Status => cmd::ci::status(cli.config.as_deref(), cli.json).await?,
            CiCmd::Results { task_id } => {
                cmd::ci::results(&task_id, cli.config.as_deref(), cli.json).await?
            }
        },
        Commands::Chat { command } => match command {
            ChatCmd::Join { channel } => {
                cmd::chat::join(&channel, cli.config.as_deref(), cli.json).await?
            }
            ChatCmd::Leave { channel } => {
                cmd::chat::leave(&channel, cli.config.as_deref(), cli.json)?
            }
            ChatCmd::Ls => cmd::chat::ls(cli.config.as_deref(), cli.json)?,
            ChatCmd::Send {
                channel,
                message,
                reply,
                blob,
                peers,
            } => {
                cmd::chat::send(
                    &channel,
                    &message,
                    reply.as_deref(),
                    blob.as_deref(),
                    &peers,
                    cli.config.as_deref(),
                    cli.json,
                )
                .await?
            }
            ChatCmd::History { channel, limit } => {
                cmd::chat::history(&channel, limit, cli.config.as_deref(), cli.json)?
            }
            ChatCmd::Watch { channel, peers } => {
                cmd::chat::watch(&channel, &peers, cli.config.as_deref(), cli.json).await?
            }
            ChatCmd::Sync { channel } => {
                cmd::chat::sync(channel.as_deref(), cli.config.as_deref(), cli.json).await?
            }
        },
        Commands::Task { command } => match command {
            TaskCmd::Create {
                title,
                priority,
                assignee,
            } => {
                cmd::task::create(
                    &title,
                    priority.as_deref(),
                    assignee.as_deref(),
                    cli.config.as_deref(),
                    cli.json,
                )
                .await?
            }
            TaskCmd::Assign { id, peer } => {
                cmd::task::assign(&id, &peer, cli.config.as_deref(), cli.json).await?
            }
            TaskCmd::List { status } => {
                cmd::task::list(status.as_deref(), cli.config.as_deref(), cli.json).await?
            }
            TaskCmd::Show { id } => cmd::task::show(&id, cli.config.as_deref(), cli.json).await?,
            TaskCmd::Update {
                id,
                status,
                priority,
            } => {
                cmd::task::update(
                    &id,
                    status.as_deref(),
                    priority.as_deref(),
                    cli.config.as_deref(),
                    cli.json,
                )
                .await?
            }
            TaskCmd::Link {
                id,
                spec,
                commit,
                ci_result,
            } => {
                cmd::task::link(
                    &id,
                    spec.as_deref(),
                    commit.as_deref(),
                    ci_result.as_deref(),
                    cli.config.as_deref(),
                    cli.json,
                )
                .await?
            }
            TaskCmd::Delete { id } => {
                cmd::task::delete(&id, cli.config.as_deref(), cli.json).await?
            }
            TaskCmd::Claim { id, ttl } => {
                cmd::task::claim(&id, ttl, cli.config.as_deref(), cli.json).await?
            }
            TaskCmd::Release { id } => {
                cmd::task::release(&id, cli.config.as_deref(), cli.json).await?
            }
        },
        Commands::Deploy { command } => match command {
            DeployCmd::Build { target } => {
                cmd::deploy::build(target.as_deref(), cli.config.as_deref(), cli.json).await?
            }
            DeployCmd::Push { peer } => {
                cmd::deploy::push(peer.as_deref(), cli.config.as_deref(), cli.json).await?
            }
            DeployCmd::Status => cmd::deploy::status(cli.config.as_deref(), cli.json)?,
        },
        Commands::Mesh { command } => match command {
            MeshCmd::Status { timeout } => {
                cmd::mesh::status(timeout, cli.config.as_deref(), cli.json).await?
            }
        },
        Commands::Mcp { command } => match command {
            McpCmd::Start { tools } => {
                cmd::mcp::start(tools.as_deref(), cli.config.as_deref(), cli.json).await?
            }
            McpCmd::Status => cmd::mcp::status(cli.config.as_deref(), cli.json)?,
        },
        Commands::Logs {
            follow,
            lines,
            peer,
            mesh,
        } => {
            cmd::daemon::stream_logs(follow, lines, peer.as_deref(), mesh, cli.config.as_deref())
                .await?
        }
        Commands::Quality { fast, full, fix } => {
            let report = quality::execute(fast, full, fix);
            quality::print_report(&report, cli.json);
            if !report.passed {
                std::process::exit(1);
            }
        }
    }

    Ok(())
}
