# Bolo

P2P mesh platform built on Rust, [Iroh](https://iroh.computer) 0.96, and [Loro](https://loro.dev) CRDTs.

No cloud. No accounts. No API keys. Just cryptographic identity and a mesh of peers.

## Install

```bash
curl -fsSL https://raw.githubusercontent.com/counterpunchtech/bolo/main/install.sh | bash
```

This detects your OS/arch, downloads the binary, and adds it to your PATH.

**Supported platforms:** macOS (Apple Silicon, Intel), Linux (ARM64).

## Quick Start

```bash
# Initialize identity (ed25519 keypair)
bolo daemon init

# Start the daemon
bolo daemon start --detach

# Connect to a peer
bolo peer add <node-id>

# Check the mesh
bolo mesh status
```

## What It Does

Bolo is a self-contained mesh network that replaces GitHub, Slack, CI, and project management with a single binary running on every developer's machine.

| Feature | Command | Sync |
|---------|---------|------|
| **Blob storage** | `bolo blob put/get` | Content-addressed, iroh-blobs |
| **CRDT documents** | `bolo doc edit/set/get` | Loro + gossip, ~200ms cross-node |
| **Git bridge** | `bolo git push/pull/clone` | Refs via CRDT, objects via blobs |
| **Distributed CI** | `bolo ci run/results` | Gossip broadcast, ~230ms round-trip |
| **Task management** | `bolo task create/claim/list` | CRDT-backed, atomic claim for multi-agent |
| **Team chat** | `bolo chat send/history` | Gossip channels, offline delivery |
| **Code review** | `bolo review approve/comment` | CRDT-synced, ed25519 signatures |
| **Pub/sub** | `bolo pub send/subscribe` | Gossip topics |
| **MCP server** | `bolo mcp start` | 74 tools for LLM integration |
| **Hot deploy** | `bolo deploy build/push` | Cross-compile + push binaries via gossip |
| **Log streaming** | `bolo logs -f --mesh` | Local + remote, real-time |

## Multi-Agent Development

Multiple Claude Code instances (or any MCP client) can work on the same codebase simultaneously:

```bash
# Agent A claims a task (atomic — prevents double assignment)
bolo task claim <task-id>

# Agent B tries the same task — gets a conflict
bolo task claim <task-id>
# → { "claimed": false, "conflict": true, "current_claimer": "agent-a-node-id" }

# Agents coordinate via chat
bolo chat send dev "starting work on feature X"

# All agents share the same CRDT-synced task board, docs, and reviews
bolo task list
```

## Architecture

```
Cargo workspace — 13 crates:

bolo-core      Shared types, identity, IPC client, config
bolo-blobs     Content-addressed blob storage (iroh-blobs)
bolo-docs      CRDT document ops (Loro + iroh-gossip)
bolo-pub       Pub/sub messaging (iroh-gossip)
bolo-relay     Relay server (iroh-relay)
bolo-git       Git bridge (local git ops, cross-node sync)
bolo-ci        Distributed CI/CD (gossip-coordinated)
bolo-task      Task management (CRDT-backed kanban board)
bolo-chat      Team chat (gossip channels, offline delivery)
bolo-mcp       MCP server (74 tools, stdio transport)
bolo-cli       Binary crate — the `bolo` command
bolo-types     Transport-agnostic types (zero iroh deps)
```

## Build From Source

```bash
git clone https://github.com/counterpunchtech/bolo.git
cd bolo
cargo build --release -p bolo-cli
# Binary at target/release/bolo
```

## Development

```bash
cargo check --workspace                    # Type check
cargo clippy --workspace -- -D warnings    # Lint (zero warnings policy)
cargo fmt --all -- --check                 # Format check
cargo test --workspace                     # Run tests (108 unit tests)
cargo test -p bolo-cli --test cross_node -- --ignored  # Integration tests (9, need daemon)
```

## License

MIT
