# Bolo

P2P mesh platform built on Rust + Iroh 0.96 + Loro CRDTs.

## Architecture

Cargo workspace with 13 crates:

- **bolo-core** — Shared types, `BoloError` (thiserror), `BoloConfig` (TOML serde), `Identity` (keypair), IPC client (`DaemonClient` over Unix socket). All other crates depend on this.
- **bolo-blobs** — Content-addressed blob storage (iroh-blobs)
- **bolo-docs** — CRDT document ops (Loro + iroh-gossip)
- **bolo-pub** — Pub/sub messaging (iroh-gossip)
- **bolo-relay** — Relay server (iroh-relay)
- **bolo-git** — Git bridge (local git ops, staging-based; cross-node sync NOT implemented)
- **bolo-ci** — CI/CD (distributed CI verified cross-node; hot deploy via gossip)
- **bolo-task** — Task management (board view, filesystem JSON; NO CRDT sync)
- **bolo-chat** — Team chat (gossip channels, signed messages, persistent history; cross-node VERIFIED)
- **bolo-mcp** — MCP server (73 tools, stdio transport)
- **bolo-cli** — Binary crate (`bolo`), clap derive CLI with subcommands: daemon, id, peer, relay, blob, doc, pub, git, review, ci, task, chat, mesh, deploy, mcp, quality

## Specs

All design specs live in `specs/`:
- `CLI.md` — Full CLI reference (`bolo <resource> <verb>` pattern)
- `VISION.md` — 9 primitives (daemon, id, peer, relay, blob, doc, pub, stream, pay)
- `ROADMAP.md` — Phase 0a through Phase 7
- `PRD.md` — Phase 1/2/3 product requirements

## Commands

```bash
cargo check --workspace                    # Type check
cargo clippy --workspace -- -D warnings    # Lint (zero warnings policy)
cargo fmt --all -- --check                 # Format check
cargo test --workspace                     # Run tests
cargo run -p bolo-cli -- --help            # CLI help
cargo run -p bolo-cli -- quality --fast    # Run fast quality gate
cargo run -p bolo-cli -- quality --full    # Run full quality gate (needs cargo-deny, cargo-audit)
```

## Conventions

- Every bolo crate has `#![deny(unsafe_code)]` in its lib.rs/main.rs
- Workspace-level `[workspace.dependencies]` for all shared deps — crates reference via `{ workspace = true }`
- `rustfmt.toml`: edition 2021, max_width 100
- `clippy.toml`: cognitive-complexity-threshold 25
- Tracing via `BOLO_LOG` env var (e.g. `BOLO_LOG=debug`)
- Stub crate deps that are declared for upcoming epics are listed in `[package.metadata.cargo-machete] ignored`
- Unimplemented subcommand handlers dispatch to `todo!()` until implemented in their respective epics
- CLI command handlers live in `bolo-cli/src/cmd/` (one file per resource)
- Use `iroh::Signature` / `iroh::PublicKey` directly — do NOT depend on ed25519-dalek (iroh wraps it internally at a pre-release version)
- Identity keys stored as hex-encoded 32-byte secret key files with 0o600 permissions
- Daemon IPC via Unix socket (`{config_dir}/daemon.sock`) using JSON-RPC 2.0 (newline-delimited). CLI commands auto-detect running daemon and use IPC; fall back to direct access when daemon is not running.
- `bolo daemon start --detach` runs in background; MCP server always uses `--detach`

## Key dep versions

| Crate | Version |
|-------|---------|
| iroh | 0.96.1 |
| iroh-blobs | 0.98.0 |
| iroh-gossip | 0.96.0 |
| iroh-relay | 0.96.1 |
| loro | 1.10.3 |

## No API keys needed

This project uses no OpenAI/Claude API keys or per-token billing. Auth is via cryptographic identity (ed25519 keypairs) only.
