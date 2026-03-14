//! CLI integration tests for the `bolo` binary.
//!
//! Each test creates an isolated temp directory for config/data and invokes
//! the binary via `std::process::Command`, checking exit codes and JSON output.

use std::path::{Path, PathBuf};
use std::process::Command;

fn bolo() -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_bolo"));
    // Suppress tracing output; the loro crate may still emit diagnostic
    // lines to stdout during snapshot export, so we also use `parse_json`
    // to extract the JSON object from potentially noisy output.
    cmd.env("BOLO_LOG", "error");
    cmd
}

/// Extract a JSON object from stdout that may contain non-JSON prefix lines
/// (e.g. diagnostic output from the loro crate's snapshot export).
fn parse_json(stdout: &[u8]) -> serde_json::Value {
    let text = String::from_utf8_lossy(stdout);
    // Try parsing the entire output first (fast path).
    if let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) {
        return v;
    }
    // Fall back: find the first '{' and parse from there.
    if let Some(start) = text.find('{') {
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(&text[start..]) {
            return v;
        }
    }
    panic!(
        "could not extract JSON from stdout:\n{text}\n(raw bytes: {} bytes)",
        stdout.len()
    );
}

/// Return a config directory path inside the temp dir that has no file extension.
///
/// `tempfile::tempdir()` creates paths like `/tmp/tmp.ABCdef` which contain a dot.
/// The CLI's `resolve_config_dir` interprets paths with extensions as config *files*
/// and returns the parent directory, which would escape our isolation. Using a
/// subdirectory without a dot avoids this.
fn config_dir(tmp: &tempfile::TempDir) -> PathBuf {
    tmp.path().join("config")
}

/// Run `bolo daemon init --json --config <dir>` and return parsed JSON output.
fn daemon_init(config_dir: &Path) -> serde_json::Value {
    let output = bolo()
        .args([
            "--json",
            "--config",
            config_dir.to_str().unwrap(),
            "daemon",
            "init",
        ])
        .output()
        .expect("failed to run bolo daemon init");
    assert!(
        output.status.success(),
        "daemon init failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    parse_json(&output.stdout)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
fn help_output() {
    let output = bolo()
        .arg("--help")
        .output()
        .expect("failed to run bolo --help");
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("Bolo"),
        "help output should mention Bolo, got: {stdout}"
    );
}

#[test]
fn daemon_init_and_status() {
    let tmp = tempfile::tempdir().unwrap();
    let cfg = config_dir(&tmp);

    // init
    let init_json = daemon_init(&cfg);
    assert!(
        init_json.get("node_id").and_then(|v| v.as_str()).is_some(),
        "daemon init JSON should contain node_id"
    );
    assert!(
        init_json
            .get("config_dir")
            .and_then(|v| v.as_str())
            .is_some(),
        "daemon init JSON should contain config_dir"
    );

    // status (daemon is not running)
    let output = bolo()
        .args([
            "--json",
            "--config",
            cfg.to_str().unwrap(),
            "daemon",
            "status",
        ])
        .output()
        .expect("failed to run daemon status");
    assert!(output.status.success());
    let status = parse_json(&output.stdout);
    assert_eq!(
        status.get("running").and_then(|v| v.as_bool()),
        Some(false),
        "daemon should not be running"
    );
}

#[test]
fn doc_lifecycle() {
    let tmp = tempfile::tempdir().unwrap();
    let cfg = config_dir(&tmp);
    daemon_init(&cfg);

    let cfg_str = cfg.to_str().unwrap();

    // create
    let output = bolo()
        .args(["--json", "--config", cfg_str, "doc", "create", "specs/test"])
        .output()
        .expect("failed to run doc create");
    assert!(
        output.status.success(),
        "doc create failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let create_json = parse_json(&output.stdout);
    assert_eq!(create_json["created"], true);
    assert_eq!(create_json["path"], "specs/test");

    // ls
    let output = bolo()
        .args(["--json", "--config", cfg_str, "doc", "ls"])
        .output()
        .expect("failed to run doc ls");
    assert!(output.status.success());
    let ls_json = parse_json(&output.stdout);
    let docs = ls_json["documents"]
        .as_array()
        .expect("documents should be an array");
    assert!(
        docs.iter().any(|d| d.as_str() == Some("specs/test")),
        "doc ls should include specs/test, got: {docs:?}"
    );

    // read (empty doc — should still succeed)
    let output = bolo()
        .args(["--json", "--config", cfg_str, "doc", "read", "specs/test"])
        .output()
        .expect("failed to run doc read");
    assert!(
        output.status.success(),
        "doc read failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let read_json = parse_json(&output.stdout);
    assert_eq!(read_json["path"], "specs/test");
    assert!(
        read_json.get("content").is_some(),
        "read JSON should have content field"
    );

    // history
    let output = bolo()
        .args([
            "--json",
            "--config",
            cfg_str,
            "doc",
            "history",
            "specs/test",
        ])
        .output()
        .expect("failed to run doc history");
    assert!(
        output.status.success(),
        "doc history failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let hist_json = parse_json(&output.stdout);
    assert_eq!(hist_json["path"], "specs/test");
    assert!(
        hist_json.get("peers").is_some(),
        "history JSON should have peers field"
    );
}

#[test]
fn doc_set_and_get() {
    let tmp = tempfile::tempdir().unwrap();
    let cfg = config_dir(&tmp);
    daemon_init(&cfg);

    let cfg_str = cfg.to_str().unwrap();

    // set (auto-creates the doc)
    let output = bolo()
        .args([
            "--json", "--config", cfg_str, "doc", "set", "specs/kv", "greeting", "hello",
        ])
        .output()
        .expect("failed to run doc set");
    assert!(
        output.status.success(),
        "doc set failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let set_json = parse_json(&output.stdout);
    assert_eq!(set_json["key"], "greeting");
    assert_eq!(set_json["value"], "hello");

    // get the key back
    let output = bolo()
        .args([
            "--json", "--config", cfg_str, "doc", "get", "specs/kv", "greeting",
        ])
        .output()
        .expect("failed to run doc get");
    assert!(
        output.status.success(),
        "doc get failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let get_json = parse_json(&output.stdout);
    assert_eq!(get_json["path"], "specs/kv");
    assert_eq!(get_json["key"], "greeting");
    // The value contains the debug representation of the LoroValue
    let val = get_json["value"].as_str().unwrap_or("");
    assert!(
        val.contains("hello"),
        "get value should contain 'hello', got: {val}"
    );
}

#[test]
fn doc_export_import() {
    let tmp = tempfile::tempdir().unwrap();
    let cfg = config_dir(&tmp);
    daemon_init(&cfg);

    let cfg_str = cfg.to_str().unwrap();

    // create doc and set a key
    let output = bolo()
        .args([
            "--json",
            "--config",
            cfg_str,
            "doc",
            "set",
            "specs/original",
            "key1",
            "value1",
        ])
        .output()
        .expect("failed to set doc");
    assert!(
        output.status.success(),
        "doc set for export test failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    // export
    let export_file = tmp.path().join("export.snapshot");
    let export_file_str = export_file.to_str().unwrap();
    let output = bolo()
        .args([
            "--json",
            "--config",
            cfg_str,
            "doc",
            "export",
            "specs/original",
            export_file_str,
        ])
        .output()
        .expect("failed to run doc export");
    assert!(
        output.status.success(),
        "doc export failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let export_json = parse_json(&output.stdout);
    assert!(
        export_json["size"].as_u64().unwrap_or(0) > 0,
        "exported file should have non-zero size"
    );

    // import into a different path
    let output = bolo()
        .args([
            "--json",
            "--config",
            cfg_str,
            "doc",
            "import",
            export_file_str,
            "specs/imported",
        ])
        .output()
        .expect("failed to run doc import");
    assert!(
        output.status.success(),
        "doc import failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let import_json = parse_json(&output.stdout);
    assert_eq!(import_json["path"], "specs/imported");

    // verify imported doc has the same data
    let output = bolo()
        .args([
            "--json",
            "--config",
            cfg_str,
            "doc",
            "get",
            "specs/imported",
            "key1",
        ])
        .output()
        .expect("failed to read imported doc");
    assert!(
        output.status.success(),
        "doc get on imported failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let get_json = parse_json(&output.stdout);
    let val = get_json["value"].as_str().unwrap_or("");
    assert!(
        val.contains("value1"),
        "imported doc should contain 'value1', got: {val}"
    );
}

#[test]
fn id_show() {
    let tmp = tempfile::tempdir().unwrap();
    let cfg = config_dir(&tmp);
    let init_json = daemon_init(&cfg);

    let output = bolo()
        .args(["--json", "--config", cfg.to_str().unwrap(), "id", "show"])
        .output()
        .expect("failed to run id show");
    assert!(
        output.status.success(),
        "id show failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let id_json = parse_json(&output.stdout);
    let node_id = id_json["node_id"]
        .as_str()
        .expect("id show should return node_id");
    assert!(!node_id.is_empty(), "node_id should not be empty");
    assert_eq!(
        node_id,
        init_json["node_id"].as_str().unwrap(),
        "id show node_id should match daemon init node_id"
    );
}

#[test]
fn git_status_in_repo() {
    // Run from the bolo-specs repo root which is a git repository.
    let repo_root = env!("CARGO_MANIFEST_DIR")
        .strip_suffix("/bolo-cli")
        .unwrap_or(env!("CARGO_MANIFEST_DIR"));
    let output = bolo()
        .args(["--json", "git", "status", "--path", repo_root])
        .output()
        .expect("failed to run git status");
    assert!(
        output.status.success(),
        "git status failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let status = parse_json(&output.stdout);
    assert!(
        status.get("repo_path").is_some(),
        "git status JSON should have repo_path, got: {status}"
    );
}

#[test]
fn mcp_status() {
    let output = bolo()
        .args(["--json", "mcp", "status"])
        .output()
        .expect("failed to run mcp status");
    assert!(
        output.status.success(),
        "mcp status failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let status = parse_json(&output.stdout);
    assert_eq!(status["server"], "bolo-mcp");
    let tools_count = status["tools_count"]
        .as_u64()
        .expect("tools_count should be a number");
    assert!(
        tools_count > 0,
        "tools_count should be > 0, got: {tools_count}"
    );
}

#[test]
fn unknown_command_fails() {
    let output = bolo()
        .arg("nonexistent")
        .output()
        .expect("failed to run bolo nonexistent");
    assert!(
        !output.status.success(),
        "unknown subcommand should exit non-zero"
    );
}
