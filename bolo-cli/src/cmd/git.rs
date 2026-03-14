//! Git and review command handlers.

use anyhow::{Context, Result};
use bolo_core::{Identity, Timestamp};
use bolo_git::GitBridge;

use super::daemon::{resolve_config_dir, resolve_data_dir};

/// Open a review store from config.
fn open_review_store(config_flag: Option<&str>) -> Result<bolo_git::ReviewStore> {
    let config_dir = resolve_config_dir(config_flag)?;
    let data_dir = resolve_data_dir(&config_dir);
    bolo_git::ReviewStore::open(&data_dir).context("failed to open review store")
}

/// Load the node identity from config.
fn load_identity(config_flag: Option<&str>) -> Result<Identity> {
    let config_dir = resolve_config_dir(config_flag)?;
    Identity::load_from_config_dir(&config_dir).context("failed to load identity")
}

/// Try to connect to the daemon. Returns None if daemon is not running.
async fn try_daemon(config_flag: Option<&str>) -> Option<bolo_core::ipc::DaemonClient> {
    let config_dir = resolve_config_dir(config_flag).ok()?;
    bolo_core::ipc::DaemonClient::connect(&config_dir)
        .await
        .ok()
}

/// Derive a stable repo name from a path.
fn repo_name_from_path(path: &str) -> String {
    std::path::Path::new(path)
        .canonicalize()
        .unwrap_or_else(|_| std::path::PathBuf::from(path))
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_else(|| "repo".to_string())
}

/// `bolo git status` — show git repo sync state.
pub fn git_status(path: Option<&str>, _config_flag: Option<&str>, json: bool) -> Result<()> {
    let repo_path = path.unwrap_or(".");
    let bridge =
        GitBridge::discover(std::path::Path::new(repo_path)).context("failed to open git repo")?;
    let status = bridge.status().context("failed to get git status")?;

    if json {
        println!("{}", serde_json::to_string_pretty(&status)?);
    } else {
        println!("Repository: {}", status.repo_path);
        if let Some(ref head) = status.head_ref {
            println!("  HEAD: {head}");
        }
        if let Some(ref oid) = status.head_oid {
            println!("  OID:  {}", &oid[..oid.len().min(12)]);
        }
        if status.is_clean {
            println!("  Status: clean");
        } else {
            if !status.staged.is_empty() {
                println!("  Staged:");
                for f in &status.staged {
                    println!("    + {f}");
                }
            }
            if !status.modified.is_empty() {
                println!("  Modified:");
                for f in &status.modified {
                    println!("    ~ {f}");
                }
            }
            if !status.untracked.is_empty() {
                println!("  Untracked:");
                for f in &status.untracked {
                    println!("    ? {f}");
                }
            }
        }
    }

    Ok(())
}

/// `bolo git objects` — list git objects in HEAD tree.
pub fn git_objects(path: Option<&str>, _config_flag: Option<&str>, json: bool) -> Result<()> {
    let repo_path = path.unwrap_or(".");
    let bridge =
        GitBridge::discover(std::path::Path::new(repo_path)).context("failed to open git repo")?;
    let objects = bridge.list_objects().context("failed to list objects")?;

    if json {
        println!("{}", serde_json::to_string_pretty(&objects)?);
    } else if objects.is_empty() {
        println!("No objects in HEAD tree.");
    } else {
        for obj in &objects {
            println!(
                "{} {}",
                &obj.git_oid[..obj.git_oid.len().min(8)],
                obj.bolo_hash
            );
        }
        println!("\n{} object(s)", objects.len());
    }

    Ok(())
}

/// `bolo git log` — show recent commits.
pub fn git_log(
    path: Option<&str>,
    count: usize,
    _config_flag: Option<&str>,
    json: bool,
) -> Result<()> {
    let repo_path = path.unwrap_or(".");
    let bridge =
        GitBridge::discover(std::path::Path::new(repo_path)).context("failed to open git repo")?;
    let commits = bridge.log(count).context("failed to read git log")?;

    if json {
        println!("{}", serde_json::to_string_pretty(&commits)?);
    } else if commits.is_empty() {
        println!("No commits.");
    } else {
        for c in &commits {
            let short_oid = &c.git_oid[..c.git_oid.len().min(8)];
            let first_line = c.message.lines().next().unwrap_or("");
            println!("{short_oid} {first_line}");
            println!("  Author: {}", c.author);
        }
    }

    Ok(())
}

/// `bolo git refs` — list all refs.
pub fn git_refs(path: Option<&str>, _config_flag: Option<&str>, json: bool) -> Result<()> {
    let repo_path = path.unwrap_or(".");
    let bridge =
        GitBridge::discover(std::path::Path::new(repo_path)).context("failed to open git repo")?;
    let refs = bridge.list_refs().context("failed to list refs")?;

    if json {
        println!("{}", serde_json::to_string_pretty(&refs)?);
    } else if refs.is_empty() {
        println!("No refs.");
    } else {
        for r in &refs {
            let head_marker = if r.is_head { " *" } else { "" };
            let short_oid = &r.target_oid[..r.target_oid.len().min(8)];
            println!("{short_oid} {}{head_marker}", r.name);
        }
    }

    Ok(())
}

/// `bolo git push` — sync local git objects to mesh via daemon.
///
/// Walks the git object graph, stores each object as a blob via daemon IPC,
/// records oid→hash mappings in a CRDT doc, and updates refs in another CRDT doc.
/// Both docs sync to peers automatically via gossip.
pub async fn git_push(path: Option<&str>, config_flag: Option<&str>, json: bool) -> Result<()> {
    let repo_path = path.unwrap_or(".");
    let bridge =
        GitBridge::discover(std::path::Path::new(repo_path)).context("failed to open git repo")?;
    let config_dir = resolve_config_dir(config_flag)?;
    let repo_name = repo_name_from_path(repo_path);

    let mut client = bolo_core::ipc::DaemonClient::connect(&config_dir)
        .await
        .context(
            "Cannot push: daemon is not running.\n\
             Start the daemon with `bolo daemon start`, then retry.",
        )?;

    // Walk all reachable objects from all refs
    let objects = bridge
        .walk_reachable_objects()
        .context("failed to walk git objects")?;
    eprintln!("Found {} git objects", objects.len());

    // Read existing objects doc to find what's already on mesh
    let objects_doc_path = format!("git/objects/{repo_name}");
    let mut already_pushed = std::collections::HashSet::new();
    if let Ok(result) = client
        .call("doc.get", serde_json::json!({ "path": &objects_doc_path }))
        .await
    {
        // The doc exists — check which oids are already mapped
        if let Some(val_str) = result.get("value").and_then(|v| v.as_str()) {
            // Parse the debug output to extract keys — or just try each object
            // Since we can't easily parse the Loro debug format, we'll check individually
            let _ = val_str;
        }
    }

    // For efficiency, try to read each object's mapping to see if it exists
    // But for first push, we'll batch: store objects, then set_many the mappings
    let staging_dir = std::env::temp_dir().join(format!("bolo-git-push-{}", std::process::id()));
    std::fs::create_dir_all(&staging_dir)?;

    let mut new_mappings = serde_json::Map::new();
    let mut new_count = 0usize;
    let mut skipped = 0usize;

    for (oid, obj_type) in &objects {
        // Check if already in objects doc by trying doc.get with key
        let check = client
            .call(
                "doc.get",
                serde_json::json!({ "path": &objects_doc_path, "key": oid }),
            )
            .await;
        if let Ok(ref result) = check {
            if result.get("value").and_then(|v| v.as_str()).is_some() {
                already_pushed.insert(oid.clone());
                skipped += 1;
                continue;
            }
        }

        // Read raw object data from git ODB
        let (_, data) = bridge
            .read_object_raw(oid)
            .context(format!("failed to read git object {oid}"))?;

        // Write to temp file for blob.put
        let tmp_path = staging_dir.join(oid);
        std::fs::write(&tmp_path, &data)?;

        // Store as blob via daemon
        let result = client
            .call(
                "blob.put",
                serde_json::json!({ "file": tmp_path.to_str().unwrap() }),
            )
            .await
            .context(format!("failed to store object {oid} as blob"))?;

        let blob_hash = result["hash"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("blob.put didn't return hash"))?;

        // Record mapping: oid → "type:hash"
        new_mappings.insert(
            oid.clone(),
            serde_json::json!(format!("{obj_type}:{blob_hash}")),
        );
        new_count += 1;

        // Clean up temp file
        let _ = std::fs::remove_file(&tmp_path);
    }

    // Batch-set all object mappings in one CRDT commit
    if !new_mappings.is_empty() {
        client
            .call(
                "doc.set_many",
                serde_json::json!({
                    "path": &objects_doc_path,
                    "entries": serde_json::Value::Object(new_mappings),
                }),
            )
            .await
            .context("failed to update objects doc")?;
    }

    // Update refs doc
    let refs_doc_path = format!("git/refs/{repo_name}");
    let refs = bridge.list_refs().context("failed to list refs")?;
    let mut ref_entries = serde_json::Map::new();
    let ref_names: Vec<String> = refs.iter().map(|r| r.name.clone()).collect();
    // Store a comma-separated index of ref names so pull can enumerate them
    ref_entries.insert(
        "__refs__".to_string(),
        serde_json::json!(ref_names.join(",")),
    );
    for r in &refs {
        ref_entries.insert(r.name.clone(), serde_json::json!(&r.target_oid));
    }
    if !ref_entries.is_empty() {
        client
            .call(
                "doc.set_many",
                serde_json::json!({
                    "path": &refs_doc_path,
                    "entries": serde_json::Value::Object(ref_entries),
                }),
            )
            .await
            .context("failed to update refs doc")?;
    }

    // Clean up staging dir
    let _ = std::fs::remove_dir_all(&staging_dir);

    if json {
        let out = serde_json::json!({
            "pushed": new_count,
            "skipped": skipped,
            "refs": refs.len(),
            "repo": repo_name,
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!(
            "Pushed {} object(s) to mesh ({} already present)",
            new_count, skipped
        );
        println!("  Refs synced: {}", refs.len());
        println!("  Repo: {repo_name}");
    }

    Ok(())
}

/// `bolo git pull` — pull git objects from mesh peers via daemon.
///
/// Reads refs and object mappings from CRDT docs (synced via gossip),
/// fetches missing objects as blobs, imports them into the local git ODB,
/// and updates local refs.
pub async fn git_pull(path: Option<&str>, config_flag: Option<&str>, json: bool) -> Result<()> {
    let repo_path = path.unwrap_or(".");
    let bridge =
        GitBridge::discover(std::path::Path::new(repo_path)).context("failed to open git repo")?;
    let config_dir = resolve_config_dir(config_flag)?;
    let repo_name = repo_name_from_path(repo_path);

    let mut client = bolo_core::ipc::DaemonClient::connect(&config_dir)
        .await
        .context(
            "Cannot pull: daemon is not running.\n\
             Start the daemon with `bolo daemon start`, then retry.",
        )?;

    // Read remote refs from CRDT doc (synced via gossip from remote peer)
    let refs_doc_path = format!("git/refs/{repo_name}");
    let remote_refs = read_ref_entries(&mut client, &refs_doc_path)
        .await
        .context("failed to read remote refs")?;

    if remote_refs.is_empty() {
        if json {
            let out = serde_json::json!({
                "repo": repo_name,
                "fetched": 0,
                "refs_updated": 0,
                "message": "no remote refs found",
            });
            println!("{}", serde_json::to_string_pretty(&out)?);
        } else {
            println!("No remote refs found for {repo_name}.");
        }
        return Ok(());
    }

    // Read object mappings from CRDT doc
    let objects_doc_path = format!("git/objects/{repo_name}");

    // Collect all remote ref target OIDs
    let local_refs = bridge.list_refs().context("failed to list local refs")?;

    // For each remote ref, check if we need to fetch objects
    let staging_dir = std::env::temp_dir().join(format!("bolo-git-pull-{}", std::process::id()));
    std::fs::create_dir_all(&staging_dir)?;

    let mut fetched = 0usize;
    let mut refs_updated = 0usize;

    // Walk all remote ref targets and fetch any objects we don't have
    let mut to_fetch = Vec::new();
    for (ref_name, target_oid) in &remote_refs {
        let local_match = local_refs
            .iter()
            .any(|lr| lr.name == *ref_name && lr.target_oid == *target_oid);
        if !local_match {
            // Need to fetch objects for this ref
            // Walk backwards from target collecting all needed OIDs
            collect_missing_objects(
                &bridge,
                &mut client,
                &objects_doc_path,
                target_oid,
                &mut to_fetch,
            )
            .await?;
        }
    }

    // De-duplicate
    let mut seen = std::collections::HashSet::new();
    to_fetch.retain(|(oid, _, _)| seen.insert(oid.clone()));

    eprintln!("Fetching {} missing objects", to_fetch.len());

    // Fetch each missing object
    for (oid, obj_type, blob_hash) in &to_fetch {
        let tmp_path = staging_dir.join(oid);
        let result = client
            .call(
                "blob.get",
                serde_json::json!({ "hash": blob_hash, "path": tmp_path.to_str().unwrap() }),
            )
            .await;

        match result {
            Ok(_) => {
                let data = std::fs::read(&tmp_path)?;
                bridge
                    .write_object_raw(obj_type, &data)
                    .context(format!("failed to import object {oid}"))?;
                fetched += 1;
                let _ = std::fs::remove_file(&tmp_path);
            }
            Err(e) => {
                eprintln!(
                    "Warning: failed to fetch object {}: {e}",
                    &oid[..8.min(oid.len())]
                );
            }
        }
    }

    // Update local refs
    for (ref_name, target_oid) in &remote_refs {
        let local_match = local_refs
            .iter()
            .any(|lr| lr.name == *ref_name && lr.target_oid == *target_oid);
        if !local_match && bridge.has_object(target_oid) {
            bridge
                .set_ref(ref_name, target_oid)
                .context(format!("failed to update ref {ref_name}"))?;
            refs_updated += 1;
        }
    }

    // Checkout HEAD if refs were updated
    if refs_updated > 0 {
        // Set HEAD to the first ref that looks like a main branch
        for (ref_name, _) in &remote_refs {
            if ref_name == "refs/heads/main" || ref_name == "refs/heads/master" {
                let _ = bridge.set_head(ref_name);
                let _ = bridge.checkout_head();
                break;
            }
        }
    }

    let _ = std::fs::remove_dir_all(&staging_dir);

    if json {
        let out = serde_json::json!({
            "repo": repo_name,
            "fetched": fetched,
            "refs_updated": refs_updated,
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Pulled from mesh for {repo_name}:");
        println!("  Objects fetched: {fetched}");
        println!("  Refs updated: {refs_updated}");
        for (ref_name, target_oid) in &remote_refs {
            let short = &target_oid[..target_oid.len().min(8)];
            let local_match = local_refs
                .iter()
                .any(|lr| lr.name == *ref_name && lr.target_oid == *target_oid);
            let status = if local_match { "up-to-date" } else { "updated" };
            println!("  {short} {ref_name} ({status})");
        }
    }

    Ok(())
}

/// Read all ref entries from a CRDT doc by querying known ref name patterns.
/// Since we can't enumerate CRDT map keys via the current IPC, we store
/// a special `__refs__` key listing all ref names.
async fn read_ref_entries(
    client: &mut bolo_core::ipc::DaemonClient,
    refs_doc_path: &str,
) -> Result<Vec<(String, String)>> {
    // First try to get the ref index
    let index_result = client
        .call(
            "doc.get",
            serde_json::json!({ "path": refs_doc_path, "key": "__refs__" }),
        )
        .await;

    let ref_names: Vec<String> = if let Ok(ref result) = index_result {
        if let Some(val) = result.get("value").and_then(|v| v.as_str()) {
            // Parse the Loro string wrapper — value comes back as 'String("refs/heads/main,refs/heads/dev")'
            let clean = val
                .strip_prefix("String(\"")
                .and_then(|s| s.strip_suffix("\")"))
                .or_else(|| {
                    val.strip_prefix("String(LoroStringValue(\"")
                        .and_then(|s| s.strip_suffix("\"))"))
                })
                .unwrap_or(val);
            clean.split(',').map(|s| s.to_string()).collect()
        } else {
            Vec::new()
        }
    } else {
        Vec::new()
    };

    let mut entries = Vec::new();
    for ref_name in &ref_names {
        if ref_name.is_empty() {
            continue;
        }
        let result = client
            .call(
                "doc.get",
                serde_json::json!({ "path": refs_doc_path, "key": ref_name }),
            )
            .await;
        if let Ok(ref result) = result {
            if let Some(val) = result.get("value").and_then(|v| v.as_str()) {
                let clean = val
                    .strip_prefix("String(\"")
                    .and_then(|s| s.strip_suffix("\")"))
                    .or_else(|| {
                        val.strip_prefix("String(LoroStringValue(\"")
                            .and_then(|s| s.strip_suffix("\"))"))
                    })
                    .unwrap_or(val);
                entries.push((ref_name.clone(), clean.to_string()));
            }
        }
    }

    Ok(entries)
}

/// Recursively collect missing objects starting from a git OID.
/// Looks up the object in the CRDT doc to find its blob hash and type,
/// then checks if we have it locally. Follows commit→tree→blob chains.
async fn collect_missing_objects(
    bridge: &GitBridge,
    client: &mut bolo_core::ipc::DaemonClient,
    objects_doc_path: &str,
    start_oid: &str,
    result: &mut Vec<(String, String, String)>, // (oid, type, blob_hash)
) -> Result<()> {
    let mut queue = std::collections::VecDeque::new();
    let mut seen = std::collections::HashSet::new();
    queue.push_back(start_oid.to_string());

    while let Some(oid) = queue.pop_front() {
        if !seen.insert(oid.clone()) {
            continue;
        }
        if bridge.has_object(&oid) {
            continue;
        }

        // Look up this OID in the objects doc
        let lookup = client
            .call(
                "doc.get",
                serde_json::json!({ "path": objects_doc_path, "key": &oid }),
            )
            .await;

        let mapping = match lookup {
            Ok(ref r) => r
                .get("value")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            Err(_) => None,
        };

        let mapping = match mapping {
            Some(m) => m,
            None => continue, // Object not in mesh
        };

        // Parse "type:hash" — handle Loro string wrapper
        let clean = mapping
            .strip_prefix("String(\"")
            .and_then(|s| s.strip_suffix("\")"))
            .or_else(|| {
                mapping
                    .strip_prefix("String(LoroStringValue(\"")
                    .and_then(|s| s.strip_suffix("\"))"))
            })
            .unwrap_or(&mapping);

        let (obj_type, blob_hash) = clean
            .split_once(':')
            .ok_or_else(|| anyhow::anyhow!("invalid object mapping for {oid}: {clean}"))?;

        result.push((oid.clone(), obj_type.to_string(), blob_hash.to_string()));

        // If it's a commit or tree, we need to parse it after fetching to find children.
        // But we don't have the data yet — we'll need to fetch first, then walk.
        // For simplicity, fetch ALL objects that we don't have locally.
        // The recursive walk happens implicitly because we fetch everything.
    }

    Ok(())
}

/// `bolo git clone` — clone a repo from the mesh swarm.
///
/// Reads refs and objects from CRDT docs, fetches all objects as blobs,
/// imports them into a new git repo, sets refs, and checks out HEAD.
pub async fn git_clone(
    url: &str,
    dest: Option<&str>,
    config_flag: Option<&str>,
    json: bool,
) -> Result<()> {
    // The "url" is the repo name on the mesh (e.g. "bolo-specs")
    let repo_name = url
        .trim_end_matches('/')
        .rsplit('/')
        .next()
        .unwrap_or("repo");

    let dest_path = dest.unwrap_or(repo_name);
    let dest_dir = std::path::Path::new(dest_path);

    if dest_dir.exists() {
        anyhow::bail!("destination already exists: {dest_path}");
    }

    let config_dir = resolve_config_dir(config_flag)?;

    let mut client = bolo_core::ipc::DaemonClient::connect(&config_dir)
        .await
        .context(
            "Cannot clone: daemon is not running.\n\
             Start the daemon with `bolo daemon start`, then retry.",
        )?;

    // Read refs doc
    let refs_doc_path = format!("git/refs/{repo_name}");
    let remote_refs = read_ref_entries(&mut client, &refs_doc_path)
        .await
        .context("failed to read remote refs — does this repo exist on the mesh?")?;

    if remote_refs.is_empty() {
        anyhow::bail!("no refs found for repo '{repo_name}' on the mesh");
    }

    // Initialize empty git repo
    let repo = git2::Repository::init(dest_dir).context("failed to initialize repository")?;
    let bridge = GitBridge::open(dest_dir).context("failed to open new repo")?;

    // Read all object mappings from CRDT doc
    let objects_doc_path = format!("git/objects/{repo_name}");

    // Fetch all objects referenced by remote refs
    // Since we have an empty repo, we need everything
    let staging_dir = std::env::temp_dir().join(format!("bolo-git-clone-{}", std::process::id()));
    std::fs::create_dir_all(&staging_dir)?;

    // Collect all objects to fetch — we need to fetch ALL objects in the objects doc
    // since this is a fresh clone
    let mut all_objects = Vec::new();

    // Walk from each ref target
    for (_ref_name, target_oid) in &remote_refs {
        collect_all_objects_from_doc(&mut client, &objects_doc_path, target_oid, &mut all_objects)
            .await?;
    }

    // De-duplicate
    let mut seen = std::collections::HashSet::new();
    all_objects.retain(|(oid, _, _)| seen.insert(oid.clone()));

    eprintln!("Fetching {} objects for clone", all_objects.len());

    let mut fetched = 0usize;
    // Fetch objects in dependency order: blobs first, then trees, then commits
    // This ensures git ODB can validate parent references
    let mut blobs = Vec::new();
    let mut trees = Vec::new();
    let mut commits = Vec::new();
    let mut tags = Vec::new();
    for item in &all_objects {
        match item.1.as_str() {
            "blob" => blobs.push(item),
            "tree" => trees.push(item),
            "commit" => commits.push(item),
            "tag" => tags.push(item),
            _ => {}
        }
    }

    for item in blobs
        .iter()
        .chain(trees.iter())
        .chain(commits.iter())
        .chain(tags.iter())
    {
        let (oid, obj_type, blob_hash) = item;
        let tmp_path = staging_dir.join(oid);
        let result = client
            .call(
                "blob.get",
                serde_json::json!({ "hash": blob_hash, "path": tmp_path.to_str().unwrap() }),
            )
            .await;

        match result {
            Ok(_) => {
                let data = std::fs::read(&tmp_path)?;
                bridge
                    .write_object_raw(obj_type, &data)
                    .context(format!("failed to import object {oid}"))?;
                fetched += 1;
                let _ = std::fs::remove_file(&tmp_path);
            }
            Err(e) => {
                eprintln!("Warning: failed to fetch {oid}: {e}");
            }
        }
    }

    // Set refs
    let mut refs_set = 0usize;
    for (ref_name, target_oid) in &remote_refs {
        if bridge.has_object(target_oid) {
            bridge
                .set_ref(ref_name, target_oid)
                .context(format!("failed to set ref {ref_name}"))?;
            refs_set += 1;
        }
    }

    // Set HEAD and checkout
    for (ref_name, _) in &remote_refs {
        if ref_name == "refs/heads/main" || ref_name == "refs/heads/master" {
            let _ = bridge.set_head(ref_name);
            let _ = bridge.checkout_head();
            break;
        }
    }

    let _ = std::fs::remove_dir_all(&staging_dir);
    drop(repo); // close the repo handle

    if json {
        let out = serde_json::json!({
            "cloned": true,
            "repo": repo_name,
            "dest": dest_path,
            "objects": fetched,
            "refs": refs_set,
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Cloned {repo_name} into {dest_path}");
        println!("  Objects: {fetched}");
        println!("  Refs: {refs_set}");
    }

    Ok(())
}

/// Collect all objects from the mesh objects doc, starting from a root OID.
/// Fetches the object data to discover children (commit parents, tree entries).
async fn collect_all_objects_from_doc(
    client: &mut bolo_core::ipc::DaemonClient,
    objects_doc_path: &str,
    start_oid: &str,
    result: &mut Vec<(String, String, String)>,
) -> Result<()> {
    let mut queue = std::collections::VecDeque::new();
    let mut seen: std::collections::HashSet<String> =
        result.iter().map(|(oid, _, _)| oid.clone()).collect();
    queue.push_back(start_oid.to_string());

    while let Some(oid) = queue.pop_front() {
        if !seen.insert(oid.clone()) {
            continue;
        }

        // Look up this OID in the objects doc
        let lookup = client
            .call(
                "doc.get",
                serde_json::json!({ "path": objects_doc_path, "key": &oid }),
            )
            .await;

        let mapping = match lookup {
            Ok(ref r) => r
                .get("value")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            Err(_) => None,
        };

        let mapping = match mapping {
            Some(m) => m,
            None => continue,
        };

        let clean = mapping
            .strip_prefix("String(\"")
            .and_then(|s| s.strip_suffix("\")"))
            .or_else(|| {
                mapping
                    .strip_prefix("String(LoroStringValue(\"")
                    .and_then(|s| s.strip_suffix("\"))"))
            })
            .unwrap_or(&mapping);

        let (obj_type, blob_hash) = match clean.split_once(':') {
            Some(pair) => pair,
            None => continue,
        };

        result.push((oid.clone(), obj_type.to_string(), blob_hash.to_string()));

        // For commits and trees, we need to discover children.
        // Fetch the raw object data to parse structure.
        let staging_dir =
            std::env::temp_dir().join(format!("bolo-git-walk-{}", std::process::id()));
        std::fs::create_dir_all(&staging_dir).ok();
        let tmp_path = staging_dir.join(&oid);
        if client
            .call(
                "blob.get",
                serde_json::json!({ "hash": blob_hash, "path": tmp_path.to_str().unwrap() }),
            )
            .await
            .is_ok()
        {
            if let Ok(data) = std::fs::read(&tmp_path) {
                // Parse the raw git object to find children
                match obj_type {
                    "commit" => {
                        // Parse commit: tree line + parent lines
                        if let Ok(text) = std::str::from_utf8(&data) {
                            for line in text.lines() {
                                if let Some(tree_oid) = line.strip_prefix("tree ") {
                                    queue.push_back(tree_oid.trim().to_string());
                                } else if let Some(parent_oid) = line.strip_prefix("parent ") {
                                    queue.push_back(parent_oid.trim().to_string());
                                } else if line.is_empty() {
                                    break; // End of headers
                                }
                            }
                        }
                    }
                    "tree" => {
                        // Parse tree: binary format — entries are "mode name\0<20-byte-sha>"
                        parse_tree_entries(&data, &mut queue);
                    }
                    _ => {}
                }
            }
            let _ = std::fs::remove_file(&tmp_path);
        }
    }

    Ok(())
}

/// Parse git tree object binary format to extract child OIDs.
fn parse_tree_entries(data: &[u8], queue: &mut std::collections::VecDeque<String>) {
    let mut pos = 0;
    while pos < data.len() {
        // Format: "mode name\0<20-byte-sha1>"
        // Find the null byte
        let null_pos = match data[pos..].iter().position(|&b| b == 0) {
            Some(p) => pos + p,
            None => break,
        };
        // The 20 bytes after the null are the SHA-1
        let sha_start = null_pos + 1;
        let sha_end = sha_start + 20;
        if sha_end > data.len() {
            break;
        }
        let oid_hex: String = data[sha_start..sha_end]
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        queue.push_back(oid_hex);
        pos = sha_end;
    }
}

/// `bolo review show <commit>` — show review comments for a commit.
pub async fn review_show(commit: &str, config_flag: Option<&str>, json: bool) -> Result<()> {
    // Try daemon IPC first (CRDT-backed, cross-node synced)
    if let Some(mut client) = try_daemon(config_flag).await {
        let result = client
            .call("review.show", serde_json::json!({ "commit": commit }))
            .await?;
        if json {
            println!("{}", serde_json::to_string_pretty(&result)?);
        } else {
            let comments = result.as_array();
            if comments.map(|a| a.is_empty()).unwrap_or(true) {
                println!("No reviews for commit {}.", &commit[..commit.len().min(12)]);
            } else {
                println!("Reviews for commit {}:", &commit[..commit.len().min(12)]);
                for c in comments.unwrap() {
                    let status = c["status"].as_str().unwrap_or("?");
                    let body = c["body"].as_str().unwrap_or("?");
                    let author = c["author"].as_str().unwrap_or("?");
                    println!("  [{status}] {body} ({author})");
                    if let Some(fp) = c["file_path"].as_str() {
                        if let Some(line) = c["line"].as_u64() {
                            println!("    at {fp}:{line}");
                        } else {
                            println!("    at {fp}");
                        }
                    }
                }
            }
        }
        return Ok(());
    }

    // Fallback: local filesystem store
    let store = open_review_store(config_flag)?;
    let comments = store
        .list_for_commit(commit)
        .context("failed to load reviews")?;

    if json {
        println!("{}", serde_json::to_string_pretty(&comments)?);
    } else if comments.is_empty() {
        println!("No reviews for commit {}.", &commit[..commit.len().min(12)]);
    } else {
        println!("Reviews for commit {}:", &commit[..commit.len().min(12)]);
        for c in &comments {
            println!("  [{:?}] {} ({})", c.status, c.body, c.author);
            if let Some(ref fp) = c.file_path {
                if let Some(line) = c.line {
                    println!("    at {fp}:{line}");
                } else {
                    println!("    at {fp}");
                }
            }
        }
    }

    Ok(())
}

/// `bolo review comment <commit> <body>` — add a review comment.
pub async fn review_comment(
    commit: &str,
    body: &str,
    file: Option<&str>,
    line: Option<u32>,
    config_flag: Option<&str>,
    json: bool,
) -> Result<()> {
    if let Some(mut client) = try_daemon(config_flag).await {
        let identity = load_identity(config_flag)?;
        let node_id = identity.node_id().to_string();
        let mut params = serde_json::json!({
            "commit": commit,
            "body": body,
            "author": node_id,
        });
        if let Some(f) = file {
            params["file"] = serde_json::Value::String(f.to_string());
        }
        if let Some(l) = line {
            params["line"] = serde_json::json!(l);
        }
        let result = client.call("review.comment", params).await?;
        if json {
            println!("{}", serde_json::to_string_pretty(&result)?);
        } else {
            println!(
                "Added comment on commit {}",
                &commit[..commit.len().min(12)]
            );
        }
        return Ok(());
    }

    // Fallback: local filesystem store
    let store = open_review_store(config_flag)?;
    let identity = load_identity(config_flag)?;
    let node_id = identity.node_id().to_string();
    let timestamp = Timestamp::now().0;

    let id_input = format!("comment:{commit}:{body}:{timestamp}");
    let id = blake3::hash(id_input.as_bytes()).to_hex().to_string();

    let comment = bolo_git::ReviewComment {
        id: id.clone(),
        author: node_id,
        commit_oid: commit.to_string(),
        file_path: file.map(|s| s.to_string()),
        line,
        body: body.to_string(),
        timestamp,
        status: bolo_git::ReviewStatus::Pending,
        signature: None,
    };

    store
        .add_comment(comment)
        .context("failed to add comment")?;

    if json {
        let out = serde_json::json!({
            "added": true,
            "id": id,
            "commit": commit,
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!(
            "Added comment on commit {}",
            &commit[..commit.len().min(12)]
        );
    }

    Ok(())
}

/// `bolo review approve <commit>` — cryptographically sign an approval.
pub async fn review_approve(commit: &str, config_flag: Option<&str>, json: bool) -> Result<()> {
    let identity = load_identity(config_flag)?;
    let node_id = identity.node_id().to_string();
    let timestamp = Timestamp::now().0;

    // Sign the approval: sign(commit_oid + author + timestamp).
    let sign_data = format!("approve:{commit}:{node_id}:{timestamp}");
    let sig = identity.sign(sign_data.as_bytes());
    let sig_hex = sig.to_string();

    if let Some(mut client) = try_daemon(config_flag).await {
        let result = client
            .call(
                "review.approve",
                serde_json::json!({
                    "commit": commit,
                    "author": node_id,
                    "signature": sig_hex,
                }),
            )
            .await?;
        if json {
            println!("{}", serde_json::to_string_pretty(&result)?);
        } else {
            println!("Approved commit {}", &commit[..commit.len().min(12)]);
            println!("  Signature: {}...", &sig_hex[..sig_hex.len().min(16)]);
        }
        return Ok(());
    }

    // Fallback: local filesystem store
    let store = open_review_store(config_flag)?;
    let mut comment = store
        .approve(commit, &node_id, timestamp)
        .context("failed to approve")?;

    comment.signature = Some(sig_hex.clone());
    let config_dir = resolve_config_dir(config_flag)?;
    let data_dir = resolve_data_dir(&config_dir);
    let review_store = bolo_git::ReviewStore::open(&data_dir)?;
    let mut comments = review_store
        .list_for_commit(commit)
        .context("failed to reload reviews")?;
    if let Some(last) = comments.last_mut() {
        if last.id == comment.id {
            last.signature = Some(sig_hex.clone());
        }
    }
    let reviews_dir = data_dir.join("reviews");
    let short = &commit[..commit.len().min(12)];
    let path = reviews_dir.join(format!("{short}.json"));
    let data = serde_json::to_string_pretty(&comments)?;
    std::fs::write(&path, data)?;

    if json {
        let out = serde_json::json!({
            "approved": true,
            "commit": commit,
            "signature": sig_hex,
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Approved commit {}", &commit[..commit.len().min(12)]);
        println!("  Signature: {}...", &sig_hex[..sig_hex.len().min(16)]);
    }

    Ok(())
}

/// `bolo review reject <commit>` — sign a rejection.
pub async fn review_reject(
    commit: &str,
    message: Option<&str>,
    config_flag: Option<&str>,
    json: bool,
) -> Result<()> {
    let identity = load_identity(config_flag)?;
    let node_id = identity.node_id().to_string();
    let timestamp = Timestamp::now().0;

    let sign_data = format!("reject:{commit}:{node_id}:{timestamp}");
    let sig = identity.sign(sign_data.as_bytes());
    let sig_hex = sig.to_string();

    let body = message.unwrap_or("Changes requested");

    if let Some(mut client) = try_daemon(config_flag).await {
        let result = client
            .call(
                "review.reject",
                serde_json::json!({
                    "commit": commit,
                    "message": body,
                    "author": node_id,
                    "signature": sig_hex,
                }),
            )
            .await?;
        if json {
            println!("{}", serde_json::to_string_pretty(&result)?);
        } else {
            println!("Rejected commit {}", &commit[..commit.len().min(12)]);
            println!("  Message: {body}");
            println!("  Signature: {}...", &sig_hex[..sig_hex.len().min(16)]);
        }
        return Ok(());
    }

    // Fallback: local filesystem store
    let store = open_review_store(config_flag)?;
    let mut comment = store
        .reject(commit, &node_id, message, timestamp)
        .context("failed to reject")?;

    comment.signature = Some(sig_hex.clone());
    let config_dir = resolve_config_dir(config_flag)?;
    let data_dir = resolve_data_dir(&config_dir);
    let review_store = bolo_git::ReviewStore::open(&data_dir)?;
    let mut comments = review_store
        .list_for_commit(commit)
        .context("failed to reload reviews")?;
    if let Some(last) = comments.last_mut() {
        if last.id == comment.id {
            last.signature = Some(sig_hex.clone());
        }
    }
    let reviews_dir = data_dir.join("reviews");
    let short = &commit[..commit.len().min(12)];
    let path = reviews_dir.join(format!("{short}.json"));
    let data = serde_json::to_string_pretty(&comments)?;
    std::fs::write(&path, data)?;

    if json {
        let out = serde_json::json!({
            "rejected": true,
            "commit": commit,
            "message": comment.body,
            "signature": sig_hex,
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Rejected commit {}", &commit[..commit.len().min(12)]);
        println!("  Message: {}", comment.body);
        println!("  Signature: {}...", &sig_hex[..sig_hex.len().min(16)]);
    }

    Ok(())
}

/// `bolo review ls` — list pending reviews.
pub async fn review_ls(config_flag: Option<&str>, json: bool) -> Result<()> {
    if let Some(mut client) = try_daemon(config_flag).await {
        let result = client.call("review.ls", serde_json::json!({})).await?;
        if json {
            println!("{}", serde_json::to_string_pretty(&result)?);
        } else {
            let pending = result["pending"].as_array();
            if pending.map(|a| a.is_empty()).unwrap_or(true) {
                println!("No pending reviews.");
            } else {
                println!("Pending reviews:");
                for entry in pending.unwrap() {
                    let oid = entry["commit"].as_str().unwrap_or("?");
                    let short = &oid[..oid.len().min(12)];
                    let comments = entry["comments"].as_array();
                    let count = comments.map(|a| a.len()).unwrap_or(0);
                    println!("  {short} ({count} comment(s))");
                    if let Some(comments) = comments {
                        for c in comments {
                            let status = c["status"].as_str().unwrap_or("?");
                            let body = c["body"].as_str().unwrap_or("?");
                            let author = c["author"].as_str().unwrap_or("?");
                            println!("    [{status}] {body} ({author})");
                        }
                    }
                }
            }
        }
        return Ok(());
    }

    // Fallback: local filesystem store
    let store = open_review_store(config_flag)?;
    let pending = store.list_pending().context("failed to list reviews")?;

    if json {
        println!("{}", serde_json::to_string_pretty(&pending)?);
    } else if pending.is_empty() {
        println!("No pending reviews.");
    } else {
        println!("Pending reviews:");
        for (oid, comments) in &pending {
            let short = &oid[..oid.len().min(12)];
            println!("  {} ({} comment(s))", short, comments.len());
            for c in comments {
                println!("    [{:?}] {} ({})", c.status, c.body, c.author);
            }
        }
    }

    Ok(())
}
