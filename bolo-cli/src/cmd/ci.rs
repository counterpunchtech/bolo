use anyhow::{Context, Result};
use bolo_ci::{BuildStatus, BuildTask, CiStore, TaskType, Verification};

fn open_ci_store(config_flag: Option<&str>) -> Result<CiStore> {
    let config_dir = super::daemon::resolve_config_dir(config_flag)?;
    let data_dir = super::daemon::resolve_data_dir(&config_dir);
    CiStore::open(&data_dir).context("failed to open CI store")
}

/// `bolo ci run` -- trigger a CI build.
///
/// When daemon is running, broadcasts the task to the mesh for distributed execution.
/// Falls back to local-only execution when daemon is not available.
pub async fn run(
    task_type: Option<&str>,
    path: Option<&str>,
    config_flag: Option<&str>,
    json: bool,
) -> Result<()> {
    let config_dir = super::daemon::resolve_config_dir(config_flag)?;
    let work_dir = std::path::Path::new(path.unwrap_or("."));

    // Determine source tree identifier
    let source_tree = if let Ok(bridge) = bolo_git::GitBridge::discover(work_dir) {
        bridge
            .status()
            .ok()
            .and_then(|s| s.head_oid)
            .unwrap_or_else(|| "unknown".into())
    } else {
        "local".to_string()
    };

    let task_type_str = task_type.unwrap_or("full");

    // Try daemon IPC first — this broadcasts to mesh for distributed execution
    if let Ok(mut client) = bolo_core::ipc::DaemonClient::connect(&config_dir).await {
        let result = client
            .call(
                "ci.run",
                serde_json::json!({
                    "task_type": task_type_str,
                    "source_tree": source_tree,
                }),
            )
            .await
            .context("IPC ci.run failed")?;

        let task_id = result["task_id"].as_str().unwrap_or("unknown");

        if !json {
            println!("CI task created: {task_id}");
            println!("Broadcast to mesh — peers will build.");
            println!("Also running locally...");
        }

        // Run locally too (the triggering node also builds)
        let local_result = run_local(task_type_str, &source_tree, work_dir, config_flag).await?;

        if json {
            let out = serde_json::json!({
                "task_id": task_id,
                "broadcast": true,
                "local_passed": local_result.passed,
                "local_duration_ms": local_result.duration_ms,
                "local_summary": local_result.summary,
                "test_results": local_result.test_results,
            });
            println!("{}", serde_json::to_string_pretty(&out)?);
        } else {
            let status = if local_result.passed {
                "PASSED"
            } else {
                "FAILED"
            };
            println!("\nLocal: {status} in {}ms", local_result.duration_ms);
            if let Some(ref tr) = local_result.test_results {
                println!(
                    "  Tests: {} passed, {} failed, {} ignored",
                    tr.passed, tr.failed, tr.ignored
                );
                for f in &tr.failures {
                    println!("  FAIL: {}", f.name);
                }
            }
        }
        return Ok(());
    }

    // Fallback: local-only execution
    if !json {
        println!("Daemon not running — running locally only (no mesh broadcast).");
    }

    let result = run_local(task_type_str, &source_tree, work_dir, config_flag).await?;

    if json {
        let out = serde_json::json!({
            "task_id": result.task_id,
            "passed": result.passed,
            "duration_ms": result.duration_ms,
            "summary": result.summary,
            "test_results": result.test_results,
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        let status = if result.passed { "PASSED" } else { "FAILED" };
        println!("\n{status} in {}ms", result.duration_ms);
        if let Some(ref tr) = result.test_results {
            println!(
                "  Tests: {} passed, {} failed, {} ignored",
                tr.passed, tr.failed, tr.ignored
            );
            for f in &tr.failures {
                println!("  FAIL: {}", f.name);
            }
        }
    }

    Ok(())
}

/// Run a build task locally and store the result.
async fn run_local(
    task_type_str: &str,
    source_tree: &str,
    work_dir: &std::path::Path,
    config_flag: Option<&str>,
) -> Result<bolo_ci::BuildResult> {
    let store = open_ci_store(config_flag)?;

    let task_type = match task_type_str {
        "build" => TaskType::Build,
        "test" => TaskType::Test,
        "check" => TaskType::Check,
        "clippy" => TaskType::Clippy,
        "fmt" => TaskType::Fmt,
        "full" => TaskType::Full,
        other => anyhow::bail!("unknown task type: {other}"),
    };

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    let task = BuildTask {
        id: String::new(),
        task_type,
        source_tree: source_tree.to_string(),
        config_hash: None,
        rust_version: None,
        targets: vec![std::env::consts::ARCH.to_string()],
        status: BuildStatus::Pending,
        verification: Verification::default(),
        triggered_by: "local".to_string(),
        created_at: now,
        updated_at: now,
    };

    let mut task = store.create_task(task)?;

    task.status = BuildStatus::Running {
        peer: "local".to_string(),
    };
    task.updated_at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    store.update_task(&task)?;

    let result = bolo_ci::runner::run_task(&task, work_dir).await;

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    task.status = if result.passed {
        BuildStatus::Passed {
            peer: "local".to_string(),
            duration_ms: result.duration_ms,
        }
    } else {
        BuildStatus::Failed {
            peer: "local".to_string(),
            duration_ms: result.duration_ms,
            error: result.summary.clone(),
        }
    };
    task.updated_at = now;
    store.update_task(&task)?;
    store.save_result(&result)?;

    Ok(result)
}

/// `bolo ci status` -- show CI build status.
///
/// Routes through daemon IPC when available.
pub async fn status(config_flag: Option<&str>, json: bool) -> Result<()> {
    let config_dir = super::daemon::resolve_config_dir(config_flag)?;

    // Try daemon IPC
    if let Ok(mut client) = bolo_core::ipc::DaemonClient::connect(&config_dir).await {
        let result = client
            .call("ci.status", serde_json::json!({}))
            .await
            .context("IPC ci.status failed")?;

        if json {
            println!("{}", serde_json::to_string_pretty(&result)?);
        } else {
            let tasks = result["tasks"].as_array();
            if tasks.is_none_or(|t| t.is_empty()) {
                println!("No CI tasks.");
            } else {
                for t in tasks.unwrap() {
                    println!(
                        "{} {} {}",
                        t["id"].as_str().unwrap_or("?"),
                        t["source_tree"].as_str().unwrap_or("?"),
                        t["status"].as_str().unwrap_or("?"),
                    );
                }
            }
        }
        return Ok(());
    }

    // Fallback: direct store
    let store = open_ci_store(config_flag)?;
    let tasks = store.list_tasks()?;

    if json {
        println!("{}", serde_json::to_string_pretty(&tasks)?);
    } else if tasks.is_empty() {
        println!("No CI tasks.");
    } else {
        for task in &tasks {
            let status_str = match &task.status {
                BuildStatus::Pending => "PENDING".to_string(),
                BuildStatus::Running { peer } => format!("RUNNING ({peer})"),
                BuildStatus::Passed { duration_ms, .. } => format!("PASSED ({duration_ms}ms)"),
                BuildStatus::Failed { error, .. } => format!("FAILED: {error}"),
                BuildStatus::Cancelled => "CANCELLED".to_string(),
            };
            println!("{} {} {status_str}", task.id, task.source_tree);
        }
    }
    Ok(())
}

/// `bolo ci results <task-hash>` -- show build results.
///
/// Routes through daemon IPC when available.
pub async fn results(task_id: &str, config_flag: Option<&str>, json: bool) -> Result<()> {
    let config_dir = super::daemon::resolve_config_dir(config_flag)?;

    // Try daemon IPC
    if let Ok(mut client) = bolo_core::ipc::DaemonClient::connect(&config_dir).await {
        let result = client
            .call("ci.results", serde_json::json!({ "task_id": task_id }))
            .await
            .context("IPC ci.results failed")?;

        if json {
            println!("{}", serde_json::to_string_pretty(&result)?);
        } else {
            let results = result["results"].as_array();
            if results.is_none_or(|r| r.is_empty()) {
                println!("No results for task {task_id}.");
            } else {
                for r in results.unwrap() {
                    let passed = r["passed"].as_bool().unwrap_or(false);
                    let status = if passed { "PASSED" } else { "FAILED" };
                    println!(
                        "  {} by {} in {}ms: {}",
                        status,
                        r["peer"].as_str().unwrap_or("?"),
                        r["duration_ms"].as_u64().unwrap_or(0),
                        r["summary"].as_str().unwrap_or(""),
                    );
                }
            }
        }
        return Ok(());
    }

    // Fallback: direct store
    let store = open_ci_store(config_flag)?;
    let task = store.load_task(task_id)?;
    let results = store.load_results(task_id)?;

    if json {
        let out = serde_json::json!({
            "task": task,
            "results": results,
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("Task: {} ({})", task.id, task.source_tree);
        if results.is_empty() {
            println!("  No results yet.");
        } else {
            for r in &results {
                let status = if r.passed { "PASSED" } else { "FAILED" };
                println!(
                    "  {} by {} in {}ms: {}",
                    status, r.peer, r.duration_ms, r.summary
                );
                if let Some(ref tr) = r.test_results {
                    println!(
                        "    Tests: {} passed, {} failed, {} ignored",
                        tr.passed, tr.failed, tr.ignored
                    );
                }
            }
        }
    }
    Ok(())
}
