use anyhow::{Context, Result};
use bolo_task::{Board, Priority, Task, TaskStatus, TaskStore};

fn open_task_store(config_flag: Option<&str>) -> Result<TaskStore> {
    let config_dir = super::daemon::resolve_config_dir(config_flag)?;
    let data_dir = super::daemon::resolve_data_dir(&config_dir);
    TaskStore::open(&data_dir).context("failed to open task store")
}

/// Try to connect to the daemon. Returns None if daemon is not running.
async fn try_daemon(config_flag: Option<&str>) -> Option<bolo_core::ipc::DaemonClient> {
    let config_dir = super::daemon::resolve_config_dir(config_flag).ok()?;
    bolo_core::ipc::DaemonClient::connect(&config_dir)
        .await
        .ok()
}

/// `bolo task create <title>` — create a task.
pub async fn create(
    title: &str,
    priority: Option<&str>,
    assignee: Option<&str>,
    config_flag: Option<&str>,
    json: bool,
) -> Result<()> {
    // Try IPC first (daemon has CRDT-backed sync)
    if let Some(mut client) = try_daemon(config_flag).await {
        let mut params = serde_json::json!({ "title": title });
        if let Some(p) = priority {
            params["priority"] = serde_json::Value::String(p.to_string());
        }
        if let Some(a) = assignee {
            params["assignee"] = serde_json::Value::String(a.to_string());
        }
        let result = client.call("task.create", params).await?;
        if json {
            println!("{}", serde_json::to_string_pretty(&result)?);
        } else {
            let id = result["id"].as_str().unwrap_or("?");
            let title = result["title"].as_str().unwrap_or("?");
            println!("Created task: {id} ({title})");
        }
        return Ok(());
    }

    // Fallback: local filesystem store
    let store = open_task_store(config_flag)?;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    let task = Task {
        id: String::new(),
        title: title.to_string(),
        status: TaskStatus::Backlog,
        assignee: assignee.map(|s| s.to_string()),
        priority: Priority::from_str_or_default(priority),
        spec_doc: None,
        dependencies: Vec::new(),
        commits: Vec::new(),
        ci_results: Vec::new(),
        review_doc: None,
        created_by: "local".to_string(),
        created_at: now,
        updated_at: now,
        claimed_by: None,
        claimed_at: None,
    };
    let task = store.create(task)?;

    if json {
        println!("{}", serde_json::to_string_pretty(&task)?);
    } else {
        println!("Created task: {} ({})", task.id, task.title);
    }
    Ok(())
}

/// `bolo task assign <id> <peer>` — assign to human or agent.
pub async fn assign(id: &str, peer: &str, config_flag: Option<&str>, json: bool) -> Result<()> {
    if let Some(mut client) = try_daemon(config_flag).await {
        let result = client
            .call(
                "task.update",
                serde_json::json!({ "id": id, "assignee": peer }),
            )
            .await?;
        if json {
            println!("{}", serde_json::to_string_pretty(&result)?);
        } else {
            println!("Assigned {id} to {peer}");
        }
        return Ok(());
    }

    let store = open_task_store(config_flag)?;
    let mut task = store.load(id)?;
    task.assignee = Some(peer.to_string());
    task.updated_at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    store.save(&task)?;

    if json {
        println!("{}", serde_json::to_string_pretty(&task)?);
    } else {
        println!("Assigned {} to {peer}", task.id);
    }
    Ok(())
}

/// `bolo task list` — show board view.
pub async fn list(
    status_filter: Option<&str>,
    config_flag: Option<&str>,
    json: bool,
) -> Result<()> {
    if let Some(mut client) = try_daemon(config_flag).await {
        let result = client.call("task.list", serde_json::json!({})).await?;
        if json {
            println!("{}", serde_json::to_string_pretty(&result)?);
        } else {
            // Parse board from IPC response and display
            let columns = [
                ("BACKLOG", "backlog"),
                ("READY", "ready"),
                ("IN-PROGRESS", "in_progress"),
                ("REVIEW", "review"),
                ("DONE", "done"),
            ];
            let mut total = 0usize;
            for (_name, key) in &columns {
                if let Some(tasks) = result.get(key).and_then(|v| v.as_array()) {
                    total += tasks.len();
                }
            }
            if total == 0 {
                println!("No tasks.");
                return Ok(());
            }
            for (name, key) in columns {
                if let Some(filter) = status_filter {
                    if !name.eq_ignore_ascii_case(filter) {
                        continue;
                    }
                }
                if let Some(tasks) = result.get(key).and_then(|v| v.as_array()) {
                    if !tasks.is_empty() {
                        println!("\n{name} ({}):", tasks.len());
                        for t in tasks {
                            let id = t["id"].as_str().unwrap_or("?");
                            let priority = t["priority"].as_str().unwrap_or("medium");
                            let title = t["title"].as_str().unwrap_or("?");
                            let assignee = t["assignee"].as_str().unwrap_or("unassigned");
                            println!("  {id} [{priority}] {title} (@{assignee})");
                        }
                    }
                }
            }
        }
        return Ok(());
    }

    let store = open_task_store(config_flag)?;
    let board = Board::from_store(&store)?;

    if json {
        println!("{}", serde_json::to_string_pretty(&board)?);
    } else if board.total() == 0 {
        println!("No tasks.");
    } else {
        let columns: Vec<(&str, &[Task])> = vec![
            ("BACKLOG", &board.backlog),
            ("READY", &board.ready),
            ("IN-PROGRESS", &board.in_progress),
            ("REVIEW", &board.review),
            ("DONE", &board.done),
        ];
        for (name, tasks) in columns {
            if let Some(filter) = status_filter {
                if !name.eq_ignore_ascii_case(filter) {
                    continue;
                }
            }
            if !tasks.is_empty() {
                println!("\n{name} ({}):", tasks.len());
                for t in tasks.iter() {
                    let assignee = t.assignee.as_deref().unwrap_or("unassigned");
                    let priority = &t.priority;
                    println!("  {} [{}] {} (@{})", t.id, priority, t.title, assignee);
                }
            }
        }
    }
    Ok(())
}

/// `bolo task update <id> --status <status>` — update task status.
pub async fn update(
    id: &str,
    status: Option<&str>,
    priority: Option<&str>,
    config_flag: Option<&str>,
    json: bool,
) -> Result<()> {
    if let Some(mut client) = try_daemon(config_flag).await {
        let mut params = serde_json::json!({ "id": id });
        if let Some(s) = status {
            params["status"] = serde_json::Value::String(s.to_string());
        }
        if let Some(p) = priority {
            params["priority"] = serde_json::Value::String(p.to_string());
        }
        let result = client.call("task.update", params).await?;
        if json {
            println!("{}", serde_json::to_string_pretty(&result)?);
        } else {
            let s = result["status"].as_str().unwrap_or("?");
            let p = result["priority"].as_str().unwrap_or("?");
            println!("Updated task {id}: status={s}, priority={p}");
        }
        return Ok(());
    }

    let store = open_task_store(config_flag)?;
    let mut task = store.load(id)?;

    if let Some(s) = status {
        task.status = match s {
            "backlog" => TaskStatus::Backlog,
            "ready" => TaskStatus::Ready,
            "in-progress" => TaskStatus::InProgress,
            "review" => TaskStatus::Review,
            "done" => TaskStatus::Done,
            other => anyhow::bail!(
                "invalid status: {other}. Valid: backlog, ready, in-progress, review, done"
            ),
        };
    }
    if let Some(p) = priority {
        task.priority = Priority::from_str_or_default(Some(p));
    }
    task.updated_at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    store.save(&task)?;

    if json {
        println!("{}", serde_json::to_string_pretty(&task)?);
    } else {
        println!(
            "Updated task {}: status={}, priority={}",
            task.id, task.status, task.priority
        );
    }
    Ok(())
}

/// `bolo task link <id>` — link artifacts.
pub async fn link(
    id: &str,
    spec: Option<&str>,
    commit: Option<&str>,
    ci_result: Option<&str>,
    config_flag: Option<&str>,
    json: bool,
) -> Result<()> {
    if let Some(mut client) = try_daemon(config_flag).await {
        let mut params = serde_json::json!({ "id": id });
        if let Some(s) = spec {
            params["spec_doc"] = serde_json::Value::String(s.to_string());
        }
        if let Some(c) = commit {
            params["commit"] = serde_json::Value::String(c.to_string());
        }
        if let Some(r) = ci_result {
            params["ci_result"] = serde_json::Value::String(r.to_string());
        }
        let result = client.call("task.update", params).await?;
        if json {
            println!("{}", serde_json::to_string_pretty(&result)?);
        } else {
            println!("Linked artifacts to task {id}");
        }
        return Ok(());
    }

    let store = open_task_store(config_flag)?;
    let mut task = store.load(id)?;

    if let Some(s) = spec {
        task.spec_doc = Some(s.to_string());
    }
    if let Some(c) = commit {
        if !task.commits.contains(&c.to_string()) {
            task.commits.push(c.to_string());
        }
    }
    if let Some(r) = ci_result {
        if !task.ci_results.contains(&r.to_string()) {
            task.ci_results.push(r.to_string());
        }
    }
    task.updated_at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    store.save(&task)?;

    if json {
        println!("{}", serde_json::to_string_pretty(&task)?);
    } else {
        println!("Linked artifacts to task {}", task.id);
    }
    Ok(())
}

/// `bolo task show <id>` — show task details.
pub async fn show(id: &str, config_flag: Option<&str>, json: bool) -> Result<()> {
    if let Some(mut client) = try_daemon(config_flag).await {
        let result = client
            .call("task.show", serde_json::json!({ "id": id }))
            .await?;
        if json {
            println!("{}", serde_json::to_string_pretty(&result)?);
        } else {
            let title = result["title"].as_str().unwrap_or("?");
            let status = result["status"].as_str().unwrap_or("?");
            let priority = result["priority"].as_str().unwrap_or("?");
            let assignee = result["assignee"].as_str().unwrap_or("unassigned");
            println!("Task: {id} — {title}");
            println!("  Status:   {status}");
            println!("  Priority: {priority}");
            println!("  Assignee: {assignee}");
            if let Some(spec) = result["spec_doc"].as_str() {
                println!("  Spec:     {spec}");
            }
            if let Some(deps) = result["dependencies"].as_array() {
                if !deps.is_empty() {
                    let dep_strs: Vec<&str> = deps.iter().filter_map(|v| v.as_str()).collect();
                    println!("  Deps:     {}", dep_strs.join(", "));
                }
            }
            if let Some(commits) = result["commits"].as_array() {
                if !commits.is_empty() {
                    let c: Vec<&str> = commits.iter().filter_map(|v| v.as_str()).collect();
                    println!("  Commits:  {}", c.join(", "));
                }
            }
        }
        return Ok(());
    }

    let store = open_task_store(config_flag)?;
    let task = store.load(id)?;

    if json {
        println!("{}", serde_json::to_string_pretty(&task)?);
    } else {
        println!("Task: {} — {}", task.id, task.title);
        println!("  Status:   {}", task.status);
        println!("  Priority: {}", task.priority);
        println!(
            "  Assignee: {}",
            task.assignee.as_deref().unwrap_or("unassigned")
        );
        if let Some(ref spec) = task.spec_doc {
            println!("  Spec:     {spec}");
        }
        if !task.dependencies.is_empty() {
            println!("  Deps:     {}", task.dependencies.join(", "));
        }
        if !task.commits.is_empty() {
            println!("  Commits:  {}", task.commits.join(", "));
        }
    }
    Ok(())
}

/// `bolo task claim <id>` — atomic claim-or-fail for agent coordination.
pub async fn claim(id: &str, ttl_secs: u64, config_flag: Option<&str>, json: bool) -> Result<()> {
    if let Some(mut client) = try_daemon(config_flag).await {
        let result = client
            .call(
                "task.claim",
                serde_json::json!({
                    "id": id,
                    "ttl_ms": ttl_secs * 1000,
                }),
            )
            .await?;
        if json {
            println!("{}", serde_json::to_string_pretty(&result)?);
        } else if result["claimed"].as_bool() == Some(true) {
            let status = result["status"].as_str().unwrap_or("?");
            println!("Claimed task {id} (status={status}, ttl={ttl_secs}s)");
        } else {
            let current = result["current_claimer"].as_str().unwrap_or("unknown");
            let short = &current[..current.len().min(16)];
            println!("Claim conflict: task {id} already claimed by {short}");
        }
        return Ok(());
    }

    anyhow::bail!("task.claim requires a running daemon (CRDT-backed coordination)")
}

/// `bolo task release <id>` — release a claimed task.
pub async fn release(id: &str, config_flag: Option<&str>, json: bool) -> Result<()> {
    if let Some(mut client) = try_daemon(config_flag).await {
        let result = client
            .call("task.release", serde_json::json!({ "id": id }))
            .await?;
        if json {
            println!("{}", serde_json::to_string_pretty(&result)?);
        } else {
            println!("Released task {id}");
        }
        return Ok(());
    }

    anyhow::bail!("task.release requires a running daemon (CRDT-backed coordination)")
}

/// `bolo task delete <id>` — delete a task.
pub async fn delete(id: &str, config_flag: Option<&str>, json: bool) -> Result<()> {
    if let Some(mut client) = try_daemon(config_flag).await {
        let result = client
            .call("task.delete", serde_json::json!({ "id": id }))
            .await?;
        if json {
            println!("{}", serde_json::to_string_pretty(&result)?);
        } else {
            println!("Deleted task {id}");
        }
        return Ok(());
    }

    let store = open_task_store(config_flag)?;
    store.delete(id)?;

    if json {
        println!("{}", serde_json::json!({"deleted": true, "id": id}));
    } else {
        println!("Deleted task {id}");
    }
    Ok(())
}
