use crate::types::*;

/// Run a build task locally and produce a result.
pub async fn run_task(task: &BuildTask, work_dir: &std::path::Path) -> BuildResult {
    let start = std::time::Instant::now();
    let peer = "local".to_string();

    let cmd_result = match task.task_type {
        TaskType::Check => run_cargo(work_dir, &["check", "--workspace"]).await,
        TaskType::Build => run_cargo(work_dir, &["build", "--workspace"]).await,
        TaskType::Test => run_cargo(work_dir, &["test", "--workspace"]).await,
        TaskType::Clippy => {
            run_cargo(work_dir, &["clippy", "--workspace", "--", "-D", "warnings"]).await
        }
        TaskType::Fmt => run_cargo(work_dir, &["fmt", "--all", "--", "--check"]).await,
        TaskType::Full => run_full(work_dir).await,
    };

    let duration_ms = start.elapsed().as_millis() as u64;
    let (passed, summary, output, test_results) = match cmd_result {
        Ok((stdout, stderr)) => {
            let test_results = if matches!(task.task_type, TaskType::Test | TaskType::Full) {
                parse_test_output(&stdout)
            } else {
                None
            };
            (
                true,
                "passed".to_string(),
                format!("{stdout}\n{stderr}"),
                test_results,
            )
        }
        Err((stdout, stderr, msg)) => {
            let test_results = if matches!(task.task_type, TaskType::Test | TaskType::Full) {
                parse_test_output(&stdout)
            } else {
                None
            };
            (false, msg, format!("{stdout}\n{stderr}"), test_results)
        }
    };

    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    BuildResult {
        task_id: task.id.clone(),
        peer,
        passed,
        duration_ms,
        summary,
        output,
        test_results,
        artifacts: Vec::new(),
        timestamp,
    }
}

async fn run_cargo(
    work_dir: &std::path::Path,
    args: &[&str],
) -> Result<(String, String), (String, String, String)> {
    let output = tokio::process::Command::new("cargo")
        .args(args)
        .current_dir(work_dir)
        .output()
        .await
        .map_err(|e| {
            (
                String::new(),
                String::new(),
                format!("failed to run cargo: {e}"),
            )
        })?;

    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();

    if output.status.success() {
        Ok((stdout, stderr))
    } else {
        Err((
            stdout,
            stderr,
            format!("cargo {} failed (exit {})", args[0], output.status),
        ))
    }
}

async fn run_full(
    work_dir: &std::path::Path,
) -> Result<(String, String), (String, String, String)> {
    let steps = [
        &["fmt", "--all", "--", "--check"][..],
        &["check", "--workspace"],
        &["clippy", "--workspace", "--", "-D", "warnings"],
        &["test", "--workspace"],
    ];
    let mut all_stdout = String::new();
    let mut all_stderr = String::new();
    for step in steps {
        match run_cargo(work_dir, step).await {
            Ok((stdout, stderr)) => {
                all_stdout.push_str(&stdout);
                all_stderr.push_str(&stderr);
            }
            Err((stdout, stderr, msg)) => {
                all_stdout.push_str(&stdout);
                all_stderr.push_str(&stderr);
                return Err((all_stdout, all_stderr, msg));
            }
        }
    }
    Ok((all_stdout, all_stderr))
}

/// Parse cargo test output to extract test counts.
fn parse_test_output(output: &str) -> Option<TestResults> {
    // Look for lines like: "test result: ok. 5 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out"
    let mut total_passed = 0u32;
    let mut total_failed = 0u32;
    let mut total_ignored = 0u32;
    let mut failures = Vec::new();
    let mut found = false;

    for line in output.lines() {
        if line.starts_with("test result:") {
            found = true;
            // Parse: "test result: ok. X passed; Y failed; Z ignored; ..."
            for part in line.split(';') {
                let part = part.trim();
                if let Some(n) = part.strip_suffix(" passed") {
                    if let Ok(n) = n.split_whitespace().last().unwrap_or("0").parse::<u32>() {
                        total_passed += n;
                    }
                } else if let Some(n) = part.strip_suffix(" failed") {
                    if let Ok(n) = n.split_whitespace().last().unwrap_or("0").parse::<u32>() {
                        total_failed += n;
                    }
                } else if let Some(n) = part.strip_suffix(" ignored") {
                    if let Ok(n) = n.split_whitespace().last().unwrap_or("0").parse::<u32>() {
                        total_ignored += n;
                    }
                }
            }
        }
        // Capture failure names: "test some::test ... FAILED"
        if line.starts_with("test ") && line.ends_with("FAILED") {
            let name = line
                .strip_prefix("test ")
                .unwrap_or(line)
                .strip_suffix(" ... FAILED")
                .unwrap_or(line)
                .trim()
                .to_string();
            failures.push(TestFailure {
                name,
                message: String::new(),
            });
        }
    }

    if found {
        Some(TestResults {
            total: total_passed + total_failed + total_ignored,
            passed: total_passed,
            failed: total_failed,
            ignored: total_ignored,
            failures,
        })
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_test_output_basic() {
        let output = "running 3 tests\ntest foo ... ok\ntest bar ... ok\ntest baz ... FAILED\n\ntest result: ok. 2 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out\n";
        let results = parse_test_output(output).unwrap();
        assert_eq!(results.passed, 2);
        assert_eq!(results.failed, 1);
        assert_eq!(results.ignored, 0);
        assert_eq!(results.failures.len(), 1);
        assert_eq!(results.failures[0].name, "baz");
    }

    #[test]
    fn parse_test_output_none_when_no_results() {
        assert!(parse_test_output("no test output here").is_none());
    }

    #[test]
    fn parse_test_output_multiple_crates() {
        let output = "test result: ok. 5 passed; 0 failed; 1 ignored; 0 measured; 0 filtered out\ntest result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out\n";
        let results = parse_test_output(output).unwrap();
        assert_eq!(results.passed, 8);
        assert_eq!(results.ignored, 1);
    }
}
