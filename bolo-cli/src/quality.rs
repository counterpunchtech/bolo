//! `bolo quality` — run project quality checks.

use std::process::Command;
use std::time::Instant;

use serde::Serialize;

/// Quality check results.
#[derive(Debug, Serialize)]
pub struct QualityReport {
    pub passed: bool,
    pub checks: Vec<CheckResult>,
    pub duration_ms: u64,
}

#[derive(Debug, Serialize)]
pub struct CheckResult {
    pub name: String,
    pub passed: bool,
    pub output: String,
    pub duration_ms: u64,
}

fn run_check(name: &str, cmd: &str, args: &[&str]) -> CheckResult {
    let start = Instant::now();
    let result = Command::new(cmd).args(args).output();
    let duration_ms = start.elapsed().as_millis() as u64;

    match result {
        Ok(output) => {
            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);
            let combined = if stderr.is_empty() {
                stdout.to_string()
            } else {
                format!("{stdout}\n{stderr}")
            };
            CheckResult {
                name: name.to_string(),
                passed: output.status.success(),
                output: combined,
                duration_ms,
            }
        }
        Err(e) => CheckResult {
            name: name.to_string(),
            passed: false,
            output: format!("failed to execute: {e}"),
            duration_ms,
        },
    }
}

/// Run the fast quality gate: fmt + clippy + machete.
pub fn run_fast(fix: bool) -> Vec<CheckResult> {
    let mut checks = Vec::new();

    if fix {
        checks.push(run_check("fmt", "cargo", &["fmt", "--all"]));
        checks.push(run_check(
            "clippy",
            "cargo",
            &[
                "clippy",
                "--workspace",
                "--fix",
                "--allow-dirty",
                "--allow-staged",
            ],
        ));
    } else {
        checks.push(run_check(
            "fmt",
            "cargo",
            &["fmt", "--all", "--", "--check"],
        ));
        checks.push(run_check(
            "clippy",
            "cargo",
            &["clippy", "--workspace", "--", "-D", "warnings"],
        ));
    }

    checks.push(run_check(
        "machete",
        "cargo",
        &["machete", "--skip-target-dir"],
    ));

    checks
}

/// Run the full quality gate: fast + test + deny + audit + doc.
pub fn run_full(fix: bool) -> Vec<CheckResult> {
    let mut checks = run_fast(fix);

    checks.push(run_check("test", "cargo", &["test", "--workspace"]));
    checks.push(run_check("deny", "cargo", &["deny", "check"]));
    checks.push(run_check("audit", "cargo", &["audit"]));
    checks.push(run_check(
        "doc",
        "cargo",
        &["doc", "--workspace", "--no-deps"],
    ));

    checks
}

/// Execute the quality command and return the report.
pub fn execute(fast: bool, full: bool, fix: bool) -> QualityReport {
    let start = Instant::now();

    let checks = if full {
        run_full(fix)
    } else if fast {
        run_fast(fix)
    } else {
        // Default to fast
        run_fast(fix)
    };

    let passed = checks.iter().all(|c| c.passed);
    let duration_ms = start.elapsed().as_millis() as u64;

    QualityReport {
        passed,
        checks,
        duration_ms,
    }
}

/// Print the report to stdout.
pub fn print_report(report: &QualityReport, json: bool) {
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(report).expect("serialize report")
        );
        return;
    }

    for check in &report.checks {
        let status = if check.passed { "PASS" } else { "FAIL" };
        println!(
            "[{status}] {} ({:.1}s)",
            check.name,
            check.duration_ms as f64 / 1000.0
        );
        if !check.passed && !check.output.is_empty() {
            for line in check.output.lines().take(20) {
                println!("  {line}");
            }
        }
    }

    let overall = if report.passed { "PASS" } else { "FAIL" };
    println!(
        "\nOverall: {overall} ({:.1}s)",
        report.duration_ms as f64 / 1000.0
    );
}
