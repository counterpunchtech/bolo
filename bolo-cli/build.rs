use std::process::Command;

fn main() {
    // Emit a build timestamp so each binary has a unique version.
    // Format: 0.1.0-20260313T171234Z (semver + ISO 8601 compact UTC)
    let pkg_version = env!("CARGO_PKG_VERSION");

    let timestamp = Command::new("date")
        .args(["-u", "+%Y%m%dT%H%M%SZ"])
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .unwrap_or_default()
        .trim()
        .to_string();

    let git_short = Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .unwrap_or_default()
        .trim()
        .to_string();

    let build_version = if git_short.is_empty() {
        format!("{pkg_version}-{timestamp}")
    } else {
        format!("{pkg_version}-{timestamp}-{git_short}")
    };

    println!("cargo:rustc-env=BOLO_BUILD_VERSION={build_version}");

    // Re-run if git HEAD changes (new commits) or if we explicitly rebuild
    println!("cargo:rerun-if-changed=.git/HEAD");
    println!("cargo:rerun-if-changed=build.rs");
    // Always re-run so timestamp updates on each build
    println!("cargo:rerun-if-env-changed=FORCE_REBUILD");
}
