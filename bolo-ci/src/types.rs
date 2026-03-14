use serde::{Deserialize, Serialize};

/// Status of a CI build task.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BuildStatus {
    /// Task has been created but not yet claimed
    Pending,
    /// A peer has claimed the task and is building
    Running { peer: String },
    /// Build completed successfully
    Passed { peer: String, duration_ms: u64 },
    /// Build failed
    Failed {
        peer: String,
        duration_ms: u64,
        error: String,
    },
    /// Build was cancelled
    Cancelled,
}

/// A CI build task definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BuildTask {
    /// Unique task ID (blake3 hash of source_tree + config)
    pub id: String,
    /// Type of task
    pub task_type: TaskType,
    /// Hash of the source tree (commit or tree OID)
    pub source_tree: String,
    /// Build configuration hash
    pub config_hash: Option<String>,
    /// Rust toolchain version
    pub rust_version: Option<String>,
    /// Target platforms
    pub targets: Vec<String>,
    /// Current status
    pub status: BuildStatus,
    /// Verification requirements
    pub verification: Verification,
    /// Who triggered this build
    pub triggered_by: String,
    /// When the task was created (ms since epoch)
    pub created_at: u64,
    /// When the task was last updated
    pub updated_at: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TaskType {
    Build,
    Test,
    Check,
    Clippy,
    Fmt,
    Full, // all of the above
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Verification {
    /// How many independent peers must produce the same result
    pub redundancy: u32,
    /// Timeout in seconds
    pub timeout_seconds: u64,
}

impl Default for Verification {
    fn default() -> Self {
        Self {
            redundancy: 1,
            timeout_seconds: 600,
        }
    }
}

/// Result of a build task execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BuildResult {
    /// Task ID this result belongs to
    pub task_id: String,
    /// Peer that produced this result
    pub peer: String,
    /// Whether the build passed
    pub passed: bool,
    /// Duration in milliseconds
    pub duration_ms: u64,
    /// Output summary
    pub summary: String,
    /// Detailed output (stdout + stderr)
    pub output: String,
    /// Test results if this was a test task
    pub test_results: Option<TestResults>,
    /// Artifact hashes produced
    pub artifacts: Vec<ArtifactRef>,
    /// When this result was produced
    pub timestamp: u64,
}

/// Structured test results.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestResults {
    pub total: u32,
    pub passed: u32,
    pub failed: u32,
    pub ignored: u32,
    pub failures: Vec<TestFailure>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestFailure {
    pub name: String,
    pub message: String,
}

/// Reference to a build artifact stored as a blob.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArtifactRef {
    pub name: String,
    pub hash: String,
    pub size: u64,
}
