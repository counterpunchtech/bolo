use serde::{Deserialize, Serialize};

/// Task status (kanban column).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum TaskStatus {
    Backlog,
    Ready,
    InProgress,
    Review,
    Done,
}

impl std::fmt::Display for TaskStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Backlog => write!(f, "backlog"),
            Self::Ready => write!(f, "ready"),
            Self::InProgress => write!(f, "in-progress"),
            Self::Review => write!(f, "review"),
            Self::Done => write!(f, "done"),
        }
    }
}

/// Priority level (0 = critical, 3 = low).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Priority {
    Critical = 0,
    High = 1,
    Medium = 2,
    Low = 3,
}

impl std::fmt::Display for Priority {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Critical => write!(f, "critical"),
            Self::High => write!(f, "high"),
            Self::Medium => write!(f, "medium"),
            Self::Low => write!(f, "low"),
        }
    }
}

impl Priority {
    pub fn from_str_or_default(s: Option<&str>) -> Self {
        match s {
            Some("critical") | Some("0") => Self::Critical,
            Some("high") | Some("1") => Self::High,
            Some("medium") | Some("2") => Self::Medium,
            Some("low") | Some("3") => Self::Low,
            _ => Self::Medium,
        }
    }
}

/// Default claim TTL: 5 minutes (in milliseconds).
pub const DEFAULT_CLAIM_TTL_MS: u64 = 5 * 60 * 1000;

/// A task on the board.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Task {
    /// Unique task ID (short blake3 hash)
    pub id: String,
    /// Human-readable title
    pub title: String,
    /// Current status
    pub status: TaskStatus,
    /// Assigned peer (node ID or "unassigned")
    pub assignee: Option<String>,
    /// Priority level
    pub priority: Priority,
    /// Link to spec document (bolo doc path)
    pub spec_doc: Option<String>,
    /// Blocked-by task IDs
    pub dependencies: Vec<String>,
    /// Associated commit hashes
    pub commits: Vec<String>,
    /// Associated CI result task hashes
    pub ci_results: Vec<String>,
    /// Link to review document
    pub review_doc: Option<String>,
    /// Who created this task
    pub created_by: String,
    /// Creation timestamp (ms since epoch)
    pub created_at: u64,
    /// Last update timestamp
    pub updated_at: u64,
    /// Agent that has claimed this task (node ID). None = unclaimed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claimed_by: Option<String>,
    /// When the claim was last refreshed (ms since epoch). Used for TTL expiry.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claimed_at: Option<u64>,
}

impl Task {
    /// Returns true if this task has an active (non-expired) claim.
    pub fn is_claimed(&self, now_ms: u64, ttl_ms: u64) -> bool {
        match (self.claimed_by.as_ref(), self.claimed_at) {
            (Some(_), Some(at)) => now_ms.saturating_sub(at) < ttl_ms,
            _ => false,
        }
    }

    /// Returns the active claimer's node ID, or None if unclaimed or expired.
    pub fn active_claimer(&self, now_ms: u64, ttl_ms: u64) -> Option<&str> {
        if self.is_claimed(now_ms, ttl_ms) {
            self.claimed_by.as_deref()
        } else {
            None
        }
    }
}
