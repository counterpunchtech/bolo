//! Object mapping types for git objects to bolo mesh.

use serde::{Deserialize, Serialize};

/// A git blob mapped to a bolo blob hash.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MappedBlob {
    pub git_oid: String,
    pub bolo_hash: String,
    pub size: u64,
}

/// A git tree entry mapped for mesh sync.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MappedTree {
    pub git_oid: String,
    pub entries: Vec<TreeEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TreeEntry {
    pub name: String,
    pub kind: EntryKind,
    pub oid: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EntryKind {
    Blob,
    Tree,
    Commit,
}

/// A git commit mapped for mesh sync.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MappedCommit {
    pub git_oid: String,
    pub tree_oid: String,
    pub parent_oids: Vec<String>,
    pub author: String,
    pub message: String,
    pub timestamp: i64,
}

/// A git ref tracked on the mesh.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MappedRef {
    pub name: String,
    pub target_oid: String,
    pub is_head: bool,
}

/// Summary of git repo status for mesh sync.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GitStatus {
    pub repo_path: String,
    pub head_ref: Option<String>,
    pub head_oid: Option<String>,
    pub is_clean: bool,
    pub staged: Vec<String>,
    pub modified: Vec<String>,
    pub untracked: Vec<String>,
}

/// A review comment on a commit or file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReviewComment {
    pub id: String,
    pub author: String,
    pub commit_oid: String,
    pub file_path: Option<String>,
    pub line: Option<u32>,
    pub body: String,
    pub timestamp: u64,
    pub status: ReviewStatus,
    /// Ed25519 signature (hex-encoded) for approve/reject.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ReviewStatus {
    Pending,
    Approved,
    ChangesRequested,
}
