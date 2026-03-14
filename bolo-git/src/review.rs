//! Filesystem-backed review store.
//!
//! Each commit's reviews are stored as `reviews/<commit_oid>.json` containing
//! a JSON array of [`ReviewComment`].

use std::path::{Path, PathBuf};

use crate::types::{ReviewComment, ReviewStatus};

/// A filesystem-backed review store.
pub struct ReviewStore {
    reviews_dir: PathBuf,
}

impl ReviewStore {
    /// Open or create a review store in the given data directory.
    pub fn open(data_dir: &Path) -> Result<Self, bolo_core::BoloError> {
        let reviews_dir = data_dir.join("reviews");
        std::fs::create_dir_all(&reviews_dir)?;
        Ok(Self { reviews_dir })
    }

    /// Resolve the on-disk path for a commit's review file.
    fn review_path(&self, commit_oid: &str) -> PathBuf {
        let short = &commit_oid[..commit_oid.len().min(12)];
        self.reviews_dir.join(format!("{short}.json"))
    }

    /// Load existing reviews for a commit, or return an empty vec.
    fn load_reviews(&self, commit_oid: &str) -> Result<Vec<ReviewComment>, bolo_core::BoloError> {
        let path = self.review_path(commit_oid);
        if !path.exists() {
            return Ok(Vec::new());
        }
        let data = std::fs::read_to_string(&path)?;
        let comments: Vec<ReviewComment> = serde_json::from_str(&data)
            .map_err(|e| bolo_core::BoloError::Serialization(format!("bad review JSON: {e}")))?;
        Ok(comments)
    }

    /// Save reviews for a commit.
    fn save_reviews(
        &self,
        commit_oid: &str,
        comments: &[ReviewComment],
    ) -> Result<(), bolo_core::BoloError> {
        let path = self.review_path(commit_oid);
        let data = serde_json::to_string_pretty(comments)
            .map_err(|e| bolo_core::BoloError::Serialization(format!("serialize reviews: {e}")))?;
        std::fs::write(&path, data)?;
        Ok(())
    }

    /// Add a review comment for a commit.
    pub fn add_comment(&self, comment: ReviewComment) -> Result<(), bolo_core::BoloError> {
        let mut comments = self.load_reviews(&comment.commit_oid)?;
        let oid = comment.commit_oid.clone();
        comments.push(comment);
        self.save_reviews(&oid, &comments)
    }

    /// List all reviews for a specific commit.
    pub fn list_for_commit(
        &self,
        commit_oid: &str,
    ) -> Result<Vec<ReviewComment>, bolo_core::BoloError> {
        self.load_reviews(commit_oid)
    }

    /// List all commits with pending reviews (no Approved status).
    pub fn list_pending(&self) -> Result<Vec<(String, Vec<ReviewComment>)>, bolo_core::BoloError> {
        let mut pending = Vec::new();
        if !self.reviews_dir.exists() {
            return Ok(pending);
        }
        for entry in std::fs::read_dir(&self.reviews_dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("json") {
                continue;
            }
            let data = std::fs::read_to_string(&path)?;
            let comments: Vec<ReviewComment> = serde_json::from_str(&data).map_err(|e| {
                bolo_core::BoloError::Serialization(format!("bad review JSON: {e}"))
            })?;
            if comments.is_empty() {
                continue;
            }
            let has_approval = comments
                .iter()
                .any(|c| matches!(c.status, ReviewStatus::Approved));
            if !has_approval {
                // Use the commit_oid from the first comment.
                let oid = comments[0].commit_oid.clone();
                pending.push((oid, comments));
            }
        }
        pending.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(pending)
    }

    /// Approve a commit. Creates an Approved review comment and adds it.
    pub fn approve(
        &self,
        commit_oid: &str,
        author: &str,
        timestamp: u64,
    ) -> Result<ReviewComment, bolo_core::BoloError> {
        let id_input = format!("approve:{commit_oid}:{author}:{timestamp}");
        let id = blake3::hash(id_input.as_bytes()).to_hex().to_string();
        let comment = ReviewComment {
            id,
            author: author.to_string(),
            commit_oid: commit_oid.to_string(),
            file_path: None,
            line: None,
            body: "Approved".to_string(),
            timestamp,
            status: ReviewStatus::Approved,
            signature: None,
        };
        self.add_comment(comment.clone())?;
        Ok(comment)
    }

    /// Reject a commit. Creates a ChangesRequested review comment and adds it.
    pub fn reject(
        &self,
        commit_oid: &str,
        author: &str,
        message: Option<&str>,
        timestamp: u64,
    ) -> Result<ReviewComment, bolo_core::BoloError> {
        let id_input = format!("reject:{commit_oid}:{author}:{timestamp}");
        let id = blake3::hash(id_input.as_bytes()).to_hex().to_string();
        let comment = ReviewComment {
            id,
            author: author.to_string(),
            commit_oid: commit_oid.to_string(),
            file_path: None,
            line: None,
            body: message.unwrap_or("Changes requested").to_string(),
            timestamp,
            status: ReviewStatus::ChangesRequested,
            signature: None,
        };
        self.add_comment(comment.clone())?;
        Ok(comment)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_comment(commit_oid: &str, body: &str) -> ReviewComment {
        let id_input = format!("test:{commit_oid}:{body}");
        ReviewComment {
            id: blake3::hash(id_input.as_bytes()).to_hex().to_string(),
            author: "test-node".to_string(),
            commit_oid: commit_oid.to_string(),
            file_path: None,
            line: None,
            body: body.to_string(),
            timestamp: 1000,
            status: ReviewStatus::Pending,
            signature: None,
        }
    }

    #[test]
    fn add_and_list_comments() {
        let tmp = tempfile::tempdir().unwrap();
        let store = ReviewStore::open(tmp.path()).unwrap();

        let comment = make_comment("abc123def456", "Looks good");
        store.add_comment(comment.clone()).unwrap();

        let comments = store.list_for_commit("abc123def456").unwrap();
        assert_eq!(comments.len(), 1);
        assert_eq!(comments[0].body, "Looks good");
        assert_eq!(comments[0].author, "test-node");
    }

    #[test]
    fn approve_creates_approved_status() {
        let tmp = tempfile::tempdir().unwrap();
        let store = ReviewStore::open(tmp.path()).unwrap();

        let comment = store.approve("abc123def456", "reviewer", 2000).unwrap();
        assert!(matches!(comment.status, ReviewStatus::Approved));
        assert_eq!(comment.body, "Approved");

        let comments = store.list_for_commit("abc123def456").unwrap();
        assert_eq!(comments.len(), 1);
        assert!(matches!(comments[0].status, ReviewStatus::Approved));
    }

    #[test]
    fn list_pending_filters_approved() {
        let tmp = tempfile::tempdir().unwrap();
        let store = ReviewStore::open(tmp.path()).unwrap();

        // Add a pending comment for commit A.
        store
            .add_comment(make_comment("aaa111222333", "needs work"))
            .unwrap();

        // Add a pending comment for commit B, then approve it.
        store
            .add_comment(make_comment("bbb444555666", "looks fine"))
            .unwrap();
        store.approve("bbb444555666", "reviewer", 3000).unwrap();

        let pending = store.list_pending().unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].0, "aaa111222333");
    }
}
