//! GitBridge wraps git2::Repository and provides mesh-friendly operations.

use git2::Repository;

use crate::types::*;

/// Bridge between a local git repository and the bolo mesh.
pub struct GitBridge {
    repo: Repository,
}

impl GitBridge {
    /// Open a git repository at the given path.
    pub fn open(path: &std::path::Path) -> Result<Self, bolo_core::BoloError> {
        let repo = Repository::open(path)
            .map_err(|e| bolo_core::BoloError::ConfigError(format!("git open failed: {e}")))?;
        Ok(Self { repo })
    }

    /// Discover a git repository from a path (walks up parent dirs).
    pub fn discover(path: &std::path::Path) -> Result<Self, bolo_core::BoloError> {
        let repo = Repository::discover(path)
            .map_err(|e| bolo_core::BoloError::ConfigError(format!("git discover failed: {e}")))?;
        Ok(Self { repo })
    }

    /// Get the repository status.
    pub fn status(&self) -> Result<GitStatus, bolo_core::BoloError> {
        let repo_path = self
            .repo
            .workdir()
            .unwrap_or_else(|| self.repo.path())
            .to_string_lossy()
            .to_string();

        let head_ref = self
            .repo
            .head()
            .ok()
            .and_then(|h| h.shorthand().map(|s| s.to_string()));
        let head_oid = self
            .repo
            .head()
            .ok()
            .and_then(|h| h.target().map(|oid| oid.to_string()));

        let statuses = self
            .repo
            .statuses(None)
            .map_err(|e| bolo_core::BoloError::ConfigError(format!("git status failed: {e}")))?;

        let mut staged = Vec::new();
        let mut modified = Vec::new();
        let mut untracked = Vec::new();

        for entry in statuses.iter() {
            let path = entry.path().unwrap_or("?").to_string();
            let s = entry.status();
            if s.intersects(
                git2::Status::INDEX_NEW
                    | git2::Status::INDEX_MODIFIED
                    | git2::Status::INDEX_DELETED
                    | git2::Status::INDEX_RENAMED
                    | git2::Status::INDEX_TYPECHANGE,
            ) {
                staged.push(path.clone());
            }
            if s.intersects(
                git2::Status::WT_MODIFIED
                    | git2::Status::WT_DELETED
                    | git2::Status::WT_RENAMED
                    | git2::Status::WT_TYPECHANGE,
            ) {
                modified.push(path.clone());
            }
            if s.contains(git2::Status::WT_NEW) {
                untracked.push(path);
            }
        }

        let is_clean = staged.is_empty() && modified.is_empty() && untracked.is_empty();

        Ok(GitStatus {
            repo_path,
            head_ref,
            head_oid,
            is_clean,
            staged,
            modified,
            untracked,
        })
    }

    /// List all refs in the repository.
    pub fn list_refs(&self) -> Result<Vec<MappedRef>, bolo_core::BoloError> {
        let head_oid = self.repo.head().ok().and_then(|h| h.target());
        let refs = self
            .repo
            .references()
            .map_err(|e| bolo_core::BoloError::ConfigError(format!("git refs failed: {e}")))?;

        let mut mapped = Vec::new();
        for reference in refs.flatten() {
            if let (Some(name), Some(target)) = (reference.name(), reference.target()) {
                mapped.push(MappedRef {
                    name: name.to_string(),
                    target_oid: target.to_string(),
                    is_head: Some(target) == head_oid,
                });
            }
        }
        Ok(mapped)
    }

    /// List recent commits from HEAD.
    pub fn log(&self, max_count: usize) -> Result<Vec<MappedCommit>, bolo_core::BoloError> {
        let head = self
            .repo
            .head()
            .map_err(|e| bolo_core::BoloError::ConfigError(format!("no HEAD: {e}")))?;
        let head_oid = head
            .target()
            .ok_or_else(|| bolo_core::BoloError::ConfigError("HEAD has no target".into()))?;

        let mut revwalk = self
            .repo
            .revwalk()
            .map_err(|e| bolo_core::BoloError::ConfigError(format!("revwalk failed: {e}")))?;
        revwalk
            .push(head_oid)
            .map_err(|e| bolo_core::BoloError::ConfigError(format!("revwalk push failed: {e}")))?;
        revwalk
            .set_sorting(git2::Sort::TIME)
            .map_err(|e| bolo_core::BoloError::ConfigError(format!("revwalk sort failed: {e}")))?;

        let mut commits = Vec::new();
        for oid in revwalk.take(max_count).flatten() {
            let commit = self.repo.find_commit(oid).map_err(|e| {
                bolo_core::BoloError::ConfigError(format!("find commit failed: {e}"))
            })?;
            let author_sig = commit.author();
            let author = format!(
                "{} <{}>",
                author_sig.name().unwrap_or("?"),
                author_sig.email().unwrap_or("?")
            );
            commits.push(MappedCommit {
                git_oid: oid.to_string(),
                tree_oid: commit.tree_id().to_string(),
                parent_oids: commit.parent_ids().map(|id| id.to_string()).collect(),
                author,
                message: commit.message().unwrap_or("").to_string(),
                timestamp: commit.time().seconds(),
            });
        }
        Ok(commits)
    }

    /// Export all git blobs in HEAD tree as individual files to a staging directory.
    /// Returns the list of exported blobs with their git OIDs and file paths.
    pub fn export_objects(
        &self,
        staging_dir: &std::path::Path,
    ) -> Result<Vec<MappedBlob>, bolo_core::BoloError> {
        std::fs::create_dir_all(staging_dir)?;
        let head = self
            .repo
            .head()
            .map_err(|e| bolo_core::BoloError::ConfigError(format!("no HEAD: {e}")))?;
        let commit = head
            .peel_to_commit()
            .map_err(|e| bolo_core::BoloError::ConfigError(format!("peel failed: {e}")))?;
        let tree = commit
            .tree()
            .map_err(|e| bolo_core::BoloError::ConfigError(format!("tree failed: {e}")))?;

        let mut blobs = Vec::new();
        let repo = &self.repo;
        tree.walk(git2::TreeWalkMode::PreOrder, |dir, entry| {
            if entry.kind() == Some(git2::ObjectType::Blob) {
                let oid = entry.id();
                if let Ok(blob) = repo.find_blob(oid) {
                    let out_path = staging_dir.join(oid.to_string());
                    if std::fs::write(&out_path, blob.content()).is_ok() {
                        let name = entry.name().unwrap_or("?");
                        let file_path = if dir.is_empty() {
                            name.to_string()
                        } else {
                            format!("{dir}{name}")
                        };
                        blobs.push(MappedBlob {
                            git_oid: oid.to_string(),
                            bolo_hash: file_path,
                            size: blob.size() as u64,
                        });
                    }
                }
            }
            git2::TreeWalkResult::Ok
        })
        .map_err(|e| bolo_core::BoloError::ConfigError(format!("tree walk failed: {e}")))?;

        Ok(blobs)
    }

    /// Import git objects from a staging directory into the local repo.
    /// Reads blob files and creates corresponding git objects.
    pub fn import_objects(
        &self,
        staging_dir: &std::path::Path,
    ) -> Result<usize, bolo_core::BoloError> {
        let mut count = 0usize;
        if !staging_dir.exists() {
            return Ok(count);
        }
        let odb = self
            .repo
            .odb()
            .map_err(|e| bolo_core::BoloError::ConfigError(format!("odb failed: {e}")))?;
        for entry in std::fs::read_dir(staging_dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_file() {
                let data = std::fs::read(&path)?;
                odb.write(git2::ObjectType::Blob, &data).map_err(|e| {
                    bolo_core::BoloError::ConfigError(format!("odb write failed: {e}"))
                })?;
                count += 1;
            }
        }
        Ok(count)
    }

    /// Walk all reachable objects from all refs.
    /// Returns (git_oid, object_type) pairs for every commit, tree, blob, and tag.
    pub fn walk_reachable_objects(&self) -> Result<Vec<(String, String)>, bolo_core::BoloError> {
        let mut seen = std::collections::HashSet::new();
        let mut result = Vec::new();
        let mut queue = std::collections::VecDeque::new();

        // Start from all refs
        let refs = self
            .repo
            .references()
            .map_err(|e| bolo_core::BoloError::ConfigError(format!("refs failed: {e}")))?;
        for reference in refs.flatten() {
            if let Some(target) = reference.target() {
                queue.push_back(target);
            }
        }

        while let Some(oid) = queue.pop_front() {
            if !seen.insert(oid) {
                continue;
            }

            let obj = match self.repo.find_object(oid, None) {
                Ok(o) => o,
                Err(_) => continue,
            };
            let kind = match obj.kind() {
                Some(k) => k,
                None => continue,
            };

            let type_str = match kind {
                git2::ObjectType::Commit => "commit",
                git2::ObjectType::Tree => "tree",
                git2::ObjectType::Blob => "blob",
                git2::ObjectType::Tag => "tag",
                _ => continue,
            };

            result.push((oid.to_string(), type_str.to_string()));

            match kind {
                git2::ObjectType::Commit => {
                    if let Ok(commit) = obj.into_commit() {
                        queue.push_back(commit.tree_id());
                        for parent_id in commit.parent_ids() {
                            queue.push_back(parent_id);
                        }
                    }
                }
                git2::ObjectType::Tree => {
                    if let Ok(tree) = obj.into_tree() {
                        for entry in tree.iter() {
                            queue.push_back(entry.id());
                        }
                    }
                }
                git2::ObjectType::Tag => {
                    if let Ok(tag) = obj.into_tag() {
                        queue.push_back(tag.target_id());
                    }
                }
                _ => {}
            }
        }

        Ok(result)
    }

    /// Read the raw content of a git object from the ODB.
    /// Returns (object_type, raw_data).
    pub fn read_object_raw(
        &self,
        oid_str: &str,
    ) -> Result<(String, Vec<u8>), bolo_core::BoloError> {
        let oid = git2::Oid::from_str(oid_str)
            .map_err(|e| bolo_core::BoloError::ConfigError(format!("invalid OID: {e}")))?;
        let odb = self
            .repo
            .odb()
            .map_err(|e| bolo_core::BoloError::ConfigError(format!("odb failed: {e}")))?;
        let obj = odb
            .read(oid)
            .map_err(|e| bolo_core::BoloError::ConfigError(format!("odb read failed: {e}")))?;
        let type_str = match obj.kind() {
            git2::ObjectType::Commit => "commit",
            git2::ObjectType::Tree => "tree",
            git2::ObjectType::Blob => "blob",
            git2::ObjectType::Tag => "tag",
            _ => "unknown",
        };
        Ok((type_str.to_string(), obj.data().to_vec()))
    }

    /// Write a raw object into the git ODB. Returns the computed OID.
    pub fn write_object_raw(
        &self,
        type_str: &str,
        data: &[u8],
    ) -> Result<String, bolo_core::BoloError> {
        let obj_type = match type_str {
            "commit" => git2::ObjectType::Commit,
            "tree" => git2::ObjectType::Tree,
            "blob" => git2::ObjectType::Blob,
            "tag" => git2::ObjectType::Tag,
            _ => {
                return Err(bolo_core::BoloError::ConfigError(format!(
                    "unknown object type: {type_str}"
                )));
            }
        };
        let odb = self
            .repo
            .odb()
            .map_err(|e| bolo_core::BoloError::ConfigError(format!("odb failed: {e}")))?;
        let oid = odb
            .write(obj_type, data)
            .map_err(|e| bolo_core::BoloError::ConfigError(format!("odb write failed: {e}")))?;
        Ok(oid.to_string())
    }

    /// Check if an object exists in the local ODB.
    pub fn has_object(&self, oid_str: &str) -> bool {
        if let Ok(oid) = git2::Oid::from_str(oid_str) {
            if let Ok(odb) = self.repo.odb() {
                return odb.exists(oid);
            }
        }
        false
    }

    /// Set a reference to point at the given OID. Creates or updates.
    pub fn set_ref(&self, name: &str, oid_str: &str) -> Result<(), bolo_core::BoloError> {
        let oid = git2::Oid::from_str(oid_str)
            .map_err(|e| bolo_core::BoloError::ConfigError(format!("invalid OID: {e}")))?;
        self.repo
            .reference(name, oid, true, "bolo git pull")
            .map_err(|e| bolo_core::BoloError::ConfigError(format!("set ref failed: {e}")))?;
        Ok(())
    }

    /// Set HEAD to the given reference name.
    pub fn set_head(&self, refname: &str) -> Result<(), bolo_core::BoloError> {
        self.repo
            .set_head(refname)
            .map_err(|e| bolo_core::BoloError::ConfigError(format!("set HEAD failed: {e}")))?;
        Ok(())
    }

    /// Checkout HEAD (force).
    pub fn checkout_head(&self) -> Result<(), bolo_core::BoloError> {
        self.repo
            .checkout_head(Some(git2::build::CheckoutBuilder::default().force()))
            .map_err(|e| bolo_core::BoloError::ConfigError(format!("checkout failed: {e}")))?;
        Ok(())
    }

    /// List all objects (blobs) in the current HEAD tree.
    pub fn list_objects(&self) -> Result<Vec<MappedBlob>, bolo_core::BoloError> {
        let head = self
            .repo
            .head()
            .map_err(|e| bolo_core::BoloError::ConfigError(format!("no HEAD: {e}")))?;
        let commit = head
            .peel_to_commit()
            .map_err(|e| bolo_core::BoloError::ConfigError(format!("peel failed: {e}")))?;
        let tree = commit
            .tree()
            .map_err(|e| bolo_core::BoloError::ConfigError(format!("tree failed: {e}")))?;

        let mut blobs = Vec::new();
        tree.walk(git2::TreeWalkMode::PreOrder, |dir, entry| {
            if entry.kind() == Some(git2::ObjectType::Blob) {
                let name = entry.name().unwrap_or("?");
                let path = if dir.is_empty() {
                    name.to_string()
                } else {
                    format!("{dir}{name}")
                };
                blobs.push(MappedBlob {
                    git_oid: entry.id().to_string(),
                    bolo_hash: path,
                    size: 0, // would need object lookup for size
                });
            }
            git2::TreeWalkResult::Ok
        })
        .map_err(|e| bolo_core::BoloError::ConfigError(format!("tree walk failed: {e}")))?;

        Ok(blobs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open_nonexistent_repo_fails() {
        let result = GitBridge::open(std::path::Path::new("/nonexistent/path"));
        assert!(result.is_err());
    }

    #[test]
    fn discover_from_cwd() {
        // This test works if run from within a git repo
        if let Ok(bridge) = GitBridge::discover(std::path::Path::new(".")) {
            let status = bridge.status().unwrap();
            assert!(!status.repo_path.is_empty());
        }
    }
}
