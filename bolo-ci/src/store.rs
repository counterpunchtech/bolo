use std::path::{Path, PathBuf};

use bolo_core::BoloError;

use crate::types::*;

pub struct CiStore {
    tasks_dir: PathBuf,
    results_dir: PathBuf,
}

impl CiStore {
    pub fn open(data_dir: &Path) -> Result<Self, BoloError> {
        let tasks_dir = data_dir.join("ci").join("tasks");
        let results_dir = data_dir.join("ci").join("results");
        std::fs::create_dir_all(&tasks_dir)?;
        std::fs::create_dir_all(&results_dir)?;
        Ok(Self {
            tasks_dir,
            results_dir,
        })
    }

    /// Create a new build task. Returns the task with generated ID.
    pub fn create_task(&self, mut task: BuildTask) -> Result<BuildTask, BoloError> {
        // Generate ID from source_tree + config
        let id_input = format!(
            "{}:{}",
            task.source_tree,
            task.config_hash.as_deref().unwrap_or("")
        );
        task.id = blake3::hash(id_input.as_bytes()).to_hex()[..16].to_string();
        let path = self.tasks_dir.join(format!("{}.json", task.id));
        let json = serde_json::to_string_pretty(&task)
            .map_err(|e| BoloError::Serialization(format!("failed to serialize task: {e}")))?;
        std::fs::write(&path, json)?;
        Ok(task)
    }

    /// Load a task by ID.
    pub fn load_task(&self, id: &str) -> Result<BuildTask, BoloError> {
        let path = self.tasks_dir.join(format!("{id}.json"));
        if !path.exists() {
            return Err(BoloError::DocumentNotFound(format!(
                "CI task not found: {id}"
            )));
        }
        let json = std::fs::read_to_string(&path)?;
        serde_json::from_str(&json)
            .map_err(|e| BoloError::Serialization(format!("failed to parse task: {e}")))
    }

    /// Update a task (overwrites).
    pub fn update_task(&self, task: &BuildTask) -> Result<(), BoloError> {
        let path = self.tasks_dir.join(format!("{}.json", task.id));
        let json = serde_json::to_string_pretty(task)
            .map_err(|e| BoloError::Serialization(format!("failed to serialize task: {e}")))?;
        std::fs::write(&path, json)?;
        Ok(())
    }

    /// List all tasks.
    pub fn list_tasks(&self) -> Result<Vec<BuildTask>, BoloError> {
        let mut tasks = Vec::new();
        for entry in std::fs::read_dir(&self.tasks_dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) == Some("json") {
                let json = std::fs::read_to_string(&path)?;
                if let Ok(task) = serde_json::from_str::<BuildTask>(&json) {
                    tasks.push(task);
                }
            }
        }
        tasks.sort_by(|a, b| b.created_at.cmp(&a.created_at));
        Ok(tasks)
    }

    /// Save a build result.
    pub fn save_result(&self, result: &BuildResult) -> Result<(), BoloError> {
        let dir = self.results_dir.join(&result.task_id);
        std::fs::create_dir_all(&dir)?;
        let path = dir.join(format!("{}.json", result.peer));
        let json = serde_json::to_string_pretty(result)
            .map_err(|e| BoloError::Serialization(format!("failed to serialize result: {e}")))?;
        std::fs::write(&path, json)?;
        Ok(())
    }

    /// Load all results for a task.
    pub fn load_results(&self, task_id: &str) -> Result<Vec<BuildResult>, BoloError> {
        let dir = self.results_dir.join(task_id);
        if !dir.exists() {
            return Ok(Vec::new());
        }
        let mut results = Vec::new();
        for entry in std::fs::read_dir(&dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) == Some("json") {
                let json = std::fs::read_to_string(&path)?;
                if let Ok(result) = serde_json::from_str::<BuildResult>(&json) {
                    results.push(result);
                }
            }
        }
        results.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));
        Ok(results)
    }

    /// Count result files for a task.
    pub fn count_results(&self, task_id: &str) -> Result<usize, BoloError> {
        let dir = self.results_dir.join(task_id);
        if !dir.exists() {
            return Ok(0);
        }
        let count = std::fs::read_dir(&dir)?
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().and_then(|ext| ext.to_str()) == Some("json"))
            .count();
        Ok(count)
    }

    /// Prune old tasks, keeping only the newest `keep` tasks.
    /// Also removes associated results for pruned tasks.
    /// Returns the number of tasks deleted.
    pub fn prune_tasks(&self, keep: usize) -> Result<usize, BoloError> {
        let tasks = self.list_tasks()?; // already sorted by created_at desc
        if tasks.len() <= keep {
            return Ok(0);
        }
        let mut deleted = 0;
        for task in tasks.into_iter().skip(keep) {
            // Remove task file
            let task_path = self.tasks_dir.join(format!("{}.json", task.id));
            if task_path.exists() {
                std::fs::remove_file(&task_path)?;
            }
            // Remove associated results directory
            let results_path = self.results_dir.join(&task.id);
            if results_path.exists() {
                std::fs::remove_dir_all(&results_path)?;
            }
            deleted += 1;
        }
        Ok(deleted)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_task(source_tree: &str, created_at: u64) -> BuildTask {
        BuildTask {
            id: String::new(),
            task_type: TaskType::Check,
            source_tree: source_tree.to_string(),
            config_hash: None,
            rust_version: None,
            targets: vec!["x86_64".to_string()],
            status: BuildStatus::Pending,
            verification: Verification::default(),
            triggered_by: "test".to_string(),
            created_at,
            updated_at: created_at,
        }
    }

    #[test]
    fn create_and_load_task() {
        let tmp = tempfile::tempdir().unwrap();
        let store = CiStore::open(tmp.path()).unwrap();

        let task = make_task("abc123", 1000);
        let created = store.create_task(task).unwrap();
        assert!(!created.id.is_empty());

        let loaded = store.load_task(&created.id).unwrap();
        assert_eq!(loaded.source_tree, "abc123");
        assert_eq!(loaded.triggered_by, "test");
    }

    #[test]
    fn list_tasks_sorted() {
        let tmp = tempfile::tempdir().unwrap();
        let store = CiStore::open(tmp.path()).unwrap();

        // Create tasks with different timestamps and different source trees
        // so they get different IDs
        let t1 = make_task("aaa", 1000);
        let t2 = make_task("bbb", 3000);
        let t3 = make_task("ccc", 2000);

        store.create_task(t1).unwrap();
        store.create_task(t2).unwrap();
        store.create_task(t3).unwrap();

        let tasks = store.list_tasks().unwrap();
        assert_eq!(tasks.len(), 3);
        // Should be sorted by created_at descending
        assert_eq!(tasks[0].created_at, 3000);
        assert_eq!(tasks[1].created_at, 2000);
        assert_eq!(tasks[2].created_at, 1000);
    }

    #[test]
    fn prune_tasks() {
        let tmp = tempfile::tempdir().unwrap();
        let store = CiStore::open(tmp.path()).unwrap();

        // Create 5 tasks with different source trees and timestamps
        for i in 0..5 {
            let task = make_task(&format!("tree-{i}"), 1000 + i as u64);
            store.create_task(task).unwrap();
        }

        assert_eq!(store.list_tasks().unwrap().len(), 5);
        let deleted = store.prune_tasks(2).unwrap();
        assert_eq!(deleted, 3);
        let remaining = store.list_tasks().unwrap();
        assert_eq!(remaining.len(), 2);
        // Newest two should remain (created_at 1004, 1003)
        assert_eq!(remaining[0].created_at, 1004);
        assert_eq!(remaining[1].created_at, 1003);
    }

    #[test]
    fn save_and_load_results() {
        let tmp = tempfile::tempdir().unwrap();
        let store = CiStore::open(tmp.path()).unwrap();

        let task = store.create_task(make_task("def456", 1000)).unwrap();

        let result = BuildResult {
            task_id: task.id.clone(),
            peer: "peer-a".to_string(),
            passed: true,
            duration_ms: 5000,
            summary: "passed".to_string(),
            output: "all good".to_string(),
            test_results: Some(TestResults {
                total: 10,
                passed: 9,
                failed: 1,
                ignored: 0,
                failures: vec![TestFailure {
                    name: "test_foo".to_string(),
                    message: "assertion failed".to_string(),
                }],
            }),
            artifacts: vec![],
            timestamp: 2000,
        };

        store.save_result(&result).unwrap();

        let results = store.load_results(&task.id).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].peer, "peer-a");
        assert!(results[0].passed);
        assert_eq!(results[0].test_results.as_ref().unwrap().total, 10);
    }
}
