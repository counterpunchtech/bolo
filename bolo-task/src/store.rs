use std::path::{Path, PathBuf};

use crate::types::*;
use bolo_core::BoloError;

pub struct TaskStore {
    tasks_dir: PathBuf,
}

impl TaskStore {
    pub fn open(data_dir: &Path) -> Result<Self, BoloError> {
        let tasks_dir = data_dir.join("tasks");
        std::fs::create_dir_all(&tasks_dir)?;
        Ok(Self { tasks_dir })
    }

    /// Create a new task. Generates ID and saves.
    pub fn create(&self, mut task: Task) -> Result<Task, BoloError> {
        let id_input = format!("{}:{}:{}", task.title, task.created_by, task.created_at);
        task.id = blake3::hash(id_input.as_bytes()).to_hex()[..12].to_string();
        self.save(&task)?;
        Ok(task)
    }

    /// Save/update a task.
    pub fn save(&self, task: &Task) -> Result<(), BoloError> {
        let path = self.tasks_dir.join(format!("{}.json", task.id));
        let json = serde_json::to_string_pretty(task)
            .map_err(|e| BoloError::Serialization(format!("serialize task: {e}")))?;
        std::fs::write(&path, json)?;
        Ok(())
    }

    /// Load a task by ID.
    pub fn load(&self, id: &str) -> Result<Task, BoloError> {
        let path = self.tasks_dir.join(format!("{id}.json"));
        if !path.exists() {
            return Err(BoloError::DocumentNotFound(format!("task not found: {id}")));
        }
        let json = std::fs::read_to_string(&path)?;
        serde_json::from_str(&json)
            .map_err(|e| BoloError::Serialization(format!("parse task: {e}")))
    }

    /// List all tasks, sorted by priority then creation time.
    pub fn list(&self) -> Result<Vec<Task>, BoloError> {
        let mut tasks = Vec::new();
        if !self.tasks_dir.exists() {
            return Ok(tasks);
        }
        for entry in std::fs::read_dir(&self.tasks_dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) == Some("json") {
                let json = std::fs::read_to_string(&path)?;
                if let Ok(task) = serde_json::from_str::<Task>(&json) {
                    tasks.push(task);
                }
            }
        }
        tasks.sort_by(|a, b| {
            a.priority
                .cmp(&b.priority)
                .then(a.created_at.cmp(&b.created_at))
        });
        Ok(tasks)
    }

    /// Delete a task by ID.
    pub fn delete(&self, id: &str) -> Result<(), BoloError> {
        let path = self.tasks_dir.join(format!("{id}.json"));
        if path.exists() {
            std::fs::remove_file(&path)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_task(title: &str, priority: Priority) -> Task {
        Task {
            id: String::new(),
            title: title.to_string(),
            status: TaskStatus::Backlog,
            assignee: None,
            priority,
            spec_doc: None,
            dependencies: Vec::new(),
            commits: Vec::new(),
            ci_results: Vec::new(),
            review_doc: None,
            created_by: "test-node".to_string(),
            created_at: 1000,
            updated_at: 1000,
            claimed_by: None,
            claimed_at: None,
        }
    }

    #[test]
    fn create_and_load() {
        let tmp = tempfile::tempdir().unwrap();
        let store = TaskStore::open(tmp.path()).unwrap();

        let task = make_task("Implement feature X", Priority::High);
        let created = store.create(task).unwrap();
        assert!(!created.id.is_empty());
        assert_eq!(created.title, "Implement feature X");

        let loaded = store.load(&created.id).unwrap();
        assert_eq!(loaded.id, created.id);
        assert_eq!(loaded.title, "Implement feature X");
        assert_eq!(loaded.priority, Priority::High);
        assert_eq!(loaded.status, TaskStatus::Backlog);
        assert_eq!(loaded.created_by, "test-node");
    }

    #[test]
    fn list_sorted_by_priority() {
        let tmp = tempfile::tempdir().unwrap();
        let store = TaskStore::open(tmp.path()).unwrap();

        let low = make_task("Low task", Priority::Low);
        let critical = make_task("Critical task", Priority::Critical);
        let medium = make_task("Medium task", Priority::Medium);

        store.create(low).unwrap();
        store.create(critical).unwrap();
        store.create(medium).unwrap();

        let tasks = store.list().unwrap();
        assert_eq!(tasks.len(), 3);
        assert_eq!(tasks[0].priority, Priority::Critical);
        assert_eq!(tasks[1].priority, Priority::Medium);
        assert_eq!(tasks[2].priority, Priority::Low);
    }

    #[test]
    fn update_status() {
        let tmp = tempfile::tempdir().unwrap();
        let store = TaskStore::open(tmp.path()).unwrap();

        let task = make_task("Update me", Priority::Medium);
        let created = store.create(task).unwrap();

        let mut loaded = store.load(&created.id).unwrap();
        loaded.status = TaskStatus::InProgress;
        loaded.updated_at = 2000;
        store.save(&loaded).unwrap();

        let reloaded = store.load(&created.id).unwrap();
        assert_eq!(reloaded.status, TaskStatus::InProgress);
        assert_eq!(reloaded.updated_at, 2000);
    }

    #[test]
    fn delete_task() {
        let tmp = tempfile::tempdir().unwrap();
        let store = TaskStore::open(tmp.path()).unwrap();

        let task = make_task("Delete me", Priority::Low);
        let created = store.create(task).unwrap();

        store.delete(&created.id).unwrap();
        let result = store.load(&created.id);
        assert!(result.is_err());
    }
}
