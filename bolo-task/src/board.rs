use crate::store::TaskStore;
use crate::types::*;

/// Board view of tasks grouped by status.
pub struct Board {
    pub backlog: Vec<Task>,
    pub ready: Vec<Task>,
    pub in_progress: Vec<Task>,
    pub review: Vec<Task>,
    pub done: Vec<Task>,
}

impl Board {
    /// Build a board view from all tasks in the store.
    pub fn from_store(store: &TaskStore) -> Result<Self, bolo_core::BoloError> {
        let tasks = store.list()?;
        Ok(Self::from_tasks(tasks))
    }

    /// Build a board from a list of tasks.
    pub fn from_tasks(tasks: Vec<Task>) -> Self {
        let mut board = Self {
            backlog: Vec::new(),
            ready: Vec::new(),
            in_progress: Vec::new(),
            review: Vec::new(),
            done: Vec::new(),
        };
        for task in tasks {
            match task.status {
                TaskStatus::Backlog => board.backlog.push(task),
                TaskStatus::Ready => board.ready.push(task),
                TaskStatus::InProgress => board.in_progress.push(task),
                TaskStatus::Review => board.review.push(task),
                TaskStatus::Done => board.done.push(task),
            }
        }
        board
    }

    /// Check if a task's dependencies are all done.
    pub fn dependencies_met(&self, task: &Task) -> bool {
        if task.dependencies.is_empty() {
            return true;
        }
        let done_ids: std::collections::HashSet<&str> =
            self.done.iter().map(|t| t.id.as_str()).collect();
        task.dependencies
            .iter()
            .all(|dep| done_ids.contains(dep.as_str()))
    }

    /// Get the total task count.
    pub fn total(&self) -> usize {
        self.backlog.len()
            + self.ready.len()
            + self.in_progress.len()
            + self.review.len()
            + self.done.len()
    }
}

impl serde::Serialize for Board {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(Some(5))?;
        map.serialize_entry("backlog", &self.backlog)?;
        map.serialize_entry("ready", &self.ready)?;
        map.serialize_entry("in_progress", &self.in_progress)?;
        map.serialize_entry("review", &self.review)?;
        map.serialize_entry("done", &self.done)?;
        map.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_task(id: &str, title: &str, status: TaskStatus) -> Task {
        Task {
            id: id.to_string(),
            title: title.to_string(),
            status,
            assignee: None,
            priority: Priority::Medium,
            spec_doc: None,
            dependencies: Vec::new(),
            commits: Vec::new(),
            ci_results: Vec::new(),
            review_doc: None,
            created_by: "test".to_string(),
            created_at: 1000,
            updated_at: 1000,
            claimed_by: None,
            claimed_at: None,
        }
    }

    #[test]
    fn board_from_tasks_groups_by_status() {
        let tasks = vec![
            make_task("a", "Backlog task", TaskStatus::Backlog),
            make_task("b", "Ready task", TaskStatus::Ready),
            make_task("c", "In-progress task", TaskStatus::InProgress),
            make_task("d", "Review task", TaskStatus::Review),
            make_task("e", "Done task", TaskStatus::Done),
        ];
        let board = Board::from_tasks(tasks);
        assert_eq!(board.backlog.len(), 1);
        assert_eq!(board.ready.len(), 1);
        assert_eq!(board.in_progress.len(), 1);
        assert_eq!(board.review.len(), 1);
        assert_eq!(board.done.len(), 1);
        assert_eq!(board.total(), 5);
    }

    #[test]
    fn dependencies_met_when_all_done() {
        let tasks = vec![
            make_task("dep1", "Dep 1", TaskStatus::Done),
            make_task("dep2", "Dep 2", TaskStatus::Done),
        ];
        let board = Board::from_tasks(tasks);

        let task = Task {
            dependencies: vec!["dep1".to_string(), "dep2".to_string()],
            ..make_task("main", "Main task", TaskStatus::Ready)
        };
        assert!(board.dependencies_met(&task));
    }

    #[test]
    fn dependencies_not_met() {
        let tasks = vec![
            make_task("dep1", "Dep 1", TaskStatus::Done),
            make_task("dep2", "Dep 2", TaskStatus::InProgress),
        ];
        let board = Board::from_tasks(tasks);

        let task = Task {
            dependencies: vec!["dep1".to_string(), "dep2".to_string()],
            ..make_task("main", "Main task", TaskStatus::Ready)
        };
        assert!(!board.dependencies_met(&task));
    }
}
