//! Local document store backed by the filesystem.
//!
//! Each document is stored as a Loro snapshot file under `<data_dir>/docs/<path>.loro`.

use std::path::{Path, PathBuf};

use bolo_core::error::BoloError;
use loro::LoroDoc;

/// A filesystem-backed document store.
pub struct DocStore {
    docs_dir: PathBuf,
}

impl DocStore {
    /// Open or create a document store in the given data directory.
    pub fn open(data_dir: &Path) -> Result<Self, BoloError> {
        let docs_dir = data_dir.join("docs");
        std::fs::create_dir_all(&docs_dir)?;
        Ok(Self { docs_dir })
    }

    /// Resolve the on-disk path for a document.
    fn doc_path(&self, name: &str) -> PathBuf {
        self.docs_dir.join(format!("{name}.loro"))
    }

    /// Create a new empty document. Returns error if it already exists.
    pub fn create(&self, name: &str) -> Result<LoroDoc, BoloError> {
        let path = self.doc_path(name);
        if path.exists() {
            return Err(BoloError::ConfigError(format!(
                "document already exists: {name}"
            )));
        }
        let doc = LoroDoc::new();
        self.save(name, &doc)?;
        Ok(doc)
    }

    /// Load an existing document. Returns error if not found.
    pub fn load(&self, name: &str) -> Result<LoroDoc, BoloError> {
        let path = self.doc_path(name);
        if !path.exists() {
            return Err(BoloError::DocumentNotFound(name.to_string()));
        }
        let bytes = std::fs::read(&path)?;
        LoroDoc::from_snapshot(&bytes)
            .map_err(|e| BoloError::Serialization(format!("failed to load document: {e}")))
    }

    /// Save a document to disk as a snapshot.
    pub fn save(&self, name: &str, doc: &LoroDoc) -> Result<(), BoloError> {
        let path = self.doc_path(name);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let snapshot = doc
            .export(loro::ExportMode::Snapshot)
            .map_err(|e| BoloError::Serialization(format!("failed to export snapshot: {e}")))?;
        std::fs::write(&path, snapshot)?;
        Ok(())
    }

    /// Delete a document.
    pub fn delete(&self, name: &str) -> Result<(), BoloError> {
        let path = self.doc_path(name);
        if !path.exists() {
            return Err(BoloError::DocumentNotFound(name.to_string()));
        }
        std::fs::remove_file(&path)?;
        Ok(())
    }

    /// List all document names.
    pub fn list(&self) -> Result<Vec<String>, BoloError> {
        let mut names = Vec::new();
        if !self.docs_dir.exists() {
            return Ok(names);
        }
        Self::list_recursive(&self.docs_dir, &self.docs_dir, &mut names)?;
        names.sort();
        Ok(names)
    }

    fn list_recursive(base: &Path, dir: &Path, names: &mut Vec<String>) -> Result<(), BoloError> {
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                Self::list_recursive(base, &path, names)?;
            } else if path.extension().and_then(|e| e.to_str()) == Some("loro") {
                if let Ok(rel) = path.strip_prefix(base) {
                    let name = rel.with_extension("").to_string_lossy().to_string();
                    names.push(name);
                }
            }
        }
        Ok(())
    }

    /// Check if a document exists.
    pub fn exists(&self, name: &str) -> bool {
        self.doc_path(name).exists()
    }

    /// Count all documents in the store.
    pub fn count(&self) -> Result<usize, BoloError> {
        Ok(self.list()?.len())
    }

    /// Evict documents by LRU (least-recently-modified file), keeping `keep` docs.
    /// Returns names of evicted documents.
    pub fn evict_lru(&self, keep: usize) -> Result<Vec<String>, BoloError> {
        let names = self.list()?;
        if names.len() <= keep {
            return Ok(Vec::new());
        }
        // Collect (name, modified_time) pairs
        let mut entries: Vec<(String, std::time::SystemTime)> = Vec::new();
        for name in &names {
            let path = self.doc_path(name);
            let modified = std::fs::metadata(&path)?
                .modified()
                .unwrap_or(std::time::UNIX_EPOCH);
            entries.push((name.clone(), modified));
        }
        // Sort by modified time ascending (oldest first)
        entries.sort_by_key(|(_, t)| *t);
        let to_evict = entries.len() - keep;
        let mut evicted = Vec::new();
        for (name, _) in entries.into_iter().take(to_evict) {
            self.delete(&name)?;
            evicted.push(name);
        }
        Ok(evicted)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_load_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let store = DocStore::open(tmp.path()).unwrap();

        let doc = store.create("test/doc1").unwrap();
        let map = doc.get_map("meta");
        map.insert("title", "Hello").unwrap();
        doc.commit();
        store.save("test/doc1", &doc).unwrap();

        let loaded = store.load("test/doc1").unwrap();
        let map = loaded.get_map("meta");
        assert_eq!(
            map.get("title")
                .unwrap()
                .into_value()
                .unwrap()
                .into_string()
                .unwrap()
                .to_string(),
            "Hello"
        );
    }

    #[test]
    fn list_documents() {
        let tmp = tempfile::tempdir().unwrap();
        let store = DocStore::open(tmp.path()).unwrap();

        store.create("alpha").unwrap();
        store.create("beta").unwrap();
        store.create("nested/gamma").unwrap();

        let names = store.list().unwrap();
        assert_eq!(names, vec!["alpha", "beta", "nested/gamma"]);
    }

    #[test]
    fn evict_lru() {
        let tmp = tempfile::tempdir().unwrap();
        let store = DocStore::open(tmp.path()).unwrap();

        store.create("oldest").unwrap();
        // Small delay to ensure different modification times
        std::thread::sleep(std::time::Duration::from_millis(50));
        store.create("middle").unwrap();
        std::thread::sleep(std::time::Duration::from_millis(50));
        store.create("newest").unwrap();

        assert_eq!(store.count().unwrap(), 3);
        let evicted = store.evict_lru(1).unwrap();
        assert_eq!(evicted.len(), 2);
        assert_eq!(store.count().unwrap(), 1);
        // newest should remain
        assert!(store.exists("newest"));
    }

    #[test]
    fn delete_document() {
        let tmp = tempfile::tempdir().unwrap();
        let store = DocStore::open(tmp.path()).unwrap();

        store.create("to_delete").unwrap();
        assert!(store.exists("to_delete"));

        store.delete("to_delete").unwrap();
        assert!(!store.exists("to_delete"));
    }

    /// Verify that multiple save-load-insert cycles accumulate entries correctly.
    /// This simulates the task CRDT pattern: each task.create loads, inserts, saves.
    #[test]
    fn incremental_map_inserts_survive_save_load_cycles() {
        let tmp = tempfile::tempdir().unwrap();
        let store = DocStore::open(tmp.path()).unwrap();

        // Create doc with first entry
        let doc = store.create("tasks").unwrap();
        doc.get_map("data").insert("key1", "value1").unwrap();
        doc.commit();
        store.save("tasks", &doc).unwrap();

        // Load, add second entry
        let doc = store.load("tasks").unwrap();
        doc.get_map("data").insert("key2", "value2").unwrap();
        doc.commit();
        store.save("tasks", &doc).unwrap();

        // Load, add third entry
        let doc = store.load("tasks").unwrap();
        doc.get_map("data").insert("key3", "value3").unwrap();
        doc.commit();
        store.save("tasks", &doc).unwrap();

        // Verify all 3 entries survive
        let doc = store.load("tasks").unwrap();
        let map = doc.get_map("data");
        let value = map.get_value();
        let json = serde_json::to_value(&value).unwrap();
        let obj = json.as_object().unwrap();
        assert_eq!(obj.len(), 3, "map should have 3 entries: {json}");
        assert_eq!(obj["key1"], "value1");
        assert_eq!(obj["key2"], "value2");
        assert_eq!(obj["key3"], "value3");
    }

    /// Verify that snapshot export/import preserves all map entries (simulates cross-node sync).
    #[test]
    fn snapshot_sync_preserves_map_entries() {
        let tmp_a = tempfile::tempdir().unwrap();
        let tmp_b = tempfile::tempdir().unwrap();
        let store_a = DocStore::open(tmp_a.path()).unwrap();
        let store_b = DocStore::open(tmp_b.path()).unwrap();

        // Node A: create doc with 3 entries (each via save-load cycle)
        let doc = store_a.create("tasks").unwrap();
        doc.get_map("data").insert("t1", r#"{"id":"t1"}"#).unwrap();
        doc.commit();
        store_a.save("tasks", &doc).unwrap();

        let doc = store_a.load("tasks").unwrap();
        doc.get_map("data").insert("t2", r#"{"id":"t2"}"#).unwrap();
        doc.commit();
        store_a.save("tasks", &doc).unwrap();

        let doc = store_a.load("tasks").unwrap();
        doc.get_map("data").insert("t3", r#"{"id":"t3"}"#).unwrap();
        doc.commit();
        store_a.save("tasks", &doc).unwrap();

        // Verify A has all 3
        let doc_a = store_a.load("tasks").unwrap();
        let val_a = doc_a.get_map("data").get_value();
        let json_a = serde_json::to_value(&val_a).unwrap();
        assert_eq!(
            json_a.as_object().unwrap().len(),
            3,
            "Node A should have 3: {json_a}"
        );

        // Node B: import A's snapshot
        let snap = doc_a.export(loro::ExportMode::Snapshot).unwrap();
        let doc_b = LoroDoc::from_snapshot(&snap).unwrap();
        store_b.save("tasks", &doc_b).unwrap();

        // Verify B has all 3
        let doc_b = store_b.load("tasks").unwrap();
        let val_b = doc_b.get_map("data").get_value();
        let json_b = serde_json::to_value(&val_b).unwrap();
        assert_eq!(
            json_b.as_object().unwrap().len(),
            3,
            "Node B should have 3: {json_b}"
        );
    }
}
