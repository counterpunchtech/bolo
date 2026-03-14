//! Unified garbage collection orchestrator.
//!
//! Enforces storage budgets from `StorageConfig` across all stores.

use std::path::Path;

use bolo_core::config::StorageConfig;
use bolo_core::BoloError;

/// Summary of a GC run.
#[derive(Debug, Default, serde::Serialize)]
pub struct GcReport {
    /// Chat messages pruned across all channels.
    pub chat_messages_pruned: usize,
    /// CI tasks pruned.
    pub ci_tasks_pruned: usize,
    /// CRDT documents evicted.
    pub docs_evicted: usize,
    /// Names of evicted documents.
    pub docs_evicted_names: Vec<String>,
    /// Blob bytes reclaimed (placeholder — iroh-blobs GC is separate).
    pub blob_bytes_reclaimed: u64,
}

/// Run garbage collection against all stores using the given storage config.
pub fn run_gc(data_dir: &Path, config: &StorageConfig) -> Result<GcReport, BoloError> {
    let mut report = GcReport::default();

    // 1. Chat: prune each channel to configured limit
    let chat_store = bolo_chat::ChatStore::open(data_dir)?;
    report.chat_messages_pruned = chat_store.prune_all_channels(config.chat_history_per_channel)?;

    // 2. CI: prune old tasks
    let ci_store = bolo_ci::CiStore::open(data_dir)?;
    report.ci_tasks_pruned = ci_store.prune_tasks(config.ci_results_retain)?;

    // 3. Docs: evict LRU if over budget
    let doc_store = bolo_docs::DocStore::open(data_dir)?;
    report.docs_evicted_names = doc_store.evict_lru(config.max_docs)?;
    report.docs_evicted = report.docs_evicted_names.len();

    // 4. Blobs: budget check is deferred to iroh-blobs GC (requires async runtime)
    // The blob store uses iroh-blobs FsStore which has its own GC mechanism.
    // We'll hook into that separately in the daemon's async context.

    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gc_empty_stores() {
        let tmp = tempfile::tempdir().unwrap();
        let config = StorageConfig::default();
        let report = run_gc(tmp.path(), &config).unwrap();
        assert_eq!(report.chat_messages_pruned, 0);
        assert_eq!(report.ci_tasks_pruned, 0);
        assert_eq!(report.docs_evicted, 0);
    }

    #[test]
    fn gc_prunes_chat() {
        let tmp = tempfile::tempdir().unwrap();
        let chat = bolo_chat::ChatStore::open(tmp.path()).unwrap();
        chat.join_channel("test").unwrap();
        for i in 0..20 {
            let msg = bolo_chat::ChatMessage {
                id: bolo_chat::ChatMessage::compute_id("test", "node", i, &format!("m{i}")),
                channel: "test".to_string(),
                sender: "node".to_string(),
                timestamp: i,
                content: format!("m{i}"),
                parent: None,
                blob: None,
                signature: "sig".to_string(),
            };
            chat.append(&msg).unwrap();
        }

        let config = StorageConfig {
            chat_history_per_channel: 5,
            ..StorageConfig::default()
        };
        let report = run_gc(tmp.path(), &config).unwrap();
        assert_eq!(report.chat_messages_pruned, 15);
    }
}
