use std::path::{Path, PathBuf};

use bolo_core::BoloError;

use crate::message::ChatMessage;

/// Persistent chat message store.
///
/// Layout: `{data_dir}/chat/{channel}/` with one JSON file per message.
pub struct ChatStore {
    chat_dir: PathBuf,
}

impl ChatStore {
    /// Open or create the chat store under the given data directory.
    pub fn open(data_dir: &Path) -> Result<Self, BoloError> {
        let chat_dir = data_dir.join("chat");
        std::fs::create_dir_all(&chat_dir)?;
        Ok(Self { chat_dir })
    }

    /// Get the directory for a specific channel.
    fn channel_dir(&self, channel: &str) -> PathBuf {
        self.chat_dir.join(sanitize_channel_name(channel))
    }

    /// Join a channel (creates the directory and a marker file).
    pub fn join_channel(&self, channel: &str) -> Result<(), BoloError> {
        let dir = self.channel_dir(channel);
        std::fs::create_dir_all(&dir)?;
        // Write a metadata file so we know the original channel name
        let meta_path = dir.join(".channel");
        if !meta_path.exists() {
            std::fs::write(&meta_path, channel)?;
        }
        Ok(())
    }

    /// Leave a channel (removes the directory).
    pub fn leave_channel(&self, channel: &str) -> Result<(), BoloError> {
        let dir = self.channel_dir(channel);
        if dir.exists() {
            std::fs::remove_dir_all(&dir)?;
        }
        Ok(())
    }

    /// List all joined channels.
    pub fn list_channels(&self) -> Result<Vec<String>, BoloError> {
        let mut channels = Vec::new();
        if !self.chat_dir.exists() {
            return Ok(channels);
        }
        for entry in std::fs::read_dir(&self.chat_dir)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                let meta_path = entry.path().join(".channel");
                if meta_path.exists() {
                    let name = std::fs::read_to_string(&meta_path)?;
                    channels.push(name.trim().to_string());
                } else {
                    // Fallback: use directory name
                    if let Some(name) = entry.file_name().to_str() {
                        channels.push(name.to_string());
                    }
                }
            }
        }
        channels.sort();
        Ok(channels)
    }

    /// Append a message to a channel's log.
    pub fn append(&self, msg: &ChatMessage) -> Result<(), BoloError> {
        let dir = self.channel_dir(&msg.channel);
        std::fs::create_dir_all(&dir)?;
        let filename = format!("{}_{}.json", msg.timestamp, msg.id);
        let path = dir.join(filename);
        let json = serde_json::to_string_pretty(msg)
            .map_err(|e| BoloError::Serialization(format!("serialize message: {e}")))?;
        std::fs::write(&path, json)?;
        Ok(())
    }

    /// Check if a message already exists in the store.
    pub fn has_message(&self, channel: &str, msg_id: &str) -> bool {
        let dir = self.channel_dir(channel);
        if !dir.exists() {
            return false;
        }
        // Scan for file containing the message ID
        if let Ok(entries) = std::fs::read_dir(&dir) {
            for entry in entries.flatten() {
                let name = entry.file_name();
                let name = name.to_string_lossy();
                if name.contains(msg_id) && name.ends_with(".json") {
                    return true;
                }
            }
        }
        false
    }

    /// Get message history for a channel, ordered by timestamp (newest last).
    /// Returns up to `limit` messages. If `limit` is 0, returns all.
    pub fn history(&self, channel: &str, limit: usize) -> Result<Vec<ChatMessage>, BoloError> {
        let dir = self.channel_dir(channel);
        if !dir.exists() {
            return Ok(Vec::new());
        }

        let mut messages = Vec::new();
        for entry in std::fs::read_dir(&dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) == Some("json") {
                let json = std::fs::read_to_string(&path)?;
                if let Ok(msg) = serde_json::from_str::<ChatMessage>(&json) {
                    messages.push(msg);
                }
            }
        }

        // Sort by timestamp ascending
        messages.sort_by_key(|m| m.timestamp);

        if limit > 0 && messages.len() > limit {
            // Return the last `limit` messages
            messages = messages.split_off(messages.len() - limit);
        }

        Ok(messages)
    }

    /// Get messages in a channel with timestamp strictly greater than `since`.
    /// Returns messages sorted by timestamp ascending.
    pub fn messages_since(&self, channel: &str, since: u64) -> Result<Vec<ChatMessage>, BoloError> {
        let all = self.history(channel, 0)?;
        Ok(all.into_iter().filter(|m| m.timestamp > since).collect())
    }

    /// Get the latest message timestamp in a channel, or None if empty.
    pub fn latest_timestamp(&self, channel: &str) -> Result<Option<u64>, BoloError> {
        let dir = self.channel_dir(channel);
        if !dir.exists() {
            return Ok(None);
        }
        let mut latest: Option<u64> = None;
        for entry in std::fs::read_dir(&dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.ends_with(".json") {
                // Filename format: {timestamp}_{id}.json — parse the timestamp prefix
                if let Some(ts_str) = name.split('_').next() {
                    if let Ok(ts) = ts_str.parse::<u64>() {
                        latest = Some(latest.map_or(ts, |l: u64| l.max(ts)));
                    }
                }
            }
        }
        Ok(latest)
    }

    /// Count messages in a channel.
    pub fn count(&self, channel: &str) -> Result<usize, BoloError> {
        let dir = self.channel_dir(channel);
        if !dir.exists() {
            return Ok(0);
        }
        let count = std::fs::read_dir(&dir)?
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().and_then(|ext| ext.to_str()) == Some("json"))
            .count();
        Ok(count)
    }

    /// Prune a channel to keep only the newest `keep` messages.
    /// Returns the number of messages deleted.
    pub fn prune_channel(&self, channel: &str, keep: usize) -> Result<usize, BoloError> {
        let dir = self.channel_dir(channel);
        if !dir.exists() {
            return Ok(0);
        }
        // Collect JSON files sorted by name (timestamp_id.json — lexicographic = chronological)
        let mut files: Vec<_> = std::fs::read_dir(&dir)?
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().and_then(|ext| ext.to_str()) == Some("json"))
            .map(|e| e.path())
            .collect();
        if files.len() <= keep {
            return Ok(0);
        }
        files.sort();
        let to_delete = files.len() - keep;
        let mut deleted = 0;
        for path in files.into_iter().take(to_delete) {
            std::fs::remove_file(&path)?;
            deleted += 1;
        }
        Ok(deleted)
    }

    /// Prune all channels to keep only the newest `keep` messages per channel.
    /// Returns the total number of messages deleted.
    pub fn prune_all_channels(&self, keep: usize) -> Result<usize, BoloError> {
        let channels = self.list_channels()?;
        let mut total = 0;
        for channel in &channels {
            total += self.prune_channel(channel, keep)?;
        }
        Ok(total)
    }
}

/// Sanitize channel name for use as a directory name.
fn sanitize_channel_name(channel: &str) -> String {
    channel
        .chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_msg(channel: &str, sender: &str, content: &str, ts: u64) -> ChatMessage {
        let id = ChatMessage::compute_id(channel, sender, ts, content);
        ChatMessage {
            id,
            channel: channel.to_string(),
            sender: sender.to_string(),
            timestamp: ts,
            content: content.to_string(),
            parent: None,
            blob: None,
            signature: "deadbeef".to_string(),
        }
    }

    #[test]
    fn join_and_list_channels() {
        let tmp = tempfile::tempdir().unwrap();
        let store = ChatStore::open(tmp.path()).unwrap();

        store.join_channel("development").unwrap();
        store.join_channel("alerts").unwrap();

        let channels = store.list_channels().unwrap();
        assert_eq!(channels, vec!["alerts", "development"]);
    }

    #[test]
    fn append_and_history() {
        let tmp = tempfile::tempdir().unwrap();
        let store = ChatStore::open(tmp.path()).unwrap();

        let msg1 = make_msg("dev", "node-a", "hello", 1000);
        let msg2 = make_msg("dev", "node-b", "world", 2000);
        let msg3 = make_msg("dev", "node-a", "third", 3000);

        store.append(&msg1).unwrap();
        store.append(&msg2).unwrap();
        store.append(&msg3).unwrap();

        let history = store.history("dev", 0).unwrap();
        assert_eq!(history.len(), 3);
        assert_eq!(history[0].content, "hello");
        assert_eq!(history[1].content, "world");
        assert_eq!(history[2].content, "third");
    }

    #[test]
    fn history_with_limit() {
        let tmp = tempfile::tempdir().unwrap();
        let store = ChatStore::open(tmp.path()).unwrap();

        for i in 0..10 {
            let msg = make_msg("dev", "node-a", &format!("msg-{i}"), 1000 + i);
            store.append(&msg).unwrap();
        }

        let history = store.history("dev", 3).unwrap();
        assert_eq!(history.len(), 3);
        assert_eq!(history[0].content, "msg-7");
        assert_eq!(history[2].content, "msg-9");
    }

    #[test]
    fn has_message() {
        let tmp = tempfile::tempdir().unwrap();
        let store = ChatStore::open(tmp.path()).unwrap();

        let msg = make_msg("dev", "node-a", "hello", 1000);
        let id = msg.id.clone();
        store.append(&msg).unwrap();

        assert!(store.has_message("dev", &id));
        assert!(!store.has_message("dev", "nonexistent"));
    }

    #[test]
    fn prune_channel() {
        let tmp = tempfile::tempdir().unwrap();
        let store = ChatStore::open(tmp.path()).unwrap();

        for i in 0..10 {
            let msg = make_msg("dev", "node-a", &format!("msg-{i}"), 1000 + i);
            store.append(&msg).unwrap();
        }

        assert_eq!(store.count("dev").unwrap(), 10);
        let deleted = store.prune_channel("dev", 3).unwrap();
        assert_eq!(deleted, 7);
        assert_eq!(store.count("dev").unwrap(), 3);

        // Verify we kept the newest 3
        let history = store.history("dev", 0).unwrap();
        assert_eq!(history.len(), 3);
        assert_eq!(history[0].content, "msg-7");
        assert_eq!(history[2].content, "msg-9");
    }

    #[test]
    fn prune_all_channels() {
        let tmp = tempfile::tempdir().unwrap();
        let store = ChatStore::open(tmp.path()).unwrap();

        store.join_channel("dev").unwrap();
        store.join_channel("ops").unwrap();

        for i in 0..5 {
            store
                .append(&make_msg("dev", "node-a", &format!("d-{i}"), 1000 + i))
                .unwrap();
            store
                .append(&make_msg("ops", "node-b", &format!("o-{i}"), 2000 + i))
                .unwrap();
        }

        let deleted = store.prune_all_channels(2).unwrap();
        assert_eq!(deleted, 6); // 3 from each channel
        assert_eq!(store.count("dev").unwrap(), 2);
        assert_eq!(store.count("ops").unwrap(), 2);
    }

    #[test]
    fn leave_channel() {
        let tmp = tempfile::tempdir().unwrap();
        let store = ChatStore::open(tmp.path()).unwrap();

        store.join_channel("temp").unwrap();
        assert_eq!(store.list_channels().unwrap().len(), 1);

        store.leave_channel("temp").unwrap();
        assert_eq!(store.list_channels().unwrap().len(), 0);
    }

    #[test]
    fn messages_since() {
        let tmp = tempfile::tempdir().unwrap();
        let store = ChatStore::open(tmp.path()).unwrap();

        for i in 0..5 {
            let msg = make_msg("dev", "node-a", &format!("msg-{i}"), 1000 + i * 100);
            store.append(&msg).unwrap();
        }

        let since = store.messages_since("dev", 1200).unwrap();
        assert_eq!(since.len(), 2); // timestamps 1300 and 1400
        assert_eq!(since[0].content, "msg-3");
        assert_eq!(since[1].content, "msg-4");

        let all = store.messages_since("dev", 0).unwrap();
        assert_eq!(all.len(), 5);

        let none = store.messages_since("dev", 1400).unwrap();
        assert_eq!(none.len(), 0);
    }

    #[test]
    fn latest_timestamp() {
        let tmp = tempfile::tempdir().unwrap();
        let store = ChatStore::open(tmp.path()).unwrap();

        assert_eq!(store.latest_timestamp("dev").unwrap(), None);

        store.join_channel("dev").unwrap();
        assert_eq!(store.latest_timestamp("dev").unwrap(), None);

        store
            .append(&make_msg("dev", "node-a", "first", 1000))
            .unwrap();
        assert_eq!(store.latest_timestamp("dev").unwrap(), Some(1000));

        store
            .append(&make_msg("dev", "node-b", "second", 2000))
            .unwrap();
        assert_eq!(store.latest_timestamp("dev").unwrap(), Some(2000));

        // Earlier timestamp doesn't change the latest
        store
            .append(&make_msg("dev", "node-c", "old", 500))
            .unwrap();
        assert_eq!(store.latest_timestamp("dev").unwrap(), Some(2000));
    }

    #[test]
    fn history_sync_protocol() {
        // Simulates two nodes: node A has messages, node B joins later and syncs.
        let tmp_a = tempfile::tempdir().unwrap();
        let tmp_b = tempfile::tempdir().unwrap();
        let store_a = ChatStore::open(tmp_a.path()).unwrap();
        let store_b = ChatStore::open(tmp_b.path()).unwrap();

        // Node A sends 5 messages
        store_a.join_channel("dev").unwrap();
        for i in 0..5 {
            store_a
                .append(&make_msg(
                    "dev",
                    "node-a",
                    &format!("msg-{i}"),
                    1000 + i * 100,
                ))
                .unwrap();
        }

        // Node B joins later — has no messages
        store_b.join_channel("dev").unwrap();
        assert_eq!(store_b.latest_timestamp("dev").unwrap(), None);

        // Simulate sync: B asks for messages since its latest (0)
        let since_b = store_b.latest_timestamp("dev").unwrap().unwrap_or(0);
        let missed = store_a.messages_since("dev", since_b).unwrap();
        assert_eq!(missed.len(), 5);

        // B ingests the missed messages, deduplicating by ID
        for msg in &missed {
            if !store_b.has_message("dev", &msg.id) {
                store_b.append(msg).unwrap();
            }
        }
        assert_eq!(store_b.count("dev").unwrap(), 5);

        // Verify content matches
        let history_a = store_a.history("dev", 0).unwrap();
        let history_b = store_b.history("dev", 0).unwrap();
        for (a, b) in history_a.iter().zip(history_b.iter()) {
            assert_eq!(a.id, b.id);
            assert_eq!(a.content, b.content);
            assert_eq!(a.timestamp, b.timestamp);
        }

        // Simulate partial sync: B already has some messages, A sends more
        store_a
            .append(&make_msg("dev", "node-a", "msg-5", 1500))
            .unwrap();
        store_a
            .append(&make_msg("dev", "node-a", "msg-6", 1600))
            .unwrap();

        let since_b = store_b.latest_timestamp("dev").unwrap().unwrap_or(0);
        assert_eq!(since_b, 1400); // B's latest is msg-4 at 1400
        let missed = store_a.messages_since("dev", since_b).unwrap();
        assert_eq!(missed.len(), 2); // only msg-5 and msg-6

        for msg in &missed {
            if !store_b.has_message("dev", &msg.id) {
                store_b.append(msg).unwrap();
            }
        }
        assert_eq!(store_b.count("dev").unwrap(), 7);
    }
}
