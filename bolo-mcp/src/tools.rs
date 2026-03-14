use crate::protocol::{ToolDefinition, ToolResult};
use serde_json::json;

/// Build the list of all available MCP tools (1:1 mapping to CLI commands).
pub fn tool_definitions() -> Vec<ToolDefinition> {
    vec![
        // Daemon
        tool(
            "bolo_daemon_init",
            "Initialize the bolo daemon (create keypair and config)",
            json!({
                "type": "object",
                "properties": {
                    "config": { "type": "string", "description": "Config directory path" }
                }
            }),
        ),
        tool(
            "bolo_daemon_start",
            "Start the bolo daemon",
            json!({
                "type": "object",
                "properties": {
                    "config": { "type": "string", "description": "Config directory path" }
                }
            }),
        ),
        tool(
            "bolo_daemon_stop",
            "Stop the bolo daemon",
            json!({
                "type": "object",
                "properties": {
                    "config": { "type": "string", "description": "Config directory path" }
                }
            }),
        ),
        tool(
            "bolo_daemon_status",
            "Get daemon status",
            json!({
                "type": "object",
                "properties": {
                    "config": { "type": "string", "description": "Config directory path" }
                }
            }),
        ),
        // Identity
        tool(
            "bolo_id_show",
            "Show this node's identity",
            json!({
                "type": "object",
                "properties": {
                    "config": { "type": "string", "description": "Config directory path" }
                }
            }),
        ),
        // Peer
        tool(
            "bolo_peer_list",
            "List known peers",
            json!({
                "type": "object",
                "properties": {
                    "config": { "type": "string", "description": "Config directory path" }
                }
            }),
        ),
        tool(
            "bolo_peer_add",
            "Add a peer",
            json!({
                "type": "object",
                "properties": {
                    "node_id": { "type": "string", "description": "Node ID to add" },
                    "config": { "type": "string", "description": "Config directory path" }
                },
                "required": ["node_id"]
            }),
        ),
        tool(
            "bolo_peer_remove",
            "Remove a peer",
            json!({
                "type": "object",
                "properties": {
                    "node_id": { "type": "string", "description": "Node ID to remove" },
                    "config": { "type": "string", "description": "Config directory path" }
                },
                "required": ["node_id"]
            }),
        ),
        tool(
            "bolo_peer_ping",
            "Ping a peer",
            json!({
                "type": "object",
                "properties": {
                    "node_id": { "type": "string", "description": "Node ID to ping" },
                    "config": { "type": "string", "description": "Config directory path" }
                },
                "required": ["node_id"]
            }),
        ),
        tool(
            "bolo_peer_bench",
            "Benchmark throughput to a peer",
            json!({
                "type": "object",
                "properties": {
                    "node_id": { "type": "string", "description": "Node ID to benchmark" },
                    "size": { "type": "integer", "description": "Payload size in MB (default 10)" },
                    "config": { "type": "string", "description": "Config directory path" }
                },
                "required": ["node_id"]
            }),
        ),
        // Blob
        tool(
            "bolo_blob_add",
            "Add a file as a blob",
            json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string", "description": "File path to add" },
                    "config": { "type": "string", "description": "Config directory path" }
                },
                "required": ["path"]
            }),
        ),
        tool(
            "bolo_blob_get",
            "Get a blob by hash",
            json!({
                "type": "object",
                "properties": {
                    "hash": { "type": "string", "description": "Blob hash" },
                    "out": { "type": "string", "description": "Output path" },
                    "config": { "type": "string", "description": "Config directory path" }
                },
                "required": ["hash"]
            }),
        ),
        tool(
            "bolo_blob_list",
            "List all blobs",
            json!({
                "type": "object",
                "properties": {
                    "config": { "type": "string", "description": "Config directory path" }
                }
            }),
        ),
        tool(
            "bolo_blob_share",
            "Share a blob with a peer",
            json!({
                "type": "object",
                "properties": {
                    "hash": { "type": "string", "description": "Blob hash" },
                    "peer": { "type": "string", "description": "Peer node ID" },
                    "config": { "type": "string", "description": "Config directory path" }
                },
                "required": ["hash", "peer"]
            }),
        ),
        tool(
            "bolo_blob_fetch",
            "Fetch a blob from a peer",
            json!({
                "type": "object",
                "properties": {
                    "hash": { "type": "string", "description": "Blob hash" },
                    "peer": { "type": "string", "description": "Peer node ID" },
                    "config": { "type": "string", "description": "Config directory path" }
                },
                "required": ["hash", "peer"]
            }),
        ),
        // Doc
        tool(
            "bolo_doc_new",
            "Create a new document",
            json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string", "description": "Document path" },
                    "config": { "type": "string", "description": "Config directory path" }
                },
                "required": ["path"]
            }),
        ),
        tool(
            "bolo_doc_edit",
            "Edit a document",
            json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string", "description": "Document path" },
                    "content": { "type": "string", "description": "New content to set" },
                    "config": { "type": "string", "description": "Config directory path" }
                },
                "required": ["path"]
            }),
        ),
        tool(
            "bolo_doc_read",
            "Read a document",
            json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string", "description": "Document path" },
                    "config": { "type": "string", "description": "Config directory path" }
                },
                "required": ["path"]
            }),
        ),
        tool(
            "bolo_doc_list",
            "List all documents",
            json!({
                "type": "object",
                "properties": {
                    "config": { "type": "string", "description": "Config directory path" }
                }
            }),
        ),
        tool(
            "bolo_doc_history",
            "Show document history",
            json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string", "description": "Document path" },
                    "config": { "type": "string", "description": "Config directory path" }
                },
                "required": ["path"]
            }),
        ),
        tool(
            "bolo_doc_diff",
            "Show document diff",
            json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string", "description": "Document path" },
                    "config": { "type": "string", "description": "Config directory path" }
                },
                "required": ["path"]
            }),
        ),
        tool(
            "bolo_doc_export",
            "Export a document",
            json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string", "description": "Document path" },
                    "output": { "type": "string", "description": "Output file path" },
                    "config": { "type": "string", "description": "Config directory path" }
                },
                "required": ["path"]
            }),
        ),
        tool(
            "bolo_doc_import",
            "Import a document",
            json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string", "description": "Document path" },
                    "file": { "type": "string", "description": "Input file path" },
                    "config": { "type": "string", "description": "Config directory path" }
                },
                "required": ["path", "file"]
            }),
        ),
        // Pub/Sub
        tool(
            "bolo_pub_send",
            "Publish a message to a topic",
            json!({
                "type": "object",
                "properties": {
                    "topic": { "type": "string", "description": "Topic name" },
                    "message": { "type": "string", "description": "Message content" },
                    "config": { "type": "string", "description": "Config directory path" }
                },
                "required": ["topic", "message"]
            }),
        ),
        tool(
            "bolo_pub_subscribe",
            "Subscribe to a topic",
            json!({
                "type": "object",
                "properties": {
                    "topic": { "type": "string", "description": "Topic name" },
                    "config": { "type": "string", "description": "Config directory path" }
                },
                "required": ["topic"]
            }),
        ),
        tool(
            "bolo_pub_topics",
            "List active topics",
            json!({
                "type": "object",
                "properties": {
                    "config": { "type": "string", "description": "Config directory path" }
                }
            }),
        ),
        // Git
        tool(
            "bolo_git_status",
            "Show git repo status for mesh sync",
            json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string", "description": "Repository path" },
                    "config": { "type": "string", "description": "Config directory path" }
                }
            }),
        ),
        tool(
            "bolo_git_objects",
            "List git objects in HEAD",
            json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string", "description": "Repository path" },
                    "config": { "type": "string", "description": "Config directory path" }
                }
            }),
        ),
        tool(
            "bolo_git_log",
            "Show git commit log",
            json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string", "description": "Repository path" },
                    "count": { "type": "integer", "description": "Number of commits to show" },
                    "config": { "type": "string", "description": "Config directory path" }
                }
            }),
        ),
        tool(
            "bolo_git_refs",
            "List git refs",
            json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string", "description": "Repository path" },
                    "config": { "type": "string", "description": "Config directory path" }
                }
            }),
        ),
        // Git (push/pull/clone)
        tool(
            "bolo_git_push",
            "Sync local git objects to mesh",
            json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string", "description": "Repository path" },
                    "config": { "type": "string", "description": "Config directory path" }
                }
            }),
        ),
        tool(
            "bolo_git_pull",
            "Pull git objects from mesh peers",
            json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string", "description": "Repository path" },
                    "config": { "type": "string", "description": "Config directory path" }
                }
            }),
        ),
        tool(
            "bolo_git_clone",
            "Clone a repo from the mesh swarm",
            json!({
                "type": "object",
                "properties": {
                    "url": { "type": "string", "description": "Peer/repo identifier (e.g. <peer-id>/<repo>)" },
                    "dest": { "type": "string", "description": "Destination path" },
                    "config": { "type": "string", "description": "Config directory path" }
                },
                "required": ["url"]
            }),
        ),
        // Review
        tool(
            "bolo_review_show",
            "Show review comments for a commit",
            json!({
                "type": "object",
                "properties": {
                    "commit": { "type": "string", "description": "Commit OID" },
                    "config": { "type": "string", "description": "Config directory path" }
                },
                "required": ["commit"]
            }),
        ),
        tool(
            "bolo_review_comment",
            "Add a review comment",
            json!({
                "type": "object",
                "properties": {
                    "commit": { "type": "string", "description": "Commit OID" },
                    "message": { "type": "string", "description": "Comment body" },
                    "file": { "type": "string", "description": "Attach to a specific file" },
                    "line": { "type": "integer", "description": "Attach to a specific line" },
                    "config": { "type": "string", "description": "Config directory path" }
                },
                "required": ["commit", "message"]
            }),
        ),
        tool(
            "bolo_review_approve",
            "Sign an approval for a commit",
            json!({
                "type": "object",
                "properties": {
                    "commit": { "type": "string", "description": "Commit OID" },
                    "config": { "type": "string", "description": "Config directory path" }
                },
                "required": ["commit"]
            }),
        ),
        tool(
            "bolo_review_reject",
            "Sign a rejection for a commit",
            json!({
                "type": "object",
                "properties": {
                    "commit": { "type": "string", "description": "Commit OID" },
                    "message": { "type": "string", "description": "Rejection reason" },
                    "config": { "type": "string", "description": "Config directory path" }
                },
                "required": ["commit"]
            }),
        ),
        tool(
            "bolo_review_ls",
            "List pending reviews",
            json!({
                "type": "object",
                "properties": {
                    "config": { "type": "string", "description": "Config directory path" }
                }
            }),
        ),
        // Daemon export/import
        tool(
            "bolo_daemon_export",
            "Export full node state",
            json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string", "description": "Output path" },
                    "config": { "type": "string", "description": "Config directory path" }
                },
                "required": ["path"]
            }),
        ),
        tool(
            "bolo_daemon_import",
            "Import node state from archive",
            json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string", "description": "Input path" },
                    "force": { "type": "boolean", "description": "Overwrite existing state" },
                    "config": { "type": "string", "description": "Config directory path" }
                },
                "required": ["path"]
            }),
        ),
        tool(
            "bolo_daemon_upgrade",
            "Upgrade bolo binary from the mesh (OTA update)",
            json!({
                "type": "object",
                "properties": {
                    "platform": { "type": "string", "description": "Target platform (e.g. macos/aarch64). Auto-detected if omitted." },
                    "config": { "type": "string", "description": "Config directory path" }
                }
            }),
        ),
        // Doc (additional commands)
        tool(
            "bolo_doc_set",
            "Set a key in a map document",
            json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string", "description": "Document path" },
                    "key": { "type": "string", "description": "Key" },
                    "value": { "type": "string", "description": "Value" },
                    "config": { "type": "string", "description": "Config directory path" }
                },
                "required": ["path", "key", "value"]
            }),
        ),
        tool(
            "bolo_doc_del",
            "Delete a key from a document",
            json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string", "description": "Document path" },
                    "key": { "type": "string", "description": "Key" },
                    "config": { "type": "string", "description": "Config directory path" }
                },
                "required": ["path", "key"]
            }),
        ),
        tool(
            "bolo_doc_rm",
            "Delete a document",
            json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string", "description": "Document path" },
                    "config": { "type": "string", "description": "Config directory path" }
                },
                "required": ["path"]
            }),
        ),
        tool(
            "bolo_doc_append",
            "Append value to a list document",
            json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string", "description": "Document path" },
                    "value": { "type": "string", "description": "Value to append" },
                    "config": { "type": "string", "description": "Config directory path" }
                },
                "required": ["path", "value"]
            }),
        ),
        tool(
            "bolo_doc_watch",
            "Stream document changes",
            json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string", "description": "Document path" },
                    "config": { "type": "string", "description": "Config directory path" }
                },
                "required": ["path"]
            }),
        ),
        tool(
            "bolo_doc_sync",
            "Force sync a document",
            json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string", "description": "Document path (sync all if omitted)" },
                    "config": { "type": "string", "description": "Config directory path" }
                }
            }),
        ),
        tool(
            "bolo_doc_share",
            "Generate a share ticket for a document",
            json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string", "description": "Document path" },
                    "config": { "type": "string", "description": "Config directory path" }
                },
                "required": ["path"]
            }),
        ),
        tool(
            "bolo_doc_join",
            "Join a shared document",
            json!({
                "type": "object",
                "properties": {
                    "ticket": { "type": "string", "description": "Share ticket" },
                    "config": { "type": "string", "description": "Config directory path" }
                },
                "required": ["ticket"]
            }),
        ),
        tool(
            "bolo_doc_compact",
            "Compact CRDT history",
            json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string", "description": "Document path" },
                    "all": { "type": "boolean", "description": "Compact all documents" },
                    "config": { "type": "string", "description": "Config directory path" }
                },
                "required": ["path"]
            }),
        ),
        // CI
        tool(
            "bolo_ci_run",
            "Trigger a CI build",
            json!({
                "type": "object",
                "properties": {
                    "task_type": { "type": "string", "description": "Task type: build, test, check, clippy, fmt, full (default: full)" },
                    "path": { "type": "string", "description": "Repository/workspace path" },
                    "config": { "type": "string", "description": "Config directory path" }
                }
            }),
        ),
        tool(
            "bolo_ci_status",
            "Show CI build status",
            json!({
                "type": "object",
                "properties": {
                    "config": { "type": "string", "description": "Config directory path" }
                }
            }),
        ),
        tool(
            "bolo_ci_results",
            "View build results",
            json!({
                "type": "object",
                "properties": {
                    "task_id": { "type": "string", "description": "Task hash" },
                    "config": { "type": "string", "description": "Config directory path" }
                },
                "required": ["task_id"]
            }),
        ),
        // Task
        tool(
            "bolo_task_create",
            "Create a new task",
            json!({
                "type": "object",
                "properties": {
                    "title": { "type": "string", "description": "Task title" },
                    "priority": { "type": "string", "description": "Priority: critical, high, medium, low" },
                    "assignee": { "type": "string", "description": "Assignee node ID" },
                    "config": { "type": "string", "description": "Config directory path" }
                },
                "required": ["title"]
            }),
        ),
        tool(
            "bolo_task_assign",
            "Assign a task to a peer",
            json!({
                "type": "object",
                "properties": {
                    "id": { "type": "string", "description": "Task ID" },
                    "peer": { "type": "string", "description": "Peer node ID" },
                    "config": { "type": "string", "description": "Config directory path" }
                },
                "required": ["id", "peer"]
            }),
        ),
        tool(
            "bolo_task_list",
            "Show task board",
            json!({
                "type": "object",
                "properties": {
                    "status": { "type": "string", "description": "Filter by status column" },
                    "config": { "type": "string", "description": "Config directory path" }
                }
            }),
        ),
        tool(
            "bolo_task_show",
            "Show task details",
            json!({
                "type": "object",
                "properties": {
                    "id": { "type": "string", "description": "Task ID" },
                    "config": { "type": "string", "description": "Config directory path" }
                },
                "required": ["id"]
            }),
        ),
        tool(
            "bolo_task_update",
            "Update task status or priority",
            json!({
                "type": "object",
                "properties": {
                    "id": { "type": "string", "description": "Task ID" },
                    "status": { "type": "string", "description": "New status: backlog, ready, in-progress, review, done" },
                    "priority": { "type": "string", "description": "New priority: critical, high, medium, low" },
                    "config": { "type": "string", "description": "Config directory path" }
                },
                "required": ["id"]
            }),
        ),
        tool(
            "bolo_task_link",
            "Link artifacts to a task",
            json!({
                "type": "object",
                "properties": {
                    "id": { "type": "string", "description": "Task ID" },
                    "spec": { "type": "string", "description": "Spec document path" },
                    "commit": { "type": "string", "description": "Commit OID" },
                    "ci_result": { "type": "string", "description": "CI result ID" },
                    "config": { "type": "string", "description": "Config directory path" }
                },
                "required": ["id"]
            }),
        ),
        tool(
            "bolo_task_delete",
            "Delete a task",
            json!({
                "type": "object",
                "properties": {
                    "id": { "type": "string", "description": "Task ID" },
                    "config": { "type": "string", "description": "Config directory path" }
                },
                "required": ["id"]
            }),
        ),
        // Chat
        tool(
            "bolo_chat_join",
            "Join a chat channel",
            json!({
                "type": "object",
                "properties": {
                    "channel": { "type": "string", "description": "Channel name" },
                    "config": { "type": "string", "description": "Config directory path" }
                },
                "required": ["channel"]
            }),
        ),
        tool(
            "bolo_chat_leave",
            "Leave a chat channel",
            json!({
                "type": "object",
                "properties": {
                    "channel": { "type": "string", "description": "Channel name" },
                    "config": { "type": "string", "description": "Config directory path" }
                },
                "required": ["channel"]
            }),
        ),
        tool(
            "bolo_chat_ls",
            "List joined chat channels",
            json!({
                "type": "object",
                "properties": {
                    "config": { "type": "string", "description": "Config directory path" }
                }
            }),
        ),
        tool(
            "bolo_chat_send",
            "Send a message to a chat channel",
            json!({
                "type": "object",
                "properties": {
                    "channel": { "type": "string", "description": "Channel name" },
                    "message": { "type": "string", "description": "Message text" },
                    "reply": { "type": "string", "description": "Reply to message ID (thread)" },
                    "blob": { "type": "string", "description": "Attached blob hash" },
                    "peer": { "type": "string", "description": "Bootstrap peer node ID" },
                    "config": { "type": "string", "description": "Config directory path" }
                },
                "required": ["channel", "message"]
            }),
        ),
        tool(
            "bolo_chat_history",
            "View chat message history for a channel",
            json!({
                "type": "object",
                "properties": {
                    "channel": { "type": "string", "description": "Channel name" },
                    "limit": { "type": "integer", "description": "Number of messages (default 50)" },
                    "config": { "type": "string", "description": "Config directory path" }
                },
                "required": ["channel"]
            }),
        ),
        tool(
            "bolo_chat_watch",
            "Watch for new messages in a chat channel (streaming)",
            json!({
                "type": "object",
                "properties": {
                    "channel": { "type": "string", "description": "Channel name" },
                    "peer": { "type": "string", "description": "Bootstrap peer node ID" },
                    "config": { "type": "string", "description": "Config directory path" }
                },
                "required": ["channel"]
            }),
        ),
        tool(
            "bolo_chat_sync",
            "Sync missed chat messages from peers (requests history since last known message)",
            json!({
                "type": "object",
                "properties": {
                    "channel": { "type": "string", "description": "Channel name (sync all if omitted)" },
                    "config": { "type": "string", "description": "Config directory path" }
                }
            }),
        ),
        // Deploy
        tool(
            "bolo_deploy_build",
            "Cross-compile bolo binary and stage as blob for deployment",
            json!({
                "type": "object",
                "properties": {
                    "target": { "type": "string", "description": "Rust target triple (default: aarch64-unknown-linux-gnu)" },
                    "config": { "type": "string", "description": "Config directory path" }
                }
            }),
        ),
        tool(
            "bolo_deploy_push",
            "Push staged binary to a mesh peer via gossip",
            json!({
                "type": "object",
                "properties": {
                    "peer": { "type": "string", "description": "Target peer node ID" },
                    "config": { "type": "string", "description": "Config directory path" }
                }
            }),
        ),
        tool(
            "bolo_deploy_status",
            "Show staged deploy build info",
            json!({
                "type": "object",
                "properties": {
                    "config": { "type": "string", "description": "Config directory path" }
                }
            }),
        ),
        // Daemon logs
        tool(
            "bolo_daemon_logs",
            "View daemon log output from the local node",
            json!({
                "type": "object",
                "properties": {
                    "lines": { "type": "integer", "description": "Number of lines to show (default: 100)" },
                    "config": { "type": "string", "description": "Config directory path" }
                }
            }),
        ),
        // Peer logs
        tool(
            "bolo_peer_logs",
            "View logs from a remote peer in the mesh",
            json!({
                "type": "object",
                "properties": {
                    "node_id": { "type": "string", "description": "Target peer node ID" },
                    "lines": { "type": "integer", "description": "Number of lines to show (default: 100)" },
                    "config": { "type": "string", "description": "Config directory path" }
                },
                "required": ["node_id"]
            }),
        ),
        // Mesh
        tool(
            "bolo_mesh_status",
            "Show aggregate mesh capabilities (CPU, RAM, storage, GPUs) across all nodes",
            json!({
                "type": "object",
                "properties": {
                    "timeout": { "type": "integer", "description": "Discovery timeout in seconds (default: 5)" },
                    "config": { "type": "string", "description": "Config directory path" }
                }
            }),
        ),
        // Quality
        tool(
            "bolo_quality",
            "Run quality checks on the workspace",
            json!({
                "type": "object",
                "properties": {
                    "fast": { "type": "boolean", "description": "Run only fast checks" },
                    "full": { "type": "boolean", "description": "Run all checks" },
                    "fix": { "type": "boolean", "description": "Auto-fix issues" }
                }
            }),
        ),
    ]
}

/// Filter tool definitions by namespace.
/// If namespaces is empty, return all tools.
pub fn tool_definitions_filtered(namespaces: &[&str]) -> Vec<ToolDefinition> {
    if namespaces.is_empty() {
        return tool_definitions();
    }
    tool_definitions()
        .into_iter()
        .filter(|t| {
            namespaces.iter().any(|ns| {
                t.name.starts_with(&format!("bolo_{ns}_")) || t.name == format!("bolo_{ns}")
            })
        })
        .collect()
}

fn tool(name: &str, description: &str, input_schema: serde_json::Value) -> ToolDefinition {
    ToolDefinition {
        name: name.to_string(),
        description: description.to_string(),
        input_schema,
    }
}

/// Execute a tool by shelling out to `bolo --json <subcommand>`.
pub async fn execute_tool(name: &str, arguments: &serde_json::Value) -> ToolResult {
    let args = match build_cli_args(name, arguments) {
        Ok(args) => args,
        Err(e) => return ToolResult::error(format!("Invalid tool call: {e}")),
    };

    match run_bolo_command(&args).await {
        Ok(output) => ToolResult::text(output),
        Err(e) => ToolResult::error(format!("Command failed: {e}")),
    }
}

/// Map tool name + arguments to CLI args.
pub fn build_cli_args(name: &str, args: &serde_json::Value) -> Result<Vec<String>, String> {
    let config_args = if let Some(config) = args.get("config").and_then(|v| v.as_str()) {
        vec!["--config".to_string(), config.to_string()]
    } else {
        vec![]
    };

    let mut cli_args = vec!["--json".to_string()];
    cli_args.extend(config_args);

    match name {
        // Daemon
        "bolo_daemon_init" => cli_args.extend(["daemon".into(), "init".into()]),
        "bolo_daemon_start" => {
            cli_args.extend(["daemon".into(), "start".into(), "--detach".into()])
        }
        "bolo_daemon_stop" => cli_args.extend(["daemon".into(), "stop".into()]),
        "bolo_daemon_status" => cli_args.extend(["daemon".into(), "status".into()]),
        // Identity
        "bolo_id_show" => cli_args.extend(["id".into(), "show".into()]),
        // Peer
        "bolo_peer_list" => cli_args.extend(["peer".into(), "ls".into()]),
        "bolo_peer_add" => {
            let node_id = args
                .get("node_id")
                .and_then(|v| v.as_str())
                .ok_or("missing node_id")?;
            cli_args.extend(["peer".into(), "add".into(), node_id.into()]);
        }
        "bolo_peer_remove" => {
            let node_id = args
                .get("node_id")
                .and_then(|v| v.as_str())
                .ok_or("missing node_id")?;
            cli_args.extend(["peer".into(), "rm".into(), node_id.into()]);
        }
        "bolo_peer_ping" => {
            let node_id = args
                .get("node_id")
                .and_then(|v| v.as_str())
                .ok_or("missing node_id")?;
            cli_args.extend(["peer".into(), "ping".into(), node_id.into()]);
        }
        "bolo_peer_bench" => {
            let node_id = args
                .get("node_id")
                .and_then(|v| v.as_str())
                .ok_or("missing node_id")?;
            cli_args.extend(["peer".into(), "bench".into(), node_id.into()]);
            if let Some(size) = args.get("size").and_then(|v| v.as_u64()) {
                cli_args.extend(["--size".into(), size.to_string()]);
            }
        }
        // Blob
        "bolo_blob_add" => {
            let path = args
                .get("path")
                .and_then(|v| v.as_str())
                .ok_or("missing path")?;
            cli_args.extend(["blob".into(), "put".into(), path.into()]);
        }
        "bolo_blob_get" => {
            let hash = args
                .get("hash")
                .and_then(|v| v.as_str())
                .ok_or("missing hash")?;
            cli_args.extend(["blob".into(), "get".into(), hash.into()]);
            if let Some(out) = args.get("out").and_then(|v| v.as_str()) {
                cli_args.push(out.into());
            }
        }
        "bolo_blob_list" => cli_args.extend(["blob".into(), "ls".into()]),
        "bolo_blob_share" => {
            let hash = args
                .get("hash")
                .and_then(|v| v.as_str())
                .ok_or("missing hash")?;
            let peer = args
                .get("peer")
                .and_then(|v| v.as_str())
                .ok_or("missing peer")?;
            cli_args.extend(["blob".into(), "fetch".into(), hash.into(), peer.into()]);
        }
        "bolo_blob_fetch" => {
            let hash = args
                .get("hash")
                .and_then(|v| v.as_str())
                .ok_or("missing hash")?;
            let peer = args
                .get("peer")
                .and_then(|v| v.as_str())
                .ok_or("missing peer")?;
            cli_args.extend(["blob".into(), "fetch".into(), hash.into(), peer.into()]);
        }
        // Doc
        "bolo_doc_new" => {
            let path = args
                .get("path")
                .and_then(|v| v.as_str())
                .ok_or("missing path")?;
            cli_args.extend(["doc".into(), "create".into(), path.into()]);
        }
        "bolo_doc_edit" => {
            let path = args
                .get("path")
                .and_then(|v| v.as_str())
                .ok_or("missing path")?;
            cli_args.extend(["doc".into(), "edit".into(), path.into()]);
        }
        "bolo_doc_read" => {
            let path = args
                .get("path")
                .and_then(|v| v.as_str())
                .ok_or("missing path")?;
            cli_args.extend(["doc".into(), "read".into(), path.into()]);
        }
        "bolo_doc_list" => cli_args.extend(["doc".into(), "ls".into()]),
        "bolo_doc_history" => {
            let path = args
                .get("path")
                .and_then(|v| v.as_str())
                .ok_or("missing path")?;
            cli_args.extend(["doc".into(), "history".into(), path.into()]);
        }
        "bolo_doc_diff" => {
            let path = args
                .get("path")
                .and_then(|v| v.as_str())
                .ok_or("missing path")?;
            cli_args.extend(["doc".into(), "diff".into(), path.into()]);
        }
        "bolo_doc_export" => {
            let path = args
                .get("path")
                .and_then(|v| v.as_str())
                .ok_or("missing path")?;
            cli_args.extend(["doc".into(), "export".into(), path.into()]);
            if let Some(output) = args.get("output").and_then(|v| v.as_str()) {
                cli_args.push(output.into());
            }
        }
        "bolo_doc_import" => {
            let path = args
                .get("path")
                .and_then(|v| v.as_str())
                .ok_or("missing path")?;
            let file = args
                .get("file")
                .and_then(|v| v.as_str())
                .ok_or("missing file")?;
            cli_args.extend(["doc".into(), "import".into(), file.into(), path.into()]);
        }
        // Pub/Sub
        "bolo_pub_send" => {
            let topic = args
                .get("topic")
                .and_then(|v| v.as_str())
                .ok_or("missing topic")?;
            let message = args
                .get("message")
                .and_then(|v| v.as_str())
                .ok_or("missing message")?;
            cli_args.extend(["pub".into(), "send".into(), topic.into(), message.into()]);
        }
        "bolo_pub_subscribe" => {
            let topic = args
                .get("topic")
                .and_then(|v| v.as_str())
                .ok_or("missing topic")?;
            cli_args.extend(["pub".into(), "sub".into(), topic.into()]);
        }
        "bolo_pub_topics" => cli_args.extend(["pub".into(), "ls".into()]),
        // Git
        "bolo_git_status" => {
            cli_args.extend(["git".into(), "status".into()]);
            if let Some(path) = args.get("path").and_then(|v| v.as_str()) {
                cli_args.extend(["--path".into(), path.into()]);
            }
        }
        "bolo_git_objects" => {
            cli_args.extend(["git".into(), "objects".into()]);
            if let Some(path) = args.get("path").and_then(|v| v.as_str()) {
                cli_args.extend(["--path".into(), path.into()]);
            }
        }
        "bolo_git_log" => {
            cli_args.extend(["git".into(), "log".into()]);
            if let Some(path) = args.get("path").and_then(|v| v.as_str()) {
                cli_args.extend(["--path".into(), path.into()]);
            }
            if let Some(count) = args.get("count").and_then(|v| v.as_u64()) {
                cli_args.extend(["--count".into(), count.to_string()]);
            }
        }
        "bolo_git_refs" => {
            cli_args.extend(["git".into(), "refs".into()]);
            if let Some(path) = args.get("path").and_then(|v| v.as_str()) {
                cli_args.extend(["--path".into(), path.into()]);
            }
        }
        // Git (push/pull/clone)
        "bolo_git_push" => {
            cli_args.extend(["git".into(), "push".into()]);
            if let Some(path) = args.get("path").and_then(|v| v.as_str()) {
                cli_args.extend(["--path".into(), path.into()]);
            }
        }
        "bolo_git_pull" => {
            cli_args.extend(["git".into(), "pull".into()]);
            if let Some(path) = args.get("path").and_then(|v| v.as_str()) {
                cli_args.extend(["--path".into(), path.into()]);
            }
        }
        "bolo_git_clone" => {
            let url = args
                .get("url")
                .and_then(|v| v.as_str())
                .ok_or("missing url")?;
            cli_args.extend(["git".into(), "clone".into(), url.into()]);
            if let Some(dest) = args.get("dest").and_then(|v| v.as_str()) {
                cli_args.push(dest.into());
            }
        }
        // Review
        "bolo_review_show" => {
            let commit = args
                .get("commit")
                .and_then(|v| v.as_str())
                .ok_or("missing commit")?;
            cli_args.extend(["review".into(), "show".into(), commit.into()]);
        }
        "bolo_review_comment" => {
            let commit = args
                .get("commit")
                .and_then(|v| v.as_str())
                .ok_or("missing commit")?;
            let message = args
                .get("message")
                .and_then(|v| v.as_str())
                .ok_or("missing message")?;
            cli_args.extend([
                "review".into(),
                "comment".into(),
                commit.into(),
                "--message".into(),
                message.into(),
            ]);
            if let Some(file) = args.get("file").and_then(|v| v.as_str()) {
                cli_args.extend(["--file".into(), file.into()]);
            }
            if let Some(line) = args.get("line").and_then(|v| v.as_u64()) {
                cli_args.extend(["--line".into(), line.to_string()]);
            }
        }
        "bolo_review_approve" => {
            let commit = args
                .get("commit")
                .and_then(|v| v.as_str())
                .ok_or("missing commit")?;
            cli_args.extend(["review".into(), "approve".into(), commit.into()]);
        }
        "bolo_review_reject" => {
            let commit = args
                .get("commit")
                .and_then(|v| v.as_str())
                .ok_or("missing commit")?;
            cli_args.extend(["review".into(), "reject".into(), commit.into()]);
            if let Some(message) = args.get("message").and_then(|v| v.as_str()) {
                cli_args.extend(["--message".into(), message.into()]);
            }
        }
        "bolo_review_ls" => cli_args.extend(["review".into(), "ls".into()]),
        // Daemon export/import
        "bolo_daemon_export" => {
            let path = args
                .get("path")
                .and_then(|v| v.as_str())
                .ok_or("missing path")?;
            cli_args.extend(["daemon".into(), "export".into(), path.into()]);
        }
        "bolo_daemon_import" => {
            let path = args
                .get("path")
                .and_then(|v| v.as_str())
                .ok_or("missing path")?;
            cli_args.extend(["daemon".into(), "import".into(), path.into()]);
            if args.get("force").and_then(|v| v.as_bool()).unwrap_or(false) {
                cli_args.push("--force".into());
            }
        }
        "bolo_daemon_upgrade" => {
            cli_args.extend(["daemon".into(), "upgrade".into()]);
            if let Some(platform) = args.get("platform").and_then(|v| v.as_str()) {
                cli_args.extend(["--platform".into(), platform.into()]);
            }
        }
        // Doc (additional commands)
        "bolo_doc_set" => {
            let path = args
                .get("path")
                .and_then(|v| v.as_str())
                .ok_or("missing path")?;
            let key = args
                .get("key")
                .and_then(|v| v.as_str())
                .ok_or("missing key")?;
            let value = args
                .get("value")
                .and_then(|v| v.as_str())
                .ok_or("missing value")?;
            cli_args.extend([
                "doc".into(),
                "set".into(),
                path.into(),
                key.into(),
                value.into(),
            ]);
        }
        "bolo_doc_del" => {
            let path = args
                .get("path")
                .and_then(|v| v.as_str())
                .ok_or("missing path")?;
            let key = args
                .get("key")
                .and_then(|v| v.as_str())
                .ok_or("missing key")?;
            cli_args.extend(["doc".into(), "del".into(), path.into(), key.into()]);
        }
        "bolo_doc_rm" => {
            let path = args
                .get("path")
                .and_then(|v| v.as_str())
                .ok_or("missing path")?;
            cli_args.extend(["doc".into(), "rm".into(), path.into()]);
        }
        "bolo_doc_append" => {
            let path = args
                .get("path")
                .and_then(|v| v.as_str())
                .ok_or("missing path")?;
            let value = args
                .get("value")
                .and_then(|v| v.as_str())
                .ok_or("missing value")?;
            cli_args.extend(["doc".into(), "append".into(), path.into(), value.into()]);
        }
        "bolo_doc_watch" => {
            let path = args
                .get("path")
                .and_then(|v| v.as_str())
                .ok_or("missing path")?;
            cli_args.extend(["doc".into(), "watch".into(), path.into()]);
        }
        "bolo_doc_sync" => {
            cli_args.extend(["doc".into(), "sync".into()]);
            if let Some(path) = args.get("path").and_then(|v| v.as_str()) {
                cli_args.push(path.into());
            }
        }
        "bolo_doc_share" => {
            let path = args
                .get("path")
                .and_then(|v| v.as_str())
                .ok_or("missing path")?;
            cli_args.extend(["doc".into(), "share".into(), path.into()]);
        }
        "bolo_doc_join" => {
            let ticket = args
                .get("ticket")
                .and_then(|v| v.as_str())
                .ok_or("missing ticket")?;
            cli_args.extend(["doc".into(), "join".into(), ticket.into()]);
        }
        "bolo_doc_compact" => {
            let path = args
                .get("path")
                .and_then(|v| v.as_str())
                .ok_or("missing path")?;
            cli_args.extend(["doc".into(), "compact".into(), path.into()]);
            if args.get("all").and_then(|v| v.as_bool()).unwrap_or(false) {
                cli_args.push("--all".into());
            }
        }
        // CI
        "bolo_ci_run" => {
            cli_args.extend(["ci".into(), "run".into()]);
            if let Some(task_type) = args.get("task_type").and_then(|v| v.as_str()) {
                cli_args.extend(["--task-type".into(), task_type.into()]);
            }
            if let Some(path) = args.get("path").and_then(|v| v.as_str()) {
                cli_args.extend(["--path".into(), path.into()]);
            }
        }
        "bolo_ci_status" => cli_args.extend(["ci".into(), "status".into()]),
        "bolo_ci_results" => {
            let task_id = args
                .get("task_id")
                .and_then(|v| v.as_str())
                .ok_or("missing task_id")?;
            cli_args.extend(["ci".into(), "results".into(), task_id.into()]);
        }
        // Task
        "bolo_task_create" => {
            let title = args
                .get("title")
                .and_then(|v| v.as_str())
                .ok_or("missing title")?;
            cli_args.extend(["task".into(), "create".into(), title.into()]);
            if let Some(priority) = args.get("priority").and_then(|v| v.as_str()) {
                cli_args.extend(["--priority".into(), priority.into()]);
            }
            if let Some(assignee) = args.get("assignee").and_then(|v| v.as_str()) {
                cli_args.extend(["--assignee".into(), assignee.into()]);
            }
        }
        "bolo_task_assign" => {
            let id = args
                .get("id")
                .and_then(|v| v.as_str())
                .ok_or("missing id")?;
            let peer = args
                .get("peer")
                .and_then(|v| v.as_str())
                .ok_or("missing peer")?;
            cli_args.extend(["task".into(), "assign".into(), id.into(), peer.into()]);
        }
        "bolo_task_list" => {
            cli_args.extend(["task".into(), "list".into()]);
            if let Some(status) = args.get("status").and_then(|v| v.as_str()) {
                cli_args.extend(["--status".into(), status.into()]);
            }
        }
        "bolo_task_show" => {
            let id = args
                .get("id")
                .and_then(|v| v.as_str())
                .ok_or("missing id")?;
            cli_args.extend(["task".into(), "show".into(), id.into()]);
        }
        "bolo_task_update" => {
            let id = args
                .get("id")
                .and_then(|v| v.as_str())
                .ok_or("missing id")?;
            cli_args.extend(["task".into(), "update".into(), id.into()]);
            if let Some(status) = args.get("status").and_then(|v| v.as_str()) {
                cli_args.extend(["--status".into(), status.into()]);
            }
            if let Some(priority) = args.get("priority").and_then(|v| v.as_str()) {
                cli_args.extend(["--priority".into(), priority.into()]);
            }
        }
        "bolo_task_link" => {
            let id = args
                .get("id")
                .and_then(|v| v.as_str())
                .ok_or("missing id")?;
            cli_args.extend(["task".into(), "link".into(), id.into()]);
            if let Some(spec) = args.get("spec").and_then(|v| v.as_str()) {
                cli_args.extend(["--spec".into(), spec.into()]);
            }
            if let Some(commit) = args.get("commit").and_then(|v| v.as_str()) {
                cli_args.extend(["--commit".into(), commit.into()]);
            }
            if let Some(ci_result) = args.get("ci_result").and_then(|v| v.as_str()) {
                cli_args.extend(["--ci-result".into(), ci_result.into()]);
            }
        }
        "bolo_task_delete" => {
            let id = args
                .get("id")
                .and_then(|v| v.as_str())
                .ok_or("missing id")?;
            cli_args.extend(["task".into(), "delete".into(), id.into()]);
        }
        // Chat
        "bolo_chat_join" => {
            let channel = args
                .get("channel")
                .and_then(|v| v.as_str())
                .ok_or("missing channel")?;
            cli_args.extend(["chat".into(), "join".into(), channel.into()]);
        }
        "bolo_chat_leave" => {
            let channel = args
                .get("channel")
                .and_then(|v| v.as_str())
                .ok_or("missing channel")?;
            cli_args.extend(["chat".into(), "leave".into(), channel.into()]);
        }
        "bolo_chat_ls" => {
            cli_args.extend(["chat".into(), "ls".into()]);
        }
        "bolo_chat_send" => {
            let channel = args
                .get("channel")
                .and_then(|v| v.as_str())
                .ok_or("missing channel")?;
            let message = args
                .get("message")
                .and_then(|v| v.as_str())
                .ok_or("missing message")?;
            cli_args.extend(["chat".into(), "send".into(), channel.into(), message.into()]);
            if let Some(reply) = args.get("reply").and_then(|v| v.as_str()) {
                cli_args.extend(["--reply".into(), reply.into()]);
            }
            if let Some(blob) = args.get("blob").and_then(|v| v.as_str()) {
                cli_args.extend(["--blob".into(), blob.into()]);
            }
            if let Some(peer) = args.get("peer").and_then(|v| v.as_str()) {
                cli_args.extend(["--peer".into(), peer.into()]);
            }
        }
        "bolo_chat_history" => {
            let channel = args
                .get("channel")
                .and_then(|v| v.as_str())
                .ok_or("missing channel")?;
            cli_args.extend(["chat".into(), "history".into(), channel.into()]);
            if let Some(limit) = args.get("limit").and_then(|v| v.as_u64()) {
                cli_args.extend(["-n".into(), limit.to_string()]);
            }
        }
        "bolo_chat_watch" => {
            let channel = args
                .get("channel")
                .and_then(|v| v.as_str())
                .ok_or("missing channel")?;
            cli_args.extend(["chat".into(), "watch".into(), channel.into()]);
            if let Some(peer) = args.get("peer").and_then(|v| v.as_str()) {
                cli_args.extend(["--peer".into(), peer.into()]);
            }
        }
        "bolo_chat_sync" => {
            cli_args.push("chat".into());
            cli_args.push("sync".into());
            if let Some(channel) = args.get("channel").and_then(|v| v.as_str()) {
                cli_args.push(channel.into());
            }
        }
        // Deploy
        "bolo_deploy_build" => {
            cli_args.extend(["deploy".into(), "build".into()]);
            if let Some(target) = args.get("target").and_then(|v| v.as_str()) {
                cli_args.extend(["--target".into(), target.into()]);
            }
        }
        "bolo_deploy_push" => {
            cli_args.extend(["deploy".into(), "push".into()]);
            if let Some(peer) = args.get("peer").and_then(|v| v.as_str()) {
                cli_args.push(peer.into());
            }
        }
        "bolo_deploy_status" => cli_args.extend(["deploy".into(), "status".into()]),
        // Daemon logs
        "bolo_daemon_logs" => {
            cli_args.extend(["daemon".into(), "logs".into()]);
            if let Some(lines) = args.get("lines").and_then(|v| v.as_u64()) {
                cli_args.extend(["-n".into(), lines.to_string()]);
            }
        }
        // Peer logs
        "bolo_peer_logs" => {
            cli_args.extend(["peer".into(), "logs".into()]);
            if let Some(node_id) = args.get("node_id").and_then(|v| v.as_str()) {
                cli_args.push(node_id.into());
            }
            if let Some(lines) = args.get("lines").and_then(|v| v.as_u64()) {
                cli_args.extend(["-n".into(), lines.to_string()]);
            }
        }
        // Mesh
        "bolo_mesh_status" => {
            cli_args.extend(["mesh".into(), "status".into()]);
            if let Some(timeout) = args.get("timeout").and_then(|v| v.as_u64()) {
                cli_args.extend(["--timeout".into(), timeout.to_string()]);
            }
        }
        // Quality
        "bolo_quality" => {
            cli_args.push("quality".into());
            if args.get("fast").and_then(|v| v.as_bool()).unwrap_or(false) {
                cli_args.push("--fast".into());
            }
            if args.get("full").and_then(|v| v.as_bool()).unwrap_or(false) {
                cli_args.push("--full".into());
            }
            if args.get("fix").and_then(|v| v.as_bool()).unwrap_or(false) {
                cli_args.push("--fix".into());
            }
        }
        _ => return Err(format!("unknown tool: {name}")),
    }

    Ok(cli_args)
}

/// Shell out to `bolo` CLI with the given arguments.
///
/// Uses the current executable path so the MCP server can find itself
/// regardless of whether `bolo` is in `$PATH`.
async fn run_bolo_command(args: &[String]) -> Result<String, String> {
    let bolo_bin =
        std::env::current_exe().map_err(|e| format!("failed to resolve bolo binary path: {e}"))?;
    let output = tokio::process::Command::new(&bolo_bin)
        .args(args)
        .output()
        .await
        .map_err(|e| format!("failed to run bolo: {e}"))?;

    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();

    if output.status.success() {
        Ok(stdout)
    } else {
        Err(format!("{stdout}{stderr}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tool_definitions_not_empty() {
        let tools = tool_definitions();
        assert!(!tools.is_empty());
    }

    #[test]
    fn build_daemon_init_args() {
        let args = build_cli_args("bolo_daemon_init", &json!({})).unwrap();
        assert!(args.contains(&"daemon".to_string()));
        assert!(args.contains(&"init".to_string()));
        assert!(args.contains(&"--json".to_string()));
    }

    #[test]
    fn build_doc_read_args() {
        let args = build_cli_args("bolo_doc_read", &json!({"path": "specs/vision"})).unwrap();
        assert!(args.contains(&"doc".to_string()));
        assert!(args.contains(&"read".to_string()));
        assert!(args.contains(&"specs/vision".to_string()));
    }

    #[test]
    fn build_args_with_config() {
        let args = build_cli_args("bolo_daemon_status", &json!({"config": "/tmp/bolo"})).unwrap();
        assert!(args.contains(&"--config".to_string()));
        assert!(args.contains(&"/tmp/bolo".to_string()));
    }

    #[test]
    fn build_unknown_tool_fails() {
        let result = build_cli_args("unknown_tool", &json!({}));
        assert!(result.is_err());
    }
}
