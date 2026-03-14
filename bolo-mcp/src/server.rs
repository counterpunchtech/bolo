use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

use crate::protocol::*;
use crate::tools;

pub struct McpServer {
    _bolo_binary: String,
    namespaces: Vec<String>,
}

impl McpServer {
    pub fn new() -> Self {
        Self {
            _bolo_binary: "bolo".to_string(),
            namespaces: vec![],
        }
    }

    pub fn with_binary(binary: String) -> Self {
        Self {
            _bolo_binary: binary,
            namespaces: vec![],
        }
    }

    pub fn with_namespaces(namespaces: Vec<String>) -> Self {
        Self {
            _bolo_binary: "bolo".to_string(),
            namespaces,
        }
    }

    /// Run the MCP server, reading from stdin and writing to stdout.
    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        let stdin = tokio::io::stdin();
        let mut stdout = tokio::io::stdout();
        let reader = BufReader::new(stdin);
        let mut lines = reader.lines();

        while let Ok(Some(line)) = lines.next_line().await {
            let line = line.trim().to_string();
            if line.is_empty() {
                continue;
            }

            let response = self.handle_message(&line).await;
            if let Some(resp) = response {
                let json = serde_json::to_string(&resp).unwrap_or_default();
                let _ = stdout.write_all(json.as_bytes()).await;
                let _ = stdout.write_all(b"\n").await;
                let _ = stdout.flush().await;
            }
        }

        Ok(())
    }

    async fn handle_message(&self, line: &str) -> Option<JsonRpcResponse> {
        let request: JsonRpcRequest = match serde_json::from_str(line) {
            Ok(req) => req,
            Err(_) => {
                return Some(JsonRpcResponse::error(
                    None,
                    PARSE_ERROR,
                    "Parse error".to_string(),
                ));
            }
        };

        let id = request.id.clone();

        match request.method.as_str() {
            "initialize" => {
                let result = InitializeResult {
                    protocol_version: "2024-11-05".to_string(),
                    capabilities: ServerCapabilities {
                        tools: ToolsCapability {
                            list_changed: false,
                        },
                    },
                    server_info: ServerInfo {
                        name: "bolo-mcp".to_string(),
                        version: env!("CARGO_PKG_VERSION").to_string(),
                    },
                };
                Some(JsonRpcResponse::success(
                    id,
                    serde_json::to_value(result).unwrap(),
                ))
            }
            "notifications/initialized" => {
                // No response needed for notifications
                None
            }
            "tools/list" => {
                let ns_refs: Vec<&str> = self.namespaces.iter().map(|s| s.as_str()).collect();
                let tools = tools::tool_definitions_filtered(&ns_refs);
                let result = serde_json::json!({ "tools": tools });
                Some(JsonRpcResponse::success(id, result))
            }
            "tools/call" => {
                let params: ToolCallParams = match serde_json::from_value(request.params) {
                    Ok(p) => p,
                    Err(e) => {
                        return Some(JsonRpcResponse::error(
                            id,
                            INVALID_PARAMS,
                            format!("Invalid params: {e}"),
                        ));
                    }
                };

                let result = tools::execute_tool(&params.name, &params.arguments).await;
                Some(JsonRpcResponse::success(
                    id,
                    serde_json::to_value(result).unwrap(),
                ))
            }
            "ping" => Some(JsonRpcResponse::success(id, serde_json::json!({}))),
            _ => Some(JsonRpcResponse::error(
                id,
                METHOD_NOT_FOUND,
                format!("Method not found: {}", request.method),
            )),
        }
    }
}

impl Default for McpServer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn handle_initialize() {
        let server = McpServer::new();
        let msg = r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"test"}}}"#;
        let resp = server.handle_message(msg).await.unwrap();
        assert!(resp.result.is_some());
        assert!(resp.error.is_none());
    }

    #[tokio::test]
    async fn handle_tools_list() {
        let server = McpServer::new();
        let msg = r#"{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}"#;
        let resp = server.handle_message(msg).await.unwrap();
        let result = resp.result.unwrap();
        let tools = result.get("tools").unwrap().as_array().unwrap();
        assert!(!tools.is_empty());
    }

    #[tokio::test]
    async fn handle_tools_list_filtered() {
        let server = McpServer::with_namespaces(vec!["doc".to_string()]);
        let msg = r#"{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}"#;
        let resp = server.handle_message(msg).await.unwrap();
        let result = resp.result.unwrap();
        let tools = result.get("tools").unwrap().as_array().unwrap();
        assert!(!tools.is_empty());
        for t in tools {
            let name = t.get("name").unwrap().as_str().unwrap();
            assert!(name.starts_with("bolo_doc"), "unexpected tool: {name}");
        }
    }

    #[tokio::test]
    async fn handle_ping() {
        let server = McpServer::new();
        let msg = r#"{"jsonrpc":"2.0","id":3,"method":"ping","params":{}}"#;
        let resp = server.handle_message(msg).await.unwrap();
        assert!(resp.result.is_some());
    }

    #[tokio::test]
    async fn handle_unknown_method() {
        let server = McpServer::new();
        let msg = r#"{"jsonrpc":"2.0","id":4,"method":"unknown/method","params":{}}"#;
        let resp = server.handle_message(msg).await.unwrap();
        assert!(resp.error.is_some());
        assert_eq!(resp.error.unwrap().code, METHOD_NOT_FOUND);
    }

    #[tokio::test]
    async fn handle_parse_error() {
        let server = McpServer::new();
        let resp = server.handle_message("not json").await.unwrap();
        assert!(resp.error.is_some());
        assert_eq!(resp.error.unwrap().code, PARSE_ERROR);
    }

    #[tokio::test]
    async fn notification_returns_none() {
        let server = McpServer::new();
        let msg = r#"{"jsonrpc":"2.0","method":"notifications/initialized","params":{}}"#;
        let resp = server.handle_message(msg).await;
        assert!(resp.is_none());
    }
}
