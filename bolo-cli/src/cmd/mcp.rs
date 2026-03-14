//! MCP server command handlers.

use anyhow::Result;
use bolo_mcp::McpServer;

/// `bolo mcp start` — launch the MCP server (stdio transport).
pub async fn start(
    tools_filter: Option<&str>,
    _config_flag: Option<&str>,
    _json: bool,
) -> Result<()> {
    let server = if let Some(filter) = tools_filter {
        let namespaces: Vec<String> = filter.split(',').map(|s| s.trim().to_string()).collect();
        McpServer::with_namespaces(namespaces)
    } else {
        McpServer::new()
    };
    server
        .run()
        .await
        .map_err(|e| anyhow::anyhow!("MCP server error: {e}"))?;
    Ok(())
}

/// All known tool namespaces.
const NAMESPACES: &[&str] = &[
    "daemon", "id", "peer", "blob", "doc", "pub", "git", "review", "ci", "task", "chat", "deploy",
    "mesh", "quality",
];

/// `bolo mcp status` — show MCP server info.
pub fn status(_config_flag: Option<&str>, json: bool) -> Result<()> {
    let tools = bolo_mcp::tools::tool_definitions();

    if json {
        let out = serde_json::json!({
            "server": "bolo-mcp",
            "version": env!("CARGO_PKG_VERSION"),
            "transport": "stdio",
            "tools_count": tools.len(),
            "namespaces": NAMESPACES,
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!("MCP Server: bolo-mcp");
        println!("  Version:    {}", env!("CARGO_PKG_VERSION"));
        println!("  Transport:  stdio");
        println!("  Tools:      {}", tools.len());
        println!("  Namespaces: {}", NAMESPACES.join(", "));
    }

    Ok(())
}
