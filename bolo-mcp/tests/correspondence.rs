use bolo_mcp::tools::{build_cli_args, tool_definitions, tool_definitions_filtered};
use serde_json::json;

#[test]
fn all_tools_have_valid_cli_mapping() {
    let tools = tool_definitions();
    for tool in &tools {
        // Every tool should produce valid CLI args with empty/minimal arguments.
        // Some will fail on "missing X" which is expected for required params,
        // but the tool name should be recognized (not "unknown tool").
        let result = build_cli_args(&tool.name, &json!({}));
        if let Err(e) = &result {
            assert!(
                !e.contains("unknown tool"),
                "Tool {} is not recognized by build_cli_args: {e}",
                tool.name
            );
        }
    }
}

#[test]
fn tool_count_covers_all_commands() {
    let tools = tool_definitions();
    // Verify we have tools for all major command groups
    let has = |prefix: &str| tools.iter().any(|t| t.name.starts_with(prefix));
    assert!(has("bolo_daemon"), "missing daemon tools");
    assert!(has("bolo_id"), "missing id tools");
    assert!(has("bolo_peer"), "missing peer tools");
    assert!(has("bolo_blob"), "missing blob tools");
    assert!(has("bolo_doc"), "missing doc tools");
    assert!(has("bolo_pub"), "missing pub tools");
    assert!(has("bolo_git"), "missing git tools");
    assert!(has("bolo_review"), "missing review tools");
    assert!(has("bolo_ci"), "missing ci tools");
    assert!(has("bolo_quality"), "missing quality tools");
}

#[test]
fn namespace_filtering_works() {
    let doc_tools = tool_definitions_filtered(&["doc"]);
    assert!(doc_tools.iter().all(|t| t.name.starts_with("bolo_doc")));
    assert!(!doc_tools.is_empty());

    let multi = tool_definitions_filtered(&["daemon", "git"]);
    assert!(multi
        .iter()
        .all(|t| t.name.starts_with("bolo_daemon") || t.name.starts_with("bolo_git")));
    assert!(!multi.is_empty());

    let all = tool_definitions_filtered(&[]);
    assert_eq!(all.len(), tool_definitions().len());
}

#[test]
fn quality_namespace_filter() {
    // bolo_quality has no underscore after the namespace, test the special case
    let quality_tools = tool_definitions_filtered(&["quality"]);
    assert_eq!(quality_tools.len(), 1);
    assert_eq!(quality_tools[0].name, "bolo_quality");
}
