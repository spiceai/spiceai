/*
Copyright 2026 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

//! Spice's tool calls in `/v1/messages` output, and their replay.
//!
//! Spice runs its own tools inside a request. The client receives each call as an
//! `mcp_tool_use` block and its result as an `mcp_tool_result` block, the blocks the Messages
//! API gives a call that the server side ran, with `server_name` [`SERVER_NAME`] and the model's
//! own `tool_use` ID. A client that keeps its own history sends them back in the assistant
//! message, and [`replay_spice_tool_calls`] gives the model what it saw: the `tool_use` blocks in
//! the assistant message, and their `tool_result` blocks in the user message after it.

use std::collections::HashSet;

use serde_json::{Map, Value, json};

use crate::model::responses_tool_history::ToolOutcome;

/// The `server_name` of Spice's `mcp_tool_use` and `mcp_tool_result` blocks.
pub(crate) const SERVER_NAME: &str = "spice";

/// The result the model is given for a replayed call that came back without its result: every
/// `tool_use` must be answered in the user message after it.
const RESULT_NOT_SENT_BACK: &str =
    "Spice ran this call, but its result was not sent back with the conversation.";

/// A `tool_use` block of the model's, a call to one of Spice's tools, as the `mcp_tool_use`
/// block the client receives.
pub(crate) fn mcp_tool_use(tool_use: &Value) -> Value {
    let mut block = tool_use.as_object().cloned().unwrap_or_default();
    block.insert("type".to_string(), Value::from("mcp_tool_use"));
    block.insert("server_name".to_string(), Value::from(SERVER_NAME));
    Value::Object(block)
}

/// The content of the result of a call, as the model receives it.
fn result_content(outcome: &ToolOutcome) -> Value {
    json!([{"type": "text", "text": outcome.model_output()}])
}

/// The `tool_result` block the model receives for the call `tool_use_id`. The block is the same
/// in the next round of the tool-use loop and in each later turn that replays the call, so that
/// the provider's prompt cache serves the later turn up to the end of the earlier one.
pub(crate) fn tool_result(tool_use_id: &str, outcome: &ToolOutcome) -> Value {
    tool_result_block(
        Value::from(tool_use_id),
        result_content(outcome),
        matches!(outcome, ToolOutcome::Failed(_)),
    )
}

fn tool_result_block(tool_use_id: Value, content: Value, is_error: bool) -> Value {
    let mut block = Map::new();
    block.insert("type".to_string(), Value::from("tool_result"));
    block.insert("tool_use_id".to_string(), tool_use_id);
    block.insert("content".to_string(), content);
    if is_error {
        block.insert("is_error".to_string(), Value::Bool(true));
    }
    Value::Object(block)
}

/// The `mcp_tool_result` block the client receives for the call `tool_use_id`.
pub(crate) fn mcp_tool_result(tool_use_id: &str, outcome: &ToolOutcome) -> Value {
    json!({
        "type": "mcp_tool_result",
        "tool_use_id": tool_use_id,
        "is_error": matches!(outcome, ToolOutcome::Failed(_)),
        "content": result_content(outcome),
    })
}

fn is_spice_tool_use(block: &Value) -> bool {
    block.get("type").and_then(Value::as_str) == Some("mcp_tool_use")
        && block.get("server_name").and_then(Value::as_str) == Some(SERVER_NAME)
}

/// Whether `block` is the `mcp_tool_result` of one of `calls`, the IDs of Spice's calls. A
/// result names no server, only the call it answers.
fn is_spice_tool_result(block: &Value, calls: &HashSet<String>) -> bool {
    block.get("type").and_then(Value::as_str) == Some("mcp_tool_result")
        && block
            .get("tool_use_id")
            .and_then(Value::as_str)
            .is_some_and(|id| calls.contains(id))
}

/// `block`, one of Spice's `mcp_tool_use` blocks, as the model made the call.
fn replayed_tool_use(block: Value) -> Value {
    let mut block = match block {
        Value::Object(block) => block,
        other => return other,
    };
    block.insert("type".to_string(), Value::from("tool_use"));
    block.remove("server_name");
    Value::Object(block)
}

/// `block`, one of Spice's `mcp_tool_result` blocks, as the model received the result.
fn replayed_tool_result(block: Value) -> Value {
    let mut block = match block {
        Value::Object(block) => block,
        other => return other,
    };
    let mut result = tool_result_block(
        block.remove("tool_use_id").unwrap_or(Value::Null),
        block.remove("content").unwrap_or_else(|| json!([])),
        block.remove("is_error").and_then(|e| e.as_bool()).unwrap_or(false),
    );
    if let (Some(cache_control), Some(result)) =
        (block.remove("cache_control"), result.as_object_mut())
    {
        result.insert("cache_control".to_string(), cache_control);
    }
    result
}

fn message(role: &str, content: Vec<Value>) -> Value {
    json!({"role": role, "content": content})
}

/// Whether the request's own MCP servers include one named [`SERVER_NAME`], whose blocks are
/// then the client's, not Spice's.
fn names_a_spice_mcp_server(body: &Map<String, Value>) -> bool {
    body.get("mcp_servers")
        .and_then(Value::as_array)
        .is_some_and(|servers| {
            servers
                .iter()
                .any(|server| server.get("name").and_then(Value::as_str) == Some(SERVER_NAME))
        })
}

/// The request with each of Spice's tool calls given back as the model made it: in each
/// assistant message, an `mcp_tool_use` block becomes the `tool_use` block it was, and the
/// `mcp_tool_result` blocks after it become `tool_result` blocks in a user message, so the
/// assistant message is split where they were. Results at the end of an assistant message go at
/// the start of the next user message, beside the results of the client's own calls. Other
/// blocks, a hosted MCP server's included, are unchanged.
pub(crate) fn replay_spice_tool_calls(body: &mut Map<String, Value>) {
    if names_a_spice_mcp_server(body) {
        return;
    }
    let Some(Value::Array(messages)) = body.get_mut("messages") else {
        return;
    };
    let carries_spice_blocks = |message: &Value| {
        message.get("role").and_then(Value::as_str) == Some("assistant")
            && message
                .get("content")
                .and_then(Value::as_array)
                .is_some_and(|content| content.iter().any(is_spice_tool_use))
    };
    if !messages.iter().any(carries_spice_blocks) {
        return;
    }

    let mut replayed = Vec::with_capacity(messages.len() + 2);
    let mut pending_results: Vec<Value> = Vec::new();
    for mut message_value in std::mem::take(messages) {
        if carries_spice_blocks(&message_value) {
            if !pending_results.is_empty() {
                replayed.push(message("user", std::mem::take(&mut pending_results)));
            }
            let content = match message_value.get_mut("content").map(Value::take) {
                Some(Value::Array(content)) => content,
                _ => Vec::new(),
            };
            let calls: HashSet<String> = content
                .iter()
                .filter(|block| is_spice_tool_use(block))
                .filter_map(|block| block.get("id").and_then(Value::as_str))
                .map(ToString::to_string)
                .collect();
            let call_order: Vec<String> = content
                .iter()
                .filter(|block| is_spice_tool_use(block))
                .filter_map(|block| block.get("id").and_then(Value::as_str))
                .map(ToString::to_string)
                .collect();
            let answered: HashSet<String> = content
                .iter()
                .filter(|block| is_spice_tool_result(block, &calls))
                .filter_map(|block| block.get("tool_use_id").and_then(Value::as_str))
                .map(ToString::to_string)
                .collect();
            let mut assistant = Vec::new();
            let mut results = Vec::new();
            for block in content {
                if is_spice_tool_result(&block, &calls) {
                    results.push(replayed_tool_result(block));
                    continue;
                }
                if !results.is_empty() {
                    replayed.push(message("assistant", std::mem::take(&mut assistant)));
                    replayed.push(message("user", std::mem::take(&mut results)));
                }
                if is_spice_tool_use(&block) {
                    assistant.push(replayed_tool_use(block));
                } else {
                    assistant.push(block);
                }
            }
            if !assistant.is_empty() {
                replayed.push(message("assistant", assistant));
            }
            for id in call_order.iter().filter(|id| !answered.contains(*id)) {
                results.push(tool_result_block(
                    Value::from(id.as_str()),
                    json!([{"type": "text", "text": RESULT_NOT_SENT_BACK}]),
                    false,
                ));
            }
            pending_results = results;
            continue;
        }
        if !pending_results.is_empty() {
            if message_value.get("role").and_then(Value::as_str) == Some("user") {
                let content = match message_value.get_mut("content").map(Value::take) {
                    Some(Value::Array(content)) => content,
                    Some(Value::String(text)) => vec![json!({"type": "text", "text": text})],
                    _ => Vec::new(),
                };
                let mut results = std::mem::take(&mut pending_results);
                results.extend(content);
                if let Some(message) = message_value.as_object_mut() {
                    message.insert("content".to_string(), Value::Array(results));
                }
            } else {
                replayed.push(message("user", std::mem::take(&mut pending_results)));
            }
        }
        replayed.push(message_value);
    }
    if !pending_results.is_empty() {
        replayed.push(message("user", pending_results));
    }
    *messages = replayed;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn replay(body: &Value) -> Value {
        let mut body = body.as_object().cloned().expect("an object");
        replay_spice_tool_calls(&mut body);
        Value::Object(body)
    }

    fn sql_use(id: &str) -> Value {
        json!({"type": "tool_use", "id": id, "name": "sql", "input": {"query": "SELECT 1"}})
    }

    #[test]
    fn a_turn_that_ran_spice_tools_replays_as_the_rounds_the_model_saw() {
        let completed = ToolOutcome::Completed(json!([{"1": 1}]));
        let failed = ToolOutcome::Failed("Failed to call the tool sql.".to_string());
        // What the client received: two rounds of the loop in one assistant message.
        let received = json!({"messages": [
            {"role": "user", "content": "How many?"},
            {"role": "assistant", "content": [
                {"type": "text", "text": "Querying."},
                mcp_tool_use(&sql_use("toolu_1")),
                mcp_tool_use(&sql_use("toolu_2")),
                mcp_tool_result("toolu_1", &completed),
                mcp_tool_result("toolu_2", &failed),
                {"type": "text", "text": "One."},
            ]},
            {"role": "user", "content": "Thanks."},
        ]});
        assert_eq!(
            replay(&received),
            json!({"messages": [
                {"role": "user", "content": "How many?"},
                {"role": "assistant", "content": [
                    {"type": "text", "text": "Querying."},
                    sql_use("toolu_1"),
                    sql_use("toolu_2"),
                ]},
                {"role": "user", "content": [
                    tool_result("toolu_1", &completed),
                    tool_result("toolu_2", &failed),
                ]},
                {"role": "assistant", "content": [{"type": "text", "text": "One."}]},
                {"role": "user", "content": "Thanks."},
            ]})
        );
        assert_eq!(
            tool_result("toolu_2", &failed),
            json!({"type": "tool_result", "tool_use_id": "toolu_2", "content": [{"type": "text", "text": "Failed to call the tool sql."}], "is_error": true})
        );
    }

    #[test]
    fn results_that_end_a_turn_join_the_clients_own_results() {
        let completed = ToolOutcome::Completed(json!("42"));
        let client_call = json!({"type": "tool_use", "id": "toolu_c", "name": "read_file", "input": {}});
        let client_result = json!({"type": "tool_result", "tool_use_id": "toolu_c", "content": "file"});
        let received = json!({"messages": [
            {"role": "user", "content": "q"},
            {"role": "assistant", "content": [
                mcp_tool_use(&sql_use("toolu_s")),
                client_call,
                mcp_tool_result("toolu_s", &completed),
            ]},
            {"role": "user", "content": [client_result]},
        ]});
        assert_eq!(
            replay(&received),
            json!({"messages": [
                {"role": "user", "content": "q"},
                {"role": "assistant", "content": [sql_use("toolu_s"), client_call]},
                {"role": "user", "content": [tool_result("toolu_s", &completed), client_result]},
            ]})
        );

        // A turn the loop paused at the end of: the results become the last message.
        let paused = json!({"messages": [
            {"role": "user", "content": "q"},
            {"role": "assistant", "content": [mcp_tool_use(&sql_use("toolu_s")), mcp_tool_result("toolu_s", &completed)]},
        ]});
        assert_eq!(
            replay(&paused),
            json!({"messages": [
                {"role": "user", "content": "q"},
                {"role": "assistant", "content": [sql_use("toolu_s")]},
                {"role": "user", "content": [tool_result("toolu_s", &completed)]},
            ]})
        );

        // A user message with text after the results keeps its text.
        let continued = json!({"messages": [
            {"role": "assistant", "content": [mcp_tool_use(&sql_use("toolu_s")), mcp_tool_result("toolu_s", &completed)]},
            {"role": "user", "content": "Go on."},
        ]});
        assert_eq!(
            replay(&continued)["messages"][1],
            json!({"role": "user", "content": [tool_result("toolu_s", &completed), {"type": "text", "text": "Go on."}]})
        );
    }

    #[test]
    fn a_call_whose_result_was_not_sent_back_is_still_answered() {
        let received = json!({"messages": [
            {"role": "user", "content": "q"},
            {"role": "assistant", "content": [
                mcp_tool_use(&sql_use("toolu_1")),
                {"type": "text", "text": "One."},
            ]},
            {"role": "user", "content": "Thanks."},
        ]});
        assert_eq!(
            replay(&received),
            json!({"messages": [
                {"role": "user", "content": "q"},
                {"role": "assistant", "content": [sql_use("toolu_1"), {"type": "text", "text": "One."}]},
                {"role": "user", "content": [
                    {"type": "tool_result", "tool_use_id": "toolu_1", "content": [{"type": "text", "text": "Spice ran this call, but its result was not sent back with the conversation."}]},
                    {"type": "text", "text": "Thanks."},
                ]},
            ]})
        );
    }

    #[test]
    fn other_mcp_servers_blocks_are_unchanged() {
        let hosted = json!({"messages": [
            {"role": "user", "content": "q"},
            {"role": "assistant", "content": [
                {"type": "mcp_tool_use", "id": "mcptoolu_1", "name": "search", "server_name": "github", "input": {}},
                {"type": "mcp_tool_result", "tool_use_id": "mcptoolu_1", "is_error": false, "content": []},
            ]},
        ]});
        assert_eq!(replay(&hosted), hosted);

        let mut named_spice = hosted.clone();
        named_spice["mcp_servers"] = json!([{"type": "url", "name": "spice", "url": "https://example.com/mcp"}]);
        named_spice["messages"][1]["content"][0]["server_name"] = json!("spice");
        named_spice["messages"][1]["content"][1]["server_name"] = json!("spice");
        assert_eq!(replay(&named_spice), named_spice);
    }
}
