/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! Spice's tool calls in `/v1/responses` output, and their replay.
//!
//! Spice runs its own tools inside a request. The client receives each call and its result as an
//! `mcp_call` item, the item type clients treat as run by the server, labelled [`SERVER_LABEL`]
//! and identified by [`ID_PREFIX`] and the model's `call_id`. A client that keeps its own history
//! sends the item back, and [`replay_spice_tool_calls`] gives the provider what the model saw:
//! the `function_call`, then its `function_call_output`, paired by `call_id`.

use async_openai::types::responses::{
    FunctionCallOutput, FunctionCallOutputItemParam, FunctionToolCall, InputItem, InputParam, Item,
    MCPToolCall, MCPToolCallStatus, OutputStatus,
};
use itertools::Either;
use serde_json::Value;

/// The `server_label` of Spice's `mcp_call` items.
pub(crate) const SERVER_LABEL: &str = "spice";

/// The start of the ID of Spice's `mcp_call` items, which the model's `call_id` follows. `OpenAI`
/// accepts an `mcp_call` input item only with an ID that begins with `mcp`. Its own are `mcp_` and
/// hex digits, so a hosted MCP server's call never starts with `mcp_spice_`.
pub(crate) const ID_PREFIX: &str = "mcp_spice_";

/// The result the model is given for a replayed call that came back without its result: the
/// `OpenAI` Agents SDK for JavaScript drops `output` from every `mcp_call` it sends back.
const RESULT_NOT_SENT_BACK: &str =
    "Spice ran this call, but its result was not sent back with the conversation.";

/// What running one of Spice's tools produced.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum ToolOutcome {
    /// The tool's result.
    Completed(Value),
    /// Why the call failed, as the model is told.
    Failed(String),
}

impl ToolOutcome {
    /// The call's result as the model receives it: a string as is, any other value as JSON, and a
    /// failure as its message.
    pub(crate) fn model_output(&self) -> String {
        match self {
            ToolOutcome::Completed(Value::String(text)) | ToolOutcome::Failed(text) => text.clone(),
            ToolOutcome::Completed(value) => value.to_string(),
        }
    }
}

/// `call`, a call to one of Spice's tools, as the `mcp_call` item the client receives. `outcome`
/// is what running it produced; until it runs, the item's status is `pending`.
///
/// `output` is the result exactly as the model received it, a failure's message included, so that
/// the item replays as the model saw it.
pub(crate) fn mcp_call(
    call: &FunctionToolCall,
    outcome: Option<&ToolOutcome>,
    pending: MCPToolCallStatus,
) -> MCPToolCall {
    let status = match outcome {
        Some(ToolOutcome::Completed(_)) => MCPToolCallStatus::Completed,
        Some(ToolOutcome::Failed(_)) => MCPToolCallStatus::Failed,
        None => pending,
    };
    MCPToolCall {
        id: format!("{ID_PREFIX}{}", call.call_id),
        server_label: SERVER_LABEL.to_string(),
        name: call.name.clone(),
        arguments: call.arguments.clone(),
        approval_request_id: None,
        error: None,
        output: outcome.map(ToolOutcome::model_output),
        status: Some(status),
    }
}

/// The model's `call_id` of `call` when it is one of Spice's `mcp_call` items.
fn spice_call_id(call: &MCPToolCall) -> Option<&str> {
    if call.server_label != SERVER_LABEL {
        return None;
    }
    call.id.strip_prefix(ID_PREFIX)
}

fn is_spice_tool_call(item: &InputItem) -> bool {
    matches!(item, InputItem::Item(Item::McpCall(call)) if spice_call_id(call).is_some())
}

/// `input` with each of Spice's `mcp_call` items replaced by what the model saw: the
/// `function_call` (see [`replayed_call`]), then its `function_call_output`. Other items, hosted
/// MCP calls included, are unchanged.
pub(crate) fn replay_spice_tool_calls(input: InputParam) -> InputParam {
    let InputParam::Items(items) = input else {
        return input;
    };
    if !items.iter().any(is_spice_tool_call) {
        return InputParam::Items(items);
    }
    InputParam::Items(
        items
            .into_iter()
            .flat_map(|item| match item {
                InputItem::Item(Item::McpCall(call)) if spice_call_id(&call).is_some() => {
                    Either::Left(replayed(call).into_iter())
                }
                item => Either::Right(std::iter::once(item)),
            })
            .collect(),
    )
}

/// A call to one of Spice's tools, as Spice sends it to the provider in each request after the
/// one that made it: the next round of the tool-use loop, and each later turn.
///
/// Both must be the same, item for item, so that the provider's prompt cache serves a later turn
/// up to the end of the earlier one. Measured on 2026-10-15 with `gpt-4o-mini`: when the round
/// kept the call's `id` and the later turn did not, the later turn read 4,864 of 12,784 tokens
/// from the cache; when neither kept it, 12,672 of 12,787.
///
/// The call has no `id`, because the `mcp_call` item that a client sends back does not keep it.
/// Also, `OpenAI` resolves a call's `id` to the call it stored, and then requires the reasoning
/// item that the call was emitted with; without an `id`, it requires neither.
pub(crate) fn replayed_call(call_id: String, name: String, arguments: String) -> FunctionToolCall {
    FunctionToolCall {
        arguments,
        call_id,
        namespace: None,
        name,
        id: None,
        status: Some(OutputStatus::Completed),
    }
}

/// `call`, one of Spice's `mcp_call` items, as the model saw it.
fn replayed(call: MCPToolCall) -> [InputItem; 2] {
    let call_id = spice_call_id(&call).unwrap_or_default().to_string();
    let output = call
        .output
        .or(call.error)
        .unwrap_or_else(|| RESULT_NOT_SENT_BACK.to_string());
    [
        InputItem::Item(Item::FunctionCall(replayed_call(
            call_id.clone(),
            call.name,
            call.arguments,
        ))),
        InputItem::Item(Item::FunctionCallOutput(FunctionCallOutputItemParam {
            call_id,
            output: FunctionCallOutput::Text(output),
            id: None,
            status: None,
        })),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_openai::types::responses::OutputItem;
    use serde_json::json;

    fn input(items: &Value) -> InputParam {
        serde_json::from_value(items.clone()).expect("a valid input")
    }

    fn replay(items: &Value) -> Value {
        serde_json::to_value(replay_spice_tool_calls(input(items))).expect("a serializable input")
    }

    fn call(arguments: &str) -> FunctionToolCall {
        serde_json::from_value(json!({
            "type": "function_call", "id": "fc_1", "call_id": "call_1", "name": "sql",
            "arguments": arguments, "status": "completed",
        }))
        .expect("a valid function call")
    }

    /// What the model saw of `call` and `outcome` in the round after it made the call, less the
    /// call's `id`.
    fn as_the_model_saw_it(call: &FunctionToolCall, outcome: &ToolOutcome) -> Value {
        json!([
            {
                "type": "function_call", "call_id": call.call_id, "name": call.name,
                "arguments": call.arguments, "status": "completed",
            },
            {
                "type": "function_call_output", "call_id": call.call_id,
                "output": outcome.model_output(),
            },
        ])
    }

    #[test]
    fn test_spice_tool_call_replays_as_the_model_saw_it() {
        let arguments = r#"{"query":"SELECT region, 'it''s \"quoted\" ✓' FROM orders"}"#;
        for outcome in [
            ToolOutcome::Completed(Value::String(r#"[{"region":"north"}]"#.to_string())),
            ToolOutcome::Completed(json!([{ "region": "north", "revenue": 124.5 }])),
            ToolOutcome::Completed(Value::Null),
            ToolOutcome::Failed(
                "Failed to call the tool sql. An error occurred: \"x\"".to_string(),
            ),
        ] {
            let call = call(arguments);
            let item = OutputItem::McpCall(mcp_call(
                &call,
                Some(&outcome),
                MCPToolCallStatus::InProgress,
            ));
            assert_eq!(
                replay(&json!([item])),
                as_the_model_saw_it(&call, &outcome),
                "{outcome:?}"
            );
        }
    }

    // What the model receives is what a replayed item gives back, so it is pinned here.
    #[test]
    fn test_model_output() {
        assert_eq!(
            ToolOutcome::Completed(Value::String(r#"[{"region":"north"}]"#.to_string()))
                .model_output(),
            r#"[{"region":"north"}]"#
        );
        assert_eq!(
            ToolOutcome::Completed(json!([{ "region": "north", "revenue": 124.5 }])).model_output(),
            r#"[{"region":"north","revenue":124.5}]"#
        );
        assert_eq!(ToolOutcome::Completed(Value::Null).model_output(), "null");
        assert_eq!(
            ToolOutcome::Failed("Failed to call the tool sql. An error occurred: x".to_string())
                .model_output(),
            "Failed to call the tool sql. An error occurred: x"
        );
    }

    #[test]
    fn test_mcp_call_item() {
        let call = call(r#"{"query":"SELECT 1"}"#);
        assert_eq!(
            serde_json::to_value(mcp_call(
                &call,
                Some(&ToolOutcome::Completed(json!([{ "one": 1 }]))),
                MCPToolCallStatus::InProgress,
            ))
            .expect("a serializable item"),
            json!({
                "id": "mcp_spice_call_1", "server_label": "spice", "name": "sql",
                "arguments": "{\"query\":\"SELECT 1\"}", "approval_request_id": null,
                "error": null, "output": "[{\"one\":1}]", "status": "completed",
            })
        );
        assert_eq!(
            serde_json::to_value(mcp_call(
                &call,
                Some(&ToolOutcome::Failed("no such table".to_string())),
                MCPToolCallStatus::InProgress,
            ))
            .expect("a serializable item"),
            json!({
                "id": "mcp_spice_call_1", "server_label": "spice", "name": "sql",
                "arguments": "{\"query\":\"SELECT 1\"}", "approval_request_id": null,
                "error": null, "output": "no such table", "status": "failed",
            })
        );
        assert_eq!(
            serde_json::to_value(mcp_call(&call, None, MCPToolCallStatus::InProgress))
                .expect("a serializable item")["status"],
            "in_progress"
        );
    }

    #[test]
    fn test_replay_keeps_the_conversation_in_order() {
        let mcp = |n: u32| {
            json!({
                "type": "mcp_call", "id": format!("mcp_spice_call_{n}"), "server_label": "spice",
                "name": "sql", "arguments": "{}", "output": format!("\"{n}\""),
                "status": "completed",
            })
        };
        let reasoning = json!({ "type": "reasoning", "id": "rs_1", "summary": [] });
        let message = json!({
            "type": "message", "id": "msg_1", "role": "assistant", "status": "completed",
            "content": [{ "type": "output_text", "text": "Done.", "annotations": [], "logprobs": null }],
        });
        let user = json!({ "type": "message", "role": "user", "content": "Revenue?" });
        let function_call = |n: u32| {
            json!({
                "type": "function_call", "call_id": format!("call_{n}"), "name": "sql",
                "arguments": "{}", "status": "completed",
            })
        };
        let function_call_output = |n: u32| {
            json!({
                "type": "function_call_output", "call_id": format!("call_{n}"),
                "output": format!("\"{n}\""),
            })
        };

        assert_eq!(
            replay(&json!([user, reasoning, mcp(1), mcp(2), message, user])),
            json!([
                user,
                reasoning,
                function_call(1),
                function_call_output(1),
                function_call(2),
                function_call_output(2),
                message,
                user
            ])
        );
    }

    #[test]
    fn test_hosted_mcp_calls_reach_the_provider_unchanged() {
        // As OpenAI returns them: an `mcp_` and hex ID, under the label of the request's MCP tool,
        // which may be `spice` too.
        for label in ["deepwiki", "spice"] {
            let items = json!([
                { "type": "message", "role": "user", "content": "Topics?" },
                {
                    "type": "mcp_call", "id": "mcp_01f5b95a7907c7b3006aceddc4c38087d0bc30de4571ed346f",
                    "server_label": label, "name": "read_wiki_structure",
                    "arguments": "{\"repoName\":\"spiceai/spiceai\"}", "output": "1 Overview",
                    "error": null, "approval_request_id": null, "status": "completed",
                },
            ]);
            assert_eq!(replay(&items), items, "{label}");
        }
    }

    #[test]
    fn test_a_spice_tool_call_sent_back_without_its_result() {
        // As the OpenAI Agents SDK for JavaScript 0.20.0 sends one back: without `output`.
        let item = json!({
            "type": "mcp_call", "id": "mcp_spice_call_1", "server_label": "spice", "name": "sql",
            "arguments": "{\"query\": \"SELECT COUNT(*) FROM orders WHERE region = 'north'\"}",
            "error": null, "approval_request_id": null, "status": "completed",
        });
        assert_eq!(
            replay(&json!([item])),
            json!([
                {
                    "type": "function_call", "call_id": "call_1", "name": "sql",
                    "arguments": "{\"query\": \"SELECT COUNT(*) FROM orders WHERE region = 'north'\"}",
                    "status": "completed",
                },
                {
                    "type": "function_call_output", "call_id": "call_1",
                    "output": "Spice ran this call, but its result was not sent back with the conversation.",
                },
            ])
        );
    }

    #[test]
    fn test_input_without_spice_tool_calls_is_unchanged() {
        let text = InputParam::Text("What datasets do you have access to?".to_string());
        assert_eq!(replay_spice_tool_calls(text.clone()), text);

        let items = json!([
            { "type": "message", "role": "user", "content": "Revenue?" },
            {
                "type": "function_call", "id": "fc_1", "call_id": "call_1", "name": "client_tool",
                "arguments": "{}", "status": "completed",
            },
            { "type": "function_call_output", "call_id": "call_1", "output": "1" },
        ]);
        assert_eq!(replay(&items), items);
    }
}
