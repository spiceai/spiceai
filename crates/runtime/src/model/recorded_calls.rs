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

//! A model call as task history records it.
//!
//! A tool says how its arguments are recorded
//! ([`SpiceModelTool::recorded_arguments`]): the `sql` tool leaves out the
//! credentials of a definition. A model call carries those arguments too, in
//! the tool calls a model makes and in the request that sends them back with
//! each tool's result, so it is recorded with each tool call's arguments as
//! its tool records them, and each result without the values they took out.

use std::borrow::Cow;
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

use datafusion_ddl::component_options::redact_values;
use serde::Serialize;
use serde_json::{Map, Value};
use tools::SpiceModelTool;

use crate::model::tool_use::encode_tool_name;

/// The tools of the conversation a model call belongs to.
pub(crate) type ConversationTools = Arc<Vec<Arc<dyn SpiceModelTool>>>;

tokio::task_local! {
    static TOOLS: ConversationTools;
}

/// Run `call`, a model call, with `tools` as the tools its conversation can
/// call, so the call is recorded as they record their arguments.
pub(crate) async fn with_tools<T>(
    tools: &[Arc<dyn SpiceModelTool>],
    call: impl Future<Output = T>,
) -> T {
    TOOLS.scope(Arc::new(tools.to_vec()), call).await
}

/// The tools of the conversation of the running model call, if it has any.
pub(crate) fn current_tools() -> Option<ConversationTools> {
    TOOLS.try_with(Arc::clone).ok()
}

/// `value`, a model request or response, as task history records it.
pub(crate) fn recorded(
    value: &impl Serialize,
    tools: Option<&ConversationTools>,
) -> Result<String, serde_json::Error> {
    let text = serde_json::to_string(value)?;
    let Some(tools) = tools.filter(|tools| !tools.is_empty()) else {
        return Ok(text);
    };
    let Ok(mut json) = serde_json::from_str::<Value>(&text) else {
        return Ok(text);
    };
    if record_tool_calls(&mut json, tools) {
        Ok(json.to_string())
    } else {
        Ok(text)
    }
}

/// Rewrite in `value` the arguments of each call to one of `tools` as that
/// tool records them, and each result of such a call without the values the
/// recording took out. Returns whether anything was rewritten.
fn record_tool_calls(value: &mut Value, tools: &[Arc<dyn SpiceModelTool>]) -> bool {
    let mut removed: HashMap<String, Vec<String>> = HashMap::new();
    let rewritten = rewrite_calls(value, tools, &mut removed);
    if removed.is_empty() {
        rewritten
    } else {
        redact_results(value, &removed) | rewritten
    }
}

fn rewrite_calls(
    value: &mut Value,
    tools: &[Arc<dyn SpiceModelTool>],
    removed: &mut HashMap<String, Vec<String>>,
) -> bool {
    match value {
        Value::Array(items) => items.iter_mut().fold(false, |rewritten, item| {
            rewrite_calls(item, tools, removed) | rewritten
        }),
        Value::Object(map) => {
            let rewritten = rewrite_call(map, tools, removed);
            map.values_mut().fold(rewritten, |rewritten, item| {
                rewrite_calls(item, tools, removed) | rewritten
            })
        }
        _ => false,
    }
}

/// Rewrite `map` when it is a tool call, in the shape of any API Spice serves:
/// Chat Completions' `{"id", "function": {"name", "arguments"}}`, Responses'
/// `{"type": "function_call", "call_id", "name", "arguments"}`, and Messages'
/// `{"type": "tool_use" | "mcp_tool_use", "id", "name", "input"}`.
fn rewrite_call(
    map: &mut Map<String, Value>,
    tools: &[Arc<dyn SpiceModelTool>],
    removed: &mut HashMap<String, Vec<String>>,
) -> bool {
    // A Responses item has an `id` of its own beside the `call_id` its output
    // names.
    let id = ["call_id", "id"]
        .iter()
        .find_map(|key| map.get(*key).and_then(Value::as_str))
        .map(str::to_string);
    if let Some(Value::Object(function)) = map.get_mut("function") {
        return record_arguments(function, "arguments", id, tools, removed);
    }
    let key = match map.get("type").and_then(Value::as_str) {
        Some("function_call") => "arguments",
        Some("tool_use" | "mcp_tool_use") => "input",
        _ => return false,
    };
    record_arguments(map, key, id, tools, removed)
}

/// Record the arguments at `key` of `call`, which names its tool, as that
/// tool records them, and remember under `id` the values taken out.
fn record_arguments(
    call: &mut Map<String, Value>,
    key: &str,
    id: Option<String>,
    tools: &[Arc<dyn SpiceModelTool>],
    removed: &mut HashMap<String, Vec<String>>,
) -> bool {
    let Some(name) = call.get("name").and_then(Value::as_str) else {
        return false;
    };
    let Some(tool) = tools.iter().find(|tool| {
        let tool_name = tool.name();
        tool_name == name || encode_tool_name(&tool_name) == name
    }) else {
        return false;
    };
    let (arguments, is_text) = match call.get(key) {
        Some(Value::String(text)) => (text.clone(), true),
        Some(other) => (other.to_string(), false),
        None => return false,
    };
    let (recorded, values) = tool.recorded_arguments(&arguments);
    if recorded == arguments.as_str() {
        return false;
    }
    let recorded = recorded.into_owned();
    let recorded = if is_text {
        Value::String(recorded)
    } else {
        serde_json::from_str(&recorded).unwrap_or(Value::String(recorded))
    };
    call.insert(key.to_string(), recorded);
    if let Some(id) = id {
        removed.entry(id).or_default().extend(values);
    }
    true
}

/// Take the values `removed` holds for each tool call out of its result:
/// Chat Completions' `{"role": "tool", "tool_call_id", "content"}`, Responses'
/// `{"type": "function_call_output", "call_id", "output"}`, and Messages'
/// `{"type": "tool_result" | "mcp_tool_result", "tool_use_id", "content"}`.
fn redact_results(value: &mut Value, removed: &HashMap<String, Vec<String>>) -> bool {
    match value {
        Value::Array(items) => items.iter_mut().fold(false, |rewritten, item| {
            redact_results(item, removed) | rewritten
        }),
        Value::Object(map) => {
            let values = ["tool_call_id", "call_id", "tool_use_id"]
                .iter()
                .find_map(|key| map.get(*key).and_then(Value::as_str))
                .and_then(|id| removed.get(id))
                .cloned();
            let mut rewritten = false;
            if let Some(values) = values {
                for key in ["content", "output"] {
                    if let Some(result) = map.get_mut(key) {
                        rewritten |= redact_strings(result, &values);
                    }
                }
            }
            map.values_mut().fold(rewritten, |rewritten, item| {
                redact_results(item, removed) | rewritten
            })
        }
        _ => false,
    }
}

fn redact_strings(value: &mut Value, values: &[String]) -> bool {
    match value {
        Value::String(text) => match redact_values(text, values) {
            Cow::Owned(redacted) => {
                *text = redacted;
                true
            }
            Cow::Borrowed(_) => false,
        },
        Value::Array(items) => items.iter_mut().fold(false, |rewritten, item| {
            redact_strings(item, values) | rewritten
        }),
        Value::Object(map) => map.values_mut().fold(false, |rewritten, item| {
            redact_strings(item, values) | rewritten
        }),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use serde_json::json;

    /// A tool that records its arguments without the word `hunter2`.
    struct RedactingTool;

    #[async_trait]
    impl SpiceModelTool for RedactingTool {
        fn name(&self) -> Cow<'_, str> {
            Cow::Borrowed("sql")
        }

        fn description(&self) -> Option<Cow<'_, str>> {
            None
        }

        fn parameters(&self) -> Option<Value> {
            None
        }

        async fn call(
            &self,
            _arg: &str,
        ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
            Ok(Value::Null)
        }

        fn recorded_arguments<'a>(&self, arg: &'a str) -> (Cow<'a, str>, Vec<String>) {
            if arg.contains("hunter2") {
                (
                    Cow::Owned(arg.replace("hunter2", "<redacted>")),
                    vec!["hunter2".to_string()],
                )
            } else {
                (Cow::Borrowed(arg), Vec::new())
            }
        }
    }

    fn tools() -> ConversationTools {
        Arc::new(vec![Arc::new(RedactingTool) as Arc<dyn SpiceModelTool>])
    }

    fn recorded_json(value: &Value) -> Value {
        let text = recorded(value, Some(&tools())).expect("a JSON value serializes");
        serde_json::from_str(&text).expect("the recorded call is JSON")
    }

    /// A model call is recorded with each tool call's arguments as the tool
    /// records them, and the tool's result without the values they took out,
    /// in the shape of each API Spice serves.
    #[test]
    fn a_model_call_records_tool_calls_as_their_tools_do() {
        // Chat Completions: the request that sends a tool's result back.
        let chat = json!({"messages": [
            {"role": "user", "content": "define it"},
            {"role": "assistant", "tool_calls": [{"id": "c1", "type": "function",
                "function": {"name": "sql", "arguments": "{\"query\":\"CREATE TABLE t WITH (pg_pass = 'hunter2')\"}"}}]},
            {"role": "tool", "tool_call_id": "c1", "content": "password 'hunter2' refused"},
        ]});
        assert_eq!(
            recorded_json(&chat),
            json!({"messages": [
                {"role": "user", "content": "define it"},
                {"role": "assistant", "tool_calls": [{"id": "c1", "type": "function",
                    "function": {"name": "sql", "arguments": "{\"query\":\"CREATE TABLE t WITH (pg_pass = '<redacted>')\"}"}}]},
                {"role": "tool", "tool_call_id": "c1", "content": "password '<redacted>' refused"},
            ]})
        );

        // Responses: a function call and its output.
        let responses = json!({"input": [
            {"type": "function_call", "id": "fc_1", "call_id": "r1", "name": "sql",
                "arguments": "{\"query\":\"CREATE TABLE t WITH (pg_pass = 'hunter2')\"}"},
            {"type": "function_call_output", "call_id": "r1", "output": "password 'hunter2' refused"},
        ]});
        assert_eq!(
            recorded_json(&responses),
            json!({"input": [
                {"type": "function_call", "id": "fc_1", "call_id": "r1", "name": "sql",
                    "arguments": "{\"query\":\"CREATE TABLE t WITH (pg_pass = '<redacted>')\"}"},
                {"type": "function_call_output", "call_id": "r1", "output": "password '<redacted>' refused"},
            ]})
        );

        // Messages: a tool use, whose input is an object, and its result.
        let messages = json!({"messages": [
            {"role": "assistant", "content": [{"type": "tool_use", "id": "m1", "name": "sql",
                "input": {"query": "CREATE TABLE t WITH (pg_pass = 'hunter2')"}}]},
            {"role": "user", "content": [{"type": "tool_result", "tool_use_id": "m1",
                "content": [{"type": "text", "text": "password 'hunter2' refused"}]}]},
        ]});
        assert_eq!(
            recorded_json(&messages),
            json!({"messages": [
                {"role": "assistant", "content": [{"type": "tool_use", "id": "m1", "name": "sql",
                    "input": {"query": "CREATE TABLE t WITH (pg_pass = '<redacted>')"}}]},
                {"role": "user", "content": [{"type": "tool_result", "tool_use_id": "m1",
                    "content": [{"type": "text", "text": "password '<redacted>' refused"}]}]},
            ]})
        );
    }

    /// A call to a tool not in the conversation, and a call whose tool records
    /// its arguments as sent, are recorded as sent, text unchanged.
    #[test]
    fn other_calls_are_recorded_as_sent() {
        let request = json!({"messages": [{"role": "assistant", "tool_calls": [
            {"id": "c1", "function": {"name": "search", "arguments": "{\"text\":\"hunter2\"}"}},
            {"id": "c2", "function": {"name": "sql", "arguments": "{\"query\":\"SELECT 1\"}"}},
        ]}]});
        let text = serde_json::to_string(&request).expect("a JSON value serializes");
        assert_eq!(
            recorded(&request, Some(&tools())).expect("a JSON value serializes"),
            text
        );
        assert_eq!(
            recorded(&request, None).expect("a JSON value serializes"),
            text
        );
    }
}
