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
//! credentials of a definition. A conversation carries those arguments too, in
//! the tool calls a model makes, in the requests that send them back with each
//! tool's result, and in what a client sends back on its next turn. So a model
//! call, and any payload of a conversation task history records, holds each
//! tool call's arguments as its tool records them, and none of the values that
//! recording took out, wherever else in the payload they appear.
//!
//! The model calls Spice makes in its tool loops know their tools
//! ([`with_tools`]). Task history also reads every payload it stores
//! ([`recorded_by_name`]), for a conversation whose tools Spice did not run,
//! such as one a client sends back: a call to an `sql` tool by its name, and a
//! call to any other tool by what its arguments hold.
//!
//! What a payload repeats of a value is taken out only when the payload holds
//! the call too: a client's next turn that holds a model's earlier answer, but
//! not the call Spice ran before it, is recorded as written.

use std::borrow::Cow;
use std::future::Future;
use std::sync::Arc;

use datafusion_ddl::component_options::{
    UNREADABLE_STATEMENT, may_define, recorded_call_arguments, redact_values,
};
use runtime_tools::builtin::sql::{is_sql_tool, recorded_input, sql_tool_names};
use serde::Serialize;
use serde_json::{Map, Value};
use tools::SpiceModelTool;
use tools::naming::decode_tool_name;

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

/// How a call to the named tool records its arguments: the recorded text and
/// the values taken out, or `None` when they are recorded as sent.
type Recorder<'r> = dyn Fn(&str, &str) -> Option<(String, Vec<String>)> + 'r;

/// `value`, a model request or response, as task history records it, each
/// call to one of `tools` recorded as that tool records it, and a call to any
/// other tool by what its arguments hold.
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
    let record = |name: &str, arguments: &str| {
        tools
            .iter()
            .find(|tool| {
                let tool_name = tool.name();
                tool_name == name || encode_tool_name(&tool_name) == name
            })
            .and_then(|tool| changed(arguments, tool.recorded_arguments(arguments)))
            .or_else(|| recorded_call_arguments(arguments))
    };
    if record_tool_calls(&mut json, &record).is_some() {
        Ok(json.to_string())
    } else {
        Ok(text)
    }
}

/// `text`, a payload of a conversation task history stores, with each call to
/// an `sql` tool, directly or through `tool_invoke`, recorded as that tool
/// records it, whatever tools the conversation's model has, and a call to any
/// other tool by what its arguments hold ([`recorded_call_arguments`]). An
/// `sql` tool of an MCP server, such as another Spice's (`peer__sql`), counts
/// as one. Returns the recorded text and the values taken out, or `None` when
/// nothing changes, as for text that is not JSON.
pub(crate) fn recorded_by_name(text: &str) -> Option<(String, Vec<String>)> {
    let sql_tools = sql_tool_names();
    if !may_name(text, &sql_tools) && !may_define(text) {
        return None;
    }
    let mut json = serde_json::from_str::<Value>(text).ok()?;
    let is_sql = |name: &str| {
        is_sql_tool(name)
            || sql_tools.iter().any(|tool| encode_tool_name(tool) == name)
            || decode_tool_name(name).is_some_and(|(_, tool)| is_sql_tool(&tool))
    };
    let record = |name: &str, arguments: &str| {
        if is_sql(name) {
            changed(arguments, recorded_input(arguments))
        } else if name == "tool_invoke" {
            recorded_invocation(arguments, &is_sql)
        } else {
            recorded_call_arguments(arguments)
        }
    };
    let removed = record_tool_calls(&mut json, &record)?;
    Some((json.to_string(), removed))
}

/// Whether `text`, JSON, may hold a string that names `tool_invoke` or one of
/// `sql_tools`. A name of only `[A-Za-z0-9_-]`, as every tool name a model API
/// takes is, is written in a JSON string as itself or with `\u` escapes, so
/// text with neither holds none. That check is cheap enough for every payload
/// task history keeps; only text that passes it is parsed.
fn may_name(text: &str, sql_tools: &[String]) -> bool {
    text.contains("\\u")
        || text.contains("tool_invoke")
        || sql_tools.iter().any(|name| {
            !name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-'))
                || text.contains(name.as_str())
        })
}

/// The recording of `arguments`, when it is not the arguments themselves.
fn changed(
    arguments: &str,
    (recorded, removed): (Cow<'_, str>, Vec<String>),
) -> Option<(String, Vec<String>)> {
    (recorded != arguments).then(|| (recorded.into_owned(), removed))
}

/// The arguments of a `tool_invoke` call, `{"tool_id", "arguments"}`, with
/// those of a call to an `sql` tool recorded as that tool records them, and
/// those of a call to any other tool by what they hold.
fn recorded_invocation(
    arguments: &str,
    is_sql: &dyn Fn(&str) -> bool,
) -> Option<(String, Vec<String>)> {
    let Ok(Value::Object(mut invocation)) = serde_json::from_str::<Value>(arguments) else {
        // Not `tool_invoke`'s JSON: it runs nothing, and keeps nothing.
        return Some((UNREADABLE_STATEMENT.to_string(), Vec::new()));
    };
    // A call that names no tool runs nothing, and keeps nothing either.
    let Some(tool) = invocation.get("tool_id").and_then(Value::as_str) else {
        return Some((UNREADABLE_STATEMENT.to_string(), Vec::new()));
    };
    let is_sql_call = is_sql(tool);
    let (inner, is_text) = match invocation.get("arguments") {
        Some(Value::String(text)) => (text.clone(), true),
        Some(Value::Null) | None => return None,
        Some(other) => (other.to_string(), false),
    };
    let (recorded, removed) = if is_sql_call {
        changed(&inner, recorded_input(&inner))?
    } else {
        recorded_call_arguments(&inner)?
    };
    invocation.insert("arguments".to_string(), as_sent(recorded, is_text));
    Some((Value::Object(invocation).to_string(), removed))
}

/// Record in `value` the arguments of each tool call `record` recognizes, and
/// take every value it removes out of every text in `value`, a tool's result
/// or a model's own words alike. Returns the values taken out, or `None` when
/// nothing changed.
fn record_tool_calls(value: &mut Value, record: &Recorder<'_>) -> Option<Vec<String>> {
    let mut removed = Vec::new();
    let rewritten = rewrite_calls(value, record, &mut removed);
    let redacted = !removed.is_empty() && redact_strings(value, &removed);
    (rewritten || redacted).then_some(removed)
}

fn rewrite_calls(value: &mut Value, record: &Recorder<'_>, removed: &mut Vec<String>) -> bool {
    match value {
        Value::Array(items) => items.iter_mut().fold(false, |rewritten, item| {
            rewrite_calls(item, record, removed) | rewritten
        }),
        Value::Object(map) => {
            let rewritten = rewrite_call(map, record, removed);
            map.values_mut().fold(rewritten, |rewritten, item| {
                rewrite_calls(item, record, removed) | rewritten
            })
        }
        _ => false,
    }
}

/// Rewrite `map` when it is a tool call: an object that names its tool and
/// holds its arguments, whatever its type, as every API Spice serves writes
/// one: Chat Completions' `function` and legacy `function_call` (`name`,
/// `arguments`), Responses' `function_call`, `mcp_call`, and
/// `mcp_approval_request` (`name`, `arguments`), and Messages' `tool_use` and
/// `mcp_tool_use` (`name`, `input`).
fn rewrite_call(
    map: &mut Map<String, Value>,
    record: &Recorder<'_>,
    removed: &mut Vec<String>,
) -> bool {
    let key = if map.contains_key("arguments") {
        "arguments"
    } else if map.contains_key("input") {
        "input"
    } else {
        return false;
    };
    record_arguments(map, key, record, removed)
}

/// Record the arguments at `key` of `call`, which names its tool, as `record`
/// says, and remember the values taken out.
fn record_arguments(
    call: &mut Map<String, Value>,
    key: &str,
    record: &Recorder<'_>,
    removed: &mut Vec<String>,
) -> bool {
    let Some(name) = call.get("name").and_then(Value::as_str) else {
        return false;
    };
    let (arguments, is_text) = match call.get(key) {
        Some(Value::String(text)) => (text.clone(), true),
        Some(other) => (other.to_string(), false),
        None => return false,
    };
    let Some((recorded, values)) = record(name, &arguments) else {
        return false;
    };
    call.insert(key.to_string(), as_sent(recorded, is_text));
    removed.extend(values);
    true
}

/// `recorded` arguments in the shape they were sent in: text, or the JSON the
/// text holds. A placeholder is text either way.
fn as_sent(recorded: String, is_text: bool) -> Value {
    if is_text {
        Value::String(recorded)
    } else {
        serde_json::from_str(&recorded).unwrap_or(Value::String(recorded))
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

    /// A payload a client sends back, or one of a model whose tools Spice did
    /// not run, is recorded with each call to an `sql` tool, directly or
    /// through `tool_invoke`, recorded as `sql` records it, in every shape;
    /// what that took out is taken out of the whole payload, a person's words
    /// included.
    #[test]
    fn a_payload_records_calls_to_sql_by_name() {
        let statement = "CREATE TABLE t WITH (from = 'postgres:public.t', pg_pass = 'hunter2')";
        let recorded_statement =
            "CREATE TABLE t WITH (from = 'postgres:public.t', pg_pass = '<redacted>')";
        let messages = json!({"messages": [
            {"role": "user", "content": "define t, its password is hunter2"},
            {"role": "assistant", "content": [
                {"type": "text", "text": "Defining t with the password hunter2."},
                {"type": "mcp_tool_use", "id": "m1", "name": "sql",
                    "server_name": "spice", "input": {"query": statement}}]},
            {"role": "user", "content": [{"type": "mcp_tool_result", "tool_use_id": "m1",
                "content": [{"type": "text", "text": "password 'hunter2' refused"}]}]},
        ]});
        let recorded: Value = serde_json::from_str(
            &recorded_by_name(&messages.to_string())
                .expect("the payload changes")
                .0,
        )
        .expect("the recorded payload is JSON");
        assert_eq!(
            recorded,
            json!({"messages": [
                {"role": "user", "content": "define t, its password is <redacted>"},
                {"role": "assistant", "content": [
                    {"type": "text", "text": "Defining t with the password <redacted>."},
                    {"type": "mcp_tool_use", "id": "m1", "name": "sql",
                        "server_name": "spice", "input": {"query": recorded_statement}}]},
                {"role": "user", "content": [{"type": "mcp_tool_result", "tool_use_id": "m1",
                    "content": [{"type": "text", "text": "password <redacted> refused"}]}]},
            ]})
        );

        // Responses' `mcp_call`, which holds its own output, and `tool_invoke`,
        // whose arguments for the tool it calls are JSON or text.
        let query = json!({"query": statement}).to_string();
        let responses = json!({"input": [
            {"type": "mcp_call", "id": "mcp_1", "name": "sql", "server_label": "spice",
                "arguments": query, "output": "password 'hunter2' refused"},
            {"type": "function_call", "call_id": "r2", "name": "tool_invoke",
                "arguments": json!({"tool_id": "sql", "arguments": {"query": statement}}).to_string()},
            {"type": "function_call", "call_id": "r3", "name": "tool_invoke",
                "arguments": json!({"tool_id": "sql", "arguments": query}).to_string()},
        ]});
        let recorded: Value = serde_json::from_str(
            &recorded_by_name(&responses.to_string())
                .expect("the payload changes")
                .0,
        )
        .expect("the recorded payload is JSON");
        let recorded_query = json!({"query": recorded_statement}).to_string();
        assert_eq!(
            recorded["input"][0],
            json!({"type": "mcp_call", "id": "mcp_1", "name": "sql", "server_label": "spice",
                "arguments": recorded_query, "output": "password <redacted> refused"})
        );
        let invocation = |index: usize| -> Value {
            serde_json::from_str(
                recorded["input"][index]["arguments"]
                    .as_str()
                    .expect("the call's arguments are text"),
            )
            .expect("the call's arguments are JSON")
        };
        assert_eq!(
            invocation(1),
            json!({"tool_id": "sql", "arguments": {"query": recorded_statement}})
        );
        assert_eq!(
            invocation(2),
            json!({"tool_id": "sql", "arguments": recorded_query})
        );

        // The `sql` tool of an MCP server, such as another Spice's.
        let peer = json!({"messages": [{"role": "assistant", "tool_calls": [
            {"id": "c1", "function": {"name": "peer__sql", "arguments": query}}]}]});
        let recorded: Value = serde_json::from_str(
            &recorded_by_name(&peer.to_string())
                .expect("the payload changes")
                .0,
        )
        .expect("the recorded payload is JSON");
        assert_eq!(
            recorded,
            json!({"messages": [{"role": "assistant", "tool_calls": [
                {"id": "c1", "function": {"name": "peer__sql", "arguments": recorded_query}}]}]})
        );

        // A name written with escapes is still read as the name.
        let escaped = format!(
            r#"{{"messages": [{{"role": "assistant", "tool_calls": [{{"id": "c1", "function": {{"name": "s\u0071l", "arguments": {}}}}}]}}]}}"#,
            Value::String(query)
        );
        let recorded: Value =
            serde_json::from_str(&recorded_by_name(&escaped).expect("the payload changes").0)
                .expect("the recorded payload is JSON");
        assert_eq!(
            recorded,
            json!({"messages": [{"role": "assistant", "tool_calls": [
                {"id": "c1", "function": {"name": "sql", "arguments": recorded_query}}]}]})
        );

        // A call to another tool, and text that is not JSON, are kept.
        let search = json!({"messages": [{"role": "assistant", "tool_calls": [
            {"id": "c1", "function": {"name": "search", "arguments": "{\"query\":\"hunter2\"}"}}]}]});
        assert_eq!(recorded_by_name(&search.to_string()), None);
        assert_eq!(recorded_by_name("define t, its password is hunter2"), None);
    }

    /// A call under any other name, in any shape an API writes one, is
    /// recorded by what its arguments hold: an MCP client's name for Spice's
    /// `sql` tool, a tool a Spicepod renamed in the legacy `function_call`, and
    /// an MCP approval request. A call whose arguments define nothing keeps
    /// them, without the values the other calls took out.
    #[test]
    fn a_call_to_any_tool_is_recorded_by_what_its_arguments_hold() {
        let statement = "CREATE TABLE t WITH (from = 'postgres:public.t', pg_pass = 'hunter2')";
        let recorded_statement =
            "CREATE TABLE t WITH (from = 'postgres:public.t', pg_pass = '<redacted>')";
        let query = json!({"query": statement}).to_string();
        let recorded_query = json!({"query": recorded_statement}).to_string();
        let payload = json!({"messages": [
            {"role": "assistant", "content": [{"type": "tool_use", "id": "m1",
                "name": "mcp__spice__sql", "input": {"query": statement}}]},
            {"role": "assistant", "content": null,
                "function_call": {"name": "run_query", "arguments": query}},
            {"type": "mcp_approval_request", "id": "a1", "name": "SQL", "server_label": "spice",
                "arguments": query},
            {"role": "assistant", "tool_calls": [{"id": "c1", "function": {"name": "search",
                "arguments": "{\"query\":\"hunter2 recipes\"}"}}]},
        ]});
        let (recorded, removed) =
            recorded_by_name(&payload.to_string()).expect("the payload changes");
        let recorded: Value =
            serde_json::from_str(&recorded).expect("the recorded payload is JSON");
        assert_eq!(
            recorded,
            json!({"messages": [
                {"role": "assistant", "content": [{"type": "tool_use", "id": "m1",
                    "name": "mcp__spice__sql", "input": {"query": recorded_statement}}]},
                {"role": "assistant", "content": null,
                    "function_call": {"name": "run_query", "arguments": recorded_query}},
                {"type": "mcp_approval_request", "id": "a1", "name": "SQL", "server_label": "spice",
                    "arguments": recorded_query},
                {"role": "assistant", "tool_calls": [{"id": "c1", "function": {"name": "search",
                    "arguments": "{\"query\":\"<redacted> recipes\"}"}}]},
            ]})
        );
        assert!(
            removed.iter().any(|value| value == "hunter2"),
            "{removed:?}"
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
