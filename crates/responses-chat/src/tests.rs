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

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_openai::error::{ApiError, OpenAIError};
use async_openai::types::chat::{
    ChatCompletionResponseStream, CreateChatCompletionRequest, CreateChatCompletionResponse,
    CreateChatCompletionStreamResponse,
};
use async_openai::types::responses::CreateResponse;
use async_trait::async_trait;
use chat_api::{Chat, SqlGeneration};
use futures::StreamExt;
use serde_json::{Value, json};

use crate::ChatResponses;
use crate::request::{Translated, chat_request};
use crate::response::{error_object, response};
use crate::stream::response_stream;

/// A request Codex 0.164 sent, sanitized: `client_metadata` and `prompt_cache_key` are
/// removed, local paths replaced, and long texts truncated.
const CODEX_REQUEST: &str = include_str!("../fixtures/codex_request.json");

const DOCS: &str = "https://spiceai.org/docs/components/models";

fn typed(body: &Value) -> CreateResponse {
    serde_json::from_value(body.clone()).expect("a Responses API request")
}

fn translate(body: &Value) -> Result<Translated, OpenAIError> {
    chat_request(typed(body))
}

/// The chat completion request `body` translates to, as JSON.
fn chat(body: &Value) -> Value {
    serde_json::to_value(translate(body).expect("a translatable request").chat)
        .expect("serializable")
}

fn json_of(value: &impl serde::Serialize) -> Value {
    serde_json::to_value(value).expect("serializable")
}

/// `value` as pretty JSON with every object's keys in sorted order, so a snapshot does not depend
/// on whether the workspace builds `serde_json` with `preserve_order`.
fn pretty(value: &Value) -> String {
    fn sorted(value: &Value) -> Value {
        match value {
            Value::Object(map) => {
                let mut entries: Vec<(&String, &Value)> = map.iter().collect();
                entries.sort_by_key(|(key, _)| *key);
                Value::Object(
                    entries
                        .into_iter()
                        .map(|(key, value)| (key.clone(), sorted(value)))
                        .collect(),
                )
            }
            Value::Array(items) => Value::Array(items.iter().map(sorted).collect()),
            other => other.clone(),
        }
    }
    serde_json::to_string_pretty(&sorted(value)).expect("serializable")
}

/// Replaces each ID the translation generates with its prefix and the order it first appeared
/// in (`<msg_1>`, `<msg_2>`, ...), and each timestamp with `<timestamp>`, after checking that it
/// is one: what the translation is free to choose, it is checked for, not compared.
#[derive(Default)]
struct Normalizer {
    ids: HashMap<String, String>,
    counts: HashMap<String, usize>,
}

/// The earliest timestamp a Response made now can have.
const EARLIEST: u64 = 1_750_000_000;

impl Normalizer {
    fn generated_prefix(text: &str) -> Option<&str> {
        let (prefix, suffix) = text.split_once('_')?;
        (["resp", "msg", "fc", "ctc", "call"].contains(&prefix)
            && suffix.len() == 24
            && suffix.chars().all(|c| c.is_ascii_alphanumeric()))
        .then_some(prefix)
    }

    fn value(&mut self, key: Option<&str>, value: &Value) -> Value {
        match value {
            Value::Object(map) => Value::Object(
                map.iter()
                    .map(|(key, value)| (key.clone(), self.value(Some(key), value)))
                    .collect(),
            ),
            Value::Array(items) => {
                Value::Array(items.iter().map(|item| self.value(None, item)).collect())
            }
            Value::Number(number) if matches!(key, Some("created_at" | "completed_at")) => {
                let seconds = number.as_u64().expect("a timestamp in seconds");
                assert!(seconds >= EARLIEST, "{key:?} {seconds} is not a timestamp");
                json!("<timestamp>")
            }
            Value::String(text) => match Self::generated_prefix(text) {
                Some(prefix) => {
                    let prefix = prefix.to_string();
                    let count = self.counts.entry(prefix.clone()).or_default();
                    let replacement = self.ids.entry(text.clone()).or_insert_with(|| {
                        *count += 1;
                        format!("<{prefix}_{count}>")
                    });
                    json!(replacement)
                }
                None => value.clone(),
            },
            other => other.clone(),
        }
    }
}

fn normalized(value: &Value) -> Value {
    Normalizer::default().value(None, value)
}

/// The `type`, `param` and `message` of an `OpenAIError::ApiError`.
fn error_fields(error: OpenAIError) -> (Option<String>, Option<String>, String) {
    match error {
        OpenAIError::ApiError(error) => (error.r#type, error.param, error.message),
        other => panic!("expected an API error, got {other:?}"),
    }
}

/// The refusal of something a chat completion cannot carry, as the request's model `gpt`.
fn refusal(param: &str, what: &str, remedy: &str) -> (Option<String>, Option<String>, String) {
    (
        Some("invalid_request_error".to_string()),
        Some(param.to_string()),
        format!(
            "Model 'gpt' serves the Responses API by translating requests into chat completions, which cannot carry {what}. {remedy}, or use a model whose provider serves the Responses API natively (`from: openai`, `from: azure`, or `from: xai`). See: {DOCS}"
        ),
    )
}

// --- Requests ---

#[test]
fn the_codex_request_translates_to_a_chat_completion_request() {
    let body: Value = serde_json::from_str(CODEX_REQUEST).expect("the fixture is JSON");
    let chat = chat(&body);
    insta::assert_snapshot!("codex_request_as_chat_completion", pretty(&chat));

    let functions: Vec<&str> = chat["tools"]
        .as_array()
        .expect("tools")
        .iter()
        .map(|tool| tool["function"]["name"].as_str().expect("a function name"))
        .collect();
    assert_eq!(
        functions,
        [
            "exec_command",
            "write_stdin",
            "request_user_input",
            "view_image",
            "multi_agent_v1__close_agent",
            "multi_agent_v1__resume_agent",
            "multi_agent_v1__send_input",
            "multi_agent_v1__spawn_agent",
            "multi_agent_v1__wait_agent",
            "get_goal",
            "create_goal",
            "update_goal",
        ],
        "the namespace's tools are flattened, and the hosted `web_search` is left out"
    );
    let roles: Vec<&Value> = chat["messages"]
        .as_array()
        .expect("messages")
        .iter()
        .map(|message| &message["role"])
        .collect();
    assert_eq!(
        roles,
        [
            &json!("system"),
            &json!("system"),
            &json!("user"),
            &json!("user")
        ]
    );

    // The request as Codex sent it, with the telemetry the fixture leaves out, is the same chat
    // completion request.
    let mut sent = body;
    sent["client_metadata"] = json!({"session_id": "s", "x-codex-turn-metadata": "{}"});
    sent["prompt_cache_key"] = json!("key");
    assert_eq!(self::chat(&sent), chat);
}

#[test]
fn instructions_and_a_string_input_are_a_system_and_a_user_message() {
    assert_eq!(
        chat(&json!({"model": "gpt", "instructions": "Be brief.", "input": "hi"})),
        json!({
            "model": "gpt",
            "messages": [
                {"role": "system", "content": "Be brief."},
                {"role": "user", "content": "hi"},
            ],
        })
    );
}

#[test]
fn input_items_become_chat_messages() {
    let cases = [
        (
            "messages of each role, developer as system",
            json!([
                {"type": "message", "role": "developer", "content": [{"type": "input_text", "text": "Rules."}]},
                {"type": "message", "role": "system", "content": [
                    {"type": "input_text", "text": "One."},
                    {"type": "input_text", "text": "Two."},
                ]},
                {"role": "user", "content": "Hi."},
                {"type": "message", "role": "user", "content": [
                    {"type": "input_text", "text": "Look:"},
                    {"type": "input_image", "image_url": "https://example.com/a.png"},
                    {"type": "input_image", "image_url": "data:image/png;base64,iVBOR", "detail": "high"},
                ]},
                {"role": "assistant", "content": "Looked."},
            ]),
            json!([
                {"role": "system", "content": "Rules."},
                {"role": "system", "content": [{"type": "text", "text": "One."}, {"type": "text", "text": "Two."}]},
                {"role": "user", "content": "Hi."},
                {"role": "user", "content": [
                    {"type": "text", "text": "Look:"},
                    {"type": "image_url", "image_url": {"url": "https://example.com/a.png"}},
                    {"type": "image_url", "image_url": {"url": "data:image/png;base64,iVBOR", "detail": "high"}},
                ]},
                {"role": "assistant", "content": "Looked."},
            ]),
        ),
        (
            "the text before tool calls, and the calls, are one assistant message",
            json!([
                {"role": "user", "content": "Read a.rs and b.rs."},
                {"type": "message", "id": "msg_1", "role": "assistant", "status": "completed", "content": [
                    {"type": "output_text", "text": "Reading both.", "annotations": []},
                ]},
                {"type": "reasoning", "id": "rs_1", "summary": [], "encrypted_content": "opaque"},
                {"type": "function_call", "call_id": "call_1", "name": "read_file", "arguments": "{\"path\":\"a.rs\"}"},
                {"type": "function_call", "id": "fc_2", "call_id": "call_2", "name": "read_file", "arguments": "{\"path\":\"b.rs\"}", "status": "completed"},
                {"type": "function_call_output", "call_id": "call_1", "output": "fn a() {}"},
                {"type": "function_call_output", "call_id": "call_2", "output": [
                    {"type": "input_text", "text": "fn b() {}"},
                    {"type": "input_text", "text": "// end"},
                ]},
                {"type": "message", "role": "assistant", "status": "completed", "id": "msg_2", "content": [
                    {"type": "output_text", "text": "Both read.", "annotations": []},
                ]},
            ]),
            json!([
                {"role": "user", "content": "Read a.rs and b.rs."},
                {"role": "assistant", "content": "Reading both.", "tool_calls": [
                    {"id": "call_1", "type": "function", "function": {"name": "read_file", "arguments": "{\"path\":\"a.rs\"}"}},
                    {"id": "call_2", "type": "function", "function": {"name": "read_file", "arguments": "{\"path\":\"b.rs\"}"}},
                ]},
                {"role": "tool", "tool_call_id": "call_1", "content": "fn a() {}"},
                {"role": "tool", "tool_call_id": "call_2", "content": "fn b() {}\n// end"},
                {"role": "assistant", "content": "Both read."},
            ]),
        ),
        (
            "images a tool returns follow the run of tool messages",
            json!([
                {"type": "function_call", "call_id": "call_1", "name": "view_image", "arguments": "{\"path\":\"a.png\"}"},
                {"type": "function_call", "call_id": "call_2", "name": "view_image", "arguments": "{\"path\":\"b.png\"}"},
                {"type": "function_call_output", "call_id": "call_1", "output": [
                    {"type": "input_image", "image_url": "data:image/png;base64,AAAA"},
                ]},
                {"type": "function_call_output", "call_id": "call_2", "output": [
                    {"type": "input_text", "text": "b.png"},
                    {"type": "input_image", "image_url": "data:image/png;base64,BBBB"},
                ]},
                {"role": "user", "content": "Compare them."},
            ]),
            json!([
                {"role": "assistant", "tool_calls": [
                    {"id": "call_1", "type": "function", "function": {"name": "view_image", "arguments": "{\"path\":\"a.png\"}"}},
                    {"id": "call_2", "type": "function", "function": {"name": "view_image", "arguments": "{\"path\":\"b.png\"}"}},
                ]},
                {"role": "tool", "tool_call_id": "call_1", "content": ""},
                {"role": "tool", "tool_call_id": "call_2", "content": "b.png"},
                {"role": "user", "content": [
                    {"type": "image_url", "image_url": {"url": "data:image/png;base64,AAAA"}},
                    {"type": "image_url", "image_url": {"url": "data:image/png;base64,BBBB"}},
                ]},
                {"role": "user", "content": "Compare them."},
            ]),
        ),
        (
            "a namespaced call names the namespace's function",
            json!([
                {"type": "function_call", "call_id": "call_1", "namespace": "multi_agent_v1", "name": "spawn_agent", "arguments": "{}"},
                {"type": "function_call", "call_id": "call_2", "namespace": "mcp__github__", "name": "create_issue", "arguments": "{}"},
                {"type": "function_call_output", "call_id": "call_1", "output": "spawned"},
                {"type": "function_call_output", "call_id": "call_2", "output": "created"},
            ]),
            json!([
                {"role": "assistant", "tool_calls": [
                    {"id": "call_1", "type": "function", "function": {"name": "multi_agent_v1__spawn_agent", "arguments": "{}"}},
                    {"id": "call_2", "type": "function", "function": {"name": "mcp__github__create_issue", "arguments": "{}"}},
                ]},
                {"role": "tool", "tool_call_id": "call_1", "content": "spawned"},
                {"role": "tool", "tool_call_id": "call_2", "content": "created"},
            ]),
        ),
        (
            "a custom tool call is a call of its function with the input",
            json!([
                {"type": "custom_tool_call", "id": "ctc_1", "call_id": "call_1", "name": "apply_patch", "input": "*** Begin Patch\n\"quoted\"\n*** End Patch"},
                {"type": "custom_tool_call_output", "call_id": "call_1", "output": "Done."},
                {"type": "custom_tool_call", "id": "ctc_2", "call_id": "call_2", "name": "apply_patch", "input": ""},
                {"type": "custom_tool_call_output", "call_id": "call_2", "output": [{"type": "input_text", "text": "Empty."}]},
            ]),
            json!([
                {"role": "assistant", "tool_calls": [
                    {"id": "call_1", "type": "function", "function": {"name": "apply_patch", "arguments": "{\"input\":\"*** Begin Patch\\n\\\"quoted\\\"\\n*** End Patch\"}"}},
                ]},
                {"role": "tool", "tool_call_id": "call_1", "content": "Done."},
                {"role": "assistant", "tool_calls": [
                    {"id": "call_2", "type": "function", "function": {"name": "apply_patch", "arguments": "{\"input\":\"\"}"}},
                ]},
                {"role": "tool", "tool_call_id": "call_2", "content": "Empty."},
            ]),
        ),
        (
            "text after tool calls, before their outputs, joins the message that called them",
            json!([
                {"type": "function_call", "call_id": "call_1", "name": "read_file", "arguments": "{}"},
                {"type": "message", "id": "msg_1", "role": "assistant", "status": "completed", "content": [
                    {"type": "output_text", "text": "Reading.", "annotations": []},
                    {"type": "refusal", "refusal": " Not that one."},
                ]},
                {"type": "function_call", "call_id": "call_2", "name": "read_file", "arguments": "{}"},
                {"type": "function_call_output", "call_id": "call_1", "output": "1"},
                {"type": "function_call_output", "call_id": "call_2", "output": "2"},
            ]),
            json!([
                {"role": "assistant", "content": "Reading. Not that one.", "tool_calls": [
                    {"id": "call_1", "type": "function", "function": {"name": "read_file", "arguments": "{}"}},
                    {"id": "call_2", "type": "function", "function": {"name": "read_file", "arguments": "{}"}},
                ]},
                {"role": "tool", "tool_call_id": "call_1", "content": "1"},
                {"role": "tool", "tool_call_id": "call_2", "content": "2"},
            ]),
        ),
        (
            "an assistant message without text, and reasoning, carry nothing",
            json!([
                {"role": "user", "content": "Hi."},
                {"type": "reasoning", "summary": [{"type": "summary_text", "text": "Greeting."}]},
                {"role": "assistant", "content": ""},
                {"role": "assistant", "content": "Hello."},
            ]),
            json!([
                {"role": "user", "content": "Hi."},
                {"role": "assistant", "content": "Hello."},
            ]),
        ),
    ];
    for (case, input, messages) in cases {
        assert_eq!(
            chat(&json!({"model": "gpt", "input": input}))["messages"],
            messages,
            "{case}"
        );
    }
}

/// The tools of an agent: a function, a namespace of functions, a custom tool with a grammar,
/// and hosted tools.
fn agent_tools() -> Value {
    json!([
        {"type": "function", "name": "read_file", "description": "Reads a file.", "strict": false,
         "parameters": {"type": "object", "properties": {"path": {"type": "string"}}, "required": ["path"]}},
        {"type": "namespace", "name": "multi_agent_v1", "description": "Sub-agents.", "tools": [
            {"type": "function", "name": "spawn_agent", "description": "Spawns an agent.",
             "parameters": {"type": "object", "properties": {"message": {"type": "string"}}}},
        ]},
        {"type": "custom", "name": "apply_patch", "description": "Edits files.",
         "format": {"type": "grammar", "syntax": "lark", "definition": "start: \"*** Begin Patch\""}},
        {"type": "web_search", "external_web_access": false},
        {"type": "file_search", "vector_store_ids": ["vs_1"]},
        {"type": "code_interpreter", "container": {"type": "auto"}},
        {"type": "image_generation"},
        {"type": "mcp", "server_label": "deepwiki", "server_url": "https://mcp.deepwiki.com/mcp"},
        {"type": "local_shell"},
        {"type": "computer_use_preview", "environment": "browser", "display_width": 1024, "display_height": 768},
    ])
}

#[test]
fn the_hosted_tools_left_out_are_named() {
    let translated = translate(&json!({"model": "gpt", "input": "hi", "tools": agent_tools()}))
        .expect("a translatable request");
    assert_eq!(
        translated.left_out_tools,
        [
            "web_search",
            "file_search",
            "code_interpreter",
            "image_generation",
            "mcp",
            "local_shell",
            "computer_use_preview"
        ]
    );
}

#[test]
fn tools_become_functions_and_hosted_tools_are_left_out() {
    let chat = chat(&json!({"model": "gpt", "input": "hi", "tools": agent_tools()}));
    assert_eq!(
        chat["tools"],
        json!([
            {"type": "function", "function": {"name": "read_file", "description": "Reads a file.", "strict": false,
             "parameters": {"type": "object", "properties": {"path": {"type": "string"}}, "required": ["path"]}}},
            {"type": "function", "function": {"name": "multi_agent_v1__spawn_agent", "description": "Spawns an agent.",
             "parameters": {"type": "object", "properties": {"message": {"type": "string"}}}}},
            {"type": "function", "function": {"name": "apply_patch", "description": "Edits files.",
             "parameters": {
                 "type": "object",
                 "properties": {"input": {"type": "string", "description": "The tool's input, as free-form text that matches this Lark grammar:\nstart: \"*** Begin Patch\""}},
                 "required": ["input"],
                 "additionalProperties": false,
             }}},
        ])
    );
    let text_tool = self::chat(&json!({"model": "gpt", "input": "hi", "tools": [
        {"type": "custom", "name": "note", "format": {"type": "text"}},
    ]}));
    assert_eq!(
        text_tool["tools"][0]["function"]["parameters"]["properties"]["input"],
        json!({"type": "string", "description": "The tool's input, as free-form text."})
    );
}

#[test]
fn two_tools_offered_as_one_function_are_refused() {
    let error = translate(&json!({"model": "gpt", "input": "hi", "tools": [
        {"type": "function", "name": "ns__run"},
        {"type": "namespace", "name": "ns", "description": "", "tools": [{"type": "function", "name": "run"}]},
    ]}))
    .expect_err("a collision");
    assert_eq!(
        error_fields(error),
        (
            Some("invalid_request_error".to_string()),
            Some("tools".to_string()),
            format!(
                "Model 'gpt' serves the Responses API by translating requests into chat completions, which offer each tool as a function, and two of the request's tools would both be the function 'ns__run'. Rename one of them, or use a model whose provider serves the Responses API natively (`from: openai`, `from: azure`, or `from: xai`). See: {DOCS}"
            )
        )
    );
}

#[test]
fn request_options_become_chat_options() {
    let with = |extra: Value| {
        let mut body = json!({"model": "gpt", "input": "hi", "tools": [{"type": "function", "name": "read_file"}]});
        for (key, value) in extra.as_object().expect("an object") {
            body[key] = value.clone();
        }
        let mut chat = chat(&body);
        let object = chat.as_object_mut().expect("an object");
        object.remove("messages");
        object.remove("tools");
        chat
    };
    let cases = [
        (json!({}), json!({"model": "gpt"})),
        (
            json!({"tool_choice": "auto"}),
            json!({"model": "gpt", "tool_choice": "auto"}),
        ),
        (
            json!({"tool_choice": "none"}),
            json!({"model": "gpt", "tool_choice": "none"}),
        ),
        (
            json!({"tool_choice": "required"}),
            json!({"model": "gpt", "tool_choice": "required"}),
        ),
        (
            json!({"tool_choice": {"type": "function", "name": "read_file"}}),
            json!({"model": "gpt", "tool_choice": {"type": "function", "function": {"name": "read_file"}}}),
        ),
        (
            json!({"tool_choice": {"type": "custom", "name": "apply_patch"}}),
            json!({"model": "gpt", "tool_choice": {"type": "function", "function": {"name": "apply_patch"}}}),
        ),
        (
            json!({"tool_choice": {"type": "allowed_tools", "mode": "required", "tools": [
                {"type": "function", "name": "read_file"},
                {"type": "function", "namespace": "multi_agent_v1", "name": "spawn_agent"},
                {"type": "image_generation"},
            ]}}),
            json!({"model": "gpt", "tool_choice": {"type": "allowed_tools", "allowed_tools": {"mode": "required", "tools": [
                {"type": "function", "function": {"name": "read_file"}},
                {"type": "function", "function": {"name": "multi_agent_v1__spawn_agent"}},
            ]}}}),
        ),
        (
            json!({"tool_choice": {"type": "allowed_tools", "mode": "auto", "tools": [{"type": "image_generation"}]}}),
            json!({"model": "gpt", "tool_choice": "none"}),
        ),
        (
            json!({"parallel_tool_calls": false}),
            json!({"model": "gpt", "parallel_tool_calls": false}),
        ),
        (
            json!({"temperature": 0.5, "top_p": 0.25, "max_output_tokens": 1000}),
            json!({"model": "gpt", "temperature": 0.5, "top_p": 0.25, "max_completion_tokens": 1000}),
        ),
        (
            json!({"reasoning": {"effort": "high", "summary": "auto"}}),
            json!({"model": "gpt", "reasoning_effort": "high"}),
        ),
        (
            json!({"text": {"format": {"type": "json_schema", "name": "answer", "schema": {"type": "object"}, "strict": true}}}),
            json!({"model": "gpt", "response_format": {"type": "json_schema", "json_schema": {"name": "answer", "schema": {"type": "object"}, "strict": true}}}),
        ),
        (
            json!({"text": {"format": {"type": "json_object"}}}),
            json!({"model": "gpt", "response_format": {"type": "json_object"}}),
        ),
        (
            json!({"text": {"format": {"type": "text"}}}),
            json!({"model": "gpt"}),
        ),
        (
            json!({"stream": true}),
            json!({"model": "gpt", "stream": true, "stream_options": {"include_usage": true}}),
        ),
    ];
    for (extra, expected) in cases {
        assert_eq!(with(extra.clone()), expected, "{extra}");
    }
}

/// `parallel_tool_calls` and `tool_choice` mean nothing without tools, and some servers refuse
/// them then; the hosted tools are not offered, so a request with only those offers none.
#[test]
fn tool_options_without_functions_to_call_are_left_out() {
    let body = json!({
        "model": "gpt", "input": "hi", "tools": [{"type": "web_search"}],
        "tool_choice": "auto", "parallel_tool_calls": true,
    });
    assert_eq!(
        chat(&body),
        json!({"model": "gpt", "messages": [{"role": "user", "content": "hi"}]})
    );
    let mut required = body;
    required["tool_choice"] = json!("required");
    assert_eq!(
        error_fields(translate(&required).expect_err("nothing to call")),
        refusal(
            "tool_choice",
            "a `tool_choice` that asks for a tool call when the model is offered no function to call",
            "Add the function to `tools`"
        )
    );
}

#[test]
fn what_only_changes_how_a_request_is_served_is_ignored() {
    let base = json!({"model": "gpt", "input": "hi"});
    let expected = chat(&base);
    for field in [
        json!({"store": false}),
        json!({"include": ["reasoning.encrypted_content", "message.output_text.logprobs"]}),
        json!({"prompt_cache_key": "key"}),
        json!({"prompt_cache_retention": "24h"}),
        json!({"client_metadata": {"session_id": "s"}}),
        json!({"metadata": {"k": "v"}}),
        json!({"service_tier": "flex"}),
        json!({"truncation": "auto"}),
        json!({"safety_identifier": "user-1"}),
        json!({"user": "user-1"}),
        json!({"stream_options": {"include_obfuscation": false}}),
        json!({"max_tool_calls": 3}),
        json!({"reasoning": {"summary": "detailed"}}),
        json!({"text": {"verbosity": "low"}}),
        json!({"background": false}),
        json!({"top_logprobs": 0}),
    ] {
        let mut body = base.clone();
        for (key, value) in field.as_object().expect("an object") {
            body[key] = value.clone();
        }
        let chat = translate(&body).unwrap_or_else(|e| panic!("{field} refused: {e}"));
        assert_eq!(json_of(&chat.chat), expected, "{field}");
    }
}

#[test]
fn what_a_chat_completion_cannot_carry_is_refused_by_name() {
    let with = |key: &str, value: Value| {
        let mut body = json!({"model": "gpt", "input": "hi"});
        body[key] = value;
        body
    };
    let input = |items: Value| json!({"model": "gpt", "input": items});
    let cases = [
        (
            with("previous_response_id", json!("resp_1")),
            refusal(
                "previous_response_id",
                "`previous_response_id`, since the provider keeps no earlier responses",
                "Send the whole conversation in `input` instead",
            ),
        ),
        (
            with("conversation", json!("conv_1")),
            refusal(
                "conversation",
                "`conversation`, since the provider keeps no conversations",
                "Send the whole conversation in `input` instead",
            ),
        ),
        (
            with("background", json!(true)),
            refusal(
                "background",
                "`background: true`, since a chat completion runs only while the request waits for it",
                "Remove `background`",
            ),
        ),
        (
            with("prompt", json!({"id": "pmpt_1"})),
            refusal(
                "prompt",
                "a stored `prompt` template",
                "Send the prompt's text in `instructions` and `input` instead",
            ),
        ),
        (
            with("top_logprobs", json!(5)),
            refusal("top_logprobs", "`top_logprobs`", "Remove `top_logprobs`"),
        ),
        (
            input(json!([{"type": "message", "role": "user", "content": [
                {"type": "input_file", "file_id": "file_1"},
            ]}])),
            refusal(
                "input",
                "`input_file` content",
                "Send the file's text as `input_text`, or an image as `input_image`",
            ),
        ),
        (
            input(
                json!([{"type": "function_call_output", "call_id": "call_1", "output": [
                    {"type": "input_file", "file_data": "data:application/pdf;base64,JVBER", "filename": "a.pdf"},
                ]}]),
            ),
            refusal(
                "input",
                "`input_file` content",
                "Send the file's text as `input_text`, or an image as `input_image`",
            ),
        ),
        (
            input(json!([{"type": "item_reference", "id": "msg_1"}])),
            refusal(
                "input",
                "the reference to item 'msg_1', since the provider keeps no earlier items",
                "Send the item itself instead of a reference to it",
            ),
        ),
        (
            input(
                json!([{"role": "user", "content": [{"type": "input_image", "file_id": "file_1"}]}]),
            ),
            refusal(
                "input",
                "an image by `file_id`, since the provider keeps no files",
                "Send the image as an `image_url`, which may be a `data:` URL",
            ),
        ),
        (
            input(json!([{"type": "message", "role": "developer", "content": [
                {"type": "input_image", "image_url": "https://example.com/a.png"},
            ]}])),
            refusal(
                "input",
                "an image in a system or developer message",
                "Send the image in a user message",
            ),
        ),
        (
            input(
                json!([{"type": "web_search_call", "id": "ws_1", "status": "completed", "action": {"type": "search", "query": "q"}}]),
            ),
            refusal(
                "input",
                "`web_search_call` items, which only the provider's own tools produce",
                "Remove the `web_search_call` items from `input`",
            ),
        ),
        (
            input(json!([{"type": "compaction", "encrypted_content": "opaque"}])),
            refusal(
                "input",
                "`compaction` items, which only the provider's own tools produce",
                "Remove the `compaction` items from `input`",
            ),
        ),
        (
            json!({"model": "gpt", "input": "hi", "tools": [{"type": "web_search_preview"}], "tool_choice": {"type": "web_search_preview"}}),
            refusal(
                "tool_choice",
                "a `tool_choice` of the provider's own tool `web_search_preview`, which the model is not offered",
                "Choose `auto`, `none`, `required`, or a function",
            ),
        ),
        (
            json!({"model": "gpt", "input": "hi", "tools": [{"type": "image_generation"}],
                   "tool_choice": {"type": "allowed_tools", "mode": "required", "tools": [{"type": "image_generation"}]}}),
            refusal(
                "tool_choice",
                "a `tool_choice` that requires one of the provider's own tools, which the model is not offered",
                "Allow a function in `tool_choice.tools`",
            ),
        ),
    ];
    for (body, expected) in cases {
        let error = translate(&body).expect_err("a refused request");
        assert_eq!(error_fields(error), expected, "{body}");
    }
}

#[test]
fn a_request_without_a_model_is_refused() {
    let error = translate(&json!({"input": "hi"})).expect_err("no model");
    assert_eq!(
        error_fields(error),
        (
            Some("invalid_request_error".to_string()),
            Some("model".to_string()),
            "A Responses API request names the `model` that answers it. Set `model` to the name of a model in the Spicepod.".to_string()
        )
    );
}

// --- Responses ---

fn completion(message: &Value, finish_reason: &str, usage: &Value) -> CreateChatCompletionResponse {
    serde_json::from_value(json!({
        "id": "chatcmpl-abc",
        "object": "chat.completion",
        "created": 0,
        "model": "provider-model-2026",
        "choices": [{"index": 0, "message": message, "finish_reason": finish_reason}],
        "usage": usage,
    }))
    .expect("a chat completion")
}

fn full_usage() -> Value {
    json!({
        "prompt_tokens": 120, "completion_tokens": 30, "total_tokens": 150,
        "prompt_tokens_details": {"cached_tokens": 100},
        "completion_tokens_details": {"reasoning_tokens": 12},
    })
}

/// A request with the agent's tools, and every option a Response repeats.
fn agent_request() -> Value {
    json!({
        "model": "gpt",
        "instructions": "Be brief.",
        "input": "Spawn a helper, then patch a.rs.",
        "tools": agent_tools(),
        "tool_choice": "auto",
        "parallel_tool_calls": true,
        "temperature": 0.5,
        "top_p": 0.25,
        "max_output_tokens": 1000,
    })
}

fn context(body: &Value) -> crate::response::ResponseContext {
    translate(body).expect("a translatable request").context
}

#[test]
fn a_completion_becomes_a_response() {
    let answer = completion(
        &json!({
            "role": "assistant",
            "content": "Spawning, then patching.",
            "tool_calls": [
                {"id": "call_1", "type": "function", "function": {"name": "multi_agent_v1__spawn_agent", "arguments": "{\"message\":\"help\"}"}},
                {"id": "call_2", "type": "function", "function": {"name": "apply_patch", "arguments": "{\"input\":\"*** Begin Patch\\n*** End Patch\"}"}},
                {"id": "call_3", "type": "function", "function": {"name": "read_file", "arguments": ""}},
                {"id": "", "type": "function", "function": {"name": "not_offered", "arguments": "{}"}},
            ],
        }),
        "tool_calls",
        &full_usage(),
    );
    let response = response(answer, &context(&agent_request())).expect("a response");
    let offered: Vec<Value> = agent_tools()
        .as_array()
        .expect("tools")
        .iter()
        .filter(|tool| {
            ["function", "namespace", "custom"].contains(&tool["type"].as_str().unwrap_or_default())
        })
        .cloned()
        .collect();
    assert_eq!(
        json_of(&response)["tools"],
        json_of(&typed(&json!({"model": "gpt", "input": "hi", "tools": offered})).tools),
        "the Response repeats the tools the model was offered, and none of the hosted ones it never saw"
    );

    let mut body = normalized(&json_of(&response));
    body.as_object_mut().expect("an object").remove("tools");
    assert_eq!(
        body,
        json!({
            "id": "<resp_1>",
            "object": "response",
            "created_at": "<timestamp>",
            "completed_at": "<timestamp>",
            "status": "completed",
            "model": "gpt",
            "instructions": "Be brief.",
            "max_output_tokens": 1000,
            "parallel_tool_calls": true,
            "temperature": 0.5,
            "top_p": 0.25,
            "tool_choice": "auto",
            "output": [
                {"type": "message", "id": "<msg_1>", "role": "assistant", "status": "completed", "content": [
                    {"type": "output_text", "text": "Spawning, then patching.", "annotations": [], "logprobs": []},
                ]},
                {"type": "function_call", "id": "<fc_1>", "call_id": "call_1", "namespace": "multi_agent_v1", "name": "spawn_agent",
                 "arguments": "{\"message\":\"help\"}", "status": "completed"},
                {"type": "custom_tool_call", "id": "<ctc_1>", "call_id": "call_2", "name": "apply_patch",
                 "input": "*** Begin Patch\n*** End Patch"},
                {"type": "function_call", "id": "<fc_2>", "call_id": "call_3", "name": "read_file", "arguments": "{}", "status": "completed"},
                {"type": "function_call", "id": "<fc_3>", "call_id": "<call_1>", "name": "not_offered", "arguments": "{}", "status": "completed"},
            ],
            "usage": {
                "input_tokens": 120, "input_tokens_details": {"cached_tokens": 100},
                "output_tokens": 30, "output_tokens_details": {"reasoning_tokens": 12},
                "total_tokens": 150,
            },
        })
    );
}

#[test]
fn a_response_repeats_its_requests_tools_and_defaults() {
    let answer = completion(
        &json!({"role": "assistant", "content": "hi"}),
        "stop",
        &full_usage(),
    );
    let tools = vec![json!({"type": "function", "name": "read_file"})];
    let with_tools = json_of(
        &response(
            answer.clone(),
            &context(&json!({"model": "gpt", "input": "hi", "tools": tools})),
        )
        .expect("a response"),
    );
    assert_eq!(
        (
            &with_tools["tools"],
            &with_tools["tool_choice"],
            &with_tools["parallel_tool_calls"]
        ),
        (&json!(tools), &json!("auto"), &json!(true))
    );
    let bare = json_of(
        &response(answer, &context(&json!({"model": "gpt", "input": "hi"}))).expect("a response"),
    );
    assert_eq!(
        [
            &bare["tools"],
            &bare["tool_choice"],
            &bare["parallel_tool_calls"],
            &bare["temperature"],
            &bare["top_p"],
            &bare["max_output_tokens"],
            &bare["instructions"],
        ],
        [
            &json!([]),
            &json!("auto"),
            &json!(true),
            &Value::Null,
            &Value::Null,
            &Value::Null,
            &Value::Null
        ],
        "the Responses API's defaults for tools, and nothing for sampling the model chose"
    );
}

#[test]
fn finish_reasons_become_statuses() {
    let context = context(&agent_request());
    let summary = |message: Value, finish: &str| {
        let response = json_of(
            &response(completion(&message, finish, &full_usage()), &context).expect("a response"),
        );
        json!({
            "status": response["status"],
            "incomplete_details": response["incomplete_details"],
            "completed_at": response["completed_at"].is_u64(),
            "items": response["output"].as_array().expect("output").iter().map(|item| json!([item["type"], item["status"]])).collect::<Vec<_>>(),
        })
    };
    let text = json!({"role": "assistant", "content": "Partial"});
    let cases = [
        (
            text.clone(),
            "stop",
            json!({"status": "completed", "incomplete_details": null, "completed_at": true, "items": [["message", "completed"]]}),
        ),
        (
            text.clone(),
            "length",
            json!({"status": "incomplete", "incomplete_details": {"reason": "max_output_tokens"}, "completed_at": false, "items": [["message", "incomplete"]]}),
        ),
        (
            text,
            "content_filter",
            json!({"status": "incomplete", "incomplete_details": {"reason": "content_filter"}, "completed_at": false, "items": [["message", "completed"]]}),
        ),
        (
            json!({"role": "assistant", "content": "Reading.", "tool_calls": [
                {"id": "call_1", "type": "function", "function": {"name": "read_file", "arguments": "{\"path\":\"a.rs\"}"}},
                {"id": "call_2", "type": "function", "function": {"name": "read_file", "arguments": "{\"path\":"}},
            ]}),
            "length",
            json!({"status": "incomplete", "incomplete_details": {"reason": "max_output_tokens"}, "completed_at": false,
                   "items": [["message", "completed"], ["function_call", "completed"], ["function_call", "incomplete"]]}),
        ),
        (
            json!({"role": "assistant", "content": null, "tool_calls": [
                {"id": "call_1", "type": "function", "function": {"name": "read_file", "arguments": "{}"}},
            ]}),
            "tool_calls",
            json!({"status": "completed", "incomplete_details": null, "completed_at": true, "items": [["function_call", "completed"]]}),
        ),
    ];
    for (message, finish, expected) in cases {
        assert_eq!(
            summary(message.clone(), finish),
            expected,
            "{finish}: {message}"
        );
    }
}

#[test]
fn chat_usage_becomes_response_usage() {
    let context = context(&json!({"model": "gpt", "input": "hi"}));
    let usage_of = |usage: Value| {
        json_of(
            &response(
                completion(
                    &json!({"role": "assistant", "content": "hi"}),
                    "stop",
                    &usage,
                ),
                &context,
            )
            .expect("a response"),
        )
        .get("usage")
        .cloned()
    };
    assert_eq!(
        [
            usage_of(full_usage()),
            usage_of(json!({"prompt_tokens": 7, "completion_tokens": 3, "total_tokens": 10})),
            usage_of(Value::Null),
        ],
        [
            Some(
                json!({"input_tokens": 120, "input_tokens_details": {"cached_tokens": 100}, "output_tokens": 30, "output_tokens_details": {"reasoning_tokens": 12}, "total_tokens": 150})
            ),
            Some(
                json!({"input_tokens": 7, "input_tokens_details": {"cached_tokens": 0}, "output_tokens": 3, "output_tokens_details": {"reasoning_tokens": 0}, "total_tokens": 10})
            ),
            None,
        ],
        "a breakdown the completion did not report is 0, and usage it did not report is absent"
    );
}

#[test]
fn a_completion_without_a_choice_is_a_server_error() {
    let mut answer = completion(
        &json!({"role": "assistant", "content": "hi"}),
        "stop",
        &full_usage(),
    );
    answer.choices.clear();
    let error =
        response(answer, &context(&json!({"model": "gpt", "input": "hi"}))).expect_err("no choice");
    assert_eq!(
        error_fields(error),
        (
            Some("server_error".to_string()),
            None,
            "Model 'gpt' returned a chat completion without a choice.".to_string()
        )
    );
}

#[test]
fn chat_errors_become_response_errors_of_their_kind() {
    let api = |r#type: Option<&str>, code: Option<&str>| {
        OpenAIError::ApiError(ApiError {
            message: "m".to_string(),
            r#type: r#type.map(str::to_string),
            param: None,
            code: code.map(str::to_string),
        })
    };
    let cases = [
        (
            api(
                Some("invalid_request_error"),
                Some("context_length_exceeded"),
            ),
            "context_length_exceeded",
            "m",
        ),
        (
            api(Some("rate_limit_error"), None),
            "rate_limit_exceeded",
            "m",
        ),
        (
            api(None, Some("insufficient_quota")),
            "insufficient_quota",
            "m",
        ),
        (
            api(Some("invalid_request_error"), None),
            "invalid_prompt",
            "m",
        ),
        (api(Some("authentication_error"), None), "server_error", "m"),
        (api(Some("overloaded_error"), None), "server_error", "m"),
        (
            OpenAIError::InvalidArgument("bad".to_string()),
            "server_error",
            "invalid args: bad",
        ),
    ];
    for (error, code, message) in cases {
        let object = json_of(&error_object(&error));
        assert_eq!(
            object,
            json!({"code": code, "message": message}),
            "{error:?}"
        );
    }
}

/// A Response's output, given back as the next request's input with the tools' outputs, is the
/// conversation the chat model had: a client that keeps no state of its own, as Codex keeps
/// none with `store: false`, can carry on with it.
#[test]
fn a_responses_output_is_read_back_as_the_next_requests_input() {
    let first = agent_request();
    let answer = completion(
        &json!({
            "role": "assistant",
            "content": "Spawning, then patching.",
            "tool_calls": [
                {"id": "call_1", "type": "function", "function": {"name": "multi_agent_v1__spawn_agent", "arguments": "{\"message\":\"help\"}"}},
                {"id": "call_2", "type": "function", "function": {"name": "apply_patch", "arguments": "{\"input\":\"*** Begin Patch\\n*** End Patch\"}"}},
            ],
        }),
        "tool_calls",
        &full_usage(),
    );
    let output =
        json_of(&response(answer, &context(&first)).expect("a response"))["output"].clone();

    let mut input = vec![json!({"role": "user", "content": "Spawn a helper, then patch a.rs."})];
    input.extend(output.as_array().expect("output").iter().cloned());
    input.push(json!({"type": "function_call_output", "call_id": "call_1", "output": "agent-7"}));
    input.push(
        json!({"type": "custom_tool_call_output", "call_id": "call_2", "output": "Patched."}),
    );
    let mut next = first;
    next["input"] = json!(input);

    assert_eq!(
        chat(&next)["messages"],
        json!([
            {"role": "system", "content": "Be brief."},
            {"role": "user", "content": "Spawn a helper, then patch a.rs."},
            {"role": "assistant", "content": "Spawning, then patching.", "tool_calls": [
                {"id": "call_1", "type": "function", "function": {"name": "multi_agent_v1__spawn_agent", "arguments": "{\"message\":\"help\"}"}},
                {"id": "call_2", "type": "function", "function": {"name": "apply_patch", "arguments": "{\"input\":\"*** Begin Patch\\n*** End Patch\"}"}},
            ]},
            {"role": "tool", "tool_call_id": "call_1", "content": "agent-7"},
            {"role": "tool", "tool_call_id": "call_2", "content": "Patched."},
        ])
    );
}

// --- Streams ---

fn chunk(choice: &Value) -> CreateChatCompletionStreamResponse {
    serde_json::from_value(json!({
        "id": "chatcmpl-abc", "object": "chat.completion.chunk", "created": 0, "model": "provider-model-2026",
        "choices": [choice],
    }))
    .expect("a chunk")
}

fn text(text: &str) -> Scripted {
    Scripted::Chunk(chunk(
        &json!({"index": 0, "delta": {"content": text}, "finish_reason": null}),
    ))
}

fn refused(text: &str) -> Scripted {
    Scripted::Chunk(chunk(
        &json!({"index": 0, "delta": {"refusal": text}, "finish_reason": null}),
    ))
}

fn finish(reason: &str) -> Scripted {
    Scripted::Chunk(chunk(
        &json!({"index": 0, "delta": {}, "finish_reason": reason}),
    ))
}

fn usage() -> Scripted {
    Scripted::Chunk(
        serde_json::from_value(json!({
            "id": "chatcmpl-abc", "object": "chat.completion.chunk", "created": 0, "model": "provider-model-2026",
            "choices": [],
            "usage": full_usage(),
        }))
        .expect("a usage chunk"),
    )
}

fn call(index: u32, id: Option<&str>, name: Option<&str>, arguments: &str) -> Scripted {
    let mut function = json!({"arguments": arguments});
    if let Some(name) = name {
        function["name"] = json!(name);
    }
    let mut call = json!({"index": index, "function": function});
    if let Some(id) = id {
        call["id"] = json!(id);
        call["type"] = json!("function");
    }
    Scripted::Chunk(chunk(
        &json!({"index": 0, "delta": {"tool_calls": [call]}, "finish_reason": null}),
    ))
}

fn rate_limited() -> Scripted {
    Scripted::Error(ApiError {
        message: "Rate limit reached".to_string(),
        r#type: Some("requests".to_string()),
        param: None,
        code: Some("rate_limit_exceeded".to_string()),
    })
}

/// What a scripted chat model streams: a chunk, or an error.
#[derive(Clone)]
enum Scripted {
    Chunk(CreateChatCompletionStreamResponse),
    Error(ApiError),
}

/// A chat model that records each request and answers with `answer`, or streams `stream`.
struct ScriptedChat {
    requests: Mutex<Vec<CreateChatCompletionRequest>>,
    answer: Option<CreateChatCompletionResponse>,
    stream: Vec<Scripted>,
}

impl ScriptedChat {
    fn new(answer: Option<CreateChatCompletionResponse>, stream: Vec<Scripted>) -> Arc<Self> {
        Arc::new(Self {
            requests: Mutex::new(Vec::new()),
            answer,
            stream,
        })
    }
}

#[async_trait]
impl Chat for ScriptedChat {
    fn as_sql(&self) -> Option<&dyn SqlGeneration> {
        None
    }

    async fn chat_request(
        &self,
        req: CreateChatCompletionRequest,
    ) -> Result<CreateChatCompletionResponse, OpenAIError> {
        self.requests.lock().expect("requests lock").push(req);
        self.answer
            .clone()
            .ok_or_else(|| OpenAIError::InvalidArgument("no answer scripted".to_string()))
    }

    async fn chat_stream(
        &self,
        req: CreateChatCompletionRequest,
    ) -> Result<ChatCompletionResponseStream, OpenAIError> {
        self.requests.lock().expect("requests lock").push(req);
        let items: Vec<_> = self
            .stream
            .iter()
            .cloned()
            .map(|scripted| match scripted {
                Scripted::Chunk(chunk) => Ok(chunk),
                Scripted::Error(error) => Err(OpenAIError::ApiError(error)),
            })
            .collect();
        Ok(Box::pin(futures::stream::iter(items)))
    }
}

/// The events of the stream `chat` answers `body` with.
async fn streamed(chat: &Arc<ScriptedChat>, body: &Value) -> Vec<Value> {
    ChatResponses::new(Arc::clone(chat) as Arc<dyn Chat>)
        .create_stream(typed(body))
        .await
        .expect("a stream")
        .map(|event| json_of(&event.expect("every event is Ok")))
        .collect()
        .await
}

async fn stream_of(scripted: Vec<Scripted>, body: &Value) -> Vec<Value> {
    streamed(&ScriptedChat::new(None, scripted), body).await
}

/// Checks that `events` are a well-formed Response stream, by building each output item from
/// its events alone, and returns the Response the last event carries.
///
/// The stream starts with `response.created` and `response.in_progress` and ends with one event
/// that ends the Response; its sequence numbers count up from 0; one item is open at a time,
/// at the next output index; each delta extends the open item's last part or its arguments;
/// each `done` event repeats what the deltas built; and the Response's output is the done items.
fn well_formed(events: &[Value]) -> Value {
    for (position, event) in events.iter().enumerate() {
        assert_eq!(
            event["sequence_number"],
            json!(position),
            "sequence numbers count up from 0: {event}"
        );
    }
    let kinds: Vec<&str> = events
        .iter()
        .map(|event| event["type"].as_str().expect("a type"))
        .collect();
    assert_eq!(
        kinds.get(..2),
        Some(&["response.created", "response.in_progress"][..])
    );
    let ends = [
        "response.completed",
        "response.incomplete",
        "response.failed",
    ];
    let (last, between) = events.split_last().expect("events");
    let last_kind = last["type"].as_str().expect("a type");
    assert!(
        ends.contains(&last_kind),
        "the last event ends the Response: {last}"
    );
    assert!(
        between
            .iter()
            .all(|event| !ends.contains(&event["type"].as_str().expect("a type"))),
        "only the last event ends the Response"
    );
    assert_eq!(events[0]["response"]["status"], json!("in_progress"));
    assert_eq!(events[0]["response"]["output"], json!([]));
    assert_eq!(events[1]["response"], events[0]["response"]);

    let mut items: Vec<Value> = Vec::new();
    let mut open: Option<usize> = None;
    let mut done: Vec<Value> = Vec::new();
    for event in &between[2..] {
        let kind = event["type"].as_str().expect("a type");
        if kind == "response.output_item.added" {
            assert_eq!(open, None, "an item starts while another is open: {event}");
            assert_eq!(event["output_index"], json!(items.len()), "{event}");
            let item = event["item"].clone();
            match item["type"].as_str() {
                Some("message") => assert_eq!(
                    (&item["status"], &item["content"]),
                    (&json!("in_progress"), &json!([])),
                    "{event}"
                ),
                Some("function_call") => assert_eq!(
                    (&item["status"], &item["arguments"]),
                    (&json!("in_progress"), &json!("")),
                    "{event}"
                ),
                Some("custom_tool_call") => assert_eq!(item["input"], json!(""), "{event}"),
                other => panic!("an item of an unexpected type {other:?}: {event}"),
            }
            open = Some(items.len());
            items.push(item);
            continue;
        }
        let index = open.unwrap_or_else(|| panic!("no item is open: {event}"));
        assert_eq!(event["output_index"], json!(index), "{event}");
        let item = &mut items[index];
        if kind != "response.output_item.done" {
            assert_eq!(event["item_id"], item["id"], "{event}");
        }
        let append = |value: &mut Value, delta: &Value| {
            let before = value
                .as_str()
                .unwrap_or_else(|| panic!("not text: {value}"));
            *value = json!(format!("{before}{}", delta.as_str().expect("a delta")));
        };
        match kind {
            "response.content_part.added" => {
                let content = item["content"].as_array_mut().expect("a message's content");
                assert_eq!(event["content_index"], json!(content.len()), "{event}");
                assert!(
                    event["part"]
                        == json!({"type": "output_text", "text": "", "annotations": [], "logprobs": []})
                        || event["part"] == json!({"type": "refusal", "refusal": ""}),
                    "a part starts empty: {event}"
                );
                content.push(event["part"].clone());
            }
            "response.output_text.delta"
            | "response.refusal.delta"
            | "response.output_text.done"
            | "response.refusal.done"
            | "response.content_part.done" => {
                let content = item["content"].as_array_mut().expect("a message's content");
                assert_eq!(
                    event["content_index"],
                    json!(content.len() - 1),
                    "it is about the last part: {event}"
                );
                let part = content.last_mut().expect("a part");
                let field = if kind.starts_with("response.refusal") || part["type"] == "refusal" {
                    "refusal"
                } else {
                    "text"
                };
                match kind {
                    "response.output_text.delta" | "response.refusal.delta" => {
                        assert_eq!(
                            part["type"],
                            json!(if field == "refusal" {
                                "refusal"
                            } else {
                                "output_text"
                            }),
                            "{event}"
                        );
                        append(&mut part[field], &event["delta"]);
                    }
                    "response.content_part.done" => assert_eq!(event["part"], *part, "{event}"),
                    _ => assert_eq!(event[field], part[field], "{event}"),
                }
            }
            "response.function_call_arguments.delta" => {
                assert_eq!(item["type"], json!("function_call"), "{event}");
                append(&mut item["arguments"], &event["delta"]);
            }
            "response.function_call_arguments.done" => {
                assert_eq!(
                    (&event["arguments"], &event["name"]),
                    (&item["arguments"], &item["name"]),
                    "{event}"
                );
            }
            "response.custom_tool_call_input.delta" => {
                assert_eq!(item["type"], json!("custom_tool_call"), "{event}");
                append(&mut item["input"], &event["delta"]);
            }
            "response.custom_tool_call_input.done" => {
                assert_eq!(event["input"], item["input"], "{event}");
            }
            "response.output_item.done" => {
                let mut expected = item.clone();
                if expected.get("status").is_some() {
                    assert!(
                        ["completed", "incomplete"]
                            .contains(&event["item"]["status"].as_str().expect("a status")),
                        "{event}"
                    );
                    expected["status"] = event["item"]["status"].clone();
                }
                assert_eq!(
                    event["item"], expected,
                    "the done item is what its events built"
                );
                done.push(event["item"].clone());
                open = None;
            }
            other => panic!("an unexpected event {other}: {event}"),
        }
    }
    if last_kind != "response.failed" {
        assert_eq!(open, None, "an item is left open when the Response ends");
    }
    assert_eq!(
        last["response"]["output"],
        Value::Array(done),
        "the Response's output is the items the events gave"
    );
    last["response"].clone()
}

/// Each event's type, its output index, and the ID of the item it adds.
fn shape(events: &[Value]) -> Vec<Value> {
    let mut normalizer = Normalizer::default();
    events
        .iter()
        .map(|event| {
            let mut shape = vec![event["type"].clone()];
            if let Some(index) = event.get("output_index") {
                shape.push(index.clone());
            }
            if event["type"] == "response.output_item.added" {
                shape.push(normalizer.value(None, &event["item"]["id"]));
            }
            Value::Array(shape)
        })
        .collect()
}

#[tokio::test]
async fn a_text_answer_streams_as_response_events() {
    let events = stream_of(
        vec![
            Scripted::Chunk(chunk(
                &json!({"index": 0, "delta": {"role": "assistant", "content": ""}, "finish_reason": null}),
            )),
            text("Hello"),
            text(", world."),
            finish("stop"),
            usage(),
        ],
        &json!({"model": "gpt", "instructions": "Be brief.", "input": "Say hello."}),
    )
    .await;
    well_formed(&events);
    insta::assert_snapshot!(
        "text_answer_as_response_events",
        pretty(&normalized(&Value::Array(events)))
    );
}

#[tokio::test]
async fn a_tool_call_answer_streams_as_response_events() {
    let events = stream_of(
        vec![
            text("Reading it."),
            call(0, Some("call_1"), Some("read_file"), ""),
            call(0, None, None, "{\"path\":"),
            call(0, None, None, "\"a.rs\"}"),
            call(
                1,
                Some("call_2"),
                Some("multi_agent_v1__spawn_agent"),
                "{\"message\":",
            ),
            call(1, None, None, "\"help\"}"),
            finish("tool_calls"),
            usage(),
        ],
        &json!({"model": "gpt", "input": "Read a.rs, and get help.", "tools": [
            {"type": "function", "name": "read_file"},
            {"type": "namespace", "name": "multi_agent_v1", "description": "Sub-agents.", "tools": [
                {"type": "function", "name": "spawn_agent"},
            ]},
        ]}),
    )
    .await;
    let response = well_formed(&events);
    assert_eq!(
        normalized(&response["output"]),
        json!([
            {"type": "message", "id": "<msg_1>", "role": "assistant", "status": "completed", "content": [
                {"type": "output_text", "text": "Reading it.", "annotations": [], "logprobs": []},
            ]},
            {"type": "function_call", "id": "<fc_1>", "call_id": "call_1", "name": "read_file", "arguments": "{\"path\":\"a.rs\"}", "status": "completed"},
            {"type": "function_call", "id": "<fc_2>", "call_id": "call_2", "namespace": "multi_agent_v1", "name": "spawn_agent", "arguments": "{\"message\":\"help\"}", "status": "completed"},
        ])
    );
    insta::assert_snapshot!(
        "tool_call_answer_as_response_events",
        pretty(&normalized(&Value::Array(events)))
    );
}

/// The streamed and the unstreamed answers are two translations of the same completion, so each
/// checks the other: they carry the same Response.
#[tokio::test]
async fn a_streamed_and_an_unstreamed_answer_carry_the_same_response() {
    let cases = [
        (
            "text",
            json!({"role": "assistant", "content": "Hello, world."}),
            "stop",
            vec![text("Hello"), text(", world."), finish("stop"), usage()],
        ),
        (
            "text and tool calls of each kind",
            json!({"role": "assistant", "content": "Patching.", "tool_calls": [
                {"id": "call_1", "type": "function", "function": {"name": "apply_patch", "arguments": "{\"input\":\"*** Begin Patch\\n*** End Patch\"}"}},
                {"id": "call_2", "type": "function", "function": {"name": "multi_agent_v1__spawn_agent", "arguments": "{}"}},
                {"id": "call_3", "type": "function", "function": {"name": "read_file", "arguments": ""}},
            ]}),
            "tool_calls",
            vec![
                text("Patch"),
                text("ing."),
                call(
                    0,
                    Some("call_1"),
                    Some("apply_patch"),
                    "{\"input\":\"*** Begin",
                ),
                call(0, None, None, " Patch\\n*** End Patch\"}"),
                call(1, Some("call_2"), Some("multi_agent_v1__spawn_agent"), "{}"),
                call(2, Some("call_3"), Some("read_file"), ""),
                finish("tool_calls"),
                usage(),
            ],
        ),
        (
            "text cut off by the token limit",
            json!({"role": "assistant", "content": "Partial"}),
            "length",
            vec![text("Part"), text("ial"), finish("length"), usage()],
        ),
        (
            "a refusal",
            json!({"role": "assistant", "content": null, "refusal": "I can't help with that."}),
            "stop",
            vec![
                refused("I can't "),
                refused("help with that."),
                finish("stop"),
                usage(),
            ],
        ),
    ];
    for (case, message, finish_reason, chunks) in cases {
        let chat = ScriptedChat::new(
            Some(completion(&message, finish_reason, &full_usage())),
            chunks,
        );
        let responses = ChatResponses::new(Arc::clone(&chat) as Arc<dyn Chat>);
        let created = responses
            .create(typed(&agent_request()))
            .await
            .expect("a response");
        let events = streamed(&chat, &agent_request()).await;
        let streamed_response = well_formed(&events);
        assert_eq!(
            normalized(&streamed_response),
            normalized(&json_of(&created)),
            "{case}"
        );

        let requests = chat.requests.lock().expect("requests lock");
        assert_eq!(
            requests
                .iter()
                .map(|request| (
                    request.stream,
                    request
                        .stream_options
                        .as_ref()
                        .and_then(|options| options.include_usage)
                ))
                .collect::<Vec<_>>(),
            [(None, None), (Some(true), Some(true))],
            "{case}: the unstreamed request is not streamed, and the streamed one asks for usage"
        );
    }
}

/// A model may interleave the argument chunks of parallel tool calls; the Responses API streams
/// one item at a time. The call that starts first streams as it arrives; the one that starts
/// while it is not whole is held back, and given once it is.
#[tokio::test]
async fn interleaved_tool_calls_stream_one_item_at_a_time() {
    let events = stream_of(
        vec![
            call(0, Some("call_1"), Some("read_file"), ""),
            call(1, Some("call_2"), Some("read_file"), ""),
            call(0, None, None, "{\"path\":"),
            call(1, None, None, "{\"path\":\"b.rs\"}"),
            call(0, None, None, "\"a.rs\"}"),
            finish("tool_calls"),
        ],
        &json!({"model": "gpt", "input": "Read a.rs and b.rs.", "tools": [{"type": "function", "name": "read_file"}]}),
    )
    .await;
    let response = well_formed(&events);
    assert_eq!(
        shape(&events),
        [
            json!(["response.created"]),
            json!(["response.in_progress"]),
            json!(["response.output_item.added", 0, "<fc_1>"]),
            json!(["response.function_call_arguments.delta", 0]),
            json!(["response.function_call_arguments.delta", 0]),
            json!(["response.function_call_arguments.done", 0]),
            json!(["response.output_item.done", 0]),
            json!(["response.output_item.added", 1, "<fc_2>"]),
            json!(["response.function_call_arguments.delta", 1]),
            json!(["response.function_call_arguments.done", 1]),
            json!(["response.output_item.done", 1]),
            json!(["response.completed"]),
        ],
        "call_1 streams as it arrives, and call_2 follows it whole"
    );
    assert_eq!(
        response["output"]
            .as_array()
            .expect("output")
            .iter()
            .map(|item| json!([item["call_id"], item["arguments"]]))
            .collect::<Vec<_>>(),
        [
            json!(["call_1", "{\"path\":\"a.rs\"}"]),
            json!(["call_2", "{\"path\":\"b.rs\"}"])
        ]
    );
    assert_eq!(response.get("usage"), None, "the chunks reported no usage");
}

/// A function's name is a delta like its arguments: a provider may stream it in pieces, and
/// every event that names the call names the whole of it.
#[tokio::test]
async fn a_function_name_streamed_in_pieces_is_whole_in_the_call() {
    let events = stream_of(
        vec![
            call(0, Some("call_1"), Some("read_"), ""),
            call(0, None, Some("file"), "{\"path\":"),
            call(0, None, None, "\"a.rs\"}"),
            finish("tool_calls"),
        ],
        &json!({"model": "gpt", "input": "Read a.rs.", "tools": [{"type": "function", "name": "read_file"}]}),
    )
    .await;
    let response = well_formed(&events);
    let names: Vec<&Value> = events
        .iter()
        .filter_map(|event| match event["type"].as_str() {
            Some("response.output_item.added" | "response.output_item.done") => {
                Some(&event["item"]["name"])
            }
            Some("response.function_call_arguments.done") => Some(&event["name"]),
            _ => None,
        })
        .chain(
            response["output"]
                .as_array()
                .expect("output")
                .iter()
                .map(|item| &item["name"]),
        )
        .collect();
    assert_eq!(names, [&json!("read_file"); 4], "{events:?}");
    assert_eq!(
        response["output"][0]["arguments"],
        json!("{\"path\":\"a.rs\"}")
    );
}

#[tokio::test]
async fn text_around_tool_calls_is_its_own_message_in_order() {
    let body = json!({"model": "gpt", "input": "Read a.rs.", "tools": [{"type": "function", "name": "read_file"}]});
    let cases = [
        (
            "text after a whole call",
            vec![
                call(0, Some("call_1"), Some("read_file"), "{\"path\":\"a.rs\"}"),
                text("Reading "),
                text("it."),
                finish("tool_calls"),
            ],
            json!([
                ["function_call", "{\"path\":\"a.rs\"}"],
                ["message", "Reading it."]
            ]),
        ),
        (
            "text in the middle of a call's arguments waits for them",
            vec![
                text("First."),
                call(0, Some("call_1"), Some("read_file"), "{\"path\":"),
                text("Reading "),
                text("it."),
                call(0, None, None, "\"a.rs\"}"),
                text(" More."),
                finish("tool_calls"),
            ],
            json!([
                ["message", "First."],
                ["function_call", "{\"path\":\"a.rs\"}"],
                ["message", "Reading it. More."]
            ]),
        ),
        (
            "a call held back behind another, then text",
            vec![
                call(0, Some("call_1"), Some("read_file"), "{\"path\":"),
                call(1, Some("call_2"), Some("read_file"), "{\"path\":\"b.rs\"}"),
                text("Reading both."),
                call(0, None, None, "\"a.rs\"}"),
                finish("tool_calls"),
            ],
            json!([
                ["function_call", "{\"path\":\"a.rs\"}"],
                ["function_call", "{\"path\":\"b.rs\"}"],
                ["message", "Reading both."],
            ]),
        ),
    ];
    for (case, chunks, expected) in cases {
        let response = well_formed(&stream_of(chunks, &body).await);
        let items: Vec<Value> = response["output"]
            .as_array()
            .expect("output")
            .iter()
            .map(|item| match item["type"].as_str() {
                Some("message") => json!(["message", item["content"][0]["text"]]),
                _ => json!([item["type"], item["arguments"]]),
            })
            .collect();
        assert_eq!(Value::Array(items), expected, "{case}");
        assert_eq!(response["status"], json!("completed"), "{case}");
    }
}

#[tokio::test]
async fn a_custom_tool_call_streams_its_input_once_whole() {
    let events = stream_of(
        vec![
            call(
                0,
                Some("call_1"),
                Some("apply_patch"),
                "{\"input\":\"*** Begin",
            ),
            call(0, None, None, " Patch\\n*** End Patch\"}"),
            finish("tool_calls"),
        ],
        &json!({"model": "gpt", "input": "Patch a.rs.", "tools": [
            {"type": "custom", "name": "apply_patch", "format": {"type": "text"}},
        ]}),
    )
    .await;
    well_formed(&events);
    assert_eq!(
        normalized(&Value::Array(events[2..events.len() - 1].to_vec())),
        json!([
            {"type": "response.output_item.added", "sequence_number": 2, "output_index": 0,
             "item": {"type": "custom_tool_call", "id": "<ctc_1>", "call_id": "call_1", "name": "apply_patch", "input": ""}},
            {"type": "response.custom_tool_call_input.delta", "sequence_number": 3, "output_index": 0, "item_id": "<ctc_1>",
             "delta": "*** Begin Patch\n*** End Patch"},
            {"type": "response.custom_tool_call_input.done", "sequence_number": 4, "output_index": 0, "item_id": "<ctc_1>",
             "input": "*** Begin Patch\n*** End Patch"},
            {"type": "response.output_item.done", "sequence_number": 5, "output_index": 0,
             "item": {"type": "custom_tool_call", "id": "<ctc_1>", "call_id": "call_1", "name": "apply_patch", "input": "*** Begin Patch\n*** End Patch"}},
        ])
    );
}

#[tokio::test]
async fn a_refusal_after_text_is_a_second_part_of_the_message() {
    let events = stream_of(
        vec![text("Here is "), refused("not that."), finish("stop")],
        &json!({"model": "gpt", "input": "hi"}),
    )
    .await;
    let response = well_formed(&events);
    assert_eq!(
        response["output"][0]["content"],
        json!([
            {"type": "output_text", "text": "Here is ", "annotations": [], "logprobs": []},
            {"type": "refusal", "refusal": "not that."},
        ])
    );
}

#[tokio::test]
async fn a_stream_cut_off_by_the_token_limit_ends_incomplete() {
    let events = stream_of(
        vec![
            text("Reading."),
            call(0, Some("call_1"), Some("read_file"), "{\"path\":"),
            finish("length"),
            usage(),
        ],
        &json!({"model": "gpt", "input": "Read a.rs.", "tools": [{"type": "function", "name": "read_file"}]}),
    )
    .await;
    let response = well_formed(&events);
    assert_eq!(
        (
            events.last().map(|event| event["type"].clone()),
            &response["status"],
            &response["incomplete_details"],
            response["output"]
                .as_array()
                .expect("output")
                .iter()
                .map(|item| json!([item["type"], item["status"]]))
                .collect::<Vec<_>>(),
        ),
        (
            Some(json!("response.incomplete")),
            &json!("incomplete"),
            &json!({"reason": "max_output_tokens"}),
            vec![
                json!(["message", "completed"]),
                json!(["function_call", "incomplete"])
            ],
        )
    );
}

/// An error from the chat model fails the Response, and nothing follows it.
#[tokio::test]
async fn a_chat_stream_error_fails_the_response_and_ends_the_stream() {
    let events = stream_of(
        vec![
            text("Partial"),
            rate_limited(),
            text("never sent"),
            finish("stop"),
        ],
        &json!({"model": "gpt", "input": "hi"}),
    )
    .await;
    let response = well_formed(&events);
    assert_eq!(
        shape(&events),
        [
            json!(["response.created"]),
            json!(["response.in_progress"]),
            json!(["response.output_item.added", 0, "<msg_1>"]),
            json!(["response.content_part.added", 0]),
            json!(["response.output_text.delta", 0]),
            json!(["response.failed"]),
        ]
    );
    assert_eq!(
        (&response["status"], &response["error"], &response["output"]),
        (
            &json!("failed"),
            &json!({"code": "rate_limit_exceeded", "message": "Rate limit reached"}),
            &json!([])
        )
    );
}

#[tokio::test]
async fn a_stream_the_response_cannot_carry_fails_it() {
    let body = json!({"model": "gpt", "input": "Read a.rs.", "tools": [{"type": "function", "name": "read_file"}]});
    let cases = [
        (
            vec![text("Partial")],
            "Model 'gpt' ended its stream before finishing the response, so the response is incomplete.",
            json!([]),
        ),
        (
            vec![],
            "Model 'gpt' ended its stream before finishing the response, so the response is incomplete.",
            json!([]),
        ),
        (
            vec![call(0, None, None, "{}"), finish("tool_calls")],
            "Model 'gpt' streamed a tool call without the tool's name.",
            json!([]),
        ),
        (
            vec![
                call(0, Some("call_1"), Some("read_file"), "{\"path\":\"a.rs\"}"),
                text("Reading."),
                call(0, None, None, "  "),
                call(0, None, None, "{\"more\":1}"),
                finish("tool_calls"),
            ],
            "Model 'gpt' streamed more arguments for a tool call after they were a whole JSON value, so the call cannot be streamed. Send the request without `stream`.",
            json!(["function_call"]),
        ),
        (
            vec![
                call(0, Some("call_1"), Some("read_file"), "{\"path\":"),
                call(0, None, Some("_v2"), "\"a.rs\"}"),
                finish("tool_calls"),
            ],
            "Model 'gpt' streamed more of a tool call's name after the call started, so the call cannot be streamed. Send the request without `stream`.",
            json!([]),
        ),
    ];
    for (chunks, message, done_items) in cases {
        let events = stream_of(chunks, &body).await;
        let response = well_formed(&events);
        assert_eq!(
            (
                &response["status"],
                &response["error"],
                response["output"]
                    .as_array()
                    .expect("output")
                    .iter()
                    .map(|item| item["type"].clone())
                    .collect::<Vec<_>>()
            ),
            (
                &json!("failed"),
                &json!({"code": "server_error", "message": message}),
                done_items.as_array().expect("types").clone()
            ),
            "{message}"
        );
    }
}

/// `response_stream` drives the translation of any chunk stream, not only a model's.
#[tokio::test]
async fn a_chunk_stream_without_choices_still_completes() {
    let chunks: ChatCompletionResponseStream = Box::pin(futures::stream::iter(vec![Ok(chunk(
        &json!({"index": 0, "delta": {}, "finish_reason": "stop"}),
    ))]));
    let events: Vec<Value> =
        response_stream(chunks, context(&json!({"model": "gpt", "input": "hi"})))
            .map(|event| json_of(&event.expect("an event")))
            .collect()
            .await;
    let response = well_formed(&events);
    assert_eq!(
        (&response["status"], &response["output"]),
        (&json!("completed"), &json!([]))
    );
}
