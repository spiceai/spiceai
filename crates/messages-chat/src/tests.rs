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

use std::sync::{Arc, Mutex};

use async_openai::error::{ApiError, OpenAIError};
use async_openai::types::chat::{
    ChatCompletionResponseStream, CreateChatCompletionRequest, CreateChatCompletionResponse,
    CreateChatCompletionStreamResponse,
};
use async_trait::async_trait;
use chat_api::Chat;
use chat_api::SqlGeneration;
use futures::StreamExt;
use messages_api::stream::StreamEvent;
use messages_api::{ErrorType, Messages, MessagesError, MessagesRequest};
use serde_json::{Value, json};

use crate::ChatMessages;
use crate::request::chat_request;
use crate::response::{message, messages_error};
use crate::stream::{Translator, message_stream};

fn typed(body: &Value) -> messages_api::types::CreateMessageRequest {
    serde_json::from_value(body.clone()).expect("a valid Messages API request")
}

fn translated(body: &Value) -> Result<Value, MessagesError> {
    chat_request(typed(body)).map(|request| serde_json::to_value(request).expect("serializable"))
}

fn pretty(value: &Value) -> String {
    serde_json::to_string_pretty(value).expect("serializable")
}

/// A request shaped like the ones a coding agent sends: a system prompt in blocks with cache
/// breakpoints, tools, adaptive thinking with an effort, and a history with thinking, a tool
/// call, and a tool result that returned an image.
fn agent_request() -> Value {
    json!({
        "model": "gpt",
        "max_tokens": 32000,
        "stream": true,
        "system": [
            {"type": "text", "text": "You are a coding agent."},
            {"type": "text", "text": "Be brief.", "cache_control": {"type": "ephemeral"}},
        ],
        "tools": [{
            "name": "read_file",
            "description": "Reads a file.",
            "input_schema": {"type": "object", "properties": {"path": {"type": "string"}}, "required": ["path"]},
            "cache_control": {"type": "ephemeral"},
        }],
        "tool_choice": {"type": "auto", "disable_parallel_tool_use": true},
        "thinking": {"type": "adaptive", "display": "summarized"},
        "output_config": {"effort": "high"},
        "metadata": {"user_id": "user-1"},
        "temperature": 0.5,
        "stop_sequences": ["END"],
        "cache_control": {"type": "ephemeral"},
        "context_management": {"edits": [{"type": "clear_tool_uses_20250919"}]},
        "messages": [
            {"role": "user", "content": "Show me the logo."},
            {"role": "assistant", "content": [
                {"type": "thinking", "thinking": "I should read it.", "signature": "sig"},
                {"type": "text", "text": "Reading it."},
                {"type": "tool_use", "id": "toolu_1", "name": "read_file", "input": {"path": "logo.png"}},
            ]},
            {"role": "user", "content": [
                {"type": "tool_result", "tool_use_id": "toolu_1", "content": [
                    {"type": "text", "text": "logo.png (2 KB)"},
                    {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": "iVBOR"}},
                ]},
                {"type": "text", "text": "Describe it.", "cache_control": {"type": "ephemeral"}},
            ]},
        ],
    })
}

#[test]
fn an_agent_request_translates_to_a_chat_completion_request() {
    let chat = translated(&agent_request()).expect("a translatable request");
    insta::assert_snapshot!("agent_request_as_chat_completion", pretty(&chat));
}

#[test]
fn what_only_changes_how_a_request_is_served_is_accepted() {
    for field in [
        json!({"cache_control": {"type": "ephemeral"}}),
        json!({"service_tier": "standard_only"}),
        json!({"speed": "fast"}),
        json!({"context_management": {"edits": []}}),
        json!({"fallbacks": "default"}),
        json!({"diagnostics": {"previous_message_id": null}}),
        json!({"thinking": {"type": "adaptive", "display": "omitted"}}),
        json!({"output_config": {"effort": "high"}}),
    ] {
        let mut body = json!({"model": "gpt", "max_tokens": 10, "messages": [{"role": "user", "content": "hi"}]});
        for (key, value) in field.as_object().expect("an object") {
            body[key] = value.clone();
        }
        let chat = translated(&body).unwrap_or_else(|e| panic!("{field} refused: {e}"));
        assert_eq!(
            chat,
            json!({"model": "gpt", "messages": [{"role": "user", "content": "hi"}], "max_completion_tokens": 10}),
            "{field}"
        );
    }
}

#[test]
fn what_a_chat_completion_cannot_carry_is_refused_by_name() {
    let base = || json!({"model": "gpt", "max_tokens": 10, "messages": [{"role": "user", "content": "hi"}]});
    let with = |key: &str, value: Value| {
        let mut body = base();
        body[key] = value;
        body
    };
    let message = |content: Value| {
        let mut body = base();
        body["messages"] = content;
        body
    };
    let cases = [
        (
            with("top_k", json!(5)),
            "cannot carry `top_k`. Remove `top_k`",
        ),
        (
            with("container", json!("container_1")),
            "cannot carry `container`. Remove `container`",
        ),
        (
            with("mcp_servers", json!([])),
            "cannot carry `mcp_servers`.",
        ),
        (
            with("inference_geo", json!("us")),
            "cannot carry `inference_geo`.",
        ),
        (
            with(
                "tools",
                json!([{"type": "web_search_20250305", "name": "web_search"}]),
            ),
            "cannot carry the Anthropic-defined tool `web_search_20250305`.",
        ),
        (
            with("output_config", json!({"format": {"type": "json_object"}})),
            "cannot carry the output format `json_object` without a `schema`.",
        ),
        (
            message(
                json!([{"role": "user", "content": [{"type": "document", "source": {"type": "text", "data": "d"}, "citations": {"enabled": true}}]}]),
            ),
            "cannot carry citations.",
        ),
        (
            message(
                json!([{"role": "user", "content": [{"type": "image", "source": {"type": "file", "file_id": "file_1"}}]}]),
            ),
            "cannot carry an image from the Anthropic Files API.",
        ),
        (
            message(
                json!([{"role": "user", "content": [{"type": "search_result", "source": "s", "title": "t", "content": []}]}]),
            ),
            "cannot carry a `search_result` block in a user message.",
        ),
        (
            message(
                json!([{"role": "user", "content": "q"}, {"role": "assistant", "content": [{"type": "server_tool_use", "id": "srvtoolu_1", "name": "web_search", "input": {}}]}]),
            ),
            "cannot carry a `server_tool_use` block in an assistant message.",
        ),
        (
            message(
                json!([{"role": "user", "content": "q"}, {"role": "assistant", "content": [{"type": "mcp_tool_use", "id": "mcptoolu_1", "name": "f", "server_name": "github", "input": {}}]}]),
            ),
            "cannot carry a `mcp_tool_use` block in an assistant message.",
        ),
    ];
    for (body, expected) in cases {
        let error = translated(&body).expect_err("a refused request");
        assert_eq!(
            (error.status, &error.error_type),
            (400, &ErrorType::InvalidRequest),
            "{body}"
        );
        assert!(
            error.message.starts_with("Model 'gpt' serves the Messages API by translating requests into chat completions, which cannot carry")
                && error.message.contains(expected)
                && error.message.ends_with("See: https://spiceai.org/docs/components/models"),
            "{}\nexpected it to contain: {expected}",
            error.message
        );
    }
}

/// A tool's `cache_control` and `eager_input_streaming` only change how a request is served, so
/// the tool is translated without them; any other field of a tool, such as `defer_loading`, is
/// refused by name, since dropping it would offer the model a tool the client meant otherwise.
#[test]
fn a_tools_serving_fields_are_accepted_and_its_other_fields_refused_by_name() {
    let body = |tool: Value| json!({"model": "gpt", "max_tokens": 10, "messages": [{"role": "user", "content": "hi"}], "tools": [tool]});
    let chat = translated(&body(json!({
        "type": "custom", "name": "read_file", "description": "Reads a file.",
        "input_schema": {"type": "object"}, "strict": true,
        "cache_control": {"type": "ephemeral"}, "eager_input_streaming": true,
    })))
    .expect("a translated request");
    assert_eq!(
        chat["tools"],
        json!([{"type": "function", "function": {"name": "read_file", "description": "Reads a file.", "parameters": {"type": "object"}, "strict": true}}])
    );
    for (field, value) in [
        ("defer_loading", json!(true)),
        ("input_examples", json!([{"path": "a.rs"}])),
        ("allowed_callers", json!(["code_execution_20250825"])),
    ] {
        let mut tool = json!({"name": "read_file", "input_schema": {"type": "object"}});
        tool[field] = value;
        let error = translated(&body(tool)).expect_err("a refused tool");
        assert_eq!(
            (error.status, &error.error_type, error.message.as_str()),
            (
                400,
                &ErrorType::InvalidRequest,
                format!("Model 'gpt' serves the Messages API by translating requests into chat completions, which cannot carry `{field}` on the tool 'read_file'. Remove `{field}`, or use a model with `from: anthropic`. See: https://spiceai.org/docs/components/models").as_str()
            )
        );
    }
}

#[test]
fn a_request_that_is_not_a_messages_request_is_an_invalid_request() {
    let error = crate::parse(MessagesRequest::new(
        json!({"model": "gpt", "messages": []})
            .as_object()
            .cloned()
            .expect("an object"),
    ))
    .expect_err("max_tokens is required");
    assert_eq!(
        error,
        MessagesError::invalid_request("missing field `max_tokens`")
    );
}

fn completion(message: &Value, finish_reason: &str) -> CreateChatCompletionResponse {
    serde_json::from_value(json!({
        "id": "chatcmpl-abc",
        "object": "chat.completion",
        "created": 0,
        "model": "gpt",
        "choices": [{"index": 0, "message": message, "finish_reason": finish_reason}],
        "usage": {"prompt_tokens": 120, "completion_tokens": 30, "total_tokens": 150, "prompt_tokens_details": {"cached_tokens": 100}},
    }))
    .expect("a chat completion")
}

#[test]
fn a_completion_becomes_a_message() {
    let answer = completion(
        &json!({
            "role": "assistant",
            "content": "Reading it.",
            "reasoning_content": "The user wants the file.",
            "tool_calls": [
                {"id": "call_1", "type": "function", "function": {"name": "read_file", "arguments": "{\"path\":\"a.rs\"}"}},
                {"id": "call_2", "type": "function", "function": {"name": "list_files", "arguments": ""}},
            ],
        }),
        "tool_calls",
    );
    assert_eq!(
        serde_json::to_value(message(answer.clone(), true).expect("a message"))
            .expect("serializable"),
        json!({
            "id": "msg_abc",
            "type": "message",
            "role": "assistant",
            "model": "gpt",
            "content": [
                {"type": "thinking", "thinking": "The user wants the file.", "signature": ""},
                {"type": "text", "text": "Reading it."},
                {"type": "tool_use", "id": "call_1", "name": "read_file", "input": {"path": "a.rs"}},
                {"type": "tool_use", "id": "call_2", "name": "list_files", "input": {}},
            ],
            "stop_reason": "tool_use",
            "stop_sequence": null,
            "usage": {"input_tokens": 20, "output_tokens": 30, "cache_read_input_tokens": 100},
        })
    );
    let without_thinking = message(answer, false).expect("a message");
    assert_eq!(
        without_thinking.content.first(),
        Some(&messages_api::types::ContentBlock::Text {
            text: "Reading it.".to_string()
        })
    );
}

#[test]
fn finish_reasons_become_stop_reasons() {
    let stop = |finish: &str, message: Value| {
        serde_json::to_value(
            crate::response::message(completion(&message, finish), false).expect("a message"),
        )
        .expect("serializable")["stop_reason"]
            .clone()
    };
    let text = json!({"role": "assistant", "content": "hi"});
    assert_eq!(
        [
            stop("stop", text.clone()),
            stop("length", text.clone()),
            stop("content_filter", text),
            stop(
                "stop",
                json!({"role": "assistant", "content": null, "refusal": "I can't help with that."})
            ),
        ],
        [
            json!("end_turn"),
            json!("max_tokens"),
            json!("refusal"),
            json!("refusal")
        ]
    );
}

#[test]
fn tool_arguments_that_are_not_an_object_are_an_api_error() {
    let answer = completion(
        &json!({"role": "assistant", "content": null, "tool_calls": [
            {"id": "call_1", "type": "function", "function": {"name": "read_file", "arguments": "{\"path\": "}},
        ]}),
        "tool_calls",
    );
    assert_eq!(
        message(answer, false).expect_err("truncated arguments"),
        MessagesError::api(
            "Model 'gpt' called the tool 'read_file' with arguments that are not a JSON object, which the Messages API cannot return: {\"path\": "
        )
    );
}

fn chunk(choice: &Value) -> CreateChatCompletionStreamResponse {
    serde_json::from_value(json!({
        "id": "chatcmpl-abc", "object": "chat.completion.chunk", "created": 0, "model": "gpt",
        "choices": [choice],
    }))
    .expect("a chunk")
}

fn usage_chunk() -> CreateChatCompletionStreamResponse {
    serde_json::from_value(json!({
        "id": "chatcmpl-abc", "object": "chat.completion.chunk", "created": 0, "model": "gpt",
        "choices": [],
        "usage": {"prompt_tokens": 120, "completion_tokens": 30, "total_tokens": 150, "prompt_tokens_details": {"cached_tokens": 100}},
    }))
    .expect("a usage chunk")
}

fn tool_call(index: u32, id: Option<&str>, name: Option<&str>, arguments: &str) -> Value {
    let mut function = json!({"arguments": arguments});
    if let Some(name) = name {
        function["name"] = json!(name);
    }
    let mut call = json!({"index": index, "function": function});
    if let Some(id) = id {
        call["id"] = json!(id);
        call["type"] = json!("function");
    }
    json!({"index": 0, "delta": {"tool_calls": [call]}, "finish_reason": null})
}

async fn events(
    chunks: Vec<Result<CreateChatCompletionStreamResponse, OpenAIError>>,
) -> Vec<Value> {
    let chunks: ChatCompletionResponseStream = Box::pin(futures::stream::iter(chunks));
    message_stream(chunks, "gpt".to_string())
        .map(|event| event.unwrap_or_else(|error| error.to_envelope()))
        .collect()
        .await
}

#[tokio::test]
async fn a_chunk_stream_becomes_a_message_event_stream() {
    let streamed = events(vec![
        Ok(chunk(&json!({"index": 0, "delta": {"role": "assistant", "content": ""}, "finish_reason": null}))),
        Ok(chunk(&json!({"index": 0, "delta": {"content": "Reading "}, "finish_reason": null}))),
        Ok(chunk(&json!({"index": 0, "delta": {"content": "both."}, "finish_reason": null}))),
        Ok(chunk(&tool_call(0, Some("call_1"), Some("read_file"), ""))),
        Ok(chunk(&tool_call(0, None, None, "{\"path\":"))),
        Ok(chunk(&tool_call(0, None, None, "\"a.rs\"}"))),
        Ok(chunk(&tool_call(1, Some("call_2"), Some("read_file"), "{\"path\":\"b.rs\"}"))),
        Ok(chunk(&json!({"index": 0, "delta": {}, "finish_reason": "tool_calls"}))),
        Ok(usage_chunk()),
    ])
    .await;
    insta::assert_snapshot!(
        "chunk_stream_as_message_events",
        pretty(&Value::Array(streamed))
    );
}

/// A chunk stream that ends before the completion finishes is a message cut short: its end is an
/// error, never a `message_stop`, which would present the partial message as whole.
#[tokio::test]
async fn a_chunk_stream_that_ends_unfinished_ends_with_an_error() {
    let incomplete = json!({"type": "error", "error": {"type": "api_error", "message": "Model 'gpt' ended its stream before finishing the message, so the message is incomplete."}});
    let partial = events(vec![Ok(chunk(
        &json!({"index": 0, "delta": {"content": "Partial"}, "finish_reason": null}),
    ))])
    .await;
    assert_eq!(
        partial
            .iter()
            .map(|event| event["type"].clone())
            .collect::<Vec<_>>(),
        [
            json!("message_start"),
            json!("content_block_start"),
            json!("content_block_delta"),
            json!("error")
        ]
    );
    assert_eq!(partial.last(), Some(&incomplete));
    assert_eq!(events(vec![]).await, [incomplete]);
}

/// The (type, index) of each event, and the `id` of each block it starts.
fn shape(events: &[Value]) -> Vec<Value> {
    events
        .iter()
        .map(|event| {
            let mut shape = json!([event["type"], event["index"]]);
            if let Some(id) = event["content_block"]["id"].as_str() {
                shape = json!([event["type"], event["index"], id]);
            }
            shape
        })
        .collect()
}

/// A model may interleave the argument chunks of parallel tool calls, and the Messages API streams
/// one block at a time: a call that starts before the streaming call's arguments are whole is held
/// back and given whole after it, so the stream carries the message the unstreamed answer does.
#[tokio::test]
async fn interleaved_tool_calls_stream_the_message_the_unstreamed_answer_carries() {
    let chat = Arc::new(ScriptedChat {
        requests: Mutex::new(Vec::new()),
        answer: completion(
            &json!({"role": "assistant", "content": null, "tool_calls": [
                {"id": "call_1", "type": "function", "function": {"name": "read_file", "arguments": "{\"path\":\"a.rs\"}"}},
                {"id": "call_2", "type": "function", "function": {"name": "read_file", "arguments": "{\"path\":\"b.rs\"}"}},
            ]}),
            "tool_calls",
        ),
        chunks: vec![
            chunk(&tool_call(0, Some("call_1"), Some("read_file"), "")),
            chunk(&tool_call(1, Some("call_2"), Some("read_file"), "")),
            chunk(&tool_call(0, None, None, "{\"path\":")),
            chunk(&tool_call(1, None, None, "{\"path\":\"b.rs\"}")),
            chunk(&tool_call(0, None, None, "\"a.rs\"}")),
            chunk(&json!({"index": 0, "delta": {}, "finish_reason": "tool_calls"})),
            usage_chunk(),
        ],
    });
    let messages = ChatMessages::new(Arc::clone(&chat) as Arc<dyn Chat>);
    let body = json!({"model": "gpt", "max_tokens": 100, "messages": [{"role": "user", "content": "Read a.rs and b.rs"}]});

    let created = messages
        .create(messages_request(&body))
        .await
        .expect("a message");
    let streamed: Vec<Value> = messages
        .create_stream(messages_request(&body))
        .await
        .expect("a stream")
        .map(|event| event.expect("an event"))
        .collect()
        .await;

    assert_eq!(accumulate(&streamed), created);
    assert_eq!(
        shape(&streamed),
        [
            json!(["message_start", null]),
            json!(["content_block_start", 0, "call_1"]),
            json!(["content_block_delta", 0]),
            json!(["content_block_delta", 0]),
            json!(["content_block_stop", 0]),
            json!(["content_block_start", 1, "call_2"]),
            json!(["content_block_delta", 1]),
            json!(["content_block_stop", 1]),
            json!(["message_delta", null]),
            json!(["message_stop", null]),
        ],
        "call_1 streams as it arrives, and call_2 follows it whole"
    );
}

/// Parallel tool calls streamed one after another, as `OpenAI` streams them, each stream as they
/// arrive: a call that starts once the one before it is whole ends that one's block.
#[test]
fn a_tool_call_that_starts_after_a_whole_one_streams_as_it_arrives() {
    let mut translator = Translator::new("gpt".to_string());
    let values = |events: Result<Vec<StreamEvent>, MessagesError>| -> Vec<Value> {
        events
            .expect("events")
            .iter()
            .map(|event| serde_json::to_value(event).expect("an event"))
            .collect()
    };
    values(translator.chunk(chunk(&tool_call(
        0,
        Some("call_1"),
        Some("read_file"),
        "{\"path\":\"a.rs\"}",
    ))));
    assert_eq!(
        values(translator.chunk(chunk(&tool_call(
            1,
            Some("call_2"),
            Some("read_file"),
            "{\"path\":",
        )))),
        [
            json!({"type": "content_block_stop", "index": 0}),
            json!({"type": "content_block_start", "index": 1, "content_block": {"type": "tool_use", "id": "call_2", "name": "read_file", "input": {}}}),
            json!({"type": "content_block_delta", "index": 1, "delta": {"type": "input_json_delta", "partial_json": "{\"path\":"}}),
        ]
    );
    assert_eq!(
        values(translator.chunk(chunk(&tool_call(1, None, None, "\"b.rs\"}")))),
        [
            json!({"type": "content_block_delta", "index": 1, "delta": {"type": "input_json_delta", "partial_json": "\"b.rs\"}"}}),
        ]
    );
}

/// Tool calls held back are given before text that follows them, keeping the order the model
/// wrote its blocks in.
#[tokio::test]
async fn held_back_tool_calls_come_before_the_text_after_them() {
    let streamed = events(vec![
        Ok(chunk(&tool_call(0, Some("call_1"), Some("read_file"), ""))),
        Ok(chunk(&tool_call(
            1,
            Some("call_2"),
            Some("read_file"),
            "{\"path\":\"b.rs\"}",
        ))),
        Ok(chunk(&tool_call(0, None, None, "{\"path\":\"a.rs\"}"))),
        Ok(chunk(
            &json!({"index": 0, "delta": {"content": "Reading both."}, "finish_reason": null}),
        )),
        Ok(chunk(
            &json!({"index": 0, "delta": {}, "finish_reason": "tool_calls"}),
        )),
    ])
    .await;
    assert_eq!(
        accumulate(&streamed)["content"],
        json!([
            {"type": "tool_use", "id": "call_1", "name": "read_file", "input": {"path": "a.rs"}},
            {"type": "tool_use", "id": "call_2", "name": "read_file", "input": {"path": "b.rs"}},
            {"type": "text", "text": "Reading both."},
        ])
    );
}

/// Text ends the open tool call's block, which then has to be whole: text in the middle of a
/// call's arguments ends the stream with an error, and so does more of a call after its block
/// ended, since its input was already given whole.
#[tokio::test]
async fn a_tool_call_that_text_ended_is_checked_and_cannot_continue() {
    let reading = || {
        Ok(chunk(
            &json!({"index": 0, "delta": {"content": "Reading."}, "finish_reason": null}),
        ))
    };
    let finish = || {
        Ok(chunk(
            &json!({"index": 0, "delta": {}, "finish_reason": "tool_calls"}),
        ))
    };
    let cases = [
        (
            vec![
                Ok(chunk(&tool_call(
                    0,
                    Some("call_1"),
                    Some("read_file"),
                    "{\"path\":",
                ))),
                reading(),
                Ok(chunk(&tool_call(0, None, None, "\"a.rs\"}"))),
                finish(),
            ],
            "Model 'gpt' called the tool 'read_file' with arguments that are not a JSON object, which the Messages API cannot return: {\"path\":",
        ),
        (
            vec![
                Ok(chunk(&tool_call(
                    0,
                    Some("call_1"),
                    Some("read_file"),
                    "{\"path\":\"a.rs\"}",
                ))),
                reading(),
                Ok(chunk(&tool_call(0, None, None, "{}"))),
                finish(),
            ],
            "Model 'gpt' streamed more of a tool call after its content block had ended, which the Messages API cannot stream: it streams each content block whole before the next. Send the request without `stream`.",
        ),
    ];
    for (chunks, message) in cases {
        let streamed = events(chunks).await;
        assert_eq!(
            streamed.last(),
            Some(&json!({"type": "error", "error": {"type": "api_error", "message": message}}))
        );
        assert_eq!(
            streamed
                .iter()
                .filter(|event| event["type"] == "message_stop")
                .count(),
            0,
            "{message}: an errored stream does not also end as a whole message"
        );
    }
}

#[tokio::test]
async fn a_chunk_stream_error_ends_the_stream() {
    let streamed = events(vec![
        Ok(chunk(
            &json!({"index": 0, "delta": {"content": "Partial"}, "finish_reason": null}),
        )),
        Err(OpenAIError::ApiError(ApiError {
            message: "Rate limit reached".to_string(),
            r#type: Some("requests".to_string()),
            param: None,
            code: Some("rate_limit_exceeded".to_string()),
        })),
        Ok(chunk(
            &json!({"index": 0, "delta": {"content": "never sent"}, "finish_reason": null}),
        )),
    ])
    .await;
    assert_eq!(
        streamed
            .iter()
            .map(|event| event["type"].clone())
            .collect::<Vec<_>>(),
        [
            json!("message_start"),
            json!("content_block_start"),
            json!("content_block_delta"),
            json!("error")
        ]
    );
    assert_eq!(
        streamed.last(),
        Some(
            &json!({"type": "error", "error": {"type": "rate_limit_error", "message": "Rate limit reached"}})
        )
    );
}

#[test]
fn chat_completion_errors_keep_their_kind() {
    let api_error = |code: &str| {
        messages_error(OpenAIError::ApiError(ApiError {
            message: "m".to_string(),
            r#type: None,
            param: None,
            code: Some(code.to_string()),
        }))
    };
    assert_eq!(
        [
            api_error("invalid_request_error"),
            api_error("invalid_api_key"),
            api_error("permission_error"),
            api_error("not_found_error"),
            api_error("rate_limit_exceeded"),
            api_error("insufficient_quota"),
            api_error("server_is_on_fire"),
        ]
        .map(|error| (error.status, error.error_type.as_str().to_string())),
        [
            (400, "invalid_request_error".to_string()),
            (401, "authentication_error".to_string()),
            (403, "permission_error".to_string()),
            (404, "not_found_error".to_string()),
            (429, "rate_limit_error".to_string()),
            (402, "billing_error".to_string()),
            (500, "api_error".to_string()),
        ]
    );
}

/// A chat model that records each request and answers with `answer`, or streams `chunks`.
struct ScriptedChat {
    requests: Mutex<Vec<CreateChatCompletionRequest>>,
    answer: CreateChatCompletionResponse,
    chunks: Vec<CreateChatCompletionStreamResponse>,
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
        Ok(self.answer.clone())
    }

    async fn chat_stream(
        &self,
        req: CreateChatCompletionRequest,
    ) -> Result<ChatCompletionResponseStream, OpenAIError> {
        self.requests.lock().expect("requests lock").push(req);
        Ok(Box::pin(futures::stream::iter(
            self.chunks.clone().into_iter().map(Ok),
        )))
    }
}

fn messages_request(body: &Value) -> MessagesRequest {
    MessagesRequest::new(body.as_object().cloned().expect("an object"))
}

#[tokio::test]
async fn a_streamed_and_an_unstreamed_answer_carry_the_same_message() {
    let chat = Arc::new(ScriptedChat {
        requests: Mutex::new(Vec::new()),
        answer: completion(
            &json!({"role": "assistant", "content": "Reading it.", "tool_calls": [
                {"id": "call_1", "type": "function", "function": {"name": "read_file", "arguments": "{\"path\":\"a.rs\"}"}},
            ]}),
            "tool_calls",
        ),
        chunks: vec![
            chunk(
                &json!({"index": 0, "delta": {"role": "assistant", "content": "Reading "}, "finish_reason": null}),
            ),
            chunk(&json!({"index": 0, "delta": {"content": "it."}, "finish_reason": null})),
            chunk(&tool_call(
                0,
                Some("call_1"),
                Some("read_file"),
                "{\"path\":",
            )),
            chunk(&tool_call(0, None, None, "\"a.rs\"}")),
            chunk(&json!({"index": 0, "delta": {}, "finish_reason": "tool_calls"})),
            usage_chunk(),
        ],
    });
    let messages = ChatMessages::new(Arc::clone(&chat) as Arc<dyn Chat>);
    let body = json!({"model": "gpt", "max_tokens": 100, "messages": [{"role": "user", "content": "Read a.rs"}]});

    let created = messages
        .create(messages_request(&body))
        .await
        .expect("a message");
    let streamed: Vec<Value> = messages
        .create_stream(messages_request(&body))
        .await
        .expect("a stream")
        .map(|event| event.expect("an event"))
        .collect()
        .await;

    assert_eq!(accumulate(&streamed), created);
    let requests = chat.requests.lock().expect("requests lock");
    assert_eq!(
        requests
            .iter()
            .map(|request| (request.stream, request.stream_options.is_some()))
            .collect::<Vec<_>>(),
        [(None, false), (Some(true), true)],
        "the unstreamed request is not marked as streaming, and the streamed one asks for usage"
    );
}

/// The unstreamed and the streamed answer of a model that answers with `answer`, or streams
/// `chunks`; a stream's error is its last event, as its envelope.
async fn both_answers(
    answer: CreateChatCompletionResponse,
    chunks: Vec<CreateChatCompletionStreamResponse>,
) -> (Result<Value, MessagesError>, Vec<Value>) {
    let chat = Arc::new(ScriptedChat {
        requests: Mutex::new(Vec::new()),
        answer,
        chunks,
    });
    let messages = ChatMessages::new(Arc::clone(&chat) as Arc<dyn Chat>);
    let body = json!({"model": "gpt", "max_tokens": 100, "messages": [{"role": "user", "content": "Read a.rs"}]});
    let created = messages.create(messages_request(&body)).await;
    let streamed = messages
        .create_stream(messages_request(&body))
        .await
        .expect("a stream")
        .map(|event| event.unwrap_or_else(|error| error.to_envelope()))
        .collect()
        .await;
    (created, streamed)
}

/// A tool call whose arguments are not a JSON object cannot be a `tool_use` block: the streamed
/// answer ends with the error the unstreamed one is, whether the call streamed as it arrived or
/// was held back behind another.
#[tokio::test]
async fn a_streamed_tool_call_whose_input_is_not_an_object_ends_with_the_unstreamed_error() {
    let not_an_object = |id: &str| json!({"id": id, "type": "function", "function": {"name": "read_file", "arguments": "[]"}});
    let cases = [
        (
            json!([not_an_object("call_1")]),
            vec![
                chunk(&tool_call(0, Some("call_1"), Some("read_file"), "[]")),
                chunk(&json!({"index": 0, "delta": {}, "finish_reason": "tool_calls"})),
            ],
        ),
        (
            json!([
                {"id": "call_1", "type": "function", "function": {"name": "read_file", "arguments": "{}"}},
                not_an_object("call_2"),
            ]),
            vec![
                chunk(&tool_call(0, Some("call_1"), Some("read_file"), "")),
                chunk(&tool_call(1, Some("call_2"), Some("read_file"), "[]")),
                chunk(&tool_call(0, None, None, "{}")),
                chunk(&json!({"index": 0, "delta": {}, "finish_reason": "tool_calls"})),
            ],
        ),
    ];
    for (tool_calls, chunks) in cases {
        let (created, streamed) = both_answers(
            completion(
                &json!({"role": "assistant", "content": null, "tool_calls": tool_calls}),
                "tool_calls",
            ),
            chunks,
        )
        .await;
        let error = created.expect_err("an unstreamed error").to_envelope();
        assert_eq!(
            error,
            json!({"type": "error", "error": {"type": "api_error", "message": "Model 'gpt' called the tool 'read_file' with arguments that are not a JSON object, which the Messages API cannot return: []"}})
        );
        assert_eq!(streamed.last(), Some(&error), "{tool_calls}");
        assert!(
            !streamed.iter().any(|event| event["type"] == "message_stop"),
            "{tool_calls}: an errored stream does not also end as a whole message"
        );
    }
}

/// A completion cut off by `max_tokens` in the middle of a held-back call's arguments leaves the
/// call out, as the unstreamed answer does, and ends with `max_tokens`.
#[tokio::test]
async fn a_held_back_tool_call_cut_off_by_max_tokens_is_left_out() {
    let (created, streamed) = both_answers(
        completion(
            &json!({"role": "assistant", "content": null, "tool_calls": [
                {"id": "call_1", "type": "function", "function": {"name": "read_file", "arguments": "{\"path\":\"a.rs\"}"}},
                {"id": "call_2", "type": "function", "function": {"name": "read_file", "arguments": "{\"path\":"}},
            ]}),
            "length",
        ),
        vec![
            chunk(&tool_call(0, Some("call_1"), Some("read_file"), "")),
            chunk(&tool_call(1, Some("call_2"), Some("read_file"), "{\"path\":")),
            chunk(&tool_call(0, None, None, "{\"path\":\"a.rs\"}")),
            chunk(&json!({"index": 0, "delta": {}, "finish_reason": "length"})),
            usage_chunk(),
        ],
    )
    .await;
    let created = created.expect("an unstreamed message");
    assert_eq!(accumulate(&streamed), created);
    assert_eq!(
        (&created["stop_reason"], &created["content"]),
        (
            &json!("max_tokens"),
            &json!([{"type": "tool_use", "id": "call_1", "name": "read_file", "input": {"path": "a.rs"}}])
        )
    );
}

/// The Message a client builds from `events`, as the Anthropic SDKs accumulate a stream.
fn accumulate(events: &[Value]) -> Value {
    let mut message = Value::Null;
    let mut partial_json: Vec<String> = Vec::new();
    for event in events {
        match event["type"].as_str().expect("a typed event") {
            "message_start" => message = event["message"].clone(),
            "content_block_start" => {
                message["content"]
                    .as_array_mut()
                    .expect("content")
                    .push(event["content_block"].clone());
                partial_json.push(String::new());
            }
            "content_block_delta" => {
                let index =
                    usize::try_from(event["index"].as_u64().expect("an index")).expect("an index");
                let block = &mut message["content"][index];
                let delta = &event["delta"];
                match delta["type"].as_str().expect("a typed delta") {
                    "text_delta" => {
                        let text = format!(
                            "{}{}",
                            block["text"].as_str().unwrap_or_default(),
                            delta["text"].as_str().unwrap_or_default()
                        );
                        block["text"] = json!(text);
                    }
                    "input_json_delta" => partial_json[index]
                        .push_str(delta["partial_json"].as_str().unwrap_or_default()),
                    other => panic!("unexpected delta {other}"),
                }
            }
            "content_block_stop" => {
                let index =
                    usize::try_from(event["index"].as_u64().expect("an index")).expect("an index");
                if message["content"][index]["type"] == "tool_use"
                    && !partial_json[index].is_empty()
                {
                    message["content"][index]["input"] =
                        serde_json::from_str(&partial_json[index]).expect("whole tool input");
                }
            }
            "message_delta" => {
                message["stop_reason"] = event["delta"]["stop_reason"].clone();
                message["stop_sequence"] = event["delta"]["stop_sequence"].clone();
                for (key, value) in event["usage"].as_object().expect("usage") {
                    message["usage"][key] = value.clone();
                }
            }
            "message_stop" | "ping" => {}
            other => panic!("unexpected event {other}"),
        }
    }
    message
}

#[tokio::test]
async fn only_a_model_served_by_anthropic_counts_tokens() {
    let chat = Arc::new(ScriptedChat {
        requests: Mutex::new(Vec::new()),
        answer: completion(&json!({"role": "assistant", "content": "hi"}), "stop"),
        chunks: Vec::new(),
    });
    let error = ChatMessages::new(chat)
        .count_tokens(messages_request(&json!({"model": "gpt", "messages": []})))
        .await
        .expect_err("no tokenizer");
    assert_eq!(
        error,
        MessagesError::invalid_request(
            "Model 'gpt' serves the Messages API by translating requests into chat completions, and only a model with `from: anthropic` can count tokens: counts are specific to the model's tokenizer. See: https://spiceai.org/docs/components/models"
        )
    );
}

#[tokio::test]
async fn tool_calls_that_finish_with_stop_are_a_tool_use_stop() {
    let answer = completion(
        &json!({"role": "assistant", "content": null, "tool_calls": [
            {"id": "call_1", "type": "function", "function": {"name": "read_file", "arguments": "{}"}},
        ]}),
        "stop",
    );
    assert_eq!(
        message(answer, false).expect("a message").stop_reason,
        Some(messages_api::types::StopReason::ToolUse)
    );
    let streamed = events(vec![
        Ok(chunk(&tool_call(
            0,
            Some("call_1"),
            Some("read_file"),
            "{}",
        ))),
        Ok(chunk(
            &json!({"index": 0, "delta": {}, "finish_reason": "stop"}),
        )),
    ])
    .await;
    assert_eq!(
        streamed
            .iter()
            .find(|event| event["type"] == "message_delta")
            .map(|event| event["delta"]["stop_reason"].clone()),
        Some(json!("tool_use"))
    );
}

#[test]
fn a_failed_tool_result_says_so() {
    let chat = translated(&json!({
        "model": "gpt",
        "max_tokens": 10,
        "messages": [
            {"role": "user", "content": "Read a.rs"},
            {"role": "assistant", "content": [{"type": "tool_use", "id": "toolu_1", "name": "read_file", "input": {}}]},
            {"role": "user", "content": [
                {"type": "tool_result", "tool_use_id": "toolu_1", "is_error": true, "content": "ENOENT: no such file"},
                {"type": "tool_result", "tool_use_id": "toolu_2", "is_error": false, "content": "fine"},
            ]},
        ],
    }))
    .expect("a translatable request");
    assert_eq!(
        chat["messages"].as_array().expect("messages")[2..],
        [
            json!({"role": "tool", "content": "Error: ENOENT: no such file", "tool_call_id": "toolu_1"}),
            json!({"role": "tool", "content": "fine", "tool_call_id": "toolu_2"}),
        ]
    );
}

#[test]
fn a_tool_call_cut_off_by_max_tokens_is_left_out() {
    let answer = completion(
        &json!({"role": "assistant", "content": "Writing it.", "tool_calls": [
            {"id": "call_1", "type": "function", "function": {"name": "write_file", "arguments": "{\"path\": \"a.rs\", \"content\": \"fn ma"}},
        ]}),
        "length",
    );
    let cut = message(answer, false).expect("a message");
    assert_eq!(
        serde_json::to_value(&cut).expect("serializable")["content"],
        json!([{"type": "text", "text": "Writing it."}])
    );
    assert_eq!(
        cut.stop_reason,
        Some(messages_api::types::StopReason::MaxTokens)
    );
}

/// A document's title and context reach the model whatever its source: they say what the
/// document is and how to read it.
#[test]
fn a_documents_title_and_context_are_kept_for_every_source() {
    let chat = translated(&json!({
        "model": "gpt",
        "max_tokens": 10,
        "messages": [{"role": "user", "content": [
            {"type": "document", "title": "Ledger", "context": "Amounts are in cents", "source": {"type": "content", "content": "100"}},
            {"type": "document", "title": "Ledger", "context": "Amounts are in cents", "source": {"type": "content", "content": [{"type": "text", "text": "200"}]}},
            {"type": "document", "title": "invoice.pdf", "context": "Amounts are in cents", "source": {"type": "base64", "media_type": "application/pdf", "data": "JVBER"}},
        ]}],
    }))
    .expect("a translatable request");
    assert_eq!(
        chat["messages"][0]["content"],
        json!([
            {"type": "text", "text": "Ledger\nAmounts are in cents"},
            {"type": "text", "text": "100"},
            {"type": "text", "text": "Ledger\nAmounts are in cents"},
            {"type": "text", "text": "200"},
            {"type": "text", "text": "invoice.pdf\nAmounts are in cents"},
            {"type": "file", "file": {"file_data": "data:application/pdf;base64,JVBER", "filename": "invoice.pdf"}},
        ])
    );
}

/// A text block carries over its text; its `citations`, which point into documents an earlier
/// answer was given, are left behind like that answer's thinking; any other field is refused.
#[test]
fn a_text_block_is_carried_over_and_an_unknown_field_of_it_refused() {
    let history = |assistant_text: Value| {
        json!({
            "model": "gpt",
            "max_tokens": 10,
            "messages": [
                {"role": "user", "content": [{"type": "text", "text": "Summarize.", "cache_control": {"type": "ephemeral"}}]},
                {"role": "assistant", "content": [assistant_text]},
                {"role": "user", "content": "More?"},
            ],
        })
    };
    let chat = translated(&history(json!({
        "type": "text",
        "text": "It grew 4%.",
        "citations": [{"type": "char_location", "cited_text": "grew 4%", "document_index": 0, "start_char_index": 0, "end_char_index": 7}],
    })))
    .expect("history with citations");
    assert_eq!(
        chat["messages"][1],
        json!({"role": "assistant", "content": "It grew 4%."})
    );

    let error = translated(&history(
        json!({"type": "text", "text": "It grew 4%.", "highlight": true}),
    ))
    .expect_err("an unknown text-block field");
    assert!(
        error
            .message
            .contains("cannot carry `highlight` on a text block."),
        "{}",
        error.message
    );
}
