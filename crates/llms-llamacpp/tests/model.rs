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

//! Requests against Qwen3-0.6B on the llama.cpp engine, at temperature 0.

use async_openai::types::chat::{ChatCompletionMessageToolCalls, FinishReason};
use futures::StreamExt;
use serde_json::{Value, json};

use crate::{qwen3, request};

fn weather_tools() -> Value {
    json!([{
        "type": "function",
        "function": {
            "name": "get_weather",
            "description": "Get the current weather in a city",
            "parameters": {
                "type": "object",
                "properties": { "city": { "type": "string", "description": "The city" } },
                "required": ["city"],
            },
        },
    }])
}

fn numbers(text: &str) -> Vec<u32> {
    text.split(|c: char| !c.is_ascii_digit())
        .filter(|s| !s.is_empty())
        .map(|s| s.parse().expect("a run of digits is a number"))
        .collect()
}

#[tokio::test]
async fn counts_to_ten_without_reasoning() {
    let model = qwen3().await;
    let reply = model
        .chat_request(request(json!({
            "model": "qwen3",
            "messages": [{ "role": "user", "content": "Count from 1 to 10. Reply with only the numbers, separated by commas." }],
            "temperature": 0,
            "reasoning_effort": "none",
            "max_tokens": 64,
        })))
        .await
        .expect("a reply");
    let choice = &reply.choices[0];
    let content = choice.message.content.clone().expect("content");
    assert_eq!(numbers(&content), (1..=10).collect::<Vec<_>>(), "{content}");
    assert_eq!(choice.finish_reason, Some(FinishReason::Stop));
    assert_eq!(choice.message.reasoning_content, None);
}

#[tokio::test]
async fn reasons_by_default_and_answers_after() {
    let model = qwen3().await;
    let reply = model
        .chat_request(request(json!({
            "model": "qwen3",
            "messages": [{ "role": "user", "content": "What is 17 + 25? Reply with only the number." }],
            "temperature": 0,
            "max_tokens": 1024,
        })))
        .await
        .expect("a reply");
    let message = &reply.choices[0].message;
    assert!(
        message
            .reasoning_content
            .as_deref()
            .is_some_and(|r| !r.trim().is_empty()),
        "{message:?}"
    );
    assert_eq!(
        message.content.as_deref().map(str::trim),
        Some("42"),
        "{message:?}"
    );
}

#[tokio::test]
async fn a_tool_call_comes_back_as_tool_calls() {
    let model = qwen3().await;
    let reply = model
        .chat_request(request(json!({
            "model": "qwen3",
            "messages": [{ "role": "user", "content": "What is the weather in Paris right now?" }],
            "tools": weather_tools(),
            "temperature": 0,
            "reasoning_effort": "none",
            "max_tokens": 128,
        })))
        .await
        .expect("a reply");
    let choice = &reply.choices[0];
    assert_eq!(
        choice.finish_reason,
        Some(FinishReason::ToolCalls),
        "{choice:?}"
    );
    assert_eq!(
        choice.message.content, None,
        "no tag text leaks: {choice:?}"
    );
    let calls = choice.message.tool_calls.as_ref().expect("tool calls");
    assert_eq!(calls.len(), 1, "{calls:?}");
    let ChatCompletionMessageToolCalls::Function(call) = &calls[0] else {
        panic!("a function call: {calls:?}");
    };
    assert_eq!(call.function.name, "get_weather");
    let arguments: Value = serde_json::from_str(&call.function.arguments).expect("JSON arguments");
    assert_eq!(arguments, json!({ "city": "Paris" }));
}

#[tokio::test]
async fn a_streamed_tool_call_arrives_whole_with_its_finish_reason() {
    let model = qwen3().await;
    let mut stream = model
        .chat_stream(request(json!({
            "model": "qwen3",
            "messages": [{ "role": "user", "content": "What is the weather in Paris right now?" }],
            "tools": weather_tools(),
            "temperature": 0,
            "reasoning_effort": "none",
            "max_tokens": 128,
            "stream": true,
        })))
        .await
        .expect("a stream");
    let (mut calls, mut finish) = (Vec::new(), None);
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.expect("a chunk");
        for choice in chunk.choices {
            calls.extend(choice.delta.tool_calls.into_iter().flatten());
            finish = choice.finish_reason.or(finish);
        }
    }
    assert_eq!(finish, Some(FinishReason::ToolCalls));
    assert_eq!(calls.len(), 1, "{calls:?}");
    let function = calls[0].function.as_ref().expect("a function");
    assert_eq!(function.name.as_deref(), Some("get_weather"));
    let arguments: Value =
        serde_json::from_str(function.arguments.as_deref().expect("arguments")).expect("JSON");
    assert_eq!(arguments, json!({ "city": "Paris" }));
}

#[tokio::test]
async fn a_required_tool_call_is_made() {
    let model = qwen3().await;
    let reply = model
        .chat_request(request(json!({
            "model": "qwen3",
            "messages": [{ "role": "user", "content": "I'm heading to Tokyo tomorrow." }],
            "tools": weather_tools(),
            "tool_choice": "required",
            "temperature": 0,
            "reasoning_effort": "none",
            "max_tokens": 256,
        })))
        .await
        .expect("a reply");
    let choice = &reply.choices[0];
    assert_eq!(
        choice.finish_reason,
        Some(FinishReason::ToolCalls),
        "{choice:?}"
    );
    let calls = choice.message.tool_calls.as_ref().expect("tool calls");
    let ChatCompletionMessageToolCalls::Function(call) = &calls[0] else {
        panic!("a function call: {calls:?}");
    };
    assert_eq!(call.function.name, "get_weather");
}

#[tokio::test]
async fn a_json_schema_constrains_the_reply() {
    let model = qwen3().await;
    let reply = model
        .chat_request(request(json!({
            "model": "qwen3",
            "messages": [{ "role": "user", "content": "What is 6 times 7?" }],
            "response_format": {
                "type": "json_schema",
                "json_schema": {
                    "name": "answer",
                    "strict": true,
                    "schema": {
                        "type": "object",
                        "properties": { "answer": { "type": "integer" } },
                        "required": ["answer"],
                        "additionalProperties": false,
                    },
                },
            },
            "temperature": 0,
            "reasoning_effort": "none",
            "max_tokens": 64,
        })))
        .await
        .expect("a reply");
    let content = reply.choices[0].message.content.clone().expect("content");
    let value: Value = serde_json::from_str(&content).expect("the reply is JSON");
    assert_eq!(value, json!({ "answer": 42 }), "{content}");
}

fn answer_format() -> Value {
    json!({
        "type": "json_schema",
        "json_schema": {
            "name": "answer",
            "strict": true,
            "schema": {
                "type": "object",
                "properties": { "answer": { "type": "integer" } },
                "required": ["answer"],
                "additionalProperties": false,
            },
        },
    })
}

#[tokio::test]
async fn beside_tools_a_json_schema_reply_or_a_tool_call_comes_back() {
    let model = qwen3().await;
    let ask = |content: &str| {
        request(json!({
            "model": "qwen3",
            "messages": [{ "role": "user", "content": content }],
            "tools": weather_tools(),
            "response_format": answer_format(),
            "temperature": 0,
            "reasoning_effort": "none",
            "max_tokens": 128,
        }))
    };

    // The grammar admits exactly two replies: the schema's JSON, or one call to the tool.
    for question in [
        "What is 6 times 7?",
        "What is the weather in Paris right now?",
    ] {
        let reply = model.chat_request(ask(question)).await.expect("a reply");
        let choice = &reply.choices[0];
        match &choice.message.tool_calls {
            Some(calls) => {
                assert_eq!(
                    choice.finish_reason,
                    Some(FinishReason::ToolCalls),
                    "{choice:?}"
                );
                assert_eq!(
                    choice.message.content, None,
                    "no tag text leaks: {choice:?}"
                );
                let ChatCompletionMessageToolCalls::Function(call) = &calls[0] else {
                    panic!("a function call: {calls:?}");
                };
                assert_eq!(call.function.name, "get_weather");
                let arguments: Value =
                    serde_json::from_str(&call.function.arguments).expect("JSON arguments");
                assert!(arguments["city"].is_string(), "{arguments}");
            }
            None => {
                let content = choice.message.content.clone().expect("content");
                let value: Value =
                    serde_json::from_str(&content).expect("the reply is the schema's JSON");
                assert!(value["answer"].is_i64(), "{content}");
                assert_eq!(
                    value.as_object().map(serde_json::Map::len),
                    Some(1),
                    "{content}"
                );
            }
        }
    }
    // A weather question gets the call.
    let reply = model
        .chat_request(ask("What is the weather in Paris right now?"))
        .await
        .expect("a reply");
    assert_eq!(
        reply.choices[0].finish_reason,
        Some(FinishReason::ToolCalls)
    );
}

#[tokio::test]
async fn a_streamed_reply_is_the_non_streamed_one() {
    let model = qwen3().await;
    let body = json!({
        "model": "qwen3",
        "messages": [{ "role": "user", "content": "Name the three primary colors of light." }],
        "temperature": 0,
        "reasoning_effort": "none",
        "max_tokens": 48,
    });
    let whole = model
        .chat_request(request(body.clone()))
        .await
        .expect("a reply");
    let mut streamed_body = body;
    streamed_body["stream"] = json!(true);
    let mut stream = model
        .chat_stream(request(streamed_body))
        .await
        .expect("a stream");
    let (mut content, mut usage, mut finish) = (String::new(), None, None);
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.expect("a chunk");
        usage = chunk.usage.or(usage);
        for choice in chunk.choices {
            content.push_str(choice.delta.content.as_deref().unwrap_or_default());
            finish = choice.finish_reason.or(finish);
        }
    }
    assert_eq!(Some(content), whole.choices[0].message.content);
    assert_eq!(finish, whole.choices[0].finish_reason);
    assert_eq!(
        usage.map(|u| (u.prompt_tokens, u.completion_tokens)),
        whole.usage.map(|u| (u.prompt_tokens, u.completion_tokens))
    );
}

#[tokio::test]
async fn max_tokens_ends_the_reply_at_its_length() {
    let model = qwen3().await;
    let reply = model
        .chat_request(request(json!({
            "model": "qwen3",
            "messages": [{ "role": "user", "content": "Write a long story about the sea." }],
            "temperature": 0,
            "reasoning_effort": "none",
            "max_tokens": 5,
        })))
        .await
        .expect("a reply");
    assert_eq!(reply.choices[0].finish_reason, Some(FinishReason::Length));
    assert_eq!(reply.usage.map(|u| u.completion_tokens), Some(5));
}
