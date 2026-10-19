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

//! What a `/v1/messages` request answered by a chat model costs to translate, without the
//! network: the request into a chat completion request, the completion back into a Message, and
//! a streamed completion's chunks into Message events.
//!
//! The request is a coding agent's 40th turn: a system prompt in blocks, 12 tools, and a history
//! of 40 messages alternating tool calls with their results.

#![expect(clippy::expect_used, reason = "benchmark setup")]

use async_openai::types::chat::{CreateChatCompletionResponse, CreateChatCompletionStreamResponse};
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use messages_api::types::CreateMessageRequest;
use messages_chat::request::chat_request;
use messages_chat::response::message;
use messages_chat::stream::Translator;
use serde_json::{Value, json};

fn agent_turn() -> Value {
    let tools: Vec<Value> = (0..12)
        .map(|i| {
            json!({
                "name": format!("tool_{i}"),
                "description": "Reads or writes a file in the workspace, and returns what it found.",
                "input_schema": {"type": "object", "properties": {"path": {"type": "string"}, "content": {"type": "string"}}, "required": ["path"]},
            })
        })
        .collect();
    let mut messages = vec![json!({"role": "user", "content": "Refactor the parser module."})];
    for i in 0..20 {
        messages.push(json!({"role": "assistant", "content": [
            {"type": "thinking", "thinking": "I should look at the next file.", "signature": "EqQBCkgIARABGAIiQ"},
            {"type": "text", "text": "Reading the next file."},
            {"type": "tool_use", "id": format!("toolu_{i}"), "name": "tool_1", "input": {"path": format!("src/file_{i}.rs")}},
        ]}));
        messages.push(json!({"role": "user", "content": [
            {"type": "tool_result", "tool_use_id": format!("toolu_{i}"), "content": "fn parse(input: &str) -> Result<Ast> {\n    todo!()\n}\n".repeat(20)},
        ]}));
    }
    json!({
        "model": "gpt",
        "max_tokens": 32000,
        "stream": true,
        "system": [
            {"type": "text", "text": "You are a coding agent. ".repeat(100)},
            {"type": "text", "text": "Be brief.", "cache_control": {"type": "ephemeral"}},
        ],
        "tools": tools,
        "thinking": {"type": "adaptive"},
        "output_config": {"effort": "high"},
        "messages": messages,
    })
}

fn completion() -> CreateChatCompletionResponse {
    serde_json::from_value(json!({
        "id": "chatcmpl-abc", "object": "chat.completion", "created": 0, "model": "gpt",
        "choices": [{"index": 0, "finish_reason": "tool_calls", "message": {
            "role": "assistant", "content": "Updating the parser.",
            "tool_calls": [{"id": "call_1", "type": "function", "function": {"name": "tool_2", "arguments": "{\"path\":\"src/parser.rs\",\"content\":\"fn parse() {}\"}"}}],
        }}],
        "usage": {"prompt_tokens": 30000, "completion_tokens": 400, "total_tokens": 30400, "prompt_tokens_details": {"cached_tokens": 28000}},
    }))
    .expect("a completion")
}

/// A streamed answer of 200 text chunks and a tool call in 50 argument chunks.
fn chunks() -> Vec<CreateChatCompletionStreamResponse> {
    let chunk = |delta: Value, finish: Value| -> CreateChatCompletionStreamResponse {
        serde_json::from_value(json!({
            "id": "chatcmpl-abc", "object": "chat.completion.chunk", "created": 0, "model": "gpt",
            "choices": [{"index": 0, "delta": delta, "finish_reason": finish}],
        }))
        .expect("a chunk")
    };
    let mut chunks = vec![chunk(json!({"role": "assistant"}), Value::Null)];
    chunks.extend((0..200).map(|_| chunk(json!({"content": "token "}), Value::Null)));
    chunks.push(chunk(json!({"tool_calls": [{"index": 0, "id": "call_1", "type": "function", "function": {"name": "tool_2", "arguments": ""}}]}), Value::Null));
    chunks.extend((0..50).map(|_| {
        chunk(
            json!({"tool_calls": [{"index": 0, "function": {"arguments": "abcd"}}]}),
            Value::Null,
        )
    }));
    chunks.push(chunk(json!({}), json!("tool_calls")));
    chunks
}

fn bench(c: &mut Criterion) {
    let request = agent_turn();
    c.bench_function("request: parse and translate an agent turn", |b| {
        b.iter_batched(
            || request.clone(),
            |request| {
                let typed: CreateMessageRequest =
                    serde_json::from_value(request).expect("a request");
                chat_request(typed).expect("a chat request")
            },
            BatchSize::SmallInput,
        );
    });

    let answer = completion();
    c.bench_function("response: a completion as a Message", |b| {
        b.iter_batched(
            || answer.clone(),
            |answer| {
                serde_json::to_value(message(answer, false).expect("a message")).expect("JSON")
            },
            BatchSize::SmallInput,
        );
    });

    let streamed = chunks();
    c.bench_function("stream: 252 chunks as Message events", |b| {
        b.iter_batched(
            || streamed.clone(),
            |streamed| {
                let mut translator = Translator::new("gpt".to_string());
                let mut events = 0;
                for chunk in streamed {
                    for event in translator.chunk(chunk).expect("events") {
                        events += usize::from(serde_json::to_string(&event).is_ok());
                    }
                }
                events + translator.finish().len()
            },
            BatchSize::SmallInput,
        );
    });
}

criterion_group!(benches, bench);
criterion_main!(benches);
