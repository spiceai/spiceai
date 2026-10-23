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
    let reasoning = message.reasoning_content.as_deref().unwrap_or_default();
    let content = message.content.as_deref().unwrap_or_default();
    assert!(!reasoning.trim().is_empty(), "{message:?}");
    // The tags split the reply and appear in neither part. The answer's wording differs
    // between the Metal and CPU backends, which this model's greedy reply is sensitive to.
    for part in [reasoning, content] {
        assert!(
            !part.contains("<think>") && !part.contains("</think>"),
            "{message:?}"
        );
    }
    assert!(content.contains("42"), "{message:?}");
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
        if let Some(calls) = &choice.message.tool_calls {
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
        } else {
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

/// A prompt the model cannot finish in five tokens, asked once, and asked for two choices.
/// At temperature 0 every choice repeats the single reply.
fn one_and_two_choices(stream: bool) -> (Value, Value) {
    let one = json!({
        "model": "qwen3",
        "messages": [{ "role": "user", "content": "Write a long story about the sea." }],
        "temperature": 0,
        "reasoning_effort": "none",
        "max_tokens": 5,
    });
    let mut two = one.clone();
    two["n"] = json!(2);
    two["stream"] = json!(stream);
    (one, two)
}

#[tokio::test]
async fn n_returns_that_many_choices() {
    let model = qwen3().await;
    let (one, two) = one_and_two_choices(false);
    let one = model.chat_request(request(one)).await.expect("a reply");
    let reply = model.chat_request(request(two)).await.expect("a reply");
    let expected = one.choices[0].message.content.clone();
    let choices: Vec<_> = reply
        .choices
        .iter()
        .map(|c| (c.index, c.message.content.clone(), c.finish_reason))
        .collect();
    assert_eq!(
        choices,
        vec![
            (0, expected.clone(), Some(FinishReason::Length)),
            (1, expected, Some(FinishReason::Length)),
        ]
    );
    let prompt_tokens = one.usage.expect("usage").prompt_tokens;
    assert_eq!(
        reply
            .usage
            .map(|u| (u.prompt_tokens, u.completion_tokens, u.total_tokens)),
        Some((prompt_tokens, 10, prompt_tokens + 10))
    );
}

#[tokio::test]
async fn n_streams_that_many_choices() {
    let model = qwen3().await;
    let (one, two) = one_and_two_choices(true);
    let one = model.chat_request(request(one)).await.expect("a reply");
    let mut stream = model.chat_stream(request(two)).await.expect("a stream");
    let mut choices: Vec<(String, Option<FinishReason>)> = Vec::new();
    let mut usage = None;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.expect("a chunk");
        usage = chunk.usage.or(usage);
        for choice in chunk.choices {
            let index = usize::try_from(choice.index).expect("an index fits usize");
            if choices.len() <= index {
                choices.resize(index + 1, (String::new(), None));
            }
            let (content, finish) = &mut choices[index];
            content.push_str(choice.delta.content.as_deref().unwrap_or_default());
            *finish = choice.finish_reason.or(*finish);
        }
    }
    let expected = one.choices[0].message.content.clone().expect("content");
    assert_eq!(
        choices,
        vec![
            (expected.clone(), Some(FinishReason::Length)),
            (expected, Some(FinishReason::Length)),
        ]
    );
    let prompt_tokens = one.usage.expect("usage").prompt_tokens;
    assert_eq!(
        usage.map(|u| (u.prompt_tokens, u.completion_tokens, u.total_tokens)),
        Some((prompt_tokens, 10, prompt_tokens + 10))
    );
}

#[tokio::test]
async fn n_of_zero_is_refused() {
    let Err(error) = qwen3()
        .await
        .chat_request(request(json!({
            "model": "qwen3",
            "messages": [{ "role": "user", "content": "Hello" }],
            "n": 0,
        })))
        .await
    else {
        panic!("`n: 0` should be refused");
    };
    assert_eq!(
        error.to_string(),
        "invalid args: `n` must be between 1 and 128, the number of choices to generate."
    );
}

/// Under `tool_choice: none` the model may not call a tool, so a `json_schema` reply is held to
/// its schema alone, even when the prompt asks for what a tool would answer.
#[tokio::test]
async fn under_tool_choice_none_a_json_schema_reply_keeps_its_schema() {
    let model = qwen3().await;
    let reply = model
        .chat_request(request(json!({
            "model": "qwen3",
            "messages": [{ "role": "user", "content": "What is the weather in Paris right now?" }],
            "tools": weather_tools(),
            "tool_choice": "none",
            "response_format": {
                "type": "json_schema",
                "json_schema": {
                    "name": "answer",
                    "schema": {
                        "type": "object",
                        "properties": { "city": { "type": "string" } },
                        "required": ["city"],
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
    let message = &reply.choices[0].message;
    assert_eq!(message.tool_calls, None);
    let content = message.content.clone().expect("content");
    let value: Value = serde_json::from_str(&content)
        .unwrap_or_else(|e| panic!("the reply is not JSON ({e}): {content}"));
    let keys: Vec<&String> = value.as_object().expect("an object").keys().collect();
    assert_eq!(keys, ["city"], "{content}");
    assert!(value["city"].is_string(), "{content}");
}

/// A model dropped while a client holds a stream it has stopped reading frees promptly: the
/// decode thread, waiting for room in that client's full buffer, sees the model stopping.
#[tokio::test]
async fn a_model_dropped_under_a_stalled_stream_stops_promptly() {
    let model = qwen3().await;
    let mut stream = model
        .chat_stream(request(json!({
            "model": "qwen3",
            "messages": [{ "role": "user", "content": "Write a very long story about the sea." }],
            "temperature": 0,
            "reasoning_effort": "none",
            "max_tokens": 4000,
            "stream": true,
        })))
        .await
        .expect("a stream");
    stream
        .next()
        .await
        .expect("a first chunk")
        .expect("a chunk");
    // The stream's buffer holds 64 tokens, which Qwen3-0.6B decodes in well under a second;
    // the count drained below shows the buffer was full when the model was dropped.
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    let started = std::time::Instant::now();
    drop(model);
    let stopped = started.elapsed();
    let mut buffered = 0;
    while let Some(Ok(_)) = stream.next().await {
        buffered += 1;
    }
    assert!(
        buffered >= 64,
        "the buffer held {buffered} chunks, so it was not full"
    );
    assert!(
        stopped < std::time::Duration::from_secs(5),
        "dropping the model took {stopped:?}"
    );
}

/// `frequency_penalty` and `presence_penalty` reach the sampler: at temperature 0, a reply
/// asked to repeat a word repeats it less under either penalty than without one.
#[tokio::test]
async fn frequency_and_presence_penalties_reduce_repetition() {
    let model = qwen3().await;
    let repeats = |penalty: Value| {
        let model = std::sync::Arc::clone(&model);
        async move {
            let mut body = json!({
                "model": "qwen3",
                "messages": [{ "role": "user", "content": "Write the word yes thirty times, separated by spaces, and nothing else." }],
                "temperature": 0,
                "reasoning_effort": "none",
                "max_tokens": 64,
            });
            if let (Some(body), Some(penalty)) = (body.as_object_mut(), penalty.as_object()) {
                body.extend(penalty.clone());
            }
            let reply = model.chat_request(request(body)).await.expect("a reply");
            let content = reply.choices[0].message.content.clone().expect("content");
            (content.to_lowercase().matches("yes").count(), content)
        }
    };
    let (plain, plain_text) = repeats(json!({})).await;
    let (frequency, frequency_text) = repeats(json!({ "frequency_penalty": 2.0 })).await;
    let (presence, presence_text) = repeats(json!({ "presence_penalty": 2.0 })).await;
    assert!(plain >= 5, "without a penalty: {plain_text}");
    assert!(
        frequency < plain,
        "frequency_penalty: {frequency} vs {plain}: {frequency_text} | {plain_text}"
    );
    assert!(
        presence < plain,
        "presence_penalty: {presence} vs {plain}: {presence_text} | {plain_text}"
    );
}
