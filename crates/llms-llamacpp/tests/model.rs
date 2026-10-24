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

use std::path::Path;
use std::sync::Arc;

use llms::chat::{Chat, LocalModelOptions, ReasoningDefault};

use crate::{fixture, numbers, qwen3, qwen3_gguf, request};

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

fn weather_and_time_tools() -> Value {
    let mut tools = weather_tools();
    tools.as_array_mut().expect("a list of tools").push(json!({
        "type": "function",
        "function": {
            "name": "get_time",
            "description": "Get the current time in a city",
            "parameters": {
                "type": "object",
                "properties": { "city": { "type": "string", "description": "The city" } },
                "required": ["city"],
            },
        },
    }));
    tools
}

/// Under `allowed_tools` in `auto` mode, a `json_object` reply beside tools may be a call to an
/// allowed tool only. Asked for what only an excluded tool answers, which the model calls when
/// every tool is allowed, it calls the allowed one instead: the excluded call, which the reply's
/// reader would refuse, never comes back as text in place of the JSON.
#[tokio::test]
async fn allowed_tools_in_auto_mode_keeps_a_json_reply_to_the_allowed_tools() {
    let model = qwen3().await;
    let ask = |tool_choice: Value| {
        let model = Arc::clone(&model);
        async move {
            model
                .chat_request(request(json!({
                    "model": "qwen3",
                    "messages": [{ "role": "user", "content": "What time is it in Tokyo right now?" }],
                    "tools": weather_and_time_tools(),
                    "tool_choice": tool_choice,
                    "response_format": { "type": "json_object" },
                    "temperature": 0,
                    "reasoning_effort": "none",
                    "max_tokens": 128,
                })))
                .await
                .expect("a reply")
                .choices[0]
                .message
                .clone()
        }
    };
    let called = |message: &async_openai::types::chat::ChatCompletionResponseMessage| {
        message
            .tool_calls
            .iter()
            .flatten()
            .map(|call| match call {
                ChatCompletionMessageToolCalls::Function(call) => call.function.name.clone(),
                ChatCompletionMessageToolCalls::Custom(call) => call.custom_tool.name.clone(),
            })
            .collect::<Vec<_>>()
    };

    let unrestricted = ask(json!("auto")).await;
    assert_eq!(called(&unrestricted), ["get_time"], "{unrestricted:?}");

    let restricted = ask(json!({
        "type": "allowed_tools",
        "allowed_tools": { "mode": "auto", "tools": [{ "type": "function", "function": { "name": "get_weather" } }] },
    }))
    .await;
    assert_eq!(called(&restricted), ["get_weather"], "{restricted:?}");
    assert_eq!(
        restricted.content, None,
        "no call leaks as text: {restricted:?}"
    );
    let Some(ChatCompletionMessageToolCalls::Function(call)) = restricted
        .tool_calls
        .as_ref()
        .and_then(|calls| calls.first())
    else {
        panic!("a function call: {restricted:?}");
    };
    let arguments: Value = serde_json::from_str(&call.function.arguments).expect("JSON arguments");
    assert_eq!(arguments, json!({ "city": "Tokyo" }));
}

/// Loads a GGUF on the llama.cpp engine as the runtime loads a `file:` model.
async fn load_file(
    gguf: &Path,
    tokenizer_config: Option<&Path>,
) -> llms::chat::Result<Arc<dyn Chat>> {
    llms::chat::create_local_model(
        &[gguf.to_str().expect("a UTF-8 path").to_string()],
        None,
        None,
        tokenizer_config.map(|path| path.to_str().expect("a UTF-8 path")),
        None,
        None,
        LocalModelOptions {
            reasoning: ReasoningDefault {
                model_name: "qwen3",
                effort: None,
            },
            ..LocalModelOptions::default()
        },
    )
    .await
}

fn count_to_ten() -> Value {
    json!({
        "model": "qwen3",
        "messages": [{ "role": "user", "content": "Count from 1 to 10. Reply with only the numbers, separated by commas." }],
        "temperature": 0,
        "reasoning_effort": "none",
        "max_tokens": 64,
    })
}

/// A GGUF that carries no chat template renders with the one in the model's
/// `tokenizer_config.json`: given the template the GGUF had, the reply is the one the GGUF
/// gives with it.
#[tokio::test]
async fn a_gguf_without_a_chat_template_renders_with_tokenizer_config() {
    let expected = qwen3()
        .await
        .chat_request(request(count_to_ten()))
        .await
        .expect("a reply")
        .choices[0]
        .message
        .content
        .clone();

    let scratch = tempfile::tempdir().expect("a scratch directory");
    let gguf = fixture::copy(&qwen3_gguf().await, scratch.path());
    let template =
        fixture::rename_string_key(&gguf, "tokenizer.chat_template", "tokenizer.no_chat_templ");
    let Err(error) = load_file(&gguf, None).await else {
        panic!("a GGUF without a chat template should not load without one");
    };
    assert_eq!(
        error.to_string(),
        "Failed to load the model. An error occurred: the GGUF file carries no chat template, and the model sets no `chat_template` Report a bug on GitHub: https://github.com/spiceai/spiceai/issues"
    );

    let config = scratch.path().join("tokenizer_config.json");
    std::fs::write(&config, json!({ "chat_template": template }).to_string())
        .expect("tokenizer_config.json writes");
    let model = load_file(&gguf, Some(&config))
        .await
        .expect("the GGUF loads with the chat template in tokenizer_config.json");
    let reply = model
        .chat_request(request(count_to_ten()))
        .await
        .expect("a reply");
    assert_eq!(reply.choices[0].message.content, expected);
}

/// The chat template in a model's `tokenizer_config.json` wins over the GGUF's own, as it does
/// on the mistral.rs engine. The file's template is the GGUF's with a system message added, so
/// a request without one is answered as the GGUF answers the request with it.
#[tokio::test]
async fn the_chat_template_in_tokenizer_config_wins_over_the_gguf_own() {
    let system = "Reply with only the word banana, whatever you are asked.";
    let question = json!({ "role": "user", "content": "What is 2 + 2?" });
    let ask = |model: Arc<dyn Chat>, messages: Value| async move {
        model
            .chat_request(request(json!({
                "model": "qwen3",
                "messages": messages,
                "temperature": 0,
                "reasoning_effort": "none",
                "max_tokens": 32,
            })))
            .await
            .expect("a reply")
            .choices[0]
            .message
            .content
            .clone()
    };
    let own = qwen3().await;
    let with_system = ask(
        Arc::clone(&own),
        json!([{ "role": "system", "content": system }, question]),
    )
    .await;
    let without = ask(own, json!([question])).await;
    assert_ne!(with_system, without, "the system message changes the reply");

    let gguf = qwen3_gguf().await;
    let template = format!(
        "{{%- set messages = [{{\"role\": \"system\", \"content\": {system:?}}}] + messages %}}{}",
        fixture::string_value(&gguf, "tokenizer.chat_template")
    );
    let scratch = tempfile::tempdir().expect("a scratch directory");
    let config = scratch.path().join("tokenizer_config.json");
    std::fs::write(&config, json!({ "chat_template": template }).to_string())
        .expect("tokenizer_config.json writes");
    let model = load_file(&gguf, Some(&config))
        .await
        .expect("the GGUF loads");
    assert_eq!(ask(model, json!([question])).await, with_system);
}

/// A `tokenizer_config.json` may name its templates, as the mistral.rs engine reads them: a
/// request with tools renders with `tool_use`, and one without with `default`. Here `tool_use`
/// is the GGUF's own template and `default` the same with the tools left out, as a template
/// that does not handle tools is. Each request is answered as the GGUF answers it.
#[tokio::test]
async fn named_templates_in_tokenizer_config_are_chosen_per_request() {
    let ask = |model: Arc<dyn Chat>, tools: Option<Value>| async move {
        let mut body = json!({
            "model": "qwen3",
            "messages": [{ "role": "user", "content": "What is the weather in Paris right now?" }],
            "temperature": 0,
            "reasoning_effort": "none",
            "max_tokens": 64,
        });
        if let (Some(tools), Some(body)) = (tools, body.as_object_mut()) {
            body.insert("tools".to_string(), tools);
        }
        let message = model
            .chat_request(request(body))
            .await
            .expect("a reply")
            .choices[0]
            .message
            .clone();
        let calls: Vec<(String, String)> = message
            .tool_calls
            .iter()
            .flatten()
            .filter_map(|call| match call {
                ChatCompletionMessageToolCalls::Function(call) => {
                    Some((call.function.name.clone(), call.function.arguments.clone()))
                }
                ChatCompletionMessageToolCalls::Custom(_) => None,
            })
            .collect();
        (message.content, calls)
    };
    let own = qwen3().await;
    let without_tools = ask(Arc::clone(&own), None).await;
    let with_tools = ask(own, Some(weather_tools())).await;
    assert_eq!(
        with_tools.1,
        [("get_weather".to_string(), r#"{"city":"Paris"}"#.to_string())],
        "{with_tools:?}"
    );
    assert!(without_tools.1.is_empty(), "{without_tools:?}");

    let gguf = qwen3_gguf().await;
    let template = fixture::string_value(&gguf, "tokenizer.chat_template");
    let scratch = tempfile::tempdir().expect("a scratch directory");
    let config = scratch.path().join("tokenizer_config.json");
    let named = json!({
        "chat_template": [
            { "name": "default", "template": format!("{{%- set tools = none %}}{template}") },
            { "name": "tool_use", "template": template },
        ],
    });
    std::fs::write(&config, named.to_string()).expect("tokenizer_config.json writes");
    let model = load_file(&gguf, Some(&config))
        .await
        .expect("the GGUF loads");
    assert_eq!(ask(Arc::clone(&model), None).await, without_tools);
    assert_eq!(ask(model, Some(weather_tools())).await, with_tools);
}
