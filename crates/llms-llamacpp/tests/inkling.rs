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

//! Inkling replies held to grammars over the vocabulary's marker tokens. The model is
//! Qwen3-0.6B with eight of its control tokens respelled as Inkling's markers: this engine,
//! like llama.cpp, knows a vocabulary's Inkling markers by their text, so the copy is served
//! as an Inkling GGUF is, and its replies are read and held to grammars the same way. Qwen's
//! end of turn, which ends generation, becomes `<|content_model_end_sampling|>`, which ends an
//! Inkling reply.

use std::path::Path;
use std::sync::Arc;

use async_openai::types::chat::{ChatCompletionMessageToolCalls, FinishReason};
use futures::StreamExt;
use serde_json::{Value, json};

use llms::chat::{Chat, LocalModelOptions, ReasoningDefault};

use crate::{fixture, qwen3_gguf, request};

/// Qwen3's control tokens, respelled as Inkling's markers.
const MARKERS: [(&str, &str); 9] = [
    ("<|im_start|>", "<|message_model|>"),
    ("<|im_end|>", "<|content_model_end_sampling|>"),
    ("<|box_start|>", "<|content_thinking|>"),
    ("<|box_end|>", "<|content_text|>"),
    ("<|quad_start|>", "<|content_invoke_tool_json|>"),
    ("<|quad_end|>", "<|end_message|>"),
    ("<|object_ref_start|>", "<|content_invoke_tool_text|>"),
    ("<|object_ref_end|>", "<|content_tool_error|>"),
    ("<|vision_start|>", "<|message_user|>"),
];

/// An Inkling template whose generation prompt opens the reply with `opened`, as a custom
/// template may; Inkling's own has none. Its image marker is never written, but it is what
/// tells the template, with the user-message marker, as Inkling's.
fn template(opened: &str) -> String {
    format!(
        "{{% if tools %}}<|message_system|>tool_declare<|content_xml|>{{{{ tools | tojson }}}}<|end_message|>{{% endif %}}{{% for m in messages %}}<|message_user|>{{% if m.content is not string %}}<|content_image|>{{% else %}}<|content_text|>{{{{ m.content }}}}{{% endif %}}<|end_message|>{{% endfor %}}{{% if add_generation_prompt %}}{opened}{{% endif %}}"
    )
}

/// The respelled copy of Qwen3-0.6B, without its own chat template, in `dir`.
async fn gguf(dir: &Path) -> std::path::PathBuf {
    let gguf = fixture::copy_with_tokens(&qwen3_gguf().await, dir, &MARKERS);
    fixture::rename_string_key(&gguf, "tokenizer.chat_template", "tokenizer.no_chat_templ");
    gguf
}

async fn load(gguf: &Path, template: &str) -> Arc<dyn Chat> {
    llms::chat::create_local_model(
        &[gguf.to_str().expect("a UTF-8 path").to_string()],
        None,
        None,
        None,
        None,
        None,
        LocalModelOptions {
            chat_template_literal: Some(template),
            reasoning: ReasoningDefault {
                model_name: "inkling",
                effort: None,
            },
            ..LocalModelOptions::default()
        },
    )
    .await
    .expect("the respelled GGUF loads as an Inkling model")
}

/// The answer to France's capital, held to one of two cities.
fn structured() -> Value {
    json!({
        "model": "inkling",
        "messages": [{ "role": "user", "content": "What is the capital of France?" }],
        "response_format": {
            "type": "json_schema",
            "json_schema": {
                "name": "answer",
                "schema": {
                    "type": "object",
                    "properties": { "answer": { "type": "string", "enum": ["Paris", "Lyon"] } },
                    "required": ["answer"],
                    "additionalProperties": false,
                },
            },
        },
        "temperature": 0,
        "max_tokens": 32,
        "stream": true,
    })
}

/// The content a streamed reply sends, piece by piece, and why it ends.
async fn streamed(model: &Arc<dyn Chat>, body: Value) -> (Vec<String>, Option<FinishReason>) {
    let mut stream = model.chat_stream(request(body)).await.expect("a stream");
    let (mut pieces, mut finish) = (Vec::new(), None);
    while let Some(chunk) = stream.next().await {
        for choice in chunk.expect("a chunk").choices {
            pieces.extend(choice.delta.content.filter(|c| !c.is_empty()));
            finish = choice.finish_reason.or(finish);
        }
    }
    (pieces, finish)
}

/// The grammar of a structured reply is advanced over the text block the prompt opened,
/// `{"answer":"Ly`, so the reply goes on in it, sending it first, can only finish `Lyon`, and
/// ends at the block's `<|end_message|>` and the end-of-generation token after it. A block the
/// grammar does not allow, `{"answer":"Ber`, is refused without the engine aborting: the
/// grammar is built again at its root, the reply starts between blocks, and the refused text
/// is not sent.
#[tokio::test]
async fn an_inkling_grammar_goes_on_from_a_text_block_the_prompt_opened() {
    let scratch = tempfile::tempdir().expect("a scratch directory");
    let gguf = gguf(scratch.path()).await;

    let model = load(
        &gguf,
        &template("<|message_model|><|content_text|>{\"answer\":\"Ly"),
    )
    .await;
    let (pieces, finish) = streamed(&model, structured()).await;
    drop(model);
    assert_eq!(
        pieces.first().map(String::as_str),
        Some("{\"answer\":\"Ly"),
        "the body the prompt began is sent first: {pieces:?}"
    );
    let content: Value = serde_json::from_str(&pieces.concat()).expect("the content is JSON");
    assert_eq!(
        (content, finish),
        (json!({ "answer": "Lyon" }), Some(FinishReason::Stop))
    );

    let model = load(
        &gguf,
        &template("<|message_model|><|content_text|>{\"answer\":\"Ber"),
    )
    .await;
    let (pieces, finish) = streamed(&model, structured()).await;
    let content = pieces.concat();
    assert!(
        !content.starts_with("{\"answer\":\"Ber"),
        "the refused body is not sent: {content:?}"
    );
    // The model writes the reply from the grammar's root, so its own choices decide whether it
    // reaches the text block in the token budget; when it does, the content is the schema's.
    let answers = [json!({ "answer": "Paris" }), json!({ "answer": "Lyon" })];
    match finish {
        Some(FinishReason::Stop) => {
            let content: Value = serde_json::from_str(&content).expect("the content is JSON");
            assert!(answers.contains(&content), "{content}");
        }
        // A budget spent thinking: the reply is the model's, from the root.
        Some(FinishReason::Length) => {}
        other => panic!("the reply ends at its end or its budget, not {other:?}"),
    }
}

/// A required call is made even when the prompt opens a text block: the grammar holds the
/// reply to a thinking block or none and then the call, so the text block is refused, the
/// grammar is built again at its root, and the model has to call the tool rather than answer
/// in text until its budget runs out. The tool is named `assistant` because that is what Qwen writes
/// after the token respelled as the block header, so the respelled model makes the call where
/// the grammar lets it rather than open a thinking block it never closes.
#[tokio::test]
async fn a_required_inkling_call_is_made_when_the_prompt_opened_a_text_block() {
    let scratch = tempfile::tempdir().expect("a scratch directory");
    let gguf = gguf(scratch.path()).await;
    let model = load(&gguf, &template("<|message_model|><|content_text|>")).await;
    let response = model
        .chat_request(request(json!({
            "model": "inkling",
            "messages": [{ "role": "user", "content": "What is the weather in Paris?" }],
            "tools": [{
                "type": "function",
                "function": {
                    "name": "assistant",
                    "parameters": {
                        "type": "object",
                        "properties": { "city": { "type": "string", "maxLength": 12 } },
                        "required": ["city"],
                        "additionalProperties": false,
                    },
                },
            }],
            "tool_choice": "required",
            "temperature": 0,
            "max_tokens": 64,
        })))
        .await
        .expect("the request succeeds");
    let choice = response.choices.first().expect("one choice");
    let calls: Vec<(String, Value)> = choice
        .message
        .tool_calls
        .iter()
        .flatten()
        .map(|call| match call {
            ChatCompletionMessageToolCalls::Function(call) => (
                call.function.name.clone(),
                serde_json::from_str(&call.function.arguments).expect("the arguments are JSON"),
            ),
            ChatCompletionMessageToolCalls::Custom(_) => panic!("a function call: {call:?}"),
        })
        .collect();
    assert_eq!(
        choice.finish_reason,
        Some(FinishReason::ToolCalls),
        "{calls:?} {:?}",
        choice.message.content
    );
    let [(name, arguments)] = calls.as_slice() else {
        panic!("one call: {calls:?}");
    };
    assert_eq!(
        (
            name.as_str(),
            arguments
                .as_object()
                .map(|a| a.keys().cloned().collect::<Vec<_>>())
        ),
        ("assistant", Some(vec!["city".to_string()])),
        "{arguments}"
    );
}

/// A required call's grammar is advanced over the call block the prompt opened, so the reply
/// goes on in its arguments, holds them to the tool's schema and closes the block, and the
/// call is made when the end-of-generation token ends the reply. With `parallel_tool_calls`
/// off, the grammar allows that one call only.
#[tokio::test]
async fn a_required_inkling_call_goes_on_from_the_call_block_the_prompt_opened() {
    let scratch = tempfile::tempdir().expect("a scratch directory");
    let gguf = gguf(scratch.path()).await;
    let model = load(
        &gguf,
        &template(
            "<|message_model|>get_weather<|content_invoke_tool_json|>{\"name\":\"get_weather\",\"args\":{\"city\":\"Par",
        ),
    )
    .await;
    let response = model
        .chat_request(request(json!({
            "model": "inkling",
            "messages": [{ "role": "user", "content": "What is the weather in Paris?" }],
            "tools": [{
                "type": "function",
                "function": {
                    "name": "get_weather",
                    "parameters": {
                        "type": "object",
                        "properties": { "city": { "type": "string", "maxLength": 12 } },
                        "required": ["city"],
                        "additionalProperties": false,
                    },
                },
            }],
            "tool_choice": "required",
            "parallel_tool_calls": false,
            "temperature": 0,
            "max_tokens": 32,
        })))
        .await
        .expect("the request succeeds");
    let choice = response.choices.first().expect("one choice");
    let calls: Vec<(String, Value)> = choice
        .message
        .tool_calls
        .iter()
        .flatten()
        .map(|call| match call {
            ChatCompletionMessageToolCalls::Function(call) => (
                call.function.name.clone(),
                serde_json::from_str(&call.function.arguments).expect("the arguments are JSON"),
            ),
            ChatCompletionMessageToolCalls::Custom(_) => panic!("a function call: {call:?}"),
        })
        .collect();
    assert_eq!(
        choice.finish_reason,
        Some(FinishReason::ToolCalls),
        "{calls:?}"
    );
    let [(name, arguments)] = calls.as_slice() else {
        panic!("one call: {calls:?}");
    };
    let city = arguments
        .get("city")
        .and_then(Value::as_str)
        .expect("the arguments name a city");
    assert_eq!(
        (
            name.as_str(),
            arguments.as_object().map(serde_json::Map::len)
        ),
        ("get_weather", Some(1)),
        "{arguments}"
    );
    assert!(
        city.starts_with("Par"),
        "the call goes on from the arguments the prompt began: {city:?}"
    );
}

/// The reasoning a streamed reply sends, piece by piece.
async fn reasoned(model: &Arc<dyn Chat>, body: Value) -> Vec<String> {
    let mut stream = model.chat_stream(request(body)).await.expect("a stream");
    let mut pieces = Vec::new();
    while let Some(chunk) = stream.next().await {
        for choice in chunk.expect("a chunk").choices {
            pieces.extend(choice.delta.reasoning_content.filter(|c| !c.is_empty()));
        }
    }
    pieces
}

/// Text a grammar holds spells no `<|`, which the next turn's prompt would read as the start
/// of a marker. The grammar is advanced over thinking the prompt began: `Note <` is allowed,
/// so the reply goes on from it and sends it first, and `Note <<|` is refused, so it is not
/// sent and the reply starts between blocks.
#[tokio::test]
async fn held_inkling_text_spells_no_marker_opening() {
    let scratch = tempfile::tempdir().expect("a scratch directory");
    let gguf = gguf(scratch.path()).await;
    let model = load(
        &gguf,
        &template("<|message_model|><|content_thinking|>Note <"),
    )
    .await;
    let pieces = reasoned(&model, structured()).await;
    drop(model);
    assert_eq!(
        pieces.first().map(String::as_str),
        Some("Note <"),
        "the thinking the prompt began is sent first: {pieces:?}"
    );
    let model = load(
        &gguf,
        &template("<|message_model|><|content_thinking|>Note <<|"),
    )
    .await;
    let pieces = reasoned(&model, structured()).await;
    assert!(
        !pieces.concat().starts_with("Note <<|"),
        "the refused thinking is not sent: {pieces:?}"
    );
}
