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

//! A Harmony reply held to a `response_format` after a chat template whose generation prompt
//! opens the reply's final message and begins its body. No small gpt-oss GGUF is published, so
//! the model is Qwen3-0.6B with seven of its control tokens respelled as Harmony's markers:
//! llama.cpp, and this engine, know a vocabulary's Harmony markers by their text, so the copy
//! is served as a gpt-oss GGUF is, and its replies are read and held to grammars the same way.

use std::path::Path;
use std::sync::Arc;

use async_openai::types::chat::FinishReason;
use futures::StreamExt;
use serde_json::{Value, json};

use llms::chat::{Chat, LocalModelOptions, ReasoningDefault};

use crate::{fixture, qwen3_gguf, request};

/// Qwen3's control tokens, respelled as the Harmony markers: `<|im_end|>`, Qwen's end of turn,
/// becomes `<|return|>`, which ends a Harmony reply.
const MARKERS: [(&str, &str); 7] = [
    ("<|im_start|>", "<|start|>"),
    ("<|im_end|>", "<|return|>"),
    ("<|box_end|>", "<|channel|>"),
    ("<|fim_pad|>", "<|message|>"),
    ("<|box_start|>", "<|constrain|>"),
    ("<|quad_end|>", "<|end|>"),
    ("<|quad_start|>", "<|call|>"),
];

/// A Harmony template whose generation prompt opens the final message and begins its body
/// with `begun`.
fn template(begun: &str) -> String {
    format!(
        "{{% for m in messages %}}<|start|>{{{{ m.role }}}}<|message|>{{{{ m.content }}}}<|end|>{{% endfor %}}<|start|>assistant<|channel|>final<|message|>{begun}"
    )
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
                model_name: "harmony",
                effort: None,
            },
            ..LocalModelOptions::default()
        },
    )
    .await
    .expect("the respelled GGUF loads as a Harmony model")
}

/// The answer to France's capital, held to one of two cities.
fn structured() -> Value {
    json!({
        "model": "harmony",
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

/// The grammar of a structured reply is advanced over the body the prompt began, `{"answer":"Ly`,
/// so the reply goes on from it, sending it first, and can only finish `Lyon`. A body the
/// grammar does not allow, `{"answer":"Ber`, is refused without the engine aborting: the grammar
/// is built again at its root, the reply starts in a header, and the refused body is not sent.
#[tokio::test]
async fn a_harmony_grammar_goes_on_from_a_body_the_prompt_began_when_it_allows_it() {
    let scratch = tempfile::tempdir().expect("a scratch directory");
    let gguf = fixture::copy_with_tokens(&qwen3_gguf().await, scratch.path(), &MARKERS);
    fixture::rename_string_key(&gguf, "tokenizer.chat_template", "tokenizer.no_chat_templ");

    let model = load(&gguf, &template("{\"answer\":\"Ly")).await;
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

    let model = load(&gguf, &template("{\"answer\":\"Ber")).await;
    let (pieces, finish) = streamed(&model, structured()).await;
    let content = pieces.concat();
    assert!(
        !content.starts_with("{\"answer\":\"Ber"),
        "the refused body is not sent: {content:?}"
    );
    // The model writes the reply from the grammar's root, so its own choices decide whether it
    // reaches the final message in the token budget; when it does, the content is the schema's.
    let answers = [json!({ "answer": "Paris" }), json!({ "answer": "Lyon" })];
    match finish {
        Some(FinishReason::Stop) => {
            let content: Value = serde_json::from_str(&content).expect("the content is JSON");
            assert!(answers.contains(&content), "{content}");
        }
        // A budget spent before the final message: the reply is the model's, from the root.
        Some(FinishReason::Length) => {}
        other => panic!("the reply ends at its end or its budget, not {other:?}"),
    }
}
