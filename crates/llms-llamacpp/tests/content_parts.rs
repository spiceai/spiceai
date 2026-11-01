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

//! A message's content sent as an array of text parts renders as llama-server renders it, for
//! every role: a chat template that reads parts gets them, so Qwen3.5's renders them one after
//! another, and one that reads only strings gets their text joined with newlines, as Qwen3's
//! does. Either way the prompt is the one some string renders, and the expected prompts below
//! are llama-server's (`/apply-template`, same GGUF, `enable_thinking: false`).

use std::path::Path;

use async_openai::error::OpenAIError;
use async_openai::types::chat::CreateChatCompletionRequest;
use llms::chat::{Chat, LocalModelOptions, ReasoningDefault};
use llms_llamacpp::LlamaCpp;
use serde_json::{Value, json};

use crate::{mtp::qwen35_gguf, prefix_reuse::gemma_gguf, qwen3_gguf, request};

const A: &str = "The secret word is PLUM.";
const B: &str = "Be brief.";

async fn load(gguf: &Path, name: &'static str) -> LlamaCpp {
    LlamaCpp::from(
        &[gguf.to_path_buf()],
        LocalModelOptions {
            reasoning: ReasoningDefault {
                model_name: name,
                effort: None,
            },
            ..LocalModelOptions::default()
        },
    )
    .await
    .expect("the model loads on llama.cpp")
}

fn text_parts(texts: &[&str]) -> Value {
    texts
        .iter()
        .map(|text| json!({ "type": "text", "text": text }))
        .collect()
}

/// For each role, a request whose message of that role has `content`, and whose other messages
/// are strings.
fn requests(content: &Value) -> Vec<(&'static str, CreateChatCompletionRequest)> {
    let hi = json!({ "role": "user", "content": "hi" });
    let call = json!({
        "role": "assistant",
        "content": null,
        "tool_calls": [{ "id": "c1", "type": "function", "function": { "name": "lookup", "arguments": "{}" } }],
    });
    let tools = json!([{
        "type": "function",
        "function": { "name": "lookup", "description": "Looks a word up.", "parameters": { "type": "object", "properties": {} } },
    }]);
    let conversations = [
        (
            "system",
            json!([{ "role": "system", "content": content }, hi]),
        ),
        (
            "developer",
            json!([{ "role": "developer", "content": content }, hi]),
        ),
        ("user", json!([{ "role": "user", "content": content }])),
        (
            "assistant",
            json!([hi, { "role": "assistant", "content": content }, { "role": "user", "content": "ok" }]),
        ),
        (
            "tool",
            json!([hi, call, { "role": "tool", "tool_call_id": "c1", "content": content }]),
        ),
    ];
    conversations
        .into_iter()
        .map(|(role, messages)| {
            let mut body = json!({
                "model": "qwen",
                "messages": messages,
                "reasoning_effort": "none",
                "max_tokens": 1,
            });
            if role == "tool" {
                body["tools"] = tools.clone();
            }
            (role, request(body))
        })
        .collect()
}

/// Asserts that, for each of `roles`, a message whose content is the text parts `parts`
/// renders the prompt the same message renders with the string `text`, which holds `text`.
/// Every role is checked before the assertion, so a failure lists each role that renders wrong.
fn assert_parts_render_as(model: &LlamaCpp, roles: &[&str], parts: &[&str], text: &str) {
    let with_parts = requests(&text_parts(parts));
    let with_text = requests(&json!(text));
    let mut wrong = Vec::new();
    for ((role, with_parts), (_, with_text)) in with_parts.iter().zip(&with_text) {
        if !roles.contains(role) {
            continue;
        }
        let expected = model
            .render_prompt(with_text)
            .unwrap_or_else(|e| panic!("a {role} message's string renders: {e}"));
        assert!(
            expected.contains(text),
            "the {role} message is in its prompt: {expected:?}"
        );
        match model.render_prompt(with_parts) {
            Ok(rendered) if rendered == expected => {}
            Ok(rendered) => wrong.push(format!(
                "{role}: rendered {rendered:?}, but its string renders {expected:?}"
            )),
            Err(e) => wrong.push(format!("{role}: {e}")),
        }
    }
    assert!(
        wrong.is_empty(),
        "text parts {parts:?} render as {text:?}, but for:\n{}",
        wrong.join("\n")
    );
}

/// Qwen3.5's chat template reads a message's text parts and renders them one after another,
/// for every role, and raises `Unexpected item type in content` on a part without `text`.
#[tokio::test]
async fn qwen3_5_renders_text_parts_one_after_another() {
    let model = load(&qwen35_gguf().await, "qwen3.5").await;
    let all = ["system", "developer", "user", "assistant", "tool"];
    assert_parts_render_as(&model, &all, &[A], A);
    assert_parts_render_as(&model, &all, &[A, B], &format!("{A}{B}"));

    // llama-server's prompt for the user message of two parts.
    let user = &requests(&text_parts(&[A, B]))[2].1;
    assert_eq!(
        model.render_prompt(user).expect("the user message renders"),
        "<|im_start|>user\nThe secret word is PLUM.Be brief.<|im_end|>\n<|im_start|>assistant\n<think>\n\n</think>\n\n"
    );

    // The whole request, decoded: its prompt is as long as the string's.
    let joined = &requests(&json!(format!("{A}{B}")))[2].1;
    assert_eq!(
        prompt_tokens(&model, user).await,
        prompt_tokens(&model, joined).await
    );
}

/// How many tokens `req`'s prompt is, as the engine reports it in the reply's usage.
async fn prompt_tokens(model: &LlamaCpp, req: &CreateChatCompletionRequest) -> u32 {
    model
        .chat_request(req.clone())
        .await
        .unwrap_or_else(|e| panic!("a reply: {e}"))
        .usage
        .expect("usage")
        .prompt_tokens
}

/// Qwen3's chat template reads only a string, so a message's text parts reach it joined with
/// newlines, as llama-server joins them. Its template has no developer role, which llama-server
/// renders as a system message instead, so that role is left out here.
#[tokio::test]
async fn qwen3_renders_text_parts_joined_with_newlines() {
    let model = load(&qwen3_gguf().await, "qwen3").await;
    let roles = ["system", "user", "assistant", "tool"];
    assert_parts_render_as(&model, &roles, &[A], A);
    assert_parts_render_as(&model, &roles, &[A, B], &format!("{A}\n{B}"));

    // llama-server's prompt for the user message of two parts.
    let user = &requests(&text_parts(&[A, B]))[2].1;
    assert_eq!(
        model.render_prompt(user).expect("the user message renders"),
        "<|im_start|>user\nThe secret word is PLUM.\nBe brief.<|im_end|>\n<|im_start|>assistant\n<think>\n\n</think>\n\n"
    );
}

/// Gemma 3's chat template reads every part of a user or assistant message but only the first
/// part of a system message, so a system message's parts reach it joined with newlines and keep
/// all their text. llama-server, which probes a user message alone, sends the system message's
/// parts as they are, and its prompt holds only the first.
#[tokio::test]
async fn gemma_3_gets_a_system_message_s_parts_joined() {
    let model = load(&gemma_gguf().await, "gemma-3").await;
    assert_parts_render_as(&model, &["system"], &[A, B], &format!("{A}\n{B}"));
    assert_parts_render_as(&model, &["user", "assistant"], &[A, B], &format!("{A}{B}"));
}

/// Qwen3-0.6B with the chat template in `file` (a `.jinja` file or a `tokenizer_config.json`),
/// written in `scratch` with `contents`.
async fn with_template(scratch: &Path, file: &str, contents: &str) -> LlamaCpp {
    let file = scratch.join(file);
    std::fs::write(&file, contents).expect("the template writes");
    LlamaCpp::from(
        &[qwen3_gguf().await],
        LocalModelOptions {
            chat_template_file: Some(&file),
            reasoning: ReasoningDefault {
                model_name: "qwen3",
                effort: None,
            },
            ..LocalModelOptions::default()
        },
    )
    .await
    .expect("the model loads with the template")
}

/// A user message's prompt, with its content `content`, and with tools when `with_tools`.
fn user_prompt(model: &LlamaCpp, content: &Value, with_tools: bool) -> String {
    let mut body = json!({
        "model": "qwen3",
        "messages": [{ "role": "user", "content": content }],
        "max_tokens": 1,
    });
    if with_tools {
        body["tools"] = json!([{ "type": "function", "function": { "name": "lookup", "parameters": { "type": "object", "properties": {} } } }]);
    }
    model
        .render_prompt(&request(body))
        .unwrap_or_else(|e| panic!("{content} renders: {e}"))
}

/// Renders each message's `content` as it stands, as a minimal template does: given parts, it
/// would print the array itself, so the parts reach it as their text joined with newlines.
const AS_IT_STANDS: &str = "{% for message in messages %}{{ message['content'] }}{% endfor %}";

/// Reads only an array of text parts, as some templates do.
const PARTS_ONLY: &str = "{% for message in messages %}{% if message['content'] is string %}{{ raise_exception('Content must be a list of parts.') }}{% endif %}{% for part in message['content'] %}{{ part['text'] }}{% endfor %}{% endfor %}";

/// Reads a string, and only the first of an array's parts, as a template that takes a system
/// message's first part does, and as mistral.rs's vision pipelines take a message's text.
const FIRST_PART: &str = "{% for message in messages %}{% if message['content'] is string %}{{ message['content'] }}{% else %}{{ message['content'][0]['text'] }}{% endif %}{% endfor %}";

#[tokio::test]
async fn a_template_that_prints_content_as_it_stands_gets_parts_joined() {
    let scratch = tempfile::tempdir().expect("a scratch directory");
    let model = with_template(scratch.path(), "chat_template.jinja", AS_IT_STANDS).await;
    assert_eq!(
        user_prompt(&model, &text_parts(&[A, B]), false),
        format!("{A}\n{B}")
    );
}

#[tokio::test]
async fn a_template_that_reads_only_parts_gets_a_string_as_one_part() {
    let scratch = tempfile::tempdir().expect("a scratch directory");
    let model = with_template(scratch.path(), "chat_template.jinja", PARTS_ONLY).await;
    assert_eq!(user_prompt(&model, &json!(A), false), A);
    assert_eq!(
        user_prompt(&model, &text_parts(&[A, B]), false),
        format!("{A}{B}")
    );
}

#[tokio::test]
async fn a_template_that_reads_only_the_first_part_gets_parts_joined() {
    let scratch = tempfile::tempdir().expect("a scratch directory");
    let model = with_template(scratch.path(), "chat_template.jinja", FIRST_PART).await;
    assert_eq!(
        user_prompt(&model, &text_parts(&[A, B]), false),
        format!("{A}\n{B}")
    );
}

/// Named templates are probed apart: a request renders with `tool_use` when it offers tools,
/// and with `default` when it does not, and each gets its content in the shape its template
/// reads.
#[tokio::test]
async fn named_templates_each_get_the_content_they_read() {
    let scratch = tempfile::tempdir().expect("a scratch directory");
    let named = json!({
        "chat_template": [
            { "name": "default", "template": AS_IT_STANDS },
            { "name": "tool_use", "template": PARTS_ONLY },
        ],
    });
    let model = with_template(scratch.path(), "tokenizer_config.json", &named.to_string()).await;
    let parts = text_parts(&[A, B]);
    assert_eq!(user_prompt(&model, &parts, false), format!("{A}\n{B}"));
    assert_eq!(user_prompt(&model, &parts, true), format!("{A}{B}"));
    assert_eq!(user_prompt(&model, &json!(A), true), A);
}

/// A part other than text is refused, since the engine gives the chat template only text: an
/// image would otherwise reach Qwen3.5's template as its placeholder, with no image behind it.
#[tokio::test]
async fn a_part_other_than_text_is_refused() {
    let model = load(&qwen35_gguf().await, "qwen3.5").await;
    let req = request(json!({
        "model": "qwen3.5",
        "messages": [{ "role": "user", "content": [
            { "type": "text", "text": "What is in this picture?" },
            { "type": "image_url", "image_url": { "url": "https://example.com/cat.png" } },
        ] }],
        "reasoning_effort": "none",
        "max_tokens": 1,
    }));
    let message = match model.chat_request(req).await {
        Err(OpenAIError::InvalidArgument(message)) => message,
        Err(e) => panic!("an image part is refused as an invalid argument, not {e:?}"),
        Ok(reply) => panic!(
            "an image part is refused, but the model replied, from a prompt of {:?} tokens",
            reply.usage.map(|usage| usage.prompt_tokens)
        ),
    };
    assert_eq!(
        message,
        "Model 'qwen3.5' reads only text, so it cannot take the `image_url` part of a user message. Send the message's content as text."
    );
}
