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

//! A system or developer message after the conversation has started, as Claude Code sends its
//! environment after the first user message, reaches the model with its text whatever the chat
//! template does with one: in place for a template that renders it there, as Qwen3's does, and
//! otherwise joined to the user message before it, or as a user message of its own after a
//! tool result. Qwen3.5's template leaves such a message out of the prompt, and some templates
//! raise `System message must be at the beginning`.

use llms::chat::Chat;
use serde_json::{Value, json};

use crate::content_parts::{load, with_template};
use crate::{mtp::qwen35_gguf, qwen3_gguf, request};

const SECRET: &str = "The secret word is PLUM.";
const QUESTION: &str = "What is the secret word? Answer with the word only.";

/// A conversation that starts with a system message, as Claude Code's does, and has a message
/// of `role` holding [`SECRET`] after its first user message.
fn later(model: &str, role: &str, first: &Value) -> Value {
    json!({
        "model": model,
        "messages": [
            { "role": "system", "content": "Be brief." },
            { "role": "user", "content": first },
            { "role": role, "content": SECRET },
            { "role": "user", "content": QUESTION },
        ],
        "reasoning_effort": "none",
        "temperature": 0,
        "max_tokens": 8,
    })
}

/// Qwen3.5's template renders only the system and developer messages the conversation starts
/// with, and leaves a later one out. Here the later message's text is joined to the user
/// message before it, after a blank line, so the model reads it and answers from it.
#[tokio::test]
async fn qwen3_5_reads_a_later_system_message_in_the_user_message_before_it() {
    let model = load(&qwen35_gguf().await, "qwen3.5").await;
    for role in ["system", "developer"] {
        let req = request(later("qwen3.5", role, &json!("Hi.")));
        assert_eq!(
            model.render_prompt(&req).expect("the conversation renders"),
            "<|im_start|>system\nBe brief.<|im_end|>\n<|im_start|>user\nHi.\n\nThe secret word is PLUM.<|im_end|>\n<|im_start|>user\nWhat is the secret word? Answer with the word only.<|im_end|>\n<|im_start|>assistant\n<think>\n\n</think>\n\n",
            "a later {role} message"
        );
        let reply = model.chat_request(req).await.unwrap_or_else(|e| {
            panic!("a reply to a conversation with a later {role} message: {e}")
        });
        let answer = reply.choices[0].message.content.clone().expect("content");
        assert!(
            answer.to_uppercase().contains("PLUM"),
            "the model answers from the later {role} message: {answer:?}"
        );
    }

    // A user message of text parts, as Claude Code sends its first one: the later message's
    // text follows them as a part of its own, and Qwen3.5 renders parts one after another.
    let parts = json!([{ "type": "text", "text": "Hi." }, { "type": "text", "text": "Be quick." }]);
    let req = request(later("qwen3.5", "system", &parts));
    let prompt = model.render_prompt(&req).expect("the conversation renders");
    assert!(
        prompt.contains("<|im_start|>user\nHi.Be quick.\n\nThe secret word is PLUM.<|im_end|>\n"),
        "the later message follows the user message's parts: {prompt:?}"
    );
}

/// Qwen3's template renders a later system message in place, as a system turn, so it reaches
/// the template as it came.
#[tokio::test]
async fn qwen3_renders_a_later_system_message_in_place() {
    let model = load(&qwen3_gguf().await, "qwen3").await;
    let req = request(later("qwen3", "system", &json!("Hi.")));
    assert_eq!(
        model.render_prompt(&req).expect("the conversation renders"),
        "<|im_start|>system\nBe brief.<|im_end|>\n<|im_start|>user\nHi.<|im_end|>\n<|im_start|>system\nThe secret word is PLUM.<|im_end|>\n<|im_start|>user\nWhat is the secret word? Answer with the word only.<|im_end|>\n<|im_start|>assistant\n<think>\n\n</think>\n\n"
    );
}

/// Raises on a system message that is not the conversation's first, as Qwen3.8's template does
/// (`System message must be at the beginning.`), and prints each message as `<role>content`.
const AT_THE_START_ONLY: &str = "{%- for message in messages %}{%- if message.role == 'system' and not loop.first %}{{- raise_exception('System message must be at the beginning.') }}{%- endif %}{{- '<' ~ message.role ~ '>' ~ (message.content if message.content is string else '') }}{%- endfor %}";

/// A template that raises on a later system message gets the message's text in the user
/// message before it, and, after a tool result, as a user message of its own. A later developer
/// message, which it renders in place, reaches it as it came.
#[tokio::test]
async fn a_template_that_refuses_a_later_system_message_gets_it_as_a_user_s() {
    let scratch = tempfile::tempdir().expect("a scratch directory");
    let model = with_template(scratch.path(), "chat_template.jinja", AT_THE_START_ONLY).await;

    let req = request(later("qwen3", "system", &json!("Hi.")));
    assert_eq!(
        model.render_prompt(&req).expect("the conversation renders"),
        format!("<system>Be brief.<user>Hi.\n\n{SECRET}<user>{QUESTION}")
    );
    let req = request(later("qwen3", "developer", &json!("Hi.")));
    assert_eq!(
        model.render_prompt(&req).expect("the conversation renders"),
        format!("<system>Be brief.<user>Hi.<developer>{SECRET}<user>{QUESTION}")
    );

    let after_a_tool_result = request(json!({
        "model": "qwen3",
        "messages": [
            { "role": "system", "content": "Be brief." },
            { "role": "user", "content": "Look the word up." },
            {
                "role": "assistant",
                "content": "",
                "tool_calls": [{ "id": "c1", "type": "function", "function": { "name": "lookup", "arguments": "{}" } }],
            },
            { "role": "tool", "tool_call_id": "c1", "content": "PLUM" },
            { "role": "system", "content": SECRET },
        ],
        "tools": [{
            "type": "function",
            "function": { "name": "lookup", "description": "Looks a word up.", "parameters": { "type": "object", "properties": {} } },
        }],
        "max_tokens": 1,
    }));
    assert_eq!(
        model
            .render_prompt(&after_a_tool_result)
            .expect("the conversation renders"),
        format!("<system>Be brief.<user>Look the word up.<assistant><tool>PLUM<user>{SECRET}")
    );
}
