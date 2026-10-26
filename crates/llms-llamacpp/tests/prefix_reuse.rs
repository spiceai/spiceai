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

//! A conversation's next turn repeats its prompt so far, which the KV cache already holds, so
//! the engine decodes only what the turn adds. Greedy, each reply must be the one the model
//! gives when it decodes the whole prompt from an empty cache, as it does freshly loaded.

use std::path::Path;

use llms::chat::{Chat, LocalModelOptions, ReasoningDefault};
use llms_llamacpp::LlamaCpp;
use serde_json::{Value, json};

use crate::request;

/// The user's messages, each about the reply before it.
const USER_TURNS: &[&str] = &[
    "Name three colors of the rainbow, separated by commas.",
    "Which of those is closest to violet? Answer in one short sentence.",
    "Spell that color backwards, letter by letter, separated by spaces.",
];

/// How many tokens a chat template's generation prompt adds after `<|im_start|>assistant\n`
/// without reasoning, `<think>\n\n</think>\n\n`, which Qwen's templates leave out of an earlier
/// assistant turn: the next prompt shares the last one up to there.
const EMPTY_THINK_TOKENS: usize = 4;

/// How many tokens before a prompt's end a model with a recurrent state checkpoints it, as the
/// engine's `CHECKPOINT_BEFORE_END`.
const CHECKPOINT_BEFORE_END: usize = 4;

/// unsloth's Gemma 3 270M at a pinned revision: sliding-window attention, small enough for CI.
const GEMMA_FILE: &str = "gemma-3-270m-it-Q8_0.gguf";

/// One request of the conversation, and what came of it.
struct Turn {
    messages: Vec<Value>,
    reply: String,
    prompt_tokens: usize,
    reused: usize,
}

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

/// The greedy reply to `messages`, and how many tokens its prompt is.
async fn ask(model: &LlamaCpp, name: &str, messages: &[Value]) -> (String, usize) {
    let reply = model
        .chat_request(request(json!({
            "model": name,
            "messages": messages,
            "temperature": 0,
            "reasoning_effort": "none",
            "max_tokens": 32,
        })))
        .await
        .expect("a reply");
    let usage = reply.usage.expect("usage");
    let content = reply.choices[0].message.content.clone().expect("content");
    let prompt_tokens = usize::try_from(usage.prompt_tokens).expect("a prompt length fits");
    (content, prompt_tokens)
}

/// Asks `messages` of `model`, recording the reply and how much of its prompt was reused.
async fn record(model: &LlamaCpp, name: &str, messages: Vec<Value>) -> Turn {
    let (reply, prompt_tokens) = ask(model, name, &messages).await;
    Turn {
        messages,
        reply,
        prompt_tokens,
        reused: model.reused_prompt_tokens(),
    }
}

/// The conversation on one loaded model, then its last turn again.
async fn converse(model: &LlamaCpp, name: &str) -> Vec<Turn> {
    let mut messages = Vec::new();
    let mut turns = Vec::new();
    for &user in USER_TURNS {
        messages.push(json!({ "role": "user", "content": user }));
        let turn = record(model, name, messages.clone()).await;
        messages.push(json!({ "role": "assistant", "content": turn.reply }));
        turns.push(turn);
    }
    messages.pop();
    turns.push(record(model, name, messages).await);
    turns
}

/// Holds the conversation, asks each turn again of a freshly loaded model, whose empty cache
/// reuses nothing, for the same reply, then checks how many prompt tokens each turn reused
/// against `expected`, given the turns' prompt lengths.
async fn replies_as_from_an_empty_cache(
    gguf: &Path,
    name: &'static str,
    expected: impl Fn(&[usize]) -> Vec<usize>,
) {
    let model = load(gguf, name).await;
    let turns = converse(&model, name).await;
    drop(model);
    for turn in &turns {
        eprintln!(
            "{name}: {} prompt tokens, reused {}: {:?}",
            turn.prompt_tokens, turn.reused, turn.reply
        );
    }
    for (i, turn) in turns.iter().enumerate() {
        let fresh = load(gguf, name).await;
        let (reply, prompt_tokens) = ask(&fresh, name, &turn.messages).await;
        assert_eq!(
            fresh.reused_prompt_tokens(),
            0,
            "a freshly loaded model reuses nothing"
        );
        assert_eq!(prompt_tokens, turn.prompt_tokens, "turn {i}'s prompt");
        assert_eq!(reply, turn.reply, "turn {i}'s reply from an empty cache");
    }
    let prompt_tokens: Vec<usize> = turns.iter().map(|turn| turn.prompt_tokens).collect();
    let reused: Vec<usize> = turns.iter().map(|turn| turn.reused).collect();
    assert_eq!(
        reused,
        expected(&prompt_tokens),
        "prompt tokens {prompt_tokens:?}"
    );
}

/// Qwen3-0.6B keeps only an attention cache, which truncates to any position: each turn reuses
/// the whole prompt it shares with the last one, and the repeated turn all of its prompt but the
/// last token, whose logits it samples from.
#[tokio::test]
async fn an_attention_cache_reuses_the_shared_prompt() {
    let gguf = crate::qwen3_gguf().await;
    replies_as_from_an_empty_cache(&gguf, "qwen3-0.6b-prefix-reuse", |prompts| {
        vec![
            0,
            prompts[0] - EMPTY_THINK_TOKENS,
            prompts[1] - EMPTY_THINK_TOKENS,
            prompts[3] - 1,
        ]
    })
    .await;
}

/// Qwen3.5-0.8B's gated delta net layers keep a recurrent state, which rolls back only over
/// the two tokens its MTP head drafts, never over a reply: each turn restores the checkpoint
/// saved just before the last prompt's generation prompt and decodes from there, its MTP head's
/// cache truncated to the same position.
#[tokio::test]
async fn a_recurrent_state_resumes_from_its_checkpoint() {
    let gguf = crate::mtp::qwen35_gguf().await;
    replies_as_from_an_empty_cache(&gguf, "qwen3.5-prefix-reuse", |prompts| {
        vec![
            0,
            prompts[0] - CHECKPOINT_BEFORE_END,
            prompts[1] - CHECKPOINT_BEFORE_END,
            prompts[3] - CHECKPOINT_BEFORE_END,
        ]
    })
    .await;
}

/// Gemma 3 270M attends over a 512-position sliding window in five of every six layers, and
/// llama.cpp keeps that cache at full size, so a truncation leaves every window whole: each
/// turn reuses everything it shares with the cache, as an attention cache does.
#[tokio::test]
async fn a_sliding_window_cache_reuses_the_shared_prompt() {
    let gguf = llms::chat::hf_files::download_model(
        "unsloth/gemma-3-270m-it-GGUF:c90975dbd40c0c7b275fefaae758c3415c906238",
        &[GEMMA_FILE],
        llms::hf::HubParams::default(),
        "gemma-3-270m",
    )
    .await
    .expect("Gemma 3 270M downloads")
    .join(GEMMA_FILE);
    // Gemma's template repeats a reply as it was generated, so a turn shares the last prompt and
    // its reply: llama-server (`--swa-full`, the same full-size window) reports `cache_n`
    // 0/40/65/87 on this conversation, and 0/15/59/83 with its default sliding window.
    replies_as_from_an_empty_cache(&gguf, "gemma-3-270m-prefix-reuse", |prompts| {
        assert_eq!(prompts, [20, 64, 88, 88], "the conversation's prompts");
        vec![0, 40, 65, 87]
    })
    .await;
}

/// `n` choices run one after another on the same prompt, so every choice after the first
/// reuses the whole prompt but its last token, and greedy, all choices are the same reply.
#[tokio::test]
async fn each_further_choice_reuses_the_prompt() {
    let gguf = crate::qwen3_gguf().await;
    let model = load(&gguf, "qwen3-0.6b-choices").await;
    let reply = model
        .chat_request(request(json!({
            "model": "qwen3-0.6b-choices",
            "messages": [{ "role": "user", "content": USER_TURNS[0] }],
            "temperature": 0,
            "reasoning_effort": "none",
            "max_tokens": 32,
            "n": 2,
        })))
        .await
        .expect("a reply");
    let prompt_tokens =
        usize::try_from(reply.usage.expect("usage").prompt_tokens).expect("a prompt length fits");
    let texts: Vec<_> = reply
        .choices
        .iter()
        .map(|choice| choice.message.content.clone().expect("content"))
        .collect();
    assert_eq!(texts.len(), 2);
    assert_eq!(texts[0], texts[1], "greedy choices are the same reply");
    assert_eq!(model.reused_prompt_tokens(), prompt_tokens - 1);
}
