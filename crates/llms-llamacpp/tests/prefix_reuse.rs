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

/// The positions Gemma 3 270M's sliding-window layers attend over.
const SLIDING_WINDOW: usize = 512;

/// `lines` numbered lines of filler, a long prompt's bulk.
fn notes(lines: usize) -> String {
    use std::fmt::Write;
    let mut notes = String::new();
    for i in 1..=lines {
        let _ = write!(notes, "Line {i} of the notes says nothing that matters. ");
    }
    notes
}

pub(crate) async fn gemma_gguf() -> std::path::PathBuf {
    llms::chat::hf_files::download_model(
        "unsloth/gemma-3-270m-it-GGUF:c90975dbd40c0c7b275fefaae758c3415c906238",
        &[GEMMA_FILE],
        llms::chat::hf_files::Budget::Unmeasured,
        llms::hf::HubParams::default(),
        "gemma-3-270m",
    )
    .await
    .expect("Gemma 3 270M downloads")
    .dir
    .join(GEMMA_FILE)
}

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
    ask_up_to(model, name, messages, Some(32)).await
}

/// The greedy reply to `messages` of up to `max_tokens` tokens, or with no output limit when
/// `None`, and how many tokens its prompt is.
async fn ask_up_to(
    model: &LlamaCpp,
    name: &str,
    messages: &[Value],
    max_tokens: Option<u32>,
) -> (String, usize) {
    let mut body = json!({
        "model": name,
        "messages": messages,
        "temperature": 0,
        "reasoning_effort": "none",
    });
    if let Some(max_tokens) = max_tokens {
        body["max_tokens"] = json!(max_tokens);
    }
    let reply = model.chat_request(request(body)).await.expect("a reply");
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

/// The conversation on one loaded model, its first message led by `preamble`, then its last
/// turn again.
async fn converse(model: &LlamaCpp, name: &str, preamble: &str) -> Vec<Turn> {
    let mut messages = Vec::new();
    let mut turns = Vec::new();
    for (i, &user) in USER_TURNS.iter().enumerate() {
        let content = if i == 0 {
            format!("{preamble}{user}")
        } else {
            user.to_string()
        };
        messages.push(json!({ "role": "user", "content": content }));
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
    let (prompt_tokens, reused) = held_and_fresh_replies_agree(gguf, name, "").await;
    assert_eq!(
        reused,
        expected(&prompt_tokens),
        "prompt tokens {prompt_tokens:?}"
    );
}

/// Holds the conversation, its first message led by `preamble`, and asks each turn again of a
/// freshly loaded model, whose empty cache reuses nothing, for the same reply. Returns each
/// turn's prompt length and how many of its tokens it reused.
async fn held_and_fresh_replies_agree(
    gguf: &Path,
    name: &'static str,
    preamble: &str,
) -> (Vec<usize>, Vec<usize>) {
    let model = load(gguf, name).await;
    let turns = converse(&model, name, preamble).await;
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
    (prompt_tokens, reused)
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
    let gguf = gemma_gguf().await;
    // Gemma's template repeats a reply as it was generated, so a turn shares the last prompt and
    // its reply: llama-server (`--swa-full`, the same full-size window) reports `cache_n`
    // 0/40/65/87 on this conversation, and 0/15/59/83 with its default sliding window.
    replies_as_from_an_empty_cache(&gguf, "gemma-3-270m-prefix-reuse", |prompts| {
        assert_eq!(prompts, [20, 64, 88, 88], "the conversation's prompts");
        vec![0, 40, 65, 87]
    })
    .await;
}

/// Gemma 3 270M's sliding window holds 512 positions. With a first message long enough that
/// every prompt passes the window, each turn still reuses the whole of the last prompt, more
/// than a window of it, and replies as a freshly loaded model does, and the repeated turn reuses
/// all of its prompt but the last token.
#[tokio::test]
async fn a_sliding_window_cache_reuses_past_its_window() {
    let gguf = gemma_gguf().await;
    let preamble = notes(200) + "\n\n";
    let (prompts, reused) =
        held_and_fresh_replies_agree(&gguf, "gemma-3-270m-past-window", &preamble).await;
    eprintln!("prompts {prompts:?}, reused {reused:?}");
    assert!(prompts[0] > SLIDING_WINDOW, "prompts {prompts:?}");
    assert_eq!(reused[0], 0, "reused {reused:?}");
    for i in 1..3 {
        assert!(
            reused[i] >= prompts[i - 1] && reused[i] < prompts[i],
            "turn {i} reuses the whole last prompt: prompts {prompts:?}, reused {reused:?}"
        );
    }
    assert_eq!(
        reused[3],
        prompts[3] - 1,
        "the repeated turn: reused {reused:?}"
    );
}

/// A prompt that shares the start of a cache longer than Gemma 3 270M's window, but not its
/// last window, cuts the cache back by more than a window: the sliding-window layers must still
/// hold every position the shared prefix ends on, as a cache kept at full size does, so the
/// reply is a freshly loaded model's. It shares most of the cache, as a request that would keep
/// less than half of a cache takes an empty one instead.
#[tokio::test]
async fn a_sliding_window_cache_is_cut_back_more_than_a_window() {
    let gguf = gemma_gguf().await;
    let name = "gemma-3-270m-cut-back";
    let long = vec![json!({
        "role": "user",
        "content": format!("{}\n\nName three colors of the rainbow, separated by commas.", notes(200)),
    })];
    let short = vec![json!({
        "role": "user",
        "content": format!("{}\n\nWhich planet is closest to the Sun? Reply with one word.", notes(120)),
    })];
    let model = load(&gguf, name).await;
    let (_, long_tokens) = ask(&model, name, &long).await;
    let (held, short_tokens) = ask(&model, name, &short).await;
    let reused = model.reused_prompt_tokens();
    drop(model);
    eprintln!("long {long_tokens}, short {short_tokens}, reused {reused}: {held:?}");
    assert!(
        long_tokens > short_tokens + SLIDING_WINDOW,
        "the cache is cut back by more than a window: {long_tokens} and {short_tokens} tokens"
    );
    assert!(
        reused > 0 && reused < short_tokens,
        "the short prompt reuses the start it shares: {reused} of {short_tokens}"
    );
    let fresh = load(&gguf, name).await;
    let (cold, _) = ask(&fresh, name, &short).await;
    assert_eq!(
        held, cold,
        "the reply after the cut against a fresh model's"
    );
}

/// A request made as `caller`, an authenticated principal's cache namespace, as the runtime
/// scopes every request it serves.
async fn as_caller<F: std::future::Future>(caller: &str, f: F) -> F::Output {
    use runtime_request_context::{CacheNamespace, Protocol, RequestContext};
    std::sync::Arc::new(
        RequestContext::builder(Protocol::Http)
            .with_cache_namespace(CacheNamespace::Principal(std::sync::Arc::from(caller)))
            .build(),
    )
    .scope(f)
    .await
}

/// A cached prompt is reused only by the caller who sent it: one caller's conversation reuses
/// its own turns, and another caller sending the same messages reads none of them from the
/// cache, so the time a request takes says nothing about what another caller asked. Each
/// reply is the one an empty cache gives.
#[tokio::test]
async fn a_caller_reuses_only_its_own_prompts() {
    let gguf = crate::qwen3_gguf().await;
    let name = "qwen3-0.6b-per-caller";
    let model = load(&gguf, name).await;
    let first = vec![json!({ "role": "user", "content": USER_TURNS[0] })];
    let (reply, _) = as_caller("alice", ask(&model, name, &first)).await;
    let mut second = first.clone();
    second.push(json!({ "role": "assistant", "content": reply }));
    second.push(json!({ "role": "user", "content": USER_TURNS[1] }));

    let (alice, alice_tokens) = as_caller("alice", ask(&model, name, &second)).await;
    let alice_reused = model.reused_prompt_tokens();
    let (bob, bob_tokens) = as_caller("bob", ask(&model, name, &second)).await;
    let bob_reused = model.reused_prompt_tokens();
    let (again, _) = as_caller("bob", ask(&model, name, &second)).await;
    let bob_again = model.reused_prompt_tokens();
    eprintln!(
        "alice reused {alice_reused} of {alice_tokens}, bob {bob_reused} of {bob_tokens}, then {bob_again}"
    );

    assert!(
        alice_reused > 0 && alice_reused < alice_tokens,
        "alice's next turn reuses her first: {alice_reused} of {alice_tokens}"
    );
    assert_eq!(bob_reused, 0, "bob reads nothing alice's requests left");
    assert_eq!(bob_again, bob_tokens - 1, "bob reuses his own prompt");
    drop(model);
    let fresh = load(&gguf, name).await;
    let (cold, _) = ask(&fresh, name, &second).await;
    assert_eq!(alice, cold, "alice's reply against an empty cache's");
    assert_eq!(bob, cold, "bob's reply against an empty cache's");
    assert_eq!(again, cold, "bob's repeated reply against an empty cache's");
}

/// A conversation's prompt survives a request with another prefix, as a coding agent's
/// permission check or another conversation sends between two of its turns: the engine saves
/// the conversation's prompt to host memory while the other request runs, so the
/// conversation's next turn reads its whole shared prompt from the cache, reports it as
/// `cached_tokens`, and replies as from an empty cache.
#[tokio::test]
async fn a_conversation_keeps_its_prompt_across_an_unrelated_request() {
    let name = "qwen3-0.6b-cached-prompts";
    keeps_its_prompt_across(name, Some(32), async |model: &LlamaCpp| {
        // Another prefix, which with one cached prompt would replace the conversation's.
        let other = [json!({ "role": "user", "content": "Name one fruit." })];
        ask(model, name, &other).await;
    })
    .await;
}

/// A request that names no output limit reserves the whole context, which it needs only if it
/// replies at that length: the conversation's prompt waits in host memory meanwhile rather than
/// being dropped for room, so it survives a short unrelated reply with no limit.
#[tokio::test]
async fn a_conversation_keeps_its_prompt_across_requests_with_no_output_limit() {
    let name = "qwen3-0.6b-cached-prompts-no-limit";
    keeps_its_prompt_across(name, None, async |model: &LlamaCpp| {
        let other = [json!({ "role": "user", "content": "Name one fruit." })];
        ask_up_to(model, name, &other, None).await;
    })
    .await;
}

/// A request's greedy reply is the one an idle model gives, whatever conversations are cached,
/// and a conversation cached across more requests than the model runs at once still serves its
/// next turn from the cache: idle prompts wait in host memory, so none takes room in the shared
/// cache from the request, and none is dropped for it.
#[tokio::test]
async fn a_reply_is_the_one_an_idle_model_gives_whatever_prompts_are_cached() {
    let name = "qwen3-0.6b-cached-prompts-alone";
    let gguf = crate::qwen3_gguf().await;
    let model = load(&gguf, name).await;
    let conversation = vec![json!({
        "role": "user",
        "content": format!("{}{}", notes(300), USER_TURNS[0]),
    })];
    let (reply, first_tokens) = ask(&model, name, &conversation).await;
    for question in UNRELATED {
        ask(
            &model,
            name,
            &[json!({ "role": "user", "content": question })],
        )
        .await;
    }
    let probe = [json!({ "role": "user", "content": "Write a limerick about a cat." })];
    let (warm, _) = ask_up_to(&model, name, &probe, Some(64)).await;

    let mut next = conversation.clone();
    next.push(json!({ "role": "assistant", "content": reply }));
    next.push(json!({ "role": "user", "content": USER_TURNS[1] }));
    ask(&model, name, &next).await;
    let reused = model.reused_prompt_tokens();
    assert!(
        reused >= first_tokens - EMPTY_THINK_TOKENS,
        "the conversation's prompt was cached while the probe ran: {reused} of {first_tokens} reused"
    );
    drop(model);

    let fresh = load(&gguf, name).await;
    let (cold, _) = ask_up_to(&fresh, name, &probe, Some(64)).await;
    assert_eq!(
        warm, cold,
        "the probe's reply beside cached prompts and on an idle model"
    );
}

/// A request that continues only part of a conversation's saved prompt, as a coding agent's
/// classifier repeats a conversation's opening with its own question, leaves that prompt whole:
/// the conversation's next turn still reads all of it from the cache.
#[tokio::test]
async fn a_request_continuing_part_of_a_saved_prompt_leaves_it_whole() {
    let name = "qwen3-0.6b-cached-prompts-partial";
    keeps_its_prompt_across(name, Some(32), async |model: &LlamaCpp| {
        // moves the conversation's prompt out of the shared cache
        let other = [json!({ "role": "user", "content": UNRELATED[0] })];
        ask(model, name, &other).await;
        // repeats the conversation's notes, then asks its own question
        let partial = [json!({
            "role": "user",
            "content": format!("{}\n\nHow many lines do the notes have?", notes(60)),
        })];
        ask(model, name, &partial).await;
    })
    .await;
}

/// Questions that share no prefix with each other or with the conversation's, beyond a chat
/// template's opening tokens, so none continues another's prompt.
const UNRELATED: &[&str] = &[
    "What is the capital of France?",
    "Write a haiku about rain.",
    "Translate 'good morning' into Spanish.",
];

/// A request that extends the last prompt past the context fails, which empties its own
/// sequence and nothing saved to host memory: it could never fit, so the prompts saved while
/// unrelated requests ran keep their place. An unrelated request then runs, and the
/// conversation's prompt, saved longest ago, still serves its next turn.
#[tokio::test]
async fn a_request_too_long_for_the_context_leaves_the_saved_prompts() {
    let name = "qwen3-0.6b-cached-prompts-failed";
    keeps_its_prompt_across(name, Some(32), async |model: &LlamaCpp| {
        let others = UNRELATED.len() - 1;
        for question in &UNRELATED[..others] {
            ask(
                model,
                name,
                &[json!({ "role": "user", "content": question })],
            )
            .await;
        }
        // The last prompt, long beside its reply, so a request extending it continues it.
        let filler = "Paragraph of filler that says nothing. ".repeat(20);
        let last = [json!({ "role": "user", "content": format!("{filler}Name one tool.") })];
        ask(model, name, &last).await;
        let too_long = [json!({ "role": "user", "content": format!("{filler}{}", notes(1500)) })];
        let refused = model
            .chat_request(request(json!({
                "model": name,
                "messages": too_long,
                "temperature": 0,
                "reasoning_effort": "none",
                "max_tokens": 32,
            })))
            .await
            .expect_err("a prompt longer than the context is refused");
        assert!(
            refused
                .to_string()
                .contains("and the model's context holds"),
            "refused for its length: {refused}"
        );
        let unrelated = [json!({ "role": "user", "content": UNRELATED[others] })];
        ask(model, name, &unrelated).await;
    })
    .await;
}

/// Asks a conversation's first turn, runs `between` on the same model, then asks its next turn,
/// which must read the first turn's prompt from the cache, report it as `cached_tokens`, and
/// reply as from an empty cache. With `max_tokens` `None`, the turns name no output limit.
async fn keeps_its_prompt_across(
    name: &'static str,
    max_tokens: Option<u32>,
    between: impl AsyncFnOnce(&LlamaCpp),
) {
    let gguf = crate::qwen3_gguf().await;
    let model = load(&gguf, name).await;
    let first = vec![json!({
        "role": "user",
        "content": format!("{}\n\n{}", notes(60), USER_TURNS[0]),
    })];
    let (reply, first_tokens) = ask_up_to(&model, name, &first, max_tokens).await;
    between(&model).await;

    let mut next = first.clone();
    next.push(json!({ "role": "assistant", "content": reply }));
    next.push(json!({ "role": "user", "content": USER_TURNS[1] }));
    let mut body = json!({
        "model": name,
        "messages": next,
        "temperature": 0,
        "reasoning_effort": "none",
    });
    if let Some(max_tokens) = max_tokens {
        body["max_tokens"] = json!(max_tokens);
    }
    let answer = model.chat_request(request(body)).await.expect("a reply");
    let reused = model.reused_prompt_tokens();
    let cached = answer
        .usage
        .as_ref()
        .and_then(|usage| usage.prompt_tokens_details.as_ref())
        .and_then(|details| details.cached_tokens);
    eprintln!("the next turn reused {reused} tokens; the first turn's prompt was {first_tokens}");
    assert!(
        reused >= first_tokens - EMPTY_THINK_TOKENS,
        "the next turn reads the first turn's prompt from the cache: {reused} of {first_tokens}"
    );
    assert_eq!(
        cached,
        Some(u32::try_from(reused).expect("a count that fits")),
        "the usage reports the tokens read from the cache"
    );
    let reply = answer.choices[0].message.content.clone().expect("content");
    drop(model);
    let fresh = load(&gguf, name).await;
    let (cold, _) = ask_up_to(&fresh, name, &next, max_tokens).await;
    assert_eq!(reply, cold, "the reply against an empty cache's");
}

/// Asks `messages` of `model` as a streamed request: the reply's text joined from its chunks,
/// and its prompt length from the usage the stream reports.
async fn ask_streamed(model: &LlamaCpp, name: &str, messages: &[Value]) -> (String, usize) {
    use futures::StreamExt;
    let mut stream = model
        .chat_stream(request(json!({
            "model": name,
            "messages": messages,
            "temperature": 0,
            "reasoning_effort": "none",
            "max_tokens": 32,
            "stream": true,
        })))
        .await
        .expect("a stream");
    let (mut content, mut usage) = (String::new(), None);
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.expect("a chunk");
        usage = chunk.usage.or(usage);
        for choice in chunk.choices {
            content.push_str(choice.delta.content.as_deref().unwrap_or_default());
        }
    }
    let prompt_tokens = usage.expect("usage").prompt_tokens;
    let prompt_tokens = usize::try_from(prompt_tokens).expect("a prompt length fits");
    (content, prompt_tokens)
}

/// A streamed request is scoped to its caller as a whole reply is: `chat_stream` reads the
/// caller's cache namespace on its own path, so another caller's streamed request reads none
/// of a conversation's cached turns. Each streamed reply is the one an empty cache gives.
#[tokio::test]
async fn a_streamed_request_reuses_only_its_callers_prompts() {
    let gguf = crate::qwen3_gguf().await;
    let name = "qwen3-0.6b-per-caller-streamed";
    let model = load(&gguf, name).await;
    let first = vec![json!({ "role": "user", "content": USER_TURNS[0] })];
    let (reply, _) = as_caller("alice", ask_streamed(&model, name, &first)).await;
    let mut second = first.clone();
    second.push(json!({ "role": "assistant", "content": reply }));
    second.push(json!({ "role": "user", "content": USER_TURNS[1] }));

    let (alice, alice_tokens) = as_caller("alice", ask_streamed(&model, name, &second)).await;
    let alice_reused = model.reused_prompt_tokens();
    let (bob, bob_tokens) = as_caller("bob", ask_streamed(&model, name, &second)).await;
    let bob_reused = model.reused_prompt_tokens();
    eprintln!(
        "streamed: alice reused {alice_reused} of {alice_tokens}, bob {bob_reused} of {bob_tokens}"
    );

    assert!(
        alice_reused > 0 && alice_reused < alice_tokens,
        "alice's next streamed turn reuses her first: {alice_reused} of {alice_tokens}"
    );
    assert_eq!(
        bob_reused, 0,
        "bob's streamed request reads nothing alice's requests left"
    );
    drop(model);
    let fresh = load(&gguf, name).await;
    let (cold, _) = ask(&fresh, name, &second).await;
    assert_eq!(
        alice, cold,
        "alice's streamed reply against an empty cache's"
    );
    assert_eq!(bob, cold, "bob's streamed reply against an empty cache's");
}

/// On a model that serves one request at a time, as one whose memory holds no more does, `n`
/// choices run one after another on the same prompt, so every choice after the first reuses the
/// whole prompt but its last token, and greedy, all choices are the same reply.
#[tokio::test]
async fn each_further_choice_reuses_the_prompt() {
    let gguf = crate::qwen3_gguf().await;
    let model = LlamaCpp::from(
        std::slice::from_ref(&gguf),
        LocalModelOptions {
            reasoning: ReasoningDefault {
                model_name: "qwen3-0.6b-choices",
                effort: None,
            },
            concurrent_requests: std::num::NonZeroU32::new(1),
            ..LocalModelOptions::default()
        },
    )
    .await
    .expect("the model loads on llama.cpp");
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

/// A template that renders each message's text and nothing else, so a prompt is exactly the text
/// asked, and the next prompt can extend it by a single token.
const RAW_TEMPLATE: &str = "{% for message in messages %}{{ message['content'] }}{% endfor %}";

/// The greedy continuation of `text`, rendered as it stands, and how many tokens its prompt is.
async fn continue_text(
    model: &LlamaCpp,
    name: &str,
    text: &str,
    max_tokens: u32,
) -> (String, usize) {
    let reply = model
        .chat_request(request(json!({
            "model": name,
            "messages": [{ "role": "user", "content": text }],
            "temperature": 0,
            "max_tokens": max_tokens,
        })))
        .await
        .expect("a reply");
    let usage = reply.usage.expect("usage");
    let content = reply.choices[0].message.content.clone().unwrap_or_default();
    let prompt_tokens = usize::try_from(usage.prompt_tokens).expect("a prompt length fits");
    (content, prompt_tokens)
}

/// A recurrent state rolls back only through snapshots of its last decode's tokens, which
/// llama.cpp keeps for rejected drafts. Once a prompt that extends the cache by one token is
/// read, rolling that token back would restore a snapshot from the decode before it, a state
/// one token short. So the same prompt again restores the checkpoint saved before the shorter
/// prompt's end, as llama.cpp's server does, and continues as it does from an empty cache.
#[tokio::test]
async fn a_recurrent_state_is_not_rolled_back_past_its_last_decode() {
    let gguf = crate::mtp::qwen35_gguf().await;
    let dir = tempfile::tempdir().expect("a temporary directory");
    let template = dir.path().join("raw.jinja");
    std::fs::write(&template, RAW_TEMPLATE).expect("the template is written");
    let name = "qwen3.5-rollback";
    let load = || async {
        LlamaCpp::from(
            std::slice::from_ref(&gguf),
            LocalModelOptions {
                chat_template_file: Some(&template),
                reasoning: ReasoningDefault {
                    model_name: name,
                    effort: None,
                },
                ..LocalModelOptions::default()
            },
        )
        .await
        .expect("the model loads on llama.cpp")
    };
    // Rolled back by its last token instead, the repeat continues ' had a very long and very long
    // tail', where an empty cache continues ' was born.'
    let shorter = "Once upon a time, a dragon";
    let longer = format!("{shorter} A");

    let model = load().await;
    let mut reused = Vec::new();
    let (_, shorter_tokens) = continue_text(&model, name, shorter, 1).await;
    reused.push(model.reused_prompt_tokens());
    let (_, longer_tokens) = continue_text(&model, name, &longer, 1).await;
    reused.push(model.reused_prompt_tokens());
    assert_eq!(
        longer_tokens,
        shorter_tokens + 1,
        "the longer prompt extends the shorter by one token"
    );
    let (warm, _) = continue_text(&model, name, &longer, 24).await;
    reused.push(model.reused_prompt_tokens());
    drop(model);

    let fresh = load().await;
    let (cold, _) = continue_text(&fresh, name, &longer, 24).await;
    assert_eq!(
        fresh.reused_prompt_tokens(),
        0,
        "a freshly loaded model reuses nothing"
    );
    eprintln!(
        "reused {reused:?} of {shorter_tokens}/{longer_tokens}; warm {warm:?}; cold {cold:?}"
    );
    assert_eq!(
        warm, cold,
        "the continuation from the cache is the one from an empty cache"
    );
    assert_eq!(
        reused,
        [0, shorter_tokens, shorter_tokens - CHECKPOINT_BEFORE_END],
        "the longer prompt reads only its new token, then its repeat resumes from the checkpoint"
    );
}
