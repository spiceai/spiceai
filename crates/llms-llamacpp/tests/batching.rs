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

//! Several requests served together, each in its own sequence of one shared cache, against the
//! same requests served one at a time: the engine's path when it holds one sequence, which the
//! batching code does not run.

use std::num::NonZeroU32;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use futures::StreamExt;
use llms::chat::{Chat, LocalModelOptions, ReasoningDefault};
use llms_llamacpp::LlamaCpp;
use serde_json::{Value, json};

use crate::mtp::qwen35_gguf;
use crate::{qwen3_gguf, request};

/// Prompts whose replies Qwen3-0.6B gives with wide margins at every token, so serving them in a
/// batch of another shape cannot flip a near tie: a difference is the scheduler's. The smallest
/// gap between the top two logits over each greedy reply, read alone, is 4.207, 2.727, 3.488 and
/// 3.024.
const PROMPTS: [&str; 4] = [
    "Count from 1 to 40, separated by commas. Reply with only the numbers.",
    "List the letters of the English alphabet in order, separated by spaces. Reply with only the letters.",
    "List the days of the week starting from Monday, one per line. Reply with only the days.",
    "List the months of the year starting from January, one per line. Reply with only the months.",
];

async fn load(gguf: &Path, name: &'static str, concurrent: u32, context: usize) -> Arc<dyn Chat> {
    load_engine(gguf, name, concurrent, context).await
}

async fn load_engine(
    gguf: &Path,
    name: &'static str,
    concurrent: u32,
    context: usize,
) -> Arc<LlamaCpp> {
    Arc::new(
        LlamaCpp::from(
            &[gguf.to_path_buf()],
            LocalModelOptions {
                reasoning: ReasoningDefault {
                    model_name: name,
                    effort: None,
                },
                context_length: Some(context),
                concurrent_requests: NonZeroU32::new(concurrent),
                ..LocalModelOptions::default()
            },
        )
        .await
        .expect("the model loads"),
    )
}

fn body(name: &str, prompt: &str, max_tokens: u32) -> Value {
    json!({
        "model": name,
        "messages": [{ "role": "user", "content": prompt }],
        "temperature": 0,
        "reasoning_effort": "none",
        "max_tokens": max_tokens,
    })
}

async fn reply(model: &Arc<dyn Chat>, body: Value) -> String {
    let reply = model.chat_request(request(body)).await.expect("a reply");
    reply.choices[0].message.content.clone().expect("content")
}

/// The same for Qwen3.5-0.8B, whose smallest gaps are 1.707, 1.525, 1.749 and 2.144. The
/// alphabet is not among them: its first token is a near tie there, "abc" against "A" by 0.002.
const PROMPTS_MTP: [&str; 4] = [
    "List the even numbers from 2 to 60, separated by commas. Reply with only the numbers.",
    "Write the multiples of 5 from 5 to 100, separated by commas. Reply with only the numbers.",
    "List the days of the week starting from Monday, one per line. Reply with only the days.",
    "List the months of the year starting from January, one per line. Reply with only the months.",
];

/// Each prompt's reply, served one at a time on a model that holds one sequence, then all at
/// once on one that holds as many as there are prompts.
async fn alone_and_together(
    gguf: &Path,
    name: &'static str,
    prompts: &[&str],
    max_tokens: u32,
    context: usize,
) -> (Vec<String>, Vec<String>) {
    let alone = {
        let model = load(gguf, name, 1, context).await;
        let mut replies = Vec::new();
        for prompt in prompts {
            replies.push(reply(&model, body(name, prompt, max_tokens)).await);
        }
        replies
    };
    let concurrent = u32::try_from(prompts.len()).expect("a count");
    let model = load(gguf, name, concurrent, context).await;
    let together = futures::future::join_all(
        prompts
            .iter()
            .map(|prompt| reply(&model, body(name, prompt, max_tokens))),
    )
    .await;
    (alone, together)
}

/// Requests served together reply exactly as each does alone, on a model without a draft head.
#[tokio::test]
async fn requests_served_together_reply_as_each_does_alone() {
    let gguf = qwen3_gguf().await;
    let (alone, together) = alone_and_together(&gguf, "qwen3-0.6b", &PROMPTS, 96, 4096).await;
    assert!(
        alone.iter().all(|reply| reply.len() > 20),
        "every reply has content: {alone:?}"
    );
    assert_eq!(together, alone, "replies served together, against alone");
}

/// Requests served together reply exactly as each does alone with an MTP head drafting for
/// every one of them in one pass.
#[tokio::test]
async fn requests_drafted_for_together_reply_as_each_does_alone() {
    let gguf = qwen35_gguf().await;
    let (alone, together) = alone_and_together(&gguf, "qwen3.5", &PROMPTS_MTP, 96, 4096).await;
    assert!(
        alone.iter().all(|reply| reply.len() > 20),
        "every reply has content: {alone:?}"
    );
    assert_eq!(together, alone, "replies served together, against alone");
}

/// Requests that do not all fit the shared cache at once still all reply, as each does alone:
/// each is admitted with room for its prompt and its whole reply, so the later ones wait for
/// the earlier ones instead of failing for lack of room. With a 1024-token context, each of
/// the four requests reserves its prompt and 320 reply tokens, more than a third of the context,
/// so at most two run at a time.
#[tokio::test]
async fn requests_beyond_the_cache_wait_their_turn_and_reply_as_alone() {
    let gguf = qwen3_gguf().await;
    let (alone, together) = alone_and_together(&gguf, "qwen3-0.6b", &PROMPTS, 320, 1024).await;
    assert_eq!(together, alone, "replies served together, against alone");
}

/// A request that needs the room idle sequences' caches hold gets it, even when no other request
/// runs. On a 1024-token context, two requests with prompts of about 450 tokens run together and
/// leave that much cached in each sequence; a third, whose prompt of about 680 tokens shares
/// nothing with them but the chat template's opening, fits only once the cache it does not reuse
/// is emptied. It replies as it does alone, rather
/// than failing when the cache has no room for its prompt.
#[tokio::test]
async fn idle_caches_give_up_their_room_to_a_request_that_needs_it() {
    let gguf = qwen3_gguf().await;
    let listing = |thing: &str, n: usize| -> String {
        let items: Vec<String> = (1..=n).map(|i| format!("{thing} {i} is ready.")).collect();
        format!("{} Reply with only the word OK.", items.join(" "))
    };
    let first = [listing("Box", 60), listing("Crate", 60)];
    let third = listing("Parcel", 90);
    let alone = {
        let model = load(&gguf, "qwen3-0.6b", 1, 1024).await;
        reply(&model, body("qwen3-0.6b", &third, 8)).await
    };
    let model = load(&gguf, "qwen3-0.6b", 2, 1024).await;
    let filled = futures::future::join_all(
        first
            .iter()
            .map(|prompt| reply(&model, body("qwen3-0.6b", prompt, 8))),
    )
    .await;
    assert!(
        filled.iter().all(|reply| !reply.is_empty()),
        "both earlier requests replied: {filled:?}"
    );
    let after = model
        .chat_request(request(body("qwen3-0.6b", &third, 8)))
        .await
        .map(|reply| reply.choices[0].message.content.clone().unwrap_or_default())
        .map_err(|e| e.to_string());
    assert_eq!(after, Ok(alone), "the third reply, against alone");
}

/// A conversation's next turn reuses its own prefix beside another request and replies as it does
/// alone. On Qwen3.5-0.8B, a hybrid model, reusing the prefix restores the recurrent-state
/// checkpoint turn 1 saved while its prompt was read in steps shared with the other request; a
/// checkpoint saved for the wrong sequence, or after the wrong tokens, changes turn 2's reply.
/// Turn 2 lists turn 1's days in reverse, so its reply depends on what the restored state holds.
/// The smallest top-two logit gaps over the replies, read alone, are 1.749 (turn 1), 2.191
/// (turn 2) and 1.707 (the other request).
#[tokio::test]
async fn a_next_turn_reuses_its_prefix_beside_another_request_and_replies_as_alone() {
    let gguf = qwen35_gguf().await;
    let name = "qwen3.5";
    let question = PROMPTS_MTP[2];
    let turn = |answer: Option<&str>| -> Value {
        let mut messages = vec![json!({ "role": "user", "content": question })];
        if let Some(answer) = answer {
            messages.push(json!({ "role": "assistant", "content": answer }));
            messages.push(
                json!({ "role": "user", "content": "Now list those days in reverse order, one per line. Reply with only the days." }),
            );
        }
        json!({
            "model": name,
            "messages": messages,
            "temperature": 0,
            "reasoning_effort": "none",
            "max_tokens": 96,
        })
    };
    let answer = "Monday\nTuesday\nWednesday\nThursday\nFriday\nSaturday\nSunday";
    let other = body(name, PROMPTS_MTP[0], 300);
    let (alone, reused_alone) = {
        let engine = load_engine(&gguf, name, 1, 4096).await;
        let model: Arc<dyn Chat> = Arc::clone(&engine) as Arc<dyn Chat>;
        let first = reply(&model, turn(None)).await;
        let second = reply(&model, turn(Some(answer))).await;
        let reused = engine.reused_prompt_tokens();
        let other = reply(&model, other.clone()).await;
        ((first, second, other), reused)
    };
    let engine = load_engine(&gguf, name, 2, 4096).await;
    let model: Arc<dyn Chat> = Arc::clone(&engine) as Arc<dyn Chat>;
    let beside = {
        let model = Arc::clone(&model);
        tokio::spawn(async move { reply(&model, other).await })
    };
    let first = reply(&model, turn(None)).await;
    let second = reply(&model, turn(Some(answer))).await;
    // turn 2 was the last request to start, so the count is its own
    let reused = engine.reused_prompt_tokens();
    let other = beside.await.expect("the other request runs");
    assert!(reused_alone > 0, "turn 2 reuses turn 1's prefix alone");
    assert_eq!(
        reused, reused_alone,
        "the prompt tokens turn 2 reused beside another request, against alone"
    );
    assert_eq!(
        (first, second, other),
        alone,
        "turn 1, turn 2 and the request beside them, against alone"
    );
}

/// A request that arrives while another is answering streams its first token before the other
/// sends its last: the second request does not wait for the first one's reply to end.
#[tokio::test]
async fn a_request_arriving_mid_reply_streams_before_the_first_one_ends() {
    let gguf = qwen3_gguf().await;
    let counting = "Count from 1 to 200, separated by commas. Reply with only the numbers.";
    let (long_alone, short_alone) = {
        let model = load(&gguf, "qwen3-0.6b", 1, 4096).await;
        (
            reply(&model, body("qwen3-0.6b", counting, 300)).await,
            reply(&model, body("qwen3-0.6b", "Say hello.", 16)).await,
        )
    };
    let model = load(&gguf, "qwen3-0.6b", 2, 4096).await;
    let mut long = model
        .chat_stream(request(body("qwen3-0.6b", counting, 300)))
        .await
        .expect("a stream");
    let mut long_text = String::new();
    // the long reply is under way before the short request is sent
    loop {
        let chunk = long
            .next()
            .await
            .expect("the long reply streams")
            .expect("a chunk");
        let started = chunk
            .choices
            .iter()
            .any(|c| c.delta.content.as_ref().is_some_and(|t| !t.is_empty()));
        for choice in chunk.choices {
            long_text.extend(choice.delta.content);
        }
        if started {
            break;
        }
    }
    let short_model = Arc::clone(&model);
    let short = tokio::spawn(async move {
        let mut stream = short_model
            .chat_stream(request(body("qwen3-0.6b", "Say hello.", 16)))
            .await
            .expect("a stream");
        let mut first = None;
        let mut text = String::new();
        while let Some(chunk) = stream.next().await {
            for choice in chunk.expect("a chunk").choices {
                if let Some(content) = choice.delta.content.filter(|c| !c.is_empty()) {
                    first.get_or_insert_with(Instant::now);
                    text.push_str(&content);
                }
            }
        }
        (first, text)
    });
    while let Some(chunk) = long.next().await {
        for choice in chunk.expect("a chunk").choices {
            long_text.extend(choice.delta.content);
        }
    }
    let long_ended = Instant::now();
    let (short_first, short_text) = short.await.expect("the short request runs");
    let short_first = short_first.expect("the short reply streams content");
    assert_eq!(
        (long_text, short_text),
        (long_alone, short_alone),
        "the replies served together, against alone"
    );
    assert!(
        short_first < long_ended,
        "the short reply's first token came {:?} after the long reply ended",
        short_first.duration_since(long_ended)
    );
}

/// A client that leaves while its request decodes beside another's leaves the other's reply as
/// it is alone.
#[tokio::test]
async fn a_client_leaving_mid_batch_leaves_the_other_reply_unchanged() {
    let gguf = qwen3_gguf().await;
    let alone = {
        let model = load(&gguf, "qwen3-0.6b", 1, 4096).await;
        reply(&model, body("qwen3-0.6b", PROMPTS[0], 96)).await
    };
    let model = load(&gguf, "qwen3-0.6b", 2, 4096).await;
    let mut leaving = model
        .chat_stream(request(body("qwen3-0.6b", PROMPTS[1], 300)))
        .await
        .expect("a stream");
    let _ = leaving.next().await;
    let staying = {
        let model = Arc::clone(&model);
        tokio::spawn(async move { reply(&model, body("qwen3-0.6b", PROMPTS[0], 96)).await })
    };
    // the leaving client reads a few more chunks while the other request runs, then goes
    for _ in 0..4 {
        let _ = leaving.next().await;
    }
    drop(leaving);
    let staying = staying.await.expect("the other request runs");
    assert_eq!(staying, alone, "the reply beside a client that left");
}
