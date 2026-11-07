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
use serde_json::{Value, json};

use crate::mtp::qwen35_gguf;
use crate::{qwen3_gguf, request};

/// Prompts whose replies a small model gives with wide margins at every token, so serving them
/// in a batch of another shape cannot flip a near tie: a difference is the scheduler's.
const PROMPTS: [&str; 4] = [
    "Count from 1 to 40, separated by commas. Reply with only the numbers.",
    "List the letters of the English alphabet in order, separated by spaces. Reply with only the letters.",
    "List the days of the week starting from Monday, one per line. Reply with only the days.",
    "List the months of the year starting from January, one per line. Reply with only the months.",
];

async fn load(gguf: &Path, name: &'static str, concurrent: u32, context: usize) -> Arc<dyn Chat> {
    let path = gguf.to_str().expect("a UTF-8 path").to_string();
    llms::chat::create_local_model(
        &[path],
        None,
        None,
        None,
        None,
        None,
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
    .expect("the model loads")
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
    let (alone, together) = alone_and_together(&gguf, "qwen3.5", &PROMPTS, 96, 4096).await;
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
