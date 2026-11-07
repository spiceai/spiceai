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
use std::time::Duration;

use async_openai::types::chat::ChatCompletionResponseStream;
use futures::StreamExt;
use llms::chat::{Chat, LocalModelOptions, ReasoningDefault};
use llms_llamacpp::{LlamaCpp, STREAM_BUFFER};
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
/// How long a request beside another may take to stream: a request that cannot be served
/// until the other's client reads would never finish.
const SERVED_WITHIN: Duration = Duration::from_secs(60);

/// A streamed reply's content, and how many chunks carried it.
struct Streamed {
    text: String,
    chunks: usize,
}

async fn streamed(model: &Arc<dyn Chat>, body: Value) -> Streamed {
    let mut stream = model.chat_stream(request(body)).await.expect("a stream");
    let mut reply = Streamed {
        text: String::new(),
        chunks: 0,
    };
    while let Some(chunk) = stream.next().await {
        for choice in chunk.expect("a chunk").choices {
            if let Some(content) = choice.delta.content.filter(|c| !c.is_empty()) {
                reply.chunks += 1;
                reply.text.push_str(&content);
            }
        }
    }
    reply
}

/// Reads `stream` up to its first chunk of content, and returns the content read.
async fn first_content(stream: &mut ChatCompletionResponseStream) -> String {
    let mut text = String::new();
    while text.is_empty() {
        let chunk = stream
            .next()
            .await
            .expect("the reply streams content")
            .expect("a chunk");
        for choice in chunk.choices {
            text.extend(choice.delta.content);
        }
    }
    text
}

/// Reads `stream` until it has streamed `chunks` chunks of content, and returns their content.
async fn content_chunks(stream: &mut ChatCompletionResponseStream, chunks: usize) -> String {
    let mut text = String::new();
    let mut read = 0;
    while read < chunks {
        let chunk = stream
            .next()
            .await
            .expect("the reply streams that many chunks")
            .expect("a chunk");
        for choice in chunk.choices {
            if let Some(content) = choice.delta.content.filter(|c| !c.is_empty()) {
                read += 1;
                text.push_str(&content);
            }
        }
    }
    text
}

/// The content left in `stream`.
async fn rest(mut stream: ChatCompletionResponseStream) -> String {
    let mut text = String::new();
    while let Some(chunk) = stream.next().await {
        for choice in chunk.expect("a chunk").choices {
            text.extend(choice.delta.content);
        }
    }
    text
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

/// A request that arrives while another is answering is served while the other's client has
/// stopped reading. The first reply holds more tokens than a client buffers, so one request at a
/// time would wait on that client and never reach the second request. The second reply is as
/// long, so it is still decoding when the first client's buffer fills, and a step that waited on
/// that client would stall it too. Served together, the second reply arrives whole while the
/// first is paused, and both replies are as alone. The prompt is the one confident reply longer
/// than a client's buffer, so both requests send it; replies of different prompts are kept apart
/// in `requests_served_together_reply_as_each_does_alone`.
#[tokio::test]
async fn a_request_arriving_mid_reply_streams_before_the_first_one_ends() {
    let gguf = qwen3_gguf().await;
    let alone = {
        let model = load(&gguf, "qwen3-0.6b", 1, 4096).await;
        streamed(&model, body("qwen3-0.6b", PROMPTS[0], 96)).await
    };
    assert!(
        alone.chunks > STREAM_BUFFER,
        "the reply streams {} chunks, which a client that stops reading buffers whole \
         ({STREAM_BUFFER}), so it could end without its client",
        alone.chunks
    );
    let model = load(&gguf, "qwen3-0.6b", 2, 4096).await;
    let mut first = model
        .chat_stream(request(body("qwen3-0.6b", PROMPTS[0], 96)))
        .await
        .expect("a stream");
    // the first reply is under way, then its client stops reading
    let mut first_text = first_content(&mut first).await;
    let second = tokio::time::timeout(
        SERVED_WITHIN,
        streamed(&model, body("qwen3-0.6b", PROMPTS[0], 96)),
    )
    .await
    .expect("the second request replies while the first reply's client is not reading");
    first_text.push_str(&rest(first).await);
    assert_eq!(
        (first_text, second.text),
        (alone.text.clone(), alone.text),
        "the replies served together, against alone"
    );
}

/// A client that leaves while its request waits beside another's leaves the other's reply as it
/// is alone. The leaving client reads one chunk and no more, and its reply holds more tokens
/// than a client buffers, so once the other request has streamed more chunks than that buffer,
/// the leaving request is paused on its full buffer, still holding its sequence. The client goes
/// then, while the other reply still has tokens to decode. The prompt is the one confident reply
/// longer than a client's buffer, so both requests send it.
#[tokio::test]
async fn a_client_leaving_mid_batch_leaves_the_other_reply_unchanged() {
    let gguf = qwen3_gguf().await;
    let alone = {
        let model = load(&gguf, "qwen3-0.6b", 1, 4096).await;
        streamed(&model, body("qwen3-0.6b", PROMPTS[0], 96)).await
    };
    assert!(
        alone.chunks > STREAM_BUFFER + 1,
        "the reply streams {} chunks, which its client buffers whole ({STREAM_BUFFER}), so it \
         could end before the client goes",
        alone.chunks
    );
    let model = load(&gguf, "qwen3-0.6b", 2, 4096).await;
    let mut leaving = model
        .chat_stream(request(body("qwen3-0.6b", PROMPTS[0], 96)))
        .await
        .expect("a stream");
    first_content(&mut leaving).await;
    let mut staying = model
        .chat_stream(request(body("qwen3-0.6b", PROMPTS[0], 96)))
        .await
        .expect("a stream");
    // by the time the other request has streamed more chunks than a client buffers, the
    // leaving request has filled its client's buffer and waits
    let mut staying_text = tokio::time::timeout(
        SERVED_WITHIN,
        content_chunks(&mut staying, STREAM_BUFFER + 1),
    )
    .await
    .expect("the other request streams while the leaving one waits");
    drop(leaving);
    staying_text.push_str(&rest(staying).await);
    assert_eq!(
        staying_text, alone.text,
        "the reply beside a client that left"
    );
}
