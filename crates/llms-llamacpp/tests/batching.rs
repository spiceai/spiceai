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
use llms_llamacpp::STREAM_BUFFER;
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
