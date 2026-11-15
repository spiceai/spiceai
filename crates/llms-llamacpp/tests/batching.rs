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

/// Prompts whose replies a small model gives with wide margins at every token, so serving them
/// in a batch of another shape cannot flip a near tie: a difference is the scheduler's.
pub(crate) const PROMPTS: [&str; 4] = [
    "Count from 1 to 40, separated by commas. Reply with only the numbers.",
    "List the letters of the English alphabet in order, separated by spaces. Reply with only the letters.",
    "List the days of the week starting from Monday, one per line. Reply with only the days.",
    "List the months of the year starting from January, one per line. Reply with only the months.",
];

/// Loads `gguf` serving up to `concurrent` requests at once, or the engine's own ceiling for 0.
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

pub(crate) fn body(name: &str, prompt: &str, max_tokens: u32) -> Value {
    json!({
        "model": name,
        "messages": [{ "role": "user", "content": prompt }],
        "temperature": 0,
        "reasoning_effort": "none",
        "max_tokens": max_tokens,
    })
}

pub(crate) async fn reply(model: &Arc<dyn Chat>, body: Value) -> String {
    let reply = model.chat_request(request(body)).await.expect("a reply");
    reply.choices[0].message.content.clone().expect("content")
}

/// How long a request beside another may take to stream: a request that cannot be served
/// until the other's client reads would never finish.
pub(crate) const SERVED_WITHIN: Duration = Duration::from_secs(60);

/// A streamed reply's content, and how many chunks carried it.
pub(crate) struct Streamed {
    pub(crate) text: String,
    pub(crate) chunks: usize,
}

pub(crate) async fn streamed(model: &Arc<dyn Chat>, body: Value) -> Streamed {
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
pub(crate) async fn first_content(stream: &mut ChatCompletionResponseStream) -> String {
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
pub(crate) async fn rest(mut stream: ChatCompletionResponseStream) -> String {
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

/// A request admitted while another decodes reads the end of its prompt in the other's next
/// step, beside its draft, instead of in a decode of its own, and both reply as each does alone.
/// `Qwen3.5`'s recurrent state saves a checkpoint four tokens before the prompt's end, and those
/// four are what the step reads; its MTP head drafts for the running request in the same step.
#[tokio::test]
async fn a_request_admitted_mid_reply_reads_its_prompts_end_in_the_running_ones_step() {
    let gguf = qwen35_gguf().await;
    let name = "qwen3.5";
    let (first_prompt, second_prompt) = (PROMPTS[0], PROMPTS[1]);
    let alone = {
        let model = load(&gguf, name, 1, 4096).await;
        (
            reply(&model, body(name, first_prompt, 96)).await,
            reply(&model, body(name, second_prompt, 96)).await,
        )
    };
    let engine = Arc::new(
        LlamaCpp::from(
            std::slice::from_ref(&gguf),
            LocalModelOptions {
                reasoning: ReasoningDefault {
                    model_name: name,
                    effort: None,
                },
                context_length: Some(4096),
                concurrent_requests: NonZeroU32::new(2),
                ..LocalModelOptions::default()
            },
        )
        .await
        .expect("the model loads"),
    );
    let model: Arc<dyn Chat> = Arc::clone(&engine) as Arc<dyn Chat>;
    let mut first = model
        .chat_stream(request(body(name, first_prompt, 96)))
        .await
        .expect("a stream");
    // the first reply is under way, and its client keeps reading while the second arrives
    let mut first_text = first_content(&mut first).await;
    let (rest_of_first, second) = tokio::time::timeout(
        SERVED_WITHIN,
        futures::future::join(rest(first), reply(&model, body(name, second_prompt, 96))),
    )
    .await
    .expect("both requests reply");
    first_text.push_str(&rest_of_first);
    assert_eq!(
        (first_text, second),
        alone,
        "a reply and one admitted beside it mid reply, against each alone"
    );
    assert_eq!(
        engine.prompt_ends_in_steps(),
        1,
        "the second request read its prompt's end in the first one's step"
    );
}

/// Requests whose output limits together pass the shared cache still all reply, as each does
/// alone. With a 1024-token context, each of the four requests may write 320 reply tokens, more
/// than a third of the context; each is admitted while its prompt fits beside the running
/// requests' tokens, and the request admitted last would be preempted should their tokens
/// outgrow the context.
#[tokio::test]
async fn requests_whose_limits_pass_the_cache_reply_as_alone() {
    let gguf = qwen3_gguf().await;
    let (alone, together) = alone_and_together(&gguf, "qwen3-0.6b", &PROMPTS, 320, 1024).await;
    assert_eq!(together, alone, "replies served together, against alone");
}

/// A request with no output limit: neither `max_tokens` nor `max_completion_tokens`.
fn unlimited(name: &str, prompt: &str) -> Value {
    json!({
        "model": name,
        "messages": [{ "role": "user", "content": prompt }],
        "temperature": 0,
        "reasoning_effort": "none",
    })
}

/// A prompt whose greedy reply runs on past [`SMALL_CONTEXT`], so the reply alone ends where
/// the context does.
const PAST_THE_CONTEXT: &str =
    "Count from 1 to 3000 in digits, separated by single spaces. Reply with only the numbers.";

/// A context that one reply to [`PAST_THE_CONTEXT`] fills alone.
const SMALL_CONTEXT: usize = 512;

/// Two requests with no output limit decode together on a cache every sequence shares, and
/// when their tokens outgrow the context, one is preempted and resumes later: both reply as
/// alone.
///
/// The first request's client stops reading after its first chunk, so the request holds its
/// tokens without stepping once its client's buffer is full. The second streams to the end of the
/// context, which it reaches only if the first's tokens leave the shared cache. The first's client
/// then reads the rest, and the first resumes. Were each request admitted with room for its
/// whole possible reply, the second would wait for the first to end, which waits on a client
/// that is not reading. `while_the_first_waits` runs after the second reply ends and before the
/// first's client reads again, while the first, preempted, is the only request left.
async fn requests_with_no_output_limit_preempt_and_reply_as_alone(
    gguf: &Path,
    name: &'static str,
    while_the_first_waits: impl Future<Output = ()>,
) {
    let alone = {
        let model = load(gguf, name, 1, SMALL_CONTEXT).await;
        model
            .chat_request(request(unlimited(name, PAST_THE_CONTEXT)))
            .await
            .expect("a reply")
    };
    let usage = alone.usage.as_ref().expect("the reply reports its usage");
    let alone = alone.choices[0].message.content.clone().expect("content");
    let (held, prompt) = (
        usize::try_from(usage.total_tokens).expect("a count"),
        usize::try_from(usage.prompt_tokens).expect("a count"),
    );
    assert!(
        held + prompt > SMALL_CONTEXT,
        "one reply alone holds {held} tokens and the other's prompt {prompt}, which must not \
         fit the {SMALL_CONTEXT}-token context together: {alone}"
    );
    let model = load(gguf, name, 0, SMALL_CONTEXT).await;
    let mut first = model
        .chat_stream(request(unlimited(name, PAST_THE_CONTEXT)))
        .await
        .expect("a stream");
    let mut first_text = first_content(&mut first).await;
    let second = tokio::time::timeout(
        SERVED_WITHIN,
        streamed(&model, unlimited(name, PAST_THE_CONTEXT)),
    )
    .await
    .expect("the second request replies while the first reply's client is not reading");
    while_the_first_waits.await;
    first_text.push_str(&rest(first).await);
    assert_eq!(
        (first_text, second.text),
        (alone.clone(), alone),
        "the replies served together, against alone"
    );
}

/// [`requests_with_no_output_limit_preempt_and_reply_as_alone`] on a model without a draft head.
#[tokio::test]
async fn requests_with_no_output_limit_preempt_and_reply_as_alone_without_a_draft() {
    let gguf = qwen3_gguf().await;
    requests_with_no_output_limit_preempt_and_reply_as_alone(&gguf, "qwen3-0.6b", async {}).await;
}

/// Set in the environment of the process `preempted_while_its_client_is_not_reading` runs in.
#[cfg(unix)]
const IDLE_PROBE: &str = "SPICE_LLAMACPP_TEST_IDLE_PROBE";

/// How long the process's CPU time is measured while its only request waits on its client.
#[cfg(unix)]
const IDLE_WINDOW: Duration = Duration::from_secs(1);

/// While the only request left is one preempted for room whose client is not reading, the
/// decode thread waits for that client rather than looping. The check runs in a process of its
/// own, as it measures the CPU time of the whole process.
#[cfg(unix)]
#[tokio::test]
async fn a_preempted_request_whose_client_is_not_reading_leaves_the_decode_thread_idle() {
    let status =
        std::process::Command::new(std::env::current_exe().expect("the test binary's path"))
            .args([
                "--exact",
                "batching::preempted_while_its_client_is_not_reading",
                "--nocapture",
            ])
            .env(IDLE_PROBE, "1")
            .status()
            .expect("the probe runs");
    assert!(status.success(), "the probe's checks failed: {status}");
}

/// [`requests_with_no_output_limit_preempt_and_reply_as_alone`] on a model without a draft
/// head, run only by [`a_preempted_request_whose_client_is_not_reading_leaves_the_decode_thread_idle`]
/// in a process of its own. Between the second reply's end and the first client reading again,
/// the process may spend under a quarter of [`IDLE_WINDOW`] on the CPU: a decode thread that
/// loops without pausing spends about all of it. Without [`IDLE_PROBE`] it does nothing.
#[cfg(unix)]
#[tokio::test]
async fn preempted_while_its_client_is_not_reading() {
    if std::env::var_os(IDLE_PROBE).is_none() {
        return;
    }
    let gguf = qwen3_gguf().await;
    requests_with_no_output_limit_preempt_and_reply_as_alone(&gguf, "qwen3-0.6b", async {
        let before = cpu_time();
        tokio::time::sleep(IDLE_WINDOW).await;
        let spent = cpu_time().saturating_sub(before);
        eprintln!("CPU time while the preempted request waited: {spent:?} of {IDLE_WINDOW:?}");
        assert!(
            spent < IDLE_WINDOW / 4,
            "the process spent {spent:?} on the CPU in {IDLE_WINDOW:?} while its only request \
             waited, preempted, on a client that was not reading"
        );
    })
    .await;
}

/// The CPU time this process has spent, in user and system mode, across its threads.
#[cfg(unix)]
fn cpu_time() -> Duration {
    use nix::sys::resource::{UsageWho, getrusage};
    use nix::sys::time::TimeValLike as _;
    let usage = getrusage(UsageWho::RUSAGE_SELF).expect("the process's resource usage");
    let micros = usage.user_time().num_microseconds() + usage.system_time().num_microseconds();
    Duration::from_micros(u64::try_from(micros).expect("a CPU time is not negative"))
}

/// [`requests_with_no_output_limit_preempt_and_reply_as_alone`] on a hybrid model with an MTP
/// head, whose recurrent state and head state leave the shared cache with its tokens.
#[tokio::test]
async fn requests_with_no_output_limit_preempt_and_reply_as_alone_with_an_mtp_head() {
    let gguf = qwen35_gguf().await;
    requests_with_no_output_limit_preempt_and_reply_as_alone(&gguf, "qwen3.5", async {}).await;
}

/// A preempted request whose client reads again resumes in place of a running request whose
/// client has stopped reading, even the only one running: a request whose client is not reading
/// holds no room another request can use.
///
/// The first request's client stops reading after its first chunk, and the first is preempted
/// for the second's room, as in [`requests_with_no_output_limit_preempt_and_reply_as_alone`].
/// The second's client then stops reading too, partway through its reply: the second holds more
/// of the context than the first's tokens leave, and stalls before the end of the context. The
/// first's client then reads its whole reply, which it can only once the second leaves the shared
/// cache for it, and then the second's client reads the rest. Both reply as alone.
#[tokio::test]
async fn a_preempted_request_resumes_in_place_of_one_whose_client_stopped_reading() {
    let (gguf, name) = (qwen3_gguf().await, "qwen3-0.6b");
    let (alone, prompt) = {
        let model = load(&gguf, name, 1, SMALL_CONTEXT).await;
        let usage = model
            .chat_request(request(unlimited(name, PAST_THE_CONTEXT)))
            .await
            .expect("a reply")
            .usage
            .expect("the reply reports its usage");
        let prompt = usize::try_from(usage.prompt_tokens).expect("a count");
        (
            streamed(&model, unlimited(name, PAST_THE_CONTEXT)).await,
            prompt,
        )
    };
    // The first stalls holding its prompt and about a buffer's worth of its reply, and leaves the
    // shared cache once the second's tokens need its room: past `past_the_first` chunks of the
    // second's reply. The second's client keeps reading until then, so the second still steps
    // when room runs out, and stops a buffer's worth short of the end, so the second stalls,
    // holding the room the first needs, before its reply ends. It stops halfway between.
    let past_the_first = SMALL_CONTEXT - 2 * prompt - STREAM_BUFFER;
    let short_of_the_end = SMALL_CONTEXT - prompt - STREAM_BUFFER;
    let stop_after = past_the_first.midpoint(short_of_the_end);
    assert!(
        alone.chunks > stop_after + STREAM_BUFFER,
        "the reply alone streams {} chunks, so the second could end without its client reading",
        alone.chunks
    );
    let model = load(&gguf, name, 0, SMALL_CONTEXT).await;
    let mut first = model
        .chat_stream(request(unlimited(name, PAST_THE_CONTEXT)))
        .await
        .expect("a stream");
    let mut first_text = first_content(&mut first).await;
    let mut second = model
        .chat_stream(request(unlimited(name, PAST_THE_CONTEXT)))
        .await
        .expect("a stream");
    let mut second_text =
        tokio::time::timeout(SERVED_WITHIN, content_chunks(&mut second, stop_after))
            .await
            .expect("the second streams while the first's client is not reading");
    first_text.push_str(
        &tokio::time::timeout(SERVED_WITHIN, rest(first))
            .await
            .expect("the first resumes and replies while the second's client is not reading"),
    );
    second_text.push_str(
        &tokio::time::timeout(SERVED_WITHIN, rest(second))
            .await
            .expect("the second resumes once its client reads"),
    );
    assert_eq!(
        (first_text, second_text),
        (alone.text.clone(), alone.text),
        "the replies served together, against alone"
    );
}

/// The room #15598 specifies a request starts with beside the running ones on a cache every
/// sequence shares, after its prompt.
const SPECIFIED_HEADROOM: usize = 256;

/// How far apart in the wrong order this test may read two events the engine sent in order:
/// far beyond the microseconds between two reads in one poll, far below the seconds a request
/// started too early leads by.
const READ_ORDER_SLACK: Duration = Duration::from_millis(100);

/// On a cache every sequence shares, a request whose prompt would leave less than
/// [`SPECIFIED_HEADROOM`] tokens of room beside a running request does not start beside it, only
/// to be preempted a few steps later: it waits for the running request to end, and both reply as
/// alone.
///
/// The second request's prompt of about 300 tokens fits beside the first's with room for a step,
/// but not with the headroom, from the first's first chunk on.
#[tokio::test]
async fn a_request_that_would_leave_less_than_the_headroom_waits_for_room() {
    let (gguf, name) = (qwen3_gguf().await, "qwen3-0.6b");
    let long = format!(
        "{}\n\n{}",
        "The quick brown fox jumps over the lazy dog. ".repeat(30),
        PROMPTS[0]
    );
    let first_body = unlimited(name, PAST_THE_CONTEXT);
    let second_body = body(name, &long, 300);
    let (first_alone, second_alone, first_prompt, second_prompt) = {
        let model = load(&gguf, name, 1, SMALL_CONTEXT).await;
        let first = model
            .chat_request(request(first_body.clone()))
            .await
            .expect("a reply");
        let second = model
            .chat_request(request(second_body.clone()))
            .await
            .expect("a reply");
        let prompt = |reply: &async_openai::types::chat::CreateChatCompletionResponse| {
            usize::try_from(reply.usage.as_ref().expect("usage").prompt_tokens).expect("a count")
        };
        (
            first.choices[0].message.content.clone().expect("content"),
            second.choices[0].message.content.clone().expect("content"),
            prompt(&first),
            prompt(&second),
        )
    };
    assert!(
        first_prompt + 1 + second_prompt + SPECIFIED_HEADROOM > SMALL_CONTEXT
            && first_prompt + STREAM_BUFFER + second_prompt + 1 < SMALL_CONTEXT,
        "prompts of {first_prompt} and {second_prompt} tokens must leave less than the headroom \
         from the first's first token on, and room for a step"
    );
    let model = load(&gguf, name, 0, SMALL_CONTEXT).await;
    let mut first = model
        .chat_stream(request(first_body))
        .await
        .expect("a stream");
    let mut first_text = first_content(&mut first).await;
    let mut second = model
        .chat_stream(request(second_body))
        .await
        .expect("a stream");
    let ((first_ended, first_rest), (second_started, second_first)) =
        tokio::time::timeout(SERVED_WITHIN, async {
            tokio::join!(
                async {
                    let text = rest(first).await;
                    (std::time::Instant::now(), text)
                },
                async {
                    let text = first_content(&mut second).await;
                    (std::time::Instant::now(), text)
                },
            )
        })
        .await
        .expect("both requests reply");
    // Both instants are when this test read each stream, not when the engine sent to it: when
    // the first's end and the second's first content are both waiting, one poll of `join!`
    // reads both, in either order, within microseconds. A second request started beside the
    // first leads its end by the rest of the first's reply, seconds on this model.
    let lead = first_ended.saturating_duration_since(second_started);
    assert!(
        lead < READ_ORDER_SLACK,
        "the second request's first content came {lead:?} before the first request ended"
    );
    first_text.push_str(&first_rest);
    let second_text = second_first + &rest(second).await;
    assert_eq!(
        (first_text, second_text),
        (first_alone, second_alone),
        "the replies, against alone"
    );
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
/// in `requests_served_together_reply_as_each_does_alone`. The model served together sets no
/// ceiling, so the engine's own serves the requests together.
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
    let model = load(&gguf, "qwen3-0.6b", 0, 4096).await;
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
