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

//! How long a streamed request holds the permit of the model's rate controller: while Darkbloom
//! generates the response, and no longer. These drive the client over HTTP against a local
//! server that streams each response in parts the test releases one at a time, so a response can
//! be kept open mid-generation while a second request is made.

use std::io::{Read as _, Write as _};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::time::Duration;

use async_openai::error::OpenAIError;
use async_openai::types::chat::{
    ChatCompletionRequestMessage, ChatCompletionRequestUserMessage,
    ChatCompletionRequestUserMessageContent, CreateChatCompletionRequest, FinishReason,
};
use async_openai::types::responses::{CreateResponse, CreateResponseArgs, ResponseStreamEvent};
use futures::StreamExt;
use runtime_rate_control::RateController;
use serde_json::{Value, json};

use super::Darkbloom;
use crate::chat::Chat;
use crate::responses::Responses;

const KEY: &str = "eigeninference-test";

/// Long enough that a request waiting for the permit is plainly waiting.
const WAIT: Duration = Duration::from_millis(300);

/// Long enough for a request that is sent to be answered.
const SENT: Duration = Duration::from_secs(10);

/// One part of a streamed response: sent once the test releases it, or at once.
struct Part {
    release: Option<mpsc::Receiver<()>>,
    body: String,
}

/// A part sent at once.
fn now(body: String) -> Part {
    Part {
        release: None,
        body,
    }
}

/// A part sent once the test sends on the returned sender. Dropping the sender unsent closes
/// the response there.
fn held(body: String) -> (mpsc::Sender<()>, Part) {
    let (release, wait) = mpsc::channel();
    (
        release,
        Part {
            release: Some(wait),
            body,
        },
    )
}

/// A stand-in for Darkbloom that streams each response in parts. Each request takes the next
/// queued response, on a connection of its own.
struct SteppedServer {
    endpoint: String,
    received: Arc<AtomicUsize>,
    responses: mpsc::Sender<Vec<Part>>,
}

impl SteppedServer {
    fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind a local port");
        let endpoint = format!(
            "http://{}/v1",
            listener.local_addr().expect("read the bound address")
        );
        let received = Arc::new(AtomicUsize::new(0));
        let (responses, queued) = mpsc::channel::<Vec<Part>>();
        let counter = Arc::clone(&received);
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else {
                    return;
                };
                let parts = queued.try_recv().ok();
                let counter = Arc::clone(&counter);
                std::thread::spawn(move || answer(stream, parts, &counter));
            }
        });
        Self {
            endpoint,
            received,
            responses,
        }
    }

    fn queue(&self, parts: Vec<Part>) {
        self.responses.send(parts).expect("the server is running");
    }

    /// How many requests have reached the server.
    fn received(&self) -> usize {
        self.received.load(Ordering::SeqCst)
    }
}

/// Reads one request from `stream`, counts it, and streams `parts` back as server-sent events,
/// with no length, so the response ends when the connection closes.
fn answer(mut stream: TcpStream, parts: Option<Vec<Part>>, received: &AtomicUsize) {
    let mut head = Vec::new();
    let mut byte = [0_u8; 1];
    while !head.ends_with(b"\r\n\r\n") {
        if stream.read(&mut byte).unwrap_or(0) != 1 {
            return;
        }
        head.push(byte[0]);
    }
    let head = String::from_utf8_lossy(&head).into_owned();
    let content_length = head
        .lines()
        .filter_map(|line| line.split_once(':'))
        .find(|(name, _)| name.trim().eq_ignore_ascii_case("content-length"))
        .and_then(|(_, value)| value.trim().parse::<usize>().ok())
        .unwrap_or(0);
    let mut body = vec![0_u8; content_length];
    if stream.read_exact(&mut body).is_err() {
        return;
    }
    received.fetch_add(1, Ordering::SeqCst);

    let Some(parts) = parts else {
        let _ = stream.write_all(
            b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
        );
        return;
    };
    if stream
        .write_all(
            b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n",
        )
        .and_then(|()| stream.flush())
        .is_err()
    {
        return;
    }
    for part in parts {
        if let Some(release) = part.release
            && release.recv().is_err()
        {
            return;
        }
        if stream
            .write_all(part.body.as_bytes())
            .and_then(|()| stream.flush())
            .is_err()
        {
            return;
        }
    }
}

fn sse(event: &Value) -> String {
    format!("data: {event}\n\n")
}

const DONE: &str = "data: [DONE]\n\n";

fn chat_chunk(content: &str, finish_reason: Option<&str>) -> Value {
    json!({
        "id": "chatcmpl-1",
        "object": "chat.completion.chunk",
        "created": 1_778_000_000,
        "model": "gpt-oss-20b",
        "choices": [{"index": 0, "delta": {"role": "assistant", "content": content}, "finish_reason": finish_reason}]
    })
}

/// A Responses API event that carries the whole response, such as `response.created`.
fn response_event(kind: &str, sequence_number: u64, text: &str) -> Value {
    json!({
        "type": kind,
        "sequence_number": sequence_number,
        "response": {
            "id": "resp_1",
            "object": "response",
            "created_at": 1_778_000_000,
            "model": "gpt-oss-20b",
            "status": "completed",
            "output": [{
                "type": "message",
                "id": "msg_1",
                "role": "assistant",
                "status": "completed",
                "content": [{"type": "output_text", "annotations": [], "text": text}]
            }]
        }
    })
}

fn text_delta(delta: &str) -> Value {
    json!({
        "type": "response.output_text.delta",
        "sequence_number": 1,
        "item_id": "msg_1",
        "output_index": 0,
        "content_index": 0,
        "delta": delta
    })
}

fn chat_request() -> CreateChatCompletionRequest {
    CreateChatCompletionRequest {
        model: "m".to_string(),
        messages: vec![ChatCompletionRequestMessage::User(
            ChatCompletionRequestUserMessage {
                name: None,
                content: ChatCompletionRequestUserMessageContent::Text("ping".to_string()),
            },
        )],
        stream: Some(true),
        ..Default::default()
    }
}

fn responses_request() -> CreateResponse {
    CreateResponseArgs::default()
        .model("m")
        .input("ping")
        .build()
        .expect("a Responses API request")
}

/// A model whose rate controller has one permit, as `max_concurrency: 1` gives it.
fn model_with_one_permit(server: &SteppedServer) -> Darkbloom {
    Darkbloom::new("gpt-oss-20b", KEY, Some(&server.endpoint)).with_rate_controller(
        RateController::builder()
            .with_max_concurrent_requests(1)
            .build(),
    )
}

/// A streamed chat completion holds the permit while Darkbloom generates it, so a Responses API
/// request waits for it. It holds it only until the last chunk, not until the stream ends:
/// Spice's tool loops send a round's next request on its last chunk, while its stream is still
/// open, and that request must not wait on its own round.
#[tokio::test]
async fn a_chat_stream_holds_its_permit_until_the_model_finishes_generating() {
    let server = SteppedServer::start();
    let (finish, finish_part) = held(sse(&chat_chunk("lo", Some("stop"))));
    let (end, end_part) = held(DONE.to_string());
    server.queue(vec![
        now(sse(&chat_chunk("Hel", None))),
        finish_part,
        end_part,
    ]);
    server.queue(vec![
        now(sse(&response_event("response.created", 0, ""))),
        now(sse(&response_event("response.completed", 2, "pong"))),
    ]);
    let model = model_with_one_permit(&server);

    let mut chat = model
        .chat_stream(chat_request())
        .await
        .expect("the chat stream opens");
    let first = chat
        .next()
        .await
        .expect("a first chunk")
        .expect("a chunk, not an error");
    assert_eq!(first.choices[0].delta.content.as_deref(), Some("Hel"));

    tokio::time::timeout(
        WAIT,
        Responses::responses_stream(&model, responses_request()),
    )
    .await
    .map(|opened| opened.map(|_| "the stream opened"))
    .expect_err("a Responses API request must wait while the chat completion is being generated");
    assert_eq!(
        server.received(),
        1,
        "the waiting request must not reach Darkbloom"
    );

    finish.send(()).expect("the server holds the last chunk");
    let last = chat
        .next()
        .await
        .expect("the last chunk")
        .expect("a chunk, not an error");
    assert_eq!(last.choices[0].finish_reason, Some(FinishReason::Stop));

    // Darkbloom has not sent `[DONE]`, so the chat stream is still open.
    let events: Vec<_> = tokio::time::timeout(SENT, async {
        Responses::responses_stream(&model, responses_request())
            .await
            .expect("the Responses API stream opens")
            .collect::<Vec<_>>()
            .await
    })
    .await
    .expect("once the model has finished the chat completion, the Responses API request is sent");
    assert_eq!(
        events.len(),
        2,
        "response.created, then response.completed: {events:?}"
    );
    let Some(Ok(ResponseStreamEvent::ResponseCompleted(completed))) = events.last() else {
        panic!("the stream ends with response.completed: {events:?}");
    };
    assert_eq!(completed.response.output_text().as_deref(), Some("pong"));
    assert_eq!(server.received(), 2);

    end.send(()).expect("the server holds `[DONE]`");
    let after_done = chat.next().await;
    assert!(
        after_done.is_none(),
        "the chat stream ends with `[DONE]`: {after_done:?}"
    );
}

/// The same for a streamed Responses API response: a chat completion waits until
/// `response.completed`, and not until the stream ends.
#[tokio::test]
async fn a_responses_stream_holds_its_permit_until_the_model_finishes_generating() {
    let server = SteppedServer::start();
    let (finish, finish_part) = held(sse(&response_event("response.completed", 2, "Hello")));
    let (end, end_part) = held(String::new());
    server.queue(vec![
        now(sse(&response_event("response.created", 0, ""))),
        now(sse(&text_delta("Hel"))),
        finish_part,
        end_part,
    ]);
    server.queue(vec![
        now(sse(&chat_chunk("pong", Some("stop")))),
        now(DONE.to_string()),
    ]);
    let model = model_with_one_permit(&server);

    let mut responses = Responses::responses_stream(&model, responses_request())
        .await
        .expect("the Responses API stream opens");
    let created = responses
        .next()
        .await
        .expect("a first event")
        .expect("an event, not an error");
    assert!(
        matches!(created, ResponseStreamEvent::ResponseCreated(_)),
        "the first event is response.created: {created:?}"
    );
    let delta = responses
        .next()
        .await
        .expect("a second event")
        .expect("an event, not an error");
    let ResponseStreamEvent::ResponseOutputTextDelta(delta) = delta else {
        panic!("the second event is a text delta: {delta:?}");
    };
    assert_eq!(delta.delta, "Hel");

    tokio::time::timeout(WAIT, model.chat_stream(chat_request()))
        .await
        .map(|opened| opened.map(|_| "the stream opened"))
        .expect_err(
            "a chat completion must wait while the Responses API response is being generated",
        );
    assert_eq!(
        server.received(),
        1,
        "the waiting request must not reach Darkbloom"
    );

    finish
        .send(())
        .expect("the server holds response.completed");
    let completed = responses
        .next()
        .await
        .expect("a third event")
        .expect("an event, not an error");
    assert!(
        matches!(completed, ResponseStreamEvent::ResponseCompleted(_)),
        "the third event is response.completed: {completed:?}"
    );

    // Darkbloom has not closed the Responses API stream.
    let chunks: Vec<_> = tokio::time::timeout(SENT, async {
        model
            .chat_stream(chat_request())
            .await
            .expect("the chat stream opens")
            .collect::<Vec<_>>()
            .await
    })
    .await
    .expect("once the model has finished the Responses API response, the chat completion is sent");
    assert_eq!(chunks.len(), 1, "one chunk, then `[DONE]`: {chunks:?}");
    let chunk = chunks[0].as_ref().expect("a chunk, not an error");
    assert_eq!(chunk.choices[0].delta.content.as_deref(), Some("pong"));
    assert_eq!(server.received(), 2);

    end.send(())
        .expect("the server holds the end of the stream");
    let after_end = responses.next().await;
    assert!(
        after_end.is_none(),
        "the Responses API stream ends when Darkbloom closes it: {after_end:?}"
    );
}

/// A stream that fails, or that its consumer drops, before the model has finished gives its
/// permit back at once, though Darkbloom has not closed the connection.
#[tokio::test]
async fn a_stream_that_fails_or_is_dropped_mid_generation_gives_its_permit_back() {
    let server = SteppedServer::start();
    let error_event = "data: {\"error\":{\"message\":\"provider disconnected mid-stream\",\"type\":\"server_error\",\"code\":\"provider_error\"}}\n\n";
    let (_failed_stays_open, failed_tail) = held(String::new());
    server.queue(vec![
        now(sse(&chat_chunk("po", None))),
        now(error_event.to_string()),
        failed_tail,
    ]);
    let (_dropped_stays_open, dropped_tail) = held(sse(&chat_chunk("ng", Some("stop"))));
    server.queue(vec![now(sse(&chat_chunk("po", None))), dropped_tail]);
    server.queue(vec![
        now(sse(&chat_chunk("pong", Some("stop")))),
        now(DONE.to_string()),
    ]);
    let model = model_with_one_permit(&server);

    let mut failed = model
        .chat_stream(chat_request())
        .await
        .expect("the first stream opens");
    let first = failed
        .next()
        .await
        .expect("a first chunk")
        .expect("a chunk, not an error");
    assert_eq!(first.choices[0].delta.content.as_deref(), Some("po"));
    let error = failed
        .next()
        .await
        .expect("Darkbloom's error event")
        .expect_err("an error, not a chunk");
    assert!(
        matches!(&error, OpenAIError::ApiError(e) if e.message == "provider disconnected mid-stream"),
        "the stream fails with Darkbloom's error: {error:?}"
    );

    let mut dropped = tokio::time::timeout(SENT, model.chat_stream(chat_request()))
        .await
        .expect("once the first stream has failed, the next request is sent")
        .expect("the second stream opens");
    let chunk = dropped
        .next()
        .await
        .expect("a first chunk")
        .expect("a chunk, not an error");
    assert_eq!(chunk.choices[0].delta.content.as_deref(), Some("po"));
    drop(dropped);

    let chunks: Vec<_> = tokio::time::timeout(SENT, async {
        model
            .chat_stream(chat_request())
            .await
            .expect("the third stream opens")
            .collect::<Vec<_>>()
            .await
    })
    .await
    .expect("once the second stream is dropped, the next request is sent");
    assert_eq!(chunks.len(), 1, "one chunk, then `[DONE]`: {chunks:?}");
    let chunk = chunks[0].as_ref().expect("a chunk, not an error");
    assert_eq!(chunk.choices[0].delta.content.as_deref(), Some("pong"));
    assert_eq!(server.received(), 3);
    drop(failed);
}
