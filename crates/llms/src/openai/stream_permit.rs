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

//! When a request for a streamed response gives back its rate-controller permit.

use std::pin::Pin;
use std::task::{Context, Poll, ready};

use async_openai::error::OpenAIError;
use async_openai::types::chat::CreateChatCompletionStreamResponse;
use async_openai::types::responses::ResponseStreamEvent;
use futures::{Stream, StreamExt};
use runtime_rate_control::Permit;

/// A streamed response, as the client returns it.
type BoxStream<T> = Pin<Box<dyn Stream<Item = Result<T, OpenAIError>> + Send>>;

/// When a request for a streamed response gives back its rate-controller permit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StreamPermit {
    /// When the response starts, so the controller bounds the requests waiting for a response.
    /// The client's built-in limiter works this way.
    OnResponse,
    /// When the model finishes generating the response, so the controller bounds the responses
    /// being generated at once. A controller a model was given, from its `max_concurrency`,
    /// works this way.
    OnGenerated,
}

impl StreamPermit {
    /// `stream`, holding `permit` for as long as this says; `ends_generation` tells which item
    /// is the model's last.
    pub(crate) fn hold<S, T>(
        self,
        permit: Permit,
        stream: S,
        ends_generation: fn(&T) -> bool,
    ) -> BoxStream<T>
    where
        S: Stream<Item = Result<T, OpenAIError>> + Send + Unpin + 'static,
        T: 'static,
    {
        match self {
            Self::OnResponse => {
                drop(permit);
                Box::pin(stream)
            }
            Self::OnGenerated => Box::pin(GeneratingStream {
                stream,
                permit: Some(permit),
                ends_generation,
            }),
        }
    }
}

/// Whether a chat completion chunk is the model's last: the chunk that carries a choice's
/// `finish_reason`, after which only usage and `[DONE]` follow. Of several choices (`n`), the
/// first to finish ends it, so a tool loop continuing that choice never waits on its own request.
pub(crate) fn chat_chunk_ends_generation(chunk: &CreateChatCompletionStreamResponse) -> bool {
    chunk
        .choices
        .iter()
        .any(|choice| choice.finish_reason.is_some())
}

/// Whether a Responses API event is the model's last: `response.completed`,
/// `response.incomplete`, `response.failed` or `error`.
pub(crate) fn responses_event_ends_generation(event: &ResponseStreamEvent) -> bool {
    matches!(
        event,
        ResponseStreamEvent::ResponseCompleted(_)
            | ResponseStreamEvent::ResponseIncomplete(_)
            | ResponseStreamEvent::ResponseFailed(_)
            | ResponseStreamEvent::ResponseError(_)
    )
}

/// A streamed response that holds its request's permit until the model has finished
/// generating it: until the item `ends_generation` picks out, an error, or the end of the
/// stream, whichever comes first. A stream dropped before then, by a client that went away,
/// gives the permit back with it.
///
/// The permit goes back when generation ends rather than when the stream does, because Spice's
/// tool loops send a round's next request on the round's last item, while the round's stream
/// is still open: a permit held to the end of that stream would leave the next request waiting
/// on its own round.
struct GeneratingStream<S, T> {
    stream: S,
    permit: Option<Permit>,
    ends_generation: fn(&T) -> bool,
}

impl<S, T> Stream for GeneratingStream<S, T>
where
    S: Stream<Item = Result<T, OpenAIError>> + Unpin,
{
    type Item = Result<T, OpenAIError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let item = ready!(self.stream.poll_next_unpin(cx));
        let generated = match &item {
            Some(Ok(item)) => (self.ends_generation)(item),
            Some(Err(_)) | None => true,
        };
        if generated {
            self.permit = None;
        }
        Poll::Ready(item)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }
}
