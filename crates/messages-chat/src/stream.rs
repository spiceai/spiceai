/*
Copyright 2026 The Spice.ai OSS Authors

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

//! A chat completion's stream of chunks as a Message's stream of events.
//!
//! A chunk's text extends the open text block, and a chunk's tool call the open `tool_use`
//! block, so text and tool input reach the client as the model writes them. The Messages API
//! sends one block at a time, start to stop, and a stream that interleaves the arguments of two
//! tool calls cannot be told apart from one whose blocks were mixed up, so it ends with an
//! error instead.

use std::collections::{HashSet, VecDeque};

use async_openai::types::chat::{
    ChatCompletionResponseStream, CompletionUsage, CreateChatCompletionStreamResponse,
    FinishReason,
};
use futures::StreamExt;
use messages_api::stream::{ContentBlockDelta, DeltaUsage, MessageDelta, StreamEvent};
use messages_api::types::{ContentBlock, Message, StopReason};
use messages_api::{MessageStream, MessagesError};
use serde_json::{Map, Value};

use crate::response::{input_tokens, message_id, messages_error, stop_reason, tool_use_id};

/// The content block an event stream has open.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OpenBlock {
    Text { index: usize },
    ToolUse { index: usize, tool_call: u32 },
}

impl OpenBlock {
    fn index(self) -> usize {
        match self {
            OpenBlock::Text { index } | OpenBlock::ToolUse { index, .. } => index,
        }
    }
}

/// Turns chat completion chunks into Message events.
#[derive(Debug)]
pub struct Translator {
    /// The model the request named, which a chunk without one is answered as.
    model: String,
    started: bool,
    open: Option<OpenBlock>,
    next_index: usize,
    /// The chat completion indexes of the tool calls that have a block.
    tool_calls: HashSet<u32>,
    finish_reason: Option<FinishReason>,
    refused: bool,
    usage: Option<CompletionUsage>,
}

impl Translator {
    #[must_use]
    pub fn new(model: String) -> Self {
        Self {
            model,
            started: false,
            open: None,
            next_index: 0,
            tool_calls: HashSet::new(),
            finish_reason: None,
            refused: false,
            usage: None,
        }
    }

    fn start(&mut self, chunk: Option<&CreateChatCompletionStreamResponse>, events: &mut Vec<StreamEvent>) {
        if self.started {
            return;
        }
        self.started = true;
        let id = message_id(chunk.map(|chunk| chunk.id.as_str()).unwrap_or_default());
        let model = chunk
            .map(|chunk| chunk.model.clone())
            .filter(|model| !model.is_empty())
            .unwrap_or_else(|| self.model.clone());
        events.push(StreamEvent::MessageStart {
            message: Message::started(id, model),
        });
    }

    fn close(&mut self, events: &mut Vec<StreamEvent>) {
        if let Some(open) = self.open.take() {
            events.push(StreamEvent::ContentBlockStop {
                index: open.index(),
            });
        }
    }

    fn open(&mut self, block: ContentBlock, events: &mut Vec<StreamEvent>) -> usize {
        self.close(events);
        let index = self.next_index;
        self.next_index += 1;
        events.push(StreamEvent::ContentBlockStart {
            index,
            content_block: block,
        });
        index
    }

    fn text(&mut self, text: String, events: &mut Vec<StreamEvent>) {
        let index = match self.open {
            Some(OpenBlock::Text { index }) => index,
            _ => {
                let index = self.open(
                    ContentBlock::Text {
                        text: String::new(),
                    },
                    events,
                );
                self.open = Some(OpenBlock::Text { index });
                index
            }
        };
        events.push(StreamEvent::ContentBlockDelta {
            index,
            delta: ContentBlockDelta::TextDelta { text },
        });
    }

    /// The events for one chunk.
    ///
    /// # Errors
    ///
    /// Returns an `api_error` when the chunk continues a tool call whose block has already been
    /// closed, or starts a tool call without a name.
    pub fn chunk(
        &mut self,
        chunk: CreateChatCompletionStreamResponse,
    ) -> Result<Vec<StreamEvent>, MessagesError> {
        let mut events = Vec::new();
        self.start(Some(&chunk), &mut events);
        if chunk.usage.is_some() {
            self.usage = chunk.usage;
        }
        // A request for one choice is answered with the choice at index 0.
        let Some(choice) = chunk.choices.into_iter().find(|choice| choice.index == 0) else {
            return Ok(events);
        };
        let delta = choice.delta;
        if let Some(text) = delta.content.filter(|text| !text.is_empty()) {
            self.text(text, &mut events);
        }
        if let Some(refusal) = delta.refusal.filter(|text| !text.is_empty()) {
            self.refused = true;
            self.text(refusal, &mut events);
        }
        for call in delta.tool_calls.into_iter().flatten() {
            let (name, arguments) = call
                .function
                .map(|function| (function.name, function.arguments))
                .unwrap_or_default();
            let index = match self.open {
                Some(OpenBlock::ToolUse { index, tool_call }) if tool_call == call.index => index,
                _ if self.tool_calls.contains(&call.index) => {
                    return Err(MessagesError::api(format!(
                        "Model '{}' streamed the arguments of its tool calls interleaved, which the Messages API cannot stream: each tool call has to be streamed whole before the next. Send the request without `stream`.",
                        self.model
                    )));
                }
                _ => {
                    let Some(name) = name.filter(|name| !name.is_empty()) else {
                        return Err(MessagesError::api(format!(
                            "Model '{}' streamed a tool call without the tool's name.",
                            self.model
                        )));
                    };
                    self.tool_calls.insert(call.index);
                    let index = self.open(
                        ContentBlock::ToolUse {
                            id: call.id.filter(|id| !id.is_empty()).unwrap_or_else(tool_use_id),
                            name,
                            input: Value::Object(Map::new()),
                        },
                        &mut events,
                    );
                    self.open = Some(OpenBlock::ToolUse {
                        index,
                        tool_call: call.index,
                    });
                    index
                }
            };
            if let Some(arguments) = arguments.filter(|arguments| !arguments.is_empty()) {
                events.push(StreamEvent::ContentBlockDelta {
                    index,
                    delta: ContentBlockDelta::InputJsonDelta {
                        partial_json: arguments,
                    },
                });
            }
        }
        if choice.finish_reason.is_some() {
            self.finish_reason = choice.finish_reason;
        }
        Ok(events)
    }

    /// The events that end the stream: the open block's stop, `message_delta` with the stop
    /// reason and the usage of the whole message, and `message_stop`.
    #[must_use]
    pub fn finish(mut self) -> Vec<StreamEvent> {
        let mut events = Vec::new();
        self.start(None, &mut events);
        self.close(&mut events);
        let stop_reason = if self.refused {
            StopReason::Refusal
        } else {
            stop_reason(self.finish_reason, !self.tool_calls.is_empty())
        };
        let usage = self.usage.as_ref().map_or_else(DeltaUsage::default, |usage| {
            let (input, cache_read) = input_tokens(usage);
            DeltaUsage {
                output_tokens: u64::from(usage.completion_tokens),
                input_tokens: Some(input),
                cache_creation_input_tokens: None,
                cache_read_input_tokens: cache_read,
            }
        });
        events.push(StreamEvent::MessageDelta {
            delta: MessageDelta {
                stop_reason: Some(stop_reason),
                stop_sequence: None,
            },
            usage,
        });
        events.push(StreamEvent::MessageStop);
        events
    }
}

fn event_value(event: &StreamEvent) -> Result<Value, MessagesError> {
    serde_json::to_value(event)
        .map_err(|e| MessagesError::api(format!("Failed to serialize a stream event: {e}")))
}

struct State {
    chunks: ChatCompletionResponseStream,
    translator: Option<Translator>,
    queue: VecDeque<Result<Value, MessagesError>>,
}

impl State {
    fn queue(&mut self, events: Vec<StreamEvent>) {
        self.queue
            .extend(events.iter().map(event_value));
    }
}

/// `chunks`, a chat completion's stream, as a Message's stream of events. An error ends it.
#[must_use]
pub fn message_stream(chunks: ChatCompletionResponseStream, model: String) -> MessageStream {
    let state = State {
        chunks,
        translator: Some(Translator::new(model)),
        queue: VecDeque::new(),
    };
    Box::pin(futures::stream::unfold(state, |mut state| async move {
        loop {
            if let Some(item) = state.queue.pop_front() {
                return Some((item, state));
            }
            let translator = state.translator.as_mut()?;
            match state.chunks.next().await {
                Some(Ok(chunk)) => match translator.chunk(chunk) {
                    Ok(events) => state.queue(events),
                    Err(error) => {
                        state.translator = None;
                        state.queue.push_back(Err(error));
                    }
                },
                Some(Err(error)) => {
                    state.translator = None;
                    state.queue.push_back(Err(messages_error(error)));
                }
                None => {
                    let events = state.translator.take().map(Translator::finish);
                    state.queue(events.unwrap_or_default());
                }
            }
        }
    }))
}
