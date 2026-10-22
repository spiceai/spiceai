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
//! sends one block at a time, start to stop, while a model may interleave the arguments of its
//! parallel tool calls: a call that starts before the open call's arguments are whole is held
//! back, and given whole once the open block ends, before any text that follows. A tool call
//! continued after its block ended cannot be streamed, so it ends the stream with an error.

use std::collections::{HashSet, VecDeque};

use async_openai::types::chat::{
    ChatCompletionResponseStream, CompletionUsage, CreateChatCompletionStreamResponse, FinishReason,
};
use futures::StreamExt;
use messages_api::stream::{ContentBlockDelta, DeltaUsage, MessageDelta, StreamEvent};
use messages_api::types::{ContentBlock, Message, StopReason};
use messages_api::{MessageStream, MessagesError};
use serde_json::{Map, Value};

use crate::response::{
    input_tokens, message_id, messages_error, stop_reason, tool_input, tool_use_id,
};

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

/// A tool call held back while another streams, to be given whole after it.
#[derive(Debug)]
struct HeldToolCall {
    tool_call: u32,
    id: String,
    name: String,
    arguments: String,
}

/// Turns chat completion chunks into Message events.
#[derive(Debug)]
pub struct Translator {
    /// The model the request named, which a chunk without one is answered as.
    model: String,
    started: bool,
    open: Option<OpenBlock>,
    /// The tool the open `tool_use` block calls.
    tool_name: String,
    /// The arguments the open `tool_use` block has streamed so far.
    arguments: String,
    /// The tool calls held back while the open one streams, in the order they started.
    held: Vec<HeldToolCall>,
    next_index: usize,
    /// The chat completion indexes of the tool calls seen, streamed or held back.
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
            tool_name: String::new(),
            arguments: String::new(),
            held: Vec::new(),
            next_index: 0,
            tool_calls: HashSet::new(),
            finish_reason: None,
            refused: false,
            usage: None,
        }
    }

    fn start(
        &mut self,
        chunk: Option<&CreateChatCompletionStreamResponse>,
        events: &mut Vec<StreamEvent>,
    ) {
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

    /// Whether the completion ran out of tokens, so its last block may have been cut off.
    fn cut_off(&self) -> bool {
        self.finish_reason == Some(FinishReason::Length)
    }

    /// Ends the open block. A `tool_use` block's input has to be a JSON object, as in the
    /// unstreamed answer, unless the completion ran out of tokens in the middle of it.
    fn close(&mut self, events: &mut Vec<StreamEvent>) -> Result<(), MessagesError> {
        if let Some(open) = self.open.take() {
            if matches!(open, OpenBlock::ToolUse { .. }) && !self.cut_off() {
                tool_input(&self.model, &self.tool_name, &self.arguments)?;
            }
            events.push(StreamEvent::ContentBlockStop {
                index: open.index(),
            });
        }
        self.arguments.clear();
        Ok(())
    }

    /// Ends the open block, then gives each held-back tool call whole, in the order they started.
    fn release_held(&mut self, events: &mut Vec<StreamEvent>) -> Result<(), MessagesError> {
        self.close(events)?;
        for held in std::mem::take(&mut self.held) {
            match tool_input(&self.model, &held.name, &held.arguments) {
                Ok(_) => {}
                // The completion ran out of tokens in the middle of the call's arguments: it is
                // left out, as in the unstreamed answer, since it was never made whole.
                Err(_) if self.cut_off() => continue,
                Err(error) => return Err(error),
            }
            let index = self.open(
                ContentBlock::ToolUse {
                    id: held.id,
                    name: held.name,
                    input: Value::Object(Map::new()),
                },
                events,
            )?;
            if !held.arguments.is_empty() {
                events.push(StreamEvent::ContentBlockDelta {
                    index,
                    delta: ContentBlockDelta::InputJsonDelta {
                        partial_json: held.arguments,
                    },
                });
            }
            events.push(StreamEvent::ContentBlockStop { index });
        }
        Ok(())
    }

    /// Whether a tool call starting now has to be held back: calls before it are, or the open
    /// tool call's arguments are not yet whole, so more of them are still to come.
    fn must_hold(&self) -> bool {
        !self.held.is_empty()
            || (matches!(self.open, Some(OpenBlock::ToolUse { .. }))
                && serde_json::from_str::<Value>(&self.arguments).is_err())
    }

    /// Streams `arguments` as more of the open `tool_use` block at `index`.
    fn input_json(&mut self, index: usize, arguments: String, events: &mut Vec<StreamEvent>) {
        if arguments.is_empty() {
            return;
        }
        self.arguments.push_str(&arguments);
        events.push(StreamEvent::ContentBlockDelta {
            index,
            delta: ContentBlockDelta::InputJsonDelta {
                partial_json: arguments,
            },
        });
    }

    fn open(
        &mut self,
        block: ContentBlock,
        events: &mut Vec<StreamEvent>,
    ) -> Result<usize, MessagesError> {
        self.close(events)?;
        let index = self.next_index;
        self.next_index += 1;
        events.push(StreamEvent::ContentBlockStart {
            index,
            content_block: block,
        });
        Ok(index)
    }

    fn text(&mut self, text: String, events: &mut Vec<StreamEvent>) -> Result<(), MessagesError> {
        let index = if let Some(OpenBlock::Text { index }) = self.open {
            index
        } else {
            self.release_held(events)?;
            let index = self.open(
                ContentBlock::Text {
                    text: String::new(),
                },
                events,
            )?;
            self.open = Some(OpenBlock::Text { index });
            index
        };
        events.push(StreamEvent::ContentBlockDelta {
            index,
            delta: ContentBlockDelta::TextDelta { text },
        });
        Ok(())
    }

    /// The events for one chunk.
    ///
    /// # Errors
    ///
    /// Returns an `api_error` when the chunk continues a tool call whose block has already ended,
    /// starts a tool call without a name, or ends one whose arguments are not a JSON object.
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
            self.text(text, &mut events)?;
        }
        if let Some(refusal) = delta.refusal.filter(|text| !text.is_empty()) {
            self.refused = true;
            self.text(refusal, &mut events)?;
        }
        for call in delta.tool_calls.into_iter().flatten() {
            let (name, arguments) = call
                .function
                .map(|function| (function.name, function.arguments))
                .unwrap_or_default();
            let arguments = arguments.unwrap_or_default();
            if let Some(OpenBlock::ToolUse { index, tool_call }) = self.open
                && tool_call == call.index
            {
                self.input_json(index, arguments, &mut events);
                continue;
            }
            if let Some(held) = self
                .held
                .iter_mut()
                .find(|held| held.tool_call == call.index)
            {
                held.arguments.push_str(&arguments);
                continue;
            }
            if self.tool_calls.contains(&call.index) {
                return Err(MessagesError::api(format!(
                    "Model '{}' streamed more of a tool call after its content block had ended, which the Messages API cannot stream: it streams each content block whole before the next. Send the request without `stream`.",
                    self.model
                )));
            }
            let Some(name) = name.filter(|name| !name.is_empty()) else {
                return Err(MessagesError::api(format!(
                    "Model '{}' streamed a tool call without the tool's name.",
                    self.model
                )));
            };
            self.tool_calls.insert(call.index);
            let id = call
                .id
                .filter(|id| !id.is_empty())
                .unwrap_or_else(tool_use_id);
            if self.must_hold() {
                self.held.push(HeldToolCall {
                    tool_call: call.index,
                    id,
                    name,
                    arguments,
                });
                continue;
            }
            let index = self.open(
                ContentBlock::ToolUse {
                    id,
                    name: name.clone(),
                    input: Value::Object(Map::new()),
                },
                &mut events,
            )?;
            self.tool_name = name;
            self.open = Some(OpenBlock::ToolUse {
                index,
                tool_call: call.index,
            });
            self.input_json(index, arguments, &mut events);
        }
        if choice.finish_reason.is_some() {
            self.finish_reason = choice.finish_reason;
        }
        Ok(events)
    }

    /// The events that end the stream when the chunks ran out: the open block's stop, the tool
    /// calls held back, given whole, `message_delta` with the stop reason and the usage of the
    /// whole message, and `message_stop`.
    ///
    /// # Errors
    ///
    /// Returns an `api_error` when the chunks ran out before the completion finished: the message
    /// was cut short, and ending it with `message_stop` would present it as whole. Also when a
    /// tool call it ends has arguments that are not a JSON object.
    pub fn end(self) -> Result<Vec<StreamEvent>, MessagesError> {
        if self.finish_reason.is_none() {
            return Err(MessagesError::api(format!(
                "Model '{}' ended its stream before finishing the message, so the message is incomplete.",
                self.model
            )));
        }
        self.finish()
    }

    fn finish(mut self) -> Result<Vec<StreamEvent>, MessagesError> {
        let mut events = Vec::new();
        self.start(None, &mut events);
        self.release_held(&mut events)?;
        let stop_reason = if self.refused {
            StopReason::Refusal
        } else {
            stop_reason(self.finish_reason, !self.tool_calls.is_empty())
        };
        let usage = self
            .usage
            .as_ref()
            .map_or_else(DeltaUsage::default, |usage| {
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
        Ok(events)
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
    fn queue(&mut self, events: &[StreamEvent]) {
        self.queue.extend(events.iter().map(event_value));
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
                    Ok(events) => state.queue(&events),
                    Err(error) => {
                        state.translator = None;
                        state.queue.push_back(Err(error));
                    }
                },
                Some(Err(error)) => {
                    state.translator = None;
                    state.queue.push_back(Err(messages_error(error)));
                }
                None => match state.translator.take().map(Translator::end) {
                    Some(Ok(events)) => state.queue(&events),
                    Some(Err(error)) => state.queue.push_back(Err(error)),
                    None => {}
                },
            }
        }
    }))
}
