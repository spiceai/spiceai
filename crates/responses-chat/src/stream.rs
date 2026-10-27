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

//! A chat completion's stream of chunks as a Response's stream of events.
//!
//! The stream opens with `response.created` and `response.in_progress`, and ends with one of
//! `response.completed`, `response.incomplete` or `response.failed`, which carries the whole
//! Response. Every event's `sequence_number` is one more than the one before it.
//!
//! In between, each output item is streamed whole before the next one starts, as the Responses
//! API streams them: `response.output_item.added`, its deltas, then `response.output_item.done`.
//! Text and a function call's arguments reach the client as the model writes them. A custom
//! tool's input is given once its call is whole, since the model writes it as the string inside
//! the JSON arguments of a function call.
//!
//! A model may interleave the argument chunks of its parallel tool calls, or write text in the
//! middle of them. What starts while the open item is a call whose arguments are not yet a whole
//! JSON value is held back, in the order it started, and given once the open call's arguments
//! are whole, or once the completion ends. A call continued after its item ended cannot be
//! streamed, so it fails the Response.
//!
//! The completion's usage comes in a chunk of its own after the last choice, so the Response
//! ends when the chunks do. Chunks that end before the completion finished, or an error from
//! the chat model, fail the Response with `response.failed`, and the stream ends there.

use std::collections::{HashSet, VecDeque};

use async_openai::error::OpenAIError;
use async_openai::types::chat::{
    ChatCompletionMessageToolCallChunk, ChatCompletionResponseStream, CompletionUsage,
    CreateChatCompletionStreamResponse, FinishReason,
};
use async_openai::types::responses::{
    OutputContent, OutputItem, OutputMessageContent, OutputStatus, RefusalContent, Response,
    ResponseCompletedEvent, ResponseContentPartAddedEvent, ResponseContentPartDoneEvent,
    ResponseCreatedEvent, ResponseCustomToolCallInputDeltaEvent,
    ResponseCustomToolCallInputDoneEvent, ResponseFailedEvent,
    ResponseFunctionCallArgumentsDeltaEvent, ResponseFunctionCallArgumentsDoneEvent,
    ResponseInProgressEvent, ResponseIncompleteEvent, ResponseOutputItemAddedEvent,
    ResponseOutputItemDoneEvent, ResponseRefusalDeltaEvent, ResponseRefusalDoneEvent,
    ResponseStream, ResponseStreamEvent, ResponseTextDeltaEvent, ResponseTextDoneEvent, Status,
};
use futures::StreamExt;

use crate::request::ToolTarget;
use crate::response::{
    ResponseContext, call_id, call_item, call_payload, error_object, finish, in_progress, is_whole,
    message_id, message_item, now, output_text, response_id, response_usage, server_error,
    tool_call_item_id,
};

/// An index into the output, or into a message's content, as the Responses API numbers it.
fn index(position: usize) -> u32 {
    u32::try_from(position).unwrap_or(u32::MAX)
}

/// A content part of the open message: its text, or its refusal.
#[derive(Debug)]
struct Part {
    refusal: bool,
    text: String,
}

impl Part {
    fn content(refusal: bool, text: String) -> OutputContent {
        if refusal {
            OutputContent::Refusal(RefusalContent { refusal: text })
        } else {
            OutputContent::OutputText(output_text(text))
        }
    }

    fn message_content(self) -> OutputMessageContent {
        if self.refusal {
            OutputMessageContent::Refusal(RefusalContent { refusal: self.text })
        } else {
            OutputMessageContent::OutputText(output_text(self.text))
        }
    }
}

#[derive(Debug)]
struct OpenMessage {
    output_index: u32,
    id: String,
    /// Its parts, of which the last is open.
    parts: Vec<Part>,
}

#[derive(Debug)]
struct OpenCall {
    output_index: u32,
    id: String,
    /// The index the chat completion gives the call in its chunks.
    chat_index: u32,
    call_id: String,
    target: ToolTarget,
    /// The arguments streamed so far.
    arguments: String,
}

/// The output item the stream has open.
#[derive(Debug)]
enum Open {
    Message(OpenMessage),
    Call(OpenCall),
}

/// What started while the open item was a call whose arguments were not whole, held back to
/// be given after it.
#[derive(Debug)]
enum Pending {
    Content {
        refusal: bool,
        text: String,
    },
    Call {
        chat_index: u32,
        call_id: String,
        function: String,
        arguments: String,
    },
}

/// Turns chat completion chunks into Response events.
#[derive(Debug)]
pub struct Translator {
    context: ResponseContext,
    /// The Response in progress, which each event that carries the Response starts from.
    response: Response,
    sequence_number: u64,
    /// The events made since they were last taken.
    events: Vec<ResponseStreamEvent>,
    /// The output items that are done, in order.
    output: Vec<OutputItem>,
    open: Option<Open>,
    held: VecDeque<Pending>,
    /// The chat completion indexes of the tool calls whose items are done.
    done_calls: HashSet<u32>,
    finish_reason: Option<FinishReason>,
    usage: Option<CompletionUsage>,
    /// Whether the stream has ended, with or without the Response.
    finished: bool,
}

impl Translator {
    #[must_use]
    pub fn new(context: ResponseContext) -> Self {
        let response = in_progress(&context, response_id(), now());
        Self {
            context,
            response,
            sequence_number: 0,
            events: Vec::new(),
            output: Vec::new(),
            open: None,
            held: VecDeque::new(),
            done_calls: HashSet::new(),
            finish_reason: None,
            usage: None,
            finished: false,
        }
    }

    /// Whether the stream has ended: it sent the event that ends the Response.
    #[must_use]
    pub fn is_finished(&self) -> bool {
        self.finished
    }

    fn emit(&mut self, event: impl FnOnce(u64) -> ResponseStreamEvent) {
        let sequence_number = self.sequence_number;
        self.sequence_number += 1;
        self.events.push(event(sequence_number));
    }

    fn take_events(&mut self) -> Vec<ResponseStreamEvent> {
        std::mem::take(&mut self.events)
    }

    /// The events that open the stream: `response.created` and `response.in_progress`.
    pub fn start(&mut self) -> Vec<ResponseStreamEvent> {
        let response = self.response.clone();
        self.emit(|sequence_number| {
            ResponseStreamEvent::ResponseCreated(ResponseCreatedEvent {
                sequence_number,
                response: response.clone(),
            })
        });
        self.emit(|sequence_number| {
            ResponseStreamEvent::ResponseInProgress(ResponseInProgressEvent {
                sequence_number,
                response,
            })
        });
        self.take_events()
    }

    /// The events for one chunk. A chunk the stream cannot carry fails the Response.
    pub fn chunk(&mut self, chunk: CreateChatCompletionStreamResponse) -> Vec<ResponseStreamEvent> {
        if self.finished {
            return Vec::new();
        }
        if let Err(error) = self.translate(chunk) {
            self.fail_with(&error);
        }
        self.take_events()
    }

    /// The events that end the stream when the chunks ran out: the items still open or held
    /// back, then `response.completed`, or `response.incomplete` when the completion ran out of
    /// tokens or was filtered. Chunks that ran out before the completion finished fail the
    /// Response, which was cut short: ending it any other way would present it as whole.
    pub fn end(&mut self) -> Vec<ResponseStreamEvent> {
        if self.finished {
            return Vec::new();
        }
        if self.finish_reason.is_none() {
            let error = server_error(format!(
                "Model '{}' ended its stream before finishing the response, so the response is incomplete.",
                self.context.model
            ));
            self.fail_with(&error);
            return self.take_events();
        }
        if let Err(error) = self.advance(true) {
            self.fail_with(&error);
            return self.take_events();
        }
        let mut response = self.response.clone();
        response.output = std::mem::take(&mut self.output);
        finish(
            &mut response,
            self.finish_reason,
            self.usage.as_ref(),
            now(),
        );
        let incomplete = response.status == Status::Incomplete;
        self.emit(|sequence_number| {
            if incomplete {
                ResponseStreamEvent::ResponseIncomplete(ResponseIncompleteEvent {
                    sequence_number,
                    response,
                })
            } else {
                ResponseStreamEvent::ResponseCompleted(ResponseCompletedEvent {
                    sequence_number,
                    response,
                })
            }
        });
        self.finished = true;
        self.take_events()
    }

    /// The event that fails the Response with `error`, the chat model's error.
    pub fn fail(&mut self, error: &OpenAIError) -> Vec<ResponseStreamEvent> {
        if self.finished {
            return Vec::new();
        }
        self.fail_with(error);
        self.take_events()
    }

    /// Fails the Response with `error`. Its output is the items that were done.
    fn fail_with(&mut self, error: &OpenAIError) {
        let mut response = self.response.clone();
        response.status = Status::Failed;
        response.error = Some(error_object(error));
        response.output = std::mem::take(&mut self.output);
        response.usage = self.usage.as_ref().map(response_usage);
        self.emit(|sequence_number| {
            ResponseStreamEvent::ResponseFailed(ResponseFailedEvent {
                sequence_number,
                response,
            })
        });
        self.finished = true;
    }

    fn translate(&mut self, chunk: CreateChatCompletionStreamResponse) -> Result<(), OpenAIError> {
        if chunk.usage.is_some() {
            self.usage = chunk.usage;
        }
        // A request for one choice is answered with the choice at index 0.
        let Some(choice) = chunk.choices.into_iter().find(|choice| choice.index == 0) else {
            return Ok(());
        };
        let delta = choice.delta;
        if let Some(text) = delta.content.filter(|text| !text.is_empty()) {
            self.content(false, text)?;
        }
        if let Some(refusal) = delta.refusal.filter(|text| !text.is_empty()) {
            self.content(true, refusal)?;
        }
        for call in delta.tool_calls.into_iter().flatten() {
            self.tool_call(call)?;
        }
        if choice.finish_reason.is_some() {
            self.finish_reason = choice.finish_reason;
        }
        Ok(())
    }

    /// Whether the completion ran out of tokens, so what it was writing was cut off.
    fn cut_off(&self) -> bool {
        self.finish_reason == Some(FinishReason::Length)
    }

    /// Text, or a refusal: more of the open message, or held back behind the open call.
    fn content(&mut self, refusal: bool, text: String) -> Result<(), OpenAIError> {
        if self.held.is_empty() && matches!(self.open, Some(Open::Message(_))) {
            if let Some(Open::Message(message)) = self.open.take() {
                let message = self.extend(message, refusal, text);
                self.open = Some(Open::Message(message));
            }
            return Ok(());
        }
        match self.held.back_mut() {
            Some(Pending::Content {
                refusal: held_refusal,
                text: held,
            }) if *held_refusal == refusal => held.push_str(&text),
            _ => self.held.push_back(Pending::Content { refusal, text }),
        }
        self.advance(false)
    }

    fn tool_call(&mut self, call: ChatCompletionMessageToolCallChunk) -> Result<(), OpenAIError> {
        let (name, arguments) = call
            .function
            .map(|function| (function.name, function.arguments.unwrap_or_default()))
            .unwrap_or_default();
        if let Some(Open::Call(open)) = &mut self.open
            && open.chat_index == call.index
        {
            if !arguments.is_empty() {
                open.arguments.push_str(&arguments);
                if !open.target.custom {
                    let (item_id, output_index) = (open.id.clone(), open.output_index);
                    self.emit(|sequence_number| {
                        ResponseStreamEvent::ResponseFunctionCallArgumentsDelta(
                            ResponseFunctionCallArgumentsDeltaEvent {
                                sequence_number,
                                item_id,
                                output_index,
                                delta: arguments,
                            },
                        )
                    });
                }
            }
            return self.advance(false);
        }
        if let Some(Pending::Call {
            arguments: held, ..
        }) = self.held.iter_mut().find(|pending| {
            matches!(pending, Pending::Call { chat_index, .. } if *chat_index == call.index)
        }) {
            held.push_str(&arguments);
            return Ok(());
        }
        if self.done_calls.contains(&call.index) {
            if arguments.trim().is_empty() {
                return Ok(());
            }
            return Err(server_error(format!(
                "Model '{}' streamed more arguments for a tool call after they were a whole JSON value, so the call cannot be streamed. Send the request without `stream`.",
                self.context.model
            )));
        }
        let Some(function) = name.filter(|name| !name.is_empty()) else {
            return Err(server_error(format!(
                "Model '{}' streamed a tool call without the tool's name.",
                self.context.model
            )));
        };
        self.held.push_back(Pending::Call {
            chat_index: call.index,
            call_id: call.id.filter(|id| !id.is_empty()).unwrap_or_else(call_id),
            function,
            arguments,
        });
        self.advance(false)
    }

    /// Ends the open item while something waits behind it and it can end (a call, once its
    /// arguments are whole), and opens what waits; or, `at_end`, ends everything.
    fn advance(&mut self, at_end: bool) -> Result<(), OpenAIError> {
        loop {
            let close = match &self.open {
                Some(Open::Message(_)) => at_end || !self.held.is_empty(),
                Some(Open::Call(call)) => {
                    at_end || (!self.held.is_empty() && is_whole(&call.arguments))
                }
                None => {
                    let Some(next) = self.held.pop_front() else {
                        return Ok(());
                    };
                    self.open_pending(next)?;
                    continue;
                }
            };
            if !close {
                return Ok(());
            }
            match self.open.take() {
                Some(Open::Message(message)) => self.close_message(message, at_end),
                Some(Open::Call(call)) => self.close_call(call, at_end)?,
                None => {}
            }
        }
    }

    fn open_pending(&mut self, pending: Pending) -> Result<(), OpenAIError> {
        match pending {
            Pending::Content { refusal, text } => {
                let mut message = self.open_message();
                message = self.extend(message, refusal, text);
                // What was held back in a row is one message.
                while let Some(Pending::Content { .. }) = self.held.front() {
                    if let Some(Pending::Content { refusal, text }) = self.held.pop_front() {
                        message = self.extend(message, refusal, text);
                    }
                }
                self.open = Some(Open::Message(message));
            }
            Pending::Call {
                chat_index,
                call_id,
                function,
                arguments,
            } => {
                let call = self.open_call(chat_index, call_id, &function, arguments)?;
                self.open = Some(Open::Call(call));
            }
        }
        Ok(())
    }

    fn open_message(&mut self) -> OpenMessage {
        let message = OpenMessage {
            output_index: index(self.output.len()),
            id: message_id(),
            parts: Vec::new(),
        };
        let (output_index, item) = (
            message.output_index,
            message_item(message.id.clone(), Vec::new(), OutputStatus::InProgress),
        );
        self.emit(|sequence_number| {
            ResponseStreamEvent::ResponseOutputItemAdded(ResponseOutputItemAddedEvent {
                sequence_number,
                output_index,
                item,
            })
        });
        message
    }

    /// Streams `text` as more of `message`: of its open part when that is of the same kind, or
    /// of a new part after it.
    fn extend(&mut self, mut message: OpenMessage, refusal: bool, text: String) -> OpenMessage {
        if message
            .parts
            .last()
            .is_none_or(|part| part.refusal != refusal)
        {
            self.close_part(&message);
            let (item_id, output_index, content_index) = (
                message.id.clone(),
                message.output_index,
                index(message.parts.len()),
            );
            message.parts.push(Part {
                refusal,
                text: String::new(),
            });
            self.emit(|sequence_number| {
                ResponseStreamEvent::ResponseContentPartAdded(ResponseContentPartAddedEvent {
                    sequence_number,
                    item_id,
                    output_index,
                    content_index,
                    part: Part::content(refusal, String::new()),
                })
            });
        }
        let (item_id, output_index, content_index) = (
            message.id.clone(),
            message.output_index,
            index(message.parts.len().saturating_sub(1)),
        );
        if let Some(part) = message.parts.last_mut() {
            part.text.push_str(&text);
        }
        self.emit(|sequence_number| {
            if refusal {
                ResponseStreamEvent::ResponseRefusalDelta(ResponseRefusalDeltaEvent {
                    sequence_number,
                    item_id,
                    output_index,
                    content_index,
                    delta: text,
                })
            } else {
                ResponseStreamEvent::ResponseOutputTextDelta(ResponseTextDeltaEvent {
                    sequence_number,
                    item_id,
                    output_index,
                    content_index,
                    delta: text,
                    logprobs: Some(Vec::new()),
                })
            }
        });
        message
    }

    /// Ends the last part of `message`, if it has one.
    fn close_part(&mut self, message: &OpenMessage) {
        let Some(part) = message.parts.last() else {
            return;
        };
        let (item_id, output_index, content_index) = (
            message.id.clone(),
            message.output_index,
            index(message.parts.len() - 1),
        );
        let (refusal, text) = (part.refusal, part.text.clone());
        let done_item_id = item_id.clone();
        let done_text = text.clone();
        self.emit(|sequence_number| {
            if refusal {
                ResponseStreamEvent::ResponseRefusalDone(ResponseRefusalDoneEvent {
                    sequence_number,
                    item_id: done_item_id,
                    output_index,
                    content_index,
                    refusal: done_text,
                })
            } else {
                ResponseStreamEvent::ResponseOutputTextDone(ResponseTextDoneEvent {
                    sequence_number,
                    item_id: done_item_id,
                    output_index,
                    content_index,
                    text: done_text,
                    logprobs: Some(Vec::new()),
                })
            }
        });
        self.emit(|sequence_number| {
            ResponseStreamEvent::ResponseContentPartDone(ResponseContentPartDoneEvent {
                sequence_number,
                item_id,
                output_index,
                content_index,
                part: Part::content(refusal, text),
            })
        });
    }

    /// Ends `message`. One open `at_end` of a completion that ran out of tokens was cut off.
    fn close_message(&mut self, message: OpenMessage, at_end: bool) {
        self.close_part(&message);
        let status = if at_end && self.cut_off() {
            OutputStatus::Incomplete
        } else {
            OutputStatus::Completed
        };
        let OpenMessage {
            output_index,
            id,
            parts,
        } = message;
        let item = message_item(
            id,
            parts.into_iter().map(Part::message_content).collect(),
            status,
        );
        self.output.push(item.clone());
        self.emit(|sequence_number| {
            ResponseStreamEvent::ResponseOutputItemDone(ResponseOutputItemDoneEvent {
                sequence_number,
                output_index,
                item,
            })
        });
    }

    fn open_call(
        &mut self,
        chat_index: u32,
        call_id: String,
        function: &str,
        arguments: String,
    ) -> Result<OpenCall, OpenAIError> {
        let target = self.context.tool_names.target(function);
        let call = OpenCall {
            output_index: index(self.output.len()),
            id: tool_call_item_id(&target),
            chat_index,
            call_id,
            target,
            arguments,
        };
        let item = call_item(
            &call.target,
            call.id.clone(),
            call.call_id.clone(),
            String::new(),
            OutputStatus::InProgress,
        )?;
        let output_index = call.output_index;
        self.emit(|sequence_number| {
            ResponseStreamEvent::ResponseOutputItemAdded(ResponseOutputItemAddedEvent {
                sequence_number,
                output_index,
                item,
            })
        });
        if !call.target.custom && !call.arguments.is_empty() {
            let (item_id, delta) = (call.id.clone(), call.arguments.clone());
            self.emit(|sequence_number| {
                ResponseStreamEvent::ResponseFunctionCallArgumentsDelta(
                    ResponseFunctionCallArgumentsDeltaEvent {
                        sequence_number,
                        item_id,
                        output_index,
                        delta,
                    },
                )
            });
        }
        Ok(call)
    }

    /// Ends `call`. One whose arguments are not whole `at_end` of a completion that ran out of
    /// tokens was cut off.
    fn close_call(&mut self, call: OpenCall, at_end: bool) -> Result<(), OpenAIError> {
        self.done_calls.insert(call.chat_index);
        let status = if at_end && self.cut_off() && !is_whole(&call.arguments) {
            OutputStatus::Incomplete
        } else {
            OutputStatus::Completed
        };
        let OpenCall {
            output_index,
            id,
            call_id,
            target,
            arguments,
            ..
        } = call;
        let streamed_nothing = arguments.is_empty();
        let payload = call_payload(&target, arguments);
        if target.custom {
            if !payload.is_empty() {
                let (item_id, delta) = (id.clone(), payload.clone());
                self.emit(|sequence_number| {
                    ResponseStreamEvent::ResponseCustomToolCallInputDelta(
                        ResponseCustomToolCallInputDeltaEvent {
                            sequence_number,
                            output_index,
                            item_id,
                            delta,
                        },
                    )
                });
            }
            let (item_id, input) = (id.clone(), payload.clone());
            self.emit(|sequence_number| {
                ResponseStreamEvent::ResponseCustomToolCallInputDone(
                    ResponseCustomToolCallInputDoneEvent {
                        sequence_number,
                        output_index,
                        item_id,
                        input,
                    },
                )
            });
        } else {
            // The arguments of a call that streamed none are `{}`, which its deltas then say.
            if streamed_nothing {
                let (item_id, delta) = (id.clone(), payload.clone());
                self.emit(|sequence_number| {
                    ResponseStreamEvent::ResponseFunctionCallArgumentsDelta(
                        ResponseFunctionCallArgumentsDeltaEvent {
                            sequence_number,
                            item_id,
                            output_index,
                            delta,
                        },
                    )
                });
            }
            let (item_id, arguments, name) = (id.clone(), payload.clone(), target.name.clone());
            self.emit(|sequence_number| {
                ResponseStreamEvent::ResponseFunctionCallArgumentsDone(
                    ResponseFunctionCallArgumentsDoneEvent {
                        name: Some(name),
                        sequence_number,
                        item_id,
                        output_index,
                        arguments,
                    },
                )
            });
        }
        let item = call_item(&target, id, call_id, payload, status)?;
        self.output.push(item.clone());
        self.emit(|sequence_number| {
            ResponseStreamEvent::ResponseOutputItemDone(ResponseOutputItemDoneEvent {
                sequence_number,
                output_index,
                item,
            })
        });
        Ok(())
    }
}

struct State {
    chunks: ChatCompletionResponseStream,
    translator: Translator,
    queue: VecDeque<ResponseStreamEvent>,
}

/// `chunks`, a chat completion's stream, as the stream of events of the Response to the
/// request of `context`. Every item is `Ok`: a failure is the `response.failed` event, after
/// which the stream ends.
#[must_use]
pub fn response_stream(
    chunks: ChatCompletionResponseStream,
    context: ResponseContext,
) -> ResponseStream {
    let mut translator = Translator::new(context);
    let queue = translator.start().into();
    let state = State {
        chunks,
        translator,
        queue,
    };
    Box::pin(futures::stream::unfold(state, |mut state| async move {
        loop {
            if let Some(event) = state.queue.pop_front() {
                return Some((Ok(event), state));
            }
            if state.translator.is_finished() {
                return None;
            }
            let events = match state.chunks.next().await {
                Some(Ok(chunk)) => state.translator.chunk(chunk),
                Some(Err(error)) => state.translator.fail(&error),
                None => state.translator.end(),
            };
            state.queue.extend(events);
        }
    }))
}
