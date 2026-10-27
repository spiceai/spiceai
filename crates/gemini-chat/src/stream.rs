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

//! A chat completion's stream of chunks as a stream of Gemini responses.
//!
//! Each chunk with text becomes a response carrying that text as a part, so text reaches the
//! client as the model writes it. Gemini streams a function call whole, in one part, so the
//! argument deltas of each tool call are gathered by the call's index, and the calls are given,
//! in index order, in the last response, once their arguments are whole. The last response also
//! carries the `finishReason` and the usage of the whole answer. An error ends the stream.

use std::collections::BTreeMap;

use async_openai::types::chat::{
    ChatCompletionResponseStream, CompletionUsage, CreateChatCompletionStreamResponse,
    FinishReason as ChatFinishReason,
};
use futures::StreamExt;
use gemini_api::{GeminiError, GenerateContentResponse, GenerateContentStream, Part};

use crate::response::{
    finish_reason, function_call, gemini_error, response, response_id, usage_metadata,
};

/// A tool call gathered from its deltas.
#[derive(Debug, Default)]
struct StreamedCall {
    id: String,
    name: String,
    arguments: String,
}

/// Turns chat completion chunks into Gemini responses.
#[derive(Debug)]
pub struct Translator {
    /// The model the request named, which every response names as its `modelVersion`.
    model: String,
    response_id: String,
    /// The tool calls so far, by their chat completion index.
    calls: BTreeMap<u32, StreamedCall>,
    finish_reason: Option<ChatFinishReason>,
    usage: Option<CompletionUsage>,
}

impl Translator {
    /// A translator for the answer `response_id` of `model`.
    #[must_use]
    pub fn new(model: String, response_id: String) -> Self {
        Self {
            model,
            response_id,
            calls: BTreeMap::new(),
            finish_reason: None,
            usage: None,
        }
    }

    /// The response for one chunk: its text, or `None` when it has none.
    pub fn chunk(
        &mut self,
        chunk: CreateChatCompletionStreamResponse,
    ) -> Option<GenerateContentResponse> {
        if chunk.usage.is_some() {
            self.usage = chunk.usage;
        }
        // A request for one choice is answered with the choice at index 0.
        let choice = chunk.choices.into_iter().find(|choice| choice.index == 0)?;
        let delta = choice.delta;
        for call in delta.tool_calls.into_iter().flatten() {
            let streamed = self.calls.entry(call.index).or_default();
            if let Some(id) = call.id.filter(|id| !id.is_empty())
                && streamed.id.is_empty()
            {
                streamed.id = id;
            }
            if let Some(function) = call.function {
                if let Some(name) = function.name.filter(|name| !name.is_empty())
                    && streamed.name.is_empty()
                {
                    streamed.name = name;
                }
                if let Some(arguments) = function.arguments {
                    streamed.arguments.push_str(&arguments);
                }
            }
        }
        if choice.finish_reason.is_some() {
            self.finish_reason = choice.finish_reason;
        }
        let text: String = [delta.content, delta.refusal]
            .into_iter()
            .flatten()
            .collect();
        (!text.is_empty()).then(|| {
            response(
                &self.model,
                &self.response_id,
                vec![Part::text(text)],
                None,
                None,
            )
        })
    }

    /// The last response, once the chunks ran out: the function calls, whole, the finish reason
    /// and the usage of the whole answer.
    ///
    /// # Errors
    ///
    /// Returns `INTERNAL` when the chunks ran out before the completion finished, since the
    /// answer was cut short and a finish reason would present it as whole; and when a function
    /// call has no name, or arguments that are not a JSON object.
    pub fn end(self) -> Result<GenerateContentResponse, GeminiError> {
        let Self {
            model,
            response_id,
            calls,
            finish_reason: chat_finish_reason,
            usage,
        } = self;
        if chat_finish_reason.is_none() {
            return Err(GeminiError::internal(format!(
                "Model '{model}' ended its stream before finishing the answer, so the answer is incomplete."
            )));
        }
        let cut_off = chat_finish_reason == Some(ChatFinishReason::Length);
        let mut parts = Vec::with_capacity(calls.len());
        for call in calls.into_values() {
            if call.name.is_empty() {
                if cut_off {
                    continue;
                }
                return Err(GeminiError::internal(format!(
                    "Model '{model}' streamed a function call without the function's name."
                )));
            }
            match function_call(&model, call.id, call.name, &call.arguments) {
                Ok(part) => parts.push(part),
                // The completion ran out of tokens in the middle of the call's arguments: the
                // answer ends with MAX_TOKENS without the call, as the unstreamed answer does.
                Err(_) if cut_off => {}
                Err(error) => return Err(error),
            }
        }
        Ok(response(
            &model,
            &response_id,
            parts,
            Some(finish_reason(chat_finish_reason)),
            usage_metadata(usage.as_ref()),
        ))
    }
}

struct State {
    chunks: ChatCompletionResponseStream,
    translator: Option<Translator>,
}

/// `chunks`, a chat completion's stream, as a stream of Gemini responses of `model`. An error
/// ends it.
#[must_use]
pub fn response_stream(
    chunks: ChatCompletionResponseStream,
    model: String,
) -> GenerateContentStream {
    let state = State {
        chunks,
        translator: Some(Translator::new(model, response_id())),
    };
    Box::pin(futures::stream::unfold(state, |mut state| async move {
        loop {
            let translator = state.translator.as_mut()?;
            match state.chunks.next().await {
                Some(Ok(chunk)) => {
                    if let Some(response) = translator.chunk(chunk) {
                        return Some((Ok(response), state));
                    }
                }
                Some(Err(error)) => {
                    state.translator = None;
                    return Some((Err(gemini_error(error)), state));
                }
                None => {
                    let last = state.translator.take()?.end();
                    return Some((last, state));
                }
            }
        }
    }))
}
