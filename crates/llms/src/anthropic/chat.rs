/*
Copyright 2024 The Spice.ai OSS Authors

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
#![allow(clippy::missing_errors_doc)]
use std::pin::Pin;
use std::str::FromStr;

use crate::chat::nsql::SqlGeneration;
use crate::chat::{Chat, Error as ChatError, Result as ChatResult};
use async_openai::error::OpenAIError;
use async_openai::types::{
    ChatCompletionRequestMessage, ChatCompletionRequestMessageContentPartText,
    ChatCompletionRequestSystemMessage, ChatCompletionRequestSystemMessageContent,
    ChatCompletionRequestSystemMessageContentPart, ChatCompletionResponseStream,
    CreateChatCompletionRequest, CreateChatCompletionResponse, Stop,
};

use async_openai::types::{
    ChatCompletionRequestSystemMessageArgs, CreateChatCompletionRequestArgs,
};
use async_stream::stream;
use async_trait::async_trait;
use futures::{Stream, StreamExt};
use itertools::Itertools;
use snafu::ResultExt;
use tracing_futures::Instrument;

use super::types::{MessageCreateParams, MetadataParam, ModelVariant};
use super::Anthropic;

#[async_trait]
impl Chat for Anthropic {
    fn as_sql(&self) -> Option<&dyn SqlGeneration> {
        None
    }

    async fn run(&self, prompt: String) -> ChatResult<Option<String>> {
        Err(ChatError::UnsupportedModalityType {
            modality: "run".to_string(),
        })
    }

    async fn stream<'a>(
        &self,
        prompt: String,
    ) -> ChatResult<Pin<Box<dyn Stream<Item = ChatResult<Option<String>>> + Send>>> {
        Err(ChatError::UnsupportedModalityType {
            modality: "stream".to_string(),
        })
    }

    async fn chat_stream(
        &self,
        req: CreateChatCompletionRequest,
    ) -> Result<ChatCompletionResponseStream, OpenAIError> {
        Err(OpenAIError::InvalidArgument("()".to_string()))
    }

    async fn chat_request(
        &self,
        req: CreateChatCompletionRequest,
    ) -> Result<CreateChatCompletionResponse, OpenAIError> {
        Err(OpenAIError::InvalidArgument("()".to_string()))
    }
}

impl TryFrom<CreateChatCompletionRequest> for MessageCreateParams {
    type Error = OpenAIError;
    fn try_from(value: CreateChatCompletionRequest) -> Result<Self, Self::Error> {
        let model = ModelVariant::from_str(value.model.as_str())?;

        Ok(MessageCreateParams {
            top_k: value.top_logprobs.map(u32::from),
            top_p: value.top_p,
            temperature: value.temperature,
            max_tokens: value
                .max_completion_tokens
                .unwrap_or(model.default_max_tokens()),
            stream: value.stream,
            metadata: value
                .metadata
                .and_then(|m| m.get("user_id"))
                .and_then(|id| {
                    Some(MetadataParam {
                        user_id: id.as_str().map(String::from),
                    })
                }),
            model: model,
            stop_sequences: value.stop.map(|s| match s {
                Stop::String(s) => vec![s],
                Stop::StringArray(a) => a,
            }),
            system: system_message_from_messages(&value.messages),
        })
    }
}

fn system_message_from_messages(messages: &[ChatCompletionRequestMessage]) -> Option<String> {
    let system_messages: Vec<_> = messages
        .iter()
        .filter_map(|m| match m {
            ChatCompletionRequestMessage::System(ChatCompletionRequestSystemMessage {
                content,
                ..
            }) => match content {
                ChatCompletionRequestSystemMessageContent::Text(text) => Some(text.clone()),
                ChatCompletionRequestSystemMessageContent::Array(a) => {
                    let elements: Vec<_> = a
                        .iter()
                        .filter_map(|part| match part {
                            ChatCompletionRequestSystemMessageContentPart::Text(
                                ChatCompletionRequestMessageContentPartText { text },
                            ) => Some(text),
                            _ => None,
                        })
                        .cloned()
                        .collect();
                    Some(elements.as_slice().join("\n"))
                }
            },
            _ => None,
        })
        .collect();

    if system_messages.len() > 1 {
        tracing::warn!("More than one ({count}) system message found in messages. Concatenating into a single String.", count = system_messages.len());
    }
    if system_messages.is_empty() {
        None
    } else {
        Some(system_messages.join("\n"))
    }
}
