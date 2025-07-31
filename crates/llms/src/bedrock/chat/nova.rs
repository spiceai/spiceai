/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use std::sync::Arc;

use async_openai::{
    error::OpenAIError,
    types::{
        ChatCompletionRequestMessage, CreateChatCompletionRequest, CreateChatCompletionResponse,
        Stop,
    },
};
use aws_sdk_bedrockruntime::{
    operation::converse::builders::ConverseFluentBuilder,
    primitives::Blob,
    types::{InferenceConfiguration, Message},
};
use serde_json::Value;

use crate::bedrock::{BedrockClient, chat::BedrockLlmConversion};

pub struct BedrockNovaConversion {}

impl BedrockLlmConversion for BedrockNovaConversion {
    #[allow(clippy::deprecated)]
    fn to_converse(
        &self,
        client: Arc<BedrockClient>,
        req: CreateChatCompletionRequest,
    ) -> Result<ConverseFluentBuilder, OpenAIError> {
        let system = req
            .messages
            .iter()
            .find(|&m| matches!(m, ChatCompletionRequestMessage::System(_)));

        // Cannot have system prompt.
        let messages: Vec<Message> = req.messages.iter().map(|m| {}).collect();

        let mut bldr = client
            .client
            .converse()
            .model_id(req.model.clone())
            .set_messages(Some(messages));

        if let Some(Value::Object(m)) = req.metadata {
            bldr = bldr.set_request_metadata(Some(
                m.into_iter().map(|(k, v)| (k, v.to_string())).collect(),
            ));
        };

        bldr = bldr.inference_config(
            InferenceConfiguration::builder()
                .set_max_tokens(
                    req.max_completion_tokens
                        .or(req.max_tokens)
                        .map(|u| u as i32),
                )
                .set_stop_sequences(req.stop.map(|stop| match stop {
                    Stop::String(s) => vec![s],
                    Stop::StringArray(arr) => arr.clone(),
                }))
                .set_temperature(req.temperature)
                .set_top_p(req.top_p)
                .build(),
        );

        // pub tools: Option<Vec<ChatCompletionTool>>,
        // pub tool_choice: Option<ChatCompletionToolChoiceOption>,
        Ok(bldr)
    }

    fn from_blob(&self, blob: Blob) -> Result<CreateChatCompletionResponse, OpenAIError> {
        Ok(CreateChatCompletionResponse::default())
    }
}
