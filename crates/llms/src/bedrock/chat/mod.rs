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

use crate::bedrock::BedrockClient;

use crate::chat::Chat;
use crate::chat::nsql::SqlGeneration;
use async_openai::error::{ApiError, OpenAIError};
use async_openai::types::{
    ChatCompletionResponseStream, CompletionUsage, CreateChatCompletionRequest,
    CreateChatCompletionResponse, PromptTokensDetails,
};
use async_trait::async_trait;
use aws_sdk_bedrockruntime::operation::converse::ConverseOutput;
use aws_sdk_bedrockruntime::operation::converse::builders::ConverseFluentBuilder;
use aws_sdk_bedrockruntime::primitives::Blob;
use std::sync::Arc;

pub mod nova;

pub struct BedrockLlm {
    client: Arc<BedrockClient>,
    model_id: String,
    conversion: Arc<dyn BedrockLlmConversion>,
}

pub trait BedrockLlmConversion: Send + Sync {
    fn to_converse(
        &self,
        client: Arc<BedrockClient>,
        req: CreateChatCompletionRequest,
    ) -> Result<ConverseFluentBuilder, OpenAIError>;

    fn from_converse_output(
        &self,
        output: ConverseOutput,
    ) -> Result<CreateChatCompletionResponse, OpenAIError> {
        let usage = output.usage().map(|u| CompletionUsage {
            completion_tokens: u.output_tokens as u32,
            prompt_tokens: u.input_tokens as u32,
            total_tokens: u.total_tokens as u32,
            prompt_tokens_details: Some(PromptTokensDetails {
                cached_tokens: u.cache_read_input_tokens.map(|i| i as u32),
                audio_tokens: None,
            }),
            completion_tokens_details: None,
        });
        Ok(CreateChatCompletionResponse { usage })
    }
}

#[async_trait]
impl Chat for BedrockLlm {
    #[allow(deprecated)]
    async fn chat_stream(
        &self,
        req: CreateChatCompletionRequest,
    ) -> Result<ChatCompletionResponseStream, OpenAIError> {
        Err(OpenAIError::InvalidArgument(String::new()))
    }

    async fn chat_request(
        &self,
        req: CreateChatCompletionRequest,
    ) -> Result<CreateChatCompletionResponse, OpenAIError> {
        let z = self
            .client
            .do_converse(self.conversion.to_converse(self.client.clone(), req)?)
            .await
            .map_err(|e| {
                OpenAIError::ApiError(ApiError {
                    message: e.to_string(),
                    code: None,
                    r#type: None,
                    param: None,
                })
            })?;

        self.conversion.from_converse_output(z)
    }

    fn as_sql(&self) -> Option<&dyn SqlGeneration> {
        None
    }
}
