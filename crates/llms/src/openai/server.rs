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

use async_openai::{
    error::OpenAIError,
    types::{
        CreateChatCompletionRequest, CreateChatCompletionResponse, CreateEmbeddingRequest,
        CreateEmbeddingResponse,
    },
};
use async_trait::async_trait;

/// A trait that mirrors the OpenAI API. Implementations of [`Server`] can automatically be used to create OpenAI-compatible services.
#[async_trait]
pub trait Server: Sync + Send {
    async fn chat(
        &mut self,
        req: CreateChatCompletionRequest,
    ) -> Result<CreateChatCompletionResponse, OpenAIError>;

    async fn embed(
        &mut self,
        req: CreateEmbeddingRequest,
    ) -> Result<CreateEmbeddingResponse, OpenAIError>;
}
