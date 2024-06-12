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

use crate::chat::{Chat, Error as ChatError, Result as ChatResult};
use crate::embeddings::{Embed, Error as EmbedError, Result as EmbedResult};

use async_openai::error::OpenAIError;
use async_openai::types::{
    ChatCompletionResponseStream, CreateChatCompletionRequest, CreateChatCompletionResponse,
    CreateEmbeddingRequest, CreateEmbeddingRequestArgs, CreateEmbeddingResponse,
};

use async_openai::{
    config::OpenAIConfig,
    types::{
        ChatCompletionRequestSystemMessageArgs, CreateChatCompletionRequestArgs, EmbeddingInput,
    },
    Client,
};
use async_stream::stream;
use async_trait::async_trait;
use futures::future::try_join_all;
use futures::{Stream, StreamExt};
use snafu::ResultExt;

pub const MAX_COMPLETION_TOKENS: u16 = 1024_u16; // Avoid accidentally using infinite tokens. Should think about this more.

pub(crate) const GPT3_5_TURBO_INSTRUCT: &str = "gpt-3.5-turbo";
pub(crate) const TEXT_EMBED_3_SMALL: &str = "text-embedding-3-small";

pub const DEFAULT_LLM_MODEL: &str = GPT3_5_TURBO_INSTRUCT;
pub const DEFAULT_EMBEDDING_MODEL: &str = TEXT_EMBED_3_SMALL;

pub struct Openai {
    client: Client<OpenAIConfig>,
    model: String,
}

impl Default for Openai {
    fn default() -> Self {
        Self::new(DEFAULT_LLM_MODEL.to_string(), None, None, None, None)
    }
}

impl Openai {
    #[must_use]
    pub fn new(
        model: String,
        api_base: Option<String>,
        api_key: Option<String>,
        org_id: Option<String>,
        project_id: Option<String>,
    ) -> Self {
        let mut cfg = OpenAIConfig::new()
            .with_org_id(org_id.unwrap_or_default())
            .with_project_id(project_id.unwrap_or_default());

        // If an API key is provided, use it. Otherwise use default from env variables.
        if let Some(api_key) = api_key {
            cfg = cfg.with_api_key(api_key);
        }
        if let Some(api_base) = api_base {
            cfg = cfg.with_api_base(api_base);
        }
        Self {
            client: Client::with_config(cfg),
            model,
        }
    }
}

#[async_trait]
impl Chat for Openai {
    async fn run(&mut self, prompt: String) -> ChatResult<Option<String>> {
        let req = CreateChatCompletionRequestArgs::default()
            .model(self.model.clone())
            .messages(vec![ChatCompletionRequestSystemMessageArgs::default()
                .content(prompt)
                .build()
                .boxed()
                .map_err(|source| ChatError::FailedToLoadTokenizer { source })?
                .into()])
            .build()
            .boxed()
            .map_err(|source| ChatError::FailedToLoadModel { source })?;

        let resp = self
            .chat_request(req)
            .await
            .boxed()
            .map_err(|source| ChatError::FailedToRunModel { source })?;

        Ok(resp
            .choices
            .into_iter()
            .next()
            .and_then(|c| c.message.content))
    }

    async fn stream<'a>(
        &mut self,
        prompt: String,
    ) -> ChatResult<Pin<Box<dyn Stream<Item = ChatResult<Option<String>>> + Send>>> {
        let req = CreateChatCompletionRequestArgs::default()
            .model(self.model.clone())
            .stream(true)
            .messages(vec![ChatCompletionRequestSystemMessageArgs::default()
                .content(prompt)
                .build()
                .boxed()
                .map_err(|source| ChatError::FailedToLoadTokenizer { source })?
                .into()])
            .build()
            .boxed()
            .map_err(|source| ChatError::FailedToLoadModel { source })?;
        let mut chat_stream = self
            .chat_stream(req)
            .await
            .boxed()
            .map_err(|source| ChatError::FailedToRunModel { source })?;
        Ok(Box::pin(stream! {
            while let Some(msg) = chat_stream.next().await {
                match msg {
                    Ok(resp) => {
                        yield Ok(resp.choices.into_iter().next().and_then(|c| c.delta.content));
                    }
                    Err(e) => {
                        yield Err(ChatError::FailedToRunModel { source: Box::new(e) });
                    }
                }
            }
        }))
    }

    async fn chat_stream(
        &mut self,
        req: CreateChatCompletionRequest,
    ) -> Result<ChatCompletionResponseStream, OpenAIError> {
        let mut inner_req = req.clone();
        inner_req.model.clone_from(&self.model);
        self.client.chat().create_stream(inner_req).await
    }

    /// An OpenAI-compatible interface for the `v1/chat/completion` `Chat` trait. If not implemented, the default
    /// implementation will be constructed based on the trait's [`run`] method.
    async fn chat_request(
        &mut self,
        req: CreateChatCompletionRequest,
    ) -> Result<CreateChatCompletionResponse, OpenAIError> {
        let mut inner_req = req.clone();
        inner_req.model.clone_from(&self.model);
        self.client.chat().create(inner_req).await
    }
}

#[async_trait]
impl Embed for Openai {
    async fn embed_request(
        &mut self,
        req: CreateEmbeddingRequest,
    ) -> Result<CreateEmbeddingResponse, OpenAIError> {
        let mut inner_req = req.clone();
        inner_req.model.clone_from(&self.model);
        self.client.embeddings().create(inner_req).await
    }

    async fn embed(&mut self, input: EmbeddingInput) -> EmbedResult<Vec<Vec<f32>>> {
        // Batch requests to OpenAI endpoint because "any array must be 2048 dimensions or less".
        // https://platform.openai.com/docs/api-reference/embeddings/create#embeddings-create-input
        let embed_batches = match input {
            EmbeddingInput::StringArray(ref batch) => batch
                .chunks(2048)
                .map(|chunk| EmbeddingInput::StringArray(chunk.to_vec()))
                .collect(),
            EmbeddingInput::ArrayOfIntegerArray(ref batch) => batch
                .chunks(2048)
                .map(|chunk| EmbeddingInput::ArrayOfIntegerArray(chunk.to_vec()))
                .collect(),
            _ => vec![input],
        };

        let request_batches_result: EmbedResult<Vec<CreateEmbeddingRequest>> = embed_batches
            .into_iter()
            .map(|batch| {
                CreateEmbeddingRequestArgs::default()
                    .model(self.model.clone())
                    .input(batch)
                    .build()
                    .boxed()
                    .map_err(|source| EmbedError::FailedToPrepareInput { source })
            })
            .collect();

        let embed_futures: Vec<_> = request_batches_result?
            .into_iter()
            .map(|req| {
                let local_client = self.client.clone();
                async move {
                    let embedding: Vec<Vec<f32>> = local_client
                        .embeddings()
                        .create(req)
                        .await
                        .boxed()
                        .map_err(|source| EmbedError::FailedToCreateEmbedding { source })?
                        .data
                        .iter()
                        .map(|d| d.embedding.clone())
                        .collect();
                    Ok::<Vec<Vec<f32>>, EmbedError>(embedding)
                }
            })
            .collect();

        let combined_results: Vec<Vec<f32>> = try_join_all(embed_futures)
            .await?
            .into_iter()
            .flatten()
            .collect();

        Ok(combined_results)
    }

    fn size(&self) -> i32 {
        match self.model.as_str() {
            "text-embedding-3-large" => 3_072,
            "text-embedding-3-small" | "text-embedding-ada-002" => 1_536,
            _ => 0, // unreachable. If not a valid model, it won't create embeddings.
        }
    }
}
