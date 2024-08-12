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

use std::sync::Arc;

use crate::datafusion::DataFusion;
use async_openai::{
    error::OpenAIError,
    types::{CreateEmbeddingRequest, CreateEmbeddingResponse, EmbeddingInput},
};
use async_trait::async_trait;
use llms::embeddings::{Embed, Result as EmbedResult};
use tracing::{Instrument, Span};

pub struct TaskEmbed {
    inner: Box<dyn Embed>,
    df: Arc<DataFusion>,
}

impl TaskEmbed {
    pub fn new(inner: Box<dyn Embed>, df: Arc<DataFusion>) -> Self {
        Self { inner, df }
    }
}

#[async_trait]
impl Embed for TaskEmbed {
    async fn embed<'b>(&'b mut self, input: EmbeddingInput) -> EmbedResult<Vec<Vec<f32>>> {
        let span = tracing::span!(target: "task_history", tracing::Level::INFO, "text_embed", input = %serde_json::to_string(&input).unwrap_or_default());

        match self.inner.embed(input).instrument(span.clone()).await {
            Ok(response) => {
                tracing::info!(name: "labels", target: "task_history", parent: &span, outputs_produced = response.len());
                Ok(response)
            }
            Err(e) => {
                tracing::error!(target: "task_history", parent: &span, "{e}");
                Err(e)
            }
        }
    }

    async fn health<'b>(&'b mut self) -> EmbedResult<()> {
        self.inner.health().await
    }

    fn size(&self) -> i32 {
        self.inner.size()
    }

    #[allow(clippy::cast_possible_truncation)]
    async fn embed_request<'b>(
        &'b mut self,
        req: CreateEmbeddingRequest,
    ) -> Result<CreateEmbeddingResponse, OpenAIError> {
        let span = tracing::span!(target: "task_history", tracing::Level::INFO, "text_embed", input = %serde_json::to_string(&req.input).unwrap_or_default());

        labels_from_request(&req, &span);
        match self.inner.embed_request(req).instrument(span.clone()).await {
            Ok(response) => {
                tracing::info!(name: "labels", target: "task_history", parent: &span, outputs_produced = response.data.len());
                Ok(response)
            }
            Err(e) => {
                tracing::error!(target: "task_history", parent: &span, "{e}");
                Err(e)
            }
        }
    }
}

fn labels_from_request(req: &CreateEmbeddingRequest, span: &Span) {
    let _guard = span.enter();
    tracing::info!(name: "labels", target: "task_history", model = req.model);

    if let Some(encoding_format) = &req.encoding_format {
        let encoding_format_str = match encoding_format {
            async_openai::types::EncodingFormat::Base64 => "base64",
            async_openai::types::EncodingFormat::Float => "float",
        };
        tracing::info!(name: "labels", target: "task_history", encoding_format = %encoding_format_str);
    }
    if let Some(user) = &req.user {
        tracing::info!(name: "labels", target: "task_history", user = %user);
    }

    if let Some(dims) = req.dimensions {
        tracing::info!(name: "labels", target: "task_history", dimensions = %dims);
    }
}
