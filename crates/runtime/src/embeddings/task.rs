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

use crate::{datafusion::DataFusion, task_history};
use async_openai::{
    error::OpenAIError,
    types::{CreateEmbeddingRequest, CreateEmbeddingResponse, EmbeddingInput},
};
use async_trait::async_trait;
use llms::embeddings::{Embed, Result as EmbedResult};

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
        let task_span = task_history::TaskSpan::new(
            Arc::clone(&self.df),
            uuid::Uuid::new_v4(),
            task_history::TaskType::TextEmbed,
            Arc::new(serde_json::to_string(&input).unwrap_or_default()),
            None,
        );

        match self.inner.embed(input).await {
            Ok(response) => {
                task_span.outputs_produced(response.len() as u64).finish();
                Ok(response)
            }
            Err(e) => {
                task_span.with_error_message(e.to_string().clone()).finish();
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
        let task_span = task_history::TaskSpan::new(
            Arc::clone(&self.df),
            uuid::Uuid::new_v4(),
            task_history::TaskType::TextEmbed,
            Arc::new(serde_json::to_string(&req.input).unwrap_or_default()),
            None,
        )
        .label("model".to_string(), req.model.clone());

        match self.inner.embed_request(req).await {
            Ok(response) => {
                task_span
                    .outputs_produced(response.data.len() as u64)
                    .finish();
                Ok(response)
            }
            Err(e) => {
                task_span.with_error_message(e.to_string().clone()).finish();
                Err(e)
            }
        }
    }
}
