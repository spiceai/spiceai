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

use std::sync::Arc;

use async_openai::types::EmbeddingInput;
use async_trait::async_trait;
use data_components::s3_vectors::query_provider::ComputeQueryVector;

use snafu::ResultExt;

use crate::model::EmbeddingModelStore;
use tokio::sync::RwLock;

#[derive(Debug)]
pub struct ComputeQuery {
    pub model_name: String,
    pub embedding_models: Arc<RwLock<EmbeddingModelStore>>,
}

#[async_trait]
impl ComputeQueryVector for ComputeQuery {
    async fn compute_vector(
        &self,
        query: &str,
    ) -> Result<Vec<f32>, Box<dyn std::error::Error + Send + Sync>> {
        let models = self.embedding_models.try_read().boxed()?;
        let Some(embedding_model) = models.get(&self.model_name) else {
            return Err(Box::from(format!(
                "Vector index requires '{}' embedding model, but is not available.",
                self.model_name
            )));
        };
        let mut resp = embedding_model
            .embed(EmbeddingInput::String(query.to_string()))
            .await
            .boxed()?;
        let Some(query_vector) = resp.pop() else {
            return Err(Box::from(format!(
                "Embedding model '{}' produced no embedding for the query '{query}'.",
                self.model_name,
            )));
        };

        Ok(query_vector)
    }
}
