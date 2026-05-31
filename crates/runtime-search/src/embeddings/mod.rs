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

pub mod common;
pub mod execution_plan;
pub mod table;

use std::sync::Arc;

use chunking::{Chunker, ChunkingConfig};
use llms::embeddings::{Embed, Error as EmbedError};
use std::collections::HashMap;
use tokio::sync::RwLock;

pub type EmbeddingModelStore = HashMap<String, Arc<dyn Embed>>;

pub async fn construct_chunker(
    model_name: &str,
    chunk_config: &ChunkingConfig<'_>,
    embedding_models: &Arc<RwLock<EmbeddingModelStore>>,
) -> Result<Arc<dyn Chunker>, EmbedError> {
    let embedding_models_guard = embedding_models.read().await;
    let Some(embed_model) = embedding_models_guard.get(model_name) else {
        return Err(EmbedError::ModelDoesNotExist {
            model_name: model_name.to_string(),
        });
    };
    embed_model.chunker(chunk_config)
}
