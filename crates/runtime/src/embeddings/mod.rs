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
pub mod common;
pub mod connector;
pub mod execution_plan;

pub mod index;
pub mod metrics;
pub mod table;
pub mod task;
pub mod udtf;

use std::sync::Arc;

use chunking::{Chunker, ChunkingConfig};
use llms::embeddings::Error as EmbedError;

use crate::model::EmbeddingModelStore;
use tokio::sync::RwLock;

/// Makes a [`Chunker`] from [`ChunkingConfig`] and a column's embedding model in [`EmbeddingModelStore`].
async fn construct_chunker(
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

#[cfg(test)]
pub(crate) mod tests {
    use datafusion::common::utils::quote_identifier;

    use crate::{embedding_col, offset_col};

    #[test]
    fn test_quoting_embedding_columns() {
        // lowercase
        assert_eq!(offset_col!("embedding"), "embedding_offset");
        assert_eq!(embedding_col!("embedding"), "embedding_embedding");
        assert_eq!(
            quote_identifier(&offset_col!("embedding")),
            "embedding_offset"
        );
        assert_eq!(
            quote_identifier(&embedding_col!("embedding")),
            "embedding_embedding"
        );
        assert_eq!(
            offset_col!(quote_identifier("embedding")),
            "embedding_offset"
        );
        assert_eq!(
            embedding_col!(quote_identifier("embedding")),
            "embedding_embedding"
        );

        // mixed case
        assert_eq!(offset_col!("Embedding"), "Embedding_offset");
        assert_eq!(embedding_col!("Embedding"), "Embedding_embedding");
        assert_eq!(
            quote_identifier(&offset_col!("Embedding")),
            "\"Embedding_offset\""
        );

        assert_eq!(
            quote_identifier(&embedding_col!("Embedding")),
            "\"Embedding_embedding\""
        );

        assert_eq!(
            offset_col!(quote_identifier("Embedding")),
            "\"Embedding\"_offset"
        );

        assert_eq!(
            embedding_col!(quote_identifier("Embedding")),
            "\"Embedding\"_embedding"
        );
    }
}
