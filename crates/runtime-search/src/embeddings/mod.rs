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

#![allow(clippy::missing_errors_doc)]

pub mod common;
pub mod execution_plan;
pub mod table;
#[cfg(any(feature = "s3_vectors", feature = "elasticsearch", feature = "qdrant"))]
pub mod warm_index;

use std::sync::Arc;

use chunking::{Chunker, ChunkingConfig};
use llms::embeddings::{Embed, Error as EmbedError};
use runtime_acceleration::acceleration::{Acceleration, ZeroResultsAction};
use std::collections::HashMap;
use tokio::sync::RwLock;

pub type EmbeddingModelStore = HashMap<String, Arc<dyn Embed>>;

/// The read behavior a warm search tier should be built with for a table with this
/// `acceleration`, as `warm_index::with_memory_warm_index` takes it.
///
/// `None` — no warm tier at all — when the table has no enabled acceleration: a warm tier
/// starts empty on every process start and is filled by the acceleration write path, so
/// without acceleration nothing hydrates it and serving searches from it would narrow
/// results to whatever rows a scan happened to write, or to nothing at all.
#[must_use]
pub fn warm_index_on_zero_results(
    acceleration: Option<&Acceleration>,
) -> Option<&ZeroResultsAction> {
    acceleration
        .filter(|acceleration| acceleration.enabled)
        .map(|acceleration| &acceleration.on_zero_results)
}

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

#[cfg(test)]
mod tests {
    use super::*;

    /// Regression test for #12101: the acceleration write path is the only thing that fills a
    /// warm search tier, so an absent or disabled acceleration must yield no warm tier at all.
    #[test]
    fn on_zero_results_is_none_without_an_enabled_acceleration() {
        assert_eq!(
            warm_index_on_zero_results(None),
            None,
            "a table with no acceleration must get no warm tier"
        );

        let disabled = Acceleration {
            enabled: false,
            on_zero_results: ZeroResultsAction::UseSource,
            ..Acceleration::default()
        };
        assert_eq!(
            warm_index_on_zero_results(Some(&disabled)),
            None,
            "a disabled acceleration must get no warm tier"
        );

        let enabled = Acceleration {
            enabled: true,
            on_zero_results: ZeroResultsAction::UseSource,
            ..Acceleration::default()
        };
        assert_eq!(
            warm_index_on_zero_results(Some(&enabled)),
            Some(&ZeroResultsAction::UseSource),
            "an enabled acceleration passes its on_zero_results through"
        );
    }
}
