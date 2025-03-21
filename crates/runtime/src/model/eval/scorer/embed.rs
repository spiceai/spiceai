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
use tokio::sync::RwLock;

use crate::model::{eval::scorer::mean, EmbeddingModelStore};

use super::{extract_text, DatasetInput, DatasetOutput, Error, Scorer};

/// [`EmbedScorer`] scores based on the similarity of two [`DatasetOutput`] using the L2 distance of the outputs' embeddings.
pub struct EmbedScorer {
    models: Arc<RwLock<EmbeddingModelStore>>,
    model_name: String,

    /// Provide domain context for improved embedding comparison.
    prefix: Option<String>,
}

impl EmbedScorer {
    pub fn new(models: Arc<RwLock<EmbeddingModelStore>>, model_name: String) -> Self {
        Self {
            models,
            model_name,
            prefix: None,
        }
    }
}

#[async_trait]
impl Scorer for EmbedScorer {
    async fn score(
        &self,
        input: &DatasetInput,
        actual: &DatasetOutput,
        ideal: &DatasetOutput,
    ) -> super::Result<f32> {
        let prefix = self.prefix.as_deref().unwrap_or_default();
        let actual_input = format!("{prefix}{}", extract_text(actual),);
        let ideal_input = format!("{prefix}{}", extract_text(ideal));

        let model_lock = self.models.read().await;
        let Some(model) = model_lock.get(&self.model_name) else {
            return Err(Error::ErrorScoringCase {
                input: input.clone(),
                actual: actual.clone(),
                ideal: ideal.clone(),
                source: Box::from(format!(
                    "Embedding model {} not found in store. This is unexpected.",
                    self.model_name
                )),
            });
        };

        let embeddings = model
            .embed(EmbeddingInput::StringArray(vec![actual_input, ideal_input]))
            .await
            .map_err(|e| Error::ErrorScoringCase {
                input: input.clone(),
                actual: actual.clone(),
                ideal: ideal.clone(),
                source: Box::from(format!(
                    "Underlying embedding model failed to embed inputs: {e}"
                )),
            })?;

        let (Some(actual_vector), Some(ideal_vector)) = (embeddings.first(), embeddings.get(1))
        else {
            return Err(Error::ErrorScoringCase {
                input: input.clone(),
                actual: actual.clone(),
                ideal: ideal.clone(),
                source: Box::from(format!(
                    "Embedding model returned {} vectors, expected 2",
                    embeddings.len()
                )),
            });
        };
        if actual_vector.len() != ideal_vector.len() {
            return Err(Error::ErrorScoringCase {
                input: input.clone(),
                actual: actual.clone(),
                ideal: ideal.clone(),
                source: Box::from(format!(
                    "Embedding model returned vectors of different lengths: {} != {}",
                    actual_vector.len(),
                    ideal_vector.len()
                )),
            });
        }

        let distance = actual_vector
            .iter()
            .zip(ideal_vector.iter())
            .map(|(a, b)| (a - b).powi(2))
            .sum::<f32>()
            .sqrt();

        Ok(1.0 / (1.0 + distance))
    }

    fn metrics(&self, scores: &[f32]) -> Vec<(String, f32)> {
        vec![("mean".to_string(), mean(scores))]
    }
}
