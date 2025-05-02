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

use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use snafu::Snafu;
use tokio::sync::RwLock;
use tracing_futures::Instrument;

use super::{DatasetInput, DatasetOutput};

#[cfg(feature = "models")]
pub mod embed;
#[cfg(feature = "models")]
pub use embed::EmbedScorer;
#[cfg(feature = "models")]
pub mod fuzzy_match;
#[cfg(feature = "models")]
pub mod includes;
#[cfg(feature = "models")]
pub mod json_match;
#[cfg(feature = "models")]
pub mod levenshtein;
#[cfg(feature = "models")]
pub mod match_;
#[cfg(feature = "models")]
pub mod modelgraded;
#[cfg(feature = "models")]
pub use modelgraded::ModelGradedScorer;

#[async_trait]
pub trait Scorer: Sync + Send {
    async fn score(
        &self,
        input: &DatasetInput,
        actual: &DatasetOutput,
        ideal: &DatasetOutput,
    ) -> Result<f32>;

    /// Compute the relevant metrics for this [`Scorer`], given a precomputed scores.
    fn metrics(&self, scores: &[f32]) -> Vec<(String, f32)>;
}

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Could not score case [input={input:?}, actual={actual:?}, expected={ideal:?}]. Error: {source}"
    ))]
    ErrorScoringCase {
        input: DatasetInput,
        actual: DatasetOutput,
        ideal: DatasetOutput,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Scorer failed: {} - {}", name, source))]
    ScorerFailed { name: String, source: Box<Error> },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub type EvalScorerRegistry = Arc<RwLock<HashMap<String, Arc<dyn Scorer>>>>;

/// Compute the scores for each [`Scorer`] selected given the results of running a model.
pub(crate) async fn score_results(
    input: &[DatasetInput],
    actual: &[DatasetOutput],
    expected: &[DatasetOutput],
    scorers: &HashMap<String, Arc<dyn Scorer>>,
) -> Result<HashMap<String, Vec<f32>>> {
    let mut aggregate: HashMap<String, Vec<f32>> = HashMap::with_capacity(actual.len());
    for ((input, actual), expected) in input.iter().zip(actual.iter()).zip(expected.iter()) {
        for (name, scorer) in scorers {
            let span = tracing::span!(
                target: "task_history",
                tracing::Level::INFO,
                "run_scorer", // This is immediately overriden by `task_override`.
                input = %serde_json::to_string(&input).unwrap_or_default(),
            );
            span.in_scope(
                || tracing::info!(target: "task_history", task_override = %format!("run_scorer::{name}"), "labels"),
            );
            let s = scorer
                .score(input, actual, expected)
                .instrument(span.clone())
                .await
                .map_err(|e| {
                    tracing::error!(target: "task_history", parent: &span, "{e}");
                    Error::ScorerFailed {
                        name: name.clone(),
                        source: Box::new(e),
                    }
                })?;
            if let Some(scorer_results) = aggregate.get_mut(name) {
                scorer_results.push(s);
            } else {
                aggregate.insert((*name).to_string(), vec![s]);
            }
        }
    }
    Ok(aggregate)
}

#[must_use]
pub fn builtin_scorer() -> Vec<(&'static str, Arc<dyn Scorer>)> {
    #[cfg(feature = "models")]
    {
        vec![
            ("levenshtein", Arc::new(levenshtein::Levenshtein {})),
            ("match", Arc::new(match_::MatchScorer {})),
            ("json_match", Arc::new(json_match::JsonMatch {})),
            ("includes", Arc::new(includes::Includes {})),
            ("fuzzy_match", Arc::new(fuzzy_match::FuzzyMatch {})),
        ]
    }
    #[cfg(not(feature = "models"))]
    {
        vec![]
    }
}

#[allow(clippy::cast_precision_loss)]
fn mean(values: &[f32]) -> f32 {
    let n = values.len();
    if n == 0 {
        return 0.0;
    }

    values.iter().sum::<f32>() / n as f32
}

fn extract_text(output: &DatasetOutput) -> String {
    match output {
        DatasetOutput::AssistantResponse(text) => text.clone(),
        DatasetOutput::Choices(choices) => choices
            .first()
            .map(|choice| choice.message.content.clone().unwrap_or_default())
            .unwrap_or_default(),
    }
}
