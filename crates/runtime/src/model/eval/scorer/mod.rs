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
use match_::MatchScorer;
use tokio::sync::RwLock;

use super::{DatasetInput, DatasetOutput};

pub mod fuzzy_match;
pub mod includes;
pub mod json_match;
pub mod match_;

#[async_trait]
pub trait Scorer: Sync + Send {
    async fn score(
        &self,
        input: &DatasetInput,
        actual: &DatasetOutput,
        ideal: &DatasetOutput,
    ) -> f32;

    /// Compute the relevant metrics for this [`Scorer`], given a precomputed scores.
    fn metrics(&self, scores: &[f32]) -> Vec<(String, f32)>;
}

pub type EvalScorerRegistry = Arc<RwLock<HashMap<String, Arc<dyn Scorer>>>>;

/// Compute the scores for each [`Scorer`] selected given the results of running a model.
pub(crate) async fn score_results(
    input: &[DatasetInput],
    output: &[DatasetOutput],
    expected: &[DatasetOutput],
    scorers: &HashMap<String, Arc<dyn Scorer>>,
) -> HashMap<String, Vec<f32>> {
    let mut aggregate: HashMap<String, Vec<f32>> = HashMap::with_capacity(output.len());
    for ((input, output), expected) in input.iter().zip(output.iter()).zip(expected.iter()) {
        for (name, scorer) in scorers {
            let s = scorer.score(input, output, expected).await;
            if let Some(scorer_results) = aggregate.get_mut(name) {
                scorer_results.push(s);
            } else {
                aggregate.insert((*name).to_string(), vec![s]);
            };
        }
    }
    aggregate
}

#[must_use]
pub fn builtin_scorer() -> Vec<(&'static str, Arc<dyn Scorer>)> {
    vec![
        ("match", Arc::new(MatchScorer {})),
        ("json_match", Arc::new(json_match::JsonMatch {})),
        ("includes", Arc::new(includes::Includes {})),
        ("fuzzy_match", Arc::new(fuzzy_match::FuzzyMatch {})),
    ]
}

#[allow(clippy::cast_precision_loss)]
fn mean(values: &[f32]) -> f32 {
    let n = values.len();
    if n == 0 {
        return 0.0;
    }

    values.iter().sum::<f32>() / n as f32
}
