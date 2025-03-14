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

use async_trait::async_trait;

use crate::model::eval::scorer::mean;
use strsim::levenshtein;

use super::{extract_text, DatasetInput, DatasetOutput, Scorer};

/// Scorer that computes the Levenshtein distance between two strings.
///
/// The Levenshtein distance is a measure of the similarity between two strings.
///
/// This implementation normalizes the distance by the length of the longest string.
pub struct Levenshtein {}

#[async_trait]
impl Scorer for Levenshtein {
    #[allow(clippy::cast_precision_loss)]
    async fn score(
        &self,
        _input: &DatasetInput,
        actual: &DatasetOutput,
        ideal: &DatasetOutput,
    ) -> f32 {
        let actual_text = extract_text(actual);
        let ideal_text = extract_text(ideal);

        // Calculate the Levenshtein distance between the two texts.
        let distance = levenshtein(&actual_text, &ideal_text);
        let max_len = actual_text.len().max(ideal_text.len());

        // If both strings are empty, treat it as an exact match.
        if max_len == 0 {
            return 1.0;
        }

        // Normalize
        1.0 - (distance as f32 / max_len as f32)
    }

    fn metrics(&self, scores: &[f32]) -> Vec<(String, f32)> {
        vec![("mean".to_string(), mean(scores))]
    }
}
