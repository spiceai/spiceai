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

use async_trait::async_trait;
use regex::Regex;

use crate::model::eval::scorer::mean;

use super::{DatasetInput, DatasetOutput, Scorer};

#[derive(Debug)]
pub struct FuzzyMatch;

#[async_trait]
impl Scorer for FuzzyMatch {
    async fn score(
        &self,
        _input: &DatasetInput,
        actual: &DatasetOutput,
        ideal: &DatasetOutput,
    ) -> f32 {
        // Extract strings from outputs
        let actual_str: Vec<_> = match actual {
            DatasetOutput::AssistantResponse(a) => vec![a.clone()],
            DatasetOutput::Choices(c) => c
                .iter()
                .map(|c| c.message.content.clone().unwrap_or_default())
                .collect(),
        };

        let ideal_strs = match ideal {
            DatasetOutput::AssistantResponse(a) => vec![a.clone()],
            DatasetOutput::Choices(ref c) => c
                .iter()
                .map(|c| c.message.content.clone().unwrap_or_default())
                .collect(),
        };

        if ideal_strs.len() != actual_str.len() {
            return 0.0;
        }

        // Perform fuzzy matching on all corresponding pairs
        let is_match = actual_str.iter().zip(ideal_strs.iter()).all(|(a, i)| {
            let normalized_a = Self::normalize(a);
            let normalized_i = Self::normalize(i);

            if normalized_a.is_empty() || normalized_i.is_empty() {
                normalized_a == normalized_i
            } else {
                normalized_a.contains(&normalized_i) || normalized_i.contains(&normalized_a)
            }
        });

        // TODO: Should consider using F1 score instead of boolean accuracy.
        if is_match {
            1.0
        } else {
            0.0
        }
    }

    fn metrics(&self, scores: &[f32]) -> Vec<(String, f32)> {
        vec![("mean".to_string(), mean(scores))]
    }
}

impl FuzzyMatch {
    /// Normalizes a string by:
    /// - Converting to lowercase
    /// - Removing punctuation
    /// - Removing articles (a, an, the)
    /// - Collapsing multiple whitespaces into one
    fn normalize(s: &str) -> String {
        let lower = s.to_lowercase();

        // Remove punctuation
        let mut no_punct: String = lower
            .chars()
            .filter(|c| !c.is_ascii_punctuation())
            .collect();

        // Remove articles using regex
        if let Ok(re) = Regex::new(r"\b(a|an|the)\b") {
            no_punct = re.replace_all(&no_punct, " ").to_string();
        }

        // Collapse multiple whitespaces
        let normalized = no_punct.split_whitespace().collect::<Vec<&str>>().join(" ");

        normalized
    }
}
