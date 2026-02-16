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

use anyhow::Result;
use async_openai::types::chat::ChatChoice;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Dataset input representation
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum DatasetInput {
    Messages(Vec<serde_json::Value>),
    UserInput(String),
}

/// Dataset output representation
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum DatasetOutput {
    Choices(Vec<ChatChoice>),
    AssistantResponse(String),
}

/// Trait for scoring evaluation results
pub trait Scorer: Send + Sync {
    fn score(
        &self,
        _input: &DatasetInput,
        actual: &DatasetOutput,
        expected: &DatasetOutput,
    ) -> Result<f32>;

    fn metrics(&self, scores: &[f32]) -> Vec<(String, f32)>;
}

/// Exact match scorer
pub struct MatchScorer;

impl Scorer for MatchScorer {
    fn score(
        &self,
        _input: &DatasetInput,
        actual: &DatasetOutput,
        expected: &DatasetOutput,
    ) -> Result<f32> {
        let actual_text = extract_text(actual);
        let expected_text = extract_text(expected);

        Ok(if actual_text == expected_text {
            1.0
        } else {
            0.0
        })
    }

    fn metrics(&self, scores: &[f32]) -> Vec<(String, f32)> {
        vec![("accuracy".to_string(), mean(scores))]
    }
}

/// Includes scorer - checks if expected text is contained in actual
pub struct IncludesScorer;

impl Scorer for IncludesScorer {
    fn score(
        &self,
        _input: &DatasetInput,
        actual: &DatasetOutput,
        expected: &DatasetOutput,
    ) -> Result<f32> {
        let actual_text = extract_text(actual);
        let expected_text = extract_text(expected);

        Ok(if actual_text.contains(&expected_text) {
            1.0
        } else {
            0.0
        })
    }

    fn metrics(&self, scores: &[f32]) -> Vec<(String, f32)> {
        vec![("accuracy".to_string(), mean(scores))]
    }
}

/// Levenshtein distance scorer (normalized)
pub struct LevenshteinScorer;

impl Scorer for LevenshteinScorer {
    fn score(
        &self,
        _input: &DatasetInput,
        actual: &DatasetOutput,
        expected: &DatasetOutput,
    ) -> Result<f32> {
        let actual_text = extract_text(actual);
        let expected_text = extract_text(expected);

        let distance = levenshtein_distance(&actual_text, &expected_text);
        let max_len = actual_text.len().max(expected_text.len());

        if max_len == 0 {
            return Ok(1.0);
        }

        #[expect(clippy::cast_precision_loss)]
        let normalized = 1.0 - (distance as f32 / max_len as f32);
        Ok(normalized)
    }

    fn metrics(&self, scores: &[f32]) -> Vec<(String, f32)> {
        vec![("similarity".to_string(), mean(scores))]
    }
}

/// Get built-in scorers
pub fn builtin_scorers() -> HashMap<String, Box<dyn Scorer>> {
    let mut scorers: HashMap<String, Box<dyn Scorer>> = HashMap::new();
    scorers.insert("match".to_string(), Box::new(MatchScorer));
    scorers.insert("includes".to_string(), Box::new(IncludesScorer));
    scorers.insert("levenshtein".to_string(), Box::new(LevenshteinScorer));
    scorers
}

/// Extract text from `DatasetOutput`
fn extract_text(output: &DatasetOutput) -> String {
    match output {
        DatasetOutput::AssistantResponse(text) => text.clone(),
        DatasetOutput::Choices(choices) => choices
            .first()
            .and_then(|choice| choice.message.content.clone())
            .unwrap_or_default(),
    }
}

/// Calculate mean of scores
#[expect(clippy::cast_precision_loss)]
fn mean(values: &[f32]) -> f32 {
    if values.is_empty() {
        return 0.0;
    }
    values.iter().sum::<f32>() / values.len() as f32
}

/// Calculate Levenshtein distance
fn levenshtein_distance(s1: &str, s2: &str) -> usize {
    let len1 = s1.chars().count();
    let len2 = s2.chars().count();

    if len1 == 0 {
        return len2;
    }
    if len2 == 0 {
        return len1;
    }

    let mut matrix = vec![vec![0; len2 + 1]; len1 + 1];

    for (i, row) in matrix.iter_mut().enumerate().take(len1 + 1) {
        row[0] = i;
    }
    for (j, cell) in matrix[0].iter_mut().enumerate().take(len2 + 1) {
        *cell = j;
    }

    let s1_chars: Vec<char> = s1.chars().collect();
    let s2_chars: Vec<char> = s2.chars().collect();

    for (i, c1) in s1_chars.iter().enumerate() {
        for (j, c2) in s2_chars.iter().enumerate() {
            let cost = usize::from(c1 != c2);
            matrix[i + 1][j + 1] = (matrix[i][j + 1] + 1)
                .min(matrix[i + 1][j] + 1)
                .min(matrix[i][j] + cost);
        }
    }

    matrix[len1][len2]
}
