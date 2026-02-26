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

use async_trait::async_trait;

use crate::model::eval::scorer::mean;

use super::{DatasetInput, DatasetOutput, Scorer, extract_text};

/// Scorer that computes the Levenshtein distance between two strings.
///
/// The Levenshtein distance is a measure of the similarity between two strings.
///
/// This implementation normalizes the distance by the length of the longest string.
pub struct Levenshtein {}

#[async_trait]
impl Scorer for Levenshtein {
    #[expect(clippy::cast_possible_truncation)]
    async fn score(
        &self,
        _input: &DatasetInput,
        actual: &DatasetOutput,
        ideal: &DatasetOutput,
    ) -> super::Result<f32> {
        let actual_text = extract_text(actual);
        let ideal_text = extract_text(ideal);

        // Use the common util::levenshtein implementation
        let similarity = util::levenshtein::similarity(&actual_text, &ideal_text);

        Ok(similarity as f32)
    }

    fn metrics(&self, scores: &[f32]) -> Vec<(String, f32)> {
        vec![("mean".to_string(), mean(scores))]
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use std::f32;

    // Helper function to compare float values within a tolerance.
    fn float_eq(a: f32, b: f32) -> bool {
        (a - b).abs() < f32::EPSILON
    }

    #[tokio::test]
    async fn test_score_identical_assistant_response() {
        let score = Levenshtein {}
            .score(
                &DatasetInput::UserInput("Tell me something".to_string()),
                &DatasetOutput::from_raw("Hello"),
                &DatasetOutput::from_raw("Hello"),
            )
            .await
            .expect("Levenshtein returned error");
        assert!(float_eq(score, 1.0), "Expected score 1.0, got {score}");
    }

    #[tokio::test]
    async fn test_score_different_assistant_response() {
        let score = Levenshtein {}
            .score(
                &DatasetInput::UserInput("Compare these".to_string()),
                &DatasetOutput::from_raw("kitten"),
                &DatasetOutput::from_raw("sitting"),
            )
            .await
            .expect("Levenshtein returned error");
        let expected = 1.0 - (3.0 / 7.0);
        assert!(
            float_eq(score, expected),
            "Expected score {expected}, got {score}",
        );
    }

    #[tokio::test]
    async fn test_score_empty_strings() {
        let score = Levenshtein {}
            .score(
                &DatasetInput::UserInput("Empty test".to_string()),
                &DatasetOutput::from_raw(""),
                &DatasetOutput::from_raw(""),
            )
            .await
            .expect("Levenshtein returned error");
        assert!(float_eq(score, 1.0), "Expected score 1.0, got {score}");
    }

    #[tokio::test]
    async fn test_score_one_empty() {
        let score = Levenshtein {}
            .score(
                &DatasetInput::UserInput("One empty test".to_string()),
                &DatasetOutput::from_raw(""),
                &DatasetOutput::from_raw("Hello"),
            )
            .await
            .expect("Levenshtein returned error");
        assert!(float_eq(score, 0.0), "Expected score 0.0, got {score}");
    }

    #[tokio::test]
    async fn test_score_choices_identical() {
        let json_data = json!([
            {
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": "Hello World"
                }
            }
        ]);

        let actual = DatasetOutput::try_from_value(json_data.clone())
            .expect("Failed to parse actual DatasetOutput")
            .expect("Actual DatasetOutput is None");
        let ideal = DatasetOutput::try_from_value(json_data)
            .expect("Failed to parse ideal DatasetOutput")
            .expect("Ideal DatasetOutput is None");

        let score = Levenshtein {}
            .score(&DatasetInput::Messages(vec![]), &actual, &ideal)
            .await
            .expect("Levenshtein returned error");
        assert!(float_eq(score, 1.0), "Expected score 1.0, got {score}");
    }

    #[tokio::test]
    async fn test_score_choices_different() {
        let actual = DatasetOutput::try_from_value(json!([
            {
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": "Hello World"
                }
            }
        ]))
        .expect("Failed to parse actual DatasetOutput")
        .expect("Actual DatasetOutput is None");

        let ideal = DatasetOutput::try_from_value(json!([
            {
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": "Hi"
                }
            }
        ]))
        .expect("Failed to parse ideal DatasetOutput")
        .expect("Ideal DatasetOutput is None");

        let score = Levenshtein {}
            .score(&DatasetInput::Messages(vec![]), &actual, &ideal)
            .await
            .expect("Levenshtein returned error");

        let expected = 1.0 - (10.0 / 11.0);
        assert!(
            (score - expected).abs() < f32::EPSILON,
            "Expected score approximately {expected}, got {score}"
        );
    }

    #[tokio::test]
    async fn test_score_mixed_input_types_match() {
        let ideal = DatasetOutput::try_from_value(json!([
            {
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": "Greetings"
                }
            }
        ]))
        .expect("Failed to parse ideal DatasetOutput")
        .expect("Ideal DatasetOutput is None");

        let score = Levenshtein {}
            .score(
                &DatasetInput::UserInput("Mixed input".to_string()),
                &DatasetOutput::from_raw("Greetings"),
                &ideal,
            )
            .await
            .expect("Levenshtein returned error");
        assert!(float_eq(score, 1.0), "Expected score 1.0, got {score}");
    }

    #[tokio::test]
    async fn test_score_mixed_input_types_mismatch() {
        let ideal = DatasetOutput::try_from_value(json!([
            {
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": "Hello World"
                }
            }
        ]))
        .expect("Failed to parse ideal DatasetOutput")
        .expect("Ideal DatasetOutput is None");

        let score = Levenshtein {}
            .score(
                &DatasetInput::UserInput("Mixed input mismatch".to_string()),
                &DatasetOutput::from_raw("Hi"),
                &ideal,
            )
            .await
            .expect("Levenshtein returned error");

        let expected = 1.0 - (10.0 / 11.0);
        assert!(
            (score - expected).abs() < f32::EPSILON,
            "Expected score approximately {expected}, got {score}",
        );
    }
}
