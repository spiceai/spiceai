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

        // TODO: Also use F1 Score, see https://github.com/spiceai/spiceai/issues/3932
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

#[allow(deprecated)]
#[cfg(test)]
mod tests {
    use super::FuzzyMatch;
    use super::Scorer;
    use super::{DatasetInput, DatasetOutput};
    use async_openai::types::{ChatChoice, ChatCompletionResponseMessage, FinishReason, Role};
    use paste::paste;

    // Macro to define test cases for FuzzyMatch scorer with AssistantResponse variant.
    macro_rules! test_fuzzy_match_case {
        ($test_case_name:ident, $actual:expr, $ideal:expr, $score:expr) => {
            paste! {
                #[tokio::test]
                async fn [<test_ $test_case_name>]() {
                    let fuzzy_match = FuzzyMatch;
                    let actual_output = DatasetOutput::AssistantResponse($actual.to_string());
                    let ideal_output = DatasetOutput::AssistantResponse($ideal.to_string());
                    let actual_score = fuzzy_match.score(&DatasetInput::UserInput(String::new()), &actual_output, &ideal_output).await;
                    assert!(
                        ($score - actual_score).abs() < f32::EPSILON,
                        "Test case `{}` failed: expected {}, got {}",
                        stringify!($test_case_name),
                        $score,
                        actual_score
                    );
                }
            }
        };
    }

    // Macro to define test cases for FuzzyMatch scorer with Choices variant.
    macro_rules! test_fuzzy_match_choices_case {
        ($test_case_name:ident, $actual_vec:expr, $ideal_vec:expr, $score:expr) => {
            paste! {
                #[tokio::test]
                async fn [<test_ $test_case_name>]() {
                    let fuzzy_match = FuzzyMatch;

                    // Construct actual Choices
                    let actual_choices: Vec<ChatChoice> = $actual_vec.iter().map(|content| {
                        ChatChoice {
                            index: 0, // Index can be arbitrary for tests
                            message: ChatCompletionResponseMessage {
                                content: Some(content.to_string()),
                                role: Role::Assistant,
                                function_call: None,
                                tool_calls: None,
                                refusal: None,
                                audio: None,
                            },
                            finish_reason: Some(FinishReason::Stop),
                            logprobs: None,
                        }
                    }).collect();

                    // Construct ideal Choices
                    let ideal_choices: Vec<ChatChoice> = $ideal_vec.iter().map(|content| {
                        ChatChoice {
                            index: 0, // Index can be arbitrary for tests
                            message: ChatCompletionResponseMessage {
                                content: Some(content.to_string()),
                                role: Role::Assistant,
                                function_call: None,
                                tool_calls: None,
                                refusal: None,
                                audio: None,
                            },
                            finish_reason: Some(FinishReason::Stop),
                            logprobs: None,
                        }
                    }).collect();

                    let actual_output = DatasetOutput::Choices(actual_choices);
                    let ideal_output = DatasetOutput::Choices(ideal_choices);
                    let actual_score = fuzzy_match.score(&DatasetInput::UserInput(String::new()), &actual_output, &ideal_output).await;
                    assert!(
                        ($score - actual_score).abs() < f32::EPSILON,
                        "Test case `{}` failed: expected {}, got {}",
                        stringify!($test_case_name),
                        $score,
                        actual_score
                    );
                }
            }
        };
    }

    test_fuzzy_match_case!(
        exact_match,
        "The quick brown fox jumps over the lazy dog.",
        "The quick brown fox jumps over the lazy dog.",
        1.0
    );

    test_fuzzy_match_case!(
        case_insensitive_match,
        "The Quick Brown Fox.",
        "the quick brown fox",
        1.0
    );

    test_fuzzy_match_case!(
        punctuation_ignored_match,
        "Hello, world!",
        "Hello world",
        1.0
    );

    test_fuzzy_match_case!(
        articles_ignored_match,
        "An apple a day keeps the doctor away.",
        "apple day keeps doctor away",
        1.0
    );

    test_fuzzy_match_case!(
        partial_inclusion_actual_contains_ideal,
        "The quick brown fox jumps over the lazy dog near the river.",
        "quick brown fox jumps over lazy dog",
        1.0
    );

    test_fuzzy_match_case!(
        partial_inclusion_ideal_contains_actual,
        "quick brown fox",
        "The quick brown fox jumps over the lazy dog.",
        1.0
    );

    test_fuzzy_match_case!(
        no_match,
        "A completely different sentence.",
        "The quick brown fox jumps over the lazy dog.",
        0.0
    );

    // 8. Empty Strings Match
    test_fuzzy_match_case!(empty_strings_match, "", "", 1.0);

    test_fuzzy_match_case!(one_empty_one_non_empty, "", "non-empty string", 0.0);

    test_fuzzy_match_choices_case!(
        mismatched_number_of_outputs,
        ["The quick brown fox jumps over the lazy dog."],
        [
            "quick brown fox jumps over lazy dog",
            "Another ideal answer."
        ],
        0.0
    );

    test_fuzzy_match_choices_case!(
        multiple_choices_match,
        ["The quick brown fox.", "Jumps over the lazy dog."],
        ["quick brown fox", "lazy dog"],
        1.0
    );

    test_fuzzy_match_choices_case!(
        multiple_choices_partial_match,
        ["The quick brown fox.", "Jumps over a dog."],
        ["quick brown fox", "lazy dog"],
        0.0
    );

    test_fuzzy_match_case!(
        fuzzy_match_numeric_tolerance,
        "The population is approximately 1,002,500.",
        "Population is 1,000,000",
        0.0
    );

    test_fuzzy_match_case!(
        fuzzy_match_with_synonyms,
        "The swift auburn fox leaps over the idle canine.",
        "The quick brown fox jumps over the lazy dog.",
        0.0
    );

    test_fuzzy_match_case!(
        fuzzy_match_extra_words_in_ideal,
        "The quick brown fox jumps over the lazy dog.",
        "The quick brown fox jumps over the very lazy dog.",
        0.0
    );

    test_fuzzy_match_case!(
        fuzzy_match_only_partial_overlap,
        "The quick brown fox.",
        "quick fox jumps over the lazy dog",
        0.0
    );
}
