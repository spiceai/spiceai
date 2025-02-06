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

use super::{DatasetInput, DatasetOutput, Scorer};

#[derive(Debug)]
pub struct Includes;

#[async_trait]
impl Scorer for Includes {
    // Ideal should also be a &[DatasetOutput] so that actual can be any one of the list.
    // Currently, [`Includes`] checks whether the `ideal` is within the actual response. This allows for the model to be correct when it's okay to have additional tokens.
    async fn score(
        &self,
        _input: &DatasetInput,
        actual: &DatasetOutput,
        ideal: &DatasetOutput,
    ) -> f32 {
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

        let is_match = actual_str
            .iter()
            .zip(ideal_strs.iter())
            .all(|(a, i)| a.contains(i));

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

#[allow(deprecated)]
#[cfg(test)]
mod tests {
    use super::Includes;
    use super::Scorer;
    use super::{DatasetInput, DatasetOutput};
    use async_openai::types::{ChatChoice, ChatCompletionResponseMessage, FinishReason, Role};
    use paste::paste;

    /// Macro to define test cases for Includes scorer with `AssistantResponse` variant.
    macro_rules! test_includes_case {
        ($test_case_name:ident, $actual:expr, $ideal:expr, $score:expr) => {
            paste! {
                #[tokio::test]
                async fn [<test_ $test_case_name>]() {
                    let actual_score = Includes{}.score(&DatasetInput::UserInput(String::new()), &DatasetOutput::AssistantResponse($actual.to_string()), &DatasetOutput::AssistantResponse($ideal.to_string())).await;
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

    /// Macro to define test cases for Includes scorer with Choices variant.
    macro_rules! test_includes_choices_case {
        ($test_case_name:ident, $actual_vec:expr, $ideal_vec:expr, $score:expr, $ignore_case:expr) => {
            paste! {
                #[tokio::test]
                async fn [<test_ $test_case_name>]() {

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

                    let actual_score = Includes{}.score(&DatasetInput::UserInput(String::new()), &DatasetOutput::Choices(actual_choices), &DatasetOutput::Choices(ideal_choices)).await;
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

    test_includes_case!(
        exact_match,
        "The quick brown fox jumps over the lazy dog.",
        "The quick brown fox jumps over the lazy dog.",
        1.0
    );

    test_includes_case!(
        articles_ignored_match,
        "An apple a day keeps the doctor away.",
        "apple a day keeps the doctor away",
        1.0
    );

    test_includes_case!(
        partial_inclusion_actual_contains_ideal,
        "The quick brown fox jumps over the lazy dog near the river.",
        "quick brown fox jumps over the lazy dog",
        1.0
    );

    test_includes_case!(
        no_match,
        "A completely different sentence.",
        "The quick brown fox jumps over the lazy dog.",
        0.0
    );

    test_includes_case!(empty_strings_match, "", "", 1.0);

    test_includes_case!(
        one_empty_one_non_empty_actual_empty,
        "",
        "non-empty string",
        0.0
    );

    test_includes_choices_case!(
        mismatched_number_of_outputs_more_actual,
        [
            "The quick brown fox jumps over the lazy dog.",
            "Additional response."
        ],
        ["quick brown fox jumps over lazy dog"],
        0.0,
        false
    );

    test_includes_choices_case!(
        multiple_choices_match,
        ["The quick brown fox.", "Jumps over the lazy dog."],
        ["quick brown fox", "lazy dog"],
        1.0,
        false
    );
}
