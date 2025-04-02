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

use async_openai::{
    error::OpenAIError,
    types::{
        ChatCompletionRequestUserMessageArgs, CreateChatCompletionRequest,
        CreateChatCompletionRequestArgs,
    },
};
use async_trait::async_trait;
use llms::chat::Chat;
use serde_json::{json, Number, Value};

use crate::model::eval::scorer::mean;

use super::{DatasetInput, DatasetOutput, Error, Scorer};

/// [`ModelGradedScorer`] scores based on the score provided in response by the language model. The model response is expected to be JSON, with at least a `score` field of float or integer type. Additional fields are allowed, but ignored.
///
/// The [`DatasetInput`] and both [`DatasetOutput`]s are provided to the [`Chat`] model via request metadata (i.e. [`CreateChatCompletionRequest`]'s metadata field]).
pub struct ModelGradedScorer {
    model: Arc<dyn Chat>,
}

impl ModelGradedScorer {
    pub fn new(model: Arc<dyn Chat>) -> Self {
        Self { model }
    }

    fn construct_request(
        input: &DatasetInput,
        actual: &DatasetOutput,
        ideal: &DatasetOutput,
    ) -> Result<CreateChatCompletionRequest, OpenAIError> {
        CreateChatCompletionRequestArgs::default()
            .metadata(json!({
                "input": input,
                "actual": actual,
                "ideal": ideal,
            }))
            .messages(vec![ChatCompletionRequestUserMessageArgs::default()
                .content(String::new())
                .build()?
                .into()])
            .store(true)
            .build()
    }

    #[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation)]
    fn convert_number_to_f32(value: &Number) -> Option<f32> {
        match value {
            v if v.is_u64() => Some(v.as_u64().unwrap_or_default() as f32),
            v if v.is_i64() => Some(v.as_i64().unwrap_or_default() as f32),
            v if v.is_f64() => Some(v.as_f64().unwrap_or_default() as f32),
            _ => None,
        }
    }
}

#[async_trait]
impl Scorer for ModelGradedScorer {
    async fn score(
        &self,
        input: &DatasetInput,
        actual: &DatasetOutput,
        ideal: &DatasetOutput,
    ) -> super::Result<f32> {
        let req =
            Self::construct_request(input, actual, ideal).map_err(|e| Error::ErrorScoringCase {
                input: input.clone(),
                actual: actual.clone(),
                ideal: ideal.clone(),
                source: Box::from(format!(
                    "Failed to build request for model graded scorer: {e}"
                )),
            })?;

        let response = self
            .model
            .chat_request(req)
            .await
            .map_err(|e| Error::ErrorScoringCase {
                input: input.clone(),
                actual: actual.clone(),
                ideal: ideal.clone(),
                source: Box::from(format!("Underlying language model failed: {e}")),
            })?;
        let Some(content) = response
            .choices
            .first()
            .and_then(|c| c.message.content.clone())
        else {
            return Err(Error::ErrorScoringCase {
                input: input.clone(),
                actual: actual.clone(),
                ideal: ideal.clone(),
                source: Box::from("Underlying language model produced no content in response"),
            });
        };
        let Ok(Some(Value::Number(score))) =
            serde_json::from_str::<Value>(content.as_str()).map(|v| v.get("score").cloned())
        else {
            return Err(Error::ErrorScoringCase {
                input: input.clone(),
                actual: actual.clone(),
                ideal: ideal.clone(),
                source: Box::from("Underlying language model produced no score in response"),
            });
        };
        Self::convert_number_to_f32(&score).map_or_else(
            || {
                Err(Error::ErrorScoringCase {
                    input: input.clone(),
                    actual: actual.clone(),
                    ideal: ideal.clone(),
                    source: Box::from(
                        format!("Underlying language model produced a non-numeric value for its 'score'={score}"),
                    ),
                })
            },
            Ok,
        )
    }

    fn metrics(&self, scores: &[f32]) -> Vec<(String, f32)> {
        vec![("mean".to_string(), mean(scores))]
    }
}
