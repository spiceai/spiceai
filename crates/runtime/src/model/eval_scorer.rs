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

use std::sync::Arc;

use super::eval::{DatasetInput, DatasetOutput};
use async_trait::async_trait;

#[async_trait]
pub trait Scorer: Sync + Send {
    async fn score(
        &self,
        input: &DatasetInput,
        actual: &DatasetOutput,
        ideal: &DatasetOutput,
    ) -> f32;
}

/// [`MatchScorer`] checks for equality between the actual and ideal values.
///
/// The score is an exact match, but it only compare the less detailed of the two [`DatasetOutput`]. ([`DatasetOutput::AssistantResponse`] < [`DatasetOutput::Messages`]).
pub struct MatchScorer {}

#[async_trait]
impl Scorer for MatchScorer {
    async fn score(
        &self,
        _input: &DatasetInput,
        actual: &DatasetOutput,
        ideal: &DatasetOutput,
    ) -> f32 {
        let is_equal = match (actual, ideal) {
            (DatasetOutput::AssistantResponse(a), DatasetOutput::AssistantResponse(b)) => *a == *b,
            (DatasetOutput::AssistantResponse(a), DatasetOutput::Messages(m))
            | (DatasetOutput::Messages(m), DatasetOutput::AssistantResponse(a)) => {
                let b = m
                    .first()
                    .map(|m| m.message.content.clone().unwrap_or_default())
                    .unwrap_or_default();
                *a == b
            }
            (DatasetOutput::Messages(a), DatasetOutput::Messages(b)) => a == b,
        };
        if is_equal {
            1.0
        } else {
            0.0
        }
    }
}

#[must_use]
pub fn builtin_scorer() -> Vec<(&'static str, Arc<dyn Scorer>)> {
    vec![("Match", Arc::new(MatchScorer {}))]
}
