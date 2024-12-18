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
