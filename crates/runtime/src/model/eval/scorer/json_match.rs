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
use serde_json::{Number, Value};

pub struct JsonMatch {}

impl JsonMatch {
    fn normalize_numbers(value: Value) -> Value {
        match value {
            Value::Number(n) => {
                // Convert to f64 and back to remove differences like 1 vs 1.0
                if let Some(Some(f)) = n.as_f64().map(Number::from_f64) {
                    Value::Number(f)
                } else {
                    Value::Number(n)
                }
            }
            Value::Array(vec) => {
                Value::Array(vec.into_iter().map(Self::normalize_numbers).collect())
            }
            Value::Object(map) => Value::Object(
                map.into_iter()
                    .map(|(k, v)| (k, Self::normalize_numbers(v)))
                    .collect(),
            ),
            _ => value,
        }
    }
}

#[async_trait]
impl Scorer for JsonMatch {
    async fn score(
        &self,
        _input: &DatasetInput,
        actual: &DatasetOutput,
        ideal: &DatasetOutput,
    ) -> f32 {
        // Extract JSON string from outputs
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
        };

        let is_match = actual_str.iter().zip(ideal_strs).all(|(a, i)| {
            let Ok(actual_json) = serde_json::from_str::<Value>(a) else {
                return false;
            };

            let Ok(ideal_json) = serde_json::from_str::<Value>(&i) else {
                return false;
            };

            Self::normalize_numbers(actual_json) == Self::normalize_numbers(ideal_json)
        });

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

#[cfg(test)]
mod tests {
    use super::JsonMatch;
    use super::Scorer;
    use super::{DatasetInput, DatasetOutput};
    use paste::paste;
    use serde_json::json;

    macro_rules! test_eval_case {
        ($test_case_name:expr, $actual:expr, $ideal:expr, $score:expr) => {
            paste! {
                #[tokio::test]
                async fn [<test_ $test_case_name>]() {
                    let ideal = $ideal;
                    let actual = $actual;
                    let score = $score;
                    let actual_output = DatasetOutput::AssistantResponse(actual);
                    let ideal_output = DatasetOutput::AssistantResponse(ideal);
                    let actual_score = JsonMatch{}.score(&DatasetInput::UserInput(String::new()), &actual_output, &ideal_output).await;
                    assert!((score-actual_score).abs() < f32::EPSILON);
                }
            }
        };
    }

    test_eval_case!(
        basic_match,
        json!({ "key": "value" }).to_string(),
        json!({ "key": "value" }).to_string(),
        1.0
    );
    test_eval_case!(
        whitespace_insignificant,
        "{\n   \"key\":\"value\"\n   }\n".to_string(),
        json!({ "key": "value" }).to_string(),
        1.0
    );
    test_eval_case!(
        key_order_not_significant,
        json!({ "key2": "foo", "key1": "bar" }).to_string(),
        json!({ "key1": "bar", "key2": "foo" }).to_string(),
        1.0
    );
    test_eval_case!(
        values_different,
        json!({ "key": "value" }).to_string(),
        json!({ "key": "notvalue" }).to_string(),
        0.0
    );
    test_eval_case!(
        numeric_values_match,
        json!({ "key": 100 }).to_string(),
        json!({ "key": 100 }).to_string(),
        1.0
    );
    test_eval_case!(
        numerical_values_differ,
        json!({ "key": 100 }).to_string(),
        json!({ "key": 100.1 }).to_string(),
        0.0
    );
    test_eval_case!(
        completion_not_found_in_array,
        json!({ "key": 100 }).to_string(),
        json!([{ "key": 100.1 }, { "key": 99.9 }]).to_string(),
        0.0
    );
    test_eval_case!(
        different_keys,
        json!({ "key": "value" }).to_string(),
        json!({ "anotherkey": "value" }).to_string(),
        0.0
    );
    test_eval_case!(
        missing_keys,
        json!({ "key": "value" }).to_string(),
        json!({ "key": "value", "anotherkey": "value" }).to_string(),
        0.0
    );
    test_eval_case!(
        extra_keys,
        json!({ "key": "value", "anotherkey": "value" }).to_string(),
        json!({ "key": "value" }).to_string(),
        0.0
    );
    test_eval_case!(
        list_element_equality,
        json!({ "key": [1.0, 2.0, 3.0] }).to_string(),
        json!({ "key": [1, 2, 3] }).to_string(),
        1.0
    );
    test_eval_case!(
        list_length_differ,
        json!({ "key": [1, 2, 3] }).to_string(),
        json!({ "key": [1, 2, 3, 3] }).to_string(),
        0.0
    );
    test_eval_case!(
        list_index_inequality,
        json!({ "key": [1, 2, 3] }).to_string(),
        json!({ "key": [1, 3, 2] }).to_string(),
        0.0
    );
    test_eval_case!(
        empty_list_vs_nonempty,
        json!({ "key": [] }).to_string(),
        json!({ "key": [1] }).to_string(),
        0.0
    );
    test_eval_case!(
        invalid_json,
        json!("{ \"key\": \"value }").to_string(),
        json!({ "key": "value" }).to_string(),
        0.0
    );
}
