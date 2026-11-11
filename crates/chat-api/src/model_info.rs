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

//! What a provider's model listing says about a model.

use std::collections::HashMap;

use serde_json::Value;

/// The names providers and OpenAI-compatible servers report a model's context window
/// under, in a model object of their `/models` listing:
///
/// - `context_window`: a common gateway spelling;
/// - `max_input_tokens`: Anthropic's Models API;
/// - `context_length`: `OpenRouter`, and gateways modeled on it;
/// - `max_context_length`: LM Studio;
/// - `max_model_len`: vLLM;
/// - `inputTokenLimit`: the Gemini API.
const CONTEXT_WINDOW_FIELDS: [&str; 6] = [
    "context_window",
    "max_input_tokens",
    "context_length",
    "max_context_length",
    "max_model_len",
    "inputTokenLimit",
];

/// The context window `model`, one model object of a provider's model listing, reports.
///
/// Read from the first of [`CONTEXT_WINDOW_FIELDS`] that holds a positive integer, then
/// from `meta.n_ctx`, the context of one llama.cpp server slot: what one request can hold,
/// which is less than the server's whole context when it runs several slots. `None` when
/// it reports none.
#[must_use]
pub fn context_window_of(model: &Value) -> Option<u64> {
    CONTEXT_WINDOW_FIELDS
        .iter()
        .find_map(|field| positive(model.get(field)))
        .or_else(|| positive(model.pointer("/meta/n_ctx")))
}

/// The names providers and OpenAI-compatible servers report a model's output cap under, in
/// a model object of their `/models` listing:
///
/// - `max_output_tokens`: a common gateway spelling;
/// - `max_completion_tokens`: `OpenRouter`'s `top_provider`, and gateways modeled on it;
/// - `outputTokenLimit`: the Gemini API.
///
/// Anthropic's `max_tokens` is read by its provider alone: elsewhere the name can mean a
/// request default rather than a cap.
const MAX_OUTPUT_FIELDS: [&str; 3] = [
    "max_output_tokens",
    "max_completion_tokens",
    "outputTokenLimit",
];

/// The output cap `model`, one model object of a provider's model listing, reports: the
/// first of [`MAX_OUTPUT_FIELDS`] that holds a positive integer, then `OpenRouter`'s
/// `top_provider.max_completion_tokens`. `None` when it reports none.
#[must_use]
pub fn max_output_tokens_of(model: &Value) -> Option<u64> {
    MAX_OUTPUT_FIELDS
        .iter()
        .find_map(|field| positive(model.get(field)))
        .or_else(|| positive(model.pointer("/top_provider/max_completion_tokens")))
}

/// The context window and output cap `model`, one model object of a provider's model
/// listing, reports.
#[must_use]
pub fn token_limits_of(model: &Value) -> crate::TokenLimits {
    crate::TokenLimits {
        context_window: context_window_of(model),
        max_output_tokens: max_output_tokens_of(model),
    }
}

/// Anthropic's Models API: `max_input_tokens` is the window and `max_tokens` the most one
/// answer can hold.
#[must_use]
pub fn anthropic_token_limits_of(model: &Value) -> crate::TokenLimits {
    crate::TokenLimits {
        context_window: positive(model.get("max_input_tokens")),
        max_output_tokens: positive(model.get("max_tokens")),
    }
}

/// The token limits of each model a model listing (`{"data": [...]}`) lists, by its `id`.
#[must_use]
pub fn listed_token_limits(listing: &Value) -> HashMap<String, crate::TokenLimits> {
    listing
        .get("data")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|model| {
            let id = model.get("id")?.as_str()?;
            Some((id.to_string(), token_limits_of(model)))
        })
        .collect()
}

/// A token count: a positive integer, or a string of one, as some servers send.
fn positive(value: Option<&Value>) -> Option<u64> {
    let value = value?;
    value
        .as_u64()
        .or_else(|| value.as_str().and_then(|text| text.trim().parse().ok()))
        .filter(|count| *count > 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn each_providers_spelling_is_read() {
        for (model, expected) in [
            (json!({"id": "m", "context_window": 128_000}), 128_000),
            // Anthropic's Models API.
            (
                json!({"id": "claude", "max_input_tokens": 200_000, "max_tokens": 64_000}),
                200_000,
            ),
            // OpenRouter.
            (json!({"id": "m", "context_length": 32_768}), 32_768),
            // LM Studio.
            (json!({"id": "m", "max_context_length": 8_192}), 8_192),
            // vLLM.
            (json!({"id": "m", "max_model_len": 131_072}), 131_072),
            // The Gemini API.
            (
                json!({"name": "models/m", "inputTokenLimit": 1_048_576}),
                1_048_576,
            ),
            // The llama.cpp server: the context it runs with, not the model's trained one.
            (
                json!({"id": "m", "meta": {"n_ctx": 65_536, "n_ctx_train": 40_960}}),
                65_536,
            ),
            (json!({"id": "m", "context_length": "4096"}), 4_096),
        ] {
            assert_eq!(context_window_of(&model), Some(expected), "{model}");
        }
    }

    #[test]
    fn a_model_that_reports_no_window_has_none() {
        // OpenAI's own listing carries no window.
        assert_eq!(
            context_window_of(&json!({"id": "gpt-5", "object": "model", "owned_by": "openai"})),
            None
        );
        // Zero, negative and null are not windows.
        assert_eq!(context_window_of(&json!({"context_window": 0})), None);
        assert_eq!(context_window_of(&json!({"context_window": -1})), None);
        assert_eq!(context_window_of(&json!({"max_input_tokens": null})), None);
        // A missing window under the first name falls through to the next.
        assert_eq!(
            context_window_of(&json!({"context_window": null, "max_model_len": 2_048})),
            Some(2_048)
        );
    }

    #[test]
    fn an_output_cap_is_read_only_where_a_provider_reports_one() {
        for (model, expected) in [
            (json!({"id": "m", "max_output_tokens": 8_192}), Some(8_192)),
            // OpenRouter.
            (
                json!({"id": "m", "context_length": 200_000, "top_provider": {"context_length": 200_000, "max_completion_tokens": 64_000}}),
                Some(64_000),
            ),
            // The Gemini API.
            (
                json!({"name": "models/m", "inputTokenLimit": 1_048_576, "outputTokenLimit": 65_536}),
                Some(65_536),
            ),
            // A request default some servers list under `max_tokens` is not a cap.
            (json!({"id": "m", "max_tokens": 4_096}), None),
            // vLLM and the llama.cpp server cap nothing beyond the window.
            (json!({"id": "m", "max_model_len": 131_072}), None),
        ] {
            assert_eq!(max_output_tokens_of(&model), expected, "{model}");
        }
        assert_eq!(
            anthropic_token_limits_of(
                &json!({"id": "claude", "max_input_tokens": 200_000, "max_tokens": 64_000})
            ),
            crate::TokenLimits {
                context_window: Some(200_000),
                max_output_tokens: Some(64_000),
            }
        );
    }

    #[test]
    fn each_listed_model_is_found_by_id() {
        let listing = json!({"object": "list", "data": [
            {"id": "a", "context_length": 1},
            {"id": "b", "context_length": 2, "max_output_tokens": 1},
            {"context_length": 3},
        ]});
        assert_eq!(
            listed_token_limits(&listing),
            HashMap::from([
                (
                    "a".to_string(),
                    crate::TokenLimits {
                        context_window: Some(1),
                        max_output_tokens: None,
                    }
                ),
                (
                    "b".to_string(),
                    crate::TokenLimits {
                        context_window: Some(2),
                        max_output_tokens: Some(1),
                    }
                ),
            ])
        );
        assert!(listed_token_limits(&json!({"models": []})).is_empty());
    }
}
