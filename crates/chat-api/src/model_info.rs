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

/// The model `id` in a model listing (`{"data": [...]}`), when it lists it.
#[must_use]
pub fn listed_model<'a>(listing: &'a Value, id: &str) -> Option<&'a Value> {
    listing
        .get("data")?
        .as_array()?
        .iter()
        .find(|model| model.get("id").and_then(Value::as_str) == Some(id))
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
    fn the_listed_model_is_found_by_id() {
        let listing = json!({"object": "list", "data": [
            {"id": "a", "context_length": 1},
            {"id": "b", "context_length": 2},
        ]});
        assert_eq!(
            listed_model(&listing, "b").and_then(context_window_of),
            Some(2)
        );
        assert!(listed_model(&listing, "c").is_none());
        assert!(listed_model(&json!({"models": []}), "a").is_none());
    }
}
