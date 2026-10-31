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

//! Chat completions as they go out on the wire.
//!
//! The chat types serialize every unset token-count detail as `null` —
//! `"prompt_tokens_details": {"audio_tokens": null, "cached_tokens": 13}`. The
//! `OpenAI` API never sends `null` there: it reports a count or leaves the field
//! out. Clients that deserialize usage into integer fields, such as Grok Build,
//! reject the whole response over it, so a count nobody reported is omitted
//! instead. It is never replaced with `0`, which would claim a measurement.

use async_openai::types::chat::CreateChatCompletionStreamResponse;
use serde::{Serialize, Serializer};
use serde_json::Value;

/// The usage objects whose members are optional counts.
const USAGE_DETAILS: [&str; 2] = ["prompt_tokens_details", "completion_tokens_details"];

/// `response` — a chat completion or one of its stream chunks — as the JSON
/// sent to a client.
///
/// # Errors
///
/// Returns an error when `response` cannot be serialized.
pub fn chat_completion_json<T: Serialize>(response: &T) -> serde_json::Result<Value> {
    let mut value = serde_json::to_value(response)?;
    omit_unreported_usage(&mut value);
    Ok(value)
}

/// A stream chunk as the JSON sent to a client: [`chat_completion_json`] when it
/// carries usage, and otherwise the chunk itself, which has no usage to clean.
/// A stream sends one of these per token, and only its last chunk carries usage,
/// so the rest skip the intermediate [`Value`].
///
/// # Errors
///
/// Returns an error when a chunk that carries usage cannot be serialized.
pub fn stream_chunk_json(
    chunk: &CreateChatCompletionStreamResponse,
) -> serde_json::Result<Wire<'_, CreateChatCompletionStreamResponse>> {
    if chunk.usage.is_none() {
        return Ok(Wire::AsIs(chunk));
    }
    chat_completion_json(chunk).map(Wire::Cleaned)
}

/// A response ready to serialize for a client: unchanged, or cleaned into JSON.
#[derive(Debug)]
pub enum Wire<'a, T> {
    /// Serialized as it is: it has nothing to clean.
    AsIs(&'a T),
    /// Cleaned by [`chat_completion_json`].
    Cleaned(Value),
}

impl<T: Serialize> Serialize for Wire<'_, T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::AsIs(response) => response.serialize(serializer),
            Self::Cleaned(value) => value.serialize(serializer),
        }
    }
}

/// Remove the `null` counts from `value["usage"]`'s detail objects, and a
/// detail object left with no counts at all.
pub fn omit_unreported_usage(value: &mut Value) {
    let Some(usage) = value.get_mut("usage").and_then(Value::as_object_mut) else {
        return;
    };
    for key in USAGE_DETAILS {
        let Some(details) = usage.get_mut(key) else {
            continue;
        };
        if let Some(counts) = details.as_object_mut() {
            counts.retain(|_, count| !count.is_null());
            if !counts.is_empty() {
                continue;
            }
        } else if !details.is_null() {
            continue;
        }
        usage.remove(key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_openai::types::chat::{
        CompletionTokensDetails, CompletionUsage, PromptTokensDetails,
    };
    use serde_json::json;

    fn usage(
        prompt: Option<PromptTokensDetails>,
        completion: Option<CompletionTokensDetails>,
    ) -> CompletionUsage {
        CompletionUsage {
            prompt_tokens: 14,
            completion_tokens: 16,
            total_tokens: 30,
            prompt_tokens_details: prompt,
            completion_tokens_details: completion,
        }
    }

    // regression test: Grok Build rejected the final chunk of every streamed
    // completion over `"audio_tokens": null`.
    #[test]
    fn an_unreported_count_is_omitted_and_a_reported_one_kept() {
        let chunk = json!({
            "object": "chat.completion.chunk",
            "choices": [],
            "usage": usage(
                Some(PromptTokensDetails { audio_tokens: None, cached_tokens: Some(13) }),
                None,
            ),
        });
        assert_eq!(
            chat_completion_json(&chunk).expect("serialize"),
            json!({
                "object": "chat.completion.chunk",
                "choices": [],
                "usage": {
                    "prompt_tokens": 14,
                    "completion_tokens": 16,
                    "total_tokens": 30,
                    "prompt_tokens_details": { "cached_tokens": 13 },
                },
            })
        );
    }

    #[test]
    fn a_zero_count_is_a_measurement_and_stays() {
        let chunk = json!({
            "usage": usage(
                Some(PromptTokensDetails { audio_tokens: Some(0), cached_tokens: Some(0) }),
                Some(CompletionTokensDetails {
                    accepted_prediction_tokens: None,
                    audio_tokens: None,
                    reasoning_tokens: Some(0),
                    rejected_prediction_tokens: None,
                }),
            ),
        });
        assert_eq!(
            chat_completion_json(&chunk).expect("serialize")["usage"],
            json!({
                "prompt_tokens": 14,
                "completion_tokens": 16,
                "total_tokens": 30,
                "prompt_tokens_details": { "audio_tokens": 0, "cached_tokens": 0 },
                "completion_tokens_details": { "reasoning_tokens": 0 },
            })
        );
    }

    #[test]
    fn a_detail_object_with_no_counts_is_omitted() {
        let chunk = json!({
            "usage": usage(
                Some(PromptTokensDetails { audio_tokens: None, cached_tokens: None }),
                Some(CompletionTokensDetails {
                    accepted_prediction_tokens: None,
                    audio_tokens: None,
                    reasoning_tokens: None,
                    rejected_prediction_tokens: None,
                }),
            ),
        });
        assert_eq!(
            chat_completion_json(&chunk).expect("serialize")["usage"],
            json!({ "prompt_tokens": 14, "completion_tokens": 16, "total_tokens": 30 })
        );
    }

    fn stream_chunk(usage: &Value) -> CreateChatCompletionStreamResponse {
        serde_json::from_value(json!({
            "id": "chatcmpl-abc", "object": "chat.completion.chunk", "created": 0, "model": "qwen",
            "choices": [{"index": 0, "delta": {"content": " the"}, "finish_reason": null}],
            "usage": usage,
        }))
        .expect("a stream chunk")
    }

    /// The shortcut for a chunk without usage writes exactly what cleaning it would.
    #[test]
    fn a_stream_chunk_is_sent_as_cleaning_it_would_send_it() {
        for usage in [
            Value::Null,
            json!({"prompt_tokens": 3, "completion_tokens": 1, "total_tokens": 4}),
            json!({
                "prompt_tokens": 3, "completion_tokens": 1, "total_tokens": 4,
                "prompt_tokens_details": {"audio_tokens": null, "cached_tokens": 2},
                "completion_tokens_details": {"reasoning_tokens": null},
            }),
        ] {
            let chunk = stream_chunk(&usage);
            let sent = serde_json::to_string(&stream_chunk_json(&chunk).expect("serialize"))
                .expect("write");
            let cleaned = serde_json::to_string(&chat_completion_json(&chunk).expect("clean"))
                .expect("write");
            assert_eq!(
                serde_json::from_str::<Value>(&sent).expect("parse"),
                serde_json::from_str::<Value>(&cleaned).expect("parse"),
                "{usage}"
            );
        }
        // And a chunk with usage is cleaned.
        let chunk = stream_chunk(&json!({
            "prompt_tokens": 3, "completion_tokens": 1, "total_tokens": 4,
            "prompt_tokens_details": {"audio_tokens": null, "cached_tokens": 2},
        }));
        let sent =
            serde_json::to_value(stream_chunk_json(&chunk).expect("serialize")).expect("write");
        assert_eq!(
            sent["usage"]["prompt_tokens_details"],
            json!({"cached_tokens": 2})
        );
    }

    #[test]
    fn a_chunk_without_usage_is_unchanged() {
        let chunk = json!({ "usage": null, "choices": [{ "delta": { "content": null } }] });
        assert_eq!(chat_completion_json(&chunk).expect("serialize"), chunk);
    }
}
