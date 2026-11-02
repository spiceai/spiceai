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

//! Serde round trips of the wire types.
//!
//! The request bodies are the `curl` examples of the API reference
//! (<https://ai.google.dev/api/generate-content>, `/api/tokens`, `/api/caching`), with their
//! shell variables filled in. Google's parser tolerates trailing commas, which several of them
//! carry; they are removed here. Each body is read, written in canonical form (camelCase, arrays
//! for repeated fields), and the canonical form read back to the same value.
//!
//! The reference shows the responses only as field listings, without values, so the response
//! bodies are those listings with values filled in.

use std::fmt::Debug;

use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

use crate::{
    CountTokensRequest, CountTokensResponse, FinishReason, FunctionCallingMode,
    GenerateContentRequest, GenerateContentResponse, ListModelsResponse,
};

/// `value` as the JSON sent on the wire. Serializing through a string, rather than to a
/// [`Value`], writes an `f32` such as `0.8` as `0.8`, not as its `f64` widening.
fn wire<T: Serialize>(value: &T) -> Value {
    serde_json::from_str(&serde_json::to_string(value).expect("serializable")).expect("JSON")
}

/// Reads `docs`, checks that it is written as `canonical`, and that `canonical` reads back to the
/// same value.
fn round_trip<T: Serialize + DeserializeOwned + PartialEq + Debug>(
    docs: &str,
    canonical: &Value,
) -> T {
    let parsed: T = serde_json::from_str(docs).expect("the documented body parses");
    assert_eq!(&wire(&parsed), canonical);
    let reparsed: T = serde_json::from_value(canonical.clone()).expect("the canonical body parses");
    assert_eq!(reparsed, parsed);
    parsed
}

#[test]
fn text_generation_request() {
    // text_generation.sh
    let docs = r#"{
      "contents": [{
        "parts":[{"text": "Write a story about a magic backpack."}]
        }]
       }"#;
    let request: GenerateContentRequest = round_trip(
        docs,
        &json!({"contents": [{"parts": [{"text": "Write a story about a magic backpack."}]}]}),
    );
    assert_eq!(
        (request.contents.len(), request.contents[0].role.as_deref()),
        (1, None)
    );
}

#[test]
fn snake_case_inline_and_file_data_read_as_camel_case() {
    // text_generation.sh: an image inline, and a file of the Files API.
    let docs = r#"{
      "contents": [{
        "parts":[
          {"text": "Tell me about this instrument"},
          {
            "inline_data": {
              "mime_type":"image/jpeg",
              "data": "/9j/4AAQSkZJRg=="
            }
          },
          {"file_data":{"mime_type": "audio/mpeg", "file_uri": "https://generativelanguage.googleapis.com/v1beta/files/abc123"}}
        ]
      }]
    }"#;
    let request: GenerateContentRequest = round_trip(
        docs,
        &json!({"contents": [{"parts": [
            {"text": "Tell me about this instrument"},
            {"inlineData": {"mimeType": "image/jpeg", "data": "/9j/4AAQSkZJRg=="}},
            {"fileData": {"mimeType": "audio/mpeg", "fileUri": "https://generativelanguage.googleapis.com/v1beta/files/abc123"}},
        ]}]}),
    );
    let parts = &request.contents[0].parts;
    assert_eq!(
        parts[1]
            .inline_data
            .as_ref()
            .map(|blob| blob.mime_type.as_str()),
        Some("image/jpeg")
    );
    assert_eq!(
        parts[2]
            .file_data
            .as_ref()
            .map(|file| file.file_uri.as_str()),
        Some("https://generativelanguage.googleapis.com/v1beta/files/abc123")
    );
}

#[test]
fn chat_request_with_roles() {
    // chat.sh
    let docs = r#"{
      "contents": [
        {"role":"user",
         "parts":[{
           "text": "Hello"}]},
        {"role": "model",
         "parts":[{
           "text": "Great to meet you. What would you like to know?"}]},
        {"role":"user",
         "parts":[{
           "text": "I have two dogs in my house. How many paws are in my house?"}]}
      ]
    }"#;
    let request: GenerateContentRequest = round_trip(
        docs,
        &json!({"contents": [
            {"role": "user", "parts": [{"text": "Hello"}]},
            {"role": "model", "parts": [{"text": "Great to meet you. What would you like to know?"}]},
            {"role": "user", "parts": [{"text": "I have two dogs in my house. How many paws are in my house?"}]},
        ]}),
    );
    assert_eq!(
        request
            .contents
            .iter()
            .map(|content| content.role.as_deref())
            .collect::<Vec<_>>(),
        [Some("user"), Some("model"), Some("user")]
    );
}

#[test]
fn generation_config_and_safety_settings() {
    // configure_model_parameters.sh, with the safety settings of safety_settings.sh.
    let docs = r#"{
        "safetySettings": [
            {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_ONLY_HIGH"},
            {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_MEDIUM_AND_ABOVE"}
        ],
        "contents": [{
            "parts":[
                {"text": "Explain how AI works"}
            ]
        }],
        "generationConfig": {
            "stopSequences": [
                "Title"
            ],
            "temperature": 1.0,
            "maxOutputTokens": 800,
            "topP": 0.8,
            "topK": 10
        }
    }"#;
    let request: GenerateContentRequest = round_trip(
        docs,
        &json!({
            "contents": [{"parts": [{"text": "Explain how AI works"}]}],
            "generationConfig": {"stopSequences": ["Title"], "temperature": 1.0, "maxOutputTokens": 800, "topP": 0.8, "topK": 10},
            "safetySettings": [
                {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_ONLY_HIGH"},
                {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_MEDIUM_AND_ABOVE"},
            ],
        }),
    );
    let config = request.generation_config.expect("a generation config");
    assert_eq!(
        (
            config.max_output_tokens,
            config.top_k,
            config.stop_sequences.as_slice()
        ),
        (Some(800), Some(10), ["Title".to_string()].as_slice())
    );
}

#[test]
fn controlled_generation_schema_is_kept_as_written() {
    // controlled_generation.sh
    let docs = r#"{
    "contents": [{
      "parts":[
        {"text": "List 5 popular cookie recipes"}
        ]
    }],
    "generationConfig": {
        "response_mime_type": "application/json",
        "response_schema": {
          "type": "ARRAY",
          "items": {
            "type": "OBJECT",
            "properties": {
              "recipe_name": {"type":"STRING"}
            }
          }
        }
    }
}"#;
    round_trip::<GenerateContentRequest>(
        docs,
        &json!({
            "contents": [{"parts": [{"text": "List 5 popular cookie recipes"}]}],
            "generationConfig": {
                "responseMimeType": "application/json",
                "responseSchema": {"type": "ARRAY", "items": {"type": "OBJECT", "properties": {"recipe_name": {"type": "STRING"}}}},
            },
        }),
    );
}

/// A single object stands for an array of one, and an enum value is read in any case, as in
/// Google's own example.
#[test]
fn function_calling_request_with_single_objects_and_a_lowercase_mode() {
    // function_calling.sh
    let docs = r#"{
    "system_instruction": {
      "parts": {
        "text": "You are a helpful lighting system bot. You can turn lights on and off, and you can set the color. Do not perform any other tasks."
      }
    },
    "tools": [{
      "function_declarations": [
        {
          "name": "enable_lights",
          "description": "Turn on the lighting system."
        },
        {
          "name": "set_light_color",
          "description": "Set the light color. Lights must be enabled for this to work.",
          "parameters": {
            "type": "object",
            "properties": {
              "rgb_hex": {
                "type": "string",
                "description": "The light color as a 6-digit hex string, e.g. ff0000 for red."
              }
            },
            "required": [
              "rgb_hex"
            ]
          }
        },
        {
          "name": "stop_lights",
          "description": "Turn off the lighting system."
        }
      ]
    }],
    "tool_config": {
      "function_calling_config": {"mode": "auto"}
    },
    "contents": {
      "role": "user",
      "parts": {
        "text": "Turn on the lights please."
      }
    }
  }"#;
    let request: GenerateContentRequest = round_trip(
        docs,
        &json!({
            "contents": [{"role": "user", "parts": [{"text": "Turn on the lights please."}]}],
            "systemInstruction": {"parts": [{"text": "You are a helpful lighting system bot. You can turn lights on and off, and you can set the color. Do not perform any other tasks."}]},
            "tools": [{"functionDeclarations": [
                {"name": "enable_lights", "description": "Turn on the lighting system."},
                {"name": "set_light_color", "description": "Set the light color. Lights must be enabled for this to work.", "parameters": {
                    "type": "object",
                    "properties": {"rgb_hex": {"type": "string", "description": "The light color as a 6-digit hex string, e.g. ff0000 for red."}},
                    "required": ["rgb_hex"],
                }},
                {"name": "stop_lights", "description": "Turn off the lighting system."},
            ]}],
            "toolConfig": {"functionCallingConfig": {"mode": "AUTO"}},
        }),
    );
    assert_eq!(
        request
            .tool_config
            .and_then(|config| config.function_calling_config)
            .and_then(|config| config.mode),
        Some(FunctionCallingMode::Auto)
    );
}

#[test]
fn system_instruction_request() {
    // system_instruction.sh
    let docs = r#"{ "system_instruction": {
    "parts":
      { "text": "You are a cat. Your name is Neko."}},
    "contents": {
      "parts": {
        "text": "Hello there"}}}"#;
    round_trip::<GenerateContentRequest>(
        docs,
        &json!({
            "contents": [{"parts": [{"text": "Hello there"}]}],
            "systemInstruction": {"parts": [{"text": "You are a cat. Your name is Neko."}]},
        }),
    );
}

/// Fields the crate does not name are kept by name, at the top level of the request.
#[test]
fn cached_content_and_unnamed_fields_are_kept() {
    // cache.sh, with a field outside the public API and one the crate does not name.
    let docs = r#"{
      "contents": [
        {
          "parts":[{
            "text": "Please summarize this transcript"
          }],
          "role": "user"
        }
      ],
      "cachedContent": "cachedContents/a1b2c3",
      "labels": {"client": "example"},
      "sessionId": "-1234"
    }"#;
    let request: GenerateContentRequest = round_trip(
        docs,
        &json!({
            "contents": [{"role": "user", "parts": [{"text": "Please summarize this transcript"}]}],
            "cachedContent": "cachedContents/a1b2c3",
            "labels": {"client": "example"},
            "sessionId": "-1234",
        }),
    );
    assert_eq!(
        request.cached_content.as_deref(),
        Some("cachedContents/a1b2c3")
    );
    assert_eq!(
        request.other.keys().collect::<Vec<_>>(),
        ["labels", "sessionId"]
    );
}

#[test]
fn count_tokens_requests() {
    // count_tokens.sh: contents alone, and a chat.
    round_trip::<CountTokensRequest>(
        r#"{
      "contents": [{
        "parts":[{
          "text": "The quick brown fox jumps over the lazy dog."
          }]
        }]
      }"#,
        &json!({"contents": [{"parts": [{"text": "The quick brown fox jumps over the lazy dog."}]}]}),
    );
    round_trip::<CountTokensRequest>(
        r#"{
      "contents": [
        {"role": "user",
        "parts": [{"text": "Hi, my name is Bob."}]
        },
        {"role": "model",
         "parts":[{"text": "Hi Bob"}]
        }
      ]
      }"#,
        &json!({"contents": [
            {"role": "user", "parts": [{"text": "Hi, my name is Bob."}]},
            {"role": "model", "parts": [{"text": "Hi Bob"}]},
        ]}),
    );
    let response: CountTokensResponse = round_trip(
        r#"{"totalTokens": 10, "cachedContentTokenCount": 4}"#,
        &json!({"totalTokens": 10, "cachedContentTokenCount": 4}),
    );
    assert_eq!(
        (response.total_tokens, response.cached_content_token_count),
        (10, Some(4))
    );
}

#[test]
fn a_response_with_text_a_function_call_and_usage() {
    let body = json!({
        "candidates": [{
            "content": {"role": "model", "parts": [
                {"text": "Turning them on."},
                {"functionCall": {"id": "call_1", "name": "enable_lights", "args": {}}},
                {"text": "", "thoughtSignature": "c2ln"},
            ]},
            "finishReason": "STOP",
            "index": 0,
            "safetyRatings": [{"category": "HARM_CATEGORY_HARASSMENT", "probability": "NEGLIGIBLE"}],
        }],
        "promptFeedback": {"safetyRatings": [{"category": "HARM_CATEGORY_HATE_SPEECH", "probability": "LOW", "blocked": false}]},
        "usageMetadata": {
            "promptTokenCount": 120,
            "cachedContentTokenCount": 100,
            "candidatesTokenCount": 30,
            "thoughtsTokenCount": 12,
            "totalTokenCount": 162,
        },
        "modelVersion": "gemini-2.5-flash",
        "responseId": "8Bv3aLnOJ5Lbz7IP",
    });
    let response: GenerateContentResponse = round_trip(&body.to_string(), &body);
    let candidate = &response.candidates[0];
    assert_eq!(candidate.finish_reason, Some(FinishReason::Stop));
    assert_eq!(
        candidate.content.as_ref().map(|content| content
            .parts
            .iter()
            .filter_map(|part| part.function_call.as_ref().map(|call| call.name.as_str()))
            .collect::<Vec<_>>()),
        Some(vec!["enable_lights"])
    );
    assert_eq!(
        response
            .usage_metadata
            .and_then(|usage| usage.thoughts_token_count),
        Some(12)
    );
}

#[test]
fn every_finish_reason_reads_and_writes_by_its_documented_name() {
    let documented = [
        "FINISH_REASON_UNSPECIFIED",
        "STOP",
        "MAX_TOKENS",
        "SAFETY",
        "RECITATION",
        "LANGUAGE",
        "OTHER",
        "BLOCKLIST",
        "PROHIBITED_CONTENT",
        "SPII",
        "MALFORMED_FUNCTION_CALL",
        "IMAGE_SAFETY",
        "IMAGE_PROHIBITED_CONTENT",
        "IMAGE_OTHER",
        "NO_IMAGE",
        "IMAGE_RECITATION",
        "UNEXPECTED_TOOL_CALL",
        "TOO_MANY_TOOL_CALLS",
        "MISSING_THOUGHT_SIGNATURE",
        "MALFORMED_RESPONSE",
        "ESCALATION",
        "PUP_LIMITED_DISABLED",
        "CONTINUATION",
    ];
    for name in documented {
        let reason: FinishReason =
            serde_json::from_value(json!(name)).unwrap_or_else(|e| panic!("{name}: {e}"));
        assert_eq!(wire(&reason), json!(name));
    }
}

#[test]
fn a_blocked_prompt_has_feedback_and_no_candidates() {
    let body = json!({
        "promptFeedback": {"blockReason": "SAFETY", "safetyRatings": [{"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "probability": "HIGH", "blocked": true}]},
        "usageMetadata": {"promptTokenCount": 8, "totalTokenCount": 8},
        "modelVersion": "gemini-2.5-flash",
    });
    let response: GenerateContentResponse = round_trip(&body.to_string(), &body);
    assert!(response.candidates.is_empty(), "{response:?}");
}

#[test]
fn a_model_listing() {
    let body = json!({
        "models": [{
            "name": "models/gemini-2.0-flash",
            "baseModelId": "gemini-2.0-flash",
            "version": "2.0",
            "displayName": "Gemini 2.0 Flash",
            "description": "Gemini 2.0 Flash",
            "inputTokenLimit": 1_048_576,
            "outputTokenLimit": 8192,
            "supportedGenerationMethods": ["generateContent", "countTokens"],
        }],
        "nextPageToken": "page-2",
    });
    let listing: ListModelsResponse = round_trip(&body.to_string(), &body);
    assert_eq!(listing.models[0].id(), "gemini-2.0-flash");
}

#[test]
fn an_unknown_function_calling_mode_is_refused_with_the_valid_ones() {
    let error = serde_json::from_value::<GenerateContentRequest>(json!({
        "contents": [],
        "toolConfig": {"functionCallingConfig": {"mode": "SOMETIMES"}},
    }))
    .expect_err("an unknown mode");
    assert_eq!(
        error.to_string(),
        "unknown variant `SOMETIMES`, expected one of `MODE_UNSPECIFIED`, `AUTO`, `ANY`, `NONE`, `VALIDATED`"
    );
}
