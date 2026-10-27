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

use std::sync::{Arc, Mutex};

use async_openai::error::{ApiError, OpenAIError, StreamError};
use async_openai::types::chat::{
    ChatCompletionResponseStream, CreateChatCompletionRequest, CreateChatCompletionResponse,
    CreateChatCompletionStreamResponse,
};
use async_trait::async_trait;
use chat_api::{Chat, SqlGeneration};
use futures::StreamExt;
use gemini_api::{
    CountTokensRequest, GeminiError, GeminiStatus, GenerateContentRequest, GenerateContentResponse,
};
use serde::Serialize;
use serde_json::{Value, json};

use crate::request::{chat_request, json_schema};
use crate::response::{gemini_error, generate_content_response};
use crate::stream::Translator;
use crate::{GeminiChat, model_listing};

const DOCS: &str = "See: https://spiceai.org/docs/components/models";

/// `value` as the JSON sent on the wire. Serializing through a string, rather than to a
/// [`Value`], writes an `f32` such as `0.25` as written, not as its `f64` widening.
fn wire<T: Serialize>(value: &T) -> Value {
    serde_json::from_str(&serde_json::to_string(value).expect("serializable")).expect("JSON")
}

fn typed(body: &Value) -> GenerateContentRequest {
    serde_json::from_value(body.clone()).expect("a valid Gemini API request")
}

fn translated(body: &Value) -> Result<Value, GeminiError> {
    chat_request("gpt", typed(body), false).map(|request| wire(&request))
}

/// `value` as pretty JSON with every object's keys in sorted order, so a snapshot does not depend
/// on whether the workspace builds `serde_json` with `preserve_order`.
fn pretty(value: &Value) -> String {
    fn sorted(value: &Value) -> Value {
        match value {
            Value::Object(map) => {
                let mut entries: Vec<(&String, &Value)> = map.iter().collect();
                entries.sort_by_key(|(key, _)| *key);
                Value::Object(
                    entries
                        .into_iter()
                        .map(|(key, value)| (key.clone(), sorted(value)))
                        .collect(),
                )
            }
            Value::Array(items) => Value::Array(items.iter().map(sorted).collect()),
            other => other.clone(),
        }
    }
    serde_json::to_string_pretty(&sorted(value)).expect("serializable")
}

fn user_text(text: &str) -> Value {
    json!({"role": "user", "parts": [{"text": text}]})
}

/// A request with `field` set beside one user turn.
fn hello_with(field: &str, value: Value) -> Value {
    let mut body = json!({"contents": [user_text("hi")]});
    body[field] = value;
    body
}

/// Two requests the Antigravity CLI 1.4.7 sent to its LLM gateway, sanitized: without `labels`
/// and `sessionId`, and with long texts cut to 300 characters.
#[test]
fn the_antigravity_cli_requests_translate_to_chat_completion_requests() {
    for (snapshot, fixture) in [
        (
            "agy_title_request_as_chat_completion",
            include_str!("../fixtures/agy_stream_generate_content.json"),
        ),
        (
            "agy_agent_request_as_chat_completion",
            include_str!("../fixtures/agy_stream_generate_content_2.json"),
        ),
    ] {
        let request: GenerateContentRequest =
            serde_json::from_str(fixture).expect("a valid Gemini API request");
        let chat = chat_request("qwen", request, true)
            .unwrap_or_else(|error| panic!("{snapshot} refused: {error}"));
        insta::assert_snapshot!(snapshot, pretty(&wire(&chat)));
    }
}

/// The fields the Antigravity CLI sends outside the public API, and the fields that only change
/// how a request is served or that the chat model's own behavior stands in for, are accepted and
/// left out.
#[test]
fn what_a_chat_completion_has_no_equivalent_for_is_accepted_and_left_out() {
    let expected = json!({"model": "gpt", "messages": [{"role": "user", "content": "hi"}]});
    for (field, value) in [
        ("labels", json!({"client": "antigravity-cli"})),
        ("sessionId", json!("-1234")),
        ("serviceTier", json!("standard")),
        ("store", json!(false)),
        (
            "safetySettings",
            json!([{"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_NONE"}]),
        ),
        ("generationConfig", json!({"topK": 40})),
        (
            "generationConfig",
            json!({"thinkingConfig": {"includeThoughts": true, "thinkingBudget": 0, "thinkingLevel": "HIGH"}}),
        ),
        ("generationConfig", json!({"candidateCount": 1})),
        (
            "generationConfig",
            json!({"responseMimeType": "text/plain"}),
        ),
        ("generationConfig", json!({"responseModalities": ["TEXT"]})),
        (
            "tools",
            json!([{"googleSearch": {}}, {"codeExecution": {}}, {"urlContext": {}}, {"googleSearchRetrieval": {}}]),
        ),
    ] {
        let chat = translated(&hello_with(field, value.clone()))
            .unwrap_or_else(|error| panic!("{field}: {value} refused: {error}"));
        assert_eq!(chat, expected, "{field}: {value}");
    }
}

#[test]
fn openapi_schemas_become_json_schema() {
    let cases = [
        // Nested objects, with types in either case.
        (
            json!({"type": "OBJECT", "properties": {"location": {"type": "object", "description": "Where.", "properties": {"city": {"type": "STRING"}, "zip": {"type": "INTEGER"}}, "required": ["city"]}}, "required": ["location"]}),
            json!({"type": "object", "properties": {"location": {"type": "object", "description": "Where.", "properties": {"city": {"type": "string"}, "zip": {"type": "integer"}}, "required": ["city"]}}, "required": ["location"]}),
        ),
        // Arrays, with counts written as int64 strings.
        (
            json!({"type": "ARRAY", "items": {"type": "NUMBER"}, "minItems": "1", "maxItems": 5}),
            json!({"type": "array", "items": {"type": "number"}, "minItems": 1, "maxItems": 5}),
        ),
        (
            json!({"type": "ARRAY", "items": {"type": "ARRAY", "items": {"type": "BOOLEAN"}}}),
            json!({"type": "array", "items": {"type": "array", "items": {"type": "boolean"}}}),
        ),
        // An enum, whose `format: enum` JSON Schema has no place for.
        (
            json!({"type": "STRING", "format": "enum", "enum": ["celsius", "fahrenheit"]}),
            json!({"type": "string", "enum": ["celsius", "fahrenheit"]}),
        ),
        // Nullable: a type union, and `null` among the values of an enum and the options of an
        // `anyOf`.
        (
            json!({"type": "STRING", "nullable": true}),
            json!({"type": ["string", "null"]}),
        ),
        (
            json!({"type": "STRING", "enum": ["a", "b"], "nullable": true}),
            json!({"type": ["string", "null"], "enum": ["a", "b", null]}),
        ),
        (
            json!({"anyOf": [{"type": "STRING"}, {"type": "INTEGER", "nullable": true}], "nullable": true}),
            json!({"anyOf": [{"type": "string"}, {"type": ["integer", "null"]}, {"type": "null"}]}),
        ),
        (
            json!({"type": "OBJECT", "properties": {"note": {"type": "STRING", "nullable": true}}, "nullable": false}),
            json!({"type": "object", "properties": {"note": {"type": ["string", "null"]}}}),
        ),
        (
            json!({"description": "Anything.", "nullable": true}),
            json!({"description": "Anything."}),
        ),
        // The other keywords, kept; a single `example` as `examples`.
        (
            json!({"type": "STRING", "format": "date-time", "title": "When", "pattern": "^2", "minLength": "20", "example": "2026-01-01T00:00:00Z", "default": "2026-01-01T00:00:00Z"}),
            json!({"type": "string", "format": "date-time", "title": "When", "pattern": "^2", "minLength": 20, "examples": ["2026-01-01T00:00:00Z"], "default": "2026-01-01T00:00:00Z"}),
        ),
        (
            json!({"type": "INTEGER", "minimum": 0, "maximum": 10}),
            json!({"type": "integer", "minimum": 0, "maximum": 10}),
        ),
        // `propertyOrdering` is left out, and an unspecified type with it.
        (
            json!({"type": "OBJECT", "properties": {"b": {"type": "TYPE_UNSPECIFIED"}, "a": {"type": "NULL"}}, "propertyOrdering": ["b", "a"], "minProperties": "1"}),
            json!({"type": "object", "properties": {"b": {}, "a": {"type": "null"}}, "minProperties": 1}),
        ),
        // Keywords in snake_case take their JSON Schema names.
        (
            json!({"type": "ARRAY", "items": {"any_of": [{"type": "STRING"}]}, "max_items": "3", "property_ordering": []}),
            json!({"type": "array", "items": {"anyOf": [{"type": "string"}]}, "maxItems": 3}),
        ),
    ];
    for (schema, expected) in cases {
        let converted = json_schema(schema.clone(), "parameters")
            .unwrap_or_else(|error| panic!("{schema}: {error:?}"));
        assert_eq!(converted, expected, "{schema}");
    }
}

fn weather_tool(parameters: &Value) -> Value {
    json!([{"functionDeclarations": [{"name": "get_weather", "description": "Gets the weather.", "parameters": parameters}]}])
}

#[test]
fn a_schema_that_is_not_one_is_refused_where_it_is_wrong() {
    let cases = [
        (
            json!({"type": "OBJECT", "properties": {"when": {"type": "DATE"}}}),
            "`parameters.properties.when.type` is \"DATE\", which is not one of STRING, NUMBER, INTEGER, BOOLEAN, ARRAY, OBJECT and NULL",
        ),
        (
            json!({"type": "ARRAY", "items": {"type": "STRING", "maxLength": "long"}}),
            "`parameters.items.maxLength` is \"long\", not a count",
        ),
        (
            json!({"type": "OBJECT", "properties": ["city"]}),
            "`parameters.properties` is [\"city\"], not an object of Schemas",
        ),
        (
            json!({"anyOf": [{"type": "STRING"}, "INTEGER"]}),
            "`parameters.anyOf[1]` is \"INTEGER\", not a Schema object",
        ),
    ];
    for (parameters, problem) in cases {
        let error = translated(&hello_with("tools", weather_tool(&parameters)))
            .expect_err("an invalid schema");
        assert_eq!(
            error,
            GeminiError::invalid_argument(format!(
                "Failed to read the `parameters` of the function 'get_weather' as a Schema: {problem}. See: https://ai.google.dev/api/caching#Schema"
            )),
            "{parameters}"
        );
    }
}

#[test]
fn turns_and_parts_become_chat_messages() {
    let body = json!({
        "systemInstruction": {"role": "user", "parts": [{"text": "You are a coding agent."}, {"text": "Be brief."}]},
        "contents": [
            {"role": "user", "parts": [
                {"text": "Show me "},
                {"text": "the logo."},
                {"inlineData": {"mimeType": "image/png", "data": "iVBORw0KGgo"}},
                {"text": "Describe it."},
            ]},
            {"role": "model", "parts": [
                {"text": "I should look at it.", "thought": true},
                {"text": "It is a ", "thoughtSignature": "c2ln"},
                {"text": "red circle."},
            ]},
            {"parts": [{"text": "Thanks"}]},
            // A turn of reasoning alone carries nothing for another model.
            {"role": "model", "parts": [{"text": "Only reasoning.", "thought": true}, {"text": "", "thoughtSignature": "c2ln"}]},
            // In snake_case, in the URL-safe alphabet and without padding, as Google reads it.
            {"role": "user", "parts": [{"inline_data": {"mime_type": "IMAGE/JPEG", "data": "_-8"}}]},
        ],
    });
    assert_eq!(
        translated(&body).expect("a translatable request"),
        json!({"model": "gpt", "messages": [
            {"role": "system", "content": "You are a coding agent.\n\nBe brief."},
            {"role": "user", "content": [
                {"type": "text", "text": "Show me the logo."},
                {"type": "image_url", "image_url": {"url": "data:image/png;base64,iVBORw0KGgo="}},
                {"type": "text", "text": "Describe it."},
            ]},
            {"role": "assistant", "content": "It is a red circle."},
            {"role": "user", "content": "Thanks"},
            {"role": "user", "content": [{"type": "image_url", "image_url": {"url": "data:IMAGE/JPEG;base64,/+8="}}]},
        ]})
    );
}

#[test]
fn a_system_instruction_without_text_adds_no_system_message() {
    for system in [
        json!({"parts": []}),
        json!({"role": "user", "parts": [{"text": ""}, {"text": "Hidden.", "thought": true}]}),
    ] {
        assert_eq!(
            translated(&hello_with("systemInstruction", system.clone()))
                .expect("a translatable request")["messages"],
            json!([{"role": "user", "content": "hi"}]),
            "{system}"
        );
    }
}

#[test]
fn function_calls_and_responses_with_ids_keep_them() {
    let body = json!({"contents": [
        user_text("What's the weather in Paris and Rome?"),
        {"role": "model", "parts": [
            {"text": "Checking."},
            {"functionCall": {"id": "fc_1", "name": "get_weather", "args": {"city": "Paris"}}},
            {"functionCall": {"id": "fc_2", "name": "get_weather", "args": {"city": "Rome"}}},
        ]},
        {"role": "user", "parts": [
            {"functionResponse": {"id": "fc_2", "name": "get_weather", "response": {"celsius": 20}}},
            {"text": "Compare them."},
            {"functionResponse": {"id": "fc_1", "name": "get_weather", "response": {"celsius": 18}}},
        ]},
    ]});
    assert_eq!(
        translated(&body).expect("a translatable request")["messages"],
        json!([
            {"role": "user", "content": "What's the weather in Paris and Rome?"},
            {"role": "assistant", "content": "Checking.", "tool_calls": [
                {"type": "function", "id": "fc_1", "function": {"name": "get_weather", "arguments": "{\"city\":\"Paris\"}"}},
                {"type": "function", "id": "fc_2", "function": {"name": "get_weather", "arguments": "{\"city\":\"Rome\"}"}},
            ]},
            // Tool messages come first, as the chat completion pairs them with the calls.
            {"role": "tool", "tool_call_id": "fc_2", "content": "{\"celsius\":20}"},
            {"role": "tool", "tool_call_id": "fc_1", "content": "{\"celsius\":18}"},
            {"role": "user", "content": "Compare them."},
        ])
    );
}

/// Calls without IDs get `call_<n>`, stable within the request, and the responses without IDs
/// answer the first unanswered call to the function they name, in order. A response whose ID
/// answers no call is matched by its name.
#[test]
fn function_calls_without_ids_get_stable_ids_and_responses_match_by_name_in_order() {
    let body = json!({"contents": [
        user_text("Paris and Rome, and the time?"),
        {"role": "model", "parts": [
            {"functionCall": {"name": "get_weather", "args": {"city": "Paris"}}},
            {"functionCall": {"name": "get_time"}},
            {"functionCall": {"name": "get_weather", "args": {"city": "Rome"}}},
        ]},
        {"role": "user", "parts": [
            {"functionResponse": {"name": "get_time", "response": {"time": "12:00"}}},
            {"functionResponse": {"name": "get_weather", "response": {"city": "Paris"}}},
            {"functionResponse": {"id": "not_a_call", "name": "get_weather", "response": {"city": "Rome"}}},
        ]},
        {"role": "model", "parts": [{"functionCall": {"name": "get_weather", "args": {"city": "Oslo"}}}]},
        // The role older Gemini clients give function responses.
        {"role": "function", "parts": [{"functionResponse": {"name": "get_weather", "response": {"city": "Oslo"}}}]},
    ]});
    assert_eq!(
        translated(&body).expect("a translatable request")["messages"],
        json!([
            {"role": "user", "content": "Paris and Rome, and the time?"},
            {"role": "assistant", "tool_calls": [
                {"type": "function", "id": "call_1", "function": {"name": "get_weather", "arguments": "{\"city\":\"Paris\"}"}},
                {"type": "function", "id": "call_2", "function": {"name": "get_time", "arguments": "{}"}},
                {"type": "function", "id": "call_3", "function": {"name": "get_weather", "arguments": "{\"city\":\"Rome\"}"}},
            ]},
            {"role": "tool", "tool_call_id": "call_2", "content": "{\"time\":\"12:00\"}"},
            {"role": "tool", "tool_call_id": "call_1", "content": "{\"city\":\"Paris\"}"},
            {"role": "tool", "tool_call_id": "call_3", "content": "{\"city\":\"Rome\"}"},
            {"role": "assistant", "tool_calls": [
                {"type": "function", "id": "call_4", "function": {"name": "get_weather", "arguments": "{\"city\":\"Oslo\"}"}},
            ]},
            {"role": "tool", "tool_call_id": "call_4", "content": "{\"city\":\"Oslo\"}"},
        ])
    );
}

/// A function's images reach the model in a user message after the tool messages, since a tool
/// message carries only text.
#[test]
fn images_a_function_returned_follow_its_tool_message() {
    let body = json!({"contents": [
        user_text("Show me the chart."),
        {"role": "model", "parts": [{"functionCall": {"id": "fc_1", "name": "render_chart", "args": {}}}]},
        {"role": "user", "parts": [{"functionResponse": {"id": "fc_1", "name": "render_chart", "response": {"rendered": true}, "parts": [
            {"inlineData": {"mimeType": "image/png", "data": "iVBORw0K"}},
        ]}}]},
    ]});
    assert_eq!(
        translated(&body).expect("a translatable request")["messages"],
        json!([
            {"role": "user", "content": "Show me the chart."},
            {"role": "assistant", "tool_calls": [{"type": "function", "id": "fc_1", "function": {"name": "render_chart", "arguments": "{}"}}]},
            {"role": "tool", "tool_call_id": "fc_1", "content": "{\"rendered\":true}"},
            {"role": "user", "content": [{"type": "image_url", "image_url": {"url": "data:image/png;base64,iVBORw0K"}}]},
        ])
    );
}

#[test]
fn generation_config_becomes_the_chat_completions_settings() {
    let body = hello_with(
        "generationConfig",
        json!({
            "temperature": 0.5,
            "topP": 0.25,
            "maxOutputTokens": 1024,
            "stopSequences": ["END", ""],
            "presencePenalty": 0.5,
            "frequencyPenalty": -0.5,
            "seed": 7,
            "responseMimeType": "application/json",
            "responseSchema": {"type": "OBJECT", "properties": {"answer": {"type": "STRING", "nullable": true}}, "required": ["answer"]},
        }),
    );
    assert_eq!(
        translated(&body).expect("a translatable request"),
        json!({
            "model": "gpt",
            "messages": [{"role": "user", "content": "hi"}],
            "temperature": 0.5,
            "top_p": 0.25,
            "max_completion_tokens": 1024,
            "stop": ["END"],
            "presence_penalty": 0.5,
            "frequency_penalty": -0.5,
            "seed": 7,
            "response_format": {"type": "json_schema", "json_schema": {
                "name": "response",
                "schema": {"type": "object", "properties": {"answer": {"type": ["string", "null"]}}, "required": ["answer"]},
            }},
        })
    );
}

#[test]
fn a_json_response_mime_type_asks_for_json() {
    let format = |config: Value| {
        translated(&hello_with("generationConfig", config.clone()))
            .unwrap_or_else(|error| panic!("{config} refused: {error}"))["response_format"]
            .clone()
    };
    // JSON Schema is sent as it is, and before an OpenAPI schema.
    let schema = json!({"type": "object", "properties": {"n": {"type": "integer"}}, "additionalProperties": false});
    assert_eq!(
        format(
            json!({"responseMimeType": "application/json", "responseJsonSchema": schema, "responseSchema": {"type": "STRING"}})
        ),
        json!({"type": "json_schema", "json_schema": {"name": "response", "schema": schema}})
    );
    assert_eq!(
        format(json!({"response_mime_type": "Application/JSON"})),
        json!({"type": "json_object"})
    );
}

#[test]
fn function_declarations_become_tools_and_the_calling_mode_a_tool_choice() {
    let declarations = json!([{"functionDeclarations": [
        {"name": "get_weather", "description": "Gets the weather.", "parameters": {"type": "OBJECT", "properties": {"city": {"type": "STRING"}}, "required": ["city"]}},
        {"name": "get_time", "parametersJsonSchema": {"type": "object", "properties": {}, "additionalProperties": false}},
    ]}, {"googleSearch": {}}, {"functionDeclarations": [{"name": "stop"}]}]);
    let weather = json!({"type": "function", "function": {"name": "get_weather", "description": "Gets the weather.", "parameters": {"type": "object", "properties": {"city": {"type": "string"}}, "required": ["city"]}}});
    let time = json!({"type": "function", "function": {"name": "get_time", "parameters": {"type": "object", "properties": {}, "additionalProperties": false}}});
    let stop = json!({"type": "function", "function": {"name": "stop"}});
    let translate = |config: Value| {
        let mut body = hello_with("tools", declarations.clone());
        if !config.is_null() {
            body["toolConfig"] = json!({"functionCallingConfig": config});
        }
        let chat = translated(&body).unwrap_or_else(|error| panic!("{config} refused: {error}"));
        (chat["tools"].clone(), chat["tool_choice"].clone())
    };
    let all = json!([weather, time, stop]);
    let cases = [
        (Value::Null, (all.clone(), Value::Null)),
        (
            json!({"mode": "MODE_UNSPECIFIED"}),
            (all.clone(), Value::Null),
        ),
        (json!({"mode": "AUTO"}), (all.clone(), json!("auto"))),
        (json!({"mode": "none"}), (all.clone(), json!("none"))),
        (json!({"mode": "ANY"}), (all.clone(), json!("required"))),
        (json!({"mode": "VALIDATED"}), (all, json!("auto"))),
        (
            json!({"mode": "ANY", "allowedFunctionNames": ["get_time"]}),
            (
                json!([time]),
                json!({"type": "function", "function": {"name": "get_time"}}),
            ),
        ),
        (
            json!({"mode": "any", "allowed_function_names": ["get_time", "get_weather"]}),
            (json!([weather, time]), json!("required")),
        ),
        (
            json!({"mode": "AUTO", "allowedFunctionNames": ["get_weather"]}),
            (json!([weather]), json!("auto")),
        ),
    ];
    for (config, expected) in cases {
        assert_eq!(translate(config.clone()), expected, "{config}");
    }
}

#[test]
fn what_a_chat_completion_cannot_carry_is_refused_by_name() {
    let unsupported = |what: &str, remedy: &str| {
        format!(
            "Model 'gpt' serves the Gemini API by translating requests into chat completions, which cannot carry {what}. {remedy} {DOCS}"
        )
    };
    let model_turn =
        |parts: Value| json!({"contents": [user_text("q"), {"role": "model", "parts": parts}]});
    let user_turn = |parts: Value| json!({"contents": [{"role": "user", "parts": parts}]});
    let cases = [
        (
            hello_with("generationConfig", json!({"candidateCount": 2})),
            unsupported(
                "2 candidates",
                "Set `candidateCount` to 1, or leave it unset, and send a request for each answer.",
            ),
        ),
        (
            hello_with("cachedContent", json!("cachedContents/abc")),
            unsupported(
                "cached content ('cachedContents/abc')",
                "Send the cached contents in `contents` instead of `cachedContent`.",
            ),
        ),
        (
            hello_with(
                "generationConfig",
                json!({"responseModalities": ["TEXT", "IMAGE"]}),
            ),
            unsupported(
                "the response modality 'IMAGE'",
                "Ask for `responseModalities` [\"TEXT\"], or leave it unset.",
            ),
        ),
        (
            hello_with(
                "generationConfig",
                json!({"responseMimeType": "text/x.enum"}),
            ),
            unsupported(
                "the response MIME type 'text/x.enum'",
                "Use the `responseMimeType` 'application/json' or 'text/plain'.",
            ),
        ),
        (
            user_turn(json!([{"inlineData": {"mimeType": "application/pdf", "data": "JVBERi0="}}])),
            unsupported(
                "inline data of type 'application/pdf' in `contents[0]`",
                "A chat completion carries only images: send the content as text, or as an image (`image/*`).",
            ),
        ),
        (
            user_turn(
                json!([{"fileData": {"mimeType": "image/png", "fileUri": "https://generativelanguage.googleapis.com/v1beta/files/abc"}}]),
            ),
            unsupported(
                "data by URI ('https://generativelanguage.googleapis.com/v1beta/files/abc') in `contents[0]`",
                "Send the data inline as `inlineData` (an image) or as text.",
            ),
        ),
        (
            model_turn(json!([{"inlineData": {"mimeType": "image/png", "data": "iVBORw0K"}}])),
            unsupported(
                "inline data of type 'image/png' in `contents[1]`",
                "A model turn carries only text and function calls.",
            ),
        ),
        (
            model_turn(json!([{"executableCode": {"language": "PYTHON", "code": "print(1)"}}])),
            unsupported(
                "an `executableCode` part in `contents[1]`",
                "A chat completion cannot run the `codeExecution` tool: remove the part.",
            ),
        ),
        (
            user_turn(json!([{"codeExecutionResult": {"outcome": "OUTCOME_OK", "output": "1"}}])),
            unsupported(
                "a `codeExecutionResult` part in `contents[0]`",
                "A chat completion cannot run the `codeExecution` tool: remove the part.",
            ),
        ),
        (
            model_turn(json!([{"toolCall": {"toolType": "GOOGLE_SEARCH_WEB", "args": {}}}])),
            unsupported("a `toolCall` part in `contents[1]`", "Remove the part."),
        ),
        (
            hello_with(
                "systemInstruction",
                json!({"parts": [{"inlineData": {"mimeType": "image/png", "data": "iVBORw0K"}}]}),
            ),
            unsupported(
                "inline data of type 'image/png' in `systemInstruction`",
                "Send the system instruction as text.",
            ),
        ),
        (
            json!({"contents": [
                user_text("q"),
                {"role": "model", "parts": [{"functionCall": {"name": "fetch"}}]},
                {"role": "user", "parts": [{"functionResponse": {"name": "fetch", "response": {}, "parts": [{"inlineData": {"mimeType": "audio/wav", "data": "UklGRg=="}}]}}]},
            ]}),
            unsupported(
                "inline data of type 'audio/wav' in the response of 'fetch' in `contents[2]`",
                "A chat completion carries only images: return the content in `response`, or as an image (`image/*`).",
            ),
        ),
    ];
    for (body, expected) in cases {
        let error = translated(&body).expect_err("a refused request");
        assert_eq!(
            (error.http_status(), error),
            (400, GeminiError::invalid_argument(expected)),
            "{body}"
        );
    }
}

#[test]
fn a_request_that_is_not_a_valid_one_is_an_invalid_argument() {
    let cases = [
        (
            json!({"contents": []}),
            "The request has no `contents`. Send the conversation in `contents`, at least one turn.",
        ),
        (
            json!({"contents": [{"role": "assistant", "parts": [{"text": "hi"}]}]}),
            "`contents[0]` has the role 'assistant'. Use the role `user` or `model`.",
        ),
        (
            json!({"contents": [{"role": "user", "parts": [{"functionCall": {"name": "get_time"}}]}]}),
            "`contents[0]` has the role 'user' and carries a `functionCall` part, but only the model calls functions. Send function calls in a turn with the role `model`.",
        ),
        (
            json!({"contents": [user_text("q"), {"role": "model", "parts": [{"functionResponse": {"name": "get_time", "response": {}}}]}]}),
            "`contents[1]` has the role 'model' and carries a `functionResponse` part. Send function responses in a turn with the role `user`.",
        ),
        (
            json!({"contents": [
                user_text("q"),
                {"role": "model", "parts": [{"functionCall": {"name": "get_weather"}}]},
                {"role": "user", "parts": [{"functionResponse": {"name": "get_time", "response": {}}}]},
            ]}),
            "The `functionResponse` of 'get_time' in `contents[2]` answers none of the `functionCall` parts of the model turn before it. Send each function response in the user turn right after the model turn that called the function, with the call's `id` or the function's `name`.",
        ),
        (
            json!({"contents": [
                user_text("q"),
                {"role": "model", "parts": [{"functionCall": {"name": "get_weather"}}]},
                {"role": "user", "parts": [
                    {"functionResponse": {"name": "get_weather", "response": {}}},
                    {"functionResponse": {"name": "get_weather", "response": {}}},
                ]},
            ]}),
            "The `functionResponse` of 'get_weather' in `contents[2]` answers none of the `functionCall` parts of the model turn before it. Send each function response in the user turn right after the model turn that called the function, with the call's `id` or the function's `name`.",
        ),
        (
            json!({"contents": [{"role": "user", "parts": [{"text": "q", "functionCall": {"name": "get_time"}}]}]}),
            "A part of `contents[0]` carries `text` and `functionCall`, but a part carries one kind of data. Send each in a part of its own.",
        ),
        (
            hello_with(
                "generationConfig",
                json!({"responseSchema": {"type": "STRING"}}),
            ),
            "`generationConfig.responseSchema` needs the `responseMimeType` 'application/json'. Set `responseMimeType` to 'application/json', or remove `responseSchema`.",
        ),
        (
            hello_with(
                "generationConfig",
                json!({"responseMimeType": "text/plain", "responseJsonSchema": {"type": "string"}}),
            ),
            "`generationConfig.responseJsonSchema` needs the `responseMimeType` 'application/json'. Set `responseMimeType` to 'application/json', or remove `responseJsonSchema`.",
        ),
        (
            json!({"contents": [user_text("q")], "tools": weather_tool(&json!({"type": "OBJECT"})), "toolConfig": {"functionCallingConfig": {"mode": "ANY", "allowedFunctionNames": ["delete_file"]}}}),
            "`allowedFunctionNames` names 'delete_file', which no function declaration in `tools` declares. Declare the function, or remove it from `allowedFunctionNames`.",
        ),
        (
            json!({"contents": [user_text("q")], "tools": [{"googleSearch": {}}], "toolConfig": {"functionCallingConfig": {"mode": "ANY"}}}),
            "`functionCallingConfig` has the mode ANY, which has the model call a function, but `tools` declares no function. Declare functions in `tools`, or use the mode AUTO.",
        ),
    ];
    for (body, expected) in cases {
        let error = translated(&body).expect_err("an invalid request");
        assert_eq!(
            (error.status, error.message.as_str()),
            (GeminiStatus::InvalidArgument, expected),
            "{body}"
        );
    }
}

fn completion(
    message: &Value,
    finish_reason: Option<&str>,
    usage: &Value,
) -> CreateChatCompletionResponse {
    serde_json::from_value(json!({
        "id": "chatcmpl-abc",
        "object": "chat.completion",
        "created": 0,
        "model": "provider-model-name",
        "choices": [{"index": 0, "message": message, "finish_reason": finish_reason}],
        "usage": usage,
    }))
    .expect("a chat completion")
}

fn full_usage() -> Value {
    json!({
        "prompt_tokens": 120, "completion_tokens": 30, "total_tokens": 150,
        "prompt_tokens_details": {"cached_tokens": 100},
        "completion_tokens_details": {"reasoning_tokens": 12},
    })
}

/// `response` as JSON, its random `responseId` checked and left out.
fn without_response_id(response: &GenerateContentResponse) -> Value {
    let mut value = wire(response);
    let id = value
        .as_object_mut()
        .and_then(|object| object.remove("responseId"))
        .expect("a responseId");
    let id = id.as_str().expect("a string");
    assert!(
        id.len() == 24 && id.chars().all(|c| c.is_ascii_alphanumeric()),
        "{id}"
    );
    value
}

#[test]
fn a_completion_becomes_one_candidate_with_text_then_function_calls() {
    let answer = completion(
        &json!({
            "role": "assistant",
            "content": "Reading both.",
            "reasoning_content": "The user wants the files.",
            "tool_calls": [
                {"id": "call_1", "type": "function", "function": {"name": "read_file", "arguments": "{\"path\":\"a.rs\"}"}},
                {"id": "", "type": "function", "function": {"name": "list_files", "arguments": " "}},
            ],
        }),
        Some("tool_calls"),
        &full_usage(),
    );
    let response = generate_content_response("qwen", answer).expect("a response");
    assert_eq!(
        without_response_id(&response),
        json!({
            "candidates": [{
                "content": {"role": "model", "parts": [
                    {"text": "Reading both."},
                    {"functionCall": {"id": "call_1", "name": "read_file", "args": {"path": "a.rs"}}},
                    {"functionCall": {"name": "list_files", "args": {}}},
                ]},
                "finishReason": "STOP",
                "index": 0,
            }],
            "usageMetadata": {
                "promptTokenCount": 120,
                "cachedContentTokenCount": 100,
                "candidatesTokenCount": 18,
                "thoughtsTokenCount": 12,
                "totalTokenCount": 150,
            },
            "modelVersion": "qwen",
        })
    );
}

#[test]
fn finish_reasons_become_gemini_finish_reasons() {
    let finish = |reason: Option<&str>| {
        let answer = completion(
            &json!({"role": "assistant", "content": "hi"}),
            reason,
            &Value::Null,
        );
        wire(&generate_content_response("gpt", answer).expect("a response"))["candidates"][0]
            ["finishReason"]
            .clone()
    };
    assert_eq!(
        [
            finish(Some("stop")),
            finish(Some("tool_calls")),
            finish(Some("function_call")),
            finish(Some("length")),
            finish(Some("content_filter")),
            finish(None),
        ],
        [
            json!("STOP"),
            json!("STOP"),
            json!("STOP"),
            json!("MAX_TOKENS"),
            json!("SAFETY"),
            json!("STOP"),
        ]
    );
}

/// The counts a chat completion did not report are left out, never reported as `0`.
#[test]
fn usage_carries_the_counts_the_chat_completion_reported() {
    let usage = |usage: Value| {
        let answer = completion(
            &json!({"role": "assistant", "content": "hi"}),
            Some("stop"),
            &usage,
        );
        wire(&generate_content_response("gpt", answer).expect("a response"))["usageMetadata"]
            .clone()
    };
    let cases = [
        (
            full_usage(),
            json!({"promptTokenCount": 120, "cachedContentTokenCount": 100, "candidatesTokenCount": 18, "thoughtsTokenCount": 12, "totalTokenCount": 150}),
        ),
        (
            json!({"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15}),
            json!({"promptTokenCount": 10, "candidatesTokenCount": 5, "totalTokenCount": 15}),
        ),
        (
            json!({"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15, "prompt_tokens_details": {"cached_tokens": null, "audio_tokens": 3}, "completion_tokens_details": {"reasoning_tokens": null}}),
            json!({"promptTokenCount": 10, "candidatesTokenCount": 5, "totalTokenCount": 15}),
        ),
        (
            json!({"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15, "prompt_tokens_details": {"cached_tokens": 0}, "completion_tokens_details": {"reasoning_tokens": 0}}),
            json!({"promptTokenCount": 10, "cachedContentTokenCount": 0, "candidatesTokenCount": 5, "thoughtsTokenCount": 0, "totalTokenCount": 15}),
        ),
        // A provider that reports more reasoning tokens than completion tokens counted them apart.
        (
            json!({"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 24, "completion_tokens_details": {"reasoning_tokens": 9}}),
            json!({"promptTokenCount": 10, "candidatesTokenCount": 5, "thoughtsTokenCount": 9, "totalTokenCount": 24}),
        ),
        (Value::Null, Value::Null),
    ];
    for (reported, expected) in cases {
        assert_eq!(usage(reported.clone()), expected, "{reported}");
    }
}

#[test]
fn function_arguments_that_are_not_an_object_are_an_internal_error() {
    for arguments in ["{\"path\": ", "[\"a.rs\"]"] {
        let answer = completion(
            &json!({"role": "assistant", "content": null, "tool_calls": [
                {"id": "call_1", "type": "function", "function": {"name": "read_file", "arguments": arguments}},
            ]}),
            Some("tool_calls"),
            &Value::Null,
        );
        assert_eq!(
            generate_content_response("gpt", answer).expect_err("invalid arguments"),
            GeminiError::internal(format!(
                "Model 'gpt' called the function 'read_file' with arguments that are not a JSON object, which the Gemini API cannot return: {arguments}"
            ))
        );
    }
}

/// A call cut off by the token limit was never made whole: the answer ends with `MAX_TOKENS`
/// without it.
#[test]
fn a_function_call_cut_off_by_the_token_limit_is_left_out() {
    let answer = completion(
        &json!({"role": "assistant", "content": "Writing it.", "tool_calls": [
            {"id": "call_1", "type": "function", "function": {"name": "write_file", "arguments": "{\"path\": \"a.rs\", \"content\": \"fn ma"}},
        ]}),
        Some("length"),
        &Value::Null,
    );
    let response = generate_content_response("gpt", answer).expect("a response");
    assert_eq!(
        wire(&response)["candidates"],
        json!([{"content": {"role": "model", "parts": [{"text": "Writing it."}]}, "finishReason": "MAX_TOKENS", "index": 0}])
    );
}

#[test]
fn chat_completion_errors_keep_their_kind() {
    let api_error = |code: &str| {
        gemini_error(OpenAIError::ApiError(ApiError {
            message: "m".to_string(),
            r#type: None,
            param: None,
            code: Some(code.to_string()),
        }))
    };
    let errors = [
        api_error("invalid_request_error"),
        api_error("invalid_api_key"),
        api_error("permission_error"),
        api_error("not_found_error"),
        api_error("rate_limit_exceeded"),
        api_error("insufficient_quota"),
        api_error("server_is_on_fire"),
        gemini_error(OpenAIError::InvalidArgument("bad".to_string())),
        gemini_error(OpenAIError::StreamError(Box::new(
            StreamError::EventStream("connection reset".to_string()),
        ))),
        gemini_error(OpenAIError::JSONDeserialize(
            serde_json::from_str::<Value>("{").expect_err("invalid JSON"),
            "{".to_string(),
        )),
    ];
    assert_eq!(
        errors.map(|error| (error.http_status(), error.status.as_str())),
        [
            (400, "INVALID_ARGUMENT"),
            (401, "UNAUTHENTICATED"),
            (403, "PERMISSION_DENIED"),
            (404, "NOT_FOUND"),
            (429, "RESOURCE_EXHAUSTED"),
            (429, "RESOURCE_EXHAUSTED"),
            (500, "INTERNAL"),
            (400, "INVALID_ARGUMENT"),
            (503, "UNAVAILABLE"),
            (500, "INTERNAL"),
        ]
    );
}

fn chunk(choice: &Value) -> CreateChatCompletionStreamResponse {
    serde_json::from_value(json!({
        "id": "chatcmpl-abc", "object": "chat.completion.chunk", "created": 0, "model": "provider-model-name",
        "choices": [choice],
    }))
    .expect("a chunk")
}

fn text_chunk(text: &str) -> CreateChatCompletionStreamResponse {
    chunk(&json!({"index": 0, "delta": {"content": text}, "finish_reason": null}))
}

fn finish_chunk(reason: &str) -> CreateChatCompletionStreamResponse {
    chunk(&json!({"index": 0, "delta": {}, "finish_reason": reason}))
}

fn usage_chunk() -> CreateChatCompletionStreamResponse {
    serde_json::from_value(json!({
        "id": "chatcmpl-abc", "object": "chat.completion.chunk", "created": 0, "model": "provider-model-name",
        "choices": [],
        "usage": full_usage(),
    }))
    .expect("a usage chunk")
}

fn tool_call_chunk(
    index: u32,
    id: Option<&str>,
    name: Option<&str>,
    arguments: &str,
) -> CreateChatCompletionStreamResponse {
    let mut function = json!({"arguments": arguments});
    if let Some(name) = name {
        function["name"] = json!(name);
    }
    let mut call = json!({"index": index, "function": function});
    if let Some(id) = id {
        call["id"] = json!(id);
        call["type"] = json!("function");
    }
    chunk(&json!({"index": 0, "delta": {"tool_calls": [call]}, "finish_reason": null}))
}

type ScriptedChunks = Vec<Result<CreateChatCompletionStreamResponse, OpenAIError>>;

/// A chat model that records each request, and answers with the scripted answer or streams the
/// scripted chunks.
struct ScriptedChat {
    requests: Mutex<Vec<CreateChatCompletionRequest>>,
    answer: Mutex<Option<Result<CreateChatCompletionResponse, OpenAIError>>>,
    chunks: Mutex<ScriptedChunks>,
}

impl ScriptedChat {
    fn new(
        answer: Option<Result<CreateChatCompletionResponse, OpenAIError>>,
        chunks: ScriptedChunks,
    ) -> Arc<Self> {
        Arc::new(Self {
            requests: Mutex::new(Vec::new()),
            answer: Mutex::new(answer),
            chunks: Mutex::new(chunks),
        })
    }

    fn requests(&self) -> Vec<Value> {
        self.requests
            .lock()
            .expect("requests lock")
            .iter()
            .map(wire)
            .collect()
    }
}

#[async_trait]
impl Chat for ScriptedChat {
    fn as_sql(&self) -> Option<&dyn SqlGeneration> {
        None
    }

    async fn chat_request(
        &self,
        req: CreateChatCompletionRequest,
    ) -> Result<CreateChatCompletionResponse, OpenAIError> {
        self.requests.lock().expect("requests lock").push(req);
        self.answer
            .lock()
            .expect("answer lock")
            .take()
            .unwrap_or_else(|| {
                Err(OpenAIError::InvalidArgument(
                    "no answer scripted".to_string(),
                ))
            })
    }

    async fn chat_stream(
        &self,
        req: CreateChatCompletionRequest,
    ) -> Result<ChatCompletionResponseStream, OpenAIError> {
        self.requests.lock().expect("requests lock").push(req);
        let chunks = std::mem::take(&mut *self.chunks.lock().expect("chunks lock"));
        Ok(Box::pin(futures::stream::iter(chunks)))
    }
}

/// The responses of a stream of `chunks`, as JSON, an error as its body. Every response carries
/// the same random `responseId`, which is checked and replaced with `<responseId>`.
async fn streamed(chunks: ScriptedChunks) -> Vec<Value> {
    let chat = ScriptedChat::new(None, chunks);
    let stream = GeminiChat::new(chat)
        .generate_stream("gpt", typed(&json!({"contents": [user_text("hi")]})))
        .await
        .expect("a stream");
    let mut items: Vec<Value> = stream
        .map(|item| item.map_or_else(|error| error.body(), |response| wire(&response)))
        .collect()
        .await;
    let ids: Vec<String> = items
        .iter()
        .filter_map(|item| item["responseId"].as_str().map(ToString::to_string))
        .collect();
    if let Some(first) = ids.first() {
        assert!(
            first.len() == 24 && ids.iter().all(|id| id == first),
            "one random responseId for the stream: {ids:?}"
        );
    }
    for item in &mut items {
        if item.get("responseId").is_some() {
            item["responseId"] = json!("<responseId>");
        }
    }
    items
}

#[tokio::test]
async fn a_text_stream_becomes_a_response_per_text_chunk_and_a_last_one_with_the_usage() {
    let items = streamed(vec![
        Ok(chunk(&json!({"index": 0, "delta": {"role": "assistant", "content": ""}, "finish_reason": null}))),
        Ok(text_chunk("Hello")),
        Ok(text_chunk(", world.")),
        Ok(finish_chunk("stop")),
        Ok(usage_chunk()),
    ])
    .await;
    insta::assert_snapshot!(
        "text_stream_as_gemini_responses",
        pretty(&Value::Array(items))
    );
}

/// Gemini streams a function call whole: the argument deltas of each call, split across chunks
/// and interleaved between calls, are gathered and given in the last response.
#[tokio::test]
async fn a_tool_call_stream_gives_each_function_call_whole_at_the_end() {
    let items = streamed(vec![
        Ok(text_chunk("Reading both.")),
        Ok(tool_call_chunk(0, Some("call_1"), Some("read_file"), "")),
        Ok(tool_call_chunk(0, None, None, "{\"pa")),
        Ok(tool_call_chunk(
            1,
            Some("call_2"),
            Some("read_file"),
            "{\"path\":",
        )),
        Ok(tool_call_chunk(0, None, None, "th\":\"a.rs\"}")),
        Ok(tool_call_chunk(1, None, None, "\"b.rs\"}")),
        Ok(finish_chunk("tool_calls")),
        Ok(usage_chunk()),
    ])
    .await;
    insta::assert_snapshot!(
        "tool_call_stream_as_gemini_responses",
        pretty(&Value::Array(items))
    );
}

#[tokio::test]
async fn a_chat_stream_error_ends_the_stream() {
    let items = streamed(vec![
        Ok(text_chunk("Partial")),
        Err(OpenAIError::ApiError(ApiError {
            message: "Rate limit reached".to_string(),
            r#type: Some("requests".to_string()),
            param: None,
            code: Some("rate_limit_exceeded".to_string()),
        })),
        Ok(text_chunk("never sent")),
        Ok(finish_chunk("stop")),
    ])
    .await;
    assert_eq!(
        items,
        [
            json!({"candidates": [{"content": {"role": "model", "parts": [{"text": "Partial"}]}, "index": 0}], "modelVersion": "gpt", "responseId": "<responseId>"}),
            json!({"error": {"code": 429, "message": "Rate limit reached", "status": "RESOURCE_EXHAUSTED"}}),
        ]
    );
}

/// A chunk stream that ends before the completion finishes is an answer cut short: it ends with
/// an error, never a finish reason, which would present the partial answer as whole.
#[tokio::test]
async fn a_chat_stream_that_ends_unfinished_ends_with_an_error() {
    let incomplete = json!({"error": {"code": 500, "message": "Model 'gpt' ended its stream before finishing the answer, so the answer is incomplete.", "status": "INTERNAL"}});
    assert_eq!(
        streamed(vec![Ok(text_chunk("Partial"))]).await,
        [
            json!({"candidates": [{"content": {"role": "model", "parts": [{"text": "Partial"}]}, "index": 0}], "modelVersion": "gpt", "responseId": "<responseId>"}),
            incomplete.clone(),
        ]
    );
    assert_eq!(streamed(vec![]).await, [incomplete]);
}

#[tokio::test]
async fn a_streamed_function_call_that_is_not_whole_ends_with_an_error_or_is_cut_off() {
    let invalid = streamed(vec![
        Ok(tool_call_chunk(
            0,
            Some("call_1"),
            Some("read_file"),
            "{\"path\":",
        )),
        Ok(finish_chunk("tool_calls")),
    ])
    .await;
    assert_eq!(
        invalid,
        [
            json!({"error": {"code": 500, "message": "Model 'gpt' called the function 'read_file' with arguments that are not a JSON object, which the Gemini API cannot return: {\"path\":", "status": "INTERNAL"}})
        ]
    );
    let nameless = streamed(vec![
        Ok(tool_call_chunk(0, Some("call_1"), None, "{}")),
        Ok(finish_chunk("tool_calls")),
    ])
    .await;
    assert_eq!(
        nameless,
        [
            json!({"error": {"code": 500, "message": "Model 'gpt' streamed a function call without the function's name.", "status": "INTERNAL"}})
        ]
    );
    let cut_off = streamed(vec![
        Ok(tool_call_chunk(
            0,
            Some("call_1"),
            Some("read_file"),
            "{\"path\":\"a.rs\"}",
        )),
        Ok(tool_call_chunk(
            1,
            Some("call_2"),
            Some("write_file"),
            "{\"path\":",
        )),
        Ok(finish_chunk("length")),
    ])
    .await;
    assert_eq!(
        cut_off,
        [json!({
            "candidates": [{"content": {"role": "model", "parts": [
                {"functionCall": {"id": "call_1", "name": "read_file", "args": {"path": "a.rs"}}},
            ]}, "finishReason": "MAX_TOKENS", "index": 0}],
            "modelVersion": "gpt",
            "responseId": "<responseId>",
        })]
    );
}

/// The responses of a stream, joined as a Gemini client joins them: text parts in order, and
/// the other parts as they come; the finish reason and usage of the last.
fn joined(responses: &[GenerateContentResponse]) -> Value {
    let mut text = String::new();
    let mut parts = Vec::new();
    let mut last = Value::Null;
    for response in responses {
        let value = wire(response);
        for part in value["candidates"][0]["content"]["parts"]
            .as_array()
            .into_iter()
            .flatten()
        {
            match part["text"].as_str() {
                Some(piece) => text.push_str(piece),
                None => parts.push(part.clone()),
            }
        }
        last = value;
    }
    if !text.is_empty() {
        parts.insert(0, json!({"text": text}));
    }
    json!({
        "parts": parts,
        "finishReason": last["candidates"][0]["finishReason"],
        "usageMetadata": last["usageMetadata"],
    })
}

/// The same completion, unstreamed and streamed, is the same answer.
#[tokio::test]
async fn a_streamed_and_an_unstreamed_answer_carry_the_same_content() {
    let chat = ScriptedChat::new(
        Some(Ok(completion(
            &json!({"role": "assistant", "content": "Reading it.", "tool_calls": [
                {"id": "call_1", "type": "function", "function": {"name": "read_file", "arguments": "{\"path\":\"a.rs\"}"}},
                {"id": "call_2", "type": "function", "function": {"name": "list_files", "arguments": ""}},
            ]}),
            Some("tool_calls"),
            &full_usage(),
        ))),
        vec![
            Ok(text_chunk("Reading ")),
            Ok(text_chunk("it.")),
            Ok(tool_call_chunk(
                0,
                Some("call_1"),
                Some("read_file"),
                "{\"path\":",
            )),
            Ok(tool_call_chunk(1, Some("call_2"), Some("list_files"), "")),
            Ok(tool_call_chunk(0, None, None, "\"a.rs\"}")),
            Ok(finish_chunk("tool_calls")),
            Ok(usage_chunk()),
        ],
    );
    let gemini = GeminiChat::new(Arc::clone(&chat) as Arc<dyn Chat>);
    let request = typed(&json!({"contents": [user_text("Read a.rs")]}));
    let unstreamed = gemini
        .generate("gpt", request.clone())
        .await
        .expect("an answer");
    let streamed: Vec<GenerateContentResponse> = gemini
        .generate_stream("gpt", request)
        .await
        .expect("a stream")
        .map(|item| item.expect("a response"))
        .collect()
        .await;
    assert_eq!(streamed.len(), 3, "two text responses and the last");
    assert_eq!(joined(&streamed), joined(&[unstreamed]));
    assert_eq!(
        joined(&streamed)["parts"],
        json!([
            {"text": "Reading it."},
            {"functionCall": {"id": "call_1", "name": "read_file", "args": {"path": "a.rs"}}},
            {"functionCall": {"id": "call_2", "name": "list_files", "args": {}}},
        ])
    );
}

/// The function calls of an answer, sent back in the history with responses that name the
/// function only, reach the chat model as the calls the model made, with their IDs.
#[tokio::test]
async fn function_calls_round_trip_through_the_history() {
    let tool_calls = json!([
        {"id": "call_a", "type": "function", "function": {"name": "read_file", "arguments": "{\"path\":\"a.rs\"}"}},
        {"id": "call_b", "type": "function", "function": {"name": "read_file", "arguments": "{\"path\":\"b.rs\"}"}},
    ]);
    let chat = ScriptedChat::new(
        Some(Ok(completion(
            &json!({"role": "assistant", "content": null, "tool_calls": tool_calls}),
            Some("tool_calls"),
            &Value::Null,
        ))),
        Vec::new(),
    );
    let answer = GeminiChat::new(chat)
        .generate("gpt", typed(&json!({"contents": [user_text("Read both")]})))
        .await
        .expect("an answer");
    let turn = wire(&answer)["candidates"][0]["content"].clone();
    let history = json!({"contents": [
        user_text("Read both"),
        turn,
        {"role": "user", "parts": [
            {"functionResponse": {"name": "read_file", "response": {"text": "a"}}},
            {"functionResponse": {"name": "read_file", "response": {"text": "b"}}},
        ]},
    ]});
    let messages = translated(&history).expect("a translatable request")["messages"].clone();
    assert_eq!(
        messages,
        json!([
            {"role": "user", "content": "Read both"},
            {"role": "assistant", "tool_calls": tool_calls},
            {"role": "tool", "tool_call_id": "call_a", "content": "{\"text\":\"a\"}"},
            {"role": "tool", "tool_call_id": "call_b", "content": "{\"text\":\"b\"}"},
        ])
    );
}

/// The model is named by the URL, `models/{model}`, and a stream asks for its usage.
#[tokio::test]
async fn the_model_comes_from_the_url_and_a_stream_asks_for_usage() {
    let chat = ScriptedChat::new(
        Some(Ok(completion(
            &json!({"role": "assistant", "content": "pong"}),
            Some("stop"),
            &Value::Null,
        ))),
        vec![Ok(text_chunk("pong")), Ok(finish_chunk("stop"))],
    );
    let gemini = GeminiChat::new(Arc::clone(&chat) as Arc<dyn Chat>);
    let request = typed(&json!({"contents": [user_text("ping")]}));
    let answer = gemini
        .generate("models/qwen", request.clone())
        .await
        .expect("an answer");
    assert_eq!(answer.model_version.as_deref(), Some("qwen"));
    let streamed: Vec<_> = gemini
        .generate_stream("qwen", request)
        .await
        .expect("a stream")
        .collect()
        .await;
    assert_eq!(streamed.len(), 2);
    assert_eq!(
        chat.requests(),
        [
            json!({"model": "qwen", "messages": [{"role": "user", "content": "ping"}]}),
            json!({"model": "qwen", "messages": [{"role": "user", "content": "ping"}], "stream": true, "stream_options": {"include_usage": true}}),
        ]
    );
}

#[tokio::test]
async fn a_chat_model_failure_is_answered_with_an_error_of_its_kind() {
    let chat = ScriptedChat::new(
        Some(Err(OpenAIError::ApiError(ApiError {
            message: "Incorrect API key provided.".to_string(),
            r#type: Some("invalid_request_error".to_string()),
            param: None,
            code: Some("invalid_api_key".to_string()),
        }))),
        Vec::new(),
    );
    let error = GeminiChat::new(chat)
        .generate("gpt", typed(&json!({"contents": [user_text("hi")]})))
        .await
        .expect_err("a failure");
    assert_eq!(
        error.body(),
        json!({"error": {"code": 401, "message": "Incorrect API key provided.", "status": "UNAUTHENTICATED"}})
    );
}

#[tokio::test]
async fn counting_tokens_is_refused() {
    let chat = ScriptedChat::new(None, Vec::new());
    let error = GeminiChat::new(Arc::clone(&chat) as Arc<dyn Chat>)
        .count_tokens("models/qwen", CountTokensRequest::default())
        .await
        .expect_err("no tokenizer");
    assert_eq!(
        error,
        GeminiError::invalid_argument(
            "Model 'qwen' serves the Gemini API by translating requests into chat completions, which cannot count tokens: counts are specific to the model's tokenizer. The `usageMetadata` of a `generateContent` response counts its prompt. See: https://spiceai.org/docs/components/models"
        )
    );
    assert_eq!(chat.requests(), Vec::<Value>::new());
}

#[test]
fn a_model_is_listed_by_its_resource_name() {
    let expected = json!({"name": "models/qwen", "displayName": "qwen", "supportedGenerationMethods": ["generateContent"]});
    assert_eq!(wire(&model_listing("qwen")), expected);
    assert_eq!(wire(&model_listing("models/qwen")), expected);
}

/// A chunk with neither text nor a choice adds no response; the usage it carries reaches the
/// last.
#[test]
fn a_chunk_without_text_adds_no_response() {
    let mut translator = Translator::new("gpt".to_string(), "r1".to_string());
    assert_eq!(translator.chunk(usage_chunk()), None);
    assert_eq!(
        translator.chunk(tool_call_chunk(0, Some("call_1"), Some("get_time"), "")),
        None
    );
    assert_eq!(translator.chunk(finish_chunk("tool_calls")), None);
    assert_eq!(
        wire(&translator.end().expect("a last response")),
        json!({
            "candidates": [{"content": {"role": "model", "parts": [{"functionCall": {"id": "call_1", "name": "get_time", "args": {}}}]}, "finishReason": "STOP", "index": 0}],
            "usageMetadata": {"promptTokenCount": 120, "cachedContentTokenCount": 100, "candidatesTokenCount": 18, "thoughtsTokenCount": 12, "totalTokenCount": 150},
            "modelVersion": "gpt",
            "responseId": "r1",
        })
    );
}
