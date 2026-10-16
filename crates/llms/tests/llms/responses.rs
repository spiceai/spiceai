/*
Copyright 2026 The Spice.ai OSS Authors

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

//! The chat tests of [`super`], for the Responses API (`/v1/responses`): each provider that
//! implements [`Responses`], streamed and not.
//!
//! Each test makes its own model rather than taking one from a cache. A cached model's HTTP
//! client can outlive the runtime of the test that made it (#13575), and these models are cheap
//! to make: no hosted provider downloads anything.

use std::sync::{Arc, LazyLock};

use async_openai::types::responses::{
    CreateResponse, FunctionToolCall, OutputItem, Response, ResponseStream, ResponseStreamEvent,
    Status,
};
use futures::StreamExt;
use llms::responses::Responses;
use rstest::rstest;
use serde_json::{Value, json};

use super::{DOTENV, OPENAI_MODEL, WeatherArgs, XAI_MODEL, create};
use crate::{TEST_ARGS, init_tracing};

/// The model that tests select by `model_name`.
fn create_model(model_name: &str) -> Result<Arc<dyn Responses>, anyhow::Error> {
    match model_name {
        "openai" => Ok(create::create_openai_responses(OPENAI_MODEL)),
        "xai" => create::create_xai_responses(XAI_MODEL),
        _ => Err(anyhow::anyhow!("no Responses model is named {model_name}")),
    }
}

/// Sends `req`, the JSON of a [`CreateResponse`], to the model `model_name` and returns the
/// completed response, or `None` if the model is skipped.
///
/// Fails the test if the request fails or the response does not complete.
async fn run_test(
    model_name: &str,
    test_name: &str,
    req: Value,
    as_stream: bool,
) -> Option<Response> {
    LazyLock::force(&DOTENV);
    // Hold the guard for the body: `set_default` installs the subscriber only for the
    // guard's lifetime, so dropping it here would discard every event this test logs.
    let _tracing_guard = init_tracing(None);

    if TEST_ARGS.skip_model(model_name) {
        tracing::debug!("Skipping test {test_name}/{model_name}");
        return None;
    }

    let label = format!("{test_name}/{model_name} (stream: {as_stream})");
    let model =
        create_model(model_name).unwrap_or_else(|e| panic!("{label}: failed to create model: {e}"));
    let mut req: CreateResponse =
        serde_json::from_value(req).unwrap_or_else(|e| panic!("{label}: invalid request: {e}"));
    tracing::info!("Running test {label} with {req:?}");

    let response = if as_stream {
        req.stream = Some(true);
        let stream = model
            .responses_stream(req)
            .await
            .unwrap_or_else(|e| panic!("{label}: responses_stream failed. Error: {e:#?}"));
        completed_response(&label, stream).await
    } else {
        model
            .responses_request(req)
            .await
            .unwrap_or_else(|e| panic!("{label}: responses_request failed. Error: {e:#?}"))
    };
    tracing::debug!("Response for {label}: {response:?}");

    assert_eq!(response.status, Status::Completed, "{label}: {response:#?}");
    Some(response)
}

/// The response that `stream` completes with.
///
/// Also checks that the text deltas of the stream add up to the text of the completed response,
/// so that a stream that sends its text only in the last event does not pass as a stream.
async fn completed_response(label: &str, mut stream: ResponseStream) -> Response {
    let mut streamed_text = String::new();
    while let Some(event) = stream.next().await {
        let event = event.unwrap_or_else(|e| {
            panic!("{label}: the stream failed before the response completed: {e:#?}")
        });
        match event {
            ResponseStreamEvent::ResponseOutputTextDelta(delta) => {
                streamed_text.push_str(&delta.delta);
            }
            ResponseStreamEvent::ResponseCompleted(completed) => {
                assert_eq!(
                    completed.response.output_text().unwrap_or_default(),
                    streamed_text,
                    "{label}: the text deltas do not add up to the completed response's text"
                );
                return completed.response;
            }
            ResponseStreamEvent::ResponseIncomplete(event) => {
                panic!("{label}: the response did not complete: {event:#?}")
            }
            ResponseStreamEvent::ResponseFailed(event) => {
                panic!("{label}: the response failed: {event:#?}")
            }
            _ => {}
        }
    }
    panic!("{label}: the stream ended without a `response.completed` event")
}

/// The text of `response`, which must have some.
fn output_text(label: &str, response: &Response) -> String {
    response
        .output_text()
        .unwrap_or_else(|| panic!("{label}: the response has no text: {response:#?}"))
}

#[rstest]
#[tokio::test]
async fn test_responses_basic(
    #[values("openai", "xai")] model_name: &str,
    #[values(false, true)] as_stream: bool,
) {
    let req = json!({ "input": [{ "role": "user", "content": "Say Hello" }] });

    let Some(response) = run_test(model_name, "responses_basic", req, as_stream).await else {
        return;
    };

    let label = format!("responses_basic/{model_name}");
    let text = output_text(&label, &response);
    assert!(text.to_lowercase().contains("hello"), "{label}: {text:?}");
}

#[rstest]
#[tokio::test]
async fn test_responses_usage(
    #[values("openai", "xai")] model_name: &str,
    #[values(false, true)] as_stream: bool,
) {
    let req = json!({ "input": [{ "role": "user", "content": "Say Hello" }] });

    let Some(response) = run_test(model_name, "responses_usage", req, as_stream).await else {
        return;
    };

    let usage = response
        .usage
        .unwrap_or_else(|| panic!("responses_usage/{model_name}: the response has no usage"));
    assert!(
        usage.input_tokens > 0,
        "responses_usage/{model_name}: {usage:?}"
    );
    assert!(
        usage.output_tokens > 0,
        "responses_usage/{model_name}: {usage:?}"
    );
    assert!(
        usage.total_tokens >= usage.input_tokens,
        "responses_usage/{model_name}: {usage:?}"
    );
    assert!(
        usage.total_tokens >= usage.output_tokens,
        "responses_usage/{model_name}: {usage:?}"
    );
}

/// Where a request gives the model its system prompt.
#[derive(Debug, Clone, Copy)]
enum SystemPrompt {
    /// The request's `instructions`.
    Instructions,
    /// An input message with the `system` role.
    SystemMessage,
    /// An input message with the `developer` role.
    DeveloperMessage,
}

impl SystemPrompt {
    /// A request with the system prompt `system`, then the user message `user`.
    fn request(self, system: &str, user: &str) -> Value {
        let user = json!({ "role": "user", "content": user });
        let mut req = match self {
            Self::Instructions => json!({ "instructions": system, "input": [user] }),
            Self::SystemMessage => {
                json!({ "input": [{ "role": "system", "content": system }, user] })
            }
            Self::DeveloperMessage => {
                json!({ "input": [{ "role": "developer", "content": system }, user] })
            }
        };
        // A limit that a reasoning model does not use up before it answers.
        req["max_output_tokens"] = json!(1024);
        req
    }
}

#[rstest]
#[tokio::test]
async fn test_responses_system_prompt(
    #[values("openai", "xai")] model_name: &str,
    #[values(false, true)] as_stream: bool,
    #[values(
        SystemPrompt::Instructions,
        SystemPrompt::SystemMessage,
        SystemPrompt::DeveloperMessage
    )]
    system_prompt: SystemPrompt,
) {
    let req = system_prompt.request("Quote back the exact message from the user", "pong");

    let Some(response) = run_test(model_name, "responses_system_prompt", req, as_stream).await
    else {
        return;
    };

    let label = format!("responses_system_prompt/{model_name} ({system_prompt:?})");
    let text = output_text(&label, &response);
    assert!(text.to_lowercase().contains("pong"), "{label}: {text:?}");
}

#[rstest]
#[tokio::test]
async fn test_responses_supports_basic_message_roles(
    #[values("openai", "xai")] model_name: &str,
    #[values(false, true)] as_stream: bool,
) {
    let req = json!({
        "input": [
            { "role": "system", "content": "Quote back the exact message from the user" },
            { "role": "user", "content": "call a tool" },
            { "role": "assistant", "content": "Sorry I, can't call a tool. " },
            { "role": "user", "content": "That's fine. Tell me a joke." }
        ]
    });

    let Some(response) = run_test(
        model_name,
        "responses_supports_basic_message_roles",
        req,
        as_stream,
    )
    .await
    else {
        return;
    };

    output_text(
        &format!("responses_supports_basic_message_roles/{model_name}"),
        &response,
    );
}

/// A model must accept a conversation that holds a function call and its result: the input of
/// every round after the first in a tool-use loop.
#[rstest]
#[tokio::test]
async fn test_responses_supports_function_call_items(
    #[values("openai", "xai")] model_name: &str,
    #[values(false, true)] as_stream: bool,
) {
    let req = json!({
        "input": [
            { "role": "system", "content": "Quote back the exact message from the user" },
            { "role": "user", "content": "call a tool" },
            {
                "type": "function_call",
                "call_id": "call_1",
                "name": "get_current_weather",
                "arguments": "{\"location\": \"San Francisco, CA\"}"
            },
            { "type": "function_call_output", "call_id": "call_1", "output": "72" }
        ],
        "tools": [{
            "type": "function",
            "name": "get_current_weather",
            "parameters": {
                "type": "object",
                "properties": { "location": { "type": "string" } },
                "required": ["location"],
                "additionalProperties": false
            }
        }]
    });

    run_test(
        model_name,
        "responses_supports_function_call_items",
        req,
        as_stream,
    )
    .await;
}

#[rstest]
#[tokio::test]
async fn test_responses_tool_use(
    #[values("openai", "xai")] model_name: &str,
    #[values(false, true)] as_stream: bool,
) {
    let req = json!({
        "input": [{ "role": "user", "content": "What's the weather like in Boston today?" }],
        "tool_choice": { "type": "function", "name": "get_current_weather" },
        "tools": [{
            "type": "function",
            "name": "get_current_weather",
            "description": "Get the current weather in a given location, in Celsius",
            "parameters": {
                "type": "object",
                "properties": {
                    "location": {
                        "type": "string",
                        "description": "The city and state, e.g. San Francisco."
                    },
                    "unit": { "type": "string", "enum": ["celsius", "fahrenheit"] }
                },
                "required": ["location", "unit"],
                "additionalProperties": false
            }
        }]
    });

    let Some(response) = run_test(model_name, "responses_tool_use", req, as_stream).await else {
        return;
    };

    let call: &FunctionToolCall = response
        .output
        .iter()
        .find_map(|item| match item {
            OutputItem::FunctionCall(call) => Some(call),
            _ => None,
        })
        .unwrap_or_else(|| {
            panic!(
                "responses_tool_use/{model_name}: no function call: {:#?}",
                response.output
            )
        });
    assert_eq!(
        call.name, "get_current_weather",
        "responses_tool_use/{model_name}"
    );

    // The arguments are model-written, so check them against the tool's schema rather than one
    // model's wording: providers differ on `"Boston"` vs `"Boston, MA"` and on which unit to pick.
    let args: WeatherArgs = serde_json::from_str(&call.arguments).unwrap_or_else(|e| {
        panic!(
            "responses_tool_use/{model_name}: arguments {:?} do not match the get_current_weather schema: {e}",
            call.arguments
        )
    });
    assert!(
        args.location.to_lowercase().contains("boston"),
        "responses_tool_use/{model_name}: location {:?} does not name the city asked about",
        args.location
    );
}
