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

//! The models that serve the Anthropic Messages API natively, and what Spice adds to each
//! request they serve: the Spicepod's system prompt and request defaults, its own tools, the
//! model's Spicepod name in place of the provider's id, and telemetry.
//!
//! A model without a native Messages API serves it through its chat completions, which add the
//! same things on their own path (see `http::v1::messages`).

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use async_trait::async_trait;
use futures::Stream;
use llms::chat::Error as LlmError;
use messages_api::stream::event_type;
use messages_api::{MessageStream, Messages, MessagesError, MessagesRequest};
use opentelemetry::KeyValue;
use secrecy::SecretString;
use serde_json::{Map, Value, json};
use spicepod::component::model::{Model, ModelSource};
use tokio::time::Instant;
use tracing_futures::Instrument;

use crate::Runtime;
use crate::model::LoadedChatModel;
use crate::model::chat::{anthropic_client, typed_params};
use crate::model::metrics::{handle_metrics, handle_token_metrics, request_labels_messages};
use crate::model::params::anthropic::AnthropicModelParams;
use crate::model::recorded_calls;
use crate::model::tool_use_messages::ToolUsingMessages;
use messages_chat::ChatMessages;
use model_capabilities::MessagesBackend;

/// The Messages API handlers that are built when a model loads, by model name: a native
/// model's, and a chat model's that runs Spice's tools. Any other chat model serves the API
/// through a handler made for each request from its chat model.
pub type LLMMessagesModelStore = HashMap<String, Arc<dyn Messages>>;

/// The Messages API handler of `component`, a model loaded as `chat`, when it needs one built at
/// load: a native handler for a model whose provider has the API (`backend` is `Native`), with
/// Spice's tools when the model's `tools` param enables them; and, for a model served through
/// chat, a handler that runs Spice's tools at the Messages API's level, so that their calls
/// reach the client as they do from a native model. `None` for a chat model without tools.
///
/// # Errors
///
/// Returns an error when the model's parameters are invalid, or `backend` is `Native` for a
/// provider without a native Messages API.
#[expect(clippy::implicit_hasher)]
pub async fn try_to_messages_model(
    component: &Model,
    params: &HashMap<String, SecretString>,
    rt: Arc<Runtime>,
    chat: &LoadedChatModel,
    backend: MessagesBackend,
) -> Result<Option<Arc<dyn Messages>>, LlmError> {
    let (tools, recursion_limit) = chat.tools();
    let messages: Arc<dyn Messages> = match backend {
        MessagesBackend::Native => native(component, params, &rt).await?,
        MessagesBackend::ViaChat if tools.is_empty() => return Ok(None),
        MessagesBackend::ViaChat => Arc::new(ChatMessages::new(chat.without_tools())),
    };
    if tools.is_empty() {
        return Ok(Some(messages));
    }
    Ok(Some(Arc::new(ToolUsingMessages::new(
        messages,
        tools.to_vec(),
        recursion_limit,
    ))))
}

/// The native Messages API of `component`, with what Spice adds to each request.
async fn native(
    component: &Model,
    params: &HashMap<String, SecretString>,
    rt: &Arc<Runtime>,
) -> Result<Arc<dyn Messages>, LlmError> {
    let source = component.get_source().ok_or(LlmError::UnknownModelSource {
        from: component.from.clone(),
    })?;
    let (native, provider_model): (Arc<dyn Messages>, String) = match source {
        ModelSource::Anthropic => {
            let p = typed_params::<AnthropicModelParams>(
                component,
                params,
                source.clone(),
                &rt.secrets(),
            )
            .await?;
            let native = anthropic_client(component.get_model_id().as_deref(), &p)?.messages();
            let provider_model = native.model().to_string();
            (Arc::new(native), provider_model)
        }
        other => {
            return Err(LlmError::FailedToLoadModel {
                source: format!(
                    "Model '{}' uses provider '{other}', which has no native Messages API.",
                    component.name
                )
                .into(),
            });
        }
    };

    let system_prompt = match component.params.get("system_prompt") {
        Some(Value::String(s)) => Some(s.clone()),
        Some(v) => {
            return Err(LlmError::InvalidParamValueError {
                param: "system_prompt".to_string(),
                message: format!("Expected a string, got: {v:?}"),
            });
        }
        None => None,
    };
    Ok(Arc::new(MessagesWrapper {
        public_name: component.name.clone(),
        messages: native,
        system_prompt,
        defaults: crate::model::chat::get_openai_request_overrides(component, source.short_name()),
        provider: Provider {
            name: "Anthropic",
            prefix: "anthropic",
            model: provider_model,
            docs: "https://spiceai.org/docs/components/models/anthropic",
        },
    }))
}

/// Wraps a native Messages API model with what Spice adds to each request: the Spicepod's
/// system prompt and request defaults, the model's Spicepod name in place of the provider's id,
/// and telemetry.
pub struct MessagesWrapper {
    pub public_name: String,
    pub messages: Arc<dyn Messages>,
    pub system_prompt: Option<String>,
    /// The model's request defaults, by their chat completion name.
    pub defaults: Vec<(String, Value)>,
    pub provider: Provider,
}

/// The provider that serves a model's Messages API natively, as its errors name it.
pub struct Provider {
    /// Its name, such as `Anthropic`.
    pub name: &'static str,
    /// The prefix of its `from` and of its `params`, such as `anthropic`.
    pub prefix: &'static str,
    /// Its id for the model.
    pub model: String,
    pub docs: &'static str,
}

/// The sampling controls a model can refuse outright, which a Spicepod default can set.
const SAMPLING_CONTROLS: [&str; 2] = ["temperature", "top_p"];

impl MessagesWrapper {
    /// `request` with the system prompt and the request defaults, and the sampling controls the
    /// defaults set.
    fn prepare(&self, mut request: MessagesRequest) -> (MessagesRequest, Vec<&'static str>) {
        if let Some(prompt) = &self.system_prompt {
            with_system_prompt(&mut request.body, prompt);
        }
        let is_set = |body: &Map<String, Value>, control: &str| {
            body.get(control).is_some_and(|value| !value.is_null())
        };
        let set_by_request = SAMPLING_CONTROLS.map(|control| is_set(&request.body, control));
        with_model_defaults(&mut request.body, &self.defaults, &self.public_name);
        let defaulted = SAMPLING_CONTROLS
            .into_iter()
            .zip(set_by_request)
            .filter(|(control, set_by_request)| !set_by_request && is_set(&request.body, control))
            .map(|(control, _)| control)
            .collect();
        (request, defaulted)
    }

    /// `error` with a message that names where a sampling control the model refused came from,
    /// when it came from the model's `params`, not the request: the provider's own says only that
    /// the control "is deprecated for this model", about a field the client never sent.
    fn explain_rejected_default(
        &self,
        mut error: MessagesError,
        defaulted: &[&str],
    ) -> MessagesError {
        if error.error_type != messages_api::ErrorType::InvalidRequest {
            return error;
        }
        let Some(control) = defaulted
            .iter()
            .find(|control| error.message == format!("`{control}` is deprecated for this model."))
        else {
            return error;
        };
        let Provider {
            name,
            prefix,
            model,
            docs,
        } = &self.provider;
        error.message = format!(
            "Failed to answer a Messages API request with {name} model '{model}': the model does not accept the `{control}` parameter, which model '{public_name}' sets in its `params` (as `{control}`, `{prefix}_{control}` or `openai_{control}`) for every request that leaves it unset. Remove `{control}` from the model's `params`, or set `from: {prefix}:<model_id>` to a model that accepts it. Cause: {cause} See: {docs}",
            public_name = self.public_name,
            cause = error.message,
        );
        error
    }
}

/// Puts `prompt` before the request's own system prompt: as a first text block when it is
/// blocks, so that the client's cache breakpoints stay where it put them.
fn with_system_prompt(body: &mut Map<String, Value>, prompt: &str) {
    let system = match body.remove("system") {
        None | Some(Value::Null) => Value::String(prompt.to_string()),
        Some(Value::String(existing)) => Value::String(format!("{prompt}\n\n{existing}")),
        Some(Value::Array(mut blocks)) => {
            blocks.insert(0, json!({"type": "text", "text": prompt}));
            Value::Array(blocks)
        }
        Some(other) => other,
    };
    body.insert("system".to_string(), system);
}

/// Sets each field the request leaves unset to the model's default for it. The defaults are
/// named as chat completions name them, which is how a Spicepod sets them, and carried over to
/// the Messages API field that means the same; a default with no such field is left out.
fn with_model_defaults(body: &mut Map<String, Value>, defaults: &[(String, Value)], model: &str) {
    let offers_tools = body
        .get("tools")
        .and_then(Value::as_array)
        .is_some_and(|tools| !tools.is_empty());
    // `parallel_tool_calls` sets a field of `tool_choice`, and creates it when it is missing, so it
    // is applied after a `tool_choice` default: the defaults come in no fixed order.
    let (parallel, others): (Vec<_>, Vec<_>) = defaults
        .iter()
        .partition(|(key, _)| key == "parallel_tool_calls");
    for (key, value) in others.into_iter().chain(parallel) {
        let applied = match key.as_str() {
            "temperature" | "top_p" => set_default(body, key, value.as_f64().map(Value::from)),
            "max_completion_tokens" => {
                set_default(body, "max_tokens", value.as_u64().map(Value::from))
            }
            "stop" => set_default(
                body,
                "stop_sequences",
                match value {
                    Value::String(stop) => Some(json!([stop])),
                    Value::Array(stops) => Some(Value::Array(stops.clone())),
                    _ => None,
                },
            ),
            "user" => {
                let has_user = body
                    .get("metadata")
                    .and_then(|metadata| metadata.get("user_id"))
                    .is_some();
                match (value.as_str(), has_user) {
                    (Some(user), false) => {
                        let metadata = body
                            .entry("metadata")
                            .or_insert_with(|| Value::Object(Map::new()));
                        if let Some(metadata) = metadata.as_object_mut() {
                            metadata.insert("user_id".to_string(), Value::from(user));
                        }
                        Some(true)
                    }
                    (Some(_), true) => Some(false),
                    (None, _) => None,
                }
            }
            "reasoning_effort" => {
                let effort = value
                    .as_str()
                    .filter(|effort| matches!(*effort, "low" | "medium" | "high" | "xhigh"));
                set_output_config(body, "effort", effort.map(Value::from))
            }
            "response_format" => {
                let schema = (value.get("type").and_then(Value::as_str) == Some("json_schema"))
                    .then(|| value.pointer("/json_schema/schema").cloned())
                    .flatten();
                set_output_config(
                    body,
                    "format",
                    schema.map(|schema| json!({"type": "json_schema", "schema": schema})),
                )
            }
            "tool_choice" if offers_tools => {
                let choice = match value {
                    Value::String(mode) => match mode.as_str() {
                        "auto" => Some(json!({"type": "auto"})),
                        "none" => Some(json!({"type": "none"})),
                        "required" => Some(json!({"type": "any"})),
                        _ => None,
                    },
                    named => named
                        .pointer("/function/name")
                        .and_then(Value::as_str)
                        .map(|name| json!({"type": "tool", "name": name})),
                };
                set_default(body, "tool_choice", choice)
            }
            "parallel_tool_calls" if offers_tools => match value.as_bool() {
                Some(false) => {
                    let choice = body
                        .entry("tool_choice")
                        .or_insert_with(|| json!({"type": "auto"}));
                    match choice.as_object_mut() {
                        Some(choice) if !choice.contains_key("disable_parallel_tool_use") => {
                            choice
                                .insert("disable_parallel_tool_use".to_string(), Value::Bool(true));
                            Some(true)
                        }
                        _ => Some(false),
                    }
                }
                Some(true) => Some(false),
                None => None,
            },
            _ => {
                tracing::debug!(
                    "Model '{model}' has a default for `{key}`, which the Messages API has no field for, so requests to it on /v1/messages are sent without it."
                );
                Some(false)
            }
        };
        if applied.is_none() {
            tracing::warn!(
                "Failed to apply the `{key}` default of model '{model}' to a Messages API request, because {value} is not a valid `{key}`. The request is sent without it. Set a valid value for `{key}` in the model's `params`. See: https://spiceai.org/docs/components/models"
            );
        }
    }
}

/// Sets `field` to `value` when the request leaves it unset. `None` when `value` is `None`: the
/// default is not valid. `Some(applied)` otherwise.
fn set_default(body: &mut Map<String, Value>, field: &str, value: Option<Value>) -> Option<bool> {
    let value = value?;
    if body.get(field).is_some_and(|existing| !existing.is_null()) {
        return Some(false);
    }
    body.insert(field.to_string(), value);
    Some(true)
}

fn set_output_config(
    body: &mut Map<String, Value>,
    field: &str,
    value: Option<Value>,
) -> Option<bool> {
    let value = value?;
    let config = body
        .entry("output_config")
        .or_insert_with(|| Value::Object(Map::new()));
    let Some(config) = config.as_object_mut() else {
        return Some(false);
    };
    Some(set_default(config, field, Some(value)).unwrap_or(false))
}

/// The text of a Message's text blocks, as `task_history` captures a model's output.
fn message_text(message: &Value) -> String {
    message
        .get("content")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter(|block| block.get("type").and_then(Value::as_str) == Some("text"))
        .filter_map(|block| block.get("text").and_then(Value::as_str))
        .collect()
}

/// The Messages API usage fields, as token counts.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct TokenUsage {
    input: u64,
    output: u64,
    cache_read: u64,
    cache_creation: u64,
}

impl TokenUsage {
    /// Overlays the fields `usage` reports. A stream reports its input in `message_start`, and
    /// its output, and in later API versions everything again, in `message_delta`.
    fn update(&mut self, usage: &Value) {
        let field = |name: &str| usage.get(name).and_then(Value::as_u64);
        if let Some(input) = field("input_tokens") {
            self.input = input;
        }
        if let Some(output) = field("output_tokens") {
            self.output = output;
        }
        if let Some(cache_read) = field("cache_read_input_tokens") {
            self.cache_read = cache_read;
        }
        if let Some(cache_creation) = field("cache_creation_input_tokens") {
            self.cache_creation = cache_creation;
        }
    }

    /// All the input tokens, whether read from the cache, written to it, or neither.
    fn prompt(self) -> u64 {
        self.input + self.cache_read + self.cache_creation
    }

    fn record(self, span: &tracing::Span, labels: &[KeyValue]) {
        tracing::info!(
            target: "task_history",
            parent: span,
            completion_tokens = %self.output,
            total_tokens = %(self.prompt() + self.output),
            prompt_tokens = %self.prompt(),
            cached_tokens = %self.cache_read,
            "labels"
        );
        handle_token_metrics(
            u32::try_from(self.prompt()).unwrap_or(u32::MAX),
            u32::try_from(self.output).unwrap_or(u32::MAX),
            labels,
        );
    }
}

#[async_trait]
impl Messages for MessagesWrapper {
    async fn create(&self, request: MessagesRequest) -> Result<Value, MessagesError> {
        let start = Instant::now();
        let (request, defaulted) = self.prepare(request);
        let labels = request_labels_messages(&self.public_name, &request.body, false);
        let input = recorded_calls::recorded(
            &Value::Object(request.body.clone()),
            recorded_calls::current_tools().as_ref(),
        )
        .unwrap_or_default();
        let span = tracing::span!(target: "task_history", tracing::Level::INFO, "ai_completion", stream = false, model = %self.public_name, input = %input);
        let result = self.messages.create(request).instrument(span.clone()).await;
        handle_metrics(start.elapsed(), result.is_err(), &labels);
        match result {
            Ok(mut message) => {
                tracing::info!(target: "task_history", parent: &span, captured_output = %message_text(&message));
                if let Some(id) = message.get("id").and_then(Value::as_str) {
                    tracing::info!(target: "task_history", parent: &span, id = %id, "labels");
                }
                if let Some(usage) = message.get("usage") {
                    let mut tokens = TokenUsage::default();
                    tokens.update(usage);
                    tokens.record(&span, &labels);
                }
                if let Some(object) = message.as_object_mut() {
                    object.insert("model".to_string(), Value::String(self.public_name.clone()));
                }
                Ok(message)
            }
            Err(e) => {
                let e = self.explain_rejected_default(e, &defaulted);
                tracing::error!(target: "task_history", parent: &span, "{e}");
                Err(e)
            }
        }
    }

    async fn create_stream(
        &self,
        request: MessagesRequest,
    ) -> Result<MessageStream, MessagesError> {
        let start = Instant::now();
        let (request, defaulted) = self.prepare(request);
        let labels = request_labels_messages(&self.public_name, &request.body, true);
        let input = recorded_calls::recorded(
            &Value::Object(request.body.clone()),
            recorded_calls::current_tools().as_ref(),
        )
        .unwrap_or_default();
        let span = tracing::span!(target: "task_history", tracing::Level::INFO, "ai_completion", stream = true, model = %self.public_name, input = %input);
        match self
            .messages
            .create_stream(request)
            .instrument(span.clone())
            .await
        {
            Ok(events) => Ok(Box::pin(TracedMessageStream {
                events,
                span,
                public_name: self.public_name.clone(),
                started: start,
                labels,
                output: String::new(),
                usage: TokenUsage::default(),
                ended: false,
            })),
            Err(e) => {
                let e = self.explain_rejected_default(e, &defaulted);
                tracing::error!(target: "task_history", parent: &span, "{e}");
                handle_metrics(start.elapsed(), true, &labels);
                Err(e)
            }
        }
    }

    async fn count_tokens(&self, mut request: MessagesRequest) -> Result<Value, MessagesError> {
        // Counted as the model would be sent the request, the system prompt included. The
        // request defaults are left out: they set sampling and output fields, which a token count
        // takes none of.
        if let Some(prompt) = &self.system_prompt {
            with_system_prompt(&mut request.body, prompt);
        }
        self.messages.count_tokens(request).await
    }
}

/// A stream of a native model's events, with the model's Spicepod name in `message_start`, and
/// its output, usage and outcome recorded when it ends.
struct TracedMessageStream {
    events: MessageStream,
    span: tracing::Span,
    public_name: String,
    started: Instant,
    labels: Vec<KeyValue>,
    output: String,
    usage: TokenUsage,
    ended: bool,
}

impl TracedMessageStream {
    fn observe(&mut self, event: &mut Value) {
        match event_type(event) {
            Some("message_start") => {
                if let Some(message) = event.get_mut("message") {
                    if let Some(usage) = message.get("usage") {
                        self.usage.update(usage);
                    }
                    if let Some(id) = message.get("id").and_then(Value::as_str) {
                        tracing::info!(target: "task_history", parent: &self.span, id = %id, "labels");
                    }
                    if let Some(message) = message.as_object_mut() {
                        message
                            .insert("model".to_string(), Value::String(self.public_name.clone()));
                    }
                }
            }
            Some("content_block_delta") => {
                if let Some(text) = event.pointer("/delta/text").and_then(Value::as_str)
                    && event.pointer("/delta/type").and_then(Value::as_str) == Some("text_delta")
                {
                    self.output.push_str(text);
                }
            }
            Some("message_delta") => {
                if let Some(usage) = event.get("usage") {
                    self.usage.update(usage);
                }
            }
            Some("message_stop") => {
                self.ended = true;
                handle_metrics(self.started.elapsed(), false, &self.labels);
                self.usage.record(&self.span, &self.labels);
            }
            _ => {}
        }
    }
}

impl Stream for TracedMessageStream {
    type Item = Result<Value, MessagesError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.events.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(mut event))) => {
                self.observe(&mut event);
                Poll::Ready(Some(Ok(event)))
            }
            Poll::Ready(Some(Err(e))) => {
                if !self.ended {
                    self.ended = true;
                    tracing::error!(target: "task_history", parent: &self.span, "{e}");
                    handle_metrics(self.started.elapsed(), true, &self.labels);
                }
                Poll::Ready(Some(Err(e)))
            }
            other => other,
        }
    }
}

impl Drop for TracedMessageStream {
    fn drop(&mut self) {
        tracing::info!(target: "task_history", parent: &self.span, captured_output = %self.output);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn body(value: &Value) -> Map<String, Value> {
        value.as_object().cloned().expect("an object")
    }

    #[test]
    fn the_spicepod_system_prompt_comes_first() {
        let cases = [
            (json!({}), json!("Spice.")),
            (json!({"system": "Client."}), json!("Spice.\n\nClient.")),
            (
                json!({"system": [{"type": "text", "text": "Client.", "cache_control": {"type": "ephemeral"}}]}),
                json!([
                    {"type": "text", "text": "Spice."},
                    {"type": "text", "text": "Client.", "cache_control": {"type": "ephemeral"}},
                ]),
            ),
        ];
        for (request, system) in cases {
            let mut request = body(&request);
            with_system_prompt(&mut request, "Spice.");
            assert_eq!(request["system"], system);
        }
    }

    #[test]
    fn chat_completion_defaults_carry_over_to_their_messages_fields() {
        let defaults: Vec<(String, Value)> = [
            ("temperature", json!(0.2)),
            ("max_completion_tokens", json!(512)),
            ("stop", json!("END")),
            ("user", json!("svc")),
            ("reasoning_effort", json!("high")),
            ("response_format", json!({"type": "json_schema", "json_schema": {"name": "x", "schema": {"type": "object"}}})),
            ("tool_choice", json!("required")),
            ("parallel_tool_calls", json!(false)),
            ("presence_penalty", json!(0.5)),
        ]
        .into_iter()
        .map(|(key, value)| (key.to_string(), value))
        .collect();

        let mut request =
            body(&json!({"messages": [], "tools": [{"name": "t", "input_schema": {}}]}));
        with_model_defaults(&mut request, &defaults, "claude");
        assert_eq!(
            Value::Object(request),
            json!({
                "messages": [],
                "tools": [{"name": "t", "input_schema": {}}],
                "temperature": 0.2,
                "max_tokens": 512,
                "stop_sequences": ["END"],
                "metadata": {"user_id": "svc"},
                "output_config": {"effort": "high", "format": {"type": "json_schema", "schema": {"type": "object"}}},
                "tool_choice": {"type": "any", "disable_parallel_tool_use": true},
            })
        );

        // What the request sets is kept, and tool defaults need tools.
        let mut request = body(&json!({
            "messages": [],
            "temperature": 1.0,
            "max_tokens": 64,
            "metadata": {"user_id": "client"},
            "output_config": {"effort": "low"},
        }));
        with_model_defaults(&mut request, &defaults, "claude");
        assert_eq!(
            Value::Object(request),
            json!({
                "messages": [],
                "temperature": 1.0,
                "max_tokens": 64,
                "metadata": {"user_id": "client"},
                "output_config": {"effort": "low", "format": {"type": "json_schema", "schema": {"type": "object"}}},
                "stop_sequences": ["END"],
            })
        );
    }

    /// A Spicepod's defaults come in no fixed order: the tool choice it sets is kept whichever
    /// of `tool_choice` and `parallel_tool_calls` is applied first.
    #[test]
    fn a_tool_choice_default_is_kept_whatever_the_order_of_the_defaults() {
        let tool_choice = ("tool_choice".to_string(), json!("required"));
        let parallel = ("parallel_tool_calls".to_string(), json!(false));
        for defaults in [
            vec![tool_choice.clone(), parallel.clone()],
            vec![parallel, tool_choice],
        ] {
            let mut request =
                body(&json!({"messages": [], "tools": [{"name": "t", "input_schema": {}}]}));
            with_model_defaults(&mut request, &defaults, "claude");
            assert_eq!(
                request["tool_choice"],
                json!({"type": "any", "disable_parallel_tool_use": true}),
                "{defaults:?}"
            );
        }
    }

    #[test]
    fn stream_usage_is_overlaid_and_input_counts_every_kind() {
        let mut usage = TokenUsage::default();
        usage.update(&json!({"input_tokens": 10, "cache_read_input_tokens": 100, "cache_creation_input_tokens": 5, "output_tokens": 1}));
        usage.update(&json!({"output_tokens": 42}));
        assert_eq!(
            (usage, usage.prompt()),
            (
                TokenUsage {
                    input: 10,
                    output: 42,
                    cache_read: 100,
                    cache_creation: 5
                },
                115
            )
        );
    }
}
