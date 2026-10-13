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

#![allow(clippy::implicit_hasher)]
use async_openai::{
    error::OpenAIError,
    types::chat::{
        ChatCompletionRequestMessage, ChatCompletionRequestSystemMessageArgs,
        ChatCompletionResponseStream, ChatCompletionStreamOptions, ChatCompletionToolChoiceOption,
        CreateChatCompletionRequest, CreateChatCompletionResponse,
        CreateChatCompletionStreamResponse,
    },
};
use async_trait::async_trait;
use futures::Stream;
use futures::TryStreamExt;
use llms::{
    accumulate::{empty_completion_response, fold_completion_stream},
    chat::{Chat, Result as ChatResult, nsql::SqlGeneration},
};
use opentelemetry::KeyValue;
use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use tera::Tera;
use tokio::time::Instant;
use tracing_futures::Instrument;

use crate::model::metrics::{handle_metrics, handle_token_metrics};

use std::sync::{Arc, LazyLock, Mutex};
use std::task::{Context, Poll};

use super::metrics::request_labels;

mod ast;
pub mod responses;

pub(crate) static OPENAI_DEFAULT_PARAM_KEYS: LazyLock<HashSet<&'static str>> =
    LazyLock::new(|| {
        HashSet::from([
            "frequency_penalty",
            "logit_bias",
            "logprobs",
            "top_logprobs",
            "max_completion_tokens",
            "reasoning_effort",
            "store",
            "metadata",
            "n",
            "presence_penalty",
            "response_format",
            "seed",
            "stop",
            "stream",
            "stream_options",
            "temperature",
            "top_p",
            "tool_choice",
            "parallel_tool_calls",
            "prompt_cache_key",
            "user",
        ])
    });

/// Wraps [`Chat`] models with additional handling specifically for the spice runtime (e.g. telemetry, injecting system prompts).
pub struct ChatWrapper {
    pub public_name: String,
    pub chat: Arc<dyn Chat>,
    pub system_prompt: Option<String>,

    /// If true, the system prompt will be treated as a template and will be parameterized with the input prompt.
    pub attempt_to_template_system_prompt: bool,
    pub defaults: Vec<(String, serde_json::Value)>,
}

/// Sets a field of a [`CreateChatCompletionRequest`] to the model's default for it when
/// the request leaves it unset.
macro_rules! set_default_w_warning {
    ($req:expr, $field:ident, $value:expr, $model:expr) => {
        $req.$field = $req
            .$field
            .or_else(|| parse_default(stringify!($field), $value, &$model))
    };
}

/// `value` as the model's default for `field`, or `None`, with a warning, when it is not
/// a valid value for that field.
fn parse_default<T: serde::de::DeserializeOwned>(
    field: &str,
    value: &serde_json::Value,
    model: &str,
) -> Option<T> {
    if let Ok(parsed) = T::deserialize(value) {
        Some(parsed)
    } else {
        tracing::warn!(
            "Failed to parse `{field}` model parameter override for model='{model}'. Ensure {value:?} is of the correct format."
        );
        None
    }
}

impl ChatWrapper {
    pub fn new(
        chat: Arc<dyn Chat>,
        public_name: &str,
        system_prompt: Option<&str>,
        defaults: Vec<(String, serde_json::Value)>,
    ) -> Self {
        let s = Self {
            public_name: public_name.to_string(),
            chat,
            system_prompt: system_prompt.map(ToString::to_string),
            defaults,
            attempt_to_template_system_prompt: false,
        };

        // Check defaults provided are valid at startup.
        // `with_model_defaults` will emit appropriate warnings to user.
        s.with_model_defaults(CreateChatCompletionRequest::default());

        s
    }

    /// If it is allowed to parameterised, check if there is a system prompt, and the system prompt is a template.
    /// If its not a template, or there is no system prompt, no reason to attempt templating on each [`ChatWrapper::chat_request`] call.
    pub fn allowed_to_parameterise(mut self) -> Self {
        if self
            .system_prompt
            .as_ref()
            .is_some_and(|p| system_prompt_is_template_with_variables(p.as_str()))
        {
            self.attempt_to_template_system_prompt = true;
        }
        self
    }

    fn prepare_req(
        &self,
        req: CreateChatCompletionRequest,
    ) -> Result<CreateChatCompletionRequest, OpenAIError> {
        let mut prepared_req = self.with_system_prompt(req)?;

        prepared_req = self.with_model_defaults(prepared_req);
        prepared_req = Self::with_stream_usage(prepared_req);
        Ok(prepared_req)
    }

    /// Injects a system prompt as the first message in the request, if it exists.
    fn with_system_prompt(
        &self,
        mut req: CreateChatCompletionRequest,
    ) -> Result<CreateChatCompletionRequest, OpenAIError> {
        let prompt_opt = match (
            self.system_prompt.as_ref(),
            self.attempt_to_template_system_prompt,
        ) {
            // Template existing system prompt
            (Some(prompt), true) => {
                let ctx = match req
                    .metadata
                    .as_ref()
                    .and_then(|m| serde_json::to_value(m).ok())
                {
                    Some(serde_json::Value::Object(m)) => m
                        .into_iter()
                        .collect::<HashMap<String, serde_json::Value>>(),
                    Some(_) | None => HashMap::new(),
                };

                // If request `store` is not set, remove metadata.
                if !req.store.is_some_and(|s| s) {
                    req.metadata = None;
                }

                match template_system_prompt(prompt.as_str(), &ctx) {
                    Ok(templated_prompt) => Some(templated_prompt),
                    Err(e) => {
                        tracing::warn!(
                            "Failed to template system prompt for model='{}': {}. Using system_prompt as is.",
                            self.public_name,
                            e
                        );
                        Some(prompt.clone())
                    }
                }
            }
            // Don't template, just use system prompt as is.
            (Some(prompt), false) => Some(prompt.clone()),
            _ => None,
        };
        if let Some(prompt) = prompt_opt {
            let system_message = ChatCompletionRequestSystemMessageArgs::default()
                .content(prompt)
                .build()?;
            req.messages
                .insert(0, ChatCompletionRequestMessage::System(system_message));
        }
        Ok(req)
    }

    /// Ensure that streaming requests have `stream_options: {"include_usage": true}` internally.
    fn with_stream_usage(mut req: CreateChatCompletionRequest) -> CreateChatCompletionRequest {
        if req.stream.is_some_and(|s| s) {
            req.stream_options = match req.stream_options {
                Some(mut opts) => {
                    opts.include_usage = Some(true);
                    Some(opts)
                }
                None => Some(ChatCompletionStreamOptions {
                    include_obfuscation: None,
                    include_usage: Some(true),
                }),
            };
        }
        req
    }

    /// For [`None`] valued fields in a [`CreateChatCompletionRequest`], if the chat model has non-`None` defaults, use those instead.
    #[expect(deprecated)] // seed and user fields are deprecated in async-openai
    fn with_model_defaults(
        &self,
        mut req: CreateChatCompletionRequest,
    ) -> CreateChatCompletionRequest {
        let offers_tools = req.tools.as_ref().is_some_and(|tools| !tools.is_empty());
        for (key, value) in &self.defaults {
            match key.as_str() {
                // These defaults are only checked, so a malformed one is still reported at
                // startup. Whether to stream is decided by the call, `chat_request` or
                // `chat_stream`, and `chat_request` fails on a request marked as streaming.
                // `stream_options` is only valid when `stream` is true; applying it after
                // the `stream` default is ignored would send a non-streaming request that
                // providers reject. A tool choice or parallel-call setting means nothing
                // to a request that offers no tools, and providers reject one sent alone.
                "stream" => {
                    parse_default::<bool>("stream", value, &self.public_name);
                }
                "stream_options" if !req.stream.is_some_and(|s| s) => {
                    parse_default::<ChatCompletionStreamOptions>(
                        "stream_options",
                        value,
                        &self.public_name,
                    );
                }
                "tool_choice" if !offers_tools => {
                    parse_default::<ChatCompletionToolChoiceOption>(
                        "tool_choice",
                        value,
                        &self.public_name,
                    );
                }
                "parallel_tool_calls" if !offers_tools => {
                    parse_default::<bool>("parallel_tool_calls", value, &self.public_name);
                }
                "frequency_penalty" => {
                    set_default_w_warning!(req, frequency_penalty, value, self.public_name);
                }
                "logit_bias" => set_default_w_warning!(req, logit_bias, value, self.public_name),
                "logprobs" => set_default_w_warning!(req, logprobs, value, self.public_name),
                "top_logprobs" => {
                    set_default_w_warning!(req, top_logprobs, value, self.public_name);
                }
                "max_completion_tokens" => {
                    set_default_w_warning!(req, max_completion_tokens, value, self.public_name);
                }
                "reasoning_effort" => {
                    set_default_w_warning!(req, reasoning_effort, value, self.public_name);
                }
                "store" => set_default_w_warning!(req, store, value, self.public_name),
                "metadata" => set_default_w_warning!(req, metadata, value, self.public_name),
                "n" => set_default_w_warning!(req, n, value, self.public_name),
                "presence_penalty" => {
                    set_default_w_warning!(req, presence_penalty, value, self.public_name);
                }
                "response_format" => {
                    set_default_w_warning!(req, response_format, value, self.public_name);
                }
                "seed" => set_default_w_warning!(req, seed, value, self.public_name),
                "stop" => set_default_w_warning!(req, stop, value, self.public_name),
                "stream_options" => {
                    set_default_w_warning!(req, stream_options, value, self.public_name);
                }
                "temperature" => set_default_w_warning!(req, temperature, value, self.public_name),
                "top_p" => set_default_w_warning!(req, top_p, value, self.public_name),
                "tool_choice" => set_default_w_warning!(req, tool_choice, value, self.public_name),
                "parallel_tool_calls" => {
                    set_default_w_warning!(req, parallel_tool_calls, value, self.public_name);
                }
                "prompt_cache_key" => {
                    set_default_w_warning!(req, prompt_cache_key, value, self.public_name);
                }
                "user" => set_default_w_warning!(req, user, value, self.public_name),
                _ => {
                    tracing::debug!("Ignoring unknown default key: {}", key);
                }
            }
        }
        req
    }
}

#[deny(clippy::missing_trait_methods)]
#[async_trait]
impl Chat for ChatWrapper {
    /// Expect `captured_output` to be instrumented by the underlying chat model (to not reopen/parse streams). i.e.
    /// ```rust
    /// tracing::info!(target: "task_history", captured_output = %chat_output)
    /// ```
    async fn chat_stream(
        &self,
        req: CreateChatCompletionRequest,
    ) -> Result<ChatCompletionResponseStream, OpenAIError> {
        let start = Instant::now();
        let req = self.prepare_req(req)?;
        let span = tracing::span!(target: "task_history", tracing::Level::INFO, "ai_completion", stream=true, model = %req.model, input = %serde_json::to_string(&req).unwrap_or_default());

        if let Some(metadata) = &req.metadata {
            tracing::info!(target: "task_history", metadata = ?metadata);
        }

        let labels = request_labels(&req);
        match self.chat.chat_stream(req).instrument(span.clone()).await {
            Ok(resp) => {
                let public_name = self.public_name.clone();
                let logged_stream = resp.map_ok(move |mut r| {
                    r.model.clone_from(&public_name);
                    r
                });

                // Wrap the stream with our custom aggregator that logs when dropped.
                Ok(Box::pin(TracedChatCompletionStream::new(
                    logged_stream,
                    span.clone(),
                    self.public_name.clone(),
                    labels,
                )))
            }
            Err(e) => {
                tracing::error!(target: "task_history", parent: &span, "Failed to run chat model: {}", e);
                handle_metrics(start.elapsed(), true, &labels);
                Err(e)
            }
        }
    }

    async fn health(&self) -> ChatResult<()> {
        self.chat.health().await
    }

    /// Unlike [`ChatWrapper::chat_stream`], this method will instrument the `captured_output` for the model output.
    async fn chat_request(
        &self,
        req: CreateChatCompletionRequest,
    ) -> Result<CreateChatCompletionResponse, OpenAIError> {
        let start = Instant::now();

        let req = self.prepare_req(req)?;
        let span = tracing::span!(target: "task_history", tracing::Level::INFO, "ai_completion", stream=false, model = %req.model, input = %serde_json::to_string(&req).unwrap_or_default());

        let labels = request_labels(&req);
        if let Some(metadata) = &req.metadata {
            tracing::info!(target: "task_history", parent: &span, metadata = ?metadata, "labels");
        }

        let result = match self.chat.chat_request(req).instrument(span.clone()).await {
            Ok(mut resp) => {
                if let Some(usage) = resp.usage.clone() {
                    tracing::info!(target: "task_history", parent: &span, completion_tokens = %usage.completion_tokens, total_tokens = %usage.total_tokens, prompt_tokens = %usage.prompt_tokens, id=resp.id, "labels");
                    handle_token_metrics(usage.prompt_tokens, usage.completion_tokens, &labels);
                }
                let captured_output: Vec<_> = resp.choices.iter().map(|c| &c.message).collect();
                match serde_json::to_string(&captured_output) {
                    Ok(output) => {
                        tracing::info!(target: "task_history", parent: &span, captured_output = %output);
                    }
                    Err(e) => tracing::error!("Failed to serialize truncated output: {e}"),
                }
                resp.model.clone_from(&self.public_name);
                Ok(resp)
            }
            Err(e) => {
                tracing::error!(target: "task_history", parent: &span, "Failed to run chat model: {}", e);
                Err(e)
            }
        };
        handle_metrics(start.elapsed(), result.is_err(), &labels);
        result
    }

    async fn run(&self, prompt: String) -> ChatResult<Option<String>> {
        self.chat.run(prompt).await
    }

    async fn stream<'a>(
        &self,
        prompt: String,
    ) -> ChatResult<Pin<Box<dyn Stream<Item = ChatResult<Option<String>>> + Send>>> {
        self.chat.stream(prompt).await
    }

    fn as_sql(&self) -> Option<&dyn SqlGeneration> {
        self.chat.as_sql()
    }
}

/// [`TracedChatCompletionStream`] wraps a [`ChatCompletionResponseStream`]-like stream and provides metrics and `task_history` tracing. Importantly, when aggregrating the output, it does not need to block until the full stream is consumed.
struct TracedChatCompletionStream<S> {
    inner: S,
    accumulated_response: Arc<Mutex<CreateChatCompletionResponse>>,
    span: tracing::Span,
    model_public_name: String,
    started: Instant,
    labels: Vec<KeyValue>,
}

impl<S> TracedChatCompletionStream<S>
where
    S: Stream<Item = Result<CreateChatCompletionStreamResponse, OpenAIError>> + Unpin,
{
    pub fn new(
        inner: S,
        span: tracing::Span,
        model_public_name: String,
        labels: Vec<KeyValue>,
    ) -> Self {
        Self {
            inner,
            accumulated_response: Arc::new(Mutex::new(empty_completion_response())),
            span,
            model_public_name,
            started: Instant::now(),
            labels,
        }
    }
}

impl<S> Stream for TracedChatCompletionStream<S>
where
    S: Stream<Item = Result<CreateChatCompletionStreamResponse, OpenAIError>> + Unpin,
{
    type Item = Result<CreateChatCompletionStreamResponse, OpenAIError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(item))) => {
                // Aggregate the response.
                if let Ok(mut acc) = self.accumulated_response.lock() {
                    fold_completion_stream(&mut acc, &item);
                }

                // Log usage info if available.
                if let Some(usage) = item.usage.clone() {
                    tracing::info!(
                        target: "task_history",
                        completion_tokens = %usage.completion_tokens,
                        total_tokens = %usage.total_tokens,
                        prompt_tokens = %usage.prompt_tokens,
                        "Usage info"
                    );

                    // Usage should be on last message, so we can add latency metrics here.
                    handle_metrics(self.started.elapsed(), false, &self.labels);
                    handle_token_metrics(
                        usage.prompt_tokens,
                        usage.completion_tokens,
                        &self.labels,
                    );
                }
                Poll::Ready(Some(Ok(item)))
            }
            Poll::Ready(Some(Err(e))) => {
                handle_metrics(self.started.elapsed(), true, &self.labels);
                Poll::Ready(Some(Err(e)))
            }
            other => other,
        }
    }
}

impl<S> Drop for TracedChatCompletionStream<S> {
    fn drop(&mut self) {
        if let Ok(output) = self.accumulated_response.lock() {
            let _guard = self.span.enter();
            if let Ok(resp_str) = serde_json::to_string(&*output) {
                tracing::info!(target: "task_history", captured_output = %*resp_str);
            }
        } else {
            tracing::warn!(
                "Failed to write output of ai_completion for '{}' model",
                self.model_public_name
            );
        }
    }
}

fn template_system_prompt(
    prompt: &str,
    inputs: &HashMap<String, serde_json::Value>,
) -> tera::Result<String> {
    let mut t = Tera::default();
    t.add_raw_template("system_prompt", prompt)?;
    let mut context = tera::Context::new();
    for (k, v) in inputs {
        context.insert(k, v);
    }
    t.render("system_prompt", &context)
}

/// Return true if the system prompt is a template that would have its variables replaced with the input values.
fn system_prompt_is_template_with_variables(prompt: &str) -> bool {
    let mut t = Tera::default();
    if t.add_raw_template("system_prompt", prompt).is_err() {
        return false;
    }

    let Ok(tt) = t.get_template("system_prompt") else {
        return false;
    };
    ast::has_variables_in_ast(&tt.ast)
}

/// Guards the null-suppression patch the `spiceai/async-openai` fork carries:
/// `ChatCompletionStreamOptions` skips `include_usage` and `include_obfuscation`
/// when they are `None`.
///
/// [`ChatWrapper::with_stream_usage`] sets `include_usage` on every streaming
/// request and leaves `include_obfuscation` unset, so this shape is what Spice puts
/// on the wire for *all* streamed chat completions. Without the patch each of them
/// carries `"include_obfuscation": null`, and the `OpenAI`-compatible servers that
/// reject fields they do not know — NVIDIA NIM is the one the fork names — refuse
/// the request outright rather than degrading.
///
/// The fork's own `ser_de.rs` test leaves with the branch at the next re-cut, which
/// is why the guard lives here. `docs/dev/fork_patches.md` is the ledger it is named
/// in.
#[cfg(test)]
mod stream_options_null_suppression {
    use async_openai::types::chat::{ChatCompletionStreamOptions, CreateChatCompletionRequestArgs};
    use serde_json::json;

    use super::ChatWrapper;

    /// The request Spice actually streams: `with_stream_usage` fills in
    /// `include_usage` and nothing else, so the serialized options must carry that
    /// one field and no null beside it.
    #[test]
    fn a_streamed_request_carries_no_null_stream_option() {
        let request = CreateChatCompletionRequestArgs::default()
            .model("test-model")
            .messages(Vec::new())
            .stream(true)
            .build()
            .expect("a streaming request with no stream options");
        assert!(
            request.stream_options.is_none(),
            "the fixture has to start with no stream options, or it is not the shape `with_stream_usage` fills in"
        );

        let request = ChatWrapper::with_stream_usage(request);
        let options = request
            .stream_options
            .expect("a streaming request gets stream options");

        assert_eq!(
            serde_json::to_value(options).expect("stream options serialize"),
            json!({ "include_usage": true }),
            "a field left unset must be absent, not null: a server that rejects fields it does not know refuses the whole request"
        );
    }

    /// The same property at the type, so the guard still holds if the wrapper stops
    /// being the only place stream options are built.
    #[test]
    fn unset_stream_options_serialize_to_an_empty_object() {
        let options = ChatCompletionStreamOptions {
            include_usage: None,
            include_obfuscation: None,
        };
        assert_eq!(
            serde_json::to_string(&options).expect("stream options serialize"),
            "{}"
        );
    }
}

#[cfg(test)]
mod call_dependent_defaults {
    use std::sync::Arc;

    use async_openai::types::chat::{
        ChatCompletionToolChoiceOption, CreateChatCompletionRequest, ToolChoiceOptions,
    };
    use async_trait::async_trait;
    use llms::chat::{Chat, nsql::SqlGeneration};
    use serde_json::json;

    use super::ChatWrapper;

    struct NoModel;

    #[async_trait]
    impl Chat for NoModel {
        fn as_sql(&self) -> Option<&dyn SqlGeneration> {
            None
        }
    }

    fn wrapper() -> ChatWrapper {
        ChatWrapper::new(
            Arc::new(NoModel),
            "judge",
            None,
            vec![
                ("stream".to_string(), json!(true)),
                ("stream_options".to_string(), json!({"include_usage": true})),
                ("tool_choice".to_string(), json!("auto")),
                ("parallel_tool_calls".to_string(), json!(false)),
                ("temperature".to_string(), json!(0.5)),
            ],
        )
    }

    fn request(tools: &serde_json::Value) -> CreateChatCompletionRequest {
        serde_json::from_value(json!({"model": "judge", "messages": [], "tools": tools}))
            .expect("chat request")
    }

    /// A `stream` default would turn a non-streaming call into one `chat_request`
    /// refuses (`When stream is true, use Chat::create_stream`); the call decides.
    #[test]
    fn a_stream_default_is_never_applied() {
        let prepared = wrapper().with_model_defaults(request(&serde_json::Value::Null));

        assert_eq!(prepared.stream, None);
    }

    /// `stream_options` is only valid when `stream` is true. A companion default
    /// must not ride onto a non-streaming call after the `stream` default is ignored
    /// (evaluation and other `chat_request` paths).
    #[test]
    fn a_stream_options_default_is_not_applied_unless_streaming() {
        let prepared = wrapper().with_model_defaults(request(&serde_json::Value::Null));

        assert_eq!(prepared.stream, None);
        assert_eq!(
            prepared.stream_options, None,
            "stream_options on a non-streaming request is rejected by providers"
        );

        let streaming = serde_json::from_value(json!({
            "model": "judge",
            "messages": [],
            "stream": true
        }))
        .expect("streaming chat request");
        let prepared = wrapper().with_model_defaults(streaming);

        assert_eq!(prepared.stream, Some(true));
        assert_eq!(
            prepared.stream_options,
            Some(async_openai::types::chat::ChatCompletionStreamOptions {
                include_usage: Some(true),
                include_obfuscation: None,
            })
        );
    }

    /// A request with no tools, such as an evaluation's, must not be sent a tool choice
    /// on its own: providers reject it, and a `required` choice asks for a call that
    /// cannot be made.
    #[test]
    fn a_request_without_tools_gets_no_tool_defaults() {
        let prepared = wrapper().with_model_defaults(request(&serde_json::Value::Null));

        assert_eq!(prepared.tool_choice, None);
        assert_eq!(prepared.parallel_tool_calls, None);
        assert_eq!(
            prepared.temperature,
            Some(0.5),
            "other defaults still apply"
        );
    }

    #[test]
    fn a_request_with_tools_gets_the_tool_defaults() {
        let prepared = wrapper().with_model_defaults(request(&json!([{
            "type": "function",
            "function": {"name": "sql", "parameters": {"type": "object"}}
        }])));

        assert_eq!(
            prepared.tool_choice,
            Some(ChatCompletionToolChoiceOption::Mode(
                ToolChoiceOptions::Auto
            ))
        );
        assert_eq!(prepared.parallel_tool_calls, Some(false));
    }
}
