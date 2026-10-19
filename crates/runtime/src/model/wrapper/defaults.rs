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

//! The Spicepod's request defaults for a chat model, applied once to each client request.
//!
//! [`RequestDefaults`] sits above the Spice tool loop ([`super::super::tool_use::ToolUsingChat`]),
//! so the loop sees the request that the client and the Spicepod asked for together. Applied
//! below the loop instead, a default fills a field that the loop left unset again on every
//! round: a `tool_choice: required` default would force a tool call on each round, and a
//! `max_completion_tokens` default would give each round the whole budget again.

use std::pin::Pin;
use std::sync::Arc;

use async_openai::{
    error::OpenAIError,
    types::chat::{
        ChatCompletionResponseStream, ChatCompletionStreamOptions, ChatCompletionToolChoiceOption,
        CreateChatCompletionRequest, CreateChatCompletionResponse, ToolChoiceOptions,
    },
};
use async_trait::async_trait;
use futures::Stream;
use llms::chat::{Chat, Result as ChatResult, nsql::SqlGeneration};

/// The tools that the model below [`RequestDefaults`] sends the provider with a request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ToolOffer {
    /// Only the tools of the request itself.
    RequestOnly,
    /// The Spice runtime tools too, unless the request's `tool_choice` is `none`.
    WithRuntimeTools,
}

/// Fills each field that a chat request leaves unset with the Spicepod model's default for it.
pub struct RequestDefaults {
    chat: Arc<dyn Chat>,
    public_name: String,
    defaults: Vec<(String, serde_json::Value)>,
    tool_offer: ToolOffer,
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

impl RequestDefaults {
    #[must_use]
    pub fn new(
        chat: Arc<dyn Chat>,
        public_name: &str,
        defaults: Vec<(String, serde_json::Value)>,
        tool_offer: ToolOffer,
    ) -> Self {
        let s = Self {
            chat,
            public_name: public_name.to_string(),
            defaults,
            tool_offer,
        };

        // Check the defaults at startup: `with_model_defaults` warns about each malformed one.
        s.with_model_defaults(CreateChatCompletionRequest::default());

        s
    }

    /// The same defaults, for `chat`, which offers `tool_offer`.
    #[must_use]
    pub fn over(&self, chat: Arc<dyn Chat>, tool_offer: ToolOffer) -> Self {
        Self {
            chat,
            public_name: self.public_name.clone(),
            defaults: self.defaults.clone(),
            tool_offer,
        }
    }

    /// Whether the provider gets tools with `req`, and so can take a tool choice for it.
    fn offers_tools(&self, req: &CreateChatCompletionRequest) -> bool {
        let has_own_tools = req.tools.as_ref().is_some_and(|tools| !tools.is_empty());
        let adds_runtime_tools = self.tool_offer == ToolOffer::WithRuntimeTools
            && req.tool_choice
                != Some(ChatCompletionToolChoiceOption::Mode(
                    ToolChoiceOptions::None,
                ));
        has_own_tools || adds_runtime_tools
    }

    /// For [`None`] valued fields in a [`CreateChatCompletionRequest`], if the chat model has non-`None` defaults, use those instead.
    #[expect(deprecated)] // seed and user fields are deprecated in async-openai
    fn with_model_defaults(
        &self,
        mut req: CreateChatCompletionRequest,
    ) -> CreateChatCompletionRequest {
        // A `tool_choice` default of `none` keeps the runtime tools off the request, so it is
        // applied before the parallel-call default is checked against the tools offered.
        if self.offers_tools(&req)
            && let Some((_, value)) = self.defaults.iter().find(|(k, _)| k == "tool_choice")
        {
            set_default_w_warning!(req, tool_choice, value, self.public_name);
        }
        let offers_tools = self.offers_tools(&req);

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
                // Applied above.
                "tool_choice" => {}
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
impl Chat for RequestDefaults {
    async fn chat_stream(
        &self,
        req: CreateChatCompletionRequest,
    ) -> Result<ChatCompletionResponseStream, OpenAIError> {
        self.chat.chat_stream(self.with_model_defaults(req)).await
    }

    async fn chat_request(
        &self,
        req: CreateChatCompletionRequest,
    ) -> Result<CreateChatCompletionResponse, OpenAIError> {
        self.chat.chat_request(self.with_model_defaults(req)).await
    }

    async fn health(&self) -> ChatResult<()> {
        self.chat.health().await
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

#[cfg(test)]
mod call_dependent_defaults {
    use std::sync::Arc;

    use async_openai::types::chat::{
        ChatCompletionToolChoiceOption, CreateChatCompletionRequest, ToolChoiceOptions,
    };
    use async_trait::async_trait;
    use llms::chat::{Chat, nsql::SqlGeneration};
    use serde_json::json;

    use super::{RequestDefaults, ToolOffer};

    struct NoModel;

    #[async_trait]
    impl Chat for NoModel {
        fn as_sql(&self) -> Option<&dyn SqlGeneration> {
            None
        }
    }

    fn defaults(tool_offer: ToolOffer, tool_choice: serde_json::Value) -> RequestDefaults {
        RequestDefaults::new(
            Arc::new(NoModel),
            "judge",
            vec![
                ("stream".to_string(), json!(true)),
                ("stream_options".to_string(), json!({"include_usage": true})),
                ("tool_choice".to_string(), tool_choice),
                ("parallel_tool_calls".to_string(), json!(false)),
                ("temperature".to_string(), json!(0.5)),
            ],
            tool_offer,
        )
    }

    fn wrapper() -> RequestDefaults {
        defaults(ToolOffer::RequestOnly, json!("auto"))
    }

    fn request(tools: &serde_json::Value) -> CreateChatCompletionRequest {
        serde_json::from_value(json!({"model": "judge", "messages": [], "tools": tools}))
            .expect("chat request")
    }

    fn sql_tool() -> serde_json::Value {
        json!([{
            "type": "function",
            "function": {"name": "sql", "parameters": {"type": "object"}}
        }])
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
        let prepared = wrapper().with_model_defaults(request(&sql_tool()));

        assert_eq!(
            prepared.tool_choice,
            Some(ChatCompletionToolChoiceOption::Mode(
                ToolChoiceOptions::Auto
            ))
        );
        assert_eq!(prepared.parallel_tool_calls, Some(false));
    }

    /// Above the Spice tool loop, a request with no tools of its own still reaches the
    /// provider with the runtime tools, so it gets the tool defaults.
    #[test]
    fn a_request_that_gets_runtime_tools_gets_the_tool_defaults() {
        let prepared = defaults(ToolOffer::WithRuntimeTools, json!("required"))
            .with_model_defaults(request(&serde_json::Value::Null));

        assert_eq!(
            prepared.tool_choice,
            Some(ChatCompletionToolChoiceOption::Mode(
                ToolChoiceOptions::Required
            ))
        );
        assert_eq!(prepared.parallel_tool_calls, Some(false));
    }

    /// A `tool_choice: none`, from the client or as the default, keeps the runtime tools off
    /// the request, so a parallel-call default must not be sent alone.
    #[test]
    fn a_none_tool_choice_gets_no_parallel_call_default() {
        let prepared = defaults(ToolOffer::WithRuntimeTools, json!("none"))
            .with_model_defaults(request(&serde_json::Value::Null));

        assert_eq!(
            prepared.tool_choice,
            Some(ChatCompletionToolChoiceOption::Mode(
                ToolChoiceOptions::None
            ))
        );
        assert_eq!(prepared.parallel_tool_calls, None);

        let none_from_client = serde_json::from_value(json!({
            "model": "judge",
            "messages": [],
            "tool_choice": "none"
        }))
        .expect("chat request");
        let prepared = defaults(ToolOffer::WithRuntimeTools, json!("required"))
            .with_model_defaults(none_from_client);

        assert_eq!(
            prepared.tool_choice,
            Some(ChatCompletionToolChoiceOption::Mode(
                ToolChoiceOptions::None
            ))
        );
        assert_eq!(prepared.parallel_tool_calls, None);
    }
}
