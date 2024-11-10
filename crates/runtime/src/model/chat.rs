/*
Copyright 2024 The Spice.ai OSS Authors

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
use async_openai::{
    error::OpenAIError,
    types::{
        ChatCompletionRequestMessage, ChatCompletionRequestSystemMessageArgs,
        ChatCompletionResponseStream, ChatCompletionStreamOptions, CreateChatCompletionRequest,
        CreateChatCompletionResponse,
    },
};
use async_trait::async_trait;
use futures::stream::StreamExt;
use futures::Stream;
use llms::openai::DEFAULT_LLM_MODEL;
use llms::{
    anthropic::{Anthropic, AnthropicConfig},
    chat::{nsql::SqlGeneration, Chat, Error as LlmError, Result as ChatResult},
};
use secrecy::{ExposeSecret, Secret, SecretString};
use spicepod::component::model::{Model, ModelFileType, ModelSource};
use std::pin::Pin;
use std::{collections::HashMap, sync::Arc};
use tracing_futures::Instrument;

use super::tool_use::ToolUsingChat;
use crate::{
    tools::{options::SpiceToolsOptions, utils::get_tools},
    Runtime,
};

pub type LLMModelStore = HashMap<String, Box<dyn Chat>>;

/// Extract a secret from a hashmap of secrets, if it exists.
macro_rules! extract_secret {
    ($params:expr, $key:expr) => {
        $params.get($key).map(Secret::expose_secret).cloned()
    };
}

/// Attempt to derive a runnable Chat model from a given component from the Spicepod definition.
pub async fn try_to_chat_model<S: ::std::hash::BuildHasher>(
    component: &Model,
    params: &HashMap<String, SecretString, S>,
    rt: Arc<Runtime>,
) -> Result<Box<dyn Chat>, LlmError> {
    let model = construct_model(component, params)?;

    // Handle tool usage
    let spice_tool_opt: Option<SpiceToolsOptions> = extract_secret!(params, "spice_tools")
        .map(|x| x.parse())
        .transpose()
        .map_err(|_| LlmError::UnsupportedSpiceToolUseParameterError {})?;

    let spice_recursion_limit: Option<usize> = extract_secret!(params, "tool_recursion_limit")
        .map(|x| {
            x.parse().map_err(|e| LlmError::FailedToLoadModel {
                source: format!(
                    "Invalid value specified for `params.recursion_depth`: {x}. Error: {e}"
                )
                .into(),
            })
        })
        .transpose()?;

    let tool_model = match spice_tool_opt {
        Some(opts) if opts.can_use_tools() => Box::new(ToolUsingChat::new(
            Arc::new(model),
            Arc::clone(&rt),
            get_tools(Arc::clone(&rt), &opts).await,
            spice_recursion_limit,
        )),
        Some(_) | None => model,
    };
    Ok(tool_model)
}

pub fn construct_model<S: ::std::hash::BuildHasher>(
    component: &spicepod::component::model::Model,
    params: &HashMap<String, SecretString, S>,
) -> Result<Box<dyn Chat>, LlmError> {
    let model_id = component.get_model_id();
    let prefix = component.get_source().ok_or(LlmError::UnknownModelSource {
        source: format!(
            "Unknown model source for spicepod component from: {}",
            component.from.clone()
        )
        .into(),
    })?;
    let model = match prefix {
        ModelSource::HuggingFace => {
            let Some(id) = model_id else {
                return Err(LlmError::FailedToLoadModel {
                    source: "No model id for Huggingface model".to_string().into(),
                });
            };
            let model_type = extract_secret!(params, "model_type");
            let hf_token = params.get("hf_token");

            llms::chat::create_hf_model(&id, &model_type, hf_token)
        }
        ModelSource::File => {
            let weights_path = model_id
                .clone()
                .or(component.find_any_file_path(ModelFileType::Weights))
                .ok_or(LlmError::FailedToLoadModel {
                    source: "No 'weights_path' parameter provided".into(),
                })?
                .clone();
            let tokenizer_path = component.find_any_file_path(ModelFileType::Tokenizer);
            let tokenizer_config_path =
                component.find_any_file_path(ModelFileType::TokenizerConfig);
            let config_path = component.find_any_file_path(ModelFileType::Config);
            let chat_template_literal = params
                .get("chat_template")
                .map(|s| s.expose_secret().as_str());

            llms::chat::create_local_model(
                &weights_path,
                config_path.as_deref(),
                tokenizer_path.as_deref(),
                tokenizer_config_path.as_deref(),
                chat_template_literal,
            )
        }
        ModelSource::SpiceAI => Err(LlmError::UnsupportedTaskForModel {
            from: "spiceai".into(),
            task: "llm".into(),
        }),
        ModelSource::Anthropic => {
            let api_base = extract_secret!(params, "endpoint");
            let api_key = extract_secret!(params, "anthropic_api_key");
            let auth_token = extract_secret!(params, "anthropic_auth_token");

            if api_key.is_none() && auth_token.is_none() {
                return Err(LlmError::FailedToLoadModel {
                    source: "One of following `model.params` is required: `anthropic_api_key` or `anthropic_auth_token`.".into(),
                });
            }

            let cfg = AnthropicConfig::default()
                .with_api_key(api_key)
                .with_auth_token(auth_token)
                .with_base_url(api_base);

            let anthropic =
                Anthropic::new(cfg, model_id.as_deref(), &component.name).map_err(|_| {
                    LlmError::FailedToLoadModel {
                        source: format!("Unknown anthropic model: {:?}", model_id.clone()).into(),
                    }
                })?;

            Ok(Box::new(anthropic) as Box<dyn Chat>)
        }
        ModelSource::OpenAi => {
            let api_base = extract_secret!(params, "endpoint");
            let api_key = extract_secret!(params, "openai_api_key");
            let org_id = extract_secret!(params, "openai_org_id");
            let project_id = extract_secret!(params, "openai_project_id");

            Ok(Box::new(llms::openai::Openai::new(
                model_id.unwrap_or(DEFAULT_LLM_MODEL.to_string()),
                api_base,
                api_key,
                org_id,
                project_id,
            )) as Box<dyn Chat>)
        }
    }?;

    // Handle runtime wrapping
    let system_prompt = component
        .params
        .get("system_prompt")
        .cloned()
        .map(|s| s.to_string());
    let wrapper = ChatWrapper::new(
        model,
        system_prompt,
        component.get_openai_request_overrides(),
    );
    Ok(Box::new(wrapper))
}

/// Wraps [`Chat`] models with additional handling specifically for the spice runtime (e.g. telemetry, injecting system prompts).
pub struct ChatWrapper {
    pub chat: Box<dyn Chat>,
    pub system_prompt: Option<String>,
    pub defaults: Vec<(String, serde_json::Value)>,
}
impl ChatWrapper {
    pub fn new(
        chat: Box<dyn Chat>,
        system_prompt: Option<String>,
        defaults: Vec<(String, serde_json::Value)>,
    ) -> Self {
        Self {
            chat,
            system_prompt,
            defaults,
        }
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
        if let Some(prompt) = self.system_prompt.clone() {
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
                    opts.include_usage = true;
                    Some(opts)
                }
                None => Some(ChatCompletionStreamOptions {
                    include_usage: true,
                }),
            };
        }
        req
    }

    /// For [`None`] valued fields in a [`CreateChatCompletionRequest`], if the chat model has non-`None` defaults, use those instead.
    fn with_model_defaults(
        &self,
        mut req: CreateChatCompletionRequest,
    ) -> CreateChatCompletionRequest {
        for (key, v) in &self.defaults {
            let value = v.clone();
            match key.as_str() {
                "frequency_penalty" => {
                    req.frequency_penalty = req
                        .frequency_penalty
                        .or_else(|| serde_json::from_value(value).ok());
                }
                "logit_bias" => {
                    req.logit_bias = req
                        .logit_bias
                        .or_else(|| serde_json::from_value(value).ok());
                }
                "logprobs" => {
                    req.logprobs = req.logprobs.or_else(|| serde_json::from_value(value).ok());
                }
                "top_logprobs" => {
                    req.top_logprobs = req
                        .top_logprobs
                        .or_else(|| serde_json::from_value(value).ok());
                }
                "max_completion_tokens" => {
                    req.max_completion_tokens = req
                        .max_completion_tokens
                        .or_else(|| serde_json::from_value(value).ok());
                }
                "store" => {
                    req.store = req.store.or_else(|| serde_json::from_value(value).ok());
                }
                "metadata" => {
                    req.metadata = req.metadata.or_else(|| serde_json::from_value(value).ok());
                }
                "n" => req.n = req.n.or_else(|| serde_json::from_value(value).ok()),
                "presence_penalty" => {
                    req.presence_penalty = req
                        .presence_penalty
                        .or_else(|| serde_json::from_value(value).ok());
                }
                "response_format" => {
                    req.response_format = req
                        .response_format
                        .or_else(|| serde_json::from_value(value).ok());
                }
                "seed" => req.seed = req.seed.or_else(|| serde_json::from_value(value).ok()),
                "stop" => req.stop = req.stop.or_else(|| serde_json::from_value(value).ok()),
                "stream" => req.stream = req.stream.or_else(|| serde_json::from_value(value).ok()),
                "stream_options" => {
                    req.stream_options = req
                        .stream_options
                        .or_else(|| serde_json::from_value(value).ok());
                }
                "temperature" => {
                    req.temperature = req
                        .temperature
                        .or_else(|| serde_json::from_value(value).ok());
                }
                "top_p" => req.top_p = req.top_p.or_else(|| serde_json::from_value(value).ok()),
                "tools" => req.tools = req.tools.or_else(|| serde_json::from_value(value).ok()),
                "tool_choice" => {
                    req.tool_choice = req
                        .tool_choice
                        .or_else(|| serde_json::from_value(value).ok());
                }
                "parallel_tool_calls" => {
                    req.parallel_tool_calls = req
                        .parallel_tool_calls
                        .or_else(|| serde_json::from_value(value).ok());
                }
                "user" => req.user = req.user.or_else(|| serde_json::from_value(value).ok()),
                _ => {
                    tracing::debug!("Ignoring unknown default key: {}", key);
                }
            };
        }
        req
    }
}

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
        let span = tracing::span!(target: "task_history", tracing::Level::INFO, "ai_completion", stream=true, model = %req.model, input = %serde_json::to_string(&req).unwrap_or_default(), "labels");
        let req = self.prepare_req(req)?;

        if let Some(metadata) = &req.metadata {
            tracing::info!(target: "task_history", metadata = %metadata, "labels");
        }

        match self.chat.chat_stream(req).instrument(span.clone()).await {
            Ok(resp) => {
                let stream_span = span.clone();
                let logged_stream = resp.instrument(stream_span.clone()).inspect(move |item| {
                    if let Ok(item) = item {

                        // not incremental; provider only emits usage on last chunk.
                        if let Some(usage) = item.usage.clone() {
                            tracing::info!(target: "task_history", parent: &stream_span.clone(), completion_tokens = %usage.completion_tokens, total_tokens = %usage.total_tokens, prompt_tokens = %usage.prompt_tokens, "labels");
                        }
                    }
                });
                Ok(Box::pin(logged_stream))
            }
            Err(e) => {
                tracing::error!(target: "task_history", parent: &span, "Failed to run chat model: {}", e);
                Err(e)
            }
        }
    }

    /// Unlike [`ChatWrapper::chat_stream`], this method will instrument the `captured_output` for the model output.
    async fn chat_request(
        &self,
        req: CreateChatCompletionRequest,
    ) -> Result<CreateChatCompletionResponse, OpenAIError> {
        let span = tracing::span!(target: "task_history", tracing::Level::INFO, "ai_completion", stream=false, model = %req.model, input = %serde_json::to_string(&req).unwrap_or_default(), "labels");
        let req = self.prepare_req(req)?;

        if let Some(metadata) = &req.metadata {
            tracing::info!(target: "task_history", metadata = %metadata, "labels");
        }

        match self.chat.chat_request(req).instrument(span.clone()).await {
            Ok(resp) => {
                if let Some(usage) = resp.usage.clone() {
                    tracing::info!(target: "task_history", parent: &span, completion_tokens = %usage.completion_tokens, total_tokens = %usage.total_tokens, prompt_tokens = %usage.prompt_tokens, "labels");
                };
                let captured_output: Vec<_> = resp.choices.iter().map(|c| &c.message).collect();
                match serde_json::to_string(&captured_output) {
                    Ok(output) => {
                        tracing::info!(target: "task_history", captured_output = %output);
                    }
                    Err(e) => tracing::error!("Failed to serialize truncated output: {}", e),
                }
                Ok(resp)
            }
            Err(e) => {
                tracing::error!(target: "task_history", parent: &span, "Failed to run chat model: {}", e);
                Err(e)
            }
        }
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
