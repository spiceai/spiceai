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
        ChatCompletionResponseStream, CreateChatCompletionRequest, CreateChatCompletionResponse,
    },
};
use async_trait::async_trait;
use futures::Stream;
use llms::chat::{Chat, Error as LlmError, Result as ChatResult};
use llms::openai::DEFAULT_LLM_MODEL;
use secrecy::{ExposeSecret, Secret, SecretString};
use spicepod::component::model::{Model, ModelFileType, ModelSource};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing_futures::Instrument;

use super::tool_use::ToolUsingChat;
use crate::{
    tools::{get_tools, options::SpiceToolsOptions},
    Runtime,
};

pub type LLMModelStore = HashMap<String, RwLock<Box<dyn Chat>>>;

/// Attempt to derive a runnable Chat model from a given component from the Spicepod definition.
pub async fn try_to_chat_model<S: ::std::hash::BuildHasher>(
    component: &Model,
    params: &HashMap<String, SecretString, S>,
    rt: Arc<Runtime>,
) -> Result<Box<dyn Chat>, LlmError> {
    let model_id = component.get_model_id();
    let prefix = component.get_source().ok_or(LlmError::UnknownModelSource {
        source: format!(
            "Unknown model source for spicepod component from: {}",
            component.from.clone()
        )
        .into(),
    })?;

    let model = construct_model(&prefix, model_id, component, params)?;

    // Handle tool usage
    let spice_tool_opt: Option<SpiceToolsOptions> = params
        .get("spice_tools")
        .map(Secret::expose_secret)
        .map(|x| x.parse())
        .transpose()
        .map_err(|_| LlmError::UnsupportedSpiceToolUseParameterError {})?;

    let tool_model = match spice_tool_opt {
        Some(opts) if opts.can_use_tools() => Box::new(ToolUsingChat::new(
            Arc::new(model),
            Arc::clone(&rt),
            get_tools(Arc::clone(&rt), &opts).await,
        )),
        Some(_) | None => model,
    };
    Ok(tool_model)
}

pub fn construct_model<S: ::std::hash::BuildHasher>(
    prefix: &ModelSource,
    model_id: Option<String>,
    component: &spicepod::component::model::Model,
    params: &HashMap<String, SecretString, S>,
) -> Result<Box<dyn Chat>, LlmError> {
    let model = match prefix {
        ModelSource::HuggingFace => {
            let model_type = params.get("model_type").map(Secret::expose_secret).cloned();

            let tokenizer_path = component.find_any_file_path(ModelFileType::Tokenizer);
            let tokenizer_config_path =
                component.find_any_file_path(ModelFileType::TokenizerConfig);
            let weights_path = model_id
                .clone()
                .or(component.find_any_file_path(ModelFileType::Weights));

            match model_id {
                Some(id) => {
                    llms::chat::create_hf_model(
                        &id,
                        model_type.map(|x| x.to_string()),
                        &weights_path,
                        &tokenizer_path,
                        &tokenizer_config_path, // TODO handle inline chat templates
                    )
                }
                None => Err(LlmError::FailedToLoadModel {
                    source: "No model id for Huggingface model".to_string().into(),
                }),
            }
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
            let tokenizer_config_path = component
                .find_any_file_path(ModelFileType::TokenizerConfig)
                .ok_or(LlmError::FailedToLoadTokenizer {
                    source: "No 'tokenizer_config_path' parameter provided".into(),
                })?;

            llms::chat::create_local_model(
                &weights_path,
                tokenizer_path.as_deref(),
                tokenizer_config_path.as_ref(),
            )
        }
        ModelSource::SpiceAI => Err(LlmError::UnsupportedTaskForModel {
            from: "spiceai".into(),
            task: "llm".into(),
        }),
        ModelSource::OpenAi => {
            let api_base = params.get("endpoint").map(Secret::expose_secret).cloned();
            let api_key = params
                .get("openai_api_key")
                .map(Secret::expose_secret)
                .cloned();
            let org_id = params
                .get("openai_org_id")
                .map(Secret::expose_secret)
                .cloned();
            let project_id = params
                .get("openai_project_id")
                .map(Secret::expose_secret)
                .cloned();

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
    let wrapper = ChatWrapper::new(model, component.params.get("system_prompt").cloned());
    Ok(Box::new(wrapper))
}

/// Wraps [`Chat`] models with additional handling specifically for the spice runtime (e.g. telemetry, injecting system prompts).
pub struct ChatWrapper {
    pub chat: Box<dyn Chat>,
    pub system_prompt: Option<String>,
}
impl ChatWrapper {
    pub fn new(chat: Box<dyn Chat>, system_prompt: Option<String>) -> Self {
        Self {
            chat,
            system_prompt,
        }
    }

    /// Create a new [`CreateChatCompletionRequest`] with the system prompt injected as the first message, if it exists.
    fn prepare_req(
        &self,
        req: CreateChatCompletionRequest,
    ) -> Result<CreateChatCompletionRequest, OpenAIError> {
        match &self.system_prompt {
            Some(prompt) => {
                let system_message = ChatCompletionRequestSystemMessageArgs::default()
                    .content(prompt)
                    .build()?;
                let mut req = req.clone();
                req.messages
                    .insert(0, ChatCompletionRequestMessage::System(system_message));
                Ok(req)
            }
            None => Ok(req),
        }
    }
}

#[async_trait]
impl Chat for ChatWrapper {
    /// Expect `truncated_output` to be instrumented by the underlying chat model (to not reopen/parse streams). i.e.
    /// ```rust
    /// tracing::info!(target: "task_history", truncated_output = %chat_output)
    /// ```
    async fn chat_stream(
        &self,
        req: CreateChatCompletionRequest,
    ) -> Result<ChatCompletionResponseStream, OpenAIError> {
        let req = self.prepare_req(req)?;
        let span = tracing::span!(target: "task_history", tracing::Level::INFO, "ai_completion", stream=true, model = %req.model, input = %serde_json::to_string(&req).unwrap_or_default(),  "labels");

        match self.chat.chat_stream(req).instrument(span.clone()).await {
            Ok(resp) => Ok(Box::pin(resp.instrument(span))),
            Err(e) => {
                tracing::error!(target: "task_history", parent: &span, "Failed to run chat model: {}", e);
                Err(e)
            }
        }
    }

    /// Unlike [`ChatWrapper::chat_stream`], this method will instrument the `truncated_output` for the model output.
    async fn chat_request(
        &self,
        req: CreateChatCompletionRequest,
    ) -> Result<CreateChatCompletionResponse, OpenAIError> {
        let req = self.prepare_req(req)?;
        let span = tracing::span!(target: "task_history", tracing::Level::INFO, "ai_completion", stream=false, model = %req.model, input = %serde_json::to_string(&req).unwrap_or_default(), "labels");

        match self.chat.chat_request(req).instrument(span.clone()).await {
            Ok(resp) => {
                let truncated_output: Vec<_> = resp.choices.iter().map(|c| &c.message).collect();
                match serde_json::to_string(&truncated_output) {
                    Ok(output) => {
                        tracing::info!(target: "task_history", truncated_output = %output);
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
}
