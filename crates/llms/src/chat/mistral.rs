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
#![allow(clippy::missing_errors_doc)]
#![allow(clippy::borrowed_box)]
#![allow(clippy::needless_pass_by_value)]

use crate::chat::message_to_mistral;

use super::{nsql::SqlGeneration, Chat, Error as ChatError, FailedToRunModelSnafu, Result};
use async_openai::{
    error::{ApiError, OpenAIError},
    types::{
        ChatCompletionNamedToolChoice, ChatCompletionTool, ChatCompletionToolChoiceOption,
        CreateChatCompletionRequest, CreateChatCompletionResponse, Stop,
    },
};
use async_stream::stream;
use async_trait::async_trait;
use futures::Stream;
use mistralrs::{
    ChatCompletionResponse, Constraint, Device, DeviceMapMetadata, Function, GGMLLoaderBuilder,
    GGMLSpecificConfig, GGUFLoaderBuilder, GGUFSpecificConfig, LocalModelPaths, MistralRs,
    MistralRsBuilder, ModelDType, ModelPaths, NormalLoaderBuilder, NormalRequest, Pipeline,
    Request as MistralRequest, RequestMessage, Response as MistralResponse, SamplingParams,
    TokenSource, Tool, ToolChoice, ToolType,
};

use snafu::ResultExt;
use std::{
    collections::HashMap,
    num::NonZeroUsize,
    path::Path,
    pin::Pin,
    str::FromStr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
use tokio::sync::mpsc::{channel, Sender};

pub struct MistralLlama {
    pipeline: Arc<MistralRs>,
    counter: AtomicUsize,
}

#[allow(deprecated)]
fn to_openai_response(resp: &ChatCompletionResponse) -> Result<CreateChatCompletionResponse> {
    let resp_str = serde_json::to_string(resp)
        .boxed()
        .context(FailedToRunModelSnafu)?;
    serde_json::from_str(&resp_str)
        .boxed()
        .context(FailedToRunModelSnafu)
}

impl MistralLlama {
    pub fn from(
        tokenizer: Option<&Path>,
        model_weights: &Path,
        tokenizer_config: &Path,
    ) -> Result<Self> {
        let extension = model_weights
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or_default();
        match extension {
            "ggml" => match tokenizer {
                Some(tokenizer) => Self::from_ggml(tokenizer, model_weights, tokenizer_config),
                None => Err(ChatError::FailedToLoadModel {
                    source: "Tokenizer path is required for GGML model".into(),
                }),
            },
            "gguf" => Self::from_gguf(tokenizer, model_weights, tokenizer_config),
            _ => Err(ChatError::FailedToLoadModel {
                source: format!("Unknown model type: {extension}").into(),
            }),
        }
    }

    fn create_paths(
        model_weights: &Path,
        tokenizer: Option<&Path>,
        tokenizer_config: &Path,
    ) -> Box<dyn ModelPaths> {
        Box::new(LocalModelPaths::new(
            tokenizer.map(Into::into).unwrap_or_default(),
            // Not needed for LLama2 / DuckDB Chat, but needed in `EricLBuehler/mistral.rs`.
            tokenizer.map(Into::into).unwrap_or_default(),
            tokenizer_config.into(),
            vec![model_weights.into()],
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        ))
    }

    fn load_gguf_pipeline(
        paths: Box<dyn ModelPaths>,
        device: &Device,
        _tokenizer: Option<&Path>,
        model_id: &str,
    ) -> Result<Arc<tokio::sync::Mutex<dyn Pipeline + Sync + Send>>> {
        let chat_template = paths
            .get_template_filename()
            .clone()
            .map(|f| f.to_string_lossy().to_string());

        let gguf_file: Vec<String> = paths
            .get_weight_filenames()
            .iter()
            .map(|p| p.to_string_lossy().to_string())
            .collect();

        GGUFLoaderBuilder::new(
            chat_template,
            None,
            model_id.to_string(),
            gguf_file,
            GGUFSpecificConfig::default(),
        )
        .build()
        .load_model_from_path(
            &paths,
            &ModelDType::Auto,
            device,
            false,
            DeviceMapMetadata::dummy(),
            None,
            None,
        )
        .map_err(|e| ChatError::FailedToLoadModel { source: e.into() })
    }

    fn load_ggml_pipeline(
        paths: Box<dyn ModelPaths>,
        device: &Device,
        tokenizer: &Path,
        model_id: &str,
    ) -> Result<Arc<tokio::sync::Mutex<dyn Pipeline + Sync + Send>>> {
        GGMLLoaderBuilder::new(
            GGMLSpecificConfig::default(),
            None,
            Some(tokenizer.to_string_lossy().to_string()),
            None,
            String::new(),
            model_id.to_string(),
        )
        .build()
        .load_model_from_path(
            &paths,
            &ModelDType::Auto,
            device,
            false,
            DeviceMapMetadata::dummy(),
            None,
            None,
        )
        .map_err(|e| ChatError::FailedToLoadModel { source: e.into() })
    }

    pub fn from_gguf(
        tokenizer: Option<&Path>,
        model_weights: &Path,
        tokenizer_config: &Path,
    ) -> Result<Self> {
        let paths = Self::create_paths(model_weights, tokenizer, tokenizer_config);
        let model_id = model_weights
            .file_name()
            .and_then(|x| x.to_str())
            .map_or_else(
                || model_weights.to_string_lossy().to_string(),
                std::string::ToString::to_string,
            );

        let device = Self::get_device();

        let pipeline = Self::load_gguf_pipeline(paths, &device, tokenizer, &model_id)?;

        Ok(Self::from_pipeline(pipeline))
    }

    pub fn from_ggml(
        tokenizer: &Path,
        model_weights: &Path,
        tokenizer_config: &Path,
    ) -> Result<Self> {
        let paths = Self::create_paths(model_weights, Some(tokenizer), tokenizer_config);
        let model_id = model_weights.to_string_lossy().to_string();
        let device = Self::get_device();

        let pipeline = Self::load_ggml_pipeline(paths, &device, tokenizer, &model_id)?;

        Ok(Self::from_pipeline(pipeline))
    }

    /// Get the device to use for the model.
    /// Preference order: CUDA, Metal, CPU.
    fn get_device() -> Device {
        #[cfg(feature = "cuda")]
        {
            Device::cuda_if_available(0).unwrap_or(Device::Cpu)
        }
        #[cfg(all(not(feature = "cuda"), feature = "metal"))]
        {
            Device::new_metal(0).unwrap_or(Device::Cpu)
        }
        #[cfg(all(not(feature = "cuda"), not(feature = "metal")))]
        {
            Device::Cpu
        }
    }

    pub fn from_hf(model_id: &str, arch: &str, hf_token_literal: Option<String>) -> Result<Self> {
        let model_parts: Vec<&str> = model_id.split(':').collect();

        let loader_type = mistralrs::NormalLoaderType::from_str(arch).map_err(|_| {
            ChatError::FailedToLoadModel {
                source: format!("Unknown model type: {arch}").into(),
            }
        })?;

        let builder = NormalLoaderBuilder::new(
            mistralrs::NormalSpecificConfig::default(),
            None,
            None,
            Some(model_parts[0].to_string()),
        );
        let device = Self::get_device();
        let token_source = hf_token_literal.map_or(TokenSource::CacheToken, TokenSource::Literal);

        let pipeline = builder
            .build(Some(loader_type))
            .map_err(|e| ChatError::FailedToLoadModel { source: e.into() })?
            .load_model_from_hf(
                model_parts.get(1).map(|&x| x.to_string()),
                token_source,
                &ModelDType::Auto,
                &device,
                false,
                DeviceMapMetadata::dummy(),
                None,
                None,
            )
            .map_err(|e| ChatError::FailedToLoadModel { source: e.into() })?;

        Ok(Self::from_pipeline(pipeline))
    }

    #[allow(clippy::expect_used)]
    fn from_pipeline(p: Arc<tokio::sync::Mutex<dyn Pipeline + Sync + Send>>) -> Self {
        Self {
            pipeline: MistralRsBuilder::new(
                p,
                mistralrs::SchedulerConfig::DefaultScheduler {
                    method: mistralrs::DefaultSchedulerMethod::Fixed(
                        NonZeroUsize::new(5).expect("unreachable 5 > 0"),
                    ),
                },
            )
            .build(),
            counter: AtomicUsize::new(0),
        }
    }

    // TODO: handle passing in [`SamplingParams`] from request.
    fn to_mistralrs_request(
        &self,
        message: RequestMessage,
        is_streaming: bool,
        tx: Sender<MistralResponse>,
        tools: Option<Vec<Tool>>,
        tool_choice: Option<ToolChoice>,
        sampling: Option<SamplingParams>,
    ) -> MistralRequest {
        MistralRequest::Normal(NormalRequest {
            messages: message,
            sampling_params: sampling.unwrap_or(SamplingParams::deterministic()),
            response: tx,
            return_logprobs: false,
            is_streaming,
            id: self.counter.fetch_add(1, Ordering::SeqCst),
            constraint: Constraint::None,
            suffix: None,
            adapters: None,
            tools,
            tool_choice,
            logits_processors: None,
        })
    }

    async fn run_internal(
        &self,
        message: RequestMessage,
        tools: Option<Vec<Tool>>,
        tool_choice: Option<ToolChoice>,
        sampling: Option<SamplingParams>,
    ) -> Result<ChatCompletionResponse> {
        let (snd, mut rcv) = channel::<MistralResponse>(10_000);

        tracing::trace!("Sending request to pipeline");
        self.pipeline
            .get_sender()
            .boxed()
            .context(FailedToRunModelSnafu)?
            .send(self.to_mistralrs_request(
                message.clone(),
                false,
                snd,
                tools,
                tool_choice,
                sampling,
            ))
            .await
            .boxed()
            .context(FailedToRunModelSnafu)?;
        tracing::trace!("Request sent!");

        match rcv.recv().await {
            Some(response) => match response {
                MistralResponse::Done(resp) => Ok(resp),
                MistralResponse::ModelError(e, _) => {
                    Err(ChatError::FailedToRunModel { source: e.into() })
                }
                MistralResponse::InternalError(e) | MistralResponse::ValidationError(e) => {
                    tracing::error!(
                        "Internal mistral.rs error: {e} for messages: {:#?}",
                        message.clone()
                    );
                    Err(ChatError::FailedToRunModel { source: e })
                }

                // Don't expect MistralResponse::Chunk, should be streaming only.
                _ => Err(ChatError::FailedToRunModel {
                    source: "Unexpected error occurred".into(),
                }),
            },
            None => Err(ChatError::FailedToRunModel {
                source: "Mistral pipeline unexpectedly closed".into(),
            }),
        }
    }
}

#[async_trait]
impl Chat for MistralLlama {
    fn as_sql(&self) -> Option<&dyn SqlGeneration> {
        None
    }
    async fn health(&self) -> Result<()> {
        // If [`MistralLlama`] is instantiated successfully, it is healthy.
        Ok(())
    }

    async fn stream<'a>(
        &self,
        prompt: String,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Option<String>>> + Send>>> {
        let (snd, mut rcv) = channel::<MistralResponse>(1000);
        tracing::trace!("Sending request to pipeline");
        self.pipeline
            .get_sender()
            .boxed()
            .context(FailedToRunModelSnafu)?
            .send(self.to_mistralrs_request(
                RequestMessage::Completion {
                    text: prompt,
                    echo_prompt: false,
                    best_of: 1,
                },
                true,
                snd,
                None,
                None,
                None,
            ))
            .await
            .boxed()
            .context(FailedToRunModelSnafu)?;
        tracing::trace!("Request sent!");
        Ok(Pin::from(Box::new(stream! {
            while let Some(resp) = rcv.recv().await {
                tracing::trace!("Received response from pipeline");
                match resp {
                    MistralResponse::CompletionChunk(chunk) => {
                        if let Some(choice) = chunk.choices.first() {
                            yield Ok(Some(choice.text.clone()));
                            if choice.finish_reason.is_some() {
                                break;
                            }
                        } else {
                            yield Ok(None);
                            break;
                        }
                    },
                    MistralResponse::Chunk(chunk) => {
                        if let Some(choice) = chunk.choices.first() {
                            yield Ok(Some(choice.delta.content.clone()));
                            if choice.finish_reason.is_some() {
                                break;
                            }
                        } else {
                            yield Ok(None);
                            break;
                        }
                    },
                    MistralResponse::ModelError(err_msg, _) | MistralResponse::CompletionModelError(err_msg, _) => {
                        yield Err(ChatError::FailedToRunModel {
                            source: err_msg.into(),
                        })
                    },
                    MistralResponse::InternalError(err_msg) | MistralResponse::ValidationError(err_msg) => {
                        yield Err(ChatError::FailedToRunModel {
                            source: err_msg,
                        })
                    },
                    MistralResponse::CompletionDone(cr) => {
                        yield Ok(Some(cr.choices[0].text.clone()));
                        break;
                    },
                    MistralResponse::ImageGeneration(_) => {
                        yield Err(ChatError::UnsupportedModalityType {
                            modality: "image generation".into(),
                        });
                        break;
                    },
                    MistralResponse::Done(_) => {
                        // Only reachable if message is [`RequestMessage::Chat`]. This function is using [`RequestMessage::Completion`].
                        unreachable!()
                    },
                }
            }
        })))
    }

    async fn run(&self, prompt: String) -> Result<Option<String>> {
        let resp = self
            .run_internal(
                RequestMessage::Completion {
                    text: prompt,
                    echo_prompt: false,
                    best_of: 1,
                },
                None,
                None,
                None,
            )
            .await?;

        match resp.choices.first() {
            Some(choice) => Ok(choice.message.content.clone()),
            None => Ok(None),
        }
    }

    #[allow(deprecated, clippy::cast_possible_truncation)]
    async fn chat_request(
        &self,
        req: CreateChatCompletionRequest,
    ) -> Result<CreateChatCompletionResponse, OpenAIError> {
        let messages = RequestMessage::Chat(
            req.messages
                .iter()
                .map(message_to_mistral)
                .collect::<Vec<_>>(),
        );

        let tools: Option<Vec<Tool>> = req.tools.map(|t| t.iter().map(convert_tool).collect());
        let tool_choice: Option<ToolChoice> = req.tool_choice.map(|s| convert_tool_choice(&s));

        // TODO confirm this transformtion.
        let p = SamplingParams {
            temperature: req.temperature.map(f64::from),
            top_k: None,
            top_p: req.top_p.map(f64::from),
            min_p: None,
            top_n_logprobs: req.top_logprobs.unwrap_or_default().into(),
            frequency_penalty: req.frequency_penalty,
            presence_penalty: req.presence_penalty,
            stop_toks: req.stop.map(|s| match s {
                Stop::String(s) => mistralrs::StopTokens::Seqs(vec![s]),
                Stop::StringArray(s) => mistralrs::StopTokens::Seqs(s),
            }),
            max_len: req.max_completion_tokens.map(|x| x as usize),
            // logits_bias: req.logit_bias,
            logits_bias: None,
            n_choices: req.n.unwrap_or(1) as usize,
            dry_params: None,
        };

        let resp = self
            .run_internal(messages, tools, tool_choice, Some(p))
            .await
            .map_err(|e| {
                OpenAIError::ApiError(ApiError {
                    message: e.to_string(),
                    r#type: None,
                    param: None,
                    code: None,
                })
            })?;

        to_openai_response(&resp).map_err(|e| {
            OpenAIError::ApiError(ApiError {
                message: e.to_string(),
                r#type: None,
                param: None,
                code: None,
            })
        })
    }
}

fn convert_tool_choice(x: &ChatCompletionToolChoiceOption) -> ToolChoice {
    match x {
        ChatCompletionToolChoiceOption::None => ToolChoice::None,
        ChatCompletionToolChoiceOption::Auto => ToolChoice::Auto,
        ChatCompletionToolChoiceOption::Required => {
            unimplemented!("`mistral_rs::core` does not yet have `ToolChoice::Required`")
        }
        ChatCompletionToolChoiceOption::Named(t) => ToolChoice::Tool(convert_named_tool(t)),
    }
}

/// [`MistralRs`] uses `Tool` for both choosing a tool, and defining a tool.
/// This use of tool, is for choosing a tool. [`convert_tool`] is for defining a tool.
fn convert_named_tool(x: &ChatCompletionNamedToolChoice) -> Tool {
    Tool {
        tp: ToolType::Function,
        function: Function {
            description: None,
            name: x.function.name.clone(),
            parameters: None,
        },
    }
}

fn convert_tool(x: &ChatCompletionTool) -> Tool {
    Tool {
        tp: ToolType::Function,
        function: Function {
            description: x.function.description.clone(),
            name: x.function.name.clone(),
            parameters: x
                .function
                .parameters
                .clone()
                .and_then(|p| p.as_object().map(|p| HashMap::from_iter(p.clone()))),
        },
    }
}
