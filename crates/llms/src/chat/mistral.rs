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
#![allow(clippy::missing_errors_doc)]
#![allow(clippy::borrowed_box)]
#![allow(clippy::needless_pass_by_value)]

use crate::chat::message_to_mistral;

use super::{nsql::SqlGeneration, Chat, Error as ChatError, FailedToRunModelSnafu, Result};
use async_openai::{
    error::{ApiError, OpenAIError},
    types::{
        ChatChoiceStream, ChatCompletionMessageToolCallChunk, ChatCompletionNamedToolChoice,
        ChatCompletionRequestUserMessageArgs, ChatCompletionResponseStream,
        ChatCompletionStreamResponseDelta, ChatCompletionTool, ChatCompletionToolChoiceOption,
        ChatCompletionToolType, CompletionUsage, CreateChatCompletionRequest,
        CreateChatCompletionRequestArgs, CreateChatCompletionResponse,
        CreateChatCompletionStreamResponse, FinishReason, FunctionCallStream, Role, Stop,
    },
};
use async_stream::stream;
use async_trait::async_trait;
use futures::{Stream, TryStreamExt};
use mistralrs::{
    AutoDeviceMapParams, ChatCompletionChunkResponse, ChatCompletionResponse, ChunkChoice,
    Constraint, Device, DeviceMapSetting, Function, GGMLLoaderBuilder, GGMLSpecificConfig,
    GGUFLoaderBuilder, GGUFSpecificConfig, Loader, LocalModelPaths, MistralRs, MistralRsBuilder,
    ModelDType, ModelPaths, NormalLoaderBuilder, NormalRequest, Pipeline,
    Request as MistralRequest, RequestMessage, Response as MistralResponse, SamplingParams,
    TokenSource, Tool, ToolCallResponse, ToolChoice, ToolType,
};

use secrecy::{ExposeSecret, Secret};
use snafu::ResultExt;
use std::{
    collections::HashMap,
    num::NonZeroUsize,
    path::{Path, PathBuf},
    pin::Pin,
    str::FromStr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
use tokio::sync::mpsc::{channel, Receiver, Sender};

pub struct MistralLlama {
    pipeline: Arc<MistralRs>,
    counter: AtomicUsize,
}

fn to_openai_response(
    resp: &ChatCompletionResponse,
) -> Result<CreateChatCompletionResponse, OpenAIError> {
    let resp_str = serde_json::to_string(resp)?;
    serde_json::from_str(&resp_str).map_err(OpenAIError::from)
}

impl MistralLlama {
    pub fn from(
        model_weights: &[PathBuf],
        config: Option<&Path>,
        tokenizer: Option<&Path>,
        tokenizer_config: Option<&Path>,
        generation_config: Option<&Path>,
        chat_template_literal: Option<&str>,
    ) -> Result<Self> {
        for weight in model_weights {
            if !weight.exists() {
                return Err(ChatError::LocalModelNotFound {
                    expected_path: weight.to_string_lossy().to_string(),
                });
            }
        }

        if let Some(config) = config {
            if !config.exists() {
                return Err(ChatError::LocalModelConfigNotFound {
                    expected_path: config.to_string_lossy().to_string(),
                });
            }
        }

        if let Some(tokenizer) = tokenizer {
            if !tokenizer.exists() {
                return Err(ChatError::LocalTokenizerNotFound {
                    expected_path: tokenizer.to_string_lossy().to_string(),
                });
            }
        }

        if let Some(tokenizer_config) = tokenizer_config {
            if !tokenizer_config.exists() {
                return Err(ChatError::LocalTokenizerNotFound {
                    expected_path: tokenizer_config.to_string_lossy().to_string(),
                });
            }
        }

        let paths = Self::create_paths(
            model_weights,
            config,
            tokenizer,
            tokenizer_config,
            generation_config,
        );
        let model_id = model_weights
            .first()
            .map(|w| w.to_string_lossy().to_string())
            .unwrap_or_default();
        let device = Self::get_device();

        let extension = model_weights
            .first()
            .and_then(|p| p.as_path().extension())
            .and_then(|e| e.to_str());

        let pipeline = match extension {
            Some("ggml") => {
                Self::load_ggml_pipeline(paths, &device, &model_id, chat_template_literal)?
            }
            Some("gguf") => {
                Self::load_gguf_pipeline(paths, &device, &model_id, chat_template_literal)?
            }
            _ => Self::load_default_pipeline(paths, &device, &model_id, chat_template_literal)?,
        };

        Ok(Self::from_pipeline(pipeline))
    }

    /// Create paths object, [`ModelPaths`], to create new [`MistralLlama`].
    ///
    /// `model_weights`: Currently only single file formats (GGUF, GGML, safetensors).
    /// `config`: e.g. `config.json`. Not needed for GGUF.
    /// `tokenizer`: e.g. `tokenizer.json`. Not needed for GGUF.
    /// `tokenizer_config`: e.g. `tokenizer_config.json`. Not needed for GGUF.
    ///
    fn create_paths(
        model_weights: &[PathBuf],
        config: Option<&Path>,
        tokenizer: Option<&Path>,
        tokenizer_config: Option<&Path>,
        generation_config: Option<&Path>,
    ) -> Box<dyn ModelPaths> {
        Box::new(LocalModelPaths::new(
            tokenizer.map(Into::into).unwrap_or_default(),
            config.map(Into::into).unwrap_or_default(),
            tokenizer_config.map(Into::into),
            model_weights.iter().map(Into::into).collect(),
            None,
            None,
            None,
            None,
            None,
            generation_config.map(Into::into),
            None,
            None,
            None,
            None,
        ))
    }

    fn load_default_pipeline(
        paths: Box<dyn ModelPaths>,
        device: &Device,
        model_id: &str,
        chat_template_literal: Option<&str>,
    ) -> Result<Arc<tokio::sync::Mutex<dyn Pipeline + Sync + Send>>> {
        let model_parts: Vec<&str> = model_id.split(':').collect();
        NormalLoaderBuilder::new(
            mistralrs::NormalSpecificConfig::default(),
            chat_template_literal.map(ToString::to_string),
            None,
            model_parts.first().map(ToString::to_string),
        )
        .build(None) // Infer loader type
        .map_err(|e| ChatError::FailedToLoadModel { source: e.into() })?
        .load_model_from_path(
            &paths,
            &ModelDType::Auto,
            device,
            true,
            DeviceMapSetting::Auto(AutoDeviceMapParams::default_text()),
            None,
            None,
        )
        .map_err(|e| ChatError::FailedToLoadModel { source: e.into() })
    }

    fn load_gguf_pipeline(
        paths: Box<dyn ModelPaths>,
        device: &Device,
        model_id: &str,
        chat_template_literal: Option<&str>,
    ) -> Result<Arc<tokio::sync::Mutex<dyn Pipeline + Sync + Send>>> {
        // Note: GGUF supports chat templates in the file, but since GGML/llama.cpp does
        // not write them into GGUF with their conversions, often it requires user
        // override via the template.
        // See `<https://github.com/ggerganov/ggml/pull/302#issuecomment-1784164986>`
        let mut chat_template = paths
            .get_template_filename()
            .clone()
            .map(|f| f.to_string_lossy().to_string());

        // mistralrs currently does not support both `chat_template` string literals and a template file (e.g. tokenizer_config.json).
        // One may want both since the template file has more configurations for the model.
        // Default to use file over string literal.
        if let Some(filename) = chat_template.as_ref() {
            if chat_template_literal.is_some() {
                tracing::warn!("For GGUF model, both a template file was specific '{filename}' and a string literal chat_template. For GGUF only one can be provided, defaulting to the file.");
            };
        } else {
            tracing::debug!("For GGUF model, no chat template file provided. Using the provided chat template literal.");
            chat_template = chat_template_literal.map(Into::into);
        };

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
            true,
            DeviceMapSetting::Auto(AutoDeviceMapParams::default_text()),
            None,
            None,
        )
        .map_err(|e| ChatError::FailedToLoadModel { source: e.into() })
    }

    fn load_ggml_pipeline(
        paths: Box<dyn ModelPaths>,
        device: &Device,
        model_id: &str,
        chat_template_literal: Option<&str>,
    ) -> Result<Arc<tokio::sync::Mutex<dyn Pipeline + Sync + Send>>> {
        let tokenizer = paths.get_tokenizer_filename().to_string_lossy().to_string();
        GGMLLoaderBuilder::new(
            GGMLSpecificConfig::default(),
            chat_template_literal.map(ToString::to_string),
            Some(tokenizer),
            None,
            String::new(),
            model_id.to_string(),
        )
        .build()
        .load_model_from_path(
            &paths,
            &ModelDType::Auto,
            device,
            true,
            DeviceMapSetting::Auto(AutoDeviceMapParams::default_text()),
            None,
            None,
        )
        .map_err(|e| ChatError::FailedToLoadModel { source: e.into() })
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

    pub fn from_hf(
        model_id: &str,
        arch: Option<&str>,
        hf_token_literal: Option<&Secret<String>>,
        gguf_filename: Option<PathBuf>,
    ) -> Result<Self> {
        let model_parts: Vec<&str> = model_id.split(':').collect();

        // Loading the GGUF directly (as if it is a quantized model, although it need not be quantized).
        let loader: Result<Box<dyn Loader>> = if let Some(gguf) = gguf_filename {
            Ok(GGUFLoaderBuilder::new(
                None,
                None,
                model_parts[0].to_string(),
                vec![gguf.to_string_lossy().to_string()],
                GGUFSpecificConfig::default(),
            )
            .build())
        } else {
            // Hardcoded model architecture can ensure correct loading type.
            // If not provided, it will be inferred (generally from `.model_type` in a downloaded `config.json`)
            let loader_type = arch
                .map(|a| {
                    mistralrs::NormalLoaderType::from_str(a)
                        .map_err(|e| ChatError::UnsupportedModelType { source: e.into() })
                })
                .transpose()?;

            let builder = NormalLoaderBuilder::new(
                mistralrs::NormalSpecificConfig::default(),
                None,
                None,
                Some(model_parts[0].to_string()),
            );

            builder
                .build(loader_type)
                .map_err(|e| ChatError::FailedToLoadModel { source: e.into() })
        };

        let device = Self::get_device();
        let token_source = hf_token_literal.map_or(TokenSource::CacheToken, |secret| {
            tracing::debug!("A HuggingFace token was specified in parameters. The specified token will be used instead of any system/environment defaults.");
            TokenSource::Literal(secret.expose_secret().clone())
        });

        let pipeline = loader?
            .load_model_from_hf(
                model_parts.get(1).map(|&x| x.to_string()),
                token_source,
                &ModelDType::Auto,
                &device,
                false,
                DeviceMapSetting::Auto(AutoDeviceMapParams::default_text()),
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
            return_raw_logits: false,
        })
    }

    /// Prepares and sends a [`CreateChatCompletionRequest`] to the model pipeline.
    #[allow(clippy::cast_possible_truncation)]
    async fn send_message(
        &self,
        req: CreateChatCompletionRequest,
    ) -> Result<Receiver<MistralResponse>> {
        let message = RequestMessage::Chat(
            req.messages
                .iter()
                .map(message_to_mistral)
                .collect::<Vec<_>>(),
        );

        let tools: Option<Vec<Tool>> = req.tools.map(|t| t.iter().map(convert_tool).collect());
        let tool_choice: Option<ToolChoice> = req.tool_choice.map(|s| convert_tool_choice(&s));

        let sampling = SamplingParams {
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
            logits_bias: None,
            n_choices: req.n.unwrap_or(1) as usize,
            dry_params: None,
        };
        let (tx, rx) = channel::<MistralResponse>(10_000);

        tracing::trace!("Sending request to pipeline");
        self.pipeline
            .get_sender()
            .boxed()
            .context(FailedToRunModelSnafu)?
            .send(self.to_mistralrs_request(
                message.clone(),
                req.stream.unwrap_or_default(),
                tx,
                tools,
                tool_choice,
                Some(sampling),
            ))
            .await
            .boxed()
            .context(FailedToRunModelSnafu)?;
        tracing::trace!("Request sent!");

        Ok(rx)
    }

    /// Process a single `stream=false` request to generate `OpenAi` chat completion.
    fn post_process_req(resp: MistralResponse) -> Result<ChatCompletionResponse, OpenAIError> {
        match resp {
            MistralResponse::Done(mut resp) => {
                // mistralrs does not return "tool_calls" as a finish_reason correctly (like OpenAI spec).
                // This is a workaround to set it correctly.
                resp.choices.iter_mut().for_each(|c| {
                    if c.finish_reason == "stop" && !c.message.tool_calls.is_empty() {
                        c.finish_reason = "tool_calls".to_string();
                    }
                });

                Ok(resp)
            }
            MistralResponse::ModelError(e, _) => Err(OpenAIError::ApiError(ApiError {
                message: e,
                r#type: None,
                param: None,
                code: None,
            })),
            MistralResponse::InternalError(e) | MistralResponse::ValidationError(e) => {
                tracing::error!("Internal mistral.rs error: {e}",);
                Err(OpenAIError::ApiError(ApiError {
                    message: e.to_string(),
                    r#type: None,
                    param: None,
                    code: None,
                }))
            }

            // Don't expect MistralResponse::Chunk, should be streaming only.
            _ => Err(OpenAIError::ApiError(ApiError {
                message: "Unexpected error occurred".to_string(),
                r#type: None,
                param: None,
                code: None,
            })),
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
        let user_message = ChatCompletionRequestUserMessageArgs::default()
            .content(prompt)
            .build()
            .boxed()
            .context(FailedToRunModelSnafu)?;

        let resp = self
            .chat_stream(
                CreateChatCompletionRequestArgs::default()
                    .messages(vec![user_message.into()])
                    .build()
                    .boxed()
                    .context(FailedToRunModelSnafu)?,
            )
            .await
            .boxed()
            .context(FailedToRunModelSnafu)?;

        let new_stream = resp
            .map_ok(|r| r.choices.first().and_then(|c| c.delta.content.clone()))
            .map_err(|e| ChatError::FailedToRunModel {
                source: Box::<dyn std::error::Error + Send + Sync>::from(e.to_string()),
            });

        Ok(Box::pin(new_stream))
    }

    async fn run(&self, prompt: String) -> Result<Option<String>> {
        let user_message = ChatCompletionRequestUserMessageArgs::default()
            .content(prompt)
            .build()
            .boxed()
            .context(FailedToRunModelSnafu)?;

        let resp = self
            .chat_request(
                CreateChatCompletionRequestArgs::default()
                    .messages(vec![user_message.into()])
                    .build()
                    .boxed()
                    .context(FailedToRunModelSnafu)?,
            )
            .await
            .boxed()
            .context(FailedToRunModelSnafu)?;

        match resp.choices.first() {
            Some(choice) => Ok(choice.message.content.clone()),
            None => Ok(None),
        }
    }

    async fn chat_stream(
        &self,
        req: CreateChatCompletionRequest,
    ) -> Result<ChatCompletionResponseStream, OpenAIError> {
        let recver = self.send_message(req).await.map_err(|e| {
            OpenAIError::ApiError(ApiError {
                message: e.to_string(),
                r#type: None,
                param: None,
                code: None,
            })
        })?;
        Ok(stream_from_response(recver))
    }

    async fn chat_request(
        &self,
        req: CreateChatCompletionRequest,
    ) -> Result<CreateChatCompletionResponse, OpenAIError> {
        let mut recver = self.send_message(req).await.map_err(|e| {
            OpenAIError::ApiError(ApiError {
                message: e.to_string(),
                r#type: None,
                param: None,
                code: None,
            })
        })?;
        let Some(resp) = recver.recv().await else {
            return Err(OpenAIError::ApiError(ApiError {
                message: "model pipeline unexpectedly closed".to_string(),
                r#type: None,
                param: None,
                code: None,
            }));
        };

        Self::post_process_req(resp).map(|z| to_openai_response(&z))?
    }
}

fn stream_from_response(
    mut rcv: Receiver<MistralResponse>,
) -> Pin<Box<dyn Stream<Item = Result<CreateChatCompletionStreamResponse, OpenAIError>> + Send>> {
    Box::pin(stream! {
        while let Some(resp) = rcv.recv().await {
            tracing::trace!("Received response from pipeline");

            match resp {
                // MistralResponse::CompletionChunk(chunk) => yield chunk_to_openai_stream(chunk),
                MistralResponse::Chunk(chunk) => yield chunk_to_openai_stream(chunk),
                MistralResponse::ModelError(err_msg, _) | MistralResponse::CompletionModelError(err_msg, _)=> {
                    yield Err(OpenAIError::ApiError(ApiError {
                        message: err_msg,
                        r#type: None,
                        param: None,
                        code: None,
                    }));
                },
                MistralResponse::InternalError(err_msg) | MistralResponse::ValidationError(err_msg) => {
                    yield Err(OpenAIError::ApiError(ApiError {
                        message: err_msg.to_string(),
                        r#type: None,
                        param: None,
                        code: None,
                    }));
                },
                MistralResponse::ImageGeneration(_) => {
                    yield Err(OpenAIError::ApiError(ApiError {
                        message: "image generation".to_string(),
                        r#type: None,
                        param: None,
                        code: None,
                    }));
                },
                MistralResponse::CompletionChunk(_) | MistralResponse::CompletionDone(_) => {
                    // Only reachable if message is [`RequestMessage::Completion`]
                    unreachable!()
                },
                MistralResponse::Done(_) => {
                    // Only reacable if `stream=false`.
                    unreachable!()
                },
                MistralResponse::Raw{..} => {
                    unreachable!("We set `return_raw_logits: false`")
                }
             }
        }
    })
}

/// Convert a [`CompletionChunkResponse`] to a [`CreateChatCompletionStreamResponse`].
#[allow(clippy::cast_possible_truncation)]
fn chunk_to_openai_stream(
    c: ChatCompletionChunkResponse,
) -> Result<CreateChatCompletionStreamResponse, OpenAIError> {
    let choices = c
        .choices
        .iter()
        .map(chunk_choices_to_openai)
        .collect::<Result<Vec<_>, OpenAIError>>()?;
    Ok(CreateChatCompletionStreamResponse {
        id: c.id,
        model: c.model,
        system_fingerprint: Some(c.system_fingerprint),
        object: "chat.completion.chunk".to_string(),
        // mistralrs uses milliseconds, OpenAI uses seconds
        created: (c.created / 1000) as u32,
        service_tier: None,
        usage: c.usage.map(|u| CompletionUsage {
            prompt_tokens: u.prompt_tokens as u32,
            completion_tokens: u.completion_tokens as u32,
            total_tokens: u.total_tokens as u32,
            prompt_tokens_details: None,
            completion_tokens_details: None,
        }),
        choices,
    })
}

#[allow(
    deprecated,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap
)]
fn chunk_choices_to_openai(choice: &ChunkChoice) -> Result<ChatChoiceStream, OpenAIError> {
    let ChunkChoice {
        index,
        delta,
        finish_reason,
        ..
    } = choice;
    let role: Role = serde_json::from_str(&format!("\"{}\"", delta.role))
        .map_err(OpenAIError::JSONDeserialize)?;

    let finish_reason: Option<FinishReason> = finish_reason
        .as_ref()
        .map(|f| serde_json::from_str(&format!("\"{f}\"")))
        .transpose()
        .map_err(OpenAIError::JSONDeserialize)?;

    Ok(ChatChoiceStream {
        index: *index as u32,
        delta: ChatCompletionStreamResponseDelta {
            content: delta.content.clone(),
            function_call: None,
            tool_calls: delta.tool_calls.as_ref().map(|t| {
                t.iter()
                    .map(|x| parse_tool_call_response(*index as u32, x))
                    .collect()
            }),
            role: Some(role),
            refusal: None,
        },
        finish_reason,
        logprobs: None,
    })
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

fn parse_tool_call_response(
    index: u32,
    r: &ToolCallResponse,
) -> ChatCompletionMessageToolCallChunk {
    ChatCompletionMessageToolCallChunk {
        id: Some(r.id.clone()),
        index,
        r#type: Some(ChatCompletionToolType::Function),
        function: Some(FunctionCallStream {
            name: Some(r.function.name.clone()),
            arguments: Some(r.function.arguments.clone()),
        }),
    }
}
