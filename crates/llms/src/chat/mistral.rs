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

use crate::chat::{LocalModelOptions, PagedAttentionMode, ReasoningDefault, message_to_mistral};
use crate::streaming_utils::create_stream_response_with_timestamp;

use super::{Chat, Error as ChatError, FailedToRunModelSnafu, Result, nsql::SqlGeneration};
use async_openai::{
    error::{ApiError, OpenAIError},
    types::chat::{
        ChatChoiceStream, ChatCompletionAllowedToolsChoice, ChatCompletionMessageToolCallChunk,
        ChatCompletionNamedToolChoice, ChatCompletionRequestUserMessageArgs,
        ChatCompletionResponseStream, ChatCompletionStreamResponseDelta,
        ChatCompletionToolChoiceOption, ChatCompletionTools, CompletionUsage,
        CreateChatCompletionRequest, CreateChatCompletionRequestArgs, CreateChatCompletionResponse,
        CreateChatCompletionStreamResponse, FinishReason, FunctionCallStream, FunctionType,
        ReasoningEffort, Role, StopConfiguration, ToolChoiceAllowedMode, ToolChoiceOptions,
    },
};
use async_stream::stream;
use async_trait::async_trait;
use either::Either;
use futures::{Stream, TryStreamExt};
use indexmap::IndexMap;
use mistralrs::core::{
    AdapterPaths, AllowedToolChoice, AllowedToolsMode, AllowedToolsToolChoice,
    AllowedToolsToolChoiceType, AutoDeviceMapParams, DeviceMapSetting, GGMLLoaderBuilder,
    GGMLSpecificConfig, GGUFLoaderBuilder, GGUFSpecificConfig, Loader, LocalModelPaths, MistralRs,
    MistralRsBuilder, ModelPaths, MultimodalLoaderBuilder, MultimodalLoaderType,
    MultimodalSpecificConfig, NormalLoaderBuilder, NormalLoaderType, NormalSpecificConfig,
    Pipeline, ReasoningEffort as TemplateReasoningEffort, RequestMessage, TokenSource,
};
use mistralrs::{
    ChatCompletionChunkResponse, ChatCompletionResponse, ChunkChoice, Constraint, Device, Function,
    ModelDType, NormalRequest, Request as MistralRequest, Response as MistralResponse,
    SamplingParams, Tool, ToolCallResponse, ToolChoice, ToolType,
};

use serde::Deserialize;
use snafu::ResultExt;
use std::{
    collections::HashMap,
    num::NonZeroUsize,
    path::{Path, PathBuf},
    pin::Pin,
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
use tokio::sync::mpsc::{Receiver, Sender, channel};

/// Preserve the existing local LLM scheduler concurrency. Paged attention uses
/// the same cap so enabling cache-aware scheduling does not change request parallelism.
const LOCAL_LLM_MAX_SEQS: NonZeroUsize = NonZeroUsize::MIN.saturating_add(4);

pub struct MistralLlama {
    pipeline: Arc<MistralRs>,
    counter: AtomicUsize,
    /// The Spicepod name a refused `reasoning_effort` names.
    model_name: String,
    /// What the chat template does with each `reasoning_effort` value.
    reasoning_support: ReasoningSupport,
    /// RAII drop guard that keeps the per-node `RING_CONFIG` temp file alive for
    /// the model's lifetime (its `Drop` deletes the file on model teardown).
    /// `None` for single-node models. Never read directly.
    #[expect(dead_code)]
    ring_config: Option<tempfile::TempPath>,
}

fn to_openai_response(
    resp: &ChatCompletionResponse,
) -> Result<CreateChatCompletionResponse, OpenAIError> {
    let resp_str = serde_json::to_string(resp)
        .map_err(|e| OpenAIError::InvalidArgument(format!("Failed to serialize response: {e}")))?;
    serde_json::from_str(&resp_str)
        .map_err(|e| OpenAIError::InvalidArgument(format!("Failed to deserialize response: {e}")))
}

impl MistralLlama {
    pub async fn from(
        model_weights: &[PathBuf],
        config: Option<&Path>,
        tokenizer: Option<&Path>,
        tokenizer_config: Option<&Path>,
        generation_config: Option<&Path>,
        options: LocalModelOptions<'_>,
        ring_config_path: Option<tempfile::TempPath>,
    ) -> Result<Self> {
        // mistral.rs reads the template file from `tokenizer_config` itself.
        let LocalModelOptions {
            chat_template_literal,
            chat_template_file: _,
            context_length,
            paged_attention,
            reasoning,
        } = options;
        for weight in model_weights {
            if !weight.exists() {
                return Err(ChatError::LocalModelNotFound {
                    expected_path: weight.to_string_lossy().to_string(),
                });
            }
        }

        if let Some(config) = config
            && !config.exists()
        {
            return Err(ChatError::LocalModelConfigNotFound {
                expected_path: config.to_string_lossy().to_string(),
            });
        }

        if let Some(tokenizer) = tokenizer
            && !tokenizer.exists()
        {
            return Err(ChatError::LocalTokenizerNotFound {
                expected_path: tokenizer.to_string_lossy().to_string(),
            });
        }

        if let Some(tokenizer_config) = tokenizer_config
            && !tokenizer_config.exists()
        {
            return Err(ChatError::LocalTokenizerNotFound {
                expected_path: tokenizer_config.to_string_lossy().to_string(),
            });
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

        let paged_attn_config = match paged_attention {
            PagedAttentionMode::Disabled => {
                tracing::info!(
                    "Serving model {model_id} with dense attention (paged_attention: disabled)"
                );
                None
            }
            PagedAttentionMode::Auto => Self::paged_attention_config(&device),
        };
        let device_map = DeviceMapSetting::Auto(Self::text_device_map_params(context_length));
        let paged_attn_requested = paged_attn_config.is_some();
        let pipeline = match extension {
            Some("ggml") => Self::load_ggml_pipeline(
                paths,
                &device,
                &model_id,
                chat_template_literal,
                device_map,
                paged_attn_config,
            )?,
            Some("gguf") => Self::load_gguf_pipeline(
                paths,
                &device,
                &model_id,
                chat_template_literal,
                device_map,
                paged_attn_config,
            )?,
            _ => Self::load_default_pipeline(
                paths,
                &device,
                &model_id,
                chat_template_literal,
                device_map,
                paged_attn_config,
            )?,
        };

        Self::from_pipeline(pipeline, paged_attn_requested, ring_config_path, reasoning).await
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
        // NOTE: `LocalModelPaths::new` wraps its 3rd arg (`template_filename`) in `Some`,
        // so passing an empty `PathBuf` (GGUF models carry no `tokenizer_config`) yields
        // `Some("")`, which panics in mistral.rs `get_chat_template` (`.extension()` on an
        // empty path -> "Template filename must be a file"). Construct the struct directly
        // so `template_filename` is `None` when absent: GGUF models then fall back to the
        // chat template embedded in the GGUF metadata. A present `tokenizer_config`
        // (safetensors) is still used as the template source, preserving prior behavior.
        Box::new(LocalModelPaths {
            tokenizer_filename: tokenizer.map(PathBuf::from).unwrap_or_default(),
            config_filename: config.map(PathBuf::from).unwrap_or_default(),
            template_filename: tokenizer_config.map(PathBuf::from),
            filenames: model_weights.to_vec(),
            adapter_paths: AdapterPaths::None,
            gen_conf: generation_config.map(PathBuf::from),
            preprocessor_config: None,
            video_preprocessor_config: None,
            processor_config: None,
            chat_template_json_filename: None,
        })
    }

    fn load_default_pipeline(
        paths: Box<dyn ModelPaths>,
        device: &Device,
        model_id: &str,
        chat_template_literal: Option<&str>,
        device_map: DeviceMapSetting,
        paged_attn_config: Option<mistralrs::PagedAttentionConfig>,
    ) -> Result<Arc<tokio::sync::Mutex<dyn Pipeline + Sync + Send>>> {
        let model_parts: Vec<&str> = model_id.split(':').collect();
        NormalLoaderBuilder::new(
            NormalSpecificConfig::default(),
            chat_template_literal.map(ToString::to_string),
            None,
            model_parts.first().map(ToString::to_string),
            false, // enable KV cache,
            None,
        )
        .build(None) // Infer loader type
        .map_err(|e| ChatError::FailedToLoadModel { source: e.into() })?
        .load_model_from_path(
            &*paths,
            &ModelDType::Auto,
            device,
            true,
            device_map,
            None,
            paged_attn_config,
        )
        .map_err(|e| ChatError::FailedToLoadModel { source: e.into() })
    }

    fn load_gguf_pipeline(
        paths: Box<dyn ModelPaths>,
        device: &Device,
        model_id: &str,
        chat_template_literal: Option<&str>,
        device_map: DeviceMapSetting,
        paged_attn_config: Option<mistralrs::PagedAttentionConfig>,
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
                tracing::warn!(
                    "For GGUF model, both a template file was specific '{filename}' and a string literal chat_template. For GGUF only one can be provided, defaulting to the file."
                );
            }
        } else {
            tracing::debug!(
                "For GGUF model, no chat template file provided. Using the provided chat template literal."
            );
            chat_template = chat_template_literal.map(Into::into);
        }

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
            false,
            None,
        )
        .build()
        .load_model_from_path(
            &*paths,
            &ModelDType::Auto,
            device,
            true,
            device_map,
            None,
            paged_attn_config,
        )
        .map_err(|e| ChatError::FailedToLoadModel { source: e.into() })
    }

    fn load_ggml_pipeline(
        paths: Box<dyn ModelPaths>,
        device: &Device,
        model_id: &str,
        chat_template_literal: Option<&str>,
        device_map: DeviceMapSetting,
        paged_attn_config: Option<mistralrs::PagedAttentionConfig>,
    ) -> Result<Arc<tokio::sync::Mutex<dyn Pipeline + Sync + Send>>> {
        let tokenizer = paths.get_tokenizer_filename().to_string_lossy().to_string();
        GGMLLoaderBuilder::new(
            GGMLSpecificConfig::default(),
            chat_template_literal.map(ToString::to_string),
            Some(tokenizer),
            None,
            String::new(),
            model_id.to_string(),
            false,
            None,
        )
        .build()
        .load_model_from_path(
            &*paths,
            &ModelDType::Auto,
            device,
            true,
            device_map,
            None,
            paged_attn_config,
        )
        .map_err(|e| ChatError::FailedToLoadModel { source: e.into() })
    }

    /// Build the mistral.rs `PagedAttention` config for a locally served model, or `None`
    /// where this build cannot use it.
    ///
    /// Requesting it is all a caller can do: the engine downgrades to dense attention for
    /// architectures with no paged kernel (the Multi-head Latent Attention GGUFs), which
    /// is knowledge that belongs with its loaders rather than here.
    fn paged_attention_config(device: &Device) -> Option<mistralrs::PagedAttentionConfig> {
        if matches!(device, Device::Cpu) || !Self::paged_attention_supported() {
            return None;
        }

        match mistralrs::PagedAttentionMetaBuilder::default().build() {
            Ok(config) => Some(config),
            Err(e) => {
                tracing::warn!("Failed to initialize local LLM paged attention cache: {e}");
                None
            }
        }
    }

    fn paged_attention_supported() -> bool {
        cfg!(all(feature = "cuda", target_family = "unix"))
    }

    /// Auto device-map params for text models, honoring an optional operator-set context
    /// length (the `context_length` model param); the engine's own default applies when
    /// unset. This is the sequence-length budget used to plan cross-device layer placement
    /// and size the KV reservation — it does not raise the context the weights were
    /// trained for, which the model's own metadata still caps.
    fn text_device_map_params(context_length: Option<usize>) -> AutoDeviceMapParams {
        context_length.map_or_else(AutoDeviceMapParams::default_text, |max_seq_len| {
            AutoDeviceMapParams::Text {
                max_seq_len,
                max_batch_size: AutoDeviceMapParams::DEFAULT_MAX_BATCH_SIZE,
            }
        })
    }

    fn default_scheduler_config() -> mistralrs::SchedulerConfig {
        mistralrs::SchedulerConfig::DefaultScheduler {
            method: mistralrs::DefaultSchedulerMethod::Fixed(LOCAL_LLM_MAX_SEQS),
        }
    }

    async fn scheduler_config(
        pipeline: &Arc<tokio::sync::Mutex<dyn Pipeline + Sync + Send>>,
        paged_attn_requested: bool,
    ) -> mistralrs::SchedulerConfig {
        if paged_attn_requested {
            let cache_config = pipeline.lock().await.get_metadata().cache_config.clone();
            if let Some(config) = cache_config {
                return mistralrs::SchedulerConfig::PagedAttentionMeta {
                    max_num_seqs: LOCAL_LLM_MAX_SEQS.get(),
                    max_num_batched_tokens: mistralrs_core::DEFAULT_MAX_NUM_BATCHED_TOKENS,
                    max_prefill_chunk_tokens: mistralrs_core::DEFAULT_MAX_PREFILL_CHUNK_TOKENS,
                    max_decode_steps_before_prefill:
                        mistralrs_core::DEFAULT_MAX_DECODE_STEPS_BEFORE_PREFILL,
                    config,
                };
            }

            tracing::debug!(
                "Paged attention was requested for local LLM caching, but the model did not initialize paged cache metadata. Falling back to the default scheduler."
            );
        }

        Self::default_scheduler_config()
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

    /// Loads a model from `dir`, a local copy of a Hugging Face model repository's files (the
    /// cache's snapshot of it). mistral.rs reads it as a local model directory, so it never
    /// downloads anything or reads a token: Spice downloaded the files.
    ///
    /// `gguf_filename` is a GGUF file's path in the directory; when set, the model is loaded
    /// from it.
    pub async fn from_snapshot(
        dir: &Path,
        arch: Option<&str>,
        gguf_filename: Option<&str>,
        chat_template_literal: Option<&str>,
        ring_config_path: Option<tempfile::TempPath>,
        reasoning: ReasoningDefault<'_>,
    ) -> Result<Self> {
        let model_id = dir.to_string_lossy().to_string();
        let chat_template = chat_template_literal.map(ToString::to_string);

        // Loading the GGUF directly (as if it is a quantized model, although it need not be quantized).
        let (loader, device_map_params): (Result<Box<dyn Loader>>, AutoDeviceMapParams) =
            if let Some(gguf) = gguf_filename {
                (
                    Ok(GGUFLoaderBuilder::new(
                        chat_template.clone(),
                        None,
                        model_id.clone(),
                        vec![gguf.to_string()],
                        GGUFSpecificConfig::default(),
                        false,
                        None,
                    )
                    .build()),
                    AutoDeviceMapParams::default_text(),
                )
            } else {
                // Hardcoded model architecture can ensure correct loading type.
                // If not provided, it will be inferred (generally from `.model_type` in a downloaded `config.json`).
                // Try the text (Normal) loader types first; if the architecture is unknown to that
                // enum, fall back to the multimodal loader (e.g. Gemma 4, Gemma 3, Qwen2-VL, ...).
                let normal_loader_type = arch
                    .map(NormalLoaderType::from_str)
                    .transpose()
                    .ok()
                    .flatten();

                if arch.is_some() && normal_loader_type.is_none() {
                    let multimodal_loader_type = arch
                        .map(|a| {
                            MultimodalLoaderType::from_str(a)
                                .map_err(|e| ChatError::UnsupportedModelType { source: e.into() })
                        })
                        .transpose()?;

                    let builder = MultimodalLoaderBuilder::new(
                        MultimodalSpecificConfig::default(),
                        chat_template,
                        None,
                        Some(model_id.clone()),
                        None,
                    );

                    (
                        Ok(builder.build(multimodal_loader_type)),
                        AutoDeviceMapParams::default_multimodal(),
                    )
                } else {
                    let builder = NormalLoaderBuilder::new(
                        NormalSpecificConfig::default(),
                        chat_template,
                        None,
                        Some(model_id.clone()),
                        false,
                        None,
                    );

                    (
                        builder
                            .build(normal_loader_type)
                            .map_err(|e| ChatError::FailedToLoadModel { source: e.into() }),
                        AutoDeviceMapParams::default_text(),
                    )
                }
            };

        let device = Self::get_device();
        let paged_attn_config = Self::paged_attention_config(&device);
        let paged_attn_requested = paged_attn_config.is_some();

        // A model id that names an existing directory is read from it, with no revision and
        // no token: the loader lists and opens the directory's files.
        let pipeline = loader?
            .load_model_from_hf(
                None,
                TokenSource::None,
                &ModelDType::Auto,
                &device,
                false,
                DeviceMapSetting::Auto(device_map_params),
                None,
                paged_attn_config,
            )
            .map_err(|e| ChatError::FailedToLoadModel { source: e.into() })?;

        Self::from_pipeline(pipeline, paged_attn_requested, ring_config_path, reasoning).await
    }

    /// Starts the engine for a loaded pipeline. A `reasoning_effort` default that the chat
    /// template can't honor fails here, so the model doesn't load to refuse every request.
    async fn from_pipeline(
        pipeline: Arc<tokio::sync::Mutex<dyn Pipeline + Sync + Send>>,
        paged_attn_requested: bool,
        ring_config: Option<tempfile::TempPath>,
        reasoning: ReasoningDefault<'_>,
    ) -> Result<Self> {
        let reasoning_support = Self::reasoning_support(&pipeline).await;
        if let Some(effort) = reasoning.effort {
            reasoning_controls(Some(effort), &reasoning_support, reasoning.model_name).map_err(
                |message| ChatError::InvalidParamValueError {
                    param: "reasoning_effort".to_string(),
                    message,
                },
            )?;
        }

        let scheduler_config = Self::scheduler_config(&pipeline, paged_attn_requested).await;
        Ok(Self {
            pipeline: MistralRsBuilder::new(pipeline, scheduler_config, false, None)
                .build()
                .await,
            counter: AtomicUsize::new(0),
            model_name: reasoning.model_name.to_string(),
            reasoning_support,
            ring_config,
        })
    }

    /// Renders the generation prompt of a one-message conversation once per reasoning
    /// control, to learn what the chat template does with each. Rendering a large template
    /// and tokenizing it is CPU work, so it runs off the async runtime.
    async fn reasoning_support(
        pipeline: &Arc<tokio::sync::Mutex<dyn Pipeline + Sync + Send>>,
    ) -> ReasoningSupport {
        let pipeline = Arc::clone(pipeline);
        tokio::task::spawn_blocking(move || {
            let pipeline = pipeline.blocking_lock();
            let processor = pipeline.get_processor();
            ReasoningSupport::from_renders(|enable_thinking, reasoning_effort| {
                let message = IndexMap::from([
                    ("role".to_string(), Either::Left("user".to_string())),
                    ("content".to_string(), Either::Left("Hi".to_string())),
                ]);
                processor
                    .process(
                        &*pipeline,
                        vec![message],
                        true,
                        false,
                        enable_thinking,
                        reasoning_effort,
                        Vec::new(),
                    )
                    .map(|(_, prompt)| prompt)
                    .inspect_err(|e| {
                        tracing::debug!(
                            "The chat template failed to render with enable_thinking {enable_thinking:?} and reasoning_effort {reasoning_effort:?}, so `reasoning_effort` is passed to it unchecked: {e}"
                        );
                    })
                    .ok()
            })
        })
        .await
        .unwrap_or_else(|e| {
            tracing::debug!(
                "Checking the chat template's reasoning controls stopped, so `reasoning_effort` is passed to it unchecked: {e}"
            );
            ReasoningSupport::default()
        })
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
        MistralRequest::Normal(Box::new(NormalRequest {
            messages: message,
            sampling_params: sampling.unwrap_or(SamplingParams::deterministic()),
            seed: None,
            response: tx,
            return_logprobs: false,
            is_streaming,
            id: self.counter.fetch_add(1, Ordering::SeqCst),
            queued_at: None,
            constraint: Constraint::None,
            suffix: None,
            web_search_options: None,
            tools,
            tool_choice,
            logits_processors: None,
            return_raw_logits: false,
            model_id: None, // Not actually needed.
            truncate_sequence: false,
            max_tool_rounds: None,
            tool_dispatch_url: None,
            // mistral.rs v0.9.0 agentic / code-execution / shell / files features:
            // Spice does not use them, so opt out with defaults.
            enable_code_execution: false,
            enable_shell: false,
            shell_options: None,
            code_execution_permission: None,
            code_execution_approval_notifier: None,
            agent_permission: None,
            agent_approval_handler: None,
            agent_approval_notifier: None,
            session_id: None,
            files: None,
            input_files: Vec::new(),
            adapter: None,
        }))
    }

    /// The reasoning controls to render `req` with, or an invalid-argument error when its
    /// `reasoning_effort` asks for something this model's chat template can't do.
    fn request_reasoning(
        &self,
        req: &CreateChatCompletionRequest,
    ) -> Result<ReasoningControls, OpenAIError> {
        reasoning_controls(
            req.reasoning_effort.as_ref(),
            &self.reasoning_support,
            &self.model_name,
        )
        .map_err(OpenAIError::InvalidArgument)
    }

    /// The most tokens a completion may generate. `max_tokens` is the deprecated spelling
    /// most clients still send; `max_completion_tokens` wins when both are set.
    #[expect(deprecated)]
    fn completion_limit(req: &CreateChatCompletionRequest) -> Option<usize> {
        req.max_completion_tokens
            .or(req.max_tokens)
            .map(|limit| limit as usize)
    }

    /// Prepares and sends a [`CreateChatCompletionRequest`] to the model pipeline.
    ///
    /// `tool_choice` and `reasoning` are converted by the caller ([`convert_tool_choice`],
    /// [`reasoning_controls`]) so a value this pipeline cannot honor is refused as an invalid
    /// argument before anything is sent.
    async fn send_message(
        &self,
        req: CreateChatCompletionRequest,
        tool_choice: Option<ToolChoice>,
        reasoning: ReasoningControls,
    ) -> Result<Receiver<MistralResponse>> {
        let max_len = Self::completion_limit(&req);
        let message = RequestMessage::Chat {
            messages: req
                .messages
                .iter()
                .map(message_to_mistral)
                .collect::<Vec<_>>(),
            enable_thinking: reasoning.enable_thinking,
            reasoning_effort: reasoning.reasoning_effort,
        };

        let tools: Option<Vec<Tool>> = req.tools.map(|t| t.iter().map(convert_tool).collect());

        let sampling = SamplingParams {
            temperature: req.temperature.map(f64::from),
            top_k: None,
            top_p: req.top_p.map(f64::from),
            min_p: None,
            top_n_logprobs: req.top_logprobs.unwrap_or_default().into(),
            frequency_penalty: req.frequency_penalty,
            presence_penalty: req.presence_penalty,
            repetition_penalty: None,
            stop_toks: req.stop.map(|s| match s {
                StopConfiguration::String(s) => mistralrs::StopTokens::Seqs(vec![s]),
                StopConfiguration::StringArray(s) => mistralrs::StopTokens::Seqs(s),
            }),
            ignore_eos: false,
            max_len,
            logits_bias: None,
            n_choices: req.n.unwrap_or(1) as usize,
            dry_params: None,
        };
        let (tx, rx) = channel::<MistralResponse>(10_000);

        tracing::trace!("Sending request to pipeline");
        self.pipeline
            .get_sender(None) // Only one model running in this pipeline.
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
                    if c.finish_reason == "stop"
                        && c.message
                            .tool_calls
                            .as_ref()
                            .is_some_and(|cc| !cc.is_empty())
                    {
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

    /// The longest sequence mistral.rs runs the model with: it refuses a longer prompt.
    async fn context_window(&self) -> Option<u64> {
        // Only one model runs in this pipeline.
        let length = self.pipeline.max_sequence_length(None).ok()??;
        u64::try_from(length).ok().filter(|window| *window > 0)
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
        let tool_choice = request_tool_choice(&req)?;
        let reasoning = self.request_reasoning(&req)?;
        let recver = self
            .send_message(req, tool_choice, reasoning)
            .await
            .map_err(|e| {
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
        let tool_choice = request_tool_choice(&req)?;
        let reasoning = self.request_reasoning(&req)?;
        let mut recver = self
            .send_message(req, tool_choice, reasoning)
            .await
            .map_err(|e| {
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
                MistralResponse::Speech{..} => {
                    yield Err(OpenAIError::ApiError(ApiError {
                        message: "Speech generation is not supported".to_string(),
                        r#type: None,
                        param: None,
                        code: None,
                    }));
                },
                MistralResponse::ImageGeneration(_) => {
                    yield Err(OpenAIError::ApiError(ApiError {
                        message: "image generation is not supported".to_string(),
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
                },
                MistralResponse::Embeddings{..} => {
                    yield Err(OpenAIError::ApiError(ApiError {
                        message: "Embeddings response is not supported in chat".to_string(),
                        r#type: None,
                        param: None,
                        code: None,
                    }));
                }
                // mistral.rs v0.9.0 agentic / diffusion / file responses: Spice does
                // not enable those features on the chat path (agent_permission/files
                // are None), so they should not occur here. Surface an error rather
                // than panic if the engine ever emits one.
                MistralResponse::AgenticToolCallProgress { .. }
                | MistralResponse::BlockDenoisingProgress(_)
                | MistralResponse::AgenticToolApprovalRequired { .. }
                | MistralResponse::File(_) => {
                    yield Err(OpenAIError::ApiError(ApiError {
                        message: "Unsupported response type for chat completions".to_string(),
                        r#type: None,
                        param: None,
                        code: None,
                    }));
                }
             }
        }
    })
}

/// Convert a [`CompletionChunkResponse`] to a [`CreateChatCompletionStreamResponse`].
#[expect(deprecated, clippy::cast_possible_truncation)]
fn chunk_to_openai_stream(
    c: ChatCompletionChunkResponse,
) -> Result<CreateChatCompletionStreamResponse, OpenAIError> {
    let choices = c
        .choices
        .iter()
        .map(chunk_choices_to_openai)
        .collect::<Result<Vec<_>, OpenAIError>>()?;

    let usage = c.usage.map(|u| CompletionUsage {
        prompt_tokens: u.prompt_tokens as u32,
        completion_tokens: u.completion_tokens as u32,
        total_tokens: u.total_tokens as u32,
        prompt_tokens_details: None,
        completion_tokens_details: None,
    });

    // mistralrs uses milliseconds, OpenAI uses seconds
    let created = (c.created / 1000) as u32;

    let mut response =
        create_stream_response_with_timestamp(&c.id, &c.model, choices, usage, created)?;
    response.system_fingerprint = Some(c.system_fingerprint);
    Ok(response)
}

#[expect(deprecated, clippy::cast_possible_truncation)]
fn chunk_choices_to_openai(choice: &ChunkChoice) -> Result<ChatChoiceStream, OpenAIError> {
    let ChunkChoice {
        index,
        delta,
        finish_reason,
        ..
    } = choice;
    let role: Role = serde_json::from_str(&format!("\"{}\"", delta.role))
        .map_err(|e| OpenAIError::InvalidArgument(format!("Failed to parse role: {e}")))?;

    let finish_reason: Option<FinishReason> = finish_reason
        .as_ref()
        .map(|f| serde_json::from_str(&format!("\"{f}\"")))
        .transpose()
        .map_err(|e| OpenAIError::InvalidArgument(format!("Failed to parse finish_reason: {e}")))?;

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

/// Where a refused `reasoning_effort` points the caller.
const REASONING_DOCS: &str = "https://spiceai.org/docs/components/models";

/// The opening tag of a `<think>…</think>` reasoning block.
const THINK_OPEN_TAG: &str = "<think>";

/// The token Gemma 4 chat templates add to the prompt to turn reasoning on.
const GEMMA_THINK_TOKEN: &str = "<|think|>";

/// The levels a request can ask a chat template for, lowest first.
const REASONING_LEVELS: [TemplateReasoningEffort; 4] = [
    TemplateReasoningEffort::Low,
    TemplateReasoningEffort::Medium,
    TemplateReasoningEffort::High,
    TemplateReasoningEffort::XHigh,
];

/// What a model's chat template does with `reasoning_effort`, learned by rendering its
/// generation prompt once per value when the model loads.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReasoningSupport {
    /// Turning reasoning off can't stop the model reasoning: the prompt rendered with it off
    /// still opens a reasoning block, as GLM-5.3's does, or matches the template's own
    /// reasoning default, as gpt-oss's does by rendering `Reasoning: medium`.
    always_reasons: bool,
    /// The levels the template takes, lowest first. `None` when it takes every level alike,
    /// as an on/off switch does.
    levels: Option<Vec<TemplateReasoningEffort>>,
}

impl ReasoningSupport {
    /// Classifies a chat template from `render`, which renders its generation prompt for an
    /// `enable_thinking` and a level, or returns `None` when the template fails. A template
    /// that fails to render with reasoning off or unset, or at every level, is left
    /// unchecked, so requests reach it as they would without this check.
    pub fn from_renders(
        render: impl Fn(Option<bool>, Option<TemplateReasoningEffort>) -> Option<String>,
    ) -> Self {
        let (Some(off), Some(unset)) = (render(Some(false), None), render(None, None)) else {
            return Self::default();
        };
        // A level the template fails to render at is one it refuses, as Qwen3.8-Flash-Next
        // refuses `high`.
        let renders: Vec<_> = REASONING_LEVELS
            .iter()
            .filter_map(|&level| render(None, Some(level)).map(|prompt| (level, prompt)))
            .collect();
        let Some((_, first)) = renders.first() else {
            return Self::default();
        };
        let refuses_a_level = renders.len() < REASONING_LEVELS.len();
        let distinguishes = renders.iter().any(|(_, prompt)| prompt != first);
        // Off renders the template's default, and that default is one of its reasoning
        // levels: the template has a reasoning control but no off switch. A template with no
        // reasoning control at all renders the same prompt for everything, and doesn't reason.
        let off_is_a_level = off == unset && renders.iter().any(|(_, prompt)| *prompt == unset);
        // A template that names its levels takes those, and any level whose prompt is its own,
        // unlike the default's and every other level's. GLM-5.3 renders `medium` and `xhigh`
        // as its default, `Max`; Qwen3.8-Flash-Next takes `medium` without naming it. A
        // template that names no level encodes levels its own way, and takes every level it
        // renders.
        let names_levels = renders
            .iter()
            .any(|(level, prompt)| names_level(prompt, *level));
        let taken: Vec<_> = renders
            .iter()
            .filter(|(level, prompt)| {
                !names_levels
                    || names_level(prompt, *level)
                    || (*prompt != unset
                        && renders.iter().filter(|(_, other)| other == prompt).count() == 1)
            })
            .map(|(level, _)| *level)
            .collect();
        let restricts = (distinguishes && names_levels) || refuses_a_level;
        Self {
            always_reasons: opens_reasoning(&off) || (distinguishes && off_is_a_level),
            levels: (restricts && !taken.is_empty()).then_some(taken),
        }
    }
}

/// Whether a rendered prompt starts the model inside a reasoning block, by the test
/// `mistral.rs` applies to parse the reply: a prompt that ends in `<think>`, or one that
/// carries Gemma 4's `<|think|>`.
fn opens_reasoning(prompt: &str) -> bool {
    prompt.trim_end().ends_with(THINK_OPEN_TAG) || prompt.contains(GEMMA_THINK_TOKEN)
}

/// Whether `prompt` names `level` as a whole word, ignoring case, as GLM-5.3 does with
/// `Reasoning Effort: High`.
fn names_level(prompt: &str, level: TemplateReasoningEffort) -> bool {
    prompt
        .split(|c: char| !c.is_ascii_alphanumeric())
        .any(|word| word.eq_ignore_ascii_case(level.as_str()))
}

/// The reasoning controls a chat template is rendered with.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReasoningControls {
    pub enable_thinking: Option<bool>,
    pub reasoning_effort: Option<TemplateReasoningEffort>,
}

/// The controls for a request's `reasoning_effort`. Unset leaves both to the chat template,
/// `none` turns reasoning off, and a level turns it on at that level. `minimal` asks for
/// `low`, the least a template can be asked for. A value the template can't honor is
/// refused with the message to show the caller.
pub fn reasoning_controls(
    effort: Option<&ReasoningEffort>,
    support: &ReasoningSupport,
    model: &str,
) -> Result<ReasoningControls, String> {
    let (requested, level) = match effort {
        None => return Ok(ReasoningControls::default()),
        Some(ReasoningEffort::None) if support.always_reasons => {
            return Err(always_reasons_message(model, support));
        }
        Some(ReasoningEffort::None) => {
            return Ok(ReasoningControls {
                enable_thinking: Some(false),
                reasoning_effort: None,
            });
        }
        Some(ReasoningEffort::Minimal) => ("minimal", TemplateReasoningEffort::Low),
        Some(ReasoningEffort::Low) => ("low", TemplateReasoningEffort::Low),
        Some(ReasoningEffort::Medium) => ("medium", TemplateReasoningEffort::Medium),
        Some(ReasoningEffort::High) => ("high", TemplateReasoningEffort::High),
        Some(ReasoningEffort::Xhigh) => ("xhigh", TemplateReasoningEffort::XHigh),
    };
    if let Some(levels) = &support.levels
        && !levels.contains(&level)
    {
        return Err(ignored_level_message(model, levels, requested));
    }
    Ok(ReasoningControls {
        enable_thinking: None,
        reasoning_effort: Some(level),
    })
}

/// The refusal for `reasoning_effort: none` on a model whose chat template always reasons.
fn always_reasons_message(model: &str, support: &ReasoningSupport) -> String {
    let least = support
        .levels
        .as_ref()
        .and_then(|levels| levels.first().copied())
        .map_or("low", TemplateReasoningEffort::as_str);
    format!(
        "Model '{model}' always reasons, so `reasoning_effort: none` can't turn reasoning off. Omit `reasoning_effort`, or set '{least}' for the least reasoning. See: {REASONING_DOCS}"
    )
}

/// The refusal for a level the chat template doesn't name, which it would run at its own
/// default instead.
fn ignored_level_message(
    model: &str,
    levels: &[TemplateReasoningEffort],
    requested: &str,
) -> String {
    let names: Vec<String> = levels
        .iter()
        .map(|level| format!("'{}'", level.as_str()))
        .collect();
    let names = match names.split_last() {
        Some((last, rest)) if !rest.is_empty() => format!("{} or {last}", rest.join(", ")),
        _ => names.concat(),
    };
    format!(
        "Model '{model}' takes `reasoning_effort` {names}, and ignores '{requested}'. Set {names}, or omit `reasoning_effort` for the model's default. See: {REASONING_DOCS}"
    )
}

/// Where a refused `tool_choice` points the caller.
const TOOL_CHOICE_DOCS: &str = "https://spiceai.org/docs/components/models/huggingface";

/// The `tool_choice` to hand `mistral.rs` for `req`, or an invalid-argument error when the
/// request asks for a constraint that cannot be enforced — including a tool call required of a
/// request that offers no tools, which `mistral.rs` would otherwise answer in prose, and a tool
/// the request does not offer, which `mistral.rs` would otherwise fail as an internal error.
pub fn request_tool_choice(
    req: &CreateChatCompletionRequest,
) -> Result<Option<ToolChoice>, OpenAIError> {
    let Some(choice) = req
        .tool_choice
        .as_ref()
        .map(convert_tool_choice)
        .transpose()?
    else {
        return Ok(None);
    };
    let requires_call = matches!(
        &choice,
        ToolChoice::Required
            | ToolChoice::AllowedTools(AllowedToolsToolChoice {
                mode: AllowedToolsMode::Required,
                ..
            })
    );
    if requires_call && req.tools.as_ref().is_none_or(Vec::is_empty) {
        return Err(OpenAIError::InvalidArgument(format!(
            "tool_choice requires a tool call, but the request lists no tools. Add the tools the model may call to 'tools', or use tool_choice 'auto'. See: {TOOL_CHOICE_DOCS}"
        )));
    }

    let named: Vec<&str> = match &choice {
        ToolChoice::Tool(tool) => vec![tool.function.name.as_str()],
        ToolChoice::AllowedTools(allowed) => allowed
            .tools
            .iter()
            .filter_map(|tool| match tool {
                AllowedToolChoice::Function { name } => Some(name.as_str()),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    };
    let offered = |name: &str| {
        req.tools.iter().flatten().any(
            |tool| matches!(tool, ChatCompletionTools::Function(tool) if tool.function.name == name),
        )
    };
    if let Some(name) = named.into_iter().find(|name| !offered(name)) {
        return Err(OpenAIError::InvalidArgument(format!(
            "tool_choice names tool '{name}', which the request does not list in 'tools'. Add '{name}' to 'tools', or name one of the tools the request lists. See: {TOOL_CHOICE_DOCS}"
        )));
    }
    Ok(Some(choice))
}

/// Maps a request's `tool_choice` onto the one `mistral.rs` enforces.
///
/// Every choice either maps to the constraint it asks for or is refused: a choice mapped to a
/// weaker one (`required` answered as `auto`, `allowed_tools` as `none`) returns a normal
/// completion that silently ignores what the caller asked for.
fn convert_tool_choice(x: &ChatCompletionToolChoiceOption) -> Result<ToolChoice, OpenAIError> {
    match x {
        ChatCompletionToolChoiceOption::Mode(ToolChoiceOptions::None) => Ok(ToolChoice::None),
        ChatCompletionToolChoiceOption::Mode(ToolChoiceOptions::Auto) => Ok(ToolChoice::Auto),
        ChatCompletionToolChoiceOption::Mode(ToolChoiceOptions::Required) => {
            Ok(ToolChoice::Required)
        }
        ChatCompletionToolChoiceOption::Function(t) => Ok(ToolChoice::Tool(convert_named_tool(t))),
        ChatCompletionToolChoiceOption::AllowedTools(choice) => convert_allowed_tools(choice),
        ChatCompletionToolChoiceOption::Custom(_) => Err(OpenAIError::InvalidArgument(format!(
            "tool_choice of type 'custom' is not supported by locally hosted models. Use 'none', 'auto', 'required', 'allowed_tools', or a named 'function' tool. See: {TOOL_CHOICE_DOCS}"
        ))),
    }
}

/// Maps an `allowed_tools` choice onto `mistral.rs`'s equivalent, which can restrict the model
/// only to named function tools.
fn convert_allowed_tools(
    choice: &ChatCompletionAllowedToolsChoice,
) -> Result<ToolChoice, OpenAIError> {
    let invalid = |reason: &str| {
        OpenAIError::InvalidArgument(format!(
            "tool_choice 'allowed_tools' {reason}. See: {TOOL_CHOICE_DOCS}"
        ))
    };

    let allowed = &choice.allowed_tools;
    if allowed.tools.is_empty() {
        return Err(invalid("lists no tools"));
    }
    let mode = match allowed.mode {
        ToolChoiceAllowedMode::Auto => AllowedToolsMode::Auto,
        ToolChoiceAllowedMode::Required => AllowedToolsMode::Required,
    };
    let tools = allowed
        .tools
        .iter()
        .map(|tool| match ChatCompletionTools::deserialize(tool) {
            Ok(ChatCompletionTools::Function(tool)) => Ok(AllowedToolChoice::Function {
                name: tool.function.name,
            }),
            _ => Err(invalid(
                "lists a tool that is not a named 'function' tool, the only kind locally hosted models can be restricted to",
            )),
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(ToolChoice::AllowedTools(AllowedToolsToolChoice {
        tp: AllowedToolsToolChoiceType::AllowedTools,
        mode,
        tools,
    }))
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
            strict: None,
        },
    }
}

#[must_use]
pub fn convert_tool(x: &ChatCompletionTools) -> Tool {
    match x {
        ChatCompletionTools::Function(tool) => Tool {
            tp: ToolType::Function,
            function: Function {
                description: tool.function.description.clone(),
                name: tool.function.name.clone(),
                parameters: tool
                    .function
                    .parameters
                    .clone()
                    .and_then(|p| p.as_object().map(|p| HashMap::from_iter(p.clone()))),
                strict: None,
            },
        },
        ChatCompletionTools::Custom(_) => Tool {
            tp: ToolType::Function,
            function: Function {
                description: None,
                name: String::new(),
                parameters: None,
                strict: None,
            },
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
        r#type: Some(FunctionType::Function),
        function: Some(FunctionCallStream {
            name: Some(r.function.name.clone()),
            arguments: Some(r.function.arguments.clone()),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn max_tokens_limits_the_completion() {
        let req = request(serde_json::json!({
            "model": "m",
            "messages": [{"role": "user", "content": "hi"}],
            "max_tokens": 512
        }));
        assert_eq!(MistralLlama::completion_limit(&req), Some(512));
    }

    #[test]
    fn max_completion_tokens_wins_when_both_are_set() {
        let req = request(serde_json::json!({
            "model": "m",
            "messages": [{"role": "user", "content": "hi"}],
            "max_tokens": 512,
            "max_completion_tokens": 2048
        }));
        assert_eq!(MistralLlama::completion_limit(&req), Some(2048));
    }

    #[test]
    fn no_limit_is_invented_when_neither_is_set() {
        let req = request(serde_json::json!({
            "model": "m",
            "messages": [{"role": "user", "content": "hi"}]
        }));
        assert_eq!(MistralLlama::completion_limit(&req), None);
    }

    fn choice(json: serde_json::Value) -> ChatCompletionToolChoiceOption {
        serde_json::from_value(json).expect("tool_choice should deserialize")
    }

    fn invalid_argument<T: std::fmt::Debug>(result: Result<T, OpenAIError>) -> String {
        match result {
            Err(OpenAIError::InvalidArgument(message)) => message,
            other => panic!("expected an invalid-argument error, got {other:?}"),
        }
    }

    // regression test for #14230: `required` reached an `unimplemented!` and aborted the request.
    #[test]
    fn required_maps_to_required() {
        let converted = convert_tool_choice(&choice(serde_json::json!("required")))
            .expect("required is supported");
        assert!(matches!(converted, ToolChoice::Required), "{converted:?}");
    }

    #[test]
    fn modes_map_to_the_same_mode() {
        let none = convert_tool_choice(&choice(serde_json::json!("none"))).expect("none");
        assert!(matches!(none, ToolChoice::None), "{none:?}");
        let auto = convert_tool_choice(&choice(serde_json::json!("auto"))).expect("auto");
        assert!(matches!(auto, ToolChoice::Auto), "{auto:?}");
    }

    #[test]
    fn named_function_maps_to_that_tool() {
        let converted = convert_tool_choice(&choice(serde_json::json!({
            "type": "function",
            "function": { "name": "get_weather" }
        })))
        .expect("a named function is supported");
        let ToolChoice::Tool(tool) = converted else {
            panic!("expected a forced tool, got {converted:?}");
        };
        assert_eq!(tool.function.name, "get_weather");
    }

    /// `allowed_tools` was mapped to `none`, so a request restricting the model to — or requiring
    /// — a subset of its tools was answered with tools disabled altogether.
    #[test]
    fn allowed_tools_keeps_its_mode_and_tools() {
        let converted = convert_tool_choice(&choice(serde_json::json!({
            "type": "allowed_tools",
            "allowed_tools": {
                "mode": "required",
                "tools": [
                    { "type": "function", "function": { "name": "get_weather" } },
                    { "type": "function", "function": { "name": "get_time" } }
                ]
            }
        })))
        .expect("function tools with one mode are supported");
        let ToolChoice::AllowedTools(allowed) = converted else {
            panic!("expected allowed tools, got {converted:?}");
        };
        assert_eq!(allowed.mode, AllowedToolsMode::Required);
        let names: Vec<_> = allowed
            .tools
            .iter()
            .map(|tool| match tool {
                AllowedToolChoice::Function { name } => name.as_str(),
                other => panic!("expected a function tool, got {other:?}"),
            })
            .collect();
        assert_eq!(names, ["get_weather", "get_time"]);
    }

    #[test]
    fn allowed_tools_that_cannot_be_enforced_are_refused() {
        let hosted = invalid_argument(convert_tool_choice(&choice(serde_json::json!({
            "type": "allowed_tools",
            "allowed_tools": { "mode": "auto", "tools": [{ "type": "web_search" }] }
        }))));
        assert_eq!(
            hosted,
            format!(
                "tool_choice 'allowed_tools' lists a tool that is not a named 'function' tool, the only kind locally hosted models can be restricted to. See: {TOOL_CHOICE_DOCS}"
            )
        );

        let empty = invalid_argument(convert_tool_choice(&choice(serde_json::json!({
            "type": "allowed_tools",
            "allowed_tools": { "mode": "auto", "tools": [] }
        }))));
        assert_eq!(
            empty,
            format!("tool_choice 'allowed_tools' lists no tools. See: {TOOL_CHOICE_DOCS}")
        );
    }

    /// `mistral.rs` fails a choice naming a tool the request does not offer as an internal
    /// error; the caller named the tool, so it is refused as an invalid argument first.
    #[test]
    fn a_choice_naming_a_tool_the_request_does_not_offer_is_refused() {
        let messages = serde_json::json!([{ "role": "user", "content": "hi" }]);
        let tools = serde_json::json!([{ "type": "function", "function": { "name": "a" } }]);
        for tool_choice in [
            serde_json::json!({ "type": "function", "function": { "name": "nope" } }),
            serde_json::json!({
                "type": "allowed_tools",
                "allowed_tools": {
                    "mode": "required",
                    "tools": [
                        { "type": "function", "function": { "name": "a" } },
                        { "type": "function", "function": { "name": "nope" } }
                    ]
                }
            }),
        ] {
            let message = invalid_argument(request_tool_choice(&request(serde_json::json!({
                "model": "m", "messages": messages, "tools": tools, "tool_choice": tool_choice
            }))));
            assert_eq!(
                message,
                format!(
                    "tool_choice names tool 'nope', which the request does not list in 'tools'. Add 'nope' to 'tools', or name one of the tools the request lists. See: {TOOL_CHOICE_DOCS}"
                ),
                "{tool_choice}"
            );
        }

        let offered = request_tool_choice(&request(serde_json::json!({
            "model": "m",
            "messages": messages,
            "tools": tools,
            "tool_choice": { "type": "function", "function": { "name": "a" } }
        })))
        .expect("a tool the request offers is supported");
        let Some(ToolChoice::Tool(tool)) = offered else {
            panic!("expected a forced tool, got {offered:?}");
        };
        assert_eq!(tool.function.name, "a");
    }

    /// GLM-5.3-Flash's template as rendered for a one-message conversation: it takes
    /// `low` and `high`, maps every other value to `Max`, and always opens `<think>`.
    fn glm_5_3_render(_: Option<bool>, effort: Option<TemplateReasoningEffort>) -> String {
        let shown = match effort {
            Some(TemplateReasoningEffort::Low) => "Low",
            Some(TemplateReasoningEffort::High) => "High",
            _ => "Max",
        };
        format!("[gMASK]<sop><|system|>Reasoning Effort: {shown}<|user|>Hi<|assistant|><think>")
    }

    /// A Qwen3 template: thinking off closes an empty block; any level only turns it on.
    fn qwen3_render(enable_thinking: Option<bool>, _: Option<TemplateReasoningEffort>) -> String {
        let close = if enable_thinking == Some(false) {
            "<think>\n\n</think>\n\n"
        } else {
            ""
        };
        format!("<|im_start|>user\nHi<|im_end|>\n<|im_start|>assistant\n{close}")
    }

    /// A `DeepSeek-V4.1` template: answers directly unless asked, closing the block up front.
    fn deepseek_v4_1_render(
        enable_thinking: Option<bool>,
        effort: Option<TemplateReasoningEffort>,
    ) -> String {
        let thinks = enable_thinking.unwrap_or(effort.is_some());
        let open = if thinks { "<think>" } else { "</think>" };
        format!("<｜begin▁of▁sentence｜><｜User｜>Hi<｜Assistant｜>{open}")
    }

    /// A Gemma 4 template: `<|think|>` turns reasoning on.
    fn gemma_4_render(
        enable_thinking: Option<bool>,
        effort: Option<TemplateReasoningEffort>,
    ) -> String {
        let think = if enable_thinking.unwrap_or(effort.is_some()) {
            "<|think|>"
        } else {
            ""
        };
        format!("{think}<bos><|turn>user\nHi<turn|>\n<|turn>model\n")
    }

    /// gpt-oss's template: it ignores `enable_thinking`, defaults to `medium`, and renders
    /// whatever level it is given as `Reasoning: <level>`.
    fn gpt_oss_render(_: Option<bool>, effort: Option<TemplateReasoningEffort>) -> String {
        let level = effort.map_or("medium", TemplateReasoningEffort::as_str);
        format!(
            "<|start|>system<|message|>Reasoning: {level}\n\n<|end|><|start|>user<|message|>Hi<|end|><|start|>assistant"
        )
    }

    /// A template with no reasoning control, as Llama 3's: every control renders alike.
    fn llama_3_render(_: Option<bool>, _: Option<TemplateReasoningEffort>) -> String {
        "<|start_header_id|>user<|end_header_id|>\n\nHi<|eot_id|><|start_header_id|>assistant<|end_header_id|>\n\n".to_string()
    }

    /// Qwen3.8-Flash-Next's template: it takes `low`, `medium` and its default `xhigh`, adds
    /// instructions naming only `low` and `xhigh`, and raises on any other level.
    fn qwen_3_8_render(
        enable_thinking: Option<bool>,
        effort: Option<TemplateReasoningEffort>,
    ) -> Option<String> {
        let (instructions, open) = if enable_thinking == Some(false) {
            ("", "<think>\n\n</think>\n\n")
        } else {
            let instructions = match effort.unwrap_or(TemplateReasoningEffort::XHigh) {
                TemplateReasoningEffort::XHigh => {
                    "<|im_start|>system\nReasoning effort is set to xhigh. Please think carefully through the task.<|im_end|>\n"
                }
                TemplateReasoningEffort::Low => {
                    "<|im_start|>system\nReasoning effort is set to low. Keep your thinking brief and focused.<|im_end|>\n"
                }
                TemplateReasoningEffort::Medium => "",
                _ => return None,
            };
            (instructions, "<think>\n")
        };
        Some(format!(
            "{instructions}<|im_start|>user\nHi<|im_end|>\n<|im_start|>assistant\n{open}"
        ))
    }

    /// Classifies a template from one of the renders above, none of which fails.
    fn classify(
        render: fn(Option<bool>, Option<TemplateReasoningEffort>) -> String,
    ) -> ReasoningSupport {
        ReasoningSupport::from_renders(|enable_thinking, effort| {
            Some(render(enable_thinking, effort))
        })
    }

    fn effort(json: serde_json::Value) -> ReasoningEffort {
        serde_json::from_value(json).expect("reasoning_effort should deserialize")
    }

    #[test]
    fn glm_5_3_always_reasons_and_names_only_low_and_high() {
        assert_eq!(
            classify(glm_5_3_render),
            ReasoningSupport {
                always_reasons: true,
                levels: Some(vec![
                    TemplateReasoningEffort::Low,
                    TemplateReasoningEffort::High
                ]),
            }
        );
    }

    #[test]
    fn templates_with_an_off_switch_take_none_and_every_level() {
        for (name, render) in [
            ("qwen3", qwen3_render as fn(_, _) -> _),
            ("deepseek-v4.1", deepseek_v4_1_render),
            ("gemma-4", gemma_4_render),
        ] {
            assert_eq!(
                classify(render),
                ReasoningSupport {
                    always_reasons: false,
                    levels: None
                },
                "{name}"
            );
        }
    }

    #[test]
    fn gpt_oss_always_reasons_and_names_every_level() {
        assert_eq!(
            classify(gpt_oss_render),
            ReasoningSupport {
                always_reasons: true,
                levels: Some(REASONING_LEVELS.to_vec()),
            }
        );
    }

    #[test]
    fn a_template_without_reasoning_controls_takes_any_value() {
        assert_eq!(classify(llama_3_render), ReasoningSupport::default());
    }

    // Regression test: Qwen3.8-Flash-Next's template raises on `high`, which made the whole
    // template unchecked, so a request for `high` failed in the template with HTTP 500.
    #[test]
    fn qwen_3_8_takes_the_levels_it_renders_and_not_high() {
        assert_eq!(
            ReasoningSupport::from_renders(qwen_3_8_render),
            ReasoningSupport {
                always_reasons: false,
                levels: Some(vec![
                    TemplateReasoningEffort::Low,
                    TemplateReasoningEffort::Medium,
                    TemplateReasoningEffort::XHigh
                ]),
            }
        );
    }

    #[test]
    fn a_level_the_template_fails_to_render_is_refused_with_the_levels_it_takes() {
        let support = ReasoningSupport::from_renders(qwen_3_8_render);
        assert_eq!(
            reasoning_controls(Some(&ReasoningEffort::High), &support, "qwen3.8-flash-next"),
            Err("Model 'qwen3.8-flash-next' takes `reasoning_effort` 'low', 'medium' or 'xhigh', and ignores 'high'. Set 'low', 'medium' or 'xhigh', or omit `reasoning_effort` for the model's default. See: https://spiceai.org/docs/components/models".to_string())
        );
        let accepted = [
            ("none", Some(false), None),
            ("minimal", None, Some(TemplateReasoningEffort::Low)),
            ("medium", None, Some(TemplateReasoningEffort::Medium)),
            ("xhigh", None, Some(TemplateReasoningEffort::XHigh)),
        ];
        for (value, enable_thinking, reasoning_effort) in accepted {
            assert_eq!(
                reasoning_controls(
                    Some(&effort(serde_json::json!(value))),
                    &support,
                    "qwen3.8-flash-next"
                ),
                Ok(ReasoningControls {
                    enable_thinking,
                    reasoning_effort
                }),
                "{value}"
            );
        }
    }

    #[test]
    fn a_template_that_encodes_levels_without_naming_them_takes_every_level() {
        let render = |enable_thinking: Option<bool>, effort: Option<TemplateReasoningEffort>| {
            let instruction = match (enable_thinking, effort) {
                (Some(false), _) => "Answer directly.",
                (_, Some(TemplateReasoningEffort::Low)) => "Think briefly.",
                (_, Some(TemplateReasoningEffort::High | TemplateReasoningEffort::XHigh)) => {
                    "Think hard."
                }
                _ => "Think.",
            };
            Some(format!("{instruction}<|user|>Hi<|assistant|>"))
        };
        assert_eq!(
            ReasoningSupport::from_renders(render),
            ReasoningSupport::default()
        );
    }

    #[test]
    fn a_template_that_renders_no_level_is_left_unchecked() {
        assert_eq!(
            ReasoningSupport::from_renders(|_, effort| effort.is_none().then(|| "Hi".to_string())),
            ReasoningSupport::default()
        );
    }

    #[test]
    fn a_template_that_fails_to_render_is_left_unchecked() {
        assert_eq!(
            ReasoningSupport::from_renders(|_, _| None),
            ReasoningSupport::default()
        );
    }

    #[test]
    fn unset_reasoning_effort_leaves_the_template_default() {
        let support = classify(glm_5_3_render);
        assert_eq!(
            reasoning_controls(None, &support, "glm-5.3-flash"),
            Ok(ReasoningControls::default())
        );
    }

    #[test]
    fn reasoning_effort_maps_to_template_controls() {
        let support = classify(qwen3_render);
        let cases = [
            ("none", Some(false), None),
            ("minimal", None, Some(TemplateReasoningEffort::Low)),
            ("low", None, Some(TemplateReasoningEffort::Low)),
            ("medium", None, Some(TemplateReasoningEffort::Medium)),
            ("high", None, Some(TemplateReasoningEffort::High)),
            ("xhigh", None, Some(TemplateReasoningEffort::XHigh)),
        ];
        for (value, enable_thinking, reasoning_effort) in cases {
            assert_eq!(
                reasoning_controls(Some(&effort(serde_json::json!(value))), &support, "qwen3"),
                Ok(ReasoningControls {
                    enable_thinking,
                    reasoning_effort
                }),
                "{value}"
            );
        }
    }

    #[test]
    fn none_is_refused_by_a_model_that_always_reasons() {
        let support = classify(glm_5_3_render);
        assert_eq!(
            reasoning_controls(Some(&ReasoningEffort::None), &support, "glm-5.3-flash"),
            Err("Model 'glm-5.3-flash' always reasons, so `reasoning_effort: none` can't turn reasoning off. Omit `reasoning_effort`, or set 'low' for the least reasoning. See: https://spiceai.org/docs/components/models".to_string())
        );
    }

    #[test]
    fn a_level_the_template_ignores_is_refused_with_the_levels_it_takes() {
        let support = classify(glm_5_3_render);
        for value in ["medium", "xhigh"] {
            assert_eq!(
                reasoning_controls(
                    Some(&effort(serde_json::json!(value))),
                    &support,
                    "glm-5.3-flash"
                ),
                Err(format!(
                    "Model 'glm-5.3-flash' takes `reasoning_effort` 'low' or 'high', and ignores '{value}'. Set 'low' or 'high', or omit `reasoning_effort` for the model's default. See: https://spiceai.org/docs/components/models"
                ))
            );
        }
        assert_eq!(
            reasoning_controls(Some(&ReasoningEffort::Minimal), &support, "glm-5.3-flash"),
            Ok(ReasoningControls {
                enable_thinking: None,
                reasoning_effort: Some(TemplateReasoningEffort::Low)
            })
        );
    }

    #[test]
    fn three_named_levels_are_listed_with_commas() {
        assert_eq!(
            ignored_level_message(
                "m",
                &[
                    TemplateReasoningEffort::Low,
                    TemplateReasoningEffort::Medium,
                    TemplateReasoningEffort::High
                ],
                "xhigh"
            ),
            "Model 'm' takes `reasoning_effort` 'low', 'medium' or 'high', and ignores 'xhigh'. Set 'low', 'medium' or 'high', or omit `reasoning_effort` for the model's default. See: https://spiceai.org/docs/components/models"
        );
    }

    fn request(json: serde_json::Value) -> CreateChatCompletionRequest {
        serde_json::from_value(json).expect("request should deserialize")
    }

    /// With no tools to call, `mistral.rs` cannot enforce `required` and answers in prose.
    #[test]
    fn a_required_call_needs_tools() {
        let messages = serde_json::json!([{ "role": "user", "content": "hi" }]);
        for tool_choice in [
            serde_json::json!("required"),
            serde_json::json!({
                "type": "allowed_tools",
                "allowed_tools": { "mode": "required", "tools": [{ "type": "function", "function": { "name": "a" } }] }
            }),
        ] {
            let message = invalid_argument(request_tool_choice(&request(serde_json::json!({
                "model": "m", "messages": messages, "tool_choice": tool_choice
            }))));
            assert!(message.contains("lists no tools"), "{message}");
        }

        let with_tools = request_tool_choice(&request(serde_json::json!({
            "model": "m",
            "messages": messages,
            "tools": [{ "type": "function", "function": { "name": "a" } }],
            "tool_choice": "required"
        })))
        .expect("required with tools is supported");
        assert!(
            matches!(with_tools, Some(ToolChoice::Required)),
            "{with_tools:?}"
        );

        let auto = request_tool_choice(&request(serde_json::json!({
            "model": "m", "messages": messages, "tool_choice": "auto"
        })))
        .expect("auto needs no tools");
        assert!(matches!(auto, Some(ToolChoice::Auto)), "{auto:?}");
    }

    #[test]
    fn custom_is_refused() {
        let message = invalid_argument(convert_tool_choice(&choice(serde_json::json!({
            "type": "custom",
            "custom": { "name": "grammar_tool" }
        }))));
        assert!(message.contains("'custom' is not supported"), "{message}");
    }
}
