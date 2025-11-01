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

use super::{Chat, Error as ChatError, Result, nsql::SqlGeneration};
use async_openai::{
    error::{ApiError, OpenAIError},
    types::{
        ChatChoice, ChatCompletionResponseMessage, ChatCompletionResponseStream, CompletionUsage,
        CreateChatCompletionRequest, CreateChatCompletionResponse, FinishReason, Role,
    },
};
use async_trait::async_trait;
use futures::Stream;
use llama_cpp_2::{
    LogOptions,
    context::params::LlamaContextParams,
    llama_backend::LlamaBackend,
    llama_batch::LlamaBatch,
    model::params::LlamaModelParams,
    model::{AddBos, LlamaModel, Special},
    sampling::LlamaSampler,
    send_logs_to_tracing,
};
use secrecy::SecretString;
use std::{
    fmt::Write,
    path::{Path, PathBuf},
    pin::Pin,
    sync::{Arc, OnceLock},
    time::SystemTime,
};
use tokio_stream::wrappers::ReceiverStream;

use crate::streaming_utils::generate_stream_id;

/// Type alias for streaming result
type StreamResult = Pin<Box<dyn Stream<Item = Result<Option<String>>> + Send>>;

/// Initialize llama.cpp logging once globally.
///
/// By default, llama.cpp/ggml logs are filtered out (via `OFF_FILTERS` in bin/spiced/src/tracing.rs).
/// To enable llama.cpp logs, set the `SPICED_LOG` or `RUST_LOG` environment variable to include
/// `llama_cpp_2`, `llama.cpp`, or `ggml`. For example:
/// - `SPICED_LOG=llama_cpp_2=debug` - Enable debug logs from llama-cpp-2 bindings
/// - `SPICED_LOG=llama.cpp=info` - Enable info logs from llama.cpp library
/// - `SPICED_LOG=ggml=trace` - Enable trace logs from ggml library
fn init_llama_logging() {
    static INIT: OnceLock<()> = OnceLock::new();
    INIT.get_or_init(|| {
        // Check if verbose logging is enabled via SPICED_LOG or RUST_LOG
        let verbose = std::env::var("SPICED_LOG")
            .or_else(|_| std::env::var("RUST_LOG"))
            .map(|log_config| {
                // Enable llama.cpp logs if llama_cpp_2 is explicitly enabled in the log config
                log_config.contains("llama_cpp_2")
                    || log_config.contains("llama.cpp")
                    || log_config.contains("ggml")
            })
            .unwrap_or(false);

        // Send logs to tracing, but they'll be filtered by OFF_FILTERS unless explicitly enabled
        send_logs_to_tracing(LogOptions::default().with_logs_enabled(verbose));
    });
}

/// Sampling parameters for llama.cpp inference
#[derive(Debug, Clone)]
struct SamplingParams {
    temperature: f32,
    top_p: f32,
    top_k: i32,
    max_tokens: usize,
    frequency_penalty: f32,
    presence_penalty: f32,
    repeat_penalty: f32,
    n_ctx: u32,
}

impl Default for SamplingParams {
    fn default() -> Self {
        Self {
            temperature: 0.8,
            top_p: 0.95,
            top_k: 40,
            max_tokens: 512,
            frequency_penalty: 0.0,
            presence_penalty: 0.0,
            repeat_penalty: 1.1,
            // Default context size for llama.cpp models.
            // 2048 is a common default for many LLMs, balancing memory usage and context length.
            // This value is currently not user-configurable; making it user-settable is a planned future enhancement.
            n_ctx: 2048,
        }
    }
}

impl SamplingParams {
    /// Create sampling parameters from an `OpenAI` chat completion request
    fn from_request(req: &CreateChatCompletionRequest) -> Self {
        let mut params = Self::default();

        if let Some(temp) = req.temperature {
            params.temperature = temp;
        }

        if let Some(top_p) = req.top_p {
            params.top_p = top_p;
        }

        // Prefer max_completion_tokens (new field) over max_tokens (deprecated)
        if let Some(max_completion_tokens) = req.max_completion_tokens {
            params.max_tokens = max_completion_tokens as usize;
        } else {
            #[allow(deprecated)]
            if let Some(max_tokens) = req.max_tokens {
                params.max_tokens = max_tokens as usize;
            }
        }

        if let Some(freq_penalty) = req.frequency_penalty {
            params.frequency_penalty = freq_penalty;
        }

        if let Some(pres_penalty) = req.presence_penalty {
            params.presence_penalty = pres_penalty;
        }

        params
    }

    /// Create a sampler chain from these parameters
    fn create_sampler(&self) -> LlamaSampler {
        const EPSILON: f32 = 1e-6;
        let mut samplers = Vec::new();

        // Add penalties if configured
        if (self.repeat_penalty - 1.0).abs() > EPSILON
            || self.frequency_penalty.abs() > EPSILON
            || self.presence_penalty.abs() > EPSILON
        {
            // penalty_last_n: number of tokens to consider for repetition penalty (64 is reasonable)
            // penalty_repeat: base repetition penalty
            // penalty_freq: frequency penalty (maps to OpenAI frequency_penalty)
            // penalty_present: presence penalty (maps to OpenAI presence_penalty)
            samplers.push(LlamaSampler::penalties(
                64,
                self.repeat_penalty,
                self.frequency_penalty,
                self.presence_penalty,
            ));
        }

        // Add top-k sampling if enabled
        if self.top_k > 0 {
            samplers.push(LlamaSampler::top_k(self.top_k));
        }

        // Add top-p (nucleus) sampling
        if self.top_p < 1.0 {
            samplers.push(LlamaSampler::top_p(self.top_p, 1));
        }

        // Add temperature sampling
        if self.temperature > 0.0 {
            samplers.push(LlamaSampler::temp(self.temperature));
        } else {
            // Temperature 0 means greedy sampling
            return LlamaSampler::greedy();
        }

        // Chain samplers together if we have multiple
        if samplers.is_empty() {
            LlamaSampler::greedy()
        } else if samplers.len() == 1 {
            // We've verified samplers.len() == 1, so pop() is guaranteed to return Some
            match samplers.pop() {
                Some(sampler) => sampler,
                None => unreachable!("samplers.len() == 1 guarantees pop() returns Some"),
            }
        } else {
            LlamaSampler::chain_simple(samplers)
        }
    }
}

pub struct LlamaCpp {
    model: Arc<LlamaModel>,
    backend: Arc<LlamaBackend>,
    model_name: String,
}

impl LlamaCpp {
    /// Create a new `LlamaCpp` instance from a model file path
    pub async fn from_file(model_path: &Path) -> Result<Self> {
        // Initialize logging configuration once
        init_llama_logging();

        if !model_path.exists() {
            return Err(ChatError::LocalModelNotFound {
                expected_path: model_path.to_string_lossy().to_string(),
            });
        }

        // Load model on blocking thread pool since it's CPU-intensive
        let model_path_owned = model_path.to_path_buf();
        let backend = Arc::new(
            LlamaBackend::init().map_err(|e| ChatError::FailedToLoadModel {
                source: format!("Failed to initialize llama backend: {e}").into(),
            })?,
        );

        let backend_clone = Arc::clone(&backend);
        let model = tokio::task::spawn_blocking(move || {
            let params = LlamaModelParams::default();
            LlamaModel::load_from_file(backend_clone.as_ref(), &model_path_owned, &params).map_err(
                |e| ChatError::FailedToLoadModel {
                    source: Box::new(e) as Box<dyn std::error::Error + Send + Sync>,
                },
            )
        })
        .await
        .map_err(|e| ChatError::FailedToLoadModel {
            source: Box::new(e) as Box<dyn std::error::Error + Send + Sync>,
        })??;

        Ok(Self {
            model: Arc::new(model),
            backend,
            // Extract model name from file path for display/logging purposes.
            // Falls back to "unknown" if the path has no filename or contains invalid UTF-8.
            model_name: model_path
                .file_name()
                .and_then(|n| n.to_str())
                .map_or_else(|| "unknown".to_string(), ToString::to_string),
        })
    }

    /// Create from `HuggingFace` model ID
    ///
    /// Note: llama.cpp engine does not support automatic model downloading from `HuggingFace`.
    /// The `model_id` parameter is accepted for API consistency but is not used.
    /// Users must pre-download GGUF files and specify the local path via `gguf_filename`.
    /// The `hf_token_literal` is also unused as no downloading occurs.
    pub async fn from_hf(
        _model_id: &str,
        _hf_token_literal: Option<&SecretString>,
        gguf_filename: Option<PathBuf>,
    ) -> Result<Self> {
        // llama.cpp requires local GGUF files
        // Users should download the model first or specify the local path
        let model_path = gguf_filename.ok_or_else(|| ChatError::FailedToLoadModel {
            source: "llama.cpp engine requires a local GGUF file path. \
                    Please download the model and specify the file path in model files configuration."
                .into(),
        })?;

        Self::from_file(&model_path).await
    }

    /// Create from local file paths
    pub async fn from(model_weights: &[PathBuf]) -> Result<Self> {
        if model_weights.is_empty() {
            return Err(ChatError::FailedToLoadModel {
                source: "No model weights provided".into(),
            });
        }

        // Use first weight file (GGUF format)
        Self::from_file(&model_weights[0]).await
    }

    /// Convert `OpenAI` chat request messages to a prompt string
    fn messages_to_prompt(req: &CreateChatCompletionRequest) -> String {
        let mut prompt = String::new();

        for message in &req.messages {
            match message {
                async_openai::types::ChatCompletionRequestMessage::System(msg) => {
                    let content_str = match &msg.content {
                        async_openai::types::ChatCompletionRequestSystemMessageContent::Text(
                            text,
                        ) => text.clone(),
                        async_openai::types::ChatCompletionRequestSystemMessageContent::Array(
                            parts,
                        ) => {
                            let mut text = String::new();
                            for part in parts {
                                let async_openai::types::ChatCompletionRequestSystemMessageContentPart::Text(t) = part;
                                text.push_str(&t.text);
                            }
                            text
                        }
                    };
                    let _ = write!(prompt, "System: {content_str}\n\n");
                }
                async_openai::types::ChatCompletionRequestMessage::User(msg) => {
                    let content = match &msg.content {
                        async_openai::types::ChatCompletionRequestUserMessageContent::Text(
                            text,
                        ) => text.clone(),
                        async_openai::types::ChatCompletionRequestUserMessageContent::Array(
                            parts,
                        ) => {
                            let mut text = String::new();
                            for part in parts {
                                if let async_openai::types::ChatCompletionRequestUserMessageContentPart::Text(t) = part {
                                    text.push_str(&t.text);
                                }
                            }
                            text
                        }
                    };
                    let _ = write!(prompt, "User: {content}\n\n");
                }
                async_openai::types::ChatCompletionRequestMessage::Assistant(msg) => {
                    if let Some(content) = &msg.content {
                        let content_str = match content {
                            async_openai::types::ChatCompletionRequestAssistantMessageContent::Text(text) => text.clone(),
                            async_openai::types::ChatCompletionRequestAssistantMessageContent::Array(parts) => {
                                let mut text = String::new();
                                for part in parts {
                                    if let async_openai::types::ChatCompletionRequestAssistantMessageContentPart::Text(t) = part {
                                        text.push_str(&t.text);
                                    }
                                }
                                text
                            }
                        };
                        let _ = write!(prompt, "Assistant: {content_str}\n\n");
                    }
                }
                _ => {} // Ignore other message types for now
            }
        }

        prompt.push_str("Assistant:");
        prompt
    }

    /// Internal method for running inference with configurable sampling parameters
    async fn run_with_params(
        &self,
        prompt: String,
        params: &SamplingParams,
    ) -> Result<Option<String>> {
        let model = Arc::clone(&self.model);
        let backend = Arc::clone(&self.backend);
        let params_clone = params.clone();

        tokio::task::spawn_blocking(move || {
            // Create context for inference with configured context size
            let ctx_params = LlamaContextParams::default()
                .with_n_ctx(std::num::NonZeroU32::new(params_clone.n_ctx))
                .with_n_batch(512);

            let mut ctx = model.new_context(&backend, ctx_params).map_err(|e| {
                ChatError::FailedToRunModel {
                    source: Box::new(e) as Box<dyn std::error::Error + Send + Sync>,
                }
            })?;

            // Tokenize the prompt
            let tokens = model.str_to_token(&prompt, AddBos::Always).map_err(|e| {
                ChatError::FailedToRunModel {
                    source: Box::new(e) as Box<dyn std::error::Error + Send + Sync>,
                }
            })?;

            if tokens.is_empty() {
                return Ok(Some(String::new()));
            }

            // Create batch for the prompt
            // We only request logits for the last token to save computation
            let mut batch = LlamaBatch::new(512, 1);
            let n_prompt_tokens = tokens.len();

            for (i, &token) in tokens.iter().enumerate() {
                let is_last = i == n_prompt_tokens - 1;
                let i_i32 = i32::try_from(i).map_err(|e| ChatError::FailedToRunModel {
                    source: Box::new(e) as Box<dyn std::error::Error + Send + Sync>,
                })?;
                batch.add(token, i_i32, &[0], is_last).map_err(|e| {
                    ChatError::FailedToRunModel {
                        source: Box::new(e) as Box<dyn std::error::Error + Send + Sync>,
                    }
                })?;
            }

            // Decode the prompt
            ctx.decode(&mut batch)
                .map_err(|e| ChatError::FailedToRunModel {
                    source: Box::new(e) as Box<dyn std::error::Error + Send + Sync>,
                })?;

            // Create sampler from parameters
            let mut sampler = params_clone.create_sampler();

            let mut output = String::new();
            let mut n_generated = 0;
            let mut n_cur =
                i32::try_from(n_prompt_tokens).map_err(|e| ChatError::FailedToRunModel {
                    source: Box::new(e) as Box<dyn std::error::Error + Send + Sync>,
                })?;

            // Generate tokens
            while n_generated < params_clone.max_tokens {
                // Sample the next token from the last position
                let new_token = sampler.sample(&ctx, -1);

                // Check for end of generation
                if model.is_eog_token(new_token) {
                    break;
                }

                // Convert token to string
                match model.token_to_str(new_token, Special::Tokenize) {
                    Ok(token_str) => output.push_str(&token_str),
                    Err(_) => continue, // Skip tokens that can't be converted
                }

                // Accept the token in the sampler
                sampler.accept(new_token);

                // Prepare next batch with the new token at the next position
                batch.clear();
                batch.add(new_token, n_cur, &[0], true).map_err(|e| {
                    ChatError::FailedToRunModel {
                        source: Box::new(e) as Box<dyn std::error::Error + Send + Sync>,
                    }
                })?;

                // Decode the new token
                ctx.decode(&mut batch)
                    .map_err(|e| ChatError::FailedToRunModel {
                        source: Box::new(e) as Box<dyn std::error::Error + Send + Sync>,
                    })?;

                n_cur += 1;
                n_generated += 1;
            }

            Ok(Some(output))
        })
        .await
        .map_err(|e| ChatError::FailedToRunModel {
            source: Box::new(e) as Box<dyn std::error::Error + Send + Sync>,
        })?
    }

    /// Internal method for streaming inference with configurable sampling parameters
    fn stream_with_params(&self, prompt: String, params: &SamplingParams) -> StreamResult {
        let model = Arc::clone(&self.model);
        let backend = Arc::clone(&self.backend);
        let params_clone = params.clone();

        // Create a channel to send tokens from the blocking thread
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<Option<String>>>(100);

        tokio::task::spawn_blocking(move || {
            // Create context for inference with configured context size
            let ctx_params = LlamaContextParams::default()
                .with_n_ctx(std::num::NonZeroU32::new(params_clone.n_ctx))
                .with_n_batch(512);

            let mut ctx = match model.new_context(&backend, ctx_params) {
                Ok(ctx) => ctx,
                Err(e) => {
                    let _ = tx.blocking_send(Err(ChatError::FailedToRunModel {
                        source: Box::new(e) as Box<dyn std::error::Error + Send + Sync>,
                    }));
                    return;
                }
            };

            // Tokenize the prompt
            let tokens = match model.str_to_token(&prompt, AddBos::Always) {
                Ok(tokens) => tokens,
                Err(e) => {
                    let _ = tx.blocking_send(Err(ChatError::FailedToRunModel {
                        source: Box::new(e) as Box<dyn std::error::Error + Send + Sync>,
                    }));
                    return;
                }
            };

            if tokens.is_empty() {
                return;
            }

            // Create batch for the prompt
            // We only request logits for the last token to save computation
            let mut batch = LlamaBatch::new(512, 1);
            let n_prompt_tokens = tokens.len();

            for (i, &token) in tokens.iter().enumerate() {
                let is_last = i == n_prompt_tokens - 1;
                let i_i32 = match i32::try_from(i) {
                    Ok(v) => v,
                    Err(e) => {
                        let _ = tx.blocking_send(Err(ChatError::FailedToRunModel {
                            source: Box::new(e) as Box<dyn std::error::Error + Send + Sync>,
                        }));
                        return;
                    }
                };
                if let Err(e) = batch.add(token, i_i32, &[0], is_last) {
                    let _ = tx.blocking_send(Err(ChatError::FailedToRunModel {
                        source: Box::new(e) as Box<dyn std::error::Error + Send + Sync>,
                    }));
                    return;
                }
            }

            // Decode the prompt
            if let Err(e) = ctx.decode(&mut batch) {
                let _ = tx.blocking_send(Err(ChatError::FailedToRunModel {
                    source: Box::new(e) as Box<dyn std::error::Error + Send + Sync>,
                }));
                return;
            }

            // Create sampler from parameters
            let mut sampler = params_clone.create_sampler();

            let mut n_generated = 0;
            let mut n_cur = match i32::try_from(n_prompt_tokens) {
                Ok(v) => v,
                Err(e) => {
                    let _ = tx.blocking_send(Err(ChatError::FailedToRunModel {
                        source: Box::new(e) as Box<dyn std::error::Error + Send + Sync>,
                    }));
                    return;
                }
            };

            // Generate and stream tokens
            while n_generated < params_clone.max_tokens {
                // Sample the next token from the last position
                let new_token = sampler.sample(&ctx, -1);

                // Check for end of generation
                if model.is_eog_token(new_token) {
                    break;
                }

                // Convert token to string and send it
                match model.token_to_str(new_token, Special::Tokenize) {
                    Ok(token_str) => {
                        if tx.blocking_send(Ok(Some(token_str))).is_err() {
                            // Receiver dropped, stop generation
                            break;
                        }
                    }
                    Err(_) => continue, // Skip tokens that can't be converted
                }

                // Accept the token in the sampler
                sampler.accept(new_token);

                // Prepare next batch with the new token at the next position
                batch.clear();
                if let Err(e) = batch.add(new_token, n_cur, &[0], true) {
                    let _ = tx.blocking_send(Err(ChatError::FailedToRunModel {
                        source: Box::new(e) as Box<dyn std::error::Error + Send + Sync>,
                    }));
                    break;
                }

                // Decode the new token
                if let Err(e) = ctx.decode(&mut batch) {
                    let _ = tx.blocking_send(Err(ChatError::FailedToRunModel {
                        source: Box::new(e) as Box<dyn std::error::Error + Send + Sync>,
                    }));
                    break;
                }

                n_cur += 1;
                n_generated += 1;
            }
        });

        // Create stream from receiver using ReceiverStream (avoids async_stream::stream! macro)
        let stream = ReceiverStream::new(rx);

        Box::pin(stream)
    }
}

#[async_trait]
impl Chat for LlamaCpp {
    fn as_sql(&self) -> Option<&dyn SqlGeneration> {
        None
    }

    async fn health(&self) -> Result<()> {
        // Health check: Try to run a simple inference
        tracing::debug!(
            "Running health check for llama.cpp model '{}'",
            self.model_name
        );

        match self.run("ping".to_string()).await {
            Ok(_) => {
                tracing::info!(
                    "Health check passed for llama.cpp model '{}'",
                    self.model_name
                );
                Ok(())
            }
            Err(e) => {
                tracing::error!(
                    "Health check failed for llama.cpp model '{}': {}",
                    self.model_name,
                    e
                );
                Err(e)
            }
        }
    }

    async fn stream<'a>(
        &self,
        _prompt: String,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Option<String>>> + Send>>> {
        // Use default sampling parameters for trait method
        let params = SamplingParams::default();
        Ok(self.stream_with_params(_prompt, &params))
    }

    async fn run(&self, _prompt: String) -> Result<Option<String>> {
        // Use default sampling parameters for trait method
        let params = SamplingParams::default();
        self.run_with_params(_prompt, &params).await
    }

    async fn chat_stream(
        &self,
        req: CreateChatCompletionRequest,
    ) -> Result<ChatCompletionResponseStream, OpenAIError> {
        let prompt = Self::messages_to_prompt(&req);
        tracing::debug!("Generated prompt for llama.cpp streaming: {}", prompt);

        let model_id = req.model.clone();

        // Extract sampling parameters from request
        let params = SamplingParams::from_request(&req);

        // Use the stream() method with parameters
        let stream = self.stream_with_params(prompt, &params);

        Ok(crate::streaming_utils::string_stream_to_chat_stream(
            model_id, stream,
        ))
    }

    async fn chat_request(
        &self,
        req: CreateChatCompletionRequest,
    ) -> Result<CreateChatCompletionResponse, OpenAIError> {
        let prompt = Self::messages_to_prompt(&req);
        tracing::debug!("Generated prompt for llama.cpp: {}", prompt);

        // Extract sampling parameters from request
        let params = SamplingParams::from_request(&req);

        // Count prompt tokens
        let prompt_tokens = {
            let model = Arc::clone(&self.model);
            let prompt_clone = prompt.clone();
            match tokio::task::spawn_blocking(move || {
                model
                    .str_to_token(&prompt_clone, AddBos::Always)
                    .map(|tokens| u32::try_from(tokens.len()).unwrap_or(u32::MAX))
            })
            .await
            {
                Ok(Ok(count)) => count,
                Ok(Err(e)) => {
                    tracing::warn!("Failed to tokenize prompt for token counting: {}", e);
                    0
                }
                Err(e) => {
                    tracing::warn!(
                        "Failed to spawn blocking task for prompt tokenization: {}",
                        e
                    );
                    0
                }
            }
        };

        // Use the run() method with parameters to get the completion
        let response_text = self.run_with_params(prompt, &params).await.map_err(|e| {
            OpenAIError::ApiError(ApiError {
                message: e.to_string(),
                r#type: None,
                param: None,
                code: None,
            })
        })?;

        // response_text is Option<String> from run_with_params.
        // None indicates empty generation (e.g., immediate EOS token).
        let content = response_text.unwrap_or_default();

        // Count completion tokens
        let completion_tokens = {
            let model = Arc::clone(&self.model);
            let content_clone = content.clone();
            match tokio::task::spawn_blocking(move || {
                model
                    .str_to_token(&content_clone, AddBos::Never)
                    .map(|tokens| u32::try_from(tokens.len()).unwrap_or(u32::MAX))
            })
            .await
            {
                Ok(Ok(count)) => count,
                Ok(Err(e)) => {
                    tracing::warn!("Failed to tokenize completion for token counting: {}", e);
                    0
                }
                Err(e) => {
                    tracing::warn!(
                        "Failed to spawn blocking task for completion tokenization: {}",
                        e
                    );
                    0
                }
            }
        };

        let created = u32::try_from(
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .map_err(|e| OpenAIError::InvalidArgument(e.to_string()))?
                .as_secs(),
        )
        .unwrap_or(0);

        let stream_id = generate_stream_id(&self.model_name);

        let response = CreateChatCompletionResponse {
            id: stream_id,
            object: "chat.completion".to_string(),
            created,
            model: self.model_name.clone(),
            choices: vec![ChatChoice {
                index: 0,
                message: ChatCompletionResponseMessage {
                    content: Some(content),
                    role: Role::Assistant,
                    #[allow(deprecated)]
                    function_call: None,
                    tool_calls: None,
                    refusal: None,
                    audio: None,
                },
                finish_reason: Some(FinishReason::Stop),
                logprobs: None,
            }],
            usage: Some(CompletionUsage {
                prompt_tokens,
                completion_tokens,
                total_tokens: prompt_tokens + completion_tokens,
                completion_tokens_details: None,
                prompt_tokens_details: None,
            }),
            system_fingerprint: None,
            service_tier: None,
        };

        Ok(response)
    }
}
