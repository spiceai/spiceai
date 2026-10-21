/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! Serves a single-node GGUF model with llama.cpp.
//!
//! The prompt is the one the mistral.rs engine would send: the same messages
//! ([`message_to_mistral`]) rendered by the same chat-template code
//! ([`apply_chat_template_to`]), with `reasoning_effort` checked by the same
//! [`ReasoningSupport`]. llama.cpp only runs the forward passes and samples.
//!
//! One thread owns the backend, the model and its context. Requests wait on a channel and
//! run one at a time from an empty KV cache, and their tokens stream back over a Tokio
//! channel, so a forward pass never blocks an async worker.

use std::{
    num::NonZeroU32,
    path::{Path, PathBuf},
    pin::Pin,
    sync::{
        OnceLock,
        atomic::{AtomicUsize, Ordering},
        mpsc as std_mpsc,
    },
    thread,
    time::{SystemTime, UNIX_EPOCH},
};

use async_openai::{
    error::{ApiError, OpenAIError},
    types::chat::{
        ChatChoice, ChatChoiceStream, ChatCompletionRequestMessage, ChatCompletionResponseMessage,
        ChatCompletionResponseStream, ChatCompletionStreamResponseDelta, CompletionUsage,
        CreateChatCompletionRequest, CreateChatCompletionResponse,
        CreateChatCompletionStreamResponse, FinishReason, Role, StopConfiguration,
    },
};
use async_trait::async_trait;
use either::Either;
use futures::Stream;
use indexmap::IndexMap;
use llama_cpp_2::{
    context::{
        LlamaContext,
        params::{LlamaContextParams, LlamaContextType},
    },
    llama_backend::LlamaBackend,
    llama_batch::LlamaBatch,
    model::{LlamaModel, params::LlamaModelParams},
    sampling::LlamaSampler,
    speculative::{MtpSpeculative, MtpSpeculativeParams},
    token::LlamaToken,
};
use mistralrs_core::{
    ChatTemplateValue, MessageContent, ReasoningEffort as TemplateReasoningEffort,
    apply_chat_template_to,
};
use tokio::sync::{mpsc, oneshot};

use super::mistral::{ReasoningControls, ReasoningSupport, opens_reasoning, reasoning_controls};
use super::{
    Chat, Error as ChatError, LocalModelOptions, Result, message_to_mistral, nsql::SqlGeneration,
};
use crate::streaming_utils::generate_stream_id;

/// Tokens decoded per forward pass while reading the prompt.
const PROMPT_BATCH: usize = 512;

/// The context size when the model sets no `context_length`.
const DEFAULT_CONTEXT: u32 = 8192;

/// Draft tokens an MTP head proposes per step. Two measured fastest for Qwen3.8-Flash-Next
/// on a DGX Spark (53.7 tok/s against 47.5 without a draft, and 42.4 with three).
const MTP_DRAFT: i32 = 2;

/// How many streamed tokens may wait for a slow client before decoding pauses.
const STREAM_BUFFER: usize = 64;

/// The opening and closing tags of a reasoning block.
const THINK_OPEN: &str = "<think>";
const THINK_CLOSE: &str = "</think>";

/// llama.cpp's backend is process-wide and can be initialized once.
static BACKEND: OnceLock<std::result::Result<LlamaBackend, String>> = OnceLock::new();

fn backend() -> std::result::Result<&'static LlamaBackend, String> {
    BACKEND
        .get_or_init(|| {
            llama_cpp_2::send_logs_to_tracing(llama_cpp_2::LogOptions::default());
            LlamaBackend::init().map_err(|e| e.to_string())
        })
        .as_ref()
        .map_err(Clone::clone)
}

pub struct LlamaCpp {
    jobs: std_mpsc::Sender<Job>,
    model_name: String,
    template: String,
    special: SpecialTokens,
    reasoning_support: ReasoningSupport,
    counter: AtomicUsize,
}

/// The token strings a chat template may refer to.
#[derive(Debug, Clone, Default)]
struct SpecialTokens {
    bos: Option<String>,
    eos: Option<String>,
}

/// What the decode thread reports once the model is loaded.
struct Loaded {
    template: Option<String>,
    special: SpecialTokens,
}

#[derive(Debug, Clone, Copy, Default)]
struct Sampling {
    temperature: Option<f32>,
    top_p: Option<f32>,
    seed: u32,
}

struct Job {
    prompt: String,
    limit: Option<usize>,
    sampling: Sampling,
    stop: Vec<String>,
    events: mpsc::Sender<Event>,
}

enum Event {
    /// The text one generated token added; empty while a multi-byte character or a
    /// possible stop sequence is still incomplete.
    Token(String),
    Done(Outcome),
    Failed(String),
}

#[derive(Debug, Clone, Copy)]
struct Outcome {
    finish: FinishReason,
    prompt_tokens: u32,
    completion_tokens: u32,
}

impl LlamaCpp {
    /// Loads a GGUF model. For a sharded GGUF, `weights` lists every shard and llama.cpp
    /// opens the rest from the first.
    pub async fn from(weights: &[PathBuf], options: LocalModelOptions<'_>) -> Result<Self> {
        for weight in weights {
            if !weight.exists() {
                return Err(ChatError::LocalModelNotFound {
                    expected_path: weight.to_string_lossy().to_string(),
                });
            }
        }
        // An MTP head ships as its own GGUF (`mtp-*.gguf`); it drafts for the model, so it is
        // loaded beside it rather than as one of its shards.
        let (drafts, trunk): (Vec<_>, Vec<_>) =
            weights.iter().cloned().partition(|w| is_mtp_head(w));
        let first = first_shard(&trunk).ok_or_else(|| ChatError::LocalModelNotFound {
            expected_path: String::new(),
        })?;
        let draft = drafts.into_iter().next();
        let n_ctx = options
            .context_length
            .and_then(|length| u32::try_from(length).ok())
            .and_then(NonZeroU32::new)
            .or(NonZeroU32::new(DEFAULT_CONTEXT));

        let (ready_tx, ready_rx) = oneshot::channel();
        let (jobs_tx, jobs_rx) = std_mpsc::channel();
        thread::Builder::new()
            .name("llama-cpp".to_string())
            .spawn(move || serve(&first, draft.as_deref(), n_ctx, ready_tx, &jobs_rx))
            .map_err(|e| ChatError::FailedToLoadModel {
                source: Box::new(e),
            })?;
        let loaded = ready_rx
            .await
            .map_err(|e| ChatError::FailedToLoadModel {
                source: Box::new(e),
            })?
            .map_err(|message| ChatError::FailedToLoadModel {
                source: message.into(),
            })?;

        // As on the mistral.rs path, a GGUF's own template wins over `chat_template`.
        let template = loaded
            .template
            .or_else(|| options.chat_template_literal.map(ToString::to_string))
            .ok_or_else(|| ChatError::FailedToLoadModel {
                source:
                    "the GGUF file carries no chat template, and the model sets no `chat_template`"
                        .into(),
            })?;

        let model_name = options.reasoning.model_name.to_string();
        let reasoning_support = {
            let template = template.clone();
            let special = loaded.special.clone();
            tokio::task::spawn_blocking(move || {
                ReasoningSupport::from_renders(|enable_thinking, effort| {
                    let message = IndexMap::from([
                        ("role".to_string(), Either::Left("user".to_string())),
                        ("content".to_string(), Either::Left("Hi".to_string())),
                    ]);
                    render(&template, &special, vec![message], enable_thinking, effort).ok()
                })
            })
            .await
            .unwrap_or_default()
        };
        if let Some(effort) = options.reasoning.effort {
            reasoning_controls(Some(effort), &reasoning_support, &model_name).map_err(
                |message| ChatError::InvalidParamValueError {
                    param: "reasoning_effort".to_string(),
                    message,
                },
            )?;
        }

        Ok(Self {
            jobs: jobs_tx,
            model_name,
            template,
            special: loaded.special,
            reasoning_support,
            counter: AtomicUsize::new(0),
        })
    }

    fn prompt(
        &self,
        messages: &[ChatCompletionRequestMessage],
        reasoning: ReasoningControls,
    ) -> std::result::Result<String, OpenAIError> {
        render(
            &self.template,
            &self.special,
            messages.iter().map(message_to_mistral).collect(),
            reasoning.enable_thinking,
            reasoning.reasoning_effort,
        )
        .map_err(|e| {
            OpenAIError::InvalidArgument(format!("Failed to render the chat template: {e}"))
        })
    }

    /// Validates `req`, renders its prompt and queues it on the decode thread.
    fn submit(
        &self,
        req: &CreateChatCompletionRequest,
    ) -> std::result::Result<(mpsc::Receiver<Event>, bool), OpenAIError> {
        if req.tools.as_ref().is_some_and(|tools| !tools.is_empty()) {
            return Err(OpenAIError::InvalidArgument(format!(
                "Model '{}' is served by llama.cpp, which does not take `tools` yet.",
                self.model_name
            )));
        }
        let reasoning = reasoning_controls(
            req.reasoning_effort.as_ref(),
            &self.reasoning_support,
            &self.model_name,
        )
        .map_err(OpenAIError::InvalidArgument)?;
        let prompt = self.prompt(&req.messages, reasoning)?;
        let reasons = opens_reasoning(&prompt);
        let (events, rx) = mpsc::channel(STREAM_BUFFER);
        let job = Job {
            prompt,
            limit: completion_limit(req),
            sampling: Sampling {
                temperature: req.temperature,
                top_p: req.top_p,
                seed: rand::random(),
            },
            stop: match &req.stop {
                Some(StopConfiguration::String(s)) => vec![s.clone()],
                Some(StopConfiguration::StringArray(s)) => s.clone(),
                None => Vec::new(),
            },
            events,
        };
        self.jobs
            .send(job)
            .map_err(|_| api_error("the llama.cpp decode thread has stopped"))?;
        Ok((rx, reasons))
    }

    fn id(&self) -> String {
        let n = self.counter.fetch_add(1, Ordering::Relaxed);
        format!("{}-{n}", generate_stream_id(&self.model_name))
    }
}

#[async_trait]
impl Chat for LlamaCpp {
    fn as_sql(&self) -> Option<&dyn SqlGeneration> {
        None
    }

    async fn health(&self) -> Result<()> {
        // A model that loaded is healthy; the decode thread lives as long as `self`.
        Ok(())
    }

    async fn chat_stream(
        &self,
        req: CreateChatCompletionRequest,
    ) -> std::result::Result<ChatCompletionResponseStream, OpenAIError> {
        let (rx, reasons) = self.submit(&req)?;
        let id = self.id();
        let model = req.model;
        let created = now();
        let state = StreamState {
            rx,
            split: ThinkSplit::new(reasons),
            first: true,
            done: false,
        };
        let stream = futures::stream::unfold(state, move |mut state| {
            let id = id.clone();
            let model = model.clone();
            async move {
                if state.done {
                    return None;
                }
                let event = state.rx.recv().await;
                let first = std::mem::replace(&mut state.first, false);
                let item = match event {
                    Some(Event::Token(piece)) => {
                        let (_, content) = state.split.push(&piece);
                        Ok(chunk(
                            &id,
                            &model,
                            created,
                            first,
                            non_empty(content),
                            None,
                            None,
                        ))
                    }
                    Some(Event::Done(outcome)) => {
                        state.done = true;
                        let (_, content) = state.split.finish();
                        Ok(chunk(
                            &id,
                            &model,
                            created,
                            first,
                            non_empty(content),
                            Some(outcome.finish),
                            Some(usage(outcome)),
                        ))
                    }
                    Some(Event::Failed(message)) => {
                        state.done = true;
                        Err(api_error(&message))
                    }
                    None => {
                        state.done = true;
                        Err(api_error(
                            "the llama.cpp decode thread stopped mid-response",
                        ))
                    }
                };
                Some((item, state))
            }
        });
        Ok(Box::pin(stream) as Pin<Box<dyn Stream<Item = _> + Send>>)
    }

    #[expect(
        deprecated,
        reason = "`function_call` and `system_fingerprint` are deprecated fields of the response"
    )]
    async fn chat_request(
        &self,
        req: CreateChatCompletionRequest,
    ) -> std::result::Result<CreateChatCompletionResponse, OpenAIError> {
        let (mut rx, reasons) = self.submit(&req)?;
        let mut split = ThinkSplit::new(reasons);
        let (mut reasoning, mut content) = (String::new(), String::new());
        let outcome = loop {
            match rx.recv().await {
                Some(Event::Token(piece)) => {
                    let (r, c) = split.push(&piece);
                    reasoning.push_str(&r);
                    content.push_str(&c);
                }
                Some(Event::Done(outcome)) => {
                    let (r, c) = split.finish();
                    reasoning.push_str(&r);
                    content.push_str(&c);
                    break outcome;
                }
                Some(Event::Failed(message)) => return Err(api_error(&message)),
                None => {
                    return Err(api_error(
                        "the llama.cpp decode thread stopped mid-response",
                    ));
                }
            }
        };
        let message = ChatCompletionResponseMessage {
            content: Some(content),
            reasoning_content: non_empty(reasoning),
            refusal: None,
            tool_calls: None,
            annotations: None,
            role: Role::Assistant,
            function_call: None,
            audio: None,
        };
        Ok(CreateChatCompletionResponse {
            id: self.id(),
            object: "chat.completion".to_string(),
            created: now(),
            model: req.model,
            choices: vec![ChatChoice {
                index: 0,
                message,
                finish_reason: Some(outcome.finish),
                logprobs: None,
            }],
            usage: Some(usage(outcome)),
            service_tier: None,
            system_fingerprint: None,
        })
    }
}

struct StreamState {
    rx: mpsc::Receiver<Event>,
    split: ThinkSplit,
    first: bool,
    done: bool,
}

/// Renders a conversation the way the mistral.rs engine does.
fn render(
    template: &str,
    special: &SpecialTokens,
    messages: Vec<IndexMap<String, MessageContent>>,
    enable_thinking: Option<bool>,
    reasoning_effort: Option<TemplateReasoningEffort>,
) -> std::result::Result<String, String> {
    apply_chat_template_to(
        messages,
        true,
        enable_thinking,
        reasoning_effort,
        &ChatTemplateValue(Either::Left(template.to_string())),
        special.bos.clone(),
        special.eos.clone(),
        None,
        Vec::new(),
    )
    .map_err(|e| e.to_string())
}

/// The decode thread: loads the model, reports readiness, then runs jobs in order.
fn serve(
    path: &Path,
    draft: Option<&Path>,
    n_ctx: Option<NonZeroU32>,
    ready: oneshot::Sender<std::result::Result<Loaded, String>>,
    jobs: &std_mpsc::Receiver<Job>,
) {
    let backend = match backend() {
        Ok(backend) => backend,
        Err(message) => {
            let _ = ready.send(Err(message));
            return;
        }
    };
    let params = LlamaModelParams::default().with_n_gpu_layers(u32::MAX);
    let model = match LlamaModel::load_from_file(backend, path, &params) {
        Ok(model) => model,
        Err(e) => {
            let _ = ready.send(Err(format!(
                "llama.cpp could not load '{}': {e}",
                path.display()
            )));
            return;
        }
    };
    let draft_model = match draft.map(|d| {
        let params = LlamaModelParams::default()
            .with_n_gpu_layers(u32::MAX)
            .with_load_mtp(true);
        LlamaModel::load_from_file(backend, d, &params).map_err(|e| {
            format!(
                "llama.cpp could not load the MTP head '{}': {e}",
                d.display()
            )
        })
    }) {
        Some(Err(message)) => {
            let _ = ready.send(Err(message));
            return;
        }
        Some(Ok(model)) => Some(model),
        None => None,
    };
    // The contexts borrow the models, so all of them live in this frame for the thread's
    // lifetime. With a draft head, verification rolls back up to `MTP_DRAFT` rejected tokens,
    // and a hybrid model's recurrent layers can roll back only as many tokens as the context
    // keeps state snapshots for, so it keeps that many, as llama.cpp's server sizes it.
    let snapshots = if draft_model.is_some() {
        u32::try_from(MTP_DRAFT).unwrap_or(0)
    } else {
        0
    };
    let context_params = LlamaContextParams::default()
        .with_n_ctx(n_ctx)
        .with_n_batch(u32::try_from(PROMPT_BATCH).unwrap_or(u32::MAX))
        .with_n_rs_seq(snapshots);
    let context = match model.new_context(backend, context_params) {
        Ok(context) => context,
        Err(e) => {
            let _ = ready.send(Err(format!("llama.cpp could not create a context: {e}")));
            return;
        }
    };
    let mut decoder = match &draft_model {
        None => Decoder::Plain(context),
        Some(draft_model) => match mtp(backend, draft_model, context) {
            Ok(spec) => Decoder::Mtp(Box::new(spec)),
            Err(message) => {
                let _ = ready.send(Err(message));
                return;
            }
        },
    };
    let vocab = model.vocab();
    let special = SpecialTokens {
        bos: piece(&model, vocab.bos()),
        eos: piece(&model, vocab.eos()),
    };
    let template = model
        .chat_template(None)
        .ok()
        .and_then(|t| t.to_string().ok());
    if ready.send(Ok(Loaded { template, special })).is_err() {
        return;
    }
    while let Ok(job) = jobs.recv() {
        let events = job.events.clone();
        let result = match &mut decoder {
            Decoder::Plain(context) => generate(&model, context, job),
            Decoder::Mtp(spec) => generate_mtp(&model, spec, job),
        };
        let event = match result {
            Ok(outcome) => Event::Done(outcome),
            Err(message) => Event::Failed(message),
        };
        let _ = events.blocking_send(event);
    }
}

/// The decode thread's engine: plain decoding, or decoding with an MTP draft head.
enum Decoder<'m> {
    Plain(LlamaContext<'m>),
    Mtp(Box<MtpSpeculative<'m>>),
}

/// Creates an MTP draft context linked to `context`, as llama.cpp's own server does: an
/// MTP-type context of the same size, reading the target's hidden states.
fn mtp<'m>(
    backend: &LlamaBackend,
    draft_model: &'m LlamaModel,
    context: LlamaContext<'m>,
) -> std::result::Result<MtpSpeculative<'m>, String> {
    let params = LlamaContextParams::default()
        .with_n_ctx(NonZeroU32::new(context.n_ctx()))
        .with_n_batch(u32::try_from(PROMPT_BATCH).unwrap_or(u32::MAX))
        .with_context_type(LlamaContextType::Mtp)
        .with_n_rs_seq(0);
    let draft = draft_model
        .new_context_with_ctx_other(backend, params, &context)
        .map_err(|e| format!("llama.cpp could not create the MTP draft context: {e}"))?;
    MtpSpeculative::new(
        context,
        draft,
        MtpSpeculativeParams {
            n_max: MTP_DRAFT,
            n_min: 0,
            p_min: 0.0,
        },
    )
    .map_err(|e| format!("llama.cpp could not start MTP drafting: {e}"))
}

/// What prompt reading needs from either engine.
trait Target<'m> {
    fn context(&self) -> &LlamaContext<'m>;
    fn context_mut(&mut self) -> &mut LlamaContext<'m>;
    /// Called after every target decode; the MTP head reads the batch's hidden states.
    fn decoded(&mut self, batch: &LlamaBatch<'_>) -> std::result::Result<(), String>;
}

impl<'m> Target<'m> for LlamaContext<'m> {
    fn context(&self) -> &LlamaContext<'m> {
        self
    }
    fn context_mut(&mut self) -> &mut LlamaContext<'m> {
        self
    }
    fn decoded(&mut self, _: &LlamaBatch<'_>) -> std::result::Result<(), String> {
        Ok(())
    }
}

impl<'m> Target<'m> for MtpSpeculative<'m> {
    fn context(&self) -> &LlamaContext<'m> {
        self.target_context()
    }
    fn context_mut(&mut self) -> &mut LlamaContext<'m> {
        self.target_context_mut()
    }
    fn decoded(&mut self, batch: &LlamaBatch<'_>) -> std::result::Result<(), String> {
        self.process(batch).map_err(|e| e.to_string())
    }
}

/// Tokenizes the prompt and decodes it from an empty KV cache, returning the tokens and the
/// batch whose last logits predict the first generated token.
fn read_prompt<'m>(
    model: &LlamaModel,
    target: &mut impl Target<'m>,
    prompt: &str,
) -> std::result::Result<(Vec<LlamaToken>, LlamaBatch<'static>), String> {
    target.context_mut().clear_kv_cache();
    let tokens = model.vocab().tokenize(prompt.as_bytes(), false, true);
    let n_ctx = usize::try_from(target.context().n_ctx()).unwrap_or(usize::MAX);
    if tokens.is_empty() {
        return Err("the rendered prompt is empty".to_string());
    }
    if tokens.len() >= n_ctx {
        return Err(format!(
            "the prompt is {} tokens, and the model's context holds {n_ctx}. Shorten the conversation, or raise `context_length`.",
            tokens.len()
        ));
    }
    let mut batch = LlamaBatch::new(PROMPT_BATCH, 1);
    let mut pos: i32 = 0;
    let chunks = tokens.chunks(PROMPT_BATCH).count();
    for (i, chunk) in tokens.chunks(PROMPT_BATCH).enumerate() {
        batch.clear();
        for (j, &token) in chunk.iter().enumerate() {
            let last = i + 1 == chunks && j + 1 == chunk.len();
            batch
                .add(token, pos, &[0], last)
                .map_err(|e| e.to_string())?;
            pos += 1;
        }
        target
            .context_mut()
            .decode(&mut batch)
            .map_err(|e| e.to_string())?;
        target.decoded(&batch)?;
    }
    Ok((tokens, batch))
}

/// Sends generated tokens to the client, ending at an end-of-generation token, a stop
/// sequence or the token budget.
struct Output {
    events: mpsc::Sender<Event>,
    limit: Option<usize>,
    stops: StopMatcher,
    pending: Vec<u8>,
    produced: u32,
}

impl Output {
    fn new(job: Job) -> (String, Sampling, Self) {
        let output = Self {
            events: job.events,
            limit: job.limit,
            stops: StopMatcher::new(job.stop),
            pending: Vec::new(),
            produced: 0,
        };
        (job.prompt, job.sampling, output)
    }

    /// Sends one sampled token, or returns why generation ends.
    fn token(&mut self, model: &LlamaModel, token: LlamaToken) -> Option<FinishReason> {
        let vocab = model.vocab();
        if vocab.is_eog(token) {
            return Some(FinishReason::Stop);
        }
        self.produced += 1;
        self.pending.extend(vocab.token_to_piece(token, true, None));
        let text = take_utf8(&mut self.pending);
        let (emit, stopped) = self.stops.push(&text);
        if self.events.blocking_send(Event::Token(emit)).is_err() || stopped {
            return Some(FinishReason::Stop);
        }
        self.limit
            .is_some_and(|limit| self.produced as usize >= limit)
            .then_some(FinishReason::Length)
    }

    /// Tokens the budget still allows.
    fn remaining(&self) -> usize {
        self.limit.map_or(usize::MAX, |limit| {
            limit.saturating_sub(self.produced as usize)
        })
    }

    fn finish(mut self, finish: FinishReason, prompt_tokens: usize) -> Outcome {
        let tail = self.stops.finish();
        if !tail.is_empty() {
            let _ = self.events.blocking_send(Event::Token(tail));
        }
        Outcome {
            finish,
            prompt_tokens: u32::try_from(prompt_tokens).unwrap_or(u32::MAX),
            completion_tokens: self.produced,
        }
    }
}

/// Runs one request, one token per forward pass.
fn generate(
    model: &LlamaModel,
    context: &mut LlamaContext<'_>,
    job: Job,
) -> std::result::Result<Outcome, String> {
    let (prompt, sampling, mut out) = Output::new(job);
    let (tokens, mut batch) = read_prompt(model, context, &prompt)?;
    let n_ctx = usize::try_from(context.n_ctx()).unwrap_or(usize::MAX);
    let mut pos = i32::try_from(tokens.len()).map_err(|e| e.to_string())?;
    let mut sampler = sampler(sampling);
    let finish = loop {
        let token = sampler.sample(context, batch.n_tokens() - 1);
        if let Some(finish) = out.token(model, token) {
            break finish;
        }
        if usize::try_from(pos).unwrap_or(usize::MAX) + 1 >= n_ctx {
            break FinishReason::Length;
        }
        batch.clear();
        batch
            .add(token, pos, &[0], true)
            .map_err(|e| e.to_string())?;
        pos += 1;
        context.decode(&mut batch).map_err(|e| e.to_string())?;
    };
    Ok(out.finish(finish, tokens.len()))
}

/// Runs one request with MTP drafting: each forward pass verifies the last token plus up
/// to [`MTP_DRAFT`] drafted ones, and keeps the drafts the target would have sampled.
fn generate_mtp(
    model: &LlamaModel,
    spec: &mut MtpSpeculative<'_>,
    job: Job,
) -> std::result::Result<Outcome, String> {
    let (prompt, sampling, mut out) = Output::new(job);
    spec.draft_context_mut().clear_kv_cache();
    // `read_prompt` hands every decoded prompt batch to the head, which needs the target's
    // hidden state at each prompt position before `begin`.
    let (tokens, mut batch) = read_prompt(model, spec, &prompt)?;
    spec.begin(&tokens).map_err(|e| e.to_string())?;
    let n_ctx = usize::try_from(spec.target_context().n_ctx()).unwrap_or(usize::MAX);
    let mut history = tokens.clone();
    let mut n_past = i32::try_from(tokens.len()).map_err(|e| e.to_string())?;
    let mut sampler = sampler(sampling);
    let mut id = sampler.sample(spec.target_context(), batch.n_tokens() - 1);
    let finish = 'generate: loop {
        if let Some(finish) = out.token(model, id) {
            break finish;
        }
        let past = usize::try_from(n_past).unwrap_or(usize::MAX);
        if past + 1 >= n_ctx {
            break FinishReason::Length;
        }
        let mut draft = spec
            .draft(n_past, id, &history)
            .map_err(|e| e.to_string())?;
        // `accept` is owed exactly when the head drafted something.
        let drafted = !draft.is_empty();
        draft.truncate(out.remaining().min(n_ctx - past - 1));
        // Drafting wrote its positions into the head's cache, and processing the verify batch
        // writes them again, so they go first.
        truncate_cache(spec.draft_context_mut(), n_past)?;

        batch.clear();
        batch
            .add(id, n_past, &[0], true)
            .map_err(|e| e.to_string())?;
        for (i, &token) in draft.iter().enumerate() {
            let at = n_past + 1 + i32::try_from(i).map_err(|e| e.to_string())?;
            batch
                .add(token, at, &[0], true)
                .map_err(|e| e.to_string())?;
        }
        spec.target_context_mut()
            .decode(&mut batch)
            .map_err(|e| e.to_string())?;
        spec.process(&batch).map_err(|e| e.to_string())?;
        history.push(id);

        let mut accepted: usize = 0;
        let next = loop {
            let index = i32::try_from(accepted).map_err(|e| e.to_string())?;
            let token = sampler.sample(spec.target_context(), index);
            if accepted < draft.len() && token == draft[accepted] {
                accepted += 1;
                if let Some(finish) = out.token(model, token) {
                    break 'generate finish;
                }
                history.push(token);
            } else {
                break token;
            }
        };
        n_past += 1 + i32::try_from(accepted).map_err(|e| e.to_string())?;
        if drafted {
            spec.accept(u16::try_from(accepted).unwrap_or(u16::MAX))
                .map_err(|e| e.to_string())?;
        }
        // The rejected drafts leave both caches.
        truncate_cache(spec.target_context_mut(), n_past)?;
        truncate_cache(spec.draft_context_mut(), n_past)?;
        id = next;
    };
    Ok(out.finish(finish, tokens.len()))
}

/// Removes sequence 0's cache entries from position `from` on. A context that cannot roll
/// back that far, such as a recurrent layer with too few state snapshots, fails the request
/// instead of decoding on from a state that still holds the removed tokens.
fn truncate_cache(context: &mut LlamaContext<'_>, from: i32) -> std::result::Result<(), String> {
    let from = u32::try_from(from).map_err(|e| e.to_string())?;
    match context.clear_kv_cache_seq(Some(0), Some(from), None) {
        Ok(true) => Ok(()),
        Ok(false) => Err(format!(
            "llama.cpp could not roll its cache back to position {from}"
        )),
        Err(e) => Err(e.to_string()),
    }
}

/// Greedy at temperature 0 or unset, as the mistral.rs engine's deterministic default;
/// otherwise top-p then temperature, then a seeded draw.
fn sampler(sampling: Sampling) -> LlamaSampler {
    match sampling.temperature {
        Some(t) if t > 0.0 => LlamaSampler::chain_simple([
            LlamaSampler::top_p(sampling.top_p.unwrap_or(1.0), 1),
            LlamaSampler::temp(t),
            LlamaSampler::dist(sampling.seed),
        ]),
        _ => LlamaSampler::greedy(),
    }
}

/// Whether a file is an MTP draft head, by the `mtp-` name the GGUF publishers give them.
fn is_mtp_head(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.starts_with("mtp-"))
}

/// The GGUF a sharded model is opened from: the `-00001-of-` file, or the only file.
fn first_shard(weights: &[PathBuf]) -> Option<PathBuf> {
    weights
        .iter()
        .find(|w| w.to_string_lossy().contains("-00001-of-"))
        .or_else(|| weights.first())
        .cloned()
}

fn piece(model: &LlamaModel, token: LlamaToken) -> Option<String> {
    if token.0 < 0 {
        return None;
    }
    String::from_utf8(model.vocab().token_to_piece(token, true, None))
        .ok()
        .filter(|s| !s.is_empty())
}

/// Removes and returns the longest valid UTF-8 prefix of `bytes`, keeping an incomplete
/// trailing character for the next token. Bytes that can never become valid are replaced.
fn take_utf8(bytes: &mut Vec<u8>) -> String {
    match std::str::from_utf8(bytes) {
        Ok(text) => {
            let text = text.to_string();
            bytes.clear();
            text
        }
        Err(e) if e.error_len().is_none() => {
            let valid = e.valid_up_to();
            let rest = bytes.split_off(valid);
            let text = String::from_utf8_lossy(bytes).into_owned();
            *bytes = rest;
            text
        }
        Err(_) => {
            let text = String::from_utf8_lossy(bytes).into_owned();
            bytes.clear();
            text
        }
    }
}

/// Holds back text that could still become a stop sequence, and ends generation at the
/// first one.
struct StopMatcher {
    stops: Vec<String>,
    held: String,
}

impl StopMatcher {
    fn new(stops: Vec<String>) -> Self {
        Self {
            stops: stops.into_iter().filter(|s| !s.is_empty()).collect(),
            held: String::new(),
        }
    }

    /// The text that can be released, and whether a stop sequence was reached.
    fn push(&mut self, text: &str) -> (String, bool) {
        if self.stops.is_empty() {
            return (text.to_string(), false);
        }
        self.held.push_str(text);
        if let Some(at) = self
            .stops
            .iter()
            .filter_map(|s| self.held.find(s.as_str()))
            .min()
        {
            let emit = self.held[..at].to_string();
            self.held.clear();
            return (emit, true);
        }
        let keep = self
            .stops
            .iter()
            .map(|s| longest_suffix_prefix(&self.held, s))
            .max()
            .unwrap_or(0);
        let cut = self.held.len() - keep;
        let emit = self.held[..cut].to_string();
        self.held.drain(..cut);
        (emit, false)
    }

    fn finish(&mut self) -> String {
        std::mem::take(&mut self.held)
    }
}

/// The length of the longest suffix of `text` that is a proper prefix of `pattern`, on a
/// character boundary.
fn longest_suffix_prefix(text: &str, pattern: &str) -> usize {
    (1..pattern.len().min(text.len() + 1))
        .rev()
        .find(|&n| {
            text.is_char_boundary(text.len() - n)
                && pattern.is_char_boundary(n)
                && text.ends_with(&pattern[..n])
        })
        .unwrap_or(0)
}

/// Splits generated text into reasoning and content at `<think>` and `</think>`.
struct ThinkSplit {
    in_think: bool,
    held: String,
}

impl ThinkSplit {
    fn new(in_think: bool) -> Self {
        Self {
            in_think,
            held: String::new(),
        }
    }

    /// The reasoning and content `text` adds, holding back a possible partial tag.
    fn push(&mut self, text: &str) -> (String, String) {
        self.held.push_str(text);
        let (mut reasoning, mut content) = (String::new(), String::new());
        loop {
            let tag = if self.in_think {
                THINK_CLOSE
            } else {
                THINK_OPEN
            };
            if let Some(at) = self.held.find(tag) {
                let before: String = self.held.drain(..at).collect();
                self.held.drain(..tag.len());
                if self.in_think {
                    reasoning.push_str(&before);
                } else {
                    content.push_str(&before);
                }
                self.in_think = !self.in_think;
                continue;
            }
            let keep = longest_suffix_prefix(&self.held, tag);
            let cut = self.held.len() - keep;
            let ready: String = self.held.drain(..cut).collect();
            if self.in_think {
                reasoning.push_str(&ready);
            } else {
                content.push_str(&ready);
            }
            return (reasoning, content);
        }
    }

    fn finish(&mut self) -> (String, String) {
        let rest = std::mem::take(&mut self.held);
        if self.in_think {
            (rest, String::new())
        } else {
            (String::new(), rest)
        }
    }
}

#[expect(deprecated)]
fn completion_limit(req: &CreateChatCompletionRequest) -> Option<usize> {
    req.max_completion_tokens
        .or(req.max_tokens)
        .and_then(|limit| usize::try_from(limit).ok())
}

#[expect(
    deprecated,
    reason = "`function_call` and `system_fingerprint` are deprecated fields of a chunk"
)]
fn chunk(
    id: &str,
    model: &str,
    created: u32,
    first: bool,
    content: Option<String>,
    finish_reason: Option<FinishReason>,
    usage: Option<CompletionUsage>,
) -> CreateChatCompletionStreamResponse {
    let delta = ChatCompletionStreamResponseDelta {
        content,
        function_call: None,
        tool_calls: None,
        role: first.then_some(Role::Assistant),
        refusal: None,
    };
    CreateChatCompletionStreamResponse {
        id: id.to_string(),
        choices: vec![ChatChoiceStream {
            index: 0,
            delta,
            finish_reason,
            logprobs: None,
        }],
        created,
        model: model.to_string(),
        service_tier: None,
        system_fingerprint: None,
        object: "chat.completion.chunk".to_string(),
        usage,
    }
}

fn usage(outcome: Outcome) -> CompletionUsage {
    CompletionUsage {
        prompt_tokens: outcome.prompt_tokens,
        completion_tokens: outcome.completion_tokens,
        total_tokens: outcome.prompt_tokens + outcome.completion_tokens,
        prompt_tokens_details: None,
        completion_tokens_details: None,
    }
}

fn non_empty(text: String) -> Option<String> {
    (!text.is_empty()).then_some(text)
}

fn now() -> u32 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|d| u32::try_from(d.as_secs()).ok())
        .unwrap_or_default()
}

fn api_error(message: &str) -> OpenAIError {
    OpenAIError::ApiError(ApiError {
        message: message.to_string(),
        r#type: None,
        param: None,
        code: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_split_tag_is_held_until_it_completes() {
        let mut split = ThinkSplit::new(true);
        assert_eq!(split.push("plan</th"), ("plan".to_string(), String::new()));
        assert_eq!(
            split.push("ink>\n\nanswer"),
            (String::new(), "\n\nanswer".to_string())
        );
        assert_eq!(split.finish(), (String::new(), String::new()));
    }

    #[test]
    fn a_reply_that_opens_its_own_block_is_split_too() {
        let mut split = ThinkSplit::new(false);
        assert_eq!(
            split.push("<think>why</think>42"),
            ("why".to_string(), "42".to_string())
        );
    }

    #[test]
    fn a_stop_sequence_ends_the_text_before_it() {
        let mut stops = StopMatcher::new(vec!["\n\nUser:".to_string()]);
        assert_eq!(stops.push("1, 2, 3\n"), ("1, 2, 3".to_string(), false));
        assert_eq!(stops.push("\nUser: hi"), (String::new(), true));
    }

    #[test]
    fn an_incomplete_character_waits_for_the_next_token() {
        let mut bytes = "é".as_bytes()[..1].to_vec();
        assert_eq!(take_utf8(&mut bytes), "");
        bytes.extend(&"é".as_bytes()[1..]);
        assert_eq!(take_utf8(&mut bytes), "é");
    }
}
