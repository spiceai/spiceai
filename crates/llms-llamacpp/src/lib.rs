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
//! run one at a time, each decoding only the part of its prompt that the KV cache does not
//! already hold from the request before it (`PromptCache`), and their tokens stream back
//! over a Tokio channel, so a forward pass never blocks an async worker. Dropping the model
//! stops that thread and waits for it to free the model.

use std::{
    collections::{HashMap, VecDeque},
    fmt::Write as _,
    num::NonZeroU32,
    ops::Range,
    path::{Path, PathBuf},
    pin::Pin,
    sync::{
        Arc, Mutex, PoisonError,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc as std_mpsc,
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use async_openai::{
    error::{ApiError, OpenAIError},
    types::chat::{
        ChatChoice, ChatChoiceStream, ChatCompletionMessageToolCall,
        ChatCompletionMessageToolCallChunk, ChatCompletionMessageToolCalls,
        ChatCompletionRequestMessage, ChatCompletionResponseMessage, ChatCompletionResponseStream,
        ChatCompletionStreamResponseDelta, CompletionUsage, CreateChatCompletionRequest,
        CreateChatCompletionResponse, CreateChatCompletionStreamResponse, FinishReason,
        FunctionCall, FunctionCallStream, FunctionType, ResponseFormat, Role, StopConfiguration,
    },
};
use async_trait::async_trait;
use either::Either;
use futures::Stream;
use indexmap::IndexMap;
use llama_cpp::{
    Backend, Batch, Context, ContextParams, DraftKind, Model, ModelParams, Mtp, MtpParams, Sampler,
    Token, Truncation, Vocab,
};
use mistralrs_core::reasoning_parsers::{
    TagReasoningContext,
    harmony::is_harmony_template,
    tag_based::{
        THINK_OPEN_TAG, is_channel_tag_template, is_gemma_turn_template, is_think_tag_template,
    },
};
use mistralrs_core::{
    AllowedToolChoice, AllowedToolsMode, AllowedToolsToolChoice, ChatTemplateValue, MessageContent,
    ReasoningEffort as TemplateReasoningEffort, Tool, ToolCallResponse, ToolCallingMatcher,
    ToolChoice, apply_chat_template_to,
};
use tokio::sync::{mpsc, oneshot};

use llms::chat::engine::LocalChatEngine;
use llms::chat::mistral::{
    ReasoningControls, ReasoningSupport, convert_tool, reasoning_controls, request_tool_choice,
};
use llms::chat::{
    Chat, Error as ChatError, LocalModelOptions, PagedAttentionMode, Result, message_to_mistral,
    nsql::SqlGeneration,
};
use llms::streaming_utils::generate_stream_id;
use runtime_request_context::{AsyncMarker, CacheNamespace, RequestContext};

mod distributed;
mod harmony;

llms::register_local_chat_engine!(
    LLAMA_CPP,
    LocalChatEngine {
        name: "llama.cpp",
        serves: serves_weights,
        distributed: Some(llama_cpp::rpc::splits_by_tensor),
        load: load_weights,
    }
);

/// llama.cpp serves a model whose weights are all GGUF files: shards of one model, and an
/// optional MTP draft head.
fn serves_weights(weights: &[PathBuf]) -> bool {
    !weights.is_empty()
        && weights.iter().all(|w| {
            w.extension()
                .is_some_and(|e| e.eq_ignore_ascii_case("gguf"))
        })
}

fn load_weights<'a>(
    weights: &'a [PathBuf],
    options: LocalModelOptions<'a>,
) -> futures::future::BoxFuture<'a, Result<std::sync::Arc<dyn Chat>>> {
    Box::pin(async move {
        if let Some(cfg) = options.distributed {
            distributed::ensure_insecure_allowed(cfg, options.reasoning.model_name)?;
            // A rank other than 0 lends its GPU and never reads the weights: the head sends each
            // rank its share.
            if cfg.node_rank > 0 {
                return distributed::lend_gpu(cfg, options.reasoning.model_name)
                    .await
                    .map(|share| std::sync::Arc::new(share) as std::sync::Arc<dyn Chat>);
            }
        }
        LlamaCpp::from(weights, options)
            .await
            .map(|model| std::sync::Arc::new(model) as std::sync::Arc<dyn Chat>)
    })
}

/// Tokens decoded per forward pass while reading the prompt.
const PROMPT_BATCH: usize = 512;

/// The context size when the model sets no `context_length`.
const DEFAULT_CONTEXT: u32 = 8192;

/// How many tokens before a prompt's end a model with a recurrent state saves a checkpoint of
/// it, as llama.cpp's server does. A chat's next prompt usually differs from this one only from
/// the generation prompt on, which the template renders differently for an earlier turn, so the
/// next request restores the checkpoint instead of reading its whole prompt again; when it
/// differs earlier, an earlier request's checkpoint still saves reading the turns before.
const CHECKPOINT_BEFORE_END: usize = 4;

/// The most checkpoints a model keeps; a newer one replaces the oldest.
const MAX_CHECKPOINTS: usize = 4;

/// The weight of the newest measurement in the smoothed checkpoint costs.
const COST_SMOOTHING: f64 = 0.25;

/// Draft tokens an MTP head proposes per step. Two measured fastest for Qwen3.8-Flash-Next
/// on a DGX Spark: over eight prompts, 60.7 tok/s against 53.8 with one and 47.5 without a
/// draft, and three was slower than none on a story prompt.
const MTP_DRAFT: i32 = 2;

/// Draft tokens an MTP head proposes per step for Qwen3.8-Flash-Next (`qwen4exp`) split across
/// nodes. Split three ways over DGX Sparks, three drafted measured fastest over eight prompts:
/// 53.6 tok/s against 51.8 with two through spiced, 55.0-56.7 against 51.2-53.2 under
/// llama-server. GLM-5.3 split the same way ran slower with three (39-40 against 42.1-42.6), so
/// it keeps [`MTP_DRAFT`].
const MTP_DRAFT_SPLIT_QWEN4EXP: i32 = 3;

/// Draft tokens a `DFlash` or `DSpark` drafter proposes per step. Three measured fastest on DGX
/// Sparks over eight prompts: DeepSeek-V4.1 with `DSpark` at 35.0 tok/s on two nodes (two and
/// four drafted 30.0 and 29.7), Qwen3.8-Flash-Next `Q2_K` with `DFlash2` at 58.7 on one (four:
/// 53.7).
const DFLASH_DRAFT: i32 = 3;

/// How many tokens an MTP head proposes per step for `model`, which is `split` across nodes or not.
fn mtp_draft(model: &Model, split: bool) -> i32 {
    if split && model.architecture().as_deref() == Some("qwen4exp") {
        MTP_DRAFT_SPLIT_QWEN4EXP
    } else {
        MTP_DRAFT
    }
}

/// How many tokens a draft model of this kind proposes per step, an MTP head `mtp`.
fn draft_len(kind: DraftKind, mtp: i32) -> i32 {
    match kind {
        DraftKind::Mtp => mtp,
        DraftKind::DFlash | DraftKind::DSpark => DFLASH_DRAFT,
    }
}

/// How many streamed tokens may wait for a slow client before decoding pauses.
const STREAM_BUFFER: usize = 64;

/// The most choices one request may ask for, as `OpenAI`'s `n` allows.
const MAX_CHOICES: u8 = 128;

/// How long the decode thread waits before it retries a send to a client whose buffer is full.
const SEND_RETRY: Duration = Duration::from_millis(1);

/// How long dropping a model waits for its decode thread to stop and free it.
const UNLOAD_WAIT: Duration = Duration::from_secs(30);

/// Why a request ends early: its decode thread stopped, as when the model is unloaded.
const DECODE_STOPPED: &str = "the llama.cpp decode thread stopped mid-response";

/// The token Gemma 4 chat templates add to the prompt to turn reasoning on.
const GEMMA_THINK_TOKEN: &str = "<|think|>";

pub struct LlamaCpp {
    decoder: DecodeThread,
    model_name: String,
    template: Template,
    special: SpecialTokens,
    /// The markers of the Harmony replies the template asks for, when it asks for them.
    harmony: Option<harmony::Markers>,
    reasoning_support: ReasoningSupport,
    counter: AtomicUsize,
    /// How many prompt tokens the last request read from the KV cache, which the decode thread
    /// sets.
    reused_prompt_tokens: Arc<AtomicUsize>,
}

/// A model's chat template: one, or named ones from a `tokenizer_config.json`, which a request
/// chooses between as on the mistral.rs engine (`tool_use` with tools, `default` without).
type Template = Either<String, Vec<HashMap<String, String>>>;

/// The token strings a chat template may refer to.
#[derive(Debug, Clone, Default)]
struct SpecialTokens {
    bos: Option<String>,
    eos: Option<String>,
}

/// What the decode thread reports once the model is loaded.
struct Loaded {
    template: Option<String>,
    /// `general.architecture`, as the GGUF names it.
    architecture: Option<String>,
    special: SpecialTokens,
    /// The Harmony markers, when the vocabulary holds them.
    harmony: Option<harmony::Markers>,
    /// How many prompt tokens each request reads from the KV cache, as the thread sets it.
    reused_prompt_tokens: Arc<AtomicUsize>,
}

#[derive(Debug, Clone, Default)]
struct Sampling {
    temperature: Option<f32>,
    top_p: Option<f32>,
    /// `OpenAI`'s frequency and presence penalties, over every token the reply generates.
    frequency_penalty: Option<f32>,
    presence_penalty: Option<f32>,
    seed: u32,
    /// The GBNF grammar the content must match: `response_format`'s, or a required tool
    /// call's.
    grammar: Option<String>,
    /// Rules out every end-of-generation token, because `tool_choice` requires a tool call
    /// and no grammar can force one in the template's format.
    ban_eog: bool,
}

struct Job {
    prompt: String,
    /// The caller's cache namespace: a request reuses only what its own caller's requests left
    /// in the KV cache.
    namespace: CacheNamespace,
    limit: Option<usize>,
    sampling: Sampling,
    stop: Vec<String>,
    /// Tells the reply's reasoning apart from its answer, when the chat template marks one.
    reasoning: Option<TagReasoningContext>,
    /// Reads tool calls out of the content, when the request offers tools.
    tools: Option<ToolCallingMatcher>,
    /// Reads a Harmony reply, which carries its reasoning, content and tool calls in its own
    /// messages, in place of `reasoning` and `tools`.
    harmony: Option<harmony::Reader>,
    /// Whether `tool_choice` requires a tool call, so a reply without one fails.
    require_tool_call: bool,
    /// Set when the decode thread is stopping, which ends the request.
    stopping: Arc<AtomicBool>,
    events: mpsc::Sender<Event>,
}

/// The model's decode thread, as the model's handle holds it. Dropping it stops the thread
/// and waits, for up to [`UNLOAD_WAIT`], until the thread has freed the model, so a model
/// that is dropped holds no memory afterwards. On Apple GPUs that matters at exit too: ggml
/// aborts a process that exits with Metal memory still allocated.
struct DecodeThread {
    /// The model's name, for the log line when it is slow to stop.
    model: String,
    jobs: Option<std_mpsc::Sender<Job>>,
    stopping: Arc<AtomicBool>,
    /// Disconnects once the thread has freed everything it held. In a mutex only so the
    /// model's handle can be shared; only `drop` reads it.
    freed: Option<Mutex<std_mpsc::Receiver<()>>>,
}

impl Drop for DecodeThread {
    fn drop(&mut self) {
        self.stopping.store(true, Ordering::Relaxed);
        // Closing the queue ends the thread's loop once a running request stops.
        drop(self.jobs.take());
        if let Some(freed) = self.freed.take()
            && freed
                .into_inner()
                .unwrap_or_else(PoisonError::into_inner)
                .recv_timeout(UNLOAD_WAIT)
                == Err(std_mpsc::RecvTimeoutError::Timeout)
        {
            tracing::debug!(
                "Model '{}' did not stop decoding within {}s of being dropped; it is freed when it does.",
                self.model,
                UNLOAD_WAIT.as_secs()
            );
        }
    }
}

/// What the decode thread sends back for one request, in order.
enum Event {
    Reasoning(String),
    Content(String),
    /// Complete tool calls, after which generation stops.
    ToolCalls(Vec<ToolCallResponse>),
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
    ///
    /// # Errors
    ///
    /// When a weight file is missing, llama.cpp cannot load the model or its MTP head, the
    /// model has no chat template or writes Harmony replies, or `reasoning_effort` is one
    /// its chat template refuses, or `options` makes this node a rank other than 0 of a
    /// distributed model: such a rank serves no model of its own, and the engine's `load`
    /// returns the share it holds for the head instead.
    pub async fn from(weights: &[PathBuf], options: LocalModelOptions<'_>) -> Result<Self> {
        if let Some(cfg) = options.distributed
            && cfg.node_rank > 0
        {
            return Err(ChatError::FailedToLoadModel {
                source: format!(
                    "Model '{}' is a share held for the first node in `nodes` on this node (rank {}), so it loads through the engine, not as a model of its own.",
                    options.reasoning.model_name, cfg.node_rank
                )
                .into(),
            });
        }
        for weight in weights {
            if !weight.exists() {
                return Err(ChatError::LocalModelNotFound {
                    expected_path: weight.to_string_lossy().to_string(),
                });
            }
        }
        // An MTP head ships as its own GGUF (`mtp-*.gguf`); it drafts for the model, so it is
        // loaded beside it rather than as one of its shards.
        let (drafts, trunk): (Vec<_>, Vec<_>) = weights
            .iter()
            .cloned()
            .partition(|w| is_mtp_head(w) || DraftKind::dflash_of(w).is_some());
        let first = first_shard(&trunk).ok_or_else(|| ChatError::LocalModelNotFound {
            expected_path: String::new(),
        })?;
        let draft = drafts.into_iter().next();
        let n_ctx = options
            .context_length
            .and_then(|length| u32::try_from(length).ok())
            .and_then(NonZeroU32::new)
            .or(NonZeroU32::new(DEFAULT_CONTEXT));

        if options.paged_attention == PagedAttentionMode::Disabled {
            tracing::info!("{}", paged_attention_note(options.reasoning.model_name));
        }
        // The template file is read first, so one that can't be read fails the load before the
        // model is read.
        let file_template = match options.chat_template_file {
            Some(file) => template_in_file(file).await?,
            None => None,
        };
        // A distributed model: the head reaches every rank's GPU and splits the model across
        // them.
        let devices = match options.distributed {
            None => Vec::new(),
            Some(cfg) => distributed::reach_ranks(cfg, options.reasoning.model_name).await?,
        };
        let (ready_tx, ready_rx) = oneshot::channel();
        let (jobs_tx, jobs_rx) = std_mpsc::channel();
        let (freed_tx, freed_rx) = std_mpsc::channel::<()>();
        let name = options.reasoning.model_name.to_string();
        let file = file_name(&first);
        thread::Builder::new()
            .name("llama-cpp".to_string())
            .spawn(move || {
                serve(
                    &name,
                    &first,
                    draft.as_deref(),
                    &devices,
                    n_ctx,
                    ready_tx,
                    &jobs_rx,
                );
                // Everything `serve` held is freed by now.
                drop(freed_tx);
            })
            .map_err(|e| ChatError::FailedToLoadModel {
                source: Box::new(e),
            })?;
        // From here on, an early return stops the thread and waits for it to free the model.
        let decoder = DecodeThread {
            model: options.reasoning.model_name.to_string(),
            jobs: Some(jobs_tx),
            stopping: Arc::new(AtomicBool::new(false)),
            freed: Some(Mutex::new(freed_rx)),
        };
        let loaded = ready_rx.await.map_err(|e| ChatError::FailedToLoadModel {
            source: Box::new(e),
        })??;

        // As on the mistral.rs path, a template file's template wins over a GGUF's own, and a
        // GGUF's own wins over `chat_template`.
        let template = file_template
            .or(loaded.template.map(Either::Left))
            .or_else(|| {
                options
                    .chat_template_literal
                    .map(|literal| Either::Left(literal.to_string()))
            })
            .ok_or_else(|| ChatError::FailedToLoadModel {
                source:
                    "the GGUF file carries no chat template, and the model sets no `chat_template`"
                        .into(),
            })?;
        let harmony = check_reply_format(&template, loaded.architecture, loaded.harmony, file)?;

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
                    render(
                        &template,
                        &special,
                        vec![message],
                        enable_thinking,
                        effort,
                        Vec::new(),
                    )
                    .ok()
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
            decoder,
            model_name,
            template,
            special: loaded.special,
            harmony,
            reasoning_support,
            counter: AtomicUsize::new(0),
            reused_prompt_tokens: loaded.reused_prompt_tokens,
        })
    }

    /// How many prompt tokens the last request read from the KV cache the request before it
    /// left, instead of decoding them again. For tests.
    #[doc(hidden)]
    #[must_use]
    pub fn reused_prompt_tokens(&self) -> usize {
        self.reused_prompt_tokens.load(Ordering::Relaxed)
    }

    fn prompt(
        &self,
        messages: &[ChatCompletionRequestMessage],
        reasoning: ReasoningControls,
        tools: Vec<Tool>,
    ) -> std::result::Result<String, OpenAIError> {
        render(
            &self.template,
            &self.special,
            messages.iter().map(message_to_mistral).collect(),
            reasoning.enable_thinking,
            reasoning.reasoning_effort,
            tools,
        )
        .map_err(|e| {
            OpenAIError::InvalidArgument(format!("Failed to render the chat template: {e}"))
        })
    }

    /// Validates `req`, renders its prompt and queues one job per choice on the decode
    /// thread, which decodes them in turn. The receivers are in choice order.
    fn submit(
        &self,
        req: &CreateChatCompletionRequest,
        namespace: &CacheNamespace,
    ) -> std::result::Result<Vec<mpsc::Receiver<Event>>, OpenAIError> {
        let choices = choice_count(req)?;
        let reasoning = reasoning_controls(
            req.reasoning_effort.as_ref(),
            &self.reasoning_support,
            &self.model_name,
        )
        .map_err(OpenAIError::InvalidArgument)?;
        // Tools are rendered into the prompt and read back out of the reply with the
        // mistral.rs engine's own conversions and parser, so both engines agree.
        let tool_choice = request_tool_choice(req)?;
        let tools: Vec<Tool> = req.tools.iter().flatten().map(convert_tool).collect();
        // A required call is forced with a grammar where the template's format allows one,
        // and otherwise only held to by refusing to end until the model makes one.
        let obliged = obliged_tools(tool_choice.as_ref(), &tools);
        let callable = callable_tools(tool_choice.as_ref(), &tools);
        // The template this request renders with, for what its markup decides.
        let template = chosen_template(&self.template, !tools.is_empty())
            .map_err(OpenAIError::InvalidArgument)?;
        let content = || {
            content_grammar(
                &self.model_name,
                template,
                req.response_format.as_ref(),
                &callable,
                &obliged,
            )
        };
        // A Harmony grammar holds the whole reply, by the vocabulary's markers.
        let grammar = match &self.harmony {
            Some(markers) if !obliged.is_empty() => Some(harmony_call(markers, &obliged)?),
            Some(markers) => content()?.map(|reply| harmony_reply(markers, &reply)),
            None => content()?,
        };
        // Each choice reads its own reply, so each has its own reader.
        let matchers = (0..choices)
            .map(|_| {
                if tools.is_empty() {
                    return Ok(None);
                }
                ToolCallingMatcher::new_with_format(
                    tool_choice.clone().unwrap_or(ToolChoice::Auto),
                    Some(&tools),
                    None,
                )
                .map(Some)
                .map_err(|e| OpenAIError::InvalidArgument(format!("Invalid `tools`: {e}")))
            })
            .collect::<std::result::Result<Vec<_>, OpenAIError>>()?;
        let require_tool_call = matchers
            .first()
            .and_then(Option::as_ref)
            .is_some_and(ToolCallingMatcher::requires_tool_call);
        let ban_eog = require_tool_call && grammar.is_none();
        let nameable = nameable_tools(&callable, &obliged);
        let held = if !obliged.is_empty() {
            harmony::Held::Call
        } else if grammar.is_some() {
            harmony::Held::Reply
        } else {
            harmony::Held::Nothing
        };
        let prompt = self.prompt(&req.messages, reasoning, tools)?;
        let stop = match &req.stop {
            Some(StopConfiguration::String(s)) => vec![s.clone()],
            Some(StopConfiguration::StringArray(s)) => s.clone(),
            None => Vec::new(),
        };
        let stopped = || api_error("the llama.cpp decode thread has stopped");
        let jobs = self.decoder.jobs.as_ref().ok_or_else(stopped)?;
        let mut receivers = Vec::with_capacity(choices);
        for tools in matchers {
            let (events, rx) = mpsc::channel(STREAM_BUFFER);
            let harmony = self
                .harmony
                .map(|markers| harmony::Reader::new(markers, nameable.clone(), held));
            let job = Job {
                reasoning: reasoning_parser(template, &prompt),
                prompt: prompt.clone(),
                namespace: namespace.clone(),
                limit: completion_limit(req),
                // Each choice samples with its own seed, so choices differ above temperature 0.
                sampling: Sampling {
                    temperature: req.temperature,
                    top_p: req.top_p,
                    frequency_penalty: req.frequency_penalty,
                    presence_penalty: req.presence_penalty,
                    seed: rand::random(),
                    grammar: grammar.clone(),
                    ban_eog,
                },
                stop: stop.clone(),
                tools: tools.filter(|_| harmony.is_none()),
                harmony,
                require_tool_call,
                stopping: Arc::clone(&self.decoder.stopping),
                events,
            };
            jobs.send(job).map_err(|_| stopped())?;
            receivers.push(rx);
        }
        Ok(receivers)
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
        let choices = self.submit(&req, &caller().await)?.into();
        let id = self.id();
        let model = req.model;
        let created = now();
        let state = StreamState {
            choices,
            index: 0,
            first: true,
            completion_tokens: 0,
            done: false,
        };
        let stream = futures::stream::unfold(state, move |mut state| {
            let id = id.clone();
            let model = model.clone();
            async move {
                if state.done {
                    return None;
                }
                // A streamed delta has no reasoning field, so reasoning is not streamed, as on
                // the mistral.rs engine; it is in a non-streamed response's `reasoning_content`.
                let item = loop {
                    let (index, first) = (state.index, state.first);
                    match state.choices.front_mut()?.recv().await {
                        Some(Event::Reasoning(_)) => {}
                        Some(Event::Content(content)) => {
                            state.first = false;
                            break Ok(chunk(
                                &id,
                                &model,
                                created,
                                index,
                                first,
                                Some(content),
                                None,
                                None,
                                None,
                            ));
                        }
                        Some(Event::ToolCalls(calls)) => {
                            state.first = false;
                            let calls = calls
                                .iter()
                                .enumerate()
                                .map(|(i, call)| tool_call_chunk(i, call))
                                .collect();
                            break Ok(chunk(
                                &id,
                                &model,
                                created,
                                index,
                                first,
                                None,
                                Some(calls),
                                None,
                                None,
                            ));
                        }
                        Some(Event::Done(outcome)) => {
                            state.choices.pop_front();
                            state.completion_tokens = state
                                .completion_tokens
                                .saturating_add(outcome.completion_tokens);
                            // The next choice starts its own message; the last choice's
                            // final chunk carries the usage of them all.
                            state.done = state.choices.is_empty();
                            state.index += 1;
                            state.first = true;
                            let usage = state
                                .done
                                .then(|| usage(outcome.prompt_tokens, state.completion_tokens));
                            break Ok(chunk(
                                &id,
                                &model,
                                created,
                                index,
                                first,
                                None,
                                None,
                                Some(outcome.finish),
                                usage,
                            ));
                        }
                        Some(Event::Failed(message)) => {
                            state.done = true;
                            break Err(api_error(&message));
                        }
                        None => {
                            state.done = true;
                            break Err(api_error(DECODE_STOPPED));
                        }
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
        let mut choices = Vec::new();
        let (mut prompt_tokens, mut completion_tokens) = (0, 0_u32);
        let namespace = caller().await;
        for (index, mut rx) in (0..).zip(self.submit(&req, &namespace)?) {
            let (mut reasoning, mut content, mut tool_calls) =
                (String::new(), String::new(), Vec::new());
            let outcome = loop {
                match rx.recv().await {
                    Some(Event::Reasoning(text)) => reasoning.push_str(&text),
                    Some(Event::Content(text)) => content.push_str(&text),
                    Some(Event::ToolCalls(calls)) => {
                        tool_calls.extend(calls.iter().map(tool_call));
                    }
                    Some(Event::Done(outcome)) => break outcome,
                    Some(Event::Failed(message)) => return Err(api_error(&message)),
                    None => {
                        return Err(api_error(DECODE_STOPPED));
                    }
                }
            };
            let message = ChatCompletionResponseMessage {
                // With tool calls, text the model wrote beside them is the content, if any.
                content: if tool_calls.is_empty() {
                    Some(content)
                } else {
                    non_empty(content)
                },
                reasoning_content: non_empty(reasoning),
                refusal: None,
                tool_calls: (!tool_calls.is_empty()).then_some(tool_calls),
                annotations: None,
                role: Role::Assistant,
                function_call: None,
                audio: None,
            };
            // Every choice reads the same prompt, which counts once.
            prompt_tokens = outcome.prompt_tokens;
            completion_tokens = completion_tokens.saturating_add(outcome.completion_tokens);
            choices.push(ChatChoice {
                index,
                message,
                finish_reason: Some(outcome.finish),
                logprobs: None,
            });
        }
        Ok(CreateChatCompletionResponse {
            id: self.id(),
            object: "chat.completion".to_string(),
            created: now(),
            model: req.model,
            choices,
            usage: Some(usage(prompt_tokens, completion_tokens)),
            service_tier: None,
            system_fingerprint: None,
        })
    }
}

struct StreamState {
    /// The choices still to stream, in order; the front one is streaming.
    choices: VecDeque<mpsc::Receiver<Event>>,
    /// The front choice's index.
    index: u32,
    /// Whether the front choice has sent nothing yet, so its next chunk names the role.
    first: bool,
    /// The completion tokens of the choices streamed so far.
    completion_tokens: u32,
    done: bool,
}

/// A tool call as a streamed chunk carries it, whole, at position `index` among the calls.
fn tool_call_chunk(index: usize, call: &ToolCallResponse) -> ChatCompletionMessageToolCallChunk {
    ChatCompletionMessageToolCallChunk {
        index: u32::try_from(index).unwrap_or(u32::MAX),
        id: Some(call.id.clone()),
        r#type: Some(FunctionType::Function),
        function: Some(FunctionCallStream {
            name: Some(call.function.name.clone()),
            arguments: Some(call.function.arguments.clone()),
        }),
    }
}

/// A tool call as a non-streamed response carries it.
fn tool_call(call: &ToolCallResponse) -> ChatCompletionMessageToolCalls {
    ChatCompletionMessageToolCalls::Function(ChatCompletionMessageToolCall {
        id: call.id.clone(),
        function: FunctionCall {
            name: call.function.name.clone(),
            arguments: call.function.arguments.clone(),
        },
    })
}

/// Renders a conversation the way the mistral.rs engine does.
fn render(
    template: &Template,
    special: &SpecialTokens,
    messages: Vec<IndexMap<String, MessageContent>>,
    enable_thinking: Option<bool>,
    reasoning_effort: Option<TemplateReasoningEffort>,
    tools: Vec<Tool>,
) -> std::result::Result<String, String> {
    apply_chat_template_to(
        messages,
        true,
        enable_thinking,
        reasoning_effort,
        &ChatTemplateValue(template.clone()),
        special.bos.clone(),
        special.eos.clone(),
        None,
        tools,
    )
    .map_err(|e| e.to_string())
}

/// The decode thread: loads the model, reports readiness, then runs jobs in order.
fn serve(
    name: &str,
    path: &Path,
    draft: Option<&Path>,
    devices: &[llama_cpp::rpc::Device],
    n_ctx: Option<NonZeroU32>,
    ready: oneshot::Sender<std::result::Result<Loaded, ChatError>>,
    jobs: &std_mpsc::Receiver<Job>,
) {
    let backend = Backend::get();
    // On one node the model goes to this node's GPUs; split across nodes, every weight is
    // split across `devices`, and so is a draft model's.
    let params = ModelParams::default().with_devices(devices.to_vec());
    // A GGUF may carry its own MTP (NextN) layers, which only load when asked for; a file
    // without them loads as it would otherwise. A model that no longer fits with its MTP
    // layer still loads without it, and decodes without drafting.
    let (model, loaded_mtp) = match Model::load(backend, path, &params.clone().with_load_mtp(true))
    {
        Ok(model) => (model, true),
        Err(with_mtp) => {
            let Ok(model) = Model::load(backend, path, &params) else {
                let _ = ready.send(Err(load_error(path, with_mtp)));
                return;
            };
            tracing::info!("{}", without_mtp_note(name, &with_mtp));
            (model, false)
        }
    };
    // This engine decodes one token after another with a decoder alone. An encoder-decoder
    // model's decoder aborts the process when no encoder pass ran, and a diffusion model
    // decodes nothing, so both are refused while they load.
    if model.has_encoder() || model.is_diffusion() {
        let _ = ready.send(Err(ChatError::UnsupportedArchitecture {
            architecture: model.architecture().unwrap_or_default(),
            file: file_name(path),
        }));
        return;
    }
    let draft_model = match draft.map(|d| {
        // A DFlash or DSpark drafter says so in its header; any other draft file is an MTP head.
        let kind = DraftKind::dflash_of(d).unwrap_or(DraftKind::Mtp);
        // A draft model goes where the target is: an MTP head drafts through the target's
        // context, and a DFlash or DSpark drafter uses the target's own output head.
        let params = params.clone().with_load_mtp(kind == DraftKind::Mtp);
        let head = Model::load(backend, d, &params).map_err(|e| {
            format!(
                "llama.cpp could not load the draft model '{}': {e}",
                d.display()
            )
        })?;
        if kind != DraftKind::Mtp || head.n_layer_nextn() > 0 {
            Ok((head, kind))
        } else {
            Err(not_a_head(d))
        }
    }) {
        Some(Err(message)) => {
            let _ = ready.send(Err(failed(message)));
            return;
        }
        Some(Ok(model)) => Some(model),
        None => None,
    };
    // Without a separate head, a model with its own MTP layer drafts for itself, as
    // llama.cpp's server does. A model loaded without the layer still counts it in its
    // header, so only one loaded with it drafts.
    let drafts_itself = draft_model.is_none() && loaded_mtp && model.n_layer_nextn() > 0;
    let split = devices.len() > 1;
    let mtp_len = mtp_draft(&model, split);
    // The model that drafts, if any, and how.
    let head = match &draft_model {
        Some((head, kind)) => Some((head, *kind)),
        None => drafts_itself.then_some((&model, DraftKind::Mtp)),
    };
    // The contexts borrow the models, so all of them live in this frame for the thread's
    // lifetime. With a draft model, verification rolls back up to its draft length of rejected
    // tokens, and a hybrid model's recurrent layers can roll back only as many tokens as the
    // context keeps state snapshots for, so it keeps that many, as llama.cpp's server sizes it.
    let snapshots = head.map_or(0, |(_, kind)| {
        u32::try_from(draft_len(kind, mtp_len)).unwrap_or(0)
    });
    let context_params = ContextParams::default()
        .with_n_ctx(n_ctx)
        .with_n_batch(u32::try_from(PROMPT_BATCH).unwrap_or(u32::MAX))
        .with_n_rs_seq(snapshots);
    let context = match model.new_context(backend, &context_params) {
        Ok(context) => context,
        Err(e) => {
            let _ = ready.send(Err(failed(format!(
                "llama.cpp could not create a context: {e}"
            ))));
            return;
        }
    };
    let mut decoder = match head {
        None => Decoder::Plain(context),
        Some((head, kind)) => match mtp(head, kind, context, mtp_len) {
            Ok(spec) => Decoder::Mtp(Box::new(spec)),
            Err(message) => {
                let _ = ready.send(Err(failed(message)));
                return;
            }
        },
    };
    let vocab = model.vocab();
    let special = SpecialTokens {
        bos: piece(&model, vocab.bos()),
        eos: piece(&model, vocab.eos()),
    };
    // How far the target's cache can be cut back decides whether reusing it needs checkpoints.
    let truncation = match &mut decoder {
        Decoder::Plain(context) => context.truncation(),
        Decoder::Mtp(spec) => spec.target_mut().truncation(),
    };
    let mut cache = PromptCache::new(name, truncation);
    let loaded = Loaded {
        template: model.chat_template(),
        architecture: model.architecture(),
        special,
        harmony: harmony::Markers::find(vocab),
        reused_prompt_tokens: Arc::clone(&cache.reused),
    };
    // Ruled out while a required tool call is outstanding.
    let eog = vocab.eog_tokens();
    if ready.send(Ok(loaded)).is_err() {
        return;
    }
    while let Ok(job) = jobs.recv() {
        // A client that has gone, as one that stopped reading before a later choice's turn,
        // is not decoded for.
        if job.events.is_closed() {
            continue;
        }
        let events = job.events.clone();
        let stopping = Arc::clone(&job.stopping);
        let result = match &mut decoder {
            Decoder::Plain(context) => generate(&model, context, &mut cache, job, &eog),
            Decoder::Mtp(spec) => generate_mtp(&model, spec, split, &mut cache, job, &eog),
        };
        let event = match result {
            Ok(outcome) => Event::Done(outcome),
            Err(message) => Event::Failed(message),
        };
        send_unless_stopping(&events, &stopping, event);
    }
}

/// Sends `event` to its client, waiting while the client's buffer is full, and returns whether
/// the client took it. A stopping thread does not wait for a client that has stopped reading:
/// it gives up as soon as it sees `stopping`, which a blocked send would never notice.
fn send_unless_stopping(events: &mpsc::Sender<Event>, stopping: &AtomicBool, event: Event) -> bool {
    let mut event = event;
    loop {
        match events.try_send(event) {
            Ok(()) => return true,
            Err(mpsc::error::TrySendError::Closed(_)) => return false,
            Err(mpsc::error::TrySendError::Full(back)) => {
                if stopping.load(Ordering::Relaxed) {
                    return false;
                }
                event = back;
                std::thread::sleep(SEND_RETRY);
            }
        }
    }
}

/// Why a model loads without drafting: its MTP layer would not load beside it.
fn without_mtp_note(model: &str, error: &llama_cpp::Error) -> String {
    format!(
        "Model '{model}' loaded without its MTP layer, so it decodes without drafting: the model did not load with it. Cause: {error}"
    )
}

/// The error for a file named as an MTP draft head that carries no MTP layer.
fn not_a_head(path: &Path) -> String {
    let file = path.file_name().map_or_else(
        || path.display().to_string(),
        |n| n.to_string_lossy().to_string(),
    );
    format!(
        "'{file}' is named as an MTP draft head (it starts with `mtp-`), but it carries no MTP layer. Remove it from `files`, or rename it if it is a shard of the model."
    )
}

/// What a model that sets `paged_attention: disabled` is told at load: llama.cpp manages its
/// own KV cache, so the setting has nothing to turn off.
fn paged_attention_note(model: &str) -> String {
    format!(
        "Model '{model}' sets `paged_attention: disabled`, which has no effect: GGUF models are served by llama.cpp, which manages its own KV cache. Remove the parameter to silence this message."
    )
}

fn failed(message: String) -> ChatError {
    ChatError::FailedToLoadModel {
        source: message.into(),
    }
}

/// The chat template in `file`: the whole of a `.jinja` file, or the `chat_template` of a
/// `tokenizer_config.json`, which holds either one template or a list of named ones. A
/// `tokenizer_config.json` without a `chat_template` holds none.
async fn template_in_file(file: &Path) -> Result<Option<Template>> {
    let unreadable = |reason: String| {
        failed(format!(
            "could not read the chat template in '{}': {reason}",
            file.display()
        ))
    };
    let text = tokio::fs::read_to_string(file)
        .await
        .map_err(|e| unreadable(e.to_string()))?;
    if file
        .extension()
        .is_some_and(|extension| extension == "jinja")
    {
        return Ok(Some(Either::Left(text)));
    }
    let config: serde_json::Value =
        serde_json::from_str(&text).map_err(|e| unreadable(e.to_string()))?;
    match config.get("chat_template") {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(serde_json::Value::String(template)) => Ok(Some(Either::Left(template.clone()))),
        Some(named @ serde_json::Value::Array(_)) => {
            let named: Vec<HashMap<String, String>> = serde_json::from_value(named.clone())
                .map_err(|_| {
                    unreadable(
                        "its `chat_template` list holds an entry that is not names and templates"
                            .to_string(),
                    )
                })?;
            let template = Either::Right(named);
            chosen_template(&template, false).map_err(unreadable)?;
            Ok(Some(template))
        }
        Some(_) => Err(unreadable(
            "its `chat_template` is neither a template nor a list of named templates".to_string(),
        )),
    }
}

/// Every template `template` holds.
fn templates(template: &Template) -> impl Iterator<Item = &str> {
    let (one, named) = match template {
        Either::Left(one) => (Some(one.as_str()), None),
        Either::Right(named) => (None, Some(named)),
    };
    one.into_iter()
        .chain(named.into_iter().flatten().flat_map(|entry| {
            ["template", "tool_use", "default"]
                .into_iter()
                .filter_map(|key| entry.get(key).map(String::as_str))
        }))
}

/// The template a request renders with, chosen as the mistral.rs engine chooses: of named
/// templates, `tool_use` for a request with tools and `default` for one without. A request
/// with tools fails where no template is named `tool_use`.
fn chosen_template(template: &Template, with_tools: bool) -> std::result::Result<&str, String> {
    let named = match template {
        Either::Left(one) => return Ok(one),
        Either::Right(named) => named,
    };
    let has_tool_use = named.iter().any(|entry| {
        entry.get("name").is_some_and(|name| name == "tool_use") || entry.contains_key("tool_use")
    });
    if with_tools && !has_tool_use {
        return Err(
            "Tools were provided but this chat template does not handle tool usage".to_string(),
        );
    }
    let mut found = None;
    for entry in named {
        if let Some(name) = entry.get("name") {
            found = entry.get("template");
            if (name == "tool_use" && with_tools) || (name == "default" && !with_tools) {
                break;
            }
        } else if with_tools && entry.contains_key("tool_use") {
            found = entry.get("tool_use");
            break;
        } else if !with_tools && entry.contains_key("default") {
            found = entry.get("default");
            break;
        }
    }
    found.map(String::as_str).ok_or_else(|| {
        "its `chat_template` list holds no template named `default` or `tool_use`".to_string()
    })
}

/// Why `path` did not load. An architecture this release's llama.cpp does not know gets its
/// own message, since the fix is a different model or Spice release, not a bug report.
fn load_error(path: &Path, error: llama_cpp::Error) -> ChatError {
    const UNKNOWN: &str = "unknown model architecture: '";
    let message = error.to_string();
    if let Some(at) = message.find(UNKNOWN) {
        let rest = &message[at + UNKNOWN.len()..];
        if let Some(end) = rest.find('\'') {
            return ChatError::UnsupportedArchitecture {
                architecture: rest[..end].to_string(),
                file: file_name(path),
            };
        }
    }
    ChatError::FailedToLoadModel {
        source: Box::new(error),
    }
}

/// A Harmony reply (gpt-oss) is read token by token, by its markers, so a model whose chat
/// template writes one is served only when its vocabulary holds every marker as one token, and
/// refused otherwise rather than served with its channel markup in the content. Returns the
/// markers a Harmony template's replies are read by.
fn check_reply_format(
    template: &Template,
    architecture: Option<String>,
    markers: Option<harmony::Markers>,
    file: String,
) -> Result<Option<harmony::Markers>> {
    if !templates(template).any(is_harmony_template) {
        return Ok(None);
    }
    markers
        .map(Some)
        .ok_or_else(|| ChatError::UnsupportedArchitecture {
            architecture: architecture.unwrap_or_else(|| "gpt-oss".to_string()),
            file,
        })
}

/// The file an error names: its name, or the whole path when it has none.
fn file_name(path: &Path) -> String {
    path.file_name().map_or_else(
        || path.display().to_string(),
        |n| n.to_string_lossy().to_string(),
    )
}

/// The decode thread's engine: plain decoding, or decoding with an MTP draft head.
enum Decoder<'m> {
    Plain(Context<'m>),
    Mtp(Box<Mtp<'m>>),
}

/// Creates an MTP draft context linked to `context`, as llama.cpp's own server does: an
/// MTP-type context of the same size, reading the target's hidden states.
fn mtp<'m>(
    draft_model: &'m Model,
    kind: DraftKind,
    context: Context<'m>,
    mtp_len: i32,
) -> std::result::Result<Mtp<'m>, String> {
    let params = ContextParams::default()
        .with_n_ctx(NonZeroU32::new(context.n_ctx()))
        .with_n_batch(u32::try_from(PROMPT_BATCH).unwrap_or(u32::MAX));
    Mtp::new(
        context,
        draft_model,
        &params,
        MtpParams {
            kind,
            n_max: draft_len(kind, mtp_len),
            n_min: 0,
            p_min: 0.0,
        },
    )
    .map_err(|e| format!("llama.cpp could not start drafting: {e}"))
}

/// What prompt reading needs from either engine.
trait Target<'m> {
    fn context(&self) -> &Context<'m>;
    fn context_mut(&mut self) -> &mut Context<'m>;
    /// Removes every cache's entries from position `from` on: the target's, and a draft
    /// model's, which holds the same positions. False when one cannot roll back that far.
    fn truncate(&mut self, from: i32) -> bool;
    /// Empties every cache, the target's and a draft model's.
    fn clear(&mut self);
    /// Called after every target decode; the MTP head reads the batch's hidden states.
    fn decoded(&mut self, batch: &Batch) -> std::result::Result<(), String>;
}

impl<'m> Target<'m> for Context<'m> {
    fn context(&self) -> &Context<'m> {
        self
    }
    fn context_mut(&mut self) -> &mut Context<'m> {
        self
    }
    fn truncate(&mut self, from: i32) -> bool {
        Context::truncate(self, from)
    }
    fn clear(&mut self) {
        Context::clear(self);
    }
    fn decoded(&mut self, _: &Batch) -> std::result::Result<(), String> {
        Ok(())
    }
}

impl<'m> Target<'m> for Mtp<'m> {
    fn context(&self) -> &Context<'m> {
        self.target()
    }
    fn context_mut(&mut self) -> &mut Context<'m> {
        self.target_mut()
    }
    fn truncate(&mut self, from: i32) -> bool {
        self.target_mut().truncate(from) && self.draft_mut().truncate(from)
    }
    fn clear(&mut self) {
        self.target_mut().clear();
        self.draft_mut().clear();
    }
    fn decoded(&mut self, batch: &Batch) -> std::result::Result<(), String> {
        self.process(batch).map_err(|e| e.to_string())
    }
}

/// What the KV cache holds from the last request, so the next one decodes only the part of its
/// prompt that differs, as llama.cpp's server reuses a slot's cached prompt. A chat's next turn
/// repeats the conversation so far, so it decodes little more than the reply and the new
/// message.
struct PromptCache {
    /// The model's name, for the log line.
    model: String,
    /// The tokens in the KV cache, in order: the last prompt, then the reply tokens decoded
    /// after it. Empty while a request runs, so one that fails leaves nothing to reuse.
    tokens: Vec<Token>,
    /// The cache namespace of the caller whose request left `tokens` and the checkpoints.
    namespace: Option<CacheNamespace>,
    /// For a cache that cannot be cut back to any position, as a recurrent state or a
    /// compressed cache cannot.
    checkpoints: Option<Checkpoints>,
    /// How many prompt tokens the last request read from the cache.
    reused: Arc<AtomicUsize>,
    /// What saving a checkpoint costs against reading prompt tokens again.
    cost: CheckpointCost,
}

impl PromptCache {
    fn new(name: &str, truncation: Truncation) -> Self {
        Self {
            model: name.to_string(),
            tokens: Vec::new(),
            namespace: None,
            checkpoints: (truncation != Truncation::Anywhere).then(Checkpoints::default),
            reused: Arc::default(),
            cost: CheckpointCost::default(),
        }
    }

    /// Readies `target` to read `tokens`, and returns how many of them its cache already holds:
    /// the longest prefix they share with the cache, short of the last token, whose logits the
    /// request samples from. A cache that cannot be cut back to any position is used as it
    /// stands only when the prompt extends it; otherwise it restores its latest checkpoint within
    /// that prefix, and failing that, the cache is emptied.
    ///
    /// Only a request from the caller in `namespace` whose request filled the cache reuses it:
    /// how long a request takes must not say what another caller asked. Another caller's
    /// request starts from an empty cache, its checkpoints dropped.
    fn reuse<'m>(
        &mut self,
        target: &mut impl Target<'m>,
        tokens: &[Token],
        namespace: &CacheNamespace,
    ) -> usize {
        let mut cached = std::mem::take(&mut self.tokens);
        if self.namespace.as_ref() != Some(namespace) {
            cached.clear();
            if let Some(checkpoints) = &mut self.checkpoints {
                checkpoints.clear();
            }
            self.namespace = Some(namespace.clone());
        }
        let common = cached
            .iter()
            .zip(tokens)
            .take_while(|(a, b)| a == b)
            .count();
        let shared = common.min(tokens.len().saturating_sub(1));
        // A checkpoint past the shared prefix holds the state of tokens this prompt lacks.
        if let Some(checkpoints) = &mut self.checkpoints {
            checkpoints.truncate(shared);
        }
        // A recurrent state is never rolled back to a prompt prefix, as llama.cpp's server never
        // rolls one back: its rollback snapshots serve rejected drafts, and a decode refreshes only
        // as many of them as it has tokens, so after a shorter decode a rollback can restore a
        // snapshot that predates it.
        let truncates = self.checkpoints.is_none() || shared == cached.len();
        let reused = if shared > 0 && truncates && truncate_to(target, shared) {
            shared
        } else if let Some(at) = self
            .checkpoints
            .as_ref()
            .and_then(|checkpoints| checkpoints.restore(target.context_mut()))
            && truncate_to(target, at)
        {
            at
        } else {
            target.clear();
            if let Some(checkpoints) = &mut self.checkpoints {
                checkpoints.clear();
            }
            0
        };
        tracing::debug!(
            "Model '{}' reused {reused} of its {} prompt tokens from the previous request's cache.",
            self.model,
            tokens.len()
        );
        self.reused.store(reused, Ordering::Relaxed);
        reused
    }

    /// Where reading a prompt of `len` tokens from `start` on saves a checkpoint, if anywhere:
    /// only where reading the tokens it covers again would cost the next request more than
    /// saving it costs this one.
    fn checkpoint_at(&self, start: usize, len: usize) -> Option<usize> {
        let at = len.checked_sub(CHECKPOINT_BEFORE_END)?;
        (self.checkpoints.is_some() && at > start && self.cost.pays(at - start)).then_some(at)
    }

    fn save_checkpoint(&mut self, context: &Context<'_>, n_tokens: usize) {
        if let Some(checkpoints) = &mut self.checkpoints {
            let started = Instant::now();
            if let Some(bytes) = checkpoints.save(context, n_tokens) {
                let took = started.elapsed();
                self.cost.saved(took);
                tracing::debug!(
                    "Model '{}' saved a {bytes}-byte checkpoint after {n_tokens} prompt tokens in {:.1} ms.",
                    self.model,
                    took.as_secs_f64() * 1000.0
                );
            }
        }
    }

    /// Records that reading `tokens` prompt tokens took `took`, checkpoint saves aside.
    fn read(&mut self, tokens: usize, took: Duration) {
        self.cost.read(tokens, took);
    }

    /// Records `tokens` as what the caches hold for the next request, after truncating them to
    /// `tokens`, which drops any drafts verified after them. A cache that cannot roll back that
    /// far leaves nothing to reuse.
    fn keep<'m>(&mut self, target: &mut impl Target<'m>, tokens: Vec<Token>) {
        if truncate_to(target, tokens.len()) {
            self.tokens = tokens;
        }
    }
}

/// A model's recurrent state saved after a prompt's first tokens, oldest first, as llama.cpp's
/// server keeps context checkpoints: the attention cache truncates to any position, but the
/// recurrent state rolls back only to one of these.
#[derive(Default)]
struct Checkpoints(Vec<Checkpoint>);

struct Checkpoint {
    /// How many tokens the state follows.
    n_tokens: usize,
    state: Vec<u8>,
}

impl Checkpoints {
    /// Saves `context`'s state after its first `n_tokens` tokens, which follow every earlier
    /// checkpoint's, and returns its size in bytes; `None` when llama.cpp saves nothing.
    fn save(&mut self, context: &Context<'_>, n_tokens: usize) -> Option<usize> {
        let state = context.partial_state()?;
        if self.0.len() == MAX_CHECKPOINTS {
            self.0.remove(0);
        }
        let bytes = state.len();
        self.0.push(Checkpoint { n_tokens, state });
        Some(bytes)
    }

    /// Forgets every checkpoint after the first `n_tokens` tokens.
    fn truncate(&mut self, n_tokens: usize) {
        self.0.retain(|checkpoint| checkpoint.n_tokens <= n_tokens);
    }

    /// Restores the latest checkpoint into `context`, and returns how many tokens it follows.
    /// `None` when there is none, or llama.cpp could not restore it, which leaves `context`'s
    /// cache empty.
    fn restore(&self, context: &mut Context<'_>) -> Option<usize> {
        let latest = self.0.last()?;
        context
            .restore_partial_state(&latest.state)
            .then_some(latest.n_tokens)
    }

    fn clear(&mut self) {
        self.0.clear();
    }
}

/// What saving a checkpoint costs against reading prompt tokens again, smoothed over requests.
/// On one machine a save copies the state in about a millisecond and always pays; split across
/// machines it reads every recurrent tensor back from each of them, which can cost more than
/// reading a short prompt again.
#[derive(Debug, Default)]
struct CheckpointCost {
    /// Seconds a save takes.
    save_s: Option<f64>,
    /// Seconds a prompt token takes to read.
    token_s: Option<f64>,
}

impl CheckpointCost {
    /// Whether a checkpoint covering `tokens` more prompt tokens costs less to save than they
    /// cost to read again. Until both have been measured, a checkpoint is saved.
    fn pays(&self, tokens: usize) -> bool {
        match (self.save_s, self.token_s) {
            (Some(save), Some(per_token)) => count(tokens) * per_token >= save,
            _ => true,
        }
    }

    fn saved(&mut self, took: Duration) {
        self.save_s = Some(smoothed(self.save_s, took.as_secs_f64()));
    }

    fn read(&mut self, tokens: usize, took: Duration) {
        if tokens > 0 {
            let per_token = took.as_secs_f64() / count(tokens);
            self.token_s = Some(smoothed(self.token_s, per_token));
        }
    }
}

fn smoothed(old: Option<f64>, new: f64) -> f64 {
    old.map_or(new, |old| old + COST_SMOOTHING * (new - old))
}

/// A token count as a float; counts past `u32::MAX` saturate, which no context reaches.
fn count(tokens: usize) -> f64 {
    f64::from(u32::try_from(tokens).unwrap_or(u32::MAX))
}

/// Tokenizes the prompt and decodes the part of it the cache does not already hold, returning
/// the tokens and the batch whose last logits predict the first generated token.
fn read_prompt<'m>(
    model: &Model,
    target: &mut impl Target<'m>,
    cache: &mut PromptCache,
    (prompt, namespace): (&str, &CacheNamespace),
    stopping: &AtomicBool,
) -> std::result::Result<(Vec<Token>, Batch), String> {
    let tokens = model
        .vocab()
        .tokenize(prompt, false, true)
        .map_err(|e| e.to_string())?;
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
    let start = cache.reuse(target, &tokens, namespace);
    let mut batch = Batch::new(PROMPT_BATCH);
    // The prompt is read in two spans split at the checkpoint, so the state saved there
    // follows exactly the tokens before it; without one, the first span is empty.
    let checkpoint = cache.checkpoint_at(start, tokens.len());
    let split = checkpoint.unwrap_or(start);
    // Each span is timed to its computation's end, so the reading's cost and the save's stay
    // apart; sampling, and the save itself, wait for that computation anyway.
    let started = Instant::now();
    decode_span(target, &mut batch, &tokens, start..split, stopping)?;
    target.context_mut().synchronize();
    let mut read = started.elapsed();
    if let Some(at) = checkpoint {
        cache.save_checkpoint(target.context(), at);
    }
    let resumed = Instant::now();
    decode_span(target, &mut batch, &tokens, split..tokens.len(), stopping)?;
    target.context_mut().synchronize();
    read += resumed.elapsed();
    cache.read(tokens.len() - start, read);
    Ok((tokens, batch))
}

/// Decodes `tokens[span]` at their positions, in batches of at most `PROMPT_BATCH`, asking for
/// logits only at the prompt's last token.
fn decode_span<'m>(
    target: &mut impl Target<'m>,
    batch: &mut Batch,
    tokens: &[Token],
    span: Range<usize>,
    stopping: &AtomicBool,
) -> std::result::Result<(), String> {
    for from in span.clone().step_by(PROMPT_BATCH) {
        if stopping.load(Ordering::Relaxed) {
            return Err(DECODE_STOPPED.to_string());
        }
        let to = (from + PROMPT_BATCH).min(span.end);
        batch.clear();
        for (pos, &token) in (from..to).zip(&tokens[from..to]) {
            let last = pos + 1 == tokens.len();
            let pos = i32::try_from(pos).map_err(|e| e.to_string())?;
            batch.add(token, pos, last).map_err(|e| e.to_string())?;
        }
        target
            .context_mut()
            .decode(batch)
            .map_err(|e| e.to_string())?;
        target.decoded(batch)?;
    }
    Ok(())
}

/// Sends what generation produces to the client, in order: reasoning, content, and tool
/// calls. Generation ends at an end-of-generation token, a stop sequence, the token budget,
/// or a complete tool call.
struct Output {
    events: mpsc::Sender<Event>,
    limit: Option<usize>,
    stops: StopMatcher,
    pending: Vec<u8>,
    produced: u32,
    reasoning: Option<TagReasoningContext>,
    tools: Option<ToolSplit>,
    harmony: Option<harmony::Reader>,
    require_tool_call: bool,
    /// Whether tool calls were sent; nothing is sent after them.
    called: bool,
    /// Why reading the tool calls failed, which fails the request.
    failure: Option<String>,
    stopping: Arc<AtomicBool>,
}

impl Output {
    fn new(job: Job) -> (String, Sampling, Self) {
        let output = Self {
            events: job.events,
            limit: job.limit,
            stops: StopMatcher::new(job.stop),
            pending: Vec::new(),
            produced: 0,
            reasoning: job.reasoning,
            tools: job.tools.map(ToolSplit::new),
            harmony: job.harmony,
            require_tool_call: job.require_tool_call,
            called: false,
            failure: None,
            stopping: job.stopping,
        };
        (job.prompt, job.sampling, output)
    }

    /// Whether generation has left any reasoning block, so `response_format` applies: in a
    /// Harmony reply, whether it is in its final message.
    fn in_content(&self) -> bool {
        match &self.harmony {
            Some(reader) => reader.in_grammar(),
            None => !self
                .reasoning
                .as_ref()
                .is_some_and(TagReasoningContext::is_in_think_block),
        }
    }

    /// The end-of-generation tokens a required tool call rules out: all of `eog` but the one
    /// that ends a Harmony reply with a call.
    fn ruled_out(&self, eog: &[Token]) -> Vec<Token> {
        let call = self.harmony.as_ref().map(|reader| reader.markers().call());
        eog.iter().copied().filter(|&t| Some(t) != call).collect()
    }

    /// Sends one sampled token, or returns why generation ends.
    fn token(&mut self, model: &Model, token: Token) -> Option<FinishReason> {
        let vocab = model.vocab();
        self.read(token, vocab.is_eog(token), || {
            vocab.token_to_piece(token, true)
        })
    }

    /// Sends one sampled token, which ends generation when `eog` and otherwise reads as
    /// `piece`, or returns why generation ends.
    fn read(
        &mut self,
        token: Token,
        eog: bool,
        piece: impl FnOnce() -> Vec<u8>,
    ) -> Option<FinishReason> {
        if self.stopping.load(Ordering::Relaxed) {
            self.failure = Some(DECODE_STOPPED.to_string());
            return Some(FinishReason::Stop);
        }
        // A Harmony marker is read before anything else, because two of them end generation
        // and one of those completes a tool call.
        if self
            .harmony
            .as_ref()
            .is_some_and(|r| r.marks(token, self.stops.held()))
        {
            return self.mark(token, eog);
        }
        if eog {
            return Some(FinishReason::Stop);
        }
        self.produced += 1;
        let piece = piece();
        if let Some(reader) = self.harmony.as_mut()
            && reader.in_header()
        {
            // A header names a channel and a recipient, which no stop sequence applies to.
            reader.text(&String::from_utf8_lossy(&piece));
            return self.spent();
        }
        self.bytes(&piece)
    }

    /// Reads a Harmony marker, which ends generation when `eog`.
    fn mark(&mut self, token: Token, eog: bool) -> Option<FinishReason> {
        // What the stop matcher and the character assembly still hold belongs to the message
        // the marker closes.
        let mut held = self.stops.finish();
        held.push_str(&String::from_utf8_lossy(&std::mem::take(&mut self.pending)));
        if let Some(finish) = self.route(&held) {
            return Some(finish);
        }
        match self.harmony.as_mut()?.mark(token) {
            harmony::Mark::Next => {
                if !eog {
                    self.produced += 1;
                }
                self.spent()
            }
            harmony::Mark::Call(call) => Some(self.calls(None, vec![call])),
            harmony::Mark::Done => Some(FinishReason::Stop),
        }
    }

    /// Whether the token budget is spent.
    fn spent(&self) -> Option<FinishReason> {
        self.limit
            .is_some_and(|limit| self.produced as usize >= limit)
            .then_some(FinishReason::Length)
    }

    /// Sends the text a token's bytes complete, or returns why generation ends.
    fn bytes(&mut self, piece: &[u8]) -> Option<FinishReason> {
        self.pending.extend_from_slice(piece);
        let text = take_utf8(&mut self.pending);
        let (emit, stopped) = self.stops.push(&text);
        if stopped {
            // The reply ends at the stop sequence, so nothing after it is sent, not even the
            // start of a character that `finish` would otherwise send as a replacement.
            self.pending.clear();
        }
        if let Some(finish) = self.route(&emit) {
            return Some(finish);
        }
        if stopped {
            return Some(FinishReason::Stop);
        }
        self.spent()
    }

    /// Whether the client is still there to take `event`.
    fn send(&self, event: Event) -> bool {
        send_unless_stopping(&self.events, &self.stopping, event)
    }

    /// Splits `text` into reasoning and content and sends them; content goes through the
    /// tool-call reader first. Returns why generation ends, if it does.
    fn route(&mut self, text: &str) -> Option<FinishReason> {
        if self.called || text.is_empty() {
            return None;
        }
        if let Some(reader) = self.harmony.as_mut() {
            return match reader.text(text) {
                harmony::Text::Reasoning => {
                    (!self.send(Event::Reasoning(text.to_string()))).then_some(FinishReason::Stop)
                }
                harmony::Text::Content => self.content(text.to_string()),
                harmony::Text::Held => None,
            };
        }
        let (reasoning, content) = match self.reasoning.as_mut() {
            Some(parser) => {
                parser.process_text(text);
                (parser.get_reasoning_delta(), parser.get_content_delta())
            }
            None => (None, Some(text.to_string())),
        };
        if let Some(reasoning) = reasoning
            && !self.send(Event::Reasoning(reasoning))
        {
            return Some(FinishReason::Stop);
        }
        self.content(content.unwrap_or_default())
    }

    fn content(&mut self, content: String) -> Option<FinishReason> {
        if content.is_empty() {
            return None;
        }
        let Some(tools) = self.tools.as_mut() else {
            return (!self.send(Event::Content(content))).then_some(FinishReason::Stop);
        };
        match tools.push(&content) {
            Ok(ToolPush::Hold) => None,
            Ok(ToolPush::Content(text)) => {
                (!text.is_empty() && !self.send(Event::Content(text))).then_some(FinishReason::Stop)
            }
            Ok(ToolPush::Calls(before, calls)) => Some(self.calls(before, calls)),
            Err(message) => {
                self.failure = Some(message);
                Some(FinishReason::Stop)
            }
        }
    }

    fn calls(&mut self, before: Option<String>, calls: Vec<ToolCallResponse>) -> FinishReason {
        if let Some(text) = before {
            self.send(Event::Content(text));
        }
        self.called = true;
        self.send(Event::ToolCalls(calls));
        FinishReason::ToolCalls
    }

    /// Tokens the budget still allows.
    fn remaining(&self) -> usize {
        self.limit.map_or(usize::MAX, |limit| {
            limit.saturating_sub(self.produced as usize)
        })
    }

    /// Sends what is still held back, and reads any tool call out of it.
    fn finish(
        mut self,
        finish: FinishReason,
        prompt_tokens: usize,
    ) -> std::result::Result<Outcome, String> {
        let mut finish = finish;
        if !self.called {
            // A character still incomplete when generation ends is replaced, as a lossy
            // decode of the whole reply would.
            let mut tail = self.stops.finish();
            tail.push_str(&String::from_utf8_lossy(&std::mem::take(&mut self.pending)));
            if self.route(&tail) == Some(FinishReason::ToolCalls) {
                finish = FinishReason::ToolCalls;
            }
        }
        if !self.called
            && self.failure.is_none()
            && let Some(call) = self.harmony.as_mut().and_then(harmony::Reader::finish)
        {
            finish = self.calls(None, vec![call]);
        }
        if !self.called
            && let Some(parser) = self.reasoning.as_mut()
        {
            parser.finalize();
            let (reasoning, content) = (parser.get_reasoning_delta(), parser.get_content_delta());
            if let Some(reasoning) = reasoning {
                self.send(Event::Reasoning(reasoning));
            }
            if self.content(content.unwrap_or_default()) == Some(FinishReason::ToolCalls) {
                finish = FinishReason::ToolCalls;
            }
        }
        if !self.called
            && self.failure.is_none()
            && let Some(tools) = self.tools.as_mut()
        {
            let (content, calls) = tools.finish()?;
            if !calls.is_empty() {
                finish = self.calls(content, calls);
            } else if let Some(text) = content {
                self.send(Event::Content(text));
            }
        }
        if let Some(message) = self.failure.take() {
            return Err(message);
        }
        if self.require_tool_call && !self.called {
            return Err("Tool choice was required but no tools were called.".to_string());
        }
        Ok(Outcome {
            finish,
            prompt_tokens: u32::try_from(prompt_tokens).unwrap_or(u32::MAX),
            completion_tokens: self.produced,
        })
    }
}

/// The texts that open a tool call in the formats the mistral.rs engine's parser reads.
const TOOL_CALL_OPENERS: &[&str] = &[
    "<tool_call>",
    "<tool_calls>",
    "<|python_tag|>",
    "[TOOL_CALLS]",
    "<|tool_call>",
    "<|tool_call_start|>",
    "<｜tool▁calls▁begin｜>",
    "<｜tool▁call▁begin｜>",
];

/// Holds back content that may be the start of a tool call, and reads complete calls out of
/// it with the mistral.rs engine's parser.
struct ToolSplit {
    matcher: ToolCallingMatcher,
    held: String,
}

enum ToolPush {
    /// Text that is not a tool call, to send now.
    Content(String),
    /// Text that may still become a tool call.
    Hold,
    /// Complete tool calls, with the text before them.
    Calls(Option<String>, Vec<ToolCallResponse>),
}

impl ToolSplit {
    fn new(matcher: ToolCallingMatcher) -> Self {
        Self {
            matcher,
            held: String::new(),
        }
    }

    fn push(&mut self, text: &str) -> std::result::Result<ToolPush, String> {
        self.held.push_str(text);
        let (possible, complete) = self
            .matcher
            .prefix_could_be_tool(&self.held)
            .map_err(|e| e.to_string())?;
        if complete {
            let (content, calls) = self
                .matcher
                .get_call_with_content(&self.held)
                .map_err(|e| e.to_string())?;
            if !calls.is_empty() {
                self.held.clear();
                return Ok(ToolPush::Calls(content.filter(|c| !c.is_empty()), calls));
            }
        }
        if possible && !complete {
            return Ok(ToolPush::Hold);
        }
        // A tool-call opener spelled across tokens (as a grammar makes a model spell it) is
        // not a call yet, so its unfinished start stays held until it resolves either way.
        let keep = TOOL_CALL_OPENERS
            .iter()
            .map(|opener| longest_suffix_prefix(&self.held, opener))
            .max()
            .unwrap_or(0);
        let cut = self.held.len() - keep;
        Ok(ToolPush::Content(self.held.drain(..cut).collect()))
    }

    /// The held text at the end of generation: content, or the tool calls it completes.
    fn finish(&mut self) -> std::result::Result<(Option<String>, Vec<ToolCallResponse>), String> {
        let held = std::mem::take(&mut self.held);
        if held.is_empty() {
            return Ok((None, Vec::new()));
        }
        let (content, calls) = self
            .matcher
            .get_call_with_content(&held)
            .map_err(|e| e.to_string())?;
        Ok(if calls.is_empty() {
            (Some(held), calls)
        } else {
            (content.filter(|c| !c.is_empty()), calls)
        })
    }
}

/// Picks each token with the request's sampler. A `response_format` grammar constrains the
/// content only, so it takes over when a reasoning block ends; while a required tool call is
/// outstanding, end-of-generation tokens are ruled out. Frequency and presence penalties
/// count every token the reply generates, reasoning included, as the mistral.rs engine does.
struct Picker<'v> {
    free: Sampler<'v>,
    constrained: Option<Sampler<'v>>,
    /// Whether each chain starts with the penalties, which then also hear of the tokens the
    /// other chain picks.
    penalized: bool,
}

impl<'v> Picker<'v> {
    /// `window` is how many generated tokens the penalties remember: the context's length
    /// covers a whole reply.
    fn new(
        vocab: Vocab<'v>,
        sampling: &Sampling,
        eog: &[Token],
        window: u32,
    ) -> std::result::Result<Self, String> {
        let frequency = sampling.frequency_penalty.unwrap_or(0.0);
        let presence = sampling.presence_penalty.unwrap_or(0.0);
        if !frequency.is_finite() || !presence.is_finite() {
            return Err(format!(
                "`frequency_penalty` and `presence_penalty` must be finite numbers, got {frequency} and {presence}"
            ));
        }
        let penalized = frequency != 0.0 || presence != 0.0;
        let window = i32::try_from(window).unwrap_or(i32::MAX);
        let chain = |grammar: Option<&str>| -> std::result::Result<Sampler<'v>, String> {
            let mut samplers = Vec::new();
            if penalized {
                samplers.push(Sampler::penalties(&vocab, window, frequency, presence));
            }
            if sampling.ban_eog {
                let ruled_out: Vec<_> = eog.iter().map(|&t| (t, f32::NEG_INFINITY)).collect();
                samplers.push(Sampler::logit_bias(&vocab, &ruled_out));
            }
            if let Some(grammar) = grammar {
                samplers.push(Sampler::grammar(&vocab, grammar).map_err(|e| e.to_string())?);
            }
            samplers.extend(draw(sampling));
            Ok(Sampler::chain(samplers))
        };
        Ok(Self {
            free: chain(None)?,
            constrained: sampling
                .grammar
                .as_deref()
                .map(|g| chain(Some(g)))
                .transpose()?,
            penalized,
        })
    }

    fn sample(&mut self, context: &Context<'_>, index: i32, in_content: bool) -> Token {
        let (used, other) = match &mut self.constrained {
            Some(constrained) if in_content => (constrained, Some(&mut self.free)),
            constrained => (&mut self.free, constrained.as_mut()),
        };
        let token = used.sample(context, index);
        // The penalties lead each chain; the chain that did not pick still counts the token.
        if self.penalized
            && let Some(other) = other
        {
            other.accept_at(0, token);
        }
        token
    }
}

/// The samplers that pick the token from those the rest leave: greedy at temperature 0 or
/// unset, as the mistral.rs engine's deterministic default; otherwise the mistral.rs
/// engine's order of temperature, then top-p over the tempered probabilities, then a seeded
/// draw.
fn draw<'v>(sampling: &Sampling) -> Vec<Sampler<'v>> {
    match sampling.temperature {
        Some(t) if t > 0.0 => vec![
            Sampler::temp(t),
            Sampler::top_p(sampling.top_p.unwrap_or(1.0), 1),
            Sampler::dist(sampling.seed),
        ],
        _ => vec![Sampler::greedy()],
    }
}

/// Runs one request, one token per forward pass.
fn generate(
    model: &Model,
    context: &mut Context<'_>,
    cache: &mut PromptCache,
    job: Job,
    eog: &[Token],
) -> std::result::Result<Outcome, String> {
    let namespace = job.namespace.clone();
    let (prompt, sampling, mut out) = Output::new(job);
    let vocab = model.vocab();
    let mut picker = Picker::new(vocab, &sampling, &out.ruled_out(eog), context.n_ctx())?;
    // `tokens` grows by every reply token decoded, so it is always what the cache holds.
    let (mut tokens, mut batch) =
        read_prompt(model, context, cache, (&prompt, &namespace), &out.stopping)?;
    let prompt_tokens = tokens.len();
    let n_ctx = usize::try_from(context.n_ctx()).unwrap_or(usize::MAX);
    let finish = loop {
        let token = picker.sample(context, batch.n_tokens() - 1, out.in_content());
        if let Some(finish) = out.token(model, token) {
            break finish;
        }
        if tokens.len() + 1 >= n_ctx {
            break FinishReason::Length;
        }
        batch.clear();
        let pos = i32::try_from(tokens.len()).map_err(|e| e.to_string())?;
        batch.add(token, pos, true).map_err(|e| e.to_string())?;
        context.decode(&batch).map_err(|e| e.to_string())?;
        tokens.push(token);
    };
    cache.keep(context, tokens);
    out.finish(finish, prompt_tokens)
}

/// Runs one request with MTP drafting: each forward pass verifies the last token plus up
/// to its draft length of drafted ones, and keeps the drafts the target would have sampled.
///
/// A model `split` across nodes verifies every drafted token even when the request has fewer
/// left, so each verify batch keeps one shape: a shorter batch at a request's end rebuilds the
/// verify graph twice (to the short shape and back), which over RPC re-serializes every rank's
/// graph. Tokens past the limit are verified but never emitted.
fn generate_mtp(
    model: &Model,
    spec: &mut Mtp<'_>,
    split: bool,
    cache: &mut PromptCache,
    job: Job,
    eog: &[Token],
) -> std::result::Result<Outcome, String> {
    let namespace = job.namespace.clone();
    let (prompt, sampling, mut out) = Output::new(job);
    let vocab = model.vocab();
    let mut picker = Picker::new(vocab, &sampling, &out.ruled_out(eog), spec.target().n_ctx())?;
    // `read_prompt` hands every decoded prompt batch to the head, which needs the target's
    // hidden state at each prompt position before `begin`; its cache keeps the positions the
    // target's reuses.
    let (mut history, mut batch) =
        read_prompt(model, spec, cache, (&prompt, &namespace), &out.stopping)?;
    spec.begin(&history).map_err(|e| e.to_string())?;
    let n_ctx = usize::try_from(spec.target().n_ctx()).unwrap_or(usize::MAX);
    let prompt_tokens = history.len();
    let mut n_past = i32::try_from(prompt_tokens).map_err(|e| e.to_string())?;
    let mut id = picker.sample(spec.target(), batch.n_tokens() - 1, out.in_content());
    let full_draft = split;
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
        draft.truncate(if full_draft {
            n_ctx - past - 1
        } else {
            out.remaining().min(n_ctx - past - 1)
        });
        // The tokens the head drafted; with `full_draft` a shorter draft is padded to the head's
        // length so every verify batch keeps one shape, and only these can be accepted.
        let real = draft.len();
        if full_draft && real > 0 {
            let want = spec.n_max().min(n_ctx - past - 1);
            if real < want {
                let last = draft[real - 1];
                draft.resize(want, last);
            }
        }
        // Drafting wrote its positions into the head's cache, and processing the verify batch
        // writes them again, so they go first.
        truncate_cache(spec.draft_mut(), n_past)?;

        batch.clear();
        batch.add(id, n_past, true).map_err(|e| e.to_string())?;
        for (i, &token) in draft.iter().enumerate() {
            let at = n_past + 1 + i32::try_from(i).map_err(|e| e.to_string())?;
            batch.add(token, at, true).map_err(|e| e.to_string())?;
        }
        spec.target_mut()
            .decode(&batch)
            .map_err(|e| e.to_string())?;
        spec.process(&batch).map_err(|e| e.to_string())?;
        history.push(id);

        let mut accepted: usize = 0;
        let next = loop {
            let index = i32::try_from(accepted).map_err(|e| e.to_string())?;
            let token = picker.sample(spec.target(), index, out.in_content());
            if accepted < real && token == draft[accepted] {
                accepted += 1;
                // An accepted draft was decoded in the verify batch, so the cache holds it
                // whether or not it ends the reply, and the next request may reuse it.
                history.push(token);
                if let Some(finish) = out.token(model, token) {
                    break 'generate finish;
                }
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
        truncate_cache(spec, n_past)?;
        id = next;
    };
    // The prompt and the reply tokens before the one that finished it.
    cache.keep(spec, history);
    out.finish(finish, prompt_tokens)
}

/// Removes sequence 0's cache entries from position `from` on. A context that cannot roll
/// back that far, such as a recurrent layer with too few state snapshots, fails the request
/// instead of decoding on from a state that still holds the removed tokens.
fn truncate_cache<'m>(target: &mut impl Target<'m>, from: i32) -> std::result::Result<(), String> {
    if target.truncate(from) {
        Ok(())
    } else {
        Err(format!(
            "llama.cpp could not roll its cache back to position {from}"
        ))
    }
}

/// Truncates every cache `target` keeps to its first `n` tokens; false when one cannot roll
/// back that far.
fn truncate_to<'m>(target: &mut impl Target<'m>, n: usize) -> bool {
    i32::try_from(n).is_ok_and(|from| target.truncate(from))
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

fn piece(model: &Model, token: Token) -> Option<String> {
    if token.0 < 0 {
        return None;
    }
    String::from_utf8(model.vocab().token_to_piece(token, true))
        .ok()
        .filter(|s| !s.is_empty())
}

/// Removes and returns the text `bytes` decode to, keeping an incomplete trailing character
/// for the next token. Each sequence that can never become valid is replaced, as a lossy
/// decode replaces it, so the text does not depend on where tokens split the bytes.
fn take_utf8(bytes: &mut Vec<u8>) -> String {
    let mut text = String::with_capacity(bytes.len());
    let mut kept = 0;
    let mut chunks = bytes.utf8_chunks().peekable();
    while let Some(chunk) = chunks.next() {
        text.push_str(chunk.valid());
        let invalid = chunk.invalid();
        if invalid.is_empty() {
            continue;
        }
        let incomplete = chunks.peek().is_none()
            && std::str::from_utf8(invalid).is_err_and(|e| e.error_len().is_none());
        if incomplete {
            kept = invalid.len();
        } else {
            text.push(char::REPLACEMENT_CHARACTER);
        }
    }
    let consumed = bytes.len() - kept;
    bytes.drain(..consumed);
    text
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

    /// The text held back as the possible start of a stop sequence.
    fn held(&self) -> &str {
        &self.held
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

/// The parser that tells a reply's reasoning apart from its answer, chosen from the chat
/// template and the rendered prompt as the mistral.rs engine chooses it: Gemma 4's channel
/// tags, `<think>` tags (starting inside the block when the prompt opens it), or the Gemma
/// family's `<unused94>` thought tokens. A template that marks no reasoning gets none, and
/// the whole reply is content.
fn reasoning_parser(template: &str, prompt: &str) -> Option<TagReasoningContext> {
    let harmony = is_harmony_template(template);
    if is_channel_tag_template(template) && !harmony {
        Some(if prompt.contains(GEMMA_THINK_TOKEN) {
            TagReasoningContext::new_gemma_channel_with_implicit_thinking()
        } else {
            TagReasoningContext::new_gemma_channel()
        })
    } else if is_think_tag_template(template) && !harmony {
        Some(if prompt.trim_end().ends_with(THINK_OPEN_TAG) {
            TagReasoningContext::new_in_think_block()
        } else {
            TagReasoningContext::new_think_tags()
        })
    } else if is_gemma_turn_template(template) {
        Some(TagReasoningContext::new_gemma_thought())
    } else {
        None
    }
}

/// The tools `tool_choice` obliges the model to call one of: none when it need not call any.
fn obliged_tools<'t>(choice: Option<&ToolChoice>, tools: &'t [Tool]) -> Vec<&'t Tool> {
    let named = |name: &str| tools.iter().filter(|t| t.function.name == name).collect();
    match choice {
        Some(ToolChoice::Required) => tools.iter().collect(),
        Some(ToolChoice::Tool(tool)) => named(&tool.function.name),
        Some(ToolChoice::NamedFunction(function)) => named(&function.name),
        Some(ToolChoice::AllowedTools(allowed))
            if matches!(allowed.mode, AllowedToolsMode::Required) =>
        {
            allowed_tools(allowed, tools)
        }
        _ => Vec::new(),
    }
}

/// The tools a reply may call under `tool_choice`: none under `none`, since a call is then only
/// ever content; the allowed ones under `allowed_tools`; and otherwise every tool offered.
fn callable_tools<'t>(choice: Option<&ToolChoice>, tools: &'t [Tool]) -> Vec<&'t Tool> {
    match choice {
        Some(ToolChoice::None) => Vec::new(),
        Some(ToolChoice::AllowedTools(allowed)) => allowed_tools(allowed, tools),
        _ => tools.iter().collect(),
    }
}

/// The names a Harmony reply's call may take: the tools `tool_choice` obliges a call to, when
/// it obliges one, as a named choice does; otherwise the callable ones.
fn nameable_tools(callable: &[&Tool], obliged: &[&Tool]) -> Vec<String> {
    let tools = if obliged.is_empty() {
        callable
    } else {
        obliged
    };
    tools.iter().map(|t| t.function.name.clone()).collect()
}

/// The tools among `tools` that `allowed` names.
fn allowed_tools<'t>(allowed: &AllowedToolsToolChoice, tools: &'t [Tool]) -> Vec<&'t Tool> {
    tools
        .iter()
        .filter(|t| {
            allowed.tools.iter().any(
                |a| matches!(a, AllowedToolChoice::Function { name } if *name == t.function.name),
            )
        })
        .collect()
}

/// Whether a chat template writes tool calls as `<tool_call>{"name": ..., "arguments": ...}</tool_call>`,
/// as Qwen and Hermes templates do: the one tool-call format this engine can force with a
/// grammar.
fn tags_tool_calls(template: &str) -> bool {
    template.contains("<tool_call>") && template.contains("</tool_call>")
}

/// The grammar the content must match for one request, if any:
/// - a required call to one of `obliged`, in a template that tags tool calls: that call;
/// - a structured `response_format` with no tools offered: its JSON;
/// - a structured `response_format` beside tools, in a template that tags tool calls: either
///   a tool call or the JSON reply.
///
/// A required call in another tool format gets no grammar (generation instead cannot end
/// until a call is made); a structured reply beside tools in another format is refused.
fn content_grammar(
    model: &str,
    template: &str,
    format: Option<&ResponseFormat>,
    tools: &[&Tool],
    obliged: &[&Tool],
) -> std::result::Result<Option<String>, OpenAIError> {
    let reply = match format {
        None | Some(ResponseFormat::Text) => None,
        Some(ResponseFormat::JsonObject) => Some(serde_json::json!({ "type": "object" })),
        Some(ResponseFormat::JsonSchema { json_schema }) => Some(json_schema.schema.clone()),
    };
    let tagged = tags_tool_calls(template);
    if !obliged.is_empty() {
        // The turn must be a tool call, so a structured reply waits for a later turn.
        return if tagged {
            json_grammar(&call_schema(obliged), "tools").map(|call| Some(tagged_call(&call)))
        } else {
            Ok(None)
        };
    }
    let Some(reply) = reply else {
        return Ok(None);
    };
    let reply = json_grammar(&reply, "response_format")?;
    if tools.is_empty() {
        return Ok(Some(reply));
    }
    if !tagged {
        return Err(OpenAIError::InvalidArgument(format!(
            "Model '{model}' can't combine a `json_schema` or `json_object` `response_format` with `tools`: its chat template's tool-call format can't be enforced alongside a schema. Send one or the other."
        )));
    }
    let call = json_grammar(&call_schema(tools), "tools")?;
    Ok(Some(either_grammar(&call, &reply)))
}

/// The grammar of a whole Harmony reply that must call one of `tools`: any reasoning, then a
/// `commentary` message addressed to the tool, its arguments held to the tool's schema, as
/// llama.cpp's server forces one. Each marker is its token, never text spelling it.
fn harmony_call(
    markers: &harmony::Markers,
    tools: &[&Tool],
) -> std::result::Result<String, OpenAIError> {
    let [_, channel, message, _, _] = markers.terminals();
    let mut grammar = harmony_rules(markers);
    let mut calls = Vec::with_capacity(tools.len());
    for (i, tool) in tools.iter().enumerate() {
        let prefix = format!("tool{i}-");
        let arguments = json_grammar(&arguments_schema(tool), "tools")?;
        grammar.push_str(&prefix_rules(&arguments, &prefix));
        let name = serde_json::Value::String(tool.function.name.clone());
        calls.push(format!("{name} constrain? {message} {prefix}root"));
    }
    grammar.push_str("call ::= ");
    grammar.push_str(&calls.join(" | "));
    grammar.push('\n');
    let _ = writeln!(
        grammar,
        "root ::= analysis? {channel} \"commentary to=functions.\" call"
    );
    Ok(grammar)
}

/// The grammar of a whole Harmony reply held to a `response_format`: analysis messages, then
/// a final message whose body is `reply`'s JSON, as llama.cpp's server holds one. A preamble
/// is ruled out, since its text would be content outside the schema.
fn harmony_reply(markers: &harmony::Markers, reply: &str) -> String {
    let [_, channel, message, _, _] = markers.terminals();
    let mut grammar = harmony_rules(markers);
    grammar.push_str(&prefix_rules(reply, "reply-"));
    let _ = writeln!(
        grammar,
        "root ::= analysis* {channel} \"final\" constrain? {message} reply-root"
    );
    grammar
}

/// The rules both Harmony grammars share: an analysis message and the role header of the
/// message after it, and a header's optional content type.
fn harmony_rules(markers: &harmony::Markers) -> String {
    let [start, channel, message, end, constrain] = markers.terminals();
    let mut rules = String::new();
    let _ = writeln!(
        rules,
        "analysis ::= {channel} \"analysis\" {message} reasoning {end} {start} \"assistant\""
    );
    // The text between markers holds no `<|`, so it cannot spell one either.
    rules.push_str("reasoning ::= ([^<] | \"<\" [^|])*\n");
    let _ = writeln!(rules, "constrain ::= \" \" {constrain}? [A-Za-z0-9_-]+");
    rules
}

/// A tool's arguments schema: its parameters, or any object.
fn arguments_schema(tool: &Tool) -> serde_json::Value {
    tool.function.parameters.as_ref().map_or_else(
        || serde_json::json!({ "type": "object" }),
        |p| serde_json::Value::Object(p.clone().into_iter().collect()),
    )
}

/// The JSON schema of a call to one of `tools`, with the arguments held to the tool's schema.
fn call_schema(tools: &[&Tool]) -> serde_json::Value {
    let calls: Vec<serde_json::Value> = tools
        .iter()
        .map(|tool| {
            let arguments = arguments_schema(tool);
            serde_json::json!({
                "type": "object",
                "properties": {
                    "name": { "const": tool.function.name },
                    "arguments": arguments,
                },
                "required": ["name", "arguments"],
                "additionalProperties": false,
            })
        })
        .collect();
    match calls.as_slice() {
        [one] => one.clone(),
        _ => serde_json::json!({ "oneOf": calls }),
    }
}

/// llama.cpp's grammar for `schema`, its start rule `root`; `param` names the request field
/// it came from, for the error.
fn json_grammar(
    schema: &serde_json::Value,
    param: &str,
) -> std::result::Result<String, OpenAIError> {
    llama_cpp::json_schema_to_grammar(&schema.to_string())
        .map_err(|e| OpenAIError::InvalidArgument(format!("Invalid `{param}`: {e}")))
}

/// A tool call's JSON grammar wrapped in the `<tool_call>` tags.
fn tagged_call(call: &str) -> String {
    let mut grammar = prefix_rules(call, "call-");
    grammar.push_str("root ::= \"<tool_call>\\n\" call-root \"\\n</tool_call>\"\n");
    grammar
}

/// Either a tagged tool call or the reply's JSON.
fn either_grammar(call: &str, reply: &str) -> String {
    let mut grammar = prefix_rules(call, "call-");
    grammar.push_str(&prefix_rules(reply, "reply-"));
    grammar.push_str("root ::= \"<tool_call>\\n\" call-root \"\\n</tool_call>\" | reply-root\n");
    grammar
}

/// Renames every rule of a GBNF `grammar` with `prefix`, in its definitions and its
/// references, so two grammars can be joined without their shared rule names (`space`,
/// `string`, ...) colliding. Quoted literals, character classes and repetition counts are
/// left alone.
fn prefix_rules(grammar: &str, prefix: &str) -> String {
    let mut out = String::with_capacity(grammar.len() * 2);
    for line in grammar.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let (name, body) = match line.split_once("::=") {
            Some((name, body)) => (Some(name.trim()), body),
            None => (None, line),
        };
        if let Some(name) = name {
            out.push_str(prefix);
            out.push_str(name);
            out.push_str(" ::=");
        }
        let mut chars = body.chars().peekable();
        while let Some(c) = chars.next() {
            match c {
                '"' | '[' => {
                    let close = if c == '"' { '"' } else { ']' };
                    out.push(c);
                    while let Some(inner) = chars.next() {
                        out.push(inner);
                        if inner == '\\' {
                            if let Some(escaped) = chars.next() {
                                out.push(escaped);
                            }
                        } else if inner == close {
                            break;
                        }
                    }
                }
                '{' => {
                    out.push(c);
                    for inner in chars.by_ref() {
                        out.push(inner);
                        if inner == '}' {
                            break;
                        }
                    }
                }
                c if c.is_ascii_alphanumeric() || c == '-' || c == '_' => {
                    out.push_str(prefix);
                    out.push(c);
                    while let Some(&next) = chars.peek() {
                        if next.is_ascii_alphanumeric() || next == '-' || next == '_' {
                            out.push(next);
                            chars.next();
                        } else {
                            break;
                        }
                    }
                }
                c => out.push(c),
            }
        }
        out.push('\n');
    }
    out
}

/// How many choices `req` asks for: its `n`, one when unset.
fn choice_count(req: &CreateChatCompletionRequest) -> std::result::Result<usize, OpenAIError> {
    match req.n.unwrap_or(1) {
        n @ 1..=MAX_CHOICES => Ok(usize::from(n)),
        _ => Err(OpenAIError::InvalidArgument(format!(
            "`n` must be between 1 and {MAX_CHOICES}, the number of choices to generate."
        ))),
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
#[expect(
    clippy::too_many_arguments,
    reason = "one chunk's fields, each optional by the OpenAI stream shape"
)]
fn chunk(
    id: &str,
    model: &str,
    created: u32,
    index: u32,
    first: bool,
    content: Option<String>,
    tool_calls: Option<Vec<ChatCompletionMessageToolCallChunk>>,
    finish_reason: Option<FinishReason>,
    usage: Option<CompletionUsage>,
) -> CreateChatCompletionStreamResponse {
    let delta = ChatCompletionStreamResponseDelta {
        content,
        function_call: None,
        tool_calls,
        role: first.then_some(Role::Assistant),
        refusal: None,
    };
    CreateChatCompletionStreamResponse {
        id: id.to_string(),
        choices: vec![ChatChoiceStream {
            index,
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

/// A reply's usage: its prompt, which counts once however many choices read it, and the
/// completion tokens of every choice.
fn usage(prompt_tokens: u32, completion_tokens: u32) -> CompletionUsage {
    CompletionUsage {
        prompt_tokens,
        completion_tokens,
        total_tokens: prompt_tokens.saturating_add(completion_tokens),
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

/// The cache namespace of the caller this request runs for: its authenticated principal's, or
/// the shared one of an unauthenticated or internal request, as the runtime scopes each request.
async fn caller() -> CacheNamespace {
    RequestContext::current(AsyncMarker::new().await).cache_namespace()
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

    /// Measured on three DGX Sparks (GLM-5.3, the PR build): a 29-token prompt read in 270 ms,
    /// and a checkpoint saved in 300 ms. A checkpoint is worth saving only over a prefix that
    /// would take longer to read again than the save takes.
    #[test]
    fn a_checkpoint_is_saved_only_where_reading_its_tokens_again_costs_more() {
        let mut cost = CheckpointCost::default();
        assert!(cost.pays(1), "unmeasured, a checkpoint is saved");
        cost.saved(Duration::from_millis(300));
        assert!(
            cost.pays(1),
            "with the read cost unmeasured, a checkpoint is saved"
        );
        cost.read(29, Duration::from_millis(270));
        assert!(
            !cost.pays(25),
            "25 tokens read again in 233 ms, under the 300 ms save"
        );
        assert!(cost.pays(33), "33 tokens read again in 307 ms, over it");
        // A newer measurement moves the estimate a quarter of the way.
        cost.saved(Duration::from_millis(100));
        let save_s = cost.save_s.expect("a save was measured");
        assert!((save_s - 0.25).abs() < 1e-9, "smoothed save {save_s} s");
        assert!(cost.pays(27), "27 tokens in 251 ms, over the 250 ms save");
        assert!(!cost.pays(26), "26 tokens in 242 ms, under it");
    }

    fn weather_tool() -> Tool {
        let parameters = serde_json::from_value(serde_json::json!({
            "type": "object",
            "properties": { "city": { "type": "string" } },
            "required": ["city"],
        }))
        .expect("the schema is an object");
        Tool {
            tp: mistralrs_core::ToolType::Function,
            function: mistralrs_core::Function {
                name: "get_weather".to_string(),
                description: None,
                parameters: Some(parameters),
                strict: None,
            },
        }
    }

    /// An `Output` for a request offering the weather tool under `choice`, and what it sends.
    fn output(
        choice: ToolChoice,
        reasoning: Option<TagReasoningContext>,
    ) -> (Output, mpsc::Receiver<Event>) {
        let (events, rx) = mpsc::channel(STREAM_BUFFER);
        let tools = [weather_tool()];
        let matcher = ToolCallingMatcher::new_with_format(choice, Some(&tools), None)
            .expect("a matcher for one tool");
        let job = Job {
            prompt: String::new(),
            namespace: CacheNamespace::Public,
            limit: None,
            sampling: Sampling::default(),
            stop: Vec::new(),
            reasoning,
            require_tool_call: matcher.requires_tool_call(),
            tools: Some(matcher),
            harmony: None,
            stopping: Arc::default(),
            events,
        };
        let (_, _, out) = Output::new(job);
        (out, rx)
    }

    /// What the output sent, as text: `r:` reasoning, `c:` content, `t:` a tool call.
    fn sent(mut rx: mpsc::Receiver<Event>) -> Vec<String> {
        let mut out = Vec::new();
        while let Ok(event) = rx.try_recv() {
            match event {
                Event::Reasoning(text) => out.push(format!("r:{text}")),
                Event::Content(text) => out.push(format!("c:{text}")),
                Event::ToolCalls(calls) => out.extend(
                    calls
                        .iter()
                        .map(|c| format!("t:{}({})", c.function.name, c.function.arguments)),
                ),
                Event::Done(_) | Event::Failed(_) => {}
            }
        }
        out
    }

    #[test]
    fn a_tool_call_split_across_tokens_is_held_back_until_generation_ends() {
        let (mut out, rx) = output(ToolChoice::Auto, None);
        let pieces = [
            "Let me check.\n",
            "<tool_call>\n{\"name\": \"get_wea",
            "ther\", \"arguments\": {\"city\": \"Paris\"}}\n</tool_call>",
        ];
        // A Qwen call stays open while a parallel call could follow it.
        let finishes: Vec<_> = pieces.iter().map(|p| out.route(p)).collect();
        assert_eq!(finishes, [None, None, None]);
        let outcome = out
            .finish(FinishReason::Stop, 7)
            .expect("the request succeeds");
        assert_eq!(outcome.finish, FinishReason::ToolCalls);
        assert_eq!(
            sent(rx),
            [
                "c:Let me check.\n".to_string(),
                "t:get_weather({\"city\":\"Paris\"})".to_string(),
            ]
        );
    }

    #[test]
    fn text_after_a_complete_call_ends_generation_with_the_call() {
        let (mut out, rx) = output(ToolChoice::Auto, None);
        let call = "<tool_call>\n{\"name\": \"get_weather\", \"arguments\": {\"city\": \"Oslo\"}}\n</tool_call>";
        assert_eq!(out.route(call), None);
        assert_eq!(out.route("\nDone."), Some(FinishReason::ToolCalls));
        // Nothing after the call reaches the client.
        assert_eq!(out.route(" More."), None);
        let outcome = out
            .finish(FinishReason::ToolCalls, 3)
            .expect("the request succeeds");
        assert_eq!(outcome.finish, FinishReason::ToolCalls);
        assert_eq!(
            sent(rx),
            [
                "c:Done.".to_string(),
                "t:get_weather({\"city\":\"Oslo\"})".to_string(),
            ]
        );
    }

    #[test]
    fn an_opener_spelled_across_tokens_is_held_until_it_resolves() {
        let (mut out, rx) = output(ToolChoice::Auto, None);
        for piece in [
            "Sure. <",
            "tool",
            "_call",
            ">",
            "\n{\"name\":\"get_weather\",\"arguments\":{\"city\":\"Rome\"}}\n</tool_call>",
        ] {
            assert_eq!(out.route(piece), None, "{piece:?}");
        }
        let outcome = out
            .finish(FinishReason::Stop, 3)
            .expect("the request succeeds");
        assert_eq!(outcome.finish, FinishReason::ToolCalls);
        assert_eq!(
            sent(rx),
            [
                "c:Sure. ".to_string(),
                "t:get_weather({\"city\":\"Rome\"})".to_string()
            ]
        );
        // Text that only looked like an opener is sent once it can't be one.
        let (mut out, rx) = output(ToolChoice::Auto, None);
        assert_eq!(out.route("a <"), None);
        assert_eq!(out.route("b"), None);
        out.finish(FinishReason::Stop, 1)
            .expect("the request succeeds");
        assert_eq!(sent(rx).concat(), "c:a c:<b");
    }

    #[test]
    fn a_call_cut_off_by_the_end_of_generation_is_content() {
        let (mut out, rx) = output(ToolChoice::Auto, None);
        let cut = "<tool_call>\n{\"name\": \"get_weather\", \"arguments\": {\"city\": \"Oslo\"}}";
        assert_eq!(out.route(cut), None);
        let outcome = out
            .finish(FinishReason::Length, 3)
            .expect("the request succeeds");
        assert_eq!(outcome.finish, FinishReason::Length);
        assert_eq!(sent(rx), [format!("c:{cut}")]);
    }

    #[test]
    fn reasoning_is_not_read_for_tool_calls() {
        let (mut out, rx) = output(
            ToolChoice::Auto,
            Some(TagReasoningContext::new_in_think_block()),
        );
        let call = "<tool_call>\n{\"name\": \"get_weather\", \"arguments\": {\"city\": \"Rome\"}}\n</tool_call>";
        assert_eq!(out.route(&format!("I could call {call}</think>")), None);
        assert_eq!(out.route("The answer."), None);
        let outcome = out
            .finish(FinishReason::Stop, 3)
            .expect("the request succeeds");
        assert_eq!(outcome.finish, FinishReason::Stop);
        assert_eq!(
            sent(rx),
            [
                format!("r:I could call {call}"),
                "c:The answer.".to_string()
            ]
        );
    }

    #[test]
    fn tool_choice_none_sends_a_call_as_content() {
        let (mut out, rx) = output(ToolChoice::None, None);
        let call = "<tool_call>\n{\"name\": \"get_weather\", \"arguments\": {\"city\": \"Rome\"}}\n</tool_call>";
        assert_eq!(out.route(call), None);
        let outcome = out
            .finish(FinishReason::Stop, 3)
            .expect("the request succeeds");
        assert_eq!(outcome.finish, FinishReason::Stop);
        assert_eq!(sent(rx), [format!("c:{call}")]);
    }

    #[test]
    fn a_required_call_that_never_comes_fails_the_request() {
        let (mut out, rx) = output(ToolChoice::Required, None);
        assert_eq!(out.route("I won't call anything."), None);
        assert_eq!(
            out.finish(FinishReason::Length, 3).err().as_deref(),
            Some("Tool choice was required but no tools were called.")
        );
        drop(rx);
    }

    #[test]
    fn only_a_required_or_named_choice_obliges_a_call() {
        let mut other = weather_tool();
        other.function.name = "get_time".to_string();
        let tools = [weather_tool(), other.clone()];
        let names = |choice: Option<ToolChoice>| -> Vec<String> {
            obliged_tools(choice.as_ref(), &tools)
                .iter()
                .map(|t| t.function.name.clone())
                .collect()
        };
        assert_eq!(names(None), Vec::<String>::new());
        assert_eq!(names(Some(ToolChoice::Auto)), Vec::<String>::new());
        assert_eq!(names(Some(ToolChoice::None)), Vec::<String>::new());
        assert_eq!(
            names(Some(ToolChoice::Required)),
            ["get_weather", "get_time"]
        );
        assert_eq!(names(Some(ToolChoice::Tool(other))), ["get_time"]);
    }

    const TAGGED: &str = "{{ '<tool_call>' }}{{ '</tool_call>' }}";

    fn answer_schema() -> ResponseFormat {
        ResponseFormat::JsonSchema {
            json_schema: async_openai::types::chat::ResponseFormatJsonSchema {
                description: None,
                name: "answer".to_string(),
                schema: serde_json::json!({
                    "type": "object",
                    "properties": { "n": { "type": "integer" } },
                    "required": ["n"],
                    "additionalProperties": false,
                }),
                strict: Some(true),
            },
        }
    }

    #[test]
    fn the_load_notes_name_the_model_and_what_to_do() {
        assert_eq!(
            paged_attention_note("qwen"),
            "Model 'qwen' sets `paged_attention: disabled`, which has no effect: GGUF models are served by llama.cpp, which manages its own KV cache. Remove the parameter to silence this message."
        );
        let cause = llama_cpp::Error::LoadModel {
            path: "/models/glm.gguf".to_string(),
            reason: "out of memory".to_string(),
        };
        assert_eq!(
            without_mtp_note("glm", &cause),
            "Model 'glm' loaded without its MTP layer, so it decodes without drafting: the model did not load with it. Cause: llama.cpp could not load the model '/models/glm.gguf': out of memory"
        );
        assert_eq!(
            not_a_head(Path::new("/models/mtp-shard.gguf")),
            "'mtp-shard.gguf' is named as an MTP draft head (it starts with `mtp-`), but it carries no MTP layer. Remove it from `files`, or rename it if it is a shard of the model."
        );
    }

    #[test]
    fn rules_are_prefixed_in_definitions_and_references_but_not_in_literals() {
        let grammar = "root ::= \"{\" space ab-kv \"}\" space\nspace ::= | \" \" | \"\\n\"{1,2} [ \\t]{0,20}\nab-kv ::= \"\\\"a-b\\\"\" space \":\" space [0-9]{1,3}\n";
        assert_eq!(
            prefix_rules(grammar, "p-"),
            "p-root ::= \"{\" p-space p-ab-kv \"}\" p-space\np-space ::= | \" \" | \"\\n\"{1,2} [ \\t]{0,20}\np-ab-kv ::= \"\\\"a-b\\\"\" p-space \":\" p-space [0-9]{1,3}\n"
        );
    }

    #[test]
    fn a_required_call_is_forced_in_a_template_that_tags_calls() {
        let tool = weather_tool();
        let grammar = content_grammar("m", TAGGED, None, &[&tool], &[&tool])
            .expect("the tool's schema converts")
            .expect("a required call in a tagged template has a grammar");
        assert!(
            grammar.ends_with("root ::= \"<tool_call>\\n\" call-root \"\\n</tool_call>\"\n"),
            "{grammar}"
        );
        assert_eq!(
            grammar.matches("\nroot ::=").count() + usize::from(grammar.starts_with("root ::=")),
            1,
            "{grammar}"
        );
        assert!(grammar.contains("call-root ::="), "{grammar}");
        assert!(
            grammar.contains("get_weather"),
            "the name is held to the tool's: {grammar}"
        );
        assert_eq!(
            content_grammar("m", "{{ '[TOOL_CALLS]' }}", None, &[&tool], &[&tool])
                .expect("no conversion"),
            None,
            "another template format has no grammar; end-of-generation is held off instead"
        );
    }

    #[test]
    fn a_structured_reply_beside_tools_is_either_a_call_or_the_reply() {
        let tool = weather_tool();
        let grammar = content_grammar("m", TAGGED, Some(&answer_schema()), &[&tool], &[])
            .expect("both convert")
            .expect("a grammar");
        assert!(
            grammar.ends_with(
                "root ::= \"<tool_call>\\n\" call-root \"\\n</tool_call>\" | reply-root\n"
            ),
            "{grammar}"
        );
        assert!(grammar.contains("reply-root ::="), "{grammar}");
        assert!(
            !grammar.lines().any(|l| l.starts_with("space ::=")),
            "no rule is left unprefixed to collide: {grammar}"
        );
        let refused = content_grammar(
            "qwen",
            "{{ '[TOOL_CALLS]' }}",
            Some(&answer_schema()),
            &[&tool],
            &[],
        )
        .expect_err("another template format can't combine the two");
        assert_eq!(
            refused.to_string(),
            "invalid args: Model 'qwen' can't combine a `json_schema` or `json_object` `response_format` with `tools`: its chat template's tool-call format can't be enforced alongside a schema. Send one or the other."
        );
    }

    #[test]
    fn a_json_schema_becomes_a_grammar_and_json_object_any_object() {
        let grammar = content_grammar("m", TAGGED, Some(&answer_schema()), &[], &[])
            .expect("the schema converts")
            .expect("a json_schema format has a grammar");
        assert!(grammar.contains("root ::="), "{grammar}");
        assert!(
            grammar.contains("\\\"n\\\""),
            "the grammar names the property: {grammar}"
        );
        assert!(
            content_grammar("m", TAGGED, Some(&ResponseFormat::JsonObject), &[], &[])
                .expect("json_object converts")
                .is_some_and(|g| g.contains("root ::="))
        );
        assert_eq!(
            content_grammar("m", TAGGED, Some(&ResponseFormat::Text), &[], &[]).expect("text"),
            None
        );
        assert_eq!(
            content_grammar("m", TAGGED, None, &[], &[]).expect("no format"),
            None
        );
    }

    /// What a reply of `chunks` sends, read with the parser chosen for `template` and
    /// `prompt`.
    fn split(template: &str, prompt: &str, chunks: &[&str]) -> Vec<String> {
        let (events, rx) = mpsc::channel(STREAM_BUFFER);
        let job = Job {
            prompt: String::new(),
            namespace: CacheNamespace::Public,
            limit: None,
            sampling: Sampling::default(),
            stop: Vec::new(),
            reasoning: reasoning_parser(template, prompt),
            tools: None,
            harmony: None,
            require_tool_call: false,
            stopping: Arc::default(),
            events,
        };
        let (_, _, mut out) = Output::new(job);
        for chunk in chunks {
            assert_eq!(out.route(chunk), None);
        }
        out.finish(FinishReason::Stop, 1)
            .expect("the request succeeds");
        sent(rx)
    }

    const THINKS: &str = "{{ '<think>' }}{{ '</think>' }}";
    /// The reasoning markers of a Gemma 4 chat template.
    const GEMMA4: &str = "{{ '<|think|>' }}{{ '<|channel>thought' }}{{ '<channel|>' }}";

    #[test]
    fn a_split_tag_is_held_until_it_completes() {
        assert_eq!(
            split(THINKS, "<think>\n", &["plan</th", "ink>\n\nanswer"]),
            ["r:plan", "c:\n\nanswer"]
        );
    }

    #[test]
    fn a_reply_that_opens_its_own_block_is_split_too() {
        assert_eq!(
            split(THINKS, "", &["<think>why</think>42"]),
            ["r:why", "c:42"]
        );
    }

    #[test]
    fn gemma_4_reasoning_closes_at_its_channel_tag() {
        // regression: a Gemma 4 reply's answer was read as reasoning to the end
        let prompt = "<|think|>user\nHi";
        assert_eq!(
            split(GEMMA4, prompt, &["<|channel>thought\nplan<chan", "nel|>42"]),
            ["r:plan", "c:42"]
        );
        assert_eq!(
            split(GEMMA4, prompt, &["thought\nplan<channel|>42"]),
            ["r:plan", "c:42"]
        );
        assert_eq!(split(GEMMA4, "user\nHi", &["42"]), ["c:42"]);
    }

    #[test]
    fn a_template_that_marks_no_reasoning_keeps_the_reply_as_content() {
        assert_eq!(
            split("{{ messages }}", "<think>", &["<think>why</think>42"]),
            ["c:<think>why</think>42"]
        );
    }

    const HARMONY: &str = "{{ '<|start|>assistant<|channel|>final<|message|>' }}{{ '<|end|>' }}";
    const HARMONY_MARKERS: harmony::Markers = harmony::TEST_MARKERS;

    /// An `Output` reading a Harmony reply that may call `tools`, with `stop` sequences and a
    /// token `limit`, and what it sends.
    fn harmony_output(
        tools: &[&str],
        stop: &[&str],
        limit: Option<usize>,
    ) -> (Output, mpsc::Receiver<Event>) {
        let (events, rx) = mpsc::channel(STREAM_BUFFER);
        let tools = tools.iter().map(ToString::to_string).collect();
        let job = Job {
            prompt: String::new(),
            limit,
            sampling: Sampling::default(),
            stop: stop.iter().map(ToString::to_string).collect(),
            reasoning: None,
            tools: None,
            harmony: Some(harmony::Reader::new(
                HARMONY_MARKERS,
                tools,
                harmony::Held::Nothing,
            )),
            require_tool_call: false,
            stopping: Arc::default(),
            events,
        };
        let (_, _, out) = Output::new(job);
        (out, rx)
    }

    /// Generates `reply`: each marker as its token, which ends generation if it is
    /// `<|return|>` or `<|call|>`, and other text a character per token. Returns why generation
    /// ended, and after how many tokens.
    fn generate_harmony(out: &mut Output, reply: &str) -> (Option<FinishReason>, usize) {
        let ending = [Token(5), Token(6)];
        let mut tokens = Vec::new();
        for (marker, text) in harmony::spell(reply) {
            match marker {
                Some(token) => tokens.push((token, text)),
                None => tokens.extend(text.chars().map(|c| (Token(100), c.to_string()))),
            }
        }
        for (n, (token, text)) in tokens.into_iter().enumerate() {
            if let Some(finish) = out.read(token, ending.contains(&token), || text.into_bytes()) {
                return (Some(finish), n + 1);
            }
        }
        (None, 0)
    }

    /// `sent`, with each run of one kind of event as one.
    fn joined(sent: Vec<String>) -> Vec<String> {
        let mut out: Vec<String> = Vec::new();
        for event in sent {
            match out.last_mut() {
                Some(last)
                    if last.starts_with("r:") && event.starts_with("r:")
                        || last.starts_with("c:") && event.starts_with("c:") =>
                {
                    last.push_str(&event[2..]);
                }
                _ => out.push(event),
            }
        }
        out
    }

    #[test]
    fn a_harmony_reply_sends_its_reasoning_then_its_answer() {
        let (mut out, rx) = harmony_output(&[], &[], None);
        let reply = "<|channel|>analysis<|message|>Add them.<|end|><|start|>assistant<|channel|>final<|message|>2 + 2 = 4<|return|>";
        let (finish, n) = generate_harmony(&mut out, reply);
        assert_eq!(finish, Some(FinishReason::Stop));
        let outcome = out
            .finish(FinishReason::Stop, 9)
            .expect("the request succeeds");
        // every token but the `<|return|>` that ends the reply
        assert_eq!(outcome.completion_tokens as usize, n - 1);
        assert_eq!(
            joined(sent(rx)),
            ["r:Add them.".to_string(), "c:2 + 2 = 4".to_string()]
        );
    }

    #[test]
    fn a_harmony_call_ends_the_reply_with_its_arguments() {
        let (mut out, rx) = harmony_output(&["get_weather"], &[], None);
        let reply = "<|channel|>analysis<|message|>Look it up.<|end|><|start|>assistant<|channel|>commentary to=functions.get_weather <|constrain|>json<|message|>{\"city\":\"Paris\"}<|call|>";
        let (finish, _) = generate_harmony(&mut out, reply);
        assert_eq!(finish, Some(FinishReason::ToolCalls));
        let outcome = out
            .finish(FinishReason::ToolCalls, 9)
            .expect("the request succeeds");
        assert_eq!(outcome.finish, FinishReason::ToolCalls);
        assert_eq!(
            joined(sent(rx)),
            [
                "r:Look it up.".to_string(),
                "t:get_weather({\"city\":\"Paris\"})".to_string()
            ]
        );
    }

    #[test]
    fn a_stop_sequence_applies_to_a_harmony_body_but_not_its_header() {
        let (mut out, rx) = harmony_output(&[], &["final"], None);
        let reply = "<|channel|>analysis<|message|>r<|end|><|start|>assistant<|channel|>final<|message|>the final word<|return|>";
        let (finish, _) = generate_harmony(&mut out, reply);
        assert_eq!(finish, Some(FinishReason::Stop));
        out.finish(FinishReason::Stop, 1)
            .expect("the request succeeds");
        assert_eq!(joined(sent(rx)), ["r:r".to_string(), "c:the ".to_string()]);
    }

    #[test]
    fn text_a_stop_sequence_holds_back_goes_out_before_the_next_harmony_message() {
        // `x` may start the stop sequence, so it is held until the marker closes its message
        let (mut out, rx) = harmony_output(&[], &["xyz"], None);
        let reply = "<|channel|>analysis<|message|>abx<|end|><|start|>assistant<|channel|>final<|message|>c<|return|>";
        generate_harmony(&mut out, reply);
        out.finish(FinishReason::Stop, 1)
            .expect("the request succeeds");
        assert_eq!(joined(sent(rx)), ["r:abx".to_string(), "c:c".to_string()]);
    }

    #[test]
    fn a_harmony_reply_counts_its_markers_against_the_budget() {
        // `<|channel|>`, the five tokens of `final` and `<|message|>` leave two of nine
        let (mut out, rx) = harmony_output(&[], &[], Some(9));
        let reply = "<|channel|>final<|message|>abcdef<|return|>";
        assert_eq!(
            generate_harmony(&mut out, reply),
            (Some(FinishReason::Length), 9)
        );
        out.finish(FinishReason::Length, 1)
            .expect("the request succeeds");
        assert_eq!(joined(sent(rx)), ["c:ab".to_string()]);
    }

    #[test]
    fn a_harmony_call_cut_off_after_complete_arguments_is_still_made() {
        // `<|channel|>`, `commentary to=functions.f` (25 characters), `<|message|>` and `{}`
        let (mut out, rx) = harmony_output(&["f"], &[], Some(29));
        let reply = "<|channel|>commentary to=functions.f<|message|>{}<|call|>";
        assert_eq!(
            generate_harmony(&mut out, reply),
            (Some(FinishReason::Length), 29)
        );
        let outcome = out
            .finish(FinishReason::Length, 1)
            .expect("the request succeeds");
        assert_eq!(outcome.finish, FinishReason::ToolCalls);
        assert_eq!(joined(sent(rx)), ["t:f({})".to_string()]);
    }

    #[test]
    fn a_named_tool_choice_is_the_only_name_a_harmony_call_may_take() {
        let tool = |name: &str| {
            let mut tool = weather_tool();
            tool.function.name = name.to_string();
            tool
        };
        let (f, g) = (tool("f"), tool("g"));
        assert_eq!(nameable_tools(&[&f, &g], &[&f]), ["f".to_string()]);
        assert_eq!(
            nameable_tools(&[&f, &g], &[]),
            ["f".to_string(), "g".to_string()]
        );
        // a call to the other tool is dropped, so a required call is not made
        let (events, _rx) = mpsc::channel(STREAM_BUFFER);
        let (_, _, mut output) = Output::new(Job {
            prompt: String::new(),
            limit: None,
            sampling: Sampling::default(),
            stop: Vec::new(),
            reasoning: None,
            tools: None,
            harmony: Some(harmony::Reader::new(
                HARMONY_MARKERS,
                nameable_tools(&[&f, &g], &[&f]),
                harmony::Held::Nothing,
            )),
            require_tool_call: true,
            stopping: Arc::default(),
            events,
        });
        let reply = "<|channel|>commentary to=functions.g<|message|>{}<|call|>";
        assert_eq!(
            generate_harmony(&mut output, reply).0,
            Some(FinishReason::Stop)
        );
        assert_eq!(
            output.finish(FinishReason::Stop, 1).map(|o| o.finish),
            Err("Tool choice was required but no tools were called.".to_string())
        );
    }

    #[test]
    fn a_required_harmony_call_is_forced_by_a_grammar_over_the_whole_reply() {
        let tool = weather_tool();
        let grammar = harmony_call(&HARMONY_MARKERS, &[&tool]).expect("the tool's schema converts");
        // the test vocabulary's markers are tokens 1 to 7, matched as tokens
        assert!(
            grammar.ends_with("root ::= analysis? <[2]> \"commentary to=functions.\" call\n"),
            "{grammar}"
        );
        assert!(
            grammar.starts_with(
                "analysis ::= <[2]> \"analysis\" <[3]> reasoning <[4]> <[1]> \"assistant\"\n"
            ),
            "{grammar}"
        );
        assert!(
            grammar.contains("call ::= \"get_weather\" constrain? <[3]> tool0-root\n"),
            "{grammar}"
        );
        assert!(
            grammar.contains("constrain ::= \" \" <[7]>? [A-Za-z0-9_-]+\n"),
            "{grammar}"
        );
        assert!(
            !grammar.contains("<|"),
            "no marker is written as text: {grammar}"
        );
        assert_eq!(
            grammar.matches("\nroot ::=").count() + usize::from(grammar.starts_with("root ::=")),
            1,
            "one start rule: {grammar}"
        );
        // nor does a Harmony template make one without its markers
        assert_eq!(
            content_grammar("m", HARMONY, None, &[&tool], &[&tool]).expect("no grammar"),
            None
        );
    }

    #[test]
    fn a_structured_harmony_reply_is_held_from_its_first_header() {
        let reply = content_grammar("m", HARMONY, Some(&answer_schema()), &[], &[])
            .expect("the schema converts")
            .expect("a json_schema format has a grammar");
        let grammar = harmony_reply(&HARMONY_MARKERS, &reply);
        // analysis messages, then the final one: no preamble, whose text would be content
        assert!(
            grammar.ends_with("root ::= analysis* <[2]> \"final\" constrain? <[3]> reply-root\n"),
            "{grammar}"
        );
        assert!(!grammar.contains("commentary"), "{grammar}");
        assert_eq!(
            grammar.matches("\nroot ::=").count() + usize::from(grammar.starts_with("root ::=")),
            1,
            "one start rule: {grammar}"
        );
    }

    #[test]
    fn a_harmony_call_ends_where_its_arguments_do_though_a_stop_sequence_holds_their_end() {
        // `}x` may begin at the closing `}`, which is held back when `<|end|>` comes
        let (mut out, rx) = harmony_output(&["f"], &["}x"], None);
        let reply = "<|channel|>commentary to=functions.f<|message|>{}<|end|><|start|>assistant<|channel|>final<|message|>no<|return|>";
        let (finish, _) = generate_harmony(&mut out, reply);
        assert_eq!(finish, Some(FinishReason::ToolCalls));
        out.finish(FinishReason::ToolCalls, 1)
            .expect("the request succeeds");
        assert_eq!(joined(sent(rx)), ["t:f({})".to_string()]);
    }

    #[test]
    fn a_required_harmony_call_leaves_only_its_call_marker_possible() {
        let (out, _rx) = harmony_output(&["f"], &[], None);
        assert_eq!(
            out.ruled_out(&[Token(5), Token(6), Token(99)]),
            [Token(5), Token(99)]
        );
        let (out, _rx) = output(ToolChoice::Auto, None);
        assert_eq!(out.ruled_out(&[Token(6)]), [Token(6)]);
    }

    #[test]
    fn a_harmony_model_is_refused_only_when_its_vocabulary_lacks_the_markers() {
        let refused = check_reply_format(
            &Either::Left(HARMONY.to_string()),
            Some("gpt-oss".to_string()),
            None,
            "gpt-oss-20b-mxfp4.gguf".to_string(),
        )
        .expect_err("a Harmony template without its markers is refused");
        assert_eq!(
            refused.to_string(),
            "Architecture 'gpt-oss' in gpt-oss-20b-mxfp4.gguf is not supported by this Spice release. Upgrade Spice, or use a GGUF of a supported architecture: https://spiceai.org/docs/components/models#gguf-architectures"
        );
        assert!(reasoning_parser(HARMONY, "").is_none());
        assert_eq!(
            check_reply_format(
                &Either::Left(HARMONY.to_string()),
                Some("gpt-oss".to_string()),
                Some(HARMONY_MARKERS),
                "gpt-oss-20b-mxfp4.gguf".to_string(),
            )
            .expect("a Harmony template with its markers"),
            Some(HARMONY_MARKERS)
        );
        assert_eq!(
            check_reply_format(
                &Either::Left(THINKS.to_string()),
                None,
                Some(HARMONY_MARKERS),
                "qwen.gguf".to_string(),
            )
            .expect("a think-tag template"),
            None,
            "only a Harmony template's replies are read by the markers"
        );
        check_reply_format(
            &Either::Left(GEMMA4.to_string()),
            None,
            None,
            "gemma.gguf".to_string(),
        )
        .expect("a Gemma 4 template");
    }

    /// Named templates are chosen as the mistral.rs engine chooses them, and a Harmony template
    /// among them is read as a lone one is.
    #[test]
    fn named_templates_are_chosen_as_mistral_rs_chooses() {
        let entry = |pairs: &[(&str, &str)]| -> HashMap<String, String> {
            pairs
                .iter()
                .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
                .collect()
        };
        let named = Either::Right(vec![
            entry(&[("name", "default"), ("template", "D")]),
            entry(&[("name", "tool_use"), ("template", "T")]),
        ]);
        assert_eq!(chosen_template(&named, false), Ok("D"));
        assert_eq!(chosen_template(&named, true), Ok("T"));
        let keyed = Either::Right(vec![
            entry(&[("default", "D")]),
            entry(&[("tool_use", "T")]),
        ]);
        assert_eq!(chosen_template(&keyed, false), Ok("D"));
        assert_eq!(chosen_template(&keyed, true), Ok("T"));
        let default_only = Either::Right(vec![entry(&[("name", "default"), ("template", "D")])]);
        assert_eq!(
            chosen_template(&default_only, true),
            Err(
                "Tools were provided but this chat template does not handle tool usage".to_string()
            )
        );
        let harmony = Either::Right(vec![
            entry(&[("name", "default"), ("template", THINKS)]),
            entry(&[("name", "tool_use"), ("template", HARMONY)]),
        ]);
        assert!(matches!(
            check_reply_format(&harmony, None, None, "gpt-oss.gguf".to_string()),
            Err(ChatError::UnsupportedArchitecture { .. })
        ));
    }

    #[test]
    fn a_stop_sequence_ends_the_text_before_it() {
        let mut stops = StopMatcher::new(vec!["\n\nUser:".to_string()]);
        assert_eq!(stops.push("1, 2, 3\n"), ("1, 2, 3".to_string(), false));
        assert_eq!(stops.push("\nUser: hi"), (String::new(), true));
    }

    #[test]
    fn a_multibyte_stop_sequence_is_cut_on_character_boundaries() {
        let mut stops = StopMatcher::new(vec!["日本".to_string(), "é!".to_string()]);
        assert_eq!(
            stops.push("こんにちは日"),
            ("こんにちは".to_string(), false)
        );
        assert_eq!(stops.push("本語"), (String::new(), true));
        let mut stops = StopMatcher::new(vec!["é!".to_string()]);
        // No prefix of `é!` ends inside `é`, so none is tried against the text.
        assert_eq!(stops.push("ab"), ("ab".to_string(), false));
        assert_eq!(stops.push("café"), ("caf".to_string(), false));
        assert_eq!(stops.push("é"), ("é".to_string(), false));
        assert_eq!(stops.finish(), "é");
    }

    #[test]
    fn an_incomplete_character_waits_for_the_next_token() {
        let mut bytes = "é".as_bytes()[..1].to_vec();
        assert_eq!(take_utf8(&mut bytes), "");
        bytes.extend(&"é".as_bytes()[1..]);
        assert_eq!(take_utf8(&mut bytes), "é");
    }

    /// The text `tokens` decode to, token by token.
    fn decoded(tokens: &[&[u8]]) -> String {
        let mut bytes = Vec::new();
        let mut text = String::new();
        for token in tokens {
            bytes.extend_from_slice(token);
            text.push_str(&take_utf8(&mut bytes));
        }
        assert!(bytes.is_empty(), "left {bytes:x?}");
        text
    }

    #[test]
    fn an_invalid_byte_does_not_take_the_character_after_it() {
        // regression: replacing the invalid `ff` also replaced the `c3` that `a9` completes
        assert_eq!(decoded(&[&[0xff, 0xc3], &[0xa9]]), "\u{fffd}é");
        assert_eq!(decoded(&[&[0xc3], &[0xc3], &[0xa9]]), "\u{fffd}é");
        // Every split of the same bytes decodes as a lossy decode of all of them does.
        let bytes = [
            0x61, 0xff, 0xe2, 0x82, 0xac, 0xc3, 0xc3, 0xa9, 0xf0, 0x9f, 0x98, 0x80,
        ];
        let whole = String::from_utf8_lossy(&bytes);
        for cut in 0..=bytes.len() {
            for second in cut..=bytes.len() {
                let parts = [&bytes[..cut], &bytes[cut..second], &bytes[second..]];
                assert_eq!(decoded(&parts), whole, "split at {cut} and {second}");
            }
        }
    }

    #[test]
    fn a_character_cut_off_by_the_end_of_generation_is_replaced() {
        let (mut out, rx) = output(ToolChoice::Auto, None);
        out.pending = vec![0xe2, 0x82];
        out.finish(FinishReason::Length, 1)
            .expect("the request succeeds");
        assert_eq!(sent(rx), ["c:\u{fffd}"]);
    }

    /// The content a reply of `pieces` sends when the request stops at `STOP`.
    fn stopped_at(pieces: &[&[u8]]) -> String {
        let (events, rx) = mpsc::channel(STREAM_BUFFER);
        let job = Job {
            prompt: String::new(),
            namespace: CacheNamespace::Public,
            limit: None,
            sampling: Sampling::default(),
            stop: vec!["STOP".to_string()],
            reasoning: None,
            tools: None,
            harmony: None,
            require_tool_call: false,
            stopping: Arc::default(),
            events,
        };
        let (_, _, mut out) = Output::new(job);
        let finish = pieces
            .iter()
            .find_map(|piece| out.bytes(piece))
            .unwrap_or(FinishReason::Length);
        out.finish(finish, 1).expect("the request succeeds");
        sent(rx)
            .into_iter()
            .map(|event| {
                event
                    .strip_prefix("c:")
                    .map(ToString::to_string)
                    .expect("only content is sent")
            })
            .collect()
    }

    #[test]
    fn nothing_after_a_stop_sequence_is_sent() {
        // regression: an incomplete character after the stop sequence was sent as U+FFFD
        let reply = "beforeSTOPéafter".as_bytes();
        assert_eq!(stopped_at(&[reply]), "before");
        for cut in 0..=reply.len() {
            assert_eq!(
                stopped_at(&[&reply[..cut], &reply[cut..]]),
                "before",
                "split at byte {cut}"
            );
        }
    }

    /// The tokens nucleus sampling leaves from `logits` at `temperature` and `top_p`: the
    /// smallest most-likely set whose probability reaches `top_p`.
    fn nucleus(logits: &[f32], temperature: f32, top_p: f32) -> Vec<Token> {
        let scaled: Vec<f64> = logits
            .iter()
            .map(|&l| f64::from(l) / f64::from(temperature))
            .collect();
        let max = scaled.iter().copied().fold(f64::NEG_INFINITY, f64::max);
        let total: f64 = scaled.iter().map(|l| (l - max).exp()).sum();
        let mut ranked: Vec<(usize, f64)> = scaled
            .iter()
            .enumerate()
            .map(|(id, l)| (id, (l - max).exp() / total))
            .collect();
        ranked.sort_by(|a, b| b.1.total_cmp(&a.1));
        let mut kept = Vec::new();
        let mut mass = 0.0;
        for (id, p) in ranked {
            kept.push(Token(i32::try_from(id).expect("a small vocabulary")));
            mass += p;
            if mass >= f64::from(top_p) {
                break;
            }
        }
        kept
    }

    #[test]
    fn top_p_keeps_the_nucleus_of_the_tempered_distribution() {
        // regression: top-p ran before temperature, on the untempered distribution
        let logits = [0.0, -1.0, -2.0, -3.0];
        for (temperature, top_p) in [(2.0, 0.6), (0.5, 0.9), (1.5, 0.8), (3.0, 0.95)] {
            let sampling = Sampling {
                temperature: Some(temperature),
                top_p: Some(top_p),
                seed: 7,
                ..Sampling::default()
            };
            let mut chain = Sampler::chain(draw(&sampling));
            assert_eq!(
                chain.apply(&logits),
                nucleus(&logits, temperature, top_p),
                "temperature {temperature}, top_p {top_p}"
            );
        }
        // The first case tells the orders apart: untempered, top-p keeps only the first token.
        assert_eq!(nucleus(&logits, 1.0, 0.6), [Token(0)]);
        assert_eq!(nucleus(&logits, 2.0, 0.6), [Token(0), Token(1)]);
    }

    #[test]
    fn n_asks_for_one_to_128_choices() {
        let count = |n: Option<u8>| {
            let mut req: CreateChatCompletionRequest = serde_json::from_value(serde_json::json!({
                "model": "m",
                "messages": [{ "role": "user", "content": "Hi" }],
            }))
            .expect("a request");
            req.n = n;
            choice_count(&req).map_err(|e| e.to_string())
        };
        let refused = Err(
            "invalid args: `n` must be between 1 and 128, the number of choices to generate."
                .to_string(),
        );
        assert_eq!(count(None), Ok(1));
        assert_eq!(count(Some(1)), Ok(1));
        assert_eq!(count(Some(128)), Ok(128));
        assert_eq!(count(Some(0)), refused);
        assert_eq!(count(Some(129)), refused);
    }
}
