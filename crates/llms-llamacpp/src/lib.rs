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
        atomic::{AtomicUsize, Ordering},
        mpsc as std_mpsc,
    },
    thread,
    time::{SystemTime, UNIX_EPOCH},
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
    Backend, Batch, Context, ContextParams, Model, ModelParams, Mtp, MtpParams, Sampler, Token,
    Vocab,
};
use mistralrs_core::{
    AllowedToolChoice, AllowedToolsMode, ChatTemplateValue, MessageContent,
    ReasoningEffort as TemplateReasoningEffort, Tool, ToolCallResponse, ToolCallingMatcher,
    ToolChoice, apply_chat_template_to,
};
use tokio::sync::{mpsc, oneshot};

use llms::chat::engine::LocalChatEngine;
use llms::chat::mistral::{
    ReasoningControls, ReasoningSupport, convert_tool, opens_reasoning, reasoning_controls,
    request_tool_choice,
};
use llms::chat::{
    Chat, Error as ChatError, LocalModelOptions, PagedAttentionMode, Result, message_to_mistral,
    nsql::SqlGeneration,
};
use llms::streaming_utils::generate_stream_id;

llms::register_local_chat_engine!(
    LLAMA_CPP,
    LocalChatEngine {
        name: "llama.cpp",
        serves: serves_weights,
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
        LlamaCpp::from(weights, options)
            .await
            .map(|model| std::sync::Arc::new(model) as std::sync::Arc<dyn Chat>)
    })
}

/// Tokens decoded per forward pass while reading the prompt.
const PROMPT_BATCH: usize = 512;

/// The context size when the model sets no `context_length`.
const DEFAULT_CONTEXT: u32 = 8192;

/// Draft tokens an MTP head proposes per step. Two measured fastest for Qwen3.8-Flash-Next
/// on a DGX Spark: over eight prompts, 60.7 tok/s against 53.8 with one and 47.5 without a
/// draft, and three was slower than none on a story prompt.
const MTP_DRAFT: i32 = 2;

/// How many streamed tokens may wait for a slow client before decoding pauses.
const STREAM_BUFFER: usize = 64;

/// The opening and closing tags of a reasoning block.
const THINK_OPEN: &str = "<think>";
const THINK_CLOSE: &str = "</think>";

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

#[derive(Debug, Clone, Default)]
struct Sampling {
    temperature: Option<f32>,
    top_p: Option<f32>,
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
    limit: Option<usize>,
    sampling: Sampling,
    stop: Vec<String>,
    /// Whether the prompt leaves the model inside a reasoning block.
    reasons: bool,
    /// Reads tool calls out of the content, when the request offers tools.
    tools: Option<ToolCallingMatcher>,
    /// Whether `tool_choice` requires a tool call, so a reply without one fails.
    require_tool_call: bool,
    events: mpsc::Sender<Event>,
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

        if options.paged_attention == PagedAttentionMode::Disabled {
            tracing::info!("{}", paged_attention_note(options.reasoning.model_name));
        }
        let (ready_tx, ready_rx) = oneshot::channel();
        let (jobs_tx, jobs_rx) = std_mpsc::channel();
        let name = options.reasoning.model_name.to_string();
        thread::Builder::new()
            .name("llama-cpp".to_string())
            .spawn(move || serve(&name, &first, draft.as_deref(), n_ctx, ready_tx, &jobs_rx))
            .map_err(|e| ChatError::FailedToLoadModel {
                source: Box::new(e),
            })?;
        let loaded = ready_rx.await.map_err(|e| ChatError::FailedToLoadModel {
            source: Box::new(e),
        })??;

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

    /// Validates `req`, renders its prompt and queues it on the decode thread.
    fn submit(
        &self,
        req: &CreateChatCompletionRequest,
    ) -> std::result::Result<mpsc::Receiver<Event>, OpenAIError> {
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
        let grammar = content_grammar(
            &self.model_name,
            &self.template,
            req.response_format.as_ref(),
            &tools,
            &obliged,
        )?;
        let matcher = if tools.is_empty() {
            None
        } else {
            Some(
                ToolCallingMatcher::new_with_format(
                    tool_choice.unwrap_or(ToolChoice::Auto),
                    Some(&tools),
                    None,
                )
                .map_err(|e| OpenAIError::InvalidArgument(format!("Invalid `tools`: {e}")))?,
            )
        };
        let require_tool_call = matcher
            .as_ref()
            .is_some_and(ToolCallingMatcher::requires_tool_call);
        let ban_eog = require_tool_call && grammar.is_none();
        let prompt = self.prompt(&req.messages, reasoning, tools)?;
        let (events, rx) = mpsc::channel(STREAM_BUFFER);
        let job = Job {
            reasons: opens_reasoning(&prompt),
            prompt,
            limit: completion_limit(req),
            sampling: Sampling {
                temperature: req.temperature,
                top_p: req.top_p,
                seed: rand::random(),
                grammar,
                ban_eog,
            },
            stop: match &req.stop {
                Some(StopConfiguration::String(s)) => vec![s.clone()],
                Some(StopConfiguration::StringArray(s)) => s.clone(),
                None => Vec::new(),
            },
            tools: matcher,
            require_tool_call,
            events,
        };
        self.jobs
            .send(job)
            .map_err(|_| api_error("the llama.cpp decode thread has stopped"))?;
        Ok(rx)
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
        let rx = self.submit(&req)?;
        let id = self.id();
        let model = req.model;
        let created = now();
        let state = StreamState {
            rx,
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
                // A streamed delta has no reasoning field, so reasoning is not streamed, as on
                // the mistral.rs engine; it is in a non-streamed response's `reasoning_content`.
                let item = loop {
                    let first = state.first;
                    match state.rx.recv().await {
                        Some(Event::Reasoning(_)) => {}
                        Some(Event::Content(content)) => {
                            state.first = false;
                            break Ok(chunk(
                                &id,
                                &model,
                                created,
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
                                first,
                                None,
                                Some(calls),
                                None,
                                None,
                            ));
                        }
                        Some(Event::Done(outcome)) => {
                            state.done = true;
                            break Ok(chunk(
                                &id,
                                &model,
                                created,
                                first,
                                None,
                                None,
                                Some(outcome.finish),
                                Some(usage(outcome)),
                            ));
                        }
                        Some(Event::Failed(message)) => {
                            state.done = true;
                            break Err(api_error(&message));
                        }
                        None => {
                            state.done = true;
                            break Err(api_error(
                                "the llama.cpp decode thread stopped mid-response",
                            ));
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
        let mut rx = self.submit(&req)?;
        let (mut reasoning, mut content, mut tool_calls) =
            (String::new(), String::new(), Vec::new());
        let outcome = loop {
            match rx.recv().await {
                Some(Event::Reasoning(text)) => reasoning.push_str(&text),
                Some(Event::Content(text)) => content.push_str(&text),
                Some(Event::ToolCalls(calls)) => tool_calls.extend(calls.iter().map(tool_call)),
                Some(Event::Done(outcome)) => break outcome,
                Some(Event::Failed(message)) => return Err(api_error(&message)),
                None => {
                    return Err(api_error(
                        "the llama.cpp decode thread stopped mid-response",
                    ));
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
    first: bool,
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
    template: &str,
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
        &ChatTemplateValue(Either::Left(template.to_string())),
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
    n_ctx: Option<NonZeroU32>,
    ready: oneshot::Sender<std::result::Result<Loaded, ChatError>>,
    jobs: &std_mpsc::Receiver<Job>,
) {
    let backend = Backend::get();
    // A GGUF may carry its own MTP (NextN) layers, which only load when asked for; a file
    // without them loads as it would otherwise. A model that no longer fits with its MTP
    // layer still loads without it, and decodes without drafting.
    let model = match Model::load(backend, path, &ModelParams::default().with_load_mtp(true)) {
        Ok(model) => model,
        Err(with_mtp) => match Model::load(backend, path, &ModelParams::default()) {
            Ok(model) => {
                tracing::info!("{}", without_mtp_note(name, &with_mtp));
                model
            }
            Err(_) => {
                let _ = ready.send(Err(load_error(path, with_mtp)));
                return;
            }
        },
    };
    let draft_model = match draft.map(|d| {
        let params = ModelParams::default().with_load_mtp(true);
        let head = Model::load(backend, d, &params).map_err(|e| {
            format!(
                "llama.cpp could not load the MTP head '{}': {e}",
                d.display()
            )
        })?;
        if head.n_layer_nextn() > 0 {
            Ok(head)
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
    // llama.cpp's server does.
    let drafts_itself = draft_model.is_none() && model.n_layer_nextn() > 0;
    // The contexts borrow the models, so all of them live in this frame for the thread's
    // lifetime. With a draft head, verification rolls back up to `MTP_DRAFT` rejected tokens,
    // and a hybrid model's recurrent layers can roll back only as many tokens as the context
    // keeps state snapshots for, so it keeps that many, as llama.cpp's server sizes it.
    let snapshots = if draft_model.is_some() || drafts_itself {
        u32::try_from(MTP_DRAFT).unwrap_or(0)
    } else {
        0
    };
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
    let head = draft_model.as_ref().or(drafts_itself.then_some(&model));
    let mut decoder = match head {
        None => Decoder::Plain(context),
        Some(head) => match mtp(head, context) {
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
    let template = model.chat_template();
    // Ruled out while a required tool call is outstanding.
    let eog = vocab.eog_tokens();
    if ready.send(Ok(Loaded { template, special })).is_err() {
        return;
    }
    while let Ok(job) = jobs.recv() {
        let events = job.events.clone();
        let result = match &mut decoder {
            Decoder::Plain(context) => generate(&model, context, job, &eog),
            Decoder::Mtp(spec) => generate_mtp(&model, spec, job, &eog),
        };
        let event = match result {
            Ok(outcome) => Event::Done(outcome),
            Err(message) => Event::Failed(message),
        };
        let _ = events.blocking_send(event);
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
                file: path.file_name().map_or_else(
                    || path.display().to_string(),
                    |n| n.to_string_lossy().to_string(),
                ),
            };
        }
    }
    ChatError::FailedToLoadModel {
        source: Box::new(error),
    }
}

/// The decode thread's engine: plain decoding, or decoding with an MTP draft head.
enum Decoder<'m> {
    Plain(Context<'m>),
    Mtp(Box<Mtp<'m>>),
}

/// Creates an MTP draft context linked to `context`, as llama.cpp's own server does: an
/// MTP-type context of the same size, reading the target's hidden states.
fn mtp<'m>(draft_model: &'m Model, context: Context<'m>) -> std::result::Result<Mtp<'m>, String> {
    let params = ContextParams::default()
        .with_n_ctx(NonZeroU32::new(context.n_ctx()))
        .with_n_batch(u32::try_from(PROMPT_BATCH).unwrap_or(u32::MAX));
    Mtp::new(
        context,
        draft_model,
        &params,
        MtpParams {
            n_max: MTP_DRAFT,
            n_min: 0,
            p_min: 0.0,
        },
    )
    .map_err(|e| format!("llama.cpp could not start MTP drafting: {e}"))
}

/// What prompt reading needs from either engine.
trait Target<'m> {
    fn context(&self) -> &Context<'m>;
    fn context_mut(&mut self) -> &mut Context<'m>;
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
    fn decoded(&mut self, batch: &Batch) -> std::result::Result<(), String> {
        self.process(batch).map_err(|e| e.to_string())
    }
}

/// Tokenizes the prompt and decodes it from an empty KV cache, returning the tokens and the
/// batch whose last logits predict the first generated token.
fn read_prompt<'m>(
    model: &Model,
    target: &mut impl Target<'m>,
    prompt: &str,
) -> std::result::Result<(Vec<Token>, Batch), String> {
    target.context_mut().clear();
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
    let mut batch = Batch::new(PROMPT_BATCH);
    let mut pos: i32 = 0;
    let chunks = tokens.chunks(PROMPT_BATCH).count();
    for (i, chunk) in tokens.chunks(PROMPT_BATCH).enumerate() {
        batch.clear();
        for (j, &token) in chunk.iter().enumerate() {
            let last = i + 1 == chunks && j + 1 == chunk.len();
            batch.add(token, pos, last).map_err(|e| e.to_string())?;
            pos += 1;
        }
        target
            .context_mut()
            .decode(&batch)
            .map_err(|e| e.to_string())?;
        target.decoded(&batch)?;
    }
    Ok((tokens, batch))
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
    think: ThinkSplit,
    tools: Option<ToolSplit>,
    require_tool_call: bool,
    /// Whether tool calls were sent; nothing is sent after them.
    called: bool,
    /// Why reading the tool calls failed, which fails the request.
    failure: Option<String>,
}

impl Output {
    fn new(job: Job) -> (String, Sampling, Self) {
        let output = Self {
            events: job.events,
            limit: job.limit,
            stops: StopMatcher::new(job.stop),
            pending: Vec::new(),
            produced: 0,
            think: ThinkSplit::new(job.reasons),
            tools: job.tools.map(ToolSplit::new),
            require_tool_call: job.require_tool_call,
            called: false,
            failure: None,
        };
        (job.prompt, job.sampling, output)
    }

    /// Whether generation has left any reasoning block, so `response_format` applies.
    fn in_content(&self) -> bool {
        !self.think.in_think()
    }

    /// Sends one sampled token, or returns why generation ends.
    fn token(&mut self, model: &Model, token: Token) -> Option<FinishReason> {
        let vocab = model.vocab();
        if vocab.is_eog(token) {
            return Some(FinishReason::Stop);
        }
        self.produced += 1;
        self.pending.extend(vocab.token_to_piece(token, true));
        let text = take_utf8(&mut self.pending);
        let (emit, stopped) = self.stops.push(&text);
        if let Some(finish) = self.route(&emit) {
            return Some(finish);
        }
        if stopped {
            return Some(FinishReason::Stop);
        }
        self.limit
            .is_some_and(|limit| self.produced as usize >= limit)
            .then_some(FinishReason::Length)
    }

    /// Whether the client is still there to take `event`.
    fn send(&self, event: Event) -> bool {
        self.events.blocking_send(event).is_ok()
    }

    /// Splits `text` into reasoning and content and sends them; content goes through the
    /// tool-call reader first. Returns why generation ends, if it does.
    fn route(&mut self, text: &str) -> Option<FinishReason> {
        if self.called || text.is_empty() {
            return None;
        }
        let (reasoning, content) = self.think.push(text);
        if !reasoning.is_empty() && !self.send(Event::Reasoning(reasoning)) {
            return Some(FinishReason::Stop);
        }
        self.content(content)
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
            let tail = self.stops.finish();
            if self.route(&tail) == Some(FinishReason::ToolCalls) {
                finish = FinishReason::ToolCalls;
            }
        }
        if !self.called {
            let (reasoning, content) = self.think.finish();
            if !reasoning.is_empty() {
                self.send(Event::Reasoning(reasoning));
            }
            if self.content(content) == Some(FinishReason::ToolCalls) {
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
/// outstanding, end-of-generation tokens are ruled out.
struct Picker<'v> {
    free: Sampler<'v>,
    constrained: Option<Sampler<'v>>,
}

impl<'v> Picker<'v> {
    fn new(
        vocab: &Vocab<'v>,
        sampling: &Sampling,
        eog: &[Token],
    ) -> std::result::Result<Self, String> {
        let chain = |grammar: Option<&str>| -> std::result::Result<Sampler<'v>, String> {
            let mut samplers = Vec::new();
            if sampling.ban_eog {
                let ruled_out: Vec<_> = eog.iter().map(|&t| (t, f32::NEG_INFINITY)).collect();
                samplers.push(Sampler::logit_bias(vocab, &ruled_out));
            }
            if let Some(grammar) = grammar {
                samplers.push(Sampler::grammar(vocab, grammar).map_err(|e| e.to_string())?);
            }
            // Greedy at temperature 0 or unset, as the mistral.rs engine's deterministic
            // default; otherwise top-p then temperature, then a seeded draw.
            match sampling.temperature {
                Some(t) if t > 0.0 => {
                    samplers.push(Sampler::top_p(sampling.top_p.unwrap_or(1.0), 1));
                    samplers.push(Sampler::temp(t));
                    samplers.push(Sampler::dist(sampling.seed));
                }
                _ => samplers.push(Sampler::greedy()),
            }
            Ok(Sampler::chain(samplers))
        };
        Ok(Self {
            free: chain(None)?,
            constrained: sampling
                .grammar
                .as_deref()
                .map(|g| chain(Some(g)))
                .transpose()?,
        })
    }

    fn sample(&mut self, context: &Context<'_>, index: i32, in_content: bool) -> Token {
        match &mut self.constrained {
            Some(constrained) if in_content => constrained.sample(context, index),
            _ => self.free.sample(context, index),
        }
    }
}

/// Runs one request, one token per forward pass.
fn generate(
    model: &Model,
    context: &mut Context<'_>,
    job: Job,
    eog: &[Token],
) -> std::result::Result<Outcome, String> {
    let (prompt, sampling, mut out) = Output::new(job);
    let vocab = model.vocab();
    let mut picker = Picker::new(&vocab, &sampling, eog)?;
    let (tokens, mut batch) = read_prompt(model, context, &prompt)?;
    let n_ctx = usize::try_from(context.n_ctx()).unwrap_or(usize::MAX);
    let mut pos = i32::try_from(tokens.len()).map_err(|e| e.to_string())?;
    let finish = loop {
        let token = picker.sample(context, batch.n_tokens() - 1, out.in_content());
        if let Some(finish) = out.token(model, token) {
            break finish;
        }
        if usize::try_from(pos).unwrap_or(usize::MAX) + 1 >= n_ctx {
            break FinishReason::Length;
        }
        batch.clear();
        batch.add(token, pos, true).map_err(|e| e.to_string())?;
        pos += 1;
        context.decode(&batch).map_err(|e| e.to_string())?;
    };
    out.finish(finish, tokens.len())
}

/// Runs one request with MTP drafting: each forward pass verifies the last token plus up
/// to [`MTP_DRAFT`] drafted ones, and keeps the drafts the target would have sampled.
fn generate_mtp(
    model: &Model,
    spec: &mut Mtp<'_>,
    job: Job,
    eog: &[Token],
) -> std::result::Result<Outcome, String> {
    let (prompt, sampling, mut out) = Output::new(job);
    let vocab = model.vocab();
    let mut picker = Picker::new(&vocab, &sampling, eog)?;
    spec.draft_mut().clear();
    // `read_prompt` hands every decoded prompt batch to the head, which needs the target's
    // hidden state at each prompt position before `begin`.
    let (tokens, mut batch) = read_prompt(model, spec, &prompt)?;
    spec.begin(&tokens).map_err(|e| e.to_string())?;
    let n_ctx = usize::try_from(spec.target().n_ctx()).unwrap_or(usize::MAX);
    let mut history = tokens.clone();
    let mut n_past = i32::try_from(tokens.len()).map_err(|e| e.to_string())?;
    let mut id = picker.sample(spec.target(), batch.n_tokens() - 1, out.in_content());
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
        truncate_cache(spec.target_mut(), n_past)?;
        truncate_cache(spec.draft_mut(), n_past)?;
        id = next;
    };
    out.finish(finish, tokens.len())
}

/// Removes sequence 0's cache entries from position `from` on. A context that cannot roll
/// back that far, such as a recurrent layer with too few state snapshots, fails the request
/// instead of decoding on from a state that still holds the removed tokens.
fn truncate_cache(context: &mut Context<'_>, from: i32) -> std::result::Result<(), String> {
    if context.truncate(from) {
        Ok(())
    } else {
        Err(format!(
            "llama.cpp could not roll its cache back to position {from}"
        ))
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

fn piece(model: &Model, token: Token) -> Option<String> {
    if token.0 < 0 {
        return None;
    }
    String::from_utf8(model.vocab().token_to_piece(token, true))
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

    fn in_think(&self) -> bool {
        self.in_think
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
            tools
                .iter()
                .filter(|t| {
                    allowed.tools.iter().any(|a| {
                        matches!(a, AllowedToolChoice::Function { name } if *name == t.function.name)
                    })
                })
                .collect()
        }
        _ => Vec::new(),
    }
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
    tools: &[Tool],
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
    let offered: Vec<&Tool> = tools.iter().collect();
    let call = json_grammar(&call_schema(&offered), "tools")?;
    Ok(Some(either_grammar(&call, &reply)))
}

/// The JSON schema of a call to one of `tools`, with the arguments held to the tool's schema.
fn call_schema(tools: &[&Tool]) -> serde_json::Value {
    let calls: Vec<serde_json::Value> = tools
        .iter()
        .map(|tool| {
            let arguments = tool.function.parameters.as_ref().map_or_else(
                || serde_json::json!({ "type": "object" }),
                |p| serde_json::Value::Object(p.clone().into_iter().collect()),
            );
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
    fn output(choice: ToolChoice, reasons: bool) -> (Output, mpsc::Receiver<Event>) {
        let (events, rx) = mpsc::channel(STREAM_BUFFER);
        let tools = [weather_tool()];
        let matcher = ToolCallingMatcher::new_with_format(choice, Some(&tools), None)
            .expect("a matcher for one tool");
        let job = Job {
            prompt: String::new(),
            limit: None,
            sampling: Sampling::default(),
            stop: Vec::new(),
            reasons,
            require_tool_call: matcher.requires_tool_call(),
            tools: Some(matcher),
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
        let (mut out, rx) = output(ToolChoice::Auto, false);
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
        let (mut out, rx) = output(ToolChoice::Auto, false);
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
        let (mut out, rx) = output(ToolChoice::Auto, false);
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
        let (mut out, rx) = output(ToolChoice::Auto, false);
        assert_eq!(out.route("a <"), None);
        assert_eq!(out.route("b"), None);
        out.finish(FinishReason::Stop, 1)
            .expect("the request succeeds");
        assert_eq!(sent(rx).concat(), "c:a c:<b");
    }

    #[test]
    fn a_call_cut_off_by_the_end_of_generation_is_content() {
        let (mut out, rx) = output(ToolChoice::Auto, false);
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
        let (mut out, rx) = output(ToolChoice::Auto, true);
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
        let (mut out, rx) = output(ToolChoice::None, false);
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
        let (mut out, rx) = output(ToolChoice::Required, false);
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
        let grammar = content_grammar("m", TAGGED, None, std::slice::from_ref(&tool), &[&tool])
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
            content_grammar(
                "m",
                "{{ '[TOOL_CALLS]' }}",
                None,
                std::slice::from_ref(&tool),
                &[&tool]
            )
            .expect("no conversion"),
            None,
            "another template format has no grammar; end-of-generation is held off instead"
        );
    }

    #[test]
    fn a_structured_reply_beside_tools_is_either_a_call_or_the_reply() {
        let tool = weather_tool();
        let grammar = content_grammar(
            "m",
            TAGGED,
            Some(&answer_schema()),
            std::slice::from_ref(&tool),
            &[],
        )
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
            std::slice::from_ref(&tool),
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
