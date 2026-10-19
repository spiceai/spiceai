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
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use std::pin::Pin;
use std::task::{Context, Poll};

use itertools::Itertools;
use llms::chat::nsql::SqlGeneration;
use llms::chat::{Chat, Result as ChatResult};

use async_openai::error::OpenAIError;
use async_openai::types::chat::{
    ChatChoiceStream, ChatCompletionMessageToolCall, ChatCompletionMessageToolCalls,
    ChatCompletionRequestAssistantMessage, ChatCompletionRequestAssistantMessageArgs,
    ChatCompletionRequestMessage, ChatCompletionRequestToolMessageArgs,
    ChatCompletionResponseStream, ChatCompletionTool, ChatCompletionToolChoiceOption,
    ChatCompletionTools, CompletionTokensDetails, CompletionUsage, CreateChatCompletionRequest,
    CreateChatCompletionResponse, CreateChatCompletionStreamResponse, FinishReason, FunctionCall,
    FunctionObject, PromptTokensDetails, ToolChoiceAllowedMode, ToolChoiceOptions,
};

use async_trait::async_trait;
use futures::{Stream, StreamExt};
use pin_project::pin_project;
use serde_json::Value;

use tokio::sync::mpsc;
use tools::SpiceModelTool;
use tracing::{Instrument, Span};

use super::tool_trace::{
    CallerScope, PendingTurn, StreamedReply, ToolTraceStore, returned_message,
};
use crate::model::ModelContextExtension;
use crate::tools::utils::tool_call_error_response;
use llms::progress::Progress;
use runtime_request_context::{AsyncMarker, RequestContext};

pub struct ToolUsingChat {
    inner_chat: Arc<dyn Chat>,
    tools: Vec<Arc<dyn SpiceModelTool>>,
    recursion_limit: Option<usize>,
    /// The Spice tool steps of earlier turns, shared with every round's instance.
    traces: Arc<ToolTraceStore>,
}

impl ToolUsingChat {
    #[must_use]
    pub fn new(
        inner_chat: Arc<dyn Chat>,
        tools: Vec<Arc<dyn SpiceModelTool>>,
        recursion_limit: Option<usize>,
    ) -> Self {
        Self {
            inner_chat,
            tools,
            recursion_limit,
            traces: Arc::new(ToolTraceStore::default()),
        }
    }

    /// The model for the round after this one: one round fewer left, and the same tools and
    /// trace store.
    fn next_round(&self) -> Self {
        Self {
            inner_chat: Arc::clone(&self.inner_chat),
            tools: self.tools.clone(),
            recursion_limit: self.recursion_limit.map(|r| r.saturating_sub(1)),
            traces: Arc::clone(&self.traces),
        }
    }

    #[must_use]
    pub fn runtime_tools(&self) -> Vec<ChatCompletionTool> {
        self.tools
            .iter()
            .map(|t| ChatCompletionTool {
                function: FunctionObject {
                    strict: t.strict(),
                    name: encode_tool_name(t.name().to_string().as_str()),
                    description: t.description().map(|d| d.to_string()),
                    parameters: t.parameters(),
                },
            })
            .collect_vec()
    }

    /// Create a new [`CreateChatCompletionRequest`] with the system prompt injected as the first message.
    async fn prepare_req(
        &self,
        mut req: CreateChatCompletionRequest,
    ) -> Result<CreateChatCompletionRequest, OpenAIError> {
        if let Some(list_datasets) = self.tools.iter().find(|t| t.name() == "list_datasets") {
            let list_dataset_messages = self.create_list_dataset_messages(list_datasets).await?;
            req.messages =
                insert_initial_tools(req.messages, "list_datasets", &list_dataset_messages);
        }

        Ok(req)
    }

    /// Create the messagges expected from a model if it has called the `list_datasets` tool, and recieved a response.
    /// This is useful to prime the model as if it has already asked to list the available datasets.
    async fn create_list_dataset_messages(
        &self,
        list_datasets: &Arc<dyn SpiceModelTool>,
    ) -> Result<Vec<ChatCompletionRequestMessage>, OpenAIError> {
        let t_resp = match list_datasets.call("").await {
            Ok(resp) => resp,
            Err(e) => {
                let tool_name = list_datasets.name();
                let error = e.to_string();
                tracing::warn!(
                    "Tool '{tool_name}' failed while creating initial tool-use messages: {error}"
                );
                tool_call_error_response(tool_name.as_ref(), error)
            }
        };
        Ok(vec![
            ChatCompletionRequestAssistantMessageArgs::default()
                .tool_calls(vec![ChatCompletionMessageToolCalls::Function(
                    ChatCompletionMessageToolCall {
                        id: "initial_list_datasets".to_string(),
                        function: FunctionCall {
                            name: list_datasets.name().to_string(),
                            arguments: String::new(),
                        },
                    },
                )])
                .build()?
                .into(),
            ChatCompletionRequestToolMessageArgs::default()
                .content(t_resp.to_string())
                .tool_call_id("initial_list_datasets".to_string())
                .build()?
                .into(),
        ])
    }

    /// Check if a tool call is a spiced runtime tool.
    fn as_spiced_tool(&self, t: &ChatCompletionMessageToolCall) -> Option<Arc<dyn SpiceModelTool>> {
        self.tools
            .iter()
            .find(|tool| encode_tool_name(tool.name().as_ref()) == t.function.name)
            .cloned()
    }

    /// Call a spiced runtime tool.
    ///
    /// Return the result as a JSON value.
    async fn call_tool(&self, tool_call: &ChatCompletionMessageToolCall) -> Value {
        match self.as_spiced_tool(tool_call) {
            Some(t) => match t.call(&tool_call.function.arguments).await {
                Ok(v) => {
                    tracing::info!(
                        target: "task_history",
                        progress = Progress::log()
                            .id(Some(tool_call.id.clone()))
                            .title(format!("'{}' tool completed successfully", tool_call.function.name))
                            .json_content(v.clone())
                            .to_jsonl(),
                    );
                    v
                }
                Err(e) => {
                    tracing::info!(
                        target: "task_history",
                        progress = Progress::error()
                            .id(Some(tool_call.id.clone()))
                            .title(format!("'{}' tool completed unsuccessfully", tool_call.function.name))
                            .content(e.to_string())
                            .to_jsonl(),
                    );
                    tool_call_error_response(t.name().as_ref(), e)
                }
            },
            None => {
                // All calls to `call_tool` should have previously checked that `tool_call` has an associated tool.
                if cfg!(feature = "dev") {
                    panic!(
                        "Tool '{}' was provided to LLM, but now no longer exists. This should not be possible.",
                        tool_call.function.name
                    );
                } else {
                    tracing::warn!(
                        "Tool '{}' was provided to LLM, but now no longer exists. This should not be possible.",
                        tool_call.function.name
                    );
                    Value::Null
                }
            }
        }
    }

    /// For `requested_tools` requested by a model, check if any are spiced runtime tools, and if
    /// so, run them locally and create the messages that tell the model about the calls and
    /// their results.
    ///
    /// Returns
    /// - `None` if no spiced runtime tools were used. Note: external tools may still have been
    ///   requested.
    /// - `Some(messages)` if spiced runtime tools were used: the assistant tool-call message,
    ///   then one tool message per call. Appended to the round's messages, they are ready to be
    ///   reprocessed by the model.
    async fn process_tool_calls_and_run_spice_tools(
        &self,
        requested_tools: Vec<ChatCompletionMessageToolCall>,
    ) -> Result<Option<Vec<ChatCompletionRequestMessage>>, OpenAIError> {
        let spiced_tools = requested_tools
            .iter()
            .filter(|&t| self.as_spiced_tool(t).is_some())
            .cloned()
            .collect_vec();

        tracing::debug!(
            "spiced_tools available: {:?}. Used {:?}",
            self.tools.iter().map(|t| t.name()).collect_vec(),
            spiced_tools
        );

        // Return early if no spiced runtime tools used.
        if spiced_tools.is_empty() {
            tracing::debug!("No spiced tools used by chat model, returning early");
            return Ok(None);
        }

        // Tell model the assistant has these tools
        let assistant_message: ChatCompletionRequestMessage =
            ChatCompletionRequestAssistantMessageArgs::default()
                .tool_calls(
                    spiced_tools
                        .iter()
                        .map(|t| ChatCompletionMessageToolCalls::Function(t.clone()))
                        .collect::<Vec<_>>(),
                ) // TODO - should this include non-spiced tools?
                .build()?
                .into();

        let mut tool_and_response_content = vec![];
        for t in spiced_tools.clone() {
            tracing::info!(
                target: "task_history",
                progress = Progress::log()
                    .id(Some(t.id.clone()))
                    .title(format!("Calling '{}' tool", t.function.name))
                    .content(t.function.arguments.clone())
                    .to_jsonl(),
            );

            let content = self.call_tool(&t).await;
            tool_and_response_content.push((t, content));
        }

        tracing::debug!(
            "Ran tools, and retrieved responses: {:?}",
            tool_and_response_content
        );

        // Tell model the assistant used these tools, and provided result.
        let tool_messages: Vec<ChatCompletionRequestMessage> = tool_and_response_content
            .iter()
            .map(|(tool_call, response_content)| {
                Ok(ChatCompletionRequestToolMessageArgs::default()
                    .content(response_content.to_string())
                    .tool_call_id(tool_call.id.clone())
                    .build()?
                    .into())
            })
            .collect::<Result<_, OpenAIError>>()?;

        let mut messages = Vec::with_capacity(1 + tool_messages.len());
        messages.push(assistant_message);
        messages.extend(tool_messages);

        let context = RequestContext::current(AsyncMarker::new().await);
        crate::model::add_tools_used(&context, spiced_tools.len());

        Ok(Some(messages))
    }

    async fn chat_request_inner(
        &self,
        req: CreateChatCompletionRequest,
        recursion_limit: Option<usize>,
    ) -> Result<ToolLoopResponse, OpenAIError> {
        Box::pin(async move {
            // Don't use spice runtime tools if users has explicitly chosen to not use any tools.
            if req.tool_choice.as_ref().is_some_and(|c| {
                *c == ChatCompletionToolChoiceOption::Mode(ToolChoiceOptions::None)
            }) {
                tracing::debug!("User asked for no tools, calling inner chat model");
                return Ok(ToolLoopResponse::without_tools(
                    self.inner_chat.chat_request(req).await?,
                ));
            }

            if recursion_limit.is_some_and(|f| f == 0) {
                tracing::debug!(
                    "Tool-use recursion limit reached. Will call model, but not process further"
                );
                return Ok(ToolLoopResponse::without_tools(
                    self.inner_chat.chat_request(req).await?,
                ));
            }

            // Append spiced runtime tools to the request.
            let inner_req = self.add_runtime_tools(&req);

            let resp = self.inner_chat.chat_request(inner_req.clone()).await?;
            let usage = resp.usage.clone();

            // ChatCompletionMessageToolCall
            let tools_used = resp
                .choices
                .first()
                .and_then(|c| c.message.tool_calls.clone());

            // Extract inner ChatCompletionMessageToolCall from the ChatCompletionMessageToolCalls enum
            let tool_calls: Vec<ChatCompletionMessageToolCall> = tools_used
                .unwrap_or_default()
                .iter()
                .filter_map(|tc| match tc {
                    ChatCompletionMessageToolCalls::Function(call) => Some(call.clone()),
                    ChatCompletionMessageToolCalls::Custom(_) => None,
                })
                .collect();

            match self
                .process_tool_calls_and_run_spice_tools(tool_calls)
                .await?
            {
                // New messages means we have run spice tools locally, ready to recall model.
                Some(round) => {
                    let mut messages = req.messages;
                    messages.extend_from_slice(&round);
                    let next = self
                        .chat_request_inner(
                            create_new_recursive_req(&inner_req, messages, resp.usage.as_ref()),
                            recursion_limit.map(|r| r - 1),
                        )
                        .await?;

                    let mut response = next.response;
                    response.usage = combine_usage(usage, response.usage);
                    let mut hidden_messages = round;
                    hidden_messages.extend(next.hidden_messages);
                    Ok(ToolLoopResponse {
                        response,
                        hidden_messages,
                    })
                }
                None => Ok(ToolLoopResponse::without_tools(resp)),
            }
        })
        .await
    }

    /// Add the spice runtime tools to a list of tools (may contain external tools too), and ensure no duplicates.
    fn add_runtime_tools(&self, req: &CreateChatCompletionRequest) -> CreateChatCompletionRequest {
        let mut runtime_tools = self.runtime_tools();
        if let Some(ref request_tools) = req.tools {
            runtime_tools.extend(request_tools.iter().filter_map(|t| match t {
                ChatCompletionTools::Function(f) => Some(f.clone()),
                ChatCompletionTools::Custom(_) => None,
            }));
        }
        // Ensure function names are unique. Tool-use recursion sometimes creates duplicates.
        runtime_tools.sort_by(|a, b| a.function.name.cmp(&b.function.name));
        runtime_tools.dedup_by(|a, b| a.function.name == b.function.name);
        let mut req = req.clone();
        req.tools = Some(
            runtime_tools
                .into_iter()
                .map(ChatCompletionTools::Function)
                .collect(),
        );
        req
    }

    /// Streams the completion of `req`, running Spice tools as the model calls them. Every round
    /// that runs Spice tools adds its messages to `hidden`.
    async fn chat_stream_inner(
        &self,
        req: CreateChatCompletionRequest,
        hidden: &HiddenMessages,
    ) -> Result<ChatCompletionResponseStream, OpenAIError> {
        // Don't use spice runtime tools if users has explicitly chosen to not use any tools.
        if req
            .tool_choice
            .as_ref()
            .is_some_and(|c| *c == ChatCompletionToolChoiceOption::Mode(ToolChoiceOptions::None))
        {
            return self.inner_chat.chat_stream(req).await;
        }

        if self.recursion_limit.is_some_and(|f| f == 0) {
            tracing::debug!(
                "Tool-use recursion limit reached. Will call model, but not process further"
            );
            return self.inner_chat.chat_stream(req).await;
        }

        // Append spiced runtime tools to the request. Avoid clone if no runtime tools.
        let updated_req = self.add_runtime_tools(&req);
        let s = self.inner_chat.chat_stream(updated_req.clone()).await?;

        Ok(make_a_stream(
            Span::current(),
            RequestContext::current(AsyncMarker::new().await),
            self.next_round(),
            req,
            s,
            hidden.clone(),
        ))
    }
}

/// A chat completion from [`ToolUsingChat::chat_request_inner`], and the messages its rounds
/// added for the model, which the client does not receive.
struct ToolLoopResponse {
    response: CreateChatCompletionResponse,
    /// The assistant tool-call and tool messages of each round that ran Spice tools, in order.
    hidden_messages: Vec<ChatCompletionRequestMessage>,
}

impl ToolLoopResponse {
    /// A response from a round that ran no Spice tools.
    fn without_tools(response: CreateChatCompletionResponse) -> Self {
        Self {
            response,
            hidden_messages: Vec::new(),
        }
    }
}

/// The messages the rounds of one streamed request add for the model, which the client does
/// not receive. Each round runs in a task of its own, so they share this.
#[derive(Clone, Default)]
struct HiddenMessages(Arc<parking_lot::Mutex<Vec<ChatCompletionRequestMessage>>>);

impl HiddenMessages {
    fn extend(&self, round: &[ChatCompletionRequestMessage]) {
        self.0.lock().extend_from_slice(round);
    }

    fn take(&self) -> Vec<ChatCompletionRequestMessage> {
        std::mem::take(&mut *self.0.lock())
    }
}

#[deny(clippy::missing_trait_methods)]
#[async_trait]
impl Chat for ToolUsingChat {
    async fn run(&self, prompt: String) -> ChatResult<Option<String>> {
        self.inner_chat.run(prompt).await
    }

    async fn stream<'a>(
        &self,
        prompt: String,
    ) -> ChatResult<Pin<Box<dyn Stream<Item = ChatResult<Option<String>>> + Send>>> {
        self.inner_chat.stream(prompt).await
    }

    async fn chat_stream(
        &self,
        mut req: CreateChatCompletionRequest,
    ) -> Result<ChatCompletionResponseStream, OpenAIError> {
        let context = RequestContext::current(AsyncMarker::new().await);
        if context.extension::<ModelContextExtension>().is_none() {
            context.insert_extension(ModelContextExtension::new());
        }

        let (messages, turn) = self
            .traces
            .begin_turn(
                &CallerScope::of(&context),
                std::mem::take(&mut req.messages),
            )
            .await;
        req.messages = messages;
        let inner_req = self.prepare_req(req).await?;

        let hidden = HiddenMessages::default();
        let stream = self.chat_stream_inner(inner_req, &hidden).await?;
        let stream = record_turn_at_end(
            stream,
            Arc::clone(&self.traces),
            turn,
            hidden,
            Arc::clone(&context),
        );
        // wrap the completion stream to track the `ai_inferences_with_spice_count` when it is ready.
        Ok(Box::pin(InferenceTrackingStream::new(stream, context)))
    }

    async fn chat_request(
        &self,
        mut req: CreateChatCompletionRequest,
    ) -> Result<CreateChatCompletionResponse, OpenAIError> {
        let context = RequestContext::current(AsyncMarker::new().await);
        if context.extension::<ModelContextExtension>().is_none() {
            context.insert_extension(ModelContextExtension::new());
        }

        let (messages, turn) = self
            .traces
            .begin_turn(
                &CallerScope::of(&context),
                std::mem::take(&mut req.messages),
            )
            .await;
        req.messages = messages;
        let inner_req = self.prepare_req(req).await?;
        let response = self
            .chat_request_inner(inner_req, self.recursion_limit)
            .await;

        // track ai_inferences_with_spice_count metric
        crate::model::track_ai_inferences_with_spice_count(&context);

        let ToolLoopResponse {
            response,
            hidden_messages,
        } = response?;
        if let Some(returned) = returned_message(&response) {
            self.traces
                .finish_turn(turn, hidden_messages, &returned)
                .await;
        }
        Ok(response)
    }

    fn as_sql(&self) -> Option<&dyn SqlGeneration> {
        self.inner_chat.as_sql()
    }

    /// Override health endpoint to 1. avoid passing tools in request, 2. pre-calling `list_datasets` in [`ToolUsingChat::prepare_req`].
    async fn health(&self) -> ChatResult<()> {
        self.inner_chat.health().await
    }
}

/// Create the next round's [`CreateChatCompletionRequest`]: the new messages, and
/// the [`next_round_tool_choice`].
fn create_new_recursive_req(
    req: &CreateChatCompletionRequest,
    new_msg: Vec<ChatCompletionRequestMessage>,
    marginal_usage: Option<&CompletionUsage>,
) -> CreateChatCompletionRequest {
    let mut new_req = req.clone();
    new_req.messages = new_msg;
    new_req.tool_choice = Some(next_round_tool_choice(new_req.tool_choice.take()));

    // Adjust input `max_completion_tokens` if usage is known to ensure we don't exceed the limit.
    if let Some(max_completion_tokens) = new_req.max_completion_tokens
        && let Some(usage) = marginal_usage
    {
        new_req.max_completion_tokens =
            Some(max_completion_tokens.saturating_sub(usage.completion_tokens));
    }

    new_req
}

/// The `tool_choice` for the round after one that called tools (issue #14459).
///
/// A choice that forces a call — a named function or custom tool, `required`, or
/// `allowed_tools` in `required` mode — is satisfied once a round has called a tool,
/// so the next round gets `auto`, and `allowed_tools` keeps its tool list in `auto`
/// mode. Re-sending it would force a call on every round until
/// `tool_recursion_limit` runs out, and the turn could never end in an answer.
///
/// An unset choice becomes `auto` too: the model's Spicepod `tool_choice` default is
/// filled into an unset choice on every round, below this loop.
fn next_round_tool_choice(
    choice: Option<ChatCompletionToolChoiceOption>,
) -> ChatCompletionToolChoiceOption {
    match choice {
        None => ChatCompletionToolChoiceOption::Mode(ToolChoiceOptions::Auto),
        Some(
            ChatCompletionToolChoiceOption::Function(_)
            | ChatCompletionToolChoiceOption::Custom(_)
            | ChatCompletionToolChoiceOption::Mode(ToolChoiceOptions::Required),
        ) => {
            tracing::debug!("Not forcing a tool call again after a round that made one.");
            ChatCompletionToolChoiceOption::Mode(ToolChoiceOptions::Auto)
        }
        Some(ChatCompletionToolChoiceOption::AllowedTools(mut allowed)) => {
            allowed.allowed_tools.mode = ToolChoiceAllowedMode::Auto;
            ChatCompletionToolChoiceOption::AllowedTools(allowed)
        }
        Some(
            choice @ ChatCompletionToolChoiceOption::Mode(
                ToolChoiceOptions::Auto | ToolChoiceOptions::None,
            ),
        ) => choice,
    }
}

pub fn combine_usage(
    u1: Option<CompletionUsage>,
    u2: Option<CompletionUsage>,
) -> Option<CompletionUsage> {
    match (u1, u2) {
        (Some(u1), Some(u2)) => Some(CompletionUsage {
            prompt_tokens: u1.prompt_tokens + u2.prompt_tokens,
            completion_tokens: u1.completion_tokens + u2.completion_tokens,
            total_tokens: u1.total_tokens + u2.total_tokens,
            prompt_tokens_details: combine_token_details(
                u1.prompt_tokens_details,
                u2.prompt_tokens_details,
            ),
            completion_tokens_details: combine_completion_details(
                u1.completion_tokens_details,
                u2.completion_tokens_details,
            ),
        }),
        (Some(u1), None) => Some(u1),
        (None, Some(u2)) => Some(u2),
        (None, None) => None,
    }
}
fn combine_token_details(
    a: Option<PromptTokensDetails>,
    b: Option<PromptTokensDetails>,
) -> Option<PromptTokensDetails> {
    match (a, b) {
        (Some(a), Some(b)) => Some(PromptTokensDetails {
            audio_tokens: combine_opt_u32(a.audio_tokens, b.audio_tokens),
            cached_tokens: combine_opt_u32(a.cached_tokens, b.cached_tokens),
        }),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    }
}

fn combine_completion_details(
    a: Option<CompletionTokensDetails>,
    b: Option<CompletionTokensDetails>,
) -> Option<CompletionTokensDetails> {
    match (a, b) {
        (Some(a), Some(b)) => Some(CompletionTokensDetails {
            accepted_prediction_tokens: combine_opt_u32(
                a.accepted_prediction_tokens,
                b.accepted_prediction_tokens,
            ),
            audio_tokens: combine_opt_u32(a.audio_tokens, b.audio_tokens),
            reasoning_tokens: combine_opt_u32(a.reasoning_tokens, b.reasoning_tokens),
            rejected_prediction_tokens: combine_opt_u32(
                a.rejected_prediction_tokens,
                b.rejected_prediction_tokens,
            ),
        }),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    }
}
pub fn combine_opt_u32(a: Option<u32>, b: Option<u32>) -> Option<u32> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a + b),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    }
}

// Ensure that `tool_messages` have been added to `messages` after all initial developer/system messages and after initial user messages (i.e. not including user messages after assistant messages).
fn insert_initial_tools(
    messages: Vec<ChatCompletionRequestMessage>,
    tool_name: &str,
    tool_messages: &[ChatCompletionRequestMessage],
) -> Vec<ChatCompletionRequestMessage> {
    // Do not add `tool_messages` if already in `messages`.
    if messages.iter().any(|m| {
        let ChatCompletionRequestMessage::Assistant(ChatCompletionRequestAssistantMessage {
            tool_calls: Some(tools),
            ..
        }) = m
        else {
            return false;
        };
        tools.iter().any(|t| match t {
            ChatCompletionMessageToolCalls::Function(call) => call.function.name == tool_name,
            ChatCompletionMessageToolCalls::Custom(_) => false,
        })
    }) {
        return messages;
    }

    // Find index to insert at
    let idx = messages
        .iter()
        .enumerate()
        .find_map(|(i, m)| {
            if matches!(
                m,
                ChatCompletionRequestMessage::Assistant(_)
                    | ChatCompletionRequestMessage::Tool(_)
                    | ChatCompletionRequestMessage::Function(_)
            ) {
                return Some(i);
            }
            None
        })
        .unwrap_or(messages.len());

    let Some((a, b)) = messages.split_at_checked(idx) else {
        return messages;
    };

    [a, tool_messages, b].concat()
}

struct CustomStream {
    receiver: mpsc::Receiver<Result<CreateChatCompletionStreamResponse, OpenAIError>>,
}

impl Stream for CustomStream {
    type Item = Result<CreateChatCompletionStreamResponse, OpenAIError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.receiver.poll_recv(cx)
    }
}

/// What [`make_a_stream`] does with one choice of an upstream chunk.
#[derive(Debug, PartialEq, Eq)]
enum ChoiceDisposition {
    /// Pass the choice to the caller.
    Forward,
    /// The model finished a tool call: run the Spice tools it named and stream the
    /// follow-up answer in place of this choice.
    RunTools,
    /// Nothing in the choice is for the caller: no finish and no content.
    Drop,
}

/// Decides each choice exactly once, so a choice is never forwarded twice and a
/// `tool_calls` finish that Spice acts on is never forwarded at all. Every other
/// finish is forwarded whether or not the provider sent `content` beside it.
///
/// Some providers (`DeepSeek`, for one) put `"content": ""` on every chunk, the
/// tool-call chunks and the final `tool_calls` finish included. Deciding on
/// `content.is_some()` alone forwarded that finish after the follow-up answer, so
/// the stream ended on `finish_reason: tool_calls` although the tool had run and
/// the answer was complete, and a chunk carrying both content and `stop` was
/// forwarded twice.
fn choice_disposition(choice: &ChatChoiceStream) -> ChoiceDisposition {
    match choice.finish_reason {
        Some(FinishReason::ToolCalls) => ChoiceDisposition::RunTools,
        Some(_) => ChoiceDisposition::Forward,
        None if choice.delta.content.is_some() => ChoiceDisposition::Forward,
        None => ChoiceDisposition::Drop,
    }
}

/// Passes `stream` through to the client, and records `turn` in `traces` once the stream ends:
/// unless it yielded an error, or the request was cancelled first.
///
/// The stream is drained even after its client goes away (see `http::v1::chat`), and the
/// request's cancellation is what tells such a turn apart from one the client received whole.
fn record_turn_at_end(
    mut stream: ChatCompletionResponseStream,
    traces: Arc<ToolTraceStore>,
    turn: PendingTurn,
    hidden: HiddenMessages,
    request_context: Arc<RequestContext>,
) -> ChatCompletionResponseStream {
    Box::pin(async_stream::stream! {
        let mut reply = StreamedReply::default();
        let mut failed = false;
        while let Some(item) = stream.next().await {
            match &item {
                Ok(chunk) => reply.observe(chunk),
                Err(_) => failed = true,
            }
            yield item;
        }
        if !failed && !request_context.is_cancelled() {
            traces
                .finish_turn(turn, hidden.take(), &reply.into_message())
                .await;
        }
    })
}

fn make_a_stream(
    span: Span,
    request_context: Arc<RequestContext>,
    model: ToolUsingChat,
    req: CreateChatCompletionRequest,
    mut s: ChatCompletionResponseStream,
    hidden: HiddenMessages,
) -> ChatCompletionResponseStream {
    let (sender, receiver) = mpsc::channel(100);
    let sender_clone = sender;
    let wants_usage = req
        .stream_options
        .as_ref()
        .and_then(|o| o.include_usage)
        .unwrap_or(false);

    tokio::spawn(
        request_context
            .scope(async move {
                let tool_call_states: Arc<
                    Mutex<HashMap<(i32, i32), ChatCompletionMessageToolCall>>,
                > = Arc::new(Mutex::new(HashMap::new()));

                let mut chat_output = String::new();
                // Usage is withheld from every round and sent once, last, as the total across rounds.
                let mut total_usage: Option<CompletionUsage> = None;
                let mut usage_chunk: Option<CreateChatCompletionStreamResponse> = None;

                while let Some(result) = s.next().await {
                    let mut response = match result {
                        Ok(response) => response,
                        Err(e) => {
                            if let Err(e) = sender_clone.send(Err(e)).await
                                && !sender_clone.is_closed() {
                                    tracing::error!("Error sending error: {}", e);
                                }
                            return;
                        }
                    };
                    let round_usage = response.usage.take();
                    if round_usage.is_some() {
                        let mut template = response.clone();
                        template.choices.clear();
                        usage_chunk = Some(template);
                        total_usage = combine_usage(total_usage, round_usage.clone());
                    }
                    let mut finished_choices: Vec<ChatChoiceStream> = vec![];
                    for chat_choice1 in &response.choices {
                        let chat_choice = chat_choice1.clone();

                        // Appending the tool call chunks
                        // TODO: only concatenate, spiced tools
                        if let Some(ref tool_calls) = chat_choice.delta.tool_calls {
                            for tool_call_chunk in tool_calls {
                                let key: (i32, i32) = if let (Ok(index), Ok(tool_call_index)) = (chat_choice.index.try_into(), tool_call_chunk.index.try_into()) { (index, tool_call_index) } else {
                                    tracing::error!(
                                        "chat_choice.index value {} or tool_call_chunk.index value {} is too large to fit in an i32",
                                        chat_choice.index,
                                        tool_call_chunk.index
                                    );
                                    return;
                                };

                                let states = Arc::clone(&tool_call_states);
                                let tool_call_data = tool_call_chunk.clone();

                                let mut states_lock = match states.lock() {
                                    Ok(lock) => lock,
                                    Err(e) => {
                                        tracing::error!("Failed to lock tool_call_states: {}", e);
                                        return;
                                    }
                                };

                                let state = states_lock.entry(key).or_insert_with(|| {
                                    ChatCompletionMessageToolCall {
                                        id: tool_call_data.id.clone().unwrap_or_default(),
                                        function: FunctionCall {
                                            name: tool_call_data
                                                .function
                                                .as_ref()
                                                .and_then(|f| f.name.clone())
                                                .unwrap_or_default(),
                                            arguments: String::new(),
                                        },
                                    }
                                });

                                if let Some(arguments) = tool_call_chunk
                                    .function
                                    .as_ref()
                                    .and_then(|f| f.arguments.as_ref())
                                {
                                    state.function.arguments.push_str(arguments);
                                }
                            }
                        }
                        match choice_disposition(&chat_choice) {
                            ChoiceDisposition::Forward => {
                                finished_choices.push(chat_choice);
                                continue;
                            }
                            ChoiceDisposition::Drop => continue,
                            ChoiceDisposition::RunTools => {}
                        }

                        // A tool call has finished (i.e. we have all chunks), process it.
                        let tool_calls_to_process = match tool_call_states.lock() {
                            Ok(states_lock) => states_lock.values().cloned().collect(),
                            Err(e) => {
                                tracing::error!("Failed to lock tool_call_states: {}", e);
                                return;
                            }
                        };

                        let round = match model
                            .process_tool_calls_and_run_spice_tools(tool_calls_to_process)
                            .await
                        {
                            Ok(Some(round)) => round,
                            Ok(None) => {
                                // No spice tools within returned tools, so return as message in stream.
                                finished_choices.push(chat_choice);
                                continue;
                            }
                            Err(e) => {
                                if let Err(e) = sender_clone.send(Err(e)).await
                                    && !sender_clone.is_closed() {
                                        tracing::error!("Error sending error: {}", e);
                                    }
                                return;
                            }
                        };
                        hidden.extend(&round);
                        let mut new_messages = req.messages.clone();
                        new_messages.extend(round);

                        // Text the model wrote alongside the tool call belongs before the answer
                        // the tool results produce, not after it.
                        if chat_choice
                            .delta
                            .content
                            .as_deref()
                            .is_some_and(|text| !text.is_empty())
                        {
                            let mut text_only = chat_choice;
                            text_only.delta.tool_calls = None;
                            text_only.finish_reason = None;
                            if let Some(text) = &text_only.delta.content {
                                chat_output.push_str(text);
                            }
                            let mut resp = response.clone();
                            resp.choices = vec![text_only];
                            if let Err(e) = sender_clone.send(Ok(resp)).await {
                                if !sender_clone.is_closed() {
                                    tracing::error!("Error sending error: {}", e);
                                }
                                return;
                            }
                        }

                        match model
                            .chat_stream_inner(
                                create_new_recursive_req(
                                    &req,
                                    new_messages,
                                    round_usage.as_ref(),
                                ),
                                &hidden,
                            )
                            .await
                        {
                            Ok(mut s) => {
                                while let Some(resp) = s.next().await {
                                    let resp = match resp {
                                        Ok(mut chunk) => {
                                            if let Some(nested) = chunk.usage.take() {
                                                total_usage = combine_usage(total_usage, Some(nested));
                                                if chunk.choices.is_empty() {
                                                    usage_chunk = Some(chunk);
                                                    continue;
                                                }
                                            }
                                            Ok(chunk)
                                        }
                                        Err(e) => Err(e),
                                    };
                                    // TODO check if this works for choices > 1.
                                    if let Err(e) = sender_clone.send(resp).await {
                                        if !sender_clone.is_closed() {
                                            tracing::error!("Error sending error: {}", e);
                                        }
                                        return;
                                    }
                                }
                            }
                            Err(e) => {
                                if let Err(e) = sender_clone.send(Err(e)).await
                                    && !sender_clone.is_closed() {
                                        tracing::error!("Error sending error: {}", e);
                                    }
                                return;
                            }
                        }
                    }

                    if let Some(choice) = finished_choices.first() {
                        if let Some(intermediate_chat_output) = &choice.delta.content {
                            chat_output.push_str(intermediate_chat_output);
                        }

                        let mut resp2 = response.clone();
                        resp2.choices = finished_choices;
                        if let Err(e) = sender_clone.send(Ok(resp2)).await
                            && !sender_clone.is_closed() {
                                tracing::error!("Error sending error: {}", e);
                            }
                    }
                }

                if wants_usage
                    && let Some(mut chunk) = usage_chunk
                    && total_usage.is_some()
                {
                    chunk.usage = total_usage;
                    if let Err(e) = sender_clone.send(Ok(chunk)).await
                        && !sender_clone.is_closed() {
                            tracing::error!("Error sending error: {}", e);
                        }
                }

                tracing::info!(target: "task_history", captured_output = %chat_output);
            })
            .instrument(span),
    );
    Box::pin(CustomStream { receiver }) as ChatCompletionResponseStream
}

// OpenAI tools must satisfy '^[a-zA-Z0-9_-]+$'. Commonly external tools may have '/' in their name.
pub fn encode_tool_name(name: &str) -> String {
    if name.contains('/') {
        name.replace('_', "__").replace('/', "_")
    } else {
        name.to_string()
    }
}

#[pin_project]
struct InferenceTrackingStream<S> {
    #[pin]
    stream: S,
    context: Arc<RequestContext>,
}

impl<S: Stream> InferenceTrackingStream<S> {
    pub fn new(stream: S, context: Arc<RequestContext>) -> Self {
        InferenceTrackingStream { stream, context }
    }
}

impl<S: Stream> Stream for InferenceTrackingStream<S> {
    type Item = S::Item;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        let stream = &mut this.stream;
        let context = this.context;

        match stream.as_mut().poll_next(cx) {
            Poll::Ready(None) => {
                let context = Arc::clone(context);
                crate::model::track_ai_inferences_with_spice_count(&context);
                Poll::Ready(None)
            }
            Poll::Ready(Some(item)) => Poll::Ready(Some(item)),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_openai::types::chat::{
        ChatCompletionAllowedTools, ChatCompletionAllowedToolsChoice,
        ChatCompletionMessageToolCall, ChatCompletionNamedToolChoiceCustom,
        ChatCompletionRequestAssistantMessageArgs, ChatCompletionRequestSystemMessageArgs,
        ChatCompletionRequestToolMessageArgs, ChatCompletionRequestUserMessageArgs, CustomName,
        FunctionCall,
    };
    use runtime_auth::{AuthPrincipalRef, AuthRequestContext};
    use runtime_request_context::Protocol;
    use serde_json::json;
    use spicepod::component::runtime::ApiKey;
    use std::borrow::Cow;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn stream_choice(json: &str) -> ChatChoiceStream {
        serde_json::from_str(json).expect("valid stream choice")
    }

    #[test]
    fn a_tool_calls_finish_runs_tools_whatever_its_content() {
        // DeepSeek shape: an empty `content` beside the finish. Regression test for #13309.
        for choice in [
            r#"{"index":0,"delta":{"content":""},"finish_reason":"tool_calls"}"#,
            r#"{"index":0,"delta":{},"finish_reason":"tool_calls"}"#,
            r#"{"index":0,"delta":{"content":"Let me check."},"finish_reason":"tool_calls"}"#,
        ] {
            assert_eq!(
                choice_disposition(&stream_choice(choice)),
                ChoiceDisposition::RunTools,
                "{choice}"
            );
        }
    }

    #[test]
    fn any_other_finish_is_forwarded_once_whatever_its_content() {
        for choice in [
            r#"{"index":0,"delta":{"content":""},"finish_reason":"stop"}"#,
            r#"{"index":0,"delta":{},"finish_reason":"stop"}"#,
            r#"{"index":0,"delta":{"content":"done"},"finish_reason":"length"}"#,
            r#"{"index":0,"delta":{},"finish_reason":"content_filter"}"#,
            r#"{"index":0,"delta":{"content":""},"finish_reason":"content_filter"}"#,
        ] {
            assert_eq!(
                choice_disposition(&stream_choice(choice)),
                ChoiceDisposition::Forward,
                "{choice}"
            );
        }
    }

    #[test]
    fn a_choice_without_a_finish_is_forwarded_only_when_it_carries_content() {
        assert_eq!(
            choice_disposition(&stream_choice(
                r#"{"index":0,"delta":{"content":"The table has "}}"#
            )),
            ChoiceDisposition::Forward
        );
        assert_eq!(
            choice_disposition(&stream_choice(
                r#"{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{}"}}]}}"#
            )),
            ChoiceDisposition::Drop
        );
    }

    fn create_system_message(content: &str) -> ChatCompletionRequestMessage {
        ChatCompletionRequestSystemMessageArgs::default()
            .content(content)
            .build()
            .expect("couldn't create system message")
            .into()
    }

    fn create_user_message(content: &str) -> ChatCompletionRequestMessage {
        ChatCompletionRequestUserMessageArgs::default()
            .content(content)
            .build()
            .expect("couldn't create user message")
            .into()
    }

    fn create_assistant_message_with_tool_calls(
        tool_calls: Vec<ChatCompletionMessageToolCall>,
    ) -> ChatCompletionRequestMessage {
        ChatCompletionRequestAssistantMessageArgs::default()
            .tool_calls(
                tool_calls
                    .into_iter()
                    .map(ChatCompletionMessageToolCalls::Function)
                    .collect::<Vec<_>>(),
            )
            .build()
            .expect("couldn't create assistant message w. tools")
            .into()
    }

    fn create_tool_message(tool_call_id: &str, content: &str) -> ChatCompletionRequestMessage {
        ChatCompletionRequestToolMessageArgs::default()
            .tool_call_id(tool_call_id)
            .content(content)
            .build()
            .expect("couldn't create tool message")
            .into()
    }

    fn create_list_datasets_tool_call() -> ChatCompletionMessageToolCall {
        ChatCompletionMessageToolCall {
            id: "test_id".to_string(),
            function: FunctionCall {
                name: "list_datasets".to_string(),
                arguments: "{}".to_string(),
            },
        }
    }

    #[test]
    fn test_insert_initial_tools_empty_messages() {
        let messages = vec![];
        let tool_messages = vec![
            create_assistant_message_with_tool_calls(vec![create_list_datasets_tool_call()]),
            create_tool_message("test_id", "dataset1, dataset2"),
        ];

        let result = insert_initial_tools(messages, "list_datasets", &tool_messages);

        insta::assert_json_snapshot!(result, {
            "[].Assistant.tool_calls[].id" => "[tool_call_id]",
            "[].Tool.tool_call_id" => "[tool_call_id]"
        });
    }

    #[test]
    fn test_insert_initial_tools_with_system_and_user_messages() {
        let messages = vec![
            create_system_message("You are a helpful assistant"),
            create_user_message("Hello"),
        ];
        let tool_messages = vec![
            create_assistant_message_with_tool_calls(vec![create_list_datasets_tool_call()]),
            create_tool_message("test_id", "dataset1, dataset2"),
        ];

        let result = insert_initial_tools(messages, "list_datasets", &tool_messages);

        insta::assert_json_snapshot!(result, {
            "[].Assistant.tool_calls[].id" => "[tool_call_id]",
            "[].Tool.tool_call_id" => "[tool_call_id]"
        });
    }

    #[test]
    fn test_insert_initial_tools_with_existing_assistant_message() {
        let existing_tool_call = ChatCompletionMessageToolCall {
            id: "existing_id".to_string(),
            function: FunctionCall {
                name: "other_tool".to_string(),
                arguments: "{}".to_string(),
            },
        };

        let messages = vec![
            create_system_message("You are a helpful assistant"),
            create_user_message("Hello"),
            create_assistant_message_with_tool_calls(vec![existing_tool_call]),
        ];
        let tool_messages = vec![
            create_assistant_message_with_tool_calls(vec![create_list_datasets_tool_call()]),
            create_tool_message("test_id", "dataset1, dataset2"),
        ];

        let result = insert_initial_tools(messages, "list_datasets", &tool_messages);

        insta::assert_json_snapshot!(result, {
            "[].Assistant.tool_calls[].id" => "[tool_call_id]",
            "[].Tool.tool_call_id" => "[tool_call_id]"
        });
    }

    #[test]
    fn test_insert_initial_tools_skips_if_tool_already_exists() {
        let existing_list_datasets_call = create_list_datasets_tool_call();
        let messages = vec![
            create_system_message("You are a helpful assistant"),
            create_user_message("Hello"),
            create_assistant_message_with_tool_calls(vec![existing_list_datasets_call]),
        ];
        let tool_messages = vec![
            create_assistant_message_with_tool_calls(vec![create_list_datasets_tool_call()]),
            create_tool_message("test_id", "dataset1, dataset2"),
        ];

        let result = insert_initial_tools(messages, "list_datasets", &tool_messages);

        insta::assert_json_snapshot!(result, {
            "[].Assistant.tool_calls[].id" => "[tool_call_id]"
        });
    }

    #[test]
    fn test_insert_initial_tools_with_different_tool_name() {
        let existing_tool_call = ChatCompletionMessageToolCall {
            id: "other_id".to_string(),
            function: FunctionCall {
                name: "other_tool".to_string(),
                arguments: "{}".to_string(),
            },
        };

        let messages = vec![
            create_system_message("You are a helpful assistant"),
            create_assistant_message_with_tool_calls(vec![existing_tool_call]),
        ];
        let tool_messages = vec![
            create_assistant_message_with_tool_calls(vec![create_list_datasets_tool_call()]),
            create_tool_message("test_id", "dataset1, dataset2"),
        ];

        let result = insert_initial_tools(messages, "list_datasets", &tool_messages);

        insta::assert_json_snapshot!(result, {
            "[].Assistant.tool_calls[].id" => "[tool_call_id]",
            "[].Tool.tool_call_id" => "[tool_call_id]"
        });
    }

    #[test]
    fn test_insert_initial_tools_insertion_point_end_of_messages() {
        let messages = vec![
            create_system_message("You are a helpful assistant"),
            create_user_message("What datasets are available?"),
            create_user_message("And what about tables?"),
        ];
        let tool_messages = vec![
            create_assistant_message_with_tool_calls(vec![create_list_datasets_tool_call()]),
            create_tool_message("test_id", "dataset1, dataset2"),
        ];

        let result = insert_initial_tools(messages, "list_datasets", &tool_messages);

        insta::assert_json_snapshot!(result, {
            "[].Assistant.tool_calls[].id" => "[tool_call_id]",
            "[].Tool.tool_call_id" => "[tool_call_id]"
        });
    }

    fn allowed_tools(mode: ToolChoiceAllowedMode) -> ChatCompletionToolChoiceOption {
        ChatCompletionToolChoiceOption::AllowedTools(ChatCompletionAllowedToolsChoice {
            allowed_tools: ChatCompletionAllowedTools {
                mode,
                tools: vec![serde_json::json!({
                    "type": "function",
                    "function": { "name": "list_datasets" }
                })],
            },
        })
    }

    // regression test for #14459
    #[test]
    fn test_next_round_tool_choice() {
        let auto = ChatCompletionToolChoiceOption::Mode(ToolChoiceOptions::Auto);
        for forced in [
            None,
            Some(ChatCompletionToolChoiceOption::Mode(
                ToolChoiceOptions::Required,
            )),
            Some(ChatCompletionToolChoiceOption::Function(
                "list_datasets".into(),
            )),
            Some(ChatCompletionToolChoiceOption::Custom(
                ChatCompletionNamedToolChoiceCustom {
                    custom: CustomName {
                        name: "client_tool".to_string(),
                    },
                },
            )),
        ] {
            assert_eq!(next_round_tool_choice(forced.clone()), auto, "{forced:?}");
        }

        // `allowed_tools` keeps its tool list, in `auto` mode.
        assert_eq!(
            next_round_tool_choice(Some(allowed_tools(ToolChoiceAllowedMode::Required))),
            allowed_tools(ToolChoiceAllowedMode::Auto)
        );

        // Choices that force nothing pass through unchanged.
        for unforced in [
            ChatCompletionToolChoiceOption::Mode(ToolChoiceOptions::Auto),
            ChatCompletionToolChoiceOption::Mode(ToolChoiceOptions::None),
            allowed_tools(ToolChoiceAllowedMode::Auto),
        ] {
            assert_eq!(next_round_tool_choice(Some(unforced.clone())), unforced);
        }
    }

    fn create_assistant_message(content: &str) -> ChatCompletionRequestMessage {
        ChatCompletionRequestAssistantMessageArgs::default()
            .content(content)
            .build()
            .expect("couldn't create assistant message")
            .into()
    }

    fn user(text: &str) -> ChatCompletionRequestMessage {
        create_user_message(text)
    }

    fn assistant(text: &str) -> ChatCompletionRequestMessage {
        create_assistant_message(text)
    }

    /// A Spice `sql` tool that answers every call with one row.
    struct Sql;

    #[async_trait]
    impl SpiceModelTool for Sql {
        fn name(&self) -> Cow<'_, str> {
            "sql".into()
        }

        fn description(&self) -> Option<Cow<'_, str>> {
            None
        }

        fn parameters(&self) -> Option<Value> {
            None
        }

        async fn call(
            &self,
            _arg: &str,
        ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
            Ok(json!([{ "answer": 42 }]))
        }
    }

    /// A Spice `list_datasets` tool whose `n`th call answers `["dataset_n"]`.
    struct ListDatasets(AtomicUsize);

    #[async_trait]
    impl SpiceModelTool for ListDatasets {
        fn name(&self) -> Cow<'_, str> {
            "list_datasets".into()
        }

        fn description(&self) -> Option<Cow<'_, str>> {
            None
        }

        fn parameters(&self) -> Option<Value> {
            None
        }

        async fn call(
            &self,
            _arg: &str,
        ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
            let call = self.0.fetch_add(1, Ordering::SeqCst) + 1;
            Ok(json!([format!("dataset_{call}")]))
        }
    }

    /// What the scripted provider does with one request.
    #[derive(Clone, Copy)]
    enum Reply {
        /// Call the Spice `sql` tool, with the call id `call_<request index>`.
        CallSql,
        /// Answer with this text.
        Answer(&'static str),
        /// Fail: a request returns an error, and a stream fails after some text.
        Fail,
    }

    /// A provider that answers request `n` as `replies[n]` says, and records every request.
    struct ScriptedChat {
        replies: Vec<Reply>,
        requests: parking_lot::Mutex<Vec<CreateChatCompletionRequest>>,
    }

    impl ScriptedChat {
        fn new(replies: Vec<Reply>) -> Arc<Self> {
            Arc::new(Self {
                replies,
                requests: parking_lot::Mutex::new(Vec::new()),
            })
        }

        /// Records `req`, and returns its index and the reply to it.
        fn answer_to(&self, req: CreateChatCompletionRequest) -> (usize, Reply) {
            let mut requests = self.requests.lock();
            requests.push(req);
            let index = requests.len() - 1;
            let reply = self
                .replies
                .get(index)
                .copied()
                .expect("a scripted reply for every request");
            (index, reply)
        }

        /// The messages of request `index`, summarized.
        fn messages(&self, index: usize) -> Vec<String> {
            summary(&self.requests.lock()[index].messages)
        }
    }

    fn sql_call(index: usize) -> Value {
        json!({
            "id": format!("call_{index}"), "type": "function",
            "function": { "name": "sql", "arguments": "{}" },
        })
    }

    fn scripted_failure() -> OpenAIError {
        OpenAIError::InvalidArgument("scripted failure".to_string())
    }

    fn stream_chunk(
        index: usize,
        delta: &Value,
        finish_reason: Option<&str>,
    ) -> CreateChatCompletionStreamResponse {
        serde_json::from_value(json!({
            "id": format!("chatcmpl-{index}"), "object": "chat.completion.chunk", "created": 0,
            "model": "scripted",
            "choices": [{ "index": 0, "delta": delta, "finish_reason": finish_reason }],
        }))
        .expect("a valid chunk")
    }

    #[async_trait]
    impl Chat for ScriptedChat {
        fn as_sql(&self) -> Option<&dyn SqlGeneration> {
            None
        }

        async fn chat_request(
            &self,
            req: CreateChatCompletionRequest,
        ) -> Result<CreateChatCompletionResponse, OpenAIError> {
            let (index, reply) = self.answer_to(req);
            let (message, finish_reason) = match reply {
                Reply::CallSql => (
                    json!({ "role": "assistant", "tool_calls": [sql_call(index)] }),
                    "tool_calls",
                ),
                Reply::Answer(text) => (json!({ "role": "assistant", "content": text }), "stop"),
                Reply::Fail => return Err(scripted_failure()),
            };
            Ok(serde_json::from_value(json!({
                "id": format!("chatcmpl-{index}"), "object": "chat.completion", "created": 0,
                "model": "scripted",
                "choices": [{ "index": 0, "message": message, "finish_reason": finish_reason }],
            }))
            .expect("a valid response"))
        }

        async fn chat_stream(
            &self,
            req: CreateChatCompletionRequest,
        ) -> Result<ChatCompletionResponseStream, OpenAIError> {
            let (index, reply) = self.answer_to(req);
            let chunks = match reply {
                Reply::CallSql => {
                    let mut call = sql_call(index);
                    call["index"] = json!(0);
                    vec![
                        Ok(stream_chunk(
                            index,
                            &json!({ "role": "assistant", "tool_calls": [call] }),
                            None,
                        )),
                        Ok(stream_chunk(index, &json!({}), Some("tool_calls"))),
                    ]
                }
                Reply::Answer(text) => vec![
                    Ok(stream_chunk(
                        index,
                        &json!({ "role": "assistant", "content": text }),
                        None,
                    )),
                    Ok(stream_chunk(index, &json!({}), Some("stop"))),
                ],
                Reply::Fail => vec![
                    Ok(stream_chunk(
                        index,
                        &json!({ "role": "assistant", "content": "partial" }),
                        None,
                    )),
                    Err(scripted_failure()),
                ],
            };
            Ok(Box::pin(futures::stream::iter(chunks)))
        }
    }

    /// Each message as `role: text`, an assistant's tool calls as `assistant calls name#id`, and
    /// a tool result as `tool id: text`.
    fn summary(messages: &[ChatCompletionRequestMessage]) -> Vec<String> {
        messages
            .iter()
            .map(|message| {
                let message = serde_json::to_value(message).expect("a serializable message");
                let role = message["role"].as_str().unwrap_or_default();
                let content = message["content"].as_str().unwrap_or_default();
                if let Some(calls) = message["tool_calls"].as_array() {
                    let calls = calls
                        .iter()
                        .map(|call| {
                            format!(
                                "{}#{}",
                                call["function"]["name"].as_str().unwrap_or_default(),
                                call["id"].as_str().unwrap_or_default()
                            )
                        })
                        .join(", ");
                    format!("{role} calls {calls}")
                } else if let Some(id) = message["tool_call_id"].as_str() {
                    format!("{role} {id}: {content}")
                } else {
                    format!("{role}: {content}")
                }
            })
            .collect()
    }

    /// The steps of the round that called `sql` as request 0, summarized.
    const CALL_0: &str = "assistant calls sql#call_0";
    const RESULT_0: &str = r#"tool call_0: [{"answer":42}]"#;
    /// The steps of the round that called `sql` as request 2, summarized.
    const CALL_2: &str = "assistant calls sql#call_2";
    const RESULT_2: &str = r#"tool call_2: [{"answer":42}]"#;
    const LIST_DATASETS: &str = "assistant calls list_datasets#initial_list_datasets";

    fn model(provider: &Arc<ScriptedChat>, tools: Vec<Arc<dyn SpiceModelTool>>) -> ToolUsingChat {
        ToolUsingChat::new(Arc::clone(provider) as Arc<dyn Chat>, tools, None)
    }

    fn sql_model(provider: &Arc<ScriptedChat>) -> ToolUsingChat {
        model(provider, vec![Arc::new(Sql) as Arc<dyn SpiceModelTool>])
    }

    fn request(messages: Vec<ChatCompletionRequestMessage>) -> CreateChatCompletionRequest {
        CreateChatCompletionRequest {
            model: "scripted".to_string(),
            messages,
            ..Default::default()
        }
    }

    /// Sends `messages` to `model` as one turn, and returns the answer's text.
    async fn turn(
        model: &ToolUsingChat,
        messages: Vec<ChatCompletionRequestMessage>,
    ) -> Result<String, OpenAIError> {
        let response = model.chat_request(request(messages)).await?;
        Ok(response
            .choices
            .first()
            .and_then(|choice| choice.message.content.clone())
            .unwrap_or_default())
    }

    /// Streams `messages` through `model` as one turn, to the end of the stream, and returns the
    /// text the client receives, or the first error.
    async fn streamed_turn(
        model: &ToolUsingChat,
        messages: Vec<ChatCompletionRequestMessage>,
    ) -> Result<String, OpenAIError> {
        let chunks: Vec<_> = model.chat_stream(request(messages)).await?.collect().await;
        let mut text = String::new();
        for chunk in chunks {
            for choice in chunk?.choices {
                text.push_str(choice.delta.content.as_deref().unwrap_or_default());
            }
        }
        Ok(text)
    }

    /// A request context, authenticated with `api_key` if there is one.
    fn caller(api_key: Option<&str>) -> Arc<RequestContext> {
        let context = Arc::new(RequestContext::builder(Protocol::Http).build());
        if let Some(key) = api_key {
            context
                .set_auth_principal(Arc::new(ApiKey::ReadOnly {
                    key: key.to_string(),
                }) as AuthPrincipalRef)
                .expect("auth principal should be set");
        }
        context
    }

    #[tokio::test]
    async fn test_spice_tool_steps_reach_the_model_on_the_next_turn() {
        let provider =
            ScriptedChat::new(vec![Reply::CallSql, Reply::Answer("D"), Reply::Answer("F")]);
        let model = sql_model(&provider);

        let answer = turn(&model, vec![user("A")]).await.expect("turn 1");
        assert_eq!(answer, "D");
        assert_eq!(provider.messages(1), ["user: A", CALL_0, RESULT_0]);

        turn(&model, vec![user("A"), assistant(&answer), user("E")])
            .await
            .expect("turn 2");
        assert_eq!(
            provider.messages(2),
            ["user: A", CALL_0, RESULT_0, "assistant: D", "user: E"]
        );
    }

    #[tokio::test]
    async fn test_every_earlier_turns_steps_reach_the_model_in_place() {
        let provider = ScriptedChat::new(vec![
            Reply::CallSql,
            Reply::Answer("D"),
            Reply::CallSql,
            Reply::Answer("F"),
            Reply::Answer("H"),
        ]);
        let model = sql_model(&provider);

        turn(&model, vec![user("A")]).await.expect("turn 1");
        turn(&model, vec![user("A"), assistant("D"), user("E")])
            .await
            .expect("turn 2");
        turn(
            &model,
            vec![
                user("A"),
                assistant("D"),
                user("E"),
                assistant("F"),
                user("G"),
            ],
        )
        .await
        .expect("turn 3");

        assert_eq!(
            provider.messages(4),
            [
                "user: A",
                CALL_0,
                RESULT_0,
                "assistant: D",
                "user: E",
                CALL_2,
                RESULT_2,
                "assistant: F",
                "user: G"
            ]
        );
    }

    #[tokio::test]
    async fn test_a_turn_without_tools_carries_earlier_steps_forward() {
        let provider = ScriptedChat::new(vec![
            Reply::CallSql,
            Reply::Answer("D"),
            Reply::Answer("F"),
            Reply::Answer("H"),
        ]);
        let model = sql_model(&provider);

        turn(&model, vec![user("A")]).await.expect("turn 1");
        turn(&model, vec![user("A"), assistant("D"), user("E")])
            .await
            .expect("turn 2");
        turn(
            &model,
            vec![
                user("A"),
                assistant("D"),
                user("E"),
                assistant("F"),
                user("G"),
            ],
        )
        .await
        .expect("turn 3");

        assert_eq!(
            provider.messages(3),
            [
                "user: A",
                CALL_0,
                RESULT_0,
                "assistant: D",
                "user: E",
                "assistant: F",
                "user: G"
            ]
        );
    }

    #[tokio::test]
    async fn test_steps_are_restored_only_for_the_caller_that_ran_them() {
        let provider = ScriptedChat::new(vec![
            Reply::CallSql,
            Reply::Answer("D"),
            Reply::Answer("F"),
            Reply::Answer("F"),
            Reply::Answer("F"),
        ]);
        let model = sql_model(&provider);
        let turn_2 = || vec![user("A"), assistant("D"), user("E")];

        caller(Some("key-1"))
            .scope(turn(&model, vec![user("A")]))
            .await
            .expect("turn 1");

        caller(Some("key-2"))
            .scope(turn(&model, turn_2()))
            .await
            .expect("another caller's turn 2");
        assert_eq!(provider.messages(2), ["user: A", "assistant: D", "user: E"]);

        caller(None)
            .scope(turn(&model, turn_2()))
            .await
            .expect("an anonymous turn 2");
        assert_eq!(provider.messages(3), ["user: A", "assistant: D", "user: E"]);

        caller(Some("key-1"))
            .scope(turn(&model, turn_2()))
            .await
            .expect("turn 2");
        assert_eq!(
            provider.messages(4),
            ["user: A", CALL_0, RESULT_0, "assistant: D", "user: E"]
        );
    }

    #[tokio::test]
    async fn test_an_edited_answer_restores_nothing() {
        let provider =
            ScriptedChat::new(vec![Reply::CallSql, Reply::Answer("D"), Reply::Answer("F")]);
        let model = sql_model(&provider);

        turn(&model, vec![user("A")]).await.expect("turn 1");
        turn(&model, vec![user("A"), assistant("D, edited"), user("E")])
            .await
            .expect("turn 2");
        assert_eq!(
            provider.messages(2),
            ["user: A", "assistant: D, edited", "user: E"]
        );
    }

    /// Known limitation: the entry a turn restores from is removed once that turn succeeds, so
    /// sending the same turn again (to regenerate its answer, say) restores nothing.
    #[tokio::test]
    async fn test_sending_a_turn_again_after_it_succeeded_restores_nothing() {
        let provider = ScriptedChat::new(vec![
            Reply::CallSql,
            Reply::Answer("D"),
            Reply::Answer("F"),
            Reply::Answer("F"),
        ]);
        let model = sql_model(&provider);
        let turn_2 = || vec![user("A"), assistant("D"), user("E")];

        turn(&model, vec![user("A")]).await.expect("turn 1");
        turn(&model, turn_2()).await.expect("turn 2");
        assert_eq!(
            provider.messages(2),
            ["user: A", CALL_0, RESULT_0, "assistant: D", "user: E"]
        );

        turn(&model, turn_2()).await.expect("turn 2 again");
        assert_eq!(provider.messages(3), ["user: A", "assistant: D", "user: E"]);
    }

    #[tokio::test]
    async fn test_a_failed_turn_keeps_the_steps_for_a_retry() {
        let provider = ScriptedChat::new(vec![
            Reply::CallSql,
            Reply::Answer("D"),
            Reply::Fail,
            Reply::Answer("F"),
        ]);
        let model = sql_model(&provider);
        let turn_2 = || vec![user("A"), assistant("D"), user("E")];

        turn(&model, vec![user("A")]).await.expect("turn 1");
        turn(&model, turn_2())
            .await
            .expect_err("the provider fails turn 2");
        turn(&model, turn_2()).await.expect("turn 2, retried");
        assert_eq!(
            provider.messages(3),
            ["user: A", CALL_0, RESULT_0, "assistant: D", "user: E"]
        );
    }

    #[tokio::test]
    async fn test_streamed_spice_tool_steps_reach_the_model_on_the_next_turn() {
        let provider =
            ScriptedChat::new(vec![Reply::CallSql, Reply::Answer("D"), Reply::Answer("F")]);
        let model = sql_model(&provider);

        let answer = streamed_turn(&model, vec![user("A")])
            .await
            .expect("turn 1");
        assert_eq!(answer, "D");
        assert_eq!(provider.messages(1), ["user: A", CALL_0, RESULT_0]);

        let answer = streamed_turn(&model, vec![user("A"), assistant(&answer), user("E")])
            .await
            .expect("turn 2");
        assert_eq!(answer, "F");
        assert_eq!(
            provider.messages(2),
            ["user: A", CALL_0, RESULT_0, "assistant: D", "user: E"]
        );
    }

    #[tokio::test]
    async fn test_a_failed_streamed_turn_keeps_the_steps_for_a_retry() {
        let provider = ScriptedChat::new(vec![
            Reply::CallSql,
            Reply::Answer("D"),
            Reply::Fail,
            Reply::Answer("F"),
        ]);
        let model = sql_model(&provider);
        let turn_2 = || vec![user("A"), assistant("D"), user("E")];

        streamed_turn(&model, vec![user("A")])
            .await
            .expect("turn 1");
        streamed_turn(&model, turn_2())
            .await
            .expect_err("the provider fails turn 2");
        streamed_turn(&model, turn_2())
            .await
            .expect("turn 2, retried");
        assert_eq!(
            provider.messages(3),
            ["user: A", CALL_0, RESULT_0, "assistant: D", "user: E"]
        );
    }

    #[tokio::test]
    async fn test_the_initial_list_datasets_steps_are_never_stored() {
        let provider =
            ScriptedChat::new(vec![Reply::CallSql, Reply::Answer("D"), Reply::Answer("F")]);
        let model = model(
            &provider,
            vec![
                Arc::new(Sql) as Arc<dyn SpiceModelTool>,
                Arc::new(ListDatasets(AtomicUsize::new(0))) as Arc<dyn SpiceModelTool>,
            ],
        );

        turn(&model, vec![user("A")]).await.expect("turn 1");
        assert_eq!(
            provider.messages(1),
            [
                "user: A",
                LIST_DATASETS,
                r#"tool initial_list_datasets: ["dataset_1"]"#,
                CALL_0,
                RESULT_0
            ]
        );

        // Turn 2 gets new `list_datasets` steps, not a stored copy of turn 1's.
        turn(&model, vec![user("A"), assistant("D"), user("E")])
            .await
            .expect("turn 2");
        assert_eq!(
            provider.messages(2),
            [
                "user: A",
                LIST_DATASETS,
                r#"tool initial_list_datasets: ["dataset_2"]"#,
                CALL_0,
                RESULT_0,
                "assistant: D",
                "user: E"
            ]
        );
    }
}
