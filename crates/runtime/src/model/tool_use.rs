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
use async_openai::types::{
    ChatChoiceStream, ChatCompletionMessageToolCall, ChatCompletionRequestAssistantMessageArgs,
    ChatCompletionRequestMessage, ChatCompletionRequestToolMessageArgs,
    ChatCompletionResponseStream, ChatCompletionTool, ChatCompletionToolChoiceOption,
    ChatCompletionToolType, CompletionTokensDetails, CompletionUsage, CreateChatCompletionRequest,
    CreateChatCompletionResponse, CreateChatCompletionStreamResponse, FinishReason, FunctionCall,
    FunctionObject, PromptTokensDetails,
};

use async_trait::async_trait;
use futures::{Stream, StreamExt};
use serde_json::Value;

use tokio::sync::mpsc;
use tracing::{Instrument, Span};

use crate::request::{AsyncMarker, RequestContext};
use crate::tools::builtin::list_datasets::ListDatasetsTool;
use crate::tools::SpiceModelTool;
use crate::Runtime;

pub struct ToolUsingChat {
    inner_chat: Arc<Box<dyn Chat>>,
    rt: Arc<Runtime>,
    tools: Vec<Arc<dyn SpiceModelTool>>,
    recursion_limit: Option<usize>,
}

impl ToolUsingChat {
    #[must_use]
    pub fn new(
        inner_chat: Arc<Box<dyn Chat>>,
        rt: Arc<Runtime>,
        tools: Vec<Arc<dyn SpiceModelTool>>,
        recursion_limit: Option<usize>,
    ) -> Self {
        Self {
            inner_chat,
            rt,
            tools,
            recursion_limit,
        }
    }

    #[must_use]
    pub fn runtime_tools(&self) -> Vec<ChatCompletionTool> {
        self.tools
            .iter()
            .map(|t| ChatCompletionTool {
                r#type: ChatCompletionToolType::Function,
                function: FunctionObject {
                    strict: t.strict(),
                    name: t.name().to_string(),
                    description: t.description().map(|d| d.to_string()),
                    parameters: t.parameters(),
                },
            })
            .collect_vec()
    }

    #[must_use]
    pub fn tool_exists(&self, name: &str) -> bool {
        self.tools.iter().any(|t| t.name() == name)
    }

    /// Create a new [`CreateChatCompletionRequest`] with the system prompt injected as the first message.
    async fn prepare_req(
        &self,
        mut req: CreateChatCompletionRequest,
    ) -> Result<CreateChatCompletionRequest, OpenAIError> {
        if self.tool_exists("list_datasets") {
            // Add messages to start of message list to pretend it has already asked to list the available datasets.
            let mut list_dataset_messages = self.create_list_dataset_messages().await?;
            list_dataset_messages.extend_from_slice(req.messages.as_slice());
            req.messages = list_dataset_messages;
        };

        Ok(req)
    }

    /// Create the messagges expected from a model if it has called the `list_datasets` tool, and recieved a response.
    /// This is useful to prime the model as if it has already asked to list the available datasets.
    async fn create_list_dataset_messages(
        &self,
    ) -> Result<Vec<ChatCompletionRequestMessage>, OpenAIError> {
        let t = ListDatasetsTool::default();
        let t_resp = t
            .call("", Arc::<Runtime>::clone(&self.rt))
            .await
            .map_err(|e| OpenAIError::InvalidArgument(e.to_string()))?;
        Ok(vec![
            ChatCompletionRequestAssistantMessageArgs::default()
                .tool_calls(vec![ChatCompletionMessageToolCall {
                    id: "initial_list_datasets".to_string(),
                    r#type: ChatCompletionToolType::Function,
                    function: FunctionCall {
                        name: t.name().to_string(),
                        arguments: String::new(),
                    },
                }])
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
    fn is_spiced_tool(&self, t: &ChatCompletionMessageToolCall) -> bool {
        self.tools.iter().any(|tool| tool.name() == t.function.name)
    }

    /// Call a spiced runtime tool.
    ///
    /// Return the result as a JSON value.
    async fn call_tool(&self, func: &FunctionCall) -> Value {
        match self.tools.iter().find(|t| t.name() == func.name) {
            Some(t) => {
                match t
                    .call(&func.arguments, Arc::<Runtime>::clone(&self.rt))
                    .await
                {
                    Ok(v) => v,
                    Err(e) => Value::String(format!(
                        "Failed to call the tool {}.\nAn error occurred: {e}",
                        t.name()
                    )),
                }
            }
            None => Value::Null,
        }
    }

    /// For `requested_tools` requested from processing `original_messages` through a model, check
    /// if any are spiced runtime tools, and if so, run them locally and create new messages to be
    ///  reprocessed by the model.
    ///
    /// Returns
    /// - `None` if no spiced runtime tools were used. Note: external tools may still have been
    ///     requested.
    /// - `Some(messages)` if spiced runtime tools were used. The returned messages are ready to be
    ///     reprocessed by the model.
    async fn process_tool_calls_and_run_spice_tools(
        &self,
        original_messages: Vec<ChatCompletionRequestMessage>,
        requested_tools: Vec<ChatCompletionMessageToolCall>,
    ) -> Result<Option<Vec<ChatCompletionRequestMessage>>, OpenAIError> {
        let spiced_tools = requested_tools
            .iter()
            .filter(|&t| self.is_spiced_tool(t))
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
                .tool_calls(spiced_tools.clone()) // TODO - should this include non-spiced tools?
                .build()?
                .into();

        let mut tool_and_response_content = vec![];
        for t in spiced_tools {
            let content = self.call_tool(&t.function).await;
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

        let mut messages = original_messages.clone();
        messages.push(assistant_message);
        messages.extend(tool_messages);

        Ok(Some(messages))
    }

    async fn chat_request_inner(
        &self,
        req: CreateChatCompletionRequest,
        recursion_limit: Option<usize>,
    ) -> Result<CreateChatCompletionResponse, OpenAIError> {
        Box::pin(async move {
            // Don't use spice runtime tools if users has explicitly chosen to not use any tools.
            if req
                .tool_choice
                .as_ref()
                .is_some_and(|c| *c == ChatCompletionToolChoiceOption::None)
            {
                tracing::debug!("User asked for no tools, calling inner chat model");
                return self.inner_chat.chat_request(req).await;
            };

            if recursion_limit.is_some_and(|f| f == 0) {
                tracing::debug!(
                    "Tool-use recursion limit reached. Will call model, but not process further"
                );
                return self.inner_chat.chat_request(req).await;
            };

            // Append spiced runtime tools to the request.
            let inner_req = self.add_runtime_tools(&req);

            let resp = self.inner_chat.chat_request(inner_req.clone()).await?;
            let proceed_with_tools = resp.choices.first().is_some_and(|c| {
                c.finish_reason
                    .is_some_and(|f| matches!(f, FinishReason::ToolCalls))
            });

            // Return reason was not to call tools, so return early.
            if !proceed_with_tools {
                return Ok(resp);
            }

            let usage = resp.usage.clone();

            let tools_used = resp
                .choices
                .first()
                .and_then(|c| c.message.tool_calls.clone());

            match self
                .process_tool_calls_and_run_spice_tools(
                    req.messages,
                    tools_used.unwrap_or_default(),
                )
                .await?
            {
                // New messages means we have run spice tools locally, ready to recall model.
                Some(messages) => {
                    let mut resp = self
                        .chat_request_inner(
                            create_new_recursive_req(&inner_req, messages, resp.usage.as_ref()),
                            recursion_limit.map(|r| r - 1),
                        )
                        .await?;
                    resp.usage = combine_usage(usage, resp.usage);
                    Ok(resp)
                }
                None => Ok(resp),
            }
        })
        .await
    }

    /// Add the spice runtime tools to a list of tools (may contain external tools too), and ensure no duplicates.
    fn add_runtime_tools(&self, req: &CreateChatCompletionRequest) -> CreateChatCompletionRequest {
        let mut runtime_tools = self.runtime_tools();
        if runtime_tools.is_empty() {
            req.clone()
        } else {
            runtime_tools.extend(req.tools.clone().unwrap_or_default());
            // Ensure function names are unique. Tool-use recursion sometimes creates duplicates.
            runtime_tools.sort_by(|a, b| a.function.name.cmp(&b.function.name));
            runtime_tools.dedup_by(|a, b| a.function.name == b.function.name);
            let mut req = req.clone();
            req.tools = Some(runtime_tools);
            req
        }
    }

    async fn chat_stream_inner(
        &self,
        req: CreateChatCompletionRequest,
    ) -> Result<ChatCompletionResponseStream, OpenAIError> {
        // Don't use spice runtime tools if users has explicitly chosen to not use any tools.
        if req
            .tool_choice
            .as_ref()
            .is_some_and(|c| *c == ChatCompletionToolChoiceOption::None)
        {
            return self.inner_chat.chat_stream(req).await;
        };

        if self.recursion_limit.is_some_and(|f| f == 0) {
            tracing::debug!(
                "Tool-use recursion limit reached. Will call model, but not process further"
            );
            return self.inner_chat.chat_stream(req).await;
        };

        // Append spiced runtime tools to the request. Avoid clone if no runtime tools.
        let updated_req = self.add_runtime_tools(&req);
        let s = self.inner_chat.chat_stream(updated_req.clone()).await?;

        Ok(make_a_stream(
            Span::current(),
            RequestContext::current(AsyncMarker::new().await),
            Self::new(
                Arc::clone(&self.inner_chat),
                Arc::clone(&self.rt),
                self.tools.clone(),
                self.recursion_limit.map(|r| r - 1),
            ),
            req,
            s,
        ))
    }
}

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
        req: CreateChatCompletionRequest,
    ) -> Result<ChatCompletionResponseStream, OpenAIError> {
        let inner_req = self.prepare_req(req).await?;
        self.chat_stream_inner(inner_req).await
    }

    async fn chat_request(
        &self,
        req: CreateChatCompletionRequest,
    ) -> Result<CreateChatCompletionResponse, OpenAIError> {
        let inner_req = self.prepare_req(req).await?;
        self.chat_request_inner(inner_req, self.recursion_limit)
            .await
    }

    fn as_sql(&self) -> Option<&dyn SqlGeneration> {
        self.inner_chat.as_sql()
    }

    /// Override health endpoint to 1. avoid passing tools in request, 2. pre-calling `list_datasets` in [`ToolUsingChat::prepare_req`].
    async fn health(&self) -> ChatResult<()> {
        self.inner_chat.health().await
    }
}

/// Create a new [`CreateChatCompletionRequest`] with new messages.
///
/// Remove `tool_choice` if it is named (since it was just used), and set it to `Auto`.
fn create_new_recursive_req(
    req: &CreateChatCompletionRequest,
    new_msg: Vec<ChatCompletionRequestMessage>,
    marginal_usage: Option<&CompletionUsage>,
) -> CreateChatCompletionRequest {
    let mut new_req = req.clone();
    new_req.messages = new_msg;
    if let Some(ChatCompletionToolChoiceOption::Named(t)) = new_req.tool_choice {
        tracing::debug!("Not recursively using tool_choice named={t:?} in subsequent calls.");
        // Auto is default when tools exist.
        new_req.tool_choice = Some(ChatCompletionToolChoiceOption::Auto);
    }

    // Adjust input `max_completion_tokens` if usage is known to ensure we don't exceed the limit.
    if let Some(max_completion_tokens) = new_req.max_completion_tokens {
        if let Some(usage) = marginal_usage {
            new_req.max_completion_tokens =
                Some(max_completion_tokens.saturating_sub(usage.completion_tokens));
        }
    }

    new_req
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
fn combine_opt_u32(a: Option<u32>, b: Option<u32>) -> Option<u32> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a + b),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    }
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

#[allow(clippy::too_many_lines)]
fn make_a_stream(
    span: Span,
    request_context: Arc<RequestContext>,
    model: ToolUsingChat,
    req: CreateChatCompletionRequest,
    mut s: ChatCompletionResponseStream,
) -> ChatCompletionResponseStream {
    let (sender, receiver) = mpsc::channel(100);
    let sender_clone = sender.clone();

    tokio::spawn(
        request_context
            .scope(async move {
                let tool_call_states: Arc<
                    Mutex<HashMap<(i32, i32), ChatCompletionMessageToolCall>>,
                > = Arc::new(Mutex::new(HashMap::new()));

                let mut chat_output = String::new();

                while let Some(result) = s.next().await {
                    let response = match result {
                        Ok(response) => response,
                        Err(e) => {
                            if let Err(e) = sender_clone.send(Err(e)).await {
                                if !sender_clone.is_closed() {
                                    tracing::error!("Error sending error: {}", e);
                                }
                            }
                            return;
                        }
                    };
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
                                        r#type: ChatCompletionToolType::Function,
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
                        if chat_choice.delta.content.is_some() {
                            finished_choices.push(chat_choice.clone());
                        }

                        // If a tool has finished (i.e. we have all chunks), process them.
                        if let Some(finish_reason) = &chat_choice.finish_reason {
                            if matches!(finish_reason, FinishReason::ToolCalls) {
                                let tool_call_states_clone = Arc::clone(&tool_call_states);

                                let tool_calls_to_process = {
                                    match tool_call_states_clone.lock() {
                                        Ok(states_lock) => states_lock
                                            .iter()
                                            .map(|(_key, tool_call)| tool_call.clone())
                                            .collect::<Vec<_>>(),
                                        Err(e) => {
                                            tracing::error!(
                                                "Failed to lock tool_call_states: {}",
                                                e
                                            );
                                            return;
                                        }
                                    }
                                };

                                let new_messages = match model
                                    .process_tool_calls_and_run_spice_tools(
                                        req.messages.clone(),
                                        tool_calls_to_process,
                                    )
                                    .await
                                {
                                    Ok(Some(messages)) => messages,
                                    Ok(None) => {
                                        // No spice tools within returned tools, so return as message in stream.
                                        finished_choices.push(chat_choice);
                                        continue;
                                    }
                                    Err(e) => {
                                        if let Err(e) = sender_clone.send(Err(e)).await {
                                            if !sender_clone.is_closed() {
                                                tracing::error!("Error sending error: {}", e);
                                            }
                                        }
                                        return;
                                    }
                                };

                                match model
                                    .chat_stream_inner(create_new_recursive_req(
                                        &req,
                                        new_messages,
                                        response.usage.as_ref(),
                                    ))
                                    .await
                                {
                                    Ok(mut s) => {
                                        while let Some(resp) = s.next().await {
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
                                        if let Err(e) = sender_clone.send(Err(e)).await {
                                            if !sender_clone.is_closed() {
                                                tracing::error!("Error sending error: {}", e);
                                            }
                                        }
                                        return;
                                    }
                                };
                            } else if matches!(finish_reason, FinishReason::Stop)
                                || matches!(finish_reason, FinishReason::Length)
                            {
                                // If complete, return to stream original.
                                finished_choices.push(chat_choice.clone());
                            }
                        }
                    }

                    if let Some(choice) = finished_choices.first() {
                        if let Some(intermediate_chat_output) = &choice.delta.content {
                            chat_output.push_str(intermediate_chat_output);
                        }

                        let mut resp2 = response.clone();
                        resp2.choices = finished_choices;
                        if let Err(e) = sender_clone.send(Ok(resp2)).await {
                            if !sender_clone.is_closed() {
                                tracing::error!("Error sending error: {}", e);
                            }
                        }
                    }

                    // When there are no [`ChatChoiceStream`]s, but the model has usage, send the response (with no choices).
                    if response.choices.is_empty() && response.usage.is_some() {
                        if let Err(e) = sender_clone.send(Ok(response)).await {
                            if !sender_clone.is_closed() {
                                tracing::error!("Error sending error: {}", e);
                            }
                        }
                    }
                }

                tracing::info!(target: "task_history", captured_output = %chat_output);
            })
            .instrument(span),
    );
    Box::pin(CustomStream { receiver }) as ChatCompletionResponseStream
}
