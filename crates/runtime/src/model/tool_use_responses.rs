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

use async_openai::{
    error::OpenAIError,
    types::{
        CompletionTokensDetails, PromptTokensDetails,
        responses::{
            CodeInterpreter, CodeInterpreterContainer, CodeInterpreterContainerKind,
            CreateResponse, Function, FunctionCall, Input, InputContent, InputItem, InputMessage,
            InputMessageType, OutputContent, OutputItem, Response, ResponseEvent, ResponseStream,
            Role, ToolChoice, ToolChoiceMode, ToolDefinition, Usage, WebSearchPreview,
        },
    },
};
use async_trait::async_trait;
use futures::{Stream, StreamExt};
use itertools::Itertools;
use llms::responses::Error as ResponsesError;
use llms::responses::Responses;
use llms::{chat::Error as LlmError, progress::Progress};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use tokio::sync::mpsc;
use tools::SpiceModelTool;
use tracing::{Instrument, Span};

use crate::{
    model::tool_use::{combine_opt_u32, encode_tool_name},
    request::{AsyncMarker, RequestContext},
};

#[derive(Clone, Debug)]
pub enum OpenAIResponsesTools {
    CodeInterpreter,
    WebSearch,
}

impl From<OpenAIResponsesTools> for ToolDefinition {
    fn from(tool: OpenAIResponsesTools) -> Self {
        match tool {
            OpenAIResponsesTools::CodeInterpreter => {
                ToolDefinition::CodeInterpreter(CodeInterpreter {
                    container: CodeInterpreterContainer::Container(
                        CodeInterpreterContainerKind::Auto { file_ids: None },
                    ),
                })
            }
            OpenAIResponsesTools::WebSearch => ToolDefinition::WebSearchPreview(WebSearchPreview {
                search_context_size: None,
                user_location: None,
            }),
        }
    }
}

impl TryFrom<&str> for OpenAIResponsesTools {
    type Error = LlmError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "code_interpreter" => Ok(OpenAIResponsesTools::CodeInterpreter),
            "web_search" => Ok(OpenAIResponsesTools::WebSearch),
            _ => Err(LlmError::ToolNotFound {
                tool: value.to_string(),
            }),
        }
    }
}

pub struct ToolUsingResponses {
    inner_responses: Arc<dyn Responses>,
    openai_tools: Vec<OpenAIResponsesTools>,
    tools: Vec<Arc<dyn SpiceModelTool>>,
    recursion_limit: Option<usize>,
}

impl ToolUsingResponses {
    #[must_use]
    pub fn new(
        inner_responses: Arc<dyn Responses>,
        openai_tools: Vec<OpenAIResponsesTools>,
        tools: Vec<Arc<dyn SpiceModelTool>>,
        recursion_limit: Option<usize>,
    ) -> Self {
        Self {
            inner_responses,
            openai_tools,
            tools,
            recursion_limit,
        }
    }

    fn prepare_req(&self, mut req: CreateResponse) -> CreateResponse {
        let existing_items = match req.input.clone() {
            Input::Text(input) => vec![InputItem::Message(InputMessage {
                content: InputContent::TextInput(input),
                kind: InputMessageType::default(),
                role: Role::User,
            })],
            Input::Items(items) => items,
        };

        let openai_tool_definitions: Vec<ToolDefinition> = self
            .openai_tools
            .clone()
            .into_iter()
            .map(Into::into)
            .collect();
        req.tools = Some(openai_tool_definitions);

        req.input = Input::Items(existing_items);

        req
    }

    #[must_use]
    pub fn runtime_tools(&self) -> Vec<ToolDefinition> {
        self.tools
            .iter()
            .map(|t| {
                ToolDefinition::Function(Function {
                    strict: t.strict().unwrap_or(false),
                    name: encode_tool_name(t.name().to_string().as_str()),
                    description: t.description().map(|d| d.to_string()),
                    parameters: t
                        .parameters()
                        .map(|mut params| {
                            if let Value::Object(ref mut obj) = params {
                                obj.insert("additionalProperties".to_string(), Value::Bool(false));
                            }
                            params
                        })
                        .unwrap_or(json!({})),
                })
            })
            .collect()
    }

    fn as_spiced_tool(&self, t: &FunctionCall) -> Option<Arc<dyn SpiceModelTool>> {
        self.tools
            .iter()
            .find(|tool| encode_tool_name(tool.name().as_ref()) == t.name)
            .cloned()
    }

    async fn call_tool(&self, tool_call: &FunctionCall) -> Value {
        match self.as_spiced_tool(tool_call) {
            Some(t) => match t.call(&tool_call.arguments).await {
                Ok(v) => {
                    tracing::info!(
                        target: "task_history",
                        progress = Progress::log()
                            .id(tool_call.id.clone())
                            .title(format!("'{}' tool completed successfully", tool_call.name))
                            .json_content(v.clone())
                            .to_jsonl(),
                    );
                    v
                }
                Err(e) => {
                    tracing::info!(
                        target: "task_history",
                        progress = Progress::error()
                            .id(tool_call.id.clone())
                            .title(format!("'{}' tool completed unsuccessfully", tool_call.name))
                            .content(e.to_string())
                            .to_jsonl(),
                    );
                    Value::String(format!(
                        "Failed to call the tool {}. An error occurred: {e}",
                        t.name()
                    ))
                }
            },
            None => {
                // All calls to `call_tool` should have previously checked that `tool_call` has an associated tool.
                if cfg!(feature = "dev") {
                    panic!(
                        "Tool '{}' was provided to LLM, but now no longer exists. This should not be possible.",
                        tool_call.name
                    );
                } else {
                    tracing::warn!(
                        "Tool '{}' was provided to LLM, but now no longer exists. This should not be possible.",
                        tool_call.name
                    );
                    Value::Null
                }
            }
        }
    }

    async fn process_tool_calls_and_run_spice_tools(
        &self,
        original_messages: Vec<InputItem>,
        requested_tools: Vec<FunctionCall>,
    ) -> Result<Option<Vec<InputItem>>, OpenAIError> {
        let spiced_tools = requested_tools
            .iter()
            .filter(|&t| self.as_spiced_tool(t).is_some())
            .cloned()
            .collect_vec();

        // Return early if no spiced runtime tools used.
        if spiced_tools.is_empty() {
            tracing::debug!("No spiced tools used by chat model, returning early");
            return Ok(None);
        }

        let mut tool_and_response_content = vec![];
        for t in spiced_tools.clone() {
            tracing::info!(
                target: "task_history",
                progress = Progress::log()
                    .id(t.id.clone())
                    .title(format!("Calling '{}' tool", t.name))
                    .content(t.arguments.clone())
                    .to_jsonl(),
            );
            let content = self.call_tool(&t).await;
            tool_and_response_content.push((t, content));
        }

        // Tell model the assistant used these tools, and provided result.
        let mut tool_messages: Vec<Value> = vec![];
        for (tool_call, response_content) in &tool_and_response_content {
            // Add the function call
            tool_messages.push(json!({
                "type": "function_call",
                "call_id": tool_call.id.clone(),
                "name": tool_call.name.clone(),
                "arguments": tool_call.arguments.clone()
            }));
            // Add the function call output
            tool_messages.push(json!({
                "type": "function_call_output",
                "call_id": tool_call.id.clone(),
                "output": serde_json::to_string(&response_content).unwrap_or("Error calling tool.".to_string())
            }));
        }

        let mut messages = original_messages.clone();
        messages.extend(tool_messages.into_iter().map(InputItem::Custom));

        if !messages.is_empty() {
            let used_tools = spiced_tools.len();
            if used_tools > 0 {
                let context = RequestContext::current(AsyncMarker::new().await);
                crate::model::add_tools_used(&context, used_tools);
            }
        }

        Ok(Some(messages))
    }

    async fn responses_request_inner(
        &self,
        req: CreateResponse,
        recursion_limit: Option<usize>,
    ) -> Result<Response, OpenAIError> {
        Box::pin(async move {
            // Don't use spice runtime tools if users has explicitly chosen to not use any tools.
            if req
                .tool_choice
                .as_ref()
                .is_some_and(|t| matches!(t, ToolChoice::Mode(ToolChoiceMode::None)))
            {
                tracing::debug!("User asked for no tools, calling inner chat model");
                return self.inner_responses.responses_request(req).await;
            }

            if recursion_limit.is_some_and(|f| f == 0) {
                tracing::debug!(
                    "Tool-use recursion limit reached. Will call model, but not process further"
                );
                return self.inner_responses.responses_request(req).await;
            }

            // Append spiced runtime tools to the request.
            let inner_req = self.add_runtime_tools(&req);

            let resp = self
                .inner_responses
                .responses_request(inner_req.clone())
                .await?;

            let usage = resp.usage.clone();

            let tools_used = resp
                .output
                .iter()
                .cloned()
                .filter_map(|c| match c {
                    OutputContent::FunctionCall(t) => Some(t),
                    _ => None,
                })
                .collect_vec();

            match self
                .process_tool_calls_and_run_spice_tools(to_input_item(req.input), tools_used)
                .await?
            {
                // New messages means we have run spice tools locally, ready to recall model.
                Some(messages) => {
                    let mut resp = self
                        .responses_request_inner(
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

    async fn responses_stream_inner(
        &self,
        req: CreateResponse,
        recursion_limit: Option<usize>,
    ) -> Result<ResponseStream, OpenAIError> {
        Box::pin(async move {
            // Don't use spice runtime tools if users has explicitly chosen to not use any tools.
            if req
                .tool_choice
                .as_ref()
                .is_some_and(|t| matches!(t, ToolChoice::Mode(ToolChoiceMode::None)))
            {
                tracing::debug!("User asked for no tools, calling inner responses model");
                return self.inner_responses.responses_stream(req).await;
            }

            if recursion_limit.is_some_and(|f| f == 0) {
                tracing::debug!(
                    "Tool-use recursion limit reached. Will call model, but not process further"
                );
                return self.inner_responses.responses_stream(req).await;
            }

            // Append spiced runtime tools to the request.
            let inner_req = self.add_runtime_tools(&req);

            let s = self
                .inner_responses
                .responses_stream(inner_req.clone())
                .await?;

            Ok(make_responses_stream(
                Span::current(),
                RequestContext::current(AsyncMarker::new().await),
                Self::new(
                    Arc::clone(&self.inner_responses),
                    self.openai_tools.clone(),
                    self.tools.clone(),
                    recursion_limit.map(|r| r - 1),
                ),
                req,
                s,
            ))
        })
        .await
    }

    fn add_runtime_tools(&self, req: &CreateResponse) -> CreateResponse {
        let mut runtime_tools = self.runtime_tools();
        if runtime_tools.is_empty() {
            tracing::debug!("No runtime tools available, returning original request");
            req.clone()
        } else {
            runtime_tools.extend(req.tools.clone().unwrap_or_default());
            // Ensure function names are unique. Tool-use recursion sometimes creates duplicates.
            runtime_tools.sort_by(|a, b| get_tool_name(a).cmp(get_tool_name(b)));
            runtime_tools.dedup_by(|a, b| get_tool_name(a) == get_tool_name(b));
            let mut req = req.clone();
            req.tools = Some(runtime_tools);
            req
        }
    }
}

#[async_trait]
impl Responses for ToolUsingResponses {
    async fn health(&self) -> Result<(), ResponsesError> {
        self.inner_responses.health().await
    }

    async fn responses_stream(&self, req: CreateResponse) -> Result<ResponseStream, OpenAIError> {
        let inner_req = self.prepare_req(req.clone());
        self.responses_stream_inner(inner_req, self.recursion_limit)
            .await
    }

    async fn responses_request(&self, req: CreateResponse) -> Result<Response, OpenAIError> {
        let inner_req = self.prepare_req(req);
        self.responses_request_inner(inner_req, self.recursion_limit)
            .await
    }
}

struct CustomResponseStream {
    receiver: mpsc::Receiver<Result<ResponseEvent, OpenAIError>>,
}

impl Stream for CustomResponseStream {
    type Item = Result<ResponseEvent, OpenAIError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.receiver.poll_recv(cx)
    }
}

#[allow(clippy::too_many_lines)]
fn make_responses_stream(
    span: Span,
    request_context: Arc<RequestContext>,
    model: ToolUsingResponses,
    req: CreateResponse,
    mut s: ResponseStream,
) -> ResponseStream {
    let (sender, receiver) = mpsc::channel(100);
    let sender_clone = sender.clone();

    tokio::spawn(
        request_context
            .scope(async move {
                let function_call_builders: Arc<Mutex<HashMap<String, FunctionCall>>> =
                    Arc::new(Mutex::new(HashMap::new()));
                let ready_to_call_functions: Arc<Mutex<Vec<FunctionCall>>> =
                    Arc::new(Mutex::new(Vec::new()));

                let mut captured_output = String::new();

                while let Some(result) = s.next().await {
                    let response_event = match result {
                        Ok(event) => event,
                        Err(e) => {
                            if let Err(e) = sender_clone.send(Err(e)).await
                                && !sender_clone.is_closed()
                            {
                                tracing::error!("Unable to send error to response stream: {}", e);
                            }
                            return;
                        }
                    };

                    let mut should_forward = true;
                    let mut should_process_tools = false;

                    match &response_event {
                        ResponseEvent::ResponseOutputTextDelta(delta) => {
                            captured_output.push_str(&delta.delta);
                        }
                        ResponseEvent::ResponseOutputItemAdded(item_added) => {
                            if let OutputItem::FunctionCall(function_call) = &item_added.item {
                                let function_call_builders_clone =
                                    Arc::clone(&function_call_builders);
                                let Ok(mut builders_lock) = function_call_builders_clone.lock()
                                else {
                                    return;
                                };

                                builders_lock
                                    .insert(function_call.id.clone(), function_call.clone());
                                should_forward = false;
                            }
                        }
                        ResponseEvent::ResponseFunctionCallArgumentsDelta(delta) => {
                            let function_call_builders_clone = Arc::clone(&function_call_builders);
                            let Ok(mut builders_lock) = function_call_builders_clone.lock() else {
                                return;
                            };

                            if let Some(state) = builders_lock.get_mut(&delta.item_id) {
                                state.arguments.push_str(&delta.delta);
                            }
                        }
                        ResponseEvent::ResponseFunctionCallArgumentsDone(done) => {
                            let function_call_builders_clone = Arc::clone(&function_call_builders);
                            let Ok(builders_lock) = function_call_builders_clone.lock() else {
                                return;
                            };

                            if let Some(function_call) = builders_lock.get(&done.item_id) {
                                // Move function call to the ready to call list
                                let ready_to_call = Arc::clone(&ready_to_call_functions);
                                let Ok(mut ready_to_call_lock) = ready_to_call.lock() else {
                                    return;
                                };
                                ready_to_call_lock.push(function_call.clone());
                            }
                        }
                        ResponseEvent::ResponseOutputItemDone(item_done) => {
                            // When an output item (like a function call) is done, just note it but don't process tools yet
                            // Tool processing will happen when the entire response is complete
                            if let OutputItem::FunctionCall(function_call) = &item_done.item {
                                // Don't forward individual function call completion events for Spice tools
                                // We'll handle them when the entire response completes
                                let ready_to_call_clone = Arc::clone(&ready_to_call_functions);
                                let spice_tool_found = {
                                    let Ok(ready_to_call_lock) = ready_to_call_clone.lock() else {
                                        return;
                                    };

                                    ready_to_call_lock
                                        .iter()
                                        .find(|call| call.id == function_call.id)
                                        .is_some_and(|call| model.as_spiced_tool(call).is_some())
                                };

                                if spice_tool_found {
                                    // This is a Spice tool - don't forward this event but don't process yet
                                    should_forward = false;
                                    // Don't set should_process_tools = true here - wait for response completion
                                }
                            }
                        }
                        ResponseEvent::ResponseCompleted(_)
                        | ResponseEvent::ResponseIncomplete(_) => {
                            // Only process tools if we haven't already done so and there are spice tools
                            if !should_process_tools {
                                let ready_to_call_clone = Arc::clone(&ready_to_call_functions);
                                let has_spice_tools = {
                                    let Ok(ready_to_call_lock) = ready_to_call_clone.lock() else {
                                        return;
                                    };

                                    ready_to_call_lock
                                        .iter()
                                        .any(|call| model.as_spiced_tool(call).is_some())
                                };

                                if has_spice_tools {
                                    should_forward = false;
                                    should_process_tools = true;
                                }
                            }
                        }
                        _ => {}
                    }

                    // Process completed spiced tool calls when response is complete
                    if should_process_tools {
                        let ready_to_call_clone = Arc::clone(&ready_to_call_functions);
                        let spice_tools: Vec<FunctionCall> = {
                            let Ok(ready_to_call_lock) = ready_to_call_clone.lock() else {
                                return;
                            };

                            ready_to_call_lock
                                .iter()
                                .filter(|call| model.as_spiced_tool(call).is_some())
                                .cloned()
                                .collect()
                        }; // Lock is dropped here

                        if spice_tools.is_empty() {
                            // No spice tools, forward the completion event normally
                            if let Err(e) = sender_clone.send(Ok(response_event)).await
                                && !sender_clone.is_closed()
                            {
                                tracing::error!("Error sending event: {}", e);
                            }
                        } else {
                            // Process spice tools - don't forward the completion event
                            let new_messages = match model
                                .process_tool_calls_and_run_spice_tools(
                                    to_input_item(req.input.clone()),
                                    spice_tools,
                                )
                                .await
                            {
                                Ok(Some(messages)) => messages,
                                Ok(None) => {
                                    // No spice tools within returned tools, forward the event
                                    if let Err(e) = sender_clone.send(Ok(response_event)).await
                                        && !sender_clone.is_closed()
                                    {
                                        tracing::error!("Error sending event: {}", e);
                                    }
                                    continue;
                                }
                                Err(e) => {
                                    if let Err(e) = sender_clone.send(Err(e)).await
                                        && !sender_clone.is_closed()
                                    {
                                        tracing::error!("Error sending error: {}", e);
                                    }
                                    return;
                                }
                            };

                            // Make recursive call for tool results
                            match model
                                .responses_stream_inner(
                                    create_new_recursive_req(&req, new_messages, None),
                                    model.recursion_limit.map(|r| r - 1),
                                )
                                .await
                            {
                                Ok(mut recursive_stream) => {
                                    while let Some(recursive_result) = recursive_stream.next().await
                                    {
                                        if let Err(e) = sender_clone.send(recursive_result).await {
                                            if !sender_clone.is_closed() {
                                                tracing::error!(
                                                    "Error sending recursive event: {}",
                                                    e
                                                );
                                            }
                                            return;
                                        }
                                    }
                                    // Continue processing the original stream after recursive stream completes
                                }
                                Err(e) => {
                                    if let Err(e) = sender_clone.send(Err(e)).await
                                        && !sender_clone.is_closed()
                                    {
                                        tracing::error!("Error sending recursive error: {}", e);
                                    }
                                    return;
                                }
                            }
                        }
                    } else if should_forward {
                        // Forward the event normally
                        if let Err(e) = sender_clone.send(Ok(response_event)).await
                            && !sender_clone.is_closed()
                        {
                            tracing::error!("Error sending event: {}", e);
                        }
                    }
                }

                tracing::info!(target: "task_history", captured_output = %captured_output);
            })
            .instrument(span),
    );

    Box::pin(CustomResponseStream { receiver }) as ResponseStream
}

fn get_tool_name(tool: &ToolDefinition) -> &str {
    match tool {
        ToolDefinition::Function(f) => &f.name,
        ToolDefinition::CodeInterpreter(_) => "code_interpreter",
        ToolDefinition::WebSearchPreview(_) => "web_search",
        ToolDefinition::FileSearch(_) => "file_search",
        ToolDefinition::ComputerUsePreview(_) => "computer_use",
        ToolDefinition::Mcp(_) => "mcp",
        _ => "unknown",
    }
}

fn create_new_recursive_req(
    req: &CreateResponse,
    new_msg: Vec<InputItem>,
    marginal_usage: Option<&Usage>,
) -> CreateResponse {
    let mut new_req = req.clone();
    new_req.input = Input::Items(new_msg);

    // Remove tool_choice if it is named (since it was just used), and set it to `Auto`.
    // This also includes when a tool_choice is not set. It could be set as a default (in spicepod.yaml via openai_tool_choice), but will appear as None here. We want to set it to Auto here to ensure named tool is used once and does not cause infinite tool use.
    if matches!(
        new_req.tool_choice,
        Some(ToolChoice::Function { .. }) | None
    ) {
        // Auto is default when tools exist.
        tracing::debug!("Not recursively using named tool_choice in subsequent calls.");
        new_req.tool_choice = Some(ToolChoice::Mode(ToolChoiceMode::Auto));
    }

    // Adjust input `max_completion_tokens` if usage is known to ensure we don't exceed the limit.
    if let Some(max_output_tokens) = new_req.max_output_tokens
        && let Some(usage) = marginal_usage
    {
        new_req.max_output_tokens = Some(max_output_tokens.saturating_sub(usage.output_tokens));
    }

    new_req
}

fn to_input_item(input: Input) -> Vec<InputItem> {
    match input {
        Input::Text(text) => vec![InputItem::Message(InputMessage {
            content: InputContent::TextInput(text),
            kind: InputMessageType::default(),
            role: Role::User,
        })],
        Input::Items(items) => items,
    }
}

pub fn combine_usage(u1: Option<Usage>, u2: Option<Usage>) -> Option<Usage> {
    match (u1, u2) {
        (Some(u1), Some(u2)) => Some(Usage {
            input_tokens: u1.input_tokens + u2.input_tokens,
            input_tokens_details: combine_token_details(
                &u1.input_tokens_details,
                &u2.input_tokens_details,
            ),
            output_tokens: u1.output_tokens + u2.output_tokens,
            output_tokens_details: combine_completion_token_details(
                &u1.output_tokens_details,
                &u2.output_tokens_details,
            ),
            total_tokens: u1.total_tokens + u2.total_tokens,
        }),
        (Some(u1), None) => Some(u1),
        (None, Some(u2)) => Some(u2),
        (None, None) => None,
    }
}

pub fn combine_token_details(
    a: &PromptTokensDetails,
    b: &PromptTokensDetails,
) -> PromptTokensDetails {
    PromptTokensDetails {
        audio_tokens: combine_opt_u32(a.audio_tokens, b.audio_tokens),
        cached_tokens: combine_opt_u32(a.cached_tokens, b.cached_tokens),
    }
}

pub fn combine_completion_token_details(
    a: &CompletionTokensDetails,
    b: &CompletionTokensDetails,
) -> CompletionTokensDetails {
    CompletionTokensDetails {
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
    }
}
