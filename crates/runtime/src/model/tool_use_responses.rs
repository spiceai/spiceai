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
    types::responses::{
        CodeInterpreterTool, CreateResponse, EasyInputContent, EasyInputMessage,
        FunctionCallOutput, FunctionCallOutputItemParam, FunctionTool, FunctionToolCall, InputItem,
        InputParam, InputTokenDetails, Item, MessageItem, MessageType, OutputItem,
        OutputTokenDetails, Response, ResponseStream, ResponseStreamEvent, ResponseUsage, Role,
        Tool as ToolDefinition, ToolChoiceAllowedMode, ToolChoiceOptions, ToolChoiceParam,
        WebSearchTool,
    },
};
use async_trait::async_trait;
use futures::{Stream, StreamExt};
use itertools::Itertools;
use llms::responses::Error as ResponsesError;
use llms::responses::Responses;
use llms::{chat::Error as LlmError, progress::Progress};
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::mpsc;
use tools::SpiceModelTool;
use tracing::{Instrument, Span};

use crate::model::tool_use::encode_tool_name;
use runtime_request_context::{AsyncMarker, RequestContext};

#[derive(Clone, Debug)]
pub enum OpenAIResponsesTools {
    CodeInterpreter,
    WebSearch,
    // The legacy `web_search_preview` tool, retained for older models that do not support the
    // current `web_search` tool.
    WebSearchPreview,
}

impl From<OpenAIResponsesTools> for ToolDefinition {
    fn from(tool: OpenAIResponsesTools) -> Self {
        match tool {
            OpenAIResponsesTools::CodeInterpreter => {
                ToolDefinition::CodeInterpreter(CodeInterpreterTool::default())
            }
            OpenAIResponsesTools::WebSearch => ToolDefinition::WebSearch(WebSearchTool::default()),
            OpenAIResponsesTools::WebSearchPreview => {
                ToolDefinition::WebSearchPreview(WebSearchTool::default())
            }
        }
    }
}

impl TryFrom<&str> for OpenAIResponsesTools {
    type Error = LlmError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "code_interpreter" => Ok(OpenAIResponsesTools::CodeInterpreter),
            "web_search" => Ok(OpenAIResponsesTools::WebSearch),
            "web_search_preview" => Ok(OpenAIResponsesTools::WebSearchPreview),
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
        let existing_items = to_input_item(req.input.clone());

        let openai_tool_definitions: Vec<ToolDefinition> = self
            .openai_tools
            .clone()
            .into_iter()
            .map(Into::into)
            .collect();
        req.tools = Some(openai_tool_definitions);

        req.input = InputParam::Items(existing_items);

        req
    }

    #[must_use]
    pub fn runtime_tools(&self) -> Vec<ToolDefinition> {
        self.tools
            .iter()
            .map(|t| {
                ToolDefinition::Function(FunctionTool {
                    strict: t.strict(),
                    name: encode_tool_name(t.name().to_string().as_str()),
                    description: t.description().map(|d| d.to_string()),
                    parameters: Some(
                        t.parameters()
                            .map(|mut params| {
                                if let Value::Object(ref mut obj) = params {
                                    obj.insert(
                                        "additionalProperties".to_string(),
                                        Value::Bool(false),
                                    );
                                }
                                params
                            })
                            .unwrap_or(json!({})),
                    ),
                })
            })
            .collect()
    }

    // This is a bad function name. Its more like `find_spiced_tool`.
    fn as_spiced_tool(&self, name: &str) -> Option<Arc<dyn SpiceModelTool>> {
        self.tools
            .iter()
            .find(|tool| encode_tool_name(tool.name().as_ref()) == name)
            .cloned()
    }

    async fn call_tool(&self, tool_call: &FunctionToolCall) -> Value {
        let FunctionToolCall {
            name,
            arguments,
            id,
            ..
        } = tool_call;
        match self.as_spiced_tool(name) {
            Some(t) => match t.call(arguments).await {
                Ok(v) => {
                    tracing::info!(
                        target: "task_history",
                        progress = Progress::log()
                            .id(id.clone())
                            .title(format!("'{name}' tool completed successfully"))
                            .json_content(v.clone())
                            .to_jsonl(),
                    );
                    v
                }
                Err(e) => {
                    tracing::info!(
                        target: "task_history",
                        progress = Progress::error()
                            .id(id.clone())
                            .title(format!("'{name}' tool completed unsuccessfully"))
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
                        "Tool '{name}' was provided to LLM, but now no longer exists. This should not be possible."
                    );
                } else {
                    tracing::warn!(
                        "Tool '{name}' was provided to LLM, but now no longer exists. This should not be possible.",
                    );
                    Value::Null
                }
            }
        }
    }

    /// Whether `item` is a call to one of Spice's tools.
    fn is_spice_tool_call(&self, item: &OutputItem) -> bool {
        matches!(item, OutputItem::FunctionCall(call) if self.as_spiced_tool(&call.name).is_some())
    }

    /// `item`, an item of one round's output, as input for the next round.
    fn replay_item(&self, item: &OutputItem) -> Option<Item> {
        match item {
            OutputItem::Message(message) => {
                Some(Item::Message(MessageItem::Output(message.clone())))
            }
            OutputItem::Reasoning(reasoning) => Some(Item::Reasoning(reasoning.clone())),
            OutputItem::FunctionCall(call) => self
                .as_spiced_tool(&call.name)
                .is_some()
                .then(|| Item::FunctionCall(call.clone())),
            OutputItem::WebSearchCall(call) => Some(Item::WebSearchCall(call.clone())),
            OutputItem::FileSearchCall(call) => Some(Item::FileSearchCall(call.clone())),
            OutputItem::CodeInterpreterCall(call) => Some(Item::CodeInterpreterCall(call.clone())),
            OutputItem::ImageGenerationCall(call) => Some(Item::ImageGenerationCall(call.clone())),
            OutputItem::McpCall(call) => Some(Item::McpCall(call.clone())),
            OutputItem::McpListTools(tools) => Some(Item::McpListTools(tools.clone())),
            // Calls the client runs, which the next round has no result for, and items whose
            // input form differs from their output form.
            OutputItem::ComputerCall(_)
            | OutputItem::LocalShellCall(_)
            | OutputItem::ShellCall(_)
            | OutputItem::ShellCallOutput(_)
            | OutputItem::ApplyPatchCall(_)
            | OutputItem::ApplyPatchCallOutput(_)
            | OutputItem::McpApprovalRequest(_)
            | OutputItem::CustomToolCall(_)
            | OutputItem::Compaction(_) => None,
        }
    }

    /// Runs the Spice tool calls in `output`, one round's output, and returns the input for the
    /// next round: `input`, then `output`, then the calls' results, paired with the calls by
    /// `call_id`. `None` when `output` calls no Spice tool.
    ///
    /// The round's output is replayed as is, item IDs included: a reasoning model pairs each tool
    /// call with a reasoning item, and `OpenAI` rejects one replayed without the other. The next
    /// round also gets the round's hosted tool results, such as a web search's.
    async fn run_spice_tool_calls(
        &self,
        input: Vec<InputItem>,
        output: &[OutputItem],
    ) -> Option<Vec<InputItem>> {
        let calls = output
            .iter()
            .filter_map(|item| match item {
                OutputItem::FunctionCall(call) if self.as_spiced_tool(&call.name).is_some() => {
                    Some(call)
                }
                _ => None,
            })
            .collect_vec();

        // Return early if no spiced runtime tools used.
        if calls.is_empty() {
            tracing::debug!("No spiced tools used by chat model, returning early");
            return None;
        }

        let mut next_input = input;
        next_input.extend(
            output
                .iter()
                .filter_map(|item| self.replay_item(item))
                .map(InputItem::Item),
        );
        for call in &calls {
            tracing::info!(
                target: "task_history",
                progress = Progress::log()
                    .id(call.id.clone())
                    .title(format!("Calling '{}' tool", call.name))
                    .content(call.arguments.clone())
                    .to_jsonl(),
            );
            let content = self.call_tool(call).await;
            next_input.push(InputItem::Item(Item::FunctionCallOutput(
                FunctionCallOutputItemParam {
                    call_id: call.call_id.clone(),
                    output: FunctionCallOutput::Text(
                        serde_json::to_string(&content)
                            .unwrap_or("Error calling tool.".to_string()),
                    ),
                    id: None,
                    status: None,
                },
            )));
        }

        let context = RequestContext::current(AsyncMarker::new().await);
        crate::model::add_tools_used(&context, calls.len());

        Some(next_input)
    }

    async fn responses_request_inner(
        &self,
        req: CreateResponse,
        recursion_limit: Option<usize>,
    ) -> Result<Response, OpenAIError> {
        // Don't use spice runtime tools if users has explicitly chosen to not use any tools.
        if req
            .tool_choice
            .as_ref()
            .is_some_and(|t| matches!(t, ToolChoiceParam::Mode(ToolChoiceOptions::None)))
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
        let mut req = self.add_runtime_tools(&req);
        // The rounds that may still run Spice tools; `None` for no limit.
        let mut tool_rounds_left = recursion_limit;
        // The output of the rounds that ran Spice tools, less those calls, and their usage.
        let mut earlier_output = Vec::new();
        let mut earlier_usage = None;

        loop {
            let response = self.inner_responses.responses_request(req.clone()).await?;

            if tool_rounds_left != Some(0)
                && let Some(next_input) = self
                    .run_spice_tool_calls(to_input_item(req.input.clone()), &response.output)
                    .await
            {
                req = create_new_recursive_req(&req, next_input, response.usage.as_ref());
                earlier_usage = combine_usage(earlier_usage, response.usage);
                earlier_output.extend(
                    response
                        .output
                        .into_iter()
                        .filter(|item| !self.is_spice_tool_call(item)),
                );
                tool_rounds_left = tool_rounds_left.map(|rounds| rounds - 1);
                continue;
            }

            return Ok(with_earlier_rounds(response, earlier_output, earlier_usage));
        }
    }

    async fn responses_stream_inner(
        &self,
        req: CreateResponse,
        recursion_limit: Option<usize>,
    ) -> Result<ResponseStream, OpenAIError> {
        // Don't use spice runtime tools if users has explicitly chosen to not use any tools.
        if req
            .tool_choice
            .as_ref()
            .is_some_and(|t| matches!(t, ToolChoiceParam::Mode(ToolChoiceOptions::None)))
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
        let req = self.add_runtime_tools(&req);

        let s = self.inner_responses.responses_stream(req.clone()).await?;

        Ok(make_responses_stream(
            Span::current(),
            RequestContext::current(AsyncMarker::new().await),
            Self::new(
                Arc::clone(&self.inner_responses),
                self.openai_tools.clone(),
                self.tools.clone(),
                recursion_limit,
            ),
            req,
            s,
        ))
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
    receiver: mpsc::Receiver<Result<ResponseStreamEvent, OpenAIError>>,
}

impl Stream for CustomResponseStream {
    type Item = Result<ResponseStreamEvent, OpenAIError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.receiver.poll_recv(cx)
    }
}

/// The client's view of a streamed tool-use loop: one response, however many rounds it takes.
///
/// Each round is a response of its own from the provider, with its own lifecycle events, and its
/// own `sequence_number`s and `output_index`es counting from 0. The client is sent the first
/// round's `response.created` and `response.in_progress` only, one run of `sequence_number`s, and
/// `output_index`es that count the items it was sent across rounds. The Spice tool calls it is not
/// sent leave no gap.
#[derive(Default)]
struct ClientStream {
    /// The rounds that ran Spice tools so far.
    tool_rounds: usize,
    /// The `sequence_number` of the next event sent to the client.
    sequence_number: u64,
    /// How many output items the client was sent.
    items_sent: u64,
    /// The client's `output_index` of each item of the current round that it was sent.
    output_indexes: HashMap<u64, u64>,
    /// The `output_index`es of the current round's Spice tool calls, which the client is not sent.
    hidden_indexes: HashSet<u64>,
    /// The output items of earlier rounds that the client was sent.
    earlier_output: Vec<OutputItem>,
    /// The usage of earlier rounds.
    earlier_usage: Option<ResponseUsage>,
}

impl ClientStream {
    /// `event` as the client is to receive it, or `None` when the client is not to receive it.
    /// `hide` tells whether an output item is a Spice tool call to keep from the client.
    fn event(
        &mut self,
        mut event: ResponseStreamEvent,
        hide: impl Fn(&OutputItem) -> bool,
    ) -> Option<ResponseStreamEvent> {
        match &mut event {
            // A later round continues the response the client already has.
            ResponseStreamEvent::ResponseCreated(_)
            | ResponseStreamEvent::ResponseInProgress(_)
            | ResponseStreamEvent::ResponseQueued(_)
                if self.tool_rounds > 0 =>
            {
                return None;
            }
            ResponseStreamEvent::ResponseOutputItemAdded(added) => {
                let index = u64::from(added.output_index);
                if hide(&added.item) {
                    self.hidden_indexes.insert(index);
                    return None;
                }
                self.output_indexes.insert(index, self.items_sent);
                self.items_sent += 1;
            }
            ResponseStreamEvent::ResponseCompleted(completed) => {
                self.finish(&mut completed.response);
            }
            ResponseStreamEvent::ResponseIncomplete(incomplete) => {
                self.finish(&mut incomplete.response);
            }
            ResponseStreamEvent::ResponseFailed(failed) => self.finish(&mut failed.response),
            _ => {}
        }

        // Every event has a `sequence_number`, and every event about one output item has an
        // `output_index`. Renumbering them on the serialized event covers every event type.
        let Ok(mut value) = serde_json::to_value(&event) else {
            return Some(event);
        };
        if let Some(index) = value.get("output_index").and_then(Value::as_u64) {
            if self.hidden_indexes.contains(&index) {
                return None;
            }
            if let Some(client_index) = self.output_indexes.get(&index) {
                value["output_index"] = json!(client_index);
            }
        }
        value["sequence_number"] = json!(self.sequence_number);
        self.sequence_number += 1;
        Some(serde_json::from_value(value).unwrap_or(event))
    }

    /// Ends a round that ran Spice tools: the next round continues the client's response.
    fn end_round(&mut self, response: Response) {
        let hidden_indexes = std::mem::take(&mut self.hidden_indexes);
        self.earlier_output.extend(
            response
                .output
                .into_iter()
                .zip(0_u64..)
                .filter(|(_, index)| !hidden_indexes.contains(index))
                .map(|(item, _)| item),
        );
        self.earlier_usage = combine_usage(self.earlier_usage.take(), response.usage);
        self.output_indexes.clear();
        self.tool_rounds += 1;
    }

    /// Makes `response`, the last round's, the response to the whole loop, as
    /// [`with_earlier_rounds`] does, less the Spice tool calls the client was not sent.
    fn finish(&mut self, response: &mut Response) {
        let mut output = std::mem::take(&mut self.earlier_output);
        output.extend(
            std::mem::take(&mut response.output)
                .into_iter()
                .zip(0_u64..)
                .filter(|(_, index)| !self.hidden_indexes.contains(index))
                .map(|(item, _)| item),
        );
        response.output = output;
        response.usage = combine_usage(self.earlier_usage.take(), response.usage.take());
    }
}

/// The response a `response.completed` or `response.incomplete` event ends a round with.
fn ended_response(event: &ResponseStreamEvent) -> Option<&Response> {
    match event {
        ResponseStreamEvent::ResponseCompleted(completed) => Some(&completed.response),
        ResponseStreamEvent::ResponseIncomplete(incomplete) => Some(&incomplete.response),
        _ => None,
    }
}

fn make_responses_stream(
    span: Span,
    request_context: Arc<RequestContext>,
    model: ToolUsingResponses,
    mut req: CreateResponse,
    mut s: ResponseStream,
) -> ResponseStream {
    let (sender, receiver) = mpsc::channel(100);

    tokio::spawn(
        request_context
            .scope(async move {
                let mut client_stream = ClientStream::default();
                // The rounds that may still run Spice tools; `None` for no limit.
                let mut tool_rounds_left = model.recursion_limit;
                let mut captured_output = String::new();

                loop {
                    let runs_tools = tool_rounds_left != Some(0);
                    let hide = |item: &OutputItem| runs_tools && model.is_spice_tool_call(item);
                    // The round's last event, held back until it is known whether the round runs
                    // Spice tools.
                    let mut last_event = None;

                    while let Some(result) = s.next().await {
                        let event = match result {
                            Ok(event) => event,
                            Err(e) => {
                                let _ = sender.send(Err(e)).await;
                                return;
                            }
                        };
                        if let ResponseStreamEvent::ResponseOutputTextDelta(delta) = &event {
                            captured_output.push_str(&delta.delta);
                        }
                        if runs_tools && ended_response(&event).is_some() {
                            last_event = Some(event);
                            break;
                        }
                        if let Some(event) = client_stream.event(event, hide)
                            && sender.send(Ok(event)).await.is_err()
                        {
                            // The client went away.
                            return;
                        }
                    }

                    let Some(last_event) = last_event else {
                        break;
                    };
                    let Some(response) = ended_response(&last_event) else {
                        break;
                    };
                    let Some(next_input) = model
                        .run_spice_tool_calls(to_input_item(req.input.clone()), &response.output)
                        .await
                    else {
                        // The round called no Spice tool, so it is the last.
                        if let Some(event) = client_stream.event(last_event, hide) {
                            let _ = sender.send(Ok(event)).await;
                        }
                        break;
                    };

                    req = create_new_recursive_req(&req, next_input, response.usage.as_ref());
                    client_stream.end_round(response.clone());
                    tool_rounds_left = tool_rounds_left.map(|rounds| rounds - 1);
                    s = match model.inner_responses.responses_stream(req.clone()).await {
                        Ok(s) => s,
                        Err(e) => {
                            let _ = sender.send(Err(e)).await;
                            return;
                        }
                    };
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
        ToolDefinition::WebSearch(_) => "web_search",
        ToolDefinition::WebSearchPreview(_) => "web_search_preview",
        ToolDefinition::FileSearch(_) => "file_search",
        ToolDefinition::ComputerUsePreview(_) => "computer_use",
        ToolDefinition::Mcp(_) => "mcp",
        _ => "unknown",
    }
}

fn create_new_recursive_req(
    req: &CreateResponse,
    new_msg: Vec<InputItem>,
    marginal_usage: Option<&ResponseUsage>,
) -> CreateResponse {
    let mut new_req = req.clone();
    new_req.input = InputParam::Items(new_msg);
    new_req.tool_choice = Some(next_round_tool_choice(new_req.tool_choice.take()));

    // Adjust input `max_output_tokens` if usage is known to ensure we don't exceed the limit.
    if let Some(max_output_tokens) = new_req.max_output_tokens
        && let Some(usage) = marginal_usage
    {
        new_req.max_output_tokens = Some(max_output_tokens.saturating_sub(usage.output_tokens));
    }

    new_req
}

/// The `tool_choice` for the round after one that called tools — the Responses
/// counterpart of `tool_use::next_round_tool_choice`: a choice that forces a call
/// applies to one round (issue #14459), and `allowed_tools` keeps its tool list in
/// `auto` mode. An unset choice is sent as `auto` as well.
fn next_round_tool_choice(choice: Option<ToolChoiceParam>) -> ToolChoiceParam {
    match choice {
        None => ToolChoiceParam::Mode(ToolChoiceOptions::Auto),
        Some(
            ToolChoiceParam::Function(_)
            | ToolChoiceParam::Mcp(_)
            | ToolChoiceParam::Custom(_)
            | ToolChoiceParam::ApplyPatch
            | ToolChoiceParam::Shell
            | ToolChoiceParam::Hosted(_)
            | ToolChoiceParam::Mode(ToolChoiceOptions::Required),
        ) => {
            tracing::debug!("Not forcing a tool call again after a round that made one.");
            ToolChoiceParam::Mode(ToolChoiceOptions::Auto)
        }
        Some(ToolChoiceParam::AllowedTools(mut allowed)) => {
            allowed.mode = ToolChoiceAllowedMode::Auto;
            ToolChoiceParam::AllowedTools(allowed)
        }
        Some(choice @ ToolChoiceParam::Mode(ToolChoiceOptions::Auto | ToolChoiceOptions::None)) => {
            choice
        }
    }
}

fn to_input_item(input: InputParam) -> Vec<InputItem> {
    match input {
        InputParam::Text(text) => vec![InputItem::EasyMessage(EasyInputMessage {
            content: EasyInputContent::Text(text),
            role: Role::User,
            r#type: MessageType::Message,
        })],
        InputParam::Items(items) => items,
    }
}

/// `response`, the last round's, as the response to the whole tool-use loop: the output of the
/// earlier rounds comes first, and the usage covers every round. It keeps the last round's ID,
/// the response whose input holds the whole exchange, so that a client continuing with
/// `previous_response_id` continues from all of it.
fn with_earlier_rounds(
    mut response: Response,
    mut earlier_output: Vec<OutputItem>,
    earlier_usage: Option<ResponseUsage>,
) -> Response {
    earlier_output.append(&mut response.output);
    response.output = earlier_output;
    response.usage = combine_usage(earlier_usage, response.usage);
    response
}

pub fn combine_usage(
    u1: Option<ResponseUsage>,
    u2: Option<ResponseUsage>,
) -> Option<ResponseUsage> {
    match (u1, u2) {
        (Some(u1), Some(u2)) => Some(ResponseUsage {
            input_tokens: u1.input_tokens + u2.input_tokens,
            input_tokens_details: InputTokenDetails {
                cached_tokens: u1.input_tokens_details.cached_tokens
                    + u2.input_tokens_details.cached_tokens,
            },
            output_tokens: u1.output_tokens + u2.output_tokens,
            output_tokens_details: OutputTokenDetails {
                reasoning_tokens: u1.output_tokens_details.reasoning_tokens
                    + u2.output_tokens_details.reasoning_tokens,
            },
            total_tokens: u1.total_tokens + u2.total_tokens,
        }),
        (Some(u1), None) => Some(u1),
        (None, Some(u2)) => Some(u2),
        (None, None) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_openai::types::responses::{
        CreateResponseArgs, ToolChoiceAllowed, ToolChoiceCustom, ToolChoiceFunction,
        ToolChoiceTypes,
    };
    use parking_lot::Mutex;
    use std::borrow::Cow;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// A `list_datasets` Spice tool that counts its calls.
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
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(json!(["taxi_trips"]))
        }
    }

    /// A provider whose response in round `n` outputs the items in `rounds[n]`: a text message for
    /// `"message"`, else a call to the function by that name. Once `rounds` runs out, it answers
    /// with a message. It records every request.
    struct ScriptedResponses {
        rounds: Vec<Vec<&'static str>>,
        requests: Mutex<Vec<CreateResponse>>,
    }

    impl ScriptedResponses {
        fn new(rounds: Vec<Vec<&'static str>>) -> Arc<Self> {
            Arc::new(Self {
                rounds,
                requests: Mutex::new(Vec::new()),
            })
        }

        /// Records `req`, and returns its round and the items its response outputs.
        fn next_round(&self, req: CreateResponse) -> (usize, Vec<&'static str>) {
            let mut requests = self.requests.lock();
            requests.push(req);
            let round = requests.len() - 1;
            let items = self
                .rounds
                .get(round)
                .cloned()
                .unwrap_or_else(|| vec!["message"]);
            (round, items)
        }

        /// The input of request `round`.
        fn input(&self, round: usize) -> Value {
            serde_json::to_value(&self.requests.lock()[round].input).expect("a serializable input")
        }
    }

    #[async_trait]
    impl Responses for ScriptedResponses {
        async fn health(&self) -> Result<(), ResponsesError> {
            Ok(())
        }

        async fn responses_stream(
            &self,
            req: CreateResponse,
        ) -> Result<ResponseStream, OpenAIError> {
            let (round, items) = self.next_round(req);
            Ok(Box::pin(futures::stream::iter(
                round_events(round, &items).into_iter().map(Ok),
            )))
        }

        async fn responses_request(&self, req: CreateResponse) -> Result<Response, OpenAIError> {
            let (round, items) = self.next_round(req);
            Ok(
                serde_json::from_value(response(round, "completed", &output(round, &items)))
                    .expect("a valid response"),
            )
        }
    }

    /// Output item `index` of round `round`: a text message for `"message"`, else a call to the
    /// function by that name.
    fn item(round: usize, index: usize, name: &str, status: &str) -> Value {
        let done = status == "completed";
        if name == "message" {
            let text = json!([{
                "type": "output_text", "text": "taxi_trips", "annotations": [], "logprobs": null,
            }]);
            json!({
                "type": "message", "id": format!("msg_{round}_{index}"), "role": "assistant",
                "status": status, "content": if done { text } else { json!([]) },
            })
        } else {
            json!({
                "type": "function_call", "id": format!("fc_{round}_{index}"),
                "call_id": format!("call_{round}_{index}"), "name": name,
                "arguments": if done { "{}" } else { "" }, "status": status,
            })
        }
    }

    fn output(round: usize, items: &[&str]) -> Vec<Value> {
        items
            .iter()
            .enumerate()
            .map(|(index, name)| item(round, index, name, "completed"))
            .collect()
    }

    /// Round `round`'s response. A completed one reports 11 tokens of usage.
    fn response(round: usize, status: &str, output: &[Value]) -> Value {
        let mut response = json!({
            "id": format!("resp_{round}"), "object": "response", "created_at": 0,
            "model": "scripted", "status": status, "output": output,
        });
        if status == "completed" {
            response["usage"] = json!({
                "input_tokens": 10, "input_tokens_details": { "cached_tokens": 0 },
                "output_tokens": 1, "output_tokens_details": { "reasoning_tokens": 0 },
                "total_tokens": 11,
            });
        }
        response
    }

    /// The events of round `round`'s response, in the order `OpenAI` streams them.
    fn round_events(round: usize, items: &[&str]) -> Vec<ResponseStreamEvent> {
        let mut events = vec![json!({
            "type": "response.created", "response": response(round, "in_progress", &[]),
        })];
        for (index, name) in items.iter().enumerate() {
            let item_id = item(round, index, name, "completed")["id"].clone();
            events.push(json!({
                "type": "response.output_item.added", "output_index": index,
                "item": item(round, index, name, "in_progress"),
            }));
            if *name == "message" {
                events.push(json!({
                    "type": "response.output_text.delta", "item_id": item_id,
                    "output_index": index, "content_index": 0, "delta": "taxi_trips",
                }));
            } else {
                events.extend([
                    json!({
                        "type": "response.function_call_arguments.delta", "item_id": item_id,
                        "output_index": index, "delta": "{}",
                    }),
                    json!({
                        "type": "response.function_call_arguments.done", "item_id": item_id,
                        "output_index": index, "arguments": "{}",
                    }),
                ]);
            }
            events.push(json!({
                "type": "response.output_item.done", "output_index": index,
                "item": item(round, index, name, "completed"),
            }));
        }
        events.push(json!({
            "type": "response.completed",
            "response": response(round, "completed", &output(round, items)),
        }));
        events
            .into_iter()
            .enumerate()
            .map(|(sequence_number, mut event)| {
                event["sequence_number"] = json!(sequence_number);
                serde_json::from_value(event).expect("a valid stream event")
            })
            .collect()
    }

    fn model(
        provider: &Arc<ScriptedResponses>,
        tool: &Arc<ListDatasets>,
        recursion_limit: Option<usize>,
    ) -> ToolUsingResponses {
        ToolUsingResponses::new(
            Arc::clone(provider) as Arc<dyn Responses>,
            vec![],
            vec![Arc::clone(tool) as Arc<dyn SpiceModelTool>],
            recursion_limit,
        )
    }

    fn request(stream: bool) -> CreateResponse {
        CreateResponseArgs::default()
            .model("scripted")
            .input("What datasets do you have access to?")
            .stream(stream)
            .build()
            .expect("a valid request")
    }

    /// The events the client receives for a streamed request.
    async fn stream_through_spice(
        provider: &Arc<ScriptedResponses>,
        tool: &Arc<ListDatasets>,
        recursion_limit: Option<usize>,
    ) -> Vec<Value> {
        model(provider, tool, recursion_limit)
            .responses_stream(request(true))
            .await
            .expect("a response stream")
            .map(|event| {
                serde_json::to_value(event.expect("an event, not an error"))
                    .expect("a serializable event")
            })
            .collect()
            .await
    }

    /// Each event, as its type plus the item's name or type, or else the output index the event
    /// belongs to.
    fn summary(events: &[Value]) -> Vec<String> {
        events
            .iter()
            .map(|event| {
                let event_type = event["type"].as_str().unwrap_or_default();
                match (
                    event["item"]["name"].as_str(),
                    event["item"]["type"].as_str(),
                    event["output_index"].as_u64(),
                ) {
                    (Some(name), _, _) => format!("{event_type} {name}"),
                    (None, Some(item_type), _) => format!("{event_type} {item_type}"),
                    (None, None, Some(index)) => format!("{event_type} #{index}"),
                    (None, None, None) => event_type.to_string(),
                }
            })
            .collect()
    }

    fn user_message() -> Value {
        json!({ "type": "message", "role": "user", "content": "What datasets do you have access to?" })
    }

    fn tool_result(round: usize, index: usize) -> Value {
        json!({
            "type": "function_call_output", "call_id": format!("call_{round}_{index}"),
            "output": "[\"taxi_trips\"]",
        })
    }

    // regression test for #14905
    #[tokio::test]
    async fn test_streamed_spice_tool_call_runs_unseen_by_the_client() {
        let provider = ScriptedResponses::new(vec![vec!["list_datasets"]]);
        let tool = Arc::new(ListDatasets(AtomicUsize::new(0)));

        assert_eq!(
            summary(&stream_through_spice(&provider, &tool, None).await),
            [
                "response.created",
                "response.output_item.added message",
                "response.output_text.delta #0",
                "response.output_item.done message",
                "response.completed",
            ]
        );
        assert_eq!(tool.0.load(Ordering::SeqCst), 1);

        // The follow-up request replays the round's output, and pairs the call with its result by
        // `call_id`.
        assert_eq!(provider.requests.lock().len(), 2);
        assert_eq!(
            provider.input(1),
            json!([
                user_message(),
                item(0, 0, "list_datasets", "completed"),
                tool_result(0, 0)
            ])
        );
    }

    #[tokio::test]
    async fn test_streamed_client_tool_call_reaches_the_client_whole() {
        let provider = ScriptedResponses::new(vec![vec!["client_tool", "list_datasets"]]);
        let tool = Arc::new(ListDatasets(AtomicUsize::new(0)));

        let events = stream_through_spice(&provider, &tool, None).await;
        assert_eq!(
            summary(&events),
            [
                "response.created",
                "response.output_item.added client_tool",
                "response.function_call_arguments.delta #0",
                "response.function_call_arguments.done #0",
                "response.output_item.done client_tool",
                "response.output_item.added message",
                "response.output_text.delta #1",
                "response.output_item.done message",
                "response.completed",
            ]
        );
        assert_eq!(tool.0.load(Ordering::SeqCst), 1);
        assert_eq!(
            events
                .last()
                .map(|completed| completed["response"]["output"].clone()),
            Some(json!([
                item(0, 0, "client_tool", "completed"),
                item(1, 0, "message", "completed")
            ]))
        );

        // The next round has no result for the client's call, so it is not replayed.
        assert_eq!(
            provider.input(1),
            json!([
                user_message(),
                item(0, 1, "list_datasets", "completed"),
                tool_result(0, 1)
            ])
        );
    }

    #[tokio::test]
    async fn test_streamed_tool_rounds_reach_the_client_as_one_response() {
        // Round 0 says something before it calls `list_datasets`; round 1 answers.
        let provider = ScriptedResponses::new(vec![vec!["message", "list_datasets"]]);
        let tool = Arc::new(ListDatasets(AtomicUsize::new(0)));

        let events = stream_through_spice(&provider, &tool, None).await;
        assert_eq!(
            summary(&events),
            [
                "response.created",
                "response.output_item.added message",
                "response.output_text.delta #0",
                "response.output_item.done message",
                "response.output_item.added message",
                "response.output_text.delta #1",
                "response.output_item.done message",
                "response.completed",
            ]
        );
        assert_eq!(
            events
                .iter()
                .map(|event| event["sequence_number"].as_u64())
                .collect_vec(),
            (0..8).map(Some).collect_vec()
        );

        // The response completes as the last round's, with every item the client was sent and
        // every round's usage.
        let completed = &events[7]["response"];
        assert_eq!(completed["id"], "resp_1");
        assert_eq!(
            completed["output"],
            json!([
                item(0, 0, "message", "completed"),
                item(1, 0, "message", "completed")
            ])
        );
        assert_eq!(completed["usage"]["total_tokens"], 22);

        assert_eq!(
            provider.input(1),
            json!([
                user_message(),
                item(0, 0, "message", "completed"),
                item(0, 1, "list_datasets", "completed"),
                tool_result(0, 1)
            ])
        );
    }

    #[tokio::test]
    async fn test_tool_rounds_return_one_response() {
        let provider = ScriptedResponses::new(vec![vec!["message", "list_datasets"]]);
        let tool = Arc::new(ListDatasets(AtomicUsize::new(0)));

        let response = serde_json::to_value(
            model(&provider, &tool, None)
                .responses_request(request(false))
                .await
                .expect("a response"),
        )
        .expect("a serializable response");

        assert_eq!(tool.0.load(Ordering::SeqCst), 1);
        assert_eq!(response["id"], "resp_1");
        assert_eq!(
            response["output"],
            json!([
                item(0, 0, "message", "completed"),
                item(1, 0, "message", "completed")
            ])
        );
        assert_eq!(response["usage"]["total_tokens"], 22);
        assert_eq!(
            provider.input(1),
            json!([
                user_message(),
                item(0, 0, "message", "completed"),
                item(0, 1, "list_datasets", "completed"),
                tool_result(0, 1)
            ])
        );
    }

    // regression test for #14631
    #[tokio::test]
    async fn test_streamed_tool_rounds_follow_the_recursion_limit() {
        for limit in [1, 2, 3, 10] {
            // The provider calls `list_datasets` in every round.
            let provider = ScriptedResponses::new(vec![vec!["list_datasets"]; limit + 1]);
            let tool = Arc::new(ListDatasets(AtomicUsize::new(0)));

            let events = stream_through_spice(&provider, &tool, Some(limit)).await;

            // As in `responses_request_inner`: `limit` rounds run Spice tools, and the round
            // after them returns the model's response as is.
            assert_eq!(tool.0.load(Ordering::SeqCst), limit, "limit {limit}");
            assert_eq!(provider.requests.lock().len(), limit + 1, "limit {limit}");
            assert_eq!(
                events.last().map(|event| event["type"].clone()),
                Some(json!("response.completed")),
                "limit {limit}"
            );
        }
    }

    fn allowed_tools(mode: ToolChoiceAllowedMode) -> ToolChoiceParam {
        ToolChoiceParam::AllowedTools(ToolChoiceAllowed {
            mode,
            tools: vec![json!({ "type": "function", "name": "list_datasets" })],
        })
    }

    // regression test for #14459
    #[test]
    fn test_next_round_tool_choice() {
        let auto = ToolChoiceParam::Mode(ToolChoiceOptions::Auto);
        for forced in [
            None,
            Some(ToolChoiceParam::Mode(ToolChoiceOptions::Required)),
            Some(ToolChoiceParam::Function(ToolChoiceFunction {
                name: "list_datasets".to_string(),
            })),
            Some(ToolChoiceParam::Custom(ToolChoiceCustom {
                name: "client_tool".to_string(),
            })),
            Some(ToolChoiceParam::ApplyPatch),
            Some(ToolChoiceParam::Shell),
            Some(ToolChoiceParam::Hosted(ToolChoiceTypes::WebSearchPreview)),
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
            ToolChoiceParam::Mode(ToolChoiceOptions::Auto),
            ToolChoiceParam::Mode(ToolChoiceOptions::None),
            allowed_tools(ToolChoiceAllowedMode::Auto),
        ] {
            assert_eq!(next_round_tool_choice(Some(unforced.clone())), unforced);
        }
    }
}
