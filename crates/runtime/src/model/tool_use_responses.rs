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
        InputParam, InputTokenDetails, Item, MCPToolCallStatus, MessageItem, MessageType,
        OutputItem, OutputTokenDetails, Response, ResponseMCPCallArgumentsDeltaEvent,
        ResponseMCPCallArgumentsDoneEvent, ResponseMCPCallCompletedEvent,
        ResponseMCPCallFailedEvent, ResponseMCPCallInProgressEvent, ResponseOutputItemAddedEvent,
        ResponseOutputItemDoneEvent, ResponseStream, ResponseStreamEvent, ResponseUsage, Role,
        Tool as ToolDefinition, ToolChoiceAllowedMode, ToolChoiceOptions, ToolChoiceParam,
        WebSearchTool,
    },
};
use async_trait::async_trait;
use futures::{Stream, StreamExt};
use llms::responses::Error as ResponsesError;
use llms::responses::Responses;
use llms::{chat::Error as LlmError, progress::Progress};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::mpsc;
use tools::SpiceModelTool;
use tracing::{Instrument, Span};

use crate::model::responses_tool_history::{ToolOutcome, mcp_call, replayed_call};
use crate::model::tool_use::{
    MAX_SPICE_TOOL_CALLS_PER_RESPONSE, encode_tool_name, tool_call_not_run_message,
    warn_if_over_tool_call_limit,
};
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
                    defer_loading: None,
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

    async fn call_tool(&self, tool_call: &FunctionToolCall) -> ToolOutcome {
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
                    ToolOutcome::Completed(v)
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
                    ToolOutcome::Failed(format!(
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
                    ToolOutcome::Completed(Value::Null)
                }
            }
        }
    }

    /// Whether `item` is a call to one of Spice's tools.
    fn is_spice_tool_call(&self, item: &OutputItem) -> bool {
        matches!(item, OutputItem::FunctionCall(call) if self.as_spiced_tool(&call.name).is_some())
    }

    /// The calls to Spice's tools in `output`, one round's output.
    fn spice_tool_calls<'a>(&self, output: &'a [OutputItem]) -> Vec<&'a FunctionToolCall> {
        output
            .iter()
            .filter_map(|item| match item {
                OutputItem::FunctionCall(call) if self.as_spiced_tool(&call.name).is_some() => {
                    Some(call)
                }
                _ => None,
            })
            .collect()
    }

    /// Runs `call`, a call to one of Spice's tools.
    ///
    /// This is one dispatch. A call past the cap, and a `call_id` already run in the round, do
    /// not reach here, so `tools_used` counts dispatches and not those.
    async fn run_spice_tool_call(&self, call: &FunctionToolCall) -> ToolOutcome {
        tracing::info!(
            target: "task_history",
            progress = Progress::log()
                .id(call.id.clone())
                .title(format!("Calling '{}' tool", call.name))
                .content(call.arguments.clone())
                .to_jsonl(),
        );
        let outcome = self.call_tool(call).await;
        let context = RequestContext::current(AsyncMarker::new().await);
        crate::model::add_tools_used(&context, 1);
        outcome
    }

    /// The first [`MAX_SPICE_TOOL_CALLS_PER_RESPONSE`] of `calls`, keyed by `call_id`.
    /// The first item with an id is the one that runs. A later item that reuses the id does not
    /// replace it. `calls` is already this round's Spice calls, in output order, so building this
    /// walks at most 64 of them.
    fn admitted_spice_calls<'a>(
        calls: &[&'a FunctionToolCall],
    ) -> HashMap<&'a str, &'a FunctionToolCall> {
        let mut admitted = HashMap::new();
        for call in calls
            .iter()
            .copied()
            .take(MAX_SPICE_TOOL_CALLS_PER_RESPONSE)
        {
            admitted.entry(call.call_id.as_str()).or_insert(call);
        }
        admitted
    }

    /// Runs the admitted call for `call_id`, or the not-run error when `call_id` is not in
    /// `admitted`. `requested` is how many Spice tool calls the round made.
    async fn dispatch_admitted(
        &self,
        call_id: &str,
        admitted: &HashMap<&str, &FunctionToolCall>,
        requested: usize,
    ) -> ToolOutcome {
        if let Some(call) = admitted.get(call_id) {
            self.run_spice_tool_call(call).await
        } else {
            ToolOutcome::Failed(tool_call_not_run_message(requested))
        }
    }

    /// Runs `call`, a call to one of Spice's tools, if `runs_left` allows one more run in its
    /// round, which made `requested` Spice tool calls. A call past the round's
    /// [`MAX_SPICE_TOOL_CALLS_PER_RESPONSE`] does not run, and fails with
    /// [`tool_call_not_run_message`]. The caller walks the round's Spice calls in output order,
    /// so the calls that run are the first [`MAX_SPICE_TOOL_CALLS_PER_RESPONSE`].
    async fn run_spice_tool_call_within_limit(
        &self,
        call: &FunctionToolCall,
        runs_left: &mut usize,
        requested: usize,
    ) -> ToolOutcome {
        if *runs_left == 0 {
            return ToolOutcome::Failed(tool_call_not_run_message(requested));
        }
        *runs_left -= 1;
        self.run_spice_tool_call(call).await
    }

    /// Runs the Spice tool calls in `output`, one round's output from model `model`, and returns
    /// their outcomes by `call_id`, none when `output` calls no Spice tool. Only the first
    /// [`MAX_SPICE_TOOL_CALLS_PER_RESPONSE`] calls run. A repeated `call_id` is not run again
    /// and keeps the outcome already stored.
    async fn run_spice_tool_calls(
        &self,
        model: &str,
        output: &[OutputItem],
    ) -> HashMap<String, ToolOutcome> {
        let calls = self.spice_tool_calls(output);
        warn_if_over_tool_call_limit(model, calls.len());
        let mut runs_left = MAX_SPICE_TOOL_CALLS_PER_RESPONSE;
        let mut outcomes = HashMap::with_capacity(calls.len());
        for call in &calls {
            // A later item that reuses a `call_id` keeps the outcome already stored. Replacing
            // it would turn a call that ran into the not-run error once the cap is reached.
            // The item still takes one of the round's run slots, so a duplicate does not leave
            // a slot for a distinct id that comes after it.
            if outcomes.contains_key(&call.call_id) {
                if runs_left > 0 {
                    runs_left -= 1;
                }
                continue;
            }
            let outcome = self
                .run_spice_tool_call_within_limit(call, &mut runs_left, calls.len())
                .await;
            outcomes.insert(call.call_id.clone(), outcome);
        }
        outcomes
    }

    /// `output`, one round's output, as the client receives it: each Spice tool call becomes an
    /// `mcp_call` item with its outcome, from `outcomes` by `call_id`.
    fn client_output<'a>(
        &'a self,
        output: Vec<OutputItem>,
        outcomes: &'a HashMap<String, ToolOutcome>,
    ) -> impl Iterator<Item = OutputItem> + 'a {
        output.into_iter().map(|item| match item {
            OutputItem::FunctionCall(call) if self.as_spiced_tool(&call.name).is_some() => {
                OutputItem::McpCall(mcp_call(
                    &call,
                    outcomes.get(&call.call_id),
                    MCPToolCallStatus::Incomplete,
                ))
            }
            item => item,
        })
    }

    /// `item`, an item of one round's output, as input for the next round.
    fn replay_item(&self, item: &OutputItem) -> Option<Item> {
        match item {
            OutputItem::Message(message) => {
                Some(Item::Message(MessageItem::Output(message.clone())))
            }
            OutputItem::Reasoning(reasoning) => Some(Item::Reasoning(reasoning.clone())),
            OutputItem::FunctionCall(call) => {
                self.as_spiced_tool(&call.name).is_some().then(|| {
                    Item::FunctionCall(replayed_call(
                        call.call_id.clone(),
                        call.name.clone(),
                        call.arguments.clone(),
                    ))
                })
            }
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
            | OutputItem::FunctionCallOutput(_)
            | OutputItem::ComputerCallOutput(_)
            | OutputItem::CustomToolCallOutput(_)
            | OutputItem::ToolSearchCall(_)
            | OutputItem::ToolSearchOutput(_)
            | OutputItem::ApplyPatchCall(_)
            | OutputItem::ApplyPatchCallOutput(_)
            | OutputItem::McpApprovalRequest(_)
            | OutputItem::CustomToolCall(_)
            | OutputItem::Compaction(_) => None,
        }
    }

    /// The input for the round after one whose output is `output`, which ran Spice tools: `input`,
    /// then `output`, then the calls' results, from `outcomes` by `call_id`, paired with the calls
    /// by `call_id`.
    ///
    /// The round's output is replayed as is, item IDs included, so that a reasoning model gets
    /// each reasoning item back. Each Spice tool call is replayed as each later turn replays it
    /// (see [`replayed_call`]). The next round also gets the round's hosted tool results, such as a
    /// web search's.
    fn next_input(
        &self,
        mut input: Vec<InputItem>,
        output: &[OutputItem],
        outcomes: &HashMap<String, ToolOutcome>,
    ) -> Vec<InputItem> {
        input.extend(
            output
                .iter()
                .filter_map(|item| self.replay_item(item))
                .map(InputItem::Item),
        );
        input.extend(
            self.spice_tool_calls(output)
                .into_iter()
                .filter_map(|call| {
                    let outcome = outcomes.get(&call.call_id)?;
                    Some(InputItem::Item(Item::FunctionCallOutput(
                        FunctionCallOutputItemParam {
                            call_id: call.call_id.clone(),
                            output: FunctionCallOutput::Text(outcome.model_output()),
                            id: None,
                            status: None,
                        },
                    )))
                }),
        );

        input
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
        // The output of the rounds that ran Spice tools, as the client receives it, and their usage.
        let mut earlier_output = Vec::new();
        let mut earlier_usage = None;

        loop {
            let response = self.inner_responses.responses_request(req.clone()).await?;

            if tool_rounds_left != Some(0) {
                let outcomes = self
                    .run_spice_tool_calls(
                        req.model.as_deref().unwrap_or_default(),
                        &response.output,
                    )
                    .await;
                if !outcomes.is_empty() {
                    let next_input = self.next_input(
                        to_input_item(req.input.clone()),
                        &response.output,
                        &outcomes,
                    );
                    req = create_new_recursive_req(&req, next_input, response.usage.as_ref());
                    earlier_usage = combine_usage(earlier_usage, response.usage);
                    earlier_output.extend(self.client_output(response.output, &outcomes));
                    tool_rounds_left = tool_rounds_left.map(|rounds| rounds - 1);
                    if tool_rounds_left == Some(0) {
                        req = self.without_runtime_tools(req);
                    }
                    continue;
                }
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

    /// `req` for the round after the last that may run Spice tools: without Spice's tools, so
    /// that the model answers rather than ending the response with a Spice tool call nothing
    /// runs. `OpenAI` rejects an `allowed_tools` choice that names a tool `tools` lacks, and
    /// another provider may reject a `tool_choice` or `parallel_tool_calls` with no tools
    /// (issue #14821).
    fn without_runtime_tools(&self, mut req: CreateResponse) -> CreateResponse {
        let is_spice_tool =
            |name: Option<&str>| name.is_some_and(|name| self.as_spiced_tool(name).is_some());
        let tools = req
            .tools
            .take()
            .unwrap_or_default()
            .into_iter()
            .filter(
                |tool| !matches!(tool, ToolDefinition::Function(f) if is_spice_tool(Some(&f.name))),
            )
            .collect::<Vec<_>>();
        if tools.is_empty() {
            req.tool_choice = None;
            req.parallel_tool_calls = None;
            return req;
        }
        req.tools = Some(tools);
        if let Some(ToolChoiceParam::AllowedTools(allowed)) = &mut req.tool_choice {
            allowed
                .tools
                .retain(|tool| !is_spice_tool(tool.get("name").and_then(Value::as_str)));
            if allowed.tools.is_empty() {
                // The client allowed Spice's tools only, and none is left.
                req.tool_choice = Some(ToolChoiceParam::Mode(ToolChoiceOptions::None));
            }
        }
        req
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
/// `output_index`es that count the items it was sent across rounds.
///
/// A Spice tool call reaches the client as an `mcp_call` item, with the events `OpenAI` streams
/// for a hosted MCP server's call. The call's `function_call` events become
/// `response.mcp_call.in_progress` and `response.mcp_call_arguments.*` events as they arrive, and
/// `response.mcp_call.completed` or `.failed` and `response.output_item.done` follow once Spice
/// has run the call, after the round.
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
    /// The `mcp_call` item ID of each of the current round's Spice tool calls, by `output_index`.
    spice_calls: HashMap<u64, String>,
    /// The output items of earlier rounds, as the client was sent them.
    earlier_output: Vec<OutputItem>,
    /// The usage of earlier rounds.
    earlier_usage: Option<ResponseUsage>,
}

impl ClientStream {
    /// `event` as the client is to receive it: no event, one, or two for the start of a Spice tool
    /// call. `is_spice` tells whether an output item is a Spice tool call.
    fn events(
        &mut self,
        event: ResponseStreamEvent,
        is_spice: impl Fn(&OutputItem) -> bool,
    ) -> Vec<ResponseStreamEvent> {
        let events = match event {
            // A later round continues the response the client already has.
            ResponseStreamEvent::ResponseCreated(_)
            | ResponseStreamEvent::ResponseInProgress(_)
            | ResponseStreamEvent::ResponseQueued(_)
                if self.tool_rounds > 0 =>
            {
                return Vec::new();
            }
            ResponseStreamEvent::ResponseOutputItemAdded(added) => {
                let index = u64::from(added.output_index);
                self.output_indexes.insert(index, self.items_sent);
                self.items_sent += 1;
                match &added.item {
                    OutputItem::FunctionCall(call) if is_spice(&added.item) => {
                        let item = mcp_call(call, None, MCPToolCallStatus::InProgress);
                        let item_id = item.id.clone();
                        self.spice_calls.insert(index, item_id.clone());
                        vec![
                            ResponseStreamEvent::ResponseOutputItemAdded(
                                ResponseOutputItemAddedEvent {
                                    sequence_number: added.sequence_number,
                                    output_index: added.output_index,
                                    item: OutputItem::McpCall(item),
                                },
                            ),
                            ResponseStreamEvent::ResponseMCPCallInProgress(
                                ResponseMCPCallInProgressEvent {
                                    sequence_number: added.sequence_number,
                                    output_index: added.output_index,
                                    item_id,
                                },
                            ),
                        ]
                    }
                    _ => vec![ResponseStreamEvent::ResponseOutputItemAdded(added)],
                }
            }
            ResponseStreamEvent::ResponseFunctionCallArgumentsDelta(delta) => {
                match self.spice_calls.get(&u64::from(delta.output_index)) {
                    Some(item_id) => vec![ResponseStreamEvent::ResponseMCPCallArgumentsDelta(
                        ResponseMCPCallArgumentsDeltaEvent {
                            sequence_number: delta.sequence_number,
                            output_index: delta.output_index,
                            item_id: item_id.clone(),
                            delta: delta.delta,
                        },
                    )],
                    None => vec![ResponseStreamEvent::ResponseFunctionCallArgumentsDelta(
                        delta,
                    )],
                }
            }
            ResponseStreamEvent::ResponseFunctionCallArgumentsDone(done) => {
                match self.spice_calls.get(&u64::from(done.output_index)) {
                    Some(item_id) => vec![ResponseStreamEvent::ResponseMCPCallArgumentsDone(
                        ResponseMCPCallArgumentsDoneEvent {
                            sequence_number: done.sequence_number,
                            output_index: done.output_index,
                            item_id: item_id.clone(),
                            arguments: done.arguments,
                        },
                    )],
                    None => vec![ResponseStreamEvent::ResponseFunctionCallArgumentsDone(done)],
                }
            }
            // A Spice tool call ends here only when its round ended without a response, so the
            // call never ran.
            ResponseStreamEvent::ResponseOutputItemDone(done) => match &done.item {
                OutputItem::FunctionCall(call)
                    if self.spice_calls.contains_key(&u64::from(done.output_index)) =>
                {
                    vec![ResponseStreamEvent::ResponseOutputItemDone(
                        ResponseOutputItemDoneEvent {
                            sequence_number: done.sequence_number,
                            output_index: done.output_index,
                            item: OutputItem::McpCall(mcp_call(
                                call,
                                None,
                                MCPToolCallStatus::Incomplete,
                            )),
                        },
                    )]
                }
                _ => vec![ResponseStreamEvent::ResponseOutputItemDone(done)],
            },
            ResponseStreamEvent::ResponseCompleted(mut completed) => {
                self.finish(&mut completed.response);
                vec![ResponseStreamEvent::ResponseCompleted(completed)]
            }
            ResponseStreamEvent::ResponseIncomplete(mut incomplete) => {
                self.finish(&mut incomplete.response);
                vec![ResponseStreamEvent::ResponseIncomplete(incomplete)]
            }
            ResponseStreamEvent::ResponseFailed(mut failed) => {
                self.finish(&mut failed.response);
                vec![ResponseStreamEvent::ResponseFailed(failed)]
            }
            event => vec![event],
        };
        events
            .into_iter()
            .map(|event| self.numbered(event))
            .collect()
    }

    /// `event` with the client's `sequence_number`, and the client's `output_index` of the item
    /// it is about.
    fn numbered(&mut self, event: ResponseStreamEvent) -> ResponseStreamEvent {
        // Every event has a `sequence_number`, and every event about one output item has an
        // `output_index`. Renumbering them on the serialized event covers every event type.
        let Ok(mut value) = serde_json::to_value(&event) else {
            return event;
        };
        if let Some(index) = value.get("output_index").and_then(Value::as_u64)
            && let Some(client_index) = self.output_indexes.get(&index)
        {
            value["output_index"] = json!(client_index);
        }
        value["sequence_number"] = json!(self.sequence_number);
        self.sequence_number += 1;
        serde_json::from_value(value).unwrap_or(event)
    }

    /// Whether `event` ends one of the current round's Spice tool calls. The client is sent that
    /// end once Spice has run the call.
    fn ends_spice_tool_call(&self, event: &ResponseStreamEvent) -> bool {
        matches!(
            event,
            ResponseStreamEvent::ResponseOutputItemDone(done)
                if self.spice_calls.contains_key(&u64::from(done.output_index))
        )
    }

    /// The events that end `call`, the Spice tool call at `output_index`, which ran with
    /// `outcome`.
    fn tool_call_ended(
        &mut self,
        call: &FunctionToolCall,
        output_index: u32,
        outcome: &ToolOutcome,
    ) -> Vec<ResponseStreamEvent> {
        let item = mcp_call(call, Some(outcome), MCPToolCallStatus::Incomplete);
        let item_id = item.id.clone();
        let ended = match outcome {
            ToolOutcome::Completed(_) => {
                ResponseStreamEvent::ResponseMCPCallCompleted(ResponseMCPCallCompletedEvent {
                    sequence_number: 0,
                    output_index,
                    item_id,
                })
            }
            ToolOutcome::Failed(_) => {
                ResponseStreamEvent::ResponseMCPCallFailed(ResponseMCPCallFailedEvent {
                    sequence_number: 0,
                    output_index,
                    item_id,
                })
            }
        };
        let done = ResponseStreamEvent::ResponseOutputItemDone(ResponseOutputItemDoneEvent {
            sequence_number: 0,
            output_index,
            item: OutputItem::McpCall(item),
        });
        vec![self.numbered(ended), self.numbered(done)]
    }

    /// `output`, the current round's, as the client was sent it: each Spice tool call an
    /// `mcp_call` item with its outcome, from `outcomes` by `call_id`.
    fn client_output(
        &self,
        output: Vec<OutputItem>,
        outcomes: &HashMap<String, ToolOutcome>,
    ) -> Vec<OutputItem> {
        output
            .into_iter()
            .zip(0_u64..)
            .map(|(item, index)| match item {
                OutputItem::FunctionCall(call) if self.spice_calls.contains_key(&index) => {
                    OutputItem::McpCall(mcp_call(
                        &call,
                        outcomes.get(&call.call_id),
                        MCPToolCallStatus::Incomplete,
                    ))
                }
                item => item,
            })
            .collect()
    }

    /// Ends a round that ran Spice tools, with `outcomes` by `call_id`: the next round continues
    /// the client's response.
    fn end_round(&mut self, response: Response, outcomes: &HashMap<String, ToolOutcome>) {
        let output = self.client_output(response.output, outcomes);
        self.earlier_output.extend(output);
        self.earlier_usage = combine_usage(self.earlier_usage.take(), response.usage);
        self.output_indexes.clear();
        self.spice_calls.clear();
        self.tool_rounds += 1;
    }

    /// Makes `response`, the last round's, the response to the whole loop, as
    /// [`with_earlier_rounds`] does.
    fn finish(&mut self, response: &mut Response) {
        let mut output = std::mem::take(&mut self.earlier_output);
        output.extend(self.client_output(std::mem::take(&mut response.output), &HashMap::new()));
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

/// [`ended_response`], owned.
fn into_ended_response(event: ResponseStreamEvent) -> Option<Response> {
    match event {
        ResponseStreamEvent::ResponseCompleted(completed) => Some(completed.response),
        ResponseStreamEvent::ResponseIncomplete(incomplete) => Some(incomplete.response),
        _ => None,
    }
}

/// Sends `events` to the client. `false` when the client went away.
async fn send(
    sender: &mpsc::Sender<Result<ResponseStreamEvent, OpenAIError>>,
    events: Vec<ResponseStreamEvent>,
) -> bool {
    for event in events {
        if sender.send(Ok(event)).await.is_err() {
            return false;
        }
    }
    true
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
                    let is_spice = |item: &OutputItem| runs_tools && model.is_spice_tool_call(item);
                    // The round's last event, held back until it is known whether the round runs
                    // Spice tools.
                    let mut last_event = None;
                    // The round's events from the end of its first Spice tool call on, held back
                    // until Spice has run the call, so that the client is sent each item whole
                    // before the next.
                    let mut held = Vec::new();

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
                        if !held.is_empty() || client_stream.ends_spice_tool_call(&event) {
                            held.push(event);
                        } else if !send(&sender, client_stream.events(event, is_spice)).await {
                            // The client went away.
                            return;
                        }
                    }

                    let Some(last_event) = last_event else {
                        // The round ended without a response, so its Spice tool calls never run.
                        let events = held
                            .into_iter()
                            .flat_map(|event| client_stream.events(event, is_spice))
                            .collect();
                        send(&sender, events).await;
                        break;
                    };
                    if ended_response(&last_event)
                        .is_none_or(|response| model.spice_tool_calls(&response.output).is_empty())
                    {
                        // The round called no Spice tool, so it is the last.
                        send(&sender, client_stream.events(last_event, is_spice)).await;
                        break;
                    }
                    let Some(response) = into_ended_response(last_event) else {
                        break;
                    };

                    // Run each call as the client reaches its end, then send the round's events
                    // that followed it. Which calls run is fixed by output order, not by the
                    // order those end events arrived: a later call can finish streaming first.
                    let spice_calls = model.spice_tool_calls(&response.output);
                    let requested = spice_calls.len();
                    warn_if_over_tool_call_limit(
                        req.model.as_deref().unwrap_or_default(),
                        requested,
                    );
                    // Built once for the round. A done event must not scan `output` again: a
                    // response can name far more calls than the 64 that run.
                    let admitted = ToolUsingResponses::admitted_spice_calls(&spice_calls);
                    let mut outcomes: HashMap<String, ToolOutcome> = HashMap::new();
                    for event in held {
                        let ended_call = match &event {
                            ResponseStreamEvent::ResponseOutputItemDone(done)
                                if client_stream.ends_spice_tool_call(&event) =>
                            {
                                match &done.item {
                                    OutputItem::FunctionCall(call) => {
                                        Some((call, done.output_index))
                                    }
                                    _ => None,
                                }
                            }
                            _ => None,
                        };
                        let events = match ended_call {
                            Some((call, output_index)) => {
                                // A repeated done event reuses the outcome. The dispatch is the
                                // first admitted item with that id, not whichever done event
                                // arrived first: a later item can reuse the id with different
                                // arguments.
                                let outcome = if let Some(outcome) = outcomes.get(&call.call_id) {
                                    outcome.clone()
                                } else {
                                    let outcome = model
                                        .dispatch_admitted(&call.call_id, &admitted, requested)
                                        .await;
                                    outcomes.insert(call.call_id.clone(), outcome.clone());
                                    outcome
                                };
                                client_stream.tool_call_ended(call, output_index, &outcome)
                            }
                            None => client_stream.events(event, is_spice),
                        };
                        if !send(&sender, events).await {
                            return;
                        }
                    }
                    // A call whose end the round did not stream runs too, under the same
                    // output-order selection: the next round needs every call's result.
                    for call in spice_calls {
                        if !outcomes.contains_key(&call.call_id) {
                            let outcome = model
                                .dispatch_admitted(&call.call_id, &admitted, requested)
                                .await;
                            outcomes.insert(call.call_id.clone(), outcome);
                        }
                    }

                    let next_input = model.next_input(
                        to_input_item(req.input.clone()),
                        &response.output,
                        &outcomes,
                    );
                    req = create_new_recursive_req(&req, next_input, response.usage.as_ref());
                    client_stream.end_round(response, &outcomes);
                    tool_rounds_left = tool_rounds_left.map(|rounds| rounds - 1);
                    if tool_rounds_left == Some(0) {
                        req = model.without_runtime_tools(req);
                    }
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
            phase: None,
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
    use crate::model::ModelContextExtension;
    use crate::model::wrapper::responses::ResponsesWrapper;
    use async_openai::types::responses::{
        CreateResponseArgs, ToolChoiceAllowed, ToolChoiceCustom, ToolChoiceFunction,
        ToolChoiceTypes,
    };
    use parking_lot::Mutex;
    use runtime_request_context::Protocol;
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

    /// A `failing_tool` Spice tool whose every call fails.
    struct FailingTool;

    #[async_trait]
    impl SpiceModelTool for FailingTool {
        fn name(&self) -> Cow<'_, str> {
            "failing_tool".into()
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
            Err("no such table".into())
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

        /// The names of the tools request `round` offers, and its `tool_choice`.
        fn tools(&self, round: usize) -> (Vec<String>, Value) {
            let request = &self.requests.lock()[round];
            let names = request
                .tools
                .iter()
                .flatten()
                .map(|tool| get_tool_name(tool).to_string())
                .collect();
            (
                names,
                serde_json::to_value(&request.tool_choice).expect("a serializable tool choice"),
            )
        }

        /// The `parallel_tool_calls` of request `round`.
        fn parallel_tool_calls(&self, round: usize) -> Option<bool> {
            self.requests.lock()[round].parallel_tool_calls
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

    /// The call at `index` of round `round`, as Spice replays it to the provider: without its `id`.
    fn replayed_item(round: usize, index: usize, name: &str) -> Value {
        let mut call = item(round, index, name, "completed");
        if let Some(call) = call.as_object_mut() {
            call.remove("id");
        }
        call
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
            vec![
                Arc::clone(tool) as Arc<dyn SpiceModelTool>,
                Arc::new(FailingTool),
            ],
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

    async fn collect_events(stream: ResponseStream) -> Vec<Value> {
        stream
            .map(|event| {
                serde_json::to_value(event.expect("an event, not an error"))
                    .expect("a serializable event")
            })
            .collect()
            .await
    }

    /// The events the client receives for a streamed request.
    async fn stream_through_spice(
        provider: &Arc<ScriptedResponses>,
        tool: &Arc<ListDatasets>,
        recursion_limit: Option<usize>,
    ) -> Vec<Value> {
        collect_events(
            model(provider, tool, recursion_limit)
                .responses_stream(request(true))
                .await
                .expect("a response stream"),
        )
        .await
    }

    /// Each event, as its type plus the item's type and name, or else the output index the event
    /// belongs to.
    fn summary(events: &[Value]) -> Vec<String> {
        events
            .iter()
            .map(|event| {
                let event_type = event["type"].as_str().unwrap_or_default();
                match (
                    event["item"]["type"].as_str(),
                    event["item"]["name"].as_str(),
                    event["output_index"].as_u64(),
                ) {
                    (Some(item_type), Some(name), _) => format!("{event_type} {item_type} {name}"),
                    (Some(item_type), None, _) => format!("{event_type} {item_type}"),
                    (None, _, Some(index)) => format!("{event_type} #{index}"),
                    (None, _, None) => event_type.to_string(),
                }
            })
            .collect()
    }

    /// The summary of the events a client receives for the Spice tool call to `name` at `index`.
    fn spice_tool_call_summary(index: u64, name: &str) -> Vec<String> {
        let ended = if name == "failing_tool" {
            "failed"
        } else {
            "completed"
        };
        vec![
            format!("response.output_item.added mcp_call {name}"),
            format!("response.mcp_call.in_progress #{index}"),
            format!("response.mcp_call_arguments.delta #{index}"),
            format!("response.mcp_call_arguments.done #{index}"),
            format!("response.mcp_call.{ended} #{index}"),
            format!("response.output_item.done mcp_call {name}"),
        ]
    }

    /// The summary of the events a client receives for the text message at `index`.
    fn message_summary(index: u64) -> Vec<String> {
        vec![
            "response.output_item.added message".to_string(),
            format!("response.output_text.delta #{index}"),
            "response.output_item.done message".to_string(),
        ]
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

    /// The `mcp_call` item the client receives for the call at `index` of round `round`, once
    /// Spice has run it.
    fn mcp_item(round: usize, index: usize, name: &str) -> Value {
        let (status, output) = if name == "failing_tool" {
            (
                "failed",
                "Failed to call the tool failing_tool. An error occurred: no such table",
            )
        } else {
            ("completed", "[\"taxi_trips\"]")
        };
        json!({
            "type": "mcp_call", "id": format!("mcp_spice_call_{round}_{index}"),
            "server_label": "spice", "name": name, "arguments": "{}", "approval_request_id": null,
            "output": output, "error": null, "status": status,
        })
    }

    // regression test for #14905
    #[tokio::test]
    async fn test_streamed_spice_tool_call_reaches_the_client_as_an_mcp_call() {
        let provider = ScriptedResponses::new(vec![vec!["list_datasets"]]);
        let tool = Arc::new(ListDatasets(AtomicUsize::new(0)));

        let events = stream_through_spice(&provider, &tool, None).await;
        assert_eq!(
            summary(&events),
            [
                vec!["response.created".to_string()],
                spice_tool_call_summary(0, "list_datasets"),
                message_summary(1),
                vec!["response.completed".to_string()],
            ]
            .concat()
        );
        assert_eq!(tool.0.load(Ordering::SeqCst), 1);

        // The call starts in progress, and ends with its result once Spice has run it. Each event
        // about it names the `mcp_call` item.
        assert_eq!(
            events[1]["item"],
            json!({
                "type": "mcp_call", "id": "mcp_spice_call_0_0", "server_label": "spice",
                "name": "list_datasets", "arguments": "", "approval_request_id": null,
                "output": null, "error": null, "status": "in_progress",
            })
        );
        for event in &events[2..6] {
            assert_eq!(event["item_id"], "mcp_spice_call_0_0", "{event}");
        }
        assert_eq!(events[4]["arguments"], "{}");
        assert_eq!(events[6]["item"], mcp_item(0, 0, "list_datasets"));
        assert_eq!(
            events[10]["response"]["output"],
            json!([
                mcp_item(0, 0, "list_datasets"),
                item(1, 0, "message", "completed")
            ])
        );

        // The follow-up request replays the round's output, and pairs the call with its result by
        // `call_id`.
        assert_eq!(provider.requests.lock().len(), 2);
        assert_eq!(
            provider.input(1),
            json!([
                user_message(),
                replayed_item(0, 0, "list_datasets"),
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
                vec![
                    "response.created".to_string(),
                    "response.output_item.added function_call client_tool".to_string(),
                    "response.function_call_arguments.delta #0".to_string(),
                    "response.function_call_arguments.done #0".to_string(),
                    "response.output_item.done function_call client_tool".to_string(),
                ],
                spice_tool_call_summary(1, "list_datasets"),
                message_summary(2),
                vec!["response.completed".to_string()],
            ]
            .concat()
        );
        assert_eq!(tool.0.load(Ordering::SeqCst), 1);
        assert_eq!(
            events
                .last()
                .map(|completed| completed["response"]["output"].clone()),
            Some(json!([
                item(0, 0, "client_tool", "completed"),
                mcp_item(0, 1, "list_datasets"),
                item(1, 0, "message", "completed")
            ]))
        );

        // The next round has no result for the client's call, so it is not replayed.
        assert_eq!(
            provider.input(1),
            json!([
                user_message(),
                replayed_item(0, 1, "list_datasets"),
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
                vec!["response.created".to_string()],
                message_summary(0),
                spice_tool_call_summary(1, "list_datasets"),
                message_summary(2),
                vec!["response.completed".to_string()],
            ]
            .concat()
        );
        assert_eq!(
            events
                .iter()
                .map(|event| event["sequence_number"].as_u64())
                .collect::<Vec<_>>(),
            (0..14).map(Some).collect::<Vec<_>>()
        );

        // The response completes as the last round's, with every item the client was sent and
        // every round's usage.
        let completed = &events[13]["response"];
        assert_eq!(completed["id"], "resp_1");
        assert_eq!(
            completed["output"],
            json!([
                item(0, 0, "message", "completed"),
                mcp_item(0, 1, "list_datasets"),
                item(1, 0, "message", "completed")
            ])
        );
        assert_eq!(completed["usage"]["total_tokens"], 22);

        assert_eq!(
            provider.input(1),
            json!([
                user_message(),
                item(0, 0, "message", "completed"),
                replayed_item(0, 1, "list_datasets"),
                tool_result(0, 1)
            ])
        );
    }

    #[tokio::test]
    async fn test_streamed_items_after_a_spice_tool_call_wait_for_its_end() {
        // Round 0 calls `list_datasets` twice, with a message between the calls.
        let provider =
            ScriptedResponses::new(vec![vec!["list_datasets", "message", "list_datasets"]]);
        let tool = Arc::new(ListDatasets(AtomicUsize::new(0)));

        let events = stream_through_spice(&provider, &tool, None).await;
        assert_eq!(
            summary(&events),
            [
                vec!["response.created".to_string()],
                spice_tool_call_summary(0, "list_datasets"),
                message_summary(1),
                spice_tool_call_summary(2, "list_datasets"),
                message_summary(3),
                vec!["response.completed".to_string()],
            ]
            .concat()
        );
        assert_eq!(tool.0.load(Ordering::SeqCst), 2);
        assert_eq!(
            events
                .last()
                .map(|completed| completed["response"]["output"].clone()),
            Some(json!([
                mcp_item(0, 0, "list_datasets"),
                item(0, 1, "message", "completed"),
                mcp_item(0, 2, "list_datasets"),
                item(1, 0, "message", "completed")
            ]))
        );
        assert_eq!(
            provider.input(1),
            json!([
                user_message(),
                replayed_item(0, 0, "list_datasets"),
                item(0, 1, "message", "completed"),
                replayed_item(0, 2, "list_datasets"),
                tool_result(0, 0),
                tool_result(0, 2)
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
                mcp_item(0, 1, "list_datasets"),
                item(1, 0, "message", "completed")
            ])
        );
        assert_eq!(response["usage"]["total_tokens"], 22);
        assert_eq!(
            provider.input(1),
            json!([
                user_message(),
                item(0, 0, "message", "completed"),
                replayed_item(0, 1, "list_datasets"),
                tool_result(0, 1)
            ])
        );
    }

    #[tokio::test]
    async fn test_failed_spice_tool_call() {
        for stream in [false, true] {
            let provider = ScriptedResponses::new(vec![vec!["failing_tool"]]);
            let tool = Arc::new(ListDatasets(AtomicUsize::new(0)));

            let output = if stream {
                let events = stream_through_spice(&provider, &tool, None).await;
                assert_eq!(
                    summary(&events)[1..7],
                    spice_tool_call_summary(0, "failing_tool")
                );
                events
                    .last()
                    .map(|completed| completed["response"]["output"].clone())
                    .unwrap_or_default()
            } else {
                serde_json::to_value(
                    model(&provider, &tool, None)
                        .responses_request(request(false))
                        .await
                        .expect("a response")
                        .output,
                )
                .expect("a serializable output")
            };

            // The client gets the failure the model was given.
            assert_eq!(
                output[0],
                mcp_item(0, 0, "failing_tool"),
                "stream: {stream}"
            );
            assert_eq!(
                provider.input(1)[2],
                json!({
                    "type": "function_call_output", "call_id": "call_0_0",
                    "output": "Failed to call the tool failing_tool. An error occurred: no such table",
                }),
                "stream: {stream}"
            );
        }
    }

    // A client that keeps its own history sends a response's output back as the next request's
    // input. The provider then sees the Spice tool calls as it saw them in the round after them.
    #[tokio::test]
    async fn test_spice_tool_calls_sent_back_reach_the_provider_as_it_saw_them() {
        for stream in [false, true] {
            let provider = ScriptedResponses::new(vec![vec!["message", "list_datasets"]]);
            let model = ToolUsingResponses::new(
                Arc::new(ResponsesWrapper::new(
                    Arc::clone(&provider) as Arc<dyn Responses>,
                    "scripted",
                    None,
                    vec![],
                )),
                vec![],
                vec![Arc::new(ListDatasets(AtomicUsize::new(0))) as Arc<dyn SpiceModelTool>],
                None,
            );
            let output = output_through_spice(&model, request(stream)).await;
            let follow_up =
                json!({ "type": "message", "role": "user", "content": "Which tool did you call?" });
            let mut input = vec![user_message()];
            input.extend(output.as_array().into_iter().flatten().cloned());
            input.push(follow_up.clone());
            output_through_spice(
                &model,
                serde_json::from_value(
                    json!({ "model": "scripted", "input": input, "stream": stream }),
                )
                .expect("a valid request"),
            )
            .await;

            // Turn 2 starts with all of turn 1's second round, item for item, so that the
            // provider's prompt cache serves it.
            let mut expected = provider.input(1).as_array().cloned().unwrap_or_default();
            expected.extend([item(1, 0, "message", "completed"), follow_up]);
            assert_eq!(
                provider.input(2),
                Value::Array(expected),
                "stream: {stream}"
            );
        }
    }

    /// The output of `request` through Spice, streamed or not.
    async fn output_through_spice(model: &ToolUsingResponses, request: CreateResponse) -> Value {
        if request.stream == Some(true) {
            collect_events(
                model
                    .responses_stream(request)
                    .await
                    .expect("a response stream"),
            )
            .await
            .last()
            .map(|completed| completed["response"]["output"].clone())
            .unwrap_or_default()
        } else {
            serde_json::to_value(
                model
                    .responses_request(request)
                    .await
                    .expect("a response")
                    .output,
            )
            .expect("a serializable output")
        }
    }

    // regression test for #14631
    #[tokio::test]
    async fn test_tool_rounds_follow_the_recursion_limit() {
        for stream in [false, true] {
            for limit in [1, 2, 3, 10] {
                let provider = ScriptedResponses::new(vec![vec!["list_datasets"]; limit]);
                let tool = Arc::new(ListDatasets(AtomicUsize::new(0)));

                let mut request = request(stream);
                request.parallel_tool_calls = Some(true);
                let output =
                    output_through_spice(&model(&provider, &tool, Some(limit)), request).await;

                // `limit` rounds run Spice tools. The round after them is offered none, so the
                // response ends with the model's answer rather than a call nothing runs.
                let label = format!("stream: {stream}, limit {limit}");
                assert_eq!(tool.0.load(Ordering::SeqCst), limit, "{label}");
                assert_eq!(provider.requests.lock().len(), limit + 1, "{label}");
                for round in 0..limit {
                    assert_eq!(
                        provider.tools(round).0,
                        ["failing_tool", "list_datasets"],
                        "{label}, round {round}"
                    );
                }
                assert_eq!(provider.tools(limit), (vec![], Value::Null), "{label}");
                // Nor `parallel_tool_calls`, which a request without tools may not carry (#14821).
                assert_eq!(
                    provider.parallel_tool_calls(limit - 1),
                    Some(true),
                    "{label}"
                );
                assert_eq!(provider.parallel_tool_calls(limit), None, "{label}");
                let mut types = vec![json!("mcp_call"); limit];
                types.push(json!("message"));
                assert_eq!(
                    output.as_array().map(|output| output
                        .iter()
                        .map(|item| item["type"].clone())
                        .collect::<Vec<_>>()),
                    Some(types),
                    "{label}"
                );
            }
        }
    }

    #[tokio::test]
    async fn test_the_round_after_the_recursion_limit_keeps_the_hosted_tools() {
        let allowed = |tools: &[Value]| {
            ToolChoiceParam::AllowedTools(ToolChoiceAllowed {
                mode: ToolChoiceAllowedMode::Auto,
                tools: tools.to_vec(),
            })
        };
        let list_datasets = json!({ "type": "function", "name": "list_datasets" });
        let web_search = json!({ "type": "web_search" });
        for (choice, last_choice) in [
            (
                allowed(&[list_datasets.clone(), web_search.clone()]),
                json!({ "type": "allowed_tools", "mode": "auto", "tools": [web_search] }),
            ),
            // The client allowed only Spice's tools, so the last round allows none.
            (allowed(&[list_datasets]), json!("none")),
            (
                ToolChoiceParam::Mode(ToolChoiceOptions::Auto),
                json!("auto"),
            ),
        ] {
            let provider = ScriptedResponses::new(vec![vec!["list_datasets"]]);
            let model = ToolUsingResponses::new(
                Arc::clone(&provider) as Arc<dyn Responses>,
                vec![OpenAIResponsesTools::WebSearch],
                vec![Arc::new(ListDatasets(AtomicUsize::new(0))) as Arc<dyn SpiceModelTool>],
                Some(1),
            );
            let mut request = request(false);
            request.tool_choice = Some(choice.clone());

            output_through_spice(&model, request).await;

            assert_eq!(
                provider.tools(0).0,
                ["list_datasets", "web_search"],
                "{choice:?}"
            );
            assert_eq!(
                provider.tools(1),
                (vec!["web_search".to_string()], last_choice),
                "{choice:?}"
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

    /// A provider whose first round is `calls` Spice tool calls. It streams `output_item.done`
    /// for `done_indexes` only, in that order, after every call has been added. Later rounds are
    /// a message. It records every request.
    struct DoneOrderResponses {
        calls: usize,
        done_indexes: Vec<usize>,
        /// When set, item `index`'s `call_id` is `call_0_{index % cycle}` rather than
        /// `call_0_{index}`.
        id_cycle: Option<usize>,
        /// When set, the first this many items share `call_0_0`. Later items keep distinct ids.
        shared_head: Option<usize>,
        /// When set, a completed call's arguments are `{"n":index}` rather than `{}`.
        indexed_arguments: bool,
        requests: Mutex<Vec<CreateResponse>>,
    }

    impl DoneOrderResponses {
        fn new(calls: usize, done_indexes: Vec<usize>) -> Arc<Self> {
            Self::with_id_cycle(calls, done_indexes, None)
        }

        fn with_id_cycle(
            calls: usize,
            done_indexes: Vec<usize>,
            id_cycle: Option<usize>,
        ) -> Arc<Self> {
            Self::with_ids(calls, done_indexes, id_cycle, None, false)
        }

        /// `with_id_cycle`, and each completed call's arguments are `{"n":index}`.
        fn with_indexed_cycle(
            calls: usize,
            done_indexes: Vec<usize>,
            id_cycle: usize,
        ) -> Arc<Self> {
            Self::with_ids(calls, done_indexes, Some(id_cycle), None, true)
        }

        /// The first `shared_head` items share one `call_id`. The rest each have their own.
        fn with_shared_head(
            calls: usize,
            done_indexes: Vec<usize>,
            shared_head: usize,
        ) -> Arc<Self> {
            Self::with_ids(calls, done_indexes, None, Some(shared_head), false)
        }

        fn with_ids(
            calls: usize,
            done_indexes: Vec<usize>,
            id_cycle: Option<usize>,
            shared_head: Option<usize>,
            indexed_arguments: bool,
        ) -> Arc<Self> {
            Arc::new(Self {
                calls,
                done_indexes,
                id_cycle,
                shared_head,
                indexed_arguments,
                requests: Mutex::new(Vec::new()),
            })
        }

        fn input(&self, round: usize) -> Value {
            serde_json::to_value(&self.requests.lock()[round].input).expect("a serializable input")
        }
    }

    #[async_trait]
    impl Responses for DoneOrderResponses {
        async fn health(&self) -> Result<(), ResponsesError> {
            Ok(())
        }

        async fn responses_stream(
            &self,
            req: CreateResponse,
        ) -> Result<ResponseStream, OpenAIError> {
            let mut requests = self.requests.lock();
            let round = requests.len();
            requests.push(req);
            drop(requests);
            let events = if round == 0 {
                let names = vec!["list_datasets"; self.calls];
                done_order_events(
                    0,
                    &names,
                    &self.done_indexes,
                    self.id_cycle,
                    self.shared_head,
                    self.indexed_arguments,
                )
            } else {
                round_events(round, &["message"])
            };
            Ok(Box::pin(futures::stream::iter(events.into_iter().map(Ok))))
        }

        async fn responses_request(&self, req: CreateResponse) -> Result<Response, OpenAIError> {
            let mut requests = self.requests.lock();
            let round = requests.len();
            requests.push(req);
            drop(requests);
            let names = if round == 0 {
                vec!["list_datasets"; self.calls]
            } else {
                vec!["message"]
            };
            let output_items: Vec<Value> = names
                .iter()
                .enumerate()
                .map(|(index, name)| {
                    item_call(
                        round,
                        index,
                        name,
                        "completed",
                        self.id_cycle,
                        self.shared_head,
                        self.indexed_arguments,
                    )
                })
                .collect();
            Ok(
                serde_json::from_value(response(round, "completed", &output_items))
                    .expect("a valid response"),
            )
        }
    }

    /// `call_{round}_{index}`, or `call_{round}_{index % cycle}` when `id_cycle` is set.
    /// The first `shared_head` items share `call_{round}_0`.
    fn call_id_for(
        round: usize,
        index: usize,
        id_cycle: Option<usize>,
        shared_head: Option<usize>,
    ) -> String {
        if shared_head.is_some_and(|head| index < head) {
            return format!("call_{round}_0");
        }
        let n = match id_cycle {
            Some(cycle) => index % cycle,
            None => index,
        };
        format!("call_{round}_{n}")
    }

    /// [`item`] with `call_id` from [`call_id_for`].
    fn item_call(
        round: usize,
        index: usize,
        name: &str,
        status: &str,
        id_cycle: Option<usize>,
        shared_head: Option<usize>,
        indexed_arguments: bool,
    ) -> Value {
        let mut value = item(round, index, name, status);
        if let Some(object) = value.as_object_mut()
            && object.contains_key("call_id")
        {
            object.insert(
                "call_id".to_string(),
                Value::String(call_id_for(round, index, id_cycle, shared_head)),
            );
            if indexed_arguments && status == "completed" {
                object.insert(
                    "arguments".to_string(),
                    json!(format!(r#"{{"n":{index}}}"#)),
                );
            }
        }
        value
    }

    /// Round `round`'s stream: every item is added first, then `output_item.done` for
    /// `done_indexes` in that order. The completed response still lists every item.
    /// `id_cycle` reuses `call_id`s across items.
    fn done_order_events(
        round: usize,
        items: &[&str],
        done_indexes: &[usize],
        id_cycle: Option<usize>,
        shared_head: Option<usize>,
        indexed_arguments: bool,
    ) -> Vec<ResponseStreamEvent> {
        let mut events = vec![json!({
            "type": "response.created", "response": response(round, "in_progress", &[]),
        })];
        for (index, name) in items.iter().enumerate() {
            let item_id = item(round, index, name, "completed")["id"].clone();
            events.push(json!({
                "type": "response.output_item.added", "output_index": index,
                "item": item_call(
                    round, index, name, "in_progress", id_cycle, shared_head, indexed_arguments,
                ),
            }));
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
        for index in done_indexes {
            events.push(json!({
                "type": "response.output_item.done", "output_index": index,
                "item": item_call(
                    round,
                    *index,
                    items[*index],
                    "completed",
                    id_cycle,
                    shared_head,
                    indexed_arguments,
                ),
            }));
        }
        let output_items: Vec<Value> = items
            .iter()
            .enumerate()
            .map(|(index, name)| {
                item_call(
                    round,
                    index,
                    name,
                    "completed",
                    id_cycle,
                    shared_head,
                    indexed_arguments,
                )
            })
            .collect();
        events.push(json!({
            "type": "response.completed",
            "response": response(round, "completed", &output_items),
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

    fn function_outputs(input: &Value) -> HashMap<String, String> {
        input
            .as_array()
            .expect("the next round's input is a list")
            .iter()
            .filter_map(|item| {
                if item["type"] != "function_call_output" {
                    return None;
                }
                Some((
                    item["call_id"].as_str()?.to_string(),
                    item["output"].as_str()?.to_string(),
                ))
            })
            .collect()
    }

    /// A streamed round runs the first 64 Spice calls in output order even when their
    /// `output_item.done` events arrive in another order, or when an early call's end is not
    /// streamed.
    #[tokio::test]
    async fn streamed_tool_calls_past_the_limit_follow_output_order() {
        let calls = MAX_SPICE_TOOL_CALLS_PER_RESPONSE + 1;
        // Done events for calls 1..=64, then call 0: arrival order would run call 64 and skip
        // call 0. Output order runs call 0 and skips call 64.
        let reordered: Vec<usize> = (1..calls).chain(std::iter::once(0)).collect();
        // Call 0's end is not streamed. The fallback still runs it, and still skips call 64.
        let missing_first: Vec<usize> = (1..calls).collect();

        for done_indexes in [reordered, missing_first] {
            let provider = DoneOrderResponses::new(calls, done_indexes.clone());
            let tool = Arc::new(ListDatasets(AtomicUsize::new(0)));
            let events = collect_events(
                ToolUsingResponses::new(
                    Arc::clone(&provider) as Arc<dyn Responses>,
                    vec![],
                    vec![Arc::clone(&tool) as Arc<dyn SpiceModelTool>],
                    None,
                )
                .responses_stream(request(true))
                .await
                .expect("a response stream"),
            )
            .await;
            assert!(
                events
                    .last()
                    .is_some_and(|event| event["type"] == "response.completed"),
                "done order {done_indexes:?} did not finish the stream"
            );

            let outputs = function_outputs(&provider.input(1));
            let ran = "[\"taxi_trips\"]";
            let skipped = tool_call_not_run_message(calls);
            assert_eq!(
                outputs.get("call_0_0").map(String::as_str),
                Some(ran),
                "done order {done_indexes:?}"
            );
            assert_eq!(
                outputs
                    .get(&format!("call_0_{}", MAX_SPICE_TOOL_CALLS_PER_RESPONSE - 1))
                    .map(String::as_str),
                Some(ran),
                "done order {done_indexes:?}"
            );
            assert_eq!(
                outputs
                    .get(&format!("call_0_{MAX_SPICE_TOOL_CALLS_PER_RESPONSE}"))
                    .map(String::as_str),
                Some(skipped.as_str()),
                "done order {done_indexes:?}"
            );
            assert_eq!(
                tool.0.load(Ordering::SeqCst),
                MAX_SPICE_TOOL_CALLS_PER_RESPONSE,
                "done order {done_indexes:?}"
            );
        }
    }

    /// Runs `provider`'s stream and returns how many times `list_datasets` ran, plus the next
    /// round's function outputs by `call_id`.
    async fn streamed_list_datasets(
        provider: &Arc<DoneOrderResponses>,
    ) -> (usize, HashMap<String, String>) {
        let tool = Arc::new(ListDatasets(AtomicUsize::new(0)));
        let events = collect_events(
            ToolUsingResponses::new(
                Arc::clone(provider) as Arc<dyn Responses>,
                vec![],
                vec![Arc::clone(&tool) as Arc<dyn SpiceModelTool>],
                None,
            )
            .responses_stream(request(true))
            .await
            .expect("a response stream"),
        )
        .await;
        assert!(
            events
                .last()
                .is_some_and(|event| event["type"] == "response.completed"),
            "the stream did not finish"
        );
        (
            tool.0.load(Ordering::SeqCst),
            function_outputs(&provider.input(1)),
        )
    }

    /// A repeated `output_item.done`, or a later item that reuses an admitted `call_id`, does not
    /// run the tool again.
    #[tokio::test]
    async fn streamed_tool_calls_do_not_rerun_a_reused_call_id() {
        let cycle = MAX_SPICE_TOOL_CALLS_PER_RESPONSE;
        let ran = "[\"taxi_trips\"]";

        // 65 distinct calls. Each of the first 64 done events is repeated, then call 64's once.
        let repeated_dones: Vec<usize> = (0..cycle)
            .cycle()
            .take(cycle * 16)
            .chain(std::iter::once(cycle))
            .collect();
        let (ran_count, outputs) =
            streamed_list_datasets(&DoneOrderResponses::new(cycle + 1, repeated_dones)).await;
        let skipped = tool_call_not_run_message(cycle + 1);
        assert_eq!(ran_count, cycle, "repeated done events");
        assert_eq!(
            outputs.get("call_0_0").map(String::as_str),
            Some(ran),
            "repeated done events"
        );
        assert_eq!(
            outputs
                .get(&format!("call_0_{}", cycle - 1))
                .map(String::as_str),
            Some(ran),
            "repeated done events"
        );
        assert_eq!(
            outputs.get(&format!("call_0_{cycle}")).map(String::as_str),
            Some(skipped.as_str()),
            "repeated done events"
        );

        // Four times as many items as the cap, call ids cycling through the first 64, one done
        // event each. Every item's id is in the runnable set; only the first use of each runs.
        let reused = cycle * 4;
        let (ran_count, outputs) = streamed_list_datasets(&DoneOrderResponses::with_id_cycle(
            reused,
            (0..reused).collect(),
            Some(cycle),
        ))
        .await;
        assert_eq!(ran_count, cycle, "reused call ids");
        assert_eq!(outputs.len(), cycle, "reused call ids");
        assert_eq!(
            outputs.get("call_0_0").map(String::as_str),
            Some(ran),
            "reused call ids"
        );
        assert_eq!(
            outputs
                .get(&format!("call_0_{}", cycle - 1))
                .map(String::as_str),
            Some(ran),
            "reused call ids"
        );
    }

    /// The first 64 items share one admitted `call_id`, so the tool runs once. The next 64 have
    /// distinct ids and are not run. `tools_used` counts the dispatch, not those rejections.
    #[tokio::test]
    async fn streamed_reused_admitted_id_does_not_count_rejected_calls() {
        let cap = MAX_SPICE_TOOL_CALLS_PER_RESPONSE;
        let calls = cap * 2;
        let provider = DoneOrderResponses::with_shared_head(calls, (0..calls).collect(), cap);
        let extension = ModelContextExtension::new();
        let context = Arc::new(RequestContext::builder(Protocol::Http).build());
        context.insert_extension(extension.clone());

        let (ran_count, outputs) = context
            .scope(async move { streamed_list_datasets(&provider).await })
            .await;

        let ran = "[\"taxi_trips\"]";
        let skipped = tool_call_not_run_message(calls);
        assert_eq!(ran_count, 1);
        assert_eq!(extension.tools_used(), 1);
        assert_eq!(outputs.len(), cap + 1);
        assert_eq!(outputs.get("call_0_0").map(String::as_str), Some(ran));
        assert_eq!(
            outputs.get(&format!("call_0_{cap}")).map(String::as_str),
            Some(skipped.as_str())
        );
        assert_eq!(
            outputs
                .get(&format!("call_0_{}", calls - 1))
                .map(String::as_str),
            Some(skipped.as_str())
        );
    }

    /// One non-streaming round: how many times `list_datasets` ran, the `tools_used` count, and
    /// the next round's function outputs by `call_id`.
    async fn requested_list_datasets(
        provider: &Arc<DoneOrderResponses>,
    ) -> (usize, usize, HashMap<String, String>) {
        let tool = Arc::new(ListDatasets(AtomicUsize::new(0)));
        let extension = ModelContextExtension::new();
        let context = Arc::new(RequestContext::builder(Protocol::Http).build());
        context.insert_extension(extension.clone());
        let provider = Arc::clone(provider);
        let (ran_count, outputs) = context
            .scope(async move {
                let model = ToolUsingResponses::new(
                    Arc::clone(&provider) as Arc<dyn Responses>,
                    vec![],
                    vec![Arc::clone(&tool) as Arc<dyn SpiceModelTool>],
                    None,
                );
                model
                    .responses_request(request(false))
                    .await
                    .expect("a response");
                (
                    tool.0.load(Ordering::SeqCst),
                    function_outputs(&provider.input(1)),
                )
            })
            .await;
        (ran_count, extension.tools_used(), outputs)
    }

    /// A non-streaming item past the cap that reuses an earlier `call_id` does not replace that
    /// call's result with the not-run error. A repeated id still uses up one of the round's run
    /// slots, so 64 copies of one id followed by 64 distinct ids run the tool once.
    #[tokio::test]
    async fn nonstreamed_reused_call_id_keeps_its_result_past_the_limit() {
        let cap = MAX_SPICE_TOOL_CALLS_PER_RESPONSE;
        let ran = "[\"taxi_trips\"]";

        let tail = cap + 1;
        let (dispatches, tools_used, outputs) = requested_list_datasets(
            &DoneOrderResponses::with_id_cycle(tail, Vec::new(), Some(cap)),
        )
        .await;
        assert_eq!(dispatches, cap, "reused tail");
        assert_eq!(tools_used, cap, "reused tail");
        assert_eq!(
            outputs.get("call_0_0").map(String::as_str),
            Some(ran),
            "reused tail"
        );
        assert!(
            outputs
                .values()
                .all(|output| output != &tool_call_not_run_message(tail)),
            "reused tail replaced a result: {outputs:?}"
        );

        let head = cap * 2;
        let skipped = tool_call_not_run_message(head);
        let (dispatches, tools_used, outputs) =
            requested_list_datasets(&DoneOrderResponses::with_shared_head(head, Vec::new(), cap))
                .await;
        assert_eq!(dispatches, 1, "shared head");
        assert_eq!(tools_used, 1, "shared head");
        assert_eq!(outputs.len(), cap + 1, "shared head");
        assert_eq!(
            outputs.get("call_0_0").map(String::as_str),
            Some(ran),
            "shared head"
        );
        assert_eq!(
            outputs.get(&format!("call_0_{cap}")).map(String::as_str),
            Some(skipped.as_str()),
            "shared head"
        );
    }

    /// Records the argument string of each `list_datasets` call.
    struct RecordingList(Mutex<Vec<String>>);

    #[async_trait]
    impl SpiceModelTool for RecordingList {
        fn name(&self) -> Cow<'_, str> {
            "list_datasets".into()
        }

        fn description(&self) -> Option<Cow<'_, str>> {
            None
        }

        fn parameters(&self) -> Option<Value> {
            None
        }

        async fn call(&self, arg: &str) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
            self.0.lock().push(arg.to_string());
            Ok(json!(["taxi_trips"]))
        }
    }

    /// A later item that reuses an admitted `call_id`, and whose `output_item.done` arrives
    /// first, does not run its own arguments. The dispatch is the first admitted item's.
    #[tokio::test]
    async fn streamed_reordered_reused_id_runs_the_first_admitted_arguments() {
        let cap = MAX_SPICE_TOOL_CALLS_PER_RESPONSE;
        let calls = cap + 1;
        let mut done_indexes = vec![cap];
        done_indexes.extend(0..cap);
        let provider = DoneOrderResponses::with_indexed_cycle(calls, done_indexes, cap);
        let tool = Arc::new(RecordingList(Mutex::new(Vec::new())));
        let events = collect_events(
            ToolUsingResponses::new(
                Arc::clone(&provider) as Arc<dyn Responses>,
                vec![],
                vec![Arc::clone(&tool) as Arc<dyn SpiceModelTool>],
                None,
            )
            .responses_stream(request(true))
            .await
            .expect("a response stream"),
        )
        .await;
        assert!(
            events
                .last()
                .is_some_and(|event| event["type"] == "response.completed")
        );

        let recorded = tool.0.lock().clone();
        assert_eq!(recorded.len(), cap);
        assert_eq!(recorded[0], r#"{"n":0}"#);
        assert!(
            !recorded
                .iter()
                .any(|arg| arg == &format!(r#"{{"n":{cap}}}"#)),
            "ran the later item's arguments: {recorded:?}"
        );
        let outputs = function_outputs(&provider.input(1));
        assert_eq!(
            outputs.get("call_0_0").map(String::as_str),
            Some("[\"taxi_trips\"]")
        );
    }
}
