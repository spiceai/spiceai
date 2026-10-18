/*
Copyright 2026 The Spice.ai OSS Authors

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

//! Spice's tools, for a model that serves the Messages API natively.
//!
//! Spice offers the model its tools beside the client's, and runs the calls the model makes to
//! them: each round of the loop sends the conversation so far, and when the model stops to call
//! only Spice's tools, Spice runs them and sends the next round. The client receives one Message
//! for the whole loop, with each call to a Spice tool as an `mcp_tool_use` block and its result
//! as an `mcp_tool_result` block (see [`super::messages_tool_history`]).
//!
//! When the model also calls one of the client's tools, Spice runs its own and the Message ends
//! with `stop_reason: tool_use`, for the client to run the rest. After as many rounds as the
//! model's `tool_recursion_limit`, Spice runs the last round's calls and the Message ends with
//! `stop_reason: pause_turn`; the client continues it by sending the conversation back, as it
//! would for Anthropic's own server tools.

use std::collections::{BTreeMap, HashSet, VecDeque};
use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;
use llms::progress::Progress;
use messages_api::stream::event_type;
use messages_api::{MessageStream, Messages, MessagesError, MessagesRequest};
use runtime_request_context::{AsyncMarker, RequestContext};
use serde_json::{Map, Value, json};
use tools::SpiceModelTool;
use tracing::{Instrument, Span};

use crate::model::messages_tool_history::{mcp_tool_result, mcp_tool_use, tool_result};
use crate::model::responses_tool_history::ToolOutcome;
use crate::model::tool_use::encode_tool_name;

pub struct ToolUsingMessages {
    inner: Arc<dyn Messages>,
    tools: Arc<SpiceTools>,
    recursion_limit: usize,
}

/// Spice's tools, by the name the model calls each by.
struct SpiceTools {
    tools: Vec<Arc<dyn SpiceModelTool>>,
}

impl SpiceTools {
    fn find(&self, name: &str) -> Option<&Arc<dyn SpiceModelTool>> {
        self.tools
            .iter()
            .find(|tool| encode_tool_name(tool.name().as_ref()) == name)
    }

    /// Each tool as the Messages API defines a tool.
    fn definitions(&self) -> Vec<Value> {
        self.tools
            .iter()
            .map(|tool| {
                let mut schema = tool
                    .parameters()
                    .filter(Value::is_object)
                    .unwrap_or_else(|| json!({"type": "object", "properties": {}}));
                if let Some(schema) = schema.as_object_mut() {
                    schema.insert("additionalProperties".to_string(), Value::Bool(false));
                }
                let mut definition = Map::new();
                definition.insert(
                    "name".to_string(),
                    Value::from(encode_tool_name(tool.name().as_ref())),
                );
                if let Some(description) = tool.description() {
                    definition.insert("description".to_string(), Value::from(description));
                }
                definition.insert("input_schema".to_string(), schema);
                if tool.strict() == Some(true) {
                    definition.insert("strict".to_string(), Value::Bool(true));
                }
                Value::Object(definition)
            })
            .collect()
    }

    /// Adds the tools to the request's, and returns the names of those added: a client tool of
    /// the same name is the client's to run, so a Spice tool it shadows is left out.
    fn offer(&self, body: &mut Map<String, Value>) -> HashSet<String> {
        let tools = body
            .entry("tools")
            .or_insert_with(|| Value::Array(Vec::new()));
        let Some(tools) = tools.as_array_mut() else {
            return HashSet::new();
        };
        let taken: HashSet<String> = tools
            .iter()
            .filter_map(|tool| tool.get("name").and_then(Value::as_str))
            .map(ToString::to_string)
            .collect();
        let mut offered = HashSet::new();
        for definition in self.definitions() {
            let Some(name) = definition.get("name").and_then(Value::as_str) else {
                continue;
            };
            if !taken.contains(name) {
                offered.insert(name.to_string());
                tools.push(definition);
            }
        }
        offered
    }

    /// Runs the call `tool_use`, a `tool_use` block that names one of the tools.
    async fn run(&self, tool_use: &Value) -> ToolOutcome {
        let name = tool_use.get("name").and_then(Value::as_str).unwrap_or_default();
        let id = tool_use
            .get("id")
            .and_then(Value::as_str)
            .map(ToString::to_string);
        let arguments = tool_use
            .get("input")
            .map_or_else(|| "{}".to_string(), Value::to_string);
        tracing::info!(
            target: "task_history",
            progress = Progress::log()
                .id(id.clone())
                .title(format!("Calling '{name}' tool"))
                .content(arguments.clone())
                .to_jsonl(),
        );
        let Some(tool) = self.find(name) else {
            return ToolOutcome::Failed(format!("The tool '{name}' does not exist."));
        };
        match tool.call(&arguments).await {
            Ok(result) => {
                tracing::info!(
                    target: "task_history",
                    progress = Progress::log()
                        .id(id)
                        .title(format!("'{name}' tool completed successfully"))
                        .json_content(result.clone())
                        .to_jsonl(),
                );
                ToolOutcome::Completed(result)
            }
            Err(e) => {
                tracing::info!(
                    target: "task_history",
                    progress = Progress::error()
                        .id(id)
                        .title(format!("'{name}' tool completed unsuccessfully"))
                        .content(e.to_string())
                        .to_jsonl(),
                );
                ToolOutcome::Failed(format!(
                    "Failed to call the tool {}. An error occurred: {e}",
                    tool.name()
                ))
            }
        }
    }
}

impl ToolUsingMessages {
    #[must_use]
    pub fn new(
        inner: Arc<dyn Messages>,
        tools: Vec<Arc<dyn SpiceModelTool>>,
        recursion_limit: usize,
    ) -> Self {
        Self {
            inner,
            tools: Arc::new(SpiceTools { tools }),
            recursion_limit: recursion_limit.max(1),
        }
    }
}

fn is_tool_use(block: &Value) -> bool {
    block.get("type").and_then(Value::as_str) == Some("tool_use")
}

fn tool_name(block: &Value) -> Option<&str> {
    block.get("name").and_then(Value::as_str)
}

/// Adds `round` to `total`, a usage summed over rounds: counts are added, objects merged, and
/// anything else is taken from the latest round.
fn add_usage(total: &mut Value, round: &Value) {
    match (total, round) {
        (Value::Object(total), Value::Object(round)) => {
            for (key, value) in round {
                match total.get_mut(key) {
                    Some(existing) => add_usage(existing, value),
                    None => {
                        total.insert(key.clone(), value.clone());
                    }
                }
            }
        }
        (total @ Value::Number(_), Value::Number(round)) => {
            if let (Some(a), Some(b)) = (total.as_u64(), round.as_u64()) {
                *total = Value::from(a + b);
            } else {
                *total = Value::Number(round.clone());
            }
        }
        (total, round) => *total = round.clone(),
    }
}

/// Overlays `update` on `usage`: a stream's `message_delta` usage replaces the counts its
/// `message_start` reported.
fn overlay_usage(usage: &mut Value, update: &Value) {
    if let (Some(usage), Some(update)) = (usage.as_object_mut(), update.as_object()) {
        for (key, value) in update {
            usage.insert(key.clone(), value.clone());
        }
    } else {
        *usage = update.clone();
    }
}

/// What one round's calls to Spice's tools produced, by call ID, in call order.
type Outcomes = Vec<(String, ToolOutcome)>;

async fn run_calls(tools: &SpiceTools, calls: &[Value]) -> Outcomes {
    let mut outcomes = Vec::with_capacity(calls.len());
    for call in calls {
        let id = call
            .get("id")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        outcomes.push((id, tools.run(call).await));
    }
    outcomes
}

/// Adds a round that called Spice's tools, and their results, to the request for the next
/// round.
///
/// A `tool_choice` that forces a call, `any` or a named tool, is satisfied once a round has
/// called one, so the next round gets `auto`, with the request's own parallel tool use setting.
/// Sending it again would force a call on every round until `tool_recursion_limit` ran out, and
/// the turn could never end in an answer.
fn add_round(body: &mut Map<String, Value>, content: Vec<Value>, outcomes: &Outcomes) {
    let results: Vec<Value> = outcomes
        .iter()
        .map(|(id, outcome)| tool_result(id, outcome))
        .collect();
    if let Some(Value::Array(messages)) = body.get_mut("messages") {
        messages.push(json!({"role": "assistant", "content": content}));
        messages.push(json!({"role": "user", "content": results}));
    }
    if let Some(choice) = body.get_mut("tool_choice").and_then(Value::as_object_mut)
        && matches!(
            choice.get("type").and_then(Value::as_str),
            Some("any" | "tool")
        )
    {
        choice.insert("type".to_string(), Value::from("auto"));
        choice.remove("name");
    }
}

/// How a round that called Spice's tools ends the Message, if it does: with `tool_use` when the
/// model also called a client tool, and `pause_turn` when the loop is out of rounds.
fn stop_after_tools(called_client_tools: bool, last_round: bool) -> Option<&'static str> {
    if called_client_tools {
        Some("tool_use")
    } else if last_round {
        Some("pause_turn")
    } else {
        None
    }
}

#[async_trait]
impl Messages for ToolUsingMessages {
    async fn create(&self, request: MessagesRequest) -> Result<Value, MessagesError> {
        let MessagesRequest {
            mut body,
            betas,
            version,
        } = request;
        let spice = self.tools.offer(&mut body);
        let context = RequestContext::current(AsyncMarker::new().await);

        let mut earlier: Vec<Value> = Vec::new();
        let mut usage = Value::Object(Map::new());
        let mut id: Option<Value> = None;
        for round in 1..=self.recursion_limit {
            let mut response = self
                .inner
                .create(MessagesRequest {
                    body: body.clone(),
                    betas: betas.clone(),
                    version: version.clone(),
                })
                .await?;
            if let Some(round_usage) = response.get("usage") {
                add_usage(&mut usage, round_usage);
            }
            let first_id = id.get_or_insert_with(|| response.get("id").cloned().unwrap_or_default());
            let first_id = first_id.clone();
            let content: Vec<Value> = response
                .get("content")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            let calls: Vec<Value> = content
                .iter()
                .filter(|block| is_tool_use(block) && tool_name(block).is_some_and(|name| spice.contains(name)))
                .cloned()
                .collect();
            let stop_reason = response.get("stop_reason").and_then(Value::as_str);

            let (client_content, stop_reason) = if calls.is_empty() || stop_reason != Some("tool_use") {
                (content, None)
            } else {
                let outcomes = run_calls(&self.tools, &calls).await;
                crate::model::add_tools_used(&context, outcomes.len());
                let called_client_tools = content.iter().any(|block| {
                    is_tool_use(block) && !tool_name(block).is_some_and(|name| spice.contains(name))
                });
                let mut client_content: Vec<Value> = content
                    .iter()
                    .map(|block| {
                        if is_tool_use(block) && tool_name(block).is_some_and(|name| spice.contains(name)) {
                            mcp_tool_use(block)
                        } else {
                            block.clone()
                        }
                    })
                    .collect();
                client_content.extend(outcomes.iter().map(|(id, outcome)| mcp_tool_result(id, outcome)));
                match stop_after_tools(called_client_tools, round == self.recursion_limit) {
                    Some(stop) => (client_content, Some(stop)),
                    None => {
                        earlier.extend(client_content);
                        add_round(&mut body, content, &outcomes);
                        continue;
                    }
                }
            };

            if let Some(message) = response.as_object_mut() {
                earlier.extend(client_content);
                message.insert("id".to_string(), first_id);
                message.insert("content".to_string(), Value::Array(earlier));
                message.insert("usage".to_string(), usage);
                if let Some(stop) = stop_reason {
                    message.insert("stop_reason".to_string(), Value::from(stop));
                    message.insert("stop_sequence".to_string(), Value::Null);
                }
            }
            return Ok(response);
        }
        Err(MessagesError::api(
            "The Spice tool loop ended without a final round.",
        ))
    }

    async fn create_stream(&self, request: MessagesRequest) -> Result<MessageStream, MessagesError> {
        let MessagesRequest {
            mut body,
            betas,
            version,
        } = request;
        let spice = self.tools.offer(&mut body);
        let events = self
            .inner
            .create_stream(MessagesRequest {
                body: body.clone(),
                betas: betas.clone(),
                version: version.clone(),
            })
            .await?;
        let state = StreamLoop {
            inner: Arc::clone(&self.inner),
            tools: Arc::clone(&self.tools),
            recursion_limit: self.recursion_limit,
            context: RequestContext::current(AsyncMarker::new().await),
            span: Span::current(),
            body,
            betas,
            version,
            spice,
            events: Some(events),
            round: 1,
            offset: 0,
            next_index: 0,
            blocks: BTreeMap::new(),
            round_usage: Value::Object(Map::new()),
            usage: Value::Object(Map::new()),
            message_delta: None,
            unreplayable: None,
            queue: VecDeque::new(),
        };
        Ok(Box::pin(futures::stream::unfold(state, |mut state| async move {
            loop {
                if let Some(item) = state.queue.pop_front() {
                    return Some((item, state));
                }
                let events = state.events.as_mut()?;
                match events.next().await {
                    Some(Ok(event)) => {
                        if event_type(&event) == Some("message_stop") {
                            state.end_round().await;
                        } else {
                            state.forward(event);
                        }
                    }
                    Some(Err(error)) => {
                        state.events = None;
                        state.queue.push_back(Err(error));
                    }
                    None => state.events = None,
                }
            }
        })))
    }

    async fn count_tokens(&self, request: MessagesRequest) -> Result<Value, MessagesError> {
        // Counted with Spice's tools, as the model is sent the request.
        let MessagesRequest {
            mut body,
            betas,
            version,
        } = request;
        self.tools.offer(&mut body);
        self.inner
            .count_tokens(MessagesRequest {
                body,
                betas,
                version,
            })
            .await
    }
}

/// A content block of the current round, as the model is sent it in the next round: the block
/// `content_block_start` carried, completed by its deltas.
struct RoundBlock {
    block: Value,
    partial_json: String,
    /// Whether the block is a call to one of Spice's tools.
    spice: bool,
}

/// The state of a streamed tool-use loop: the current round's events, and what the rounds so
/// far have sent the client.
struct StreamLoop {
    inner: Arc<dyn Messages>,
    tools: Arc<SpiceTools>,
    recursion_limit: usize,
    context: Arc<RequestContext>,
    span: Span,
    body: Map<String, Value>,
    betas: Vec<String>,
    version: Option<String>,
    spice: HashSet<String>,
    events: Option<MessageStream>,
    round: usize,
    /// The client's index of the current round's first block.
    offset: usize,
    /// The client's index of the next block.
    next_index: usize,
    blocks: BTreeMap<usize, RoundBlock>,
    round_usage: Value,
    /// The usage of the rounds before the current one.
    usage: Value,
    message_delta: Option<Value>,
    /// A delta the current round could not be completed from, which keeps it from being sent
    /// back in a next round.
    unreplayable: Option<String>,
    queue: VecDeque<Result<Value, MessagesError>>,
}

impl StreamLoop {
    fn emit(&mut self, event: Value) {
        self.queue.push_back(Ok(event));
    }

    /// The event with its block index moved from the round's numbering to the client's.
    fn reindexed(&mut self, mut event: Value) -> (Option<usize>, Value) {
        let round_index = event
            .get("index")
            .and_then(Value::as_u64)
            .and_then(|index| usize::try_from(index).ok());
        if let Some(round_index) = round_index {
            let index = self.offset + round_index;
            self.next_index = self.next_index.max(index + 1);
            if let Some(event) = event.as_object_mut() {
                event.insert("index".to_string(), Value::from(index));
            }
        }
        (round_index, event)
    }

    /// Passes a round's event to the client, and records what the next round needs of it.
    fn forward(&mut self, event: Value) {
        match event_type(&event) {
            Some("message_start") => {
                if let Some(usage) = event.pointer("/message/usage") {
                    self.round_usage = usage.clone();
                }
                // The client's message starts with the first round's.
                if self.round == 1 {
                    self.emit(event);
                }
            }
            Some("content_block_start") => {
                let (round_index, mut event) = self.reindexed(event);
                let block = event.get("content_block").cloned().unwrap_or_default();
                let spice = is_tool_use(&block)
                    && tool_name(&block).is_some_and(|name| self.spice.contains(name));
                if spice && let Some(event) = event.as_object_mut() {
                    event.insert("content_block".to_string(), mcp_tool_use(&block));
                }
                if let Some(round_index) = round_index {
                    self.blocks.insert(
                        round_index,
                        RoundBlock {
                            block,
                            partial_json: String::new(),
                            spice,
                        },
                    );
                }
                self.emit(event);
            }
            Some("content_block_delta") => {
                let (round_index, event) = self.reindexed(event);
                if let Some(block) = round_index.and_then(|index| self.blocks.get_mut(&index))
                    && let Err(delta_type) = apply_delta(block, event.get("delta"))
                {
                    self.unreplayable.get_or_insert(delta_type);
                }
                self.emit(event);
            }
            Some("content_block_stop") => {
                let (round_index, event) = self.reindexed(event);
                if let Some(block) = round_index.and_then(|index| self.blocks.get_mut(&index))
                    && !block.partial_json.is_empty()
                {
                    let input = serde_json::from_str::<Value>(&block.partial_json)
                        .unwrap_or_else(|_| Value::String(block.partial_json.clone()));
                    if let Some(object) = block.block.as_object_mut() {
                        object.insert("input".to_string(), input);
                    }
                }
                self.emit(event);
            }
            Some("message_delta") => {
                if let Some(usage) = event.get("usage") {
                    overlay_usage(&mut self.round_usage, usage);
                }
                // Held until the round's end decides how the client's message ends.
                self.message_delta = Some(event);
            }
            _ => self.emit(event),
        }
    }

    /// Ends the current round: the client's message, or, when the model stopped to call only
    /// Spice's tools, their results and the next round.
    async fn end_round(&mut self) {
        let round_usage = std::mem::replace(&mut self.round_usage, Value::Object(Map::new()));
        add_usage(&mut self.usage, &round_usage);
        let mut message_delta = self
            .message_delta
            .take()
            .unwrap_or_else(|| json!({"type": "message_delta", "delta": {}}));
        let stop_reason = message_delta
            .pointer("/delta/stop_reason")
            .and_then(Value::as_str)
            .map(ToString::to_string);
        let calls: Vec<Value> = self
            .blocks
            .values()
            .filter(|block| block.spice)
            .map(|block| block.block.clone())
            .collect();

        if !calls.is_empty() && stop_reason.as_deref() == Some("tool_use") {
            let outcomes = run_calls(&self.tools, &calls)
                .instrument(self.span.clone())
                .await;
            crate::model::add_tools_used(&self.context, outcomes.len());
            for (id, outcome) in &outcomes {
                let index = self.next_index;
                self.next_index += 1;
                self.emit(json!({"type": "content_block_start", "index": index, "content_block": mcp_tool_result(id, outcome)}));
                self.emit(json!({"type": "content_block_stop", "index": index}));
            }
            let called_client_tools = self.blocks.values().any(|block| {
                !block.spice && is_tool_use(&block.block)
            });
            match stop_after_tools(called_client_tools, self.round >= self.recursion_limit) {
                Some(stop) => {
                    if let Some(delta) = message_delta.get_mut("delta").and_then(Value::as_object_mut) {
                        delta.insert("stop_reason".to_string(), Value::from(stop));
                        delta.insert("stop_sequence".to_string(), Value::Null);
                    }
                }
                None => {
                    self.next_round(outcomes).await;
                    return;
                }
            }
        }

        if let Some(event) = message_delta.as_object_mut() {
            event.insert("usage".to_string(), self.usage.clone());
        }
        self.emit(message_delta);
        self.emit(json!({"type": "message_stop"}));
        self.events = None;
    }

    async fn next_round(&mut self, outcomes: Outcomes) {
        if let Some(delta_type) = self.unreplayable.take() {
            self.events = None;
            self.queue.push_back(Err(MessagesError::api(format!(
                "Spice ran its tools, but cannot send the model its own answer back to continue: the stream carried a `{delta_type}` delta, which Spice cannot complete a content block from. Send the request without `stream`."
            ))));
            return;
        }
        let content: Vec<Value> = std::mem::take(&mut self.blocks)
            .into_values()
            .map(|block| block.block)
            .collect();
        add_round(&mut self.body, content, &outcomes);
        self.round += 1;
        self.offset = self.next_index;
        match self
            .inner
            .create_stream(MessagesRequest {
                body: self.body.clone(),
                betas: self.betas.clone(),
                version: self.version.clone(),
            })
            .instrument(self.span.clone())
            .await
        {
            Ok(events) => self.events = Some(events),
            Err(error) => {
                self.events = None;
                self.queue.push_back(Err(error));
            }
        }
    }
}

/// Applies a delta to the block it extends. `Err` with the delta's type when it is one the
/// block cannot be completed from.
fn apply_delta(block: &mut RoundBlock, delta: Option<&Value>) -> Result<(), String> {
    let Some(delta) = delta else {
        return Ok(());
    };
    let delta_type = delta.get("type").and_then(Value::as_str).unwrap_or_default();
    let append = |block: &mut Value, field: &str, text: Option<&str>| {
        if let (Some(object), Some(text)) = (block.as_object_mut(), text) {
            let existing = object
                .get(field)
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            object.insert(field.to_string(), Value::from(existing + text));
        }
    };
    match delta_type {
        "text_delta" => append(&mut block.block, "text", delta.get("text").and_then(Value::as_str)),
        "thinking_delta" => append(
            &mut block.block,
            "thinking",
            delta.get("thinking").and_then(Value::as_str),
        ),
        "signature_delta" => {
            if let (Some(object), Some(signature)) =
                (block.block.as_object_mut(), delta.get("signature"))
            {
                object.insert("signature".to_string(), signature.clone());
            }
        }
        "input_json_delta" => block.partial_json.push_str(
            delta
                .get("partial_json")
                .and_then(Value::as_str)
                .unwrap_or_default(),
        ),
        "citations_delta" => {
            if let (Some(object), Some(citation)) =
                (block.block.as_object_mut(), delta.get("citation"))
            {
                let citations = object
                    .entry("citations")
                    .or_insert_with(|| Value::Array(Vec::new()));
                if !citations.is_array() {
                    *citations = Value::Array(Vec::new());
                }
                if let Some(citations) = citations.as_array_mut() {
                    citations.push(citation.clone());
                }
            }
        }
        other => return Err(other.to_string()),
    }
    Ok(())
}

#[cfg(test)]
mod tests;
