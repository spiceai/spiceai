/*
Copyright 2024 The Spice.ai OSS Authors

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
use std::str::FromStr;
use std::task::{Context, Poll};

use arrow::array::{Array, Int64Array, RecordBatch, StringArray};
use datafusion::sql::TableReference;
use itertools::Itertools;
use llms::chat::{Chat, Result as ChatResult};

use async_openai::error::OpenAIError;
use async_openai::types::{
    ChatChoiceStream, ChatCompletionMessageToolCall, ChatCompletionRequestAssistantMessageArgs,
    ChatCompletionRequestMessage, ChatCompletionRequestToolMessageArgs,
    ChatCompletionResponseStream, ChatCompletionTool, ChatCompletionToolChoiceOption,
    ChatCompletionToolType, CreateChatCompletionRequest, CreateChatCompletionRequestArgs,
    CreateChatCompletionResponse, CreateChatCompletionStreamResponse, FinishReason, FunctionCall,
    FunctionObject,
};

use async_trait::async_trait;
use futures::{Stream, StreamExt, TryStreamExt};
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use snafu::ResultExt;
use tokio::sync::mpsc;
use tracing::{Instrument, Span};

use crate::datafusion::query::Protocol;
use crate::datafusion::{SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA};
use crate::embeddings::vector_search::{parse_explicit_primary_keys, VectorSearch};
use crate::Runtime;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SpiceToolsOptions {
    Auto,
    Disabled,
    Specific(Vec<String>),
}

impl SpiceToolsOptions {
    // Check if spice tools can be used.
    pub fn can_use_tools(&self) -> bool {
        match self {
            SpiceToolsOptions::Auto => true,
            SpiceToolsOptions::Disabled => false,
            SpiceToolsOptions::Specific(t) => !t.is_empty(),
        }
    }

    /// Filter out a list of tools permitted by the  [`SpiceToolsOptions`].
    pub fn filter_tools(
        &self,
        tools: Vec<Box<dyn SpiceModelTool>>,
    ) -> Vec<Box<dyn SpiceModelTool>> {
        match self {
            SpiceToolsOptions::Auto => tools,
            SpiceToolsOptions::Disabled => vec![],
            SpiceToolsOptions::Specific(t) => tools
                .into_iter()
                .filter(|tool| t.iter().any(|s| s == tool.name()))
                .collect(),
        }
    }
}

impl FromStr for SpiceToolsOptions {
    type Err = Box<dyn std::error::Error + Send + Sync>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_lowercase().as_str() {
            "auto" => Ok(SpiceToolsOptions::Auto),
            "disabled" => Ok(SpiceToolsOptions::Disabled),
            _ => Ok(SpiceToolsOptions::Specific(
                s.split(',')
                    .map(|item| item.trim().to_string())
                    .filter(|item| !item.is_empty())
                    .collect(),
            )),
        }
    }
}

/// Tools that implement this trait can be automatically used by LLMs in the runtime.
#[async_trait]
pub trait SpiceModelTool: Sync + Send {
    fn name(&self) -> &'static str;
    fn description(&self) -> Option<&'static str>;
    fn parameters(&self) -> Option<Value>;
    async fn call(
        &self,
        arg: &str,
        rt: Arc<Runtime>,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>>;
}

static DOCUMENT_SIMILARITY_PARAMETERS: Lazy<Value> = Lazy::new(|| {
    serde_json::json!({
        "type": "object",
        "properties": {
            "text": {
                "type": "string",
                "description": "The text to search documents for similarity",
            },
            "from": {
                "type": "array",
                "items": {
                    "type": "string"
                },
                "description": "The datasets to search for similarity. For available datasets, use the 'list_datasets' tool",
                "default": []
            },
            "limit": {
                "type": "integer",
                "description": "Number of documents to return for each dataset",
                "default": 3
            }
        },
        "required": ["text", "from"],
        "additionalProperties": false
    })
});

#[derive(Deserialize)]
struct DocumentSimilarityToolArgs {
    text: String,
    from: Vec<String>,
    limit: Option<usize>,
}
pub struct DocumentSimilarityTool {}
impl DocumentSimilarityTool {
    /// Format the document & primary keys retrieved from a [`VectorSearch::search`] into a format suitable as a tool response.
    ///
    /// The format is:
    /// ```json
    /// [
    ///    {
    ///       "document": "document text",
    ///       "primary_key": {
    ///         "column_name": "value1",
    ///         "column_name2": "value2"
    ///       }
    ///   },
    /// ]
    fn format_table_response(
        documents: &Vec<String>,
        primary_keys: &[RecordBatch],
    ) -> serde_json::Value {
        let flatten_formatted_pks = Self::format_primary_keys(primary_keys);
        Value::Array(
            flatten_formatted_pks
                .iter()
                .zip_longest(documents)
                .map(|zip| {
                    let (pk_map_opt, document) = match zip {
                        itertools::EitherOrBoth::Both(pk_batch, document) => {
                            (Some(pk_batch), document.to_string())
                        }
                        itertools::EitherOrBoth::Left(pk_batch) => (Some(pk_batch), String::new()),
                        itertools::EitherOrBoth::Right(document) => (None, document.to_string()),
                    };
                    let mut z = Map::new();
                    z.insert("document".to_string(), Value::String(document));
                    if let Some(pk_map) = pk_map_opt {
                        z.insert("primary_key".to_string(), Value::Object(pk_map.clone()));
                    }
                    Value::Object(z)
                })
                .collect_vec(),
        )
    }

    fn format_primary_keys(rb: &[RecordBatch]) -> Vec<serde_json::Map<String, Value>> {
        let mut result = Vec::new();
        for batch in rb {
            (0..batch.num_rows()).for_each(|i| {
                let mut pk_map = serde_json::Map::new();
                batch.schema().fields().iter().for_each(|field| {
                    let col_name = field.name().clone();
                    if let Some(array) = batch.column_by_name(&col_name) {
                        if let Some(str_array) = array.as_any().downcast_ref::<StringArray>() {
                            pk_map.insert(col_name, Value::String(str_array.value(i).to_string()));
                        } else if let Some(array) = array.as_any().downcast_ref::<Int64Array>() {
                            pk_map.insert(col_name, Value::Number(array.value(i).into()));
                        }
                    };
                });
                result.push(pk_map);
            });
        }

        result
    }
}

#[async_trait]
impl SpiceModelTool for DocumentSimilarityTool {
    fn name(&self) -> &'static str {
        "document_similarity"
    }

    fn description(&self) -> Option<&'static str> {
        Some("Search and retrieve documents from available datasets")
    }

    fn parameters(&self) -> Option<Value> {
        Some(DOCUMENT_SIMILARITY_PARAMETERS.clone())
    }

    async fn call(
        &self,
        arg: &str,
        rt: Arc<Runtime>,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let span = tracing::span!(target: "task_history", tracing::Level::INFO, "tool_use::document_similarity", tool = self.name(), arg);
        let args: DocumentSimilarityToolArgs = serde_json::from_str(arg)?;

        let text = args.text;
        let datasets = args
            .from
            .iter()
            .map(|d| TableReference::parse_str(d))
            .collect_vec();

        let limit = args.limit.unwrap_or(3);

        // TODO: implement `explicit_primary_keys` parsing from rt.app
        let vs = VectorSearch::new(
            rt.datafusion(),
            Arc::clone(&rt.embeds),
            parse_explicit_primary_keys(Arc::clone(&rt.app)).await,
        );

        let result = vs
            .search(
                text,
                datasets,
                crate::embeddings::vector_search::RetrievalLimit::TopN(limit),
            )
            .instrument(span.clone())
            .await
            .boxed()?;

        Ok(Value::Object(
            result
                .retrieved_entries
                .iter()
                .map(|(dataset, documents)| {
                    let empty = Vec::new();
                    let primary_keys = result.retrieved_primary_keys.get(dataset).unwrap_or(&empty);
                    (
                        dataset.to_string(),
                        Self::format_table_response(documents, primary_keys),
                    )
                })
                .collect(),
        ))
    }
}

pub struct ListTablesTool {}

#[async_trait]
impl SpiceModelTool for ListTablesTool {
    fn name(&self) -> &'static str {
        "list_tables"
    }

    fn description(&self) -> Option<&'static str> {
        Some("List all SQL tables available")
    }

    fn parameters(&self) -> Option<Value> {
        None
    }

    async fn call(
        &self,
        arg: &str,
        rt: Arc<Runtime>,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let span = tracing::span!(target: "task_history", tracing::Level::INFO, "tool_use::list_tables", tool = self.name(), arg);
        if let Some(app) = &*rt.app.read().instrument(span.clone()).await {
            Ok(Value::Array(
                app.datasets
                    .iter()
                    .map(|d| {
                        Value::String(
                            TableReference::parse_str(&d.name)
                                .resolve(SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA)
                                .to_string(),
                        )
                    })
                    .collect::<Vec<Value>>(),
            ))
        } else {
            Ok(Value::Array(vec![]))
        }
    }
}

pub struct ListDatasetsTool {}

#[async_trait]
impl SpiceModelTool for ListDatasetsTool {
    fn name(&self) -> &'static str {
        "list_datasets"
    }

    fn description(&self) -> Option<&'static str> {
        Some("List all datasets that contain searchable documents")
    }

    fn parameters(&self) -> Option<Value> {
        None
    }

    async fn call(
        &self,
        arg: &str,
        rt: Arc<Runtime>,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let span = tracing::span!(target: "task_history", tracing::Level::INFO, "tool_use::list_datasets", tool = self.name(), arg);
        let embedding_datasets = match &*rt.app.read().instrument(span.clone()).await {
            Some(app) => app
                .datasets
                .iter()
                .filter(|d| !d.embeddings.is_empty())
                .map(|d| {
                    json!({
                        "name": TableReference::parse_str(&d.name).resolve(SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA).to_string(),
                        "description": d.description,
                    })
                })
                .collect_vec(),
            None => vec![],
        };

        Ok(Value::Array(embedding_datasets))
    }
}

pub struct TableSchemaTool {}
static TABLE_SCHEMA_PARAMETERS: Lazy<Value> = Lazy::new(|| {
    serde_json::json!({
        "type": "object",
        "properties": {
            "tables": {
            "type": "array",
            "items": {
                "type": "string"
            },
            "description": "Which subset of tables to return results for. Default to all tables.",
        },
        },
        "required": [],
        "additionalProperties": false,
    })
});

#[async_trait]
impl SpiceModelTool for TableSchemaTool {
    fn name(&self) -> &'static str {
        "table_schema"
    }

    fn description(&self) -> Option<&'static str> {
        Some("Retrieve the schema of all available SQL tables")
    }

    fn parameters(&self) -> Option<Value> {
        Some(TABLE_SCHEMA_PARAMETERS.clone())
    }

    async fn call(
        &self,
        arg: &str,
        rt: Arc<Runtime>,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let span = tracing::span!(target: "task_history", tracing::Level::INFO, "tool_use::table_schema", tool = self.name(), arg);
        let arg_v = Value::from_str(arg).boxed()?;
        // TODO support default == all tables
        let tables: Vec<String> = arg_v["tables"]
            .as_array()
            .unwrap_or(&vec![])
            .iter()
            .map(|t| t.as_str().unwrap_or_default().to_string())
            .filter(|t| !t.is_empty())
            .collect_vec();

        let mut table_create_stms: Vec<String> = Vec::with_capacity(tables.len());
        for t in &tables {
            let schema = rt
                .datafusion()
                .get_arrow_schema(t)
                .instrument(span.clone())
                .await
                .boxed()?;

            // Should use a better structure
            table_create_stms.push(schema.to_string());
        }
        Ok(Value::String(table_create_stms.join("\n")))
    }
}

pub struct SqlTool {}
static SQL_PARAMETERS: Lazy<Value> = Lazy::new(|| {
    serde_json::json!({
        "type": "object",
        "properties": {
            "query": {
                "type": "string",
                "description": "The SQL query to run. Double quote all select columns and never select columns ending in '_embedding'. The table_catalog is 'spice'. Always use it in the query",
            },
        },
        "required": ["query"],
        "additionalProperties": false,
    })
});

#[async_trait]
impl SpiceModelTool for SqlTool {
    fn name(&self) -> &'static str {
        "sql"
    }

    fn description(&self) -> Option<&'static str> {
        Some("Run an SQL query on the data source")
    }

    fn parameters(&self) -> Option<Value> {
        Some(SQL_PARAMETERS.clone())
    }

    async fn call(
        &self,
        arg: &str,
        rt: Arc<Runtime>,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let span = tracing::span!(target: "task_history", tracing::Level::INFO, "tool_use::sql", tool = self.name(), arg);
        let arg_v = Value::from_str(arg).boxed()?;
        let q = arg_v["query"].as_str().unwrap_or_default();

        let query_result = rt
            .datafusion()
            .query_builder(q, Protocol::Flight)
            .build()
            .run()
            .instrument(span.clone())
            .await
            .boxed()?;
        let batches = query_result
            .data
            .try_collect::<Vec<RecordBatch>>()
            .instrument(span.clone())
            .await
            .boxed()?;

        let buf = Vec::new();
        let mut writer = arrow_json::ArrayWriter::new(buf);
        writer.write_batches(batches.iter().collect::<Vec<&RecordBatch>>().as_slice())?;
        Ok(Value::String(String::from_utf8(writer.into_inner())?))
    }
}

pub struct ToolUsingChat {
    inner_chat: Arc<Box<dyn Chat>>,
    rt: Arc<Runtime>,
    tools: Vec<Box<dyn SpiceModelTool>>,
    opts: SpiceToolsOptions,
}

impl ToolUsingChat {
    pub fn new(inner_chat: Arc<Box<dyn Chat>>, rt: Arc<Runtime>, opts: &SpiceToolsOptions) -> Self {
        Self {
            inner_chat,
            rt,
            tools: opts.filter_tools(vec![
                Box::new(DocumentSimilarityTool {}),
                Box::new(ListDatasetsTool {}),
                Box::new(TableSchemaTool {}),
                Box::new(SqlTool {}),
                Box::new(ListTablesTool {}),
            ]),
            opts: opts.clone(),
        }
    }

    pub fn runtime_tools(&self) -> Vec<ChatCompletionTool> {
        self.tools
            .iter()
            .map(|t| ChatCompletionTool {
                r#type: ChatCompletionToolType::Function,
                function: FunctionObject {
                    name: t.name().to_string(),
                    description: t.description().map(ToString::to_string),
                    parameters: t.parameters(),
                },
            })
            .collect_vec()
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
                    Err(e) => {
                        tracing::error!(target: "task_history", "{e}");
                        Value::String(format!("Error calling tool {}", t.name()))
                    }
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

    /// TODO: If response messages has ChatCompletionMessageToolCall that are spiced runtime tools, call locally, and pass back.
    async fn chat_stream(
        &self,
        req: CreateChatCompletionRequest,
    ) -> Result<ChatCompletionResponseStream, OpenAIError> {
        let span = tracing::span!(target: "task_history", tracing::Level::INFO, "ai_completion", input = %serde_json::to_string(&req).unwrap_or_default());
        span.in_scope(
            || tracing::info!(name: "labels", target: "task_history", model = %req.model),
        );
        // Don't use spice runtime tools if users has explicitly chosen to not use any tools.
        if req
            .tool_choice
            .as_ref()
            .is_some_and(|c| *c == ChatCompletionToolChoiceOption::None)
        {
            return self.inner_chat.chat_stream(req).instrument(span).await;
        };

        // Append spiced runtime tools to the request.
        let mut inner_req = req.clone();
        let mut runtime_tools = self.runtime_tools();
        if !runtime_tools.is_empty() {
            runtime_tools.extend(req.tools.clone().unwrap_or_default());
            inner_req.tools = Some(runtime_tools);
        };

        let s = self
            .inner_chat
            .chat_stream(inner_req)
            .instrument(span.clone())
            .await?;
        Ok(make_a_stream(
            span,
            Self::new(
                Arc::clone(&self.inner_chat),
                Arc::clone(&self.rt),
                &self.opts,
            ),
            req.clone(),
            s,
        ))
    }

    /// An OpenAI-compatible interface for the `v1/chat/completion` `Chat` trait. If not implemented, the default
    /// implementation will be constructed based on the trait's [`run`] method.
    async fn chat_request(
        &self,
        req: CreateChatCompletionRequest,
    ) -> Result<CreateChatCompletionResponse, OpenAIError> {
        let span = tracing::span!(target: "task_history", tracing::Level::INFO, "ai_completion", input = %serde_json::to_string(&req).unwrap_or_default());
        span.in_scope(
            || tracing::info!(name: "labels", target: "task_history", model = %req.model),
        );
        // Don't use spice runtime tools if users has explicitly chosen to not use any tools.
        if req
            .tool_choice
            .as_ref()
            .is_some_and(|c| *c == ChatCompletionToolChoiceOption::None)
        {
            tracing::debug!("User asked for no tools, calling inner chat model");
            return self.inner_chat.chat_request(req).instrument(span).await;
        };

        // Append spiced runtime tools to the request.
        let mut inner_req = req.clone();
        let mut runtime_tools = self.runtime_tools();
        if !runtime_tools.is_empty() {
            runtime_tools.extend(req.tools.unwrap_or_default());
            inner_req.tools = Some(runtime_tools);
        };

        let resp = self
            .inner_chat
            .chat_request(inner_req)
            .instrument(span.clone())
            .await?;

        let tools_used = resp
            .choices
            .first()
            .and_then(|c| c.message.tool_calls.clone());

        match self
            .process_tool_calls_and_run_spice_tools(req.messages, tools_used.unwrap_or_default())
            .instrument(span.clone())
            .await?
        {
            // New messages means we have run spice tools locally, ready to recall model.
            Some(messages) => {
                let new_req = CreateChatCompletionRequestArgs::default()
                    .model(req.model)
                    .messages(messages)
                    .build()?;
                self.chat_request(new_req).instrument(span.clone()).await
            }
            None => Ok(resp),
        }
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
    model: ToolUsingChat,
    req: CreateChatCompletionRequest,
    mut s: ChatCompletionResponseStream,
) -> ChatCompletionResponseStream {
    let (sender, receiver) = mpsc::channel(100);
    let sender_clone = sender.clone();

    tokio::spawn(
        async move {
            let tool_call_states: Arc<Mutex<HashMap<(i32, i32), ChatCompletionMessageToolCall>>> =
                Arc::new(Mutex::new(HashMap::new()));

            let mut chat_output = String::new();

            while let Some(result) = s.next().await {
                let response = match result {
                    Ok(response) => response,
                    Err(e) => {
                        if let Err(e) = sender_clone.send(Err(e)).await {
                            tracing::error!("Error sending error: {}", e);
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
                            let key = if let Ok(index) = chat_choice.index.try_into() {
                                (index, tool_call_chunk.index)
                            } else {
                                tracing::error!(
                                    "chat_choice.index value {} is too large to fit in an i32",
                                    chat_choice.index
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
                                        arguments: tool_call_data
                                            .function
                                            .as_ref()
                                            .and_then(|f| f.arguments.clone())
                                            .unwrap_or_default(),
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
                    } else {
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
                                        tracing::error!("Failed to lock tool_call_states: {}", e);
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
                                        tracing::error!("Error sending error: {}", e);
                                    }
                                    return;
                                }
                            };

                            let mut new_req = req.clone();
                            new_req.messages.clone_from(&new_messages);
                            match model.chat_stream(new_req).await {
                                Ok(mut s) => {
                                    while let Some(resp) = s.next().await {
                                        // TODO check if this works for choices > 1.
                                        if let Err(e) = sender_clone.send(resp).await {
                                            tracing::error!("Error sending error: {}", e);
                                            return;
                                        }
                                    }
                                }
                                Err(e) => {
                                    if let Err(e) = sender_clone.send(Err(e)).await {
                                        tracing::error!("Error sending error: {}", e);
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
                }
                let mut resp2 = response.clone();
                resp2.choices = finished_choices;
                if let Err(e) = sender_clone.send(Ok(resp2)).await {
                    tracing::error!("Error sending error: {}", e);
                }
            }

            tracing::info!(target: "task_history", truncated_output = %chat_output);
        }
        .instrument(span),
    );
    Box::pin(CustomStream { receiver }) as ChatCompletionResponseStream
}
