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
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;

use arrow::array::RecordBatch;
use datafusion::sql::TableReference;
use itertools::Itertools;
use llms::chat::{Chat, Result as ChatResult};

use async_openai::error::OpenAIError;
use async_openai::types::{
    ChatCompletionMessageToolCall, ChatCompletionRequestAssistantMessageArgs,
    ChatCompletionRequestMessage, ChatCompletionRequestToolMessageArgs,
    ChatCompletionResponseStream, ChatCompletionTool, ChatCompletionToolChoiceOption,
    ChatCompletionToolType, CreateChatCompletionRequest, CreateChatCompletionRequestArgs,
    CreateChatCompletionResponse, FunctionCall, FunctionObject,
};

use async_trait::async_trait;
use futures::{Stream, TryStreamExt};
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use snafu::ResultExt;

use crate::datafusion::query::Protocol;
use crate::embeddings::vector_search::VectorSearch;
use crate::Runtime;

#[derive(Debug, Serialize, Deserialize)]
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
                .filter(|tool| t.contains(&tool.name()))
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
    fn name(&self) -> String;
    fn description(&self) -> Option<String>;
    fn parameters(&self) -> Option<Value>;
    async fn call(
        &self,
        arg: &str,
        rt: Arc<Runtime>,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>>;
}

pub struct DocumentSimilarityTool {}

#[derive(Deserialize)]
struct DocumentSimilarityToolArgs {
    text: String,
    from: Vec<String>,
    limit: Option<usize>,
}

#[async_trait]
impl SpiceModelTool for DocumentSimilarityTool {
    fn name(&self) -> String {
        "document_similarity".to_string()
    }

    fn description(&self) -> Option<String> {
        Some("Search and retrieve documents from available datasets".to_string())
    }

    fn parameters(&self) -> Option<Value> {
        Some(serde_json::json!({
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
        }))
    }

    async fn call(
        &self,
        arg: &str,
        rt: Arc<Runtime>,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let args: DocumentSimilarityToolArgs = serde_json::from_str(arg)?;

        let text = args.text;
        let datasets = args
            .from
            .iter()
            .map(|d| TableReference::parse_str(d))
            .collect_vec();

        let limit = args.limit.unwrap_or(3);

        // TODO: implement `explicit_primary_keys` parsing from rt.app
        let vs = VectorSearch::new(rt.datafusion(), Arc::clone(&rt.embeds), HashMap::default());

        let result = vs
            .search(
                text,
                datasets,
                crate::embeddings::vector_search::RetrievalLimit::TopN(limit),
            )
            .await
            .boxed()?;

        Ok(Value::Object(
            result
                .retrieved_entries
                .iter()
                .map(|(dataset, documents)| {
                    (
                        dataset.to_string(),
                        Value::Array(
                            documents
                                .iter()
                                .map(|d| Value::String(d.to_string()))
                                .collect(),
                        ),
                    )
                })
                .collect(),
        ))
    }
}

pub struct ListTablesTool {}

#[async_trait]
impl SpiceModelTool for ListTablesTool {
    fn name(&self) -> String {
        "list_tables".to_string()
    }

    fn description(&self) -> Option<String> {
        Some("List all SQL tables available".to_string())
    }

    fn parameters(&self) -> Option<Value> {
        None
    }

    async fn call(
        &self,
        _arg: &str,
        rt: Arc<Runtime>,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let tables = rt.datafusion().get_public_table_names().boxed()?;
        Ok(Value::Array(
            tables.iter().map(|t| Value::String(t.clone())).collect(),
        ))
    }
}

pub struct ListDatasetsTool {}

#[async_trait]
impl SpiceModelTool for ListDatasetsTool {
    fn name(&self) -> String {
        "list_datasets".to_string()
    }

    fn description(&self) -> Option<String> {
        Some("List all datasets that contain searchable documents".to_string())
    }

    fn parameters(&self) -> Option<Value> {
        None
    }

    async fn call(
        &self,
        _arg: &str,
        rt: Arc<Runtime>,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let embedding_datasets = match &*rt.app.read().await {
            Some(app) => app
                .datasets
                .iter()
                .filter(|d| !d.embeddings.is_empty())
                .map(|d| Value::String(d.name.clone()))
                .collect_vec(),
            None => vec![],
        };

        Ok(Value::Array(embedding_datasets))
    }
}

pub struct TableSchemaTool {}
#[async_trait]
impl SpiceModelTool for TableSchemaTool {
    fn name(&self) -> String {
        "table_schema".to_string()
    }

    fn description(&self) -> Option<String> {
        Some("Retrieve the schema of all available SQL tables".to_string())
    }

    fn parameters(&self) -> Option<Value> {
        Some(serde_json::json!({
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
        }))
    }

    async fn call(
        &self,
        arg: &str,
        rt: Arc<Runtime>,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
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
            let schema = rt.datafusion().get_arrow_schema(t).await.boxed()?;

            // Should use a better structure
            table_create_stms.push(schema.to_string());
        }
        Ok(Value::String(table_create_stms.join("\n")))
    }
}

static PARAMETERS: Lazy<Value> = Lazy::new(|| {
    serde_json::json!({
        "type": "object",
        "properties": {
            "query": {
                "type": "string",
                "description": "The SQL query to run. Quote wrap all columns and never select columns ending in '_embedding'.",
            },
        },
        "required": ["query"],
        "additionalProperties": false,
    })
});

pub struct SqlTool {}

#[async_trait]
impl SpiceModelTool for SqlTool {
    fn name(&self) -> String {
        "sql".to_string()
    }

    fn description(&self) -> Option<String> {
        Some("Run an SQL query on the data source".to_string())
    }

    fn parameters(&self) -> Option<Value> {
        Some(PARAMETERS.clone())
    }

    async fn call(
        &self,
        arg: &str,
        rt: Arc<Runtime>,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let arg_v = Value::from_str(arg).boxed()?;
        let q = arg_v["query"].as_str().unwrap_or_default();

        let query_result = rt
            .datafusion()
            .query_builder(q, Protocol::Flight)
            .build()
            .run()
            .await
            .boxed()?;
        let batches = query_result
            .data
            .try_collect::<Vec<RecordBatch>>()
            .await
            .boxed()?;

        let buf = Vec::new();
        let mut writer = arrow_json::ArrayWriter::new(buf);
        writer.write_batches(batches.iter().collect::<Vec<&RecordBatch>>().as_slice())?;
        Ok(Value::String(String::from_utf8(writer.into_inner())?))
    }
}

pub struct ToolUsingChat {
    inner_chat: Box<dyn Chat>,
    rt: Arc<Runtime>,
    tools: Vec<Box<dyn SpiceModelTool>>,
}

impl ToolUsingChat {
    pub fn new(inner_chat: Box<dyn Chat>, rt: Arc<Runtime>, tools_opt: &SpiceToolsOptions) -> Self {
        Self {
            inner_chat,
            rt,
            tools: tools_opt.filter_tools(vec![
                Box::new(DocumentSimilarityTool {}),
                Box::new(ListDatasetsTool {}),
                Box::new(TableSchemaTool {}),
                Box::new(SqlTool {}),
                Box::new(ListTablesTool {}),
            ]),
        }
    }

    pub fn runtime_tools(&self) -> Vec<ChatCompletionTool> {
        self.tools
            .iter()
            .map(|t| ChatCompletionTool {
                r#type: ChatCompletionToolType::Function,
                function: FunctionObject {
                    name: t.name(),
                    description: t.description(),
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
                        tracing::error!("Error calling tool: {}", e);
                        Value::String(format!("Error calling tool {}", t.name()))
                    }
                }
            }
            None => Value::Null,
        }
    }

    ///
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
            self.tools.iter().map(|t| t.name().clone()).collect_vec(),
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
    async fn run(&mut self, prompt: String) -> ChatResult<Option<String>> {
        self.inner_chat.run(prompt).await
    }

    async fn stream<'a>(
        &mut self,
        prompt: String,
    ) -> ChatResult<Pin<Box<dyn Stream<Item = ChatResult<Option<String>>> + Send>>> {
        self.inner_chat.stream(prompt).await
    }

    /// TODO: If response messages has ChatCompletionMessageToolCall that are spiced runtime tools, call locally, and pass back.
    async fn chat_stream(
        &mut self,
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

        // Append spiced runtime tools to the request.
        let mut inner_req = req.clone();
        let mut runtime_tools = self.runtime_tools();
        if !runtime_tools.is_empty() {
            runtime_tools.extend(req.tools.unwrap_or_default());
            inner_req.tools = Some(runtime_tools);
        };

        self.inner_chat.chat_stream(inner_req).await
    }

    /// An OpenAI-compatible interface for the `v1/chat/completion` `Chat` trait. If not implemented, the default
    /// implementation will be constructed based on the trait's [`run`] method.
    async fn chat_request(
        &mut self,
        req: CreateChatCompletionRequest,
    ) -> Result<CreateChatCompletionResponse, OpenAIError> {
        // Don't use spice runtime tools if users has explicitly chosen to not use any tools.
        if req
            .tool_choice
            .as_ref()
            .is_some_and(|c| *c == ChatCompletionToolChoiceOption::None)
        {
            tracing::debug!("User asked for no tools, calling inner chat model");
            return self.inner_chat.chat_request(req).await;
        };

        // Append spiced runtime tools to the request.
        let mut inner_req = req.clone();
        let mut runtime_tools = self.runtime_tools();
        if !runtime_tools.is_empty() {
            runtime_tools.extend(req.tools.unwrap_or_default());
            inner_req.tools = Some(runtime_tools);
        };

        let resp = self.inner_chat.chat_request(inner_req).await?;

        let tools_used = resp
            .choices
            .first()
            .and_then(|c| c.message.tool_calls.clone());

        match self
            .process_tool_calls_and_run_spice_tools(req.messages, tools_used.unwrap_or_default())
            .await?
        {
            // New messages means we have run spice tools locally, ready to recall model.
            Some(messages) => {
                let new_req = CreateChatCompletionRequestArgs::default()
                    .model(req.model)
                    .messages(messages)
                    .build()?;
                self.chat_request(new_req).await
            }
            None => Ok(resp),
        }
    }
}
