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
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;

use arrow::array::RecordBatch;
use itertools::Itertools;
use llms::chat::{Chat, Result as ChatResult};

use async_openai::error::OpenAIError;
use async_openai::types::{
    ChatCompletionMessageToolCall, ChatCompletionRequestAssistantMessageArgs,
    ChatCompletionRequestMessage, ChatCompletionRequestToolMessageArgs,
    ChatCompletionResponseStream, ChatCompletionTool, ChatCompletionToolChoiceOption,
    ChatCompletionToolType, CreateChatCompletionRequest, CreateChatCompletionResponse,
    FunctionObject,
};

use async_openai::types::CreateChatCompletionRequestArgs;
use async_trait::async_trait;
use futures::{Stream, TryStreamExt};
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use snafu::ResultExt;

use crate::datafusion::query::Protocol;
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

pub struct SqlTool {}
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

static PARAMETERS: Lazy<Value> = Lazy::new(|| {
    serde_json::json!({
        "type": "object",
        "properties": {
            "query": {
                "type": "string",
                "description": "The SQL query to run.",
            },
        },
        "required": ["query"],
        "additionalProperties": false,
    })
});

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
            tools: tools_opt.filter_tools(vec![Box::new(SqlTool {}), Box::new(ListTablesTool {})]),
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
    async fn call_tool(&self, req: &ChatCompletionMessageToolCall) -> Value {
        match self.tools.iter().find(|t| t.name() == req.function.name) {
            Some(t) => t
                .call(&req.function.arguments, Arc::<Runtime>::clone(&self.rt))
                .await
                .unwrap_or(Value::String(format!("Error calling tool {}", t.name()))),
            None => Value::Null,
        }
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
        let spiced_tools = tools_used
            .map(|ts| {
                ts.iter()
                    .filter(|&t| self.is_spiced_tool(t))
                    .cloned()
                    .collect_vec()
            })
            .unwrap_or_default();

        // Return early if no spiced runtime tools used.
        if spiced_tools.is_empty() {
            tracing::debug!("No spiced tools used by chat model, returning early");
            return Ok(resp);
        }

        // Tell model the assistant has these tools
        let assistant_message: ChatCompletionRequestMessage =
            ChatCompletionRequestAssistantMessageArgs::default()
                .tool_calls(spiced_tools.clone()) // TODO - should this include non-spiced tools?
                .build()?
                .into();

        let mut tool_and_response_content = vec![];
        for t in spiced_tools {
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

        let mut messages = req.messages.clone();
        messages.push(assistant_message);
        messages.extend(tool_messages);

        // TODO: get hyperparameters from the original request.
        let new_req = CreateChatCompletionRequestArgs::default()
            .messages(messages)
            .build()?;

        self.inner_chat.chat_request(new_req).await
    }
}
