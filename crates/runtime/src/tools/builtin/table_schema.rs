use async_openai::{
    error::OpenAIError,
    types::{
        ChatCompletionMessageToolCall, ChatCompletionRequestAssistantMessage,
        ChatCompletionRequestAssistantMessageArgs, ChatCompletionRequestToolMessage,
        ChatCompletionRequestToolMessageArgs, ChatCompletionRequestToolMessageContent,
        ChatCompletionToolType, FunctionCall,
    },
};
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
use async_trait::async_trait;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::HashMap, sync::Arc};

use crate::{
    datafusion::DataFusion,
    tools::{parameters, SpiceModelTool},
    Runtime,
};
use snafu::ResultExt;
use tracing_futures::Instrument;

#[derive(Debug, Clone, JsonSchema, Serialize, Deserialize)]
pub struct TableSchemaToolParams {
    /// Which subset of tables to return results for. Default to all tables.
    tables: Vec<String>,
}

impl TableSchemaToolParams {
    #[must_use]
    pub fn new(tables: Vec<String>) -> Self {
        Self { tables }
    }
}

pub struct TableSchemaTool {
    name: String,
    description: Option<String>,
}

impl TableSchemaTool {
    #[must_use]
    pub fn new(name: &str, description: Option<String>) -> Self {
        Self {
            name: name.to_string(),
            description,
        }
    }

    pub async fn get_schema(
        &self,
        df: Arc<DataFusion>,
        req: &TableSchemaToolParams,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let span = tracing::span!(target: "task_history", tracing::Level::INFO, "tool_use::table_schema", tool = self.name(), input = serde_json::to_string(&req).boxed()?);

        let mut table_schemas: Vec<Value> = Vec::with_capacity(req.tables.len());
        for t in &req.tables {
            let schema = df
                .get_arrow_schema(t)
                .instrument(span.clone())
                .await
                .boxed()?;

            let schema_value = serde_json::value::to_value(schema).boxed()?;

            let table_schema = serde_json::json!({
                "table": t,
                "schema": schema_value
            });

            table_schemas.push(table_schema);
        }

        let captured_output_json = serde_json::to_string(&table_schemas).boxed()?;
        tracing::info!(target: "task_history", parent: &span, captured_output = %captured_output_json);

        Ok(Value::Array(table_schemas))
    }

    /// Creates a [`ChatCompletionRequestToolMessage`] as if a language model had called this tool.
    pub fn to_tool_response_message(
        &self,
        id: &str,
        result: &Value,
    ) -> Result<ChatCompletionRequestToolMessage, OpenAIError> {
        ChatCompletionRequestToolMessageArgs::default()
            .tool_call_id(id)
            .content(ChatCompletionRequestToolMessageContent::Text(
                result.to_string(),
            ))
            .build()
    }

    /// Creates a [`ChatCompletionRequestAssistantMessage`] as if a language model has requested to call this tool with the given [`TableSchemaToolParams`].
    pub fn to_assistant_request_message(
        &self,
        id: &str,
        params: &TableSchemaToolParams,
    ) -> Result<ChatCompletionRequestAssistantMessage, OpenAIError> {
        ChatCompletionRequestAssistantMessageArgs::default()
            .tool_calls(vec![ChatCompletionMessageToolCall {
                id: id.to_string(),
                r#type: ChatCompletionToolType::Function,
                function: FunctionCall {
                    name: self.name().to_string(),
                    arguments: serde_json::to_string(&params)
                        .map_err(OpenAIError::JSONDeserialize)?
                        .to_string(),
                },
            }])
            .build()
    }
}
impl Default for TableSchemaTool {
    fn default() -> Self {
        Self::new(
            "table_schema",
            Some("Retrieve the schema of all available SQL tables".to_string()),
        )
    }
}

impl From<TableSchemaTool> for spicepod::component::tool::Tool {
    fn from(val: TableSchemaTool) -> Self {
        spicepod::component::tool::Tool {
            from: format!("builtin:{}", val.name()),
            name: val.name().to_string(),
            description: val.description().map(ToString::to_string),
            params: HashMap::default(),
            depends_on: Vec::default(),
        }
    }
}

#[async_trait]
impl SpiceModelTool for TableSchemaTool {
    fn name(&self) -> &str {
        self.name.as_str()
    }

    fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }

    fn parameters(&self) -> Option<Value> {
        parameters::<TableSchemaToolParams>()
    }

    async fn call(
        &self,
        arg: &str,
        rt: Arc<Runtime>,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let req: TableSchemaToolParams = serde_json::from_str(arg)?;
        self.get_schema(rt.datafusion(), &req).await
    }
}
