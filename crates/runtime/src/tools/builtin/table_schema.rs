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
use app::App;
use arrow_schema::{Field, Schema};
use async_openai::{
    error::OpenAIError,
    types::{
        ChatCompletionMessageToolCall, ChatCompletionRequestAssistantMessage,
        ChatCompletionRequestAssistantMessageArgs, ChatCompletionRequestToolMessage,
        ChatCompletionRequestToolMessageArgs, ChatCompletionRequestToolMessageContent,
        ChatCompletionToolType, FunctionCall,
    },
};
use async_trait::async_trait;
use datafusion::sql::TableReference;
use itertools::Itertools;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use spicepod::component::dataset::{column::Column, Dataset};
use std::{borrow::Cow, collections::HashMap, sync::Arc};

use crate::{
    tools::{utils::parameters, SpiceModelTool},
    Runtime,
};
use snafu::ResultExt;
use tracing_futures::Instrument;

#[derive(Debug, Clone, JsonSchema, Serialize, Deserialize)]
pub struct TableSchemaToolParams {
    /// Which subset of tables to return results for. Default to all tables.
    tables: Vec<String>,

    /// If `full` return metadata and semantic details about the columns.
    #[serde(default)]
    output: OutputType,
}

#[derive(Debug, Clone, Copy, PartialEq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum OutputType {
    #[default]
    Full,
    Minimal,
}

impl TableSchemaToolParams {
    #[must_use]
    pub fn new(tables: Vec<String>) -> Self {
        Self {
            tables,
            output: OutputType::default(),
        }
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
        rt: Arc<Runtime>,
        req: &TableSchemaToolParams,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let span = tracing::span!(target: "task_history", tracing::Level::INFO, "tool_use::table_schema", tool = self.name().to_string(), input = serde_json::to_string(&req).boxed()?);
        let TableSchemaToolParams { tables, output } = req;

        // Precompute extra column details only if needed (for `full` output).
        let column_info = match (output, rt.app.read().await.clone()) {
            (OutputType::Full, Some(app)) => {
                Self::column_information_for_tables(tables.as_slice(), &app)
            }
            _ => vec![],
        };

        let mut table_schemas: Vec<Value> = Vec::with_capacity(tables.len());
        for (i, t) in tables.iter().enumerate() {
            let base_schema = rt
                .df
                .get_arrow_schema(t)
                .instrument(span.clone())
                .await
                .boxed()?;

            let schema = match output {
                OutputType::Minimal => base_schema,
                OutputType::Full => {
                    let Schema {
                        mut fields,
                        metadata,
                    } = base_schema;

                    if let Some(columns) = column_info.get(i) {
                        fields = fields
                            .into_iter()
                            .map(|f| {
                                columns.get(f.name()).map_or_else(
                                    || Arc::clone(f),
                                    |c| {
                                        Arc::new(
                                            Field::new(
                                                f.name(),
                                                f.data_type().clone(),
                                                f.is_nullable(),
                                            )
                                            .with_metadata(c.metadata()),
                                        )
                                    },
                                )
                            })
                            .collect();
                    }

                    Schema::new_with_metadata(fields, metadata)
                }
            };

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

    /// Retrieve column information for the given tables. Output order is the same as the input order.
    /// Output Hashmap is column name to [`Column`].
    fn column_information_for_tables(
        tables: &[String],
        app: &Arc<App>,
    ) -> Vec<HashMap<String, Column>> {
        tables
            .iter()
            .map(|t| {
                let Some(table) = Self::table_in_app(app, t) else {
                    return HashMap::new();
                };
                table
                    .columns
                    .iter()
                    .map(|c| (c.name.clone(), c.clone()))
                    .collect()
            })
            .collect_vec()
    }

    /// Checks if a given table exists in the app, by resolving and comparing as [`TableReference`].
    fn table_in_app<'a>(app: &'a App, table: &str) -> Option<&'a Dataset> {
        let tbl = TableReference::parse_str(table);

        app.datasets
            .iter()
            .find(|d| tbl.resolved_eq(&TableReference::parse_str(&d.name)))
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

#[async_trait]
impl SpiceModelTool for TableSchemaTool {
    fn name(&self) -> Cow<'_, str> {
        Cow::Borrowed(&self.name)
    }

    fn description(&self) -> Option<Cow<'_, str>> {
        self.description.as_deref().map(Cow::Borrowed)
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
        self.get_schema(rt, &req).await
    }
}
