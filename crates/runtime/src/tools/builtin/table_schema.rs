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
use crate::{
    Runtime,
    tools::{SpiceModelTool, utils::parameters},
};
use app::App;
use arrow_schema::{Field, Schema};
use arrow_tools::format::table_schemas_to_markdown_table;
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
use datafusion::{error::DataFusionError, sql::TableReference};
use itertools::Itertools;
use runtime_datafusion::allowlist::ResolvedTableAwareAllowlist;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use snafu::ResultExt;
use spicepod::semantic::Column;
use std::collections::HashMap;
use std::{borrow::Cow, sync::Arc};
use tracing_futures::Instrument;

/// A tool to retrieve the schema of one or more available SQL tables.
#[derive(Debug, Clone, JsonSchema, Serialize, Deserialize)]
pub struct TableSchemaToolParams {
    /// Which tables to return the schema of.
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
    rt: Arc<Runtime>,

    table_allowlist: Option<ResolvedTableAwareAllowlist>,
}

impl TableSchemaTool {
    #[must_use]
    pub fn new(rt: Arc<Runtime>, name: Option<&str>, description: Option<&str>) -> Self {
        Self {
            name: name.unwrap_or("table_schema").to_string(),
            description: Some(
                description
                    .unwrap_or("Retrieve the schema of all available SQL tables")
                    .to_string(),
            ),
            rt,
            table_allowlist: None,
        }
    }

    #[must_use]
    pub fn with_table_allowlist(mut self, allowlist: Option<ResolvedTableAwareAllowlist>) -> Self {
        self.table_allowlist = allowlist;
        self
    }

    pub async fn get_schema(
        &self,
        req: &TableSchemaToolParams,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let span = tracing::span!(target: "task_history", tracing::Level::INFO, "tool_use::table_schema", tool = self.name().to_string(), input = serde_json::to_string(&req).boxed()?);
        let TableSchemaToolParams { tables, output } = req;

        // Precompute extra column details only if needed (for `full` output).
        let column_info = match (output, self.rt.app.read().await.clone()) {
            (OutputType::Full, Some(app)) => tables
                .iter()
                .map(|t| {
                    let tbl = TableReference::parse_str(t);
                    let cols = Self::table_column_information_for_table(&tbl, &Arc::clone(&app));
                    (tbl, cols)
                })
                .collect_vec(),
            _ => vec![],
        };

        let result: Result<Vec<(String, Schema)>, Box<dyn std::error::Error + Send + Sync>> =
            async {
                let mut table_schemas: Vec<(String, Schema)> = Vec::with_capacity(tables.len());

                for (i, t) in tables.iter().enumerate() {
                    if self.table_allowlist.as_ref().is_some_and(|list| {
                        !list.table_is_allowed(&TableReference::parse_str(t.as_str()))
                    }) {
                        return Err(crate::datafusion::Error::UnableToGetTable {
                            source: DataFusionError::Plan(format!("No table named {t}")),
                        })
                        .boxed();
                    }
                    let base_schema = self
                        .rt
                        .datafusion()
                        .get_arrow_schema(t)
                        .instrument(span.clone())
                        .await
                        .boxed()?;

                    let schema = match output {
                        OutputType::Minimal => base_schema,
                        OutputType::Full => {
                            let Schema {
                                mut fields,
                                mut metadata,
                            } = base_schema;

                            if let Some((_tbl, Some((table_info, columns)))) = column_info.get(i) {
                                fields = fields
                                    .into_iter()
                                    .map(|f| {
                                        let col = columns.iter().find(|c| c.name == *f.name());
                                        match col {
                                            Some(c) => Arc::new(
                                                Field::new(
                                                    f.name(),
                                                    f.data_type().clone(),
                                                    f.is_nullable(),
                                                )
                                                .with_metadata(c.metadata()),
                                            ),
                                            None => Arc::clone(f),
                                        }
                                    })
                                    .collect();

                                metadata.extend(table_info.clone());
                            }

                            Schema::new_with_metadata(fields, metadata)
                        }
                    };

                    table_schemas.push((t.to_string(), schema));
                }

                Ok(table_schemas)
            }
            .instrument(span.clone())
            .await;

        match result {
            Ok(table_schemas) => {
                let schemas_as_string = table_schemas_to_markdown_table(table_schemas);
                tracing::info!(target: "task_history", parent: &span, captured_output = %schemas_as_string);
                Ok(Value::String(schemas_as_string))
            }
            Err(e) => {
                tracing::error!(target: "task_history", parent: &span, "{e}");
                Err(e)
            }
        }
    }

    /// Retrieve column information for the given table.
    fn table_column_information_for_table(
        tbl: &TableReference,
        app: &Arc<App>,
    ) -> Option<(HashMap<String, String>, Vec<Column>)> {
        if let Some(ds) = app
            .datasets
            .iter()
            .find(|d| tbl.resolved_eq(&TableReference::parse_str(&d.name)))
        {
            return Some((ds.metadata(), ds.columns.clone()));
        }
        if let Some(view) = app
            .views
            .iter()
            .find(|v| tbl.resolved_eq(&TableReference::parse_str(&v.name)))
        {
            return Some((view.metadata(), view.columns.clone()));
        }
        None
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
                        .map_err(OpenAIError::JSONDeserialize)?,
                },
            }])
            .build()
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

    async fn call(&self, arg: &str) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let req: TableSchemaToolParams = serde_json::from_str(arg)?;
        self.get_schema(&req).await
    }
}
