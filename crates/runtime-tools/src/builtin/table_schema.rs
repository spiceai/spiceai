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
#![allow(clippy::missing_errors_doc)]

use crate::builtin::list_datasets::{SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA};
use crate::metadata::{
    ListingDetail, Searchable, TableListing, field_description, search_score, search_words,
    write_table_listing,
};
use crate::utils::parameters;
use app::App;
use arrow_schema::{Field, Schema};
use async_openai::{
    error::OpenAIError,
    types::chat::{
        ChatCompletionMessageToolCall, ChatCompletionMessageToolCalls,
        ChatCompletionRequestAssistantMessage, ChatCompletionRequestAssistantMessageArgs,
        ChatCompletionRequestToolMessage, ChatCompletionRequestToolMessageArgs,
        ChatCompletionRequestToolMessageContent, FunctionCall,
    },
};
use async_trait::async_trait;
use datafusion::{
    common::{Constraint, Constraints, TableReference},
    error::DataFusionError,
};
use runtime_query_engine::allowlist::ResolvedTableAwareAllowlist;
use runtime_query_engine::query_engine::QueryEngine;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use snafu::ResultExt;
use spicepod::semantic::Column;
use std::collections::HashMap;
use std::{borrow::Cow, sync::Arc};
use tokio::sync::RwLock;
use tools::SpiceModelTool;
use tracing_futures::Instrument;

/// A tool to retrieve the schema of one or more available SQL tables.
#[derive(Debug, Clone, JsonSchema, Serialize, Deserialize)]
pub struct TableSchemaToolParams {
    /// Which tables to return the schema of: fully qualified names from `list_datasets`.
    tables: Vec<String>,

    /// `full` (the default) adds column descriptions and the table's keys; `minimal` lists
    /// column names and types only.
    #[serde(default)]
    output: OutputType,

    /// Words to look for in column names and descriptions: only the matching columns are listed,
    /// best match first, with a count of the others. Omit to list every column.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    search: Option<String>,
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
            search: None,
        }
    }
}

pub struct TableSchemaTool {
    name: String,
    description: Option<String>,
    df: Arc<dyn QueryEngine>,
    app: Arc<RwLock<Option<Arc<App>>>>,

    table_allowlist: Option<ResolvedTableAwareAllowlist>,
}

impl TableSchemaTool {
    #[must_use]
    pub fn new(
        df: Arc<dyn QueryEngine>,
        app: Arc<RwLock<Option<Arc<App>>>>,
        name: Option<&str>,
        description: Option<&str>,
    ) -> Self {
        Self {
            name: name.unwrap_or("table_schema").to_string(),
            description: Some(
                description
                    .unwrap_or("Return the columns of one or more datasets, one per line with its SQL type, `NOT NULL`, and (with `output=full`, the default) its description and the table's keys. Call this before writing a `sql` query so you know the exact column names and types. Pass `tables` as fully qualified names from `list_datasets`. For a wide table, pass `search` to list only the columns whose names or descriptions match. `output` is `full` or `minimal` (names and types only).")
                    .to_string(),
            ),
            df,
            app,
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
        let TableSchemaToolParams {
            tables,
            output,
            search,
        } = req;
        let app = self.app.read().await.clone();
        let words = search.as_deref().map(search_words).unwrap_or_default();
        let detail = match output {
            OutputType::Full => ListingDetail::Full,
            OutputType::Minimal => ListingDetail::Minimal,
        };

        let result: Result<String, Box<dyn std::error::Error + Send + Sync>> = async {
            let mut text = String::new();
            for t in tables {
                let table = TableReference::parse_str(t.as_str());
                if self
                    .table_allowlist
                    .as_ref()
                    .is_some_and(|list| !list.table_is_allowed(&table))
                {
                    return Err(DataFusionError::Plan(format!("No table named {t}"))).boxed();
                }
                let base_schema = self.df.get_arrow_schema(table.clone()).await.boxed()?;
                let spicepod = app
                    .as_ref()
                    .and_then(|app| Self::table_column_information_for_table(&table, app));
                let schema = with_column_descriptions(base_schema, spicepod.as_ref());
                let description = spicepod
                    .as_ref()
                    .and_then(|(table_info, _)| table_info.get("description").cloned())
                    .or_else(|| {
                        schema
                            .metadata()
                            .get("description")
                            .or_else(|| schema.metadata().get("comment"))
                            .cloned()
                    });
                let provider = self.df.get_table(&table).await;
                let (primary_key, unique) =
                    keys(&schema, provider.as_ref().and_then(|p| p.constraints()));
                let name = table
                    .clone()
                    .resolve(SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA)
                    .to_string();
                let listing = TableListing {
                    table: &name,
                    description: description.as_deref(),
                    schema: &schema,
                    primary_key,
                    unique,
                };
                let columns = matching_columns(&schema, &words);
                if !text.is_empty() {
                    text.push('\n');
                }
                write_table_listing(&mut text, &listing, detail, columns.as_deref());
            }
            Ok(text)
        }
        .instrument(span.clone())
        .await;

        match result {
            Ok(text) => {
                tracing::info!(target: "task_history", parent: &span, captured_output = %text);
                Ok(Value::String(text))
            }
            Err(e) => {
                tracing::error!(target: "task_history", parent: &span, "{e}");
                Err(e)
            }
        }
    }

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
            .tool_calls(vec![ChatCompletionMessageToolCalls::Function(
                ChatCompletionMessageToolCall {
                    id: id.to_string(),
                    function: FunctionCall {
                        name: self.name().to_string(),
                        arguments: serde_json::to_string(&params)
                            .map_err(|e| OpenAIError::JSONDeserialize(e, String::new()))?,
                    },
                },
            )])
            .build()
    }
}

/// `schema` with each column's Spicepod description, when the Spicepod gives one.
fn with_column_descriptions(
    schema: Schema,
    spicepod: Option<&(HashMap<String, String>, Vec<Column>)>,
) -> Schema {
    let Some((_, columns)) = spicepod else {
        return schema;
    };
    let Schema { fields, metadata } = schema;
    let fields: Vec<Arc<Field>> = fields
        .iter()
        .map(|f| match columns.iter().find(|c| c.name == *f.name()) {
            Some(c) => {
                let mut field_metadata = f.metadata().clone();
                field_metadata.extend(c.metadata());
                Arc::new(f.as_ref().clone().with_metadata(field_metadata))
            }
            None => Arc::clone(f),
        })
        .collect();
    Schema::new_with_metadata(fields, metadata)
}

/// The columns of `schema` that `words` match, by index, best match first; `None` (every column)
/// when `words` is empty.
fn matching_columns(schema: &Schema, words: &[String]) -> Option<Vec<usize>> {
    if words.is_empty() {
        return None;
    }
    let mut scored: Vec<(u32, usize)> = schema
        .fields()
        .iter()
        .enumerate()
        .filter_map(|(index, field)| {
            let score = search_score(
                words,
                &Searchable {
                    name: field.name(),
                    description: field_description(field),
                    columns: Vec::new(),
                },
            );
            (score > 0).then_some((score, index))
        })
        .collect();
    // The table's column order among equal matches.
    scored.sort_by(|(a_score, a), (b_score, b)| b_score.cmp(a_score).then(a.cmp(b)));
    Some(scored.into_iter().map(|(_, index)| index).collect())
}

/// The primary key and unique constraints of a table, as column names.
fn keys(schema: &Schema, constraints: Option<&Constraints>) -> (Vec<String>, Vec<Vec<String>>) {
    let names = |indices: &[usize]| -> Vec<String> {
        indices
            .iter()
            .filter_map(|&i| schema.fields().get(i).map(|f| f.name().clone()))
            .collect()
    };
    let mut primary_key = Vec::new();
    let mut unique = Vec::new();
    for constraint in constraints.into_iter().flat_map(|c| c.iter()) {
        match constraint {
            Constraint::PrimaryKey(indices) => primary_key = names(indices),
            Constraint::Unique(indices) => unique.push(names(indices)),
        }
    }
    (primary_key, unique)
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
