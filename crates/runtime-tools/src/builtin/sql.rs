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
use arrow::array::RecordBatch;
use async_trait::async_trait;
use std::{borrow::Cow, sync::Arc};

use crate::builtin::function_tool::current_principal_requires_read_only;
use crate::utils::{parameters, write_to_json_string};
use futures::TryStreamExt;
use runtime_query_engine::allowlist::ResolvedTableAwareAllowlist;
use runtime_query_engine::query_engine::{QueryEngine, QueryRequest};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tools::SpiceModelTool;
use tracing::Span;
use tracing_futures::Instrument;

#[derive(Debug, Clone, JsonSchema, Serialize, Deserialize)]
pub struct SqlToolParams {
    /// The SQL query to run, written in the Spice.ai SQL Dialect (`DataFusion` SQL using PostgreSQL-dialect with Spice.ai functions). Double quote all select columns and never select columns ending in '_embedding'. The `table_catalog` is 'spice'. Always use it in the query.
    query: String,
}

/// Default description advertised to LLMs / tool selection. Writes are
/// permitted only when the calling principal has the `read_write` group
/// (e.g. the request is authenticated with a `:rw`-tagged API key); the
/// strict read-only validator is still applied for `ReadOnly` / anonymous
/// callers and for any deployment that hasn't configured `runtime.auth`.
const DEFAULT_DESCRIPTION: &str = "Execute an SQL query in the Spice.ai SQL Dialect (DataFusion SQL using PostgreSQL-dialect with Spice.ai functions) against the Spice runtime and return the result rows as JSON. Use this for any deterministic lookup, aggregation, or filter where exact column names are known; prefer `search` for semantic or natural-language lookups. Quote identifiers containing capitals or reserved keywords, and quote each part of `catalog.schema.table` separately (e.g. \"spice\".\"public\".\"my_table\"). Avoid `SELECT *` and columns ending in `_offset` or `_embedding`. Write statements (INSERT/UPDATE/DELETE/DDL) require the request to be authenticated with a ReadWrite API key AND the target dataset to be configured `access: read_write`; otherwise they are rejected. Use `list_datasets` and `table_schema` first to discover tables and columns.";

pub struct SqlTool {
    name: String,
    description: String,
    df: Arc<dyn QueryEngine>,

    allowed_tables: Option<ResolvedTableAwareAllowlist>,
}

impl SqlTool {
    #[must_use]
    pub fn new(
        df: Arc<dyn QueryEngine>,
        name: Option<&str>,
        description: Option<&str>,
        allowed_tables: Option<ResolvedTableAwareAllowlist>,
    ) -> Self {
        Self {
            df,
            name: name.unwrap_or("sql").to_string(),
            description: description.unwrap_or(DEFAULT_DESCRIPTION).to_string(),
            allowed_tables,
        }
    }
}

#[async_trait]
impl SpiceModelTool for SqlTool {
    fn name(&self) -> Cow<'_, str> {
        Cow::Borrowed(&self.name)
    }

    fn description(&self) -> Option<Cow<'_, str>> {
        Some(Cow::Borrowed(&self.description))
    }

    fn parameters(&self) -> Option<Value> {
        parameters::<SqlToolParams>()
    }

    async fn call(&self, arg: &str) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let span: Span = tracing::span!(target: "task_history", tracing::Level::INFO, "tool_use::sql", tool = self.name().to_string(), input = arg);
        let tool_use_result: Result<Value, Box<dyn std::error::Error + Send + Sync>> = async {
            let SqlToolParams { query } = serde_json::from_str(arg)?;

            // Defer the read-only gate to the calling principal. The strict
            // read-only validator still fires for ReadOnly / anonymous
            // callers (and for deployments that haven't configured
            // `runtime.auth` at all, where `current_principal_requires_read_only`
            // returns `false` only because there is no authenticated
            // principal — but on the tools surface those requests are
            // rejected upstream by `require_auth_configured`). ReadWrite
            // callers fall through to the regular validator, which still
            // checks per-table writability and `access: read_write`.
            let read_only = current_principal_requires_read_only().await;

            let mut query_request = QueryRequest::new(&query).read_only(read_only);

            if let Some(ref allowlist) = self.allowed_tables {
                query_request = query_request.allow_tables(allowlist.clone());
            }

            let batches = self
                .df
                .execute_query(query_request)
                .await?
                .try_collect::<Vec<RecordBatch>>()
                .await
                .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })?;

            Ok(Value::String(write_to_json_string(&batches)?))
        }
        .instrument(span.clone())
        .await;

        match tool_use_result {
            Ok(value) => {
                let captured_output_json = serde_json::to_string(&value)?;
                tracing::info!(target: "task_history", parent: &span, captured_output = %captured_output_json);
                Ok(value)
            }
            Err(e) => {
                tracing::error!(target: "task_history", parent: &span, "{e}");
                Err(e)
            }
        }
    }
}
