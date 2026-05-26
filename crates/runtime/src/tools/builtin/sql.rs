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

use crate::{
    datafusion::query::write_to_json_string,
    tools::{SpiceModelTool, utils::parameters},
};
use futures::TryStreamExt;
use runtime_datafusion::allowlist::ResolvedTableAwareAllowlist;
use runtime_datafusion::query_engine::{QueryEngine, QueryRequest};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::Span;
use tracing_futures::Instrument;

#[derive(Debug, Clone, JsonSchema, Serialize, Deserialize)]
pub struct SqlToolParams {
    /// The SQL query to run, written in the Spice.ai SQL Dialect (`DataFusion` SQL using PostgreSQL-dialect with Spice.ai functions). Double quote all select columns and never select columns ending in '_embedding'. The `table_catalog` is 'spice'. Always use it in the query.
    query: String,
}

/// Default description advertised to LLMs / tool selection when the `sql` tool
/// is in its read-only posture (the default).
const DEFAULT_READ_ONLY_DESCRIPTION: &str = "Execute a read-only SQL query in the Spice.ai SQL Dialect (DataFusion SQL using PostgreSQL-dialect with Spice.ai functions) against the Spice runtime and return the result rows as JSON. Use this for any deterministic lookup, aggregation, or filter where exact column names are known; prefer `search` for semantic or natural-language lookups. Quote identifiers containing capitals or reserved keywords, and quote each part of `catalog.schema.table` separately (e.g. \"spice\".\"public\".\"my_table\"). Avoid `SELECT *` and columns ending in `_offset` or `_embedding`. DDL and write statements (INSERT/UPDATE/DELETE/COPY/CREATE/DROP) and session-mutating statements (PREPARE/EXECUTE/DEALLOCATE) are rejected; use `list_datasets` and `table_schema` first to discover tables and columns.";

/// Default description advertised to LLMs / tool selection when the operator
/// has opted the tool into writable mode via [`SqlTool::allow_writes`].
const DEFAULT_WRITABLE_DESCRIPTION: &str = "Execute an SQL query in the Spice.ai SQL Dialect (DataFusion SQL using PostgreSQL-dialect with Spice.ai functions) against the Spice runtime and return the result rows as JSON. This variant accepts write statements (INSERT/UPDATE/DELETE/DDL) - use with caution and only when mutation is the explicit goal. Quote identifiers containing capitals or reserved keywords, and quote each part of `catalog.schema.table` separately (e.g. \"spice\".\"public\".\"my_table\"). Avoid `SELECT *` and columns ending in `_offset` or `_embedding`. Use `list_datasets` and `table_schema` first to discover tables and columns.";

pub struct SqlTool {
    name: String,
    description: String,
    df: Arc<dyn QueryEngine>,

    allowed_tables: Option<ResolvedTableAwareAllowlist>,
    /// When true (the default), the tool rejects any DDL, DML, COPY, or
    /// `LogicalPlan::Statement` plan (including PREPARE/EXECUTE/DEALLOCATE) at
    /// execution time. This prevents LLM- or caller-supplied SQL from mutating data
    /// or session state via the `/v1/tools/sql` surface, even when a referenced
    /// catalog/dataset is configured writable. Operators that need write access from
    /// a tool should configure a distinct writable tool rather than flipping this flag.
    read_only: bool,
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
            description: description
                .unwrap_or(DEFAULT_READ_ONLY_DESCRIPTION)
                .to_string(),
            allowed_tables,
            read_only: true,
        }
    }

    /// Allow write statements (INSERT/UPDATE/DELETE/DDL). Defaults to off.
    ///
    /// This is an escape hatch for operators who have deliberately configured a
    /// separate writable tool and understand that any LLM with tool-use access will
    /// then be able to mutate the targeted catalog/dataset without per-call
    /// confirmation. Leave the default in place unless that trade-off is acceptable.
    ///
    /// If the tool is still using the default read-only description, it is swapped
    /// for the writable default so LLM/tool-selection logic is not misled by a
    /// stale "read-only" advertisement. Operator-supplied descriptions are left
    /// untouched — callers overriding the description are responsible for keeping
    /// it accurate.
    #[must_use]
    pub fn allow_writes(mut self) -> Self {
        self.read_only = false;
        if self.description == DEFAULT_READ_ONLY_DESCRIPTION {
            self.description = DEFAULT_WRITABLE_DESCRIPTION.to_string();
        }
        self
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
            let req: SqlToolParams = serde_json::from_str(arg)?;

            let mut query_request = QueryRequest::new(&req.query).read_only(self.read_only);
            if let Some(ref allowlist) = self.allowed_tables {
                query_request = query_request.allow_tables(allowlist.clone());
            }

            let batches = self
                .df
                .execute_query(query_request)
                .await?
                .try_collect::<Vec<RecordBatch>>()
                .await
                .boxed()?;

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
