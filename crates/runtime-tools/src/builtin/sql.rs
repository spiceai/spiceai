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
use datafusion::sql::parser::DFParser;
use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use datafusion_ddl::component_options::{
    UNREADABLE_STATEMENT, recorded_statement, recorded_unparsed_statement, redact_values,
};
use std::{borrow::Cow, sync::Arc};

use crate::builtin::function_tool::current_principal_allows_write;
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
const DEFAULT_DESCRIPTION: &str = "Execute an SQL query in the Spice.ai SQL Dialect (DataFusion SQL using PostgreSQL-dialect with Spice.ai functions) against the Spice runtime and return the result rows as JSON. Use this for any deterministic lookup, aggregation, or filter where exact column names are known; prefer `search` for semantic or natural-language lookups. Quote identifiers containing capitals or reserved keywords, and quote each part of `catalog.schema.table` separately (e.g. \"spice\".\"public\".\"my_table\"). Avoid `SELECT *` and columns ending in `_offset` or `_embedding`. Write statements (INSERT/UPDATE/DELETE, and DDL in a catalog configured `access: read_write_create`) require the request to be authenticated with a ReadWrite API key AND the target to be writable, such as a dataset configured `access: read_write`; defining, changing, or dropping a dataset or catalog (`CREATE TABLE ... WITH (from = ...)`, `CREATE CATALOG`, and `ALTER` or `DROP` of one) requires an admin API key; otherwise they are rejected. Use `list_datasets` and `table_schema` first to discover tables and columns.";

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

    fn recorded_arguments<'a>(&self, arg: &'a str) -> (Cow<'a, str>, Vec<String>) {
        recorded_input(arg)
    }

    async fn call(&self, arg: &str) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let (input, removed) = self.recorded_arguments(arg);
        let span: Span = tracing::span!(target: "task_history", tracing::Level::INFO, "tool_use::sql", tool = self.name().to_string(), input = %input);
        let tool_use_result: Result<Value, Box<dyn std::error::Error + Send + Sync>> = async {
            let SqlToolParams { query } = serde_json::from_str(arg)?;

            // Only a principal with write access may write; with no principal (no
            // `runtime.auth`) the query is read-only. Writes still need the target
            // to be writable (`access: read_write`).
            let read_only = !current_principal_allows_write().await;

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
                let message = e.to_string();
                let message = redact_values(&message, &removed);
                tracing::error!(target: "task_history", parent: &span, "{message}");
                Err(e)
            }
        }
    }
}

/// The tool's arguments as task history records them, and the values that
/// takes out: the statement they carry recorded as the query it runs is, but
/// with every parameter's value redacted, since the tool cannot tell which are
/// non-secret. A statement that does not parse is recorded with
/// [`recorded_unparsed_statement`]. Arguments that are not the tool's JSON run
/// nothing, and are recorded only as [`UNREADABLE_STATEMENT`].
fn recorded_input(arg: &str) -> (Cow<'_, str>, Vec<String>) {
    let Ok(SqlToolParams { query }) = serde_json::from_str::<SqlToolParams>(arg) else {
        return (Cow::Borrowed(UNREADABLE_STATEMENT), Vec::new());
    };
    let dialect = PostgreSqlDialect {};
    let recorded = if DFParser::parse_sql_with_dialect(&query, &dialect).is_ok() {
        recorded_statement(&query, &dialect, &|_| false)
    } else {
        recorded_unparsed_statement(&query, &dialect, &|_| false)
    };
    match recorded.text {
        Cow::Borrowed(_) => (Cow::Borrowed(arg), Vec::new()),
        Cow::Owned(text) => (
            Cow::Owned(serde_json::json!({ "query": text }).to_string()),
            recorded.removed,
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The query a tool call carries is recorded without its credentials, as
    /// the statement it runs is, whether or not it parses: one that does not
    /// may be a mistyped definition.
    #[test]
    fn a_tool_call_records_its_statement_without_credentials() {
        for (arg, recorded, error, recorded_error) in [
            (
                r#"{"query": "CREATE TABLE t WITH (from = 'postgres:public.t', pg_host = 'db', pg_pass = 'hunter2')"}"#,
                r#"{"query":"CREATE TABLE t WITH (from = 'postgres:public.t', pg_host = '<redacted>', pg_pass = '<redacted>')"}"#,
                "password for 'hunter2' refused",
                "password for <redacted> refused",
            ),
            (
                r#"{"query": "CRATE TABLE t WITH (pg_host = 'db', pg_pass = 'hunter2')"}"#,
                r#"{"query":"CRATE TABLE t WITH (pg_host = '<redacted>', pg_pass = '<redacted>')"}"#,
                "SQL error: ParserError(\"Expected: an SQL statement, found: 'hunter2'\")",
                "SQL error: ParserError(\"Expected: an SQL statement, found: <redacted>\")",
            ),
        ] {
            let (input, removed) = recorded_input(arg);
            assert_eq!(input, recorded);
            assert!(
                removed.iter().any(|value| value == "hunter2"),
                "{removed:?}"
            );
            assert_eq!(redact_values(error, &removed), recorded_error);
        }

        // A statement that parses and defines nothing is recorded as sent.
        for arg in [
            r#"{"query": "SELECT * FROM t WHERE name = 'a'"}"#,
            r#"{"query": "DROP TABLE t -- the old copy"}"#,
        ] {
            assert!(matches!(recorded_input(arg).0, Cow::Borrowed(text) if text == arg));
        }
    }

    /// Arguments that are not the tool's JSON run nothing, and are recorded
    /// only as a placeholder, whatever they carry.
    #[test]
    fn arguments_that_are_not_the_tools_json_are_not_recorded() {
        for arg in [
            "CREATE TABLE t WITH (pg_pass = 'hunter2')",
            r#"{"sql": "CRATE TABLE t WITH (pg_pass = 'hunter2')"}"#,
            r#"{"query": "CRATE TABLE t WITH (pg_pass = 'hunter2')""#,
            "not json",
        ] {
            assert_eq!(
                recorded_input(arg),
                (Cow::Borrowed(UNREADABLE_STATEMENT), Vec::new()),
                "{arg}"
            );
        }
    }
}
