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

use std::{collections::HashSet, sync::Arc};

use datafusion::common::ParamValues;
use datafusion::logical_expr::LogicalPlan;
use runtime_datafusion::allowlist::ResolvedTableAwareAllowlist;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::datafusion::{DataFusion, query::QueryMethod};

use super::{Query, tracker::QueryTracker};

enum SqlOrPlan<'a> {
    Sql(&'a str),
    /// Pre-parsed plan with the original SQL retained for cache key compatibility.
    Plan(Box<LogicalPlan>, Arc<str>),
}

pub struct QueryBuilder<'a> {
    df: Arc<DataFusion>,
    method: SqlOrPlan<'a>,
    parameters: Option<ParamValues>,
    table_allowlist: Option<ResolvedTableAwareAllowlist>,
    query_id: Uuid,
    cancellation_token: Option<CancellationToken>,
    read_only: bool,
}

impl<'a> QueryBuilder<'a> {
    pub fn new(sql: &'a str, df: Arc<DataFusion>) -> Self {
        Self {
            df,
            method: SqlOrPlan::Sql(sql),
            parameters: None,
            query_id: Uuid::new_v4(),
            table_allowlist: None,
            cancellation_token: None,
            read_only: false,
        }
    }

    /// Build a `QueryBuilder` from a pre-parsed [`LogicalPlan`] and its original SQL.
    ///
    /// The SQL string is retained so that results-cache lookups use the same key
    /// as an equivalent [`QueryBuilder::new`] call, meaning cached SELECT results
    /// written by one path are visible to the other.
    ///
    /// Use this when the plan has already been parsed externally (e.g. for
    /// read-only auth validation) to avoid re-parsing.
    pub fn from_plan(plan: LogicalPlan, sql: impl Into<Arc<str>>, df: Arc<DataFusion>) -> Self {
        Self {
            df,
            method: SqlOrPlan::Plan(Box::new(plan), sql.into()),
            parameters: None,
            query_id: Uuid::new_v4(),
            table_allowlist: None,
            read_only: false,
            cancellation_token: None,
        }
    }

    #[must_use]
    pub fn allow_tables(mut self, allowed_tables: ResolvedTableAwareAllowlist) -> Self {
        self.table_allowlist = Some(allowed_tables);
        self
    }

    #[must_use]
    pub fn query_id(mut self, query_id: Uuid) -> Self {
        self.query_id = query_id;
        self
    }

    #[must_use]
    pub fn parameters(mut self, parameters: Option<ParamValues>) -> Self {
        self.parameters = parameters;
        self
    }

    /// Supplies a cancellation token that this query will observe. When the
    /// token is cancelled, any in-flight stream produced by this query yields
    /// a cancellation error and terminates.
    ///
    /// If this is unset, the query attempts to observe the current
    /// [`runtime_request_context::RequestContext`]'s cancellation token.
    #[must_use]
    pub fn cancellation_token(mut self, token: CancellationToken) -> Self {
        self.cancellation_token = Some(token);
        self
    }

    /// Enforce read-only SQL execution.
    ///
    /// When enabled, the planned query is additionally checked with
    /// [`crate::datafusion::sql_validator::validate_sql_query_read_only`] and rejected if it
    /// contains any DDL, DML, COPY, or `LogicalPlan::Statement` node (including
    /// `PREPARE`/`EXECUTE`/`DEALLOCATE`) — regardless of whether the target
    /// catalogs/datasets are individually marked writable.
    ///
    /// Used by surfaces that execute SQL on behalf of an LLM or unauthenticated caller
    /// (the built-in `sql` tool, `/v1/nsql`).
    #[must_use]
    pub fn read_only(mut self, read_only: bool) -> Self {
        self.read_only = read_only;
        self
    }

    #[must_use]
    pub fn build(self) -> Query {
        let tracker = if self.df.task_history_enabled {
            Some(QueryTracker {
                schema: None,
                query_duration_secs: None,
                query_execution_duration_secs: None,
                rows_produced: 0,
                results_cache_hit: None,
                is_accelerated: None,
                error_message: None,
                error_code: None,
                query_duration_timer: Instant::now(),
                query_execution_duration_timer: Instant::now(),
                datasets: Arc::new(HashSet::default()),
            })
        } else {
            None
        };

        let query_method = match self.method {
            SqlOrPlan::Sql(sql) => QueryMethod::Text {
                sql: sql.into(),
                parameters: self.parameters,
                table_allowlist: self.table_allowlist,
                pre_parsed_plan: None,
            },
            SqlOrPlan::Plan(plan, sql) => QueryMethod::Text {
                sql,
                parameters: self.parameters,
                table_allowlist: self.table_allowlist,
                pre_parsed_plan: Some(plan),
            },
        };

        Query {
            df: Arc::clone(&self.df),
            sql: query_method,
            tracker,
            query_id: self.query_id,
            cancellation_token: self.cancellation_token,
            read_only: self.read_only,
            allow_accelerated: false,
        }
    }
}
