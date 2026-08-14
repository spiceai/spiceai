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

use crate::function_support::FunctionSupport;
use crate::sql_expr::to_sql_preserving_precedence;
use arrow::{
    array::{Array, RecordBatch, array},
    compute::cast,
    datatypes::Schema,
};
use async_stream::stream;
use async_trait::async_trait;
use flight_client::{
    MAX_DECODING_MESSAGE_SIZE, MAX_ENCODING_MESSAGE_SIZE,
    cookie::{CookieService, CookieStore},
    tls::new_tls_flight_channel,
};
use futures::{Stream, StreamExt, TryStreamExt};
use snafu::prelude::*;
use std::{fmt, sync::Arc, vec};

use arrow_flight::{
    FlightEndpoint, IpcMessage,
    error::FlightError,
    flight_service_client::FlightServiceClient,
    sql::{CommandGetTables, client::FlightSqlServiceClient},
};
use datafusion::{
    arrow::datatypes::SchemaRef,
    catalog::Session,
    common::Statistics,
    common::utils::quote_identifier,
    datasource::TableProvider,
    error::{DataFusionError, Result as DataFusionResult},
    execution::TaskContext,
    logical_expr::{Expr, TableProviderFilterPushDown, TableType},
    physical_expr::{EquivalenceProperties, LexOrdering, PhysicalSortExpr, expressions::Column},
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
        SendableRecordBatchStream, SortOrderPushdownResult,
        execution_plan::{Boundedness, EmissionType},
        metrics::{ExecutionPlanMetricsSet, MetricBuilder, MetricsSet, Time},
        project_schema,
        stream::RecordBatchStreamAdapter,
    },
    sql::TableReference,
};
use runtime_request_context::RequestContext;
use tonic::codegen::Bytes;
use tonic::transport::{Channel, channel};

use crate::Read;

/// Build a W3C `traceparent` header value (`00-{trace_id}-{span_id}-01`)
/// from the typed `Arc<RequestContext>` extension on the
/// [`TaskContext`]'s session config, if one is present. The fixed `01`
/// trace flag marks the trace as sampled.
#[must_use]
pub fn trace_parent_from_task_context(context: &TaskContext) -> Option<String> {
    let request_context = context.session_config().get_extension::<RequestContext>()?;
    let tp = request_context.trace_parent().as_ref()?;
    Some(format!("00-{}-{}-01", tp.trace_id, tp.span_id))
}

pub mod federation;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Failed to connect to the Flight server. {source} Verify configuration and try again. For details, visit https://spiceai.org/docs/components/data-connectors/flightsql#params"
    ))]
    UnableToConnectToServer { source: tonic::transport::Error },

    #[snafu(display(
        "Failed to create SQL query (flightsql). {source} An unexpected error occurred. Report a bug on GitHub: https://github.com/spiceai/spiceai/issues"
    ))]
    UnableToGenerateSQL { source: DataFusionError },

    #[snafu(display("Query execution failed (flightsql). {source}"))]
    UnableToQueryArrowFlight { source: FlightError },

    #[snafu(display(
        "Failed to retrieve table {table_name} schema (flightsql). {source} An internal error occurred. Report a bug on GitHub: https://github.com/spiceai/spiceai/issues"
    ))]
    UnableToRetrieveSchemaFromIpcMessage {
        source: arrow::error::ArrowError,
        table_name: String,
    },

    #[snafu(display(
        "Failed to detect table '{table_name}' schema (flightsql). {source} Verify the connection and try again. If the issue persists, report a bug on GitHub: https://github.com/spiceai/spiceai/issues"
    ))]
    UnableToRetrieveSchemaArrow {
        source: arrow::error::ArrowError,
        table_name: String,
    },

    #[snafu(display(
        "Failed to detect table '{table_name}' schema (flightsql). {source} Verify the connection and try again. If the issue persists, report a bug on GitHub: https://github.com/spiceai/spiceai/issues"
    ))]
    UnableToRetrieveSchemaFlight {
        source: FlightError,
        table_name: String,
    },

    #[snafu(display(
        "Failed to detect table '{table_name}' schema (flightsql). Ensure the table exists and try again."
    ))]
    UnableToRetrieveSchema { table_name: String },

    #[snafu(display("Invalid sort expression in sort pushdown: expected Column, got {expr}"))]
    InvalidSortExpression { expr: String },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub type FlightSqlClient = FlightSqlServiceClient<CookieService<Channel>>;

#[derive(Clone)]
pub struct FlightSQLFactory {
    client: FlightSqlClient,
    endpoint: String,
    cookie_store: Arc<CookieStore>,
    function_support: Option<FunctionSupport>,
    token: Option<String>,
}

impl FlightSQLFactory {
    #[must_use]
    pub fn new(client: FlightSqlClient, endpoint: String, cookie_store: Arc<CookieStore>) -> Self {
        Self {
            client,
            endpoint,
            cookie_store,
            function_support: None,
            token: None,
        }
    }

    /// Install the federation function deny-list so Spice-only UDFs are evaluated
    /// locally instead of pushed into the Flight SQL server (#10703).
    #[must_use]
    pub fn with_function_support(mut self, function_support: FunctionSupport) -> Self {
        self.function_support = Some(function_support);
        self
    }

    #[must_use]
    pub fn with_token(mut self, token: String) -> Self {
        self.token = Some(token);
        self
    }
}

impl std::fmt::Debug for FlightSQLFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FlightSQLFactory")
            .field("endpoint", &self.endpoint)
            .field("token", &self.token.as_ref().map(|_| "<redacted>"))
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl Read for FlightSQLFactory {
    async fn table_provider(
        &self,
        table_reference: TableReference,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn std::error::Error + Send + Sync>> {
        let mut table = FlightSQLTable::create(
            "flightsql",
            &self.endpoint,
            self.client.clone(),
            table_reference,
            Arc::clone(&self.cookie_store),
        )
        .await?
        .with_function_support(self.function_support.clone());
        if let Some(token) = &self.token {
            table = table.with_token(token.clone());
        }
        let table_provider = Arc::new(table);

        let table_provider = Arc::new(table_provider.create_federated_table_provider());

        Ok(table_provider)
    }
}

impl std::fmt::Debug for FlightSQLTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FlightSQLTable")
            .field("name", &self.name)
            .field("table_reference", &self.table_reference)
            .field("token", &self.token.as_ref().map(|_| "<redacted>"))
            .finish_non_exhaustive()
    }
}

pub struct FlightSQLTable {
    name: &'static str,
    join_push_down_context: String,
    client: FlightSqlClient,
    table_reference: TableReference,
    schema: SchemaRef,
    cookie_store: Arc<CookieStore>,
    /// Optional statistics to attach to the scan this provider produces.
    statistics: Option<Statistics>,
    /// Federation function deny-list. Functions on the list (Spice-only UDFs such
    /// as `json_get_str`) are evaluated locally instead of pushed into the SQL
    /// sent to the Flight SQL server. See issue #10703.
    function_support: Option<FunctionSupport>,
    token: Option<String>,
    /// When `Some`, the scans this provider creates render their FROM clause from
    /// this raw table-function call instead of `table_reference`. Used by
    /// distributed full-text search, where the source is a UDTF, not a plain table.
    from_function: Option<String>,
}

#[expect(clippy::needless_pass_by_value)]
impl FlightSQLTable {
    pub async fn create(
        name: &'static str,
        endpoint: &str,
        client: FlightSqlClient,
        table_reference: impl Into<TableReference>,
        cookie_store: Arc<CookieStore>,
    ) -> Result<Self> {
        let table_reference: TableReference = table_reference.into();
        let schema = Self::get_schema(client.clone(), table_reference.clone()).await?;
        Ok(Self {
            name,
            client,
            table_reference,
            schema,
            join_push_down_context: format!("endpoint={endpoint}"),
            cookie_store,
            statistics: None,
            function_support: None,
            token: None,
            from_function: None,
        })
    }

    pub fn create_with_schema(
        name: &'static str,
        endpoint: &str,
        client: FlightSqlClient,
        table_reference: impl Into<TableReference>,
        schema: SchemaRef,
        cookie_store: Arc<CookieStore>,
    ) -> Self {
        let table_reference: TableReference = table_reference.into();
        Self {
            name,
            client,
            table_reference,
            schema,
            join_push_down_context: format!("endpoint={endpoint}"),
            cookie_store,
            statistics: None,
            function_support: None,
            token: None,
            from_function: None,
        }
    }

    /// Render the scans' FROM clause from a raw table-function call (a valid
    /// quoted UDTF invocation) instead of the plain `table_reference`.
    #[must_use]
    pub fn with_from_function(mut self, from_function: String) -> Self {
        self.from_function = Some(from_function);
        self
    }

    /// Attach statistics to be reported by the scans this provider creates.
    #[must_use]
    pub fn with_statistics(mut self, statistics: Option<Statistics>) -> Self {
        self.statistics = statistics;
        self
    }

    /// Install the federation function deny-list (see [`FunctionSupport`]).
    #[must_use]
    pub fn with_function_support(mut self, function_support: Option<FunctionSupport>) -> Self {
        self.function_support = function_support;
        self
    }

    /// Set the bearer token to propagate to per-endpoint `DoGet` clients.
    #[must_use]
    pub fn with_token(mut self, token: String) -> Self {
        self.token = Some(token);
        self
    }

    pub async fn from_static(
        s: &'static str,
        table_reference: impl Into<TableReference>,
    ) -> Result<Self> {
        let cookie_store = Arc::new(CookieStore::new());
        let channel = channel::Endpoint::from_static(s)
            .connect()
            .await
            .context(UnableToConnectToServerSnafu)?;
        let channel = CookieService::new(channel, Arc::clone(&cookie_store));

        let flight_client = FlightServiceClient::new(channel)
            .max_encoding_message_size(MAX_ENCODING_MESSAGE_SIZE)
            .max_decoding_message_size(MAX_DECODING_MESSAGE_SIZE);

        Self::create(
            "flightsql",
            s,
            FlightSqlServiceClient::new_from_inner(flight_client),
            table_reference.into(),
            cookie_store,
        )
        .await
    }

    fn get_str_from_record_batch(b: &RecordBatch, row: usize, col_name: &str) -> Option<String> {
        if let Some(col_array) = b.column_by_name(col_name)
            && let Some(y) = col_array.as_any().downcast_ref::<array::StringArray>()
        {
            return Some(y.value(row).to_string());
        }
        None
    }

    #[must_use]
    pub fn get_table_schema_if_present(
        batches: Vec<RecordBatch>,
        table_reference: TableReference,
    ) -> Option<SchemaRef> {
        let mut possible_schema_bytz: Vec<Vec<u8>> = vec![];

        for b in batches {
            if let Some(table_schema) = b
                .column_by_name("table_schema")
                .and_then(|ts_array| ts_array.as_any().downcast_ref::<array::BinaryArray>())
                .or(None)
            {
                possible_schema_bytz.extend((0..b.num_rows()).filter_map(|i| {
                    let table_name =
                        Self::get_str_from_record_batch(&b, i, "table_name").unwrap_or_default();
                    let catalog_name =
                        Self::get_str_from_record_batch(&b, i, "catalog_name").unwrap_or_default();
                    let db_schema_name = Self::get_str_from_record_batch(&b, i, "db_schema_name")
                        .unwrap_or_default();

                    // Only check fields in `table_reference` matches.
                    if table_reference.resolved_eq(&TableReference::full(
                        catalog_name,
                        db_schema_name,
                        table_name,
                    )) {
                        Some(table_schema.value(i).to_vec())
                    } else {
                        None
                    }
                }));
            }
        }
        match possible_schema_bytz.len() {
            1 => {
                if let Some(bytz) = possible_schema_bytz.first() {
                    match Schema::try_from(IpcMessage(Bytes::copy_from_slice(bytz))).context(
                        UnableToRetrieveSchemaFromIpcMessageSnafu {
                            table_name: table_reference.to_string(),
                        },
                    ) {
                        Ok(schema) => Some(Arc::new(schema)),
                        Err(e) => {
                            tracing::error!(
                                "Error converting schema from 'table_schema' column: {e}"
                            );
                            None
                        }
                    }
                } else {
                    None
                } // Not possible due to match 1.
            }
            0 => None,
            _ => {
                tracing::error!("Multiple schemas found for table_reference: {table_reference}");
                None
            }
        }
    }

    pub async fn get_schema(
        mut client: FlightSqlClient,
        table_reference: TableReference,
    ) -> Result<SchemaRef> {
        // Preferred path: the Flight SQL `GetTables` metadata RPC (best-effort).
        if let Ok(flight_info) = client
            .get_tables(CommandGetTables {
                catalog: table_reference.catalog().map(ToString::to_string),
                db_schema_filter_pattern: table_reference.schema().map(ToString::to_string),
                table_name_filter_pattern: Some(table_reference.table().to_string()),
                include_schema: true,
                table_types: [
                    "TABLE",
                    "BASE TABLE",
                    "VIEW",
                    "LOCAL TEMPORARY",
                    "SYSTEM TABLE",
                ]
                .iter()
                .map(|&s| s.into())
                .collect(),
            })
            .await
        {
            for tkt in flight_info
                .endpoint
                .iter()
                .filter_map(|ep| ep.ticket.as_ref())
            {
                // Schema: https://github.com/apache/arrow/blob/44edc27e549d82db930421b0d4c76098941afd71/format/FlightSql.proto#L1182-L1190
                if let Ok(stream) = client.do_get(tkt.clone()).await
                    && let Ok(batch) = stream.try_collect::<Vec<_>>().await
                    && let Some(schema) =
                        Self::get_table_schema_if_present(batch, table_reference.clone())
                {
                    return Ok(schema);
                }
            }
        }

        // Fallback for Flight SQL servers that don't implement the `GetTables` metadata RPC
        // (e.g. StarRocks' experimental Flight SQL, which returns `Unimplemented`): infer the
        // schema from `SELECT * FROM <table> LIMIT 1`, served by the statement-execution path.
        let flight_info = client
            .execute(format!("SELECT * FROM {table_reference} LIMIT 1"), None)
            .await
            .context(UnableToRetrieveSchemaFlightSnafu {
                table_name: table_reference.to_string(),
            })?;
        for tkt in flight_info
            .endpoint
            .iter()
            .filter_map(|ep| ep.ticket.as_ref())
        {
            let stream =
                client
                    .do_get(tkt.clone())
                    .await
                    .context(UnableToRetrieveSchemaFlightSnafu {
                        table_name: table_reference.to_string(),
                    })?;
            let batches = stream.try_collect::<Vec<_>>().await.context(
                UnableToRetrieveSchemaFlightSnafu {
                    table_name: table_reference.to_string(),
                },
            )?;
            if let Some(batch) = batches.first() {
                return Ok(batch.schema());
            }
        }

        UnableToRetrieveSchemaSnafu {
            table_name: table_reference.to_string(),
        }
        .fail()
    }

    fn create_physical_plan(
        &self,
        projections: Option<&Vec<usize>>,
        schema: &SchemaRef,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let exec = FlightSqlExec::new(
            projections,
            schema,
            &self.table_reference,
            self.client.clone(),
            filters,
            limit,
            Arc::clone(&self.cookie_store),
        )?;
        let exec = exec.with_token(self.token.clone());
        let exec = exec.with_from_function(self.from_function.clone());
        // Project the table-level statistics onto the scan's (projected) output
        // schema so the column-statistics list lines up with the output columns.
        //
        // The stamped statistics describe the *whole* slice the executor reported
        // (its row count and column bounds), so they only stay `Exact` for a scan
        // that returns that whole slice. Both a pushed-down filter and a
        // pushed-down limit narrow what the remote query returns:
        //
        // - Every pushdown-eligible filter is reported `Exact` (see
        //   `supports_filters_pushdown`), so DataFusion drops the coordinator-side
        //   `FilterExec`.
        // - `LIMIT` is emitted into the remote SQL (see `FlightSqlExec::sql`), and
        //   `supports_limit_pushdown` lets `LimitPushdown` drop the coordinator-side
        //   `GlobalLimitExec`.
        //
        // Either way, leaving `num_rows`/bounds `Exact` lets the
        // `aggregate_statistics` optimizer rule fold `COUNT(*)`/`MIN`/`MAX` to
        // values the scan cannot produce — silently ignoring the predicate (a
        // filtered `COUNT(*)` returning the full table count, issue #11599) or the
        // limit (issue #12292). Marking the statistics inexact disables that fold
        // while keeping the bounds usable for join sizing.
        let exec = match &self.statistics {
            Some(stats) => {
                let stats = stats.clone().project(projections);
                let stats = if filters.is_empty() && limit.is_none() {
                    stats
                } else {
                    stats.to_inexact()
                };
                exec.with_statistics(stats)
            }
            None => exec,
        };
        Ok(Arc::new(exec))
    }
}

#[async_trait]
impl TableProvider for FlightSQLTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        // A table-function FROM clause cannot absorb predicates, so no filter can
        // be pushed down when the scan's source is a UDTF.
        if self.from_function.is_some() {
            return Ok(vec![
                TableProviderFilterPushDown::Unsupported;
                filters.len()
            ]);
        }

        let mut filter_push_down = vec![];
        for filter in filters {
            match to_sql_preserving_precedence(filter) {
                Ok(_) => {
                    filter_push_down.push(TableProviderFilterPushDown::Exact);
                }
                Err(_) => filter_push_down.push(TableProviderFilterPushDown::Unsupported),
            }
        }

        Ok(filter_push_down)
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        return self.create_physical_plan(projection, &self.schema(), filters, limit);
    }
}

#[derive(Clone)]
pub struct FlightSqlExec {
    projected_schema: SchemaRef,
    table_reference: TableReference,
    client: FlightSqlClient,
    filters: Vec<Expr>,
    limit: Option<usize>,
    sort_exprs: Vec<PhysicalSortExpr>,
    properties: Arc<PlanProperties>,
    cookie_store: Arc<CookieStore>,
    metrics: ExecutionPlanMetricsSet,
    /// Optional W3C `traceparent` value (e.g. `00-{trace_id}-{span_id}-01`)
    /// to attach as a gRPC metadata header on outgoing `execute()` and
    /// `do_get()` calls. When `None`, `execute()` falls back to reading
    /// the typed `Arc<RequestContext>` extension from the `TaskContext`
    /// session config and constructs a header from its `trace_parent()`.
    trace_parent: Option<String>,
    /// Statistics for the rows this scan produces. Defaults to unknown; the
    /// planner that builds the scan can supply real statistics (e.g. row
    /// counts sourced from cluster partition metadata) via
    /// [`FlightSqlExec::with_statistics`] so downstream optimizer rules such as
    /// hash-join build-side selection can use them.
    statistics: Statistics,
    token: Option<String>,
    /// When `Some`, renders the FROM clause of the emitted SQL from this raw
    /// string (a table-function call such as
    /// `text_search_stats("cat"."sch"."tbl", 'query', "col")`) instead of the
    /// quoted `table_reference`. Used by distributed full-text search, where the
    /// scan leg's source is a UDTF rather than a plain table.
    from_function: Option<String>,
}

impl FlightSqlExec {
    pub fn new(
        projections: Option<&Vec<usize>>,
        schema: &SchemaRef,
        table_reference: &TableReference,
        client: FlightSqlClient,
        filters: &[Expr],
        limit: Option<usize>,
        cookie_store: Arc<CookieStore>,
    ) -> DataFusionResult<Self> {
        let projected_schema = project_schema(schema, projections)?;
        let statistics = Statistics::new_unknown(&projected_schema);
        Ok(Self {
            projected_schema: Arc::clone(&projected_schema),
            table_reference: table_reference.clone(),
            client,
            filters: filters.to_vec(),
            limit,
            sort_exprs: Vec::new(),
            properties: Arc::new(PlanProperties::new(
                EquivalenceProperties::new(projected_schema),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            )),
            cookie_store,
            metrics: ExecutionPlanMetricsSet::new(),
            trace_parent: None,
            statistics,
            token: None,
            from_function: None,
        })
    }

    /// Attach statistics describing the rows this scan produces. The schema of
    /// the statistics' column list is expected to match the (projected) output
    /// schema; callers that only know the row count can use
    /// `Statistics::new_unknown(schema).with_num_rows(...)`.
    #[must_use]
    pub fn with_statistics(mut self, statistics: Statistics) -> Self {
        self.statistics = statistics;
        self
    }

    /// Set an explicit W3C `traceparent` header value to forward on each
    /// outgoing `FlightSQL` call. Useful when the plan-creation path has
    /// access to an `Arc<RequestContext>` but the executor-side
    /// `TaskContext` will not (e.g. when this `ExecutionPlan` is shipped
    /// to a remote executor via Ballista codecs).
    #[must_use]
    pub fn with_trace_parent(mut self, trace_parent: Option<String>) -> Self {
        self.trace_parent = trace_parent;
        self
    }

    #[must_use]
    pub fn with_token(mut self, token: Option<String>) -> Self {
        self.token = token;
        self
    }

    /// Render the FROM clause from a raw table-function call (e.g.
    /// `text_search_stats("cat"."sch"."tbl", 'query', "col")`) instead of the
    /// quoted `table_reference`. `None` keeps the plain-table behavior.
    #[must_use]
    pub fn with_from_function(mut self, from_function: Option<String>) -> Self {
        self.from_function = from_function;
        self
    }

    /// Returns the raw table-function FROM source, if this scan is over a UDTF.
    #[must_use]
    pub fn from_function(&self) -> Option<&str> {
        self.from_function.as_deref()
    }

    /// Returns the currently configured W3C `traceparent` value, if any.
    #[must_use]
    pub fn trace_parent(&self) -> Option<&str> {
        self.trace_parent.as_deref()
    }

    /// Returns a reference to the underlying `FlightSqlClient`.
    #[must_use]
    pub fn client(&self) -> &FlightSqlClient {
        &self.client
    }

    /// Returns a reference to the table reference.
    #[must_use]
    pub fn table_reference(&self) -> &TableReference {
        &self.table_reference
    }

    /// Returns a reference to the cookie store.
    #[must_use]
    pub fn cookie_store(&self) -> &Arc<CookieStore> {
        &self.cookie_store
    }

    /// Returns a reference to the projected schema.
    #[must_use]
    pub fn projected_schema(&self) -> &SchemaRef {
        &self.projected_schema
    }

    /// Returns the filter expressions.
    #[must_use]
    pub fn filters(&self) -> &[Expr] {
        &self.filters
    }

    /// Returns the limit.
    #[must_use]
    pub fn limit(&self) -> Option<usize> {
        self.limit
    }

    /// Returns the SQL query that this exec will send to the `FlightSQL` endpoint.
    pub fn sql(&self) -> Result<String> {
        let columns = self
            .projected_schema
            .fields()
            .iter()
            .map(|f| quote_identifier(f.name()))
            .collect::<Vec<_>>()
            .join(", ");

        let limit_expr = match self.limit {
            Some(limit) => format!("LIMIT {limit}"),
            None => String::new(),
        };

        let where_expr = if self.filters.is_empty() {
            String::new()
        } else {
            let filter_expr = self
                .filters
                .iter()
                .map(|f| {
                    to_sql_preserving_precedence(f).map(|sql| {
                        // Wrap each top-level filter in parentheses to preserve semantics when
                        // joining with AND, and recursively parenthesize nested binary
                        // expressions so mixed AND/OR trees inside a single filter keep the
                        // original DataFusion meaning.
                        format!("({sql})")
                    })
                })
                .collect::<DataFusionResult<Vec<_>>>()
                .context(UnableToGenerateSQLSnafu)?;
            format!("WHERE {}", filter_expr.join(" AND "))
        };
        let order_expr = if self.sort_exprs.is_empty() {
            String::new()
        } else {
            let sort_terms: Vec<String> =
                self.sort_exprs
                    .iter()
                    .map(|sort| {
                        let col = sort.expr.downcast_ref::<Column>().context(
                            InvalidSortExpressionSnafu {
                                expr: format!("{:?}", sort.expr),
                            },
                        )?;
                        let dir = if sort.options.descending {
                            "DESC"
                        } else {
                            "ASC"
                        };
                        let nulls = if sort.options.nulls_first {
                            "NULLS FIRST"
                        } else {
                            "NULLS LAST"
                        };
                        Ok(format!("{} {dir} {nulls}", quote_identifier(col.name())))
                    })
                    .collect::<Result<Vec<_>>>()?;
            format!("ORDER BY {}", sort_terms.join(", "))
        };

        // When the scan's source is a table-function call (distributed full-text
        // search), `from_function` already holds a valid quoted UDTF invocation
        // and renders verbatim; otherwise fall back to the quoted table reference.
        let from_clause = match &self.from_function {
            Some(from_function) => from_function.clone(),
            None => self.table_reference.to_quoted_string(),
        };
        let mut sql = format!("SELECT {columns} FROM {from_clause}");
        if !where_expr.is_empty() {
            sql.push(' ');
            sql.push_str(&where_expr);
        }
        if !order_expr.is_empty() {
            sql.push(' ');
            sql.push_str(&order_expr);
        }
        if !limit_expr.is_empty() {
            sql.push(' ');
            sql.push_str(&limit_expr);
        }

        Ok(sql)
    }
}

impl std::fmt::Debug for FlightSqlExec {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let sql = self.sql().unwrap_or_default();
        write!(f, "FlightSqlExec sql={sql}")
    }
}

impl DisplayAs for FlightSqlExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        let sql = self.sql().unwrap_or_default();
        write!(f, "FlightSqlExec sql={sql}")
    }
}

impl ExecutionPlan for FlightSqlExec {
    fn name(&self) -> &'static str {
        "FlightSqlExec"
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.projected_schema)
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn try_pushdown_sort(
        &self,
        order: &[PhysicalSortExpr],
    ) -> DataFusionResult<SortOrderPushdownResult<Arc<dyn ExecutionPlan>>> {
        for sort_expr in order {
            if sort_expr.expr.downcast_ref::<Column>().is_none() {
                return Ok(SortOrderPushdownResult::Unsupported);
            }
        }

        let sort_exprs = order.to_vec();
        let mut eq_properties = EquivalenceProperties::new(Arc::clone(&self.projected_schema));
        if let Some(ordering) = LexOrdering::new(sort_exprs.clone()) {
            eq_properties.add_orderings([ordering]);
        }

        let new_plan = FlightSqlExec {
            projected_schema: Arc::clone(&self.projected_schema),
            table_reference: self.table_reference.clone(),
            client: self.client.clone(),
            filters: self.filters.clone(),
            limit: self.limit,
            sort_exprs,
            properties: Arc::new(PlanProperties::new(
                eq_properties,
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            )),
            cookie_store: Arc::clone(&self.cookie_store),
            metrics: ExecutionPlanMetricsSet::new(),
            trace_parent: self.trace_parent.clone(),
            statistics: self.statistics.clone(),
            token: self.token.clone(),
            from_function: self.from_function.clone(),
        };

        Ok(SortOrderPushdownResult::Exact {
            inner: Arc::new(new_plan),
        })
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let sql = self.sql().map_err(to_execution_error)?;
        let target_schema = self.schema();

        let first_batch_time =
            MetricBuilder::new(&self.metrics).subset_time("first_batch_time", partition);
        let fetch_time = MetricBuilder::new(&self.metrics).subset_time("fetch_time", partition);

        let baseline = datafusion::common::instant::Instant::now();

        let mut client = self.client.clone();
        let trace_parent = self
            .trace_parent
            .clone()
            .or_else(|| trace_parent_from_task_context(&context));
        if let Some(value) = trace_parent {
            client.set_header("traceparent", value);
        }
        if let Some(token) = &self.token {
            client.set_token(token.clone());
        }

        let inner =
            query_to_stream(client, sql, Arc::clone(&self.cookie_store)).map(move |result| {
                result.and_then(|batch| coerce_batch_to_schema(&batch, &target_schema))
            });

        let timed_stream = stream! {
            futures::pin_mut!(inner);
            let mut stream_metrics = FlightSqlStreamMetrics::new(first_batch_time, fetch_time, baseline);

            while let Some(item) = inner.next().await {
                if item.is_ok() {
                    stream_metrics.record_first_batch();
                }
                yield item;
            }

            // For empty/error-only streams, record a fallback first-batch timing at completion.
            stream_metrics.record_first_batch_if_unset();
            stream_metrics.record_fetch();
        };

        let stream_adapter = RecordBatchStreamAdapter::new(self.schema(), timed_stream);

        Ok(Box::pin(stream_adapter))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn partition_statistics(&self, partition: Option<usize>) -> DataFusionResult<Arc<Statistics>> {
        // Single output partition (`UnknownPartitioning(1)`), so per-partition
        // statistics for partition 0 are the whole scan's statistics.
        match partition {
            None | Some(0) => Ok(Arc::new(self.statistics.clone())),
            Some(_) => Ok(Arc::new(Statistics::new_unknown(&self.projected_schema))),
        }
    }

    fn supports_limit_pushdown(&self) -> bool {
        true
    }

    fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        let merged_limit = match (self.limit, limit) {
            (Some(existing), Some(new_limit)) => Some(existing.min(new_limit)),
            (Some(existing), None) => Some(existing),
            (None, Some(new_limit)) => Some(new_limit),
            (None, None) => None,
        };

        // `LimitPushdown` calls this to move a `GlobalLimitExec` into the scan and
        // then drops it, so the limited scan becomes the only thing describing the
        // row count. `self.statistics` describes the *unlimited* slice, so it can
        // only stay `Exact` while there is no limit — see `create_physical_plan` for
        // the same reasoning about pushed-down filters (issues #11599, #12292).
        let statistics = if merged_limit.is_some() {
            self.statistics.clone().to_inexact()
        } else {
            self.statistics.clone()
        };

        let new_plan = FlightSqlExec {
            projected_schema: Arc::clone(&self.projected_schema),
            table_reference: self.table_reference.clone(),
            client: self.client.clone(),
            filters: self.filters.clone(),
            limit: merged_limit,
            sort_exprs: self.sort_exprs.clone(),
            properties: Arc::clone(&self.properties),
            cookie_store: Arc::clone(&self.cookie_store),
            metrics: ExecutionPlanMetricsSet::new(),
            trace_parent: self.trace_parent.clone(),
            statistics,
            token: self.token.clone(),
            from_function: self.from_function.clone(),
        };

        Some(Arc::new(new_plan))
    }

    fn fetch(&self) -> Option<usize> {
        self.limit
    }
}

struct FlightSqlStreamMetrics {
    first_batch_time: Time,
    fetch_time: Time,
    baseline: datafusion::common::instant::Instant,
    first_batch_recorded: bool,
    fetch_recorded: bool,
}

impl FlightSqlStreamMetrics {
    fn new(
        first_batch_time: Time,
        fetch_time: Time,
        baseline: datafusion::common::instant::Instant,
    ) -> Self {
        Self {
            first_batch_time,
            fetch_time,
            baseline,
            first_batch_recorded: false,
            fetch_recorded: false,
        }
    }

    fn record_first_batch(&mut self) {
        if !self.first_batch_recorded {
            self.first_batch_time.add_elapsed(self.baseline);
            self.first_batch_recorded = true;
        }
    }

    fn record_first_batch_if_unset(&mut self) {
        if !self.first_batch_recorded {
            self.record_first_batch();
        }
    }

    fn record_fetch(&mut self) {
        if !self.fetch_recorded {
            self.fetch_time.add_elapsed(self.baseline);
            self.fetch_recorded = true;
        }
    }
}

impl Drop for FlightSqlStreamMetrics {
    fn drop(&mut self) {
        self.record_first_batch_if_unset();
        self.record_fetch();
    }
}

/// Coerce a [`RecordBatch`] to match a target schema by casting columns whose types
/// differ but are compatible (e.g. `Utf8` → `Utf8View`). This handles cases where
/// the Flight IPC layer returns data with slightly different types than the declared schema.
fn coerce_batch_to_schema(
    batch: &RecordBatch,
    target_schema: &SchemaRef,
) -> DataFusionResult<RecordBatch> {
    if batch.num_columns() != target_schema.fields().len() {
        return Err(DataFusionError::Execution(format!(
            "FlightSQL batch column count mismatch: got {}, expected {}",
            batch.num_columns(),
            target_schema.fields().len()
        )));
    }

    // Fast path: schemas already match.
    if batch.schema().fields() == target_schema.fields() {
        return Ok(batch.clone());
    }

    let columns = batch
        .columns()
        .iter()
        .zip(target_schema.fields())
        .map(|(col, target_field)| {
            if col.data_type() == target_field.data_type() {
                Ok(Arc::clone(col))
            } else {
                cast(col, target_field.data_type())
                    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
            }
        })
        .collect::<DataFusionResult<Vec<_>>>()?;

    RecordBatch::try_new(Arc::clone(target_schema), columns)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

pub fn query_to_stream(
    mut client: FlightSqlClient,
    sql: String,
    cookie_store: Arc<CookieStore>,
) -> impl Stream<Item = DataFusionResult<RecordBatch>> {
    stream! {
        let flight_info = client
            .execute(sql, None)
            .await
            .map_err(to_execution_error)?;

        for ep in flight_info.endpoint {
            if let Some(tkt) = ep.clone().ticket {
                match get_client_for_flight_endpoint(
                    &client,
                    ep,
                    &cookie_store,
                )
                .await
                    .map_err(to_execution_error)?
                    .do_get(tkt.clone()).await {
                        Ok(mut flight_stream) => {
                            while let Some(batch) = flight_stream.next().await {
                                match batch {
                                    Ok(batch) => yield Ok(batch),
                                    Err(error) => yield Err(to_execution_error(Error::UnableToQueryArrowFlight { source: error }))
                                }
                            }
                        },
                        Err(error) => yield Err(to_execution_error(Error::UnableToQueryArrowFlight { source: error } ))
                }
            }
        };
    }
}

fn to_execution_error(e: impl Into<Box<dyn std::error::Error>>) -> DataFusionError {
    DataFusionError::Execution(format!("{}", e.into()))
}

pub async fn get_client_for_flight_endpoint(
    client: &FlightSqlClient,
    ep: FlightEndpoint,
    cookie_store: &Arc<CookieStore>,
) -> Result<FlightSqlClient, Box<dyn std::error::Error>> {
    if ep.location.is_empty() {
        Ok(client.clone())
    } else {
        // Some Flight SQL servers (e.g. StarRocks) advertise an internal/cluster-only address
        // in the endpoint location that's unreachable from external clients. Per the Flight SQL
        // spec, data served at the same address as the FlightInfo may carry an empty location;
        // StarRocks instead returns its internal FE address. If we can't reach the advertised
        // location, fall back to the original connection that served the FlightInfo.
        match new_tls_flight_channel(&ep.location[0].uri, None).await {
            Ok(channel) => {
                let channel = CookieService::new(channel, Arc::clone(cookie_store));
                let mut new_client = FlightSqlServiceClient::new(channel);
                // Propagate auth token to avoid "invalid authentication credentials" on DoGet.
                // Per-endpoint clients don't inherit the handshake session of the original client.
                if let Some(t) = client.token().cloned() {
                    new_client.set_token(t);
                }
                Ok(new_client)
            }
            Err(_) => Ok(client.clone()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{FlightSqlClient, query_to_stream};
    use crate::flightsql::FlightSqlExec;
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
    use arrow_flight::{
        Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightEndpoint,
        FlightInfo, Location, PollInfo, PutResult, SchemaResult, Ticket,
    };
    use bytes::Bytes;
    use datafusion::{
        execution::TaskContext, physical_expr::PhysicalSortExpr, physical_plan::ExecutionPlan,
        sql::TableReference,
    };
    use flight_client::cookie::{CookieService, CookieStore};
    use futures::{StreamExt, TryStreamExt};
    use std::net::SocketAddr;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    use tokio::net::TcpListener;
    use tokio::sync::oneshot;
    use tokio::task::JoinHandle;
    use tokio_stream::Empty as EmptyStream;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::transport::Channel;
    use tonic::{Request, Response, Status, async_trait};

    const COOKIE_VALUE: &str = "AWSALB=abc123";

    #[derive(Clone, Copy)]
    enum DoGetMode {
        Empty,
        Error,
    }

    struct TestServer {
        addr: SocketAddr,
        shutdown: Option<oneshot::Sender<()>>,
        handle: JoinHandle<Result<(), tonic::transport::Error>>,
    }

    impl TestServer {
        async fn start(cookie_seen: Arc<AtomicBool>, do_get_mode: DoGetMode) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0")
                .await
                .expect("listener should bind");
            let addr = listener.local_addr().expect("listener should have addr");
            let location = format!("http://{addr}");
            let service = CookieFlightSqlService::new(cookie_seen, location, do_get_mode);
            let (shutdown_tx, shutdown_rx) = oneshot::channel();
            let handle = tokio::spawn(async move {
                tonic::transport::Server::builder()
                    .add_service(FlightServiceServer::new(service))
                    .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                        let _ = shutdown_rx.await;
                    })
                    .await
            });
            Self {
                addr,
                shutdown: Some(shutdown_tx),
                handle,
            }
        }

        async fn shutdown(mut self) {
            if let Some(shutdown) = self.shutdown.take() {
                let _ = shutdown.send(());
            }
            self.handle
                .await
                .expect("server task should finish")
                .expect("server should exit cleanly");
        }
    }

    #[derive(Clone)]
    struct CookieFlightSqlService {
        cookie_required: Arc<AtomicBool>,
        cookie_seen: Arc<AtomicBool>,
        location: String,
        do_get_mode: DoGetMode,
    }

    impl CookieFlightSqlService {
        fn new(cookie_seen: Arc<AtomicBool>, location: String, do_get_mode: DoGetMode) -> Self {
            Self {
                cookie_required: Arc::new(AtomicBool::new(false)),
                cookie_seen,
                location,
                do_get_mode,
            }
        }
    }

    type EmptyResponseStream<T> = EmptyStream<Result<T, Status>>;

    #[async_trait]
    impl FlightService for CookieFlightSqlService {
        type HandshakeStream = EmptyResponseStream<arrow_flight::HandshakeResponse>;
        type ListFlightsStream = EmptyResponseStream<FlightInfo>;
        type DoGetStream = EmptyResponseStream<FlightData>;
        type DoPutStream = EmptyResponseStream<PutResult>;
        type DoExchangeStream = EmptyResponseStream<FlightData>;
        type DoActionStream = EmptyResponseStream<arrow_flight::Result>;
        type ListActionsStream = EmptyResponseStream<ActionType>;

        async fn handshake(
            &self,
            _request: Request<tonic::Streaming<arrow_flight::HandshakeRequest>>,
        ) -> Result<Response<Self::HandshakeStream>, Status> {
            Err(Status::unimplemented("handshake"))
        }

        async fn list_flights(
            &self,
            _request: Request<Criteria>,
        ) -> Result<Response<Self::ListFlightsStream>, Status> {
            Err(Status::unimplemented("list_flights"))
        }

        async fn get_flight_info(
            &self,
            _request: Request<FlightDescriptor>,
        ) -> Result<Response<FlightInfo>, Status> {
            self.cookie_required.store(true, Ordering::SeqCst);
            let endpoint = FlightEndpoint {
                ticket: Some(Ticket {
                    ticket: Bytes::from_static(b"ticket"),
                }),
                location: vec![Location {
                    uri: self.location.clone(),
                }],
                expiration_time: None,
                app_metadata: Bytes::new(),
            };

            let mut response = Response::new(FlightInfo {
                schema: Bytes::new(),
                flight_descriptor: None,
                endpoint: vec![endpoint],
                total_records: -1,
                total_bytes: -1,
                ordered: false,
                app_metadata: Bytes::new(),
            });
            response.metadata_mut().insert(
                "set-cookie",
                format!("{COOKIE_VALUE}; Path=/")
                    .parse()
                    .expect("cookie header should be valid"),
            );
            Ok(response)
        }

        async fn poll_flight_info(
            &self,
            _request: Request<FlightDescriptor>,
        ) -> Result<Response<PollInfo>, Status> {
            Err(Status::unimplemented("poll_flight_info"))
        }

        async fn get_schema(
            &self,
            _request: Request<FlightDescriptor>,
        ) -> Result<Response<SchemaResult>, Status> {
            Err(Status::unimplemented("get_schema"))
        }

        async fn do_get(
            &self,
            request: Request<Ticket>,
        ) -> Result<Response<Self::DoGetStream>, Status> {
            if self.cookie_required.load(Ordering::SeqCst) {
                let cookie_header = request
                    .metadata()
                    .get("cookie")
                    .and_then(|value| value.to_str().ok())
                    .ok_or_else(|| Status::unauthenticated("cookie missing"))?;
                if !cookie_header.contains(COOKIE_VALUE) {
                    return Err(Status::unauthenticated("cookie missing"));
                }
            }
            self.cookie_seen.store(true, Ordering::SeqCst);
            match self.do_get_mode {
                DoGetMode::Empty => Ok(Response::new(tokio_stream::empty())),
                DoGetMode::Error => Err(Status::internal("do_get failed")),
            }
        }

        async fn do_put(
            &self,
            _request: Request<tonic::Streaming<FlightData>>,
        ) -> Result<Response<Self::DoPutStream>, Status> {
            Err(Status::unimplemented("do_put"))
        }

        async fn do_exchange(
            &self,
            _request: Request<tonic::Streaming<FlightData>>,
        ) -> Result<Response<Self::DoExchangeStream>, Status> {
            Err(Status::unimplemented("do_exchange"))
        }

        async fn do_action(
            &self,
            _request: Request<Action>,
        ) -> Result<Response<Self::DoActionStream>, Status> {
            Err(Status::unimplemented("do_action"))
        }

        async fn list_actions(
            &self,
            _request: Request<Empty>,
        ) -> Result<Response<Self::ListActionsStream>, Status> {
            Err(Status::unimplemented("list_actions"))
        }
    }

    #[tokio::test]
    async fn query_to_stream_sends_cookie_to_endpoint_client() {
        let cookie_seen = Arc::new(AtomicBool::new(false));
        let server = TestServer::start(Arc::clone(&cookie_seen), DoGetMode::Empty).await;
        let cookie_store = Arc::new(CookieStore::new());
        let channel = Channel::from_shared(format!("http://{}", server.addr))
            .expect("channel should parse")
            .connect()
            .await
            .expect("channel should connect");
        let channel = CookieService::new(channel, Arc::clone(&cookie_store));
        let client: FlightSqlClient =
            arrow_flight::sql::client::FlightSqlServiceClient::new(channel);

        let batches = query_to_stream(client, "SELECT 1".to_string(), Arc::clone(&cookie_store))
            .try_collect::<Vec<_>>()
            .await
            .expect("query should succeed");
        assert!(batches.is_empty());
        assert!(cookie_seen.load(Ordering::SeqCst));

        server.shutdown().await;
    }

    fn has_metric(metrics: &datafusion::physical_plan::metrics::MetricsSet, name: &str) -> bool {
        metrics.sum_by_name(name).is_some()
    }

    /// A `FlightSqlClient` connected lazily to a non-routable address: enough to
    /// build a `FlightSQLTable`/scan plan without ever attempting a connection.
    fn lazy_client() -> FlightSqlClient {
        use arrow_flight::flight_service_client::FlightServiceClient;
        use arrow_flight::sql::client::FlightSqlServiceClient;
        use tonic::transport::Endpoint;

        let channel = Endpoint::from_static("http://127.0.0.1:1").connect_lazy();
        let cookie_channel = CookieService::new(channel, Arc::new(CookieStore::new()));
        FlightSqlServiceClient::new_from_inner(FlightServiceClient::new(cookie_channel))
    }

    /// Regression test for #11599: a cluster leaf scan carries the executor's
    /// *unfiltered* row-count/bounds as stamped statistics. Because pushed-down
    /// filters are reported `Exact` (dropping the `FilterExec`), leaving those
    /// statistics `Exact` lets `aggregate_statistics` fold a filtered `COUNT(*)`
    /// to the unfiltered total. The scan must therefore mark its statistics
    /// inexact whenever a filter is pushed to it, while leaving them exact for an
    /// unfiltered scan.
    #[tokio::test]
    async fn scan_marks_stamped_statistics_inexact_when_filter_pushed() {
        use datafusion::catalog::TableProvider;
        use datafusion::common::stats::Precision;
        use datafusion::prelude::{SessionContext, col, lit};

        let table = stamped_table();
        let session = SessionContext::new().state();

        // No filter → stamped statistics stay exact (folding a bare COUNT(*) is correct).
        let plan = table
            .scan(&session, None, &[], None)
            .await
            .expect("unfiltered scan should build");
        let unfiltered = plan
            .partition_statistics(None)
            .expect("statistics should be available");
        assert_eq!(
            unfiltered.num_rows,
            Precision::Exact(150_000),
            "unfiltered scan must keep exact num_rows"
        );

        // With a pushed-down filter → statistics must be inexact so the
        // aggregate-statistics rule cannot fold the (now filtered) COUNT(*).
        let filter = col("v").eq(lit(0_i64));
        let plan = table
            .scan(&session, None, std::slice::from_ref(&filter), None)
            .await
            .expect("filtered scan should build");
        let filtered = plan
            .partition_statistics(None)
            .expect("statistics should be available");
        assert_eq!(
            filtered.num_rows,
            Precision::Inexact(150_000),
            "filtered scan must degrade num_rows to inexact (issue #11599)"
        );
        assert!(
            matches!(
                filtered.column_statistics[0].max_value,
                Precision::Inexact(_)
            ),
            "filtered scan must degrade column bounds to inexact so MIN/MAX are not folded"
        );
    }

    /// Regression test for #12292, the limit twin of #11599: `LIMIT` is emitted
    /// into the remote SQL, so a scan built with one cannot produce the stamped
    /// slice's row count or column bounds and must not report them `Exact`.
    #[tokio::test]
    async fn scan_marks_stamped_statistics_inexact_when_limit_pushed() {
        use datafusion::catalog::TableProvider;
        use datafusion::common::stats::Precision;
        use datafusion::prelude::SessionContext;

        let table = stamped_table();
        let session = SessionContext::new().state();

        let plan = table
            .scan(&session, None, &[], Some(10))
            .await
            .expect("limited scan should build");
        let limited = plan
            .partition_statistics(None)
            .expect("statistics should be available");
        assert_eq!(
            limited.num_rows,
            Precision::Inexact(150_000),
            "a scan whose SQL carries LIMIT 10 must not claim an exact 150000-row count"
        );
        assert!(
            matches!(
                limited.column_statistics[0].max_value,
                Precision::Inexact(_)
            ),
            "the limited scan's rows are a subset, so its bounds must be inexact too"
        );
    }

    /// Regression test for #12292: `LimitPushdown` introduces the limit through
    /// `with_fetch` and then drops the `GlobalLimitExec`, so the statistics the
    /// new plan carries are the only ones left describing the row count.
    #[tokio::test]
    async fn with_fetch_marks_stamped_statistics_inexact() {
        use datafusion::common::stats::Precision;

        let exec = build_exec(lazy_client(), Arc::new(CookieStore::new()))
            .with_statistics(stamped_statistics());

        // No limit anywhere: the scan returns the whole stamped slice.
        assert_eq!(
            exec.partition_statistics(None)
                .expect("statistics should be available")
                .num_rows,
            Precision::Exact(150_000),
            "an unlimited scan must keep exact num_rows"
        );

        let limited = exec
            .with_fetch(Some(10))
            .expect("with_fetch should produce a plan");
        let stats = limited
            .partition_statistics(None)
            .expect("statistics should be available");
        assert_eq!(
            stats.num_rows,
            Precision::Inexact(150_000),
            "a limit pushed in through with_fetch must degrade num_rows to inexact"
        );
        assert!(
            matches!(stats.column_statistics[0].max_value, Precision::Inexact(_)),
            "a limit pushed in through with_fetch must degrade column bounds too"
        );

        // `with_fetch(None)` on an unlimited scan leaves the limit unset, so the
        // statistics stay exact.
        let unchanged = exec
            .with_fetch(None)
            .expect("with_fetch should produce a plan");
        assert_eq!(
            unchanged
                .partition_statistics(None)
                .expect("statistics should be available")
                .num_rows,
            Precision::Exact(150_000),
            "with_fetch(None) on an unlimited scan must not degrade statistics"
        );
    }

    /// Schema the stamped-statistics fixtures describe.
    fn stamped_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]))
    }

    /// A cluster leaf table carrying the statistics one executor reported for its
    /// whole slice — the shape `get_partitions_from_store` stamps onto each leaf.
    fn stamped_table() -> super::FlightSQLTable {
        super::FlightSQLTable::create_with_schema(
            "flightsql",
            "executor-1",
            lazy_client(),
            TableReference::bare("customer"),
            stamped_schema(),
            Arc::new(CookieStore::new()),
        )
        .with_statistics(Some(stamped_statistics()))
    }

    /// Statistics as an executor reports them for its whole table slice: an exact
    /// row count and exact column bounds over every row it holds.
    fn stamped_statistics() -> datafusion::common::Statistics {
        use datafusion::common::stats::Precision;
        use datafusion::common::{ColumnStatistics, ScalarValue, Statistics};

        Statistics {
            num_rows: Precision::Exact(150_000),
            total_byte_size: Precision::Exact(1_200_000),
            column_statistics: vec![ColumnStatistics {
                null_count: Precision::Exact(0),
                max_value: Precision::Exact(ScalarValue::Int64(Some(149_999))),
                min_value: Precision::Exact(ScalarValue::Int64(Some(0))),
                sum_value: Precision::Absent,
                distinct_count: Precision::Absent,
                byte_size: Precision::Exact(1_200_000),
            }],
        }
    }

    fn build_exec(client: FlightSqlClient, cookie_store: Arc<CookieStore>) -> FlightSqlExec {
        let schema = stamped_schema();
        FlightSqlExec::new(
            None,
            &schema,
            &TableReference::bare("metrics_table"),
            client,
            &[],
            None,
            cookie_store,
        )
        .expect("exec should build")
    }

    fn build_exec_multi_col(
        client: FlightSqlClient,
        cookie_store: Arc<CookieStore>,
    ) -> FlightSqlExec {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Utf8, true),
        ]));
        FlightSqlExec::new(
            None,
            &schema,
            &TableReference::bare("test_table"),
            client,
            &[],
            None,
            cookie_store,
        )
        .expect("exec should build")
    }

    #[tokio::test]
    async fn flight_sql_exec_metrics_recorded_for_empty_stream() {
        let cookie_seen = Arc::new(AtomicBool::new(false));
        let server = TestServer::start(Arc::clone(&cookie_seen), DoGetMode::Empty).await;
        let cookie_store = Arc::new(CookieStore::new());
        let channel = Channel::from_shared(format!("http://{}", server.addr))
            .expect("channel should parse")
            .connect()
            .await
            .expect("channel should connect");
        let channel = CookieService::new(channel, Arc::clone(&cookie_store));
        let client: FlightSqlClient =
            arrow_flight::sql::client::FlightSqlServiceClient::new(channel);

        let exec = build_exec(client, Arc::clone(&cookie_store));
        let stream = exec
            .execute(0, Arc::new(TaskContext::default()))
            .expect("execute should succeed");
        let output = stream
            .try_collect::<Vec<_>>()
            .await
            .expect("stream should succeed");
        assert!(output.is_empty());

        let metrics = exec.metrics().expect("metrics should exist");
        assert!(has_metric(&metrics, "first_batch_time"));
        assert!(has_metric(&metrics, "fetch_time"));

        server.shutdown().await;
    }

    #[tokio::test]
    async fn flight_sql_exec_metrics_handle_error_item_and_early_drop() {
        let cookie_seen = Arc::new(AtomicBool::new(false));
        let server = TestServer::start(Arc::clone(&cookie_seen), DoGetMode::Error).await;
        let cookie_store = Arc::new(CookieStore::new());
        let channel = Channel::from_shared(format!("http://{}", server.addr))
            .expect("channel should parse")
            .connect()
            .await
            .expect("channel should connect");
        let channel = CookieService::new(channel, Arc::clone(&cookie_store));
        let client: FlightSqlClient =
            arrow_flight::sql::client::FlightSqlServiceClient::new(channel);

        let exec = build_exec(client, Arc::clone(&cookie_store));
        let mut stream = exec
            .execute(0, Arc::new(TaskContext::default()))
            .expect("execute should succeed");

        let first = stream.next().await;
        assert!(first.is_some());
        first
            .expect("item should exist")
            .expect_err("item should be an error");

        drop(stream);

        let metrics = exec.metrics().expect("metrics should exist");
        assert!(has_metric(&metrics, "first_batch_time"));
        assert!(has_metric(&metrics, "fetch_time"));

        server.shutdown().await;
    }

    #[tokio::test]
    async fn try_pushdown_sort_returns_unsupported_for_non_column_expr() {
        use arrow::compute::SortOptions;
        use datafusion::common::ScalarValue;
        use datafusion::physical_expr::expressions::Literal;
        use datafusion::physical_plan::SortOrderPushdownResult;

        let cookie_seen = Arc::new(AtomicBool::new(false));
        let server = TestServer::start(Arc::clone(&cookie_seen), DoGetMode::Empty).await;
        let cookie_store = Arc::new(CookieStore::new());
        let channel = Channel::from_shared(format!("http://{}", server.addr))
            .expect("channel should parse")
            .connect()
            .await
            .expect("channel should connect");
        let channel = CookieService::new(channel, Arc::clone(&cookie_store));
        let client: FlightSqlClient =
            arrow_flight::sql::client::FlightSqlServiceClient::new(channel);

        let exec = build_exec(client, Arc::clone(&cookie_store));
        let sort_expr = PhysicalSortExpr {
            expr: Arc::new(Literal::new(ScalarValue::Int64(Some(1)))),
            options: SortOptions::default(),
        };
        let result = exec
            .try_pushdown_sort(&[sort_expr])
            .expect("try_pushdown_sort should not error");
        assert!(
            matches!(result, SortOrderPushdownResult::Unsupported),
            "expected Unsupported for non-column sort expression"
        );

        server.shutdown().await;
    }

    #[tokio::test]
    async fn try_pushdown_sort_returns_exact_for_column_expr() {
        use arrow::compute::SortOptions;
        use datafusion::physical_expr::expressions::Column;
        use datafusion::physical_plan::SortOrderPushdownResult;

        let cookie_seen = Arc::new(AtomicBool::new(false));
        let server = TestServer::start(Arc::clone(&cookie_seen), DoGetMode::Empty).await;
        let cookie_store = Arc::new(CookieStore::new());
        let channel = Channel::from_shared(format!("http://{}", server.addr))
            .expect("channel should parse")
            .connect()
            .await
            .expect("channel should connect");
        let channel = CookieService::new(channel, Arc::clone(&cookie_store));
        let client: FlightSqlClient =
            arrow_flight::sql::client::FlightSqlServiceClient::new(channel);

        let exec = build_exec_multi_col(client, Arc::clone(&cookie_store));
        let sort_expr = PhysicalSortExpr {
            expr: Arc::new(Column::new("a", 0)),
            options: SortOptions::default(),
        };
        let result = exec
            .try_pushdown_sort(&[sort_expr])
            .expect("try_pushdown_sort should not error");
        assert!(
            matches!(result, SortOrderPushdownResult::Exact { .. }),
            "expected Exact for column sort expression"
        );

        server.shutdown().await;
    }

    #[tokio::test]
    async fn sql_includes_order_by_after_sort_pushdown() {
        use arrow::compute::SortOptions;
        use datafusion::physical_expr::expressions::Column;
        use datafusion::physical_plan::SortOrderPushdownResult;

        let cookie_seen = Arc::new(AtomicBool::new(false));
        let server = TestServer::start(Arc::clone(&cookie_seen), DoGetMode::Empty).await;
        let cookie_store = Arc::new(CookieStore::new());
        let channel = Channel::from_shared(format!("http://{}", server.addr))
            .expect("channel should parse")
            .connect()
            .await
            .expect("channel should connect");
        let channel = CookieService::new(channel, Arc::clone(&cookie_store));
        let client: FlightSqlClient =
            arrow_flight::sql::client::FlightSqlServiceClient::new(channel);

        let exec = build_exec_multi_col(client, Arc::clone(&cookie_store));
        let sort_exprs = vec![
            PhysicalSortExpr {
                expr: Arc::new(Column::new("a", 0)),
                options: SortOptions {
                    descending: false,
                    nulls_first: true,
                },
            },
            PhysicalSortExpr {
                expr: Arc::new(Column::new("b", 1)),
                options: SortOptions {
                    descending: true,
                    nulls_first: false,
                },
            },
        ];
        let result = exec
            .try_pushdown_sort(&sort_exprs)
            .expect("try_pushdown_sort should not error");
        let SortOrderPushdownResult::Exact { inner } = result else {
            panic!("expected Exact result from try_pushdown_sort");
        };
        let pushed_exec = inner
            .downcast_ref::<FlightSqlExec>()
            .expect("inner should be FlightSqlExec");
        let sql = pushed_exec.sql().expect("sql should succeed");
        assert!(
            sql.contains("ORDER BY a ASC NULLS FIRST, b DESC NULLS LAST"),
            "expected ORDER BY clause in SQL, got: {sql}"
        );

        server.shutdown().await;
    }

    #[tokio::test]
    async fn query_to_stream_propagates_token_to_endpoint_client() {
        // Verify that a bearer token set on the main client is propagated to the
        // per-endpoint client created when following FlightEndpoint.location.
        // Servers like StarRocks enforce authentication on every connection.
        const TOKEN_VALUE: &str = "test-bearer-token";
        let cookie_seen = Arc::new(AtomicBool::new(false));
        let token_seen = Arc::new(AtomicBool::new(false));

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let addr = listener.local_addr().expect("listener should have addr");
        let location = format!("http://{addr}");
        let service = {
            let token_seen = Arc::clone(&token_seen);
            TokenFlightSqlService {
                cookie_seen: Arc::clone(&cookie_seen),
                token_seen,
                location: location.clone(),
                expected_token: TOKEN_VALUE.to_string(),
            }
        };
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let handle = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(FlightServiceServer::new(service))
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                    let _ = shutdown_rx.await;
                })
                .await
        });

        let cookie_store = Arc::new(CookieStore::new());
        let channel = Channel::from_shared(format!("http://{addr}"))
            .expect("channel should parse")
            .connect()
            .await
            .expect("channel should connect");
        let channel = CookieService::new(channel, Arc::clone(&cookie_store));
        let mut client: FlightSqlClient =
            arrow_flight::sql::client::FlightSqlServiceClient::new(channel);
        client.set_token(TOKEN_VALUE.to_string());

        let _batches = query_to_stream(client, "SELECT 1".to_string(), Arc::clone(&cookie_store))
            .try_collect::<Vec<_>>()
            .await
            .expect("query should succeed");

        assert!(
            token_seen.load(Ordering::SeqCst),
            "bearer token should have been forwarded to the per-endpoint DoGet client"
        );

        let _ = shutdown_tx.send(());
        handle
            .await
            .expect("server should finish")
            .expect("server should exit cleanly");
    }

    // -----------------------------------------------------------------------
    // Test server that enforces bearer-token auth on DoGet.
    // Used by `query_to_stream_propagates_token_to_endpoint_client`.
    // -----------------------------------------------------------------------
    struct TokenFlightSqlService {
        cookie_seen: Arc<AtomicBool>,
        token_seen: Arc<AtomicBool>,
        location: String,
        expected_token: String,
    }
    #[async_trait]
    impl FlightService for TokenFlightSqlService {
        type HandshakeStream = EmptyResponseStream<arrow_flight::HandshakeResponse>;
        type ListFlightsStream = EmptyResponseStream<FlightInfo>;
        type DoGetStream = EmptyResponseStream<FlightData>;
        type DoPutStream = EmptyResponseStream<PutResult>;
        type DoExchangeStream = EmptyResponseStream<FlightData>;
        type DoActionStream = EmptyResponseStream<arrow_flight::Result>;
        type ListActionsStream = EmptyResponseStream<ActionType>;

        async fn handshake(
            &self,
            _request: Request<tonic::Streaming<arrow_flight::HandshakeRequest>>,
        ) -> Result<Response<Self::HandshakeStream>, Status> {
            Err(Status::unimplemented("handshake"))
        }

        async fn list_flights(
            &self,
            _request: Request<Criteria>,
        ) -> Result<Response<Self::ListFlightsStream>, Status> {
            Err(Status::unimplemented("list_flights"))
        }

        async fn get_flight_info(
            &self,
            _request: Request<FlightDescriptor>,
        ) -> Result<Response<FlightInfo>, Status> {
            // Return an endpoint pointing back at this server so the client
            // must create a per-endpoint FlightSqlServiceClient and call DoGet on it.
            let endpoint = FlightEndpoint {
                ticket: Some(Ticket {
                    ticket: Bytes::from_static(b"tok-ticket"),
                }),
                location: vec![Location {
                    uri: self.location.clone(),
                }],
                expiration_time: None,
                app_metadata: Bytes::new(),
            };
            Ok(Response::new(FlightInfo {
                schema: Bytes::new(),
                flight_descriptor: None,
                endpoint: vec![endpoint],
                total_records: -1,
                total_bytes: -1,
                ordered: false,
                app_metadata: Bytes::new(),
            }))
        }

        async fn poll_flight_info(
            &self,
            _request: Request<FlightDescriptor>,
        ) -> Result<Response<PollInfo>, Status> {
            Err(Status::unimplemented("poll_flight_info"))
        }

        async fn get_schema(
            &self,
            _request: Request<FlightDescriptor>,
        ) -> Result<Response<SchemaResult>, Status> {
            Err(Status::unimplemented("get_schema"))
        }

        async fn do_get(
            &self,
            request: Request<Ticket>,
        ) -> Result<Response<Self::DoGetStream>, Status> {
            // Enforce bearer-token auth: reject if the token is missing or wrong.
            let auth = request
                .metadata()
                .get("authorization")
                .and_then(|v| v.to_str().ok())
                .ok_or_else(|| Status::unauthenticated("missing authorization header"))?;
            if auth != format!("Bearer {}", self.expected_token) {
                return Err(Status::unauthenticated("invalid bearer token"));
            }
            self.cookie_seen.store(true, Ordering::SeqCst);
            self.token_seen.store(true, Ordering::SeqCst);
            Ok(Response::new(tokio_stream::empty()))
        }

        async fn do_put(
            &self,
            _request: Request<tonic::Streaming<FlightData>>,
        ) -> Result<Response<Self::DoPutStream>, Status> {
            Err(Status::unimplemented("do_put"))
        }

        async fn do_exchange(
            &self,
            _request: Request<tonic::Streaming<FlightData>>,
        ) -> Result<Response<Self::DoExchangeStream>, Status> {
            Err(Status::unimplemented("do_exchange"))
        }

        async fn do_action(
            &self,
            _request: Request<Action>,
        ) -> Result<Response<Self::DoActionStream>, Status> {
            Err(Status::unimplemented("do_action"))
        }

        async fn list_actions(
            &self,
            _request: Request<Empty>,
        ) -> Result<Response<Self::ListActionsStream>, Status> {
            Err(Status::unimplemented("list_actions"))
        }
    }
}
