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

use crate::sql_expr::to_sql_preserving_precedence;
use arrow::{
    array::{Array, RecordBatch, array},
    compute::cast,
    datatypes::Schema,
};
use async_stream::stream;
use async_trait::async_trait;
use datafusion_table_providers::sql::sql_provider_datafusion::expr;
use flight_client::{
    MAX_DECODING_MESSAGE_SIZE, MAX_ENCODING_MESSAGE_SIZE,
    cookie::{CookieService, CookieStore},
    tls::new_tls_flight_channel,
};
use futures::{Stream, StreamExt, TryStreamExt};
use snafu::prelude::*;
use std::{any::Any, fmt, sync::Arc, vec};

use arrow_flight::{
    FlightEndpoint, IpcMessage,
    error::FlightError,
    flight_service_client::FlightServiceClient,
    sql::{CommandGetTables, client::FlightSqlServiceClient},
};
use datafusion::{
    arrow::datatypes::SchemaRef,
    catalog::Session,
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
fn trace_parent_from_task_context(context: &TaskContext) -> Option<String> {
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
    UnableToGenerateSQL { source: expr::Error },

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

#[derive(Debug, Clone)]
pub struct FlightSQLFactory {
    client: FlightSqlClient,
    endpoint: String,
    cookie_store: Arc<CookieStore>,
}

impl FlightSQLFactory {
    #[must_use]
    pub fn new(client: FlightSqlClient, endpoint: String, cookie_store: Arc<CookieStore>) -> Self {
        Self {
            client,
            endpoint,
            cookie_store,
        }
    }
}

#[async_trait]
impl Read for FlightSQLFactory {
    async fn table_provider(
        &self,
        table_reference: TableReference,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn std::error::Error + Send + Sync>> {
        let table_provider = Arc::new(
            FlightSQLTable::create(
                "flightsql",
                &self.endpoint,
                self.client.clone(),
                table_reference,
                Arc::clone(&self.cookie_store),
            )
            .await?,
        );

        let table_provider = Arc::new(table_provider.create_federated_table_provider());

        Ok(table_provider)
    }
}

#[derive(Debug)]
pub struct FlightSQLTable {
    name: &'static str,
    join_push_down_context: String,
    client: FlightSqlClient,
    table_reference: TableReference,
    schema: SchemaRef,
    cookie_store: Arc<CookieStore>,
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
        }
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
        let flight_info = client
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
            .context(UnableToRetrieveSchemaArrowSnafu {
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
                    .context(UnableToRetrieveSchemaArrowSnafu {
                        table_name: table_reference.to_string(),
                    })?;
            let batch = stream.try_collect::<Vec<_>>().await.context(
                UnableToRetrieveSchemaFlightSnafu {
                    table_name: table_reference.to_string(),
                },
            )?;

            // Schema: https://github.com/apache/arrow/blob/44edc27e549d82db930421b0d4c76098941afd71/format/FlightSql.proto#L1182-L1190
            if let Some(schema) = Self::get_table_schema_if_present(batch, table_reference.clone())
            {
                return Ok(schema);
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
        Ok(Arc::new(FlightSqlExec::new(
            projections,
            schema,
            &self.table_reference,
            self.client.clone(),
            filters,
            limit,
            Arc::clone(&self.cookie_store),
        )?))
    }
}

#[async_trait]
impl TableProvider for FlightSQLTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

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
    properties: PlanProperties,
    cookie_store: Arc<CookieStore>,
    metrics: ExecutionPlanMetricsSet,
    /// Optional W3C `traceparent` value (e.g. `00-{trace_id}-{span_id}-01`)
    /// to attach as a gRPC metadata header on outgoing `execute()` and
    /// `do_get()` calls. When `None`, `execute()` falls back to reading
    /// the typed `Arc<RequestContext>` extension from the `TaskContext`
    /// session config and constructs a header from its `trace_parent()`.
    trace_parent: Option<String>,
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
        Ok(Self {
            projected_schema: Arc::clone(&projected_schema),
            table_reference: table_reference.clone(),
            client,
            filters: filters.to_vec(),
            limit,
            sort_exprs: Vec::new(),
            properties: PlanProperties::new(
                EquivalenceProperties::new(projected_schema),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            ),
            cookie_store,
            metrics: ExecutionPlanMetricsSet::new(),
            trace_parent: None,
        })
    }

    /// Set an explicit W3C `traceparent` header value to forward on each
    /// outgoing FlightSQL call. Useful when the plan-creation path has
    /// access to an `Arc<RequestContext>` but the executor-side
    /// `TaskContext` will not (e.g. when this `ExecutionPlan` is shipped
    /// to a remote executor via Ballista codecs).
    #[must_use]
    pub fn with_trace_parent(mut self, trace_parent: Option<String>) -> Self {
        self.trace_parent = trace_parent;
        self
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
                .collect::<expr::Result<Vec<_>>>()
                .context(UnableToGenerateSQLSnafu)?;
            format!("WHERE {}", filter_expr.join(" AND "))
        };
        let order_expr = if self.sort_exprs.is_empty() {
            String::new()
        } else {
            let sort_terms: Vec<String> = self
                .sort_exprs
                .iter()
                .map(|sort| {
                    let col = sort.expr.as_any().downcast_ref::<Column>().context(
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

        let mut sql = format!(
            "SELECT {columns} FROM {table_reference}",
            table_reference = self.table_reference.to_quoted_string(),
        );
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

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.projected_schema)
    }

    fn properties(&self) -> &PlanProperties {
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
            if sort_expr.expr.as_any().downcast_ref::<Column>().is_none() {
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
            properties: PlanProperties::new(
                eq_properties,
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            ),
            cookie_store: Arc::clone(&self.cookie_store),
            metrics: ExecutionPlanMetricsSet::new(),
            trace_parent: self.trace_parent.clone(),
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

        let inner = query_to_stream(client, sql, Arc::clone(&self.cookie_store)).map(
            move |result| result.and_then(|batch| coerce_batch_to_schema(&batch, &target_schema)),
        );

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

        let new_plan = FlightSqlExec {
            projected_schema: Arc::clone(&self.projected_schema),
            table_reference: self.table_reference.clone(),
            client: self.client.clone(),
            filters: self.filters.clone(),
            limit: merged_limit,
            sort_exprs: self.sort_exprs.clone(),
            properties: self.properties.clone(),
            cookie_store: Arc::clone(&self.cookie_store),
            metrics: ExecutionPlanMetricsSet::new(),
            trace_parent: self.trace_parent.clone(),
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
                match get_client_for_flight_endpoint(&client, ep, &cookie_store).await
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
                        Err(error) => yield Err(to_execution_error(Error::UnableToQueryArrowFlight { source: error.into()} ))
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
        let channel = new_tls_flight_channel(&ep.location[0].uri, None).await?;
        let channel = CookieService::new(channel, Arc::clone(cookie_store));
        Ok(FlightSqlServiceClient::new(channel))
    }
}

#[cfg(test)]
mod tests {
    use super::{FlightSqlClient, query_to_stream};
    use crate::flightsql::FlightSqlExec;
    use arrow::datatypes::{DataType, Field, Schema};
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

    fn build_exec(client: FlightSqlClient, cookie_store: Arc<CookieStore>) -> FlightSqlExec {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]));
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
            .as_any()
            .downcast_ref::<FlightSqlExec>()
            .expect("inner should be FlightSqlExec");
        let sql = pushed_exec.sql().expect("sql should succeed");
        assert!(
            sql.contains("ORDER BY a ASC NULLS FIRST, b DESC NULLS LAST"),
            "expected ORDER BY clause in SQL, got: {sql}"
        );

        server.shutdown().await;
    }
}
