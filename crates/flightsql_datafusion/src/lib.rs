#![allow(clippy::missing_errors_doc)]

use arrow::{
    array::{array, Array, RecordBatch},
    datatypes::Schema,
};
use async_stream::stream;
use async_trait::async_trait;
use futures::{Stream, StreamExt, TryFutureExt, TryStreamExt};
use snafu::prelude::*;
use sql_provider_datafusion::expr;
use std::{any::Any, fmt, pin::Pin, sync::Arc, task::Poll, vec};

use arrow_flight::{
    decode::FlightRecordBatchStream,
    error::FlightError,
    sql::{client::FlightSqlServiceClient, CommandGetTables},
    IpcMessage,
};
use datafusion::{
    arrow::datatypes::SchemaRef,
    common::OwnedTableReference,
    datasource::TableProvider,
    error::{DataFusionError, Result as DataFusionResult},
    execution::{context::SessionState, RecordBatchStream, TaskContext},
    logical_expr::{Expr, TableProviderFilterPushDown, TableType},
    physical_plan::{
        project_schema, DisplayAs, DisplayFormatType, ExecutionPlan, SendableRecordBatchStream,
    },
};
use tonic::transport::{channel, Channel};

#[derive(Debug, Snafu)]
pub enum FlightSQLError {
    #[snafu(display("Unable to connect to FlightSQL Server"))]
    UnableToConnectToServer,

    #[snafu(display("Unable to generate SQL: {source}"))]
    UnableToGenerateSQL { source: expr::Error },

    #[snafu(display("Unable to query FlightSQL: {source}"))]
    Flight { source: flight_client::Error },

    #[snafu(display("Unable to query FlightSQL: {source}"))]
    ArrowFlight { source: FlightError },

    #[snafu(display("Unable to retrieve schema"))]
    NoSchema,
}

type Result<T, E = FlightSQLError> = std::result::Result<T, E>;

pub struct FlightSQLTable {
    client: FlightSqlServiceClient<Channel>,
    table_reference: OwnedTableReference,
    schema: SchemaRef,
}

#[allow(clippy::needless_pass_by_value)]
impl FlightSQLTable {
    pub async fn new(
        client: FlightSqlServiceClient<Channel>,
        table_reference: impl Into<OwnedTableReference>,
    ) -> Result<Self> {
        let table_reference: OwnedTableReference = table_reference.into();
        let schema = Self::get_schema(client.clone(), table_reference.clone()).await?;
        Ok(Self {
            client,
            table_reference,
            schema,
        })
    }

    pub async fn from_static(
        s: &'static str,
        table_reference: impl Into<OwnedTableReference>,
    ) -> Result<Self> {
        let channel = channel::Endpoint::from_static(s)
            .connect()
            .map_err(|_| FlightSQLError::UnableToConnectToServer)
            .await?;
        Self::new(FlightSqlServiceClient::new(channel), table_reference.into()).await
    }

    #[must_use]
    pub fn get_table_schema_if_present(
        vec_b: Vec<RecordBatch>,
        table_name: String,
    ) -> Option<SchemaRef> {
        let schema_bytz_opt = vec_b.iter().find_map(|b| {
            let table_schema = match b.column_by_name("table_schema") {
                Some(table_schema) => match table_schema
                    .as_any()
                    .downcast_ref::<array::LargeBinaryArray>()
                {
                    Some(ts) => ts,
                    None => return None,
                },
                None => return None, // If no table_schema, then early exit.
            };

            if let Some(table_names) = b.column_by_name("table_name") {
                if let Some(table_name_bytz) = table_names
                    .as_any()
                    .downcast_ref::<array::LargeBinaryArray>()
                {
                    for i in 0..table_name_bytz.len() {
                        if let Ok(table_str) = std::str::from_utf8(table_name_bytz.value(i))
                            .map_err(|_| FlightSQLError::NoSchema {})
                        {
                            if table_str == table_name {
                                return Some(table_schema.value(i));
                            }
                            return None; // No schema with table_name, return nothing.
                        }
                    }
                }
            };
            None
        });

        if let Some(schema_bytz) = schema_bytz_opt {
            let y = IpcMessage(tonic::codegen::Bytes::copy_from_slice(schema_bytz));
            match Schema::try_from(y).map_err(|_| FlightSQLError::NoSchema {}) {
                Ok(schema) => return Some(Arc::new(schema)),
                Err(_) => return None,
            }
        }

        None
    }

    pub async fn get_schema(
        mut client: FlightSqlServiceClient<Channel>,
        table_reference: OwnedTableReference,
    ) -> Result<SchemaRef> {
        let table_name = table_reference.table();

        let flight_info = client
            .get_tables(CommandGetTables {
                catalog: table_reference
                    .catalog()
                    .map(std::string::ToString::to_string),
                db_schema_filter_pattern: table_reference
                    .schema()
                    .map(std::string::ToString::to_string),
                table_name_filter_pattern: Some(table_name.to_string()),
                include_schema: true,
                table_types: vec![
                    "TABLE".to_string(),
                    "VIEW".to_string(),
                    "SYSTEM TABLE".to_string(),
                ],
            })
            .await
            .map_err(|e| FlightSQLError::ArrowFlight {
                source: arrow_flight::error::FlightError::Arrow(e),
            })?;

        for ep in &flight_info.endpoint {
            if let Some(tkt) = &ep.ticket {
                // Schema: https://github.com/apache/arrow/blob/44edc27e549d82db930421b0d4c76098941afd71/format/FlightSql.proto#L1182-L1190
                let res = client
                    .do_get(tkt.to_owned())
                    .await
                    .map_err(|e| FlightSQLError::ArrowFlight {
                        source: arrow_flight::error::FlightError::Arrow(e),
                    })?
                    .try_collect::<Vec<_>>()
                    .await
                    .map_err(|e| FlightSQLError::ArrowFlight { source: e })?;
                if let Some(schema) = Self::get_table_schema_if_present(res, table_name.to_string())
                {
                    return Ok(schema);
                };
            }
        }
        Err(FlightSQLError::NoSchema {})
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
            match expr::to_sql(filter) {
                Ok(_) => filter_push_down.push(TableProviderFilterPushDown::Exact),
                Err(_) => filter_push_down.push(TableProviderFilterPushDown::Unsupported),
            }
        }

        Ok(filter_push_down)
    }

    async fn scan(
        &self,
        _state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        return self.create_physical_plan(projection, &self.schema(), filters, limit);
    }
}

#[derive(Clone)]
struct FlightSqlExec {
    projected_schema: SchemaRef,
    table_reference: OwnedTableReference,
    client: FlightSqlServiceClient<Channel>,
    filters: Vec<Expr>,
    limit: Option<usize>,
}

impl FlightSqlExec {
    fn new(
        projections: Option<&Vec<usize>>,
        schema: &SchemaRef,
        table_reference: &OwnedTableReference,
        client: FlightSqlServiceClient<Channel>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Self> {
        let projected_schema = project_schema(schema, projections)?;
        Ok(Self {
            projected_schema,
            table_reference: table_reference.clone(),
            client,
            filters: filters.to_vec(),
            limit,
        })
    }

    fn sql(&self) -> Result<String> {
        let columns = self
            .projected_schema
            .fields()
            .iter()
            .map(|f| format!("\"{}\"", f.name()))
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
                .map(expr::to_sql)
                .collect::<expr::Result<Vec<_>>>()
                .context(UnableToGenerateSQLSnafu)?;
            format!("WHERE {}", filter_expr.join(" AND "))
        };

        Ok(format!(
            "SELECT {columns} FROM {table_reference} {where_expr} {limit_expr}",
            table_reference = self.table_reference,
        ))
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
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }

    fn output_partitioning(&self) -> datafusion::physical_plan::Partitioning {
        datafusion::physical_plan::Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[datafusion::physical_expr::PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let sql = match self.sql().map_err(to_execution_error) {
            Ok(sql) => sql,
            Err(error) => return Err(error),
        };

        Ok(Box::pin(StreamConverter::new(
            self.client.clone(),
            sql.as_str(),
            self.schema(),
        )))
    }
}

#[allow(clippy::needless_pass_by_value)]
fn to_stream(
    client: FlightSqlServiceClient<Channel>,
    sql: &str,
) -> impl Stream<Item = Result<RecordBatch>> {
    let client = client.clone();
    let sql = sql.to_string();
    stream! {
    match query(client, sql).await {
        Ok(stream_opt) => {
            if let Some(mut stream) = stream_opt {
                    while let Some(batch) = stream.next().await {
                        match batch {
                            Ok(batch) => yield Ok(batch),
                            Err(error) => {
                                yield Err(FlightSQLError::ArrowFlight { source: error });
                            }
                        }
                    }
                }
            },
            Err(error) => yield Err(error)
        }
    }
}

struct StreamConverter {
    stream: Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>>,
    schema: SchemaRef,
}

impl StreamConverter {
    fn new(client: FlightSqlServiceClient<Channel>, sql: &str, schema: SchemaRef) -> Self {
        let stream = to_stream(client, sql);
        Self {
            stream: Box::pin(stream),
            schema,
        }
    }
}

impl RecordBatchStream for StreamConverter {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for StreamConverter {
    type Item = datafusion::common::Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        match self.stream.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(record_batch))) => Poll::Ready(Some(Ok(record_batch))),
            Poll::Ready(Some(Err(error))) => Poll::Ready(Some(Err(to_execution_error(error)))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[allow(clippy::needless_pass_by_value)]
fn to_execution_error(e: impl Into<Box<dyn std::error::Error>>) -> DataFusionError {
    DataFusionError::Execution(format!("{}", e.into()).to_string())
}

async fn query(
    mut client: FlightSqlServiceClient<Channel>,
    query: String,
) -> Result<Option<FlightRecordBatchStream>, FlightSQLError> {
    match client.clone().execute(query, None).await {
        Ok(flight_info) => match flight_info.endpoint.first() {
            Some(ep) => {
                if let Some(tkt) = &ep.ticket {
                    match client
                        .do_get(tkt.to_owned())
                        .await
                        .map_err(|e| FlightSQLError::ArrowFlight { source: e.into() })
                    {
                        Ok(flight_data) => Ok(Some(flight_data)),
                        Err(err) => Err(err),
                    }
                } else {
                    Ok(None)
                }
            }
            None => Ok(None),
        },
        Err(e) => Err(FlightSQLError::ArrowFlight { source: e.into() }),
    }
}
