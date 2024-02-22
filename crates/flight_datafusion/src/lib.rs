#![allow(clippy::missing_errors_doc)]

use arrow::array::RecordBatch;
use async_stream::stream;
use async_trait::async_trait;
use flight_client::FlightClient;
use futures::{Stream, StreamExt};
use snafu::prelude::*;
use std::{any::Any, fmt, ops::Deref, pin::Pin, sync::Arc, task::Poll};
use tokio::runtime::Handle;

use arrow_flight::error::FlightError;
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
    sql::TableReference,
};

mod expr;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to generate SQL: {source}"))]
    UnableToGenerateSQL {
        source: expr::Error,
    },

    #[snafu(display("Unable to query FlightSQL: {source}"))]
    Flight {
        source: flight_client::Error,
    },

    #[snafu(display("Unable to query FlightSQL: {source}"))]
    ArrowFlight {
        source: FlightError,
    },

    UnableToQuery {},

    NoSchema,
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub struct FlightSQLTable {
    client: Arc<FlightClient>,
    schema: SchemaRef,
    table_reference: OwnedTableReference,
}

impl FlightSQLTable {
    pub fn new(
        client: Arc<FlightClient>,
        table_reference: impl Into<OwnedTableReference>,
    ) -> Result<Self> {
        let table_reference = table_reference.into();
        let schema = Self::get_schema(Arc::clone(&client), &table_reference)?;
        Ok(Self {
            client,
            schema,
            table_reference,
        })
    }

    pub fn new_with_schema(
        client: Arc<FlightClient>,
        schema: impl Into<SchemaRef>,
        table_reference: impl Into<OwnedTableReference>,
    ) -> Self {
        Self {
            client,
            schema: schema.into(),
            table_reference: table_reference.into(),
        }
    }

    #[allow(clippy::needless_pass_by_value)]
    fn get_schema<'a>(
        client: Arc<FlightClient>,
        table_reference: impl Into<TableReference<'a>>,
    ) -> Result<SchemaRef> {
        let mut client = client.deref().clone();
        tokio::task::block_in_place(move || {
            Handle::current().block_on(async {
                let mut stream = client
                    .query(format!("SELECT * FROM {} limit 1", table_reference.into()).as_str())
                    .await
                    .map_err(|error| Error::Flight { source: error })?;

                if stream.next().await.is_some() {
                    if let Some(schema) = stream.schema() {
                        Ok(Arc::clone(schema))
                    } else {
                        Err(Error::NoSchema {})
                    }
                } else {
                    Err(Error::NoSchema {})
                }
            })
        })
    }

    fn create_physical_plan(
        &self,
        projections: Option<&Vec<usize>>,
        schema: &SchemaRef,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(FlightSQLExec::new(
            projections,
            schema,
            &self.table_reference,
            Arc::clone(&self.client),
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
            match expr::expr_to_sql(filter) {
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
struct FlightSQLExec {
    projected_schema: SchemaRef,
    table_reference: OwnedTableReference,
    client: Arc<FlightClient>,
    filters: Vec<Expr>,
    limit: Option<usize>,
}

impl FlightSQLExec {
    fn new(
        projections: Option<&Vec<usize>>,
        schema: &SchemaRef,
        table_reference: &OwnedTableReference,
        client: Arc<FlightClient>,
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
                .map(expr::expr_to_sql)
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

impl std::fmt::Debug for FlightSQLExec {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let sql = self.sql().unwrap_or_default();
        write!(f, "FlightSQLExec sql={sql}")
    }
}

impl DisplayAs for FlightSQLExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        let sql = self.sql().unwrap_or_default();
        write!(f, "FlightSQL sql={sql}")
    }
}

impl ExecutionPlan for FlightSQLExec {
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
            Arc::clone(&self.client),
            sql.as_str(),
            self.schema(),
        )))
    }
}

#[allow(clippy::needless_pass_by_value)]
fn to_stream(client: Arc<FlightClient>, sql: &str) -> impl Stream<Item = Result<RecordBatch>> {
    let mut client = client.deref().clone();
    let sql = sql.to_string();
    stream! {
        match client.query(sql.as_str()).await {
            Ok(mut stream) => {
                while let Some(batch) = stream.next().await {
                    match batch {
                        Ok(batch) => yield Ok(batch),
                        Err(error) => {
                            yield Err(Error::ArrowFlight { source: error });
                        }
                    }
                }
            }
            Err(error) => yield Err(Error::Flight{ source: error})
        }
    }
}

struct StreamConverter {
    stream: Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>>,
    schema: SchemaRef,
}

impl StreamConverter {
    fn new(client: Arc<FlightClient>, sql: &str, schema: SchemaRef) -> Self {
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
