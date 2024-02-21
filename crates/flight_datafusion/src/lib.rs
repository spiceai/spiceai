#![allow(clippy::missing_errors_doc)]

use async_trait::async_trait;
use flight_client::FlightClient;
use futures::StreamExt;
use snafu::prelude::*;
use std::{any::Any, fmt, sync::Arc};
use tokio::runtime::Handle;

use datafusion::{
    arrow::datatypes::SchemaRef,
    common::OwnedTableReference,
    datasource::TableProvider,
    error::{DataFusionError, Result as DataFusionResult},
    execution::{context::SessionState, TaskContext},
    logical_expr::{Expr, TableProviderFilterPushDown, TableType},
    physical_plan::{
        memory::MemoryStream, project_schema, DisplayAs, DisplayFormatType, ExecutionPlan,
        SendableRecordBatchStream,
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

    Tokio {},
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub struct FlightSQLTable {
    client: FlightClient,
    schema: SchemaRef,
    table_reference: OwnedTableReference,
}

impl FlightSQLTable {
    pub fn new(
        client: FlightClient,
        table_reference: impl Into<OwnedTableReference>,
    ) -> Result<Self> {
        let table_reference = table_reference.into();
        let schema = Self::get_schema(client.clone(), &table_reference);
        Ok(Self {
            client,
            schema: schema.unwrap(),
            table_reference,
        })
    }

    pub fn new_with_schema(
        client: FlightClient,
        schema: impl Into<SchemaRef>,
        table_reference: impl Into<OwnedTableReference>,
    ) -> Self {
        Self {
            client,
            schema: schema.into(),
            table_reference: table_reference.into(),
        }
    }

    fn get_schema<'a>(
        client: FlightClient,
        table_reference: impl Into<TableReference<'a>>,
    ) -> Result<SchemaRef> {
        tokio::task::block_in_place(move || {
            Handle::current().block_on(async {
                let flight_record_batch_stream_result = client
                    .clone()
                    .query(format!("SELECT * FROM {} limit 1", table_reference.into()).as_str())
                    .await;

                match flight_record_batch_stream_result {
                    Ok(mut stream) => {
                        stream.next().await;
                        let schema = stream.schema();
                        Ok(Arc::clone(schema.unwrap()))
                    }
                    Err(error) => {
                        tracing::error!("Failed to query with flight client: {:?}", error);
                        todo!()
                    }
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
    client: FlightClient,
    filters: Vec<Expr>,
    limit: Option<usize>,
}

impl FlightSQLExec {
    fn new(
        projections: Option<&Vec<usize>>,
        schema: &SchemaRef,
        table_reference: &OwnedTableReference,
        client: FlightClient,
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
            .map(|f| f.name().as_str())
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
        let sql = self.sql().map_err(to_execution_error);
        let Ok(sql) = sql else {
            return Err(to_execution_error(sql.unwrap_err()));
        };

        tracing::info!("Executing SQL: {sql}");

        let data = tokio::task::block_in_place(move || {
            Handle::current().block_on(async {
                let result = self.client.clone().query(sql.as_str()).await;

                let mut flight_record_batch_stream = match result {
                    Ok(stream) => stream,
                    Err(error) => {
                        tracing::error!("Failed to query with flight client: {:?}", error);
                        return Err(to_execution_error(error));
                    }
                };

                let mut result_data = vec![];
                while let Some(batch) = flight_record_batch_stream.next().await {
                    match batch {
                        Ok(batch) => {
                            result_data.push(batch);
                        }
                        Err(error) => {
                            tracing::error!("Failed to read batch from flight client: {:?}", error);
                            return Err(to_execution_error(error));
                        }
                    };
                }

                Ok(result_data)
            })
        });

        Ok(Box::pin(MemoryStream::try_new(
            data.unwrap(),
            self.schema(),
            None,
        )?))
    }
}

#[allow(clippy::needless_pass_by_value)]
fn to_execution_error(e: impl Into<Box<dyn std::error::Error>>) -> DataFusionError {
    DataFusionError::Execution(format!("{}", e.into()).to_string())
}
