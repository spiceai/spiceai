use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::sql::TableReference;
use duckdb::DuckdbConnectionManager;
use duckdb::ToSql;
use flight_client::FlightClient;
use futures::StreamExt;
use snafu::prelude::*;
use snafu::{prelude::*, ResultExt};
use std::{any::Any, fmt, sync::Arc};
use tokio::runtime::Handle;

use datafusion::{
    common::OwnedTableReference,
    datasource::TableProvider,
    error::{DataFusionError, Result as DataFusionResult},
    execution::{context::SessionState, TaskContext},
    logical_expr::{Expr, TableProviderFilterPushDown, TableType},
    physical_plan::{
        memory::MemoryStream, project_schema, DisplayAs, DisplayFormatType, ExecutionPlan,
        SendableRecordBatchStream,
    },
};

use super::DbConnection;
use super::Result;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to generate SQL: {source}"))]
    UnableToGenerateSQL {
        source: super::super::expr::Error,
    },

    #[snafu(display("Unable to query FlightSQL: {source}"))]
    Flight {
        source: flight_client::Error,
    },

    NoSchema,
}

pub struct FlightConnection {
    // pub _conn: r2d2::PooledConnection<DuckdbConnectionManager>,
    client: FlightClient,
}

impl FlightConnection {
    pub fn new_with_client(client: FlightClient) -> Self {
        Self { client }
    }
}

impl DbConnection<DuckdbConnectionManager, &dyn ToSql> for FlightConnection {
    fn new(conn: r2d2::PooledConnection<DuckdbConnectionManager>) -> Self {
        panic!("not implemented");
    }

    fn get_schema(&mut self, table_reference: &TableReference) -> Result<SchemaRef> {
        tokio::task::block_in_place(move || {
            Handle::current().block_on(async {
                let mut stream = self
                    .client
                    .clone()
                    .query(format!("SELECT * FROM {table_reference} limit 1").as_str())
                    .await
                    .map_err(|error| Error::Flight { source: error })?;

                if stream.next().await.is_some() {
                    if let Some(schema) = stream.schema() {
                        Ok(Arc::clone(schema))
                    } else {
                        Err(Error::NoSchema {}.into())
                    }
                } else {
                    Err(Error::NoSchema {}.into())
                }
            })
        })
    }

    fn query_arrow(&mut self, sql: &str, params: &[&dyn ToSql]) -> Result<Vec<RecordBatch>> {
        let data = tokio::task::block_in_place(move || {
            Handle::current().block_on(async {
                let result = self.client.clone().query(sql).await;

                let mut flight_record_batch_stream = match result {
                    Ok(stream) => stream,
                    Err(error) => {
                        tracing::error!("Failed to query with flight client: {:?}", error);
                        return Err(error);
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
                        }
                    };
                }

                Ok(result_data)
            })
        });

        Ok(data?)
    }

    fn execute(&mut self, sql: &str, params: &[&dyn ToSql]) -> Result<u64> {
        Ok(1 as u64)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}
