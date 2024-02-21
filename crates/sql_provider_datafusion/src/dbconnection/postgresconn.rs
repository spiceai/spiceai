use std::any::Any;
use std::sync::Arc;

use arrow_flight::sql::client::FlightSqlServiceClient;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::sql::TableReference;
use duckdb::arrow::array::RecordBatch;
use futures::StreamExt;
use postgres::types::ToSql;
use r2d2_postgres::postgres::NoTls;
use r2d2_postgres::PostgresConnectionManager;
use snafu::{prelude::*, ResultExt};
use tokio::runtime::Handle;
use tokio::sync::Mutex;
use tonic::transport::Channel;

use super::DbConnection;
use super::Result;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("PostgresError: {source}"))]
    PostgresError { source: postgres::Error },

    #[snafu(display("No FlightSqlClient available"))]
    NoFlightSqlClient,

    #[snafu(display("Unable to query: {source}"))]
    UnableToQuery { source: arrow_schema::ArrowError },

    #[snafu(display("Received no ticket from flight SQL endpoint"))]
    NoTicketReceived,

    #[snafu(display("No schema returned from flight SQL endpoint"))]
    NoSchemaReturned,
}

#[allow(clippy::module_name_repetitions)]
pub struct PostgresConnection {
    pub conn: r2d2::PooledConnection<PostgresConnectionManager<NoTls>>,
    flight_sql_client: Option<Arc<Mutex<FlightSqlServiceClient<Channel>>>>,
}

impl PostgresConnection {
    pub fn new(
        conn: r2d2::PooledConnection<PostgresConnectionManager<NoTls>>,
        flight_sql_client: Option<Arc<Mutex<FlightSqlServiceClient<Channel>>>>,
    ) -> Self {
        PostgresConnection {
            conn,
            flight_sql_client,
        }
    }
}

impl<'a> DbConnection<PostgresConnectionManager<NoTls>, &'a (dyn ToSql + Sync)>
    for PostgresConnection
{
    fn new(conn: r2d2::PooledConnection<PostgresConnectionManager<NoTls>>) -> Self
    where
        Self: Sized,
    {
        PostgresConnection {
            conn,
            flight_sql_client: None,
        }
    }

    fn get_schema(&mut self, table_reference: &TableReference) -> Result<SchemaRef> {
        let sql = &format!("SELECT * FROM {table_reference} LIMIT 0");

        tokio::task::block_in_place(move || {
            Handle::current().block_on(async {
                let Some(client) = &self.flight_sql_client else {
                    return NoFlightSqlClientSnafu.fail()?;
                };

                let mut stmt = client
                    .lock()
                    .await
                    .prepare(sql.to_string(), None)
                    .await
                    .context(UnableToQuerySnafu)?;

                let flight_info = stmt.execute().await.context(UnableToQuerySnafu)?;

                let Some(ticket) = flight_info.endpoint[0].ticket.as_ref() else {
                    return NoTicketReceivedSnafu.fail()?;
                };
                let mut flight_record_batch_stream = client
                    .lock()
                    .await
                    .do_get(ticket.clone())
                    .await
                    .context(UnableToQuerySnafu)?;

                while let Some(_batch) = flight_record_batch_stream.next().await {
                    continue;
                }
                let Some(schema) = flight_record_batch_stream.schema() else {
                    return NoSchemaReturnedSnafu.fail()?;
                };
                Ok(schema.to_owned())
            })
        })
    }

    fn query_arrow(
        &mut self,
        sql: &str,
        _params: &[&'a (dyn ToSql + Sync)],
    ) -> Result<Vec<RecordBatch>> {
        // DataFusion ExecutionPlan does not support async, so we can't make the query_arrow method async and
        // instead need to block.
        tokio::task::block_in_place(move || {
            Handle::current().block_on(async {
                let Some(client) = &self.flight_sql_client else {
                    return NoFlightSqlClientSnafu.fail()?;
                };

                let mut stmt = client
                    .lock()
                    .await
                    .prepare(sql.to_string(), None)
                    .await
                    .context(UnableToQuerySnafu)?;

                let flight_info = stmt.execute().await.context(UnableToQuerySnafu)?;

                let Some(ticket) = flight_info.endpoint[0].ticket.as_ref() else {
                    return NoTicketReceivedSnafu.fail()?;
                };
                let mut flight_record_batch_stream = client
                    .lock()
                    .await
                    .do_get(ticket.clone())
                    .await
                    .context(UnableToQuerySnafu)?;

                let mut result_data = vec![];
                while let Some(batch) = flight_record_batch_stream.next().await {
                    match batch {
                        Ok(batch) => {
                            result_data.push(batch);
                        }
                        Err(error) => {
                            tracing::error!("Failed to read batch from flight client: {:?}", error);
                            return Ok(result_data);
                        }
                    };
                }
                Ok(result_data)
            })
        })
    }

    fn execute(&mut self, sql: &str, params: &[&(dyn ToSql + Sync)]) -> Result<u64> {
        let rows_modified = self.conn.execute(sql, params).context(PostgresSnafu)?;
        Ok(rows_modified)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}
