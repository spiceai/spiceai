use std::any::Any;

use arrow_flight::sql::client::FlightSqlServiceClient;
use arrow_schema::Schema;
use async_stream::try_stream;
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::sql::TableReference;
use futures::StreamExt;
use postgres::types::ToSql;
use snafu::{prelude::*, ResultExt};
use tokio::runtime::Handle;
use tonic::transport::Channel;

use super::DbConnection;
use super::FlightStream;
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
    pub conn: FlightSqlServiceClient<Channel>,
}

#[async_trait]
impl<'a> DbConnection<FlightSqlServiceClient<Channel>, &'a (dyn ToSql + Sync)>
    for PostgresConnection
{
    fn new(conn: FlightSqlServiceClient<Channel>) -> Self
    where
        Self: Sized,
    {
        PostgresConnection { conn }
    }

    async fn get_schema(&mut self, table_reference: &TableReference) -> Result<SchemaRef> {
        let mut client = self.conn.clone();
        client.set_header("x-flight-sql-database", "flight-sql-test");
        client
            .handshake("postgres", "postgres")
            .await
            .context(UnableToQuerySnafu)?;

        let flight_info = client
            .execute(format!("SELECT * FROM {table_reference} LIMIT 0"), None)
            .await
            .context(UnableToQuerySnafu)?;

        let Some(ticket) = flight_info.endpoint[0].ticket.as_ref() else {
            return NoTicketReceivedSnafu.fail()?;
        };
        let mut flight_record_batch_stream = client
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
    }

    fn query_arrow(
        &mut self,
        sql: &str,
        _params: &[&'a (dyn ToSql + Sync)],
    ) -> Result<SendableRecordBatchStream> {
        let mut client = self.conn.clone();
        client.set_header("x-flight-sql-database", "flight-sql-test");
        let sql = sql.to_string();
        let schema = SchemaRef::new(Schema::empty());

        let stream = Box::pin(try_stream! {
            client.handshake("postgres", "postgres")
                .await
                .map_err(to_execution_error)?;

            let flight_info = client.execute(sql.to_string(), None).await.map_err(to_execution_error)?;

            let Some(ticket) = flight_info.endpoint[0].ticket.as_ref() else {
                Err(DataFusionError::Execution("No ticket received for query".to_string()))?;
                return;
            };
            let mut flight_record_batch_stream =
                client.do_get(ticket.clone()).await.map_err(to_execution_error)?;

            while let Some(batch) = flight_record_batch_stream.next().await {
                match batch {
                    Ok(batch) => {
                        yield batch;
                    }
                    Err(e) => {
                        Err(to_execution_error(e))?;
                    }
                };
            };
        });

        Ok(Box::pin(FlightStream::new(stream, schema)))
    }

    fn execute(&mut self, sql: &str, _params: &[&(dyn ToSql + Sync)]) -> Result<u64> {
        let mut client = self.conn.clone();
        client.set_header("x-flight-sql-database", "flight-sql-test");

        tokio::task::block_in_place(move || {
            Handle::current().block_on(async {
                client
                    .handshake("postgres", "postgres")
                    .await
                    .context(UnableToQuerySnafu)?;

                let rows_modified = client
                    .execute_update(sql.to_string(), None)
                    .await
                    .context(UnableToQuerySnafu)?;
                #[allow(clippy::cast_sign_loss)] // Rows modified will never be negative
                Ok(rows_modified as u64)
            })
        })
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[allow(clippy::needless_pass_by_value)]
fn to_execution_error(e: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> DataFusionError {
    DataFusionError::Execution(format!("{}", e.into()).to_string())
}
