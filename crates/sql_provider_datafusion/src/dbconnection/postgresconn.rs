use std::any::Any;

use bb8_postgres::tokio_postgres::types::ToSql;
use bb8_postgres::tokio_postgres::NoTls;
use bb8_postgres::PostgresConnectionManager;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::sql::TableReference;
use snafu::prelude::*;

use super::DbConnection;
use super::Result;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("DuckDBError: {source}"))]
    DuckDBError { source: duckdb::Error },
}

pub struct PostgresConnection {
    pub conn: bb8::PooledConnection<'static, PostgresConnectionManager<NoTls>>,
}

impl DbConnection<bb8::PooledConnection<'static, PostgresConnectionManager<NoTls>>, &dyn ToSql>
    for PostgresConnection
{
    fn new(conn: bb8::PooledConnection<'static, PostgresConnectionManager<NoTls>>) -> Self {
        PostgresConnection { conn }
    }

    fn get_schema(&mut self, table_reference: &TableReference) -> Result<SchemaRef> {
        todo!()
    }

    fn query_arrow(
        &mut self,
        sql: &str,
        params: &[&dyn ToSql],
    ) -> Result<SendableRecordBatchStream> {
        todo!()
    }

    fn execute(&mut self, sql: &str, params: &[&dyn ToSql]) -> Result<u64> {
        todo!()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}
