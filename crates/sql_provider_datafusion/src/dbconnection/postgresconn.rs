use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::sql::TableReference;
use r2d2_postgres::postgres::NoTls;
use r2d2_postgres::PostgresConnectionManager;
use snafu::ResultExt;

use super::DbConnection;
use super::PostgresSnafu;
use super::Result;

#[allow(clippy::module_name_repetitions)]
pub struct PostgresConnection {
    pub conn: r2d2::PooledConnection<PostgresConnectionManager<NoTls>>,
}

impl DbConnection<PostgresConnectionManager<NoTls>> for PostgresConnection {
    fn new(conn: r2d2::PooledConnection<PostgresConnectionManager<NoTls>>) -> Self
    where
        Self: Sized,
    {
        PostgresConnection { conn }
    }

    fn get_schema(&self, _table_reference: &TableReference) -> Result<SchemaRef> {
        todo!()
    }

    fn query_arrow(&self, _sql: &str) -> Result<Vec<RecordBatch>> {
        todo!()
    }

    fn execute(&mut self, sql: &str) -> Result<u64> {
        self.conn.execute(sql, &[]).context(PostgresSnafu)
    }
}
