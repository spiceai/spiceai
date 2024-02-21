use std::any::Any;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::sql::TableReference;
use postgres::types::ToSql;
use r2d2_postgres::postgres::NoTls;
use r2d2_postgres::PostgresConnectionManager;
use snafu::{prelude::*, ResultExt};

use super::DbConnection;
use super::Result;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("PostgresError: {source}"))]
    PostgresError { source: postgres::Error },
}

#[allow(clippy::module_name_repetitions)]
pub struct PostgresConnection {
    pub conn: r2d2::PooledConnection<PostgresConnectionManager<NoTls>>,
}

impl DbConnection<PostgresConnectionManager<NoTls>, &(dyn ToSql + Sync)> for PostgresConnection {
    fn new(conn: r2d2::PooledConnection<PostgresConnectionManager<NoTls>>) -> Self
    where
        Self: Sized,
    {
        PostgresConnection { conn }
    }

    fn get_schema(&mut self, _table_reference: &TableReference) -> Result<SchemaRef> {
        todo!()
    }

    fn query_arrow(
        &mut self,
        _sql: &str,
        _params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<RecordBatch>> {
        todo!()
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
