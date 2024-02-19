use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use duckdb_rs::DuckdbConnectionManager;
use snafu::ResultExt;

use super::DbConnection;
use super::DuckDBSnafu;
use super::Result;

pub struct DuckDbConnection {
    conn: r2d2::PooledConnection<DuckdbConnectionManager>,
}

impl DbConnection<DuckdbConnectionManager> for DuckDbConnection {
    fn new(conn: r2d2::PooledConnection<DuckdbConnectionManager>) -> Self
    where
        Self: Sized,
    {
        DuckDbConnection { conn }
    }

    fn get_schema(&self, table: &str) -> Result<SchemaRef> {
        let mut stmt = self
            .conn
            .prepare(&format!("SELECT * FROM {table} LIMIT 0"))
            .context(DuckDBSnafu)?;

        let result: duckdb_rs::Arrow<'_> = stmt.query_arrow([]).context(DuckDBSnafu)?;

        Ok(result.get_schema())
    }

    fn query_arrow(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        let mut stmt = self.conn.prepare(sql).context(super::DuckDBSnafu)?;

        let result: duckdb_rs::Arrow<'_> = stmt.query_arrow([]).context(DuckDBSnafu)?;

        Ok(result.collect())
    }

    fn execute(&self, sql: &str, params: [usize; 2]) -> Result<usize> {
        let mut stmt = self.conn.prepare(sql).context(DuckDBSnafu)?;
        stmt.execute(params).context(DuckDBSnafu)
    }
}
