use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::sql::TableReference;
use duckdb::DuckdbConnectionManager;
use snafu::ResultExt;

use super::DbConnection;
use super::DuckDBSnafu;
use super::Result;

pub struct DuckDbConnection {
    pub conn: r2d2::PooledConnection<DuckdbConnectionManager>,
}

impl DbConnection<DuckdbConnectionManager> for DuckDbConnection {
    fn new(conn: r2d2::PooledConnection<DuckdbConnectionManager>) -> Self
    where
        Self: Sized,
    {
        DuckDbConnection { conn }
    }

    fn get_schema(&mut self, table_reference: &TableReference) -> Result<SchemaRef> {
        let mut stmt = self
            .conn
            .prepare(&format!("SELECT * FROM {table_reference} LIMIT 0"))
            .context(DuckDBSnafu)?;

        let result: duckdb::Arrow<'_> = stmt.query_arrow([]).context(DuckDBSnafu)?;

        Ok(result.get_schema())
    }

    fn query_arrow(&mut self, sql: &str) -> Result<Vec<RecordBatch>> {
        let mut stmt = self.conn.prepare(sql).context(super::DuckDBSnafu)?;

        let result: duckdb::Arrow<'_> = stmt.query_arrow([]).context(DuckDBSnafu)?;

        Ok(result.collect())
    }

    fn execute(&mut self, sql: &str) -> Result<u64> {
        let rows_modified = self.conn.execute(sql, []).context(DuckDBSnafu)?;
        Ok(rows_modified as u64)
    }
}
