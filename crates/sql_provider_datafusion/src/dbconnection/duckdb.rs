use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::sql::TableReference;
use duckdb_rs::DuckdbConnectionManager;
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

    fn get_schema(&self, table_reference: &TableReference) -> Result<SchemaRef> {
        let mut stmt = self
            .conn
            .prepare(&format!("SELECT * FROM {table_reference} LIMIT 0"))
            .context(DuckDBSnafu)?;

        let result: duckdb_rs::Arrow<'_> = stmt.query_arrow([]).context(DuckDBSnafu)?;

        Ok(result.get_schema())
    }

    fn query_arrow(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        let mut stmt = self.conn.prepare(sql).context(super::DuckDBSnafu)?;

        let result: duckdb_rs::Arrow<'_> = stmt.query_arrow([]).context(DuckDBSnafu)?;

        Ok(result.collect())
    }

    fn execute(&self, sql: &str) -> Result<usize> {
        let mut stmt = self.conn.prepare(sql).context(DuckDBSnafu)?;
        stmt.execute([]).context(DuckDBSnafu)
    }
}
