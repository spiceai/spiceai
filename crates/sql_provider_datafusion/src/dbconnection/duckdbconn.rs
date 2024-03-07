use std::any::Any;

use arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::sql::TableReference;
use duckdb::DuckdbConnectionManager;
use duckdb::ToSql;
use snafu::{prelude::*, ResultExt};

use super::DbConnection;
use super::Result;
use super::SyncDbConnection;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("DuckDBError: {source}"))]
    DuckDBError { source: duckdb::Error },
}

pub struct DuckDbConnection {
    pub conn: r2d2::PooledConnection<DuckdbConnectionManager>,
}

impl DbConnection<r2d2::PooledConnection<DuckdbConnectionManager>, &dyn ToSql>
    for DuckDbConnection
{
    fn connection_type(&self) -> &str {
        "sync"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl SyncDbConnection<r2d2::PooledConnection<DuckdbConnectionManager>, &dyn ToSql>
    for DuckDbConnection
{
    fn new(conn: r2d2::PooledConnection<DuckdbConnectionManager>) -> Self {
        DuckDbConnection { conn }
    }

    fn get_schema(&self, table_reference: &TableReference) -> Result<SchemaRef> {
        let mut stmt = self
            .conn
            .prepare(&format!("SELECT * FROM {table_reference} LIMIT 0"))
            .context(DuckDBSnafu)?;

        let result: duckdb::Arrow<'_> = stmt.query_arrow([]).context(DuckDBSnafu)?;

        Ok(result.get_schema())
    }

    fn query_arrow(&self, sql: &str, params: &[&dyn ToSql]) -> Result<SendableRecordBatchStream> {
        let mut stmt = self.conn.prepare(sql).context(DuckDBSnafu)?;

        let result: duckdb::Arrow<'_> = stmt.query_arrow(params).context(DuckDBSnafu)?;
        let schema = result.get_schema();
        let recs: Vec<RecordBatch> = result.collect();
        Ok(Box::pin(MemoryStream::try_new(recs, schema, None)?))
    }

    fn execute(&self, sql: &str, params: &[&dyn ToSql]) -> Result<u64> {
        let rows_modified = self.conn.execute(sql, params).context(DuckDBSnafu)?;
        Ok(rows_modified as u64)
    }
}
