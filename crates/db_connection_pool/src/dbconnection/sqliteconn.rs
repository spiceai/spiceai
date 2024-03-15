use std::any::Any;

use arrow::datatypes::SchemaRef;
use arrow_sql_gen::sqlite::rows_to_arrow;
use bb8_sqlite::RusqliteConnectionManager;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::sql::TableReference;
use rusqlite::ToSql;
use snafu::prelude::*;

use super::DbConnection;
use super::Result;
use super::SyncDbConnection;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to query: {source}"))]
    QueryError { source: rusqlite::Error },

    #[snafu(display("Failed to convert query result to Arrow: {source}"))]
    ConversionError {
        source: arrow_sql_gen::sqlite::Error,
    },
}

pub struct SqliteConnection {
    pub conn: bb8::PooledConnection<'static, RusqliteConnectionManager>,
}

impl<'a> DbConnection<bb8::PooledConnection<'static, RusqliteConnectionManager>, &'a dyn ToSql>
    for SqliteConnection
{
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn as_sync(
        &self,
    ) -> Option<
        &dyn SyncDbConnection<
            bb8::PooledConnection<'static, RusqliteConnectionManager>,
            &'a dyn ToSql,
        >,
    > {
        Some(self)
    }
}

impl SyncDbConnection<bb8::PooledConnection<'static, RusqliteConnectionManager>, &dyn ToSql>
    for SqliteConnection
{
    fn new(conn: bb8::PooledConnection<'static, RusqliteConnectionManager>) -> Self {
        SqliteConnection { conn }
    }

    fn get_schema(&self, table_reference: &TableReference) -> Result<SchemaRef> {
        let mut stmt = self
            .conn
            .prepare(&format!("SELECT * FROM {table_reference} LIMIT 1"))
            .context(QuerySnafu)?;
        let column_count = stmt.column_count();
        let rows = stmt.query([]).context(QuerySnafu)?;
        let rec = rows_to_arrow(rows, column_count).context(ConversionSnafu)?;
        let schema = rec.schema();
        Ok(schema)
    }

    fn query_arrow(&self, sql: &str, params: &[&dyn ToSql]) -> Result<SendableRecordBatchStream> {
        let mut stmt = self.conn.prepare(sql).context(QuerySnafu)?;
        let column_count = stmt.column_count();
        let rows = stmt.query(params).context(QuerySnafu)?;
        let rec = rows_to_arrow(rows, column_count).context(ConversionSnafu)?;
        let schema = rec.schema();
        let recs = vec![rec];
        Ok(Box::pin(MemoryStream::try_new(recs, schema, None)?))
    }

    fn execute(&self, sql: &str, params: &[&dyn ToSql]) -> Result<u64> {
        let rows_modified = self.conn.execute(sql, params)?;
        Ok(rows_modified as u64)
    }
}
