use std::any::Any;

use arrow::datatypes::SchemaRef;
use arrow_sql_gen::sqlite::rows_to_arrow;
use async_trait::async_trait;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::sql::TableReference;
use rusqlite::ToSql;
use snafu::prelude::*;
use tokio_rusqlite::Connection;

use super::AsyncDbConnection;
use super::DbConnection;
use super::Result;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("ConnectionError {source}"))]
    ConnectionError { source: tokio_rusqlite::Error },

    #[snafu(display("Unable to query: {source}"))]
    QueryError { source: rusqlite::Error },

    #[snafu(display("Failed to convert query result to Arrow: {source}"))]
    ConversionError {
        source: arrow_sql_gen::sqlite::Error,
    },
}

pub struct SqliteConnection {
    pub conn: Connection,
}

impl DbConnection<Connection, &'static (dyn ToSql + Sync)> for SqliteConnection {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn as_async(&self) -> Option<&dyn AsyncDbConnection<Connection, &'static (dyn ToSql + Sync)>> {
        Some(self)
    }
}

#[async_trait]
impl AsyncDbConnection<Connection, &'static (dyn ToSql + Sync)> for SqliteConnection {
    fn new(conn: Connection) -> Self {
        SqliteConnection { conn }
    }

    async fn get_schema(&self, table_reference: &TableReference) -> Result<SchemaRef> {
        let table_reference = table_reference.to_string();
        let schema = self
            .conn
            .call(move |conn| {
                let mut stmt = conn.prepare(&format!("SELECT * FROM {table_reference} LIMIT 1"))?;
                let column_count = stmt.column_count();
                let rows = stmt.query([])?;
                let rec = rows_to_arrow(rows, column_count)
                    .context(ConversionSnafu)
                    .map_err(to_tokio_rusqlite_error)?;
                let schema = rec.schema();
                Ok(schema)
            })
            .await
            .context(ConnectionSnafu)?;
        Ok(schema)
    }

    async fn query_arrow(
        &self,
        sql: &str,
        params: &[&'static (dyn ToSql + Sync)],
    ) -> Result<SendableRecordBatchStream> {
        let sql = sql.to_string();

        let mut params_copy = Vec::new();
        for param in params {
            params_copy.push(*param);
        }

        let rec = self
            .conn
            .call(move |conn| {
                let mut stmt = conn.prepare(sql.as_str())?;
                for (i, param) in params_copy.iter().enumerate() {
                    stmt.raw_bind_parameter(i + 1, param)?;
                }
                let column_count = stmt.column_count();
                let rows = stmt.raw_query();
                let rec = rows_to_arrow(rows, column_count)
                    .context(ConversionSnafu)
                    .map_err(to_tokio_rusqlite_error)?;
                Ok(rec)
            })
            .await
            .context(ConnectionSnafu)?;

        let schema = rec.schema();
        let recs = vec![rec];
        Ok(Box::pin(MemoryStream::try_new(recs, schema, None)?))
    }

    async fn execute(&self, sql: &str, params: &[&'static (dyn ToSql + Sync)]) -> Result<u64> {
        let sql = sql.to_string();
        let mut params_copy = Vec::new();
        for param in params {
            params_copy.push(*param);
        }

        let rows_modified = self
            .conn
            .call(move |conn| {
                let mut stmt = conn.prepare(sql.as_str())?;
                for (i, param) in params_copy.iter().enumerate() {
                    stmt.raw_bind_parameter(i + 1, param)?;
                }
                let rows_modified = stmt.raw_execute()?;
                Ok(rows_modified)
            })
            .await
            .context(ConnectionSnafu)?;
        Ok(rows_modified as u64)
    }
}

fn to_tokio_rusqlite_error(e: impl Into<Error>) -> tokio_rusqlite::Error {
    tokio_rusqlite::Error::Other(Box::new(e.into()))
}
