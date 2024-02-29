use std::any::Any;

use arrow::datatypes::SchemaRef;
use arrow_sql_gen::postgres::rows_to_arrow;
use bb8_postgres::tokio_postgres::types::ToSql;
use bb8_postgres::tokio_postgres::NoTls;
use bb8_postgres::PostgresConnectionManager;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::sql::TableReference;
use snafu::prelude::*;
use tokio::runtime::Handle;

use super::DbConnection;
use super::Result;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to query: {source}"))]
    QueryError {
        source: bb8_postgres::tokio_postgres::Error,
    },

    #[snafu(display("Failed to convert query result to Arrow: {source}"))]
    ConversionError {
        source: arrow_sql_gen::postgres::Error,
    },
}

pub struct PostgresConnection {
    pub conn: bb8::PooledConnection<'static, PostgresConnectionManager<NoTls>>,
}

impl
    DbConnection<
        bb8::PooledConnection<'static, PostgresConnectionManager<NoTls>>,
        &(dyn ToSql + Sync),
    > for PostgresConnection
{
    fn new(conn: bb8::PooledConnection<'static, PostgresConnectionManager<NoTls>>) -> Self {
        PostgresConnection { conn }
    }

    fn get_schema(&self, table_reference: &TableReference) -> Result<SchemaRef> {
        let rows = tokio::task::block_in_place(move || {
            Handle::current().block_on(async {
                self.conn
                    .query(&format!("SELECT * FROM {table_reference} LIMIT 0"), &[])
                    .await
            })
        })
        .context(QuerySnafu)?;
        let rec = rows_to_arrow(rows.as_slice()).context(ConversionSnafu)?;
        let schema = rec.schema();
        Ok(schema)
    }

    fn query_arrow(
        &self,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<SendableRecordBatchStream> {
        let rows = tokio::task::block_in_place(move || {
            Handle::current().block_on(async { self.conn.query(sql, params).await })
        })
        .context(QuerySnafu)?;
        let rec = rows_to_arrow(rows.as_slice()).context(ConversionSnafu)?;
        let schema = rec.schema();
        let recs = vec![rec];
        Ok(Box::pin(MemoryStream::try_new(recs, schema, None)?))
    }

    fn execute(&self, sql: &str, params: &[&(dyn ToSql + Sync)]) -> Result<u64> {
        Ok(tokio::task::block_in_place(move || {
            Handle::current().block_on(async { self.conn.execute(sql, params).await })
        })
        .context(QuerySnafu)?)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}
