/*
Copyright 2024 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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

impl<'a> DbConnection<r2d2::PooledConnection<DuckdbConnectionManager>, &'a dyn ToSql>
    for DuckDbConnection
{
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn as_sync(
        &self,
    ) -> Option<&dyn SyncDbConnection<r2d2::PooledConnection<DuckdbConnectionManager>, &'a dyn ToSql>>
    {
        Some(self)
    }
}

impl SyncDbConnection<r2d2::PooledConnection<DuckdbConnectionManager>, &dyn ToSql>
    for DuckDbConnection
{
    fn new(conn: r2d2::PooledConnection<DuckdbConnectionManager>) -> Self {
        DuckDbConnection { conn }
    }

    fn get_schema(&self, table_reference: &TableReference) -> Result<SchemaRef, super::Error> {
        let mut stmt = self
            .conn
            .prepare(&format!("SELECT * FROM {table_reference} LIMIT 0"))
            .boxed()
            .context(super::UnableToGetSchemaSnafu)?;

        let result: duckdb::Arrow<'_> = stmt
            .query_arrow([])
            .boxed()
            .context(super::UnableToGetSchemaSnafu)?;

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
