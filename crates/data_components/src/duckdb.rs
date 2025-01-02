/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use crate::{
    delete::{DeletionExec, DeletionSink, DeletionTableProvider},
    Read, ReadWrite,
};
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::{
    catalog::Session, datasource::TableProvider, logical_expr::Expr, physical_plan::ExecutionPlan,
    sql::TableReference,
};
use datafusion_table_providers::{
    duckdb::{write::DuckDBTableWriter, DuckDB, DuckDBTableFactory},
    sql::{
        db_connection_pool::dbconnection::duckdbconn::DuckDbConnection,
        sql_provider_datafusion::expr::Engine,
    },
    util,
};
use snafu::prelude::*;
use std::sync::Arc;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to delete data from the duckdb table: {source}"))]
    UnableToDeleteDuckdbData { source: duckdb::Error },

    #[snafu(display("Unable to query data from the duckdb table: {source}"))]
    UnableToQueryData { source: duckdb::Error },

    #[snafu(display("Unable to commit transaction: {source}"))]
    UnableToCommitTransaction { source: duckdb::Error },

    #[snafu(display("Unable to begin duckdb transaction: {source}"))]
    UnableToBeginTransaction { source: duckdb::Error },
}

type Result<T, E = Error> = std::result::Result<T, E>;

#[async_trait]
impl DeletionTableProvider for DuckDBTableWriter {
    async fn delete_from(
        &self,
        _state: &dyn Session,
        filters: &[Expr],
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(DeletionExec::new(
            Arc::new(DuckDBDeletionSink::new(self.duckdb(), filters)),
            &self.schema(),
        )))
    }
}

struct DuckDBDeletionSink {
    duckdb: Arc<DuckDB>,
    filters: Vec<Expr>,
}

impl DuckDBDeletionSink {
    fn new(duckdb: Arc<DuckDB>, filters: &[Expr]) -> Self {
        Self {
            duckdb,
            filters: filters.to_vec(),
        }
    }
}

#[async_trait]
impl DeletionSink for DuckDBDeletionSink {
    async fn delete_from(&self) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        let mut db_conn = self.duckdb.connect_sync()?;
        let duckdb_conn = DuckDB::duckdb_conn(&mut db_conn)?;
        let sql = util::filters_to_sql(&self.filters, Some(Engine::DuckDB))?;
        let count = delete_from(self.duckdb.table_name(), duckdb_conn, &sql)?;

        Ok(count)
    }
}

#[async_trait]
impl Read for DuckDBTableFactory {
    async fn table_provider(
        &self,
        table_reference: TableReference,
        _schema: Option<SchemaRef>,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn std::error::Error + Send + Sync>> {
        self.table_provider(table_reference).await
    }
}

#[async_trait]
impl ReadWrite for DuckDBTableFactory {
    async fn table_provider(
        &self,
        table_reference: TableReference,
        _schema: Option<SchemaRef>,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn std::error::Error + Send + Sync>> {
        self.read_write_table_provider(table_reference).await
    }
}

fn delete_from(
    table_name: &str,
    duckdb_conn: &mut DuckDbConnection,
    where_clause: &str,
) -> Result<u64> {
    let tx = duckdb_conn
        .conn
        .transaction()
        .context(UnableToBeginTransactionSnafu)?;

    let count_sql = format!(r#"SELECT COUNT(*) FROM "{table_name}" WHERE {where_clause}"#);

    let mut count: u64 = tx
        .query_row(&count_sql, [], |row| row.get::<usize, u64>(0))
        .context(UnableToQueryDataSnafu)?;

    let sql = format!(r#"DELETE FROM "{table_name}" WHERE {where_clause}"#);
    tx.execute(&sql, [])
        .context(UnableToDeleteDuckdbDataSnafu)?;

    count -= tx
        .query_row(&count_sql, [], |row| row.get::<usize, u64>(0))
        .context(UnableToQueryDataSnafu)?;

    tx.commit().context(UnableToCommitTransactionSnafu)?;
    Ok(count)
}
