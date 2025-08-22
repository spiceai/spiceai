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
    Read, ReadWrite,
    delete::{DeletionExec, DeletionSink, DeletionTableProvider},
};
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::{
    catalog::Session, datasource::TableProvider, logical_expr::Expr, physical_plan::ExecutionPlan,
    sql::TableReference,
};
use datafusion_table_providers::{
    duckdb::{DuckDB, DuckDBTableFactory, TableDefinition, write::DuckDBTableWriter},
    sql::{
        db_connection_pool::duckdbpool::DuckDbConnectionPool, sql_provider_datafusion::expr::Engine,
    },
    util,
};
use duckdb::Transaction;
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

    #[snafu(display(
        "Unable to delete data from the duckdb table. An internal table and base table exist for the same table. Manually migrate the table by deleting '{internal_table}' or {table_name}', and try again."
    ))]
    UnableToDeleteDataInternalTable {
        internal_table: String,
        table_name: String,
    },
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
            Arc::new(DuckDBDeletionSink::new(
                self.pool(),
                self.table_definition(),
                filters,
            )),
            &self.schema(),
        )))
    }
}

struct DuckDBDeletionSink {
    pool: Arc<DuckDbConnectionPool>,
    table_definition: Arc<TableDefinition>,
    filters: Vec<Expr>,
}

impl DuckDBDeletionSink {
    fn new(
        pool: Arc<DuckDbConnectionPool>,
        table_definition: Arc<TableDefinition>,
        filters: &[Expr],
    ) -> Self {
        Self {
            pool,
            table_definition,
            filters: filters.to_vec(),
        }
    }
}

#[async_trait]
impl DeletionSink for DuckDBDeletionSink {
    async fn delete_from(&self) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        let pool = Arc::clone(&self.pool);
        let mut db_conn = pool.connect_sync()?;
        let duckdb_conn = DuckDB::duckdb_conn(&mut db_conn)?;
        let tx = duckdb_conn
            .conn
            .transaction()
            .context(UnableToBeginTransactionSnafu)?;
        let has_table = self.table_definition.has_table(&tx)?;
        let mut internal_tables = self.table_definition.list_internal_tables(&tx)?;
        let table_name = match (internal_tables.pop(), has_table) {
            (Some((table_name, _)), true) => {
                return Err(Box::new(Error::UnableToDeleteDataInternalTable {
                    internal_table: table_name.to_string(),
                    table_name: self.table_definition.name().to_string(),
                }));
            }
            (Some((table_name, _)), false) => table_name,
            (None, true) => self.table_definition.name().clone(),
            (None, false) => {
                return Ok(0);
            }
        };

        let sql = util::filters_to_sql(&self.filters, Some(Engine::DuckDB))?;
        let count = delete_from(&table_name.to_string(), tx, &sql)?;

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

fn delete_from(table_name: &str, tx: Transaction<'_>, where_clause: &str) -> Result<u64> {
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
