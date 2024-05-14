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

use arrow::{array::RecordBatch, datatypes::SchemaRef};
use arrow_sql_gen::statement::{CreateTableBuilder, InsertBuilder};
use async_trait::async_trait;
use datafusion::{
    datasource::{provider::TableProviderFactory, TableProvider},
    error::{DataFusionError, Result as DataFusionResult},
    execution::context::SessionState,
    logical_expr::CreateExternalTable,
    sql::TableReference,
};
use db_connection_pool::{
    dbconnection::{sqliteconn::SqliteConnection, DbConnection},
    sqlitepool::SqliteConnectionPool,
    DbConnectionPool, Mode,
};
use rusqlite::{ToSql, Transaction};
use snafu::prelude::*;
use sql_provider_datafusion::SqlTable;
use std::sync::Arc;
use tokio_rusqlite::Connection;

use crate::delete::DeletionTableProviderAdapter;

use self::write::SqliteTableWriter;

pub mod write;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("DbConnectionError: {source}"))]
    DbConnectionError {
        source: db_connection_pool::dbconnection::GenericError,
    },

    #[snafu(display("DbConnectionPoolError: {source}"))]
    DbConnectionPoolError { source: db_connection_pool::Error },

    #[snafu(display("Unable to downcast DbConnection to SqliteConnection"))]
    UnableToDowncastDbConnection {},

    #[snafu(display("Unable to construct SQLTable instance: {source}"))]
    UnableToConstuctSqlTableProvider {
        source: sql_provider_datafusion::Error,
    },

    #[snafu(display("Unable to create table in Sqlite: {source}"))]
    UnableToCreateTable { source: tokio_rusqlite::Error },

    #[snafu(display("Unable to insert data into the Sqlite table: {source}"))]
    UnableToInsertIntoTable { source: rusqlite::Error },

    #[snafu(display("Unable to insert data into the Sqlite table: {source}"))]
    UnableToInsertIntoTableAsync { source: tokio_rusqlite::Error },

    #[snafu(display("Unable to deleta all table data in Sqlite: {source}"))]
    UnableToDeleteAllTableData { source: rusqlite::Error },

    #[snafu(display("There is a dangling reference to the Sqlite struct in TableProviderFactory.create. This is a bug."))]
    DanglingReferenceToSqlite,
}

type Result<T, E = Error> = std::result::Result<T, E>;

#[allow(clippy::module_name_repetitions)]
pub struct SqliteTableFactory {
    db_path_param: String,
}

impl SqliteTableFactory {
    #[must_use]
    pub fn new() -> Self {
        Self {
            db_path_param: "sqlite_file".to_string(),
        }
    }
}

impl Default for SqliteTableFactory {
    fn default() -> Self {
        Self::new()
    }
}

type DynSqliteConnectionPool =
    dyn DbConnectionPool<Connection, &'static (dyn ToSql + Sync)> + Send + Sync;

#[async_trait]
impl TableProviderFactory for SqliteTableFactory {
    async fn create(
        &self,
        _state: &SessionState,
        cmd: &CreateExternalTable,
    ) -> DataFusionResult<Arc<dyn TableProvider>> {
        let name = cmd.name.to_string();
        let mut options = cmd.options.clone();
        let mode = options.remove("mode").unwrap_or_default();
        let mode: Mode = mode.as_str().into();

        let db_path = cmd
            .options
            .get(self.db_path_param.as_str())
            .cloned()
            .unwrap_or(format!("{name}_sqlite.db"));

        let pool: Arc<SqliteConnectionPool> = Arc::new(
            SqliteConnectionPool::new(&db_path, mode)
                .await
                .context(DbConnectionPoolSnafu)
                .map_err(to_datafusion_error)?,
        );

        let schema: SchemaRef = Arc::new(cmd.schema.as_ref().into());
        let sqlite = Arc::new(Sqlite::new(
            name.clone(),
            Arc::clone(&schema),
            Arc::clone(&pool),
        ));

        let mut db_conn = sqlite.connect().await.map_err(to_datafusion_error)?;
        let sqlite_conn = Sqlite::sqlite_conn(&mut db_conn).map_err(to_datafusion_error)?;

        let table_exists = sqlite.table_exists(sqlite_conn).await;
        if !table_exists {
            let sqlite_in_conn = Arc::clone(&sqlite);
            sqlite_conn
                .conn
                .call(move |conn| {
                    let transaction = conn.transaction()?;
                    sqlite_in_conn.create_table(&transaction)?;
                    transaction.commit()?;
                    Ok(())
                })
                .await
                .context(UnableToCreateTableSnafu)
                .map_err(to_datafusion_error)?;
        }

        let dyn_pool: Arc<DynSqliteConnectionPool> = pool;

        let read_provider = Arc::new(SqlTable::new_with_schema(
            &dyn_pool,
            Arc::clone(&schema),
            TableReference::bare(name.clone()),
        ));

        let sqlite = Arc::into_inner(sqlite)
            .context(DanglingReferenceToSqliteSnafu)
            .map_err(to_datafusion_error)?;

        let read_write_provider = SqliteTableWriter::create(read_provider, sqlite);

        let delete_adapter = DeletionTableProviderAdapter::new(read_write_provider);
        Ok(Arc::new(delete_adapter))
    }
}

fn to_datafusion_error(error: Error) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

#[derive(Clone)]
pub struct Sqlite {
    table_name: String,
    schema: SchemaRef,
    pool: Arc<SqliteConnectionPool>,
}

impl Sqlite {
    #[must_use]
    pub fn new(table_name: String, schema: SchemaRef, pool: Arc<SqliteConnectionPool>) -> Self {
        Self {
            table_name,
            schema,
            pool,
        }
    }

    async fn connect(
        &self,
    ) -> Result<Box<dyn DbConnection<Connection, &'static (dyn ToSql + Sync)>>> {
        self.pool.connect().await.context(DbConnectionSnafu)
    }

    fn sqlite_conn<'a>(
        db_connection: &'a mut Box<dyn DbConnection<Connection, &'static (dyn ToSql + Sync)>>,
    ) -> Result<&'a mut SqliteConnection> {
        db_connection
            .as_any_mut()
            .downcast_mut::<SqliteConnection>()
            .ok_or_else(|| UnableToDowncastDbConnectionSnafu {}.build())
    }

    async fn table_exists(&self, sqlite_conn: &mut SqliteConnection) -> bool {
        let sql = format!(
            r#"SELECT EXISTS (
          SELECT 1
          FROM sqlite_master
          WHERE type='table'
          AND name = '{name}'
        )"#,
            name = self.table_name
        );
        tracing::trace!("{sql}");

        sqlite_conn
            .conn
            .call(move |conn| {
                let mut stmt = conn.prepare(&sql)?;
                let exists = stmt.query_row([], |row| row.get(0))?;
                Ok(exists)
            })
            .await
            .unwrap_or(false)
    }

    fn insert_batch(
        &self,
        transaction: &Transaction<'_>,
        batch: RecordBatch,
    ) -> rusqlite::Result<()> {
        let insert_table_builder = InsertBuilder::new(&self.table_name, vec![batch]);
        let sql = insert_table_builder
            .build_sqlite()
            .map_err(|e| rusqlite::Error::ToSqlConversionFailure(e.into()))?;

        transaction.execute(&sql, [])?;

        Ok(())
    }

    fn delete_all_table_data(&self, transaction: &Transaction<'_>) -> rusqlite::Result<()> {
        transaction.execute(format!(r#"DELETE FROM "{}""#, self.table_name).as_str(), [])?;

        Ok(())
    }

    fn delete_from(
        &self,
        transaction: &Transaction<'_>,
        where_clause: &str,
    ) -> rusqlite::Result<u64> {
        transaction.execute(
            format!(
                r#"DELETE FROM "{}" WHERE {}"#,
                self.table_name, where_clause
            )
            .as_str(),
            [],
        )?;
        let count: u64 = transaction.query_row("SELECT changes()", [], |row| row.get(0))?;

        Ok(count)
    }

    fn create_table(&self, transaction: &Transaction<'_>) -> rusqlite::Result<()> {
        let create_table_statement =
            CreateTableBuilder::new(Arc::clone(&self.schema), &self.table_name);
        let sql = create_table_statement.build_sqlite();

        transaction.execute(&sql, [])?;

        Ok(())
    }
}
