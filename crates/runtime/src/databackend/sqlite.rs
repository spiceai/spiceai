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

use std::{collections::HashMap, mem, sync::Arc};

use arrow::record_batch::RecordBatch;
use arrow_sql_gen::statement::{CreateTableBuilder, InsertBuilder};
use datafusion::{execution::context::SessionContext, sql::TableReference};
use db_connection_pool::{
    dbconnection::sqliteconn::SqliteConnection, sqlitepool::SqliteConnectionPool, DbConnectionPool,
    Mode,
};
use rusqlite::{ToSql, Transaction};
use snafu::{prelude::*, ResultExt};
use spicepod::component::dataset::Dataset;
use sql_provider_datafusion::SqlTable;
use tokio_rusqlite::Connection;

use crate::{
    datapublisher::{AddDataResult, DataPublisher},
    dataupdate::{DataUpdate, UpdateType},
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("DbConnectionError: {source}"))]
    DbConnectionError {
        source: db_connection_pool::dbconnection::GenericError,
    },

    #[snafu(display("DbConnectionPoolError: {source}"))]
    DbConnectionPoolError { source: db_connection_pool::Error },

    #[snafu(display("Failed to update sqlite table: {source}"))]
    UpdateError { source: tokio_rusqlite::Error },

    #[snafu(display("Unsupported data type"))]
    UnsupportedDataType {},

    #[snafu(display("SqliteDataFusionError: {source}"))]
    SqliteDataFusion {
        source: sql_provider_datafusion::Error,
    },

    #[snafu(display("DataFusionError: {source}"))]
    DataFusion {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("Unable to downcast DbConnection to SqliteConnection"))]
    UnableToDowncastDbConnection {},
}

type Result<T, E = Error> = std::result::Result<T, E>;

#[allow(clippy::module_name_repetitions)]
pub struct SqliteBackend {
    ctx: Arc<SessionContext>,
    name: String,
    pool: Arc<dyn DbConnectionPool<Connection, &'static (dyn ToSql + Sync)> + Send + Sync>,
    _primary_keys: Option<Vec<String>>,
}

impl DataPublisher for SqliteBackend {
    fn add_data(&self, _dataset: Arc<Dataset>, data_update: DataUpdate) -> AddDataResult {
        let pool = Arc::clone(&self.pool);
        let name = self.name.clone();

        Box::pin(async move {
            if let Some(batch) = data_update.data.first() {
                for field in batch.schema().fields() {
                    if field.data_type().is_nested() {
                        let field_name = self.name.clone() + "." + field.name();
                        tracing::error!("Unable to append {field_name}: nested types are not currently supported for local acceleration by sqlite");
                        return Err(
                            Box::new(Error::UnsupportedDataType {}) as Box<dyn std::error::Error>
                        );
                    }
                }
            }

            let sqlite_update = SqliteUpdate {
                name,
                data: data_update.data,
                update_type: data_update.update_type,
                pool,
            };

            sqlite_update.update().await?;

            self.initialize_datafusion().await?;
            Ok(())
        })
    }

    fn name(&self) -> &str {
        "Sqlite"
    }
}

impl SqliteBackend {
    #[allow(clippy::needless_pass_by_value)]
    pub async fn new(
        ctx: Arc<SessionContext>,
        name: &str,
        params: Arc<Option<HashMap<String, String>>>,
        mode: Mode,
        primary_keys: Option<Vec<String>>,
    ) -> Result<Self> {
        let pool = SqliteConnectionPool::new(name, mode, params)
            .await
            .context(DbConnectionPoolSnafu)?;
        Ok(SqliteBackend {
            ctx,
            name: name.to_string(),
            pool: Arc::new(pool),
            _primary_keys: primary_keys,
        })
    }

    async fn initialize_datafusion(&self) -> Result<()> {
        let table_exists = self
            .ctx
            .table_exist(TableReference::bare(self.name.clone()))
            .context(DataFusionSnafu)?;
        if table_exists {
            return Ok(());
        }

        let table = match SqlTable::new(&self.pool, TableReference::bare(self.name.clone()))
            .await
            .context(SqliteDataFusionSnafu)
        {
            Ok(table) => table,
            Err(e) => return Err(e),
        };

        self.ctx
            .register_table(&self.name, Arc::new(table))
            .context(DataFusionSnafu)?;

        Ok(())
    }
}

struct SqliteUpdate {
    name: String,
    data: Vec<RecordBatch>,
    update_type: UpdateType,
    pool: Arc<dyn DbConnectionPool<Connection, &'static (dyn ToSql + Sync)> + Send + Sync>,
}

impl SqliteUpdate {
    async fn update(mut self) -> Result<()> {
        let mut transaction_conn = self.pool.connect().await.context(DbConnectionPoolSnafu)?;
        let Some(transaction_conn) = transaction_conn
            .as_any_mut()
            .downcast_mut::<SqliteConnection>()
        else {
            return UnableToDowncastDbConnectionSnafu {}.fail();
        };

        let conn = self.pool.connect().await.context(DbConnectionPoolSnafu)?;
        let Some(conn) = conn.as_any().downcast_ref::<SqliteConnection>() else {
            return UnableToDowncastDbConnectionSnafu {}.fail();
        };

        let table_exists = self.table_exists(conn).await;

        transaction_conn
            .conn
            .call(move |conn| {
                let transaction = conn.transaction()?;

                if !table_exists {
                    self.create_table(&transaction)?;
                } else if self.update_type == UpdateType::Overwrite {
                    transaction.execute(format!(r#"DELETE FROM "{}""#, self.name).as_str(), [])?;
                };

                let data = mem::take(&mut self.data);
                for batch in data {
                    self.insert_batch(&transaction, batch)?;
                }

                transaction.commit()?;

                tracing::trace!("Processed update to Sqlite table {name}", name = self.name,);

                Ok(())
            })
            .await
            .context(UpdateSnafu)
    }

    fn insert_batch(
        &mut self,
        transaction: &Transaction<'_>,
        batch: RecordBatch,
    ) -> tokio_rusqlite::Result<()> {
        let insert_table_builder = InsertBuilder::new(&self.name, vec![batch]);
        let sql = insert_table_builder.build_sqlite();

        transaction.execute(&sql, [])?;

        Ok(())
    }

    fn create_table(&mut self, transaction: &Transaction<'_>) -> tokio_rusqlite::Result<()> {
        let Some(batch) = self.data.pop() else {
            return Ok(());
        };

        let create_table_statement = CreateTableBuilder::new(batch.schema(), &self.name);
        let sql = create_table_statement.build_sqlite();

        transaction.execute(&sql, [])?;

        self.insert_batch(transaction, batch)?;

        Ok(())
    }

    async fn table_exists(&mut self, sqlite_conn: &SqliteConnection) -> bool {
        let sql = format!(
            r#"SELECT EXISTS (
              SELECT 1
              FROM sqlite_master 
              WHERE type='table' 
              AND name = '{name}'
            )"#,
            name = self.name
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
}
