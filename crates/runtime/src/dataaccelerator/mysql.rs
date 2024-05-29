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

use crate::component::dataset::Dataset;
use arrow::array::RecordBatch;
use arrow_sql_gen::statement::{CreateTableBuilder, InsertBuilder};
use datafusion::{execution::context::SessionContext, sql::TableReference};
use db_connection_pool::{
    dbconnection::mysqlconn::MySQLConnection, mysqlpool::MySQLConnectionPool, DbConnectionPool,
};
use futures::lock::Mutex;
use mysql_async::{
    prelude::{Queryable, ToValue},
    Params, Row, Transaction, TxOpts,
};
use secrets::Secret;
use snafu::{prelude::*, ResultExt};
use sql_provider_datafusion::SqlTable;
use tract_core::downcast_rs::Downcast;

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

    #[snafu(display("Error executing transaction: {source}"))]
    TransactionError { source: mysql_async::Error },

    #[snafu(display("MySQLDataFusionError: {source}"))]
    MySQLDataFusion {
        source: sql_provider_datafusion::Error,
    },

    #[snafu(display("DataFusionError: {source}"))]
    DataFusion {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("Lock is poisoned: {message}"))]
    LockPoisoned { message: String },

    #[snafu(display("Unable to downcast DbConnection to MySQLConnection"))]
    UnableToDowncastDbConnection {},
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub struct MySQLBackend {
    ctx: Arc<SessionContext>,
    name: String,
    pool: Arc<dyn DbConnectionPool<mysql_async::Conn, &'static (dyn ToValue + Sync)> + Sync + Send>,
    create_mutex: Mutex<()>,
    _primary_keys: Option<Vec<String>>,
}

impl MySQLBackend {
    #[allow(clippy::needless_pass_by_value)]
    pub async fn new(
        ctx: Arc<SessionContext>,
        name: &str,
        params: Arc<Option<HashMap<String, String>>>,
        primary_keys: Option<Vec<String>>,
        secret: Option<Secret>,
    ) -> Result<Self> {
        let pool = MySQLConnectionPool::new(params, secret)
            .await
            .context(DbConnectionPoolSnafu)?;
        Ok(MySQLBackend {
            ctx,
            pool: Arc::new(pool),
            name: name.to_string(),
            create_mutex: Mutex::new(()),
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
            .context(MySQLDataFusionSnafu)
        {
            Ok(table) => table,
            Err(e) => {
                return Err(e);
            }
        };

        self.ctx
            .register_table(&self.name, Arc::new(table))
            .context(DataFusionSnafu)?;

        Ok(())
    }
}

impl DataPublisher for MySQLBackend {
    fn add_data(&self, _dataset: Arc<Dataset>, data_update: DataUpdate) -> AddDataResult {
        let pool = Arc::clone(&self.pool);
        let name = self.name.clone();

        Box::pin(async move {
            let mut mysql_update = MySQLUpdate {
                name,
                data: data_update.data,
                update_type: data_update.update_type,
                pool,
                create_mutex: &self.create_mutex,
            };
            mysql_update.update().await?;

            self.initialize_datafusion().await?;
            Ok(())
        })
    }

    fn name(&self) -> &str {
        "MySQL"
    }
}

struct MySQLUpdate<'a> {
    name: String,
    data: Vec<RecordBatch>,
    update_type: UpdateType,
    pool: Arc<dyn DbConnectionPool<mysql_async::Conn, &'static (dyn ToValue + Sync)> + Sync + Send>,
    create_mutex: &'a Mutex<()>,
}

impl<'a> MySQLUpdate<'a> {
    async fn update(&mut self) -> Result<()> {
        let mut transaction_conn = self.pool.connect().await.context(DbConnectionPoolSnafu)?;
        let Some(transaction_conn) = transaction_conn
            .as_any_mut()
            .downcast_mut::<MySQLConnection>()
        else {
            return UnableToDowncastDbConnectionSnafu {}.fail();
        };

        let mut transaction_conn = transaction_conn.conn.lock().await;
        let transaction_conn = &mut *transaction_conn;

        let mut transaction = transaction_conn
            .start_transaction(TxOpts::default())
            .await
            .context(TransactionSnafu)?;

        let conn = self.pool.connect().await.context(DbConnectionPoolSnafu)?;
        let Some(conn) = conn.as_any().downcast_ref::<MySQLConnection>() else {
            return UnableToDowncastDbConnectionSnafu {}.fail();
        };

        if !self.table_exists(conn).await {
            self.create_table(&mut transaction).await?;
        } else if self.update_type == UpdateType::Overwrite {
            transaction
                .exec_drop(
                    format!(r#"DELETE FROM "{}""#, self.name).as_str(),
                    Params::Empty,
                )
                .await
                .context(TransactionSnafu)?;
        };

        let data = mem::take(&mut self.data);
        for batch in data {
            self.insert_batch(&mut transaction, batch).await?;
        }

        transaction.commit().await.context(TransactionSnafu)?;

        tracing::trace!("Processed update to MySQL table {name}", name = self.name,);

        Ok(())
    }

    async fn insert_batch(
        &mut self,
        transaction: &mut Transaction<'_>,
        batch: RecordBatch,
    ) -> Result<()> {
        let insert_table_builder = InsertBuilder::new(&self.name, vec![batch]);
        let sql = insert_table_builder.build_mysql();

        transaction
            .exec_drop(&sql, Params::Empty)
            .await
            .context(TransactionSnafu)?;

        Ok(())
    }

    async fn create_table(&mut self, transaction: &mut Transaction<'_>) -> Result<()> {
        let _lock = self.create_mutex.lock();

        let Some(batch) = self.data.pop() else {
            return Ok(());
        };

        let create_table_statement = CreateTableBuilder::new(batch.schema(), &self.name);
        let sql = create_table_statement.build_mysql();

        transaction
            .exec_drop(sql, Params::Empty)
            .await
            .context(TransactionSnafu)?;

        self.insert_batch(transaction, batch).await?;

        Ok(())
    }

    async fn table_exists(&mut self, mysql_conn: &MySQLConnection) -> bool {
        let sql = format!(
            r#"SELECT EXISTS (
              SELECT 1
              FROM information_schema.tables
              WHERE table_name = '{name}'
            )"#,
            name = self.name
        );
        tracing::trace!("{sql}");
        let mut conn = mysql_conn.conn.lock().await;
        let conn = &mut *conn;

        let Ok(row) = conn.exec(&sql, Params::Empty).await else {
            return false;
        };
        let row: Vec<Row> = row;

        row.first().is_some()
    }
}
