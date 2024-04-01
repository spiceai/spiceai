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

use std::{collections::HashMap, sync::Arc};

use datafusion::{execution::context::SessionContext, sql::TableReference};
use db_connection_pool::{mysqlpool::MySQLConnectionPool, DbConnectionPool};
use futures::lock::Mutex;
use mysql_common::value::convert::ToValue;
use secrets::Secret;
use snafu::{prelude::*, ResultExt};
use spicepod::component::dataset::Dataset;
use sql_provider_datafusion::SqlTable;

use crate::{
    datapublisher::{AddDataResult, DataPublisher},
    dataupdate::DataUpdate,
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
    TransactionError {
        source: bb8_postgres::tokio_postgres::Error,
    },

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

    #[snafu(display("Unable to downcast DbConnection to PostgresConnection"))]
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
        unimplemented!()
    }

    fn name(&self) -> &str {
        "MySQL"
    }
}
