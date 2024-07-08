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

use crate::component::dataset::Dataset;
use crate::secrets::{Secret, SecretMap};
use async_trait::async_trait;
use data_components::Read;
use datafusion::datasource::TableProvider;
use datafusion_table_providers::mysql::MySQLTableFactory;
use datafusion_table_providers::sql::db_connection_pool::mysqlpool::MySQLConnectionPool;
use datafusion_table_providers::sql::db_connection_pool::{
    DbConnectionPool, Error as DbConnectionPoolError,
};
use mysql_async::prelude::ToValue;
use snafu::prelude::*;
use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::{collections::HashMap, future::Future};

use super::{DataConnector, DataConnectorFactory};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to create MySQL connection pool: {source}"))]
    UnableToCreateMySQLConnectionPool { source: DbConnectionPoolError },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub struct MySQL {
    mysql_factory: MySQLTableFactory,
}

impl DataConnectorFactory for MySQL {
    fn create(
        secret: Option<Secret>,
        params: Arc<HashMap<String, String>>,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        let mut params: SecretMap = params.as_ref().into();
        if let Some(secret) = secret {
            secret.insert_to_params(
                &mut params,
                "mysql_connection_string_key",
                "mysql_connection_string",
            );
            secret.insert_to_params(&mut params, "mysql_pass_key", "mysql_pass");
            secret.insert_to_params(&mut params, "mysql_user_key", "mysql_user");
        }

        Box::pin(async move {
            let pool: Arc<
                dyn DbConnectionPool<mysql_async::Conn, &'static (dyn ToValue + Sync)>
                    + Send
                    + Sync,
            > = Arc::new(
                MySQLConnectionPool::new(Arc::new(params.into_map()))
                    .await
                    .context(UnableToCreateMySQLConnectionPoolSnafu)?,
            );

            let mysql_factory = MySQLTableFactory::new(pool);

            Ok(Arc::new(Self { mysql_factory }) as Arc<dyn DataConnector>)
        })
    }
}

#[async_trait]
impl DataConnector for MySQL {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(
        &self,
        dataset: &Dataset,
    ) -> super::DataConnectorResult<Arc<dyn TableProvider>> {
        Ok(
            Read::table_provider(&self.mysql_factory, dataset.path().into())
                .await
                .context(super::UnableToGetReadProviderSnafu {
                    dataconnector: "mysql",
                })?,
        )
    }
}
