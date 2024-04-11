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

use async_trait::async_trait;
use data_components::mysql::MySQLTableFactory;
use data_components::Read;
use datafusion::datasource::TableProvider;
use db_connection_pool::mysqlpool::MySQLConnectionPool;
use db_connection_pool::DbConnectionPool;
use mysql_async::prelude::ToValue;
use secrets::Secret;
use snafu::prelude::*;
use spicepod::component::dataset::Dataset;
use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::{collections::HashMap, future::Future};

use super::{DataConnector, DataConnectorFactory};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to create MySQL connection pool: {source}"))]
    UnableToCreateMySQLConnectionPool { source: db_connection_pool::Error },

    #[snafu(display("{source}"))]
    UnableToGetReadProvider {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub struct MySQL {
    mysql_factory: MySQLTableFactory,
}

impl DataConnectorFactory for MySQL {
    fn create(
        secret: Option<Secret>,
        params: Arc<Option<HashMap<String, String>>>,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            let pool: Arc<
                dyn DbConnectionPool<mysql_async::Conn, &'static (dyn ToValue + Sync)>
                    + Send
                    + Sync,
            > = Arc::new(
                MySQLConnectionPool::new(params, secret)
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
    ) -> super::AnyErrorResult<Arc<dyn TableProvider>> {
        Ok(
            Read::table_provider(&self.mysql_factory, dataset.path().into())
                .await
                .context(UnableToGetReadProviderSnafu)?,
        )
    }
}
