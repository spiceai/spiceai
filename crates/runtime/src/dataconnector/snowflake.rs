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

use super::DataConnector;
use super::DataConnectorFactory;
use async_trait::async_trait;
use data_components::snowflake::SnowflakeTableFactory;
use data_components::Read;

use datafusion::datasource::TableProvider;
use db_connection_pool::snowflakepool::SnowflakeConnectionPool;
use db_connection_pool::DbConnectionPool;
use secrets::Secret;
use snafu::prelude::*;
use snowflake_api::SnowflakeApi;
use spicepod::component::dataset::Dataset;
use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::{collections::HashMap, future::Future};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("{source}"))]
    UnableToCreateSnowflakeConnectionPool { source: db_connection_pool::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub struct Snowflake {
    table_factory: SnowflakeTableFactory,
}

impl DataConnectorFactory for Snowflake {
    fn create(
        secret: Option<Secret>,
        params: Arc<Option<HashMap<String, String>>>,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            let pool: Arc<
                dyn DbConnectionPool<Arc<SnowflakeApi>, &'static (dyn Sync)> + Send + Sync,
            > = Arc::new(
                SnowflakeConnectionPool::new(&params, &secret)
                    .await
                    .context(UnableToCreateSnowflakeConnectionPoolSnafu)?,
            );

            let table_factory = SnowflakeTableFactory::new(pool);

            Ok(Arc::new(Self { table_factory }) as Arc<dyn DataConnector>)
        })
    }
}

#[async_trait]
impl DataConnector for Snowflake {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(
        &self,
        dataset: &Dataset,
    ) -> super::DataConnectorResult<Arc<dyn TableProvider>> {
        Ok(
            Read::table_provider(&self.table_factory, dataset.path().into())
                .await
                .context(super::UnableToGetReadProviderSnafu {
                    dataconnector: "snowflake",
                })?,
        )
    }
}
