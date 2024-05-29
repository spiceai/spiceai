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
use async_trait::async_trait;
use data_components::clickhouse::ClickhouseTableFactory;
use data_components::Read;
use datafusion::datasource::TableProvider;
use db_connection_pool::clickhousepool::{self, ClickhouseConnectionPool};
use secrets::Secret;
use snafu::prelude::*;
use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::{collections::HashMap, future::Future};

use super::{DataConnector, DataConnectorError, DataConnectorFactory};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to create Clickhouse connection pool: {source}"))]
    UnableToCreateClickhouseConnectionPool { source: db_connection_pool::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub struct Clickhouse {
    clickhouse_factory: ClickhouseTableFactory,
}

impl DataConnectorFactory for Clickhouse {
    fn create(
        secret: Option<Secret>,
        params: Arc<HashMap<String, String>>,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            match ClickhouseConnectionPool::new(params, secret).await {
                Ok(pool) => {
                    let clickhouse_factory = ClickhouseTableFactory::new(Arc::new(pool));
                    Ok(Arc::new(Self { clickhouse_factory }) as Arc<dyn DataConnector>)
                }

                Err(e) => match e {
                    clickhousepool::Error::InvalidUsernameOrPasswordError { .. } => Err(
                        DataConnectorError::UnableToConnectInvalidUsernameOrPassword {
                            dataconnector: "clickhouse".to_string(),
                        }
                        .into(),
                    ),
                    clickhousepool::Error::InvalidHostOrPortError {
                        host,
                        port,
                        source: _,
                    } => Err(DataConnectorError::UnableToConnectInvalidHostOrPort {
                        dataconnector: "clickhouse".to_string(),
                        host,
                        port,
                    }
                    .into()),
                    clickhousepool::Error::ConnectionTlsError { source: _ } => {
                        Err(DataConnectorError::UnableToConnectTlsError {
                            dataconnector: "clickhouse".to_string(),
                        }
                        .into())
                    }
                    _ => Err(DataConnectorError::UnableToConnectInternal {
                        dataconnector: "clickhouse".to_string(),
                        source: Box::new(e),
                    }
                    .into()),
                },
            }
        })
    }
}

#[async_trait]
impl DataConnector for Clickhouse {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(
        &self,
        dataset: &Dataset,
    ) -> super::DataConnectorResult<Arc<dyn TableProvider>> {
        Ok(
            Read::table_provider(&self.clickhouse_factory, dataset.path().into())
                .await
                .context(super::UnableToGetReadProviderSnafu {
                    dataconnector: "clickhouse",
                })?,
        )
    }
}
