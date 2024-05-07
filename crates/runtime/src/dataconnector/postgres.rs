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
use data_components::postgres::PostgresTableFactory;
use data_components::Read;
use datafusion::datasource::TableProvider;
use db_connection_pool::postgrespool::{self, PostgresConnectionPool};
use secrets::Secret;
use snafu::prelude::*;
use spicepod::component::dataset::Dataset;
use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::{collections::HashMap, future::Future};

use super::{DataConnector, DataConnectorError, DataConnectorFactory};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to create Postgres connection pool: {source}"))]
    UnableToCreatePostgresConnectionPool { source: db_connection_pool::Error },

    #[snafu(display("{source}"))]
    UnableToGetReadProvider {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("{source}"))]
    UnableToGetReadWriteProvider {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Cannot connect to PostgreSQL data connector. Authentication failed. Ensure that the username and password are correctly configured in the spicepod."))]
    UnableToConnectInvalidAuth {},

    #[snafu(display("Cannot connect to PostgreSQL data connector on {host}:{port}. Ensure that the host and port are correclty configured in the spicepod, and that the host is reachable."))]
    UnableToConnectInvalidHostOrPort { host: String, port: u16 },
}

pub struct Postgres {
    postgres_factory: PostgresTableFactory,
}

impl DataConnectorFactory for Postgres {
    fn create(
        secret: Option<Secret>,
        params: Arc<Option<HashMap<String, String>>>,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            match PostgresConnectionPool::new(params, secret).await {
                Ok(pool) => {
                    let postgres_factory = PostgresTableFactory::new(Arc::new(pool));
                    Ok(Arc::new(Self { postgres_factory }) as Arc<dyn DataConnector>)
                }
                Err(e) => match e {
                    postgrespool::Error::InvalidUsernameOrPassword { .. } => {
                        Err(DataConnectorError::UnableToConnectInvalidAuth {
                            source: Box::new(Error::UnableToConnectInvalidAuth {}),
                        }
                        .into())
                    }

                    postgrespool::Error::InvalidHostOrPortError {
                        host,
                        port,
                        source: _,
                    } => Err(DataConnectorError::UnableToConnectInvalidConfiguration {
                        source: Box::new(Error::UnableToConnectInvalidHostOrPort { host, port }),
                    }
                    .into()),

                    _ => Err(DataConnectorError::UnableToConnectInternal {
                        dataconnector: "postgres".to_string(),
                        source: Box::new(e),
                    }
                    .into()),
                },
            }
        })
    }
}

#[async_trait]
impl DataConnector for Postgres {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(
        &self,
        dataset: &Dataset,
    ) -> super::AnyErrorResult<Arc<dyn TableProvider>> {
        Ok(
            Read::table_provider(&self.postgres_factory, dataset.path().into())
                .await
                .context(UnableToGetReadProviderSnafu)?,
        )
    }
}
