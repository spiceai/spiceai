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
                    postgrespool::Error::InvalidUsernameOrPassword { .. } => Err(
                        DataConnectorError::UnableToConnectInvalidUsernameOrPassword {
                            dataconnector: "postgres".to_string(),
                        }
                        .into(),
                    ),

                    postgrespool::Error::InvalidHostOrPortError {
                        host,
                        port,
                        source: _,
                    } => Err(DataConnectorError::UnableToConnectInvalidHostOrPort {
                        dataconnector: "postgres".to_string(),
                        host,
                        port: format!("{port}"),
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
    ) -> super::DataConnectorResult<Arc<dyn TableProvider>> {
        match Read::table_provider(&self.postgres_factory, dataset.path().into()).await {
            Ok(provider) => Ok(provider),
            Err(e) => {
                if let Some(err_source) = e.source() {
                    if let Some(db_connection_pool::dbconnection::Error::UndefinedTable {
                        table_name,
                        source: _,
                    }) = err_source.downcast_ref::<db_connection_pool::dbconnection::Error>()
                    {
                        return Err(DataConnectorError::InvalidTableName {
                            dataconnector: "postgres".to_string(),
                            dataset_name: dataset.name.to_string(),
                            table_name: table_name.clone(),
                        });
                    }
                }

                return Err(DataConnectorError::UnableToGetReadProvider {
                    dataconnector: "postgres".to_string(),
                    source: e,
                });
            }
        }
    }
}
