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
use data_components::Read;
use datafusion::datasource::TableProvider;
use datafusion_table_providers::postgres::PostgresTableFactory;
use datafusion_table_providers::sql::db_connection_pool::dbconnection;
use datafusion_table_providers::sql::db_connection_pool::{
    postgrespool::{self, PostgresConnectionPool},
    Error as DbConnectionPoolError,
};
use snafu::prelude::*;
use std::any::Any;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use super::{
    ConnectorComponent, DataConnector, DataConnectorError, DataConnectorFactory,
    DataConnectorParams, ParameterSpec,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to create Postgres connection pool: {source}"))]
    UnableToCreatePostgresConnectionPool { source: DbConnectionPoolError },
}

pub struct Postgres {
    postgres_factory: PostgresTableFactory,
}

#[derive(Default, Copy, Clone)]
pub struct PostgresFactory {}

impl PostgresFactory {
    #[must_use]
    pub fn new() -> Self {
        Self {}
    }

    #[must_use]
    pub fn new_arc() -> Arc<dyn DataConnectorFactory> {
        Arc::new(Self {}) as Arc<dyn DataConnectorFactory>
    }
}

const PARAMETERS: &[ParameterSpec] = &[
    ParameterSpec::connector("connection_string").secret(),
    ParameterSpec::connector("user").secret(),
    ParameterSpec::connector("pass").secret(),
    ParameterSpec::connector("host"),
    ParameterSpec::connector("port"),
    ParameterSpec::connector("db"),
    ParameterSpec::connector("sslmode"),
    ParameterSpec::connector("sslrootcert"),
    ParameterSpec::runtime("connection_pool_size")
        .description("The maximum number of connections created in the connection pool")
        .default("10"),
];

impl DataConnectorFactory for PostgresFactory {
    fn create(
        &self,
        params: DataConnectorParams,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            match PostgresConnectionPool::new(params.parameters.to_secret_map()).await {
                Ok(pool) => {
                    let postgres_factory = PostgresTableFactory::new(Arc::new(pool));
                    Ok(Arc::new(Postgres { postgres_factory }) as Arc<dyn DataConnector>)
                }
                Err(e) => match e {
                    postgrespool::Error::InvalidUsernameOrPassword { .. } => Err(
                        DataConnectorError::UnableToConnectInvalidUsernameOrPassword {
                            dataconnector: "postgres".to_string(),
                            connector_component: params.component.clone(),
                        }
                        .into(),
                    ),

                    postgrespool::Error::InvalidHostOrPortError {
                        host,
                        port,
                        source: _,
                    } => Err(DataConnectorError::UnableToConnectInvalidHostOrPort {
                        dataconnector: "postgres".to_string(),
                        connector_component: params.component.clone(),
                        host,
                        port: format!("{port}"),
                    }
                    .into()),

                    _ => Err(DataConnectorError::UnableToConnectInternal {
                        dataconnector: "postgres".to_string(),
                        connector_component: params.component.clone(),
                        source: Box::new(e),
                    }
                    .into()),
                },
            }
        })
    }

    fn prefix(&self) -> &'static str {
        "pg"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        PARAMETERS
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
        match Read::table_provider(
            &self.postgres_factory,
            dataset.path().into(),
            dataset.schema(),
        )
        .await
        {
            Ok(provider) => Ok(provider),
            Err(e) => {
                if let Some(err_source) = e.source() {
                    if let Some(dbconnection::Error::UndefinedTable {
                        table_name,
                        source: _,
                    }) = err_source.downcast_ref::<dbconnection::Error>()
                    {
                        return Err(DataConnectorError::InvalidTableName {
                            dataconnector: "postgres".to_string(),
                            connector_component: ConnectorComponent::from(dataset),
                            table_name: table_name.clone(),
                        });
                    }
                }

                return Err(DataConnectorError::UnableToGetReadProvider {
                    dataconnector: "postgres".to_string(),
                    connector_component: ConnectorComponent::from(dataset),
                    source: e,
                });
            }
        }
    }
}
