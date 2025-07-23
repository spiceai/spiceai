/*
Copyright 2024-2025 The Spice.ai OSS Authors

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
use datafusion_table_providers::mongodb::{
    Error as MongoDBError, MongoDBTableFactory, connection_pool::MongoDBConnectionPool,
};
use secrecy::ExposeSecret;
use snafu::prelude::*;
use std::any::Any;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use super::{
    ConnectorComponent, ConnectorParams, DataConnector, DataConnectorError, DataConnectorFactory,
    ParameterSpec,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to create MongoDB connection pool: {source}"))]
    UnableToCreateMongoDBSQLConnectionPool { source: MongoDBError },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub struct MongoDB {
    mongodb_factory: MongoDBTableFactory,
}

impl std::fmt::Debug for MongoDB {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MongoDB").finish_non_exhaustive()
    }
}

#[derive(Default, Copy, Clone)]
pub struct MongoDBFactory {}

impl MongoDBFactory {
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
    ParameterSpec::component("connection_string").secret(),
    ParameterSpec::component("user").secret(),
    ParameterSpec::component("pass").secret(),
    ParameterSpec::component("host"),
    ParameterSpec::component("port"),
    ParameterSpec::component("db"),
    ParameterSpec::component("sslmode"),
    ParameterSpec::component("sslrootcert"),
    ParameterSpec::component("auth_source"),
    ParameterSpec::component("pool_min")
        .description("The minimum number of connections to keep open in the pool, lazily created when requested.")
        .default("10"),
    ParameterSpec::component("pool_max")
        .description("The maximum number of connections to allow in the pool.")
        .default("100"),
];

impl DataConnectorFactory for MongoDBFactory {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create(
        &self,
        mut params: ConnectorParams,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            let mut pool_min = params
                .parameters
                .get("pool_min")
                .ok()
                .and_then(|s| {
                    let pool_min_str = s.expose_secret();
                    let parsed_pool_min = pool_min_str.parse::<usize>();
                    if parsed_pool_min.is_err() {
                        tracing::warn!(
                            "Invalid pool_min value: {pool_min_str}, using default of 10"
                        );
                    }
                    parsed_pool_min.ok()
                })
                .unwrap_or(10);
            let mut pool_max = params
                .parameters
                .get("pool_max")
                .ok()
                .and_then(|s| {
                    let pool_max_str = s.expose_secret();
                    let parsed_pool_max = pool_max_str.parse::<usize>();
                    if parsed_pool_max.is_err() {
                        tracing::warn!(
                            "Invalid pool_max value: {pool_max_str}, using default of 100"
                        );
                    }
                    parsed_pool_max.ok()
                })
                .unwrap_or(100);

            if pool_min > pool_max {
                tracing::warn!(
                    "pool_min value: {pool_min} is greater than pool_max value: {pool_max}, using default values of 10 and 100"
                );
                pool_min = 10;
                pool_max = 100;

                params
                    .parameters
                    .insert("pool_min".to_string(), pool_min.to_string().into());
                params
                    .parameters
                    .insert("pool_max".to_string(), pool_max.to_string().into());
            }

            let pool = match MongoDBConnectionPool::new(params.parameters.to_secret_map()).await {
                Ok(pool) => Arc::new(pool),
                Err(error) => match error {
                    MongoDBError::InvalidUsernameOrPassword => {
                        return Err(
                            DataConnectorError::UnableToConnectInvalidUsernameOrPassword {
                                dataconnector: "mongodb".to_string(),
                                connector_component: params.component.clone(),
                            }
                            .into(),
                        );
                    }

                    _ => {
                        return Err(DataConnectorError::UnableToConnectInternal {
                            dataconnector: "mongodb".to_string(),
                            connector_component: params.component.clone(),
                            source: Box::new(error),
                        }
                        .into());
                    }
                },
            };

            let mongodb_factory = MongoDBTableFactory::new(pool);

            Ok(Arc::new(MongoDB { mongodb_factory }) as Arc<dyn DataConnector>)
        })
    }

    fn prefix(&self) -> &'static str {
        "mongodb"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        PARAMETERS
    }
}

#[async_trait]
impl DataConnector for MongoDB {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(
        &self,
        dataset: &Dataset,
    ) -> super::DataConnectorResult<Arc<dyn TableProvider>> {
        Ok(Read::table_provider(
            &self.mongodb_factory,
            dataset.path().into(),
            dataset.schema(),
        )
        .await
        .context(super::UnableToGetReadProviderSnafu {
            dataconnector: "mongodb",
            connector_component: ConnectorComponent::from(dataset),
        })?)
    }
}
