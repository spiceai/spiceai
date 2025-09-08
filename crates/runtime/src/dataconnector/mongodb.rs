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
use std::convert::Into;
use std::future::Future;
use std::pin::Pin;
use std::string::ToString;
use std::sync::Arc;

use super::{
    ConnectorComponent, ConnectorParams, DataConnector, DataConnectorError, DataConnectorFactory,
    ParameterSpec,
};

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

const DEFAULT_MIN_POOL_SIZE: usize = 0;
const DEFAULT_MIN_POOL_SIZE_STR: &str = "0";
const DEFAULT_MAX_POOL_SIZE: usize = 10;
const DEFAULT_MAX_POOL_SIZE_STR: &str = "10";

const PARAMETERS: &[ParameterSpec] = &[
    ParameterSpec::component("connection_string")
        .description("Full MongoDB connection URI in standard format (e.g., mongodb://user:pass@host:port/dbname). If provided, this overrides individual host, port, user, pass, and db parameters. See: https://www.mongodb.com/docs/manual/reference/connection-string/#connection-string-formats")
        .secret(),
    ParameterSpec::component("user")
        .description("Username for MongoDB authentication. Must be used together with 'pass' unless 'connection_string' is provided.")
        .secret(),
    ParameterSpec::component("pass")
        .description("Password for MongoDB authentication. Must be used together with 'user' unless 'connection_string' is provided.")
        .secret(),
    ParameterSpec::component("host")
        .description("Hostname or IP address of the MongoDB server. Defaults to 'localhost' if not specified."),
    ParameterSpec::component("port")
        .description("Port number the MongoDB server is listening on. Defaults to '27017'."),
    ParameterSpec::component("db")
        .description("Database name to connect to. Defaults to 'default' if not specified."),
    ParameterSpec::component("sslmode")
        .description("TLS/SSL mode for the connection. Supported values: 'disabled', 'required', 'preferred'. Defaults to 'required'. 'preferred' allows invalid certificates/hostnames.")
        .one_of(&["disabled", "required", "preferred"]),
    ParameterSpec::component("sslrootcert")
        .description("Path to a CA root certificate file to use for TLS verification. Optional; if not provided, system defaults are used."),
    ParameterSpec::component("auth_source")
        .description("Authentication source database. Overrides the default auth source in the connection string."),
    ParameterSpec::component("direct_connection")
        .description("Whether to connect directly to a single MongoDB host instead of discovering the topology. Accepts 'true' or 'false'.")
        .is_boolean(),
    ParameterSpec::component("time_zone")
        .description("Time zone to use for interpreting and returning timestamp values (e.g., 'UTC', 'America/Los_Angeles')."),
    ParameterSpec::component("unnest_depth")
        .description("Maximum nesting depth for unnesting embedded documents into a flattened structure. Higher values expand deeper nested fields."),
    ParameterSpec::component("num_docs_to_infer_schema")
        .description("Number of documents to use to infer the schema. Defaults to 400."),
    ParameterSpec::component("pool_min")
        .description("Minimum number of connections to keep open in the pool, created lazily when first needed. Defaults to 10.")
        .default(DEFAULT_MIN_POOL_SIZE_STR),
    ParameterSpec::component("pool_max")
        .description("Maximum number of connections allowed in the pool. Defaults to 100.")
        .default(DEFAULT_MAX_POOL_SIZE_STR),
];

const IGNORED_IF_URI: &[&str] = &[
    "host",
    "port",
    "db",
    "user",
    "pass",
    "auth_source",
    "direct_connection",
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
            // If a full connection_string is provided, warn about ignored connection details.
            if params.parameters.get("connection_string").ok().is_some() {
                let ignored: Vec<&str> = IGNORED_IF_URI
                    .iter()
                    .copied()
                    .filter(|k| params.parameters.get(k).ok().is_some())
                    .collect();

                if !ignored.is_empty() {
                    tracing::warn!(
                        "Both 'connection_string' and individual connection parameters ({parameters}) were provided for the {component}. The 'connection_string' will be used and the listed parameters will be ignored.",
                        parameters = ignored.join(", "),
                        component = params.component
                    );
                }
            }

            let mut pool_min = params
                .parameters
                .get("pool_min")
                .ok()
                .and_then(|s| {
                    let pool_min_str = s.expose_secret();
                    let parsed_pool_min = pool_min_str.parse::<usize>();
                    if parsed_pool_min.is_err() {
                        tracing::warn!(
                            "Invalid pool_min value: {pool_min_str}, using default of {DEFAULT_MIN_POOL_SIZE_STR}"
                        );
                    }
                    parsed_pool_min.ok()
                })
                .unwrap_or(DEFAULT_MIN_POOL_SIZE);
            let mut pool_max = params
                .parameters
                .get("pool_max")
                .ok()
                .and_then(|s| {
                    let pool_max_str = s.expose_secret();
                    let parsed_pool_max = pool_max_str.parse::<usize>();
                    if parsed_pool_max.is_err() {
                        tracing::warn!(
                            "Invalid pool_max value: {pool_max_str}, using default of {DEFAULT_MAX_POOL_SIZE_STR}"
                        );
                    }
                    parsed_pool_max.ok()
                })
                .unwrap_or(DEFAULT_MAX_POOL_SIZE);

            if pool_min > pool_max {
                tracing::warn!(
                    "pool_min value: {pool_min} is greater than pool_max value: {pool_max}, using default values of {DEFAULT_MIN_POOL_SIZE_STR} and {DEFAULT_MAX_POOL_SIZE_STR}"
                );
                pool_min = DEFAULT_MIN_POOL_SIZE;
                pool_max = DEFAULT_MAX_POOL_SIZE;

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
        Read::table_provider(
            &self.mongodb_factory,
            dataset.path().into(),
            dataset.schema(),
        )
        .await
        .context(super::UnableToGetReadProviderSnafu {
            dataconnector: "mongodb",
            connector_component: ConnectorComponent::from(dataset),
        })
    }
}
