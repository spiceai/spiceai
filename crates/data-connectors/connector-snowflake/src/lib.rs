/*
Copyright 2026 The Spice.ai OSS Authors

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

//! `Snowflake` data connector for Spice.ai runtime.
//!
//! This crate provides the `Snowflake` connector implementation, allowing
//! Spice.ai to connect to Snowflake data warehouses as data sources.
//!
//! This connector is extracted from the runtime crate to enable faster
//! incremental builds - changes to this connector only require rebuilding
//! this crate, not the entire runtime.

use async_trait::async_trait;
use data_components::Read;
use data_components::snowflake::SnowflakeTableFactory;
use datafusion::datasource::TableProvider;
use datafusion_table_providers::sql::db_connection_pool::DbConnectionPool;
use db_connection_pool::snowflakepool::SnowflakeConnectionPool;
use itertools::Itertools;
use runtime::component::dataset::Dataset;
use runtime::dataconnector::{
    ConnectorComponent, ConnectorParams, DataConnector, DataConnectorError, DataConnectorFactory,
    DataConnectorResult, NewDataConnectorResult,
};
use runtime::parameters::ParameterSpec;
use snafu::prelude::*;
use snowflake_api::SnowflakeApi;
use std::any::Any;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("{source}"))]
    UnableToCreateSnowflakeConnectionPool {
        source: db_connection_pool::snowflakepool::Error,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// `Snowflake` data connector.
#[derive(Debug)]
pub struct Snowflake {
    table_factory: SnowflakeTableFactory,
}

/// Factory for creating `Snowflake` connector instances.
#[derive(Default, Copy, Clone)]
pub struct SnowflakeFactory {}

impl SnowflakeFactory {
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
    ParameterSpec::component("username").secret(),
    ParameterSpec::component("password").secret(),
    ParameterSpec::component("private_key_path").secret(),
    ParameterSpec::component("private_key_passphrase").secret(),
    ParameterSpec::component("account").secret(),
    ParameterSpec::component("warehouse").secret(),
    ParameterSpec::component("role").secret(),
    ParameterSpec::component("auth_type"),
];

// https://github.com/apache/datafusion-sqlparser-rs/blob/87d190734c7b978e8252b110c9529d7a93a30cf0/src/keywords.rs#L1061
const RESERVED_KEYWORDS: &[&str] = &[
    "START",
    "CONNECT",
    "MATCH_RECOGNIZE",
    "SAMPLE",
    "TABLESAMPLE",
    "FROM",
];

impl DataConnectorFactory for SnowflakeFactory {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create(
        &self,
        params: ConnectorParams,
    ) -> Pin<Box<dyn Future<Output = NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            let pool: Arc<
                dyn DbConnectionPool<Arc<SnowflakeApi>, &'static dyn Sync> + Send + Sync,
            > = Arc::new(
                SnowflakeConnectionPool::new(&params.parameters.to_secret_map())
                    .await
                    .context(UnableToCreateSnowflakeConnectionPoolSnafu)?,
            );

            let table_factory = SnowflakeTableFactory::new(pool);

            Ok(Arc::new(Snowflake { table_factory }) as Arc<dyn DataConnector>)
        })
    }

    fn prefix(&self) -> &'static str {
        "snowflake"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        PARAMETERS
    }

    fn reserved_keywords(&self) -> &'static [&'static str] {
        RESERVED_KEYWORDS
    }
}

/// The name used to identify this connector in configuration.
pub const CONNECTOR_NAME: &str = "snowflake";

/// Returns a new instance of the `Snowflake` connector factory.
#[must_use]
pub fn factory() -> Arc<dyn DataConnectorFactory> {
    SnowflakeFactory::new_arc()
}

#[derive(Debug, Snafu)]
enum ReadProviderError {
    #[snafu(display("Unable to get read provider for {dataconnector}: {source}"))]
    UnableToGetReadProvider {
        dataconnector: &'static str,
        connector_component: ConnectorComponent,
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

impl From<ReadProviderError> for DataConnectorError {
    fn from(err: ReadProviderError) -> Self {
        match err {
            ReadProviderError::UnableToGetReadProvider {
                dataconnector,
                connector_component,
                source,
            } => DataConnectorError::UnableToGetReadProvider {
                dataconnector: dataconnector.to_string(),
                connector_component,
                source,
            },
        }
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
    ) -> DataConnectorResult<Arc<dyn TableProvider>> {
        let path = dataset
            .path()
            .split('.')
            .map(|x| {
                if x.starts_with('"') && x.ends_with('"') {
                    return x.into();
                }

                format!("\"{x}\"")
            })
            .join(".");

        Ok(Read::table_provider(&self.table_factory, path.into())
            .await
            .context(UnableToGetReadProviderSnafu {
                dataconnector: "snowflake",
                connector_component: ConnectorComponent::from(dataset),
            })?)
    }
}
