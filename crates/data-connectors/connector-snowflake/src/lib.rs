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
use data_components::snowflake::{SnowflakeTableFactory, quote_snowflake_table_path};
use data_components::{Read, ReadWrite};
use datafusion::datasource::TableProvider;
use datafusion_table_providers::sql::db_connection_pool::DbConnectionPool;
use db_connection_pool::snowflakepool::SnowflakeConnectionPool;
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
    ParameterSpec::component("private_key").secret(),
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

    #[snafu(display("Unable to get read-write provider for {dataconnector}: {source}"))]
    UnableToGetReadWriteProvider {
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
            ReadProviderError::UnableToGetReadWriteProvider {
                dataconnector,
                connector_component,
                source,
            } => DataConnectorError::UnableToGetReadWriteProvider {
                dataconnector: dataconnector.to_string(),
                connector_component,
                source,
            },
        }
    }
}

fn snowflake_table_path(dataset: &Dataset) -> DataConnectorResult<String> {
    quote_snowflake_table_path(dataset.path()).map_err(|source| {
        DataConnectorError::InvalidConfiguration {
            dataconnector: CONNECTOR_NAME.to_string(),
            connector_component: ConnectorComponent::from(dataset),
            message: format!(
                "The specified table name in dataset path is invalid '{}'. Ensure the table name uses valid Snowflake identifier syntax and try again.",
                dataset.path()
            ),
            source: Box::new(source),
        }
    })
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
        let path = snowflake_table_path(dataset)?;

        Ok(Read::table_provider(&self.table_factory, path.into())
            .await
            .context(UnableToGetReadProviderSnafu {
                dataconnector: "snowflake",
                connector_component: ConnectorComponent::from(dataset),
            })?)
    }

    async fn read_write_provider(
        &self,
        dataset: &Dataset,
    ) -> Option<DataConnectorResult<Arc<dyn TableProvider>>> {
        let path = match snowflake_table_path(dataset) {
            Ok(path) => path,
            Err(error) => return Some(Err(error)),
        };

        Some(
            ReadWrite::table_provider(&self.table_factory, path.into())
                .await
                .context(UnableToGetReadWriteProviderSnafu {
                    dataconnector: "snowflake",
                    connector_component: ConnectorComponent::from(dataset),
                })
                .map_err(Into::into),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_factory_prefix() {
        let factory = SnowflakeFactory::new();
        assert_eq!(factory.prefix(), "snowflake");
    }

    #[test]
    fn test_parameters_include_inline_private_key() {
        // Regression test for https://github.com/spiceai/spiceai/issues/10517.
        // `snowflakepool::init_snowflake_api_with_keypair_auth` reads
        // `params.get("private_key")` for inline keypair auth, so the parameter
        // must be declared in PARAMETERS — otherwise runtime parameter
        // validation strips it before it reaches the pool.
        let factory = SnowflakeFactory::new();
        let param_names: Vec<&str> = factory.parameters().iter().map(|p| p.name).collect();

        for name in [
            "username",
            "password",
            "private_key",
            "private_key_path",
            "private_key_passphrase",
            "account",
            "warehouse",
            "role",
            "auth_type",
        ] {
            assert!(
                param_names.contains(&name),
                "Snowflake PARAMETERS missing `{name}`; declared params: {param_names:?}"
            );
        }
    }

    #[test]
    fn test_private_key_is_secret() {
        let factory = SnowflakeFactory::new();
        let private_key = factory
            .parameters()
            .iter()
            .find(|p| p.name == "private_key")
            .expect("private_key parameter must be declared");
        assert!(
            private_key.secret,
            "private_key holds PEM key material and must be marked secret"
        );
    }

    #[test]
    fn snowflake_table_path_quotes_each_identifier() {
        assert_eq!(
            quote_snowflake_table_path("database.schema.table").expect("path should be quoted"),
            "\"database\".\"schema\".\"table\""
        );
    }

    #[test]
    fn snowflake_table_path_preserves_quoted_dots() {
        assert_eq!(
            quote_snowflake_table_path(r#""my.schema".table"#).expect("path should be quoted"),
            "\"my.schema\".\"table\""
        );
    }

    #[test]
    fn snowflake_table_path_escapes_embedded_double_quotes() {
        assert_eq!(
            quote_snowflake_table_path(r#"schema."a""b""#).expect("path should be quoted"),
            "\"schema\".\"a\"\"b\""
        );
    }

    #[test]
    fn snowflake_table_path_rejects_invalid_identifier_paths() {
        quote_snowflake_table_path(r#""unterminated.table"#)
            .expect_err("should reject unterminated quoted identifier");
        quote_snowflake_table_path("a.b.c.d").expect_err("should reject 4-part identifier");
    }
}
