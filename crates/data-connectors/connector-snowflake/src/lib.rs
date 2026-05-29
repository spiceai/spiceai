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
use data_components::snowflake::{
    SnowflakeConnectionPool as DynSnowflakeConnectionPool, SnowflakeTableFactory,
    quote_snowflake_table_path,
};
use data_components::{Read, ReadWrite};
use datafusion::datasource::TableProvider;
use datafusion_table_providers::sql::db_connection_pool::DbConnectionPool;
use db_connection_pool::snowflakepool::SnowflakeConnectionPool;
use runtime::component::dataset::Dataset;
use runtime::dataconnector::{
    ConnectorComponent, ConnectorParams, DataConnector, DataConnectorError, DataConnectorFactory,
    DataConnectorResult, NewDataConnectorResult,
};
use runtime::datafusion::udf::deny_spice_specific_functions;
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

            let table_factory = build_snowflake_table_factory(pool);

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

/// Build the [`SnowflakeTableFactory`] with the Spice function deny-list
/// installed.
///
/// Wiring `deny_spice_specific_functions` here prevents federation pushdown
/// from unparsing Spice-only UDFs (e.g. `json_get_str`) into Snowflake SQL,
/// which the remote engine rejects with "Unknown function". Without it, any
/// query that references one of these UDFs against a Snowflake-federated
/// dataset fails at the remote engine instead of falling back to local
/// `DataFusion` evaluation.
fn build_snowflake_table_factory(pool: Arc<DynSnowflakeConnectionPool>) -> SnowflakeTableFactory {
    SnowflakeTableFactory::new(pool)
        .with_function_support(deny_spice_specific_functions().as_ref().clone())
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
    use async_trait::async_trait;
    use datafusion::arrow::datatypes::DataType;
    use datafusion::logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf};
    use datafusion_table_providers::sql::db_connection_pool::dbconnection::DbConnection;
    use datafusion_table_providers::sql::db_connection_pool::{DbConnectionPool, JoinPushDown};
    use std::any::Any;
    use std::error::Error;

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

    struct MockConn;

    impl DbConnection<Arc<SnowflakeApi>, &'static dyn Sync> for MockConn {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn as_any_mut(&mut self) -> &mut dyn Any {
            self
        }
    }

    struct MockPool;

    #[async_trait]
    impl DbConnectionPool<Arc<SnowflakeApi>, &'static dyn Sync> for MockPool {
        async fn connect(
            &self,
        ) -> std::result::Result<
            Box<dyn DbConnection<Arc<SnowflakeApi>, &'static dyn Sync>>,
            Box<dyn Error + Send + Sync>,
        > {
            Ok(Box::new(MockConn))
        }

        fn join_push_down(&self) -> JoinPushDown {
            JoinPushDown::Disallow
        }
    }

    fn stub_udf(name: &str) -> Arc<ScalarUDF> {
        Arc::new(create_udf(
            name,
            vec![DataType::Utf8],
            DataType::Utf8,
            Volatility::Immutable,
            Arc::new(|args: &[ColumnarValue]| Ok(args[0].clone())),
        ))
    }

    #[test]
    fn build_snowflake_table_factory_installs_spice_deny_list() {
        // Regression test for https://github.com/spiceai/spiceai/issues/10703.
        // The extracted `connector-snowflake` crate (the one actually wired
        // into `bin/spiced`) was missing the
        // `.with_function_support(deny_spice_specific_functions()...)` call
        // that exists on the orphaned `crates/runtime/src/dataconnector/
        // snowflake.rs`. Without this wiring, Spice-only UDFs like
        // `json_get_str` get unparsed into Snowflake SQL during federation
        // pushdown and the remote engine rejects them with
        // "Unknown function JSON_GET_STR".
        let pool: Arc<DynSnowflakeConnectionPool> = Arc::new(MockPool);
        let factory = build_snowflake_table_factory(pool);

        let function_support = factory
            .function_support()
            .expect("connector-snowflake must install the Spice deny-list");

        assert!(
            !function_support.supports_scalar(&stub_udf("json_get_str")),
            "json_get_str must be denied so federation falls back to local DataFusion"
        );
        assert!(
            !function_support.supports_scalar(&stub_udf("cosine_distance")),
            "cosine_distance must be denied (Snowflake does not have an exact equivalent)"
        );
        assert!(
            function_support.supports_scalar(&stub_udf("upper")),
            "non-Spice functions like upper() must still federate to Snowflake"
        );
    }
}
