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

use super::ConnectorComponent;
use super::ConnectorParams;
use super::DataConnector;
use super::DataConnectorFactory;
use super::ParameterSpec;
use async_trait::async_trait;
use data_components::{Read, ReadWrite};
use data_components::snowflake::SnowflakeTableFactory;
use datafusion_table_providers::sql::db_connection_pool::DbConnectionPool;

use crate::component::dataset::Dataset;
use datafusion::datasource::TableProvider;
use db_connection_pool::snowflakepool::SnowflakeConnectionPool;
use itertools::Itertools;
use snafu::prelude::*;
use snowflake_api::SnowflakeApi;
use std::any::Any;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Failed to create Snowflake connection pool: {source}. Check your `account`, `warehouse`, `username`, and auth (`password` or `private_key`/`private_key_path`). Docs: https://spiceai.org/docs/components/data-connectors/snowflake"
    ))]
    UnableToCreateSnowflakeConnectionPool {
        source: db_connection_pool::snowflakepool::Error,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub struct Snowflake {
    table_factory: SnowflakeTableFactory,
}

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

const SNOWFLAKE_DOCS: &str = "https://spiceai.org/docs/components/data-connectors/snowflake";

const PARAMETERS: &[ParameterSpec] = &[
    ParameterSpec::component("username")
        .description("Snowflake username.")
        .help_link(SNOWFLAKE_DOCS)
        .secret(),
    ParameterSpec::component("password")
        .description("Snowflake password (when using password auth).")
        .help_link(SNOWFLAKE_DOCS)
        .secret(),
    ParameterSpec::component("private_key_path")
        .description("Path to an RSA private key file for keypair auth.")
        .examples(&["/var/secrets/snowflake_rsa.p8"])
        .help_link(SNOWFLAKE_DOCS)
        .secret(),
    ParameterSpec::component("private_key")
        .description("PEM-encoded RSA private key contents for keypair auth.")
        .help_link(SNOWFLAKE_DOCS)
        .secret(),
    ParameterSpec::component("private_key_passphrase")
        .description("Passphrase for an encrypted RSA private key.")
        .help_link(SNOWFLAKE_DOCS)
        .secret(),
    ParameterSpec::component("account")
        .description("Snowflake account identifier.")
        .examples(&["xy12345.us-east-1"])
        .help_link(SNOWFLAKE_DOCS)
        .secret(),
    ParameterSpec::component("warehouse")
        .description("Snowflake virtual warehouse to run queries against.")
        .examples(&["COMPUTE_WH"])
        .help_link(SNOWFLAKE_DOCS)
        .secret(),
    ParameterSpec::component("role")
        .description("Snowflake role to use for the session.")
        .examples(&["READ_ONLY"])
        .help_link(SNOWFLAKE_DOCS)
        .secret(),
    ParameterSpec::component("auth_type")
        .description("Authentication method: 'password' or 'snowflake_jwt' (keypair).")
        .one_of(&["password", "snowflake_jwt"])
        .help_link(SNOWFLAKE_DOCS),
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
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
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

#[async_trait]
impl DataConnector for Snowflake {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(
        &self,
        dataset: &Dataset,
    ) -> super::DataConnectorResult<Arc<dyn TableProvider>> {
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
            .context(super::UnableToGetReadProviderSnafu {
                dataconnector: "snowflake",
                connector_component: ConnectorComponent::from(dataset),
            })?)
    }

    async fn read_write_provider(
        &self,
        dataset: &Dataset,
    ) -> Option<super::DataConnectorResult<Arc<dyn TableProvider>>> {
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

        Some(
            ReadWrite::table_provider(&self.table_factory, path.into())
                .await
                .context(super::UnableToGetReadWriteProviderSnafu {
                    dataconnector: "snowflake",
                    connector_component: ConnectorComponent::from(dataset),
                }),
        )
    }
}

register_data_connector!("snowflake", SnowflakeFactory);
