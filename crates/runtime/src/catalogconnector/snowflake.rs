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

//! Snowflake catalog connector.
//!
//! Connects to a Snowflake database and provides schema/table discovery
//! via `INFORMATION_SCHEMA` queries.

use super::{CatalogConnector, ConnectorComponent, ParameterSpec};
use crate::{Runtime, component::catalog::Catalog, dataconnector::parameters::ConnectorParams};
use async_trait::async_trait;
use data_components::RefreshableCatalogProvider;
use data_components::snowflake::SnowflakeTableFactory;
use data_components::snowflake::provider::SnowflakeCatalogProvider;
use datafusion_table_providers::sql::db_connection_pool::DbConnectionPool;
use db_connection_pool::snowflakepool::SnowflakeConnectionPool;
use snafu::prelude::*;
use snowflake_api::SnowflakeApi;
use std::any::Any;
use std::sync::Arc;

pub const PREFIX: &str = "snowflake";
const SNOWFLAKE_DOCS: &str = "https://spiceai.org/docs/components/catalogs/snowflake";
const SNOWFLAKE_ACCOUNT_IDENTIFIER_DOCS: &str =
    "https://docs.snowflake.com/en/user-guide/admin-account-identifier";

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Missing required parameter: {parameter}. Specify a value. For details, visit: https://spiceai.org/docs/components/catalogs/snowflake"
    ))]
    MissingParameter { parameter: String },
}

pub const PARAMETERS: &[ParameterSpec] = &[
    ParameterSpec::component("username")
        .secret()
        .description("Snowflake username for password or key-pair authentication.")
        .examples(&["MACHINE_USER"])
        .help_link(SNOWFLAKE_DOCS),
    ParameterSpec::component("password")
        .secret()
        .description("Snowflake password. Use only with password authentication.")
        .help_link(SNOWFLAKE_DOCS),
    ParameterSpec::component("private_key")
        .secret()
        .description("PEM private key content for key-pair authentication. Use either `snowflake_private_key` or `snowflake_private_key_path`, not both.")
        .help_link(SNOWFLAKE_DOCS),
    ParameterSpec::component("private_key_path")
        .secret()
        .description("Path to a PEM private key file for key-pair authentication. Use either `snowflake_private_key_path` or `snowflake_private_key`, not both.")
        .examples(&["/secrets/snowflake/rsa_key.p8"])
        .help_link(SNOWFLAKE_DOCS),
    ParameterSpec::component("private_key_passphrase")
        .secret()
        .description("Passphrase for an encrypted private key used with key-pair authentication.")
        .help_link(SNOWFLAKE_DOCS),
    ParameterSpec::component("account")
        .secret()
        .description("Snowflake account identifier. Supports preferred account names, org-qualified names, full snowflakecomputing.com URLs, and legacy account locators.")
        .examples(&[
            "myorg-myaccount",
            "myorg.myaccount",
            "https://myorg-myaccount.snowflakecomputing.com",
            "xy12345.us-east-2.aws",
        ])
        .help_link(SNOWFLAKE_ACCOUNT_IDENTIFIER_DOCS),
    ParameterSpec::component("warehouse")
        .secret()
        .description("Snowflake warehouse to use for queries.")
        .examples(&["COMPUTE_WH"])
        .help_link(SNOWFLAKE_DOCS),
    ParameterSpec::component("role")
        .secret()
        .description("Snowflake role to use for the session.")
        .examples(&["ANALYST"])
        .help_link(SNOWFLAKE_DOCS),
    ParameterSpec::component("auth_type")
        .description("Snowflake authentication type. Use `password` or `snowflake` for password authentication, and `keypair` or `snowflake_jwt` for key-pair authentication. Defaults to password unless only key-pair credentials are provided.")
        .one_of_ignore_ascii_case(&["password", "snowflake", "keypair", "snowflake_jwt"])
        .help_link(SNOWFLAKE_DOCS),
];

/// A catalog connector for Snowflake, providing access to schemas and tables
/// within a Snowflake database.
#[derive(Clone)]
pub struct SnowflakeCatalog {
    params: ConnectorParams,
}

impl SnowflakeCatalog {
    #[must_use]
    pub fn new_connector(params: ConnectorParams) -> Arc<dyn CatalogConnector> {
        Arc::new(Self { params })
    }
}

#[async_trait]
impl CatalogConnector for SnowflakeCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn refreshable_catalog_provider(
        self: Arc<Self>,
        _runtime: Arc<Runtime>,
        catalog: &Catalog,
    ) -> super::Result<Arc<dyn RefreshableCatalogProvider>> {
        let connector_component = ConnectorComponent::from(catalog);

        let database: String = match catalog.catalog_id.as_ref() {
            Some(id) if !id.is_empty() => id.clone(),
            _ => {
                let e = Error::MissingParameter {
                    parameter: "database (from 'from: snowflake:<database>')".to_string(),
                };
                return Err(super::Error::InvalidConfigurationNoSource {
                    connector: PREFIX.to_string(),
                    connector_component,
                    message: e.to_string(),
                });
            }
        };

        let pool = SnowflakeConnectionPool::new(&self.params.parameters.to_secret_map())
            .await
            .map_err(|e| super::Error::UnableToGetCatalogProvider {
                connector: PREFIX.to_string(),
                connector_component: connector_component.clone(),
                source: Box::new(e),
            })?;

        let api = Arc::clone(&pool.api);

        let pool: Arc<dyn DbConnectionPool<Arc<SnowflakeApi>, &'static dyn Sync> + Send + Sync> =
            Arc::new(pool);

        let table_factory = Arc::new(SnowflakeTableFactory::new(pool));

        let catalog_provider = if catalog.access.allows_write() {
            Arc::new(SnowflakeCatalogProvider::new_read_write(
                api,
                database,
                table_factory,
                catalog.include.clone(),
            ))
        } else {
            Arc::new(SnowflakeCatalogProvider::new(
                api,
                database,
                table_factory,
                catalog.include.clone(),
            ))
        };

        // Initial refresh to populate schemas and tables
        catalog_provider
            .refresh()
            .await
            .map_err(|e| super::Error::UnableToGetCatalogProvider {
                connector: PREFIX.to_string(),
                connector_component,
                source: e,
            })?;

        Ok(catalog_provider as Arc<dyn RefreshableCatalogProvider>)
    }
}
