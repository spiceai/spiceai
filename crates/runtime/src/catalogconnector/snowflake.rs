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
        .description("The Snowflake username for authentication."),
    ParameterSpec::component("password")
        .secret()
        .description("The Snowflake password for authentication."),
    ParameterSpec::component("private_key")
        .secret()
        .description("The private key content for key pair authentication."),
    ParameterSpec::component("private_key_path")
        .secret()
        .description("The path to a private key file for key pair authentication."),
    ParameterSpec::component("private_key_passphrase")
        .secret()
        .description("The passphrase for the private key file."),
    ParameterSpec::component("account")
        .secret()
        .description("The Snowflake account identifier."),
    ParameterSpec::component("warehouse")
        .secret()
        .description("The Snowflake warehouse to use."),
    ParameterSpec::component("role")
        .secret()
        .description("The Snowflake role to use."),
    ParameterSpec::component("auth_type").description(
        "The authentication type ('snowflake' or 'keypair'). Defaults to 'snowflake'.",
    ),
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
