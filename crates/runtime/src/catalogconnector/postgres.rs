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

//! `PostgreSQL` catalog connector.
//!
//! Connects to a `PostgreSQL` (or Redshift) database and provides schema/table
//! discovery via `information_schema` queries.

use super::{CatalogConnector, ConnectorComponent, ParameterSpec};
use crate::catalogconnector::postgres_accelerated::{
    AcceleratedCatalogProvider, NoEligibleTablesError, SlotInUseError,
};
use crate::{Runtime, component::catalog::Catalog, dataconnector::parameters::ConnectorParams};
use async_trait::async_trait;
use data_components::RefreshableCatalogProvider;
use data_components::postgres::provider::PostgresCatalogProvider;
use datafusion_table_providers::UnsupportedTypeAction;
use datafusion_table_providers::postgres::PostgresTableFactory;
use datafusion_table_providers::sql::db_connection_pool::postgrespool::PostgresConnectionPool;
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

/// Parses the `unsupported_type_action` dataset param threaded through the
/// catalog's `dataset_params` (the same mechanism used by the Databricks and
/// Unity Catalog catalog connectors to pass per-table dataset params). Absent
/// a value, defaults to `String`, matching the direct `PostgreSQL` data
/// connector's default (see `connector-postgres`). See #11728.
fn parse_unsupported_type_action(
    dataset_params: &HashMap<String, String>,
) -> Result<UnsupportedTypeAction, String> {
    match dataset_params.get("unsupported_type_action") {
        None => Ok(UnsupportedTypeAction::String),
        Some(value) => {
            let trimmed = value.trim();
            match trimmed.to_ascii_lowercase().as_str() {
                "string" => Ok(UnsupportedTypeAction::String),
                "error" => Ok(UnsupportedTypeAction::Error),
                "warn" => Ok(UnsupportedTypeAction::Warn),
                "ignore" => Ok(UnsupportedTypeAction::Ignore),
                _ => Err(format!(
                    "Invalid value '{trimmed}' for `unsupported_type_action`. Expected one of: error, warn, ignore, string."
                )),
            }
        }
    }
}

pub const PREFIX: &str = "pg";

pub const PARAMETERS: &[ParameterSpec] = &[
    ParameterSpec::component("connection_string")
        .secret()
        .description("The PostgreSQL connection string."),
    ParameterSpec::component("user")
        .secret()
        .description("The PostgreSQL username for authentication."),
    ParameterSpec::component("pass")
        .secret()
        .description("The PostgreSQL password for authentication."),
    ParameterSpec::component("host").description("The PostgreSQL host address."),
    ParameterSpec::component("port").description("The PostgreSQL port number."),
    ParameterSpec::component("db").description("The PostgreSQL database name."),
    ParameterSpec::component("sslmode").description("The SSL mode for the connection."),
    ParameterSpec::component("sslrootcert")
        .description("The path to, or inline PEM content for, the SSL root certificate."),
];

/// A catalog connector for `PostgreSQL`, providing access to schemas and tables
/// within a `PostgreSQL` database. Also usable for Redshift.
#[derive(Clone)]
pub struct PostgresCatalog {
    params: ConnectorParams,
}

impl PostgresCatalog {
    #[must_use]
    pub fn new_connector(params: ConnectorParams) -> Arc<dyn CatalogConnector> {
        Arc::new(Self { params })
    }
}

#[async_trait]
impl CatalogConnector for PostgresCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn refreshable_catalog_provider(
        self: Arc<Self>,
        _runtime: Arc<Runtime>,
        catalog: &Catalog,
    ) -> super::Result<Arc<dyn RefreshableCatalogProvider>> {
        let connector_component = ConnectorComponent::from(catalog);

        let unsupported_type_action = parse_unsupported_type_action(&catalog.dataset_params)
            .map_err(|message| super::Error::InvalidConfigurationNoSource {
                connector: PREFIX.to_string(),
                connector_component: connector_component.clone(),
                message,
            })?;

        let pool = PostgresConnectionPool::new(self.params.parameters.to_secret_map())
            .await
            .map_err(|e| super::Error::UnableToGetCatalogProvider {
                connector: PREFIX.to_string(),
                connector_component: connector_component.clone(),
                source: Box::new(e),
            })?
            .with_unsupported_type_action(unsupported_type_action);

        let pool = Arc::new(pool);

        let catalog_provider: Arc<dyn RefreshableCatalogProvider> =
            if catalog.acceleration.is_some() {
                Arc::new(AcceleratedCatalogProvider::new(catalog, pool))
            } else {
                let table_factory = Arc::new(PostgresTableFactory::new(Arc::clone(&pool)));
                Arc::new(PostgresCatalogProvider::new(
                    catalog.name.clone(),
                    pool,
                    table_factory,
                    catalog.include.clone(),
                    catalog.exclude.clone(),
                    &catalog.orig_include,
                ))
            };

        catalog_provider.refresh().await.map_err(|e| {
            // Two classes of permanent (non-retryable) configuration problem,
            // surfaced as a terminal ERROR status instead of retried forever:
            //   - zero eligible tables: this is the *initial* refresh, so failing
            //     it means the catalog never registers and never gets a periodic
            //     refresh -- fixing the source/filters then requires a restart, so
            //     surface it loudly rather than starting an empty catalog; and
            //   - the catalog's replication slot already actively held by another
            //     live consumer after the bounded wait (running two instances
            //     against one catalog is a misconfiguration, not a transient).
            if e.downcast_ref::<NoEligibleTablesError>().is_some()
                || e.downcast_ref::<SlotInUseError>().is_some()
            {
                super::Error::InvalidConfiguration {
                    connector: PREFIX.to_string(),
                    connector_component: connector_component.clone(),
                    message: e.to_string(),
                    source: e,
                }
            } else {
                super::Error::UnableToGetCatalogProvider {
                    connector: PREFIX.to_string(),
                    connector_component: connector_component.clone(),
                    source: e,
                }
            }
        })?;

        Ok(catalog_provider)
    }
}
