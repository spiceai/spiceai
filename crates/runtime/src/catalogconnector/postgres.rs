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
use crate::{
    Runtime,
    component::catalog::{Catalog, table_selector},
    dataconnector::parameters::ConnectorParams,
};
use async_trait::async_trait;
use data_components::RefreshableCatalogProvider;
use data_components::postgres::provider::PostgresCatalogProvider;
use datafusion_table_providers::UnsupportedTypeAction;
use datafusion_table_providers::postgres::PostgresTableFactory;
use datafusion_table_providers::sql::db_connection_pool::postgrespool::PostgresConnectionPool;
use snafu::Snafu;
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
                    "Invalid value '{trimmed}' for `unsupported_type_action`. Expected one of: error, warn, ignore, string. Docs: {POSTGRES_CATALOG_DOCS}"
                )),
            }
        }
    }
}

pub const PREFIX: &str = "pg";

const POSTGRES_CATALOG_DOCS: &str = "https://spiceai.org/docs/components/catalogs/postgres";

/// The connection failure reported when a catalog cannot reach its database,
/// worded for the person who wrote the Spicepod.
///
/// It carries the cause as text and exposes no `source`: the catalog error that
/// wraps it renders the whole chain, so a nested source would append the cause a
/// second time -- after the documentation link, where it reads as part of it.
#[derive(Debug, Snafu)]
#[snafu(display(
    "Failed to connect to PostgreSQL: {cause}. Check the catalog's `pg_host`, `pg_port`, `pg_db`, `pg_user`, `pg_pass` and `pg_sslmode` parameters, and that the database is reachable from Spice. Docs: {POSTGRES_CATALOG_DOCS}"
))]
struct ConnectionFailed {
    cause: String,
}

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
                // The pool reports a connection failure in its own words, over
                // several lines and without naming a parameter to change. Keep
                // what it observed, and say what to do about it.
                source: Box::new(ConnectionFailed {
                    cause: super::error_with_causes(&e)
                        .trim_end_matches(['.', ' '])
                        .to_string(),
                }),
            })?
            .with_unsupported_type_action(unsupported_type_action);

        let pool = Arc::new(pool);

        let catalog_provider: Arc<dyn RefreshableCatalogProvider> =
            if let Some(acceleration) = catalog.acceleration.as_ref() {
                Arc::new(AcceleratedCatalogProvider::new(catalog, acceleration, pool))
            } else {
                let table_factory = Arc::new(PostgresTableFactory::new(Arc::clone(&pool)));
                Arc::new(PostgresCatalogProvider::new(
                    catalog.name.clone(),
                    pool,
                    table_factory,
                    table_selector(catalog),
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

#[cfg(test)]
mod tests {
    use super::*;

    fn dataset_params(action: &str) -> HashMap<String, String> {
        HashMap::from([("unsupported_type_action".to_string(), action.to_string())])
    }

    /// A rejected `unsupported_type_action` is a permanent configuration error,
    /// so its message is the whole of what the user gets to work from: it has to
    /// quote what they wrote, list what is accepted, and link the documentation.
    #[test]
    fn an_invalid_unsupported_type_action_names_the_value_and_the_alternatives() {
        let message = parse_unsupported_type_action(&dataset_params("strng"))
            .expect_err("'strng' is not a valid action");

        assert!(
            message.contains("Invalid value 'strng'"),
            "quotes what was configured: {message}"
        );
        assert!(
            message.contains("error, warn, ignore, string"),
            "lists every accepted value: {message}"
        );
        assert!(
            message.contains(POSTGRES_CATALOG_DOCS),
            "links the documentation: {message}"
        );
        assert!(!message.contains('\n'), "stays on one line: {message:?}");
    }

    #[test]
    fn a_valid_unsupported_type_action_is_accepted_in_any_case() {
        for value in ["error", " ERROR ", "Error"] {
            assert!(
                matches!(
                    parse_unsupported_type_action(&dataset_params(value)),
                    Ok(UnsupportedTypeAction::Error)
                ),
                "'{value}' should parse as `error`"
            );
        }
    }

    /// The connection failure the pool reports names no parameter and spans
    /// several lines; what reaches the user must name both, on one line.
    #[test]
    fn a_connection_failure_names_the_parameters_to_check() {
        let message = ConnectionFailed {
            cause: "PostgreSQL connection failed. db error: FATAL: password authentication failed"
                .to_string(),
        }
        .to_string();

        assert!(
            message.contains("password authentication failed"),
            "keeps what the database said: {message}"
        );
        assert!(
            message.contains("`pg_user`") && message.contains("`pg_pass`"),
            "names the parameters to check: {message}"
        );
        assert!(
            message.contains(POSTGRES_CATALOG_DOCS),
            "links the documentation: {message}"
        );
        assert!(!message.contains('\n'), "stays on one line: {message:?}");
        assert!(
            std::error::Error::source(&ConnectionFailed {
                cause: "x".to_string()
            })
            .is_none(),
            "the cause is carried as text, so the catalog error that wraps this cannot append it after the documentation link"
        );
    }
}
