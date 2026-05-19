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
use crate::{Runtime, component::catalog::Catalog, dataconnector::parameters::ConnectorParams};
use async_trait::async_trait;
use data_components::RefreshableCatalogProvider;
use data_components::postgres::provider::PostgresCatalogProvider;
use datafusion_table_providers::postgres::PostgresTableFactory;
use datafusion_table_providers::sql::db_connection_pool::postgrespool::PostgresConnectionPool;
use std::any::Any;
use std::sync::Arc;

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

        let pool = PostgresConnectionPool::new(self.params.parameters.to_secret_map())
            .await
            .map_err(|e| super::Error::UnableToGetCatalogProvider {
                connector: PREFIX.to_string(),
                connector_component: connector_component.clone(),
                source: Box::new(e),
            })?;

        let pool = Arc::new(pool);
        let table_factory = Arc::new(PostgresTableFactory::new(Arc::clone(&pool)));

        let catalog_provider = Arc::new(PostgresCatalogProvider::new(
            catalog.name.clone(),
            pool,
            table_factory,
            catalog.include.clone(),
        ));

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
