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

use connector_traits::{
    AnyErrorResult, ConnectorAcceleratedTable, ConnectorApp, ConnectorCatalog, ConnectorComponent,
    ConnectorDataset, ConnectorFederatedTable, ConnectorRuntime,
};
use datafusion::sql::TableReference;
use datafusion::sql::sqlparser::dialect::Dialect;
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::sync::RwLock;

use crate::Runtime;
use crate::accelerated_table::AcceleratedTable;
use crate::component::catalog::Catalog;
use crate::component::dataset::Dataset;
use crate::component::dataset::acceleration::RefreshMode as RuntimeRefreshMode;
use crate::federated_table::FederatedTable;
use app::App;
use runtime_secrets::Secrets;
use token_provider::registry::TokenProviderRegistry;

impl ConnectorDataset for Dataset {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &TableReference {
        &self.name
    }

    fn table_name(&self) -> &str {
        self.name.table()
    }

    fn from(&self) -> &str {
        self.from.as_str()
    }

    fn path(&self) -> &str {
        self.path()
    }

    fn params(&self) -> &HashMap<String, String> {
        &self.params
    }

    fn columns(&self) -> &[spicepod::semantic::Column] {
        &self.columns
    }

    fn metadata(&self) -> &HashMap<String, String> {
        &self.metadata
    }

    fn time_column(&self) -> Option<&str> {
        self.time_column.as_deref()
    }

    fn time_partition_column(&self) -> Option<&str> {
        self.time_partition_column.as_deref()
    }

    fn has_metadata_table(&self) -> bool {
        self.has_metadata_table
    }

    fn is_accelerated(&self) -> bool {
        self.is_accelerated()
    }

    fn is_file_accelerated(&self) -> bool {
        self.is_file_accelerated()
    }

    fn acceleration_configured(&self) -> bool {
        self.acceleration.is_some()
    }

    fn acceleration_params(&self) -> Option<&HashMap<String, String>> {
        self.acceleration
            .as_ref()
            .map(|acceleration| &acceleration.params)
    }

    fn refresh_mode(&self) -> Option<connector_traits::RefreshMode> {
        self.acceleration
            .as_ref()
            .and_then(|acceleration| acceleration.refresh_mode)
            .map(|mode| match mode {
                crate::component::dataset::acceleration::RefreshMode::Disabled => {
                    connector_traits::RefreshMode::Disabled
                }
                crate::component::dataset::acceleration::RefreshMode::Full => {
                    connector_traits::RefreshMode::Full
                }
                crate::component::dataset::acceleration::RefreshMode::Append => {
                    connector_traits::RefreshMode::Append
                }
                crate::component::dataset::acceleration::RefreshMode::Changes => {
                    connector_traits::RefreshMode::Changes
                }
                crate::component::dataset::acceleration::RefreshMode::Caching => {
                    connector_traits::RefreshMode::Caching
                }
            })
    }

    fn refresh_sql(&self) -> Option<String> {
        self.refresh_sql()
    }

    fn parse_path(
        &self,
        case_sensitive: bool,
        dialect: Option<&dyn Dialect>,
    ) -> AnyErrorResult<TableReference> {
        self.parse_path(case_sensitive, dialect)
            .map_err(|source| Box::new(source) as Box<dyn std::error::Error + Send + Sync>)
    }
}

impl ConnectorCatalog for Catalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn catalog_id(&self) -> Option<&str> {
        self.catalog_id.as_deref()
    }

    fn from(&self) -> &str {
        &self.from
    }

    fn params(&self) -> &HashMap<String, String> {
        &self.params
    }

    fn dataset_params(&self) -> &HashMap<String, String> {
        &self.dataset_params
    }

    fn include(&self) -> Option<&globset::GlobSet> {
        self.include.as_ref()
    }
}

impl From<&Dataset> for ConnectorComponent {
    fn from(dataset: &Dataset) -> Self {
        ConnectorComponent::Dataset(connector_traits::ConnectorComponentDataset {
            name: dataset.name().to_string(),
            table_name: dataset.table_name().to_string(),
            from: dataset.from().to_string(),
        })
    }
}

impl From<&Catalog> for ConnectorComponent {
    fn from(catalog: &Catalog) -> Self {
        ConnectorComponent::Catalog(connector_traits::ConnectorComponentCatalog {
            name: catalog.name().to_string(),
        })
    }
}

#[derive(Clone, Debug)]
pub(crate) struct RuntimeConnectorApp {
    app: Arc<App>,
}

impl RuntimeConnectorApp {
    pub(crate) fn new(app: Arc<App>) -> Self {
        Self { app }
    }
}

impl ConnectorApp for RuntimeConnectorApp {
    fn as_any(&self) -> &dyn Any {
        self.app.as_ref()
    }

    fn flight_max_message_size_bytes(&self) -> AnyErrorResult<Option<usize>> {
        match &self.app.runtime.flight {
            Some(flight) => flight.max_message_size_bytes(),
            None => Ok(None),
        }
    }
}

#[async_trait::async_trait]
impl ConnectorRuntime for Runtime {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn token_provider_registry(&self) -> Arc<TokenProviderRegistry> {
        self.token_provider_registry()
    }

    fn secrets(&self) -> Arc<RwLock<Secrets>> {
        self.secrets()
    }

    fn tokio_io_runtime(&self) -> Handle {
        self.tokio_io_runtime()
    }

    async fn runtime_param(&self, key: &str) -> AnyErrorResult<Option<String>> {
        let app = self.app();
        let app = app.read().await;
        Ok(app::App::get_runtime_param_opt(&app, key))
    }
}

impl ConnectorFederatedTable for FederatedTable {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl ConnectorAcceleratedTable for AcceleratedTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl From<RuntimeRefreshMode> for connector_traits::RefreshMode {
    fn from(mode: RuntimeRefreshMode) -> Self {
        match mode {
            RuntimeRefreshMode::Disabled => connector_traits::RefreshMode::Disabled,
            RuntimeRefreshMode::Full => connector_traits::RefreshMode::Full,
            RuntimeRefreshMode::Append => connector_traits::RefreshMode::Append,
            RuntimeRefreshMode::Changes => connector_traits::RefreshMode::Changes,
            RuntimeRefreshMode::Caching => connector_traits::RefreshMode::Caching,
        }
    }
}

impl From<connector_traits::RefreshMode> for RuntimeRefreshMode {
    fn from(mode: connector_traits::RefreshMode) -> Self {
        match mode {
            connector_traits::RefreshMode::Disabled => RuntimeRefreshMode::Disabled,
            connector_traits::RefreshMode::Full => RuntimeRefreshMode::Full,
            connector_traits::RefreshMode::Append => RuntimeRefreshMode::Append,
            connector_traits::RefreshMode::Changes => RuntimeRefreshMode::Changes,
            connector_traits::RefreshMode::Caching => RuntimeRefreshMode::Caching,
        }
    }
}
