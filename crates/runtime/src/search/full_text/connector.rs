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
use async_trait::async_trait;
use data_components::cdc::ChangesStream;
use datafusion::datasource::TableProvider;
use runtime_datafusion_index::{Index, IndexedTableProvider};
use snafu::ResultExt;
use spicepod::semantic::{IndexStore, MetadataType};
use std::any::Any;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

use crate::accelerated_table::AcceleratedTable;
use crate::changes::{Indexes, index_change_envelope};
use crate::component::{
    ComponentInitialization,
    dataset::{Dataset, FullTextSearchDatasetConfig, acceleration::RefreshMode},
    metrics::MetricsProvider,
};
use crate::dataconnector::{DataConnector, DataConnectorError, DataConnectorResult};
use crate::federated_table::FederatedTable;
use crate::make_spice_data_sub_directory;
use crate::search::util::find_index_in_table_provider;
use futures::StreamExt;

use search::generation::text_search::index::FullTextDatabaseIndex;

/// A [`DataConnector`] middleware that, for [`Dataset`]s needing full text search capabilies, creates a [`IndexedTableProvider`] using the underlying [`TableProvider`]s and a [`FullTextDatabaseIndex`]. If no full text search capabilities are needed it is not unnecessarily nested.
#[derive(Debug)]
pub struct FullTextConnector {
    inner_connector: Arc<dyn DataConnector>,
}

impl FullTextConnector {
    pub fn new(inner_connector: Arc<dyn DataConnector>) -> Self {
        Self { inner_connector }
    }

    pub(crate) fn wrap_table(
        inner_table_provider: Arc<dyn TableProvider>,
        dataset: &Dataset,
    ) -> DataConnectorResult<Arc<dyn TableProvider>> {
        let Some(FullTextSearchDatasetConfig {
            index_store,
            index_path,
            search_fields,
            primary_key,
        }) = dataset.full_text_search_config()
        else {
            return Err(DataConnectorError::InvalidConfigurationNoSource {
                dataconnector: dataset.source().to_string(),
                connector_component: dataset.into(),
                message: format!(
                    "Attempted to add full text search functionality to '{}', but configuration not available",
                    dataset.name
                ),
            });
        };

        let directory = if index_store == IndexStore::File {
            if let Some(path) = index_path {
                Some(PathBuf::from_str(path.as_str()).boxed().map_err(|e| {
                    DataConnectorError::InvalidConfiguration {
                        dataconnector: dataset.source().to_string(),
                        message: e.to_string(),
                        connector_component: dataset.into(),
                        source: e,
                    }
                })?)
            } else {
                // Default case. Example `.spice/data/fts/catalog/schema/table/`.
                Some(
                    make_spice_data_sub_directory(
                        [vec!["fts".to_string()], dataset.name.to_vec()]
                            .concat()
                            .as_slice(),
                    )
                    .boxed()
                    .map_err(|e| DataConnectorError::InvalidConfiguration {
                        dataconnector: dataset.source().to_string(),
                        message: e.to_string(),
                        connector_component: dataset.into(),
                        source: e,
                    })?,
                )
            }
        } else {
            None
        };

        let store_fields = dataset
            .columns
            .iter()
            .filter_map(|c| {
                if let Some(MetadataType::NonFilterable) = c.as_vector_metadata() {
                    return Some(c.name.clone());
                }
                None
            })
            .collect::<Vec<_>>();

        let index = FullTextDatabaseIndex::try_new(
            Arc::clone(&inner_table_provider),
            search_fields.clone(),
            Some(primary_key),
            directory,
            &store_fields,
        )
        .map_err(|e| DataConnectorError::InvalidConfiguration {
            dataconnector: dataset.source().to_string(),
            message: e.to_string(),
            connector_component: dataset.into(),
            source: Box::new(e),
        })?;

        let tbl: IndexedTableProvider = if let Some(idx_tbl) = inner_table_provider
            .as_any()
            .downcast_ref::<IndexedTableProvider>(
        ) {
            idx_tbl.clone()
        } else {
            IndexedTableProvider::new(inner_table_provider)
        };

        Ok(
            Arc::new(tbl.add_index(Arc::new(index) as Arc<dyn Index + Send + Sync>))
                as Arc<dyn TableProvider>,
        )
    }

    #[allow(clippy::needless_pass_by_value)]
    fn with_indexed_stream<F>(
        &self,
        federated_table: Arc<FederatedTable>,
        f: F,
    ) -> Option<ChangesStream>
    where
        F: Fn(&Arc<dyn DataConnector>, Arc<FederatedTable>) -> Option<ChangesStream>,
    {
        let table_provider = federated_table.try_table_provider_sync()?;

        let Some((indexed, underlying)) =
            find_index_in_table_provider::<FullTextDatabaseIndex>(&table_provider)
        else {
            tracing::debug!(
                "FullTextConnector didn't wrap underlying table with index - this is unexpected"
            );
            return None;
        };

        let indexed = indexed
            .into_iter()
            .cloned()
            .map(|i| Arc::new(i) as Arc<dyn Index + Send + Sync>)
            .collect();

        let indexed = Indexes::new(indexed);
        let ft = Arc::new(FederatedTable::Immediate(underlying));

        let stream = f(&self.inner_connector, ft)?;
        Some(
            stream
                .then(move |item| index_change_envelope(item, Arc::clone(&indexed)))
                .boxed(),
        )
    }
}

#[async_trait]
impl DataConnector for FullTextConnector {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(
        &self,
        dataset: &Dataset,
    ) -> DataConnectorResult<Arc<dyn TableProvider>> {
        Self::wrap_table(self.inner_connector.read_provider(dataset).await?, dataset)
    }

    async fn read_write_provider(
        &self,
        dataset: &Dataset,
    ) -> Option<DataConnectorResult<Arc<dyn TableProvider>>> {
        match self.inner_connector.read_write_provider(dataset).await {
            Some(Ok(inner)) => Some(Self::wrap_table(inner, dataset)),
            Some(Err(e)) => Some(Err(e)),
            None => None,
        }
    }

    async fn metadata_provider(
        &self,
        dataset: &Dataset,
    ) -> Option<DataConnectorResult<Arc<dyn TableProvider>>> {
        self.inner_connector.metadata_provider(dataset).await
    }

    fn initialization(&self) -> ComponentInitialization {
        self.inner_connector.initialization()
    }

    fn metrics_provider(&self) -> Option<Arc<dyn MetricsProvider>> {
        self.inner_connector.metrics_provider()
    }

    async fn on_accelerated_table_registration(
        &self,
        dataset: &Dataset,
        accelerated_table: &mut AcceleratedTable,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.inner_connector
            .on_accelerated_table_registration(dataset, accelerated_table)
            .await
    }

    fn resolve_refresh_mode(&self, refresh_mode: Option<RefreshMode>) -> RefreshMode {
        self.inner_connector.resolve_refresh_mode(refresh_mode)
    }

    fn supports_changes_stream(&self) -> bool {
        self.inner_connector.supports_changes_stream()
    }

    fn changes_stream(&self, federated_table: Arc<FederatedTable>) -> Option<ChangesStream> {
        self.with_indexed_stream(federated_table, |inner, ft| inner.changes_stream(ft))
    }

    fn supports_append_stream(&self) -> bool {
        self.inner_connector.supports_append_stream()
    }

    fn append_stream(&self, federated_table: Arc<FederatedTable>) -> Option<ChangesStream> {
        self.with_indexed_stream(federated_table, |inner, ft| inner.append_stream(ft))
    }
}
