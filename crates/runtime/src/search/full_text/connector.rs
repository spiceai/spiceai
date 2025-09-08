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
use datafusion::datasource::TableProvider;
use runtime_datafusion_index::{Index, IndexedTableProvider};
use snafu::ResultExt;
use spicepod::semantic::Mode;
use std::any::Any;
use std::sync::Arc;

use crate::accelerated_table::AcceleratedTable;
use crate::component::dataset::FullTextSearchDatasetConfig;
use crate::component::{ComponentInitialization, dataset::Dataset, metrics::MetricsProvider};
use crate::dataconnector::{DataConnector, DataConnectorError, DataConnectorResult};
use crate::make_spice_data_sub_directory;

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

    pub(crate) async fn wrap_table(
        &self,
        inner_table_provider: Arc<dyn TableProvider>,
        dataset: &Dataset,
    ) -> DataConnectorResult<Arc<dyn TableProvider>> {
        let Some(FullTextSearchDatasetConfig {
            mode,
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

        let directory = if mode == Mode::File {
            // Example `.spice/data/fts/catalog/schema/table/`.
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
        } else {
            None
        };

        let index = FullTextDatabaseIndex::try_new(
            Arc::clone(&inner_table_provider),
            search_fields.clone(),
            Some(primary_key),
            directory,
        )
        .await
        .map_err(|e| DataConnectorError::InvalidConfiguration {
            dataconnector: dataset.source().to_string(),
            message: e.to_string(),
            connector_component: dataset.into(),
            source: Box::new(e),
        })?;

        let tbl = IndexedTableProvider::new(inner_table_provider)
            .add_index(Arc::new(index) as Arc<dyn Index + Send + Sync>);

        Ok(Arc::new(tbl) as Arc<dyn TableProvider>)
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
        self.wrap_table(self.inner_connector.read_provider(dataset).await?, dataset)
            .await
    }

    async fn read_write_provider(
        &self,
        dataset: &Dataset,
    ) -> Option<DataConnectorResult<Arc<dyn TableProvider>>> {
        match self.inner_connector.read_write_provider(dataset).await {
            Some(Ok(inner)) => Some(self.wrap_table(inner, dataset).await),
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
}
