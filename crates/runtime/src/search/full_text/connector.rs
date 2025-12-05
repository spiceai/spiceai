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
use runtime_datafusion_index::IndexedTableProvider;
use std::any::Any;
use std::sync::Arc;

use crate::accelerated_table::AcceleratedTable;
use crate::changes::{Indexes, index_change_envelope};
use crate::component::{
    ComponentInitialization,
    dataset::{Dataset, acceleration::RefreshMode},
    metrics::MetricsProvider,
};
use crate::dataconnector::{DataConnector, DataConnectorError, DataConnectorResult};
use crate::federated_table::FederatedTable;
use crate::search::full_text::table::add_full_text_search_to_table;
use crate::search::util::find_concrete_table_provider;
use futures::StreamExt;

/// A [`DataConnector`] middleware that, for [`Dataset`]s needing full text search capabilies, creates a [`IndexedTableProvider`] using the underlying [`TableProvider`]s and a [`FullTextDatabaseIndex`]. If no full text search capabilities are needed it is not unnecessarily nested.
#[derive(Debug)]
pub struct FullTextConnector {
    inner_connector: Arc<dyn DataConnector>,
}

impl FullTextConnector {
    pub fn new(inner_connector: Arc<dyn DataConnector>) -> Self {
        Self { inner_connector }
    }

    #[expect(clippy::needless_pass_by_value)]
    fn with_indexed_stream<F>(
        &self,
        federated_table: Arc<FederatedTable>,
        f: F,
    ) -> Option<ChangesStream>
    where
        F: Fn(&Arc<dyn DataConnector>, Arc<FederatedTable>) -> Option<ChangesStream>,
    {
        let table_provider = federated_table.try_table_provider_sync()?;
        let indexed_table = find_concrete_table_provider::<IndexedTableProvider>(&table_provider)?;

        // This will process all `Index`s, including vector indexes if provided (i.e. from `EmbeddingConnector`).
        // This is required so that [`IndexedTableProvider`] can be unwrapped (i.e. [`IndexedTableProvider::get_underlying`])
        //  in both cases there is and isn't a `EmbeddingConnector` underneath.
        let indexes = Indexes::new(indexed_table.get_all_indexes());
        let ft = Arc::new(FederatedTable::Immediate(indexed_table.get_underlying()));

        let stream = f(&self.inner_connector, ft)?;
        Some(
            stream
                .then(move |item| index_change_envelope(item, Arc::clone(&indexes)))
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
        add_full_text_search_to_table(
            self.inner_connector.read_provider(dataset).await?,
            &dataset.columns,
            &dataset.name,
        )
        .map(|idx| Arc::new(idx) as Arc<dyn TableProvider>)
        .map_err(|e| DataConnectorError::InvalidConfiguration {
            dataconnector: dataset.source().to_string(),
            message: e.to_string(),
            connector_component: dataset.into(),
            source: e,
        })
    }

    async fn read_write_provider(
        &self,
        dataset: &Dataset,
    ) -> Option<DataConnectorResult<Arc<dyn TableProvider>>> {
        match self.inner_connector.read_write_provider(dataset).await {
            Some(Ok(inner)) => Some(
                add_full_text_search_to_table(inner, &dataset.columns, &dataset.name)
                    .map(|idx| Arc::new(idx) as Arc<dyn TableProvider>)
                    .map_err(|e| DataConnectorError::InvalidConfiguration {
                        dataconnector: dataset.source().to_string(),
                        message: e.to_string(),
                        connector_component: dataset.into(),
                        source: e,
                    }),
            ),
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

    fn changes_stream(
        &self,
        federated_table: Arc<FederatedTable>,
        dataset: &Dataset,
    ) -> Option<ChangesStream> {
        self.with_indexed_stream(federated_table, |inner, ft| {
            inner.changes_stream(ft, dataset)
        })
    }

    fn supports_append_stream(&self) -> bool {
        self.inner_connector.supports_append_stream()
    }

    fn append_stream(&self, federated_table: Arc<FederatedTable>) -> Option<ChangesStream> {
        self.with_indexed_stream(federated_table, |inner, ft| inner.append_stream(ft))
    }
}
