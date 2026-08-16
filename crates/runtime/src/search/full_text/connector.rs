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
use search::generation::text_search::index::FullTextDatabaseIndex;
use search::index::chunking::ChunkedSearchIndex;
use search::index::compound::CompoundSearchIndex;
use std::any::Any;
use std::sync::Arc;

use crate::changes::{Indexes, index_change_envelope};
use crate::component::dataset::DatasetSpec;
use crate::component::{ComponentInitialization, dataset::acceleration::RefreshMode};
use crate::dataconnector::{DataConnector, DataConnectorError, DataConnectorResult};
use crate::federated::FederatedTable;
use crate::search::full_text::table::add_full_text_search_to_table;
use data_connector_api::accelerated::{AcceleratorSetup, RegisteredAcceleratedTable};
use data_connector_api::federated::FederatedTableProvider;
use futures::StreamExt;
use runtime_metrics::component::MetricsProvider;
use spice_table::LayerWalk;

/// A [`DataConnector`] middleware that, for [`Dataset`]s needing full text search capabilies, creates a [`IndexLayer`] using the underlying [`TableProvider`]s and a [`FullTextDatabaseIndex`](search::generation::text_search::index::FullTextDatabaseIndex). If no full text search capabilities are needed it is not unnecessarily nested.
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
        federated_table: Arc<dyn FederatedTableProvider>,
        f: F,
    ) -> Option<ChangesStream>
    where
        F: Fn(&Arc<dyn DataConnector>, Arc<dyn FederatedTableProvider>) -> Option<ChangesStream>,
    {
        let table_provider = federated_table.try_table_provider_sync()?;
        let indexed_table = spice_table::nodes(table_provider.as_ref(), LayerWalk::Index)
            .find(|node| !node.indexes().is_empty())?;

        // This will process all `Index`s, including vector indexes if provided (i.e. from `EmbeddingConnector`).
        // This is required so that the index layer can be peeled in both cases —
        // whether or not there is an `EmbeddingConnector` underneath.
        let all_indexes = indexed_table.indexes().to_vec();

        // A full-text index written by this change stream must not defer its commits
        // to the sink write lifecycle: the two share one tantivy writer, so a window
        // commit would publish a partial refresh and a window rollback would discard
        // these change-stream documents. A layer's indexes come back
        // whatever was registered, unpeeled, so the tantivy tier can be reached only
        // indirectly — nested as the primary of a `CompoundSearchIndex` (the warm/external
        // full-text compound, registered in place of its tiers) or wrapped in a
        // `ChunkedSearchIndex` — so peel through those to reach it.
        for index in &all_indexes {
            mark_full_text_cdc_attached(index.as_any());
        }

        let indexes = Indexes::new(all_indexes);
        let ft = Arc::new(FederatedTable::Immediate(Arc::clone(indexed_table.below())));

        let stream = f(&self.inner_connector, ft)?;
        Some(
            stream
                .then(move |item| index_change_envelope(item, Arc::clone(&indexes)))
                .boxed(),
        )
    }
}

/// Marks the [`FullTextDatabaseIndex`] reachable from `index` as CDC-attached, so it never
/// opens a deferred-commit window that a change stream's writes could be rolled back out of.
///
/// `index` may not be a `FullTextDatabaseIndex` itself — the warm/external full-text compound
/// registers a `CompoundSearchIndex` in its place, with the tantivy tier nested as its primary
/// (or, in principle, wrapped in a `ChunkedSearchIndex`) — so this peels through the composing
/// index types that can hold one before giving up.
fn mark_full_text_cdc_attached(index: &dyn Any) {
    if let Some(full_text) = index.downcast_ref::<FullTextDatabaseIndex>() {
        full_text.mark_cdc_attached();
    } else if let Some(compound) = index.downcast_ref::<CompoundSearchIndex>() {
        mark_full_text_cdc_attached(compound.primary().as_any());
        mark_full_text_cdc_attached(compound.secondary().as_any());
    } else if let Some(chunked) = index.downcast_ref::<ChunkedSearchIndex>() {
        mark_full_text_cdc_attached(chunked.inner().as_any());
    }
}

#[deny(clippy::missing_trait_methods)]
#[async_trait]
impl DataConnector for FullTextConnector {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(
        &self,
        dataset: &DatasetSpec,
    ) -> DataConnectorResult<Arc<dyn TableProvider>> {
        let inner = self.inner_connector.read_provider(dataset).await?;
        add_full_text_search_to_table(&inner, &dataset.columns, &dataset.name)
            .map(|idx| idx as Arc<dyn TableProvider>)
            .map_err(|e| DataConnectorError::InvalidConfiguration {
                dataconnector: dataset.source().to_string(),
                message: e.to_string(),
                connector_component: dataset.into(),
                source: e,
            })
    }

    async fn read_write_provider(
        &self,
        dataset: &DatasetSpec,
    ) -> Option<DataConnectorResult<Arc<dyn TableProvider>>> {
        match self.inner_connector.read_write_provider(dataset).await {
            Some(Ok(inner)) => Some(
                add_full_text_search_to_table(&inner, &dataset.columns, &dataset.name)
                    .map(|idx| idx as Arc<dyn TableProvider>)
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
        dataset: &DatasetSpec,
    ) -> Option<DataConnectorResult<Arc<dyn TableProvider>>> {
        self.inner_connector.metadata_provider(dataset).await
    }

    async fn register_object_stores(
        &self,
        dataset: &DatasetSpec,
        runtime_env: &Arc<datafusion::execution::runtime_env::RuntimeEnv>,
    ) -> DataConnectorResult<()> {
        self.inner_connector
            .register_object_stores(dataset, runtime_env)
            .await
    }

    fn initialization(&self) -> ComponentInitialization {
        self.inner_connector.initialization()
    }

    fn metrics_provider(&self) -> Option<Arc<dyn MetricsProvider>> {
        self.inner_connector.metrics_provider()
    }

    async fn on_accelerator_setup(
        &self,
        dataset: &DatasetSpec,
        accelerator: &mut dyn AcceleratorSetup,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.inner_connector
            .on_accelerator_setup(dataset, accelerator)
            .await
    }

    async fn on_accelerated_table_registration(
        &self,
        dataset: &DatasetSpec,
        accelerated_table: &mut dyn RegisteredAcceleratedTable,
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

    fn supports_durable_write_back_delivery(&self) -> bool {
        self.inner_connector.supports_durable_write_back_delivery()
    }

    fn changes_stream(
        &self,
        federated_table: Arc<dyn FederatedTableProvider>,
        dataset: &DatasetSpec,
    ) -> Option<ChangesStream> {
        self.with_indexed_stream(federated_table, |inner, ft| {
            inner.changes_stream(ft, dataset)
        })
    }

    fn supports_append_stream(&self) -> bool {
        self.inner_connector.supports_append_stream()
    }

    fn append_stream(
        &self,
        federated_table: Arc<dyn FederatedTableProvider>,
    ) -> Option<ChangesStream> {
        self.with_indexed_stream(federated_table, |inner, ft| inner.append_stream(ft))
    }

    fn initialization_for_dataset(
        &self,
        dataset: &DatasetSpec,
    ) -> crate::component::ComponentInitialization {
        self.inner_connector.initialization_for_dataset(dataset)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::record_batch;
    use arrow::util::pretty::pretty_format_batches;
    use datafusion::datasource::MemTable;
    use futures::TryStreamExt;
    use search::index::SearchIndex;
    use search::index::compound::CompoundReadMode;
    use spice_table::{Index, WriteWindow};

    fn test_table() -> Arc<dyn TableProvider> {
        let batch = record_batch!(("id", Int32, [1]), ("content", Utf8, ["seed"]))
            .expect("failed to create test batch");
        Arc::new(
            MemTable::try_new(batch.schema(), vec![vec![batch]])
                .expect("failed to create test table"),
        )
    }

    fn full_text_tier() -> FullTextDatabaseIndex {
        FullTextDatabaseIndex::try_new(
            test_table(),
            vec!["content".to_string()],
            Some(vec!["id".to_string()]),
            None,
            &["content".to_string()],
        )
        .expect("failed to create FullTextDatabaseIndex")
    }

    /// Regression test for #12061: the warm/external full-text tier registers a
    /// `CompoundSearchIndex` in place of its tiers, so `IndexLayer::get_all_indexes`
    /// never surfaces the nested `FullTextDatabaseIndex` directly. `mark_full_text_cdc_attached`
    /// has to peel through the compound to reach it, or the tantivy tier keeps deferring
    /// commits and a failed refresh discards change-stream documents for good.
    #[tokio::test]
    async fn mark_full_text_cdc_attached_reaches_compound_primary() {
        let warm = full_text_tier();
        let compound = CompoundSearchIndex::try_new(
            Arc::new(warm.clone()) as Arc<dyn SearchIndex>,
            Arc::new(full_text_tier()) as Arc<dyn SearchIndex>,
            CompoundReadMode::PrimaryOnly,
        )
        .expect("two full-text tiers over the same table are compatible");

        mark_full_text_cdc_attached(&compound);

        // A sink-driven refresh opens a write window on both tiers.
        compound
            .on_write_start(WriteWindow::Append)
            .await
            .expect("on_write_start failed");

        // A change-stream document arrives while that window is open.
        compound
            .compute_index(vec![
                record_batch!(("id", Int32, [2]), ("content", Utf8, ["apple banana"]))
                    .expect("failed to create test batch"),
            ])
            .await
            .expect("compute_index failed");

        // The refresh then fails, discarding whatever the window staged.
        compound
            .on_write_failed()
            .await
            .expect("on_write_failed failed");

        warm.reader
            .reload()
            .expect("failed to reload the warm tier's reader");
        let search_index = warm
            .full_text_search_field_index("content")
            .expect("failed to create FullTextSearchFieldIndex");
        let rb = search_index
            .search("apple".to_string(), vec![], 1000)
            .expect("search failed")
            .try_collect::<Vec<_>>()
            .await
            .expect("failed to collect search results");
        let results = format!("{}", pretty_format_batches(&rb).expect("failed to format"));
        assert!(
            results.contains("apple banana"),
            "a change-stream document written through a compound must be committed, not staged in the failed window, got:\n{results}"
        );
    }
}
