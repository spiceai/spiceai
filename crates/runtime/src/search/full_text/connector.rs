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
use std::any::Any;
use std::sync::Arc;

use crate::accelerated_table::AcceleratedTable;
use crate::changes::{Indexes, index_change_envelope};
use crate::component::dataset::acceleration::RefreshMode;
use crate::component::{ComponentInitialization, dataset::Dataset, metrics::MetricsProvider};
use crate::dataconnector::{DataConnector, DataConnectorError, DataConnectorResult};
use crate::federated_table::FederatedTable;
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

    pub(crate) async fn wrap_table(
        &self,
        inner_table_provider: Arc<dyn TableProvider>,
        dataset: &Dataset,
    ) -> DataConnectorResult<Arc<dyn TableProvider>> {
        let (search_fields, primary_key_overrides): (Vec<_>, Vec<_>) = dataset
            .columns
            .iter()
            .filter_map(|c| {
                if c.full_text_search.as_ref().is_some_and(|cfg| cfg.enabled) {
                    let primary_key_overrides = c
                        .full_text_search
                        .as_ref()
                        .and_then(|cfg| cfg.row_ids.clone());
                    Some((c.name.clone(), primary_key_overrides))
                } else {
                    None
                }
            })
            .unzip();

        if search_fields.is_empty() {
            return Ok(inner_table_provider);
        }

        let index = FullTextDatabaseIndex::try_new(
            Arc::clone(&inner_table_provider),
            search_fields.clone(),
            Self::warn_different_primary_keys(
                dataset.name.to_string().as_str(),
                primary_key_overrides,
                search_fields.as_slice(),
            ),
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

    // For all full text search columns, find the first with a non-null primary key override and
    // if there are multiple, warn if they are different.
    fn warn_different_primary_keys(
        ds_name: &str,
        sets: Vec<Option<Vec<String>>>,
        fields: &[String],
    ) -> Option<Vec<String>> {
        let mut first: Option<Vec<String>> = None;
        let cmp_idx = 0;
        for (i, s) in sets.into_iter().enumerate() {
            let Some(mut pks) = s else {
                continue;
            };
            pks.sort();

            // If not first primary key defined, check it matches previous. Otherwise set to be used for next comparison.
            if let Some(ref f) = first {
                if *pks != *f {
                    tracing::warn!(
                        "Dataset '{}' has different primary keys for different full-text search columns. Using first.\n  Column '{}'. Key: {}.\n  Column '{}'. Key: {}.",
                        ds_name,
                        fields[cmp_idx],
                        f.join(", "),
                        fields[i],
                        pks.join(", "),
                    );
                }
            } else {
                first = Some(pks.clone());
            }
        }

        first
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
