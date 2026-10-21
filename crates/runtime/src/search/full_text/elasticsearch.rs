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

//! [`DataConnector`] middleware that wires Elasticsearch BM25 full-text search
//! for datasets configured with `full_text_search: engine: elasticsearch` in
//! their Spicepod YAML.

use crate::dataconnector::ConnectorContext;
use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use data_components::cdc::{AccelerationContents, ChangesStream};
use datafusion::datasource::TableProvider;
use futures::StreamExt;
use tokio::sync::RwLock;

use crate::changes::{Indexes, index_change_envelope};
use crate::component::dataset::DatasetSpec;
use crate::component::{
    ComponentInitialization,
    dataset::{
        Dataset,
        acceleration::{RefreshMode, ZeroResultsAction},
    },
};
use crate::dataconnector::{DataConnector, DataConnectorError, DataConnectorResult};
use crate::federated::FederatedTable;
use crate::search::full_text::table::{add_compound_fts_to_table, dataset_attaches_stream};
use data_connector_api::accelerated::{AcceleratorSetup, RegisteredAcceleratedTable};
use data_connector_api::federated::FederatedTableProvider;
use runtime_metrics::component::MetricsProvider;
use runtime_parameters_typed::TypedParams as _;
use runtime_search::store_params::elasticsearch::{
    ElasticsearchFtsConfig, ElasticsearchFtsParams, normalize_elasticsearch_prefix,
};
use runtime_secrets::Secrets;

pub struct ElasticsearchFullTextConnector {
    inner_connector: Arc<dyn DataConnector>,
    fts_params: ElasticsearchFtsConfig,
}

impl std::fmt::Debug for ElasticsearchFullTextConnector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ElasticsearchFullTextConnector")
            .field("inner_connector", &self.inner_connector)
            .finish_non_exhaustive()
    }
}

impl ElasticsearchFullTextConnector {
    /// Construct the connector, resolving secrets from `dataset.full_text_search.params`.
    ///
    /// Required params (in `dataset.full_text_search.params`):
    /// - `endpoint` — Elasticsearch cluster URL
    /// - `index` — ES index name (optional; defaults to dataset name)
    ///
    /// Optional params:
    /// - `user`, `pass`
    /// - `client_timeout`, `connect_timeout`
    /// - `batch_write_rows`
    /// - `index_settings`, `number_of_shards`, `number_of_replicas`, `refresh_interval`
    /// - `bulk_load_refresh_interval`, `force_merge_after_write`, `force_merge_segments`
    pub async fn try_new(
        inner_connector: Arc<dyn DataConnector>,
        dataset: &Dataset,
        secrets: Arc<RwLock<Secrets>>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let fts_store = dataset
            .full_text_search
            .as_ref()
            .ok_or_else(|| -> Box<dyn std::error::Error + Send + Sync> {
                Box::from(format!(
                    "Dataset '{}': full_text_search block is required when fts_engine is 'elasticsearch'",
                    dataset.name
                ))
            })?;

        let raw_params: std::collections::HashMap<String, String> = fts_store
            .params
            .as_ref()
            .map(spicepod::param::Params::as_string_map)
            .unwrap_or_default();

        // Resolve secrets for all params.
        let resolved =
            runtime_secrets::get_params_with_secrets(Arc::clone(&secrets), &raw_params).await;

        let normalized = normalize_elasticsearch_prefix(resolved);

        let params = ElasticsearchFtsParams::try_from_params(
            &format!("Elasticsearch full-text search on dataset {}", dataset.name),
            normalized,
            &secrets,
        )
        .await
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

        let es_index = params
            .index
            .clone()
            .unwrap_or_else(|| dataset.name.to_string().replace('.', "-").to_lowercase());

        Ok(Self {
            inner_connector,
            fts_params: ElasticsearchFtsConfig { params, es_index },
        })
    }

    /// The pieces a change/append stream needs from an indexed table: the indexes
    /// to maintain, and the federated table *below* the index layer that the
    /// inner connector should stream from.
    ///
    /// Split out from the streaming itself because `changes_stream` is `async`
    /// while `append_stream` is not; both peel the same layer.
    #[expect(clippy::needless_pass_by_value)]
    fn indexed_stream_inputs(
        federated_table: Arc<dyn FederatedTableProvider>,
    ) -> Option<(Arc<Indexes>, Arc<dyn FederatedTableProvider>)> {
        let table_provider = federated_table.try_table_provider_sync()?;
        let indexed_table =
            spice_table::nodes(table_provider.as_ref(), spice_table::LayerWalk::Index)
                .find(|node| !node.indexes().is_empty())?;

        let below = Arc::new(FederatedTable::Immediate(Arc::clone(indexed_table.below())));
        Some((Indexes::new(indexed_table.indexes().to_vec()), below))
    }

    /// Maintains `indexes` from every envelope the source stream emits.
    fn maintaining(stream: ChangesStream, indexes: Arc<Indexes>) -> ChangesStream {
        stream
            .then(move |item| index_change_envelope(item, Arc::clone(&indexes)))
            .boxed()
    }
}

#[deny(clippy::missing_trait_methods)]
#[async_trait]
impl DataConnector for ElasticsearchFullTextConnector {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(
        &self,
        context: &dyn ConnectorContext,
        dataset: &DatasetSpec,
    ) -> DataConnectorResult<Arc<dyn TableProvider>> {
        let inner = self.inner_connector.read_provider(context, dataset).await?;
        add_compound_fts_to_table(
            inner,
            &dataset.columns,
            &dataset.name,
            &self.fts_params,
            &on_zero_results(dataset),
            dataset_attaches_stream(&self.inner_connector, dataset),
        )
        .await
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
        context: &dyn ConnectorContext,
        dataset: &DatasetSpec,
    ) -> Option<DataConnectorResult<Arc<dyn TableProvider>>> {
        match self
            .inner_connector
            .read_write_provider(context, dataset)
            .await
        {
            Some(Ok(inner)) => Some(
                add_compound_fts_to_table(
                    inner,
                    &dataset.columns,
                    &dataset.name,
                    &self.fts_params,
                    &on_zero_results(dataset),
                    dataset_attaches_stream(&self.inner_connector, dataset),
                )
                .await
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

    #[cfg(feature = "elasticsearch")]
    async fn on_accelerator_setup(
        &self,
        dataset: &DatasetSpec,
        accelerator: &mut dyn AcceleratorSetup,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.inner_connector
            .on_accelerator_setup(dataset, accelerator)
            .await?;

        Ok(())
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

    async fn write_back_deliverer(
        &self,
        context: &dyn ConnectorContext,
        dataset: &DatasetSpec,
    ) -> Option<
        data_connector_api::DataConnectorResult<Arc<dyn data_connector_api::WriteBackDeliverer>>,
    > {
        self.inner_connector
            .write_back_deliverer(context, dataset)
            .await
    }

    async fn changes_stream(
        &self,
        context: &dyn ConnectorContext,
        federated_table: Arc<dyn FederatedTableProvider>,
        dataset: &DatasetSpec,
        acceleration: AccelerationContents,
    ) -> Option<ChangesStream> {
        let (indexes, below) = Self::indexed_stream_inputs(federated_table)?;
        let stream = self
            .inner_connector
            .changes_stream(context, below, dataset, acceleration)
            .await?;
        Some(Self::maintaining(stream, indexes))
    }

    fn supports_append_stream(&self) -> bool {
        self.inner_connector.supports_append_stream()
    }

    fn append_stream(
        &self,
        federated_table: Arc<dyn FederatedTableProvider>,
    ) -> Option<ChangesStream> {
        let (indexes, below) = Self::indexed_stream_inputs(federated_table)?;
        let stream = self.inner_connector.append_stream(below)?;
        Some(Self::maintaining(stream, indexes))
    }

    fn initialization_for_dataset(&self, dataset: &DatasetSpec) -> ComponentInitialization {
        self.inner_connector.initialization_for_dataset(dataset)
    }
}

/// The dataset's configured `on_zero_results` acceleration setting, defaulting to
/// [`ZeroResultsAction::ReturnEmpty`] when no acceleration is configured. Drives the compound
/// full-text index's read mode: whether an empty warm-tier result falls back to Elasticsearch.
fn on_zero_results(dataset: &DatasetSpec) -> ZeroResultsAction {
    dataset
        .acceleration
        .as_ref()
        .map(|acceleration| acceleration.on_zero_results.clone())
        .unwrap_or_default()
}
