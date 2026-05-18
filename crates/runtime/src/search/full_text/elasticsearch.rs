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

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use data_components::cdc::ChangesStream;
use datafusion::datasource::TableProvider;
use futures::StreamExt;
use runtime_datafusion_index::IndexedTableProvider;
use secrecy::ExposeSecret;
use tokio::sync::{Mutex, RwLock};

use crate::accelerated_table::{self, AcceleratedTable};
use crate::changes::{Indexes, index_change_envelope};
use crate::component::{
    ComponentInitialization,
    dataset::{Dataset, SchemaEvolution, acceleration::RefreshMode},
    metrics::MetricsProvider,
};
use crate::dataconnector::{DataConnector, DataConnectorError, DataConnectorResult};
use crate::federated_table::FederatedTable;
use crate::search::full_text::table::add_elasticsearch_fts_to_table;
use crate::search::util::find_concrete_table_provider;
use runtime_secrets::Secrets;

/// Resolved Elasticsearch FTS connection parameters.
#[derive(Clone)]
pub(crate) struct ElasticsearchFtsParams {
    /// Resolved (secrets-injected) params map.
    pub params: std::collections::HashMap<String, String>,
    /// Elasticsearch index name for full-text search documents.
    pub es_index: String,
    /// Maximum number of rows per bulk request.
    pub batch_write_rows: usize,
}

pub struct ElasticsearchFullTextConnector {
    inner_connector: Arc<dyn DataConnector>,
    fts_params: ElasticsearchFtsParams,
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
        let resolved = runtime_secrets::get_params_with_secrets(secrets, &raw_params).await;

        // Strip the `elasticsearch_` prefix so callers can use consistent prefixed params
        // (e.g. `elasticsearch_endpoint`, `elasticsearch_index`) matching the vector store convention.
        let params: std::collections::HashMap<String, String> = resolved
            .into_iter()
            .map(|(k, v)| {
                let key = k.strip_prefix("elasticsearch_").unwrap_or(&k).to_string();
                (key, v.expose_secret().to_string())
            })
            .collect();

        let es_index = params
            .get("index")
            .cloned()
            .unwrap_or_else(|| dataset.name.to_string().replace('.', "-").to_lowercase());

        let batch_write_rows = params
            .get("batch_write_rows")
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(1000);

        Ok(Self {
            inner_connector,
            fts_params: ElasticsearchFtsParams {
                params,
                es_index,
                batch_write_rows,
            },
        })
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

        let indexes = Indexes::new(indexed_table.get_all_indexes());
        let underlying_table = Arc::new(FederatedTable::Immediate(indexed_table.get_underlying()));

        let stream = f(&self.inner_connector, underlying_table)?;
        Some(
            stream
                .then(move |item| index_change_envelope(item, Arc::clone(&indexes)))
                .boxed(),
        )
    }
}

#[async_trait]
impl DataConnector for ElasticsearchFullTextConnector {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(
        &self,
        dataset: &Dataset,
    ) -> DataConnectorResult<Arc<dyn TableProvider>> {
        let inner = self.inner_connector.read_provider(dataset).await?;
        add_elasticsearch_fts_to_table(inner, &dataset.columns, &dataset.name, &self.fts_params)
            .await
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
                add_elasticsearch_fts_to_table(
                    inner,
                    &dataset.columns,
                    &dataset.name,
                    &self.fts_params,
                )
                .await
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

    async fn register_object_stores(
        &self,
        dataset: &Dataset,
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
        dataset: &Dataset,
        builder: &mut accelerated_table::Builder,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.inner_connector
            .on_accelerator_setup(dataset, builder)
            .await?;

        Ok(())
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

    fn supported_schema_evolution_modes(&self) -> &'static [SchemaEvolution] {
        self.inner_connector.supported_schema_evolution_modes()
    }

    fn supports_changes_stream(&self) -> bool {
        self.inner_connector.supports_changes_stream()
    }

    fn changes_stream(
        &self,
        federated_table: Arc<FederatedTable>,
        dataset: &Dataset,
        accelerated_table_provider: Arc<dyn TableProvider>,
        accelerator_write_mutex: Arc<Mutex<()>>,
        cpu_runtime: Option<tokio::runtime::Handle>,
    ) -> Option<ChangesStream> {
        self.with_indexed_stream(federated_table, |inner, table| {
            inner.changes_stream(
                table,
                dataset,
                Arc::clone(&accelerated_table_provider),
                Arc::clone(&accelerator_write_mutex),
                cpu_runtime.clone(),
            )
        })
    }

    fn supports_append_stream(&self) -> bool {
        self.inner_connector.supports_append_stream()
    }

    fn append_stream(&self, federated_table: Arc<FederatedTable>) -> Option<ChangesStream> {
        self.with_indexed_stream(federated_table, |inner, table| inner.append_stream(table))
    }
}
