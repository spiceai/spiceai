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
use crate::accelerated_table::AcceleratedTable;
use crate::changes::Indexes;
use crate::changes::index_change_envelope;
use crate::component::ComponentInitialization;
use crate::component::dataset::Dataset;
use crate::component::metrics::MetricsProvider;
use crate::dataconnector::{DataConnector, DataConnectorError, DataConnectorResult};
use crate::embeddings::execution_plan::{
    compute_additional_embedding_columns, construct_record_batch,
};
use crate::embeddings::index::table::wrap_table_as_index;
use crate::federated_table::FederatedTable;
use crate::model::ENABLE_MODEL_SUPPORT_MESSAGE;
use crate::model::EmbeddingModelStore;
use crate::secrets::Secrets;
use async_trait::async_trait;
use data_components::cdc::{ChangeEnvelope, ChangesStream, StreamError, replace_change_batch_data};
use datafusion::datasource::TableProvider;
use futures::StreamExt;
use itertools::Itertools;
use runtime_datafusion_index::IndexedTableProvider;
use search::generation::text_search::index::FullTextDatabaseIndex;
use search::index::VectorScanTableProvider;
use spicepod::component::embeddings::ColumnEmbeddingConfig;
use std::any::Any;
use std::sync::Arc;
use tokio::sync::RwLock;

use super::table::EmbeddingTable;

pub struct EmbeddingConnector {
    inner_connector: Arc<dyn DataConnector>,
    embedding_models: Arc<RwLock<EmbeddingModelStore>>,
    secrets: Arc<RwLock<Secrets>>,
}

impl std::fmt::Debug for EmbeddingConnector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EmbeddingConnector")
            .field("inner_connector", &self.inner_connector)
            .field("embedding_models", &self.embedding_models)
            .finish_non_exhaustive()
    }
}

impl EmbeddingConnector {
    pub fn new(
        inner_connector: Arc<dyn DataConnector>,
        embedding_models: Arc<RwLock<EmbeddingModelStore>>,
        secrets: Arc<RwLock<Secrets>>,
    ) -> Self {
        Self {
            inner_connector,
            embedding_models,
            secrets,
        }
    }

    /// Wrap an existing [`TableProvider`] with a [`EmbeddingTable`] provider. If no embeddings
    /// are needed for the [`Dataset`], it is not unnecessarily nested.
    pub(crate) async fn wrap_table(
        &self,
        inner_table_provider: Arc<dyn TableProvider>,
        dataset: &Dataset,
    ) -> DataConnectorResult<Arc<dyn TableProvider>> {
        // Runtime isn't built with model support, but user specified a dataset to use embeddings.
        if !cfg!(feature = "models") {
            return Err(DataConnectorError::InvalidConfigurationNoSource {
                dataconnector: dataset.source().to_string(),
                message: format!(
                    "The dataset is configured with an embedding model, but the runtime is not built with model support.\n{ENABLE_MODEL_SUPPORT_MESSAGE}"
                ),
                connector_component: dataset.into(),
            });
        }

        // If the dataset is enabled for a vector engine, use this instead of JIT.
        if let Some(vector_engine) = &dataset.vectors
            && vector_engine.enabled
        {
            return wrap_table_as_index(
                &dataset.runtime().datafusion().ctx,
                &self.embedding_models,
                &self.secrets,
                &dataset.name,
                &dataset.columns,
                dataset.params.get("file_format").map(String::as_str),
                Arc::clone(&inner_table_provider),
                vector_engine,
            )
            .await
            .map_err(|e| DataConnectorError::InvalidConfiguration {
                dataconnector: dataset.source().to_string(),
                connector_component: dataset.into(),
                source: e,
                message: "Could not instantiate vector index".to_string(),
            });
        }

        // Add in embedding columns from `dataset.columns.embeddings`.
        let from_columns: Vec<ColumnEmbeddingConfig> = dataset
            .columns
            .iter()
            .flat_map(|column| {
                column.embeddings.iter().map(|e| ColumnEmbeddingConfig {
                    column: column.name.clone(),
                    model: e.model.clone(),
                    chunking: e.chunking.clone(),
                    primary_keys: e.row_ids.clone(),
                    vector_size: e.vector_size,
                })
            })
            .collect_vec();

        let mut embeddings: Vec<ColumnEmbeddingConfig> = dataset.embeddings.clone();
        embeddings.extend(from_columns);

        EmbeddingTable::from_spicepod_columns(
            inner_table_provider,
            embeddings,
            &self.embedding_models,
            dataset.params.get("file_format").map(String::as_str),
        )
        .await
        .map_err(|e| DataConnectorError::InvalidConfiguration {
            dataconnector: dataset.source().to_string(),
            message: e.to_string(),
            connector_component: dataset.into(),
            source: Box::new(e),
        })
    }

    async fn embed_change_envelope(
        maybe_envelope: Result<ChangeEnvelope, StreamError>,
        embedding_table: Arc<EmbeddingTable>,
    ) -> Result<ChangeEnvelope, StreamError> {
        let envelope = maybe_envelope.map_err(|e| {
            tracing::debug!("Error in underlying base stream: {e:?}");
            e
        })?;

        let (change_committer, batch) = envelope.into_parts();
        let data_batch = batch.data_batch();

        let embeddings = compute_additional_embedding_columns(
            &data_batch,
            &embedding_table.embedded_columns,
            Arc::clone(&embedding_table.embedding_models),
        )
        .await
        .map_err(|e| {
            tracing::debug!("Error when getting embedding columns: {e:?}");
            StreamError::Arrow(e.to_string())
        })?;

        for (column_name, embeddings) in &embeddings {
            tracing::trace!(
                "Embedding column computed: {column_name}, embeddings: {:?}",
                embeddings.len()
            );
        }

        let embedded_batch =
            construct_record_batch(&data_batch, &embedding_table.schema(), &embeddings)
                .map_err(|e| StreamError::Arrow(e.to_string()))?;

        let new_change_batch = replace_change_batch_data(&embedded_batch, &batch)
            .map_err(|e| StreamError::Arrow(e.to_string()))?;

        Ok(ChangeEnvelope::new(change_committer, new_change_batch))
    }
}

#[async_trait]
impl DataConnector for EmbeddingConnector {
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

    fn supports_changes_stream(&self) -> bool {
        self.inner_connector.supports_changes_stream()
    }

    fn changes_stream(
        &self,
        federated_table: Arc<FederatedTable>,
        dataset: &Dataset,
    ) -> Option<ChangesStream> {
        let table_provider = federated_table.try_table_provider_sync()?;
        if let Some(indexed_table) = table_provider
            .as_any()
            .downcast_ref::<IndexedTableProvider>()
            .cloned()
        {
            let Some(underlying_federated_table) =
                underlying_federated_table_for_indexed_table(&table_provider)
            else {
                return self
                    .inner_connector
                    .changes_stream(federated_table, dataset);
            };

            // Avoid reindexing full-text indexes.
            let indexes = Indexes::new(
                indexed_table
                    .get_all_indexes()
                    .into_iter()
                    .filter(|idx| {
                        idx.as_any()
                            .downcast_ref::<FullTextDatabaseIndex>()
                            .is_none()
                    })
                    .collect(),
            );

            let stream = self
                .inner_connector
                .changes_stream(underlying_federated_table, dataset)?
                .then(move |item| index_change_envelope(item, Arc::clone(&indexes)))
                .boxed();

            Some(stream)

        // `VectorScanTableProvider` is generally wrapped by a `IndexedTableProvider` (as above), but in the case both [`Self`] and the [`FullTextConnector`] exist, the latter will unwrap the `IndexedTableProvider` first. It will correctly handle indexing vector indexes as that point.
        } else if let Some(vector_scan) = table_provider
            .as_any()
            .downcast_ref::<VectorScanTableProvider>()
        {
            self.inner_connector.changes_stream(
                Arc::new(FederatedTable::Immediate(Arc::clone(
                    &vector_scan.table_provider,
                ))),
                dataset,
            )
        } else if let Some(embedding_table) =
            table_provider.as_any().downcast_ref::<EmbeddingTable>()
        {
            let embedding_table = Arc::new(embedding_table.clone());
            let underlying_table = Arc::clone(&embedding_table.base_table);
            let underlying_federated_table = Arc::new(FederatedTable::Immediate(underlying_table));

            Some(
                self.inner_connector
                    .changes_stream(underlying_federated_table, dataset)?
                    .then(move |item| {
                        Self::embed_change_envelope(item, Arc::clone(&embedding_table))
                    })
                    .boxed(),
            )
        } else {
            None
        }
    }

    fn supports_append_stream(&self) -> bool {
        self.inner_connector.supports_append_stream()
    }

    fn append_stream(&self, federated_table: Arc<FederatedTable>) -> Option<ChangesStream> {
        let table_provider = federated_table.try_table_provider_sync()?;

        if let Some(indexed_table) = table_provider
            .as_any()
            .downcast_ref::<IndexedTableProvider>()
            .cloned()
        {
            let indexed_table = Arc::new(indexed_table);
            let underlying_federated_table =
                underlying_federated_table_for_indexed_table(&table_provider)?;

            let indexes = Indexes::new(indexed_table.get_all_indexes());

            let stream = self
                .inner_connector
                .append_stream(underlying_federated_table)?
                .then(move |item| index_change_envelope(item, Arc::clone(&indexes)))
                .boxed();

            return Some(stream);
        }

        let embedding_table = Arc::new(
            table_provider
                .as_any()
                .downcast_ref::<EmbeddingTable>()?
                .clone(),
        );
        let underlying_table = Arc::clone(&embedding_table.base_table);
        let underlying_federated_table = Arc::new(FederatedTable::Immediate(underlying_table));

        let stream = self
            .inner_connector
            .append_stream(underlying_federated_table)?
            .then(move |item| Self::embed_change_envelope(item, Arc::clone(&embedding_table)))
            .boxed();

        Some(stream)
    }
}

fn underlying_federated_table_for_indexed_table(
    src_table_provider: &Arc<dyn TableProvider>,
) -> Option<Arc<FederatedTable>> {
    #[cfg(not(feature = "s3_vectors"))]
    let _ = src_table_provider;

    #[cfg(feature = "s3_vectors")]
    {
        if let Some(vector_scan) = src_table_provider
            .as_any()
            .downcast_ref::<search::index::VectorScanTableProvider>()
        {
            return underlying_federated_table_for_indexed_table(&vector_scan.table_provider);
        }

        if let Some(indexed_scan) = src_table_provider
            .as_any()
            .downcast_ref::<IndexedTableProvider>()
        {
            return underlying_federated_table_for_indexed_table(&indexed_scan.underlying);
        }

        Some(Arc::new(FederatedTable::Immediate(Arc::clone(
            src_table_provider,
        ))))
    }
    #[cfg(not(feature = "s3_vectors"))]
    {
        None
    }
}
