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
use crate::embeddings::construct_chunker;
use crate::embeddings::execution_plan::{
    compute_additional_embedding_columns, construct_record_batch,
};
use crate::embeddings::index::VectorScanTableProvider;
#[cfg(feature = "s3_vectors")]
use crate::embeddings::index::s3::S3Vector;
use crate::federated_table::FederatedTable;
use crate::model::ENABLE_MODEL_SUPPORT_MESSAGE;
use crate::model::EmbeddingModelStore;
use crate::secrets::Secrets;
use async_trait::async_trait;
use chunking::ChunkingConfig;
use data_components::cdc::{ChangeEnvelope, ChangesStream, StreamError, replace_change_batch_data};
use datafusion::datasource::TableProvider;
use futures::StreamExt;
use itertools::Itertools;
use runtime_datafusion_index::Index;
use runtime_datafusion_index::IndexedTableProvider;
use search::{
    chunking::ChunkedSearchIndex,
    index::{SearchIndex, VectorIndex},
};
use snafu::ResultExt;
use spicepod::component::embeddings::ColumnEmbeddingConfig;
#[cfg(feature = "s3_vectors")]
use spicepod::component::embeddings::EmbeddingChunkConfig;
use spicepod::vector::VectorStore;
use std::any::Any;
use std::collections::HashMap;
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
            return self
                .wrap_table_as_index(dataset, Arc::clone(&inner_table_provider), vector_engine)
                .await;
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
        let mut embeddings = dataset.embeddings.clone();
        embeddings.extend(from_columns);

        if embeddings.is_empty() {
            return Ok(inner_table_provider);
        }

        let embed_columns: HashMap<String, ColumnEmbeddingConfig, _> = embeddings
            .iter()
            .map(|e| (e.column.clone(), e.clone()))
            .collect::<HashMap<_, _>>();

        // Early check if embedding models are available.
        for (column, config) in &embed_columns {
            let model = &config.model;
            if !self.embedding_models.read().await.contains_key(model) {
                return Err(DataConnectorError::InvalidConfigurationNoSource {
                    dataconnector: "EmbeddingConnector".to_string(),
                    message: format!(
                        "The dataset is configured with an embedding model '{model}' to embed column '{column}', but the model '{model}' is not defined in Spicepod (as an 'embeddings') or failed to load.\nFor details, visit: https://spiceai.org/docs/components/embeddings"
                    ),
                    connector_component: dataset.into(),
                });
            }
        }

        let embed_chunker_config: HashMap<String, ChunkingConfig> = embeddings
            .iter()
            .filter(|e| e.chunking.as_ref().is_some_and(|s| s.enabled))
            .filter_map(|e| {
                e.chunking.as_ref().map(|chunk_cfg| {
                    (
                        e.column.clone(),
                        ChunkingConfig {
                            target_chunk_size: chunk_cfg.target_chunk_size,
                            overlap_size: chunk_cfg.overlap_size,
                            trim_whitespace: chunk_cfg.trim_whitespace,
                            file_format: dataset.params.get("file_format").map(String::as_str),
                        },
                    )
                })
            })
            .collect::<HashMap<_, _>>();

        let embedding_table = EmbeddingTable::try_new(
            inner_table_provider,
            embed_columns,
            Arc::clone(&self.embedding_models),
            embed_chunker_config,
        )
        .await
        .map_err(|e| DataConnectorError::InvalidConfiguration {
            dataconnector: dataset.source().to_string(),
            message: e.to_string(),
            connector_component: dataset.into(),
            source: Box::new(e),
        })?;

        Ok(Arc::new(embedding_table) as Arc<dyn TableProvider>)
    }

    async fn wrap_table_as_index(
        &self,
        dataset: &Dataset,
        inner_table_provider: Arc<dyn TableProvider>,
        vector_store: &VectorStore,
    ) -> DataConnectorResult<Arc<dyn TableProvider>> {
        match vector_store.engine.as_deref() {
            #[cfg(feature = "s3_vectors")]
            Some("s3" | "s3_vectors") => {
                self.wrap_table_as_index_s3(dataset, inner_table_provider, vector_store)
                    .await
            }
            None => Err(DataConnectorError::InvalidConfigurationNoSource {
                dataconnector: dataset.source().to_string(),
                connector_component: dataset.into(),
                message: "No vector engine specified. Use '.datasets[].vectors.engine'".to_string(),
            }),
            Some(unknown_engine) => Err(DataConnectorError::InvalidConfigurationNoSource {
                dataconnector: dataset.source().to_string(),
                connector_component: dataset.into(),
                message: format!("Unknown vector engine '.vectors.engine: {unknown_engine}'"),
            }),
        }
    }

    #[cfg(feature = "s3_vectors")]
    async fn wrap_table_as_index_s3(
        &self,
        dataset: &Dataset,
        inner_table_provider: Arc<dyn TableProvider + 'static>,
        vector_store: &VectorStore,
    ) -> DataConnectorResult<Arc<dyn TableProvider>> {
        tracing::info!("S3 Vectors for dataset {} initializing...", dataset.name);
        let start = std::time::Instant::now();

        let partition_by =
            get_dataset_partition_expressions(dataset, &inner_table_provider, vector_store)?;

        let embedding_columns: Vec<_> = dataset
            .columns
            .iter()
            .filter_map(|c| {
                c.embeddings
                    .first()
                    .map(|embed| (c.name.clone(), embed.clone()))
            })
            .collect();
        let mut provider = IndexedTableProvider::new(Arc::clone(&inner_table_provider));
        for (column, config) in embedding_columns {
            let vector_index = super::index::s3::try_from_dataset(
                &dataset.name,
                column,
                config.clone(),
                vector_store,
                Arc::clone(&inner_table_provider),
                Arc::clone(&self.embedding_models),
                dataset.columns.clone(),
                Arc::clone(&self.secrets),
                partition_by.clone(),
            )
            .await
            .map_err(|e| DataConnectorError::UnableToConnectInternal {
                dataconnector: dataset.source().to_string(),
                connector_component: dataset.into(),
                source: e,
            })?;

            if let Some(ref chunking) = config.chunking
                && chunking.enabled
            {
                provider = self
                    .construct_s3_chunked_vector_index(
                        provider,
                        chunking,
                        vector_index,
                        dataset.params.get("file_format").map(String::as_str),
                    )
                    .await
                    .map_err(|e| DataConnectorError::UnableToConnectInternal {
                        dataconnector: dataset.source().to_string(),
                        connector_component: dataset.into(),
                        source: e,
                    })?;
            } else {
                let idx = Arc::new(vector_index);
                let vector_index = Arc::clone(&idx) as Arc<dyn VectorIndex>;

                provider.underlying = Arc::new(
                    VectorScanTableProvider::try_new(provider.underlying, &vector_index)
                        .boxed()
                        .map_err(|e| DataConnectorError::UnableToConnectInternal {
                            dataconnector: dataset.source().to_string(),
                            connector_component: dataset.into(),
                            source: e,
                        })?,
                ) as Arc<dyn TableProvider>;
                provider = provider.add_index(Arc::clone(&idx) as Arc<dyn Index>);
            }
        }
        tracing::info!(
            "S3 Vectors for dataset {} initialized in {:?}",
            dataset.name,
            start.elapsed()
        );
        Ok(Arc::new(provider))
    }

    #[cfg(feature = "s3_vectors")]
    async fn construct_s3_chunked_vector_index(
        &self,
        mut provider: IndexedTableProvider,
        chunking: &EmbeddingChunkConfig,
        vector_index: S3Vector,
        file_format: Option<&str>,
    ) -> Result<IndexedTableProvider, Box<dyn std::error::Error + Send + Sync>> {
        let chunker = construct_chunker(
            &vector_index.model_name,
            &ChunkingConfig {
                target_chunk_size: chunking.target_chunk_size,
                overlap_size: chunking.overlap_size,
                trim_whitespace: chunking.trim_whitespace,
                file_format,
            },
            &Arc::clone(&self.embedding_models),
        )
        .await
        .boxed()?;

        let additional_meta =
            ChunkedSearchIndex::additional_metadata(vector_index.search_column().as_str());
        let mut vector_index = vector_index.add_metadata(additional_meta);
        vector_index.primary_key =
            ChunkedSearchIndex::augment_primary_key(vector_index.primary_key);

        let idx = Arc::new(vector_index);
        let chunked_idx = Arc::new(ChunkedSearchIndex::new(
            idx as Arc<dyn SearchIndex>,
            chunker,
        ));

        if let Some(vector_index) = Arc::clone(&chunked_idx).as_vector_index() {
            provider.underlying = Arc::new(
                VectorScanTableProvider::try_new(provider.underlying, &vector_index).boxed()?,
            ) as Arc<dyn TableProvider>;
        }
        Ok(provider.add_index(Arc::clone(&chunked_idx) as Arc<dyn Index>))
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

#[cfg(feature = "s3_vectors")]
fn get_dataset_partition_expressions(
    dataset: &Dataset,
    inner_table_provider: &Arc<dyn TableProvider + 'static>,
    vector_store: &VectorStore,
) -> Result<Vec<datafusion_expr::Expr>, DataConnectorError> {
    use datafusion::common::ToDFSchema as _;
    use runtime_table_partition::expression::partition_by_expressions;

    let df_schema = &inner_table_provider.schema().to_dfschema().map_err(|e| {
        DataConnectorError::InvalidConfigurationSourceOnly {
            dataconnector: dataset.source().to_string(),
            connector_component: dataset.into(),
            source: e.into(),
        }
    })?;

    let partition_by = partition_by_expressions(
        &vector_store.partition_by,
        &dataset.runtime().df.ctx,
        df_schema,
    )
    .map(|p| p.expressions)
    .map_err(|e| DataConnectorError::InvalidConfigurationSourceOnly {
        dataconnector: dataset.source().to_string(),
        connector_component: dataset.into(),
        source: e.into(),
    })?;

    Ok(partition_by)
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

    fn changes_stream(&self, federated_table: Arc<FederatedTable>) -> Option<ChangesStream> {
        let table_provider = federated_table.try_table_provider_sync()?;
        if let Some(indexed_table) = table_provider
            .as_any()
            .downcast_ref::<IndexedTableProvider>()
            .cloned()
        {
            let indexed_table = Arc::new(indexed_table);
            let Some(underlying_federated_table) =
                underlying_federated_table_for_indexed_table(&table_provider)
            else {
                return self.inner_connector.changes_stream(federated_table);
            };

            let indexes = Indexes::new(indexed_table.get_all_indexes());

            let stream = self
                .inner_connector
                .changes_stream(underlying_federated_table)?
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
            .changes_stream(underlying_federated_table)?
            .then(move |item| Self::embed_change_envelope(item, Arc::clone(&embedding_table)))
            .boxed();

        Some(stream)
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
    #[cfg(feature = "s3_vectors")]
    {
        if let Some(vector_scan) = src_table_provider
            .as_any()
            .downcast_ref::<super::index::VectorScanTableProvider>()
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
