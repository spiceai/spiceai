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
use crate::component::ComponentInitialization;
use crate::component::dataset::Dataset;
use crate::component::metrics::MetricsProvider;
use crate::dataconnector::DataConnectorError;
use crate::model::EmbeddingModelStore;
use crate::secrets::Secrets;
use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use itertools::Itertools;
use llms::chunking::ChunkingConfig;
use runtime_datafusion_index::IndexedTableProvider;
use spicepod::component::embeddings::ColumnEmbeddingConfig;
use spicepod::vector::VectorStore;
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::dataconnector::DataConnector;
use crate::dataconnector::DataConnectorResult;
use crate::model::ENABLE_MODEL_SUPPORT_MESSAGE;

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
        if let Some(vector_engine) = &dataset.vectors {
            if vector_engine.enabled {
                return self
                    .wrap_table_as_index(dataset, Arc::clone(&inner_table_provider), vector_engine)
                    .await;
            }
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
                })
            })
            .collect_vec();
        let mut embeddings = dataset.embeddings.clone();
        embeddings.extend(from_columns);

        if embeddings.is_empty() {
            return Ok(inner_table_provider);
        }

        let embed_columns: HashMap<String, String, _> = embeddings
            .iter()
            .map(|e| (e.column.clone(), e.model.clone()))
            .collect::<HashMap<_, _>>();

        // Early check if embedding models are available.
        for (column, model) in &embed_columns {
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
                tracing::info!("S3 Vectors for dataset {} initializing...", dataset.name);
                let start = std::time::Instant::now();

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
                    use runtime_datafusion_index::Index;

                    use crate::embeddings::index::VectorIndex;

                    let vector_index = super::index::s3::try_from_dataset(
                        &dataset.name,
                        column,
                        config,
                        vector_store,
                        Arc::clone(&inner_table_provider),
                        Arc::clone(&self.embedding_models),
                        dataset.columns.clone(),
                        Arc::clone(&self.secrets),
                    )
                    .await
                    .map_err(|e| {
                        DataConnectorError::UnableToConnectInternal {
                            dataconnector: dataset.source().to_string(),
                            connector_component: dataset.into(),
                            source: e,
                        }
                    })?;

                    provider.underlying = (Arc::new(vector_index.clone()) as Arc<dyn VectorIndex>)
                        .augment_table(provider.underlying);
                    provider = provider.add_index(Arc::new(vector_index.clone()) as Arc<dyn Index>);
                }
                tracing::info!(
                    "S3 Vectors for dataset {} initialized in {:?}",
                    dataset.name,
                    start.elapsed()
                );
                Ok(Arc::new(provider))
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
}
