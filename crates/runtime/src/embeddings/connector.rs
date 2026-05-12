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
use crate::accelerated_table::{self, AcceleratedTable};
use crate::changes::Indexes;
use crate::changes::index_change_envelope;
use crate::component::ComponentInitialization;
use crate::component::dataset::Dataset;
#[cfg(feature = "duckdb")]
use crate::component::dataset::acceleration::Engine;
use crate::component::metrics::MetricsProvider;
#[cfg(feature = "duckdb")]
use crate::component::view::View;
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
use spicepod::semantic::Column;
#[cfg(feature = "duckdb")]
use spicepod::semantic::ColumnLevelEmbeddingConfig;
use spicepod::vector::VectorStore;
use std::any::Any;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

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
            #[cfg(feature = "duckdb")]
            if vector_engine.engine.as_deref() == Some("duckdb")
                && !dataset.acceleration.as_ref().is_some_and(|acceleration| {
                    acceleration.engine.to_unpartitioned() == Engine::DuckDB
                })
            {
                return Err(DataConnectorError::InvalidConfigurationSourceOnly {
                    dataconnector: dataset.source().to_string(),
                    connector_component: dataset.into(),
                    source: Box::<dyn std::error::Error + Send + Sync>::from(
                        "DuckDB vector engine requires DuckDB acceleration. Configure the dataset with `acceleration.engine: duckdb`.",
                    ),
                });
            }

            let mut provider = Arc::clone(&inner_table_provider);
            for (effective_vector_store, columns) in vector_index_groups(vector_engine, dataset) {
                provider = wrap_table_as_index(
                    &dataset.runtime().datafusion().ctx,
                    &self.embedding_models,
                    &self.secrets,
                    &dataset.name,
                    &columns,
                    dataset.params.get("file_format").map(String::as_str),
                    provider,
                    &effective_vector_store,
                )
                .await
                .map_err(|e| {
                    DataConnectorError::InvalidConfigurationSourceOnly {
                        dataconnector: dataset.source().to_string(),
                        connector_component: dataset.into(),
                        source: e,
                    }
                })?;
            }

            return Ok(provider);
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
                    aggregation: e.aggregation,
                    max_elements_per_row: e.max_elements_per_row,
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

        let (change_committer, batch, is_dataset_ready) = envelope.into_parts();
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

        Ok(ChangeEnvelope::new(
            change_committer,
            new_change_batch,
            is_dataset_ready,
        ))
    }
}

#[deny(clippy::missing_trait_methods)]
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

    async fn on_accelerator_setup(
        &self,
        dataset: &Dataset,
        builder: &mut accelerated_table::Builder,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.inner_connector
            .on_accelerator_setup(dataset, builder)
            .await?;

        #[cfg(feature = "duckdb")]
        if let Some(vector_engine) = duckdb_vector_store_for_accelerated_table(dataset) {
            let embedding_columns = duckdb_embedding_columns(dataset);
            if embedding_columns.is_empty() {
                return Ok(());
            }

            tracing::debug!(
                dataset = %dataset.name,
                columns = ?embedding_columns.iter().map(|(col, _)| col.as_str()).collect::<Vec<_>>(),
                "Wrapping accelerator with DuckDB HNSW vector indexes"
            );

            let accelerator = builder.get_accelerator();
            let indexed_accelerator =
                crate::embeddings::index::duckdb::wrap_accelerator_with_duckdb_vector_indexes(
                    &dataset.name,
                    embedding_columns,
                    &vector_engine,
                    accelerator,
                    Arc::clone(&self.embedding_models),
                    Arc::clone(&self.secrets),
                )
                .await?;
            builder.set_accelerator(indexed_accelerator);
            return Ok(());
        }

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
        let table_provider = federated_table.try_table_provider_sync()?;
        if let Some(indexed_table) = table_provider
            .as_any()
            .downcast_ref::<IndexedTableProvider>()
            .cloned()
        {
            let Some(underlying_federated_table) =
                underlying_federated_table_for_indexed_table(&table_provider)
            else {
                return self.inner_connector.changes_stream(
                    federated_table,
                    dataset,
                    accelerated_table_provider,
                    accelerator_write_mutex,
                    cpu_runtime,
                );
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
                .changes_stream(
                    underlying_federated_table,
                    dataset,
                    accelerated_table_provider,
                    accelerator_write_mutex,
                    cpu_runtime,
                )?
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
                accelerated_table_provider,
                accelerator_write_mutex,
                cpu_runtime,
            )
        } else if let Some(embedding_table) =
            table_provider.as_any().downcast_ref::<EmbeddingTable>()
        {
            let embedding_table = Arc::new(embedding_table.clone());
            let underlying_table = Arc::clone(&embedding_table.base_table);
            let underlying_federated_table = Arc::new(FederatedTable::Immediate(underlying_table));

            Some(
                self.inner_connector
                    .changes_stream(
                        underlying_federated_table,
                        dataset,
                        accelerated_table_provider,
                        accelerator_write_mutex,
                        cpu_runtime,
                    )?
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

#[cfg(feature = "duckdb")]
fn duckdb_vector_store_for_accelerated_table(dataset: &Dataset) -> Option<VectorStore> {
    if let Some(vector_engine) = &dataset.vectors
        && vector_engine.enabled
        && vector_engine.engine.as_deref() == Some("duckdb")
    {
        return Some(vector_engine.clone());
    }

    if !dataset.has_embeddings()
        || !dataset
            .acceleration
            .as_ref()
            .is_some_and(|acceleration| acceleration.engine.to_unpartitioned() == Engine::DuckDB)
    {
        return None;
    }

    let acceleration = dataset.acceleration.as_ref()?;
    crate::embeddings::index::duckdb::vector_store_from_embedding_params(&acceleration.params)
}

fn vector_index_groups(
    vector_store: &VectorStore,
    dataset: &Dataset,
) -> Vec<(VectorStore, Vec<Column>)> {
    let has_column_overrides = dataset.columns.iter().any(|column| {
        column
            .embeddings
            .first()
            .is_some_and(|embedding| embedding.engine.is_some() || embedding.params.is_some())
    });

    if !has_column_overrides {
        return vec![(vector_store.clone(), dataset.columns.clone())];
    }

    let mut groups: Vec<(VectorStore, Vec<Column>)> = Vec::new();
    for column in &dataset.columns {
        let Some(embedding) = column.embeddings.first() else {
            continue;
        };

        let effective_vector_store = vector_store_for_embedding(vector_store, embedding);
        let mut grouped_column = column.clone();
        grouped_column.embeddings = vec![embedding.clone()];

        if let Some((_, columns)) = groups
            .iter_mut()
            .find(|(group_vector_store, _)| group_vector_store == &effective_vector_store)
        {
            if let Some(candidate) = columns
                .iter_mut()
                .find(|candidate| candidate.name == grouped_column.name)
            {
                candidate.embeddings.clone_from(&grouped_column.embeddings);
            } else {
                columns.push(grouped_column);
            }
        } else {
            let mut columns = dataset.columns.clone();
            for candidate in &mut columns {
                candidate.embeddings.clear();
            }
            if let Some(candidate) = columns
                .iter_mut()
                .find(|candidate| candidate.name == grouped_column.name)
            {
                candidate.embeddings.clone_from(&grouped_column.embeddings);
            }
            groups.push((effective_vector_store, columns));
        }
    }

    groups
}

fn vector_store_for_embedding(
    vector_store: &VectorStore,
    embedding: &spicepod::semantic::ColumnLevelEmbeddingConfig,
) -> VectorStore {
    let mut effective = vector_store.clone();
    if let Some(engine) = embedding.engine.as_ref() {
        effective.engine = Some(engine.clone());
    }

    if let Some(params) = embedding.params.as_ref() {
        let mut merged = effective.params.clone().unwrap_or_default();
        spicepod::param::merge_params(&mut merged, Some(params));
        effective.params = if merged.data.is_empty() {
            None
        } else {
            Some(merged)
        };
    }

    effective
}

#[cfg(feature = "duckdb")]
fn duckdb_embedding_columns(dataset: &Dataset) -> Vec<(String, ColumnLevelEmbeddingConfig)> {
    let mut embedding_columns = dataset
        .embeddings
        .iter()
        .map(|embedding| {
            (
                embedding.column.clone(),
                ColumnLevelEmbeddingConfig {
                    model: embedding.model.clone(),
                    chunking: embedding.chunking.clone(),
                    row_ids: embedding.primary_keys.clone(),
                    vector_size: embedding.vector_size,
                    engine: None,
                    params: None,
                    aggregation: embedding.aggregation,
                    max_elements_per_row: embedding.max_elements_per_row,
                },
            )
        })
        .collect::<Vec<_>>();

    for column in &dataset.columns {
        // Must be `last()` to mimic what model `EmbeddingTable`'s HashMap ends up with.
        let Some(embedding) = column.embeddings.last() else {
            continue;
        };

        if let Some((_, existing)) = embedding_columns
            .iter_mut()
            .find(|(column_name, _)| column_name == &column.name)
        {
            *existing = embedding.clone();
        } else {
            embedding_columns.push((column.name.clone(), embedding.clone()));
        }
    }

    embedding_columns
}

#[cfg(feature = "duckdb")]
pub(crate) fn duckdb_vector_store_for_view(view: &View) -> Option<VectorStore> {
    if let Some(vector_engine) = &view.vectors
        && vector_engine.enabled
        && vector_engine.engine.as_deref() == Some("duckdb")
    {
        return Some(vector_engine.clone());
    }

    if !view.has_embeddings()
        || !view
            .acceleration
            .as_ref()
            .is_some_and(|acceleration| acceleration.engine.to_unpartitioned() == Engine::DuckDB)
    {
        return None;
    }

    let acceleration = view.acceleration.as_ref()?;
    crate::embeddings::index::duckdb::vector_store_from_embedding_params(&acceleration.params)
}

#[cfg(feature = "duckdb")]
pub(crate) fn duckdb_embedding_columns_from_view(
    view: &View,
) -> Vec<(String, ColumnLevelEmbeddingConfig)> {
    let mut embedding_columns = Vec::new();

    for column in &view.columns {
        let Some(embedding) = column.embeddings.last() else {
            continue;
        };

        if let Some((_, existing)) = embedding_columns
            .iter_mut()
            .find(|(column_name, _): &&mut (String, _)| column_name == &column.name)
        {
            *existing = embedding.clone();
        } else {
            embedding_columns.push((column.name.clone(), embedding.clone()));
        }
    }

    embedding_columns
}

/// Wraps the accelerator in `builder` with `DuckDB` HNSW vector indexes if the view
/// has both a `DuckDB` vector engine configured and at least one embedding column.
/// Returns `true` if the accelerator was wrapped, `false` otherwise.
#[cfg(feature = "duckdb")]
pub(crate) async fn try_wrap_view_accelerator_with_hnsw(
    view: &View,
    table: &datafusion::sql::TableReference,
    builder: &mut crate::accelerated_table::Builder,
) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    let Some(vector_engine) = duckdb_vector_store_for_view(view) else {
        return Ok(false);
    };

    let embedding_columns = duckdb_embedding_columns_from_view(view);
    if embedding_columns.is_empty() {
        return Ok(false);
    }

    tracing::debug!(
        view = %table,
        columns = ?embedding_columns.iter().map(|(col, _)| col.as_str()).collect::<Vec<_>>(),
        "Wrapping view accelerator with DuckDB HNSW vector indexes"
    );

    let accelerator = builder.get_accelerator();
    let indexed_accelerator =
        crate::embeddings::index::duckdb::wrap_accelerator_with_duckdb_vector_indexes(
            table,
            embedding_columns,
            &vector_engine,
            accelerator,
            view.runtime.embeds(),
            view.runtime.secrets(),
        )
        .await?;
    builder.set_accelerator(indexed_accelerator);

    Ok(true)
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
