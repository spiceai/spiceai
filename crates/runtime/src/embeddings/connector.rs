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
use crate::dataconnector::DataConnector;
use crate::dataconnector::DataConnectorError;
use crate::dataconnector::DataConnectorResult;
use crate::embeddings::execution_plan::compute_additional_embedding_columns;
use crate::embeddings::execution_plan::construct_record_batch;
use crate::federated_table::FederatedTable;
use crate::model::ENABLE_MODEL_SUPPORT_MESSAGE;
use crate::model::EmbeddingModelStore;
use crate::secrets::Secrets;
use async_trait::async_trait;
use chunking::ChunkingConfig;
use data_components::cdc::ChangeEnvelope;
use data_components::cdc::ChangesStream;
use data_components::cdc::StreamError;
use data_components::cdc::replace_change_batch_data;
use datafusion::common::Column;
use datafusion::common::DFSchema;
use datafusion::common::ToDFSchema;
use datafusion::datasource::TableProvider;
use datafusion::prelude::SessionContext;
use datafusion_expr::Expr;
use datafusion_expr::expr::ScalarFunction;
use futures::StreamExt;
use itertools::Itertools;
use runtime_datafusion_index::IndexedTableProvider;
use runtime_table_partition::expression::CriterionFailedSnafu;
use runtime_table_partition::expression::{
    Criterion, Error as ValidationError, ValidationResult, partition_by_expressions,
};
use snafu::OptionExt;
use snafu::ensure;
use spicepod::component::embeddings::ColumnEmbeddingConfig;
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
                use datafusion::prelude::SessionContext;

                tracing::info!("S3 Vectors for dataset {} initializing...", dataset.name);
                let start = std::time::Instant::now();

                let partition_by =
                    get_and_validate_partition_by(dataset, vector_store, &SessionContext::new())?;

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

                    use crate::embeddings::index::{VectorIndex, VectorScanTableProvider};

                    let vector_index = super::index::s3::try_from_dataset(
                        &dataset.name,
                        column,
                        config,
                        vector_store,
                        Arc::clone(&inner_table_provider),
                        Arc::clone(&self.embedding_models),
                        dataset.columns.clone(),
                        Arc::clone(&self.secrets),
                        partition_by.clone(),
                    )
                    .await
                    .map_err(|e| {
                        DataConnectorError::UnableToConnectInternal {
                            dataconnector: dataset.source().to_string(),
                            connector_component: dataset.into(),
                            source: e,
                        }
                    })?;

                    let idx = Arc::new(vector_index);
                    // augment the previous underlying table provider with the vector index
                    // this will result in recursive augmentation of the underlying table for N embedding columns
                    provider.underlying = Arc::new(VectorScanTableProvider::new(
                        provider.underlying,
                        Arc::clone(&idx) as Arc<dyn VectorIndex>,
                    )) as Arc<dyn TableProvider>;
                    provider = provider.add_index(Arc::clone(&idx) as Arc<dyn Index>);
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

fn get_and_validate_partition_by(
    dataset: &Dataset,
    vector_store: &VectorStore,
    ctx: &SessionContext,
) -> DataConnectorResult<Vec<Expr>> {
    // Expression must use the bucket UDF with a column in the dataset
    struct BucketCriterion;

    impl Criterion for BucketCriterion {
        fn doc(&self) -> String {
            "expression must use bucket directly on a column in the dataset".to_string()
        }

        fn validate(&self, expr: &Expr, schema: &DFSchema) -> ValidationResult {
            let err = CriterionFailedSnafu {
                expr: expr.to_string(),
                criterion: self.doc(),
            };

            let Expr::ScalarFunction(ScalarFunction { func, args }) = expr else {
                return err.fail();
            };

            ensure!(func.name() == "bucket", err);

            let Expr::Column(Column { name, .. }) = args.get(1).with_context(|| err.clone())?
            else {
                return Err(ValidationError::InvalidExpression {
                    message: self.doc(),
                });
            };

            ensure!(schema.columns().iter().any(|c| c.name() == name), err);

            Ok(())
        }
    }

    let df_schema = &dataset
        .schema()
        .ok_or_else(|| DataConnectorError::UnableToGetSchema {
            dataconnector: dataset.source().to_string(),
            connector_component: dataset.into(),
            table_name: dataset.name.to_string(),
        })?
        .to_dfschema()
        .map_err(|e| DataConnectorError::InvalidConfigurationSourceOnly {
            dataconnector: dataset.source().to_string(),
            connector_component: dataset.into(),
            source: e.into(),
        })?;

    let partition_by = if vector_store.partition_by.is_empty() {
        vec![]
    } else {
        partition_by_expressions(&vector_store.partition_by, ctx, df_schema, &BucketCriterion)
            .map(|p| p.expressions)
            .map_err(|e| DataConnectorError::InvalidConfigurationSourceOnly {
                dataconnector: dataset.source().to_string(),
                connector_component: dataset.into(),
                source: e.into(),
            })?
    };

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

#[cfg(test)]
mod tests {
    use app::AppBuilder;
    use arrow_schema::{DataType, Field, Schema};

    use crate::component::dataset::builder::DatasetBuilder;

    use super::*;

    #[tokio::test]
    async fn validate_partition_by() {
        let spicepod_dataset =
            spicepod::component::dataset::Dataset::new("test".to_string(), "test".to_string());

        let app = AppBuilder::new("test")
            .with_dataset(spicepod_dataset.clone())
            .build();
        let rt = Arc::new(crate::Runtime::builder().build().await);

        let dataset = DatasetBuilder::try_from(spicepod_dataset)
            .expect("valid dataset builder")
            .with_app(Arc::new(app))
            .with_runtime(Arc::clone(&rt))
            .build()
            .expect("valid dataset")
            .with_schema(Arc::new(Schema::new(vec![Field::new(
                "col",
                DataType::Utf8,
                false,
            )])));

        let mut vector_store = VectorStore {
            enabled: true,
            engine: None,
            partition_by: vec!["bucket(100, col)".to_string()],
            params: None,
        };

        let exprs = get_and_validate_partition_by(&dataset, &vector_store, &rt.df.ctx)
            .expect("expressions");

        assert_eq!(exprs.len(), 1);
        assert!(matches!(exprs[0], Expr::ScalarFunction(_)));

        vector_store.partition_by = vec!["col < 10".to_string()];

        assert!(
            get_and_validate_partition_by(&dataset, &vector_store, &SessionContext::new()).is_err()
        );
    }
}
