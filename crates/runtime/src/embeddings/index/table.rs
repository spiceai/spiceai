/*
Copyright 2025 The Spice.ai OSS Authors

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
#![allow(clippy::too_many_arguments)]

use crate::component::dataset::acceleration::ZeroResultsAction;
use crate::model::EmbeddingModelStore;
use crate::secrets::Secrets;
use datafusion::datasource::TableProvider;
use datafusion::{prelude::SessionContext, sql::TableReference};
#[cfg(feature = "models")]
use runtime_datafusion_udfs::embed::EMBED_UDF_NAME;
#[cfg(not(feature = "models"))]
const EMBED_UDF_NAME: &str = "embed";
use spicepod::vector::VectorStore;
use std::sync::Arc;
use tokio::sync::RwLock;

use spicepod::semantic::Column;

#[cfg(feature = "duckdb")]
use spicepod::component::embeddings::ColumnEmbeddingConfig;

#[cfg(feature = "elasticsearch")]
use search::metadata::MetadataColumns;
#[cfg(any(feature = "s3_vectors", feature = "elasticsearch"))]
use {
    crate::embeddings::construct_chunker,
    arrow_schema::{Schema, SchemaRef},
    chunking::ChunkingConfig,
    runtime_datafusion_index::{Index, IndexedTableProvider},
    runtime_search::embeddings::warm_index::with_memory_warm_index,
    search::index::{
        SearchIndex, VectorIndex, VectorScanTableProvider, chunking::ChunkedSearchIndex,
    },
    search::metadata::MetadataColumn,
    snafu::ResultExt,
    spicepod::component::embeddings::EmbeddingChunkConfig,
    spicepod::semantic::MetadataType,
};
#[cfg(feature = "s3_vectors")]
use {
    datafusion::common::ToDFSchema as _,
    runtime_table_partition::expression::partition_by_expressions,
    search::generation::util::get_primary_keys,
};

pub async fn wrap_table_as_index(
    ctx: &Arc<SessionContext>,
    embedding_models: &Arc<RwLock<EmbeddingModelStore>>,
    secrets: &Arc<RwLock<Secrets>>,
    tbl: &TableReference,
    columns: &[Column],
    file_format: Option<&str>,
    inner_table_provider: Arc<dyn TableProvider>,
    vector_store: &VectorStore,
    on_zero_results: Option<&ZeroResultsAction>,
) -> Result<Arc<dyn TableProvider>, Box<dyn std::error::Error + Send + Sync>> {
    let schema = inner_table_provider.schema();
    for c in columns {
        if schema.column_with_name(&c.name).is_none() {
            tracing::warn!(
                "The table {} is configured with column {} in the spicepod, but the column is not in the table's schema",
                tbl.to_string(),
                c.name
            );
        }
    }
    #[cfg(not(feature = "s3_vectors"))]
    let _ = ctx;
    #[cfg(not(any(feature = "s3_vectors", feature = "elasticsearch", feature = "duckdb")))]
    let _ = file_format;
    #[cfg(not(any(feature = "s3_vectors", feature = "elasticsearch")))]
    let _ = (secrets, on_zero_results);
    #[cfg(not(any(feature = "s3_vectors", feature = "elasticsearch", feature = "duckdb")))]
    let _ = (
        embedding_models,
        secrets,
        tbl,
        columns,
        inner_table_provider.as_ref(),
    );

    match vector_store.engine.as_deref() {
        #[cfg(feature = "s3_vectors")]
        Some("s3" | "s3_vectors") => {
            wrap_table_as_index_s3(
                ctx,
                embedding_models,
                secrets,
                tbl,
                columns,
                file_format,
                inner_table_provider,
                vector_store,
                on_zero_results,
            )
            .await
        }
        #[cfg(feature = "elasticsearch")]
        Some("elasticsearch" | "es") => {
            wrap_table_as_index_elasticsearch(
                ctx,
                embedding_models,
                secrets,
                tbl,
                columns,
                file_format,
                inner_table_provider,
                vector_store,
                on_zero_results,
            )
            .await
        }
        #[cfg(feature = "duckdb")]
        Some("duckdb") => {
            wrap_table_as_index_duckdb(
                embedding_models,
                tbl,
                columns,
                file_format,
                inner_table_provider,
            )
            .await
        }
        None => Err(Box::from(
            "No vector engine specified. Provide a vector engine under `.vectors.engine`."
                .to_string(),
        )),
        Some(unknown_engine) => Err(Box::from(format!(
            "Unknown vector engine '.vectors.engine: {unknown_engine}'"
        ))),
    }
}

#[cfg(feature = "duckdb")]
async fn wrap_table_as_index_duckdb(
    embedding_models: &Arc<RwLock<EmbeddingModelStore>>,
    tbl: &TableReference,
    columns: &[Column],
    file_format: Option<&str>,
    inner_table_provider: Arc<dyn TableProvider + 'static>,
) -> Result<Arc<dyn TableProvider>, Box<dyn std::error::Error + Send + Sync>> {
    tracing::info!("DuckDB vector engine for table {tbl} initializing...");
    let start = std::time::Instant::now();

    let embeddings = columns
        .iter()
        .flat_map(|column| {
            column
                .embeddings
                .iter()
                .map(|embedding| ColumnEmbeddingConfig {
                    column: column.name.clone(),
                    model: embedding.model.clone(),
                    chunking: embedding.chunking.clone(),
                    primary_keys: embedding.row_ids.clone(),
                    vector_size: embedding.vector_size,
                    aggregation: embedding.aggregation,
                    max_elements_per_row: embedding.max_elements_per_row,
                })
        })
        .collect::<Vec<_>>();

    let provider = runtime_search::embeddings::table::EmbeddingTable::from_spicepod_columns(
        inner_table_provider,
        embeddings,
        embedding_models,
        file_format,
    )
    .await
    .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })?;

    tracing::info!(
        "DuckDB vector engine for table {tbl} initialized in {:?}",
        start.elapsed()
    );
    Ok(provider)
}

#[cfg(feature = "s3_vectors")]
async fn wrap_table_as_index_s3(
    ctx: &Arc<SessionContext>,
    embedding_models: &Arc<RwLock<EmbeddingModelStore>>,
    secrets: &Arc<RwLock<Secrets>>,
    tbl: &TableReference,
    columns: &[Column],
    file_format: Option<&str>,
    inner_table_provider: Arc<dyn TableProvider + 'static>,
    vector_store: &VectorStore,
    on_zero_results: Option<&ZeroResultsAction>,
) -> Result<Arc<dyn TableProvider>, Box<dyn std::error::Error + Send + Sync>> {
    tracing::info!("S3 Vectors for table {tbl} initializing...");
    let start = std::time::Instant::now();

    let partition_by = get_partition_expressions(ctx, &inner_table_provider, vector_store)?;
    if let Some(partition_expr) = partition_by.first() {
        tracing::debug!("[S3Vectors][table={tbl}] partitioned by expression: {partition_expr:?}");
    } else {
        tracing::debug!("[S3Vectors][table={tbl}] No partitioning");
    }

    let Some(embed_udf) = ctx.state().scalar_functions().get(EMBED_UDF_NAME).cloned() else {
        return Err(Box::from(format!(
            "No scalar UDF '{EMBED_UDF_NAME}' found in context"
        )));
    };

    let embedding_columns: Vec<_> = columns
        .iter()
        .filter_map(|c| {
            c.embeddings
                .first()
                .map(|embed| (c.name.clone(), embed.clone()))
        })
        .collect();
    let mut provider =
        if let Some(indexed) = inner_table_provider.downcast_ref::<IndexedTableProvider>() {
            indexed.clone()
        } else {
            IndexedTableProvider::new(Arc::clone(&inner_table_provider))
        };
    for (column, config) in embedding_columns {
        let chunking = config.chunking.as_ref().filter(|cfg| cfg.enabled);
        let (columns, index_schema) = if chunking.is_some() {
            updated_chunked_search_index_format(&inner_table_provider, columns, &column)
        } else {
            (columns.to_vec(), inner_table_provider.schema())
        };

        let mut s3_index = super::s3::try_from_table(
            tbl,
            column,
            config.clone(),
            vector_store,
            // Primary key. Use override from spicepod, fallback to underlying [`TableProvider`].
            get_primary_keys(&inner_table_provider).boxed()?,
            index_schema,
            Arc::clone(embedding_models),
            columns,
            Arc::clone(secrets),
            partition_by.clone(),
        )
        .await?;

        if chunking.is_some() {
            tracing::debug!(
                "[S3Vectors][table={tbl}] Chunking column {}",
                s3_index.embedded_column
            );
            s3_index.primary_key = ChunkedSearchIndex::augment_primary_key(s3_index.primary_key);
        }

        // The S3 index exposes its metadata columns in both its list and query plans,
        // so the warm index mirrors them — a fallback read then serves the same columns
        // a warm read does.

        let metadata_columns = s3_index.metadata_columns.clone();
        let embedder = Arc::clone(&s3_index.compute_query);
        let metric = s3_index.table.distance_metric.clone();
        let vector_index = with_memory_warm_index(
            tbl,
            Arc::new(s3_index) as Arc<dyn VectorIndex>,
            metadata_columns,
            embedder,
            &embed_udf,
            &config.model,
            metric.as_str(),
            on_zero_results,
        );

        if let Some(chunking) = chunking {
            provider = construct_chunked_vector_index(
                provider,
                embedding_models,
                chunking,
                vector_index as Arc<dyn SearchIndex>,
                config.model.as_str(),
                file_format,
            )
            .await?;
        } else {
            provider.underlying = Arc::new(
                VectorScanTableProvider::try_new(provider.underlying, &vector_index).boxed()?,
            ) as Arc<dyn TableProvider>;
            provider = provider.add_index(vector_index as Arc<dyn Index>);
        }
    }
    tracing::info!(
        "S3 Vectors for table {tbl} initialized in {:?}",
        start.elapsed()
    );
    Ok(Arc::new(provider))
}

/// Wrap `index` (whose primary key must already be augmented with the chunk key via
/// [`ChunkedSearchIndex::augment_primary_key`]) in a [`ChunkedSearchIndex`] and attach
/// it to `provider`.
#[cfg(any(feature = "s3_vectors", feature = "elasticsearch"))]
async fn construct_chunked_vector_index(
    mut provider: IndexedTableProvider,
    embedding_models: &Arc<RwLock<EmbeddingModelStore>>,
    chunking: &EmbeddingChunkConfig,
    index: Arc<dyn SearchIndex>,
    model_name: &str,
    file_format: Option<&str>,
) -> Result<IndexedTableProvider, Box<dyn std::error::Error + Send + Sync>> {
    let chunker = construct_chunker(
        model_name,
        &ChunkingConfig {
            target_chunk_size: chunking.target_chunk_size,
            overlap_size: chunking.overlap_size,
            trim_whitespace: chunking.trim_whitespace,
            file_format,
        },
        &Arc::clone(embedding_models),
    )
    .await
    .boxed()?;

    let chunked_idx = Arc::new(ChunkedSearchIndex::new(index, chunker));

    if let Some(vector_index) = Arc::clone(&chunked_idx).as_vector_index() {
        provider.underlying =
            Arc::new(VectorScanTableProvider::try_new(provider.underlying, &vector_index).boxed()?)
                as Arc<dyn TableProvider>;
    }
    Ok(provider.add_index(Arc::clone(&chunked_idx) as Arc<dyn Index>))
}

/// Provide updated columns and underlying [`SchemaRef`] for a [`SearchIndex`] to use based off the index being chunked.
#[cfg(any(feature = "s3_vectors", feature = "elasticsearch"))]
fn updated_chunked_search_index_format(
    inner_table_provider: &Arc<dyn TableProvider>,
    columns: &[Column],
    column: &str,
) -> (Vec<spicepod::semantic::Column>, SchemaRef) {
    let mut fields = inner_table_provider
        .schema()
        .fields()
        .iter()
        .cloned()
        .collect::<Vec<_>>();

    let mut columns = columns.to_vec();
    if let Some((_, f)) = inner_table_provider.schema().column_with_name(column) {
        // These are internal columns that won't exist in existing columns. No need to find & replace.
        // get search field as metadata column.
        let search_metadata =
            columns
                .iter()
                .find(|&c| c.name == column)
                .and_then(|c| match c.as_vector_metadata() {
                    Some(MetadataType::NonFilterable) => {
                        Some(MetadataColumn::NonFilterable(Arc::new(f.clone())))
                    }
                    Some(MetadataType::Filterable) => {
                        Some(MetadataColumn::Filterable(Arc::new(f.clone())))
                    }
                    _ => None,
                });

        for col in ChunkedSearchIndex::additional_metadata(column, search_metadata) {
            columns.push(
                spicepod::semantic::Column::new(col.name()).with_metadata(
                    [(
                        "vectors".to_string(),
                        serde_json::Value::String(col.type_display().to_string()),
                    )]
                    .into(),
                ),
            );
            fields.push(col.field());
        }
    }
    (columns, Arc::new(Schema::new(fields)))
}

#[cfg(feature = "s3_vectors")]
fn get_partition_expressions(
    ctx: &Arc<SessionContext>,
    inner_table_provider: &Arc<dyn TableProvider + 'static>,
    vector_store: &VectorStore,
) -> Result<Vec<datafusion_expr::Expr>, Box<dyn std::error::Error + Send + Sync>> {
    let df_schema = &inner_table_provider.schema().to_dfschema().boxed()?;

    let partition_by = partition_by_expressions(&vector_store.partition_by, ctx, df_schema)
        .boxed()?
        .into_iter()
        .map(|p| p.expression)
        .collect();

    Ok(partition_by)
}

#[cfg(feature = "elasticsearch")]
async fn wrap_table_as_index_elasticsearch(
    ctx: &Arc<SessionContext>,
    embedding_models: &Arc<RwLock<EmbeddingModelStore>>,
    secrets: &Arc<RwLock<Secrets>>,
    tbl: &TableReference,
    columns: &[Column],
    file_format: Option<&str>,
    inner_table_provider: Arc<dyn TableProvider + 'static>,
    vector_store: &VectorStore,
    on_zero_results: Option<&ZeroResultsAction>,
) -> Result<Arc<dyn TableProvider>, Box<dyn std::error::Error + Send + Sync>> {
    tracing::info!("Elasticsearch vector engine for table {tbl} initializing...");
    let start = std::time::Instant::now();

    let embedding_columns: Vec<_> = columns
        .iter()
        .filter_map(|c| {
            c.embeddings
                .first()
                .map(|embed| (c.name.clone(), embed.clone()))
        })
        .collect();

    let mut provider = if let Some(indexed) =
        inner_table_provider.downcast_ref::<runtime_datafusion_index::IndexedTableProvider>()
    {
        indexed.clone()
    } else {
        runtime_datafusion_index::IndexedTableProvider::new(Arc::clone(&inner_table_provider))
    };

    let Some(embed_udf) = ctx.state().scalar_functions().get(EMBED_UDF_NAME).cloned() else {
        return Err(Box::from(format!(
            "No scalar UDF '{EMBED_UDF_NAME}' found in context"
        )));
    };

    for (column, config) in embedding_columns {
        let chunking = config.chunking.as_ref().filter(|cfg| cfg.enabled);
        let (augmented_columns, index_schema) = match chunking {
            Some(_) => updated_chunked_search_index_format(&inner_table_provider, columns, &column),
            None => (columns.to_vec(), inner_table_provider.schema()),
        };

        let mut es_index = super::elasticsearch::try_from_table(
            tbl,
            column.clone(),
            config.clone(),
            vector_store,
            &inner_table_provider,
            Arc::clone(&index_schema),
            Arc::clone(embedding_models),
            augmented_columns,
            Arc::clone(secrets),
        )
        .await?;

        provider = if let Some(chunking) = chunking {
            tracing::debug!(
                "[Elasticsearch][table={tbl}] Chunking column {}",
                es_index.embedded_column
            );
            // The Elasticsearch chunked query plan omits the chunk key column, so the
            // fallback projection from a warm in-memory index onto Elasticsearch results
            // cannot be built — serve reads from Elasticsearch directly.
            tracing::debug!(
                "Not adding an in-memory warm vector index for table {tbl}: chunking is enabled on the Elasticsearch vector engine."
            );
            es_index.primary_key = ChunkedSearchIndex::augment_primary_key(es_index.primary_key);

            construct_chunked_vector_index(
                provider,
                embedding_models,
                chunking,
                Arc::new(es_index) as Arc<dyn SearchIndex>,
                config.model.as_str(),
                file_format,
            )
            .await?
        } else {
            // Unlike S3 Vectors, the Elasticsearch list & query plans do not expose
            // metadata columns, so the warm index must not store any — otherwise the
            // fallback projection onto the Elasticsearch results cannot be built.
            let vector_index = with_memory_warm_index(
                tbl,
                Arc::new(es_index.clone()) as Arc<dyn VectorIndex>,
                MetadataColumns::none(),
                Arc::clone(&es_index.compute_query),
                &embed_udf,
                &config.model,
                es_index.similarity.as_str(),
                on_zero_results,
            );

            provider.underlying = Arc::new(
                VectorScanTableProvider::try_new(provider.underlying, &vector_index).boxed()?,
            ) as Arc<dyn TableProvider>;
            provider.add_index(vector_index as Arc<dyn Index>)
        };
    }

    tracing::info!(
        "Elasticsearch vector engine for table {tbl} initialized in {:?}",
        start.elapsed()
    );
    Ok(Arc::new(provider))
}
