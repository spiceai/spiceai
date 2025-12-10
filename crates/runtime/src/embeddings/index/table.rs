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

use crate::model::EmbeddingModelStore;
use crate::secrets::Secrets;
use datafusion::datasource::TableProvider;
use datafusion::{prelude::SessionContext, sql::TableReference};
use spicepod::vector::VectorStore;
use std::sync::Arc;
use tokio::sync::RwLock;

use spicepod::semantic::Column;

#[cfg(feature = "s3_vectors")]
use {
    crate::embeddings::construct_chunker,
    arrow_schema::{Schema, SchemaRef},
    chunking::ChunkingConfig,
    datafusion::common::ToDFSchema as _,
    runtime_datafusion_index::{Index, IndexedTableProvider},
    runtime_table_partition::expression::partition_by_expressions,
    search::generation::util::get_primary_keys,
    search::index::s3_vectors::S3Vector,
    search::index::{
        SearchIndex, VectorIndex, VectorScanTableProvider, chunking::ChunkedSearchIndex,
    },
    search::metadata::MetadataColumn,
    snafu::ResultExt,
    spicepod::component::embeddings::EmbeddingChunkConfig,
    spicepod::semantic::MetadataType,
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
    let _ = (
        ctx,
        embedding_models,
        secrets,
        tbl,
        columns,
        file_format,
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
) -> Result<Arc<dyn TableProvider>, Box<dyn std::error::Error + Send + Sync>> {
    tracing::info!("S3 Vectors for table {tbl} initializing...");
    let start = std::time::Instant::now();

    let partition_by = get_partition_expressions(ctx, &inner_table_provider, vector_store)?;

    let embedding_columns: Vec<_> = columns
        .iter()
        .filter_map(|c| {
            c.embeddings
                .first()
                .map(|embed| (c.name.clone(), embed.clone()))
        })
        .collect();
    let mut provider = if let Some(indexed) = inner_table_provider
        .as_any()
        .downcast_ref::<IndexedTableProvider>()
    {
        indexed.clone()
    } else {
        IndexedTableProvider::new(Arc::clone(&inner_table_provider))
    };
    for (column, config) in embedding_columns {
        let (columns, index_schema) = if config.chunking.as_ref().is_some_and(|cfg| cfg.enabled) {
            updated_chunked_search_index_format(&inner_table_provider, columns, &column)
        } else {
            (columns.to_vec(), inner_table_provider.schema())
        };

        let vector_index = super::s3::try_from_table(
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

        if let Some(ref chunking) = config.chunking
            && chunking.enabled
        {
            provider = construct_s3_chunked_vector_index(
                provider,
                embedding_models,
                chunking,
                vector_index,
                config.model.as_str(),
                file_format,
            )
            .await?;
        } else {
            let idx = Arc::new(vector_index);
            let vector_index = Arc::clone(&idx) as Arc<dyn VectorIndex>;

            provider.underlying = Arc::new(
                VectorScanTableProvider::try_new(provider.underlying, &vector_index).boxed()?,
            ) as Arc<dyn TableProvider>;
            provider = provider.add_index(Arc::clone(&idx) as Arc<dyn Index>);
        }
    }
    tracing::info!(
        "S3 Vectors for table {tbl} initialized in {:?}",
        start.elapsed()
    );
    Ok(Arc::new(provider))
}

#[cfg(feature = "s3_vectors")]
async fn construct_s3_chunked_vector_index(
    mut provider: IndexedTableProvider,
    embedding_models: &Arc<RwLock<EmbeddingModelStore>>,
    chunking: &EmbeddingChunkConfig,
    mut vector_index: S3Vector,
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

    vector_index.primary_key = ChunkedSearchIndex::augment_primary_key(vector_index.primary_key);

    let idx = Arc::new(vector_index);
    let chunked_idx = Arc::new(ChunkedSearchIndex::new(
        idx as Arc<dyn SearchIndex>,
        chunker,
    ));

    if let Some(vector_index) = Arc::clone(&chunked_idx).as_vector_index() {
        provider.underlying =
            Arc::new(VectorScanTableProvider::try_new(provider.underlying, &vector_index).boxed()?)
                as Arc<dyn TableProvider>;
    }
    Ok(provider.add_index(Arc::clone(&chunked_idx) as Arc<dyn Index>))
}

/// Provide updated columns and underlying [`SchemaRef`] for a [`SearchIndex`] to use based off the index being chunked.
#[cfg(feature = "s3_vectors")]
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
