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

//! Runtime integration for Elasticsearch as a vector engine.
//!
//! Constructs [`ElasticsearchIndex`] instances from dataset configuration
//! and wires them into the [`IndexedTableProvider`] pipeline.

use std::sync::Arc;

use arrow_schema::{DataType, Schema, SchemaRef};
use datafusion::datasource::TableProvider;
use datafusion::sql::TableReference;
use elasticsearch::Elasticsearch;
use search::generation::util::get_primary_keys;
use search::index::chunking::{CHUNKED_INDEX_CHUNK_KEY, ChunkedSearchIndex};
pub(crate) use search::index::elasticsearch::{
    ElasticsearchIndex, ElasticsearchIndexWriteMaintenance,
};
use search::metadata::{MetadataColumn, MetadataColumns};
use spicepod::{
    param::Params,
    semantic::{Column, ColumnLevelEmbeddingConfig, MetadataType},
    vector::VectorStore,
};
use tokio::sync::RwLock;

use crate::model::EmbeddingModelStore;
use runtime_parameters::typed::TypedParams as _;
use runtime_search::store_params::elasticsearch::{
    ElasticsearchVectorParams, EsDistanceMetric, build_write_options, merge_index_settings,
};
use runtime_secrets::{Secrets, get_params_with_secrets};

/// Attempt to construct an [`ElasticsearchIndex`] for the provided dataset/view on the given column.
#[expect(clippy::too_many_arguments)]
pub async fn try_from_table(
    ds_name: &TableReference,
    column: String,
    config: ColumnLevelEmbeddingConfig,
    vector_store_config: &VectorStore,
    inner_table_provider: &Arc<dyn TableProvider>,
    index_schema: SchemaRef,
    embedding_models: Arc<RwLock<EmbeddingModelStore>>,
    dataset_columns: Vec<Column>,
    secrets: Arc<RwLock<Secrets>>,
) -> Result<ElasticsearchIndex, Box<dyn std::error::Error + Send + Sync>> {
    let inner_schema = index_schema;
    let chunked = config.chunking.as_ref().is_some_and(|c| c.enabled);
    let primary_key = derive_primary_keys(&inner_schema, inner_table_provider, ds_name, &config)?;

    let params = get_store_params(vector_store_config, Arc::clone(&secrets)).await?;

    // Surface explicit "not yet supported" errors for params that require significant new
    // infrastructure (per-partition index routing / spill queues). Better to fail loudly
    // than silently ignore the config.
    params.validate()?;
    if !vector_store_config.partition_by.is_empty() {
        return Err(Box::<dyn std::error::Error + Send + Sync>::from(
            "`partition_by` is not yet supported for the Elasticsearch vector engine. Remove the parameter or use the S3 Vectors engine for partitioned workloads.",
        ));
    }

    let client = params.client()?;

    let es_index = params.index.clone().unwrap_or_else(|| {
        format!("{ds_name}-{column}-{}", config.model)
            .to_lowercase()
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '-' {
                    c
                } else {
                    '-'
                }
            })
            .collect()
    });

    let vector_field = params
        .vector_field
        .clone()
        .unwrap_or_else(|| format!("{column}_embedding"));

    // Determine text fields for full-text search from the dataset columns.
    // Match both Utf8 and LargeUtf8 since accelerated tables may use LargeUtf8.
    let text_fields: Vec<String> = dataset_columns
        .iter()
        .filter(|c| {
            inner_schema
                .field_with_name(&c.name)
                .is_ok_and(|f| matches!(f.data_type(), DataType::Utf8 | DataType::LargeUtf8))
        })
        .map(|c| c.name.clone())
        .collect();

    // Get the embedding model to determine dimension.
    // Clone the Arc before dropping the read lock to avoid holding it across .await.
    let model = {
        let model_read = embedding_models.read().await;
        let Some(model) = model_read.get(&config.model) else {
            return Err(Box::from(format!(
                "Cannot create Elasticsearch vector index for table '{}'. No embedding model named: '{}'.",
                ds_name, config.model
            )));
        };
        Arc::clone(model)
    };

    let dims = llms::embeddings::get_or_infer_size(&model)
        .await
        .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })?
        as i32;

    // Resolve optional vector-mapping tuning params.
    let mapping_opts = VectorMappingOptions {
        similarity: params
            .distance_metric
            .unwrap_or(EsDistanceMetric::Cosine)
            .as_str()
            .to_string(),
        hnsw_m: params.hnsw_m,
        hnsw_ef_construction: params.hnsw_ef_construction,
    };
    let index_settings = merge_index_settings(
        params.index_settings.as_ref(),
        params.number_of_shards,
        params.number_of_replicas,
        params.refresh_interval.as_deref(),
    );
    let write_options = build_write_options(
        params.bulk_load_refresh_interval.as_deref(),
        params.force_merge_after_write,
        params.force_merge_segments,
    )?;

    // A zero batch size would stall writes; fall back to the default like before.
    let batch_write_rows = if params.batch_write_rows > 0 {
        params.batch_write_rows
    } else {
        DEFAULT_BATCH_WRITE_ROWS
    };

    // Resolve spicepod `vectors: filterable | non-filterable` hints into
    // [`search::metadata::MetadataColumns`]. These shape the ES mapping
    // (`index: true` / `index: false`) so filterable fields participate in
    // query filters, and non-filterable fields are stored only in `_source`.
    let metadata_columns =
        es_metadata_columns(&dataset_columns, &inner_schema, &column, &vector_field);

    // Normalize the source schema to match what the Elasticsearch HTTP client actually produces.
    // Accelerated tables (e.g. DuckDB) may store columns with types that differ from what ES
    // returns, causing RecordBatch::try_new to fail when schemas are compared:
    //   - LargeUtf8 → Utf8  (ES always returns StringArray / Utf8)
    //   - FixedSizeList inner field → named "item", non-null Float32
    //     (build_dense_vector_array always uses Field::new("item", Float32, false))
    // Also, if the base table does not have the vector field (common when ES is the vector store),
    // explicitly append it so knn_hits_to_batch can extract it from ES _source.
    let mut source_fields: Vec<arrow_schema::FieldRef> = inner_schema
        .fields()
        .iter()
        .map(|f| {
            let normalized_type = normalize_es_data_type(f.data_type());
            if &normalized_type == f.data_type() {
                Arc::clone(f)
            } else {
                Arc::new(arrow_schema::Field::new(
                    f.name(),
                    normalized_type,
                    f.is_nullable(),
                ))
            }
        })
        .collect();

    // Append vector_field if not already present in the base schema.
    if inner_schema.field_with_name(&vector_field).is_err() {
        source_fields.push(Arc::new(arrow_schema::Field::new(
            &vector_field,
            DataType::FixedSizeList(
                Arc::new(arrow_schema::Field::new("item", DataType::Float32, false)),
                dims,
            ),
            true,
        )));
    }

    // The chunk key is not part of the base table schema (chunking injects it per chunk), so
    // append it explicitly when chunked. Without it in `source_schema`, `knn_hits_to_batch`
    // could not extract `_spice.chunk_id` from `_source` and would null-fill a non-nullable
    // field.
    if chunked
        && !source_fields
            .iter()
            .any(|f| f.name() == CHUNKED_INDEX_CHUNK_KEY)
    {
        source_fields.push(Arc::new(arrow_schema::Field::new(
            CHUNKED_INDEX_CHUNK_KEY,
            DataType::UInt64,
            false,
        )));
    }

    let source_schema = Arc::new(arrow_schema::Schema::new_with_metadata(
        source_fields,
        inner_schema.metadata().clone(),
    ));

    // Ensure the Elasticsearch index exists with the correct dense_vector mapping
    // for our vector field. This makes the ES vector engine "bring-your-own" friendly:
    // if the user has already created and populated the index, we leave it alone;
    // otherwise we create it so writes during refresh succeed.
    ensure_index_with_mapping(
        client.as_ref(),
        &es_index,
        VectorIndexMapping {
            vector_field: &vector_field,
            dims,
            text_fields: &text_fields,
            mapping_opts: &mapping_opts,
            metadata_columns: &metadata_columns,
            index_settings: index_settings.as_ref(),
            chunk_key: chunked.then_some(CHUNKED_INDEX_CHUNK_KEY),
        },
    )
    .await?;

    Ok(ElasticsearchIndex {
        client,
        es_index,
        embedded_column: column,
        vector_field,
        text_fields,
        primary_key,
        compute_query: model,
        dims,
        similarity: mapping_opts.similarity.clone(),
        source_schema,
        metadata_columns,
        batch_write_rows,
        write_maintenance: Arc::new(ElasticsearchIndexWriteMaintenance::new(write_options)),
    })
}

/// Default number of rows per Elasticsearch `_bulk` request.
const DEFAULT_BATCH_WRITE_ROWS: usize = 1000;

/// Vector-mapping tuning options (HNSW params + similarity).
#[derive(Debug, Clone)]
struct VectorMappingOptions {
    similarity: String,
    hnsw_m: Option<u32>,
    hnsw_ef_construction: Option<u32>,
}

/// Resolve spicepod `vectors` metadata hints from dataset columns into
/// [`MetadataColumns`]. Columns marked `filterable`/`non-filterable` contribute
/// to the ES mapping (index: true / index: false respectively). The embedded
/// column and the vector field itself are excluded.
fn es_metadata_columns(
    columns: &[Column],
    schema: &SchemaRef,
    embedded_column: &str,
    vector_field: &str,
) -> MetadataColumns {
    let cols: Vec<MetadataColumn> = columns
        .iter()
        .filter(|c| c.name != embedded_column && c.name != vector_field)
        .filter_map(|c| {
            let kind = c.as_vector_metadata()?;
            let field = schema.field_with_name(&c.name).ok()?.clone();
            Some(match kind {
                MetadataType::Filterable => MetadataColumn::Filterable(Arc::new(field)),
                MetadataType::NonFilterable => MetadataColumn::NonFilterable(Arc::new(field)),
            })
        })
        .collect();
    cols.into()
}

/// Create the ES index with a `dense_vector` mapping for `vector_field` if the index
/// does not already exist. If the index already exists, attempt to add or update the
/// mapping so the vector field is searchable via kNN. Mapping updates for existing
/// indices are best-effort: if `put_mapping` fails, the error is logged and the
/// function continues, which may leave the existing mapping unchanged.
async fn ensure_index_with_mapping(
    client: &dyn Elasticsearch,
    es_index: &str,
    mapping: VectorIndexMapping<'_>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let VectorIndexMapping {
        vector_field,
        dims,
        text_fields,
        mapping_opts,
        metadata_columns,
        index_settings,
        chunk_key,
    } = mapping;

    if dims <= 0 {
        return Err(format!(
            "Failed to prepare Elasticsearch index '{es_index}': embedding dimension must be positive, got {dims}. Check that the embedding model reports a valid output dimension."
        )
        .into());
    }

    let mut dense_vector = serde_json::json!({
        "type": "dense_vector",
        "dims": dims,
        "index": true,
        "similarity": mapping_opts.similarity,
    });
    if mapping_opts.hnsw_m.is_some() || mapping_opts.hnsw_ef_construction.is_some() {
        let mut index_options = serde_json::Map::new();
        index_options.insert("type".to_string(), serde_json::Value::String("hnsw".into()));
        if let Some(m) = mapping_opts.hnsw_m {
            index_options.insert("m".to_string(), serde_json::Value::from(m));
        }
        if let Some(ef) = mapping_opts.hnsw_ef_construction {
            index_options.insert("ef_construction".to_string(), serde_json::Value::from(ef));
        }
        if let Some(obj) = dense_vector.as_object_mut() {
            obj.insert(
                "index_options".to_string(),
                serde_json::Value::Object(index_options),
            );
        }
    }

    let mut properties = serde_json::Map::new();
    properties.insert(vector_field.to_string(), dense_vector);
    for t in text_fields {
        // Skip the vector field if a text column happens to share the name.
        if t == vector_field {
            continue;
        }
        properties.insert(
            t.clone(),
            serde_json::json!({
                "type": "text",
                "fields": { "keyword": { "type": "keyword", "ignore_above": 256 } },
            }),
        );
    }

    // Filterable metadata columns get mapped with `index: true` so they can
    // participate in ES query filters. Non-filterable columns are stored in
    // `_source` only (`index: false`, `doc_values: false`) — retrieved on hits
    // but never scanned for filtering. Columns already covered by the text
    // mapping above are skipped.
    for c in metadata_columns.iter() {
        let name = c.name().to_string();
        if name == vector_field || properties.contains_key(&name) {
            continue;
        }
        let mut mapping = arrow_type_to_es_mapping(c.field().data_type());
        if let Some(obj) = mapping.as_object_mut() {
            let indexable = matches!(c, MetadataColumn::Filterable(_));
            obj.insert("index".to_string(), serde_json::Value::Bool(indexable));
            if !indexable {
                // `doc_values` is not supported on `text` fields in Elasticsearch;
                // attempting to set it causes mapping creation/updates to fail.
                // For `text` mappings (and other field types that don't support
                // doc_values), skip the override — `_source` retrieval still works.
                let field_type = obj
                    .get("type")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("");
                if field_type != "text" {
                    obj.insert("doc_values".to_string(), serde_json::Value::Bool(false));
                }
            }
        }
        properties.insert(name, mapping);
    }

    // Explicitly map the chunk key when chunking is enabled. `_source` retrieval works via
    // dynamic mapping regardless; the explicit `index: false` mapping documents that it is a
    // never-filtered ordering key.
    if let Some(chunk_key) = chunk_key
        && !properties.contains_key(chunk_key)
    {
        properties.insert(
            chunk_key.to_string(),
            serde_json::json!({ "type": "long", "index": false }),
        );
    }

    let exists = client
        .index_exists(es_index)
        .await
        .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })?;

    let mapping_body = serde_json::json!({ "properties": properties });

    if exists {
        if let Err(e) = client.put_mapping(es_index, &mapping_body).await {
            // If the field already exists with an incompatible mapping, ES returns 400.
            // Surface the error but don't panic — a user may have pre-created the index
            // with a specific mapping they want preserved; log and proceed.
            tracing::warn!(
                "Elasticsearch index '{es_index}' exists but mapping update failed (continuing; \
                existing mapping will be used): {e}"
            );
        }
        return Ok(());
    }

    let mut create_body = serde_json::json!({ "mappings": { "properties": properties } });
    if let Some(settings) = index_settings
        && let Some(obj) = create_body.as_object_mut()
    {
        obj.insert("settings".to_string(), settings.clone());
    }
    match client.create_index(es_index, &create_body).await {
        Ok(_) => {
            tracing::info!(
                "Created Elasticsearch index '{es_index}' with dense_vector field '{vector_field}' (dims={dims})."
            );
            Ok(())
        }
        // TOCTOU: another runtime instance may have created the index between our
        // `index_exists` check and `create_index` call. Treat that as success and
        // best-effort apply the mapping update to match the `exists` branch above.
        Err(e) if is_index_already_exists_error(&e) => {
            tracing::info!(
                "Elasticsearch index '{es_index}' was created concurrently by another runtime instance; applying mapping updates."
            );
            if let Err(mapping_error) = client.put_mapping(es_index, &mapping_body).await {
                tracing::warn!(
                    "Elasticsearch index '{es_index}' exists after concurrent creation but mapping update failed (continuing; existing mapping will be used): {mapping_error}"
                );
            }
            Ok(())
        }
        Err(e) => Err(Box::new(e) as Box<dyn std::error::Error + Send + Sync>),
    }
}

struct VectorIndexMapping<'a> {
    vector_field: &'a str,
    dims: i32,
    text_fields: &'a [String],
    mapping_opts: &'a VectorMappingOptions,
    metadata_columns: &'a MetadataColumns,
    index_settings: Option<&'a serde_json::Value>,
    /// When chunking is enabled, the injected `_spice.chunk_id` field name. Mapped as a
    /// non-indexed `long` so `_source` retrieval works with clear intent (it is a primary-key
    /// ordering field, never filtered on).
    chunk_key: Option<&'a str>,
}

/// Check whether an error from `create_index` indicates the index already exists
/// (e.g. because another runtime instance created it concurrently).
pub(crate) fn is_index_already_exists_error(error: &(dyn std::error::Error + 'static)) -> bool {
    let mut current: Option<&(dyn std::error::Error + 'static)> = Some(error);
    while let Some(err) = current {
        let message = err.to_string();
        if message.contains("resource_already_exists_exception")
            || message.contains("already exists")
        {
            return true;
        }
        current = err.source();
    }
    false
}

/// Map an Arrow [`DataType`] to the best-effort Elasticsearch field mapping.
/// Unknown types fall back to `keyword` (lossless round-trip via `_source`).
fn arrow_type_to_es_mapping(dt: &DataType) -> serde_json::Value {
    match dt {
        DataType::Boolean => serde_json::json!({ "type": "boolean" }),
        DataType::Int8 | DataType::Int16 | DataType::UInt8 | DataType::UInt16 => {
            serde_json::json!({ "type": "integer" })
        }
        DataType::Int32 | DataType::UInt32 => serde_json::json!({ "type": "integer" }),
        DataType::Int64 | DataType::UInt64 => serde_json::json!({ "type": "long" }),
        DataType::Float32 => serde_json::json!({ "type": "float" }),
        DataType::Float64 => serde_json::json!({ "type": "double" }),
        DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _) => {
            serde_json::json!({ "type": "date" })
        }
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            serde_json::json!({
                "type": "text",
                "fields": { "keyword": { "type": "keyword", "ignore_above": 256 } },
            })
        }
        _ => serde_json::json!({ "type": "keyword" }),
    }
}

/// Normalize an Arrow [`DataType`] to match what the Elasticsearch HTTP client produces.
///
/// - `LargeUtf8` → `Utf8`: ES always deserializes strings as `StringArray` (Utf8).
/// - Floating-point `FixedSizeList` (the dense embedding vector) → `FixedSizeList` with
///   `Field::new("item", Float32, false)`: `build_dense_vector_array` always produces this
///   exact inner field.
/// - Integer `FixedSizeList` (e.g. the chunk `{start, end}` offset pair) keeps its inner
///   type; the reader decodes it back as integers. Coercing it to `Float32` here would make
///   the advertised offset column type diverge from what the reader produces.
pub(crate) fn normalize_es_data_type(dt: &DataType) -> DataType {
    match dt {
        DataType::LargeUtf8 | DataType::Utf8View => DataType::Utf8,
        DataType::FixedSizeList(inner, dim) => {
            let inner_type = match inner.data_type() {
                DataType::Float32 | DataType::Float64 => DataType::Float32,
                other => other.clone(),
            };
            DataType::FixedSizeList(
                Arc::new(arrow_schema::Field::new("item", inner_type, false)),
                *dim,
            )
        }
        other => other.clone(),
    }
}

async fn get_store_params(
    vector_store_config: &VectorStore,
    secrets: Arc<RwLock<Secrets>>,
) -> Result<ElasticsearchVectorParams, Box<dyn std::error::Error + Send + Sync>> {
    let params = vector_store_config
        .params
        .as_ref()
        .map(Params::as_string_map)
        .unwrap_or_default();

    let params_with_secrets = get_params_with_secrets(Arc::clone(&secrets), &params).await;

    Ok(ElasticsearchVectorParams::try_from_params(
        "Elasticsearch vector store",
        params_with_secrets,
        &secrets,
    )
    .await?)
}

/// Ensure the Elasticsearch index exists with `text` field mappings for the given fields.
/// Does NOT create a `dense_vector` field. Best-effort: if the index already exists, leaves it.
pub(crate) async fn ensure_index_with_text_mapping(
    client: &dyn Elasticsearch,
    es_index: &str,
    text_fields: &[String],
    index_settings: Option<&serde_json::Value>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut properties = serde_json::Map::new();
    for field in text_fields {
        properties.insert(
            field.clone(),
            serde_json::json!({
                "type": "text",
                "fields": { "keyword": { "type": "keyword", "ignore_above": 256 } }
            }),
        );
    }

    let exists = client
        .index_exists(es_index)
        .await
        .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })?;

    let mapping_body = serde_json::json!({ "properties": properties });

    if exists {
        if let Err(e) = client.put_mapping(es_index, &mapping_body).await {
            tracing::warn!(
                "Elasticsearch FTS index '{es_index}' exists but mapping update failed (continuing): {e}"
            );
        }
        return Ok(());
    }

    let mut create_body = serde_json::json!({ "mappings": { "properties": properties } });
    if let Some(settings) = index_settings
        && let Some(obj) = create_body.as_object_mut()
    {
        obj.insert("settings".to_string(), settings.clone());
    }
    match client.create_index(es_index, &create_body).await {
        Ok(_) => {
            tracing::info!("Created Elasticsearch FTS index '{es_index}'.");
            Ok(())
        }
        Err(e) if is_index_already_exists_error(&e) => Ok(()),
        Err(e) => Err(Box::new(e) as Box<dyn std::error::Error + Send + Sync>),
    }
}

/// Resolve the primary key fields for the Elasticsearch index: configured `row_ids` or
/// the table's derived primary keys, normalized (`LargeUtf8` → `Utf8` to match what the
/// Elasticsearch HTTP client returns) and, when chunking is enabled, augmented with the
/// chunk key so the warm in-memory index can fall back onto Elasticsearch.
fn derive_primary_keys(
    inner_schema: &Schema,
    inner_table_provider: &Arc<dyn TableProvider>,
    ds_name: &TableReference,
    config: &ColumnLevelEmbeddingConfig,
) -> Result<Vec<arrow_schema::Field>, Box<dyn std::error::Error + Send + Sync>> {
    let primary_keys: Vec<String> = match &config.row_ids {
        Some(row_ids) => row_ids.clone(),
        None => get_primary_keys(inner_table_provider)
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })?,
    };

    if primary_keys.is_empty() {
        return Err(Box::<dyn std::error::Error + Send + Sync>::from(format!(
            "Failed to resolve primary key columns for dataset {ds_name}: no primary key columns were configured or derived."
        )));
    }

    // Normalize LargeUtf8 → Utf8 for primary key fields: the Elasticsearch HTTP client
    // always returns string data as Arrow Utf8 (StringArray), so the schema must match.
    let primary_key: Vec<_> = primary_keys
        .iter()
        .map(|c| {
            let (_, f) = inner_schema.column_with_name(c.as_str()).ok_or_else(|| {
                Box::<dyn std::error::Error + Send + Sync>::from(format!(
                    "Failed to configure primary key for dataset {ds_name}: column '{c}' does not exist in the dataset schema."
                ))
            })?;
            if f.data_type() == &DataType::LargeUtf8 {
                Ok(arrow_schema::Field::new(
                    f.name(),
                    DataType::Utf8,
                    f.is_nullable(),
                ))
            } else {
                Ok(f.clone())
            }
        })
        .collect::<Result<Vec<_>, Box<dyn std::error::Error + Send + Sync>>>()?;

    let chunked = config.chunking.as_ref().is_some_and(|c| c.enabled);
    if chunked {
        Ok(ChunkedSearchIndex::augment_primary_key(primary_key))
    } else {
        Ok(primary_key)
    }
}
