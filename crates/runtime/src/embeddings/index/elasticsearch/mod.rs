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

use arrow_schema::{DataType, SchemaRef};
use datafusion::datasource::TableProvider;
use datafusion::sql::TableReference;
use elasticsearch::{Client, ClientOptions, Elasticsearch};
use search::generation::util::get_primary_keys;
pub(crate) use search::index::elasticsearch::{
    ElasticsearchIndex, ElasticsearchIndexWriteMaintenance, ElasticsearchIndexWriteOptions,
};
use search::metadata::{MetadataColumn, MetadataColumns};
use spicepod::{
    param::Params,
    semantic::{Column, ColumnLevelEmbeddingConfig, MetadataType},
    vector::VectorStore,
};
use tokio::sync::RwLock;

use crate::{
    model::EmbeddingModelStore,
    parameters::{ParameterSpec, Parameters},
};
use runtime_secrets::{Secrets, get_params_with_secrets};

pub(crate) const PARAMETERS: &[ParameterSpec] = &[
    ParameterSpec::component("endpoint")
        .description("Elasticsearch cluster URL (e.g., https://localhost:9200)."),
    ParameterSpec::component("user")
        .description("Username for Elasticsearch authentication.")
        .secret(),
    ParameterSpec::component("pass")
        .description("Password for Elasticsearch authentication.")
        .secret(),
    ParameterSpec::component("index").description("Elasticsearch index name for storing vectors."),
    ParameterSpec::component("vector_field").description(
        "Name of the dense_vector field in Elasticsearch. Defaults to the embedding column name.",
    ),
    ParameterSpec::component("distance_metric")
        .description(
            "Vector similarity metric for kNN search. One of: cosine | l2_norm | dot_product | max_inner_product.",
        )
        .one_of(&["cosine", "l2_norm", "dot_product", "max_inner_product"]),
    ParameterSpec::component("hnsw_m").description(
        "HNSW graph parameter m (links per node). Higher = better recall, more memory. ES default: 16.",
    ),
    ParameterSpec::component("hnsw_ef_construction").description(
        "HNSW graph build parameter ef_construction (candidate list size at build time). ES default: 100.",
    ),
    ParameterSpec::runtime("client_timeout").description(
        "Total request timeout for the Elasticsearch HTTP client, in time unit format (e.g. 30s, 1m). Default: 30s.",
    ),
    ParameterSpec::runtime("connect_timeout").description(
        "Connect timeout for the Elasticsearch HTTP client, in time unit format (e.g. 10s). Default: 10s.",
    ),
    ParameterSpec::component("max_retries").description(
        "Maximum number of retry attempts for transient Elasticsearch errors (HTTP 429 / 5xx). Default: 3.",
    ),
    ParameterSpec::component("retry_initial_backoff").description(
        "Initial backoff duration between retries, in time unit format (e.g. 100ms, 1s). Default: 200ms.",
    ),
    ParameterSpec::component("batch_write_rows").description(
        "Maximum number of rows to include in a single Elasticsearch _bulk request. Used to control memory usage and payload size during writes. Default: 1000.",
    ),
    ParameterSpec::component("index_settings").description(
        "JSON object passed as Elasticsearch index settings when creating the index. Existing indexes are not recreated.",
    ),
    ParameterSpec::component("number_of_shards").description(
        "Elasticsearch number_of_shards index setting to use when creating the index. Existing indexes are not recreated.",
    ),
    ParameterSpec::component("number_of_replicas").description(
        "Elasticsearch number_of_replicas index setting to use when creating the index. Existing indexes are not recreated.",
    ),
    ParameterSpec::component("refresh_interval").description(
        "Elasticsearch refresh_interval index setting to use when creating the index. Existing indexes are not recreated.",
    ),
    ParameterSpec::component("bulk_load_refresh_interval").description(
        "Temporary Elasticsearch index.refresh_interval to apply before full/append writes, then restore afterward. Set to -1 to disable refresh during bulk loading.",
    ),
    ParameterSpec::component("force_merge_after_write")
        .description("Run Elasticsearch _forcemerge after full/append writes. Default: false.")
        .one_of(&["true", "false"]),
    ParameterSpec::component("force_merge_segments").description(
        "Maximum number of segments to use with _forcemerge after full/append writes. Setting this also enables force merge. Default when force_merge_after_write=true: 1.",
    ),
    ParameterSpec::component("partition_by")
        .description("Not yet supported for the Elasticsearch vector engine."),
    ParameterSpec::component("spill_writes")
        .description("Not yet supported for the Elasticsearch vector engine."),
];

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
    let primary_keys: Vec<String> = match config.row_ids.clone() {
        Some(row_ids) => row_ids,
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

    let params = get_store_params(vector_store_config, Arc::clone(&secrets)).await?;

    // Surface explicit "not yet supported" errors for params that require
    // significant new infrastructure (per-partition index routing / spill
    // queues). Better to fail loudly than silently ignore the config.
    if string_from_params(&params, "partition_by").is_some()
        || !vector_store_config.partition_by.is_empty()
    {
        return Err(Box::<dyn std::error::Error + Send + Sync>::from(
            "`partition_by` is not yet supported for the Elasticsearch vector engine. Remove the parameter or use the S3 Vectors engine for partitioned workloads.",
        ));
    }
    if string_from_params(&params, "spill_writes")
        .is_some_and(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "true" | "1" | "yes"))
    {
        return Err(Box::<dyn std::error::Error + Send + Sync>::from(
            "`spill_writes` is not yet supported for the Elasticsearch vector engine.",
        ));
    }

    let endpoint = string_from_params(&params, "endpoint").ok_or_else(|| {
        Box::<dyn std::error::Error + Send + Sync>::from(
            "Missing required parameter 'endpoint' for Elasticsearch vector engine.",
        )
    })?;

    let user = string_from_params(&params, "user");
    let pass = string_from_params(&params, "pass");

    let client_options = build_client_options(&params)?;
    let client: Arc<dyn Elasticsearch> = Arc::new(
        Client::new_with_options(endpoint, user, pass, &client_options)
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })?,
    );

    let es_index = string_from_params(&params, "index").map_or_else(
        || {
            format!("{}-{}-{}", ds_name, column, config.model)
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
        },
        ToString::to_string,
    );

    let vector_field = string_from_params(&params, "vector_field")
        .map_or_else(|| format!("{column}_embedding"), ToString::to_string);

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

    // Parse optional vector-mapping tuning params.
    let mapping_opts = parse_mapping_options(&params)?;
    let index_settings = parse_index_settings(&params)?;
    let write_options = parse_write_options(&params)?;

    // Parse optional batch write size.
    let batch_write_rows = string_from_params(&params, "batch_write_rows")
        .map(|s| {
            s.parse::<usize>()
                .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                    format!(
                        "Invalid value for Elasticsearch parameter 'batch_write_rows': '{s}': {e}"
                    )
                    .into()
                })
        })
        .transpose()?
        .filter(|n| *n > 0)
        .unwrap_or(DEFAULT_BATCH_WRITE_ROWS);

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
        source_schema,
        metadata_columns,
        batch_write_rows,
        write_maintenance: Arc::new(ElasticsearchIndexWriteMaintenance::new(write_options)),
    })
}

/// Default number of rows per Elasticsearch `_bulk` request.
const DEFAULT_BATCH_WRITE_ROWS: usize = 1000;

/// Parsed vector-mapping tuning options (HNSW params + similarity).
#[derive(Debug, Clone)]
struct VectorMappingOptions {
    similarity: String,
    hnsw_m: Option<u32>,
    hnsw_ef_construction: Option<u32>,
}

impl Default for VectorMappingOptions {
    fn default() -> Self {
        Self {
            similarity: "cosine".to_string(),
            hnsw_m: None,
            hnsw_ef_construction: None,
        }
    }
}

fn parse_mapping_options(
    params: &Parameters,
) -> Result<VectorMappingOptions, Box<dyn std::error::Error + Send + Sync>> {
    let mut opts = VectorMappingOptions::default();

    if let Some(s) = string_from_params(params, "distance_metric") {
        opts.similarity = match s.trim().to_ascii_lowercase().as_str() {
            "cosine" => "cosine".to_string(),
            "l2_norm" | "l2" | "euclidean" => "l2_norm".to_string(),
            "dot_product" | "dot" => "dot_product".to_string(),
            "max_inner_product" | "mip" => "max_inner_product".to_string(),
            other => {
                return Err(format!(
                    "Invalid Elasticsearch 'distance_metric': '{other}'. Expected one of: cosine | l2_norm | dot_product | max_inner_product."
                )
                .into());
            }
        };
    }

    opts.hnsw_m = parse_u32_param(params, "hnsw_m")?;
    opts.hnsw_ef_construction = parse_u32_param(params, "hnsw_ef_construction")?;

    Ok(opts)
}

fn parse_index_settings(
    params: &Parameters,
) -> Result<Option<serde_json::Value>, Box<dyn std::error::Error + Send + Sync>> {
    let mut settings = serde_json::Map::new();

    if let Some(raw) = string_from_params(params, "index_settings") {
        let value: serde_json::Value =
            serde_json::from_str(raw).map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                format!("Invalid JSON for Elasticsearch parameter 'index_settings': {e}").into()
            })?;
        let Some(obj) = value.as_object() else {
            return Err(
                "Invalid Elasticsearch parameter 'index_settings': expected a JSON object.".into(),
            );
        };
        settings.extend(obj.iter().map(|(k, v)| (k.clone(), v.clone())));
    }

    for key in ["number_of_shards", "number_of_replicas", "refresh_interval"] {
        if let Some(value) = string_from_params(params, key) {
            settings.insert(
                key.to_string(),
                serde_json::Value::String(value.to_string()),
            );
        }
    }

    if settings.is_empty() {
        Ok(None)
    } else {
        Ok(Some(serde_json::Value::Object(settings)))
    }
}

pub(crate) fn parse_index_settings_from_map(
    params: &std::collections::HashMap<String, String>,
) -> Result<Option<serde_json::Value>, Box<dyn std::error::Error + Send + Sync>> {
    let mut settings = serde_json::Map::new();

    if let Some(raw) = params.get("index_settings") {
        let value: serde_json::Value =
            serde_json::from_str(raw).map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                format!("Invalid JSON for Elasticsearch parameter 'index_settings': {e}").into()
            })?;
        let Some(obj) = value.as_object() else {
            return Err(
                "Invalid Elasticsearch parameter 'index_settings': expected a JSON object.".into(),
            );
        };
        settings.extend(obj.iter().map(|(k, v)| (k.clone(), v.clone())));
    }

    for key in ["number_of_shards", "number_of_replicas", "refresh_interval"] {
        if let Some(value) = params.get(key) {
            settings.insert(key.to_string(), serde_json::Value::String(value.clone()));
        }
    }

    if settings.is_empty() {
        Ok(None)
    } else {
        Ok(Some(serde_json::Value::Object(settings)))
    }
}

pub(crate) fn parse_write_options_from_map(
    params: &std::collections::HashMap<String, String>,
) -> Result<ElasticsearchIndexWriteOptions, Box<dyn std::error::Error + Send + Sync>> {
    let refresh_interval_during_write = params
        .get("bulk_load_refresh_interval")
        .map(String::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string);

    let force_merge_after_write = params
        .get("force_merge_after_write")
        .map(String::as_str)
        .map(parse_bool_value)
        .transpose()?
        .unwrap_or(false);
    let force_merge_segments = params
        .get("force_merge_segments")
        .map(String::as_str)
        .map(|value| parse_positive_u32_value(value, "force_merge_segments"))
        .transpose()?;

    Ok(ElasticsearchIndexWriteOptions {
        refresh_interval_during_write,
        force_merge_segments: match (force_merge_after_write, force_merge_segments) {
            (true, None) => Some(1),
            (_, Some(value)) => Some(value),
            (false, None) => None,
        },
    })
}

fn parse_write_options(
    params: &Parameters,
) -> Result<ElasticsearchIndexWriteOptions, Box<dyn std::error::Error + Send + Sync>> {
    let refresh_interval_during_write = string_from_params(params, "bulk_load_refresh_interval")
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string);

    let force_merge_after_write = string_from_params(params, "force_merge_after_write")
        .map(parse_bool_value)
        .transpose()?
        .unwrap_or(false);
    let force_merge_segments = string_from_params(params, "force_merge_segments")
        .map(|value| parse_positive_u32_value(value, "force_merge_segments"))
        .transpose()?;

    Ok(ElasticsearchIndexWriteOptions {
        refresh_interval_during_write,
        force_merge_segments: match (force_merge_after_write, force_merge_segments) {
            (true, None) => Some(1),
            (_, Some(value)) => Some(value),
            (false, None) => None,
        },
    })
}

fn parse_bool_value(value: &str) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    match value.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" => Ok(true),
        "false" | "0" | "no" => Ok(false),
        _ => Err(format!(
            "Invalid boolean value for Elasticsearch parameter 'force_merge_after_write': '{value}'. Expected true or false."
        )
        .into()),
    }
}

fn parse_positive_u32_value(
    value: &str,
    key: &str,
) -> Result<u32, Box<dyn std::error::Error + Send + Sync>> {
    let parsed: u32 = value
        .parse()
        .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
            format!("Invalid value for Elasticsearch parameter '{key}': '{value}': {e}").into()
        })?;
    if parsed == 0 {
        return Err(format!(
            "Invalid value for Elasticsearch parameter '{key}': expected a positive integer."
        )
        .into());
    }
    Ok(parsed)
}

fn parse_u32_param(
    params: &Parameters,
    key: &str,
) -> Result<Option<u32>, Box<dyn std::error::Error + Send + Sync>> {
    let Some(s) = string_from_params(params, key) else {
        return Ok(None);
    };
    let v: u32 = s
        .parse()
        .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
            format!("Invalid value for Elasticsearch parameter '{key}': '{s}': {e}").into()
        })?;
    Ok(Some(v))
}

fn parse_duration_param(
    params: &Parameters,
    key: &str,
) -> Result<Option<std::time::Duration>, Box<dyn std::error::Error + Send + Sync>> {
    let Some(s) = string_from_params(params, key) else {
        return Ok(None);
    };
    duration_parse::parse_duration(s).map(Some).map_err(
        |e| -> Box<dyn std::error::Error + Send + Sync> {
            format!("Invalid value for Elasticsearch parameter '{key}': '{s}': {e}").into()
        },
    )
}

pub(crate) fn build_client_options(
    params: &Parameters,
) -> Result<ClientOptions, Box<dyn std::error::Error + Send + Sync>> {
    let mut opts = ClientOptions::default();
    if let Some(d) = parse_duration_param(params, "client_timeout")? {
        opts.request_timeout = d;
    }
    if let Some(d) = parse_duration_param(params, "connect_timeout")? {
        opts.connect_timeout = d;
    }
    if let Some(n) = parse_u32_param(params, "max_retries")? {
        opts.retry.max_retries = n;
    }
    if let Some(d) = parse_duration_param(params, "retry_initial_backoff")? {
        opts.retry.initial_backoff = d;
    }
    Ok(opts)
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
/// - `FixedSizeList` with any inner field → `FixedSizeList` with `Field::new("item", Float32, false)`:
///   `build_dense_vector_array` always produces this exact inner field.
pub(crate) fn normalize_es_data_type(dt: &DataType) -> DataType {
    match dt {
        DataType::LargeUtf8 | DataType::Utf8View => DataType::Utf8,
        DataType::FixedSizeList(_, dim) => DataType::FixedSizeList(
            Arc::new(arrow_schema::Field::new("item", DataType::Float32, false)),
            *dim,
        ),
        other => other.clone(),
    }
}

pub(crate) fn string_from_params<'a>(p: &'a Parameters, key: &str) -> Option<&'a str> {
    p.get(key).expose().ok()
}

async fn get_store_params(
    vector_store_config: &VectorStore,
    secrets: Arc<RwLock<Secrets>>,
) -> Result<Parameters, Box<dyn std::error::Error + Send + Sync>> {
    let params = vector_store_config
        .params
        .as_ref()
        .map(Params::as_string_map)
        .unwrap_or_default();

    let params_with_secrets = get_params_with_secrets(Arc::clone(&secrets), &params).await;

    let params = Parameters::try_new(
        "Elasticsearch vector store",
        params_with_secrets.into_iter().collect(),
        "elasticsearch",
        Arc::clone(&secrets),
        PARAMETERS,
    )
    .await?;

    Ok(params)
}

/// Build an Elasticsearch client from a raw params map.
/// Used by the FTS connector path which reads params from `dataset.params`.
pub(crate) fn get_fts_client(
    params: &std::collections::HashMap<String, String>,
) -> Result<Arc<dyn Elasticsearch>, Box<dyn std::error::Error + Send + Sync>> {
    let endpoint = params.get("endpoint").ok_or_else(|| {
        Box::<dyn std::error::Error + Send + Sync>::from(
            "Missing required parameter 'endpoint' for Elasticsearch FTS.",
        )
    })?;
    let user = params
        .get("user")
        .map(String::as_str)
        .map(ToString::to_string);
    let pass = params
        .get("pass")
        .map(String::as_str)
        .map(ToString::to_string);

    let mut opts = ClientOptions::default();
    if let Some(s) = params.get("client_timeout")
        && let Ok(d) = duration_parse::parse_duration(s)
    {
        opts.request_timeout = d;
    }
    if let Some(s) = params.get("connect_timeout")
        && let Ok(d) = duration_parse::parse_duration(s)
    {
        opts.connect_timeout = d;
    }

    let client = Client::new_with_options(endpoint, user.as_deref(), pass.as_deref(), &opts)
        .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })?;
    Ok(Arc::new(client) as Arc<dyn Elasticsearch>)
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

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::HashMap;

    #[test]
    fn parse_index_settings_from_map_combines_json_and_shortcuts() {
        let params = HashMap::from([
            (
                "index_settings".to_string(),
                r#"{"analysis":{"analyzer":{"default":{"type":"standard"}}}}"#.to_string(),
            ),
            ("number_of_shards".to_string(), "2".to_string()),
            ("number_of_replicas".to_string(), "0".to_string()),
            ("refresh_interval".to_string(), "30s".to_string()),
        ]);

        let settings = parse_index_settings_from_map(&params)
            .expect("valid index settings should parse")
            .expect("settings should be present");

        assert_eq!(
            settings,
            serde_json::json!({
                "analysis": {"analyzer": {"default": {"type": "standard"}}},
                "number_of_shards": "2",
                "number_of_replicas": "0",
                "refresh_interval": "30s",
            })
        );
    }

    #[test]
    fn parse_write_options_from_map_enables_force_merge_with_default_segments() {
        let params = HashMap::from([
            ("bulk_load_refresh_interval".to_string(), "-1".to_string()),
            ("force_merge_after_write".to_string(), "true".to_string()),
        ]);

        let options =
            parse_write_options_from_map(&params).expect("valid write options should parse");

        assert_eq!(options.refresh_interval_during_write.as_deref(), Some("-1"));
        assert_eq!(options.force_merge_segments, Some(1));
    }

    #[test]
    fn parse_write_options_from_map_rejects_zero_force_merge_segments() {
        let params = HashMap::from([("force_merge_segments".to_string(), "0".to_string())]);

        let error = parse_write_options_from_map(&params)
            .expect_err("zero force_merge_segments should fail");

        assert!(error.to_string().contains("positive integer"));
    }
}
