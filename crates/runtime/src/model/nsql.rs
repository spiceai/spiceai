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

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    error::Error as StdError,
    fmt::Write,
    sync::Arc,
};

use async_openai::types::chat::{
    ChatCompletionRequestMessage, ChatCompletionRequestSystemMessageArgs,
};
use data_components::{
    DESCRIPTION_METADATA_KEY, FOREIGN_KEYS_METADATA_KEY, SOURCE_TYPE_METADATA_KEY,
};
use datafusion::{
    arrow::datatypes::Schema,
    common::{Constraint, Constraints, utils::quote_identifier},
    execution::FunctionRegistry,
    sql::TableReference,
};
use datafusion_functions_json::udfs::{
    json_as_text_udf, json_contains_udf, json_from_scalar_udf, json_get_array_udf,
    json_get_bool_udf, json_get_float_udf, json_get_int_udf, json_get_json_udf, json_get_str_udf,
    json_get_udf, json_length_udf, json_object_keys_udf,
};
use datafusion_table_providers::util::column_reference::ColumnReference;
use futures::{StreamExt, TryStreamExt};
use itertools::Itertools;
use runtime_datafusion::allowlist::ResolvedTableAwareAllowlist;
use runtime_datafusion_index::IndexedTableProvider;
use runtime_datafusion_udfs::{
    bucket::BUCKET_SCALAR_UDF_NAME,
    cosine_distance::COSINE_DISTANCE_UDF_NAME,
    digest_many::DIGEST_UDF_NAME,
    inner_product::INNER_PRODUCT_UDF_NAME,
    l2_distance::{L2_DISTANCE_UDF_NAME, L2_SQUARED_DISTANCE_UDF_NAME},
    l2_norm::L2_NORM_UDF_NAME,
    truncate::TRUNCATE_SCALAR_UDF_NAME,
};
use runtime_request_context::RequestContext;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use snafu::Snafu;
use spicepod::{component::function::Function, semantic::Column};
use tracing::Span;
use tracing_futures::Instrument;

#[cfg(feature = "models")]
use runtime_datafusion_udfs::{ai::AI_UDF_NAME, embed::EMBED_UDF_NAME};

use crate::{
    Runtime,
    component::column::full_text_search_config,
    datafusion::{
        SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA,
        pg_catalog::{COL_DESCRIPTION_UDF_NAME, OBJ_DESCRIPTION_UDF_NAME},
        request_context_extension::get_current_datafusion,
        udf::{UserFunctionInfo, effective_user_function_volatility, user_function_infos},
        udtf::{
            flatten_json::FLATTEN_JSON_UDTF_NAME,
            json_properties::FLATTEN_JSON_PROPERTIES_UDTF_NAME, json_tree::JSON_TREE_UDTF_NAME,
        },
    },
    embeddings::table::{EmbeddingInputMode, EmbeddingTable},
    embeddings::udtf::VECTOR_SEARCH_UDTF_NAME,
    search::{
        full_text::udtf::TEXT_SEARCH_UDTF_NAME, rerank::RERANK_UDTF_NAME, rrf::RRF_UDF_NAME,
        util::find_concrete_table_provider,
    },
    tools::{
        SpiceModelTool,
        builtin::{
            sample::{
                SampleTableMethod, SampleTableParams, distinct::DistinctColumnsParams,
                random::RandomSampleParams, tool::SampleDataTool,
            },
            table_schema::{TableSchemaTool, TableSchemaToolParams},
        },
        utils::tool_call_error_response,
    },
    udtfs::LIST_UDFS_UDTF_NAME,
};

#[cfg(not(feature = "models"))]
const AI_UDF_NAME: &str = "ai";
#[cfg(not(feature = "models"))]
const EMBED_UDF_NAME: &str = "embed";

pub(crate) const DEFAULT_NSQL_CONTEXT_SAMPLE_LIMIT: usize = 3;
pub(crate) const MAX_NSQL_CONTEXT_SAMPLE_LIMIT: usize = 100;

const DATA_SAMPLING_MAX_CONCURRENT: usize = 10;

const NSQL_CONTEXT_INSTRUCTIONS: [&str; 5] = [
    "Write SQL for the Spice runtime, which uses Apache DataFusion with the SQL parser configured for the PostgreSQL dialect.",
    "Return only valid SQL code, without markdown fences.",
    "Quote column names that contain capitals or special characters with double quotes.",
    "For tables with schemas and catalogs, use \"catalog\".\"schema\".\"table\", not \"catalog.schema.table\".",
    "Use table and column descriptions, primary keys, foreign keys, unique constraints, and indexes when choosing joins and filters.",
];

const NSQL_FUNCTION_CONTEXT_SUMMARY: &str = "Spice SQL runs on Apache DataFusion with the SQL parser configured for the PostgreSQL dialect. Standard DataFusion SQL functions are available. The Spice runtime also exposes these functions when useful for Text-to-SQL, including registered user-defined functions. Function references are filtered to functions registered in the current DataFusion context";

#[derive(Debug, Snafu)]
pub(crate) enum NsqlContextError {
    #[snafu(display("Unexpected internal error. App not prepared in runtime."))]
    AppNotPrepared,

    #[snafu(display("Unexpected internal error. Model '{model_name}' datasets are invalid."))]
    InvalidModelDatasets { model_name: String },

    #[snafu(display("Dataset '{dataset}' not found"))]
    DatasetNotFound { dataset: String },

    #[snafu(display("{message}"))]
    ContextMessage { message: String },

    #[snafu(display("{source}"))]
    SchemaContext {
        source: Box<dyn StdError + Send + Sync>,
    },

    #[snafu(display("{source}"))]
    SampleContext {
        source: Box<dyn StdError + Send + Sync>,
    },
}

#[derive(Clone, Debug, Serialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub(crate) struct SampleContextBlock {
    pub(crate) title: String,
    pub(crate) content: String,
}

#[derive(Clone, Debug, Serialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub(crate) struct NsqlContextJsonResponse {
    /// The rendered NSQL context block injected into `/v1/nsql` model requests.
    pub(crate) context: String,

    /// High-level SQL generation instructions.
    pub(crate) instructions: Vec<String>,

    /// SQL engine and dialect details for Spice SQL.
    pub(crate) sql: NsqlSqlContext,

    /// In-scope datasets with schema, metadata, relationship, key, and index details.
    pub(crate) datasets: Vec<NsqlDatasetContext>,

    /// Available function groups filtered to the current DataFusion context.
    pub(crate) functions: NsqlFunctionContext,

    /// Optional sample blocks included when requested.
    pub(crate) samples: Vec<SampleContextBlock>,
}

#[derive(Clone, Debug, Serialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub(crate) struct NsqlSqlContext {
    pub(crate) engine: String,
    pub(crate) dialect: String,
    pub(crate) parser: String,
    pub(crate) notes: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub(crate) struct NsqlDatasetContext {
    pub(crate) name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) catalog: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) schema: Option<String>,
    pub(crate) table: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) description: Option<String>,
    pub(crate) metadata: BTreeMap<String, String>,
    pub(crate) columns: Vec<NsqlColumnContext>,
    pub(crate) primary_key: Vec<String>,
    pub(crate) unique_constraints: Vec<Vec<String>>,
    pub(crate) foreign_keys: Vec<NsqlForeignKeyContext>,
    pub(crate) indexes: Vec<NsqlIndexContext>,
    pub(crate) search: NsqlDatasetSearchContext,
}

#[derive(Clone, Debug, Serialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub(crate) struct NsqlColumnContext {
    pub(crate) name: String,
    pub(crate) data_type: String,
    pub(crate) nullable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) source_type: Option<String>,
    pub(crate) metadata: BTreeMap<String, String>,
    pub(crate) primary_key: bool,
    pub(crate) unique: bool,
    pub(crate) indexed: bool,
    pub(crate) vector_search: bool,
    pub(crate) full_text_search: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub(crate) struct NsqlForeignKeyContext {
    pub(crate) columns: Vec<String>,
    pub(crate) foreign_table: String,
    pub(crate) foreign_columns: Vec<String>,
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub(crate) struct NsqlIndexContext {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) name: Option<String>,
    pub(crate) columns: Vec<String>,
    pub(crate) kind: String,
    pub(crate) source: String,
}

#[derive(Clone, Debug, Default, Serialize, PartialEq, Eq)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub(crate) struct NsqlDatasetSearchContext {
    pub(crate) vector: Vec<NsqlVectorSearchContext>,
    pub(crate) full_text: Vec<NsqlFullTextSearchContext>,
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub(crate) struct NsqlVectorSearchContext {
    pub(crate) column: String,
    pub(crate) function: String,
    pub(crate) syntax: String,
    pub(crate) model: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) engine: Option<String>,
    pub(crate) row_id_columns: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) vector_size: Option<usize>,
    pub(crate) chunked: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) input_mode: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) index: Option<NsqlIndexContext>,
    pub(crate) required_columns: Vec<String>,
    pub(crate) notes: Vec<String>,
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub(crate) struct NsqlFullTextSearchContext {
    pub(crate) column: String,
    pub(crate) function: String,
    pub(crate) syntax: String,
    pub(crate) engine: String,
    pub(crate) index_store: String,
    pub(crate) row_id_columns: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) index: Option<NsqlIndexContext>,
    pub(crate) required_columns: Vec<String>,
    pub(crate) notes: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub(crate) struct NsqlFunctionContext {
    pub(crate) summary: String,
    pub(crate) json: Vec<NsqlFunctionContextEntry>,
    pub(crate) spice_specific: Vec<NsqlFunctionContextEntry>,
    pub(crate) spark_compatibility: NsqlSparkFunctionContext,
    pub(crate) user_defined: Vec<UserFunctionContextEntry>,
}

#[derive(Clone, Debug, Serialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub(crate) struct NsqlSparkFunctionContext {
    pub(crate) description: String,
    pub(crate) functions: Vec<String>,
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub(crate) struct NsqlFunctionContextEntry {
    pub(crate) name: String,
    pub(crate) syntax: String,
    pub(crate) description: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) example: Option<String>,
}

#[cfg(test)]
impl NsqlContextJsonResponse {
    pub(crate) fn minimal_for_test(context: impl Into<String>) -> Self {
        Self {
            context: context.into(),
            instructions: nsql_context_instructions(),
            sql: nsql_sql_context(),
            datasets: vec![],
            functions: NsqlFunctionContext {
                summary: NSQL_FUNCTION_CONTEXT_SUMMARY.to_string(),
                json: vec![],
                spice_specific: vec![],
                spark_compatibility: NsqlSparkFunctionContext {
                    description: "Spark-compatible scalar functions are available for arrays, maps, structs, dates, strings, hashes, URLs, XML, and other common Spark SQL expressions.".to_string(),
                    functions: vec![],
                },
                user_defined: vec![],
            },
            samples: vec![],
        }
    }
}

pub(crate) struct NsqlContext {
    pub(crate) json: NsqlContextJsonResponse,
    pub(crate) message: ChatCompletionRequestMessage,
    pub(crate) table_allowlist: Option<ResolvedTableAwareAllowlist>,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct NsqlContextOptions {
    pub(crate) include_sampling: bool,
    pub(crate) sampling_limit: usize,
    pub(crate) include_examples: bool,
    pub(crate) examples_limit: usize,
}

impl NsqlContextOptions {
    #[must_use]
    pub(crate) fn from_nsql_request(sample_data_enabled: bool) -> Self {
        Self {
            include_sampling: sample_data_enabled,
            sampling_limit: DEFAULT_NSQL_CONTEXT_SAMPLE_LIMIT,
            include_examples: sample_data_enabled,
            examples_limit: DEFAULT_NSQL_CONTEXT_SAMPLE_LIMIT,
        }
    }

    #[must_use]
    pub(crate) fn new(
        include_sampling: bool,
        sampling_limit: usize,
        include_examples: bool,
        examples_limit: usize,
    ) -> Self {
        Self {
            include_sampling,
            sampling_limit,
            include_examples,
            examples_limit,
        }
    }
}

fn value_to_context_text(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        _ => match serde_json::to_string_pretty(value) {
            Ok(text) => text,
            Err(_) => value.to_string(),
        },
    }
}

async fn tool_context_text(
    tool: &dyn SpiceModelTool,
    params: &impl Serialize,
) -> Result<String, Box<dyn StdError + Send + Sync>> {
    let arg = serde_json::to_string(params)?;
    let response = match tool.call(arg.as_str()).await {
        Ok(response) => response,
        Err(error) => tool_call_error_response(tool.name().as_ref(), error),
    };

    Ok(value_to_context_text(&response))
}

async fn table_schema_context(
    tables: &[TableReference],
    rt: Arc<Runtime>,
    table_allowlist: Option<ResolvedTableAwareAllowlist>,
) -> Result<String, Box<dyn StdError + Send + Sync>> {
    let table_schema_tool =
        TableSchemaTool::new(rt, None, None).with_table_allowlist(table_allowlist);
    let params = TableSchemaToolParams::new(tables.iter().map(ToString::to_string).collect());
    tool_context_text(&table_schema_tool, &params).await
}

#[derive(Clone, Debug)]
struct AppTableContext {
    metadata: HashMap<String, String>,
    columns: Vec<Column>,
    legacy_embeddings: Vec<spicepod::component::embeddings::ColumnEmbeddingConfig>,
    vector_store: Option<spicepod::vector::VectorStore>,
    full_text_search: Option<spicepod::fts::FtsStore>,
    configured_primary_key: Option<Vec<String>>,
    configured_indexes: Vec<NsqlIndexContext>,
}

#[derive(Clone, Debug)]
struct ConfiguredVectorSearchColumn {
    column: String,
    model: String,
    engine: Option<String>,
    row_id_columns: Vec<String>,
    vector_size: Option<usize>,
    chunked: bool,
    aggregation: Option<String>,
    max_elements_per_row: Option<usize>,
}

#[derive(Clone, Debug)]
struct ConfiguredFullTextSearchColumn {
    column: String,
    engine: String,
    index_store: String,
    row_id_columns: Vec<String>,
}

#[derive(Clone, Copy, Debug)]
struct SearchFunctionAvailability {
    vector_search: bool,
    text_search: bool,
}

fn nsql_context_instructions() -> Vec<String> {
    NSQL_CONTEXT_INSTRUCTIONS
        .iter()
        .map(ToString::to_string)
        .collect()
}

fn nsql_sql_context() -> NsqlSqlContext {
    NsqlSqlContext {
        engine: "Apache DataFusion".to_string(),
        dialect: "PostgreSQL".to_string(),
        parser: "DataFusion SQL parser configured with PostgreSQL dialect".to_string(),
        notes: vec![
            "Spice supports standard DataFusion SQL plus Spice-specific, JSON, Spark compatibility, and registered user-defined functions listed in this context.".to_string(),
            "Not every PostgreSQL extension is implemented; prefer the functions listed in this response when generating SQL for Spice.".to_string(),
            "Table functions such as text_search, vector_search, json_tree, and list_udfs are used in the FROM clause.".to_string(),
            "Use LIMIT for exploratory or broad-result queries unless the user explicitly asks for all rows.".to_string(),
        ],
    }
}

fn column_reference_columns(reference: &str) -> Vec<String> {
    ColumnReference::try_from(reference).map_or_else(
        |_| vec![reference.to_string()],
        |columns| columns.iter().map(ToString::to_string).collect(),
    )
}

fn configured_acceleration_indexes(
    acceleration: &spicepod::acceleration::Acceleration,
) -> Vec<NsqlIndexContext> {
    let mut indexes = acceleration
        .indexes
        .iter()
        .map(|(columns, index_type)| NsqlIndexContext {
            name: Some(columns.clone()),
            columns: column_reference_columns(columns),
            kind: index_type.to_string(),
            source: "spicepod.acceleration.indexes".to_string(),
        })
        .collect_vec();

    if let Some(primary_key) = &acceleration.primary_key {
        indexes.push(NsqlIndexContext {
            name: Some(primary_key.clone()),
            columns: column_reference_columns(primary_key),
            kind: "primary_key".to_string(),
            source: "spicepod.acceleration.primary_key".to_string(),
        });
    }

    indexes.sort_by(|left, right| {
        left.source
            .cmp(&right.source)
            .then_with(|| left.kind.cmp(&right.kind))
            .then_with(|| left.columns.cmp(&right.columns))
    });
    indexes
}

fn app_table_context(table: &TableReference, app: &app::App) -> Option<AppTableContext> {
    if let Some(dataset) = app
        .datasets
        .iter()
        .find(|dataset| table.resolved_eq(&TableReference::parse_str(&dataset.name)))
    {
        let configured_primary_key = dataset
            .acceleration
            .as_ref()
            .and_then(|acceleration| acceleration.primary_key.as_deref())
            .map(column_reference_columns)
            .or_else(|| dataset.primary_key_override());
        let configured_indexes = dataset
            .acceleration
            .as_ref()
            .map_or_else(Vec::new, configured_acceleration_indexes);

        return Some(AppTableContext {
            metadata: dataset.metadata(),
            columns: dataset.columns.clone(),
            legacy_embeddings: dataset.embeddings.clone(),
            vector_store: dataset.vectors.clone(),
            full_text_search: dataset.full_text_search.clone(),
            configured_primary_key,
            configured_indexes,
        });
    }

    app.views
        .iter()
        .find(|view| table.resolved_eq(&TableReference::parse_str(&view.name)))
        .map(|view| AppTableContext {
            metadata: view.metadata(),
            columns: view.columns.clone(),
            legacy_embeddings: vec![],
            vector_store: view.vectors.clone(),
            full_text_search: None,
            configured_primary_key: view.primary_key_override(),
            configured_indexes: view
                .acceleration
                .as_ref()
                .map_or_else(Vec::new, configured_acceleration_indexes),
        })
}

fn constraint_column_names(schema: &Schema, indices: &[usize]) -> Option<Vec<String>> {
    indices
        .iter()
        .map(|index| {
            schema
                .fields()
                .get(*index)
                .map(|field| field.name().clone())
        })
        .collect()
}

fn key_context_from_constraints(
    schema: &Schema,
    constraints: Option<&Constraints>,
) -> (Vec<String>, Vec<Vec<String>>) {
    let Some(constraints) = constraints else {
        return (vec![], vec![]);
    };

    let mut primary_key = Vec::new();
    let mut unique_constraints = Vec::new();

    for constraint in constraints.iter() {
        match constraint {
            Constraint::PrimaryKey(indices) => {
                if primary_key.is_empty()
                    && let Some(columns) = constraint_column_names(schema, indices)
                {
                    primary_key = columns;
                }
            }
            Constraint::Unique(indices) => {
                if let Some(columns) = constraint_column_names(schema, indices) {
                    unique_constraints.push(columns);
                }
            }
        }
    }

    (primary_key, unique_constraints)
}

fn foreign_keys_from_metadata(metadata: &HashMap<String, String>) -> Vec<NsqlForeignKeyContext> {
    let Some(foreign_keys) = metadata.get(FOREIGN_KEYS_METADATA_KEY) else {
        return vec![];
    };

    match serde_json::from_str::<Vec<NsqlForeignKeyContext>>(foreign_keys) {
        Ok(foreign_keys) => foreign_keys,
        Err(error) => {
            tracing::warn!(%error, "Failed to parse foreign key metadata for NSQL context");
            vec![]
        }
    }
}

fn runtime_indexes_from_provider(
    table_provider: Option<&Arc<dyn datafusion::datasource::TableProvider>>,
) -> Vec<NsqlIndexContext> {
    let Some(table_provider) = table_provider else {
        return vec![];
    };

    find_concrete_table_provider::<IndexedTableProvider>(table_provider).map_or_else(
        Vec::new,
        |indexed_table| {
            indexed_table
                .get_all_indexes()
                .into_iter()
                .map(|index| {
                    let mut columns = index.required_columns();
                    columns.sort();
                    NsqlIndexContext {
                        name: Some(index.name().to_string()),
                        columns,
                        kind: index.name().to_string(),
                        source: "runtime_index".to_string(),
                    }
                })
                .collect()
        },
    )
}

fn search_function_availability(available_names: &HashSet<String>) -> SearchFunctionAvailability {
    SearchFunctionAvailability {
        vector_search: available_names.contains(VECTOR_SEARCH_UDTF_NAME),
        text_search: available_names.contains(TEXT_SEARCH_UDTF_NAME),
    }
}

fn is_vector_search_index(kind: &str) -> bool {
    matches!(
        kind,
        "NativeVectorIndex"
            | "duckdb_vector_index"
            | "elasticsearch_index"
            | "s3_vector_index"
            | "ChunkedSearchIndex"
            | "ChunkedVectorIndex"
    )
}

fn is_full_text_search_index(kind: &str) -> bool {
    matches!(kind, "full_text" | "elasticsearch_text_index")
}

fn search_index_for_column(
    indexes: &[NsqlIndexContext],
    column: &str,
    index_kind: fn(&str) -> bool,
) -> Option<NsqlIndexContext> {
    indexes
        .iter()
        .find(|index| index_kind(&index.kind) && index.columns.iter().any(|c| c == column))
        .cloned()
}

fn configured_vector_search_columns(
    app_context: &AppTableContext,
) -> Vec<ConfiguredVectorSearchColumn> {
    let dataset_engine = app_context
        .vector_store
        .as_ref()
        .filter(|store| store.enabled)
        .and_then(|store| store.engine.clone());

    let mut configured = app_context
        .columns
        .iter()
        .flat_map(|column| {
            let dataset_engine = dataset_engine.clone();
            column
                .embeddings
                .iter()
                .map(move |embedding| ConfiguredVectorSearchColumn {
                    column: column.name.clone(),
                    model: embedding.model.clone(),
                    engine: embedding.engine.clone().or_else(|| dataset_engine.clone()),
                    row_id_columns: embedding.row_ids.clone().unwrap_or_default(),
                    vector_size: embedding.vector_size,
                    chunked: embedding
                        .chunking
                        .as_ref()
                        .is_some_and(|chunking| chunking.enabled),
                    aggregation: embedding
                        .aggregation
                        .map(|aggregation| aggregation.to_string()),
                    max_elements_per_row: embedding.max_elements_per_row,
                })
        })
        .collect_vec();

    configured.extend(app_context.legacy_embeddings.iter().map(|embedding| {
        ConfiguredVectorSearchColumn {
            column: embedding.column.clone(),
            model: embedding.model.clone(),
            engine: dataset_engine.clone(),
            row_id_columns: embedding.primary_keys.clone().unwrap_or_default(),
            vector_size: embedding.vector_size,
            chunked: embedding
                .chunking
                .as_ref()
                .is_some_and(|chunking| chunking.enabled),
            aggregation: embedding
                .aggregation
                .map(|aggregation| aggregation.to_string()),
            max_elements_per_row: embedding.max_elements_per_row,
        }
    }));

    let mut seen_columns = HashSet::new();
    configured.retain(|column| seen_columns.insert(column.column.clone()));
    configured.sort_by(|left, right| left.column.cmp(&right.column));
    configured
}

fn configured_full_text_search_columns(
    table: &TableReference,
    app_context: &AppTableContext,
    primary_key: &[String],
) -> Vec<ConfiguredFullTextSearchColumn> {
    let dataset_engine = app_context
        .full_text_search
        .as_ref()
        .filter(|store| store.enabled)
        .and_then(|store| store.engine.clone());
    let dataset_config_primary_key = full_text_search_config(&app_context.columns, table)
        .map(|config| config.primary_key)
        .unwrap_or_default();

    let mut configured = app_context
        .columns
        .iter()
        .filter_map(|column| {
            let full_text_search = column
                .full_text_search
                .as_ref()
                .filter(|config| config.enabled)?;

            let row_id_columns = full_text_search
                .row_ids
                .clone()
                .filter(|row_ids| !row_ids.is_empty())
                .or_else(|| {
                    (!dataset_config_primary_key.is_empty())
                        .then_some(dataset_config_primary_key.clone())
                })
                .unwrap_or_else(|| primary_key.to_vec());

            Some(ConfiguredFullTextSearchColumn {
                column: column.name.clone(),
                engine: full_text_search
                    .engine
                    .clone()
                    .or_else(|| dataset_engine.clone())
                    .unwrap_or_else(|| "tantivy".to_string()),
                index_store: full_text_search.index_store.unwrap_or_default().to_string(),
                row_id_columns,
            })
        })
        .collect_vec();

    configured.sort_by(|left, right| left.column.cmp(&right.column));
    configured
}

fn embedding_input_mode_context(input_mode: EmbeddingInputMode) -> String {
    match input_mode {
        EmbeddingInputMode::Scalar => "scalar".to_string(),
        EmbeddingInputMode::ListMulti {
            aggregation,
            max_elements_per_row,
        } => format!(
            "list_multi; aggregation={aggregation}; max_elements_per_row={max_elements_per_row}"
        ),
    }
}

fn quoted_identifier(name: &str) -> String {
    quote_identifier(name).to_string()
}

fn vector_search_syntax(table: &TableReference, column: &str) -> String {
    format!(
        "{VECTOR_SEARCH_UDTF_NAME}({table}, 'query text', {})",
        quoted_identifier(column)
    )
}

fn text_search_syntax(table: &TableReference, column: &str) -> String {
    format!(
        "{TEXT_SEARCH_UDTF_NAME}({table}, 'query text', {})",
        quoted_identifier(column)
    )
}

fn vector_search_contexts(
    table: &TableReference,
    table_provider: Option<&Arc<dyn datafusion::datasource::TableProvider>>,
    runtime_indexes: &[NsqlIndexContext],
    app_context: &AppTableContext,
    primary_key: &[String],
    available: SearchFunctionAvailability,
) -> Vec<NsqlVectorSearchContext> {
    if !available.vector_search {
        return vec![];
    }

    let embedding_table = table_provider.and_then(find_concrete_table_provider::<EmbeddingTable>);

    configured_vector_search_columns(app_context)
        .into_iter()
        .filter_map(|configured| {
            let index = search_index_for_column(
                runtime_indexes,
                &configured.column,
                is_vector_search_index,
            );
            let embedding_config = embedding_table
                .and_then(|table| table.embedded_columns.get(&configured.column));

            if index.is_none() && embedding_config.is_none() {
                return None;
            }

            let row_id_columns = if configured.row_id_columns.is_empty() {
                primary_key.to_vec()
            } else {
                configured.row_id_columns.clone()
            };
            let vector_size = configured.vector_size.or_else(|| {
                embedding_config.and_then(|config| usize::try_from(config.vector_size).ok())
            });
            let model = if configured.model.is_empty() {
                embedding_config.map_or_else(String::new, |config| config.model_name.clone())
            } else {
                configured.model.clone()
            };
            if model.is_empty() {
                return None;
            }

            let required_columns = index.as_ref().map_or_else(
                || {
                    row_id_columns
                        .iter()
                        .cloned()
                        .chain(std::iter::once(configured.column.clone()))
                        .collect_vec()
                },
                |index| index.columns.clone(),
            );
            let mut notes = Vec::new();
            if configured.chunked {
                notes.push(
                    "This column is chunked; vector_search returns the most relevant chunk in the value column."
                        .to_string(),
                );
            }
            if configured.aggregation.is_some() || configured.max_elements_per_row.is_some() {
                notes.push(
                    "This column is configured for multi-vector search; similarity is aggregated across list elements."
                        .to_string(),
                );
            }
            if index.is_none() {
                notes.push(
                    "No separate vector engine index is configured; vector_search uses the embedding table path for this column."
                        .to_string(),
                );
            }

            Some(NsqlVectorSearchContext {
                column: configured.column.clone(),
                function: VECTOR_SEARCH_UDTF_NAME.to_string(),
                syntax: vector_search_syntax(table, &configured.column),
                model,
                engine: configured
                    .engine
                    .clone()
                    .or_else(|| index.as_ref().map(|index| index.kind.clone())),
                row_id_columns,
                vector_size,
                chunked: configured.chunked,
                input_mode: embedding_config.map(|config| embedding_input_mode_context(config.input_mode)),
                required_columns,
                index,
                notes,
            })
        })
        .collect_vec()
}

fn full_text_search_contexts(
    table: &TableReference,
    runtime_indexes: &[NsqlIndexContext],
    app_context: &AppTableContext,
    primary_key: &[String],
    available: SearchFunctionAvailability,
) -> Vec<NsqlFullTextSearchContext> {
    if !available.text_search {
        return vec![];
    }

    configured_full_text_search_columns(table, app_context, primary_key)
        .into_iter()
        .filter_map(|configured| {
            let index = search_index_for_column(
                runtime_indexes,
                &configured.column,
                is_full_text_search_index,
            )?;
            let required_columns = index.columns.clone();

            Some(NsqlFullTextSearchContext {
                column: configured.column.clone(),
                function: TEXT_SEARCH_UDTF_NAME.to_string(),
                syntax: text_search_syntax(table, &configured.column),
                engine: configured.engine,
                index_store: configured.index_store,
                row_id_columns: configured.row_id_columns,
                required_columns,
                index: Some(index),
                notes: vec![],
            })
        })
        .collect_vec()
}

fn dataset_search_context(
    table: &TableReference,
    table_provider: Option<&Arc<dyn datafusion::datasource::TableProvider>>,
    runtime_indexes: &[NsqlIndexContext],
    app_context: Option<&AppTableContext>,
    primary_key: &[String],
    available: SearchFunctionAvailability,
) -> NsqlDatasetSearchContext {
    let Some(app_context) = app_context else {
        return NsqlDatasetSearchContext::default();
    };

    NsqlDatasetSearchContext {
        vector: vector_search_contexts(
            table,
            table_provider,
            runtime_indexes,
            app_context,
            primary_key,
            available,
        ),
        full_text: full_text_search_contexts(
            table,
            runtime_indexes,
            app_context,
            primary_key,
            available,
        ),
    }
}

fn dataset_context_from_schema(
    table: &TableReference,
    schema: Schema,
    constraints: Option<&Constraints>,
    runtime_indexes: Vec<NsqlIndexContext>,
    table_provider: Option<&Arc<dyn datafusion::datasource::TableProvider>>,
    app_context: Option<AppTableContext>,
    search_functions: SearchFunctionAvailability,
) -> NsqlDatasetContext {
    let mut metadata = schema.metadata().clone();
    if let Some(app_context) = &app_context {
        metadata.extend(app_context.metadata.clone());
    }

    let (mut primary_key, unique_constraints) = key_context_from_constraints(&schema, constraints);
    if primary_key.is_empty()
        && let Some(configured_primary_key) = app_context
            .as_ref()
            .and_then(|context| context.configured_primary_key.clone())
    {
        primary_key = configured_primary_key;
    }

    let mut indexes = runtime_indexes;
    if let Some(app_context) = &app_context {
        indexes.extend(app_context.configured_indexes.clone());
    }
    indexes.sort_by(|left, right| {
        left.source
            .cmp(&right.source)
            .then_with(|| left.kind.cmp(&right.kind))
            .then_with(|| left.columns.cmp(&right.columns))
    });

    let primary_key_columns = primary_key.iter().cloned().collect::<BTreeSet<_>>();
    let unique_columns = unique_constraints
        .iter()
        .flatten()
        .chain(
            indexes
                .iter()
                .filter(|index| index.kind == "unique")
                .flat_map(|index| index.columns.iter()),
        )
        .cloned()
        .collect::<BTreeSet<_>>();
    let indexed_columns = indexes
        .iter()
        .flat_map(|index| index.columns.iter().cloned())
        .collect::<BTreeSet<_>>();

    let search = dataset_search_context(
        table,
        table_provider,
        &indexes,
        app_context.as_ref(),
        &primary_key,
        search_functions,
    );
    let vector_search_columns = search
        .vector
        .iter()
        .map(|search| search.column.clone())
        .collect::<BTreeSet<_>>();
    let full_text_search_columns = search
        .full_text
        .iter()
        .map(|search| search.column.clone())
        .collect::<BTreeSet<_>>();

    let app_columns = app_context
        .as_ref()
        .map(|context| context.columns.as_slice())
        .unwrap_or_default();
    let columns = schema
        .fields()
        .iter()
        .map(|field| {
            let mut field_metadata = field.metadata().clone();
            if let Some(app_column) = app_columns
                .iter()
                .find(|column| column.name == *field.name())
            {
                field_metadata.extend(app_column.metadata());
            }

            let description = field_metadata.get(DESCRIPTION_METADATA_KEY).cloned();
            let source_type = field_metadata.get(SOURCE_TYPE_METADATA_KEY).cloned();

            NsqlColumnContext {
                name: field.name().clone(),
                data_type: field.data_type().to_string(),
                nullable: field.is_nullable(),
                description,
                source_type,
                primary_key: primary_key_columns.contains(field.name()),
                unique: unique_columns.contains(field.name()),
                indexed: indexed_columns.contains(field.name()),
                vector_search: vector_search_columns.contains(field.name()),
                full_text_search: full_text_search_columns.contains(field.name()),
                metadata: field_metadata.into_iter().collect(),
            }
        })
        .collect();

    let description = metadata.get(DESCRIPTION_METADATA_KEY).cloned();
    let foreign_keys = foreign_keys_from_metadata(&metadata);

    NsqlDatasetContext {
        name: table.to_string(),
        catalog: table.catalog().map(ToString::to_string),
        schema: table.schema().map(ToString::to_string),
        table: table.table().to_string(),
        description,
        foreign_keys,
        metadata: metadata.into_iter().collect(),
        columns,
        primary_key,
        unique_constraints,
        indexes,
        search,
    }
}

async fn dataset_contexts(
    tables: &[TableReference],
    rt: Arc<Runtime>,
    app: &app::App,
    search_functions: SearchFunctionAvailability,
) -> Result<Vec<NsqlDatasetContext>, Box<dyn StdError + Send + Sync>> {
    let df = rt.datafusion();
    let mut contexts = Vec::with_capacity(tables.len());

    for table in tables {
        let schema = df.get_arrow_schema(table.clone()).await?;
        let table_provider = df.get_table(table).await;
        let runtime_indexes = runtime_indexes_from_provider(table_provider.as_ref());
        let constraints = table_provider
            .as_ref()
            .and_then(|provider| provider.constraints());
        contexts.push(dataset_context_from_schema(
            table,
            schema,
            constraints,
            runtime_indexes,
            table_provider.as_ref(),
            app_table_context(table, app),
            search_functions,
        ));
    }

    Ok(contexts)
}

async fn sample_context_blocks(
    sample_from: &[TableReference],
    rt: Arc<Runtime>,
    table_allowlist: Option<ResolvedTableAwareAllowlist>,
    options: NsqlContextOptions,
) -> Result<Vec<SampleContextBlock>, Box<dyn StdError + Send + Sync>> {
    let context_futures = sample_from.iter().flat_map(|dataset| {
        let mut params = Vec::with_capacity(2);
        if options.include_sampling {
            params.push(SampleTableParams::DistinctColumns(DistinctColumnsParams {
                tbl: dataset.to_string(),
                limit: options.sampling_limit,
                cols: None,
            }));
        }
        if options.include_examples {
            params.push(SampleTableParams::RandomSample(RandomSampleParams {
                tbl: dataset.to_string(),
                limit: options.examples_limit,
            }));
        }

        params.into_iter().map(|params| {
            let rt = Arc::clone(&rt);
            let allowlist = table_allowlist.clone();
            async move {
                let method = SampleTableMethod::from(&params);
                let content = tool_context_text(
                    &SampleDataTool::new(rt.datafusion(), method.clone())
                        .with_table_allowlist(allowlist),
                    &params,
                )
                .instrument(Span::current())
                .await?;

                Ok(SampleContextBlock {
                    title: match method {
                        SampleTableMethod::DistinctColumns => {
                            format!("Distinct value samples for `{}`", params.dataset())
                        }
                        SampleTableMethod::RandomSample => {
                            format!("Example rows for `{}`", params.dataset())
                        }
                        SampleTableMethod::TopNSample => {
                            format!("Top rows for `{}`", params.dataset())
                        }
                    },
                    content,
                })
            }
        })
    });

    futures::stream::iter(context_futures)
        .boxed()
        .buffer_unordered(DATA_SAMPLING_MAX_CONCURRENT)
        .try_collect::<Vec<_>>()
        .await
}

fn context_message(context: &str) -> Result<ChatCompletionRequestMessage, String> {
    ChatCompletionRequestSystemMessageArgs::default()
        .content(context)
        .build()
        .map(Into::into)
        .map_err(|error| error.to_string())
}

fn available_function_names(rt: &Runtime) -> HashSet<String> {
    let mut function_names = rt
        .df
        .ctx
        .udfs()
        .into_iter()
        .map(|name| name.to_ascii_lowercase())
        .collect::<HashSet<_>>();

    function_names.extend(
        rt.df
            .ctx
            .state()
            .table_functions()
            .keys()
            .map(|name| name.to_ascii_lowercase()),
    );

    function_names
}

#[derive(Clone, Debug)]
struct FunctionContextEntry {
    name: String,
    syntax: &'static str,
    description: &'static str,
    example: Option<&'static str>,
}

impl FunctionContextEntry {
    fn new(name: impl Into<String>, syntax: &'static str, description: &'static str) -> Self {
        Self {
            name: name.into(),
            syntax,
            description,
            example: None,
        }
    }

    fn with_example(mut self, example: &'static str) -> Self {
        self.example = Some(example);
        self
    }

    fn to_context_entry(&self) -> NsqlFunctionContextEntry {
        NsqlFunctionContextEntry {
            name: self.name.clone(),
            syntax: self.syntax.to_string(),
            description: self.description.to_string(),
            example: self.example.map(ToString::to_string),
        }
    }
}

fn json_function_entries() -> Vec<FunctionContextEntry> {
    vec![
        FunctionContextEntry::new(
            json_get_udf().name(),
            "json_get(json, path)",
            "Returns a JSON union value at a path. Use typed helpers when a scalar type is known.",
        )
        .with_example("SELECT json_get(payload, 'metadata') FROM events"),
        FunctionContextEntry::new(
            json_get_str_udf().name(),
            "json_get_str(json, path)",
            "Returns a string value from JSON at a path.",
        )
        .with_example("SELECT json_get_str(payload, 'customer.name') FROM events"),
        FunctionContextEntry::new(
            json_get_int_udf().name(),
            "json_get_int(json, path)",
            "Returns an integer value from JSON at a path.",
        )
        .with_example("SELECT json_get_int(payload, 'order.id') FROM events"),
        FunctionContextEntry::new(
            json_get_float_udf().name(),
            "json_get_float(json, path)",
            "Returns a floating-point value from JSON at a path.",
        )
        .with_example("SELECT json_get_float(payload, 'score') FROM events"),
        FunctionContextEntry::new(
            json_get_bool_udf().name(),
            "json_get_bool(json, path)",
            "Returns a boolean value from JSON at a path.",
        )
        .with_example("SELECT json_get_bool(payload, 'active') FROM events"),
        FunctionContextEntry::new(
            json_get_json_udf().name(),
            "json_get_json(json, path)",
            "Returns a JSON string value from JSON at a path.",
        )
        .with_example("SELECT json_get_json(payload, 'items[0]') FROM events"),
        FunctionContextEntry::new(
            json_get_array_udf().name(),
            "json_get_array(json, path)",
            "Returns an array value from JSON at a path.",
        )
        .with_example("SELECT json_get_array(payload, 'tags') FROM events"),
        FunctionContextEntry::new(
            json_as_text_udf().name(),
            "json_as_text(json, path)",
            "Returns the JSON value at a path as text.",
        )
        .with_example("SELECT json_as_text(payload, 'metadata') FROM events"),
        FunctionContextEntry::new(
            json_contains_udf().name(),
            "json_contains(json, value)",
            "Returns whether a JSON value contains another JSON value.",
        )
        .with_example("SELECT * FROM events WHERE json_contains(payload, 'urgent')"),
        FunctionContextEntry::new(
            json_length_udf().name(),
            "json_length(json[, path])",
            "Returns the length of a JSON array or object.",
        )
        .with_example("SELECT json_length(payload, 'items') FROM orders"),
        FunctionContextEntry::new(
            json_object_keys_udf().name(),
            "json_object_keys(json)",
            "Returns object keys from a JSON value.",
        )
        .with_example("SELECT json_object_keys(payload) FROM events"),
        FunctionContextEntry::new(
            json_from_scalar_udf().name(),
            "json_from_scalar(value)",
            "Converts a scalar SQL value into JSON.",
        )
        .with_example("SELECT json_from_scalar(status) FROM tickets"),
    ]
}

fn spice_function_entries() -> Vec<FunctionContextEntry> {
    vec![
        FunctionContextEntry::new(
            FLATTEN_JSON_UDTF_NAME,
            "flatten_json(json)",
            "Flattens a JSON document into key/value rows. Use as a table function for literal JSON or with UNNEST for column values.",
        )
        .with_example("SELECT * FROM flatten_json('{\"a\":1}')"),
        FunctionContextEntry::new(
            FLATTEN_JSON_PROPERTIES_UDTF_NAME,
            "flatten_json_properties(json_schema)",
            "Flattens a JSON Schema document into rows describing nested properties.",
        )
        .with_example("SELECT * FROM flatten_json_properties(schema_json)"),
        FunctionContextEntry::new(
            JSON_TREE_UDTF_NAME,
            "json_tree(json)",
            "Walks a JSON document recursively and returns one row per node.",
        )
        .with_example("SELECT * FROM json_tree('{\"items\":[1,2]}')"),
        FunctionContextEntry::new(
            TEXT_SEARCH_UDTF_NAME,
            "text_search(dataset, 'query text'[, column])",
            "Runs full-text search over a dataset column with a configured full-text search index. Pass column when a table has more than one full-text search column.",
        )
        .with_example("SELECT * FROM text_search(docs, 'refund policy', body)"),
        FunctionContextEntry::new(
            VECTOR_SEARCH_UDTF_NAME,
            "vector_search(dataset, 'query text'[, column])",
            "Runs vector search over a dataset column with a configured embedding/vector index. Pass column when a table has more than one vector-search column.",
        )
        .with_example("SELECT * FROM vector_search(docs, 'refund policy', body)"),
        FunctionContextEntry::new(
            RRF_UDF_NAME,
            "rrf(text_search(...), vector_search(...))",
            "Combines text and vector search results with reciprocal rank fusion.",
        )
        .with_example("SELECT * FROM rrf(text_search(docs, 'refund', body), vector_search(docs, 'refund', body))"),
        FunctionContextEntry::new(
            RERANK_UDTF_NAME,
            "rerank(input => TABLE(...), model => 'model')",
            "Reranks search results with a configured reranker model.",
        )
        .with_example("SELECT * FROM rerank(input => TABLE(SELECT * FROM docs), model => 'reranker')"),
        FunctionContextEntry::new(
            COSINE_DISTANCE_UDF_NAME,
            "cosine_distance(vector_a, vector_b)",
            "Computes cosine distance between two vector/list values.",
        )
        .with_example("SELECT cosine_distance(embedding, query_embedding) FROM docs"),
        FunctionContextEntry::new(
            INNER_PRODUCT_UDF_NAME,
            "inner_product(vector_a, vector_b)",
            "Computes inner product between two vector/list values.",
        )
        .with_example("SELECT inner_product(embedding, query_embedding) FROM docs"),
        FunctionContextEntry::new(
            L2_DISTANCE_UDF_NAME,
            "l2_distance(vector_a, vector_b)",
            "Computes Euclidean distance between two vector/list values.",
        )
        .with_example("SELECT l2_distance(embedding, query_embedding) FROM docs"),
        FunctionContextEntry::new(
            L2_SQUARED_DISTANCE_UDF_NAME,
            "l2_squared_distance(vector_a, vector_b)",
            "Computes squared Euclidean distance between two vector/list values.",
        )
        .with_example("SELECT l2_squared_distance(embedding, query_embedding) FROM docs"),
        FunctionContextEntry::new(
            L2_NORM_UDF_NAME,
            "l2_norm(vector)",
            "Computes the L2 norm of a vector/list value.",
        )
        .with_example("SELECT l2_norm(embedding) FROM docs"),
        FunctionContextEntry::new(
            EMBED_UDF_NAME,
            "embed(text[, model])",
            "Generates an embedding using a configured embedding model.",
        )
        .with_example("SELECT embed('refund policy', 'embedding_model')"),
        FunctionContextEntry::new(
            AI_UDF_NAME,
            "ai(message[, model])",
            "Runs a prompt against a configured chat model from SQL.",
        )
        .with_example("SELECT ai('Summarize this ticket', 'llm_model')"),
        FunctionContextEntry::new(
            BUCKET_SCALAR_UDF_NAME,
            "bucket(value, boundaries)",
            "Assigns a value to a bucket using ordered boundaries.",
        )
        .with_example("SELECT bucket(amount, [10, 100, 1000]) FROM orders"),
        FunctionContextEntry::new(
            TRUNCATE_SCALAR_UDF_NAME,
            "truncate(width, value)",
            "Truncates a value using Iceberg/Spark-compatible semantics.",
        )
        .with_example("SELECT truncate(10, amount) FROM orders"),
        FunctionContextEntry::new(
            DIGEST_UDF_NAME,
            "digest_many(col_a, col_b, ..., digest_function_name)",
            "Hashes multiple column values using a DataFusion digest function such as md5.",
        )
        .with_example("SELECT digest_many(customer_id, order_id, 'md5') FROM orders"),
        FunctionContextEntry::new(
            OBJ_DESCRIPTION_UDF_NAME,
            "obj_description(object_id)",
            "Returns PostgreSQL-compatible object descriptions when available.",
        )
        .with_example("SELECT obj_description(table_oid)"),
        FunctionContextEntry::new(
            COL_DESCRIPTION_UDF_NAME,
            "col_description(table_id, column_number)",
            "Returns PostgreSQL-compatible column descriptions when available.",
        )
        .with_example("SELECT col_description(table_oid, 1)"),
        FunctionContextEntry::new(
            LIST_UDFS_UDTF_NAME,
            "list_udfs()",
            "Lists scalar and table UDFs registered in the Spice runtime.",
        )
        .with_example("SELECT * FROM list_udfs()"),
    ]
}

fn available_context_entries(
    entries: Vec<FunctionContextEntry>,
    available_names: &HashSet<String>,
    search_functions: SearchFunctionAvailability,
) -> Vec<NsqlFunctionContextEntry> {
    entries
        .iter()
        .filter(|entry| {
            let name = entry.name.to_ascii_lowercase();
            if name == TEXT_SEARCH_UDTF_NAME {
                return available_names.contains(&name) && search_functions.text_search;
            }
            if name == VECTOR_SEARCH_UDTF_NAME {
                return available_names.contains(&name) && search_functions.vector_search;
            }
            if name == RRF_UDF_NAME || name == RERANK_UDTF_NAME {
                return available_names.contains(&name)
                    && search_functions.text_search
                    && search_functions.vector_search;
            }
            available_names.contains(&name)
        })
        .map(FunctionContextEntry::to_context_entry)
        .collect_vec()
}

#[cfg(test)]
pub(crate) fn all_context_function_names_for_test() -> HashSet<String> {
    let mut names = json_function_entries()
        .into_iter()
        .chain(spice_function_entries())
        .map(|entry| entry.name.to_ascii_lowercase())
        .filter(|name| cfg!(feature = "models") || (name != AI_UDF_NAME && name != EMBED_UDF_NAME))
        .collect::<HashSet<_>>();

    names.extend(
        datafusion_spark::all_default_scalar_functions()
            .into_iter()
            .map(|function| function.name().to_ascii_lowercase()),
    );
    names
}

#[cfg(test)]
pub(crate) fn nsql_function_context_for_test(
    app: &app::App,
    available_names: &HashSet<String>,
    vector_search: bool,
    text_search: bool,
) -> NsqlFunctionContext {
    nsql_function_context(
        app,
        available_names,
        SearchFunctionAvailability {
            vector_search,
            text_search,
        },
    )
}

fn write_function_entries(
    output: &mut String,
    section: &str,
    entries: &[NsqlFunctionContextEntry],
) {
    if entries.is_empty() {
        return;
    }

    let _ = writeln!(output, "\n### {section}");
    for entry in entries {
        let _ = write!(
            output,
            "- `{}`: {} Syntax: `{}`.",
            entry.name, entry.description, entry.syntax
        );
        if let Some(example) = &entry.example {
            let _ = write!(output, " Example: `{example}`.");
        }
        let _ = writeln!(output);
    }
}

fn write_wrapped_function_names(output: &mut String, names: &[String]) {
    for chunk in names.chunks(8) {
        let rendered_names = chunk.iter().map(|name| format!("`{name}`")).join(", ");
        let _ = writeln!(output, "- {rendered_names}");
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub(crate) struct UserFunctionContextEntry {
    pub(crate) name: String,
    pub(crate) syntax: Option<String>,
    pub(crate) kind: String,
    pub(crate) volatility: String,
    pub(crate) from: String,
    pub(crate) description: Option<String>,
}

fn user_function_args_signature(args: &[spicepod::component::function::FunctionArg]) -> String {
    args.iter()
        .map(|arg| format!("{} {}", arg.name, arg.arrow_type))
        .join(", ")
}

fn user_function_syntax(function: &Function) -> String {
    let mut args = function
        .signature
        .tables
        .iter()
        .map(|table| {
            format!(
                "{} TABLE({})",
                table.name,
                user_function_args_signature(&table.columns)
            )
        })
        .collect_vec();
    args.extend(
        function
            .signature
            .args
            .iter()
            .map(|arg| format!("{} {}", arg.name, arg.arrow_type)),
    );

    let mut syntax = format!("{}({})", function.name, args.join(", "));
    if let Some(return_type) = function.signature.scalar_return_type() {
        let _ = write!(syntax, " -> {return_type}");
    } else if let Some(columns) = function.signature.table_return_columns() {
        let _ = write!(
            syntax,
            " -> TABLE({})",
            user_function_args_signature(columns)
        );
    }

    syntax
}

pub(crate) fn user_function_context_entries(
    app: &app::App,
    available_names: &HashSet<String>,
    user_function_infos: Vec<UserFunctionInfo>,
) -> Vec<UserFunctionContextEntry> {
    if !app.runtime.functions.enabled {
        return vec![];
    }

    let declarations_by_name = app
        .functions
        .iter()
        .filter(|function| function.enabled)
        .map(|function| (function.name.to_ascii_lowercase(), function))
        .collect::<HashMap<_, _>>();

    let mut user_functions = user_function_infos
        .into_iter()
        .filter_map(|function| {
            let name = function.name.to_ascii_lowercase();
            if !available_names.contains(&name) {
                return None;
            }
            let declaration = declarations_by_name.get(&name)?;

            Some(UserFunctionContextEntry {
                name: function.name,
                syntax: Some(user_function_syntax(declaration)),
                kind: declaration.kind.as_str().to_string(),
                volatility: effective_user_function_volatility(declaration).to_string(),
                from: declaration.from.clone(),
                description: declaration.description.clone().or(function.description),
            })
        })
        .collect_vec();
    user_functions.sort_by_key(|function| function.name.to_ascii_lowercase());
    user_functions
}

pub(crate) fn write_user_function_context_entries(
    output: &mut String,
    user_functions: &[UserFunctionContextEntry],
) {
    if user_functions.is_empty() {
        return;
    }

    let _ = writeln!(output, "\n### User-defined functions");
    for function in user_functions {
        let _ = write!(
            output,
            "- `{}`: {} function from `{}` with `{}` volatility.",
            function.name, function.kind, function.from, function.volatility
        );
        if let Some(syntax) = &function.syntax {
            let _ = write!(output, " Syntax: `{syntax}`.");
        }
        if let Some(description) = &function.description
            && !description.is_empty()
        {
            let _ = write!(output, " {description}");
        }
        let _ = writeln!(output);
    }
}

fn user_function_context(
    app: &app::App,
    available_names: &HashSet<String>,
) -> Vec<UserFunctionContextEntry> {
    user_function_context_entries(app, available_names, user_function_infos())
}

impl NsqlFunctionContext {
    fn render_markdown(&self) -> String {
        let mut context = format!("{}:\n", self.summary);

        write_function_entries(&mut context, "JSON functions", &self.json);
        write_function_entries(
            &mut context,
            "Spice-specific functions",
            &self.spice_specific,
        );

        if !self.spark_compatibility.functions.is_empty() {
            let _ = writeln!(
                context,
                "\n### Spark compatibility scalar functions\n{}",
                self.spark_compatibility.description
            );
            write_wrapped_function_names(&mut context, &self.spark_compatibility.functions);
        }

        write_user_function_context_entries(&mut context, &self.user_defined);
        context
    }
}

fn nsql_function_context(
    app: &app::App,
    available_names: &HashSet<String>,
    search_functions: SearchFunctionAvailability,
) -> NsqlFunctionContext {
    let json =
        available_context_entries(json_function_entries(), available_names, search_functions);
    let spice_specific =
        available_context_entries(spice_function_entries(), available_names, search_functions);

    let mut spark_function_names = datafusion_spark::all_default_scalar_functions()
        .into_iter()
        .map(|function| function.name().to_string())
        .filter(|name| available_names.contains(&name.to_ascii_lowercase()))
        .unique_by(|name| name.to_ascii_lowercase())
        .collect_vec();
    spark_function_names.sort_by_key(|name| name.to_ascii_lowercase());

    NsqlFunctionContext {
        summary: NSQL_FUNCTION_CONTEXT_SUMMARY.to_string(),
        json,
        spice_specific,
        spark_compatibility: NsqlSparkFunctionContext {
            description: "Spark-compatible scalar functions are available for arrays, maps, structs, dates, strings, hashes, URLs, XML, and other common Spark SQL expressions.".to_string(),
            functions: spark_function_names,
        },
        user_defined: user_function_context(app, available_names),
    }
}

pub(crate) fn render_nsql_dataset_relationship_context(datasets: &[NsqlDatasetContext]) -> String {
    let mut context = String::new();

    for dataset in datasets {
        let has_relationship_context = !dataset.primary_key.is_empty()
            || !dataset.unique_constraints.is_empty()
            || !dataset.foreign_keys.is_empty()
            || !dataset.indexes.is_empty()
            || !dataset.search.vector.is_empty()
            || !dataset.search.full_text.is_empty();
        if !has_relationship_context {
            continue;
        }

        let _ = writeln!(context, "\n### `{}`", dataset.name);

        if !dataset.primary_key.is_empty() {
            let keys = dataset
                .primary_key
                .iter()
                .map(|column| format!("`{column}`"))
                .join(", ");
            let _ = writeln!(context, "- Primary key: {keys}");
        }

        if !dataset.unique_constraints.is_empty() {
            let _ = writeln!(context, "- Unique constraints:");
            for columns in &dataset.unique_constraints {
                let columns = columns
                    .iter()
                    .map(|column| format!("`{column}`"))
                    .join(", ");
                let _ = writeln!(context, "  - ({columns})");
            }
        }

        if !dataset.foreign_keys.is_empty() {
            let _ = writeln!(context, "- Foreign keys:");
            for foreign_key in &dataset.foreign_keys {
                let columns = foreign_key
                    .columns
                    .iter()
                    .map(|column| format!("`{column}`"))
                    .join(", ");
                let foreign_columns = foreign_key
                    .foreign_columns
                    .iter()
                    .map(|column| format!("`{column}`"))
                    .join(", ");
                let _ = writeln!(
                    context,
                    "  - ({columns}) references `{}` ({foreign_columns})",
                    foreign_key.foreign_table
                );
            }
        }

        if !dataset.indexes.is_empty() {
            let _ = writeln!(context, "- Indexes:");
            for index in &dataset.indexes {
                let columns = index
                    .columns
                    .iter()
                    .map(|column| format!("`{column}`"))
                    .join(", ");
                if let Some(name) = &index.name {
                    let _ = writeln!(
                        context,
                        "  - `{name}`: {columns} ({}, {})",
                        index.kind, index.source
                    );
                } else {
                    let _ = writeln!(context, "  - {columns} ({}, {})", index.kind, index.source);
                }
            }
        }

        if !dataset.search.vector.is_empty() || !dataset.search.full_text.is_empty() {
            let _ = writeln!(context, "- Search indexes:");
            for search in &dataset.search.vector {
                let _ = write!(
                    context,
                    "  - vector search on `{}` using `{}`. Syntax: `{}`.",
                    search.column, search.model, search.syntax
                );
                if let Some(engine) = &search.engine {
                    let _ = write!(context, " Engine/index: `{engine}`.");
                }
                if !search.row_id_columns.is_empty() {
                    let keys = search
                        .row_id_columns
                        .iter()
                        .map(|column| format!("`{column}`"))
                        .join(", ");
                    let _ = write!(context, " Row id: {keys}.");
                }
                let _ = writeln!(context);
            }
            for search in &dataset.search.full_text {
                let _ = write!(
                    context,
                    "  - full-text search on `{}` using `{}`. Syntax: `{}`.",
                    search.column, search.engine, search.syntax
                );
                if !search.row_id_columns.is_empty() {
                    let keys = search
                        .row_id_columns
                        .iter()
                        .map(|column| format!("`{column}`"))
                        .join(", ");
                    let _ = write!(context, " Row id: {keys}.");
                }
                let _ = writeln!(context);
            }
        }
    }

    context
}

pub(crate) fn render_nsql_context_block(
    tables: &[TableReference],
    schema_context: &str,
    relationship_context: &str,
    function_context: &str,
    sample_context_blocks: &[SampleContextBlock],
) -> String {
    let mut context = String::from("# Spice.ai NSQL Context\n");
    for instruction in NSQL_CONTEXT_INSTRUCTIONS {
        let _ = writeln!(context, "- {instruction}");
    }
    let _ = writeln!(
        context,
        "- Schema metadata includes table and column comments/descriptions when supplied by the connector or Spicepod."
    );

    let _ = writeln!(context, "\n## Datasets");
    if tables.is_empty() {
        let _ = writeln!(context, "No datasets are currently in scope.");
    } else {
        for table in tables {
            let _ = writeln!(context, "- `{table}`");
        }
    }

    let _ = writeln!(context, "\n## Schemas");
    if schema_context.trim().is_empty() {
        let _ = writeln!(context, "No schema information is available.");
    } else {
        context.push_str(schema_context.trim());
        context.push('\n');
    }

    if !relationship_context.trim().is_empty() {
        let _ = writeln!(context, "\n## Dataset Relationships");
        context.push_str(relationship_context.trim());
        context.push('\n');
    }

    let _ = writeln!(context, "\n## SQL Functions");
    context.push_str(function_context.trim());
    context.push('\n');

    if !sample_context_blocks.is_empty() {
        let _ = writeln!(context, "\n## Sample Data");
        for sample_context in sample_context_blocks {
            let _ = writeln!(context, "\n### {}", sample_context.title);
            context.push_str(sample_context.content.trim());
            context.push('\n');
        }
    }

    context
}

fn validate_requested_datasets(
    datasets: Option<&[String]>,
    table_allowlist: Option<&ResolvedTableAwareAllowlist>,
) -> Result<(), NsqlContextError> {
    if let (Some(requested_datasets), Some(allowlist)) = (datasets, table_allowlist) {
        for dataset in requested_datasets {
            let table_ref = TableReference::parse_str(dataset);
            if !allowlist.table_is_allowed(&table_ref) {
                return Err(NsqlContextError::DatasetNotFound {
                    dataset: dataset.clone(),
                });
            }
        }
    }

    Ok(())
}

/// Construct a [`ResolvedTableAwareAllowlist`] based on the `App`'s `model.datasets`.
fn table_allowlist(
    model_name: &str,
    app: &app::App,
) -> Result<Option<ResolvedTableAwareAllowlist>, NsqlContextError> {
    let model_datasets = app
        .models
        .iter()
        .find(|m| m.name == model_name)
        .map(|m| m.datasets.clone())
        .unwrap_or_default();

    let table_allowlist = if model_datasets.is_empty() {
        None
    } else {
        match ResolvedTableAwareAllowlist::with_defaults(
            SPICE_DEFAULT_CATALOG,
            SPICE_DEFAULT_SCHEMA,
        )
        .with_table_patterns(model_datasets)
        {
            Ok(allowlist) => Some(allowlist),
            Err(_) => {
                return Err(NsqlContextError::InvalidModelDatasets {
                    model_name: model_name.to_string(),
                });
            }
        }
    };
    Ok(table_allowlist)
}

pub(crate) async fn build_nsql_context(
    rt: Arc<Runtime>,
    context: &Arc<RequestContext>,
    model: &str,
    options: NsqlContextOptions,
    datasets: Option<Vec<String>>,
) -> Result<NsqlContext, NsqlContextError> {
    let df = get_current_datafusion(context);

    let Some(app) = rt.read_app().await else {
        return Err(NsqlContextError::AppNotPrepared);
    };

    let table_allowlist_opt = table_allowlist(model, &app)?;

    validate_requested_datasets(datasets.as_deref(), table_allowlist_opt.as_ref())?;

    let tables = datasets.map_or_else(
        || {
            df.get_user_table_names()
                .into_iter()
                .filter(|table| {
                    table_allowlist_opt
                        .as_ref()
                        .is_none_or(|allowlist| allowlist.table_is_allowed(table))
                })
                .collect()
        },
        |datasets| datasets.iter().map(TableReference::from).collect_vec(),
    );

    let schema_context =
        table_schema_context(&tables, Arc::clone(&rt), table_allowlist_opt.clone())
            .instrument(Span::current())
            .await
            .map_err(|source| {
                tracing::error!("Error getting schema context: {source}");
                NsqlContextError::SchemaContext { source }
            })?;

    let sample_context_blocks = if options.include_sampling || options.include_examples {
        sample_context_blocks(
            &tables,
            Arc::clone(&rt),
            table_allowlist_opt.clone(),
            options,
        )
        .instrument(Span::current())
        .await
        .map_err(|source| {
            tracing::error!("Error sampling datasets for NSQL context: {source}");
            NsqlContextError::SampleContext { source }
        })?
    } else {
        vec![]
    };

    let available_names = available_function_names(&rt);
    let configured_search_functions = search_function_availability(&available_names);

    let dataset_contexts =
        dataset_contexts(&tables, Arc::clone(&rt), &app, configured_search_functions)
            .instrument(Span::current())
            .await
            .map_err(|source| {
                tracing::error!("Error getting structured dataset context: {source}");
                NsqlContextError::SchemaContext { source }
            })?;

    let dataset_search_functions = SearchFunctionAvailability {
        vector_search: dataset_contexts
            .iter()
            .any(|dataset| !dataset.search.vector.is_empty()),
        text_search: dataset_contexts
            .iter()
            .any(|dataset| !dataset.search.full_text.is_empty()),
    };
    let function_context = nsql_function_context(&app, &available_names, dataset_search_functions);
    let function_context_block = function_context.render_markdown();
    let relationship_context = render_nsql_dataset_relationship_context(&dataset_contexts);

    let context_block = render_nsql_context_block(
        &tables,
        &schema_context,
        &relationship_context,
        &function_context_block,
        &sample_context_blocks,
    );
    let message = context_message(&context_block)
        .map_err(|message| NsqlContextError::ContextMessage { message })?;
    let json = NsqlContextJsonResponse {
        context: context_block.clone(),
        instructions: nsql_context_instructions(),
        sql: nsql_sql_context(),
        datasets: dataset_contexts,
        functions: function_context,
        samples: sample_context_blocks.clone(),
    };

    Ok(NsqlContext {
        json,
        message,
        table_allowlist: table_allowlist_opt,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry_names(entries: &[NsqlFunctionContextEntry]) -> BTreeSet<String> {
        entries
            .iter()
            .map(|entry| entry.name.to_ascii_lowercase())
            .collect()
    }

    #[test]
    fn function_context_includes_registered_json_and_spark_functions() {
        let app = app::AppBuilder::new("test").build();
        let available_names = all_context_function_names_for_test();

        let context = nsql_function_context_for_test(&app, &available_names, true, true);

        let json_names = entry_names(&context.json);
        for expected_name in json_function_entries()
            .iter()
            .map(|entry| entry.name.as_str())
        {
            assert!(
                json_names.contains(expected_name),
                "missing JSON function {expected_name} from NSQL context"
            );
        }

        let expected_spark_names = datafusion_spark::all_default_scalar_functions()
            .into_iter()
            .map(|function| function.name().to_ascii_lowercase())
            .collect::<BTreeSet<_>>();
        let actual_spark_names = context
            .spark_compatibility
            .functions
            .iter()
            .map(|name| name.to_ascii_lowercase())
            .collect::<BTreeSet<_>>();

        assert!(
            expected_spark_names.len() > 20,
            "expected DataFusion Spark compatibility to expose many functions"
        );
        assert_eq!(actual_spark_names, expected_spark_names);
    }

    #[test]
    fn function_context_requires_text_and_vector_search_for_fusion_and_rerank() {
        let app = app::AppBuilder::new("test").build();
        let available_names = HashSet::from([
            TEXT_SEARCH_UDTF_NAME.to_string(),
            VECTOR_SEARCH_UDTF_NAME.to_string(),
            RRF_UDF_NAME.to_string(),
            RERANK_UDTF_NAME.to_string(),
        ]);

        let text_only = nsql_function_context_for_test(&app, &available_names, false, true);
        let text_only_names = entry_names(&text_only.spice_specific);
        assert!(text_only_names.contains(TEXT_SEARCH_UDTF_NAME));
        assert!(!text_only_names.contains(VECTOR_SEARCH_UDTF_NAME));
        assert!(!text_only_names.contains(RRF_UDF_NAME));
        assert!(!text_only_names.contains(RERANK_UDTF_NAME));

        let vector_only = nsql_function_context_for_test(&app, &available_names, true, false);
        let vector_only_names = entry_names(&vector_only.spice_specific);
        assert!(!vector_only_names.contains(TEXT_SEARCH_UDTF_NAME));
        assert!(vector_only_names.contains(VECTOR_SEARCH_UDTF_NAME));
        assert!(!vector_only_names.contains(RRF_UDF_NAME));
        assert!(!vector_only_names.contains(RERANK_UDTF_NAME));

        let hybrid = nsql_function_context_for_test(&app, &available_names, true, true);
        let hybrid_names = entry_names(&hybrid.spice_specific);
        assert!(hybrid_names.contains(TEXT_SEARCH_UDTF_NAME));
        assert!(hybrid_names.contains(VECTOR_SEARCH_UDTF_NAME));
        assert!(hybrid_names.contains(RRF_UDF_NAME));
        assert!(hybrid_names.contains(RERANK_UDTF_NAME));
    }
}
