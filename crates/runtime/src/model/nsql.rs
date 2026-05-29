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
    collections::{HashMap, HashSet},
    error::Error as StdError,
    fmt::Write,
    sync::Arc,
};

use async_openai::types::chat::{
    ChatCompletionRequestMessage, ChatCompletionRequestSystemMessageArgs,
};
use datafusion::execution::FunctionRegistry;
use datafusion::sql::TableReference;
use datafusion_functions_json::udfs::{
    json_as_text_udf, json_contains_udf, json_from_scalar_udf, json_get_array_udf,
    json_get_bool_udf, json_get_float_udf, json_get_int_udf, json_get_json_udf, json_get_str_udf,
    json_get_udf, json_length_udf, json_object_keys_udf,
};
use futures::{StreamExt, TryStreamExt};
use itertools::Itertools;
use runtime_datafusion::allowlist::ResolvedTableAwareAllowlist;
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
use serde::Serialize;
use serde_json::Value;
use snafu::Snafu;
use spicepod::component::function::Function;
use tracing::Span;
use tracing_futures::Instrument;

#[cfg(feature = "models")]
use runtime_datafusion_udfs::{ai::AI_UDF_NAME, embed::EMBED_UDF_NAME};

use crate::{
    Runtime,
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
    embeddings::udtf::VECTOR_SEARCH_UDTF_NAME,
    search::{full_text::udtf::TEXT_SEARCH_UDTF_NAME, rerank::RERANK_UDTF_NAME, rrf::RRF_UDF_NAME},
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

pub(crate) struct SampleContextBlock {
    pub(crate) title: String,
    pub(crate) content: String,
}

pub(crate) struct NsqlContext {
    pub(crate) block: String,
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
            "text_search(table => 'dataset', query => 'text')",
            "Runs full-text search over a configured searchable dataset.",
        )
        .with_example("SELECT * FROM text_search(table => 'docs', query => 'refund policy')"),
        FunctionContextEntry::new(
            VECTOR_SEARCH_UDTF_NAME,
            "vector_search(table => 'dataset', query => 'text')",
            "Runs vector search over a configured searchable dataset.",
        )
        .with_example("SELECT * FROM vector_search(table => 'docs', query => 'refund policy')"),
        FunctionContextEntry::new(
            RRF_UDF_NAME,
            "rrf(text_search(...), vector_search(...))",
            "Combines text and vector search results with reciprocal rank fusion.",
        )
        .with_example("SELECT * FROM rrf(text_search(table => 'docs', query => 'refund'), vector_search(table => 'docs', query => 'refund'))"),
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

fn write_function_entries(
    output: &mut String,
    section: &str,
    entries: Vec<FunctionContextEntry>,
    available_names: &HashSet<String>,
) {
    let available_entries = entries
        .into_iter()
        .filter(|entry| available_names.contains(&entry.name.to_ascii_lowercase()))
        .collect_vec();

    if available_entries.is_empty() {
        return;
    }

    let _ = writeln!(output, "\n### {section}");
    for entry in available_entries {
        let _ = write!(
            output,
            "- `{}`: {} Syntax: `{}`.",
            entry.name, entry.description, entry.syntax
        );
        if let Some(example) = entry.example {
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

#[derive(Debug, PartialEq, Eq)]
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

fn write_user_function_context(
    output: &mut String,
    app: &app::App,
    available_names: &HashSet<String>,
) {
    let user_functions = user_function_context_entries(app, available_names, user_function_infos());
    write_user_function_context_entries(output, &user_functions);
}

fn nsql_function_context(rt: &Runtime, app: &app::App) -> String {
    let available_names = available_function_names(rt);
    let mut context = String::from(
        "Use Spice.ai/DataFusion SQL. Standard DataFusion SQL functions are available. The Spice runtime also exposes these functions when useful for Text-to-SQL, including registered user-defined functions. Function references are filtered to functions registered in the current DataFusion context:\n",
    );

    write_function_entries(
        &mut context,
        "JSON functions",
        json_function_entries(),
        &available_names,
    );
    write_function_entries(
        &mut context,
        "Spice-specific functions",
        spice_function_entries(),
        &available_names,
    );

    let mut spark_function_names = datafusion_spark::all_default_scalar_functions()
        .into_iter()
        .map(|function| function.name().to_string())
        .filter(|name| available_names.contains(&name.to_ascii_lowercase()))
        .unique_by(|name| name.to_ascii_lowercase())
        .collect_vec();
    spark_function_names.sort_by_key(|name| name.to_ascii_lowercase());

    if !spark_function_names.is_empty() {
        let _ = writeln!(
            context,
            "\n### Spark compatibility scalar functions\nSpark-compatible scalar functions are available for arrays, maps, structs, dates, strings, hashes, URLs, XML, and other common Spark SQL expressions."
        );
        write_wrapped_function_names(&mut context, &spark_function_names);
    }

    write_user_function_context(&mut context, app, &available_names);
    context
}

pub(crate) fn render_nsql_context_block(
    tables: &[TableReference],
    schema_context: &str,
    function_context: &str,
    sample_context_blocks: &[SampleContextBlock],
) -> String {
    let mut context = String::from(
        "# Spice.ai NSQL Context\n\nUse this context to write SQL for the Spice runtime. Return only valid SQL code, without markdown fences. Quote column names that contain capitals. For tables with schemas and catalogs, use `\"catalog\".\"schema\".\"table\"`, not `\"catalog.schema.table\"`. Schema metadata includes table and column comments/descriptions when supplied by the connector or Spicepod.\n",
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

    let context_block = render_nsql_context_block(
        &tables,
        &schema_context,
        &nsql_function_context(&rt, &app),
        &sample_context_blocks,
    );
    let message = context_message(&context_block)
        .map_err(|message| NsqlContextError::ContextMessage { message })?;

    Ok(NsqlContext {
        block: context_block,
        message,
        table_allowlist: table_allowlist_opt,
    })
}
