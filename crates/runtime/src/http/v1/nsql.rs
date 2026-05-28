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
use crate::{
    Runtime,
    datafusion::{
        SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA,
        request_context_extension::get_current_datafusion,
        udf::{UserFunctionInfo, effective_user_function_volatility, user_function_infos},
    },
    http::v1::{ResponseMetadata, ResponseMimeType, to_http_response},
    model::LLMChatCompletionsModelStore,
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
};
use async_openai::types::chat::{
    ChatCompletionRequestMessage, ChatCompletionRequestSystemMessageArgs,
};
use axum::{
    Extension, Json,
    extract::Query,
    http::StatusCode,
    response::{
        IntoResponse, Response, Sse,
        sse::{Event, KeepAlive},
    },
};
use axum_extra::TypedHeader;
use datafusion::execution::FunctionRegistry;
use datafusion::sql::TableReference;
use futures::{StreamExt, TryStreamExt};
use headers_accept::Accept;
use http::{HeaderMap, HeaderValue, header::CONTENT_TYPE};
use runtime_datafusion::allowlist::ResolvedTableAwareAllowlist;
use runtime_request_context::{AsyncMarker, RequestContext};

use arrow::array::RecordBatch;
use itertools::Itertools;
use llms::chat::nsql::{FailedAttempt, QueryGenerationContext, default::DefaultSqlGeneration};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use spicepod::component::{function::Function, model::ModelType};
use std::{
    collections::{HashMap, HashSet},
    fmt::Write,
    sync::Arc,
    time::Duration,
};
use tokio::sync::RwLock;
use tracing::Span;
use tracing_futures::Instrument;

use super::accept_header_types;
use crate::datafusion::query::QueryBuilder;

// Default number of retries for NSQL queries if the generated query fails to execute
const DEFAULT_NSQL_RETRIES: u8 = 10;

// Maximum number of concurrent sampling tools executions for NSQL
const DATA_SAMPLING_MAX_CONCURRENT: usize = 10;

const DEFAULT_NSQL_CONTEXT_SAMPLE_LIMIT: usize = 3;
const MAX_NSQL_CONTEXT_SAMPLE_LIMIT: usize = 100;

// NSQL streaming keep alive interval in seconds
const NSQL_STREAM_KEEP_ALIVE: u64 = 30;

fn clean_model_based_sql(input: &str) -> String {
    let no_dashes = match input.strip_prefix("--") {
        Some(rest) => rest.to_string(),
        None => input.to_string(),
    };

    // Only take the first query, if there are multiple.
    let one_query = no_dashes.split(';').next().unwrap_or(&no_dashes);
    one_query.trim().to_string()
}

struct SampleContextBlock {
    title: String,
    content: String,
}

struct NsqlContext {
    block: String,
    message: ChatCompletionRequestMessage,
    table_allowlist: Option<ResolvedTableAwareAllowlist>,
}

#[derive(Clone, Copy, Debug)]
struct NsqlContextOptions {
    include_sampling: bool,
    sampling_limit: usize,
    include_examples: bool,
    examples_limit: usize,
}

impl NsqlContextOptions {
    fn from_nsql_request(sample_data_enabled: bool) -> Self {
        Self {
            include_sampling: sample_data_enabled,
            sampling_limit: DEFAULT_NSQL_CONTEXT_SAMPLE_LIMIT,
            include_examples: sample_data_enabled,
            examples_limit: DEFAULT_NSQL_CONTEXT_SAMPLE_LIMIT,
        }
    }
}

fn default_nsql_context_sample_limit() -> usize {
    DEFAULT_NSQL_CONTEXT_SAMPLE_LIMIT
}

fn validate_context_limit(
    name: &str,
    enabled: bool,
    limit: usize,
) -> Result<(), (StatusCode, String)> {
    if enabled && limit == 0 {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("Query parameter '{name}' must be greater than 0 when enabled"),
        ));
    }

    if limit > MAX_NSQL_CONTEXT_SAMPLE_LIMIT {
        return Err((
            StatusCode::BAD_REQUEST,
            format!(
                "Query parameter '{name}' must be less than or equal to {MAX_NSQL_CONTEXT_SAMPLE_LIMIT}"
            ),
        ));
    }

    Ok(())
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
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
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
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
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
) -> Result<Vec<SampleContextBlock>, Box<dyn std::error::Error + Send + Sync>> {
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

#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "lowercase")]
pub struct Request {
    /// The natural language query to be converted into SQL
    pub query: String,

    /// The name of the model to use for SQL generation. If omitted, Spice defaults to the only compatible LLM model configured in the Spicepod.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,

    /// If true, streams the response instead of waiting for completion
    #[serde(default)]
    pub stream: bool,

    /// Whether sample data is included in the context for SQL generation. Default: false
    #[serde(default = "default_sample_data_enabled")]
    pub sample_data_enabled: bool,

    /// Names of datasets to sample from when constructing model context; this is a sampling hint and does not restrict which tables queries can target. If omitted, all datasets are used.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub datasets: Option<Vec<String>>,

    /// Stable prompt-cache key forwarded to the configured NSQL model for provider-specific cache handling.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt_cache_key: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::IntoParams, utoipa::ToSchema))]
#[cfg_attr(feature = "openapi", into_params(parameter_in = Query))]
#[serde(rename_all = "lowercase")]
pub struct ContextRequest {
    /// The name of the model whose dataset allowlist should be used. If omitted, Spice defaults to the only compatible LLM model configured in the Spicepod.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,

    /// Whether distinct-value samples are included in the context block. Also accepts `sample_data_enabled` for compatibility with `/v1/nsql` request bodies. Default: false.
    #[serde(default, alias = "sample_data_enabled")]
    pub include_sampling: bool,

    /// Maximum number of rows per distinct-value sample. Default: 3, maximum: 100.
    #[serde(default = "default_nsql_context_sample_limit")]
    pub sampling_limit: usize,

    /// Whether example rows are included in the context block. Default: false.
    #[serde(default)]
    pub include_examples: bool,

    /// Maximum number of example rows per dataset. Default: 3, maximum: 100.
    #[serde(default = "default_nsql_context_sample_limit")]
    pub examples_limit: usize,

    /// Names of datasets to include in the context block. If omitted, all datasets visible to the selected model are included.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub datasets: Option<Vec<String>>,
}

impl ContextRequest {
    fn context_options(&self) -> Result<NsqlContextOptions, (StatusCode, String)> {
        validate_context_limit("sampling_limit", self.include_sampling, self.sampling_limit)?;
        validate_context_limit("examples_limit", self.include_examples, self.examples_limit)?;

        Ok(NsqlContextOptions {
            include_sampling: self.include_sampling,
            sampling_limit: self.sampling_limit,
            include_examples: self.include_examples,
            examples_limit: self.examples_limit,
        })
    }
}

fn default_sample_data_enabled() -> bool {
    false
}

/// Checks if the request is asking to only generate SQL.
fn return_sql_only(accept: Option<&TypedHeader<Accept>>) -> bool {
    accept.is_some_and(|a| accept_header_types(a).contains(&"application/sql".to_string()))
}

fn context_response_content_type(accept: Option<&TypedHeader<Accept>>) -> HeaderValue {
    if accept.is_some_and(|header| {
        let accepted = accept_header_types(header);
        accepted
            .iter()
            .any(|content_type| content_type == "text/plain")
            && !accepted
                .iter()
                .any(|content_type| content_type == "text/markdown")
    }) {
        HeaderValue::from_static("text/plain; charset=utf-8")
    } else {
        HeaderValue::from_static("text/markdown; charset=utf-8")
    }
}

fn nsql_context_response(context: String, accept: Option<TypedHeader<Accept>>) -> Response {
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, context_response_content_type(accept.as_ref()));
    (StatusCode::OK, headers, context).into_response()
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
    name: &'static str,
    syntax: &'static str,
    description: &'static str,
}

fn json_function_entries() -> Vec<FunctionContextEntry> {
    vec![
        FunctionContextEntry {
            name: "json_get",
            syntax: "json_get(json, path)",
            description: "Returns a JSON union value at a path. Use typed helpers when a scalar type is known.",
        },
        FunctionContextEntry {
            name: "json_get_str",
            syntax: "json_get_str(json, path)",
            description: "Returns a string value from JSON at a path.",
        },
        FunctionContextEntry {
            name: "json_get_int",
            syntax: "json_get_int(json, path)",
            description: "Returns an integer value from JSON at a path.",
        },
        FunctionContextEntry {
            name: "json_get_float",
            syntax: "json_get_float(json, path)",
            description: "Returns a floating-point value from JSON at a path.",
        },
        FunctionContextEntry {
            name: "json_get_bool",
            syntax: "json_get_bool(json, path)",
            description: "Returns a boolean value from JSON at a path.",
        },
        FunctionContextEntry {
            name: "json_get_json",
            syntax: "json_get_json(json, path)",
            description: "Returns a JSON string value from JSON at a path.",
        },
        FunctionContextEntry {
            name: "json_get_array",
            syntax: "json_get_array(json, path)",
            description: "Returns an array value from JSON at a path.",
        },
        FunctionContextEntry {
            name: "json_as_text",
            syntax: "json_as_text(json, path)",
            description: "Returns the JSON value at a path as text.",
        },
        FunctionContextEntry {
            name: "json_contains",
            syntax: "json_contains(json, value)",
            description: "Returns whether a JSON value contains another JSON value.",
        },
        FunctionContextEntry {
            name: "json_length",
            syntax: "json_length(json[, path])",
            description: "Returns the length of a JSON array or object.",
        },
        FunctionContextEntry {
            name: "json_object_keys",
            syntax: "json_object_keys(json)",
            description: "Returns object keys from a JSON value.",
        },
        FunctionContextEntry {
            name: "json_from_scalar",
            syntax: "json_from_scalar(value)",
            description: "Converts a scalar SQL value into JSON.",
        },
    ]
}

fn spice_function_entries() -> Vec<FunctionContextEntry> {
    vec![
        FunctionContextEntry {
            name: "flatten_json",
            syntax: "flatten_json(json)",
            description: "Flattens a JSON document into key/value rows. Use as a table function for literal JSON or with UNNEST for column values.",
        },
        FunctionContextEntry {
            name: "flatten_json_properties",
            syntax: "flatten_json_properties(json_schema)",
            description: "Flattens a JSON Schema document into rows describing nested properties.",
        },
        FunctionContextEntry {
            name: "json_tree",
            syntax: "json_tree(json)",
            description: "Walks a JSON document recursively and returns one row per node.",
        },
        FunctionContextEntry {
            name: "text_search",
            syntax: "text_search(table => 'dataset', query => 'text')",
            description: "Runs full-text search over a configured searchable dataset.",
        },
        FunctionContextEntry {
            name: "vector_search",
            syntax: "vector_search(table => 'dataset', query => 'text')",
            description: "Runs vector search over a configured searchable dataset.",
        },
        FunctionContextEntry {
            name: "rrf",
            syntax: "rrf(text_search(...), vector_search(...))",
            description: "Combines text and vector search results with reciprocal rank fusion.",
        },
        FunctionContextEntry {
            name: "rerank",
            syntax: "rerank(input => TABLE(...), model => 'model')",
            description: "Reranks search results with a configured reranker model.",
        },
        FunctionContextEntry {
            name: "cosine_distance",
            syntax: "cosine_distance(vector_a, vector_b)",
            description: "Computes cosine distance between two vector/list values.",
        },
        FunctionContextEntry {
            name: "inner_product",
            syntax: "inner_product(vector_a, vector_b)",
            description: "Computes inner product between two vector/list values.",
        },
        FunctionContextEntry {
            name: "l2_distance",
            syntax: "l2_distance(vector_a, vector_b)",
            description: "Computes Euclidean distance between two vector/list values.",
        },
        FunctionContextEntry {
            name: "l2_squared_distance",
            syntax: "l2_squared_distance(vector_a, vector_b)",
            description: "Computes squared Euclidean distance between two vector/list values.",
        },
        FunctionContextEntry {
            name: "l2_norm",
            syntax: "l2_norm(vector)",
            description: "Computes the L2 norm of a vector/list value.",
        },
        FunctionContextEntry {
            name: "embed",
            syntax: "embed(text[, model])",
            description: "Generates an embedding using a configured embedding model.",
        },
        FunctionContextEntry {
            name: "ai",
            syntax: "ai(message[, model])",
            description: "Runs a prompt against a configured chat model from SQL.",
        },
        FunctionContextEntry {
            name: "bucket",
            syntax: "bucket(value, boundaries)",
            description: "Assigns a value to a bucket using ordered boundaries.",
        },
        FunctionContextEntry {
            name: "truncate",
            syntax: "truncate(width, value)",
            description: "Truncates a value using Iceberg/Spark-compatible semantics.",
        },
        FunctionContextEntry {
            name: "digest_many",
            syntax: "digest_many(col_a, col_b, ..., digest_function_name)",
            description: "Hashes multiple column values using a DataFusion digest function such as md5.",
        },
        FunctionContextEntry {
            name: "obj_description",
            syntax: "obj_description(object_id)",
            description: "Returns PostgreSQL-compatible object descriptions when available.",
        },
        FunctionContextEntry {
            name: "col_description",
            syntax: "col_description(table_id, column_number)",
            description: "Returns PostgreSQL-compatible column descriptions when available.",
        },
        FunctionContextEntry {
            name: "list_udfs",
            syntax: "list_udfs()",
            description: "Lists scalar and table UDFs registered in the Spice runtime.",
        },
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
        let _ = writeln!(
            output,
            "- `{}`: {} Syntax: `{}`.",
            entry.name, entry.description, entry.syntax
        );
    }
}

fn write_wrapped_function_names(output: &mut String, names: &[String]) {
    for chunk in names.chunks(8) {
        let rendered_names = chunk.iter().map(|name| format!("`{name}`")).join(", ");
        let _ = writeln!(output, "- {rendered_names}");
    }
}

#[derive(Debug, PartialEq, Eq)]
struct UserFunctionContextEntry {
    name: String,
    syntax: Option<String>,
    kind: String,
    volatility: String,
    from: String,
    description: Option<String>,
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

fn user_function_context_entries(
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

fn write_user_function_context_entries(
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
        if let Some(description) = &function.description {
            if !description.is_empty() {
                let _ = write!(output, " {description}");
            }
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
        "Use Spice.ai/DataFusion SQL. Standard DataFusion SQL functions are available. The Spice runtime also exposes these functions when useful for Text-to-SQL, including registered user-defined functions:\n",
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

fn render_nsql_context_block(
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
    datasets: &Option<Vec<String>>,
    table_allowlist: &Option<ResolvedTableAwareAllowlist>,
) -> Result<(), (StatusCode, String)> {
    if let (Some(requested_datasets), Some(allowlist)) = (datasets, table_allowlist) {
        for dataset in requested_datasets {
            let table_ref = TableReference::parse_str(dataset);
            if !allowlist.table_is_allowed(&table_ref) {
                return Err((
                    StatusCode::BAD_REQUEST,
                    format!("Dataset '{dataset}' not found"),
                ));
            }
        }
    }

    Ok(())
}

async fn build_nsql_context(
    rt: Arc<Runtime>,
    context: &Arc<RequestContext>,
    model: &str,
    options: NsqlContextOptions,
    datasets: Option<Vec<String>>,
) -> Result<NsqlContext, (StatusCode, String)> {
    let df = get_current_datafusion(context);

    let Some(app) = rt.read_app().await else {
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            "Unexpected internal error. App not prepared in runtime.".to_string(),
        ));
    };

    let table_allowlist_opt =
        table_allowlist(model, &app).map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, error))?;

    validate_requested_datasets(&datasets, &table_allowlist_opt)?;

    let tables = datasets
        .map(|datasets| datasets.iter().map(TableReference::from).collect_vec())
        .unwrap_or_else(|| {
            df.get_user_table_names()
                .into_iter()
                .filter(|table| {
                    table_allowlist_opt
                        .as_ref()
                        .is_none_or(|allowlist| allowlist.table_is_allowed(table))
                })
                .collect()
        });

    let schema_context =
        table_schema_context(&tables, Arc::clone(&rt), table_allowlist_opt.clone())
            .instrument(Span::current())
            .await
            .map_err(|error| {
                tracing::error!("Error getting schema context: {error}");
                (StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
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
        .map_err(|error| {
            tracing::error!("Error sampling datasets for NSQL context: {error}");
            (StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
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
        .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, error))?;

    Ok(NsqlContext {
        block: context_block,
        message,
        table_allowlist: table_allowlist_opt,
    })
}

/// Text-to-SQL Context (NSQL)
///
/// Return the same context block that `/v1/nsql` injects into the configured NSQL model.
///
/// The response is a markdown/plain-text block containing the in-scope datasets, schemas,
/// Spice-specific SQL functions, registered user-defined functions, JSON/Spark compatibility functions,
/// and optional sample data.
#[cfg_attr(feature = "openapi", utoipa::path(
    get,
    path = "/v1/nsql/context",
    operation_id = "get_nsql_context",
    tag = "SQL",
    params(
        ("Accept" = String, Header, description = "The format of the response, one of 'text/markdown' (default) or 'text/plain'."),
        ContextRequest
    ),
    responses(
        (status = 200, description = "NSQL context block", content((
            String = "text/markdown",
            example = "# Spice.ai NSQL Context\n\n## Datasets\n- `sales_data`\n\n## Schemas\n| table | column | type | nullable |\n| --- | --- | --- | --- |\n| sales_data | customer_id | Utf8 | false |\n\n## SQL Functions\nUse Spice.ai/DataFusion SQL."
        ), (
            String = "text/plain",
            example = "# Spice.ai NSQL Context\n\n## Datasets\n- `sales_data`"
        ))),
        (status = 400, description = "Invalid request parameters", content((
            String = "application/json", example = "Model nsql not found"
        ))),
        (status = 500, description = "Internal server error", content((
            String, example = "Unexpected internal error. App not prepared in runtime."
        )))
    )
))]
pub(crate) async fn get_context(
    Extension(rt): Extension<Arc<Runtime>>,
    accept: Option<TypedHeader<Accept>>,
    Query(params): Query<ContextRequest>,
) -> Response {
    let request_context = RequestContext::current(AsyncMarker::new().await);

    let context_options = match params.context_options() {
        Ok(options) => options,
        Err((status, message)) => return (status, message).into_response(),
    };

    let model = match resolve_nsql_model_name(params.model, &rt).await {
        Ok(model) => model,
        Err((status, message)) => return (status, message).into_response(),
    };

    let context = match build_nsql_context(
        rt,
        &request_context,
        &model,
        context_options,
        params.datasets,
    )
    .await
    {
        Ok(context) => context,
        Err((status, message)) => return (status, message).into_response(),
    };

    nsql_context_response(context.block, accept)
}

/// Text-to-SQL (NSQL)
///
/// Generate and optionally execute a natural-language text-to-SQL (NSQL) query.
///
/// This endpoint generates a SQL query using a natural language query (NSQL) and optionally executes it.
/// The SQL query is generated by the specified model and executed if the `Accept` header is not set to `application/sql`.
/// When `stream` is true, the response is streamed as Server-Sent Events (SSE).
#[cfg_attr(feature = "openapi", utoipa::path(
    post,
    path = "/v1/nsql",
    operation_id = "post_nsql",
    tag = "SQL",
    params(
        ("Accept" = String, Header, description = "The format of the response, one of 'application/json' (default), 'application/vnd.spiceai.nsql.v1+json', 'application/sql', 'text/csv' or 'text/plain'. 'application/sql' will only return the SQL query generated by the model."),
    ),
    request_body(
        description = "Request body to generate an NSQL query",
        content((
            Request = "application/json",
            example = json!({
                "query": "Get the top 5 customers by total sales",
                "stream": false,
                "sample_data_enabled": false,
                "datasets": ["sales_data"],
                "prompt_cache_key": "sales-dashboard"
            })
        ))
    ),
    responses(
        (status = 200, description = "SQL query executed successfully", content((
            Vec<serde_json::Value> = "application/json",
            example = json!([
                {
                    "customer_id": "12345",
                    "total_sales": 150_000
                },
                {
                    "customer_id": "67890",
                    "total_sales": 125_000
                }
            ])
        ),
        (
            String = "application/sql",
            example = "
            SELECT customer_id, SUM(total_sales)
            FROM sales_data
            GROUP BY customer_id
            ORDER BY SUM(total_sales) DESC
            LIMIT 5
            "
        ),
        (
            serde_json::Value = "application/vnd.spiceai.nsql.v1+json",
            example = json!({
                "row_count": 2,
                "schema": {
                    "fields": [
                    {
                        "name": "customer_id",
                        "data_type": "String",
                        "nullable": false,
                        "dict_id": 0,
                        "dict_is_ordered": false
                    },
                    {
                        "name": "total_sales",
                        "data_type": "Int64",
                        "nullable": false,
                        "dict_id": 0,
                        "dict_is_ordered": false
                    }
                    ]
                },
                "data": [
                    {
                    "customer_id": "12345",
                    "total_sales": 150_000
                    },
                    {
                    "customer_id": "67890",
                    "total_sales": 125_000
                    }
                ],
                "sql": "SELECT customer_id, SUM(total_sales) AS total_sales\nFROM sales_data\nGROUP BY customer_id\nORDER BY total_sales DESC\nLIMIT 5"
            })
        ),
        (
            String = "text/event-stream",
            example = "data: {\"row_count\": 2, \"schema\": {...}, \"data\": [...], \"sql\": \"SELECT ...\"}\n\n"
        ))),
        (status = 400, description = "Invalid request parameters", content((
            String = "application/json", example = "Model nsql not found"
        ))),
        (status = 500, description = "Internal server error", content((
            String, example = "No query produced from NSQL model"
        )))
    )
))]
pub(crate) async fn post(
    Extension(rt): Extension<Arc<Runtime>>,
    Extension(llms): Extension<Arc<RwLock<LLMChatCompletionsModelStore>>>,
    accept: Option<TypedHeader<Accept>>,
    Json(payload): Json<Request>,
) -> Response {
    // track ai_inferences_with_spice_count metric
    let context = RequestContext::current(AsyncMarker::new().await);

    if payload.stream {
        let stream = futures::stream::once(handle_nsql_query(rt, context, llms, accept, payload))
            .map(|(status, _, body)| {
                if status.is_success() {
                    Ok(Event::default().data(body))
                } else {
                    Err(status.to_string())
                }
            });
        Sse::new(stream)
            .keep_alive(
                KeepAlive::new()
                    .interval(Duration::from_secs(NSQL_STREAM_KEEP_ALIVE))
                    .text("nsql still in progress"),
            )
            .into_response()
    } else {
        handle_nsql_query(rt, context, llms, accept, payload)
            .await
            .into_response()
    }
}

pub(crate) async fn handle_nsql_query(
    rt: Arc<Runtime>,
    context: Arc<RequestContext>,
    llms: Arc<RwLock<LLMChatCompletionsModelStore>>,
    accept: Option<TypedHeader<Accept>>,
    payload: Request,
) -> (StatusCode, HeaderMap, String) {
    let df = get_current_datafusion(&context);
    let headers = HeaderMap::new();

    // NSQL-scoped cancellation token (child of the request token). Used for
    // both the LLM race and as the per-query cancellation token passed to
    // `QueryBuilder`. This way `POST /v1/sql/{id}/cancel` against the
    // NSQL-issued query reliably cancels NSQL end-to-end (the inner query
    // registers this same token in the cancel registry).
    let nsql_token = context.child_cancellation_token();

    let Request {
        query,
        model: requested_model,
        sample_data_enabled,
        datasets,
        prompt_cache_key,
        ..
    } = payload;

    let model = match resolve_nsql_model_name(requested_model, &rt).await {
        Ok(model) => model,
        Err((status, message)) => return (status, headers, message),
    };

    crate::model::add_tools_used(&context, 1);

    let span = tracing::span!(target: "task_history", tracing::Level::INFO, "nsql", input = %query, model = %model, "labels");

    if let Some(traceparent) = context.trace_parent() {
        crate::http::traceparent::override_task_history_with_trace_parent(&span, traceparent);
    }

    let nsql_context = match build_nsql_context(
        Arc::clone(&rt),
        &context,
        &model,
        NsqlContextOptions::from_nsql_request(sample_data_enabled),
        datasets,
    )
    .instrument(span.clone())
    .await
    {
        Ok(context) => context,
        Err((status, message)) => return (status, headers, message),
    };
    let table_allowlist_opt = nsql_context.table_allowlist.clone();

    let nql_model = {
        let models = llms.read().await;
        let Some(nql_model) = models.get(&model) else {
            return (
                StatusCode::BAD_REQUEST,
                headers,
                format!("Model {model} not found"),
            );
        };
        Arc::clone(nql_model)
    };

    let default_sql_generation = DefaultSqlGeneration {};
    let sql_gen = nql_model.as_sql().unwrap_or(&default_sql_generation);
    // Tracks previously generated queries and associated errors to enable an efficient retry mechanism
    let mut sql_gen_ctx = QueryGenerationContext::default();
    let mut num_retries = 0;

    loop {
        // Cooperative cancellation: bail out between LLM/query iterations if
        // the NSQL token was cancelled (request token cancel propagates to
        // this child, and admin cancel via the inner query id cancels this
        // token directly).
        if nsql_token.is_cancelled() {
            return (
                StatusCode::from_u16(499).unwrap_or(StatusCode::REQUEST_TIMEOUT),
                headers,
                "NSQL request cancelled".to_string(),
            );
        }

        let Ok(mut req) = sql_gen.create_request_for_query(&model, &query, &sql_gen_ctx) else {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                headers,
                "Error preparing data for NQL model".to_string(),
            );
        };

        req.messages.push(nsql_context.message.clone());
        if let Some(prompt_cache_key) = &prompt_cache_key {
            req.prompt_cache_key = Some(prompt_cache_key.clone());
        }

        // Race the LLM call against the NSQL cancellation token so that a
        // long-running model inference does not pin the request after a
        // cancel/disconnect. Dropping the chat_request future tears down the
        // underlying client/network resources.
        let chat_fut = nql_model.chat_request(req).instrument(span.clone());
        let resp = tokio::select! {
            biased;
            () = nsql_token.cancelled() => {
                return (
                    StatusCode::from_u16(499).unwrap_or(StatusCode::REQUEST_TIMEOUT),
                    headers,
                    "NSQL request cancelled".to_string(),
                );
            }
            res = chat_fut => match res {
                Ok(r) => r,
                Err(e) => {
                    tracing::error!("Error running NQL model: {e}");
                    return (StatusCode::INTERNAL_SERVER_ERROR, headers, e.to_string());
                }
            }
        };

        // Run the SQL from the NSQL model through datafusion.
        match sql_gen.parse_response(resp) {
            Ok(Some(model_sql_query)) => {
                let cleaned_query = clean_model_based_sql(&model_sql_query);

                if return_sql_only(accept.as_ref()) {
                    tracing::trace!("Not running query, requested SQL only:\n{cleaned_query}");
                    return (StatusCode::OK, headers, cleaned_query);
                }

                tracing::debug!("Running query:\n{cleaned_query}");

                // Run the SQL with table allowlist enforcement. LLM-generated SQL is
                // always executed in read-only mode: the runtime rejects any plan that
                // contains DDL, DML, COPY, or a `LogicalPlan::Statement` node (including
                // PREPARE/EXECUTE/DEALLOCATE) regardless of per-catalog writability,
                // which mitigates model-mediated SQL injection on `/v1/nsql`.
                let query_result = {
                    let mut builder = QueryBuilder::new(&cleaned_query, Arc::clone(&df))
                        .read_only(true)
                        .cancellation_token(nsql_token.clone());
                    if let Some(ref allowlist) = table_allowlist_opt {
                        builder = builder.allow_tables(allowlist.clone());
                    }
                    builder.build().run().await
                };

                match query_result {
                    Ok(result) => match result.data.try_collect::<Vec<RecordBatch>>().await {
                        Ok(data) => {
                            return to_http_response(
                                data,
                                result.cache_status,
                                ResponseMimeType::from_accept_header(accept.as_ref()),
                                ResponseMetadata::empty().with_sql(&cleaned_query),
                            )
                            .instrument(span.clone())
                            .await;
                        }
                        Err(e) => {
                            if num_retries >= DEFAULT_NSQL_RETRIES {
                                tracing::error!("Error collecting query results: {e}");
                                return (StatusCode::BAD_REQUEST, headers, e.to_string());
                            }

                            tracing::debug!("Error collecting query results: {e}. Retrying...");

                            num_retries += 1;
                            sql_gen_ctx
                                .failed_attempts
                                .push(FailedAttempt::new(cleaned_query.clone(), e.to_string()));
                        }
                    },
                    Err(e) => {
                        // If query failed, retry with the updated context

                        if num_retries >= DEFAULT_NSQL_RETRIES {
                            tracing::error!("Error executing query: {e}");
                            return (StatusCode::BAD_REQUEST, headers, e.to_string());
                        }

                        tracing::debug!("Error executing query: {e}. Retrying...");

                        num_retries += 1;
                        sql_gen_ctx
                            .failed_attempts
                            .push(FailedAttempt::new(cleaned_query.clone(), e.to_string()));
                    }
                }
            }
            Ok(None) => {
                tracing::trace!("No query produced from NSQL model");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    headers,
                    "No query produced from NSQL model".to_string(),
                );
            }
            Err(e) => {
                tracing::error!("Error running NSQL model: {e}");
                return (StatusCode::INTERNAL_SERVER_ERROR, headers, e.to_string());
            }
        }
    }
}

async fn resolve_nsql_model_name(
    requested_model: Option<String>,
    rt: &Arc<Runtime>,
) -> Result<String, (StatusCode, String)> {
    if let Some(model) = requested_model {
        return Ok(model);
    }

    let Some(app) = rt.read_app().await else {
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            "Unexpected internal error. App not prepared in runtime.".to_string(),
        ));
    };

    resolve_nsql_model_name_from_app(app.as_ref())
        .map_err(|message| (StatusCode::BAD_REQUEST, message))
}

fn resolve_nsql_model_name_from_app(app: &app::App) -> Result<String, String> {
    let compatible_models = compatible_nsql_model_names(app);

    match compatible_models.as_slice() {
        [] => Err(
            "No model specified and no compatible LLM model is configured. Add exactly one LLM model to the Spicepod or include the 'model' field in the request."
                .to_string(),
        ),
        [model] => Ok(model.clone()),
        models => Err(format!(
            "No model specified and multiple compatible LLM models are configured ({}). Include the 'model' field in the request.",
            models.join(", ")
        )),
    }
}

fn compatible_nsql_model_names(app: &app::App) -> Vec<String> {
    app.models
        .iter()
        .filter(|model| model.model_type() == Some(ModelType::Llm))
        .map(|model| model.name.clone())
        .collect()
}

/// Construct a [`ResolvedTableAwareAllowlist`] based on the `App`'s `model.datasets`.
fn table_allowlist(
    model_name: &str,
    app: &app::App,
) -> Result<Option<ResolvedTableAwareAllowlist>, String> {
    // Create table allowlist from the model's datasets configuration
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
                return Err(format!(
                    "Unexpected internal error. Model '{model_name}' datasets are invalid."
                ));
            }
        }
    };
    Ok(table_allowlist)
}

#[cfg(test)]
mod tests {
    use super::*;
    use app::AppBuilder;
    use serde_json::json;
    use spicepod::component::{
        function::{
            Function, FunctionArg, FunctionKind, FunctionReturns, Signature,
            Volatility as FunctionVolatility,
        },
        model::Model,
        runtime::{Functions, Runtime as SpicepodRuntime},
    };
    use std::collections::{HashMap, HashSet};

    fn app_with_models(models: Vec<Model>) -> app::App {
        let mut builder = AppBuilder::new("test");
        for model in models {
            builder = builder.with_model(model);
        }
        builder.build()
    }

    fn app_with_functions(functions_enabled: bool, functions: Vec<Function>) -> app::App {
        let runtime = SpicepodRuntime {
            functions: if functions_enabled {
                Functions::enabled()
            } else {
                Functions::default()
            },
            ..Default::default()
        };

        let mut builder = AppBuilder::new("test").with_runtime(runtime);
        for function in functions {
            builder = builder.with_function(function);
        }
        builder.build()
    }

    fn test_function(name: &str, enabled: bool) -> Function {
        Function {
            name: name.to_string(),
            from: "sql".to_string(),
            enabled,
            description: Some(format!("{name} description")),
            kind: FunctionKind::Scalar,
            volatility: FunctionVolatility::Stable,
            signature: Signature {
                tables: vec![],
                args: vec![FunctionArg {
                    name: "x".to_string(),
                    arrow_type: "int64".to_string(),
                }],
                returns: Some(FunctionReturns::Scalar("int64".to_string())),
            },
            body: Some("x".to_string()),
            body_ref: None,
            metadata: HashMap::new(),
            params: HashMap::new(),
            depends_on: vec![],
            metrics: None,
            as_tool: false,
        }
    }

    fn test_table_function(name: &str, enabled: bool) -> Function {
        let mut function = test_function(name, enabled);
        function.kind = FunctionKind::Table;
        function.signature.returns = Some(FunctionReturns::Table(vec![FunctionArg {
            name: "value".to_string(),
            arrow_type: "int64".to_string(),
        }]));
        function
    }

    fn test_user_function_info(name: &str) -> UserFunctionInfo {
        UserFunctionInfo {
            name: name.to_string(),
            kind: "scalar".to_string(),
            volatility: "volatile".to_string(),
            from: "sql".to_string(),
            description: None,
        }
    }

    #[test]
    fn request_defaults_to_no_model_and_no_sample_data() {
        let request: Request = serde_json::from_value(json!({
            "query": "show total sales"
        }))
        .expect("request should deserialize with omitted optional fields");

        assert_eq!(request.model, None);
        assert!(!request.sample_data_enabled);
    }

    #[test]
    fn context_request_defaults_to_no_sampling_or_examples() {
        let request: ContextRequest = serde_json::from_value(json!({}))
            .expect("context request should deserialize with omitted optional fields");

        let options = request
            .context_options()
            .expect("default context options should be valid");

        assert!(!options.include_sampling);
        assert_eq!(options.sampling_limit, DEFAULT_NSQL_CONTEXT_SAMPLE_LIMIT);
        assert!(!options.include_examples);
        assert_eq!(options.examples_limit, DEFAULT_NSQL_CONTEXT_SAMPLE_LIMIT);
    }

    #[test]
    fn context_request_validates_enabled_limits() {
        let request: ContextRequest = serde_json::from_value(json!({
            "include_sampling": true,
            "sampling_limit": 0
        }))
        .expect("context request should deserialize");

        let (status, message) = request
            .context_options()
            .expect_err("enabled sampling should reject a zero limit");

        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(
            message,
            "Query parameter 'sampling_limit' must be greater than 0 when enabled"
        );
    }

    #[test]
    fn user_function_context_entries_include_registered_spicepod_signatures() {
        let app = app_with_functions(
            true,
            vec![
                test_function("Haversine_Km", true),
                test_table_function("Rows_Fn", true),
            ],
        );
        let available_names = HashSet::from(["haversine_km".to_string(), "rows_fn".to_string()]);
        let user_function_infos = vec![
            test_user_function_info("Rows_Fn"),
            test_user_function_info("Haversine_Km"),
        ];

        let entries = user_function_context_entries(&app, &available_names, user_function_infos);

        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].name, "Haversine_Km");
        assert_eq!(
            entries[0].syntax.as_deref(),
            Some("Haversine_Km(x int64) -> int64")
        );
        assert_eq!(entries[0].kind, "scalar");
        assert_eq!(entries[0].volatility, "stable");
        assert_eq!(
            entries[0].description.as_deref(),
            Some("Haversine_Km description")
        );
        assert_eq!(entries[1].name, "Rows_Fn");
        assert_eq!(
            entries[1].syntax.as_deref(),
            Some("Rows_Fn(x int64) -> TABLE(value int64)")
        );
        assert_eq!(entries[1].kind, "table");

        let mut output = String::new();
        write_user_function_context_entries(&mut output, &entries);
        assert!(output.contains("### User-defined functions"));
        assert!(output.contains("Syntax: `Haversine_Km(x int64) -> int64`."));
        assert!(output.contains("Syntax: `Rows_Fn(x int64) -> TABLE(value int64)`."));
    }

    #[test]
    fn user_function_context_entries_filter_to_enabled_registered_functions() {
        let app = app_with_functions(
            true,
            vec![
                test_function("Registered_Fn", true),
                test_function("Disabled_Fn", false),
            ],
        );
        let available_names = HashSet::from([
            "registered_fn".to_string(),
            "disabled_fn".to_string(),
            "missing_declaration_fn".to_string(),
        ]);
        let user_function_infos = vec![
            test_user_function_info("Registered_Fn"),
            test_user_function_info("Disabled_Fn"),
            test_user_function_info("Missing_Declaration_Fn"),
        ];

        let entries = user_function_context_entries(&app, &available_names, user_function_infos);

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "Registered_Fn");

        let disabled_app = app_with_functions(true, vec![test_function("Registered_Fn", true)]);
        let mut disabled_app = disabled_app;
        disabled_app.runtime.functions = Functions::default();
        let entries = user_function_context_entries(
            &disabled_app,
            &available_names,
            vec![test_user_function_info("Registered_Fn")],
        );

        assert!(entries.is_empty());
    }

    #[test]
    fn omitted_model_uses_single_compatible_model() {
        let app = app_with_models(vec![Model::new("openai:gpt-4o-mini", "llm_model")]);

        let model_name = resolve_nsql_model_name_from_app(&app)
            .expect("single compatible model should be selected");

        assert_eq!(model_name, "llm_model");
    }

    #[test]
    fn omitted_model_ignores_non_llm_models() {
        let app = app_with_models(vec![
            Model::new("spiceai:my-org/my-app/models/runnable", "ml_model"),
            Model::new("openai:gpt-4o-mini", "llm_model"),
        ]);

        let model_name = resolve_nsql_model_name_from_app(&app)
            .expect("single compatible model should be selected");

        assert_eq!(model_name, "llm_model");
    }

    #[test]
    fn omitted_model_errors_when_no_compatible_model_exists() {
        let app = app_with_models(vec![]);

        let error = resolve_nsql_model_name_from_app(&app)
            .expect_err("omitted model should fail without compatible models");

        assert_eq!(
            error,
            "No model specified and no compatible LLM model is configured. Add exactly one LLM model to the Spicepod or include the 'model' field in the request."
        );
    }

    #[test]
    fn omitted_model_errors_when_multiple_compatible_models_exist() {
        let app = app_with_models(vec![
            Model::new("openai:gpt-4o-mini", "first_model"),
            Model::new("openai:gpt-4o", "second_model"),
        ]);

        let error = resolve_nsql_model_name_from_app(&app)
            .expect_err("omitted model should fail with multiple compatible models");

        assert_eq!(
            error,
            "No model specified and multiple compatible LLM models are configured (first_model, second_model). Include the 'model' field in the request."
        );
    }
}
