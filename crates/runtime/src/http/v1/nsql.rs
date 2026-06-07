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
    datafusion::request_context_extension::get_current_datafusion,
    http::v1::{ResponseMetadata, ResponseMimeType, to_http_response},
    model::{
        LLMChatCompletionsModelStore,
        nsql::{
            DEFAULT_NSQL_CONTEXT_SAMPLE_LIMIT, MAX_NSQL_CONTEXT_SAMPLE_LIMIT, NsqlContextError,
            NsqlContextJsonResponse, NsqlContextOptions, build_nsql_context,
        },
    },
};
use axum::{
    Extension, Json,
    http::StatusCode,
    response::{
        IntoResponse, Response, Sse,
        sse::{Event, KeepAlive},
    },
};
use axum_extra::{TypedHeader, extract::Query};
use futures::{StreamExt, TryStreamExt};
use headers_accept::Accept;
use http::{HeaderMap, HeaderValue, header::CONTENT_TYPE};
use mediatype::{MediaType, names};
use runtime_request_context::{AsyncMarker, RequestContext};

use arrow::array::RecordBatch;
use llms::chat::nsql::{FailedAttempt, QueryGenerationContext, default::DefaultSqlGeneration};
use serde::{Deserialize, Serialize};
use spicepod::component::model::ModelType;
use std::{sync::Arc, time::Duration};
use tokio::sync::RwLock;
use tracing_futures::Instrument;

use super::accept_header_types;
use crate::datafusion::query::QueryBuilder;

// Default number of retries for NSQL queries if the generated query fails to execute
const DEFAULT_NSQL_RETRIES: u8 = 10;

// NSQL streaming keep alive interval in seconds
const NSQL_STREAM_KEEP_ALIVE: u64 = 30;

static NSQL_CONTEXT_RESPONSE_MEDIA_TYPES: [MediaType<'static>; 3] = [
    MediaType::new(names::TEXT, names::MARKDOWN),
    MediaType::new(names::TEXT, names::PLAIN),
    MediaType::new(names::APPLICATION, names::JSON),
];

enum NsqlContextResponseFormat {
    Markdown,
    PlainText,
    Json,
}
fn clean_model_based_sql(input: &str) -> String {
    let no_dashes = match input.strip_prefix("--") {
        Some(rest) => rest.to_string(),
        None => input.to_string(),
    };

    // Only take the first query, if there are multiple.
    let one_query = no_dashes.split(';').next().unwrap_or(&no_dashes);
    one_query.trim().to_string()
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

#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "snake_case")]
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
    #[serde(default = "default_sample_data_enabled", alias = "sampledataenabled")]
    pub sample_data_enabled: bool,

    /// Names of datasets to sample from when constructing model context; this is a sampling hint and does not restrict which tables queries can target. If omitted, all datasets are used.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub datasets: Option<Vec<String>>,

    /// Stable prompt-cache key forwarded to the configured NSQL model for provider-specific cache handling.
    #[serde(skip_serializing_if = "Option::is_none", alias = "promptcachekey")]
    pub prompt_cache_key: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::IntoParams, utoipa::ToSchema))]
#[cfg_attr(feature = "openapi", into_params(parameter_in = Query))]
#[serde(rename_all = "snake_case")]
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

    /// Whether example rows are included in the context block. Defaults to `include_sampling` when omitted, matching `/v1/nsql` `sample_data_enabled` behavior.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub include_examples: Option<bool>,

    /// Maximum number of example rows per dataset. Default: 3, maximum: 100.
    #[serde(default = "default_nsql_context_sample_limit")]
    pub examples_limit: usize,

    /// Names of datasets to include in the context block. If omitted, all datasets visible to the selected model are included.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub datasets: Vec<String>,
}

impl ContextRequest {
    fn context_options(&self) -> Result<NsqlContextOptions, (StatusCode, String)> {
        let include_examples = self.include_examples.unwrap_or(self.include_sampling);
        validate_context_limit("sampling_limit", self.include_sampling, self.sampling_limit)?;
        validate_context_limit("examples_limit", include_examples, self.examples_limit)?;

        Ok(NsqlContextOptions::new(
            self.include_sampling,
            self.sampling_limit,
            include_examples,
            self.examples_limit,
        ))
    }

    fn datasets(&self) -> Option<Vec<String>> {
        if self.datasets.is_empty() {
            None
        } else {
            Some(self.datasets.clone())
        }
    }
}

fn default_sample_data_enabled() -> bool {
    false
}

/// Checks if the request is asking to only generate SQL.
fn return_sql_only(accept: Option<&TypedHeader<Accept>>) -> bool {
    accept.is_some_and(|a| accept_header_types(a).contains(&"application/sql".to_string()))
}

fn context_response_format(
    accept: Option<&TypedHeader<Accept>>,
) -> Option<NsqlContextResponseFormat> {
    let content_type = accept.map_or(Some(&NSQL_CONTEXT_RESPONSE_MEDIA_TYPES[0]), |header| {
        header.0.negotiate(&NSQL_CONTEXT_RESPONSE_MEDIA_TYPES)
    })?;

    if content_type.subty == names::PLAIN {
        Some(NsqlContextResponseFormat::PlainText)
    } else if content_type.subty == names::JSON {
        Some(NsqlContextResponseFormat::Json)
    } else {
        Some(NsqlContextResponseFormat::Markdown)
    }
}

fn nsql_context_response(
    context: NsqlContextJsonResponse,
    accept: Option<&TypedHeader<Accept>>,
) -> Response {
    let Some(response_format) = context_response_format(accept) else {
        return (
            StatusCode::NOT_ACCEPTABLE,
            "Supported response content types are text/markdown, text/plain, and application/json",
        )
            .into_response();
    };

    match response_format {
        NsqlContextResponseFormat::Json => Json(context).into_response(),
        NsqlContextResponseFormat::Markdown => {
            let mut headers = HeaderMap::new();
            headers.insert(
                CONTENT_TYPE,
                HeaderValue::from_static("text/markdown; charset=utf-8"),
            );
            (StatusCode::OK, headers, context.context).into_response()
        }
        NsqlContextResponseFormat::PlainText => {
            let mut headers = HeaderMap::new();
            headers.insert(
                CONTENT_TYPE,
                HeaderValue::from_static("text/plain; charset=utf-8"),
            );
            (StatusCode::OK, headers, context.context).into_response()
        }
    }
}

fn nsql_context_error_response(error: &NsqlContextError) -> (StatusCode, String) {
    let status = match error {
        NsqlContextError::DatasetNotFound { .. } => StatusCode::BAD_REQUEST,
        NsqlContextError::AppNotPrepared
        | NsqlContextError::InvalidModelDatasets { .. }
        | NsqlContextError::ContextMessage { .. }
        | NsqlContextError::SchemaContext { .. }
        | NsqlContextError::FunctionContext { .. }
        | NsqlContextError::SampleContext { .. } => StatusCode::INTERNAL_SERVER_ERROR,
    };

    (status, error.to_string())
}

/// Text-to-SQL Context (NSQL)
///
/// Return the same context block that `/v1/nsql` injects into the configured NSQL model.
///
/// The response is a markdown/plain-text block or JSON object containing the in-scope datasets, schemas,
/// Spice-specific SQL functions, registered user-defined functions, relevant JSON/Spark compatibility functions,
/// and optional sample data.
#[cfg_attr(feature = "openapi", utoipa::path(
    get,
    path = "/v1/nsql/context",
    operation_id = "get_nsql_context",
    tag = "SQL",
    params(
        ("Accept" = String, Header, description = "The format of the response, one of 'text/markdown' (default), 'text/plain', or 'application/json'."),
        ContextRequest
    ),
    responses(
        (status = 200, description = "NSQL context block", content((
            String = "text/markdown",
            example = "# Spice.ai NSQL Context\n\n## Datasets\n- `sales_data`\n\n## Schemas\n| table | column | type | nullable |\n| --- | --- | --- | --- |\n| sales_data | customer_id | Utf8 | false |\n\n## SQL Functions\nUse Spice.ai/DataFusion SQL."
        ), (
            String = "text/plain",
            example = "# Spice.ai NSQL Context\n\n## Datasets\n- `sales_data`"
        ), (
            NsqlContextJsonResponse = "application/json",
            example = json!({
                "context": "# Spice.ai NSQL Context\n- Write SQL for the Spice runtime...",
                "instructions": ["Write SQL for the Spice runtime, which uses Apache DataFusion with the SQL parser configured for the PostgreSQL dialect."],
                "sql": {
                    "engine": "Apache DataFusion",
                    "version": "52.5.0",
                    "dialect": "PostgreSQL",
                    "parser": "DataFusion SQL parser configured with PostgreSQL dialect",
                    "notes": ["Spice supports standard DataFusion SQL plus additional Spice-specific and registered user-defined functions listed in this context."]
                },
                "datasets": [{
                    "name": "sales.orders",
                    "schema": "sales",
                    "table": "orders",
                    "description": "Customer orders",
                    "metadata": {"description": "Customer orders"},
                    "columns": [{
                        "name": "customer_id",
                        "data_type": "Utf8",
                        "nullable": false,
                        "description": "Customer account identifier",
                        "metadata": {"description": "Customer account identifier"},
                        "primary_key": false,
                        "unique": false,
                        "indexed": true,
                        "vector_search": true,
                        "full_text_search": true
                    }],
                    "primary_key": ["order_id"],
                    "unique_constraints": [],
                    "foreign_keys": [{
                        "columns": ["customer_id"],
                        "foreign_table": "spice.sales.customers",
                        "foreign_columns": ["id"]
                    }],
                    "indexes": [{
                        "name": "customer_id",
                        "columns": ["customer_id"],
                        "kind": "enabled",
                        "source": "spicepod.acceleration.indexes"
                    }],
                    "search": {
                        "vector": [{
                            "column": "customer_id",
                            "function": "vector_search",
                            "syntax": "vector_search(sales.orders, 'query text', customer_id)",
                            "model": "embed_model",
                            "engine": "duckdb_vector_index",
                            "row_id_columns": ["order_id"],
                            "vector_size": 384,
                            "chunked": false,
                            "index": {"name": "duckdb_vector_index", "columns": ["customer_id", "order_id"], "kind": "duckdb_vector_index", "source": "runtime_index"},
                            "required_columns": ["customer_id", "order_id"],
                            "notes": []
                        }],
                        "full_text": [{
                            "column": "customer_id",
                            "function": "text_search",
                            "syntax": "text_search(sales.orders, 'query text', customer_id)",
                            "engine": "tantivy",
                            "index_store": "memory",
                            "row_id_columns": ["order_id"],
                            "index": {"name": "full_text", "columns": ["customer_id", "order_id"], "kind": "full_text", "source": "runtime_index"},
                            "required_columns": ["customer_id", "order_id"],
                            "notes": []
                        }]
                    }
                }],
                "functions": {
                    "summary": "Spice SQL runs on Apache DataFusion with the SQL parser configured for the PostgreSQL dialect.",
                    "json": [],
                    "search": [],
                    "additional": [],
                    "spark_compatibility": {"description": "Spark-compatible scalar functions are available.", "functions": []},
                    "user_defined": []
                },
                "samples": []
            })
        ))),
        (status = 400, description = "Invalid request parameters", content((
            String = "text/plain", example = "Dataset 'sales.orders' not found"
        ))),
        (status = 406, description = "Requested response content type is not available", content((
            String = "text/plain", example = "Supported response content types are text/markdown, text/plain, and application/json"
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

    let model = match resolve_nsql_model_name(params.model.clone(), &rt).await {
        Ok(model) => model,
        Err((status, message)) => return (status, message).into_response(),
    };

    let context = match build_nsql_context(
        rt,
        &request_context,
        &model,
        context_options,
        params.datasets(),
    )
    .await
    {
        Ok(context) => context,
        Err(error) => {
            let (status, message) = nsql_context_error_response(&error);
            return (status, message).into_response();
        }
    };

    nsql_context_response(context.json, accept.as_ref())
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
        Err(error) => {
            let (status, message) = nsql_context_error_response(&error);
            return (status, headers, message);
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        datafusion::udf::UserFunctionInfo,
        model::nsql::{
            NsqlColumnContext, NsqlDatasetContext, NsqlDatasetSearchContext, NsqlForeignKeyContext,
            NsqlFullTextSearchContext, NsqlIndexContext, NsqlVectorSearchContext,
            SampleContextBlock, all_context_function_names_for_test,
            nsql_function_context_for_test, render_nsql_context_block,
            user_function_context_entries, write_user_function_context_entries,
        },
    };
    use app::AppBuilder;
    use axum_extra::extract::Query as ExtraQuery;
    use datafusion::sql::TableReference;
    use http::Uri;
    use http_body_util::BodyExt;
    use serde_json::json;
    use spicepod::component::{
        function::{
            Function, FunctionArg, FunctionKind, FunctionReturns, Signature,
            Volatility as FunctionVolatility,
        },
        model::Model,
        runtime::{Functions, Runtime as SpicepodRuntime},
    };
    use std::{
        collections::{BTreeMap, HashMap, HashSet},
        str::FromStr,
    };

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

    fn ticket_priority_function() -> Function {
        let mut function = test_function("ticket_priority", true);
        function.description =
            Some("Converts support-ticket sentiment into a priority score.".to_string());
        function.signature.args = vec![FunctionArg {
            name: "sentiment".to_string(),
            arrow_type: "utf8".to_string(),
        }];
        function.signature.returns = Some(FunctionReturns::Scalar("int64".to_string()));
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

    fn accept_header(value: &str) -> TypedHeader<Accept> {
        TypedHeader(Accept::from_str(value).expect("accept header should parse"))
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
    fn request_accepts_legacy_lowercase_field_aliases() {
        let request: Request = serde_json::from_value(json!({
            "query": "show total sales",
            "sampledataenabled": true,
            "promptcachekey": "sales-dashboard"
        }))
        .expect("request should deserialize legacy lowercase aliases");

        assert!(request.sample_data_enabled);
        assert_eq!(request.prompt_cache_key.as_deref(), Some("sales-dashboard"));
    }

    #[test]
    fn nsql_context_response_defaults_to_markdown() {
        let response =
            nsql_context_response(NsqlContextJsonResponse::minimal_for_test("context"), None);

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get(CONTENT_TYPE)
                .expect("content type should be set")
                .to_str()
                .expect("content type should be valid ASCII"),
            "text/markdown; charset=utf-8"
        );
    }

    #[tokio::test]
    async fn nsql_context_response_negotiates_plain_text() {
        let accept = accept_header("text/plain");
        let response = nsql_context_response(
            NsqlContextJsonResponse::minimal_for_test(nsql_context_block_with_samples_for_test()),
            Some(&accept),
        );

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get(CONTENT_TYPE)
                .expect("content type should be set")
                .to_str()
                .expect("content type should be valid ASCII"),
            "text/plain; charset=utf-8"
        );

        let body = response
            .into_body()
            .collect()
            .await
            .expect("response body should collect")
            .to_bytes();
        let body = String::from_utf8(body.to_vec()).expect("response body should be UTF-8");

        assert!(body.contains("## Datasets"));
        assert!(body.contains("`sales.orders`: Customer orders"));
        assert!(body.contains("## SQL Functions"));
        assert!(body.contains("## Sample Data"));

        insta::assert_snapshot!("nsql_context_plain_text_response", body);
    }

    #[tokio::test]
    async fn nsql_context_response_negotiates_json() {
        let accept = accept_header("application/json");
        let response = nsql_context_response(
            NsqlContextJsonResponse::minimal_for_test("context"),
            Some(&accept),
        );

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get(CONTENT_TYPE)
                .expect("content type should be set")
                .to_str()
                .expect("content type should be valid ASCII"),
            "application/json"
        );

        let body = response
            .into_body()
            .collect()
            .await
            .expect("response body should collect")
            .to_bytes();
        let body: serde_json::Value =
            serde_json::from_slice(&body).expect("response body should be JSON");

        assert_eq!(body["context"], json!("context"));
        assert_eq!(body["sql"]["engine"], json!("Apache DataFusion"));
        assert_eq!(body["sql"]["dialect"], json!("PostgreSQL"));
        assert!(
            body["datasets"]
                .as_array()
                .expect("datasets should be an array")
                .is_empty()
        );
    }

    #[test]
    fn nsql_context_response_rejects_unsupported_accept() {
        let accept = accept_header("application/xml");
        let response = nsql_context_response(
            NsqlContextJsonResponse::minimal_for_test("context"),
            Some(&accept),
        );

        assert_eq!(response.status(), StatusCode::NOT_ACCEPTABLE);
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
        assert!(request.datasets.is_empty());
    }

    #[test]
    fn context_request_sample_data_enabled_defaults_examples_to_sampling() {
        let request: ContextRequest = serde_json::from_value(json!({
            "sample_data_enabled": true
        }))
        .expect("context request should accept sample_data_enabled alias");

        let options = request
            .context_options()
            .expect("sample_data_enabled context options should be valid");

        assert!(options.include_sampling);
        assert!(options.include_examples);

        let request: ContextRequest = serde_json::from_value(json!({
            "sample_data_enabled": true,
            "include_examples": false
        }))
        .expect("context request should allow include_examples override");

        let options = request
            .context_options()
            .expect("explicit include_examples override should be valid");

        assert!(options.include_sampling);
        assert!(!options.include_examples);
    }

    #[test]
    fn context_request_parses_repeated_dataset_query_params() {
        let uri: Uri = "/v1/nsql/context?datasets=sales.orders&datasets=support_tickets"
            .parse()
            .expect("query URI should parse");

        let ExtraQuery(request) = ExtraQuery::<ContextRequest>::try_from_uri(&uri)
            .expect("context request should parse repeated dataset keys");

        assert_eq!(request.datasets, vec!["sales.orders", "support_tickets"]);
        assert_eq!(
            request.datasets(),
            Some(vec![
                "sales.orders".to_string(),
                "support_tickets".to_string()
            ])
        );
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

    fn nsql_context_function_context_for_test(include_search_functions: bool) -> String {
        let app = app_with_functions(true, vec![ticket_priority_function()]);
        let mut available_names = snapshot_context_function_names_for_test();
        available_names.insert("ticket_priority".to_string());

        nsql_function_context_for_test(
            &app,
            &available_names,
            include_search_functions,
            include_search_functions,
        )
        .render_markdown()
    }

    fn snapshot_context_function_names_for_test() -> HashSet<String> {
        let mut names = all_context_function_names_for_test();
        names.remove("ai");
        names.remove("embed");
        names
    }

    fn nsql_context_block_with_samples_for_test() -> String {
        let tables = vec![
            TableReference::parse_str("sales.orders"),
            TableReference::parse_str("support_tickets"),
        ];
        let dataset_contexts = vec![
            NsqlDatasetContext {
                name: "sales.orders".to_string(),
                catalog: None,
                schema: Some("sales".to_string()),
                table: "orders".to_string(),
                description: Some("Customer orders".to_string()),
                metadata: BTreeMap::new(),
                columns: vec![],
                primary_key: vec![],
                unique_constraints: vec![],
                foreign_keys: vec![],
                indexes: vec![],
                search: NsqlDatasetSearchContext::default(),
            },
            NsqlDatasetContext {
                name: "support_tickets".to_string(),
                catalog: None,
                schema: None,
                table: "support_tickets".to_string(),
                description: Some("Customer support tickets".to_string()),
                metadata: BTreeMap::new(),
                columns: vec![],
                primary_key: vec![],
                unique_constraints: vec![],
                foreign_keys: vec![],
                indexes: vec![],
                search: NsqlDatasetSearchContext::default(),
            },
        ];
        let schema_context = r"
| table_reference | column_name | data_type | nullable | description |
| --- | --- | --- | --- | --- |
| sales.orders | order_id | Int64 | false | Unique order identifier |
| sales.orders | customer_id | Utf8 | false | Customer account identifier |
| support_tickets | ticket_id | Int64 | false | Support ticket identifier |
| support_tickets | sentiment | Utf8 | true | Model-derived sentiment label |
    ";
        let relationship_context = r"
### `sales.orders`
- Primary key: `order_id`
- Foreign keys:
    - (`customer_id`) references `spice.sales.customers` (`id`)
- Indexes:
    - `customer_id`: `customer_id` (enabled, spicepod.acceleration.indexes)
- Search indexes:
    - vector search on `customer_id` using `embed_model`. Syntax: `vector_search(sales.orders, 'query text', customer_id)`. Engine/index: `duckdb_vector_index`. Row id: `order_id`.
    - full-text search on `customer_id` using `tantivy`. Syntax: `text_search(sales.orders, 'query text', customer_id)`. Row id: `order_id`.
        ";
        let sample_context_blocks = vec![
            SampleContextBlock {
                title: "Distinct value samples for `support_tickets`".to_string(),
                content: "| sentiment |\n| --- |\n| positive |\n| negative |".to_string(),
            },
            SampleContextBlock {
                title: "Example rows for `sales.orders`".to_string(),
                content: "| order_id | customer_id |\n| --- | --- |\n| 42 | CUST-1 |".to_string(),
            },
        ];

        render_nsql_context_block(
            &tables,
            &dataset_contexts,
            schema_context,
            relationship_context,
            &nsql_context_function_context_for_test(true),
            &sample_context_blocks,
        )
    }

    fn nsql_context_block_no_datasets_for_test() -> String {
        render_nsql_context_block(
            &[],
            &[],
            "",
            "",
            &nsql_context_function_context_for_test(false),
            &[],
        )
    }

    #[test]
    fn nsql_context_block_snapshot() {
        insta::assert_snapshot!(
            "nsql_context_block_with_samples",
            nsql_context_block_with_samples_for_test()
        );
    }

    #[test]
    fn nsql_context_block_no_datasets_snapshot() {
        let context = nsql_context_block_no_datasets_for_test();

        assert!(context.contains("No datasets are currently in scope."));
        assert!(context.contains("## SQL Functions"));
        assert!(context.contains("Apache DataFusion version"));

        insta::assert_snapshot!("nsql_context_block_no_datasets", context);
    }

    #[test]
    fn nsql_context_json_response_snapshot() {
        let mut response = NsqlContextJsonResponse::minimal_for_test("# Spice.ai NSQL Context");
        response.datasets = vec![NsqlDatasetContext {
            name: "sales.orders".to_string(),
            catalog: Some("spice".to_string()),
            schema: Some("sales".to_string()),
            table: "orders".to_string(),
            description: Some("Customer orders".to_string()),
            metadata: BTreeMap::from([("description".to_string(), "Customer orders".to_string())]),
            columns: vec![
                NsqlColumnContext {
                    name: "order_id".to_string(),
                    data_type: "Int64".to_string(),
                    nullable: false,
                    description: Some("Unique order identifier".to_string()),
                    source_type: Some("bigint".to_string()),
                    metadata: BTreeMap::from([
                        (
                            "description".to_string(),
                            "Unique order identifier".to_string(),
                        ),
                        ("source_type".to_string(), "bigint".to_string()),
                    ]),
                    primary_key: true.into(),
                    unique: false.into(),
                    indexed: true.into(),
                    vector_search: false.into(),
                    full_text_search: false.into(),
                },
                NsqlColumnContext {
                    name: "customer_id".to_string(),
                    data_type: "Utf8".to_string(),
                    nullable: false,
                    description: Some("Customer account identifier".to_string()),
                    source_type: Some("text".to_string()),
                    metadata: BTreeMap::from([
                        (
                            "description".to_string(),
                            "Customer account identifier".to_string(),
                        ),
                        ("source_type".to_string(), "text".to_string()),
                    ]),
                    primary_key: false.into(),
                    unique: false.into(),
                    indexed: true.into(),
                    vector_search: true.into(),
                    full_text_search: true.into(),
                },
            ],
            primary_key: vec!["order_id".to_string()],
            unique_constraints: vec![vec!["order_id".to_string()]],
            foreign_keys: vec![NsqlForeignKeyContext {
                columns: vec!["customer_id".to_string()],
                foreign_table: "spice.sales.customers".to_string(),
                foreign_columns: vec!["id".to_string()],
            }],
            indexes: vec![NsqlIndexContext {
                name: Some("customer_id".to_string()),
                columns: vec!["customer_id".to_string()],
                kind: "enabled".to_string(),
                source: "spicepod.acceleration.indexes".to_string(),
            }],
            search: NsqlDatasetSearchContext {
                vector: vec![NsqlVectorSearchContext {
                    column: "customer_id".to_string(),
                    function: "vector_search".to_string(),
                    syntax: "vector_search(sales.orders, 'query text', customer_id)".to_string(),
                    model: "embed_model".to_string(),
                    engine: Some("duckdb_vector_index".to_string()),
                    row_id_columns: vec!["order_id".to_string()],
                    vector_size: Some(384),
                    chunked: false,
                    input_mode: Some("scalar".to_string()),
                    required_columns: vec!["customer_id".to_string(), "order_id".to_string()],
                    index: Some(NsqlIndexContext {
                        name: Some("duckdb_vector_index".to_string()),
                        columns: vec!["customer_id".to_string(), "order_id".to_string()],
                        kind: "duckdb_vector_index".to_string(),
                        source: "runtime_index".to_string(),
                    }),
                    notes: vec![],
                }],
                full_text: vec![NsqlFullTextSearchContext {
                    column: "customer_id".to_string(),
                    function: "text_search".to_string(),
                    syntax: "text_search(sales.orders, 'query text', customer_id)".to_string(),
                    engine: "tantivy".to_string(),
                    index_store: "memory".to_string(),
                    row_id_columns: vec!["order_id".to_string()],
                    required_columns: vec!["customer_id".to_string(), "order_id".to_string()],
                    index: Some(NsqlIndexContext {
                        name: Some("full_text".to_string()),
                        columns: vec!["customer_id".to_string(), "order_id".to_string()],
                        kind: "full_text".to_string(),
                        source: "runtime_index".to_string(),
                    }),
                    notes: vec![],
                }],
            },
        }];
        let app = AppBuilder::new("test").build();
        response.functions = nsql_function_context_for_test(
            &app,
            &snapshot_context_function_names_for_test(),
            true,
            true,
        );
        response.samples = vec![SampleContextBlock {
            title: "Example rows for `sales.orders`".to_string(),
            content: "| order_id | customer_id |\n| --- | --- |\n| 42 | CUST-1 |".to_string(),
        }];

        let response = serde_json::to_string_pretty(&response)
            .expect("structured NSQL context should serialize");

        insta::assert_snapshot!("nsql_context_json_response", response);
    }

    #[test]
    fn nsql_user_function_context_snapshot() {
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
        let mut output = String::new();

        write_user_function_context_entries(&mut output, &entries);

        insta::assert_snapshot!("nsql_user_function_context", output);
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
