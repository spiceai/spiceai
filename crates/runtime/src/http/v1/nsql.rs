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
    },
    http::v1::{ResponseMetadata, ResponseMimeType, to_http_response},
    model::LLMChatCompletionsModelStore,
    tools::utils::create_tool_use_messages,
};
use runtime_tools::builtin::table_schema::{TableSchemaTool, TableSchemaToolParams};
use async_openai::types::chat::ChatCompletionRequestMessage;
use axum::{
    Extension, Json,
    http::StatusCode,
    response::{
        IntoResponse, Response, Sse,
        sse::{Event, KeepAlive},
    },
};
use axum_extra::TypedHeader;
use datafusion::sql::TableReference;
use futures::{StreamExt, TryStreamExt};
use headers_accept::Accept;
use http::HeaderMap;
use runtime_query_engine::allowlist::ResolvedTableAwareAllowlist;
use runtime_request_context::{AsyncMarker, RequestContext};

use arrow::array::RecordBatch;
use itertools::Itertools;
use llms::chat::nsql::{FailedAttempt, QueryGenerationContext, default::DefaultSqlGeneration};
use runtime_tools::builtin::sample::{
    SampleTableMethod, SampleTableParams, distinct::DistinctColumnsParams,
    random::RandomSampleParams, tool::SampleDataTool,
};
use serde::{Deserialize, Serialize};
use spicepod::component::model::ModelType;
use std::{sync::Arc, time::Duration};
use tokio::sync::RwLock;
use tracing::Span;
use tracing_futures::Instrument;

use super::accept_header_types;
use crate::datafusion::query::QueryBuilder;

// Default number of retries for NSQL queries if the generated query fails to execute
const DEFAULT_NSQL_RETRIES: u8 = 10;

// Maximum number of concurrent sampling tools executions for NSQL
const DATA_SAMPLING_MAX_CONCURRENT: usize = 10;

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

/// Create subsequent Assistant and Tool messages simulating a model requesting to use the `sample_data` tool, then receiving the result for the following sampling methods:
///  - Distinct columns
///  - Random sample
///
/// Convert the [`SampleTableParams`] into how an LLM would ask to use it (via a [`ChatCompletionRequestAssistantMessage`]).
/// Convert the result of a [`SampleDataTool`] call how we would return it to the LLM, (via a [`ChatCompletionRequestToolMessage`]).
async fn sample_messages(
    sample_from: &[TableReference],
    rt: Arc<Runtime>,
    table_allowlist: Option<ResolvedTableAwareAllowlist>,
) -> Result<Vec<ChatCompletionRequestMessage>, Box<dyn std::error::Error + Send + Sync>> {
    let message_futures = sample_from.iter().flat_map(|dataset| {
        [
            SampleTableParams::DistinctColumns(DistinctColumnsParams {
                tbl: dataset.to_string(),
                limit: 3,
                cols: None,
            }),
            SampleTableParams::RandomSample(RandomSampleParams {
                tbl: dataset.to_string(),
                limit: 3,
            }),
        ]
        .into_iter()
        .map(|params| {
            let rt = Arc::clone(&rt);
            let allowlist = table_allowlist.clone();
            async move {
                let method = SampleTableMethod::from(&params);
                create_tool_use_messages(
                    &SampleDataTool::new(rt.datafusion(), method.clone())
                        .with_table_allowlist(allowlist),
                    format!("sample-{method:?}").as_str(),
                    &params,
                )
                .instrument(Span::current())
                .await
            }
        })
    });

    let tool_call_messages = futures::stream::iter(message_futures)
        .boxed()
        .buffer_unordered(DATA_SAMPLING_MAX_CONCURRENT)
        .try_collect::<Vec<_>>()
        .await?;

    Ok(tool_call_messages.into_iter().flatten().collect())
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

fn default_sample_data_enabled() -> bool {
    false
}

/// Checks if the request is asking to only generate SQL.
fn return_sql_only(accept: Option<&TypedHeader<Accept>>) -> bool {
    accept.is_some_and(|a| accept_header_types(a).contains(&"application/sql".to_string()))
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

    let table_allowlist_opt = match table_allowlist(&model, &rt).await {
        Ok(ta) => ta,
        Err(e) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, headers, e);
        }
    };

    // Validate that requested datasets are within the model's allowlist
    if let (Some(requested_datasets), Some(allowlist)) = (&datasets, &table_allowlist_opt) {
        for ds in requested_datasets {
            let table_ref = TableReference::parse_str(ds);
            if !allowlist.table_is_allowed(&table_ref) {
                return (
                    StatusCode::BAD_REQUEST,
                    headers,
                    format!("Dataset '{ds}' not found"),
                );
            }
        }
    }

    crate::model::add_tools_used(&context, 1);

    let span = tracing::span!(target: "task_history", tracing::Level::INFO, "nsql", input = %query, model = %model, "labels");

    if let Some(traceparent) = context.trace_parent() {
        crate::http::traceparent::override_task_history_with_trace_parent(&span, traceparent);
    }

    // Default to all available tables if specific table(s) are not provided.
    let tables = datasets
        .map(|ds| ds.iter().map(TableReference::from).collect_vec())
        .unwrap_or(
            df.get_user_table_names()
                .into_iter()
                .filter(|t| {
                    table_allowlist_opt
                        .as_ref()
                        .is_none_or(|a| a.table_is_allowed(t))
                })
                .collect(),
        );

    // Create assistant/tool result messages for calling `table_schema` tool for all or provided tables.
    let schema_messages = match create_tool_use_messages(
        &TableSchemaTool::new(
            rt.datafusion() as Arc<dyn runtime_query_engine::query_engine::QueryEngine>,
            Arc::clone(&rt.app),
            None,
            None,
        )
        .with_table_allowlist(table_allowlist_opt.clone()),
        "schemas-nsql",
        &TableSchemaToolParams::new(tables.iter().map(ToString::to_string).collect::<Vec<_>>()),
    )
    .instrument(span.clone())
    .await
    {
        Ok(m) => m,
        Err(e) => {
            tracing::error!("Error getting schema messages: {e}");
            return (StatusCode::INTERNAL_SERVER_ERROR, headers, e.to_string());
        }
    };

    // Create sample data assistant/tool messages if user wants to sample from dataset(s).
    let sample_data_messages = if sample_data_enabled {
        match sample_messages(&tables, Arc::clone(&rt), table_allowlist_opt.clone())
            .instrument(span.clone())
            .await
        {
            Ok(m) => m,
            Err(e) => {
                tracing::error!("Error sampling datasets for NSQL messages: {e}");
                return (StatusCode::INTERNAL_SERVER_ERROR, headers, e.to_string());
            }
        }
    } else {
        vec![]
    };

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

        req.messages.extend(schema_messages.clone());
        req.messages.extend(sample_data_messages.clone());
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
async fn table_allowlist(
    model_name: &str,
    rt: &Arc<Runtime>,
) -> Result<Option<ResolvedTableAwareAllowlist>, String> {
    let Some(app) = rt.read_app().await else {
        return Err("Unexpected internal error. App not prepared in runtime.".to_string());
    };

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
    use spicepod::component::model::Model;

    fn app_with_models(models: Vec<Model>) -> app::App {
        let mut builder = AppBuilder::new("test");
        for model in models {
            builder = builder.with_model(model);
        }
        builder.build()
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
