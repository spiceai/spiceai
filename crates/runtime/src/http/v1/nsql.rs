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
    http::v1::{ResponseMetadata, ResponseMimeType, run_sql, to_http_response},
    model::LLMChatCompletionsModelStore,
    tools::{
        builtin::{
            sample::{
                SampleTableMethod, SampleTableParams, distinct::DistinctColumnsParams,
                random::RandomSampleParams, tool::SampleDataTool,
            },
            table_schema::{TableSchemaTool, TableSchemaToolParams},
        },
        utils::create_tool_use_messages,
    },
};
use async_openai::types::ChatCompletionRequestMessage;
use axum::{
    Extension, Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use axum_extra::TypedHeader;
use datafusion::sql::TableReference;
use futures::{StreamExt, TryStreamExt};
use headers_accept::Accept;
use runtime_request_context::{AsyncMarker, RequestContext};

use itertools::Itertools;
use llms::chat::nsql::{FailedAttempt, QueryGenerationContext, default::DefaultSqlGeneration};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::Span;
use tracing_futures::Instrument;

use super::accept_header_types;

// Default number of retries for NSQL queries if the generated query fails to execute
const DEFAULT_NSQL_RETRIES: u8 = 10;

// Maximum number of concurrent sampling tools executions for NSQL
const DATA_SAMPLING_MAX_CONCURRENT: usize = 10;

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
            async move {
                let method = SampleTableMethod::from(&params);
                create_tool_use_messages(
                    &SampleDataTool::new(rt.datafusion(), method.clone()),
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

    /// The name of the model to use for SQL generation. Default: "nql"
    #[serde(default = "default_model")]
    pub model: String,

    /// Whether sample data is included in the context for SQL generation. Default: true
    #[serde(default = "default_sample_data_enabled")]
    pub sample_data_enabled: bool,

    /// Names of datasets to sample from. If omitted, all datasets are used.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub datasets: Option<Vec<String>>,
}

fn default_sample_data_enabled() -> bool {
    true
}

fn default_model() -> String {
    "nql".to_string()
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
                "model": "nql",
                "sample_data_enabled": true,
                "datasets": ["sales_data"]
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
        ))),
        (status = 400, description = "Invalid request parameters", content((
            String = "application/json", example = "Model nsql not found"
        ))),
        (status = 500, description = "Internal server error", content((
            String, example = "No query produced from NSQL model"
        )))
    )
))]
#[allow(clippy::too_many_lines)]
pub(crate) async fn post(
    Extension(rt): Extension<Arc<Runtime>>,
    Extension(llms): Extension<Arc<RwLock<LLMChatCompletionsModelStore>>>,
    accept: Option<TypedHeader<Accept>>,
    Json(payload): Json<Request>,
) -> Response {
    // track ai_inferences_with_spice_count metric
    let context = RequestContext::current(AsyncMarker::new().await);
    let df = get_current_datafusion(&context);

    crate::model::add_tools_used(&context, 1);

    let span = tracing::span!(target: "task_history", tracing::Level::INFO, "nsql", input = %payload.query, model = %payload.model, "labels");

    if let Some(traceparent) = context.trace_parent() {
        crate::http::traceparent::override_task_history_with_trace_parent(&span, traceparent);
    }

    // Default to all available tables if specific table(s) are not provided.
    let tables = payload
        .datasets
        .map(|ds| ds.iter().map(TableReference::from).collect_vec())
        .unwrap_or(df.get_user_table_names());

    // Create assistant/tool result messages for calling `table_schema` tool for all or provided tables.
    let schema_messages = match create_tool_use_messages(
        &TableSchemaTool::new(Arc::clone(&rt), None, None),
        "schemas-nsql",
        &TableSchemaToolParams::new(tables.iter().map(ToString::to_string).collect::<Vec<_>>()),
    )
    .instrument(span.clone())
    .await
    {
        Ok(m) => m,
        Err(e) => {
            tracing::error!("Error getting schema messages: {e}");
            return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
        }
    };

    // Create sample data assistant/tool messages if user wants to sample from dataset(s).
    let sample_data_messages = if payload.sample_data_enabled {
        match sample_messages(&tables, Arc::clone(&rt))
            .instrument(span.clone())
            .await
        {
            Ok(m) => m,
            Err(e) => {
                tracing::error!("Error sampling datasets for NSQL messages: {e}");
                return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
            }
        }
    } else {
        vec![]
    };

    let models = llms.read().await;
    let Some(nql_model) = models.get(&payload.model) else {
        return (
            StatusCode::BAD_REQUEST,
            format!("Model {} not found", payload.model),
        )
            .into_response();
    };

    let sql_gen = nql_model.as_sql().unwrap_or(&DefaultSqlGeneration {});
    // Tracks previously generated queries and associated errors to enable an efficient retry mechanism
    let mut sql_gen_ctx = QueryGenerationContext::default();
    let mut num_retries = 0;

    loop {
        let Ok(mut req) =
            sql_gen.create_request_for_query(&payload.model, &payload.query, &sql_gen_ctx)
        else {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Error preparing data for NQL model".to_string(),
            )
                .into_response();
        };

        req.messages.extend(schema_messages.clone());
        req.messages.extend(sample_data_messages.clone());

        let resp = match nql_model.chat_request(req).instrument(span.clone()).await {
            Ok(r) => r,
            Err(e) => {
                tracing::error!("Error running NQL model: {e}");
                return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
            }
        };

        // Run the SQL from the NSQL model through datafusion.
        match sql_gen.parse_response(resp) {
            Ok(Some(model_sql_query)) => {
                let cleaned_query = clean_model_based_sql(&model_sql_query);

                if return_sql_only(accept.as_ref()) {
                    tracing::trace!("Not running query, requested SQL only:\n{cleaned_query}");
                    return (StatusCode::OK, cleaned_query).into_response();
                }

                tracing::debug!("Running query:\n{cleaned_query}");

                match run_sql(Arc::clone(&df), &cleaned_query, None)
                    .instrument(span.clone())
                    .await
                {
                    Ok((data, cache_status)) => {
                        return to_http_response(
                            data,
                            cache_status,
                            ResponseMimeType::from_accept_header(accept.as_ref()),
                            ResponseMetadata::empty().with_sql(&cleaned_query),
                        )
                        .instrument(span.clone())
                        .await;
                    }
                    Err(e) => {
                        // If query failed, retry with the updated context

                        if num_retries >= DEFAULT_NSQL_RETRIES {
                            tracing::error!("Error executing query: {e}");
                            return (StatusCode::BAD_REQUEST, e.to_string()).into_response();
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
                    "No query produced from NSQL model".to_string(),
                )
                    .into_response();
            }
            Err(e) => {
                tracing::error!("Error running NSQL model: {e}");
                return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
            }
        }
    }
}
