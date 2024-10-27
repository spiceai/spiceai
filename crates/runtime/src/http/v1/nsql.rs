/*
Copyright 2024 The Spice.ai OSS Authors

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
    datafusion::DataFusion,
    http::v1::sql_to_http_response,
    model::LLMModelStore,
    tools::builtin::{
        sample::{
            distinct::DistinctColumnsParams, random::RandomSampleParams, tool::SampleDataTool,
            SampleTableParams,
        },
        table_schema::{TableSchemaTool, TableSchemaToolParams},
    },
};
use async_openai::{
    error::OpenAIError,
    types::{
        ChatCompletionMessageToolCall, ChatCompletionRequestAssistantMessage,
        ChatCompletionRequestAssistantMessageArgs, ChatCompletionRequestMessage,
        ChatCompletionRequestToolMessage, ChatCompletionRequestToolMessageArgs,
        ChatCompletionRequestToolMessageContent, ChatCompletionToolType, FunctionCall,
    },
};
use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Extension, Json,
};
use axum_extra::TypedHeader;
use datafusion::sql::TableReference;
use headers_accept::Accept;

use itertools::Itertools;
use llms::chat::nsql::default::DefaultSqlGeneration;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::Span;
use tracing_futures::Instrument;

use super::ArrowFormat;

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
    df: Arc<DataFusion>,
) -> Result<Vec<ChatCompletionRequestMessage>, Box<dyn std::error::Error + Send + Sync>> {
    let mut messages = Vec::with_capacity(4 * sample_from.len());

    for dataset in sample_from {
        for params in [
            SampleTableParams::DistinctColumns(DistinctColumnsParams {
                tbl: dataset.to_string(),
                limit: 3,
                cols: None,
            }),
            SampleTableParams::RandomSample(RandomSampleParams {
                tbl: dataset.to_string(),
                limit: 3,
            }),
        ] {
            let (req, resp) = call_sample_and_create_messages(Arc::clone(&df), &params)
                .instrument(Span::current())
                .await?;
            messages.push(req.into());
            messages.push(resp.into());
        }
    }
    Ok(messages)
}

/// Runs the [`TableSchemaTool`] on the provided tables and returns an Assistant and Tool message as if requested by a language model.
async fn schema_messages(
    df: Arc<DataFusion>,
    tables: &[TableReference],
) -> Result<Vec<ChatCompletionRequestMessage>, Box<dyn std::error::Error + Send + Sync>> {
    let schema_tool = TableSchemaTool::default();
    let schema_tool_params =
        TableSchemaToolParams::new(tables.iter().map(ToString::to_string).collect::<Vec<_>>());

    let table_schemas = schema_tool
        .get_schema(Arc::clone(&df), &schema_tool_params)
        .instrument(Span::current())
        .await?;
    let table_schema_assistant_msg = schema_tool
        .to_assistant_request_message("schemas-nsql", &schema_tool_params)
        .boxed()?;
    let table_schema_tool_msg = schema_tool
        .to_tool_response_message("schemas-nsql", &table_schemas)
        .boxed()?;

    Ok(vec![
        table_schema_assistant_msg.into(),
        table_schema_tool_msg.into(),
    ])
}

/// Call the `sample_data` tool with the given parameters and create associated Assistant and Tool messages.
async fn call_sample_and_create_messages(
    df: Arc<DataFusion>,
    params: &SampleTableParams,
) -> Result<
    (
        ChatCompletionRequestAssistantMessage,
        ChatCompletionRequestToolMessage,
    ),
    Box<dyn std::error::Error + Send + Sync>,
> {
    let ds = params.dataset();
    let result = SampleDataTool::new(params.into())
        .call_with(params, Arc::clone(&df))
        .instrument(Span::current())
        .await?;

    let req = ChatCompletionRequestAssistantMessageArgs::default()
        .tool_calls(vec![ChatCompletionMessageToolCall {
            id: format!("distinct-{ds}-nsql"),
            r#type: ChatCompletionToolType::Function,
            function: FunctionCall {
                name: "sample_data".to_string(),
                arguments: serde_json::to_string(&params)
                    .map_err(OpenAIError::JSONDeserialize)?
                    .to_string(),
            },
        }])
        .build()
        .boxed()?;

    let resp = ChatCompletionRequestToolMessageArgs::default()
        .tool_call_id(format!("distinct-{ds}-nsql"))
        .content(ChatCompletionRequestToolMessageContent::Text(
            result.to_string(),
        ))
        .build()
        .boxed()?;

    Ok((req, resp))
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub struct Request {
    pub query: String,

    #[serde(default = "default_model")]
    pub model: String,

    #[serde(default = "default_sample_data_enabled")]
    pub sample_data_enabled: bool,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub datasets: Option<Vec<String>>,
}

fn default_sample_data_enabled() -> bool {
    true
}

fn default_model() -> String {
    "nql".to_string()
}

pub(crate) async fn post(
    Extension(df): Extension<Arc<DataFusion>>,
    Extension(llms): Extension<Arc<RwLock<LLMModelStore>>>,
    accept: Option<TypedHeader<Accept>>,
    Json(payload): Json<Request>,
) -> Response {
    let span = tracing::span!(target: "task_history", tracing::Level::INFO, "nsql", input = %payload.query, model = %payload.model, "labels");

    // Default to all available tables if specific table(s) are not provided.
    let tables = payload
        .datasets
        .map(|ds| ds.iter().map(TableReference::from).collect_vec())
        .unwrap_or(df.get_user_table_names());

    // Create assistant/tool result messages for calling `table_schema` tool for all or provided tables.
    let schema_messages = match schema_messages(Arc::clone(&df), &tables)
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
    let tool_use_messages = if payload.sample_data_enabled {
        match sample_messages(&tables, Arc::clone(&df))
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

    let sql_query_result = match llms.read().await.get(&payload.model) {
        Some(nql_model) => {
            let sql_gen = nql_model.as_sql().unwrap_or(&DefaultSqlGeneration {});
            let Ok(mut req) = sql_gen.create_request_for_query(&payload.model, &payload.query)
            else {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Error preparing data for NQL model".to_string(),
                )
                    .into_response();
            };

            req.messages.extend(schema_messages);
            req.messages.extend(tool_use_messages);

            let resp = match nql_model.chat_request(req).instrument(span.clone()).await {
                Ok(r) => r,
                Err(e) => {
                    tracing::error!("Error running NQL model: {e}");
                    return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
                }
            };
            sql_gen.parse_response(resp)
        }
        None => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Model {} not found", payload.model),
            )
                .into_response()
        }
    };

    // Run the SQL from the NSQL model through datafusion.
    match sql_query_result {
        Ok(Some(model_sql_query)) => {
            let cleaned_query = clean_model_based_sql(&model_sql_query);
            tracing::trace!("Running query:\n{cleaned_query}");
            sql_to_http_response(
                Arc::clone(&df),
                &cleaned_query,
                ArrowFormat::from_accept_header(&accept),
            )
            .instrument(span.clone())
            .await
        }
        Ok(None) => {
            tracing::trace!("No query produced from NSQL model");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "No query produced from NSQL model".to_string(),
            )
                .into_response()
        }
        Err(e) => {
            tracing::error!("Error running NSQL model: {e}");
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
        }
    }
}
