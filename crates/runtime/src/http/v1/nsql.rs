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
    tools::builtin::sample_data::{SampleDataTool, SampleDataToolParams},
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
use datafusion_table_providers::sql::arrow_sql_gen::statement::CreateTableBuilder;
use headers_accept::Accept;

use llms::chat::nsql::default::DefaultSqlGeneration;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use std::sync::Arc;
use tokio::sync::RwLock;
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

/// Convert the [`SampleDataToolParams`] into how an LLM would ask to use it (via a [`ChatCompletionRequestAssistantMessage`]).
fn into_assistant_message(
    id: &str,
    params: SampleDataToolParams,
) -> Result<ChatCompletionRequestAssistantMessage, OpenAIError> {
    ChatCompletionRequestAssistantMessageArgs::default()
        .tool_calls(vec![ChatCompletionMessageToolCall {
            id: id.to_string(),
            r#type: ChatCompletionToolType::Function,
            function: FunctionCall {
                name: "sample_data".to_string(),
                arguments: serde_json::to_value(params)
                    .map_err(OpenAIError::JSONDeserialize)?
                    .to_string(),
            },
        }])
        .build()
}

/// Convert the result of a [`SampleDataTool`] call how we would return it to the LLM, (via a [`ChatCompletionRequestToolMessage`]).
fn into_tool_message(
    id: &str,
    result: &serde_json::Value,
) -> Result<ChatCompletionRequestToolMessage, OpenAIError> {
    ChatCompletionRequestToolMessageArgs::default()
        .tool_call_id(id.to_string())
        .content(ChatCompletionRequestToolMessageContent::Text(
            result.to_string(),
        ))
        .build()
}

async fn sample_messages(
    sample_from: &[String],
    df: Arc<DataFusion>,
) -> Result<Vec<ChatCompletionRequestMessage>, Box<dyn std::error::Error + Send + Sync>> {
    let params = SampleDataToolParams {
        datasets: Some(sample_from.to_vec()),
        n: 3,
    };
    let result = SampleDataTool::default()
        .call_tool(&params, Arc::clone(&df))
        .await?;
    let req = into_assistant_message("sample_data-nsql", params).boxed()?;
    let resp = into_tool_message("sample_data-nsql", &result).boxed()?;

    Ok(vec![req.into(), resp.into()])
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub struct Request {
    pub query: String,

    #[serde(default = "default_model")]
    pub model: String,

    pub sample_from: Option<Vec<String>>,
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

    // Get all public table CREATE TABLE statements to add to prompt.
    let tables = match df.get_public_table_names() {
        Ok(t) => t,
        Err(e) => {
            tracing::error!("Error getting tables: {e}");
            return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
        }
    };

    let mut table_create_stms: Vec<String> = Vec::with_capacity(tables.len());
    for t in &tables {
        match df.get_arrow_schema(t).await {
            Ok(schm) => {
                tracing::trace!("Table {t} has CREATE STATEMENT='{schm}'.");

                // Ensure compiling without `--features models` is successful.
                #[cfg(feature = "models")]
                table_create_stms.extend_from_slice(
                    &CreateTableBuilder::new(Arc::new(schm), t).build_postgres(),
                );
            }
            Err(e) => {
                tracing::error!("Error getting table={t} schema: {e}");
                return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
            }
        }
    }

    // Create sample data assistant/tool messages if user wants to sample from dataset(s).
    let tool_use_messages = match payload.sample_from {
        Some(sample_from) => match sample_messages(&sample_from, Arc::clone(&df)).await {
            Ok(m) => m,
            Err(e) => {
                tracing::error!("Error sampling datasets for NSQL messages: {e}");
                return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
            }
        },
        None => vec![],
    };

    let sql_query_result = match llms.read().await.get(&payload.model) {
        Some(nql_model) => {
            let sql_gen = nql_model.as_sql().unwrap_or(&DefaultSqlGeneration {});
            let Ok(mut req) = sql_gen.create_request_for_query(
                &payload.model,
                &payload.query,
                &table_create_stms,
            ) else {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Error preparing data for NQL model".to_string(),
                )
                    .into_response();
            };

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
