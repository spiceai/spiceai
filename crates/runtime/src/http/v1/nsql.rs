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
use arrow_sql_gen::statement::CreateTableBuilder;
use async_openai::{
    error::OpenAIError,
    types::{
        ChatCompletionRequestMessage, ChatCompletionRequestSystemMessageArgs,
        ChatCompletionResponseFormat, ChatCompletionResponseFormatType,
        CreateChatCompletionRequest, CreateChatCompletionRequestArgs, CreateChatCompletionResponse,
    },
};
use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Extension, Json,
};
use datafusion::execution::context::SQLOptions;
use llms::{
    chat::{Error as ChatError, Result as ChatResult},
    openai::MAX_COMPLETION_TOKENS,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::{datafusion::DataFusion, http::v1::sql_to_http_response, model::LLMModelStore};

fn clean_model_based_sql(input: &str) -> String {
    let no_dashes = match input.strip_prefix("--") {
        Some(rest) => rest.to_string(),
        None => input.to_string(),
    };

    // Only take the first query, if there are multiple.
    let one_query = no_dashes.split(';').next().unwrap_or(&no_dashes);
    one_query.trim().to_string()
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub struct Request {
    pub query: String,

    #[serde(rename = "use", default = "default_model")]
    pub model: String,
}

fn default_model() -> String {
    "nql".to_string()
}

pub(crate) async fn post(
    Extension(df): Extension<Arc<DataFusion>>,
    Extension(llms): Extension<Arc<RwLock<LLMModelStore>>>,
    Json(payload): Json<Request>,
) -> Response {
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
                let c = CreateTableBuilder::new(Arc::new(schm), format!("public.{t}").as_str());
                table_create_stms.push(c.build_postgres());
            }
            Err(e) => {
                tracing::error!("Error getting table={t} schema: {e}");
                return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
            }
        }
    }

    // Construct prompt
    let nsql_query = format!(
            "```SQL\n{table_create_schemas}\n-- Using valid postgres SQL, without comments, answer the following questions for the tables provided above.\n-- {user_query}",
            user_query=payload.query,
            table_create_schemas=table_create_stms.join("\n")
        );

    let nsql_query_copy = nsql_query.clone();

    tracing::trace!("Running prompt: {nsql_query}");

    let model_id = payload.model.clone();
    let response = match llms.read().await.get(&model_id) {
        Some(nql_model) => {
            let Ok(req) = create_chat_request(model_id, nsql_query) else {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Error preparing data for NQL model".to_string(),
                )
                    .into_response();
            };
            match nql_model.write().await.chat_request(req).await {
                Ok(r) => r,
                Err(e) => {
                    tracing::error!("Error running NQL model: {e}");
                    return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
                }
            }
        }
        None => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Model {} not found", payload.model),
            )
                .into_response()
        }
    };

    let restricted_sql_options = SQLOptions::new()
        .with_allow_ddl(false)
        .with_allow_dml(false)
        .with_allow_statements(false);

    // Run the SQL from the NSQL model through datafusion.
    match process_response(response) {
        Ok(Some(model_sql_query)) => {
            let cleaned_query = clean_model_based_sql(&model_sql_query);
            tracing::trace!("Running query:\n{cleaned_query}");

            sql_to_http_response(
                Arc::clone(&df),
                &cleaned_query,
                Some(restricted_sql_options),
                Some(nsql_query_copy),
            )
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

/// Convert the Json object returned when using a `{ "type": "json_object" } ` response format.
/// Expected format is `"content": "{\"arbitrary_key\": \"arbitrary_value\"}"`
pub fn convert_json_object_to_sql(raw_json: &str) -> ChatResult<Option<String>> {
    let result: Value =
        serde_json::from_str(raw_json).map_err(|source| ChatError::FailedToLoadModel {
            source: Box::new(source),
        })?;
    Ok(result["sql"].as_str().map(std::string::ToString::to_string))
}

pub fn create_chat_request(
    model_id: String,
    prompt: String,
) -> Result<CreateChatCompletionRequest, OpenAIError> {
    let messages: Vec<ChatCompletionRequestMessage> = vec![
        ChatCompletionRequestSystemMessageArgs::default()
            .content("Return JSON, with the requested SQL under 'sql'.")
            .build()?
            .into(),
        ChatCompletionRequestSystemMessageArgs::default()
            .content(prompt)
            .build()?
            .into(),
    ];

    CreateChatCompletionRequestArgs::default()
        .model(model_id)
        .response_format(ChatCompletionResponseFormat {
            r#type: ChatCompletionResponseFormatType::JsonObject,
        })
        .messages(messages)
        .max_tokens(MAX_COMPLETION_TOKENS)
        .build()
}

pub fn process_response(resp: CreateChatCompletionResponse) -> ChatResult<Option<String>> {
    if let Some(usage) = resp.usage {
        if usage.completion_tokens >= u32::from(MAX_COMPLETION_TOKENS) {
            tracing::warn!(
                "Completion response may have been cut off after {} tokens",
                MAX_COMPLETION_TOKENS
            );
        }
    }

    match resp.choices.iter().find_map(|c| c.message.content.clone()) {
        Some(json_resp) => convert_json_object_to_sql(&json_resp),
        None => Ok(None),
    }
}
