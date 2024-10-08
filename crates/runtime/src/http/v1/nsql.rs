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
use crate::{datafusion::DataFusion, http::v1::sql_to_http_response, model::LLMModelStore};
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
use std::sync::Arc;
use tokio::sync::RwLock;

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

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub struct Request {
    pub query: String,

    #[serde(default = "default_model")]
    pub model: String,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
struct Params {
    format: ArrowFormat,
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

    // Construct prompt
    let nsql_query = format!(
            "```SQL\n{table_create_schemas}\n-- Using valid postgres SQL, without comments, answer the following questions for the tables provided above.\n-- {user_query}",
            user_query=payload.query,
            table_create_schemas=table_create_stms.join("\n")
        );

    tracing::trace!("Running prompt: {nsql_query}");

    let model_id = payload.model.clone();

    let sql_query_result = match llms.read().await.get(&model_id) {
        Some(nql_model) => {
            let sql_gen = nql_model.as_sql().unwrap_or(&DefaultSqlGeneration {});
            let Ok(req) =
                sql_gen.create_request_for_query(&model_id, &payload.query, &table_create_stms)
            else {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Error preparing data for NQL model".to_string(),
                )
                    .into_response();
            };
            let resp = match nql_model.chat_request(req).await {
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
                Some(&nsql_query),
                ArrowFormat::from_accept_header(&accept),
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
