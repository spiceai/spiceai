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
use arrow::array::StringArray;
use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Extension, Json,
};
use datafusion::execution::context::SQLOptions;
use futures::TryStreamExt;

use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::{
    datafusion::{
        query::{Protocol, QueryBuilder},
        DataFusion,
    },
    EmbeddingModelStore,
};

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub struct TextRequest {
    pub text: String,

    #[serde(rename = "use", default = "default_model")]
    pub model: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub struct SqlRequest {
    pub sql: String,

    #[serde(rename = "use", default = "default_model")]
    pub model: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Request {
    Sql(SqlRequest),
    Text(TextRequest),
}

fn default_model() -> String {
    "embed".to_string()
}

// For [`SqlRequest`], create the text to embed by querying [`Datafusion`].
async fn to_text(
    df: Arc<DataFusion>,
    sql: String,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let opt = SQLOptions::new()
        .with_allow_ddl(false)
        .with_allow_dml(false)
        .with_allow_statements(false);

    let query = QueryBuilder::new(sql, Arc::clone(&df), Protocol::Http)
        .restricted_sql_options(Some(opt))
        .build();

    // Attempt to convert first column to String
    let result: Result<Vec<Result<Vec<String>, _>>, _> = query
        .run()
        .await
        .map(|r| r.data)?
        .map_ok(
            |r| match r.column(0).as_any().downcast_ref::<StringArray>() {
                Some(s) => Ok(s
                    .into_iter()
                    .flatten()
                    .map(ToString::to_string)
                    .collect::<Vec<String>>()),
                None => {
                    Err("Expected first column of SQL query to return a String type".to_string())
                }
            },
        )
        .try_collect()
        .await;

    match result {
        Ok(result) => {
            let result = result
                .into_iter()
                .collect::<Result<Vec<Vec<String>>, _>>()?;
            Ok(result.into_iter().flatten().collect())
        }
        Err(e) => Err(e.into()),
    }
}

pub(crate) async fn post(
    Extension(df): Extension<Arc<DataFusion>>,
    Extension(embeddings): Extension<Arc<RwLock<EmbeddingModelStore>>>,
    Json(payload): Json<Request>,
) -> Response {
    let (text, model) = match payload {
        Request::Text(TextRequest { text, model }) => (vec![text], model),
        Request::Sql(SqlRequest { sql, model }) => {
            let text = match to_text(Arc::clone(&df), sql).await {
                Ok(text) => text,
                Err(e) => {
                    return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
                }
            };
            (text, model)
        }
    };

    match embeddings.read().await.get(&model) {
        Some(embedding_model) => {
            let mut embedding_model = embedding_model.write().await;
            match embedding_model
                .embed(llms::embeddings::EmbeddingInput::StringBatch(text))
                .await
            {
                Ok(embedding) => (StatusCode::OK, Json(embedding)).into_response(),
                Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
            }
        }
        None => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Model {model} not found"),
        )
            .into_response(),
    }
}
