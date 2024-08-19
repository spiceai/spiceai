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
use crate::embeddings::vector_search::{RetrievalLimit, VectorSearch, VectorSearchResult};
use arrow::array::RecordBatch;
use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Extension, Json,
};
use datafusion::sql::TableReference;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::HashMap, sync::Arc};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub struct Request {
    pub text: String,

    /// Which datasources in the [`DataFusion`] instance to retrieve data from.
    #[serde(rename = "from", default)]
    pub data_source: Vec<String>,

    #[serde(default = "default_limit")]
    pub limit: usize,
}

fn default_limit() -> usize {
    3
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResponse {
    pub entries: HashMap<String, Vec<String>>,
    pub retrieved_primary_keys: HashMap<String, Value>,
}

impl SearchResponse {
    pub fn from_vector_search(
        result: VectorSearchResult,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let keys = create_primary_key_payload(&result.retrieved_primary_keys)?;
        Ok(Self {
            entries: result
                .retrieved_entries
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect(),
            retrieved_primary_keys: keys,
        })
    }
}

pub(crate) async fn post(
    Extension(vs): Extension<Arc<VectorSearch>>,
    Json(payload): Json<Request>,
) -> Response {
    // For now, force the user to specify which data.
    if payload.data_source.is_empty() {
        return (StatusCode::BAD_REQUEST, "No data sources provided").into_response();
    }
    let input_tables: Vec<TableReference> = payload
        .data_source
        .iter()
        .map(TableReference::from)
        .collect();

    let span = tracing::span!(target: "task_history", tracing::Level::INFO, "vector_search", input = %payload.text);

    match vs
        .search(
            payload.text.clone(),
            input_tables,
            RetrievalLimit::TopN(payload.limit),
        )
        .await
    {
        Ok(resp) => match SearchResponse::from_vector_search(resp) {
            Ok(r) => {
                span.in_scope(|| {
                    tracing::info!(name = "labels", target = "task_history", outputs_produced = %r.entries.len());
                });
                (StatusCode::OK, Json(r)).into_response()
            }
            Err(e) => {
                span.in_scope(|| {
                    tracing::error!(target: "task_history", "{e}");
                });
                (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
            }
        },
        Err(e) => {
            span.in_scope(|| {
                tracing::error!(target: "task_history", "{e}");
            });
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
        }
    }
}

pub(crate) fn create_primary_key_payload(
    table_primary_keys: &HashMap<TableReference, Vec<RecordBatch>>,
) -> Result<HashMap<String, Value>, Box<dyn std::error::Error>> {
    table_primary_keys
        .iter()
        .map(|(tbl, pks)| {
            let buf = Vec::new();
            let mut writer = arrow_json::ArrayWriter::new(buf);
            for pk in pks {
                writer.write_batches(&[pk])?;
            }
            writer.finish()?;
            let res: Value = match String::from_utf8(writer.into_inner()) {
                Ok(res) => serde_json::from_str(&res)?,
                Err(e) => {
                    tracing::debug!("Error converting JSON buffer to string: {e}");
                    serde_json::Value::String(String::new())
                }
            };
            Ok((tbl.to_string(), res))
        })
        .collect()
}
