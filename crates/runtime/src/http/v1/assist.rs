use arrow::array::RecordBatch;
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
use async_stream::stream;
use axum::{
    extract::Query,
    http::StatusCode,
    response::{
        sse::{Event, KeepAlive, Sse},
        IntoResponse, Response,
    },
    Extension, Json,
};

use datafusion::sql::TableReference;

use llms::chat::Chat;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;

use futures::StreamExt;

use crate::{
    embeddings::vector_search::{RetrievalLimit, VectorSearch, VectorSearchResult},
    model::LLMModelStore,
};

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) struct AssistResponse {
    pub text: String,

    // Key is a serialised [`TableReference`].
    // Value is the serialized JSON representation of the primary keys from an arrow batch.
    pub from: HashMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub struct Request {
    pub text: String,

    /// The model to use for chat completion. The embedding model is determined via the data source (that has the associated embeddings).
    #[serde(rename = "use", default = "default_model")]
    pub model: String,

    /// Which datasources in the [`DataFusion`] instance to retrieve data from.
    #[serde(rename = "from", default)]
    pub data_source: Vec<String>,
}

fn default_model() -> String {
    "embed".to_string()
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub struct QueryParams {
    /// If true, provide the result as a SSE stream. Otherwise, provide the result as a JSON response after the entire LLM inference is complete.
    /// For a stream, the first event will be the primary keys of the relevant data, and the
    /// following events will be the LLM completions (as streamed from the underlying LLM).
    #[serde(default)]
    pub stream: bool,
}

fn combined_relevant_data_and_input(
    relevant_data: &HashMap<TableReference, Vec<String>>,
    input: &str,
) -> String {
    let data = relevant_data
        .iter()
        .map(|(_table_ref, rows)| format!("Table: \n{}\n", rows.join("\n")))
        .collect::<Vec<String>>()
        .join("\n");

    format!("Here is the relevant data from multiple sources:\n\n{data}\nQuestion:\n{input}\n")
}

#[allow(clippy::from_iter_instead_of_collect)]
fn create_assist_response_from(
    table_primary_keys: &HashMap<TableReference, Vec<RecordBatch>>,
) -> Result<HashMap<String, Value>, Box<dyn std::error::Error>> {
    let from_value_iter = table_primary_keys
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
        .collect::<Result<Vec<_>, Box<dyn std::error::Error>>>()?;
    let from_value: HashMap<String, Value> =
        HashMap::from_iter(from_value_iter.iter().map(|(k, v)| (k.clone(), v.clone())));
    Ok(from_value)
}

async fn context_aware_stream(
    model: &RwLock<Box<dyn Chat>>,
    vector_search_data: &VectorSearchResult,
    model_input: String,
) -> Response {
    let mut model_stream = match model.write().await.stream(model_input).await {
        Ok(model_stream) => model_stream,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    };
    let vector_data = match create_assist_response_from(&vector_search_data.retrieved_public_keys) {
        Ok(vector_data) => vector_data,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    };

    Sse::new(Box::pin(stream! {
        yield Event::default().json_data(vector_data);
        while let Some(msg_result) = model_stream.next().await {
            match msg_result {
                Err(e) => {
                    yield Err(axum::Error::new(e.to_string()));
                    break;
                }
                Ok(msg_opt) => {
                    if let Some(msg) = msg_opt {
                        yield Ok(Event::default().data(msg));
                    } else {
                        break;
                    }
                }
            }
        }
    }))
    .keep_alive(KeepAlive::default())
    .into_response()
}

async fn context_aware_chat(
    model: &RwLock<Box<dyn Chat>>,
    vector_search_data: &VectorSearchResult,
    model_input: String,
) -> Response {
    match model.write().await.run(model_input).await {
        Ok(Some(text)) => {
            match create_assist_response_from(&vector_search_data.retrieved_public_keys) {
                Ok(from) => (StatusCode::OK, Json(AssistResponse { text, from })).into_response(),
                Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
            }
        }
        Ok(None) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "No response from LLM".to_string(),
        )
            .into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

/// Assist runs a question or statement through an LLM with additional context retrieved from data within the [`DataFusion`] instance.
/// Logic:
/// 1. If user did not provide which/what data source to use, figure this out (?).
/// 2. Get embedding provider for each data source.
/// 2. Create embedding(s) of question/statement
/// 3. Retrieve relevant data from the data source.
/// 4. Run [relevant data;  question/statement] through LLM.
/// 5. Return [text response, <datasets -> .
///
/// Return format
///
/// ```json
/// {
///    "text": "response from LLM",
///    "from" : {
///       "table_name": ["primary_key1", "primary_key2", "primary_key3"]
///   }
/// }
/// ```
///  - `from` returns the primary key of the relevant rows from each `payload.datasources` if
/// primary keys for the table can be determined. An attempt to determine the primary key will be
/// from the underlying Datafusion [`TableProvider`]'s `constraints()`. It can be explicitly
/// provided  within the spicepod configuration, under the `datasets[*].embeddings.column_pk` path.
/// For example:
///
/// ```yaml
/// datasets:
///   - from: <postgres:syncs>
///     name: daily_journal
///     embeddings:
///       - column: answer
///         use: oai
///         column_pk: id
///
/// ```
///
///
#[allow(clippy::too_many_lines)]
pub(crate) async fn post(
    Extension(vs): Extension<Arc<VectorSearch>>,
    Extension(llms): Extension<Arc<RwLock<LLMModelStore>>>,
    Query(params): Query<QueryParams>,
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

    let relevant_data = match vs
        .search(payload.text.clone(), input_tables, RetrievalLimit::TopN(3))
        .await
    {
        Ok(relevant_data) => relevant_data,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    };

    tracing::debug!(
        "Relevant data from vector search: {:#?}",
        relevant_data.retrieved_entries
    );

    // Run LLM with input.
    match llms.read().await.get(&payload.model) {
        Some(llm_model) => {
            let model_input = combined_relevant_data_and_input(
                &relevant_data.retrieved_entries,
                &payload.text.clone(),
            );
            if params.stream {
                context_aware_stream(llm_model, &relevant_data, model_input).await
            } else {
                context_aware_chat(llm_model, &relevant_data, model_input).await
            }
        }
        None => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Model {} not found", payload.model),
        )
            .into_response(),
    }
}
