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
use async_openai::types::EmbeddingInput;
use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Extension, Json,
};
use datafusion::{datasource::TableProvider, sql::TableReference};

use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;

use crate::{
    datafusion::DataFusion, embeddings::table::EmbeddingTable, model::LLMModelStore,
    EmbeddingModelStore,
};

#[derive(Debug, Serialize, Deserialize)]
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

async fn create_input_embeddings(
    input: &str,
    embeddings_to_run: Vec<String>,
    embeddings: Arc<RwLock<EmbeddingModelStore>>,
) -> Result<HashMap<String, Vec<f32>>, Box<dyn std::error::Error>> {
    let mut embedded_inputs: HashMap<String, Vec<f32>> = HashMap::new();
    for (name, model) in embeddings
        .read()
        .await
        .iter()
        .filter(|(model_name, _)| embeddings_to_run.contains(model_name))
    {
        match model
            .write()
            .await
            .embed(EmbeddingInput::String(input.to_string()))
            .await
        {
            Ok(embedding) => match embedding.first() {
                Some(embedding) => {
                    embedded_inputs.insert(name.clone(), embedding.clone());
                }
                None => {
                    return Err(format!("No embeddings returned for input text from {name}").into())
                }
            },
            Err(e) => return Err(Box::new(e)),
        }
    }
    Ok(embedded_inputs)
}

fn combined_relevant_data_and_input(
    relevant_data: &HashMap<TableReference, Vec<String>>,
    input: &str,
) -> String {
    let data = relevant_data.values().map(|v| v.join("\n")).join("\n");
    format!("{data}\n{input}")
}

/// Find the name of columns in the table reference that have associated embedding columns.
fn embedding_columns_in(tbl: &Arc<dyn TableProvider>) -> Vec<String> {
    match tbl.as_any().downcast_ref::<EmbeddingTable>() {
        Some(embedding_table) => embedding_table.get_embedding_models_used(),
        None => vec![],
    }
}

/// For the data sources that assumedly exist in the [`DataFusion`] instance, find the embedding models used in each data source.
async fn find_relevant_embedding_models(
    data_sources: Vec<TableReference>,
    df: Arc<DataFusion>,
) -> Result<HashMap<TableReference, Vec<String>>, Box<dyn std::error::Error>> {
    let mut embeddings_to_run = HashMap::new();
    for data_source in data_sources {
        match df.get_table(data_source.clone()).await {
            None => {
                return Err(format!("Data source {} does not exist", data_source.clone()).into())
            }
            Some(table) => match embedding_columns_in(&table) {
                v if v.is_empty() => {
                    return Err(format!(
                        "Data source {} does not have an embedded column",
                        data_source.clone()
                    )
                    .into())
                }
                v => {
                    embeddings_to_run.insert(data_source, v);
                }
            },
        }
    }
    Ok(embeddings_to_run)
}

async fn vector_search(
    df: Arc<DataFusion>,
    embedded_inputs: HashMap<TableReference, Vec<Vec<f32>>>,
    n: usize,
) -> Result<HashMap<TableReference, Vec<String>>, Box<dyn std::error::Error>> {
    let mut search_result: HashMap<TableReference, Vec<String>> = HashMap::new();

    for (tbl, search_vectors) in embedded_inputs {
        tracing::debug!("Running vector search for table {tbl:#?}");

        let provider = df
            .get_table(tbl.clone())
            .await
            .ok_or(format!("Table {} not found", tbl.clone()))?;

        let embedding_table = provider
            .as_any()
            .downcast_ref::<EmbeddingTable>()
            .ok_or(format!("Table {tbl} is not an embedding table"))?;

        // Only support one embedding column per table.
        let embedding_columns = embedding_table.get_embedding_columns();
        let embedding_column = embedding_columns
            .first()
            .ok_or(format!("No embeddings found for table {tbl}"))?;

        if search_vectors.len() != 1 {
            return Err(format!("Only one embedding column per table currently supported. Table: {tbl} has {} embeddings", search_vectors.len()).into());
        }
        match search_vectors.first() {
            None => return Err(format!("No embeddings found for table {tbl}").into()),
            Some(embedding) => {
                let sql_query = format!("SELECT {embedding_column} FROM {tbl} ORDER BY array_distance({embedding_column}_embedding, {embedding:?}) LIMIT {n}");

                let result = df.ctx.sql(&sql_query).await?;
                let batch = result.collect().await?;

                let outt: Vec<_> = batch
                    .iter()
                    .map(|b| {
                        let z =
                            b.column(0).as_any().downcast_ref::<StringArray>().ok_or(
                                "Expected first column of SQL query to return a String type",
                            );
                        let zz = z.map(|s| {
                            s.iter()
                                .map(|ss| ss.unwrap_or_default().to_string())
                                .collect::<Vec<String>>()
                        });
                        zz
                    })
                    .collect::<Result<Vec<_>, &str>>()?;

                let outtt: Vec<String> = outt.iter().flat_map(std::clone::Clone::clone).collect();
                search_result.insert(tbl, outtt);
            }
        };
    }

    Ok(search_result)
}

/// Assist runs a question or statement through an LLM with additional context retrieved from data within the [`DataFusion`] instance.
/// Logic:
/// 1. If user did not provide which/what data source to use, figure this out (?).
/// 2. Get embedding provider for each data source.
/// 2. Create embedding(s) of question/statement
/// 3. Retrieve relevant data from the data source.
/// 4. Run [relevant data;  question/statement] through LLM.
/// 5. Return response.
pub(crate) async fn post(
    Extension(df): Extension<Arc<DataFusion>>,
    Extension(embeddings): Extension<Arc<RwLock<EmbeddingModelStore>>>,
    Extension(llms): Extension<Arc<RwLock<LLMModelStore>>>,
    Json(payload): Json<Request>,
) -> Response {
    // For now, force the user to specify which data.
    if payload.data_source.is_empty() {
        return (StatusCode::BAD_REQUEST, "No data sources provided").into_response();
    }

    // Determine which embedding models need to be run. If a table does not have an embedded column, return an error.
    let embeddings_to_run = match find_relevant_embedding_models(
        payload
            .data_source
            .iter()
            .map(TableReference::from)
            .collect(),
        Arc::clone(&df),
    )
    .await
    {
        Ok(embeddings_to_run) => embeddings_to_run,
        Err(e) => return (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    };

    // Create embedding(s) for question/statement. `embedded_inputs` model_name -> embedding.
    let embedded_inputs = match create_input_embeddings(
        &payload.text.clone(),
        embeddings_to_run.values().flatten().cloned().collect(),
        Arc::clone(&embeddings),
    )
    .await
    {
        Ok(embedded_inputs) => embedded_inputs,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    };

    let per_table_embeddings: HashMap<TableReference, _> = embeddings_to_run
        .iter()
        .map(|(t, model_names)| {
            let z: Vec<_> = model_names
                .iter()
                .filter_map(|m| embedded_inputs.get(m).cloned())
                .collect();
            (t.clone(), z)
        })
        .collect();

    // Vector search to get relevant data from data sources.
    let relevant_data = match vector_search(Arc::clone(&df), per_table_embeddings, 3).await {
        Ok(relevant_data) => relevant_data,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    };

    // Using returned data, create input for LLM.
    let model_input = combined_relevant_data_and_input(&relevant_data, &payload.text);

    // Run LLM with input.
    match llms.read().await.get(&payload.model) {
        Some(llm_model) => {
            // TODO: We need to either 1. Separate LLMs from NQL, or create a LLM trait separately.
            match llm_model.write().await.run(model_input).await {
                Ok(Some(assist)) => (StatusCode::OK, Json(assist)).into_response(),
                Ok(None) => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("No response from LLM {}", payload.model),
                )
                    .into_response(),
                Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
            }
        }
        None => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Model {} not found", payload.model),
        )
            .into_response(),
    }
}
