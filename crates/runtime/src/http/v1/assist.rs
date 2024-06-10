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
use app::App;
use arrow::array::{RecordBatch, StringArray};
use async_openai::types::EmbeddingInput;
use async_stream::stream;
use axum::{
    http::StatusCode,
    response::{
        sse::{Event, KeepAlive, Sse},
        IntoResponse, Response,
    },
    Extension, Json,
};

use datafusion::{common::Constraint, datasource::TableProvider, sql::TableReference};

use itertools::Itertools;
use llms::chat::Chat;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::HashMap, error::Error, sync::Arc};
use tokio::sync::RwLock;

use futures::{FutureExt, Stream, StreamExt};

use crate::{
    accelerated_table::AcceleratedTable, datafusion::DataFusion, embeddings::table::EmbeddingTable,
    model::LLMModelStore, EmbeddingModelStore,
};

pub(crate) struct VectorSearchResponse {
    pub retrieved_entries: HashMap<TableReference, Vec<String>>,
    pub retrieved_public_keys: HashMap<TableReference, Vec<RecordBatch>>,
}

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

/// If a [`TableProvider`] is an [`EmbeddingTable`], return the [`EmbeddingTable`].
/// This includes if the [`TableProvider`] is an [`AcceleratedTable`] with a [`EmbeddingTable`] underneath.
fn get_embedding_table(tbl: &Arc<dyn TableProvider>) -> Option<Arc<EmbeddingTable>> {
    if let Some(embedding_table) = tbl.as_any().downcast_ref::<EmbeddingTable>() {
        return Some(Arc::new(embedding_table.clone()));
    }
    if let Some(accelerated_table) = tbl.as_any().downcast_ref::<AcceleratedTable>() {
        if let Some(embedding_table) = accelerated_table
            .get_federated_table()
            .as_any()
            .downcast_ref::<EmbeddingTable>()
        {
            return Some(Arc::new(embedding_table.clone()));
        }
    }
    None
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
            Some(table) => match get_embedding_table(&table)
                .map(|e| e.get_embedding_models_used())
                .unwrap_or_default()
            {
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

/// Perform a basic vector search.
///
/// ## Arguments
/// df: The [`DataFusion`] instance. Expected to have every [`TableReference`] in `embedded_inputs`.
/// `embedded_inputs`: The embeddings to search for, for each embedding column the [`TableReference`]
///     contains (in the correct order).
/// `table_primary_keys`: Optional for each [`TableReference`], the primary keys of the table. If not
///     provided, only the column underlying the embedding column will be returned.
/// n: The number of results to return, per [`TableReference`].
///
/// ## Limitations
/// - Only supports one embedding column per table.
async fn vector_search(
    df: Arc<DataFusion>,
    embedded_inputs: HashMap<TableReference, Vec<Vec<f32>>>,
    table_primary_keys: HashMap<TableReference, Vec<String>>,
    n: usize,
) -> Result<VectorSearchResponse, Box<dyn std::error::Error>> {
    let mut response = VectorSearchResponse {
        retrieved_entries: HashMap::new(),
        retrieved_public_keys: HashMap::new(),
    };

    for (tbl, search_vectors) in embedded_inputs {
        tracing::debug!("Running vector search for table {:#?}", tbl.clone());

        // Only support one embedding column per table.
        let table_provider = df
            .get_table(tbl.clone())
            .await
            .ok_or(format!("Table {} not found", tbl.clone()))?;
        let embedding_column = get_embedding_table(&table_provider)
            .and_then(|e| e.get_embedding_columns().first().cloned())
            .ok_or(format!("No embeddings found for table {tbl}"))?;

        if search_vectors.len() != 1 {
            return Err(format!("Only one embedding column per table currently supported. Table: {tbl} has {} embeddings", search_vectors.len()).into());
        }
        match search_vectors.first() {
            None => return Err(format!("No embeddings found for table {tbl}").into()),
            Some(embedding) => {
                let mut select_keys = table_primary_keys.get(&tbl).cloned().unwrap_or(vec![]);
                select_keys.push(embedding_column.clone());

                let sql_query = format!(
                    "SELECT {} FROM {tbl} ORDER BY array_distance({embedding_column}_embedding, {embedding:?}) LIMIT {n}", select_keys.join(", ")
                );

                let result = df.ctx.sql(&sql_query).await?;
                let batch = result.collect().await?;

                let outt: Vec<_> = batch
                    .iter()
                    .map(|b| {
                        let z =
                            b.column(b.num_columns() -1).as_any().downcast_ref::<StringArray>().ok_or(
                                format!("Expected '{embedding_column}' to be last column of SQL query and return a String type"),
                            );
                        let zz = z.map(|s| {
                            s.iter()
                                .map(|ss| ss.unwrap_or_default().to_string())
                                .collect::<Vec<String>>()
                        });
                        zz
                    })
                    .collect::<Result<Vec<_>, String>>()?;

                let outtt: Vec<String> = outt.iter().flat_map(std::clone::Clone::clone).collect();

                response.retrieved_entries.insert(tbl.clone(), outtt);
                response.retrieved_public_keys.insert(tbl, batch);
            }
        };
    }
    tracing::debug!(
        "Relevant data from vector search: {:#?}",
        response.retrieved_entries,
    );
    Ok(response)
}

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

#[allow(clippy::from_iter_instead_of_collect)]
fn create_assist_response(
    text: String,
    table_primary_keys: &HashMap<TableReference, Vec<RecordBatch>>,
) -> Result<AssistResponse, Box<dyn std::error::Error>> {
    Ok(AssistResponse {
        text,
        from: create_assist_response_from(table_primary_keys)?,
    })
}

async fn prepare_and_run_vector_search(
    app: Arc<RwLock<Option<App>>>,
    df: Arc<DataFusion>,
    embeddings: Arc<RwLock<EmbeddingModelStore>>,
    payload: Request,
) -> Result<VectorSearchResponse, Box<dyn std::error::Error>> {
    
    let input_tables: Vec<TableReference> = payload
        .data_source
        .iter()
        .map(TableReference::from)
        .collect();

    let per_table_embeddings = calculate_embeddings_per_table(
        payload.text.clone(),
        input_tables.clone(),
        Arc::clone(&embeddings),
        Arc::clone(&df),
    )
    .await?;

    // Retrieve primary keys
    let tbl_to_pks = get_primary_keys(
        Arc::clone(&app),
        Arc::clone(&df),
        payload
            .data_source
            .iter()
            .map(TableReference::from)
            .collect(),
    )
    .await?;

    // Vector search to get relevant data from data sources.
    let relevant_data = vector_search(Arc::clone(&df), per_table_embeddings, tbl_to_pks, 3).await?;
    Ok(relevant_data)
}
    
/// For each embedding column that a [`TableReference`] contains, calculate the embeddings vector between the query and the column.
/// The returned `HashMap` is a mapping of [`TableReference`] to an (alphabetical by column name) in-order vector of embeddings.
async fn calculate_embeddings_per_table(
    query: String,
    data_sources: Vec<TableReference>,
    embeddings: Arc<RwLock<EmbeddingModelStore>>,
    df: Arc<DataFusion>,
) -> Result<HashMap<TableReference, Vec<Vec<f32>>>, Box<dyn std::error::Error>> {
    // Determine which embedding models need to be run. If a table does not have an embedded column, return an error.
    let embeddings_to_run = find_relevant_embedding_models(data_sources, df).await?;

    // Create embedding(s) for question/statement. `embedded_inputs` model_name -> embedding.
    let embedded_inputs = create_input_embeddings(
        &query.clone(),
        embeddings_to_run.values().flatten().cloned().collect(),
        embeddings,
    )
    .await?;

    Ok(embeddings_to_run
        .iter()
        .map(|(t, model_names)| {
            let z: Vec<_> = model_names
                .iter()
                .filter_map(|m| embedded_inputs.get(m).cloned())
                .collect();
            (t.clone(), z)
        })
        .collect())
}

/// For a set of tables, get their primary keys. Attempt to determine the primary key(s) of the
/// table from the [`TableProvider`] constraints, and if not provided, use the explicit primary
/// keys defined in the spicepod configuration.
async fn get_primary_keys(
    app: Arc<RwLock<Option<App>>>,
    df: Arc<DataFusion>,
    tables: Vec<TableReference>,
) -> Result<HashMap<TableReference, Vec<String>>, Box<dyn std::error::Error>> {
    let mut tbl_to_pks: HashMap<TableReference, Vec<String>> = HashMap::new();

    // Explicit primary keys are defined in the spicepod configuration.
    let explicit_primary_keys: HashMap<TableReference, Vec<String>> =
        app.read().await.as_ref().map_or(HashMap::new(), |app| {
            app.datasets
                .iter()
                .filter_map(|d| {
                    d.embeddings
                        .iter()
                        .find_map(|e| e.primary_keys.clone())
                        .map(|pks| (TableReference::parse_str(&d.name), pks))
                })
                .collect::<HashMap<TableReference, Vec<_>>>()
        });

    for tbl in tables {
        if let Some(tbl_ref) = df.get_table(tbl.clone()).await {
            // If we can derive the primary key(s) of the table from the [`TableProvider`] constraints, use that.
            if let Some(constraints) = tbl_ref.constraints() {
                if let Some(pks) = constraints.iter().find_map(|c| match c {
                    Constraint::PrimaryKey(columns) => Some(columns),
                    Constraint::Unique(_) => None,
                }) {
                    let schema_projection = tbl_ref.schema().project(pks)?;
                    tbl_to_pks.insert(
                        tbl.clone(),
                        schema_projection
                            .fields()
                            .iter()
                            .map(|f| f.name().clone())
                            .collect::<Vec<_>>(),
                    );
                }

            // Otherwise, if we have explicit primary keys defined in the spicepod configuration, use that.
            } else if let Some(pks) = explicit_primary_keys.get(&tbl) {
                tbl_to_pks.insert(tbl.clone(), pks.clone());
            }
        }
    }
    Ok(tbl_to_pks)
}

async fn context_aware_chat(
    model: &RwLock<Box<dyn Chat>>,
    vector_search_data: &VectorSearchResponse,
    user_input: String,
) -> Response {
    // Using returned data, create input for LLM.
    let model_input =
        combined_relevant_data_and_input(&vector_search_data.retrieved_entries, &user_input);

    match model.write().await.run(model_input).await {
        Ok(Some(assist)) => {
            match create_assist_response(assist, &vector_search_data.retrieved_public_keys) {
                Ok(assist_response) => (StatusCode::OK, Json(assist_response)).into_response(),
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
    Extension(app): Extension<Arc<RwLock<Option<App>>>>,
    Extension(df): Extension<Arc<DataFusion>>,
    Extension(embeddings): Extension<Arc<RwLock<EmbeddingModelStore>>>,
    Extension(llms): Extension<Arc<RwLock<LLMModelStore>>>,
    Json(payload): Json<Request>,
) -> Response {
    // For now, force the user to specify which data.
    if payload.data_source.is_empty() {
        return (StatusCode::BAD_REQUEST, "No data sources provided").into_response();
    }

    let relevant_data =  match prepare_and_run_vector_search(
        Arc::clone(&app),
        Arc::clone(&df),
        Arc::clone(&embeddings),
        payload.clone(),
    ).await {
        Ok(relevant_data) => relevant_data,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    };

    tracing::debug!(
        "Relevant data from vector search: {:#?}",
        relevant_data.retrieved_entries
    );

    // Run LLM with input.
    match llms.read().await.get(&payload.model) {
        Some(llm_model) => context_aware_chat(&llm_model, &relevant_data, payload.text.clone()).await,
        None => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Model {} not found", payload.model),
        )
            .into_response(),
    }
}


pub async fn sse_post(
    Extension(app): Extension<Arc<RwLock<Option<App>>>>,
    Extension(df): Extension<Arc<DataFusion>>,
    Extension(embeddings): Extension<Arc<RwLock<EmbeddingModelStore>>>,
    Extension(llms): Extension<Arc<RwLock<LLMModelStore>>>,
    Json(payload): Json<Request>,
) -> Sse<impl Stream<Item = Result<Event, axum::Error>>> {
    // For now, force the user to specify which data.
    // if payload.data_source.is_empty() {
    //     return Sse::new(stream! {
    //         yield Err(axum::Error::new(Box::new(
    //             std::io::Error::new(std::io::ErrorKind::InvalidInput, "No data sources provided")
    //         ) as Box<dyn Error + Send + Sync>))
    //     }).keep_alive(KeepAlive::default());
    // }

    let relevant_data =  match prepare_and_run_vector_search(
        Arc::clone(&app),
        Arc::clone(&df),
        Arc::clone(&embeddings),
        payload.clone(),
    ).await {
        Ok(relevant_data) => relevant_data,
        Err(e) => {
            println!("Error from prepare_and_run_vector_search: {e}");
            unimplemented!("Help")} // return Sse::new(stream! {}).keep_alive(KeepAlive::default())}
        //     yield Err(axum::Error::new(Box::new(
        //         std::io::Error::new(std::io::ErrorKind::InvalidInput, format!("Model {e} not found"))
        //     ) as Box<dyn Error + Send + Sync>))
        // }).keep_alive(KeepAlive::default());}
    };

    tracing::debug!(
        "Relevant data from vector search: {:#?}",
        relevant_data.retrieved_entries
    );

    let model_input =combined_relevant_data_and_input(&relevant_data.retrieved_entries, &payload.text.clone());
    match llms.read().await.get(&payload.model.clone()) {
        Some(llm_model) => {
            let mut model_stream = llm_model.write().await.stream(model_input).await;
            let vector_data = match create_assist_response_from(&relevant_data.retrieved_public_keys) {
                Ok(vector_data) => vector_data,
                Err(e) => {unimplemented!("Help")} // return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
            };
        
            Sse::new(Box::pin(stream!{
                yield Event::default().json_data(vector_data);
                while let Some(msg) = model_stream.next().await {
                    yield Ok(Event::default().data(msg.unwrap().unwrap()));
                }
            })).keep_alive(KeepAlive::default())
        },
        None => {unimplemented!("Help")}
        // Sse::new(stream!{yield Event::default().data(
        //     format!("Model {} not found", payload.model)
        // )}).keep_alive(KeepAlive::default())
    }
}
