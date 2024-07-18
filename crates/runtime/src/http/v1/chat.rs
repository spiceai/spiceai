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

use core::time;
use std::{collections::HashMap, sync::Arc, time::Duration};

use arrow::{array::RecordBatch, util::pretty::pretty_format_batches};
use async_openai::types::{
    ChatChoiceStream, ChatCompletionRequestSystemMessageArgs, ChatCompletionResponseStream,
    ChatCompletionStreamResponseDelta, ChatCompletionToolArgs, ChatCompletionToolType,
    CreateChatCompletionRequest, CreateChatCompletionStreamResponse, FunctionObjectArgs, Role,
};
use async_stream::stream;
use axum::{
    http::StatusCode,
    response::{
        sse::{Event, KeepAlive, Sse},
        IntoResponse, Response,
    },
    Extension, Json,
};
use datafusion_table_providers::sql::arrow_sql_gen::statement::CreateTableBuilder;
use futures::StreamExt;
use serde_json::json;
use tokio::sync::RwLock;

use crate::datafusion::DataFusion;
use crate::model::LLMModelStore;

pub(crate) async fn post(
    Extension(llms): Extension<Arc<RwLock<LLMModelStore>>>,
    Extension(df): Extension<Arc<DataFusion>>,
    Json(req): Json<CreateChatCompletionRequest>,
) -> Response {
    let model_id = req.model.clone();
    let mut req = req.clone();

    if let Ok(system_message) = ChatCompletionRequestSystemMessageArgs::default()
        .content(
            r"You are an AI assistant built on top of the Spice runtime, which provides a unified SQL interface to materialize, accelerate, and query data from any database, data warehouse, or data lake. Your primary functionalities include:
1.	Listing Registered Datasets: Provide a list of all datasets currently registered in the Spice runtime and describe their schemas.
2.	Generating SQL Queries: Generate SQL queries based on specific data requests.
3.	Querying Data: Execute specified SQL queries on the registered datasets and return the results.
4.	Querying and Summarizing Results: Execute queries and provide summaries of the results for quick insights.

Use these capabilities to assist users in efficiently managing and querying their data.
",
        )
        .build()
    {
        req.messages.insert(0, system_message.into());
    }

    let mut tools = vec![];

    let spice_datasets_function = FunctionObjectArgs::default()
        .name("spice_datasets")
        .description("Get registered Spice runtime datasets")
        .parameters(json!({
            "type": "object",
            "properties": {},
        }))
        .build();

    if let Ok(spice_datasets_function) = spice_datasets_function {
        if let Ok(tool) = ChatCompletionToolArgs::default()
            .r#type(ChatCompletionToolType::Function)
            .function(spice_datasets_function)
            .build()
        {
            tools.push(tool);
        }
    }

    let spice_dataset_refresh_function = FunctionObjectArgs::default()
        .name("spice_dataset_refresh")
        .description("Refresh Spice dataset")
        .parameters(json!({
            "type": "object",
            "properties": {
                "dataset": {
                    "type": "string",
                    "description": "Name of the dataset",
                },
            },
            "required": ["dataset"],
        }))
        .build();

    if let Ok(spice_dataset_refresh_function) = spice_dataset_refresh_function {
        if let Ok(tool) = ChatCompletionToolArgs::default()
            .r#type(ChatCompletionToolType::Function)
            .function(spice_dataset_refresh_function)
            .build()
        {
            tools.push(tool);
        }
    }

    let get_spice_dataset_schema_function = FunctionObjectArgs::default()
        .name("get_spice_dataset_schema_function")
        .description("Get Spice dataset schema")
        .parameters(json!({
            "type": "object",
            "properties": {
                "dataset": {
                    "type": "string",
                    "description": "Name of the dataset",
                },
            },
            "required": ["dataset"],
        }))
        .build();

    if let Ok(get_spice_dataset_schema_function) = get_spice_dataset_schema_function {
        if let Ok(tool) = ChatCompletionToolArgs::default()
            .r#type(ChatCompletionToolType::Function)
            .function(get_spice_dataset_schema_function)
            .build()
        {
            tools.push(tool);
        }
    }

    let query_spice_dataset_function = FunctionObjectArgs::default()
        .name("query_spice_dataset_function")
        .description("Execute SQL query on Spice dataset")
        .parameters(json!({
            "type": "object",
            "properties": {
                "dataset": {
                    "type": "string",
                    "description": "Name of the dataset",
                },
                "query": {
                    "type": "string",
                    "description": "SQL query to execute",
                },
            },
            "required": ["dataset", "query"],
        }))
        .build();

    if let Ok(query_spice_dataset_function) = query_spice_dataset_function {
        if let Ok(tool) = ChatCompletionToolArgs::default()
            .r#type(ChatCompletionToolType::Function)
            .function(query_spice_dataset_function)
            .build()
        {
            tools.push(tool);
        }
    }

    req.tools = Some(tools);

    match llms.read().await.get(&model_id) {
        Some(model) => {
            if req.stream.unwrap_or_default() {
                match model.write().await.chat_stream(req).await {
                    Ok(strm) => create_sse_response(strm, df, time::Duration::from_secs(30)),
                    Err(e) => {
                        tracing::debug!("Error from v1/chat: {e}");
                        StatusCode::INTERNAL_SERVER_ERROR.into_response()
                    }
                }
            } else {
                match model.write().await.chat_request(req).await {
                    Ok(response) => Json(response).into_response(),
                    Err(e) => {
                        tracing::debug!("Error from v1/chat: {e}");
                        StatusCode::INTERNAL_SERVER_ERROR.into_response()
                    }
                }
            }
        }
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

/// Create a SSE [`axum::response::Response`] from a [`ChatCompletionResponseStream`].
#[allow(deprecated)]
fn create_sse_response(
    mut strm: ChatCompletionResponseStream,
    df: Arc<DataFusion>,
    keep_alive_interval: Duration,
) -> Response {
    let mut tool_call_buffer = String::new();
    let mut tool_name = None;

    Sse::new(Box::pin(stream! {
        while let Some(msg) = strm.next().await {
            match msg {
                Ok(resp) => {
                    if !resp.choices.is_empty() {
                        let choice = &resp.choices[0];
                        if let Some(tool_calls) = &choice.delta.tool_calls {
                            for tool_call in tool_calls {
                                if let Some(function) = &tool_call.function {
                                    if let Some(name) = &function.name {
                                        tool_name = Some(name.clone());
                                    }
                                    if let Some(arguments) = &function.arguments {
                                        tool_call_buffer.push_str(arguments);
                                    }
                                }
                            }
                        } else {
                            if !tool_call_buffer.is_empty() && tool_name.is_some() {
                                if tool_name == Some("spice_datasets".to_string()) {
                                    let tables = match df.get_public_table_names() {
                                        Ok(tables) => tables,
                                        Err(_) => vec![]
                                    };

                                    let cloned_resp = resp.clone();
                                    let tool_resp = CreateChatCompletionStreamResponse {
                                        id: cloned_resp.id,
                                        model: cloned_resp.model,
                                        created: cloned_resp.created,
                                        system_fingerprint: None,
                                        object: cloned_resp.object,
                                        usage: None,
                                        choices: vec![
                                            ChatChoiceStream {
                                                delta: ChatCompletionStreamResponseDelta {
                                                    content: Some(format!("Registered datasets: {}", tables.join(", "))),
                                                    tool_calls: None,
                                                    role: Some(Role::Assistant),
                                                    function_call: None,
                                                },
                                                index: 0,
                                                finish_reason: None,
                                                logprobs: None,
                                            }
                                        ],
                                    };

                                    let y = Event::default();
                                    match y.json_data(tool_resp).map_err(axum::Error::new) {
                                        Ok(a) => yield Ok(a),
                                        Err(e) => yield Err(e),
                                    }
                                } else if tool_name == Some("spice_dataset_refresh".to_string()) {
                                    tracing::info!("Tool call buffer: {}", tool_call_buffer);

                                    let dataset = match serde_json::from_str::<HashMap<String, String>>(&tool_call_buffer) {
                                        Ok(map) => map.get("dataset").map(|s| s.to_string()),
                                        Err(_) => None
                                    };

                                    if let Some(dataset) = dataset {
                                        if let Ok(_) = df.refresh_table(dataset.as_str()).await {

                                        }
                                    }

                                    let cloned_resp = resp.clone();
                                    let tool_resp = CreateChatCompletionStreamResponse {
                                        id: cloned_resp.id,
                                        model: cloned_resp.model,
                                        created: cloned_resp.created,
                                        system_fingerprint: None,
                                        object: cloned_resp.object,
                                        usage: None,
                                        choices: vec![
                                            ChatChoiceStream {
                                                delta: ChatCompletionStreamResponseDelta {
                                                    content: Some(format!("Dataset refresh triggered")),
                                                    tool_calls: None,
                                                    role: Some(Role::Assistant),
                                                    function_call: None,
                                                },
                                                index: 0,
                                                finish_reason: None,
                                                logprobs: None,
                                            }
                                        ],
                                    };

                                    let y = Event::default();
                                    match y.json_data(tool_resp).map_err(axum::Error::new) {
                                        Ok(a) => yield Ok(a),
                                        Err(e) => yield Err(e),
                                    }
                                } else if tool_name == Some("get_spice_dataset_schema_function".to_string()) {
                                    let dataset = match serde_json::from_str::<HashMap<String, String>>(&tool_call_buffer) {
                                        Ok(map) => map.get("dataset").map(|s| s.to_string()),
                                        Err(_) => None
                                    };

                                    if let Some(dataset) = dataset {
                                        if let Ok(schema) = df.get_arrow_schema(dataset.as_str()).await {
                                            let statement = CreateTableBuilder::new(Arc::new(schema), format!("public.{dataset}").as_str()).build_sqlite();

                                            let cloned_resp = resp.clone();
                                            let tool_resp = CreateChatCompletionStreamResponse {
                                                id: cloned_resp.id,
                                                model: cloned_resp.model,
                                                created: cloned_resp.created,
                                                system_fingerprint: None,
                                                object: cloned_resp.object,
                                                usage: None,
                                                choices: vec![
                                                    ChatChoiceStream {
                                                        delta: ChatCompletionStreamResponseDelta {
                                                            content: Some(statement),
                                                            tool_calls: None,
                                                            role: Some(Role::Assistant),
                                                            function_call: None,
                                                        },
                                                        index: 0,
                                                        finish_reason: None,
                                                        logprobs: None,
                                                    }
                                                ],
                                            };

                                            let y = Event::default();
                                            match y.json_data(tool_resp).map_err(axum::Error::new) {
                                                Ok(a) => yield Ok(a),
                                                Err(e) => yield Err(e),
                                            }
                                        }
                                    }
                                } else if tool_name == Some("query_spice_dataset_function".to_string()) {
                                    let query = match serde_json::from_str::<HashMap<String, String>>(&tool_call_buffer) {
                                        Ok(map) => map.get("query").map(|s| s.to_string()),
                                        Err(_) => None
                                    };

                                    if let Some(query) = query {
                                        let result = df.query_builder(query, crate::datafusion::query::Protocol::Flight).build().run().await;

                                        match result {
                                            Ok(mut result) => {
                                                let mut results: Vec<RecordBatch> = vec![];
                                                while let Some(batch) = result.data.next().await {
                                                    if let Ok(batch) = batch {
                                                        results.push(batch);
                                                    }
                                                }

                                                if let Ok(table) = pretty_format_batches(&results) {
                                                    let cloned_resp = resp.clone();
                                                    let tool_resp = CreateChatCompletionStreamResponse {
                                                        id: cloned_resp.id,
                                                        model: cloned_resp.model,
                                                        created: cloned_resp.created,
                                                        system_fingerprint: None,
                                                        object: cloned_resp.object,
                                                        usage: None,
                                                        choices: vec![
                                                            ChatChoiceStream {
                                                                delta: ChatCompletionStreamResponseDelta {
                                                                    content: Some(table.to_string()),
                                                                    tool_calls: None,
                                                                    role: Some(Role::Assistant),
                                                                    function_call: None,
                                                                },
                                                                index: 0,
                                                                finish_reason: None,
                                                                logprobs: None,
                                                            }
                                                        ],
                                                    };

                                                    let y = Event::default();
                                                    match y.json_data(tool_resp).map_err(axum::Error::new) {
                                                        Ok(a) => yield Ok(a),
                                                        Err(e) => yield Err(e),
                                                    }
                                                }

                                            },
                                            _ => {}
                                        }

                                        // let query = Query

                                        // if let Ok(df) = df

                                        // .query_table(dataset.as_str(), query.as_str()).await {
                                        // let cloned_resp = resp.clone();
                                        // let tool_resp = CreateChatCompletionStreamResponse {
                                        //     id: cloned_resp.id,
                                        //     model: cloned_resp.model,
                                        //     created: cloned_resp.created,
                                        //     system_fingerprint: None,
                                        //     object: cloned_resp.object,
                                        //     usage: None,
                                        //     choices: vec![
                                        //         ChatChoiceStream {
                                        //             delta: ChatCompletionStreamResponseDelta {
                                        //                 content: Some(df.to_json().to_string()),
                                        //                 tool_calls: None,
                                        //                 role: Some(Role::Assistant),
                                        //                 function_call: None,
                                        //             },
                                        //             index: 0,
                                        //             finish_reason: None,
                                        //             logprobs: None,
                                        //         }
                                        //     ],
                                        // };

                                        // let y = Event::default();
                                        // match y.json_data(tool_resp).map_err(axum::Error::new) {
                                        //     Ok(a) => yield Ok(a),
                                        //     Err(e) => yield Err(e),
                                        // }
                                    }

                                } else {
                                }
                                tool_call_buffer.clear();
                                tool_name = None;
                            }
                        }
                    }

                    let y = Event::default();
                    match y.json_data(resp).map_err(axum::Error::new) {
                        Ok(a) => yield Ok(a),
                        Err(e) => yield Err(e),
                    }
                },
                Err(e) => {
                    yield Err(axum::Error::new(e.to_string()));
                    break;
                }
            }
        };
    }))
    .keep_alive(KeepAlive::new().interval(keep_alive_interval))
    .into_response()
}
