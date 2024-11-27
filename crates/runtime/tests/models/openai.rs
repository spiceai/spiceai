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

#![allow(clippy::expect_used)]
use super::send_embeddings_request;
use crate::models::sql_to_display;
use crate::{
    init_tracing, init_tracing_with_task_history,
    models::{
        create_api_bindings_config, get_params_with_secrets, get_taxi_trips_dataset,
        get_tpcds_dataset, http_post, normalize_chat_completion_response,
        normalize_embeddings_response, normalize_search_response, send_chat_completions_request,
    },
    utils::{runtime_ready_check, test_request_context, verify_env_secret_exists},
};
use app::AppBuilder;
use async_openai::types::{
    ChatCompletionRequestSystemMessageArgs, ChatCompletionRequestUserMessageArgs,
    CreateChatCompletionRequestArgs, EmbeddingInput,
};
use chrono::{DateTime, Utc};
use jsonpath_rust::JsonPath;
use llms::chat::Chat;
use opentelemetry_sdk::trace::TracerProvider;
use reqwest::header::{HeaderMap, HeaderValue, ACCEPT, CONTENT_TYPE};
use runtime::{auth::EndpointAuth, model::try_to_chat_model, Runtime};
use serde_json::json;
use spicepod::component::{
    embeddings::{ColumnEmbeddingConfig, Embeddings},
    model::Model,
};
use std::str::FromStr;
use std::sync::Arc;

#[allow(clippy::expect_used)]
mod nsql {

    use super::*;

    struct TestCase {
        name: &'static str,
        body: serde_json::Value,
    }

    async fn run_nsql_test(
        base_url: &str,
        ts: &TestCase,
        trace_provider: &TracerProvider,
    ) -> Result<(), anyhow::Error> {
        tracing::info!("Running test cases {}", ts.name);
        let task_start_time = std::time::SystemTime::now();

        // Call /v1/nsql, check response
        let mut headers = HeaderMap::new();
        headers.insert(ACCEPT, HeaderValue::from_static("application/json"));
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        let response = http_post(
            format!("{base_url}/v1/nsql").as_str(),
            &ts.body.to_string(),
            headers,
        )
        .await
        .map_err(|e| anyhow::anyhow!("Failed to execute HTTP POST: {}", e))?;
        insta::assert_snapshot!(format!("{}_response", ts.name), &response);

        // ensure all spans are exported into task_history
        let _ = trace_provider.force_flush();

        // Check task_history table for expected rows.
        let mut headers = HeaderMap::new();
        headers.insert(ACCEPT, HeaderValue::from_static("text/plain"));
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        println!(
            r#"SELECT task, input
                    FROM runtime.task_history
                    WHERE task NOT IN ('ai_completion', 'health', 'accelerated_refresh')
                    AND start_time > '{}'
                    ORDER BY start_time, task;
                "#,
            Into::<DateTime<Utc>>::into(task_start_time).to_rfc3339()
        );
        insta::assert_snapshot!(
            format!("{}_tasks", ts.name),
            http_post(
                format!("{base_url}/v1/sql").as_str(),
                format!(
                    r#"SELECT task, input
                            FROM runtime.task_history
                            WHERE task NOT IN ('ai_completion', 'health', 'accelerated_refresh')
                            AND start_time > '{}'
                            ORDER BY start_time, task;
                        "#,
                    Into::<DateTime<Utc>>::into(task_start_time).to_rfc3339()
                )
                .as_str(),
                headers
            )
            .await
            .expect("Failed to execute HTTP SQL query")
        );

        Ok(())
    }

    #[tokio::test]
    async fn openai_test_nsql() -> Result<(), anyhow::Error> {
        let _tracing = init_tracing(None);

        test_request_context().scope(async {
            verify_env_secret_exists("SPICE_OPENAI_API_KEY")
                .await
                .map_err(anyhow::Error::msg)?;

            let app = AppBuilder::new("text-to-sql")
                .with_dataset(get_taxi_trips_dataset())
                .with_model(get_openai_model("gpt-4o-mini", "nql"))
                .with_model(get_openai_model("gpt-4o-mini", "nql-2"))
                .build();

            let api_config = create_api_bindings_config();
            let http_base_url = format!("http://{}", api_config.http_bind_address);

            let rt = Arc::new(Runtime::builder().with_app(app).build().await);

            let (_tracing, trace_provider) = init_tracing_with_task_history(None, &rt);

            let rt_ref_copy = Arc::clone(&rt);
            tokio::spawn(async move {
                Box::pin(rt_ref_copy.start_servers(api_config, None, EndpointAuth::no_auth())).await
            });

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for components to load"));
                }
                () = rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

            let test_cases = [
                TestCase {
                    name: "basic",
                    body: json!({
                        "query": "how many records (as 'total_records') are in taxi_trips dataset?",
                        "sample_data_enabled": false,
                    }),
                },
                TestCase {
                    name: "with_model",
                    body: json!({
                        "query": "how many records (as 'total_records') are in taxi_trips dataset?",
                        "model": "nql-2",
                        "sample_data_enabled": false,
                    }),
                },
                TestCase {
                    name: "with_sample_data_enabled",
                    body: json!({
                        "query": "how many records (as 'total_records') are in taxi_trips dataset?",
                        "model": "nql",
                        "sample_data_enabled": true,
                    }),
                },
            ];

            for ts in test_cases {
                run_nsql_test(http_base_url.as_str(), &ts, &trace_provider).await?;
            }

            Ok(())
        }).await
    }
}

#[allow(clippy::expect_used)]
mod search {
    use super::*;
    struct TestCase {
        name: &'static str,
        body: serde_json::Value,
    }

    async fn run_search_test(base_url: &str, ts: &TestCase) -> Result<(), anyhow::Error> {
        tracing::info!("Running test cases {}", ts.name);

        // Call /v1/search, check response
        let mut headers = HeaderMap::new();
        headers.insert(ACCEPT, HeaderValue::from_static("application/json"));
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        let response_str = http_post(
            &format!("{base_url}/v1/search").to_string(),
            &ts.body.to_string(),
            headers,
        )
        .await
        .map_err(|e| anyhow::anyhow!("Failed to execute HTTP POST: {}", e))?;

        let response = serde_json::from_str(&response_str)
            .map_err(|e| anyhow::anyhow!("Failed to parse HTTP response: {}", e))?;

        insta::assert_snapshot!(
            format!("{}_response", ts.name),
            normalize_search_response(response)
        );
        Ok(())
    }

    #[tokio::test]
    async fn openai_test_search() -> Result<(), anyhow::Error> {
        let _tracing = init_tracing(None);

        test_request_context()
            .scope(async {
                verify_env_secret_exists("SPICE_OPENAI_API_KEY")
                    .await
                    .map_err(anyhow::Error::msg)?;

                let mut ds_tpcds_item = get_tpcds_dataset("item");
                ds_tpcds_item.embeddings = vec![ColumnEmbeddingConfig {
                    column: "i_item_desc".to_string(),
                    model: "openai_embeddings".to_string(),
                    primary_keys: Some(vec!["i_item_sk".to_string()]),
                    chunking: None,
                }];

                let app = AppBuilder::new("search_app")
                    // taxi_trips dataset is used to test search when there is a dataset w/o embeddings
                    .with_dataset(get_taxi_trips_dataset())
                    .with_dataset(ds_tpcds_item)
                    .with_embedding(get_openai_embeddings(
                        Option::<String>::None,
                        "openai_embeddings",
                    ))
                    .build();

                let api_config = create_api_bindings_config();
                let http_base_url = format!("http://{}", api_config.http_bind_address);
                let rt = Arc::new(Runtime::builder().with_app(app).build().await);

                let _ = init_tracing_with_task_history(None, &rt);

                let rt_ref_copy = Arc::clone(&rt);
                tokio::spawn(async move {
                    Box::pin(rt_ref_copy.start_servers(api_config, None, EndpointAuth::no_auth()))
                        .await
                });

                tokio::select! {
                    () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                        return Err(anyhow::anyhow!("Timed out waiting for components to load"));
                    }
                    () = rt.load_components() => {}
                }

                runtime_ready_check(&rt).await;

                let test_cases = [
                    TestCase {
                        name: "basic",
                        body: json!({
                            "text": "new patient",
                            "limit": 2,
                            "datasets": ["item"],
                            "additional_columns": ["i_color", "i_item_id"],
                        }),
                    },
                    TestCase {
                        name: "all_datasets",
                        body: json!({
                            "text": "new patient",
                            "limit": 2,
                        }),
                    },
                ];

                for ts in test_cases {
                    run_search_test(http_base_url.as_str(), &ts).await?;
                }
                Ok(())
            })
            .await
    }
}

#[allow(clippy::expect_used)]
mod embeddings {
    use super::*;
    struct EmbeddingTestCase {
        pub input: EmbeddingInput,
        pub encoding_format: Option<&'static str>,
        pub user: Option<&'static str>,
        pub dimensions: Option<u32>,
        pub test_id: &'static str,
    }

    #[tokio::test]
    async fn openai_test_embeddings() -> Result<(), anyhow::Error> {
        let _tracing = init_tracing(None);

        test_request_context()
            .scope(async {
                verify_env_secret_exists("SPICE_OPENAI_API_KEY")
                    .await
                    .map_err(anyhow::Error::msg)?;

                let app = AppBuilder::new("search_app")
                    .with_embedding(get_openai_embeddings(
                        Some("text-embedding-3-small"),
                        "openai_embeddings",
                    ))
                    .build();

                let api_config = create_api_bindings_config();
                let http_base_url = format!("http://{}", api_config.http_bind_address);
                let rt = Arc::new(Runtime::builder().with_app(app).build().await);

                let rt_ref_copy = Arc::clone(&rt);
                tokio::spawn(async move {
                    Box::pin(rt_ref_copy.start_servers(api_config, None, EndpointAuth::no_auth()))
                        .await
                });

                tokio::select! {
                    () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                        return Err(anyhow::anyhow!("Timed out waiting for components to load"));
                    }
                    () = rt.load_components() => {}
                }

                runtime_ready_check(&rt).await;

                let embeddins_test = vec![
                    EmbeddingTestCase {
                        input: EmbeddingInput::String(
                            "The food was delicious and the waiter...".to_string(),
                        ),
                        encoding_format: Some("float"),
                        user: None,
                        dimensions: None,
                        test_id: "basic",
                    },
                    EmbeddingTestCase {
                        input: EmbeddingInput::StringArray(vec![
                            "The food was delicious".to_string(),
                            "and the waiter...".to_string(),
                        ]),
                        encoding_format: None,
                        user: Some("test_user_id"),
                        dimensions: Some(256),
                        test_id: "multiple_inputs",
                    },
                    EmbeddingTestCase {
                        input: EmbeddingInput::StringArray(vec![
                            "The food was delicious".to_string(),
                            "and the waiter...".to_string(),
                        ]),
                        encoding_format: Some("base64"),
                        user: Some("test_user_id"),
                        dimensions: Some(128),
                        test_id: "base64_format",
                    },
                ];

                for EmbeddingTestCase {
                    input,
                    encoding_format,
                    user,
                    dimensions,
                    test_id,
                } in embeddins_test
                {
                    let response = send_embeddings_request(
                        http_base_url.as_str(),
                        "openai_embeddings",
                        input,
                        encoding_format,
                        user,
                        dimensions,
                    )
                    .await?;

                    insta::assert_snapshot!(
                        test_id,
                        // OpenAI's embeddings response is not deterministic (values vary for the same input, model version, and parameters) so
                        // we normalize the response before snapshotting
                        normalize_embeddings_response(response)
                    );
                }

                Ok(())
            })
            .await
    }
}

#[tokio::test]
async fn openai_test_chat_completion() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(None);

    test_request_context().scope(async {
        verify_env_secret_exists("SPICE_OPENAI_API_KEY")
            .await
            .map_err(anyhow::Error::msg)?;

        let mut model_with_tools = get_openai_model("gpt-4o-mini", "openai_model");
        model_with_tools
            .params
            .insert("tools".to_string(), "auto".into());

        let app = AppBuilder::new("text-to-sql")
            .with_dataset(get_taxi_trips_dataset())
            .with_model(model_with_tools)
            .build();

        let api_config = create_api_bindings_config();
        let http_base_url = format!("http://{}", api_config.http_bind_address);
        let rt = Arc::new(Runtime::builder().with_app(app).build().await);

        let rt_ref_copy = Arc::clone(&rt);
        tokio::spawn(async move {
            Box::pin(rt_ref_copy.start_servers(api_config, None, EndpointAuth::no_auth())).await
        });

        tokio::select! {
            () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                return Err(anyhow::anyhow!("Timed out waiting for components to load"));
            }
            () = rt.load_components() => {}
        }

        runtime_ready_check(&rt).await;

        let response = send_chat_completions_request(
            http_base_url.as_str(),
            vec![
                ("system".to_string(), "You are an assistant that responds to queries by providing only the requested data values without extra explanation.".to_string()),
                ("user".to_string(), "Provide the total number of records in the taxi trips dataset. If known, return a single numeric value.".to_string()),
            ],
            "openai_model",
            false,
        ).await?;

        insta::assert_snapshot!(
            "chat_completion",
            normalize_chat_completion_response(response, false)
        );

        Ok(())
    }).await
}

#[tokio::test]
async fn openai_test_chat_messages() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(None);

    test_request_context()
        .scope(async {
            verify_env_secret_exists("SPICE_OPENAI_API_KEY")
                .await
                .map_err(anyhow::Error::msg)?;

            let mut ds_tpcds_item = get_tpcds_dataset("item");
            ds_tpcds_item.embeddings = vec![ColumnEmbeddingConfig {
                column: "i_item_desc".to_string(),
                model: "openai_embeddings".to_string(),
                primary_keys: Some(vec!["i_item_sk".to_string()]),
                chunking: None,
            }];

            let app = AppBuilder::new("text-to-sql")
                .with_dataset(get_taxi_trips_dataset())
                .with_dataset(ds_tpcds_item)
                .with_embedding(get_openai_embeddings(
                    Some("text-embedding-3-small"),
                    "openai_embeddings",
                ))
                .build();

            let rt = Arc::new(Runtime::builder().with_app(app).build().await);

            let (_tracing, trace_provider) = init_tracing_with_task_history(None, &rt);

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for components to load"));
                }
                () = rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

            verify_sql_query_chat_completion(Arc::clone(&rt), &trace_provider).await?;
            verify_similarity_search_chat_completion(Arc::clone(&rt), &trace_provider).await?;

            Ok(())
        })
        .await
}

/// Verifies that the model correctly uses the SQL tool to process user query and return the result
async fn verify_sql_query_chat_completion(
    rt: Arc<Runtime>,
    trace_provider: &TracerProvider,
) -> Result<(), anyhow::Error> {
    let model =
        get_openai_chat_model(Arc::clone(&rt), "gpt-4o-mini", "openai_model", "auto").await?;
    let req = CreateChatCompletionRequestArgs::default()
            .messages(vec![ChatCompletionRequestSystemMessageArgs::default()
                .content("You are an assistant that responds to queries by providing only the requested data values without extra explanation.".to_string())
                .build()?
                .into(),ChatCompletionRequestUserMessageArgs::default()
                .content("Provide the total number of records in the taxi trips dataset. If known, return a single numeric value.".to_string())
                .build()?
                .into()])
            .build()?;

    let task_start_time = std::time::SystemTime::now();
    let response = model.chat_request(req).await?;

    insta::assert_snapshot!(
        "chat_1_response_choices",
        format!("{:#?}", response.choices)
    );

    let _ = trace_provider.force_flush();

    // Verify Task History
    insta::assert_snapshot!(
        "chat_1_sql_tasks",
        sql_to_display(
            &rt,
            format!(
                r#"SELECT task, count(1)
                FROM runtime.task_history
                WHERE start_time >= '{}'
                AND task in ('tool_use::list_datasets', 'tool_use::sql', 'tool_use::sql_query')
                GROUP BY task
                ORDER BY task;
            "#,
                Into::<DateTime<Utc>>::into(task_start_time).to_rfc3339()
            )
            .as_str()
        )
        .await
        .expect("Failed to execute HTTP SQL query")
    );

    insta::assert_snapshot!(
        "chat_1_ai_completion_input",
        sql_to_display(
            &rt,
            format!(
                r#"SELECT input
                FROM runtime.task_history
                WHERE start_time >= '{}'
                AND task='ai_completion'
                ORDER BY start_time
                LIMIT 1;
            "#,
                Into::<DateTime<Utc>>::into(task_start_time).to_rfc3339()
            )
            .as_str()
        )
        .await
        .expect("Failed to execute HTTP SQL query")
    );

    Ok(())
}

/// Verifies that the model correctly uses similirity search tool to process user query and return the result
async fn verify_similarity_search_chat_completion(
    rt: Arc<Runtime>,
    trace_provider: &TracerProvider,
) -> Result<(), anyhow::Error> {
    let model =
        get_openai_chat_model(Arc::clone(&rt), "gpt-4o-mini", "openai_model", "auto").await?;

    let req = CreateChatCompletionRequestArgs::default()
        .messages(vec![ChatCompletionRequestSystemMessageArgs::default()
            .content("You are an assistant that responds to queries by providing only the requested data values without extra explanation.".to_string())
            .build()?
            .into(),ChatCompletionRequestUserMessageArgs::default()
            .content("Find information about vehicles and journalists".to_string())
            .build()?
            .into()])
        .build()?;

    let task_start_time = std::time::SystemTime::now();
    let response = model.chat_request(req).await?;

    // Verify Response
    let resp_value =
        serde_json::to_value(&response).expect("Failed to serialize response.choices: {}");
    let selector = JsonPath::from_str(
        "$.choices[*].message[?(@.content~='.*there just big vehicles. Journalists.*')].length()",
    )
    .expect("Failed to create JSONPath selector");

    insta::assert_snapshot!(
        "chat_2_response",
        serde_json::to_string_pretty(&selector.find(&resp_value))
            .expect("Failed to serialize response.choices")
    );

    // ensure all spans are exported into task_history
    let _ = trace_provider.force_flush();

    // Verify Task History
    insta::assert_snapshot!(
        "chat_2_document_similarity_tasks",
        sql_to_display(
            &rt,
            format!(
                r#"SELECT input
                FROM runtime.task_history
                WHERE start_time >= '{}' and task='tool_use::document_similarity';
            "#,
                Into::<DateTime<Utc>>::into(task_start_time).to_rfc3339()
            )
            .as_str()
        )
        .await
        .expect("Failed to execute HTTP SQL query")
    );

    Ok(())
}

fn get_openai_model(model: impl Into<String>, name: impl Into<String>) -> Model {
    let mut model = Model::new(format!("openai:{}", model.into()), name);
    model.params.insert(
        "openai_api_key".to_string(),
        "${ secrets:SPICE_OPENAI_API_KEY }".into(),
    );
    model
}

async fn get_openai_chat_model(
    rt: Arc<Runtime>,
    model: impl Into<String>,
    name: impl Into<String>,
    tools: impl Into<String>,
) -> Result<Box<dyn Chat>, anyhow::Error> {
    let mut model_with_tools = get_openai_model(model, name);
    model_with_tools
        .params
        .insert("tools".to_string(), tools.into().into());

    let model_secrets = get_params_with_secrets(&model_with_tools.params, &rt).await;
    try_to_chat_model(&model_with_tools, &model_secrets, rt)
        .await
        .map_err(anyhow::Error::from)
}

fn get_openai_embeddings(model: Option<impl Into<String>>, name: impl Into<String>) -> Embeddings {
    let mut embedding = match model {
        Some(model) => Embeddings::new(format!("openai:{}", model.into()), name),
        None => Embeddings::new("openai", name),
    };
    embedding.params.insert(
        "openai_api_key".to_string(),
        "${ secrets:SPICE_OPENAI_API_KEY }".into(),
    );
    embedding
}
