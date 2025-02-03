/*
Copyright 2024-2025 The Spice.ai OSS Authors

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
use crate::models::{sort_json_keys, sql_to_display, sql_to_single_json_value};
use crate::{
    init_tracing, init_tracing_with_task_history,
    models::{
        create_api_bindings_config, get_params_with_secrets_value, get_taxi_trips_dataset,
        get_tpcds_dataset, normalize_chat_completion_response, send_chat_completions_request,
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

    use crate::models::nsql::{run_nsql_test, TestCase};

    use super::*;

    #[tokio::test]
    async fn openai_test_nsql() -> Result<(), anyhow::Error> {
        let _tracing = init_tracing(None);

        test_request_context().scope_retry(3, || async {
            verify_env_secret_exists("SPICE_OPENAI_API_KEY")
                .await
                .map_err(anyhow::Error::msg)?;

            let mut taxi_trips_with_embeddings = get_taxi_trips_dataset();
                taxi_trips_with_embeddings.embeddings = vec![ColumnEmbeddingConfig {
                    column: "store_and_fwd_flag".to_string(),
                    model: "openai_embeddings".to_string(),
                    primary_keys: None,
                    chunking: None,
                }];

            let app = AppBuilder::new("text-to-sql")
                .with_dataset(taxi_trips_with_embeddings)
                .with_model(get_openai_model("gpt-4o-mini", "nql"))
                .with_model(get_openai_model("gpt-4o-mini", "nql-2"))
                .with_embedding(get_openai_embeddings(
                    Some("text-embedding-3-small"),
                    "openai_embeddings",
                ))
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
                    name: "openai_basic",
                    body: json!({
                        "query": "how many records (as 'total_records') are in taxi_trips dataset?",
                        "sample_data_enabled": false,
                    }),
                },
                TestCase {
                    name: "openai_with_model",
                    body: json!({
                        "query": "how many records (as 'total_records') are in taxi_trips dataset?",
                        "model": "nql-2",
                        "sample_data_enabled": false,
                    }),
                },
                TestCase {
                    name: "openai_with_sample_data_enabled",
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
        }).await?;
        Ok(())
    }
}

#[allow(clippy::expect_used)]
mod search {
    use spicepod::component::embeddings::EmbeddingChunkConfig;

    use crate::models::{search::run_search_test, search::TestCase};

    use super::*;

    #[tokio::test]
    async fn openai_test_search() -> Result<(), anyhow::Error> {
        let _tracing = init_tracing(None);

        test_request_context()
            .scope(async {
                verify_env_secret_exists("SPICE_OPENAI_API_KEY")
                    .await
                    .map_err(anyhow::Error::msg)?;

                let mut ds_tpcds_item = get_tpcds_dataset("item", None, None);
                ds_tpcds_item.embeddings = vec![ColumnEmbeddingConfig {
                    column: "i_item_desc".to_string(),
                    model: "openai_embeddings".to_string(),
                    primary_keys: Some(vec!["i_item_sk".to_string()]),
                    chunking: None,
                }];

                let mut ds_tpcds_cp_with_chunking =
                    get_tpcds_dataset("catalog_page", Some("catalog_page_with_chunking"), Some("select cp_description, cp_catalog_page_sk from catalog_page_with_chunking limit 20"));
                ds_tpcds_cp_with_chunking.embeddings = vec![ColumnEmbeddingConfig {
                    column: "cp_description".to_string(),
                    model: "openai_embeddings".to_string(),
                    primary_keys: Some(vec!["cp_catalog_page_sk".to_string()]),
                    chunking: Some(EmbeddingChunkConfig {
                        enabled: true,
                        target_chunk_size: 512,
                        overlap_size: 128,
                        trim_whitespace: false,
                    }),
                }];

                let app = AppBuilder::new("search_app")
                    // taxi_trips dataset is used to test search when there is a dataset w/o embeddings
                    .with_dataset(get_taxi_trips_dataset())
                    .with_dataset(ds_tpcds_item)
                    .with_dataset(ds_tpcds_cp_with_chunking)
                    .with_embedding(get_openai_embeddings(
                        Some("text-embedding-3-small"),
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
                        name: "openai_basic",
                        body: json!({
                            "text": "new patient",
                            "limit": 2,
                            "datasets": ["item"],
                            "additional_columns": ["i_color", "i_item_id"],
                        }),
                    },
                    TestCase {
                        name: "openai_all_datasets",
                        body: json!({
                            "text": "new patient",
                            "limit": 2,
                        }),
                    },
                    TestCase {
                        name: "openai_chunking",
                        body: json!({
                            "text": "friends",
                            "datasets": ["catalog_page_with_chunking"],
                            "limit": 1,
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
    use std::time::Duration;

    use crate::models::embedding::{
        run_beta_functionality_criteria_test, run_embedding_tests, EmbeddingTestCase,
    };

    use super::*;

    #[tokio::test]
    async fn openai_embeddings_beta_requirements() -> Result<(), anyhow::Error> {
        let _tracing = init_tracing(None);
        verify_env_secret_exists("SPICE_OPENAI_API_KEY")
            .await
            .map_err(anyhow::Error::msg)?;

        test_request_context()
            .scope(async {
                run_beta_functionality_criteria_test(
                    get_openai_embeddings(Some("text-embedding-3-small"), "openai_embeddings"),
                    Duration::from_secs(30),
                )
                .await
            })
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn openai_test_embeddings() -> Result<(), anyhow::Error> {
        let _tracing = init_tracing(None);
        verify_env_secret_exists("SPICE_OPENAI_API_KEY")
            .await
            .map_err(anyhow::Error::msg)?;

        test_request_context()
            .scope(async {
                run_embedding_tests(
                    vec![get_openai_embeddings(
                        Some("text-embedding-3-small"),
                        "openai_embeddings",
                    )],
                    vec![
                        EmbeddingTestCase {
                            input: EmbeddingInput::String(
                                "The food was delicious and the waiter...".to_string(),
                            ),
                            model_name: "openai_embeddings",
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
                            model_name: "openai_embeddings",
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
                            model_name: "openai_embeddings",
                            encoding_format: Some("base64"),
                            user: Some("test_user_id"),
                            dimensions: Some(128),
                            test_id: "base64_format",
                        },
                    ],
                )
                .await?;
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
        .scope_retry(3, || async {
            verify_env_secret_exists("SPICE_OPENAI_API_KEY")
                .await
                .map_err(anyhow::Error::msg)?;

            let mut ds_tpcds_item = get_tpcds_dataset("item", None, None);
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
#[allow(clippy::expect_used)]
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
                "SELECT task, count(1) > 0 as task_used
                FROM runtime.task_history
                WHERE start_time >= '{}'
                AND task in ('tool_use::list_datasets', 'tool_use::sql', 'tool_use::sql_query')
                GROUP BY task
                ORDER BY task;",
                Into::<DateTime<Utc>>::into(task_start_time).to_rfc3339()
            )
            .as_str()
        )
        .await
        .expect("Failed to execute HTTP SQL query")
    );

    let mut task_input = sql_to_single_json_value(
        &rt,
        format!(
            "SELECT input
        FROM runtime.task_history
        WHERE start_time >= '{}'
        AND task='ai_completion'
        ORDER BY start_time
        LIMIT 1;
    ",
            Into::<DateTime<Utc>>::into(task_start_time).to_rfc3339()
        )
        .as_str(),
    )
    .await;

    sort_json_keys(&mut task_input);

    insta::assert_snapshot!(
        "chat_1_ai_completion_input",
        serde_json::to_string_pretty(&task_input).expect("Failed to serialize task_input")
    );

    Ok(())
}

/// Verifies that the model correctly uses similirity search tool to process user query and return the result
#[allow(clippy::expect_used)]
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
                "SELECT input
                FROM runtime.task_history
                WHERE start_time >= '{}' and task='tool_use::document_similarity';",
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
    model.params.insert("system_prompt".to_string(), r#"
    When writing SQL queries, do not put double quotes around schema-qualified table names. For example:

    Correct: SELECT * FROM schema.table
    Correct: SELECT * FROM database.schema.table
    Incorrect: SELECT * FROM "schema.table"
    Incorrect: SELECT * FROM "database.schema.table"

    Only use double quotes when you need to preserve case sensitivity or when identifiers contain special characters.

    Prefer quoting column names. For example:
    Correct: `SELECT COUNT(*) AS "total_records" FROM "spice"."public"."taxi_trips"`
    Incorrect: `SELECT COUNT(*) AS total_records FROM "spice"."public"."taxi_trips"`
    "#.to_string().into());
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

    let model_secrets = get_params_with_secrets_value(&model_with_tools.params, &rt).await;
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
