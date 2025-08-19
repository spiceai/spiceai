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
use crate::DEFAULT_TRACING_MODELS;
use crate::models::{sort_json_keys, sql_to_display, sql_to_json_values, sql_to_single_json_value};
use crate::{
    init_tracing,
    models::{
        create_api_bindings_config, get_params_with_secrets_value, get_taxi_trips_dataset,
        get_tpcds_dataset, normalize_chat_completion_response, send_chat_completions_request,
    },
    utils::init_tracing_with_task_history,
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
use runtime::{Runtime, auth::EndpointAuth, model::try_to_chat_model};
use serde_json::json;
use spicepod::component::{embeddings::Embeddings, model::Model};
use spicepod::semantic::{Column, ColumnLevelEmbeddingConfig};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

#[allow(clippy::expect_used)]
mod nsql {

    use crate::models::nsql::{TestCase, run_nsql_test};

    use super::*;

    #[tokio::test]
    async fn openai_test_nsql() -> Result<(), anyhow::Error> {
        let _tracing = init_tracing(None);

        test_request_context().scope_retry(3, || async {
            verify_env_secret_exists("SPICE_OPENAI_API_KEY")
                .await
                .map_err(anyhow::Error::msg)?;

            let mut taxi_trips_with_embeddings = get_taxi_trips_dataset();
            taxi_trips_with_embeddings.columns = vec![Column {
                    name: "store_and_fwd_flag".to_string(),
                    embeddings: vec![ColumnLevelEmbeddingConfig {
                        model: "openai_embeddings".to_string(),
                        row_ids: None,
                        chunking: None,
                        vector_size: None,
                    }],
                    description: None,
                    full_text_search: None,
                    metadata: HashMap::new(),
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

            let (_tracing, trace_provider) = init_tracing_with_task_history(DEFAULT_TRACING_MODELS, &rt);

            let rt_ref_copy = Arc::clone(&rt);
            tokio::spawn(async move {
                Box::pin(rt_ref_copy.start_servers(api_config, None, EndpointAuth::no_auth())).await
            });

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(120)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for components to load"));
                }
                () = Arc::clone(&rt).load_components() => {}
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

    use crate::models::{
        get_mega_science_dataset, get_small_clickbench_dataset,
        search::{SearchTestCase, run_search},
    };

    use super::*;

    #[tokio::test]
    async fn openai_test_search() -> Result<(), anyhow::Error> {
        verify_env_secret_exists("SPICE_OPENAI_API_KEY")
            .await
            .map_err(anyhow::Error::msg)?;
        run_search(
            AppBuilder::new("search_app")
                .with_embedding(get_openai_embeddings(
                    Some("text-embedding-3-small"),
                    "openai_embeddings",
                ))
                .with_dataset(get_mega_science_dataset(
                    Some("qs"),
                    None,
                    Some(Column {
                        name: "answer".to_string(),
                        embeddings: vec![ColumnLevelEmbeddingConfig {
                            model: "openai_embeddings".into(),
                            chunking: None,
                            row_ids: Some(vec!["id".to_string()]),
                            vector_size: None,
                        }],
                        description: None,
                        full_text_search: None,
                        metadata: HashMap::new(),
                    }),
                ))
                .build(),
            vec![
                SearchTestCase {
                    name: "openai_basic",
                    body: json!({
                        "text": "second",
                        "limit": 4,
                        "datasets": ["qs"],
                    }),
                },
                SearchTestCase {
                    name: "openai_additional_columns",
                    body: json!({
                        "text": "second",
                        "limit": 4,
                        "datasets": ["qs"],
                        "additional_columns": ["question"],
                    }),
                },
                SearchTestCase {
                    name: "openai_with_where",
                    body: json!({
                        "text": "secondary",
                        "datasets": ["qs"],
                        "where": "subject!='math'",
                        "limit": 4,
                    }),
                },
            ],
            vec![],
        )
        .await
    }

    #[tokio::test]
    async fn test_search_column_casing() -> Result<(), anyhow::Error> {
        verify_env_secret_exists("SPICE_OPENAI_API_KEY")
            .await
            .map_err(anyhow::Error::msg)?;

        let mut clickbench_dataset_no_chunking =
            get_small_clickbench_dataset("clickbench_no_chunking");
        let mut clickbench_dataset_chunking = get_small_clickbench_dataset("clickbench_chunking");
        clickbench_dataset_no_chunking.columns = vec![Column {
            name: "Referer".to_string(),
            embeddings: vec![ColumnLevelEmbeddingConfig {
                model: "openai_embeddings".to_string(),
                row_ids: None,
                chunking: None,
                vector_size: None,
            }],
            description: None,
            full_text_search: None,
            metadata: HashMap::new(),
        }];

        clickbench_dataset_chunking.columns = vec![Column {
            name: "Referer".to_string(),
            embeddings: vec![ColumnLevelEmbeddingConfig {
                model: "openai_embeddings".to_string(),
                row_ids: None,
                chunking: Some(EmbeddingChunkConfig {
                    enabled: true,
                    target_chunk_size: 512,
                    overlap_size: 128,
                    trim_whitespace: false,
                }),
                vector_size: None,
            }],
            description: None,
            full_text_search: None,
            metadata: HashMap::new(),
        }];

        let app = AppBuilder::new("search_app")
            .with_dataset(clickbench_dataset_no_chunking)
            .with_dataset(clickbench_dataset_chunking)
            .with_embedding(get_openai_embeddings(
                Some("text-embedding-3-small"),
                "openai_embeddings",
            ))
            .build();
        run_search(
            app,
            vec![
                SearchTestCase {
                    name: "openai_casing_no_chunking",
                    body: json!({
                        "text": "go.mail",
                        "limit": 2,
                        "datasets": ["clickbench_no_chunking"],
                    }),
                },
                SearchTestCase {
                    name: "openai_casing_chunking",
                    body: json!({
                        "text": "go.mail",
                        "limit": 2,
                        "datasets": ["clickbench_chunking"],
                    }),
                },
            ],
            vec![],
        )
        .await
    }
}

#[allow(clippy::expect_used)]
mod embeddings {
    use std::time::Duration;

    use crate::models::embedding::{
        EmbeddingTestCase, run_beta_functionality_criteria_test, run_embedding_tests,
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
            () = Arc::clone(&rt).load_components() => {}
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
            ds_tpcds_item.columns = vec![Column {
                name: "i_item_desc".to_string(),
                embeddings: vec![ColumnLevelEmbeddingConfig {
                    model: "openai_embeddings".to_string(),
                    row_ids: Some(vec!["i_item_sk".to_string()]),
                    chunking: None,
                    vector_size: None,
                }],
                description: None,
                full_text_search: None,
                metadata: HashMap::new(),
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

            let (_tracing, trace_provider) =
                init_tracing_with_task_history(DEFAULT_TRACING_MODELS, &rt);

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for components to load"));
                }
                () = Arc::clone(&rt).load_components() => {}
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
    let mut resp_value =
        serde_json::to_value(&response).expect("Failed to serialize response.choices: {}");
    sort_json_keys(&mut resp_value);

    let selector = JsonPath::from_str(r#"$.choices[?(@.finish_reason=="stop")].length()"#)
        .expect("Failed to create JSONPath selector");

    // Verify Response exsitence instead of correctness - Model is not guaranteed to return the same response
    insta::assert_snapshot!(
        "chat_2_response",
        serde_json::to_string_pretty(&selector.find(&resp_value))
            .expect("Failed to serialize response.choices")
    );

    // ensure all spans are exported into task_history
    let _ = trace_provider.force_flush();
    let task_input = sql_to_json_values(
        &rt,
        format!(
            "SELECT input
            FROM runtime.task_history
            WHERE start_time >= '{}' and task='tool_use::document_similarity';",
            Into::<DateTime<Utc>>::into(task_start_time).to_rfc3339()
        )
        .as_str(),
    )
    .await;

    let mut stable_inputs: Vec<serde_json::Value> = Vec::new();

    for mut value in task_input {
        if let serde_json::Value::Object(ref mut map) = value {
            // Remove the unstable "text" and "datasets" field
            map.remove("text");
            map.remove("datasets");

            // Sort the keys for consistent ordering
            sort_json_keys(&mut value);

            // Add the filtered value to our stable array
            stable_inputs.push(value);
        }
    }

    // Sort the array of objects for consistent ordering in snapshots
    stable_inputs.sort_by(|a, b| {
        let a_str = serde_json::to_string(a).unwrap_or_default();
        let b_str = serde_json::to_string(b).unwrap_or_default();
        a_str.cmp(&b_str)
    });

    // Instead of creating a snapshot, verify keywords contain expected values
    let mut found_journalist = false;
    let mut found_vehicle = false;
    let mut actual_keywords = Vec::new();

    for input in &stable_inputs {
        if let Some(keywords) = input.get("keywords").and_then(|k| k.as_array()) {
            actual_keywords.push(keywords.clone());
            for keyword in keywords {
                if let Some(keyword_str) = keyword.as_str() {
                    if keyword_str.contains("journalist") {
                        found_journalist = true;
                    } else if keyword_str.contains("vehicle") {
                        found_vehicle = true;
                    }
                }
            }
        }
    }

    assert!(
        found_journalist,
        "Expected to find 'journalist' in keywords, found {actual_keywords:?}",
    );
    assert!(
        found_vehicle,
        "Expected to find 'vehicle' in keywords, found {actual_keywords:?}",
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
) -> Result<Arc<dyn Chat>, anyhow::Error> {
    let mut model_with_tools = get_openai_model(model, name);
    model_with_tools
        .params
        .insert("tools".to_string(), tools.into().into());

    let model_secrets = get_params_with_secrets_value(&model_with_tools.params, &rt).await;
    try_to_chat_model(&model_with_tools, &model_secrets, rt)
        .await
        .map_err(anyhow::Error::from)
}

pub(crate) fn get_openai_embeddings(
    model: Option<impl Into<String>>,
    name: impl Into<String>,
) -> Embeddings {
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
