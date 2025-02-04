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

use std::sync::Arc;
use std::sync::LazyLock;
use std::time::Duration;

use app::AppBuilder;
use async_openai::types::{
    ChatCompletionRequestSystemMessageArgs, ChatCompletionRequestUserMessageArgs,
    CreateChatCompletionRequestArgs, EmbeddingInput,
};
use llms::chat::create_hf_model;
use runtime::{
    auth::EndpointAuth,
    model::ToolUsingChat,
    tools::{options::SpiceToolsOptions, utils::get_tools},
    Runtime,
};
use spicepod::component::{
    embeddings::{ColumnEmbeddingConfig, Embeddings},
    model::Model,
};

use llms::chat::Chat;

use crate::models::embedding::run_beta_functionality_criteria_test;
use crate::{
    init_tracing, init_tracing_with_task_history,
    models::{
        create_api_bindings_config,
        embedding::{run_embedding_tests, EmbeddingTestCase},
        get_taxi_trips_dataset, get_tpcds_dataset, normalize_chat_completion_response,
        send_chat_completions_request,
    },
    utils::{runtime_ready_check, test_request_context, verify_env_secret_exists},
};

use tokio::sync::Mutex;

// Mistral loads and initializes models sequentially, so Mutex is used to control LLMs initialization.
// This also prevents unpredicted behavior when we are attempting to load the same model multiple times in parallel.
static LOCAL_LLM_INIT_MUTEX: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

const HF_TEST_MODEL: &str = "meta-llama/Llama-3.2-3B-Instruct";
const HF_TEST_MODEL_TYPE: &str = "llama";
const HF_TEST_MODEL_REQUIRES_HF_API_KEY: bool = true;

mod nsql {

    use serde_json::json;

    use crate::{
        models::nsql::{run_nsql_test, TestCase},
        utils::verify_env_secret_exists,
    };

    use super::*;

    #[tokio::test]
    async fn huggingface_test_nsql() -> Result<(), anyhow::Error> {
        let _tracing = init_tracing(None);

        if HF_TEST_MODEL_REQUIRES_HF_API_KEY {
            verify_env_secret_exists("SPICE_HF_TOKEN")
                .await
                .map_err(anyhow::Error::msg)?;
        }

        test_request_context()
            .scope(async {

                let mut taxi_trips_with_embeddings = get_taxi_trips_dataset();
                taxi_trips_with_embeddings.embeddings = vec![ColumnEmbeddingConfig {
                    column: "store_and_fwd_flag".to_string(),
                    model: "hf_minilm".to_string(),
                    primary_keys: None,
                    chunking: None,
                }];

                let app = AppBuilder::new("text-to-sql")
                    .with_dataset(taxi_trips_with_embeddings)
                    .with_embedding(get_huggingface_embeddings(
                        "sentence-transformers/all-MiniLM-L6-v2",
                        "hf_minilm",
                    ))
                    .with_model(get_huggingface_model(
                        HF_TEST_MODEL,
                        HF_TEST_MODEL_TYPE,
                        "hf_model",
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

                let llm_init_lock = LOCAL_LLM_INIT_MUTEX.lock().await;

                tokio::select! {
                    // increased timeout to download and load huggingface model
                    () = tokio::time::sleep(std::time::Duration::from_secs(300)) => {
                        return Err(anyhow::anyhow!("Timed out waiting for components to load"));
                    }
                    () = rt.load_components() => {}
                }

                drop(llm_init_lock);

                runtime_ready_check(&rt).await;

                let test_cases = [
                    TestCase {
                        name: "hf_with_model",
                        body: json!({
                            "query": "how many records (as 'total_records') are in spice.public.taxi_trips dataset?",
                            "model": "hf_model",
                            "sample_data_enabled": false,
                        }),
                    },
                    // HTTP error: 500 Internal Server Error - model pipeline unexpectedly closed
                    // TestCase {
                    //     name: "hf_with_sample_data_enabled",
                    //     body: json!({
                    //         "query": "how many records (as 'total_records') are in taxi_trips dataset?",
                    //         "model": "hf_model",
                    //         "sample_data_enabled": true,
                    //     }),
                    // },
                    TestCase {
                        name: "hf_invalid_model_name",
                        body: json!({
                            "query": "how many records (as 'total_records') are in taxi_trips dataset?",
                            "model": "model_not_in_spice",
                            "sample_data_enabled": false,
                        }),
                    },
                    TestCase {
                        name: "hf_invalid_dataset_name",
                        body: json!({
                            "query": "how many records (as 'total_records') are in taxi_trips dataset?",
                            "model": "hf_model",
                            "datasets": ["dataset_not_in_spice"],
                            "sample_data_enabled": false,
                        }),
                    },
                ];

                for ts in test_cases {
                    run_nsql_test(http_base_url.as_str(), &ts, &trace_provider).await?;
                }

                Ok(())
            })
            .await
    }
}

mod search {
    use serde_json::json;
    use spicepod::component::embeddings::EmbeddingChunkConfig;

    use crate::models::search::{run_search_test, TestCase};

    use super::*;

    #[tokio::test]
    async fn huggingface_test_search() -> Result<(), anyhow::Error> {
        let _tracing = init_tracing(None);

        test_request_context()
            .scope(async {
                let mut ds_tpcds_item = get_tpcds_dataset("item", None, None);
                ds_tpcds_item.embeddings = vec![ColumnEmbeddingConfig {
                    column: "i_item_desc".to_string(),
                    model: "hf_minilm".to_string(),
                    primary_keys: Some(vec!["i_item_sk".to_string()]),
                    chunking: None,
                }];

                let mut ds_tpcds_cp_with_chunking =
                    get_tpcds_dataset("catalog_page", Some("catalog_page_with_chunking"), Some("select cp_description, cp_catalog_page_sk from catalog_page_with_chunking limit 20"));
                ds_tpcds_cp_with_chunking.embeddings = vec![ColumnEmbeddingConfig {
                    column: "cp_description".to_string(),
                    model: "hf_minilm".to_string(),
                    primary_keys: Some(vec!["cp_catalog_page_sk".to_string()]),
                    chunking: Some(EmbeddingChunkConfig {
                        enabled: true,
                        target_chunk_size: 512,
                        overlap_size: 128,
                        trim_whitespace: false,
                    }),
                }];

                let app = AppBuilder::new("text-to-sql")
                    .with_dataset(ds_tpcds_item)
                    .with_dataset(ds_tpcds_cp_with_chunking)
                    .with_embedding(get_huggingface_embeddings(
                        "sentence-transformers/all-MiniLM-L6-v2",
                        "hf_minilm",
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

                let test_cases = [
                    TestCase {
                        name: "hf_basic",
                        body: json!({
                            "text": "new patient",
                            "limit": 2,
                            "datasets": ["item"],
                            "additional_columns": ["i_color", "i_item_id"],
                        }),
                    },
                    TestCase {
                        name: "hf_all_datasets",
                        body: json!({
                            "text": "new patient",
                            "limit": 2,
                        }),
                    },
                    TestCase {
                        name: "hf_chunking",
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

#[tokio::test]
async fn hf_embeddings_beta_requirements() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(None);

    test_request_context()
        .scope(async {
            run_beta_functionality_criteria_test(
                get_huggingface_embeddings("sentence-transformers/all-MiniLM-L6-v2", "hf_minilm"),
                Duration::from_secs(2 * 60),
            )
            .await
        })
        .await?;

    Ok(())
}

#[tokio::test]
async fn huggingface_test_embeddings() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(None);

    test_request_context()
        .scope(async {
            run_embedding_tests(
                vec![
                    get_huggingface_embeddings(
                        "sentence-transformers/all-MiniLM-L6-v2",
                        "hf_minilm",
                    ),
                    get_huggingface_embeddings("intfloat/e5-small-v2", "hf_e5"),
                ],
                vec![
                    EmbeddingTestCase {
                        input: EmbeddingInput::String(
                            "The food was delicious and the waiter...".to_string(),
                        ),
                        model_name: "hf_minilm",
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
                        model_name: "hf_minilm",
                        user: None,
                        dimensions: Some(256),
                        test_id: "mulitple_inputs",
                    },
                    EmbeddingTestCase {
                        input: EmbeddingInput::String(
                            "The food was delicious and the waiter...".to_string(),
                        ),
                        model_name: "hf_e5",
                        encoding_format: None,
                        user: None,
                        dimensions: Some(384),
                        test_id: "basic",
                    },
                ],
            )
            .await
        })
        .await?;

    Ok(())
}

#[tokio::test]
async fn huggingface_test_chat_completion() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(None);

    if HF_TEST_MODEL_REQUIRES_HF_API_KEY {
        verify_env_secret_exists("SPICE_HF_TOKEN")
            .await
            .map_err(anyhow::Error::msg)?;
    }

    test_request_context().scope_retry(3, || async {
        let mut model_with_tools = get_huggingface_model(HF_TEST_MODEL, HF_TEST_MODEL_TYPE, "hf_model");
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

        let llm_init_lock = LOCAL_LLM_INIT_MUTEX.lock().await;

        tokio::select! {
            // increased timeout to download and load huggingface model
            () = tokio::time::sleep(std::time::Duration::from_secs(300)) => {
                return Err(anyhow::anyhow!("Timed out waiting for components to load"));
            }
            () = rt.load_components() => {}
        }

        drop(llm_init_lock);

        let response = send_chat_completions_request(
            http_base_url.as_str(),
            vec![
                ("system".to_string(), "You are an assistant that responds to queries by providing only the requested data values without extra explanation.".to_string()),
                ("user".to_string(), "Provide the total number of records in the taxi_trips dataset. If known, return a single numeric value.".to_string()),
            ],
            "hf_model",
            false,
        ).await?;

        // Message content verification is disabled due to issue below: model does not use tools and can't provide the expected response.
        // https://github.com/spiceai/spiceai/issues/3426
        insta::assert_snapshot!(
            "chat_completion",
            normalize_chat_completion_response(response, true)
        );

        Ok(())
    }).await
}

#[tokio::test]
async fn huggingface_test_chat_messages() -> Result<(), anyhow::Error> {
    if HF_TEST_MODEL_REQUIRES_HF_API_KEY {
        verify_env_secret_exists("SPICE_HF_TOKEN")
            .await
            .map_err(anyhow::Error::msg)?;
    }

    test_request_context().scope(async {
        let model = Arc::new(create_hf_model(
            HF_TEST_MODEL,
        Some(HF_TEST_MODEL_TYPE),
        None,
            None,
        )?);

        let app = AppBuilder::new("ai-app")
        .with_dataset(get_taxi_trips_dataset())
        .build();

        let rt = Arc::new(Runtime::builder().with_app(app).build().await);

        let llm_init_lock = LOCAL_LLM_INIT_MUTEX.lock().await;

        tokio::select! {
            // increased timeout to download and load huggingface model
            () = tokio::time::sleep(std::time::Duration::from_secs(300)) => {
                return Err(anyhow::anyhow!("Timed out waiting for components to load"));
            }
            () = rt.load_components() => {}
        }

        drop(llm_init_lock);

        let tool_model = Box::new(ToolUsingChat::new(
            Arc::clone(&model),
            Arc::clone(&rt),
            get_tools(Arc::clone(&rt), &SpiceToolsOptions::Auto).await,
            Some(10),
        ));

        let req = CreateChatCompletionRequestArgs::default()
            .messages(vec![ChatCompletionRequestSystemMessageArgs::default()
                .content("You are an assistant that responds to queries by providing only the requested data values without extra explanation.".to_string())
                .build()?
                .into(),ChatCompletionRequestUserMessageArgs::default()
                .content("Provide the total number of records in the taxi trips dataset. If known, return a single numeric value.".to_string())
                .build()?
                .into()])
            .build()?;

        let mut response = tool_model.chat_request(req).await?;

        // Message content verification is disabled due to issue below: model does not use tools and can't provide the expected response.
        // https://github.com/spiceai/spiceai/issues/3426
        response.choices.iter_mut().for_each(|c| {
            c.message.content = Some("__placeholder__".to_string());
        });

        insta::assert_snapshot!("chat_1_response_choices", format!("{:?}", response.choices));

        Ok(())
    })
    .await
}

fn get_huggingface_model(
    model: impl Into<String>,
    model_type: impl Into<String>,
    name: impl Into<String>,
) -> Model {
    let mut model = Model::new(format!("huggingface:huggingface.co/{}", model.into()), name);
    model
        .params
        .insert("model_type".to_string(), model_type.into().into());

    model
        .params
        .insert("hf_token".to_string(), "${ secrets:SPICE_HF_TOKEN }".into());

    model
}

fn get_huggingface_embeddings(model: impl Into<String>, name: impl Into<String>) -> Embeddings {
    Embeddings::new(format!("huggingface:huggingface.co/{}", model.into()), name)
}
