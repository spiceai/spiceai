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
use async_openai::types::EmbeddingInput;
use runtime::{Runtime, auth::EndpointAuth};
use spicepod::component::{embeddings::Embeddings, model::Model};

use crate::models::embedding::run_beta_functionality_criteria_test;
use crate::{
    init_tracing,
    models::{
        create_api_bindings_config,
        embedding::{EmbeddingTestCase, run_embedding_tests},
        get_taxi_trips_dataset, normalize_chat_completion_response, send_chat_completions_request,
    },
    utils::init_tracing_with_task_history,
    utils::{runtime_ready_check_with_timeout, test_request_context, verify_env_secret_exists},
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
    use spicepod::semantic::{Column, ColumnLevelEmbeddingConfig};

    use crate::{
        DEFAULT_TRACING_MODELS,
        models::nsql::{TestCase, run_nsql_test},
        utils::{runtime_ready_check_with_timeout, verify_env_secret_exists},
    };

    use super::*;

    // Tracking issue: https://github.com/spiceai/spiceai/issues/6328
    #[ignore]
    #[tokio::test]
    async fn huggingface_test_nsql() -> Result<(), anyhow::Error> {
        let _tracing = init_tracing(DEFAULT_TRACING_MODELS);

        if HF_TEST_MODEL_REQUIRES_HF_API_KEY {
            verify_env_secret_exists("SPICE_HF_TOKEN")
                .await
                .map_err(anyhow::Error::msg)?;
        }

        test_request_context()
            .scope(async {

                let mut taxi_trips_with_embeddings = get_taxi_trips_dataset();
                taxi_trips_with_embeddings.columns = vec![Column {
                        name: "store_and_fwd_flag".to_string(),
                        embeddings: vec![ColumnLevelEmbeddingConfig {
                            model: "hf_minilm".to_string(),
                            row_ids: None,
                            chunking: None,
                        }],
                        description: None,
                        full_text_search: None,
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

                let (_tracing, trace_provider) = init_tracing_with_task_history(DEFAULT_TRACING_MODELS, &rt);

                let rt_ref_copy = Arc::clone(&rt);
                tokio::spawn(async move {
                    Box::pin(rt_ref_copy.start_servers(api_config, None, EndpointAuth::no_auth())).await
                });

                let _llm_init_lock = LOCAL_LLM_INIT_MUTEX.lock().await;

                tokio::select! {
                    // increased timeout to download and load huggingface model
                    () = tokio::time::sleep(std::time::Duration::from_secs(300)) => {
                        return Err(anyhow::anyhow!("Timed out waiting for components to load"));
                    }
                    () = Arc::clone(&rt).load_components() => {}
                }

                runtime_ready_check_with_timeout(&rt, std::time::Duration::from_secs(120)).await;

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

    use crate::models::search::{
        SearchTestCase, catalog_page_tpch_dataset_w_embeddings, item_tpch_dataset_w_embeddings,
        run_search,
    };

    use super::*;

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn huggingface_test_search() -> Result<(), anyhow::Error> {
        let app = AppBuilder::new("text-to-sql")
            .with_dataset(item_tpch_dataset_w_embeddings(
                "item",
                "hf_minilm",
                Some(vec!["i_item_sk".to_string()]),
                None,
            ))
            .with_dataset(catalog_page_tpch_dataset_w_embeddings(
                "catalog_page_with_chunking",
                "hf_minilm",
                Some(vec!["cp_catalog_page_sk".to_string()]),
                Some(EmbeddingChunkConfig {
                    enabled: true,
                    target_chunk_size: 512,
                    overlap_size: 128,
                    trim_whitespace: false,
                }),
            ))
            .with_dataset(catalog_page_tpch_dataset_w_embeddings(
                "catalog_page_with_chunking_no_pk",
                "hf_minilm",
                None,
                Some(EmbeddingChunkConfig {
                    enabled: true,
                    target_chunk_size: 512,
                    overlap_size: 128,
                    trim_whitespace: false,
                }),
            ))
            .with_embedding(get_huggingface_embeddings(
                "sentence-transformers/all-MiniLM-L6-v2",
                "hf_minilm",
            ))
            .build();

        run_search(
            app,
            vec![
                SearchTestCase {
                    name: "hf_basic",
                    body: json!({
                        "text": "new patient",
                        "limit": 2,
                        "datasets": ["item"],
                        "additional_columns": ["i_color", "i_item_id"],
                    }),
                },
                SearchTestCase {
                    name: "hf_all_datasets",
                    body: json!({
                        "text": "new patient",
                        "limit": 2,
                    }),
                },
                SearchTestCase {
                    name: "hf_chunking",
                    body: json!({
                        "text": "friends",
                        "datasets": ["catalog_page_with_chunking"],
                        "limit": 1,
                    }),
                },
                SearchTestCase {
                    name: "hf_chunking_with_extra_columns",
                    body: json!({
                        "text": "friends",
                        "datasets": ["catalog_page_with_chunking"],
                        "additional_columns": ["cp_department"],
                        "limit": 1,
                    }),
                },
                SearchTestCase {
                    name: "hf_chunking_with_extra_columns2",
                    body: json!({
                        "text": "friends",
                        "datasets": ["catalog_page_with_chunking"],
                        "additional_columns": ["cp_catalog_page_sk", "cp_department", "cp_description"],
                        "limit": 1,
                    }),
                },
                SearchTestCase {
                    name: "hf_chunking_with_extra_columns_and_where",
                    body: json!({
                        "text": "friends",
                        "datasets": ["catalog_page_with_chunking"],
                        "additional_columns": ["cp_department"],
                        "where": "cp_catalog_number>0",
                        "limit": 1,
                    }),
                },
                SearchTestCase {
                    name: "hf_chunking_no_pk",
                    body: json!({
                        "text": "friends",
                        "datasets": ["catalog_page_with_chunking_no_pk"],
                        "limit": 1,
                    }),
                },
                SearchTestCase {
                    name: "hf_chunking_with_extra_column_no_pk",
                    body: json!({
                        "text": "friends",
                        "datasets": ["catalog_page_with_chunking_no_pk"],
                        "additional_columns": ["cp_department"],
                        "limit": 1,
                    }),
                },
                SearchTestCase {
                    name: "hf_chunking_with_extra_column_no_pk2",
                    body: json!({
                        "text": "friends",
                        "datasets": ["catalog_page_with_chunking_no_pk"],
                        "additional_columns": ["cp_catalog_page_sk", "cp_department", "cp_description"],
                        "limit": 1,
                    }),
                },
                SearchTestCase {
                    name: "hf_chunking_with_extra_columns_and_where_no_pk",
                    body: json!({
                        "text": "friends",
                        "datasets": ["catalog_page_with_chunking_no_pk"],
                        "additional_columns": ["cp_department"],
                        "where": "cp_catalog_number>0",
                        "limit": 1,
                    }),
                },
            ], vec![]
        ).await
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

        let _llm_init_lock = LOCAL_LLM_INIT_MUTEX.lock().await;

        tokio::select! {
            // increased timeout to download and load huggingface model
            () = tokio::time::sleep(std::time::Duration::from_secs(300)) => {
                return Err(anyhow::anyhow!("Timed out waiting for components to load"));
            }
            () = Arc::clone(&rt).load_components() => {}
        }

        runtime_ready_check_with_timeout(&rt, std::time::Duration::from_secs(120)).await;

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

pub(crate) fn get_huggingface_embeddings(
    model: impl Into<String>,
    name: impl Into<String>,
) -> Embeddings {
    Embeddings::new(format!("huggingface:huggingface.co/{}", model.into()), name)
}
