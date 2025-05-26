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

use app::{App, AppBuilder};
use http::HeaderValue;
use http::header::{ACCEPT, CONTENT_TYPE};
use reqwest::header::HeaderMap;
use runtime::Runtime;
use runtime::auth::EndpointAuth;
use serde_json::{Value, json};
use spicepod::component::dataset::Dataset;
use spicepod::component::embeddings::{ColumnEmbeddingConfig, EmbeddingChunkConfig};
use std::sync::Arc;

use crate::models::hf::get_huggingface_embeddings;
use crate::models::openai::get_openai_embeddings;
use crate::models::{create_api_bindings_config, http_post};
use crate::utils::{runtime_ready_check, test_request_context, verify_env_secret_exists};
use crate::{init_tracing, init_tracing_with_task_history};

use super::{get_tpcds_dataset, sort_json_keys};

pub struct SearchTestCase {
    pub name: &'static str,
    pub body: serde_json::Value,
}

pub async fn run_search_test(base_url: &str, ts: &SearchTestCase) -> Result<(), anyhow::Error> {
    tracing::info!("Running test cases {}", ts.name);

    // Call /v1/search, check response
    let mut headers = HeaderMap::new();
    headers.insert(ACCEPT, HeaderValue::from_static("application/json"));
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    match http_post(
        &format!("{base_url}/v1/search").to_string(),
        &ts.body.to_string(),
        headers,
    )
    .await
    {
        Ok(response_str) => {
            let response = serde_json::from_str(&response_str)
                .map_err(|e| anyhow::anyhow!("Failed to parse HTTP response: {}", e))?;

            insta::assert_snapshot!(
                format!("{}_response", ts.name),
                normalize_search_response(response)
            );
        }
        Err(e) => {
            insta::assert_snapshot!(format!("{}_error_response", ts.name), e.to_string());
        }
    }
    Ok(())
}

/// Normalizes vector similarity search response for consistent snapshot testing by replacing dynamic
/// values such as duration with placeholder.
fn normalize_search_response(mut json: Value) -> String {
    if let Some(matches) = json.get_mut("matches").and_then(|m| m.as_array_mut()) {
        for m in matches {
            if let Some(obj) = m.as_object_mut() {
                obj.remove("score");
            }
        }
    }

    if let Some(duration) = json.get_mut("duration_ms") {
        *duration = json!("duration_ms_val");
    }

    sort_json_keys(&mut json);

    serde_json::to_string_pretty(&json).unwrap_or_default()
}

pub(crate) fn item_tpch_dataset_w_embeddings(
    ds_name: &str,
    model: &str,
    primary_keys: Option<Vec<String>>,
    chunking: Option<EmbeddingChunkConfig>,
) -> Dataset {
    let mut ds_tpcds_item = get_tpcds_dataset("item", Some(ds_name), None);
    ds_tpcds_item.embeddings = vec![ColumnEmbeddingConfig {
        column: "i_item_desc".to_string(),
        model: model.to_string(),
        primary_keys,
        chunking,
    }];

    ds_tpcds_item
}

pub(crate) fn catalog_page_tpch_dataset_w_embeddings(
    ds_name: &str,
    model: &str,
    primary_keys: Option<Vec<String>>,
    chunking: Option<EmbeddingChunkConfig>,
) -> Dataset {
    let mut ds_tpcds_cp = get_tpcds_dataset(
        "catalog_page",
        Some(ds_name),
        Some(format!(
            "select cp_description, cp_catalog_page_sk, cp_department, cp_catalog_number from {ds_name} limit 20"
        ).as_str()),
    );
    ds_tpcds_cp.embeddings = vec![ColumnEmbeddingConfig {
        column: "cp_description".to_string(),
        model: model.to_string(),
        primary_keys,
        chunking,
    }];
    ds_tpcds_cp
}

pub(crate) async fn run_search(
    app: App,
    test_cases: Vec<SearchTestCase>,
) -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(None);

    test_request_context()
        .scope(async {
            let api_config = create_api_bindings_config();
            let http_base_url = format!("http://{}", api_config.http_bind_address);
            let rt = Arc::new(Runtime::builder().with_app(app).build().await);

            let _ = init_tracing_with_task_history(None, &rt);

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
            for ts in test_cases {
                run_search_test(http_base_url.as_str(), &ts).await?;
            }
            Ok(())
        })
        .await
}

#[ignore = "Non-deterministic order of search results makes snapshot testing unreliable"]
#[tokio::test]
async fn test_multi_column_search() -> Result<(), anyhow::Error> {
    let mut chunked = catalog_page_tpch_dataset_w_embeddings(
        "multi_column_search",
        "hf_minilm",
        Some(vec!["cp_catalog_page_sk".to_string()]),
        Some(EmbeddingChunkConfig {
            enabled: true,
            target_chunk_size: 512,
            overlap_size: 128,
            trim_whitespace: false,
        }),
    );
    chunked.embeddings.push(ColumnEmbeddingConfig {
        column: "cp_department".to_string(),
        model: "hf_minilm".to_string(),
        primary_keys: Some(vec!["cp_catalog_page_sk".to_string()]),
        chunking: None,
    });

    let app = AppBuilder::new("search_app")
        .with_dataset(chunked)
        .with_embedding(get_huggingface_embeddings(
            "sentence-transformers/all-MiniLM-L6-v2",
            "hf_minilm",
        ))
        .build();
    run_search(
        app,
        vec![
            SearchTestCase {
                name: "multi_column_basic",
                body: json!({
                    "text": "new patient",
                    "limit": 2,
                    "datasets": ["multi_column_search"]
                }),
            },
            SearchTestCase {
                name: "multi_column_additional",
                body: json!({
                    "text": "new patient",
                    "limit": 2,
                    "datasets": ["multi_column_search"],
                    "additional_columns": ["cp_catalog_number"],
                }),
            },
            SearchTestCase {
                name: "multi_column_where",
                body: json!({
                    "text": "new patient",
                    "datasets": ["multi_column_search"],
                    "where": "cp_catalog_page_sk % 2 = 1"
                }),
            },
        ],
    )
    .await
}

#[ignore = "Non-deterministic order of search results makes snapshot testing unreliable"]
#[tokio::test]
async fn test_multi_embedding_model_search() -> Result<(), anyhow::Error> {
    verify_env_secret_exists("SPICE_OPENAI_API_KEY")
        .await
        .map_err(anyhow::Error::msg)?;
    let mut chunked = catalog_page_tpch_dataset_w_embeddings(
        "multi_embedding_models",
        "openai_embeddings",
        Some(vec!["cp_catalog_page_sk".to_string()]),
        None,
    );
    chunked.embeddings.push(ColumnEmbeddingConfig {
        column: "cp_department".to_string(),
        model: "hf_minilm".to_string(),
        primary_keys: Some(vec!["cp_catalog_page_sk".to_string()]),
        chunking: None,
    });

    let app = AppBuilder::new("search_app")
        .with_dataset(chunked)
        .with_embedding(get_huggingface_embeddings(
            "sentence-transformers/all-MiniLM-L6-v2",
            "hf_minilm",
        ))
        .with_embedding(get_openai_embeddings(
            Some("text-embedding-3-small"),
            "openai_embeddings",
        ))
        .build();
    run_search(
        app,
        vec![
            SearchTestCase {
                name: "multi_embedding_models_basic",
                body: json!({
                    "text": "new patient",
                    "limit": 2,
                    "datasets": ["multi_embedding_models"]
                }),
            },
            SearchTestCase {
                name: "multi_embedding_models_additional",
                body: json!({
                    "text": "new patient",
                    "limit": 2,
                    "datasets": ["multi_embedding_models"],
                    "additional_columns": ["cp_catalog_number"],
                }),
            },
            SearchTestCase {
                name: "multi_embedding_models_where",
                body: json!({
                    "text": "new patient",
                    "datasets": ["multi_embedding_models"],
                    "where": "cp_catalog_page_sk % 2 = 0"
                }),
            },
        ],
    )
    .await
}

#[tokio::test]
async fn test_multi_column_search_no_pk() -> Result<(), anyhow::Error> {
    let mut chunked =
        catalog_page_tpch_dataset_w_embeddings("mulit_column_no_pks", "hf_minilm", None, None);
    chunked.embeddings.push(ColumnEmbeddingConfig {
        column: "cp_department".to_string(),
        model: "hf_minilm".to_string(),
        primary_keys: None,
        chunking: None,
    });

    let app = AppBuilder::new("search_app")
        .with_dataset(chunked)
        .with_embedding(get_huggingface_embeddings(
            "sentence-transformers/all-MiniLM-L6-v2",
            "hf_minilm",
        ))
        .build();
    run_search(
        app,
        vec![SearchTestCase {
            name: "multi_column_no_pks_basic",
            body: json!({
                "text": "new patient",
                "limit": 2,
                "datasets": ["mulit_column_no_pks"]
            }),
        }],
    )
    .await
}
