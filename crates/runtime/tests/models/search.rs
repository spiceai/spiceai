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
use runtime::config::Config;
use serde_json::{Value, json};
use spicepod::acceleration::Acceleration;
use spicepod::component::caching::CacheConfig;
use spicepod::component::dataset::Dataset;
use spicepod::component::embeddings::EmbeddingChunkConfig;
use spicepod::param::Params;
use spicepod::semantic::{Column, ColumnLevelEmbeddingConfig, FullTextSearchConfig};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use crate::DEFAULT_TRACING_MODELS;
use crate::models::hf::get_huggingface_embeddings;
use crate::models::openai::get_openai_embeddings;
use crate::models::{create_api_bindings_config, http_post};
use crate::utils::{runtime_ready_check, test_request_context, verify_env_secret_exists};
use crate::{init_tracing, utils::init_tracing_with_task_history};

use super::{get_tpcds_dataset, sort_json_keys};

pub struct SearchTestCase {
    pub name: &'static str,
    pub body: serde_json::Value,
}

pub async fn run_search_test(
    base_url: &str,
    ts: &SearchTestCase,
    extra_headers: Option<HeaderMap>,
) -> Result<(), anyhow::Error> {
    tracing::info!("Running test cases {}", ts.name);

    // Call /v1/search, check response
    let mut headers = HeaderMap::new();
    headers.extend(extra_headers.unwrap_or_default());

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
    if let Some(duration) = json.get_mut("duration_ms") {
        *duration = json!("duration_ms_val");
    }
    if let Some(matches) = json.get_mut("results").and_then(|m| m.as_array_mut()) {
        for m in matches {
            if let Some(obj) = m.as_object_mut() {
                if let Some(Value::Number(n)) = obj.get("score") {
                    if let Some(score) = n.as_f64() {
                        if let Some(truncated_score) =
                            serde_json::Number::from_f64((1000.0 * score).trunc() / 1000.0)
                        // Keep 2 decimals
                        {
                            obj.insert("score".to_string(), Value::Number(truncated_score));
                        }
                    }
                }
            }
        }
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
    ds_tpcds_item.columns = vec![Column {
        name: "i_item_desc".to_string(),
        embeddings: vec![ColumnLevelEmbeddingConfig {
            model: model.to_string(),
            row_ids: primary_keys,
            chunking,
        }],
        description: None,
        full_text_search: None,
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
    ds_tpcds_cp.columns = vec![Column {
        name: "cp_description".to_string(),
        embeddings: vec![ColumnLevelEmbeddingConfig {
            model: model.to_string(),
            row_ids: primary_keys,
            chunking,
        }],
        description: None,
        full_text_search: None,
    }];
    ds_tpcds_cp
}

async fn start_app(app: App) -> Result<Config, anyhow::Error> {
    let api_config = create_api_bindings_config();
    let rt = Arc::new(Runtime::builder().with_app(app).build().await);

    let _ = init_tracing_with_task_history(DEFAULT_TRACING_MODELS, &rt);

    let rt_ref_copy = Arc::clone(&rt);
    let api_config_clone = api_config.clone();
    tokio::spawn(async move {
        Box::pin(rt_ref_copy.start_servers(api_config_clone, None, EndpointAuth::no_auth())).await
    });

    tokio::select! {
        () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
            return Err(anyhow::anyhow!("Timed out waiting for components to load"));
        }
        () = Arc::clone(&rt).load_components() => {}
    }

    runtime_ready_check(&rt).await;

    Ok(api_config)
}

pub(crate) async fn run_search(
    app: App,
    test_cases: Vec<SearchTestCase>,
) -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(None);

    test_request_context()
        .scope(async {
            let api_config = start_app(app).await?;
            let http_base_url = format!("http://{}", api_config.http_bind_address);
            for ts in test_cases {
                run_search_test(http_base_url.as_str(), &ts, None).await?;
            }
            Ok(())
        })
        .await
}

#[tokio::test]
async fn test_multi_column_search() -> Result<(), anyhow::Error> {
    let mut ds = catalog_page_tpch_dataset_w_embeddings(
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
    ds.columns.push(Column {
        name: "cp_department".to_string(),
        embeddings: vec![ColumnLevelEmbeddingConfig {
            model: "hf_minilm".to_string(),
            row_ids: Some(vec!["cp_catalog_page_sk".to_string()]),
            chunking: None,
        }],
        description: None,
        full_text_search: None,
    });

    let app = AppBuilder::new("search_app")
        .with_dataset(ds)
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

#[tokio::test]
async fn test_multi_embedding_model_search() -> Result<(), anyhow::Error> {
    verify_env_secret_exists("SPICE_OPENAI_API_KEY")
        .await
        .map_err(anyhow::Error::msg)?;
    let mut ds = catalog_page_tpch_dataset_w_embeddings(
        "multi_embedding_models",
        "openai_embeddings",
        Some(vec!["cp_catalog_page_sk".to_string()]),
        None,
    );
    ds.columns.push(Column {
        name: "cp_department".to_string(),
        embeddings: vec![ColumnLevelEmbeddingConfig {
            model: "hf_minilm".to_string(),
            row_ids: Some(vec!["cp_catalog_page_sk".to_string()]),
            chunking: None,
        }],
        description: None,
        full_text_search: None,
    });

    let app = AppBuilder::new("search_app")
        .with_dataset(ds)
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
async fn test_multi_column_srch_no_pk() -> Result<(), anyhow::Error> {
    let mut chunked =
        catalog_page_tpch_dataset_w_embeddings("mulit_column_no_pks", "hf_minilm", None, None);
    chunked.columns.push(Column {
        name: "cp_department".to_string(),
        embeddings: vec![ColumnLevelEmbeddingConfig {
            model: "hf_minilm".to_string(),
            row_ids: None,
            chunking: None,
        }],
        description: None,
        full_text_search: None,
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

#[tokio::test]
async fn test_hybrid_search_single_column() -> Result<(), anyhow::Error> {
    let mut ds = catalog_page_tpch_dataset_w_embeddings(
        "hybrid_column_search",
        "hf_minilm",
        Some(vec!["cp_catalog_page_sk".to_string()]),
        None,
    );
    let col: &mut Column = ds.columns.first_mut().expect("column to be defined");
    col.full_text_search = Some(FullTextSearchConfig {
        enabled: true,
        row_ids: Some(vec!["cp_catalog_page_sk".to_string()]),
    });

    let app = AppBuilder::new("search_app")
        .with_dataset(ds)
        .with_embedding(get_huggingface_embeddings(
            "sentence-transformers/all-MiniLM-L6-v2",
            "hf_minilm",
        ))
        .build();
    run_search(
        app,
        vec![
            SearchTestCase {
                name: "hybrid_column_search_basic",
                body: json!({
                    "text": "basic",
                    "limit": 2,
                    "datasets": ["hybrid_column_search"]
                }),
            },
            SearchTestCase {
                name: "hybrid_column_search_additional",
                body: json!({
                    "text": "basic",
                    "limit": 2,
                    "datasets": ["hybrid_column_search"],
                    "additional_columns": ["cp_catalog_number"],
                }),
            },
            SearchTestCase {
                name: "hybrid_column_search_where",
                body: json!({
                    "text": "basic",
                    "datasets": ["hybrid_column_search"],
                    "where": "cp_catalog_page_sk % 2 = 1"
                }),
            },
        ],
    )
    .await
}

#[tokio::test]
async fn test_hybrid_search_multiple_column() -> Result<(), anyhow::Error> {
    let mut ds = catalog_page_tpch_dataset_w_embeddings(
        "multi_column_hybrid_search",
        "hf_minilm",
        Some(vec!["cp_catalog_page_sk".to_string()]),
        Some(EmbeddingChunkConfig {
            enabled: true,
            target_chunk_size: 512,
            overlap_size: 128,
            trim_whitespace: false,
        }),
    );
    ds.columns.push(Column {
        name: "cp_department".to_string(),
        embeddings: vec![],
        description: None,
        full_text_search: Some(FullTextSearchConfig {
            enabled: true,
            row_ids: Some(vec!["cp_catalog_page_sk".to_string()]),
        }),
    });

    let app = AppBuilder::new("search_app")
        .with_dataset(ds)
        .with_embedding(get_huggingface_embeddings(
            "sentence-transformers/all-MiniLM-L6-v2",
            "hf_minilm",
        ))
        .build();
    run_search(
        app,
        vec![
            SearchTestCase {
                name: "multi_column_hybrid_basic",
                body: json!({
                    "text": "department",
                    "limit": 2,
                    "datasets": ["multi_column_hybrid_search"]
                }),
            },
            SearchTestCase {
                name: "multi_column_hybrid_additional",
                body: json!({
                    "text": "patient",
                    "limit": 2,
                    "datasets": ["multi_column_hybrid_search"],
                    "additional_columns": ["cp_catalog_number"],
                }),
            },
            SearchTestCase {
                name: "multi_column_hybrid_where",
                body: json!({
                    "text": "general",
                    "datasets": ["multi_column_hybrid_search"],
                    "where": "cp_catalog_page_sk % 2 = 1"
                }),
            },
        ],
    )
    .await
}

#[tokio::test]
async fn test_text_search() -> Result<(), anyhow::Error> {
    let mut ds = get_tpcds_dataset("item", Some("item"), None);
    ds.columns = vec![Column {
        name: "i_item_desc".to_string(),
        embeddings: vec![],
        description: None,
        full_text_search: Some(FullTextSearchConfig {
            enabled: true,
            row_ids: Some(vec!["i_item_sk".to_string()]),
        }),
    }];

    run_search(
        AppBuilder::new("search_app").with_dataset(ds).build(),
        vec![
            SearchTestCase {
                name: "text_search_basic",
                body: json!({
                    "text": "Patient",
                    "limit": 2,
                    "datasets": ["item"],
                    "additional_columns": ["i_color", "i_item_id"],
                }),
            },
            SearchTestCase {
                name: "text_search_with_extra_columns_and_where",
                body: json!({
                    "text": "Patient",
                    "datasets": ["item"],
                    "additional_columns": ["i_color", "i_item_id"],
                    "where": "i_color='smoke'",
                    "limit": 1,
                }),
            },
        ],
    )
    .await
}

#[tokio::test]
async fn test_text_search_multiple_columns() -> Result<(), anyhow::Error> {
    let mut ds = get_tpcds_dataset(
            "catalog_page",
            Some("catalog_page"),
            Some("select cp_description, cp_catalog_page_sk, cp_department, cp_catalog_number from catalog_page limit 20".to_string().as_str()),
        );
    ds.columns = vec![
        Column {
            name: "cp_description".to_string(),
            embeddings: vec![],
            description: None,
            full_text_search: Some(FullTextSearchConfig {
                enabled: true,
                row_ids: Some(vec!["cp_catalog_page_sk".to_string()]),
            }),
        },
        Column {
            name: "cp_department".to_string(),
            embeddings: vec![],
            description: None,
            full_text_search: Some(FullTextSearchConfig {
                enabled: true,
                row_ids: Some(vec!["cp_catalog_page_sk".to_string()]),
            }),
        },
    ];
    run_search(
        AppBuilder::new("search_app").with_dataset(ds).build(),
        vec![
            SearchTestCase {
                name: "multi_text_column_basic",
                body: json!({
                    "text": "general",
                    "limit": 2,
                    "datasets": ["catalog_page"]
                }),
            },
            SearchTestCase {
                name: "multi_text_column_additional",
                body: json!({
                    "text": "general",
                    "limit": 2,
                    "datasets": ["catalog_page"],
                    "additional_columns": ["cp_catalog_number"],
                }),
            },
            SearchTestCase {
                name: "multi_text_column_where",
                body: json!({
                    "text": "patient",
                    "datasets": ["catalog_page"],
                    "where": "cp_department='DEPARTMENT'"
                }),
            },
        ],
    )
    .await
}

#[cfg(feature = "flightsql")]
#[tokio::test]
async fn test_multi_column_w_existing_embedding() -> Result<(), anyhow::Error> {
    let api_config = start_app(
        AppBuilder::new("search_app")
            .with_dataset(catalog_page_tpch_dataset_w_embeddings(
                "single_column",
                "hf_minilm",
                Some(vec!["cp_catalog_page_sk".to_string()]),
                None,
            ))
            .with_embedding(get_huggingface_embeddings(
                "sentence-transformers/all-MiniLM-L6-v2",
                "hf_minilm",
            ))
            .build(),
    )
    .await?;

    // Make a new dataset where one embedding column is prexisting (from 'single_column'),
    // and another is made in this dataset.
    let mut ds = Dataset::new("flightsql:single_column", "multiple_columns");
    let mut params = HashMap::new();
    params.insert(
        "flightsql_endpoint".to_string(),
        format!("http://{}", api_config.flight_bind_address),
    );
    ds.acceleration = Some(Acceleration {
        enabled: true,
        ..Default::default()
    });
    ds.params = Some(Params::from_string_map(params));
    ds.columns = vec![
        Column {
            name: "cp_description".to_string(),
            description: Some(
                "This column has an embedding in the underlying spice instance".to_string(),
            ),
            full_text_search: None,
            embeddings: vec![ColumnLevelEmbeddingConfig {
                model: "hf_minilm".to_string(),
                row_ids: Some(vec!["cp_catalog_page_sk".to_string()]),
                chunking: None,
            }],
        },
        Column {
            name: "cp_department".to_string(),
            description: Some("This column is newly embedded in this spice app".to_string()),
            full_text_search: None,
            embeddings: vec![ColumnLevelEmbeddingConfig {
                model: "hf_minilm".to_string(),
                row_ids: Some(vec!["cp_catalog_page_sk".to_string()]),
                chunking: None,
            }],
        },
    ];
    let app2 = AppBuilder::new("search_app2")
        .with_dataset(ds)
        .with_embedding(get_huggingface_embeddings(
            "sentence-transformers/all-MiniLM-L6-v2",
            "hf_minilm",
        ))
        .build();

    run_search(
        app2,
        vec![
            SearchTestCase {
                name: "multi_embedding_parent_child_basic",
                body: json!({
                    "text": "new patient",
                    "limit": 2,
                    "datasets": ["multiple_columns"]
                }),
            },
            SearchTestCase {
                name: "multi_embedding_parent_child_additional",
                body: json!({
                    "text": "new patient",
                    "limit": 2,
                    "datasets": ["multiple_columns"],
                    "additional_columns": ["cp_catalog_number"],
                }),
            },
            SearchTestCase {
                name: "multi_embedding_parent_child_where",
                body: json!({
                    "text": "new patient",
                    "datasets": ["multiple_columns"],
                    "where": "cp_catalog_page_sk % 2 = 0"
                }),
            },
        ],
    )
    .await
}

#[tokio::test]
async fn test_search_with_cache() -> Result<(), anyhow::Error> {
    let chunked = catalog_page_tpch_dataset_w_embeddings(
        "cached_search",
        "hf_minilm",
        Some(vec!["cp_catalog_page_sk".to_string()]),
        Some(EmbeddingChunkConfig {
            enabled: true,
            target_chunk_size: 512,
            overlap_size: 128,
            trim_whitespace: false,
        }),
    );

    let cache_config = CacheConfig {
        enabled: true,
        item_ttl: Some("10s".to_string()),
        ..Default::default()
    };

    let app = AppBuilder::new("cached_search")
        .with_dataset(chunked)
        .with_embedding(get_huggingface_embeddings(
            "sentence-transformers/all-MiniLM-L6-v2",
            "hf_minilm",
        ))
        .with_search_cache(cache_config)
        .build();

    let _tracing = init_tracing(None);

    test_request_context()
        .scope(async {
            let api_config = start_app(app).await?;
            let http_base_url = format!("http://{}", api_config.http_bind_address);
            let start = Instant::now();
            run_search_test(http_base_url.as_str(), &SearchTestCase {
                name: "pre_cache",
                body: json!({
                    "text": "new patient",
                    "limit": 2,
                }),
            }, None).await?;
            let duration = start.elapsed();
            let start = Instant::now();
            run_search_test(http_base_url.as_str(), &SearchTestCase {
                name: "post_cache",
                body: json!({
                    "text": "new patient",
                    "limit": 2,
                }),
            }, None).await?;
            let duration_cached = start.elapsed();
            assert!(duration_cached * 10 < duration, "Cache did not improve performance by an order of magnitude. First: {duration:?}, Second: {duration_cached:?}");
            Ok(())
        })
        .await
}

#[tokio::test]
async fn test_search_with_cache_bypass() -> Result<(), anyhow::Error> {
    let chunked = catalog_page_tpch_dataset_w_embeddings(
        "cached_search_bypass",
        "hf_minilm",
        Some(vec!["cp_catalog_page_sk".to_string()]),
        Some(EmbeddingChunkConfig {
            enabled: true,
            target_chunk_size: 512,
            overlap_size: 128,
            trim_whitespace: false,
        }),
    );

    let cache_config = CacheConfig {
        enabled: true,
        item_ttl: Some("10s".to_string()),
        ..Default::default()
    };

    let app = AppBuilder::new("test_search_with_cache_bypass")
        .with_dataset(chunked)
        .with_embedding(get_huggingface_embeddings(
            "sentence-transformers/all-MiniLM-L6-v2",
            "hf_minilm",
        ))
        .with_search_cache(cache_config)
        .build();

    let _tracing = init_tracing(None);

    test_request_context()
        .scope(async {
            let api_config = start_app(app).await?;
            let http_base_url = format!("http://{}", api_config.http_bind_address);
            let start = Instant::now();

            let mut bypass_headers = HeaderMap::new();
            bypass_headers.insert("Cache-Control", "no-cache".parse().expect("valid header"));
            run_search_test(http_base_url.as_str(), &SearchTestCase {
                name: "pre_cache",
                body: json!({
                    "text": "new patient",
                    "limit": 2,
                }),
            }, Some(bypass_headers.clone())).await?;
            let duration = start.elapsed().as_secs_f64();
            let start = Instant::now();
            run_search_test(http_base_url.as_str(), &SearchTestCase {
                name: "post_cache",
                body: json!({
                    "text": "new patient",
                    "limit": 2,
                }),
            }, Some(bypass_headers)).await?;
            let duration_cached = start.elapsed().as_secs_f64();

            assert!(duration >= duration_cached*0.7 || duration <= duration_cached*1.3,
                "Cache bypass did not return similar performance. First: {duration:?}, Second: {duration_cached:?}");
            Ok(())
        })
        .await
}
