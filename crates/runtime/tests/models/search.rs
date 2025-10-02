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

use crate::models::hf::get_model_to_vec_embeddings;
use crate::models::openai::get_openai_embeddings;
use crate::models::{create_api_bindings_config, get_mega_science_dataset, http_post};
use crate::utils::{runtime_ready_check, test_request_context};
use crate::{DEFAULT_TRACING_MODELS, configure_test_datafusion};
use crate::{init_tracing, utils::init_tracing_with_task_history};
use anyhow::Context;
use app::{App, AppBuilder};
use arrow::array::RecordBatch;
use futures::TryStreamExt;
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
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt::Display;
use std::sync::Arc;
use std::time::Instant;

use super::{get_tpcds_dataset, sort_json_keys};

#[derive(Clone)]
pub enum SearchTestType {
    Http(serde_json::Value),
    Sql(&'static str),
}

impl Display for SearchTestType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SearchTestType::Http(value) => write!(f, "{value}"),
            SearchTestType::Sql(query) => write!(f, "{query}"),
        }
    }
}

#[derive(Clone)]
pub struct SearchTestCase {
    pub name: String,
    pub body: SearchTestType,
    pub should_fail: bool,
    pub skip: bool,
}

impl SearchTestCase {
    pub fn new(name: impl Into<String>, body: SearchTestType) -> Self {
        Self {
            name: name.into(),
            body,
            should_fail: false,
            skip: false,
        }
    }

    pub fn should_fail(mut self) -> Self {
        self.should_fail = true;
        self
    }

    pub fn skip(mut self) -> Self {
        self.skip = true;
        self
    }
}

async fn http_sql(base_url: &str, sql: &str) -> Result<Value, anyhow::Error> {
    let mut headers = HeaderMap::new();
    headers.insert(ACCEPT, HeaderValue::from_static("application/json"));
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("text/plain"));

    let response_str = http_post(&format!("{base_url}/v1/sql").to_string(), sql, headers).await?;
    serde_json::from_str(&response_str)
        .map_err(|e| anyhow::anyhow!("Failed to parse 'v1/sql' HTTP response: {}", e))
}

pub async fn run_search_test(
    base_url: &str,
    ts: &SearchTestCase,
    extra_headers: Option<HeaderMap>,
    should_fail: bool,
) -> Result<(), anyhow::Error> {
    tracing::info!("Running test cases {}", ts.name);

    // Call /v1/search, check response
    let mut headers = HeaderMap::new();
    headers.extend(extra_headers.unwrap_or_default());

    headers.insert(ACCEPT, HeaderValue::from_static("application/json"));
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    let resp = http_post(
        &format!("{base_url}/v1/search").to_string(),
        &ts.body.to_string(),
        headers,
    )
    .await;

    if should_fail {
        if resp.is_ok() {
            return Err(anyhow::anyhow!(format!(
                "Test {} was expected to fail but succeeded",
                ts.name
            )));
        }

        let err = resp.err().context("Test was expected to fail")?;
        insta::assert_snapshot!(format!("{}_error_response", ts.name), err.to_string());
        return Ok(());
    }

    let resp = serde_json::from_str(&resp?).context("Failed to parse HTTP response")?;
    insta::assert_snapshot!(
        format!("{}_response", ts.name),
        normalize_search_response(resp)
    );

    Ok(())
}

/// Normalizes vector similarity search response for consistent snapshot testing by replacing dynamic
/// values such as duration with placeholder.
fn normalize_search_response(mut json: Value) -> String {
    if let Some(duration) = json.get_mut("duration_ms") {
        *duration = json!("duration_ms_val");
    }
    if let Some(matches) = json.get_mut("results").and_then(|m| m.as_array_mut()) {
        // To avoid inconsistent snapshots when scores are equal (common when using RRF),
        // we also order based on primary key.
        matches.sort_by(|a, b| {
            let Some(Value::Number(num_a)) = a.get("score") else {
                return Ordering::Greater;
            };
            let Some(score_a) = num_a.as_f64() else {
                return Ordering::Greater;
            };
            let Some(Value::Number(num_b)) = b.get("score") else {
                return Ordering::Less;
            };
            let Some(score_b) = num_b.as_f64() else {
                return Ordering::Less;
            };

            // Opposite because we want to order descendingly
            if score_a > score_b {
                return Ordering::Less;
            } else if score_a < score_b {
                return Ordering::Greater;
            }

            let Some(Value::Object(a_pks)) = a.get("primary_key") else {
                return Ordering::Equal;
            };
            let Some(Value::Object(b_pks)) = b.get("primary_key") else {
                return Ordering::Equal;
            };
            format!("{b_pks:?}").cmp(&format!("{a_pks:?}"))
        });
        for m in matches {
            if let Some(obj) = m.as_object_mut()
                && let Some(Value::Number(n)) = obj.get("score")
                && let Some(score) = n.as_f64()
                && let Some(truncated_score) =
                    serde_json::Number::from_f64((100.0 * score).trunc() / 100.0)
            // Keep 4 decimals
            {
                obj.insert("score".to_string(), Value::Number(truncated_score));
            }
        }
    }

    sort_json_keys(&mut json);

    serde_json::to_string_pretty(&json).unwrap_or_default()
}

pub(crate) fn item_tpcds_dataset_w_embeddings(
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
            vector_size: None,
        }],
        description: None,
        full_text_search: None,
        metadata: HashMap::new(),
    }];

    ds_tpcds_item
}

pub(crate) fn catalog_page_tpcds_dataset_w_embeddings(
    ds_name: &str,
    model: &str,
    primary_keys: Option<Vec<String>>,
    chunking: Option<EmbeddingChunkConfig>,
) -> Dataset {
    let mut ds_tpcds_cp = Dataset::new(
        // pre-apply ordering and filtering due to https://github.com/spiceai/spiceai/issues/6876
        // ordering will create more deterministic tests to prevent flakiness
        "s3://spiceai-public-datasets/integration/tpcds/catalog_page.parquet".to_string(),
        ds_name,
    );
    ds_tpcds_cp.params = Some(Params::from_string_map(
        vec![
            ("file_format".to_string(), "parquet".to_string()),
            ("client_timeout".to_string(), "120s".to_string()),
        ]
        .into_iter()
        .collect(),
    ));
    ds_tpcds_cp.acceleration = Some(Acceleration {
        enabled: true,
        ..Default::default()
    });

    ds_tpcds_cp.columns = vec![Column {
        name: "cp_description".to_string(),
        embeddings: vec![ColumnLevelEmbeddingConfig {
            model: model.to_string(),
            row_ids: primary_keys,
            chunking,
            vector_size: None,
        }],
        description: None,
        full_text_search: None,
        metadata: HashMap::new(),
    }];
    ds_tpcds_cp
}

pub async fn start_app(app: App) -> Result<Config, anyhow::Error> {
    configure_test_datafusion();
    let api_config = create_api_bindings_config();
    let rt = Arc::new(Runtime::builder().with_app(app).build().await);

    let _ = init_tracing_with_task_history(DEFAULT_TRACING_MODELS, &rt);

    let rt_ref_copy = Arc::clone(&rt);
    let api_config_clone = api_config.clone();
    tokio::spawn(async move {
        Box::pin(rt_ref_copy.start_servers(api_config_clone, None, EndpointAuth::no_auth())).await
    });

    tokio::select! {
        () = tokio::time::sleep(std::time::Duration::from_secs(120)) => {
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
    run_search_w_explain(app, test_cases, false).await
}

// if `explain_sql`, for any [`SearchTestCase`] that is [`SearchTestType::Sql`], a snapshot will be taken of the associated explain query.
pub(crate) async fn run_search_w_explain(
    app: App,
    test_cases: Vec<SearchTestCase>,
    explain_sql: bool,
) -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(None);

    test_request_context()
        .scope(async {
            let api_config = start_app(app).await?;
            let http_base_url = format!("http://{}", api_config.http_bind_address);
            let client = spiceai::ClientBuilder::new()
                .flight_url(format!("http://{}", api_config.flight_bind_address).as_str())
                .build()
                .await
                .unwrap_or_else(|_| {
                    panic!(
                        "Failed to build Spice client with flight address: 'http://{}'",
                        api_config.flight_bind_address
                    )
                });
            for ts in test_cases {
                if ts.skip {
                    tracing::info!("Skipping test {}", ts.name);
                    continue;
                }

                match ts.body {
                    SearchTestType::Http(_) => {
                        run_search_test(http_base_url.as_str(), &ts, None, ts.should_fail).await?;
                    }
                    SearchTestType::Sql(sql) => {
                        let test_name = ts.name.clone();
                        let resp = http_sql(http_base_url.as_str(), sql).await;
                        if ts.should_fail {
                            if resp.is_ok() {
                                return Err(anyhow::anyhow!(format!(
                                    "Test {test_name} was expected to fail but succeeded",
                                )));
                            }

                            let err = resp.err().context("Test was expected to fail")?;
                            insta::assert_snapshot!(
                                format!("{test_name}_error_response"),
                                err.to_string()
                            );
                            continue;
                        }

                        insta::assert_json_snapshot!(test_name.clone(), resp?);

                        if explain_sql {
                            let c = client
                                .query(format!("EXPLAIN {sql}").as_str())
                                .await?
                                .try_collect::<Vec<RecordBatch>>()
                                .await?;

                            let disp = arrow::util::pretty::pretty_format_batches(&c)?;

                            insta::with_settings!({
                                omit_expression => true,
                                description => sql
                            }, {insta::assert_snapshot!(format!("{test_name}_explain"), disp)});
                        }
                    }
                }
            }
            Ok(())
        })
        .await
}

#[tokio::test]
async fn test_multi_column_search() -> Result<(), anyhow::Error> {
    let ds = get_mega_science_dataset(
        Some("qs"),
        Some(
            Column::new("question").with_embeddings(vec![ColumnLevelEmbeddingConfig {
                model: "hf_minilm".to_string(),
                row_ids: Some(vec!["id".to_string()]),
                chunking: None,
                vector_size: None,
            }]),
        ),
        Some(
            Column::new("answer").with_embeddings(vec![ColumnLevelEmbeddingConfig {
                model: "hf_minilm".to_string(),
                row_ids: Some(vec!["id".to_string()]),
                chunking: Some(EmbeddingChunkConfig::enabled().target_chunk_size(64)),
                vector_size: None,
            }]),
        ),
    );

    let app = AppBuilder::new("search_app")
        .with_dataset(ds)
        .with_embedding(get_model_to_vec_embeddings(
            "minishlab/potion-base-2M",
            "hf_minilm",
        ))
        .build();
    run_search(
        app,
        vec![
            SearchTestCase::new(
                "multi_column_basic".to_string(),
                SearchTestType::Http(json!({
                    "text": "second",
                    "limit": 4,
                    "datasets": ["qs"],
                })),
            ),
            SearchTestCase::new(
                "multi_column_additional_columns".to_string(),
                SearchTestType::Http(json!({
                    "text": "second",
                    "limit": 4,
                    "datasets": ["qs"],
                    "additional_columns": ["question"],
                })),
            ),
            SearchTestCase::new(
                "multi_column_with_where".to_string(),
                SearchTestType::Http(json!({
                    "text": "secondary",
                    "datasets": ["qs"],
                    "where": "subject='math'",
                    "limit": 1,
                })),
            ),
            SearchTestCase::new(
                "multi_column_question_vector_search_sql_filters".to_string(),
                SearchTestType::Sql(
                    "SELECT id, answer, trunc(score, 3) as score FROM vector_search(qs, 'secondary', question) where subject!='math' order by score desc LIMIT 4",
                ),
            ),
            SearchTestCase::new(
                "multi_column_question_vector_search_sql_no_score".to_string(),
                SearchTestType::Sql(
                    "SELECT id, answer FROM vector_search(qs, 'second', question) order by score desc LIMIT 4",
                ),
            ),
            SearchTestCase::new(
                "multi_column_question_vector_search_sql_random".to_string(),
                SearchTestType::Sql(
                    "SELECT subject FROM vector_search(qs, 'second', question) order by score desc LIMIT 4",
                ),
            ),
            SearchTestCase::new(
                "multi_column_question_vector_search_sql_vectors".to_string(),
                SearchTestType::Sql(
                    "SELECT id, answer, array_length(question_embedding), round(score, 1) FROM vector_search(qs, 'second', question) order by score desc LIMIT 4;",
                ),
            ),
        ],
    )
    .await
}

// Use two different embedding models on a single column.
#[tokio::test]
async fn test_multi_embedding_model_search() -> Result<(), anyhow::Error> {
    run_search(
        AppBuilder::new("search_app")
            .with_embedding(get_model_to_vec_embeddings(
                "minishlab/potion-base-2M",
                "hf_minilm",
            ))
            .with_embedding(get_openai_embeddings(
                Some("text-embedding-3-small"),
                "openai_embeddings",
            ))
            .with_dataset(get_mega_science_dataset(
                Some("qs"),
                None,
                Some(Column::new("answer").with_embeddings(vec![
                    ColumnLevelEmbeddingConfig::model("hf_minilm").with_row_id("id"),
                    ColumnLevelEmbeddingConfig::model("openai_embeddings").with_row_id("id")
                ]))))
            .build(),
        vec![
            SearchTestCase::new(
                "multi_embeddings_basic",
                SearchTestType::Http(json!({
                    "text": "second",
                    "limit": 4,
                    "datasets": ["qs"],
                })),
            ),
            SearchTestCase::new(
                "multi_embeddings_additional_columns",
                SearchTestType::Http(json!({
                    "text": "second",
                    "limit": 4,
                    "datasets": ["qs"],
                    "additional_columns": ["question"],
                })),
            ),
            SearchTestCase::new(
                "multi_embeddings_with_where",
                SearchTestType::Http(json!({
                    "text": "secondary",
                    "datasets": ["qs"],
                    "where": "subject!='math'",
                    "limit": 4,
                })),
            ),
            SearchTestCase::new(
                "multi_embeddings_sql_vector_search",
                SearchTestType::Sql(
                    "SELECT id, question, trunc(score, 3) FROM vector_search(qs, 'second') order by score desc LIMIT 4",
                ),
            ),
        ],
    )
    .await
}

// Ensure that if there is no primary key inferrable or available, that search results for multiple columns are not returned.
#[tokio::test]
async fn test_multi_column_srch_no_pk() -> Result<(), anyhow::Error> {
    let mut chunked =
        catalog_page_tpcds_dataset_w_embeddings("mulit_column_no_pks", "hf_minilm", None, None);
    chunked.columns.push(
        Column::new("cp_department").with_embedding(ColumnLevelEmbeddingConfig::model("hf_minilm")),
    );
    let app = AppBuilder::new("search_app")
        .with_dataset(chunked)
        .with_embedding(get_model_to_vec_embeddings(
            "minishlab/potion-base-2M",
            "hf_minilm",
        ))
        .build();
    run_search(
        app,
        vec![
            SearchTestCase::new(
                "multi_column_no_pks_basic",
                SearchTestType::Http(json!({
                    "text": "new patient",
                    "limit": 2,
                    "datasets": ["mulit_column_no_pks"]
                })),
            )
            .should_fail(),
        ],
    )
    .await
}

#[tokio::test]
async fn test_hybrid_search_single_column() -> Result<(), anyhow::Error> {
    run_search(
        AppBuilder::new("search_app")
            .with_embedding(get_model_to_vec_embeddings(
                "minishlab/potion-base-2M",
                "hf_minilm",
            ))
            .with_dataset(get_mega_science_dataset(
                Some("qs"),
                Some(Column::new("question")
                    .with_embedding(ColumnLevelEmbeddingConfig::model("hf_minilm").with_row_id("id"))
                    .with_full_text_search(FullTextSearchConfig::enabled().with_row_id("id"))
                ),
                None,
            ))
            .build(),
        vec![
            SearchTestCase::new(
                "hybrid_single_column_basic",
                SearchTestType::Http(json!({
                    "text": "second",
                    "limit": 4,
                    "datasets": ["qs"],
                })),
            ),
            SearchTestCase::new(
                "hybrid_single_column_additional_columns",
                SearchTestType::Http(json!({
                    "text": "second",
                    "limit": 4,
                    "datasets": ["qs"],
                    "additional_columns": ["question"],
                })),
            ),
            SearchTestCase::new(
                "hybrid_single_column_with_where",
                SearchTestType::Http(json!({
                    "text": "secondary",
                    "datasets": ["qs"],
                    "where": "subject!='math'",
                    "limit": 4,
                })),
            ),
            SearchTestCase::new(
                "hybrid_single_column_sql_text_search",
                SearchTestType::Sql(
                    "SELECT id, answer, trunc(score, 3) FROM text_search(qs, 'second') order by score desc LIMIT 4",
                ),
            ),
            SearchTestCase::new(
                "hybrid_single_column_sql_vector_search",
                SearchTestType::Sql(
                    "SELECT id, question, trunc(score, 3) FROM vector_search(qs, 'second') order by score desc LIMIT 4",
                ),
            ),
            SearchTestCase::new(
                "hybrid_single_column_sql_vector_search_no_score",
                SearchTestType::Sql(
                    "SELECT question FROM vector_search(qs, 'second') order by score desc LIMIT 4",
                ),
            ),
        ],
    )
    .await
}

#[tokio::test]
async fn test_hybrid_search_multiple_column() -> Result<(), anyhow::Error> {
    run_search(
        AppBuilder::new("search_app")
            .with_embedding(get_model_to_vec_embeddings(
                "minishlab/potion-base-2M",
                "hf_minilm",
            ))
            .with_dataset(get_mega_science_dataset(
                Some("qs"),
                Some(Column::new("question").with_embedding(ColumnLevelEmbeddingConfig::model("hf_minilm").with_row_id("id"))),
                Some(Column::new("answer").with_full_text_search(FullTextSearchConfig::enabled().with_row_id("id"))),
            ))
            .build(),
        vec![
            SearchTestCase::new(
                "hybrid_multiple_column_basic",
                SearchTestType::Http(json!({
                    "text": "second",
                    "limit": 4,
                    "datasets": ["qs"],
                })),
            ),
            SearchTestCase::new(
                "hybrid_multiple_column_additional_columns",
                SearchTestType::Http(json!({
                    "text": "second",
                    "limit": 4,
                    "datasets": ["qs"],
                    "additional_columns": ["question"],
                })),
            ),
            SearchTestCase::new(
                "hybrid_multiple_column_with_where",
                SearchTestType::Http(json!({
                    "text": "secondary",
                    "datasets": ["qs"],
                    "where": "subject!='math'",
                    "limit": 4,
                })),
            ),
            SearchTestCase::new(
                "hybrid_multiple_column_sql_text_search",
                SearchTestType::Sql(
                    "SELECT id, answer, trunc(score, 3) FROM text_search(qs, 'second') order by score desc LIMIT 4",
                ),
            ),
            SearchTestCase::new(
                "hybrid_multiple_column_sql_text_search_wrong_column",
                SearchTestType::Sql(
                    "SELECT id, answer, trunc(score, 3) FROM text_search(qs, 'second', question) order by score desc LIMIT 4",
                ),
            ).should_fail(),
            SearchTestCase::new(
                "hybrid_multiple_column_sql_vector_search",
                SearchTestType::Sql(
                    "SELECT id, question, trunc(score, 3) FROM vector_search(qs, 'second') order by score desc LIMIT 4",
                ),
            ),
            SearchTestCase::new(
                "hybrid_multiple_column_sql_vector_search_wrong_column",
                SearchTestType::Sql(
                    "SELECT id, question, trunc(score, 3) FROM vector_search(qs, 'second', answer) order by score desc LIMIT 4",
                ),
            ).should_fail(),
        ],
    )
    .await
}

#[tokio::test]
async fn test_rrf_search() -> Result<(), anyhow::Error> {
    run_search(
        AppBuilder::new("search_app")
            .with_embedding(get_model_to_vec_embeddings(
                "minishlab/potion-base-2M",
                "hf_minilm",
            ))
            .with_dataset(get_mega_science_dataset(
                Some("qs"),
                Some(Column::new("question").with_embedding(ColumnLevelEmbeddingConfig::model("hf_minilm").with_row_id("id"))),
                Some(Column::new("answer").with_full_text_search(FullTextSearchConfig::enabled().with_row_id("id"))),
            ))
            .build(),
        vec![
            SearchTestCase::new(
                "hybrid_multiple_column_sql_rrf",
                SearchTestType::Sql(
                    "SELECT id, question, trunc(fused_score, 3) FROM rrf(vector_search(qs, 'second'), text_search(qs, 'second')) order by fused_score desc LIMIT 4",
                ),
            ),
            SearchTestCase::new(
                "hybrid_multiple_column_sql_rrf_wrong_column",
                SearchTestType::Sql(
                    "SELECT id, question, trunc(score, 3) FROM rrf(vector_search(qs, 'second', answer), text_search(qs, 'second', answer)) order by fused_score desc LIMIT 4",
                ),
            ).should_fail(),
            SearchTestCase::new(
                "hybrid_multiple_column_sql_rrf_explicit_join",
                SearchTestType::Sql(
                    "SELECT id, question, trunc(fused_score, 3) FROM rrf(vector_search(qs, 'second'), text_search(qs, 'second'), join_key => 'id') order by fused_score desc LIMIT 4",
                ),
            ),
            SearchTestCase::new(
                "hybrid_multiple_column_sql_rrf_explicit_join_wrong_column",
                SearchTestType::Sql(
                    "SELECT id, question, trunc(fused_score, 3) FROM rrf(vector_search(qs, 'second'), text_search(qs, 'second'), join_key => 'foobar') order by fused_score desc LIMIT 4",
                ),
            ).should_fail(),
            SearchTestCase::new(
                "hybrid_multiple_column_sql_rrf_one_subquery_fail",
                SearchTestType::Sql(
                    "SELECT id, question, trunc(fused_score, 3) FROM rrf(vector_search(qs, 'second')) order by fused_score desc LIMIT 4",
                ),
            ).should_fail(),
        ],
    ).await
}

// HTTP error: 500 Internal Server Error - Error occurred in search pipeline: Error occurred aggregating candidate search results: A database error occurred whilst aggregating search candidates: Schema error: No field named table_provider."""cp_department""". Valid fields are candidate_generation.value, candidate_generation.cp_catalog_page_sk, candidate_generation.cp_description, candidate_generation.score, table_provider.cp_description, table_provider.cp_catalog_page_sk, table_provider.cp_department, table_provider.cp_catalog_number.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_text_search() -> Result<(), anyhow::Error> {
    run_search(
        AppBuilder::new("search_app")
            .with_dataset(get_mega_science_dataset(
                Some("qs"),
                None,
                Some(Column::new("answer").with_full_text_search(FullTextSearchConfig::enabled().with_row_id("id"))),
            ))
            .build(),
        vec![
            SearchTestCase::new(
                "text_search_basic",
                SearchTestType::Http(json!({
                    "text": "second",
                    "limit": 4,
                    "datasets": ["qs"],
                })),
            ),
            SearchTestCase::new(
                "text_search_additional_columns",
                SearchTestType::Http(json!({
                    "text": "second",
                    "limit": 4,
                    "datasets": ["qs"],
                    "additional_columns": ["question"],
                })),
            ),
            SearchTestCase::new(
                "text_search_with_where",
                SearchTestType::Http(json!({
                    "text": "secondary",
                    "datasets": ["qs"],
                    "where": "subject!='math'",
                    "limit": 4,
                })),
            ),
            SearchTestCase::new(
                "text_search_basic_without_defined_dataset",
                SearchTestType::Http(json!({
                    "text": "second",
                    "limit": 4,
                })),
            ),
            SearchTestCase::new(
                "text_search_sql_text_search_basic",
                SearchTestType::Sql(
                    "SELECT id, answer, trunc(score, 3) FROM text_search(qs, 'second') order by score desc LIMIT 4",
                ),
            ),
            SearchTestCase::new(
                "text_search_sql_text_search_projection",
                SearchTestType::Sql(
                    "SELECT id, answer, question, subject, trunc(score, 3) as score FROM text_search(qs, 'second') order by score desc LIMIT 4",
                ),
            ),
            SearchTestCase::new(
                "text_search_sql_text_search_filters",
                SearchTestType::Sql(
                    "SELECT id, answer, trunc(score, 3) as score FROM text_search(qs, 'secondary') where subject!='math' order by score desc LIMIT 4",
                ),
            ),
            SearchTestCase::new(
                "text_search_sql_text_search_no_score",
                SearchTestType::Sql(
                    "SELECT id, answer FROM text_search(qs, 'second') order by score desc LIMIT 4",
                ),
            ),
            SearchTestCase::new(
                "text_search_sql_text_search_random",
                SearchTestType::Sql(
                    "SELECT subject FROM text_search(qs, 'second') order by score desc LIMIT 4",
                ),
            ),
        ],
    )
    .await
}

#[tokio::test]
async fn test_text_search_where_rowid_is_search_column() -> Result<(), anyhow::Error> {
    run_search(
        AppBuilder::new("search_app")
            .with_dataset(get_mega_science_dataset(
                Some("qs"),
                None,
                Some(Column::new("answer").with_full_text_search(FullTextSearchConfig::enabled().with_row_id("answer"))),
            ))
            .build(),
        vec![
            SearchTestCase::new(
                "test_text_search_where_rowid_is_search_column_basic",
                SearchTestType::Http(json!({
                    "text": "second",
                    "limit": 4,
                    "datasets": ["qs"],
                })),
            ),
            SearchTestCase::new(
                "test_text_search_sql_where_rowid_is_search_column_basic",
                SearchTestType::Sql("SELECT id, answer, trunc(score, 3) FROM text_search(qs, 'second') order by score desc LIMIT 4"),
            ),
        ]
    )
    .await
}

#[tokio::test]
async fn test_text_search_where_rowid_is_search_column_multi_column() -> Result<(), anyhow::Error> {
    run_search(
        AppBuilder::new("search_app")
            .with_dataset(get_mega_science_dataset(
                Some("qs"),
                Some(
                    Column::new("question").with_full_text_search(
                        FullTextSearchConfig::enabled().with_row_id("answer"),
                    ),
                ),
                Some(
                    Column::new("answer").with_full_text_search(
                        FullTextSearchConfig::enabled().with_row_id("answer"),
                    ),
                ),
            ))
            .build(),
        vec![SearchTestCase::new(
            "test_text_search_where_rowid_is_search_column_multi_column",
            SearchTestType::Http(json!({
                "text": "second",
                "limit": 4,
                "datasets": ["qs"],
            })),
        )],
    )
    .await
}

#[tokio::test]
async fn test_text_search_where_rowid_is_search_column_composite_pk() -> Result<(), anyhow::Error> {
    run_search(
        AppBuilder::new("search_app")
            .with_dataset(get_mega_science_dataset(
                Some("qs"),
                None,
                Some(
                    Column::new("answer").with_full_text_search(
                        FullTextSearchConfig::enabled().with_row_id("answer").with_row_id("id"),
                    ),
                ),
            ))
            .build(),
        vec![
            SearchTestCase::new(
                "test_text_search_where_rowid_is_search_column_composite_pk_basic",
                SearchTestType::Http(json!({
                    "text": "second",
                    "limit": 4,
                    "datasets": ["qs"],
                })),
            ),
            SearchTestCase::new(
                "test_text_search_sql_where_rowid_is_search_column_composite_pk_basic",
                SearchTestType::Sql("SELECT id, answer, trunc(score, 3) FROM text_search(qs, 'second') order by score desc LIMIT 4"),
            ),
        ],
    )
    .await
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn test_text_search_multiple_columns() -> Result<(), anyhow::Error> {
    run_search(
        AppBuilder::new("search_app")
            .with_dataset(get_mega_science_dataset(
                Some("qs"),
                Some(Column::new("question").with_full_text_search(FullTextSearchConfig::enabled().with_row_id("id"))),
                Some(Column::new("answer").with_full_text_search(FullTextSearchConfig::enabled().with_row_id("id"))),

            ))
            .build(),
        vec![
            SearchTestCase::new(
                "multi_text_column_basic",
                SearchTestType::Http(json!({
                    "text": "second",
                    "limit": 4,
                    "datasets": ["qs"],
                })),
            ),
            SearchTestCase::new(
                "multi_text_column_additional_columns",
                SearchTestType::Http(json!({
                    "text": "second",
                    "limit": 4,
                    "datasets": ["qs"],
                    "additional_columns": ["question"],
                })),
            ),
            SearchTestCase::new(
                "multi_text_column_with_where",
                SearchTestType::Http(json!({
                    "text": "secondary",
                    "datasets": ["qs"],
                    "where": "subject!='math'",
                    "limit": 4,
                })),
            ),
            SearchTestCase::new(
                "multi_text_column_sql_text_search_basic_answer",
                SearchTestType::Sql("SELECT id, answer, trunc(score, 3) FROM text_search(qs, 'second', answer) order by score desc LIMIT 4"),
            ),
            SearchTestCase::new(
                "multi_text_column_sql_text_search_basic_question",
                SearchTestType::Sql("SELECT id, question, trunc(score, 3) FROM text_search(qs, 'angles', question) order by score desc LIMIT 4"),
            ),
            SearchTestCase::new(
                // When there are multiple columns, `text_search` needs column explicitly as input.
                "multi_text_column_sql_text_search_error_without_column",
                SearchTestType::Sql("SELECT id, answer, trunc(score, 3) FROM text_search(qs, 'second') order by score desc LIMIT 4"),
            ).should_fail(),
            SearchTestCase::new(
                "multi_text_column_sql_text_search_projection",
                SearchTestType::Sql("SELECT id, answer, question, subject, trunc(score, 3) as score FROM text_search(qs, 'second', answer) order by score desc LIMIT 4"),
            ),
            SearchTestCase::new(
                "multi_text_column_sql_text_search_filters",
                SearchTestType::Sql("SELECT id, answer, trunc(score, 3) as score FROM text_search(qs, 'secondary', answer) where subject!='math' order by score desc LIMIT 4"),
            ),
            SearchTestCase::new(
                "multi_text_column_sql_text_search_no_score",
                SearchTestType::Sql("SELECT id, answer FROM text_search(qs, 'second', answer) order by score desc LIMIT 4"),
            ),
            SearchTestCase::new(
                "multi_text_column_sql_text_search_random",
                SearchTestType::Sql("SELECT subject FROM text_search(qs, 'second', answer) order by score desc LIMIT 4"),
            ),
        ],
    )
    .await
}

#[cfg(feature = "flightsql")]
#[tokio::test]
async fn test_multi_column_w_existing_embedding() -> Result<(), anyhow::Error> {
    use spicepod::{acceleration::Acceleration, param::Params};

    let api_config = start_app(
        AppBuilder::new("search_app")
            .with_dataset(catalog_page_tpcds_dataset_w_embeddings(
                "single_column",
                "hf_minilm",
                Some(vec!["cp_catalog_page_sk".to_string()]),
                None,
            ))
            .with_embedding(get_model_to_vec_embeddings(
                "minishlab/potion-base-2M",
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
            embeddings: vec![
                ColumnLevelEmbeddingConfig::model("hf_minilm").with_row_id("cp_catalog_page_sk"),
            ],
            metadata: HashMap::new(),
        },
        Column {
            name: "cp_department".to_string(),
            description: Some("This column is newly embedded in this spice app".to_string()),
            full_text_search: None,
            embeddings: vec![
                ColumnLevelEmbeddingConfig::model("hf_minilm").with_row_id("cp_catalog_page_sk"),
            ],
            metadata: HashMap::new(),
        },
    ];
    let app2 = AppBuilder::new("search_app2")
        .with_dataset(ds)
        .with_embedding(get_model_to_vec_embeddings(
            "minishlab/potion-base-2M",
            "hf_minilm",
        ))
        .build();

    run_search(
        app2,
        vec![
            SearchTestCase::new(
                "multi_embedding_parent_child_basic",
                SearchTestType::Http(json!({
                    "text": "new patient",
                    "limit": 2,
                    "datasets": ["multiple_columns"]
                })),
            ),
            SearchTestCase::new(
                "multi_embedding_parent_child_additional",
                SearchTestType::Http(json!({
                    "text": "new patient",
                    "limit": 2,
                    "datasets": ["multiple_columns"],
                    "additional_columns": ["cp_catalog_number"],
                })),
            ),
            SearchTestCase::new(
                "multi_embedding_parent_child_where",
                SearchTestType::Http(json!({
                    "text": "new patient",
                    "datasets": ["multiple_columns"],
                    "where": "cp_catalog_page_sk % 2 = 0 and cp_catalog_page_sk >=20"
                })),
            ),
        ],
    )
    .await
}

#[tokio::test]
async fn test_search_with_cache() -> Result<(), anyhow::Error> {
    let chunked = catalog_page_tpcds_dataset_w_embeddings(
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
        item_ttl: Some("30s".to_string()),
        max_size: Some("512mb".to_string()),
        ..Default::default()
    };

    let app = AppBuilder::new("cached_search")
        .with_dataset(chunked)
        .with_embedding(get_model_to_vec_embeddings(
            "minishlab/potion-base-32M",
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
            run_search_test(http_base_url.as_str(), &SearchTestCase::new(
                "with_cache_pre_cache",
                SearchTestType::Http(json!({
                    "text": "new patient",
                    "limit": 50,
                })),
            ), None, false).await?;
            let duration = start.elapsed();
            let mut measured_cache_times = Vec::new();
            for _ in 0..10 {
                let start = Instant::now();
                run_search_test(http_base_url.as_str(), &SearchTestCase::new(
                    "with_cache_post_cache",
                    SearchTestType::Http(json!({
                        "text": "new patient",
                        "limit": 50,
                    })),
                ), None, false).await?;
                let duration_cached = start.elapsed();
                measured_cache_times.push(duration_cached);
            }

            // take the median time from the cached responses
            measured_cache_times.sort();
            let duration_cached = measured_cache_times[measured_cache_times.len() / 2];

            assert!(duration_cached * 10 < duration, "Cache did not improve performance by an order of magnitude. First: {duration:?}, Second: {duration_cached:?}");
            Ok(())
        })
        .await
}

#[tokio::test]
async fn test_search_with_cache_bypass() -> Result<(), anyhow::Error> {
    let chunked = catalog_page_tpcds_dataset_w_embeddings(
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
        .with_embedding(get_model_to_vec_embeddings(
            "minishlab/potion-base-2M",
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
            run_search_test(http_base_url.as_str(), &SearchTestCase::new(
                "with_cache_bypass_pre_cache",
                SearchTestType::Http(json!({
                    "text": "new patient",
                    "limit": 2,
                })),
            ), Some(bypass_headers.clone()), false).await?;
            let duration = start.elapsed().as_secs_f64();
            let start = Instant::now();
            run_search_test(http_base_url.as_str(), &SearchTestCase::new(
                "with_cache_bypass_post_cache",
                SearchTestType::Http(json!({
                    "text": "new patient",
                    "limit": 2,
                })),
            ), Some(bypass_headers), false).await?;
            let duration_cached = start.elapsed().as_secs_f64();

            assert!(duration >= duration_cached*0.7 || duration <= duration_cached*1.3,
                "Cache bypass did not return similar performance. First: {duration:?}, Second: {duration_cached:?}");
            Ok(())
        })
        .await
}

#[tokio::test]
async fn test_vector_search_limit_plans() -> Result<(), anyhow::Error> {
    let ds = catalog_page_tpcds_dataset_w_embeddings(
        "basic_embedding_search",
        "hf_minilm",
        Some(vec!["cp_catalog_page_sk".to_string()]),
        None,
    );

    let app = AppBuilder::new("search_app")
        .with_dataset(ds)
        .with_embedding(get_model_to_vec_embeddings(
            "minishlab/potion-base-2M",
            "hf_minilm",
        ))
        .build();

    let queries = vec![
        (
            "EXPLAIN SELECT cp_catalog_page_sk, score FROM vector_search(spice.public.basic_embedding_search, 'basic') order by score desc LIMIT 4".to_string(),
            vec!["SortPreservingMergeExec: [score@1 DESC], fetch=4"]
        ),
        (
            "EXPLAIN SELECT cp_catalog_page_sk, score FROM vector_search(spice.public.basic_embedding_search, 'basic', 2) order by score desc LIMIT 4".to_string(),
            vec!["SortPreservingMergeExec: [score@1 DESC], fetch=4", "SortExec: TopK(fetch=2)"]
        ),
        (
            "EXPLAIN SELECT cp_catalog_page_sk, score FROM vector_search(spice.public.basic_embedding_search, 'basic', 3) order by score desc".to_string(),
            vec!["SortExec: TopK(fetch=3)"]
        )
    ];

    let api_config = start_app(app).await?;
    let http_base_url = format!("http://{}", api_config.http_bind_address);

    for (query, must_contain) in queries {
        let result = http_sql(http_base_url.as_str(), &query).await?;
        let result_str = result
            .as_array()
            .and_then(|o| o.last())
            .and_then(|v| v.as_object())
            .and_then(|v| v.get("plan"))
            .and_then(|v| v.as_str())
            .expect("Must read physical plan");

        assert!(must_contain.iter().all(|p| result_str.contains(p)));
    }

    Ok(())
}
