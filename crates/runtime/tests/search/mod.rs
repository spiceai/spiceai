/*
Copyright 2025 The Spice.ai OSS Authors

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

//! Search integration tests for Spice runtime.
//!
//! Each test function runs a single configured `spicepod.yaml` and a single data source. Spice runtime configurations
//! are defined in YAML, and test functions are generated (at `build.rs`, into `generated_search_tests.rs`) for all
//!  combinations of:
//!   - Acceleration, in `acceleration.yaml`
//!   - Vector Store, in `vector_store.yaml`
//!
//! In combination with, for each source of data (currently only `./megascience`):
//!   - Search tables, in `megascience/tables.yaml`. Either a [`spicepod::component::View`] or
//!     [`spicepod::component::Dataset`] component. A data source might require multiple (e.g. a view atop a dataset),
//!     but tests are run on one table (`.[].table_name` in YAML).
//!   - Column configurations, in `megascience/columns.yaml`.
//!   - Test cases, in `megascience/tests.yaml`
//!
//! All tests are run only with the `extended_tests` feature flag.

use anyhow::Context;
use app::{App, AppBuilder};
use arrow::array::RecordBatch;
use futures::TryStreamExt;
use http::{
    HeaderValue,
    header::{ACCEPT, CONTENT_TYPE},
};
use reqwest::header::HeaderMap;
use rstest::rstest;
use runtime::{Runtime, auth::EndpointAuth, config::Config};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use spicepod::{
    acceleration::{Acceleration, Mode},
    component::embeddings::Embeddings,
    param::ParamValue,
    vector::VectorStore,
};
use std::{
    cmp::Ordering,
    collections::HashMap,
    fmt::{self, Display},
    hash::{DefaultHasher, Hash, Hasher},
    sync::Arc,
};

use super::models::sort_json_keys;
use crate::{
    DEFAULT_TRACING_MODELS, configure_test_datafusion, init_tracing,
    models::{create_api_bindings_config, http_post},
    search::{
        s3_vectors::prepare_for_aws_tests,
        tables::{SearchTable, enrich_table},
    },
    utils::{init_tracing_with_task_history, runtime_ready_check, test_request_context},
};

pub mod megascience;
mod s3_vectors;
mod tables;

#[derive(Clone, Serialize, Debug, Deserialize)]
#[serde(untagged)]
pub enum SearchTestType {
    Sql(String),
    Http(serde_json::Value),
}

impl Display for SearchTestType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SearchTestType::Http(value) => write!(f, "{value}"),
            SearchTestType::Sql(query) => write!(f, "{query}"),
        }
    }
}

#[derive(Clone, Serialize, Debug, Deserialize)]
pub struct SearchTestCase {
    pub name: String,

    #[serde(rename = "input")]
    pub body: SearchTestType,

    #[serde(default)]
    pub should_fail: bool,
    #[serde(default)]
    pub skip: bool,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum AccelerationOptions {
    NoAcceleration,
    Arrow,
    DuckDb,
    DuckDbFile,
    Cayenne,
}

impl fmt::Display for AccelerationOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            AccelerationOptions::NoAcceleration => "no_acceleration",
            AccelerationOptions::Arrow => "arrow",
            AccelerationOptions::DuckDb => "duckdb",
            AccelerationOptions::DuckDbFile => "duckdb_file",
            AccelerationOptions::Cayenne => "cayenne",
        };
        write!(f, "{s}")
    }
}

impl AccelerationOptions {
    /// Converts to Spicepod [`Acceleration`] configuration.
    ///
    /// `unique_id` enables accelerations to set unique filepaths, when needed.
    fn to_acceleration(&self, unique_id: &str) -> Acceleration {
        match self {
            AccelerationOptions::NoAcceleration => Acceleration {
                enabled: false,
                ..Default::default()
            },
            AccelerationOptions::Arrow => Acceleration {
                enabled: true,
                engine: Some("arrow".to_string()),
                ..Default::default()
            },
            AccelerationOptions::DuckDb => Acceleration {
                enabled: true,
                engine: Some("duckdb".to_string()),
                ..Default::default()
            },
            AccelerationOptions::DuckDbFile => Acceleration {
                enabled: true,
                engine: Some("duckdb".to_string()),
                mode: Mode::File,
                params: Some(spicepod::param::Params::from_string_map(HashMap::from([(
                    "duckdb_file_path".to_string(),
                    format!(".spice/data/duckdb_acceleration_{unique_id}.db"),
                )]))),
                ..Default::default()
            },
            AccelerationOptions::Cayenne => Acceleration {
                enabled: true,
                engine: Some("cayenne".to_string()),
                mode: Mode::File,
                params: Some(spicepod::param::Params::from_string_map(HashMap::from([(
                    "cayenne_file_path".to_string(),
                    format!(".spice/data/cayenne_acceleration_{unique_id}/"),
                )]))),
                ..Default::default()
            },
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum VectorEngineOptions {
    NoVectorEngine,
    S3Vectors,
}

impl fmt::Display for VectorEngineOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            VectorEngineOptions::NoVectorEngine => "no_vector_engine",
            VectorEngineOptions::S3Vectors => "s3_vectors",
        };
        write!(f, "{s}")
    }
}

impl VectorEngineOptions {
    fn to_vector_store(&self) -> VectorStore {
        match self {
            VectorEngineOptions::NoVectorEngine => VectorStore {
                enabled: false,
                ..Default::default()
            },
            VectorEngineOptions::S3Vectors => VectorStore {
                enabled: true,
                engine: Some("s3_vectors".to_string()),
                params: Some(spicepod::param::Params::from_string_map(HashMap::from([
                    ("s3_vectors_aws_region".to_string(), "us-east-2".to_string()),
                    (
                        "s3_vectors_bucket".to_string(),
                        "spice-ci-tests-s3-vectors".to_string(),
                    ),
                    (
                        "s3_vectors_aws_access_key_id".to_string(),
                        "${ env:AWS_S3_VECTORS_KEY }".to_string(),
                    ),
                    (
                        "s3_vectors_aws_secret_access_key".to_string(),
                        "${ env:AWS_S3_VECTORS_SECRET }".to_string(),
                    ),
                ]))),
                ..Default::default()
            },
        }
    }
}

enum EmbeddingModels {
    Model2Vec8m,
    Model2Vec,
}

impl EmbeddingModels {
    fn all() -> Vec<Self> {
        vec![EmbeddingModels::Model2Vec8m, EmbeddingModels::Model2Vec]
    }
    fn to_app_embedding(&self) -> Embeddings {
        match self {
            EmbeddingModels::Model2Vec8m => {
                Embeddings::new("model2vec:minishlab/potion-base-8M", "openai_embeddings")
            }
            EmbeddingModels::Model2Vec => {
                Embeddings::new("model2vec:minishlab/potion-base-2M", "hf_minilm")
            }
        }
    }
}

#[rstest]
#[tokio::test]
#[cfg_attr(
    not(feature = "extended_tests"),
    ignore = "Extended test - run with --features extended_tests"
)]
async fn test_megascience_permutations(
    #[values(VectorEngineOptions::NoVectorEngine)] vector_engine: VectorEngineOptions,
    #[values(
        AccelerationOptions::NoAcceleration,
        AccelerationOptions::Arrow,
        AccelerationOptions::DuckDb,
        AccelerationOptions::DuckDbFile
    )]
    acceleration_opt: AccelerationOptions,
    #[values(
        megascience::TableOptions::Dataset,
        megascience::TableOptions::ViewUnionAllJoin
    )]
    table_option: megascience::TableOptions,

    #[values(
        megascience::ColumnConfigOptions::Basic,
        megascience::ColumnConfigOptions::MultiColumn,
        megascience::ColumnConfigOptions::HybridSingleColumn,
        megascience::ColumnConfigOptions::HybridMultipleColumn,
        megascience::ColumnConfigOptions::TextSearch,
        megascience::ColumnConfigOptions::MultiTextColumn,
        megascience::ColumnConfigOptions::TextSearchMetadata,
        megascience::ColumnConfigOptions::MultiEmbeddings
    )]
    column_config: megascience::ColumnConfigOptions,
) {
    let slug =
        format!("{acceleration_opt}-{vector_engine}-{table_option}-{column_config}_megascience");
    if let Err(e) = validate_combination(
        &vector_engine,
        &acceleration_opt,
        &table_option,
        &column_config,
    ) {
        tracing::info!("Skipping test {slug}. {e}");
        return;
    }

    let columns = column_config.to_columns();

    // use some hash of slug
    let mut z = DefaultHasher::new();
    slug.hash(&mut z);
    let acceleration = acceleration_opt.to_acceleration(&z.finish().to_string());

    let mut app = AppBuilder::new(slug);
    let (views, datasets) = table_option.to_tables();

    // Prepare vector store for AWS tests if needed.
    let mut vector_store = vector_engine.to_vector_store();
    prepare_for_aws_tests(&vector_store, vector_store.enabled)
        .await
        .expect("could not prepare vector store for tests");

    // Update vector store params with dynamic values as needed.
    if vector_store.engine.as_deref() == Some("s3_vectors")
        && let Some(params) = vector_store.params.as_mut()
    {
        params.data.insert(
            "s3_vectors_index".to_string(),
            ParamValue::String(format!(
                "{}-{}-{}-{}",
                acceleration_opt,
                table_option.to_string().replace('_', "-"),
                column_config.to_string().replace('_', "-"),
                rand::random::<u8>() % 11
            )),
        );
    }

    let (views, datasets) = enrich_table(
        SearchTable {
            table_name: table_option.table_to_search_on().to_string(),
            datasets,
            views,
        },
        columns,
        Some(vector_store),
        &acceleration,
    );

    for model in EmbeddingModels::all() {
        app = app.with_embedding(model.to_app_embedding());
    }

    for ds in datasets {
        app = app.with_dataset(ds);
    }

    for v in views {
        app = app.with_view(v);
    }

    run_search(
        app.build(),
        megascience::TestCases::all()
            .into_iter()
            .map(|tc| SearchTestCase {
                name: format!("{tc}"),
                body: tc.to_input(),
                should_fail: false,
                skip: false,
            })
            .collect(),
    )
    .await
    .expect("failed to run search tests");
}

fn validate_combination(
    _vector_engine: &VectorEngineOptions,
    acceleration_opt: &AccelerationOptions,
    table_option: &megascience::TableOptions,
    column_config: &megascience::ColumnConfigOptions,
) -> Result<(), String> {
    if matches!(
        (&table_option, &acceleration_opt),
        (
            megascience::TableOptions::ViewUnionAllJoin,
            AccelerationOptions::NoAcceleration
        )
    ) {
        return Err("Cannot have view with no acceleration".to_string());
    }
    if matches!(&acceleration_opt, AccelerationOptions::NoAcceleration) && column_config.is_fts() {
        return Err("Cannot have hybrid column with no acceleration".to_string());
    }
    Ok(())
}

async fn http_sql(base_url: &str, sql: &str) -> Result<Value, anyhow::Error> {
    let mut headers = HeaderMap::new();
    headers.insert(ACCEPT, HeaderValue::from_static("application/json"));
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("text/plain"));

    let response_str = http_post(&format!("{base_url}/v1/sql").to_string(), sql, headers).await?;
    serde_json::from_str(&response_str)
        .map_err(|e| anyhow::anyhow!("Failed to parse 'v1/sql' HTTP response: {e}"))
}

pub async fn run_search_test(
    app_name: &String,
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
        insta::assert_snapshot!(
            format!("{app_name}_{}_error_response", ts.name),
            err.to_string()
        );
        return Ok(());
    }

    let resp = serde_json::from_str(&resp?).context("Failed to parse HTTP response")?;
    insta::assert_snapshot!(
        format!("{app_name}_{}_response", ts.name),
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

// if `explain_sql`, for any [`SearchTestCase`] that is [`SearchTestType::Sql`], a snapshot will be taken of the associated explain query.
pub(crate) async fn run_search(
    app: App,
    test_cases: Vec<SearchTestCase>,
) -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(None);

    test_request_context()
        .scope(async {
            let app_name = app.name.clone();
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
                        run_search_test(
                            &app_name,
                            http_base_url.as_str(),
                            &ts,
                            None,
                            ts.should_fail,
                        )
                        .await?;
                    }
                    SearchTestType::Sql(sql) => {
                        let test_name = ts.name.clone();
                        let resp = http_sql(http_base_url.as_str(), &sql).await;
                        if ts.should_fail {
                            if resp.is_ok() {
                                return Err(anyhow::anyhow!(format!(
                                    "Test {test_name} was expected to fail but succeeded",
                                )));
                            }

                            let err = resp.err().context("Test was expected to fail")?;
                            insta::assert_snapshot!(
                                format!("{app_name}_{test_name}_error_response"),
                                err.to_string()
                            );
                            continue;
                        }
                        let resp = match resp {
                            Ok(v) => v,
                            Err(e) => Value::String(e.to_string()),
                        };
                        insta::with_settings!({
                            omit_expression => true,
                            description => sql.clone()
                        }, {
                            insta::assert_json_snapshot!(format!("{app_name}_{test_name}"), resp);
                        });

                        // This is okay to fail. Some times SQL plans cannot be prepared (e.g. FTS on a vector index).
                        // Do not return error, but make a snapshot to ensure if this changes in future, we can track it.
                        let disp =
                            if let Ok(c) = client.query(format!("EXPLAIN {sql}").as_str()).await {
                                let z = c.try_collect::<Vec<RecordBatch>>().await?;
                                arrow::util::pretty::pretty_format_batches(&z)?.to_string()
                            } else {
                                format!("Could not prepare EXPLAIN plan. SQL error: {resp}")
                            };
                        insta::with_settings!({
                            omit_expression => true,
                            description => sql
                        }, {
                            insta::assert_snapshot!(format!("{app_name}_{test_name}_explain"), disp);
                        });
                    }
                }
            }
            Ok(())
        })
        .await
}
