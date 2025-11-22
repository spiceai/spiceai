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
//!      [`spicepod::component::Dataset`] component. A data source might require multiple (e.g. a view atop a dataset),
//!       but tests are run on one table (`.[].table_name` in YAML).
//!   - Column configurations, in `megascience/columns.yaml`.
//!   - Test cases, in `megascience/tests.yaml`
//!
//! All tests are run only with the `extended_tests` feature flag.

use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
    fmt::Display,
    sync::{Arc, LazyLock},
};

use anyhow::Context;
use app::{App, AppBuilder};
use arrow::array::RecordBatch;
use futures::TryStreamExt;
use http::{
    HeaderValue,
    header::{ACCEPT, CONTENT_TYPE},
};
use itertools::Itertools;
use reqwest::header::HeaderMap;
use runtime::{Runtime, auth::EndpointAuth, config::Config};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use spicepod::{
    acceleration::Acceleration, component::embeddings::Embeddings, param::ParamValue,
    semantic::Column, vector::VectorStore,
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

// The spicepod fields important in testing search
pub struct SearchSpicepodConfiguration {
    acceleration: Acceleration,
    vector: Option<VectorStore>,
    table_component: SearchTable,
    columns: Vec<Column>,
}

static TABLE_ACCELERATION_OPTIONS: LazyLock<HashMap<String, Acceleration>> = LazyLock::new(|| {
    serde_yaml::from_str(include_str!("acceleration.yaml"))
        .expect("Failed to parse 'acceleration.yaml' configurations")
});

static TABLE_VECTOR_STORE_OPTIONS: LazyLock<HashMap<String, VectorStore>> = LazyLock::new(|| {
    serde_yaml::from_str(include_str!("vector_store.yaml"))
        .expect("Failed to parse 'acceleration.yaml' configurations")
});

static EMBEDDING_MODEL_OPTIONS: LazyLock<Vec<Embeddings>> = LazyLock::new(|| {
    // `"embeddings.yaml"` has "embeddings" as the top-level key to match spicepod.yaml semantics (but is not full spicepod.yaml).
    let yaml_format: HashMap<String, Vec<Embeddings>> =
        serde_yaml::from_str(include_str!("embeddings.yaml"))
            .expect("Failed to parse 'acceleration.yaml' configurations");

    yaml_format.get("embeddings").cloned().unwrap_or_default()
});

static MEGA_SCIENCE_COLUMN_CONFIGS: LazyLock<HashMap<String, Vec<Column>>> = LazyLock::new(|| {
    serde_yaml::from_str(include_str!("megascience/columns.yaml"))
        .expect("Failed to parse 'mega_science/columns.yaml' column configurations")
});

static MEGA_SCIENCE_TABLES: LazyLock<HashMap<String, SearchTable>> = LazyLock::new(|| {
    serde_yaml::from_str(include_str!("megascience/tables.yaml"))
        .expect("Failed to parse 'mega_science/tables.yaml' column configurations")
});

static MEGA_SCIENCE_TESTS: LazyLock<Vec<SearchTestCase>> = LazyLock::new(|| {
    serde_yaml::from_str(include_str!("megascience/tests.yaml"))
        .expect("Failed to parse 'mega_science/tests.yaml' test cases")
});

impl SearchSpicepodConfiguration {
    pub(super) fn from_str(
        id: &str,
        column_configs: &HashMap<String, Vec<Column>>,
        search_tables: &HashMap<String, SearchTable>,
    ) -> Result<Self, anyhow::Error> {
        let Some([engine, vector, table_component, column_configuration]) =
            id.split('-').collect_array()
        else {
            return Err(anyhow::anyhow!("Invalid search spicepod slug: '{id}'."));
        };
        let Some(acceleration) = TABLE_ACCELERATION_OPTIONS.get(engine).cloned() else {
            return Err(anyhow::anyhow!(
                "Invalid acceleration option '{column_configuration}' in search spicepod slug."
            ));
        };

        let Some(mut vector_store) = TABLE_VECTOR_STORE_OPTIONS.get(vector).cloned() else {
            return Err(anyhow::anyhow!(
                "Invalid acceleration option '{column_configuration}' in search spicepod slug."
            ));
        };

        let Some(search_table) = search_tables.get(table_component) else {
            return Err(anyhow::anyhow!(
                "Invalid acceleration option '{column_configuration}' in search spicepod slug."
            ));
        };

        // Update vector store params with dynamic values as needed.
        match vector_store.engine.as_deref() {
            Some("s3_vectors") => {
                if let Some(params) = vector_store.params.as_mut() {
                    params.data.insert(
                        "s3_vectors_index".to_string(),
                        ParamValue::String(format!(
                            "{engine}-{}-{}-{}",
                            table_component.replace("_", "-"),
                            column_configuration.replace("_", "-"),
                            rand::random::<u8>() % 11
                        )),
                    );
                }
            }
            _ => {}
        };

        let Some(columns) = column_configs.get(column_configuration).cloned() else {
            return Err(anyhow::anyhow!(
                "Invalid column configuration field '{column_configuration}' in search spicepod slug."
            ));
        };

        Ok(SearchSpicepodConfiguration {
            acceleration,
            vector: Some(vector_store),
            table_component: search_table.clone(),
            columns,
        })
    }

    pub fn embedding_models_used(
        &self,
        models_available: &[Embeddings],
    ) -> Result<Vec<Embeddings>, anyhow::Error> {
        let mut embedding_names = HashSet::new();

        for col in &self.columns {
            for clec in &col.embeddings {
                embedding_names.insert(clec.model.clone());
            }
        }
        embedding_names
            .iter()
            .map(|name| {
                let Some(model) = models_available.iter().find(|m| m.name == *name) else {
                    return Err(anyhow::anyhow!(
                        "Embedding model '{}' not found among available models.",
                        name
                    ));
                };
                Ok(model.clone())
            })
            .collect()
    }
}

macro_rules! generate_search_tests {
    ([$($slug:expr),* $(,)?]) => {
        paste::paste! {
            $(
                #[tokio::test]
                #[cfg_attr(
                    not(feature = "extended_tests"),
                    ignore = "Extended test - run with --features extended_tests"
                )]
                #[allow(non_snake_case)]
                async fn [<test_search_ $slug:snake>]() {
                    megascience_search_test_case($slug).await;
                }
            )*
        }
    };
}

async fn megascience_search_test_case(slug: &'static str) {
    let mut app = AppBuilder::new(slug);
    let cfg = SearchSpicepodConfiguration::from_str(
        slug,
        &MEGA_SCIENCE_COLUMN_CONFIGS,
        &MEGA_SCIENCE_TABLES,
    )
    .expect("could not initialise configuration");

    for emb in cfg
        .embedding_models_used(&EMBEDDING_MODEL_OPTIONS)
        .expect("could not find embedding models")
    {
        app = app.with_embedding(emb);
    }
    let SearchSpicepodConfiguration {
        columns,
        acceleration,
        table_component,
        vector,
    } = cfg;

    if let Some(v) = vector.as_ref() {
        prepare_for_aws_tests(v, v.enabled)
            .await
            .expect("could not prepare vector store for tests");
    }

    let (views, datasets) = enrich_table(table_component, columns, vector, &acceleration);

    for ds in datasets {
        app = app.with_dataset(ds);
    }

    for v in views {
        app = app.with_view(v);
    }

    run_search(app.build(), MEGA_SCIENCE_TESTS.clone())
        .await
        .expect("failed to run search tests");
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
            format!("{app_name}_megascience_{}_error_response", ts.name),
            err.to_string()
        );
        return Ok(());
    }

    let resp = serde_json::from_str(&resp?).context("Failed to parse HTTP response")?;
    insta::assert_snapshot!(
        format!("{}_megascience_{}_response", app_name, ts.name),
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
                        run_search_test(&app_name, http_base_url.as_str(), &ts, None, ts.should_fail).await?;
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
                                format!("{app_name}_megascience_{test_name}_error_response"),
                                err.to_string()
                            );
                            continue;
                        }

                        insta::assert_json_snapshot!(test_name.clone(), resp?);

                        let c = client
                            .query(format!("EXPLAIN {sql}").as_str())
                            .await?
                            .try_collect::<Vec<RecordBatch>>()
                            .await?;

                        let disp = arrow::util::pretty::pretty_format_batches(&c)?;

                        insta::with_settings!({
                            omit_expression => true,
                            description => sql
                        }, {insta::assert_snapshot!(format!("{app_name}_megascience_{test_name}_explain"), disp)});
                    }
                }
            }
            Ok(())
        })
        .await
}

// Test patterns are expanded at build time by `build.rs` (see `build_search_test_cases`).
// Requires the existance of `generate_search_tests` macro wherever it is included.
include!("generated_search_tests.rs");
