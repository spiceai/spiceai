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
    acceleration::{Acceleration, Mode, ZeroResultsAction},
    component::embeddings::Embeddings,
    fts::FtsStore,
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
#[cfg(feature = "s3_vectors")]
use crate::search::s3_vectors::prepare_for_aws_tests;
#[cfg(feature = "s3_vectors")]
use crate::utils::verify_env_secret_exists;
use crate::{
    DEFAULT_TRACING_MODELS, configure_test_datafusion, init_tracing,
    models::{create_api_bindings_config, http_post, search::replace_s3_vector_index_names},
    search::tables::{SearchTable, enrich_table},
    utils::{
        init_tracing_with_task_history, register_test_connectors, runtime_ready_check,
        test_request_context,
    },
};

mod elasticsearch;
pub mod megascience;
#[cfg(feature = "s3_vectors")]
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
    /// Cayenne acceleration with `on_zero_results: use_source`, so a warm search
    /// tier that returns nothing falls through to the external vector engine index.
    CayenneWithZeroResults,
}

impl fmt::Display for AccelerationOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            AccelerationOptions::NoAcceleration => "no_acceleration",
            AccelerationOptions::Arrow => "arrow",
            AccelerationOptions::DuckDb => "duckdb",
            AccelerationOptions::DuckDbFile => "duckdb_file",
            AccelerationOptions::Cayenne => "cayenne",
            AccelerationOptions::CayenneWithZeroResults => "cayenne_with_zero_results",
        };
        write!(f, "{s}")
    }
}

impl AccelerationOptions {
    /// Converts to Spicepod [`Acceleration`] configuration.
    ///
    /// `unique_id` enables accelerations to set unique filepaths, when needed. `table_name` is the
    /// accelerated dataset name, used to build `refresh_sql` for the zero-results fallback variant.
    fn to_acceleration(&self, unique_id: &str, table_name: &str) -> Acceleration {
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
                    "duckdb_file".to_string(),
                    format!(".spice/data/duckdb_acceleration_{unique_id}.db"),
                )]))),
                ..Default::default()
            },
            AccelerationOptions::Cayenne | AccelerationOptions::CayenneWithZeroResults => {
                let with_zero_results = matches!(self, AccelerationOptions::CayenneWithZeroResults);
                Acceleration {
                    enabled: true,
                    engine: Some("cayenne".to_string()),
                    mode: Mode::File,
                    // `CayenneWithZeroResults` opts the warm tier into falling through to the
                    // external vector engine index when it returns no results, and refreshes zero
                    // rows so the warm tier stays empty and every search takes that fallback path.
                    on_zero_results: if with_zero_results {
                        ZeroResultsAction::UseSource
                    } else {
                        ZeroResultsAction::default()
                    },
                    refresh_sql: with_zero_results
                        .then(|| format!("SELECT * FROM {table_name} LIMIT 0")),
                    params: Some(spicepod::param::Params::from_string_map(HashMap::from([
                        (
                            "cayenne_metadata_dir".to_string(),
                            format!(".spice/metadata/cayenne_acceleration_{unique_id}/"),
                        ),
                        (
                            "cayenne_file_path".to_string(),
                            format!(".spice/data/cayenne_acceleration_{unique_id}/"),
                        ),
                    ]))),
                    ..Default::default()
                }
            }
        }
    }
}

/// Name of the environment variable holding the ARN of the long-lived, pre-seeded S3 Vectors index
/// that [`VectorEngineOptions::S3VectorsFallback`] targets.
///
/// The index must be created and populated out of band (the zero-row acceleration writes nothing),
/// and it is referenced by ARN precisely because an ARN-identified index is never created or
/// written to by the runtime — so the persisted vectors survive across runs for the warm-tier
/// fallback to read.
const S3_VECTORS_FALLBACK_ARN_ENV: &str = "AWS_S3_VECTORS_FALLBACK_INDEX_ARN";

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum VectorEngineOptions {
    NoVectorEngine,
    DuckDb,
    S3Vectors,
    /// S3 Vectors backed by a long-lived, never-deleted index, used to exercise the warm-tier
    /// fallback path: when the warm tier is empty, searches fall through to this persisted index.
    S3VectorsFallback,
}

impl fmt::Display for VectorEngineOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            VectorEngineOptions::NoVectorEngine => "no_vector_engine",
            VectorEngineOptions::DuckDb => "duckdb",
            VectorEngineOptions::S3Vectors => "s3_vectors",
            VectorEngineOptions::S3VectorsFallback => "s3_vectors_fallback",
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
            VectorEngineOptions::DuckDb => VectorStore {
                enabled: true,
                engine: Some("duckdb".to_string()),
                ..Default::default()
            },
            VectorEngineOptions::S3Vectors | VectorEngineOptions::S3VectorsFallback => {
                let mut params = HashMap::from([
                    ("s3_vectors_aws_region".to_string(), "us-east-2".to_string()),
                    (
                        "s3_vectors_aws_access_key_id".to_string(),
                        "${ env:AWS_S3_VECTORS_KEY }".to_string(),
                    ),
                    (
                        "s3_vectors_aws_secret_access_key".to_string(),
                        "${ env:AWS_S3_VECTORS_SECRET }".to_string(),
                    ),
                ]);
                match self {
                    // The fallback engine targets a pre-seeded index by ARN. `s3_vectors_arn` is
                    // mutually exclusive with `s3_vectors_bucket`/`s3_vectors_index`, and an
                    // ARN-identified index is never created or written to by the runtime — so the
                    // zero-row acceleration leaves the persisted vectors intact to fall back on.
                    VectorEngineOptions::S3VectorsFallback => {
                        params.insert(
                            "s3_vectors_arn".to_string(),
                            format!("${{ env:{S3_VECTORS_FALLBACK_ARN_ENV} }}"),
                        );
                    }
                    // The per-permutation index name is injected by the caller
                    // (`test_megascience_permutations`); only the bucket is fixed here.
                    _ => {
                        params.insert(
                            "s3_vectors_bucket".to_string(),
                            "spice-ci-tests-s3-vectors".to_string(),
                        );
                    }
                }
                VectorStore {
                    enabled: true,
                    engine: Some("s3_vectors".to_string()),
                    params: Some(spicepod::param::Params::from_string_map(params)),
                    ..Default::default()
                }
            }
        }
    }
}

enum TextEngineOptions {
    NoTextEngine,
    Elasticsearch,
}

impl fmt::Display for TextEngineOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            TextEngineOptions::NoTextEngine => "no_text_engine",
            TextEngineOptions::Elasticsearch => "elasticsearch",
        };
        write!(f, "{s}")
    }
}

impl TextEngineOptions {
    /// Build an [`FtsStore`] pointing at `endpoint`.
    ///
    /// Pass the actual container endpoint (e.g. `http://localhost:19200`) for
    /// local Docker runs, or the CI service endpoint for CI.
    fn to_fts_store(&self, endpoint: &str) -> Option<FtsStore> {
        match self {
            TextEngineOptions::NoTextEngine => None,
            TextEngineOptions::Elasticsearch => Some(FtsStore {
                enabled: true,
                engine: Some("elasticsearch".to_string()),
                params: Some(spicepod::param::Params::from_string_map(
                    std::collections::HashMap::from([(
                        "endpoint".to_string(),
                        endpoint.to_string(),
                    )]),
                )),
            }),
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
    #[values(
        VectorEngineOptions::NoVectorEngine,
        VectorEngineOptions::DuckDb,
        VectorEngineOptions::S3Vectors,
        VectorEngineOptions::S3VectorsFallback
    )]
    vector_engine: VectorEngineOptions,
    #[values(TextEngineOptions::NoTextEngine, TextEngineOptions::Elasticsearch)]
    text_engine: TextEngineOptions,
    #[values(
        AccelerationOptions::NoAcceleration,
        AccelerationOptions::Arrow,
        AccelerationOptions::DuckDb,
        AccelerationOptions::DuckDbFile,
        AccelerationOptions::Cayenne,
        AccelerationOptions::CayenneWithZeroResults
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
        megascience::ColumnConfigOptions::MultiEmbeddings,
        megascience::ColumnConfigOptions::VectorSearchMetadata
    )]
    column_config: megascience::ColumnConfigOptions,
) {
    use runtime::spice_data_base_path;

    let slug = format!(
        "{acceleration_opt}-{vector_engine}-{text_engine}-{table_option}-{column_config}_megascience"
    );
    if let Err(e) = validate_combination(
        &vector_engine,
        &text_engine,
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
    std::fs::create_dir_all(spice_data_base_path()).expect("failed to create spice data base path");
    let unique_id = z.finish().to_string();
    let acceleration =
        acceleration_opt.to_acceleration(&unique_id, table_option.table_to_search_on());

    let mut app = AppBuilder::new(slug);
    let (views, datasets) = table_option.to_tables();

    // Prepare vector store for AWS tests if needed.
    let mut vector_store = vector_engine.to_vector_store();

    // Give the (non-fallback) S3 Vectors engine a unique per-permutation index name so parallel
    // runs don't clobber each other. The fallback engine deliberately keeps the ARN-identified,
    // long-lived index it set in `to_vector_store`, so a prior run's vectors remain to fall back to.
    if matches!(vector_engine, VectorEngineOptions::S3Vectors)
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
    // The fallback engine reads a pre-seeded index identified by ARN. Fail fast with an actionable
    // message if that ARN wasn't supplied, rather than surfacing an opaque secret-resolution error
    // once the runtime tries to load the vector store.
    #[cfg(feature = "s3_vectors")]
    if matches!(vector_engine, VectorEngineOptions::S3VectorsFallback) {
        verify_env_secret_exists(S3_VECTORS_FALLBACK_ARN_ENV)
            .await
            .unwrap_or_else(|e| {
                panic!(
                    "S3VectorsFallback requires the {S3_VECTORS_FALLBACK_ARN_ENV} environment variable to point at a pre-seeded S3 Vectors index ARN: {e}"
                )
            });
    }
    // The fallback engine reuses a long-lived index across runs, so it is never pre-deleted;
    // every other engine starts each test from a clean index.
    #[cfg(feature = "s3_vectors")]
    prepare_for_aws_tests(
        &vector_store,
        vector_store.enabled && !matches!(vector_engine, VectorEngineOptions::S3VectorsFallback),
    )
    .await
    .expect("could not prepare vector store for tests");

    // Start Elasticsearch Docker container if needed, and get the endpoint URL.
    // The container is kept alive for the duration of the test then dropped.
    let _es_container;
    let es_endpoint: String;
    if matches!(text_engine, TextEngineOptions::Elasticsearch) {
        // Pick a random high port to avoid collisions with other parallel test runs.
        let port = {
            use rand::RngExt;
            let mut rng = rand::rng();
            rng.random_range(19200_u16..19300_u16)
        };
        let container = elasticsearch::start_elasticsearch_docker_container(port)
            .await
            .expect("failed to start Elasticsearch Docker container");
        es_endpoint = elasticsearch::elasticsearch_endpoint(port);
        _es_container = Some(container);
    } else {
        es_endpoint = String::new();
        _es_container = None;
    }

    // Build the FTS store, injecting the live endpoint and a unique per-combination index name.
    let mut fts_store = text_engine.to_fts_store(&es_endpoint);
    if let Some(fts) = fts_store.as_mut()
        && fts.engine.as_deref() == Some("elasticsearch")
    {
        let params = fts
            .params
            .get_or_insert_with(spicepod::param::Params::default);
        // Unique index name per permutation so parallel runs don't clobber each other.
        params.data.insert(
            "index".to_string(),
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
        fts_store.as_ref(),
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
    vector_engine: &VectorEngineOptions,
    text_engine: &TextEngineOptions,
    acceleration_opt: &AccelerationOptions,
    table_option: &megascience::TableOptions,
    column_config: &megascience::ColumnConfigOptions,
) -> Result<(), String> {
    // The warm-tier fallback path is exercised by exactly one pairing: `CayenneWithZeroResults`
    // acceleration (an enabled warm tier whose `on_zero_results` falls through) over the
    // `S3VectorsFallback` engine (a long-lived index that survives across runs, so the fallback has
    // data to return). Bind the two to each other, and to a single table/column/text combination,
    // so the pairing stays one deterministic permutation.
    let fallback_acceleration = matches!(
        acceleration_opt,
        AccelerationOptions::CayenneWithZeroResults
    );
    let fallback_vector = matches!(vector_engine, VectorEngineOptions::S3VectorsFallback);
    if fallback_acceleration || fallback_vector {
        if fallback_acceleration != fallback_vector {
            return Err(
                "CayenneWithZeroResults and S3VectorsFallback are only tested together".to_string(),
            );
        }
        if !matches!(table_option, megascience::TableOptions::Dataset)
            || !matches!(column_config, megascience::ColumnConfigOptions::Basic)
            || !matches!(text_engine, TextEngineOptions::NoTextEngine)
        {
            return Err(
                "S3 Vectors fallback is tested only on the dataset/basic/no-text-engine combination"
                    .to_string(),
            );
        }
        return Ok(());
    }

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
    if matches!(text_engine, TextEngineOptions::Elasticsearch) && !column_config.is_fts() {
        return Err(
            "Elasticsearch text engine only applies to FTS column configurations".to_string(),
        );
    }
    if matches!(&vector_engine, VectorEngineOptions::DuckDb)
        && !matches!(
            &acceleration_opt,
            AccelerationOptions::DuckDb | AccelerationOptions::DuckDbFile
        )
    {
        return Err(
            "DuckDB vector engine requires DuckDB acceleration (duckdb or duckdb_file)".to_string(),
        );
    }
    if matches!(&vector_engine, VectorEngineOptions::S3Vectors)
        && !matches!(
            (&table_option, &acceleration_opt),
            (
                megascience::TableOptions::Dataset,
                AccelerationOptions::Arrow
                    | AccelerationOptions::DuckDb
                    | AccelerationOptions::Cayenne
            )
        )
    {
        return Err("S3 Vectors on reduced set of combinations".to_string());
    }
    if matches!(text_engine, TextEngineOptions::Elasticsearch) {
        if cfg!(not(feature = "elasticsearch")) {
            return Err(
                "Elasticsearch text engine tests require the elasticsearch feature".to_string(),
            );
        }
        if !column_config.is_fts() {
            return Err("Elasticsearch text engine tests require full-text columns".to_string());
        }
        if !matches!(table_option, megascience::TableOptions::Dataset) {
            return Err("Elasticsearch text engine tests are limited to datasets".to_string());
        }
        if std::env::var("ELASTICSEARCH_URL").is_err() {
            return Err("Elasticsearch text engine tests require ELASTICSEARCH_URL".to_string());
        }
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
            let Some(Value::Number(num_a)) = a.get("_score") else {
                return Ordering::Greater;
            };
            let Some(score_a) = num_a.as_f64() else {
                return Ordering::Greater;
            };
            let Some(Value::Number(num_b)) = b.get("_score") else {
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
                && let Some(Value::Number(n)) = obj.get("_score")
                && let Some(score) = n.as_f64()
            {
                let truncated = (100.0 * score).trunc() / 100.0;
                obj.insert(
                    "_score".to_string(),
                    Value::String(format!("{truncated:.2}")),
                );
            }
        }
    }

    sort_json_keys(&mut json);

    serde_json::to_string_pretty(&json).unwrap_or_default()
}

pub async fn start_app(app: App) -> Result<Config, anyhow::Error> {
    register_test_connectors().await;
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
        () = tokio::time::sleep(std::time::Duration::from_mins(2)) => {
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
                .http_url(http_base_url.as_str())
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
                        let mut disp =
                            if let Ok(stream) = client.sql(format!("EXPLAIN {sql}").as_str()).await {
                                match stream.try_collect::<Vec<arrow::record_batch::RecordBatch>>().await {
                                    Ok(c) => arrow::util::pretty::pretty_format_batches(&c)?.to_string(),
                                    Err(e) => format!("Could not prepare EXPLAIN plan: {e}")
                                }
                            } else {
                                format!("Could not prepare EXPLAIN plan. SQL error: {resp}")
                            };
                        disp = sanitize_cayenne_file_paths(&disp);
                        disp = replace_s3_vector_index_names(&disp);
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

/// Sanitize file paths in physical plans for deterministic snapshots.
/// Replaces absolute file paths with placeholders.
fn sanitize_cayenne_file_paths(plan: &str) -> String {
    // Replace absolute paths in file_groups with placeholder
    let mut result = String::new();
    for line in plan.lines() {
        if line.contains("file_groups={") && line.contains(".vortex") {
            // Find the start of file_groups
            if let Some(fg_start) = line.find("file_groups=") {
                // Find the closing ]]}
                if let Some(fg_end) = line[fg_start..].find("]]}") {
                    let prefix = &line[..fg_start];
                    let suffix = &line[fg_start + fg_end + 3..];
                    result.push_str(prefix);
                    // need to add the correct number of file_groups.
                    let num_files = line[fg_start..fg_start + fg_end + 3]
                        .matches(".vortex")
                        .count();
                    result.push_str(
                        format!(
                            r"file_groups={{{} group: [[{}]]}}",
                            num_files,
                            ["<NORMALIZED_PATH>/.vortex"].repeat(num_files).join(", ")
                        )
                        .as_str(),
                    );

                    result.push_str(suffix);
                } else {
                    result.push_str(line);
                }
            } else {
                result.push_str(line);
            }
        } else {
            result.push_str(line);
        }
        result.push('\n');
    }
    result
}
