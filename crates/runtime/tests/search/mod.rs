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
use std::{
    cmp::Ordering,
    collections::HashMap,
    fmt::Display,
    str::FromStr,
    sync::{Arc, LazyLock},
};

use anyhow::Context;
use app::{App, AppBuilder};
use arrow::array::RecordBatch;
use datafusion::sql::TableReference;
use futures::TryStreamExt;
use http::{
    HeaderValue,
    header::{ACCEPT, CONTENT_TYPE},
};
use itertools::Itertools;
use reqwest::header::HeaderMap;
use runtime::{
    Runtime, auth::EndpointAuth, component::dataset::acceleration::Engine, config::Config,
};
use serde_json::{Value, json};
use spicepod::{
    acceleration::Acceleration,
    component::embeddings::EmbeddingChunkConfig,
    param::Params,
    semantic::{Column, ColumnLevelEmbeddingConfig, FullTextSearchConfig},
    vector::VectorStore,
};

use super::models::sort_json_keys;
use crate::{
    DEFAULT_TRACING_MODELS, configure_test_datafusion, init_tracing,
    models::{
        create_api_bindings_config, get_mega_science_dataset, get_mega_science_view,
        hf::get_model_to_vec_embeddings, http_post, openai::get_openai_embeddings,
    },
    utils::{init_tracing_with_task_history, runtime_ready_check, test_request_context},
};

pub mod s3_vectors;

#[derive(Clone)]
pub enum SearchTestType {
    Http(serde_json::Value),
    Sql(String),
}

impl Display for SearchTestType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SearchTestType::Http(value) => write!(f, "{value}"),
            SearchTestType::Sql(query) => write!(f, "{query}"),
        }
    }
}

impl SearchTestType {
    pub fn from_sql(sql: impl Into<String>) -> Self {
        SearchTestType::Sql(sql.into())
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

    pub fn replace_table(&self, from: &TableReference, to: &TableReference) -> Self {
        let body = match self.body.clone() {
            SearchTestType::Http(Value::Object(mut v)) => {
                v["datasets"] = Value::Array(vec![Value::String(to.to_string())]);
                SearchTestType::Http(Value::Object(v))
            }
            SearchTestType::Sql(ref sql) => {
                SearchTestType::Sql(sql.replace(&from.to_string(), &to.to_string()))
            }
            SearchTestType::Http(http) => SearchTestType::Http(http),
        };

        Self {
            should_fail: self.should_fail,
            body,
            name: self.name.clone(),
            skip: self.skip,
        }
    }
}

/// [`TableComponentType`] defines how a SQL table to be searched upon should be constructed.
#[derive(Debug, Clone, Copy)]
pub(crate) enum SearchTableComponentType {
    /// A single [`spicepod::component::dataset::Dataset`]
    Dataset,

    /// A [`spicepod::component::view::View`] constructed from a `JOIN ON`.
    ///   e.g. `SELECT a.*, b.* FROM a JOIN b ON a.id=b.id`
    ViewJoin,

    /// A [`spicepod::component::view::View`] constructed from a `UNION ALL`.
    ///   e.g. `SELECT * FROM a UNION ALL SELECT * FROM b`
    ViewUnionAll,

    /// A [`spicepod::component::view::View`] constructed from a `UNION ALL` where one of the inputs is a ` JOIN ON`.
    ///   e.g. `SELECT * FROM a UNION ALL SELECT * FROM (SELECT b.*, c.* FROM b JOIN c ON c.id=b.id)`
    ViewUnionAllJoin,
}

impl std::fmt::Display for SearchTableComponentType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::Dataset => "dataset",
            Self::ViewJoin => "view_join",
            Self::ViewUnionAll => "view_union_all",
            Self::ViewUnionAllJoin => "view_union_all_join",
        };
        write!(f, "{}", s)
    }
}

impl FromStr for SearchTableComponentType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "dataset" => Ok(Self::Dataset),
            "view_join" => Ok(Self::ViewJoin),
            "view_union_all" => Ok(Self::ViewUnionAll),
            "view_union_all_join" => Ok(Self::ViewUnionAllJoin),
            _ => Err(format!("Unknown SearchTableComponentType: '{}'", s)),
        }
    }
}

// The spicepod fields important in testing search
pub struct SearchSpicepodConfiguration {
    acceleration: Option<Acceleration>,
    vector: Option<VectorStore>,
    table_component: SearchTableComponentType,
    columns: Vec<Column>,
}

static TABLE_ACCELERATION_OPTIONS: LazyLock<HashMap<String, Acceleration>> = LazyLock::new(|| {
    let yaml_content = include_str!("acceleration.yaml");
    serde_yaml::from_str(yaml_content).expect("Failed to parse 'acceleration.yaml' configurations")
});

static MEGA_SCIENCE_COLUMN_CONFIGS: LazyLock<HashMap<String, Vec<Column>>> = LazyLock::new(|| {
    let yaml_content = include_str!("mega_science.yaml");
    serde_yaml::from_str(yaml_content)
        .expect("Failed to parse 'mega_science.yaml' column configurations")
});

impl SearchSpicepodConfiguration {
    // duckdb.no_vector_engine.join_view.hybrid_single_column
    pub fn from_str(
        id: &str,
        column_configs: &HashMap<String, Vec<Column>>,
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

        let vector = match vector {
            "s3_vectors" => Some(VectorStore {
                enabled: true,
                engine: Some("s3_vectors".to_string()),
                params: Some(Params::from_string_map(
                    vec![
                        ("s3_vectors_aws_region".to_string(), "us-east-2".to_string()),
                        (
                            "s3_vectors_bucket".to_string(),
                            "spice-ci-tests-s3-vectors".to_string(),
                        ),
                        (
                            "s3_vectors_index".to_string(),
                            format!(
                                "{engine}-{}-{}-{}",
                                table_component.replace("_", "-"),
                                column_configuration.replace("_", "-"),
                                rand::random::<u8>() % 11
                            ),
                        ),
                        (
                            "s3_vectors_aws_access_key_id".to_string(),
                            "${env:AWS_S3_VECTORS_KEY}".to_string(),
                        ),
                        (
                            "s3_vectors_aws_secret_access_key".to_string(),
                            "${env:AWS_S3_VECTORS_SECRET}".to_string(),
                        ),
                    ]
                    .into_iter()
                    .collect(),
                )),
                partition_by: vec![],
            }),
            "no_vector_engine" => None,
            x => {
                return Err(anyhow::anyhow!(
                    "Invalid vector field '{x}' in search spicepod slug."
                ));
            }
        };

        let Some(columns) = column_configs.get(column_configuration).cloned() else {
            return Err(anyhow::anyhow!(
                "Invalid column configuration field '{column_configuration}' in search spicepod slug."
            ));
        };

        Ok(SearchSpicepodConfiguration {
            acceleration: Some(acceleration),
            vector,
            table_component: table_component
                .parse()
                .map_err(|e: String| anyhow::anyhow!(e))?,
            columns,
        })
    }
}

pub fn build_mega_science(mut app: AppBuilder, cfg: &SearchSpicepodConfiguration) -> AppBuilder {
    let answer = cfg.columns.iter().find(|col| col.name == "answer");
    let question = cfg.columns.iter().find(|col| col.name == "question");

    let (ds, views) = match cfg.table_component {
        SearchTableComponentType::Dataset => {
            let mut ds = get_mega_science_dataset(Some("qs"), question.cloned(), answer.cloned());
            ds.vectors = cfg.vector.clone();
            ds.acceleration = cfg.acceleration.clone();
            (ds, vec![])
        }
        SearchTableComponentType::ViewUnionAllJoin => {
            let (ds, mut views) =
                get_mega_science_view(Some("qs"), question.cloned(), answer.cloned());
            if let Some(v) = views.last_mut() {
                v.vectors = cfg.vector.clone();
                v.acceleration = cfg.acceleration.clone();
            }
            (ds, views)
        }
        x => {
            unimplemented!("Search test with {x} configuration")
        }
    };
    app = app.with_dataset(ds);
    for v in views {
        app = app.with_view(v);
    }
    app
}

macro_rules! generate_search_tests {
    ([$($slug:expr),* $(,)?]) => {
        paste::paste! {
            $(
                #[tokio::test]
                #[allow(non_snake_case)]
                async fn [<test_search_ $slug:snake>]() {
                    let app = AppBuilder::new("search_app").with_embedding(get_model_to_vec_embeddings(
                        "minishlab/potion-base-2M",
                        "hf_minilm",
                    ));

                    let cfg = SearchSpicepodConfiguration::from_str($slug, &MEGA_SCIENCE_COLUMN_CONFIGS)
                        .expect("could not initialise configuration");
                    run_search_w_explain(
                        build_mega_science(app, &cfg).build(),
                        basic_vector_search_tests_on_table($slug, "qs"),
                        true,
                    )
                    .await
                    .expect("failed to run search tests");
                }
            )*
        }
    };
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
                        let resp = http_sql(http_base_url.as_str(), &sql).await;
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

// Test patterns are expanded at build time by `build.rs` (see `build_search_test_cases`).
// Requires the existance of `generate_search_tests` macro wherever it is included.
include!("generated_search_tests.rs");

/// Returns common test cases for vector search on the [`get_mega_science_dataset`] dataset
///
/// Assumes datasets has name `qs` and embedding column is on `answer` column.
pub(super) fn basic_vector_search_tests_on_table(
    prefix: &'static str,
    table_name: &'static str,
) -> Vec<SearchTestCase> {
    vec![
        SearchTestCase::new(
            format!("{prefix}_basic"),
            SearchTestType::Http(json!({
                "text": "second",
                "limit": 4,
                "datasets": [table_name],
            })),
        ),
        SearchTestCase::new(
            format!("{prefix}_keywords"),
            SearchTestType::Http(json!({
                "text": "second",
                "limit": 4,
                "datasets": [table_name],
                "keywords": ["number"],
            })),
        ),
        SearchTestCase::new(
            format!("{prefix}_additional_columns"),
            SearchTestType::Http(json!({
                "text": "second",
                "limit": 4,
                "datasets": [table_name],
                "additional_columns": ["question"],
            })),
        ),
        SearchTestCase::new(
            format!("{prefix}_with_where"),
            SearchTestType::Http(json!({
                "text": "secondary",
                "datasets": [table_name],
                "where": "subject!='math'",
                "limit": 4,
            })),
        ),
        SearchTestCase::new(
            format!("{prefix}_vector_search_sql_basic"),
            SearchTestType::from_sql(format!(
                "SELECT id, answer, trunc(score, 3) FROM vector_search({table_name}, 'second', answer) order by score desc, id LIMIT 4"
            )),
        ),
        SearchTestCase::new(
            format!("{prefix}_vector_search_sql_projection"),
            SearchTestType::from_sql(format!(
                "SELECT id, answer, question, subject, trunc(score, 3) as score FROM vector_search({table_name}, 'second', answer) order by score desc, id LIMIT 4",
            )),
        ),
        SearchTestCase::new(
            format!("{prefix}_vector_search_sql_filters"),
            SearchTestType::from_sql(format!(
                "SELECT id, answer, trunc(score, 3) as score FROM vector_search({table_name}, 'secondary', answer) where subject!='math' order by score desc, id LIMIT 4",
            )),
        ),
        SearchTestCase::new(
            format!("{prefix}_vector_search_sql_no_score"),
            SearchTestType::from_sql(format!(
                "SELECT id, answer FROM vector_search({table_name}, 'second', answer) order by score desc, id LIMIT 4",
            )),
        ),
        SearchTestCase::new(
            format!("{prefix}_vector_search_sql_random"),
            SearchTestType::from_sql(format!(
                "SELECT subject FROM vector_search({table_name}, 'second', answer) order by score desc LIMIT 4",
            )),
        ),
        SearchTestCase::new(
            format!("{prefix}_vector_search_sql_vectors"),
            SearchTestType::from_sql(format!(
                "SELECT id, answer, array_length(answer_embedding), trunc(score, 3) as score  FROM vector_search({table_name}, 'second', answer) order by score desc, id desc LIMIT 4;",
            )),
        ),
    ]
}

pub(crate) fn vectors_nonfilterable_col(col: impl Into<Column>) -> Column {
    col.into().with_metadata(
        [(
            "vectors".to_string(),
            serde_json::Value::String("non-filterable".to_string()),
        )]
        .into(),
    )
}
// mod old {
//     pub use super::*;

//     #[tokio::test]
//     async fn test_multi_column_search_view() -> Result<(), anyhow::Error> {
//         let (ds, views) = get_mega_science_view(
//             Some("qs"),
//             // multi_column
//             Some(Column::new("question").with_embeddings(vec![
//                 ColumnLevelEmbeddingConfig::model("hf_minilm").with_row_id("id"),
//             ])),
//             Some(Column::new("answer").with_embeddings(vec![
//                 ColumnLevelEmbeddingConfig::model("hf_minilm").with_row_id("id"),
//             ])),
//         );

//         let mut app = AppBuilder::new("search_app")
//             .with_dataset(ds)
//             .with_embedding(get_model_to_vec_embeddings(
//                 "minishlab/potion-base-2M",
//                 "hf_minilm",
//             ));

//         for v in views {
//             app = app.with_view(v);
//         }

//         run_search_w_explain(
//         app.build(),
//         [
//         basic_vector_search_tests_on_table("multi_column_view_answer", "qs"),
//         vec![
//             SearchTestCase::new(
//                 "multi_column_view_basic".to_string(),
//                 SearchTestType::Http(json!({
//                     "text": "second",
//                     "limit": 4,
//                     "datasets": ["qs"],
//                 })),
//             ),
//             SearchTestCase::new(
//                 "multi_column_view_additional_columns".to_string(),
//                 SearchTestType::Http(json!({
//                     "text": "second",
//                     "limit": 4,
//                     "datasets": ["qs"],
//                     "additional_columns": ["question"],
//                 })),
//             ),
//             SearchTestCase::new(
//                 "multi_column_view_with_where".to_string(),
//                 SearchTestType::Http(json!({
//                     "text": "secondary",
//                     "datasets": ["qs"],
//                     "where": "subject='math'",
//                     "limit": 1,
//                 })),
//             ),
//             SearchTestCase::new(
//                 "multi_column_view_question_vector_search_sql_filters".to_string(),
//                 SearchTestType::from_sql(
//                     "SELECT id, answer, trunc(score, 3) as score FROM vector_search(qs, 'secondary', question) where subject!='math' order by score desc LIMIT 4",
//                 ),
//             ),
//             SearchTestCase::new(
//                 "multi_column_view_question_vector_search_sql_no_score".to_string(),
//                 SearchTestType::from_sql(
//                     "SELECT id, answer FROM vector_search(qs, 'second', question) order by score desc LIMIT 4",
//                 ),
//             ),
//             SearchTestCase::new(
//                 "multi_column_view_question_vector_search_sql_random".to_string(),
//                 SearchTestType::from_sql(
//                     "SELECT subject FROM vector_search(qs, 'second', question) order by score desc LIMIT 4",
//                 ),
//             ),
//             SearchTestCase::new(
//                 "multi_column_view_question_vector_search_sql_vectors".to_string(),
//                 SearchTestType::from_sql(
//                     "SELECT id, answer, array_length(question_embedding), round(score, 1) FROM vector_search(qs, 'second', question) order by score desc LIMIT 4;",
//                 ),
//             ),
//         ]
//         ].concat(),
//         true
//     )
//     .await
//     }

//     #[tokio::test]
//     async fn test_multi_column_search() -> Result<(), anyhow::Error> {
//         let ds = get_mega_science_dataset(
//             Some("qs"),
//             Some(Column::new("question").with_embeddings(vec![
//                 ColumnLevelEmbeddingConfig::model("hf_minilm").with_row_id("id"),
//             ])),
//             Some(Column::new("answer").with_embeddings(vec![
//                 ColumnLevelEmbeddingConfig::model("hf_minilm")
//                     .with_row_id("id")
//                     .chunking(EmbeddingChunkConfig::enabled().target_chunk_size(64)),
//             ])),
//         );

//         let app = AppBuilder::new("search_app")
//             .with_dataset(ds)
//             .with_embedding(get_model_to_vec_embeddings(
//                 "minishlab/potion-base-2M",
//                 "hf_minilm",
//             ))
//             .build();
//         run_search(
//         app,
//         vec![
//             SearchTestCase::new(
//                 "multi_column_basic".to_string(),
//                 SearchTestType::Http(json!({
//                     "text": "second",
//                     "limit": 4,
//                     "datasets": ["qs"],
//                 })),
//             ),
//             SearchTestCase::new(
//                 "multi_column_keywords",
//                 SearchTestType::Http(json!({
//                     "text": "second",
//                     "limit": 4,
//                     "datasets": ["qs"],
//                     "keywords": ["number"],
//                 })),
//             ),
//             SearchTestCase::new(
//                 "multi_column_additional_columns".to_string(),
//                 SearchTestType::Http(json!({
//                     "text": "second",
//                     "limit": 4,
//                     "datasets": ["qs"],
//                     "additional_columns": ["question"],
//                 })),
//             ),
//             SearchTestCase::new(
//                 "multi_column_with_where".to_string(),
//                 SearchTestType::Http(json!({
//                     "text": "secondary",
//                     "datasets": ["qs"],
//                     "where": "subject='math'",
//                     "limit": 1,
//                 })),
//             ),
//             SearchTestCase::new(
//                 "multi_column_question_vector_search_sql_filters".to_string(),
//                 SearchTestType::from_sql(
//                     "SELECT id, answer, trunc(score, 3) as score FROM vector_search(qs, 'secondary', question) where subject!='math' order by score desc LIMIT 4",
//                 ),
//             ),
//             SearchTestCase::new(
//                 "multi_column_question_vector_search_sql_no_score".to_string(),
//                 SearchTestType::from_sql(
//                     "SELECT id, answer FROM vector_search(qs, 'second', question) order by score desc LIMIT 4",
//                 ),
//             ),
//             SearchTestCase::new(
//                 "multi_column_question_vector_search_sql_random".to_string(),
//                 SearchTestType::from_sql(
//                     "SELECT subject FROM vector_search(qs, 'second', question) order by score desc LIMIT 4",
//                 ),
//             ),
//             SearchTestCase::new(
//                 "multi_column_question_vector_search_sql_vectors".to_string(),
//                 SearchTestType::from_sql(
//                     "SELECT id, answer, array_length(question_embedding), round(score, 1) FROM vector_search(qs, 'second', question) order by score desc LIMIT 4;",
//                 ),
//             ),
//         ],
//     )
//     .await
//     }

//     // Use two different embedding models on a single column.
//     #[tokio::test]
//     async fn test_multi_embedding_model_search() -> Result<(), anyhow::Error> {
//         run_search(
//         AppBuilder::new("search_app")
//             .with_embedding(get_model_to_vec_embeddings(
//                 "minishlab/potion-base-2M",
//                 "hf_minilm",
//             ))
//             .with_embedding(get_openai_embeddings(
//                 Some("text-embedding-3-small"),
//                 "openai_embeddings",
//             ))
//             .with_dataset(get_mega_science_dataset(
//                 Some("qs"),
//                 None,
//                 Some(Column::new("answer").with_embeddings(vec![
//                     ColumnLevelEmbeddingConfig::model("hf_minilm").with_row_id("id"),
//                     ColumnLevelEmbeddingConfig::model("openai_embeddings").with_row_id("id")
//                 ]))))
//             .build(),
//         vec![
//             SearchTestCase::new(
//                 "multi_embeddings_basic",
//                 SearchTestType::Http(json!({
//                     "text": "second",
//                     "limit": 4,
//                     "datasets": ["qs"],
//                 })),
//             ),
//             SearchTestCase::new(
//                 "multi_embeddings_additional_columns",
//                 SearchTestType::Http(json!({
//                     "text": "second",
//                     "limit": 4,
//                     "datasets": ["qs"],
//                     "additional_columns": ["question"],
//                 })),
//             ),
//             SearchTestCase::new(
//                 "multi_embeddings_with_where",
//                 SearchTestType::Http(json!({
//                     "text": "secondary",
//                     "datasets": ["qs"],
//                     "where": "subject!='math'",
//                     "limit": 4,
//                 })),
//             ),
//             SearchTestCase::new(
//                 "multi_embeddings_sql_vector_search",
//                 SearchTestType::from_sql(
//                     "SELECT id, question, trunc(score, 3) FROM vector_search(qs, 'second') order by score desc LIMIT 4",
//                 ),
//             ),
//         ],
//     )
//     .await
//     }

//     #[tokio::test]
//     async fn test_hybrid_search_single_column() -> Result<(), anyhow::Error> {
//         run_search(
//         AppBuilder::new("search_app")
//             .with_embedding(get_model_to_vec_embeddings(
//                 "minishlab/potion-base-2M",
//                 "hf_minilm",
//             ))
//             .with_dataset(get_mega_science_dataset(
//                 Some("qs"),
//                 Some(Column::new("question")
//                     .with_embedding(ColumnLevelEmbeddingConfig::model("hf_minilm").with_row_id("id"))
//                     .with_full_text_search(FullTextSearchConfig::enabled().with_row_id("id"))
//                 ),
//                 None,
//             ))
//             .build(),
//         vec![
//             SearchTestCase::new(
//                 "hybrid_single_column_basic",
//                 SearchTestType::Http(json!({
//                     "text": "second",
//                     "limit": 4,
//                     "datasets": ["qs"],
//                 })),
//             ),
//             SearchTestCase::new(
//                 "hybrid_single_column_keywords",
//                 SearchTestType::Http(json!({
//                     "text": "second",
//                     "limit": 4,
//                     "datasets": ["qs"],
//                     "keywords": ["number"],
//                 })),
//             ),
//             SearchTestCase::new(
//                 "hybrid_single_column_additional_columns",
//                 SearchTestType::Http(json!({
//                     "text": "second",
//                     "limit": 4,
//                     "datasets": ["qs"],
//                     "additional_columns": ["question"],
//                 })),
//             ),
//             SearchTestCase::new(
//                 "hybrid_single_column_with_where",
//                 SearchTestType::Http(json!({
//                     "text": "secondary",
//                     "datasets": ["qs"],
//                     "where": "subject!='math'",
//                     "limit": 4,
//                 })),
//             ),
//             SearchTestCase::new(
//                 "hybrid_single_column_sql_text_search",
//                 SearchTestType::from_sql(
//                     "SELECT id, answer, trunc(score, 3) FROM text_search(qs, 'second') order by score desc LIMIT 4",
//                 ),
//             ),
//             SearchTestCase::new(
//                 "hybrid_single_column_sql_vector_search",
//                 SearchTestType::from_sql(
//                     "SELECT id, question, trunc(score, 3) FROM vector_search(qs, 'second') order by score desc LIMIT 4",
//                 ),
//             ),
//             SearchTestCase::new(
//                 "hybrid_single_column_sql_vector_search_no_score",
//                 SearchTestType::from_sql(
//                     "SELECT question FROM vector_search(qs, 'second') order by score desc LIMIT 4",
//                 ),
//             ),
//         ],
//     )
//     .await
//     }

//     #[tokio::test]
//     async fn test_hybrid_search_multiple_column() -> Result<(), anyhow::Error> {
//         run_search(
//         AppBuilder::new("search_app")
//             .with_embedding(get_model_to_vec_embeddings(
//                 "minishlab/potion-base-2M",
//                 "hf_minilm",
//             ))
//             .with_dataset(get_mega_science_dataset(
//                 Some("qs"),
//                 Some(Column::new("question").with_embedding(ColumnLevelEmbeddingConfig::model("hf_minilm").with_row_id("id"))),
//                 Some(Column::new("answer").with_full_text_search(FullTextSearchConfig::enabled().with_row_id("id"))),
//             ))
//             .build(),
//         vec![
//             SearchTestCase::new(
//                 "hybrid_multiple_column_basic",
//                 SearchTestType::Http(json!({
//                     "text": "second",
//                     "limit": 4,
//                     "datasets": ["qs"],
//                 })),
//             ),
//             SearchTestCase::new(
//                 "hybrid_multiple_column_keywords",
//                 SearchTestType::Http(json!({
//                     "text": "second",
//                     "limit": 4,
//                     "datasets": ["qs"],
//                     "keywords": ["number"],
//                 })),
//             ),
//             SearchTestCase::new(
//                 "hybrid_multiple_column_additional_columns",
//                 SearchTestType::Http(json!({
//                     "text": "second",
//                     "limit": 4,
//                     "datasets": ["qs"],
//                     "additional_columns": ["question"],
//                 })),
//             ),
//             SearchTestCase::new(
//                 "hybrid_multiple_column_with_where",
//                 SearchTestType::Http(json!({
//                     "text": "secondary",
//                     "datasets": ["qs"],
//                     "where": "subject!='math'",
//                     "limit": 4,
//                 })),
//             ),
//             SearchTestCase::new(
//                 "hybrid_multiple_column_sql_text_search",
//                 SearchTestType::from_sql(
//                     "SELECT id, answer, trunc(score, 3) FROM text_search(qs, 'second') order by score desc LIMIT 4",
//                 ),
//             ),
//             SearchTestCase::new(
//                 "hybrid_multiple_column_sql_text_search_wrong_column",
//                 SearchTestType::from_sql(
//                     "SELECT id, answer, trunc(score, 3) FROM text_search(qs, 'second', question) order by score desc LIMIT 4",
//                 ),
//             ).should_fail(),
//             SearchTestCase::new(
//                 "hybrid_multiple_column_sql_vector_search",
//                 SearchTestType::from_sql(
//                     "SELECT id, question, trunc(score, 3) FROM vector_search(qs, 'second') order by score desc LIMIT 4",
//                 ),
//             ),
//             SearchTestCase::new(
//                 "hybrid_multiple_column_sql_vector_search_wrong_column",
//                 SearchTestType::from_sql(
//                     "SELECT id, question, trunc(score, 3) FROM vector_search(qs, 'second', answer) order by score desc LIMIT 4",
//                 ),
//             ).should_fail(),
//         ],
//     )
//     .await
//     }

//     #[tokio::test]
//     async fn test_rrf_search() -> Result<(), anyhow::Error> {
//         run_search(
//         AppBuilder::new("search_app")
//             .with_embedding(get_model_to_vec_embeddings(
//                 "minishlab/potion-base-2M",
//                 "hf_minilm",
//             ))
//             .with_dataset(get_mega_science_dataset(
//                 Some("qs"),
//                 Some(Column::new("question").with_embedding(ColumnLevelEmbeddingConfig::model("hf_minilm").with_row_id("id"))),
//                 Some(Column::new("answer").with_full_text_search(FullTextSearchConfig::enabled().with_row_id("id"))),
//             ))
//             .build(),
//         vec![
//             SearchTestCase::new(
//                 "hybrid_multiple_column_sql_rrf",
//                 SearchTestType::from_sql(
//                     "SELECT id, question, trunc(fused_score, 3) FROM rrf(vector_search(qs, 'second'), text_search(qs, 'second')) order by fused_score desc LIMIT 4",
//                 ),
//             ),
//             SearchTestCase::new(
//                 "hybrid_multiple_column_sql_rrf_wrong_column",
//                 SearchTestType::from_sql(
//                     "SELECT id, question, trunc(score, 3) FROM rrf(vector_search(qs, 'second', answer), text_search(qs, 'second', answer)) order by fused_score desc LIMIT 4",
//                 ),
//             ).should_fail(),
//             SearchTestCase::new(
//                 "hybrid_multiple_column_sql_rrf_explicit_join",
//                 SearchTestType::from_sql(
//                     "SELECT id, question, trunc(fused_score, 3) FROM rrf(vector_search(qs, 'second'), text_search(qs, 'second'), join_key => 'id') order by fused_score desc LIMIT 4",
//                 ),
//             ),
//             SearchTestCase::new(
//                 "hybrid_multiple_column_sql_rrf_explicit_join_wrong_column",
//                 SearchTestType::from_sql(
//                     "SELECT id, question, trunc(fused_score, 3) FROM rrf(vector_search(qs, 'second'), text_search(qs, 'second'), join_key => 'foobar') order by fused_score desc LIMIT 4",
//                 ),
//             ).should_fail(),
//             SearchTestCase::new(
//                 "hybrid_multiple_column_sql_rrf_one_subquery_fail",
//                 SearchTestType::from_sql(
//                     "SELECT id, question, trunc(fused_score, 3) FROM rrf(vector_search(qs, 'second')) order by fused_score desc LIMIT 4",
//                 ),
//             ).should_fail(),
//         ],
//     ).await
//     }

//     #[tokio::test]
//     #[allow(clippy::too_many_lines)]
//     async fn test_text_search() -> Result<(), anyhow::Error> {
//         run_search(
//         AppBuilder::new("search_app")
//             .with_dataset(get_mega_science_dataset(
//                 Some("qs"),
//                 None,
//                 Some(Column::new("answer").with_full_text_search(FullTextSearchConfig::enabled().with_row_id("id"))),
//             ))
//             .build(),
//         vec![
//             SearchTestCase::new(
//                 "text_search_basic",
//                 SearchTestType::Http(json!({
//                     "text": "second",
//                     "limit": 4,
//                     "datasets": ["qs"],
//                 })),
//             ),
//             SearchTestCase::new(
//                 "text_search_keywords",
//                 SearchTestType::Http(json!({
//                     "text": "second",
//                     "limit": 4,
//                     "datasets": ["qs"],
//                     "keywords": ["number"],
//                 })),
//             ),
//             SearchTestCase::new(
//                 "text_search_additional_columns",
//                 SearchTestType::Http(json!({
//                     "text": "second",
//                     "limit": 4,
//                     "datasets": ["qs"],
//                     "additional_columns": ["question"],
//                 })),
//             ),
//             SearchTestCase::new(
//                 "text_search_with_where",
//                 SearchTestType::Http(json!({
//                     "text": "secondary",
//                     "datasets": ["qs"],
//                     "where": "subject!='math'",
//                     "limit": 4,
//                 })),
//             ),
//             SearchTestCase::new(
//                 "text_search_basic_without_defined_dataset",
//                 SearchTestType::Http(json!({
//                     "text": "second",
//                     "limit": 4,
//                 })),
//             ),
//             SearchTestCase::new(
//                 "text_search_sql_text_search_basic",
//                 SearchTestType::from_sql(
//                     "SELECT id, answer, trunc(score, 3) FROM text_search(qs, 'second') order by score desc LIMIT 4",
//                 ),
//             ),
//             SearchTestCase::new(
//                 "text_search_sql_text_search_projection",
//                 SearchTestType::from_sql(
//                     "SELECT id, answer, question, subject, trunc(score, 3) as score FROM text_search(qs, 'second') order by score desc LIMIT 4",
//                 ),
//             ),
//             SearchTestCase::new(
//                 "text_search_sql_text_search_filters",
//                 SearchTestType::from_sql(
//                     "SELECT id, answer, trunc(score, 3) as score FROM text_search(qs, 'secondary') where subject!='math' order by score desc LIMIT 4",
//                 ),
//             ),
//             SearchTestCase::new(
//                 "text_search_sql_text_search_no_score",
//                 SearchTestType::from_sql(
//                     "SELECT id, answer FROM text_search(qs, 'second') order by score desc LIMIT 4",
//                 ),
//             ),
//             SearchTestCase::new(
//                 "text_search_sql_text_search_random",
//                 SearchTestType::from_sql(
//                     "SELECT subject FROM text_search(qs, 'second') order by score desc LIMIT 4",
//                 ),
//             ),
//         ],
//     )
//     .await
//     }

//     #[tokio::test]
//     #[allow(clippy::too_many_lines)]
//     async fn test_text_search_view() -> Result<(), anyhow::Error> {
//         let (ds, views) = get_mega_science_view(
//             Some("qs"),
//             None,
//             Some(
//                 Column::new("answer")
//                     .with_full_text_search(FullTextSearchConfig::enabled().with_row_id("id")),
//             ),
//         );

//         let mut app = AppBuilder::new("search_app").with_dataset(ds);
//         for v in views {
//             app = app.with_view(v);
//         }

//         run_search_w_explain(app.build(),
//         vec![
//             SearchTestCase::new(
//                 "text_search_view_basic",
//                 SearchTestType::Http(json!({
//                     "text": "second",
//                     "limit": 4,
//                     "datasets": ["qs"],
//                 })),
//             ),
//             SearchTestCase::new(
//                 "text_search_view_additional_columns",
//                 SearchTestType::Http(json!({
//                     "text": "second",
//                     "limit": 4,
//                     "datasets": ["qs"],
//                     "additional_columns": ["question"],
//                 })),
//             ),
//             SearchTestCase::new(
//                 "text_search_view_with_where",
//                 SearchTestType::Http(json!({
//                     "text": "secondary",
//                     "datasets": ["qs"],
//                     "where": "subject!='math'",
//                     "limit": 4,
//                 })),
//             ),
//             SearchTestCase::new(
//                 "text_search_view_basic_without_defined_dataset",
//                 SearchTestType::Http(json!({
//                     "text": "second",
//                     "limit": 4,
//                 })),
//             ),
//             SearchTestCase::new(
//                 "text_search_view_sql_text_search_basic",
//                 SearchTestType::from_sql(
//                     "SELECT id, answer, trunc(score, 3) FROM text_search(qs, 'second') order by score desc LIMIT 4",
//                 ),
//             ),
//             SearchTestCase::new(
//                 "text_search_view_sql_text_search_projection",
//                 SearchTestType::from_sql(
//                     "SELECT id, answer, question, subject, trunc(score, 3) as score FROM text_search(qs, 'second') order by score desc LIMIT 4",
//                 ),
//             ),
//             SearchTestCase::new(
//                 "text_search_view_sql_text_search_filters",
//                 SearchTestType::from_sql(
//                     "SELECT id, answer, trunc(score, 3) as score FROM text_search(qs, 'secondary') where subject!='math' order by score desc LIMIT 4",
//                 ),
//             ),
//             SearchTestCase::new(
//                 "text_search_view_sql_text_search_no_score",
//                 SearchTestType::from_sql(
//                     "SELECT id, answer FROM text_search(qs, 'second') order by score desc LIMIT 4",
//                 ),
//             ),
//             SearchTestCase::new(
//                 "text_search_view_sql_text_search_random",
//                 SearchTestType::from_sql(
//                     "SELECT subject FROM text_search(qs, 'second') order by score desc LIMIT 4",
//                 ),
//             ),
//         ],
//         true
//     )
//     .await
//     }

//     #[tokio::test]
//     async fn test_text_search_where_rowid_is_search_column() -> Result<(), anyhow::Error> {
//         run_search(
//         AppBuilder::new("search_app")
//             .with_dataset(get_mega_science_dataset(
//                 Some("qs"),
//                 None,
//                 Some(Column::new("answer").with_full_text_search(FullTextSearchConfig::enabled().with_row_id("answer"))),
//             ))
//             .build(),
//         vec![
//             SearchTestCase::new(
//                 "test_text_search_where_rowid_is_search_column_basic",
//                 SearchTestType::Http(json!({
//                     "text": "second",
//                     "limit": 4,
//                     "datasets": ["qs"],
//                 })),
//             ),
//             SearchTestCase::new(
//                 "test_text_search_sql_where_rowid_is_search_column_basic",
//                 SearchTestType::from_sql("SELECT id, answer, trunc(score, 3) FROM text_search(qs, 'second') order by score desc LIMIT 4"),
//             ),
//         ]
//     )
//     .await
//     }

//     #[tokio::test]
//     async fn test_text_search_where_rowid_is_search_column_multi_column()
//     -> Result<(), anyhow::Error> {
//         run_search(
//             AppBuilder::new("search_app")
//                 .with_dataset(get_mega_science_dataset(
//                     Some("qs"),
//                     Some(Column::new("question").with_full_text_search(
//                         FullTextSearchConfig::enabled().with_row_id("answer"),
//                     )),
//                     Some(Column::new("answer").with_full_text_search(
//                         FullTextSearchConfig::enabled().with_row_id("answer"),
//                     )),
//                 ))
//                 .build(),
//             vec![SearchTestCase::new(
//                 "test_text_search_where_rowid_is_search_column_multi_column",
//                 SearchTestType::Http(json!({
//                     "text": "second",
//                     "limit": 4,
//                     "datasets": ["qs"],
//                 })),
//             )],
//         )
//         .await
//     }

//     #[tokio::test]
//     async fn test_text_search_where_rowid_is_search_column_composite_pk()
//     -> Result<(), anyhow::Error> {
//         run_search(
//         AppBuilder::new("search_app")
//             .with_dataset(get_mega_science_dataset(
//                 Some("qs"),
//                 None,
//                 Some(
//                     Column::new("answer").with_full_text_search(
//                         FullTextSearchConfig::enabled().with_row_id("answer").with_row_id("id"),
//                     ),
//                 ),
//             ))
//             .build(),
//         vec![
//             SearchTestCase::new(
//                 "test_text_search_where_rowid_is_search_column_composite_pk_basic",
//                 SearchTestType::Http(json!({
//                     "text": "second",
//                     "limit": 4,
//                     "datasets": ["qs"],
//                 })),
//             ),
//             SearchTestCase::new(
//                 "test_text_search_sql_where_rowid_is_search_column_composite_pk_basic",
//                 SearchTestType::from_sql("SELECT id, answer, trunc(score, 3) FROM text_search(qs, 'second') order by score desc LIMIT 4"),
//             ),
//         ],
//     )
//     .await
//     }

//     #[tokio::test]
//     #[allow(clippy::too_many_lines)]
//     async fn test_text_search_multiple_columns() -> Result<(), anyhow::Error> {
//         run_search(
//         AppBuilder::new("search_app")
//             .with_dataset(get_mega_science_dataset(
//                 Some("qs"),
//                 Some(Column::new("question").with_full_text_search(FullTextSearchConfig::enabled().with_row_id("id"))),
//                 Some(Column::new("answer").with_full_text_search(FullTextSearchConfig::enabled().with_row_id("id"))),

//             ))
//             .build(),
//         vec![
//             SearchTestCase::new(
//                 "multi_text_column_basic",
//                 SearchTestType::Http(json!({
//                     "text": "second",
//                     "limit": 4,
//                     "datasets": ["qs"],
//                 })),
//             ),
//             SearchTestCase::new(
//                 "multi_text_column_keywords",
//                 SearchTestType::Http(json!({
//                     "text": "second",
//                     "limit": 4,
//                     "datasets": ["qs"],
//                     "keywords": ["number"],
//                 })),
//             ),
//             SearchTestCase::new(
//                 "multi_text_column_additional_columns",
//                 SearchTestType::Http(json!({
//                     "text": "second",
//                     "limit": 4,
//                     "datasets": ["qs"],
//                     "additional_columns": ["question"],
//                 })),
//             ),
//             SearchTestCase::new(
//                 "multi_text_column_with_where",
//                 SearchTestType::Http(json!({
//                     "text": "secondary",
//                     "datasets": ["qs"],
//                     "where": "subject!='math'",
//                     "limit": 4,
//                 })),
//             ),
//             SearchTestCase::new(
//                 "multi_text_column_sql_text_search_basic_answer",
//                 SearchTestType::from_sql("SELECT id, answer, trunc(score, 3) FROM text_search(qs, 'second', answer) order by score desc LIMIT 4"),
//             ),
//             SearchTestCase::new(
//                 "multi_text_column_sql_text_search_basic_question",
//                 SearchTestType::from_sql("SELECT id, question, trunc(score, 3) FROM text_search(qs, 'angles', question) order by score desc LIMIT 4"),
//             ),
//             SearchTestCase::new(
//                 // When there are multiple columns, `text_search` needs column explicitly as input.
//                 "multi_text_column_sql_text_search_error_without_column",
//                 SearchTestType::from_sql("SELECT id, answer, trunc(score, 3) FROM text_search(qs, 'second') order by score desc LIMIT 4"),
//             ).should_fail(),
//             SearchTestCase::new(
//                 "multi_text_column_sql_text_search_projection",
//                 SearchTestType::from_sql("SELECT id, answer, question, subject, trunc(score, 3) as score FROM text_search(qs, 'second', answer) order by score desc LIMIT 4"),
//             ),
//             SearchTestCase::new(
//                 "multi_text_column_sql_text_search_filters",
//                 SearchTestType::from_sql("SELECT id, answer, trunc(score, 3) as score FROM text_search(qs, 'secondary', answer) where subject!='math' order by score desc LIMIT 4"),
//             ),
//             SearchTestCase::new(
//                 "multi_text_column_sql_text_search_no_score",
//                 SearchTestType::from_sql("SELECT id, answer FROM text_search(qs, 'second', answer) order by score desc LIMIT 4"),
//             ),
//             SearchTestCase::new(
//                 "multi_text_column_sql_text_search_random",
//                 SearchTestType::from_sql("SELECT subject FROM text_search(qs, 'second', answer) order by score desc LIMIT 4"),
//             ),
//         ],
//     )
//     .await
//     }

//     #[tokio::test]
//     #[allow(clippy::too_many_lines)]
//     async fn test_text_search_metadata() -> Result<(), anyhow::Error> {
//         let mut ds = get_mega_science_dataset(
//             Some("qs"),
//             Some(
//                 Column::new("question")
//                     .with_full_text_search(FullTextSearchConfig::enabled().with_row_id("id"))
//                     .with_metadata(
//                         [(
//                             "vectors".to_string(),
//                             Value::String("non-filterable".to_string()),
//                         )]
//                         .into(),
//                     ),
//             ),
//             Some(
//                 Column::new("answer")
//                     .with_full_text_search(FullTextSearchConfig::enabled().with_row_id("id")),
//             ),
//         );
//         ds.columns.push(vectors_nonfilterable_col("subject"));

//         run_search_w_explain(
//         AppBuilder::new("search_app")
//             .with_dataset(ds).build(),
//         vec![
//             SearchTestCase::new(

//                 "text_search_metadata_basic",
//                 SearchTestType::Http(json!({
//                     "text": "second",
//                     "limit": 4,
//                     "datasets": ["qs"],
//                 })),
//             ),
//             SearchTestCase::new(
//                 "text_search_metadata_additional_columns",
//                 SearchTestType::Http(json!({
//                     "text": "second",
//                     "limit": 4,
//                     "datasets": ["qs"],
//                     "additional_columns": ["question"],
//                 })),
//             ),
//             SearchTestCase::new(
//                 "text_search_metadata_with_where",
//                 SearchTestType::Http(json!({
//                     "text": "secondary",
//                     "datasets": ["qs"],
//                     "where": "subject!='math'",
//                     "limit": 4,
//                 })),
//             ),
//             SearchTestCase::new(
//                 "text_search_metadata_sql_text_search_answer",
//                 SearchTestType::from_sql("SELECT id, answer, trunc(score, 3) FROM text_search(qs, 'second', answer) order by score desc LIMIT 4"),
//             ),
//             SearchTestCase::new(
//                 "text_search_metadata_sql_text_search_answer_w_question",
//                 SearchTestType::from_sql("SELECT id, question, trunc(score, 3) FROM text_search(qs, 'second', answer) order by score desc LIMIT 4"),
//             ),
//             SearchTestCase::new(
//                 "text_search_metadata_sql_text_search_question",
//                 SearchTestType::from_sql("SELECT id, question, trunc(score, 3) FROM text_search(qs, 'angles', question) order by score desc LIMIT 4"),
//             ),
//             SearchTestCase::new(
//                 "text_search_metadata_sql_text_search_subject_filter",
//                 SearchTestType::from_sql("SELECT id, question, trunc(score, 3) FROM text_search(qs, 'angles', question) where subject='math' order by score desc LIMIT 4"),
//             ),
//             SearchTestCase::new(
//                 "text_search_metadata_sql_text_search_subject_projection",
//                 SearchTestType::from_sql("SELECT id, subject, trunc(score, 3) FROM text_search(qs, 'angles', question) order by score desc LIMIT 4"),
//             ),
//         ],
//         true
//     )
//     .await
//     }

//     #[cfg(feature = "flightsql")]
//     #[tokio::test]
//     async fn test_multi_column_w_existing_embedding() -> Result<(), anyhow::Error> {
//         use spicepod::{acceleration::Acceleration, param::Params};

//         let api_config = start_app(
//             AppBuilder::new("search_app")
//                 .with_dataset(catalog_page_tpcds_dataset_w_embeddings(
//                     "single_column",
//                     "hf_minilm",
//                     Some(vec!["cp_catalog_page_sk".to_string()]),
//                     None,
//                 ))
//                 .with_embedding(get_model_to_vec_embeddings(
//                     "minishlab/potion-base-2M",
//                     "hf_minilm",
//                 ))
//                 .build(),
//         )
//         .await?;

//         // Make a new dataset where one embedding column is prexisting (from 'single_column'),
//         // and another is made in this dataset.
//         let mut ds = Dataset::new("flightsql:single_column", "multiple_columns");
//         let mut params = HashMap::new();
//         params.insert(
//             "flightsql_endpoint".to_string(),
//             format!("http://{}", api_config.flight_bind_address),
//         );
//         ds.acceleration = Some(Acceleration {
//             enabled: true,
//             ..Default::default()
//         });
//         ds.params = Some(Params::from_string_map(params));
//         ds.columns = vec![
//             Column {
//                 name: "cp_description".to_string(),
//                 description: Some(
//                     "This column has an embedding in the underlying spice instance".to_string(),
//                 ),
//                 full_text_search: None,
//                 embeddings: vec![
//                     ColumnLevelEmbeddingConfig::model("hf_minilm")
//                         .with_row_id("cp_catalog_page_sk"),
//                 ],
//                 metadata: HashMap::new(),
//             },
//             Column {
//                 name: "cp_department".to_string(),
//                 description: Some("This column is newly embedded in this spice app".to_string()),
//                 full_text_search: None,
//                 embeddings: vec![
//                     ColumnLevelEmbeddingConfig::model("hf_minilm")
//                         .with_row_id("cp_catalog_page_sk"),
//                 ],
//                 metadata: HashMap::new(),
//             },
//         ];
//         let app2 = AppBuilder::new("search_app2")
//             .with_dataset(ds)
//             .with_embedding(get_model_to_vec_embeddings(
//                 "minishlab/potion-base-2M",
//                 "hf_minilm",
//             ))
//             .build();

//         run_search(
//             app2,
//             vec![
//                 SearchTestCase::new(
//                     "multi_embedding_parent_child_basic",
//                     SearchTestType::Http(json!({
//                         "text": "new patient",
//                         "limit": 2,
//                         "datasets": ["multiple_columns"]
//                     })),
//                 ),
//                 SearchTestCase::new(
//                     "multi_embedding_parent_child_additional",
//                     SearchTestType::Http(json!({
//                         "text": "new patient",
//                         "limit": 2,
//                         "datasets": ["multiple_columns"],
//                         "additional_columns": ["cp_catalog_number"],
//                     })),
//                 ),
//                 SearchTestCase::new(
//                     "multi_embedding_parent_child_where",
//                     SearchTestType::Http(json!({
//                         "text": "new patient",
//                         "datasets": ["multiple_columns"],
//                         "where": "cp_catalog_page_sk % 2 = 0 and cp_catalog_page_sk >=20"
//                     })),
//                 ),
//             ],
//         )
//         .await
//     }
// }
