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

use crate::models::hf::{get_huggingface_embeddings, get_model_to_vec_embeddings};
use crate::models::openai::get_openai_embeddings;
#[cfg(feature = "s3_vectors")]
use crate::models::s3_vectors::basic_vector_search_tests;
use crate::models::{
    create_api_bindings_config, get_mega_science_dataset, get_mega_science_view, http_post,
};
use crate::utils::{register_test_connectors, runtime_ready_check, test_request_context};
use crate::{DEFAULT_TRACING_MODELS, configure_test_datafusion};
use crate::{init_tracing, utils::init_tracing_with_task_history};
use anyhow::Context;
use app::{App, AppBuilder};
#[cfg(feature = "s3_vectors")]
use datafusion::sql::TableReference;
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
#[cfg(feature = "duckdb")]
use spicepod::vector::VectorStore;
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};
use std::fmt::Display;
use std::sync::Arc;
use std::time::Instant;

use super::{get_tpcds_dataset, sort_json_keys};

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

    #[cfg(feature = "s3_vectors")]
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
    assert_search_response_primary_keys(base_url, &ts.name, &resp).await?;
    assert_score_ordering(&resp)
        .with_context(|| format!("Scores are not in valid ordering for {}", &ts.name))?;
    assert_search_response_snapshot(&ts.name, resp);

    Ok(())
}

async fn assert_search_response_primary_keys(
    base_url: &str,
    test_name: &str,
    resp: &Value,
) -> Result<(), anyhow::Error> {
    if !matches!(
        test_name,
        "multi_embedding_parent_child_basic" | "multi_embedding_parent_child_additional"
    ) {
        return Ok(());
    }

    let results = resp
        .get("results")
        .and_then(Value::as_array)
        .context("Search response missing results array")?;

    for result in results {
        assert_search_result_primary_key_matches(base_url, test_name, result).await?;
    }

    Ok(())
}

fn assert_score_ordering(resp: &Value) -> Result<(), anyhow::Error> {
    let results = resp
        .get("results")
        .and_then(Value::as_array)
        .context("Search response missing results array")?;

    let mut previous_score = f64::MAX;
    for result in results {
        let score: f64 = result
            .get("_score")
            .and_then(Value::as_f64)
            .context("Search responses missing '_score'.")?;

        anyhow::ensure!(
            score <= previous_score,
            "Responses not in descending order. {previous_score} is before {score}."
        );

        previous_score = score;
    }
    Ok(())
}

async fn assert_search_result_primary_key_matches(
    base_url: &str,
    test_name: &str,
    result: &Value,
) -> Result<(), anyhow::Error> {
    let dataset = result
        .get("dataset")
        .and_then(Value::as_str)
        .context("Search result missing dataset")?;
    let primary_key = result
        .get("primary_key")
        .and_then(Value::as_object)
        .context("Search result missing primary_key")?;
    let matches = result
        .get("matches")
        .and_then(Value::as_object)
        .context("Search result missing matches")?;

    let select_list = matches
        .keys()
        .map(|column| quote_sql_identifier(column))
        .collect::<Vec<_>>()
        .join(", ");
    let predicates = primary_key
        .iter()
        .map(|(column, value)| {
            if value.is_null() {
                Ok(format!("{} IS NULL", quote_sql_identifier(column)))
            } else {
                Ok(format!(
                    "{} = {}",
                    quote_sql_identifier(column),
                    sql_literal(value)?
                ))
            }
        })
        .collect::<Result<Vec<_>, anyhow::Error>>()?
        .join(" AND ");
    let sql = format!(
        "SELECT {select_list} FROM {} WHERE {predicates} LIMIT 1",
        quote_sql_path(dataset)
    );
    let rows = http_sql(base_url, &sql)
        .await
        .with_context(|| format!("Failed to validate primary key for {test_name}"))?;
    let rows = rows
        .as_array()
        .context("Primary key lookup did not return an array")?;
    let row = rows
        .first()
        .and_then(Value::as_object)
        .context("Primary key lookup did not return a matching row")?;

    for (column, expected_matches) in matches {
        let actual_value = row
            .get(column)
            .with_context(|| format!("Primary key lookup row missing column {column}"))?;
        let actual_text = json_value_to_text(actual_value)?;

        for expected in expected_matches
            .as_array()
            .context("Search result match values were not an array")?
        {
            let expected_text = json_value_to_text(expected)?;
            anyhow::ensure!(
                actual_text.contains(&expected_text) || expected_text.contains(&actual_text),
                "Search result primary key lookup did not match result content for {test_name}: dataset={dataset}, column={column}, primary_key={primary_key:?}, actual={actual_text:?}, expected={expected_text:?}"
            );
        }
    }

    Ok(())
}

fn assert_search_response_snapshot(test_name: &str, resp: Value) {
    match test_name {
        // S3 Vectors HTTP search results are non-deterministic: the backend may return
        // different items with the same rounded score across runs, or scores can drift
        // across a two-decimal rounding boundary. Use structural validation instead of
        // exact snapshot comparison for these tests.
        name if use_structural_search_response_validation(name) => {
            assert_search_response_structure(name, &resp);
        }
        _ => {
            insta::assert_snapshot!(
                format!("{test_name}_response"),
                serde_json::to_string_pretty(&normalize_search_response_json(resp, true))
                    .expect("search response snapshot serialization should succeed")
            );
        }
    }
}

fn use_structural_search_response_validation(test_name: &str) -> bool {
    let is_s3_vectors_composite =
        test_name.starts_with("s3vectors_composite") && !test_name.contains("with_where");
    let is_s3_vectors_chunking = test_name.starts_with("s3vectors_chunking");

    (is_s3_vectors_composite || is_s3_vectors_chunking) && !test_name.contains("vector_search_sql")
}

/// Validate the structure and invariants of a search response without asserting exact item content.
/// Used for S3 Vectors HTTP search tests where the result set is non-deterministic.
fn assert_search_response_structure(test_name: &str, resp: &Value) {
    let results = resp
        .get("results")
        .and_then(|r| r.as_array())
        .unwrap_or_else(|| panic!("{test_name}: response should have a 'results' array"));

    assert_eq!(
        results.len(),
        4,
        "{test_name}: expected 4 results, got {}",
        results.len()
    );

    let expected_dataset = if test_name.contains("_view") {
        "qs_view"
    } else {
        "qs"
    };

    let mut prev_score = f64::MAX;
    for (i, result) in results.iter().enumerate() {
        // Verify required fields exist
        assert!(
            result.get("_score").is_some(),
            "{test_name}: result[{i}] missing '_score'"
        );
        assert!(
            result.get("matches").is_some(),
            "{test_name}: result[{i}] missing 'matches'"
        );
        assert!(
            result.get("primary_key").is_some(),
            "{test_name}: result[{i}] missing 'primary_key'"
        );

        // Verify dataset name
        let dataset = result.get("dataset").and_then(|d| d.as_str()).unwrap_or("");
        assert_eq!(
            dataset, expected_dataset,
            "{test_name}: result[{i}] dataset should be '{expected_dataset}', got '{dataset}'"
        );

        // Verify scores are in descending order and within [0, 1]
        let score: f64 = result.get("_score").and_then(Value::as_f64).unwrap_or(-1.0);
        assert!(
            (0.0..=1.0).contains(&score),
            "{test_name}: result[{i}] score {score} not in [0, 1]"
        );
        assert!(
            score <= prev_score,
            "{test_name}: result[{i}] score {score} > previous {prev_score} (not descending)"
        );
        prev_score = score;

        // Verify matches contain the 'answer' field
        let matches = result.get("matches").and_then(|m| m.as_object());
        assert!(
            matches.is_some_and(|m| m.contains_key("answer")),
            "{test_name}: result[{i}] matches should contain 'answer'"
        );

        // For additional_columns tests, verify extra fields are returned.
        if test_name.contains("additional_columns") {
            let data = result.get("data").and_then(|d| d.as_object());
            let primary_key = result.get("primary_key").and_then(|p| p.as_object());
            assert!(
                data.is_some_and(|d| !d.is_empty()) || primary_key.is_some_and(|p| p.len() > 1),
                "{test_name}: result[{i}] data or primary_key should contain requested additional columns"
            );
        }
    }
}

#[test]
fn structural_search_response_accepts_additional_columns_in_data() {
    assert_search_response_structure(
        "s3vectors_chunking_additional_columns",
        &json!({
            "duration_ms": 1,
            "results": [
                {"_score": 0.4, "data": {"question": "q1"}, "dataset": "qs", "matches": {"answer": ["a1"]}, "primary_key": {"id": 1}},
                {"_score": 0.3, "data": {"question": "q2"}, "dataset": "qs", "matches": {"answer": ["a2"]}, "primary_key": {"id": 2}},
                {"_score": 0.2, "data": {"question": "q3"}, "dataset": "qs", "matches": {"answer": ["a3"]}, "primary_key": {"id": 3}},
                {"_score": 0.1, "data": {"question": "q4"}, "dataset": "qs", "matches": {"answer": ["a4"]}, "primary_key": {"id": 4}}
            ]
        }),
    );
}

#[test]
fn structural_search_response_accepts_additional_columns_in_primary_key() {
    assert_search_response_structure(
        "s3vectors_composite_additional_columns",
        &json!({
            "duration_ms": 1,
            "results": [
                {"_score": 0.4, "dataset": "qs", "matches": {"answer": ["a1"]}, "primary_key": {"id": 1, "question": "q1"}},
                {"_score": 0.3, "dataset": "qs", "matches": {"answer": ["a2"]}, "primary_key": {"id": 2, "question": "q2"}},
                {"_score": 0.2, "dataset": "qs", "matches": {"answer": ["a3"]}, "primary_key": {"id": 3, "question": "q3"}},
                {"_score": 0.1, "dataset": "qs", "matches": {"answer": ["a4"]}, "primary_key": {"id": 4, "question": "q4"}}
            ]
        }),
    );
}

/// Normalizes vector similarity search response for consistent snapshot testing by replacing dynamic
/// values such as duration and scores with placeholders.
fn normalize_search_response_json(mut json: Value, sort_ties_by_matches: bool) -> Value {
    if let Some(duration) = json.get_mut("duration_ms") {
        *duration = json!("duration_ms_val");
    }
    if let Some(matches) = json.get_mut("results").and_then(|m| m.as_array_mut()) {
        // Sort by score descending, breaking ties by matches content then primary key,
        // to get a stable snapshot ordering.
        // Scores are rounded to 2 decimal places before comparing to absorb ±0.01
        // variance from non-deterministic embeddings (model2vec/s3vectors/OpenAI)
        // across CI runners, matching the previous round_scores behaviour.
        matches.sort_by(|a, b| {
            let Some(score_a) = a.get("_score").and_then(Value::as_f64) else {
                return Ordering::Greater;
            };
            let Some(score_b) = b.get("_score").and_then(Value::as_f64) else {
                return Ordering::Less;
            };

            let score_a = (100.0 * score_a).round() / 100.0;
            let score_b = (100.0 * score_b).round() / 100.0;

            // Descending order
            if score_a > score_b {
                return Ordering::Less;
            } else if score_a < score_b {
                return Ordering::Greater;
            }

            if sort_ties_by_matches {
                let matches_a = a
                    .get("matches")
                    .map(|value| serde_json::to_string(value).unwrap_or_default())
                    .unwrap_or_default();
                let matches_b = b
                    .get("matches")
                    .map(|value| serde_json::to_string(value).unwrap_or_default())
                    .unwrap_or_default();
                let matches_cmp = matches_a.cmp(&matches_b);
                if matches_cmp != Ordering::Equal {
                    return matches_cmp;
                }
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
                && obj.contains_key("_score")
            {
                obj.insert("_score".to_string(), Value::String("[score]".to_string()));
            }
        }
    }

    sort_json_keys(&mut json);

    json
}

fn normalize_search_response(json: Value) -> String {
    serde_json::to_string_pretty(&normalize_search_response_json(json, false))
        .expect("search response snapshot serialization should succeed")
}

/// Normalize a `/v1/sql` HTTP response for stable snapshotting.
///
/// SQL responses are top-level JSON arrays of row objects. When a row contains
/// a `_score` column (e.g. the SELECT aliases `trunc(_score, 2) as _score`),
/// the underlying raw score can vary by ±0.01 across CI runners, which both
/// flips row ordering near rounding boundaries and changes the printed score.
/// Redact `_score` to a placeholder and re-sort on the rounded score (with a
/// stable secondary key on the rest of the row) so snapshots are deterministic.
fn normalize_sql_response_json(mut json: Value) -> Value {
    if let Some(rows) = json.as_array_mut() {
        let has_score = rows
            .iter()
            .any(|r| r.as_object().is_some_and(|o| o.contains_key("_score")));
        if has_score {
            rows.sort_by(|a, b| {
                let score_a = a.get("_score").and_then(Value::as_f64);
                let score_b = b.get("_score").and_then(Value::as_f64);
                if let (Some(sa), Some(sb)) = (score_a, score_b) {
                    let ra = (100.0 * sa).round() / 100.0;
                    let rb = (100.0 * sb).round() / 100.0;
                    if ra > rb {
                        return Ordering::Less;
                    } else if ra < rb {
                        return Ordering::Greater;
                    }
                }
                let key_a = row_tiebreak_key(a);
                let key_b = row_tiebreak_key(b);
                key_a.cmp(&key_b)
            });

            for row in rows {
                if let Some(obj) = row.as_object_mut()
                    && obj.contains_key("_score")
                {
                    obj.insert("_score".to_string(), Value::String("[score]".to_string()));
                }
            }
        }
    }
    json
}

fn row_tiebreak_key(row: &Value) -> String {
    let Some(obj) = row.as_object() else {
        return serde_json::to_string(row).expect("row tiebreak key serialization should succeed");
    };
    let mut sanitized: BTreeMap<String, Value> = BTreeMap::new();
    for (k, v) in obj {
        if k == "_score" {
            continue;
        }
        sanitized.insert(k.clone(), v.clone());
    }
    serde_json::to_string(&sanitized).expect("row tiebreak key serialization should succeed")
}

fn quote_sql_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn quote_sql_path(path: &str) -> String {
    path.split('.')
        .map(quote_sql_identifier)
        .collect::<Vec<_>>()
        .join(".")
}

fn sql_literal(value: &Value) -> Result<String, anyhow::Error> {
    match value {
        Value::Null => Ok("NULL".to_string()),
        Value::Bool(boolean) => Ok(boolean.to_string()),
        Value::Number(number) => Ok(number.to_string()),
        Value::String(string) => Ok(format!("'{}'", string.replace('\'', "''"))),
        _ => Err(anyhow::anyhow!(
            "Unsupported SQL literal value in primary key: {value}"
        )),
    }
}

fn json_value_to_text(value: &Value) -> Result<String, anyhow::Error> {
    match value {
        Value::Null => Ok("null".to_string()),
        Value::Bool(boolean) => Ok(boolean.to_string()),
        Value::Number(number) => Ok(number.to_string()),
        Value::String(string) => Ok(string.clone()),
        _ => serde_json::to_string(value)
            .map_err(|source| anyhow::anyhow!("Failed to stringify JSON value: {source}")),
    }
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
            engine: None,
            params: None,
            aggregation: None,
            max_elements_per_row: None,
        }],
        description: None,
        full_text_search: None,
        r#type: None,
        nullable: None,
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
            engine: None,
            params: None,
            aggregation: None,
            max_elements_per_row: None,
        }],
        description: None,
        full_text_search: None,
        r#type: None,
        nullable: None,
        metadata: HashMap::new(),
    }];
    ds_tpcds_cp
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

                        let normalized = normalize_sql_response_json(resp?);
                        insta::assert_json_snapshot!(test_name.clone(), normalized);

                        if explain_sql {
                            let c: Vec<arrow::record_batch::RecordBatch> = client
                                .sql(format!("EXPLAIN {sql}").as_str())
                                .await?
                                .try_collect()
                                .await?;

                            let mut disp =
                                arrow::util::pretty::pretty_format_batches(&c)?.to_string();
                            disp = replace_s3_vector_index_names(&disp);

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

pub(crate) fn vectors_nonfilterable_col(col: impl Into<Column>) -> Column {
    col.into().with_metadata(
        [(
            "vectors".to_string(),
            serde_json::Value::String("non-filterable".to_string()),
        )]
        .into(),
    )
}

/// This function redacts the `S3Vector` index name from [`S3VectorsQueryExec`] in `LogicalPlan` output.
///
/// It keeps different index names unique via `INDEX_NAME_{i}`.
pub(crate) fn replace_s3_vector_index_names(input: &str) -> String {
    let mut index_map: HashMap<String, String> = HashMap::new();
    let mut counter = 1;

    input
        .lines()
        .map(|line| {
            if !line.contains("S3VectorsQueryExec") {
                return line.to_string();
            }

            // Find the content within parentheses after "S3VectorsQueryExec"
            let Some(start_idx) = line.find("S3VectorsQueryExec (") else {
                return line.to_string();
            };
            let after_paren = start_idx + "S3VectorsQueryExec (".len();
            let Some(end_idx) = line[after_paren..].find(')') else {
                return line.to_string();
            };

            let line_length = line.len();
            let index_name = &line[after_paren..after_paren + end_idx];

            // Get or create a replacement name for this index
            let replacement = index_map
                .entry(index_name.to_string())
                .or_insert_with(|| {
                    let name = format!("INDEX_NAME_{counter}");
                    counter += 1;
                    name
                })
                .clone();

            // Build the new line with the replacement
            let before = &line[..after_paren];
            let after = &line[after_paren + end_idx..line_length - 1];
            format!(
                "{}{}{}{}|",
                before,
                replacement,
                after,
                " ".repeat(line_length - 1 - (before.len() + replacement.len() + after.len()))
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

#[tokio::test]
async fn test_multi_column_search_view() -> Result<(), anyhow::Error> {
    let (ds, views) = get_mega_science_view(
        Some("qs"),
        Some(Column::new("question").with_embeddings(vec![
            ColumnLevelEmbeddingConfig::model("hf_minilm").with_row_id("id"),
        ])),
        Some(Column::new("answer").with_embeddings(vec![
            ColumnLevelEmbeddingConfig::model("hf_minilm").with_row_id("id"),
        ])),
    );

    let mut app = AppBuilder::new("search_app")
        .with_dataset(ds)
        .with_embedding(get_model_to_vec_embeddings(
            "minishlab/potion-base-2M",
            "hf_minilm",
        ));

    for v in views {
        app = app.with_view(v);
    }

    run_search_w_explain(
        app.build(),
        [
        #[cfg(feature = "s3_vectors")]
        basic_vector_search_tests("multi_column_view_answer"),
        #[cfg(not(feature = "s3_vectors"))]
        vec![],
        vec![
            SearchTestCase::new(
                "multi_column_view_basic".to_string(),
                SearchTestType::Http(json!({
                    "text": "second",
                    "limit": 4,
                    "datasets": ["qs"],
                })),
            ),
            SearchTestCase::new(
                "multi_column_view_additional_columns".to_string(),
                SearchTestType::Http(json!({
                    "text": "second",
                    "limit": 4,
                    "datasets": ["qs"],
                    "additional_columns": ["question"],
                })),
            ),
            SearchTestCase::new(
                "multi_column_view_with_where".to_string(),
                SearchTestType::Http(json!({
                    "text": "secondary",
                    "datasets": ["qs"],
                    "where": "subject='math'",
                    "limit": 1,
                })),
            ),
            SearchTestCase::new(
                "multi_column_view_question_vector_search_sql_filters".to_string(),
                SearchTestType::from_sql(
                    "SELECT id, answer, trunc(_score, 3) as _score FROM vector_search(qs, 'secondary', question) where subject!='math' order by _score desc LIMIT 4",
                ),
            ),
            SearchTestCase::new(
                "multi_column_view_question_vector_search_sql_no_score".to_string(),
                SearchTestType::from_sql(
                    "SELECT id, answer FROM vector_search(qs, 'second', question) order by _score desc LIMIT 4",
                ),
            ),
            SearchTestCase::new(
                "multi_column_view_question_vector_search_sql_random".to_string(),
                SearchTestType::from_sql(
                    "SELECT subject FROM vector_search(qs, 'second', question) order by _score desc LIMIT 4",
                ),
            ),
            SearchTestCase::new(
                "multi_column_view_question_vector_search_sql_vectors".to_string(),
                SearchTestType::from_sql(
                    "SELECT id, answer, array_length(question_embedding), round(_score, 1) FROM vector_search(qs, 'second', question) order by _score desc LIMIT 4;",
                ),
            ),
        ]
        ].concat(),
        true,
    )
    .await
}

#[tokio::test]
async fn test_multi_column_search() -> Result<(), anyhow::Error> {
    let ds = get_mega_science_dataset(
        Some("qs"),
        Some(Column::new("question").with_embeddings(vec![
            ColumnLevelEmbeddingConfig::model("hf_minilm").with_row_id("id"),
        ])),
        Some(Column::new("answer").with_embeddings(vec![
                ColumnLevelEmbeddingConfig::model("hf_minilm")
                    .with_row_id("id")
                    .chunking(EmbeddingChunkConfig::enabled().target_chunk_size(64)),
            ])),
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
                "multi_column_keywords",
                SearchTestType::Http(json!({
                    "text": "second",
                    "limit": 4,
                    "datasets": ["qs"],
                    "keywords": ["number"],
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
                SearchTestType::from_sql(
                    "SELECT id, answer, trunc(_score, 3) as _score FROM vector_search(qs, 'secondary', question) where subject!='math' order by _score desc LIMIT 4",
                ),
            ),
            SearchTestCase::new(
                "multi_column_question_vector_search_sql_no_score".to_string(),
                SearchTestType::from_sql(
                    "SELECT id, answer FROM vector_search(qs, 'second', question) order by _score desc LIMIT 4",
                ),
            ),
            SearchTestCase::new(
                "multi_column_question_vector_search_sql_random".to_string(),
                SearchTestType::from_sql(
                    "SELECT subject FROM vector_search(qs, 'second', question) order by _score desc LIMIT 4",
                ),
            ),
            SearchTestCase::new(
                "multi_column_question_vector_search_sql_vectors".to_string(),
                SearchTestType::from_sql(
                    "SELECT id, answer, array_length(question_embedding), round(_score, 1) FROM vector_search(qs, 'second', question) order by _score desc LIMIT 4;",
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
                SearchTestType::from_sql(
                    "SELECT id, question, trunc(_score, 3) FROM vector_search(qs, 'second') order by _score desc LIMIT 4",
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
                "hybrid_single_column_keywords",
                SearchTestType::Http(json!({
                    "text": "second",
                    "limit": 4,
                    "datasets": ["qs"],
                    "keywords": ["number"],
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
                SearchTestType::from_sql(
                    "SELECT id, answer, trunc(_score, 3) FROM text_search(qs, 'second') order by _score desc LIMIT 4",
                ),
            ),
            SearchTestCase::new(
                "hybrid_single_column_sql_vector_search",
                SearchTestType::from_sql(
                    "SELECT id, question, trunc(_score, 3) FROM vector_search(qs, 'second') order by _score desc LIMIT 4",
                ),
            ),
            SearchTestCase::new(
                "hybrid_single_column_sql_vector_search_no_score",
                SearchTestType::from_sql(
                    "SELECT question FROM vector_search(qs, 'second') order by _score desc LIMIT 4",
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
                "hybrid_multiple_column_keywords",
                SearchTestType::Http(json!({
                    "text": "second",
                    "limit": 4,
                    "datasets": ["qs"],
                    "keywords": ["number"],
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
                SearchTestType::from_sql(
                    "SELECT id, answer, trunc(_score, 3) FROM text_search(qs, 'second') order by _score desc LIMIT 4",
                ),
            ),
            SearchTestCase::new(
                "hybrid_multiple_column_sql_text_search_wrong_column",
                SearchTestType::from_sql(
                    "SELECT id, answer, trunc(_score, 3) FROM text_search(qs, 'second', question) order by _score desc LIMIT 4",
                ),
            ).should_fail(),
            SearchTestCase::new(
                "hybrid_multiple_column_sql_vector_search",
                SearchTestType::from_sql(
                    "SELECT id, question, trunc(_score, 3) FROM vector_search(qs, 'second') order by _score desc LIMIT 4",
                ),
            ),
            SearchTestCase::new(
                "hybrid_multiple_column_sql_vector_search_wrong_column",
                SearchTestType::from_sql(
                    "SELECT id, question, trunc(_score, 3) FROM vector_search(qs, 'second', answer) order by _score desc LIMIT 4",
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
                SearchTestType::from_sql(
                    "SELECT id, question, trunc(_fused_score, 3) FROM rrf(vector_search(qs, 'second'), text_search(qs, 'second')) order by _fused_score desc LIMIT 4",
                ),
            ),
            SearchTestCase::new(
                "hybrid_multiple_column_sql_rrf_wrong_column",
                SearchTestType::from_sql(
                    "SELECT id, question, trunc(_score, 3) FROM rrf(vector_search(qs, 'second', answer), text_search(qs, 'second', answer)) order by _fused_score desc LIMIT 4",
                ),
            ).should_fail(),
            SearchTestCase::new(
                "hybrid_multiple_column_sql_rrf_explicit_join",
                SearchTestType::from_sql(
                    "SELECT id, question, trunc(_fused_score, 3) FROM rrf(vector_search(qs, 'second'), text_search(qs, 'second'), join_key => 'id') order by _fused_score desc LIMIT 4",
                ),
            ),
            SearchTestCase::new(
                "hybrid_multiple_column_sql_rrf_explicit_join_wrong_column",
                SearchTestType::from_sql(
                    "SELECT id, question, trunc(_fused_score, 3) FROM rrf(vector_search(qs, 'second'), text_search(qs, 'second'), join_key => 'foobar') order by _fused_score desc LIMIT 4",
                ),
            ).should_fail(),
            SearchTestCase::new(
                "hybrid_multiple_column_sql_rrf_one_subquery_fail",
                SearchTestType::from_sql(
                    "SELECT id, question, trunc(_fused_score, 3) FROM rrf(vector_search(qs, 'second')) order by _fused_score desc LIMIT 4",
                ),
            ).should_fail(),
        ],
    ).await
}

/// Regression test for <https://github.com/spiceai/spiceai/issues/9621>
///
/// This test ensures that RRF queries work correctly with `DuckDB` acceleration.
#[tokio::test]
async fn test_rrf_recency_unboosting_disjoint_regression() -> Result<(), anyhow::Error> {
    let left_path = format!(
        "file:{}/tests/models/test_data/rrf_left_fruit.csv",
        env!("CARGO_MANIFEST_DIR")
    );
    let right_path = format!(
        "file:{}/tests/models/test_data/rrf_right_fruit.csv",
        env!("CARGO_MANIFEST_DIR")
    );

    let mut left_ds = Dataset::new(left_path, "left_fruit");
    left_ds.params = Some(Params::from_string_map(HashMap::from([
        ("file_format".to_string(), "csv".to_string()),
        ("csv_has_header".to_string(), "true".to_string()),
    ])));
    left_ds.acceleration = Some(Acceleration {
        enabled: true,
        ..Default::default()
    });
    left_ds.columns = vec![
        Column::new("content")
            .with_embedding(ColumnLevelEmbeddingConfig::model("hf_minilm").with_row_id("id")),
    ];

    let mut right_ds = Dataset::new(right_path, "right_fruit");
    right_ds.params = Some(Params::from_string_map(HashMap::from([
        ("file_format".to_string(), "csv".to_string()),
        ("csv_has_header".to_string(), "true".to_string()),
    ])));
    right_ds.acceleration = Some(Acceleration {
        enabled: true,
        ..Default::default()
    });
    right_ds.columns = vec![
        Column::new("content")
            .with_embedding(ColumnLevelEmbeddingConfig::model("hf_minilm").with_row_id("id")),
    ];

    let app = AppBuilder::new("search_app")
        .with_embedding(get_model_to_vec_embeddings(
            "minishlab/potion-base-2M",
            "hf_minilm",
        ))
        .with_dataset(left_ds)
        .with_dataset(right_ds)
        .build();

    let _tracing = init_tracing(None);

    test_request_context()
        .scope(async {
            let api_config = start_app(app).await?;
            let http_base_url = format!("http://{}", api_config.http_bind_address);

            let sql = "SELECT id, content, picked_at FROM rrf(vector_search(left_fruit, 'red crispy', content), vector_search(right_fruit, 'red crispy', content), join_key => 'id', k => 0, time_column => 'picked_at', decay_constant => 0.25) ORDER BY _fused_score DESC, id ASC LIMIT 3";
            let resp = http_sql(http_base_url.as_str(), sql).await?;
            insta::with_settings!({
                description => sql
            }, {
                insta::assert_snapshot!("rrf_recency_unboosting_disjoint_regression", normalize_search_response(resp));
            });

            Ok(())
        })
        .await
}

/// Regression test for <https://github.com/spiceai/spiceai/issues/9621>
///
/// This test ensures that RRF queries work correctly with `DuckDB` acceleration.
#[cfg(feature = "duckdb")]
#[tokio::test]
async fn test_rrf_search_duckdb_acceleration() -> Result<(), anyhow::Error> {
    let mut ds = get_mega_science_dataset(
        Some("qs"),
        Some(
            Column::new("question")
                .with_embedding(ColumnLevelEmbeddingConfig::model("hf_minilm").with_row_id("id")),
        ),
        Some(
            Column::new("answer")
                .with_full_text_search(FullTextSearchConfig::enabled().with_row_id("id")),
        ),
    );
    ds.acceleration = Some(Acceleration {
        enabled: true,
        engine: Some("duckdb".to_string()),
        ..Default::default()
    });

    run_search(
        AppBuilder::new("search_app")
            .with_embedding(get_model_to_vec_embeddings(
                "minishlab/potion-base-2M",
                "hf_minilm",
            ))
            .with_dataset(ds)
            .build(),
        vec![
            // Basic hybrid search — the query pattern from the cookbook's bluesky search example
            // (https://github.com/spiceai/cookbook/blob/trunk/search/README.md).
            SearchTestCase::new(
                "hybrid_rrf_duckdb_basic",
                SearchTestType::from_sql(
                    "SELECT id, question, trunc(_fused_score, 3) FROM rrf(text_search(qs, 'second'), vector_search(qs, 'second')) order by _fused_score desc, id asc LIMIT 4",
                ),
            ),
            // Explicit join key with DuckDB acceleration
            SearchTestCase::new(
                "hybrid_rrf_duckdb_explicit_join",
                SearchTestType::from_sql(
                    "SELECT id, question, trunc(_fused_score, 3) FROM rrf(text_search(qs, 'second'), vector_search(qs, 'second'), join_key => 'id') order by _fused_score desc, id asc LIMIT 4",
                ),
            ),
        ],
    ).await
}

#[cfg(feature = "duckdb")]
#[derive(Clone, Copy)]
enum DuckDBHnswConfiguration {
    VectorEngine,
    Embeddings,
}

#[cfg(feature = "duckdb")]
fn duckdb_hnsw_search_dataset(name: &str, configuration: DuckDBHnswConfiguration) -> Dataset {
    let mut dataset = get_mega_science_dataset(
        Some(name),
        Some(
            Column::new("question")
                .with_embedding(ColumnLevelEmbeddingConfig::model("hf_minilm").with_row_id("id")),
        ),
        None,
    );
    let hnsw_params = duckdb_hnsw_params();
    let acceleration_params = match configuration {
        DuckDBHnswConfiguration::VectorEngine => None,
        DuckDBHnswConfiguration::Embeddings => Some(hnsw_params.clone()),
    };

    dataset.acceleration = Some(Acceleration {
        enabled: true,
        engine: Some("duckdb".to_string()),
        refresh_sql: Some(format!("SELECT * FROM {name} LIMIT 128")),
        params: acceleration_params,
        ..Default::default()
    });

    if matches!(configuration, DuckDBHnswConfiguration::VectorEngine) {
        dataset.vectors = Some(VectorStore {
            enabled: true,
            engine: Some("duckdb".to_string()),
            partition_by: Vec::new(),
            params: Some(hnsw_params),
        });
    }

    dataset
}

#[cfg(feature = "duckdb")]
fn duckdb_hnsw_params() -> Params {
    Params::from_string_map(HashMap::from([
        ("duckdb_distance_metric".to_string(), "l2".to_string()),
        ("duckdb_hnsw_m".to_string(), "8".to_string()),
        ("duckdb_hnsw_ef_construction".to_string(), "24".to_string()),
        ("duckdb_hnsw_ef_search".to_string(), "12".to_string()),
    ]))
}

#[cfg(feature = "duckdb")]
#[tokio::test]
async fn test_duckdb_hnsw_search_vector_engine_and_embeddings_config() -> Result<(), anyhow::Error>
{
    let vector_engine_dataset = "duckdb_hnsw_vector_engine";
    let embeddings_dataset = "duckdb_hnsw_embeddings";
    let app = AppBuilder::new("search_app")
        .with_embedding(get_model_to_vec_embeddings(
            "minishlab/potion-base-2M",
            "hf_minilm",
        ))
        .with_dataset(duckdb_hnsw_search_dataset(
            vector_engine_dataset,
            DuckDBHnswConfiguration::VectorEngine,
        ))
        .with_dataset(duckdb_hnsw_search_dataset(
            embeddings_dataset,
            DuckDBHnswConfiguration::Embeddings,
        ))
        .build();

    let api_config = start_app(app).await?;
    let http_base_url = format!("http://{}", api_config.http_bind_address);

    assert_duckdb_hnsw_vector_search(http_base_url.as_str(), vector_engine_dataset).await?;
    assert_duckdb_hnsw_vector_search(http_base_url.as_str(), embeddings_dataset).await
}

#[cfg(feature = "duckdb")]
async fn assert_duckdb_hnsw_vector_search(
    http_base_url: &str,
    dataset_name: &str,
) -> Result<(), anyhow::Error> {
    let query = format!(
        "SELECT id, _score FROM vector_search({dataset_name}, 'second') ORDER BY _score DESC LIMIT 4"
    );
    let response = http_sql(http_base_url, &query).await?;
    let rows = response
        .as_array()
        .context("DuckDB HNSW vector search response should be an array")?;
    anyhow::ensure!(
        rows.len() == 4,
        "DuckDB HNSW vector search for {dataset_name} should return 4 rows, got {}",
        rows.len()
    );

    let mut has_negative_l2_score = false;
    for row in rows {
        let score = row
            .get("_score")
            .and_then(Value::as_f64)
            .context("DuckDB HNSW vector search row should include numeric _score")?;
        anyhow::ensure!(
            score <= f64::EPSILON,
            "DuckDB HNSW vector search for {dataset_name} should use l2 scoring, got positive score {score}"
        );
        has_negative_l2_score |= score.is_sign_negative();
    }
    anyhow::ensure!(
        has_negative_l2_score,
        "DuckDB HNSW vector search for {dataset_name} should produce at least one negative l2 score"
    );

    let explain = http_sql(http_base_url, &format!("EXPLAIN {query}")).await?;
    let plan = explain
        .as_array()
        .and_then(|rows| rows.last())
        .and_then(Value::as_object)
        .and_then(|row| row.get("plan"))
        .and_then(Value::as_str)
        .context("DuckDB HNSW vector search explain response should include a plan")?;
    anyhow::ensure!(
        plan.contains("DuckDBVectorQueryExec"),
        "DuckDB HNSW vector search for {dataset_name} should use DuckDBVectorQueryExec"
    );

    Ok(())
}

#[tokio::test]
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
                "text_search_keywords",
                SearchTestType::Http(json!({
                    "text": "second",
                    "limit": 4,
                    "datasets": ["qs"],
                    "keywords": ["number"],
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
                SearchTestType::from_sql(
                    "SELECT id, answer, trunc(_score, 3) FROM text_search(qs, 'second') order by _score desc LIMIT 4",
                ),
            ),
            SearchTestCase::new(
                "text_search_sql_text_search_projection",
                SearchTestType::from_sql(
                    "SELECT id, answer, question, subject, trunc(_score, 3) as _score FROM text_search(qs, 'second') order by _score desc LIMIT 4",
                ),
            ),
            SearchTestCase::new(
                "text_search_sql_text_search_filters",
                SearchTestType::from_sql(
                    "SELECT id, answer, trunc(_score, 3) as _score FROM text_search(qs, 'secondary') where subject!='math' order by _score desc LIMIT 4",
                ),
            ),
            SearchTestCase::new(
                "text_search_sql_text_search_no_score",
                SearchTestType::from_sql(
                    "SELECT id, answer FROM text_search(qs, 'second') order by _score desc LIMIT 4",
                ),
            ),
            SearchTestCase::new(
                "text_search_sql_text_search_random",
                SearchTestType::from_sql(
                    "SELECT subject FROM text_search(qs, 'second') order by _score desc LIMIT 4",
                ),
            ),
        ],
    )
    .await
}

#[tokio::test]
async fn test_text_search_view() -> Result<(), anyhow::Error> {
    let (ds, views) = get_mega_science_view(
        Some("qs"),
        None,
        Some(
            Column::new("answer")
                .with_full_text_search(FullTextSearchConfig::enabled().with_row_id("id")),
        ),
    );

    let mut app = AppBuilder::new("search_app").with_dataset(ds);
    for v in views {
        app = app.with_view(v);
    }

    run_search_w_explain(app.build(),
        vec![
            SearchTestCase::new(
                "text_search_view_basic",
                SearchTestType::Http(json!({
                    "text": "second",
                    "limit": 4,
                    "datasets": ["qs"],
                })),
            ),
            SearchTestCase::new(
                "text_search_view_additional_columns",
                SearchTestType::Http(json!({
                    "text": "second",
                    "limit": 4,
                    "datasets": ["qs"],
                    "additional_columns": ["question"],
                })),
            ),
            SearchTestCase::new(
                "text_search_view_with_where",
                SearchTestType::Http(json!({
                    "text": "secondary",
                    "datasets": ["qs"],
                    "where": "subject!='math'",
                    "limit": 4,
                })),
            ),
            SearchTestCase::new(
                "text_search_view_basic_without_defined_dataset",
                SearchTestType::Http(json!({
                    "text": "second",
                    "limit": 4,
                })),
            ),
            SearchTestCase::new(
                "text_search_view_sql_text_search_basic",
                SearchTestType::from_sql(
                    "SELECT id, answer, trunc(_score, 3) FROM text_search(qs, 'second') order by _score desc LIMIT 4",
                ),
            ),
            SearchTestCase::new(
                "text_search_view_sql_text_search_projection",
                SearchTestType::from_sql(
                    "SELECT id, answer, question, subject, trunc(_score, 3) as _score FROM text_search(qs, 'second') order by _score desc LIMIT 4",
                ),
            ),
            SearchTestCase::new(
                "text_search_view_sql_text_search_filters",
                SearchTestType::from_sql(
                    "SELECT id, answer, trunc(_score, 3) as _score FROM text_search(qs, 'secondary') where subject!='math' order by _score desc LIMIT 4",
                ),
            ),
            SearchTestCase::new(
                "text_search_view_sql_text_search_no_score",
                SearchTestType::from_sql(
                    "SELECT id, answer FROM text_search(qs, 'second') order by _score desc LIMIT 4",
                ),
            ),
            SearchTestCase::new(
                "text_search_view_sql_text_search_random",
                SearchTestType::from_sql(
                    "SELECT subject FROM text_search(qs, 'second') order by _score desc LIMIT 4",
                ),
            ),
        ],
        true,
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
                SearchTestType::from_sql("SELECT id, answer, trunc(_score, 3) FROM text_search(qs, 'second') order by _score desc LIMIT 4"),
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
                SearchTestType::from_sql("SELECT id, answer, trunc(_score, 3) FROM text_search(qs, 'second') order by _score desc LIMIT 4"),
            ),
        ],
    )
    .await
}

#[tokio::test]
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
                "multi_text_column_keywords",
                SearchTestType::Http(json!({
                    "text": "second",
                    "limit": 4,
                    "datasets": ["qs"],
                    "keywords": ["number"],
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
                SearchTestType::from_sql("SELECT id, answer, trunc(_score, 3) FROM text_search(qs, 'second', answer) order by _score desc LIMIT 4"),
            ),
            SearchTestCase::new(
                "multi_text_column_sql_text_search_basic_question",
                SearchTestType::from_sql("SELECT id, question, trunc(_score, 3) FROM text_search(qs, 'angles', question) order by _score desc LIMIT 4"),
            ),
            SearchTestCase::new(
                // When there are multiple columns, `text_search` needs column explicitly as input.
                "multi_text_column_sql_text_search_error_without_column",
                SearchTestType::from_sql("SELECT id, answer, trunc(_score, 3) FROM text_search(qs, 'second') order by _score desc LIMIT 4"),
            ).should_fail(),
            SearchTestCase::new(
                "multi_text_column_sql_text_search_projection",
                SearchTestType::from_sql("SELECT id, answer, question, subject, trunc(_score, 3) as _score FROM text_search(qs, 'second', answer) order by _score desc LIMIT 4"),
            ),
            SearchTestCase::new(
                "multi_text_column_sql_text_search_filters",
                SearchTestType::from_sql("SELECT id, answer, trunc(_score, 3) as _score FROM text_search(qs, 'secondary', answer) where subject!='math' order by _score desc LIMIT 4"),
            ),
            SearchTestCase::new(
                "multi_text_column_sql_text_search_no_score",
                SearchTestType::from_sql("SELECT id, answer FROM text_search(qs, 'second', answer) order by _score desc LIMIT 4"),
            ),
            SearchTestCase::new(
                "multi_text_column_sql_text_search_random",
                SearchTestType::from_sql("SELECT subject FROM text_search(qs, 'second', answer) order by _score desc LIMIT 4"),
            ),
        ],
    )
    .await
}

#[tokio::test]
async fn test_text_search_metadata() -> Result<(), anyhow::Error> {
    let mut ds = get_mega_science_dataset(
        Some("qs"),
        Some(
            Column::new("question")
                .with_full_text_search(FullTextSearchConfig::enabled().with_row_id("id"))
                .with_metadata(
                    [(
                        "vectors".to_string(),
                        Value::String("non-filterable".to_string()),
                    )]
                    .into(),
                ),
        ),
        Some(
            Column::new("answer")
                .with_full_text_search(FullTextSearchConfig::enabled().with_row_id("id")),
        ),
    );
    ds.columns.push(vectors_nonfilterable_col("subject"));

    run_search_w_explain(
        AppBuilder::new("search_app")
            .with_dataset(ds).build(),
        vec![
            SearchTestCase::new(
                "text_search_metadata_basic",
                SearchTestType::Http(json!({
                    "text": "second",
                    "limit": 4,
                    "datasets": ["qs"],
                })),
            ),
            SearchTestCase::new(
                "text_search_metadata_additional_columns",
                SearchTestType::Http(json!({
                    "text": "second",
                    "limit": 4,
                    "datasets": ["qs"],
                    "additional_columns": ["question"],
                })),
            ),
            SearchTestCase::new(
                "text_search_metadata_with_where",
                SearchTestType::Http(json!({
                    "text": "secondary",
                    "datasets": ["qs"],
                    "where": "subject!='math'",
                    "limit": 4,
                })),
            ),
            SearchTestCase::new(
                "text_search_metadata_sql_text_search_answer",
                SearchTestType::from_sql("SELECT id, answer, trunc(_score, 3) FROM text_search(qs, 'second', answer) order by _score desc LIMIT 4"),
            ),
            SearchTestCase::new(
                "text_search_metadata_sql_text_search_answer_w_question",
                SearchTestType::from_sql("SELECT id, question, trunc(_score, 3) FROM text_search(qs, 'second', answer) order by _score desc LIMIT 4"),
            ),
            SearchTestCase::new(
                "text_search_metadata_sql_text_search_question",
                SearchTestType::from_sql("SELECT id, question, trunc(_score, 3) FROM text_search(qs, 'angles', question) order by _score desc LIMIT 4"),
            ),
            SearchTestCase::new(
                "text_search_metadata_sql_text_search_subject_filter",
                SearchTestType::from_sql("SELECT id, question, trunc(_score, 3) FROM text_search(qs, 'angles', question) where subject='math' order by _score desc LIMIT 4"),
            ),
            SearchTestCase::new(
                "text_search_metadata_sql_text_search_subject_projection",
                SearchTestType::from_sql("SELECT id, subject, trunc(_score, 3) FROM text_search(qs, 'angles', question) order by _score desc LIMIT 4"),
            ),
        ],
        true,
    )
    .await
}

#[cfg(feature = "flightsql")]
#[tokio::test]
#[ignore = "Failing. https://github.com/spiceai/spiceai/issues/10634"]
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
            r#type: None,
            nullable: None,
            metadata: HashMap::new(),
        },
        Column {
            name: "cp_department".to_string(),
            description: Some("This column is newly embedded in this spice app".to_string()),
            full_text_search: None,
            embeddings: vec![
                ColumnLevelEmbeddingConfig::model("hf_minilm").with_row_id("cp_catalog_page_sk"),
            ],
            r#type: None,
            nullable: None,
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
            // Constrain the search space so the parent/child test can assert exact row IDs.
            SearchTestCase::new(
                "multi_embedding_parent_child_basic",
                SearchTestType::Http(json!({
                    "text": "new patient",
                    "limit": 2,
                    "datasets": ["multiple_columns"],
                    "where": "cp_catalog_page_sk = 1 OR cp_catalog_page_sk = 11"
                })),
            ),
            SearchTestCase::new(
                "multi_embedding_parent_child_additional",
                SearchTestType::Http(json!({
                    "text": "new patient",
                    "limit": 2,
                    "datasets": ["multiple_columns"],
                    "where": "cp_catalog_page_sk = 1 OR cp_catalog_page_sk = 11",
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

#[tokio::test(flavor = "multi_thread")]
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
            run_search_test(http_base_url.as_str(), &SearchTestCase::new(
                "with_cache_pre_cache",
                SearchTestType::Http(json!({
                    "text": "new patient",
                    "limit": 100,
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
                        "limit": 100,
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
            "topk_with_fetch_4",
            "EXPLAIN SELECT cp_catalog_page_sk, _score FROM vector_search(spice.public.basic_embedding_search, 'basic') order by _score desc LIMIT 4".to_string(),
            vec!["SortExec: TopK(fetch=4)"]
        ),
        (
            "topk_with_fetch_2_from_parameter_ignores_other_limit",
            "EXPLAIN SELECT cp_catalog_page_sk, _score FROM vector_search(spice.public.basic_embedding_search, 'basic', 2) order by _score desc LIMIT 4".to_string(),
            vec!["SortExec: TopK(fetch=2)"]
        ),
        (
            "topk_with_fetch_3_from_parameter",
            "EXPLAIN SELECT cp_catalog_page_sk, _score FROM vector_search(spice.public.basic_embedding_search, 'basic', 3) order by _score desc".to_string(),
            vec!["SortExec: TopK(fetch=3)"]
        )
    ];

    let api_config = start_app(app).await?;
    let http_base_url = format!("http://{}", api_config.http_bind_address);

    for (name, query, must_contain) in queries {
        let result = http_sql(http_base_url.as_str(), &query).await?;

        insta::assert_snapshot!(
            format!("{name}_response"),
            normalize_search_response(result.clone())
        );

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
