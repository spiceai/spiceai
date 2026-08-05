/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use elasticsearch::{Client, ClientOptions, Elasticsearch};
use runtime_parameters::TypedParams;
use search::index::elasticsearch::ElasticsearchIndexWriteOptions;
use secrecy::{ExposeSecret, SecretString};

/// Vector similarity metric for Elasticsearch kNN search (the `similarity` of
/// the `dense_vector` mapping).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EsDistanceMetric {
    Cosine,
    L2Norm,
    DotProduct,
    MaxInnerProduct,
}

impl EsDistanceMetric {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            EsDistanceMetric::Cosine => "cosine",
            EsDistanceMetric::L2Norm => "l2_norm",
            EsDistanceMetric::DotProduct => "dot_product",
            EsDistanceMetric::MaxInnerProduct => "max_inner_product",
        }
    }
}

impl FromStr for EsDistanceMetric {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "cosine" => Ok(EsDistanceMetric::Cosine),
            "l2_norm" | "l2" | "euclidean" => Ok(EsDistanceMetric::L2Norm),
            "dot_product" | "dot" => Ok(EsDistanceMetric::DotProduct),
            "max_inner_product" | "mip" => Ok(EsDistanceMetric::MaxInnerProduct),
            other => Err(format!(
                "Expected one of: cosine | l2_norm | dot_product | max_inner_product. Found {other}."
            )),
        }
    }
}

/// Typed parameters for the Elasticsearch vector engine, deserialized from
/// `vector_engine.params` after secret resolution.
#[derive(Debug, TypedParams)]
#[params(prefix = "elasticsearch")]
pub struct ElasticsearchVectorParams {
    /// Elasticsearch cluster URL (e.g., `https://localhost:9200`).
    pub endpoint: String,
    /// Username for Elasticsearch authentication.
    #[param(autoload_secret)]
    pub user: Option<SecretString>,
    /// Password for Elasticsearch authentication.
    #[param(autoload_secret)]
    pub pass: Option<SecretString>,
    /// Elasticsearch index name for storing vectors.
    pub index: Option<String>,
    /// Name of the `dense_vector` field in Elasticsearch. Defaults to the embedding column name.
    pub vector_field: Option<String>,
    /// Vector similarity metric for kNN search. One of: cosine | `l2_norm` | `dot_product` | `max_inner_product`.
    pub distance_metric: Option<EsDistanceMetric>,
    /// HNSW graph parameter m (links per node). Higher = better recall, more memory. ES default: 16.
    pub hnsw_m: Option<u32>,
    /// HNSW graph build parameter `ef_construction` (candidate list size at build time). ES default: 100.
    pub hnsw_ef_construction: Option<u32>,
    /// Total request timeout for the Elasticsearch HTTP client, in time unit format (e.g. 30s, 1m). Default: 30s.
    #[param(runtime, parse_with = duration_parse::parse_duration)]
    pub client_timeout: Option<Duration>,
    /// Connect timeout for the Elasticsearch HTTP client, in time unit format (e.g. 10s). Default: 10s.
    #[param(runtime, parse_with = duration_parse::parse_duration)]
    pub connect_timeout: Option<Duration>,
    /// Maximum number of retry attempts for transient Elasticsearch errors (HTTP 429 / 5xx). Default: 3.
    pub max_retries: Option<u32>,
    /// Initial backoff duration between retries, in time unit format (e.g. 100ms, 1s). Default: 200ms.
    #[param(parse_with = duration_parse::parse_duration)]
    pub retry_initial_backoff: Option<Duration>,
    /// Maximum number of rows to include in a single Elasticsearch `_bulk` request. Used to control memory usage and payload size during writes. Default: 1000.
    #[param(default = "1000")]
    pub batch_write_rows: usize,
    /// JSON object passed as Elasticsearch index settings when creating the index. Existing indexes are not recreated.
    #[param(parse_with = parse_json_object)]
    pub index_settings: Option<serde_json::Value>,
    /// Elasticsearch `number_of_shards` index setting to use when creating the index. Existing indexes are not recreated.
    pub number_of_shards: Option<u32>,
    /// Elasticsearch `number_of_replicas` index setting to use when creating the index. Existing indexes are not recreated.
    pub number_of_replicas: Option<u32>,
    /// Elasticsearch `refresh_interval` index setting to use when creating the index (ES duration syntax; relayed as-is). Existing indexes are not recreated.
    pub refresh_interval: Option<String>,
    /// Temporary Elasticsearch `index.refresh_interval` to apply before full/append writes, then restore afterward (ES duration syntax; relayed as-is). Set to -1 to disable refresh during bulk loading.
    pub bulk_load_refresh_interval: Option<String>,
    /// Run Elasticsearch `_forcemerge` after full/append writes. Default: false.
    #[param(default = "false", parse_with = crate::store_params::parse_bool)]
    pub force_merge_after_write: bool,
    /// Maximum number of segments to use with `_forcemerge` after full/append writes; must be positive. Setting this also enables force merge. Default when `force_merge_after_write=true`: 1.
    pub force_merge_segments: Option<u32>,
    /// Not yet supported for the Elasticsearch vector engine.
    pub partition_by: Option<String>,
    /// Not yet supported for the Elasticsearch vector engine.
    #[param(parse_with = crate::store_params::parse_bool)]
    pub spill_writes: Option<bool>,
}

impl ElasticsearchVectorParams {
    /// Build an Elasticsearch HTTP client from these connection params, applying the
    /// configured timeout and retry options.
    ///
    /// # Errors
    ///
    /// Returns an error if the client fails to construct (e.g. an invalid endpoint URL).
    pub fn client(
        &self,
    ) -> Result<Arc<dyn Elasticsearch>, Box<dyn std::error::Error + Send + Sync>> {
        let opts = build_client_options(
            self.client_timeout,
            self.connect_timeout,
            self.max_retries,
            self.retry_initial_backoff,
        );
        Ok(Arc::new(
            Client::new_with_options(
                &self.endpoint,
                self.user.as_ref().map(ExposeSecret::expose_secret),
                self.pass.as_ref().map(ExposeSecret::expose_secret),
                &opts,
            )
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })?,
        ))
    }

    /// Reject params that require infrastructure the Elasticsearch vector engine does not
    /// yet support (per-partition index routing, spill queues), so misconfigurations fail
    /// loudly instead of being silently ignored.
    ///
    /// # Errors
    ///
    /// Returns an error if `partition_by` or `spill_writes` is set, since neither is
    /// supported yet for the Elasticsearch vector engine.
    pub fn validate(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.partition_by.is_some() {
            return Err(Box::<dyn std::error::Error + Send + Sync>::from(
                "`partition_by` is not yet supported for the Elasticsearch vector engine. Remove the parameter or use the S3 Vectors engine for partitioned workloads.",
            ));
        }
        if self.spill_writes == Some(true) {
            return Err(Box::<dyn std::error::Error + Send + Sync>::from(
                "`spill_writes` is not yet supported for the Elasticsearch vector engine.",
            ));
        }
        Ok(())
    }
}

/// Typed parameters for Elasticsearch full-text search, deserialized from
/// `full_text_search.params` after secret resolution.
///
/// Keys are accepted both bare (`endpoint`) and `elasticsearch_`-prefixed
/// (`elasticsearch_endpoint`): callers normalize bare keys with
/// [`normalize_elasticsearch_prefix`] before deserialization, preserving the
/// FTS path's historical acceptance of both forms.
///
/// A strict subset of [`ElasticsearchVectorParams`]: vector-only keys (e.g.
/// `elasticsearch_hnsw_m`) must stay unknown here so misconfigurations warn
/// instead of being silently accepted.
#[derive(Clone, Debug, TypedParams)]
#[params(prefix = "elasticsearch")]
pub struct ElasticsearchFtsParams {
    /// Elasticsearch cluster URL (e.g., `https://localhost:9200`).
    pub endpoint: Option<String>,
    /// Username for Elasticsearch authentication.
    #[param(autoload_secret)]
    pub user: Option<SecretString>,
    /// Password for Elasticsearch authentication.
    #[param(autoload_secret)]
    pub pass: Option<SecretString>,
    /// Elasticsearch index name for full-text search documents. Defaults to the dataset name.
    pub index: Option<String>,
    /// Maximum number of rows to include in a single Elasticsearch `_bulk` request. Default: 1000.
    #[param(default = "1000")]
    pub batch_write_rows: usize,
    /// Total request timeout for the Elasticsearch HTTP client, in time unit format (e.g. 30s, 1m).
    #[param(parse_with = duration_parse::parse_duration)]
    pub client_timeout: Option<Duration>,
    /// Connect timeout for the Elasticsearch HTTP client, in time unit format (e.g. 10s).
    #[param(parse_with = duration_parse::parse_duration)]
    pub connect_timeout: Option<Duration>,
    /// JSON object passed as Elasticsearch index settings when creating the index. Existing indexes are not recreated.
    #[param(parse_with = parse_json_object)]
    pub index_settings: Option<serde_json::Value>,
    /// Elasticsearch `number_of_shards` index setting to use when creating the index. Existing indexes are not recreated.
    pub number_of_shards: Option<u32>,
    /// Elasticsearch `number_of_replicas` index setting to use when creating the index. Existing indexes are not recreated.
    pub number_of_replicas: Option<u32>,
    /// Elasticsearch `refresh_interval` index setting to use when creating the index (ES duration syntax; relayed as-is). Existing indexes are not recreated.
    pub refresh_interval: Option<String>,
    /// Temporary Elasticsearch `index.refresh_interval` to apply before full/append writes, then restore afterward (ES duration syntax; relayed as-is). Set to -1 to disable refresh during bulk loading.
    pub bulk_load_refresh_interval: Option<String>,
    /// Run Elasticsearch `_forcemerge` after full/append writes. Default: false.
    #[param(default = "false", parse_with = crate::store_params::parse_bool)]
    pub force_merge_after_write: bool,
    /// Maximum number of segments to use with `_forcemerge` after full/append writes; must be positive. Setting this also enables force merge. Default when `force_merge_after_write=true`: 1.
    pub force_merge_segments: Option<u32>,
}

/// Resolved Elasticsearch FTS configuration: typed parameters plus the
/// dataset-dependent index name.
#[derive(Clone)]
pub struct ElasticsearchFtsConfig {
    /// Typed (secrets-resolved) parameters.
    pub params: ElasticsearchFtsParams,
    /// Elasticsearch index name for full-text search documents: the `index`
    /// parameter, or the slugified dataset name (dots to dashes, lowercased —
    /// ES index names disallow uppercase and dots).
    pub es_index: String,
}

/// Historically the FTS path accepted both bare (`endpoint`) and prefixed
/// (`elasticsearch_endpoint`) keys; normalize bare keys to the prefixed form
/// [`ElasticsearchFtsParams`] declares so both keep working.
#[must_use]
pub fn normalize_elasticsearch_prefix<S: std::hash::BuildHasher>(
    resolved: HashMap<String, SecretString, S>,
) -> HashMap<String, SecretString> {
    resolved
        .into_iter()
        .map(|(k, v)| {
            if k.starts_with("elasticsearch_") {
                (k, v)
            } else {
                (format!("elasticsearch_{k}"), v)
            }
        })
        .collect()
}

/// Parses a raw parameter value as a JSON object (`#[param(parse_with = ...)]`
/// parser for `index_settings`).
///
/// # Errors
///
/// Returns an error when the value is not valid JSON or not a JSON object.
pub fn parse_json_object(raw: &str) -> Result<serde_json::Value, String> {
    let value: serde_json::Value =
        serde_json::from_str(raw).map_err(|e| format!("Invalid JSON: {e}"))?;
    if !value.is_object() {
        return Err("Expected a JSON object.".to_string());
    }
    Ok(value)
}

/// Merges the raw `index_settings` JSON object with the shortcut settings
/// (`number_of_shards`, `number_of_replicas`, `refresh_interval`). Shortcut
/// values are relayed as JSON strings, matching what Elasticsearch accepts.
#[must_use]
pub fn merge_index_settings(
    index_settings: Option<&serde_json::Value>,
    number_of_shards: Option<u32>,
    number_of_replicas: Option<u32>,
    refresh_interval: Option<&str>,
) -> Option<serde_json::Value> {
    let mut settings = serde_json::Map::new();

    if let Some(obj) = index_settings.and_then(serde_json::Value::as_object) {
        settings.extend(obj.iter().map(|(k, v)| (k.clone(), v.clone())));
    }

    if let Some(value) = number_of_shards {
        settings.insert(
            "number_of_shards".to_string(),
            serde_json::Value::String(value.to_string()),
        );
    }
    if let Some(value) = number_of_replicas {
        settings.insert(
            "number_of_replicas".to_string(),
            serde_json::Value::String(value.to_string()),
        );
    }
    if let Some(value) = refresh_interval {
        settings.insert(
            "refresh_interval".to_string(),
            serde_json::Value::String(value.to_string()),
        );
    }

    if settings.is_empty() {
        None
    } else {
        Some(serde_json::Value::Object(settings))
    }
}

/// Builds the write-maintenance options from typed force-merge / bulk-load
/// refresh parameters, enforcing that `force_merge_segments` is positive.
///
/// # Errors
///
/// Returns an error when `force_merge_segments` is zero.
pub fn build_write_options(
    bulk_load_refresh_interval: Option<&str>,
    force_merge_after_write: bool,
    force_merge_segments: Option<u32>,
) -> Result<ElasticsearchIndexWriteOptions, Box<dyn std::error::Error + Send + Sync>> {
    if force_merge_segments == Some(0) {
        return Err(
            "Invalid value for Elasticsearch parameter 'force_merge_segments': expected a positive integer."
                .into(),
        );
    }

    let refresh_interval_during_write = bulk_load_refresh_interval
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string);

    Ok(ElasticsearchIndexWriteOptions {
        refresh_interval_during_write,
        force_merge_segments: match (force_merge_after_write, force_merge_segments) {
            (true, None) => Some(1),
            (_, Some(value)) => Some(value),
            (false, None) => None,
        },
    })
}

/// Builds the Elasticsearch HTTP client options from typed timeout/retry
/// parameters; unset values keep the client defaults.
#[must_use]
pub fn build_client_options(
    client_timeout: Option<Duration>,
    connect_timeout: Option<Duration>,
    max_retries: Option<u32>,
    retry_initial_backoff: Option<Duration>,
) -> ClientOptions {
    let mut opts = ClientOptions::default();
    if let Some(d) = client_timeout {
        opts.request_timeout = d;
    }
    if let Some(d) = connect_timeout {
        opts.connect_timeout = d;
    }
    if let Some(n) = max_retries {
        opts.retry.max_retries = n;
    }
    if let Some(d) = retry_initial_backoff {
        opts.retry.initial_backoff = d;
    }
    opts
}

#[cfg(test)]
mod tests {
    use super::*;
    use runtime_parameters_typed::{ParamsError, TypedParams as _};
    use runtime_secrets::Secrets;
    use secrecy::ExposeSecret;
    use std::sync::Arc;
    use tokio::sync::RwLock;

    async fn try_es_params(
        values: &[(&str, &str)],
    ) -> Result<ElasticsearchVectorParams, ParamsError> {
        ElasticsearchVectorParams::try_from_params(
            "Elasticsearch vector store",
            values
                .iter()
                .map(|(key, value)| ((*key).to_string(), SecretString::from((*value).to_string())))
                .collect(),
            &Arc::new(RwLock::new(Secrets::default())),
        )
        .await
    }

    async fn try_fts_params(
        values: &[(&str, &str)],
    ) -> Result<ElasticsearchFtsParams, ParamsError> {
        let raw: HashMap<String, SecretString> = values
            .iter()
            .map(|(key, value)| ((*key).to_string(), SecretString::from((*value).to_string())))
            .collect();
        ElasticsearchFtsParams::try_from_params(
            "Elasticsearch full-text search test",
            normalize_elasticsearch_prefix(raw),
            &Arc::new(RwLock::new(Secrets::default())),
        )
        .await
    }

    #[tokio::test]
    async fn vector_params_apply_defaults_and_parse_typed_fields() {
        let typed = try_es_params(&[
            ("elasticsearch_endpoint", "http://localhost:9200"),
            ("elasticsearch_distance_metric", "l2_norm"),
            ("elasticsearch_hnsw_m", "16"),
            ("client_timeout", "30s"),
            ("elasticsearch_number_of_shards", "2"),
        ])
        .await
        .expect("Elasticsearch vector parameters should be valid");

        assert_eq!(typed.endpoint, "http://localhost:9200");
        assert_eq!(typed.distance_metric, Some(EsDistanceMetric::L2Norm));
        assert_eq!(typed.hnsw_m, Some(16));
        assert_eq!(typed.client_timeout, Some(Duration::from_secs(30)));
        assert_eq!(typed.number_of_shards, Some(2));
        assert_eq!(typed.batch_write_rows, 1000);
        assert!(!typed.force_merge_after_write);
    }

    #[tokio::test]
    async fn vector_params_accept_distance_metric_aliases() {
        let typed = try_es_params(&[("elasticsearch_distance_metric", "mip")])
            .await
            .expect("distance metric alias should be accepted");
        assert_eq!(
            typed.distance_metric,
            Some(EsDistanceMetric::MaxInnerProduct)
        );
        assert_eq!(
            typed.distance_metric.map(EsDistanceMetric::as_str),
            Some("max_inner_product")
        );
    }

    #[tokio::test]
    async fn vector_params_reject_invalid_distance_metric() {
        let err = try_es_params(&[("elasticsearch_distance_metric", "manhattan")])
            .await
            .expect_err("invalid distance metric should be rejected");
        assert!(
            err.to_string()
                .contains("Invalid value for parameter 'elasticsearch_distance_metric'"),
            "unexpected message: {err}"
        );
    }

    #[tokio::test]
    async fn vector_params_reject_non_object_index_settings() {
        let err = try_es_params(&[("elasticsearch_index_settings", "[1, 2]")])
            .await
            .expect_err("non-object index_settings should be rejected");
        assert!(
            err.to_string()
                .contains("Invalid value for parameter 'elasticsearch_index_settings'"),
            "unexpected message: {err}"
        );
    }

    #[tokio::test]
    async fn fts_params_accept_bare_and_prefixed_keys() {
        let typed = try_fts_params(&[
            ("endpoint", "http://localhost:9200"),
            ("elasticsearch_index", "docs"),
            ("user", "elastic"),
            ("client_timeout", "30s"),
        ])
        .await
        .expect("FTS parameters should be valid");

        assert_eq!(typed.endpoint.as_deref(), Some("http://localhost:9200"));
        assert_eq!(typed.index.as_deref(), Some("docs"));
        assert_eq!(
            typed.user.as_ref().map(ExposeSecret::expose_secret),
            Some("elastic")
        );
        assert_eq!(typed.client_timeout, Some(Duration::from_secs(30)));
    }

    #[tokio::test]
    async fn fts_params_apply_defaults() {
        let typed = try_fts_params(&[("endpoint", "http://localhost:9200")])
            .await
            .expect("FTS parameters should be valid");

        assert_eq!(typed.batch_write_rows, 1000);
        assert!(!typed.force_merge_after_write);
        assert!(typed.index.is_none());
        assert!(typed.index_settings.is_none());
    }

    #[tokio::test]
    async fn bool_params_accept_lenient_forms() {
        let typed = try_es_params(&[("elasticsearch_force_merge_after_write", "1")])
            .await
            .expect("lenient boolean forms should be accepted");
        assert!(typed.force_merge_after_write);

        let typed = try_es_params(&[("elasticsearch_spill_writes", "No")])
            .await
            .expect("lenient boolean forms should be accepted");
        assert_eq!(typed.spill_writes, Some(false));

        let typed = try_fts_params(&[("force_merge_after_write", "Yes")])
            .await
            .expect("lenient boolean forms should be accepted");
        assert!(typed.force_merge_after_write);

        let err = try_fts_params(&[("force_merge_after_write", "sometimes")])
            .await
            .expect_err("non-boolean force_merge_after_write should be rejected");
        assert!(
            err.to_string()
                .contains("Invalid value for parameter 'elasticsearch_force_merge_after_write'"),
            "unexpected message: {err}"
        );
    }

    #[tokio::test]
    async fn fts_params_reject_malformed_batch_write_rows() {
        let err = try_fts_params(&[("batch_write_rows", "many")])
            .await
            .expect_err("malformed batch_write_rows should be rejected");
        assert!(
            err.to_string()
                .contains("Invalid value for parameter 'elasticsearch_batch_write_rows'"),
            "unexpected message: {err}"
        );
    }

    #[test]
    fn merge_index_settings_combines_json_and_shortcuts() {
        let index_settings =
            parse_json_object(r#"{"analysis":{"analyzer":{"default":{"type":"standard"}}}}"#)
                .expect("valid JSON object should parse");

        let settings = merge_index_settings(Some(&index_settings), Some(2), Some(0), Some("30s"))
            .expect("settings should be present");

        assert_eq!(
            settings,
            serde_json::json!({
                "analysis": {"analyzer": {"default": {"type": "standard"}}},
                "number_of_shards": "2",
                "number_of_replicas": "0",
                "refresh_interval": "30s",
            })
        );
    }

    #[test]
    fn merge_index_settings_is_none_when_nothing_configured() {
        assert!(merge_index_settings(None, None, None, None).is_none());
    }

    #[test]
    fn build_write_options_enables_force_merge_with_default_segments() {
        let options =
            build_write_options(Some("-1"), true, None).expect("valid write options should build");

        assert_eq!(options.refresh_interval_during_write.as_deref(), Some("-1"));
        assert_eq!(options.force_merge_segments, Some(1));
    }

    #[test]
    fn build_write_options_rejects_zero_force_merge_segments() {
        let error = build_write_options(None, false, Some(0))
            .expect_err("zero force_merge_segments should fail");

        assert!(error.to_string().contains("positive integer"));
    }
}
