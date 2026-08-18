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

use runtime_parameters::TypedParams;
use search::index::duckdb::{DuckDBDistanceMetric, DuckDBHnswOptions};
use spicepod::vector::VectorStore;

/// Typed parameters for the `DuckDB` vector engine, deserialized from
/// `vector_engine.params` after secret resolution.
#[derive(Debug, TypedParams)]
#[params(prefix = "duckdb")]
pub struct DuckDbVectorParams {
    /// Vector similarity metric for `DuckDB` VSS. One of: cosine | l2 | `inner_product`.
    #[param(alias = "metric")]
    pub distance_metric: Option<DuckDBDistanceMetric>,
    /// `DuckDB` VSS HNSW graph parameter m (links per node).
    pub hnsw_m: Option<u32>,
    /// `DuckDB` VSS HNSW build parameter `ef_construction`.
    pub hnsw_ef_construction: Option<u32>,
    /// `DuckDB` VSS query-time `ef_search` setting.
    pub hnsw_ef_search: Option<u32>,
    /// Not yet supported for the `DuckDB` vector engine.
    pub partition_by: Option<String>,
    /// Not supported for the `DuckDB` vector engine.
    #[param(parse_with = crate::store_params::parse_bool)]
    pub spill_writes: Option<bool>,
}

impl DuckDbVectorParams {
    /// The HNSW index options these parameters configure; unset fields keep
    /// the `DuckDB` VSS defaults.
    #[must_use]
    pub fn hnsw_options(&self) -> DuckDBHnswOptions {
        let mut options = DuckDBHnswOptions::default();

        if let Some(metric) = self.distance_metric {
            options.metric = metric;
        }
        options.hnsw_m = self.hnsw_m;
        options.hnsw_ef_construction = self.hnsw_ef_construction;
        options.hnsw_ef_search = self.hnsw_ef_search;

        options
    }

    /// Whether partitioning is configured, via either the `partition_by`
    /// param or the component's `partition_by` field.
    #[must_use]
    pub fn partition_by_configured(&self, vector_store_config: &VectorStore) -> bool {
        self.partition_by.is_some() || !vector_store_config.partition_by.is_empty()
    }

    /// Whether spill writes are enabled.
    #[must_use]
    pub fn spill_writes_enabled(&self) -> bool {
        self.spill_writes == Some(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use runtime_parameters_typed::{ParamsError, TypedParams as _};
    use runtime_secrets::Secrets;
    use secrecy::SecretString;
    use std::sync::Arc;
    use tokio::sync::RwLock;

    async fn try_duckdb_params(values: &[(&str, &str)]) -> Result<DuckDbVectorParams, ParamsError> {
        DuckDbVectorParams::try_from_params(
            "DuckDB vector store",
            values
                .iter()
                .map(|(key, value)| ((*key).to_string(), SecretString::from((*value).to_string())))
                .collect(),
            &Arc::new(RwLock::new(Secrets::default())),
        )
        .await
    }

    async fn duckdb_params(values: &[(&str, &str)]) -> DuckDbVectorParams {
        try_duckdb_params(values)
            .await
            .expect("DuckDB vector parameters should be valid")
    }

    #[tokio::test]
    async fn hnsw_options_accepts_aliases() {
        let params = duckdb_params(&[
            ("duckdb_metric", "ip"),
            ("duckdb_hnsw_m", "16"),
            ("duckdb_hnsw_ef_construction", "64"),
            ("duckdb_hnsw_ef_search", "20"),
        ])
        .await;

        let options = params.hnsw_options();

        assert_eq!(options.metric, DuckDBDistanceMetric::InnerProduct);
        assert_eq!(options.hnsw_m, Some(16));
        assert_eq!(options.hnsw_ef_construction, Some(64));
        assert_eq!(options.hnsw_ef_search, Some(20));
    }

    #[tokio::test]
    async fn hnsw_options_prefers_canonical_names() {
        let params = duckdb_params(&[
            ("duckdb_distance_metric", "l2"),
            ("duckdb_metric", "inner_product"),
            ("duckdb_hnsw_m", "32"),
            ("duckdb_hnsw_ef_construction", "128"),
            ("duckdb_hnsw_ef_search", "40"),
        ])
        .await;

        let options = params.hnsw_options();

        assert_eq!(options.metric, DuckDBDistanceMetric::L2);
        assert_eq!(options.hnsw_m, Some(32));
        assert_eq!(options.hnsw_ef_construction, Some(128));
        assert_eq!(options.hnsw_ef_search, Some(40));
    }

    #[tokio::test]
    async fn rejects_invalid_numeric_values() {
        let err = try_duckdb_params(&[("duckdb_hnsw_m", "large")])
            .await
            .expect_err("invalid hnsw_m should be rejected");
        assert!(
            err.to_string()
                .contains("Invalid value for parameter 'duckdb_hnsw_m'")
        );
    }

    #[tokio::test]
    async fn rejects_invalid_distance_metric() {
        let err = try_duckdb_params(&[("duckdb_distance_metric", "manhattan")])
            .await
            .expect_err("invalid distance metric should be rejected");
        assert!(
            err.to_string()
                .contains("Invalid value for parameter 'duckdb_distance_metric'")
        );
    }

    #[tokio::test]
    async fn unsupported_options_are_detected() {
        let params = duckdb_params(&[
            ("duckdb_partition_by", "bucket(10, id)"),
            ("duckdb_spill_writes", "true"),
        ])
        .await;

        assert!(params.partition_by_configured(&VectorStore::default()));
        assert!(params.spill_writes_enabled());
    }

    #[tokio::test]
    async fn spill_writes_accepts_lenient_boolean_forms() {
        let params = duckdb_params(&[("duckdb_spill_writes", "YES")]).await;
        assert!(params.spill_writes_enabled());

        let params = duckdb_params(&[("duckdb_spill_writes", "0")]).await;
        assert!(!params.spill_writes_enabled());
    }

    #[tokio::test]
    async fn rejects_non_boolean_spill_writes() {
        let err = try_duckdb_params(&[("duckdb_spill_writes", "sometimes")])
            .await
            .expect_err("non-boolean spill_writes should be rejected");
        assert!(
            err.to_string()
                .contains("Invalid value for parameter 'duckdb_spill_writes'")
        );
    }
}
