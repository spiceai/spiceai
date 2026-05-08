/*
Copyright 2026 The Spice.ai OSS Authors

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

use datafusion::common::utils::quote_identifier;

use super::metric::DuckDBDistanceMetric;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DuckDBHnswOptions {
    pub metric: DuckDBDistanceMetric,
    pub hnsw_m: Option<u32>,
    pub hnsw_ef_construction: Option<u32>,
    pub hnsw_ef_search: Option<u32>,
}

impl Default for DuckDBHnswOptions {
    fn default() -> Self {
        Self {
            metric: DuckDBDistanceMetric::Cosine,
            hnsw_m: None,
            hnsw_ef_construction: None,
            hnsw_ef_search: None,
        }
    }
}

impl DuckDBHnswOptions {
    #[must_use]
    pub fn index_name_for(table_name: &str, embedding_column: &str) -> String {
        let mut raw = format!("__spice_vss_{table_name}_{embedding_column}");
        raw.retain(|c| c.is_ascii_alphanumeric() || c == '_');
        if raw.is_empty() {
            "__spice_vss_index".to_string()
        } else {
            raw
        }
    }

    #[must_use]
    pub fn create_index_sql(
        &self,
        table_name: &str,
        embedding_column: &str,
        index_name: &str,
    ) -> String {
        let mut with_options = vec![format!("metric = '{}'", self.metric.duckdb_hnsw_metric())];
        if let Some(m) = self.hnsw_m {
            with_options.push(format!("m = {m}"));
        }
        if let Some(ef) = self.hnsw_ef_construction {
            with_options.push(format!("ef_construction = {ef}"));
        }
        if let Some(ef) = self.hnsw_ef_search {
            with_options.push(format!("ef_search = {ef}"));
        }

        format!(
            "CREATE INDEX IF NOT EXISTS {} ON {} USING HNSW ({}) WITH ({})",
            quote_identifier(index_name),
            quote_identifier(table_name),
            quote_identifier(embedding_column),
            with_options.join(", ")
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::duckdb::metric::DuckDBDistanceMetric;

    #[test]
    fn hnsw_create_index_sql_includes_configured_options() {
        let options = DuckDBHnswOptions {
            metric: DuckDBDistanceMetric::L2,
            hnsw_m: Some(24),
            hnsw_ef_construction: Some(96),
            hnsw_ef_search: Some(40),
        };

        assert_eq!(
            options.create_index_sql("docs", "body_embedding", "idx_docs_embedding"),
            "CREATE INDEX IF NOT EXISTS idx_docs_embedding ON docs USING HNSW (body_embedding) WITH (metric = 'l2sq', m = 24, ef_construction = 96)"
        );
    }
}
