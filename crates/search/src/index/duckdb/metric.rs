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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DuckDBDistanceMetric {
    Cosine,
    L2,
    InnerProduct,
}

impl DuckDBDistanceMetric {
    #[must_use]
    pub fn duckdb_hnsw_metric(self) -> &'static str {
        match self {
            Self::Cosine => "cosine",
            Self::L2 => "l2sq",
            Self::InnerProduct => "ip",
        }
    }

    #[must_use]
    pub(crate) fn distance_expr(self, column: &str, vector_literal: &str) -> String {
        let column = quote_identifier(column);
        match self {
            Self::Cosine => format!("array_cosine_distance({column}, {vector_literal})"),
            Self::L2 => format!("array_distance({column}, {vector_literal})"),
            Self::InnerProduct => {
                format!("array_negative_inner_product({column}, {vector_literal})")
            }
        }
    }

    #[must_use]
    pub(crate) fn score_expr(self, column: &str, vector_literal: &str) -> String {
        let column = quote_identifier(column);
        match self {
            Self::Cosine => format!("1.0 - array_cosine_distance({column}, {vector_literal})"),
            Self::L2 => format!("-array_distance({column}, {vector_literal})"),
            Self::InnerProduct => format!("array_inner_product({column}, {vector_literal})"),
        }
    }

    /// Compute score from a pre-computed distance alias (used in the outer CTE query).
    #[must_use]
    pub(crate) fn cte_score_expr(self, distance_alias: &str) -> String {
        match self {
            Self::Cosine => format!("1.0 - {distance_alias}"),
            Self::L2 | Self::InnerProduct => format!("-{distance_alias}"),
        }
    }
}

impl TryFrom<&str> for DuckDBDistanceMetric {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value.trim().to_ascii_lowercase().as_str() {
            "cosine" => Ok(Self::Cosine),
            "l2" | "l2_norm" | "euclidean" | "l2sq" => Ok(Self::L2),
            "ip" | "inner_product" | "dot" | "dot_product" | "max_inner_product" => {
                Ok(Self::InnerProduct)
            }
            other => Err(format!(
                "Invalid DuckDB vector distance metric '{other}'. Expected one of: cosine | l2 | inner_product."
            )),
        }
    }
}
