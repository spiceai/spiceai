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

//! Per-table filter-column hit histogram for default-on adaptive cold layout (F4).
//!
//! Scans record which columns appear in pushdown filters. When operators leave
//! `sort_columns` / `cold_clustering_columns` empty, compaction consults
//! [`FilterColumnObservations::top_columns`] and sorts the rewrite by the hottest
//! columns so zone maps prune selective queries without any spicepod setup.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::Schema;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_expr::Expr;
use parking_lot::Mutex;

/// Maximum distinct filter columns retained in the histogram.
const MAX_TRACKED_COLUMNS: usize = 32;

/// Default number of hot columns to feed into compaction sort / Z-order.
pub(crate) const DEFAULT_AUTO_CLUSTER_TOP_K: usize = 2;

/// Bounded per-table filter-column hit counts shared across provider clones.
#[derive(Debug, Default)]
pub(crate) struct FilterColumnObservations {
    state: Mutex<HashMap<String, u64>>,
}

impl FilterColumnObservations {
    /// Create an empty observation map.
    #[must_use]
    pub(crate) fn new() -> Self {
        Self {
            state: Mutex::new(HashMap::new()),
        }
    }

    /// Record columns referenced by the given scan filters (one hit per
    /// distinct column name per call, not per occurrence in a single expression).
    pub(crate) fn record_filters(&self, filters: &[Expr]) {
        if filters.is_empty() {
            return;
        }
        let mut names = Vec::new();
        for filter in filters {
            collect_column_names(filter, &mut names);
        }
        if names.is_empty() {
            return;
        }
        let mut state = self.state.lock();
        for name in names {
            let entry = state.entry(name).or_insert(0);
            *entry = entry.saturating_add(1);
        }
        // Cap cardinality: drop lowest-hit keys when over the limit so memory
        // stays bounded under wide schemas / ad-hoc column churn.
        while state.len() > MAX_TRACKED_COLUMNS {
            // Deterministic victim: the coldest column, breaking ties by name so
            // eviction never depends on the randomized `HashMap` iteration order
            // (non-determinism there would leak into the observed set and thus
            // auto-layout under column churn). Ties retain the lexicographically
            // smaller name, matching `top_columns`' tie-break.
            let victim = state
                .iter()
                .min_by(|a, b| a.1.cmp(b.1).then_with(|| b.0.cmp(a.0)))
                .map(|(name, _)| name.clone());
            if let Some(name) = victim {
                state.remove(&name);
            } else {
                break;
            }
        }
    }

    /// Return up to `top_k` column names with the highest hit counts that exist
    /// in `schema`, highest first. Empty when nothing useful has been observed.
    #[must_use]
    pub(crate) fn top_columns(&self, top_k: usize, schema: &Schema) -> Vec<String> {
        if top_k == 0 {
            return Vec::new();
        }
        let state = self.state.lock();
        let mut ranked: Vec<(&str, u64)> = state
            .iter()
            .filter(|(name, _)| schema.index_of(name.as_str()).is_ok())
            .map(|(name, hits)| (name.as_str(), *hits))
            .collect();
        // Stable tie-break by name so compaction layout is deterministic under
        // equal hit counts (tests and zone-map quality both care).
        ranked.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(b.0)));
        ranked
            .into_iter()
            .take(top_k)
            .map(|(name, _)| name.to_string())
            .collect()
    }

    /// Hit count for a column (0 if never observed). Test helper.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn hits(&self, column: &str) -> u64 {
        self.state.lock().get(column).copied().unwrap_or(0)
    }
}

/// Shared handle stored on [`crate::provider::table::CayenneTableProvider`].
pub(crate) type SharedFilterColumnObservations = Arc<FilterColumnObservations>;

fn collect_column_names(expr: &Expr, out: &mut Vec<String>) {
    let _ = expr.apply(|node| {
        if let Expr::Column(column) = node {
            let name = column.name.as_str();
            if !out.iter().any(|existing| existing == name) {
                out.push(name.to_string());
            }
        }
        Ok(TreeNodeRecursion::Continue)
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field};
    use datafusion_expr::{col, lit};

    fn test_schema() -> Schema {
        Schema::new(vec![
            Field::new("pk", DataType::Int64, false),
            Field::new("region", DataType::Utf8, false),
            Field::new("amount", DataType::Int64, false),
            Field::new("ts", DataType::Int64, false),
        ])
    }

    #[test]
    fn record_filters_counts_distinct_columns_per_call() {
        let observations = FilterColumnObservations::new();
        let filters = vec![col("region").eq(lit("west")), col("amount").gt(lit(10_i64))];
        observations.record_filters(&filters);
        observations.record_filters(&filters);
        assert_eq!(observations.hits("region"), 2);
        assert_eq!(observations.hits("amount"), 2);
        assert_eq!(observations.hits("ts"), 0);
    }

    #[test]
    fn top_columns_ranks_by_hits_and_schema() {
        let observations = FilterColumnObservations::new();
        let schema = test_schema();
        for _ in 0..5 {
            observations.record_filters(&[col("amount").gt(lit(0_i64))]);
        }
        for _ in 0..2 {
            observations.record_filters(&[col("region").eq(lit("east"))]);
        }
        // Unknown column must not surface even if "hot".
        for _ in 0..20 {
            observations.record_filters(&[col("not_in_schema").eq(lit(1_i64))]);
        }
        let top = observations.top_columns(2, &schema);
        assert_eq!(top, vec!["amount".to_string(), "region".to_string()]);
        let top1 = observations.top_columns(1, &schema);
        assert_eq!(top1, vec!["amount".to_string()]);
    }

    #[test]
    fn top_columns_empty_without_observations() {
        let observations = FilterColumnObservations::new();
        let schema = test_schema();
        assert!(observations.top_columns(2, &schema).is_empty());
    }

    #[test]
    fn nested_and_or_still_collects_leaf_columns() {
        let observations = FilterColumnObservations::new();
        let schema = test_schema();
        let filter = col("region")
            .eq(lit("west"))
            .and(col("amount").gt(lit(5_i64)).or(col("ts").lt(lit(100_i64))));
        observations.record_filters(&[filter]);
        let mut top = observations.top_columns(3, &schema);
        top.sort();
        assert_eq!(
            top,
            vec!["amount".to_string(), "region".to_string(), "ts".to_string()]
        );
    }
}
