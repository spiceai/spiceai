// Copyright 2024-2026 The Spice.ai OSS Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Inventory completeness + pure comparison-path tests for Cayenne parity.
//!
//! These tests do not link DuckDB or chDB. They prove:
//! 1. The coverage inventory is complete relative to suite query sources.
//! 2. The shipped `compare_query_result_batches` path fails on value mismatches
//!    and passes on multiset-equal reordered results.

#![allow(clippy::expect_used)]
#![allow(clippy::unwrap_used)]

mod parity;

use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parity::compare_results;
use parity::inventory::{assert_inventory_complete, build_inventory, inventory_by_suite};
use test_framework::queries::Query;
use test_framework::queries::validation::{
    QueryValidationFailReason, QueryValidationResult, RowOrder, compare_query_result_batches,
};

#[test]
fn inventory_is_complete_relative_to_suite_sources() {
    assert_inventory_complete();
    let by_suite = inventory_by_suite();
    assert!(
        by_suite.get("tpch").map_or(0, Vec::len) >= 28,
        "TPC-H inventory too small: {:?}",
        by_suite.get("tpch").map(Vec::len)
    );
    assert!(
        by_suite.get("tpcds").map_or(0, Vec::len) >= 90,
        "TPC-DS inventory too small: {:?}",
        by_suite.get("tpcds").map(Vec::len)
    );
    assert!(
        by_suite.get("clickbench").map_or(0, Vec::len) >= 40,
        "ClickBench inventory too small: {:?}",
        by_suite.get("clickbench").map(Vec::len)
    );
    assert!(
        by_suite.get("micro").map_or(0, Vec::len) >= 10,
        "micro inventory too small: {:?}",
        by_suite.get("micro").map(Vec::len)
    );
    let inv = build_inventory();
    eprintln!(
        "inventory complete: {} total entries (tpch={} tpcds={} clickbench={} micro={})",
        inv.len(),
        by_suite["tpch"].len(),
        by_suite["tpcds"].len(),
        by_suite["clickbench"].len(),
        by_suite["micro"].len(),
    );
}

#[test]
fn shipped_compare_detects_value_mismatch_not_only_counts() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Utf8, false),
        Field::new("v", DataType::Int64, false),
    ]));
    let left = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
            Arc::new(Int64Array::from(vec![1, 2, 3])),
        ],
    )
    .expect("left");
    // Same row count, different values — row-count-only checks would pass.
    let right = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
            Arc::new(Int64Array::from(vec![1, 2, 999])),
        ],
    )
    .expect("right");

    let result =
        compare_query_result_batches("value_mismatch", &[left], &[right], RowOrder::Multiset)
            .expect("compare");
    assert!(
        matches!(
            result,
            QueryValidationResult::Fail(QueryValidationFailReason::DataMismatch { .. })
        ),
        "expected DataMismatch, got {result:?}"
    );
}

#[test]
fn shipped_compare_passes_on_reordered_multiset() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Utf8, false),
        Field::new("v", DataType::Int64, false),
    ]));
    let left = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec!["a", "b"])),
            Arc::new(Int64Array::from(vec![1, 2])),
        ],
    )
    .expect("left");
    let right = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec!["b", "a"])),
            Arc::new(Int64Array::from(vec![2, 1])),
        ],
    )
    .expect("right");

    let result =
        compare_query_result_batches("reorder", &[left], &[right], RowOrder::Multiset)
            .expect("compare");
    assert_eq!(result, QueryValidationResult::Pass);
}

#[test]
fn compare_results_wrapper_uses_order_by_from_sql() {
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
    let a = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1, 2]))],
    )
    .expect("a");
    let b = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![2, 1]))],
    )
    .expect("b");

    let ordered = Query::new(
        "ordered".into(),
        "SELECT v FROM t ORDER BY v".into(),
        false,
    );
    let unordered = Query::new("unordered".into(), "SELECT v FROM t".into(), false);

    // ORDER BY path: different order → fail
    let out = compare_results(&ordered, &[a.clone()], &[b.clone()]);
    assert!(
        matches!(out, parity::ParityOutcome::Fail { .. }),
        "ORDER BY must preserve order: {out:?}"
    );

    // Multiset path: same multiset → pass
    let out = compare_results(&unordered, &[a], &[b]);
    assert!(
        matches!(out, parity::ParityOutcome::Pass),
        "unordered must accept multiset: {out:?}"
    );
}
