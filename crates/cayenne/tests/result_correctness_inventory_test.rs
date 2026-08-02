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

//! # Result correctness — inventory + pure compare path
//!
//! Not a performance test. Does not link DuckDB or chDB. Proves:
//! 1. The correctness inventory is complete vs suite SQL sources
//!    (TPC-H/TPC-DS/ClickBench/CH-benCHmark/SSB/SpiceBench/SQLLancer/micro).
//! 2. Shipped `compare_query_result_batches` fails on value mismatches and
//!    passes multiset-equal reordered results.
//!
//! See `tests/correctness/README.md`.

#![allow(clippy::expect_used)]
#![allow(clippy::unwrap_used)]
#![allow(clippy::doc_markdown)]
#![allow(clippy::cloned_ref_to_slice_refs)]

#[path = "correctness/support/mod.rs"]
mod support;

use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use support::compare_actual_results;
use support::inventory::{assert_inventory_complete, build_inventory, inventory_by_suite};
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
    assert!(
        by_suite.get("chbench").map_or(0, Vec::len) >= 20,
        "CH-benCHmark inventory too small: {:?}",
        by_suite.get("chbench").map(Vec::len)
    );
    assert!(
        by_suite.get("ssb").map_or(0, Vec::len) >= 13,
        "SSB inventory too small: {:?}",
        by_suite.get("ssb").map(Vec::len)
    );
    assert!(
        by_suite.get("spicebench").map_or(0, Vec::len) >= 28,
        "SpiceBench inventory too small: {:?}",
        by_suite.get("spicebench").map(Vec::len)
    );
    assert!(
        by_suite.get("sqllancer").map_or(0, Vec::len) >= 20,
        "SQLLancer inventory too small: {:?}",
        by_suite.get("sqllancer").map(Vec::len)
    );
    let inv = build_inventory();
    eprintln!(
        "inventory complete: {} total entries \
         (tpch={} tpcds={} clickbench={} chbench={} ssb={} spicebench={} sqllancer={} micro={})",
        inv.len(),
        by_suite["tpch"].len(),
        by_suite["tpcds"].len(),
        by_suite["clickbench"].len(),
        by_suite["chbench"].len(),
        by_suite["ssb"].len(),
        by_suite["spicebench"].len(),
        by_suite["sqllancer"].len(),
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

    let result = compare_query_result_batches("reorder", &[left], &[right], RowOrder::Multiset)
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

    let ordered = Query::new("ordered".into(), "SELECT v FROM t ORDER BY v".into(), false);
    let unordered = Query::new("unordered".into(), "SELECT v FROM t".into(), false);

    // ORDER BY without LIMIT: multiset (tie order is not guaranteed).
    let out = compare_actual_results(&ordered, &[a.clone()], &[b.clone()]);
    assert!(
        matches!(out, support::ParityOutcome::Pass),
        "ORDER BY without LIMIT must accept multiset reordering: {out:?}"
    );

    // Multiset path: same multiset → pass
    let out = compare_actual_results(&unordered, &[a.clone()], &[b.clone()]);
    assert!(
        matches!(out, support::ParityOutcome::Pass),
        "unordered must accept multiset: {out:?}"
    );

    // ORDER BY + LIMIT: order is part of the result (top-K).
    let ordered_lim = Query::new(
        "ordered_lim".into(),
        "SELECT v FROM t ORDER BY v LIMIT 10".into(),
        false,
    );
    let out = compare_actual_results(&ordered_lim, &[a], &[b]);
    assert!(
        matches!(out, support::ParityOutcome::Fail { .. }),
        "ORDER BY+LIMIT must preserve order: {out:?}"
    );
}

/// Prove the harness routes through the shipped `compare_query_result_batches`.
#[test]
fn harness_compare_actual_results_drives_shipped_path() {
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
    let left = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
    )
    .expect("left");
    let right_reorder = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![3, 1, 2]))],
    )
    .expect("reorder");
    let right_wrong = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1, 2, 99]))],
    )
    .expect("wrong");
    let q = Query::new("t".into(), "SELECT v FROM t".into(), false);

    assert!(
        matches!(
            compare_actual_results(&q, &[left.clone()], &[right_reorder]),
            support::ParityOutcome::Pass
        ),
        "harness must Pass on multiset-equal reordered actual batches"
    );
    assert!(
        matches!(
            compare_actual_results(&q, &[left.clone()], &[right_wrong.clone()]),
            support::ParityOutcome::Fail { .. }
        ),
        "harness must Fail on value mismatch in actual batches"
    );
    let shipped = compare_query_result_batches("t", &[left], &[right_wrong], RowOrder::Multiset)
        .expect("shipped");
    assert!(matches!(
        shipped,
        QueryValidationResult::Fail(QueryValidationFailReason::DataMismatch { .. })
    ));
}
