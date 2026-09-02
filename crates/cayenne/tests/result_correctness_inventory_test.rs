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
//! 3. The sort check rejects a side that does not honor the query's own
//!    top-level `ORDER BY`, while still tolerating permuted ties.
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
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use support::compare_actual_results;
use support::inventory::{assert_inventory_complete, build_inventory, inventory_by_suite};
use test_framework::queries::Query;
use test_framework::queries::validation::{
    QueryValidationFailReason, QueryValidationResult, RowOrder, SortKeyColumn, SortKeyResolution,
    compare_query_result_batches, compare_query_result_batches_with_sort_check, resolve_sort_key,
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

    // ORDER BY without LIMIT: content is compared as a multiset so tied rows may
    // come back in either order, but `[2, 1]` is not a legal answer to `ORDER BY v`
    // — 1 and 2 are not tied. The sort check catches it even though the two sides
    // hold the same rows.
    let out = compare_actual_results(&ordered, &[a.clone()], &[b.clone()]);
    assert!(
        matches!(out, support::ParityOutcome::Fail { .. }),
        "ORDER BY without LIMIT must reject a side whose rows are not sorted: {out:?}"
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

// ---------------------------------------------------------------------------
// Row order: an engine must honor its own top-level ORDER BY
//
// Multiset content equality canonically sorts both sides before comparing, so it
// establishes that two engines returned the same rows and nothing about the order.
// Most of the corpus sorts without a LIMIT — every CH-benCHmark query, every SSB
// query with an ORDER BY, 14 of TPC-H's 28 — and was compared that way, so a wrong
// sort over the right rows compared equal. These pin the check that closes it, and
// the tie tolerance that must survive it.
// ---------------------------------------------------------------------------

fn i64_batch(name: &str, values: Vec<i64>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(name, DataType::Int64, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values))]).expect("batch")
}

/// Two columns, so a tie on the first can be permuted on the second.
fn keyed_batch(keys: Vec<i64>, payload: Vec<&str>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("p", DataType::Utf8, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(keys)),
            Arc::new(StringArray::from(payload)),
        ],
    )
    .expect("batch")
}

#[test]
fn sort_check_rejects_unsorted_side_with_identical_rows() {
    let sorted = i64_batch("v", vec![1, 2, 3]);
    let scrambled = i64_batch("v", vec![3, 1, 2]);

    // Identical multisets: content comparison alone cannot tell these apart.
    assert_eq!(
        compare_query_result_batches(
            "content",
            &[sorted.clone()],
            &[scrambled.clone()],
            RowOrder::Multiset
        )
        .expect("content compare"),
        QueryValidationResult::Pass,
        "precondition: the two sides hold the same rows"
    );

    let result = compare_query_result_batches_with_sort_check(
        "sorted",
        "SELECT v FROM t ORDER BY v",
        &[sorted],
        &[scrambled],
        RowOrder::Multiset,
    )
    .expect("sort-checked compare");
    assert!(
        matches!(
            result,
            QueryValidationResult::Fail(QueryValidationFailReason::SortOrderViolation {
                ref side,
                ..
            }) if side == "right"
        ),
        "expected a SortOrderViolation naming the unsorted side, got {result:?}"
    );
}

#[test]
fn sort_check_tolerates_permuted_ties() {
    // Same key on every row, so any payload order is a legal answer.
    let left = keyed_batch(vec![7, 7, 7], vec!["a", "b", "c"]);
    let right = keyed_batch(vec![7, 7, 7], vec!["c", "a", "b"]);

    let result = compare_query_result_batches_with_sort_check(
        "ties",
        "SELECT k, p FROM t ORDER BY k",
        &[left],
        &[right],
        RowOrder::Multiset,
    )
    .expect("sort-checked compare");
    assert_eq!(
        result,
        QueryValidationResult::Pass,
        "tied rows may be returned in any order"
    );
}

#[test]
fn sort_check_honors_desc_and_multi_column_keys() {
    let descending = i64_batch("v", vec![3, 2, 1]);
    assert_eq!(
        compare_query_result_batches_with_sort_check(
            "desc",
            "SELECT v FROM t ORDER BY v DESC",
            &[descending.clone()],
            &[descending],
            RowOrder::Multiset,
        )
        .expect("desc compare"),
        QueryValidationResult::Pass
    );

    // Sorted on k, unsorted within a k group on the second key.
    let good = keyed_batch(vec![1, 1, 2], vec!["a", "b", "a"]);
    let bad = keyed_batch(vec![1, 1, 2], vec!["b", "a", "a"]);
    let result = compare_query_result_batches_with_sort_check(
        "two_keys",
        "SELECT k, p FROM t ORDER BY k, p",
        &[good],
        &[bad],
        RowOrder::Multiset,
    )
    .expect("two-key compare");
    assert!(
        matches!(
            result,
            QueryValidationResult::Fail(QueryValidationFailReason::SortOrderViolation {
                ref column,
                ..
            }) if column == "p"
        ),
        "expected the violation to name the secondary key, got {result:?}"
    );
}

#[test]
fn sort_key_resolves_ordinals_aliases_and_projected_expressions() {
    let schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("d_year", DataType::Int64, false),
        Field::new("revenue", DataType::Int64, false),
    ]));

    for (sql, expected) in [
        (
            "SELECT d_year, SUM(lo_revenue) AS revenue FROM t GROUP BY d_year ORDER BY 2 DESC",
            vec![SortKeyColumn {
                index: 1,
                name: "revenue".to_string(),
                descending: true,
            }],
        ),
        (
            "SELECT d_year, SUM(lo_revenue) AS revenue FROM t GROUP BY d_year \
             ORDER BY d_year ASC, revenue DESC",
            vec![
                SortKeyColumn {
                    index: 0,
                    name: "d_year".to_string(),
                    descending: false,
                },
                SortKeyColumn {
                    index: 1,
                    name: "revenue".to_string(),
                    descending: true,
                },
            ],
        ),
        (
            "SELECT d_year, SUM(lo_revenue) FROM t GROUP BY d_year ORDER BY SUM(lo_revenue)",
            vec![SortKeyColumn {
                index: 1,
                name: "revenue".to_string(),
                descending: false,
            }],
        ),
    ] {
        assert_eq!(
            resolve_sort_key(sql, &schema),
            SortKeyResolution::Resolved(expected),
            "unexpected resolution for {sql}"
        );
    }

    assert_eq!(
        resolve_sort_key("SELECT v FROM t", &schema),
        SortKeyResolution::Unordered
    );
}

/// An `ORDER BY` inside a subquery constrains that subquery, not the result, so
/// it must not be read as a sort key for the outer rows.
#[test]
fn sort_key_ignores_order_by_below_the_top_level() {
    let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
    assert_eq!(
        resolve_sort_key(
            "SELECT v FROM (SELECT v FROM t ORDER BY v) AS inner_q",
            &schema
        ),
        SortKeyResolution::Unordered
    );
}

/// A key the parser cannot map onto an output column must report itself rather
/// than silently reading as ordered — an unchecked cell has to stay countable.
#[test]
fn unresolvable_sort_key_reports_a_reason() {
    let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
    let resolution = resolve_sort_key("SELECT v FROM t ORDER BY other_col", &schema);
    assert!(
        matches!(resolution, SortKeyResolution::Unresolved { .. }),
        "expected Unresolved, got {resolution:?}"
    );
}

/// Engines disagree on where NULLs sort absent an explicit `NULLS FIRST`/`LAST`,
/// so a key pair involving a NULL is treated as tied rather than as a violation.
#[test]
fn sort_check_does_not_police_null_placement() {
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]));
    let nulls_last = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![Some(1), Some(2), None]))],
    )
    .expect("nulls last");
    let nulls_first = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![None, Some(1), Some(2)]))],
    )
    .expect("nulls first");

    assert_eq!(
        compare_query_result_batches_with_sort_check(
            "nulls",
            "SELECT v FROM t ORDER BY v",
            &[nulls_last],
            &[nulls_first],
            RowOrder::Multiset,
        )
        .expect("null compare"),
        QueryValidationResult::Pass
    );
}
