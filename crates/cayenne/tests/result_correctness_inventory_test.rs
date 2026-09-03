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
    SortOrderViolation, compare_query_result_batches, compare_query_result_batches_with_sort_check,
    has_top_level_limit, has_top_level_order_by, resolve_sort_key,
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
    .expect("sort-checked compare")
    .result;
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
    assert!(
        result.is_fully_verified_pass(),
        "tied rows may be returned in any order: {result:?}"
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
        .expect("desc compare")
        .result,
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
    .expect("two-key compare")
    .result;
    assert!(
        matches!(
            result,
            QueryValidationResult::Fail(QueryValidationFailReason::SortOrderViolation {
                violation: SortOrderViolation { ref column, .. },
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
                nulls_first: None,
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
                    nulls_first: None,
                },
                SortKeyColumn {
                    index: 1,
                    name: "revenue".to_string(),
                    descending: true,
                    nulls_first: None,
                },
            ],
        ),
        (
            "SELECT d_year, SUM(lo_revenue) FROM t GROUP BY d_year ORDER BY SUM(lo_revenue)",
            vec![SortKeyColumn {
                index: 1,
                name: "revenue".to_string(),
                descending: false,
                nulls_first: None,
            }],
        ),
    ] {
        assert_eq!(
            resolve_sort_key(sql, &schema),
            SortKeyResolution::Resolved {
                key: expected,
                unresolved_suffix: None
            },
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
        .expect("null compare")
        .result,
        QueryValidationResult::Pass
    );
}

/// A `NULL` in a leading key must not let the check fall through to the
/// tiebreaker. TPC-DS q71 sorts on a `SUM` that is `NULL` for nine rows, and
/// reading the second key across that boundary reported a violation on a result
/// that was correctly ordered — the engine had simply placed `NULL`s first.
#[test]
fn sort_check_does_not_consult_tiebreaker_across_a_null_in_a_leading_key() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("brand_id", DataType::Int64, false),
        Field::new("ext_price", DataType::Int64, true),
    ]));
    // NULLs first under DESC, then descending values. brand_id descends exactly
    // at the NULL/non-NULL boundary, which is where the tiebreaker must not run.
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 10_005_003, 6_012_008])),
            Arc::new(Int64Array::from(vec![None, None, None, Some(22_262)])),
        ],
    )
    .expect("batch");

    assert_eq!(
        compare_query_result_batches_with_sort_check(
            "null_leading_key",
            "SELECT brand_id, ext_price FROM t ORDER BY ext_price DESC, brand_id",
            &[batch.clone()],
            &[batch],
            RowOrder::Multiset,
        )
        .expect("compare")
        .result,
        QueryValidationResult::Pass
    );
}

// ---------------------------------------------------------------------------
// The two false-green paths an adversarial review reproduced against this
// branch: a NULL key column that swallowed the tiebreaker, and an unverifiable
// ORDER BY that reported as a clean pass.
// ---------------------------------------------------------------------------

/// Two rows that are both `NULL` in a key column are tied under every placement
/// convention, so SQL requires the next key column to decide. Skipping it left
/// TPC-DS q71's nine `NULL`-`SUM` rows free to come back in any `brand_id` order.
#[test]
fn sort_check_consults_the_tiebreaker_inside_a_null_group() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("brand_id", DataType::Int64, false),
        Field::new("ext_price", DataType::Int64, true),
    ]));
    let scrambled_within_null_group = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![2, 1])),
            Arc::new(Int64Array::from(vec![None, None])),
        ],
    )
    .expect("batch");

    let result = compare_query_result_batches_with_sort_check(
        "null_group_tiebreak",
        "SELECT brand_id, ext_price FROM t ORDER BY ext_price DESC, brand_id",
        &[scrambled_within_null_group.clone()],
        &[scrambled_within_null_group],
        RowOrder::Multiset,
    )
    .expect("compare")
    .result;
    assert!(
        matches!(
            result,
            QueryValidationResult::Fail(QueryValidationFailReason::SortOrderViolation {
                violation: SortOrderViolation { ref column, .. },
                ..
            }) if column == "brand_id"
        ),
        "a tie on the NULL key must still require brand_id to ascend, got {result:?}"
    );
}

/// `[2, NULL, 1]` is illegal under `NULLS FIRST` and `NULLS LAST` alike, but
/// neither adjacent pair can be judged on its own. Each key column's non-`NULL`
/// values are compared as a subsequence so the inversion is still caught.
#[test]
fn sort_check_catches_an_inversion_straddling_a_null() {
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]));
    let straddling = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![Some(2), None, Some(1)]))],
    )
    .expect("batch");

    let result = compare_query_result_batches_with_sort_check(
        "straddling_null",
        "SELECT v FROM t ORDER BY v",
        &[straddling.clone()],
        &[straddling],
        RowOrder::Multiset,
    )
    .expect("compare")
    .result;
    assert!(
        matches!(
            result,
            QueryValidationResult::Fail(QueryValidationFailReason::SortOrderViolation { .. })
        ),
        "2 before 1 is out of order under either NULL placement, got {result:?}"
    );
}

/// An `ORDER BY` the checker cannot locate must not read as a verified pass.
#[test]
fn an_unverifiable_sort_key_is_not_reported_as_a_clean_pass() {
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
    let scrambled = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![3, 1, 2]))],
    )
    .expect("batch");

    let comparison = compare_query_result_batches_with_sort_check(
        "unresolvable",
        "SELECT v FROM t ORDER BY not_a_projected_column",
        &[scrambled.clone()],
        &[scrambled],
        RowOrder::Multiset,
    )
    .expect("compare");

    assert_eq!(comparison.result, QueryValidationResult::Pass);
    assert!(
        !comparison.is_fully_verified_pass(),
        "an unchecked ORDER BY must not read as a fully verified pass"
    );
    assert_eq!(
        comparison.unchecked.len(),
        2,
        "both sides should report the hole: {:?}",
        comparison.unchecked
    );
}

/// Dropping a whole key because a later term is unmappable left TPC-DS q70's
/// `lochierarchy` unverified for no reason. The mappable prefix is checked, and
/// the rest is named.
#[test]
fn an_unmappable_term_still_checks_the_prefix_before_it() {
    let schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("lochierarchy", DataType::Int64, false),
        Field::new("rank_within_parent", DataType::Int64, false),
    ]));
    let sql = "SELECT lochierarchy, rank_within_parent FROM t \
               ORDER BY lochierarchy DESC, CASE WHEN lochierarchy = 0 THEN s_state END, \
               rank_within_parent";

    let SortKeyResolution::Resolved {
        key,
        unresolved_suffix,
    } = resolve_sort_key(sql, &schema)
    else {
        panic!("expected the leading term to resolve");
    };
    assert_eq!(key.len(), 1, "only the prefix before the CASE resolves");
    assert_eq!(key[0].name, "lochierarchy");
    assert!(
        unresolved_suffix.is_some(),
        "the dropped suffix must be named"
    );

    // And the prefix is genuinely enforced.
    let descending_violated = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(Int64Array::from(vec![1, 1])),
        ],
    )
    .expect("batch");
    let comparison = compare_query_result_batches_with_sort_check(
        "prefix",
        sql,
        &[descending_violated.clone()],
        &[descending_violated],
        RowOrder::Multiset,
    )
    .expect("compare");
    assert!(
        matches!(
            comparison.result,
            QueryValidationResult::Fail(QueryValidationFailReason::SortOrderViolation { .. })
        ),
        "the resolved prefix must still be enforced, got {:?}",
        comparison.result
    );
}

/// An explicit `NULLS FIRST`/`NULLS LAST` makes placement part of the requested
/// order, so it is enforced rather than left to the engine.
#[test]
fn sort_check_enforces_null_placement_when_the_query_states_it() {
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]));
    let nulls_last = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![Some(1), Some(2), None]))],
    )
    .expect("batch");

    let stated_first = compare_query_result_batches_with_sort_check(
        "nulls_first_stated",
        "SELECT v FROM t ORDER BY v NULLS FIRST",
        &[nulls_last.clone()],
        &[nulls_last.clone()],
        RowOrder::Multiset,
    )
    .expect("compare")
    .result;
    assert!(
        matches!(
            stated_first,
            QueryValidationResult::Fail(QueryValidationFailReason::SortOrderViolation { .. })
        ),
        "NULLS FIRST was stated and violated, got {stated_first:?}"
    );

    let stated_last = compare_query_result_batches_with_sort_check(
        "nulls_last_stated",
        "SELECT v FROM t ORDER BY v NULLS LAST",
        &[nulls_last.clone()],
        &[nulls_last],
        RowOrder::Multiset,
    )
    .expect("compare");
    assert!(
        stated_last.is_fully_verified_pass(),
        "NULLS LAST was stated and honored: {stated_last:?}"
    );
}

/// Both predicates that pick the content-comparison mode must read the top level
/// only — a subquery's `ORDER BY`/`LIMIT` does not constrain the outer result.
#[test]
fn top_level_predicates_ignore_subquery_clauses() {
    assert!(has_top_level_order_by("SELECT v FROM t ORDER BY v"));
    assert!(!has_top_level_order_by(
        "SELECT v FROM (SELECT v FROM t ORDER BY v) AS inner_q"
    ));
    assert!(has_top_level_limit("SELECT v FROM t LIMIT 10"));
    assert!(has_top_level_limit("SELECT v FROM t OFFSET 5"));
    assert!(!has_top_level_limit(
        "SELECT v FROM (SELECT v FROM t LIMIT 10) AS inner_q"
    ));
    assert!(!has_top_level_limit("SELECT v FROM t"));
}

/// A later key column is still constrained *within* a run of rows tied on the
/// columns before it, so a `NULL` there can hide an inversion the same way one
/// in the leading column can. Both adjacent pairs below are unjudged.
#[test]
fn sort_check_catches_an_inversion_straddling_a_null_in_a_later_key() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k1", DataType::Int64, false),
        Field::new("k2", DataType::Int64, true),
    ]));
    let inverted_within_a_tie = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 1])),
            Arc::new(Int64Array::from(vec![Some(2), None, Some(1)])),
        ],
    )
    .expect("batch");

    let result = compare_query_result_batches_with_sort_check(
        "later_key_null",
        "SELECT k1, k2 FROM t ORDER BY k1, k2",
        &[inverted_within_a_tie.clone()],
        &[inverted_within_a_tie],
        RowOrder::Multiset,
    )
    .expect("compare")
    .result;
    assert!(
        matches!(
            result,
            QueryValidationResult::Fail(QueryValidationFailReason::SortOrderViolation {
                violation: SortOrderViolation { ref column, .. },
                ..
            }) if column == "k2"
        ),
        "k2 goes 2 then 1 inside a k1 tie, which no NULL placement makes legal: {result:?}"
    );
}

/// The same shape must stay legal once an earlier key separates the rows: a
/// secondary key only orders within a tie, so it may step backwards freely.
#[test]
fn sort_check_allows_a_later_key_to_restart_when_an_earlier_one_changes() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("cnt", DataType::Int64, false),
        Field::new("state", DataType::Int64, true),
    ]));
    let restarts_per_group = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 2, 2])),
            Arc::new(Int64Array::from(vec![Some(5), None, Some(1), Some(3)])),
        ],
    )
    .expect("batch");

    let comparison = compare_query_result_batches_with_sort_check(
        "group_restart",
        "SELECT cnt, state FROM t ORDER BY cnt, state",
        &[restarts_per_group.clone()],
        &[restarts_per_group],
        RowOrder::Multiset,
    )
    .expect("compare");
    assert!(
        comparison.is_fully_verified_pass(),
        "state may drop from 5 to 1 when cnt changes: {comparison:?}"
    );
}

/// A bare name matching two output columns identifies neither. Guessing the
/// first would check the wrong column; the hole is reported instead.
#[test]
fn an_ambiguous_column_name_is_not_guessed() {
    let schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("id", DataType::Int64, false),
    ]));
    let resolution = resolve_sort_key("SELECT a.id, b.id FROM a, b ORDER BY id", &schema);
    assert!(
        matches!(resolution, SortKeyResolution::Unresolved { .. }),
        "an ambiguous name must report, not pick the first: {resolution:?}"
    );
}

/// A qualified term still resolves when the projection names it exactly, even
/// though the bare name is ambiguous across the result columns.
#[test]
fn a_qualified_name_resolves_through_the_projection() {
    let schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("id", DataType::Int64, false),
    ]));
    assert_eq!(
        resolve_sort_key("SELECT a.id, b.id FROM a, b ORDER BY b.id", &schema),
        SortKeyResolution::Resolved {
            key: vec![SortKeyColumn {
                index: 1,
                name: "id".to_string(),
                descending: false,
                nulls_first: None,
            }],
            unresolved_suffix: None,
        }
    );
}
