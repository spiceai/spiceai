// Copyright 2026 The Spice.ai OSS Authors
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

//! Correctness tests for PREDICATE-AWARE (filtered) maintained aggregates.
//!
//! The shipped maintained-aggregate registry only serves a query when its plan
//! is a clean unfiltered grouped aggregate directly over the Cayenne scan — so
//! every real CH-benCH analytical query (all carry a `WHERE`) re-scans O(rows).
//! These tests cover the lever that closes that gap: the view maintains the
//! aggregate over only the rows its filter selects, and serves a query carrying
//! the identical predicate from that maintained state.
//!
//! The hard part is correctness under CDC UPDATE/DELETE when the filter status
//! of a row CHANGES — a row updated out of the predicate must drop its old
//! contribution; one updated into the predicate must start contributing. Each
//! test compares the maintained answer against a from-scratch recompute over the
//! effective (post-CDC) dataset filtered by the same predicate.

#![allow(clippy::expect_used)]
use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{Array, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use cayenne::maintained_aggregate::{
    MaintainedAggregateExpr, MaintainedAggregateFunction, MaintainedAggregateRegistry,
    MaintainedAggregateSpec,
};
use datafusion::logical_expr::Operator;
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::expressions::{binary, col, lit};

/// Table schema: `pk` (primary key), `grp` (GROUP BY key), `delivery` (the
/// filter column, mirroring CH-benCH q1's `ol_delivery_d`), `value` (summed).
fn table_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("pk", DataType::Int64, false),
        Field::new("grp", DataType::Int64, false),
        Field::new("delivery", DataType::Int64, true),
        Field::new("value", DataType::Int64, true),
    ]))
}

/// Output schema of `grp, SUM(value), COUNT(*)`: group key then aggregate
/// outputs (both `Int64`). Field names are irrelevant to the registry's
/// type-only output-schema check; positions and types are what matter.
fn output_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("grp", DataType::Int64, false),
        Field::new("value_sum", DataType::Int64, true),
        Field::new("row_count", DataType::Int64, false),
    ]))
}

const DELIVERY_THRESHOLD: i64 = 5_000;

/// `WHERE delivery > 5000` as a physical predicate over [`table_schema`].
fn delivery_filter() -> Arc<dyn PhysicalExpr> {
    let schema = table_schema();
    binary(
        col("delivery", &schema).expect("col delivery"),
        Operator::Gt,
        lit(DELIVERY_THRESHOLD),
        &schema,
    )
    .expect("build delivery > threshold predicate")
}

/// `SUM(value), COUNT(*) GROUP BY grp WHERE delivery > 5000`.
fn filtered_spec() -> MaintainedAggregateSpec {
    MaintainedAggregateSpec {
        group_by: vec!["grp".to_string()],
        aggregates: vec![
            MaintainedAggregateExpr {
                function: MaintainedAggregateFunction::Sum,
                column: Some("value".to_string()),
            },
            MaintainedAggregateExpr {
                function: MaintainedAggregateFunction::Count,
                column: None,
            },
        ],
        filter: Some(delivery_filter()),
    }
}

/// One logical table row.
#[derive(Clone, Copy)]
struct Row {
    pk: i64,
    grp: i64,
    delivery: i64,
    value: i64,
}

fn batch(rows: &[Row]) -> RecordBatch {
    RecordBatch::try_new(
        table_schema(),
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.pk).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.grp).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.delivery).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.value).collect::<Vec<_>>(),
            )),
        ],
    )
    .expect("build batch")
}

/// Single-column `pk` batch for [`MaintainedAggregateRegistry::apply_pk_deletes`].
fn pk_delete_batch(pks: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("pk", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from(pks.to_vec()))],
    )
    .expect("build pk delete batch")
}

/// Ground truth: `SUM(value), COUNT(*) GROUP BY grp` over the rows that pass the
/// filter, computed from scratch (the DuckDB-shaped recompute the lever beats).
fn recompute(state: &BTreeMap<i64, Row>) -> BTreeMap<i64, (i64, i64)> {
    let mut truth: BTreeMap<i64, (i64, i64)> = BTreeMap::new();
    for row in state.values() {
        if row.delivery > DELIVERY_THRESHOLD {
            let entry = truth.entry(row.grp).or_insert((0, 0));
            entry.0 += row.value;
            entry.1 += 1;
        }
    }
    truth
}

/// Materialize the served batch into a sorted `grp -> (sum, count)` map.
fn served(registry: &MaintainedAggregateRegistry, epoch: u64) -> BTreeMap<i64, (i64, i64)> {
    let batch = registry
        .batch_for_spec(&filtered_spec(), epoch, output_schema())
        .expect("serve must not error")
        .expect("a fresh filtered view at the scan epoch must serve");
    let grp = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("grp Int64");
    let sum = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("sum Int64");
    let count = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count Int64");
    let mut out = BTreeMap::new();
    for row in 0..batch.num_rows() {
        out.insert(grp.value(row), (sum.value(row), count.value(row)));
    }
    out
}

fn assert_matches_recompute(
    registry: &MaintainedAggregateRegistry,
    epoch: u64,
    state: &BTreeMap<i64, Row>,
    label: &str,
) {
    let truth = recompute(state);
    let got = served(registry, epoch);
    assert_eq!(
        got, truth,
        "{label}: maintained filtered serve diverged from a from-scratch recompute"
    );
}

#[test]
fn filtered_maintain_serve_matches_recompute_across_cdc_lifecycle() {
    let schema = table_schema();
    let registry = MaintainedAggregateRegistry::try_new_with_pk(
        std::slice::from_ref(&filtered_spec()),
        &schema,
        &[0], // pk column index
        usize::MAX,
    )
    .expect("build filtered registry with PK index");

    // Mirror of the effective table, for the from-scratch recompute oracle.
    let mut state: BTreeMap<i64, Row> = BTreeMap::new();
    let mut epoch = 0_u64;

    // --- Insert: a mix of matching and non-matching rows -------------------
    // pk0 matches (delivery 10000), pk1 excluded (delivery 1000), pk2 matches.
    let initial = [
        Row {
            pk: 0,
            grp: 1,
            delivery: 10_000,
            value: 100,
        },
        Row {
            pk: 1,
            grp: 1,
            delivery: 1_000,
            value: 999,
        },
        Row {
            pk: 2,
            grp: 2,
            delivery: 8_000,
            value: 50,
        },
    ];
    epoch += 1;
    registry
        .apply_insert_batches(epoch, &[batch(&initial)])
        .expect("apply initial inserts");
    for row in initial {
        state.insert(row.pk, row);
    }
    assert_matches_recompute(&registry, epoch, &state, "after insert");
    // Concretely: grp1={sum:100,count:1} (pk1 filtered out), grp2={sum:50,count:1}.
    assert_eq!(
        served(&registry, epoch).get(&1),
        Some(&(100, 1)),
        "non-matching pk1 must NOT contribute to grp1"
    );

    // --- Update OUT of the predicate: pk0 delivery 10000 -> 2000 -----------
    // Its old contribution (grp1 +100) must be retracted; grp1 becomes empty.
    let update_out = [Row {
        pk: 0,
        grp: 1,
        delivery: 2_000,
        value: 100,
    }];
    epoch += 1;
    registry
        .apply_insert_batches(epoch, &[batch(&update_out)])
        .expect("apply update-out");
    state.insert(0, update_out[0]);
    assert_matches_recompute(&registry, epoch, &state, "after update out of predicate");
    assert_eq!(
        served(&registry, epoch).get(&1),
        None,
        "grp1 must disappear once its only matching row leaves the predicate"
    );

    // --- Update INTO the predicate: pk1 delivery 1000 -> 9000 --------------
    // pk1 was never indexed (it never matched); now it must start contributing.
    let update_in = [Row {
        pk: 1,
        grp: 1,
        delivery: 9_000,
        value: 7,
    }];
    epoch += 1;
    registry
        .apply_insert_batches(epoch, &[batch(&update_in)])
        .expect("apply update-in");
    state.insert(1, update_in[0]);
    assert_matches_recompute(&registry, epoch, &state, "after update into predicate");
    assert_eq!(
        served(&registry, epoch).get(&1),
        Some(&(7, 1)),
        "pk1 must contribute to grp1 once it enters the predicate"
    );

    // --- Delete a matching row: pk2 ----------------------------------------
    epoch += 1;
    registry
        .apply_pk_deletes(epoch, &pk_delete_batch(&[2]))
        .expect("apply delete pk2");
    state.remove(&2);
    assert_matches_recompute(&registry, epoch, &state, "after delete");
    assert_eq!(
        served(&registry, epoch).get(&2),
        None,
        "grp2 must disappear once its only row is deleted"
    );

    // --- Delete a NON-matching row is a no-op: pk0 (currently excluded) -----
    epoch += 1;
    registry
        .apply_pk_deletes(epoch, &pk_delete_batch(&[0]))
        .expect("apply delete pk0 (non-matching)");
    state.remove(&0);
    assert_matches_recompute(&registry, epoch, &state, "after delete of non-matching row");
}

#[test]
fn filtered_view_does_not_serve_unfiltered_query_and_vice_versa() {
    let schema = table_schema();
    let rows = [
        Row {
            pk: 0,
            grp: 1,
            delivery: 10_000,
            value: 100,
        },
        Row {
            pk: 1,
            grp: 1,
            delivery: 1_000,
            value: 999,
        },
    ];

    // A FILTERED view must NOT answer an UNFILTERED query (would return only the
    // filtered subset for a query that wants every row — silently wrong).
    let filtered = MaintainedAggregateRegistry::try_new_with_pk(
        std::slice::from_ref(&filtered_spec()),
        &schema,
        &[0],
        usize::MAX,
    )
    .expect("filtered registry");
    filtered
        .apply_insert_batches(1, &[batch(&rows)])
        .expect("apply");
    let unfiltered_query = MaintainedAggregateSpec {
        filter: None,
        ..filtered_spec()
    };
    assert!(
        filtered
            .batch_for_spec(&unfiltered_query, 1, output_schema())
            .expect("serve")
            .is_none(),
        "a filtered view must not answer an unfiltered query"
    );

    // An UNFILTERED view must NOT answer a FILTERED query (would return every row
    // for a query that wants only the subset — also silently wrong).
    let unfiltered_spec = MaintainedAggregateSpec {
        filter: None,
        ..filtered_spec()
    };
    let unfiltered = MaintainedAggregateRegistry::try_new_with_pk(
        std::slice::from_ref(&unfiltered_spec),
        &schema,
        &[0],
        usize::MAX,
    )
    .expect("unfiltered registry");
    unfiltered
        .apply_insert_batches(1, &[batch(&rows)])
        .expect("apply");
    assert!(
        unfiltered
            .batch_for_spec(&filtered_spec(), 1, output_schema())
            .expect("serve")
            .is_none(),
        "an unfiltered view must not answer a filtered query"
    );
    // ...but it DOES answer the matching unfiltered query (sanity: both rows sum).
    let served = unfiltered
        .batch_for_spec(&unfiltered_spec, 1, output_schema())
        .expect("serve")
        .expect("unfiltered view serves the unfiltered query");
    let sum = served
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("sum");
    assert_eq!(
        sum.value(0),
        100 + 999,
        "unfiltered view must sum every row (both pk0 and pk1)"
    );
}
