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

//! Shared Cayenne-side fixture for the maintained-filtered-aggregate benches.
//!
//! `vs_duckdb_maintained_filtered_groupby` and `vs_chdb_maintained_filtered_groupby`
//! must be separate bench binaries (DuckDB and chDB, both bundled, abort if driven
//! in one process). This module keeps the Cayenne fixture — schema, filtered spec,
//! row generator, registry load, and serve decode — identical across both so the
//! registry/spec contract can't drift between them. Each bench pulls it in via
//! `#[path = "maintained_filtered_helpers/common.rs"] mod common;`.
//!
//! Included as a submodule of each bench, so not every item is used by both —
//! hence `#![allow(dead_code)]`.

#![allow(dead_code)]
#![allow(clippy::expect_used)]
#![allow(clippy::cast_possible_wrap)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_sign_loss)]

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

pub const ROW_COUNTS: &[usize] = &[100_000, 1_000_000];
/// `ol_number` in CH-benCH q1 has ~15 distinct values; mirror that low cardinality.
pub const GROUP_COUNT: i64 = 16;
/// `delivery > THRESHOLD` selects ~90% of rows — q1's `ol_delivery_d > <early date>`
/// matches essentially every delivered order line, so the filter is highly
/// selective of NOTHING (a near-pass-through), the hard case for a maintained view
/// (it can't just drop most of the data).
pub const DELIVERY_MODULUS: i64 = 10_000;
pub const DELIVERY_THRESHOLD: i64 = 1_000;
/// Batch size for loading the base table into the registry (bounded allocation).
pub const LOAD_BATCH_ROWS: usize = 65_536;

/// Table schema the registry is built over: `pk, grp, delivery, value`.
pub fn table_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("pk", DataType::Int64, false),
        Field::new("grp", DataType::Int64, false),
        Field::new("delivery", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

/// Output schema of `grp, SUM(value), COUNT(*)` (group key then aggregate outputs).
pub fn output_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("grp", DataType::Int64, false),
        Field::new("value_sum", DataType::Int64, false),
        Field::new("row_count", DataType::Int64, false),
    ]))
}

/// `WHERE delivery > DELIVERY_THRESHOLD` as a physical predicate over the schema.
pub fn delivery_filter() -> Arc<dyn PhysicalExpr> {
    let schema = table_schema();
    binary(
        col("delivery", &schema).expect("col delivery"),
        Operator::Gt,
        lit(DELIVERY_THRESHOLD),
        &schema,
    )
    .expect("delivery > threshold predicate")
}

/// `SUM(value), COUNT(*) GROUP BY grp WHERE delivery > THRESHOLD`.
pub fn filtered_spec() -> MaintainedAggregateSpec {
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

// Deterministic per-row generators, trivial so DuckDB/chDB can reproduce them in
// SQL over `range(n)` and the cross-engine correctness gate holds.
fn grp_of(i: i64) -> i64 {
    i % GROUP_COUNT
}
fn delivery_of(i: i64) -> i64 {
    i % DELIVERY_MODULUS
}
fn value_of(i: i64) -> i64 {
    (i % 2_001) - 1_000
}

/// A `count`-row batch of the table starting at primary key `start`.
pub fn row_batch(start: i64, count: usize) -> RecordBatch {
    let pk: Vec<i64> = (0..count as i64).map(|j| start + j).collect();
    let grp: Vec<i64> = pk.iter().map(|&i| grp_of(i)).collect();
    let delivery: Vec<i64> = pk.iter().map(|&i| delivery_of(i)).collect();
    let value: Vec<i64> = pk.iter().map(|&i| value_of(i)).collect();
    RecordBatch::try_new(
        table_schema(),
        vec![
            Arc::new(Int64Array::from(pk)),
            Arc::new(Int64Array::from(grp)),
            Arc::new(Int64Array::from(delivery)),
            Arc::new(Int64Array::from(value)),
        ],
    )
    .expect("row batch")
}

/// Load `rows` into a fresh filtered registry (with PK index) at epoch 1, in
/// bounded batches.
pub fn load_registry(rows: usize) -> MaintainedAggregateRegistry {
    let schema = table_schema();
    let registry = MaintainedAggregateRegistry::try_new_with_pk(
        std::slice::from_ref(&filtered_spec()),
        &schema,
        &[0],
        usize::MAX,
    )
    .expect("filtered registry");

    let mut batches = Vec::with_capacity(rows.div_ceil(LOAD_BATCH_ROWS));
    let mut start = 0_i64;
    while (start as usize) < rows {
        let count = LOAD_BATCH_ROWS.min(rows - start as usize);
        batches.push(row_batch(start, count));
        start += count as i64;
    }
    registry
        .apply_insert_batches(1, &batches)
        .expect("load registry");
    registry
}

/// Serve the maintained filtered aggregate into a sorted `grp -> (sum, count)`.
pub fn cayenne_result(
    registry: &MaintainedAggregateRegistry,
    epoch: u64,
) -> BTreeMap<i64, (i64, i64)> {
    let batch = registry
        .batch_for_spec(&filtered_spec(), epoch, output_schema())
        .expect("serve must not error")
        .expect("filtered view must serve at the scan epoch");
    decode_grouped(&batch)
}

/// Decode a `grp, sum, count` batch into a sorted map.
pub fn decode_grouped(batch: &RecordBatch) -> BTreeMap<i64, (i64, i64)> {
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
