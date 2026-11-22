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

//! How long the change sink takes to group one CDC batch into the writes it
//! applies (`group_into_sub_batches`), for a batch whose rows carry a primary
//! key and for a keyless one, as an append stream (`DoExchange`) sends.
//!
//! Run: `cargo bench -p runtime-acceleration --bench change_sink_grouping`.
//! Each sample times the same number of calls (flat sampling), so a sample's
//! time divided by its iterations is one call's time, and the tail can be read
//! from `target/criterion/group_into_sub_batches/<case>/new/sample.json`.

#![expect(clippy::expect_used, reason = "a benchmark fails loudly on bad input")]

use std::hint::black_box;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, Float64Array, Int32Array, Int64Array, ListArray, StringArray, StructArray,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use criterion::{Criterion, SamplingMode, criterion_group, criterion_main};
use data_components::cdc::{ChangeBatch, changes_schema};
use runtime_acceleration::change_sink::provider::cdc::group_into_sub_batches;

/// Rows per batch, the default `DataFusion` batch size.
const ROWS: usize = 8192;

/// A batch of `ROWS` upserts of `(id, customer_id, amount)`, each keyed by
/// `id` when `keyed`, or with a null key list, as a keyless producer sends.
fn batch(keyed: bool) -> ChangeBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("customer_id", DataType::Int64, true),
        Field::new("amount", DataType::Float64, true),
    ]));
    let ids = (0..ROWS).map(|row| i64::try_from(row).expect("row fits in i64"));
    let data = StructArray::new(
        schema.fields().clone(),
        vec![
            Arc::new(Int64Array::from_iter_values(ids.clone())) as ArrayRef,
            Arc::new(Int64Array::from_iter_values(ids.map(|id| id % 320))),
            Arc::new(Float64Array::from_iter_values((0..ROWS).map(|row| {
                f64::from(u32::try_from(row).expect("row fits in u32")) / 4.0
            }))),
        ],
        None,
    );
    let item = Arc::new(Field::new("item", DataType::Utf8, false));
    let primary_keys = if keyed {
        ListArray::new(
            Arc::clone(&item),
            OffsetBuffer::from_lengths(std::iter::repeat_n(1, ROWS)),
            Arc::new(StringArray::from(vec!["id"; ROWS])),
            None,
        )
    } else {
        ListArray::new_null(item, ROWS)
    };
    ChangeBatch::try_new(
        RecordBatch::try_new(
            Arc::new(changes_schema(&schema)),
            vec![
                Arc::new(StringArray::from(vec!["r"; ROWS])),
                Arc::new(primary_keys),
                Arc::new(data),
            ],
        )
        .expect("CDC rows"),
    )
    .expect("valid CDC batch")
}

/// Initial-snapshot order lines with a four-column composite primary key.
fn order_line_batch() -> ChangeBatch {
    let key_names = ["ol_w_id", "ol_d_id", "ol_o_id", "ol_number"];
    let schema = Arc::new(Schema::new(vec![
        Field::new("ol_w_id", DataType::Int32, false),
        Field::new("ol_d_id", DataType::Int32, false),
        Field::new("ol_o_id", DataType::Int32, false),
        Field::new("ol_number", DataType::Int32, false),
        Field::new("ol_i_id", DataType::Int32, false),
        Field::new("ol_quantity", DataType::Int32, false),
        Field::new("ol_amount", DataType::Float64, false),
        Field::new("ol_dist_info", DataType::Utf8, false),
    ]));
    let rows = (0..ROWS)
        .map(|row| i32::try_from(row).expect("row fits in i32"))
        .collect::<Vec<_>>();
    let data = StructArray::new(
        schema.fields().clone(),
        vec![
            Arc::new(Int32Array::from(vec![1; ROWS])) as ArrayRef,
            Arc::new(Int32Array::from_iter_values(
                rows.iter().map(|row| row / 3000 + 1),
            )),
            Arc::new(Int32Array::from_iter_values(
                rows.iter().map(|row| row / 10 + 1),
            )),
            Arc::new(Int32Array::from_iter_values(
                rows.iter().map(|row| row % 10 + 1),
            )),
            Arc::new(Int32Array::from_iter_values(
                rows.iter().map(|row| row % 100_000 + 1),
            )),
            Arc::new(Int32Array::from(vec![5; ROWS])),
            Arc::new(Float64Array::from_iter_values(
                rows.iter().map(|&row| f64::from(row) / 4.0),
            )),
            Arc::new(StringArray::from(vec!["district information"; ROWS])),
        ],
        None,
    );
    let names = (0..ROWS).flat_map(|_| key_names).collect::<Vec<_>>();
    let primary_keys = ListArray::new(
        Arc::new(Field::new("item", DataType::Utf8, false)),
        OffsetBuffer::from_lengths(std::iter::repeat_n(key_names.len(), ROWS)),
        Arc::new(StringArray::from(names)),
        None,
    );
    ChangeBatch::try_new(
        RecordBatch::try_new(
            Arc::new(changes_schema(&schema)),
            vec![
                Arc::new(StringArray::from(vec!["c"; ROWS])),
                Arc::new(primary_keys),
                Arc::new(data),
            ],
        )
        .expect("order line CDC rows"),
    )
    .expect("valid order line CDC batch")
}

fn group_into_sub_batches_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("group_into_sub_batches");
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(1000);
    for (case, keyed) in [("keyed", true), ("keyless", false)] {
        let batch = batch(keyed);
        group.bench_function(case, |b| {
            b.iter(|| black_box(group_into_sub_batches(black_box(&batch))));
        });
    }
    let order_lines = order_line_batch();
    group.bench_function("order_line", |b| {
        b.iter(|| black_box(group_into_sub_batches(black_box(&order_lines))));
    });
    group.finish();
}

criterion_group!(benches, group_into_sub_batches_bench);
criterion_main!(benches);
