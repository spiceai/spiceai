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

use super::*;
use crate::row_converter::SortField;
use arrow::array::{BinaryArray, Int32Array, Int64Array, StringArray, UInt32Array};
use arrow_schema::{Field, Schema};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::execution::context::SessionContext;
use futures::TryStreamExt;

struct Random(u64);
impl Random {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn index(&mut self, n: usize) -> usize {
        usize::try_from(self.next() % u64::try_from(n).expect("small bound"))
            .expect("bounded index")
    }
}

// Utf8View batches with Utf8 converter fields are rejected by RowCodec schema
// validation, so the differential corpus uses matching physical types.
fn corpus(kind: usize, n: usize) -> Vec<ArrayRef> {
    let ints = |column: usize| {
        Arc::new(Int32Array::from(
            (0..n)
                .map(|i| {
                    if (i + column * 7).is_multiple_of(17) {
                        None
                    } else {
                        Some(
                            i32::try_from(if column == 0 {
                                i / 4
                            } else {
                                (i * (2 * column + 1)) % n
                            })
                            .expect("small corpus")
                                - 500,
                        )
                    }
                })
                .collect::<Vec<_>>(),
        )) as ArrayRef
    };
    let longs = || {
        Arc::new(Int64Array::from(
            (0..n)
                .map(|i| {
                    if i % 19 == 0 {
                        None
                    } else {
                        Some(i64::try_from(i).expect("small corpus") - 100)
                    }
                })
                .collect::<Vec<_>>(),
        )) as ArrayRef
    };
    let strings = || {
        Arc::new(StringArray::from(
            (0..n)
                .map(|i| (i % 13 != 0).then(|| format!("key-{i:06}")))
                .collect::<Vec<_>>(),
        )) as ArrayRef
    };
    match kind {
        0 => vec![longs()],
        1 => vec![ints(0), ints(1), ints(2), ints(3)],
        2 => vec![strings()],
        3 => {
            let values: Vec<_> = (0..n).map(usize::to_be_bytes).collect();
            vec![Arc::new(BinaryArray::from(
                values
                    .iter()
                    .enumerate()
                    .map(|(i, b)| (i % 11 != 0).then_some(b.as_slice()))
                    .collect::<Vec<_>>(),
            ))]
        }
        _ => vec![ints(0), strings(), longs()],
    }
}

#[tokio::test]
async fn sorted_filter_randomized_differential() {
    let initial = std::env::var("SORTED_FILTER_SEED")
        .ok()
        .map_or(0x91ab_5234_7789, |s| {
            s.parse::<u64>().expect("SORTED_FILTER_SEED must be u64")
        });
    for case in 0..300_u64 {
        let seed = initial.wrapping_add(case);
        let mut rng = Random(seed.max(1));
        let n = if case < 5 { 16000 } else { 192 };
        let columns = corpus(usize::try_from(case % 5).expect("small kind"), n);
        let converter = Arc::new(
            RowConverter::new(
                columns
                    .iter()
                    .map(|a| SortField::new(a.data_type().clone()))
                    .collect(),
            )
            .expect("converter"),
        );
        let keys = converter.convert_columns(&columns).expect("encode corpus");
        let key = |i: usize| keys.row(i).as_ref().to_vec();
        let mut index = KeyDeletionIndex::from_maps(
            (0..16).map(|i| (key(i).into_boxed_slice(), 1)).collect(),
            (16..32).map(|i| (key(i).into_boxed_slice(), 2)).collect(),
        );
        if case < 5 {
            for range in [0..10000, 10000..14520, 14520..15220] {
                index = index.extend_max_deletes(range.map(|i| (key(i), 1)));
            }
            assert_eq!(
                index.sorted_runs().expect("sorted runs").count(),
                3,
                "seed={seed}"
            );
        }
        for seq in 2..30_i64 {
            let additions: Vec<_> = (0..=rng.index(50)).map(|_| key(rng.index(n))).collect();
            match rng.index(5) {
                0 => index = index.extend_max_deletes(additions.into_iter().map(|k| (k, seq))),
                1 => index = index.extend_max_conflicts(additions, seq, seq + 1),
                2 => {
                    index = index.extend_max(
                        additions.iter().cloned().map(|k| (k, seq)),
                        additions.iter().cloned().map(|k| (k, seq, seq + 1)),
                    );
                }
                3 => index = index.prune_deletes_at_or_below(seq / 2),
                _ => {
                    let prefix = index.prune_frozen_prefix(seq / 2);
                    index = index
                        .with_pruned_prefix(&prefix)
                        .expect("unchanged prefix applies");
                }
            }
            let sorted_hashes: std::collections::HashSet<_> = index
                .sorted_runs()
                .expect("sorted set")
                .flat_map(|r| (0..r.len()).map(|i| r.hash(i)))
                .collect();
            for (hash, _) in index.iter_entries() {
                assert!(
                    sorted_hashes.contains(&hash),
                    "seed={seed}, sequence={seq}, missing hash={hash}"
                );
            }
        }
        let index = Arc::new(index);
        let mut positions: Vec<_> = (0..256).map(|_| rng.index(n)).collect();
        match case % 4 {
            0 => positions.sort_by(|&a, &b| keys.row(a).as_ref().cmp(keys.row(b).as_ref())),
            1 => {
                let run_count = 2 + rng.index(4);
                let chunk_len = positions.len().div_ceil(run_count);
                for run in positions.chunks_mut(chunk_len) {
                    run.sort_by(|&a, &b| keys.row(a).as_ref().cmp(keys.row(b).as_ref()));
                }
            }
            2 => positions.clear(),
            _ => {}
        }
        let take = UInt32Array::from(
            positions
                .iter()
                .map(|&i| u32::try_from(i).expect("small index"))
                .collect::<Vec<_>>(),
        );
        let batch_columns: Vec<_> = columns
            .iter()
            .map(|a| arrow::compute::take(a.as_ref(), &take, None).expect("take corpus"))
            .collect();
        let schema = Arc::new(Schema::new(
            batch_columns
                .iter()
                .enumerate()
                .map(|(i, a)| Field::new(format!("k{i}"), a.data_type().clone(), true))
                .collect::<Vec<_>>(),
        ));
        let batch = arrow::array::RecordBatch::try_new(Arc::clone(&schema), batch_columns.clone())
            .expect("batch");
        for handling in [InsertRecordHandling::Apply, InsertRecordHandling::Ignore] {
            for cutoff in [
                None,
                Some(i64::try_from(rng.index(30)).expect("small sequence")),
            ] {
                let metrics = DeletionFilterMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
                let sorted = sorted_deleted_rows(
                    &batch_columns,
                    &converter,
                    &index,
                    handling,
                    cutoff,
                    &metrics,
                    &mut SortedDecodeCache::new(),
                )
                .expect("native kernel");
                let encoded = converter
                    .convert_columns(&batch_columns)
                    .expect("hash encoding");
                let hash: Vec<_> = encoded
                    .iter()
                    .enumerate()
                    .filter_map(|(i, k)| {
                        index
                            .get(k.as_ref())
                            .filter(|t| !tombstone_visible(*t, handling, cutoff))
                            .map(|_| i)
                    })
                    .collect();
                if case % 4 <= 1 {
                    assert!(
                        sorted.is_some(),
                        "seed={seed}, sorted batch must use native search"
                    );
                } else if case % 4 == 3 {
                    assert!(
                        sorted.is_none(),
                        "seed={seed}, shuffled batch must fall back"
                    );
                }
                if let Some(sorted) = sorted {
                    assert_eq!(
                        sorted, hash,
                        "seed={seed}, cutoff={cutoff:?}, handling={handling:?}"
                    );
                }
                if case % 4 <= 1 {
                    assert_eq!(
                        metrics.sorted_runs.value() > 0,
                        !positions.is_empty(),
                        "seed={seed}"
                    );
                }
                let input = MemorySourceConfig::try_new_exec(
                    &[vec![batch.clone()]],
                    Arc::clone(&schema),
                    None,
                )
                .expect("input plan");
                let exec = KeyBasedDeletionFilterExec::new(
                    input,
                    Arc::clone(&index),
                    handling,
                    (0..columns.len()).collect(),
                    Arc::clone(&converter),
                    cutoff,
                );
                let ctx = SessionContext::new();
                let sorted_output: Vec<_> = exec
                    .execute_sorted_options(0, ctx.task_ctx(), true, false)
                    .expect("sorted stream")
                    .try_collect()
                    .await
                    .expect("sorted output");
                let hash_output: Vec<_> = exec
                    .execute_sorted_options(0, ctx.task_ctx(), false, false)
                    .expect("hash stream")
                    .try_collect()
                    .await
                    .expect("hash output");
                assert_eq!(
                    arrow::compute::concat_batches(&schema, &sorted_output).expect("concat sorted"),
                    arrow::compute::concat_batches(&schema, &hash_output).expect("concat hash"),
                    "seed={seed}, cutoff={cutoff:?}, handling={handling:?}"
                );
            }
        }
    }
}

#[test]
fn sorted_float_total_order_and_sort_options() {
    use arrow::array::Float64Array;
    for descending in [false, true] {
        for nulls_first in [false, true] {
            let converter = RowConverter::new(vec![SortField::new_with_options(
                arrow_schema::DataType::Float64,
                arrow::compute::SortOptions {
                    descending,
                    nulls_first,
                },
            )])
            .expect("float converter");
            let columns: Vec<ArrayRef> = vec![Arc::new(Float64Array::from(vec![
                None,
                Some(f64::NEG_INFINITY),
                Some(-0.0),
                Some(0.0),
                Some(f64::INFINITY),
                Some(f64::from_bits(0xfff8_0000_0000_0001)),
                Some(f64::from_bits(0x7ff8_0000_0000_0001)),
                Some(f64::from_bits(0x7ff8_0000_0000_0002)),
            ]))];
            let keys = converter.convert_columns(&columns).expect("float encoding");
            let mut order: Vec<_> = (0..keys.num_rows()).collect();
            order.sort_by(|&a, &b| keys.row(a).as_ref().cmp(keys.row(b).as_ref()));
            let take = UInt32Array::from(
                order
                    .iter()
                    .map(|&i| u32::try_from(i).expect("small index"))
                    .collect::<Vec<_>>(),
            );
            let columns =
                vec![arrow::compute::take(columns[0].as_ref(), &take, None).expect("sort floats")];
            let keys = converter
                .convert_columns(&columns)
                .expect("sorted float encoding");
            let tombstones = KeyDeletionIndex::from_map(
                keys.iter()
                    .enumerate()
                    .filter(|(i, _)| i % 2 == 0)
                    .map(|(_, k)| (k.as_ref().into(), 1))
                    .collect(),
            );
            let metrics = DeletionFilterMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
            assert_eq!(
                sorted_deleted_rows(
                    &columns,
                    &converter,
                    &tombstones,
                    InsertRecordHandling::Ignore,
                    None,
                    &metrics,
                    &mut SortedDecodeCache::new()
                )
                .expect("float kernel"),
                Some(vec![0, 2, 4, 6]),
                "descending={descending}, nulls_first={nulls_first}"
            );
        }
    }
}

#[test]
fn sorted_endpoints_duplicates_and_reinsert_markers() {
    let converter = RowConverter::new(vec![SortField::new(arrow_schema::DataType::Int64)])
        .expect("integer converter");
    let deletion_columns: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(vec![-100, 2, 3, 100]))];
    let keys = converter
        .convert_columns(&deletion_columns)
        .expect("deletion keys");
    let index = KeyDeletionIndex::from_map(
        [0, 1, 3]
            .into_iter()
            .map(|i| (keys.row(i).as_ref().into(), 1))
            .collect(),
    )
    .extend_max_conflicts([keys.row(1)], 5, 6)
    .extend_max_deletes([(keys.row(2), 10)]);
    let columns: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(vec![
        0, 1, 2, 2, 3, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13,
    ]))];
    let mut cache = SortedDecodeCache::new();
    for (handling, cutoff, expected) in [
        (InsertRecordHandling::Apply, None, vec![4, 5]),
        (InsertRecordHandling::Ignore, None, vec![2, 3, 4, 5]),
        (InsertRecordHandling::Ignore, Some(5), vec![4, 5]),
        (InsertRecordHandling::Ignore, Some(10), vec![]),
    ] {
        let metrics = DeletionFilterMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
        assert_eq!(
            sorted_deleted_rows(
                &columns, &converter, &index, handling, cutoff, &metrics, &mut cache
            )
            .expect("sorted endpoints"),
            Some(expected),
            "handling={handling:?}, cutoff={cutoff:?}"
        );
        assert_eq!(
            metrics.sorted_candidates.value(),
            4,
            "duplicate candidates are probed once per row"
        );
        assert_eq!(metrics.sorted_runs.value(), 1, "single sorted batch run");
    }
}

#[test]
fn sorted_decode_error_is_returned() {
    let converter = RowConverter::new(vec![SortField::new(arrow_schema::DataType::Int64)])
        .expect("integer converter");
    let index = KeyDeletionIndex::from_map([(Box::<[u8]>::default(), 1)].into());
    let columns: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(vec![1; 16]))];
    let metrics = DeletionFilterMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
    let error = sorted_deleted_rows(
        &columns,
        &converter,
        &index,
        InsertRecordHandling::Ignore,
        None,
        &metrics,
        &mut SortedDecodeCache::new(),
    )
    .expect_err("malformed encoded key must return an error");
    let datafusion_common::DataFusionError::ArrowError(source, _) = error else {
        panic!("expected structured Arrow decoding error: {error}");
    };
    assert_eq!(
        source.to_string(),
        "Invalid argument error: row_converter: truncated row, expected at least 9 bytes, got 0"
    );
}
