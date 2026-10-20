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

fn hash_deleted_rows(
    columns: &[ArrayRef],
    converter: &RowConverter,
    index: &KeyDeletionIndex,
    handling: InsertRecordHandling,
    cutoff: Option<i64>,
) -> Vec<usize> {
    let rows = converter.convert_columns(columns).expect("hash encoding");
    let mut deleted = Vec::new();
    index.get_batch_with_scratch(rows.iter(), &mut Vec::new(), &mut Vec::new(), |i, t| {
        if !tombstone_visible(t, handling, cutoff) {
            deleted.push(i);
        }
    });
    deleted
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
            assert_eq!(index.sorted_runs().count(), 3, "seed={seed}");
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
            let sorted = index.sorted_fused_entries();
            for key in &keys {
                assert_eq!(
                    sorted.get(key.as_ref()).copied(),
                    index.core_fused_entry(key.as_ref()),
                    "seed={seed}, sequence={seq}, key={:?}",
                    key.as_ref()
                );
            }
            for (key, entry) in &sorted {
                assert_eq!(
                    Some(*entry),
                    index.core_fused_entry(key),
                    "seed={seed}, sequence={seq}, unexpected sorted key={key:?}"
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
                    &mut SortedFilterScratch::new(),
                )
                .expect("native kernel");
                let hash = hash_deleted_rows(&batch_columns, &converter, &index, handling, cutoff);
                assert_eq!(
                    sorted, hash,
                    "seed={seed}, cutoff={cutoff:?}, handling={handling:?}"
                );
                assert_eq!(
                    metrics.permuted_batches.value(),
                    usize::from(case % 4 == 3),
                    "seed={seed}, permutation path"
                );
                assert_eq!(
                    metrics.permuted_rows.value(),
                    if case % 4 == 3 { positions.len() } else { 0 },
                    "seed={seed}, permuted rows"
                );
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
                    &mut SortedFilterScratch::new()
                )
                .expect("float kernel"),
                vec![0, 2, 4, 6],
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
    let mut cache = SortedFilterScratch::new();
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
            expected,
            "handling={handling:?}, cutoff={cutoff:?}"
        );
        assert_eq!(
            metrics.sorted_candidates.value(),
            4,
            "duplicate candidates are counted once per row"
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
        &mut SortedFilterScratch::new(),
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

#[test]
fn sorted_entries_fuse_across_several_runs() {
    let converter = RowConverter::new(vec![SortField::new(arrow_schema::DataType::Int64)])
        .expect("integer converter");
    let columns: Vec<ArrayRef> = vec![Arc::new(Int64Array::from_iter_values(0..16000))];
    let keys = converter.convert_columns(&columns).expect("encode keys");
    let mut index = KeyDeletionIndex::from_maps(
        (0..10000)
            .map(|i| (keys.row(i).as_ref().into(), 10))
            .collect(),
        [(keys.row(0).as_ref().into(), 100)].into(),
    );
    index = index.extend_max_conflicts(
        (10000..14520).map(|i| keys.row(i)).chain([keys.row(0)]),
        40,
        20,
    );
    index = index.extend_max_conflicts([keys.row(0)], 30, 90);
    assert_eq!(index.sorted_runs().count(), 3);
    let entries: Vec<_> = index
        .sorted_runs()
        .map(|r| {
            (0..r.len())
                .find(|&i| r.key_bytes(i) == keys.row(0).as_ref())
                .map(|i| r.entry(i))
                .expect("key is in each sorted run")
        })
        .collect();
    assert_eq!(
        entries
            .iter()
            .map(|e| (e.delete_sequence(), e.insert_sequence()))
            .collect::<Vec<_>>(),
        vec![
            (Some(10), Some(100)),
            (Some(40), Some(20)),
            (Some(30), Some(90))
        ]
    );
    assert_eq!(
        index
            .sorted_fused_entries()
            .get(keys.row(0).as_ref())
            .copied(),
        index.core_fused_entry(keys.row(0).as_ref())
    );
    let batch: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(vec![0; 32]))];
    for (handling, cutoff, expected) in [
        (InsertRecordHandling::Apply, None, vec![]),
        (InsertRecordHandling::Ignore, None, (0..32).collect()),
        (InsertRecordHandling::Ignore, Some(40), vec![]),
        (InsertRecordHandling::Apply, Some(15), vec![]),
    ] {
        let metrics = DeletionFilterMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
        let deleted = sorted_deleted_rows(
            &batch,
            &converter,
            &index,
            handling,
            cutoff,
            &metrics,
            &mut SortedFilterScratch::new(),
        )
        .expect("fused sorted kernel");
        assert_eq!(
            deleted, expected,
            "handling={handling:?}, cutoff={cutoff:?}"
        );
        assert_eq!(
            deleted,
            hash_deleted_rows(&batch, &converter, &index, handling, cutoff)
        );
        assert_eq!(metrics.sorted_candidates.value(), 32);
    }
}

fn typed_corpus() -> Vec<(ArrayRef, bool)> {
    use arrow::array::{Float32Array, Float64Array};
    use arrow_schema::{DataType, TimeUnit};
    let base: ArrayRef = Arc::new(Int64Array::from(
        (0..32)
            .map(|i| match i % 7 {
                0 => Some(3),
                1 => None,
                3 => Some(0),
                4 => Some(2),
                _ => Some(1),
            })
            .collect::<Vec<_>>(),
    ));
    let base32 = arrow::compute::cast(&base, &DataType::Int32).expect("Time32 native values");
    let mut arrays: Vec<_> = [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::UInt8,
        DataType::UInt16,
        DataType::UInt32,
        DataType::UInt64,
        DataType::Date32,
        DataType::Date64,
        DataType::Time32(TimeUnit::Second),
        DataType::Time32(TimeUnit::Millisecond),
        DataType::Time64(TimeUnit::Microsecond),
        DataType::Time64(TimeUnit::Nanosecond),
        DataType::Timestamp(TimeUnit::Second, None),
        DataType::Timestamp(TimeUnit::Millisecond, None),
        DataType::Timestamp(TimeUnit::Microsecond, None),
        DataType::Timestamp(TimeUnit::Nanosecond, None),
        DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
        DataType::Boolean,
    ]
    .iter()
    .map(|t| {
        (
            arrow::compute::cast(
                if matches!(t, DataType::Time32(_)) {
                    &base32
                } else {
                    &base
                },
                t,
            )
            .expect("typed corpus cast"),
            false,
        )
    })
    .collect();
    let strings: ArrayRef = Arc::new(StringArray::from(
        (0..32)
            .map(|i| match i % 7 {
                0 => Some("z"),
                1 => None,
                2 => Some(""),
                3 => Some("a\0b"),
                4 => Some("é"),
                _ => Some("a long key exceeding the inline string view buffer"),
            })
            .collect::<Vec<_>>(),
    ));
    for t in [
        DataType::Utf8,
        DataType::LargeUtf8,
        DataType::Utf8View,
        DataType::Binary,
        DataType::LargeBinary,
        DataType::BinaryView,
    ] {
        arrays.push((
            arrow::compute::cast(&strings, &t).expect("byte corpus cast"),
            false,
        ));
    }
    arrays.push((
        Arc::new(Float32Array::from(vec![
            None,
            Some(f32::from_bits(0xffc0_0001)),
            Some(-0.0),
            Some(0.0),
            Some(f32::INFINITY),
            Some(f32::NEG_INFINITY),
            Some(f32::from_bits(0x7fc0_0001)),
            Some(f32::from_bits(0x7fc0_0002)),
        ])),
        false,
    ));
    arrays.push((
        Arc::new(Float64Array::from(vec![
            None,
            Some(f64::from_bits(0xfff8_0000_0000_0001)),
            Some(-0.0),
            Some(0.0),
            Some(f64::INFINITY),
            Some(f64::NEG_INFINITY),
            Some(f64::from_bits(0x7ff8_0000_0000_0001)),
            Some(f64::from_bits(0x7ff8_0000_0000_0002)),
        ])),
        false,
    ));
    for t in [DataType::Decimal128(20, 3), DataType::Decimal256(40, 3)] {
        arrays.push((
            arrow::compute::cast(&base, &t).expect("decimal corpus cast"),
            true,
        ));
    }
    let slices: Vec<_> = arrays
        .iter()
        .map(|(a, fallback)| (a.slice(1, a.len() - 2), *fallback))
        .collect();
    arrays.extend(slices);
    arrays
}

#[test]
fn sorted_typed_comparators_match_row_encoding() {
    use sorted_comparator::{KeyCompare, TypedComparator};
    for (source_array, fallback) in typed_corpus() {
        for descending in [false, true] {
            for nulls_first in [false, true] {
                for nullable in [false, true] {
                    let array = if nullable {
                        Arc::clone(&source_array)
                    } else {
                        let valid = arrow::array::BooleanArray::from(
                            (0..source_array.len())
                                .map(|i| source_array.is_valid(i))
                                .collect::<Vec<_>>(),
                        );
                        arrow::compute::filter(source_array.as_ref(), &valid)
                            .expect("non-null corpus")
                    };
                    let options = arrow::compute::SortOptions {
                        descending,
                        nulls_first,
                    };
                    let converter = RowConverter::new(vec![SortField::new_with_options(
                        array.data_type().clone(),
                        options,
                    )])
                    .expect("typed converter");
                    let columns = vec![array];
                    let encoded = converter.convert_columns(&columns).expect("typed encoding");
                    let comparator =
                        TypedComparator::new(&columns, &columns, converter.sort_fields())
                            .expect("typed comparator");
                    assert_eq!(
                        comparator.fallback_columns(),
                        usize::from(fallback),
                        "type={:?}",
                        columns[0].data_type()
                    );
                    for a in 0..encoded.num_rows() {
                        for b in 0..encoded.num_rows() {
                            assert_eq!(
                                comparator.compare(a, b),
                                encoded.row(a).as_ref().cmp(encoded.row(b).as_ref()),
                                "type={:?}, options={options:?}, nullable={nullable}, rows={a},{b}",
                                columns[0].data_type()
                            );
                        }
                    }
                    let deletion_keys = converter
                        .convert_columns(&[Arc::clone(&source_array)])
                        .expect("typed deletion encoding");
                    let index = KeyDeletionIndex::from_map(
                        deletion_keys
                            .iter()
                            .enumerate()
                            .filter(|(i, _)| i % 3 == 0)
                            .map(|(_, k)| (k.as_ref().into(), 10))
                            .collect(),
                    );
                    let mut sorted: Vec<_> = (0..encoded.num_rows()).collect();
                    sorted.sort_by(|&a, &b| encoded.row(a).as_ref().cmp(encoded.row(b).as_ref()));
                    for order in [(0..encoded.num_rows()).collect::<Vec<_>>(), sorted] {
                        let take = UInt32Array::from(
                            order
                                .iter()
                                .map(|&i| u32::try_from(i).expect("small index"))
                                .collect::<Vec<_>>(),
                        );
                        let batch = vec![
                            arrow::compute::take(columns[0].as_ref(), &take, None)
                                .expect("permutation"),
                        ];
                        let metrics =
                            DeletionFilterMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
                        assert_eq!(
                            sorted_deleted_rows(
                                &batch,
                                &converter,
                                &index,
                                InsertRecordHandling::Ignore,
                                None,
                                &metrics,
                                &mut SortedFilterScratch::new()
                            )
                            .expect("typed sorted kernel"),
                            hash_deleted_rows(
                                &batch,
                                &converter,
                                &index,
                                InsertRecordHandling::Ignore,
                                None
                            ),
                            "type={:?}, options={options:?}, nullable={nullable}",
                            columns[0].data_type()
                        );
                    }
                }
            }
        }
    }
}

#[test]
fn sorted_integer_tuple_comparator_is_monomorphised() {
    use sorted_comparator::{IntegerComparator, KeyCompare};
    for descending in [false, true] {
        let fields = (0..4)
            .map(|i| {
                SortField::new_with_options(
                    if i % 2 == 0 {
                        arrow_schema::DataType::Int32
                    } else {
                        arrow_schema::DataType::Int64
                    },
                    arrow::compute::SortOptions {
                        descending,
                        nulls_first: false,
                    },
                )
            })
            .collect();
        let converter = RowConverter::new(fields).expect("mixed integer converter");
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(vec![1, 1, 0, 2, 1])),
            Arc::new(Int64Array::from(vec![0, 0, 9, 0, 0])),
            Arc::new(Int32Array::from(vec![1, 1, 0, 0, 1])),
            Arc::new(Int64Array::from(vec![3, 1, 9, 1, 3])),
        ];
        let comparator = IntegerComparator::<4>::new(&columns, &columns, converter.sort_fields())
            .expect("integer tuple");
        let encoded = converter
            .convert_columns(&columns)
            .expect("integer encoding");
        for a in 0..5 {
            for b in 0..5 {
                assert_eq!(
                    comparator.compare(a, b),
                    encoded.row(a).as_ref().cmp(encoded.row(b).as_ref()),
                    "descending={descending}, rows={a},{b}"
                );
            }
        }
        let index = KeyDeletionIndex::from_map([(encoded.row(0).as_ref().into(), 10)].into());
        let metrics = DeletionFilterMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
        assert_eq!(
            sorted_deleted_rows(
                &columns,
                &converter,
                &index,
                InsertRecordHandling::Ignore,
                None,
                &metrics,
                &mut SortedFilterScratch::new()
            )
            .expect("integer tuple kernel"),
            vec![0, 4]
        );
        assert_eq!(metrics.permuted_batches.value(), 1);
    }
}

#[tokio::test]
async fn sorted_scan_is_independent_of_hash_core() {
    let converter = Arc::new(
        RowConverter::new(vec![SortField::new(arrow_schema::DataType::Int64)])
            .expect("integer converter"),
    );
    let columns: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(vec![3, 1, 2, 1, 4]))];
    let keys = converter.convert_columns(&columns).expect("encode keys");
    let index = KeyDeletionIndex::from_map([(keys.row(1).as_ref().into(), 10)].into());
    let index = Arc::new(index.without_hash_core_for_test());
    let schema = Arc::new(Schema::new(vec![Field::new(
        "id",
        arrow_schema::DataType::Int64,
        false,
    )]));
    let batch = arrow::array::RecordBatch::try_new(Arc::clone(&schema), columns).expect("batch");
    let input = MemorySourceConfig::try_new_exec(&[vec![batch.clone()]], Arc::clone(&schema), None)
        .expect("input");
    let exec = KeyBasedDeletionFilterExec::new(
        input,
        index,
        InsertRecordHandling::Ignore,
        vec![0],
        converter,
        None,
    );
    let ctx = SessionContext::new();
    let native: Vec<_> = exec
        .execute_sorted_options(0, ctx.task_ctx(), true, false)
        .expect("native stream")
        .try_collect()
        .await
        .expect("native output");
    let oracle: Vec<_> = exec
        .execute_sorted_options(0, ctx.task_ctx(), false, false)
        .expect("hash stream")
        .try_collect()
        .await
        .expect("hash output");
    let expected = arrow::array::RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![3, 2, 4]))],
    )
    .expect("expected");
    assert_eq!(
        arrow::compute::concat_batches(&schema, &native).expect("concat native"),
        expected
    );
    assert_eq!(
        arrow::compute::concat_batches(&schema, &oracle).expect("concat hash"),
        batch
    );
}

#[test]
fn sorted_fixed_binary_comparator_matches_arrow() {
    use arrow::array::FixedSizeBinaryArray;
    // Fixed-size binary is outside RowConverter's supported PK encodings.
    // Compare the native comparator directly without changing persisted bytes.
    let array: ArrayRef = Arc::new(
        FixedSizeBinaryArray::try_from_sparse_iter_with_size(
            [
                Some(b"zzzz".as_slice()),
                None,
                Some(b"a\0bc".as_slice()),
                Some(b"aaaa".as_slice()),
            ]
            .into_iter(),
            4,
        )
        .expect("fixed binary corpus"),
    );
    for descending in [false, true] {
        for nulls_first in [false, true] {
            let options = arrow::compute::SortOptions {
                descending,
                nulls_first,
            };
            let fields = vec![SortField::new_with_options(
                array.data_type().clone(),
                options,
            )];
            let comparator =
                TypedComparator::new(&[Arc::clone(&array)], &[Arc::clone(&array)], &fields)
                    .expect("native fixed binary comparator");
            let oracle = arrow::array::make_comparator(array.as_ref(), array.as_ref(), options)
                .expect("Arrow fixed binary comparator");
            assert_eq!(comparator.fallback_columns(), 0);
            for a in 0..array.len() {
                for b in 0..array.len() {
                    assert_eq!(
                        comparator.compare(a, b),
                        oracle(a, b),
                        "options={options:?}, rows={a},{b}"
                    );
                }
            }
        }
    }
}

#[test]
fn sorted_vectorized_states_and_sparse_scratch_reuse() {
    for descending in [false, true] {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(vec![1; 96])),
            Arc::new(Int64Array::from(vec![2; 96])),
            Arc::new(Int32Array::from_iter_values((0..96).map(|i| (i * 31) % 17))),
        ];
        let fields: Vec<_> = columns
            .iter()
            .map(|c| {
                SortField::new_with_options(
                    c.data_type().clone(),
                    arrow::compute::SortOptions {
                        descending,
                        nulls_first: false,
                    },
                )
            })
            .collect();
        let converter = RowConverter::new(fields.clone()).expect("integer converter");
        let same =
            IntegerComparator::<3>::new(&columns, &columns, &fields).expect("integer comparator");
        let mut states = vec![0; 95];
        same.adjacent_states(&mut states);
        let typed = TypedComparator::new(&columns, &columns, &fields)
            .expect("typed first-column prepass comparator");
        let mut typed_states = vec![0; 95];
        typed.adjacent_states(&mut typed_states);
        assert_eq!(typed_states, states, "descending={descending}");
        for (i, &state) in states.iter().enumerate() {
            let cmp = same.compare(i, i + 1);
            assert_eq!(
                state,
                if cmp.is_lt() {
                    1
                } else if cmp.is_gt() {
                    2
                } else {
                    0
                },
                "descending={descending} row={i}"
            );
        }
        let rows = converter.convert_columns(&columns).expect("encode keys");
        let index = KeyDeletionIndex::from_map(
            rows.iter()
                .enumerate()
                .filter(|(i, _)| i % 3 == 0)
                .map(|(_, row)| (row.as_ref().into(), 7))
                .collect(),
        );
        let metrics = DeletionFilterMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
        let mut scratch = SortedFilterScratch::new();
        for length in [96, 17, 96, 1, 0, 48] {
            let batch: Vec<_> = columns.iter().map(|c| c.slice(0, length)).collect();
            let actual = sorted_deleted_rows(
                &batch,
                &converter,
                &index,
                InsertRecordHandling::Apply,
                None,
                &metrics,
                &mut scratch,
            )
            .expect("sorted kernel");
            let encoded = converter.convert_columns(&batch).expect("oracle keys");
            let expected: Vec<_> = encoded
                .iter()
                .enumerate()
                .filter_map(|(row, key)| {
                    index
                        .get_with_min_seq(key.as_ref(), None)
                        .filter(|t| !tombstone_visible(*t, InsertRecordHandling::Apply, None))
                        .map(|_| row)
                })
                .collect();
            assert_eq!(actual, expected, "descending={descending} length={length}");
        }
        let changed: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![2])),
            Arc::new(Int32Array::from(vec![3])),
        ];
        let error = sorted_deleted_rows(
            &changed,
            &converter,
            &index,
            InsertRecordHandling::Apply,
            None,
            &metrics,
            &mut scratch,
        )
        .expect_err("changed PK types must fail");
        assert!(error.to_string().contains("types changed"));
    }
}
