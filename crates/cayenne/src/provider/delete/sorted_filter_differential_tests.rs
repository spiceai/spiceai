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

thread_local! {
    static RANDOMIZED_KERNEL: std::cell::Cell<Option<&'static str>> = const {
        std::cell::Cell::new(None)
    };
}

pub(super) fn record_randomized_kernel<C>() {
    RANDOMIZED_KERNEL.set(Some(std::any::type_name::<C>()));
}

// Utf8View batches with Utf8 converter fields are rejected by RowCodec schema
// validation, so this corpus uses matching physical types.
// Each family varies one column at a time, so even large chosen domains contain
// long common prefixes. Symbols map injectively to every physical column type.
fn small_domain_corpus(
    kind: usize,
    nullable: bool,
    rng: &mut Random,
    n: usize,
) -> (Vec<ArrayRef>, Vec<Vec<Option<usize>>>) {
    let width = match kind {
        1 => 4,
        2 => 2,
        5 => 3,
        _ => 1,
    };
    let domains: Vec<_> = (0..width).map(|_| [2, 3, 4, 8, 64][rng.index(5)]).collect();
    let mut symbols = vec![vec![Some(0); width]; n];
    // A live key and a tombstone differ only in their last column. NULLs also
    // occupy that distinguishing position in nullable cases.
    symbols[0][width - 1] = if nullable { None } else { Some(0) };
    symbols[1][width - 1] = Some(1);
    symbols[2][0] = Some(1);
    for base in (3..n).step_by(8) {
        let prefix: Vec<_> = domains
            .iter()
            .map(|&d| {
                if nullable && rng.index(5) == 0 {
                    None
                } else {
                    Some(rng.index(d))
                }
            })
            .collect();
        let varying = rng.index(width);
        for row in symbols.iter_mut().take((base + 8).min(n)).skip(base) {
            row.clone_from(&prefix);
            row[varying] = if nullable && rng.index(5) == 0 {
                None
            } else {
                Some(rng.index(domains[varying]))
            };
        }
    }
    if nullable {
        for column in 0..width {
            symbols[3 + column] = vec![Some(0); width];
            symbols[3 + column][column] = None;
        }
    }
    let text = |v: usize| match v {
        0 => "a".to_owned(),
        1 => "aa".to_owned(),
        2 => "ab".to_owned(),
        _ => format!("aa{v:02}"),
    };
    let columns = (0..width)
        .map(|column| {
            let values: Vec<_> = symbols.iter().map(|row| row[column]).collect();
            match (kind, column) {
                (0, _) | (2, 1) | (5, 2) => Arc::new(Int64Array::from(
                    values
                        .iter()
                        .map(|v| v.map(|v| i64::try_from(v).expect("small domain") - 1))
                        .collect::<Vec<_>>(),
                )) as ArrayRef,
                (3, _) | (5, 1) => Arc::new(StringArray::from(
                    values.iter().map(|v| v.map(text)).collect::<Vec<_>>(),
                )) as ArrayRef,
                (4, _) => {
                    let bytes: Vec<_> = values
                        .iter()
                        .map(|v| v.map(|v| text(v).into_bytes()))
                        .collect();
                    Arc::new(BinaryArray::from(
                        bytes.iter().map(|v| v.as_deref()).collect::<Vec<_>>(),
                    )) as ArrayRef
                }
                _ => Arc::new(Int32Array::from(
                    values
                        .iter()
                        .map(|v| v.map(|v| i32::try_from(v).expect("small domain") - 1))
                        .collect::<Vec<_>>(),
                )) as ArrayRef,
            }
        })
        .collect();
    (columns, symbols)
}

#[tokio::test]
async fn sorted_filter_randomized_differential() {
    let initial = std::env::var("SORTED_FILTER_SEED")
        .ok()
        .map_or(0x91ab_5234_7789, |s| {
            s.parse::<u64>().expect("SORTED_FILTER_SEED must be u64")
        });
    let started = std::time::Instant::now();
    let mut deleting_cases = 0;
    let mut near_miss_cases = 0;
    let mut integer_cases = 0;
    let mut typed_cases = 0;
    for case in 0..300_u64 {
        let seed = initial.wrapping_add(case);
        let mut rng = Random(seed.max(1));
        for (kind, nullable) in [
            (usize::try_from(case % 3).expect("small shape"), false),
            (
                3 + usize::try_from(case % 3).expect("small shape"),
                case % 2 == 0,
            ),
        ] {
            let n = 96;
            let (columns, symbols) = small_domain_corpus(kind, nullable, &mut rng, n);
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
            let reserved: Vec<_> = (0..3).map(key).collect();
            let available: Vec<_> = (3..n).filter(|&i| !reserved.contains(&key(i))).collect();
            let mut index = KeyDeletionIndex::from_maps(
                available
                    .iter()
                    .take(8)
                    .map(|&i| (key(i).into_boxed_slice(), 1))
                    .collect(),
                available
                    .iter()
                    .skip(8)
                    .take(8)
                    .map(|&i| (key(i).into_boxed_slice(), 2))
                    .collect(),
            );
            for seq in 2..30_i64 {
                let additions: Vec<_> = if available.is_empty() {
                    Vec::new()
                } else {
                    (0..=rng.index(12))
                        .map(|_| key(available[rng.index(available.len())]))
                        .collect()
                };
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
            // Consolidate a delete into the old run, then place newer per-side
            // maxima in the tail. First-run-wins must disagree with the oracle.
            if columns.len() > 1 {
                index = index.extend_max_deletes([(key(2), 100)]);
            }
            index = index.prune_deletes_at_or_below(-1);
            if columns.len() > 1 {
                index = index.extend_max_conflicts([key(2)], 101, 102);
            }
            index = index.extend_max_deletes([(key(1), 103)]);
            if columns.len() > 1 {
                assert!(
                    index.sorted_runs().count() >= 2,
                    "seed={seed}, kind={kind}, missing cross-run fusion coverage"
                );
            }
            let index = Arc::new(index);
            let mut positions: Vec<_> = (0..96).map(|_| rng.index(n)).collect();
            positions.extend([0, 0, 1, 1, 1, 2, 2]);
            let near_misses = symbols
                .iter()
                .enumerate()
                .filter(|(i, tomb)| {
                    index.get_with_min_seq(&key(*i), None).is_some()
                        && positions.iter().any(|&row| {
                            tomb.iter()
                                .zip(&symbols[row])
                                .filter(|(a, b)| a != b)
                                .count()
                                == 1
                        })
                })
                .count();
            assert!(
                near_misses > 0,
                "seed={seed}, kind={kind}, missing near-miss tombstones"
            );
            near_miss_cases += usize::from(near_misses > 0);
            match case % 4 {
                0 | 2 => positions.sort_by(|&a, &b| keys.row(a).as_ref().cmp(keys.row(b).as_ref())),
                1 => {
                    let run_count = 2 + rng.index(4);
                    let chunk_len = positions.len().div_ceil(run_count);
                    for run in positions.chunks_mut(chunk_len) {
                        run.sort_by(|&a, &b| keys.row(a).as_ref().cmp(keys.row(b).as_ref()));
                    }
                }
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
            let batch =
                arrow::array::RecordBatch::try_new(Arc::clone(&schema), batch_columns.clone())
                    .expect("batch");
            let mut oracle_deletes = false;
            for handling in [InsertRecordHandling::Apply, InsertRecordHandling::Ignore] {
                for cutoff in [
                    None,
                    Some(i64::try_from(rng.index(30)).expect("small sequence")),
                ] {
                    let metrics = DeletionFilterMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
                    RANDOMIZED_KERNEL.set(None);
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
                    let kernel = RANDOMIZED_KERNEL.get().unwrap_or_default();
                    assert_eq!(
                        kernel.contains("IntegerComparator"),
                        kind <= 2,
                        "seed={seed}, kind={kind}, wrong kernel: {kernel}"
                    );
                    assert_eq!(
                        kernel.contains("TypedComparator"),
                        kind >= 3,
                        "seed={seed}, kind={kind}, wrong kernel: {kernel}"
                    );
                    let hash =
                        hash_deleted_rows(&batch_columns, &converter, &index, handling, cutoff);
                    if cutoff.is_none() {
                        for (row, &source) in positions.iter().enumerate() {
                            if source == 0 || source == 1 {
                                assert_eq!(
                                    hash.contains(&row),
                                    source == 1,
                                    "seed={seed}, kind={kind}, oracle near-miss/duplicate fixture row={row}"
                                );
                            }
                        }
                    }
                    oracle_deletes |= !hash.is_empty();
                    assert_eq!(
                        sorted, hash,
                        "seed={seed}, kind={kind}, nullable={nullable}, cutoff={cutoff:?}, handling={handling:?}"
                    );
                    let actual_runs = 1 + positions
                        .windows(2)
                        .filter(|pair| keys.row(pair[0]).as_ref() > keys.row(pair[1]).as_ref())
                        .count();
                    assert_eq!(
                        metrics.permuted_batches.value(),
                        usize::from(actual_runs > (positions.len() / 16).max(1)),
                        "seed={seed}, kind={kind}, permutation path"
                    );
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
                        arrow::compute::concat_batches(&schema, &sorted_output)
                            .expect("concat sorted"),
                        arrow::compute::concat_batches(&schema, &hash_output).expect("concat hash"),
                        "seed={seed}, kind={kind}, nullable={nullable}, cutoff={cutoff:?}, handling={handling:?}"
                    );
                }
            }
            assert!(
                oracle_deletes,
                "seed={seed}, kind={kind}, oracle never deleted a row"
            );
            deleting_cases += usize::from(oracle_deletes);
            integer_cases += usize::from(kind <= 2);
            typed_cases += usize::from(kind >= 3);
        }
    }
    assert_eq!(integer_cases, 300, "seed={initial}, integer coverage");
    assert_eq!(typed_cases, 300, "seed={initial}, typed coverage");
    assert!(
        deleting_cases >= 450,
        "seed={initial}, deleting cases={deleting_cases}/600"
    );
    assert!(
        near_miss_cases >= 450,
        "seed={initial}, near-miss cases={near_miss_cases}/600"
    );
    eprintln!(
        "seed={initial}, integer_cases={integer_cases}, typed_cases={typed_cases}, deleting_cases={deleting_cases}, near_miss_cases={near_miss_cases}, debug_runtime={:?}",
        started.elapsed()
    );
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

#[test]
fn sorted_galloping_wide_ranges_match_hash_oracle() {
    let converter = RowConverter::new(vec![SortField::new(arrow_schema::DataType::Int64)])
        .expect("wide-range converter");
    let columns: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(vec![
        -1, 0, 0, 31, 20_000, 20_000, 49_999, 50_000, 100_000,
    ]))];
    for (count, stride) in [(50_000, 1), (5, 10_001)] {
        let keys: Vec<ArrayRef> = vec![Arc::new(Int64Array::from_iter_values(
            (0..count).map(|i| i * stride),
        ))];
        let encoded = converter.convert_columns(&keys).expect("tombstone keys");
        let index = KeyDeletionIndex::from_map(
            encoded.iter().map(|key| (key.as_ref().into(), 7)).collect(),
        );
        let metrics = DeletionFilterMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
        let actual = sorted_deleted_rows(
            &columns,
            &converter,
            &index,
            InsertRecordHandling::Apply,
            None,
            &metrics,
            &mut SortedFilterScratch::new(),
        )
        .expect("galloping intersection");
        assert_eq!(
            actual,
            hash_deleted_rows(
                &columns,
                &converter,
                &index,
                InsertRecordHandling::Apply,
                None
            ),
            "count={count}, stride={stride}"
        );
        assert_eq!(metrics.sorted_runs.value(), 1);
        assert_eq!(metrics.permuted_batches.value(), 0);
    }
    // Dense batches against sparse tombstones exercise galloping the row side.
    let dense: Vec<ArrayRef> = vec![Arc::new(Int64Array::from_iter_values(0..8192))];
    let encoded = converter.convert_columns(&dense).expect("dense row keys");
    let index = KeyDeletionIndex::from_map(
        [0, 4095, 8191]
            .map(|i| (encoded.row(i).as_ref().into(), 9))
            .into(),
    );
    let metrics = DeletionFilterMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
    assert_eq!(
        sorted_deleted_rows(
            &dense,
            &converter,
            &index,
            InsertRecordHandling::Ignore,
            None,
            &metrics,
            &mut SortedFilterScratch::new()
        )
        .expect("sparse tombstone intersection"),
        hash_deleted_rows(
            &dense,
            &converter,
            &index,
            InsertRecordHandling::Ignore,
            None
        )
    );
}

#[test]
fn sorted_packed_integer_extremes_match_encoding() {
    let a: ArrayRef = Arc::new(Int32Array::from(vec![
        i32::MIN,
        -1,
        0,
        i32::MAX,
        -1,
        1,
        0,
        i32::MIN,
    ]));
    let b: ArrayRef = Arc::new(Int64Array::from(vec![
        i64::MAX,
        i64::MIN,
        0,
        -1,
        1,
        i64::MIN,
        i64::MAX,
        i64::MIN,
    ]));
    for columns in [
        vec![Arc::clone(&a)],
        vec![Arc::clone(&b)],
        vec![Arc::clone(&a), Arc::clone(&b)],
        vec![Arc::clone(&b), Arc::clone(&b)],
        vec![
            Arc::clone(&a),
            Arc::clone(&a),
            Arc::clone(&a),
            Arc::clone(&a),
        ],
    ] {
        for descending in [false, true] {
            let fields: Vec<_> = columns
                .iter()
                .enumerate()
                .map(|(i, c)| {
                    SortField::new_with_options(
                        c.data_type().clone(),
                        arrow::compute::SortOptions {
                            descending: descending ^ (i % 2 == 1),
                            nulls_first: true,
                        },
                    )
                })
                .collect();
            let converter = RowConverter::new(fields.clone()).expect("packed converter");
            let encoded = converter.convert_columns(&columns).expect("packed keys");
            let mut expected: Vec<_> = (0..8).collect();
            expected.sort_by_key(|&row| (encoded.row(row).as_ref().to_vec(), row));
            let mut pairs = Vec::new();
            let mut order = Vec::new();
            let packed = match columns.len() {
                1 => IntegerComparator::<1>::new(&columns, &columns, &fields)
                    .expect("one integer")
                    .packed_order(&mut pairs, &mut order),
                4 => IntegerComparator::<4>::new(&columns, &columns, &fields)
                    .expect("four integer tuple")
                    .packed_order(&mut pairs, &mut order),
                _ => IntegerComparator::<2>::new(&columns, &columns, &fields)
                    .expect("integer tuple")
                    .packed_order(&mut pairs, &mut order),
            };
            assert!(packed);
            assert_eq!(
                order, expected,
                "descending={descending}, fields={fields:?}"
            );
            let index = KeyDeletionIndex::from_map(
                [0, 2, 5]
                    .map(|i| (encoded.row(i).as_ref().into(), 9))
                    .into(),
            );
            let metrics = DeletionFilterMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
            assert_eq!(
                sorted_deleted_rows(
                    &columns,
                    &converter,
                    &index,
                    InsertRecordHandling::Apply,
                    None,
                    &metrics,
                    &mut SortedFilterScratch::new()
                )
                .expect("packed kernel"),
                hash_deleted_rows(
                    &columns,
                    &converter,
                    &index,
                    InsertRecordHandling::Apply,
                    None
                )
            );
            assert_eq!(metrics.permuted_batches.value(), 1);
        }
    }
}

#[test]
#[ignore = "local permutation comparison microbenchmark"]
#[expect(clippy::cast_precision_loss, reason = "diagnostic timing percentiles")]
fn sorted_permutation_microbench() {
    use arrow::compute::{SortColumn, lexsort_to_indices};
    use std::time::Instant;
    for shape in ["utf8_i64", "mixed", "binary_i64", "float_i64"] {
        let n = 8192;
        let ids: Vec<i64> = (0..n).map(|i| ((i * 7919) % n).into()).collect();
        let mut columns: Vec<ArrayRef> = match shape {
            "mixed" => vec![
                Arc::new(Int32Array::from(
                    ids.iter()
                        .map(|&i| i32::try_from(i % 17).expect("small prefix"))
                        .collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from_iter_values(
                    ids.iter().map(|i| format!("cust-{i:010}")),
                )),
            ],
            "binary_i64" => vec![Arc::new(BinaryArray::from_iter_values(
                ids.iter().map(|i| i.to_be_bytes()),
            ))],
            "float_i64" => vec![Arc::new(arrow::array::Float64Array::from_iter_values(
                ids.iter()
                    .map(|&i| f64::from(i32::try_from(i).expect("small id"))),
            ))],
            _ => vec![Arc::new(StringArray::from_iter_values(
                ids.iter().map(|i| format!("cust-{i:010}")),
            ))],
        };
        columns.push(Arc::new(Int64Array::from(ids)));
        let fields: Vec<_> = columns
            .iter()
            .map(|c| SortField::new(c.data_type().clone()))
            .collect();
        let comparator = TypedComparator::new(&columns, &columns, &fields).expect("comparator");
        let sort: Vec<_> = columns
            .iter()
            .map(|c| SortColumn {
                values: Arc::clone(c),
                options: Some(arrow::compute::SortOptions::default()),
            })
            .collect();
        let mut samples = [Vec::new(), Vec::new()];
        for rep in 0..201 {
            let mut results = [Vec::new(), Vec::new()];
            for arm in if rep % 2 == 0 { [0, 1] } else { [1, 0] } {
                let start = Instant::now();
                results[arm] = if arm == 0 {
                    let mut rows: Vec<_> = (0..8192).collect();
                    rows.sort_unstable_by(|&a, &b| comparator.compare(a, b));
                    rows
                } else {
                    lexsort_to_indices(&sort, None)
                        .expect("Arrow permutation")
                        .values()
                        .iter()
                        .map(|&i| i as usize)
                        .collect()
                };
                let elapsed = start.elapsed().as_nanos();
                if rep > 0 {
                    samples[arm].push(elapsed);
                }
            }
            assert_eq!(results[0], results[1], "shape={shape}, rep={rep}");
        }
        for (arm, mut times) in samples.into_iter().enumerate() {
            times.sort_unstable();
            println!(
                "PERMUTE shape={shape} arm={} n={} p99_us={:.3} max_us={:.3}",
                if arm == 0 { "typed" } else { "arrow" },
                times.len(),
                times[times.len() * 99 / 100] as f64 / 1000.0,
                times[times.len() - 1] as f64 / 1000.0
            );
        }
    }
}
