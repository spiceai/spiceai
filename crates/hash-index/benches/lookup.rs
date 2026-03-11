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

// Benchmark code has different lint requirements than production code
#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::cast_sign_loss,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::needless_pass_by_value,
    clippy::semicolon_if_nothing_returned,
    clippy::redundant_closure_for_method_calls,
    clippy::uninlined_format_args,
    clippy::explicit_iter_loop
)]

//! Comprehensive benchmarks for the SIMD hash index.
//!
//! Benchmark categories:
//! - Index build performance (various sizes and key types)
//! - Point lookup (single key, hit/miss)
//! - Batch lookup (multiple keys at once)
//! - Insert operations (sequential, random)
//! - Delete operations
//! - Mixed workloads (read/write/delete)
//! - Concurrent operations
//! - String keys of various lengths
//! - Composite keys

use arrow::array::{BinaryArray, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use hash_index::{HashIndex, HashIndexBuilder, RowLocation, hash_key, index_threshold};
use rand::Rng;
use std::hint::black_box;
use std::sync::Arc;

// =============================================================================
// Helper Functions
// =============================================================================

fn create_int64_batch(ids: Vec<i64>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let id_array = Int64Array::from(ids);
    RecordBatch::try_new(schema, vec![Arc::new(id_array)]).expect("failed to create batch")
}

fn create_string_batch(ids: Vec<String>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)]));
    let id_array = StringArray::from(ids);
    RecordBatch::try_new(schema, vec![Arc::new(id_array)]).expect("failed to create batch")
}

fn create_binary_batch(ids: Vec<Vec<u8>>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Binary, false)]));
    let id_array = BinaryArray::from(ids.iter().map(|v| v.as_slice()).collect::<Vec<_>>());
    RecordBatch::try_new(schema, vec![Arc::new(id_array)]).expect("failed to create batch")
}

fn create_composite_batch(ids: Vec<i64>, names: Vec<String>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let id_array = Int64Array::from(ids);
    let name_array = StringArray::from(names);
    RecordBatch::try_new(schema, vec![Arc::new(id_array), Arc::new(name_array)])
        .expect("failed to create batch")
}

// =============================================================================
// Index Build Benchmarks
// =============================================================================

fn bench_build_index(c: &mut Criterion) {
    let mut group = c.benchmark_group("index_build");

    // Int64 keys at various sizes
    for size in [1_000, 10_000, 100_000, 1_000_000] {
        let ids: Vec<i64> = (0..size).collect();
        let batch = create_int64_batch(ids);
        let partitions = vec![vec![batch]];

        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::new("int64", size), &partitions, |b, p| {
            b.iter(|| {
                HashIndexBuilder::new(vec!["id".to_string()])
                    .with_expected_rows(size as usize)
                    .build(black_box(p))
                    .expect("build failed")
            })
        });
    }

    // String key build (shorter strings)
    for size in [1_000, 10_000, 100_000] {
        let ids: Vec<String> = (0..size).map(|i| format!("key_{i:08}")).collect();
        let batch = create_string_batch(ids);
        let partitions = vec![vec![batch]];

        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(
            BenchmarkId::new("string_short", size),
            &partitions,
            |b, p| {
                b.iter(|| {
                    HashIndexBuilder::new(vec!["id".to_string()])
                        .with_expected_rows(size as usize)
                        .build(black_box(p))
                        .expect("build failed")
                })
            },
        );
    }

    // Long string keys
    for size in [1_000, 10_000] {
        let ids: Vec<String> = (0..size)
            .map(|i| format!("{:0>100}", i)) // 100 char strings
            .collect();
        let batch = create_string_batch(ids);
        let partitions = vec![vec![batch]];

        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(
            BenchmarkId::new("string_long", size),
            &partitions,
            |b, p| {
                b.iter(|| {
                    HashIndexBuilder::new(vec!["id".to_string()])
                        .with_expected_rows(size as usize)
                        .build(black_box(p))
                        .expect("build failed")
                })
            },
        );
    }

    // Binary key build
    for size in [1_000, 10_000] {
        let ids: Vec<Vec<u8>> = (0..size).map(|i: i64| i.to_le_bytes().to_vec()).collect();
        let batch = create_binary_batch(ids);
        let partitions = vec![vec![batch]];

        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::new("binary", size), &partitions, |b, p| {
            b.iter(|| {
                HashIndexBuilder::new(vec!["id".to_string()])
                    .with_expected_rows(size as usize)
                    .build(black_box(p))
                    .expect("build failed")
            })
        });
    }

    // Composite key build
    for size in [1_000, 10_000, 100_000] {
        let ids: Vec<i64> = (0..size).collect();
        let names: Vec<String> = (0..size).map(|i| format!("name_{i}")).collect();
        let batch = create_composite_batch(ids, names);
        let partitions = vec![vec![batch]];

        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::new("composite", size), &partitions, |b, p| {
            b.iter(|| {
                HashIndexBuilder::new(vec!["id".to_string(), "name".to_string()])
                    .with_expected_rows(size as usize)
                    .build(black_box(p))
                    .expect("build failed")
            })
        });
    }

    // Multi-batch build
    for num_batches in [10, 100] {
        let size_per_batch = 10_000;
        let total = num_batches * size_per_batch;
        let batches: Vec<RecordBatch> = (0..num_batches)
            .map(|batch_idx| {
                let start = batch_idx * size_per_batch;
                let ids: Vec<i64> = (start..(start + size_per_batch))
                    .map(|i| i as i64)
                    .collect();
                create_int64_batch(ids)
            })
            .collect();
        let partitions = vec![batches];

        group.throughput(Throughput::Elements(total as u64));
        group.bench_with_input(
            BenchmarkId::new("multi_batch", num_batches),
            &partitions,
            |b, p| {
                b.iter(|| {
                    HashIndexBuilder::new(vec!["id".to_string()])
                        .with_expected_rows(total)
                        .build(black_box(p))
                        .expect("build failed")
                })
            },
        );
    }

    group.finish();
}

// =============================================================================
// Point Lookup Benchmarks
// =============================================================================

fn bench_point_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("point_lookup");

    for size in [1_000, 10_000, 100_000, 1_000_000] {
        let ids: Vec<i64> = (0..size).collect();
        let batch = create_int64_batch(ids);
        let partitions = vec![vec![batch]];

        let index = HashIndexBuilder::new(vec!["id".to_string()])
            .with_expected_rows(size as usize)
            .build(&partitions)
            .expect("build failed");

        // Pre-compute hashes for lookup (hits)
        let mut rng = rand::rng();
        let num_lookups = 1000;
        let lookup_keys: Vec<i64> = (0..num_lookups)
            .map(|_| rng.random_range(0..size))
            .collect();
        let lookup_hashes: Vec<u64> = lookup_keys.iter().map(hash_key).collect();

        group.throughput(Throughput::Elements(num_lookups as u64));
        group.bench_with_input(
            BenchmarkId::new("int64_hit", size),
            &(index.clone(), lookup_hashes.clone()),
            |b, (idx, hashes): &(HashIndex, Vec<u64>)| {
                b.iter(|| {
                    for &hash in hashes {
                        black_box(idx.get_by_hash(hash));
                    }
                })
            },
        );

        // Miss lookups (keys not in index)
        let miss_hashes: Vec<u64> = (size..size + num_lookups).map(|k| hash_key(&k)).collect();
        group.bench_with_input(
            BenchmarkId::new("int64_miss", size),
            &(index, miss_hashes),
            |b, (idx, hashes): &(HashIndex, Vec<u64>)| {
                b.iter(|| {
                    for &hash in hashes {
                        black_box(idx.get_by_hash(hash));
                    }
                })
            },
        );
    }

    // String key lookups
    for size in [10_000, 100_000] {
        let ids: Vec<String> = (0..size).map(|i| format!("key_{i:08}")).collect();
        let batch = create_string_batch(ids.clone());
        let partitions = vec![vec![batch]];

        let index = HashIndexBuilder::new(vec!["id".to_string()])
            .with_expected_rows(size as usize)
            .build(&partitions)
            .expect("build failed");

        let mut rng = rand::rng();
        let num_lookups = 1000;
        let lookup_keys: Vec<String> = (0..num_lookups)
            .map(|_| format!("key_{:08}", rng.random_range(0..size)))
            .collect();
        let lookup_hashes: Vec<u64> = lookup_keys.iter().map(|k| hash_key(&k.as_str())).collect();

        group.throughput(Throughput::Elements(num_lookups as u64));
        group.bench_with_input(
            BenchmarkId::new("string_hit", size),
            &(index, lookup_hashes),
            |b, (idx, hashes): &(HashIndex, Vec<u64>)| {
                b.iter(|| {
                    for &hash in hashes {
                        black_box(idx.get_by_hash(hash));
                    }
                })
            },
        );
    }

    group.finish();
}

// =============================================================================
// Batch Lookup Benchmarks
// =============================================================================

fn bench_batch_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_lookup");

    for size in [10_000, 100_000, 1_000_000] {
        let ids: Vec<i64> = (0..size).collect();
        let batch = create_int64_batch(ids);
        let partitions = vec![vec![batch]];

        let index = HashIndexBuilder::new(vec!["id".to_string()])
            .with_expected_rows(size as usize)
            .build(&partitions)
            .expect("build failed");

        let mut rng = rand::rng();
        let batch_sizes = [10, 100, 1000];

        for batch_size in batch_sizes {
            let lookup_keys: Vec<i64> =
                (0..batch_size).map(|_| rng.random_range(0..size)).collect();
            let lookup_hashes: Vec<u64> = lookup_keys.iter().map(hash_key).collect();

            group.throughput(Throughput::Elements(batch_size as u64));
            group.bench_with_input(
                BenchmarkId::new(format!("size_{size}_batch_{batch_size}"), batch_size),
                &(index.clone(), lookup_hashes),
                |b, (idx, hashes): &(HashIndex, Vec<u64>)| {
                    b.iter(|| idx.get_batch(black_box(hashes)))
                },
            );
        }

        // Mixed hit/miss batch lookup
        let mixed_hashes: Vec<u64> = (0..1000_i64)
            .map(|i| {
                if i % 2 == 0 {
                    hash_key(&rng.random_range(0..size)) // hit
                } else {
                    hash_key(&(size + i)) // miss
                }
            })
            .collect();

        group.throughput(Throughput::Elements(1000));
        group.bench_with_input(
            BenchmarkId::new(format!("size_{size}_mixed"), 1000),
            &(index, mixed_hashes),
            |b, (idx, hashes): &(HashIndex, Vec<u64>)| b.iter(|| idx.get_batch(black_box(hashes))),
        );
    }

    group.finish();
}

// =============================================================================
// Insert Benchmarks
// =============================================================================

fn bench_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert");

    for size in [1_000, 10_000, 100_000] {
        // Sequential inserts
        let entries: Vec<(u64, RowLocation)> = (0..size)
            .map(|i| (hash_key(&i), RowLocation::simple(0, i as u32)))
            .collect();

        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::new("sequential", size), &entries, |b, e| {
            b.iter(|| {
                let index = HashIndex::new(vec!["id".to_string()]);
                for &(hash, loc) in e {
                    index.insert(hash, loc);
                }
                black_box(index)
            })
        });

        // Random order inserts
        let mut rng = rand::rng();
        let mut shuffled: Vec<(u64, RowLocation)> = entries.clone();
        for i in (1..shuffled.len()).rev() {
            let j = rng.random_range(0..=i);
            shuffled.swap(i, j);
        }

        group.bench_with_input(BenchmarkId::new("random", size), &shuffled, |b, e| {
            b.iter(|| {
                let index = HashIndex::new(vec!["id".to_string()]);
                for &(hash, loc) in e {
                    index.insert(hash, loc);
                }
                black_box(index)
            })
        });
    }

    // Insert with growth
    group.bench_function("insert_with_growth_10k", |b| {
        b.iter(|| {
            let index = HashIndex::new(vec!["id".to_string()]);
            for i in 0..10_000_i64 {
                let hash = hash_key(&i);
                let loc = RowLocation::simple(0, i as u32);
                index.insert(hash, loc);
            }
            black_box(index)
        })
    });

    group.finish();
}

// =============================================================================
// Delete Benchmarks
// =============================================================================

fn bench_delete(c: &mut Criterion) {
    let mut group = c.benchmark_group("delete");

    for size in [1_000, 10_000] {
        let entries: Vec<(u64, RowLocation)> = (0..size)
            .map(|i| (hash_key(&i), RowLocation::simple(0, i as u32)))
            .collect();

        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::new("sequential", size), &entries, |b, e| {
            b.iter(|| {
                let index = HashIndex::new(vec!["id".to_string()]);
                for &(hash, loc) in e {
                    index.insert(hash, loc);
                }
                // Delete all
                for &(hash, _) in e {
                    index.remove(hash);
                }
                black_box(index)
            })
        });

        // Delete half
        group.bench_with_input(BenchmarkId::new("delete_half", size), &entries, |b, e| {
            b.iter(|| {
                let index = HashIndex::new(vec!["id".to_string()]);
                for &(hash, loc) in e {
                    index.insert(hash, loc);
                }
                for (i, &(hash, _)) in e.iter().enumerate() {
                    if i % 2 == 0 {
                        index.remove(hash);
                    }
                }
                black_box(index)
            })
        });
    }

    group.finish();
}

// =============================================================================
// Mixed Workload Benchmarks
// =============================================================================

fn bench_mixed_workload(c: &mut Criterion) {
    let mut group = c.benchmark_group("mixed_workload");

    // 80% read, 20% write
    let size = 10_000_i64;
    let ids: Vec<i64> = (0..size).collect();
    let batch = create_int64_batch(ids);
    let partitions = vec![vec![batch]];

    let index = HashIndexBuilder::new(vec!["id".to_string()])
        .with_expected_rows(size as usize)
        .build(&partitions)
        .expect("build failed");

    let mut rng = rand::rng();
    let num_ops = 10_000;

    // Pre-generate operations: 80% reads, 20% writes
    let ops: Vec<(bool, i64)> = (0..num_ops)
        .map(|_| {
            let is_read = rng.random_ratio(8, 10);
            let key = if is_read {
                rng.random_range(0..size)
            } else {
                rng.random_range(size..size * 2) // new keys for writes
            };
            (is_read, key)
        })
        .collect();

    group.throughput(Throughput::Elements(num_ops as u64));
    group.bench_with_input(
        BenchmarkId::new("read_heavy", num_ops),
        &(index.clone(), ops),
        |b, (idx, operations): &(HashIndex, Vec<(bool, i64)>)| {
            b.iter(|| {
                for &(is_read, key) in operations {
                    if is_read {
                        black_box(idx.get(&key));
                    } else {
                        let hash = hash_key(&key);
                        idx.insert_or_replace(hash, RowLocation::simple(0, key as u32));
                    }
                }
            })
        },
    );

    // 50% read, 50% write
    let ops_balanced: Vec<(bool, i64)> = (0..num_ops)
        .map(|_| {
            let is_read = rng.random_ratio(1, 2);
            let key = if is_read {
                rng.random_range(0..size)
            } else {
                rng.random_range(size..size * 2)
            };
            (is_read, key)
        })
        .collect();

    group.bench_with_input(
        BenchmarkId::new("balanced", num_ops),
        &(index.clone(), ops_balanced),
        |b, (idx, operations): &(HashIndex, Vec<(bool, i64)>)| {
            b.iter(|| {
                for &(is_read, key) in operations {
                    if is_read {
                        black_box(idx.get(&key));
                    } else {
                        let hash = hash_key(&key);
                        idx.insert_or_replace(hash, RowLocation::simple(0, key as u32));
                    }
                }
            })
        },
    );

    // Read, write, delete mix (60% read, 20% write, 20% delete)
    let ops_with_delete: Vec<(u8, i64)> = (0..num_ops)
        .map(|_| {
            let op_type = rng.random_range(0..10);
            let op = if op_type < 6 {
                0
            } else if op_type < 8 {
                1
            } else {
                2
            }; // 0=read, 1=write, 2=delete
            let key = rng.random_range(0..size);
            (op, key)
        })
        .collect();

    group.bench_with_input(
        BenchmarkId::new("with_delete", num_ops),
        &(index, ops_with_delete),
        |b, (idx, operations): &(HashIndex, Vec<(u8, i64)>)| {
            b.iter(|| {
                for &(op, key) in operations {
                    match op {
                        0 => {
                            black_box(idx.get(&key));
                        }
                        1 => {
                            let hash = hash_key(&key);
                            idx.insert_or_replace(hash, RowLocation::simple(0, key as u32));
                        }
                        _ => {
                            let hash = hash_key(&key);
                            idx.remove(hash);
                        }
                    }
                }
            })
        },
    );

    group.finish();
}

// =============================================================================
// Concurrent Access Benchmarks
// =============================================================================

fn bench_concurrent_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_lookup");

    let size = 1_000_000_i64;
    let ids: Vec<i64> = (0..size).collect();
    let batch = create_int64_batch(ids);
    let partitions = vec![vec![batch]];

    let index = Arc::new(
        HashIndexBuilder::new(vec!["id".to_string()])
            .with_expected_rows(size as usize)
            .build(&partitions)
            .expect("build failed"),
    );

    let mut rng = rand::rng();
    let num_lookups = 10_000;
    let lookup_hashes: Vec<u64> = (0..num_lookups)
        .map(|_| hash_key(&rng.random_range(0..size)))
        .collect();

    group.throughput(Throughput::Elements(num_lookups as u64));
    group.bench_function("single_thread", |b| {
        b.iter(|| {
            for &hash in &lookup_hashes {
                black_box(index.get_by_hash(hash));
            }
        })
    });

    group.finish();
}

// =============================================================================
// Rebuild Benchmarks
// =============================================================================

fn bench_rebuild(c: &mut Criterion) {
    let mut group = c.benchmark_group("rebuild");

    for size in [10_000, 100_000] {
        // Create initial index
        let ids: Vec<i64> = (0..size).collect();
        let batch = create_int64_batch(ids);
        let partitions = vec![vec![batch]];

        let index = HashIndexBuilder::new(vec!["id".to_string()])
            .with_expected_rows(size as usize)
            .build(&partitions)
            .expect("build failed");

        // New data for rebuild
        let new_ids: Vec<i64> = (size..size * 2).collect();
        let new_batch = create_int64_batch(new_ids);
        let new_partitions = vec![vec![new_batch]];

        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(
            BenchmarkId::new("full_rebuild", size),
            &(index, new_partitions),
            |b, (idx, p): &(HashIndex, Vec<Vec<RecordBatch>>)| {
                b.iter(|| {
                    idx.rebuild(black_box(p)).expect("rebuild failed");
                })
            },
        );
    }

    group.finish();
}

// =============================================================================
// Clear Benchmarks
// =============================================================================

fn bench_clear(c: &mut Criterion) {
    let mut group = c.benchmark_group("clear");

    for size in [10_000, 100_000] {
        let ids: Vec<i64> = (0..size).collect();
        let batch = create_int64_batch(ids);
        let partitions = vec![vec![batch]];

        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::new("clear", size), &partitions, |b, p| {
            b.iter(|| {
                let index = HashIndexBuilder::new(vec!["id".to_string()])
                    .with_expected_rows(size as usize)
                    .build(p)
                    .expect("build failed");
                index.clear();
                black_box(index)
            })
        });
    }

    group.finish();
}

// =============================================================================
// Threshold Benchmarks - Index vs Linear Scan
// =============================================================================

/// Simulates a linear scan lookup (what happens when no index is used).
fn linear_scan_lookup(batches: &[RecordBatch], target_id: i64) -> Option<(usize, usize)> {
    for (batch_idx, batch) in batches.iter().enumerate() {
        let id_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("expected Int64Array");
        for row_idx in 0..id_array.len() {
            if id_array.value(row_idx) == target_id {
                return Some((batch_idx, row_idx));
            }
        }
    }
    None
}

fn bench_threshold_comparison(c: &mut Criterion) {
    let mut group = c.benchmark_group("threshold_comparison");

    // Simulate different parallelism levels (e.g., 8 cores)
    let parallelism = 8;
    let threshold = index_threshold(parallelism);

    // Test sizes: below threshold, at threshold, above threshold
    let sizes = [
        (threshold / 4, "quarter_threshold"),
        (threshold / 2, "half_threshold"),
        (threshold, "at_threshold"),
        (threshold * 2, "2x_threshold"),
        (threshold * 4, "4x_threshold"),
    ];

    for (size, label) in sizes {
        let ids: Vec<i64> = (0..size as i64).collect();
        let batch = create_int64_batch(ids);
        let batches = vec![batch.clone()];
        let partitions = vec![vec![batch]];

        // Random lookup targets (hit cases)
        let mut rng = rand::rng();
        let lookup_targets: Vec<i64> = (0..100).map(|_| rng.random_range(0..size as i64)).collect();

        // Benchmark indexed lookup
        let index = HashIndexBuilder::new(vec!["id".to_string()])
            .with_expected_rows(size)
            .build(&partitions)
            .expect("build failed");

        group.throughput(Throughput::Elements(100));
        group.bench_with_input(
            BenchmarkId::new("indexed_lookup", label),
            &(&index, &lookup_targets),
            |b, (idx, targets)| {
                b.iter(|| {
                    for &target in targets.iter() {
                        black_box(idx.get(&target));
                    }
                })
            },
        );

        // Benchmark linear scan lookup
        group.bench_with_input(
            BenchmarkId::new("linear_scan", label),
            &(&batches, &lookup_targets),
            |b, (data, targets)| {
                b.iter(|| {
                    for &target in targets.iter() {
                        black_box(linear_scan_lookup(data, target));
                    }
                })
            },
        );
    }

    group.finish();
}

fn bench_try_build_threshold(c: &mut Criterion) {
    let mut group = c.benchmark_group("try_build_threshold");

    // Test with parallelism of 8 (threshold = 2048)
    let parallelism = 8;
    let threshold = index_threshold(parallelism);

    // Below threshold - should return None
    {
        let size = threshold / 2;
        let ids: Vec<i64> = (0..size as i64).collect();
        let batch = create_int64_batch(ids);
        let partitions = vec![vec![batch]];

        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(
            BenchmarkId::new("below_threshold", size),
            &partitions,
            |b, p| {
                b.iter(|| {
                    let result = HashIndexBuilder::new(vec!["id".to_string()])
                        .with_expected_rows(size)
                        .with_min_rows_threshold(threshold)
                        .try_build(black_box(p))
                        .expect("try_build failed");
                    assert!(result.is_none(), "should skip index below threshold");
                    result
                })
            },
        );
    }

    // Above threshold - should build index
    {
        let size = threshold * 2;
        let ids: Vec<i64> = (0..size as i64).collect();
        let batch = create_int64_batch(ids);
        let partitions = vec![vec![batch]];

        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(
            BenchmarkId::new("above_threshold", size),
            &partitions,
            |b, p| {
                b.iter(|| {
                    let result = HashIndexBuilder::new(vec!["id".to_string()])
                        .with_expected_rows(size)
                        .with_min_rows_threshold(threshold)
                        .try_build(black_box(p))
                        .expect("try_build failed");
                    assert!(result.is_some(), "should build index above threshold");
                    result
                })
            },
        );
    }

    group.finish();
}

// =============================================================================
// Criterion Groups
// =============================================================================

criterion_group!(
    benches,
    bench_build_index,
    bench_point_lookup,
    bench_batch_lookup,
    bench_insert,
    bench_delete,
    bench_mixed_workload,
    bench_concurrent_lookup,
    bench_rebuild,
    bench_clear,
    bench_threshold_comparison,
    bench_try_build_threshold,
);

criterion_main!(benches);
