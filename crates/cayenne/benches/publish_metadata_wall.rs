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

//! Before-anchor for the write-side metadata publish wall.
//!
//! Each CDC checkpoint publish calls `commit_on_conflict_deletions`, which
//! writes one `cayenne_insert_record` row per upserted key through the single
//! `BEGIN IMMEDIATE` SQLite writer. At SF-100 this was ~98% of publish cost: a
//! 10-minute run accumulated 40.3M rows / 1.8-2.0 GB and writer-wait p99 hit
//! 796ms over 44882 commits. This bench measures that cost two ways so the
//! write-behind / group-commit lever has a real before-number:
//!
//! - `per_commit/<keys>` — cost of ONE publish of N keys against a FRESH
//!   (empty) catalog. The per-publish floor, isolated from b-tree growth.
//! - `grown_catalog/<preloaded>` — cost of one 10K-key publish against a
//!   catalog already holding `preloaded` insert-record rows. Isolates the
//!   b-tree-insert growth tax (the 1.8 GB slowdown).
//!
//! Timing uses `iter_custom`: the fresh-catalog setup and the sequence
//! reservation happen OUTSIDE the timed span; only `commit_on_conflict_deletions`
//! is measured. Every commit asserts `Ok` (a fast error is not a measurement).

#![allow(clippy::expect_used)]

use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_schema::{DataType, Field, Schema};
use cayenne::metadata::CreateTableOptions;
use cayenne::{CayenneCatalog, MetadataCatalog};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use datafusion_table_providers::util::{
    column_reference::ColumnReference, on_conflict::OnConflict,
};

const PER_COMMIT_KEYS: &[usize] = &[1_000, 10_000, 100_000];
const GROWN_PRELOAD: &[usize] = &[0, 100_000, 1_000_000];
const GROWN_COMMIT_KEYS: usize = 10_000;

/// Fresh on-disk sqlite catalog with one upsert table; returns (catalog, id,
/// tempdir-guard). The tempdir must outlive the catalog.
async fn fresh_table() -> (Arc<dyn MetadataCatalog>, String, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("temp dir");
    let db_path = dir.path().join("meta.db");
    let catalog = Arc::new(
        CayenneCatalog::new(&format!("sqlite://{}", db_path.display())).expect("create catalog"),
    ) as Arc<dyn MetadataCatalog>;
    catalog.init().await.expect("init catalog");
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));
    let table_id = catalog
        .create_table(CreateTableOptions {
            table_name: "wall".to_string(),
            schema,
            primary_key: vec!["id".to_string()],
            on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec!["id".to_string()]))),
            base_path: dir.path().join("data").to_string_lossy().to_string(),
            partition_column: None,
            vortex_config: cayenne::metadata::VortexConfig::default(),
        })
        .await
        .expect("create table");
    (catalog, table_id, dir)
}

fn pk_block(start: i64, count: usize) -> Vec<Vec<u8>> {
    (start..start + count as i64)
        .map(|k| k.to_be_bytes().to_vec())
        .collect()
}

/// Commit `keys` insert-records (no delete files) at a fresh snapshot sequence.
/// Returns the next unused key offset so callers can keep key ranges disjoint
/// (avoids the ON CONFLICT update path skewing the insert-cost measurement).
async fn commit_keys(catalog: &Arc<dyn MetadataCatalog>, table_id: &str, start: i64, keys: usize) {
    let seq = catalog
        .reserve_sequence_numbers(table_id, 2)
        .await
        .expect("reserve sequences");
    catalog
        .commit_on_conflict_deletions(
            Vec::new(),
            table_id,
            pk_block(start, keys),
            seq + 1,
            Some(cayenne::catalog::SnapshotSequenceCommit {
                snapshot_id: uuid::Uuid::now_v7().to_string(),
                sequence_number: seq + 1,
            }),
        )
        .await
        .expect("commit_on_conflict_deletions");
}

fn bench_publish_wall(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    let mut per_commit = c.benchmark_group("publish_metadata_wall/per_commit");
    per_commit.sample_size(10);
    for &keys in PER_COMMIT_KEYS {
        per_commit.throughput(Throughput::Elements(keys as u64));
        per_commit.bench_with_input(BenchmarkId::from_parameter(keys), &keys, |bencher, &keys| {
            bencher.to_async(&rt).iter_custom(|iters| async move {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    // Fresh catalog + table per iter: isolates the per-commit
                    // floor from b-tree growth. Setup is UNTIMED.
                    let (catalog, table_id, _dir) = fresh_table().await;
                    let seq = catalog
                        .reserve_sequence_numbers(&table_id, 2)
                        .await
                        .expect("reserve sequences");
                    let pk_bytes = pk_block(0, keys);
                    let snapshot_id = uuid::Uuid::now_v7().to_string();

                    let started = Instant::now();
                    catalog
                        .commit_on_conflict_deletions(
                            Vec::new(),
                            &table_id,
                            pk_bytes,
                            seq + 1,
                            Some(cayenne::catalog::SnapshotSequenceCommit {
                                snapshot_id,
                                sequence_number: seq + 1,
                            }),
                        )
                        .await
                        .expect("commit publish of N keys");
                    total += started.elapsed();
                    black_box(&catalog);
                }
                total
            });
        });
    }
    per_commit.finish();

    let mut grown = c.benchmark_group("publish_metadata_wall/grown_catalog");
    grown.sample_size(10);
    grown.throughput(Throughput::Elements(GROWN_COMMIT_KEYS as u64));
    for &preload in GROWN_PRELOAD {
        grown.bench_with_input(BenchmarkId::from_parameter(preload), &preload, |bencher, &preload| {
            bencher.to_async(&rt).iter_custom(|iters| async move {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    let (catalog, table_id, _dir) = fresh_table().await;
                    // Preload (UNTIMED) so the timed commit hits a large b-tree.
                    let mut start = 0_i64;
                    while (start as usize) < preload {
                        let chunk = 50_000.min(preload - start as usize);
                        commit_keys(&catalog, &table_id, start, chunk).await;
                        start += chunk as i64;
                    }
                    let seq = catalog
                        .reserve_sequence_numbers(&table_id, 2)
                        .await
                        .expect("reserve sequences");
                    let pk_bytes = pk_block(start, GROWN_COMMIT_KEYS);
                    let snapshot_id = uuid::Uuid::now_v7().to_string();

                    let started = Instant::now();
                    catalog
                        .commit_on_conflict_deletions(
                            Vec::new(),
                            &table_id,
                            pk_bytes,
                            seq + 1,
                            Some(cayenne::catalog::SnapshotSequenceCommit {
                                snapshot_id,
                                sequence_number: seq + 1,
                            }),
                        )
                        .await
                        .expect("commit publish into grown catalog");
                    total += started.elapsed();
                    black_box(&catalog);
                }
                total
            });
        });
    }
    grown.finish();
}

criterion_group!(benches, bench_publish_wall);
criterion_main!(benches);
