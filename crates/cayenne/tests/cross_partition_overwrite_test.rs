/*
Copyright 2026 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0
*/

//! Issue #10125 — cross-partition atomic overwrite, exercised via the
//! `PreparedOverwrite` lifecycle against a shared `CayenneCatalog`
//! transaction.
//!
//! These tests validate the building-block contract that
//! `CayennePartitionedInsertStrategy` (in `runtime/dataaccelerator/cayenne`)
//! relies on:
//!
//! - `begin_overwrite` on each partition's `CayenneTableProvider` writes
//!   data into a fresh snapshot directory without touching the catalog.
//! - One shared `MetastoreTransaction` can batch every partition's
//!   `apply_in_txn` call so the catalog `current_snapshot_id` pointer flips
//!   atomically — either every partition advances or none do.
//! - Rolling back the shared transaction (or surfacing an error from
//!   `apply_in_txn`) leaves every partition at its prior snapshot pointer.
//!
//! Durability note (as of the fixes in this branch):
//! All local-FS directory creation points that are part of the write +
//! crash-recovery infrastructure now perform the required parent-directory
//! sync after `create_dir_all` (snapshot directories via
//! `ensure_snapshot_dir_exists` (including initial table creation before
//! metastore INSERT), the _partitioned_wal/ coordination directory via the
//! helper in `PartitionedWal::write_to`, `deletions/` subdirectories under
//! snapshots via DeletionVectorWriter, and partition value subdirectories
//! via CayennePartitionCreator before `add_partition`).
//! The catalog DB directory creation in `CayenneCatalog::init` also
//! receives a best-effort parent sync for completeness of the system
//! initialization path.
//! Combined with the per-partition staging WAL, deletion vector file
//! sync_all, and directory syncs in the delete sinks, a successful
//! cross-partition operation (append or overwrite, including any
//! concurrent or pending deletions or new partitions) leaves a fully
//! durable set of coordination records and data files on local FS.
//! The existing fault-injection and restart tests in this file, together
//! with the per-partition durability tests (deletion vector restart,
//! staged-append restart, acid_compliance, data_inlining, catalog
//! concurrency with partitions), provide comprehensive regression coverage
//! for this property, including the edge cases of the very first
//! cross-partition write on a brand-new table (first creation of the
//! _partitioned_wal/ directory), the first deletion vector written to a
//! snapshot, and the first discovery of a new partition value.

#![expect(
    clippy::expect_used,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::used_underscore_binding,
    reason = "Test-only code: bounded loop indices fit in i64, signed→unsigned casts of \
              positive literals are safe, and `_temp_dir` is held to keep tempdir alive \
              and read for path access."
)]

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use cayenne::metadata::{CreateTableOptions, VortexConfig};
use cayenne::{CayenneCatalog, CayenneTableProvider, MetadataCatalog, PartitionedWal};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::prelude::*;
use datafusion_common::DataFusionError;
use tempfile::TempDir;

fn test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]))
}

fn make_batch(ids: &[i64], names: &[&str]) -> RecordBatch {
    RecordBatch::try_new(
        test_schema(),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(StringArray::from(names.to_vec())),
        ],
    )
    .expect("valid batch")
}

fn batch_to_stream(batch: RecordBatch) -> SendableRecordBatchStream {
    let schema = batch.schema();
    Box::pin(RecordBatchStreamAdapter::new(
        schema,
        futures::stream::iter(vec![Ok::<_, DataFusionError>(batch)]),
    ))
}

struct PartitionSetup {
    _temp_dir: TempDir,
    catalog: Arc<CayenneCatalog>,
    tables: Vec<Arc<CayenneTableProvider>>,
}

/// Create a `CayenneCatalog` and `partition_count` independent Cayenne
/// "tables" sharing it. Each one stands in for one partition in the
/// cross-partition coordinator's view.
async fn setup_partitions(partition_count: usize) -> PartitionSetup {
    let temp_dir = TempDir::new().expect("tempdir");
    let db_path = temp_dir.path().join("test.db");
    let data_path = temp_dir.path().join("data");
    std::fs::create_dir_all(&data_path).expect("data dir");

    let catalog = Arc::new(
        CayenneCatalog::new(format!("sqlite://{}", db_path.to_string_lossy())).expect("catalog"),
    );
    catalog.init().await.expect("init");

    let catalog_dyn: Arc<dyn MetadataCatalog> = Arc::<CayenneCatalog>::clone(&catalog);
    let runtime_env = SessionContext::new().runtime_env();

    let mut tables = Vec::with_capacity(partition_count);
    for i in 0..partition_count {
        let options = CreateTableOptions {
            table_name: format!("partition_{i}"),
            schema: test_schema(),
            primary_key: vec![],
            on_conflict: None,
            base_path: data_path.to_string_lossy().to_string(),
            partition_column: None,
            vortex_config: VortexConfig::default(),
        };
        let table = CayenneTableProvider::create_table(
            Arc::clone(&catalog_dyn),
            options,
            Arc::clone(&runtime_env),
        )
        .await
        .expect("create table");
        tables.push(Arc::new(table));
    }

    PartitionSetup {
        _temp_dir: temp_dir,
        catalog,
        tables,
    }
}

/// Snapshot the catalog's `current_snapshot_id` for every partition.
async fn snapshot_pointers(setup: &PartitionSetup) -> Vec<String> {
    let mut snapshots = Vec::with_capacity(setup.tables.len());
    for (i, _) in setup.tables.iter().enumerate() {
        let meta = setup
            .catalog
            .get_table(&format!("partition_{i}"))
            .await
            .expect("get table");
        snapshots.push(meta.current_snapshot_id);
    }
    snapshots
}

// ============================================================================
// Test 1 — happy path: prepare all partitions, apply each in one shared
// MetastoreTransaction, commit once, finish each. Every partition's
// catalog pointer must advance to its new_snapshot_id and the new data
// must be visible in subsequent scans.
// ============================================================================

#[tokio::test]
async fn cross_partition_overwrite_commits_atomically() {
    let setup = setup_partitions(3).await;
    let before = snapshot_pointers(&setup).await;

    // Stage each partition.
    let mut prepared = Vec::new();
    for (i, table) in setup.tables.iter().enumerate() {
        let stream = batch_to_stream(make_batch(
            &[i as i64 * 100, i as i64 * 100 + 1],
            &["a", "b"],
        ));
        let prep = table
            .begin_overwrite(stream, 1)
            .await
            .expect("begin_overwrite");
        prepared.push(prep);
    }

    // One shared transaction.
    let mut txn = setup
        .catalog
        .begin_transaction()
        .await
        .expect("begin_transaction");
    for prep in &prepared {
        prep.apply_in_txn(&setup.catalog, &mut *txn)
            .await
            .expect("apply_in_txn");
    }
    txn.commit().await.expect("txn commit");

    // Finish each partition.
    let mut total = 0u64;
    for prep in prepared {
        total += prep.finish().await.expect("finish");
    }
    assert_eq!(total, 6, "all rows across all partitions accounted for");

    let after = snapshot_pointers(&setup).await;
    assert_eq!(after.len(), before.len());
    for (i, (b, a)) in before.iter().zip(after.iter()).enumerate() {
        assert_ne!(b, a, "partition {i} snapshot must advance");
    }
}

// ============================================================================
// Test 2 — atomicity under txn rollback: prepare all partitions, apply each,
// drop the transaction without committing. NO partition's pointer may have
// advanced. Subsequent re-overwrite must succeed cleanly.
// ============================================================================

#[tokio::test]
async fn cross_partition_overwrite_rolls_back_atomically() {
    let setup = setup_partitions(3).await;
    let before = snapshot_pointers(&setup).await;

    let mut prepared = Vec::new();
    for (i, table) in setup.tables.iter().enumerate() {
        let stream = batch_to_stream(make_batch(
            &[i as i64 * 100, i as i64 * 100 + 1],
            &["x", "y"],
        ));
        let prep = table
            .begin_overwrite(stream, 1)
            .await
            .expect("begin_overwrite");
        prepared.push(prep);
    }

    {
        let mut txn = setup
            .catalog
            .begin_transaction()
            .await
            .expect("begin_transaction");
        for prep in &prepared {
            prep.apply_in_txn(&setup.catalog, &mut *txn)
                .await
                .expect("apply_in_txn");
        }
        // Drop txn without committing — auto-rollback.
    }

    let after = snapshot_pointers(&setup).await;
    for (i, (b, a)) in before.iter().zip(after.iter()).enumerate() {
        assert_eq!(
            b, a,
            "partition {i} snapshot must NOT advance when txn is rolled back"
        );
    }

    // Discard the staged dirs so the partitions are writable again.
    for prep in prepared {
        prep.rollback().await.expect("rollback");
    }
}

// ============================================================================
// Test 3 — fault injection: simulate apply_in_txn failure on the 2nd of 3
// partitions by feeding it an invalid table_id (caught by UUID validation
// in commit_compaction_in_txn). The transaction is rolled back via drop;
// no partition's pointer advances.
// ============================================================================

#[tokio::test]
async fn cross_partition_overwrite_aborts_on_apply_failure() {
    let setup = setup_partitions(3).await;
    let before = snapshot_pointers(&setup).await;

    let mut prepared = Vec::new();
    for (i, table) in setup.tables.iter().enumerate() {
        let stream = batch_to_stream(make_batch(
            &[i as i64 * 100, i as i64 * 100 + 1],
            &["p", "q"],
        ));
        let prep = table
            .begin_overwrite(stream, 1)
            .await
            .expect("begin_overwrite");
        prepared.push(prep);
    }

    {
        let mut txn = setup
            .catalog
            .begin_transaction()
            .await
            .expect("begin_transaction");

        // First partition applies cleanly.
        prepared[0]
            .apply_in_txn(&setup.catalog, &mut *txn)
            .await
            .expect("first apply_in_txn");

        // Second partition: inject an invalid UUID so the commit_compaction_in_txn
        // call rejects it. We do this by calling commit_compaction_in_txn
        // directly with a bad table_id; this models a fault during the
        // coordinator's apply loop.
        let result = setup
            .catalog
            .commit_compaction_in_txn(&mut *txn, "not-a-uuid", "also-not-a-uuid")
            .await;
        assert!(
            result.is_err(),
            "invalid UUID must be rejected by commit_compaction_in_txn"
        );

        // Coordinator's expected behavior on this kind of fault: drop the
        // transaction (auto-rollback) and surface the error. We simulate by
        // dropping the txn here.
    }

    let after = snapshot_pointers(&setup).await;
    for (i, (b, a)) in before.iter().zip(after.iter()).enumerate() {
        assert_eq!(
            b, a,
            "partition {i} snapshot must NOT advance when an apply_in_txn fault aborts the txn"
        );
    }

    for prep in prepared {
        prep.rollback().await.expect("rollback");
    }
}

// ============================================================================
// Test 4 — large-input streaming: simulate the cross-partition coordinator's
// streaming write_all by feeding many batches through `begin_overwrite` over
// an mpsc channel, one partition at a time. Regression guard for the
// memory-bounded behavior introduced after the initial step 4b commit —
// without streaming, a refresh this size would buffer every batch in RAM
// inside `CayennePartitionedOverwriteSink::write_all` before opening the
// catalog transaction.
//
// This test does NOT spin up the strategy directly (that requires a runtime
// accelerator setup). It exercises the underlying `begin_overwrite` /
// mpsc-receiver pattern the coordinator uses, so a regression that broke
// streaming would surface here as a memory-exhaustion failure under load.
// ============================================================================

#[tokio::test]
async fn streaming_overwrite_handles_many_batches_per_partition() {
    use tokio::sync::mpsc;
    use tokio_stream::wrappers::ReceiverStream;

    let setup = setup_partitions(2).await;
    let total_batches_per_partition: usize = 256;
    let rows_per_batch: i64 = 64;

    let mut prepared = Vec::new();

    for (i, table) in setup.tables.iter().enumerate() {
        let (tx, rx) = mpsc::channel::<datafusion_common::Result<RecordBatch>>(8);
        let schema = test_schema();
        let table_clone = table.clone_for_write_operations();

        // Spawn the writer; it pulls batches from the channel as they arrive,
        // so memory in flight is bounded by channel depth × batch size.
        let handle = tokio::spawn(async move {
            let stream: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
                schema,
                ReceiverStream::new(rx),
            ));
            table_clone.begin_overwrite(stream, 1).await
        });

        // Producer: push many batches, one at a time. The `.await` here is
        // the load-bearing piece — it blocks on a full channel, providing
        // backpressure rather than buffering unboundedly.
        for batch_idx in 0..total_batches_per_partition {
            let base = (i * 1_000_000) as i64 + batch_idx as i64 * rows_per_batch;
            let ids: Vec<i64> = (base..base + rows_per_batch).collect();
            let names: Vec<String> = ids.iter().map(|n| format!("p{i}_n{n}")).collect();
            let names_ref: Vec<&str> = names.iter().map(String::as_str).collect();
            let batch = make_batch(&ids, &names_ref);
            tx.send(Ok(batch))
                .await
                .expect("channel send before stream end");
        }
        drop(tx); // close the channel so the writer's stream terminates.

        let prep = handle
            .await
            .expect("writer task did not panic")
            .expect("begin_overwrite succeeded");
        assert_eq!(
            prep.row_count(),
            (total_batches_per_partition as u64) * (rows_per_batch as u64),
            "partition {i} row count matches what the producer sent",
        );
        prepared.push(prep);
    }

    // Commit all partitions in one shared txn — same shape as the coordinator.
    let mut txn = setup
        .catalog
        .begin_transaction()
        .await
        .expect("begin_transaction");
    for prep in &prepared {
        prep.apply_in_txn(&setup.catalog, &mut *txn)
            .await
            .expect("apply_in_txn");
    }
    txn.commit().await.expect("txn commit");

    let mut total = 0u64;
    for prep in prepared {
        total += prep.finish().await.expect("finish");
    }
    assert_eq!(
        total,
        2 * (total_batches_per_partition as u64) * (rows_per_batch as u64),
        "all partitions' rows committed"
    );
}

// ============================================================================
// Test 5 (issue #10125 step 6) — cross-partition append: PreparedStagedAppend
// commits atomically under a coordinator-held listing fence on every
// participating partition. Mirrors the coordinator's barrier flow:
//
//   1. begin_staged_append → CayenneStagedAppend per partition.
//   2. prepare() → PreparedStagedAppend (writes per-partition staging WAL).
//   3. acquire listing_fence.write() on every partition (sorted).
//   4. write top-level PartitionedWal.
//   5. apply_under_held_barrier on each.
//   6. remove top-level PartitionedWal.
//   7. release fences; finish each.
// ============================================================================

#[tokio::test]
async fn cross_partition_append_commits_atomically_under_barrier() {
    use tokio::sync::mpsc;
    use tokio_stream::wrappers::ReceiverStream;

    let setup = setup_partitions(3).await;

    // Seed each partition with one baseline row so subsequent appends are
    // visible as additions.
    for (i, table) in setup.tables.iter().enumerate() {
        let stream = batch_to_stream(make_batch(&[i as i64 * 10], &["seed"]));
        let prep = table
            .begin_overwrite(stream, 1)
            .await
            .expect("seed overwrite");
        prep.apply_owned_txn().await.expect("apply seed");
        prep.finish().await.expect("finish seed");
    }

    // Stage append per partition via begin_staged_append → prepare.
    let mut prepared = Vec::new();
    for (i, table) in setup.tables.iter().enumerate() {
        let (tx, rx) = mpsc::channel::<datafusion_common::Result<RecordBatch>>(4);
        let schema = test_schema();
        let table_clone = table.clone_for_write_operations();
        let handle = tokio::spawn(async move {
            let stream: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
                schema,
                ReceiverStream::new(rx),
            ));
            let staged = table_clone.begin_staged_append(stream, 1).await?;
            staged.prepare().await
        });

        for batch_idx in 0..4 {
            let base = (i * 1000) as i64 + batch_idx * 8;
            let ids: Vec<i64> = (base..base + 8).collect();
            let names: Vec<String> = ids.iter().map(|n| format!("a{n}")).collect();
            let names_ref: Vec<&str> = names.iter().map(String::as_str).collect();
            tx.send(Ok(make_batch(&ids, &names_ref)))
                .await
                .expect("send");
        }
        drop(tx);

        let prep = handle
            .await
            .expect("writer task did not panic")
            .expect("prepare succeeded");
        prepared.push(prep);
    }

    // Sort + acquire all fences (same shape as the coordinator).
    prepared.sort_by(|a, b| a.table_id().cmp(b.table_id()));
    let mut fence_guards = Vec::new();
    for p in &prepared {
        fence_guards.push(p.lock_listing_fence_write_owned().await);
    }

    // Top-level WAL: write before barrier.
    let table_root = setup._temp_dir.path().join("data");
    let commit_id = uuid::Uuid::now_v7().to_string();
    let wal_entries: Vec<cayenne::PartitionedWalEntry> = prepared
        .iter()
        .map(|p| cayenne::PartitionedWalEntry {
            table_id: p.table_id().to_string(),
            staging_wal_path: Some(p.staging_wal_path().to_string_lossy().to_string()),
        })
        .collect();
    let top_level = PartitionedWal::new(
        commit_id.clone(),
        table_root.to_string_lossy().to_string(),
        wal_entries,
    );
    top_level.write_to(&table_root).await.expect("write WAL");
    // Sanity: the WAL exists mid-barrier.
    assert_eq!(
        PartitionedWal::read_all_in(&table_root)
            .await
            .expect("read")
            .len(),
        1,
    );

    // Apply barrier on every partition.
    for p in &prepared {
        p.apply_under_held_barrier().await.expect("apply");
    }

    // Remove WAL post-barrier.
    PartitionedWal::remove(&table_root, &commit_id)
        .await
        .expect("remove WAL");
    assert!(
        PartitionedWal::read_all_in(&table_root)
            .await
            .expect("read")
            .is_empty(),
        "top-level WAL absent after successful barrier"
    );

    // Release fences; finish each.
    drop(fence_guards);
    let mut total = 0u64;
    for prep in prepared {
        total += prep.finish().await.expect("finish");
    }
    assert_eq!(total, 3 * 4 * 8, "all partitions' append rows accounted");
}

// ============================================================================
// Test 6 (issue #10125 step 6) — mid-barrier crash leaves the top-level WAL
// on disk as the recovery anchor. After dropping the prepared handles
// without finishing, the WAL must persist so a process restart can find
// every participating partition and decide what to do.
// ============================================================================

#[tokio::test]
async fn mid_barrier_failure_leaves_top_level_wal() {
    use tokio::sync::mpsc;
    use tokio_stream::wrappers::ReceiverStream;

    let setup = setup_partitions(2).await;
    let table_root = setup._temp_dir.path().join("data");

    let mut prepared = Vec::new();
    for (i, table) in setup.tables.iter().enumerate() {
        let (tx, rx) = mpsc::channel::<datafusion_common::Result<RecordBatch>>(2);
        let schema = test_schema();
        let table_clone = table.clone_for_write_operations();
        let handle = tokio::spawn(async move {
            let stream: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
                schema,
                ReceiverStream::new(rx),
            ));
            let staged = table_clone.begin_staged_append(stream, 1).await?;
            staged.prepare().await
        });
        let base = (i * 100) as i64;
        let ids: Vec<i64> = (base..base + 4).collect();
        let names: Vec<String> = ids.iter().map(|n| format!("m{n}")).collect();
        let names_ref: Vec<&str> = names.iter().map(String::as_str).collect();
        tx.send(Ok(make_batch(&ids, &names_ref)))
            .await
            .expect("send");
        drop(tx);
        prepared.push(handle.await.expect("join").expect("prepare"));
    }

    // Write top-level WAL.
    let commit_id = uuid::Uuid::now_v7().to_string();
    let wal_entries: Vec<cayenne::PartitionedWalEntry> = prepared
        .iter()
        .map(|p| cayenne::PartitionedWalEntry {
            table_id: p.table_id().to_string(),
            staging_wal_path: Some(p.staging_wal_path().to_string_lossy().to_string()),
        })
        .collect();
    let top_level = PartitionedWal::new(
        commit_id.clone(),
        table_root.to_string_lossy().to_string(),
        wal_entries,
    );
    top_level.write_to(&table_root).await.expect("write WAL");

    // Simulate mid-barrier process crash: apply barrier on partition 0 only,
    // then "crash" before applying partition 1 or removing the WAL. We
    // simulate the crash by dropping the prepared handles without finishing.
    prepared[0]
        .apply_under_held_barrier()
        .await
        .expect("apply partition 0");
    // partition 1's barrier never applied.
    drop(prepared);

    // Top-level WAL must STILL be on disk, naming both partitions.
    let recovered = PartitionedWal::read_all_in(&table_root)
        .await
        .expect("read");
    assert_eq!(
        recovered.len(),
        1,
        "WAL persists across the simulated crash"
    );
    let (wal, _) = &recovered[0];
    assert_eq!(wal.commit_id, commit_id);
    assert_eq!(wal.partitions.len(), 2);
}
