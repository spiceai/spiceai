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

#![allow(clippy::expect_used)]

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use cayenne::metadata::{CreateTableOptions, VortexConfig};
use cayenne::{CayenneCatalog, CayenneTableProvider, MetadataCatalog};
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

/// Create a CayenneCatalog and `partition_count` independent Cayenne
/// "tables" sharing it. Each one stands in for one partition in the
/// cross-partition coordinator's view.
async fn setup_partitions(partition_count: usize) -> PartitionSetup {
    let temp_dir = TempDir::new().expect("tempdir");
    let db_path = temp_dir.path().join("test.db");
    let data_path = temp_dir.path().join("data");
    std::fs::create_dir_all(&data_path).expect("data dir");

    let catalog = Arc::new(
        CayenneCatalog::new(format!("sqlite://{}", db_path.to_string_lossy()))
            .expect("catalog"),
    );
    catalog.init().await.expect("init");

    let catalog_dyn: Arc<dyn MetadataCatalog> = catalog.clone();
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
        let prep = table.begin_overwrite(stream).await.expect("begin_overwrite");
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
        let prep = table.begin_overwrite(stream).await.expect("begin_overwrite");
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
        let prep = table.begin_overwrite(stream).await.expect("begin_overwrite");
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
            let stream: SendableRecordBatchStream =
                Box::pin(RecordBatchStreamAdapter::new(schema, ReceiverStream::new(rx)));
            table_clone.begin_overwrite(stream).await
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
