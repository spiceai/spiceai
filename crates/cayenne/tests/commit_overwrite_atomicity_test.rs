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

//! Regression tests for `CayenneCatalog::commit_overwrite` and
//! `commit_overwrite_in_txn` (ACID Atomicity + Consistency on the
//! per-snapshot state bundle).
//!
//! The new `commit_overwrite_in_txn` (added alongside the two-phase
//! `PreparedOverwrite` lifecycle) promises an atomic seven-statement
//! bundle:
//!
//! 1. clear `cayenne_delete_file`     (per-snapshot deletion vectors)
//! 2. clear `cayenne_insert_record`   (PK re-insert sequence map)
//! 3. clear `cayenne_snapshot_sequence` (Iceberg-style sequence ordering)
//! 4. clear `cayenne_inlined_data`    (small-batch IPC blobs)
//! 5. clear `cayenne_inlined_delete`  (small-batch deletion IDs)
//! 6. clear `cayenne_table_statistics` (planner-bias stats)
//! 7. update `cayenne_table.current_snapshot_id`
//!
//! All seven happen inside the caller's `MetastoreTransaction`, so either
//! every clear lands together with the pointer flip or none of them do.
//!
//! The cross-partition tests in `cross_partition_overwrite_test.rs`
//! already exercise atomicity across partitions for the pointer flip
//! itself, but they don't pre-plant inlined data, table stats, or insert
//! records into the catalog — so they don't exercise rows 4-6 of the
//! bundle. These tests fill that gap and would fail if anyone reverted
//! `commit_overwrite_in_txn` to the `commit_compaction_in_txn` shape
//! (which intentionally PRESERVES inlined data and table stats).

#![allow(clippy::expect_used)]

use std::sync::Arc;

use arrow::array::{BinaryArray, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::StreamWriter;
use cayenne::metadata::{CreateTableOptions, DeletionType, VortexConfig};
use cayenne::{
    CayenneCatalog, DeleteFile, InlinedData, InlinedDelete, MetadataCatalog, TableStatistics,
};
use tempfile::TempDir;

/// Build a fresh on-disk `SQLite` catalog. Returns the catalog handle and
/// the tempdir (kept alive by the caller to keep the DB and data dir
/// rooted).
async fn fresh_catalog() -> (Arc<CayenneCatalog>, TempDir) {
    let tmp = TempDir::new().expect("tempdir");
    let db_path = tmp.path().join("commit_overwrite_test.db");
    let conn = format!("sqlite://{}", db_path.to_string_lossy());
    let catalog = Arc::new(CayenneCatalog::new(conn).expect("catalog"));
    catalog.init().await.expect("catalog init");
    (catalog, tmp)
}

/// Create a no-PK table on the given catalog and return its `table_id`
/// plus a freshly-generated "old" snapshot id (the one the catalog will
/// hold before overwrite).
async fn create_test_table(
    catalog: &CayenneCatalog,
    tmp: &TempDir,
    name: &str,
) -> (String, String) {
    let data_path = tmp.path().join("data");
    std::fs::create_dir_all(&data_path).expect("data dir");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));
    let table_id = catalog
        .create_table(CreateTableOptions {
            table_name: name.to_string(),
            schema,
            primary_key: vec![],
            on_conflict: None,
            base_path: data_path.to_string_lossy().to_string(),
            partition_column: None,
            vortex_config: VortexConfig::default(),
        })
        .await
        .expect("create_table");
    let table_meta = catalog.get_table(name).await.expect("get_table");
    (table_id, table_meta.current_snapshot_id)
}

/// Plant the full pre-overwrite per-snapshot state: a delete file row,
/// an insert record, a snapshot sequence row, inlined data, inlined
/// delete, and table statistics.
///
/// Returns nothing on success; panics with a clear message on any setup
/// error so test failures point at the offending state-planting step.
async fn plant_full_pre_overwrite_state(
    catalog: &CayenneCatalog,
    table_id: &str,
    snapshot_id: &str,
) {
    // 1. cayenne_delete_file row.
    let delete_file = DeleteFile {
        delete_file_id: uuid::Uuid::now_v7().to_string(),
        table_id: table_id.to_string(),
        source_data_file_path: Some("file_0001.vortex".to_string()),
        path: format!("{snapshot_id}/deletions/dv_0001.arrow"),
        path_is_relative: true,
        format: "arrow_ipc".to_string(),
        delete_count: 3,
        file_size_bytes: 128,
        deletion_type: DeletionType::PositionBased,
        sequence_number: 7,
        // Position-based file: carries no keys, so no re-insert sequence.
        reinsert_sequence: None,
    };
    catalog
        .add_delete_file(delete_file)
        .await
        .expect("add_delete_file");

    // 2. cayenne_insert_record row (single PK).
    catalog
        .add_insert_record(table_id, b"pk_one".to_vec(), 11)
        .await
        .expect("add_insert_record");

    // 3. cayenne_snapshot_sequence row.
    catalog
        .set_snapshot_sequence(table_id, snapshot_id, 17)
        .await
        .expect("set_snapshot_sequence");

    // 4. cayenne_inlined_data row.
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3]))],
    )
    .expect("batch");
    let mut ipc = Vec::new();
    {
        let mut w = StreamWriter::try_new(&mut ipc, &schema).expect("ipc writer");
        w.write(&batch).expect("write");
        w.finish().expect("finish");
    }
    catalog
        .add_inlined_data(InlinedData {
            inlined_id: String::new(),
            table_id: table_id.to_string(),
            partition_key: None,
            data_ipc: ipc,
            record_count: 3,
            sequence_number: 21,
            created_at: String::new(),
        })
        .await
        .expect("add_inlined_data");

    // 5. cayenne_inlined_delete row.
    let del_schema = Arc::new(Schema::new(vec![Field::new(
        "row_key",
        DataType::Binary,
        false,
    )]));
    let key_bytes: Vec<[u8; 8]> = (0..2_i64).map(i64::to_be_bytes).collect();
    let key_slices: Vec<&[u8]> = key_bytes.iter().map(<[u8; 8]>::as_slice).collect();
    let del_batch = RecordBatch::try_new(
        Arc::clone(&del_schema),
        vec![Arc::new(BinaryArray::from_vec(key_slices))],
    )
    .expect("del batch");
    let mut del_ipc = Vec::new();
    {
        let mut w = StreamWriter::try_new(&mut del_ipc, &del_schema).expect("ipc writer (deletes)");
        w.write(&del_batch).expect("write");
        w.finish().expect("finish");
    }
    catalog
        .add_inlined_delete(InlinedDelete {
            inlined_id: String::new(),
            table_id: table_id.to_string(),
            delete_ipc: del_ipc,
            delete_count: 2,
            sequence_number: 23,
            created_at: String::new(),
            published: true,
        })
        .await
        .expect("add_inlined_delete");

    // 6. cayenne_table_statistics row.
    catalog
        .upsert_table_statistics(&TableStatistics {
            table_id: table_id.to_string(),
            statistics_blob: vec![0xDE, 0xAD, 0xBE, 0xEF],
            num_rows: 42,
            ndv_sketches: None,
        })
        .await
        .expect("upsert_table_statistics");
}

/// Snapshot of "is the per-snapshot state populated?" — convenient for
/// before/after diffs without re-querying individual rows.
#[derive(Debug, PartialEq, Eq)]
struct StateProbe {
    delete_files: usize,
    insert_records: usize,
    snapshot_sequences: usize,
    inlined_records: i64,
    inlined_deletes: usize,
    has_table_stats: bool,
    current_snapshot_id: String,
}

async fn probe_state(catalog: &CayenneCatalog, table_id: &str, table_name: &str) -> StateProbe {
    let delete_files = catalog
        .get_table_delete_files(table_id)
        .await
        .expect("get_table_delete_files")
        .len();
    let insert_records = catalog
        .get_insert_records(table_id)
        .await
        .expect("get_insert_records")
        .len();
    let snapshot_sequences = catalog
        .get_all_snapshot_sequences(table_id)
        .await
        .expect("get_all_snapshot_sequences")
        .len();
    let inlined_records = catalog
        .get_inlined_data_count(table_id)
        .await
        .expect("get_inlined_data_count");
    let inlined_deletes = catalog
        .get_inlined_deletes(table_id)
        .await
        .expect("get_inlined_deletes")
        .len();
    let has_table_stats = catalog
        .get_table_statistics(table_id)
        .await
        .expect("get_table_statistics")
        .is_some();
    let current_snapshot_id = catalog
        .get_table(table_name)
        .await
        .expect("get_table")
        .current_snapshot_id;
    StateProbe {
        delete_files,
        insert_records,
        snapshot_sequences,
        inlined_records,
        inlined_deletes,
        has_table_stats,
        current_snapshot_id,
    }
}

// ============================================================================
// Test 1 — happy path: commit_overwrite clears every per-snapshot side
// table AND advances the snapshot pointer, all in one shot.
// ============================================================================
#[tokio::test]
async fn commit_overwrite_clears_all_per_snapshot_state() {
    let (catalog, tmp) = fresh_catalog().await;
    let (table_id, old_snapshot) = create_test_table(&catalog, &tmp, "overwrite_clears_all").await;

    plant_full_pre_overwrite_state(&catalog, &table_id, &old_snapshot).await;

    let before = probe_state(&catalog, &table_id, "overwrite_clears_all").await;
    assert_eq!(before.delete_files, 1, "pre: 1 delete file planted");
    assert_eq!(before.insert_records, 1, "pre: 1 insert record planted");
    assert_eq!(
        before.snapshot_sequences, 1,
        "pre: 1 snapshot sequence planted"
    );
    assert_eq!(before.inlined_records, 3, "pre: 3 inlined records planted");
    assert_eq!(before.inlined_deletes, 1, "pre: 1 inlined delete planted");
    assert!(before.has_table_stats, "pre: table stats planted");
    assert_eq!(before.current_snapshot_id, old_snapshot);

    let new_snapshot = uuid::Uuid::now_v7().to_string();
    catalog
        .commit_overwrite(&table_id, &new_snapshot)
        .await
        .expect("commit_overwrite happy path");

    let after = probe_state(&catalog, &table_id, "overwrite_clears_all").await;
    assert_eq!(after.delete_files, 0, "post: delete_file must be cleared");
    assert_eq!(
        after.insert_records, 0,
        "post: insert_record must be cleared"
    );
    assert_eq!(
        after.snapshot_sequences, 0,
        "post: snapshot_sequence must be cleared"
    );
    assert_eq!(
        after.inlined_records, 0,
        "post: inlined_data must be cleared (the headline new behavior)"
    );
    assert_eq!(
        after.inlined_deletes, 0,
        "post: inlined_delete must be cleared (the headline new behavior)"
    );
    assert!(
        !after.has_table_stats,
        "post: table_statistics must be cleared (the headline new behavior)"
    );
    assert_eq!(
        after.current_snapshot_id, new_snapshot,
        "post: current_snapshot_id must advance"
    );
}

// ============================================================================
// Test 2 — atomicity: commit_overwrite_in_txn against a transaction that
// is rolled back must leave EVERY side table at its pre-call state.
// Without bundling, a partial clear would persist any rows that the
// implementation forgot to gate on the transaction.
// ============================================================================
#[tokio::test]
async fn commit_overwrite_in_txn_rolls_back_atomically() {
    let (catalog, tmp) = fresh_catalog().await;
    let (table_id, old_snapshot) = create_test_table(&catalog, &tmp, "overwrite_rolls_back").await;

    plant_full_pre_overwrite_state(&catalog, &table_id, &old_snapshot).await;

    let before = probe_state(&catalog, &table_id, "overwrite_rolls_back").await;

    {
        let mut txn = catalog
            .begin_transaction()
            .await
            .expect("begin_transaction");
        let bogus_new_snapshot = uuid::Uuid::now_v7().to_string();
        catalog
            .commit_overwrite_in_txn(&mut *txn, &table_id, &bogus_new_snapshot)
            .await
            .expect("commit_overwrite_in_txn against borrowed txn");

        // Explicit rollback (don't rely on Drop's spawned best-effort
        // task — we need the visibility guarantee BEFORE the next probe).
        txn.rollback().await.expect("rollback");
    }

    let after = probe_state(&catalog, &table_id, "overwrite_rolls_back").await;
    assert_eq!(
        before, after,
        "rolled-back commit_overwrite_in_txn must be a complete no-op on every side table"
    );
}

// ============================================================================
// Test 3 — input validation: commit_overwrite_in_txn must reject
// malformed UUIDs (defense in depth — SQL is built via string
// interpolation, so the UUID parse is the SQL-injection guard).
// ============================================================================
#[tokio::test]
async fn commit_overwrite_in_txn_rejects_invalid_uuid() {
    let (catalog, _tmp) = fresh_catalog().await;

    let mut txn = catalog
        .begin_transaction()
        .await
        .expect("begin_transaction");

    let bad_table_id = catalog
        .commit_overwrite_in_txn(&mut *txn, "'; DROP TABLE cayenne_table; --", "1234")
        .await;
    assert!(
        bad_table_id.is_err(),
        "table_id with quotes/semicolons must be rejected by UUID parse"
    );

    let valid_table = uuid::Uuid::now_v7().to_string();
    let bad_snapshot = catalog
        .commit_overwrite_in_txn(&mut *txn, &valid_table, "not-a-uuid")
        .await;
    assert!(
        bad_snapshot.is_err(),
        "snapshot_id 'not-a-uuid' must be rejected by UUID parse"
    );

    // Drop the txn so the connection is released for tempdir cleanup.
    drop(txn);
}

// ============================================================================
// Test 4 — isolation: overwriting table A must NOT touch table B's
// per-snapshot state, even though both tables sit in the same SQLite DB
// (same metastore, same `WHERE table_id = ?` clauses must scope every
// DELETE).
// ============================================================================
#[tokio::test]
async fn commit_overwrite_isolated_by_table_id() {
    let (catalog, tmp) = fresh_catalog().await;
    let (table_a, snap_a) = create_test_table(&catalog, &tmp, "iso_table_a").await;
    let (table_b, snap_b) = create_test_table(&catalog, &tmp, "iso_table_b").await;

    plant_full_pre_overwrite_state(&catalog, &table_a, &snap_a).await;
    plant_full_pre_overwrite_state(&catalog, &table_b, &snap_b).await;

    let before_b = probe_state(&catalog, &table_b, "iso_table_b").await;

    let new_snap_a = uuid::Uuid::now_v7().to_string();
    catalog
        .commit_overwrite(&table_a, &new_snap_a)
        .await
        .expect("commit_overwrite table_a");

    // Table A is cleared.
    let after_a = probe_state(&catalog, &table_a, "iso_table_a").await;
    assert_eq!(after_a.delete_files, 0);
    assert_eq!(after_a.insert_records, 0);
    assert_eq!(after_a.snapshot_sequences, 0);
    assert_eq!(after_a.inlined_records, 0);
    assert_eq!(after_a.inlined_deletes, 0);
    assert!(!after_a.has_table_stats);
    assert_eq!(after_a.current_snapshot_id, new_snap_a);

    // Table B is identical to its pre-overwrite snapshot.
    let after_b = probe_state(&catalog, &table_b, "iso_table_b").await;
    assert_eq!(
        before_b, after_b,
        "overwrite on table_a must NOT touch table_b's per-snapshot state"
    );
}

// ============================================================================
// Test 5 — empty pre-state: commit_overwrite on a table with no
// inlined data, no delete files, no stats must succeed cleanly and
// advance the pointer. This is the common case on a brand-new table.
// ============================================================================
#[tokio::test]
async fn commit_overwrite_succeeds_on_empty_pre_state() {
    let (catalog, tmp) = fresh_catalog().await;
    let (table_id, old_snapshot) = create_test_table(&catalog, &tmp, "overwrite_empty").await;

    // No state planted — fresh table.
    let new_snapshot = uuid::Uuid::now_v7().to_string();
    catalog
        .commit_overwrite(&table_id, &new_snapshot)
        .await
        .expect("commit_overwrite on empty pre-state");

    let after = probe_state(&catalog, &table_id, "overwrite_empty").await;
    assert_eq!(after.delete_files, 0);
    assert_eq!(after.insert_records, 0);
    assert_eq!(after.snapshot_sequences, 0);
    assert_eq!(after.inlined_records, 0);
    assert_eq!(after.inlined_deletes, 0);
    assert!(!after.has_table_stats);
    assert_ne!(
        after.current_snapshot_id, old_snapshot,
        "current_snapshot_id must advance even on an empty pre-state"
    );
    assert_eq!(after.current_snapshot_id, new_snapshot);
}

// ============================================================================
// Test 6 — DEVIL'S ADVOCATE / behavior-divergence: this is the test
// that proves commit_overwrite_in_txn is actually different from
// commit_compaction_in_txn in the way the module comments claim.
//
// Compaction PRESERVES inlined data and table stats (the rewrite only
// consolidates Vortex files, the inline memtable is still valid for the
// new snapshot). Overwrite REPLACES the table's contents, so the same
// rows must be CLEARED.
//
// If a future refactor accidentally points commit_overwrite_in_txn at
// commit_compaction_in_txn's SQL, this test fails on inlined_records,
// inlined_deletes, and has_table_stats while every other assertion
// still passes — a clear, narrow regression signal.
// ============================================================================
#[tokio::test]
async fn commit_overwrite_clears_inlined_state_unlike_commit_compaction() {
    let (catalog, tmp) = fresh_catalog().await;

    // Two tables. Same planted state. One gets commit_compaction, the
    // other gets commit_overwrite. The behavior must diverge on the
    // inlined-data/inlined-delete/table-stats rows.
    let (compact_id, compact_snap) = create_test_table(&catalog, &tmp, "divergence_compact").await;
    let (overwrite_id, overwrite_snap) =
        create_test_table(&catalog, &tmp, "divergence_overwrite").await;

    plant_full_pre_overwrite_state(&catalog, &compact_id, &compact_snap).await;
    plant_full_pre_overwrite_state(&catalog, &overwrite_id, &overwrite_snap).await;

    let new_compact_snap = uuid::Uuid::now_v7().to_string();
    let new_overwrite_snap = uuid::Uuid::now_v7().to_string();

    catalog
        .commit_compaction(&compact_id, &new_compact_snap)
        .await
        .expect("commit_compaction");
    catalog
        .commit_overwrite(&overwrite_id, &new_overwrite_snap)
        .await
        .expect("commit_overwrite");

    let compact_after = probe_state(&catalog, &compact_id, "divergence_compact").await;
    let overwrite_after = probe_state(&catalog, &overwrite_id, "divergence_overwrite").await;

    // Both clear the per-snapshot delete/insert/sequence side tables.
    assert_eq!(compact_after.delete_files, 0);
    assert_eq!(overwrite_after.delete_files, 0);
    assert_eq!(compact_after.insert_records, 0);
    assert_eq!(overwrite_after.insert_records, 0);
    assert_eq!(compact_after.snapshot_sequences, 0);
    assert_eq!(overwrite_after.snapshot_sequences, 0);

    // The divergence: compaction PRESERVES inlined data + inlined deletes
    // + table stats; overwrite CLEARS them. If this assertion ever
    // breaks, either commit_compaction is silently clearing data the
    // inline memtable still needs, or commit_overwrite has regressed to
    // compaction-style semantics and stale rows will re-surface in scans
    // after an INSERT OVERWRITE.
    assert_eq!(
        compact_after.inlined_records, 3,
        "compaction must PRESERVE inlined data (the inline memtable is still valid)"
    );
    assert_eq!(
        overwrite_after.inlined_records, 0,
        "overwrite must CLEAR inlined data (the old contents are gone)"
    );
    assert_eq!(
        compact_after.inlined_deletes, 1,
        "compaction must PRESERVE inlined deletes"
    );
    assert_eq!(
        overwrite_after.inlined_deletes, 0,
        "overwrite must CLEAR inlined deletes"
    );
    assert!(
        compact_after.has_table_stats,
        "compaction must PRESERVE table statistics"
    );
    assert!(
        !overwrite_after.has_table_stats,
        "overwrite must CLEAR table statistics"
    );

    // Both advance the snapshot pointer.
    assert_eq!(compact_after.current_snapshot_id, new_compact_snap);
    assert_eq!(overwrite_after.current_snapshot_id, new_overwrite_snap);
}

// ============================================================================
// Test 7 — cross-partition shared-transaction shape: two table_ids,
// one shared `MetastoreTransaction`, both calls to
// commit_overwrite_in_txn. After commit, both tables' per-snapshot
// state is fully cleared and both pointers advance. This mirrors the
// `PartitionedInsertStrategy` coordinator's call pattern, and proves
// that two `commit_overwrite_in_txn` calls in the same transaction do
// not stomp each other or leak cross-table state.
// ============================================================================
#[tokio::test]
async fn two_commit_overwrites_in_one_txn_both_apply() {
    let (catalog, tmp) = fresh_catalog().await;
    let (table_a, snap_a) = create_test_table(&catalog, &tmp, "shared_txn_a").await;
    let (table_b, snap_b) = create_test_table(&catalog, &tmp, "shared_txn_b").await;

    plant_full_pre_overwrite_state(&catalog, &table_a, &snap_a).await;
    plant_full_pre_overwrite_state(&catalog, &table_b, &snap_b).await;

    let new_a = uuid::Uuid::now_v7().to_string();
    let new_b = uuid::Uuid::now_v7().to_string();

    {
        let mut txn = catalog
            .begin_transaction()
            .await
            .expect("begin_transaction");
        catalog
            .commit_overwrite_in_txn(&mut *txn, &table_a, &new_a)
            .await
            .expect("commit_overwrite_in_txn table_a");
        catalog
            .commit_overwrite_in_txn(&mut *txn, &table_b, &new_b)
            .await
            .expect("commit_overwrite_in_txn table_b");
        txn.commit().await.expect("shared txn commit");
    }

    for (tid, name, new_snap) in [
        (&table_a, "shared_txn_a", &new_a),
        (&table_b, "shared_txn_b", &new_b),
    ] {
        let after = probe_state(&catalog, tid, name).await;
        assert_eq!(after.delete_files, 0, "{name}: delete_files cleared");
        assert_eq!(after.insert_records, 0, "{name}: insert_records cleared");
        assert_eq!(
            after.snapshot_sequences, 0,
            "{name}: snapshot_sequences cleared"
        );
        assert_eq!(after.inlined_records, 0, "{name}: inlined_records cleared");
        assert_eq!(after.inlined_deletes, 0, "{name}: inlined_deletes cleared");
        assert!(!after.has_table_stats, "{name}: table_statistics cleared");
        assert_eq!(
            &after.current_snapshot_id, new_snap,
            "{name}: current_snapshot_id advanced"
        );
    }
}

// ============================================================================
// Test 8 — partial rollback edge: stage one good apply, then a SECOND
// commit_overwrite_in_txn against the same shared txn with an invalid
// UUID. The shared txn must surface the error so the coordinator can
// roll back; on rollback, the FIRST table's state must be restored
// too. (This is the cross-partition "all-or-nothing" property at the
// state-bundle level — the cross-partition test verifies it at the
// pointer level; this one verifies it for the full bundle.)
// ============================================================================
#[tokio::test]
async fn commit_overwrite_in_txn_partial_failure_rolls_back_full_bundle() {
    let (catalog, tmp) = fresh_catalog().await;
    let (table_a, snap_a) = create_test_table(&catalog, &tmp, "partial_a").await;

    plant_full_pre_overwrite_state(&catalog, &table_a, &snap_a).await;
    let before = probe_state(&catalog, &table_a, "partial_a").await;

    {
        let mut txn = catalog
            .begin_transaction()
            .await
            .expect("begin_transaction");

        // First call lands inside the txn (clears every side table for
        // table_a — but only WITHIN the transaction view; nothing is
        // committed yet).
        let new_a = uuid::Uuid::now_v7().to_string();
        catalog
            .commit_overwrite_in_txn(&mut *txn, &table_a, &new_a)
            .await
            .expect("first call lands");

        // Second call against the same txn with an invalid UUID is
        // rejected at validation; the txn is still alive but tainted.
        let bad = catalog
            .commit_overwrite_in_txn(&mut *txn, "not-a-uuid", "also-not-a-uuid")
            .await;
        assert!(bad.is_err(), "invalid UUID call must be rejected");

        // Coordinator's response: roll back the shared txn.
        txn.rollback().await.expect("rollback");
    }

    let after = probe_state(&catalog, &table_a, "partial_a").await;
    assert_eq!(
        before, after,
        "rolled-back shared txn must restore EVERY side-table row and the snapshot pointer"
    );
}
