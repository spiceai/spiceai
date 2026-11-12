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

//! Catalog transaction regression guards for mem-tier seals and checkpoints
//! (#16147), against both metastores. These transaction-internal boundaries
//! cannot be driven through a Spicepod.
//!
//! - SQLite: a trigger injects failure at the checkpoint's final inline-delete
//!   statement; a conflicting vector injects failure after a seal's inline
//!   append. The failed transaction must leave the catalog unchanged.
//! - Object-native: the fault-injecting store refuses the dataset head update,
//!   or applies it and loses the response while every read of the head fails.
//!   After a reopen, the catalog holds none of the seal or checkpoint in the
//!   first case and all of it in the second.

#![expect(
    clippy::expect_used,
    reason = "test setup and exact catalog assertions"
)]

use arrow::datatypes::{DataType, Field, Schema};
use cayenne::catalog::{MemTierDurableCommit, SnapshotSequenceCommit};
use cayenne::metadata::SnapshotFileStatistics;
use cayenne::metadata::{CreateTableOptions, DeletionType, VortexConfig};
use cayenne::metastore::ExecuteParams;
use cayenne::metastore::object_native::{ObjectNativeCatalog, dataset_log_at};
use cayenne::{
    CayenneCatalog, DeleteFile, InlinedData, InlinedDelete, MetadataCatalog, TableStatistics,
};
use object_store::{ObjectStore, memory::InMemory, path::Path};
use object_store_sim::{
    Op,
    fault::{Fault, FaultStore},
};
use std::sync::Arc;
use tempfile::TempDir;

const TABLE: &str = "atomic";
const PREFIX: &str = "catalog";
/// A snapshot other than the current one, whose per-file statistics a
/// checkpoint drops.
const STALE_SNAPSHOT: &str = "01900000-0000-7000-8000-000000000001";
const STALE_FILE: &str = "stale.vortex";

fn table_options(base_path: String) -> CreateTableOptions {
    CreateTableOptions {
        table_name: TABLE.to_string(),
        schema: Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
        primary_key: vec![],
        on_conflict: None,
        base_path,
        partition_column: None,
        vortex_config: VortexConfig::default(),
    }
}

/// The table's state before the seal or checkpoint: one inline row, one
/// published inline tombstone, one durable vector, and per-file statistics of a
/// stale snapshot.
async fn plant(catalog: &dyn MetadataCatalog, table_id: &str) {
    catalog
        .add_inlined_data(inline(table_id))
        .await
        .expect("plant inline");
    catalog
        .add_inlined_delete(InlinedDelete {
            inlined_id: String::new(),
            table_id: table_id.to_string(),
            delete_ipc: vec![9],
            delete_count: 1,
            sequence_number: 2,
            created_at: String::new(),
            published: true,
        })
        .await
        .expect("plant tombstone");
    catalog
        .add_delete_file(vector(table_id, "old", 1))
        .await
        .expect("plant vector");
    catalog
        .upsert_snapshot_file_statistics(&SnapshotFileStatistics {
            table_id: table_id.to_string(),
            snapshot_id: STALE_SNAPSHOT.to_string(),
            file_path: STALE_FILE.to_string(),
            file_size_bytes: 100,
            num_rows: 1,
            statistics_blob: vec![4, 5, 6],
        })
        .await
        .expect("plant stale file statistics");
}

/// The statistics a data-bearing checkpoint records.
fn table_statistics(table_id: &str) -> TableStatistics {
    TableStatistics {
        table_id: table_id.to_string(),
        statistics_blob: vec![1, 2],
        num_rows: 5,
        ndv_sketches: None,
        num_rows_exact: false,
    }
}

async fn setup() -> (CayenneCatalog, TempDir, String) {
    let tmp = TempDir::new().expect("tempdir");
    let catalog = CayenneCatalog::new(format!(
        "sqlite://{}",
        tmp.path().join("catalog.db").display()
    ))
    .expect("catalog");
    catalog.init().await.expect("init");
    let table_id = catalog
        .create_table(table_options(
            tmp.path().join("data").to_string_lossy().into_owned(),
        ))
        .await
        .expect("table");
    plant(&catalog, &table_id).await;
    (catalog, tmp, table_id)
}

fn inline(table_id: &str) -> InlinedData {
    InlinedData {
        inlined_id: String::new(),
        table_id: table_id.to_string(),
        partition_key: None,
        data_ipc: vec![1, 2, 3],
        record_count: 1,
        sequence_number: 0,
        created_at: String::new(),
    }
}

fn vector(table_id: &str, path: &str, sequence_number: i64) -> DeleteFile {
    DeleteFile {
        delete_file_id: String::new(),
        table_id: table_id.to_string(),
        source_data_file_path: None,
        path: path.to_string(),
        path_is_relative: true,
        format: "arrow_ipc".to_string(),
        delete_count: 1,
        file_size_bytes: 10,
        deletion_type: DeletionType::KeyBased,
        sequence_number,
        reinsert_sequence: None,
    }
}

/// A seal: shadow rows, a delete-only vector and a reinsert vector.
fn seal(table_id: &str) -> MemTierDurableCommit {
    MemTierDurableCommit {
        table_id: table_id.to_string(),
        inline_data: vec![inline(table_id)],
        pure_delete_files: vec![vector(table_id, "pure", 3)],
        reinsert_delete_files: vec![vector(table_id, "reinsert", 4)],
        insert_pk_bytes: vec![vec![7]],
        sequence_number: 10,
        ..Default::default()
    }
}

/// A data-bearing checkpoint (a reinsert vector, a protected snapshot, insert
/// records for re-inserted keys and the table's statistics), or a
/// tombstones-only one; both clear the inline corpus and stale per-file
/// statistics.
fn checkpoint(table_id: &str, data_bearing: bool, snapshot_id: &str) -> MemTierDurableCommit {
    MemTierDurableCommit {
        table_id: table_id.to_string(),
        pure_delete_files: vec![vector(table_id, "pure", 3)],
        reinsert_delete_files: if data_bearing {
            vec![vector(table_id, "reinsert", 4)]
        } else {
            vec![]
        },
        insert_pk_bytes: if data_bearing { vec![vec![7]] } else { vec![] },
        sequence_number: 10,
        snapshot_sequence: data_bearing.then(|| SnapshotSequenceCommit {
            snapshot_id: snapshot_id.to_string(),
            sequence_number: 10,
        }),
        clear_inlined: true,
        insert_records: if data_bearing { vec![vec![8]] } else { vec![] },
        table_statistics: data_bearing.then(|| table_statistics(table_id)),
        keep_file_statistics_of: Some(snapshot_id.to_string()),
        ..Default::default()
    }
}

/// `(insert records, table statistics, whether the stale per-file statistics
/// remain)`.
async fn extras(
    catalog: &dyn MetadataCatalog,
    table_id: &str,
) -> (Vec<(Vec<u8>, i64)>, Option<(Vec<u8>, i64, bool)>, bool) {
    let mut records: Vec<(Vec<u8>, i64)> = catalog
        .get_insert_records(table_id)
        .await
        .expect("insert records")
        .into_iter()
        .map(|(pk, sequence)| (pk.into_vec(), sequence))
        .collect();
    records.sort_unstable();
    let stats = catalog
        .get_table_statistics(table_id)
        .await
        .expect("table statistics")
        .map(|stats| (stats.statistics_blob, stats.num_rows, stats.num_rows_exact));
    let stale = catalog
        .get_snapshot_file_statistics(table_id, STALE_SNAPSHOT, STALE_FILE)
        .await
        .expect("file statistics")
        .is_some();
    (records, stats, stale)
}

async fn state(catalog: &dyn MetadataCatalog, table_id: &str) -> String {
    let mut files = catalog
        .get_table_delete_files(table_id)
        .await
        .expect("vectors");
    files.sort_by(|a, b| a.path.cmp(&b.path));
    format!(
        "{:?}|{:?}|{:?}|{:?}",
        files,
        catalog.get_inlined_data(table_id).await.expect("inline"),
        catalog
            .get_inlined_deletes(table_id)
            .await
            .expect("tombstones"),
        catalog
            .get_all_snapshot_sequences(table_id)
            .await
            .expect("snapshots")
    )
}

/// The planted state is intact and nothing of the seal or checkpoint is there.
async fn assert_untouched(catalog: &dyn MetadataCatalog, table_id: &str) {
    let rows = catalog.get_inlined_data(table_id).await.expect("inline");
    assert_eq!(
        rows.iter()
            .map(|row| (row.data_ipc.clone(), row.sequence_number))
            .collect::<Vec<_>>(),
        vec![(vec![1, 2, 3], 0)],
        "only the planted inline row"
    );
    assert_eq!(
        catalog
            .get_inlined_deletes(table_id)
            .await
            .expect("tombstones")
            .len(),
        1,
        "only the planted inline tombstone"
    );
    let files = catalog
        .get_table_delete_files(table_id)
        .await
        .expect("vectors");
    assert_eq!(
        files
            .iter()
            .map(|file| (
                file.path.as_str(),
                file.sequence_number,
                file.reinsert_sequence
            ))
            .collect::<Vec<_>>(),
        vec![("old", 1, None)],
        "only the planted vector"
    );
    assert!(
        catalog
            .get_all_snapshot_sequences(table_id)
            .await
            .expect("snapshots")
            .is_empty(),
        "no protected snapshot"
    );
    assert_eq!(
        extras(catalog, table_id).await,
        (vec![], None, true),
        "no insert records or statistics, and the stale file statistics remain"
    );
}

/// All of the seal: the shadow row at the seal sequence beside the planted row,
/// both vectors, and the planted inline tombstone kept.
async fn assert_sealed(catalog: &dyn MetadataCatalog, table_id: &str) {
    let rows = catalog.get_inlined_data(table_id).await.expect("inline");
    assert_eq!(rows.len(), 2);
    let shadow = rows
        .iter()
        .find(|row| row.sequence_number == 10)
        .expect("shadow stamp");
    assert_eq!(shadow.data_ipc, vec![1, 2, 3]);
    assert_eq!(shadow.record_count, 1);
    assert_vectors(catalog, table_id, true).await;
    assert_eq!(
        catalog
            .get_inlined_deletes(table_id)
            .await
            .expect("tombstones")
            .len(),
        1
    );
    assert_eq!(
        catalog
            .get_all_snapshot_sequences(table_id)
            .await
            .expect("snapshots")
            .len(),
        0
    );
    assert_eq!(extras(catalog, table_id).await, (vec![], None, true));
}

/// All of the checkpoint: its vectors, the inline corpus and its tombstones
/// cleared, and the protected snapshot when it carries data.
async fn assert_checkpointed(
    catalog: &dyn MetadataCatalog,
    table_id: &str,
    data_bearing: bool,
    snapshot_id: &str,
) {
    assert_vectors(catalog, table_id, data_bearing).await;
    assert_eq!(
        catalog
            .get_inlined_data(table_id)
            .await
            .expect("inline")
            .len(),
        0
    );
    assert_eq!(
        catalog
            .get_inlined_deletes(table_id)
            .await
            .expect("tombstones")
            .len(),
        0
    );
    assert_eq!(
        catalog
            .get_snapshot_sequence(table_id, snapshot_id)
            .await
            .expect("snapshot"),
        data_bearing.then_some(10)
    );
    assert_eq!(
        extras(catalog, table_id).await,
        if data_bearing {
            (vec![(vec![8], 10)], Some((vec![1, 2], 5, false)), false)
        } else {
            (vec![], None, false)
        },
        "insert records and statistics as committed, and the stale file statistics dropped"
    );
}

#[tokio::test]
async fn mem_tier_seal_atomicity() {
    let (catalog, _tmp, table_id) = setup().await;
    let before = state(&catalog, &table_id).await;
    // Same vector path, different sequence: the SQL idempotency guard fails
    // after staging the shadow insert, rather than rejecting input up front.
    let mut conflicting = seal(&table_id);
    conflicting.reinsert_delete_files = vec![vector(&table_id, "old", 4)];
    let error = catalog
        .commit_mem_tier_durable(conflicting)
        .await
        .expect_err("conflicting vector must fail");
    assert_eq!(state(&catalog, &table_id).await, before);
    assert!(
        error.to_string().contains("NOT NULL constraint failed"),
        "{error}"
    );

    catalog
        .commit_mem_tier_durable(seal(&table_id))
        .await
        .expect("seal");
    assert_sealed(&catalog, &table_id).await;
}

async fn assert_vectors(catalog: &dyn MetadataCatalog, table_id: &str, reinsert: bool) {
    let files = catalog
        .get_table_delete_files(table_id)
        .await
        .expect("vectors");
    let mut actual: Vec<_> = files
        .iter()
        .map(|file| {
            (
                file.path.as_str(),
                file.sequence_number,
                file.reinsert_sequence,
            )
        })
        .collect();
    actual.sort_unstable();
    let mut expected = vec![("old", 1, None), ("pure", 3, None)];
    if reinsert {
        expected.push(("reinsert", 4, Some(10)));
    }
    assert_eq!(actual, expected);
}

#[tokio::test]
async fn mem_tier_checkpoint_atomicity() {
    checkpoint_atomicity(true).await;
}

#[tokio::test]
async fn mem_tier_tombstones_only_checkpoint_atomicity() {
    checkpoint_atomicity(false).await;
}

async fn checkpoint_atomicity(data_bearing: bool) {
    let (catalog, _tmp, table_id) = setup().await;
    let before = state(&catalog, &table_id).await;
    let snapshot_id = uuid::Uuid::now_v7().to_string();
    let tx = catalog
        .begin_transaction()
        .await
        .expect("trigger transaction");
    tx.execute(ExecuteParams {
        sql: "CREATE TRIGGER fail_checkpoint BEFORE DELETE ON cayenne_inlined_delete BEGIN SELECT RAISE(ABORT, 'checkpoint injected failure'); END",
        params: vec![],
    }).await.expect("install trigger");
    tx.commit().await.expect("trigger commit");
    let error = catalog
        .commit_mem_tier_durable(checkpoint(&table_id, data_bearing, &snapshot_id))
        .await
        .expect_err("injected failure");
    assert!(
        error.to_string().contains("checkpoint injected failure"),
        "{error}"
    );
    assert_eq!(state(&catalog, &table_id).await, before);
    let tx = catalog
        .begin_transaction()
        .await
        .expect("remove trigger transaction");
    tx.execute(ExecuteParams {
        sql: "DROP TRIGGER fail_checkpoint",
        params: vec![],
    })
    .await
    .expect("drop trigger");
    tx.commit().await.expect("drop trigger commit");
    catalog
        .commit_mem_tier_durable(checkpoint(&table_id, data_bearing, &snapshot_id))
        .await
        .expect("checkpoint");
    assert_checkpointed(&catalog, &table_id, data_bearing, &snapshot_id).await;
}

/// An object-native catalog over a fault-injecting in-memory store, with the
/// planted table.
struct Native {
    faults: Arc<FaultStore>,
    storage: Arc<dyn ObjectStore>,
    catalog: ObjectNativeCatalog,
    table_id: String,
    /// The dataset log's head object.
    head: Path,
}

impl Native {
    async fn setup() -> Self {
        let storage: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let faults = Arc::new(FaultStore::new(Arc::clone(&storage)));
        let catalog = ObjectNativeCatalog::create(
            Arc::clone(&faults) as Arc<dyn ObjectStore>,
            Path::from(PREFIX),
        )
        .await
        .expect("create the catalog");
        let table_id = catalog
            .create_table(table_options("/tmp/unused/atomic".to_string()))
            .await
            .expect("table");
        plant(&catalog, &table_id).await;
        let head = dataset_log_at(Arc::clone(&storage), &Path::from(PREFIX), TABLE)
            .await
            .expect("read the registry")
            .expect("the dataset has a log")
            .join("head");
        Self {
            faults,
            storage,
            catalog,
            table_id,
            head,
        }
    }

    /// Fail every request of `ops` on the dataset head until the rules clear.
    fn fail_head(&self, ops: &[Op]) {
        for &op in ops {
            let head = self.head.clone();
            self.faults.fail_while(op, move |path| *path == head);
        }
    }

    /// A new process opening the catalog from what reached the store.
    async fn reopen(self) -> (ObjectNativeCatalog, String) {
        self.faults.clear_rules();
        let Self {
            storage,
            catalog,
            table_id,
            ..
        } = self;
        drop(catalog);
        let reopened = ObjectNativeCatalog::open(storage, Path::from(PREFIX))
            .await
            .expect("reopen the catalog");
        (reopened, table_id)
    }
}

/// How the dataset head update fails.
#[derive(Clone, Copy, Debug)]
enum HeadFailure {
    /// Never reaches the store, and every retry fails too.
    Refused,
    /// Reaches the store, but its response is lost and every read of the head
    /// that would resolve it fails.
    Unknown,
}

/// The commit fails on the live catalog, which then serves no state at all
/// rather than state that is not durable; a reopen finds all of the commit or
/// none of it. Time is paused, so the catalog's retry backoff costs nothing.
async fn native_atomicity(
    commit: impl Fn(&str) -> MemTierDurableCommit,
    failure: HeadFailure,
) -> (ObjectNativeCatalog, String) {
    let native = Native::setup().await;
    assert_untouched(&native.catalog, &native.table_id).await;
    let reads = [Op::Get, Op::GetConditional, Op::Head];
    match failure {
        HeadFailure::Refused => native.fail_head(&[Op::Cas]),
        HeadFailure::Unknown => {
            let head = native.head.clone();
            native
                .faults
                .arm(Op::Cas, move |path| *path == head, Fault::DropResponse);
            native.fail_head(&reads);
        }
    }
    native
        .catalog
        .commit_mem_tier_durable(commit(&native.table_id))
        .await
        .expect_err("the head update does not succeed");
    let refused = native
        .catalog
        .get_inlined_data(&native.table_id)
        .await
        .expect_err("a catalog whose commit failed serves no state");
    assert!(
        refused
            .to_string()
            .contains("catalog publication outcome is uncertain; reopen before reading"),
        "{failure:?}: unexpected error: {refused}"
    );
    native.reopen().await
}

#[tokio::test(start_paused = true)]
async fn object_native_mem_tier_seal_atomicity() {
    let (catalog, table_id) = native_atomicity(seal, HeadFailure::Refused).await;
    assert_untouched(&catalog, &table_id).await;
    catalog
        .commit_mem_tier_durable(seal(&table_id))
        .await
        .expect("seal");
    assert_sealed(&catalog, &table_id).await;

    let (catalog, table_id) = native_atomicity(seal, HeadFailure::Unknown).await;
    assert_sealed(&catalog, &table_id).await;
}

#[tokio::test(start_paused = true)]
async fn object_native_mem_tier_checkpoint_atomicity() {
    object_native_checkpoint_atomicity(true).await;
}

#[tokio::test(start_paused = true)]
async fn object_native_mem_tier_tombstones_only_checkpoint_atomicity() {
    object_native_checkpoint_atomicity(false).await;
}

async fn object_native_checkpoint_atomicity(data_bearing: bool) {
    let snapshot_id = uuid::Uuid::now_v7().to_string();
    let commit = |table_id: &str| checkpoint(table_id, data_bearing, &snapshot_id);

    let (catalog, table_id) = native_atomicity(commit, HeadFailure::Refused).await;
    assert_untouched(&catalog, &table_id).await;
    catalog
        .commit_mem_tier_durable(commit(&table_id))
        .await
        .expect("checkpoint");
    assert_checkpointed(&catalog, &table_id, data_bearing, &snapshot_id).await;

    let (catalog, table_id) = native_atomicity(commit, HeadFailure::Unknown).await;
    assert_checkpointed(&catalog, &table_id, data_bearing, &snapshot_id).await;
}
