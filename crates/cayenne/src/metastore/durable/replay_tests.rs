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

//! Every kind of catalog action replays to the catalog that recorded it. The
//! oracle is the writer's own replica: a replica rebuilt from object storage
//! must hold exactly its rows, rowids included.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion_table_providers::util::{
    column_reference::ColumnReference, on_conflict::OnConflict,
};
use futures::TryStreamExt;
use object_store::ObjectStore;
use object_store_occ::LocalConditionalPut;
use tempfile::TempDir;

use super::log::DurableLog;
use super::{CatalogLocation, wire};
use crate::catalog::{MetadataCatalog, SnapshotSequenceCommit};
use crate::cayenne_catalog::CayenneCatalog;
use crate::metadata::{
    ColdTierFile, CreateTableOptions, DeleteFile, DeletionType, IndexRunRecord, InlinedData,
    InlinedDelete, PartitionMetadata, SnapshotFile, SnapshotFileStatistics, TableStatistics,
    VortexConfig,
};
use crate::provider::on_conflict::PreparedOnConflictDurablePayload;

struct Harness {
    dir: TempDir,
    store: Arc<dyn ObjectStore>,
}

impl Harness {
    fn new() -> Self {
        let dir = TempDir::new().expect("temp dir");
        let bucket = dir.path().join("bucket");
        std::fs::create_dir_all(&bucket).expect("bucket dir");
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalConditionalPut::new(bucket).expect("local conditional store"));
        Self { dir, store }
    }

    fn location(&self) -> CatalogLocation {
        CatalogLocation::new(
            Arc::clone(&self.store),
            object_store::path::Path::from("tables/actions/catalog"),
            "file:///bucket/tables/actions/catalog",
        )
    }

    fn replica(&self, name: &str) -> PathBuf {
        self.dir.path().join(name).join("cayenne.db")
    }

    fn data_path(&self) -> String {
        self.dir.path().join("data").to_string_lossy().to_string()
    }

    async fn catalog(&self, replica: &str) -> CayenneCatalog {
        let catalog = CayenneCatalog::new_durable(
            format!("sqlite://{}", self.replica(replica).display()),
            self.location(),
        )
        .expect("durable catalog");
        catalog.init().await.expect("init durable catalog");
        catalog
    }

    async fn images(&self) -> usize {
        let keys: Vec<String> = self
            .store
            .list(None)
            .map_ok(|meta| meta.location.to_string())
            .try_collect()
            .await
            .expect("list bucket");
        keys.iter().filter(|key| key.contains("/images/")).count()
    }

    /// The kinds of action in the durable transactions after `sequence`.
    async fn action_kinds_after(&self, sequence: u64) -> BTreeSet<&'static str> {
        let log = DurableLog::open(self.location())
            .await
            .expect("open the catalog log")
            .expect("a catalog");
        let recovery = log.recovery(Some(sequence)).await.expect("recovery plan");
        assert!(
            recovery.image.is_none(),
            "the transactions after {sequence} must replay without a newer image"
        );
        let mut kinds = BTreeSet::new();
        for entry in &recovery.logs {
            let transaction = log.fetch(entry).await.expect("fetch transaction");
            for action in &transaction.actions {
                kinds.insert(kind(action.action.as_ref().expect("an action")));
            }
        }
        kinds
    }
}

/// Every action kind, by name. Exhaustive, so a new kind fails to compile here
/// until this test covers it.
fn kind(action: &wire::action::Action) -> &'static str {
    use wire::action::Action as A;
    match action {
        A::CreateTable(_) => "CreateTable",
        A::UpdateTableSchema(_) => "UpdateTableSchema",
        A::UpdateTableSettings(_) => "UpdateTableSettings",
        A::DropTable(_) => "DropTable",
        A::SetCurrentSnapshot(_) => "SetCurrentSnapshot",
        A::SetCurrentSnapshots(_) => "SetCurrentSnapshots",
        A::ReserveSequenceNumbers(_) => "ReserveSequenceNumbers",
        A::SetSnapshotSequence(_) => "SetSnapshotSequence",
        A::ClearSnapshotSequence(_) => "ClearSnapshotSequence",
        A::AddDeleteFile(_) => "AddDeleteFile",
        A::AddDeleteFiles(_) => "AddDeleteFiles",
        A::RemoveDeleteFiles(_) => "RemoveDeleteFiles",
        A::ClearDeleteFiles(_) => "ClearDeleteFiles",
        A::CommitDeleteFilesWithInlinedRewrite(_) => "CommitDeleteFilesWithInlinedRewrite",
        A::AddInsertRecords(_) => "AddInsertRecords",
        A::ClearInsertRecords(_) => "ClearInsertRecords",
        A::CommitOnConflictDeletions(_) => "CommitOnConflictDeletions",
        A::CommitOnConflictDeletionsWithTombstone(_) => "CommitOnConflictDeletionsWithTombstone",
        A::ApplyPreparedOnConflict(_) => "ApplyPreparedOnConflict",
        A::CommitCompaction(_) => "CommitCompaction",
        A::CommitCompactionFenced(_) => "CommitCompactionFenced",
        A::SwapProtectedSnapshots(_) => "SwapProtectedSnapshots",
        A::CommitOverwrite(_) => "CommitOverwrite",
        A::CommitOverwriteToCold(_) => "CommitOverwriteToCold",
        A::AddPartition(_) => "AddPartition",
        A::UpsertTableStatistics(_) => "UpsertTableStatistics",
        A::ClearTableStatistics(_) => "ClearTableStatistics",
        A::UpsertSnapshotFileStatistics(_) => "UpsertSnapshotFileStatistics",
        A::ClearSnapshotFileStatistics(_) => "ClearSnapshotFileStatistics",
        A::ClearSnapshotCachedMetadata(_) => "ClearSnapshotCachedMetadata",
        A::UpsertSnapshotFile(_) => "UpsertSnapshotFile",
        A::ReplaceSnapshotFiles(_) => "ReplaceSnapshotFiles",
        A::ClearSnapshotFiles(_) => "ClearSnapshotFiles",
        A::UpsertPkIndex(_) => "UpsertPkIndex",
        A::ClearPkIndex(_) => "ClearPkIndex",
        A::RegisterIndexRun(_) => "RegisterIndexRun",
        A::RemoveIndexRun(_) => "RemoveIndexRun",
        A::AddInlinedData(_) => "AddInlinedData",
        A::ClearInlinedData(_) => "ClearInlinedData",
        A::ClearInlinedDeletes(_) => "ClearInlinedDeletes",
        A::AddInlinedDelete(_) => "AddInlinedDelete",
        A::SetInlinedDeletePublished(_) => "SetInlinedDeletePublished",
        A::PublishOrphanInlinedDeletes(_) => "PublishOrphanInlinedDeletes",
        A::CommitInlinedMutation(_) => "CommitInlinedMutation",
        A::MarkDirtyKeys(_) => "MarkDirtyKeys",
        A::ClearPendingWriteBack(_) => "ClearPendingWriteBack",
    }
}

const ACTION_KINDS: usize = 46;

/// Every row of every catalog table in the file at `db`, with the rowid of
/// rowid tables, in a canonical order. The position table differs by design.
fn dump(db: &Path) -> BTreeMap<String, Vec<String>> {
    let conn =
        rusqlite::Connection::open_with_flags(db, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .expect("open replica");
    let tables: Vec<(String, bool)> = conn
        .prepare(
            "SELECT name, wr FROM pragma_table_list WHERE schema = 'main' AND type = 'table' AND name LIKE 'cayenne_%' AND name <> 'cayenne_durable_position'",
        )
        .and_then(|mut stmt| {
            stmt.query_map([], |row| Ok((row.get(0)?, row.get::<_, i64>(1)? != 0)))?
                .collect()
        })
        .expect("list tables");
    let mut out = BTreeMap::new();
    for (table, without_rowid) in tables {
        let sql = if without_rowid {
            format!("SELECT * FROM \"{table}\"")
        } else {
            format!("SELECT rowid, * FROM \"{table}\"")
        };
        let mut stmt = conn.prepare(&sql).expect("prepare dump");
        let width = stmt.column_count();
        let mut rows: Vec<String> = stmt
            .query_map([], |row| {
                (0..width)
                    .map(|i| row.get::<_, rusqlite::types::Value>(i))
                    .collect::<Result<Vec<_>, _>>()
                    .map(|values| format!("{values:?}"))
            })
            .and_then(Iterator::collect)
            .expect("dump rows");
        rows.sort();
        out.insert(table, rows);
    }
    out
}

fn assert_same_catalog(
    restored: &BTreeMap<String, Vec<String>>,
    expected: &BTreeMap<String, Vec<String>>,
    phase: &str,
) {
    assert_eq!(
        restored.keys().collect::<Vec<_>>(),
        expected.keys().collect::<Vec<_>>(),
        "{phase}: restored catalog tables"
    );
    for (table, rows) in expected {
        assert_eq!(&restored[table], rows, "{phase}: table {table}");
    }
}

fn id() -> String {
    uuid::Uuid::now_v7().to_string()
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]))
}

fn options(harness: &Harness, table_name: &str, vortex_config: VortexConfig) -> CreateTableOptions {
    CreateTableOptions {
        table_name: table_name.to_string(),
        schema: schema(),
        primary_key: vec!["id".to_string()],
        on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
            "id".to_string(),
        ]))),
        base_path: harness.data_path(),
        partition_column: None,
        vortex_config,
    }
}

fn delete_file(
    table_id: &str,
    path: &str,
    sequence_number: i64,
    deletion_type: DeletionType,
) -> DeleteFile {
    DeleteFile {
        delete_file_id: String::new(),
        table_id: table_id.to_string(),
        source_data_file_path: (deletion_type == DeletionType::PositionBased)
            .then(|| format!("{path}.vortex")),
        path: path.to_string(),
        path_is_relative: false,
        format: "arrow_ipc".to_string(),
        delete_count: 1,
        file_size_bytes: 10,
        deletion_type,
        sequence_number,
        reinsert_sequence: None,
    }
}

fn inlined_data(
    table_id: &str,
    inlined_id: &str,
    payload: u8,
    sequence_number: i64,
) -> InlinedData {
    InlinedData {
        inlined_id: inlined_id.to_string(),
        table_id: table_id.to_string(),
        partition_key: None,
        data_ipc: vec![payload; 4],
        record_count: 1,
        sequence_number,
        created_at: String::new(),
    }
}

fn inlined_delete(table_id: &str, inlined_id: &str, sequence_number: i64) -> InlinedDelete {
    InlinedDelete {
        inlined_id: inlined_id.to_string(),
        table_id: table_id.to_string(),
        delete_ipc: vec![1, 2, 3],
        delete_count: 1,
        sequence_number,
        created_at: String::new(),
        published: false,
    }
}

fn snapshot_file(table_id: &str, snapshot_id: &str, file_path: &str) -> SnapshotFile {
    SnapshotFile {
        table_id: table_id.to_string(),
        snapshot_id: snapshot_id.to_string(),
        file_path: file_path.to_string(),
        row_count: 3,
        file_size_bytes: 30,
        min_sequence: 1,
        max_sequence: 3,
        digest: Some(format!("digest-{file_path}")),
    }
}

fn file_statistics(table_id: &str, snapshot_id: &str, file_path: &str) -> SnapshotFileStatistics {
    SnapshotFileStatistics {
        table_id: table_id.to_string(),
        snapshot_id: snapshot_id.to_string(),
        file_path: file_path.to_string(),
        file_size_bytes: 30,
        num_rows: 3,
        statistics_blob: vec![7, 7],
    }
}

fn table_statistics(table_id: &str, num_rows: i64) -> TableStatistics {
    TableStatistics {
        table_id: table_id.to_string(),
        statistics_blob: vec![1, 2],
        num_rows,
        num_rows_exact: true,
        ndv_sketches: Some(vec![3, 4]),
    }
}

/// Phase one: every action that keeps the table's current snapshot.
async fn phase_one(catalog: &CayenneCatalog, harness: &Harness) -> String {
    let table_id = catalog
        .create_table(options(harness, "actions", VortexConfig::default()))
        .await
        .expect("create table");
    let s0 = catalog
        .get_table("actions")
        .await
        .expect("table")
        .current_snapshot_id;
    catalog
        .update_table_schema(&table_id, &schema())
        .await
        .expect("update schema");
    catalog
        .update_table_schema_dropping_statistics(&table_id, &schema())
        .await
        .expect("update schema dropping statistics");
    // Reopening with a new sort order persists it in place.
    let resorted = VortexConfig {
        sort_columns: vec!["id".to_string()],
        ..VortexConfig::default()
    };
    assert_eq!(
        catalog
            .create_table(options(harness, "actions", resorted))
            .await
            .expect("reopen with new settings"),
        table_id
    );
    let s1 = id();
    catalog
        .set_current_snapshot(&table_id, &s0, &s1)
        .await
        .expect("set current snapshot");
    catalog
        .reserve_sequence_numbers(&table_id, 100)
        .await
        .expect("reserve sequences");
    let (p1, p2, p3) = (id(), id(), id());
    for (snapshot, sequence) in [(&p1, 10), (&p2, 11), (&p3, 12)] {
        catalog
            .set_snapshot_sequence(&table_id, snapshot, sequence)
            .await
            .expect("set snapshot sequence");
    }
    catalog
        .clear_snapshot_sequence(&table_id, &p3)
        .await
        .expect("clear snapshot sequence");

    let first = delete_file(&table_id, "del/a.arrow", 5, DeletionType::PositionBased);
    let first_id = catalog
        .add_delete_file(first.clone())
        .await
        .expect("add delete file");
    assert_eq!(
        catalog.add_delete_file(first).await.expect("re-add"),
        first_id,
        "an identical delete file is added once"
    );
    catalog
        .add_delete_files(vec![
            delete_file(&table_id, "del/b.arrow", 6, DeletionType::PositionBased),
            delete_file(&table_id, "del/c.arrow", 7, DeletionType::KeyBased),
        ])
        .await
        .expect("add delete files");
    let b_id = catalog
        .get_table_delete_files(&table_id)
        .await
        .expect("delete files")
        .into_iter()
        .find(|file| file.path == "del/b.arrow")
        .expect("delete file b")
        .delete_file_id;
    catalog
        .remove_delete_files(&table_id, &[b_id])
        .await
        .expect("remove delete files");

    catalog
        .add_insert_record(&table_id, vec![1], 20)
        .await
        .expect("add insert record");
    catalog
        .add_insert_records_batch(&table_id, vec![vec![2], vec![3]], 21)
        .await
        .expect("add insert records");

    let (p4, p5, p6) = (id(), id(), id());
    catalog
        .commit_on_conflict_deletions(
            vec![delete_file(
                &table_id,
                "del/d.arrow",
                30,
                DeletionType::KeyBased,
            )],
            &table_id,
            vec![vec![4]],
            31,
            Some(SnapshotSequenceCommit {
                snapshot_id: p4,
                sequence_number: 31,
            }),
        )
        .await
        .expect("on-conflict publish");
    let tombstone = catalog
        .commit_on_conflict_deletions_with_tombstone(
            vec![delete_file(
                &table_id,
                "del/e.arrow",
                32,
                DeletionType::KeyBased,
            )],
            &table_id,
            vec![vec![5]],
            33,
            Some(SnapshotSequenceCommit {
                snapshot_id: p5,
                sequence_number: 33,
            }),
            Some(inlined_delete(&table_id, "", 33)),
            &[],
        )
        .await
        .expect("on-conflict publish with tombstone")
        .expect("tombstone id");
    let mut payload = PreparedOnConflictDurablePayload {
        table_id: table_id.clone(),
        delete_files: vec![delete_file(
            &table_id,
            "del/f.arrow",
            34,
            DeletionType::KeyBased,
        )],
        insert_pk_bytes: vec![vec![6]],
        inline_tombstone: Some(inlined_delete(&table_id, &id(), 35)),
        pending_durable_flips: vec![tombstone],
    };
    let txn = catalog.begin_transaction().await.expect("begin");
    let action = CayenneCatalog::prepared_on_conflict_action(&mut payload, &p6, 35, Some(35))
        .expect("prepared publish");
    CayenneCatalog::apply_prepared_on_conflict(&*txn, &action)
        .await
        .expect("apply prepared publish");
    txn.record(action.into());
    txn.commit().await.expect("commit prepared publish");

    catalog
        .upsert_table_statistics(&table_statistics(&table_id, 3))
        .await
        .expect("upsert statistics");
    catalog
        .clear_table_statistics(&table_id)
        .await
        .expect("clear statistics");
    catalog
        .upsert_table_statistics(&table_statistics(&table_id, 4))
        .await
        .expect("upsert statistics again");
    catalog
        .upsert_snapshot_file_statistics(&file_statistics(&table_id, &s1, "a"))
        .await
        .expect("upsert file statistics");
    catalog
        .upsert_snapshot_file_statistics(&file_statistics(&table_id, &p1, "b"))
        .await
        .expect("upsert file statistics of another snapshot");
    catalog
        .clear_snapshot_file_statistics_except(&table_id, &s1)
        .await
        .expect("clear other file statistics");
    catalog
        .upsert_snapshot_file(&snapshot_file(&table_id, &s1, "a"))
        .await
        .expect("upsert snapshot file");
    catalog
        .replace_snapshot_files(
            &table_id,
            &p1,
            &[
                snapshot_file(&table_id, &p1, "b"),
                snapshot_file(&table_id, &p1, "c"),
            ],
        )
        .await
        .expect("replace snapshot files");
    catalog
        .clear_snapshot_cached_metadata(&table_id, &p1)
        .await
        .expect("clear a snapshot's cached metadata");
    catalog
        .replace_snapshot_files(&table_id, &p2, &[snapshot_file(&table_id, &p2, "d")])
        .await
        .expect("replace snapshot files of another snapshot");
    catalog
        .clear_snapshot_files_except(&table_id, &s1)
        .await
        .expect("clear other snapshot files");

    catalog
        .upsert_pk_index(&table_id, &s1, &[9, 9])
        .await
        .expect("upsert pk index");
    catalog
        .clear_pk_index(&table_id)
        .await
        .expect("clear pk index");
    catalog
        .upsert_pk_index(&table_id, &s1, &[8, 8])
        .await
        .expect("upsert pk index again");
    for run_name in ["r1", "r2"] {
        catalog
            .register_index_run(&IndexRunRecord {
                table_id: table_id.clone(),
                index_key: "k".to_string(),
                run_name: run_name.to_string(),
                row_count: 1,
                size_bytes: 10,
            })
            .await
            .expect("register index run");
    }
    catalog
        .remove_index_run(&table_id, "k", "r1")
        .await
        .expect("remove index run");
    catalog
        .add_partition(PartitionMetadata {
            partition_id: String::new(),
            table_id: table_id.clone(),
            partition_columns: vec!["name".to_string()],
            partition_values: vec!["a".to_string()],
            path: format!("{}/name=a", harness.data_path()),
            path_is_relative: false,
            record_count: 0,
            file_size_bytes: 0,
        })
        .await
        .expect("add partition");

    let inlined = catalog
        .add_inlined_data(inlined_data(&table_id, "", 1, 36))
        .await
        .expect("add inlined data");
    catalog
        .commit_inlined_mutation(
            &table_id,
            vec![inlined_data(&table_id, &inlined, 2, 36)],
            Vec::new(),
            vec![inlined_data(&table_id, "", 3, 0)],
            40,
        )
        .await
        .expect("inline mutation");
    catalog
        .commit_delete_files_with_inlined_rewrite(
            vec![delete_file(
                &table_id,
                "del/g.arrow",
                41,
                DeletionType::PositionBased,
            )],
            &table_id,
            vec![inlined_data(&table_id, &inlined, 4, 36)],
            Vec::new(),
        )
        .await
        .expect("delete with inline rewrite");
    let delete = catalog
        .add_inlined_delete(inlined_delete(&table_id, "", 42))
        .await
        .expect("add inlined delete");
    catalog
        .mark_inlined_delete_published(&table_id, &delete)
        .await
        .expect("publish inlined delete");
    catalog
        .mark_inlined_delete_unpublished(&table_id, &delete)
        .await
        .expect("unpublish inlined delete");
    // The prepared publish's tombstone and the one just unpublished.
    assert_eq!(
        catalog
            .publish_orphan_inlined_deletes(&table_id)
            .await
            .expect("publish orphans"),
        2
    );
    table_id
}

/// Phase two: the rewrites, the clears and a dropped table.
async fn phase_two(catalog: &CayenneCatalog, harness: &Harness, table_id: &str) {
    let s2 = id();
    let mut txn = catalog.begin_transaction().await.expect("begin");
    catalog
        .set_current_snapshots_in_txn(&mut *txn, &[(table_id, s2.as_str())])
        .await
        .expect("set current snapshots");
    catalog
        .clear_table_statistics_in_txn(&mut *txn, table_id)
        .await
        .expect("clear statistics in transaction");
    catalog
        .replace_snapshot_files_in_txn(
            &mut *txn,
            table_id,
            &s2,
            &[snapshot_file(table_id, &s2, "e")],
        )
        .await
        .expect("replace snapshot files in transaction");
    txn.commit().await.expect("commit pointer swap");

    let s3 = id();
    catalog
        .commit_compaction(table_id, &s2, &s3)
        .await
        .expect("compaction");
    let (p7, p8, p9) = (id(), id(), id());
    for (snapshot, sequence) in [(&p7, 45), (&p8, 46)] {
        catalog
            .set_snapshot_sequence(table_id, snapshot, sequence)
            .await
            .expect("set protected snapshot");
    }
    let s4 = id();
    catalog
        .commit_compaction_fenced(table_id, &s3, &s4, 45, std::slice::from_ref(&p7))
        .await
        .expect("fenced compaction");
    assert!(
        catalog
            .swap_protected_snapshots(table_id, &[p8], &p9, 50)
            .await
            .expect("swap protected snapshots")
    );
    let s5 = id();
    catalog
        .commit_overwrite(
            table_id,
            &s5,
            Some(&inlined_data(table_id, &id(), 5, 70)),
            &[delete_file(
                table_id,
                "del/h.arrow",
                71,
                DeletionType::PositionBased,
            )],
        )
        .await
        .expect("overwrite");
    let s6 = id();
    catalog
        .commit_overwrite_to_cold(
            table_id,
            &s6,
            &[ColdTierFile {
                table_id: table_id.to_string(),
                file_url: "s3://cold/actions/1.vortex".to_string(),
                row_count: 3,
                file_size_bytes: 30,
                min_sequence: 1,
                max_sequence: 71,
                statistics_blob: vec![6],
                pk_bloom: Some(vec![5]),
            }],
        )
        .await
        .expect("overwrite to cold");

    let mut txn = catalog.begin_transaction().await.expect("begin");
    catalog
        .mark_dirty_keys_in_txn(&mut *txn, table_id, &[vec![1], vec![2]], 60)
        .await
        .expect("mark dirty keys");
    txn.commit().await.expect("commit dirty keys");
    catalog
        .clear_pending_write_back(table_id, &[(vec![1], 60)])
        .await
        .expect("clear delivered keys");

    catalog
        .add_inlined_data(inlined_data(table_id, "", 6, 72))
        .await
        .expect("add inlined data");
    catalog
        .add_inlined_delete(inlined_delete(table_id, "", 72))
        .await
        .expect("add inlined delete");
    catalog
        .clear_inlined_data_and_deletes(table_id)
        .await
        .expect("clear inlined data and deletes");
    catalog
        .add_inlined_data(inlined_data(table_id, "", 7, 73))
        .await
        .expect("add inlined data again");
    catalog
        .clear_inlined_data(table_id)
        .await
        .expect("clear inlined data");
    catalog
        .add_inlined_delete(inlined_delete(table_id, "", 73))
        .await
        .expect("add inlined delete again");
    catalog
        .clear_inlined_deletes(table_id)
        .await
        .expect("clear inlined deletes");
    catalog
        .add_delete_file(delete_file(
            table_id,
            "del/i.arrow",
            74,
            DeletionType::PositionBased,
        ))
        .await
        .expect("add delete file");
    catalog
        .clear_delete_files(table_id)
        .await
        .expect("clear delete files");
    catalog
        .add_insert_record(table_id, vec![7], 75)
        .await
        .expect("add insert record");
    catalog
        .clear_insert_records(table_id)
        .await
        .expect("clear insert records");
    catalog
        .upsert_snapshot_file(&snapshot_file(table_id, &s6, "f"))
        .await
        .expect("upsert snapshot file");
    catalog
        .clear_snapshot_files(table_id)
        .await
        .expect("clear snapshot files");
    catalog
        .upsert_snapshot_file_statistics(&file_statistics(table_id, &s6, "f"))
        .await
        .expect("upsert file statistics");
    catalog
        .clear_snapshot_file_statistics(table_id)
        .await
        .expect("clear file statistics");
    catalog
        .add_inlined_data(inlined_data(table_id, "", 8, 76))
        .await
        .expect("add inlined data to clear");
    catalog
        .commit_on_conflict_deletions_and_clear_inlined(
            vec![delete_file(
                table_id,
                "del/j.arrow",
                77,
                DeletionType::KeyBased,
            )],
            table_id,
            vec![vec![8]],
            78,
            None,
        )
        .await
        .expect("on-conflict publish clearing inlined rows");
    catalog
        .increment_sequence_number(table_id)
        .await
        .expect("increment sequence");

    let dropped = catalog
        .create_table(options(harness, "dropped", VortexConfig::default()))
        .await
        .expect("create a table to drop");
    catalog
        .upsert_table_statistics(&table_statistics(&dropped, 1))
        .await
        .expect("statistics of the table to drop");
    assert!(catalog.drop_table("dropped").await.expect("drop table"));
    // Leave rows the comparison can see.
    catalog
        .upsert_table_statistics(&table_statistics(table_id, 9))
        .await
        .expect("final statistics");
}

#[tokio::test]
async fn every_catalog_action_replays_to_the_catalog_that_recorded_it() {
    let harness = Harness::new();

    // Phase one on node-a, then a new node rebuilt from the first image and
    // every transaction after it.
    let a = harness.catalog("node-a").await;
    let table_id = phase_one(&a, &harness).await;
    a.shutdown().await.expect("shutdown node-a");
    let mut kinds = harness.action_kinds_after(0).await;
    assert_eq!(harness.images().await, 1, "phase one must not rotate");
    let expected = dump(&harness.replica("node-a"));
    drop(a);
    let b = harness.catalog("node-b").await;
    assert_same_catalog(&dump(&harness.replica("node-b")), &expected, "phase one");

    // Phase two on node-b, which rotated to an image of what it restored; a
    // third node replays phase two onto that image.
    let images = harness.images().await;
    let position = super::read_position(&harness.replica("node-b"))
        .expect("read node-b position")
        .expect("node-b position")
        .sequence;
    phase_two(&b, &harness, &table_id).await;
    b.shutdown().await.expect("shutdown node-b");
    kinds.extend(harness.action_kinds_after(position).await);
    assert_eq!(harness.images().await, images, "phase two must not rotate");
    let expected = dump(&harness.replica("node-b"));
    drop(b);
    let c = harness.catalog("node-c").await;
    assert_same_catalog(&dump(&harness.replica("node-c")), &expected, "phase two");
    c.shutdown().await.expect("shutdown node-c");

    assert_eq!(
        kinds.len(),
        ACTION_KINDS,
        "every action kind must be replayed; replayed {kinds:?}"
    );
}
