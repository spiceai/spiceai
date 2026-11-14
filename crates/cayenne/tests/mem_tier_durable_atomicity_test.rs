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

//! Catalog transaction regression guards for mem-tier seals and checkpoints (#16147).
//! A trigger injects failure at the checkpoint's final inline-delete statement;
//! a conflicting vector injects failure after a seal's inline append. These
//! transaction-internal boundaries cannot be driven through a Spicepod.

#![expect(
    clippy::expect_used,
    reason = "test setup and exact catalog assertions"
)]

use arrow::datatypes::{DataType, Field, Schema};
use cayenne::catalog::{MemTierDurableCommit, SnapshotSequenceCommit};
use cayenne::metadata::{CreateTableOptions, DeletionType, VortexConfig};
use cayenne::metastore::ExecuteParams;
use cayenne::{CayenneCatalog, DeleteFile, InlinedData, InlinedDelete, MetadataCatalog};
use std::sync::Arc;
use tempfile::TempDir;

async fn setup() -> (CayenneCatalog, TempDir, String) {
    let tmp = TempDir::new().expect("tempdir");
    let catalog = CayenneCatalog::new(format!(
        "sqlite://{}",
        tmp.path().join("catalog.db").display()
    ))
    .expect("catalog");
    catalog.init().await.expect("init");
    let table_id = catalog
        .create_table(CreateTableOptions {
            table_name: "atomic".to_string(),
            schema: Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
            primary_key: vec![],
            on_conflict: None,
            base_path: tmp.path().join("data").to_string_lossy().into_owned(),
            partition_column: None,
            vortex_config: VortexConfig::default(),
        })
        .await
        .expect("table");
    catalog
        .add_inlined_data(inline(&table_id))
        .await
        .expect("plant inline");
    catalog
        .add_inlined_delete(InlinedDelete {
            inlined_id: String::new(),
            table_id: table_id.clone(),
            delete_ipc: vec![9],
            delete_count: 1,
            sequence_number: 2,
            created_at: String::new(),
            published: true,
        })
        .await
        .expect("plant tombstone");
    catalog
        .add_delete_file(vector(&table_id, "old", 1))
        .await
        .expect("plant vector");
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

async fn state(catalog: &CayenneCatalog, table_id: &str) -> String {
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

#[tokio::test]
async fn mem_tier_seal_atomicity() {
    let (catalog, _tmp, table_id) = setup().await;
    let before = state(&catalog, &table_id).await;
    // Same vector path, different sequence: the SQL idempotency guard fails
    // after staging the shadow insert, rather than rejecting input up front.
    let error = catalog
        .commit_mem_tier_durable(MemTierDurableCommit {
            table_id: table_id.clone(),
            inline_data: vec![inline(&table_id)],
            pure_delete_files: vec![vector(&table_id, "pure", 3)],
            reinsert_delete_files: vec![vector(&table_id, "old", 4)],
            insert_pk_bytes: vec![vec![7]],
            sequence_number: 10,
            ..Default::default()
        })
        .await
        .expect_err("conflicting vector must fail");
    assert_eq!(state(&catalog, &table_id).await, before);
    assert!(
        error.to_string().contains("NOT NULL constraint failed"),
        "{error}"
    );

    catalog
        .commit_mem_tier_durable(MemTierDurableCommit {
            table_id: table_id.clone(),
            inline_data: vec![inline(&table_id)],
            pure_delete_files: vec![vector(&table_id, "pure", 3)],
            reinsert_delete_files: vec![vector(&table_id, "reinsert", 4)],
            insert_pk_bytes: vec![vec![7]],
            sequence_number: 10,
            ..Default::default()
        })
        .await
        .expect("seal");
    let rows = catalog.get_inlined_data(&table_id).await.expect("inline");
    assert_eq!(rows.len(), 2);
    let shadow = rows
        .iter()
        .find(|row| row.sequence_number == 10)
        .expect("shadow stamp");
    assert_eq!(shadow.data_ipc, vec![1, 2, 3]);
    assert_eq!(shadow.record_count, 1);
    assert_vectors(&catalog, &table_id, true).await;
    assert_eq!(
        catalog
            .get_inlined_deletes(&table_id)
            .await
            .expect("tombstones")
            .len(),
        1
    );
    assert_eq!(
        catalog
            .get_all_snapshot_sequences(&table_id)
            .await
            .expect("snapshots")
            .len(),
        0
    );
}

async fn assert_vectors(catalog: &CayenneCatalog, table_id: &str, reinsert: bool) {
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
    let make_commit = || MemTierDurableCommit {
        table_id: table_id.clone(),
        pure_delete_files: vec![vector(&table_id, "pure", 3)],
        reinsert_delete_files: if data_bearing {
            vec![vector(&table_id, "reinsert", 4)]
        } else {
            vec![]
        },
        insert_pk_bytes: if data_bearing { vec![vec![7]] } else { vec![] },
        sequence_number: 10,
        snapshot_sequence: data_bearing.then(|| SnapshotSequenceCommit {
            snapshot_id: snapshot_id.clone(),
            sequence_number: 10,
        }),
        clear_inlined: true,
        ..Default::default()
    };
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
        .commit_mem_tier_durable(make_commit())
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
        .commit_mem_tier_durable(make_commit())
        .await
        .expect("checkpoint");
    assert_vectors(&catalog, &table_id, data_bearing).await;
    assert_eq!(
        catalog
            .get_inlined_data(&table_id)
            .await
            .expect("inline")
            .len(),
        0
    );
    assert_eq!(
        catalog
            .get_inlined_deletes(&table_id)
            .await
            .expect("tombstones")
            .len(),
        0
    );
    assert_eq!(
        catalog
            .get_snapshot_sequence(&table_id, &snapshot_id)
            .await
            .expect("snapshot"),
        data_bearing.then_some(10)
    );
}
