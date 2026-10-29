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

//! Builds storage-independent catalog actions from Cayenne metadata types.

use cayenne_metastore_actions::proto::{
    self, Action, ColdEntry, DvEntry, FileEntry, FileStat, Select, TableRef, action::Kind,
};

use crate::metadata::{
    ColdTierFile, DeleteFile, InlinedData, SnapshotFile, SnapshotFileStatistics,
};

pub(crate) fn table(table_id: &str) -> TableRef {
    TableRef {
        table_id: table_id.to_string(),
    }
}

pub(crate) fn add_files(
    table_id: &str,
    snapshot_id: &str,
    files: &[SnapshotFile],
    upsert: bool,
) -> Action {
    Action::new(Kind::AddFiles(proto::AddFiles {
        table_id: table_id.to_string(),
        snapshot_id: snapshot_id.to_string(),
        files: files
            .iter()
            .map(|f| FileEntry {
                path: f.file_path.clone(),
                rows: f.row_count,
                size: f.file_size_bytes,
                min_seq: f.min_sequence,
                max_seq: f.max_sequence,
                digest: f.digest.clone(),
            })
            .collect(),
        upsert,
    }))
}

/// One action per file, for upserts that name their snapshot per file.
pub(crate) fn upsert_files(files: &[SnapshotFile]) -> Vec<Action> {
    files
        .iter()
        .map(|f| add_files(&f.table_id, &f.snapshot_id, std::slice::from_ref(f), true))
        .collect()
}

pub(crate) fn remove_files(table_id: &str, select: Select, snapshot_ids: &[String]) -> Action {
    Action::new(Kind::RemoveFiles(proto::RemoveBySnapshot {
        table_id: table_id.to_string(),
        select: select as i32,
        snapshot_ids: snapshot_ids.to_vec(),
    }))
}

pub(crate) fn remove_file_stats(table_id: &str, select: Select, snapshot_ids: &[String]) -> Action {
    Action::new(Kind::RemoveFileStats(proto::RemoveBySnapshot {
        table_id: table_id.to_string(),
        select: select as i32,
        snapshot_ids: snapshot_ids.to_vec(),
    }))
}

pub(crate) fn set_file_stats(
    blob: impl Fn(&[u8]) -> proto::ObjRef,
    stats: &SnapshotFileStatistics,
) -> Action {
    Action::new(Kind::SetFileStats(proto::SetFileStats {
        table_id: stats.table_id.clone(),
        stat: Some(FileStat {
            snapshot_id: stats.snapshot_id.clone(),
            path: stats.file_path.clone(),
            size: stats.file_size_bytes,
            rows: stats.num_rows,
            blob: Some(blob(&stats.statistics_blob)),
        }),
    }))
}

pub(crate) fn set_group(table_id: &str, snapshot_id: &str, seq: i64) -> Action {
    Action::new(Kind::SetGroup(proto::SetGroup {
        table_id: table_id.to_string(),
        snapshot_id: snapshot_id.to_string(),
        seq,
    }))
}

pub(crate) fn remove_groups(
    table_id: &str,
    select: Select,
    ids: &[String],
    require_present: bool,
) -> Action {
    Action::new(Kind::RemoveGroups(proto::RemoveGroups {
        table_id: table_id.to_string(),
        select: select as i32,
        snapshot_ids: ids.to_vec(),
        require_present,
    }))
}

pub(crate) fn dv(file: &DeleteFile) -> DvEntry {
    DvEntry {
        id: file.delete_file_id.clone(),
        path: file.path.clone(),
        relative: file.path_is_relative,
        format: file.format.clone(),
        count: file.delete_count,
        size: file.file_size_bytes,
        source: file.source_data_file_path.clone(),
        seq: file.sequence_number,
        reinsert: file.reinsert_sequence,
    }
}

pub(crate) fn add_delete_files(table_id: &str, files: &[DeleteFile]) -> Action {
    Action::new(Kind::AddDeleteFiles(proto::AddDeleteFiles {
        table_id: table_id.to_string(),
        files: files.iter().map(dv).collect(),
    }))
}

pub(crate) fn remove_delete_files(
    table_id: &str,
    select: Select,
    ids: &[String],
    seq_le: Option<i64>,
) -> Action {
    Action::new(Kind::RemoveDeleteFiles(proto::RemoveDeleteFiles {
        table_id: table_id.to_string(),
        select: select as i32,
        ids: ids.to_vec(),
        seq_le,
    }))
}

pub(crate) fn clear_insert_records(table_id: &str, seq_le: Option<i64>) -> Action {
    Action::new(Kind::ClearInsertRecords(proto::ClearInsertRecords {
        table_id: table_id.to_string(),
        seq_le,
    }))
}

pub(crate) fn set_current(
    table_id: &str,
    snapshot_id: &str,
    expected_previous: Option<&str>,
) -> Action {
    Action::new(Kind::SetCurrentSnapshot(proto::SetCurrentSnapshot {
        table_id: table_id.to_string(),
        snapshot_id: snapshot_id.to_string(),
        expected_previous: expected_previous.map(str::to_string),
    }))
}

pub(crate) fn clear_table_stats(table_id: &str) -> Action {
    Action::new(Kind::ClearTableStats(table(table_id)))
}

pub(crate) fn add_cold(
    blob: impl Fn(&[u8]) -> proto::ObjRef,
    table_id: &str,
    files: &[ColdTierFile],
) -> Action {
    Action::new(Kind::AddColdFiles(proto::AddColdFiles {
        table_id: table_id.to_string(),
        files: files
            .iter()
            .map(|f| ColdEntry {
                url: f.file_url.clone(),
                rows: f.row_count,
                size: f.file_size_bytes,
                min_seq: f.min_sequence,
                max_seq: f.max_sequence,
                stats: Some(blob(&f.statistics_blob)),
                bloom: f.pk_bloom.as_deref().map(&blob),
            })
            .collect(),
    }))
}

pub(crate) fn add_inlined(table_id: &str, data: &InlinedData, id: &str, seq: i64) -> Action {
    Action::new(Kind::AddInlinedData(proto::InlinedData {
        table_id: table_id.to_string(),
        id: id.to_string(),
        partition_key: data.partition_key.clone(),
        payload: data.data_ipc.clone(),
        count: data.record_count,
        seq,
        created_at: String::new(),
    }))
}

pub(crate) fn remove_inlined(table_id: &str, select: Select, ids: &[String]) -> Action {
    Action::new(Kind::RemoveInlinedData(proto::RemoveInlined {
        table_id: table_id.to_string(),
        select: select as i32,
        ids: ids.to_vec(),
    }))
}

pub(crate) fn remove_inlined_deletes(table_id: &str, select: Select, ids: &[String]) -> Action {
    Action::new(Kind::RemoveInlinedDeletes(proto::RemoveInlined {
        table_id: table_id.to_string(),
        select: select as i32,
        ids: ids.to_vec(),
    }))
}

pub(crate) fn set_published(
    table_id: &str,
    select: Select,
    ids: &[String],
    published: bool,
) -> Action {
    Action::new(Kind::SetInlinedDeletesPublished(
        proto::SetInlinedDeletesPublished {
            table_id: table_id.to_string(),
            select: select as i32,
            ids: ids.to_vec(),
            published,
        },
    ))
}

/// The actions of `commit_overwrite_in_txn`'s clearing batch, in its order.
pub(crate) fn overwrite_clear(table_id: &str, new_snapshot_id: &str) -> Vec<Action> {
    vec![
        remove_delete_files(table_id, Select::All, &[], None),
        clear_insert_records(table_id, None),
        remove_groups(table_id, Select::All, &[], false),
        remove_inlined(table_id, Select::All, &[]),
        remove_inlined_deletes(table_id, Select::All, &[]),
        clear_table_stats(table_id),
        remove_file_stats(table_id, Select::All, &[]),
        Action::new(Kind::ClearPkIndex(table(table_id))),
        Action::new(Kind::ClearColdFiles(table(table_id))),
        set_current(table_id, new_snapshot_id, None),
    ]
}

/// The exact inline tombstone payload and id bound by the catalog method.
pub(crate) fn inlined_delete(
    delete: &crate::metadata::InlinedDelete,
    id: &str,
    idempotent: bool,
) -> Action {
    Action::new(Kind::AddInlinedDelete(proto::InlinedDelete {
        created_at: String::new(),
        table_id: delete.table_id.clone(),
        id: id.to_string(),
        payload: delete.delete_ipc.clone(),
        count: delete.delete_count,
        seq: delete.sequence_number,
        published: delete.published,
        idempotent,
    }))
}
