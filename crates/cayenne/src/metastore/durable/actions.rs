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

//! Building [`wire::Action`]s from the catalog's own types.
//!
//! An action carries every value its catalog write uses, including the ones
//! the write chooses itself: a new identifier is minted here, and a creation
//! time is read from the clock here, so applying the action again reproduces
//! the write exactly.

use bytes::Bytes;

use super::wire;
use crate::metadata::{
    ColdTierFile, DeleteFile, DeletionType, IndexRunRecord, InlinedData, InlinedDelete,
    PartitionMetadata, SnapshotFile, SnapshotFileStatistics, TableStatistics,
};

macro_rules! action_from {
    ($($variant:ident),* $(,)?) => {
        $(
            impl From<wire::$variant> for wire::Action {
                fn from(action: wire::$variant) -> Self {
                    Self {
                        action: Some(wire::action::Action::$variant(action)),
                    }
                }
            }
        )*
    };
}

action_from!(
    CreateTable,
    UpdateTableSchema,
    UpdateTableSettings,
    DropTable,
    SetCurrentSnapshot,
    SetCurrentSnapshots,
    ReserveSequenceNumbers,
    SetSnapshotSequence,
    ClearSnapshotSequence,
    AddDeleteFile,
    AddDeleteFiles,
    RemoveDeleteFiles,
    ClearDeleteFiles,
    CommitDeleteFilesWithInlinedRewrite,
    AddInsertRecords,
    ClearInsertRecords,
    CommitOnConflictDeletions,
    CommitOnConflictDeletionsWithTombstone,
    ApplyPreparedOnConflict,
    CommitCompaction,
    CommitCompactionFenced,
    SwapProtectedSnapshots,
    CommitOverwrite,
    CommitOverwriteToCold,
    AddPartition,
    UpsertTableStatistics,
    ClearTableStatistics,
    UpsertSnapshotFileStatistics,
    ClearSnapshotFileStatistics,
    ClearSnapshotCachedMetadata,
    UpsertSnapshotFile,
    ReplaceSnapshotFiles,
    ClearSnapshotFiles,
    UpsertPkIndex,
    ClearPkIndex,
    RegisterIndexRun,
    RemoveIndexRun,
    AddInlinedData,
    ClearInlinedData,
    ClearInlinedDeletes,
    AddInlinedDelete,
    SetInlinedDeletePublished,
    PublishOrphanInlinedDeletes,
    CommitInlinedMutation,
    MarkDirtyKeys,
    ClearPendingWriteBack,
);

/// The current time in the form `SQLite`'s `strftime('%Y-%m-%dT%H:%M:%fZ')`
/// writes, the form the catalog stores creation times in.
pub(crate) fn now_timestamp() -> String {
    chrono::Utc::now()
        .format("%Y-%m-%dT%H:%M:%S%.3fZ")
        .to_string()
}

/// A new catalog identifier.
pub(crate) fn new_id() -> String {
    uuid::Uuid::now_v7().to_string()
}

/// `file` under a newly minted identifier: the catalog assigns delete-file
/// identifiers itself.
pub(crate) fn delete_file(file: &DeleteFile) -> wire::DeleteFile {
    wire::DeleteFile {
        delete_file_id: new_id(),
        table_id: file.table_id.clone(),
        source_data_file_path: file.source_data_file_path.clone(),
        path: file.path.clone(),
        path_is_relative: file.path_is_relative,
        format: file.format.clone(),
        delete_count: file.delete_count,
        file_size_bytes: file.file_size_bytes,
        deletion_type: match file.deletion_type {
            DeletionType::PositionBased => wire::DeletionType::PositionBased,
            DeletionType::KeyBased => wire::DeletionType::KeyBased,
        }
        .into(),
        sequence_number: file.sequence_number,
        reinsert_sequence: file.reinsert_sequence,
    }
}

pub(crate) fn delete_files(files: &[DeleteFile]) -> Vec<wire::DeleteFile> {
    files.iter().map(delete_file).collect()
}

pub(crate) fn snapshot_file(file: &SnapshotFile) -> wire::SnapshotFile {
    wire::SnapshotFile {
        table_id: file.table_id.clone(),
        snapshot_id: file.snapshot_id.clone(),
        file_path: file.file_path.clone(),
        row_count: file.row_count,
        file_size_bytes: file.file_size_bytes,
        min_sequence: file.min_sequence,
        max_sequence: file.max_sequence,
        digest: file.digest.clone(),
    }
}

pub(crate) fn snapshot_file_statistics(
    statistics: &SnapshotFileStatistics,
) -> wire::SnapshotFileStatistics {
    wire::SnapshotFileStatistics {
        table_id: statistics.table_id.clone(),
        snapshot_id: statistics.snapshot_id.clone(),
        file_path: statistics.file_path.clone(),
        file_size_bytes: statistics.file_size_bytes,
        num_rows: statistics.num_rows,
        statistics_blob: Bytes::copy_from_slice(&statistics.statistics_blob),
    }
}

pub(crate) fn table_statistics(statistics: &TableStatistics) -> wire::TableStatistics {
    wire::TableStatistics {
        table_id: statistics.table_id.clone(),
        statistics_blob: Bytes::copy_from_slice(&statistics.statistics_blob),
        num_rows: statistics.num_rows,
        num_rows_exact: statistics.num_rows_exact,
        ndv_sketches: statistics
            .ndv_sketches
            .as_deref()
            .map(Bytes::copy_from_slice),
    }
}

pub(crate) fn cold_tier_file(file: &ColdTierFile) -> wire::ColdTierFile {
    wire::ColdTierFile {
        table_id: file.table_id.clone(),
        file_url: file.file_url.clone(),
        row_count: file.row_count,
        file_size_bytes: file.file_size_bytes,
        min_sequence: file.min_sequence,
        max_sequence: file.max_sequence,
        statistics_blob: Bytes::copy_from_slice(&file.statistics_blob),
        pk_bloom: file.pk_bloom.as_deref().map(Bytes::copy_from_slice),
    }
}

/// `partition` under a newly minted identifier.
pub(crate) fn partition(partition: &PartitionMetadata) -> wire::Partition {
    wire::Partition {
        partition_id: new_id(),
        table_id: partition.table_id.clone(),
        partition_columns: partition.partition_columns.clone(),
        partition_values: partition.partition_values.clone(),
        path: partition.path.clone(),
        path_is_relative: partition.path_is_relative,
        record_count: partition.record_count,
        file_size_bytes: partition.file_size_bytes,
    }
}

pub(crate) fn index_run(run: &IndexRunRecord) -> wire::IndexRun {
    wire::IndexRun {
        table_id: run.table_id.clone(),
        index_key: run.index_key.clone(),
        run_name: run.run_name.clone(),
        row_count: run.row_count,
        size_bytes: run.size_bytes,
    }
}

/// `data` as `table_id`'s batch at `sequence_number`, created now, under its
/// own identifier or, when it has none, a newly minted one.
pub(crate) fn inlined_data(
    data: &InlinedData,
    table_id: &str,
    sequence_number: i64,
) -> wire::InlinedData {
    wire::InlinedData {
        inlined_id: if data.inlined_id.is_empty() {
            new_id()
        } else {
            data.inlined_id.clone()
        },
        table_id: table_id.to_string(),
        partition_key: data.partition_key.clone(),
        data_ipc: Bytes::copy_from_slice(&data.data_ipc),
        record_count: data.record_count,
        sequence_number,
        created_at: now_timestamp(),
    }
}

pub(crate) fn inlined_data_update(data: &InlinedData) -> wire::InlinedDataUpdate {
    wire::InlinedDataUpdate {
        inlined_id: data.inlined_id.clone(),
        data_ipc: Bytes::copy_from_slice(&data.data_ipc),
        record_count: data.record_count,
    }
}

/// `delete`, created now, under its own identifier or, when it has none, a
/// newly minted one.
pub(crate) fn inlined_delete(delete: &InlinedDelete) -> wire::InlinedDelete {
    wire::InlinedDelete {
        inlined_id: if delete.inlined_id.is_empty() {
            new_id()
        } else {
            delete.inlined_id.clone()
        },
        table_id: delete.table_id.clone(),
        delete_ipc: Bytes::copy_from_slice(&delete.delete_ipc),
        delete_count: delete.delete_count,
        sequence_number: delete.sequence_number,
        created_at: now_timestamp(),
        published: delete.published,
    }
}

/// Whether `file` is a key-based deletion vector.
pub(crate) fn is_key_based(file: &wire::DeleteFile) -> bool {
    file.deletion_type() == wire::DeletionType::KeyBased
}

/// The catalog's own form of a delete file an action carries.
pub(crate) fn delete_file_from_wire(file: &wire::DeleteFile) -> DeleteFile {
    DeleteFile {
        delete_file_id: file.delete_file_id.clone(),
        table_id: file.table_id.clone(),
        source_data_file_path: file.source_data_file_path.clone(),
        path: file.path.clone(),
        path_is_relative: file.path_is_relative,
        format: file.format.clone(),
        delete_count: file.delete_count,
        file_size_bytes: file.file_size_bytes,
        deletion_type: if is_key_based(file) {
            DeletionType::KeyBased
        } else {
            DeletionType::PositionBased
        },
        sequence_number: file.sequence_number,
        reinsert_sequence: file.reinsert_sequence,
    }
}

/// The catalog's own form of a partition an action carries.
pub(crate) fn partition_from_wire(partition: &wire::Partition) -> PartitionMetadata {
    PartitionMetadata {
        partition_id: partition.partition_id.clone(),
        table_id: partition.table_id.clone(),
        partition_columns: partition.partition_columns.clone(),
        partition_values: partition.partition_values.clone(),
        path: partition.path.clone(),
        path_is_relative: partition.path_is_relative,
        record_count: partition.record_count,
        file_size_bytes: partition.file_size_bytes,
    }
}
