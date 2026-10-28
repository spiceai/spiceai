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

//! Sizes the design cares about, computed from states rather than from the
//! SQL-shaped record.
//!
//! The SQL metastore copies every carried-over file row into each new
//! snapshot, so a record mirroring its statements over-counts: the design's
//! live-file collection has no snapshot dimension, and a file that stays live
//! needs no action. [`design_record`] therefore derives the record the design
//! would write from the states before and after a commit: files that became
//! live, files that left, files that moved group, and every other collection's
//! change.

use prost::Message;

use crate::proto::{
    self, Action, AddDeleteFiles, AddFiles, AddKeys, ColdEntry, DvEntry, FileEntry, ObjRef,
    PkSeq, Record, RemoveBySnapshot, RemoveDeleteFiles, Select, SetGroup,
    SetTableStats, TableRef, action::Kind,
};
use crate::state::TableState;

fn payload_of(len: u64) -> Vec<u8> {
    vec![0_u8; usize::try_from(len).unwrap_or(0)]
}

/// The record the design would commit for one table, given its state before
/// and after the SQL transaction.
#[must_use]
#[expect(clippy::too_many_lines, reason = "one block per collection")]
pub fn design_record(table_id: &str, before: Option<&TableState>, after: Option<&TableState>) -> Record {
    let empty = TableState::default();
    let old = before.unwrap_or(&empty);
    let new = after.unwrap_or(&empty);
    let mut actions = Vec::new();
    let t = || table_id.to_string();

    if before.is_none() && after.is_some() {
        actions.push(Action::new(Kind::CreateTable(proto::CreateTable {
            table_id: t(),
            name: new.name.clone(),
            path: new.path.clone(),
            path_is_relative: new.path_is_relative,
            schema: new.schema.clone(),
            primary_key_json: new.primary_key_json.clone(),
            on_conflict_json: new.on_conflict_json.clone(),
            partition_column: new.partition_column.clone(),
            vortex_config_json: new.vortex_config_json.clone(),
            current_snapshot_id: String::new(),
            seq: new.seq,
        })));
    }
    if before.is_some() && after.is_none() {
        actions.push(Action::new(Kind::DropTable(TableRef { table_id: t() })));
        return Record { actions };
    }
    if before.is_some() && old.schema != new.schema {
        actions.push(Action::new(Kind::SetSchema(proto::SetSchema {
            table_id: t(),
            schema: new.schema.clone(),
        })));
    }

    // Live files: the design's collection, keyed by path, with its group.
    let old_live = old.live_files();
    let new_live = new.live_files();
    let mut added: Vec<FileEntry> = Vec::new();
    let mut removed: Vec<String> = Vec::new();
    let mut regrouped = 0_usize;
    for (path, (snapshot, entry)) in &new_live {
        match old_live.get(path) {
            None => added.push((*entry).clone()),
            Some((old_snapshot, old_entry)) => {
                let old_group = old.groups.contains_key(*old_snapshot).then_some(*old_snapshot);
                let new_group = new.groups.contains_key(*snapshot).then_some(*snapshot);
                if old_entry != entry || old_group != new_group {
                    regrouped += 1;
                }
            }
        }
    }
    for path in old_live.keys() {
        if !new_live.contains_key(path) {
            removed.push((*path).to_string());
        }
    }
    if !added.is_empty() {
        actions.push(Action::new(Kind::AddFiles(AddFiles {
            table_id: t(),
            snapshot_id: String::new(),
            files: added,
            upsert: false,
        })));
    }
    if !removed.is_empty() || regrouped > 0 {
        // A removal names the file; a regroup names the file and its group.
        let mut names = removed;
        names.extend(std::iter::repeat_n(String::from("x".repeat(48)), regrouped));
        actions.push(Action::new(Kind::RemoveFiles(RemoveBySnapshot {
            table_id: t(),
            select: Select::Listed as i32,
            snapshot_ids: names,
        })));
    }

    for (group, fence) in &new.groups {
        if old.groups.get(group) != Some(fence) {
            actions.push(Action::new(Kind::SetGroup(SetGroup {
                table_id: t(),
                snapshot_id: group.clone(),
                seq: *fence,
            })));
        }
    }
    let dropped_groups: Vec<String> = old
        .groups
        .keys()
        .filter(|g| !new.groups.contains_key(*g))
        .cloned()
        .collect();
    if !dropped_groups.is_empty() {
        actions.push(Action::new(Kind::RemoveGroups(proto::RemoveGroups {
            table_id: t(),
            select: Select::Listed as i32,
            snapshot_ids: dropped_groups,
            require_present: true,
        })));
    }

    let added_dvs: Vec<DvEntry> = new
        .delete_files
        .iter()
        .filter(|(id, _)| !old.delete_files.contains_key(*id))
        .map(|(_, dv)| dv.clone())
        .collect();
    if !added_dvs.is_empty() {
        actions.push(Action::new(Kind::AddDeleteFiles(AddDeleteFiles {
            table_id: t(),
            files: added_dvs,
        })));
    }
    let removed_dvs: Vec<String> = old
        .delete_files
        .keys()
        .filter(|id| !new.delete_files.contains_key(*id))
        .cloned()
        .collect();
    if !removed_dvs.is_empty() {
        actions.push(Action::new(Kind::RemoveDeleteFiles(RemoveDeleteFiles {
            table_id: t(),
            select: Select::Listed as i32,
            ids: removed_dvs,
            seq_le: None,
        })));
    }

    if old.stats != new.stats {
        match &new.stats {
            Some(stats) => actions.push(Action::new(Kind::SetTableStats(SetTableStats {
                table_id: t(),
                blob: Some(stats.blob.clone()),
                num_rows: stats.num_rows,
                ndv: stats.ndv.clone(),
                exact: stats.exact,
            }))),
            None => actions.push(Action::new(Kind::ClearTableStats(TableRef { table_id: t() }))),
        }
    }

    if old.cold != new.cold {
        let files: Vec<ColdEntry> = new
            .cold
            .iter()
            .filter(|(url, entry)| old.cold.get(*url) != Some(*entry))
            .map(|(_, entry)| entry.clone())
            .collect();
        if new.cold.len() < old.cold.len() || files.len() == new.cold.len() {
            actions.push(Action::new(Kind::ClearColdFiles(TableRef { table_id: t() })));
        }
        actions.push(Action::new(Kind::AddColdFiles(proto::AddColdFiles {
            table_id: t(),
            files,
        })));
    }

    // The PK index lives in checkpoints, not in records.
    if old.pk_index.is_some() && new.pk_index.is_none() {
        actions.push(Action::new(Kind::ClearPkIndex(TableRef { table_id: t() })));
    }

    for (id, row) in &new.inlined_data {
        if old.inlined_data.get(id) != Some(row) {
            actions.push(Action::new(Kind::AddInlinedData(proto::InlinedData {
                table_id: t(),
                id: id.clone(),
                partition_key: row.partition_key.clone(),
                payload: payload_of(row.payload.len),
                count: row.count,
                seq: row.seq,
            })));
        }
    }
    let gone_inline: Vec<String> = old
        .inlined_data
        .keys()
        .filter(|id| !new.inlined_data.contains_key(*id))
        .cloned()
        .collect();
    if !gone_inline.is_empty() {
        actions.push(Action::new(Kind::RemoveInlinedData(proto::RemoveInlined {
            table_id: t(),
            select: Select::Listed as i32,
            ids: gone_inline,
        })));
    }
    for (id, row) in &new.inlined_deletes {
        if old.inlined_deletes.get(id) != Some(row) {
            actions.push(Action::new(Kind::AddInlinedDelete(proto::InlinedDelete {
                table_id: t(),
                id: id.clone(),
                payload: payload_of(row.payload.len),
                count: row.count,
                seq: row.seq,
                published: row.published,
                idempotent: false,
            })));
        }
    }
    let gone_tombstones: Vec<String> = old
        .inlined_deletes
        .keys()
        .filter(|id| !new.inlined_deletes.contains_key(*id))
        .cloned()
        .collect();
    if !gone_tombstones.is_empty() {
        actions.push(Action::new(Kind::RemoveInlinedDeletes(proto::RemoveInlined {
            table_id: t(),
            select: Select::Listed as i32,
            ids: gone_tombstones,
        })));
    }

    let keys = |map: &std::collections::BTreeMap<Vec<u8>, i64>,
                other: &std::collections::BTreeMap<Vec<u8>, i64>| {
        map.iter()
            .filter(|(pk, seq)| other.get(*pk) != Some(*seq))
            .map(|(pk, seq)| PkSeq {
                pk: pk.clone(),
                seq: *seq,
            })
            .collect::<Vec<_>>()
    };
    let dirty = keys(&new.pending_write_back, &old.pending_write_back);
    if !dirty.is_empty() {
        actions.push(Action::new(Kind::MarkDirty(AddKeys {
            table_id: t(),
            keys: dirty,
        })));
    }
    let delivered = keys(&old.pending_write_back, &new.pending_write_back);
    if !delivered.is_empty() {
        actions.push(Action::new(Kind::ClearDelivered(AddKeys {
            table_id: t(),
            keys: delivered,
        })));
    }
    let reinserted = keys(&new.insert_records, &old.insert_records);
    if !reinserted.is_empty() {
        actions.push(Action::new(Kind::AddInsertRecords(AddKeys {
            table_id: t(),
            keys: reinserted,
        })));
    }
    if new.insert_records.len() < old.insert_records.len() {
        actions.push(Action::new(Kind::ClearInsertRecords(proto::ClearInsertRecords {
            table_id: t(),
            seq_le: Some(0),
        })));
    }
    // The `seq` high-water mark is maintained by `apply` from record ranges in
    // the design; a per-table range costs about this much.
    if old.seq != new.seq {
        actions.push(Action::new(Kind::AdvanceSeq(proto::AdvanceSeq {
            table_id: t(),
            to: new.seq,
        })));
    }
    Record { actions }
}

/// The encoded size of a table's checkpoint under the design, split into the
/// part without the PK index and the PK index's own bytes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CheckpointSize {
    pub without_pk_index: u64,
    pub pk_index: u64,
    pub live_files: u64,
    pub delete_files: u64,
    pub inline_bytes: u64,
}

/// Estimates the checkpoint of one table: the encoded live-file, deletion-vector,
/// group, cold, inline and marker collections, with large objects counted at
/// their own length (statistics) or apart (the PK index).
#[must_use]
pub fn checkpoint_size(table: &TableState) -> CheckpointSize {
    let files: Vec<FileEntry> = table.live_files().values().map(|(_, e)| (*e).clone()).collect();
    let live = AddFiles {
        table_id: String::new(),
        snapshot_id: String::new(),
        files,
        upsert: false,
    };
    let dvs = AddDeleteFiles {
        table_id: String::new(),
        files: table.delete_files.values().cloned().collect(),
    };
    let cold = proto::AddColdFiles {
        table_id: String::new(),
        files: table.cold.values().cloned().collect(),
    };
    let markers = AddKeys {
        table_id: String::new(),
        keys: table
            .pending_write_back
            .iter()
            .chain(table.insert_records.iter())
            .map(|(pk, seq)| PkSeq {
                pk: pk.clone(),
                seq: *seq,
            })
            .collect(),
    };
    let inline_bytes: u64 = table
        .inlined_data
        .values()
        .map(|row| row.payload.len)
        .chain(table.inlined_deletes.values().map(|row| row.payload.len))
        .sum();
    let refs = ObjRef::default().encoded_len() as u64 + 34;
    let fixed = table.groups.len() as u64 * 48 + refs * 3 + 256;
    let live_files = live.encoded_len() as u64;
    let delete_files = dvs.encoded_len() as u64;
    let pk_index = table.pk_index.as_ref().map_or(0, |(_, index)| index.len);
    let stats = table.stats.as_ref().map_or(0, |s| s.blob.len + s.ndv.as_ref().map_or(0, |n| n.len));
    CheckpointSize {
        without_pk_index: live_files
            + delete_files
            + cold.encoded_len() as u64
            + markers.encoded_len() as u64
            + inline_bytes
            + stats
            + fixed,
        pk_index,
        live_files,
        delete_files,
        inline_bytes,
    }
}

/// The encoded length of a record.
#[must_use]
pub fn encoded_len(record: &Record) -> usize {
    record.encoded_len()
}
