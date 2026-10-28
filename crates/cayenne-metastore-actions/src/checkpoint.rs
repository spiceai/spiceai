// Copyright 2026 The Spice.ai OSS Authors
// SPDX-License-Identifier: Apache-2.0

//! Deterministic, replayable table checkpoints.
use crate::TableState;
use crate::proto::{self as p, Action, Record, action::Kind};

/// Actions that rebuild every collection of a table from an empty dataset.
/// Source positions belong to `DatasetState`, not to a table. No catalog path
/// emits `AdvanceSourcePosition`: CDC positions live in `spice_sys_*` tables
/// written outside the tap. Per-table roots therefore carry no positions.
#[must_use]
pub fn state_as_record(s: &TableState, table_id: &str) -> Record {
    let t = || table_id.to_string();
    let mut actions = vec![Action::new(Kind::CreateTable(p::CreateTable {
        table_id: t(),
        name: s.name.clone(),
        path: s.path.clone(),
        path_is_relative: s.path_is_relative,
        schema: s.schema.clone(),
        primary_key_json: s.primary_key_json.clone(),
        on_conflict_json: s.on_conflict_json.clone(),
        partition_column: s.partition_column.clone(),
        vortex_config_json: s.vortex_config_json.clone(),
        current_snapshot_id: s.current_snapshot_id.clone(),
        seq: s.seq,
    }))];
    let mut snapshots = std::collections::BTreeMap::<&str, Vec<p::FileEntry>>::new();
    for ((snapshot, _), file) in &s.files {
        snapshots
            .entry(snapshot.as_str())
            .or_default()
            .push(file.clone());
    }
    for (snapshot, files) in snapshots {
        actions.push(Action::new(Kind::AddFiles(p::AddFiles {
            table_id: t(),
            snapshot_id: snapshot.to_string(),
            files,
            upsert: false,
        })));
    }
    for stat in s.file_stats.values() {
        actions.push(Action::new(Kind::SetFileStats(p::SetFileStats {
            table_id: t(),
            stat: Some(stat.clone()),
        })));
    }
    for (snapshot, seq) in &s.groups {
        actions.push(Action::new(Kind::SetGroup(p::SetGroup {
            table_id: t(),
            snapshot_id: snapshot.clone(),
            seq: *seq,
        })));
    }
    actions.push(Action::new(Kind::AddDeleteFiles(p::AddDeleteFiles {
        table_id: t(),
        files: s.delete_files.values().cloned().collect(),
    })));
    if let Some(stat) = &s.stats {
        actions.push(Action::new(Kind::SetTableStats(p::SetTableStats {
            table_id: t(),
            blob: Some(stat.blob.clone()),
            num_rows: stat.num_rows,
            ndv: stat.ndv.clone(),
            exact: stat.exact,
        })));
    }
    actions.push(Action::new(Kind::AddColdFiles(p::AddColdFiles {
        table_id: t(),
        files: s.cold.values().cloned().collect(),
    })));
    if let Some((snapshot, index)) = &s.pk_index {
        actions.push(Action::new(Kind::SetPkIndex(p::SetPkIndex {
            table_id: t(),
            snapshot_id: snapshot.clone(),
            index: Some(index.clone()),
        })));
    }
    for ((key, name), (rows, size)) in &s.index_runs {
        actions.push(Action::new(Kind::AddIndexRun(p::IndexRun {
            table_id: t(),
            index_key: key.clone(),
            run_name: name.clone(),
            rows: *rows,
            size: *size,
        })));
    }
    for (id, row) in &s.inlined_data {
        actions.push(Action::new(Kind::AddInlinedData(p::InlinedData {
            created_at: row.created_at.clone(),
            table_id: t(),
            id: id.clone(),
            partition_key: row.partition_key.clone(),
            payload: row.raw_payload.clone(),
            count: row.count,
            seq: row.seq,
        })));
    }
    for (id, row) in &s.inlined_deletes {
        actions.push(Action::new(Kind::AddInlinedDelete(p::InlinedDelete {
            created_at: row.created_at.clone(),
            table_id: t(),
            id: id.clone(),
            payload: row.raw_payload.clone(),
            count: row.count,
            seq: row.seq,
            published: row.published,
            idempotent: false,
        })));
    }
    let keys = |m: &std::collections::BTreeMap<Vec<u8>, i64>| {
        m.iter()
            .map(|(pk, seq)| p::PkSeq {
                pk: pk.clone(),
                seq: *seq,
            })
            .collect()
    };
    actions.push(Action::new(Kind::AddInsertRecords(p::AddKeys {
        table_id: t(),
        keys: keys(&s.insert_records),
    })));
    actions.push(Action::new(Kind::MarkDirty(p::AddKeys {
        table_id: t(),
        keys: keys(&s.pending_write_back),
    })));
    for (id, part) in &s.partitions {
        actions.push(Action::new(Kind::AddPartition(p::AddPartition {
            table_id: t(),
            partition_id: id.clone(),
            columns: part.columns.clone(),
            values: part.values.clone(),
            path: part.path.clone(),
            path_is_relative: part.path_is_relative,
            record_count: part.record_count,
            size: part.size,
        })));
    }
    Record { actions }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{Inlined, InlinedDel, Partition, TableStats};
    use crate::{DatasetState, ObjRef};

    #[test]
    fn checkpoint_roundtrip() {
        let mut s = TableState {
            name: "table".into(),
            seq: 19,
            current_snapshot_id: "live".into(),
            schema: Some(ObjRef::of(b"schema")),
            ..TableState::default()
        };
        for snapshot in ["live", "old"] {
            s.files.insert(
                (snapshot.into(), "file".into()),
                p::FileEntry {
                    path: "file".into(),
                    rows: 2,
                    ..Default::default()
                },
            );
            s.file_stats.insert(
                (snapshot.into(), "file".into()),
                p::FileStat {
                    snapshot_id: snapshot.into(),
                    path: "file".into(),
                    blob: Some(ObjRef::of(b"stats")),
                    ..Default::default()
                },
            );
            s.groups.insert(snapshot.into(), 7);
        }
        s.files.insert(
            ("live".into(), "second".into()),
            p::FileEntry {
                path: "second".into(),
                rows: 0,
                ..Default::default()
            },
        );
        s.delete_files.insert(
            "dv".into(),
            p::DvEntry {
                id: "dv".into(),
                path: "delete".into(),
                seq: 9,
                ..Default::default()
            },
        );
        s.cold.insert(
            "url".into(),
            p::ColdEntry {
                url: "url".into(),
                bloom: Some(ObjRef::of(b"")),
                ..Default::default()
            },
        );
        s.stats = Some(TableStats {
            blob: ObjRef::of(b"stats"),
            num_rows: 4,
            ndv: None,
            exact: true,
        });
        s.pk_index = Some(("live".into(), ObjRef::of(b"index")));
        s.index_runs
            .insert(("index".into(), "run".into()), (u64::MAX, 1));
        s.inlined_data.insert(
            "inline".into(),
            Inlined {
                created_at: String::new(),
                partition_key: Some("p".into()),
                payload: ObjRef::of(b"data"),
                raw_payload: b"data".to_vec(),
                count: 1,
                seq: 10,
            },
        );
        s.inlined_deletes.insert(
            "del".into(),
            InlinedDel {
                created_at: String::new(),
                payload: ObjRef::of(b"del"),
                raw_payload: b"del".to_vec(),
                count: 1,
                seq: 11,
                published: true,
            },
        );
        s.insert_records.insert(vec![1], 12);
        s.pending_write_back.insert(vec![2], 13);
        s.partitions.insert(
            "p".into(),
            Partition {
                columns: vec!["id".into()],
                values: vec!["1".into()],
                path: "p".into(),
                path_is_relative: true,
                record_count: 1,
                size: 2,
            },
        );
        let mut rebuilt = DatasetState::default();
        rebuilt
            .apply(&state_as_record(&s, "id"))
            .expect("checkpoint applies");
        assert_eq!(rebuilt.tables["id"], s);
    }
}
