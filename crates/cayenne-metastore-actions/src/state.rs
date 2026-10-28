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

//! The dataset state and the one function that changes it.

use std::collections::{BTreeMap, BTreeSet};

use snafu::{Snafu, ensure};

use crate::proto::{
    Action, ColdEntry, DvEntry, FileEntry, FileStat, ObjRef, Record, Select, action::Kind,
};

/// Why a record could not be applied: a guard failed or an action referred to
/// state that does not exist. Every writer, reader and recovery path applies
/// the same records, so this is a corruption of the log, never a skip.
#[derive(Debug, Snafu, PartialEq, Eq)]
pub enum ApplyError {
    #[snafu(display("action {action} names table {table_id}, which does not exist"))]
    NoTable {
        action: &'static str,
        table_id: String,
    },
    #[snafu(display("action {action} on table {table_id}: guard failed: {guard}"))]
    Guard {
        action: &'static str,
        table_id: String,
        guard: String,
    },
    #[snafu(display("an action has no kind"))]
    EmptyAction,
}

pub type Result<T, E = ApplyError> = std::result::Result<T, E>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableStats {
    pub blob: ObjRef,
    pub num_rows: i64,
    pub ndv: Option<ObjRef>,
    pub exact: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Inlined {
    pub partition_key: Option<String>,
    pub payload: ObjRef,
    /// Original inline bytes, retained so checkpoints can replay the row.
    pub raw_payload: Vec<u8>,
    pub count: i64,
    pub seq: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InlinedDel {
    pub payload: ObjRef,
    /// Original inline bytes, retained so checkpoints can replay the row.
    pub raw_payload: Vec<u8>,
    pub count: i64,
    pub seq: i64,
    pub published: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Partition {
    pub columns: Vec<String>,
    pub values: Vec<String>,
    pub path: String,
    pub path_is_relative: bool,
    pub record_count: i64,
    pub size: i64,
}

/// Everything the SQL metastore holds for one table.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct TableState {
    pub name: String,
    pub path: String,
    pub path_is_relative: bool,
    pub schema: Option<ObjRef>,
    pub primary_key_json: Option<String>,
    pub on_conflict_json: Option<String>,
    pub partition_column: Option<String>,
    pub vortex_config_json: Option<String>,
    pub current_snapshot_id: String,
    pub seq: i64,
    /// Data files by `(snapshot, path)`.
    pub files: BTreeMap<(String, String), FileEntry>,
    /// Per-file statistics by `(snapshot, path)`.
    pub file_stats: BTreeMap<(String, String), FileStat>,
    /// Protected snapshots ("groups") and their delete fences.
    pub groups: BTreeMap<String, i64>,
    /// Deletion vectors by id.
    pub delete_files: BTreeMap<String, DvEntry>,
    pub stats: Option<TableStats>,
    pub cold: BTreeMap<String, ColdEntry>,
    pub pk_index: Option<(String, ObjRef)>,
    pub index_runs: BTreeMap<(String, String), (u64, u64)>,
    pub inlined_data: BTreeMap<String, Inlined>,
    pub inlined_deletes: BTreeMap<String, InlinedDel>,
    pub insert_records: BTreeMap<Vec<u8>, i64>,
    pub pending_write_back: BTreeMap<Vec<u8>, i64>,
    pub partitions: BTreeMap<String, Partition>,
}

impl TableState {
    /// The snapshots a scan reads: the current one and every protected one.
    #[must_use]
    pub fn live_snapshots(&self) -> BTreeSet<&str> {
        let mut live: BTreeSet<&str> = self.groups.keys().map(String::as_str).collect();
        live.insert(self.current_snapshot_id.as_str());
        live
    }

    /// The live data files by path, with the snapshot that holds each. This is
    /// the design's live-file collection.
    #[must_use]
    pub fn live_files(&self) -> BTreeMap<&str, (&str, &FileEntry)> {
        let live = self.live_snapshots();
        self.files
            .iter()
            .filter(|((snapshot, _), _)| live.contains(snapshot.as_str()))
            .map(|((snapshot, path), entry)| (path.as_str(), (snapshot.as_str(), entry)))
            .collect()
    }
}

/// The state of every table one metastore holds, plus source positions.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct DatasetState {
    pub tables: BTreeMap<String, TableState>,
    pub positions: BTreeMap<(String, String), Vec<u8>>,
    /// Tables an unconverted mutation touched: their state here is unknown.
    pub unknown: BTreeSet<String>,
}

fn select(value: i32) -> Select {
    Select::try_from(value).unwrap_or(Select::Listed)
}

fn selects(select_kind: Select, ids: &[String], id: &str) -> bool {
    match select_kind {
        Select::All => true,
        Select::Listed => ids.iter().any(|candidate| candidate == id),
        Select::AllExcept => !ids.iter().any(|candidate| candidate == id),
    }
}

impl DatasetState {
    fn table(&mut self, action: &'static str, table_id: &str) -> Result<&mut TableState> {
        self.tables
            .get_mut(table_id)
            .ok_or_else(|| ApplyError::NoTable {
                action,
                table_id: table_id.to_string(),
            })
    }

    /// Applies every action of `record`, in order, or none of them.
    ///
    /// # Errors
    ///
    /// Returns [`ApplyError`] when a guard fails; the state is then unchanged.
    pub fn apply(&mut self, record: &Record) -> Result<()> {
        let mut next = self.clone();
        for action in &record.actions {
            next.apply_action(action)?;
        }
        *self = next;
        Ok(())
    }

    #[expect(clippy::too_many_lines, reason = "one arm per action")]
    fn apply_action(&mut self, action: &Action) -> Result<()> {
        let name = action.name();
        let kind = action.kind.as_ref().ok_or(ApplyError::EmptyAction)?;
        if let Some(table_id) = action.table_id()
            && self.unknown.contains(table_id)
            && !matches!(kind, Kind::CreateTable(_) | Kind::DropTable(_))
        {
            // Its state is already unknown; nothing here can be checked.
            return Ok(());
        }
        match kind {
            Kind::CreateTable(a) => {
                ensure!(
                    !self.tables.contains_key(&a.table_id)
                        && !self.tables.values().any(|t| t.name == a.name),
                    GuardSnafu {
                        action: name,
                        table_id: a.table_id.clone(),
                        guard: format!("table id and name '{}' are free", a.name),
                    }
                );
                self.unknown.remove(&a.table_id);
                self.tables.insert(
                    a.table_id.clone(),
                    TableState {
                        name: a.name.clone(),
                        path: a.path.clone(),
                        path_is_relative: a.path_is_relative,
                        schema: a.schema.clone(),
                        primary_key_json: a.primary_key_json.clone(),
                        on_conflict_json: a.on_conflict_json.clone(),
                        partition_column: a.partition_column.clone(),
                        vortex_config_json: a.vortex_config_json.clone(),
                        current_snapshot_id: a.current_snapshot_id.clone(),
                        seq: a.seq,
                        ..TableState::default()
                    },
                );
            }
            Kind::DropTable(a) => {
                self.unknown.remove(&a.table_id);
                self.tables.remove(&a.table_id);
            }
            Kind::SetSchema(a) => self.table(name, &a.table_id)?.schema.clone_from(&a.schema),
            Kind::SetConfig(a) => {
                let table = self.table(name, &a.table_id)?;
                table.vortex_config_json.clone_from(&a.vortex_config_json);
                table.on_conflict_json.clone_from(&a.on_conflict_json);
            }
            Kind::SetCurrentSnapshot(a) => {
                let table = self.table(name, &a.table_id)?;
                if let Some(expected) = &a.expected_previous {
                    ensure!(
                        &table.current_snapshot_id == expected,
                        GuardSnafu {
                            action: name,
                            table_id: a.table_id.clone(),
                            guard: format!(
                                "current snapshot is {expected}, found {}",
                                table.current_snapshot_id
                            ),
                        }
                    );
                }
                table.current_snapshot_id.clone_from(&a.snapshot_id);
            }
            Kind::AdvanceSeq(a) => {
                let table = self.table(name, &a.table_id)?;
                ensure!(
                    a.to >= table.seq,
                    GuardSnafu {
                        action: name,
                        table_id: a.table_id.clone(),
                        guard: format!("seq moves forward from {} to {}", table.seq, a.to),
                    }
                );
                table.seq = a.to;
            }
            Kind::AddFiles(a) => {
                let table = self.table(name, &a.table_id)?;
                for file in &a.files {
                    let key = (a.snapshot_id.clone(), file.path.clone());
                    ensure!(
                        a.upsert || !table.files.contains_key(&key),
                        GuardSnafu {
                            action: name,
                            table_id: a.table_id.clone(),
                            guard: format!("file {} of snapshot {} is new", key.1, key.0),
                        }
                    );
                    table.files.insert(key, file.clone());
                }
            }
            Kind::RemoveFiles(a) => {
                let table = self.table(name, &a.table_id)?;
                let kind = select(a.select);
                table
                    .files
                    .retain(|(snapshot, _), _| !selects(kind, &a.snapshot_ids, snapshot));
            }
            Kind::SetFileStats(a) => {
                let table = self.table(name, &a.table_id)?;
                if let Some(stat) = &a.stat {
                    table
                        .file_stats
                        .insert((stat.snapshot_id.clone(), stat.path.clone()), stat.clone());
                }
            }
            Kind::RemoveFileStats(a) => {
                let table = self.table(name, &a.table_id)?;
                let kind = select(a.select);
                table
                    .file_stats
                    .retain(|(snapshot, _), _| !selects(kind, &a.snapshot_ids, snapshot));
            }
            Kind::SetGroup(a) => {
                self.table(name, &a.table_id)?
                    .groups
                    .insert(a.snapshot_id.clone(), a.seq);
            }
            Kind::RemoveGroups(a) => {
                let table = self.table(name, &a.table_id)?;
                if a.require_present {
                    ensure!(
                        a.snapshot_ids
                            .iter()
                            .all(|id| table.groups.contains_key(id)),
                        GuardSnafu {
                            action: name,
                            table_id: a.table_id.clone(),
                            guard: "every replaced protected snapshot is still registered"
                                .to_string(),
                        }
                    );
                }
                let kind = select(a.select);
                table
                    .groups
                    .retain(|snapshot, _| !selects(kind, &a.snapshot_ids, snapshot));
            }
            Kind::AddDeleteFiles(a) => {
                let table = self.table(name, &a.table_id)?;
                for file in &a.files {
                    // Unique by path: an identical re-add is a no-op that keeps
                    // the first id; a different one is refused.
                    if let Some(existing) =
                        table.delete_files.values().find(|dv| dv.path == file.path)
                    {
                        let same = DvEntry {
                            id: existing.id.clone(),
                            ..file.clone()
                        };
                        ensure!(
                            &same == existing,
                            GuardSnafu {
                                action: name,
                                table_id: a.table_id.clone(),
                                guard: format!(
                                    "deletion vector {} matches the one already registered",
                                    file.path
                                ),
                            }
                        );
                        continue;
                    }
                    ensure!(
                        !table.delete_files.contains_key(&file.id),
                        GuardSnafu {
                            action: name,
                            table_id: a.table_id.clone(),
                            guard: format!("deletion vector id {} is new", file.id),
                        }
                    );
                    table.delete_files.insert(file.id.clone(), file.clone());
                }
            }
            Kind::RemoveDeleteFiles(a) => {
                let table = self.table(name, &a.table_id)?;
                let kind = select(a.select);
                table.delete_files.retain(|id, dv| {
                    let chosen =
                        selects(kind, &a.ids, id) && a.seq_le.is_none_or(|seq_le| dv.seq <= seq_le);
                    !chosen
                });
            }
            Kind::SetTableStats(a) => {
                let table = self.table(name, &a.table_id)?;
                table.stats = Some(TableStats {
                    blob: a.blob.clone().unwrap_or_default(),
                    num_rows: a.num_rows,
                    ndv: a.ndv.clone(),
                    exact: a.exact,
                });
            }
            Kind::ClearTableStats(a) => self.table(name, &a.table_id)?.stats = None,
            Kind::AddColdFiles(a) => {
                let table = self.table(name, &a.table_id)?;
                for file in &a.files {
                    table.cold.insert(file.url.clone(), file.clone());
                }
            }
            Kind::ClearColdFiles(a) => self.table(name, &a.table_id)?.cold.clear(),
            Kind::ClearColdStats(a) => {
                for file in self.table(name, &a.table_id)?.cold.values_mut() {
                    file.stats = Some(ObjRef::of(&[]));
                }
            }
            Kind::SetPkIndex(a) => {
                self.table(name, &a.table_id)?.pk_index =
                    Some((a.snapshot_id.clone(), a.index.clone().unwrap_or_default()));
            }
            Kind::ClearPkIndex(a) => self.table(name, &a.table_id)?.pk_index = None,
            Kind::AddIndexRun(a) => {
                self.table(name, &a.table_id)?
                    .index_runs
                    .insert((a.index_key.clone(), a.run_name.clone()), (a.rows, a.size));
            }
            Kind::RemoveIndexRun(a) => {
                self.table(name, &a.table_id)?
                    .index_runs
                    .remove(&(a.index_key.clone(), a.run_name.clone()));
            }
            Kind::AddInlinedData(a) => {
                let table = self.table(name, &a.table_id)?;
                ensure!(
                    !table.inlined_data.contains_key(&a.id),
                    GuardSnafu {
                        action: name,
                        table_id: a.table_id.clone(),
                        guard: format!("inline row id {} is new", a.id),
                    }
                );
                table.inlined_data.insert(
                    a.id.clone(),
                    Inlined {
                        partition_key: a.partition_key.clone(),
                        payload: ObjRef::of(&a.payload),
                        raw_payload: a.payload.clone(),
                        count: a.count,
                        seq: a.seq,
                    },
                );
            }
            Kind::UpdateInlinedData(a) => {
                let table = self.table(name, &a.table_id)?;
                ensure!(
                    !a.require_present || table.inlined_data.contains_key(&a.id),
                    GuardSnafu {
                        action: name,
                        table_id: a.table_id.clone(),
                        guard: format!("inline row {} is present", a.id)
                    }
                );
                if let Some(row) = table.inlined_data.get_mut(&a.id) {
                    row.payload = ObjRef::of(&a.payload);
                    row.raw_payload.clone_from(&a.payload);
                    row.count = a.count;
                }
            }
            Kind::RemoveInlinedData(a) => {
                let table = self.table(name, &a.table_id)?;
                let kind = select(a.select);
                table
                    .inlined_data
                    .retain(|id, _| !selects(kind, &a.ids, id));
            }
            Kind::AddInlinedDelete(a) => {
                let table = self.table(name, &a.table_id)?;
                let entry = InlinedDel {
                    payload: ObjRef::of(&a.payload),
                    raw_payload: a.payload.clone(),
                    count: a.count,
                    seq: a.seq,
                    published: a.published,
                };
                match table.inlined_deletes.get(&a.id) {
                    Some(existing) => ensure!(
                        a.idempotent && existing == &entry,
                        GuardSnafu {
                            action: name,
                            table_id: a.table_id.clone(),
                            guard: format!("inline tombstone {} matches the stored one", a.id),
                        }
                    ),
                    None => {
                        table.inlined_deletes.insert(a.id.clone(), entry);
                    }
                }
            }
            Kind::SetInlinedDeletesPublished(a) => {
                let table = self.table(name, &a.table_id)?;
                let kind = select(a.select);
                for (id, row) in &mut table.inlined_deletes {
                    if selects(kind, &a.ids, id) {
                        row.published = a.published;
                    }
                }
            }
            Kind::RemoveInlinedDeletes(a) => {
                let table = self.table(name, &a.table_id)?;
                let kind = select(a.select);
                table
                    .inlined_deletes
                    .retain(|id, _| !selects(kind, &a.ids, id));
            }
            Kind::AddInsertRecords(a) => {
                let table = self.table(name, &a.table_id)?;
                for key in &a.keys {
                    table.insert_records.insert(key.pk.clone(), key.seq);
                }
            }
            Kind::ClearInsertRecords(a) => {
                let table = self.table(name, &a.table_id)?;
                table
                    .insert_records
                    .retain(|_, seq| a.seq_le.is_some_and(|seq_le| *seq > seq_le));
            }
            Kind::MarkDirty(a) => {
                let table = self.table(name, &a.table_id)?;
                for key in &a.keys {
                    let stored = table
                        .pending_write_back
                        .entry(key.pk.clone())
                        .or_insert(key.seq);
                    *stored = (*stored).max(key.seq);
                }
            }
            Kind::ClearDelivered(a) => {
                let table = self.table(name, &a.table_id)?;
                for key in &a.keys {
                    if table
                        .pending_write_back
                        .get(&key.pk)
                        .is_some_and(|stored| *stored <= key.seq)
                    {
                        table.pending_write_back.remove(&key.pk);
                    }
                }
            }
            Kind::AddPartition(a) => {
                let table = self.table(name, &a.table_id)?;
                table.partitions.insert(
                    a.partition_id.clone(),
                    Partition {
                        columns: a.columns.clone(),
                        values: a.values.clone(),
                        path: a.path.clone(),
                        path_is_relative: a.path_is_relative,
                        record_count: a.record_count,
                        size: a.size,
                    },
                );
            }
            Kind::AdvanceSourcePosition(a) => {
                self.positions
                    .insert((a.source.clone(), a.partition.clone()), a.position.clone());
            }
            Kind::Unconverted(a) => {
                self.unknown.insert(a.table_id.clone());
            }
        }
        Ok(())
    }
}

/// Where two states differ, one line per difference, for a failing
/// differential.
#[must_use]
pub fn differences(expected: &TableState, actual: &TableState) -> Vec<String> {
    let mut out = Vec::new();
    macro_rules! scalar {
        ($field:ident) => {
            if expected.$field != actual.$field {
                out.push(format!(
                    "{}: expected {:?}, found {:?}",
                    stringify!($field),
                    expected.$field,
                    actual.$field
                ));
            }
        };
    }
    scalar!(name);
    scalar!(path);
    scalar!(path_is_relative);
    scalar!(schema);
    scalar!(primary_key_json);
    scalar!(on_conflict_json);
    scalar!(partition_column);
    scalar!(vortex_config_json);
    scalar!(current_snapshot_id);
    scalar!(seq);
    scalar!(stats);
    scalar!(pk_index);
    map_differences("files", &expected.files, &actual.files, &mut out);
    map_differences(
        "file_stats",
        &expected.file_stats,
        &actual.file_stats,
        &mut out,
    );
    map_differences("groups", &expected.groups, &actual.groups, &mut out);
    map_differences(
        "delete_files",
        &expected.delete_files,
        &actual.delete_files,
        &mut out,
    );
    map_differences("cold", &expected.cold, &actual.cold, &mut out);
    map_differences(
        "index_runs",
        &expected.index_runs,
        &actual.index_runs,
        &mut out,
    );
    map_differences(
        "inlined_data",
        &expected.inlined_data,
        &actual.inlined_data,
        &mut out,
    );
    map_differences(
        "inlined_deletes",
        &expected.inlined_deletes,
        &actual.inlined_deletes,
        &mut out,
    );
    map_differences(
        "insert_records",
        &expected.insert_records,
        &actual.insert_records,
        &mut out,
    );
    map_differences(
        "pending_write_back",
        &expected.pending_write_back,
        &actual.pending_write_back,
        &mut out,
    );
    map_differences(
        "partitions",
        &expected.partitions,
        &actual.partitions,
        &mut out,
    );
    out
}

fn map_differences<K: Ord + std::fmt::Debug, V: PartialEq + std::fmt::Debug>(
    name: &str,
    expected: &BTreeMap<K, V>,
    actual: &BTreeMap<K, V>,
    out: &mut Vec<String>,
) {
    const LIMIT: usize = 5;
    let mut shown = 0;
    for (key, value) in expected {
        match actual.get(key) {
            None => {
                shown += 1;
                if shown <= LIMIT {
                    out.push(format!("{name}[{key:?}]: expected {value:?}, missing"));
                }
            }
            Some(found) if found != value => {
                shown += 1;
                if shown <= LIMIT {
                    out.push(format!(
                        "{name}[{key:?}]: expected {value:?}, found {found:?}"
                    ));
                }
            }
            Some(_) => {}
        }
    }
    for (key, value) in actual {
        if !expected.contains_key(key) {
            shown += 1;
            if shown <= LIMIT {
                out.push(format!("{name}[{key:?}]: unexpected {value:?}"));
            }
        }
    }
    if shown > LIMIT {
        out.push(format!("{name}: {} more differences", shown - LIMIT));
    }
}
