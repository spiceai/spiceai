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

use prost::Message;
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
    pub created_at: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InlinedDel {
    pub payload: ObjRef,
    /// Original inline bytes, retained so checkpoints can replay the row.
    pub raw_payload: Vec<u8>,
    pub count: i64,
    pub seq: i64,
    pub created_at: String,
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
    /// Data files are keyed by their path alone ([`Self::file_key`]).
    pub flat: bool,
    /// Keys of the files that left the live set and are not yet deleted: the
    /// durable retirement record. The record that takes a file out of the live
    /// set retires it: [`DatasetState::apply`] adds the keys live before the
    /// record and not after it. A file a record carries stays live and is never
    /// retired. A sweep deletes a retired file only after that record, then
    /// forgets it (`ForgetRetired`), and no record may make a retired key live
    /// again. A retired data file keeps its entry and statistics until it is
    /// forgotten, so a scan that captured it before the record still reads it.
    pub retired: BTreeSet<String>,
}

/// The key of the data file at `path` in `snapshot`: see [`TableState::file_key`].
#[must_use]
pub fn file_key(flat: bool, snapshot: &str, path: &str) -> String {
    if flat {
        path.to_string()
    } else {
        format!("{snapshot}/{path}")
    }
}

/// A deletion vector's table-relative key when its path lies under the table.
#[must_use]
pub fn delete_file_key(
    table_path: &str,
    path_is_relative: bool,
    path: &str,
    relative: bool,
) -> String {
    if relative == path_is_relative
        && !table_path.is_empty()
        && let Some(rest) = path
            .strip_prefix(table_path)
            .and_then(|rest| rest.strip_prefix('/'))
    {
        return rest.to_string();
    }
    path.to_string()
}

impl TableState {
    /// The snapshots a scan reads: the current one and every protected one.
    #[must_use]
    pub fn live_snapshots(&self) -> BTreeSet<&str> {
        let mut live: BTreeSet<&str> = self.groups.keys().map(String::as_str).collect();
        live.insert(self.current_snapshot_id.as_str());
        live
    }

    /// The key of the data file at `path` in `snapshot`: its path relative to
    /// the table. In a flat table that is `path` itself; otherwise the file
    /// lies in its snapshot's directory, `<snapshot>/<path>`, so two snapshots
    /// never share a key.
    #[must_use]
    pub fn file_key(&self, snapshot: &str, path: &str) -> String {
        file_key(self.flat, snapshot, path)
    }

    /// The key of a deletion vector: its path relative to the table's path when
    /// it lies under it (`<snapshot>/deletions/<file>` beside the table's
    /// snapshot directories), and otherwise the path as recorded.
    #[must_use]
    pub fn delete_file_key(&self, dv: &DvEntry) -> String {
        delete_file_key(&self.path, self.path_is_relative, &dv.path, dv.relative)
    }

    /// The keys of every file a scan of the table can read: the data files of
    /// the live snapshots and every deletion vector.
    #[must_use]
    pub fn live_keys(&self) -> BTreeSet<String> {
        let live = self.live_snapshots();
        self.files
            .keys()
            .filter(|(snapshot, _)| live.contains(snapshot.as_str()))
            .map(|(snapshot, path)| self.file_key(snapshot, path))
            .chain(
                self.delete_files
                    .values()
                    .map(|dv| self.delete_file_key(dv)),
            )
            .collect()
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
    /// Unresolved transactions across datasets, by id.
    pub prepared: BTreeMap<String, Prepared>,
}

/// A transaction's `Prepare` on this dataset, held until its `Resolve`.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct Prepared {
    /// The tables it reserves.
    pub footprint: BTreeSet<String>,
    /// Its actions, as an encoded [`Record`], applied by a `Resolve` that commits.
    pub actions: Vec<u8>,
}

impl Prepared {
    /// The prepared actions.
    ///
    /// # Errors
    ///
    /// Returns [`ApplyError`] when the stored record does not decode.
    pub fn record(&self, txn: &str) -> Result<Record> {
        Record::decode(self.actions.as_slice()).map_err(|error| ApplyError::Guard {
            action: "Resolve",
            table_id: txn.to_string(),
            guard: format!("prepared actions decode: {error}"),
        })
    }
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
        // Keep the prior value of only what the record can change, so a failed
        // guard restores it without copying the tables it does not touch.
        let mut tables = BTreeMap::new();
        let mut positions = BTreeMap::new();
        let mut creates = false;
        let mut transactions = false;
        // A committing `Resolve` changes what its `Prepare` holds.
        let mut resolved = Vec::new();
        for action in &record.actions {
            if let Some(Kind::Resolve(a)) = &action.kind
                && a.commit
                && let Some(prepared) = self.prepared.get(&a.txn)
                && let Ok(inner) = prepared.record(&a.txn)
            {
                resolved.extend(inner.actions);
            }
        }
        for action in record.actions.iter().chain(&resolved) {
            if let Some(table_id) = action.table_id() {
                tables
                    .entry(table_id.to_string())
                    .or_insert_with(|| self.tables.get(table_id).cloned());
            }
            match &action.kind {
                Some(Kind::AdvanceSourcePosition(a)) => {
                    let key = (a.source.clone(), a.partition.clone());
                    let prior = self.positions.get(&key).cloned();
                    positions.entry(key).or_insert(prior);
                }
                Some(Kind::CreateTable(_) | Kind::DropTable(_) | Kind::Unconverted(_)) => {
                    creates = true;
                }
                Some(Kind::Prepare(_) | Kind::Resolve(_)) => transactions = true,
                _ => {}
            }
        }
        let unknown = creates.then(|| self.unknown.clone());
        let prepared = transactions.then(|| self.prepared.clone());
        if let Err(error) = self.apply_actions(&record.actions) {
            for (table_id, prior) in tables {
                match prior {
                    Some(table) => self.tables.insert(table_id, table),
                    None => self.tables.remove(&table_id),
                };
            }
            for (key, prior) in positions {
                match prior {
                    Some(position) => self.positions.insert(key, position),
                    None => self.positions.remove(&key),
                };
            }
            if let Some(unknown) = unknown {
                self.unknown = unknown;
            }
            if let Some(prepared) = prepared {
                self.prepared = prepared;
            }
            return Err(error);
        }
        Ok(())
    }

    /// Applies every action of `record` in order, without keeping what it
    /// changes: the cost is that of the actions alone, whatever the state's size.
    ///
    /// # Errors
    ///
    /// Returns [`ApplyError`] when a guard fails. The state then holds the
    /// actions before the failing one and no longer matches any committed
    /// state: the caller must discard it and rebuild from the log.
    pub fn apply_in_place(&mut self, record: &Record) -> Result<()> {
        self.apply_actions(&record.actions)
    }

    /// Applies `actions` as one record: each in order, then retires every key
    /// that was live before the record and is not after it, in each table whose
    /// live set the record can shrink. A record that makes a retired key live
    /// again is refused.
    fn apply_actions(&mut self, actions: &[Action]) -> Result<()> {
        let before = self.live_keys_before(actions);
        for action in actions {
            self.apply_action(action)?;
        }
        for (table_id, (action, live_before)) in before {
            if self.unknown.contains(&table_id) {
                continue;
            }
            let Some(table) = self.tables.get_mut(&table_id) else {
                continue;
            };
            let live_after = table.live_keys();
            if let Some(key) = live_after.iter().find(|key| table.retired.contains(*key)) {
                return GuardSnafu {
                    action,
                    table_id,
                    guard: format!("retired file {key} is not made live again"),
                }
                .fail();
            }
            table
                .retired
                .extend(live_before.difference(&live_after).cloned());
        }
        Ok(())
    }

    /// The live keys, before `actions` apply, of every table one of them can
    /// take a file out of the live set of, or bring a whole snapshot into it,
    /// with the first such action's name. The actions of a `Resolve` that
    /// commits count as the record's own.
    fn live_keys_before(
        &self,
        actions: &[Action],
    ) -> BTreeMap<String, (&'static str, BTreeSet<String>)> {
        let mut resolved = Vec::new();
        for action in actions {
            if let Some(Kind::Resolve(a)) = &action.kind
                && a.commit
                && let Some(prepared) = self.prepared.get(&a.txn)
                && let Ok(inner) = prepared.record(&a.txn)
            {
                resolved.extend(inner.actions);
            }
        }
        let mut before = BTreeMap::new();
        for action in actions.iter().chain(&resolved) {
            let changes_live = matches!(
                action.kind,
                Some(
                    Kind::SetCurrentSnapshot(_)
                        | Kind::SetGroup(_)
                        | Kind::RemoveGroups(_)
                        | Kind::RemoveFiles(_)
                )
            ) || matches!(&action.kind, Some(Kind::RemoveDeleteFiles(a)) if a.select != Select::Listed as i32 || !a.ids.is_empty());
            if let (true, Some(table_id)) = (changes_live, action.table_id()) {
                before.entry(table_id.to_string()).or_insert_with(|| {
                    (
                        action.name(),
                        self.tables
                            .get(table_id)
                            .map(TableState::live_keys)
                            .unwrap_or_default(),
                    )
                });
            }
        }
        before
    }

    #[expect(clippy::too_many_lines, reason = "one arm per action")]
    fn apply_action(&mut self, action: &Action) -> Result<()> {
        let name = action.name();
        let kind = action.kind.as_ref().ok_or(ApplyError::EmptyAction)?;
        if let Some(table_id) = action.table_id()
            && let Some((txn, _)) = self
                .prepared
                .iter()
                .find(|(_, prepared)| prepared.footprint.contains(table_id))
        {
            // The reservation: what a `Prepare` checked must still hold at its
            // `Resolve`, which re-runs no guard.
            return GuardSnafu {
                action: name,
                table_id: table_id.to_string(),
                guard: format!("no unresolved transaction '{txn}' reserves the table"),
            }
            .fail();
        }
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
                        flat: a.flat_layout,
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
                let live = table.live_snapshots().contains(a.snapshot_id.as_str());
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
                    // A snapshot made live later is checked by the record that
                    // makes it live.
                    if live {
                        let file_key = table.file_key(&key.0, &key.1);
                        ensure!(
                            !table.retired.contains(&file_key),
                            GuardSnafu {
                                action: name,
                                table_id: a.table_id.clone(),
                                guard: format!("retired file {file_key} is not made live again"),
                            }
                        );
                    }
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
                let table = self.table(name, &a.table_id)?;
                table.groups.insert(a.snapshot_id.clone(), a.seq);
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
                    let key = table.delete_file_key(file);
                    ensure!(
                        !table.retired.contains(&key),
                        GuardSnafu {
                            action: name,
                            table_id: a.table_id.clone(),
                            guard: format!("retired file {key} is not made live again"),
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
                        created_at: a.created_at.clone(),
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
                    created_at: a.created_at.clone(),
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
                let key = (a.source.clone(), a.partition.clone());
                if a.clear {
                    self.positions.remove(&key);
                } else {
                    self.positions.insert(key, a.position.clone());
                }
            }
            Kind::Unconverted(a) => {
                self.unknown.insert(a.table_id.clone());
            }
            Kind::RetireFiles(a) => {
                let table = self.table(name, &a.table_id)?;
                let live = table.live_keys();
                for key in &a.keys {
                    ensure!(
                        !live.contains(key),
                        GuardSnafu {
                            action: name,
                            table_id: a.table_id.clone(),
                            guard: format!("file {key} is not live"),
                        }
                    );
                }
                table.retired.extend(a.keys.iter().cloned());
            }
            Kind::ForgetRetired(a) => {
                let table = self.table(name, &a.table_id)?;
                let forgotten: BTreeSet<&str> = a
                    .keys
                    .iter()
                    .filter(|key| table.retired.remove(key.as_str()))
                    .map(String::as_str)
                    .collect();
                if !forgotten.is_empty() {
                    // A retired key is live nowhere, so every entry that names
                    // it goes; the live check only keeps that true if a key
                    // were somehow both.
                    let mut live: BTreeSet<&str> =
                        table.groups.keys().map(String::as_str).collect();
                    live.insert(table.current_snapshot_id.as_str());
                    let flat = table.flat;
                    let gone = |snapshot: &String, path: &String| {
                        !live.contains(snapshot.as_str())
                            && if flat {
                                forgotten.contains(path.as_str())
                            } else {
                                forgotten.contains(file_key(false, snapshot, path).as_str())
                            }
                    };
                    table
                        .files
                        .retain(|(snapshot, path), _| !gone(snapshot, path));
                    table
                        .file_stats
                        .retain(|(snapshot, path), _| !gone(snapshot, path));
                }
            }
            Kind::Prepare(a) => self.prepare(a)?,
            Kind::Resolve(a) => {
                let footprint: BTreeSet<String> = a.footprint.iter().cloned().collect();
                ensure!(
                    self.prepared
                        .get(&a.txn)
                        .is_some_and(|prepared| prepared.footprint == footprint),
                    GuardSnafu {
                        action: name,
                        table_id: a.txn.clone(),
                        guard: "an unresolved Prepare with the same transaction and footprint"
                            .to_string(),
                    }
                );
                // Removed first, so its own reservation no longer applies to it.
                let Some(prepared) = self.prepared.remove(&a.txn) else {
                    return Ok(());
                };
                if a.commit {
                    for action in prepared.record(&a.txn)?.actions {
                        self.apply_action(&action)?;
                    }
                }
            }
        }
        Ok(())
    }

    /// Accept a `Prepare` iff no unresolved one shares a table with it and its
    /// actions apply to its footprint's tables now. Those tables stay reserved,
    /// so the actions still apply at the `Resolve`.
    fn prepare(&mut self, a: &crate::proto::Prepare) -> Result<()> {
        let guard = |guard: String| ApplyError::Guard {
            action: "Prepare",
            table_id: a.txn.clone(),
            guard,
        };
        let footprint: BTreeSet<String> = a.footprint.iter().cloned().collect();
        if a.txn.is_empty() || footprint.is_empty() || self.prepared.contains_key(&a.txn) {
            return Err(guard(
                "a new transaction id and a non-empty footprint".to_string(),
            ));
        }
        if let Some((other, _)) = self
            .prepared
            .iter()
            .find(|(_, prepared)| !prepared.footprint.is_disjoint(&footprint))
        {
            return Err(guard(format!(
                "no unresolved transaction '{other}' shares a table with it"
            )));
        }
        let record = Record::decode(a.actions.as_slice())
            .map_err(|error| guard(format!("prepared actions decode: {error}")))?;
        let mut scratch = DatasetState::default();
        for table_id in &footprint {
            let table = self
                .tables
                .get(table_id)
                .ok_or_else(|| ApplyError::NoTable {
                    action: "Prepare",
                    table_id: table_id.clone(),
                })?;
            scratch.tables.insert(table_id.clone(), table.clone());
            if self.unknown.contains(table_id) {
                scratch.unknown.insert(table_id.clone());
            }
        }
        for action in &record.actions {
            let in_footprint = action
                .table_id()
                .is_some_and(|table_id| footprint.contains(table_id));
            let changes_data = !matches!(
                action.kind,
                Some(
                    Kind::CreateTable(_)
                        | Kind::DropTable(_)
                        | Kind::Unconverted(_)
                        | Kind::Prepare(_)
                        | Kind::Resolve(_)
                )
            );
            if !(in_footprint && changes_data) {
                return Err(guard(format!(
                    "{} changes a table of its footprint and no table's identity",
                    action.name()
                )));
            }
        }
        scratch.apply_actions(&record.actions)?;
        self.prepared.insert(
            a.txn.clone(),
            Prepared {
                footprint,
                actions: a.actions.clone(),
            },
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{
        AddFiles, AdvanceSeq, AdvanceSourcePosition, CreateTable, Prepare, Resolve, TableRef,
        Unconverted,
    };

    /// The obviously atomic apply: every action on a copy, kept only if all apply.
    fn reference_apply(state: &mut DatasetState, record: &Record) -> Result<()> {
        let mut next = state.clone();
        next.apply_actions(&record.actions)?;
        *state = next;
        Ok(())
    }

    /// A small deterministic generator, so a failure names its seed.
    struct Lcg(u64);
    impl Lcg {
        fn below(&mut self, n: u64) -> u64 {
            self.0 = self
                .0
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            (self.0 >> 33) % n
        }
    }

    fn random_action(rng: &mut Lcg) -> Action {
        let table_id = format!("t{}", rng.below(4));
        let kind = match rng.below(9) {
            0 => Kind::CreateTable(CreateTable {
                table_id,
                name: format!("n{}", rng.below(5)),
                current_snapshot_id: "s".into(),
                ..CreateTable::default()
            }),
            1 => Kind::DropTable(TableRef { table_id }),
            2 | 3 => Kind::AddFiles(AddFiles {
                table_id,
                snapshot_id: "s".into(),
                files: (0..=rng.below(3))
                    .map(|_| FileEntry {
                        path: format!("f{}", rng.below(12)),
                        rows: 1,
                        ..FileEntry::default()
                    })
                    .collect(),
                upsert: rng.below(4) == 0,
            }),
            4 => Kind::AdvanceSeq(AdvanceSeq {
                table_id,
                to: i64::try_from(rng.below(40)).unwrap_or_default(),
            }),
            5 => Kind::AdvanceSourcePosition(AdvanceSourcePosition {
                source: format!("src{}", rng.below(3)),
                partition: String::new(),
                position: vec![u8::try_from(rng.below(250)).unwrap_or_default()],
                clear: rng.below(5) == 0,
            }),
            6 => Kind::Unconverted(Unconverted {
                table_id,
                method: "m".into(),
            }),
            7 => {
                let other = format!("t{}", rng.below(4));
                let mut footprint = vec![table_id.clone(), other.clone()];
                footprint.sort();
                footprint.dedup();
                let inner = Record {
                    actions: vec![
                        Action::new(Kind::AdvanceSeq(AdvanceSeq {
                            table_id,
                            to: i64::try_from(rng.below(40)).unwrap_or_default(),
                        })),
                        Action::new(Kind::AddFiles(AddFiles {
                            table_id: other,
                            snapshot_id: "s".into(),
                            files: vec![FileEntry {
                                path: format!("f{}", rng.below(12)),
                                rows: 1,
                                ..FileEntry::default()
                            }],
                            upsert: false,
                        })),
                    ],
                };
                Kind::Prepare(Prepare {
                    txn: format!("x{}", rng.below(3)),
                    footprint,
                    actions: inner.encode_to_vec(),
                })
            }
            _ => {
                let other = format!("t{}", rng.below(4));
                let mut footprint = vec![table_id, other];
                footprint.sort();
                footprint.dedup();
                Kind::Resolve(Resolve {
                    txn: format!("x{}", rng.below(3)),
                    footprint,
                    commit: rng.below(2) == 0,
                })
            }
        };
        Action::new(kind)
    }

    fn create(table_id: &str) -> Action {
        Action::new(Kind::CreateTable(CreateTable {
            table_id: table_id.into(),
            name: table_id.into(),
            current_snapshot_id: "s".into(),
            ..CreateTable::default()
        }))
    }

    fn advance(table_id: &str, to: i64) -> Action {
        Action::new(Kind::AdvanceSeq(AdvanceSeq {
            table_id: table_id.into(),
            to,
        }))
    }

    fn prepare(txn: &str, footprint: &[&str], actions: Vec<Action>) -> Action {
        Action::new(Kind::Prepare(Prepare {
            txn: txn.into(),
            footprint: footprint.iter().map(ToString::to_string).collect(),
            actions: Record { actions }.encode_to_vec(),
        }))
    }

    fn resolve(txn: &str, footprint: &[&str], commit: bool) -> Action {
        Action::new(Kind::Resolve(Resolve {
            txn: txn.into(),
            footprint: footprint.iter().map(ToString::to_string).collect(),
            commit,
        }))
    }

    fn one(action: Action) -> Record {
        Record {
            actions: vec![action],
        }
    }

    fn set_current(table_id: &str, snapshot: &str) -> Action {
        Action::new(Kind::SetCurrentSnapshot(crate::proto::SetCurrentSnapshot {
            table_id: table_id.into(),
            snapshot_id: snapshot.into(),
            ..Default::default()
        }))
    }

    fn set_group(table_id: &str, snapshot: &str) -> Action {
        Action::new(Kind::SetGroup(crate::proto::SetGroup {
            table_id: table_id.into(),
            snapshot_id: snapshot.into(),
            seq: 1,
        }))
    }

    fn remove_groups(table_id: &str, snapshots: &[&str]) -> Action {
        Action::new(Kind::RemoveGroups(crate::proto::RemoveGroups {
            table_id: table_id.into(),
            select: crate::proto::Select::Listed as i32,
            snapshot_ids: snapshots.iter().map(ToString::to_string).collect(),
            require_present: true,
        }))
    }

    fn retired<'a>(state: &'a DatasetState, table_id: &str) -> Vec<&'a str> {
        state.tables[table_id]
            .retired
            .iter()
            .map(String::as_str)
            .collect()
    }

    fn files(table_id: &str, snapshot: &str, paths: &[&str]) -> Action {
        Action::new(Kind::AddFiles(AddFiles {
            table_id: table_id.into(),
            snapshot_id: snapshot.into(),
            files: paths
                .iter()
                .map(|path| FileEntry {
                    path: (*path).into(),
                    rows: 1,
                    ..FileEntry::default()
                })
                .collect(),
            upsert: false,
        }))
    }

    fn stat(table_id: &str, snapshot: &str, path: &str) -> Action {
        Action::new(Kind::SetFileStats(crate::proto::SetFileStats {
            table_id: table_id.into(),
            stat: Some(FileStat {
                snapshot_id: snapshot.into(),
                path: path.into(),
                rows: 1,
                ..FileStat::default()
            }),
        }))
    }

    /// A table whose files lie under `data/<table_id>/<snapshot>/` and whose
    /// deletion vectors under `data/<snapshot>/deletions/`, as Cayenne places
    /// them; `flat` keys data files by their path alone.
    fn create_at(table_id: &str, flat: bool) -> Action {
        Action::new(Kind::CreateTable(CreateTable {
            table_id: table_id.into(),
            name: table_id.into(),
            path: "data".into(),
            current_snapshot_id: "s".into(),
            flat_layout: flat,
            ..CreateTable::default()
        }))
    }

    fn add_dv(table_id: &str, id: &str, snapshot: &str) -> Action {
        Action::new(Kind::AddDeleteFiles(crate::proto::AddDeleteFiles {
            table_id: table_id.into(),
            files: vec![DvEntry {
                id: id.into(),
                path: format!("data/{snapshot}/deletions/{id}.arrow"),
                seq: 1,
                ..DvEntry::default()
            }],
        }))
    }

    fn remove_dvs(table_id: &str, ids: &[&str]) -> Action {
        Action::new(Kind::RemoveDeleteFiles(crate::proto::RemoveDeleteFiles {
            table_id: table_id.into(),
            select: crate::proto::Select::Listed as i32,
            ids: ids.iter().map(ToString::to_string).collect(),
            seq_le: None,
        }))
    }

    fn forget(table_id: &str, keys: &[&str]) -> Action {
        Action::new(Kind::ForgetRetired(crate::proto::FileKeys {
            table_id: table_id.into(),
            keys: keys.iter().map(ToString::to_string).collect(),
        }))
    }

    fn refused_as(outcome: &Result<()>, expected: &str) -> bool {
        matches!(outcome, Err(ApplyError::Guard { guard, .. }) if guard == expected)
    }

    /// Every file that leaves the live set is retired by the record that takes
    /// it out, and only those: the files of the snapshots a protected merge
    /// replaces, of the current snapshot the current pointer moves away from
    /// (not those of the protected snapshot it moves to), of a full rewrite,
    /// and a removed deletion vector. A forget takes keys out once and for all,
    /// and a live file cannot be stated retired.
    #[test]
    fn a_file_that_leaves_the_live_set_is_retired_by_that_record() {
        let mut state = DatasetState::default();
        state.apply(&one(create_at("t", false))).expect("create");
        state
            .apply(&Record {
                actions: vec![
                    files("t", "s", &["a"]),
                    files("t", "g1", &["x"]),
                    set_group("t", "g1"),
                    files("t", "g2", &["y"]),
                    set_group("t", "g2"),
                ],
            })
            .expect("protect");
        assert!(retired(&state, "t").is_empty());

        // A protected merge: g1 and g2 out, g3 in.
        state
            .apply(&Record {
                actions: vec![
                    remove_groups("t", &["g1", "g2"]),
                    files("t", "g3", &["z"]),
                    set_group("t", "g3"),
                ],
            })
            .expect("merge");
        assert_eq!(retired(&state, "t"), ["g1/x", "g2/y"]);

        // The current snapshot moves to a protected one: the old current's
        // files retire, the protected one's stay live.
        state.apply(&one(set_current("t", "g3"))).expect("move");
        assert_eq!(retired(&state, "t"), ["g1/x", "g2/y", "s/a"]);

        // A deletion vector, keyed relative to the table's path, retires when
        // it is removed.
        state.apply(&one(add_dv("t", "d1", "g3"))).expect("add dv");
        state
            .apply(&one(remove_dvs("t", &["d1"])))
            .expect("drop dv");
        assert_eq!(
            retired(&state, "t"),
            ["g1/x", "g2/y", "g3/deletions/d1.arrow", "s/a"]
        );

        // A full rewrite: every protected snapshot and the current one retire.
        state
            .apply(&Record {
                actions: vec![
                    Action::new(Kind::RemoveGroups(crate::proto::RemoveGroups {
                        table_id: "t".into(),
                        select: crate::proto::Select::All as i32,
                        ..Default::default()
                    })),
                    files("t", "c2", &["w"]),
                    set_current("t", "c2"),
                ],
            })
            .expect("rewrite");
        assert_eq!(
            retired(&state, "t"),
            ["g1/x", "g2/y", "g3/deletions/d1.arrow", "g3/z", "s/a"]
        );

        // Forgotten after deletion; forgetting twice changes nothing more.
        state
            .apply(&one(forget("t", &["g2/y", "s/a"])))
            .expect("forget");
        state
            .apply(&one(forget("t", &["g2/y", "s/a"])))
            .expect("forget again");
        assert_eq!(
            retired(&state, "t"),
            ["g1/x", "g3/deletions/d1.arrow", "g3/z"]
        );

        // A live file cannot be stated retired.
        let refused = state.apply(&one(Action::new(Kind::RetireFiles(
            crate::proto::FileKeys {
                table_id: "t".into(),
                keys: vec!["c2/w".into()],
            },
        ))));
        assert!(refused_as(&refused, "file c2/w is not live"), "{refused:?}");
        assert_eq!(
            retired(&state, "t"),
            ["g1/x", "g3/deletions/d1.arrow", "g3/z"]
        );
    }

    /// A compaction that carries a file in place names it in its new snapshot
    /// under the same key: the record retires only the files it removes, never
    /// the carried one, even though the snapshot that held it leaves the live
    /// set.
    #[test]
    fn a_carried_file_is_never_retired() {
        let mut state = DatasetState::default();
        state.apply(&one(create_at("t", true))).expect("create");
        state
            .apply(&one(files("t", "s", &["a.vortex", "b.vortex"])))
            .expect("append");
        state
            .apply(&Record {
                actions: vec![
                    files("t", "s2", &["a.vortex", "c.vortex"]),
                    set_current("t", "s2"),
                ],
            })
            .expect("compact, carrying a.vortex");
        assert_eq!(retired(&state, "t"), ["b.vortex"]);
        assert_eq!(
            state.tables["t"]
                .live_keys()
                .into_iter()
                .collect::<Vec<_>>(),
            ["a.vortex", "c.vortex"]
        );

        // Carried again by a protected merge, and still never retired.
        state
            .apply(&Record {
                actions: vec![
                    files("t", "g1", &["a.vortex"]),
                    set_group("t", "g1"),
                    files("t", "s3", &["c.vortex"]),
                    set_current("t", "s3"),
                ],
            })
            .expect("move the current snapshot, carrying both files");
        assert_eq!(retired(&state, "t"), ["b.vortex"]);
    }

    /// No record makes a retired file live again: not moving the current
    /// snapshot back to the one that held it, not protecting that snapshot, not
    /// adding the file to a live snapshot, and not adding a removed deletion
    /// vector back. Each refusal leaves the state as it was.
    #[test]
    fn a_retired_key_cannot_become_live_again() {
        let mut state = DatasetState::default();
        state.apply(&one(create_at("t", false))).expect("create");
        state.apply(&one(files("t", "s", &["a"]))).expect("append");
        state
            .apply(&Record {
                actions: vec![files("t", "s2", &["b"]), set_current("t", "s2")],
            })
            .expect("replace s");
        state.apply(&one(add_dv("t", "d1", "s2"))).expect("add dv");
        state
            .apply(&one(remove_dvs("t", &["d1"])))
            .expect("drop dv");
        assert_eq!(retired(&state, "t"), ["s/a", "s2/deletions/d1.arrow"]);
        let settled = state.clone();

        let back = state.apply(&one(set_current("t", "s")));
        assert!(
            refused_as(&back, "retired file s/a is not made live again"),
            "{back:?}"
        );
        let protect = state.apply(&one(set_group("t", "s")));
        assert!(
            refused_as(&protect, "retired file s/a is not made live again"),
            "{protect:?}"
        );
        state
            .apply(&Record {
                actions: vec![files("t", "s3", &["a"])],
            })
            .expect("a snapshot that is not live may name any file");
        let settled = {
            assert_eq!(state.tables["t"].retired, settled.tables["t"].retired);
            state.clone()
        };
        let readd = state.apply(&Record {
            actions: vec![remove_groups("t", &[]), files("t", "s2", &["a"])],
        });
        assert!(readd.is_ok(), "s2/a is a new key: {readd:?}");
        let carry_back = state.apply(&one(set_group("t", "s")));
        assert!(
            refused_as(&carry_back, "retired file s/a is not made live again"),
            "{carry_back:?}"
        );
        let dv_back = state.apply(&one(add_dv("t", "d1-again", "s2")));
        assert!(dv_back.is_ok(), "another deletion vector: {dv_back:?}");
        let mut same_path = add_dv("t", "d2", "s2");
        if let Some(Kind::AddDeleteFiles(a)) = &mut same_path.kind {
            a.files[0].path = "data/s2/deletions/d1.arrow".into();
        }
        let dv_again = state.apply(&one(same_path));
        assert!(
            refused_as(
                &dv_again,
                "retired file s2/deletions/d1.arrow is not made live again"
            ),
            "{dv_again:?}"
        );
        assert_eq!(state.tables["t"].retired, settled.tables["t"].retired);
        assert_eq!(state.tables["t"].current_snapshot_id, "s2");
        assert!(state.tables["t"].groups.is_empty());

        // In a flat table a compaction cannot carry a file it retired.
        state
            .apply(&one(create_at("u", true)))
            .expect("create flat");
        state.apply(&one(files("u", "s", &["a"]))).expect("append");
        state
            .apply(&Record {
                actions: vec![files("u", "s2", &["b"]), set_current("u", "s2")],
            })
            .expect("replace a");
        let carried = state.apply(&Record {
            actions: vec![files("u", "s3", &["a", "b"]), set_current("u", "s3")],
        });
        assert!(
            refused_as(&carried, "retired file a is not made live again"),
            "{carried:?}"
        );
        assert_eq!(state.tables["u"].current_snapshot_id, "s2");
        assert_eq!(retired(&state, "u"), ["a"]);
    }

    /// `ForgetRetired` removes exactly its keys from the retired set, with the
    /// entries and statistics that name them; another retired file keeps both,
    /// and so does a live file, even one whose key a forget lists.
    #[test]
    fn forget_drops_rows_and_stats_of_its_keys_only() {
        let mut state = DatasetState::default();
        state.apply(&one(create_at("t", false))).expect("create");
        state
            .apply(&Record {
                actions: vec![
                    files("t", "s", &["a", "b"]),
                    stat("t", "s", "a"),
                    stat("t", "s", "b"),
                ],
            })
            .expect("append");
        state
            .apply(&Record {
                actions: vec![
                    files("t", "s2", &["c"]),
                    stat("t", "s2", "c"),
                    set_current("t", "s2"),
                ],
            })
            .expect("replace s");
        assert_eq!(retired(&state, "t"), ["s/a", "s/b"]);

        state
            .apply(&one(forget("t", &["s/a", "s2/c", "nothing/here"])))
            .expect("forget");
        let table = &state.tables["t"];
        assert_eq!(retired(&state, "t"), ["s/b"]);
        let entries: Vec<(&str, &str)> = table
            .files
            .keys()
            .map(|(snapshot, path)| (snapshot.as_str(), path.as_str()))
            .collect();
        assert_eq!(entries, [("s", "b"), ("s2", "c")]);
        let stat_keys: Vec<(&str, &str)> = table
            .file_stats
            .keys()
            .map(|(snapshot, path)| (snapshot.as_str(), path.as_str()))
            .collect();
        assert_eq!(stat_keys, [("s", "b"), ("s2", "c")]);

        // A checkpoint carries the retired set, and the forget after it too.
        let mut rebuilt = DatasetState::default();
        rebuilt
            .apply(&crate::checkpoint::state_as_root(&state))
            .expect("root");
        assert_eq!(rebuilt, state);
        rebuilt.apply(&one(forget("t", &["s/b"]))).expect("forget");
        assert!(rebuilt.tables["t"].retired.is_empty());
        assert_eq!(rebuilt.tables["t"].files.len(), 1);
        assert_eq!(rebuilt.tables["t"].file_stats.len(), 1);
    }

    #[test]
    fn a_prepared_table_refuses_other_changes_until_its_resolve_commits_the_actions() {
        let mut state = DatasetState::default();
        state
            .apply(&Record {
                actions: vec![create("a"), create("b")],
            })
            .expect("create");
        state
            .apply(&one(prepare("x", &["a"], vec![advance("a", 5)])))
            .expect("prepare");
        assert_eq!(state.tables["a"].seq, 0, "a prepared action is not visible");
        assert_eq!(
            state.apply(&one(advance("a", 3))),
            Err(ApplyError::Guard {
                action: "AdvanceSeq",
                table_id: "a".into(),
                guard: "no unresolved transaction 'x' reserves the table".into(),
            })
        );
        assert_eq!(
            state.apply(&one(prepare("y", &["a", "b"], vec![advance("b", 1)]))),
            Err(ApplyError::Guard {
                action: "Prepare",
                table_id: "y".into(),
                guard: "no unresolved transaction 'x' shares a table with it".into(),
            })
        );
        state
            .apply(&one(advance("b", 2)))
            .expect("b is not reserved");
        assert_eq!(
            state.apply(&one(resolve("x", &["a", "b"], true))),
            Err(ApplyError::Guard {
                action: "Resolve",
                table_id: "x".into(),
                guard: "an unresolved Prepare with the same transaction and footprint".into(),
            })
        );
        state
            .apply(&one(resolve("x", &["a"], true)))
            .expect("resolve");
        assert_eq!(state.tables["a"].seq, 5);
        assert!(state.prepared.is_empty());
        state.apply(&one(advance("a", 6))).expect("released");
        assert_eq!(state.tables["a"].seq, 6);
    }

    #[test]
    fn an_aborted_prepare_discards_its_actions_and_a_refused_one_is_not_held() {
        let mut state = DatasetState::default();
        state.apply(&one(create("a"))).expect("create");
        state
            .apply(&one(prepare("x", &["a"], vec![advance("a", 5)])))
            .expect("prepare");
        state
            .apply(&one(resolve("x", &["a"], false)))
            .expect("abort");
        assert_eq!(state.tables["a"].seq, 0);
        // An action outside the footprint is refused, and the Prepare is not held.
        assert_eq!(
            state.apply(&one(prepare("y", &["a"], vec![advance("b", 1)]))),
            Err(ApplyError::Guard {
                action: "Prepare",
                table_id: "y".into(),
                guard: "AdvanceSeq changes a table of its footprint and no table's identity".into(),
            })
        );
        assert!(state.prepared.is_empty());
        state.apply(&one(advance("a", 1))).expect("not reserved");
    }

    #[test]
    fn a_checkpoint_keeps_an_unresolved_prepare_and_its_reservation() {
        let mut state = DatasetState::default();
        state.apply(&one(create("a"))).expect("create");
        state
            .apply(&one(prepare("x", &["a"], vec![advance("a", 5)])))
            .expect("prepare");
        let mut rebuilt = DatasetState::default();
        rebuilt
            .apply(&crate::checkpoint::state_as_root(&state))
            .expect("root");
        assert_eq!(rebuilt, state);
        assert!(
            rebuilt.apply(&one(advance("a", 1))).is_err(),
            "still reserved"
        );
        rebuilt
            .apply(&one(resolve("x", &["a"], true)))
            .expect("resolve after the checkpoint");
        assert_eq!(rebuilt.tables["a"].seq, 5);
    }

    /// Applying in place restores what a refused record changed: after every
    /// record, accepted or refused, the state equals the copy-based apply's.
    #[test]
    fn a_refused_record_leaves_the_state_as_the_copy_based_apply_does() {
        let (mut refused, mut accepted) = (0, 0);
        for seed in 0..200_u64 {
            let mut rng = Lcg(seed);
            let (mut state, mut expected) = (DatasetState::default(), DatasetState::default());
            for step in 0..200 {
                let record = Record {
                    actions: (0..=rng.below(4))
                        .map(|_| random_action(&mut rng))
                        .collect(),
                };
                let got = state.apply(&record);
                let want = reference_apply(&mut expected, &record);
                assert_eq!(got, want, "seed {seed} step {step}: outcome");
                assert_eq!(state, expected, "seed {seed} step {step}: state");
                if want.is_ok() {
                    accepted += 1;
                } else {
                    refused += 1;
                }
            }
        }
        // Both paths ran often: a generator that never fails a guard checks nothing.
        assert!(
            refused > 5_000 && accepted > 5_000,
            "refused {refused}, accepted {accepted}"
        );
    }
}
