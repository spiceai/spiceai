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

//! Domain lifecycle, replay, and state operations.

use super::{
    super::{actions, state_reader, state_reader::DatasetStateReader},
    Access, Domain, FENCED_ON_REOPEN, Head, Inner, NativeCatalogConfig, OpenSummary, Stop,
    ValueChange, fenced_message, invalid, refused_change_message, replica_change_message,
    slot::{StateLease, StateSlot},
    subject, transactions_beside,
};
use crate::metadata::TableStatistics;
use crate::{
    catalog::{CatalogError, CatalogResult},
    metadata::CreateTableOptions,
};
use arrow_schema::SchemaRef;
use cayenne_metastore_actions::{Action, DatasetState, Kind, ObjRef, Record, proto};
use object_store::{ObjectStore, ObjectStoreExt, path::Path};
use object_store_occ::headlog::{Collector, Group, HeadLog, Opened, Poll, Reader, Writer};
use parking_lot::Mutex;
use prost::Message;
use std::sync::Arc;
use tokio::sync::Notify;

impl Domain {
    /// This log's part of a transaction across datasets: commit its buffered
    /// changes, then a `Prepare` holding the publications' actions. Returns the
    /// footprint. The caller holds [`StateSlot::reserve`].
    pub(super) async fn prepare(
        &self,
        txn: &str,
        publications: &[crate::catalog::CatalogPublication<'_>],
    ) -> CatalogResult<Vec<String>> {
        let mut inner = self.inner.take_reserved().await;
        let mut batch = inner.publication_actions(publications)?;
        inner.stamp(&mut batch);
        let footprint: std::collections::BTreeSet<String> = batch
            .iter()
            .filter_map(|action| action.table_id().map(ToString::to_string))
            .collect();
        let footprint: Vec<String> = footprint.into_iter().collect();
        inner
            .commit_now(Action::new(Kind::Prepare(proto::Prepare {
                txn: txn.to_string(),
                footprint: footprint.clone(),
                actions: Record { actions: batch }.encode_to_vec(),
            })))
            .await?;
        Ok(footprint)
    }
    /// Commit the decided outcome of `txn` on this log.
    pub(super) async fn resolve(
        &self,
        txn: &str,
        footprint: Vec<String>,
        commit: bool,
    ) -> CatalogResult<()> {
        self.inner
            .take_reserved()
            .await
            .commit_now(Action::new(Kind::Resolve(proto::Resolve {
                txn: txn.to_string(),
                footprint,
                commit,
            })))
            .await
    }
    /// Every table on this log: its id, its name, and whether it partitions
    /// into child tables.
    pub(super) async fn tables(&self) -> Vec<(String, String, bool)> {
        self.inner
            .take()
            .await
            .reader
            .state
            .tables
            .iter()
            .map(|(id, table)| {
                (
                    id.clone(),
                    table.name.clone(),
                    table.partition_column.is_some(),
                )
            })
            .collect()
    }
    /// The positions whose key names `dataset` (`<store>/<dataset>`), with
    /// their counters.
    pub(super) async fn positions_of(&self, dataset: &str) -> Vec<((String, String), Vec<u8>)> {
        self.inner
            .take()
            .await
            .reader
            .state
            .positions
            .iter()
            .filter(|((source, _), _)| {
                source
                    .split_once('/')
                    .is_some_and(|(_, named)| named == dataset)
            })
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect()
    }
    /// The values under `source`, by partition.
    pub(super) async fn positions_under(&self, source: &str) -> Vec<(String, Vec<u8>)> {
        self.inner
            .take()
            .await
            .reader
            .state
            .positions
            .iter()
            .filter(|((key, _), _)| key == source)
            .map(|((_, partition), value)| (partition.clone(), value.clone()))
            .collect()
    }
    /// Commit `actions` as a change made by a catalog method.
    pub(super) async fn commit_actions(&self, actions: Vec<Action>) -> CatalogResult<()> {
        if actions.is_empty() {
            return Ok(());
        }
        self.inner.take().await.commit(actions).await
    }
    /// The transactions prepared on this log and not yet resolved, with their
    /// footprints.
    pub(super) async fn unresolved(&self) -> Vec<(String, Vec<String>)> {
        self.inner
            .take()
            .await
            .reader
            .state
            .prepared
            .iter()
            .map(|(txn, prepared)| (txn.clone(), prepared.footprint.iter().cloned().collect()))
            .collect()
    }
    pub(super) async fn read(&self) -> CatalogResult<StateLease<'_>> {
        let inner = self.inner.take().await;
        match inner.stop {
            None | Some(Stop::Fenced { reloaded: true } | Stop::Replica) => Ok(inner),
            Some(stop) => Err(inner.stopped(stop)),
        }
    }
    /// Create an empty log at a new prefix.
    /// # Errors
    /// Returns an error when conditional writes are unsupported or the prefix exists.
    pub(super) async fn create_with_config(
        store: Arc<dyn ObjectStore>,
        prefix: Path,
        config: NativeCatalogConfig,
        immediate: bool,
        anchor: Option<Arc<str>>,
        dataset: &str,
    ) -> CatalogResult<Self> {
        HeadLog::create(Arc::clone(&store), prefix.clone(), config.log)
            .await
            .map_err(invalid)?;
        Self::open_with_config(store, prefix, config, immediate, anchor, dataset).await
    }
    /// Replay committed records, then fence the previous writer with the next
    /// lease. An `immediate` log commits every change when it is made, even
    /// inside a deferred scope. Data paths under `anchor` are recorded relative
    /// to it ([`state_reader::stored_path`]).
    /// # Errors
    /// Returns an error on invalid history, takeover conflict, or storage failure.
    pub(super) async fn open_with_config(
        store: Arc<dyn ObjectStore>,
        prefix: Path,
        config: NativeCatalogConfig,
        immediate: bool,
        anchor: Option<Arc<str>>,
        dataset: &str,
    ) -> CatalogResult<Self> {
        let (inner, opened) = Inner::open(
            store,
            prefix,
            config,
            immediate,
            anchor,
            Arc::from(dataset),
            None,
        )
        .await?;
        let data_writes = match config.access {
            Access::Writer => crate::catalog::DataWriteGate::open(),
            Access::Replica => crate::catalog::DataWriteGate::closed(),
        };
        Ok(Self {
            opened,
            folder: String::new(),
            data_writes,
            inner: Arc::new(StateSlot {
                available: Notify::new(),
                reserved: std::sync::atomic::AtomicBool::new(false),
                inner: Mutex::new(Some(inner)),
            }),
        })
    }
}

impl Inner {
    /// Replay the log at `prefix` and take it over with the next lease. With
    /// `held`, the lease this process wrote under, a log that has a newer one
    /// belongs to another instance: it is not taken over, and the error is
    /// [`FENCED_ON_REOPEN`].
    pub(super) async fn open(
        store: Arc<dyn ObjectStore>,
        prefix: Path,
        config: NativeCatalogConfig,
        immediate: bool,
        anchor: Option<Arc<str>>,
        dataset: Arc<str>,
        held: Option<u64>,
    ) -> CatalogResult<(Self, OpenSummary)> {
        let (reader, opened) = Reader::open(Arc::clone(&store), prefix.clone())
            .await
            .map_err(invalid)?;
        let replayed = replay_state(opened)?;
        if config.access == Access::Replica {
            return Ok(Self::replica(
                store, prefix, config, immediate, anchor, dataset, reader, replayed,
            ));
        }
        let mut replayed = replayed;
        if let Some(held) = held
            && reader.state().head.lease > held
        {
            return Err(CatalogError::InvalidOperationNoSource {
                message: FENCED_ON_REOPEN.to_string(),
            });
        }
        let generation = reader
            .state()
            .head
            .lease
            .checked_add(1)
            .ok_or_else(|| invalid("catalog lease exhausted"))?;
        let mut writer =
            Writer::take_over(Arc::clone(&store), prefix.clone(), config.log, generation)
                .await
                .map_err(invalid)?;
        writer.set_attempt_horizon(config.attempt_horizon);
        // Takeover fences the old writer. If it committed during replay,
        // rebuild from that fenced head before acknowledging the open.
        if writer.state().head.commit != reader.state().head.commit
            || writer.state().head.incarnation != reader.state().head.incarnation
        {
            let (fenced, opened) = Reader::open(Arc::clone(&store), prefix.clone())
                .await
                .map_err(invalid)?;
            if fenced.state().head != writer.state().head
                || fenced.state().version != writer.state().version
            {
                return Err(invalid(
                    "catalog changed during takeover; reopen the catalog",
                ));
            }
            replayed = replay_state(opened)?;
        }
        let Replayed {
            state,
            floor_state,
            since_floor,
        } = replayed;
        let opened = OpenSummary {
            replayed: since_floor.len(),
            lease: generation,
        };
        Ok((
            Self {
                collector: Arc::new(tokio::sync::Mutex::new(Collector::new(
                    Arc::clone(&store),
                    prefix.clone(),
                    config.retain_below_floor,
                ))),
                reader: DatasetStateReader::from_state(state, store, prefix).with_anchor(anchor),
                head: Head::Writer(writer),
                stop: None,
                recovering: false,
                dataset,
                config,
                floor_state,
                since_floor,
                staged_checkpoint: 0,
                pending: Vec::new(),
                immediate,
            },
            opened,
        ))
    }
    /// Whether a background task has to bring this log back.
    pub(super) fn needs_recovery(&self) -> bool {
        !self.recovering
            && matches!(
                self.stop,
                Some(Stop::Unresolved | Stop::Fenced { reloaded: false })
            )
    }
    /// Bring this log back from [`Stop::Unresolved`] by reopening it, or reload
    /// the committed state of a fenced one.
    pub(super) async fn recover(&mut self) -> CatalogResult<()> {
        match self.stop {
            Some(Stop::Unresolved) => {
                let held = self.writer()?.state().head.lease;
                let reopened = Self::open(
                    Arc::clone(&self.reader.store),
                    self.reader.prefix.clone(),
                    self.config,
                    self.immediate,
                    self.reader.anchor.clone(),
                    Arc::clone(&self.dataset),
                    Some(held),
                )
                .await;
                match reopened {
                    Ok((reopened, _)) => {
                        // Changes buffered but never committed were never
                        // acknowledged; the reopened state is what committed.
                        *self = reopened;
                        self.resolve_decided_transactions().await?;
                        tracing::info!(
                            "{} read its state back after an unconfirmed change and accepts changes again.",
                            subject(&self.dataset)
                        );
                        Ok(())
                    }
                    Err(CatalogError::InvalidOperationNoSource { message })
                        if message == FENCED_ON_REOPEN =>
                    {
                        self.stop = Some(Stop::Fenced { reloaded: false });
                        tracing::info!("{}", fenced_message(&self.dataset));
                        self.reload_committed().await
                    }
                    Err(error) => Err(error),
                }
            }
            Some(Stop::Fenced { reloaded: false }) => self.reload_committed().await,
            _ => Ok(()),
        }
    }
    /// Apply the recorded outcome of every transaction across datasets the
    /// reopened log holds prepared, as opening the catalog does: a `Resolve`
    /// that did not land (its commit's outcome was unknown) would otherwise keep
    /// the transaction's tables reserved until a restart. A transaction with no
    /// recorded outcome is left to its coordinator.
    pub(super) async fn resolve_decided_transactions(&mut self) -> CatalogResult<()> {
        let prepared: Vec<(String, Vec<String>)> = self
            .reader
            .state
            .prepared
            .iter()
            .map(|(txn, prepared)| (txn.clone(), prepared.footprint.iter().cloned().collect()))
            .collect();
        if prepared.is_empty() {
            return Ok(());
        }
        let transactions = transactions_beside(&self.reader.prefix, self.dataset.is_empty());
        for (txn, footprint) in prepared {
            let Some(commit) =
                super::transactions::read_decision(&self.reader.store, &transactions, &txn).await?
            else {
                continue;
            };
            self.commit_now(Action::new(Kind::Resolve(proto::Resolve {
                txn: txn.clone(),
                footprint,
                commit,
            })))
            .await?;
            tracing::info!(
                "{} applied transaction {txn} across datasets ({}) after reading its state back.",
                subject(&self.dataset),
                if commit { "committed" } else { "aborted" }
            );
        }
        Ok(())
    }
    /// Replace the state with the log's committed state, without taking the log
    /// over: a fenced log's reads then show only what committed.
    pub(super) async fn reload_committed(&mut self) -> CatalogResult<()> {
        let (_, opened) = Reader::open(Arc::clone(&self.reader.store), self.reader.prefix.clone())
            .await
            .map_err(invalid)?;
        let replayed = replay_state(opened)?;
        self.reader.state = replayed.state;
        self.floor_state = replayed.floor_state;
        self.since_floor = replayed.since_floor;
        self.pending.clear();
        self.stop = Some(Stop::Fenced { reloaded: true });
        Ok(())
    }
    /// The error a caller gets from a log stopped for `stop`.
    pub(super) fn stopped(&self, stop: Stop) -> CatalogError {
        let message = match stop {
            Stop::Unresolved => format!(
                "{} is reading its state back after a change whose outcome is unknown, so it accepts no change until that finishes; retry shortly.",
                subject(&self.dataset)
            ),
            Stop::Fenced { .. } => fenced_message(&self.dataset),
            Stop::Refused => refused_change_message(&self.dataset, "see the earlier error"),
            Stop::Replica => replica_change_message(&self.dataset),
        };
        CatalogError::InvalidOperationNoSource { message }
    }
    /// `Err` when this log accepts no change.
    pub(super) fn ensure_accepting(&self) -> CatalogResult<()> {
        match self.stop {
            None => Ok(()),
            Some(stop) => Err(self.stopped(stop)),
        }
    }
}

impl Inner {
    /// A log opened as a replica, from what `reader` replayed.
    #[expect(clippy::too_many_arguments, reason = "the fields of an opened log")]
    fn replica(
        store: Arc<dyn ObjectStore>,
        prefix: Path,
        config: NativeCatalogConfig,
        immediate: bool,
        anchor: Option<Arc<str>>,
        dataset: Arc<str>,
        reader: Reader,
        replayed: Replayed,
    ) -> (Self, OpenSummary) {
        let Replayed {
            state,
            floor_state,
            since_floor,
        } = replayed;
        let opened = OpenSummary {
            replayed: since_floor.len(),
            lease: reader.state().head.lease,
        };
        (
            Self {
                collector: Arc::new(tokio::sync::Mutex::new(Collector::new(
                    Arc::clone(&store),
                    prefix.clone(),
                    config.retain_below_floor,
                ))),
                reader: DatasetStateReader::from_state(state, store, prefix).with_anchor(anchor),
                head: Head::Replica(reader),
                stop: Some(Stop::Replica),
                recovering: false,
                dataset,
                config,
                floor_state,
                since_floor,
                staged_checkpoint: 0,
                pending: Vec::new(),
                immediate,
            },
            opened,
        )
    }

    /// The head writer, which only the instance that writes the log has.
    pub(super) fn writer(&self) -> CatalogResult<&Writer> {
        match &self.head {
            Head::Writer(writer) => Ok(writer),
            Head::Replica(_) => Err(self.stopped(Stop::Replica)),
        }
    }

    pub(super) fn writer_mut(&mut self) -> CatalogResult<&mut Writer> {
        match &mut self.head {
            Head::Writer(writer) => Ok(writer),
            Head::Replica(_) => Err(replica_error(&self.dataset)),
        }
    }

    /// On a replica, apply the commits the writer made since this log last
    /// read its head: one conditional GET when nothing changed. Returns whether
    /// the state moved. The writer's own state is always current.
    pub(super) async fn follow(&mut self) -> CatalogResult<bool> {
        let Head::Replica(reader) = &mut self.head else {
            return Ok(false);
        };
        match reader.poll().await.map_err(invalid)? {
            Poll::Unchanged => Ok(false),
            Poll::Records(entries) => {
                let moved = !entries.is_empty();
                for entry in entries {
                    for payload in Group::decode(entry.record.payload)
                        .map_err(invalid)?
                        .payloads
                    {
                        let record = Record::decode(payload).map_err(invalid)?;
                        self.reader.state.apply(&record).map_err(invalid)?;
                    }
                }
                Ok(moved)
            }
            Poll::Reopen => {
                // The writer checkpointed past this replica's cursor and the
                // records in between may be collected: read the log again.
                let (reader, opened) =
                    Reader::open(Arc::clone(&self.reader.store), self.reader.prefix.clone())
                        .await
                        .map_err(invalid)?;
                let replayed = replay_state(opened)?;
                self.reader.state = replayed.state;
                self.floor_state = replayed.floor_state;
                self.since_floor = replayed.since_floor;
                self.head = Head::Replica(reader);
                Ok(true)
            }
        }
    }
}

fn replica_error(dataset: &str) -> CatalogError {
    CatalogError::InvalidOperationNoSource {
        message: replica_change_message(dataset),
    }
}

pub(super) struct Replayed {
    pub(super) state: DatasetState,
    pub(super) floor_state: DatasetState,
    pub(super) since_floor: Vec<(u64, Vec<Record>)>,
}

pub(super) fn replay_state(opened: Opened) -> CatalogResult<Replayed> {
    let mut state = DatasetState::default();
    if let Some(root) = opened.checkpoint_root {
        state
            .apply(&Record::decode(root).map_err(invalid)?)
            .map_err(invalid)?;
    }
    let floor_state = state.clone();
    let mut since_floor = Vec::with_capacity(opened.records.len());
    for entry in opened.records {
        let mut records = Vec::new();
        for payload in Group::decode(entry.record.payload)
            .map_err(invalid)?
            .payloads
        {
            let record = Record::decode(payload).map_err(invalid)?;
            state.apply(&record).map_err(invalid)?;
            records.push(record);
        }
        since_floor.push((entry.commit, records));
    }
    if !state.unknown.is_empty() {
        return Err(invalid("unconverted actions in native catalog history"));
    }
    Ok(Replayed {
        state,
        floor_state,
        since_floor,
    })
}

impl Inner {
    /// Checkpoint at the newest sealed batch once enough batches are listed. The
    /// root is the floor state with every record up to that batch end applied.
    pub(super) async fn maybe_checkpoint(&mut self) -> CatalogResult<()> {
        let Head::Writer(writer) = &self.head else {
            return Ok(());
        };
        let head = &writer.state().head;
        let Some(&at) = head.batches.last() else {
            return Ok(());
        };
        let floor = head.floor.max(self.staged_checkpoint);
        let listed = head.batches.iter().filter(|&&end| end > floor).count();
        if at <= floor || listed < self.config.checkpoint_after_batches {
            return Ok(());
        }
        // Fold the records up to `at` into the floor state in place. They leave
        // `since_floor` even if the checkpoint does not land: the floor state now
        // holds them, and a later checkpoint is at `at` or past it.
        let folded = self
            .since_floor
            .partition_point(|(commit, _)| *commit <= at);
        for (_, records) in self.since_floor.drain(..folded) {
            for record in &records {
                if let Err(error) = self.floor_state.apply_in_place(record) {
                    // These records already applied to the live state from the
                    // same floor, so the floor state no longer describes the log.
                    self.stop = Some(Stop::Refused);
                    tracing::error!(
                        "{}",
                        refused_change_message(&self.dataset, &error.to_string())
                    );
                    return Err(invalid(error));
                }
            }
        }
        let root = cayenne_metastore_actions::state_as_root(&self.floor_state).encode_to_vec();
        self.writer_mut()?
            .publish_checkpoint(at, root.into())
            .await
            .map_err(invalid)?;
        self.staged_checkpoint = at;
        self.collect();
        Ok(())
    }
    /// Whether a change to `table_id` is applied here but not yet committed.
    pub(super) fn has_uncommitted(&self, table_id: &str) -> bool {
        self.pending
            .iter()
            .any(|buffer| buffer.tables.contains(table_id) || buffer.tables.contains(""))
    }
    /// Whether an unresolved transaction across datasets reserves `table_id`.
    pub(super) fn reserved(&self, table_id: &str) -> bool {
        self.reader
            .state
            .prepared
            .values()
            .any(|prepared| prepared.footprint.contains(table_id))
    }
    /// Delete the objects below the floor that no reader can still need, on a
    /// background task, unless a collection of this log is already running.
    /// A failed collection leaves them for the next one.
    pub(super) fn collect(&self) {
        let Ok(mut collector) = Arc::clone(&self.collector).try_lock_owned() else {
            return;
        };
        tokio::spawn(async move {
            match collector.collect().await {
                Ok(Some(collected)) => tracing::trace!(
                    "Cayenne object-native metastore deleted {} log objects below commit {}",
                    collected.deleted,
                    collected.floor
                ),
                Ok(None) => {}
                Err(error) => tracing::debug!(
                    "Cayenne object-native metastore left log objects below the floor for the next collection: {error}"
                ),
            }
        });
    }
    pub(super) fn table(&self, id: &str) -> CatalogResult<&cayenne_metastore_actions::TableState> {
        self.reader
            .state
            .tables
            .get(id)
            .ok_or_else(|| CatalogError::TableNotFound {
                table_name: id.to_string(),
            })
    }
    pub(super) fn check_snapshot(&self, id: &str, expected: &str) -> CatalogResult<()> {
        let current = &self.table(id)?.current_snapshot_id;
        if current != expected {
            return Err(CatalogError::SnapshotReplaced {
                table_id: id.to_string(),
                replaced: expected.to_string(),
                current: current.clone(),
            });
        }
        Ok(())
    }
    pub(super) async fn blob(&self, bytes: &[u8]) -> CatalogResult<ObjRef> {
        let reference = ObjRef::of(bytes);
        let path = self
            .reader
            .prefix
            .clone()
            .join("objects")
            .join("blobs")
            .join(blake3::hash(bytes).to_hex().as_str());
        self.reader
            .store
            .put(&path, bytes::Bytes::copy_from_slice(bytes).into())
            .await
            .map_err(invalid)?;
        Ok(reference)
    }
    /// The action that sets `stats`, with its blobs stored.
    pub(super) async fn table_stats_action(
        &self,
        stats: &TableStatistics,
    ) -> CatalogResult<Action> {
        let blob = Some(self.blob(&stats.statistics_blob).await?);
        let ndv = match &stats.ndv_sketches {
            Some(bytes) => Some(self.blob(bytes).await?),
            None => None,
        };
        Ok(Action::new(Kind::SetTableStats(proto::SetTableStats {
            table_id: stats.table_id.clone(),
            blob,
            ndv,
            num_rows: stats.num_rows,
            exact: stats.num_rows_exact,
        })))
    }
    pub(super) async fn commit(&mut self, actions: Vec<Action>) -> CatalogResult<()> {
        self.ensure_accepting()?;
        let mut actions = actions;
        self.stamp(&mut actions);
        let record = Record { actions };
        // Applied without a copy of the state. A refused record is a guard this
        // catalog's own checks should have caught, and leaves the state partly
        // changed, so the log stops until the process restarts.
        if let Err(error) = self.reader.state.apply_in_place(&record) {
            self.stop = Some(Stop::Refused);
            let message = refused_change_message(&self.dataset, &error.to_string());
            tracing::error!("{message}");
            return Err(CatalogError::InvalidOperationNoSource { message });
        }
        self.buffer(record).await
    }
    /// Set the commit time on the actions that keep one.
    fn stamp(&self, actions: &mut [Action]) {
        let timestamp = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
        for action in actions {
            match action.kind.as_mut() {
                Some(Kind::AddInlinedData(row)) => {
                    row.created_at.clone_from(&timestamp);
                }
                Some(Kind::AddInlinedDelete(row)) => {
                    row.created_at = self
                        .reader
                        .state
                        .tables
                        .get(&row.table_id)
                        .and_then(|table| table.inlined_deletes.get(&row.id))
                        .filter(|_| row.idempotent)
                        .map_or_else(|| timestamp.clone(), |existing| existing.created_at.clone());
                }
                _ => {}
            }
        }
    }
}

impl Domain {
    /// The runtime's bookkeeping value under `key` (a CDC position, a dataset
    /// checkpoint), kept in the catalog beside the data it describes.
    /// # Errors
    /// Returns an error when the catalog must be reopened.
    pub async fn sidecar_value(&self, key: &str) -> CatalogResult<Option<Vec<u8>>> {
        let inner = self.read().await?;
        Ok(inner
            .reader
            .state
            .positions
            .get(&(key.to_string(), String::new()))
            .cloned())
    }
    /// The counters kept under `key` by [`Self::advance_sidecar_counters`], by name.
    /// # Errors
    /// Returns an error when the catalog must be reopened.
    pub async fn sidecar_counters(&self, key: &str) -> CatalogResult<Vec<(String, i64)>> {
        let inner = self.read().await?;
        let start = (key.to_string(), "\0".to_string());
        inner
            .reader
            .state
            .positions
            .range(start..)
            .take_while(|((source, _), _)| source == key)
            .map(|((_, name), value)| {
                let bytes: [u8; 8] = value.as_slice().try_into().map_err(|_| {
                    invalid(format!("counter '{name}' under '{key}' is not 8 bytes"))
                })?;
                Ok((name.clone(), i64::from_be_bytes(bytes)))
            })
            .collect()
    }
    /// Change the value under `key` and advance its counters in the same commit.
    /// # Errors
    /// Returns an error when a counter name is empty or the commit is not accepted.
    pub(super) async fn set_sidecar_value_and_counters(
        &self,
        key: &str,
        value: ValueChange,
        counters: &[(String, i64)],
    ) -> CatalogResult<()> {
        let value = match value {
            ValueChange::Keep => None,
            ValueChange::Set(value) => Some(Some(value)),
            ValueChange::Remove => Some(None),
        };
        let mut highest = std::collections::BTreeMap::<&str, i64>::new();
        for (name, count) in counters {
            if name.is_empty() {
                return Err(invalid(format!("a counter under '{key}' has no name")));
            }
            let entry = highest.entry(name.as_str()).or_insert(*count);
            *entry = (*entry).max(*count);
        }
        let mut inner = self.inner.take().await;
        let mut actions = Vec::new();
        if let Some(value) = value
            && inner
                .reader
                .state
                .positions
                .get(&(key.to_string(), String::new()))
                != value.as_ref()
        {
            actions.push(Action::new(Kind::AdvanceSourcePosition(
                proto::AdvanceSourcePosition {
                    source: key.to_string(),
                    partition: String::new(),
                    clear: value.is_none(),
                    position: value.unwrap_or_default(),
                },
            )));
        }
        for (name, count) in highest {
            let stored = inner
                .reader
                .state
                .positions
                .get(&(key.to_string(), name.to_string()))
                .and_then(|bytes| <[u8; 8]>::try_from(bytes.as_slice()).ok())
                .map(i64::from_be_bytes);
            if stored.is_some_and(|stored| stored >= count) {
                continue;
            }
            actions.push(Action::new(Kind::AdvanceSourcePosition(
                proto::AdvanceSourcePosition {
                    source: key.to_string(),
                    partition: name.to_string(),
                    position: count.to_be_bytes().to_vec(),
                    clear: false,
                },
            )));
        }
        if actions.is_empty() && inner.stop.is_none() {
            return Ok(());
        }
        inner.commit(actions).await
    }
    /// Set (`Some`) or remove (`None`) the bookkeeping value under `key`. Setting
    /// the value it already holds commits nothing.
    /// # Errors
    /// Returns an error when the commit is not accepted.
    pub async fn set_sidecar_value(&self, key: &str, value: Option<Vec<u8>>) -> CatalogResult<()> {
        // A saved position commits in the same record as every buffered change
        // before it. Inside a deferred scope it joins the record the scope's
        // `commit_pending` commits; otherwise it commits now.
        let mut inner = self.inner.take().await;
        if inner.stop.is_none()
            && inner
                .reader
                .state
                .positions
                .get(&(key.to_string(), String::new()))
                == value.as_ref()
        {
            return Ok(());
        }
        let clear = value.is_none();
        inner
            .commit(vec![Action::new(Kind::AdvanceSourcePosition(
                proto::AdvanceSourcePosition {
                    source: key.to_string(),
                    partition: String::new(),
                    position: value.unwrap_or_default(),
                    clear,
                },
            ))])
            .await
    }
    pub(crate) async fn create_identified(
        &self,
        mut options: CreateTableOptions,
        id: String,
        snapshot: String,
    ) -> CatalogResult<String> {
        if !crate::metastore_layout::is_object_native_data_path(&options.base_path) {
            return Err(CatalogError::InvalidOperationNoSource {
                message: crate::metastore_layout::object_native_data_path_refusal(
                    &options.table_name,
                ),
            });
        }
        options.schema = arrow_tools::map_entries::conforming_schema(options.schema);
        crate::cayenne_catalog::validate_create_table_options(&options)?;
        let mut inner = self.inner.take().await;
        if inner
            .reader
            .state
            .tables
            .values()
            .any(|table| table.name == options.table_name)
        {
            return inner.reopen_table(&options).await;
        }
        if !crate::is_object_store_path(&options.base_path) && !options.vortex_config.memory_mode {
            let table_root = std::path::PathBuf::from(&options.base_path).join(&id);
            tokio::fs::create_dir_all(table_root.join(&snapshot)).await?;
            tokio::task::spawn_blocking(move || {
                std::fs::File::open(table_root).and_then(|file| file.sync_all())
            })
            .await??;
        }
        let schema = crate::cayenne_catalog::serialize_schema_ipc_base64(options.schema.as_ref())?;
        let schema = Some(inner.blob(schema.as_bytes()).await?);
        let primary_key_json = if options.primary_key.is_empty() {
            None
        } else {
            Some(serde_json::to_string(&options.primary_key).map_err(invalid)?)
        };
        let vortex_config_json =
            Some(serde_json::to_string(&options.vortex_config).map_err(invalid)?);
        let (path, path_is_relative) =
            state_reader::stored_path(inner.reader.anchor.as_deref(), &options.base_path);
        inner
            .commit(vec![Action::new(Kind::CreateTable(proto::CreateTable {
                table_id: id.clone(),
                name: options.table_name,
                path,
                path_is_relative,
                schema,
                primary_key_json,
                on_conflict_json: options.on_conflict.as_ref().map(ToString::to_string),
                partition_column: options.partition_column,
                vortex_config_json,
                current_snapshot_id: snapshot,
                seq: 0,
            }))])
            .await?;
        Ok(id)
    }
    pub(super) async fn persist_schema(
        &self,
        table_id: &str,
        schema: &SchemaRef,
        drop_statistics: bool,
    ) -> CatalogResult<()> {
        let mut inner = self.inner.take().await;
        if !inner.reader.state.tables.contains_key(table_id) {
            return Ok(());
        }
        let schema = crate::cayenne_catalog::serialize_schema_ipc_base64(schema.as_ref())?;
        let reference = inner.blob(schema.as_bytes()).await?;
        let mut batch = vec![Action::new(Kind::SetSchema(proto::SetSchema {
            table_id: table_id.to_string(),
            schema: Some(reference),
        }))];
        if drop_statistics {
            batch.extend([
                actions::clear_table_stats(table_id),
                actions::remove_file_stats(table_id, proto::Select::All, &[]),
                Action::new(Kind::ClearColdStats(actions::table(table_id))),
            ]);
        }
        inner.commit(batch).await
    }
}

impl Inner {
    /// Reopen an existing table under `options`, then check its live snapshots'
    /// data files on disk, as the SQLite catalog does.
    async fn reopen_table(&mut self, options: &CreateTableOptions) -> CatalogResult<String> {
        let table_id = self.reopen_table_settings(options).await?;
        let table = self.reader.get_table(&options.table_name).await?;
        crate::cayenne_catalog::verify_stored_data_files_in(
            &table,
            options.vortex_config.memory_mode,
            options.vortex_config.settings_change,
            async { Ok(self.reader.get_all_snapshot_sequences(&table.table_id)) },
            |snapshot_id| {
                std::future::ready(Ok(self
                    .reader
                    .get_snapshot_files(&table.table_id, &snapshot_id)))
            },
        )
        .await?;
        Ok(table_id)
    }
    async fn reopen_table_settings(
        &mut self,
        options: &CreateTableOptions,
    ) -> CatalogResult<String> {
        use crate::cayenne_catalog::{
            describe_setting_changes, enables_pk_conflict_detection, identity_setting_changes,
        };
        use crate::metadata::SettingsChangeAction;
        let mut stored = self.reader.get_table(&options.table_name).await?;
        let changes = identity_setting_changes(&stored, options);
        if !changes.is_empty() {
            if options.vortex_config.settings_change == SettingsChangeAction::Rebuild {
                return Err(CatalogError::SettingsChangeRequiresRebuild {
                    table_name: options.table_name.clone(),
                    changes: describe_setting_changes(&changes),
                });
            }
            if enables_pk_conflict_detection(&stored, options) {
                return Err(
                    if options.vortex_config.settings_change
                        == SettingsChangeAction::HoldUntilSourceReadable
                    {
                        CatalogError::PkConflictDetectionChangeHeld {
                            table_name: options.table_name.clone(),
                        }
                    } else {
                        CatalogError::PkConflictDetectionEnabledOnExistingTable {
                            table_name: options.table_name.clone(),
                        }
                    },
                );
            }
            return Ok(stored.table_id);
        }
        let normalize = |location: &Option<String>| {
            location
                .as_deref()
                .map(|l| l.trim().trim_end_matches('/').to_string())
                .filter(|l| !l.is_empty())
        };
        let stored_location = normalize(&stored.vortex_config.cold_tier_location);
        let configured_location = normalize(&options.vortex_config.cold_tier_location);
        if let Some(location) = &stored_location
            && configured_location.as_ref() != Some(location)
            && !self.table(&stored.table_id)?.cold.is_empty()
        {
            return Err(CatalogError::ColdTierLocationChanged {
                table_name: stored.table_name.clone(),
                stored: location.clone(),
                configured: configured_location.unwrap_or_else(|| "<unset>".to_string()),
            });
        }
        let original_config = serde_json::to_string(&stored.vortex_config).map_err(invalid)?;
        let original_conflict = stored.on_conflict.as_ref().map(ToString::to_string);
        let new_vc = &options.vortex_config;
        // Layout.
        stored
            .vortex_config
            .sort_columns
            .clone_from(&new_vc.sort_columns);
        stored.vortex_config.sort_columns_origin = new_vc.sort_columns_origin;
        stored
            .vortex_config
            .cluster_by
            .clone_from(&new_vc.cluster_by);
        stored
            .vortex_config
            .compression_strategy
            .clone_from(&new_vc.compression_strategy);
        stored.vortex_config.delta_encoding = new_vc.delta_encoding;
        stored
            .vortex_config
            .shard_key_columns
            .clone_from(&new_vc.shard_key_columns);
        stored.vortex_config.pk_conflict_detection = new_vc.pk_conflict_detection;
        stored.on_conflict.clone_from(&options.on_conflict);

        // Runtime.
        stored.vortex_config.scan_concurrency = new_vc.scan_concurrency;
        stored
            .vortex_config
            .cold_tier_location
            .clone_from(&new_vc.cold_tier_location);
        stored.vortex_config.cold_target_file_size_mb = new_vc.cold_target_file_size_mb;
        stored.vortex_config.cold_clustering_run_size_mb = new_vc.cold_clustering_run_size_mb;
        stored.vortex_config.cold_tier_warm_max_bytes = new_vc.cold_tier_warm_max_bytes;
        stored.vortex_config.cold_tier_warm_max_files = new_vc.cold_tier_warm_max_files;
        stored.vortex_config.cold_tier_background_interval_ms =
            new_vc.cold_tier_background_interval_ms;

        let config = serde_json::to_string(&stored.vortex_config).map_err(invalid)?;
        let conflict = stored.on_conflict.as_ref().map(ToString::to_string);
        let mut batch = Vec::new();
        if config != original_config || conflict != original_conflict {
            batch.push(Action::new(Kind::SetConfig(proto::SetConfig {
                table_id: stored.table_id.clone(),
                vortex_config_json: Some(config),
                on_conflict_json: conflict,
            })));
        }
        if !options.vortex_config.schema_evolution.is_disabled() {
            use arrow_tools::schema_evolution::{EvolutionContext, SchemaEvolution, classify};
            let context = EvolutionContext {
                constraint_columns: &stored.primary_key,
            };
            if let SchemaEvolution::Widening(plan) =
                classify(&stored.schema, &options.schema, &context)
                && options.vortex_config.schema_evolution.allows(&plan)
            {
                let schema = crate::cayenne_catalog::serialize_schema_ipc_base64(
                    plan.evolved_schema.as_ref(),
                )?;
                let schema = self.blob(schema.as_bytes()).await?;
                batch.push(Action::new(Kind::SetSchema(proto::SetSchema {
                    table_id: stored.table_id.clone(),
                    schema: Some(schema),
                })));
                if plan.changes_decimal_scale() {
                    batch.extend([
                        actions::clear_table_stats(&stored.table_id),
                        actions::remove_file_stats(&stored.table_id, proto::Select::All, &[]),
                        Action::new(Kind::ClearColdStats(actions::table(&stored.table_id))),
                    ]);
                }
            }
        }
        if !batch.is_empty() {
            self.commit(batch).await?;
        }
        Ok(stored.table_id)
    }
}
