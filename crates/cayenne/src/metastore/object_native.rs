// Copyright 2024-2026 The Spice.ai OSS Authors
/*
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

//! SQL-free catalog backed by a catalog-wide conditional-write head log.

use super::{actions, state_reader::ShadowCatalog};
use crate::{
    catalog::{CatalogError, CatalogResult, MetadataCatalog, SnapshotSequenceCommit},
    metadata::{
        ColdTierFile, CreateTableOptions, DeleteFile, IndexRunRecord, InlinedData,
        InlinedDataStats, InlinedDelete, PartitionMetadata, SnapshotFile, SnapshotFileStatistics,
        TableMetadata, TableStatistics, TableStorageStats,
    },
};
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use cayenne_metastore_actions::{Action, DatasetState, Kind, ObjRef, Record, proto};
use object_store::{ObjectStore, ObjectStoreExt, path::Path};
use object_store_occ::headlog::{Group, HeadLog, HeadLogConfig, Opened, Outcome, Reader, Writer};
use parking_lot::Mutex;
use prost::Message;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Notify;

/// A serial writer and its last acknowledged state. No database is used.
pub struct ObjectNativeCatalog {
    inner: StateSlot,
    opened: OpenSummary,
}

/// What opening the catalog found, for the startup log.
#[derive(Clone, Copy, Debug)]
struct OpenSummary {
    checkpoint: u64,
    replayed: usize,
    tables: usize,
    lease: u64,
}
struct Inner {
    reader: ShadowCatalog,
    writer: Writer,
    poisoned: bool,
    config: NativeCatalogConfig,
    /// The state with every record up to the last checkpoint this catalog
    /// attempted (or opened from) applied, and the records committed after it,
    /// from which the next checkpoint root is built.
    floor_state: DatasetState,
    since_floor: Vec<(u64, Vec<Record>)>,
    /// The commit of the last checkpoint staged; its floor publishes with the next commit.
    staged_checkpoint: u64,
    /// Changes accepted and visible on this node but not yet committed, one
    /// buffer per set of tables they touch, oldest first. A buffer commits as one
    /// record at an acknowledgement point for one of its tables, or when it
    /// reaches the size or age bound; another table's commit never publishes it.
    pending: Vec<Pending>,
}

/// Buffered changes for a set of tables. Actions on different tables commute,
/// so buffers can commit in any order and the log still replays to the state
/// this node applied. A record that touches several tables keeps them in one
/// buffer, so it still commits whole.
struct Pending {
    /// Table ids, and `""` for changes no table owns.
    tables: std::collections::BTreeSet<String>,
    actions: Vec<Action>,
    bytes: usize,
    since: std::time::Instant,
}

/// Commit buffered changes once they are this large or this old, even before an
/// acknowledgement point asks for them.
const PENDING_MAX_BYTES: usize = 256 * 1024;
const PENDING_MAX_AGE: std::time::Duration = std::time::Duration::from_secs(1);

/// Head-log tuning for a native catalog.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NativeCatalogConfig {
    /// The log's budgets; every writer of one log must use the same values.
    pub log: HeadLogConfig,
    /// Sealed batches listed in the head before the catalog checkpoints at the
    /// newest one.
    pub checkpoint_after_batches: usize,
    /// How long checkpoints keep the evidence that resolves an uncertain commit.
    pub attempt_horizon: std::time::Duration,
}

impl Default for NativeCatalogConfig {
    fn default() -> Self {
        Self {
            log: HeadLogConfig::default(),
            // Small for the prototype, so short CDC runs checkpoint.
            checkpoint_after_batches: 1,
            attempt_horizon: object_store_occ::headlog::DEFAULT_ATTEMPT_HORIZON,
        }
    }
}
fn invalid(error: impl std::fmt::Display) -> CatalogError {
    CatalogError::InvalidOperationNoSource {
        message: error.to_string(),
    }
}
// The slot transfers ownership, so storage I/O never holds a mutex guard.
// A cancelled operation returns its state, including any uncertainty marker.
struct StateSlot {
    inner: Mutex<Option<Inner>>,
    available: Notify,
}
struct StateLease<'a> {
    slot: &'a StateSlot,
    inner: Option<Inner>,
}
impl StateSlot {
    async fn take(&self) -> StateLease<'_> {
        loop {
            let notified = self.available.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let inner = self.inner.lock().take();
            if let Some(inner) = inner {
                return StateLease {
                    slot: self,
                    inner: Some(inner),
                };
            }
            notified.await;
        }
    }
}
impl std::ops::Deref for StateLease<'_> {
    type Target = Inner;
    fn deref(&self) -> &Inner {
        match &self.inner {
            Some(inner) => inner,
            None => unreachable!("a catalog state lease owns its state until drop"),
        }
    }
}
impl std::ops::DerefMut for StateLease<'_> {
    fn deref_mut(&mut self) -> &mut Inner {
        match &mut self.inner {
            Some(inner) => inner,
            None => unreachable!("a catalog state lease owns its state until drop"),
        }
    }
}
impl Drop for StateLease<'_> {
    fn drop(&mut self) {
        *self.slot.inner.lock() = self.inner.take();
        self.slot.available.notify_one();
    }
}
impl ObjectNativeCatalog {
    async fn read(&self) -> CatalogResult<StateLease<'_>> {
        let inner = self.inner.take().await;
        if inner.poisoned {
            return Err(invalid(
                "catalog publication outcome is uncertain; reopen before reading commit evidence",
            ));
        }
        Ok(inner)
    }

    /// Create an empty catalog at a new prefix.
    /// # Errors
    /// Returns an error when conditional writes are unsupported or the prefix exists.
    pub async fn create(store: Arc<dyn ObjectStore>, prefix: Path) -> CatalogResult<Self> {
        Self::create_with_config(store, prefix, NativeCatalogConfig::default()).await
    }
    /// [`Self::create`] with explicit head-log tuning.
    /// # Errors
    /// Returns an error when conditional writes are unsupported or the prefix exists.
    pub async fn create_with_config(
        store: Arc<dyn ObjectStore>,
        prefix: Path,
        config: NativeCatalogConfig,
    ) -> CatalogResult<Self> {
        HeadLog::create(Arc::clone(&store), prefix.clone(), config.log)
            .await
            .map_err(invalid)?;
        Self::open_with_config(store, prefix, config).await
    }
    /// Open the catalog kept in a local directory, creating it on first use. The
    /// directory is an fsync'd object store with conditional writes.
    /// # Errors
    /// Returns an error when the directory cannot be used or the history is invalid.
    pub async fn open_or_create_local(directory: &std::path::Path) -> CatalogResult<Self> {
        tokio::fs::create_dir_all(directory)
            .await
            .map_err(invalid)?;
        let store: Arc<dyn ObjectStore> = Arc::new(
            object_store_occ::LocalConditionalPut::new_durable(directory).map_err(invalid)?,
        );
        Self::open_or_create_at(
            store,
            Path::from("catalog"),
            &directory.display().to_string(),
        )
        .await
    }

    /// Open the catalog at `prefix` in `store`, creating it on first use.
    /// `location` names where it is, for the startup log.
    /// # Errors
    /// Returns an error when the store cannot be used or the history is invalid.
    pub async fn open_or_create_at(
        store: Arc<dyn ObjectStore>,
        prefix: Path,
        location: &str,
    ) -> CatalogResult<Self> {
        let (catalog, verb) = match store.head(&prefix.clone().join("head")).await {
            Ok(_) => (Self::open(store, prefix).await?, "Opened"),
            Err(object_store::Error::NotFound { .. }) => {
                (Self::create(store, prefix).await?, "Created")
            }
            Err(error) => return Err(invalid(error)),
        };
        let OpenSummary {
            checkpoint,
            replayed,
            tables,
            lease,
        } = catalog.opened;
        tracing::info!(
            "{verb} the Cayenne object-native metastore at '{location}': {tables} tables from the checkpoint at commit {checkpoint} plus {replayed} later commits, writing as lease {lease}"
        );
        Ok(catalog)
    }
    /// Replay committed records, then fence the previous writer with the next lease.
    /// # Errors
    /// Returns an error on invalid history, takeover conflict, or storage failure.
    pub async fn open(store: Arc<dyn ObjectStore>, prefix: Path) -> CatalogResult<Self> {
        Self::open_with_config(store, prefix, NativeCatalogConfig::default()).await
    }
    /// [`Self::open`] with explicit head-log tuning.
    /// # Errors
    /// Returns an error on invalid history, takeover conflict, or storage failure.
    pub async fn open_with_config(
        store: Arc<dyn ObjectStore>,
        prefix: Path,
        config: NativeCatalogConfig,
    ) -> CatalogResult<Self> {
        let (reader, opened) = Reader::open(Arc::clone(&store), prefix.clone())
            .await
            .map_err(invalid)?;
        let mut replayed = replay_state(opened)?;
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
            floor,
        } = replayed;
        let opened = OpenSummary {
            checkpoint: floor,
            replayed: since_floor.len(),
            tables: state.tables.len(),
            lease: generation,
        };
        Ok(Self {
            opened,
            inner: StateSlot {
                available: Notify::new(),
                inner: Mutex::new(Some(Inner {
                    reader: ShadowCatalog::from_state(state, store, prefix),
                    writer,
                    poisoned: false,
                    config,
                    floor_state,
                    since_floor,
                    staged_checkpoint: 0,
                    pending: Vec::new(),
                })),
            },
        })
    }
}
struct Replayed {
    state: DatasetState,
    floor_state: DatasetState,
    since_floor: Vec<(u64, Vec<Record>)>,
    floor: u64,
}
fn replay_state(opened: Opened) -> CatalogResult<Replayed> {
    let mut state = DatasetState::default();
    if let Some(root) = opened.checkpoint_root {
        state
            .apply(&Record::decode(root).map_err(invalid)?)
            .map_err(invalid)?;
    }
    let floor_state = state.clone();
    let floor = opened.floor;
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
        floor,
    })
}

impl Inner {
    /// Checkpoint at the newest sealed batch once enough batches are listed. The
    /// root is the floor state with every record up to that batch end applied.
    async fn maybe_checkpoint(&mut self) -> CatalogResult<()> {
        let head = &self.writer.state().head;
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
                    self.poisoned = true;
                    return Err(invalid(error));
                }
            }
        }
        let root = cayenne_metastore_actions::state_as_root(&self.floor_state).encode_to_vec();
        self.writer
            .publish_checkpoint(at, root.into())
            .await
            .map_err(invalid)?;
        self.staged_checkpoint = at;
        Ok(())
    }

    fn table(&self, id: &str) -> CatalogResult<&cayenne_metastore_actions::TableState> {
        self.reader
            .state
            .tables
            .get(id)
            .ok_or_else(|| CatalogError::TableNotFound {
                table_name: id.to_string(),
            })
    }
    fn check_snapshot(&self, id: &str, expected: &str) -> CatalogResult<()> {
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
    async fn blob(&self, bytes: &[u8]) -> CatalogResult<ObjRef> {
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

    async fn commit(&mut self, actions: Vec<Action>) -> CatalogResult<()> {
        if self.poisoned {
            return Err(invalid(
                "catalog requires reopening after an uncertain or fenced commit",
            ));
        }
        let mut actions = actions;
        let timestamp = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
        for action in &mut actions {
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
        let record = Record { actions };
        // Applied without a copy of the state. A refused record is a guard this
        // catalog's own checks should have caught, and leaves the state partly
        // changed, so the catalog stops until it is reopened from the log.
        if let Err(error) = self.reader.state.apply_in_place(&record) {
            self.poisoned = true;
            tracing::error!(
                "Cayenne object-native metastore stopped accepting changes, so its datasets stop updating until spiced restarts: a change was refused: {error}"
            );
            return Err(invalid(error));
        }
        let tables = self.tables_of(&record.actions);
        let bytes = record.encoded_len();
        // Merge every buffer this record shares a table with, oldest first.
        let mut merged = Pending {
            tables: tables.clone(),
            actions: Vec::new(),
            bytes: 0,
            since: std::time::Instant::now(),
        };
        let mut kept = Vec::with_capacity(self.pending.len());
        for buffer in std::mem::take(&mut self.pending) {
            if buffer.tables.is_disjoint(&tables) {
                kept.push(buffer);
            } else {
                merged.tables.extend(buffer.tables);
                merged.actions.extend(buffer.actions);
                merged.bytes += buffer.bytes;
                merged.since = merged.since.min(buffer.since);
            }
        }
        merged.actions.extend(record.actions);
        merged.bytes += bytes;
        // Outside a deferred scope a change is durable when its method returns.
        let due = !crate::catalog::commits_deferred()
            || merged.bytes >= PENDING_MAX_BYTES
            || merged.since.elapsed() >= PENDING_MAX_AGE;
        self.pending = kept;
        if due {
            return self.publish_buffers(vec![merged]).await;
        }
        self.pending.push(merged);
        Ok(())
    }

    /// The tables `actions` change: each action's table, and for a source
    /// position the table of the dataset its key names (`<store>/<dataset>`).
    /// `""` stands for a change no table owns.
    fn tables_of(&self, actions: &[Action]) -> std::collections::BTreeSet<String> {
        actions
            .iter()
            .map(|action| match action.kind.as_ref() {
                Some(Kind::AdvanceSourcePosition(position)) => position
                    .source
                    .split_once('/')
                    .and_then(|(_, dataset)| {
                        self.reader
                            .state
                            .tables
                            .iter()
                            .find(|(_, table)| table.name == dataset)
                            .map(|(id, _)| id.clone())
                    })
                    .unwrap_or_default(),
                _ => action.table_id().unwrap_or_default().to_string(),
            })
            .collect()
    }

    /// Commit the buffers touching `table_id` as one record, with the changes no
    /// table owns: a position whose key names no table cannot wait for an
    /// acknowledgement point of its own.
    async fn flush_table(&mut self, table_id: &str) -> CatalogResult<()> {
        let (due, kept): (Vec<_>, Vec<_>) = std::mem::take(&mut self.pending)
            .into_iter()
            .partition(|buffer| buffer.tables.contains(table_id) || buffer.tables.contains(""));
        self.pending = kept;
        self.publish_buffers(due).await
    }

    /// Commit every buffered change as one record. Nothing that depends on a
    /// buffered change may be acknowledged before this returns `Ok`.
    async fn flush(&mut self) -> CatalogResult<()> {
        if self.poisoned {
            return Err(invalid(
                "catalog requires reopening after an uncertain or fenced commit",
            ));
        }
        let buffers = std::mem::take(&mut self.pending);
        self.publish_buffers(buffers).await
    }

    /// Commit `buffers`, oldest first, as one record.
    async fn publish_buffers(&mut self, buffers: Vec<Pending>) -> CatalogResult<()> {
        if self.poisoned {
            return Err(invalid(
                "catalog requires reopening after an uncertain or fenced commit",
            ));
        }
        if buffers.is_empty() {
            return Ok(());
        }
        let record = Record {
            actions: buffers
                .into_iter()
                .flat_map(|buffer| buffer.actions)
                .collect(),
        };
        // Buffered changes are already applied on this node. If this record does
        // not commit, they must never be acknowledged: the catalog refuses all
        // further work until it is reopened from what did commit.
        self.poisoned = true;
        let attempt = self
            .writer
            .prepare_group(vec![record.encode_to_vec().into()])
            .await
            .map_err(invalid)?;
        // Refuse subsequent mutations unless this exact attempt is accepted.
        match self.publish(&attempt).await? {
            Outcome::Accepted { commit } => {
                self.poisoned = false;
                self.since_floor.push((commit, vec![record]));
                // The commit is acknowledged whatever happens to the checkpoint: a
                // failed one leaves the batch index to grow until a later one lands.
                if let Err(error) = self.maybe_checkpoint().await {
                    tracing::warn!(
                        "Cayenne object-native catalog could not checkpoint, so its history keeps growing until a later checkpoint succeeds: {error}"
                    );
                }
                Ok(())
            }
            outcome => {
                tracing::error!(
                    "Cayenne object-native metastore stopped accepting changes, so its datasets stop updating until spiced restarts: the commit was {outcome:?}"
                );
                Err(invalid(format!(
                    "catalog publication was not accepted: {outcome:?}; reopen before retrying"
                )))
            }
        }
    }

    /// Publish `attempt`, resolving an uncertain outcome by reading the head back
    /// and retrying only the same bytes under the same condition, so a transient
    /// store error neither loses nor duplicates the commit. `Rejected` and
    /// `Fenced` are final: another writer owns the log.
    async fn publish(
        &mut self,
        attempt: &object_store_occ::headlog::PreparedAttempt,
    ) -> CatalogResult<Outcome> {
        const ATTEMPTS: u32 = 8;
        let mut outcome = self.writer.commit(attempt).await;
        for retry in 1..=ATTEMPTS {
            let error = match outcome {
                Ok(Outcome::Unresolved) => "the outcome was unresolved".to_string(),
                Ok(final_outcome) => return Ok(final_outcome),
                Err(error) => error.to_string(),
            };
            let delay = std::time::Duration::from_millis(100 * 2_u64.pow(retry.min(6)));
            tracing::warn!(
                "Cayenne object-native metastore could not confirm a commit ({error}), so it is checking whether the commit landed and retrying it in {} ms (attempt {retry} of {ATTEMPTS})",
                delay.as_millis()
            );
            tokio::time::sleep(delay).await;
            outcome = self.writer.resolve(attempt).await;
        }
        outcome.map_err(invalid)
    }
}

#[async_trait]
impl MetadataCatalog for ObjectNativeCatalog {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    async fn commit_pending(&self) -> CatalogResult<()> {
        self.inner.take().await.flush().await
    }

    async fn commit_pending_for(&self, table_id: &str) -> CatalogResult<()> {
        self.inner.take().await.flush_table(table_id).await
    }

    async fn commit_publications(
        &self,
        publications: &[crate::catalog::CatalogPublication],
    ) -> CatalogResult<()> {
        let mut inner = self.inner.take().await;
        let mut batch = Vec::new();
        for publication in publications {
            batch.extend(inner.conflict_batch(
                publication.delete_files.clone(),
                &publication.table_id,
                &publication.insert_pk_bytes,
                publication.insert_sequence,
                Some(SnapshotSequenceCommit {
                    snapshot_id: publication.snapshot_id.clone(),
                    sequence_number: publication.snapshot_sequence,
                }),
            )?);
            if let Some(tombstone) = &publication.inline_tombstone {
                batch.push(actions::inlined_delete(
                    tombstone,
                    &tombstone.inlined_id,
                    true,
                ));
            }
            if !publication.pending_durable_flips.is_empty() {
                batch.push(actions::set_published(
                    &publication.table_id,
                    proto::Select::Listed,
                    &publication.pending_durable_flips,
                    true,
                ));
            }
            if !publication.dirty_pk_bytes.is_empty() {
                batch.push(Action::new(Kind::MarkDirty(proto::AddKeys {
                    table_id: publication.table_id.clone(),
                    keys: publication
                        .dirty_pk_bytes
                        .iter()
                        .map(|pk| proto::PkSeq {
                            pk: pk.clone(),
                            seq: publication.snapshot_sequence,
                        })
                        .collect(),
                })));
            }
        }
        if batch.is_empty() {
            return Ok(());
        }
        inner.commit(batch).await
    }
    async fn list_pending_write_back(
        &self,
        table_id: &str,
        limit: usize,
        after: Option<&(i64, Vec<u8>)>,
    ) -> CatalogResult<Vec<(Vec<u8>, i64)>> {
        self.read()
            .await?
            .reader
            .list_pending_write_back(table_id, limit, after)
            .await
    }
    async fn clear_pending_write_back(
        &self,
        table_id: &str,
        keys: &[(Vec<u8>, i64)],
    ) -> CatalogResult<()> {
        if keys.is_empty() {
            return Ok(());
        }
        self.inner
            .take()
            .await
            .commit(vec![Action::new(Kind::ClearDelivered(proto::AddKeys {
                table_id: table_id.to_string(),
                keys: keys
                    .iter()
                    .map(|(pk, seq)| proto::PkSeq {
                        pk: pk.clone(),
                        seq: *seq,
                    })
                    .collect(),
            }))])
            .await
    }
    async fn pending_write_back_count(&self, table_id: &str) -> CatalogResult<i64> {
        self.read()
            .await?
            .reader
            .pending_write_back_count(table_id)
            .await
    }
    async fn init(&self) -> CatalogResult<()> {
        self.read().await?.reader.init().await
    }
    async fn shutdown(&self) -> CatalogResult<()> {
        let mut inner = self.inner.take().await;
        inner.flush().await?;
        inner.reader.shutdown().await
    }
    async fn list_table_names(&self) -> CatalogResult<Vec<String>> {
        self.read().await?.reader.list_table_names().await
    }
    async fn get_table(&self, table_name: &str) -> CatalogResult<TableMetadata> {
        self.read().await?.reader.get_table(table_name).await
    }
    async fn get_sequence_number(&self, table_id: &str) -> CatalogResult<i64> {
        self.read()
            .await?
            .reader
            .get_sequence_number(table_id)
            .await
    }
    async fn get_table_delete_files(&self, table_id: &str) -> CatalogResult<Vec<DeleteFile>> {
        self.read()
            .await?
            .reader
            .get_table_delete_files(table_id)
            .await
    }
    async fn get_orphan_eligible_delete_files(
        &self,
        table_id: &str,
        max_sequence: i64,
        limit: usize,
    ) -> CatalogResult<Vec<DeleteFile>> {
        self.read()
            .await?
            .reader
            .get_orphan_eligible_delete_files(table_id, max_sequence, limit)
            .await
    }
    async fn get_insert_records(&self, table_id: &str) -> CatalogResult<HashMap<Box<[u8]>, i64>> {
        self.read().await?.reader.get_insert_records(table_id).await
    }
    async fn get_snapshot_sequence(
        &self,
        table_id: &str,
        snapshot_id: &str,
    ) -> CatalogResult<Option<i64>> {
        self.read()
            .await?
            .reader
            .get_snapshot_sequence(table_id, snapshot_id)
            .await
    }
    async fn get_all_snapshot_sequences(
        &self,
        table_id: &str,
    ) -> CatalogResult<HashMap<String, i64>> {
        self.read()
            .await?
            .reader
            .get_all_snapshot_sequences(table_id)
            .await
    }
    async fn get_partitions(&self, table_id: &str) -> CatalogResult<Vec<PartitionMetadata>> {
        self.read().await?.reader.get_partitions(table_id).await
    }
    async fn get_table_statistics(&self, table_id: &str) -> CatalogResult<Option<TableStatistics>> {
        self.read()
            .await?
            .reader
            .get_table_statistics(table_id)
            .await
    }
    async fn get_snapshot_file_statistics(
        &self,
        table_id: &str,
        snapshot_id: &str,
        file_path: &str,
    ) -> CatalogResult<Option<SnapshotFileStatistics>> {
        self.read()
            .await?
            .reader
            .get_snapshot_file_statistics(table_id, snapshot_id, file_path)
            .await
    }
    async fn get_all_snapshot_files(&self, table_id: &str) -> CatalogResult<Vec<SnapshotFile>> {
        self.read()
            .await?
            .reader
            .get_all_snapshot_files(table_id)
            .await
    }
    async fn get_snapshot_files(
        &self,
        table_id: &str,
        snapshot_id: &str,
    ) -> CatalogResult<Vec<SnapshotFile>> {
        self.read()
            .await?
            .reader
            .get_snapshot_files(table_id, snapshot_id)
            .await
    }
    async fn list_cold_tier_files(&self, table_id: &str) -> CatalogResult<Vec<ColdTierFile>> {
        self.read()
            .await?
            .reader
            .list_cold_tier_files(table_id)
            .await
    }
    async fn get_pk_index(&self, table_id: &str) -> CatalogResult<Option<(String, Vec<u8>)>> {
        self.read().await?.reader.get_pk_index(table_id).await
    }
    async fn list_index_runs(&self, table_id: &str) -> CatalogResult<Vec<IndexRunRecord>> {
        self.read().await?.reader.list_index_runs(table_id).await
    }
    async fn get_inlined_data(&self, table_id: &str) -> CatalogResult<Vec<InlinedData>> {
        self.read().await?.reader.get_inlined_data(table_id).await
    }
    async fn get_inlined_data_for_partition(
        &self,
        table_id: &str,
        partition_key: &str,
    ) -> CatalogResult<Vec<InlinedData>> {
        self.read()
            .await?
            .reader
            .get_inlined_data_for_partition(table_id, partition_key)
            .await
    }
    async fn get_inlined_data_count(&self, table_id: &str) -> CatalogResult<i64> {
        self.read()
            .await?
            .reader
            .get_inlined_data_count(table_id)
            .await
    }
    async fn get_inlined_data_stats(&self, table_id: &str) -> CatalogResult<InlinedDataStats> {
        self.read()
            .await?
            .reader
            .get_inlined_data_stats(table_id)
            .await
    }
    async fn get_inlined_deletes(&self, table_id: &str) -> CatalogResult<Vec<InlinedDelete>> {
        self.read()
            .await?
            .reader
            .get_inlined_deletes(table_id)
            .await
    }
    async fn get_published_inlined_deletes(
        &self,
        table_id: &str,
    ) -> CatalogResult<Vec<InlinedDelete>> {
        self.read()
            .await?
            .reader
            .get_published_inlined_deletes(table_id)
            .await
    }
    async fn get_unpublished_inlined_delete_ids(
        &self,
        table_id: &str,
    ) -> CatalogResult<Vec<String>> {
        self.read()
            .await?
            .reader
            .get_unpublished_inlined_delete_ids(table_id)
            .await
    }
    async fn table_storage_stats(&self, table_id: &str) -> CatalogResult<TableStorageStats> {
        self.read()
            .await?
            .reader
            .table_storage_stats(table_id)
            .await
    }
    async fn create_table(&self, options: CreateTableOptions) -> CatalogResult<String> {
        self.create_identified(
            options,
            uuid::Uuid::now_v7().to_string(),
            uuid::Uuid::now_v7().to_string(),
        )
        .await
    }
    async fn update_table_schema(&self, table_id: &str, schema: &SchemaRef) -> CatalogResult<()> {
        self.persist_schema(table_id, schema, false).await
    }
    async fn update_table_schema_dropping_statistics(
        &self,
        table_id: &str,
        schema: &SchemaRef,
    ) -> CatalogResult<()> {
        self.persist_schema(table_id, schema, true).await
    }
    async fn set_current_snapshot(
        &self,
        table_id: &str,
        replaced_snapshot_id: &str,
        new_snapshot_id: &str,
    ) -> CatalogResult<()> {
        let mut inner = self.inner.take().await;
        inner.check_snapshot(table_id, replaced_snapshot_id)?;
        inner
            .commit(vec![actions::set_current(
                table_id,
                new_snapshot_id,
                Some(replaced_snapshot_id),
            )])
            .await
    }
    async fn increment_sequence_number(&self, table_id: &str) -> CatalogResult<i64> {
        self.reserve_sequence_numbers(table_id, 1).await
    }
    async fn reserve_sequence_numbers(&self, table_id: &str, count: u32) -> CatalogResult<i64> {
        if count == 0 {
            return Err(invalid("reserve_sequence_numbers called with count=0"));
        }
        let mut inner = self.inner.take().await;
        let old = inner.reader.state.tables.get(table_id).ok_or_else(|| invalid(format!("Cannot reserve {count} sequence numbers for table_id '{table_id}': table row does not exist")))?.seq;
        let to = old
            .checked_add(i64::from(count))
            .ok_or_else(|| invalid("sequence number exhausted"))?;
        inner
            .commit(vec![Action::new(Kind::AdvanceSeq(proto::AdvanceSeq {
                table_id: table_id.to_string(),
                to,
            }))])
            .await?;
        Ok(old + 1)
    }
    async fn add_delete_file(&self, delete_file: DeleteFile) -> CatalogResult<String> {
        let mut inner = self.inner.take().await;
        let (id, batch) = inner.delete_batch(std::slice::from_ref(&delete_file))?;
        let id = id.ok_or_else(|| invalid("delete file has no identity"))?;
        inner.commit(batch).await?;
        Ok(id)
    }
    async fn add_delete_files(&self, delete_files: Vec<DeleteFile>) -> CatalogResult<()> {
        if delete_files.is_empty() {
            return Ok(());
        }
        let table_id = &delete_files[0].table_id;
        for file in &delete_files {
            if &file.table_id != table_id {
                return Err(invalid(format!(
                    "Delete-file table_id '{}' does not match atomic delete table_id '{table_id}'",
                    file.table_id
                )));
            }
        }
        let mut inner = self.inner.take().await;
        let (_, batch) = inner.delete_batch(&delete_files)?;
        inner.commit(batch).await
    }
    async fn commit_delete_files_with_inlined_rewrite(
        &self,
        delete_files: Vec<DeleteFile>,
        table_id: &str,
        updated_data: Vec<InlinedData>,
        deleted_inlined_ids: Vec<String>,
        replaced_delete_file_ids: Vec<String>,
    ) -> CatalogResult<()> {
        if delete_files.iter().any(|f| f.table_id != table_id) {
            return Err(invalid(
                "delete file table does not match combined delete table",
            ));
        }
        let mut inner = self.inner.take().await;
        let (_, mut batch) =
            inner.delete_batch_after_removal(&delete_files, &replaced_delete_file_ids)?;
        batch.insert(
            0,
            actions::remove_delete_files(
                table_id,
                proto::Select::Listed,
                &replaced_delete_file_ids,
                None,
            ),
        );
        batch.extend(rewrite_inline(
            table_id,
            &updated_data,
            &deleted_inlined_ids,
            true,
        )?);
        inner.commit(batch).await
    }
    async fn remove_delete_files(
        &self,
        table_id: &str,
        delete_file_ids: &[String],
    ) -> CatalogResult<()> {
        self.inner
            .take()
            .await
            .commit(vec![actions::remove_delete_files(
                table_id,
                proto::Select::Listed,
                delete_file_ids,
                None,
            )])
            .await
    }
    async fn clear_delete_files(&self, table_id: &str) -> CatalogResult<()> {
        self.inner
            .take()
            .await
            .commit(vec![actions::remove_delete_files(
                table_id,
                proto::Select::All,
                &[],
                None,
            )])
            .await
    }
    async fn add_insert_record(
        &self,
        table_id: &str,
        pk_bytes: Vec<u8>,
        sequence_number: i64,
    ) -> CatalogResult<()> {
        self.add_insert_records_batch(table_id, vec![pk_bytes], sequence_number)
            .await
    }
    async fn add_insert_records_batch(
        &self,
        table_id: &str,
        pk_bytes_list: Vec<Vec<u8>>,
        sequence_number: i64,
    ) -> CatalogResult<()> {
        if pk_bytes_list.is_empty() {
            return Ok(());
        }
        self.inner
            .take()
            .await
            .commit(vec![Action::new(Kind::AddInsertRecords(proto::AddKeys {
                table_id: table_id.to_string(),
                keys: pk_bytes_list
                    .into_iter()
                    .map(|pk| proto::PkSeq {
                        pk,
                        seq: sequence_number,
                    })
                    .collect(),
            }))])
            .await
    }
    async fn commit_on_conflict_deletions(
        &self,
        delete_files: Vec<DeleteFile>,
        table_id: &str,
        insert_pk_bytes_list: Vec<Vec<u8>>,
        insert_sequence: i64,
        snapshot_sequence: Option<SnapshotSequenceCommit>,
    ) -> CatalogResult<()> {
        self.commit_on_conflict_deletions_with_tombstone(
            delete_files,
            table_id,
            insert_pk_bytes_list,
            insert_sequence,
            snapshot_sequence,
            None,
            &[],
        )
        .await
        .map(|_| ())
    }
    async fn commit_on_conflict_deletions_with_tombstone(
        &self,
        delete_files: Vec<DeleteFile>,
        table_id: &str,
        insert_pk_bytes_list: Vec<Vec<u8>>,
        insert_sequence: i64,
        snapshot_sequence: Option<SnapshotSequenceCommit>,
        inline_tombstone: Option<InlinedDelete>,
        pending_durable_flips: &[String],
    ) -> CatalogResult<Option<String>> {
        let generated_id = if inline_tombstone
            .as_ref()
            .is_some_and(|d| d.inlined_id.is_empty())
        {
            Some(uuid::Uuid::now_v7().to_string())
        } else {
            None
        };
        let mut inner = self.inner.take().await;
        let mut batch = inner.conflict_batch(
            delete_files,
            table_id,
            &insert_pk_bytes_list,
            Some(insert_sequence),
            snapshot_sequence,
        )?;
        let id = if let Some(delete) = inline_tombstone {
            if delete.table_id != table_id {
                return Err(invalid("tombstone table does not match commit table"));
            }
            let id = if delete.inlined_id.is_empty() {
                generated_id.unwrap_or_default()
            } else {
                delete.inlined_id.clone()
            };
            batch.push(actions::inlined_delete(&delete, &id, false));
            Some(id)
        } else {
            None
        };
        if !pending_durable_flips.is_empty() {
            batch.push(actions::set_published(
                table_id,
                proto::Select::Listed,
                pending_durable_flips,
                true,
            ));
        }
        if !batch.is_empty() {
            inner.commit(batch).await?;
        }
        Ok(id)
    }
    async fn clear_insert_records(&self, table_id: &str) -> CatalogResult<()> {
        self.inner
            .take()
            .await
            .commit(vec![actions::clear_insert_records(table_id, None)])
            .await
    }
    async fn set_snapshot_sequence(
        &self,
        table_id: &str,
        snapshot_id: &str,
        sequence_number: i64,
    ) -> CatalogResult<()> {
        self.inner
            .take()
            .await
            .commit(vec![actions::set_group(
                table_id,
                snapshot_id,
                sequence_number,
            )])
            .await
    }
    async fn clear_snapshot_sequence(
        &self,
        table_id: &str,
        snapshot_id: &str,
    ) -> CatalogResult<()> {
        self.inner
            .take()
            .await
            .commit(vec![actions::remove_groups(
                table_id,
                proto::Select::Listed,
                &[snapshot_id.to_string()],
                false,
            )])
            .await
    }
    async fn commit_compaction(
        &self,
        table_id: &str,
        replaced_snapshot_id: &str,
        new_snapshot_id: &str,
    ) -> CatalogResult<()> {
        validate_uuid("table_id", table_id)?;
        validate_uuid("new_snapshot_id", new_snapshot_id)?;
        let mut inner = self.inner.take().await;
        inner.check_snapshot(table_id, replaced_snapshot_id)?;
        inner
            .commit(vec![
                actions::remove_delete_files(table_id, proto::Select::All, &[], None),
                actions::clear_insert_records(table_id, None),
                actions::remove_groups(table_id, proto::Select::All, &[], false),
                actions::set_current(table_id, new_snapshot_id, Some(replaced_snapshot_id)),
            ])
            .await
    }
    async fn commit_compaction_fenced(
        &self,
        table_id: &str,
        replaced_snapshot_id: &str,
        new_snapshot_id: &str,
        cutoff: i64,
        protected_snapshot_ids_to_clear: &[String],
    ) -> CatalogResult<()> {
        validate_uuid("table_id", table_id)?;
        validate_uuid("new_snapshot_id", new_snapshot_id)?;
        for id in protected_snapshot_ids_to_clear {
            validate_uuid("protected snapshot id", id)?;
        }
        let mut inner = self.inner.take().await;
        inner.check_snapshot(table_id, replaced_snapshot_id)?;
        let table = inner.table(table_id)?;
        let present = protected_snapshot_ids_to_clear
            .iter()
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .filter(|id| table.groups.contains_key(*id))
            .count();
        let missing = protected_snapshot_ids_to_clear.len() - present;
        if missing > 0 {
            return Err(CatalogError::ProtectedSnapshotsReplaced {
                table_id: table_id.to_string(),
                folded: protected_snapshot_ids_to_clear.len(),
                missing,
            });
        }
        inner
            .commit(vec![
                actions::remove_delete_files(table_id, proto::Select::All, &[], Some(cutoff)),
                actions::clear_insert_records(table_id, Some(cutoff)),
                actions::remove_groups(
                    table_id,
                    proto::Select::Listed,
                    protected_snapshot_ids_to_clear,
                    true,
                ),
                actions::set_current(table_id, new_snapshot_id, Some(replaced_snapshot_id)),
            ])
            .await
    }
    async fn swap_protected_snapshots(
        &self,
        table_id: &str,
        old_snapshot_ids: &[String],
        new_snapshot_id: &str,
        new_sequence_number: i64,
    ) -> CatalogResult<bool> {
        validate_uuid("table_id", table_id)?;
        validate_uuid("new_snapshot_id", new_snapshot_id)?;
        for id in old_snapshot_ids {
            validate_uuid("old_snapshot_id", id)?;
        }
        let mut inner = self.inner.take().await;
        let table = inner.table(table_id)?;
        let unique: std::collections::BTreeSet<_> = old_snapshot_ids.iter().collect();
        if old_snapshot_ids.is_empty()
            || unique.len() != old_snapshot_ids.len()
            || old_snapshot_ids
                .iter()
                .any(|id| !table.groups.contains_key(id))
        {
            return Ok(false);
        }
        inner
            .commit(vec![
                actions::remove_groups(table_id, proto::Select::Listed, old_snapshot_ids, true),
                actions::remove_files(table_id, proto::Select::Listed, old_snapshot_ids),
                actions::remove_file_stats(table_id, proto::Select::Listed, old_snapshot_ids),
                actions::set_group(table_id, new_snapshot_id, new_sequence_number),
            ])
            .await?;
        Ok(true)
    }
    async fn commit_overwrite(
        &self,
        table_id: &str,
        new_snapshot_id: &str,
        inlined: Option<&InlinedData>,
        delete_files: &[crate::metadata::DeleteFile],
    ) -> CatalogResult<()> {
        validate_uuid("table_id", table_id)?;
        validate_uuid("new_snapshot_id", new_snapshot_id)?;
        let mut inner = self.inner.take().await;
        let mut batch = actions::overwrite_clear(table_id, new_snapshot_id);
        if let Some(data) = inlined {
            batch.push(actions::add_inlined(
                table_id,
                data,
                &data.inlined_id,
                data.sequence_number,
            ));
        }
        batch.push(actions::add_delete_files(table_id, delete_files));
        inner.commit(batch).await
    }
    async fn add_partition(&self, partition: PartitionMetadata) -> CatalogResult<String> {
        if partition.partition_columns.is_empty()
            || partition.partition_values.is_empty()
            || partition.partition_columns.len() != partition.partition_values.len()
        {
            return Err(CatalogError::InvalidPartitionMetadata {
                message: "partition columns and values must be nonempty and have equal lengths"
                    .to_string(),
            });
        }
        let generated_id = uuid::Uuid::now_v7().to_string();
        let mut inner = self.inner.take().await;
        if let Some((id, _)) = inner
            .table(&partition.table_id)?
            .partitions
            .iter()
            .find(|(_, p)| {
                p.columns == partition.partition_columns && p.values == partition.partition_values
            })
        {
            return Ok(id.clone());
        }
        let id = generated_id;
        inner
            .commit(vec![Action::new(Kind::AddPartition(proto::AddPartition {
                table_id: partition.table_id,
                partition_id: id.clone(),
                columns: partition.partition_columns,
                values: partition.partition_values,
                path: partition.path,
                path_is_relative: partition.path_is_relative,
                record_count: partition.record_count,
                size: partition.file_size_bytes,
            }))])
            .await?;
        Ok(id)
    }
    async fn upsert_table_statistics(&self, stats: &TableStatistics) -> CatalogResult<()> {
        let mut inner = self.inner.take().await;
        let blob = Some(inner.blob(&stats.statistics_blob).await?);
        let ndv = match &stats.ndv_sketches {
            Some(bytes) => Some(inner.blob(bytes).await?),
            None => None,
        };
        inner
            .commit(vec![Action::new(Kind::SetTableStats(
                proto::SetTableStats {
                    table_id: stats.table_id.clone(),
                    blob,
                    ndv,
                    num_rows: stats.num_rows,
                    exact: stats.num_rows_exact,
                },
            ))])
            .await
    }
    async fn clear_table_statistics(&self, table_id: &str) -> CatalogResult<()> {
        self.inner
            .take()
            .await
            .commit(vec![actions::clear_table_stats(table_id)])
            .await
    }
    async fn upsert_snapshot_file_statistics(
        &self,
        stats: &SnapshotFileStatistics,
    ) -> CatalogResult<()> {
        let mut inner = self.inner.take().await;
        inner.blob(&stats.statistics_blob).await?;
        inner
            .commit(vec![actions::set_file_stats(ObjRef::of, stats)])
            .await
    }
    async fn clear_snapshot_file_statistics_except(
        &self,
        table_id: &str,
        snapshot_id: &str,
    ) -> CatalogResult<()> {
        self.inner
            .take()
            .await
            .commit(vec![actions::remove_file_stats(
                table_id,
                proto::Select::AllExcept,
                &[snapshot_id.to_string()],
            )])
            .await
    }
    async fn clear_snapshot_file_statistics(&self, table_id: &str) -> CatalogResult<()> {
        self.inner
            .take()
            .await
            .commit(vec![actions::remove_file_stats(
                table_id,
                proto::Select::All,
                &[],
            )])
            .await
    }
    async fn upsert_snapshot_file(&self, file: &SnapshotFile) -> CatalogResult<()> {
        self.inner
            .take()
            .await
            .commit(vec![actions::add_files(
                &file.table_id,
                &file.snapshot_id,
                std::slice::from_ref(file),
                true,
            )])
            .await
    }
    async fn replace_snapshot_files(
        &self,
        table_id: &str,
        snapshot_id: &str,
        files: &[SnapshotFile],
    ) -> CatalogResult<()> {
        if files
            .iter()
            .any(|f| f.table_id != table_id || f.snapshot_id != snapshot_id)
        {
            return Err(invalid("snapshot file identity does not match replacement"));
        }
        self.inner
            .take()
            .await
            .commit(vec![
                actions::remove_files(table_id, proto::Select::Listed, &[snapshot_id.to_string()]),
                actions::add_files(table_id, snapshot_id, files, false),
            ])
            .await
    }
    async fn clear_snapshot_cached_metadata(
        &self,
        table_id: &str,
        snapshot_id: &str,
    ) -> CatalogResult<()> {
        self.inner
            .take()
            .await
            .commit(vec![
                actions::remove_files(table_id, proto::Select::Listed, &[snapshot_id.to_string()]),
                actions::remove_file_stats(
                    table_id,
                    proto::Select::Listed,
                    &[snapshot_id.to_string()],
                ),
            ])
            .await
    }
    async fn clear_snapshot_files_except(
        &self,
        table_id: &str,
        snapshot_id: &str,
    ) -> CatalogResult<()> {
        self.inner
            .take()
            .await
            .commit(vec![actions::remove_files(
                table_id,
                proto::Select::AllExcept,
                &[snapshot_id.to_string()],
            )])
            .await
    }
    async fn clear_snapshot_files(&self, table_id: &str) -> CatalogResult<()> {
        self.inner
            .take()
            .await
            .commit(vec![actions::remove_files(
                table_id,
                proto::Select::All,
                &[],
            )])
            .await
    }
    async fn commit_overwrite_to_cold(
        &self,
        table_id: &str,
        new_snapshot_id: &str,
        cold_files: &[ColdTierFile],
    ) -> CatalogResult<()> {
        validate_uuid("table_id", table_id)?;
        validate_uuid("new_snapshot_id", new_snapshot_id)?;
        let mut inner = self.inner.take().await;
        for file in cold_files {
            inner.blob(&file.statistics_blob).await?;
            if let Some(bloom) = &file.pk_bloom {
                inner.blob(bloom).await?;
            }
        }
        let mut batch = actions::overwrite_clear(table_id, new_snapshot_id);
        batch.push(actions::add_cold(ObjRef::of, table_id, cold_files));
        inner.commit(batch).await
    }
    async fn upsert_pk_index(
        &self,
        table_id: &str,
        snapshot_id: &str,
        index_blob: &[u8],
    ) -> CatalogResult<()> {
        let mut inner = self.inner.take().await;
        let index = inner.blob(index_blob).await?;
        inner
            .commit(vec![Action::new(Kind::SetPkIndex(proto::SetPkIndex {
                table_id: table_id.to_string(),
                snapshot_id: snapshot_id.to_string(),
                index: Some(index),
            }))])
            .await
    }
    async fn clear_pk_index(&self, table_id: &str) -> CatalogResult<()> {
        self.inner
            .take()
            .await
            .commit(vec![Action::new(Kind::ClearPkIndex(actions::table(
                table_id,
            )))])
            .await
    }
    async fn register_index_run(&self, run: &IndexRunRecord) -> CatalogResult<()> {
        self.inner
            .take()
            .await
            .commit(vec![Action::new(Kind::AddIndexRun(proto::IndexRun {
                table_id: run.table_id.clone(),
                index_key: run.index_key.clone(),
                run_name: run.run_name.clone(),
                rows: run.row_count,
                size: run.size_bytes,
            }))])
            .await
    }
    async fn remove_index_run(
        &self,
        table_id: &str,
        index_key: &str,
        run_name: &str,
    ) -> CatalogResult<()> {
        self.inner
            .take()
            .await
            .commit(vec![Action::new(Kind::RemoveIndexRun(proto::IndexRun {
                table_id: table_id.to_string(),
                index_key: index_key.to_string(),
                run_name: run_name.to_string(),
                rows: 0,
                size: 0,
            }))])
            .await
    }
    async fn add_inlined_data(&self, data: InlinedData) -> CatalogResult<String> {
        let id = if data.inlined_id.is_empty() {
            uuid::Uuid::now_v7().to_string()
        } else {
            data.inlined_id.clone()
        };
        self.inner
            .take()
            .await
            .commit(vec![actions::add_inlined(
                &data.table_id,
                &data,
                &id,
                data.sequence_number,
            )])
            .await?;
        Ok(id)
    }
    async fn get_inlined_data_above_sequence(
        &self,
        table_id: &str,
        after_sequence: i64,
    ) -> CatalogResult<Vec<InlinedData>> {
        self.read()
            .await?
            .reader
            .get_inlined_data_above_sequence(table_id, after_sequence)
            .await
    }
    async fn clear_inlined_data(&self, table_id: &str) -> CatalogResult<()> {
        self.inner
            .take()
            .await
            .commit(vec![actions::remove_inlined(
                table_id,
                proto::Select::All,
                &[],
            )])
            .await
    }
    async fn clear_inlined_data_and_deletes(&self, table_id: &str) -> CatalogResult<()> {
        self.inner
            .take()
            .await
            .commit(vec![
                actions::remove_inlined(table_id, proto::Select::All, &[]),
                actions::remove_inlined_deletes(table_id, proto::Select::All, &[]),
            ])
            .await
    }
    async fn commit_on_conflict_deletions_and_clear_inlined(
        &self,
        delete_files: Vec<DeleteFile>,
        table_id: &str,
        insert_pk_bytes_list: Vec<Vec<u8>>,
        insert_sequence: i64,
        snapshot_sequence: Option<SnapshotSequenceCommit>,
    ) -> CatalogResult<()> {
        let mut inner = self.inner.take().await;
        let mut batch = inner.conflict_batch(
            delete_files,
            table_id,
            &insert_pk_bytes_list,
            Some(insert_sequence),
            snapshot_sequence,
        )?;
        batch.push(actions::remove_inlined(table_id, proto::Select::All, &[]));
        batch.push(actions::remove_inlined_deletes(
            table_id,
            proto::Select::All,
            &[],
        ));
        inner.commit(batch).await
    }
    async fn add_inlined_delete(&self, delete: InlinedDelete) -> CatalogResult<String> {
        let id = if delete.inlined_id.is_empty() {
            uuid::Uuid::now_v7().to_string()
        } else {
            delete.inlined_id.clone()
        };
        self.inner
            .take()
            .await
            .commit(vec![actions::inlined_delete(&delete, &id, false)])
            .await?;
        Ok(id)
    }
    async fn mark_inlined_delete_published(
        &self,
        table_id: &str,
        inlined_id: &str,
    ) -> CatalogResult<()> {
        self.inner
            .take()
            .await
            .commit(vec![actions::set_published(
                table_id,
                proto::Select::Listed,
                &[inlined_id.to_string()],
                true,
            )])
            .await
    }
    async fn mark_inlined_delete_unpublished(
        &self,
        table_id: &str,
        inlined_id: &str,
    ) -> CatalogResult<()> {
        self.inner
            .take()
            .await
            .commit(vec![actions::set_published(
                table_id,
                proto::Select::Listed,
                &[inlined_id.to_string()],
                false,
            )])
            .await
    }
    async fn publish_orphan_inlined_deletes(&self, table_id: &str) -> CatalogResult<u64> {
        let mut inner = self.inner.take().await;
        let count = inner.reader.state.tables.get(table_id).map_or(0, |t| {
            t.inlined_deletes.values().filter(|d| !d.published).count()
        });
        if count > 0 {
            inner
                .commit(vec![actions::set_published(
                    table_id,
                    proto::Select::All,
                    &[],
                    true,
                )])
                .await?;
        }
        u64::try_from(count).map_err(invalid)
    }
    async fn commit_inlined_mutation(
        &self,
        table_id: &str,
        updated_data: Vec<InlinedData>,
        deleted_inlined_ids: Vec<String>,
        data: Vec<InlinedData>,
        assigned_sequence: i64,
    ) -> CatalogResult<Option<i64>> {
        if updated_data.is_empty() && deleted_inlined_ids.is_empty() && data.is_empty() {
            return Ok(None);
        }
        let mut batch = rewrite_inline(table_id, &updated_data, &deleted_inlined_ids, false)?;
        for entry in &data {
            if entry.table_id != table_id {
                return Err(invalid("inline data table does not match commit table"));
            }
            let id = if entry.inlined_id.is_empty() {
                uuid::Uuid::now_v7().to_string()
            } else {
                entry.inlined_id.clone()
            };
            batch.push(actions::add_inlined(
                table_id,
                entry,
                &id,
                assigned_sequence,
            ));
        }
        self.inner.take().await.commit(batch).await?;
        Ok((!data.is_empty()).then_some(assigned_sequence))
    }
    async fn clear_inlined_deletes(&self, table_id: &str) -> CatalogResult<()> {
        self.inner
            .take()
            .await
            .commit(vec![actions::remove_inlined_deletes(
                table_id,
                proto::Select::All,
                &[],
            )])
            .await
    }
    async fn checkpoint_wal(&self) -> CatalogResult<()> {
        Ok(())
    }
    async fn incremental_vacuum(&self) -> CatalogResult<u64> {
        Ok(0)
    }
    async fn drop_table(&self, table_name: &str) -> CatalogResult<bool> {
        let mut inner = self.inner.take().await;
        let Some((id, _)) = inner
            .reader
            .state
            .tables
            .iter()
            .find(|(_, t)| t.name == table_name)
        else {
            return Ok(false);
        };
        let id = id.clone();
        let table = inner.table(&id)?;
        let mut removed = std::collections::BTreeSet::new();
        for partition in table.partitions.values() {
            let [current, legacy] = crate::partition_naming::partition_child_candidate_names(
                table_name,
                &partition.values,
            );
            for (child_id, child) in &inner.reader.state.tables {
                if child.name == current || (child.name == legacy && child.path == partition.path) {
                    removed.insert(child_id.clone());
                }
            }
        }
        removed.insert(id);
        inner
            .commit(
                removed
                    .into_iter()
                    .map(|id| Action::new(Kind::DropTable(actions::table(&id))))
                    .collect(),
            )
            .await?;
        Ok(true)
    }
    async fn export_dataset_slice(
        &self,
        _dataset_name: &str,
        _data_dir_anchor: &std::path::Path,
    ) -> CatalogResult<crate::metastore::snapshot::DatasetMetastoreSlice> {
        Err(CatalogError::NotImplemented {
            function: "object-native: export_dataset_slice".to_string(),
        })
    }
    async fn import_dataset_slice(
        &self,
        _slice: &crate::metastore::snapshot::DatasetMetastoreSlice,
        _data_dir_anchor: &std::path::Path,
    ) -> CatalogResult<()> {
        Err(CatalogError::NotImplemented {
            function: "object-native: import_dataset_slice".to_string(),
        })
    }
}

fn rewrite_inline(
    table_id: &str,
    updated: &[InlinedData],
    deleted: &[String],
    require_present: bool,
) -> CatalogResult<Vec<Action>> {
    let mut batch = Vec::new();
    for entry in updated {
        if entry.table_id != table_id || entry.inlined_id.is_empty() {
            return Err(invalid(
                "updated inline row must match commit table and include an id",
            ));
        }
        batch.push(Action::new(Kind::UpdateInlinedData(
            proto::UpdateInlinedData {
                table_id: table_id.to_string(),
                id: entry.inlined_id.clone(),
                payload: entry.data_ipc.clone(),
                count: entry.record_count,
                require_present,
            },
        )));
    }
    if !deleted.is_empty() {
        batch.push(actions::remove_inlined(
            table_id,
            proto::Select::Listed,
            deleted,
        ));
    }
    Ok(batch)
}
impl ObjectNativeCatalog {
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

    /// Move each named counter under `key` up to its given value, keeping the
    /// larger of the stored and given values, in one commit (or in the enclosing
    /// deferred record). Names must not be empty: the empty name is the value of
    /// [`Self::set_sidecar_value`].
    /// # Errors
    /// Returns an error when a name is empty or the commit is not accepted.
    pub async fn advance_sidecar_counters(
        &self,
        key: &str,
        counters: &[(String, i64)],
    ) -> CatalogResult<()> {
        self.set_sidecar_value_and_counters(key, None, counters)
            .await
    }

    /// [`Self::set_sidecar_value`] (when `value` is `Some`) and
    /// [`Self::advance_sidecar_counters`] in the same commit.
    /// # Errors
    /// Returns an error when a counter name is empty or the commit is not accepted.
    pub async fn set_sidecar_value_and_counters(
        &self,
        key: &str,
        value: Option<Option<Vec<u8>>>,
        counters: &[(String, i64)],
    ) -> CatalogResult<()> {
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
        if actions.is_empty() && !inner.poisoned {
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
        if !inner.poisoned
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
        if !options.base_path.starts_with("s3://") && !options.vortex_config.memory_mode {
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
        inner
            .commit(vec![Action::new(Kind::CreateTable(proto::CreateTable {
                table_id: id.clone(),
                name: options.table_name,
                path: options.base_path,
                path_is_relative: false,
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
    async fn persist_schema(
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
    fn delete_batch(&self, files: &[DeleteFile]) -> CatalogResult<(Option<String>, Vec<Action>)> {
        self.delete_batch_after_removal(files, &[])
    }
    fn delete_batch_after_removal(
        &self,
        files: &[DeleteFile],
        removed_ids: &[String],
    ) -> CatalogResult<(Option<String>, Vec<Action>)> {
        let mut first_id = None;
        let mut batch = Vec::new();
        let mut seen = std::collections::BTreeMap::new();
        for file in files {
            let table = self.table(&file.table_id)?;
            let existing = table
                .delete_files
                .values()
                .filter(|f| !removed_ids.contains(&f.id))
                .find(|f| f.path == file.path)
                .or_else(|| seen.get(&file.path));
            let entry = actions::dv(file);
            if let Some(existing) = existing {
                let mut compared = entry.clone();
                compared.id.clone_from(&existing.id);
                if existing != &compared {
                    return Err(invalid(format!(
                        "delete file {} has conflicting metadata",
                        file.path
                    )));
                }
                if first_id.is_none() {
                    first_id = Some(existing.id.clone());
                }
            } else {
                let mut file = file.clone();
                file.delete_file_id = uuid::Uuid::now_v7().to_string();
                if first_id.is_none() {
                    first_id = Some(file.delete_file_id.clone());
                }
                seen.insert(file.path.clone(), actions::dv(&file));
                batch.push(actions::add_delete_files(
                    &file.table_id,
                    std::slice::from_ref(&file),
                ));
            }
        }
        Ok((first_id, batch))
    }
    fn conflict_batch(
        &self,
        mut files: Vec<DeleteFile>,
        table_id: &str,
        keys: &[Vec<u8>],
        insert_sequence: Option<i64>,
        snapshot: Option<SnapshotSequenceCommit>,
    ) -> CatalogResult<Vec<Action>> {
        if files.iter().any(|f| f.table_id != table_id) {
            return Err(invalid("delete file table does not match commit table"));
        }
        let reinsert_sequence = insert_sequence.filter(|_| !keys.is_empty());
        if !keys.is_empty() {
            if !files
                .iter()
                .any(|f| f.deletion_type == crate::metadata::DeletionType::KeyBased)
            {
                return Err(invalid("reinsert keys require a key-based delete file"));
            }
            if files
                .iter()
                .any(|f| reinsert_sequence.is_some_and(|sequence| sequence <= f.sequence_number))
            {
                return Err(invalid("insert sequence must exceed delete sequence"));
            }
        }
        for file in &mut files {
            file.reinsert_sequence = (file.deletion_type
                == crate::metadata::DeletionType::KeyBased)
                .then_some(reinsert_sequence)
                .flatten();
        }
        let (_, mut batch) = self.delete_batch(&files)?;
        if let Some(snapshot) = snapshot {
            batch.push(actions::set_group(
                table_id,
                &snapshot.snapshot_id,
                snapshot.sequence_number,
            ));
        }
        Ok(batch)
    }
}

fn validate_uuid(name: &str, value: &str) -> CatalogResult<()> {
    uuid::Uuid::parse_str(value)
        .map_err(|_| invalid(format!("{name} is not a valid UUID: {value}")))?;
    Ok(())
}

impl Inner {
    async fn reopen_table(&mut self, options: &CreateTableOptions) -> CatalogResult<String> {
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
