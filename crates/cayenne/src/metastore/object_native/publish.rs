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

//! Buffered changes and publication.

use super::{
    super::{actions, state_reader},
    Inner, Pending, Stop, fenced_message, invalid, subject, unknown_outcome_message,
    unresolved_commit_message,
};
use crate::{
    catalog::{CatalogError, CatalogResult, SnapshotSequenceCommit},
    metadata::{DeleteFile, InlinedData},
};
use cayenne_metastore_actions::{Action, Kind, Record, proto};
use object_store_occ::headlog::Outcome;
use prost::Message;

impl Inner {
    /// Buffer an applied record with the buffers it shares a table with, or
    /// commit it now outside a deferred scope.
    pub(super) async fn buffer(&mut self, record: Record) -> CatalogResult<()> {
        let mut tables = self.tables_of(&record.actions);
        // Creating or dropping a table changes which table a name refers to, which
        // table ids alone do not order: a create under a name whose drop is still
        // buffered must not commit first, or the log would hold the name twice. Such
        // a record therefore takes every buffered change with it.
        if record.actions.iter().any(|action| {
            matches!(
                action.kind,
                Some(Kind::CreateTable(_) | Kind::DropTable(_) | Kind::Unconverted(_))
            )
        }) {
            for buffer in &self.pending {
                tables.extend(buffer.tables.iter().cloned());
            }
        }
        // Merge every buffer this record shares a table with, oldest first.
        let mut merged = Pending {
            tables: tables.clone(),
            actions: Vec::new(),
        };
        let mut kept = Vec::with_capacity(self.pending.len());
        for buffer in std::mem::take(&mut self.pending) {
            if buffer.tables.is_disjoint(&tables) {
                kept.push(buffer);
            } else {
                merged.tables.extend(buffer.tables);
                merged.actions.extend(buffer.actions);
            }
        }
        merged.actions.extend(record.actions);
        self.pending = kept;
        // Outside a deferred scope a change is durable when its method returns.
        // Inside one it waits for the scope's acknowledgement point however large
        // or old it grows, so the position staged last shares its record; every
        // deferred scope ends at such a point.
        if self.immediate || !crate::catalog::commits_deferred() {
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
    pub(super) async fn flush_table(&mut self, table_id: &str) -> CatalogResult<()> {
        let (due, kept): (Vec<_>, Vec<_>) = std::mem::take(&mut self.pending)
            .into_iter()
            .partition(|buffer| buffer.tables.contains(table_id) || buffer.tables.contains(""));
        self.pending = kept;
        self.publish_buffers(due).await
    }
    /// Commit every buffered change as one record. Nothing that depends on a
    /// buffered change may be acknowledged before this returns `Ok`.
    pub(super) async fn flush(&mut self) -> CatalogResult<()> {
        // A replica buffers nothing, so it has nothing to commit.
        if self.stop == Some(Stop::Replica) && self.pending.is_empty() {
            return Ok(());
        }
        self.ensure_accepting()?;
        let buffers = std::mem::take(&mut self.pending);
        self.publish_buffers(buffers).await
    }
    /// Commit `buffers`, oldest first, as one record.
    async fn publish_buffers(&mut self, buffers: Vec<Pending>) -> CatalogResult<()> {
        self.ensure_accepting()?;
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
        // not commit, they must never be acknowledged: the log refuses all
        // further work until it is reopened from what did commit. Set before the
        // first await, so a cancelled caller leaves the log to be resolved too.
        // A replica has no writer and refuses here, before anything changes.
        self.writer()?;
        self.stop = Some(Stop::Unresolved);
        let prepared = self
            .writer_mut()?
            .prepare_group(vec![record.encode_to_vec().into()])
            .await;
        let attempt = match prepared {
            Ok(attempt) => attempt,
            Err(error) => return Err(self.unresolved(&error.to_string())),
        };
        // Refuse subsequent mutations unless this exact attempt is accepted.
        let outcome = match self.publish(&attempt).await {
            Ok(outcome) => outcome,
            Err(error) => return Err(self.unresolved(&error.to_string())),
        };
        match outcome {
            Outcome::Accepted { commit } => {
                self.stop = None;
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
            Outcome::Unresolved => Err(self.unresolved("the outcome was unresolved")),
            // Another instance owns the log: normal when one takes over from
            // another, so it is reported, not raised as an error. Its reads keep
            // working once the committed state is reloaded.
            Outcome::Fenced { .. } | Outcome::Rejected => {
                tracing::info!("{}", fenced_message(&self.dataset));
                self.stop = Some(Stop::Fenced { reloaded: false });
                if let Err(error) = self.reload_committed().await {
                    tracing::debug!(
                        "{} could not reload its committed state yet, so its queries wait for a background retry: {error}",
                        subject(&self.dataset)
                    );
                }
                Err(CatalogError::InvalidOperationNoSource {
                    message: format!(
                        "{} The change was not saved (the commit was {outcome:?}).",
                        fenced_message(&self.dataset)
                    ),
                })
            }
        }
    }
    /// Stop the log for a commit whose outcome is unknown, and the error its
    /// caller gets. The lease's drop starts the background reopen.
    pub(super) fn unresolved(&mut self, cause: &str) -> CatalogError {
        self.stop = Some(Stop::Unresolved);
        tracing::warn!("{}", unresolved_commit_message(&self.dataset, cause));
        CatalogError::InvalidOperationNoSource {
            message: unknown_outcome_message(&self.dataset, cause),
        }
    }
    /// Commit a transaction's `Prepare` or `Resolve` now, as its own record,
    /// after every buffered change. A record whose guard fails is refused and
    /// leaves the state as it was.
    pub(super) async fn commit_now(&mut self, action: Action) -> CatalogResult<()> {
        self.ensure_accepting()?;
        self.flush().await?;
        let record = Record {
            actions: vec![action],
        };
        self.reader.state.apply(&record).map_err(invalid)?;
        self.publish_buffers(vec![Pending {
            tables: std::collections::BTreeSet::new(),
            actions: record.actions,
        }])
        .await
    }
    /// The actions that publish `publications` in one record.
    pub(super) fn publication_actions(
        &self,
        publications: &[crate::catalog::CatalogPublication<'_>],
    ) -> CatalogResult<Vec<Action>> {
        let mut batch = Vec::new();
        for publication in publications {
            if !publication.files.is_empty() {
                if publication.files.iter().any(|file| {
                    file.table_id != publication.table_id
                        || file.snapshot_id != publication.snapshot_id
                }) {
                    return Err(invalid(
                        "a published file does not belong to the published snapshot",
                    ));
                }
                batch.push(actions::add_files(
                    publication.table_id,
                    publication.snapshot_id,
                    publication.files,
                    true,
                ));
            }
            batch.extend(self.conflict_batch(
                publication.delete_files.to_vec(),
                publication.table_id,
                publication.insert_pk_bytes,
                publication.insert_sequence,
                Some(SnapshotSequenceCommit {
                    snapshot_id: publication.snapshot_id.to_string(),
                    sequence_number: publication.snapshot_sequence,
                }),
            )?);
            if let Some(tombstone) = publication.inline_tombstone {
                batch.push(actions::inlined_delete(
                    tombstone,
                    &tombstone.inlined_id,
                    true,
                ));
            }
            if !publication.pending_durable_flips.is_empty() {
                batch.push(actions::set_published(
                    publication.table_id,
                    proto::Select::Listed,
                    publication.pending_durable_flips,
                    true,
                ));
            }
            if !publication.dirty_pk_bytes.is_empty() {
                batch.push(Action::new(Kind::MarkDirty(proto::AddKeys {
                    table_id: publication.table_id.to_string(),
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
        Ok(batch)
    }
    /// Publish `attempt`, resolving an uncertain outcome by reading the head back
    /// and retrying only the same bytes under the same condition, so a transient
    /// store error neither loses nor duplicates the commit. `Rejected` and
    /// `Fenced` are final: another writer owns the log.
    async fn publish(
        &mut self,
        attempt: &object_store_occ::headlog::PreparedAttempt,
    ) -> CatalogResult<Outcome> {
        let attempts = self.config.publish_attempts;
        let base = self.config.retry_delay;
        let mut outcome = self.writer_mut()?.commit(attempt).await;
        for retry in 1..=attempts {
            let error = match outcome {
                Ok(Outcome::Unresolved) => "the outcome was unresolved".to_string(),
                Ok(final_outcome) => return Ok(final_outcome),
                Err(error) => error.to_string(),
            };
            let delay = base.saturating_mul(2_u32.pow(retry.min(6)));
            tracing::warn!(
                "{} could not confirm a commit ({error}), so it is checking whether the commit landed and retrying it in {} ms (attempt {retry} of {attempts})",
                subject(&self.dataset),
                delay.as_millis()
            );
            tokio::time::sleep(delay).await;
            outcome = self.writer_mut()?.resolve(attempt).await;
        }
        outcome.map_err(invalid)
    }
}

pub(super) fn rewrite_inline(
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

impl Inner {
    pub(super) fn delete_batch(
        &self,
        files: &[DeleteFile],
    ) -> CatalogResult<(Option<String>, Vec<Action>)> {
        self.delete_batch_after_removal(files, &[])
    }
    pub(super) fn delete_batch_after_removal(
        &self,
        files: &[DeleteFile],
        removed_ids: &[String],
    ) -> CatalogResult<(Option<String>, Vec<Action>)> {
        let mut first_id = None;
        let mut batch = Vec::new();
        let mut seen = std::collections::BTreeMap::new();
        // Compared and recorded as the state keeps them.
        let files: Vec<DeleteFile> = files
            .iter()
            .map(|file| {
                let (path, path_is_relative) =
                    state_reader::stored_path(self.reader.anchor.as_deref(), &file.path);
                DeleteFile {
                    path,
                    path_is_relative,
                    ..file.clone()
                }
            })
            .collect();
        for file in &files {
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
    pub(super) fn conflict_batch(
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
