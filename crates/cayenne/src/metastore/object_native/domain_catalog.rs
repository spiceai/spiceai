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

//! Metadata catalog operations for a domain.

use super::{super::actions, Domain, invalid, publish::rewrite_inline, validate_uuid};
use crate::{
    catalog::{
        CatalogError, CatalogResult, MemTierDurableCommit, MetadataCatalog, SnapshotSequenceCommit,
    },
    metadata::{
        ColdTierFile, CreateTableOptions, DeleteFile, IndexRunRecord, InlinedData,
        InlinedDataStats, InlinedDelete, PartitionMetadata, SnapshotFile, SnapshotFileStatistics,
        TableMetadata, TableStatistics, TableStorageStats,
    },
};
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use cayenne_metastore_actions::{Action, Kind, ObjRef, proto};
use std::collections::HashMap;

#[async_trait]
impl MetadataCatalog for Domain {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    async fn commit_pending(&self) -> CatalogResult<()> {
        self.inner.take().await.flush().await
    }
    async fn commit_pending_for(&self, table_id: &str) -> CatalogResult<()> {
        self.inner.take().await.flush_table(table_id).await
    }
    fn commits_source_positions(&self) -> bool {
        true
    }
    async fn commit_publications(
        &self,
        publications: &[crate::catalog::CatalogPublication<'_>],
    ) -> CatalogResult<()> {
        let mut inner = self.inner.take().await;
        let batch = inner.publication_actions(publications)?;
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
        Ok(self
            .read()
            .await?
            .reader
            .list_pending_write_back(table_id, limit, after))
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
        self.read().await?.reader.pending_write_back_count(table_id)
    }
    fn data_write_gate(&self, _table_id: &str) -> crate::catalog::DataWriteGate {
        self.data_writes.clone()
    }
    async fn follow_commits(&self, _table_id: &str) -> CatalogResult<bool> {
        // The instance that writes the log is always current, and learns that
        // without taking the log's lock on every query: its gate is open
        // exactly while it writes the log.
        if self.data_writes.allows() {
            return Ok(false);
        }
        self.inner.take().await.follow().await
    }
    async fn init(&self) -> CatalogResult<()> {
        self.read().await?;
        Ok(())
    }
    async fn shutdown(&self) -> CatalogResult<()> {
        {
            let mut inner = self.inner.take().await;
            inner.flush().await?;
        }
        // Hand the writer lease back, so another instance takes over at once.
        self.release_lease().await;
        Ok(())
    }
    async fn list_table_names(&self) -> CatalogResult<Vec<String>> {
        Ok(self.read().await?.reader.list_table_names())
    }
    async fn get_table(&self, table_name: &str) -> CatalogResult<TableMetadata> {
        self.read().await?.reader.get_table(table_name).await
    }
    async fn get_sequence_number(&self, table_id: &str) -> CatalogResult<i64> {
        self.read().await?.reader.get_sequence_number(table_id)
    }
    async fn get_table_delete_files(&self, table_id: &str) -> CatalogResult<Vec<DeleteFile>> {
        Ok(self.read().await?.reader.get_table_delete_files(table_id))
    }
    async fn get_orphan_eligible_delete_files(
        &self,
        table_id: &str,
        max_sequence: i64,
        limit: usize,
    ) -> CatalogResult<Vec<DeleteFile>> {
        Ok(self.read().await?.reader.get_orphan_eligible_delete_files(
            table_id,
            max_sequence,
            limit,
        ))
    }
    async fn get_insert_records(&self, table_id: &str) -> CatalogResult<HashMap<Box<[u8]>, i64>> {
        Ok(self.read().await?.reader.get_insert_records(table_id))
    }
    async fn get_snapshot_sequence(
        &self,
        table_id: &str,
        snapshot_id: &str,
    ) -> CatalogResult<Option<i64>> {
        Ok(self
            .read()
            .await?
            .reader
            .get_snapshot_sequence(table_id, snapshot_id))
    }
    async fn get_all_snapshot_sequences(
        &self,
        table_id: &str,
    ) -> CatalogResult<HashMap<String, i64>> {
        Ok(self
            .read()
            .await?
            .reader
            .get_all_snapshot_sequences(table_id))
    }
    async fn get_partitions(&self, table_id: &str) -> CatalogResult<Vec<PartitionMetadata>> {
        Ok(self.read().await?.reader.get_partitions(table_id))
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
        Ok(self.read().await?.reader.get_all_snapshot_files(table_id))
    }
    fn keeps_retirement_record(&self) -> bool {
        true
    }
    async fn retired_files(&self, table_id: &str) -> CatalogResult<Option<Vec<String>>> {
        let inner = self.inner.take().await;
        // Only committed state may license a delete: a buffered change is applied
        // here but may never commit, and a stopped log's last commit may not
        // have landed, or a fenced one belongs to another writer.
        if inner.stop.is_some() || inner.has_uncommitted(table_id) || inner.reserved(table_id) {
            return Ok(None);
        }
        Ok(Some(
            inner.table(table_id)?.retired.iter().cloned().collect(),
        ))
    }
    async fn committed_file_keys(
        &self,
        table_id: &str,
    ) -> CatalogResult<Option<std::collections::BTreeSet<String>>> {
        let inner = self.inner.take().await;
        if inner.stop.is_some() || inner.has_uncommitted(table_id) || inner.reserved(table_id) {
            return Ok(None);
        }
        let table = inner.table(table_id)?;
        let mut keys = table.live_keys();
        keys.extend(table.retired.iter().cloned());
        Ok(Some(keys))
    }
    async fn forget_retired_files(&self, table_id: &str, keys: &[String]) -> CatalogResult<bool> {
        if keys.is_empty() {
            return Ok(true);
        }
        let mut inner = self.inner.take().await;
        // A reserved table refuses every change until its transaction resolves,
        // and a refused record stops the whole log; the next sweep forgets them.
        if inner.reserved(table_id) {
            return Ok(false);
        }
        let retired = &inner.table(table_id)?.retired;
        let keys: Vec<String> = keys
            .iter()
            .filter(|key| retired.contains(*key))
            .cloned()
            .collect();
        if keys.is_empty() {
            return Ok(true);
        }
        // The forget drops the manifest rows and statistics of its keys too.
        inner
            .commit(vec![actions::forget_retired(table_id, &keys)])
            .await?;
        Ok(true)
    }
    async fn get_snapshot_files(
        &self,
        table_id: &str,
        snapshot_id: &str,
    ) -> CatalogResult<Vec<SnapshotFile>> {
        Ok(self
            .read()
            .await?
            .reader
            .get_snapshot_files(table_id, snapshot_id))
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
        Ok(self.read().await?.reader.list_index_runs(table_id))
    }
    async fn get_inlined_data(&self, table_id: &str) -> CatalogResult<Vec<InlinedData>> {
        Ok(self.read().await?.reader.get_inlined_data(table_id))
    }
    async fn get_inlined_data_for_partition(
        &self,
        table_id: &str,
        partition_key: &str,
    ) -> CatalogResult<Vec<InlinedData>> {
        Ok(self
            .read()
            .await?
            .reader
            .get_inlined_data_for_partition(table_id, partition_key))
    }
    async fn get_inlined_data_count(&self, table_id: &str) -> CatalogResult<i64> {
        self.read().await?.reader.get_inlined_data_count(table_id)
    }
    async fn get_inlined_data_stats(&self, table_id: &str) -> CatalogResult<InlinedDataStats> {
        self.read().await?.reader.get_inlined_data_stats(table_id)
    }
    async fn get_inlined_deletes(&self, table_id: &str) -> CatalogResult<Vec<InlinedDelete>> {
        Ok(self.read().await?.reader.get_inlined_deletes(table_id))
    }
    async fn get_published_inlined_deletes(
        &self,
        table_id: &str,
    ) -> CatalogResult<Vec<InlinedDelete>> {
        Ok(self
            .read()
            .await?
            .reader
            .get_published_inlined_deletes(table_id))
    }
    async fn get_unpublished_inlined_delete_ids(
        &self,
        table_id: &str,
    ) -> CatalogResult<Vec<String>> {
        Ok(self
            .read()
            .await?
            .reader
            .get_unpublished_inlined_delete_ids(table_id))
    }
    async fn table_storage_stats(&self, table_id: &str) -> CatalogResult<TableStorageStats> {
        self.read().await?.reader.table_storage_stats(table_id)
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
        if !replaced_delete_file_ids.is_empty() {
            batch.insert(
                0,
                actions::remove_delete_files(
                    table_id,
                    proto::Select::Listed,
                    &replaced_delete_file_ids,
                    None,
                ),
            );
        }
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
        // The inputs' files leave the live set and are retired, but keep their
        // manifest rows: a scan that captured them as live before the in-memory
        // swap still reads them from those rows. The retirement sweep forgets
        // the rows together with the files, once no scan holds them.
        inner
            .commit(vec![
                actions::remove_groups(table_id, proto::Select::Listed, old_snapshot_ids, true),
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
        let action = inner.table_stats_action(stats).await?;
        inner.commit(vec![action]).await
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
    async fn upsert_snapshot_files(&self, files: &[SnapshotFile]) -> CatalogResult<()> {
        if files.is_empty() {
            return Ok(());
        }
        let mut groups: Vec<(&str, &str, Vec<SnapshotFile>)> = Vec::new();
        for file in files {
            match groups.iter_mut().find(|(table, snapshot, _)| {
                *table == file.table_id && *snapshot == file.snapshot_id
            }) {
                Some((_, _, rows)) => rows.push(file.clone()),
                None => groups.push((&file.table_id, &file.snapshot_id, vec![file.clone()])),
            }
        }
        self.inner
            .take()
            .await
            .commit(
                groups
                    .iter()
                    .map(|(table, snapshot, rows)| actions::add_files(table, snapshot, rows, true))
                    .collect(),
            )
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
    async fn remove_snapshot_files(
        &self,
        table_id: &str,
        snapshot_id: &str,
        file_names: &[String],
    ) -> CatalogResult<()> {
        if file_names.is_empty() {
            return Ok(());
        }
        // The log removes a snapshot's files by snapshot, so one record removes
        // the snapshot's manifest and adds back the files it still holds.
        let mut inner = self.inner.take().await;
        let kept: Vec<proto::FileEntry> = inner
            .table(table_id)?
            .files
            .iter()
            .filter(|((snapshot, path), _)| snapshot == snapshot_id && !file_names.contains(path))
            .map(|(_, file)| file.clone())
            .collect();
        let mut batch = vec![actions::remove_files(
            table_id,
            proto::Select::Listed,
            &[snapshot_id.to_string()],
        )];
        if !kept.is_empty() {
            batch.push(Action::new(Kind::AddFiles(proto::AddFiles {
                table_id: table_id.to_string(),
                snapshot_id: snapshot_id.to_string(),
                files: kept,
                upsert: false,
            })));
        }
        inner.commit(batch).await
    }
    async fn verify_existing_stored_data_files(&self, table_name: &str) -> CatalogResult<()> {
        let inner = self.read().await?;
        let table = inner.reader.get_table(table_name).await?;
        crate::cayenne_catalog::verify_stored_data_files_in(
            &table,
            table.vortex_config.memory_mode,
            crate::metadata::SettingsChangeAction::KeepStored,
            async { Ok(inner.reader.get_all_snapshot_sequences(&table.table_id)) },
            |snapshot_id| {
                std::future::ready(Ok(inner
                    .reader
                    .get_snapshot_files(&table.table_id, &snapshot_id)))
            },
        )
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
        Ok(self
            .read()
            .await?
            .reader
            .get_inlined_data_above_sequence(table_id, after_sequence))
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
    /// One record holding the seal's or checkpoint's inline rows, delete files,
    /// snapshot sequence and inline clear, in the order the SQLite metastore
    /// applies them in one transaction.
    async fn commit_mem_tier_durable(&self, commit: MemTierDurableCommit) -> CatalogResult<()> {
        let MemTierDurableCommit {
            table_id,
            mut pure_delete_files,
            mut reinsert_delete_files,
            insert_pk_bytes,
            sequence_number,
            snapshot_sequence,
            inline_data,
            clear_inlined,
            insert_records,
            table_statistics,
            keep_file_statistics_of,
            manifest_files,
        } = commit;
        if !insert_pk_bytes.is_empty()
            && !reinsert_delete_files
                .iter()
                .any(|f| f.deletion_type == crate::metadata::DeletionType::KeyBased)
        {
            return Err(invalid("reinsert keys require a key-based delete file"));
        }
        // Pure deletes stay delete-only; only the reinsert group is stamped.
        let reinsert_sequence = (!insert_pk_bytes.is_empty()).then_some(sequence_number);
        for file in &mut pure_delete_files {
            file.reinsert_sequence = None;
        }
        for file in &mut reinsert_delete_files {
            if reinsert_sequence.is_some_and(|sequence| sequence <= file.sequence_number) {
                return Err(invalid("insert sequence must exceed delete sequence"));
            }
            file.reinsert_sequence = (file.deletion_type
                == crate::metadata::DeletionType::KeyBased)
                .then_some(reinsert_sequence)
                .flatten();
        }
        pure_delete_files.append(&mut reinsert_delete_files);
        if pure_delete_files.iter().any(|f| f.table_id != table_id)
            || inline_data.iter().any(|d| d.table_id != table_id)
            || table_statistics
                .as_ref()
                .is_some_and(|stats| stats.table_id != table_id)
            || manifest_files.iter().any(|f| f.table_id != table_id)
        {
            return Err(invalid(format!(
                "Mem-tier commit rows must belong to table '{table_id}'"
            )));
        }
        let mut inner = self.inner.take().await;
        let mut batch: Vec<Action> = inline_data
            .iter()
            .map(|data| {
                let id = if data.inlined_id.is_empty() {
                    uuid::Uuid::now_v7().to_string()
                } else {
                    data.inlined_id.clone()
                };
                actions::add_inlined(&table_id, data, &id, sequence_number)
            })
            .collect();
        let (_, deletes) = inner.delete_batch(&pure_delete_files)?;
        batch.extend(deletes);
        let mut by_snapshot: Vec<(&str, Vec<SnapshotFile>)> = Vec::new();
        for file in &manifest_files {
            match by_snapshot
                .iter_mut()
                .find(|(snapshot, _)| *snapshot == file.snapshot_id)
            {
                Some((_, rows)) => rows.push(file.clone()),
                None => by_snapshot.push((&file.snapshot_id, vec![file.clone()])),
            }
        }
        for (snapshot, rows) in &by_snapshot {
            batch.push(actions::add_files(&table_id, snapshot, rows, true));
        }
        if let Some(snapshot) = snapshot_sequence {
            batch.push(actions::set_group(
                &table_id,
                &snapshot.snapshot_id,
                snapshot.sequence_number,
            ));
        }
        if clear_inlined {
            batch.push(actions::remove_inlined(&table_id, proto::Select::All, &[]));
            batch.push(actions::remove_inlined_deletes(
                &table_id,
                proto::Select::All,
                &[],
            ));
        }
        if !insert_records.is_empty() {
            batch.push(Action::new(Kind::AddInsertRecords(proto::AddKeys {
                table_id: table_id.clone(),
                keys: insert_records
                    .into_iter()
                    .map(|pk| proto::PkSeq {
                        pk,
                        seq: sequence_number,
                    })
                    .collect(),
            })));
        }
        if let Some(stats) = &table_statistics {
            batch.push(inner.table_stats_action(stats).await?);
        }
        if let Some(snapshot_id) = keep_file_statistics_of {
            batch.push(actions::remove_file_stats(
                &table_id,
                proto::Select::AllExcept,
                &[snapshot_id],
            ));
        }
        if batch.is_empty() {
            return Ok(());
        }
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
