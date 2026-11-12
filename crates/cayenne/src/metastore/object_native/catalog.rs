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

//! The object-native catalog: a name registry and one head log per dataset,
//! so datasets commit independently. A transaction across datasets commits by
//! two-phase commit over their logs.

use super::{Domain, invalid, registry::ObjectNativeCatalog};
use crate::{
    catalog::{CatalogResult, MemTierDurableCommit, MetadataCatalog, SnapshotSequenceCommit},
    metadata::{
        ColdTierFile, CreateTableOptions, DeleteFile, IndexRunRecord, InlinedData,
        InlinedDataStats, InlinedDelete, PartitionMetadata, SnapshotFile, SnapshotFileStatistics,
        TableMetadata, TableStatistics, TableStorageStats,
    },
};
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use std::{collections::HashMap, sync::Arc};

#[async_trait]
impl MetadataCatalog for ObjectNativeCatalog {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    async fn commit_pending(&self) -> CatalogResult<()> {
        self.on_every_log(|domain| async move { domain.commit_pending().await })
            .await
            .map(|_| ())
    }
    async fn commit_publications(
        &self,
        publications: &[crate::catalog::CatalogPublication<'_>],
    ) -> CatalogResult<()> {
        let mut groups: Vec<(Arc<Domain>, Vec<crate::catalog::CatalogPublication<'_>>)> =
            Vec::new();
        for publication in publications {
            let domain = self.for_table(publication.table_id);
            match groups
                .iter_mut()
                .find(|(grouped, _)| Arc::ptr_eq(grouped, &domain))
            {
                Some((_, group)) => group.push(publication.clone()),
                None => groups.push((domain, vec![publication.clone()])),
            }
        }
        match groups.len() {
            0 => Ok(()),
            1 => {
                let (domain, publications) = groups.remove(0);
                domain.commit_publications(&publications).await
            }
            _ => self.commit_across(groups).await,
        }
    }
    async fn init(&self) -> CatalogResult<()> {
        self.on_every_log(|domain| async move { domain.init().await })
            .await
            .map(|_| ())
    }
    async fn shutdown(&self) -> CatalogResult<()> {
        self.on_every_log(|domain| async move { domain.shutdown().await })
            .await
            .map(|_| ())
    }
    async fn list_table_names(&self) -> CatalogResult<Vec<String>> {
        let mut names: Vec<String> = self
            .on_every_log(|domain| async move { domain.list_table_names().await })
            .await?
            .into_iter()
            .flatten()
            .collect();
        names.sort();
        names.dedup();
        Ok(names)
    }
    async fn create_table(&self, options: CreateTableOptions) -> CatalogResult<String> {
        let _ddl = self.ddl.lock().await;
        if let Some(folder) = self.folder_named_by(&options.table_name, &options.base_path) {
            // A create that names a folder whose deletion is pending keeps it.
            self.dropping.lock().remove(&folder);
        }
        self.create_dataset(options).await
    }
    async fn drop_table(&self, table_name: &str) -> CatalogResult<bool> {
        let _ddl = self.ddl.lock().await;
        self.drop_dataset(table_name).await
    }
    async fn add_delete_files(&self, delete_files: Vec<DeleteFile>) -> CatalogResult<()> {
        let Some(first) = delete_files.first() else {
            return Ok(());
        };
        let domain = self.for_table(&first.table_id);
        if delete_files
            .iter()
            .any(|file| !Arc::ptr_eq(&self.for_table(&file.table_id), &domain))
        {
            return Err(invalid(
                "deletion vectors added together must belong to one dataset",
            ));
        }
        domain.add_delete_files(delete_files).await
    }
    async fn checkpoint_wal(&self) -> CatalogResult<()> {
        Ok(())
    }
    async fn incremental_vacuum(&self) -> CatalogResult<u64> {
        Ok(0)
    }
    async fn export_dataset_slice(
        &self,
        dataset_name: &str,
        data_dir_anchor: &std::path::Path,
    ) -> CatalogResult<crate::metastore::snapshot::DatasetMetastoreSlice> {
        self.for_name(dataset_name)
            .export_dataset_slice(dataset_name, data_dir_anchor)
            .await
    }
    async fn import_dataset_slice(
        &self,
        slice: &crate::metastore::snapshot::DatasetMetastoreSlice,
        data_dir_anchor: &std::path::Path,
    ) -> CatalogResult<()> {
        self.registry
            .import_dataset_slice(slice, data_dir_anchor)
            .await
    }
    async fn commit_pending_for(&self, table_id: &str) -> CatalogResult<()> {
        self.for_table(table_id).commit_pending_for(table_id).await
    }
    fn commits_source_positions(&self) -> bool {
        true
    }
    async fn list_pending_write_back(
        &self,
        table_id: &str,
        limit: usize,
        after: Option<&(i64, Vec<u8>)>,
    ) -> CatalogResult<Vec<(Vec<u8>, i64)>> {
        self.for_table(table_id)
            .list_pending_write_back(table_id, limit, after)
            .await
    }
    async fn clear_pending_write_back(
        &self,
        table_id: &str,
        keys: &[(Vec<u8>, i64)],
    ) -> CatalogResult<()> {
        self.for_table(table_id)
            .clear_pending_write_back(table_id, keys)
            .await
    }
    async fn pending_write_back_count(&self, table_id: &str) -> CatalogResult<i64> {
        self.for_table(table_id)
            .pending_write_back_count(table_id)
            .await
    }
    async fn get_table(&self, table_name: &str) -> CatalogResult<TableMetadata> {
        self.for_name(table_name).get_table(table_name).await
    }
    async fn get_sequence_number(&self, table_id: &str) -> CatalogResult<i64> {
        self.for_table(table_id).get_sequence_number(table_id).await
    }
    async fn get_table_delete_files(&self, table_id: &str) -> CatalogResult<Vec<DeleteFile>> {
        self.for_table(table_id)
            .get_table_delete_files(table_id)
            .await
    }
    async fn get_orphan_eligible_delete_files(
        &self,
        table_id: &str,
        max_sequence: i64,
        limit: usize,
    ) -> CatalogResult<Vec<DeleteFile>> {
        self.for_table(table_id)
            .get_orphan_eligible_delete_files(table_id, max_sequence, limit)
            .await
    }
    async fn get_insert_records(&self, table_id: &str) -> CatalogResult<HashMap<Box<[u8]>, i64>> {
        self.for_table(table_id).get_insert_records(table_id).await
    }
    async fn get_snapshot_sequence(
        &self,
        table_id: &str,
        snapshot_id: &str,
    ) -> CatalogResult<Option<i64>> {
        self.for_table(table_id)
            .get_snapshot_sequence(table_id, snapshot_id)
            .await
    }
    async fn get_all_snapshot_sequences(
        &self,
        table_id: &str,
    ) -> CatalogResult<HashMap<String, i64>> {
        self.for_table(table_id)
            .get_all_snapshot_sequences(table_id)
            .await
    }
    async fn get_partitions(&self, table_id: &str) -> CatalogResult<Vec<PartitionMetadata>> {
        self.for_table(table_id).get_partitions(table_id).await
    }
    async fn get_table_statistics(&self, table_id: &str) -> CatalogResult<Option<TableStatistics>> {
        self.for_table(table_id)
            .get_table_statistics(table_id)
            .await
    }
    async fn get_snapshot_file_statistics(
        &self,
        table_id: &str,
        snapshot_id: &str,
        file_path: &str,
    ) -> CatalogResult<Option<SnapshotFileStatistics>> {
        self.for_table(table_id)
            .get_snapshot_file_statistics(table_id, snapshot_id, file_path)
            .await
    }
    async fn get_all_snapshot_files(&self, table_id: &str) -> CatalogResult<Vec<SnapshotFile>> {
        self.for_table(table_id)
            .get_all_snapshot_files(table_id)
            .await
    }
    fn keeps_retirement_record(&self) -> bool {
        true
    }
    async fn retired_files(&self, table_id: &str) -> CatalogResult<Option<Vec<String>>> {
        self.for_table(table_id).retired_files(table_id).await
    }
    async fn committed_file_keys(
        &self,
        table_id: &str,
    ) -> CatalogResult<Option<std::collections::BTreeSet<String>>> {
        self.for_table(table_id).committed_file_keys(table_id).await
    }
    async fn forget_retired_files(&self, table_id: &str, keys: &[String]) -> CatalogResult<bool> {
        self.for_table(table_id)
            .forget_retired_files(table_id, keys)
            .await
    }
    async fn get_snapshot_files(
        &self,
        table_id: &str,
        snapshot_id: &str,
    ) -> CatalogResult<Vec<SnapshotFile>> {
        self.for_table(table_id)
            .get_snapshot_files(table_id, snapshot_id)
            .await
    }
    async fn list_cold_tier_files(&self, table_id: &str) -> CatalogResult<Vec<ColdTierFile>> {
        self.for_table(table_id)
            .list_cold_tier_files(table_id)
            .await
    }
    async fn get_pk_index(&self, table_id: &str) -> CatalogResult<Option<(String, Vec<u8>)>> {
        self.for_table(table_id).get_pk_index(table_id).await
    }
    async fn list_index_runs(&self, table_id: &str) -> CatalogResult<Vec<IndexRunRecord>> {
        self.for_table(table_id).list_index_runs(table_id).await
    }
    async fn get_inlined_data(&self, table_id: &str) -> CatalogResult<Vec<InlinedData>> {
        self.for_table(table_id).get_inlined_data(table_id).await
    }
    async fn get_inlined_data_for_partition(
        &self,
        table_id: &str,
        partition_key: &str,
    ) -> CatalogResult<Vec<InlinedData>> {
        self.for_table(table_id)
            .get_inlined_data_for_partition(table_id, partition_key)
            .await
    }
    async fn get_inlined_data_count(&self, table_id: &str) -> CatalogResult<i64> {
        self.for_table(table_id)
            .get_inlined_data_count(table_id)
            .await
    }
    async fn get_inlined_data_stats(&self, table_id: &str) -> CatalogResult<InlinedDataStats> {
        self.for_table(table_id)
            .get_inlined_data_stats(table_id)
            .await
    }
    async fn get_inlined_deletes(&self, table_id: &str) -> CatalogResult<Vec<InlinedDelete>> {
        self.for_table(table_id).get_inlined_deletes(table_id).await
    }
    async fn get_published_inlined_deletes(
        &self,
        table_id: &str,
    ) -> CatalogResult<Vec<InlinedDelete>> {
        self.for_table(table_id)
            .get_published_inlined_deletes(table_id)
            .await
    }
    async fn get_unpublished_inlined_delete_ids(
        &self,
        table_id: &str,
    ) -> CatalogResult<Vec<String>> {
        self.for_table(table_id)
            .get_unpublished_inlined_delete_ids(table_id)
            .await
    }
    async fn table_storage_stats(&self, table_id: &str) -> CatalogResult<TableStorageStats> {
        self.for_table(table_id).table_storage_stats(table_id).await
    }
    async fn update_table_schema(&self, table_id: &str, schema: &SchemaRef) -> CatalogResult<()> {
        self.for_table(table_id)
            .update_table_schema(table_id, schema)
            .await
    }
    async fn update_table_schema_dropping_statistics(
        &self,
        table_id: &str,
        schema: &SchemaRef,
    ) -> CatalogResult<()> {
        self.for_table(table_id)
            .update_table_schema_dropping_statistics(table_id, schema)
            .await
    }
    async fn set_current_snapshot(
        &self,
        table_id: &str,
        replaced_snapshot_id: &str,
        new_snapshot_id: &str,
    ) -> CatalogResult<()> {
        self.for_table(table_id)
            .set_current_snapshot(table_id, replaced_snapshot_id, new_snapshot_id)
            .await
    }
    async fn increment_sequence_number(&self, table_id: &str) -> CatalogResult<i64> {
        self.for_table(table_id)
            .increment_sequence_number(table_id)
            .await
    }
    async fn reserve_sequence_numbers(&self, table_id: &str, count: u32) -> CatalogResult<i64> {
        self.for_table(table_id)
            .reserve_sequence_numbers(table_id, count)
            .await
    }
    async fn add_delete_file(&self, delete_file: DeleteFile) -> CatalogResult<String> {
        self.for_table(&delete_file.table_id)
            .add_delete_file(delete_file)
            .await
    }
    async fn commit_delete_files_with_inlined_rewrite(
        &self,
        delete_files: Vec<DeleteFile>,
        table_id: &str,
        updated_data: Vec<InlinedData>,
        deleted_inlined_ids: Vec<String>,
        replaced_delete_file_ids: Vec<String>,
    ) -> CatalogResult<()> {
        self.for_table(table_id)
            .commit_delete_files_with_inlined_rewrite(
                delete_files,
                table_id,
                updated_data,
                deleted_inlined_ids,
                replaced_delete_file_ids,
            )
            .await
    }
    async fn remove_delete_files(
        &self,
        table_id: &str,
        delete_file_ids: &[String],
    ) -> CatalogResult<()> {
        self.for_table(table_id)
            .remove_delete_files(table_id, delete_file_ids)
            .await
    }
    async fn clear_delete_files(&self, table_id: &str) -> CatalogResult<()> {
        self.for_table(table_id).clear_delete_files(table_id).await
    }
    async fn add_insert_record(
        &self,
        table_id: &str,
        pk_bytes: Vec<u8>,
        sequence_number: i64,
    ) -> CatalogResult<()> {
        self.for_table(table_id)
            .add_insert_record(table_id, pk_bytes, sequence_number)
            .await
    }
    async fn add_insert_records_batch(
        &self,
        table_id: &str,
        pk_bytes_list: Vec<Vec<u8>>,
        sequence_number: i64,
    ) -> CatalogResult<()> {
        self.for_table(table_id)
            .add_insert_records_batch(table_id, pk_bytes_list, sequence_number)
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
        self.for_table(table_id)
            .commit_on_conflict_deletions(
                delete_files,
                table_id,
                insert_pk_bytes_list,
                insert_sequence,
                snapshot_sequence,
            )
            .await
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
        self.for_table(table_id)
            .commit_on_conflict_deletions_with_tombstone(
                delete_files,
                table_id,
                insert_pk_bytes_list,
                insert_sequence,
                snapshot_sequence,
                inline_tombstone,
                pending_durable_flips,
            )
            .await
    }
    async fn clear_insert_records(&self, table_id: &str) -> CatalogResult<()> {
        self.for_table(table_id)
            .clear_insert_records(table_id)
            .await
    }
    async fn set_snapshot_sequence(
        &self,
        table_id: &str,
        snapshot_id: &str,
        sequence_number: i64,
    ) -> CatalogResult<()> {
        self.for_table(table_id)
            .set_snapshot_sequence(table_id, snapshot_id, sequence_number)
            .await
    }
    async fn clear_snapshot_sequence(
        &self,
        table_id: &str,
        snapshot_id: &str,
    ) -> CatalogResult<()> {
        self.for_table(table_id)
            .clear_snapshot_sequence(table_id, snapshot_id)
            .await
    }
    async fn commit_compaction(
        &self,
        table_id: &str,
        replaced_snapshot_id: &str,
        new_snapshot_id: &str,
    ) -> CatalogResult<()> {
        self.for_table(table_id)
            .commit_compaction(table_id, replaced_snapshot_id, new_snapshot_id)
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
        self.for_table(table_id)
            .commit_compaction_fenced(
                table_id,
                replaced_snapshot_id,
                new_snapshot_id,
                cutoff,
                protected_snapshot_ids_to_clear,
            )
            .await
    }
    async fn swap_protected_snapshots(
        &self,
        table_id: &str,
        old_snapshot_ids: &[String],
        new_snapshot_id: &str,
        new_sequence_number: i64,
    ) -> CatalogResult<bool> {
        self.for_table(table_id)
            .swap_protected_snapshots(
                table_id,
                old_snapshot_ids,
                new_snapshot_id,
                new_sequence_number,
            )
            .await
    }
    async fn commit_overwrite(
        &self,
        table_id: &str,
        new_snapshot_id: &str,
        inlined: Option<&InlinedData>,
        delete_files: &[crate::metadata::DeleteFile],
    ) -> CatalogResult<()> {
        self.for_table(table_id)
            .commit_overwrite(table_id, new_snapshot_id, inlined, delete_files)
            .await
    }
    async fn add_partition(&self, partition: PartitionMetadata) -> CatalogResult<String> {
        self.for_table(&partition.table_id)
            .add_partition(partition)
            .await
    }
    async fn upsert_table_statistics(&self, stats: &TableStatistics) -> CatalogResult<()> {
        self.for_table(&stats.table_id)
            .upsert_table_statistics(stats)
            .await
    }
    async fn clear_table_statistics(&self, table_id: &str) -> CatalogResult<()> {
        self.for_table(table_id)
            .clear_table_statistics(table_id)
            .await
    }
    async fn upsert_snapshot_file_statistics(
        &self,
        stats: &SnapshotFileStatistics,
    ) -> CatalogResult<()> {
        self.for_table(&stats.table_id)
            .upsert_snapshot_file_statistics(stats)
            .await
    }
    async fn clear_snapshot_file_statistics_except(
        &self,
        table_id: &str,
        snapshot_id: &str,
    ) -> CatalogResult<()> {
        self.for_table(table_id)
            .clear_snapshot_file_statistics_except(table_id, snapshot_id)
            .await
    }
    async fn clear_snapshot_file_statistics(&self, table_id: &str) -> CatalogResult<()> {
        self.for_table(table_id)
            .clear_snapshot_file_statistics(table_id)
            .await
    }
    async fn upsert_snapshot_file(&self, file: &SnapshotFile) -> CatalogResult<()> {
        self.for_table(&file.table_id)
            .upsert_snapshot_file(file)
            .await
    }
    async fn upsert_snapshot_files(&self, files: &[SnapshotFile]) -> CatalogResult<()> {
        let Some(first) = files.first() else {
            return Ok(());
        };
        // One table's files go to one log: route by the first and check the rest.
        let domain = self.for_table(&first.table_id);
        if files.iter().all(|file| file.table_id == first.table_id) {
            return domain.upsert_snapshot_files(files).await;
        }
        for file in files {
            self.for_table(&file.table_id)
                .upsert_snapshot_file(file)
                .await?;
        }
        Ok(())
    }
    async fn replace_snapshot_files(
        &self,
        table_id: &str,
        snapshot_id: &str,
        files: &[SnapshotFile],
    ) -> CatalogResult<()> {
        self.for_table(table_id)
            .replace_snapshot_files(table_id, snapshot_id, files)
            .await
    }
    async fn clear_snapshot_cached_metadata(
        &self,
        table_id: &str,
        snapshot_id: &str,
    ) -> CatalogResult<()> {
        self.for_table(table_id)
            .clear_snapshot_cached_metadata(table_id, snapshot_id)
            .await
    }
    async fn clear_snapshot_files_except(
        &self,
        table_id: &str,
        snapshot_id: &str,
    ) -> CatalogResult<()> {
        self.for_table(table_id)
            .clear_snapshot_files_except(table_id, snapshot_id)
            .await
    }
    async fn remove_snapshot_files(
        &self,
        table_id: &str,
        snapshot_id: &str,
        file_names: &[String],
    ) -> CatalogResult<()> {
        self.for_table(table_id)
            .remove_snapshot_files(table_id, snapshot_id, file_names)
            .await
    }
    async fn verify_existing_stored_data_files(&self, table_name: &str) -> CatalogResult<()> {
        self.for_name(table_name)
            .verify_existing_stored_data_files(table_name)
            .await
    }
    async fn clear_snapshot_files(&self, table_id: &str) -> CatalogResult<()> {
        self.for_table(table_id)
            .clear_snapshot_files(table_id)
            .await
    }
    async fn commit_overwrite_to_cold(
        &self,
        table_id: &str,
        new_snapshot_id: &str,
        cold_files: &[ColdTierFile],
    ) -> CatalogResult<()> {
        self.for_table(table_id)
            .commit_overwrite_to_cold(table_id, new_snapshot_id, cold_files)
            .await
    }
    async fn upsert_pk_index(
        &self,
        table_id: &str,
        snapshot_id: &str,
        index_blob: &[u8],
    ) -> CatalogResult<()> {
        self.for_table(table_id)
            .upsert_pk_index(table_id, snapshot_id, index_blob)
            .await
    }
    async fn clear_pk_index(&self, table_id: &str) -> CatalogResult<()> {
        self.for_table(table_id).clear_pk_index(table_id).await
    }
    async fn register_index_run(&self, run: &IndexRunRecord) -> CatalogResult<()> {
        self.for_table(&run.table_id).register_index_run(run).await
    }
    async fn remove_index_run(
        &self,
        table_id: &str,
        index_key: &str,
        run_name: &str,
    ) -> CatalogResult<()> {
        self.for_table(table_id)
            .remove_index_run(table_id, index_key, run_name)
            .await
    }
    async fn add_inlined_data(&self, data: InlinedData) -> CatalogResult<String> {
        self.for_table(&data.table_id).add_inlined_data(data).await
    }
    async fn get_inlined_data_above_sequence(
        &self,
        table_id: &str,
        after_sequence: i64,
    ) -> CatalogResult<Vec<InlinedData>> {
        self.for_table(table_id)
            .get_inlined_data_above_sequence(table_id, after_sequence)
            .await
    }
    async fn clear_inlined_data(&self, table_id: &str) -> CatalogResult<()> {
        self.for_table(table_id).clear_inlined_data(table_id).await
    }
    async fn clear_inlined_data_and_deletes(&self, table_id: &str) -> CatalogResult<()> {
        self.for_table(table_id)
            .clear_inlined_data_and_deletes(table_id)
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
        self.for_table(table_id)
            .commit_on_conflict_deletions_and_clear_inlined(
                delete_files,
                table_id,
                insert_pk_bytes_list,
                insert_sequence,
                snapshot_sequence,
            )
            .await
    }
    async fn commit_mem_tier_durable(&self, commit: MemTierDurableCommit) -> CatalogResult<()> {
        self.for_table(&commit.table_id)
            .commit_mem_tier_durable(commit)
            .await
    }
    async fn add_inlined_delete(&self, delete: InlinedDelete) -> CatalogResult<String> {
        self.for_table(&delete.table_id)
            .add_inlined_delete(delete)
            .await
    }
    async fn mark_inlined_delete_published(
        &self,
        table_id: &str,
        inlined_id: &str,
    ) -> CatalogResult<()> {
        self.for_table(table_id)
            .mark_inlined_delete_published(table_id, inlined_id)
            .await
    }
    async fn mark_inlined_delete_unpublished(
        &self,
        table_id: &str,
        inlined_id: &str,
    ) -> CatalogResult<()> {
        self.for_table(table_id)
            .mark_inlined_delete_unpublished(table_id, inlined_id)
            .await
    }
    async fn publish_orphan_inlined_deletes(&self, table_id: &str) -> CatalogResult<u64> {
        self.for_table(table_id)
            .publish_orphan_inlined_deletes(table_id)
            .await
    }
    async fn commit_inlined_mutation(
        &self,
        table_id: &str,
        updated_data: Vec<InlinedData>,
        deleted_inlined_ids: Vec<String>,
        data: Vec<InlinedData>,
        assigned_sequence: i64,
    ) -> CatalogResult<Option<i64>> {
        self.for_table(table_id)
            .commit_inlined_mutation(
                table_id,
                updated_data,
                deleted_inlined_ids,
                data,
                assigned_sequence,
            )
            .await
    }
    async fn clear_inlined_deletes(&self, table_id: &str) -> CatalogResult<()> {
        self.for_table(table_id)
            .clear_inlined_deletes(table_id)
            .await
    }
}
