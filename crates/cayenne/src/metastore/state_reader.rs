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

//! Immutable, storage-only view of the prototype's per-table shadow logs.
//!
//! Each head is opened independently; this is not a cross-table snapshot.
//! Data and deletion-vector files must remain available at their recorded paths.
//! Provider scans retain Cayenne's file discovery: manifests are best-effort
//! caches, so the writer's data directories must be quiescent. This prototype
//! does not pin files against a live writer's reclamation. SQL-metastore slice
//! export is unsupported.

use crate::{
    catalog::{CatalogError, CatalogResult, MetadataCatalog, SnapshotSequenceCommit},
    metadata::{
        ColdTierFile, CreateTableOptions, DeleteFile, DeletionType, IndexRunRecord, InlinedData,
        InlinedDataStats, InlinedDelete, PartitionMetadata, SnapshotFile, SnapshotFileStatistics,
        TableMetadata, TableStatistics, TableStorageStats,
    },
};
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use cayenne_metastore_actions::{DatasetState, ObjRef, TableState};
use object_store::{ObjectStore, ObjectStoreExt, path::Path};
use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
};

/// A frozen catalog reconstructed exclusively from shadow storage.
/// Data files remain in the locations recorded by the writer.
pub struct ShadowCatalog {
    pub(crate) state: DatasetState,
    empty: TableState,
    pub(crate) store: Arc<dyn ObjectStore>,
    pub(crate) prefix: Path,
}

fn invalid(error: impl std::fmt::Display) -> CatalogError {
    CatalogError::InvalidOperationNoSource {
        message: error.to_string(),
    }
}

impl ShadowCatalog {
    /// Open every table log under `prefix`, without a SQL metastore or tap state.
    ///
    /// # Errors
    /// Returns an error on unreadable logs, invalid actions, or duplicate table identities.
    pub async fn open(store: Arc<dyn ObjectStore>, prefix: Path) -> CatalogResult<Self> {
        let datasets = prefix.clone().join("datasets");
        let entries = store
            .list_with_delimiter(Some(&datasets))
            .await
            .map_err(invalid)?;
        let mut state = DatasetState::default();
        let mut names = BTreeSet::new();
        for table_prefix in entries.common_prefixes {
            let rebuilt = crate::metastore_tap::shadow::replay(Arc::clone(&store), table_prefix)
                .await
                .map_err(invalid)?;
            for (id, table) in rebuilt.tables {
                if !names.insert(table.name.clone()) || state.tables.insert(id, table).is_some() {
                    return Err(invalid("duplicate shadow table identity"));
                }
            }
        }
        Ok(Self {
            empty: TableState::default(),
            state,
            store,
            prefix,
        })
    }

    pub(crate) fn from_state(
        state: DatasetState,
        store: Arc<dyn ObjectStore>,
        prefix: Path,
    ) -> Self {
        Self {
            state,
            empty: TableState::default(),
            store,
            prefix,
        }
    }

    fn table(&self, id: &str) -> CatalogResult<&TableState> {
        self.state
            .tables
            .get(id)
            .ok_or_else(|| CatalogError::TableNotFound {
                table_name: id.to_string(),
            })
    }

    fn rows(&self, id: &str) -> &TableState {
        self.state.tables.get(id).unwrap_or(&self.empty)
    }

    async fn blob(&self, reference: &ObjRef) -> CatalogResult<Vec<u8>> {
        // Clearing cold statistics records the digest of the empty byte string.
        if reference == &ObjRef::of(&[]) {
            return Ok(Vec::new());
        }
        let digest =
            blake3::Hash::from_bytes(reference.digest.as_slice().try_into().map_err(invalid)?)
                .to_hex();
        let path = self
            .prefix
            .clone()
            .join("objects")
            .join("blobs")
            .join(digest.as_str());
        let bytes = self
            .store
            .get(&path)
            .await
            .map_err(invalid)?
            .bytes()
            .await
            .map_err(invalid)?;
        if &ObjRef::of(&bytes) != reference {
            return Err(invalid(format!(
                "shadow blob {path} failed integrity validation"
            )));
        }
        Ok(bytes.to_vec())
    }
}

#[async_trait]
impl MetadataCatalog for ShadowCatalog {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    async fn commit_publications(
        &self,
        _publications: &[crate::catalog::CatalogPublication],
    ) -> CatalogResult<()> {
        Err(invalid("the shadow catalog is read-only"))
    }
    async fn list_pending_write_back(
        &self,
        table_id: &str,
        limit: usize,
        after: Option<&(i64, Vec<u8>)>,
    ) -> CatalogResult<Vec<(Vec<u8>, i64)>> {
        let mut rows: Vec<_> = self
            .rows(table_id)
            .pending_write_back
            .iter()
            .map(|(pk, seq)| (pk.clone(), *seq))
            .filter(|(pk, seq)| after.is_none_or(|(s, k)| (seq, pk) > (s, k)))
            .collect();
        rows.sort_by(|(a, x), (b, y)| (x, a).cmp(&(y, b)));
        rows.truncate(limit);
        Ok(rows)
    }
    async fn clear_pending_write_back(
        &self,
        _table_id: &str,
        _keys: &[(Vec<u8>, i64)],
    ) -> CatalogResult<()> {
        Err(invalid("the shadow catalog is read-only"))
    }
    async fn pending_write_back_count(&self, table_id: &str) -> CatalogResult<i64> {
        count(self.rows(table_id).pending_write_back.len())
    }
    async fn init(&self) -> CatalogResult<()> {
        Ok(())
    }
    async fn shutdown(&self) -> CatalogResult<()> {
        Ok(())
    }
    async fn list_table_names(&self) -> CatalogResult<Vec<String>> {
        let mut names: Vec<_> = self.state.tables.values().map(|t| t.name.clone()).collect();
        names.sort();
        Ok(names)
    }
    async fn get_table(&self, table_name: &str) -> CatalogResult<TableMetadata> {
        let (id, t) = self
            .state
            .tables
            .iter()
            .find(|(_, t)| t.name == table_name)
            .ok_or_else(|| CatalogError::TableNotFound {
                table_name: table_name.to_string(),
            })?;
        let schema = self
            .blob(
                t.schema
                    .as_ref()
                    .ok_or_else(|| invalid("shadow table has no schema"))?,
            )
            .await?;
        Ok(TableMetadata {
            table_id: id.clone(),
            table_name: t.name.clone(),
            path: t.path.clone(),
            path_is_relative: t.path_is_relative,
            schema: arrow_tools::map_entries::conforming_schema(Arc::new(
                crate::cayenne_catalog::deserialize_schema_ipc_base64(
                    std::str::from_utf8(&schema).map_err(invalid)?,
                )?,
            )),
            primary_key: t
                .primary_key_json
                .as_deref()
                .map(serde_json::from_str)
                .transpose()
                .map_err(invalid)?
                .unwrap_or_default(),
            on_conflict: t
                .on_conflict_json
                .as_deref()
                .map(datafusion_table_providers::util::on_conflict::OnConflict::try_from)
                .transpose()
                .map_err(invalid)?,
            current_snapshot_id: t.current_snapshot_id.clone(),
            partition_column: t.partition_column.clone(),
            vortex_config: t
                .vortex_config_json
                .as_deref()
                .map(serde_json::from_str)
                .transpose()
                .map_err(invalid)?
                .unwrap_or_default(),
            current_sequence_number: t.seq,
        })
    }
    async fn get_sequence_number(&self, table_id: &str) -> CatalogResult<i64> {
        Ok(self.table(table_id)?.seq)
    }
    async fn get_table_delete_files(&self, table_id: &str) -> CatalogResult<Vec<DeleteFile>> {
        Ok(self
            .rows(table_id)
            .delete_files
            .values()
            .map(|d| DeleteFile {
                delete_file_id: d.id.clone(),
                table_id: table_id.to_string(),
                source_data_file_path: d.source.clone(),
                path: d.path.clone(),
                path_is_relative: d.relative,
                format: d.format.clone(),
                delete_count: d.count,
                file_size_bytes: d.size,
                deletion_type: DeletionType::default(),
                sequence_number: d.seq,
                reinsert_sequence: d.reinsert,
            })
            .collect())
    }
    async fn get_orphan_eligible_delete_files(
        &self,
        table_id: &str,
        max_sequence: i64,
        limit: usize,
    ) -> CatalogResult<Vec<DeleteFile>> {
        Ok(self
            .get_table_delete_files(table_id)
            .await?
            .into_iter()
            .filter(|d| d.source_data_file_path.is_none() && d.sequence_number <= max_sequence)
            .take(limit)
            .collect())
    }
    async fn get_insert_records(&self, table_id: &str) -> CatalogResult<HashMap<Box<[u8]>, i64>> {
        Ok(self
            .rows(table_id)
            .insert_records
            .iter()
            .map(|(k, v)| (k.clone().into_boxed_slice(), *v))
            .collect())
    }
    async fn get_snapshot_sequence(
        &self,
        table_id: &str,
        snapshot_id: &str,
    ) -> CatalogResult<Option<i64>> {
        Ok(self.rows(table_id).groups.get(snapshot_id).copied())
    }
    async fn get_all_snapshot_sequences(
        &self,
        table_id: &str,
    ) -> CatalogResult<HashMap<String, i64>> {
        Ok(self.rows(table_id).groups.clone().into_iter().collect())
    }
    async fn get_partitions(&self, table_id: &str) -> CatalogResult<Vec<PartitionMetadata>> {
        Ok(self
            .rows(table_id)
            .partitions
            .iter()
            .map(|(id, p)| PartitionMetadata {
                partition_id: id.clone(),
                table_id: table_id.to_string(),
                partition_columns: p.columns.clone(),
                partition_values: p.values.clone(),
                path: p.path.clone(),
                path_is_relative: p.path_is_relative,
                record_count: p.record_count,
                file_size_bytes: p.size,
            })
            .collect())
    }
    async fn get_table_statistics(&self, table_id: &str) -> CatalogResult<Option<TableStatistics>> {
        let Some(s) = &self.rows(table_id).stats else {
            return Ok(None);
        };
        Ok(Some(TableStatistics {
            table_id: table_id.to_string(),
            statistics_blob: self.blob(&s.blob).await?,
            num_rows: s.num_rows,
            num_rows_exact: s.exact,
            ndv_sketches: match &s.ndv {
                Some(r) => Some(self.blob(r).await?),
                None => None,
            },
        }))
    }
    async fn get_snapshot_file_statistics(
        &self,
        table_id: &str,
        snapshot_id: &str,
        file_path: &str,
    ) -> CatalogResult<Option<SnapshotFileStatistics>> {
        let Some(s) = self
            .rows(table_id)
            .file_stats
            .get(&(snapshot_id.to_string(), file_path.to_string()))
        else {
            return Ok(None);
        };
        Ok(Some(SnapshotFileStatistics {
            table_id: table_id.to_string(),
            snapshot_id: snapshot_id.to_string(),
            file_path: file_path.to_string(),
            file_size_bytes: s.size,
            num_rows: s.rows,
            statistics_blob: self
                .blob(
                    s.blob
                        .as_ref()
                        .ok_or_else(|| invalid("file statistics have no blob"))?,
                )
                .await?,
        }))
    }
    async fn get_all_snapshot_files(&self, table_id: &str) -> CatalogResult<Vec<SnapshotFile>> {
        Ok(self
            .rows(table_id)
            .files
            .iter()
            .map(|((snapshot, path), f)| SnapshotFile {
                table_id: table_id.to_string(),
                snapshot_id: snapshot.clone(),
                file_path: path.clone(),
                row_count: f.rows,
                file_size_bytes: f.size,
                min_sequence: f.min_seq,
                max_sequence: f.max_seq,
                digest: f.digest.clone(),
            })
            .collect())
    }
    async fn get_snapshot_files(
        &self,
        table_id: &str,
        snapshot_id: &str,
    ) -> CatalogResult<Vec<SnapshotFile>> {
        Ok(self
            .get_all_snapshot_files(table_id)
            .await?
            .into_iter()
            .filter(|f| f.snapshot_id == snapshot_id)
            .collect())
    }
    async fn list_cold_tier_files(&self, table_id: &str) -> CatalogResult<Vec<ColdTierFile>> {
        let mut files = Vec::new();
        for f in self.rows(table_id).cold.values() {
            files.push(ColdTierFile {
                table_id: table_id.to_string(),
                file_url: f.url.clone(),
                row_count: f.rows,
                file_size_bytes: f.size,
                min_sequence: f.min_seq,
                max_sequence: f.max_seq,
                statistics_blob: self
                    .blob(
                        f.stats
                            .as_ref()
                            .ok_or_else(|| invalid("cold file has no statistics reference"))?,
                    )
                    .await?,
                pk_bloom: match &f.bloom {
                    Some(r) => Some(self.blob(r).await?),
                    None => None,
                },
            });
        }
        Ok(files)
    }
    async fn get_pk_index(&self, table_id: &str) -> CatalogResult<Option<(String, Vec<u8>)>> {
        match &self.rows(table_id).pk_index {
            Some((s, r)) => Ok(Some((s.clone(), self.blob(r).await?))),
            None => Ok(None),
        }
    }
    async fn list_index_runs(&self, table_id: &str) -> CatalogResult<Vec<IndexRunRecord>> {
        Ok(self
            .rows(table_id)
            .index_runs
            .iter()
            .map(|((key, name), (rows, size))| IndexRunRecord {
                table_id: table_id.to_string(),
                index_key: key.clone(),
                run_name: name.clone(),
                row_count: *rows,
                size_bytes: *size,
            })
            .collect())
    }
    async fn get_inlined_data(&self, table_id: &str) -> CatalogResult<Vec<InlinedData>> {
        let mut rows: Vec<_> = self
            .rows(table_id)
            .inlined_data
            .iter()
            .map(|(id, r)| InlinedData {
                inlined_id: id.clone(),
                table_id: table_id.to_string(),
                partition_key: r.partition_key.clone(),
                data_ipc: r.raw_payload.clone(),
                record_count: r.count,
                sequence_number: r.seq,
                created_at: r.created_at.clone(),
            })
            .collect();
        rows.sort_by_key(|r| r.sequence_number);
        Ok(rows)
    }
    async fn get_inlined_data_for_partition(
        &self,
        table_id: &str,
        partition_key: &str,
    ) -> CatalogResult<Vec<InlinedData>> {
        Ok(self
            .get_inlined_data(table_id)
            .await?
            .into_iter()
            .filter(|r| r.partition_key.as_deref() == Some(partition_key))
            .collect())
    }
    async fn get_inlined_data_count(&self, table_id: &str) -> CatalogResult<i64> {
        sum(self.rows(table_id).inlined_data.values().map(|r| r.count))
    }
    async fn get_inlined_data_stats(&self, table_id: &str) -> CatalogResult<InlinedDataStats> {
        let t = self.rows(table_id);
        Ok(InlinedDataStats {
            record_count: self.get_inlined_data_count(table_id).await?,
            entry_count: count(t.inlined_data.len())?,
            ipc_bytes: sum(t
                .inlined_data
                .values()
                .map(|r| count(r.raw_payload.len()))
                .collect::<CatalogResult<Vec<_>>>()?)?,
            tombstone_entry_count: count(t.inlined_deletes.len())?,
            tombstone_ipc_bytes: sum(t
                .inlined_deletes
                .values()
                .map(|r| count(r.raw_payload.len()))
                .collect::<CatalogResult<Vec<_>>>()?)?,
        })
    }
    async fn get_inlined_deletes(&self, table_id: &str) -> CatalogResult<Vec<InlinedDelete>> {
        let mut rows: Vec<_> = self
            .rows(table_id)
            .inlined_deletes
            .iter()
            .map(|(id, r)| InlinedDelete {
                inlined_id: id.clone(),
                table_id: table_id.to_string(),
                delete_ipc: r.raw_payload.clone(),
                delete_count: r.count,
                sequence_number: r.seq,
                published: r.published,
                created_at: r.created_at.clone(),
            })
            .collect();
        rows.sort_by_key(|r| r.sequence_number);
        Ok(rows)
    }
    async fn get_published_inlined_deletes(
        &self,
        table_id: &str,
    ) -> CatalogResult<Vec<InlinedDelete>> {
        Ok(self
            .get_inlined_deletes(table_id)
            .await?
            .into_iter()
            .filter(|r| r.published)
            .collect())
    }
    async fn get_unpublished_inlined_delete_ids(
        &self,
        table_id: &str,
    ) -> CatalogResult<Vec<String>> {
        Ok(self
            .rows(table_id)
            .inlined_deletes
            .iter()
            .filter(|(_, r)| !r.published)
            .map(|(id, _)| id.clone())
            .collect())
    }
    async fn table_storage_stats(&self, table_id: &str) -> CatalogResult<TableStorageStats> {
        let t = self.rows(table_id);
        let inline = self.get_inlined_data_stats(table_id).await?;
        let mut s = TableStorageStats {
            cold_files: count(t.cold.len())?,
            cold_bytes: sum(t.cold.values().map(|f| f.size))?,
            cold_rows: sum(t.cold.values().map(|f| f.rows))?,
            delete_files: count(t.delete_files.len())?,
            delete_file_bytes: sum(t.delete_files.values().map(|f| f.size))?,
            delete_file_tombstones: sum(t.delete_files.values().map(|f| f.count))?,
            snapshot_sequences: count(t.groups.len())?,
            file_statistics_rows: count(t.file_stats.len())?,
            insert_records: count(t.insert_records.len())?,
            index_run_rows: count(t.index_runs.len())?,
            inlined_entries: inline.entry_count,
            inlined_rows: inline.record_count,
            inlined_bytes: inline.ipc_bytes,
            inlined_delete_entries: inline.tombstone_entry_count,
            inlined_delete_rows: sum(t.inlined_deletes.values().map(|r| r.count))?,
            ..TableStorageStats::default()
        };
        for ((snapshot, _), f) in &t.files {
            if snapshot == &t.current_snapshot_id {
                s.current_files = add(s.current_files, 1)?;
                s.current_bytes = add(s.current_bytes, f.size)?;
                s.current_rows = add(s.current_rows, f.rows)?;
            } else if t.groups.contains_key(snapshot) {
                s.protected_files = add(s.protected_files, 1)?;
                s.protected_bytes = add(s.protected_bytes, f.size)?;
                s.protected_rows = add(s.protected_rows, f.rows)?;
            } else {
                s.unreachable_manifest_rows = add(s.unreachable_manifest_rows, 1)?;
            }
        }
        s.reachable_manifest_rows = add(s.current_files, s.protected_files)?;
        Ok(s)
    }
    async fn create_table(&self, _options: CreateTableOptions) -> CatalogResult<String> {
        Err(invalid("the shadow catalog is read-only"))
    }
    async fn update_table_schema(&self, _table_id: &str, _schema: &SchemaRef) -> CatalogResult<()> {
        Err(invalid("the shadow catalog is read-only"))
    }
    async fn update_table_schema_dropping_statistics(
        &self,
        _table_id: &str,
        _schema: &SchemaRef,
    ) -> CatalogResult<()> {
        Err(invalid("the shadow catalog is read-only"))
    }
    async fn set_current_snapshot(
        &self,
        _table_id: &str,
        _replaced_snapshot_id: &str,
        _new_snapshot_id: &str,
    ) -> CatalogResult<()> {
        Err(invalid("the shadow catalog is read-only"))
    }
    async fn increment_sequence_number(&self, _table_id: &str) -> CatalogResult<i64> {
        Err(invalid("the shadow catalog is read-only"))
    }
    async fn reserve_sequence_numbers(&self, _table_id: &str, _count: u32) -> CatalogResult<i64> {
        Err(invalid("the shadow catalog is read-only"))
    }
    async fn add_delete_file(&self, _delete_file: DeleteFile) -> CatalogResult<String> {
        Err(invalid("the shadow catalog is read-only"))
    }
    async fn add_delete_files(&self, _delete_files: Vec<DeleteFile>) -> CatalogResult<()> {
        Err(invalid("the shadow catalog is read-only"))
    }
    async fn commit_delete_files_with_inlined_rewrite(
        &self,
        _delete_files: Vec<DeleteFile>,
        _table_id: &str,
        _updated_data: Vec<InlinedData>,
        _deleted_inlined_ids: Vec<String>,
        _replaced_delete_file_ids: Vec<String>,
    ) -> CatalogResult<()> {
        Err(invalid("the shadow catalog is read-only"))
    }
    async fn remove_delete_files(
        &self,
        _table_id: &str,
        _delete_file_ids: &[String],
    ) -> CatalogResult<()> {
        Err(invalid("the shadow catalog is read-only"))
    }
    async fn clear_delete_files(&self, _table_id: &str) -> CatalogResult<()> {
        Err(invalid("the shadow catalog is read-only"))
    }
    async fn add_insert_record(
        &self,
        _table_id: &str,
        _pk_bytes: Vec<u8>,
        _sequence_number: i64,
    ) -> CatalogResult<()> {
        Err(invalid("the shadow catalog is read-only"))
    }
    async fn add_insert_records_batch(
        &self,
        _table_id: &str,
        _pk_bytes_list: Vec<Vec<u8>>,
        _sequence_number: i64,
    ) -> CatalogResult<()> {
        Err(invalid("the shadow catalog is read-only"))
    }
    async fn commit_on_conflict_deletions(
        &self,
        _delete_files: Vec<DeleteFile>,
        _table_id: &str,
        _insert_pk_bytes_list: Vec<Vec<u8>>,
        _insert_sequence: i64,
        _snapshot_sequence: Option<SnapshotSequenceCommit>,
    ) -> CatalogResult<()> {
        Err(invalid("the shadow catalog is read-only"))
    }
    async fn commit_on_conflict_deletions_with_tombstone(
        &self,
        _delete_files: Vec<DeleteFile>,
        _table_id: &str,
        _insert_pk_bytes_list: Vec<Vec<u8>>,
        _insert_sequence: i64,
        _snapshot_sequence: Option<SnapshotSequenceCommit>,
        _inline_tombstone: Option<InlinedDelete>,
        _pending_durable_flips: &[String],
    ) -> CatalogResult<Option<String>> {
        Err(invalid("the shadow catalog is read-only"))
    }
    async fn clear_insert_records(&self, _table_id: &str) -> CatalogResult<()> {
        Err(invalid("the shadow catalog is read-only"))
    }
    async fn set_snapshot_sequence(
        &self,
        _table_id: &str,
        _snapshot_id: &str,
        _sequence_number: i64,
    ) -> CatalogResult<()> {
        Err(invalid("the shadow catalog is read-only"))
    }
    async fn clear_snapshot_sequence(
        &self,
        _table_id: &str,
        _snapshot_id: &str,
    ) -> CatalogResult<()> {
        Err(invalid("the shadow catalog is read-only"))
    }
    async fn commit_compaction(
        &self,
        _table_id: &str,
        _replaced_snapshot_id: &str,
        _new_snapshot_id: &str,
    ) -> CatalogResult<()> {
        Err(invalid("the shadow catalog is read-only"))
    }
    async fn commit_compaction_fenced(
        &self,
        _table_id: &str,
        _replaced_snapshot_id: &str,
        _new_snapshot_id: &str,
        _cutoff: i64,
        _protected_snapshot_ids_to_clear: &[String],
    ) -> CatalogResult<()> {
        Err(invalid("the shadow catalog is read-only"))
    }
    async fn swap_protected_snapshots(
        &self,
        _table_id: &str,
        _old_snapshot_ids: &[String],
        _new_snapshot_id: &str,
        _new_sequence_number: i64,
    ) -> CatalogResult<bool> {
        Err(invalid("the shadow catalog is read-only"))
    }
    async fn commit_overwrite(
        &self,
        _table_id: &str,
        _new_snapshot_id: &str,
        _inlined: Option<&InlinedData>,
        _delete_files: &[crate::metadata::DeleteFile],
    ) -> CatalogResult<()> {
        Err(invalid("the shadow catalog is read-only"))
    }
    async fn add_partition(&self, _partition: PartitionMetadata) -> CatalogResult<String> {
        Err(invalid("the shadow catalog is read-only"))
    }
    async fn upsert_table_statistics(&self, _stats: &TableStatistics) -> CatalogResult<()> {
        Err(invalid("the shadow catalog is read-only"))
    }
    async fn clear_table_statistics(&self, _table_id: &str) -> CatalogResult<()> {
        Err(invalid("the shadow catalog is read-only"))
    }
    async fn upsert_snapshot_file_statistics(
        &self,
        _stats: &SnapshotFileStatistics,
    ) -> CatalogResult<()> {
        Err(invalid("the shadow catalog is read-only"))
    }
    async fn clear_snapshot_file_statistics_except(
        &self,
        _table_id: &str,
        _snapshot_id: &str,
    ) -> CatalogResult<()> {
        Err(invalid("the shadow catalog is read-only"))
    }
    async fn clear_snapshot_file_statistics(&self, _table_id: &str) -> CatalogResult<()> {
        Err(invalid("the shadow catalog is read-only"))
    }
    async fn upsert_snapshot_file(&self, _file: &SnapshotFile) -> CatalogResult<()> {
        Err(invalid("the shadow catalog is read-only"))
    }
    async fn replace_snapshot_files(
        &self,
        _table_id: &str,
        _snapshot_id: &str,
        _files: &[SnapshotFile],
    ) -> CatalogResult<()> {
        Err(invalid("the shadow catalog is read-only"))
    }
    async fn clear_snapshot_cached_metadata(
        &self,
        _table_id: &str,
        _snapshot_id: &str,
    ) -> CatalogResult<()> {
        Err(invalid("the shadow catalog is read-only"))
    }
    async fn clear_snapshot_files_except(
        &self,
        _table_id: &str,
        _snapshot_id: &str,
    ) -> CatalogResult<()> {
        Err(invalid("the shadow catalog is read-only"))
    }
    async fn clear_snapshot_files(&self, _table_id: &str) -> CatalogResult<()> {
        Err(invalid("the shadow catalog is read-only"))
    }
    async fn commit_overwrite_to_cold(
        &self,
        _table_id: &str,
        _new_snapshot_id: &str,
        _cold_files: &[ColdTierFile],
    ) -> CatalogResult<()> {
        Err(invalid("the shadow catalog is read-only"))
    }
    async fn upsert_pk_index(
        &self,
        _table_id: &str,
        _snapshot_id: &str,
        _index_blob: &[u8],
    ) -> CatalogResult<()> {
        Err(invalid("the shadow catalog is read-only"))
    }
    async fn clear_pk_index(&self, _table_id: &str) -> CatalogResult<()> {
        Err(invalid("the shadow catalog is read-only"))
    }
    async fn register_index_run(&self, _run: &IndexRunRecord) -> CatalogResult<()> {
        Err(invalid("the shadow catalog is read-only"))
    }
    async fn remove_index_run(
        &self,
        _table_id: &str,
        _index_key: &str,
        _run_name: &str,
    ) -> CatalogResult<()> {
        Err(invalid("the shadow catalog is read-only"))
    }
    async fn add_inlined_data(&self, _data: InlinedData) -> CatalogResult<String> {
        Err(invalid("the shadow catalog is read-only"))
    }
    async fn get_inlined_data_above_sequence(
        &self,
        table_id: &str,
        after_sequence: i64,
    ) -> CatalogResult<Vec<InlinedData>> {
        Ok(self
            .get_inlined_data(table_id)
            .await?
            .into_iter()
            .filter(|r| r.sequence_number > after_sequence)
            .collect())
    }
    async fn clear_inlined_data(&self, _table_id: &str) -> CatalogResult<()> {
        Err(invalid("the shadow catalog is read-only"))
    }
    async fn clear_inlined_data_and_deletes(&self, _table_id: &str) -> CatalogResult<()> {
        Err(invalid("the shadow catalog is read-only"))
    }
    async fn commit_on_conflict_deletions_and_clear_inlined(
        &self,
        _delete_files: Vec<DeleteFile>,
        _table_id: &str,
        _insert_pk_bytes_list: Vec<Vec<u8>>,
        _insert_sequence: i64,
        _snapshot_sequence: Option<SnapshotSequenceCommit>,
    ) -> CatalogResult<()> {
        Err(invalid("the shadow catalog is read-only"))
    }
    async fn add_inlined_delete(&self, _delete: InlinedDelete) -> CatalogResult<String> {
        Err(invalid("the shadow catalog is read-only"))
    }
    async fn mark_inlined_delete_published(
        &self,
        _table_id: &str,
        _inlined_id: &str,
    ) -> CatalogResult<()> {
        Err(invalid("the shadow catalog is read-only"))
    }
    async fn mark_inlined_delete_unpublished(
        &self,
        _table_id: &str,
        _inlined_id: &str,
    ) -> CatalogResult<()> {
        Err(invalid("the shadow catalog is read-only"))
    }
    async fn publish_orphan_inlined_deletes(&self, _table_id: &str) -> CatalogResult<u64> {
        Err(invalid("the shadow catalog is read-only"))
    }
    async fn commit_inlined_mutation(
        &self,
        _table_id: &str,
        _updated_data: Vec<InlinedData>,
        _deleted_inlined_ids: Vec<String>,
        _data: Vec<InlinedData>,
        _assigned_sequence: i64,
    ) -> CatalogResult<Option<i64>> {
        Err(invalid("the shadow catalog is read-only"))
    }
    async fn clear_inlined_deletes(&self, _table_id: &str) -> CatalogResult<()> {
        Err(invalid("the shadow catalog is read-only"))
    }
    async fn checkpoint_wal(&self) -> CatalogResult<()> {
        Err(invalid("the shadow catalog is read-only"))
    }
    async fn incremental_vacuum(&self) -> CatalogResult<u64> {
        Err(invalid("the shadow catalog is read-only"))
    }
    async fn drop_table(&self, _table_name: &str) -> CatalogResult<bool> {
        Err(invalid("the shadow catalog is read-only"))
    }
    async fn import_dataset_slice(
        &self,
        _slice: &crate::metastore::snapshot::DatasetMetastoreSlice,
        _data_dir_anchor: &std::path::Path,
    ) -> CatalogResult<()> {
        Err(invalid("the shadow catalog is read-only"))
    }
}

fn count(value: usize) -> CatalogResult<i64> {
    i64::try_from(value).map_err(invalid)
}
fn add(a: i64, b: i64) -> CatalogResult<i64> {
    a.checked_add(b)
        .ok_or_else(|| invalid("shadow aggregate overflow"))
}
fn sum(values: impl IntoIterator<Item = i64>) -> CatalogResult<i64> {
    values.into_iter().try_fold(0, add)
}
