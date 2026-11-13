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

//! Immutable, storage-only view of object-native dataset logs.
//!
//! [`DatasetStateReader`] answers the object-native catalog's metadata reads
//! from a [`DatasetState`] replayed out of object storage, loading the
//! immutable blobs that state references (schemas, statistics, indexes, inline
//! payloads) from the store and checking each against its recorded digest. It
//! holds no database and writes nothing. The object-native catalog applies
//! committed records to its state. Data and deletion-vector files must remain
//! available at the paths the state records.

use crate::{
    catalog::{CatalogError, CatalogResult},
    metadata::{
        ColdTierFile, DeleteFile, DeletionType, IndexRunRecord, InlinedData, InlinedDataStats,
        InlinedDelete, PartitionMetadata, SnapshotFile, SnapshotFileStatistics, TableMetadata,
        TableStatistics, TableStorageStats,
    },
};
use cayenne_metastore_actions::{DatasetState, ObjRef, TableState};
use object_store::{ObjectStore, ObjectStoreExt, path::Path};
use std::{collections::HashMap, sync::Arc};

/// Metadata reads over one replayed [`DatasetState`].
/// Data files remain in the locations recorded by the writer.
pub(crate) struct DatasetStateReader {
    pub(crate) state: DatasetState,
    empty: TableState,
    pub(crate) store: Arc<dyn ObjectStore>,
    pub(crate) prefix: Path,
    /// The URL of the folder the catalog's `datasets/` folder is in: a data path
    /// under it is recorded relative to it, so a copied location keeps working.
    pub(crate) anchor: Option<Arc<str>>,
}

/// `path` as the state records it: relative to `anchor` when it is a dataset
/// folder's data under it (`datasets/<folder>/…`), as given otherwise.
pub(crate) fn stored_path(anchor: Option<&str>, path: &str) -> (String, bool) {
    anchor
        .and_then(|anchor| path.strip_prefix(anchor))
        .and_then(|rest| rest.strip_prefix('/'))
        .filter(|rest| rest.starts_with("datasets/"))
        .map_or_else(
            || (path.to_string(), false),
            |rest| (rest.to_string(), true),
        )
}

/// A path the state records, as its readers use it: a relative one resolved
/// against `anchor`. Without an anchor a relative path is returned as recorded.
pub(crate) fn resolved_path(anchor: Option<&str>, path: &str, relative: bool) -> (String, bool) {
    match anchor {
        Some(anchor) if relative => (format!("{anchor}/{path}"), false),
        _ => (path.to_string(), relative),
    }
}

fn invalid(error: impl std::fmt::Display) -> CatalogError {
    CatalogError::InvalidOperationNoSource {
        message: error.to_string(),
    }
}

impl DatasetStateReader {
    /// A reader over `state`, whose blobs are stored under `prefix` in `store`.
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
            anchor: None,
        }
    }

    /// This reader with relative data paths resolved against `anchor`.
    pub(crate) fn with_anchor(mut self, anchor: Option<Arc<str>>) -> Self {
        self.anchor = anchor;
        self
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
                "metastore blob {path} failed integrity validation"
            )));
        }
        Ok(bytes.to_vec())
    }
}

impl DatasetStateReader {
    pub(crate) fn list_pending_write_back(
        &self,
        table_id: &str,
        limit: usize,
        after: Option<&(i64, Vec<u8>)>,
    ) -> Vec<(Vec<u8>, i64)> {
        let mut rows: Vec<_> = self
            .rows(table_id)
            .pending_write_back
            .iter()
            .map(|(pk, seq)| (pk.clone(), *seq))
            .filter(|(pk, seq)| after.is_none_or(|(s, k)| (seq, pk) > (s, k)))
            .collect();
        rows.sort_by(|(a, x), (b, y)| (x, a).cmp(&(y, b)));
        rows.truncate(limit);
        rows
    }
    pub(crate) fn pending_write_back_count(&self, table_id: &str) -> CatalogResult<i64> {
        count(self.rows(table_id).pending_write_back.len())
    }
    pub(crate) fn list_table_names(&self) -> Vec<String> {
        let mut names: Vec<_> = self.state.tables.values().map(|t| t.name.clone()).collect();
        names.sort();
        names
    }
    pub(crate) async fn get_table(&self, table_name: &str) -> CatalogResult<TableMetadata> {
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
                    .ok_or_else(|| invalid("the stored table has no schema"))?,
            )
            .await?;
        let (path, path_is_relative) =
            resolved_path(self.anchor.as_deref(), &t.path, t.path_is_relative);
        Ok(TableMetadata {
            table_id: id.clone(),
            table_name: t.name.clone(),
            path,
            path_is_relative,
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
    pub(crate) fn get_sequence_number(&self, table_id: &str) -> CatalogResult<i64> {
        Ok(self.table(table_id)?.seq)
    }
    pub(crate) fn get_table_delete_files(&self, table_id: &str) -> Vec<DeleteFile> {
        self.rows(table_id)
            .delete_files
            .values()
            .map(|d| {
                let (path, path_is_relative) =
                    resolved_path(self.anchor.as_deref(), &d.path, d.relative);
                (d, path, path_is_relative)
            })
            .map(|(d, path, path_is_relative)| DeleteFile {
                delete_file_id: d.id.clone(),
                table_id: table_id.to_string(),
                source_data_file_path: d.source.clone(),
                path,
                path_is_relative,
                format: d.format.clone(),
                delete_count: d.count,
                file_size_bytes: d.size,
                deletion_type: DeletionType::default(),
                sequence_number: d.seq,
                reinsert_sequence: d.reinsert,
            })
            .collect()
    }
    pub(crate) fn get_orphan_eligible_delete_files(
        &self,
        table_id: &str,
        max_sequence: i64,
        limit: usize,
    ) -> Vec<DeleteFile> {
        self.get_table_delete_files(table_id)
            .into_iter()
            .filter(|d| d.source_data_file_path.is_none() && d.sequence_number <= max_sequence)
            .take(limit)
            .collect()
    }
    pub(crate) fn get_insert_records(&self, table_id: &str) -> HashMap<Box<[u8]>, i64> {
        self.rows(table_id)
            .insert_records
            .iter()
            .map(|(k, v)| (k.clone().into_boxed_slice(), *v))
            .collect()
    }
    pub(crate) fn get_snapshot_sequence(&self, table_id: &str, snapshot_id: &str) -> Option<i64> {
        self.rows(table_id).groups.get(snapshot_id).copied()
    }
    pub(crate) fn get_all_snapshot_sequences(&self, table_id: &str) -> HashMap<String, i64> {
        self.rows(table_id).groups.clone().into_iter().collect()
    }
    pub(crate) fn get_partitions(&self, table_id: &str) -> Vec<PartitionMetadata> {
        self.rows(table_id)
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
            .collect()
    }
    pub(crate) async fn get_table_statistics(
        &self,
        table_id: &str,
    ) -> CatalogResult<Option<TableStatistics>> {
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
    pub(crate) async fn get_snapshot_file_statistics(
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
    pub(crate) fn get_all_snapshot_files(&self, table_id: &str) -> Vec<SnapshotFile> {
        self.rows(table_id)
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
            .collect()
    }
    pub(crate) fn get_snapshot_files(
        &self,
        table_id: &str,
        snapshot_id: &str,
    ) -> Vec<SnapshotFile> {
        self.get_all_snapshot_files(table_id)
            .into_iter()
            .filter(|f| f.snapshot_id == snapshot_id)
            .collect()
    }
    pub(crate) async fn list_cold_tier_files(
        &self,
        table_id: &str,
    ) -> CatalogResult<Vec<ColdTierFile>> {
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
    pub(crate) async fn get_pk_index(
        &self,
        table_id: &str,
    ) -> CatalogResult<Option<(String, Vec<u8>)>> {
        match &self.rows(table_id).pk_index {
            Some((s, r)) => Ok(Some((s.clone(), self.blob(r).await?))),
            None => Ok(None),
        }
    }
    pub(crate) fn list_index_runs(&self, table_id: &str) -> Vec<IndexRunRecord> {
        self.rows(table_id)
            .index_runs
            .iter()
            .map(|((key, name), (rows, size))| IndexRunRecord {
                table_id: table_id.to_string(),
                index_key: key.clone(),
                run_name: name.clone(),
                row_count: *rows,
                size_bytes: *size,
            })
            .collect()
    }
    pub(crate) fn get_inlined_data(&self, table_id: &str) -> Vec<InlinedData> {
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
        rows
    }
    pub(crate) fn get_inlined_data_for_partition(
        &self,
        table_id: &str,
        partition_key: &str,
    ) -> Vec<InlinedData> {
        self.get_inlined_data(table_id)
            .into_iter()
            .filter(|r| r.partition_key.as_deref() == Some(partition_key))
            .collect()
    }
    pub(crate) fn get_inlined_data_count(&self, table_id: &str) -> CatalogResult<i64> {
        sum(self.rows(table_id).inlined_data.values().map(|r| r.count))
    }
    pub(crate) fn get_inlined_data_stats(&self, table_id: &str) -> CatalogResult<InlinedDataStats> {
        let t = self.rows(table_id);
        Ok(InlinedDataStats {
            record_count: self.get_inlined_data_count(table_id)?,
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
    pub(crate) fn get_inlined_deletes(&self, table_id: &str) -> Vec<InlinedDelete> {
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
        rows
    }
    pub(crate) fn get_published_inlined_deletes(&self, table_id: &str) -> Vec<InlinedDelete> {
        self.get_inlined_deletes(table_id)
            .into_iter()
            .filter(|r| r.published)
            .collect()
    }
    pub(crate) fn get_unpublished_inlined_delete_ids(&self, table_id: &str) -> Vec<String> {
        self.rows(table_id)
            .inlined_deletes
            .iter()
            .filter(|(_, r)| !r.published)
            .map(|(id, _)| id.clone())
            .collect()
    }
    pub(crate) fn table_storage_stats(&self, table_id: &str) -> CatalogResult<TableStorageStats> {
        let t = self.rows(table_id);
        let inline = self.get_inlined_data_stats(table_id)?;
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
    pub(crate) fn get_inlined_data_above_sequence(
        &self,
        table_id: &str,
        after_sequence: i64,
    ) -> Vec<InlinedData> {
        self.get_inlined_data(table_id)
            .into_iter()
            .filter(|r| r.sequence_number > after_sequence)
            .collect()
    }
}

fn count(value: usize) -> CatalogResult<i64> {
    i64::try_from(value).map_err(invalid)
}
fn add(a: i64, b: i64) -> CatalogResult<i64> {
    a.checked_add(b)
        .ok_or_else(|| invalid("table storage statistics overflowed"))
}
fn sum(values: impl IntoIterator<Item = i64>) -> CatalogResult<i64> {
    values.into_iter().try_fold(0, add)
}
