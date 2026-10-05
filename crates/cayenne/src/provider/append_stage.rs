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

//! Whole-input key resolution in an unpublished append snapshot.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Instant;

use datafusion::execution::SendableRecordBatchStream;
use parking_lot::Mutex;

use super::Result;
use super::column_stats::ColumnStatsAccumulator;
use super::key_conflicts::{KeyResolver, Survivor};
use super::on_conflict::{OnConflictDeletions, PostValidationState};
use super::overwrite::{FileStatsObserver, WriteShape};
use super::overwrite_postpass::{self, ArrivalStream, DedupShare};
use super::pk_index::PkDigestSet;
use super::table::{CayenneTableProvider, record_cayenne_write_phase};

pub(super) enum ValidationScope {
    /// Emptiness was checked under the caller's held write lock.
    Empty,
    /// Validation may check out the shared key cache under the write lock.
    Locked,
    /// Validation uses a private keyset; the caller checks its token at commit.
    Optimistic,
}

pub(super) struct StagedAppend {
    pub new_snapshot_id: String,
    pub on_conflict_deletions: OnConflictDeletions,
    pub validated_keys: PkDigestSet,
    pub stats: Arc<ColumnStatsAccumulator>,
    pub row_count: u64,
    pub superseded: usize,
}

type ResolvedFiles = (u64, Arc<ColumnStatsAccumulator>, HashMap<String, Vec<u32>>);

impl CayenneTableProvider {
    pub(super) async fn stage_resolved_append(
        &self,
        data: SendableRecordBatchStream,
        resolver: KeyResolver,
        write: WriteShape,
        scope: ValidationScope,
    ) -> Result<StagedAppend> {
        let survivor = Survivor::for_policy(resolver.policy());
        let schema = self.table_schema();
        let indices = self.primary_key_indices()?.unwrap_or_default();
        let key_columns = overwrite_postpass::key_column_names(&schema, &indices);
        let arrival_name = overwrite_postpass::arrival_column(&schema);
        let _dedup_share = DedupShare::claim();
        let arrival = ArrivalStream::new(data, resolver, &arrival_name);
        let stamped_batches = arrival.stamped_batches();
        let data: SendableRecordBatchStream = Box::pin(arrival);
        let (data, post_validation) = match scope {
            ValidationScope::Empty => (data, Arc::new(Mutex::new(None))),
            ValidationScope::Locked | ValidationScope::Optimistic => {
                let prepared = match scope {
                    ValidationScope::Optimistic => {
                        self.prepare_stream_for_insert_offlock_resolving_repeats(data)
                            .await?
                    }
                    _ => {
                        self.prepare_stream_for_insert_resolving_repeats(data)
                            .await?
                    }
                };
                let post_validation = prepared.post_validation();
                (prepared.stream, post_validation)
            }
        };
        let new_snapshot_id = uuid::Uuid::now_v7().to_string();
        let file_stats = (!self.should_capture_positions())
            .then(|| Arc::new(FileStatsObserver::new(Arc::clone(&schema), None)));
        let start = Instant::now();
        let written: Result<ResolvedFiles> = async {
            let (rows, _, stats) = self
                .write_to_snapshot_with_schema(
                    data,
                    write.target_size_bytes,
                    &new_snapshot_id,
                    write.target_partitions,
                    None,
                    write.write_policy,
                    None,
                    file_stats
                        .as_ref()
                        .map(|observer| Arc::clone(observer) as _),
                    overwrite_postpass::with_arrival(&schema, &arrival_name),
                )
                .await?;
            self.sync_local_snapshot_dir(&new_snapshot_id).await?;
            if rows == 0 || stamped_batches.load(Ordering::Relaxed) <= 1 {
                return Ok((rows, stats, HashMap::new()));
            }
            let superseded = self
                .find_superseded_by_arrival(&new_snapshot_id, survivor, &key_columns, rows)
                .await?;
            match file_stats.as_deref() {
                Some(file_stats) if !superseded.is_empty() => {
                    let dropped: u64 = superseded.values().map(|rows| rows.len() as u64).sum();
                    let stats = self
                        .fold_superseded_copies(
                            &new_snapshot_id,
                            &superseded,
                            write,
                            file_stats,
                            &stats,
                        )
                        .await?;
                    Ok((rows.saturating_sub(dropped), stats, HashMap::new()))
                }
                _ => Ok((rows, stats, superseded)),
            }
        }
        .await;
        record_cayenne_write_phase(self.table_name(), "vortex_write", start);
        let (row_count, stats, hidden) = match written {
            Ok(written) => written,
            Err(error) => {
                drop(post_validation.lock().take());
                if !matches!(scope, ValidationScope::Optimistic) {
                    self.clear_cached_pk_keyset();
                }
                super::staged_upsert::cleanup_orphan_snapshot_dir(self, &new_snapshot_id).await;
                return Err(error);
            }
        };
        let PostValidationState {
            mut on_conflict_deletions,
            validated_keys,
        } = post_validation.lock().take().unwrap_or_default();
        let superseded = on_conflict_deletions
            .total_superseded()
            .saturating_add(hidden.values().map(Vec::len).sum());
        for (path, positions) in hidden {
            on_conflict_deletions
                .delete_specs
                .entry(Arc::from(path.as_str()))
                .or_default()
                .extend(positions.into_iter().map(u64::from));
        }
        Ok(StagedAppend {
            new_snapshot_id,
            on_conflict_deletions,
            validated_keys,
            stats,
            row_count,
            superseded,
        })
    }
}
