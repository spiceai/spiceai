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
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use arrow::datatypes::SchemaRef;
use datafusion::execution::SendableRecordBatchStream;
use parking_lot::Mutex;

use super::Result;
use super::column_stats::ColumnStatsAccumulator;
use super::key_conflicts::KeyResolver;
use super::on_conflict::{OnConflictDeletions, PostValidationState};
use super::overwrite::{FileStatsObserver, WriteShape};
use super::overwrite_postpass::{self, ArrivalStream, CopyOrder, DedupShare};
use super::pk_index::PkDigestSet;
use super::table::{CayenneTableProvider, record_cayenne_write_phase};

/// A write that resolves the keys it repeats across record batches after it is
/// written ([`super::overwrite_postpass`]): each batch resolves its own repeats
/// and is stamped with its arrival, and once the files are written, a query
/// over them finds every copy the policy does not keep.
pub(super) struct ResolveAfterWrite {
    order: CopyOrder,
    key_columns: Vec<String>,
    write_schema: SchemaRef,
    stamped_batches: Arc<AtomicU64>,
    _share: DedupShare,
}

impl ResolveAfterWrite {
    /// `data` stamped for resolution under `resolver`, and the resolution to
    /// run once it is written.
    pub(super) fn start(
        table: &CayenneTableProvider,
        data: SendableRecordBatchStream,
        resolver: KeyResolver,
    ) -> Result<(Self, SendableRecordBatchStream)> {
        let schema = table.table_schema();
        let indices = table.primary_key_indices()?.unwrap_or_default();
        let arrival_name = overwrite_postpass::arrival_column(&schema);
        let share = DedupShare::claim();
        let arrival = ArrivalStream::new(data, resolver, &arrival_name);
        let stamped_batches = arrival.stamped_batches();
        // A writer that supplies row times (a refresh with a `time_column`) orders
        // a key's copies by time, then arrival.
        let (order, write_schema, data): (_, _, SendableRecordBatchStream) =
            match &table.row_versions {
                Some(versions) => {
                    let version_name = overwrite_postpass::version_column(&schema);
                    (
                        CopyOrder::Version,
                        overwrite_postpass::with_versions(&schema, &arrival_name, &version_name),
                        Box::pin(arrival.with_versions(Arc::clone(versions), &version_name)),
                    )
                }
                None => (
                    CopyOrder::Arrival,
                    overwrite_postpass::with_arrival(&schema, &arrival_name),
                    Box::pin(arrival),
                ),
            };
        let resolution = Self {
            order,
            key_columns: overwrite_postpass::key_column_names(&schema, &indices),
            write_schema,
            stamped_batches,
            _share: share,
        };
        Ok((resolution, data))
    }

    /// The schema the stamped stream is written with.
    pub(super) fn write_schema(&self) -> SchemaRef {
        Arc::clone(&self.write_schema)
    }

    /// The file and position of every copy, among the `rows` written to
    /// `snapshot_id`, that the policy does not keep.
    pub(super) async fn superseded(
        &self,
        table: &CayenneTableProvider,
        snapshot_id: &str,
        rows: u64,
    ) -> Result<HashMap<String, Vec<u32>>> {
        // A write of at most one batch repeats no key once that batch resolved
        // its own repeats.
        if rows == 0 || self.stamped_batches.load(Ordering::Relaxed) <= 1 {
            return Ok(HashMap::new());
        }
        table
            .find_superseded_by_arrival(snapshot_id, self.order, &self.key_columns, rows)
            .await
    }
}

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
    /// Write `data` to the staging snapshot `staging_snapshot_id`, then fold the
    /// copies of the keys it repeats that `resolution` does not keep out of the
    /// staged files, returning the rows left.
    pub(super) async fn stage_resolving_repeats(
        &self,
        data: SendableRecordBatchStream,
        resolution: &ResolveAfterWrite,
        staging_snapshot_id: &str,
        target_partitions: usize,
    ) -> Result<u64> {
        let write = WriteShape {
            target_size_bytes: self.target_file_size_bytes(),
            target_partitions,
            write_policy: super::delta_encoding::WritePolicy::DELTA,
        };
        let file_stats = Arc::new(FileStatsObserver::new(
            self.table_name(),
            &self.table_schema(),
            None,
        )?);
        self.staging_may_have_files().store(true, Ordering::Release);
        let (rows, _, stats) = self
            .write_to_snapshot_with_schema(
                data,
                write.target_size_bytes,
                staging_snapshot_id,
                write.target_partitions,
                None,
                write.write_policy,
                None,
                Some(Arc::clone(&file_stats) as _),
                resolution.write_schema(),
            )
            .await?;
        let superseded = resolution
            .superseded(self, staging_snapshot_id, rows)
            .await?;
        if superseded.is_empty() {
            return Ok(rows);
        }
        let dropped: u64 = superseded.values().map(|rows| rows.len() as u64).sum();
        self.fold_superseded_copies(staging_snapshot_id, &superseded, write, &file_stats, &stats)
            .await?;
        Ok(rows.saturating_sub(dropped))
    }

    pub(super) async fn stage_resolved_append(
        &self,
        data: SendableRecordBatchStream,
        resolver: KeyResolver,
        write: WriteShape,
        scope: ValidationScope,
    ) -> Result<StagedAppend> {
        let schema = self.table_schema();
        let (resolution, data) = ResolveAfterWrite::start(self, data, resolver)?;
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
            .then(|| FileStatsObserver::new(self.table_name(), &schema, None).map(Arc::new))
            .transpose()?;
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
                    resolution.write_schema(),
                )
                .await?;
            self.sync_local_snapshot_dir(&new_snapshot_id).await?;
            let superseded = resolution.superseded(self, &new_snapshot_id, rows).await?;
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
