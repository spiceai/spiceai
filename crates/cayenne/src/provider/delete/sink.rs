/*
Copyright 2025-2026 The Spice.ai OSS Authors

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

//! Deletion sink for Cayenne tables.
//!
//! This module provides `CayenneDeletionSink`, which handles the process of marking
//! rows as deleted by writing deletion vectors to storage.
//!
//! # Deletion Strategies
//!
//! The sink supports three deletion strategies based on table configuration:
//!
//! - **Position-based**: For tables WITHOUT a primary key.
//!   Scans the table to find matching rows and records their file-local positions.
//!   Creates per-file deletion vectors that map to `RoaringBitmap` for efficient
//!   exclusion during Vortex scans.
//!
//! - **Int64 PK-based**: For tables with a single-column Int64 primary key.
//!   Optimized path that extracts PK values directly without `RowConverter` overhead.
//!   The deleted PKs (at the write's `delete_sequence`) are folded into the published
//!   [`super::super::deletion_index::DeletionIndex`] via `extend_max_deletes`.
//!
//! - **Key-based**: For tables with composite or non-integer primary keys.
//!   Uses Arrow's `RowConverter` to create deterministic byte keys, which are folded
//!   into the published [`super::super::deletion_index::KeyDeletionIndex`] via
//!   `extend_max_deletes`.
//!
//! # Workflow
//!
//! 1. Receive deletion request with filter expressions
//! 2. Scan table (and protected snapshots) to find matching rows
//! 3. Extract identifiers (positions or keys) based on deletion strategy
//! 4. Write deletion vectors to storage via `DeletionVectorWriter`
//! 5. Register delete files in catalog
//! 6. Update in-memory caches for immediate query consistency

use super::super::Error;
use super::super::deletion_strategy::{
    Int64PkDeletionSnapshot, PkDeletionStrategyWithCache, RowConverterDeletionSnapshot,
};
use super::super::memory_account::CayenneMemoryAccount;
use super::super::pk_validation::null_primary_key_message;
use super::super::utils::{bytes_key, convert_to_u64_box, i64_key};
use super::filter_exec::{InsertRecordHandling, is_pk_visible_i64, is_pk_visible_row_key};
use super::vector_io::DeletionVectorWriteResult;
use super::vector_io::{
    DeletionIdentifier, DeletionVectorWriteSpec, DeletionVectorWriter,
    cleanup_uncommitted_delete_paths,
};
use crate::catalog::MetadataCatalog;
use crate::metadata::{DeleteFile, TableMetadata};
use arc_swap::ArcSwap;
use arrow::array::{Array, ArrayRef};
use arrow_schema::SchemaRef;
use std::collections::HashMap;

use crate::row_converter::RowConverter;
use async_trait::async_trait;
use data_components::delete::DeletionSink;
use datafusion::datasource::listing::ListingTable;
use datafusion::execution::TaskContext;
use datafusion::execution::config::SessionConfig;
use datafusion::execution::context::SessionContext;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::optimizer::analyzer::type_coercion::TypeCoercionRewriter;
use datafusion::physical_plan::execute_stream;
use datafusion_catalog::TableProvider;
use datafusion_common::DFSchema;
use datafusion_common::tree_node::TreeNode;
use datafusion_expr::Expr;
use datafusion_expr::execution_props::ExecutionProps;
use datafusion_physical_expr::{PhysicalExpr, create_physical_expr};
use futures::StreamExt;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::Mutex as TokioMutex;

// Position-based deletion methods implemented in sink/position_based.rs
mod position_based;

// File-based deletion for time-based retention (deletes entire expired files)
pub(crate) mod file_based;

mod pk_filter_extract;
use pk_filter_extract::ExtractedPkDeletes;

const PK_DELETE_FLUSH_BATCH_SIZE: usize = 50_000;

enum StagedPkDelete {
    Int64 {
        delete_files: Vec<DeleteFile>,
        /// Tombstone snapshot captured when the delete began. Used only to count
        /// how many keys are newly deleted (not already tombstoned) — it is never
        /// re-published, so a concurrent update is never observed here.
        initial: Arc<Int64PkDeletionSnapshot>,
        /// This delete's primary keys, de-duplicated across chunks. Published by
        /// merging (compare-and-swap) onto the live snapshot at commit.
        new_pks: HashSet<i64>,
        /// The single delete sequence shared by every chunk of this delete.
        delete_sequence: Option<i64>,
    },
    RowKeys {
        delete_files: Vec<DeleteFile>,
        initial: Arc<RowConverterDeletionSnapshot>,
        new_keys: HashSet<Box<[u8]>>,
        delete_sequence: Option<i64>,
    },
}

impl Drop for StagedPkDelete {
    fn drop(&mut self) {
        let delete_files = match self {
            Self::Int64 { delete_files, .. } | Self::RowKeys { delete_files, .. } => {
                std::mem::take(delete_files)
            }
        };
        if delete_files.is_empty() {
            return;
        }
        let paths = delete_files
            .into_iter()
            .map(|file| std::path::PathBuf::from(file.path))
            .collect::<Vec<_>>();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                cleanup_uncommitted_delete_paths(&paths).await;
            });
        } else {
            cleanup_uncommitted_delete_paths_blocking(paths);
        }
    }
}

pub(crate) struct PreparedDeletionPublish {
    strategy: PkDeletionStrategyWithCache,
    table_memory: Arc<CayenneMemoryAccount>,
    delete_files: Vec<DeleteFile>,
    publish: PreparedDeletionCache,
    deleted_count: u64,
    cleanup_armed: bool,
}

enum PreparedDeletionCache {
    Int64 {
        pks: HashSet<i64>,
        sequence: Option<i64>,
    },
    RowKeys {
        keys: HashSet<Box<[u8]>>,
        sequence: Option<i64>,
    },
}

impl PreparedDeletionPublish {
    pub(crate) fn delete_files(&self) -> &[DeleteFile] {
        &self.delete_files
    }

    pub(crate) fn deleted_count(&self) -> u64 {
        self.deleted_count
    }

    /// Replace the computed count with the non-authoritative sentinel `0`. Used
    /// by the CDC `pk IN (...)` fast path: its extracted keys are an upper bound
    /// (not verified live rows), so returning a real count would require the
    /// table scan that path deliberately skips. The CDC caller discards the count.
    #[must_use]
    pub(crate) fn with_sentinel_count(mut self) -> Self {
        self.deleted_count = 0;
        self
    }

    fn cleanup_paths(&self) -> Vec<std::path::PathBuf> {
        self.delete_files
            .iter()
            .map(|file| std::path::PathBuf::from(&file.path))
            .collect()
    }

    pub(crate) fn publish(mut self) -> super::super::Result<()> {
        let publish = std::mem::replace(
            &mut self.publish,
            PreparedDeletionCache::Int64 {
                pks: HashSet::new(),
                sequence: None,
            },
        );
        match publish {
            PreparedDeletionCache::Int64 { pks, sequence } => {
                let snapshot =
                    self.strategy
                        .int64_pk_snapshot()
                        .ok_or_else(|| Error::Internal {
                            table: "unknown".to_string(),
                            message: "Atomic Int64 deletion used with incompatible strategy"
                                .to_string(),
                        })?;
                if let Some(sequence) = sequence {
                    snapshot.rcu(|current| {
                        Arc::new(Int64PkDeletionSnapshot::from_index(
                            current
                                .tombstones
                                .extend_max_deletes(pks.iter().map(|&pk| (pk, sequence))),
                        ))
                    });
                }
            }
            PreparedDeletionCache::RowKeys { keys, sequence } => {
                let snapshot =
                    self.strategy
                        .row_keys_snapshot()
                        .ok_or_else(|| Error::Internal {
                            table: "unknown".to_string(),
                            message: "Atomic key deletion used with incompatible strategy"
                                .to_string(),
                        })?;
                if let Some(sequence) = sequence {
                    snapshot.rcu(|current| {
                        Arc::new(RowConverterDeletionSnapshot::from_index(
                            current
                                .tombstones
                                .extend_max_deletes(keys.iter().map(|key| (key, sequence))),
                        ))
                    });
                }
            }
        }
        self.table_memory
            .set_deletion_bytes(self.strategy.approx_resident_bytes());
        Ok(())
    }

    pub(crate) fn mark_catalog_committed(&mut self) {
        self.cleanup_armed = false;
    }
}

impl Drop for PreparedDeletionPublish {
    fn drop(&mut self) {
        if !self.cleanup_armed {
            return;
        }
        let paths = self.cleanup_paths();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                cleanup_uncommitted_delete_paths(&paths).await;
            });
        } else {
            cleanup_uncommitted_delete_paths_blocking(paths);
        }
    }
}

fn cleanup_uncommitted_delete_paths_blocking(paths: Vec<std::path::PathBuf>) {
    std::thread::spawn(move || {
        for path in paths {
            match std::fs::remove_file(path) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => tracing::warn!(
                    %error,
                    "Failed to clean uncommitted deletion-vector file"
                ),
            }
        }
    });
}

/// One table a filtered delete scans, with the visibility rules its rows are read under.
///
/// Both are carried rather than derived from each other, because the three sources do
/// not line up two-by-two: the main listing applies re-inserts with no sequence cutoff
/// (`apply_deletion_filter_with_insert_records`), a protected snapshot ignores them and
/// has a cutoff (`apply_partial_deletion_filter`), and the COLD tier ignores them with
/// NO cutoff (`apply_deletion_filter`). Deriving the mode from "is there a cutoff" gets
/// cold wrong, and cold is the case where it costs a row: its files hold fully
/// superseded data, so treating a re-inserted key as live there matches the stale value
/// and tombstones the key — deleting the replacement that never matched the predicate.
#[derive(Clone)]
pub(crate) struct DeleteScanSource {
    /// Only deletions NEWER than this apply to these rows. `None` — every deletion
    /// applies — for the main listing and the cold tier.
    pub(crate) min_delete_seq: Option<i64>,
    /// Whether a key re-inserted after its delete reads as live again.
    pub(crate) insert_records: InsertRecordHandling,
    pub(crate) table: Arc<ListingTable>,
}

impl StagedPkDelete {
    fn new(strategy: &PkDeletionStrategyWithCache, table_name: &str) -> super::super::Result<Self> {
        match strategy {
            PkDeletionStrategyWithCache::Int64Pk {
                deletion_snapshot, ..
            } => Ok(Self::Int64 {
                delete_files: Vec::new(),
                initial: deletion_snapshot.load_full(),
                new_pks: HashSet::new(),
                delete_sequence: None,
            }),
            PkDeletionStrategyWithCache::RowConverterBased {
                deletion_snapshot, ..
            } => Ok(Self::RowKeys {
                delete_files: Vec::new(),
                initial: deletion_snapshot.load_full(),
                new_keys: HashSet::new(),
                delete_sequence: None,
            }),
            PkDeletionStrategyWithCache::PositionBased { .. } => Err(Error::Internal {
                table: table_name.to_string(),
                message: "Primary-key delete staging used with position-based strategy".to_string(),
            }),
        }
    }

    fn absorb(
        &mut self,
        results: Vec<DeletionVectorWriteResult>,
        delete_sequence: i64,
        table_name: &str,
    ) -> super::super::Result<()> {
        match self {
            Self::Int64 {
                delete_files,
                new_pks,
                delete_sequence: staged_sequence,
                ..
            } => {
                // Every chunk shares the one reserved delete sequence.
                *staged_sequence = Some(delete_sequence);
                for result in results {
                    delete_files.push(result.delete_file);
                    match result.identifiers {
                        DeletionIdentifier::KeyBased(keys) => {
                            for key in keys {
                                let bytes: [u8; 8] =
                                    key.as_ref().try_into().map_err(|_| Error::Internal {
                                        table: table_name.to_string(),
                                        message:
                                            "Int64 deletion key did not contain exactly 8 bytes"
                                                .to_string(),
                                    })?;
                                new_pks.insert(i64::from_be_bytes(bytes));
                            }
                        }
                        DeletionIdentifier::PositionBased { .. } => {
                            return Err(Error::Internal {
                                table: table_name.to_string(),
                                message: "Unexpected position identifiers in atomic Int64 delete"
                                    .to_string(),
                            });
                        }
                    }
                }
            }
            Self::RowKeys {
                delete_files,
                new_keys,
                delete_sequence: staged_sequence,
                ..
            } => {
                *staged_sequence = Some(delete_sequence);
                for result in results {
                    delete_files.push(result.delete_file);
                    match result.identifiers {
                        DeletionIdentifier::KeyBased(result_keys) => new_keys.extend(result_keys),
                        DeletionIdentifier::PositionBased { .. } => {
                            return Err(Error::Internal {
                                table: table_name.to_string(),
                                message: "Unexpected position identifiers in atomic key delete"
                                    .to_string(),
                            });
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Number of keys this delete newly tombstones — keys not already deleted
    /// when the delete began. Re-deletions of already-tombstoned keys are not
    /// counted, preserving the pre-refactor "rows affected" semantics.
    fn new_count(&self) -> usize {
        match self {
            Self::Int64 {
                initial, new_pks, ..
            } => new_pks
                .iter()
                .filter(|pk| initial.tombstones.get(**pk).is_none())
                .count(),
            Self::RowKeys {
                initial, new_keys, ..
            } => new_keys
                .iter()
                .filter(|key| initial.tombstones.get(key.as_ref()).is_none())
                .count(),
        }
    }
}

/// Deletion sink for Cayenne tables.
///
/// This sink handles the process of marking rows as deleted by writing
/// deletion vectors to storage. Supports three deletion strategies:
/// - Position-based deletion (for tables without primary key)
/// - Int64 PK deletion (for tables with single-column Int64 primary key)
/// - Key-based deletion (for tables with composite/non-integer primary key)
#[derive(Clone)]
pub struct CayenneDeletionSink {
    table_metadata: TableMetadata,
    catalog: Arc<dyn MetadataCatalog>,
    listing_table: Arc<ArcSwap<ListingTable>>,
    schema: SchemaRef,
    filters: Vec<Expr>,
    /// Deletion strategy for this table, with embedded caches.
    pk_deletion_strategy: PkDeletionStrategyWithCache,
    /// Shared table memory account updated when this sink publishes deletion cache snapshots.
    table_memory: Arc<CayenneMemoryAccount>,
    /// `RowConverter` for converting primary key columns to byte representation.
    /// Only set for tables with composite or non-integer primary keys.
    pk_row_converter: Option<Arc<RowConverter>>,
    /// Indices of primary key columns in the table schema.
    pk_column_indices: Vec<usize>,
    /// Extra listing tables to also scan for deletion keys, beyond the main listing
    /// table — the protected snapshots and (for cold-tier tables) the cold-tier files —
    /// each paired with the delete sequence its rows are visible above.
    ///
    /// A protected snapshot carries `max_delete_seq_at_creation`: only deletes NEWER than
    /// that apply to its rows, which is precisely what tells a superseded version from
    /// the row that replaced it. Without it, a key-based delete can match a version an
    /// upsert already retired and tombstone the KEY, taking the live row with it. `None`
    /// is the base case — the current snapshot and cold-tier files, where every delete
    /// applies.
    additional_scan_tables: Vec<DeleteScanSource>,
    /// How the main listing table treats an upsert's re-insert marker. Carried from the
    /// caller rather than fixed here, because it is conditional in exactly the way
    /// `scan` makes it conditional: with no protected snapshot, main holds the only copy
    /// of a key and a re-insert marker means the row is live (`Apply`); with a protected
    /// snapshot present, the replacement lives THERE and main holds only the superseded
    /// version, which the marker must not resurrect (`Ignore`). Assuming `Apply` lets a
    /// predicate matching only the retired value tombstone the KEY and take the
    /// replacement — which never matched the predicate — with it.
    main_insert_records: InsertRecordHandling,
    /// The table's live protected-snapshot map, re-read under the execution-time
    /// `write_lock` to catch the one transition `main_insert_records` cannot be captured
    /// across: the plan is built before that lock is taken, so an ordinary upsert can
    /// publish a protected snapshot in between and turn a captured `Apply` into the
    /// resurrection case. Only 0 -> non-empty is unsafe; a capture that already saw one
    /// is `Ignore` and stays correct however many more appear.
    protected_snapshots: Arc<ArcSwap<HashMap<String, i64>>>,
    /// Shared `RuntimeEnv` for S3 object store access.
    runtime_env: Arc<RuntimeEnv>,
    /// Shared write lock to prevent concurrent writes/refreshes from racing with deletions.
    /// `None` when the caller already holds the write lock (e.g. retention filters applied
    /// during `write_all_append`).
    write_lock: Option<Arc<TokioMutex<()>>>,
    /// Shared in-memory sequence allocator (lever B2) of the owning
    /// `CayenneTableProvider`. The DML `DELETE` sink routes its sequence
    /// allocations through the SAME allocator as every other writer of this
    /// table, so memory and the DB `current_sequence_number` never diverge.
    seq_allocator: Arc<TokioMutex<super::super::table::SeqAllocator>>,
    /// Whether this sink must return a VERIFIED deleted-row count — i.e. it backs
    /// a user-visible `DELETE`, where the count is surfaced to the SQL client as
    /// "rows affected". When false (the CDC/internal default), the `pk IN (...)`
    /// fast path may persist deletions WITHOUT a scan and return 0. That 0 is a
    /// deliberate non-authoritative placeholder, not an upper bound — the filter's
    /// extracted PK key set is the upper bound on deletions, and computing the
    /// exact figure would need the scan this path skips (see
    /// [`Self::delete_filtered_rows_from_tables`]). When true, the fast path is
    /// bypassed so the scan-based path returns an exact count of the live rows
    /// actually removed.
    count_exact: bool,
}

impl CayenneDeletionSink {
    /// Create a new deletion sink.
    #[expect(clippy::too_many_arguments)]
    pub(crate) fn new(
        table_metadata: TableMetadata,
        catalog: Arc<dyn MetadataCatalog>,
        listing_table: Arc<ArcSwap<ListingTable>>,
        schema: SchemaRef,
        filters: &[Expr],
        pk_deletion_strategy: PkDeletionStrategyWithCache,
        table_memory: Arc<CayenneMemoryAccount>,
        pk_row_converter: Option<Arc<RowConverter>>,
        pk_column_indices: Vec<usize>,
        additional_scan_tables: Vec<DeleteScanSource>,
        main_insert_records: InsertRecordHandling,
        protected_snapshots: Arc<ArcSwap<HashMap<String, i64>>>,
        runtime_env: Arc<RuntimeEnv>,
        write_lock: Option<Arc<TokioMutex<()>>>,
        seq_allocator: Arc<TokioMutex<super::super::table::SeqAllocator>>,
    ) -> Self {
        Self {
            table_metadata,
            catalog,
            listing_table,
            schema,
            filters: filters.to_vec(),
            pk_deletion_strategy,
            table_memory,
            pk_row_converter,
            pk_column_indices,
            additional_scan_tables,
            main_insert_records,
            protected_snapshots,
            runtime_env,
            write_lock,
            seq_allocator,
            count_exact: false,
        }
    }

    /// Set whether this sink must return an exact, verified deleted-row count.
    ///
    /// `true` (a user-visible `DELETE`, where "rows affected" is shown to the
    /// client) bypasses the count-skipping `pk IN (...)` fast path so the
    /// scan-based path counts only the live rows actually removed. `false` (the
    /// default) keeps the fast path for CDC/internal callers that do not surface
    /// the count. See [`Self::count_exact`].
    pub(crate) fn with_exact_count(mut self, exact: bool) -> Self {
        self.count_exact = exact;
        self
    }

    fn refresh_deletion_memory_accounting(&self) {
        self.table_memory
            .set_deletion_bytes(self.pk_deletion_strategy.approx_resident_bytes());
    }

    fn assigned_delete_sequence(
        sequence: Option<i64>,
        table_name: &str,
    ) -> super::super::Result<i64> {
        sequence.ok_or_else(|| Error::Internal {
            table: table_name.to_string(),
            message: "Deletion-vector write completed without assigning a delete sequence"
                .to_string(),
        })
    }

    async fn prepare_delete_all_rows_from_tables(
        &self,
        ctx: &SessionContext,
        tables: &[Arc<ListingTable>],
    ) -> super::super::Result<Option<PreparedDeletionPublish>> {
        let table_name = &self.table_metadata.table_name;
        // For position-based deletions, we need per-file row tracking
        // For PK-based deletions, we can still batch across all files
        match &self.pk_deletion_strategy {
            PkDeletionStrategyWithCache::Int64Pk { .. } => {
                let mut pending_pk_values = HashSet::with_capacity(PK_DELETE_FLUSH_BATCH_SIZE);
                let mut delete_sequence = None;
                let mut staged = StagedPkDelete::new(&self.pk_deletion_strategy, table_name)?;
                for table in tables {
                    let scan_plan = table
                        .scan(&ctx.state(), Some(&self.pk_column_indices), &[], None)
                        .await?;
                    let mut stream = execute_stream(scan_plan, ctx.task_ctx())?;
                    while let Some(batch) = stream.next().await {
                        let batch = batch?;
                        let projected_sink = Self {
                            pk_column_indices: vec![0],
                            ..self.clone()
                        };
                        pending_pk_values.extend(projected_sink.extract_int64_pk_values(&batch)?);
                        if pending_pk_values.len() >= PK_DELETE_FLUSH_BATCH_SIZE {
                            let row_keys = pending_pk_values.drain().map(i64_key).collect();
                            let results = self
                                .write_key_based_chunk_with_shared_sequence(
                                    row_keys,
                                    &mut delete_sequence,
                                )
                                .await?;
                            staged.absorb(
                                results,
                                Self::assigned_delete_sequence(delete_sequence, table_name)?,
                                table_name,
                            )?;
                        }
                    }
                }
                if !pending_pk_values.is_empty() {
                    let row_keys = pending_pk_values.into_iter().map(i64_key).collect();
                    let results = self
                        .write_key_based_chunk_with_shared_sequence(row_keys, &mut delete_sequence)
                        .await?;
                    staged.absorb(
                        results,
                        Self::assigned_delete_sequence(delete_sequence, table_name)?,
                        table_name,
                    )?;
                }
                if delete_sequence.is_none() {
                    return Ok(None);
                }
                self.prepare_staged_pk_deletions(staged).map(Some)
            }
            PkDeletionStrategyWithCache::RowConverterBased { .. } => {
                let Some(ref row_converter) = self.pk_row_converter else {
                    return Err(Error::Internal {
                        table: table_name.clone(),
                        message: "RowConverter not available for RowConverterBased strategy"
                            .to_string(),
                    });
                };
                let mut pending_row_keys = HashSet::with_capacity(PK_DELETE_FLUSH_BATCH_SIZE);
                let mut delete_sequence = None;
                let mut staged = StagedPkDelete::new(&self.pk_deletion_strategy, table_name)?;
                for table in tables {
                    let scan_plan = table
                        .scan(&ctx.state(), Some(&self.pk_column_indices), &[], None)
                        .await?;
                    let mut stream = execute_stream(scan_plan, ctx.task_ctx())?;
                    let projected_indices: Vec<usize> = (0..self.pk_column_indices.len()).collect();
                    while let Some(batch) = stream.next().await {
                        let batch = batch?;
                        let pk_columns: Vec<ArrayRef> = projected_indices
                            .iter()
                            .map(|&index| Arc::clone(batch.column(index)))
                            .collect();
                        if pk_columns.iter().any(|column| column.null_count() > 0) {
                            return Err(Error::DataValidation {
                                table: table_name.clone(),
                                message: null_primary_key_message(&batch, &projected_indices),
                            });
                        }
                        let rows = row_converter.convert_columns(&pk_columns)?;
                        pending_row_keys.extend(rows.iter().map(|row| bytes_key(row.as_ref())));
                        if pending_row_keys.len() >= PK_DELETE_FLUSH_BATCH_SIZE {
                            let results = self
                                .write_key_based_chunk_with_shared_sequence(
                                    pending_row_keys.drain().collect(),
                                    &mut delete_sequence,
                                )
                                .await?;
                            staged.absorb(
                                results,
                                Self::assigned_delete_sequence(delete_sequence, table_name)?,
                                table_name,
                            )?;
                        }
                    }
                }
                if !pending_row_keys.is_empty() {
                    let results = self
                        .write_key_based_chunk_with_shared_sequence(
                            pending_row_keys.into_iter().collect(),
                            &mut delete_sequence,
                        )
                        .await?;
                    staged.absorb(
                        results,
                        Self::assigned_delete_sequence(delete_sequence, table_name)?,
                        table_name,
                    )?;
                }
                if delete_sequence.is_none() {
                    return Ok(None);
                }
                self.prepare_staged_pk_deletions(staged).map(Some)
            }
            PkDeletionStrategyWithCache::PositionBased { .. } => {
                // Position-based deletion for "delete all" (delete w/o filters)
                // Note: Delete all is NOT available using retention so this is unreachable
                Err(Error::Internal {
                    table: table_name.clone(),
                    message: "Position-based deletion without primary key is not yet supported for delete-all operations".to_string(),
                })
            }
        }
    }

    // NOTE: delete_filtered_rows_streaming_position_based is implemented in sink/position_based.rs

    /// Extract Int64 primary key values from a batch.
    fn extract_int64_pk_values(
        &self,
        batch: &arrow::array::RecordBatch,
    ) -> super::super::Result<Vec<i64>> {
        use arrow::array::Int64Array;

        let table_name = &self.table_metadata.table_name;

        // For Int64 PK strategy, we only have one PK column
        let pk_column_index = self
            .pk_column_indices
            .first()
            .ok_or_else(|| Error::Internal {
                table: table_name.clone(),
                message: "Int64 PK strategy requires exactly one PK column index".to_string(),
            })?;

        let pk_column = batch.column(*pk_column_index);
        let pk_array = pk_column
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| Error::Internal {
                table: table_name.clone(),
                message: format!(
                    "Expected Int64Array for PK column at index {pk_column_index}, got {:?}",
                    pk_column.data_type()
                ),
            })?;

        if pk_array.null_count() > 0 {
            return Err(Error::DataValidation {
                table: table_name.clone(),
                message: null_primary_key_message(batch, std::slice::from_ref(pk_column_index)),
            });
        }

        let pk_values: Vec<i64> = pk_array.values().iter().copied().collect();
        Ok(pk_values)
    }

    /// How the main listing table must treat an upsert's re-insert marker, re-checked
    /// against the live protected-snapshot map.
    ///
    /// `main_insert_records` is decided while the DELETE plan is built, which is before
    /// the execution-time `write_lock` is held, so an ordinary upsert can publish a
    /// protected snapshot in the gap. Scanning main with `Apply` once one exists is the
    /// resurrection case: main then holds only the superseded version, a predicate
    /// matching its retired value tombstones the KEY, and that tombstone hides the
    /// replacement that never matched.
    ///
    /// Downgrading to `Ignore` leaves a residual, and it is the safe direction to be
    /// wrong in. The snapshot published after the capture is not in
    /// `additional_scan_tables` either, so a key whose superseded version matched is
    /// left undeleted rather than destroyed. The resulting STATE is the one the serial
    /// order "this DELETE, then that upsert" produces — the key survives at the
    /// replacement value, which is where the upsert put it — so no row is lost or
    /// resurrected. What diverges is the `rows affected` handed back to the client: it
    /// under-reports those keys, and a user `DELETE` has no later pass to correct that
    /// (retention, which re-runs, does). The next `DELETE` captures the snapshot and
    /// sees them.
    ///
    /// Rebuilding the scan sources here against the live map would narrow that window
    /// but not close it: `write_lock` is not the boundary that orders protected-snapshot
    /// publication. A mem-tier checkpoint drops `write_lock` right after its capture and
    /// publishes under `listing_fence.write()` alone (see `RewriteScope`), so a snapshot
    /// can still appear while this DELETE holds `write_lock`, and mid-scan.
    fn live_main_insert_records(&self) -> InsertRecordHandling {
        if self.main_insert_records == InsertRecordHandling::Apply
            && !self.protected_snapshots.load().is_empty()
        {
            // Counted, because the trade is only sound while it stays rare: a rate that
            // climbs with ingest load means user DELETEs routinely under-report the rows
            // they affected, which is the point at which rebuilding the scan sources
            // against the live map — and paying a fence for the residual race above —
            // buys something. Without the counter that is unanswerable.
            telemetry::cayenne::track_delete_main_visibility_downgrade(&[
                telemetry::KeyValue::new("table", self.table_metadata.table_name.clone()),
            ]);
            return InsertRecordHandling::Ignore;
        }
        self.main_insert_records
    }

    /// Whether an Int64-keyed row from a snapshot whose deletions are visible above
    /// `min_delete_seq` is still live. `None` is the base case — the current snapshot and
    /// cold tier, where every delete applies.
    ///
    /// Delegates to the read path's own predicate rather than probing the index directly:
    /// a bare `get_with_min_seq(..).is_none()` is only half the rule, and misses that a
    /// tombstoned key re-inserted after its delete is visible again.
    fn is_live_int64_pk(&self, pk: i64, source: &DeleteScanSource) -> bool {
        match &self.pk_deletion_strategy {
            PkDeletionStrategyWithCache::Int64Pk {
                deletion_snapshot, ..
            } => is_pk_visible_i64(
                pk,
                &deletion_snapshot.load().tombstones,
                source.insert_records,
                source.min_delete_seq,
            ),
            _ => true,
        }
    }

    /// [`Self::is_live_int64_pk`] for composite / non-integer primary keys.
    fn is_live_row_key(&self, key: &[u8], source: &DeleteScanSource) -> bool {
        match &self.pk_deletion_strategy {
            PkDeletionStrategyWithCache::RowConverterBased {
                deletion_snapshot, ..
            } => is_pk_visible_row_key(
                key,
                &deletion_snapshot.load().tombstones,
                source.insert_records,
                source.min_delete_seq,
            ),
            _ => true,
        }
    }

    /// Extract row keys from a batch using the `RowConverter`.
    fn extract_row_keys(
        &self,
        batch: &arrow::array::RecordBatch,
        row_converter: &RowConverter,
    ) -> super::super::Result<Vec<Box<[u8]>>> {
        let pk_columns: Vec<ArrayRef> = self
            .pk_column_indices
            .iter()
            .map(|&idx| Arc::clone(batch.column(idx)))
            .collect();

        let rows = row_converter.convert_columns(&pk_columns)?;

        let row_keys: Vec<Box<[u8]>> = rows.iter().map(|row| bytes_key(row.as_ref())).collect();

        Ok(row_keys)
    }

    async fn prepare_delete_filtered_rows_from_tables(
        &self,
        ctx: &SessionContext,
        tables: &[DeleteScanSource],
    ) -> super::super::Result<Option<PreparedDeletionPublish>> {
        let table_name = &self.table_metadata.table_name;

        // For position-based deletion, use the streaming per-file approach directly.
        // This avoids loading all data into memory and provides correct file-local row IDs.
        if self.pk_deletion_strategy.is_position_based() {
            return Err(Error::Internal {
                table: table_name.clone(),
                message: "Staged key delete cannot use a position-based strategy".to_string(),
            });
        }

        let coerced_filters = self.coerce_filters_for_schema()?;

        // PK-IN-list fast path: when the filter encodes the PK deletion values
        // directly (`pk IN (...)`), extract them and write deletion vectors
        // WITHOUT a full table scan.
        //
        // This path cannot produce a verified count: the extracted key set is
        // the filter's UPPER BOUND on deletions (some keys may not exist), so a
        // meaningful "rows deleted" number would require the scan it is avoiding.
        // It therefore returns 0. CDC/internal callers don't surface the count
        // and take this fast path.
        //
        // A user-visible `DELETE` (`count_exact`) surfaces "rows affected" to the
        // client, so it must NOT take the fast path: it falls through to the
        // scan-based path below, which extracts PKs from the LIVE rows the scan
        // matched and thus counts only rows actually removed.
        if !self.count_exact {
            match self.try_extract_pks_from_filters(&coerced_filters) {
                Some(ExtractedPkDeletes::Int64(pk_values)) => {
                    tracing::debug!(
                        table = %table_name,
                        count = pk_values.len(),
                        "Fast-path delete: extracted Int64 PK values directly from filters, skipping table scan"
                    );
                    if pk_values.is_empty() {
                        return Ok(None);
                    }
                    let mut delete_sequence = None;
                    let mut staged = StagedPkDelete::new(&self.pk_deletion_strategy, table_name)?;
                    let row_keys = pk_values.into_iter().map(i64_key).collect();
                    let results = self
                        .write_key_based_chunk_with_shared_sequence(row_keys, &mut delete_sequence)
                        .await?;
                    staged.absorb(
                        results,
                        Self::assigned_delete_sequence(delete_sequence, table_name)?,
                        table_name,
                    )?;
                    return self
                        .prepare_staged_pk_deletions(staged)
                        .map(|prepared| Some(prepared.with_sentinel_count()));
                }
                Some(ExtractedPkDeletes::RowKeys(row_keys)) => {
                    tracing::debug!(
                        table = %table_name,
                        count = row_keys.len(),
                        "Fast-path delete: extracted row keys directly from filters, skipping table scan"
                    );
                    if row_keys.is_empty() {
                        return Ok(None);
                    }
                    let mut delete_sequence = None;
                    let mut staged = StagedPkDelete::new(&self.pk_deletion_strategy, table_name)?;
                    let results = self
                        .write_key_based_chunk_with_shared_sequence(row_keys, &mut delete_sequence)
                        .await?;
                    staged.absorb(
                        results,
                        Self::assigned_delete_sequence(delete_sequence, table_name)?,
                        table_name,
                    )?;
                    return self
                        .prepare_staged_pk_deletions(staged)
                        .map(|prepared| Some(prepared.with_sentinel_count()));
                }
                None => {}
            }
        }

        let physical_filters = self.build_physical_filters(&coerced_filters)?;

        match &self.pk_deletion_strategy {
            PkDeletionStrategyWithCache::Int64Pk { .. } => {
                let mut pending_pk_values: Vec<i64> =
                    Vec::with_capacity(PK_DELETE_FLUSH_BATCH_SIZE);
                let mut delete_sequence: Option<i64> = None;
                let mut staged = StagedPkDelete::new(&self.pk_deletion_strategy, table_name)?;

                for source in tables {
                    let scan_plan = source.table.scan(&ctx.state(), None, &[], None).await?;
                    let mut stream = execute_stream(scan_plan, ctx.task_ctx())?;

                    while let Some(batch_result) = stream.next().await {
                        let batch =
                            self.apply_physical_filters_to_batch(batch_result?, &physical_filters)?;
                        if batch.num_rows() == 0 {
                            continue;
                        }

                        // Drop rows this snapshot's own visibility already retires. A
                        // tombstone newer than the snapshot's threshold means an upsert
                        // superseded this version; tombstoning its KEY would take the row
                        // that replaced it — which never matched the predicate — with it.
                        // One bloom-prefiltered probe per row: no second scan, and nothing
                        // held that the raw scan did not already hold.
                        pending_pk_values.extend(
                            self.extract_int64_pk_values(&batch)?
                                .into_iter()
                                .filter(|pk| self.is_live_int64_pk(*pk, source)),
                        );

                        if pending_pk_values.len() >= PK_DELETE_FLUSH_BATCH_SIZE {
                            let chunk_values = std::mem::take(&mut pending_pk_values);
                            let row_keys = chunk_values.into_iter().map(i64_key).collect();
                            let results = self
                                .write_key_based_chunk_with_shared_sequence(
                                    row_keys,
                                    &mut delete_sequence,
                                )
                                .await?;
                            staged.absorb(
                                results,
                                Self::assigned_delete_sequence(delete_sequence, table_name)?,
                                table_name,
                            )?;
                        }
                    }
                }

                if !pending_pk_values.is_empty() {
                    let chunk_values = std::mem::take(&mut pending_pk_values);
                    let row_keys = chunk_values.into_iter().map(i64_key).collect();
                    let results = self
                        .write_key_based_chunk_with_shared_sequence(row_keys, &mut delete_sequence)
                        .await?;
                    staged.absorb(
                        results,
                        Self::assigned_delete_sequence(delete_sequence, table_name)?,
                        table_name,
                    )?;
                }
                if delete_sequence.is_none() {
                    return Ok(None);
                }
                self.prepare_staged_pk_deletions(staged).map(Some)
            }
            PkDeletionStrategyWithCache::RowConverterBased { .. } => {
                let Some(row_converter) = self.pk_row_converter.as_ref() else {
                    return Err(Error::Internal {
                        table: table_name.clone(),
                        message: "RowConverter not available for RowConverterBased strategy"
                            .to_string(),
                    });
                };

                let mut pending_row_keys: Vec<Box<[u8]>> =
                    Vec::with_capacity(PK_DELETE_FLUSH_BATCH_SIZE);
                let mut delete_sequence: Option<i64> = None;
                let mut staged = StagedPkDelete::new(&self.pk_deletion_strategy, table_name)?;

                for source in tables {
                    let scan_plan = source.table.scan(&ctx.state(), None, &[], None).await?;
                    let mut stream = execute_stream(scan_plan, ctx.task_ctx())?;

                    while let Some(batch_result) = stream.next().await {
                        let batch =
                            self.apply_physical_filters_to_batch(batch_result?, &physical_filters)?;
                        if batch.num_rows() == 0 {
                            continue;
                        }

                        // See the Int64 branch: this snapshot's threshold is what tells
                        // a superseded version from the row that replaced it.
                        pending_row_keys.extend(
                            self.extract_row_keys(&batch, row_converter)?
                                .into_iter()
                                .filter(|key| self.is_live_row_key(key, source)),
                        );

                        if pending_row_keys.len() >= PK_DELETE_FLUSH_BATCH_SIZE {
                            let chunk_keys = std::mem::take(&mut pending_row_keys);
                            let results = self
                                .write_key_based_chunk_with_shared_sequence(
                                    chunk_keys,
                                    &mut delete_sequence,
                                )
                                .await?;
                            staged.absorb(
                                results,
                                Self::assigned_delete_sequence(delete_sequence, table_name)?,
                                table_name,
                            )?;
                        }
                    }
                }

                if !pending_row_keys.is_empty() {
                    let chunk_keys = std::mem::take(&mut pending_row_keys);
                    let results = self
                        .write_key_based_chunk_with_shared_sequence(
                            chunk_keys,
                            &mut delete_sequence,
                        )
                        .await?;
                    staged.absorb(
                        results,
                        Self::assigned_delete_sequence(delete_sequence, table_name)?,
                        table_name,
                    )?;
                }
                if delete_sequence.is_none() {
                    return Ok(None);
                }
                self.prepare_staged_pk_deletions(staged).map(Some)
            }
            PkDeletionStrategyWithCache::PositionBased { .. } => {
                unreachable!(
                    "PositionBased strategy should have returned early via delete_filtered_rows_position_based"
                )
            }
        }
    }

    pub(crate) async fn prepare_delete(
        &self,
    ) -> super::super::Result<Option<PreparedDeletionPublish>> {
        if self.pk_deletion_strategy.is_position_based() {
            return Err(Error::Internal {
                table: self.table_metadata.table_name.clone(),
                message: "Staged delete publication is unsupported for position-based deletes"
                    .to_string(),
            });
        }
        let ctx = SessionContext::new_with_config_rt(
            SessionConfig::default(),
            Arc::clone(&self.runtime_env),
        );
        let listing_table = self.listing_table.load_full();
        let mut all_tables = vec![DeleteScanSource {
            min_delete_seq: None,
            insert_records: self.live_main_insert_records(),
            table: Arc::clone(&listing_table),
        }];
        all_tables.extend(self.additional_scan_tables.iter().cloned());
        if self.filters.is_empty() {
            // Delete-all removes every row, so no version is preferred over another and
            // the thresholds carry no information.
            let plain: Vec<Arc<ListingTable>> = all_tables
                .iter()
                .map(|source| Arc::clone(&source.table))
                .collect();
            self.prepare_delete_all_rows_from_tables(&ctx, &plain).await
        } else {
            self.prepare_delete_filtered_rows_from_tables(&ctx, &all_tables)
                .await
        }
    }

    /// Attempt to extract primary key values directly from the deletion filters,
    /// without scanning any data files.
    ///
    /// Expects a single filter expr and recognizes the following filter shapes:
    ///
    /// - **Single PK**: `pk_col IN (v1, v2, ...)` — a flat `Expr::InList`.
    /// - **Composite PK (OR-of-AND)**: A balanced OR tree of AND-equality
    ///   conjunctions, e.g. `(pk1 = a AND pk2 = b) OR (pk1 = c AND pk2 = d)`.
    /// - **Composite PK (tuple-IN)**: `(pk1, pk2, ...) IN ((a, b, ...), (c, d, ...))`.
    fn try_extract_pks_from_filters(&self, filters: &[Expr]) -> Option<ExtractedPkDeletes> {
        if filters.len() != 1 {
            return None;
        }
        let filter = &filters[0];
        let pk_columns = &self.table_metadata.primary_key;

        match &self.pk_deletion_strategy {
            PkDeletionStrategyWithCache::Int64Pk { .. } => {
                if pk_columns.len() != 1 {
                    return None;
                }
                pk_filter_extract::try_extract_int64_in_list(filter, &pk_columns[0])
                    .map(ExtractedPkDeletes::Int64)
            }
            PkDeletionStrategyWithCache::RowConverterBased { .. } => {
                let row_converter = self.pk_row_converter.as_ref()?;
                // Single non-Int64 PK - try InList first.
                if pk_columns.len() == 1 {
                    let pk_idx = *self.pk_column_indices.first()?;
                    let target_type = self.schema.field(pk_idx).data_type();
                    if let Some(keys) = pk_filter_extract::try_extract_in_list_row_keys(
                        filter,
                        &pk_columns[0],
                        target_type,
                        row_converter,
                    ) {
                        return Some(ExtractedPkDeletes::RowKeys(keys));
                    }
                }
                // Composite PK — balanced OR-of-AND equality tree or tuple-IN of struct literals.
                let pk_target_types: Vec<&arrow_schema::DataType> = self
                    .pk_column_indices
                    .iter()
                    .map(|&idx| self.schema.field(idx).data_type())
                    .collect();
                if let Some(keys) = pk_filter_extract::try_extract_tuple_in_pk_keys(
                    filter,
                    pk_columns,
                    &pk_target_types,
                    row_converter,
                ) {
                    return Some(ExtractedPkDeletes::RowKeys(keys));
                }
                pk_filter_extract::try_extract_or_of_and_pk_keys(
                    filter,
                    pk_columns,
                    &pk_target_types,
                    row_converter,
                )
                .map(ExtractedPkDeletes::RowKeys)
            }
            PkDeletionStrategyWithCache::PositionBased { .. } => None,
        }
    }

    fn coerce_filters_for_schema(&self) -> super::super::Result<Vec<Expr>> {
        let df_schema = DFSchema::try_from(self.schema.as_ref().clone())?;
        let mut coerced_filters = Vec::with_capacity(self.filters.len());

        for filter in &self.filters {
            let mut rewriter = TypeCoercionRewriter::new(&df_schema);
            coerced_filters.push(filter.clone().rewrite(&mut rewriter)?.data);
        }

        Ok(coerced_filters)
    }

    fn build_physical_filters(
        &self,
        filters: &[Expr],
    ) -> super::super::Result<Vec<Arc<dyn PhysicalExpr>>> {
        let df_schema = DFSchema::try_from(self.schema.as_ref().clone())?;
        let execution_props = ExecutionProps::new();

        let physical_filters = filters
            .iter()
            .map(|filter| create_physical_expr(filter, &df_schema, &execution_props))
            .collect::<datafusion_common::Result<Vec<_>>>()?;

        Ok(physical_filters)
    }

    fn apply_physical_filters_to_batch(
        &self,
        mut batch: arrow::record_batch::RecordBatch,
        physical_filters: &[Arc<dyn PhysicalExpr>],
    ) -> super::super::Result<arrow::record_batch::RecordBatch> {
        let table_name = &self.table_metadata.table_name;

        for filter in physical_filters {
            if batch.num_rows() == 0 {
                break;
            }

            let filter_value = filter.evaluate(&batch)?;
            let filter_array = filter_value.into_array(batch.num_rows())?;
            let filter_array = filter_array
                .as_any()
                .downcast_ref::<arrow::array::BooleanArray>()
                .ok_or_else(|| Error::Internal {
                    table: table_name.clone(),
                    message: format!(
                        "Filter expression did not evaluate to BooleanArray, got {:?}",
                        filter_array.data_type()
                    ),
                })?;

            batch = arrow::compute::filter_record_batch(&batch, filter_array)?;
        }

        Ok(batch)
    }

    async fn write_key_based_chunk_with_shared_sequence(
        &self,
        row_keys: Vec<Box<[u8]>>,
        delete_sequence: &mut Option<i64>,
    ) -> super::super::Result<Vec<DeletionVectorWriteResult>> {
        if row_keys.is_empty() {
            return Ok(Vec::new());
        }
        let sequence = if let Some(sequence) = delete_sequence {
            *sequence
        } else {
            let sequence = super::super::table::reserve_sequences_in(
                &self.seq_allocator,
                &self.catalog,
                &self.table_metadata.table_id,
                &self.table_metadata.table_name,
                1,
            )
            .await?;
            *delete_sequence = Some(sequence);
            sequence
        };
        let mut metadata = self.table_metadata.clone();
        metadata.current_sequence_number = sequence;
        DeletionVectorWriter::new(&metadata)
            .write(vec![DeletionVectorWriteSpec::new_key_based(row_keys)])
            .await
    }

    fn prepare_staged_pk_deletions(
        &self,
        mut staged: StagedPkDelete,
    ) -> super::super::Result<PreparedDeletionPublish> {
        let deleted_count =
            convert_to_u64_box(staged.new_count(), "deleted row count").map_err(|error| {
                Error::Internal {
                    table: self.table_metadata.table_name.clone(),
                    message: error.to_string(),
                }
            })?;
        let (delete_files, publish) = match &mut staged {
            StagedPkDelete::Int64 {
                new_pks,
                delete_sequence,
                delete_files,
                ..
            } => (
                std::mem::take(delete_files),
                PreparedDeletionCache::Int64 {
                    pks: std::mem::take(new_pks),
                    sequence: *delete_sequence,
                },
            ),
            StagedPkDelete::RowKeys {
                new_keys,
                delete_sequence,
                delete_files,
                ..
            } => (
                std::mem::take(delete_files),
                PreparedDeletionCache::RowKeys {
                    keys: std::mem::take(new_keys),
                    sequence: *delete_sequence,
                },
            ),
        };
        Ok(PreparedDeletionPublish {
            strategy: self.pk_deletion_strategy.clone(),
            table_memory: Arc::clone(&self.table_memory),
            delete_files,
            publish,
            deleted_count,
            cleanup_armed: true,
        })
    }

    async fn cleanup_uncommitted_delete_file(path: &str) {
        cleanup_uncommitted_delete_paths(&[std::path::PathBuf::from(path)]).await;
    }
}

#[async_trait]
impl DeletionSink for CayenneDeletionSink {
    async fn delete_from(
        &self,
        _context: Arc<TaskContext>,
    ) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        // Acquire write lock (if provided) to prevent racing with concurrent inserts or catalog refreshes.
        // When called from within write_all_append (e.g. retention filters), the caller already
        // holds the lock, so write_lock is None to avoid deadlocking the non-reentrant mutex.
        let _write_guard = match &self.write_lock {
            Some(lock) => Some(lock.lock().await),
            None => None,
        };

        let ctx = SessionContext::new_with_config_rt(
            SessionConfig::default(),
            Arc::clone(&self.runtime_env),
        );

        // Wait-free ArcSwap snapshot. Concurrent listing-table refreshes are
        // serialized against this code path by `self.write_lock`, which the
        // caller holds (or, for sub-sinks, is held by the orchestrating
        // operation), so we never observe a torn swap here.
        let listing_table = self.listing_table.load_full();

        // Collect all tables to scan: main listing table + the extra tables
        // (protected snapshots and, for cold-tier tables, the cold-tier files).
        let mut all_tables = vec![DeleteScanSource {
            min_delete_seq: None,
            insert_records: self.live_main_insert_records(),
            table: Arc::clone(&listing_table),
        }];
        all_tables.extend(self.additional_scan_tables.iter().cloned());

        let prepared = if self.filters.is_empty() {
            // See above: delete-all needs no per-snapshot visibility.
            let plain: Vec<Arc<ListingTable>> = all_tables
                .iter()
                .map(|source| Arc::clone(&source.table))
                .collect();
            self.prepare_delete_all_rows_from_tables(&ctx, &plain).await
        } else if self.pk_deletion_strategy.is_position_based() {
            // A position tombstone names a file and row position, so it can only ever
            // hide the version it matched — the per-snapshot thresholds that keep a key
            // tombstone off a live row carry nothing for it.
            let position_tables: Vec<Arc<ListingTable>> = all_tables
                .iter()
                .map(|source| Arc::clone(&source.table))
                .collect();
            return self
                .delete_filtered_rows_position_based(&ctx, &position_tables)
                .await
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>);
        } else {
            self.prepare_delete_filtered_rows_from_tables(&ctx, &all_tables)
                .await
        }
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

        let Some(mut prepared) = prepared else {
            return Ok(0);
        };
        if let Err(error) = self
            .catalog
            .add_delete_files(prepared.delete_files().to_vec())
            .await
        {
            return Err(Box::new(error));
        }
        let deleted = prepared.deleted_count();
        prepared.mark_catalog_committed();
        prepared
            .publish()
            .map_err(|error| Box::new(error) as Box<dyn std::error::Error + Send + Sync>)?;
        Ok(deleted)
    }
}
