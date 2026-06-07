/*
Copyright 2025 The Spice.ai OSS Authors

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

//! Trait abstraction for metadata catalog operations.
//!
//! This trait defines the interface for managing table metadata
//! and file references. It can be implemented by different RDBMS backends
//! (`SQLite`, `PostgreSQL`, etc.).

use super::metadata::{
    CreateTableOptions, DeleteFile, InlinedData, InlinedDataStats, InlinedDelete,
    PartitionMetadata, TableMetadata, TableStatistics,
};
use async_trait::async_trait;
use snafu::Snafu;
use std::collections::HashMap;
use std::sync::Arc;

/// Error type for catalog operations.
#[expect(missing_docs)]
#[derive(Debug, Snafu)]
pub enum CatalogError {
    /// Database error
    #[snafu(display("Database error: {message}"))]
    Database {
        /// Error message
        message: String,
    },

    /// Table not found
    #[snafu(display("Table not found: {table_name}"))]
    TableNotFound {
        /// Name of the table that was not found
        table_name: String,
    },

    /// Table already exists
    #[snafu(display("Table already exists: {table_name}"))]
    TableAlreadyExists {
        /// Name of the table that already exists
        table_name: String,
    },

    /// Invalid operation
    #[snafu(display("Invalid operation: {message} {source}"))]
    InvalidOperation {
        /// Description of the invalid operation
        message: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// Invalid operation (without underlying source error)
    #[snafu(display("Invalid operation: {message}"))]
    InvalidOperationNoSource {
        /// Description of the invalid operation
        message: String,
    },

    /// IO error
    #[snafu(display("IO error: {source}"))]
    Io {
        /// The underlying IO error
        source: std::io::Error,
    },

    /// `SQLite` error
    #[snafu(transparent)]
    Sqlite {
        /// The underlying `SQLite` error
        source: rusqlite::Error,
    },

    /// Task join error
    #[snafu(transparent)]
    TaskJoin {
        /// The underlying task join error
        source: tokio::task::JoinError,
    },

    /// IO error (from `std::io::Error`)
    #[snafu(transparent)]
    IoError {
        /// The underlying IO error  
        source: std::io::Error,
    },

    /// Lock poisoning error
    #[snafu(display(
        "Lock poisoned during {operation}: a thread panicked while holding this lock. This indicates an internal error that requires restarting the runtime."
    ))]
    LockPoisoned {
        /// The operation that failed due to lock poisoning
        operation: String,
    },

    #[snafu(display("Invalid database path: {path}"))]
    InvalidDatabasePath { path: String },

    #[snafu(display("The function '{function}' is not implemented"))]
    NotImplemented { function: String },

    #[snafu(display(
        "Cayenne metadata schema mismatch for table '{table}'. The metadata database format has changed and is incompatible with this version. To continue, clear your acceleration data (delete the Cayenne metadata directory) so it can be recreated. Existing accelerated data will be re-synced from the source."
    ))]
    SchemaMismatch { table: String },

    #[snafu(display(
        "Deletion vectors require non-negative row IDs, found negative values: {row_ids}"
    ))]
    NegativeRowId { row_ids: String },

    #[snafu(display("Failed to get catalog table. {source}"))]
    FailedToGetTable { source: Box<CatalogError> },

    #[snafu(display("Failed to get current snapshot. {source}"))]
    FailedToGetCurrentSnapshot { source: Box<CatalogError> },

    #[snafu(display("Failed to set current snapshot. {source}"))]
    FailedToSetCurrentSnapshot { source: Box<CatalogError> },

    #[snafu(display("Failed to create catalog table. {source}"))]
    FailedToCreateTable { source: Box<CatalogError> },

    #[snafu(display("Failed to add delete file. {source}"))]
    FailedToAddDeleteFile { source: Box<CatalogError> },

    #[snafu(display("Failed to get delete files for table. {source}"))]
    FailedToGetTableDeleteFiles { source: Box<CatalogError> },

    #[snafu(display("Failed to add partition. {source}"))]
    FailedToAddPartition { source: Box<CatalogError> },

    #[snafu(display("Failed to get partitions. {source}"))]
    FailedToGetPartitions { source: Box<CatalogError> },

    #[snafu(display("Failed to get partition. {source}"))]
    FailedToGetPartition { source: Box<CatalogError> },

    #[snafu(display(
        "Multiple partitions found for table ID {table_id} and partition value '{partition_value}'"
    ))]
    InvalidPartitionCount {
        table_id: i64,
        partition_value: String,
    },

    #[snafu(display("Failed to update partition stats. {source}"))]
    FailedToUpdatePartitionStats { source: Box<CatalogError> },

    #[snafu(display("Failed to get partition stats. {source}"))]
    FailedToGetPartitionStats { source: Box<CatalogError> },

    #[snafu(display("Failed to get partition data files. {source}"))]
    FailedToGetPartitionDataFiles { source: Box<CatalogError> },

    #[snafu(display(
        "Turso backend requested but 'turso' feature is not enabled. Enable with --features turso"
    ))]
    TursoNotEnabled,

    /// Constraint violation (e.g., unique constraint, foreign key constraint)
    /// Used for handling concurrent insert conflicts.
    #[snafu(display("Constraint violation: {message}"))]
    ConstraintViolation {
        /// Details about the constraint that was violated
        message: String,
    },

    /// Invalid partition metadata (e.g., mismatched columns/values count, empty partition)
    /// This prevents persisting malformed partition data that could cause incorrect query results.
    #[snafu(display("Invalid partition metadata: {message}"))]
    InvalidPartitionMetadata {
        /// Description of why the partition metadata is invalid
        message: String,
    },

    #[snafu(display(
        "Table '{table_name}' already exists with different configuration. Delete the acceleration, and try again."
    ))]
    ChangedConfiguration { table_name: String },

    #[snafu(display(
        "Table '{table_name}' metadata is invalid or corrupted. Delete the acceleration, and try again. {source}"
    ))]
    InvalidMetadata {
        table_name: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

/// Result type for catalog operations.
pub type CatalogResult<T> = std::result::Result<T, CatalogError>;

/// Snapshot sequence metadata to commit atomically with a related catalog mutation.
#[derive(Debug, Clone)]
pub struct SnapshotSequenceCommit {
    /// Snapshot id whose sequence should be recorded.
    pub snapshot_id: String,
    /// Sequence number assigned to the snapshot.
    pub sequence_number: i64,
}

// Transaction support is currently not exposed at the catalog level.
// Each catalog implementation can use backend-specific transactions internally
// to ensure atomicity of operations.
//
// Future work: Expose catalog-level transactions when needed.

/// Trait for metadata catalog operations.
///
/// This trait provides the core operations needed to manage a Cayenne catalog,
/// including table creation and file tracking.
#[async_trait]
pub trait MetadataCatalog: Send + Sync {
    /// Initialize the catalog, creating necessary tables if they don't exist.
    async fn init(&self) -> CatalogResult<()>;

    /// List all table names in the catalog.
    async fn list_table_names(&self) -> CatalogResult<Vec<String>>;

    /// Create a new table.
    async fn create_table(&self, options: CreateTableOptions) -> CatalogResult<String>;

    /// Get table metadata by name.
    async fn get_table(&self, table_name: &str) -> CatalogResult<TableMetadata>;

    /// Set the current snapshot ID for a table (`UUIDv7` string).
    async fn set_current_snapshot(&self, table_id: &str, snapshot_id: &str) -> CatalogResult<()>;

    /// Increment the table's sequence number and return the new value.
    ///
    /// Sequence numbers are used to order operations (inserts and deletes).
    /// This method atomically increments and returns the new sequence.
    async fn increment_sequence_number(&self, table_id: &str) -> CatalogResult<i64>;

    /// Get the current sequence number for a table.
    async fn get_sequence_number(&self, table_id: &str) -> CatalogResult<i64>;

    /// Reserve `count` consecutive sequence numbers for this table in one atomic
    /// operation and return the *first* number of the reserved block.
    /// `count` must be at least 1.
    ///
    /// The caller may then use `[result, result + count)` locally (e.g. `result`,
    /// `result+1`) without any further catalog round-trips. This is the
    /// recommended way for writers that need more than one sequence number for a
    /// logical operation (most notably the on-conflict/upsert path, which needs a
    /// delete sequence and a higher insert sequence for the replacement rows).
    ///
    /// On serialized backends such as the embedded `SQLite` metastore, this
    /// significantly reduces writer-lock acquisitions and RPC latency compared
    /// with calling `increment_sequence_number` multiple times.
    ///
    /// The returned value is guaranteed to be unique and strictly increasing
    /// across all calls for the table (modulo crashes that lose in-flight
    /// reservations — gaps are acceptable for the sequence ordering contract).
    async fn reserve_sequence_numbers(&self, table_id: &str, count: u32) -> CatalogResult<i64>;

    /// Add a delete file (deletion vector) for a data file.
    ///
    /// Tracks a deletion vector file that marks rows as deleted in a specific
    /// virtual file (`ListingTable`).
    async fn add_delete_file(&self, delete_file: DeleteFile) -> CatalogResult<String>;

    /// Get all active delete files for a table (across all virtual files).
    async fn get_table_delete_files(&self, table_id: &str) -> CatalogResult<Vec<DeleteFile>>;

    /// Remove delete files (deletion vectors) by ID for a table.
    async fn remove_delete_files(
        &self,
        table_id: &str,
        delete_file_ids: &[String],
    ) -> CatalogResult<()>;

    /// Clear all delete files for a table.
    ///
    /// This is called after compaction to remove deletion vectors that have been
    /// applied to the data files.
    async fn clear_delete_files(&self, table_id: &str) -> CatalogResult<()>;

    /// Add an insert record for a primary key with its sequence number.
    ///
    /// Insert records track PKs that were re-inserted after being deleted.
    /// The sequence number determines ordering: if `insert_sequence` > `delete_sequence`
    /// for a PK, the row is visible; otherwise it's filtered out.
    ///
    /// Uses INSERT OR REPLACE to update the sequence if the PK already exists.
    ///
    /// # Arguments
    ///
    /// * `table_id` - The table to add the insert record to
    /// * `pk_bytes` - The primary key bytes (from `RowConverter` or Int64 encoding)
    /// * `sequence_number` - The sequence at which this insert occurred
    async fn add_insert_record(
        &self,
        table_id: &str,
        pk_bytes: Vec<u8>,
        sequence_number: i64,
    ) -> CatalogResult<()>;

    /// Add multiple insert records in a batch.
    ///
    /// More efficient than calling `add_insert_record` multiple times.
    async fn add_insert_records_batch(
        &self,
        table_id: &str,
        pk_bytes_list: Vec<Vec<u8>>,
        sequence_number: i64,
    ) -> CatalogResult<()>;

    /// Atomically commit the catalog side of an on-conflict (upsert)
    /// deletion.
    ///
    /// Implementations MUST commit every delete-file row and every
    /// insert-record row in one durable transaction. The caller allocates
    /// the needed sequence numbers (via `reserve_sequence_numbers` or
    /// `increment_sequence_number`) before this call, so a failed transaction
    /// may leave a sequence-number gap, but it must not leave a partially
    /// committed delete-file / insert-record pair. A non-atomic implementation
    /// reintroduces the crash window this method exists to close.
    /// When insert records are provided, `insert_sequence` must be greater than
    /// each delete file's sequence number so replacement rows remain visible.
    ///
    /// This replaces the legacy 3-call sequence on the on-conflict path
    /// (`add_delete_file` per file → `add_insert_records_batch`), which
    /// left a crash window where the catalog could persist deletion
    /// records without their corresponding insert sequences. After
    /// restart, the new row (already in the data files) would then be
    /// permanently hidden by the deletion filter — see
    /// [`crate::provider::table::CayenneTableProvider::apply_on_conflict_deletions`]
    /// for the original sequence.
    ///
    /// # Errors
    ///
    /// Returns an error if the transaction cannot be opened, any insert
    /// fails, or the commit fails. Failures roll back the entire txn —
    /// the catalog is unchanged.
    async fn commit_on_conflict_deletions(
        &self,
        delete_files: Vec<DeleteFile>,
        table_id: &str,
        insert_pk_bytes_list: Vec<Vec<u8>>,
        insert_sequence: i64,
        snapshot_sequence: Option<SnapshotSequenceCommit>,
    ) -> CatalogResult<()>;

    /// Stage-A fold for a pipelined CDC upsert: commit the on-conflict deletion
    /// metadata (delete files + insert records + protected-snapshot sequence)
    /// AND, in the SAME backend transaction, INSERT the inline tombstone (Option
    /// D, `published = false`) for any inlined rows the upsert replaces.
    ///
    /// This is the single-transaction equivalent of calling
    /// [`Self::commit_on_conflict_deletions`] immediately followed by
    /// [`Self::add_inlined_delete`] with `published = false`: it preserves the
    /// exact statement order (delete files → insert records → snapshot sequence →
    /// tombstone INSERT) but acquires the process-wide metastore writer **once**
    /// instead of twice. On a serialized backend (the embedded `SQLite` metastore
    /// WAL-serializes every writer across all tables) this halves the writer
    /// round-trips a heavy-upsert table's staged batch pays, which is the
    /// dominant per-batch metastore-queueing term under sustained CDC load.
    ///
    /// `inline_tombstone` carries the already-serialized deletion IPC and key
    /// count; its `inlined_id` (if empty) is generated inside the transaction and
    /// returned so the owning snapshot's finalize can flip exactly that tombstone
    /// to `published = true` via [`Self::mark_inlined_delete_published`]. Returns
    /// `None` when no inline tombstone was supplied.
    ///
    /// Durability ordering is unchanged versus the two-call form: the staging WAL
    /// (which lists the replacement files) is written by the caller BEFORE this
    /// method runs, and the Stage-B `published` flip runs strictly AFTER, so the
    /// Option-D exactly-once crash matrix holds. Folding the two writes into one
    /// transaction is strictly safer than the prior two-transaction form — it
    /// removes the intermediate crash state where the snapshot sequence was
    /// durable but the tombstone was not.
    ///
    /// `pending_durable_flips` carries `inlined_id`s of PREVIOUSLY-finalized
    /// tombstones (cycle-4 lever b1★) whose owning Stage-B activated them in
    /// memory but deferred their durable `published = 1` flip. They are applied as
    /// extra `UPDATE … SET published = 1` statements inside this SAME transaction,
    /// so the deferred flips cost NO additional metastore-writer acquisition —
    /// they ride this batch's commit. Idempotent: an id whose row was already
    /// flipped or cleared by a checkpoint is a no-op UPDATE. They are applied
    /// LAST (after the tombstone INSERT) and only durably take effect on commit,
    /// so a rollback leaves them un-flipped — and the caller only removes them
    /// from its in-memory pending queue on commit success, so a failed commit
    /// re-defers them to the next batch (or the maintenance drain). Their durable
    /// `published = 0` state is independently crash-healed by
    /// `publish_orphan_inlined_deletes` on reopen, so a never-committed flip never
    /// resurfaces a row.
    ///
    /// # Errors
    ///
    /// Returns an error if the transaction cannot be opened, any statement fails,
    /// or the commit fails. Failures roll back the entire transaction; the
    /// catalog is left unchanged (no partial delete-file / insert-record /
    /// snapshot-sequence / tombstone / pending-flip state).
    #[expect(clippy::too_many_arguments)]
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
        // Default: preserve the previous two-transaction behavior for any
        // implementor without a single-transaction primitive. `CayenneCatalog`
        // overrides this to fold all writes into one transaction.
        self.commit_on_conflict_deletions(
            delete_files,
            table_id,
            insert_pk_bytes_list,
            insert_sequence,
            snapshot_sequence,
        )
        .await?;
        // Apply the deferred durable flips (b1★). Not transactional in the default
        // path, but each is an idempotent single-row UPDATE and the caller only
        // clears its in-memory queue on overall success.
        for inlined_id in pending_durable_flips {
            self.mark_inlined_delete_published(table_id, inlined_id)
                .await?;
        }
        match inline_tombstone {
            Some(tombstone) => Ok(Some(self.add_inlined_delete(tombstone).await?)),
            None => Ok(None),
        }
    }

    /// Get all insert records for a table.
    ///
    /// Returns a map of PK bytes to their sequence numbers.
    async fn get_insert_records(&self, table_id: &str) -> CatalogResult<HashMap<Box<[u8]>, i64>>;

    /// Clear all insert records for a table.
    ///
    /// Called after compaction when deletions and insert records have been merged.
    async fn clear_insert_records(&self, table_id: &str) -> CatalogResult<()>;

    /// Set the sequence number for a snapshot.
    ///
    /// This records when the snapshot was created relative to deletions.
    /// Used for Iceberg-style sequence ordering: deletions only apply to
    /// snapshots with sequence <= `delete_sequence`.
    async fn set_snapshot_sequence(
        &self,
        table_id: &str,
        snapshot_id: &str,
        sequence_number: i64,
    ) -> CatalogResult<()>;

    /// Get the sequence number for a snapshot.
    ///
    /// Returns `None` if the snapshot has no sequence (created before sequence tracking).
    async fn get_snapshot_sequence(
        &self,
        table_id: &str,
        snapshot_id: &str,
    ) -> CatalogResult<Option<i64>>;

    /// Get all snapshot sequences for a table.
    ///
    /// Returns a map of `snapshot_id` -> `sequence_number` for all snapshots
    /// that have sequence tracking enabled.
    async fn get_all_snapshot_sequences(
        &self,
        table_id: &str,
    ) -> CatalogResult<HashMap<String, i64>>;

    /// Clear the sequence record for a specific snapshot.
    ///
    /// This is used when a protected snapshot is superseded by a newer one.
    /// The old sequence record becomes orphaned and can be cleaned up.
    async fn clear_snapshot_sequence(&self, table_id: &str, snapshot_id: &str)
    -> CatalogResult<()>;

    /// Atomically update snapshot and clear delete files in a single transaction.
    async fn commit_compaction(&self, table_id: &str, new_snapshot_id: &str) -> CatalogResult<()>;

    /// Atomically swap a subset of protected snapshots for a single merged
    /// snapshot (fast protected-snapshot compaction, "Step 1").
    ///
    /// Runs in one transaction:
    /// 1. CAS guard: verifies every `old_snapshot_id` still has a sequence row.
    /// 2. Deletes those sequence rows.
    /// 3. Inserts the new merged snapshot's sequence row.
    ///
    /// Unlike [`commit_compaction`], this does NOT touch `current_snapshot_id`,
    /// delete files, or insert records — the current snapshot is unchanged, so
    /// its deletion vectors must remain intact (the merged inputs only had
    /// deletions with `seq > max_delete_seq_at_creation` applied during the
    /// rewrite, while `current` still relies on the full delete-file set).
    ///
    /// Returns `Ok(false)` if any input snapshot is no longer active (the
    /// caller should discard the rewritten output and retry on a later trigger).
    async fn swap_protected_snapshots(
        &self,
        table_id: &str,
        old_snapshot_ids: &[String],
        new_snapshot_id: &str,
        new_sequence_number: i64,
    ) -> CatalogResult<bool>;

    /// Atomically commit an overwrite: update the snapshot pointer, clear all
    /// per-snapshot delete tracking, AND drop inlined data, inlined deletes,
    /// and table statistics — everything that belonged to the old snapshot
    /// and no longer applies once the user has replaced the table's contents.
    ///
    /// Differs from [`commit_compaction`] in that compaction PRESERVES inlined
    /// data (the inlined memtable is still valid for the new snapshot — the
    /// rewrite only consolidates Vortex files). Overwrite REPLACES everything,
    /// so anything keyed on the old snapshot must be dropped atomically with
    /// the pointer flip; otherwise a crash between the pointer flip and the
    /// (separate) inlined-data clear would leave stale inlined rows that scan
    /// would union into the new snapshot's results.
    ///
    /// # Errors
    ///
    /// Returns an error if the transaction cannot be committed.
    async fn commit_overwrite(&self, table_id: &str, new_snapshot_id: &str) -> CatalogResult<()>;

    /// Add a partition to a table.
    async fn add_partition(&self, partition: PartitionMetadata) -> CatalogResult<String>;

    /// Get all partitions for a table.
    async fn get_partitions(&self, table_id: &str) -> CatalogResult<Vec<PartitionMetadata>>;

    /// Upsert table-level aggregate statistics.
    ///
    /// Replaces existing stats for the given table. The statistics blob contains
    /// a serialized Vortex `FileStatistics` flatbuffer with per-column stats.
    async fn upsert_table_statistics(&self, stats: &TableStatistics) -> CatalogResult<()>;

    /// Get table-level aggregate statistics for a table.
    async fn get_table_statistics(&self, table_id: &str) -> CatalogResult<Option<TableStatistics>>;

    /// Clear table-level aggregate statistics for a table.
    async fn clear_table_statistics(&self, table_id: &str) -> CatalogResult<()>;

    /// Upsert the persisted primary-key existence index (a bloom checkpoint),
    /// tagged with the snapshot id it covers. Stored in the metastore so it is
    /// captured by metastore snapshots — letting both a restart and a new node
    /// bootstrapped from a snapshot skip the O(total-rows) full keyset rebuild.
    async fn upsert_pk_index(
        &self,
        table_id: &str,
        snapshot_id: &str,
        index_blob: &[u8],
    ) -> CatalogResult<()>;

    /// Get the persisted PK existence index for a table as `(snapshot_id, blob)`,
    /// or `None` if absent. Callers must treat the blob as an optimization hint
    /// and fall back to a full rebuild on any mismatch/corruption.
    async fn get_pk_index(&self, table_id: &str) -> CatalogResult<Option<(String, Vec<u8>)>>;

    /// Clear the persisted PK existence index for a table.
    async fn clear_pk_index(&self, table_id: &str) -> CatalogResult<()>;

    // ── Inlined data (data inlining for small writes) ──────────────────

    /// Add a small batch of insert data inlined in the metastore.
    ///
    /// The `data_ipc` field contains an Arrow IPC serialized `RecordBatch`.
    async fn add_inlined_data(&self, data: InlinedData) -> CatalogResult<String>;

    /// Get all inlined data entries for a table.
    async fn get_inlined_data(&self, table_id: &str) -> CatalogResult<Vec<InlinedData>>;

    /// Get the inlined data entries for a table whose `sequence_number` is
    /// strictly greater than `after_sequence`, ordered by `sequence_number`.
    ///
    /// This is the incremental read path's variant of [`get_inlined_data`]: the
    /// inline-memtable cache (`CayenneTableProvider::populate_inlined_cache`)
    /// uses it to fetch ONLY the rows appended since the last materialized view
    /// instead of re-reading and re-decoding the entire `cayenne_inlined_data`
    /// corpus on every cache miss under sustained CDC. Callers only route the
    /// query here when they have proven (via a structural-epoch check) that the
    /// generation delta was an append-only mutation — no rewrite, removal, or
    /// newly published tombstone — so the previously materialized entries remain
    /// byte-for-byte valid and the appended rows can simply be merged on top.
    ///
    /// The default implementation reads the full corpus and filters in memory so
    /// every [`MetadataCatalog`] impl keeps compiling; production implementors
    /// SHOULD override it to push the `sequence_number > ?` predicate into SQL
    /// so the expensive `data_ipc` blobs of older entries are never shipped.
    async fn get_inlined_data_above_sequence(
        &self,
        table_id: &str,
        after_sequence: i64,
    ) -> CatalogResult<Vec<InlinedData>> {
        let mut all = self.get_inlined_data(table_id).await?;
        all.retain(|entry| entry.sequence_number > after_sequence);
        Ok(all)
    }

    /// Get inlined data entries for a specific partition of a table.
    async fn get_inlined_data_for_partition(
        &self,
        table_id: &str,
        partition_key: &str,
    ) -> CatalogResult<Vec<InlinedData>>;

    /// Get the total number of inlined rows for a table.
    async fn get_inlined_data_count(&self, table_id: &str) -> CatalogResult<i64>;

    /// Get aggregate inline data size information for a table.
    async fn get_inlined_data_stats(&self, table_id: &str) -> CatalogResult<InlinedDataStats>;

    /// Remove all inlined data for a table (called after checkpoint flushes to Vortex).
    async fn clear_inlined_data(&self, table_id: &str) -> CatalogResult<()>;

    /// Remove all inlined data and inlined deletes for a table atomically.
    ///
    /// Implementations may override this to use a single backend transaction or
    /// batch call. The default preserves the existing behavior for implementors
    /// that do not have a combined primitive.
    async fn clear_inlined_data_and_deletes(&self, table_id: &str) -> CatalogResult<()> {
        self.clear_inlined_data(table_id).await?;
        self.clear_inlined_deletes(table_id).await
    }

    /// Add a small batch of delete identifiers inlined in the metastore.
    ///
    /// Returns the `inlined_id` of the written row so the caller can later flip
    /// its activation flag with [`Self::mark_inlined_delete_published`]. A
    /// staged inline-conflict tombstone is written with [`InlinedDelete::published`]
    /// = `false` and stays inert (the read filter skips it) until its owning
    /// snapshot publishes.
    async fn add_inlined_delete(&self, delete: InlinedDelete) -> CatalogResult<String>;

    /// Durably flip a single inline tombstone's activation flag to `published =
    /// true`, identified by `(table_id, inlined_id)`.
    ///
    /// Called from the staged on-conflict finalize (under the listing fence,
    /// before the replacement files are moved into the target snapshot) so the
    /// tombstone becomes active exactly when — and never before — its
    /// replacement rows become discoverable. Idempotent: flipping an
    /// already-published row is a no-op.
    async fn mark_inlined_delete_published(
        &self,
        table_id: &str,
        inlined_id: &str,
    ) -> CatalogResult<()>;

    /// Durably revert a single inline tombstone's activation flag back to
    /// `published = false`, identified by `(table_id, inlined_id)`. The exact
    /// inverse of [`Self::mark_inlined_delete_published`].
    ///
    /// Called from the staged on-conflict finalize ERROR path: if the file move
    /// / publish fails AFTER the flag was flipped, the tombstone would otherwise
    /// be left active (`published = true`) with its replacement still absent from
    /// the listing, so a concurrent inline insert (which bumps
    /// `inlined_generation` and rebuilds the cache) would apply the tombstone and
    /// hide the old row while the replacement is unlisted — a transient vanish
    /// that heals only on reopen. Reverting restores self-consistency
    /// immediately. Re-published exactly-once on the next open by
    /// [`Self::publish_orphan_inlined_deletes`] after the interrupted move is
    /// recovered. Idempotent: reverting an already-unpublished row is a no-op.
    async fn mark_inlined_delete_unpublished(
        &self,
        table_id: &str,
        inlined_id: &str,
    ) -> CatalogResult<()>;

    /// Durably flip EVERY currently-unpublished inline tombstone for a table to
    /// `published = true`, returning how many rows were flipped.
    ///
    /// Called once at table open, AFTER `ensure_no_incomplete_write` has
    /// recovered any interrupted staged append (which moves the replacement files
    /// into their snapshot — completing the upsert without data loss). At that
    /// point every `published = false` tombstone corresponds to staged work whose
    /// replacement is now durable, so activating them makes the upsert apply
    /// exactly once across the crash (replacement visible, old inline copy
    /// hidden) rather than leaving a duplicate. There are no in-flight runtime
    /// writers at open time, so this never races a live stage.
    async fn publish_orphan_inlined_deletes(&self, table_id: &str) -> CatalogResult<u64>;

    /// Atomically rewrite existing inline data rows, remove emptied inline data rows,
    /// and append new inline data rows.
    ///
    /// Inline mutations rewrite row-store metadata entries in place instead of adding
    /// delete-marker side records. Newly appended inline data rows are stamped with
    /// the caller-supplied `assigned_sequence` (all appended rows in one call share
    /// that sequence); rewritten rows retain their original sequence.
    ///
    /// `assigned_sequence` is reserved by the caller from the per-table in-memory
    /// sequence allocator (lever B2) — strictly above any other sequence the
    /// caller allocated for the same logical operation (e.g. a paired file
    /// `delete_seq`). This call performs NO counter mutation: the allocator owns
    /// allocation and keeps `cayenne_table.current_sequence_number` at-or-ahead
    /// via its reserve-ahead refill, so stamping the row from a bound parameter
    /// (instead of bumping + reading back the DB counter) keeps the inlined-row
    /// sequence strictly above every value the allocator has handed out.
    ///
    /// Returns `Some(assigned_sequence)` when new `data` rows were appended (so
    /// the caller can advance its published-visibility watermark), or `None` when
    /// no new data rows were appended.
    async fn commit_inlined_mutation(
        &self,
        table_id: &str,
        updated_data: Vec<InlinedData>,
        deleted_inlined_ids: Vec<String>,
        data: Vec<InlinedData>,
        assigned_sequence: i64,
    ) -> CatalogResult<Option<i64>>;

    /// Get all inlined delete entries for a table.
    async fn get_inlined_deletes(&self, table_id: &str) -> CatalogResult<Vec<InlinedDelete>>;

    /// Get only the *published* inlined delete entries for a table.
    ///
    /// This is the hot read path's variant of [`get_inlined_deletes`]: it pushes
    /// the `published = 1` predicate into SQL so the expensive `delete_ipc` blobs
    /// of in-flight (`published = 0`) tombstones are never materialised or
    /// shipped only to be discarded in memory. The `published = 1` SQL filter is
    /// exactly equivalent to skipping `!delete.published` rows in Rust, so it
    /// preserves the per-tombstone activation gate that
    /// `load_inlined_deletion_maps` relies on for the no-transient-PK-vanish
    /// invariant — it returns every published tombstone and no unpublished one.
    ///
    /// Diagnostic/test callers that must inspect unpublished rows continue to use
    /// [`get_inlined_deletes`].
    async fn get_published_inlined_deletes(
        &self,
        table_id: &str,
    ) -> CatalogResult<Vec<InlinedDelete>>;

    /// Remove all inlined deletes for a table (called after checkpoint).
    async fn clear_inlined_deletes(&self, table_id: &str) -> CatalogResult<()>;

    /// Shutdown the catalog, performing any necessary cleanup (e.g., WAL checkpoint, optimize).
    /// Default implementation does nothing.
    async fn shutdown(&self) -> CatalogResult<()> {
        Ok(())
    }

    /// Run a NON-BLOCKING WAL checkpoint off the hot commit path (cycle-5 TASK
    /// 2b). Called from the background maintenance tick so the WAL is drained on
    /// a timer instead of relying on the inline `wal_autocheckpoint` firing a
    /// passive checkpoint on a CDC commit/UPDATE — which lands an fsync inside
    /// the WAL-write-locked Stage-A/Stage-B window. The checkpoint must NOT block
    /// writers or wait for readers (`SQLite` `PASSIVE`), so a failure or a busy WAL
    /// is a no-op the next tick retries. Default implementation does nothing.
    async fn checkpoint_wal(&self) -> CatalogResult<()> {
        Ok(())
    }

    /// Drop a table and all its associated metadata (delete files, insert records,
    /// snapshot sequences, partitions).
    ///
    /// This is used for `file_create` mode to clean up existing table metadata
    /// before recreating the table fresh.
    ///
    /// # Arguments
    ///
    /// * `table_name` - The name of the table to drop
    ///
    /// # Returns
    ///
    /// Returns `Ok(true)` if the table was dropped, `Ok(false)` if the table didn't exist.
    async fn drop_table(&self, table_name: &str) -> CatalogResult<bool>;

    /// Export the metastore rows for `dataset_name` as a portable, versioned
    /// slice with path columns rewritten relative to `data_dir_anchor`.
    ///
    /// Default implementation returns [`CatalogError::InvalidOperation`].
    /// `CayenneCatalog` overrides this with a real implementation.
    ///
    /// # Errors
    ///
    /// Returns an error if the dataset is not present or the underlying metastore
    /// query fails.
    async fn export_dataset_slice(
        &self,
        dataset_name: &str,
        data_dir_anchor: &std::path::Path,
    ) -> CatalogResult<crate::metastore::snapshot::DatasetMetastoreSlice> {
        let _ = (dataset_name, data_dir_anchor);
        Err(CatalogError::InvalidOperationNoSource {
            message: "export_dataset_slice is not supported by this catalog".to_string(),
        })
    }

    /// Atomically import a dataset slice into the metastore, replacing any
    /// prior rows for the same `dataset_name`. Path columns are re-anchored
    /// at `data_dir_anchor`.
    ///
    /// Default implementation returns [`CatalogError::InvalidOperation`].
    /// `CayenneCatalog` overrides this with a real implementation.
    ///
    /// # Errors
    ///
    /// Returns an error if the slice format is unsupported, the engine identifier
    /// mismatches, or any DML in the underlying transaction fails.
    async fn import_dataset_slice(
        &self,
        slice: &crate::metastore::snapshot::DatasetMetastoreSlice,
        data_dir_anchor: &std::path::Path,
    ) -> CatalogResult<()> {
        let _ = (slice, data_dir_anchor);
        Err(CatalogError::InvalidOperationNoSource {
            message: "import_dataset_slice is not supported by this catalog".to_string(),
        })
    }
}

/// Factory trait for creating catalog instances.
pub trait CatalogFactory: Send + Sync {
    /// Create a new catalog instance.
    ///
    /// # Errors
    ///
    /// Returns an error if the catalog cannot be created.
    fn create(&self, connection_string: &str) -> CatalogResult<Arc<dyn MetadataCatalog>>;
}
