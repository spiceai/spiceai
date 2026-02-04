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

use super::metadata::{CreateTableOptions, DeleteFile, PartitionMetadata, TableMetadata};
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
    #[snafu(display("Lock poisoned during {operation}: a thread panicked while holding this lock. This indicates an internal error that requires restarting the runtime."))]
    LockPoisoned {
        /// The operation that failed due to lock poisoning
        operation: String,
    },

    #[snafu(display("Invalid database path: {path}"))]
    InvalidDatabasePath { path: String },

    #[snafu(display("The function '{function}' is not implemented"))]
    NotImplemented { function: String },

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
}

/// Result type for catalog operations.
pub type CatalogResult<T> = std::result::Result<T, CatalogError>;

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

    /// Create a new table.
    async fn create_table(&self, options: CreateTableOptions) -> CatalogResult<i64>;

    /// Get table metadata by name.
    async fn get_table(&self, table_name: &str) -> CatalogResult<TableMetadata>;

    /// Set the current snapshot ID for a table (`UUIDv7` string).
    async fn set_current_snapshot(&self, table_id: i64, snapshot_id: &str) -> CatalogResult<()>;

    /// Increment the table's sequence number and return the new value.
    ///
    /// Sequence numbers are used to order operations (inserts and deletes).
    /// This method atomically increments and returns the new sequence.
    async fn increment_sequence_number(&self, table_id: i64) -> CatalogResult<i64>;

    /// Get the current sequence number for a table.
    async fn get_sequence_number(&self, table_id: i64) -> CatalogResult<i64>;

    /// Add a delete file (deletion vector) for a data file.
    ///
    /// Tracks a deletion vector file that marks rows as deleted in a specific
    /// virtual file (`ListingTable`).
    async fn add_delete_file(&self, delete_file: DeleteFile) -> CatalogResult<i64>;

    /// Get all active delete files for a table (across all virtual files).
    async fn get_table_delete_files(&self, table_id: i64) -> CatalogResult<Vec<DeleteFile>>;

    /// Remove delete files (deletion vectors) by ID for a table.
    async fn remove_delete_files(
        &self,
        table_id: i64,
        delete_file_ids: &[i64],
    ) -> CatalogResult<()>;

    /// Clear all delete files for a table.
    ///
    /// This is called after compaction to remove deletion vectors that have been
    /// applied to the data files.
    async fn clear_delete_files(&self, table_id: i64) -> CatalogResult<()>;

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
        table_id: i64,
        pk_bytes: Vec<u8>,
        sequence_number: i64,
    ) -> CatalogResult<()>;

    /// Add multiple insert records in a batch.
    ///
    /// More efficient than calling `add_insert_record` multiple times.
    async fn add_insert_records_batch(
        &self,
        table_id: i64,
        pk_bytes_list: Vec<Vec<u8>>,
        sequence_number: i64,
    ) -> CatalogResult<()>;

    /// Get all insert records for a table.
    ///
    /// Returns a map of PK bytes to their sequence numbers.
    async fn get_insert_records(&self, table_id: i64) -> CatalogResult<HashMap<Box<[u8]>, i64>>;

    /// Clear all insert records for a table.
    ///
    /// Called after compaction when deletions and insert records have been merged.
    async fn clear_insert_records(&self, table_id: i64) -> CatalogResult<()>;

    /// Set the sequence number for a snapshot.
    ///
    /// This records when the snapshot was created relative to deletions.
    /// Used for Iceberg-style sequence ordering: deletions only apply to
    /// snapshots with sequence <= `delete_sequence`.
    async fn set_snapshot_sequence(
        &self,
        table_id: i64,
        snapshot_id: &str,
        sequence_number: i64,
    ) -> CatalogResult<()>;

    /// Get the sequence number for a snapshot.
    ///
    /// Returns `None` if the snapshot has no sequence (created before sequence tracking).
    async fn get_snapshot_sequence(
        &self,
        table_id: i64,
        snapshot_id: &str,
    ) -> CatalogResult<Option<i64>>;

    /// Get all snapshot sequences for a table.
    ///
    /// Returns a map of `snapshot_id` -> `sequence_number` for all snapshots
    /// that have sequence tracking enabled.
    async fn get_all_snapshot_sequences(
        &self,
        table_id: i64,
    ) -> CatalogResult<HashMap<String, i64>>;

    /// Clear the sequence record for a specific snapshot.
    ///
    /// This is used when a protected snapshot is superseded by a newer one.
    /// The old sequence record becomes orphaned and can be cleaned up.
    async fn clear_snapshot_sequence(&self, table_id: i64, snapshot_id: &str) -> CatalogResult<()>;

    /// Atomically update snapshot and clear delete files in a single transaction.
    ///
    /// This ensures ACID compliance during compaction: the snapshot update and
    /// deletion of obsolete delete files happen together or not at all.
    /// This prevents data inconsistency if the operation is interrupted.
    ///
    /// # Atomicity Guarantee
    ///
    /// If this operation fails or is interrupted:
    /// - The old snapshot remains active
    /// - All delete files remain intact
    /// - The system remains in a consistent state
    ///
    /// On success:
    /// - The new snapshot is active
    /// - All delete files for the table are removed (they were applied during compaction)
    async fn commit_compaction(&self, table_id: i64, new_snapshot_id: &str) -> CatalogResult<()>;

    /// Add a partition to a table.
    async fn add_partition(&self, partition: PartitionMetadata) -> CatalogResult<i64>;

    /// Get all partitions for a table.
    async fn get_partitions(&self, table_id: i64) -> CatalogResult<Vec<PartitionMetadata>>;

    /// Shutdown the catalog, performing any necessary cleanup (e.g., WAL checkpoint, optimize).
    /// Default implementation does nothing.
    async fn shutdown(&self) -> CatalogResult<()> {
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
