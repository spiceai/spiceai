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

//! Deletion vector I/O for Cayenne tables.
//!
//! This module handles reading and writing deletion vector files using Arrow IPC format.
//! Deletion vectors are used to mark rows as deleted without rewriting data files.
//!
//! # File Format
//!
//! Deletion vectors are stored as Arrow IPC files with one of two schemas:
//!
//! - **Position-based** (the vector schema for a file-scoped delete):
//!   - `row_id: UInt64` - File-local row position (0-indexed)
//!   - `deleted_at: Int64` - Deletion timestamp (microseconds)
//!
//! - **Key-based** (the vector schema for a table-wide delete by key):
//!   - `row_key: Binary` - Primary key bytes (via Arrow's `RowConverter`)
//!   - `deleted_at: Int64` - Deletion timestamp (microseconds)

use object_store::ObjectStoreExt as _;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock};

use arrow::array::{Array, BinaryArray, Int64Array, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow_ipc::reader::FileReader;
use arrow_ipc::writer::FileWriter;
use arrow_schema::SchemaRef;
use chrono::Utc;
use roaring::RoaringBitmap;
use uuid::Uuid;

use crate::dataset_storage::DatasetStorage;
use crate::metadata::{DeleteFile, DeletionType, TableMetadata};
use crate::provider::utils::bytes_key;
use crate::provider::{Error, Result};

#[derive(Debug, Clone, Copy)]
struct KeyDeletionReadState {
    delete_sequence: i64,
    reinsert_sequence: Option<i64>,
}

/// A key-based deletion-vector whose catalog row exists but whose `.arrow` file is
/// missing on disk. Reported (instead of erroring) by [`read_deletion_vectors`]
/// so the async loader can discriminate a self-healable orphan (the file-first
/// orphaned-DV sweep unlinked the file but a crash interrupted the row removal)
/// from genuine data loss, by comparing `sequence_number` against the
/// surviving-sequence floor. Position-based DVs are never reported here.
#[derive(Debug, Clone)]
pub struct MissingKeyDeletionVector {
    pub delete_file_id: String,
    pub path: String,
    pub sequence_number: i64,
}

/// Directory under the table snapshot where deletion vectors are stored.
pub(crate) const DELETION_DIR_NAME: &str = "deletions";
/// File extension used for deletion-vector files.
const DELETION_FILE_EXTENSION: &str = "arrow";
/// File format recorded in the catalog for deletion vectors.
const DELETION_FILE_FORMAT: &str = "arrow_ipc";

#[cfg(test)]
#[derive(Debug, Default)]
struct TestWriterHook {
    block_writers: std::sync::atomic::AtomicBool,
    fail_writer_number: AtomicUsize,
    writers_started: AtomicUsize,
    writers_completed: AtomicUsize,
}

#[cfg(test)]
struct TestWriterHookGuard(Arc<TestWriterHook>);

#[cfg(test)]
impl TestWriterHookGuard {
    fn new(fail_writer_number: usize, block_writers: bool) -> Self {
        let hook = Arc::new(TestWriterHook::default());
        hook.fail_writer_number
            .store(fail_writer_number, Ordering::Release);
        hook.block_writers.store(block_writers, Ordering::Release);
        Self(hook)
    }

    fn hook(&self) -> Arc<TestWriterHook> {
        Arc::clone(&self.0)
    }
}

#[cfg(test)]
impl Drop for TestWriterHookGuard {
    fn drop(&mut self) {
        self.0.block_writers.store(false, Ordering::Release);
    }
}

/// Identifies rows for deletion using either position-based IDs or primary key-based keys.
///
/// # Deletion Strategies
///
/// - **Position-based (`row_ids`)**: Uses row position within a specific data file.
///   File-local positions (0 to N-1) ensure correct deletion regardless of scan order.
///   Used when no primary key is defined.
///
/// - **Key-based (`row_keys`)**: Uses the byte representation of primary key columns
///   (via Arrow's `RowConverter`). Position-independent and survives data reorganization.
///   Used when a primary key is defined.
#[derive(Debug)]
pub enum DeletionIdentifier {
    /// Position-based row IDs for a specific data file. The file path identifies
    /// which data file these row positions belong to.
    PositionBased {
        file_path: String,
        row_ids: Vec<u64>,
        /// When `true`, `row_ids` is already strictly increasing and the writer
        /// can skip its sort/dedup pass.
        pre_sorted: bool,
    },
    /// Row keys, which apply table-wide rather than to one data file.
    KeyBased(Vec<Box<[u8]>>),
}

impl DeletionIdentifier {
    /// Returns `true` if there are no rows to delete.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        match self {
            Self::PositionBased { row_ids, .. } => row_ids.is_empty(),
            Self::KeyBased(keys) => keys.is_empty(),
        }
    }
}

/// Specification describing a deletion-vector file that should be produced.
///
/// For position-based deletions, the file path is embedded in the `DeletionIdentifier`.
/// For key-based deletions, the deletion applies to the entire table.
#[derive(Debug)]
pub struct DeletionVectorWriteSpec {
    /// Row identifiers (position-based with file path, or key-based)
    pub identifiers: DeletionIdentifier,
    /// The sequence the file's deletes carry in the catalog, when it differs
    /// from the writer's table metadata (see [`Self::with_sequence_number`]).
    sequence_number: Option<i64>,
    /// Each key's own delete sequence, index-aligned with a key-based spec's keys,
    /// for a file that holds several sequences (see
    /// [`Self::new_key_based_with_sequences`]).
    row_sequences: Option<Vec<i64>>,
}

impl DeletionVectorWriteSpec {
    /// Create a new specification with position-based row IDs for a specific data file.
    ///
    /// The row IDs should be file-local positions (0 to N-1 within the specified file).
    /// The writer sorts and deduplicates them; use [`Self::new_position_based_sorted`]
    /// instead when the caller can guarantee monotone-unique input to skip the redundant
    /// O(N log N) pass.
    ///
    /// Currently only the test suite exercises this constructor; production
    /// code uses the `_sorted` variant.
    #[cfg(test)]
    #[must_use]
    pub fn new_position_based(file_path: String, row_ids: Vec<u64>) -> Self {
        Self {
            identifiers: DeletionIdentifier::PositionBased {
                file_path,
                row_ids,
                pre_sorted: false,
            },
            sequence_number: None,
            row_sequences: None,
        }
    }

    /// Same as [`Self::new_position_based`] but the caller guarantees `row_ids` is
    /// strictly increasing (sorted + deduplicated). The writer skips the redundant
    /// sort/dedup pass. See `position_delete_redundant_walks` bench.
    #[must_use]
    pub fn new_position_based_sorted(file_path: String, row_ids: Vec<u64>) -> Self {
        Self {
            identifiers: DeletionIdentifier::PositionBased {
                file_path,
                row_ids,
                pre_sorted: true,
            },
            sequence_number: None,
            row_sequences: None,
        }
    }

    /// Create a new specification with key-based row keys.
    #[must_use]
    pub fn new_key_based(row_keys: Vec<Box<[u8]>>) -> Self {
        Self {
            identifiers: DeletionIdentifier::KeyBased(row_keys),
            sequence_number: None,
            row_sequences: None,
        }
    }

    /// Create a key-based specification whose keys carry delete sequences of their
    /// own, so the deletes of many sequences go into one file. The file is recorded
    /// at the highest of them, and a reader takes each key's own sequence from the
    /// file rather than the recorded one. A key listed more than once keeps its
    /// highest sequence.
    #[must_use]
    pub fn new_key_based_with_sequences(keys: Vec<(Box<[u8]>, i64)>) -> Self {
        let max_sequence = keys.iter().map(|(_, sequence)| *sequence).max();
        let (row_keys, row_sequences): (Vec<Box<[u8]>>, Vec<i64>) = keys.into_iter().unzip();
        Self {
            identifiers: DeletionIdentifier::KeyBased(row_keys),
            sequence_number: max_sequence,
            row_sequences: Some(row_sequences),
        }
    }

    /// Record this file's deletes at `sequence_number` instead of the writer's
    /// table sequence, so one [`DeletionVectorWriter::write`] can write files for
    /// several delete sequences together.
    #[must_use]
    pub fn with_sequence_number(mut self, sequence_number: i64) -> Self {
        self.sequence_number = Some(sequence_number);
        self
    }

    /// Returns `true` if there are no row IDs to write.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.identifiers.is_empty()
    }
}

/// Result of writing a deletion-vector file.
#[derive(Debug)]
pub struct DeletionVectorWriteResult {
    /// Metadata entry that should be registered with the catalog.
    pub delete_file: DeleteFile,
    /// The deletion identifiers that were written (position-based or key-based).
    pub identifiers: DeletionIdentifier,
}

// ============================================================================
// Writer
// ============================================================================

/// Most deletion-vector files [`DeletionVectorWriter::write`] has in flight at
/// once, each holding a blocking-pool thread through its write and file sync.
const DELETION_VECTOR_WRITE_CONCURRENCY: usize = 64;

/// One written deletion-vector file: its path, delete count, size, the
/// identifiers it holds, the data file a position-based delete targets, and the
/// sequence its spec asked for.
type WrittenDeletionFile = (
    PathBuf,
    usize,
    u64,
    DeletionIdentifier,
    Option<String>,
    Option<i64>,
);

/// A store and an object path in it.
type StoreLocation = (Arc<dyn object_store::ObjectStore>, object_store::path::Path);

/// The dataset store and object path of a deletion-vector file addressed by
/// `path`; `None` for a table with no store of its own (its data path is a local
/// directory), whose vectors are read and written as files.
///
/// # Errors
///
/// A URL outside `storage` is an error: no store of this dataset holds it.
fn deletion_vector_location(
    storage: Option<&DatasetStorage>,
    path: &str,
) -> std::result::Result<Option<StoreLocation>, object_store::Error> {
    match storage {
        Some(storage) => Ok(storage
            .resolve(path)?
            .map(|location| (Arc::clone(storage.store()), location))),
        None => Ok(None),
    }
}

/// Writes deletion-vector files for a specific Cayenne table snapshot.
#[derive(Debug)]
pub struct DeletionVectorWriter<'a> {
    table: &'a TableMetadata,
    /// The table's storage when its data path is an object-store URL; the
    /// vectors are then written through its store.
    storage: Option<Arc<DatasetStorage>>,
    #[cfg(test)]
    test_hook: Option<Arc<TestWriterHook>>,
}

struct PendingBatchWrites {
    remaining: AtomicUsize,
    completed: tokio::sync::Notify,
}

struct PendingWriteCompletion(Arc<PendingBatchWrites>);

impl Drop for PendingWriteCompletion {
    fn drop(&mut self) {
        if self.0.remaining.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.0.completed.notify_waiters();
        }
    }
}

struct UncommittedDeleteFilesGuard {
    paths: Vec<PathBuf>,
    storage: Option<Arc<DatasetStorage>>,
    pending: Arc<PendingBatchWrites>,
    armed: bool,
}

impl UncommittedDeleteFilesGuard {
    fn new(paths: Vec<PathBuf>, storage: Option<Arc<DatasetStorage>>) -> Self {
        Self {
            storage,
            pending: Arc::new(PendingBatchWrites {
                remaining: AtomicUsize::new(paths.len()),
                completed: tokio::sync::Notify::new(),
            }),
            paths,
            armed: true,
        }
    }

    fn completion(&self) -> PendingWriteCompletion {
        PendingWriteCompletion(Arc::clone(&self.pending))
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for UncommittedDeleteFilesGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let paths = std::mem::take(&mut self.paths);
        let storage = self.storage.take();
        let pending = Arc::clone(&self.pending);
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                while pending.remaining.load(Ordering::Acquire) != 0 {
                    let notified = pending.completed.notified();
                    if pending.remaining.load(Ordering::Acquire) != 0 {
                        notified.await;
                    }
                }
                cleanup_uncommitted_delete_paths(&paths, storage.as_deref()).await;
            });
        } else {
            std::thread::spawn(move || {
                while pending.remaining.load(Ordering::Acquire) != 0 {
                    std::thread::yield_now();
                }
                if let Some(record) = storage.as_deref().and_then(DatasetStorage::writer_record) {
                    record.release_deletion_files(
                        paths.iter().filter_map(|path| deletion_file_name(path)),
                    );
                }
                for path in paths {
                    // Object-store files are removed through the store, never as
                    // a local path; without a runtime they are left for the sweep.
                    if crate::is_object_store_path(&path.to_string_lossy()) {
                        continue;
                    }
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
    }
}

impl<'a> DeletionVectorWriter<'a> {
    /// Create a new writer bound to the provided table metadata, writing through
    /// `storage` when the table's data path is an object-store URL.
    #[must_use]
    pub fn new(table: &'a TableMetadata, storage: Option<&Arc<DatasetStorage>>) -> Self {
        Self {
            table,
            storage: storage.map(Arc::clone),
            #[cfg(test)]
            test_hook: None,
        }
    }

    #[cfg(test)]
    fn new_with_test_hook(table: &'a TableMetadata, test_hook: Arc<TestWriterHook>) -> Self {
        Self {
            table,
            storage: None,
            test_hook: Some(test_hook),
        }
    }

    /// Write deletion-vector files for the supplied specifications.
    ///
    /// Callers are responsible for registering the returned [`DeleteFile`] metadata
    /// with the catalog. Empty specifications are skipped automatically.
    ///
    /// # Errors
    ///
    /// Returns an error if row IDs are negative, if Arrow record batches cannot be
    /// constructed, or if any filesystem/IO operations fail.
    pub async fn write(
        &self,
        specs: Vec<DeletionVectorWriteSpec>,
    ) -> Result<Vec<DeletionVectorWriteResult>> {
        let specs: Vec<DeletionVectorWriteSpec> =
            specs.into_iter().filter(|spec| !spec.is_empty()).collect();
        if specs.is_empty() {
            return Ok(Vec::new());
        }

        let deletion_dir = self.table_snapshot_deletion_dir();
        let snapshot_dir = deletion_dir
            .parent()
            .map(Path::to_path_buf)
            .ok_or_else(|| Error::Internal {
                table: self.table.path.clone(),
                message: format!(
                    "Deletion vector directory '{}' has no snapshot parent",
                    deletion_dir.display()
                ),
            })?;

        // Ensure the deletions/ subdirectory exists (once per call — it was
        // previously re-checked per spec, a no-op after the first).
        // If we just created it, sync its parent (the snapshot directory)
        // so the subdir entry is durable on local FS.
        //
        // This is required for the same contract we now enforce for
        // snapshot directories themselves (ensure_snapshot_dir_exists)
        // and for the _partitioned_wal/ coordination directory:
        // on POSIX, mkdir in a directory updates the parent's metadata.
        // A crash immediately after this create_dir_all but before the
        // subsequent file write + file fsync + catalog record could
        // otherwise leave a catalog entry pointing at a deletions/
        // directory whose creation was lost.
        //
        // The sync is one-time per snapshot (first deletion vector
        // written to it). Subsequent deletions reuse the directory.
        let object_store_table = self.storage.is_some();
        let sync_snapshot_parent = if object_store_table {
            false
        } else {
            match tokio::fs::create_dir(&deletion_dir).await {
                Ok(()) => true,
                Err(source) if source.kind() == std::io::ErrorKind::AlreadyExists => false,
                Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
                    tokio::fs::create_dir_all(&deletion_dir).await?;
                    true
                }
                Err(source) => return Err(Error::IoError { source }),
            }
        };
        if sync_snapshot_parent {
            let table = self.table.path.clone();
            // Directory ordering tier (plain fsync on macOS, full fsync
            // on other platforms) — see `provider/fsync_tier.rs`.
            tokio::task::spawn_blocking(move || {
                let dir = std::fs::File::open(&snapshot_dir)?;
                crate::provider::fsync_tier::ordering_sync_dir_std(&dir)
            })
            .await
            .map_err(|source| Error::TaskPanicked { table, source })??;
        }

        // Write every spec's deletion-vector file CONCURRENTLY, at most
        // `DELETION_VECTOR_WRITE_CONCURRENCY` at a time. The files are
        // independent (one per spec, UUIDv7 filenames), so their writes + file
        // fsyncs overlap on the blocking pool — on network-attached storage
        // (EBS) this turns N serialized ~1 ms fsync round-trips into ~1 round
        // of in-flight barriers — while a checkpoint carrying thousands of
        // delete sequences cannot occupy the whole pool. `buffered` preserves
        // spec order in the returned results. Every future, and with it its
        // completion token, exists before the first is polled, so the cleanup
        // guard's count covers specs that never start. The cleanup guard owns
        // every generated path until this method returns a complete result. If
        // this future is cancelled, it waits for detached blocking writers
        // before unlinking their paths.
        let batch_paths = (0..specs.len())
            .map(|_| Self::deletion_file_path(&deletion_dir))
            .collect::<Vec<_>>();
        // A dataset in the object-native location holds the files in its writer
        // record before any is uploaded, so the orphan sweep never takes one
        // whose write or commit is still in flight.
        if let Some(record) = self
            .storage
            .as_deref()
            .and_then(DatasetStorage::writer_record)
        {
            record.hold_deletion_files(
                batch_paths
                    .iter()
                    .filter_map(|path| deletion_file_name(path)),
            );
        }
        let mut cleanup_guard =
            UncommittedDeleteFilesGuard::new(batch_paths.clone(), self.storage.clone());
        let write_futures = specs
            .into_iter()
            .zip(batch_paths.iter().cloned())
            .map(|(spec, file_path)| {
                let mut completion = Some(cleanup_guard.completion());
                let sequence_number = spec.sequence_number;
                let row_sequences = spec.row_sequences;
                let table_name = self.table.table_name.clone();
                let storage = self.storage.clone();
                #[cfg(test)]
                let test_hook = self.test_hook.as_ref().map(Arc::clone);
                async move {
                    let (batch, schema, count, identifiers, source_data_file_path) = match spec
                        .identifiers
                    {
                        DeletionIdentifier::PositionBased {
                            file_path: source_file,
                            mut row_ids,
                            pre_sorted,
                        } => {
                            if pre_sorted {
                                debug_assert!(
                                    row_ids.windows(2).all(|w| w[0] < w[1]),
                                    "pre_sorted=true but row_ids is not strictly increasing",
                                );
                            } else {
                                row_ids.sort_unstable();
                                row_ids.dedup();
                            }
                            let count = row_ids.len();
                            let schema = position_based_deletion_schema();
                            let batch = build_position_based_batch(&schema, &row_ids)?;
                            (
                                batch,
                                schema,
                                count,
                                DeletionIdentifier::PositionBased {
                                    file_path: source_file.clone(),
                                    row_ids,
                                    pre_sorted,
                                },
                                Some(source_file),
                            )
                        }
                        DeletionIdentifier::KeyBased(row_keys) => {
                            let (row_keys, schema, batch) = match row_sequences {
                                None => {
                                    // Sort and deduplicate keys
                                    let mut row_keys = row_keys;
                                    row_keys.sort();
                                    row_keys.dedup();
                                    let schema = key_based_deletion_schema();
                                    let batch = build_key_based_batch(&schema, &row_keys)?;
                                    (row_keys, schema, batch)
                                }
                                Some(row_sequences) => {
                                    let keyed =
                                        sorted_keys_with_max_sequence(row_keys, row_sequences)?;
                                    let schema = sequenced_key_based_deletion_schema();
                                    let batch = build_sequenced_key_based_batch(&schema, &keyed)?;
                                    let row_keys = keyed.into_iter().map(|(key, _)| key).collect();
                                    (row_keys, schema, batch)
                                }
                            };
                            let count = row_keys.len();
                            (
                                batch,
                                schema,
                                count,
                                DeletionIdentifier::KeyBased(row_keys),
                                None,
                            )
                        }
                    };

                    // From this point the completion token is transferred into the
                    // detached blocking writer. Any earlier `?` drops it here and
                    // decrements the pending count, so cancellation cleanup cannot
                    // wait forever for a writer that was never spawned.

                    #[cfg(test)]
                    let file_size_bytes = write_deletion_file(
                        &file_path,
                        storage.as_deref(),
                        Arc::clone(&schema),
                        batch,
                        &table_name,
                        completion.take(),
                        test_hook,
                    )
                    .await?;
                    #[cfg(not(test))]
                    let file_size_bytes = write_deletion_file(
                        &file_path,
                        storage.as_deref(),
                        Arc::clone(&schema),
                        batch,
                        &table_name,
                        completion.take(),
                    )
                    .await?;

                    Ok::<_, Error>((
                        file_path,
                        count,
                        file_size_bytes,
                        identifiers,
                        source_data_file_path,
                        sequence_number,
                    ))
                }
            })
            .collect::<Vec<_>>();
        let written: Vec<WrittenDeletionFile> =
            match futures::StreamExt::collect::<Vec<_>>(futures::StreamExt::buffered(
                futures::stream::iter(write_futures),
                DELETION_VECTOR_WRITE_CONCURRENCY,
            ))
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()
            {
                Ok(written) => written,
                Err(error) => {
                    // Every write future owns a distinct UUID path. The stream
                    // runs all of them to completion, so after one fails no sibling
                    // write is still racing this cleanup. Remove every final-path artifact
                    // from the failed logical batch before returning; otherwise a
                    // later failure or task cancellation at a caller could leak
                    // unreferenced deletion vectors indefinitely.
                    cleanup_uncommitted_delete_paths(&batch_paths, self.storage.as_deref()).await;
                    cleanup_guard.disarm();
                    return Err(error);
                }
            };

        // ONE coalesced deletions/-dir sync for the whole batch of files
        // (replaces the previous per-file parent-dir fsync): a directory fsync
        // flushes ALL of its pending entries, so syncing once after every file
        // exists provides the same dirent durability at 1/N the barrier count.
        // Must complete before the catalog records any of the paths.
        if !object_store_table {
            let table = self.table.path.clone();
            let deletion_dir_for_sync = deletion_dir.clone();
            tokio::task::spawn_blocking(move || {
                let dir = std::fs::File::open(&deletion_dir_for_sync)?;
                crate::provider::fsync_tier::ordering_sync_dir_std(&dir)
            })
            .await
            .map_err(|source| Error::TaskPanicked { table, source })??;
        }

        let mut results = Vec::with_capacity(written.len());
        for (
            file_path,
            count,
            file_size_bytes,
            identifiers,
            source_data_file_path,
            sequence_number,
        ) in written
        {
            let deletion_type = match &identifiers {
                DeletionIdentifier::PositionBased { .. } => DeletionType::PositionBased,
                DeletionIdentifier::KeyBased(_) => DeletionType::KeyBased,
            };

            let mut delete_file = build_delete_file(
                self.table,
                &file_path,
                count,
                file_size_bytes,
                deletion_type,
                source_data_file_path,
            )?;
            if let Some(sequence_number) = sequence_number {
                delete_file.sequence_number = sequence_number;
            }

            results.push(DeletionVectorWriteResult {
                delete_file,
                identifiers,
            });
        }

        cleanup_guard.disarm();
        Ok(results)
    }

    fn table_snapshot_deletion_dir(&self) -> PathBuf {
        let base = Path::new(&self.table.path);
        let snapshot_path = base.join(&self.table.current_snapshot_id);

        snapshot_path.join(DELETION_DIR_NAME)
    }

    fn deletion_file_path(deletion_dir: &Path) -> PathBuf {
        let file_name = format!("delete_{}.{}", Uuid::now_v7(), DELETION_FILE_EXTENSION);
        deletion_dir.join(file_name)
    }
}

// ============================================================================
// Reader
// ============================================================================

/// What a deletion-vector read returns: deleted positions per data file, deleted
/// keys with their delete sequence, re-inserted keys with theirs, and the key
/// vectors whose file is missing.
pub type DeletionVectorReadResult = (
    HashMap<String, RoaringBitmap>,
    HashMap<Box<[u8]>, i64>,
    HashMap<Box<[u8]>, i64>,
    Vec<MissingKeyDeletionVector>,
);

trait ReadSeek: std::io::Read + std::io::Seek + Send {}
impl<T: std::io::Read + std::io::Seek + Send> ReadSeek for T {}

/// Most deletion-vector objects [`read_deletion_vectors`] fetches at once from an
/// object store. A GET's cost there is its round trip, not its bytes, so opening a
/// table with tens of thousands of vectors takes about `count / this` round trips
/// rather than `count`. It also bounds memory: at most twice this many fetched
/// objects wait to be decoded at any time.
const DELETION_VECTOR_READ_CONCURRENCY_REMOTE: usize = 64;

/// [`DELETION_VECTOR_READ_CONCURRENCY_REMOTE`] for a dataset store on local disk,
/// where a read costs a page-cache copy rather than a round trip.
const DELETION_VECTOR_READ_CONCURRENCY_LOCAL: usize = 8;

/// The deletion-vector state read so far, merged file by file.
#[derive(Default)]
struct DeletionVectorReadState {
    per_file_row_ids: HashMap<String, RoaringBitmap>,
    // Key-based DV rows whose `.arrow` file is missing — reported, not errored, so
    // the async loader can self-heal provable orphans (see `MissingKeyDeletionVector`).
    missing_key_dvs: Vec<MissingKeyDeletionVector>,
    // Metadata-only publish: keep delete and reinsert sequence state in one map
    // while reading key-based vectors. This avoids cloning every key in the hot
    // read loop just to update a second map; the legacy return shape is derived
    // once after all files are scanned.
    key_row_state: HashMap<Box<[u8]>, KeyDeletionReadState>,
}

impl DeletionVectorReadState {
    /// Merge one vector, given its catalog row and the result of opening its file.
    fn add(
        &mut self,
        delete_file: &DeleteFile,
        opened: std::io::Result<Box<dyn ReadSeek>>,
    ) -> datafusion_common::Result<()> {
        let path = std::path::Path::new(&delete_file.path);
        let file = match opened {
            Ok(file) => file,
            // A missing KEY-based DV file is tolerated and reported: the file-first
            // orphaned-DV sweep of a dataset outside the object-native location
            // unlinks the file before removing its catalog row, so
            // a crash in that window leaves a discoverable dangling row. The async
            // loader decides self-heal vs error against the floor. Position-based DVs
            // (source_data_file_path = Some) are tied to a live data file, so a
            // missing one is still a hard error.
            Err(e)
                if e.kind() == std::io::ErrorKind::NotFound
                    && delete_file.source_data_file_path.is_none() =>
            {
                self.missing_key_dvs.push(MissingKeyDeletionVector {
                    delete_file_id: delete_file.delete_file_id.clone(),
                    path: delete_file.path.clone(),
                    sequence_number: delete_file.sequence_number,
                });
                return Ok(());
            }
            Err(e) => {
                return Err(datafusion_common::DataFusionError::Execution(format!(
                    "Failed to open deletion vector file {}: {e}",
                    path.display()
                )));
            }
        };

        let reader = FileReader::try_new(file, None).map_err(|e| {
            datafusion_common::DataFusionError::Execution(format!(
                "Failed to read deletion vector file {}: {e}",
                path.display()
            ))
        })?;

        // Detect type from schema: first column name determines type
        // "row_id" (UInt64) = position-based, "row_key" (Binary) = key-based
        let schema = reader.schema();
        let first_field = schema.field(0);
        let is_key_based = matches!(first_field.data_type(), DataType::Binary);
        // A key vector that holds several delete sequences carries each row's own
        // (see `KEY_DELETE_SEQUENCE_COLUMN`); otherwise every row has the file's.
        let row_sequence_column = if is_key_based {
            schema.index_of(KEY_DELETE_SEQUENCE_COLUMN).ok()
        } else {
            None
        };

        // Get the sequence number for this delete file (for sequence-based ordering)
        let file_sequence = delete_file.sequence_number;
        // Metadata-only publish: the per-commit reinsert sequence carried on this
        // file's row (None for legacy rows / pure deletes — those fall back to the
        // `cayenne_insert_record` table at the load site).
        let file_reinsert = delete_file.reinsert_sequence;

        for batch_result in reader {
            let batch = batch_result.map_err(|e| {
                datafusion_common::DataFusionError::Execution(format!(
                    "Failed to read batch from deletion vector: {e}"
                ))
            })?;

            if is_key_based {
                // Key-based: extract Binary row_key column
                let row_key_array = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .ok_or_else(|| {
                        datafusion_common::DataFusionError::Execution(
                            "Expected BinaryArray for row_key column".to_string(),
                        )
                    })?;
                let row_sequences = match row_sequence_column {
                    Some(index) => Some(
                        batch
                            .column(index)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .ok_or_else(|| {
                                datafusion_common::DataFusionError::Execution(format!(
                                    "Deletion vector file {} has a non-Int64 {KEY_DELETE_SEQUENCE_COLUMN} column",
                                    path.display()
                                ))
                            })?,
                    ),
                    None => None,
                };

                for i in 0..row_key_array.len() {
                    if !row_key_array.is_null(i) {
                        let row_sequence = match row_sequences {
                            Some(sequences) if sequences.is_valid(i) => sequences.value(i),
                            Some(_) => {
                                return Err(datafusion_common::DataFusionError::Execution(
                                    format!(
                                        "Deletion vector file {} has a key with no {KEY_DELETE_SEQUENCE_COLUMN}",
                                        path.display()
                                    ),
                                ));
                            }
                            None => file_sequence,
                        };
                        let key = bytes_key(row_key_array.value(i));
                        let entry = self
                            .key_row_state
                            .entry(key)
                            .or_insert(KeyDeletionReadState {
                                delete_sequence: row_sequence,
                                reinsert_sequence: file_reinsert,
                            });
                        entry.delete_sequence = entry.delete_sequence.max(row_sequence);
                        if let Some(reinsert) = file_reinsert {
                            entry.reinsert_sequence = Some(
                                entry
                                    .reinsert_sequence
                                    .map_or(reinsert, |seq| seq.max(reinsert)),
                            );
                        }
                    }
                }
            } else {
                // Position-based: extract UInt64 row_id column
                // Use source_data_file_path to group deletions by their originating data file
                let source_file = delete_file.source_data_file_path.clone().unwrap_or_else(|| {
                    tracing::warn!(
                        "Position-based deletion vector at {:?} has no source_data_file_path - using empty key",
                        path
                    );
                    String::new()
                });

                let row_id_array = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .ok_or_else(|| {
                        datafusion_common::DataFusionError::Execution(
                            "Expected UInt64Array for row_id column".to_string(),
                        )
                    })?;

                // Bulk insert row IDs - schema guarantees no nulls (nullable: false)
                let bitmap = self.per_file_row_ids.entry(source_file).or_default();
                let values = row_id_array.values();
                for &row_id in values {
                    let row_id_u32 = u32::try_from(row_id).map_err(|_| {
                        datafusion_common::DataFusionError::Execution(format!(
                            "Position deletion vector {} contains row ID {row_id}, which exceeds the supported maximum {}. Compact the table into files with at most {} rows before using position-based deletion.",
                            path.display(),
                            u32::MAX,
                            u32::MAX
                        ))
                    })?;
                    bitmap.insert(row_id_u32);
                }
            }
        }
        Ok(())
    }

    fn finish(self, file_count: usize) -> DeletionVectorReadResult {
        let mut deleted_row_keys: HashMap<Box<[u8]>, i64> =
            HashMap::with_capacity(self.key_row_state.len());
        let mut reinserted_row_keys: HashMap<Box<[u8]>, i64> = HashMap::new();
        for (key, state) in self.key_row_state {
            if let Some(reinsert_sequence) = state.reinsert_sequence {
                reinserted_row_keys.insert(key.clone(), reinsert_sequence);
            }
            deleted_row_keys.insert(key, state.delete_sequence);
        }

        let total_position_based: u64 =
            self.per_file_row_ids.values().map(RoaringBitmap::len).sum();
        tracing::debug!(
            "Loaded {} position-based deletions across {} files + {} key-based deleted rows from {} deletion vector files",
            total_position_based,
            self.per_file_row_ids.len(),
            deleted_row_keys.len(),
            file_count
        );

        (
            self.per_file_row_ids,
            deleted_row_keys,
            reinserted_row_keys,
            self.missing_key_dvs,
        )
    }
}

/// Opens a local deletion-vector file. Blocking.
fn open_local_deletion_vector(path: &str) -> std::io::Result<Box<dyn ReadSeek>> {
    std::fs::File::open(path).map(|f| Box::new(f) as Box<dyn ReadSeek>)
}

/// Read the deletion-vector files of a table whose vectors are local files (it has no
/// dataset store), one at a time, detecting whether each file is position-based or
/// key-based from its schema. [`read_deletion_vectors`] is the form the table calls.
///
/// For position-based deletions, returns a map of source data file path to the `RoaringBitmap`
/// of file-local row positions. This enables correct deletion filtering regardless of file
/// scan order.
///
/// # Blocking I/O Warning
///
/// This function performs **blocking file system I/O** and must be called from within
/// `tokio::task::spawn_blocking`.
///
/// # Returns
///
/// A tuple of `(per_file_row_ids, key_based_row_keys_with_sequence, reinserted_row_keys, missing)`.
/// - `per_file_row_ids`: Map of source data file path -> `RoaringBitmap` of deleted row positions
/// - `key_based_row_keys_with_sequence`: Map of PK bytes -> max delete sequence number
/// - `reinserted_row_keys`: Map of PK bytes -> max reinsert sequence number derived from delete-file metadata
/// - `missing`: key vectors whose file is missing
///
/// # Errors
///
/// Returns an error if any deletion vector file cannot be read or parsed.
fn detect_deletion_type_and_read(
    delete_files: Vec<DeleteFile>,
) -> datafusion_common::Result<DeletionVectorReadResult> {
    let file_count = delete_files.len();
    let mut state = DeletionVectorReadState::default();
    for delete_file in delete_files {
        let opened = open_local_deletion_vector(&delete_file.path);
        state.add(&delete_file, opened)?;
    }
    Ok(state.finish(file_count))
}

/// A deletion-vector object fetched from its dataset store, or a vector that lives
/// outside the store and is opened as a local file by the decoding thread.
enum FetchedDeletionVector {
    Bytes(std::io::Result<bytes::Bytes>),
    Local,
    /// A vector the committed state of a dataset in the object-native location
    /// names but its store does not hold.
    Lost(crate::dataset_storage::DataFileError),
}

async fn fetch_deletion_vector(storage: &DatasetStorage, path: &str) -> FetchedDeletionVector {
    match deletion_vector_location(Some(storage), path) {
        Ok(Some((store, location))) => {
            FetchedDeletionVector::Bytes(match store.get(&location).await {
                Ok(result) => result.bytes().await.map_err(std::io::Error::other),
                // A dataset in the object-native location records a deletion
                // vector's removal before it deletes the object, so one its
                // committed state still names is never an interrupted sweep: it is
                // lost, and reading on without it would bring back rows the
                // dataset deleted.
                Err(object_store::Error::NotFound { .. }) if storage.in_location() => {
                    return FetchedDeletionVector::Lost(
                        storage.missing(crate::dataset_storage::DataFileKind::Deletion, &location),
                    );
                }
                Err(object_store::Error::NotFound { .. }) => {
                    Err(std::io::Error::from(std::io::ErrorKind::NotFound))
                }
                Err(error) => Err(std::io::Error::other(error)),
            })
        }
        Ok(None) => FetchedDeletionVector::Local,
        Err(error) => FetchedDeletionVector::Bytes(Err(std::io::Error::other(error))),
    }
}

/// Aborts a spawned task when dropped, so a cancelled read stops fetching.
struct AbortOnDrop(tokio::task::AbortHandle);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Read a table's deletion vectors without blocking an async thread: the files of a
/// table with no dataset store one by one on a blocking thread
/// ([`detect_deletion_type_and_read`]), and the objects of a table whose vectors are
/// in its store concurrently.
///
/// A task on the async runtime fetches the objects, at most
/// [`DELETION_VECTOR_READ_CONCURRENCY_REMOTE`] at once (or
/// [`DELETION_VECTOR_READ_CONCURRENCY_LOCAL`] for a store on local disk), and hands
/// them in catalog order over a bounded channel to one blocking thread that decodes
/// and merges them, so neither the fetches nor the decoding hold an async thread.
/// The result is the same as reading the files one by one: the merge takes the
/// maximum sequence per key, whatever the order. Every vector the catalog names is
/// either merged, reported missing (a key vector, as the sequential read does), or
/// the read fails; a read that stops early is an error, never fewer deletes.
///
/// # Errors
///
/// Returns an error if any deletion vector cannot be fetched, read or parsed.
pub async fn read_deletion_vectors(
    delete_files: Vec<DeleteFile>,
    storage: Option<Arc<DatasetStorage>>,
) -> datafusion_common::Result<DeletionVectorReadResult> {
    let join_error = |error: tokio::task::JoinError| {
        datafusion_common::DataFusionError::Execution(format!(
            "Deletion vector reader task failed: {error}"
        ))
    };
    let Some(storage) = storage else {
        return tokio::task::spawn_blocking(move || detect_deletion_type_and_read(delete_files))
            .await
            .map_err(join_error)?;
    };
    let file_count = delete_files.len();
    if file_count == 0 {
        return Ok(DeletionVectorReadState::default().finish(0));
    }
    let concurrency = if storage.is_local() {
        DELETION_VECTOR_READ_CONCURRENCY_LOCAL
    } else {
        DELETION_VECTOR_READ_CONCURRENCY_REMOTE
    };

    let (sender, mut receiver) =
        tokio::sync::mpsc::channel::<(DeleteFile, FetchedDeletionVector)>(concurrency);
    let fetcher = tokio::spawn(async move {
        use futures::StreamExt as _;
        let mut fetched = futures::stream::iter(delete_files)
            .map(|delete_file| {
                let storage = Arc::clone(&storage);
                async move {
                    let fetched = fetch_deletion_vector(&storage, &delete_file.path).await;
                    (delete_file, fetched)
                }
            })
            .buffered(concurrency);
        while let Some(item) = fetched.next().await {
            if sender.send(item).await.is_err() {
                // The decoder stopped (an error); nothing reads the rest.
                break;
            }
        }
    });
    let _fetcher = AbortOnDrop(fetcher.abort_handle());

    tokio::task::spawn_blocking(move || {
        let mut state = DeletionVectorReadState::default();
        let mut merged_files = 0_usize;
        while let Some((delete_file, fetched)) = receiver.blocking_recv() {
            merged_files += 1;
            let opened = match fetched {
                FetchedDeletionVector::Bytes(bytes) => {
                    bytes.map(|bytes| Box::new(std::io::Cursor::new(bytes)) as Box<dyn ReadSeek>)
                }
                FetchedDeletionVector::Local => open_local_deletion_vector(&delete_file.path),
                FetchedDeletionVector::Lost(error) => {
                    return Err(datafusion_common::DataFusionError::External(Box::new(
                        error,
                    )));
                }
            };
            state.add(&delete_file, opened)?;
        }
        if merged_files != file_count {
            return Err(datafusion_common::DataFusionError::Execution(format!(
                "Deletion vector reader stopped after {merged_files} of {file_count} files"
            )));
        }
        Ok(state.finish(file_count))
    })
    .await
    .map_err(join_error)?
}

// ============================================================================
// Helpers
// ============================================================================

/// Build a deletion batch for position-based row IDs.
fn build_position_based_batch(schema: &SchemaRef, row_ids: &[u64]) -> Result<RecordBatch> {
    let deleted_at = Utc::now().timestamp_micros();

    // `from_iter_values` builds directly from the slice; `from(Vec)` would
    // require an extra `to_vec()` allocation we never need to keep.
    let row_id_array = UInt64Array::from_iter_values(row_ids.iter().copied());
    let deleted_at_array = Int64Array::from(vec![deleted_at; row_ids.len()]);

    Ok(RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(row_id_array) as Arc<dyn Array>,
            Arc::new(deleted_at_array),
        ],
    )?)
}

/// Build a deletion batch for key-based row keys (primary key bytes).
fn build_key_based_batch(schema: &SchemaRef, row_keys: &[Box<[u8]>]) -> Result<RecordBatch> {
    let deleted_at = Utc::now().timestamp_micros();

    // Convert Box<[u8]> to &[u8] for BinaryArray
    let key_refs: Vec<&[u8]> = row_keys.iter().map(AsRef::as_ref).collect();
    let row_key_array = BinaryArray::from(key_refs);
    let deleted_at_array = Int64Array::from(vec![deleted_at; row_keys.len()]);

    Ok(RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(row_key_array) as Arc<dyn Array>,
            Arc::new(deleted_at_array),
        ],
    )?)
}

/// `row_keys` paired with `row_sequences`, sorted by key, each key once at its
/// highest sequence.
fn sorted_keys_with_max_sequence(
    row_keys: Vec<Box<[u8]>>,
    row_sequences: Vec<i64>,
) -> Result<Vec<(Box<[u8]>, i64)>> {
    if row_keys.len() != row_sequences.len() {
        return Err(Error::Internal {
            table: String::new(),
            message: format!(
                "A key deletion vector has {} keys but {} delete sequences",
                row_keys.len(),
                row_sequences.len()
            ),
        });
    }
    let mut keyed: Vec<(Box<[u8]>, i64)> = row_keys.into_iter().zip(row_sequences).collect();
    // Highest sequence first within a key, so `dedup_by` keeps it.
    keyed.sort_unstable_by(|(a_key, a_seq), (b_key, b_seq)| {
        a_key.cmp(b_key).then_with(|| b_seq.cmp(a_seq))
    });
    keyed.dedup_by(|later, earlier| later.0 == earlier.0);
    Ok(keyed)
}

/// Build a key deletion batch whose rows carry their own delete sequence.
fn build_sequenced_key_based_batch(
    schema: &SchemaRef,
    keyed: &[(Box<[u8]>, i64)],
) -> Result<RecordBatch> {
    let deleted_at = Utc::now().timestamp_micros();
    let row_key_array = BinaryArray::from_iter_values(keyed.iter().map(|(key, _)| key.as_ref()));
    let deleted_at_array = Int64Array::from(vec![deleted_at; keyed.len()]);
    let sequence_array = Int64Array::from_iter_values(keyed.iter().map(|(_, sequence)| *sequence));
    Ok(RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(row_key_array) as Arc<dyn Array>,
            Arc::new(deleted_at_array),
            Arc::new(sequence_array),
        ],
    )?)
}

async fn write_deletion_file(
    file_path: &Path,
    storage: Option<&DatasetStorage>,
    schema: SchemaRef,
    batch: RecordBatch,
    table_name: &str,
    completion: Option<PendingWriteCompletion>,
    #[cfg(test)] test_hook: Option<Arc<TestWriterHook>>,
) -> Result<u64> {
    let output_path = file_path.to_path_buf();
    let table_name = table_name.to_string();

    let location =
        deletion_vector_location(storage, &output_path.to_string_lossy()).map_err(|source| {
            Error::IoError {
                source: std::io::Error::other(source),
            }
        })?;
    if let Some((store, location)) = location {
        let _completion = completion;
        let mut writer = FileWriter::try_new(Vec::new(), &schema)?;
        writer.write(&batch)?;
        writer.finish()?;
        let bytes = writer.into_inner()?;
        let len = bytes.len() as u64;
        store
            .put(&location, bytes.into())
            .await
            .map_err(|source| Error::IoError {
                source: std::io::Error::other(source),
            })?;
        return Ok(len);
    }

    tokio::task::spawn_blocking(move || -> Result<u64> {
        let _completion = completion;
        #[cfg(test)]
        if let Some(test_hook) = test_hook {
            test_hook.writers_started.fetch_add(1, Ordering::AcqRel);
            while test_hook.block_writers.load(Ordering::Acquire) {
                std::thread::yield_now();
            }
            if test_hook.fail_writer_number.load(Ordering::Acquire)
                == test_hook.writers_completed.fetch_add(1, Ordering::AcqRel) + 1
            {
                return Err(Error::IoError {
                    source: std::io::Error::other("injected deletion-vector writer failure"),
                });
            }
        }
        // Crash-safe write. Ensure the deletion vector file content is durable
        // before we record a pointer to it in the catalog. A crash without
        // this sync could leave a zero-length or partial .arrow file while the
        // catalog transaction that references it has committed (or is about
        // to). On recovery, readers would then hit a missing/corrupt deletion
        // vector for a "committed" delete — either erroring or (worse)
        // returning deleted rows. This is the exact durability requirement we
        // enforce for data files and WAL markers in the append path.
        //
        // 1. Stream Arrow IPC into the file.
        // 2. Recover the underlying std::fs::File from the writer and fsync
        //    its data with the ordering tier (`fsync_tier::ordering_sync_std`:
        //    plain fsync on macOS, fdatasync on Linux — on macOS both std
        //    `sync_all` AND `sync_data` are F_FULLFSYNC ~4-5 ms, while the
        //    catalog commit referencing this file is SQLite
        //    synchronous=NORMAL, so a full drive-cache flush here cannot
        //    raise end-to-end durability; see `provider/fsync_tier.rs`). A
        //    previous revision also re-opened the file to fsync it a second
        //    time — that reopen+fsync was redundant work on every delete and
        //    has been removed.
        //
        // The parent-directory fsync (so the new dirent is written through
        // before the catalog records the path) is NOT done here: the caller
        // (`DeletionVectorWriter::write`) writes all of a batch's deletion
        // files concurrently and issues ONE coalesced deletions/-dir sync
        // after they complete — same dirent durability, 1/N the barriers.
        let file = std::fs::File::create(&output_path)?;
        let mut writer = FileWriter::try_new(file, &schema)?;
        writer.write(&batch)?;
        writer.finish()?;
        let inner = writer.into_inner()?;
        crate::provider::fsync_tier::ordering_sync_std(&inner)?;
        drop(inner);

        let metadata = std::fs::metadata(&output_path)?;

        Ok(metadata.len())
    })
    .await
    .map_err(|source| Error::TaskPanicked {
        table: table_name,
        source,
    })?
}

/// The file name of the deletion-vector file at `path` (a local path or a URL).
fn deletion_file_name(path: &Path) -> Option<&str> {
    path.file_name().and_then(std::ffi::OsStr::to_str)
}

/// Stops holding the deletion-vector files at `paths` in the dataset's writer
/// record: their writer deleted them, or no commit names them any more.
pub(crate) fn release_deletion_files<'a>(
    storage: Option<&DatasetStorage>,
    paths: impl IntoIterator<Item = &'a Path>,
) {
    if let Some(record) = storage.and_then(DatasetStorage::writer_record) {
        record.release_deletion_files(paths.into_iter().filter_map(deletion_file_name));
    }
}

/// Best-effort removal of physical deletion-vector files that were written but
/// never committed to the catalog.
pub(crate) async fn cleanup_uncommitted_delete_paths(
    paths: &[PathBuf],
    storage: Option<&DatasetStorage>,
) {
    release_deletion_files(storage, paths.iter().map(PathBuf::as_path));
    for path in paths {
        match deletion_vector_location(storage, &path.to_string_lossy()) {
            Ok(Some((store, location))) => {
                match store.delete(&location).await {
                    Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                    Err(error) => {
                        tracing::warn!(path = %path.display(), %error, "Failed to remove an uncommitted deletion-vector object");
                    }
                }
                continue;
            }
            Ok(None) => {}
            Err(error) => {
                tracing::warn!(path = %path.display(), %error, "Failed to remove an uncommitted deletion-vector object");
                continue;
            }
        }
        if let Err(error) = tokio::fs::remove_file(path).await
            && error.kind() != std::io::ErrorKind::NotFound
        {
            tracing::warn!(
                path = %path.display(),
                %error,
                "Failed to remove an uncommitted deletion-vector file"
            );
        }
    }
}

/// Remove the files of deletion-vector rows that a committed catalog change
/// dropped: rows a newer vector for the same data file superseded, or rows a
/// rewrite cleared after folding their deletes into its output. No row names
/// these files once that change commits, so nothing else would ever remove them.
///
/// Runs in the background. It first takes `file_deletion_fence` exclusively,
/// which an acceleration-snapshot archive holds shared while it copies the
/// table's files, and a caller holding the table's write lock must not wait for
/// that. Object-store keys (`path_is_relative`) are left in place, as the
/// orphaned-vector sweep leaves them.
pub(crate) fn remove_dropped_deletion_vector_files(
    files: Vec<DeleteFile>,
    file_deletion_fence: Arc<tokio::sync::RwLock<()>>,
    storage: Option<Arc<DatasetStorage>>,
) {
    // No commit names these files any more: the orphan sweep takes any of them
    // left in the store.
    release_deletion_files(
        storage.as_deref(),
        files.iter().map(|file| Path::new(file.path.as_str())),
    );
    let paths: Vec<PathBuf> = files
        .into_iter()
        .filter(|file| !file.path_is_relative)
        .map(|file| PathBuf::from(file.path))
        .collect();
    if paths.is_empty() {
        return;
    }
    crate::provider::compaction::spawn_compaction(async move {
        let _deleting = file_deletion_fence.write().await;
        for path in paths {
            match deletion_vector_location(storage.as_deref(), &path.to_string_lossy()) {
                Ok(Some((store, location))) => {
                    if let Err(error) = store.delete(&location).await
                        && !matches!(error, object_store::Error::NotFound { .. })
                    {
                        tracing::debug!(path = %path.display(), %error, "Failed to remove a deletion-vector object no catalog row references");
                    }
                    continue;
                }
                Ok(None) => {}
                Err(error) => {
                    tracing::debug!(path = %path.display(), %error, "Failed to remove a deletion-vector object no catalog row references");
                    continue;
                }
            }
            if let Err(error) = tokio::fs::remove_file(&path).await
                && error.kind() != std::io::ErrorKind::NotFound
            {
                tracing::debug!(
                    path = %path.display(),
                    %error,
                    "Failed to remove a deletion-vector file no catalog row references"
                );
            }
        }
    });
}

fn build_delete_file(
    table: &TableMetadata,
    file_path: &Path,
    delete_count: usize,
    file_size_bytes: u64,
    deletion_type: DeletionType,
    source_data_file_path: Option<String>,
) -> Result<DeleteFile> {
    let delete_count_i64 = i64::try_from(delete_count).map_err(|_| Error::Internal {
        table: table.table_name.clone(),
        message: format!("Deletion count overflow ({delete_count})."),
    })?;
    let file_size_i64 = i64::try_from(file_size_bytes).map_err(|_| Error::Internal {
        table: table.table_name.clone(),
        message: format!("Deletion vector file too large ({file_size_bytes} bytes)."),
    })?;

    Ok(DeleteFile {
        delete_file_id: String::new(),
        table_id: table.table_id.clone(),
        source_data_file_path,
        path: file_path.to_string_lossy().to_string(),
        path_is_relative: false,
        format: DELETION_FILE_FORMAT.to_string(),
        delete_count: delete_count_i64,
        file_size_bytes: file_size_i64,
        deletion_type,
        // Sequence number is set by the caller after getting the current sequence from catalog
        sequence_number: table.current_sequence_number,
        // Metadata-only publish: the per-commit reinsert sequence is stamped by the
        // catalog commit (it owns the upsert's insert_sequence); a freshly built
        // file is None until then. Pure deletes / position files keep None.
        reinsert_sequence: None,
    })
}

/// Schema for position-based deletion vectors. Selected by the shape of the
/// delete being recorded — a known `(file path, file-local position)` — not by
/// whether the table has a primary key: a primary-key table in
/// `deletion_mode: position` writes these, and falls back to the key schema
/// below for any row whose position it does not know.
static POSITION_BASED_DELETION_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("row_id", DataType::UInt64, false),
        Field::new("deleted_at", DataType::Int64, false),
    ]))
});

/// Schema for key-based deletion vectors. Requires a primary key, but primary-key
/// presence does not select it — see the position-based schema above.
static KEY_BASED_DELETION_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("row_key", DataType::Binary, false),
        Field::new("deleted_at", DataType::Int64, false),
    ]))
});

/// The column a key vector holding several delete sequences adds: each key's own
/// delete sequence, which then takes the place of the file's catalog sequence for
/// that key. One checkpoint's tombstones span many source transactions, so this is
/// what lets it write them as one file rather than one per sequence.
pub(crate) const KEY_DELETE_SEQUENCE_COLUMN: &str = "delete_sequence";

/// Schema for key-based deletion vectors that carry a delete sequence per key.
static SEQUENCED_KEY_BASED_DELETION_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("row_key", DataType::Binary, false),
        Field::new("deleted_at", DataType::Int64, false),
        Field::new(KEY_DELETE_SEQUENCE_COLUMN, DataType::Int64, false),
    ]))
});

/// Returns the schema for position-based deletion vectors.
fn position_based_deletion_schema() -> SchemaRef {
    Arc::clone(&POSITION_BASED_DELETION_SCHEMA)
}

/// Returns the schema for key-based deletion vectors.
fn key_based_deletion_schema() -> SchemaRef {
    Arc::clone(&KEY_BASED_DELETION_SCHEMA)
}

/// Returns the schema for key-based deletion vectors with a sequence per key.
fn sequenced_key_based_deletion_schema() -> SchemaRef {
    Arc::clone(&SEQUENCED_KEY_BASED_DELETION_SCHEMA)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::ipc::reader::FileReader;
    use tempfile::TempDir;

    fn build_table_metadata(temp_dir: &TempDir) -> TableMetadata {
        TableMetadata {
            table_id: "test-table-id".to_string(),
            table_name: "test_table".to_string(),
            path: temp_dir.path().to_string_lossy().to_string(),
            path_is_relative: false,
            schema: Arc::new(arrow::datatypes::Schema::empty()),
            primary_key: vec!["id".to_string()],
            on_conflict: None,
            current_snapshot_id: Uuid::now_v7().to_string(),
            partition_column: None,
            vortex_config: crate::metadata::VortexConfig::default(),
            current_sequence_number: 0,
        }
    }

    #[tokio::test]
    async fn writes_deletion_vector_and_returns_metadata() {
        let temp_dir = TempDir::new().expect("temp dir");
        let table_metadata = build_table_metadata(&temp_dir);
        let writer = DeletionVectorWriter::new(&table_metadata, None);

        let specs = vec![DeletionVectorWriteSpec::new_position_based(
            "test_file.vortex".to_string(),
            vec![3, 1, 3, 2],
        )];
        let results = writer.write(specs).await.expect("write deletion vector");

        assert_eq!(results.len(), 1);
        let result = &results[0];

        // Extract the row IDs from the result
        let row_ids = match &result.identifiers {
            DeletionIdentifier::PositionBased { row_ids, .. } => row_ids.clone(),
            DeletionIdentifier::KeyBased(_) => panic!("Expected position-based identifiers"),
        };

        assert_eq!(row_ids, vec![1, 2, 3]);
        assert_eq!(result.delete_file.table_id, table_metadata.table_id);
        assert_eq!(
            result.delete_file.delete_count,
            i64::try_from(row_ids.len()).expect("convert delete count")
        );
        assert_eq!(result.delete_file.format, DELETION_FILE_FORMAT);

        let file = std::fs::File::open(&result.delete_file.path).expect("open deletion file");
        let reader = FileReader::try_new(file, None).expect("create reader");
        let batches: Vec<_> = reader
            .into_iter()
            .collect::<std::result::Result<Vec<_>, _>>()
            .expect("read batches");
        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), row_ids.len());

        let row_ids_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("row_id column");
        let read_row_ids: Vec<_> = (0..row_ids_col.len())
            .map(|idx| row_ids_col.value(idx))
            .collect();
        assert_eq!(read_row_ids, row_ids);
    }

    #[tokio::test]
    async fn skips_empty_specs() {
        let temp_dir = TempDir::new().expect("temp dir");
        let table_metadata = build_table_metadata(&temp_dir);
        let writer = DeletionVectorWriter::new(&table_metadata, None);

        let results = writer
            .write(vec![
                DeletionVectorWriteSpec::new_position_based("empty.vortex".to_string(), vec![]),
                DeletionVectorWriteSpec::new_position_based("test.vortex".to_string(), vec![0]),
            ])
            .await
            .expect("write deletion vector");

        assert_eq!(results.len(), 1);
    }

    #[tokio::test]
    async fn rejects_position_ids_above_bitmap_range() {
        let temp_dir = TempDir::new().expect("temp dir");
        let table_metadata = build_table_metadata(&temp_dir);
        let writer = DeletionVectorWriter::new(&table_metadata, None);
        let results = writer
            .write(vec![DeletionVectorWriteSpec::new_position_based(
                "large.vortex".to_string(),
                vec![u64::from(u32::MAX) + 1],
            )])
            .await
            .expect("write oversized position deletion vector");

        let delete_files = results
            .into_iter()
            .map(|result| result.delete_file)
            .collect();
        let error = detect_deletion_type_and_read(delete_files)
            .expect_err("oversized position must not be silently skipped");

        assert!(
            error.to_string().contains("exceeds the supported maximum"),
            "unexpected error: {error}"
        );
    }

    /// One write carries files for more delete sequences than it runs at once,
    /// and each file reads back at its own spec's sequence and keys, in spec
    /// order; a spec with no sequence keeps the table's.
    #[tokio::test]
    async fn one_write_records_each_spec_at_its_own_sequence() {
        let temp_dir = TempDir::new().expect("temp dir");
        let mut table_metadata = build_table_metadata(&temp_dir);
        table_metadata.current_sequence_number = 7;
        let key = |i: usize| -> Box<[u8]> { format!("key-{i}").into_bytes().into_boxed_slice() };
        let sequence = |i: usize| 1000 + i64::try_from(i).expect("small index");
        let groups = DELETION_VECTOR_WRITE_CONCURRENCY * 3 + 1;
        let mut specs: Vec<DeletionVectorWriteSpec> = (0..groups)
            .map(|i| {
                DeletionVectorWriteSpec::new_key_based(vec![key(i), key(i + groups)])
                    .with_sequence_number(sequence(i))
            })
            .collect();
        specs.push(DeletionVectorWriteSpec::new_key_based(vec![
            key(3 * groups),
        ]));
        let results = DeletionVectorWriter::new(&table_metadata, None)
            .write(specs)
            .await
            .expect("write deletion vectors");

        assert_eq!(results.len(), groups + 1);
        let recorded: Vec<i64> = results
            .iter()
            .map(|result| result.delete_file.sequence_number)
            .collect();
        let expected: Vec<i64> = (0..groups).map(sequence).chain([7]).collect();
        assert_eq!(recorded, expected);

        let (positions, keys, reinserted, missing) = detect_deletion_type_and_read(
            results
                .into_iter()
                .map(|result| result.delete_file)
                .collect(),
        )
        .expect("read the written deletion vectors back");
        assert!(positions.is_empty() && reinserted.is_empty() && missing.is_empty());
        let mut expected_keys: HashMap<Box<[u8]>, i64> = (0..groups)
            .flat_map(|i| [(key(i), sequence(i)), (key(i + groups), sequence(i))])
            .collect();
        expected_keys.insert(key(3 * groups), 7);
        assert_eq!(keys, expected_keys);
    }

    /// A key vector written for several delete sequences is one file recorded at
    /// the highest, and reads back each key at its own sequence; a key listed at
    /// two sequences keeps the higher.
    #[tokio::test]
    async fn one_key_vector_carries_each_key_at_its_own_sequence() {
        let temp_dir = TempDir::new().expect("temp dir");
        let table_metadata = build_table_metadata(&temp_dir);
        let key = |name: &str| -> Box<[u8]> { name.as_bytes().into() };
        let results = DeletionVectorWriter::new(&table_metadata, None)
            .write(vec![DeletionVectorWriteSpec::new_key_based_with_sequences(
                vec![
                    (key("c"), 12),
                    (key("a"), 10),
                    (key("b"), 11),
                    (key("a"), 14),
                    (key("d"), 9),
                ],
            )])
            .await
            .expect("write the sequenced key vector");
        assert_eq!(results.len(), 1, "one file for every sequence");
        assert_eq!(results[0].delete_file.sequence_number, 14);
        assert_eq!(results[0].delete_file.delete_count, 4);

        let (positions, keys, reinserted, missing) = detect_deletion_type_and_read(
            results
                .into_iter()
                .map(|result| result.delete_file)
                .collect(),
        )
        .expect("read the sequenced key vector back");
        assert!(positions.is_empty() && reinserted.is_empty() && missing.is_empty());
        let expected: HashMap<Box<[u8]>, i64> = [
            (key("a"), 14),
            (key("b"), 11),
            (key("c"), 12),
            (key("d"), 9),
        ]
        .into_iter()
        .collect();
        assert_eq!(keys, expected);
    }

    #[tokio::test]
    async fn later_writer_failure_cleans_every_batch_path() {
        let temp_dir = TempDir::new().expect("temp dir");
        let table_metadata = build_table_metadata(&temp_dir);
        let test_hook = TestWriterHookGuard::new(2, false);
        let writer = DeletionVectorWriter::new_with_test_hook(&table_metadata, test_hook.hook());
        writer
            .write(vec![
                DeletionVectorWriteSpec::new_position_based("first.vortex".to_string(), vec![1]),
                DeletionVectorWriteSpec::new_position_based("second.vortex".to_string(), vec![2]),
                DeletionVectorWriteSpec::new_position_based("third.vortex".to_string(), vec![3]),
            ])
            .await
            .expect_err("injected later writer must fail the logical batch");

        let deletion_dir = Path::new(&table_metadata.path)
            .join(&table_metadata.current_snapshot_id)
            .join(DELETION_DIR_NAME);
        let entries = std::fs::read_dir(&deletion_dir)
            .expect("read deletion directory after failure")
            .collect::<std::io::Result<Vec<_>>>()
            .expect("collect deletion directory entries");
        assert!(entries.is_empty(), "failed batch leaked deletion vectors");
    }

    #[tokio::test]
    async fn cancellation_waits_for_detached_writers_then_cleans_paths() {
        let temp_dir = TempDir::new().expect("temp dir");
        let table_metadata = build_table_metadata(&temp_dir);
        let test_hook = TestWriterHookGuard::new(0, true);
        let metadata = table_metadata.clone();
        let task_hook = test_hook.hook();
        let task = tokio::spawn(async move {
            DeletionVectorWriter::new_with_test_hook(&metadata, task_hook)
                .write(vec![
                    DeletionVectorWriteSpec::new_position_based(
                        "first.vortex".to_string(),
                        vec![1],
                    ),
                    DeletionVectorWriteSpec::new_position_based(
                        "second.vortex".to_string(),
                        vec![2],
                    ),
                ])
                .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while test_hook.0.writers_started.load(Ordering::Acquire) < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("writers should reach the deterministic gate");
        task.abort();
        test_hook.0.block_writers.store(false, Ordering::Release);
        let _ = task.await;

        let deletion_dir = Path::new(&table_metadata.path)
            .join(&table_metadata.current_snapshot_id)
            .join(DELETION_DIR_NAME);
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let empty =
                    std::fs::read_dir(&deletion_dir).map_or(true, |entries| entries.count() == 0);
                if empty {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("cancellation cleanup should remove every generated path");
    }
}
