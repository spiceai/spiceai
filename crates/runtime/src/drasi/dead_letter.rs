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

//! Durable store for change batches Drasi would not accept.
//!
//! Under `delivery: queued` the runtime acknowledges the source's replication
//! position without waiting for Drasi, so the replication log is no longer the
//! thing that replays a failed change — this is. A batch that cannot be
//! delivered is written here and retried until it lands.
//!
//! # Ordering
//!
//! Both Drasi wire formats treat an insert or update as a **full-state replace**
//! keyed by element id. Replaying a failed batch after a later batch for the same
//! row already landed would therefore overwrite newer state with older state, so
//! redelivery cannot simply run in the background while new changes flow past
//! it.
//!
//! The store is instead **stop-the-line**: once anything is pending, every
//! subsequent batch for that component is appended behind it, and delivery
//! resumes only once the store drains in sequence order. Replication still never
//! waits — that is what `queued` buys — but Drasi's view of a component advances
//! strictly in order or not at all.
//!
//! # Bound
//!
//! The store is capped. Past the cap the **oldest** batch is discarded, which is
//! the less harmful direction for full-state-replace semantics: the newest state
//! for a row is the state worth keeping. A discarded batch is counted, and a
//! non-zero count means Drasi's view of that component has a gap.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use arrow::ipc::reader::FileReader;
use arrow::ipc::writer::FileWriter;
use serde::{Deserialize, Serialize};
use snafu::prelude::*;

use crate::drasi::queue::QueuedBatch;

/// Batches retained per component before the oldest is discarded.
pub(crate) const DEFAULT_MAX_BATCHES: usize = 1024;

/// How long to wait between attempts to drain a non-empty store.
pub(crate) const RETRY_INTERVAL: std::time::Duration = std::time::Duration::from_secs(15);

/// Schema-metadata key holding the per-batch delivery metadata.
const METADATA_KEY: &str = "spice.drasi.dead_letter";

/// Written alongside the batch so a recovered file can be delivered without the
/// change stream that produced it.
#[derive(Debug, Serialize, Deserialize)]
struct BatchMetadata {
    op_codes: Vec<String>,
    primary_key_columns: Vec<Vec<String>>,
    source_commit_ts_ms: Option<i64>,
}

#[derive(Debug, Snafu)]
pub(crate) enum Error {
    #[snafu(display("Failed to prepare the Drasi dead-letter directory {}: {source}", path.display()))]
    PrepareDirectory {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("Failed to read the Drasi dead-letter directory {}: {source}", path.display()))]
    ReadDirectory {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("Failed to write the Drasi dead-letter batch {}: {source}", path.display()))]
    WriteBatch {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("Failed to encode the Drasi dead-letter batch: {source}"))]
    EncodeBatch { source: arrow::error::ArrowError },

    #[snafu(display("Failed to read the Drasi dead-letter batch {}: {source}", path.display()))]
    DecodeBatch {
        path: PathBuf,
        source: arrow::error::ArrowError,
    },

    #[snafu(display("The Drasi dead-letter batch {} is missing its delivery metadata", path.display()))]
    MissingMetadata { path: PathBuf },
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// A component's on-disk dead-letter store.
#[derive(Debug)]
pub(crate) struct DeadLetterStore {
    dir: PathBuf,
    component: String,
    max_batches: usize,
    /// Next filename sequence. Recovered from the directory on open so ordering
    /// survives a restart.
    next_sequence: AtomicU64,
    /// Batches discarded because the store was full.
    discarded: AtomicU64,
}

impl DeadLetterStore {
    /// Opens (creating if needed) the store under `dir`, recovering any batches
    /// a previous run left behind.
    ///
    /// # Errors
    ///
    /// Returns an error if the directory cannot be created or listed.
    pub(crate) async fn open(
        dir: PathBuf,
        component: String,
        max_batches: usize,
    ) -> Result<Self> {
        tokio::fs::create_dir_all(&dir)
            .await
            .context(PrepareDirectorySnafu { path: dir.clone() })?;

        let pending = list_batches(&dir).await?;
        // Continue past the highest sequence already on disk, so a recovered
        // batch is still delivered before anything written after it.
        let next = pending.last().map_or(0, |(sequence, _)| sequence + 1);

        if !pending.is_empty() {
            tracing::info!(
                "Recovered {} undelivered Drasi change batch(es) for {component} from {}",
                pending.len(),
                dir.display()
            );
        }

        Ok(Self {
            dir,
            component,
            max_batches,
            next_sequence: AtomicU64::new(next),
            discarded: AtomicU64::new(0),
        })
    }

    /// Whether anything is awaiting redelivery.
    ///
    /// Read before every delivery: a non-empty store means new batches must
    /// queue behind what is already pending rather than overtake it.
    pub(crate) async fn is_empty(&self) -> bool {
        match list_batches(&self.dir).await {
            Ok(pending) => pending.is_empty(),
            Err(e) => {
                // Treat an unreadable store as non-empty: appending is the safe
                // direction, since delivering would risk reordering.
                tracing::warn!(
                    "Could not read the Drasi dead-letter store for {}: {e}",
                    self.component
                );
                false
            }
        }
    }

    /// How many batches have been discarded because the store was full.
    pub(crate) fn discarded(&self) -> u64 {
        self.discarded.load(Ordering::Relaxed)
    }

    /// Persists `batch` for later redelivery.
    ///
    /// # Errors
    ///
    /// Returns an error if the batch cannot be encoded or written.
    pub(crate) async fn append(&self, batch: &QueuedBatch) -> Result<()> {
        self.enforce_cap().await?;

        let sequence = self.next_sequence.fetch_add(1, Ordering::Relaxed);
        let path = self.dir.join(batch_file_name(sequence));
        let temporary = self.dir.join(format!("{}.partial", batch_file_name(sequence)));

        let metadata = serde_json::to_string(&BatchMetadata {
            op_codes: batch.op_codes.clone(),
            primary_key_columns: batch.primary_key_columns.clone(),
            source_commit_ts_ms: batch.source_commit_ts_ms,
        })
        .map_err(|e| Error::EncodeBatch {
            source: arrow::error::ArrowError::JsonError(e.to_string()),
        })?;

        let data = batch.data.clone();
        // Arrow IPC encoding is synchronous and CPU-bound.
        let encoded =
            tokio::task::spawn_blocking(move || encode(&data, &metadata))
                .await
                .map_err(|e| Error::EncodeBatch {
                    source: arrow::error::ArrowError::ExternalError(Box::new(e)),
                })??;

        // Written to a sibling name and renamed, so a crash mid-write cannot
        // leave a truncated file that recovery would try to deliver.
        tokio::fs::write(&temporary, encoded)
            .await
            .context(WriteBatchSnafu {
                path: temporary.clone(),
            })?;
        tokio::fs::rename(&temporary, &path)
            .await
            .context(WriteBatchSnafu { path: path.clone() })?;

        Ok(())
    }

    /// Attempts to deliver every pending batch, oldest first, stopping at the
    /// first failure so ordering is preserved.
    ///
    /// Returns the number delivered.
    pub(crate) async fn drain<F, Fut>(&self, deliver: F) -> usize
    where
        F: Fn(QueuedBatch) -> Fut,
        Fut: std::future::Future<Output = bool>,
    {
        let pending = match list_batches(&self.dir).await {
            Ok(pending) => pending,
            Err(e) => {
                tracing::warn!(
                    "Could not read the Drasi dead-letter store for {}: {e}",
                    self.component
                );
                return 0;
            }
        };

        let mut delivered = 0;
        for (_, path) in pending {
            let batch = match read_batch(&path).await {
                Ok(batch) => batch,
                Err(e) => {
                    // An unreadable file would block the queue forever. Drop it,
                    // count it, and continue — it is already a gap either way.
                    tracing::warn!(
                        "Discarding an unreadable Drasi dead-letter batch for {}: {e}",
                        self.component
                    );
                    self.discarded.fetch_add(1, Ordering::Relaxed);
                    let _ = tokio::fs::remove_file(&path).await;
                    continue;
                }
            };

            if !deliver(batch).await {
                // Stop at the first failure: everything after it is newer, and
                // delivering it now would apply state out of order.
                break;
            }

            if let Err(e) = tokio::fs::remove_file(&path).await {
                // The batch landed but the record of it did not go away, so it
                // would be delivered twice. Both formats are idempotent per
                // element id, so a duplicate is safe; stop anyway to avoid
                // looping on it.
                tracing::warn!(
                    "Delivered a Drasi dead-letter batch for {} but could not remove {}: {e}",
                    self.component,
                    path.display()
                );
                break;
            }

            delivered += 1;
        }

        if delivered > 0 {
            tracing::info!(
                "Redelivered {delivered} Drasi change batch(es) for {}",
                self.component
            );
        }

        delivered
    }

    /// Discards the oldest batches until there is room for one more.
    async fn enforce_cap(&self) -> Result<()> {
        let pending = list_batches(&self.dir).await?;
        if pending.len() < self.max_batches {
            return Ok(());
        }

        let excess = pending.len() - self.max_batches + 1;
        for (_, path) in pending.into_iter().take(excess) {
            if tokio::fs::remove_file(&path).await.is_ok() {
                let total = self.discarded.fetch_add(1, Ordering::Relaxed) + 1;
                if total == 1 || total.is_multiple_of(1000) {
                    tracing::warn!(
                        "The Drasi dead-letter store for {} is full at {} batches; discarded the oldest ({total} discarded so far). Drasi's view of this component has a gap.",
                        self.component,
                        self.max_batches
                    );
                }
            }
        }

        Ok(())
    }
}

/// Pending batch files, oldest first.
async fn list_batches(dir: &Path) -> Result<Vec<(u64, PathBuf)>> {
    let mut entries = tokio::fs::read_dir(dir)
        .await
        .context(ReadDirectorySnafu { path: dir })?;

    let mut batches = Vec::new();
    while let Some(entry) = entries
        .next_entry()
        .await
        .context(ReadDirectorySnafu { path: dir })?
    {
        let path = entry.path();
        // `.partial` files are half-written; recovery must not deliver them.
        if let Some(sequence) = path
            .file_name()
            .and_then(|name| name.to_str())
            .and_then(parse_batch_file_name)
        {
            batches.push((sequence, path));
        }
    }

    batches.sort_by_key(|(sequence, _)| *sequence);
    Ok(batches)
}

/// Zero-padded so lexicographic order matches numeric order.
fn batch_file_name(sequence: u64) -> String {
    format!("{sequence:020}.arrow")
}

fn parse_batch_file_name(name: &str) -> Option<u64> {
    name.strip_suffix(".arrow")?.parse().ok()
}

fn encode(batch: &RecordBatch, metadata: &str) -> Result<Vec<u8>> {
    let schema = Schema::new_with_metadata(
        batch.schema().fields().clone(),
        [(METADATA_KEY.to_string(), metadata.to_string())]
            .into_iter()
            .collect(),
    );

    let mut buffer = Vec::new();
    let mut writer =
        FileWriter::try_new(&mut buffer, &schema).context(EncodeBatchSnafu)?;
    writer.write(batch).context(EncodeBatchSnafu)?;
    writer.finish().context(EncodeBatchSnafu)?;
    drop(writer);

    Ok(buffer)
}

async fn read_batch(path: &Path) -> Result<QueuedBatch> {
    let bytes = tokio::fs::read(path).await.context(WriteBatchSnafu { path })?;
    let path = path.to_path_buf();

    tokio::task::spawn_blocking(move || decode(&bytes, &path))
        .await
        .map_err(|e| Error::EncodeBatch {
            source: arrow::error::ArrowError::ExternalError(Box::new(e)),
        })?
}

fn decode(bytes: &[u8], path: &Path) -> Result<QueuedBatch> {
    let reader =
        FileReader::try_new(std::io::Cursor::new(bytes), None).context(DecodeBatchSnafu { path })?;

    let metadata: BatchMetadata = reader
        .schema()
        .metadata()
        .get(METADATA_KEY)
        .and_then(|raw| serde_json::from_str(raw).ok())
        .context(MissingMetadataSnafu { path })?;

    let batches = reader
        .collect::<std::result::Result<Vec<_>, _>>()
        .context(DecodeBatchSnafu { path })?;

    // One batch per file, by construction in `append`.
    let data = batches
        .into_iter()
        .next()
        .context(MissingMetadataSnafu { path })?;

    Ok(QueuedBatch {
        op_codes: metadata.op_codes,
        primary_key_columns: metadata.primary_key_columns,
        data,
        source_commit_ts_ms: metadata.source_commit_ts_ms,
    })
}

/// The directory a component's dead-letter store lives in.
pub(crate) fn store_path(component: &str) -> PathBuf {
    // Component names are table references (`runtime.task_history`, `orders`),
    // which can contain characters a path should not take verbatim.
    let sanitized: String = component
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.' {
                c
            } else {
                '_'
            }
        })
        .collect();

    PathBuf::from(data_accelerator_api::spice_data_base_path())
        .join("drasi")
        .join(sanitized)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Array;
    use std::sync::Arc;
    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field};

    fn batch(id: &str) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)])),
            vec![Arc::new(StringArray::from(vec![id]))],
        )
        .expect("valid batch")
    }

    fn queued(id: &str) -> QueuedBatch {
        QueuedBatch::uniform("c", &["id".to_string()], batch(id), Some(42))
    }

    async fn store(dir: &tempfile::TempDir, max: usize) -> DeadLetterStore {
        DeadLetterStore::open(dir.path().to_path_buf(), "orders".to_string(), max)
            .await
            .expect("opens")
    }

    fn ids(batch: &RecordBatch) -> Vec<String> {
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("utf8");
        (0..column.len()).map(|i| column.value(i).to_string()).collect()
    }

    #[tokio::test]
    async fn a_batch_round_trips_with_its_delivery_metadata() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = store(&dir, DEFAULT_MAX_BATCHES).await;

        store.append(&queued("row-1")).await.expect("appends");

        let seen = std::sync::Mutex::new(Vec::new());
        store
            .drain(|batch| {
                seen.lock().expect("not poisoned").push(batch);
                async { true }
            })
            .await;

        let seen = seen.into_inner().expect("not poisoned");
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].op_codes, vec!["c".to_string()]);
        assert_eq!(seen[0].primary_key_columns, vec![vec!["id".to_string()]]);
        assert_eq!(seen[0].source_commit_ts_ms, Some(42));
        assert_eq!(ids(&seen[0].data), vec!["row-1".to_string()]);
    }

    #[tokio::test]
    async fn a_delivered_batch_is_removed() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = store(&dir, DEFAULT_MAX_BATCHES).await;

        store.append(&queued("row-1")).await.expect("appends");
        assert!(!store.is_empty().await);

        let delivered = store.drain(|_| async { true }).await;

        assert_eq!(delivered, 1);
        assert!(store.is_empty().await, "a delivered batch must not be retried");
    }

    #[tokio::test]
    async fn a_failed_batch_is_retained() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = store(&dir, DEFAULT_MAX_BATCHES).await;

        store.append(&queued("row-1")).await.expect("appends");
        let delivered = store.drain(|_| async { false }).await;

        assert_eq!(delivered, 0);
        assert!(!store.is_empty().await, "a failed batch must be retried later");
    }

    /// The ordering guarantee: a full-state replace applied out of order would
    /// overwrite newer state with older, so a failure stops the drain.
    #[tokio::test]
    async fn a_failure_stops_the_drain_so_later_batches_do_not_overtake_it() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = store(&dir, DEFAULT_MAX_BATCHES).await;

        for id in ["row-1", "row-2", "row-3"] {
            store.append(&queued(id)).await.expect("appends");
        }

        let seen = std::sync::Mutex::new(Vec::new());
        store
            .drain(|batch| {
                let id = ids(&batch.data)[0].clone();
                seen.lock().expect("not poisoned").push(id.clone());
                // The second batch fails.
                async move { id != "row-2" }
            })
            .await;

        assert_eq!(
            *seen.lock().expect("not poisoned"),
            vec!["row-1".to_string(), "row-2".to_string()],
            "row-3 must not be attempted while row-2 is undelivered"
        );
        assert!(!store.is_empty().await);
    }

    #[tokio::test]
    async fn batches_are_delivered_oldest_first() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = store(&dir, DEFAULT_MAX_BATCHES).await;

        for id in ["row-1", "row-2", "row-3"] {
            store.append(&queued(id)).await.expect("appends");
        }

        let seen = std::sync::Mutex::new(Vec::new());
        store
            .drain(|batch| {
                seen.lock()
                    .expect("not poisoned")
                    .push(ids(&batch.data)[0].clone());
                async { true }
            })
            .await;

        assert_eq!(
            *seen.lock().expect("not poisoned"),
            vec!["row-1".to_string(), "row-2".to_string(), "row-3".to_string()]
        );
    }

    /// The whole point of persisting: a restart must not lose what was pending.
    #[tokio::test]
    async fn pending_batches_survive_a_reopen() {
        let dir = tempfile::tempdir().expect("tempdir");
        {
            let store = store(&dir, DEFAULT_MAX_BATCHES).await;
            store.append(&queued("row-1")).await.expect("appends");
            store.append(&queued("row-2")).await.expect("appends");
        }

        let reopened = store(&dir, DEFAULT_MAX_BATCHES).await;
        assert!(!reopened.is_empty().await);

        let seen = std::sync::Mutex::new(Vec::new());
        reopened
            .drain(|batch| {
                seen.lock()
                    .expect("not poisoned")
                    .push(ids(&batch.data)[0].clone());
                async { true }
            })
            .await;

        assert_eq!(
            *seen.lock().expect("not poisoned"),
            vec!["row-1".to_string(), "row-2".to_string()],
            "recovered batches keep their original order"
        );
    }

    /// A batch appended after recovery must still sort after the recovered ones.
    #[tokio::test]
    async fn a_reopened_store_continues_the_sequence() {
        let dir = tempfile::tempdir().expect("tempdir");
        {
            let store = store(&dir, DEFAULT_MAX_BATCHES).await;
            store.append(&queued("row-1")).await.expect("appends");
        }

        let reopened = store(&dir, DEFAULT_MAX_BATCHES).await;
        reopened.append(&queued("row-2")).await.expect("appends");

        let seen = std::sync::Mutex::new(Vec::new());
        reopened
            .drain(|batch| {
                seen.lock()
                    .expect("not poisoned")
                    .push(ids(&batch.data)[0].clone());
                async { true }
            })
            .await;

        assert_eq!(
            *seen.lock().expect("not poisoned"),
            vec!["row-1".to_string(), "row-2".to_string()],
            "a batch written after recovery must not overtake a recovered one"
        );
    }

    /// Past the cap the oldest goes, because the newest state for a row is the
    /// state worth keeping under full-state-replace semantics.
    #[tokio::test]
    async fn a_full_store_discards_the_oldest() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = store(&dir, 2).await;

        for id in ["row-1", "row-2", "row-3"] {
            store.append(&queued(id)).await.expect("appends");
        }

        assert_eq!(store.discarded(), 1);

        let seen = std::sync::Mutex::new(Vec::new());
        store
            .drain(|batch| {
                seen.lock()
                    .expect("not poisoned")
                    .push(ids(&batch.data)[0].clone());
                async { true }
            })
            .await;

        assert_eq!(
            *seen.lock().expect("not poisoned"),
            vec!["row-2".to_string(), "row-3".to_string()],
            "the oldest is the one dropped"
        );
    }

    /// A crash mid-write leaves a `.partial` file; recovery must ignore it
    /// rather than try to decode a truncated batch.
    #[tokio::test]
    async fn a_partial_file_is_ignored() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = store(&dir, DEFAULT_MAX_BATCHES).await;

        tokio::fs::write(dir.path().join("00000000000000000007.arrow.partial"), b"garbage")
            .await
            .expect("writes");

        assert!(store.is_empty().await, "a partial write is not a pending batch");
    }

    /// An unreadable file would otherwise block the queue forever.
    #[tokio::test]
    async fn an_undecodable_batch_is_discarded_rather_than_blocking() {
        let dir = tempfile::tempdir().expect("tempdir");

        // Written before the store opens, so recovery assigns the appended batch
        // a later sequence rather than reusing this one.
        tokio::fs::write(dir.path().join(batch_file_name(0)), b"not arrow")
            .await
            .expect("writes");

        let store = store(&dir, DEFAULT_MAX_BATCHES).await;
        store.append(&queued("row-1")).await.expect("appends");

        let delivered = store.drain(|_| async { true }).await;

        assert_eq!(delivered, 1, "the readable batch behind it still lands");
        assert_eq!(store.discarded(), 1);
        assert!(store.is_empty().await);
    }

    #[test]
    fn file_names_sort_numerically() {
        assert!(batch_file_name(2) < batch_file_name(10));
        assert_eq!(parse_batch_file_name(&batch_file_name(10)), Some(10));
        assert_eq!(parse_batch_file_name("00000000000000000010.arrow.partial"), None);
        assert_eq!(parse_batch_file_name("notes.txt"), None);
    }

    #[test]
    fn store_path_sanitizes_a_qualified_table_name() {
        let path = store_path("runtime.task_history");
        assert!(path.ends_with("drasi/runtime.task_history"), "{path:?}");

        let path = store_path("weird/../name");
        assert!(
            path.ends_with("drasi/weird_.._name"),
            "a path separator must not escape the store directory: {path:?}"
        );
    }
}
