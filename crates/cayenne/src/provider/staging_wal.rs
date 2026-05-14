/*
Copyright 2026 The Spice.ai OSS Authors

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

//! Staging Write-Ahead Log (WAL) for crash-safe staged appends.
//!
//! When a Cayenne table performs a staged append, data files are first written
//! to a `_staging/` directory and then moved to the active snapshot. The move
//! is **not** atomic as a batch (individual renames are atomic on local FS, but
//! the loop over files is not).
//!
//! The staging WAL bridges this gap:
//!
//! 1. After all data files are written to `_staging/`, a `_wal.json` file is
//!    created that records which files need to be moved and to which snapshot.
//! 2. The files are then moved to the snapshot directory.
//! 3. On success, the WAL file is removed.
//!
//! If the process crashes during step 2, the WAL file survives and is detected
//! on the next table open or write attempt, alerting the operator to a potentially inconsistent
//! state.

use super::Result;
use super::constants::{STAGING_DIR_NAME, STAGING_WAL_FILENAME};
use super::table::CayenneTableProvider;
use crate::provider::Error;
use datafusion::execution::SendableRecordBatchStream;
use futures::TryStreamExt;
use object_store::path::Path as ObjectStorePath;
use tokio::sync::OwnedMutexGuard;

/// Coordinates staged writes and the staging WAL lifecycle for a Cayenne table.
///
/// This struct supports two usage patterns:
/// - Granular orchestration via `write_wal`, `move_staged_files`,
///   `remove_wal`, and `refresh_listing_table`
/// - One-shot orchestration via `finalize_staged_write`
///
/// `begin_staged_append` returns this handle after writing data into `_staging/`
/// so external consumers can synchronize writes and call `commit` only when ready.
pub struct CayenneStagedAppend {
    table: CayenneTableProvider,
    write_guard: Option<OwnedMutexGuard<()>>,
    row_count: u64,
}

impl std::fmt::Debug for CayenneStagedAppend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CayenneStagedAppend")
            .field("table", &self.table.table_name())
            .field("has_write_guard", &self.write_guard.is_some())
            .field("row_count", &self.row_count)
            .finish()
    }
}

impl CayenneStagedAppend {
    pub(crate) fn from_staged_append(
        table: CayenneTableProvider,
        write_guard: OwnedMutexGuard<()>,
        row_count: u64,
    ) -> Self {
        Self {
            table,
            write_guard: Some(write_guard),
            row_count,
        }
    }

    pub(crate) fn from_existing_staging(table: CayenneTableProvider) -> Self {
        Self {
            table,
            write_guard: None,
            row_count: 0,
        }
    }

    /// Returns the number of rows staged for commit.
    #[must_use]
    pub fn row_count(&self) -> u64 {
        self.row_count
    }

    /// Writes the staging WAL for the current `_staging/` files.
    ///
    /// # Errors
    ///
    /// Returns an error if writing the WAL file fails.
    pub async fn write_wal(&self) -> Result<()> {
        self.table.write_staging_wal().await
    }

    /// Moves staged files into the current snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error if moving the staged files fails.
    pub async fn move_staged_files(&self) -> Result<()> {
        self.table.move_files_to_current_snapshot().await
    }

    /// Removes the staging WAL after a successful move.
    ///
    /// # Errors
    ///
    /// Returns an error if removing the WAL file fails.
    pub async fn remove_wal(&self) -> Result<()> {
        self.table.remove_staging_wal().await
    }

    /// Refreshes the listing table so newly committed files become visible.
    ///
    /// # Errors
    ///
    /// Returns an error if refreshing the listing table fails.
    pub fn refresh_listing_table(&self) -> Result<()> {
        self.table.refresh_listing_table()
    }

    /// Executes the full WAL finalize sequence in order.
    ///
    /// # Errors
    ///
    /// Returns an error if any step in the finalize sequence (write WAL, move files,
    /// remove WAL, or refresh listing table) fails.
    pub async fn finalize_staged_write(&self) -> Result<()> {
        self.write_wal().await?;
        self.move_staged_files().await?;
        self.remove_wal().await?;
        self.refresh_listing_table()?;
        Ok(())
    }

    /// Commits the staged append, making the new rows visible to readers.
    ///
    /// # Errors
    ///
    /// Returns an error if the finalize sequence fails.
    pub async fn commit(self) -> Result<u64> {
        self.finalize_staged_write().await?;
        let _ = self.write_guard;
        Ok(self.row_count)
    }

    /// Discards the staged append and removes any staged files.
    ///
    /// # Errors
    ///
    /// Returns an error if clearing the staging directory fails.
    pub async fn rollback(self) -> Result<()> {
        let _ = self.write_guard;
        self.table.clear_staging_dir().await
    }
}

/// Staging WAL (Write-Ahead Log) entry.
///
/// Written to `_staging/_wal.json` after all data files are staged but before
/// the move-to-snapshot operation begins. Records the intent so that an
/// interrupted move can be detected on the next table open.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct StagingWal {
    /// The table this WAL entry belongs to.
    pub table_name: String,
    /// The snapshot directory the staged files should be moved to.
    pub target_snapshot: String,
    /// Names of the data files in the staging directory.
    pub staged_files: Vec<String>,
    /// ISO-8601 timestamp when this WAL entry was created.
    pub created_at: String,
}

impl CayenneTableProvider {
    /// Create a staging WAL handle for data already written to `_staging/`.
    pub(crate) fn staged_append_for_existing_staging(&self) -> CayenneStagedAppend {
        CayenneStagedAppend::from_existing_staging(self.clone_for_write())
    }

    /// Stage an append into Cayenne without making the new rows visible.
    ///
    /// This path supports append-only semantics and returns a handle that allows
    /// callers to commit or roll back once external coordination succeeds.
    ///
    /// # Errors
    ///
    /// Returns an error if the table has an incomplete write, uses a non-position-based
    /// deletion strategy, has pending deletions, the incoming stream contains upsert or
    /// on-conflict writes, or if writing the staged data fails.
    pub async fn begin_staged_append(
        &self,
        data: SendableRecordBatchStream,
    ) -> Result<CayenneStagedAppend> {
        let write_guard = self.write_lock_arc().lock_owned().await;

        self.ensure_no_incomplete_write().await?;

        if !self.pk_deletion_strategy().is_position_based() {
            return Err(Error::Unsupported {
                operation: "staged append for Cayenne tables with primary-key deletion handling",
            });
        }

        if self.has_pending_deletions() {
            return Err(Error::Unsupported {
                operation: "staged append for Cayenne tables with pending deletions",
            });
        }

        let prepared_insert = self.prepare_stream_for_insert(data).await?;

        if !prepared_insert.on_conflict_deletions.is_empty() {
            return Err(Error::Unsupported {
                operation: "staged append for Cayenne upsert or on-conflict writes",
            });
        }

        self.clear_staging_dir().await?;

        let (row_count, _writer_ops, _stats_acc) = self
            .write_to_snapshot(
                prepared_insert.stream,
                self.target_file_size_bytes(),
                STAGING_DIR_NAME,
                self.internal_target_partitions(),
            )
            .await?;

        Ok(CayenneStagedAppend::from_staged_append(
            self.clone_for_write(),
            write_guard,
            row_count,
        ))
    }

    /// Write the staging WAL file that records the pending move operation.
    ///
    /// The WAL is written after all data files have been staged but before any
    /// files are moved to the snapshot directory. It records the list of staged
    /// files and the target snapshot so that an interrupted move can be detected
    /// on the next table open.
    ///
    /// # Layout
    ///
    /// The WAL file is placed at `{table_path}/{table_id}/_staging/_wal.json`
    /// (local FS) or at the corresponding S3 key.
    pub(crate) async fn write_staging_wal(&self) -> Result<()> {
        let current_snapshot = self.get_current_snapshot_id()?;

        if self.table_path().starts_with("s3://") {
            self.write_staging_wal_s3(&current_snapshot).await
        } else {
            self.write_staging_wal_local(&current_snapshot).await
        }
    }

    /// Write the staging WAL on local filesystem.
    async fn write_staging_wal_local(&self, target_snapshot: &str) -> Result<()> {
        let staging_dir =
            Self::snapshot_dir_path(self.table_path(), self.table_id(), STAGING_DIR_NAME);

        // Collect staged file names (exclude the WAL file itself).
        let mut staged_files = Vec::new();
        let mut entries = tokio::fs::read_dir(&staging_dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            if entry.file_type().await?.is_file() {
                let name = entry.file_name().to_string_lossy().to_string();
                if name != STAGING_WAL_FILENAME {
                    staged_files.push(name);
                }
            }
        }

        let wal = StagingWal {
            table_name: self.table_name().to_string(),
            target_snapshot: target_snapshot.to_string(),
            staged_files,
            created_at: chrono::Utc::now().to_rfc3339(),
        };

        let wal_path = staging_dir.join(STAGING_WAL_FILENAME);
        let content = serde_json::to_string_pretty(&wal).map_err(|e| Error::Internal {
            table: self.table_name().to_string(),
            message: format!("Failed to serialize staging WAL: {e}"),
        })?;
        tokio::fs::write(&wal_path, content.as_bytes()).await?;

        // fsync the WAL file to ensure it is durable before we begin moving files.
        let file = tokio::fs::File::open(&wal_path).await?;
        file.sync_all().await?;

        tracing::debug!(
            "Wrote staging WAL for table {} with {} file(s) targeting snapshot {target_snapshot}",
            self.table_name(),
            wal.staged_files.len(),
        );

        Ok(())
    }

    /// Write the staging WAL on S3.
    async fn write_staging_wal_s3(&self, target_snapshot: &str) -> Result<()> {
        let config = self.require_object_store()?;

        let Some(staging_prefix) = self.snapshot_object_store_prefix(STAGING_DIR_NAME)? else {
            return Ok(());
        };

        // List staged files (exclude the WAL file itself).
        let objects: Vec<_> = config
            .store
            .list(Some(&staging_prefix))
            .try_collect()
            .await
            .map_err(|e| Error::ObjectStore {
                operation: "list staging objects for WAL",
                table: self.table_name().to_string(),
                source: e,
            })?;

        let staged_files: Vec<String> = objects
            .iter()
            .filter_map(|meta| {
                let name = meta
                    .location
                    .as_ref()
                    .strip_prefix(staging_prefix.as_ref())
                    .unwrap_or(meta.location.as_ref());
                if name == STAGING_WAL_FILENAME {
                    None
                } else {
                    Some(name.to_string())
                }
            })
            .collect();

        let wal = StagingWal {
            table_name: self.table_name().to_string(),
            target_snapshot: target_snapshot.to_string(),
            staged_files,
            created_at: chrono::Utc::now().to_rfc3339(),
        };

        let content = serde_json::to_string_pretty(&wal).map_err(|e| Error::Internal {
            table: self.table_name().to_string(),
            message: format!("Failed to serialize staging WAL: {e}"),
        })?;

        let wal_key =
            ObjectStorePath::from(format!("{}{STAGING_WAL_FILENAME}", staging_prefix.as_ref()));
        config
            .store
            .put(&wal_key, content.into())
            .await
            .map_err(|e| Error::ObjectStore {
                operation: "write staging WAL",
                table: self.table_name().to_string(),
                source: e,
            })?;

        tracing::debug!(
            "Wrote staging WAL (S3) for table {} with {} file(s) targeting snapshot {target_snapshot}",
            self.table_name(),
            wal.staged_files.len(),
        );

        Ok(())
    }

    /// Remove the staging WAL file after a successful move.
    ///
    /// This signals that all staged files have been moved successfully. If this
    /// removal fails, the WAL is stale (files already moved) and will be detected
    /// as a false positive on next open — harmless but logged.
    pub(crate) async fn remove_staging_wal(&self) -> Result<()> {
        if self.table_path().starts_with("s3://") {
            let config = self.require_object_store()?;
            if let Some(staging_prefix) = self.snapshot_object_store_prefix(STAGING_DIR_NAME)? {
                let wal_key = ObjectStorePath::from(format!(
                    "{}{STAGING_WAL_FILENAME}",
                    staging_prefix.as_ref()
                ));
                // Best-effort delete — if the key doesn't exist, that's fine.
                match config.store.delete(&wal_key).await {
                    Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                    Err(e) => {
                        tracing::warn!(
                            "Failed to remove staging WAL (S3) for table {}: {e}",
                            self.table_name(),
                        );
                    }
                }
            }
        } else {
            let staging_dir =
                Self::snapshot_dir_path(self.table_path(), self.table_id(), STAGING_DIR_NAME);
            let wal_path = staging_dir.join(STAGING_WAL_FILENAME);
            match tokio::fs::remove_file(&wal_path).await {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => {
                    tracing::warn!(
                        "Failed to remove staging WAL for table {}: {e}",
                        self.table_name(),
                    );
                }
            }
        }
        Ok(())
    }

    /// Ensure no incomplete write is pending before starting a new write.
    ///
    /// Checks for a leftover staging WAL, which indicates a previous staged
    /// append was interrupted during the file-move phase. Returns an error to
    /// block further writes until the inconsistency is resolved.
    ///
    /// # Errors
    ///
    /// Returns [`Error::IncompleteWrite`] if a staging WAL file is found.
    pub(crate) async fn ensure_no_incomplete_write(&self) -> Result<()> {
        let wal = if self.table_path().starts_with("s3://") {
            self.read_staging_wal_s3().await
        } else {
            self.read_staging_wal_local().await
        };

        if let Some((wal, wal_location)) = wal {
            // Automated recovery attempt will be implemented in the future — for now we just error with details to help the operator resolve the issue.

            return Err(Error::IncompleteWrite {
                table: self.table_name().to_string(),
                message: format!(
                    "A previous write was interrupted while moving {} file(s) to '{}' (started at {}). Some files may have been partially written and require manual resolution. The WAL file is located at '{wal_location}'.",
                    wal.staged_files.len(),
                    wal.target_snapshot,
                    wal.created_at,
                ),
            });
        }

        Ok(())
    }

    /// Read the staging WAL from local filesystem, if present.
    /// Returns the WAL data and the absolute path to the WAL file.
    async fn read_staging_wal_local(&self) -> Option<(StagingWal, String)> {
        let staging_dir =
            Self::snapshot_dir_path(self.table_path(), self.table_id(), STAGING_DIR_NAME);
        let wal_path = staging_dir.join(STAGING_WAL_FILENAME);
        let location = wal_path.to_string_lossy().to_string();
        match tokio::fs::read_to_string(&wal_path).await {
            Ok(content) => match serde_json::from_str::<StagingWal>(&content) {
                Ok(wal) => Some((wal, location)),
                Err(e) => {
                    tracing::warn!(
                        "Failed to parse staging WAL for table {}: {e}",
                        self.table_name(),
                    );
                    None
                }
            },
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(e) => {
                tracing::warn!(
                    "Failed to read staging WAL for table {}: {e}",
                    self.table_name(),
                );
                None
            }
        }
    }

    /// Read the staging WAL from S3, if present.
    /// Returns the WAL data and the S3 key of the WAL file.
    async fn read_staging_wal_s3(&self) -> Option<(StagingWal, String)> {
        let config = self.require_object_store().ok()?;
        let staging_prefix = self.snapshot_object_store_prefix(STAGING_DIR_NAME).ok()??;
        let wal_key =
            ObjectStorePath::from(format!("{}{STAGING_WAL_FILENAME}", staging_prefix.as_ref()));
        let location = wal_key.to_string();
        match config.store.get(&wal_key).await {
            Ok(result) => {
                let bytes = result.bytes().await.ok()?;
                let wal = serde_json::from_slice::<StagingWal>(&bytes).ok()?;
                Some((wal, location))
            }
            Err(object_store::Error::NotFound { .. }) => None,
            Err(e) => {
                tracing::warn!(
                    "Failed to read staging WAL (S3) for table {}: {e}",
                    self.table_name(),
                );
                None
            }
        }
    }
}
