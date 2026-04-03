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

use tokio::sync::OwnedMutexGuard;

use super::constants::STAGING_DIR_NAME;
use super::table::CayenneTableProvider;
use super::Result;
use crate::provider::Error;
use datafusion::execution::SendableRecordBatchStream;

/// A staged Cayenne append that is not made visible until `commit()` is called.
///
/// Data is written into the staging area and kept out of the active snapshot until
/// the caller decides whether to commit or roll back.
pub struct CayenneStagedAppend {
    table: CayenneTableProvider,
    _write_guard: OwnedMutexGuard<()>,
    row_count: u64,
}

impl CayenneStagedAppend {
    /// Returns the number of rows staged for commit.
    #[must_use]
    pub fn row_count(&self) -> u64 {
        self.row_count
    }

    /// Commits the staged append, making the new rows visible to readers.
    pub async fn commit(self) -> Result<u64> {
        self.table.write_staging_wal().await?;
        self.table.move_files_to_current_snapshot().await?;
        self.table.remove_staging_wal().await?;
        self.table.refresh_listing_table()?;
        Ok(self.row_count)
    }

    /// Discards the staged append and removes any staged files.
    pub async fn rollback(self) -> Result<()> {
        self.table.clear_staging_dir().await
    }
}

impl CayenneTableProvider {
    /// Stage an append into Cayenne without making the new rows visible.
    ///
    /// This MVP path intentionally supports only simple append semantics: no overwrite,
    /// no replacement writes, and no primary-key delete/upsert flows.
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

        if self.has_pending_deletions()? {
            return Err(Error::Unsupported {
                operation: "staged append for Cayenne tables with pending deletions",
            });
        }

        let (prepared_stream, delete_specs, deleted_pk_i64, deleted_row_keys) =
            self.prepare_stream_for_insert(data).await?;

        if !delete_specs.is_empty() || !deleted_pk_i64.is_empty() || !deleted_row_keys.is_empty() {
            return Err(Error::Unsupported {
                operation: "staged append for Cayenne upsert or on-conflict writes",
            });
        }

        self.clear_staging_dir().await?;

        let (row_count, _writer_ops) = self
            .write_to_snapshot(
                prepared_stream,
                self.target_file_size_bytes(),
                STAGING_DIR_NAME,
            )
            .await?;

        Ok(CayenneStagedAppend {
            table: self.clone_for_write(),
            _write_guard: write_guard,
            row_count,
        })
    }
}
