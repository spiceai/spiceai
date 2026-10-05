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

//! Atomic publication of a fully validated append snapshot.

use std::sync::Arc;
use std::time::Instant;

use tokio::sync::OwnedMutexGuard;
use turso_shared::{DEFAULT_CONCURRENT_WRITE_MAX_ATTEMPTS, retry_backoff_delay};

use super::column_stats::ColumnStatsAccumulator;
use super::context::CayenneContext;
use super::on_conflict::{OnConflictDeletions, PreparedOnConflictDeletionPublish};
use super::pk_index::PkDigestSet;
use super::table::{CayenneTableProvider, record_cayenne_write_phase};
use super::{Error, Result};
use crate::CayenneCatalog;

/// Owns the write lock until durable publication, visibility, and maintenance
/// bookkeeping are complete, including when the requesting task is cancelled.
pub(super) struct PreparedResolvedAppend {
    pub table: CayenneTableProvider,
    pub context: Arc<CayenneContext>,
    pub _write_guard: OwnedMutexGuard<()>,
    pub snapshot_id: String,
    pub rows: u64,
    pub stats: Arc<ColumnStatsAccumulator>,
    pub deletions: OnConflictDeletions,
    pub validated_keys: PkDigestSet,
    pub superseded: usize,
    pub into_empty_table: bool,
}

impl PreparedResolvedAppend {
    pub async fn commit(mut self) -> Result<u64> {
        let table_name = self.table.table_name().to_string();
        tokio::spawn(async move {
            let result = self.commit_inner().await;
            if result.is_err() {
                self.table.clear_cached_pk_keyset();
                super::staged_upsert::cleanup_orphan_snapshot_dir(&self.table, &self.snapshot_id)
                    .await;
            }
            result
        })
        .await
        .map_err(|source| Error::TaskPanicked {
            table: table_name,
            source,
        })?
    }

    async fn commit_inner(&mut self) -> Result<u64> {
        let catalog = self
            .table
            .catalog()
            .as_any()
            .downcast_ref::<CayenneCatalog>()
            .ok_or(Error::Unsupported {
                operation: "atomic append with a non-Cayenne metadata catalog",
            })?;
        self.table
            .sync_local_snapshot_dir(&self.snapshot_id)
            .await?;
        let lock_start = Instant::now();
        let _visibility = self.table.visibility_lock_arc().lock_owned().await;
        let _fence = self.table.lock_listing_fence_write_owned().await;
        record_cayenne_write_phase(self.table.table_name(), "publish_lock_wait", lock_start);

        let publish_start = Instant::now();
        let mut prepared = self
            .table
            .prepare_on_conflict_deletions_for_staged_snapshot(
                std::mem::take(&mut self.deletions),
                self.snapshot_id.clone(),
                true,
            )
            .await?;
        prepared.publish_as_protected_snapshot = true;
        // The replacement snapshot and its inline tombstone become durable in
        // the same transaction. Restart must not depend on a later flag update.
        if let Some(payload) = prepared.durable_payload.as_mut()
            && let Some(tombstone) = payload.inline_tombstone.as_mut()
        {
            tombstone.published = true;
        }
        let sequence = prepared.snapshot_sequence;
        let reserved_delta = self.table.reserve_live_rows_delta();
        commit_metadata(catalog, &mut prepared).await?;

        let cas_start = Instant::now();
        self.table.publish_prepared_on_conflict_deletions(prepared);
        self.table.feed_staged_ivm_under_fence(None);
        let published_delta = reserved_delta.published();
        let retention = self.table.has_retention_delete_filters();
        let delta = i64::try_from(self.rows)
            .unwrap_or(i64::MAX)
            .saturating_sub(i64::try_from(self.superseded).unwrap_or(i64::MAX));
        self.table.schedule_post_write_maintenance(
            Some(Arc::clone(&self.stats)),
            true,
            retention,
            delta,
            published_delta,
        );
        if retention || self.into_empty_table {
            self.table.clear_cached_pk_keyset();
        } else {
            self.table
                .record_file_pk_keys(&self.validated_keys, sequence);
        }
        record_cayenne_write_phase(self.table.table_name(), "publish_cas", cas_start);
        record_cayenne_write_phase(self.table.table_name(), "publish", publish_start);
        self.context.record_publish_latency(publish_start.elapsed());
        Ok(self.rows)
    }
}

async fn commit_metadata(
    catalog: &CayenneCatalog,
    prepared: &mut PreparedOnConflictDeletionPublish,
) -> Result<()> {
    for attempt in 1..=DEFAULT_CONCURRENT_WRITE_MAX_ATTEMPTS {
        let mut txn = catalog.begin_transaction().await?;
        if let Err(error) = catalog
            .apply_prepared_on_conflict_in_txn(txn.as_mut(), prepared)
            .await
        {
            let _ = txn.rollback().await;
            return Err(error.into());
        }
        match txn.commit().await {
            Ok(()) => {
                prepared.mark_catalog_committed();
                return Ok(());
            }
            Err(error)
                if attempt < DEFAULT_CONCURRENT_WRITE_MAX_ATTEMPTS
                    && crate::is_retryable_write_conflict(&error) =>
            {
                tokio::time::sleep(retry_backoff_delay(attempt)).await;
            }
            Err(error) => return Err(error.into()),
        }
    }
    Err(Error::WriteConflict {
        table: prepared.table.table_name().to_string(),
    })
}
