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
use crate::accelerated::SnapshotCreateTrigger;
use crate::accelerated::caching::is_reserved_caching_column;
use crate::accelerated::refresh::Refresh;
use crate::accelerated::refresh_completion::{RefreshCompletion, RefreshCompletionOutcome};
use crate::accelerated::write::{CayenneWriteTarget, dual_write::extract_cayenne_write_target};
use arrow_schema::{FieldRef, Schema, SchemaRef};
use data_accelerator_api::DataAccelerator;
use data_accelerator_api::ReloadProviderFactory;
use data_accelerator_api::swappable::SwappableTableProvider;
use data_connector_api::accelerated::RefreshRequester;
use datafusion::common::TableReference;
use datafusion::datasource::TableProvider;
use runtime_acceleration::acceleration_source::AccelerationSource;
use runtime_acceleration::dataset_checkpoint::DatasetCheckpointer;
use runtime_acceleration::snapshot::notifications::Subscription;
use runtime_acceleration::snapshot::{
    ForceCreate, SnapshotLockGuard, SnapshotManager, SnapshotUploadError,
    metrics as snapshot_metrics,
};
use runtime_async::is_shutdown_cancellation;
use runtime_status::{RuntimeStatus, WaitOutcome};
use snafu::{ResultExt, Snafu};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::time::Duration;
use tokio::sync::{Mutex, RwLock};
use tokio::time::interval;
use util::{RetryError, retry, retry_strategy::RetryBackoffBuilder};

/// Per-dataset state required to drive `refresh_mode: snapshot`.
///
/// This bundle is built once during dataset registration and threaded down
/// through `Refresher` -> `RefreshTaskBuilder` -> `RefreshTask`. The refresh
/// task uses it on every tick to:
///   1. Compare the remote `current_snapshot_id` against `current_snapshot_id`.
///   2. Download and reload only when a strictly newer snapshot is available.
///   3. Atomically swap the live `TableProvider` via `swappable_provider`.
#[derive(Clone)]
pub struct SnapshotRefreshState {
    pub manager: Arc<SnapshotManager>,
    pub accelerator: Arc<dyn DataAccelerator>,
    pub source: Arc<dyn AccelerationSource>,
    pub swappable_provider: Arc<SwappableTableProvider>,
    /// Factory that re-runs `create_accelerator_table` for this dataset to
    /// build a fresh provider over the on-disk snapshot file.
    pub provider_factory: ReloadProviderFactory,
    /// The currently-loaded snapshot id, if any. `None` means no snapshot has
    /// been loaded yet for this dataset (e.g. fresh start with no bootstrap).
    /// Wrapped in a sync `Mutex` because updates are infrequent (once per
    /// successful reload) and the inner `Option<u64>` is `Copy` so reads are
    /// trivial. Snapshot ids are not constrained: id `0` is a valid first
    /// snapshot, so `Option<u64>` is the correct representation rather than
    /// using a sentinel value.
    pub current_snapshot_id: Arc<StdMutex<Option<u64>>>,
    /// The snapshot metadata `ETag` of the last poll that completed: a reload that was
    /// swapped in, or a check that found nothing newer. The next poll reads the metadata
    /// conditionally on it and skips when it is unchanged. `None` until the first such poll.
    pub metadata_e_tag: Arc<StdMutex<Option<String>>>,
}

impl std::fmt::Debug for SnapshotRefreshState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let id = self.current_snapshot_id.lock().map_or(None, |g| *g);
        f.debug_struct("SnapshotRefreshState")
            .field("current_snapshot_id", &id)
            .finish_non_exhaustive()
    }
}

impl SnapshotRefreshState {
    /// Returns the currently-loaded snapshot id, or `None` if no snapshot has
    /// been loaded yet.
    #[must_use]
    pub fn current_loaded_id(&self) -> Option<u64> {
        self.current_snapshot_id
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .copied()
    }

    /// Records `snapshot_id` as the most recently loaded snapshot id, read from the
    /// metadata with `metadata_e_tag`.
    pub fn set_current_loaded_id(&self, snapshot_id: u64, metadata_e_tag: Option<String>) {
        let mut guard = self
            .current_snapshot_id
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *guard = Some(snapshot_id);
        drop(guard);
        self.record_metadata_e_tag(metadata_e_tag);
    }

    /// Returns the snapshot metadata `ETag` of the last completed poll.
    #[must_use]
    pub fn metadata_e_tag(&self) -> Option<String> {
        self.metadata_e_tag
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Records the snapshot metadata `ETag` of a poll that completed.
    pub fn record_metadata_e_tag(&self, metadata_e_tag: Option<String>) {
        *self
            .metadata_e_tag
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = metadata_e_tag;
    }
}

/// Reloads a `refresh_mode: snapshot` table each time its snapshot location
/// announces a snapshot of it newer than the one it has loaded or last asked
/// for. Returns when the table stops accepting refreshes.
///
/// A request cancels the refresh in flight, so the loop waits for the reload it
/// asked for before asking again. Otherwise a writer that publishes faster than
/// this reader downloads would restart the download each time and the reader
/// would never finish one. Announcements that arrive meanwhile coalesce, and
/// the latest is acted on once the reload lands. If the reload fails, the
/// scheduled refresh (`refresh_check_interval`) keeps trying, and the loop
/// resumes after the next refresh that succeeds.
pub async fn reload_on_snapshot_notifications(
    mut subscription: Subscription,
    loaded_snapshot_id: impl Fn() -> Option<u64> + Send,
    requester: Arc<dyn RefreshRequester>,
    completion: RefreshCompletion,
) {
    let mut last_requested = None;
    while let Some(announced) = subscription.next_snapshot().await {
        let known = last_requested.max(loaded_snapshot_id());
        if known.is_some_and(|known| announced <= known) {
            continue;
        }
        last_requested = Some(announced);
        let reloaded = completion.next();
        if requester.request_refresh().await.is_err()
            || reloaded.wait().await == RefreshCompletionOutcome::Abandoned
        {
            return;
        }
    }
}

#[derive(Debug, Clone)]
pub struct SnapshotCreationConfig {
    pub manager: Arc<SnapshotManager>,
    pub create_trigger: SnapshotCreateTrigger,
}

impl SnapshotCreationConfig {
    #[must_use]
    pub fn new(manager: Arc<SnapshotManager>, create_trigger: SnapshotCreateTrigger) -> Self {
        Self {
            manager,
            create_trigger,
        }
    }
}

pub type SnapshotCallback =
    Arc<Mutex<Box<dyn FnMut() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send>>>;

/// Builds the canonical schema persisted by the dataset checkpoint (and recorded in
/// snapshot metadata): the accelerator's field order — type widenings applied in place,
/// added columns appended at the end — with the hidden `__spice_cache_namespace`
/// storage column removed. Federated source columns that the accelerator does not
/// materialize (e.g. columns projected away by `refresh_sql`) are appended after the
/// accelerator fields so the persisted field SET stays equal to the full source schema.
///
/// Field definitions are taken from the federated source schema by name where
/// available, so engine-internal type rewrites (e.g. `DuckDB`'s timestamptz microsecond
/// normalization or dictionary unwrapping) don't leak into the persisted schema and
/// trigger false schema-change detection on the next restart. The accelerator order is
/// what must be durable: engines can only `ADD COLUMN` at the end, so persisting source
/// order after an evolution would positionally transpose columns on restart.
///
/// The full field set must be retained because `FederatedTable::new` gates restart-time
/// registration on a name-based `schema_difference` between this checkpoint and the full
/// source provider schema — persisting only the projected (accelerator) subset would make
/// `refresh_sql` + file-accelerated datasets defer forever under the default `block`
/// policy. `schema_difference` is order-insensitive, so the accelerator-first ordering is
/// safe for that gate while remaining load-bearing after an evolution.
#[must_use]
pub(crate) fn canonical_checkpoint_schema(
    accelerator_schema: &SchemaRef,
    federated_schema: &SchemaRef,
) -> SchemaRef {
    let mut fields: Vec<FieldRef> = accelerator_schema
        .fields()
        .iter()
        .filter(|field| !is_reserved_caching_column(field.name()))
        .map(|field| {
            federated_schema.field_with_name(field.name()).map_or_else(
                |_| Arc::clone(field),
                |source_field| Arc::new(source_field.clone()),
            )
        })
        .collect();

    // Append any federated source columns the accelerator doesn't materialize
    // (refresh_sql projections), preserving the full source field set so the
    // restart-time block gate's name-based comparison matches.
    for source_field in federated_schema.fields() {
        if !fields.iter().any(|f| f.name() == source_field.name()) {
            fields.push(Arc::new(source_field.as_ref().clone()));
        }
    }

    Arc::new(Schema::new_with_metadata(
        fields,
        federated_schema.metadata().clone(),
    ))
}

/// Like [`canonical_checkpoint_schema`], but prefers the ACCELERATOR's own field
/// definition for a column whose type or nullability has diverged from the
/// federated source.
///
/// Used when re-deriving the checkpoint schema at checkpoint time: a live
/// (in-place) widening evolution of the accelerator (e.g. Cayenne CDC widening
/// `Int32` -> `Int64`, or relaxing `NOT NULL`) moves the engine table ahead of
/// the start-time federated schema. Preferring the source def there — as
/// `canonical_checkpoint_schema` does — would revert the persisted checkpoint to
/// the older, narrower type and desync it from the live engine table. For
/// unchanged columns the source def is kept (source-accurate nullability /
/// encoding), so this is byte-identical to `canonical_checkpoint_schema` when
/// nothing has evolved. Non-materialized federated (`refresh_sql`) columns are
/// still appended.
fn live_accelerator_checkpoint_schema(
    accelerator_schema: &SchemaRef,
    federated_schema: &SchemaRef,
) -> SchemaRef {
    let mut fields: Vec<FieldRef> = accelerator_schema
        .fields()
        .iter()
        .filter(|field| !is_reserved_caching_column(field.name()))
        .map(|field| {
            federated_schema.field_with_name(field.name()).map_or_else(
                |_| Arc::clone(field),
                |source_field| {
                    if source_field.data_type() == field.data_type()
                        && source_field.is_nullable() == field.is_nullable()
                    {
                        Arc::new(source_field.clone())
                    } else {
                        Arc::clone(field)
                    }
                },
            )
        })
        .collect();

    for source_field in federated_schema.fields() {
        if !fields.iter().any(|f| f.name() == source_field.name()) {
            fields.push(Arc::new(source_field.as_ref().clone()));
        }
    }

    Arc::new(Schema::new_with_metadata(
        fields,
        federated_schema.metadata().clone(),
    ))
}

/// Spawns a task that periodically creates snapshots at the specified interval.
///
/// The task uses the checkpointer's `last_checkpoint_time()` to determine when the next
/// snapshot should be created:
/// - If `snapshots_create_interval` has passed since the last checkpoint, create immediately
/// - Otherwise, schedule the first snapshot at `last_checkpoint_time + snapshots_create_interval`
///
/// If no previous checkpoint exists, a snapshot is created immediately after the runtime is ready.
#[expect(clippy::too_many_arguments)]
pub fn spawn_snapshot_interval_task(
    snapshots_create_interval: Option<Duration>,
    checkpointer: Option<Arc<dyn DatasetCheckpointer>>,
    snapshot_manager: Option<Arc<SnapshotManager>>,
    accelerator_write_mutex: Arc<Mutex<()>>,
    dataset_name: TableReference,
    checkpoint_schema: Arc<Schema>,
    federated_schema: Arc<Schema>,
    runtime_status: Arc<RuntimeStatus>,
    bootstrap_status: data_accelerator_api::BootstrapStatus,
    last_updated_at: Arc<AtomicI64>,
    accelerator: Option<Arc<dyn TableProvider>>,
    refresh: Arc<RwLock<Refresh>>,
) -> Option<tokio::task::JoinHandle<()>> {
    let interval_duration = snapshots_create_interval?;
    let checkpointer = checkpointer?;
    let snapshot_manager = snapshot_manager?;

    tracing::info!(
        "Snapshots for dataset {dataset_name} will be created every {}s",
        interval_duration.as_secs()
    );

    Some(tokio::spawn(async move {
        // Wait for the runtime to become ready. A shutdown that starts first
        // means the runtime never became ready, so there is nothing to snapshot.
        if runtime_status.wait_for_ready().await == WaitOutcome::ShuttingDown {
            return;
        }

        // Determine the initial delay based on last checkpoint time
        let initial_delay = if bootstrap_status.is_bootstrapped() {
            match checkpointer.last_checkpoint_time().await {
                Ok(Some(last_checkpoint)) => {
                    let elapsed = last_checkpoint.elapsed().unwrap_or(Duration::ZERO);
                    if elapsed >= interval_duration {
                        Duration::ZERO
                    } else {
                        interval_duration
                            .checked_sub(elapsed)
                            .unwrap_or(Duration::ZERO)
                    }
                }
                Ok(None) | Err(_) => Duration::ZERO,
            }
        } else {
            Duration::ZERO
        };

        if !initial_delay.is_zero() {
            tokio::time::sleep(initial_delay).await;
        }

        let refresh_sql = refresh
            .read()
            .await
            .sql
            .as_ref()
            .map(super::refresh::RefreshSQL::to_sql);
        create_checkpoint_and_snapshot(
            &checkpointer,
            Some(&snapshot_manager),
            &checkpoint_schema,
            &accelerator_write_mutex,
            &dataset_name,
            &last_updated_at,
            // Force creation when interval already elapsed.
            // Even though this may create a snapshot identical to the last one, we do this to avoid
            // losing snapshots due to potential object storage retention policy.
            // Consider use case: periodic
            ForceCreate(initial_delay.is_zero()),
            accelerator.as_ref(),
            Some(&federated_schema),
            refresh_sql.as_deref(),
        )
        .await;

        let mut ticker = interval(interval_duration);
        // Consume the first tick which returns immediately per tokio::time::interval behavior
        ticker.tick().await;

        loop {
            // Wait for the next snapshot interval (accounting for time spent during previous snapshot creation)
            ticker.tick().await;

            let refresh_sql = refresh
                .read()
                .await
                .sql
                .as_ref()
                .map(super::refresh::RefreshSQL::to_sql);
            create_checkpoint_and_snapshot(
                &checkpointer,
                Some(&snapshot_manager),
                &checkpoint_schema,
                &accelerator_write_mutex,
                &dataset_name,
                &last_updated_at,
                ForceCreate(false),
                accelerator.as_ref(),
                Some(&federated_schema),
                refresh_sql.as_deref(),
            )
            .await;
        }
    }))
}

/// Creates a callback that triggers snapshot creation after a specified number of batch updates.
///
/// Batch counting starts after runtime readiness and the initial snapshot attempt.
/// The returned task belongs to the table generation and must be stopped and joined
/// before its storage can be removed or rebound.
#[expect(clippy::too_many_arguments)]
pub fn create_periodic_snapshot_callback(
    batches: i64,
    checkpointer: Option<Arc<dyn DatasetCheckpointer>>,
    snapshot_manager: Option<Arc<SnapshotManager>>,
    accelerator_write_mutex: Arc<Mutex<()>>,
    dataset_name: &TableReference,
    checkpoint_schema: Arc<Schema>,
    federated_schema: Arc<Schema>,
    runtime_status: Arc<RuntimeStatus>,
    bootstrap_status: data_accelerator_api::BootstrapStatus,
    last_updated_at: Arc<AtomicI64>,
    accelerator: Option<Arc<dyn TableProvider>>,
    refresh: Arc<RwLock<Refresh>>,
) -> Option<(tokio::task::JoinHandle<()>, SnapshotCallback)> {
    match (checkpointer, snapshot_manager) {
        (Some(checkpointer), Some(snapshot_manager)) => {
            let dataset_name = dataset_name.clone();

            tracing::info!(
                "Snapshots for dataset {dataset_name} will be created every {batches} batch updates"
            );

            // Track number of processed batches since last snapshot
            let batches_processed = Arc::new(RwLock::new(0i64));

            // Gates when checkpoint counting can start after runtime is ready.
            // Set to true after the initial snapshot task completes (regardless of success).
            let checkpoint_counting_enabled = Arc::new(AtomicBool::new(false));

            // Spawn a task to create initial snapshot once runtime is ready
            let checkpoint_counting_enabled_clone = Arc::clone(&checkpoint_counting_enabled);
            let dataset_name_clone = dataset_name.clone();
            let last_updated_at_clone = Arc::clone(&last_updated_at);
            let checkpointer_clone = Arc::clone(&checkpointer);
            let snapshot_manager_clone = Arc::clone(&snapshot_manager);
            let checkpoint_schema_clone = Arc::clone(&checkpoint_schema);
            let federated_schema_clone = Arc::clone(&federated_schema);
            let accelerator_write_mutex_clone = Arc::clone(&accelerator_write_mutex);
            let accelerator_clone = accelerator.clone();
            let refresh_clone = Arc::clone(&refresh);
            let initial_snapshot = tokio::spawn(async move {
                if runtime_status.wait_for_ready().await == WaitOutcome::ShuttingDown {
                    return;
                }
                if !bootstrap_status.is_bootstrapped() {
                    let refresh_sql = refresh_clone
                        .read()
                        .await
                        .sql
                        .as_ref()
                        .map(super::refresh::RefreshSQL::to_sql);
                    create_checkpoint_and_snapshot(
                        &checkpointer_clone,
                        Some(&snapshot_manager_clone),
                        &checkpoint_schema_clone,
                        &accelerator_write_mutex_clone,
                        &dataset_name_clone,
                        &last_updated_at_clone,
                        ForceCreate(true),
                        accelerator_clone.as_ref(),
                        Some(&federated_schema_clone),
                        refresh_sql.as_deref(),
                    )
                    .await;
                }
                checkpoint_counting_enabled_clone.store(true, Ordering::Release);
                tracing::debug!(
                    "Batch-based snapshot counting for {dataset_name_clone} starting after runtime ready"
                );
            });

            let callback = Arc::new(Mutex::new(Box::new(move || {
                let checkpointer = Arc::clone(&checkpointer);
                let snapshot_manager = Arc::clone(&snapshot_manager);
                let accelerator_write_mutex = Arc::clone(&accelerator_write_mutex);
                let batches_processed = Arc::clone(&batches_processed);
                let checkpoint_schema = Arc::<Schema>::clone(&checkpoint_schema);
                let federated_schema = Arc::<Schema>::clone(&federated_schema);
                let dataset_name = dataset_name.clone();
                let checkpoint_counting_enabled = Arc::clone(&checkpoint_counting_enabled);
                let last_updated_at = Arc::clone(&last_updated_at);
                let accelerator = accelerator.clone();
                let refresh = Arc::clone(&refresh);

                Box::pin(async move {
                    let mut batches_processed_value = batches_processed.write().await;

                    // Only count batches after checkpoint counting is enabled
                    if !checkpoint_counting_enabled.load(Ordering::Acquire) {
                        return;
                    }

                    *batches_processed_value += 1;
                    if *batches_processed_value >= batches {
                        *batches_processed_value = 0;

                        let refresh_sql = refresh
                            .read()
                            .await
                            .sql
                            .as_ref()
                            .map(super::refresh::RefreshSQL::to_sql);
                        create_checkpoint_and_snapshot(
                            &checkpointer,
                            Some(&snapshot_manager),
                            &checkpoint_schema,
                            &accelerator_write_mutex,
                            &dataset_name,
                            &last_updated_at,
                            ForceCreate(false),
                            accelerator.as_ref(),
                            Some(&federated_schema),
                            refresh_sql.as_deref(),
                        )
                        .await;
                    }
                }) as Pin<Box<dyn Future<Output = ()> + Send>>
            })
                as Box<dyn FnMut() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send>));

            Some((initial_snapshot, callback))
        }
        _ => None,
    }
}

/// Retries after a failed attempt; each one re-takes the write lock.
const SNAPSHOT_MAX_RETRIES: usize = 3;

/// One attempt of [`create_checkpoint_and_snapshot`], by the step that failed.
#[derive(Debug, Snafu)]
enum SnapshotAttemptError {
    /// Step 1: writing the dataset checkpoint (schema, refresh SQL) to the local store.
    #[snafu(display("Failed to checkpoint dataset {dataset}: {source}"))]
    Checkpoint {
        dataset: TableReference,
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    /// Step 2: archiving the acceleration and uploading it to the snapshot store.
    #[snafu(display("{source}"))]
    Upload { source: SnapshotUploadError },
}

impl SnapshotAttemptError {
    fn is_retriable(&self) -> bool {
        match self {
            Self::Checkpoint { .. } => true,
            Self::Upload { source } => source.is_retriable(),
        }
    }
}

/// Checkpoints the dataset and creates a snapshot, retrying a failed attempt with
/// backoff. The lock is released between attempts, so a retry re-derives everything
/// (checkpoint, row count, metastore slice, archive) from the table as of its own lock.
#[expect(clippy::too_many_arguments)]
pub async fn create_checkpoint_and_snapshot(
    checkpointer: &Arc<dyn DatasetCheckpointer>,
    snapshot_manager: Option<&Arc<SnapshotManager>>,
    checkpoint_schema: &Arc<Schema>,
    accelerator_write_mutex: &Arc<Mutex<()>>,
    dataset_name: &TableReference,
    last_updated_at: &Arc<AtomicI64>,
    force_create: ForceCreate,
    accelerator: Option<&Arc<dyn TableProvider>>,
    federated_schema: Option<&Arc<Schema>>,
    refresh_sql: Option<&str>,
) {
    let backoff = RetryBackoffBuilder::new()
        .max_retries(Some(SNAPSHOT_MAX_RETRIES))
        .build();
    let result = retry(backoff, || async {
        create_checkpoint_and_snapshot_once(
            checkpointer,
            snapshot_manager,
            checkpoint_schema,
            accelerator_write_mutex,
            dataset_name,
            last_updated_at,
            force_create,
            accelerator,
            federated_schema,
            refresh_sql,
        )
        .await
        .map_err(|e| {
            if !e.is_retriable() || is_shutdown_cancellation(&e) {
                return RetryError::permanent(e);
            }
            tracing::debug!(dataset = %dataset_name, error = %e, "Snapshot attempt failed, retrying");
            RetryError::transient(e)
        })
    })
    .await;

    match result {
        Ok(()) => {}
        // Expected under shutdown; reporting it at `warn` makes a clean stop look like a failure.
        Err(e) if is_shutdown_cancellation(&e) => {
            tracing::debug!(dataset = %dataset_name, error = %e, "Did not create snapshot: the runtime is shutting down");
        }
        Err(e @ SnapshotAttemptError::Checkpoint { .. }) => tracing::warn!("{e}"),
        Err(e) => {
            snapshot_metrics::record_snapshot_failure(&dataset_name.to_string());
            tracing::warn!(dataset = %dataset_name, error = %e, "Failed to create snapshot");
        }
    }
}

#[expect(clippy::too_many_arguments)]
async fn create_checkpoint_and_snapshot_once(
    checkpointer: &Arc<dyn DatasetCheckpointer>,
    snapshot_manager: Option<&Arc<SnapshotManager>>,
    checkpoint_schema: &Arc<Schema>,
    accelerator_write_mutex: &Arc<Mutex<()>>,
    dataset_name: &TableReference,
    last_updated_at: &Arc<AtomicI64>,
    force_create: ForceCreate,
    accelerator: Option<&Arc<dyn TableProvider>>,
    federated_schema: Option<&Arc<Schema>>,
    refresh_sql: Option<&str>,
) -> Result<(), SnapshotAttemptError> {
    // Keeps Cayenne maintenance from deleting files until the archive is written.
    // Taken before the write lock, so writers never wait on a sweep batch.
    let file_deletion_hold = match accelerator.and_then(extract_cayenne_write_target) {
        Some(CayenneWriteTarget::Staged(table)) => Some(table.hold_file_deletions().await),
        _ => None,
    };
    let lock_guard = Arc::clone(accelerator_write_mutex).lock_owned().await;
    // Re-derive the checkpoint schema from the LIVE accelerator schema when both
    // the accelerator and the federated (source) schema are available, so an
    // in-place / live schema evolution (e.g. Cayenne CDC) that widened the
    // accelerator while the runtime is up is persisted to the checkpoint and
    // snapshot metadata — rather than overwriting it with the schema captured at
    // refresher start. Falls back to the precomputed `checkpoint_schema`, and is
    // byte-identical when the accelerator schema has not changed since start.
    let live_checkpoint_schema;
    let checkpoint_schema = if let (Some(acc), Some(fed)) = (accelerator, federated_schema) {
        live_checkpoint_schema = live_accelerator_checkpoint_schema(&acc.schema(), fed);
        &live_checkpoint_schema
    } else {
        checkpoint_schema
    };
    checkpointer
        .checkpoint(checkpoint_schema, refresh_sql)
        .await
        .context(CheckpointSnafu {
            dataset: dataset_name.clone(),
        })?;

    let Some(snapshot_manager) = snapshot_manager else {
        return Ok(());
    };
    let updated_at = match last_updated_at.load(Ordering::Acquire) {
        0 => None,
        i => Some(i),
    };
    // Counted under the write lock so it describes the archived data.
    let row_count = if let Some(accelerator) = accelerator {
        get_row_count(accelerator, dataset_name).await
    } else {
        None
    };
    snapshot_manager
        .create_snapshot(
            checkpoint_schema,
            SnapshotLockGuard::from(lock_guard).with(file_deletion_hold),
            updated_at,
            row_count,
            force_create,
        )
        .await
        .map(|_| ())
        .context(UploadSnafu)
}

/// Gets the row count from the accelerator using the `DataFrame` API.
///
/// Returns `None` if the row count cannot be determined (e.g., due to errors).
async fn get_row_count(
    accelerator: &Arc<dyn TableProvider>,
    dataset_name: &TableReference,
) -> Option<u64> {
    let ctx = util::session_state::session_context();
    let table_name = dataset_name.table();

    if ctx
        .register_table(table_name, Arc::clone(accelerator))
        .is_err()
    {
        tracing::debug!(dataset = %dataset_name, "Failed to register accelerator table for row count query");
        return None;
    }

    match ctx.table(table_name).await {
        Ok(df) => match df.count().await {
            Ok(count) => {
                if let Ok(row_count) = u64::try_from(count) {
                    Some(row_count)
                } else {
                    tracing::debug!(dataset = %dataset_name, "Row count for snapshot exceeds u64::MAX; proceeding without it");
                    None
                }
            }
            Err(e) => {
                tracing::debug!(dataset = %dataset_name, error = %e, "Failed to get row count for snapshot; proceeding without it");
                None
            }
        },
        Err(e) => {
            tracing::debug!(dataset = %dataset_name, error = %e, "Failed to get DataFrame for row count query");
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::accelerated::refresh_completion::RefreshRequestId;
    use arrow_schema::{DataType, Field};
    use async_trait::async_trait;
    use data_connector_api::accelerated::RefreshRequestError;
    use runtime_acceleration::dataset_checkpoint::Result as CheckpointResult;
    use runtime_acceleration::snapshot::notifications::TestAnnouncer;
    use std::sync::atomic::AtomicUsize;
    use std::time::SystemTime;
    use tokio::task::JoinHandle;

    /// Stands in for a table's refresh loop. Each request is a reload of the
    /// snapshot published when it was requested; it completes at once unless
    /// held, like a download still in progress.
    #[derive(Debug, Default)]
    struct FakeRefresh {
        completion: RefreshCompletion,
        requests: AtomicUsize,
        published: StdMutex<Option<u64>>,
        loaded: Arc<StdMutex<Option<u64>>>,
        held: StdMutex<Vec<(RefreshRequestId, Option<u64>)>>,
        hold: AtomicBool,
        gone: AtomicBool,
    }

    #[async_trait::async_trait]
    impl RefreshRequester for FakeRefresh {
        async fn request_refresh(&self) -> Result<(), RefreshRequestError> {
            if self.gone.load(Ordering::SeqCst) {
                return Err(RefreshRequestError::TableGone);
            }
            self.requests.fetch_add(1, Ordering::SeqCst);
            let id = self.completion.issue();
            let snapshot = *lock(&self.published);
            if self.hold.load(Ordering::SeqCst) {
                lock(&self.held).push((id, snapshot));
            } else {
                self.finish(id, snapshot);
            }
            Ok(())
        }
    }

    impl FakeRefresh {
        fn publish(&self, snapshot_id: u64) {
            *lock(&self.published) = Some(snapshot_id);
        }

        fn finish(&self, id: RefreshRequestId, snapshot: Option<u64>) {
            *lock(&self.loaded) = snapshot;
            self.completion.record(id);
        }

        /// Completes the reloads held so far and stops holding new ones.
        fn release(&self) {
            self.hold.store(false, Ordering::SeqCst);
            let held = std::mem::take(&mut *lock(&self.held));
            for (id, snapshot) in held {
                self.finish(id, snapshot);
            }
        }

        fn requests(&self) -> usize {
            self.requests.load(Ordering::SeqCst)
        }

        fn loaded(&self) -> Option<u64> {
            *lock(&self.loaded)
        }

        /// Runs the reload loop for `dataset` against this fake.
        fn spawn_loop(self: &Arc<Self>, dataset: &str) -> (TestAnnouncer, JoinHandle<()>) {
            let (announcer, subscription) = TestAnnouncer::subscribe(dataset);
            let loaded = Arc::clone(&self.loaded);
            let task = tokio::spawn(reload_on_snapshot_notifications(
                subscription,
                move || *lock(&loaded),
                Arc::clone(self) as Arc<dyn RefreshRequester>,
                self.completion.clone(),
            ));
            (announcer, task)
        }
    }

    fn lock<T>(mutex: &StdMutex<T>) -> std::sync::MutexGuard<'_, T> {
        mutex
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Lets the loop run to its next wait. The test runtime is single-threaded,
    /// so a bounded number of yields is enough for it to act on an announcement.
    async fn settle() {
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test]
    async fn each_new_snapshot_is_requested_once() {
        let fake = Arc::new(FakeRefresh::default());
        let (announcer, task) = fake.spawn_loop("orders");

        fake.publish(1);
        announcer.announce("orders", 1);
        settle().await;
        assert_eq!(fake.requests(), 1);
        assert_eq!(fake.loaded(), Some(1));

        // Another dataset's publish repeats this dataset's unchanged snapshot.
        announcer.announce("customers", 9);
        settle().await;
        assert_eq!(
            fake.requests(),
            1,
            "an unchanged snapshot is not requested again"
        );

        fake.publish(2);
        announcer.announce("orders", 2);
        settle().await;
        assert_eq!(fake.requests(), 2);
        assert_eq!(fake.loaded(), Some(2));
        task.abort();
    }

    #[tokio::test]
    async fn a_newer_snapshot_waits_for_the_reload_in_flight() {
        let fake = Arc::new(FakeRefresh::default());
        fake.hold.store(true, Ordering::SeqCst);
        let (announcer, task) = fake.spawn_loop("orders");

        fake.publish(1);
        announcer.announce("orders", 1);
        settle().await;
        assert_eq!(fake.requests(), 1);

        // A request would cancel the reload in flight, so none is made yet.
        fake.publish(2);
        announcer.announce("orders", 2);
        settle().await;
        assert_eq!(
            fake.requests(),
            1,
            "a newer snapshot must not restart the reload in flight"
        );

        // Once snapshot 1 lands, the coalesced announcement of 2 is acted on.
        fake.release();
        settle().await;
        assert_eq!(fake.requests(), 2);
        assert_eq!(fake.loaded(), Some(2));
        task.abort();
    }

    #[tokio::test]
    async fn a_snapshot_already_loaded_is_not_requested() {
        let fake = Arc::new(FakeRefresh::default());
        *lock(&fake.loaded) = Some(3);
        let (announcer, task) = fake.spawn_loop("orders");

        announcer.announce("orders", 3);
        settle().await;
        assert_eq!(fake.requests(), 0);

        fake.publish(4);
        announcer.announce("orders", 4);
        settle().await;
        assert_eq!(fake.requests(), 1);
        task.abort();
    }

    #[tokio::test]
    async fn the_loop_ends_when_the_table_stops_accepting_refreshes() {
        let fake = Arc::new(FakeRefresh::default());
        fake.gone.store(true, Ordering::SeqCst);
        let (announcer, task) = fake.spawn_loop("orders");

        announcer.announce("orders", 1);
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("the loop must end once the table is gone")
            .expect("the loop must not panic");
    }

    /// Fails the first `failures` checkpoints, then succeeds.
    struct FlakyCheckpointer {
        failures: usize,
        calls: AtomicUsize,
    }

    #[async_trait]
    impl DatasetCheckpointer for FlakyCheckpointer {
        async fn exists(&self) -> bool {
            true
        }
        async fn checkpoint(&self, _: &SchemaRef, _: Option<&str>) -> CheckpointResult<()> {
            if self.calls.fetch_add(1, Ordering::SeqCst) < self.failures {
                return Err("database is locked".into());
            }
            Ok(())
        }
        async fn get_schema(&self) -> CheckpointResult<Option<SchemaRef>> {
            Ok(None)
        }
        async fn last_checkpoint_time(&self) -> CheckpointResult<Option<SystemTime>> {
            Ok(None)
        }
        async fn get_refresh_sql(&self) -> CheckpointResult<Option<String>> {
            Ok(None)
        }
        async fn set_schema(&self, _: &SchemaRef) -> CheckpointResult<()> {
            Ok(())
        }
        async fn delete(&self) -> CheckpointResult<()> {
            Ok(())
        }
    }

    #[tokio::test(start_paused = true)]
    async fn failed_attempt_is_retried_with_the_lock_released() {
        let flaky = Arc::new(FlakyCheckpointer {
            failures: 2,
            calls: AtomicUsize::new(0),
        });
        let checkpointer: Arc<dyn DatasetCheckpointer> = Arc::clone(&flaky) as _;
        let mutex = Arc::new(Mutex::new(()));
        let task = tokio::spawn({
            let mutex = Arc::clone(&mutex);
            async move {
                create_checkpoint_and_snapshot(
                    &checkpointer,
                    None,
                    &Arc::new(Schema::empty()),
                    &mutex,
                    &TableReference::bare("t"),
                    &Arc::new(AtomicI64::new(0)),
                    ForceCreate(false),
                    None,
                    None,
                    None,
                )
                .await;
            }
        });

        // Runs once the task parks in the backoff after its first failure.
        while flaky.calls.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        assert!(!task.is_finished(), "the retry is still pending");
        assert!(
            mutex.try_lock().is_ok(),
            "the write lock is free during the backoff"
        );

        task.await.expect("snapshot task completes");
        assert_eq!(
            flaky.calls.load(Ordering::SeqCst),
            3,
            "two failures, then the attempt that succeeded"
        );
    }

    /// A live (in-place) widening evolution moves the accelerator ahead of the
    /// start-time federated schema; the checkpoint must record the accelerator's
    /// evolved field defs (not revert to the older source types), while keeping
    /// the source def for unchanged columns and still appending non-materialized
    /// source columns.
    #[test]
    fn live_accelerator_checkpoint_schema_prefers_evolved_types() {
        let federated = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("v", DataType::Int32, true),
            Field::new("w", DataType::Utf8, false),
            // A non-materialized source column (e.g. refresh_sql projection).
            Field::new("src_only", DataType::Float64, true),
        ]));
        let accelerator = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("v", DataType::Int64, true), // widened Int32 -> Int64
            Field::new("w", DataType::Utf8, true),  // relaxed NOT NULL -> nullable
            Field::new("tag", DataType::Utf8, true), // added column
        ]));

        let checkpoint = live_accelerator_checkpoint_schema(&accelerator, &federated);
        let field = |name: &str| {
            checkpoint
                .field_with_name(name)
                .expect("field present in checkpoint schema")
                .clone()
        };

        // Unchanged column keeps its (source-accurate) definition.
        assert_eq!(field("id").data_type(), &DataType::Int64);
        // Widened type uses the accelerator's evolved type, not the source's.
        assert_eq!(field("v").data_type(), &DataType::Int64);
        // Relaxed nullability uses the accelerator's evolved nullability.
        assert!(field("w").is_nullable());
        // Added column comes from the accelerator.
        assert_eq!(field("tag").data_type(), &DataType::Utf8);
        // Non-materialized source column is still appended.
        assert_eq!(field("src_only").data_type(), &DataType::Float64);
    }
}
