/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use crate::federated::FederatedTable;
use runtime_status as status;

use super::{
    metrics, refresh::RefreshOverrides, refresh_completion::RefreshRequestId,
    refresh_task::RefreshTask, synchronized_table::SynchronizedTable,
};
use futures::{FutureExt, future::BoxFuture};
use tokio::{
    runtime::Handle,
    select,
    sync::{
        Semaphore,
        mpsc::{self, Receiver, Sender},
    },
    task::JoinHandle,
};

use std::sync::atomic::{AtomicBool, AtomicI64};
use std::{any::Any, panic::AssertUnwindSafe, sync::Arc};
use tokio::sync::{Mutex, RwLock};

use super::refresh::Refresh;
use datafusion::{datasource::TableProvider, sql::TableReference};
use opentelemetry::KeyValue;
use runtime_component::dataset::acceleration::RefreshMode;
use spicepod::metric::Metrics;

pub struct RefreshTaskRunnerBuilder {
    runtime_status: Arc<status::RuntimeStatus>,
    dataset_name: TableReference,
    federated: Arc<FederatedTable>,
    federated_source: Option<String>,
    refresh: Arc<RwLock<Refresh>>,
    accelerator: Arc<dyn TableProvider>,
    disable_federation: bool,
    semaphore: Option<Arc<Semaphore>>,
    metrics: Option<Metrics>,
    cpu_runtime: Option<Handle>,
    io_runtime: Handle,
    resource_monitor: Option<runtime_resources::ResourceMonitor>,
    /// Mutex to protect concurrent access to the accelerator during cache/snapshot operations.
    /// Shared with `CachingAccelerationScanExec`.
    accelerator_write_mutex: Arc<Mutex<()>>,
    last_updated_at: Arc<AtomicI64>,
    initial_load_completed: Option<Arc<AtomicBool>>,
    /// Whether the acceleration uses S3 Express One Zone storage.
    is_s3_express_acceleration: bool,
    engine_type_rewrites: arrow_tools::type_rewrite::TypeRewriteRules,
    snapshot_refresh_state: Option<crate::accelerated::snapshots::SnapshotRefreshState>,
    /// Forwarded to the refresh task so the caching stale-row refresh claims
    /// the keys it replaces.
    in_flight_revalidations: Option<crate::accelerated::caching::InFlightRevalidations>,
}

impl RefreshTaskRunnerBuilder {
    #[expect(clippy::too_many_arguments)]
    #[must_use]
    pub fn new(
        runtime_status: Arc<status::RuntimeStatus>,
        dataset_name: TableReference,
        federated: Arc<FederatedTable>,
        federated_source: Option<String>,
        refresh: Arc<RwLock<Refresh>>,
        accelerator: Arc<dyn TableProvider>,
        io_runtime: Handle,
        accelerator_write_mutex: Arc<Mutex<()>>,
    ) -> Self {
        Self {
            runtime_status,
            dataset_name,
            federated,
            federated_source,
            refresh,
            accelerator,
            disable_federation: false,
            semaphore: None,
            metrics: None,
            cpu_runtime: None,
            io_runtime,
            resource_monitor: None,
            accelerator_write_mutex,
            last_updated_at: Arc::new(AtomicI64::new(0)),
            initial_load_completed: None,
            is_s3_express_acceleration: false,
            engine_type_rewrites: &[],
            snapshot_refresh_state: None,
            in_flight_revalidations: None,
        }
    }

    /// Sets the `disable_federation` flag
    #[must_use]
    pub fn with_disable_federation(mut self, disable: bool) -> Self {
        self.disable_federation = disable;
        self
    }

    #[must_use]
    pub fn with_semaphore(mut self, semaphore: Arc<Semaphore>) -> Self {
        self.semaphore = Some(semaphore);
        self
    }

    #[must_use]
    pub fn with_metrics(mut self, metrics: Option<Metrics>) -> Self {
        self.metrics = metrics;
        self
    }

    #[must_use]
    pub fn with_cpu_runtime(mut self, runtime: Option<Handle>) -> Self {
        self.cpu_runtime = runtime;
        self
    }

    #[must_use]
    pub fn with_resource_monitor(mut self, monitor: runtime_resources::ResourceMonitor) -> Self {
        self.resource_monitor = Some(monitor);
        self
    }

    #[must_use]
    pub fn with_last_updated_at(mut self, last_updated_at: Arc<AtomicI64>) -> Self {
        self.last_updated_at = last_updated_at;
        self
    }

    #[must_use]
    pub fn with_initial_load_completed(mut self, flag: Arc<AtomicBool>) -> Self {
        self.initial_load_completed = Some(flag);
        self
    }

    /// Set whether the acceleration uses S3 Express One Zone storage.
    #[must_use]
    pub fn with_s3_express_acceleration(mut self, is_s3_express: bool) -> Self {
        self.is_s3_express_acceleration = is_s3_express;
        self
    }

    /// Declare the acceleration engine's own type rewrites.
    #[must_use]
    pub fn with_engine_type_rewrites(
        mut self,
        rules: arrow_tools::type_rewrite::TypeRewriteRules,
    ) -> Self {
        self.engine_type_rewrites = rules;
        self
    }

    /// Provide the snapshot-refresh state required for `RefreshMode::Snapshot`.
    #[must_use]
    pub fn with_snapshot_refresh_state(
        mut self,
        state: Option<crate::accelerated::snapshots::SnapshotRefreshState>,
    ) -> Self {
        self.snapshot_refresh_state = state;
        self
    }

    /// Shares the caching accelerator's claim set with the refresh task, so
    /// its periodic stale-row refresh claims the keys it replaces.
    #[must_use]
    pub fn with_in_flight_revalidations(
        mut self,
        in_flight_revalidations: crate::accelerated::caching::InFlightRevalidations,
    ) -> Self {
        self.in_flight_revalidations = Some(in_flight_revalidations);
        self
    }

    #[must_use]
    pub fn build(self) -> RefreshTaskRunner {
        let accelerator_write_mutex = Arc::clone(&self.accelerator_write_mutex);
        let mut refresh_task_builder = RefreshTask::builder(
            self.runtime_status,
            self.dataset_name.clone(),
            self.federated,
            self.federated_source,
            self.accelerator,
            self.io_runtime,
            accelerator_write_mutex,
        )
        .with_disable_federation(self.disable_federation)
        .with_last_updated_at(Arc::clone(&self.last_updated_at))
        .with_metrics(self.metrics);

        if let Some(semaphore) = self.semaphore {
            refresh_task_builder = refresh_task_builder.with_semaphore(semaphore);
        }

        refresh_task_builder = refresh_task_builder.with_cpu_runtime(self.cpu_runtime);

        if let Some(resource_monitor) = self.resource_monitor {
            refresh_task_builder = refresh_task_builder.with_resource_monitor(resource_monitor);
        }

        refresh_task_builder =
            refresh_task_builder.with_s3_express_acceleration(self.is_s3_express_acceleration);

        refresh_task_builder =
            refresh_task_builder.with_engine_type_rewrites(self.engine_type_rewrites);

        refresh_task_builder =
            refresh_task_builder.with_snapshot_refresh_state(self.snapshot_refresh_state);

        if let Some(flag) = self.initial_load_completed {
            refresh_task_builder = refresh_task_builder.with_initial_load_completed(flag);
        }

        if let Some(in_flight_revalidations) = self.in_flight_revalidations {
            refresh_task_builder =
                refresh_task_builder.with_in_flight_revalidations(in_flight_revalidations);
        }

        let refresh_task = Arc::new(refresh_task_builder.build());

        RefreshTaskRunner {
            dataset_name: self.dataset_name,
            refresh: self.refresh,
            refresh_task,
            accelerator_write_mutex: self.accelerator_write_mutex,
            task: None,
        }
    }
}

/// `RefreshTaskRunner` is responsible for running all refresh tasks for a dataset. It is expected
/// that only one [`RefreshTaskRunner`] is used per dataset, and that is is the only entity
/// refreshing an `accelerator`.
#[derive(Debug)]
pub struct RefreshTaskRunner {
    dataset_name: TableReference,
    refresh: Arc<RwLock<Refresh>>,
    refresh_task: Arc<RefreshTask>,
    /// Same lock `create_checkpoint_and_snapshot` holds when it samples provenance.
    /// Retract/restore take it first so a snapshot cannot publish the previous rows
    /// under a newer attestation or fingerprint identity.
    accelerator_write_mutex: Arc<Mutex<()>>,
    task: Option<JoinHandle<()>>,
}

type RefreshRunFuture =
    BoxFuture<'static, std::result::Result<super::Result<()>, Box<dyn Any + Send>>>;

/// One refresh request: the id it was issued under, and the overrides it
/// carries.
///
/// The id travels with the request so the completion it produces can be
/// attributed to it. A refresh already running when a request arrives is
/// cancelled and never completes, so a completion cannot be attributed by
/// counting requests — see [`RefreshRequestId`].
pub type RefreshRequest = (RefreshRequestId, Option<RefreshOverrides>);

/// A finished refresh, reported under the id of the request that started it.
pub type RefreshTaskCompletion = (RefreshRequestId, super::Result<()>);

type RefreshTaskStartSender = Sender<RefreshRequest>;
type RefreshTaskCompletionReceiver = Receiver<RefreshTaskCompletion>;

impl RefreshTaskRunner {
    #[expect(clippy::too_many_arguments)]
    #[must_use]
    pub fn builder(
        runtime_status: Arc<status::RuntimeStatus>,
        dataset_name: TableReference,
        federated: Arc<FederatedTable>,
        federated_source: Option<String>,
        refresh: Arc<RwLock<Refresh>>,
        accelerator: Arc<dyn TableProvider>,
        io_runtime: Handle,
        accelerator_write_mutex: Arc<Mutex<()>>,
    ) -> RefreshTaskRunnerBuilder {
        RefreshTaskRunnerBuilder::new(
            runtime_status,
            dataset_name,
            federated,
            federated_source,
            refresh,
            accelerator,
            io_runtime,
            accelerator_write_mutex,
        )
    }

    /// # Errors
    ///
    /// Returns an error if the refresh task cannot be spawned.
    pub fn start(
        &mut self,
    ) -> super::Result<(RefreshTaskStartSender, RefreshTaskCompletionReceiver)> {
        if self.task.is_some() {
            return Err(super::Error::RefreshTaskAlreadyStarted {});
        }

        let (start_refresh, mut on_start_refresh) = mpsc::channel::<RefreshRequest>(1);

        let (notify_refresh_complete, on_refresh_complete) =
            mpsc::channel::<RefreshTaskCompletion>(1);

        let dataset_name = self.dataset_name.clone();
        let notify_refresh_complete = Arc::new(notify_refresh_complete);

        let base_refresh = Arc::clone(&self.refresh);

        let refresh_task = Arc::clone(&self.refresh_task);
        let accelerator_write_mutex = Arc::clone(&self.accelerator_write_mutex);

        self.task = Some(tokio::spawn(async move {
            let mut task_completion: Option<RefreshRunFuture> = None;
            // The request the in-flight refresh was started for, so its
            // completion is reported under the id that asked for it. A request
            // arriving mid-refresh replaces both together: the running future is
            // dropped by the `select!` below, so it never reports at all.
            let mut running_request: RefreshRequestId = 0;
            // Provenance of the run in `task_completion`, asserted only once it succeeds.
            let mut pending_configured = false;

            loop {
                if let Some(task) = task_completion.take() {
                    select! {
                        res = task => {
                            match res {
                                Ok(Ok(())) => {
                                    tracing::debug!("Dataset {dataset_name} refreshed successfully");
                                    // Now, and only now, do the accelerator's rows come from
                                    // this run, so this is when its provenance may be
                                    // asserted. A failed or panicked run asserts nothing and
                                    // leaves the mark retracted, which declines a publish of
                                    // whatever rows survived it. Same write mutex the snapshot
                                    // path holds when it samples the mark.
                                    // Re-check live SQL under the write mutex: an
                                    // intervening `update_refresh_sql` can change the
                                    // live definition (and retract provenance) while
                                    // this run was in flight. Restoring the dequeue-time
                                    // `pending_configured` would clobber that and let
                                    // rows from the old SQL publish under the new live
                                    // state.
                                    Self::restore_materialization_under_write_mutex(
                                        &base_refresh,
                                        pending_configured,
                                        &accelerator_write_mutex,
                                    )
                                    .await;
                                    if let Err(err) = notify_refresh_complete.send((running_request, Ok(()))).await {
                                        tracing::debug!("Failed to send refresh task completion for dataset {dataset_name}: {err}");
                                    }
                                },
                                Ok(Err(err)) => {
                                    tracing::debug!("Dataset {dataset_name} failed to refresh with error: {err}");
                                    if let Err(err) = notify_refresh_complete.send((running_request, Err(err))).await {
                                        tracing::debug!("Failed to send refresh task completion for dataset {dataset_name}: {err}");
                                    }
                                },
                                Err(panic_payload) => {
                                    let dataset_label = dataset_name.to_string();
                                    let panic_message = Self::panic_to_message(panic_payload);
                                    tracing::error!(
                                        dataset = %dataset_label,
                                        %panic_message,
                                        "Refresh worker panicked; continuing refresh loop"
                                    );
                                    metrics::REFRESH_WORKER_PANICS.add(1, &[KeyValue::new("dataset", dataset_label.clone())]);

                                    let panic_error = super::Error::RefreshWorkerPanicked {
                                        dataset_name: dataset_label,
                                        message: panic_message.clone(),
                                    };

                                    if let Err(err) = notify_refresh_complete.send((running_request, Err(panic_error))).await {
                                        tracing::debug!("Failed to send refresh task completion for dataset {dataset_name}: {err}");
                                    }
                                }
                            }
                        },
                        Some((request_id, overrides_opt)) = on_start_refresh.recv() => {
                            running_request = request_id;
                            let (request, configured) = Self::create_refresh_from_overrides(Arc::clone(&base_refresh), overrides_opt, &accelerator_write_mutex).await;
                            pending_configured = configured;
                            task_completion = Some(Self::wrap_refresh_future(Arc::clone(&refresh_task), request));
                        }
                    }
                } else {
                    select! {
                        Some((request_id, overrides_opt)) = on_start_refresh.recv() => {
                            running_request = request_id;
                            let (request, configured) = Self::create_refresh_from_overrides(Arc::clone(&base_refresh), overrides_opt, &accelerator_write_mutex).await;
                            pending_configured = configured;
                            task_completion = Some(Self::wrap_refresh_future(Arc::clone(&refresh_task), request));
                        }
                        else => {
                            // The parent refresher is shutting down, we should too
                            break;
                        }
                    }
                }
            }
        }));

        Ok((start_refresh, on_refresh_complete))
    }

    /// Subscribes a new acceleration table provider to the existing `AccelerationSink` managed by this `RefreshTask`.
    pub async fn add_synchronized_table(&self, synchronized_table: SynchronizedTable) {
        self.refresh_task
            .add_synchronized_table(synchronized_table)
            .await;
    }

    /// The [`RefreshTask`] this runner drives. Used by the refresher loop to
    /// resolve the live set of dataset names (self + synchronized children) at
    /// refresh completion — children attach after their own initial load, so
    /// the set cannot be captured up front.
    pub(crate) fn refresh_task(&self) -> &Arc<RefreshTask> {
        &self.refresh_task
    }

    /// Writes [`Refresh::set_materialization_is_configured`] while holding the
    /// accelerator write mutex — the same lock
    /// [`super::snapshots::create_checkpoint_and_snapshot`] samples under.
    ///
    /// Production completion uses [`Self::restore_materialization_under_write_mutex`];
    /// this helper remains for unit tests that set an absolute provenance bit.
    #[cfg(test)]
    async fn set_materialization_under_write_mutex(
        refresh: &Arc<RwLock<Refresh>>,
        configured: bool,
        accelerator_write_mutex: &Arc<Mutex<()>>,
    ) {
        let _guard = accelerator_write_mutex.lock().await;
        refresh
            .read()
            .await
            .set_materialization_is_configured(configured);
    }

    /// Restores provenance after a successful refresh, but only if the live
    /// refresh SQL still matches the Spicepod definition the dequeue-time
    /// `pending_configured` was computed against.
    ///
    /// `update_refresh_sql` acquires the same write mutex, patches live SQL, and
    /// retracts provenance while a run started earlier is still executing. When
    /// that older run completes it must not overwrite the newer live state with
    /// the dequeue-time bit.
    async fn restore_materialization_under_write_mutex(
        refresh: &Arc<RwLock<Refresh>>,
        pending_configured: bool,
        accelerator_write_mutex: &Arc<Mutex<()>>,
    ) {
        let _guard = accelerator_write_mutex.lock().await;
        let live = refresh.read().await;
        let configured = pending_configured && live.live_refresh_sql_matches_configured();
        live.set_materialization_is_configured(configured);
    }

    /// Create a new [`Refresh`] based on defaults and overrides, and report what this run
    /// would let us say about the accelerator's provenance if it succeeds.
    ///
    /// Also begins a new materialization generation (new epoch, `configured = false`),
    /// because from here the accelerator's rows are being replaced and describe
    /// nothing definite until the run finishes. Retracting up front is what makes
    /// every window safe for the configured bit: a snapshot that lands mid-refresh,
    /// or after a refresh that failed, finds "not known configured" and declines.
    /// The epoch is what makes the window safe for attestation: a snapshot that
    /// already sampled the previous generation under the write mutex cannot adopt
    /// this run's plan shape. The caller re-asserts the mark only once the run has
    /// actually succeeded.
    ///
    /// Retract waits for `accelerator_write_mutex` first. A snapshot already in
    /// `create_checkpoint_and_snapshot` holds that lock through its provenance sample
    /// *and* the publish-gate check, so it finishes against the previous identity. This
    /// function returns — and the refresh scan that records a new view attestation can
    /// start — only after that snapshot releases. A snapshot that arrives later finds
    /// the mark retracted and declines.
    ///
    /// A run can only *establish* provenance if it replaces the whole accelerator. An
    /// incremental run (`Append`, `Changes`) adds to what is already there, so a clean
    /// incremental refresh on top of rows an override appended leaves those rows in place —
    /// re-asserting provenance there would stamp the next snapshot as the configured
    /// definition's result while it still contains rows that definition never produced.
    /// Incremental runs therefore carry the provenance they inherited forward at best, and
    /// only a full replace can restore it.
    ///
    /// A runtime `refresh_sql` PATCH is the same class of mismatch as a request-scoped
    /// override: it changes which rows land without updating the Spicepod fingerprint
    /// stamped at snapshot-manager construction. A later full refresh with no override
    /// must not re-assert provenance until the live SQL matches that configured
    /// definition again.
    async fn create_refresh_from_overrides(
        defaults: Arc<RwLock<Refresh>>,
        overrides_opt: Option<RefreshOverrides>,
        accelerator_write_mutex: &Arc<Mutex<()>>,
    ) -> (Refresh, bool) {
        // Mutex first, then the `Refresh` lock — same order as
        // `create_checkpoint_and_snapshot`, so the two cannot deadlock.
        let (r, inherited) = {
            let _guard = accelerator_write_mutex.lock().await;
            let r = defaults.read().await.clone();
            let inherited = r.materialization_is_configured();
            // Retract `configured` and bump the shared epoch; snapshot bind and
            // view attestation sample that cell later — this site has no consumer.
            let _ = r.begin_materialization();
            (r, inherited)
        };
        let live_matches_configured = r.live_refresh_sql_matches_configured();
        let (mut request, overridden) = match overrides_opt {
            Some(overrides) => {
                let overridden = overrides.changes_materialization();
                (r.with_overrides(&overrides), overridden)
            }
            None => (r, false),
        };
        // `request.mode` is the mode this run will actually use, overrides applied.
        let replaces_everything = matches!(request.mode, RefreshMode::Full);
        let configured =
            !overridden && live_matches_configured && (replaces_everything || inherited);
        // Re-establishing provenance rather than carrying it forward: the mark is retracted
        // and only a real full replacement earns it back. `RefreshTask::run_once` can return
        // success from the unchanged-source skip without writing anything, which would stamp
        // an earlier override's rows as the configured definition's result, so that run has
        // to fetch. Where the mark is merely inherited the rows already carry it and the skip
        // changes nothing.
        request.must_materialize = configured && !inherited;
        (request, configured)
    }

    fn wrap_refresh_future(refresh_task: Arc<RefreshTask>, request: Refresh) -> RefreshRunFuture {
        Box::pin(AssertUnwindSafe(async move { refresh_task.run(request).await }).catch_unwind())
    }

    fn panic_to_message(panic: Box<dyn Any + Send>) -> String {
        match panic.downcast::<String>() {
            Ok(message) => *message,
            Err(panic) => match panic.downcast::<&'static str>() {
                Ok(message) => (*message).to_string(),
                Err(_) => "refresh worker panicked with a non-string payload".to_string(),
            },
        }
    }

    pub fn abort(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
            self.task = None;
        }
    }
}

impl Drop for RefreshTaskRunner {
    fn drop(&mut self) {
        self.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::RefreshTaskRunner;
    use crate::accelerated::refresh::Refresh;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::sql::TableReference;
    use runtime_component::dataset::acceleration::RefreshMode;
    use runtime_datafusion::refresh_sql::{RefreshSQL, parse_refresh_sql};
    use std::sync::Arc;
    use tokio::sync::{Mutex, RwLock};

    fn write_mutex() -> Arc<Mutex<()>> {
        Arc::new(Mutex::new(()))
    }

    fn orders_refresh_sql(sql: &str) -> RefreshSQL {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("region", DataType::Utf8, false),
        ]));
        parse_refresh_sql(TableReference::bare("orders"), sql, schema)
            .expect("test refresh SQL should parse")
            .0
    }

    /// `PATCH /v1/datasets/{name}/acceleration` replaces live `Refresh.sql` and
    /// then a later full refresh arrives with no request override — the
    /// transition Copilot traced. The runner must not mark that run configured:
    /// the snapshot fingerprint is still the Spicepod definition.
    #[tokio::test]
    async fn patched_refresh_sql_cannot_publish_under_the_startup_fingerprint() {
        let configured_sql = "SELECT * FROM orders WHERE region = 'us'";
        let live_sql_after_patch = "SELECT * FROM orders WHERE region = 'eu'";

        let refresh =
            Refresh::new(RefreshMode::Full).refresh_sql(orders_refresh_sql(configured_sql));
        refresh.set_materialization_is_configured(true);
        let defaults = Arc::new(RwLock::new(refresh));

        let (_request, before_patch) = RefreshTaskRunner::create_refresh_from_overrides(
            Arc::clone(&defaults),
            None,
            &write_mutex(),
        )
        .await;
        assert!(
            before_patch,
            "a full refresh of the Spicepod SQL must still be publishable"
        );
        defaults
            .write()
            .await
            .set_materialization_is_configured(true);

        {
            let mut live = defaults.write().await;
            live.apply_runtime_refresh_sql(orders_refresh_sql(live_sql_after_patch));
            assert!(
                !live.live_refresh_sql_matches_configured(),
                "live_sql_after_patch must diverge from the Spicepod definition"
            );
        }

        let (_request, configured) = RefreshTaskRunner::create_refresh_from_overrides(
            Arc::clone(&defaults),
            None,
            &write_mutex(),
        )
        .await;

        assert!(
            !configured,
            "a full refresh after PATCH /acceleration must not publish under the startup fingerprint"
        );
    }

    /// Completing a refresh that was dequeued as configured must not restore
    /// provenance after `update_refresh_sql` has already replaced the live SQL.
    #[tokio::test]
    async fn old_completion_cannot_clobber_newer_live_refresh_sql() {
        let configured_sql = "SELECT * FROM orders WHERE region = 'us'";
        let live_sql_after_patch = "SELECT * FROM orders WHERE region = 'eu'";

        let refresh =
            Refresh::new(RefreshMode::Full).refresh_sql(orders_refresh_sql(configured_sql));
        refresh.set_materialization_is_configured(true);
        let defaults = Arc::new(RwLock::new(refresh));
        let mutex = write_mutex();

        // Dequeue-time decision: this run would be configured.
        let (_request, pending_configured) =
            RefreshTaskRunner::create_refresh_from_overrides(Arc::clone(&defaults), None, &mutex)
                .await;
        assert!(
            pending_configured,
            "precondition: dequeue decided configured"
        );

        // While the run is in flight, PATCH replaces live SQL and retracts.
        {
            let mut live = defaults.write().await;
            live.apply_runtime_refresh_sql(orders_refresh_sql(live_sql_after_patch));
            assert!(
                !live.live_refresh_sql_matches_configured(),
                "precondition: live SQL diverged"
            );
            assert!(
                !live.materialization_is_configured(),
                "precondition: PATCH retracted provenance"
            );
        }

        // Old completion must not flip provenance back to true.
        RefreshTaskRunner::restore_materialization_under_write_mutex(
            &defaults,
            pending_configured,
            &mutex,
        )
        .await;

        let after = defaults.read().await;
        assert!(
            !after.materialization_is_configured(),
            "old completion must not clobber a newer live refresh SQL"
        );
        assert!(
            !after.live_refresh_sql_matches_configured(),
            "live SQL must remain the patched value"
        );
    }

    #[tokio::test]
    async fn restoring_configured_refresh_sql_can_reestablish_provenance() {
        let configured_sql = "SELECT * FROM orders WHERE region = 'us'";
        let refresh =
            Refresh::new(RefreshMode::Full).refresh_sql(orders_refresh_sql(configured_sql));
        let defaults = Arc::new(RwLock::new(refresh));

        defaults
            .write()
            .await
            .apply_runtime_refresh_sql(orders_refresh_sql(
                "SELECT * FROM orders WHERE region = 'eu'",
            ));
        let (_request, after_patch) = RefreshTaskRunner::create_refresh_from_overrides(
            Arc::clone(&defaults),
            None,
            &write_mutex(),
        )
        .await;
        assert!(
            !after_patch,
            "precondition: the patched SQL must not be treated as configured"
        );

        defaults
            .write()
            .await
            .apply_runtime_refresh_sql(orders_refresh_sql(configured_sql));
        assert!(
            defaults.read().await.live_refresh_sql_matches_configured(),
            "restored live SQL must match the Spicepod definition"
        );
        let (_request, after_restore) = RefreshTaskRunner::create_refresh_from_overrides(
            Arc::clone(&defaults),
            None,
            &write_mutex(),
        )
        .await;
        assert!(
            after_restore,
            "PATCH back to the Spicepod SQL must let a later full refresh publish again"
        );
    }

    fn configured_full_refresh() -> Refresh {
        let refresh = Refresh::new(RefreshMode::Full).refresh_sql(orders_refresh_sql(
            "SELECT * FROM orders WHERE region = 'us'",
        ));
        refresh.set_materialization_is_configured(true);
        refresh
    }

    /// A snapshot that already holds `accelerator_write_mutex` must finish
    /// sampling provenance (and the publish gate) before a newly dequeued
    /// refresh can retract the mark or start the scan that records a new
    /// attestation. Without taking that lock here, this test fails: retract
    /// completes while the snapshot still holds the mutex.
    #[tokio::test]
    async fn provenance_retract_waits_for_the_accelerator_write_mutex() {
        let unlocked = Arc::new(RwLock::new(configured_full_refresh()));
        let (_request, configured) = RefreshTaskRunner::create_refresh_from_overrides(
            Arc::clone(&unlocked),
            None,
            &write_mutex(),
        )
        .await;
        assert!(
            configured,
            "control: an unlocked full refresh of the Spicepod SQL is still publishable"
        );
        assert!(
            !unlocked.read().await.materialization_is_configured(),
            "control: retract must be visible once the write mutex is free"
        );

        let defaults = Arc::new(RwLock::new(configured_full_refresh()));
        let write_mutex = write_mutex();
        let snapshot_guard = write_mutex.lock().await;
        let retract = tokio::spawn({
            let defaults = Arc::clone(&defaults);
            let write_mutex = Arc::clone(&write_mutex);
            async move {
                RefreshTaskRunner::create_refresh_from_overrides(defaults, None, &write_mutex).await
            }
        });

        for _ in 0..32 {
            tokio::task::yield_now().await;
            assert!(
                !retract.is_finished(),
                "create_refresh_from_overrides must not retract provenance while a snapshot holds the write mutex"
            );
            assert!(
                defaults.read().await.materialization_is_configured(),
                "a snapshot holding the write mutex must still see the previous materialization as configured"
            );
        }

        drop(snapshot_guard);
        let (_request, configured) = retract
            .await
            .expect("retract task should finish after the snapshot releases the write mutex");
        assert!(
            configured,
            "the blocked full refresh must still be treated as configured once it runs"
        );
        assert!(
            !defaults.read().await.materialization_is_configured(),
            "retract must run once the snapshot releases the write mutex"
        );
    }

    #[tokio::test]
    async fn provenance_restore_waits_for_the_accelerator_write_mutex() {
        let defaults = Arc::new(RwLock::new(Refresh::new(RefreshMode::Full)));
        let write_mutex = write_mutex();
        let snapshot_guard = write_mutex.lock().await;
        let restore = tokio::spawn({
            let defaults = Arc::clone(&defaults);
            let write_mutex = Arc::clone(&write_mutex);
            async move {
                RefreshTaskRunner::set_materialization_under_write_mutex(
                    &defaults,
                    true,
                    &write_mutex,
                )
                .await;
            }
        });

        for _ in 0..32 {
            tokio::task::yield_now().await;
            assert!(
                !restore.is_finished(),
                "restoring provenance must wait for the snapshot's write mutex"
            );
            assert!(
                !defaults.read().await.materialization_is_configured(),
                "restore must not become visible while the snapshot holds the write mutex"
            );
        }

        drop(snapshot_guard);
        restore
            .await
            .expect("restore task should finish after the snapshot releases the write mutex");
        assert!(
            defaults.read().await.materialization_is_configured(),
            "restore must be visible once the snapshot releases the write mutex"
        );
    }

    #[tokio::test]
    async fn dequeue_advances_the_materialization_epoch() {
        let refresh = Refresh::new(RefreshMode::Full);
        let first = refresh.begin_materialization();
        refresh.set_materialization_is_configured(true);
        let defaults = Arc::new(RwLock::new(refresh));

        let before = defaults.read().await.sample_materialization();
        assert!(before.configured);
        assert_eq!(before.epoch, first);

        let (_request, _configured) = RefreshTaskRunner::create_refresh_from_overrides(
            Arc::clone(&defaults),
            None,
            &write_mutex(),
        )
        .await;

        let after = defaults.read().await.sample_materialization();
        assert!(
            !after.configured,
            "dequeue must retract configured with the new epoch"
        );
        assert_eq!(
            after.epoch,
            first + 1,
            "dequeue must advance the epoch so a snapshot of the previous rows cannot adopt this run's attestation"
        );
    }
}
