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

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};

use arrow::array::RecordBatch;
use arrow_tools::record_batch::try_cast_to;
use datafusion::common::{DataFusionError, Result, TableReference};
use datafusion::datasource::TableProvider;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion::execution::{SendableRecordBatchStream, SessionState, TaskContext};
use datafusion::physical_plan::{collect, stream::RecordBatchStreamAdapter};
use datafusion::prelude::SessionContext;
use futures::TryStreamExt;
use runtime_status::RuntimeStatus;
use tokio::runtime::Handle;
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use super::{
    CACHE_NAMESPACE_COLUMN, CacheKeyClaim, CacheRefreshHelper, CacheWriteHealth, CacheWriteRequest,
    MAX_CONCURRENT_REFRESHES, namespace_filter_expr, stamp_namespace_column,
};
use crate::accelerated::write::append::AppendPlanCache;
#[cfg(not(windows))]
use crate::accelerated::write::append::cayenne_append_target;

/// Admission held from before the source fetch through mutation completion.
pub(crate) struct CacheFillPermit {
    _slot: OwnedSemaphorePermit,
    reservation: MemoryReservation,
    collected_reservations: Vec<MemoryReservation>,
    memory_pool: Arc<dyn MemoryPool>,
    stopping: CancellationToken,
}

impl CacheFillPermit {
    /// Preserve the source session's configuration and services while charging
    /// execution to the same pool as the admitted response.
    pub(crate) fn task_context(&self, state: &SessionState) -> Result<Arc<TaskContext>> {
        if Arc::ptr_eq(&state.runtime_env().memory_pool, &self.memory_pool) {
            return Ok(state.task_ctx());
        }
        let runtime = RuntimeEnvBuilder::from_runtime_env(state.runtime_env())
            .with_memory_pool(Arc::clone(&self.memory_pool))
            .build_arc()?;
        Ok(Arc::new(TaskContext::from(state).with_runtime(runtime)))
    }

    /// Closing cancels source planning and collection, not accepted mutations.
    pub(crate) async fn run_source<T>(
        &self,
        future: impl std::future::Future<Output = Result<T>>,
    ) -> Result<T> {
        tokio::select! {
            biased;
            () = self.stopping.cancelled() => Err(DataFusionError::Execution(format!(
                "{} cancelled because cache writes are closed",
                self.reservation.consumer().name(),
            ))),
            result = future => result,
        }
    }

    pub(crate) fn reserve(&mut self, batch: &RecordBatch) -> Result<()> {
        self.reservation.try_grow(batch.get_array_memory_size())
    }

    /// Charge each batch before polling the source again. A failed or cancelled
    /// collection drops its partial reservation without consuming the permit.
    pub(crate) async fn collect(
        &mut self,
        mut stream: SendableRecordBatchStream,
    ) -> Result<Vec<RecordBatch>> {
        let reservation = self.reservation.new_empty();
        let (batches, reservation) = self
            .run_source(async move {
                let mut batches = Vec::new();
                while let Some(batch) = stream.try_next().await? {
                    reservation.try_grow(batch.get_array_memory_size())?;
                    batches.push(batch);
                }
                Ok((batches, reservation))
            })
            .await?;
        self.collected_reservations.push(reservation);
        Ok(batches)
    }
}

/// Dataset-owned direct applies. There is no pending-response queue or flush timer.
#[derive(Clone)]
pub struct CacheWriter(Arc<Inner>);

struct Inner {
    accelerator: Arc<dyn TableProvider>,
    dataset: TableReference,
    write_mutex: Arc<Mutex<()>>,
    last_updated_at: Arc<AtomicI64>,
    health: parking_lot::Mutex<CacheWriteHealth>,
    plans: Mutex<Option<AppendPlanCache>>,
    context: SessionContext,
    memory_pool: Arc<dyn MemoryPool>,
    slots: Arc<Semaphore>,
    tasks: TaskTracker,
    stopping: CancellationToken,
    /// Serializes closing admission with registering accepted tasks.
    closed: parking_lot::Mutex<bool>,
    /// Refuse successor mutations if durable recovery could not finish.
    recovery_failed: AtomicBool,
    apply_runtime: Handle,
    io_runtime: Handle,
}

impl Drop for Inner {
    fn drop(&mut self) {
        self.slots.close();
        self.stopping.cancel();
        self.tasks.close();
    }
}

impl CacheWriter {
    #[expect(clippy::too_many_arguments)]
    pub(crate) fn new(
        accelerator: Arc<dyn TableProvider>,
        dataset: TableReference,
        write_mutex: Arc<Mutex<()>>,
        last_updated_at: Arc<AtomicI64>,
        runtime_status: Arc<RuntimeStatus>,
        memory_pool: Arc<dyn MemoryPool>,
        apply_runtime: Handle,
        io_runtime: Handle,
    ) -> Self {
        let runtime_env = RuntimeEnv {
            memory_pool: Arc::clone(&memory_pool),
            ..RuntimeEnv::default()
        };
        let context = SessionContext::new_with_config_rt(
            util::session_state::session_config(),
            Arc::new(runtime_env),
        );
        Self(Arc::new(Inner {
            accelerator,
            health: parking_lot::Mutex::new(CacheWriteHealth::new(runtime_status, dataset.clone())),
            dataset,
            write_mutex,
            last_updated_at,
            plans: Mutex::new(None),
            context,
            memory_pool,
            slots: Arc::new(Semaphore::new(MAX_CONCURRENT_REFRESHES)),
            tasks: TaskTracker::new(),
            stopping: CancellationToken::new(),
            closed: parking_lot::Mutex::new(false),
            recovery_failed: AtomicBool::new(false),
            apply_runtime,
            io_runtime,
        }))
    }

    pub(crate) async fn admit(&self) -> Result<CacheFillPermit> {
        let slot = Arc::clone(&self.0.slots)
            .acquire_owned()
            .await
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        Ok(CacheFillPermit {
            _slot: slot,
            reservation: MemoryConsumer::new(format!("Cache fill {}", self.0.dataset))
                .register(&self.0.memory_pool),
            collected_reservations: Vec::new(),
            memory_pool: Arc::clone(&self.0.memory_pool),
            stopping: self.0.stopping.clone(),
        })
    }

    pub(crate) fn try_admit(&self) -> Option<CacheFillPermit> {
        let slot = Arc::clone(&self.0.slots).try_acquire_owned().ok()?;
        Some(CacheFillPermit {
            _slot: slot,
            reservation: MemoryConsumer::new(format!("Cache fill {}", self.0.dataset))
                .register(&self.0.memory_pool),
            collected_reservations: Vec::new(),
            memory_pool: Arc::clone(&self.0.memory_pool),
            stopping: self.0.stopping.clone(),
        })
    }

    /// Accepted mutations outlive cancellation of the caller. The task retains
    /// the key claim, admission and payload until publication or reconciliation.
    pub(crate) async fn write(
        &self,
        request: CacheWriteRequest,
        claim: CacheKeyClaim,
        permit: CacheFillPermit,
    ) -> Result<()> {
        self.write_with_children(request, claim, permit, None).await
    }

    pub(crate) async fn write_with_children(
        &self,
        request: CacheWriteRequest,
        claim: CacheKeyClaim,
        mut permit: CacheFillPermit,
        children: Option<super::SynchronizedChildren>,
    ) -> Result<()> {
        debug_assert_eq!(request.cache_key, claim.key());
        let task = {
            let closed = self.0.closed.lock();
            if *closed {
                return Err(DataFusionError::Execution(format!(
                    "Cache writes for dataset '{}' are closed",
                    self.0.dataset,
                )));
            }
            let writer = self.clone();
            self.0.tasks.spawn_on(
                async move {
                    let _claim = claim;
                    tracing::trace!(
                        dataset = %writer.0.dataset,
                        thread = std::thread::current().name().unwrap_or("unnamed"),
                        "Applying cache response",
                    );
                    let propagation = children.map(|children| {
                        (
                            children,
                            request.filters.clone(),
                            request.batches.clone(),
                            request.replaces_existing,
                            Arc::clone(&request.namespace_id),
                        )
                    });
                    writer.apply(request, &mut permit).await?;
                    if let Some((children, filters, batches, replaces_existing, namespace)) =
                        propagation
                    {
                        CacheRefreshHelper::propagate_to_synchronized_children(
                            &children,
                            &writer.0.dataset.to_string(),
                            &filters,
                            &batches,
                            replaces_existing,
                            &namespace,
                        )
                        .await;
                    }
                    Ok(())
                },
                &self.0.apply_runtime,
            )
        };
        task.await
            .map_err(|error| DataFusionError::External(Box::new(error)))?
    }

    /// Source refreshes may be cancelled at shutdown. A mutation accepted by
    /// `write` has its own tracked task and must finish independently.
    pub(crate) fn spawn_refresh(
        &self,
        refresh: impl std::future::Future<Output = ()> + Send + 'static,
    ) {
        let closed = self.0.closed.lock();
        if *closed {
            return;
        }
        let stopping = self.0.stopping.clone();
        self.0.tasks.spawn_on(
            async move {
                tokio::select! {
                    () = stopping.cancelled() => {},
                    () = refresh => {},
                }
            },
            &self.0.io_runtime,
        );
    }

    pub(crate) fn close(&self) {
        let mut closed = self.0.closed.lock();
        *closed = true;
        self.0.slots.close();
        self.0.stopping.cancel();
        self.0.tasks.close();
    }

    pub(crate) async fn shutdown(&self) {
        self.close();
        self.0.tasks.wait().await;
    }

    #[cfg(test)]
    pub(super) async fn send(&self, request: CacheWriteRequest) -> Result<()> {
        let claims = Arc::new(parking_lot::Mutex::new(Default::default()));
        let super::ClaimOutcome::Leader(claim) =
            CacheKeyClaim::acquire(&claims, request.cache_key.clone(), None)
        else {
            return Err(DataFusionError::Internal(
                "Test cache key was already claimed".into(),
            ));
        };
        let mut permit = self.admit().await?;
        for batch in &request.batches {
            permit.reserve(batch)?;
        }
        self.write(request, claim, permit).await
    }

    #[cfg(test)]
    pub(super) fn drain_task(&self) -> tokio::task::JoinHandle<()> {
        let tasks = self.0.tasks.clone();
        tokio::spawn(async move { tasks.wait().await })
    }

    async fn apply(&self, request: CacheWriteRequest, permit: &mut CacheFillPermit) -> Result<()> {
        if request.batches.iter().all(|batch| batch.num_rows() == 0) {
            return Ok(());
        }
        // Preparation cannot mutate storage. Refusal here must neither trigger
        // recovery nor change the accelerator's write-health state.
        let request = self.prepare(request, permit).inspect_err(|error| {
            tracing::debug!(dataset = %self.0.dataset, "Cache response was not prepared: {error}");
        })?;
        let _guard = self.0.write_mutex.lock().await;
        let result = if self.0.recovery_failed.load(Ordering::Acquire) {
            Err(DataFusionError::Execution(format!(
                "Cache writes for dataset '{}' require accelerator recovery",
                self.0.dataset,
            )))
        } else {
            self.replace(request).await
        };
        if let Err(error) = &result {
            #[cfg(not(windows))]
            if let Some(cayenne) = cayenne_append_target(self.0.accelerator.as_ref()) {
                match cayenne.recover_incomplete_writes().await {
                    Ok(()) => self.0.recovery_failed.store(false, Ordering::Release),
                    Err(recovery) => {
                        self.0.recovery_failed.store(true, Ordering::Release);
                        tracing::debug!(dataset = %self.0.dataset, "Cache mutation recovery failed: {recovery}");
                    }
                }
            }
            tracing::warn!(
                "Failed to write cached responses for dataset '{}', so those entries are not cached and the next query for them will be answered from the origin. Cause: {error}",
                self.0.dataset,
            );
            self.0.health.lock().record_failure(error);
        } else {
            self.0.health.lock().record_success();
            crate::accelerated::AcceleratedTable::set_timestamp_to_now(&self.0.last_updated_at);
        }
        result
    }

    fn prepare(
        &self,
        mut request: CacheWriteRequest,
        permit: &mut CacheFillPermit,
    ) -> Result<CacheWriteRequest> {
        // A namespace filter alone must never authorize a replacement.
        if request.replaces_existing && request.filters.is_empty() {
            return Err(DataFusionError::Execution(format!(
                "Cannot replace a cache entry for dataset '{}' without a request predicate",
                self.0.dataset,
            )));
        }
        let schema = self.0.accelerator.schema();
        request.batches = request
            .batches
            .into_iter()
            .map(|batch| {
                let stamped = stamp_namespace_column(batch, &schema, &request.namespace_id)?;
                let batch = try_cast_to(stamped, Arc::clone(&schema))
                    .map_err(|error| DataFusionError::External(Box::new(error)))?;
                permit.reserve(&batch)?;
                Ok(batch)
            })
            .collect::<Result<Vec<_>>>()?;
        if schema.column_with_name(CACHE_NAMESPACE_COLUMN).is_some() {
            request
                .filters
                .push(namespace_filter_expr(&request.namespace_id));
        }
        Ok(request)
    }

    async fn replace(&self, request: CacheWriteRequest) -> Result<()> {
        let CacheWriteRequest {
            batches,
            filters,
            replaces_existing,
            ..
        } = request;
        let state = self.0.context.state();
        if replaces_existing {
            match self
                .0
                .accelerator
                .delete_from(&state, filters.clone())
                .await
            {
                Ok(plan) => {
                    collect(plan, self.0.context.task_ctx()).await?;
                }
                Err(DataFusionError::NotImplemented(_)) => {
                    return CacheRefreshHelper::upsert_into_accelerator(
                        &self.0.accelerator,
                        &self.0.dataset.to_string(),
                        &filters,
                        batches,
                    )
                    .await;
                }
                Err(error) => return Err(error),
            }
        }
        let input = Box::pin(RecordBatchStreamAdapter::new(
            self.0.accelerator.schema(),
            futures::stream::iter(batches.into_iter().map(Ok)),
        ));
        #[cfg(not(windows))]
        if let Some(cayenne) = cayenne_append_target(self.0.accelerator.as_ref())
            && !cayenne.is_cdc_memory_mode()
        {
            cayenne
                .write_cdc_append_stream(input, &self.0.context.task_ctx())
                .await
                .map_err(DataFusionError::from)?
                .finish()
                .await
                .map_err(DataFusionError::from)?;
            return Ok(());
        }
        AppendPlanCache::append(
            &mut *self.0.plans.lock().await,
            &self.0.accelerator,
            &state,
            self.0.context.task_ctx(),
            input,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::super::{ClaimOutcome, InFlightRevalidations};
    use super::*;
    use arrow::array::{ArrayRef, StringArray};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use async_trait::async_trait;
    use datafusion::catalog::Session;
    use datafusion::datasource::TableType;
    use datafusion::execution::memory_pool::GreedyMemoryPool;
    use datafusion::logical_expr::{Expr, col, dml::InsertOp, lit};
    use datafusion::physical_plan::ExecutionPlan;
    use futures::StreamExt;
    use std::sync::atomic::AtomicUsize;
    use std::time::Duration;
    use tokio::sync::Notify;

    #[derive(Debug)]
    struct PausedInsert {
        inner: Arc<dyn TableProvider>,
        started: Notify,
        resume: Notify,
        fail_insert: AtomicBool,
        insert_thread: parking_lot::Mutex<Option<String>>,
    }

    #[async_trait]
    impl TableProvider for PausedInsert {
        fn schema(&self) -> SchemaRef {
            self.inner.schema()
        }
        fn table_type(&self) -> TableType {
            TableType::Base
        }
        async fn scan(
            &self,
            state: &dyn Session,
            projection: Option<&Vec<usize>>,
            filters: &[Expr],
            limit: Option<usize>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            self.inner.scan(state, projection, filters, limit).await
        }
        async fn insert_into(
            &self,
            state: &dyn Session,
            input: Arc<dyn ExecutionPlan>,
            op: InsertOp,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            *self.insert_thread.lock() = std::thread::current().name().map(str::to_owned);
            self.started.notify_one();
            self.resume.notified().await;
            if self.fail_insert.load(Ordering::Relaxed) {
                return Err(DataFusionError::Execution(
                    "accelerator insert failed".into(),
                ));
            }
            self.inner.insert_into(state, input, op).await
        }
    }

    fn fixture(pool: Arc<dyn MemoryPool>) -> (CacheWriter, Arc<PausedInsert>, RecordBatch) {
        fixture_with_runtimes(pool, Handle::current(), Handle::current())
    }

    fn fixture_with_runtimes(
        pool: Arc<dyn MemoryPool>,
        apply_runtime: Handle,
        io_runtime: Handle,
    ) -> (CacheWriter, Arc<PausedInsert>, RecordBatch) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("request_path", DataType::Utf8, false),
            Field::new("content", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["/a"])) as ArrayRef,
                Arc::new(StringArray::from(vec!["response"])) as ArrayRef,
            ],
        )
        .expect("response batch");
        let inner = Arc::new(
            data_components::arrow::write::MemTable::try_new(schema, vec![vec![]])
                .expect("empty accelerator"),
        );
        let paused = Arc::new(PausedInsert {
            inner,
            started: Notify::new(),
            resume: Notify::new(),
            fail_insert: AtomicBool::new(false),
            insert_thread: parking_lot::Mutex::new(None),
        });
        let writer = CacheWriter::new(
            Arc::clone(&paused) as Arc<dyn TableProvider>,
            TableReference::bare("test_dataset"),
            Arc::new(Mutex::new(())),
            Arc::new(AtomicI64::new(0)),
            RuntimeStatus::new(),
            pool,
            apply_runtime,
            io_runtime,
        );
        (writer, paused, batch)
    }

    fn request(batch: &RecordBatch) -> CacheWriteRequest {
        CacheWriteRequest {
            batches: vec![batch.clone()],
            filters: vec![col("request_path").eq(lit("/a"))],
            cache_key: "key".into(),
            replaces_existing: false,
            namespace_id: "public".into(),
        }
    }

    #[tokio::test]
    async fn preparation_refusal_does_not_wait_for_mutation_or_change_write_health() {
        for prior_failures in [0, 3] {
            let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1));
            let (writer, paused, batch) = fixture(Arc::clone(&pool));
            let status = Arc::clone(&writer.0.health.lock().runtime_status);
            status.update_dataset(&writer.0.dataset, runtime_status::ComponentStatus::Ready);
            for _ in 0..prior_failures {
                writer
                    .0
                    .health
                    .lock()
                    .record_failure(&"existing storage failure");
            }
            let original_message = status
                .get_dataset_status(&writer.0.dataset)
                .and_then(|status| status.error_message().map(str::to_owned));
            let claims: InFlightRevalidations =
                Arc::new(parking_lot::Mutex::new(Default::default()));
            let _write_guard = writer.0.write_mutex.lock().await;
            for _ in 0..3 {
                let ClaimOutcome::Leader(claim) =
                    CacheKeyClaim::acquire(&claims, "key".into(), None)
                else {
                    panic!("refused preparation must release its claim");
                };
                let permit = writer.admit().await.expect("admission");
                let error = tokio::time::timeout(
                    Duration::from_secs(5),
                    writer.write(request(&batch), claim, permit),
                )
                .await
                .expect("preparation must not wait for the held mutation lock")
                .expect_err("preparation exceeds the budget");
                assert!(matches!(error, DataFusionError::ResourcesExhausted(_)));
                assert_eq!(writer.0.health.lock().consecutive_failures, prior_failures);
                assert_eq!(
                    status
                        .get_dataset_status(&writer.0.dataset)
                        .and_then(|status| status.error_message().map(str::to_owned)),
                    original_message,
                );
                assert!(!writer.0.recovery_failed.load(Ordering::Acquire));
                assert_eq!(writer.0.last_updated_at.load(Ordering::Relaxed), 0);
                assert!(paused.insert_thread.lock().is_none());
                assert!(claims.lock().is_empty());
                assert_eq!(pool.reserved(), 0);
            }
            writer.shutdown().await;
        }
    }

    #[tokio::test]
    async fn mutation_failures_still_report_write_health_and_success_clears_them() {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1024 * 1024));
        let (writer, paused, batch) = fixture(Arc::clone(&pool));
        let status = Arc::clone(&writer.0.health.lock().runtime_status);
        status.update_dataset(&writer.0.dataset, runtime_status::ComponentStatus::Ready);
        paused.fail_insert.store(true, Ordering::Relaxed);
        for failures in 1..=3 {
            paused.resume.notify_one();
            let error = tokio::time::timeout(Duration::from_secs(5), writer.send(request(&batch)))
                .await
                .expect("write finishes")
                .expect_err("accelerator refuses the insert");
            assert!(error.to_string().contains("accelerator insert failed"));
            assert_eq!(writer.0.health.lock().consecutive_failures, failures);
            assert_eq!(writer.0.last_updated_at.load(Ordering::Relaxed), 0);
            assert_eq!(pool.reserved(), 0);
        }
        assert!(
            status
                .get_dataset_status(&writer.0.dataset)
                .expect("dataset status")
                .is_error()
        );
        paused.fail_insert.store(false, Ordering::Relaxed);
        paused.resume.notify_one();
        tokio::time::timeout(Duration::from_secs(5), writer.send(request(&batch)))
            .await
            .expect("write finishes")
            .expect("accelerator accepts the insert");
        assert_eq!(writer.0.health.lock().consecutive_failures, 0);
        assert_eq!(
            status.get_dataset_status(&writer.0.dataset),
            Some(runtime_status::ComponentStatus::Ready),
        );
        assert!(writer.0.last_updated_at.load(Ordering::Relaxed) > 0);
        assert_eq!(pool.reserved(), 0);
        writer.shutdown().await;
    }

    #[test]
    fn mutations_and_source_refreshes_use_their_assigned_runtimes() {
        let io = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .thread_name("cache-test-io")
            .enable_all()
            .build()
            .expect("I/O runtime");
        let apply = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .thread_name("cache-test-apply")
            .enable_all()
            .build()
            .expect("apply runtime");
        io.block_on(async {
            let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1024 * 1024));
            let (writer, paused, batch) =
                fixture_with_runtimes(pool, apply.handle().clone(), io.handle().clone());
            paused.resume.notify_one();
            tokio::time::timeout(Duration::from_secs(5), writer.send(request(&batch)))
                .await
                .expect("write finishes")
                .expect("cache write");
            assert_eq!(
                paused.insert_thread.lock().as_deref(),
                Some("cache-test-apply")
            );
            let (tx, rx) = tokio::sync::oneshot::channel();
            writer.spawn_refresh(async move {
                tx.send(std::thread::current().name().map(str::to_owned))
                    .expect("refresh observer");
            });
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(5), rx)
                    .await
                    .expect("refresh runs")
                    .expect("refresh reports its thread")
                    .as_deref(),
                Some("cache-test-io"),
            );
            tokio::time::timeout(Duration::from_secs(5), writer.shutdown())
                .await
                .expect("both runtimes drained");
        });
    }

    #[tokio::test]
    async fn cancellation_retains_the_claim_and_shutdown_waits_for_publication() {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1024 * 1024));
        let (writer, paused, batch) = fixture(Arc::clone(&pool));
        let in_flight: InFlightRevalidations =
            Arc::new(parking_lot::Mutex::new(Default::default()));
        let ClaimOutcome::Leader(claim) = CacheKeyClaim::acquire(&in_flight, "key".into(), None)
        else {
            panic!("first fetch owns the key");
        };
        let mut permit = writer.admit().await.expect("admission");
        permit.reserve(&batch).expect("response memory");
        let request = CacheWriteRequest {
            batches: vec![batch],
            filters: vec![col("request_path").eq(lit("/a"))],
            cache_key: "key".into(),
            replaces_existing: false,
            namespace_id: "public".into(),
        };
        let caller_writer = writer.clone();
        let caller = tokio::spawn(async move { caller_writer.write(request, claim, permit).await });
        tokio::time::timeout(Duration::from_secs(5), paused.started.notified())
            .await
            .expect("write started");
        caller.abort();
        assert!(caller.await.expect_err("caller cancelled").is_cancelled());
        assert!(in_flight.lock().contains_key("key"));
        assert!(pool.reserved() > 0);

        let shutdown = writer.shutdown();
        tokio::pin!(shutdown);
        assert!(futures::poll!(shutdown.as_mut()).is_pending());
        assert!(writer.try_admit().is_none());
        paused.resume.notify_one();
        tokio::time::timeout(Duration::from_secs(5), shutdown)
            .await
            .expect("write drained");
        assert!(in_flight.lock().is_empty());
        assert_eq!(pool.reserved(), 0);
        let batches = SessionContext::new()
            .read_table(Arc::clone(&paused.inner))
            .expect("read accelerator")
            .collect()
            .await
            .expect("stored response");
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
        assert!(writer.0.last_updated_at.load(Ordering::Relaxed) > 0);
    }

    #[tokio::test]
    async fn shutdown_cancels_an_unaccepted_refresh_and_releases_its_claim() {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1024 * 1024));
        let (writer, _, batch) = fixture(Arc::clone(&pool));
        let in_flight: InFlightRevalidations =
            Arc::new(parking_lot::Mutex::new(Default::default()));
        let ClaimOutcome::Leader(claim) = CacheKeyClaim::acquire(&in_flight, "key".into(), None)
        else {
            panic!("first fetch owns the key");
        };
        let mut permit = writer.try_admit().expect("available slot");
        permit.reserve(&batch).expect("response memory");
        let started = Arc::new(tokio::sync::Notify::new());
        let signal = Arc::clone(&started);
        writer.spawn_refresh(async move {
            let _owned = (claim, permit);
            signal.notify_one();
            futures::future::pending::<()>().await;
        });
        tokio::time::timeout(Duration::from_secs(5), started.notified())
            .await
            .expect("refresh started");
        assert!(in_flight.lock().contains_key("key"));
        tokio::time::timeout(Duration::from_secs(5), writer.shutdown())
            .await
            .expect("refresh cancelled");
        assert!(in_flight.lock().is_empty());
        assert_eq!(pool.reserved(), 0);
        assert!(writer.try_admit().is_none());
    }

    #[tokio::test]
    async fn closing_refuses_a_fill_admitted_before_the_fence() {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1024 * 1024));
        let (writer, paused, batch) = fixture(Arc::clone(&pool));
        assert!(Arc::ptr_eq(
            &writer.0.context.runtime_env().memory_pool,
            &pool
        ));
        let claims: InFlightRevalidations = Arc::new(parking_lot::Mutex::new(Default::default()));
        let super::super::ClaimOutcome::Leader(claim) =
            CacheKeyClaim::acquire(&claims, "key".into(), None)
        else {
            panic!("first fetch owns the key");
        };
        let mut permit = writer.admit().await.expect("admit before closing");
        permit.reserve(&batch).expect("reserve payload");
        writer.close();
        writer
            .write(
                CacheWriteRequest {
                    batches: vec![batch],
                    filters: vec![col("request_path").eq(lit("/a"))],
                    cache_key: "key".into(),
                    replaces_existing: false,
                    namespace_id: "public".into(),
                },
                claim,
                permit,
            )
            .await
            .expect_err("no mutation may begin after closing");
        tokio::time::timeout(Duration::from_secs(5), writer.shutdown())
            .await
            .expect("no accepted write to drain");
        assert!(claims.lock().is_empty());
        assert_eq!(pool.reserved(), 0);
        let batches = SessionContext::new()
            .read_table(Arc::clone(&paused.inner))
            .expect("read")
            .collect()
            .await
            .expect("scan");
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);
    }

    #[tokio::test]
    async fn source_context_keeps_session_services_and_uses_the_fill_pool() {
        #[derive(Debug)]
        struct SourceConfig;

        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(32));
        let (writer, _, _) = fixture(Arc::clone(&pool));
        let permit = writer.admit().await.expect("admission");
        let extension = Arc::new(SourceConfig);
        let state = datafusion::execution::SessionStateBuilder::new()
            .with_default_features()
            .with_config(
                datafusion::prelude::SessionConfig::new()
                    .with_target_partitions(3)
                    .with_extension(Arc::clone(&extension)),
            )
            .build();
        let context = permit.task_context(&state).expect("source context");
        assert!(Arc::ptr_eq(context.memory_pool(), &pool));
        assert_eq!(context.session_id(), state.session_id());
        assert_eq!(context.session_config().target_partitions(), 3);
        assert!(Arc::ptr_eq(
            &context
                .session_config()
                .get_extension::<SourceConfig>()
                .expect("source extension"),
            &extension,
        ));
        let original = state.runtime_env();
        let runtime = context.runtime_env();
        assert!(Arc::ptr_eq(&runtime.disk_manager, &original.disk_manager));
        assert!(Arc::ptr_eq(
            &runtime.object_store_registry,
            &original.object_store_registry,
        ));
        assert!(Arc::ptr_eq(
            &runtime.cache_manager.get_file_metadata_cache(),
            &original.cache_manager.get_file_metadata_cache(),
        ));
        assert!(!state.scalar_functions().is_empty());
        for (name, function) in state.scalar_functions() {
            assert!(Arc::ptr_eq(
                context
                    .scalar_functions()
                    .get(name)
                    .expect("source function"),
                function,
            ));
        }
        let reservation = MemoryConsumer::new("source operator").register(context.memory_pool());
        reservation.try_grow(32).expect("fill pool budget");
        assert!(reservation.try_grow(1).is_err());
        assert_eq!(pool.reserved(), 32);
        assert_eq!(original.memory_pool.reserved(), 0);
        drop(reservation);
        assert_eq!(pool.reserved(), 0);
    }

    #[tokio::test]
    async fn closing_cancels_admitted_source_planning() {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1024));
        let (writer, _, _) = fixture(pool);
        let permit = writer.admit().await.expect("admission");
        let mut planning = Box::pin(permit.run_source(futures::future::pending::<Result<()>>()));
        assert!(futures::poll!(planning.as_mut()).is_pending());
        writer.close();
        let error = planning
            .await
            .expect_err("planning is cancelled by closing");
        assert!(error.to_string().contains("cache writes are closed"));
        let polled = AtomicBool::new(false);
        permit
            .run_source(async {
                polled.store(true, Ordering::SeqCst);
                Ok(())
            })
            .await
            .expect_err("a closed permit cannot start source work");
        assert!(!polled.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn closing_cancels_collection_and_releases_its_partial_payload() {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1024 * 1024));
        let (writer, _, batch) = fixture(Arc::clone(&pool));
        let bytes = batch.get_array_memory_size();
        let schema = batch.schema();
        let mut permit = writer.admit().await.expect("admission");
        let source = futures::stream::iter([Ok(batch)]).chain(futures::stream::pending());
        let mut collect =
            Box::pin(permit.collect(Box::pin(RecordBatchStreamAdapter::new(schema, source))));
        assert!(futures::poll!(collect.as_mut()).is_pending());
        assert_eq!(pool.reserved(), bytes);
        writer.close();
        let error = collect
            .await
            .expect_err("collection is cancelled by closing");
        assert!(error.to_string().contains("cache writes are closed"));
        assert_eq!(pool.reserved(), 0);
        assert!(permit.collected_reservations.is_empty());
    }

    #[tokio::test]
    async fn collection_stops_at_the_budget_and_releases_only_its_own_reservation() {
        let setup_pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1024 * 1024));
        let (_, _, batch) = fixture(setup_pool);
        let bytes = batch.get_array_memory_size();
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(bytes * 2));
        let (writer, _, _) = fixture(Arc::clone(&pool));
        let mut permit = writer.admit().await.expect("admission");
        permit.reserve(&batch).expect("pre-existing payload");
        let polled = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&polled);
        let schema = batch.schema();
        let source = futures::stream::iter((0..3).map(move |_| {
            observed.fetch_add(1, Ordering::SeqCst);
            Ok(batch.clone())
        }));
        let error = permit
            .collect(Box::pin(RecordBatchStreamAdapter::new(schema, source)))
            .await
            .expect_err("second collected batch exceeds the remaining budget");
        assert!(matches!(error, DataFusionError::ResourcesExhausted(_)));
        assert_eq!(polled.load(Ordering::SeqCst), 2);
        assert_eq!(
            pool.reserved(),
            bytes,
            "failed collection releases its prefix"
        );
        drop(permit);
        assert_eq!(pool.reserved(), 0);
    }

    #[tokio::test]
    async fn cancelling_collection_releases_its_partial_payload() {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1024 * 1024));
        let (writer, _, batch) = fixture(Arc::clone(&pool));
        let bytes = batch.get_array_memory_size();
        let schema = batch.schema();
        let mut permit = writer.admit().await.expect("admission");
        let source = futures::stream::iter([Ok(batch)]).chain(futures::stream::pending());
        let mut collect =
            Box::pin(permit.collect(Box::pin(RecordBatchStreamAdapter::new(schema, source))));
        assert!(futures::poll!(collect.as_mut()).is_pending());
        assert_eq!(pool.reserved(), bytes);
        drop(collect);
        assert_eq!(pool.reserved(), 0);
        assert!(permit.collected_reservations.is_empty());
    }

    #[tokio::test]
    async fn source_errors_release_the_collected_prefix() {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1024 * 1024));
        let (writer, _, batch) = fixture(Arc::clone(&pool));
        let schema = batch.schema();
        let mut permit = writer.admit().await.expect("admission");
        let source = futures::stream::iter([
            Ok(batch),
            Err(DataFusionError::Execution("source failed".into())),
        ]);
        let error = permit
            .collect(Box::pin(RecordBatchStreamAdapter::new(schema, source)))
            .await
            .expect_err("source error");
        assert!(error.to_string().contains("source failed"));
        assert_eq!(pool.reserved(), 0);
        assert!(permit.collected_reservations.is_empty());
    }

    #[tokio::test]
    async fn completed_collection_stays_charged_until_the_fill_finishes() {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1024 * 1024));
        let (writer, _, batch) = fixture(Arc::clone(&pool));
        let bytes = batch.get_array_memory_size();
        let schema = batch.schema();
        let mut permit = writer.admit().await.expect("admission");
        let source = futures::stream::iter([Ok(batch)]);
        let batches = permit
            .collect(Box::pin(RecordBatchStreamAdapter::new(schema, source)))
            .await
            .expect("collect response");
        assert_eq!(batches.len(), 1);
        assert_eq!(pool.reserved(), bytes);
        drop(batches);
        drop(permit);
        assert_eq!(pool.reserved(), 0);
    }

    #[tokio::test]
    async fn admission_and_payload_memory_are_bounded() {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1));
        let (writer, _, batch) = fixture(Arc::clone(&pool));
        let mut permits: Vec<_> = (0..MAX_CONCURRENT_REFRESHES)
            .map(|_| writer.try_admit().expect("available slot"))
            .collect();
        assert!(writer.try_admit().is_none());
        permits[0]
            .reserve(&batch)
            .expect_err("payload exceeds budget");
        assert_eq!(pool.reserved(), 0);
        drop(permits);
        assert!(writer.try_admit().is_some());
        writer.shutdown().await;
        assert!(writer.admit().await.is_err());
    }
}
