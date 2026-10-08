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

use std::collections::BTreeMap;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::{
    Arc, OnceLock, Weak,
    atomic::{AtomicI64, AtomicU64, Ordering},
};
use std::time::Duration;

use arrow::array::{
    Array, ArrayData, ArrayDataBuilder, ArrayRef, AsArray, RecordBatch, RecordBatchOptions,
    make_array,
};
use arrow::buffer::{BooleanBuffer, Buffer, NullBuffer};
use arrow::datatypes::{DataType, SchemaRef};
use data_components::http::provider::{HttpExec, HttpFetchCompletion};
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{DataFusionError, Result, ScalarValue, TableReference};
use datafusion::datasource::TableProvider;
use datafusion::execution::context::{SessionContext, SessionState};
use datafusion::execution::{
    TaskContext,
    memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation},
    runtime_env::RuntimeEnv,
};
use datafusion::logical_expr::{Expr, Operator, col, lit};
use datafusion::physical_plan::{
    ChildrenPropertiesMode, ExecutionPlan, ReplaceChildrenOptions,
    coalesce_partitions::CoalescePartitionsExec, projection::ProjectionExec,
};
use futures::{FutureExt, TryStreamExt};
use runtime_acceleration::change_sink::batching::{AppendIngress, CoalescingLimits};
use runtime_acceleration::change_sink::{ChangeBatch, ChangeSink, Recovery, SetKey, WriteOptions};
use runtime_datafusion::execution_plan::schema_cast::SchemaCastScanExec;
use runtime_status::RuntimeStatus;
use tokio::runtime::Handle;
use tokio::sync::{mpsc, oneshot};
use tokio_util::task::TaskTracker;

use super::{
    CACHE_NAMESPACE_COLUMN, CACHE_WRITE_CHANNEL_CAPACITY, CACHE_WRITE_FLUSH_INTERVAL_MS,
    CacheKeyClaim, CacheRefreshHelper, CacheWriteHealth, CacheWriteRequest, InFlightRevalidations,
    REQUEST_KEY_COLUMNS, namespace_filter_expr, stamp_namespace_column,
};

/// Maximum time a cache fill waits for its writer's queue admission.
const ADMISSION_TIMEOUT: Duration = Duration::from_millis(100);

/// A frozen buffer charge shared only by consumers admitted to the same pool.
/// Other pools reserve the full retained capacity and keep the original charge
/// alive. Weak peers let concurrent consumers reuse an admission without keeping
/// an idle caller's pool charged for the lifetime of the original response.
#[derive(Debug)]
pub(super) struct RetainedBufferCharge {
    pool: Arc<dyn MemoryPool>,
    reservation: MemoryReservation,
    original: Option<Arc<Self>>,
    peers: Arc<parking_lot::Mutex<PeerRegistry>>,
}

#[derive(Debug)]
struct PeerCharge {
    pool: Weak<dyn MemoryPool>,
    charge: Weak<RetainedBufferCharge>,
}

const PEER_NODE_BYTES: usize =
    std::mem::size_of::<RetainedBufferCharge>() + 2 * std::mem::size_of::<usize>();

#[derive(Debug)]
struct PeerRegistry {
    entries: Vec<PeerCharge>,
    metadata: MemoryReservation,
}

impl PeerRegistry {
    #[cfg(test)]
    fn len(&self) -> usize {
        self.entries.len()
    }

    fn new(pool: &Arc<dyn MemoryPool>) -> Result<Self> {
        let metadata = MemoryConsumer::new("cache response sharing metadata").register(pool);
        metadata.try_grow(
            std::mem::size_of::<parking_lot::Mutex<Self>>() + 2 * std::mem::size_of::<usize>(),
        )?;
        Ok(Self {
            entries: Vec::new(),
            metadata,
        })
    }

    fn reserve_entry(&mut self) -> Result<()> {
        if self.entries.len() < self.entries.capacity() {
            return self.metadata.try_grow(PEER_NODE_BYTES);
        }
        let capacity = checked_copy_sum(self.entries.len(), 1)?;
        let bytes = checked_copy_product(capacity, std::mem::size_of::<PeerCharge>())?;
        let peak = checked_copy_sum(bytes, PEER_NODE_BYTES)?;
        self.metadata.try_grow(peak)?;
        // Allocate separately so refusal cannot leave a larger uncharged vector
        // attached to the live registry. Both vectors are charged during growth.
        let mut entries = Vec::new();
        if let Err(error) = entries.try_reserve_exact(capacity) {
            self.metadata.shrink(peak);
            return Err(DataFusionError::ResourcesExhausted(format!(
                "Cache sharing metadata allocation failed: {error}"
            )));
        }
        if entries.capacity() != capacity {
            drop(entries);
            self.metadata.shrink(peak);
            return Err(DataFusionError::ResourcesExhausted(
                "Cache sharing metadata exceeds its admitted capacity".into(),
            ));
        }
        let old_bytes = self.entries.capacity() * std::mem::size_of::<PeerCharge>();
        entries.append(&mut self.entries);
        drop(std::mem::replace(&mut self.entries, entries));
        self.metadata.shrink(old_bytes);
        Ok(())
    }
}

impl Drop for RetainedBufferCharge {
    fn drop(&mut self) {
        let this = std::ptr::from_ref(self);
        let mut peers = self.peers.lock();
        let before = peers.entries.len();
        peers
            .entries
            .retain(|peer| !std::ptr::eq(peer.charge.as_ptr(), this));
        if peers.entries.len() < before {
            peers.metadata.shrink(PEER_NODE_BYTES);
        }
    }
}

impl RetainedBufferCharge {
    pub(super) fn new(
        pool: &Arc<dyn MemoryPool>,
        reservation: MemoryReservation,
    ) -> Result<Arc<Self>> {
        let mut peers = PeerRegistry::new(pool)?;
        peers.reserve_entry()?;
        Ok(Arc::new_cyclic(|this| {
            peers.entries.push(PeerCharge {
                pool: Arc::downgrade(pool),
                charge: Weak::clone(this),
            });
            Self {
                pool: Arc::clone(pool),
                reservation,
                original: None,
                peers: Arc::new(parking_lot::Mutex::new(peers)),
            }
        }))
    }

    #[cfg(test)]
    pub(super) fn metadata_bytes(&self) -> usize {
        self.peers.lock().metadata.size()
    }

    pub(super) fn for_batches(
        pool: &Arc<dyn MemoryPool>,
        batches: &[arrow::array::RecordBatch],
    ) -> Result<Arc<Self>> {
        let reservation = MemoryConsumer::new("cache retained response").register(pool);
        for batch in batches {
            reserve_batch(&reservation, batch)?;
        }
        Self::new(pool, reservation)
    }

    pub(super) fn retain_for_pool(
        self: &Arc<Self>,
        pool: &Arc<dyn MemoryPool>,
    ) -> Result<Arc<Self>> {
        if Arc::ptr_eq(&self.pool, pool) {
            return Ok(Arc::clone(self));
        }
        // Pool identity is deliberately conservative: opaque wrappers over one
        // underlying pool still require independent admission.
        let mut peers = self.peers.lock();
        let identity = Arc::downgrade(pool);
        // Do not upgrade unrelated peers under the lock: dropping their last
        // strong reference would re-enter this registry from their destructor.
        for peer in &peers.entries {
            if Weak::ptr_eq(&peer.pool, &identity)
                && let Some(charge) = peer.charge.upgrade()
            {
                return Ok(charge);
            }
        }
        let reservation = MemoryConsumer::new("cache shared response").register(pool);
        reservation.try_grow(self.reservation.size())?;
        peers.reserve_entry()?;
        let charge = Arc::new(Self {
            pool: Arc::clone(pool),
            reservation,
            original: Some(Arc::clone(self.original.as_ref().unwrap_or(self))),
            peers: Arc::clone(&self.peers),
        });
        peers.entries.push(PeerCharge {
            pool: identity,
            charge: Arc::downgrade(&charge),
        });
        Ok(charge)
    }
}

/// Cache write admission, bound once to the composed table generation.
#[derive(Clone)]
pub enum CacheWriteSender {
    Batched(mpsc::Sender<CacheWriteRequest>),
    Sink(Arc<CacheSinkWriter>),
}

/// A synchronized child always uses its own writer and key claims.
#[derive(Clone)]
pub struct SynchronizedCacheTarget {
    pub accelerator: Arc<dyn TableProvider>,
    pub writer: CacheWriteSender,
    pub in_flight: InFlightRevalidations,
}

/// Completion of preparation and fanout registered before a cache generation
/// was fenced. Waiting does not stop or take ownership of those jobs.
#[derive(Clone)]
pub struct CacheWorkDrain {
    work: Arc<CacheWork>,
}

impl CacheWorkDrain {
    /// Wait for every accepted cache job to finish. A finished job proves its
    /// cleanup whatever its outcome: it reports its own failure, and storage
    /// failures are latched by the sink, whose close fails the generation drain.
    pub async fn wait(&self) {
        self.work.tasks.wait().await;
    }
}

struct CacheWork {
    tasks: TaskTracker,
    closed: parking_lot::Mutex<bool>,
}

/// Cache policy and completion effects for an already-bound table owner.
#[derive(Clone)]
pub struct CacheSinkWriter {
    sink: ChangeSink,
    schema: SchemaRef,
    health: Arc<parking_lot::Mutex<CacheWriteHealth>>,
    last_updated_at: Arc<AtomicI64>,
    memory_pool: Arc<dyn MemoryPool>,
    task_context: Option<Arc<TaskContext>>,
    work: Arc<CacheWork>,
    append_ingress: Arc<AppendIngress>,
    freshness: Arc<CacheFreshness>,
    scan_revision: Option<u64>,
}

#[derive(Default)]
struct CacheFreshness {
    revision: AtomicU64,
    claims: OnceLock<InFlightRevalidations>,
}

impl CacheFreshness {
    fn observe(&self) -> Option<u64> {
        let revision = self.revision.load(Ordering::Acquire);
        (revision != u64::MAX).then_some(revision)
    }

    fn bind_claim(&self, claim: &CacheKeyClaim) -> Result<()> {
        let claims = self.claims.get_or_init(|| Arc::clone(&claim.in_flight));
        if !Arc::ptr_eq(claims, &claim.in_flight) {
            return Err(DataFusionError::Internal(
                "A native cache generation requires one shared scope-claim map".into(),
            ));
        }
        Ok(())
    }

    fn confirm_empty_scan(&self, observation: Option<u64>, claim: &mut CacheKeyClaim) {
        claim.proven_fresh = self.bind_claim(claim).is_ok()
            && observation
                .is_some_and(|revision| revision == self.revision.load(Ordering::Acquire));
    }
}

/// Invalidates observations before releasing a claim, including when an
/// accepted callback or a refused admission is dropped without invocation.
struct CachePublication {
    freshness: Arc<CacheFreshness>,
    _claim: CacheKeyClaim,
    _reservation: MemoryReservation,
}

impl Drop for CachePublication {
    fn drop(&mut self) {
        let _ =
            self.freshness
                .revision
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |revision| {
                    Some(revision.saturating_add(1))
                });
    }
}

impl CacheWriteSender {
    /// Bind table-owned cache allocations to the runtime's configured pool.
    /// Helper planning sessions must not select an unlimited fallback pool.
    #[must_use]
    pub fn from_sink(
        sink: ChangeSink,
        schema: SchemaRef,
        dataset: TableReference,
        runtime_status: Arc<RuntimeStatus>,
        last_updated_at: Arc<AtomicI64>,
        memory_pool: Arc<dyn MemoryPool>,
    ) -> Self {
        let append_ingress = Arc::new(AppendIngress::new(
            &dataset,
            CoalescingLimits {
                max_inputs: CACHE_WRITE_CHANNEL_CAPACITY,
                max_bytes: 8 * 1024 * 1024,
                max_age: Duration::from_millis(CACHE_WRITE_FLUSH_INTERVAL_MS),
            },
        ));
        Self::Sink(Arc::new(CacheSinkWriter {
            sink,
            schema,
            health: Arc::new(parking_lot::Mutex::new(CacheWriteHealth::new(
                runtime_status,
                dataset,
            ))),
            last_updated_at,
            memory_pool,
            task_context: None,
            work: Arc::new(CacheWork {
                tasks: TaskTracker::new(),
                closed: parking_lot::Mutex::new(false),
            }),
            append_ingress,
            freshness: Arc::new(CacheFreshness::default()),
            scan_revision: None,
        }))
    }

    /// Capture before planning a storage scan, since planning may bind a snapshot.
    /// An empty result proves absence only after the same scope has been claimed
    /// and no intervening publication has invalidated this observation.
    #[must_use]
    pub fn observe_cache_scan(&self) -> Self {
        match self {
            Self::Batched(_) => self.clone(),
            Self::Sink(writer) => {
                let mut writer = writer.as_ref().clone();
                writer.scan_revision = writer.freshness.observe();
                Self::Sink(Arc::new(writer))
            }
        }
    }

    /// Forget the storage observation, so no empty scan can prove absence.
    #[must_use]
    pub(super) fn without_scan_observation(&self) -> Self {
        match self {
            Self::Batched(_) => self.clone(),
            Self::Sink(writer) => {
                let mut writer = writer.as_ref().clone();
                writer.scan_revision = None;
                Self::Sink(Arc::new(writer))
            }
        }
    }

    pub(super) fn confirm_empty_scan(&self, claim: &mut CacheKeyClaim) {
        if let Self::Sink(writer) = self {
            writer
                .freshness
                .confirm_empty_scan(writer.scan_revision, claim);
        }
    }

    /// Fence preparation and fanout before closing the generation's sink.
    /// The generation owner must await these jobs before closing the sink,
    /// outside registry and accelerator locks. Sink flush does not await them.
    #[must_use]
    pub fn begin_drain(&self) -> Option<CacheWorkDrain> {
        let Self::Sink(writer) = self else {
            return None;
        };
        let mut closed = writer.work.closed.lock();
        *closed = true;
        writer.work.tasks.close();
        Some(CacheWorkDrain {
            work: Arc::clone(&writer.work),
        })
    }

    pub(super) fn spawn_owned(
        &self,
        runtime: &Handle,
        work: impl Future<Output = Result<()>> + Send + 'static,
    ) -> Result<()> {
        match self {
            Self::Batched(_) => {
                drop(runtime.spawn(work));
            }
            Self::Sink(writer) => {
                let closed = writer.work.closed.lock();
                if *closed {
                    return Err(DataFusionError::Execution(
                        "Cache generation is draining; new cache preparation is refused".into(),
                    ));
                }
                let dataset = writer.health.lock().dataset.clone();
                drop(writer.work.tasks.spawn_on(
                    async move {
                        // A job reports its own failure; only a panic is reported here.
                        if AssertUnwindSafe(work).catch_unwind().await.is_err() {
                            tracing::error!(
                                "A cache population task for dataset '{dataset}' panicked, so its response was not cached and the next query for it will be answered from the origin."
                            );
                        }
                    },
                    runtime,
                ));
            }
        }
        Ok(())
    }

    /// Use the request's context and pool without creating another sink owner.
    /// Original and prepared buffers retain this pool through publication.
    #[must_use]
    pub fn with_task_context(&self, task_context: Arc<TaskContext>) -> Self {
        match self {
            Self::Batched(_) => self.clone(),
            Self::Sink(writer) => {
                let mut writer = writer.as_ref().clone();
                writer.memory_pool = Arc::clone(task_context.memory_pool());
                writer.task_context = Some(task_context);
                Self::Sink(Arc::new(writer))
            }
        }
    }

    pub(super) fn task_context(&self, state: &SessionState) -> Arc<TaskContext> {
        match self {
            Self::Batched(_) => state.task_ctx(),
            Self::Sink(writer) => writer
                .task_context
                .as_ref()
                .map_or_else(|| state.task_ctx(), Arc::clone),
        }
    }

    pub(super) fn memory_pool(&self) -> Option<&Arc<dyn MemoryPool>> {
        match self {
            Self::Batched(_) => None,
            Self::Sink(writer) => Some(&writer.memory_pool),
        }
    }

    pub(super) fn session_context(&self) -> SessionContext {
        match self {
            Self::Batched(_) => util::session_state::session_context(),
            Self::Sink(writer) => {
                let (config, runtime) = writer.task_context.as_ref().map_or_else(
                    || {
                        (
                            util::session_state::session_config(),
                            Arc::new(RuntimeEnv {
                                memory_pool: Arc::clone(&writer.memory_pool),
                                ..RuntimeEnv::default()
                            }),
                        )
                    },
                    |context| (context.session_config().clone(), context.runtime_env()),
                );
                // This temporary planning context has no registered runtime tables.
                // The writer retains resources, never the context or its catalog.
                SessionContext::new_with_config_rt(config, runtime)
            }
        }
    }

    pub(super) async fn collect_snapshot(
        &self,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<(
        Vec<arrow::array::RecordBatch>,
        Option<Arc<RetainedBufferCharge>>,
    )> {
        let context = self.session_context();
        let task_context = self.task_context(&context.state());
        let charge = self
            .memory_pool()
            .map(|pool| MemoryConsumer::new("cache retained response").register(pool));
        let mut stream = datafusion::physical_plan::execute_stream(plan, task_context)?;
        let mut batches = Vec::new();
        while let Some(batch) = stream.try_next().await? {
            let batch = match &charge {
                Some(charge) => own_snapshot_batch(batch, charge)?,
                None => batch,
            };
            batches.push(batch);
        }
        let charge = charge
            .zip(self.memory_pool())
            .map(|(reservation, pool)| RetainedBufferCharge::new(pool, reservation))
            .transpose()?;
        Ok((batches, charge))
    }

    pub(super) fn reserve_input(
        &self,
        batches: &[arrow::array::RecordBatch],
    ) -> Result<Option<Arc<RetainedBufferCharge>>> {
        self.memory_pool()
            .map(|pool| RetainedBufferCharge::for_batches(pool, batches))
            .transpose()
    }

    pub(super) fn retain_input(
        &self,
        batches: &[arrow::array::RecordBatch],
        charge: Option<&Arc<RetainedBufferCharge>>,
    ) -> Result<Option<Arc<RetainedBufferCharge>>> {
        match (self.memory_pool(), charge) {
            (Some(pool), Some(charge)) => charge.retain_for_pool(pool).map(Some),
            (None, Some(charge)) => Ok(Some(Arc::clone(charge))),
            (_, None) => self.reserve_input(batches),
        }
    }

    pub(super) fn reserve_work_metadata(
        &self,
        request: &CacheWriteRequest,
        claim: &CacheKeyClaim,
    ) -> Result<MemoryReservation> {
        let Self::Sink(writer) = self else {
            return Err(DataFusionError::NotImplemented(
                "Batched cache work uses its queue owner".into(),
            ));
        };
        let reservation =
            MemoryConsumer::new("cache preparation metadata").register(&writer.memory_pool);
        let mut bytes = std::mem::size_of::<super::NativeCacheWrite>()
            .saturating_add(request.cache_key.capacity())
            .saturating_add(claim.key.capacity())
            .saturating_add(claim.key.len())
            .saturating_add(request.namespace_id.len())
            .saturating_add(
                request
                    .batches
                    .capacity()
                    .saturating_mul(std::mem::size_of::<arrow::array::RecordBatch>())
                    .saturating_mul(2),
            );
        for filter in &request.filters {
            filter.apply(|expression| {
                bytes = bytes.saturating_add(std::mem::size_of::<Expr>());
                match expression {
                    Expr::Literal(value, _) => bytes = bytes.saturating_add(value.size()),
                    Expr::Column(column) => bytes = bytes.saturating_add(column.name.capacity()),
                    _ => {}
                }
                Ok(TreeNodeRecursion::Continue)
            })?;
        }
        // The job retains its request and a separate copy for child fanout.
        reservation.try_grow(bytes.saturating_mul(2))?;
        Ok(reservation)
    }

    #[must_use]
    pub fn requires_complete_fetch(&self) -> bool {
        matches!(self, Self::Sink(_))
    }

    /// Send to a batched cache consumer. Native writes require `send_claimed`
    /// so exclusive scope ownership spans observation, fetch and publication.
    ///
    /// # Errors
    ///
    /// Returns an error when the writer is closed or does not admit the request
    /// within the admission timeout.
    pub async fn send(&self, request: CacheWriteRequest) -> Result<()> {
        match self {
            Self::Batched(sender) => tokio::time::timeout(ADMISSION_TIMEOUT, sender.send(request))
                .await
                .map_err(|_| admission_timeout())?
                .map_err(|error| DataFusionError::External(Box::new(error))),
            Self::Sink(writer) => writer.enqueue(request, None, None).await,
        }
    }

    /// Transfer both input and claim to the writer. Accepted writes retain the
    /// claim even if the fetching query is cancelled or drops its response.
    ///
    /// # Errors
    ///
    /// Returns an error when the writer is closed or does not admit the request
    /// within the admission timeout. A refused request releases its claim.
    pub async fn send_claimed(
        &self,
        request: CacheWriteRequest,
        claim: CacheKeyClaim,
    ) -> Result<()> {
        match self {
            Self::Batched(sender) => {
                let permit = tokio::time::timeout(ADMISSION_TIMEOUT, sender.reserve())
                    .await
                    .map_err(|_| admission_timeout())?
                    .map_err(|error| DataFusionError::External(Box::new(error)))?;
                // No await between releasing local ownership and transferring the request.
                claim.into_queued();
                permit.send(request);
                Ok(())
            }
            Self::Sink(writer) => writer.enqueue(request, Some(claim), None).await,
        }
    }

    /// Wait for a sink-owned background refresh or child initialization to publish.
    /// Cancelling this observer does not cancel accepted storage work.
    ///
    /// # Errors
    ///
    /// Returns an error for a batched writer, when the request is not admitted,
    /// or when the accepted write fails to publish.
    pub async fn send_claimed_and_wait(
        &self,
        request: CacheWriteRequest,
        claim: CacheKeyClaim,
    ) -> Result<()> {
        let Self::Sink(writer) = self else {
            return Err(DataFusionError::NotImplemented(
                "Batched cache writers do not expose per-request publication".into(),
            ));
        };
        let (completed, completion) = oneshot::channel();
        writer
            .enqueue(request, Some(claim), Some(completed))
            .await?;
        completion
            .await
            .map_err(|error| DataFusionError::External(Box::new(error)))?
    }
}

impl CacheSinkWriter {
    fn prepare(
        &self,
        request: CacheWriteRequest,
        reservation: &MemoryReservation,
        input_is_charged: bool,
    ) -> Result<ChangeBatch> {
        if !input_is_charged {
            for batch in &request.batches {
                reserve_batch(reservation, batch)?;
            }
        }
        let batches = request
            .batches
            .into_iter()
            .map(|batch| {
                let input_columns = batch.columns().to_vec();
                let stamped = stamp_namespace_column(batch, &self.schema, &request.namespace_id)?;
                let cast =
                    arrow_tools::record_batch::try_cast_to(stamped, Arc::clone(&self.schema))?;
                // Identical array Arcs are covered by the retained input charge.
                // Distinct arrays are charged at full backing capacity, even if sliced.
                let extra = cast
                    .columns()
                    .iter()
                    .filter(|column| !input_columns.iter().any(|input| Arc::ptr_eq(input, column)))
                    .map(Array::get_array_memory_size)
                    .fold(0, usize::saturating_add);
                reservation.try_grow(extra)?;
                Ok(cast)
            })
            .collect::<Result<Vec<_>>>()?;
        let canonical = canonical_request_filters(&request.filters);
        if request.replaces_existing
            && batches.iter().all(|batch| batch.num_rows() == 0)
            && (canonical
                .as_ref()
                .is_none_or(|filters| filters.len() != REQUEST_KEY_COLUMNS.len())
                || REQUEST_KEY_COLUMNS
                    .iter()
                    .any(|name| self.schema.column_with_name(name).is_none()))
        {
            return Err(DataFusionError::Plan(
                "An empty cache replacement requires the complete logical request key".into(),
            ));
        }
        let mut filters = canonical
            .map(|filters| {
                filters
                    .into_iter()
                    .filter(|filter| {
                        filter
                            .column_refs()
                            .iter()
                            .all(|column| self.schema.column_with_name(&column.name).is_some())
                    })
                    .collect()
            })
            .unwrap_or(request.filters);
        // Include omitted request dimensions from the response so replacing one
        // query/body does not delete other responses stored under the same path.
        if let Some(first) = batches.iter().find(|batch| batch.num_rows() > 0) {
            filters.extend(CacheRefreshHelper::extract_filters_from_row(first, 0)?);
        }
        if filters.is_empty() {
            return Err(DataFusionError::Plan(
                "A cache replacement requires request grouping columns, not only a namespace"
                    .into(),
            ));
        }
        if self
            .schema
            .column_with_name(CACHE_NAMESPACE_COLUMN)
            .is_some()
        {
            filters.push(namespace_filter_expr(&request.namespace_id));
        }
        let scope = SetKey::from_filters(Arc::clone(&self.schema), &filters)?;
        let row_bytes = batches
            .iter()
            .map(arrow::array::RecordBatch::get_array_memory_size)
            .fold(0, usize::saturating_add);
        let batch = if request.replaces_existing {
            ChangeBatch::replace_set(scope, Arc::clone(&self.schema), batches)?
        } else {
            ChangeBatch::append_scoped(scope, Arc::clone(&self.schema), batches)?
                .with_append_ingress(Arc::clone(&self.append_ingress))?
        };
        reservation.try_grow(batch.estimated_bytes().saturating_sub(row_bytes))?;
        Ok(batch)
    }

    async fn enqueue(
        &self,
        mut request: CacheWriteRequest,
        claim: Option<CacheKeyClaim>,
        completed: Option<oneshot::Sender<Result<()>>>,
    ) -> Result<()> {
        let mut claim = claim.ok_or_else(|| {
            DataFusionError::Internal(
                "Native cache writes require exclusive scope ownership".into(),
            )
        })?;
        self.freshness.bind_claim(&claim)?;
        request.replaces_existing |= !claim.proven_fresh;
        if !request.replaces_existing && request.batches.iter().all(|batch| batch.num_rows() == 0) {
            if let Some(completed) = completed {
                let _ = completed.send(Ok(()));
            }
            return Ok(());
        }
        claim.input_charge = claim
            .input_charge
            .take()
            .map(|charge| charge.retain_for_pool(&self.memory_pool))
            .transpose()?;
        let reservation =
            MemoryConsumer::new("cache write preparation").register(&self.memory_pool);
        let input_is_charged = claim.input_charge.is_some();
        let batch = match self.prepare(request, &reservation, input_is_charged) {
            Ok(batch) => batch,
            Err(error) => {
                self.health.lock().record_failure(&error);
                return Err(error);
            }
        };
        let changed = batch.num_rows() > 0 || batch.replacement_scope().is_some();
        let health = Arc::clone(&self.health);
        let last_updated_at = Arc::clone(&self.last_updated_at);
        let publication = CachePublication {
            freshness: Arc::clone(&self.freshness),
            _claim: claim,
            _reservation: reservation,
        };
        let completion = Box::new(move |result: Result<()>| {
            {
                let mut health = health.lock();
                match &result {
                    Ok(()) if changed => {
                        health.record_success();
                        crate::accelerated::AcceleratedTable::set_timestamp_to_now(
                            &last_updated_at,
                        );
                    }
                    Ok(()) => {}
                    Err(error) => {
                        tracing::warn!(
                            "Failed to write cached responses for dataset '{}', so those entries are not cached and the next query for them will be answered from the origin. Cause: {error}",
                            health.dataset,
                        );
                        health.record_failure(error);
                    }
                }
            }
            drop(publication);
            if let Some(completed) = completed {
                let _ = completed.send(result);
            }
        });
        let options = WriteOptions {
            recovery: Recovery::Rebuildable,
            ..WriteOptions::default()
        };
        let result = tokio::time::timeout(
            ADMISSION_TIMEOUT,
            self.sink.enqueue(batch, options, completion),
        )
        .await
        .map_err(|_| admission_timeout())
        .and_then(std::convert::identity);
        if let Err(error) = &result {
            self.health.lock().record_failure(error);
        }
        result
    }
}

/// Give each cache-owned HTTP fetch a fresh exhaustion token. Only an unbounded
/// single-request scope can use that token to authorize population.
pub(super) fn prepare_source_fetch(
    plan: Arc<dyn ExecutionPlan>,
    filters: &[Expr],
    limit: Option<usize>,
) -> Result<(Arc<dyn ExecutionPlan>, Option<HttpFetchCompletion>)> {
    if let Some((tracked, completion, single_unbounded_request)) = tracked_http_plan(&plan)? {
        let eligible = limit.is_none()
            && single_unbounded_request
            && canonical_request_filters(filters).is_some();
        return Ok((tracked, eligible.then_some(completion)));
    }
    Ok((plan, None))
}

/// Accept only request-key predicates that describe one request. This does not
/// infer omitted dimensions or normalize HTTP metadata; nonempty response rows
/// supply the storage scope. Response predicates cannot authorize replacement.
pub(super) fn canonical_request_filters(filters: &[Expr]) -> Option<Vec<Expr>> {
    let mut values: BTreeMap<usize, Option<&str>> = BTreeMap::new();
    let mut pending: Vec<_> = filters.iter().collect();
    while let Some(expression) = pending.pop() {
        let (column, literal) = match expression {
            Expr::BinaryExpr(binary) if binary.op == Operator::And => {
                pending.push(&binary.left);
                pending.push(&binary.right);
                continue;
            }
            Expr::BinaryExpr(binary)
                if matches!(binary.op, Operator::Eq | Operator::IsNotDistinctFrom) =>
            {
                match (binary.left.as_ref(), binary.right.as_ref()) {
                    (Expr::Column(column), Expr::Literal(value, _))
                    | (Expr::Literal(value, _), Expr::Column(column))
                        if binary.op == Operator::IsNotDistinctFrom || !value.is_null() =>
                    {
                        (column, Some(value))
                    }
                    _ => return None,
                }
            }
            Expr::IsNull(inner) => match inner.as_ref() {
                Expr::Column(column) => (column, None),
                _ => return None,
            },
            _ => return None,
        };
        if column.relation.is_some() {
            return None;
        }
        let index = REQUEST_KEY_COLUMNS
            .iter()
            .position(|name| *name == column.name)?;
        let value = match literal {
            None | Some(ScalarValue::Null) => None,
            Some(
                ScalarValue::Utf8(value)
                | ScalarValue::LargeUtf8(value)
                | ScalarValue::Utf8View(value),
            ) => value.as_deref(),
            _ => return None,
        };
        if let Some(previous) = values.insert(index, value)
            && previous != value
        {
            return None;
        }
    }
    Some(
        values
            .into_iter()
            .map(|(index, value)| match value {
                Some(value) => col(REQUEST_KEY_COLUMNS[index]).eq(lit(value)),
                None => col(REQUEST_KEY_COLUMNS[index]).is_null(),
            })
            .collect(),
    )
}

/// Columns the HTTP connector builds a request from. It ignores predicates on
/// every other column, which filter the response.
const HTTP_REQUEST_COLUMNS: [&str; 4] = [
    "request_path",
    "request_query",
    "request_body",
    "request_headers",
];

/// The request-key conjuncts of a read that also filters response columns.
/// The origin answers that request with its whole response whatever the
/// response predicates are, so the read can fill the cache for the request.
/// Returns `None` when no conjunct filters only the response, or when the
/// remaining conjuncts do not describe one request.
pub(super) fn response_filtered_request(filters: &[Expr]) -> Option<Vec<Expr>> {
    let mut request = Vec::new();
    let mut filters_response = false;
    let mut pending: Vec<_> = filters.iter().rev().collect();
    while let Some(expression) = pending.pop() {
        if let Expr::BinaryExpr(binary) = expression
            && binary.op == Operator::And
        {
            pending.push(&binary.right);
            pending.push(&binary.left);
            continue;
        }
        if expression
            .column_refs()
            .iter()
            .any(|column| HTTP_REQUEST_COLUMNS.contains(&column.name.as_str()))
        {
            request.push(expression.clone());
        } else {
            filters_response = true;
        }
    }
    (filters_response && !request.is_empty() && canonical_request_filters(&request).is_some())
        .then_some(request)
}

/// A cache-fetch plan, its completion token, and whether it can authorize a replacement.
type TrackedHttpPlan = (Arc<dyn ExecutionPlan>, HttpFetchCompletion, bool);

fn tracked_http_plan(plan: &Arc<dyn ExecutionPlan>) -> Result<Option<TrackedHttpPlan>> {
    if let Some(http) = plan.downcast_ref::<HttpExec>() {
        // The completion token must also prove that execution did not follow
        // another page: page metadata can describe different storage keys.
        let eligible = http.limit().is_none() && http.partitions().len() == 1;
        let (plan, completion) = http.for_cache_fetch();
        return Ok(Some((Arc::new(plan), completion, eligible)));
    }
    if plan.is::<ProjectionExec>()
        || plan.is::<SchemaCastScanExec>()
        || plan.is::<CoalescePartitionsExec>()
    {
        let children = plan.children();
        if children.len() == 1
            && let Some((child, completion, eligible)) = tracked_http_plan(children[0])?
        {
            return Ok(Some((
                Arc::clone(plan).replace_children(
                    vec![child],
                    ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
                )?,
                completion,
                eligible,
            )));
        }
    }
    Ok(None)
}

/// Own snapshot buffers before retaining them past the storage scan. Foreign
/// buffer lengths size the destination copy, never the producer's allocation.
/// Decoder memory remains outside this retention budget.
fn own_snapshot_batch(batch: RecordBatch, reservation: &MemoryReservation) -> Result<RecordBatch> {
    // Foreign-buffer inspection also constructs ArrayData. Admit descriptors
    // before invoking it, using only borrowed standard-array child accessors.
    let descriptors = reservation.new_empty();
    let mut descriptor_budget = SnapshotDescriptorBudget::default();
    for column in batch.columns() {
        descriptor_budget.include(snapshot_descriptor_budget(column.as_ref(), 0)?)?;
    }
    descriptors.try_grow(descriptor_budget.bytes)?;
    if !arrow_tools::record_batch::rests_on_unowned_memory(&batch) {
        reservation.try_grow(checked_copy_sum(
            batch.get_array_memory_size(),
            descriptor_budget.datatype_bytes,
        )?)?;
        return Ok(batch);
    }

    let data: Vec<_> = batch.columns().iter().map(Array::to_data).collect();
    let mut budget = SnapshotCopyBudget::default();
    let mut array_metadata = 0usize;
    for (column, data) in batch.columns().iter().zip(&data) {
        budget.visit(data, 0)?;
        array_metadata = checked_copy_sum(
            array_metadata,
            column
                .get_array_memory_size()
                .saturating_sub(column.get_buffer_memory_size()),
        )?;
    }
    // Source and destination array objects coexist with input descriptors,
    // rebuilt descriptors and validation/build temporaries during preparation.
    let preparation = checked_copy_sum(
        checked_copy_product(budget.metadata, 3)?,
        checked_copy_product(array_metadata, 2)?,
    )?;
    let peak = checked_copy_sum(
        checked_copy_sum(budget.copy, budget.owned_input)?,
        preparation,
    )?;
    reservation.try_grow(peak)?;
    let copied = (|| -> Result<(RecordBatch, usize)> {
        let columns = data
            .into_iter()
            .map(copy_snapshot_data)
            .map(|data| data.map(make_array))
            .collect::<Result<Vec<_>>>()?;
        let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
        let copied = RecordBatch::try_new_with_options(batch.schema(), columns, &options)?;
        drop(batch);
        if arrow_tools::record_batch::rests_on_unowned_memory(&copied) {
            return Err(DataFusionError::ResourcesExhausted(
                "Cache snapshot copy still retains externally owned buffers".into(),
            ));
        }
        let retained = checked_copy_sum(
            copied.get_array_memory_size(),
            descriptor_budget.datatype_bytes,
        )?;
        if retained > peak {
            return Err(DataFusionError::ResourcesExhausted(
                "Cache snapshot copy exceeds its admitted allocation budget".into(),
            ));
        }
        Ok((copied, retained))
    })();
    match copied {
        Ok((batch, retained)) => {
            // Only the owned result survives; release dropped input and copy
            // temporaries, not capacity still retained by the result's buffers.
            reservation.shrink(peak - retained);
            Ok(batch)
        }
        Err(error) => {
            reservation.shrink(peak);
            Err(error)
        }
    }
}

#[derive(Default, Clone, Copy)]
struct SnapshotDescriptorBudget {
    bytes: usize,
    datatype_bytes: usize,
}

impl SnapshotDescriptorBudget {
    fn include(&mut self, other: Self) -> Result<()> {
        self.bytes = checked_copy_sum(self.bytes, other.bytes)?;
        self.datatype_bytes = checked_copy_sum(self.datatype_bytes, other.datatype_bytes)?;
        Ok(())
    }
}

// Dictionary datatype clones allocate their boxed key/value types recursively.
// Field and timezone containers are shared Arcs and do not allocate on clone.
fn datatype_clone_bytes(data_type: &DataType, depth: usize) -> Result<usize> {
    if depth >= 64 {
        return Err(snapshot_layout_error(data_type));
    }
    let DataType::Dictionary(key, value) = data_type else {
        return Ok(0);
    };
    checked_copy_sum(
        2 * std::mem::size_of::<DataType>(),
        checked_copy_sum(
            datatype_clone_bytes(key, depth + 1)?,
            datatype_clone_bytes(value, depth + 1)?,
        )?,
    )
}

/// Bounds descriptor construction without allocating `ArrayData` or cloning a
/// container. Two descriptor sets cover conversion temporaries plus the stored
/// set. Variable byte-view buffer counts come from the array, not its row count.
fn snapshot_descriptor_budget(array: &dyn Array, depth: usize) -> Result<SnapshotDescriptorBudget> {
    if depth >= 64 {
        return Err(snapshot_layout_error(array.data_type()));
    }
    let mut children = SnapshotDescriptorBudget::default();
    let buffers = match array.data_type() {
        DataType::Utf8View => checked_copy_sum(
            array
                .as_string_view_opt()
                .ok_or_else(|| snapshot_layout_error(array.data_type()))?
                .data_buffers()
                .len(),
            1,
        )?,
        DataType::BinaryView => checked_copy_sum(
            array
                .as_binary_view_opt()
                .ok_or_else(|| snapshot_layout_error(array.data_type()))?
                .data_buffers()
                .len(),
            1,
        )?,
        DataType::List(_) => {
            children = snapshot_descriptor_budget(
                array
                    .as_list_opt::<i32>()
                    .ok_or_else(|| snapshot_layout_error(array.data_type()))?
                    .values()
                    .as_ref(),
                depth + 1,
            )?;
            1
        }
        DataType::LargeList(_) => {
            children = snapshot_descriptor_budget(
                array
                    .as_list_opt::<i64>()
                    .ok_or_else(|| snapshot_layout_error(array.data_type()))?
                    .values()
                    .as_ref(),
                depth + 1,
            )?;
            1
        }
        DataType::FixedSizeList(_, _) => {
            children = snapshot_descriptor_budget(
                array
                    .as_fixed_size_list_opt()
                    .ok_or_else(|| snapshot_layout_error(array.data_type()))?
                    .values()
                    .as_ref(),
                depth + 1,
            )?;
            0
        }
        DataType::Map(_, _) => {
            children = snapshot_descriptor_budget(
                array
                    .as_map_opt()
                    .ok_or_else(|| snapshot_layout_error(array.data_type()))?
                    .entries(),
                depth + 1,
            )?;
            1
        }
        DataType::Struct(_) => {
            for column in array
                .as_struct_opt()
                .ok_or_else(|| snapshot_layout_error(array.data_type()))?
                .columns()
            {
                children.include(snapshot_descriptor_budget(column.as_ref(), depth + 1)?)?;
            }
            0
        }
        DataType::Dictionary(_, _) => {
            children = snapshot_descriptor_budget(
                array
                    .as_any_dictionary_opt()
                    .ok_or_else(|| snapshot_layout_error(array.data_type()))?
                    .values()
                    .as_ref(),
                depth + 1,
            )?;
            1
        }
        DataType::Null if array.as_any().is::<arrow::array::NullArray>() => 0,
        DataType::Boolean if array.as_boolean_opt().is_some() => 1,
        DataType::Utf8 if array.as_string_opt::<i32>().is_some() => 2,
        DataType::LargeUtf8 if array.as_string_opt::<i64>().is_some() => 2,
        DataType::Binary if array.as_binary_opt::<i32>().is_some() => 2,
        DataType::LargeBinary if array.as_binary_opt::<i64>().is_some() => 2,
        DataType::FixedSizeBinary(_) if array.as_fixed_size_binary_opt().is_some() => 1,
        _ if standard_primitive_array(array) => 1,
        data_type => return Err(snapshot_layout_error(data_type)),
    };
    let local = checked_copy_sum(
        std::mem::size_of::<ArrayData>()
            + std::mem::size_of::<ArrayDataBuilder>()
            + std::mem::size_of::<DataType>()
            + std::mem::size_of::<ArrayRef>(),
        checked_copy_product(buffers, std::mem::size_of::<Buffer>())?,
    )?;
    let datatype_bytes = datatype_clone_bytes(array.data_type(), depth)?;
    Ok(SnapshotDescriptorBudget {
        bytes: checked_copy_sum(
            checked_copy_product(checked_copy_sum(local, datatype_bytes)?, 2)?,
            children.bytes,
        )?,
        datatype_bytes: checked_copy_sum(datatype_bytes, children.datatype_bytes)?,
    })
}

fn standard_primitive_array(array: &dyn Array) -> bool {
    macro_rules! supported {
        ($t:ty, $array:ident) => {
            $array.as_primitive_opt::<$t>().is_some()
        };
    }
    arrow::array::downcast_primitive!(array.data_type() => (supported, array), _ => false)
}

fn snapshot_layout_error(data_type: &DataType) -> DataFusionError {
    DataFusionError::NotImplemented(format!(
        "Cache snapshot cannot pre-admit this array layout: {data_type}"
    ))
}

#[derive(Default)]
struct SnapshotCopyBudget {
    copy: usize,
    owned_input: usize,
    metadata: usize,
}

impl SnapshotCopyBudget {
    fn visit(&mut self, data: &ArrayData, depth: usize) -> Result<()> {
        if depth >= 64
            || !(data.data_type().is_primitive()
                || matches!(
                    data.data_type(),
                    DataType::Null
                        | DataType::Boolean
                        | DataType::Utf8
                        | DataType::LargeUtf8
                        | DataType::Binary
                        | DataType::LargeBinary
                        | DataType::FixedSizeBinary(_)
                        | DataType::Utf8View
                        | DataType::BinaryView
                        | DataType::List(_)
                        | DataType::LargeList(_)
                        | DataType::FixedSizeList(_, _)
                        | DataType::Struct(_)
                        | DataType::Map(_, _)
                        | DataType::Dictionary(_, _)
                ))
        {
            return Err(DataFusionError::NotImplemented(format!(
                "Cache snapshot cannot own this array layout: {} at depth {depth}",
                data.data_type()
            )));
        }
        let descriptors = checked_copy_sum(
            checked_copy_product(data.buffers().len(), std::mem::size_of::<Buffer>())?,
            checked_copy_product(data.child_data().len(), std::mem::size_of::<ArrayData>())?,
        )?;
        self.metadata = checked_copy_sum(
            self.metadata,
            checked_copy_sum(
                checked_copy_sum(descriptors, datatype_clone_bytes(data.data_type(), depth)?)?,
                std::mem::size_of::<ArrayData>()
                    + std::mem::size_of::<ArrayDataBuilder>()
                    + std::mem::size_of::<ArrayRef>(),
            )?,
        )?;
        for buffer in data
            .buffers()
            .iter()
            .chain(data.nulls().map(NullBuffer::buffer))
        {
            // Arrow's pre-sized MutableBuffer rounds once to 64-byte capacity.
            // It does not grow while copying exactly this many bytes.
            let aligned = buffer
                .len()
                .checked_next_multiple_of(64)
                .filter(|bytes| isize::try_from(*bytes).is_ok())
                .ok_or_else(copy_budget_overflow)?;
            self.copy = checked_copy_sum(self.copy, aligned)?;
            if !buffer.has_custom_allocation() {
                self.owned_input = checked_copy_sum(self.owned_input, buffer.capacity())?;
            }
        }
        for child in data.child_data() {
            self.visit(child, depth + 1)?;
        }
        Ok(())
    }
}

fn copy_snapshot_data(data: ArrayData) -> Result<ArrayData> {
    let (data_type, len, nulls, offset, buffers, children) = data.into_parts();
    let buffers = buffers
        .into_iter()
        .map(|buffer| Buffer::from_slice_ref(buffer.as_slice()))
        .collect();
    let nulls = nulls.map(|nulls| {
        NullBuffer::new(BooleanBuffer::new(
            Buffer::from_slice_ref(nulls.buffer().as_slice()),
            nulls.offset(),
            nulls.len(),
        ))
    });
    let children = children
        .into_iter()
        .map(copy_snapshot_data)
        .collect::<Result<Vec<_>>>()?;
    Ok(ArrayData::builder(data_type)
        .len(len)
        .offset(offset)
        .buffers(buffers)
        .nulls(nulls)
        .child_data(children)
        .build()?)
}

fn checked_copy_sum(left: usize, right: usize) -> Result<usize> {
    left.checked_add(right).ok_or_else(copy_budget_overflow)
}

fn checked_copy_product(left: usize, right: usize) -> Result<usize> {
    left.checked_mul(right).ok_or_else(copy_budget_overflow)
}

fn copy_budget_overflow() -> DataFusionError {
    DataFusionError::ResourcesExhausted("Cache snapshot copy allocation size overflow".into())
}

pub(super) fn reserve_batch(
    reservation: &MemoryReservation,
    batch: &arrow::array::RecordBatch,
) -> Result<()> {
    if arrow_tools::record_batch::rests_on_unowned_memory(batch) {
        return Err(DataFusionError::ResourcesExhausted(
            "Cache input retains externally owned buffers that cannot be bounded by its memory pool".into(),
        ));
    }
    reservation.try_grow(batch.get_array_memory_size())
}

fn admission_timeout() -> DataFusionError {
    DataFusionError::ResourcesExhausted(
        "Cache write admission timed out; the fetched response remains available to the query"
            .into(),
    )
}

#[cfg(test)]
mod tests {
    use super::super::{ClaimOutcome, FetchState};
    use super::*;
    use datafusion::execution::memory_pool::UnboundedMemoryPool;

    fn claim(map: &InFlightRevalidations, key: &str) -> CacheKeyClaim {
        match CacheKeyClaim::acquire(map, key.into(), None) {
            ClaimOutcome::Leader(claim) => claim,
            ClaimOutcome::Follower(_) => panic!("scope must not already be claimed"),
        }
    }

    fn publication(freshness: &Arc<CacheFreshness>, claim: CacheKeyClaim) -> CachePublication {
        let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
        CachePublication {
            freshness: Arc::clone(freshness),
            _claim: claim,
            _reservation: MemoryConsumer::new("cache publication test").register(&pool),
        }
    }

    fn foreign_buffer(buffer: Buffer, owners: &mut Vec<Weak<Buffer>>) -> Buffer {
        let owner = Arc::new(buffer);
        owners.push(Arc::downgrade(&owner));
        let ptr = std::ptr::NonNull::new(owner.as_ptr().cast_mut()).expect("buffer pointer");
        // The owner retains this exact buffer and therefore its aligned bytes
        // until the custom allocation's final reference is dropped.
        unsafe { Buffer::from_custom_allocation(ptr, owner.len(), owner) }
    }

    fn foreign_data(data: ArrayData, owners: &mut Vec<Weak<Buffer>>) -> ArrayData {
        let (data_type, len, nulls, offset, buffers, children) = data.into_parts();
        let buffers = buffers
            .into_iter()
            .map(|buffer| foreign_buffer(buffer, owners))
            .collect();
        let nulls = nulls.map(|nulls| {
            NullBuffer::new(BooleanBuffer::new(
                foreign_buffer(nulls.buffer().clone(), owners),
                nulls.offset(),
                nulls.len(),
            ))
        });
        let children = children
            .into_iter()
            .map(|child| foreign_data(child, owners))
            .collect();
        ArrayData::builder(data_type)
            .len(len)
            .offset(offset)
            .buffers(buffers)
            .nulls(nulls)
            .child_data(children)
            .build()
            .expect("foreign array")
    }

    fn foreign_batch(batch: &RecordBatch) -> (RecordBatch, Vec<Weak<Buffer>>) {
        let mut owners = Vec::new();
        let columns = batch
            .columns()
            .iter()
            .map(|column| make_array(foreign_data(column.to_data(), &mut owners)))
            .collect();
        (
            RecordBatch::try_new(batch.schema(), columns).expect("foreign batch"),
            owners,
        )
    }

    #[test]
    fn snapshot_copy_preserves_sliced_nested_nullable_and_dictionary_data() {
        use arrow::array::{
            DictionaryArray, Int8Array, ListArray, MapBuilder, StringArray, StringBuilder,
            StringViewArray, StructArray,
        };
        use arrow::buffer::OffsetBuffer;
        use arrow::datatypes::{Field, Int8Type, Schema};
        use std::collections::HashMap;

        let text: ArrayRef = Arc::new(StringArray::from(vec![Some("雪\0"), None, Some("雪\0")]));
        let view: ArrayRef = Arc::new(StringViewArray::from(vec![
            Some("long duplicated view content"),
            None,
            Some("long duplicated view content"),
        ]));
        let nested: ArrayRef = Arc::new(StructArray::from(vec![(
            Arc::new(Field::new("value", DataType::Utf8View, true)),
            view,
        )]));
        let list: ArrayRef = Arc::new(ListArray::new(
            Arc::new(Field::new("item", DataType::Utf8, true)),
            OffsetBuffer::from_lengths([1, 0, 2]),
            Arc::clone(&text),
            None,
        ));
        let dictionary: ArrayRef = Arc::new(
            DictionaryArray::<Int8Type>::try_new(
                Int8Array::from(vec![Some(1), None, Some(1)]),
                Arc::new(StringArray::from(vec!["unused", "duplicate"])),
            )
            .expect("dictionary"),
        );
        let mut map = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
        for valid in [true, false, true] {
            if valid {
                map.keys().append_value("header");
                map.values().append_value("value");
            }
            map.append(valid).expect("map row");
        }
        let batch = RecordBatch::try_from_iter(vec![
            ("text", text),
            ("nested", nested),
            ("list", list),
            ("dictionary", dictionary),
            ("headers", Arc::new(map.finish()) as ArrayRef),
        ])
        .expect("nested snapshot");
        let schema = Arc::new(Schema::new_with_metadata(
            batch.schema().fields().clone(),
            HashMap::from([("snapshot".to_string(), "preserved".to_string())]),
        ));
        let batch = RecordBatch::try_new(schema, batch.columns().to_vec()).expect("metadata");
        for expected in [batch.clone(), batch.slice(1, 2)] {
            let (foreign, owners) = foreign_batch(&expected);
            assert!(arrow_tools::record_batch::rests_on_unowned_memory(&foreign));
            let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
            let reservation = MemoryConsumer::new("snapshot test").register(&pool);
            let copied = own_snapshot_batch(foreign, &reservation).expect("owned snapshot");
            assert_eq!(copied, expected);
            assert_eq!(copied.schema(), expected.schema());
            assert!(!arrow_tools::record_batch::rests_on_unowned_memory(&copied));
            assert!(owners.iter().all(|owner| owner.upgrade().is_none()));
            assert_eq!(
                reservation.size(),
                copied.get_array_memory_size() + 2 * std::mem::size_of::<DataType>()
            );
            drop(copied);
            drop(reservation);
            assert_eq!(pool.reserved(), 0);
        }
    }

    #[test]
    fn nested_dictionary_descriptor_and_retained_types_are_charged() {
        use arrow::array::{DictionaryArray, Int8Array, StringViewArray};
        use arrow::datatypes::Int8Type;
        use datafusion::execution::memory_pool::GreedyMemoryPool;
        let depth = 16;
        let mut array: ArrayRef = Arc::new(StringViewArray::from(vec![
            Some("long duplicate view content"),
            None,
            Some("long duplicate view content"),
        ]));
        for _ in 0..depth {
            array = Arc::new(
                DictionaryArray::<Int8Type>::try_new(
                    Int8Array::from(vec![Some(0), None, Some(2)]),
                    array,
                )
                .expect("nested dictionary"),
            );
        }
        let budget =
            snapshot_descriptor_budget(array.as_ref(), 0).expect("borrowed descriptor bound");
        let datatype_bytes = depth * (depth + 1) * std::mem::size_of::<DataType>();
        assert_eq!(budget.datatype_bytes, datatype_bytes);
        assert!(budget.bytes > 2 * datatype_bytes);
        let expected =
            RecordBatch::try_from_iter(vec![("dictionary", array)]).expect("dictionary batch");
        for limit in [0, budget.bytes - 1] {
            let (foreign, owners) = foreign_batch(&expected);
            let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(limit));
            let reservation = MemoryConsumer::new("dictionary descriptor refusal").register(&pool);
            assert!(matches!(
                own_snapshot_batch(foreign, &reservation),
                Err(DataFusionError::ResourcesExhausted(_))
            ));
            assert!(owners.iter().all(|owner| owner.upgrade().is_none()));
            assert_eq!(pool.reserved(), 0);
        }
        for foreign in [false, true] {
            let (input, owners) = if foreign {
                foreign_batch(&expected)
            } else {
                (expected.clone(), vec![])
            };
            let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
            let reservation = MemoryConsumer::new("dictionary snapshot").register(&pool);
            let copied = own_snapshot_batch(input, &reservation).expect("owned nested dictionary");
            assert_eq!(copied, expected);
            assert!(!arrow_tools::record_batch::rests_on_unowned_memory(&copied));
            assert!(owners.iter().all(|owner| owner.upgrade().is_none()));
            assert_eq!(
                reservation.size(),
                copied.get_array_memory_size() + datatype_bytes
            );
            println!(
                "nested dictionary: depth={depth} foreign={foreign} descriptor_bound={} datatype_bytes={datatype_bytes} retained={}",
                budget.bytes,
                reservation.size()
            );
            drop(copied);
            drop(reservation);
            assert_eq!(pool.reserved(), 0);
        }
    }

    #[derive(Debug)]
    struct DescriptorWitnessArray {
        values: arrow::array::Int32Array,
        constructions: Arc<std::sync::atomic::AtomicUsize>,
    }

    // Every operation delegates to the valid primitive array. Its distinct
    // concrete type lets the snapshot boundary refuse custom representations.
    unsafe impl Array for DescriptorWitnessArray {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
        fn to_data(&self) -> ArrayData {
            self.constructions.fetch_add(1, Ordering::Relaxed);
            self.values.to_data()
        }
        fn into_data(self) -> ArrayData {
            self.to_data()
        }
        fn data_type(&self) -> &DataType {
            self.values.data_type()
        }
        fn slice(&self, offset: usize, length: usize) -> ArrayRef {
            Arc::new(Self {
                values: self.values.slice(offset, length),
                constructions: Arc::clone(&self.constructions),
            })
        }
        fn len(&self) -> usize {
            self.values.len()
        }
        fn is_empty(&self) -> bool {
            self.values.is_empty()
        }
        fn offset(&self) -> usize {
            self.values.offset()
        }
        fn nulls(&self) -> Option<&NullBuffer> {
            self.values.nulls()
        }
        fn get_buffer_memory_size(&self) -> usize {
            self.values.get_buffer_memory_size()
        }
        fn get_array_memory_size(&self) -> usize {
            self.values.get_array_memory_size()
        }
    }

    #[test]
    fn custom_primitive_is_refused_without_constructing_descriptors() {
        use datafusion::execution::memory_pool::GreedyMemoryPool;
        for limit in [0, 1 << 20] {
            let constructions = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let array: ArrayRef = Arc::new(DescriptorWitnessArray {
                values: arrow::array::Int32Array::from(vec![1, 2, 3]),
                constructions: Arc::clone(&constructions),
            });
            let batch =
                RecordBatch::try_from_iter(vec![("custom", array)]).expect("custom array batch");
            let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(limit));
            let reservation = MemoryConsumer::new("custom array test").register(&pool);
            assert!(matches!(
                own_snapshot_batch(batch, &reservation),
                Err(DataFusionError::NotImplemented(_))
            ));
            assert_eq!(constructions.load(Ordering::Relaxed), 0);
            assert_eq!(pool.reserved(), 0);
            println!(
                "custom representation: pool_limit={limit} descriptor_constructions=0 retained=0"
            );
        }
    }

    #[test]
    fn snapshot_descriptor_admission_counts_unused_view_buffers() {
        use arrow::array::StringViewArray;
        use datafusion::execution::memory_pool::GreedyMemoryPool;
        let seed = StringViewArray::from(vec!["a view value longer than twelve bytes"]);
        let narrow = snapshot_descriptor_budget(&seed, 0)
            .expect("narrow descriptor bound")
            .bytes;
        let wide = StringViewArray::new(
            seed.views().clone(),
            vec![seed.data_buffers()[0].clone(); 128],
            None,
        );
        let bound = snapshot_descriptor_budget(&wide, 0)
            .expect("wide descriptor bound")
            .bytes;
        assert_eq!(bound - narrow, 127 * std::mem::size_of::<Buffer>() * 2);
        let batch = RecordBatch::try_from_iter(vec![("v", Arc::new(wide) as ArrayRef)])
            .expect("many view buffers");
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(bound - 1));
        let reservation = MemoryConsumer::new("descriptor admission").register(&pool);
        own_snapshot_batch(batch, &reservation).expect_err("descriptor budget refuses the batch");
        assert_eq!(pool.reserved(), 0);
        println!(
            "descriptor admission: buffers=129 bound={bound} limit={} retained=0",
            bound - 1
        );
    }

    #[test]
    fn snapshot_copy_refuses_pool_before_retaining_foreign_buffers() {
        use arrow::array::StringArray;
        use datafusion::execution::memory_pool::GreedyMemoryPool;
        let expected = RecordBatch::try_from_iter(vec![(
            "v",
            Arc::new(StringArray::from(vec!["content"])) as ArrayRef,
        )])
        .expect("snapshot");
        let (foreign, owners) = foreign_batch(&expected);
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(0));
        let reservation = MemoryConsumer::new("snapshot test").register(&pool);
        own_snapshot_batch(foreign, &reservation)
            .expect_err("reservation refuses the foreign copy");
        assert!(owners.iter().all(|owner| owner.upgrade().is_none()));
        assert_eq!(pool.reserved(), 0);
        checked_copy_sum(usize::MAX, 1).expect_err("sum overflows");
        checked_copy_product(usize::MAX, 2).expect_err("product overflows");
    }

    #[test]
    fn sharing_metadata_is_bounded_by_the_original_pool() {
        use datafusion::execution::memory_pool::GreedyMemoryPool;
        let bytes = 4096;
        let metadata = std::mem::size_of::<parking_lot::Mutex<PeerRegistry>>()
            + 2 * std::mem::size_of::<usize>()
            + PEER_NODE_BYTES
            + std::mem::size_of::<PeerCharge>();
        let exact: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(bytes));
        let reservation = MemoryConsumer::new("no metadata room").register(&exact);
        reservation.try_grow(bytes).expect("buffers fit");
        RetainedBufferCharge::new(&exact, reservation).expect_err("metadata does not fit the pool");
        assert_eq!(exact.reserved(), 0);

        let original_pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(bytes + metadata));
        let second_pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(bytes));
        let reservation = MemoryConsumer::new("one peer only").register(&original_pool);
        reservation.try_grow(bytes).expect("buffer admission");
        let original =
            RetainedBufferCharge::new(&original_pool, reservation).expect("root metadata fits");
        original
            .retain_for_pool(&second_pool)
            .expect_err("a peer past the metadata budget is refused");
        assert_eq!(
            second_pool.reserved(),
            0,
            "rollback declined secondary admission"
        );
        assert_eq!(original.peers.lock().len(), 1);
        assert_eq!(original.metadata_bytes(), metadata);
        assert_eq!(original_pool.reserved(), bytes + metadata);
        drop(original);
        assert_eq!(original_pool.reserved(), 0);
        println!(
            "sharing metadata: buffer_bytes={bytes} root_metadata={metadata} declined_secondary_retained=0 final_original=0"
        );
    }

    #[test]
    fn concurrent_secondary_admissions_share_and_remove_dead_peers() {
        use datafusion::execution::memory_pool::GreedyMemoryPool;
        let bytes = 4096;
        let original_pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(bytes + 16384));
        let second_pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(bytes));
        let reservation = MemoryConsumer::new("concurrent source").register(&original_pool);
        reservation.try_grow(bytes).expect("source capacity");
        let original = RetainedBufferCharge::new(&original_pool, reservation).expect("root charge");
        let barrier = std::sync::Barrier::new(8);
        let secondary = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..8)
                .map(|_| {
                    let barrier = &barrier;
                    let original = &original;
                    let second_pool = &second_pool;
                    scope.spawn(move || {
                        barrier.wait();
                        let admitted = original.retain_for_pool(second_pool);
                        barrier.wait();
                        admitted
                    })
                })
                .collect();
            handles
                .into_iter()
                .map(|handle| {
                    handle
                        .join()
                        .expect("admission thread")
                        .expect("one shared secondary charge")
                })
                .collect::<Vec<_>>()
        });
        assert!(
            secondary
                .iter()
                .all(|charge| Arc::ptr_eq(charge, &secondary[0]))
        );
        assert_eq!(second_pool.reserved(), bytes);
        assert_eq!(original.peers.lock().len(), 2);
        println!(
            "concurrent retention: consumers=8 bytes={bytes} original={} secondary={} peer_entries=2",
            original_pool.reserved(),
            second_pool.reserved()
        );
        let weak = Arc::downgrade(&second_pool);
        drop(second_pool);
        drop(secondary);
        assert!(
            weak.upgrade().is_none(),
            "no dead peer keeps its pool alive"
        );
        assert_eq!(
            original.peers.lock().len(),
            1,
            "remove without another admission"
        );
        assert_eq!(original_pool.reserved(), bytes + original.metadata_bytes());
        drop(original);
        assert_eq!(original_pool.reserved(), 0);
        println!(
            "concurrent retention released: peer_entries=1 before root drop, secondary_pool_dropped=true original=0"
        );
    }

    #[test]
    fn concurrent_release_with_unrelated_peer_admission() {
        use datafusion::execution::memory_pool::GreedyMemoryPool;
        let bytes = 4096;
        let original_pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(bytes + 16384));
        let second_pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(bytes));
        let third_pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(bytes));
        let reservation = MemoryConsumer::new("concurrent release").register(&original_pool);
        reservation.try_grow(bytes).expect("source capacity");
        let original = RetainedBufferCharge::new(&original_pool, reservation).expect("root charge");
        let secondary = original.retain_for_pool(&second_pool).expect("secondary");
        let third = original.retain_for_pool(&third_pool).expect("third");
        let releases: Vec<_> = (0..8).map(|_| Arc::clone(&secondary)).collect();
        drop(secondary);
        let barrier = std::sync::Barrier::new(9);
        std::thread::scope(|scope| {
            for charge in releases {
                let barrier = &barrier;
                scope.spawn(move || {
                    barrier.wait();
                    drop(charge);
                });
            }
            scope.spawn(|| {
                barrier.wait();
                for _ in 0..100 {
                    drop(
                        original
                            .retain_for_pool(&third_pool)
                            .expect("reuse live third admission"),
                    );
                }
            });
        });
        assert_eq!(second_pool.reserved(), 0);
        assert_eq!(third_pool.reserved(), bytes);
        assert_eq!(original.peers.lock().len(), 2);
        drop(third);
        assert_eq!(third_pool.reserved(), 0);
        assert_eq!(original.peers.lock().len(), 1);
        drop(original);
        assert_eq!(original_pool.reserved(), 0);
        println!("concurrent release: readers=8 other_admissions=100 all_reservations=0");
    }

    #[test]
    fn shared_charge_requires_full_capacity_in_each_distinct_pool() {
        use arrow::array::{RecordBatch, StringArray};
        use datafusion::execution::memory_pool::GreedyMemoryPool;

        let batch = RecordBatch::try_from_iter(vec![(
            "content",
            Arc::new(StringArray::from(vec!["x".repeat(4096), "y".repeat(4096)]))
                as arrow::array::ArrayRef,
        )])
        .expect("large backing buffer")
        .slice(0, 1);
        let bytes = batch.get_array_memory_size();
        assert!(bytes > 8192, "slice retains full backing allocation");
        let original_pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(bytes + 16384));
        let second_pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(bytes));
        let small_pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(bytes - 1));
        let original = RetainedBufferCharge::for_batches(&original_pool, &[batch])
            .expect("original admission");
        let same = original.retain_for_pool(&original_pool).expect("same pool");
        assert!(Arc::ptr_eq(&same, &original));
        original
            .retain_for_pool(&small_pool)
            .expect_err("small pool cannot hold the retained buffers");
        assert_eq!(small_pool.reserved(), 0);
        let second = original
            .retain_for_pool(&second_pool)
            .expect("second admission");
        let another = original
            .retain_for_pool(&second_pool)
            .expect("shared second admission");
        assert!(Arc::ptr_eq(&second, &another));
        assert_eq!(second_pool.reserved(), bytes);
        drop(another);
        drop(same);
        drop(original);
        assert_eq!(
            original_pool.reserved(),
            bytes + second.metadata_bytes(),
            "secondary retention holds original and charged metadata"
        );
        drop(second);
        assert_eq!(original_pool.reserved(), 0);
        assert_eq!(second_pool.reserved(), 0);
    }

    #[test]
    fn publication_between_scan_and_claim_invalidates_absence() {
        let freshness = Arc::new(CacheFreshness::default());
        let map = InFlightRevalidations::default();
        let earlier = claim(&map, "request");
        let observation = freshness.observe();
        drop(publication(&freshness, earlier));
        let mut later = claim(&map, "request");
        freshness.confirm_empty_scan(observation, &mut later);
        assert!(!later.proven_fresh);
    }

    #[test]
    fn held_claim_preserves_fresh_proof_across_other_publications() {
        let freshness = Arc::new(CacheFreshness::default());
        let map = InFlightRevalidations::default();
        let observation = freshness.observe();
        let mut first = claim(&map, "first");
        freshness.confirm_empty_scan(observation, &mut first);
        assert!(first.proven_fresh);
        drop(publication(&freshness, claim(&map, "other")));
        assert!(first.proven_fresh);
        let mut later = claim(&map, "later");
        freshness.confirm_empty_scan(observation, &mut later);
        assert!(!later.proven_fresh);
    }

    #[test]
    fn missing_observation_and_different_claim_map_do_not_prove_absence() {
        let freshness = CacheFreshness::default();
        let map = InFlightRevalidations::default();
        let mut first = claim(&map, "first");
        freshness.confirm_empty_scan(None, &mut first);
        assert!(!first.proven_fresh);
        let other_map = InFlightRevalidations::default();
        let mut second = claim(&other_map, "second");
        freshness.confirm_empty_scan(freshness.observe(), &mut second);
        assert!(!second.proven_fresh);
        assert!(freshness.bind_claim(&second).is_err());
    }

    #[test]
    fn saturated_revision_never_wraps_into_a_fresh_observation() {
        let freshness = Arc::new(CacheFreshness::default());
        freshness.revision.store(u64::MAX, Ordering::Release);
        let map = InFlightRevalidations::default();
        drop(publication(&freshness, claim(&map, "request")));
        assert_eq!(freshness.observe(), None);
    }

    #[test]
    fn native_empty_result_refetches_while_claim_is_held() {
        let map = InFlightRevalidations::default();
        let mut leader = claim(&map, "request");
        leader.publish_if_cacheable(&[], None, true);
        let ClaimOutcome::Follower(follower) = CacheKeyClaim::acquire(&map, "request".into(), None)
        else {
            panic!("the pending clear must retain scope ownership");
        };
        assert!(matches!(*follower.state.borrow(), FetchState::Failed));
        drop(leader);
        assert!(map.lock().is_empty());
    }

    #[test]
    fn pagination_configuration_requires_execution_proof() {
        use data_components::http::provider::{HttpTableProvider, PaginationConfig};
        for paginated in [false, true] {
            #[expect(
                clippy::default_trait_access,
                reason = "this crate has no direct reqwest dependency"
            )]
            let provider = HttpTableProvider::new(
                "http://localhost/items".parse().expect("URL"),
                Default::default(),
                "json".into(),
                true,
            );
            let provider = if paginated {
                provider
                    .with_pagination(PaginationConfig::default())
                    .expect("pagination")
            } else {
                provider
            };
            let plan = HttpExec::new(
                provider.schema(),
                Arc::new(provider),
                vec![(Some("/items".into()), None, None, None)],
                None,
            );
            let (_, completion) = prepare_source_fetch(
                Arc::new(plan),
                &[col("request_path").eq(lit("/items"))],
                None,
            )
            .expect("tracked plan");
            let completion = completion.expect("track both configured and plain requests");
            assert!(!completion.is_complete_single_request());
        }
    }

    #[test]
    fn cache_claim_keys_preserve_supplied_predicates() {
        let path = col("request_path").eq(lit("/items"));
        let omitted = super::super::compute_cache_key_from_filters(std::slice::from_ref(&path));
        let explicit =
            super::super::compute_cache_key_from_filters(&[path, col("request_body").eq(lit(""))]);
        assert_ne!(
            omitted, explicit,
            "claim keys must not infer request dimensions"
        );
    }

    #[test]
    fn request_scope_does_not_infer_omitted_dimensions() {
        let path = col("request_path").eq(lit("/items"));
        assert_eq!(
            canonical_request_filters(std::slice::from_ref(&path)),
            Some(vec![path.clone()])
        );
        let explicit = vec![
            path,
            col("request_query").eq(lit("")),
            col("request_body").eq(lit("")),
        ];
        assert_eq!(canonical_request_filters(&explicit), Some(explicit));
        assert_eq!(canonical_request_filters(&[]), Some(vec![]));
        assert!(
            canonical_request_filters(&[
                col("request_path").eq(lit("/items")),
                col("content").eq(lit("subset"))
            ])
            .is_none()
        );
    }

    #[test]
    fn response_predicates_fill_only_the_request_they_leave_intact() {
        let path = col("request_path").eq(lit("/items"));
        let query = col("request_query").eq(lit("q=a"));
        let rank = col("rank").eq(lit("1"));
        assert_eq!(
            response_filtered_request(&[path.clone(), rank.clone(), query.clone()]),
            Some(vec![path.clone(), query.clone()])
        );
        assert_eq!(
            response_filtered_request(&[path.clone().and(rank.clone())]),
            Some(vec![path.clone()])
        );
        for unscoped in [
            vec![path.clone(), query],
            vec![rank.clone()],
            vec![path.clone().or(rank.clone()), rank.clone()],
            vec![
                path.clone(),
                col("rank").eq(col("request_query")),
                rank.clone(),
            ],
            vec![path, rank, col("request_headers").eq(lit("x-key: 1"))],
        ] {
            assert_eq!(response_filtered_request(&unscoped), None, "{unscoped:?}");
        }
    }
}
