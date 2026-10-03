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
use std::sync::{
    Arc, OnceLock,
    atomic::{AtomicI64, AtomicU64, Ordering},
};
use std::time::Duration;

use arrow::datatypes::SchemaRef;
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
    ExecutionPlan, coalesce_partitions::CoalescePartitionsExec, projection::ProjectionExec,
};
use futures::TryStreamExt;
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
    tasks: TaskTracker,
}

impl CacheWorkDrain {
    pub async fn wait(&self) {
        self.tasks.wait().await;
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

    pub(super) fn confirm_empty_scan(&self, claim: &mut CacheKeyClaim) {
        if let Self::Sink(writer) = self {
            writer
                .freshness
                .confirm_empty_scan(writer.scan_revision, claim);
        }
    }

    /// Fence preparation and fanout before closing the generation's sink.
    /// The generation owner must await this drain as well as sink completion,
    /// outside registry and accelerator locks. Sink flush does not await it.
    #[must_use]
    pub fn begin_drain(&self) -> Option<CacheWorkDrain> {
        let Self::Sink(writer) = self else {
            return None;
        };
        let mut closed = writer.work.closed.lock();
        *closed = true;
        writer.work.tasks.close();
        Some(CacheWorkDrain {
            tasks: writer.work.tasks.clone(),
        })
    }

    pub(super) fn spawn_owned(
        &self,
        runtime: &Handle,
        work: impl Future<Output = ()> + Send + 'static,
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
                drop(writer.work.tasks.spawn_on(work, runtime));
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
        Option<Arc<MemoryReservation>>,
    )> {
        let context = self.session_context();
        let task_context = self.task_context(&context.state());
        let charge = self.reserve_input(&[])?;
        let mut stream = datafusion::physical_plan::execute_stream(plan, task_context)?;
        let mut batches = Vec::new();
        while let Some(batch) = stream.try_next().await? {
            if let Some(charge) = &charge {
                reserve_batch(charge, &batch)?;
            }
            batches.push(batch);
        }
        Ok((batches, charge))
    }

    pub(super) fn reserve_input(
        &self,
        batches: &[arrow::array::RecordBatch],
    ) -> Result<Option<Arc<MemoryReservation>>> {
        let Self::Sink(writer) = self else {
            return Ok(None);
        };
        let reservation =
            MemoryConsumer::new("cache retained response").register(&writer.memory_pool);
        for batch in batches {
            reserve_batch(&reservation, batch)?;
        }
        Ok(Some(Arc::new(reservation)))
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
                    .map(|column| column.get_array_memory_size())
                    .fold(0, usize::saturating_add);
                reservation.try_grow(extra)?;
                Ok(cast)
            })
            .collect::<Result<Vec<_>>>()?;
        let canonical = canonical_request_filters(&request.filters);
        if request.replaces_existing
            && batches.iter().all(|batch| batch.num_rows() == 0)
            && (canonical.is_none()
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
        let claim = claim.ok_or_else(|| {
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

/// Cache-owned HTTP uses an empty path for the base URI and NULL query/body
/// for absent overrides. Empty query/body strings are explicit overrides and
/// remain distinct from NULL, including when the URI supplies query defaults.
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
        REQUEST_KEY_COLUMNS
            .iter()
            .enumerate()
            .map(|(index, name)| {
                match values.get(&index).copied().unwrap_or(if index == 0 {
                    Some("")
                } else {
                    None
                }) {
                    Some(value) => col(*name).eq(lit(value)),
                    None => col(*name).is_null(),
                }
            })
            .collect(),
    )
}

fn tracked_http_plan(
    plan: &Arc<dyn ExecutionPlan>,
) -> Result<Option<(Arc<dyn ExecutionPlan>, HttpFetchCompletion, bool)>> {
    if let Some(http) = plan.downcast_ref::<HttpExec>() {
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
                Arc::clone(plan).with_new_children(vec![child])?,
                completion,
                eligible,
            )));
        }
    }
    Ok(None)
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
    fn omitted_query_and_body_are_null_not_empty_overrides() {
        let path = col("request_path").eq(lit("/items"));
        assert_eq!(
            canonical_request_filters(std::slice::from_ref(&path)),
            Some(vec![
                path.clone(),
                col("request_query").is_null(),
                col("request_body").is_null(),
            ])
        );
        let explicit = vec![
            path,
            col("request_query").eq(lit("")),
            col("request_body").eq(lit("")),
        ];
        assert_eq!(canonical_request_filters(&explicit), Some(explicit));
    }
}
