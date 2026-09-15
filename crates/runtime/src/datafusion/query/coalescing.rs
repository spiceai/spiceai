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

//! Coalesce concurrent equality lookups, preserving a separate `LIMIT 1` per request.
//!
//! Only anonymous HTTP requests over accelerated tables are eligible. Recognition
//! accepts a deliberately narrow logical-plan shape; everything else uses ordinary
//! query admission. Collection holds no execution permits. A family has at most
//! one shared producer executing, and arrivals can join its next pending batch.

use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, SchemaRef};
use datafusion::common::{Column, ScalarValue};
use datafusion::datasource::source_as_provider;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::{SendableRecordBatchStream, SessionState, TaskContext};
use datafusion::logical_expr::{Expr, LogicalPlan, LogicalPlanBuilder, Operator, lit};
use datafusion::physical_plan::{ExecutionPlan, stream::RecordBatchStreamAdapter};
use futures::StreamExt;
use runtime_request_context::{Protocol, RequestContext};
use tokio::sync::{Notify, Semaphore, oneshot};
use tokio::time::Instant;

type Reply = Result<(RecordBatch, Arc<dyn ExecutionPlan>)>;
type Replies = HashMap<Vec<ScalarValue>, Vec<oneshot::Sender<Reply>>>;
pub(super) struct Ticket {
    receiver: oneshot::Receiver<Reply>,
    alive: Arc<AtomicBool>,
}

impl Future for Ticket {
    type Output = std::result::Result<Reply, oneshot::error::RecvError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.receiver).poll(cx)
    }
}

impl Drop for Ticket {
    fn drop(&mut self) {
        self.alive.store(false, Ordering::Release);
    }
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct Family {
    provider: usize,
    table: String,
    session: String,
    namespace: runtime_request_context::CacheNamespace,
    columns: Vec<usize>,
    projection: Vec<usize>,
    schema: SchemaRef,
}

pub(super) struct Lookup {
    pub(super) supported_table: bool,
    family: Family,
    scan: LogicalPlan,
    columns: Vec<Column>,
    values: Vec<ScalarValue>,
}

struct Member {
    alive: Arc<AtomicBool>,
    values: Vec<ScalarValue>,
    reply: oneshot::Sender<Reply>,
}

struct Batch {
    lookup: Lookup,
    session: SessionState,
    opened_at: Instant,
    deadline: Option<Instant>,
    full: Notify,
    members: Mutex<Members>,
}

struct Members {
    requests: Vec<Member>,
    full: bool,
}

/// How an eligible lookup proceeds.
pub(super) enum Admission {
    /// The lookup joined a shared scan; the ticket delivers its row.
    Coalesced(Ticket),
    /// A permit was free and no shared scan of the family is running, so the
    /// lookup executes on its own.
    Individual(tokio::sync::OwnedSemaphorePermit),
}

pub(crate) struct Coalescer {
    max_batch: usize,
    window: Option<Duration>,
    // Lock order: families, then Batch::members. Neither is held across await.
    families: Mutex<HashMap<Family, FamilyState>>,
    shutdown: CancellationToken,
    finished: Notify,
}

struct FamilyState {
    pending: Option<Arc<Batch>>,
    gate: Arc<Semaphore>,
    producers: usize,
}

/// Removes completed, cancelled, or panicked producers from the registry.
struct ProducerGuard {
    coalescer: Arc<Coalescer>,
    batch: Arc<Batch>,
}

impl Drop for ProducerGuard {
    fn drop(&mut self) {
        let mut families = self.coalescer.families.lock();
        if let Some(state) = families.get_mut(&self.batch.lookup.family) {
            if state
                .pending
                .as_ref()
                .is_some_and(|p| Arc::ptr_eq(p, &self.batch))
            {
                state.pending = None;
            }
            state.producers -= 1;
            if state.producers == 0 {
                families.remove(&self.batch.lookup.family);
            }
        }
        self.coalescer.finished.notify_waiters();
    }
}

impl Coalescer {
    pub(crate) fn from_env() -> Arc<Self> {
        let max_batch = std::env::var("SPICE_QUERY_COALESCE_MAX_BATCH_SIZE")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(1)
            .clamp(1, 65_536);
        let mode = std::env::var("SPICE_QUERY_COALESCE_MODE").unwrap_or_else(|_| "queued".into());
        let window = if mode == "window" {
            let millis = std::env::var("SPICE_QUERY_COALESCE_WINDOW_MS")
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(1)
                .clamp(1, 1000);
            Some(Duration::from_millis(millis))
        } else {
            if mode != "queued" {
                tracing::warn!(%mode, "Unknown prototype coalescing mode; using queued");
            }
            None
        };
        if max_batch > 1 {
            tracing::info!(
                max_batch,
                ?window,
                "Prototype point-query coalescing enabled"
            );
        }
        Arc::new(Self {
            max_batch,
            window,
            families: Mutex::new(HashMap::new()),
            shutdown: CancellationToken::new(),
            finished: Notify::new(),
        })
    }

    fn metrics() -> &'static runtime_metrics::query_coalescing::Metrics {
        &runtime_metrics::query_coalescing::METRICS
    }

    pub(super) fn enabled(&self) -> bool {
        self.max_batch > 1
    }

    /// A queued lookup runs individually only when a permit is immediately free
    /// and the family has no shared producer. Window mode always collects.
    pub(super) fn admit(
        self: &Arc<Self>,
        lookup: Lookup,
        session: &SessionState,
        semaphore: Arc<Semaphore>,
    ) -> Admission {
        let (receive, producer) = {
            let mut families = self.families.lock();
            if self.shutdown.is_cancelled() {
                let (reply, receiver) = oneshot::channel();
                let _ = reply.send(Err(DataFusionError::Execution(
                    "Query execution is shutting down".into(),
                )));
                return Admission::Coalesced(Ticket {
                    receiver,
                    alive: Arc::new(AtomicBool::new(true)),
                });
            }
            if self.window.is_none()
                && !families.contains_key(&lookup.family)
                && let Ok(permit) = Arc::clone(&semaphore).try_acquire_owned()
            {
                return Admission::Individual(permit);
            }
            let (reply, receiver) = oneshot::channel();
            let alive = Arc::new(AtomicBool::new(true));
            let receive = Ticket {
                receiver,
                alive: Arc::clone(&alive),
            };
            let values = lookup.values.clone();
            let state = families
                .entry(lookup.family.clone())
                .or_insert_with(|| FamilyState {
                    pending: None,
                    gate: Arc::new(Semaphore::new(1)),
                    producers: 0,
                });
            let (batch, start) = if let Some(batch) = &state.pending {
                (Arc::clone(batch), false)
            } else {
                // Only the batch opener clones a session. The producer must not
                // inherit that request's cancellation token.
                let mut session = session.clone();
                session.config_mut().set_extension(Arc::new(
                    RequestContext::builder(Protocol::Internal)
                        .with_cache_namespace(lookup.family.namespace.clone())
                        .build(),
                ));
                let now = Instant::now();
                let batch = Arc::new(Batch {
                    lookup,
                    session,
                    opened_at: now,
                    deadline: self.window.map(|window| now + window),
                    full: Notify::new(),
                    members: Mutex::new(Members {
                        requests: Vec::new(),
                        full: false,
                    }),
                });
                state.pending = Some(Arc::clone(&batch));
                state.producers += 1;
                (batch, true)
            };
            let full = {
                let mut members = batch.members.lock();
                members.requests.push(Member {
                    alive,
                    values,
                    reply,
                });
                members.full = members.requests.len() >= self.max_batch;
                members.full
            };
            if full {
                state.pending = None;
                batch.full.notify_one();
            }
            (
                receive,
                start.then(|| {
                    (
                        ProducerGuard {
                            coalescer: Arc::clone(self),
                            batch,
                        },
                        Arc::clone(&state.gate),
                    )
                }),
            )
        };
        Self::metrics().requests.add(1, &[]);
        if let Some((guard, gate)) = producer {
            tokio::spawn(async move {
                guard
                    .coalescer
                    .run_batch(&guard.batch, gate, semaphore)
                    .await;
            });
        }
        Admission::Coalesced(receive)
    }

    fn seal(&self, batch: &Arc<Batch>) -> (Vec<Member>, &'static str) {
        let mut families = self.families.lock();
        if let Some(state) = families.get_mut(&batch.lookup.family)
            && state
                .pending
                .as_ref()
                .is_some_and(|p| Arc::ptr_eq(p, batch))
        {
            state.pending = None;
        }
        let mut members = batch.members.lock();
        let reason = if members.full { "size" } else { "admission" };
        (std::mem::take(&mut members.requests), reason)
    }

    /// Detach atomically with the last-client check so a new arrival cannot
    /// join a batch whose producer has already decided to stop.
    async fn abandoned(&self, batch: &Arc<Batch>) {
        let mut tick = tokio::time::interval(Duration::from_millis(10));
        loop {
            tick.tick().await;
            let mut families = self.families.lock();
            if batch
                .members
                .lock()
                .requests
                .iter()
                .all(|member| member.reply.is_closed())
            {
                if let Some(state) = families.get_mut(&batch.lookup.family)
                    && state
                        .pending
                        .as_ref()
                        .is_some_and(|p| Arc::ptr_eq(p, batch))
                {
                    state.pending = None;
                }
                return;
            }
        }
    }

    async fn run_batch(&self, batch: &Arc<Batch>, gate: Arc<Semaphore>, semaphore: Arc<Semaphore>) {
        let admission = async {
            if let Some(deadline) = batch.deadline {
                // The deadline is a minimum collection time. The batch stays
                // open until admission unless it reaches the size cap first.
                tokio::select! {
                    () = tokio::time::sleep_until(deadline) => {},
                    () = batch.full.notified() => {},
                }
            }
            let wait_started = Instant::now();
            let serial = gate.acquire_owned().await?;
            let permit = semaphore.acquire_owned().await?;
            Ok::<_, tokio::sync::AcquireError>((serial, permit, wait_started))
        };
        let admitted = tokio::select! {
            () = self.abandoned(batch) => return,
            () = self.shutdown.cancelled() => return,
            admitted = admission => admitted,
        };
        let (members, reason) = self.seal(batch);
        let mut replies = Replies::new();
        let mut clients = Vec::new();
        let mut size = 0;
        for member in members {
            if !member.reply.is_closed() {
                clients.push(member.alive);
                replies.entry(member.values).or_default().push(member.reply);
                size += 1;
            }
        }
        if replies.is_empty() {
            return;
        }
        let Ok((_serial, _permit, wait_started)) = admitted else {
            fail(&mut replies, "Query admission closed");
            return;
        };
        let metrics = Self::metrics();
        metrics.batches.add(1, &[]);
        metrics
            .flushes
            .add(1, &[opentelemetry::KeyValue::new("reason", reason)]);
        metrics.batch_size.record(size, &[]);
        metrics.unique_keys.add(replies.len() as u64, &[]);
        metrics
            .collection_ms
            .record(batch.opened_at.elapsed().as_secs_f64() * 1000.0, &[]);
        metrics
            .admission_ms
            .record(wait_started.elapsed().as_secs_f64() * 1000.0, &[]);
        let execution_started = Instant::now();
        let result = tokio::select! {
            result = Self::execute(batch, &mut replies) => result,
            () = clients_cancelled(&clients) => return,
            () = self.shutdown.cancelled() => Err(DataFusionError::Execution("Query execution is shutting down".into())),
        };
        if let Err(error) = result {
            fail(&mut replies, &error.to_string());
        }
        metrics
            .execution_ms
            .record(execution_started.elapsed().as_secs_f64() * 1000.0, &[]);
    }

    pub(crate) async fn shutdown(&self) {
        self.shutdown.cancel();
        loop {
            let finished = self.finished.notified();
            tokio::pin!(finished);
            finished.as_mut().enable();
            if self.families.lock().is_empty() {
                return;
            }
            finished.await;
        }
    }

    async fn execute(batch: &Batch, replies: &mut Replies) -> Result<()> {
        let lookup = &batch.lookup;
        let family = &lookup.family;
        // The shared scan reads only the key columns and the requested output
        // columns, each once even when a column is both.
        let mut scan_columns: Vec<usize> = family
            .columns
            .iter()
            .chain(family.projection.iter())
            .copied()
            .collect();
        scan_columns.sort_unstable();
        scan_columns.dedup();
        let position = |column: usize| scan_columns.binary_search(&column).ok();
        let key_positions = family
            .columns
            .iter()
            .map(|&column| position(column))
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| {
                DataFusionError::Internal("Key column missing from the shared scan".into())
            })?;
        let output_positions = family
            .projection
            .iter()
            .map(|&column| position(column))
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| {
                DataFusionError::Internal("Output column missing from the shared scan".into())
            })?;
        let mut predicates = Vec::with_capacity(lookup.columns.len());
        for (index, column) in lookup.columns.iter().enumerate() {
            let values: HashSet<_> = replies.keys().map(|key| key[index].clone()).collect();
            predicates.push(
                Expr::Column(column.clone()).in_list(values.into_iter().map(lit).collect(), false),
            );
        }
        let predicate = predicates
            .into_iter()
            .reduce(Expr::and)
            .ok_or_else(|| DataFusionError::Internal("Empty lookup predicate".into()))?;
        // Independent IN lists are only a prefilter. Exact tuple membership
        // below rejects cross-pairs before a row can satisfy any request.
        // There is no global LIMIT: each tuple has its own first-row winner.
        let qualified = lookup.scan.schema().columns();
        let projection = scan_columns
            .iter()
            .map(|&column| {
                qualified
                    .get(column)
                    .cloned()
                    .map(Expr::Column)
                    .ok_or_else(|| {
                        DataFusionError::Internal("Shared scan column out of range".into())
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        let plan = LogicalPlanBuilder::from(lookup.scan.clone())
            .filter(predicate)?
            .project(projection)?
            .build()?;
        let planning_started = Instant::now();
        let physical = batch.session.create_physical_plan(&plan).await?;
        Self::metrics()
            .planning_ms
            .record(planning_started.elapsed().as_secs_f64() * 1000.0, &[]);
        let mut first_batch = true;
        let mut stream = super::execute_stream_preserving_output_order(
            Arc::clone(&physical),
            Arc::new(TaskContext::from(&batch.session)),
        )?;
        let mut cancellation_tick = tokio::time::interval(Duration::from_millis(10));
        while !replies.is_empty() {
            let next = tokio::select! {
                _ = cancellation_tick.tick() => {
                    replies.retain(|_, senders| {
                        senders.retain(|sender| !sender.is_closed());
                        !senders.is_empty()
                    });
                    continue;
                }
                next = stream.next() => next,
            };
            let Some(result) = next else { break };
            let rows = result?;
            if first_batch {
                first_batch = false;
                Self::metrics()
                    .first_batch_ms
                    .record(planning_started.elapsed().as_secs_f64() * 1000.0, &[]);
            }
            Self::metrics()
                .candidate_rows
                .add(rows.num_rows() as u64, &[]);
            for row in 0..rows.num_rows() {
                if row % 256 == 0 {
                    tokio::task::yield_now().await;
                }
                let key = key_positions
                    .iter()
                    .map(|&position| {
                        ScalarValue::try_from_array(rows.column(position), row).map(canonical)
                    })
                    .collect::<Result<Vec<_>>>()?;
                if let Some(senders) = replies.remove(&key) {
                    let projected = rows.slice(row, 1).project(&output_positions)?;
                    let answer = RecordBatch::try_new(
                        Arc::clone(&lookup.family.schema),
                        projected.columns().to_vec(),
                    )?;
                    for sender in senders {
                        let _ = sender.send(Ok((answer.clone(), Arc::clone(&physical))));
                    }
                }
                if replies.is_empty() {
                    break;
                }
            }
        }
        for (_, senders) in replies.drain() {
            for sender in senders {
                let _ = sender.send(Ok((
                    RecordBatch::new_empty(Arc::clone(&lookup.family.schema)),
                    Arc::clone(&physical),
                )));
            }
        }
        Ok(())
    }
}

async fn clients_cancelled(clients: &[Arc<AtomicBool>]) {
    let mut tick = tokio::time::interval(Duration::from_millis(10));
    loop {
        tick.tick().await;
        if clients.iter().all(|client| !client.load(Ordering::Acquire)) {
            return;
        }
    }
}

fn fail(replies: &mut Replies, message: &str) {
    for (_, senders) in replies.drain() {
        for sender in senders {
            let _ = sender.send(Err(DataFusionError::Execution(message.to_owned())));
        }
    }
}

fn canonical(value: ScalarValue) -> ScalarValue {
    match value {
        ScalarValue::Utf8View(value) | ScalarValue::LargeUtf8(value) => ScalarValue::Utf8(value),
        value => value,
    }
}

fn equality_terms(expr: &Expr, out: &mut Vec<(Column, ScalarValue)>) -> Option<()> {
    let Expr::BinaryExpr(binary) = expr else {
        return None;
    };
    if binary.op == Operator::And {
        equality_terms(&binary.left, out)?;
        equality_terms(&binary.right, out)
    } else if binary.op == Operator::Eq {
        let ((Expr::Column(column), Expr::Literal(value, _))
        | (Expr::Literal(value, _), Expr::Column(column))) =
            (binary.left.as_ref(), binary.right.as_ref())
        else {
            return None;
        };
        if value.is_null() {
            return None;
        }
        out.push((column.clone(), value.clone()));
        Some(())
    } else {
        None
    }
}

pub(super) fn outside_transaction(context: &RequestContext) -> bool {
    #[cfg(not(windows))]
    {
        context.extension::<cayenne::CayenneTransaction>().is_none()
    }
    #[cfg(windows)]
    {
        let _ = context;
        true
    }
}

pub(super) fn recognize(
    plan: &LogicalPlan,
    session: &SessionState,
    namespace: runtime_request_context::CacheNamespace,
) -> Option<Lookup> {
    let LogicalPlan::Limit(limit) = plan else {
        return None;
    };
    if !matches!(
        limit.fetch.as_deref(),
        Some(Expr::Literal(ScalarValue::Int64(Some(1)), _))
    ) {
        return None;
    }
    if !matches!(
        limit.skip.as_deref(),
        None | Some(Expr::Literal(ScalarValue::Int64(Some(0)), _))
    ) {
        return None;
    }
    let (input, projection) = match limit.input.as_ref() {
        LogicalPlan::Projection(projection) => (projection.input.as_ref(), Some(projection)),
        input => (input, None),
    };
    let LogicalPlan::Filter(filter) = input else {
        return None;
    };
    let LogicalPlan::TableScan(scan) = filter.input.as_ref() else {
        return None;
    };
    if scan.projection.is_some() || scan.fetch.is_some() || !scan.filters.is_empty() {
        return None;
    }
    let mut terms = Vec::new();
    equality_terms(&filter.predicate, &mut terms)?;
    if terms.is_empty() || terms.len() > 8 {
        return None;
    }
    let mut keys = Vec::new();
    for (column, value) in terms {
        let index = scan.projected_schema.index_of_column(&column).ok()?;
        let ty = scan.projected_schema.field(index).data_type();
        if !matches!(
            ty,
            DataType::Utf8
                | DataType::Utf8View
                | DataType::LargeUtf8
                | DataType::Boolean
                | DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
        ) {
            return None;
        }
        // Only exact integer conversions and string representation changes are
        // eligible. Casting floating, decimal, or string literals to an integer
        // can change the equality coercion chosen for the original query.
        let value_type = value.data_type();
        let string_type =
            |t: &DataType| matches!(t, DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8);
        if value_type != *ty
            && !(value_type.is_integer() && ty.is_integer())
            && !(string_type(&value_type) && string_type(ty))
        {
            return None;
        }
        let cast = value.cast_to(ty).ok()?;
        if canonical(cast.cast_to(&value.data_type()).ok()?) != canonical(value) {
            return None;
        }
        keys.push((index, column, canonical(cast)));
    }
    keys.sort_by_key(|(index, _, _)| *index);
    if keys.windows(2).any(|pair| pair[0].0 == pair[1].0) {
        return None;
    }
    let projection = match projection {
        Some(projection) => projection
            .expr
            .iter()
            .map(|expr| {
                let expr = match expr {
                    Expr::Alias(alias) => alias.expr.as_ref(),
                    expr => expr,
                };
                let Expr::Column(column) = expr else {
                    return None;
                };
                scan.projected_schema.index_of_column(column).ok()
            })
            .collect::<Option<Vec<_>>>()?,
        None => (0..scan.projected_schema.fields().len()).collect(),
    };
    let provider = source_as_provider(&scan.source).ok()?;
    let supported_table = spice_table::find_layer::<runtime_table::accelerated::AcceleratedTable>(
        provider.as_ref(),
        spice_table::LayerWalk::Read,
    )
    .is_some_and(runtime_table::AcceleratedTable::supports_shared_lookup);
    Some(Lookup {
        supported_table,
        family: Family {
            provider: Arc::as_ptr(&provider).cast::<()>() as usize,
            table: scan.table_name.to_string(),
            session: session.session_id().to_string(),
            namespace,
            columns: keys.iter().map(|(index, _, _)| *index).collect(),
            projection,
            schema: Arc::clone(plan.schema().inner()),
        },
        scan: LogicalPlan::TableScan(scan.clone()),
        columns: keys.iter().map(|(_, column, _)| column.clone()).collect(),
        values: keys.into_iter().map(|(_, _, value)| value).collect(),
    })
}

pub(super) fn response_stream(batch: RecordBatch) -> SendableRecordBatchStream {
    Box::pin(RecordBatchStreamAdapter::new(
        batch.schema(),
        futures::stream::once(async move { Ok(batch) }),
    ))
}

#[cfg(test)]
mod tests;
