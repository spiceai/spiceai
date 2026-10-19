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

use std::collections::VecDeque;
use std::ops::ControlFlow;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_tools::schema_evolution::WideningPlan;
use datafusion::common::TableReference;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SessionContext;
use tokio::runtime::Handle;
use tokio::sync::{mpsc, oneshot, watch};

use super::batching::{AppendBurst, ApplyingGuard, CdcBurst, CdcIngress, CoalescingBurst};
use super::source_policy::SchemaDecision;
use super::{
    BackendWrite, CdcRows, ChangeBatch, ChangeCapabilities, ChangeSinkBackend, ChangeWrite,
    DurabilityObserver, Recovery, RowsWrite, SchemaEvolutionSupport, ScopedWrite,
    StorageDurability, WriteOptions,
};

type SharedResult = std::result::Result<(), Arc<DataFusionError>>;
type Completion = Box<dyn FnOnce(Result<()>) + Send + 'static>;

/// Query-visible completion. Waiting observes work already owned by the sink.
#[derive(Clone)]
pub enum Publication {
    Ready,
    Pending(watch::Receiver<Option<SharedResult>>),
}

impl Publication {
    #[must_use]
    pub fn is_ready(&self) -> bool {
        match self {
            Self::Ready => true,
            Self::Pending(receiver) => receiver.borrow().is_some(),
        }
    }

    /// Wait for publication without taking ownership of the write.
    ///
    /// # Errors
    /// Returns the publication failure, or an error if the owner stops without a result.
    pub async fn wait(&self) -> Result<()> {
        let Self::Pending(receiver) = self else {
            return Ok(());
        };
        let mut receiver = receiver.clone();
        loop {
            if let Some(result) = receiver.borrow_and_update().clone() {
                return result.map_err(DataFusionError::Shared);
            }
            receiver.changed().await.map_err(|_| stopped())?;
        }
    }
}

/// Completion milestones for one accepted logical operation.
#[derive(Clone)]
pub struct WriteReceipt {
    pub changed: bool,
    pub publication: Publication,
    /// The storage guarantee after successful publication, not at admission.
    pub durability: StorageDurability,
    /// Last source input in this coalesced write. Singleton writes set this true.
    pub batch_end: bool,
}

impl WriteReceipt {
    /// Observe query-visible completion of this write.
    ///
    /// # Errors
    /// Returns the publication failure, or an error if the owner stops without a result.
    pub async fn published(&self) -> Result<()> {
        self.publication.wait().await
    }
}

/// A result observer, not the owner of accepted data or execution.
pub struct Submission {
    result: oneshot::Receiver<Result<WriteReceipt>>,
    outcome: Option<std::result::Result<WriteReceipt, Arc<DataFusionError>>>,
}

impl Submission {
    /// Cancellation-safe: losing a select race retains the receipt. Repeated
    /// waits return the same milestones; dropping this observer cancels no work.
    ///
    /// # Errors
    /// Returns the write failure, or an error if the owner stops without a receipt.
    pub async fn wait(&mut self) -> Result<WriteReceipt> {
        if self.outcome.is_none() {
            let result = (&mut self.result).await.unwrap_or_else(|_| Err(stopped()));
            self.outcome = Some(result.map_err(Arc::new));
        }
        self.outcome
            .clone()
            .ok_or_else(stopped)?
            .map_err(DataFusionError::Shared)
    }
}

struct Reply {
    result: Option<oneshot::Sender<Result<WriteReceipt>>>,
    completion: Option<Completion>,
}

impl Reply {
    fn refuse(self, error: &Arc<DataFusionError>) {
        if let Some(completion) = self.completion {
            completion(Err(DataFusionError::Shared(Arc::clone(error))));
        }
        if let Some(result) = self.result {
            let _ = result.send(Err(DataFusionError::Shared(Arc::clone(error))));
        }
    }
}

/// Exactly one charge per queued CDC input. Dequeue and queue destruction both
/// release it; carrying a dequeued command cannot discharge it a second time.
struct QueueCharge {
    ingress: Arc<CdcIngress>,
    bytes: usize,
    capacity: usize,
}

impl Drop for QueueCharge {
    fn drop(&mut self) {
        self.ingress.queue_leave(self.bytes, self.capacity);
    }
}

struct ApplyCommand {
    batch: ChangeBatch,
    options: WriteOptions,
    reply: Reply,
    admitted_at: Instant,
    charge: Option<QueueCharge>,
}

enum Command {
    /// Boxed: `ApplyCommand` carries a `ChangeBatch` / `LazyChangeBatch`, and
    /// the reader's deferred-row prebuild keeps a `ChangeBatchError` beside
    /// the source. An unboxed `Apply` trips `clippy::large_enum_variant`.
    Apply(Box<ApplyCommand>),
    Flush(oneshot::Sender<Result<()>>),
    Evolve {
        plan: WideningPlan,
        reply: oneshot::Sender<Result<()>>,
    },
    Close(watch::Sender<Option<SharedResult>>),
}

impl Command {
    fn received(&mut self) {
        if let Self::Apply(input) = self {
            drop(input.charge.take());
        }
    }
}

struct Inner {
    sender: mpsc::Sender<Command>,
    /// Serializes synchronous admission with close. Never held across await.
    closing: parking_lot::Mutex<Option<Publication>>,
    stop_linger: watch::Sender<bool>,
    backend: Arc<dyn ChangeSinkBackend>,
    runtime: Handle,
}

/// Reserved queue capacity. No input has been transferred yet. Dropping a
/// reservation releases capacity; submitting is synchronous and ordered with close.
pub struct ChangePermit {
    permit: mpsc::OwnedPermit<Command>,
    inner: Arc<Inner>,
    admission_wait: Duration,
}

impl ChangePermit {
    /// Transfer the input into the reserved queue capacity.
    ///
    /// # Errors
    /// Returns an error if admission closed after the capacity was reserved.
    pub fn submit(self, batch: ChangeBatch, options: WriteOptions) -> Result<Submission> {
        let (result, receiver) = oneshot::channel();
        self.send(
            batch,
            options,
            Reply {
                result: Some(result),
                completion: None,
            },
        )?;
        Ok(Submission {
            result: receiver,
            outcome: None,
        })
    }

    fn send(self, batch: ChangeBatch, options: WriteOptions, reply: Reply) -> Result<()> {
        let closing = self.inner.closing.lock();
        if closing.is_some() {
            return Err(stopped());
        }
        let charge = match &batch {
            ChangeBatch::Cdc(rows) => rows.ingress().map(|ingress| {
                let bytes = batch.estimated_bytes();
                let capacity = self.inner.sender.max_capacity();
                ingress.record_send_wait(self.admission_wait);
                ingress.queue_enter(bytes, capacity);
                QueueCharge {
                    ingress: Arc::clone(ingress),
                    bytes,
                    capacity,
                }
            }),
            ChangeBatch::Append { .. }
            | ChangeBatch::AppendScoped { .. }
            | ChangeBatch::Replace { .. } => None,
        };
        self.permit.send(Command::Apply(Box::new(ApplyCommand {
            batch,
            options,
            reply,
            charge,
            admitted_at: Instant::now(),
        })));
        Ok(())
    }
}

/// Cloneable handle to one table-generation owner. Cloning never starts a worker.
#[derive(Clone)]
pub struct ChangeSink {
    inner: Arc<Inner>,
}

impl ChangeSink {
    /// Bind one owner to an engine implementation and its apply runtime.
    #[must_use]
    pub fn new(
        backend: Arc<dyn ChangeSinkBackend>,
        context: SessionContext,
        runtime: &Handle,
        capacity: usize,
    ) -> Self {
        let (sender, receiver) = mpsc::channel(capacity.max(1));
        let (stop_linger, closing) = watch::channel(false);
        runtime.spawn(
            Owner {
                receiver,
                backend: Arc::clone(&backend),
                context,
                closing,
                carried: None,
                pending: None,
                failure: None,
                started_at: Instant::now(),
                previous_cycle: None,
                previous_append_cycle: None,
            }
            .run(),
        );
        Self {
            inner: Arc::new(Inner {
                sender,
                closing: parking_lot::Mutex::new(None),
                stop_linger,
                backend,
                runtime: runtime.clone(),
            }),
        }
    }

    #[must_use]
    pub fn capabilities(&self) -> ChangeCapabilities {
        self.inner.backend.capabilities()
    }

    pub fn set_durability_observer(&self, observer: Arc<dyn DurabilityObserver>) {
        self.inner.backend.set_durability_observer(observer);
    }

    /// Await capacity without taking ownership of input. A cancelled wait
    /// transfers no work. Close can still refuse the synchronous submit.
    ///
    /// # Errors
    /// Returns an error if admission is closed or the owner stops.
    pub async fn reserve(&self) -> Result<ChangePermit> {
        if self.inner.closing.lock().is_some() {
            return Err(stopped());
        }
        let start = Instant::now();
        let permit = self
            .inner
            .sender
            .clone()
            .reserve_owned()
            .await
            .map_err(|_| stopped())?;
        Ok(ChangePermit {
            permit,
            inner: Arc::clone(&self.inner),
            admission_wait: start.elapsed(),
        })
    }

    async fn admit(&self, command: Command) -> Result<()> {
        let permit = self.inner.sender.reserve().await.map_err(|_| stopped())?;
        let closing = self.inner.closing.lock();
        if closing.is_some() {
            return Err(stopped());
        }
        permit.send(command);
        Ok(())
    }

    /// Transfer ownership and observe application. Dropping this future after
    /// admission does not cancel execution or finalization.
    ///
    /// # Errors
    /// Returns an error if admission closes, the owner stops, or applying the input fails.
    pub async fn submit(&self, batch: ChangeBatch, options: WriteOptions) -> Result<WriteReceipt> {
        self.reserve().await?.submit(batch, options)?.wait().await
    }

    /// The owner invokes the callback after publication or failure, even if the
    /// submitting task ends. A pre-admission refusal drops the callback.
    ///
    /// # Errors
    /// Returns an error if admission is closed or the owner stops before admission.
    pub async fn enqueue(
        &self,
        batch: ChangeBatch,
        options: WriteOptions,
        completion: Completion,
    ) -> Result<()> {
        self.reserve().await?.send(
            batch,
            options,
            Reply {
                result: None,
                completion: Some(completion),
            },
        )
    }

    /// Establish durability only to the extent promised by the bound backend.
    ///
    /// # Errors
    /// Returns an error if admission closes, the owner stops, or pending writes or flushing fail.
    pub async fn flush(&self) -> Result<()> {
        let (reply, result) = oneshot::channel();
        self.admit(Command::Flush(reply)).await?;
        result.await.map_err(|_| stopped())?
    }

    /// Apply a schema evolution after preceding writes have settled.
    ///
    /// # Errors
    /// Returns an error if admission closes, the owner stops, or pending writes or evolution fail.
    pub async fn evolve_schema(&self, plan: &WideningPlan) -> Result<()> {
        let (reply, result) = oneshot::channel();
        self.admit(Command::Evolve {
            plan: plan.clone(),
            reply,
        })
        .await?;
        result.await.map_err(|_| stopped())?
    }

    /// Fence admission and start an owner-controlled drain. It continues even
    /// if every caller stops waiting. Repeated calls observe the same drain.
    #[must_use]
    pub fn begin_close(&self) -> Publication {
        let mut closing = self.inner.closing.lock();
        if let Some(publication) = &*closing {
            return publication.clone();
        }
        let (reply, result) = watch::channel(None);
        let publication = Publication::Pending(result);
        *closing = Some(publication.clone());
        self.inner.stop_linger.send_replace(true);
        let sender = self.inner.sender.clone();
        self.inner.runtime.spawn(async move {
            if let Err(error) = sender.send(Command::Close(reply)).await
                && let Command::Close(reply) = error.0
            {
                reply.send_replace(Some(Err(Arc::new(stopped()))));
            }
        });
        publication
    }

    /// A timeout does not abort accepted work or authorize another generation
    /// to publish to the same target. The caller must keep the target fenced.
    ///
    /// # Errors
    /// Returns an error if the drain times out, the owner stops, or accepted writes or flushing fail.
    pub async fn close(&self, timeout: Duration) -> Result<()> {
        let publication = self.begin_close();
        tokio::time::timeout(timeout, publication.wait())
            .await
            .map_err(|_| {
                DataFusionError::Execution(
                    "Change ingestion drain timed out; the table generation remains fenced".into(),
                )
            })?
    }
}

fn scoped(write: ScopedWrite) -> ChangeWrite {
    ChangeWrite::Rows(write.into())
}

fn stopped() -> DataFusionError {
    DataFusionError::Execution(
        "Change ingestion is closed; accepted work may require recovery".into(),
    )
}

fn start_finalization(write: BackendWrite, completions: Vec<Completion>) -> WriteReceipt {
    let publication = if let Some(finalizer) = write.finalizer {
        let (sender, receiver) = watch::channel(None);
        tokio::spawn(async move {
            let result = finalizer.await.map_err(Arc::new);
            for completion in completions {
                completion(result.clone().map_err(DataFusionError::Shared));
            }
            sender.send_replace(Some(result));
        });
        Publication::Pending(receiver)
    } else {
        for completion in completions {
            completion(Ok(()));
        }
        Publication::Ready
    };
    WriteReceipt {
        changed: write.changed,
        publication,
        durability: write.durability,
        batch_end: true,
    }
}

struct BurstTimer {
    ingress: Option<Arc<CdcIngress>>,
    start: Instant,
}

impl Drop for BurstTimer {
    fn drop(&mut self) {
        if let Some(ingress) = &self.ingress {
            ingress.record_duration(self.start);
        }
    }
}

struct Owner {
    receiver: mpsc::Receiver<Command>,
    backend: Arc<dyn ChangeSinkBackend>,
    context: SessionContext,
    closing: watch::Receiver<bool>,
    /// One dequeued boundary command, not an additional producer queue.
    carried: Option<Command>,
    pending: Option<Publication>,
    failure: Option<Arc<DataFusionError>>,
    started_at: Instant,
    previous_cycle: Option<Instant>,
    previous_append_cycle: Option<Instant>,
}

impl Owner {
    fn fail(&mut self, error: DataFusionError) -> Arc<DataFusionError> {
        Arc::clone(self.failure.get_or_insert_with(|| Arc::new(error)))
    }

    async fn settle(&mut self) -> SharedResult {
        if let Some(publication) = self.pending.take()
            && let Err(error) = publication.wait().await
        {
            return Err(self.fail(error));
        }
        match &self.failure {
            Some(error) => Err(Arc::clone(error)),
            None => Ok(()),
        }
    }

    async fn flush(&mut self) -> SharedResult {
        self.settle().await?;
        self.backend.flush().await.map_err(|error| self.fail(error))
    }

    async fn run(mut self) {
        loop {
            let command = match self.carried.take() {
                Some(command) => Some(command),
                None => self.receiver.recv().await,
            };
            let Some(mut command) = command else {
                break;
            };
            command.received();
            match command {
                Command::Apply(input) => {
                    let ApplyCommand {
                        batch,
                        options,
                        reply,
                        admitted_at,
                        ..
                    } = *input;
                    if let Some(error) = &self.failure {
                        reply.refuse(error);
                    } else if let ChangeBatch::Cdc(rows) = batch {
                        self.apply_cdc(rows, options, reply, admitted_at).await;
                    } else {
                        match AppendBurst::new(batch, options) {
                            ControlFlow::Continue(burst) => {
                                self.apply_append(burst, reply, admitted_at).await;
                            }
                            // A row vector without a lane is one operation. Its
                            // scope and chunk boundaries are not coalescing boundaries.
                            ControlFlow::Break(batch) => {
                                self.apply(batch.into(), options, vec![reply]).await;
                            }
                        }
                    }
                }
                Command::Flush(reply) => {
                    let _ = reply.send(self.flush().await.map_err(DataFusionError::Shared));
                }
                Command::Close(reply) => {
                    reply.send_replace(Some(self.flush().await));
                    break;
                }
                Command::Evolve { plan, reply } => {
                    let result = match self.settle().await {
                        Ok(()) => self
                            .backend
                            .evolve_schema(&plan)
                            .await
                            .map_err(|error| self.fail(error)),
                        Err(error) => Err(error),
                    };
                    let _ = reply.send(result.map_err(DataFusionError::Shared));
                }
            }
        }
        let _ = self.settle().await;
    }

    async fn drain(
        &mut self,
        burst: &mut impl CoalescingBurst,
        replies: &mut VecDeque<Reply>,
        first_received: Instant,
        previous_cycle: Option<Instant>,
    ) -> &'static str {
        let ingress = burst.cdc_metrics().cloned();
        let limits = burst.limits();
        let deadline = limits.and_then(|limits| {
            previous_cycle
                .unwrap_or(self.started_at)
                .min(first_received)
                .checked_add(limits.max_age)
        });
        let mut linger = None;
        let reason = loop {
            if burst.is_full() {
                break if limits.is_some_and(|limits| burst.len() >= limits.max_inputs.max(1)) {
                    "envelope_cap"
                } else if limits.is_some_and(|limits| burst.bytes() >= limits.max_bytes.max(1)) {
                    "byte_cap"
                } else {
                    "buffer_drained"
                };
            }
            let mut command = match self.receiver.try_recv() {
                Ok(command) => command,
                Err(mpsc::error::TryRecvError::Disconnected) => break "channel_closed",
                Err(mpsc::error::TryRecvError::Empty) => {
                    if *self.closing.borrow() {
                        break "shutdown";
                    }
                    if limits.is_none_or(|limits| limits.max_age.is_zero()) {
                        break "buffer_drained";
                    }
                    let Some(deadline) = deadline else {
                        break "deadline";
                    };
                    if Instant::now() >= deadline {
                        break "deadline";
                    }
                    linger.get_or_insert_with(Instant::now);
                    tokio::select! {
                        biased;
                        _ = self.closing.changed() => break "shutdown",
                        () = tokio::time::sleep_until(deadline.into()) => break "deadline",
                        command = self.receiver.recv() => match command {
                            Some(command) => command,
                            None => break "channel_closed",
                        },
                    }
                }
            };
            command.received();
            match command {
                Command::Apply(input) => {
                    let ApplyCommand {
                        batch,
                        options,
                        reply,
                        admitted_at,
                        charge,
                    } = *input;
                    let byte_cap = limits.is_some_and(|limits| {
                        burst.bytes().saturating_add(batch.estimated_bytes())
                            > limits.max_bytes.max(1)
                    });
                    match burst.push(batch, options) {
                        ControlFlow::Continue(()) => replies.push_back(reply),
                        ControlFlow::Break(batch) => {
                            self.carried = Some(Command::Apply(Box::new(ApplyCommand {
                                batch,
                                options,
                                reply,
                                admitted_at,
                                charge,
                            })));
                            break if byte_cap {
                                "byte_cap"
                            } else {
                                "buffer_drained"
                            };
                        }
                    }
                }
                other => {
                    self.carried = Some(other);
                    break "buffer_drained";
                }
            }
        };
        if let (Some(ingress), Some(start)) = (ingress, linger) {
            ingress.record_linger(start);
        }
        reason
    }

    async fn apply_append(&mut self, mut burst: AppendBurst, reply: Reply, admitted_at: Instant) {
        let mut replies = VecDeque::from([reply]);
        let reason = self
            .drain(
                &mut burst,
                &mut replies,
                admitted_at,
                self.previous_append_cycle,
            )
            .await;
        self.previous_append_cycle = Some(Instant::now());
        tracing::debug!(
            dataset = %burst.dataset(), inputs = burst.len(), bytes = burst.bytes(),
            close_reason = reason, "Applying coalesced Rows append"
        );
        let burst_dataset = burst.dataset().clone();
        let (write, options) = burst.finish();
        let replies = replies.into_iter().collect::<Vec<_>>();
        match write {
            // Each member's own write, kept to retry one at a time if the
            // backend refuses the coalesced write before changing anything.
            RowsWrite::Scoped(write)
                if replies.len() > 1 && write.inputs().len() == replies.len() =>
            {
                let members = write.members();
                self.apply_members(&burst_dataset, write, members, options, replies)
                    .await;
            }
            write => {
                self.apply(write.into(), options, replies).await;
            }
        }
    }

    /// Apply a coalesced Rows burst so that one member's refusal refuses only
    /// that member. When the backend refuses the burst before mutation and names
    /// the first member that would be refused on its own, the members before it
    /// are applied together, that member is refused, and the rest coalesce
    /// again. In arrival order, every member meets exactly the state it would
    /// have met had the burst never coalesced.
    async fn apply_members(
        &mut self,
        dataset: &TableReference,
        mut write: ScopedWrite,
        mut members: Vec<ScopedWrite>,
        options: WriteOptions,
        mut replies: Vec<Reply>,
    ) {
        loop {
            let Err((error, returned)) = self
                .apply_unless_refused(scoped(write), options, replies)
                .await
            else {
                return;
            };
            replies = returned;
            if !super::provider::refusal::is_before_mutation(&error) {
                self.refuse(error, options, replies);
                return;
            }
            let Some(position) = super::provider::refusal::refused_member(&error)
                .filter(|&position| position < members.len())
            else {
                tracing::debug!(
                    dataset = %dataset, members = replies.len(), %error,
                    "Retrying a refused coalesced Rows burst one member at a time"
                );
                self.apply_one_by_one(members, options, replies).await;
                return;
            };
            let mut rest_members = members.split_off(position);
            let mut rest_replies = replies.split_off(position);
            rest_members.remove(0);
            let refused = rest_replies.remove(0);
            if !members.is_empty() {
                self.apply_together(members, options, replies).await;
            }
            self.refuse(error, options, vec![refused]);
            if let Some(failure) = &self.failure {
                for reply in rest_replies {
                    reply.refuse(failure);
                }
                return;
            }
            if rest_members.is_empty() {
                return;
            }
            let Some(rest) = ScopedWrite::coalesce(&rest_members) else {
                self.apply_one_by_one(rest_members, options, rest_replies)
                    .await;
                return;
            };
            write = rest;
            members = rest_members;
            replies = rest_replies;
        }
    }

    /// Apply members already known to pass in order as one write.
    async fn apply_together(
        &mut self,
        members: Vec<ScopedWrite>,
        options: WriteOptions,
        replies: Vec<Reply>,
    ) {
        match ScopedWrite::coalesce(&members) {
            Some(write) => {
                self.apply(scoped(write), options, replies).await;
            }
            None => self.apply_one_by_one(members, options, replies).await,
        }
    }

    async fn apply_one_by_one(
        &mut self,
        members: Vec<ScopedWrite>,
        options: WriteOptions,
        replies: Vec<Reply>,
    ) {
        for (member, reply) in members.into_iter().zip(replies) {
            if let Some(failure) = &self.failure {
                reply.refuse(failure);
            } else {
                self.apply(scoped(member), options, vec![reply]).await;
            }
        }
    }

    async fn apply_cdc(
        &mut self,
        rows: CdcRows,
        options: WriteOptions,
        reply: Reply,
        admitted_at: Instant,
    ) {
        let mut replies = VecDeque::from([reply]);
        let mut burst = CdcBurst::new(rows, options);
        let reason = self
            .drain(&mut burst, &mut replies, admitted_at, self.previous_cycle)
            .await;
        let ingress = burst.ingress().cloned();
        let _applying = ingress.as_ref().map(ApplyingGuard::enter);
        if let Some(ingress) = &ingress {
            ingress.record_drain(burst.len(), burst.bytes(), admitted_at, reason);
            if let Some(previous) = self.previous_cycle {
                ingress.record_cycle(previous);
            }
        }
        let start = Instant::now();
        self.previous_cycle = Some(start);
        let _timer = BurstTimer { ingress, start };
        let capabilities = self.backend.capabilities();
        let prepared = match burst.prepare(self.backend.schema(), capabilities).await {
            Ok(prepared) => prepared,
            Err(error) => {
                let error = self.fail(error);
                for reply in replies {
                    reply.refuse(&error);
                }
                return;
            }
        };
        if prepared
            .iter()
            .map(|group| group.input_count)
            .sum::<usize>()
            != replies.len()
        {
            let error = self.fail(DataFusionError::Internal(
                "CDC burst lost a result observer during preparation".into(),
            ));
            for reply in replies {
                reply.refuse(&error);
            }
            return;
        }
        // Non-live evolution must refuse the whole burst before an earlier
        // group can delete or truncate. The backend supplies the exact refusal.
        if capabilities.schema_evolution != SchemaEvolutionSupport::Live
            && let Some(plan) = prepared.iter().find_map(|group| match &group.schema {
                SchemaDecision::Evolve(plan) => Some(plan),
                SchemaDecision::Proceed => None,
            })
        {
            let error = if let Err(error) = self.settle().await {
                error
            } else {
                let error = self
                    .backend
                    .evolve_schema(plan)
                    .await
                    .err()
                    .unwrap_or_else(|| {
                        DataFusionError::Internal(
                            "A non-live CDC backend accepted schema evolution".into(),
                        )
                    });
                self.fail(error)
            };
            for reply in replies {
                reply.refuse(&error);
            }
            return;
        }
        for group in prepared {
            if let Some(error) = &self.failure {
                for reply in replies {
                    reply.refuse(error);
                }
                break;
            }
            if let SchemaDecision::Evolve(plan) = &group.schema {
                let result = match self.settle().await {
                    Ok(()) => self
                        .backend
                        .evolve_schema(plan)
                        .await
                        .map_err(|error| self.fail(error)),
                    Err(error) => Err(error),
                };
                if let Err(error) = result {
                    for reply in replies {
                        reply.refuse(&error);
                    }
                    break;
                }
                if let Some(ingress) = &group.ingress {
                    ingress.policy.applied(plan);
                }
            }
            let observers = replies.drain(..group.input_count).collect();
            if self
                .apply(group.batch.into(), group.options, observers)
                .await
                && let Some(ingress) = &group.ingress
            {
                ingress.record_applied(group.rows);
            }
        }
    }

    async fn apply(
        &mut self,
        write: ChangeWrite,
        options: WriteOptions,
        replies: Vec<Reply>,
    ) -> bool {
        match self.apply_unless_refused(write, options, replies).await {
            Ok(applied) => applied,
            Err((error, replies)) => self.refuse(error, options, replies),
        }
    }

    /// Apply one write. A backend error returns the observers unrefused so the
    /// caller can decide whether the write is retried in parts.
    async fn apply_unless_refused(
        &mut self,
        batch: ChangeWrite,
        options: WriteOptions,
        mut replies: Vec<Reply>,
    ) -> std::result::Result<bool, (DataFusionError, Vec<Reply>)> {
        // Only decoded, preflighted CDC upserts may overlap a previous finalizer.
        let must_settle = self
            .pending
            .as_ref()
            .is_some_and(|publication| publication.is_ready() || !batch.permits_pipelined_append());
        if must_settle && let Err(error) = self.settle().await {
            for reply in replies {
                reply.refuse(&error);
            }
            return Ok(false);
        }
        match self.backend.apply(batch, options, &self.context).await {
            Ok(write) => {
                // Staging may overlap, but finalization cannot cross a failed
                // predecessor. Dropping observers never drops this ownership.
                if let Err(error) = self.settle().await {
                    drop(write);
                    for reply in replies {
                        reply.refuse(&error);
                    }
                    return Ok(false);
                }
                let completions = replies
                    .iter_mut()
                    .filter_map(|reply| reply.completion.take())
                    .collect();
                let receipt = start_finalization(write, completions);
                if !matches!(receipt.publication, Publication::Ready) {
                    self.pending = Some(receipt.publication.clone());
                }
                let last = replies.len().saturating_sub(1);
                for (index, reply) in replies.into_iter().enumerate() {
                    if let Some(result) = reply.result {
                        let mut member = receipt.clone();
                        member.batch_end = index == last;
                        let _ = result.send(Ok(member));
                    }
                }
                Ok(true)
            }
            Err(error) => Err((error, replies)),
        }
    }

    /// Refuse every observer of a failed write. Only a rebuildable write
    /// refused before mutation leaves the owner accepting work.
    fn refuse(
        &mut self,
        error: DataFusionError,
        options: WriteOptions,
        replies: Vec<Reply>,
    ) -> bool {
        let can_continue = options.recovery == Recovery::Rebuildable
            && super::provider::refusal::is_before_mutation(&error);
        let error = if can_continue {
            Arc::new(error)
        } else {
            self.fail(error)
        };
        for reply in replies {
            reply.refuse(&error);
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn apply_is_boxed_so_command_stays_under_the_large_variant_lint() {
        let command = std::mem::size_of::<Command>();
        let apply = std::mem::size_of::<ApplyCommand>();
        assert!(
            apply >= 200,
            "ApplyCommand is {apply} bytes; the lint exists because this payload is large"
        );
        assert!(
            command < 200,
            "Command is {command} bytes; box Apply so clippy::large_enum_variant stays silent"
        );
    }
}
