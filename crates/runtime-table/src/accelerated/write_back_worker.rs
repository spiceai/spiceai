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

//! Durable federated write-back delivery worker (#11838).
//!
//! One worker per durable-write-back Cayenne dataset, spawned onto the
//! [`AcceleratedTable`](super::AcceleratedTable)'s `handlers` (aborted on drop).
//! It reconciles the dirty-key markers a committed write leaves in
//! `cayenne_pending_write_back` to the federated source, in strict order:
//!
//! 1. **List** a batch of the oldest markers (`list_dirty_keys`) — a plain
//!    read, NOT an atomic reservation; concurrency safety comes from the
//!    compare-and-clear in step 4, not from claiming these markers here.
//! 2. **Read** those keys' *current* committed values from the accelerator
//!    (a fenced point scan), AFTER the list.
//! 3. **Deliver** the rows the read returned to the source as an idempotent
//!    upsert. A claimed key the read did not return is *withheld* — see *Absence
//!    is not a deletion*.
//! 4. **Compare-and-clear** the delivered markers whose stored sequence is still
//!    at or below the sequence listed in step 1 — a newer commit that bumped a
//!    marker during delivery leaves it in place, so the stale delivery never
//!    clears a fresh mark.
//!
//! Delivery failure never blocks accelerator commits; the dirty set simply
//! grows until the next successful pass. Marking happens only in the
//! commit-publish transaction (never in the CDC apply path), so an echo of our
//! own write cannot spawn a fresh delivery.
//!
//! # Absence is not a deletion (#13398)
//!
//! A marker means "this key was written and has not reached the source yet". It
//! can mean nothing else: markers are written only by the transactional commit
//! path, and a transaction accepts only `INSERT`/`UPDATE` with no primary-key
//! reassignment (`runtime::datafusion::query::transaction`), while a write-back
//! dataset refuses `DELETE` outright. So no key can leave the durable set by any
//! user action, and a claimed key the post-claim read does not return says
//! nothing about the source row.
//!
//! Such a key is therefore withheld: nothing is delivered for it and its marker
//! is kept for a later pass. Reading absence as a deletion is what let a
//! retention prune, a commit that was not yet scan-visible, or any scan anomaly
//! delete rows from the system of record that nobody deleted.
//!
//! # Claim window
//!
//! A withheld marker stays in the table and the claim is ordered by commit
//! sequence, so re-claiming the oldest markers every pass would hand every later
//! pass the same undeliverable set once a page of them accumulated, and never
//! reach a newer write. A pass that could not deliver everything it claimed
//! therefore resumes past that page, and starts again at the oldest marker when a
//! page comes back short.
//!
//! # Known limitation — a source-side change to an undelivered key
//!
//! A key with an undelivered marker that also changes at the source is a second
//! writer on that row, and the two are not reconciled. If the delivery lands
//! first it overwrites the source's change; if the source's change arrives over
//! CDC first it removes or replaces the row locally, and a delete leaves a marker
//! that can never be delivered — the write is acknowledged and lost. Neither is
//! detected. Durable write-back is supported only where the accelerator is the
//! sole writer of the rows it delivers.
//!
//! # Known limitation — mixed writers
//!
//! The present-key upsert is an unconditional `ON CONFLICT (pk) DO UPDATE`: it
//! overwrites the source row with the accelerator's value regardless of what
//! the source currently holds. A second writer that mutates the same source row
//! directly (not through this accelerator) can therefore be clobbered — this
//! worker does no compare-and-set against the source. Durable write-back is
//! safe only when the accelerator is the sole writer of the rows it delivers.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, LazyLock, Weak};
use std::time::{Duration, Instant};

use arrow::array::{Array, ArrayRef};
use arrow::record_batch::RecordBatch;
use cayenne::CayenneTableProvider;
use data_connector_api::write_back::WriteBackDeliverer;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::Expr;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion::scalar::ScalarValue;
use opentelemetry::KeyValue;
use parking_lot::Mutex;
use runtime_metrics::acceleration as metrics;
use tokio::task::JoinHandle;
use util::fibonacci_backoff::FibonacciBackoffBuilder;
use util::tracers::SpacedTracer;
use util::warn_spaced;

use data_components::pk_filter_expr::build_pk_in_list;
use runtime_acceleration::acceleration::{DurableWriteBackKey, classify_durable_write_back_key};

use super::AcceleratedTableBuilderError;
use super::write::write_back::execute_insert;
use crate::federated::FederatedTable;

/// Markers claimed per delivery pass.
const CLAIM_BATCH: usize = 1024;
/// Idle poll interval when the dirty set is empty (not a failure — the error
/// backoff must not grow on empty polls).
const POLL_INTERVAL: Duration = Duration::from_secs(1);
/// How often a delivery stall repeats its explanation. Long enough that a stall
/// does not flood the log at the poll interval, short enough that an operator
/// reading logs finds the reason without waiting.
const WITHHELD_WARNING_INTERVAL: Duration = Duration::from_mins(5);

/// A generation token for workers that publish the same dataset-labelled gauge.
struct WriteBackGaugeOwner;

/// Live write-back workers in spawn order for each dataset.
///
/// Hot reload starts a successor before aborting its predecessor. Both workers
/// use the same metric labels, so only the newest live generation may publish;
/// otherwise a delayed predecessor teardown can overwrite the successor's
/// backlog with zero. Keeping the predecessor in the stack lets it resume
/// ownership if a candidate replacement fails before installation.
#[derive(Default)]
struct WriteBackGaugeOwners {
    by_dataset: HashMap<String, Vec<Weak<WriteBackGaugeOwner>>>,
}

impl WriteBackGaugeOwners {
    fn register(&mut self, dataset_name: &str, owner: &Arc<WriteBackGaugeOwner>) {
        let owners = self.by_dataset.entry(dataset_name.to_string()).or_default();
        owners.retain(|candidate| candidate.strong_count() > 0);
        owners.push(Arc::downgrade(owner));
    }

    fn is_current(&self, dataset_name: &str, owner: &Arc<WriteBackGaugeOwner>) -> bool {
        self.by_dataset
            .get(dataset_name)
            .and_then(|owners| owners.last())
            .is_some_and(|current| current.as_ptr() == Arc::as_ptr(owner))
    }

    /// Remove `owner`; return whether its teardown should zero the gauge.
    fn unregister(&mut self, dataset_name: &str, owner: &Arc<WriteBackGaugeOwner>) -> bool {
        let owner_ptr = Arc::as_ptr(owner);
        let Some(owners) = self.by_dataset.get_mut(dataset_name) else {
            return false;
        };
        let was_current = owners
            .last()
            .is_some_and(|current| current.as_ptr() == owner_ptr);
        owners.retain(|candidate| candidate.as_ptr() != owner_ptr && candidate.strong_count() > 0);
        let no_owner_remains = owners.is_empty();
        if no_owner_remains {
            self.by_dataset.remove(dataset_name);
        }
        was_current && no_owner_remains
    }
}

static WRITE_BACK_GAUGE_OWNERS: LazyLock<Mutex<WriteBackGaugeOwners>> =
    LazyLock::new(|| Mutex::new(WriteBackGaugeOwners::default()));

pub(crate) struct WriteBackWorker {
    /// A write-clone of the durable-write-back Cayenne provider — shares the
    /// live table's catalog, listing fence, and keyset, so the marker CRUD and
    /// the point scan observe committed state.
    provider: Arc<CayenneTableProvider>,
    federated: Arc<FederatedTable>,
    /// Connector-owned delivery, when the source provides it (`PostgreSQL`, so it
    /// can stamp each delivery transaction's id for the CDC echo filter). `None`
    /// keeps the `TableProvider` delivery path below, byte-identical.
    deliverer: Option<Arc<dyn WriteBackDeliverer>>,
    /// The single primary-key column deliveries are keyed on, proven at
    /// construction so no later step re-derives it.
    pk_column: String,
    dataset_name: String,
    /// `(sequence_number, pk_bytes)` of the last marker claimed, when the last
    /// pass could not deliver all of it; `None` claims from the oldest marker.
    /// See *Claim window*.
    claim_after: Option<(i64, Vec<u8>)>,
    /// Consecutive sweeps whose first page was wholly unreadable; the
    /// stuck-delivery warning fires on the second.
    unreadable_first_pages: u32,
    /// Rate limiter for the stuck-delivery warning. A stall persists, and the
    /// operator who starts reading logs after it began still needs to be told
    /// why the backlog is not falling, so the message repeats on an interval
    /// rather than once.
    withheld_tracer: SpacedTracer,
    /// The backlog gauge's label set, built once.
    dataset_labels: [KeyValue; 1],
    gauge_owner: Arc<WriteBackGaugeOwner>,
}

impl WriteBackWorker {
    /// Start the delivery loop; the returned handle is pushed onto the accelerated
    /// table's `handlers` and aborted when the table drops.
    ///
    /// Separate from [`Self::new`] so the fallible half runs before the table
    /// starts any background task: a failure after that point returns from
    /// `build`, and the accumulated handles detach rather than abort.
    pub(crate) fn start(mut self) -> JoinHandle<()> {
        WRITE_BACK_GAUGE_OWNERS
            .lock()
            .register(&self.dataset_name, &self.gauge_owner);
        tokio::spawn(async move { self.run().await })
    }

    /// Build the worker, or refuse a key it could never deliver on.
    ///
    /// Separate from [`Self::spawn`] so a test constructs a worker the same way
    /// production does — through both key checks — instead of filling the fields
    /// itself and skipping them.
    ///
    /// # Errors
    ///
    /// [`AcceleratedTableBuilderError::DurableWriteBackUndeliverableKey`] when the
    /// accelerator resolved anything but a single primary-key column, and
    /// [`AcceleratedTableBuilderError::DurableWriteBackKeyMismatch`] when the
    /// deliverer would upsert on a different column than the one this worker marks.
    pub(crate) fn new(
        provider: CayenneTableProvider,
        federated: Arc<FederatedTable>,
        dataset_name: String,
        deliverer: Option<Arc<dyn WriteBackDeliverer>>,
    ) -> Result<Self, AcceleratedTableBuilderError> {
        let pk_columns = provider.pk_column_names();
        // The same classifier registration used, over the key the accelerator
        // actually resolved, so the rule cannot hold on one side and not the other.
        // The proven column is carried from here rather than re-derived, so
        // nothing downstream has to remember that the check happened.
        let pk_column = match classify_durable_write_back_key(&pk_columns) {
            DurableWriteBackKey::Single(single) => single.to_string(),
            DurableWriteBackKey::Undeclared | DurableWriteBackKey::Composite(_) => {
                return Err(
                    AcceleratedTableBuilderError::DurableWriteBackUndeliverableKey {
                        dataset_name,
                        pk_columns: pk_columns.len(),
                    },
                );
            }
        };
        // The connector builds its upsert target from the DECLARED key while the
        // markers above use the RESOLVED one. Same arity is not enough: if they
        // name different columns, `ON CONFLICT (declared)` inserts a second source
        // row rather than updating the row this worker marked.
        //
        // This covers the connector-owned path only. The `TableProvider` fallback
        // below writes through a provider whose conflict target this worker cannot
        // see, so the same disagreement would go unchecked there. Registration
        // refuses a connector that does not advertise durable delivery, and the one
        // that advertises it always supplies a deliverer, so no dataset reaches the
        // fallback today; a connector added with atomic `InsertOp::Replace` and no
        // deliverer would need this comparison extended to it.
        if let Some(deliverer) = &deliverer {
            let target = deliverer.conflict_key();
            if target != std::slice::from_ref(&pk_column) {
                return Err(AcceleratedTableBuilderError::DurableWriteBackKeyMismatch {
                    dataset_name,
                    resolved: pk_column,
                    declared: target.join(", "),
                });
            }
        }

        Ok(Self {
            claim_after: None,
            unreadable_first_pages: 0,
            withheld_tracer: SpacedTracer::new(WITHHELD_WARNING_INTERVAL),
            provider: Arc::new(provider),
            federated,
            deliverer,
            pk_column,
            dataset_labels: [KeyValue::new("dataset", dataset_name.clone())],
            dataset_name,
            gauge_owner: Arc::new(WriteBackGaugeOwner),
        })
    }

    async fn run(&mut self) {
        // Infinite Fibonacci backoff on delivery ERRORS (delivery must never
        // permanently give up). Rebuilt after every successful pass so a
        // transient failure never leaves us stuck at a long delay, and never
        // advanced by an empty poll (an empty dirty set is not a failure).
        let mut backoff = FibonacciBackoffBuilder::new().max_retries(None).build();
        let mut last_backlog_sample = None;
        loop {
            match self.deliver_batch().await {
                Ok(pass) => {
                    // Success — reset the error backoff.
                    backoff = FibonacciBackoffBuilder::new().max_retries(None).build();
                    if pass.claimed == 0 && pass.claimed_from_oldest {
                        // Nothing claimed from the oldest marker on, which proves
                        // the dirty set is empty — publish that without paying
                        // for a count.
                        self.publish_backlog(0);
                        last_backlog_sample = Some(Instant::now());
                        tokio::time::sleep(POLL_INTERVAL).await;
                    } else if pass.claimed < CLAIM_BATCH || pass.delivered == 0 {
                        // The dirty set is drained, or this pass delivered nothing
                        // of what it claimed. Idle-poll either way — NOT an error,
                        // so the backoff stays reset, and a pass that delivers
                        // nothing must not spin: paging markers it cannot deliver
                        // at full tilt would burn a query, a scan and a plan per
                        // iteration for no delivery.
                        if self.count_and_publish_backlog().await {
                            last_backlog_sample = Some(Instant::now());
                        }
                        tokio::time::sleep(POLL_INTERVAL).await;
                    } else if last_backlog_sample
                        .is_none_or(|sampled_at| sampled_at.elapsed() >= POLL_INTERVAL)
                    {
                        // A count scans the remaining dirty set, so sample at
                        // most once per poll interval while full batches drain.
                        if self.count_and_publish_backlog().await {
                            last_backlog_sample = Some(Instant::now());
                        }
                    }
                }
                Err(e) => {
                    let delay = backoff.next_duration().unwrap_or(POLL_INTERVAL);
                    tracing::warn!(
                        dataset = %self.dataset_name,
                        error = %e,
                        "durable write-back delivery failed; retrying in {delay:?}"
                    );
                    if self.count_and_publish_backlog().await {
                        last_backlog_sample = Some(Instant::now());
                    }
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }

    /// Publish an already-known undelivered-marker backlog for this dataset.
    fn publish_backlog(&self, pending: i64) {
        let owners = WRITE_BACK_GAUGE_OWNERS.lock();
        if !owners.is_current(&self.dataset_name, &self.gauge_owner) {
            return;
        }
        metrics::WRITE_BACK_PENDING_KEYS.record(pending, &self.dataset_labels);
    }

    /// Count this dataset's undelivered markers and publish them.
    ///
    /// A delivery that can never succeed surfaces only as a backlog that never
    /// falls, so the failing pass has to publish it too — that is the state the
    /// gauge exists to make visible.
    async fn count_and_publish_backlog(&self) -> bool {
        match self.provider.dirty_key_count().await {
            Ok(pending) => {
                self.publish_backlog(pending);
                true
            }
            Err(e) => {
                tracing::debug!(
                    dataset = %self.dataset_name,
                    error = %e,
                    "durable write-back: could not read the undelivered-marker backlog for the gauge"
                );
                false
            }
        }
    }

    /// One claim → read → deliver → clear pass.
    async fn deliver_batch(&mut self) -> DataFusionResult<PassOutcome> {
        let claimed_from_oldest = self.claim_after.is_none();
        let claimed = self
            .provider
            .list_dirty_keys(CLAIM_BATCH, self.claim_after.as_ref())
            .await
            .map_err(to_df_err)?;
        if claimed.is_empty() {
            self.claim_after = None;
            if claimed_from_oldest {
                self.unreadable_first_pages = 0;
            }
            return Ok(PassOutcome {
                claimed_from_oldest,
                claimed: 0,
                delivered: 0,
            });
        }

        let pk_bytes: Vec<Vec<u8>> = claimed.iter().map(|(bytes, _)| bytes.clone()).collect();
        let pk_arrays = self.provider.decode_pk_keys(&pk_bytes).map_err(to_df_err)?;
        let pk_values = pk_arrays.into_iter().next().ok_or_else(|| {
            DataFusionError::Internal(
                "durable write-back marker decode returned no primary-key column".to_string(),
            )
        })?;
        let filter = pk_in_filter(&self.pk_column, &pk_values)?;

        // Read the claimed keys' current committed values from the accelerator,
        // AFTER the claim (a newer commit bumps the marker above the claimed
        // sequence, so the clear below no-ops for it). Build the context from the
        // provider's SHARED RuntimeEnv (object-store registrations for S3, memory
        // pool, caches) — a fresh `SessionContext::new()` would lose them and fail
        // object-store-backed scans.
        let ctx =
            SessionContext::new_with_config_rt(SessionConfig::new(), self.provider.runtime_env());
        // `Arc<CayenneTableProvider>` coerces to the `Arc<dyn TableProvider>`
        // `read_table` expects at the call argument below.
        let accelerator = Arc::clone(&self.provider);
        let current = ctx
            .read_table(accelerator)?
            .filter(filter)?
            .collect()
            .await?;
        let session_state = ctx.state();

        // Deliver only the claimed keys the post-claim read returned. A key it
        // did not return is withheld, marker and all — see *Absence is not a
        // deletion*.
        let pk_col = self.pk_column.as_str();
        let deliverable = deliverable_markers(pk_col, &claimed, &pk_values, &current)?;
        // Whether this pass sends anything. Derived from `deliverable` rather
        // than from the batches a second way: the read is filtered to the
        // claimed keys, so the two agree, and this one cannot deliver a row that
        // no marker will retire.
        let has_present = !deliverable.is_empty();
        if claimed_from_oldest {
            // Counted on a sweep's first page only. A withheld marker keeps its
            // sequence number, so a key that never becomes readable settles on
            // that page and is claimed there again every sweep.
            if has_present {
                self.unreadable_first_pages = 0;
            } else {
                self.unreadable_first_pages += 1;
            }
        }
        if deliverable.len() < claimed.len() {
            let withheld = claimed.len() - deliverable.len();
            // A withhold is usually a commit the scan cannot see yet, gone by the
            // next pass. A first page still wholly unreadable a sweep later is
            // not: it is the shape of keys that will never become readable, and
            // the only other signal is a
            // `dataset_acceleration_write_back_pending_keys` that stops falling.
            if claimed_from_oldest && self.unreadable_first_pages >= 2 {
                let message = stuck_delivery_warning(&self.dataset_name, withheld);
                warn_spaced!(self.withheld_tracer, "{}{message}", "");
            } else {
                tracing::debug!(
                    dataset = %self.dataset_name,
                    withheld,
                    "durable write-back: {withheld} committed key(s) were not readable in the accelerator; keeping their markers to retry rather than reading absence as a deletion",
                );
            }
        }

        if let Some(deliverer) = &self.deliverer {
            // Connector-owned delivery (Postgres): the connector owns the
            // delivery transaction so it can stamp the transaction id for the CDC
            // echo filter. One transaction covers every readable row, and the
            // upsert is idempotent, so a failed pass replays whole. Casting each
            // batch to the source schema is the deliverer's own concern (see
            // `WriteBackDeliverer::deliver_upserts`), not the worker's.
            if has_present {
                deliverer
                    .deliver_upserts(current)
                    .await
                    .map_err(delivery_to_df_err)?;
            }
        } else {
            let federated_provider = self.federated.table_provider().await;

            // A native upsert, or nothing. Emulating one as a separate DELETE
            // and INSERT would commit a delete at the source that the CDC stream
            // echoes back and applies to the accelerator, erasing the committed
            // row; a failure between the two legs then loses the write from both
            // sides. Registration refuses a connector that cannot deliver
            // atomically (`supports_durable_write_back_delivery`), so reaching
            // this is that gate and the connector disagreeing — surface it and
            // keep the markers rather than emulate.
            if has_present {
                let expected: usize = current.iter().map(RecordBatch::num_rows).sum();
                let reported = execute_insert(
                    Arc::clone(&federated_provider),
                    self.provider.table_schema(),
                    current,
                    InsertOp::Replace,
                    &session_state,
                    None,
                )
                .await
                .map_err(|e| match e {
                    DataFusionError::NotImplemented(_) => DataFusionError::Execution(format!(
                        "Failed to write back dataset '{}' to its federated source: the source cannot apply a delivered row in one statement, so the rows committed to the accelerator have not reached it. They are kept and retried. This dataset should not have been accepted for durable write-back; check the connector's support for atomic delivery. See: https://spiceai.org/docs/reference/spicepod/datasets#acceleration",
                        self.dataset_name
                    )),
                    other => other,
                })?;

                // The markers clear next, and clearing one is this worker's only
                // record that the row was owed. A source that accepted the
                // statement but reports writing fewer rows than were sent has not
                // taken them all, so keep every marker and retry rather than
                // retire a delivery that did not happen. A source that reports no
                // count at all says nothing either way and is left alone.
                if let Some(written) = reported
                    && written < expected as u64
                {
                    return Err(DataFusionError::Execution(format!(
                        "Failed to write back dataset '{}' to its federated source: {written} of {expected} committed row(s) were written, so the rest have not reached the source. They are kept and retried. See: https://spiceai.org/docs/reference/spicepod/datasets#acceleration",
                        self.dataset_name
                    )));
                }
            }
        }

        // Ack: clear only the delivered markers, and only while their stored
        // sequence is still at or below the claimed one. A withheld key keeps its
        // marker; nothing but delivery retires one.
        self.provider
            .clear_dirty_keys(&deliverable)
            .await
            .map_err(to_df_err)?;

        // Resume past this page when it held anything undeliverable, so one such
        // key cannot pin the claim and starve every newer write. Assigned only
        // now, after the pass has delivered and cleared: a pass that failed
        // part-way returned above, and has to claim the same page again rather
        // than leave markers it never judged behind the cursor.
        self.claim_after = if deliverable.len() < claimed.len() {
            claimed
                .last()
                .map(|(pk_bytes, sequence)| (*sequence, pk_bytes.clone()))
        } else {
            None
        };
        Ok(PassOutcome {
            claimed_from_oldest,
            claimed: claimed.len(),
            delivered: deliverable.len(),
        })
    }
}

impl Drop for WriteBackWorker {
    fn drop(&mut self) {
        let mut owners = WRITE_BACK_GAUGE_OWNERS.lock();
        if owners.unregister(&self.dataset_name, &self.gauge_owner) {
            // No live worker owns these labels. Synchronous gauges retain their
            // last value, so clear the removed dataset's stale backlog.
            metrics::WRITE_BACK_PENDING_KEYS.record(0, &self.dataset_labels);
        }
    }
}

/// Build `pk_col IN (values…)` from a decoded primary-key array, in the shape
/// Cayenne's `pk_filter_extract` fast path recognises.
fn pk_in_filter(pk_col: &str, values: &ArrayRef) -> DataFusionResult<Expr> {
    let mut scalars: Vec<ScalarValue> = Vec::with_capacity(values.len());
    for index in 0..values.len() {
        scalars.push(ScalarValue::try_from_array(values.as_ref(), index)?);
    }
    build_pk_in_list(pk_col, scalars).ok_or_else(|| {
        DataFusionError::Internal(
            "durable write-back built a primary-key filter from no keys".to_string(),
        )
    })
}

/// What one delivery pass claimed and how much of it it could deliver.
#[derive(Clone, Copy, Debug)]
struct PassOutcome {
    /// Whether the claim started at the oldest marker rather than resuming past
    /// a page an earlier pass could not deliver.
    claimed_from_oldest: bool,
    /// Markers claimed.
    claimed: usize,
    /// Markers whose row reached the source, and whose markers cleared.
    delivered: usize,
}

/// The warning a dataset gets when a sweep's first page is wholly unreadable two
/// sweeps running.
///
/// This is the only account an operator gets of a backlog that stops falling, so
/// it names the dataset, what it means for the data, and the one cause that does
/// not resolve itself.
fn stuck_delivery_warning(dataset_name: &str, withheld: usize) -> String {
    format!(
        "Dataset '{dataset_name}' could not read any of the {withheld} committed row(s) it is delivering to its federated source, so none of them reached the source and their write-back is being retried rather than treated as a deletion. Rows removed from the accelerator by something other than a write to this dataset -- a change stream applying a source-side delete, or a truncate -- never become readable, and their delivery stays stuck: check whether another writer is changing this dataset at the source. See: https://spiceai.org/docs/reference/spicepod/datasets#acceleration"
    )
}

/// The claimed markers this pass may deliver and then clear: those whose key the
/// post-claim accelerator read returned, in claim order.
///
/// `claimed_pks` are the decoded keys of `claimed`, position for position;
/// `current` holds the rows the read returned. A claimed key the read did not
/// return is left out, which withholds it — its marker is not cleared and nothing
/// is delivered for it. That is the whole of #13398: absence is not evidence that
/// anyone deleted the row, and a write-back dataset has no delete path at all, so
/// a marker can only ever mean "this key was written".
fn deliverable_markers(
    pk_col: &str,
    claimed: &[(Vec<u8>, i64)],
    claimed_pks: &ArrayRef,
    current: &[RecordBatch],
) -> DataFusionResult<Vec<(Vec<u8>, i64)>> {
    if claimed.len() != claimed_pks.len() {
        return Err(DataFusionError::Internal(format!(
            "durable write-back decoded {} primary key(s) for {} claimed marker(s)",
            claimed_pks.len(),
            claimed.len()
        )));
    }

    let mut present: HashSet<ScalarValue> = HashSet::with_capacity(claimed.len());
    for batch in current {
        let Some(column) = batch.column_by_name(pk_col) else {
            return Err(DataFusionError::Execution(format!(
                "durable write-back: primary-key column '{pk_col}' missing from the accelerator read"
            )));
        };
        for row in 0..column.len() {
            present.insert(ScalarValue::try_from_array(column.as_ref(), row)?);
        }
    }

    let mut deliverable = Vec::with_capacity(claimed.len());
    for (position, marker) in claimed.iter().enumerate() {
        let key = ScalarValue::try_from_array(claimed_pks.as_ref(), position)?;
        if present.contains(&key) {
            deliverable.push(marker.clone());
        }
    }
    Ok(deliverable)
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "passed to `Result::map_err`, which moves the error value in"
)]
fn to_df_err(e: cayenne::provider::Error) -> DataFusionError {
    DataFusionError::Execution(format!("durable write-back: {e}"))
}

/// Map a connector-owned delivery failure into the worker's error type. The
/// worker only needs the message for its log and its retry backoff — a failed
/// pass replays whole, so the variant is immaterial.
#[expect(
    clippy::needless_pass_by_value,
    reason = "passed to `Result::map_err`, which moves the error value in"
)]
fn delivery_to_df_err(e: data_connector_api::write_back::DeliveryError) -> DataFusionError {
    DataFusionError::Context(
        "durable write-back".to_string(),
        Box::new(DataFusionError::Execution(e.to_string())),
    )
}

#[cfg(test)]
mod tests {
    use super::{WriteBackGaugeOwner, WriteBackGaugeOwners, deliverable_markers, pk_in_filter};
    use arrow::array::{Int32Array, StringArray};
    use arrow::record_batch::RecordBatch;
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    fn id_batch(ids: Vec<i32>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(ids))])
            .expect("id batch builds")
    }

    /// Markers in claim order, `(pk_bytes, sequence)` — the shape
    /// `list_dirty_keys` returns.
    fn markers(ids: &[i32]) -> Vec<(Vec<u8>, i64)> {
        ids.iter()
            .map(|id| (id.to_be_bytes().to_vec(), i64::from(*id)))
            .collect()
    }

    fn keys(ids: &[i32]) -> arrow::array::ArrayRef {
        Arc::new(Int32Array::from(ids.to_vec()))
    }

    /// Only the claimed keys the read returned may be delivered and cleared.
    #[test]
    fn only_the_keys_the_read_returned_are_deliverable() {
        let claimed = markers(&[1, 2, 3, 4]);
        // The read returned rows for 1 and 3 only.
        let current = vec![id_batch(vec![1, 3])];

        let deliverable = deliverable_markers("id", &claimed, &keys(&[1, 2, 3, 4]), &current)
            .expect("classifies");
        assert_eq!(
            deliverable,
            markers(&[1, 3]),
            "the markers of the keys the read returned, in claim order"
        );
    }

    /// #13398: a read that returned nothing delivers nothing and clears nothing.
    /// It is not evidence that anyone deleted those rows — a write-back dataset
    /// has no delete path at all, so a marker can only ever mean "this key was
    /// written".
    #[test]
    fn an_empty_read_withholds_every_marker_rather_than_deleting() {
        let claimed = markers(&[5, 6, 7]);
        let deliverable =
            deliverable_markers("id", &claimed, &keys(&[5, 6, 7]), &[]).expect("classifies");
        assert!(
            deliverable.is_empty(),
            "an unreadable key is withheld, and its marker kept"
        );
    }

    /// A missing primary-key column in the read is an error, not a silent
    /// withholding of every key.
    #[test]
    fn missing_pk_column_in_read_is_an_error() {
        let schema = Arc::new(Schema::new(vec![Field::new("other", DataType::Utf8, true)]));
        let current = vec![
            RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec![Some("x")]))])
                .expect("batch"),
        ];
        deliverable_markers("id", &markers(&[1]), &keys(&[1]), &current)
            .expect_err("a missing pk column must error rather than withhold every key");
    }

    /// Markers and decoded keys pair by position, so a decode that does not cover
    /// the claim must error rather than clear a marker for another key.
    #[test]
    fn a_decode_that_does_not_cover_every_claimed_marker_is_an_error() {
        deliverable_markers("id", &markers(&[1, 2]), &keys(&[1]), &[id_batch(vec![1])])
            .expect_err("fewer decoded keys than claimed markers must error");
    }

    /// The read filter is `pk IN (keys…)`: the point-scan that decides which
    /// claimed markers are deliverable.
    /// A stalled dataset's backlog stops falling and this line is the only
    /// explanation, so it has to name the dataset, how many rows are stuck, what
    /// that means for them, and where to look.
    #[test]
    fn the_stall_warning_names_the_dataset_the_count_and_the_cause() {
        let message = super::stuck_delivery_warning("orders", 7);
        for expected in [
            "'orders'",
            "7 committed row(s)",
            "none of them reached the source",
            "rather than treated as a deletion",
            "another writer",
            "https://spiceai.org/docs/reference/spicepod/datasets#acceleration",
        ] {
            assert!(
                message.contains(expected),
                "the stall warning must contain {expected:?}: {message}"
            );
        }
    }

    #[test]
    fn pk_in_filter_lists_the_keys() {
        let values: arrow::array::ArrayRef = Arc::new(Int32Array::from(vec![8, 9]));
        let expr = pk_in_filter("id", &values).expect("filter builds");
        let rendered = format!("{expr}");
        assert!(rendered.contains("id"), "names the pk column: {rendered}");
        assert!(
            rendered.contains('8') && rendered.contains('9'),
            "lists the keys: {rendered}"
        );
    }

    #[test]
    fn gauge_ownership_returns_to_a_live_predecessor() {
        let mut owners = WriteBackGaugeOwners::default();
        let predecessor = Arc::new(WriteBackGaugeOwner);
        let successor = Arc::new(WriteBackGaugeOwner);

        owners.register("orders", &predecessor);
        owners.register("orders", &successor);
        assert!(!owners.is_current("orders", &predecessor));
        assert!(owners.is_current("orders", &successor));

        assert!(!owners.unregister("orders", &successor));
        assert!(owners.is_current("orders", &predecessor));
        assert!(owners.unregister("orders", &predecessor));
    }

    #[test]
    fn stale_predecessor_teardown_cannot_zero_the_current_gauge() {
        let mut owners = WriteBackGaugeOwners::default();
        let predecessor = Arc::new(WriteBackGaugeOwner);
        let successor = Arc::new(WriteBackGaugeOwner);

        owners.register("orders", &predecessor);
        owners.register("orders", &successor);

        assert!(!owners.unregister("orders", &predecessor));
        assert!(owners.is_current("orders", &successor));
        assert!(owners.unregister("orders", &successor));
    }
}

/// End-to-end coverage of [`WriteBackWorker::deliver_batch`] against a real
/// (local, no external dependency) `CayenneTableProvider` and a fake
/// [`WriteBackDeliverer`] — proving the routing this module's tests above
/// don't reach: present rows select `deliver_upserts`, an absent key is withheld
/// with its marker intact, a delivery error retains the markers and re-claims the
/// same page, and a fully successful pass clears them.
#[cfg(test)]
mod deliverer_tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    use arrow::array::{Int32Array, StringArray};
    use arrow::record_batch::RecordBatch;
    use arrow_schema::{DataType, Field, Schema, SchemaRef};
    use cayenne::metadata::{CreateTableOptions, VortexConfig};
    use cayenne::{
        CayenneCatalog, CayenneTableProvider, CayenneTableProviderBuilder, CayenneTransaction,
        MetadataCatalog,
    };
    use data_connector_api::write_back::{DeliveryError, DeliveryResult};
    use datafusion::datasource::{MemTable, TableProvider};
    use datafusion::execution::SendableRecordBatchStream;
    use datafusion::logical_expr::{col, lit};
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use datafusion::prelude::SessionContext;
    use datafusion_table_providers::util::column_reference::ColumnReference;
    use datafusion_table_providers::util::on_conflict::OnConflict;
    use parking_lot::Mutex;

    use super::{CLAIM_BATCH, WriteBackDeliverer, WriteBackWorker};
    use crate::federated::FederatedTable;

    /// Records every call it receives; `arm_failure` makes the next call
    /// (upsert or delete, whichever comes first) return an error instead.
    #[derive(Default)]
    struct FakeDeliverer {
        upserts: Mutex<Vec<Vec<RecordBatch>>>,
        fail_next: AtomicBool,
        /// The column this fake upserts on; must match what the fixture's
        /// provider resolves, exactly as a real connector's must.
        conflict_key: String,
    }

    impl FakeDeliverer {
        fn arc() -> Arc<Self> {
            Arc::new(Self {
                conflict_key: "id".to_string(),
                ..Self::default()
            })
        }

        fn arm_failure(&self) {
            self.fail_next.store(true, Ordering::SeqCst);
        }

        fn maybe_fail(&self) -> DeliveryResult<()> {
            if self.fail_next.swap(false, Ordering::SeqCst) {
                return Err(DeliveryError::Delivery {
                    message: "injected delivery failure".to_string(),
                    source: Box::new(std::io::Error::other("injected")),
                });
            }
            Ok(())
        }
    }

    #[async_trait::async_trait]
    impl WriteBackDeliverer for FakeDeliverer {
        fn conflict_key(&self) -> &[String] {
            std::slice::from_ref(&self.conflict_key)
        }

        async fn deliver_upserts(&self, rows: Vec<RecordBatch>) -> DeliveryResult {
            self.maybe_fail()?;
            self.upserts.lock().push(rows);
            Ok(())
        }
    }

    fn orders_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    /// A durable-write-back-enabled `CayenneTableProvider` over a fresh tempdir
    /// (sqlite metadata catalog + local Vortex data path) — no external
    /// dependency, following the recipe in `cayenne::provider::sink` tests.
    async fn new_provider(temp_dir: &tempfile::TempDir) -> CayenneTableProvider {
        new_provider_keyed(temp_dir, vec!["id".to_string()]).await
    }

    /// As [`new_provider`], with the primary key the accelerator resolves.
    async fn new_provider_keyed(
        temp_dir: &tempfile::TempDir,
        primary_key: Vec<String>,
    ) -> CayenneTableProvider {
        let ctx = SessionContext::new();
        let metadata_dir = format!("{}/metadata", temp_dir.path().to_str().expect("path"));
        let data_dir = format!("{}/data", temp_dir.path().to_str().expect("path"));
        std::fs::create_dir_all(&metadata_dir).expect("metadata dir");
        let catalog = Arc::new(
            CayenneCatalog::new(format!("sqlite://{metadata_dir}/cayenne.db")).expect("catalog"),
        ) as Arc<dyn MetadataCatalog>;
        catalog.init().await.expect("catalog init");

        let options = CreateTableOptions {
            table_name: "orders".to_string(),
            schema: orders_schema(),
            primary_key,
            on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
                "id".to_string(),
            ]))),
            base_path: data_dir,
            partition_column: None,
            vortex_config: VortexConfig::default(),
        };
        CayenneTableProviderBuilder::new(catalog, ctx.runtime_env())
            .with_durable_write_back(true)
            .create(options)
            .await
            .expect("table created")
    }

    /// Commit one row through a Cayenne transaction.
    ///
    /// A transaction commit is the only writer of `cayenne_pending_write_back`
    /// markers, so it is the only write the delivery worker can observe. A bare
    /// `insert_into` marks nothing, leaving every test below with an empty
    /// dirty set.
    async fn insert_row(provider: &CayenneTableProvider, id: i32, name: &str) {
        let batch = RecordBatch::try_new(
            orders_schema(),
            vec![
                Arc::new(Int32Array::from(vec![id])),
                Arc::new(StringArray::from(vec![Some(name)])),
            ],
        )
        .expect("batch builds");
        let stream: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            batch.schema(),
            futures::stream::iter([Ok(batch)]),
        ));

        let table_id = provider.table_id().to_string();
        let txn = CayenneTransaction::new();
        txn.register(
            table_id.clone(),
            provider.transaction_write_token().await,
            provider.clone_for_write_operations(),
        );
        let token = txn
            .take_token(&table_id)
            .expect("the registered participant's write token");
        let staged = provider
            .begin_staged_upsert_occ(token, stream, 1)
            .await
            .expect("stage the upsert");
        txn.set_staged(&table_id, staged);
        txn.commit().await.expect("commit the transaction");
    }

    /// Physically remove a row WITHOUT marking it.
    ///
    /// The counterpart to `insert_row`: the delete path records no write-back
    /// marker, so the absent-key test keeps the marker its insert produced
    /// while the row itself is gone — the "claimed key the post-claim read
    /// cannot find" case. Routing this through `insert_row` would destroy it.
    async fn delete_row(provider: &CayenneTableProvider, id: i32) {
        let ctx = SessionContext::new();
        let plan = provider
            .delete_from(&ctx.state(), vec![col("id").eq(lit(id))])
            .await
            .expect("delete plan");
        datafusion::physical_plan::collect(plan, ctx.task_ctx())
            .await
            .expect("delete executes");
    }

    fn make_worker(
        provider: CayenneTableProvider,
        deliverer: Option<Arc<dyn WriteBackDeliverer>>,
    ) -> WriteBackWorker {
        make_worker_with_federated(
            provider,
            deliverer,
            Arc::new(MemTable::try_new(orders_schema(), vec![vec![]]).expect("mem table")),
        )
    }

    /// As [`make_worker`], with the federated source the `TableProvider`
    /// fallback delivers through.
    fn make_worker_with_federated(
        provider: CayenneTableProvider,
        deliverer: Option<Arc<dyn WriteBackDeliverer>>,
        federated: Arc<dyn TableProvider>,
    ) -> WriteBackWorker {
        // Through `new`, so a fixture passes both key checks exactly as production
        // does rather than filling the fields itself and skipping them. These tests
        // drive `deliver_batch` directly and never `run`, so nothing publishes the
        // backlog gauge; the owner stays unregistered rather than mutating the
        // process-wide owner stack from a test, and `Drop` treats an unregistered
        // owner as owning nothing.
        WriteBackWorker::new(
            provider,
            Arc::new(FederatedTable::new_unchecked(federated)),
            "orders".to_string(),
            deliverer,
        )
        .expect("the fixture's key must be one the worker can deliver on")
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn present_rows_deliver_via_upsert_and_clear_their_markers() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let provider = new_provider(&temp_dir).await;
        insert_row(&provider, 1, "widget").await;

        let deliverer = FakeDeliverer::arc();
        let mut worker = make_worker(
            provider,
            Some(Arc::clone(&deliverer) as Arc<dyn WriteBackDeliverer>),
        );

        let pass = worker.deliver_batch().await.expect("pass succeeds");
        assert_eq!(pass.claimed, 1, "the one dirty marker was claimed");
        assert_eq!(
            deliverer.upserts.lock().len(),
            1,
            "the present row was delivered via deliver_upserts"
        );

        let remaining = worker
            .provider
            .list_dirty_keys(CLAIM_BATCH, None)
            .await
            .expect("list after delivery");
        assert!(remaining.is_empty(), "the marker was cleared on success");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_key_the_read_cannot_find_is_withheld_never_deleted_at_the_source() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let provider = new_provider(&temp_dir).await;
        insert_row(&provider, 2, "gadget").await;
        // Remove the row before the delivery pass claims its marker — a
        // retention prune, a compaction hole and a not-yet-visible commit all
        // look exactly like this to the post-claim read.
        delete_row(&provider, 2).await;

        let deliverer = FakeDeliverer::arc();
        let mut worker = make_worker(
            provider,
            Some(Arc::clone(&deliverer) as Arc<dyn WriteBackDeliverer>),
        );

        let pass = worker.deliver_batch().await.expect("pass succeeds");
        assert_eq!(pass.claimed, 1, "the one dirty marker was claimed");
        assert_eq!(pass.delivered, 0, "and nothing was delivered for it");
        assert!(
            deliverer.upserts.lock().is_empty(),
            "nothing is delivered for an unreadable key — and the deliverer has no \
             delete primitive at all, so nothing here can remove the source row"
        );
        assert_eq!(
            worker
                .provider
                .list_dirty_keys(CLAIM_BATCH, None)
                .await
                .expect("list after the pass")
                .len(),
            1,
            "its marker is kept: only delivery retires one"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_delivery_error_retains_the_dirty_key_marker() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let provider = new_provider(&temp_dir).await;
        insert_row(&provider, 3, "thing").await;

        let deliverer = FakeDeliverer::arc();
        deliverer.arm_failure();
        let mut worker = make_worker(
            provider,
            Some(Arc::clone(&deliverer) as Arc<dyn WriteBackDeliverer>),
        );

        worker
            .deliver_batch()
            .await
            .expect_err("an armed delivery failure surfaces as an error");

        let remaining = worker
            .provider
            .list_dirty_keys(CLAIM_BATCH, None)
            .await
            .expect("list after the failed pass");
        assert_eq!(
            remaining.len(),
            1,
            "the marker is retained so the pass can be replayed"
        );
    }

    /// A `TableProvider` whose insert reports writing `reports` rows however many
    /// it was handed — a source that accepts the statement and takes less than it
    /// was sent.
    #[derive(Debug)]
    struct ShortWriteProvider {
        schema: SchemaRef,
        reports: u64,
    }

    #[async_trait::async_trait]
    impl TableProvider for ShortWriteProvider {
        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }

        fn table_type(&self) -> datafusion::datasource::TableType {
            datafusion::datasource::TableType::Base
        }

        async fn scan(
            &self,
            _state: &dyn datafusion::catalog::Session,
            _projection: Option<&Vec<usize>>,
            _filters: &[datafusion::logical_expr::Expr],
            _limit: Option<usize>,
        ) -> datafusion::error::Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
            Err(datafusion::error::DataFusionError::NotImplemented(
                "scan".to_string(),
            ))
        }

        async fn insert_into(
            &self,
            _state: &dyn datafusion::catalog::Session,
            _input: Arc<dyn datafusion::physical_plan::ExecutionPlan>,
            _insert_op: datafusion::logical_expr::dml::InsertOp,
        ) -> datafusion::error::Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
            Ok(crate::accelerated::write::count_exec(self.reports))
        }
    }

    /// Clearing a marker is the worker's only record that the row was owed, so a
    /// source that reports taking fewer rows than it was sent must not have that
    /// record retired: the pass fails and every marker is kept.
    #[tokio::test]
    async fn a_source_that_writes_fewer_rows_than_sent_keeps_its_markers() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let provider = new_provider(&temp_dir).await;
        insert_row(&provider, 6, "widget").await;
        insert_row(&provider, 7, "gadget").await;

        let mut worker = make_worker_with_federated(
            provider,
            None,
            Arc::new(ShortWriteProvider {
                schema: orders_schema(),
                reports: 1,
            }),
        );

        worker
            .deliver_batch()
            .await
            .expect_err("a short federated write must fail the pass");

        let remaining = worker
            .provider
            .list_dirty_keys(CLAIM_BATCH, None)
            .await
            .expect("list after the short write");
        assert_eq!(
            remaining.len(),
            2,
            "both markers are kept so the rows are delivered again"
        );
    }

    /// Same arity is not the same key. The connector's `ON CONFLICT` target comes
    /// from the declared Spicepod key while markers and point scans use the key the
    /// accelerator resolved; a persisted table keeps its key across a Spicepod edit,
    /// so the two can name different single columns. Delivering then updates a row
    /// this worker never marked — or inserts a second one — so the table refuses to
    /// build.
    #[tokio::test]
    async fn a_deliverer_keyed_on_a_different_column_refuses_to_build() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let provider = new_provider(&temp_dir).await;
        let federated = Arc::new(FederatedTable::new_unchecked(Arc::new(
            MemTable::try_new(orders_schema(), vec![vec![]]).expect("mem table"),
        )));

        // The provider resolves `id`; this deliverer would upsert on `name`.
        let deliverer = Arc::new(FakeDeliverer {
            conflict_key: "name".to_string(),
            ..FakeDeliverer::default()
        });

        let error = WriteBackWorker::new(
            provider,
            federated,
            "orders".to_string(),
            Some(deliverer as Arc<dyn WriteBackDeliverer>),
        )
        .err()
        .expect("a deliverer keyed on another column must refuse to build");

        let message = error.to_string();
        for expected in ["orders", "'id'", "'name'", "second row"] {
            assert!(
                message.contains(expected),
                "the refusal must contain {expected:?}: {message}"
            );
        }
    }

    /// The second half of the single-key rule. Registration refuses the
    /// configurations it can see, over the key the Spicepod declares; this is the
    /// key the accelerator actually resolved, which can differ — the declaration
    /// may have changed since the table was created, and several `AcceleratedTable`
    /// callers never pass through registration at all. A table that cannot key a
    /// delivery must refuse to build rather than accept writes, mark them, and
    /// deliver none.
    #[tokio::test]
    async fn a_table_whose_resolved_key_cannot_be_delivered_on_refuses_to_build() {
        for (primary_key, shape) in [
            (Vec::new(), "no key at all"),
            (
                vec!["id".to_string(), "name".to_string()],
                "a composite key",
            ),
        ] {
            let temp_dir = tempfile::tempdir().expect("temp dir");
            let provider = new_provider_keyed(&temp_dir, primary_key).await;
            let federated = Arc::new(FederatedTable::new_unchecked(Arc::new(
                MemTable::try_new(orders_schema(), vec![vec![]]).expect("mem table"),
            )));

            let error = WriteBackWorker::new(
                provider,
                federated,
                "orders".to_string(),
                Some(FakeDeliverer::arc() as Arc<dyn WriteBackDeliverer>),
            )
            .err()
            .expect("a key that cannot be delivered on must refuse to build");

            let message = error.to_string();
            assert!(
                message.contains("orders") && message.contains("single-column"),
                "{shape} must be refused with a message naming the dataset and the fix: {message}"
            );
        }
    }

    #[tokio::test]
    async fn a_failed_pass_claims_the_same_page_again() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let provider = new_provider(&temp_dir).await;
        insert_row(&provider, 4, "widget").await;
        insert_row(&provider, 5, "gadget").await;

        let deliverer = FakeDeliverer::arc();
        deliverer.arm_failure();
        let mut worker = make_worker(
            provider,
            Some(Arc::clone(&deliverer) as Arc<dyn WriteBackDeliverer>),
        );

        worker
            .deliver_batch()
            .await
            .expect_err("an armed delivery failure surfaces as an error");

        // The failed pass judged nothing, so it must not have moved the claim
        // past the markers it abandoned: the retry has to see them again.
        let pass = worker.deliver_batch().await.expect("the retry succeeds");
        assert!(
            pass.claimed_from_oldest,
            "the retry resumes at the oldest marker rather than past the failed page"
        );
        assert_eq!(pass.claimed, 2, "the retry claims both abandoned markers");
        assert_eq!(pass.delivered, 2, "and delivers them");

        let remaining = worker
            .provider
            .list_dirty_keys(CLAIM_BATCH, None)
            .await
            .expect("list after the retry");
        assert!(
            remaining.is_empty(),
            "so no marker is stranded behind the cursor"
        );
    }
}
