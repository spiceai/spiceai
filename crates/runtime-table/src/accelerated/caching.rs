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

use std::fmt;
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::atomic::AtomicI64;
use std::time::{Duration, SystemTime};

use arrow::array::StringArray;
use arrow::array::{Array, ArrayRef, RecordBatch, TimestampNanosecondArray};
use arrow::compute::cast;
use arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use arrow_tools::format::SchemaDisplay;
use data_components::http::provider::HttpTableProvider;
use datafusion::common::{DataFusionError, Result as DataFusionResult, TableReference};
use datafusion::datasource::TableProvider;
use datafusion::execution::TaskContext;
use datafusion::execution::context::SessionState;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::logical_expr::{Expr, dml::InsertOp, not};
use datafusion::logical_expr::{col, lit};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, SendableRecordBatchStream,
    stream::RecordBatchStreamAdapter,
};
use datafusion::physical_plan::{Distribution, Partitioning, PlanProperties};
use datafusion::scalar::ScalarValue;
use datafusion_expr::expr::ExprListDisplay;
use futures::{StreamExt, TryStreamExt};
use std::collections::{HashMap, HashSet};
use tokio::runtime::Handle;
use tokio::sync::{Mutex, RwLock, mpsc, watch};
use tokio::task::JoinHandle;

use runtime_acceleration::acceleration::StaleIfError;
use runtime_acceleration::dataupdate::StreamingDataUpdateExecutionPlan;
use runtime_datafusion::execution_plan::TableScanParams;
use runtime_datafusion::execution_plan::schema_cast::SchemaCastScanExec;
use runtime_request_context::CacheNamespace;
use runtime_status::{ComponentStatus, RuntimeStatus};
use util::expr::combine_exprs_balanced;

mod writer;
use writer::RetainedBufferCharge;
pub use writer::{CacheSinkWriter, CacheWorkDrain, CacheWriteSender, SynchronizedCacheTarget};

/// One entry per cache key with a fetch in flight, mapping the key to the
/// [`InFlightFetch`] whose outcome ([`FetchState`]) followers wait on.
///
/// The entry plays two coordinating roles at once. It is the write-ownership
/// claim — one key, one writer, so two concurrent writers cannot each append
/// their response and leave the key holding it twice (a duplicated source row
/// is a wrong query result). It is also the single-flight rendezvous: the one
/// caller that inserts the entry (the *leader*) fetches the origin, and every
/// caller that finds an entry already present (a *follower*) replays the
/// leader's published batches instead of asking the origin again — provided
/// the leader's fetch can answer it (see [`InFlightFetch::serves`]). Claims are
/// taken and released through [`CacheKeyClaim`] / [`ClaimOutcome`] rather than
/// by touching this directly.
///
/// The key is derived from the filter expressions (`request_path`,
/// `request_query`, `request_body`) and the namespace
/// ([`compute_cache_key_from_filters_and_namespace`]), so two callers that
/// coalesce onto the same entry already share one namespace.
///
/// A synchronous lock: every critical section is one `HashMap` operation, never
/// held across an `.await`. A follower clones the [`InFlightFetch`] while
/// holding the guard, drops the guard, then awaits; [`CacheKeyClaim`]'s `Drop`
/// must be able to release without one.
pub type InFlightRevalidations = Arc<parking_lot::Mutex<HashMap<String, InFlightFetch>>>;

/// A fetch in flight for a cache key: what the leader asked the origin for, and
/// the [`watch`] channel its outcome arrives on. Followers get a clone from
/// [`CacheKeyClaim::acquire`].
#[derive(Debug, Clone)]
pub struct InFlightFetch {
    /// The row limit the leader passed to the origin; `None` is unbounded.
    limit: Option<usize>,
    /// The leader's published [`FetchState`].
    state: watch::Receiver<FetchState>,
}

impl InFlightFetch {
    /// Whether this fetch can answer a request that asked the origin for `limit`
    /// rows.
    ///
    /// The origin truncates a bounded fetch to its limit, so replaying a fetch
    /// bounded *below* the request would hand it fewer rows than the origin
    /// holds for the same filters — a wrong result, not a slower one. An
    /// unbounded fetch answers any request; a bounded one only a request bounded
    /// at or below it. Replaying *more* rows than requested is safe: `DataFusion`
    /// keeps its own `Limit` above the scan and trims the surplus.
    fn serves(&self, limit: Option<usize>) -> bool {
        match (self.limit, limit) {
            (None, _) => true,
            (Some(_), None) => false,
            (Some(fetched), Some(wanted)) => wanted <= fetched,
        }
    }
}

/// The outcome of a leader's fetch, published to followers over a [`watch`]
/// channel held in [`InFlightRevalidations`].
///
/// `RecordBatch` clones are cheap — they clone `Arc` buffer pointers, not the
/// underlying data — so followers replay the leader's already-collected batches
/// without re-scanning the accelerator and without a second origin call.
#[derive(Debug, Clone)]
enum FetchState {
    /// The leader is still fetching; no result yet.
    Pending,
    /// The leader collected these batches from the origin — none at all when
    /// the origin had no rows for the key. Followers replay them.
    Ready(Arc<Vec<RecordBatch>>, Option<Arc<RetainedBufferCharge>>),
    /// The leader could not publish a result — it failed, was cancelled, or its
    /// response was not cacheable — so followers stop waiting and fetch the
    /// origin themselves.
    Failed,
}

/// The result of trying to claim a cache key with [`CacheKeyClaim::acquire`].
pub enum ClaimOutcome {
    /// No entry existed: this caller inserted it and owns the fetch and write.
    Leader(CacheKeyClaim),
    /// An entry already existed: another caller owns the fetch and the write
    /// for this key. This caller replays that fetch when it
    /// [serves](InFlightFetch::serves) its request, and otherwise fetches for
    /// itself without writing.
    Follower(InFlightFetch),
}

/// A cache key claimed for single-flight fetching and writing, released when
/// this is dropped.
///
/// One key, one writer. Two writers for the same key each append their
/// response, leaving the key holding it twice — which queries return as
/// duplicated source rows. The same entry also makes the leader the only caller
/// that reaches the origin: [`ClaimOutcome::Follower`]s wait on the batches the
/// leader publishes through [`Self::publish_ready`].
///
/// The claim deliberately spans the whole window in which a writer's view of
/// the key can go stale, so it is taken *before* the origin is asked rather
/// than after. Replacing an entry is a delete followed by an append, and a
/// reader scanning in between sees no rows and reads a miss. By the time that
/// reader's own fetch returns, the replacement may have landed and released;
/// claiming only then would let it append beside a response it never saw.
///
/// Dropping releases the claim, so a fetch that never returns or a query
/// cancelled while waiting for write-channel capacity cannot leave a key
/// claimed for the life of the process — which would refuse every later write
/// *and* revalidation for it, making one transient failure permanent. A drop
/// before a result is published also publishes [`FetchState::Failed`], so
/// followers fall through to their own fetch instead of waiting forever.
pub struct CacheKeyClaim {
    key: String,
    in_flight: InFlightRevalidations,
    /// Publishes the fetch outcome to followers waiting on this key. `watch`
    /// retains the last value, so a follower that clones the receiver after the
    /// leader has published still observes the result.
    sender: watch::Sender<FetchState>,
    /// Set once a terminal [`FetchState`] (`Ready` or `Failed`) has been
    /// published, so `Drop` does not overwrite a real result with `Failed`.
    published: bool,
    /// Set once a queued write owns the claim; that write removes the map entry
    /// after it has landed, so dropping this must not.
    queued: bool,
    input_charge: Option<Arc<RetainedBufferCharge>>,
    proven_fresh: bool,
}

impl CacheKeyClaim {
    /// Claims `key` for fetching and writing, or returns the fetch already in
    /// flight for it. `limit` is the row limit this caller will pass to the
    /// origin; it is recorded on the entry so followers can tell whether the
    /// leader's fetch serves them ([`InFlightFetch::serves`]).
    ///
    /// The map is locked for exactly one operation: the follower branch clones
    /// the entry and drops the guard before its caller awaits, so the lock is
    /// never held across an `.await` (see [`InFlightRevalidations`]).
    #[must_use]
    pub fn acquire(
        in_flight: &InFlightRevalidations,
        key: String,
        limit: Option<usize>,
    ) -> ClaimOutcome {
        let mut guard = in_flight.lock();
        if let Some(fetch) = guard.get(&key) {
            let fetch = fetch.clone();
            drop(guard);
            return ClaimOutcome::Follower(fetch);
        }
        let (sender, state) = watch::channel(FetchState::Pending);
        guard.insert(key.clone(), InFlightFetch { limit, state });
        drop(guard);
        ClaimOutcome::Leader(Self {
            key,
            in_flight: Arc::clone(in_flight),
            sender,
            published: false,
            queued: false,
            input_charge: None,
            proven_fresh: false,
        })
    }

    #[must_use]
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Publishes the leader's collected batches — possibly none — to any
    /// followers waiting on this key. Call once the fetch has returned a result
    /// followers may replay, before any write is enqueued, so they can proceed
    /// without waiting for the write to land.
    #[cfg(test)]
    fn publish_ready(&mut self, batches: Arc<Vec<RecordBatch>>) {
        self.publish_charged(batches, None);
    }

    fn publish_charged(
        &mut self,
        batches: Arc<Vec<RecordBatch>>,
        charge: Option<Arc<RetainedBufferCharge>>,
    ) {
        self.input_charge.clone_from(&charge);
        self.sender.send_replace(FetchState::Ready(batches, charge));
        self.published = true;
    }

    /// Shares cacheable source rows without waiting for storage publication.
    /// Native empty results release followers to refetch; retaining an empty
    /// Ready value while fanout owns the claim would act as a negative cache.
    fn publish_if_cacheable(
        &mut self,
        batches: &[RecordBatch],
        charge: Option<Arc<RetainedBufferCharge>>,
        native: bool,
    ) {
        if native && batches.iter().all(|batch| batch.num_rows() == 0) {
            self.input_charge = charge;
            self.sender.send_replace(FetchState::Failed);
            self.published = true;
        } else if cache::batches_cacheable(batches) {
            self.publish_charged(Arc::new(batches.to_vec()), charge);
        }
    }

    /// Hands the claim to a write that has been queued, which removes the map
    /// entry once the write has landed. Call only after the send has succeeded:
    /// a claim given to a request that never reaches the flush is never released.
    fn into_queued(mut self) {
        self.queued = true;
    }
}

impl Drop for CacheKeyClaim {
    fn drop(&mut self) {
        // A leader that never published a result — dropped mid-fetch, cancelled,
        // or holding a non-cacheable response — must release its followers, or
        // they wait on a `Pending` that never resolves. Publish before the map
        // entry is removed so a follower that already cloned the receiver is
        // woken.
        if !self.published {
            self.sender.send_replace(FetchState::Failed);
        }
        if self.queued {
            return;
        }
        self.in_flight.lock().remove(&self.key);
    }
}

/// How long a follower waits on the leader's in-flight fetch before giving up
/// and querying the origin itself. A backstop for a leader whose fetch hangs
/// without returning or being cancelled (a cancelled leader publishes
/// [`FetchState::Failed`] on drop, waking followers at once); timing out only
/// costs the extra origin call the follower would have made anyway.
const FOLLOWER_WAIT_TIMEOUT: Duration = Duration::from_secs(30);

/// The terminal outcome a follower observes while waiting on a leader's fetch.
enum FollowerResult {
    /// The leader published batches for the follower to replay.
    Ready(Arc<Vec<RecordBatch>>, Option<Arc<RetainedBufferCharge>>),
    /// The leader published no usable result; the follower fetches for itself.
    Failed,
}

/// A cache miss served by its own origin fetch, outside the single-flight
/// coalescing on its key: a follower whose leader published no usable result
/// or did not answer in time, or a miss the in-flight fetch cannot serve
/// because it asked the origin for fewer rows ([`InFlightFetch::serves`]).
///
/// It holds no claim, so it never writes — the leader owns the write — but it
/// applies the same `caching_stale_if_error` / empty / error handling the
/// leader path applies on the arms that do not write.
struct UncoalescedFetch<'a> {
    federated: Arc<dyn TableProvider>,
    session_state: &'a SessionState,
    dataset_name: &'a str,
    filters: &'a [Expr],
    limit: Option<usize>,
    task_context: Arc<TaskContext>,
    /// The schema an empty or error stream is given.
    schema: SchemaRef,
    stale_if_error: StaleIfError,
    /// The `caching_ttl` the expired batches are measured past, so a finite
    /// `caching_stale_if_error` window is applied here the way the leader
    /// applies it.
    max_age: Duration,
    expired_batches: Option<CacheFallback>,
}

impl UncoalescedFetch<'_> {
    /// Fetches the origin and streams the outcome to the caller.
    async fn run(self) -> SendableRecordBatchStream {
        let Self {
            federated,
            session_state,
            dataset_name,
            filters,
            limit,
            task_context,
            schema,
            stale_if_error,
            max_age,
            expired_batches,
        } = self;

        let fetch_started_at = SystemTime::now();
        match CacheRefreshHelper::fetch_from_source(
            &federated,
            session_state,
            dataset_name,
            filters,
            limit,
            task_context,
        )
        .await
        {
            Ok(batches) if !batches.is_empty() => {
                let batch_schema = batches[0].schema();

                // The guard for a 429 or 5xx status reaching here on a
                // *successful* fetch; serve the expired cached response instead
                // when `caching_stale_if_error` allows it. The HTTP connector
                // refuses such a status itself, so its own failures take the
                // `Err` arm.
                if !cache::batches_cacheable(&batches)
                    && let Some(stale) = match expired_batches {
                        Some(fallback) => fallback.read().await,
                        None => None,
                    }
                {
                    let staleness = staleness_past_max_age(&stale, max_age, fetch_started_at);
                    if stale_if_error.within_error_window(staleness) {
                        tracing::warn!(
                            "Origin for dataset '{dataset_name}' answered with a transient failure, so the expired cached response is being served instead because `caching_stale_if_error` allows it."
                        );
                        let stale_schema = stale[0].schema();
                        return Box::pin(RecordBatchStreamAdapter::new(
                            stale_schema,
                            futures::stream::iter(stale.into_iter().map(Ok)),
                        ));
                    }
                    tracing::debug!(
                        "Stale entry for dataset '{dataset_name}' is {staleness:?} past the stale-if-error window ({stale_if_error}), returning the origin's transient response."
                    );
                }

                Box::pin(RecordBatchStreamAdapter::new(
                    batch_schema,
                    futures::stream::iter(batches.into_iter().map(Ok)),
                ))
            }
            Ok(_) => Box::pin(RecordBatchStreamAdapter::new(
                schema,
                futures::stream::empty(),
            )),
            Err(e) => {
                if let Some(batches) = match expired_batches {
                    Some(fallback) => fallback.read().await,
                    None => None,
                } {
                    let staleness = staleness_past_max_age(&batches, max_age, fetch_started_at);
                    if stale_if_error.within_error_window(staleness) {
                        tracing::warn!(
                            "Origin fetch for dataset '{dataset_name}' failed, so the expired cached response is being served instead because `caching_stale_if_error` allows it. Cause: {e}"
                        );
                        let stale_schema = batches[0].schema();
                        return Box::pin(RecordBatchStreamAdapter::new(
                            stale_schema,
                            futures::stream::iter(batches.into_iter().map(Ok)),
                        ));
                    }
                    tracing::debug!(
                        "Stale entry for dataset '{dataset_name}' is {staleness:?} past the stale-if-error window ({stale_if_error}), propagating the origin error."
                    );
                }

                tracing::error!("Cache miss fetch failed for dataset {dataset_name}: {e}");
                Box::pin(RecordBatchStreamAdapter::new(
                    schema,
                    futures::stream::once(async move { Err(e) }),
                ))
            }
        }
    }
}

pub const CACHE_REFRESHED_AT_COLUMN: &str = "_fetched_at";

/// How long a cached entry stays fresh when the dataset sets no `caching_ttl`.
///
/// Read by the scan that decides whether a row may be served, the sweep that
/// decides whether it may be kept, and the Spicepod parser that checks the
/// caching windows fit a `Duration`. One constant, because a sweep with a
/// shorter default than the scan would delete rows the scan still calls fresh.
pub use runtime_acceleration::acceleration::DEFAULT_CACHING_TTL;

/// The TTL a caching scan actually applies, filling in [`DEFAULT_CACHING_TTL`]
/// for a dataset that configured none.
///
/// Named rather than inlined so the eviction sweep's fallback can be asserted
/// against the same value the read path uses: a sweep with the shorter of the
/// two would delete rows the scan still calls fresh.
#[must_use]
pub fn effective_max_age(configured: Option<Duration>) -> Duration {
    configured.unwrap_or(DEFAULT_CACHING_TTL)
}

/// Reserved column name added to caching-mode accelerator storage to scope
/// cached rows by [`runtime_request_context::CacheNamespace`]. The column is
/// hidden from the user-facing schema and may not be referenced in user
/// projections, filters, or dataset definitions.
///
/// Stored value is the namespace's stable string id (e.g. `"public"`,
/// `"system"`, or `"apikey:<sha256[..16]>"`). Comparing rows by this column
/// is what enforces cross-principal isolation inside the caching
/// accelerator.
pub const CACHE_NAMESPACE_COLUMN: &str = "__spice_cache_namespace";

/// Returns true if `name` collides with a column reserved by the caching
/// accelerator. Used by dataset configuration validation so a user-defined
/// column never silently overwrites internal cache state.
#[must_use]
pub fn is_reserved_caching_column(name: &str) -> bool {
    name.eq_ignore_ascii_case(CACHE_NAMESPACE_COLUMN)
}

/// Returns a copy of `batch` with [`CACHE_NAMESPACE_COLUMN`] appended,
/// populated with `namespace_id` for every row. Idempotent: if the column is
/// already present the batch is returned unchanged, and if `storage_schema`
/// does not declare the column the batch is returned unchanged too — a
/// unit-test mock with an unextended schema opts out that way.
///
/// Called immediately before a [`CacheWriteRequest`]'s batches are handed to
/// the accelerator, so that every persisted row carries the tag the read path
/// filters on (`__spice_cache_namespace = $current_ns`).
///
/// # Errors
///
/// Returns a `DataFusionError` if the stamped batch is rejected — the tag array
/// and the batch disagree on row count, or the resulting schema is invalid.
pub fn stamp_namespace_column(
    batch: RecordBatch,
    storage_schema: &arrow::datatypes::Schema,
    namespace_id: &str,
) -> DataFusionResult<RecordBatch> {
    use arrow::datatypes::{DataType, Field, Schema};

    let schema = batch.schema();
    if storage_schema
        .column_with_name(CACHE_NAMESPACE_COLUMN)
        .is_none()
        || schema.column_with_name(CACHE_NAMESPACE_COLUMN).is_some()
    {
        return Ok(batch);
    }

    let mut fields: Vec<Field> = schema.fields().iter().map(|f| (**f).clone()).collect();
    fields.push(Field::new(CACHE_NAMESPACE_COLUMN, DataType::Utf8, false));

    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
    columns.push(Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
        namespace_id,
        batch.num_rows(),
    ))) as ArrayRef);

    let new_schema = Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()));
    RecordBatch::try_new(new_schema, columns).map_err(|e| {
        DataFusionError::Execution(format!("failed to stamp {CACHE_NAMESPACE_COLUMN}: {e}"))
    })
}

/// Returns a filter expression scoping a scan to a single namespace.
/// Pushed into the accelerator alongside user filters so cached rows
/// belonging to other principals are not visible to this request.
#[must_use]
pub fn namespace_filter_expr(namespace_id: &str) -> Expr {
    col(CACHE_NAMESPACE_COLUMN).eq(lit(namespace_id))
}

/// Extends a federated/source schema with [`CACHE_NAMESPACE_COLUMN`] so the
/// caching accelerator can store the per-namespace tag alongside cached
/// rows. This is only applied to storage; the user-facing
/// [`crate::accelerated::AcceleratedTable`] schema continues to
/// expose the original column set.
///
/// Returns an error if the source schema already defines a column whose
/// name collides with [`CACHE_NAMESPACE_COLUMN`] — that name is reserved
/// for the runtime and a collision would silently corrupt cached rows.
///
/// # Errors
///
/// Returns a `DataFusionError` if `schema` already defines a column named
/// [`CACHE_NAMESPACE_COLUMN`].
pub fn extend_schema_with_cache_namespace(
    dataset_name: &str,
    schema: &arrow::datatypes::Schema,
) -> DataFusionResult<arrow::datatypes::Schema> {
    use arrow::datatypes::Field;
    // Check case-insensitively. Arrow `column_with_name` is case-sensitive,
    // so a source field named `__SPICE_CACHE_NAMESPACE` would otherwise
    // slip past this guard, after which we would append our own lowercase
    // internal column and the schema would end up with two fields whose
    // names only differ by case — bad regardless of whether downstream
    // engines treat them as equal or distinct.
    if schema
        .fields()
        .iter()
        .any(|f| f.name().eq_ignore_ascii_case(CACHE_NAMESPACE_COLUMN))
    {
        return Err(DataFusionError::Plan(format!(
            "dataset `{dataset_name}` declares a column named `{CACHE_NAMESPACE_COLUMN}` (case-insensitive match), which is reserved by the caching accelerator for per-user cache scoping. Rename the source column to avoid this name.",
        )));
    }
    let mut fields: Vec<Field> = schema.fields().iter().map(|f| (**f).clone()).collect();
    fields.push(Field::new(
        CACHE_NAMESPACE_COLUMN,
        arrow::datatypes::DataType::Utf8,
        false,
    ));
    Ok(arrow::datatypes::Schema::new_with_metadata(
        fields,
        schema.metadata().clone(),
    ))
}

/// The request columns that together identify one cache entry — the key a
/// refresh is rebuilt from and the key eviction removes an entry by.
pub const REQUEST_KEY_COLUMNS: [&str; 3] = ["request_path", "request_query", "request_body"];

/// The request-key column that tells an HTTP GET from a POST: the HTTP
/// connector sends a POST for each body a lookup's filters name and a GET when
/// they name none.
pub const REQUEST_BODY_COLUMN: &str = "request_body";

/// Whether `filters` make the HTTP connector send an explicit-empty POST.
///
/// Its response is stored with `request_body = ''`, exactly as a GET's is, so
/// the cache cannot tell the two apart; such a read bypasses the cache and goes
/// to the source, as the unaccelerated dataset would.
#[must_use]
pub fn sends_explicit_empty_request_body(filters: &[Expr]) -> bool {
    HttpTableProvider::request_filter_values(filters, REQUEST_BODY_COLUMN).contains(&"")
}

/// The storage-only predicates that keep a lookup to the entries of the method
/// it uses: `request_body = ''` when the lookup names a request but sends no
/// body. The HTTP connector stores a GET with `request_body = ''` and a POST
/// with its body, so without it a GET lookup would match a POST cached for the
/// same path.
///
/// Only `request_body` identifies a cached request reliably: a paginated
/// response stores each page's own path and query, but every page the
/// request's body. Empty when the lookup has no predicate on a request column,
/// so filters only on other columns read across every cached entry, as an
/// unfiltered scan does.
///
/// Like the namespace predicate, they scope the accelerator read only; the
/// source still receives the user's filters, so the request is unchanged.
#[must_use]
pub fn request_identity_filters(
    filters: &[Expr],
    cache_schema: &arrow::datatypes::Schema,
) -> Vec<Expr> {
    // Any predicate on a request column makes the read a lookup — even one the
    // connector turns into no request value, such as `request_body <> 'z'`,
    // which still sends a GET. Request headers count, though they are not part
    // of the key an entry is evicted and refreshed by.
    let names_request = filters.iter().flat_map(Expr::column_refs).any(|column| {
        REQUEST_KEY_COLUMNS
            .into_iter()
            .chain(["request_headers"])
            .any(|name| column.name == name)
    });
    if names_request
        && cache_schema.column_with_name(REQUEST_BODY_COLUMN).is_some()
        && HttpTableProvider::request_filter_values(filters, REQUEST_BODY_COLUMN).is_empty()
    {
        vec![col(REQUEST_BODY_COLUMN).eq(lit(""))]
    } else {
        Vec::new()
    }
}

/// The filters that re-request a stored entry from the source. The row stores
/// a GET with `request_body = ''`, and an explicit-empty POST is never cached,
/// so that predicate is dropped: sent to the source it would replay the GET as
/// an empty POST and store the POST's response under the GET's entry.
fn source_replay_filters(filters: &[Expr]) -> Vec<Expr> {
    filters
        .iter()
        .filter(|filter| !is_empty_request_body_predicate(filter))
        .cloned()
        .collect()
}

fn is_empty_request_body_predicate(filter: &Expr) -> bool {
    let Expr::BinaryExpr(binary) = filter else {
        return false;
    };
    binary.op == datafusion::logical_expr::Operator::Eq
        && matches!(binary.left.as_ref(), Expr::Column(column) if column.name == REQUEST_BODY_COLUMN)
        && matches!(
            binary.right.as_ref(),
            Expr::Literal(
                ScalarValue::Utf8(Some(value))
                    | ScalarValue::LargeUtf8(Some(value))
                    | ScalarValue::Utf8View(Some(value)),
                _,
            ) if value.is_empty()
        )
}

/// Maximum number of concurrent refresh requests
const MAX_CONCURRENT_REFRESHES: usize = 10;

/// Channel capacity for batched cache writes. Allows buffering many concurrent requests.
/// This value controls how many cache write requests can be buffered before
/// backpressure is applied to producers.
const CACHE_WRITE_CHANNEL_CAPACITY: usize = 8_192;
/// Flush interval for batched cache writes. Writes are collected and flushed
/// periodically to reduce the overhead of individual write operations.
const CACHE_WRITE_FLUSH_INTERVAL_MS: u64 = 500;

/// One complete cache response submitted to the table's bound writer.
/// The producer must establish completeness before sink admission.
#[derive(Debug)]
pub struct CacheWriteRequest {
    /// Batches to write to the accelerator
    pub batches: Vec<RecordBatch>,
    /// Filter expressions to identify the cache key (for upsert operations)
    pub filters: Vec<Expr>,
    /// Cache key computed from filters, used to track in-flight writes
    pub cache_key: String,
    /// Whether rows for this key are already stored, so the write must remove
    /// them before appending.
    ///
    /// A key nothing holds yet is appended with no delete. That matters more
    /// than it looks: on Cayenne a `delete_from` first checkpoints the inline
    /// memtable to a file, so deleting on every write — including the
    /// overwhelming majority that cannot collide with anything — collapsed
    /// measured ingest from thousands of entries per second to about ten.
    pub replaces_existing: bool,
    /// Stable storage id of the originating namespace (see
    /// [`runtime_request_context::CacheNamespace::storage_id`]). Stamped into
    /// `__spice_cache_namespace` on every row before storage and added to the
    /// upsert filter set so concurrent writers in different namespaces never
    /// overwrite each other's rows for the same `(request_path, query, body)`
    /// key.
    pub namespace_id: Arc<str>,
}

/// Receiver half of the cache write channel
pub type CacheWriteReceiver = mpsc::Receiver<CacheWriteRequest>;

/// Creates a new cache write channel with the configured capacity.
///
/// Returns the sender (for `CachingAccelerationScanExec` to send writes) and
/// the receiver (for the consumer task to process batched writes).
#[must_use]
pub fn create_cache_write_channel() -> (CacheWriteSender, CacheWriteReceiver) {
    let (sender, receiver) = mpsc::channel(CACHE_WRITE_CHANNEL_CAPACITY);
    (CacheWriteSender::Batched(sender), receiver)
}

/// Consecutive failed flushes after which a caching accelerator is reported unhealthy.
///
/// At [`CACHE_WRITE_FLUSH_INTERVAL_MS`] this is a few seconds of a cache that is accepting
/// work and storing none of it. One failed flush can be a transient write conflict; a run of
/// them is a configuration the accelerator cannot write to.
const CACHE_WRITE_FAILURES_BEFORE_UNHEALTHY: u32 = 3;

/// The `error!` and dataset status a caching accelerator gets when it can accept writes but
/// cannot store any of them.
///
/// A write failure here is on a degrade-and-continue path that does not degrade: the flush
/// warns, the runtime keeps reporting itself healthy, and queries return no error. What the
/// operator sees next depends on what the accelerator already held - one that never stored a
/// row serves nothing and uses almost no memory, one that stored rows before the writes
/// started failing keeps serving those and never updates them - so the message states the
/// failure and what stops happening rather than guessing which case it is
/// (spiceai/spiceai#13524).
fn accelerator_unwritable_message(
    dataset: &TableReference,
    failures: u32,
    cause: &dyn fmt::Display,
) -> String {
    format!(
        "Dataset '{dataset}' failed to write to its accelerator {failures} times in a row, \
         so no new result is being cached and anything already cached will not be updated. \
         Cause: {cause}. \
         See: https://spiceai.org/docs/components/data-accelerators"
    )
}

/// Tracks whether a caching accelerator is storing what it is given, and reports the dataset
/// unhealthy once it clearly is not.
struct CacheWriteHealth {
    runtime_status: Arc<RuntimeStatus>,
    dataset: TableReference,
    consecutive_failures: u32,
    /// Whether this run of failures has been escalated, so the `error!` is logged once per
    /// run rather than once per flush.
    escalated: bool,
    /// The message this tracker last wrote to the dataset status. It re-asserts only once
    /// that has been replaced, and clears only what it set.
    reported: Option<String>,
}

impl CacheWriteHealth {
    fn new(runtime_status: Arc<RuntimeStatus>, dataset: TableReference) -> Self {
        Self {
            runtime_status,
            dataset,
            consecutive_failures: 0,
            escalated: false,
            reported: None,
        }
    }

    fn record_failure(&mut self, cause: &dyn fmt::Display) {
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
        if self.consecutive_failures < CACHE_WRITE_FAILURES_BEFORE_UNHEALTHY {
            return;
        }

        let current = self.runtime_status.get_dataset_status(&self.dataset);
        let current_message = current.as_ref().and_then(ComponentStatus::error_message);
        if current_message.is_some() && current_message == self.reported.as_deref() {
            // The status this tracker set still stands. Rewriting it on every flush would
            // churn the registry twice a second for a state that has not changed.
            return;
        }

        // Reached on the first crossing, and again whenever something else has replaced this
        // tracker's status - a periodic refresh reporting `Refreshing`, say - while the
        // accelerator is still unwritable. Without the second case the write outage would
        // disappear from the dataset's status for good the first time that happened.
        let message =
            accelerator_unwritable_message(&self.dataset, self.consecutive_failures, cause);
        if !self.escalated {
            tracing::error!("{message}");
            self.escalated = true;
        }

        if current.as_ref().is_some_and(ComponentStatus::is_error) {
            // Someone else's failure is standing. It is no less real than this one and this
            // tracker does not own it, so leave it be: overwriting it would lose their
            // diagnostic, and would then let this tracker's own recovery clear a fault that
            // is still unresolved. The `error!` above and the per-flush `warn!` carry the
            // write outage until a dataset can report both at once (spiceai/spiceai#13572).
            return;
        }

        self.runtime_status.update_dataset(
            &self.dataset,
            ComponentStatus::error_with_message(message.clone()),
        );
        self.reported = Some(message);
    }

    fn record_success(&mut self) {
        self.consecutive_failures = 0;
        self.escalated = false;
        let Some(reported) = self.reported.take() else {
            return;
        };

        // Only clear the status this tracker set. A refresh failure reported since then is a
        // separate problem and must not be masked by a cache write starting to succeed.
        // `ComponentStatus` compares by variant alone, so the message identifies the reporter
        // where the status cannot.
        if self
            .runtime_status
            .get_dataset_status(&self.dataset)
            .as_ref()
            .and_then(ComponentStatus::error_message)
            == Some(reported.as_str())
        {
            self.runtime_status
                .update_dataset(&self.dataset, ComponentStatus::Ready);
        }
        tracing::info!(
            "Dataset '{}' is writing to its accelerator again.",
            self.dataset
        );
    }
}

/// Spawns a background task that batches cache writes on interval basis.
///
/// Removes cache keys from `in_flight_revalidations` after writes complete.
/// Updates `last_updated_at` after successful writes to support `snapshots_creation_policy: on_change`.
pub fn spawn_batched_cache_write_task(
    mut rx: CacheWriteReceiver,
    accelerator: Arc<dyn TableProvider>,
    dataset: TableReference,
    accelerator_write_mutex: Arc<Mutex<()>>,
    in_flight_revalidations: InFlightRevalidations,
    last_updated_at: Arc<AtomicI64>,
    runtime_status: Arc<RuntimeStatus>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let dataset_name = dataset.to_string();
        let mut health = CacheWriteHealth::new(runtime_status, dataset);
        let mut batch_buffer: Vec<CacheWriteRequest> = Vec::new();
        let mut flush_ticker =
            tokio::time::interval(Duration::from_millis(CACHE_WRITE_FLUSH_INTERVAL_MS));
        // First tick completes immediately, skip it
        flush_ticker.tick().await;

        tracing::debug!(
            "Cache batch writer started for dataset={dataset_name}, flush_interval={CACHE_WRITE_FLUSH_INTERVAL_MS}ms"
        );

        loop {
            tokio::select! {
                biased;

                maybe_req = rx.recv() => {
                    if let Some(req) = maybe_req {
                        batch_buffer.push(req);
                    } else {
                        // Channel closed - flush remaining and exit
                        if !batch_buffer.is_empty() {
                            tracing::debug!(
                                "Cache batch writer channel closed for dataset={dataset_name}, flushing {} remaining requests",
                                batch_buffer.len()
                            );
                            flush_cache_writes(
                                &mut batch_buffer,
                                &accelerator,
                                &dataset_name,
                                &accelerator_write_mutex,
                                &in_flight_revalidations,
                                &last_updated_at,
                                &mut health,
                            ).await;
                        }
                        break;
                    }
                }

                _ = flush_ticker.tick() => {
                    // Flush on interval if there are pending writes
                    if !batch_buffer.is_empty() {
                        flush_cache_writes(
                            &mut batch_buffer,
                            &accelerator,
                            &dataset_name,
                            &accelerator_write_mutex,
                            &in_flight_revalidations,
                            &last_updated_at,
                            &mut health,
                        ).await;
                    }
                }
            }
        }

        tracing::debug!("Cache batch writer task exiting for dataset={dataset_name}");
    })
}

/// Flushes accumulated cache write requests as a single batched operation.
async fn flush_cache_writes(
    buffer: &mut Vec<CacheWriteRequest>,
    accelerator: &Arc<dyn TableProvider>,
    dataset_name: &str,
    accelerator_write_mutex: &Arc<Mutex<()>>,
    in_flight_revalidations: &InFlightRevalidations,
    last_updated_at: &Arc<AtomicI64>,
    health: &mut CacheWriteHealth,
) {
    if buffer.is_empty() {
        return;
    }

    let flush_start = std::time::Instant::now();

    let queued = buffer.len();

    // One key, one write. Two requests for the same key in a single flush would
    // share one OR'd delete and then each append their batches, leaving the key
    // holding the response twice — and a duplicated source row is a wrong query
    // result, not a housekeeping problem. Walking newest-first keeps the latest
    // write for each key; an older one is a response that key no longer holds.
    //
    // The in-flight key set makes this unreachable from the two paths that
    // enqueue today. It is enforced here as well so the flush does not depend
    // on its callers for it.
    let mut seen: HashSet<String> = HashSet::with_capacity(queued);
    let mut requests: Vec<CacheWriteRequest> = buffer
        .drain(..)
        .rev()
        .filter(|req| seen.insert(req.cache_key.clone()))
        .collect();
    requests.reverse();

    if requests.len() < queued {
        tracing::debug!(
            "Dropping {superseded} superseded cache write(s) for dataset={dataset_name}: a later write for the same key is in this flush",
            superseded = queued - requests.len()
        );
    }

    let request_count = requests.len();
    let total_rows: usize = requests
        .iter()
        .flat_map(|r| r.batches.iter())
        .map(RecordBatch::num_rows)
        .sum();

    // Every key claimed for this flush, including the superseded writes, whose
    // claim is the same string and is released with it.
    let cache_keys = seen;

    tracing::trace!(
        "Flushing {request_count} cache write requests ({total_rows} total rows) for dataset={dataset_name}"
    );

    // Separate inserts from upserts. Stamp the caching accelerator's reserved
    // columns on every batch and add a `__spice_cache_namespace = $ns_id`
    // predicate to each upsert filter set so concurrent writers from different
    // namespaces never overwrite each other's rows for the same logical key.
    // We do this here (not at the send-site) so unit-test mocks with a non-
    // extended schema can opt out by simply not declaring the columns.
    let storage_schema = accelerator.schema();
    let needs_namespace_stamp = storage_schema
        .column_with_name(CACHE_NAMESPACE_COLUMN)
        .is_some();

    let mut all_batches: Vec<RecordBatch> = Vec::new();
    let mut replace_filters: Vec<Vec<Expr>> = Vec::new();

    for req in requests {
        let ns_id: &str = &req.namespace_id;
        let batches = match req
            .batches
            .into_iter()
            .map(|b| stamp_namespace_column(b, &storage_schema, ns_id))
            .collect::<DataFusionResult<Vec<_>>>()
        {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(
                    "Failed to stamp {CACHE_NAMESPACE_COLUMN} for dataset={dataset_name}: {e}; dropping write"
                );
                continue;
            }
        };
        let mut filters = req.filters;
        // A GET entry's replace must not delete the POST entries cached for
        // the same path, so it carries the predicate its lookup reads by.
        if !filters.is_empty() {
            let identity = request_identity_filters(&filters, &storage_schema);
            filters.extend(identity);
        }
        if needs_namespace_stamp {
            filters.push(namespace_filter_expr(ns_id));
        }

        all_batches.extend(batches);

        // Only a write that replaces stored rows deletes first: a delete for a
        // key nothing holds matches nothing and is not free. Two writes racing
        // to insert the same fresh key are prevented where the miss is
        // enqueued, by the in-flight key set, rather than by deleting here.
        if req.replaces_existing && !filters.is_empty() {
            replace_filters.push(filters);
        }
    }

    let write_rows: usize = all_batches.iter().map(RecordBatch::num_rows).sum();
    let replace_count = replace_filters.len();

    let write_start = std::time::Instant::now();

    // Acquire the mutex once for the entire batch
    let lock_wait_start = std::time::Instant::now();
    let lock_guard = accelerator_write_mutex.lock().await;
    let lock_wait_ms = lock_wait_start.elapsed().as_millis();

    // A declared constraint gets no write path of its own. Replacing an entry
    // means the rows the cache key now maps to are exactly the ones the response
    // carried, so the superseded rows are deleted before the new ones are
    // appended -- a native upsert keyed on the constraint would append and
    // update in place, leaving behind any row that the response no longer
    // returns for that key.
    let result = if all_batches.is_empty() {
        Ok(())
    } else if replace_filters.is_empty() {
        CacheRefreshHelper::insert_into_accelerator(accelerator, dataset_name, all_batches).await
    } else {
        CacheRefreshHelper::batched_upsert_into_accelerator(
            accelerator,
            dataset_name,
            &replace_filters,
            all_batches,
        )
        .await
    };

    drop(lock_guard);

    let write_ms = write_start.elapsed().as_millis();
    if let Err(e) = result {
        tracing::warn!(
            "Failed to write cached responses for dataset '{dataset_name}', so those entries are not cached and the next query for them will be answered from the origin. Cause: {e}"
        );
        health.record_failure(&e);
    } else if write_rows > 0 {
        health.record_success();

        // Update last_updated_at for snapshots_creation_policy: on_change support
        super::AcceleratedTable::set_timestamp_to_now(last_updated_at);

        tracing::trace!(
            "Cache write completed for dataset={dataset_name}: {write_rows} rows across {replace_count} keys in {write_ms}ms"
        );
    }

    // Remove cache keys from in-flight tracking now that writes are persisted
    {
        let mut in_flight = in_flight_revalidations.lock();
        for key in &cache_keys {
            in_flight.remove(key);
        }
    }

    let total_ms = flush_start.elapsed().as_millis();
    tracing::debug!(
        "Cache flush completed for dataset={dataset_name}: {request_count} requests, {total_rows} rows, lock_wait={lock_wait_ms}ms, total={total_ms}ms"
    );
}

/// Get the first `fetched_at` timestamp from a batch, if present and not null.
fn get_first_fetched_at_timestamp(batch: &RecordBatch) -> Option<i64> {
    let (idx, _) = batch.schema().column_with_name(CACHE_REFRESHED_AT_COLUMN)?;
    let ts_array = batch
        .column(idx)
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()?;
    if ts_array.is_empty() || ts_array.is_null(0) {
        return None;
    }
    Some(ts_array.value(0))
}

/// The oldest `_fetched_at` value across every row of every batch, as
/// nanoseconds since the epoch, normalizing the column's stored precision
/// first — an accelerator may store it at a coarser resolution (Cayenne keeps
/// microseconds), which a bare nanosecond downcast would silently miss.
/// `None` when any batch is missing the column or any row's value is null —
/// the same fail-closed contract `check_cache_freshness` applies when scanning
/// the same column, so the two agree on how stale the worst row is.
fn oldest_fetched_at_nanos(batches: &[RecordBatch]) -> Option<i64> {
    let mut oldest: Option<i64> = None;
    for batch in batches {
        let (idx, _) = batch.schema().column_with_name(CACHE_REFRESHED_AT_COLUMN)?;
        let ns_array = as_timestamp_nanosecond_array(batch.column(idx)).ok()?;
        let ts_array = ns_array
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()?;
        for i in 0..ts_array.len() {
            if ts_array.is_null(i) {
                return None;
            }
            let ts = ts_array.value(i);
            oldest = Some(oldest.map_or(ts, |o: i64| o.min(ts)));
        }
    }
    oldest
}

/// How stale a cached entry is *past the point it went stale* at `at` — `at -
/// fetched_at - max_age`, saturating at zero — or `None` when the entry carries
/// no usable fetch time (missing/null `_fetched_at`), computed from the oldest
/// row across every batch so a single stale row in a multi-batch response
/// cannot be masked by fresher rows ahead of it.
///
/// This is the staleness `StaleIfError::within_error_window` gates on: a `For(N)`
/// window is measured from the stale point (past `caching_ttl`), not from the
/// fetch. `None` makes a finite window fail closed and leaves `Enabled`
/// unaffected, exactly the read-path decision the caller needs.
fn staleness_past_max_age(
    batches: &[RecordBatch],
    max_age: Duration,
    at: SystemTime,
) -> Option<Duration> {
    let fetched_at = oldest_fetched_at_nanos(batches)?;
    let now_nanos =
        i64::try_from(at.duration_since(SystemTime::UNIX_EPOCH).ok()?.as_nanos()).ok()?;
    let max_age_nanos = i64::try_from(max_age.as_nanos()).ok()?;
    let past = now_nanos
        .saturating_sub(fetched_at)
        .saturating_sub(max_age_nanos)
        .max(0);
    Some(Duration::from_nanos(u64::try_from(past).ok()?))
}

/// Represents the freshness state of cached data
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheFreshness {
    /// Data is within `max_age` TTL - can be served directly without refresh
    Fresh,
    /// Data is past `max_age` but within `stale_while_revalidate` - serve but trigger background refresh
    Stale,
    /// Data is past both TTLs - treat as cache miss
    Expired,
}

/// Check the freshness state of cached data based on `max_age` and `stale_while_revalidate` TTLs
///
/// - `Fresh`: Data was fetched within `max_age` duration
/// - `Stale`: Data was fetched more than `max_age` ago but within `max_age + stale_while_revalidate`
/// - `Expired`: Data was fetched more than `max_age + stale_while_revalidate` ago (or has no timestamp)
fn check_cache_freshness(
    batches: &[RecordBatch],
    max_age: Duration,
    stale_while_revalidate: Option<Duration>,
) -> DataFusionResult<CacheFreshness> {
    tracing::trace!(
        "check_cache_freshness CALLED: num_batches={}, max_age={:?}, swr={:?}",
        batches.len(),
        max_age,
        stale_while_revalidate
    );
    if batches.is_empty() {
        return Ok(CacheFreshness::Fresh); // No data means nothing to check
    }

    // Check the first batch for schema information
    let schema = batches[0].schema();
    if schema.column_with_name(CACHE_REFRESHED_AT_COLUMN).is_none() {
        // No metadata column means data was never refreshed in cache mode - treat as expired
        tracing::debug!(
            "check_cache_freshness: no {} column, returning Expired",
            CACHE_REFRESHED_AT_COLUMN
        );
        return Ok(CacheFreshness::Expired);
    }

    #[expect(clippy::cast_possible_truncation)] // Safe: nanoseconds won't exceed i64::MAX
    let now_nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?
        .as_nanos() as i64;

    // Calculate thresholds
    #[expect(clippy::cast_possible_truncation)]
    let max_age_nanos = max_age.as_nanos() as i64;
    let fresh_threshold = now_nanos - max_age_nanos;

    // Calculate expired threshold (max_age + stale_while_revalidate)
    let expired_threshold = if let Some(swr) = stale_while_revalidate {
        #[expect(clippy::cast_possible_truncation)]
        let swr_nanos = swr.as_nanos() as i64;
        now_nanos - max_age_nanos - swr_nanos
    } else {
        // If no stale_while_revalidate, stale items become expired immediately
        fresh_threshold
    };

    // Directly scan Arrow arrays for freshness (avoid DataFusion overhead)
    // Track the worst freshness status seen
    let mut worst_freshness = CacheFreshness::Fresh;

    for batch in batches {
        let col_idx = batch
            .schema()
            .index_of(CACHE_REFRESHED_AT_COLUMN)
            .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;
        let array = batch.column(col_idx);

        // Normalize timestamp to nanoseconds for comparison (if needed). Accelerators may store
        // timestamps with different precisions (e.g., Cayenne uses Microseconds).
        // We cast only this column here vs SchemaCastScanExec which casts user schema (other columns).
        let ns_array = as_timestamp_nanosecond_array(array)?;
        let ts_array = ns_array
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(format!(
                    "{CACHE_REFRESHED_AT_COLUMN} conversion to TimestampNanosecond failed"
                ))
            })?;
        for i in 0..ts_array.len() {
            if !ts_array.is_valid(i) {
                // Null value = expired, return immediately (can't get worse)
                tracing::debug!(
                    "check_cache_freshness: NULL timestamp at index {i}, returning Expired"
                );
                return Ok(CacheFreshness::Expired);
            }
            let ts = ts_array.value(i);
            if ts < expired_threshold {
                // Expired is the worst, return immediately
                return Ok(CacheFreshness::Expired);
            }
            if ts < fresh_threshold && worst_freshness == CacheFreshness::Fresh {
                // Found a stale row - update worst status but continue checking for expired
                worst_freshness = CacheFreshness::Stale;
            }
        }
    }

    Ok(worst_freshness)
}

/// Compute a cache key from filter expressions only.
///
/// Use this only when the resulting key is compared against other keys
/// computed from the *same* call site (i.e. intra-call deduplication
/// where the namespace is constant). For any cross-request keying
/// (in-flight revalidation, batched-write dedup, etc.) use
/// [`compute_cache_key_from_filters_and_namespace`] so two principals
/// running the same SQL do not collide.
fn compute_cache_key_from_filters(filters: &[Expr]) -> String {
    let mut parts: Vec<String> = filters.iter().map(ToString::to_string).collect();
    parts.sort();
    parts.join("|")
}

/// Compute an in-flight / cache-write key from filter expressions plus
/// the originating cache namespace. The namespace tag is mixed in so that
/// two principals running the same query do not share an in-flight
/// revalidation slot or a write-batch dedup slot — if alice and bob both
/// trigger SWR for the same SQL, both must actually fetch and write
/// independently into their own namespaces.
fn compute_cache_key_from_filters_and_namespace(filters: &[Expr], namespace_id: &str) -> String {
    let mut key = compute_cache_key_from_filters(filters);
    key.push_str("|__ns=");
    key.push_str(namespace_id);
    key
}

/// Convert a timestamp array to nanosecond precision, returning the original if already nanoseconds.
fn as_timestamp_nanosecond_array(array: &ArrayRef) -> DataFusionResult<ArrayRef> {
    // Fast path: if already nanoseconds, return Arc clone (no data copy)
    if array.data_type() == &DataType::Timestamp(TimeUnit::Nanosecond, None) {
        return Ok(Arc::clone(array));
    }

    // This handles Microsecond (Cayenne), Millisecond, Second precisions
    cast(array, &DataType::Timestamp(TimeUnit::Nanosecond, None)).map_err(|e| {
        DataFusionError::Execution(format!("Failed to cast timestamp to nanoseconds: {e}"))
    })
}

/// One cache entry found stale in the accelerator: the request to replay
/// against the source, and the namespace its rows belong to.
struct StaleCacheEntry {
    filters: Vec<Expr>,
    namespace: Option<String>,
}

/// What a revalidation learned about the source.
///
/// The distinction matters because a failing origin need not arrive as an
/// error: a fetch can succeed and carry a 429 or 5xx in its rows, so a caller
/// that only inspects `Result` sees "the source answered" and cannot tell that
/// it answered with a failure. That is precisely when `caching_stale_if_error`
/// is supposed to act. The HTTP connector refuses such a status itself — see
/// its `on_error_response` — so this outcome is what covers a row that reaches
/// the cache by any other route.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RevalidationOutcome {
    /// The source answered, and its rows were queued to replace the entry.
    Refreshed { rows: usize },
    /// The source answered and had nothing to cache. The origin is healthy.
    Empty,
    /// The response is usable, but its completeness cannot be established for population.
    NotPopulated,
    /// The source could not be revalidated: it answered with a transient
    /// failure after its own retries were exhausted. Nothing was written, and
    /// whatever is cached is the best answer available.
    OriginUnavailable,
}

impl RevalidationOutcome {
    /// Rows queued for write; zero unless the entry was refreshed.
    #[must_use]
    pub fn rows(self) -> usize {
        match self {
            Self::Refreshed { rows } => rows,
            Self::Empty | Self::NotPopulated | Self::OriginUnavailable => 0,
        }
    }
}

/// An expired response is read only when a failing origin needs a fallback.
enum CacheFallback {
    Loaded(Vec<RecordBatch>),
    Deferred {
        input: CachingScanInput,
        partition: usize,
        context: Arc<TaskContext>,
    },
}

impl CacheFallback {
    async fn read(self) -> Option<Vec<RecordBatch>> {
        let batches = match self {
            Self::Loaded(batches) => batches,
            Self::Deferred {
                input,
                partition,
                context,
            } => input
                .into_plan()
                .await
                .inspect_err(|error| tracing::debug!(%error, "Cache fallback planning failed"))
                .ok()?
                .execute(partition, context)
                .inspect_err(|error| tracing::debug!(%error, "Cache fallback execution failed"))
                .ok()?
                .try_collect()
                .await
                .inspect_err(|error| tracing::debug!(%error, "Cache fallback collection failed"))
                .ok()?,
        };
        let batches: Vec<RecordBatch> = batches
            .into_iter()
            .filter(|batch| batch.num_rows() > 0)
            .collect();
        (!batches.is_empty()).then_some(batches)
    }
}

fn charged_response_stream(
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    charge: Option<Arc<RetainedBufferCharge>>,
) -> SendableRecordBatchStream {
    let stream = futures::stream::iter(batches.into_iter().map(Ok)).map(move |batch| {
        let _retained = &charge;
        batch
    });
    Box::pin(RecordBatchStreamAdapter::new(schema, stream))
}

struct CacheFetch {
    batches: Vec<RecordBatch>,
    complete: bool,
    charge: Option<Arc<RetainedBufferCharge>>,
}

struct NativeCacheWrite {
    writer: CacheWriteSender,
    request: CacheWriteRequest,
    claim: CacheKeyClaim,
    children: SynchronizedChildren,
    dataset_name: String,
    input_charge: Arc<RetainedBufferCharge>,
    metadata_charge: MemoryReservation,
}

impl NativeCacheWrite {
    fn new(
        writer: CacheWriteSender,
        request: CacheWriteRequest,
        claim: CacheKeyClaim,
        children: SynchronizedChildren,
        dataset_name: String,
        input_charge: Arc<RetainedBufferCharge>,
    ) -> DataFusionResult<Self> {
        let metadata_charge = writer.reserve_work_metadata(&request, &claim)?;
        metadata_charge.try_grow(dataset_name.capacity().saturating_mul(2))?;
        Ok(Self {
            writer,
            request,
            claim,
            children,
            dataset_name,
            input_charge,
            metadata_charge,
        })
    }

    async fn run(self) -> DataFusionResult<()> {
        let batches = self.request.batches.clone();
        let filters = self.request.filters.clone();
        let namespace_id = Arc::clone(&self.request.namespace_id);

        // Admission follows registry acquisition. Initialization can therefore
        // flush every write that observed its old list, while a job waiting for
        // the registry cannot make that flush wait on itself.
        let registered = self.children.read().await;
        let mut targets = Vec::<(SynchronizedCacheTarget, CacheKeyClaim)>::new();
        for child in registered.iter() {
            if Arc::ptr_eq(&self.claim.in_flight, &child.in_flight) {
                return Err(DataFusionError::Internal(
                    "A synchronized child must not share its parent's cache claims".into(),
                ));
            }
            if (!child.writer.requires_complete_fetch()
                && batches.iter().all(|batch| batch.num_rows() == 0))
                || targets
                    .iter()
                    .any(|(target, _)| Arc::ptr_eq(&target.in_flight, &child.in_flight))
            {
                continue;
            }
            let key = compute_cache_key_from_filters_and_namespace(&filters, &namespace_id);
            self.metadata_charge.try_grow(
                key.capacity()
                    .saturating_add(key.len().saturating_mul(2))
                    .saturating_add(
                        std::mem::size_of::<(SynchronizedCacheTarget, CacheKeyClaim)>()
                            .saturating_mul(2),
                    ),
            )?;
            let claim = loop {
                match CacheKeyClaim::acquire(&child.in_flight, key.clone(), None) {
                    ClaimOutcome::Leader(claim) => break Some(claim),
                    ClaimOutcome::Follower(mut fetch) if child.writer.requires_complete_fetch() => {
                        // Ready permits response replay, not another replacement.
                        // The native writer drops the sender only after publication.
                        while fetch.state.changed().await.is_ok() {}
                    }
                    ClaimOutcome::Follower(_) => break None,
                }
            };
            if let Some(claim) = claim {
                targets.push((child.clone(), claim));
            }
        }

        // Child claims precede release of the parent claim. A newer parent fill
        // must wait behind these child replacements rather than skip or overtake them.
        self.writer
            .send_claimed_and_wait(self.request, self.claim)
            .await?;
        drop(registered);

        for (index, (child, mut claim)) in targets.into_iter().enumerate() {
            let charge = match child
                .writer
                .retain_input(&batches, Some(&self.input_charge))
            {
                Ok(charge) => charge,
                Err(error) => {
                    tracing::debug!(
                        "Declining child cache population for dataset '{}': {error}",
                        self.dataset_name
                    );
                    continue;
                }
            };
            claim.publish_if_cacheable(&batches, charge, child.writer.requires_complete_fetch());
            let request = CacheWriteRequest {
                batches: batches.clone(),
                filters: filters.clone(),
                cache_key: claim.key().to_string(),
                replaces_existing: true,
                namespace_id: Arc::clone(&namespace_id),
            };
            let result = if child.writer.requires_complete_fetch() {
                child.writer.send_claimed_and_wait(request, claim).await
            } else {
                child.writer.send_claimed(request, claim).await
            };
            if let Err(error) = result {
                tracing::warn!(
                    "Failed to propagate cached data to synchronized child {} for dataset {}: {}",
                    index,
                    self.dataset_name,
                    error
                );
            }
        }
        Ok(())
    }
}

pub struct CacheRefreshHelper;

impl CacheRefreshHelper {
    /// Refresh ALL stale rows in the cache by querying the accelerator for rows with old `fetched_at` timestamps,
    /// then re-executing the query on the federated source with the original filter parameters.
    /// This is specifically designed for HTTP connector caching mode and is used by the periodic refresh task.
    ///
    /// For single-entry refresh (e.g., SWR pattern), use `refresh_entry` instead.
    ///
    /// # Errors
    ///
    /// Returns a `DataFusionError` if the accelerator cannot be scanned for stale
    /// rows, if re-executing a query against the federated source fails, or if
    /// writing the refreshed rows back to the accelerator fails.
    #[expect(clippy::too_many_arguments)]
    pub async fn refresh_all_stale_rows(
        federated: Arc<dyn TableProvider>,
        accelerator: Arc<dyn TableProvider>,
        session_state: Arc<SessionState>,
        dataset_name: &str,
        ttl: Duration,
        accelerator_write_mutex: Arc<Mutex<()>>,
        in_flight_revalidations: InFlightRevalidations,
        cache_write_tx: CacheWriteSender,
    ) -> DataFusionResult<usize> {
        // Background refresh retains the table's configured cache pool.
        // Data fetched before this threshold is considered stale
        #[expect(clippy::cast_possible_truncation)] // Safe: nanoseconds won't exceed i64::MAX
        let stale_threshold = (SystemTime::now() - ttl)
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?
            .as_nanos() as i64;

        tracing::debug!(
            "Querying for stale rows in dataset {dataset_name} with TTL {ttl:?} (threshold: {stale_threshold})",
        );

        // Scan the accelerator with a filter for stale rows
        // WHERE fetched_at <= threshold (data is at least TTL old)
        let filters =
            vec![
                col(CACHE_REFRESHED_AT_COLUMN).lt_eq(lit(ScalarValue::TimestampNanosecond(
                    Some(stale_threshold),
                    None,
                ))),
            ];

        let plan = accelerator
            .scan(session_state.as_ref(), None, &filters, None)
            .await?;
        let (stale_batches, stale_charge) = cache_write_tx.collect_snapshot(plan).await?;

        // Extract unique entries from stale rows
        let stale_entries = Self::extract_unique_stale_entries(&stale_batches)?;

        let total_stale_rows: usize = stale_batches.iter().map(RecordBatch::num_rows).sum();
        tracing::debug!(
            "Found {total_stale_rows} stale rows ({} unique filter sets) to refresh for dataset {dataset_name}",
            stale_entries.len()
        );

        drop(stale_batches);
        drop(stale_charge);
        if stale_entries.is_empty() {
            return Ok(0);
        }

        // Create futures for all refresh operations and run them with limited concurrency.
        // Each refresh fetches from the source and then upserts into the accelerator,
        // which preserves data for other cache entries (different request paths/queries).
        let refresh_futures = stale_entries.into_iter().map(|entry| {
            let federated = Arc::clone(&federated);
            let accelerator = Arc::clone(&accelerator);
            let session_state = Arc::clone(&session_state);
            let dataset_name = dataset_name.to_string();
            let accelerator_write_mutex = Arc::clone(&accelerator_write_mutex);
            let in_flight_revalidations = Arc::clone(&in_flight_revalidations);
            let cache_write_tx = cache_write_tx.clone();
            let StaleCacheEntry {
                filters: row_filters,
                namespace,
            } = entry;

            async move {
                // This path replaces the entry it refreshes, so it opens the
                // same delete-then-append gap every other writer does and must
                // hold the key for it. Without the claim a reader scanning in
                // that gap reads a miss and appends its own copy beside this
                // one. Held until the write below has landed; dropped with this
                // future on any early return.
                let namespace_id = namespace
                    .as_deref()
                    .unwrap_or_else(|| CacheNamespace::Public.storage_id());
                let ClaimOutcome::Leader(mut claim) = CacheKeyClaim::acquire(
                    &in_flight_revalidations,
                    compute_cache_key_from_filters_and_namespace(&row_filters, namespace_id),
                    None,
                ) else {
                    tracing::debug!(
                        "Skipping stale refresh for dataset {dataset_name}: a write for this entry is already pending"
                    );
                    return Ok::<usize, datafusion::error::DataFusionError>(0);
                };

                tracing::debug!(
                    "Refreshing stale data for dataset {} with {} filters",
                    dataset_name,
                    row_filters.len()
                );

                let CacheFetch { batches, complete, charge } = Self::fetch_for_population(
                    &federated,
                    &session_state,
                    &dataset_name,
                    &source_replay_filters(&row_filters),
                    None,
                    cache_write_tx.task_context(&session_state),
                    cache_write_tx.memory_pool(),
                )
                .await?;

                // A miss for this entry that arrives while the claim is held
                // follows it; hand it these rows now rather than after the
                // write below, so it neither waits for the write nor asks the
                // origin again.
                if !cache_write_tx.requires_complete_fetch() || charge.is_some() {
                    claim.publish_if_cacheable(&batches, charge, cache_write_tx.requires_complete_fetch());
                }

                if batches.is_empty() && !cache_write_tx.requires_complete_fetch() {
                    return Ok::<usize, datafusion::error::DataFusionError>(0);
                }

                // This path overwrites the entry it refreshes, so a fetch
                // that succeeded while carrying a 429 or 5xx would replace the
                // last good response with the origin's error body and serve it
                // as a cache hit until it expires — keep what is cached, which
                // is also what `caching_stale_if_error` exists to do. The HTTP
                // connector refuses such a status itself; this covers a row
                // reaching here by any other route.
                if !cache::batches_cacheable(&batches) {
                    tracing::debug!(
                        "Background refresh for dataset '{dataset_name}' found the origin failing (transient HTTP error response); keeping what is cached"
                    );
                    return Ok(0);
                }

                let refreshed_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
                if refreshed_rows == 0 && !cache_write_tx.requires_complete_fetch() {
                    return Ok(0);
                }
                if cache_write_tx.requires_complete_fetch() {
                    if !complete {
                        return Ok(0);
                    }
                    let request = CacheWriteRequest {
                        batches,
                        filters: row_filters,
                        cache_key: claim.key().to_string(),
                        namespace_id: namespace_id.into(),
                        replaces_existing: true,
                    };
                    cache_write_tx.send_claimed_and_wait(request, claim).await?;
                    return Ok(refreshed_rows);
                }

                // Source rows arrive with the connector's columns only, so they
                // must be stamped with the caching accelerator's own before they
                // can replace what is stored. Skipping this would write rows with
                // no namespace and no expiry — the latter reading as "no deadline
                // to hold this to", which the eviction sweep removes on its next
                // pass, so a refresh would undo itself.
                let storage_schema = accelerator.schema();
                let batches = batches
                    .into_iter()
                    .map(|batch| stamp_namespace_column(batch, &storage_schema, namespace_id))
                    .collect::<DataFusionResult<Vec<_>>>()?;

                // Scope the replacement to the namespace the stale rows came
                // from, so refreshing one principal's copy does not delete
                // another's.
                let mut write_filters = row_filters.clone();
                if namespace.is_some() {
                    write_filters.push(namespace_filter_expr(namespace_id));
                }

                // Acquire the mutex to protect accelerator operations
                let lock_guard = accelerator_write_mutex.lock().await;

                // Upsert this specific cache entry - removes rows matching the filters
                // and adds the new data, preserving other cache entries.
                Self::upsert_into_accelerator(&accelerator, &dataset_name, &write_filters, batches)
                    .await?;

                drop(lock_guard); // Release the mutex

                Ok(refreshed_rows)
            }
        });

        let mut refresh_stream =
            futures::stream::iter(refresh_futures).buffer_unordered(MAX_CONCURRENT_REFRESHES);

        let mut total_refreshed: usize = 0;
        while let Some(result) = refresh_stream.next().await {
            match result {
                Ok(rows) => {
                    total_refreshed += rows;
                }
                Err(e) => {
                    tracing::warn!(
                        "Failed to refresh stale data for dataset {}: {}",
                        dataset_name,
                        e
                    );
                }
            }
        }

        Ok(total_refreshed)
    }

    /// Refreshes specific cache entry by fetching fresh data from the source.
    /// This is used for Stale-While-Revalidate (SWR) pattern where only the accessed entry
    /// should be refreshed, not all stale entries.
    ///
    /// Transfers ownership to the bound cache writer without waiting for publication.
    ///
    /// # Errors
    ///
    /// Returns a `DataFusionError` if the federated source cannot be queried for
    /// this entry, or if the refreshed rows cannot be queued for write.
    pub async fn refresh_entry(
        federated: Arc<dyn TableProvider>,
        session_state: &SessionState,
        dataset_name: &str,
        filters: &[Expr],
        namespace: CacheNamespace,
        batch_write_tx: CacheWriteSender,
        mut claim: CacheKeyClaim,
    ) -> DataFusionResult<RevalidationOutcome> {
        tracing::trace!(
            "Refreshing single cache entry for dataset {dataset_name} with {} filters",
            filters.len()
        );

        // Fetch fresh data for this specific entry
        let CacheFetch {
            batches,
            complete,
            charge,
        } = Self::fetch_for_population(
            &federated,
            session_state,
            dataset_name,
            filters,
            None,
            batch_write_tx.task_context(session_state),
            batch_write_tx.memory_pool(),
        )
        .await?;

        // A miss for this key that arrives while the claim is held follows it;
        // hand it these rows now rather than after the write is queued, so it
        // neither waits for the write nor asks the origin again.
        if !batch_write_tx.requires_complete_fetch() || charge.is_some() {
            claim.publish_if_cacheable(&batches, charge, batch_write_tx.requires_complete_fetch());
        }

        // Skip cache writes if the source response contains transient HTTP
        // errors. Returning here drops `claim`, releasing the key.
        if !cache::batches_cacheable(&batches) {
            tracing::debug!(
                "Revalidation for dataset={dataset_name} found the origin failing (transient HTTP error response); keeping what is cached"
            );
            return Ok(RevalidationOutcome::OriginUnavailable);
        }

        if batches.iter().all(|batch| batch.num_rows() == 0)
            && !batch_write_tx.requires_complete_fetch()
        {
            tracing::debug!("No cacheable data for dataset={dataset_name} (source returned empty)");
            return Ok(RevalidationOutcome::Empty);
        }
        if batch_write_tx.requires_complete_fetch() && !complete {
            return Ok(RevalidationOutcome::NotPopulated);
        }

        // The bound writer stamps the namespace and validates the replacement scope.

        let refreshed_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();

        let request = CacheWriteRequest {
            batches,
            filters: filters.to_vec(),
            cache_key: claim.key().to_string(),
            namespace_id: namespace.storage_id().into(),
            replaces_existing: true,
        };

        batch_write_tx.send_claimed(request, claim).await?;

        tracing::trace!("Queued refresh for dataset={dataset_name}, {refreshed_rows} rows");

        Ok(RevalidationOutcome::Refreshed {
            rows: refreshed_rows,
        })
    }

    /// Extract filter expressions from a row containing `request_path`, `request_query`, `request_body`
    fn extract_filters_from_row(
        batch: &RecordBatch,
        row_idx: usize,
    ) -> DataFusionResult<Vec<Expr>> {
        let schema = batch.schema();
        let mut filters = Vec::new();

        let filter_columns = REQUEST_KEY_COLUMNS;

        for column_name in filter_columns {
            if let Some((idx, _)) = schema.column_with_name(column_name) {
                let value = ScalarValue::try_from_array(batch.column(idx), row_idx)?;
                if value.is_null() {
                    filters.push(col(column_name).is_null());
                } else {
                    filters.push(col(column_name).eq(lit(value)));
                }
            }
        }

        tracing::debug!(
            "Extracted {} total filters from row (including empty values)",
            filters.len()
        );
        Ok(filters)
    }

    /// Extract the unique cache entries represented by `batches`, deduplicating
    /// rows with identical `(request_path, request_query, request_body)` and
    /// namespace.
    ///
    /// Deduplication is needed because HTTP connector JSON array responses are
    /// stored as multiple rows with identical request parameters. Without it,
    /// refreshing N rows from the same JSON array would trigger N identical HTTP
    /// requests.
    ///
    /// The namespace tag is carried beside the filters rather than folded into
    /// them, because the filters are replayed against the *source* — a connector
    /// has no `__spice_cache_namespace` column to match on — while the namespace
    /// is needed to write the refreshed rows back into the same principal's scope
    /// they came from. Deduplication keys on both, so two principals holding the
    /// same request each get their own refresh.
    fn extract_unique_stale_entries(
        batches: &[RecordBatch],
    ) -> DataFusionResult<Vec<StaleCacheEntry>> {
        let mut seen_filter_keys = std::collections::HashSet::new();
        let mut entries: Vec<StaleCacheEntry> = Vec::new();

        for batch in batches {
            let namespaces = batch.column_by_name(CACHE_NAMESPACE_COLUMN);

            for row_idx in 0..batch.num_rows() {
                let filters = Self::extract_filters_from_row(batch, row_idx)?;
                let namespace = match namespaces {
                    Some(array) => match ScalarValue::try_from_array(array, row_idx)? {
                        ScalarValue::Utf8(Some(value))
                        | ScalarValue::LargeUtf8(Some(value))
                        | ScalarValue::Utf8View(Some(value)) => Some(value),
                        _ => {
                            return Err(DataFusionError::Plan(
                                "Cached rows require a non-NULL string namespace".into(),
                            ));
                        }
                    },
                    None => None,
                };
                let cache_key = match &namespace {
                    Some(namespace) => {
                        compute_cache_key_from_filters_and_namespace(&filters, namespace)
                    }
                    None => compute_cache_key_from_filters(&filters),
                };
                if seen_filter_keys.insert(cache_key) {
                    entries.push(StaleCacheEntry { filters, namespace });
                }
            }
        }

        Ok(entries)
    }

    /// Overwrite the data in the accelerator with the provided batches
    ///
    /// Public so `runtime`'s DuckDB-accelerator tests can drive it against a real
    /// engine; `create_table_provider` lives there, and reaching back for it would
    /// put a dev-dependency on the whole runtime.
    ///
    /// # Errors
    ///
    /// Returns a `DataFusionError` if the accelerator rejects the overwrite —
    /// a schema mismatch between `batches` and the target table, or a failure
    /// executing the insert plan.
    pub async fn overwrite_accelerator(
        accelerator: Arc<dyn TableProvider>,
        dataset_name: &str,
        batches: Vec<RecordBatch>,
    ) -> DataFusionResult<()> {
        if batches.is_empty() {
            tracing::debug!(
                "overwrite_accelerator called with empty batches for dataset={dataset_name}"
            );
            return Ok(());
        }

        let ctx = util::session_state::session_context();
        let state = ctx.state();
        let schema = batches[0].schema();
        let total_rows: usize = batches
            .iter()
            .map(arrow::array::RecordBatch::num_rows)
            .sum();
        let batch_count = batches.len();

        tracing::debug!(
            "overwrite_accelerator - inserting {batch_count} batches ({total_rows} total rows) into accelerator for dataset={dataset_name}",
        );

        // Log the schema and sample data for debugging
        if let Some(first_batch) = batches.first()
            && let Some(timestamp) = get_first_fetched_at_timestamp(first_batch)
        {
            tracing::debug!(
                "overwrite_accelerator first batch has {CACHE_REFRESHED_AT_COLUMN} timestamp={timestamp}"
            );
        }

        // Create a stream from the batches
        let batch_stream = futures::stream::iter(batches.into_iter().map(Ok));
        let adapter = datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
            Arc::clone(&schema),
            batch_stream,
        );

        // Create an execution plan that produces this stream
        let streaming_plan: Arc<dyn ExecutionPlan> =
            Arc::new(StreamingDataUpdateExecutionPlan::new(Box::pin(adapter)));

        // Wrap with SchemaCastScanExec to ensure data types match the accelerator schema
        // (e.g., timestamp precision conversion from Nanosecond to Microsecond for Cayenne)
        let target_schema = accelerator.schema();
        let plan: Arc<dyn ExecutionPlan> =
            Arc::new(SchemaCastScanExec::new(streaming_plan, target_schema));

        // For caching mode, we use InsertOp::Overwrite to replace all existing data
        // because HTTP responses can contain multiple rows with the same filter values
        // (e.g., search results), which would violate primary key constraints if we used
        // InsertOp::Append. This means each query overwrites the cache, which is acceptable
        // for the caching use case.
        let insert_op = InsertOp::Overwrite;

        tracing::debug!(
            "overwrite_accelerator calling accelerator.insert_into with op={:?} for dataset={dataset_name}",
            insert_op,
        );
        let insert_plan = accelerator.insert_into(&state, plan, insert_op).await?;

        // Execute the insertion
        tracing::debug!("overwrite_accelerator executing insert plan for dataset={dataset_name}",);
        let task_ctx = ctx.task_ctx();
        let _ = datafusion::physical_plan::collect(insert_plan, task_ctx).await?;
        tracing::debug!(
            "overwrite_accelerator COMPLETED - successfully inserted {total_rows} rows into accelerator for dataset={dataset_name}",
        );
        Ok(())
    }

    /// Insert new data into the accelerator by combining with existing data and overwriting.
    /// This is used when there is no existing data in the cache for the given filters (cache miss).
    ///
    /// Note: We use read-combine-overwrite instead of `InsertOp::Append` because the `DuckDB`
    /// accelerator uses views with underlying data tables, and `DuckDB` views don't support
    /// direct INSERT operations. The `InsertOp::Append` fails with "is not an table" error.
    async fn insert_into_accelerator(
        accelerator: &Arc<dyn TableProvider>,
        dataset_name: &str,
        new_batches: Vec<RecordBatch>,
    ) -> DataFusionResult<()> {
        if new_batches.is_empty() {
            tracing::debug!(
                "insert_into_accelerator called with empty batches for dataset={dataset_name}"
            );
            return Ok(());
        }

        // Nothing is being replaced, so there is nothing to read first: append
        // the new rows and leave the rest of the table alone. Reading the whole
        // table back only to write it out again made the cost of caching one
        // response grow with everything already cached.
        Self::append_to_accelerator(accelerator, dataset_name, new_batches).await
    }

    /// Upsert data into the accelerator by removing rows matching the filters
    /// and inserting new data. Used when cached data exists but is expired.
    ///
    /// One cache entry is just the single-entry case of
    /// [`Self::batched_upsert_into_accelerator`], so it defers to it rather
    /// than keeping a second copy of the same delete-and-append and
    /// read-filter-write logic.
    async fn upsert_into_accelerator(
        accelerator: &Arc<dyn TableProvider>,
        dataset_name: &str,
        filters: &[Expr],
        new_batches: Vec<RecordBatch>,
    ) -> DataFusionResult<()> {
        Self::batched_upsert_into_accelerator(
            accelerator,
            dataset_name,
            &[filters.to_vec()],
            new_batches,
        )
        .await
    }

    /// Replaces the rows matching `filter_sets` with `new_batches` by issuing a
    /// `DELETE` against the accelerator and appending, instead of reading the
    /// whole table back, filtering it in memory and overwriting it.
    ///
    /// `DuckDB` and Cayenne — and `SQLite`, Turso and the in-memory accelerator —
    /// implement `TableProvider::delete_from`, so the engine removes the
    /// superseded rows itself and the cost of replacing one cache entry is
    /// proportional to that entry rather than to everything cached.
    ///
    /// Returns `Ok(false)`, having changed nothing, when the accelerator does
    /// not implement deletes; the caller falls back to the read-filter-write
    /// path. A provider without `delete_from` reports that while *planning*, so
    /// this cannot leave a half-applied replacement behind.
    ///
    /// The delete and the append are two statements, not one transaction, so a
    /// concurrent reader can see the entry briefly absent. That reads as a
    /// cache miss and refetches — it costs a request, and never serves a
    /// partial entry, because the append that follows carries every row. Such a
    /// reader cannot write what it fetched: the key is claimed for the whole
    /// replacement, including this gap (see [`CacheKeyClaim`]).
    ///
    /// **Deleting first is deliberate, and it is the order that fails safely.**
    /// Without a transaction one of the two statements can land alone. If the
    /// append fails after the delete, the entry is gone and the next read
    /// re-fetches it — a cache miss, which is always a correct answer. Appending
    /// first would instead leave the key holding both responses if the delete
    /// failed, and a duplicated source row is a *wrong* answer that queries go
    /// on returning until something else removes it. A cache may lose an entry;
    /// it may not invent rows.
    ///
    /// What the deletion costs is the `caching_stale_if_error` fallback for that
    /// entry: there is no longer an expired copy to serve if the origin is down
    /// when the next read arrives. The failure is logged and recorded against
    /// the dataset's health rather than passed over.
    ///
    /// # Errors
    ///
    /// Returns a `DataFusionError` if the delete fails for any reason other
    /// than being unimplemented, or if the append fails.
    async fn delete_and_append(
        accelerator: &Arc<dyn TableProvider>,
        dataset_name: &str,
        filter_sets: &[Vec<Expr>],
        new_batches: Vec<RecordBatch>,
    ) -> DataFusionResult<bool> {
        // Rows to remove: those matching ANY filter set, i.e. OR of AND-of-set.
        // An empty set would delete everything, so it disqualifies the whole
        // delete rather than being skipped.
        if filter_sets.iter().any(Vec::is_empty) {
            return Ok(false);
        }
        // Balanced rather than left-nested: this ORs one predicate per entry, and
        // a flush can carry many, which `simplify_expr` then walks recursively.
        let per_entry: Vec<Expr> = filter_sets
            .iter()
            .filter_map(|filters| combine_exprs_balanced(filters.clone(), Expr::and))
            .collect();
        let Some(delete_expr) = combine_exprs_balanced(per_entry, Expr::or) else {
            return Ok(false);
        };

        let ctx = util::session_state::session_context();
        let state = ctx.state();

        let plan = match accelerator.delete_from(&state, vec![delete_expr]).await {
            Ok(plan) => plan,
            Err(DataFusionError::NotImplemented(_)) => {
                tracing::debug!(
                    "Accelerator for dataset={dataset_name} does not support deletes; falling back to read-filter-write for cache replacement"
                );
                return Ok(false);
            }
            Err(e) => return Err(e),
        };

        let deleted = datafusion::physical_plan::collect(plan, ctx.task_ctx())
            .await?
            .first()
            .map_or(0, |batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow::array::UInt64Array>()
                    .filter(|a| !a.is_empty())
                    .map_or(0, |a| a.value(0))
            });

        tracing::debug!(
            "delete_and_append - removed {deleted} superseded rows across {} cache entries for dataset={dataset_name}",
            filter_sets.len()
        );

        Self::append_to_accelerator(accelerator, dataset_name, new_batches).await?;
        Ok(true)
    }

    /// Append data to the accelerator using native upsert.
    ///
    /// This uses `InsertOp::Append` which, when the accelerator is configured with
    /// `OnConflict::Upsert` on primary key columns, will automatically use the database's
    /// native upsert mechanism:
    /// - `DuckDB`: `INSERT INTO ... ON CONFLICT (pk_cols) DO UPDATE SET ...`
    /// - Arrow/MemTable: `filter_existing()` to remove colliding rows before insert
    ///
    /// This avoids first reading and then writing the entire table and is more efficient
    ///
    /// Public so `runtime`'s DuckDB-accelerator tests can drive it against a real
    /// engine; `create_table_provider` lives there, and reaching back for it would
    /// put a dev-dependency on the whole runtime.
    ///
    /// # Errors
    ///
    /// Returns a `DataFusionError` if the accelerator rejects the append — a
    /// schema mismatch between `batches` and the target table, or a failure
    /// executing the insert plan (including the engine's native upsert).
    pub async fn append_to_accelerator(
        accelerator: &Arc<dyn TableProvider>,
        dataset_name: &str,
        batches: Vec<RecordBatch>,
    ) -> DataFusionResult<()> {
        if batches.is_empty() {
            tracing::debug!(
                "append_to_accelerator called with empty batches for dataset={dataset_name}"
            );
            return Ok(());
        }

        let ctx = util::session_state::session_context();
        let state = ctx.state();
        let schema = batches[0].schema();
        let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        let batch_count = batches.len();

        tracing::trace!(
            "append_to_accelerator - appending {batch_count} batches ({total_rows} total rows) to accelerator for dataset={dataset_name}",
        );

        let batch_stream = futures::stream::iter(batches.into_iter().map(Ok));
        let adapter = datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
            Arc::clone(&schema),
            batch_stream,
        );

        let streaming_plan: Arc<dyn ExecutionPlan> =
            Arc::new(StreamingDataUpdateExecutionPlan::new(Box::pin(adapter)));

        // Wrap with SchemaCastScanExec to ensure data types match the accelerator schema
        // (e.g., timestamp precision conversion from Nanosecond to Microsecond for Cayenne)
        let target_schema = accelerator.schema();
        let plan: Arc<dyn ExecutionPlan> =
            Arc::new(SchemaCastScanExec::new(streaming_plan, target_schema));

        // Use InsertOp::Append - the accelerator's OnConflict::Upsert handles deduplication
        let insert_op = InsertOp::Append;

        let insert_plan = accelerator.insert_into(&state, plan, insert_op).await?;

        let task_ctx = ctx.task_ctx();
        let _ = datafusion::physical_plan::collect(insert_plan, task_ctx).await?;

        tracing::debug!(
            "append_to_accelerator COMPLETED - successfully appended {total_rows} rows to accelerator for dataset={dataset_name}"
        );

        Ok(())
    }

    /// Batched upsert: replace multiple cache entries in a single read-filter-write operation.
    pub(crate) async fn batched_upsert_into_accelerator(
        accelerator: &Arc<dyn TableProvider>,
        dataset_name: &str,
        filter_sets: &[Vec<Expr>],
        new_batches: Vec<RecordBatch>,
    ) -> DataFusionResult<()> {
        if new_batches.is_empty() {
            tracing::debug!(
                "batched_upsert_into_accelerator called with empty batches for dataset={dataset_name}"
            );
            return Ok(());
        }

        // Prefer letting the engine remove the superseded rows itself.
        if !filter_sets.is_empty()
            && Self::delete_and_append(accelerator, dataset_name, filter_sets, new_batches.clone())
                .await?
        {
            return Ok(());
        }

        let ctx = util::session_state::session_context();
        let state = ctx.state();

        tracing::trace!(
            "batched_upsert_into_accelerator - reading existing data from accelerator for dataset={dataset_name}, {} filter sets",
            filter_sets.len()
        );

        // Scan all data from the accelerator (no filters to get everything)
        let plan = accelerator.scan(&state, None, &[], None).await?;
        let task_ctx = ctx.task_ctx();
        let existing_batches = datafusion::physical_plan::collect(plan, task_ctx).await?;

        let existing_rows: usize = existing_batches.iter().map(RecordBatch::num_rows).sum();
        tracing::trace!(
            "batched_upsert_into_accelerator - found {} existing rows in accelerator for dataset={}",
            existing_rows,
            dataset_name
        );

        // If there's no existing data, just insert the new data
        if existing_batches.is_empty() || existing_rows == 0 {
            tracing::trace!(
                "batched_upsert_into_accelerator - no existing data, performing simple insert for dataset={dataset_name}"
            );
            return Self::insert_into_accelerator(accelerator, dataset_name, new_batches).await;
        }

        // Build a combined exclusion filter: keep rows that don't match ANY of the filter sets
        // NOT(filter_set_1) AND NOT(filter_set_2) AND ... AND NOT(filter_set_N)
        let exclusion_filter = Self::build_combined_exclusion_filter(filter_sets);

        tracing::trace!(
            "batched_upsert_into_accelerator - filtering out rows matching {} filter sets for dataset={dataset_name}",
            filter_sets.len()
        );

        // Filter existing data to keep only non-matching rows
        let df = ctx.read_batches(existing_batches)?;
        let filtered_df = if let Some(filter) = exclusion_filter {
            df.filter(filter)?
        } else {
            // No filters means replace everything
            tracing::debug!(
                "batched_upsert_into_accelerator - no filters provided, will replace all data for dataset={}",
                dataset_name
            );
            return Self::overwrite_accelerator(Arc::clone(accelerator), dataset_name, new_batches)
                .await;
        };

        let kept_batches = filtered_df.collect().await?;
        let kept_rows: usize = kept_batches.iter().map(RecordBatch::num_rows).sum();
        let new_rows: usize = new_batches.iter().map(RecordBatch::num_rows).sum();

        tracing::debug!(
            "batched_upsert_into_accelerator - keeping {kept_rows} rows, adding {new_rows} new rows for dataset={dataset_name}",
        );

        // Combine kept rows with new rows
        let mut combined_batches = kept_batches;
        combined_batches.extend(new_batches);

        // Overwrite the accelerator with the combined data
        Self::overwrite_accelerator(Arc::clone(accelerator), dataset_name, combined_batches).await
    }

    /// Build exclusion filter: NOT(set1) AND NOT(set2) AND ... AND NOT(setN).
    /// Keeps rows that don't match ANY filter set. Uses balanced tree (O(log n) depth).
    fn build_combined_exclusion_filter(filter_sets: &[Vec<Expr>]) -> Option<Expr> {
        let exclusions: Vec<Expr> = filter_sets
            .iter()
            .filter_map(|filters| filters.iter().cloned().reduce(Expr::and).map(not))
            .collect();

        if exclusions.is_empty() {
            return None;
        }

        combine_exprs_balanced(exclusions, Expr::and)
    }

    /// Admit fetched data through each synchronized child's own writer and claims.
    /// Each writer stamps its storage namespace and reports its own completion;
    /// parent admission does not promise publication in any child.
    async fn propagate_to_synchronized_children(
        synchronized_children: &SynchronizedChildren,
        dataset_name: &str,
        filters: &[Expr],
        batches: &[RecordBatch],
        complete: bool,
        input_charge: Option<Arc<RetainedBufferCharge>>,
        namespace_id: &str,
    ) {
        let children = synchronized_children.read().await.clone();
        if children.is_empty() {
            return;
        }

        let num_children = children.len();
        tracing::debug!(
            "Propagating {} batches to {} synchronized children for dataset={}",
            batches.len(),
            num_children,
            dataset_name
        );

        for (idx, child) in children.iter().enumerate() {
            if (child.writer.requires_complete_fetch() && !complete)
                || (!child.writer.requires_complete_fetch()
                    && batches.iter().all(|batch| batch.num_rows() == 0))
            {
                continue;
            }
            let ClaimOutcome::Leader(mut claim) = CacheKeyClaim::acquire(
                &child.in_flight,
                compute_cache_key_from_filters_and_namespace(filters, namespace_id),
                None,
            ) else {
                continue;
            };
            let charge = match child.writer.retain_input(batches, input_charge.as_ref()) {
                Ok(charge) => charge,
                Err(error) => {
                    tracing::debug!(
                        "Declining child cache population for dataset '{dataset_name}': {error}"
                    );
                    continue;
                }
            };
            claim.publish_if_cacheable(batches, charge, child.writer.requires_complete_fetch());
            let request = CacheWriteRequest {
                batches: batches.to_vec(),
                filters: filters.to_vec(),
                cache_key: claim.key().to_string(),
                namespace_id: namespace_id.into(),
                // The parent's cache miss does not establish that the child is empty.
                replaces_existing: true,
            };
            if let Err(e) = child.writer.send_claimed(request, claim).await {
                tracing::warn!(
                    "Failed to propagate cached data to synchronized child {} for dataset {}: {}",
                    idx,
                    dataset_name,
                    e
                );
            } else {
                tracing::debug!(
                    "Admitted cached data to synchronized child {} for dataset={}",
                    idx,
                    dataset_name
                );
            }
        }
    }

    /// Initialize a child accelerator from the parent's existing cached data.
    /// This is called when setting up localpod synchronization to ensure the child
    /// starts with the parent's existing cache state (e.g., from a file-mode `DuckDB`
    /// accelerator that was restored from disk or a snapshot).
    ///
    /// # Arguments
    /// * `parent_accelerator` - The parent's accelerator containing existing cached data
    /// * `child` - The child's composed accelerator, bound writer, and claim map
    /// * `dataset_name` - Name of the dataset for logging
    ///
    /// # Returns
    /// The number of rows copied.
    ///
    /// # Errors
    ///
    /// Returns a `DataFusionError` if the parent accelerator cannot be scanned,
    /// or if writing the copied rows into the child accelerator fails.
    pub async fn initialize_child_from_parent(
        parent_accelerator: &Arc<dyn TableProvider>,
        child: &SynchronizedCacheTarget,
        dataset_name: &str,
    ) -> DataFusionResult<usize> {
        let ctx = child.writer.session_context();
        let state = ctx.state();

        tracing::debug!(
            "Scanning parent accelerator for existing cached data to initialize child for dataset={}",
            dataset_name
        );

        // Scan all existing data from the parent accelerator
        let plan = parent_accelerator.scan(&state, None, &[], None).await?;
        let (batches, charge) = child.writer.collect_snapshot(plan).await?;

        let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();

        if child.writer.requires_complete_fetch() {
            Self::initialize_sink_child(child, batches, charge, dataset_name).await?;
            return Ok(total_rows);
        }

        if batches.is_empty() || total_rows == 0 {
            tracing::debug!(
                "No existing data in parent accelerator to initialize child for dataset={}",
                dataset_name
            );
            return Ok(0);
        }

        tracing::debug!(
            "Initializing child accelerator with {} rows from parent for dataset={}",
            total_rows,
            dataset_name
        );

        // Use overwrite to ensure clean state in child
        Self::overwrite_accelerator(Arc::clone(&child.accelerator), dataset_name, batches).await?;

        Ok(total_rows)
    }

    /// Replace each cached group, including child-only groups, before exposing the
    /// child to readers or registering it for concurrent parent propagation.
    async fn initialize_sink_child(
        child: &SynchronizedCacheTarget,
        batches: Vec<RecordBatch>,
        _snapshot_charge: Option<Arc<RetainedBufferCharge>>,
        dataset_name: &str,
    ) -> DataFusionResult<()> {
        let ctx = child.writer.session_context();
        let old_plan = child
            .accelerator
            .scan(&ctx.state(), None, &[], None)
            .await?;
        let (old_batches, old_charge) = child.writer.collect_snapshot(old_plan).await?;
        let mut all_batches = batches.clone();
        all_batches.extend(old_batches);
        let entries = Self::extract_unique_stale_entries(&all_batches)?;
        drop(all_batches);
        drop(old_charge);
        for entry in entries {
            if entry.filters.is_empty() {
                return Err(DataFusionError::Plan(format!(
                    "Cannot initialize cache for dataset '{dataset_name}' without request grouping columns"
                )));
            }
            let namespace_id = entry
                .namespace
                .as_deref()
                .unwrap_or_else(|| CacheNamespace::Public.storage_id());
            let ClaimOutcome::Leader(mut claim) = CacheKeyClaim::acquire(
                &child.in_flight,
                compute_cache_key_from_filters_and_namespace(&entry.filters, namespace_id),
                None,
            ) else {
                return Err(DataFusionError::Execution(format!(
                    "Cannot initialize cache for dataset '{dataset_name}' while a group is being written"
                )));
            };
            let matching = if batches.iter().all(|batch| batch.num_rows() == 0) {
                Vec::new()
            } else {
                let mut filters = entry.filters.clone();
                if batches[0]
                    .schema()
                    .column_with_name(CACHE_NAMESPACE_COLUMN)
                    .is_some()
                {
                    filters.push(namespace_filter_expr(namespace_id));
                }
                let predicate = combine_exprs_balanced(filters, Expr::and).ok_or_else(|| {
                    DataFusionError::Plan("A cached group requires a replacement predicate".into())
                })?;
                ctx.read_batches(batches.clone())?
                    .filter(predicate)?
                    .collect()
                    .await?
            };
            claim.input_charge = child.writer.reserve_input(&matching)?;
            let request = CacheWriteRequest {
                batches: matching,
                filters: entry.filters,
                cache_key: claim.key().to_string(),
                namespace_id: namespace_id.into(),
                replaces_existing: true,
            };
            child.writer.send_claimed_and_wait(request, claim).await?;
        }
        Ok(())
    }

    /// Whether the accelerator holds no row for `request` in `namespace_id`.
    async fn holds_no_rows(
        accelerator: &Arc<dyn TableProvider>,
        session_state: &SessionState,
        request: &[Expr],
        namespace_id: &str,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<bool> {
        let mut filters = request.to_vec();
        if accelerator
            .schema()
            .column_with_name(CACHE_NAMESPACE_COLUMN)
            .is_some()
        {
            filters.push(namespace_filter_expr(namespace_id));
        }
        let plan = TableScanParams::new(session_state, None, &filters, Some(1))
            .scan_and_optimize(accelerator.as_ref(), &filters)
            .await?;
        let mut stream = plan.execute(0, context)?;
        while let Some(batch) = stream.try_next().await? {
            if batch.num_rows() > 0 {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Fetch data from federated source for given filters
    async fn fetch_from_source(
        federated: &Arc<dyn TableProvider>,
        session_state: &SessionState,
        dataset_name: &str,
        filters: &[Expr],
        limit: Option<usize>,
        task_context: Arc<TaskContext>,
    ) -> DataFusionResult<Vec<RecordBatch>> {
        Self::fetch_for_population(
            federated,
            session_state,
            dataset_name,
            filters,
            limit,
            task_context,
            None,
        )
        .await
        .map(|fetch| fetch.batches)
    }

    async fn fetch_for_population(
        federated: &Arc<dyn TableProvider>,
        session_state: &SessionState,
        dataset_name: &str,
        filters: &[Expr],
        limit: Option<usize>,
        task_context: Arc<TaskContext>,
        memory_pool: Option<&Arc<dyn MemoryPool>>,
    ) -> DataFusionResult<CacheFetch> {
        tracing::debug!(
            "Fetching from source for dataset {dataset_name} with {} filters, limit={limit:?}",
            filters.len()
        );
        for (i, filter) in filters.iter().enumerate() {
            tracing::debug!("Source fetch filter {i}: {}", filter.human_display());
        }

        // Query source with same filters/limit but all columns
        tracing::debug!("About to scan federated source for dataset={dataset_name}");
        let plan = federated.scan(session_state, None, filters, limit).await?;
        tracing::debug!(
            "Federated source SCAN successful for dataset={dataset_name}, plan has {} partitions",
            plan.properties().output_partitioning().partition_count()
        );
        let (plan, completion) = writer::prepare_source_fetch(plan, filters, limit)?;
        let mut reservation =
            memory_pool.map(|pool| MemoryConsumer::new("cache source response").register(pool));
        let mut stream = datafusion::physical_plan::execute_stream(plan, task_context)?;
        let mut all_batches = Vec::new();
        while let Some(batch) = stream.try_next().await? {
            if let Some(charge) = &reservation
                && let Err(error) = writer::reserve_batch(charge, &batch)
            {
                // The query still receives the response, but the cache must not
                // retain it or publish uncharged copies to followers.
                tracing::debug!("Declining cache population for dataset '{dataset_name}': {error}");
                reservation = None;
            }
            all_batches.push(batch);
        }
        let charge = reservation
            .zip(memory_pool)
            .and_then(|(reservation, pool)| {
                RetainedBufferCharge::new(pool, reservation)
                    .inspect_err(|error| {
                        tracing::debug!(
                            "Declining cache response sharing for dataset '{dataset_name}': {error}"
                        );
                    })
                    .ok()
            });
        let complete = completion.as_ref().is_some_and(
            data_components::http::provider::HttpFetchCompletion::is_complete_single_request,
        ) && (memory_pool.is_none() || charge.is_some());

        tracing::debug!(
            "Federated source returned {} batches for dataset={}",
            all_batches.len(),
            dataset_name
        );

        Ok(CacheFetch {
            batches: all_batches,
            complete,
            charge,
        })
    }

    /// Handle a cache miss by fetching from source and returning a stream.
    /// Returns a `SendableRecordBatchStream` containing the fetched data, empty stream, or error stream.
    ///
    /// # Arguments
    /// * `is_expired` - If `true`, data exists in the cache but is expired, so we use upsert.
    ///   If `false`, no data exists in the cache, so we use insert (append).
    /// * `response_filtered` - The cache read also filtered response columns. An empty
    ///   response then stores nothing, since it cannot name the request's complete key.
    /// * `stale_if_error` - `Disabled` never serves stale; `Enabled` serves it with no bound;
    ///   `For(duration)` serves it only while its measured staleness is within `duration` of
    ///   going stale, and propagates the origin's failure once past that window.
    /// * `expired_batches` - The expired cached data to serve if `stale_if_error` allows it and
    ///   the source returns an error.
    /// * `synchronized_children` - Child accelerators that should also receive the cached data.
    /// * `batch_write_tx` - The table-generation cache writer.
    /// * `in_flight_revalidations` - The keys a write is already pending for. A
    ///   claim on this key is taken *before* the source is asked and held until
    ///   the write lands, so a reader whose observation of the cache is made
    ///   stale by a concurrent replacement cannot append beside it. See
    ///   [`CacheKeyClaim`].
    #[expect(clippy::too_many_arguments)]
    async fn handle_cache_miss(
        federated: Arc<dyn TableProvider>,
        session_state: &SessionState,
        dataset_name: &str,
        filters: &[Expr],
        limit: Option<usize>,
        fallback_schema: SchemaRef,
        is_expired: bool,
        response_filtered: bool,
        stale_if_error: StaleIfError,
        max_age: Duration,
        expired_batches: Option<CacheFallback>,
        io_runtime: &Handle,
        synchronized_children: SynchronizedChildren,
        batch_write_tx: CacheWriteSender,
        namespace: CacheNamespace,
        in_flight_revalidations: InFlightRevalidations,
    ) -> SendableRecordBatchStream {
        // Claimed before the origin is asked, not after: see [`CacheKeyClaim`]
        // for why the window this covers has to include the fetch. A caller that
        // finds a fetch already in flight for this key becomes a follower and
        // replays the leader's batches instead of asking the origin again —
        // unless that fetch asked the origin for fewer rows than this caller
        // needs, in which case it fetches for itself and, holding no claim,
        // does not write.
        let mut claim = match CacheKeyClaim::acquire(
            &in_flight_revalidations,
            compute_cache_key_from_filters_and_namespace(filters, namespace.storage_id()),
            limit,
        ) {
            ClaimOutcome::Leader(claim) => claim,
            ClaimOutcome::Follower(in_flight) => {
                let own_fetch = UncoalescedFetch {
                    federated,
                    session_state,
                    dataset_name,
                    filters,
                    limit,
                    task_context: batch_write_tx.task_context(session_state),
                    schema: fallback_schema,
                    stale_if_error,
                    max_age,
                    expired_batches,
                };
                if in_flight.serves(limit) {
                    return Self::follow_cache_miss(in_flight.state, own_fetch).await;
                }
                tracing::debug!(
                    "Cache miss for dataset {dataset_name} found an in-flight fetch for the same key bounded at {fetched:?} rows, below this request's limit {limit:?}; fetching for itself",
                    fetched = in_flight.limit
                );
                drop(in_flight);
                return own_fetch.run().await;
            }
        };

        if !is_expired {
            batch_write_tx.confirm_empty_scan(&mut claim);
        }

        // Capture time before the origin fetch while leaving the cached rows
        // unread until a failed fetch actually needs them.
        let fetch_started_at = SystemTime::now();

        match Self::fetch_for_population(
            &federated,
            session_state,
            dataset_name,
            filters,
            limit,
            batch_write_tx.task_context(session_state),
            batch_write_tx.memory_pool(),
        )
        .await
        {
            Ok(CacheFetch {
                batches,
                complete,
                charge,
            }) => {
                let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
                tracing::debug!(
                    "Fetched {} batches ({} total rows) from source for dataset {}",
                    batches.len(),
                    total_rows,
                    dataset_name
                );

                let batch_schema = batches
                    .first()
                    .map_or_else(|| Arc::clone(&fallback_schema), RecordBatch::schema);
                tracing::trace!("Fetched batch schema:\n{}", SchemaDisplay(&batch_schema));

                // Skip cache writes if the source response contains transient HTTP
                // errors.
                let batches_cacheable = cache::batches_cacheable(&batches);

                // A failing origin usually takes the `Err` arm below: the HTTP
                // connector refuses a 429 or 5xx that outlives its retries
                // whatever `on_error_response` says. This arm is the guard for
                // such a row reaching here by some other route, because an
                // operator who asked for
                // `caching_stale_if_error` would otherwise be served the
                // origin's error body instead of the cached response.
                if !batches_cacheable
                    && let Some(stale) = match expired_batches {
                        Some(fallback) => fallback.read().await,
                        None => None,
                    }
                {
                    let staleness = staleness_past_max_age(&stale, max_age, fetch_started_at);
                    if stale_if_error.within_error_window(staleness) {
                        tracing::warn!(
                            "Origin for dataset '{dataset_name}' answered with a transient failure, so the expired cached response is being served instead because `caching_stale_if_error` allows it."
                        );
                        let batch_schema = stale[0].schema();
                        let batch_stream = futures::stream::iter(stale.into_iter().map(Ok));
                        return Box::pin(RecordBatchStreamAdapter::new(batch_schema, batch_stream));
                    }
                    // Past the `caching_stale_if_error` window, or its age is
                    // unknown and the window is finite (fail closed): hand the
                    // caller the origin's transient response rather than a copy
                    // too stale to promise.
                    tracing::debug!(
                        "Stale entry for dataset '{dataset_name}' is {staleness:?} past the stale-if-error window ({stale_if_error}), returning the origin's transient response."
                    );
                }

                // Each arm that does not enqueue drops the claim, which releases
                // the key for the next writer and publishes `Failed` so any
                // follower waiting on it falls through to its own fetch.
                if batches_cacheable {
                    // Share the collected batches with any followers before the
                    // write is enqueued, so they replay these rows without a
                    // second origin call or a re-scan of the accelerator.
                    // `RecordBatch::clone` is cheap: it clones Arc pointers,
                    // not the underlying data.
                    if !batch_write_tx.requires_complete_fetch() || charge.is_some() {
                        claim.publish_if_cacheable(
                            &batches,
                            charge.clone(),
                            batch_write_tx.requires_complete_fetch(),
                        );
                    }

                    let native = batch_write_tx.requires_complete_fetch();
                    if (native && complete && (total_rows > 0 || !response_filtered))
                        || (!native && total_rows > 0)
                    {
                        let write_request = CacheWriteRequest {
                            batches: batches.clone(),
                            filters: filters.to_vec(),
                            cache_key: claim.key().to_string(),
                            namespace_id: namespace.storage_id().into(),
                            replaces_existing: is_expired,
                        };
                        if native {
                            if let Some(input_charge) = &charge {
                                let result = NativeCacheWrite::new(
                                    batch_write_tx.clone(),
                                    write_request,
                                    claim,
                                    Arc::clone(&synchronized_children),
                                    dataset_name.to_string(),
                                    Arc::clone(input_charge),
                                ).and_then(|job| {
                                    let dataset = dataset_name.to_string();
                                    batch_write_tx.spawn_owned(io_runtime, async move {
                                        let result = job.run().await;
                                        if let Err(error) = &result {
                                            tracing::warn!(
                                                "Failed to complete cache population for dataset '{dataset}', so later queries may need to fetch the response again. Cause: {error}"
                                            );
                                        }
                                        result
                                    })
                                });
                                if let Err(error) = result {
                                    tracing::debug!(
                                        "Declining cache population for dataset '{dataset_name}': {error}"
                                    );
                                }
                            }
                        } else if let Err(error) =
                            batch_write_tx.send_claimed(write_request, claim).await
                        {
                            tracing::warn!(
                                "Failed to admit a cache write for dataset '{dataset_name}', so the fetched response will be served without caching it. Cause: {error}"
                            );
                        }
                    }

                    // Batched parents retain their independent background fanout.
                    if !native && total_rows > 0 {
                        let dataset = dataset_name.to_string();
                        let filters = filters.to_vec();
                        let batches = batches.clone();
                        let charge = charge.clone();
                        io_runtime.spawn(async move {
                            Self::propagate_to_synchronized_children(
                                &synchronized_children,
                                &dataset,
                                &filters,
                                &batches,
                                complete,
                                charge,
                                namespace.storage_id(),
                            )
                            .await;
                        });
                    }
                } else {
                    tracing::debug!(
                        "Fetch returned transient HTTP error responses, skipping cache write for dataset={dataset_name}"
                    );
                }

                // Return all source rows, including non-cacheable responses.
                charged_response_stream(batch_schema, batches, charge)
            }
            Err(e) => {
                // Check if we should serve stale (expired) data on error
                if let Some(batches) = match expired_batches {
                    Some(fallback) => fallback.read().await,
                    None => None,
                } {
                    let staleness = staleness_past_max_age(&batches, max_age, fetch_started_at);
                    if stale_if_error.within_error_window(staleness) {
                        tracing::warn!(
                            "Origin fetch for dataset '{dataset_name}' failed, so the expired cached response is being served instead because `caching_stale_if_error` allows it. Cause: {e}"
                        );
                        let batch_schema = batches[0].schema();
                        let batch_stream = futures::stream::iter(batches.into_iter().map(Ok));
                        let adapter = RecordBatchStreamAdapter::new(batch_schema, batch_stream);
                        return Box::pin(adapter);
                    }
                    // Outside the finite window, or its age is unknown (fail
                    // closed): fall through and propagate the origin error.
                    tracing::debug!(
                        "Stale entry for dataset '{dataset_name}' is {staleness:?} past the stale-if-error window ({stale_if_error}), propagating the origin error."
                    );
                }

                tracing::error!(
                    "Cache miss fetch failed for dataset {}: {}",
                    dataset_name,
                    e
                );
                let error_stream = RecordBatchStreamAdapter::new(
                    fallback_schema,
                    futures::stream::once(async move { Err(e) }),
                );
                Box::pin(error_stream)
            }
        }
    }

    /// Serve a cache miss that coalesced onto a fetch already in flight for the
    /// same key: wait for the leader's result and replay its batches.
    ///
    /// On [`FetchState::Ready`] the leader's already-collected batches are
    /// streamed to the caller with no re-scan of the accelerator and no second
    /// origin call, and the follower never writes — the leader owns the write.
    /// On [`FetchState::Failed`] (the leader failed, was cancelled, or its
    /// response was not cacheable) or a wait that exceeds
    /// [`FOLLOWER_WAIT_TIMEOUT`], the follower runs `own_fetch` instead,
    /// degrading to the behaviour it would have had without coalescing.
    async fn follow_cache_miss(
        mut receiver: watch::Receiver<FetchState>,
        own_fetch: UncoalescedFetch<'_>,
    ) -> SendableRecordBatchStream {
        let dataset_name = own_fetch.dataset_name;
        tracing::debug!(
            "Cache miss for dataset {dataset_name} is coalescing onto an in-flight fetch for the same key"
        );

        let result = tokio::time::timeout(
            FOLLOWER_WAIT_TIMEOUT,
            Self::await_fetch_state(&mut receiver),
        )
        .await;
        // No source fallback may keep the leader's watch value alive, including
        // a Ready value published after this follower timed out.
        drop(receiver);
        match result {
            Ok(FollowerResult::Ready(batches, charge)) => {
                let admitted = match &charge {
                    Some(charge) => charge.retain_for_pool(own_fetch.task_context.memory_pool()),
                    None => RetainedBufferCharge::for_batches(
                        own_fetch.task_context.memory_pool(),
                        &batches,
                    ),
                };
                let admitted = match admitted {
                    Ok(admitted) => admitted,
                    Err(error) => {
                        tracing::debug!(
                            "Declining shared cache response for dataset '{dataset_name}': {error}"
                        );
                        drop(batches);
                        drop(charge);
                        return own_fetch.run().await;
                    }
                };
                if batches.is_empty() {
                    return Box::pin(RecordBatchStreamAdapter::new(
                        own_fetch.schema,
                        futures::stream::empty(),
                    ));
                }
                let batch_schema = batches[0].schema();
                // `RecordBatch::clone` clones Arc buffer pointers, not data.
                let replay: Vec<RecordBatch> = batches.iter().cloned().collect();
                tracing::debug!(
                    "Cache miss for dataset {dataset_name} replayed {} shared batch(es) from the in-flight fetch without a second origin call",
                    replay.len()
                );
                charged_response_stream(batch_schema, replay, Some(admitted))
            }
            Ok(FollowerResult::Failed) => {
                tracing::debug!(
                    "The in-flight fetch for dataset {dataset_name} published no result; querying the origin directly"
                );
                own_fetch.run().await
            }
            Err(_elapsed) => {
                tracing::debug!(
                    "Cache miss for dataset {dataset_name} waited longer than {timeout}s on an in-flight fetch for the same key; querying the origin directly",
                    timeout = FOLLOWER_WAIT_TIMEOUT.as_secs()
                );
                own_fetch.run().await
            }
        }
    }

    /// Waits until the leader publishes a terminal [`FetchState`].
    ///
    /// The current value is inspected first, so a follower that clones the
    /// receiver after the leader has already published observes the result
    /// immediately. The `watch::Ref` is dropped before every `.await`, so no
    /// internal lock is held across a suspension point.
    async fn await_fetch_state(receiver: &mut watch::Receiver<FetchState>) -> FollowerResult {
        loop {
            {
                let state = receiver.borrow_and_update();
                match &*state {
                    FetchState::Ready(batches, charge) => {
                        return FollowerResult::Ready(Arc::clone(batches), charge.clone());
                    }
                    FetchState::Failed => return FollowerResult::Failed,
                    FetchState::Pending => {}
                }
            }

            if receiver.changed().await.is_err() {
                // The sender dropped without a terminal state. `CacheKeyClaim`'s
                // `Drop` publishes `Failed` before the sender goes away, so this
                // is a belt-and-braces fall-through rather than an expected path.
                return FollowerResult::Failed;
            }
        }
    }

    /// Handle a cache hit by returning cached data and optionally triggering background refresh.
    /// Returns a `SendableRecordBatchStream` containing the cached data.
    ///
    /// Cache behavior based on freshness:
    /// - `Fresh`: Return cached data immediately, no refresh needed
    /// - `Stale`: Return cached data immediately, trigger background refresh (if not already in-flight)
    /// - `Expired`: This should not be called for expired data (handled as cache miss)
    #[expect(clippy::too_many_arguments)]
    fn handle_cache_hit(
        cached_batches: Vec<RecordBatch>,
        federated: &Arc<dyn TableProvider>,
        session_state: &Arc<SessionState>,
        dataset_name: &str,
        max_age: Option<Duration>,
        stale_while_revalidate: Option<Duration>,
        io_runtime: &Handle,
        schema: SchemaRef,
        filters: &[Expr],
        in_flight_revalidations: &InFlightRevalidations,
        batch_write_tx: &CacheWriteSender,
        namespace: CacheNamespace,
    ) -> SendableRecordBatchStream {
        let total_cached_rows: usize = cached_batches.iter().map(RecordBatch::num_rows).sum();

        tracing::debug!(
            dataset = %dataset_name,
            num_batches = cached_batches.len(),
            total_rows = total_cached_rows,
            "CACHE HIT - accelerator returned {} rows in {} batches",
            total_cached_rows,
            cached_batches.len()
        );

        // Check freshness and trigger background refresh if stale
        if let Some(max_age) = max_age {
            let freshness = check_cache_freshness(&cached_batches, max_age, stale_while_revalidate)
                .unwrap_or(CacheFreshness::Expired);

            match freshness {
                CacheFreshness::Fresh => {
                    tracing::debug!(
                        "Data is fresh for dataset={dataset_name}, no background refresh needed"
                    );
                }
                CacheFreshness::Stale => {
                    // One revalidation per key: the claim is held for the
                    // whole refresh and released by the write that lands it. A
                    // follower here means another caller already owns the
                    // revalidation, so this path simply skips.
                    match CacheKeyClaim::acquire(
                        in_flight_revalidations,
                        compute_cache_key_from_filters_and_namespace(
                            filters,
                            namespace.storage_id(),
                        ),
                        None,
                    ) {
                        ClaimOutcome::Leader(claim) => {
                            tracing::debug!(
                                "Data is stale for dataset={dataset_name}, triggering background refresh"
                            );

                            // Log current fetched_at for debugging
                            if let Some(timestamp) =
                                get_first_fetched_at_timestamp(&cached_batches[0])
                            {
                                tracing::debug!(
                                    "Current stale data has {CACHE_REFRESHED_AT_COLUMN} timestamp={timestamp}"
                                );
                            }

                            let federated_clone = Arc::clone(federated);
                            let session_state_clone = Arc::clone(session_state);
                            let dataset_name_clone = dataset_name.to_string();
                            let filters_for_refresh: Vec<Expr> = filters.to_vec();
                            let batch_write_tx_clone = batch_write_tx.clone();
                            let namespace_clone = namespace;

                            if let Err(error) = batch_write_tx.spawn_owned(io_runtime, async move {
                            tracing::debug!(
                                "SWR: Background refresh for single entry started for dataset={dataset_name_clone}"
                            );
                            let result = Self::refresh_entry(
                                federated_clone,
                                &session_state_clone,
                                &dataset_name_clone,
                                &filters_for_refresh,
                                namespace_clone,
                                batch_write_tx_clone,
                                claim,
                            )
                            .await;

                            match result {
                                Ok(RevalidationOutcome::OriginUnavailable) => {
                                    // Not an error to the caller: the entry is
                                    // still inside its stale-while-revalidate
                                    // window and keeps being served. Said out
                                    // loud because "refreshed 0 rows" would
                                    // read as an origin with nothing to give.
                                    tracing::warn!(
                                        "Background revalidation for dataset '{dataset_name_clone}' could not reach a healthy origin, so the cached response is being served past its `caching_ttl` until the origin recovers or the entry falls out of its `caching_stale_while_revalidate_ttl` window."
                                    );
                                    Ok(())
                                }
                                Ok(outcome) => {
                                    tracing::debug!("Background refresh task completed for dataset={dataset_name_clone}, refreshed {rows} rows", rows = outcome.rows());
                                    Ok(())
                                }
                                Err(e) => {
                                    // The claim was dropped with the failed
                                    // refresh, so the key is already released.
                                    tracing::error!(
                                        "Background refresh task failed for dataset={dataset_name_clone}: {e}"
                                    );
                                    Err(e)
                                }
                            }
                        }) {
                                tracing::debug!("Declining cache revalidation for dataset '{dataset_name}': {error}");
                            }
                        }
                        ClaimOutcome::Follower(_) => {
                            tracing::debug!(
                                "Skipping background refresh for dataset={dataset_name} because should_revalidate=false (revalidation already in progress for this cache key)"
                            );
                        }
                    }
                }
                CacheFreshness::Expired => {
                    // This shouldn't happen as expired data should be handled as cache miss
                    tracing::warn!(
                        "Unexpected expired data in handle_cache_hit for dataset={dataset_name}"
                    );
                }
            }
        } else {
            tracing::debug!(
                "No caching_ttl configured for dataset={dataset_name}, serving cached data without refresh check"
            );
        }

        // Return the cached data
        let batch_stream = futures::stream::iter(cached_batches.into_iter().map(Ok));
        let adapter = RecordBatchStreamAdapter::new(schema, batch_stream);
        Box::pin(adapter)
    }
}

/// Type alias for synchronized child accelerators
pub type SynchronizedChildren = Arc<RwLock<Vec<SynchronizedCacheTarget>>>;

/// Shared across every `CachingAccelerationScanExec`: the filters passed into `scan()` are
/// arbitrary caller `Expr`s, so full default features are kept rather than a stripped-down
/// set, but the registry itself never varies by dataset or query, so it's built once for the
/// process instead of once per exec.
pub(crate) static SHARED_SESSION_STATE: LazyLock<Arc<SessionState>> = LazyLock::new(|| {
    Arc::new(
        SessionStateBuilder::new()
            .with_config(util::session_state::session_config())
            .with_default_features()
            .build(),
    )
});

/// Whether a filtered cache read must ask the origin before it may serve stored rows.
pub(super) fn uses_source_first(
    filters: &[Expr],
    max_age: Option<Duration>,
    stale_while_revalidate: Option<Duration>,
    stale_if_error: StaleIfError,
) -> bool {
    !filters.is_empty()
        && effective_max_age(max_age).is_zero()
        && stale_while_revalidate.unwrap_or_default().is_zero()
        && stale_if_error.serves_stale_on_error()
}

/// The accelerator read input. A deferred input owns query-specific scan arguments,
/// not a storage snapshot; its read view is selected only if the origin fails.
#[derive(Clone)]
pub(super) enum CachingScanInput {
    Planned(Arc<dyn ExecutionPlan>),
    Deferred {
        accelerator: Arc<dyn TableProvider>,
        scan_params: TableScanParams,
        filters_to_reapply: Vec<Expr>,
        schema: SchemaRef,
    },
}

impl From<Arc<dyn ExecutionPlan>> for CachingScanInput {
    fn from(input: Arc<dyn ExecutionPlan>) -> Self {
        Self::Planned(input)
    }
}

impl CachingScanInput {
    fn schema(&self) -> SchemaRef {
        match self {
            Self::Planned(input) => input.schema(),
            Self::Deferred { schema, .. } => Arc::clone(schema),
        }
    }

    async fn into_plan(self) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        match self {
            Self::Planned(input) => Ok(input),
            Self::Deferred {
                accelerator,
                scan_params,
                filters_to_reapply,
                schema,
            } => {
                let input = scan_params
                    .scan_and_optimize(accelerator.as_ref(), &filters_to_reapply)
                    .await?;
                if input.schema().fields() != schema.fields() {
                    tracing::debug!(expected = ?schema, actual = ?input.schema(), "Cache fallback schema mismatch");
                    return Err(DataFusionError::Execution(
                        "The cached response schema changed while fetching the origin".to_string(),
                    ));
                }
                // Physical scans may omit schema-level table metadata. Keep the
                // deferred node's output schema without accepting changed fields.
                if input.schema() == schema {
                    Ok(input)
                } else {
                    Ok(Arc::new(SchemaCastScanExec::new(input, schema)))
                }
            }
        }
    }
}

/// Caching acceleration execution plan that checks staleness and triggers background refresh
pub struct CachingAccelerationScanExec {
    input: CachingScanInput,
    plan_properties: Arc<PlanProperties>,
    /// Maximum time data is considered "fresh" - can be served without refresh
    max_age: Option<Duration>,
    /// Time window after `max_age` during which stale data can be served while revalidating
    stale_while_revalidate: Option<Duration>,
    /// How expired cached data is served when the upstream source fails: never,
    /// always, or within a finite staleness window.
    stale_if_error: StaleIfError,
    federated: Arc<dyn TableProvider>,
    accelerator: Arc<dyn TableProvider>,
    dataset_name: String,
    io_runtime: Handle,
    filters: Vec<Expr>,
    projection: Option<Vec<usize>>,
    limit: Option<usize>,
    /// Mutex to protect concurrent access to the accelerator during cache/snapshot operations
    accelerator_write_mutex: Arc<Mutex<()>>,
    /// Tracks in-flight revalidation requests to avoid duplicate upstream requests during SWR window
    in_flight_revalidations: InFlightRevalidations,
    /// Child accelerators that should receive cached data when this parent stores new cache entries
    synchronized_children: SynchronizedChildren,
    /// Sender for batched cache writes
    batch_write_tx: CacheWriteSender,
    /// Built once instead of a fresh `SessionContext` per fetch.
    session_state: Arc<SessionState>,
}

impl CachingAccelerationScanExec {
    #[expect(clippy::too_many_arguments)]
    pub(super) fn new(
        input: impl Into<CachingScanInput>,
        max_age: Option<Duration>,
        stale_while_revalidate: Option<Duration>,
        stale_if_error: StaleIfError,
        federated: Arc<dyn TableProvider>,
        accelerator: Arc<dyn TableProvider>,
        dataset_name: String,
        io_runtime: Handle,
        filters: Vec<Expr>,
        projection: Option<Vec<usize>>,
        limit: Option<usize>,
        accelerator_write_mutex: Arc<Mutex<()>>,
        in_flight_revalidations: InFlightRevalidations,
        synchronized_children: SynchronizedChildren,
        batch_write_tx: CacheWriteSender,
    ) -> Self {
        let max_age = Some(effective_max_age(max_age));
        let input = input.into();

        let plan_properties = Arc::new(match &input {
            CachingScanInput::Planned(input) => input
                .properties()
                .as_ref()
                .clone()
                .with_emission_type(EmissionType::Final)
                .with_partitioning(Partitioning::UnknownPartitioning(1)),
            CachingScanInput::Deferred { schema, .. } => PlanProperties::new(
                EquivalenceProperties::new(Arc::clone(schema)),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Final,
                Boundedness::Bounded,
            ),
        });

        let session_state = Arc::clone(&SHARED_SESSION_STATE);

        Self {
            input,
            plan_properties,
            max_age,
            stale_while_revalidate,
            stale_if_error,
            federated,
            accelerator,
            dataset_name,
            io_runtime,
            filters,
            projection,
            limit,
            accelerator_write_mutex,
            in_flight_revalidations,
            synchronized_children,
            batch_write_tx,
            session_state,
        }
    }
}

impl std::fmt::Debug for CachingAccelerationScanExec {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "CachingAccelerationScanExec")
    }
}

impl DisplayAs for CachingAccelerationScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match &self.input {
            CachingScanInput::Planned(_) => write!(f, "CachingAccelerationScanExec"),
            CachingScanInput::Deferred { .. } => {
                write!(f, "CachingAccelerationScanExec: cache_scan=deferred")
            }
        }
    }
}

impl ExecutionPlan for CachingAccelerationScanExec {
    fn name(&self) -> &'static str {
        "CachingAccelerationScanExec"
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn properties(&self) -> &Arc<datafusion::physical_plan::PlanProperties> {
        &self.plan_properties
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::SinglePartition; self.children().len()]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(
            &Arc<dyn datafusion::physical_plan::PhysicalExpr>,
        ) -> datafusion::error::Result<
            datafusion::common::tree_node::TreeNodeRecursion,
        >,
    ) -> datafusion::error::Result<datafusion::common::tree_node::TreeNodeRecursion> {
        Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        match &self.input {
            CachingScanInput::Planned(input) => vec![input],
            CachingScanInput::Deferred { .. } => vec![],
        }
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let input = match (&self.input, children.as_slice()) {
            (CachingScanInput::Planned(_), [input]) => Arc::clone(input),
            (CachingScanInput::Deferred { .. }, []) => return Ok(self),
            _ => {
                return Err(DataFusionError::Internal(
                    "CachingAccelerationScanExec received an invalid number of children"
                        .to_string(),
                ));
            }
        };
        Ok(Arc::new(Self::new(
            input,
            self.max_age,
            self.stale_while_revalidate,
            self.stale_if_error,
            Arc::clone(&self.federated),
            Arc::clone(&self.accelerator),
            self.dataset_name.clone(),
            self.io_runtime.clone(),
            self.filters.clone(),
            self.projection.clone(),
            self.limit,
            Arc::clone(&self.accelerator_write_mutex),
            Arc::clone(&self.in_flight_revalidations),
            Arc::clone(&self.synchronized_children),
            self.batch_write_tx.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        tracing::debug!(
            "CachingAccelerationScanExec::execute called for dataset={} partition={partition}",
            self.dataset_name
        );

        if partition != 0 {
            return Err(DataFusionError::Execution(format!(
                "CachingAccelerationScanExec only supports partition 0, got {partition}"
            )));
        }

        // The originating request context is attached to the session as
        // an extension by `Query::run_internal`. We read it from the
        // `TaskContext` here and NOT from `RequestContext::current()`,
        // because DataFusion does not propagate Tokio task-locals through
        // `execute()`. The task-local lookup would silently fall back to
        // the global `INTERNAL_REQUEST_CONTEXT` (Protocol::Internal, no
        // principal), collapsing every caller to `CacheNamespace::System`
        // and defeating isolation.
        let batch_write_tx = self.batch_write_tx.with_task_context(Arc::clone(&context));
        let request_context = context
            .session_config()
            .get_extension::<runtime_request_context::RequestContext>();

        // A native read that also filters response columns fetches, claims and
        // stores the whole response of its request. The filter above this scan
        // applies the response predicates to the result.
        let response_filtered = batch_write_tx
            .requires_complete_fetch()
            .then(|| writer::response_filtered_request(&self.filters))
            .flatten();
        let (fill_filters, response_filtered) = match response_filtered {
            Some(request) => (request, true),
            None => (self.filters.clone(), false),
        };

        // With neither a fresh nor an SWR window, a stale-if-error entry can
        // only be served after a failing fetch. Do not execute its scan until
        // that failure; successful fetches replace any stored response for the key.
        if uses_source_first(
            &self.filters,
            self.max_age,
            self.stale_while_revalidate,
            self.stale_if_error,
        ) {
            let schema = self.input.schema();
            let stream_schema = Arc::clone(&schema);
            let input = self.input.clone();
            let federated = Arc::clone(&self.federated);
            let session_state = Arc::clone(&self.session_state);
            let dataset_name = self.dataset_name.clone();
            let limit = self.limit;
            let stale_if_error = self.stale_if_error;
            let io_runtime = self.io_runtime.clone();
            let synchronized_children = Arc::clone(&self.synchronized_children);
            let in_flight_revalidations = Arc::clone(&self.in_flight_revalidations);
            let stream = futures::stream::once(async move {
                let namespace = request_context.as_deref().map_or(
                    runtime_request_context::CacheNamespace::System,
                    runtime_request_context::RequestContext::cache_namespace,
                );
                CacheRefreshHelper::handle_cache_miss(
                    federated,
                    &session_state,
                    &dataset_name,
                    &fill_filters,
                    limit,
                    stream_schema,
                    true, // replace any stored response without a preliminary lookup
                    response_filtered,
                    stale_if_error,
                    Duration::ZERO,
                    Some(CacheFallback::Deferred {
                        input,
                        partition,
                        context,
                    }),
                    &io_runtime,
                    synchronized_children,
                    batch_write_tx,
                    namespace,
                    in_flight_revalidations,
                )
                .await
            })
            .flatten();
            return Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)));
        }

        let CachingScanInput::Planned(input) = &self.input else {
            return Err(DataFusionError::Internal(
                "A cache-first read requires a planned accelerator input".to_string(),
            ));
        };
        let accelerator_stream = input.execute(partition, Arc::clone(&context))?;

        // When no filters are provided (e.g., SELECT *), return cached data directly
        // without triggering HTTP requests to the federated source or staleness checks.
        if self.filters.is_empty() {
            tracing::debug!(
                "CachingAccelerationScanExec::execute: No filters for dataset={}, returning accelerator stream directly",
                self.dataset_name
            );
            return Ok(accelerator_stream);
        }

        let schema = accelerator_stream.schema();
        let schema_clone = Arc::clone(&schema);

        let federated = Arc::clone(&self.federated);
        let accelerator = Arc::clone(&self.accelerator);
        let session_state = Arc::clone(&self.session_state);
        let dataset_name = self.dataset_name.clone();
        let filters = self.filters.clone();
        let limit = self.limit;
        let max_age = self.max_age;
        let stale_while_revalidate = self.stale_while_revalidate;
        let stale_if_error = self.stale_if_error;
        let io_runtime = self.io_runtime.clone();
        let in_flight_revalidations = Arc::clone(&self.in_flight_revalidations);
        let synchronized_children = Arc::clone(&self.synchronized_children);

        tracing::debug!(
            "CacheAccelerationScanExec::execute about to spawn cache check for dataset={}",
            dataset_name
        );

        // Use stream::once pattern to handle cache miss like FallbackOnZeroResultsScanExec
        let cache_miss_or_stale_stream = futures::stream::once(async move {
            // Capture the originating request's cache namespace once. Every
            // subsequent cache lookup, write, and SWR refresh inherits this
            // value so the entire scan stays inside one principal's scope.
            let namespace = request_context.as_deref().map_or(
                runtime_request_context::CacheNamespace::System,
                runtime_request_context::RequestContext::cache_namespace,
            );

            tracing::debug!(
                "CacheAccelerationScanExec cache check STARTED for dataset={}",
                dataset_name
            );

            // Collect all batches from the accelerator stream
            tracing::debug!(
                dataset = %dataset_name,
                num_filters = filters.len(),
                "About to read batches from accelerator stream; filters: {}", ExprListDisplay::comma_separated(&filters)
            );

            let cached_batches: Vec<RecordBatch> = match accelerator_stream.try_collect().await {
                Ok(batches) => batches,
                Err(e) => {
                    // Error from accelerator - return the error
                    let error_stream = RecordBatchStreamAdapter::new(
                        Arc::clone(&schema_clone),
                        futures::stream::once(async move { Err(e) }),
                    );
                    return Box::pin(error_stream) as SendableRecordBatchStream;
                }
            };

            // Filter out empty batches and count total rows
            let cached_batches: Vec<RecordBatch> = cached_batches
                .into_iter()
                .filter(|b| b.num_rows() > 0)
                .collect();
            let total_cached_rows: usize = cached_batches.iter().map(RecordBatch::num_rows).sum();

            if total_cached_rows > 0 {
                // Check if data is expired (past max_age + stale_while_revalidate)
                // If expired, treat as cache miss with is_expired=true (will upsert)
                if let Some(max_age) = max_age {
                    let freshness = check_cache_freshness(&cached_batches, max_age, stale_while_revalidate).unwrap_or_else(|e| {
                        tracing::warn!("Failed to check cache data freshness for dataset={dataset_name}: {e}, treating as Expired");
                        CacheFreshness::Expired
                    });

                    if freshness == CacheFreshness::Expired {
                        tracing::debug!(
                            "Data is expired for dataset={dataset_name}, treating as cache miss (upsert)"
                        );
                        // Keep the expired batches to fall back to unless the
                        // fallback is off entirely. Whether the entry is still
                        // *inside* the window is decided at error time, against
                        // its `_fetched_at` — collecting them here does not
                        // commit to serving them.
                        let expired_batches = if stale_if_error.serves_stale_on_error() {
                            Some(CacheFallback::Loaded(cached_batches))
                        } else {
                            None
                        };
                        return CacheRefreshHelper::handle_cache_miss(
                            federated,
                            &session_state,
                            &dataset_name,
                            &fill_filters,
                            limit,
                            Arc::clone(&schema_clone),
                            true, // is_expired = true, will upsert
                            response_filtered,
                            stale_if_error,
                            max_age,
                            expired_batches,
                            &io_runtime,
                            Arc::clone(&synchronized_children),
                            batch_write_tx.clone(),
                            namespace,
                            Arc::clone(&in_flight_revalidations),
                        )
                        .await;
                    }
                }

                // Data is fresh or stale - serve from cache (stale triggers background refresh)
                CacheRefreshHelper::handle_cache_hit(
                    cached_batches,
                    &federated,
                    &session_state,
                    &dataset_name,
                    max_age,
                    stale_while_revalidate,
                    &io_runtime,
                    Arc::clone(&schema_clone),
                    &fill_filters,
                    &in_flight_revalidations,
                    &batch_write_tx,
                    namespace,
                )
            } else {
                // Cache miss - no data in accelerator - retrieve from source and store in accelerator
                tracing::debug!(
                    "No cached data for dataset={dataset_name}, treating as cache miss (insert)"
                );
                // A response predicate narrowed the empty read, so it proves
                // nothing about the request. The fill may append only after a
                // read of the whole request also comes back empty.
                let batch_write_tx = if response_filtered {
                    let observed = batch_write_tx.observe_cache_scan();
                    match CacheRefreshHelper::holds_no_rows(
                        &accelerator,
                        &session_state,
                        &fill_filters,
                        namespace.storage_id(),
                        context,
                    )
                    .await
                    {
                        Ok(true) => observed,
                        Ok(false) => batch_write_tx.without_scan_observation(),
                        Err(error) => {
                            tracing::debug!(
                                "Replacing cached rows for dataset {dataset_name} because their absence could not be checked: {error}"
                            );
                            batch_write_tx.without_scan_observation()
                        }
                    }
                } else {
                    batch_write_tx
                };
                CacheRefreshHelper::handle_cache_miss(
                    federated,
                    &session_state,
                    &dataset_name,
                    &fill_filters,
                    limit,
                    Arc::clone(&schema_clone),
                    false,                  // is_expired = false, will insert (append)
                    response_filtered,
                    StaleIfError::Disabled, // no cached entry to fall back to
                    max_age.unwrap_or_default(), // unused: no expired batches
                    None,                   // no expired batches
                    &io_runtime,
                    synchronized_children,
                    batch_write_tx,
                    namespace,
                    Arc::clone(&in_flight_revalidations),
                )
                .await
            }
        })
        .flatten();

        let adapter = RecordBatchStreamAdapter::new(schema, cache_miss_or_stale_stream);
        Ok(Box::pin(adapter))
    }
}

#[cfg(test)]
mod cache_namespace_column_tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};

    #[test]
    fn reserved_column_name_is_case_insensitive() {
        assert!(is_reserved_caching_column(CACHE_NAMESPACE_COLUMN));
        assert!(is_reserved_caching_column("__SPICE_CACHE_NAMESPACE"));
        assert!(!is_reserved_caching_column("_fetched_at"));
        assert!(!is_reserved_caching_column("request_path"));
    }

    #[test]
    fn extend_schema_appends_namespace_column() {
        let schema = Schema::new(vec![
            Field::new("request_path", DataType::Utf8, false),
            Field::new("content", DataType::Utf8, true),
        ]);
        let extended = extend_schema_with_cache_namespace("ds", &schema).expect("ok");
        assert_eq!(extended.fields().len(), 3);
        assert_eq!(extended.field(2).name(), CACHE_NAMESPACE_COLUMN);
        assert_eq!(extended.field(2).data_type(), &DataType::Utf8);
        assert!(
            !extended.field(2).is_nullable(),
            "namespace column is required so missing-on-read indicates a corrupt table"
        );
    }

    #[test]
    fn extend_schema_rejects_collision_with_clear_error() {
        let schema = Schema::new(vec![
            Field::new("request_path", DataType::Utf8, false),
            Field::new(CACHE_NAMESPACE_COLUMN, DataType::Utf8, true),
        ]);
        let err = extend_schema_with_cache_namespace("http_cache", &schema)
            .expect_err("collision must error");
        let msg = err.to_string();
        assert!(
            msg.contains("http_cache") && msg.contains(CACHE_NAMESPACE_COLUMN),
            "error must name the dataset and the reserved column: {msg}"
        );
    }

    #[test]
    fn extend_schema_rejects_case_insensitive_collision() {
        // The reserved-name guard must be case-insensitive: a source
        // field named `__SPICE_CACHE_NAMESPACE` (or any other casing)
        // collides with the lowercase internal column we would append,
        // and must be rejected with the same error.
        for variant in [
            "__SPICE_CACHE_NAMESPACE",
            "__Spice_Cache_Namespace",
            "__spice_CACHE_namespace",
        ] {
            let schema = Schema::new(vec![
                Field::new("request_path", DataType::Utf8, false),
                Field::new(variant, DataType::Utf8, true),
            ]);
            let err = extend_schema_with_cache_namespace("ds", &schema)
                .err()
                .unwrap_or_else(|| panic!("variant `{variant}` should be rejected as a collision"));
            assert!(
                err.to_string().contains("reserved"),
                "variant `{variant}` should produce a clear collision error: {err}"
            );
        }
    }

    #[test]
    fn stamp_namespace_column_appends_constant_string_array() {
        use arrow::array::{Int32Array, StringArray};
        let payload = Schema::new(vec![Field::new("id", DataType::Int32, false)]);
        let storage = extend_schema_with_cache_namespace("ds", &payload).expect("extend");
        let batch = arrow::array::RecordBatch::try_new(
            Arc::new(payload),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef],
        )
        .expect("batch");

        let stamped = stamp_namespace_column(batch, &storage, "apikey:abc").expect("ok");
        assert_eq!(stamped.num_columns(), 2);
        assert_eq!(stamped.schema().field(1).name(), CACHE_NAMESPACE_COLUMN);
        let ns_col = stamped
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("utf8");
        assert_eq!(ns_col.len(), 3);
        for i in 0..ns_col.len() {
            assert_eq!(ns_col.value(i), "apikey:abc");
        }
    }

    #[test]
    fn stamp_namespace_column_skips_storage_that_does_not_declare_it() {
        use arrow::array::StringArray;
        // A mock accelerator with an unextended schema must be written exactly
        // as it is, or the insert would fail on arity.
        let payload = Schema::new(vec![Field::new("content", DataType::Utf8, false)]);
        let batch = arrow::array::RecordBatch::try_new(
            Arc::new(payload.clone()),
            vec![Arc::new(StringArray::from(vec!["x"])) as ArrayRef],
        )
        .expect("batch");

        let stamped = stamp_namespace_column(batch, &payload, "public").expect("ok");
        assert_eq!(stamped.num_columns(), 1);
    }

    #[test]
    fn stamp_namespace_column_is_idempotent_when_already_present() {
        use arrow::array::{Int32Array, StringArray};
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(CACHE_NAMESPACE_COLUMN, DataType::Utf8, false),
        ]));
        let batch = arrow::array::RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1])) as ArrayRef,
                Arc::new(StringArray::from(vec!["system"])) as ArrayRef,
            ],
        )
        .expect("batch");

        let stamped = stamp_namespace_column(batch, &schema, "public").expect("ok");
        // Existing column wins; we must not silently rewrite a row's tag
        // because doing so could mask a bug in upstream stamping.
        let ns_col = stamped
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("utf8");
        assert_eq!(ns_col.value(0), "system");
    }
}

#[cfg(test)]
mod pool_tests {
    use super::*;
    use arrow::datatypes::{Field, Schema};
    use datafusion::execution::context::SessionContext;
    use datafusion::execution::memory_pool::GreedyMemoryPool;
    use datafusion::execution::runtime_env::RuntimeEnv;
    use runtime_acceleration::change_sink::provider::ProviderChangeSinkBackend;
    use runtime_acceleration::change_sink::{ChangeSink, ChangeSinkContext};

    fn context(pool: &Arc<dyn MemoryPool>) -> SessionContext {
        SessionContext::new_with_config_rt(
            util::session_state::session_config(),
            Arc::new(RuntimeEnv {
                memory_pool: Arc::clone(pool),
                ..RuntimeEnv::default()
            }),
        )
    }

    fn row(content: &str) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("request_path", DataType::Utf8, false),
            Field::new("request_query", DataType::Utf8, true),
            Field::new("request_body", DataType::Utf8, true),
            Field::new("content", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["/items"])),
                Arc::new(StringArray::from(vec![None::<&str>])),
                Arc::new(StringArray::from(vec![None::<&str>])),
                Arc::new(StringArray::from(vec![content])),
            ],
        )
        .expect("request row")
    }

    fn table(batches: Vec<RecordBatch>) -> Arc<dyn TableProvider> {
        Arc::new(
            data_components::arrow::write::MemTable::try_new(row("").schema(), vec![batches])
                .expect("real memory table"),
        )
    }

    fn target(
        pool: &Arc<dyn MemoryPool>,
        stored: Vec<RecordBatch>,
    ) -> (SynchronizedCacheTarget, ChangeSink, Arc<Mutex<()>>) {
        let accelerator = table(stored);
        let dataset = TableReference::bare("pool_test");
        let backend = ChangeSinkContext::new(dataset.clone(), Arc::clone(&accelerator));
        let lock = Arc::clone(&backend.write_lock);
        let sink = ChangeSink::new(
            Arc::new(ProviderChangeSinkBackend::new(backend).with_ordered_replacement()),
            context(pool),
            &Handle::current(),
            8,
        );
        let writer = CacheWriteSender::from_sink(
            sink.clone(),
            accelerator.schema(),
            dataset,
            RuntimeStatus::new(),
            Arc::new(AtomicI64::new(0)),
            Arc::clone(pool),
        );
        (
            SynchronizedCacheTarget {
                accelerator,
                writer,
                in_flight: InFlightRevalidations::default(),
            },
            sink,
            lock,
        )
    }

    async fn contents(provider: &Arc<dyn TableProvider>) -> Vec<String> {
        let batches = SessionContext::new()
            .read_table(Arc::clone(provider))
            .expect("read storage")
            .collect()
            .await
            .expect("collect storage");
        batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column_by_name("content")
                    .expect("content")
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("Utf8")
                    .iter()
                    .map(|value| value.expect("non-null content").to_string())
            })
            .collect()
    }

    async fn cache_drain_table(
        target: &SynchronizedCacheTarget,
        sink: &ChangeSink,
    ) -> super::super::AcceleratedTable {
        let mut table = super::super::Builder::new(
            RuntimeStatus::new(),
            TableReference::bare("pool_test"),
            Arc::new(crate::federated::FederatedTable::new_unchecked(Arc::clone(
                &target.accelerator,
            ))),
            "arrow".into(),
            Arc::clone(&target.accelerator),
            super::super::refresh::Refresh::new(
                runtime_component::dataset::acceleration::RefreshMode::Disabled,
            ),
            Handle::current(),
        )
        .build()
        .await
        .expect("drain owner");
        table.change_sink = Some(sink.clone());
        table.batch_write_tx = Some(target.writer.clone());
        table
    }

    #[tokio::test]
    async fn drain_retains_accepted_cache_preparation_and_fences_new_jobs() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 20));
            let (target, sink, _) = target(&pool, vec![row("old")]);
            let table = cache_drain_table(&target, &sink).await;
            let filters = vec![col("request_path").eq(lit("/items"))];
            let key = compute_cache_key_from_filters_and_namespace(&filters, "public");
            let ClaimOutcome::Leader(claim) =
                CacheKeyClaim::acquire(&target.in_flight, key.clone(), None)
            else {
                panic!("exclusive claim");
            };
            let batches = vec![row("new")];
            let charge = RetainedBufferCharge::for_batches(&pool, &batches).expect("input charge");
            let job = NativeCacheWrite::new(
                target.writer.clone(),
                CacheWriteRequest {
                    batches,
                    filters,
                    cache_key: key,
                    namespace_id: "public".into(),
                    replaces_existing: true,
                },
                claim,
                Arc::new(tokio::sync::RwLock::new(Vec::new())),
                "pool_test".into(),
                charge,
            )
            .expect("owned cache input");
            let (release, held) = tokio::sync::oneshot::channel();
            target
                .writer
                .spawn_owned(&Handle::current(), async move {
                    held.await.expect("release accepted preparation");
                    job.run().await
                })
                .expect("admitted cache job");
            let first = table.begin_changes_drain();
            let second = table.begin_changes_drain();
            let mut cancelled_waiter = Box::pin(first.wait());
            assert!(futures::poll!(cancelled_waiter.as_mut()).is_pending());
            drop(cancelled_waiter);
            assert!(
                target
                    .writer
                    .spawn_owned(&Handle::current(), async { Ok(()) })
                    .is_err()
            );
            assert_eq!(contents(&target.accelerator).await, vec!["old"]);
            assert!(pool.reserved() > 0);
            assert_eq!(target.in_flight.lock().len(), 1);
            release.send(()).expect("finish accepted work");
            second.wait().await.expect("generation drain");
            table
                .drain_changes()
                .await
                .expect("repeated successful drain");
            assert_eq!(contents(&target.accelerator).await, vec!["new"]);
            assert!(target.in_flight.lock().is_empty());
            assert_eq!(pool.reserved(), 0);
            assert!(
                sink.reserve().await.is_err(),
                "storage closes after publication"
            );
        })
        .await
        .expect("accepted cache drain must settle");
    }

    /// A cache job that failed before reaching storage has still finished, so it
    /// must not fence the generation: a later reload has to be able to replace it.
    #[tokio::test]
    async fn drain_settles_failed_cache_jobs_without_fencing_and_closes_storage() {
        tokio::time::timeout(Duration::from_secs(5), async {
            for panics in [false, true] {
                let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 20));
                let (target, sink, _) = target(&pool, vec![row("old")]);
                let table = cache_drain_table(&target, &sink).await;
                let (release, held) = tokio::sync::oneshot::channel();
                target
                    .writer
                    .spawn_owned(&Handle::current(), async move {
                        held.await.expect("release accepted job");
                        assert!(!panics, "controlled cache task panic");
                        Err(DataFusionError::Execution(
                            "controlled cache preparation failure".into(),
                        ))
                    })
                    .expect("accepted job");
                let first = table.begin_changes_drain();
                let second = table.begin_changes_drain();
                release.send(()).expect("release failed work");
                first
                    .wait()
                    .await
                    .expect("a finished job does not fail the drain");
                second.wait().await.expect("repeated drain observer");
                assert!(
                    sink.reserve().await.is_err(),
                    "failed cache work must not skip storage close"
                );
                assert_eq!(contents(&target.accelerator).await, vec!["old"]);
                assert!(
                    target
                        .writer
                        .spawn_owned(&Handle::current(), async { Ok(()) })
                        .is_err()
                );
            }
        })
        .await
        .expect("failed cache drains must settle");
    }

    #[tokio::test]
    async fn follower_pools_share_admission_until_last_response_stream_drops() {
        let batch = row("shared");
        let bytes = batch.get_array_memory_size();
        let leader_pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(bytes + 16384));
        let other_pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(bytes));
        let charge = RetainedBufferCharge::for_batches(&leader_pool, std::slice::from_ref(&batch))
            .expect("leader admission");
        let ownership = Arc::downgrade(&charge);
        let (sender, receiver) =
            watch::channel(FetchState::Ready(Arc::new(vec![batch]), Some(charge)));
        let filters = [col("request_path").eq(lit("/items"))];
        let source = table(vec![row("must not fetch")]);
        let mut streams = Vec::new();
        for pool in [&leader_pool, &other_pool, &other_pool] {
            let state = context(pool).state();
            streams.push(
                CacheRefreshHelper::follow_cache_miss(
                    receiver.clone(),
                    UncoalescedFetch {
                        federated: Arc::clone(&source),
                        session_state: &state,
                        task_context: state.task_ctx(),
                        dataset_name: "pool_test",
                        filters: &filters,
                        limit: None,
                        schema: source.schema(),
                        stale_if_error: StaleIfError::Disabled,
                        max_age: Duration::ZERO,
                        expired_batches: None,
                    },
                )
                .await,
            );
        }
        drop(sender);
        drop(receiver);
        assert_eq!(
            leader_pool.reserved(),
            bytes + ownership.upgrade().expect("live streams").metadata_bytes()
        );
        assert_eq!(
            other_pool.reserved(),
            bytes,
            "secondary-pool followers share admission"
        );
        let first = streams[0].try_next().await.expect("stream").expect("batch");
        assert_eq!(
            first
                .column_by_name("content")
                .expect("content")
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("Utf8")
                .value(0),
            "shared"
        );
        drop(streams.pop());
        assert_eq!(other_pool.reserved(), bytes);
        drop(streams.pop());
        assert_eq!(other_pool.reserved(), 0);
        assert_eq!(
            leader_pool.reserved(),
            bytes + ownership.upgrade().expect("leader stream").metadata_bytes()
        );
        drop(streams);
        assert_eq!(leader_pool.reserved(), 0);
    }

    #[tokio::test]
    async fn refused_follower_releases_shared_buffers_before_real_http_fallback() {
        use data_components::http::provider::HttpTableProvider;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        for body in [None, Some("")] {
            let batch = row("leader");
            let leader_pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 20));
            let caller_pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(0));
            let charge =
                RetainedBufferCharge::for_batches(&leader_pool, std::slice::from_ref(&batch))
                    .expect("leader charge");
            let weak = Arc::downgrade(&charge);
            let (sender, receiver) =
                watch::channel(FetchState::Ready(Arc::new(vec![batch]), Some(charge)));
            drop(sender);
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("listener");
            let url = format!("http://{}/", listener.local_addr().expect("address"));
            let pool = Arc::clone(&leader_pool);
            let server = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.expect("HTTP request");
                let mut request = Vec::new();
                loop {
                    let mut buffer = [0; 1024];
                    let size = socket.read(&mut buffer).await.expect("read request");
                    assert!(size > 0);
                    request.extend_from_slice(&buffer[..size]);
                    if request.windows(4).any(|part| part == b"\r\n\r\n") {
                        break;
                    }
                    assert!(request.len() < 8192);
                }
                assert!(
                    weak.upgrade().is_none(),
                    "fallback must release shared watch and charge"
                );
                assert_eq!(pool.reserved(), 0);
                let payload = "fresh雪\0";
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{payload}",
                    payload.len()
                );
                socket
                    .write_all(response.as_bytes())
                    .await
                    .expect("response");
                String::from_utf8(request).expect("request headers")
            });
            #[expect(
                clippy::default_trait_access,
                reason = "this crate has no direct reqwest dependency"
            )]
            let source: Arc<dyn TableProvider> = Arc::new(
                HttpTableProvider::new(
                    url.parse().expect("URL"),
                    Default::default(),
                    "text".into(),
                    true,
                )
                .with_allowed_paths(["/items"])
                .expect("allow test endpoint")
                .enable_body_filters(1024),
            );
            let mut filters = vec![col("request_path").eq(lit("/items"))];
            if let Some(body) = body {
                filters.push(col("request_body").eq(lit(body)));
            }
            let state = context(&caller_pool).state();
            let batches: Vec<RecordBatch> = tokio::time::timeout(Duration::from_secs(10), async {
                CacheRefreshHelper::follow_cache_miss(
                    receiver,
                    UncoalescedFetch {
                        federated: Arc::clone(&source),
                        session_state: &state,
                        task_context: state.task_ctx(),
                        dataset_name: "pool_test",
                        filters: &filters,
                        limit: None,
                        schema: source.schema(),
                        stale_if_error: StaleIfError::Disabled,
                        max_age: Duration::ZERO,
                        expired_batches: None,
                    },
                )
                .await
                .try_collect()
                .await
            })
            .await
            .expect("bounded HTTP fetch")
            .expect("fallback response");
            let wire = server.await.expect("HTTP server");
            assert!(wire.starts_with(if body.is_some() {
                "POST /items "
            } else {
                "GET /items "
            }));
            assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
            let strings = |name: &str| {
                batches[0]
                    .column_by_name(name)
                    .expect("column")
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("Utf8")
            };
            assert_eq!(
                strings("request_query").iter().collect::<Vec<_>>(),
                vec![Some("")]
            );
            assert_eq!(
                strings("request_body").iter().collect::<Vec<_>>(),
                vec![Some(body.unwrap_or(""))]
            );
            assert_eq!(strings("content").value(0), "fresh雪\0");
            assert_eq!(caller_pool.reserved(), 0);
            assert_eq!(leader_pool.reserved(), 0);
        }
    }

    #[tokio::test]
    async fn periodic_explicit_request_replays_stored_scope() {
        use data_components::http::provider::HttpTableProvider;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        for enriched in [false, true] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("listener");
            let url = format!("http://{}/?key=A", listener.local_addr().expect("address"));
            let server = tokio::spawn(async move {
                let mut requests = Vec::new();
                for payload in ["v1", "v2"] {
                    let (mut socket, _) = listener.accept().await.expect("HTTP request");
                    let mut request = Vec::new();
                    loop {
                        let mut buffer = [0; 1024];
                        let size = socket.read(&mut buffer).await.expect("read request");
                        assert!(size > 0);
                        request.extend_from_slice(&buffer[..size]);
                        if request.windows(4).any(|part| part == b"\r\n\r\n") {
                            break;
                        }
                        assert!(request.len() < 8192);
                    }
                    requests.push(String::from_utf8(request).expect("headers"));
                    socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{payload}", payload.len()).as_bytes())
                        .await.expect("response");
                }
                requests
            });
            #[expect(
                clippy::default_trait_access,
                reason = "this crate has no direct reqwest dependency"
            )]
            let source: Arc<dyn TableProvider> = Arc::new(
                HttpTableProvider::new(
                    url.parse().expect("URL"),
                    Default::default(),
                    "text".into(),
                    true,
                )
                .with_allowed_paths(["/items"])
                .expect("allow endpoint")
                .enable_query_filters(1024)
                .enable_body_filters(1024)
                .with_max_retries(0),
            );
            let source = if enriched {
                data_components::metadata_enriched_table_provider(
                    source,
                    std::collections::HashMap::from([(
                        "test_scope".to_string(),
                        "periodic".to_string(),
                    )]),
                    std::collections::HashMap::default(),
                )
            } else {
                source
            };
            let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 20));
            let state = Arc::new(context(&pool).state());
            assert!(
                source
                    .scan(
                        state.as_ref(),
                        None,
                        &[col("request_path").eq(lit(""))],
                        None
                    )
                    .await
                    .is_err(),
                "public empty path stays invalid"
            );
            // A GET: the row stores `request_body = ''`, which the refresh
            // must not replay as an explicit-empty POST (#14768).
            let filters = vec![
                col("request_path").eq(lit("/items")),
                col("request_query").eq(lit("key=A")),
            ];
            let initial = CacheRefreshHelper::fetch_for_population(
                &source,
                &state,
                "periodic_test",
                &filters,
                None,
                state.task_ctx(),
                Some(&pool),
            )
            .await
            .expect("initial HTTP fetch");
            assert!(initial.complete);
            let expected_scope =
                CacheRefreshHelper::extract_filters_from_row(&initial.batches[0], 0)
                    .expect("initial stored scope");
            let accelerator: Arc<dyn TableProvider> = Arc::new(
                data_components::arrow::write::MemTable::try_new(
                    source.schema(),
                    vec![
                        initial
                            .batches
                            .into_iter()
                            .map(|batch| {
                                arrow_tools::record_batch::try_cast_to(batch, source.schema())
                                    .expect("cast initial response to storage schema")
                            })
                            .collect(),
                    ],
                )
                .expect("stored initial response"),
            );
            drop(initial.charge);
            let dataset = TableReference::bare("periodic_test");
            let backend = ChangeSinkContext::new(dataset.clone(), Arc::clone(&accelerator));
            let write_lock = Arc::clone(&backend.write_lock);
            let sink = ChangeSink::new(
                Arc::new(ProviderChangeSinkBackend::new(backend).with_ordered_replacement()),
                context(&pool),
                &Handle::current(),
                8,
            );
            let writer = CacheWriteSender::from_sink(
                sink.clone(),
                source.schema(),
                dataset,
                RuntimeStatus::new(),
                Arc::new(AtomicI64::new(0)),
                Arc::clone(&pool),
            );
            let rows = tokio::time::timeout(
                Duration::from_secs(10),
                CacheRefreshHelper::refresh_all_stale_rows(
                    source,
                    Arc::clone(&accelerator),
                    state,
                    "periodic_test",
                    Duration::ZERO,
                    write_lock,
                    InFlightRevalidations::default(),
                    writer,
                ),
            )
            .await
            .expect("bounded periodic refresh")
            .expect("refresh");
            assert_eq!(rows, 1);
            let refreshed = context(&pool)
                .read_table(Arc::clone(&accelerator))
                .expect("read")
                .collect()
                .await
                .expect("stored rows");
            assert_eq!(
                refreshed.iter().map(RecordBatch::num_rows).sum::<usize>(),
                1
            );
            assert_eq!(
                CacheRefreshHelper::extract_filters_from_row(&refreshed[0], 0)
                    .expect("refreshed scope"),
                expected_scope
            );
            assert_eq!(contents(&accelerator).await, vec!["v2"]);
            let requests = server.await.expect("HTTP server");
            assert_eq!(requests.len(), 2);
            assert!(
                requests
                    .iter()
                    .all(|wire| wire.starts_with("GET /items?key=A ")),
                "enriched={enriched}: {requests:?}",
            );
            println!(
                "periodic replay: enriched={enriched} original_scope={expected_scope:?} wire={:?} stored=v2",
                requests
                    .iter()
                    .map(|wire| wire.lines().next().expect("request line"))
                    .collect::<Vec<_>>()
            );
            sink.close(Duration::from_secs(5))
                .await
                .expect("close periodic sink");
            assert_eq!(pool.reserved(), 0);
        }
    }

    #[tokio::test]
    async fn child_admission_and_pending_publication_retain_both_pools() {
        let parent_pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 20));
        let child_pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 20));
        let (child, sink, lock) = target(&child_pool, vec![row("old")]);
        let guard = lock.lock().await;
        let batch = row("new");
        let bytes = batch.get_array_memory_size();
        let charge = RetainedBufferCharge::for_batches(&parent_pool, std::slice::from_ref(&batch))
            .expect("parent admission");
        let ownership = Arc::downgrade(&charge);
        let children = Arc::new(tokio::sync::RwLock::new(vec![child.clone()]));
        CacheRefreshHelper::propagate_to_synchronized_children(
            &children,
            "pool_test",
            &[col("request_path").eq(lit("/items"))],
            &[batch],
            true,
            Some(charge),
            "public",
        )
        .await;
        assert_eq!(
            parent_pool.reserved(),
            bytes
                + ownership
                    .upgrade()
                    .expect("pending publication")
                    .metadata_bytes()
        );
        assert!(
            child_pool.reserved() >= bytes,
            "child input plus distinct preparation"
        );
        let receiver = child
            .in_flight
            .lock()
            .values()
            .next()
            .expect("pending child claim")
            .state
            .clone();
        drop(guard);
        sink.flush().await.expect("child publication");
        assert_eq!(contents(&child.accelerator).await, vec!["new"]);
        assert!(child.in_flight.lock().is_empty());
        assert_eq!(
            parent_pool.reserved(),
            bytes + ownership.upgrade().expect("watch charge").metadata_bytes(),
            "watch still owns source and metadata charges"
        );
        assert_eq!(
            child_pool.reserved(),
            bytes,
            "watch still owns child admission"
        );
        drop(receiver);
        assert_eq!(parent_pool.reserved(), 0);
        assert_eq!(child_pool.reserved(), 0);
        sink.close(Duration::from_secs(5))
            .await
            .expect("close child");
    }

    #[tokio::test]
    async fn native_fanout_declines_stricter_child_without_changing_its_storage() {
        let parent_pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 20));
        let child_pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(0));
        let (parent, parent_sink, _) = target(&parent_pool, vec![]);
        let (child, child_sink, _) = target(&child_pool, vec![row("old")]);
        let filters = vec![col("request_path").eq(lit("/items"))];
        let key = compute_cache_key_from_filters_and_namespace(&filters, "public");
        let ClaimOutcome::Leader(mut claim) =
            CacheKeyClaim::acquire(&parent.in_flight, key.clone(), None)
        else {
            panic!("exclusive parent");
        };
        let batches = vec![row("new")];
        let charge =
            RetainedBufferCharge::for_batches(&parent_pool, &batches).expect("parent charge");
        claim.publish_if_cacheable(&batches, Some(Arc::clone(&charge)), true);
        NativeCacheWrite::new(
            parent.writer.clone(),
            CacheWriteRequest {
                batches,
                filters,
                cache_key: key,
                namespace_id: "public".into(),
                replaces_existing: true,
            },
            claim,
            Arc::new(tokio::sync::RwLock::new(vec![child.clone()])),
            "pool_test".into(),
            charge,
        )
        .expect("fanout work")
        .run()
        .await
        .expect("parent publication");
        assert_eq!(contents(&parent.accelerator).await, vec!["new"]);
        assert_eq!(contents(&child.accelerator).await, vec!["old"]);
        assert!(child.in_flight.lock().is_empty());
        assert!(parent.in_flight.lock().is_empty());
        assert_eq!(child_pool.reserved(), 0);
        assert_eq!(parent_pool.reserved(), 0);
        parent_sink
            .close(Duration::from_secs(5))
            .await
            .expect("close parent");
        child_sink
            .close(Duration::from_secs(5))
            .await
            .expect("close child");
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use arrow::array::{
        Int32Array, RecordBatch, StringArray, TimestampNanosecondArray, UInt16Array,
    };
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use arrow_tools::metadata_keys::HTTP_RESPONSE_STATUS_METADATA_KEY;
    use async_trait::async_trait;
    use cache::utils::RESPONSE_STATUS_COLUMN;
    use datafusion::catalog::Session;
    use datafusion::datasource::TableType;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::physical_plan::{ChildrenPropertiesMode, ReplaceChildrenOptions};
    use datafusion::prelude::SessionContext;
    use parking_lot::RwLock;
    use std::sync::Arc;
    use std::time::{Duration, SystemTime};

    /// Every filter shape that makes the HTTP connector send an empty POST
    /// body bypasses the cache; shapes that send a GET or a non-empty body do
    /// not.
    #[test]
    fn explicit_empty_request_body_is_detected_in_every_filter_shape() {
        let body = || col("request_body");
        let cases: Vec<(&str, Vec<Expr>, bool)> = vec![
            ("eq ''", vec![body().eq(lit(""))], true),
            (
                "in list",
                vec![body().in_list(vec![lit("x"), lit("")], false)],
                true,
            ),
            ("or", vec![body().eq(lit("x")).or(body().eq(lit("")))], true),
            (
                "beside other filters",
                vec![col("request_path").eq(lit("/items")), body().eq(lit(""))],
                true,
            ),
            ("eq 'x'", vec![body().eq(lit("x"))], false),
            ("not eq '' sends a GET", vec![body().not_eq(lit(""))], false),
            (
                "no body filter",
                vec![col("request_path").eq(lit("/items"))],
                false,
            ),
            (
                "empty query, no body",
                vec![col("request_query").eq(lit(""))],
                false,
            ),
            ("no filters", vec![], false),
        ];
        for (name, filters, expected) in cases {
            assert_eq!(
                sends_explicit_empty_request_body(&filters),
                expected,
                "{name}: {filters:?}"
            );
        }
    }

    /// A lookup that names a request but sends no body is pinned to GET
    /// entries; a POST lookup, one that names no request value, or a cache
    /// without the column, is not pinned.
    #[test]
    fn request_identity_filters_pin_get_lookups_to_get_entries() {
        let http = Schema::new(vec![
            Field::new("request_path", DataType::Utf8, true),
            Field::new("request_query", DataType::Utf8, true),
            Field::new("request_body", DataType::Utf8, true),
        ]);
        let other = Schema::new(vec![Field::new("id", DataType::Int32, true)]);
        let get = vec![col("request_body").eq(lit(""))];
        let path = col("request_path").eq(lit("/items"));
        let cases: Vec<(&str, Vec<Expr>, &Schema, Vec<Expr>)> = vec![
            ("path only", vec![path.clone()], &http, get.clone()),
            (
                "query only",
                vec![col("request_query").eq(lit("q=a"))],
                &http,
                get.clone(),
            ),
            (
                "body predicate that sends no body",
                vec![path.clone(), col("request_body").not_eq(lit("z"))],
                &http,
                get.clone(),
            ),
            (
                "path and body",
                vec![path.clone(), col("request_body").eq(lit("x"))],
                &http,
                vec![],
            ),
            (
                "headers only",
                vec![col("request_headers").eq(lit(r#"{"x-test":"a"}"#))],
                &http,
                get.clone(),
            ),
            (
                "no request value named",
                vec![col("response_status").eq(lit(200_u16))],
                &http,
                vec![],
            ),
            (
                "only a body predicate that sends no body",
                vec![col("request_body").not_eq(lit("z"))],
                &http,
                get,
            ),
            ("not an HTTP cache", vec![path], &other, vec![]),
        ];
        for (name, filters, schema, expected) in cases {
            assert_eq!(
                request_identity_filters(&filters, schema),
                expected,
                "{name}"
            );
        }
    }

    /// A stored GET entry is re-requested without its `request_body = ''`
    /// predicate, and every other request predicate is kept.
    #[test]
    fn source_replay_filters_drop_only_the_empty_body() {
        let path = col("request_path").eq(lit("/items"));
        let query = col("request_query").eq(lit(""));
        let empty_body = col("request_body").eq(lit(""));
        let body = col("request_body").eq(lit("x"));
        assert_eq!(
            source_replay_filters(&[path.clone(), query.clone(), empty_body]),
            vec![path.clone(), query.clone()]
        );
        assert_eq!(
            source_replay_filters(&[path.clone(), query.clone(), body.clone()]),
            vec![path, query, body]
        );
    }

    /// Test-only stand-in for the shared `Arc<SessionState>`.
    fn test_session_state() -> Arc<SessionState> {
        Arc::new(SessionStateBuilder::new().with_default_features().build())
    }

    /// Mock `TableProvider` that records filters passed to `scan()` for verification.
    #[derive(Debug)]
    struct FilterTrackingTableProvider {
        schema: SchemaRef,
        /// Data to return from scan
        data: Vec<RecordBatch>,
        /// Record of all filter sets passed to `scan()` calls
        recorded_filters: Arc<RwLock<Vec<Vec<String>>>>,
    }

    impl FilterTrackingTableProvider {
        fn new(schema: SchemaRef, data: Vec<RecordBatch>) -> Self {
            Self {
                schema,
                data,
                recorded_filters: Arc::new(RwLock::new(Vec::new())),
            }
        }

        fn get_recorded_filters(&self) -> Vec<Vec<String>> {
            self.recorded_filters.read().clone()
        }
    }

    #[async_trait]
    impl TableProvider for FilterTrackingTableProvider {
        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }

        fn table_type(&self) -> TableType {
            TableType::Base
        }

        async fn scan(
            &self,
            _state: &dyn Session,
            _projection: Option<&Vec<usize>>,
            filters: &[Expr],
            _limit: Option<usize>,
        ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
            // Record the filters for later verification
            let filter_strings: Vec<String> = filters
                .iter()
                .map(|f| f.human_display().to_string())
                .collect();
            self.recorded_filters.write().push(filter_strings);

            // Return the configured data
            Ok(Arc::new(DataSourceExec::new(Arc::new(
                MemorySourceConfig::try_new(
                    std::slice::from_ref(&self.data),
                    Arc::clone(&self.schema),
                    None,
                )?,
            ))))
        }
    }

    /// Mock accelerator that supports `insert_into` for upsert operations.
    /// Tracks what data was written to it.
    #[derive(Debug)]
    struct MockAcceleratorTableProvider {
        schema: SchemaRef,
        /// Current data in the accelerator
        data: Arc<RwLock<Vec<RecordBatch>>>,
    }

    impl MockAcceleratorTableProvider {
        fn new(schema: SchemaRef, initial_data: Vec<RecordBatch>) -> Self {
            Self {
                schema,
                data: Arc::new(RwLock::new(initial_data)),
            }
        }

        fn get_data(&self) -> Vec<RecordBatch> {
            self.data.read().clone()
        }
    }

    /// The line an operator acts on has to carry the dataset, what they will observe, and
    /// where to go next; it is the only explanation they get for an accelerator serving
    /// nothing while the runtime reports itself healthy.
    #[test]
    fn unwritable_message_names_the_dataset_the_consequence_and_the_docs() {
        let message = accelerator_unwritable_message(
            &TableReference::bare("api_data"),
            3,
            &"no encoding for Map",
        );
        assert!(message.contains("'api_data'"), "message: {message}");
        // The impact has to hold whether or not the accelerator already stored rows: a
        // populated cache goes stale rather than empty, so the message must not claim the
        // table returns nothing.
        assert!(
            message.contains("no new result is being cached"),
            "message: {message}"
        );
        assert!(
            message.contains("already cached will not be updated"),
            "message: {message}"
        );
        assert!(
            message.contains("no encoding for Map"),
            "message: {message}"
        );
        assert!(
            message.contains("https://spiceai.org/docs/components/data-accelerators"),
            "message: {message}"
        );
    }

    /// Drive the tracker to the point where it reports the dataset unhealthy.
    fn fail_until_unhealthy(health: &mut CacheWriteHealth) {
        for _ in 0..CACHE_WRITE_FAILURES_BEFORE_UNHEALTHY {
            health.record_failure(&"write failed");
        }
    }

    #[test]
    fn a_run_of_failed_flushes_reports_the_dataset_unhealthy() {
        let status = RuntimeStatus::new();
        let dataset = TableReference::bare("api_data");
        let mut health = CacheWriteHealth::new(Arc::clone(&status), dataset.clone());

        for _ in 1..CACHE_WRITE_FAILURES_BEFORE_UNHEALTHY {
            health.record_failure(&"write failed");
            assert_eq!(
                status.get_dataset_status(&dataset),
                None,
                "a failed flush short of the threshold can be transient and must not report the \
                 dataset unhealthy"
            );
        }

        health.record_failure(&"write failed");
        let message = status
            .get_dataset_status(&dataset)
            .as_ref()
            .and_then(ComponentStatus::error_message)
            .map(str::to_owned)
            .expect("dataset should be unhealthy once the accelerator stops storing rows");
        assert!(message.contains("'api_data'"), "message: {message}");

        // A flush that stores rows is the accelerator working again.
        health.record_success();
        assert_eq!(
            status.get_dataset_status(&dataset),
            Some(ComponentStatus::Ready)
        );
    }

    /// The status a tracker sets can be replaced by any other writer - a periodic refresh
    /// reporting `Refreshing` while the accelerator is still unwritable, say. If the tracker
    /// only ever reported the threshold crossing, the write outage would vanish from the
    /// dataset's status for good the first time that happened.
    #[test]
    fn an_unhealthy_dataset_is_reported_again_after_another_writer_replaces_the_status() {
        let status = RuntimeStatus::new();
        let dataset = TableReference::bare("api_data");
        let mut health = CacheWriteHealth::new(Arc::clone(&status), dataset.clone());
        fail_until_unhealthy(&mut health);

        // A refresh cycle takes the dataset through a non-error state.
        status.update_dataset(&dataset, ComponentStatus::Refreshing);

        health.record_failure(&"write failed");
        // The re-report names the dataset and carries the run as it now stands: a
        // fourth consecutive failure, not the third the first report counted.
        assert_eq!(
            status
                .get_dataset_status(&dataset)
                .as_ref()
                .and_then(ComponentStatus::error_message),
            Some(
                "Dataset 'api_data' failed to write to its accelerator 4 times in a row, so no new \
                 result is being cached and anything already cached will not be updated. Cause: \
                 write failed. See: https://spiceai.org/docs/components/data-accelerators"
            ),
            "a still-failing accelerator must report itself again once its status is replaced"
        );
    }

    /// Two faults, one status cell. Until a dataset can carry both (spiceai/spiceai#13572),
    /// the tracker must not overwrite an error it did not set - doing so would lose the other
    /// diagnostic, and would then let this tracker's own recovery clear a fault that is still
    /// unresolved.
    #[test]
    fn an_error_reported_by_another_path_is_not_overwritten_or_later_cleared() {
        let status = RuntimeStatus::new();
        let dataset = TableReference::bare("api_data");
        let mut health = CacheWriteHealth::new(Arc::clone(&status), dataset.clone());

        status.update_dataset(
            &dataset,
            ComponentStatus::error_with_message("refresh failed"),
        );
        fail_until_unhealthy(&mut health);

        let refresh_error_stands = |step: &str| {
            assert_eq!(
                status
                    .get_dataset_status(&dataset)
                    .as_ref()
                    .and_then(ComponentStatus::error_message),
                Some("refresh failed"),
                "the refresh failure must survive {step}"
            );
        };
        refresh_error_stands("a run of failed cache writes");

        health.record_success();
        refresh_error_stands("the cache writes recovering");
    }

    #[test]
    fn recovery_does_not_clear_an_error_reported_by_another_path() {
        let status = RuntimeStatus::new();
        let dataset = TableReference::bare("api_data");
        let mut health = CacheWriteHealth::new(Arc::clone(&status), dataset.clone());
        fail_until_unhealthy(&mut health);

        // The refresh path reports its own failure after this tracker reported its.
        status.update_dataset(
            &dataset,
            ComponentStatus::Error(Some("refresh failed".to_string())),
        );

        health.record_success();
        // `ComponentStatus` equates every `Error` regardless of message, so assert on the
        // message: comparing the statuses would pass even if the refresh error were replaced.
        let current = status.get_dataset_status(&dataset);
        assert_eq!(
            current.as_ref().and_then(ComponentStatus::error_message),
            Some("refresh failed"),
            "cache writes succeeding again says nothing about a refresh that is still failing"
        );
    }

    /// Helper to create a test cache write channel and spawn a consumer that writes to an accelerator.
    ///
    /// Uses the real `spawn_batched_cache_write_task` for realistic testing.
    /// Returns the sender for queuing writes and a handle to the consumer task.
    fn spawn_test_cache_write_consumer(
        accelerator: &Arc<MockAcceleratorTableProvider>,
        in_flight_revalidations: &InFlightRevalidations,
    ) -> (CacheWriteSender, tokio::task::JoinHandle<()>) {
        let (tx, rx) = create_cache_write_channel();
        let accelerator_write_mutex = Arc::new(Mutex::new(()));
        let last_updated_at = Arc::new(AtomicI64::new(0));
        let handle = spawn_batched_cache_write_task(
            rx,
            Arc::clone(accelerator) as Arc<dyn TableProvider>,
            TableReference::bare("test_dataset"),
            accelerator_write_mutex,
            Arc::clone(in_flight_revalidations),
            last_updated_at,
            RuntimeStatus::new(),
        );
        (tx, handle)
    }

    #[async_trait]
    impl TableProvider for MockAcceleratorTableProvider {
        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }

        fn table_type(&self) -> TableType {
            TableType::Base
        }

        async fn scan(
            &self,
            _state: &dyn Session,
            _projection: Option<&Vec<usize>>,
            _filters: &[Expr],
            _limit: Option<usize>,
        ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
            let data = self.data.read().clone();
            Ok(Arc::new(DataSourceExec::new(Arc::new(
                MemorySourceConfig::try_new(&[data], Arc::clone(&self.schema), None)?,
            ))))
        }

        async fn insert_into(
            &self,
            _state: &dyn Session,
            input: Arc<dyn ExecutionPlan>,
            overwrite: InsertOp,
        ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
            // Execute the input plan to get the data
            let task_ctx = Arc::new(datafusion::execution::context::TaskContext::default());
            let batches = datafusion::physical_plan::collect(Arc::clone(&input), task_ctx).await?;

            let mut data = self.data.write();
            if matches!(overwrite, InsertOp::Overwrite) {
                data.clear();
            }
            data.extend(batches);

            // Return an empty exec as we don't need output
            Ok(Arc::new(DataSourceExec::new(Arc::new(
                MemorySourceConfig::try_new(&[vec![]], Arc::clone(&self.schema), None)?,
            ))))
        }
    }

    /// Mock HTTP source table provider that returns data with configurable response status codes.
    /// Used to test that 5xx responses are returned to users but NOT cached.
    #[derive(Debug)]
    struct MockHttpTableProvider {
        schema: SchemaRef,
        /// Data to return from scan (should include `response_status` column)
        data: Vec<RecordBatch>,
        delay: Duration,
    }

    impl MockHttpTableProvider {
        /// Create a mock HTTP provider that returns data with the specified response status code.
        fn with_status(status_code: u16, content: &str) -> Self {
            let schema = Arc::new(
                Schema::new(vec![
                    Field::new("request_path", DataType::Utf8, true),
                    Field::new("request_query", DataType::Utf8, true),
                    Field::new("content", DataType::Utf8, true),
                    Field::new(RESPONSE_STATUS_COLUMN, DataType::UInt16, false),
                    Field::new(
                        CACHE_REFRESHED_AT_COLUMN,
                        DataType::Timestamp(TimeUnit::Nanosecond, None),
                        true,
                    ),
                ])
                // Tagged the way the real HTTP connector's `base_table_schema`
                // tags it, so `cache::http_fetch_status` recognizes it —
                // see `HTTP_RESPONSE_STATUS_METADATA_KEY`.
                .with_metadata(std::collections::HashMap::from([(
                    HTTP_RESPONSE_STATUS_METADATA_KEY.to_string(),
                    "1".to_string(),
                )])),
            );

            #[expect(clippy::cast_possible_truncation)]
            let now = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .expect("Time went backwards")
                .as_nanos() as i64;

            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(StringArray::from(vec!["/api/test"])),
                    Arc::new(StringArray::from(vec!["q=test"])),
                    Arc::new(StringArray::from(vec![content])),
                    Arc::new(UInt16Array::from(vec![status_code])),
                    Arc::new(TimestampNanosecondArray::from(vec![Some(now)])),
                ],
            )
            .expect("to create batch");

            Self {
                schema,
                data: vec![batch],
                delay: Duration::ZERO,
            }
        }

        fn with_delay(mut self, delay: Duration) -> Self {
            self.delay = delay;
            self
        }
    }

    #[async_trait]
    impl TableProvider for MockHttpTableProvider {
        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }

        fn table_type(&self) -> TableType {
            TableType::Base
        }

        async fn scan(
            &self,
            _state: &dyn Session,
            _projection: Option<&Vec<usize>>,
            _filters: &[Expr],
            _limit: Option<usize>,
        ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
            if !self.delay.is_zero() {
                tokio::time::sleep(self.delay).await;
            }
            Ok(Arc::new(DataSourceExec::new(Arc::new(
                MemorySourceConfig::try_new(
                    std::slice::from_ref(&self.data),
                    Arc::clone(&self.schema),
                    None,
                )?,
            ))))
        }
    }

    /// A source that counts every scan and can delay before answering, so a test
    /// can prove single-flight: N concurrent cache misses for one key must reach
    /// the origin exactly once, the rest replaying the leader's batches.
    ///
    /// Holds `rows` identical rows and, like the HTTP connector, truncates a
    /// scan to its `limit`; with no rows it yields no batches at all.
    #[derive(Debug)]
    struct CountingHttpTableProvider {
        schema: SchemaRef,
        content: String,
        status_code: u16,
        rows: usize,
        scans: Arc<std::sync::atomic::AtomicUsize>,
        delay: Duration,
    }

    impl CountingHttpTableProvider {
        fn new(status_code: u16, content: &str, delay: Duration) -> Self {
            let schema = Arc::new(Schema::new(vec![
                Field::new("request_path", DataType::Utf8, true),
                Field::new("request_query", DataType::Utf8, true),
                Field::new("content", DataType::Utf8, true),
                Field::new(RESPONSE_STATUS_COLUMN, DataType::UInt16, false),
                Field::new(
                    CACHE_REFRESHED_AT_COLUMN,
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    true,
                ),
            ]));
            Self {
                schema,
                content: content.to_string(),
                status_code,
                rows: 1,
                scans: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
                delay,
            }
        }

        /// The number of rows the origin holds for any filters (default 1).
        fn with_rows(mut self, rows: usize) -> Self {
            self.rows = rows;
            self
        }

        fn scan_count(&self) -> usize {
            self.scans.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl TableProvider for CountingHttpTableProvider {
        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }

        fn table_type(&self) -> TableType {
            TableType::Base
        }

        async fn scan(
            &self,
            _state: &dyn Session,
            _projection: Option<&Vec<usize>>,
            _filters: &[Expr],
            limit: Option<usize>,
        ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
            self.scans.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

            if !self.delay.is_zero() {
                tokio::time::sleep(self.delay).await;
            }

            // The HTTP connector truncates its response to the scan's limit,
            // so a bounded scan returns at most that many of the origin's rows.
            let rows = limit.map_or(self.rows, |limit| limit.min(self.rows));
            if rows == 0 {
                return Ok(Arc::new(DataSourceExec::new(Arc::new(
                    MemorySourceConfig::try_new(&[vec![]], Arc::clone(&self.schema), None)?,
                ))));
            }

            #[expect(clippy::cast_possible_truncation)]
            let now = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .expect("time")
                .as_nanos() as i64;

            let batch = RecordBatch::try_new(
                Arc::clone(&self.schema),
                vec![
                    Arc::new(StringArray::from(vec!["/api/test"; rows])) as ArrayRef,
                    Arc::new(StringArray::from(vec!["q=test"; rows])) as ArrayRef,
                    Arc::new(StringArray::from(vec![self.content.as_str(); rows])) as ArrayRef,
                    Arc::new(UInt16Array::from(vec![self.status_code; rows])) as ArrayRef,
                    Arc::new(TimestampNanosecondArray::from(vec![Some(now); rows])) as ArrayRef,
                ],
            )?;

            Ok(Arc::new(DataSourceExec::new(Arc::new(
                MemorySourceConfig::try_new(&[vec![batch]], Arc::clone(&self.schema), None)?,
            ))))
        }
    }

    /// Acquires a key as the leader of an unbounded fetch, or panics if a fetch
    /// for it is already in flight. Test-only convenience for the many
    /// claim/refresh tests.
    fn leader_claim(in_flight: &InFlightRevalidations, key: &str) -> CacheKeyClaim {
        match CacheKeyClaim::acquire(in_flight, key.to_string(), None) {
            ClaimOutcome::Leader(claim) => claim,
            ClaimOutcome::Follower(_) => panic!("expected to lead the claim for key {key}"),
        }
    }

    /// Counts the rows a stream yields.
    async fn drain_rows(stream: SendableRecordBatchStream) -> usize {
        drain(stream).await.iter().map(RecordBatch::num_rows).sum()
    }

    /// Waits for a batched cache writer to exit. It exits once every sender is
    /// gone, after flushing whatever is still queued, so the accelerator then
    /// holds every write that was ever enqueued: a test can assert the absence
    /// of a write as firmly as its presence, with no flush-interval guess.
    async fn await_writer_exit(writer: tokio::task::JoinHandle<()>) {
        tokio::time::timeout(Duration::from_secs(5), writer)
            .await
            .expect("the cache writer should exit within 5s of its last sender dropping")
            .expect("the cache writer task should not panic");
    }

    /// Waits until `accelerator` holds at least `rows` rows, so a test can
    /// observe a periodic flush while the write channel stays open.
    async fn wait_for_accelerator_rows(accelerator: &MockAcceleratorTableProvider, rows: usize) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let held: usize = accelerator
                .get_data()
                .iter()
                .map(RecordBatch::num_rows)
                .sum();
            if held >= rows {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "the accelerator held {held} row(s) within 5s, expected {rows}"
            );
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    /// The `content` of every row the accelerator holds, in storage order.
    fn stored_contents(accelerator: &MockAcceleratorTableProvider) -> Vec<String> {
        accelerator
            .get_data()
            .iter()
            .flat_map(|batch| {
                let content = batch
                    .column_by_name("content")
                    .and_then(|c| c.as_any().downcast_ref::<StringArray>())
                    .expect("content column");
                (0..batch.num_rows())
                    .map(|row| content.value(row).to_string())
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    /// Waits until `origin` has been scanned `scans` times, so a test can start a
    /// second caller only once the first holds its claim and is inside its fetch.
    async fn wait_for_scans(origin: &CountingHttpTableProvider, scans: usize) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while origin.scan_count() < scans {
            assert!(
                tokio::time::Instant::now() < deadline,
                "the origin was scanned {} time(s) within 5s, expected {scans}",
                origin.scan_count()
            );
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    /// An in-flight fetch serves a request only when it asked the origin for at
    /// least as many rows: unbounded serves anything, bounded serves a request
    /// bounded at or below it, and nothing bounded serves an unbounded request.
    #[test]
    fn an_in_flight_fetch_serves_requests_bounded_at_or_below_its_limit() {
        let fetch = |limit| InFlightFetch {
            limit,
            state: watch::channel(FetchState::Pending).1,
        };

        assert!(fetch(None).serves(None));
        assert!(fetch(None).serves(Some(3)));
        assert!(fetch(Some(3)).serves(Some(3)));
        assert!(fetch(Some(3)).serves(Some(1)));
        assert!(
            !fetch(Some(1)).serves(Some(3)),
            "a fetch truncated to 1 row cannot answer a request for 3"
        );
        assert!(
            !fetch(Some(3)).serves(None),
            "a bounded fetch cannot answer an unbounded request"
        );
        assert!(
            !fetch(Some(0)).serves(Some(1)),
            "a fetch bounded at zero rows answers nothing but another zero-row request"
        );
        assert!(fetch(Some(0)).serves(Some(0)));
    }

    fn create_test_schema_with_refresh_timestamp() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new(
                CACHE_REFRESHED_AT_COLUMN,
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
        ]))
    }

    fn create_test_schema_without_refresh_timestamp() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]))
    }

    fn create_test_schema_with_request_params() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("request_path", DataType::Utf8, true),
            Field::new("request_query", DataType::Utf8, true),
            Field::new("request_body", DataType::Utf8, true),
            Field::new(RESPONSE_STATUS_COLUMN, DataType::UInt16, false),
            Field::new(
                CACHE_REFRESHED_AT_COLUMN,
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
        ]))
    }

    #[test]
    fn test_extract_filters_from_row_all_columns_present() {
        let schema = create_test_schema_with_request_params();
        let id_array = Int32Array::from(vec![1, 2]);
        let path_array = StringArray::from(vec![Some("/api/users"), Some("/api/posts")]);
        let query_array = StringArray::from(vec![Some("page=1"), Some("limit=10")]);
        let body_array = StringArray::from(vec![Some("{\"id\":1}"), Some("{\"id\":2}")]);
        let status_array = UInt16Array::from(vec![200, 200]);

        #[expect(clippy::cast_possible_truncation)]
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("Time went backwards")
            .as_nanos() as i64;

        let refresh_timestamps = TimestampNanosecondArray::from(vec![Some(now), Some(now)]);

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(id_array),
                Arc::new(path_array),
                Arc::new(query_array),
                Arc::new(body_array),
                Arc::new(status_array),
                Arc::new(refresh_timestamps),
            ],
        )
        .expect("Failed to create batch");

        let filters = CacheRefreshHelper::extract_filters_from_row(&batch, 0)
            .expect("Should extract filters");
        assert_eq!(filters.len(), 3, "Should extract 3 filters");
    }

    #[test]
    fn test_extract_filters_from_row_with_nulls() {
        let schema = create_test_schema_with_request_params();
        let id_array = Int32Array::from(vec![1]);
        let path_array = StringArray::from(vec![Some("/api/users")]);
        let query_array = StringArray::from(vec![None::<&str>]); // Null query
        let body_array = StringArray::from(vec![Some("{\"id\":1}")]);
        let status_array = UInt16Array::from(vec![200]);

        #[expect(clippy::cast_possible_truncation)]
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("Time went backwards")
            .as_nanos() as i64;

        let refresh_timestamps = TimestampNanosecondArray::from(vec![Some(now)]);

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(id_array),
                Arc::new(path_array),
                Arc::new(query_array),
                Arc::new(body_array),
                Arc::new(status_array),
                Arc::new(refresh_timestamps),
            ],
        )
        .expect("Failed to create batch");

        let filters = CacheRefreshHelper::extract_filters_from_row(&batch, 0)
            .expect("Should extract filters");
        assert_eq!(filters.len(), 3);
        assert_eq!(filters[1], col("request_query").is_null());
    }

    #[test]
    fn test_extract_filters_from_row_with_empty_strings() {
        let schema = create_test_schema_with_request_params();
        let id_array = Int32Array::from(vec![1]);
        let path_array = StringArray::from(vec![Some("")]); // Empty string
        let query_array = StringArray::from(vec![Some("page=1")]);
        let body_array = StringArray::from(vec![Some("")]); // Empty string
        let status_array = UInt16Array::from(vec![200]);

        #[expect(clippy::cast_possible_truncation)]
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("Time went backwards")
            .as_nanos() as i64;

        let refresh_timestamps = TimestampNanosecondArray::from(vec![Some(now)]);

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(id_array),
                Arc::new(path_array),
                Arc::new(query_array),
                Arc::new(body_array),
                Arc::new(status_array),
                Arc::new(refresh_timestamps),
            ],
        )
        .expect("Failed to create batch");

        let filters = CacheRefreshHelper::extract_filters_from_row(&batch, 0)
            .expect("Should extract filters");
        assert_eq!(filters.len(), 3);
        assert_eq!(filters[0], col("request_path").eq(lit("")));
        assert_eq!(filters[2], col("request_body").eq(lit("")));
    }

    #[test]
    fn test_extract_filters_from_row_missing_columns() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                CACHE_REFRESHED_AT_COLUMN,
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
        ]));

        let id_array = Int32Array::from(vec![1]);

        #[expect(clippy::cast_possible_truncation)]
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("Time went backwards")
            .as_nanos() as i64;

        let refresh_timestamps = TimestampNanosecondArray::from(vec![Some(now)]);

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(id_array), Arc::new(refresh_timestamps)],
        )
        .expect("Failed to create batch");

        let filters = CacheRefreshHelper::extract_filters_from_row(&batch, 0)
            .expect("Should extract filters");
        assert_eq!(
            filters.len(),
            0,
            "Should extract 0 filters when columns are missing"
        );
    }

    #[tokio::test]
    async fn test_cache_freshness_fresh_data() {
        let schema = create_test_schema_with_refresh_timestamp();
        let id_array = Int32Array::from(vec![1, 2]);
        let name_array = StringArray::from(vec!["alice", "bob"]);

        #[expect(clippy::cast_possible_truncation)]
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("Time went backwards")
            .as_nanos() as i64;

        // Data fetched 10 seconds ago
        let refresh_timestamps = TimestampNanosecondArray::from(vec![
            Some(now - 10_000_000_000),
            Some(now - 15_000_000_000),
        ]);

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(id_array),
                Arc::new(name_array),
                Arc::new(refresh_timestamps),
            ],
        )
        .expect("Failed to create batch");

        let max_age = Duration::from_mins(1);
        let stale_while_revalidate = Some(Duration::from_secs(30));

        let freshness = check_cache_freshness(&[batch], max_age, stale_while_revalidate)
            .expect("Should check freshness");
        assert_eq!(
            freshness,
            CacheFreshness::Fresh,
            "Data within max_age should be fresh"
        );
    }

    #[tokio::test]
    async fn test_cache_freshness_stale_data_with_swr() {
        let schema = create_test_schema_with_refresh_timestamp();
        let id_array = Int32Array::from(vec![1, 2]);
        let name_array = StringArray::from(vec!["alice", "bob"]);

        #[expect(clippy::cast_possible_truncation)]
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("Time went backwards")
            .as_nanos() as i64;

        // Data fetched 70 seconds ago (past max_age of 60s, but within max_age + swr of 90s)
        let refresh_timestamps = TimestampNanosecondArray::from(vec![
            Some(now - 70_000_000_000),
            Some(now - 75_000_000_000),
        ]);

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(id_array),
                Arc::new(name_array),
                Arc::new(refresh_timestamps),
            ],
        )
        .expect("Failed to create batch");

        let max_age = Duration::from_mins(1);
        let stale_while_revalidate = Some(Duration::from_secs(30));

        let freshness = check_cache_freshness(&[batch], max_age, stale_while_revalidate)
            .expect("Should check freshness");
        assert_eq!(
            freshness,
            CacheFreshness::Stale,
            "Data past max_age but within swr should be stale"
        );
    }

    #[tokio::test]
    async fn test_cache_freshness_expired_data() {
        let schema = create_test_schema_with_refresh_timestamp();
        let id_array = Int32Array::from(vec![1, 2]);
        let name_array = StringArray::from(vec!["alice", "bob"]);

        #[expect(clippy::cast_possible_truncation)]
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("Time went backwards")
            .as_nanos() as i64;

        // Data fetched 100 seconds ago (past max_age + swr of 90s)
        let refresh_timestamps = TimestampNanosecondArray::from(vec![
            Some(now - 100_000_000_000),
            Some(now - 110_000_000_000),
        ]);

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(id_array),
                Arc::new(name_array),
                Arc::new(refresh_timestamps),
            ],
        )
        .expect("Failed to create batch");

        let max_age = Duration::from_mins(1);
        let stale_while_revalidate = Some(Duration::from_secs(30));

        let freshness = check_cache_freshness(&[batch], max_age, stale_while_revalidate)
            .expect("Should check freshness");
        assert_eq!(
            freshness,
            CacheFreshness::Expired,
            "Data past max_age + swr should be expired"
        );
    }

    #[tokio::test]
    async fn test_cache_freshness_no_swr_stale_becomes_expired() {
        let schema = create_test_schema_with_refresh_timestamp();
        let id_array = Int32Array::from(vec![1, 2]);
        let name_array = StringArray::from(vec!["alice", "bob"]);

        #[expect(clippy::cast_possible_truncation)]
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("Time went backwards")
            .as_nanos() as i64;

        // Data fetched 70 seconds ago (past max_age of 60s)
        let refresh_timestamps = TimestampNanosecondArray::from(vec![
            Some(now - 70_000_000_000),
            Some(now - 75_000_000_000),
        ]);

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(id_array),
                Arc::new(name_array),
                Arc::new(refresh_timestamps),
            ],
        )
        .expect("Failed to create batch");

        let max_age = Duration::from_mins(1);
        let stale_while_revalidate = None; // No stale-while-revalidate

        let freshness = check_cache_freshness(&[batch], max_age, stale_while_revalidate)
            .expect("Should check freshness");
        assert_eq!(
            freshness,
            CacheFreshness::Expired,
            "Without swr, data past max_age should be expired (not stale)"
        );
    }

    #[tokio::test]
    async fn test_cache_freshness_null_timestamps_are_expired() {
        let schema = create_test_schema_with_refresh_timestamp();
        let id_array = Int32Array::from(vec![1, 2]);
        let name_array = StringArray::from(vec!["alice", "bob"]);

        let refresh_timestamps = TimestampNanosecondArray::from(vec![None, None]);

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(id_array),
                Arc::new(name_array),
                Arc::new(refresh_timestamps),
            ],
        )
        .expect("Failed to create batch");

        let max_age = Duration::from_mins(1);
        let stale_while_revalidate = Some(Duration::from_secs(30));

        let freshness = check_cache_freshness(&[batch], max_age, stale_while_revalidate)
            .expect("Should check freshness");
        assert_eq!(
            freshness,
            CacheFreshness::Expired,
            "Data with null timestamps should be expired"
        );
    }

    #[tokio::test]
    async fn test_cache_freshness_no_refresh_column_is_expired() {
        let schema = create_test_schema_without_refresh_timestamp();
        let id_array = Int32Array::from(vec![1, 2]);
        let name_array = StringArray::from(vec!["alice", "bob"]);

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(id_array), Arc::new(name_array)],
        )
        .expect("Failed to create batch");

        let max_age = Duration::from_mins(1);
        let stale_while_revalidate = Some(Duration::from_secs(30));

        let freshness = check_cache_freshness(&[batch], max_age, stale_while_revalidate)
            .expect("Should check freshness");
        assert_eq!(
            freshness,
            CacheFreshness::Expired,
            "Data without refresh column should be expired"
        );
    }

    #[tokio::test]
    async fn test_cache_freshness_mixed_timestamps_worst_case_wins() {
        let schema = create_test_schema_with_refresh_timestamp();
        let id_array = Int32Array::from(vec![1, 2, 3]);
        let name_array = StringArray::from(vec!["alice", "bob", "charlie"]);

        #[expect(clippy::cast_possible_truncation)]
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("Time went backwards")
            .as_nanos() as i64;

        // Mix: fresh (10s), stale (70s), expired (100s)
        let refresh_timestamps = TimestampNanosecondArray::from(vec![
            Some(now - 10_000_000_000),  // Fresh
            Some(now - 70_000_000_000),  // Stale (past 60s, within 90s)
            Some(now - 100_000_000_000), // Expired (past 90s)
        ]);

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(id_array),
                Arc::new(name_array),
                Arc::new(refresh_timestamps),
            ],
        )
        .expect("Failed to create batch");

        let max_age = Duration::from_mins(1);
        let stale_while_revalidate = Some(Duration::from_secs(30));

        let freshness = check_cache_freshness(&[batch], max_age, stale_while_revalidate)
            .expect("Should check freshness");
        assert_eq!(
            freshness,
            CacheFreshness::Expired,
            "If any row is expired, the whole batch should be considered expired"
        );
    }

    #[tokio::test]
    async fn test_cache_freshness_boundary_conditions() {
        let schema = create_test_schema_with_refresh_timestamp();
        let id_array = Int32Array::from(vec![1]);
        let name_array = StringArray::from(vec!["alice"]);

        #[expect(clippy::cast_possible_truncation)]
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("Time went backwards")
            .as_nanos() as i64;

        let max_age = Duration::from_mins(1);
        let stale_while_revalidate = Duration::from_secs(30);
        #[expect(clippy::cast_possible_truncation)]
        let max_age_nanos = max_age.as_nanos() as i64;
        #[expect(clippy::cast_possible_truncation)]
        let swr_nanos = stale_while_revalidate.as_nanos() as i64;

        // Just within max_age (59 seconds ago)
        let refresh_timestamps_fresh =
            TimestampNanosecondArray::from(vec![Some(now - max_age_nanos + 1_000_000_000)]);

        let batch_fresh = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(id_array.clone()),
                Arc::new(name_array.clone()),
                Arc::new(refresh_timestamps_fresh),
            ],
        )
        .expect("Failed to create batch");

        let freshness =
            check_cache_freshness(&[batch_fresh], max_age, Some(stale_while_revalidate))
                .expect("Should check freshness");
        assert_eq!(freshness, CacheFreshness::Fresh, "Just within max_age");

        // Just past max_age but within swr (61 seconds ago)
        let refresh_timestamps_stale =
            TimestampNanosecondArray::from(vec![Some(now - max_age_nanos - 1_000_000_000)]);

        let batch_stale = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(id_array.clone()),
                Arc::new(name_array.clone()),
                Arc::new(refresh_timestamps_stale),
            ],
        )
        .expect("Failed to create batch");

        let freshness =
            check_cache_freshness(&[batch_stale], max_age, Some(stale_while_revalidate))
                .expect("Should check freshness");
        assert_eq!(freshness, CacheFreshness::Stale, "Just past max_age");

        // Just past max_age + swr (91 seconds ago)
        let refresh_timestamps_expired = TimestampNanosecondArray::from(vec![Some(
            now - max_age_nanos - swr_nanos - 1_000_000_000,
        )]);

        let batch_expired = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(id_array),
                Arc::new(name_array),
                Arc::new(refresh_timestamps_expired),
            ],
        )
        .expect("Failed to create batch");

        let freshness =
            check_cache_freshness(&[batch_expired], max_age, Some(stale_while_revalidate))
                .expect("Should check freshness");
        assert_eq!(
            freshness,
            CacheFreshness::Expired,
            "Just past max_age + swr"
        );
    }

    #[tokio::test]
    async fn test_cache_freshness_empty_batches() {
        let batches: Vec<RecordBatch> = Vec::new();
        let max_age = Duration::from_mins(1);
        let stale_while_revalidate = Some(Duration::from_secs(30));

        let freshness = check_cache_freshness(&batches, max_age, stale_while_revalidate)
            .expect("Should check freshness");
        assert_eq!(
            freshness,
            CacheFreshness::Fresh,
            "Empty batches should be considered fresh (nothing to check)"
        );
    }

    /// Test that `extract_unique_filter_sets` correctly deduplicates rows with identical
    /// (`request_path`, `request_query`, `request_body`) values from actual `RecordBatches`.
    #[test]
    fn test_extract_unique_filter_sets() {
        use arrow::array::StringBuilder;
        use arrow::datatypes::{DataType, Field, Schema};

        // Create a schema with request columns (simulating HTTP connector cache)
        let schema = Arc::new(Schema::new(vec![
            Field::new("request_path", DataType::Utf8, true),
            Field::new("request_query", DataType::Utf8, true),
            Field::new("request_body", DataType::Utf8, true),
            Field::new("data", DataType::Utf8, true), // Simulated response data column
        ]));

        // Build arrays - simulating 5 rows from a JSON array (same request params)
        // plus 1 row from a different request
        let mut path_builder = StringBuilder::new();
        let mut query_builder = StringBuilder::new();
        let mut body_builder = StringBuilder::new();
        let mut data_builder = StringBuilder::new();

        // 5 rows with identical request params (like JSON array elements)
        for i in 0..5 {
            path_builder.append_value("/api/people");
            query_builder.append_value("search=luke");
            body_builder.append_value("");
            data_builder.append_value(format!("person_{i}"));
        }

        // 1 row with different request params
        path_builder.append_value("/api/shows");
        query_builder.append_value("search=breaking");
        body_builder.append_value("");
        data_builder.append_value("show_1");

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(path_builder.finish()),
                Arc::new(query_builder.finish()),
                Arc::new(body_builder.finish()),
                Arc::new(data_builder.finish()),
            ],
        )
        .expect("Should create batch");

        assert_eq!(batch.num_rows(), 6, "Should have 6 rows total");

        // Extract unique filter sets
        let filter_sets = CacheRefreshHelper::extract_unique_stale_entries(&[batch])
            .expect("Should extract filter sets");

        // Should only have 2 unique filter sets (5 duplicates + 1 unique)
        assert_eq!(
            filter_sets.len(),
            2,
            "Should deduplicate 5 identical rows + 1 different row into 2 filter sets"
        );
    }

    #[test]
    fn test_check_cache_freshness_without_fetched_at_column() {
        // Test that batches without fetched_at column are treated as expired
        let schema = create_test_schema_without_refresh_timestamp();
        let id_array = Int32Array::from(vec![1, 2]);
        let name_array = StringArray::from(vec![Some("Alice"), Some("Bob")]);

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(id_array), Arc::new(name_array)],
        )
        .expect("Failed to create batch");

        let max_age = Duration::from_mins(1);
        let freshness =
            check_cache_freshness(&[batch], max_age, None).expect("Should check freshness");

        assert_eq!(
            freshness,
            CacheFreshness::Expired,
            "Batches without _fetched_at column should be expired"
        );
    }

    #[test]
    fn test_check_cache_freshness_with_null_timestamp() {
        // Test that batches with NULL fetched_at are treated as expired
        let schema = create_test_schema_with_refresh_timestamp();
        let id_array = Int32Array::from(vec![1]);
        let name_array = StringArray::from(vec![Some("Alice")]);
        let refresh_timestamps = TimestampNanosecondArray::from(vec![None]); // NULL timestamp

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(id_array),
                Arc::new(name_array),
                Arc::new(refresh_timestamps),
            ],
        )
        .expect("Failed to create batch");

        let max_age = Duration::from_mins(1);
        let freshness =
            check_cache_freshness(&[batch], max_age, None).expect("Should check freshness");

        assert_eq!(
            freshness,
            CacheFreshness::Expired,
            "Batches with NULL _fetched_at should be expired"
        );
    }

    /// Verifies the SWR flow through `handle_cache_hit`.
    /// This ensures that when stale data is accessed, the background refresh uses
    /// `refresh_entry` with the specific access filters (not all cached entries).
    ///
    /// Test flow:
    /// 1. Create stale cached data for multiple entries
    /// 2. Call `handle_cache_hit` with filters for ONE specific entry
    /// 3. Wait for background refresh to complete
    /// 4. Verify federated source was called with ONLY the specific entry's filters
    /// 5. Verify accelerator received the fresh data (rows were updated)
    #[tokio::test]
    async fn test_swr_handle_cache_hit_refreshes_only_accessed_entry() {
        // Create schema with request columns (HTTP connector cache pattern)
        // Includes response_status column required by retain_cacheable_responses
        let schema = Arc::new(Schema::new(vec![
            Field::new("request_path", DataType::Utf8, true),
            Field::new("request_query", DataType::Utf8, true),
            Field::new("data", DataType::Utf8, true),
            Field::new(RESPONSE_STATUS_COLUMN, DataType::UInt16, false),
            Field::new(
                CACHE_REFRESHED_AT_COLUMN,
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
        ]));

        // Create fresh data that the federated source will return when refreshing
        let fresh_data = {
            let path = StringArray::from(vec!["/api/users"]);
            let query = StringArray::from(vec!["id=1"]);
            let data = StringArray::from(vec!["fresh_user_data"]);
            let status = UInt16Array::from(vec![200]); // 200 OK - will pass filter_transient_error_responses

            #[expect(clippy::cast_possible_truncation)]
            let now = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .expect("Time went backwards")
                .as_nanos() as i64;
            let timestamp = TimestampNanosecondArray::from(vec![Some(now)]);

            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(path),
                    Arc::new(query),
                    Arc::new(data),
                    Arc::new(status),
                    Arc::new(timestamp),
                ],
            )
            .expect("Should create batch")
        };

        // Create mock federated source that tracks filters
        let federated = Arc::new(FilterTrackingTableProvider::new(
            Arc::clone(&schema),
            vec![fresh_data],
        ));

        // Create stale cached data - MULTIPLE entries in the cache, ALL stale
        // This tests that only the ACCESSED entry gets refreshed, not all stale entries
        let stale_cached_data = {
            // 3 stale entries: /api/users?id=1, /api/posts?id=2, /api/comments?id=3
            // All fetched 2 minutes ago (TTL is 60s), so all are stale
            let path = StringArray::from(vec!["/api/users", "/api/posts", "/api/comments"]);
            let query = StringArray::from(vec!["id=1", "id=2", "id=3"]);
            let data = StringArray::from(vec![
                "stale_user_data",
                "stale_post_data",
                "stale_comment_data",
            ]);
            let status = UInt16Array::from(vec![200, 200, 200]); // All 200 OK

            #[expect(clippy::cast_possible_truncation)]
            let two_min_ago = (SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .expect("Time went backwards")
                .as_nanos()
                - Duration::from_mins(2).as_nanos()) as i64;
            // All entries have the same stale timestamp
            let timestamp = TimestampNanosecondArray::from(vec![
                Some(two_min_ago),
                Some(two_min_ago),
                Some(two_min_ago),
            ]);

            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(path),
                    Arc::new(query),
                    Arc::new(data),
                    Arc::new(status),
                    Arc::new(timestamp),
                ],
            )
            .expect("Should create batch")
        };

        // Create accelerator with all stale entries
        let accelerator = Arc::new(MockAcceleratorTableProvider::new(
            Arc::clone(&schema),
            vec![stale_cached_data.clone()],
        ));

        // Define filters for accessing ONLY ONE specific entry (/api/users?id=1)
        // The other stale entries (/api/posts, /api/comments) should NOT be refreshed
        let access_filters = vec![
            col("request_path").eq(lit("/api/users")),
            col("request_query").eq(lit("id=1")),
        ];

        let max_age = Some(Duration::from_mins(1)); // 60 second TTL
        let stale_while_revalidate = Some(Duration::from_mins(5)); // 5 minute SWR window
        let in_flight_revalidations: InFlightRevalidations =
            Arc::new(parking_lot::Mutex::new(std::collections::HashMap::new()));

        let (batch_write_tx, consumer_handle) =
            spawn_test_cache_write_consumer(&accelerator, &in_flight_revalidations);

        // Create a tokio runtime handle for the background task
        let io_runtime = tokio::runtime::Handle::current();

        // Call handle_cache_hit - this should:
        // 1. Return the stale data immediately
        // 2. Spawn a background task to refresh ONLY the accessed entry
        let _stream = CacheRefreshHelper::handle_cache_hit(
            vec![stale_cached_data],
            &(Arc::clone(&federated) as Arc<dyn TableProvider>),
            &test_session_state(),
            "test_dataset",
            max_age,
            stale_while_revalidate,
            &io_runtime,
            Arc::clone(&schema),
            &access_filters,
            &in_flight_revalidations,
            &batch_write_tx,
            CacheNamespace::Public,
        );
        // `handle_cache_hit` clones the sender it is lent, so dropping this one
        // leaves the background refresh holding the only sender: once it has queued
        // its write and finished, the writer flushes that write and exits.
        drop(batch_write_tx);
        await_writer_exit(consumer_handle).await;

        // Verify the federated source was called with the SPECIFIC filters only
        let recorded = federated.get_recorded_filters();
        assert_eq!(
            recorded.len(),
            1,
            "Federated source should be called exactly once for the accessed entry. \
             If called 0 times, the refresh didn't trigger. \
             If called >1 times, multiple entries were refreshed (old buggy behavior)."
        );

        let filter_strs = &recorded[0];

        // The key assertion: verify filters match ONLY the ACCESSED entry
        let has_users_path = filter_strs
            .iter()
            .any(|f| f.contains("request_path") && f.contains("/api/users"));
        let has_id_query = filter_strs
            .iter()
            .any(|f| f.contains("request_query") && f.contains("id=1"));

        assert!(
            has_users_path && has_id_query,
            "Background refresh should use filters for the ACCESSED entry (/api/users?id=1). \
             Got filters: {filter_strs:?}"
        );

        // Verify that OTHER stale entries were NOT included in the refresh
        // This is the key test: with the bug, all 3 stale entries would be refreshed
        let has_posts_path = filter_strs.iter().any(|f| f.contains("/api/posts"));
        let has_comments_path = filter_strs.iter().any(|f| f.contains("/api/comments"));

        assert!(
            !has_posts_path && !has_comments_path,
            "Background refresh should NOT include other stale entries (/api/posts, /api/comments). \
             Only the accessed entry should be refreshed. Got filters: {filter_strs:?}"
        );

        // Verify in-flight tracking was cleaned up
        let in_flight = in_flight_revalidations.lock();
        assert!(
            in_flight.is_empty(),
            "In-flight revalidation set should be empty after refresh completes"
        );
        drop(in_flight);

        // Verify the accelerator received the fresh data
        let accelerator_data = accelerator.get_data();
        assert!(
            !accelerator_data.is_empty(),
            "Accelerator should have data after refresh"
        );

        // Find the data column and verify it contains fresh data
        let mut found_fresh_data = false;
        for batch in &accelerator_data {
            if let Ok(data_col_idx) = batch.schema().index_of("data") {
                let data_array = batch
                    .column(data_col_idx)
                    .as_any()
                    .downcast_ref::<StringArray>();
                if let Some(arr) = data_array {
                    for i in 0..arr.len() {
                        if arr.value(i) == "fresh_user_data" {
                            found_fresh_data = true;
                            break;
                        }
                    }
                }
            }
            if found_fresh_data {
                break;
            }
        }

        assert!(
            found_fresh_data,
            "Accelerator should contain fresh data ('fresh_user_data') after background refresh. \
             Current data: {accelerator_data:?}"
        );
    }

    /// Tests that batched cache writer accumulates multiple requests and flushes them periodically.
    ///
    /// Paused time: the flush interval elapses only when the test advances the
    /// clock, so "not yet written" and "written at the interval" are both checked
    /// deterministically, with the channel left open throughout.
    #[tokio::test(start_paused = true)]
    async fn test_batched_cache_writer_flushes_multiple_requests() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        let accelerator = Arc::new(MockAcceleratorTableProvider::new(
            Arc::clone(&schema),
            vec![],
        ));
        let in_flight: InFlightRevalidations =
            Arc::new(parking_lot::Mutex::new(std::collections::HashMap::new()));

        let (tx, _handle) = spawn_test_cache_write_consumer(&accelerator, &in_flight);

        // Send 3 write requests
        for i in 0..3 {
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int32Array::from(vec![i]))],
            )
            .expect("to create batch");
            tx.send(CacheWriteRequest {
                batches: vec![batch],
                filters: vec![],
                cache_key: format!("key_{i}"),
                namespace_id: "public".into(),
                replaces_existing: false,
            })
            .await
            .expect("to send write request");
        }

        // The writer takes the requests but holds them: nothing is written per
        // request, only when the flush interval elapses.
        tokio::task::yield_now().await;
        assert!(
            accelerator.get_data().is_empty(),
            "requests must be buffered until the flush interval, not written one by one"
        );

        tokio::time::advance(Duration::from_millis(CACHE_WRITE_FLUSH_INTERVAL_MS)).await;
        wait_for_accelerator_rows(&accelerator, 3).await;

        // All three requests landed in that one flush, each row exactly once.
        let mut ids: Vec<i32> = accelerator
            .get_data()
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .expect("id column")
                    .values()
                    .to_vec()
            })
            .collect();
        ids.sort_unstable();
        assert_eq!(ids, vec![0, 1, 2], "Should have the 3 rows from 3 requests");
    }

    /// Test that 5xx and 429 responses are returned to users but NOT written to the cache.
    ///
    /// Simulates cache miss flow for both transient error types:
    /// 1. Send a 500 request through `handle_cache_miss` — verify user sees it
    /// 2. Send a 429 request through `handle_cache_miss` — verify user sees it
    /// 3. Verify accelerator remains empty (transient errors not persisted)
    #[tokio::test]
    async fn test_transient_error_responses_returned_to_user_but_not_cached() {
        use futures::StreamExt;

        let http_source_500 = Arc::new(MockHttpTableProvider::with_status(
            500,
            "Internal Server Error",
        ));
        let schema = http_source_500.schema();

        let accelerator = Arc::new(MockAcceleratorTableProvider::new(
            Arc::clone(&schema),
            vec![],
        ));
        let in_flight: InFlightRevalidations =
            Arc::new(parking_lot::Mutex::new(std::collections::HashMap::new()));

        let (batch_write_tx, handle) = spawn_test_cache_write_consumer(&accelerator, &in_flight);

        // --- 500 request ---
        let mut stream = CacheRefreshHelper::handle_cache_miss(
            Arc::clone(&http_source_500) as Arc<dyn TableProvider>,
            &test_session_state(),
            "test_dataset",
            &[col("content").eq(lit("test"))],
            None,
            Arc::clone(&schema),
            false,
            false,
            StaleIfError::Disabled,
            Duration::ZERO,
            None,
            &tokio::runtime::Handle::current(),
            Arc::new(vec![].into()),
            batch_write_tx.clone(),
            CacheNamespace::Public,
            Arc::clone(&in_flight),
        )
        .await;

        let mut user_batches = Vec::new();
        while let Some(result) = stream.next().await {
            user_batches.push(result.expect("stream should not error"));
        }

        assert_eq!(user_batches.len(), 1, "User should receive 1 batch for 500");
        let status_col = user_batches[0]
            .column(
                user_batches[0]
                    .schema()
                    .index_of(RESPONSE_STATUS_COLUMN)
                    .expect("column exists"),
            )
            .as_any()
            .downcast_ref::<UInt16Array>()
            .expect("status column");
        assert_eq!(status_col.value(0), 500, "User should see status 500");

        // --- 429 request ---
        let http_source_429 =
            Arc::new(MockHttpTableProvider::with_status(429, "Too Many Requests"));

        let mut stream = CacheRefreshHelper::handle_cache_miss(
            Arc::clone(&http_source_429) as Arc<dyn TableProvider>,
            &test_session_state(),
            "test_dataset",
            &[col("content").eq(lit("test"))],
            None,
            Arc::clone(&schema),
            false,
            false,
            StaleIfError::Disabled,
            Duration::ZERO,
            None,
            &tokio::runtime::Handle::current(),
            Arc::new(vec![].into()),
            batch_write_tx,
            CacheNamespace::Public,
            Arc::clone(&in_flight),
        )
        .await;

        let mut user_batches = Vec::new();
        while let Some(result) = stream.next().await {
            user_batches.push(result.expect("stream should not error"));
        }

        assert_eq!(user_batches.len(), 1, "User should receive 1 batch for 429");
        let status_col = user_batches[0]
            .column(
                user_batches[0]
                    .schema()
                    .index_of(RESPONSE_STATUS_COLUMN)
                    .expect("column exists"),
            )
            .as_any()
            .downcast_ref::<UInt16Array>()
            .expect("status column");
        assert_eq!(status_col.value(0), 429, "User should see status 429");

        // Both misses have returned and dropped their senders, so the writer
        // flushes anything they enqueued and exits: the check below covers every
        // write ever queued, not only those flushed within a guessed window.
        await_writer_exit(handle).await;

        // Verify accelerator is empty — neither 5xx nor 429 should be cached
        let cached_data = accelerator.get_data();
        assert!(
            cached_data.is_empty(),
            "Transient error responses (5xx/429) should NOT be in accelerator"
        );
    }

    /// A stale cached response, in the shape `MockHttpTableProvider` produces.
    fn stale_cached_batch(schema: &SchemaRef, content: &str) -> RecordBatch {
        use arrow::array::{StringArray, TimestampNanosecondArray, UInt16Array};
        RecordBatch::try_new(
            Arc::clone(schema),
            vec![
                Arc::new(StringArray::from(vec!["/api"])) as ArrayRef,
                Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef,
                Arc::new(StringArray::from(vec![content])) as ArrayRef,
                Arc::new(UInt16Array::from(vec![200_u16])) as ArrayRef,
                Arc::new(TimestampNanosecondArray::from(vec![Some(1_i64)])) as ArrayRef,
            ],
        )
        .expect("batch")
    }

    async fn drain(mut stream: SendableRecordBatchStream) -> Vec<RecordBatch> {
        use futures::StreamExt;
        let mut out = Vec::new();
        while let Some(result) = stream.next().await {
            out.push(result.expect("stream should not error"));
        }
        out
    }

    /// A failing origin reaches us as a *successful* fetch carrying a 5xx row,
    /// so `stale_if_error` has to act on the response's status, not just on a
    /// transport error. Without this an operator who asked to fall back to the
    /// cache gets the origin's error body instead.
    #[tokio::test]
    async fn stale_if_error_serves_the_cached_response_when_the_origin_answers_5xx() {
        let http_source = Arc::new(MockHttpTableProvider::with_status(503, "upstream down"));
        let schema = http_source.schema();
        let accelerator = Arc::new(MockAcceleratorTableProvider::new(
            Arc::clone(&schema),
            vec![],
        ));
        let in_flight: InFlightRevalidations =
            Arc::new(parking_lot::Mutex::new(std::collections::HashMap::new()));
        let (batch_write_tx, _handle) = spawn_test_cache_write_consumer(&accelerator, &in_flight);

        let stale = stale_cached_batch(&schema, "last good response");

        let stream = CacheRefreshHelper::handle_cache_miss(
            Arc::clone(&http_source) as Arc<dyn TableProvider>,
            &test_session_state(),
            "test_dataset",
            &[col("content").eq(lit("test"))],
            None,
            Arc::clone(&schema),
            true,                                     // is_expired
            false,                                    // response_filtered
            StaleIfError::Enabled,                    // stale_if_error enabled (∞)
            Duration::ZERO,                           // max_age (ignored by Enabled)
            Some(CacheFallback::Loaded(vec![stale])), // expired entry
            &tokio::runtime::Handle::current(),
            Arc::new(vec![].into()),
            batch_write_tx,
            CacheNamespace::Public,
            Arc::clone(&in_flight),
        )
        .await;

        let served = drain(stream).await;
        assert_eq!(served.len(), 1);
        let content = served[0]
            .column_by_name("content")
            .and_then(|c| c.as_any().downcast_ref::<arrow::array::StringArray>())
            .expect("content column");
        assert_eq!(
            content.value(0),
            "last good response",
            "the cached response must be served, not the origin's 5xx body"
        );
    }

    /// The same origin failure with `stale_if_error` disabled must still hand
    /// the caller what the origin said — the fallback is opt-in.
    #[tokio::test]
    async fn a_failing_origin_is_returned_to_the_caller_when_stale_if_error_is_off() {
        let http_source = Arc::new(MockHttpTableProvider::with_status(503, "upstream down"));
        let schema = http_source.schema();
        let accelerator = Arc::new(MockAcceleratorTableProvider::new(
            Arc::clone(&schema),
            vec![],
        ));
        let in_flight: InFlightRevalidations =
            Arc::new(parking_lot::Mutex::new(std::collections::HashMap::new()));
        let (batch_write_tx, _handle) = spawn_test_cache_write_consumer(&accelerator, &in_flight);

        let stale = stale_cached_batch(&schema, "last good response");

        let stream = CacheRefreshHelper::handle_cache_miss(
            Arc::clone(&http_source) as Arc<dyn TableProvider>,
            &test_session_state(),
            "test_dataset",
            &[col("content").eq(lit("test"))],
            None,
            Arc::clone(&schema),
            true,
            false,
            StaleIfError::Disabled, // stale_if_error disabled
            Duration::ZERO,         // max_age (ignored by Disabled)
            Some(CacheFallback::Loaded(vec![stale])),
            &tokio::runtime::Handle::current(),
            Arc::new(vec![].into()),
            batch_write_tx,
            CacheNamespace::Public,
            Arc::clone(&in_flight),
        )
        .await;

        let served = drain(stream).await;
        assert_eq!(served.len(), 1);
        let content = served[0]
            .column_by_name("content")
            .and_then(|c| c.as_any().downcast_ref::<arrow::array::StringArray>())
            .expect("content column");
        assert_eq!(content.value(0), "upstream down");
    }

    /// Nanoseconds since the epoch, for placing a cached entry a known distance
    /// in the past.
    fn now_nanos() -> i64 {
        i64::try_from(
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .expect("clock after epoch")
                .as_nanos(),
        )
        .expect("nanoseconds fit i64")
    }

    /// A whole number of seconds as nanoseconds, as an `i64` timestamp offset.
    fn secs_nanos(secs: u64) -> i64 {
        i64::try_from(Duration::from_secs(secs).as_nanos()).expect("nanoseconds fit i64")
    }

    /// A stale cached response in the `MockHttpTableProvider` shape, with an
    /// explicit `_fetched_at` (or a null one when `fetched_at` is `None`).
    fn stale_batch_with_fetched_at(
        schema: &SchemaRef,
        content: &str,
        fetched_at: Option<i64>,
    ) -> RecordBatch {
        use arrow::array::{StringArray, TimestampNanosecondArray, UInt16Array};
        RecordBatch::try_new(
            Arc::clone(schema),
            vec![
                Arc::new(StringArray::from(vec!["/api"])) as ArrayRef,
                Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef,
                Arc::new(StringArray::from(vec![content])) as ArrayRef,
                Arc::new(UInt16Array::from(vec![200_u16])) as ArrayRef,
                Arc::new(TimestampNanosecondArray::from(vec![fetched_at])) as ArrayRef,
            ],
        )
        .expect("batch")
    }

    /// Read the single `content` cell of a served batch.
    fn served_content(batch: &RecordBatch) -> String {
        batch
            .column_by_name("content")
            .and_then(|c| c.as_any().downcast_ref::<arrow::array::StringArray>())
            .map(|c| c.value(0).to_string())
            .expect("content column")
    }

    /// Drive `handle_cache_miss` through the transient-5xx arm: a 503 origin plus
    /// the given expired entry and `stale_if_error`/`max_age`. Returns the one
    /// served content cell — the stale copy when served, the origin body when not.
    async fn transient_5xx_outcome(
        stale: RecordBatch,
        stale_if_error: StaleIfError,
        max_age: Duration,
    ) -> String {
        transient_5xx_outcome_with_delay(stale, stale_if_error, max_age, Duration::ZERO).await
    }

    async fn transient_5xx_outcome_with_delay(
        stale: RecordBatch,
        stale_if_error: StaleIfError,
        max_age: Duration,
        delay: Duration,
    ) -> String {
        let http_source =
            Arc::new(MockHttpTableProvider::with_status(503, "upstream down").with_delay(delay));
        let schema = http_source.schema();
        let accelerator = Arc::new(MockAcceleratorTableProvider::new(
            Arc::clone(&schema),
            vec![],
        ));
        let stale_schema = stale.schema();
        let input: Arc<dyn ExecutionPlan> = Arc::new(DataSourceExec::new(Arc::new(
            MemorySourceConfig::try_new(&[vec![stale]], stale_schema, None).expect("cache input"),
        )));
        let input = input.into();
        let in_flight: InFlightRevalidations =
            Arc::new(parking_lot::Mutex::new(std::collections::HashMap::new()));
        let (batch_write_tx, _handle) = spawn_test_cache_write_consumer(&accelerator, &in_flight);

        let stream = CacheRefreshHelper::handle_cache_miss(
            Arc::clone(&http_source) as Arc<dyn TableProvider>,
            &test_session_state(),
            "test_dataset",
            &[col("content").eq(lit("test"))],
            None,
            Arc::clone(&schema),
            true,
            false,
            stale_if_error,
            max_age,
            Some(CacheFallback::Deferred {
                input,
                partition: 0,
                context: Arc::new(TaskContext::default()),
            }),
            &tokio::runtime::Handle::current(),
            Arc::new(vec![].into()),
            batch_write_tx,
            CacheNamespace::Public,
            Arc::clone(&in_flight),
        )
        .await;

        let served = drain(stream).await;
        assert_eq!(served.len(), 1, "exactly one batch is served");
        served_content(&served[0])
    }

    /// A finite `caching_stale_if_error` window serves an entry whose staleness is
    /// inside it and refuses one past it — the RFC 5861 `stale-if-error=N` core.
    #[tokio::test]
    async fn a_finite_window_serves_inside_and_propagates_outside() {
        let http_source = Arc::new(MockHttpTableProvider::with_status(503, "upstream down"));
        let schema = http_source.schema();
        let max_age = Duration::from_secs(10);
        let window = StaleIfError::For(Duration::from_mins(1));

        // Staleness = now - fetched_at - max_age. 30s past the stale point is
        // inside a 60s window: the cached copy is served.
        let inside = stale_batch_with_fetched_at(
            &schema,
            "cached response",
            Some(now_nanos() - secs_nanos(10 + 30)),
        );
        assert_eq!(
            transient_5xx_outcome(inside, window, max_age).await,
            "cached response",
            "an entry 30s past the stale point is inside a 60s window"
        );

        // 90s past the stale point is outside a 60s window: the origin's
        // transient response is returned instead of a copy too stale to promise.
        let outside = stale_batch_with_fetched_at(
            &schema,
            "cached response",
            Some(now_nanos() - secs_nanos(10 + 90)),
        );
        assert_eq!(
            transient_5xx_outcome(outside, window, max_age).await,
            "upstream down",
            "an entry 90s past the stale point is outside a 60s window"
        );
    }

    /// `For(N)` fails closed when the entry's age cannot be proven — a missing or
    /// null `_fetched_at` — so it cannot serve a copy of unknown staleness.
    #[tokio::test]
    async fn a_finite_window_fails_closed_on_unknown_staleness() {
        use arrow::array::{StringArray, UInt16Array};

        let window = StaleIfError::For(Duration::from_mins(1));
        let max_age = Duration::from_secs(10);

        // A null `_fetched_at` value.
        let http_source = Arc::new(MockHttpTableProvider::with_status(503, "upstream down"));
        let schema = http_source.schema();
        let null_ts = stale_batch_with_fetched_at(&schema, "cached response", None);
        assert_eq!(
            transient_5xx_outcome(null_ts, window, max_age).await,
            "upstream down",
            "a null fetch time cannot prove the entry is inside the window"
        );

        // A batch with no `_fetched_at` column at all.
        let schema_no_ts: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("request_path", DataType::Utf8, true),
            Field::new("request_query", DataType::Utf8, true),
            Field::new("content", DataType::Utf8, true),
            Field::new(RESPONSE_STATUS_COLUMN, DataType::UInt16, false),
        ]));
        let no_column = RecordBatch::try_new(
            Arc::clone(&schema_no_ts),
            vec![
                Arc::new(StringArray::from(vec!["/api"])) as ArrayRef,
                Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef,
                Arc::new(StringArray::from(vec!["cached response"])) as ArrayRef,
                Arc::new(UInt16Array::from(vec![200_u16])) as ArrayRef,
            ],
        )
        .expect("batch");
        assert_eq!(
            transient_5xx_outcome(no_column, window, max_age).await,
            "upstream down",
            "a missing fetch-time column cannot prove the entry is inside the window"
        );
    }

    /// A source that always fails, after sleeping `delay` first. Stands in for
    /// a real network timeout/connection failure that takes real time to be
    /// detected, so a test can prove staleness is measured before that delay,
    /// not after it.
    #[derive(Debug)]
    struct SlowFailingProvider {
        schema: SchemaRef,
        delay: Duration,
    }

    #[async_trait]
    impl TableProvider for SlowFailingProvider {
        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }

        fn table_type(&self) -> TableType {
            TableType::Base
        }

        async fn scan(
            &self,
            _state: &dyn Session,
            _projection: Option<&Vec<usize>>,
            _filters: &[Expr],
            _limit: Option<usize>,
        ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
            tokio::time::sleep(self.delay).await;
            Err(DataFusionError::Execution(
                "connection refused (test)".to_string(),
            ))
        }
    }

    /// Drive `handle_cache_miss` through the `Err(e)` (transport-failure) arm
    /// against a source that takes `delay` to fail, and report whether the
    /// stale entry was served or the error was propagated instead.
    async fn slow_failure_outcome(
        stale: RecordBatch,
        stale_if_error: StaleIfError,
        max_age: Duration,
        delay: Duration,
    ) -> Result<String, String> {
        use futures::StreamExt;

        let schema = stale.schema();
        let failing_source = Arc::new(SlowFailingProvider {
            schema: Arc::clone(&schema),
            delay,
        });
        let accelerator = Arc::new(MockAcceleratorTableProvider::new(
            Arc::clone(&schema),
            vec![],
        ));
        let input: Arc<dyn ExecutionPlan> = Arc::new(DataSourceExec::new(Arc::new(
            MemorySourceConfig::try_new(&[vec![stale]], Arc::clone(&schema), None)
                .expect("cache input"),
        )));
        let input = input.into();
        let in_flight: InFlightRevalidations =
            Arc::new(parking_lot::Mutex::new(std::collections::HashMap::new()));
        let (batch_write_tx, _handle) = spawn_test_cache_write_consumer(&accelerator, &in_flight);

        let mut stream = CacheRefreshHelper::handle_cache_miss(
            failing_source as Arc<dyn TableProvider>,
            &test_session_state(),
            "test_dataset",
            &[col("content").eq(lit("test"))],
            None,
            Arc::clone(&schema),
            true,
            false,
            stale_if_error,
            max_age,
            Some(CacheFallback::Deferred {
                input,
                partition: 0,
                context: Arc::new(TaskContext::default()),
            }),
            &tokio::runtime::Handle::current(),
            Arc::new(vec![].into()),
            batch_write_tx,
            CacheNamespace::Public,
            Arc::clone(&in_flight),
        )
        .await;

        match stream.next().await {
            Some(Ok(batch)) => Ok(served_content(&batch)),
            Some(Err(e)) => Err(e.to_string()),
            None => Err("stream ended with no batches".to_string()),
        }
    }

    /// Regression test for staleness being measured before the (possibly
    /// slow) source-fetch attempt, not after it. An entry whose staleness is
    /// inside the configured window at the moment the fetch is attempted must
    /// still be served stale even if the failing fetch itself takes longer
    /// than the remaining slack in that window -- the fetch's own latency
    /// must not count against the window.
    #[tokio::test]
    async fn a_finite_window_is_measured_before_the_slow_fetch_not_after() {
        let max_age = Duration::from_millis(100);
        let window = StaleIfError::For(Duration::from_millis(150));
        // Staleness at the moment the fetch is attempted: 50ms past the stale
        // point, comfortably inside the 150ms window.
        let stale_at_attempt_ms = 50;
        // The fetch itself takes 200ms to fail. Measured after the fetch,
        // staleness would appear to be 50ms + 200ms = 250ms, past the 150ms
        // window -- exactly the bug this test guards against.
        let fetch_delay = Duration::from_millis(200);

        #[expect(clippy::cast_possible_truncation)]
        let max_age_nanos = max_age.as_nanos() as i64;
        let stale_at_attempt_nanos = i64::from(stale_at_attempt_ms) * 1_000_000;
        let schema = MockHttpTableProvider::with_status(200, "unused").schema();
        let stale = stale_batch_with_fetched_at(
            &schema,
            "cached response",
            Some(now_nanos() - stale_at_attempt_nanos - max_age_nanos),
        );

        let outcome = slow_failure_outcome(stale, window, max_age, fetch_delay).await;
        assert_eq!(
            outcome,
            Ok("cached response".to_string()),
            "an entry inside the window when the fetch was attempted must be served \
             stale, even though the fetch's own {fetch_delay:?} delay would have pushed \
             a post-fetch staleness measurement past the window"
        );
    }

    #[tokio::test]
    async fn a_finite_window_is_measured_before_a_delayed_http_503() {
        let max_age = Duration::from_millis(100);
        let window = StaleIfError::For(Duration::from_millis(150));
        let fetch_delay = Duration::from_millis(200);
        let schema = MockHttpTableProvider::with_status(503, "upstream down").schema();
        assert_eq!(
            schema
                .metadata()
                .get(HTTP_RESPONSE_STATUS_METADATA_KEY)
                .map(String::as_str),
            Some("1"),
            "the origin batch must take the HTTP transient-status path"
        );
        let stale = stale_batch_with_fetched_at(
            &schema,
            "cached response",
            Some(
                now_nanos()
                    - i64::try_from((max_age + Duration::from_millis(50)).as_nanos())
                        .expect("age fits in nanoseconds"),
            ),
        );

        let outcome = transient_5xx_outcome_with_delay(stale, window, max_age, fetch_delay).await;
        assert_eq!(
            outcome, "cached response",
            "the delayed 503 must use staleness at fetch start, before its delay expires the window"
        );
    }

    /// `Enabled` has no bound to check, so it serves even when the entry's age is
    /// unknown — the fail-open behavior a finite window deliberately drops.
    #[tokio::test]
    async fn enabled_serves_even_when_staleness_is_unknown() {
        let http_source = Arc::new(MockHttpTableProvider::with_status(503, "upstream down"));
        let schema = http_source.schema();
        let null_ts = stale_batch_with_fetched_at(&schema, "cached response", None);
        assert_eq!(
            transient_5xx_outcome(null_ts, StaleIfError::Enabled, Duration::ZERO).await,
            "cached response",
            "Enabled fails open on an unknown fetch time"
        );
    }

    /// Cayenne stores `_fetched_at` in microseconds. The read path must
    /// normalize its precision, or a finite window could never prove an entry is
    /// inside `N` and would always fail closed on such an accelerator.
    #[tokio::test]
    async fn a_finite_window_reads_a_microsecond_fetched_at() {
        use arrow::array::{StringArray, TimestampMicrosecondArray, UInt16Array};

        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("request_path", DataType::Utf8, true),
            Field::new("request_query", DataType::Utf8, true),
            Field::new("content", DataType::Utf8, true),
            Field::new(RESPONSE_STATUS_COLUMN, DataType::UInt16, false),
            Field::new(
                CACHE_REFRESHED_AT_COLUMN,
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
        ]));

        // 30s past a 10s stale point is inside a 60s window. Stored in micros,
        // as Cayenne would.
        let fetched_at_micros = (now_nanos() - secs_nanos(10 + 30)) / 1_000;
        let stale = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["/api"])) as ArrayRef,
                Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef,
                Arc::new(StringArray::from(vec!["cached response"])) as ArrayRef,
                Arc::new(UInt16Array::from(vec![200_u16])) as ArrayRef,
                Arc::new(TimestampMicrosecondArray::from(vec![Some(
                    fetched_at_micros,
                )])) as ArrayRef,
            ],
        )
        .expect("batch");

        assert_eq!(
            transient_5xx_outcome(
                stale,
                StaleIfError::For(Duration::from_mins(1)),
                Duration::from_secs(10)
            )
            .await,
            "cached response",
            "a microsecond fetch time must be normalized and read as inside the window"
        );
    }

    #[tokio::test]
    async fn a_revalidation_reports_a_failing_origin_rather_than_zero_rows() {
        let http_source = Arc::new(MockHttpTableProvider::with_status(429, "slow down"));
        let in_flight: InFlightRevalidations =
            Arc::new(parking_lot::Mutex::new(std::collections::HashMap::new()));
        let accelerator = Arc::new(MockAcceleratorTableProvider::new(
            http_source.schema(),
            vec![],
        ));
        let (batch_write_tx, _handle) = spawn_test_cache_write_consumer(&accelerator, &in_flight);

        let outcome = CacheRefreshHelper::refresh_entry(
            Arc::clone(&http_source) as Arc<dyn TableProvider>,
            &test_session_state(),
            "test_dataset",
            &[col("content").eq(lit("test"))],
            CacheNamespace::Public,
            batch_write_tx,
            leader_claim(&in_flight, "key"),
        )
        .await
        .expect("revalidation should not error");

        assert_eq!(
            outcome,
            RevalidationOutcome::OriginUnavailable,
            "a 429 is a failing origin, not an origin with nothing to give"
        );
        assert_eq!(outcome.rows(), 0);
    }

    /// A reader that coalesces onto a fetch already in flight (a follower)
    /// replays the leader's published batches and never writes — so a key can
    /// never hold two copies of the same response.
    #[tokio::test]
    async fn a_follower_replays_the_leaders_batches_and_does_not_write() {
        use futures::StreamExt;

        let http_source = Arc::new(MockHttpTableProvider::with_status(200, "body"));
        let schema = http_source.schema();
        let accelerator = Arc::new(MockAcceleratorTableProvider::new(
            Arc::clone(&schema),
            vec![],
        ));
        let in_flight: InFlightRevalidations =
            Arc::new(parking_lot::Mutex::new(std::collections::HashMap::new()));
        let (batch_write_tx, handle) = spawn_test_cache_write_consumer(&accelerator, &in_flight);

        // Stand in for the leader: it holds the claim for this key and has
        // already published its fetched batches, exactly as the miss path does
        // before it enqueues the write.
        let filters = vec![col("content").eq(lit("test"))];
        let key = compute_cache_key_from_filters_and_namespace(
            &filters,
            CacheNamespace::Public.storage_id(),
        );
        let mut leader = leader_claim(&in_flight, &key);
        let published = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["/leader"])) as ArrayRef,
                Arc::new(StringArray::from(vec![""])) as ArrayRef,
                Arc::new(StringArray::from(vec!["leader-body"])) as ArrayRef,
                Arc::new(UInt16Array::from(vec![200_u16])) as ArrayRef,
                Arc::new(TimestampNanosecondArray::from(vec![Some(0_i64)])) as ArrayRef,
            ],
        )
        .expect("leader batch");
        leader.publish_ready(Arc::new(vec![published]));

        let mut stream = CacheRefreshHelper::handle_cache_miss(
            Arc::clone(&http_source) as Arc<dyn TableProvider>,
            &test_session_state(),
            "test_dataset",
            &filters,
            None,
            Arc::clone(&schema),
            false,
            false,
            StaleIfError::Disabled,
            Duration::ZERO,
            None,
            &tokio::runtime::Handle::current(),
            Arc::new(vec![].into()),
            batch_write_tx,
            CacheNamespace::Public,
            Arc::clone(&in_flight),
        )
        .await;

        // The follower is served the leader's published row, not the origin's.
        let mut served = Vec::new();
        while let Some(batch) = stream.next().await {
            served.push(batch.expect("stream"));
        }
        let rows: usize = served.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(rows, 1, "the follower replays the leader's one row");
        let content = served[0]
            .column_by_name("content")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .expect("content column");
        assert_eq!(
            content.value(0),
            "leader-body",
            "the follower must replay the leader's batches, not fetch its own"
        );

        drop(stream);
        drop(leader);
        // The follower's sender went with the miss, so the writer flushes
        // anything ever enqueued and exits.
        await_writer_exit(handle).await;

        assert!(
            accelerator.get_data().is_empty(),
            "the follower must not write a second copy of the response"
        );
    }

    /// Single-flight: N concurrent cache misses for one key reach the origin
    /// exactly once. The leader fetches; every other caller replays its batches.
    ///
    /// This fails on the pre-single-flight code, where each miss fetches the
    /// origin unconditionally (the in-flight set only suppressed the second
    /// *write*, not the second *fetch*), so the counter would read the number of
    /// callers rather than one.
    #[tokio::test]
    async fn concurrent_cache_misses_for_one_key_fetch_the_origin_once() {
        // Any delay makes the leader's scan await, and `join!` below drives all
        // five misses on one task: the followers reach `acquire` and coalesce
        // while the leader is parked inside its single scan.
        let origin = Arc::new(CountingHttpTableProvider::new(
            200,
            "shared-body",
            Duration::from_millis(1),
        ));
        let schema = origin.schema();
        let accelerator = Arc::new(MockAcceleratorTableProvider::new(
            Arc::clone(&schema),
            vec![],
        ));
        let in_flight: InFlightRevalidations =
            Arc::new(parking_lot::Mutex::new(std::collections::HashMap::new()));
        let (batch_write_tx, handle) = spawn_test_cache_write_consumer(&accelerator, &in_flight);
        let filters = vec![col("content").eq(lit("test"))];
        let io = tokio::runtime::Handle::current();
        let session_state = test_session_state();

        let make = || {
            CacheRefreshHelper::handle_cache_miss(
                Arc::clone(&origin) as Arc<dyn TableProvider>,
                &session_state,
                "test_dataset",
                &filters,
                None,
                Arc::clone(&schema),
                false,
                false,
                StaleIfError::Disabled,
                Duration::ZERO,
                None,
                &io,
                Arc::new(vec![].into()),
                batch_write_tx.clone(),
                CacheNamespace::Public,
                Arc::clone(&in_flight),
            )
        };

        // Driven on one task, `join!` interleaves the five futures: the first to
        // be polled inserts the claim and enters its scan (which then awaits),
        // so the other four find the claim present and become followers.
        let (s0, s1, s2, s3, s4) = tokio::join!(make(), make(), make(), make(), make());

        let all = [
            drain(s0).await,
            drain(s1).await,
            drain(s2).await,
            drain(s3).await,
            drain(s4).await,
        ];

        assert_eq!(
            origin.scan_count(),
            1,
            "five concurrent misses for one key must reach the origin exactly once"
        );

        for batches in &all {
            let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
            assert_eq!(rows, 1, "every caller is served the one shared row");
            let content = batches[0]
                .column_by_name("content")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>())
                .expect("content column");
            assert_eq!(
                content.value(0),
                "shared-body",
                "every caller sees the leader's fetched batches"
            );
        }

        // Single-flight writes once too: only the leader holds the claim. With
        // every sender gone the writer flushes all that was queued and exits.
        drop(batch_write_tx);
        await_writer_exit(handle).await;
        assert_eq!(
            stored_contents(&accelerator),
            vec!["shared-body".to_string()],
            "five misses for one key must cache its response exactly once"
        );
    }

    /// Single-flight applies to an empty origin too: N concurrent misses for a
    /// key the origin has no rows for reach it exactly once, and every caller is
    /// served the same empty result.
    #[tokio::test]
    async fn concurrent_cache_misses_for_an_empty_origin_fetch_it_once() {
        let origin = Arc::new(
            CountingHttpTableProvider::new(200, "unused", Duration::from_millis(200)).with_rows(0),
        );
        let schema = origin.schema();
        let accelerator = Arc::new(MockAcceleratorTableProvider::new(
            Arc::clone(&schema),
            vec![],
        ));
        let in_flight: InFlightRevalidations =
            Arc::new(parking_lot::Mutex::new(std::collections::HashMap::new()));
        let (batch_write_tx, handle) = spawn_test_cache_write_consumer(&accelerator, &in_flight);
        let filters = vec![col("content").eq(lit("test"))];
        let io = tokio::runtime::Handle::current();
        let session_state = test_session_state();

        let make = || {
            CacheRefreshHelper::handle_cache_miss(
                Arc::clone(&origin) as Arc<dyn TableProvider>,
                &session_state,
                "test_dataset",
                &filters,
                None,
                Arc::clone(&schema),
                false,
                false,
                StaleIfError::Disabled,
                Duration::ZERO,
                None,
                &io,
                Arc::new(vec![].into()),
                batch_write_tx.clone(),
                CacheNamespace::Public,
                Arc::clone(&in_flight),
            )
        };

        let (s0, s1, s2, s3, s4) = tokio::join!(make(), make(), make(), make(), make());

        let served = [
            drain_rows(s0).await,
            drain_rows(s1).await,
            drain_rows(s2).await,
            drain_rows(s3).await,
            drain_rows(s4).await,
        ];

        assert_eq!(
            origin.scan_count(),
            1,
            "five concurrent misses for a key the origin is empty for must reach it exactly once"
        );
        assert_eq!(
            served, [0; 5],
            "every caller is served the shared empty result"
        );

        // With every sender gone the writer flushes all that was queued and
        // exits, so the check covers every write ever enqueued.
        drop(batch_write_tx);
        await_writer_exit(handle).await;
        assert!(
            accelerator.get_data().is_empty(),
            "an empty result is not written to the cache"
        );
    }

    /// A miss that asks for more rows than the fetch already in flight for its
    /// key must not replay that fetch. The origin truncates a bounded fetch to
    /// its limit, so replaying a `LIMIT 1` fetch would hand a `LIMIT 3` caller
    /// one row where the origin holds three — a wrong result, not a slower one.
    /// The caller fetches for itself instead and, holding no claim, does not
    /// write.
    #[tokio::test]
    async fn a_miss_bounded_above_the_in_flight_fetch_does_not_replay_it() {
        let origin = Arc::new(
            CountingHttpTableProvider::new(200, "row", Duration::from_millis(200)).with_rows(5),
        );
        let schema = origin.schema();
        let accelerator = Arc::new(MockAcceleratorTableProvider::new(
            Arc::clone(&schema),
            vec![],
        ));
        let in_flight: InFlightRevalidations =
            Arc::new(parking_lot::Mutex::new(std::collections::HashMap::new()));
        let (batch_write_tx, handle) = spawn_test_cache_write_consumer(&accelerator, &in_flight);
        let filters = vec![col("content").eq(lit("test"))];
        let io = tokio::runtime::Handle::current();
        let session_state = test_session_state();

        let make = |limit| {
            CacheRefreshHelper::handle_cache_miss(
                Arc::clone(&origin) as Arc<dyn TableProvider>,
                &session_state,
                "test_dataset",
                &filters,
                limit,
                Arc::clone(&schema),
                false,
                false,
                StaleIfError::Disabled,
                Duration::ZERO,
                None,
                &io,
                Arc::new(vec![].into()),
                batch_write_tx.clone(),
                CacheNamespace::Public,
                Arc::clone(&in_flight),
            )
        };

        // Polled first, the `LIMIT 1` miss leads and is inside its scan when the
        // `LIMIT 3` miss reaches `acquire` for the same key.
        let (leader, follower) = tokio::join!(make(Some(1)), make(Some(3)));

        assert_eq!(
            drain_rows(leader).await,
            1,
            "the leader gets the one row it asked for"
        );
        assert_eq!(
            drain_rows(follower).await,
            3,
            "a miss asking for more rows than the in-flight fetch must fetch for itself"
        );
        assert_eq!(
            origin.scan_count(),
            2,
            "the bounded-below fetch cannot be shared, so the origin is asked twice"
        );

        drop(batch_write_tx);
        await_writer_exit(handle).await;
        assert_eq!(
            stored_contents(&accelerator),
            vec!["row".to_string()],
            "only the leader writes its one row; the caller that fetched for itself holds no claim"
        );
    }

    /// A stale-while-revalidate refresh holds the claim for its key across its
    /// origin fetch, so a miss for the same key arriving meanwhile coalesces onto
    /// it. The refresh must publish what it fetched: otherwise the miss waits out
    /// the whole refresh and then asks the origin a second time.
    #[tokio::test]
    async fn a_miss_during_a_revalidation_replays_the_revalidations_fetch() {
        let origin = Arc::new(CountingHttpTableProvider::new(
            200,
            "revalidated-body",
            Duration::from_millis(200),
        ));
        let schema = origin.schema();
        let accelerator = Arc::new(MockAcceleratorTableProvider::new(
            Arc::clone(&schema),
            vec![],
        ));
        let in_flight: InFlightRevalidations =
            Arc::new(parking_lot::Mutex::new(std::collections::HashMap::new()));
        let (batch_write_tx, handle) = spawn_test_cache_write_consumer(&accelerator, &in_flight);
        let filters = vec![col("content").eq(lit("test"))];

        // The revalidation claims the key the way `handle_cache_hit` does, then
        // enters its fetch.
        let claim = leader_claim(
            &in_flight,
            &compute_cache_key_from_filters_and_namespace(
                &filters,
                CacheNamespace::Public.storage_id(),
            ),
        );
        let revalidation = tokio::spawn({
            let origin = Arc::clone(&origin) as Arc<dyn TableProvider>;
            let filters = filters.clone();
            let batch_write_tx = batch_write_tx.clone();
            async move {
                CacheRefreshHelper::refresh_entry(
                    origin,
                    &test_session_state(),
                    "test_dataset",
                    &filters,
                    CacheNamespace::Public,
                    batch_write_tx,
                    claim,
                )
                .await
            }
        });
        wait_for_scans(&origin, 1).await;

        // The entry has expired past its stale window, so this reader misses.
        let served = drain(
            CacheRefreshHelper::handle_cache_miss(
                Arc::clone(&origin) as Arc<dyn TableProvider>,
                &test_session_state(),
                "test_dataset",
                &filters,
                None,
                Arc::clone(&schema),
                true,
                false,
                StaleIfError::Disabled,
                Duration::ZERO,
                None,
                &tokio::runtime::Handle::current(),
                Arc::new(vec![].into()),
                batch_write_tx.clone(),
                CacheNamespace::Public,
                Arc::clone(&in_flight),
            )
            .await,
        )
        .await;

        let outcome = revalidation
            .await
            .expect("revalidation task")
            .expect("revalidation");
        assert_eq!(outcome.rows(), 1, "the revalidation refreshed its one row");
        assert_eq!(
            origin.scan_count(),
            1,
            "a miss that coalesced onto a revalidation must not ask the origin again"
        );
        let rows: usize = served.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(rows, 1, "the miss is served the revalidation's row");
        let content = served[0]
            .column_by_name("content")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .expect("content column");
        assert_eq!(content.value(0), "revalidated-body");

        // The revalidation's write is the only one: the miss followed it and
        // holds no claim. With every sender gone the writer flushes and exits.
        drop(batch_write_tx);
        await_writer_exit(handle).await;
        assert_eq!(
            stored_contents(&accelerator),
            vec!["revalidated-body".to_string()],
            "the revalidation writes its one row and the coalesced miss writes nothing"
        );
    }

    /// The periodic stale-row refresh holds the claim for each entry across its
    /// origin fetch too, so a miss for that entry arriving meanwhile must replay
    /// the refresh's fetch rather than wait for it and then ask the origin again.
    #[tokio::test]
    async fn a_miss_during_the_periodic_refresh_replays_the_refreshs_fetch() {
        let origin = Arc::new(CountingHttpTableProvider::new(
            200,
            "refreshed-body",
            Duration::from_millis(200),
        ));
        let schema = origin.schema();

        #[expect(clippy::cast_possible_truncation)]
        let fetched_long_ago = (SystemTime::now() - Duration::from_hours(1))
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("clock")
            .as_nanos() as i64;
        let stale = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["/a"])) as ArrayRef,
                Arc::new(StringArray::from(vec![""])) as ArrayRef,
                Arc::new(StringArray::from(vec!["old-body"])) as ArrayRef,
                Arc::new(UInt16Array::from(vec![200_u16])) as ArrayRef,
                Arc::new(TimestampNanosecondArray::from(vec![Some(fetched_long_ago)])) as ArrayRef,
            ],
        )
        .expect("stale row");

        // The miss asks for the entry under exactly the filters the refresh
        // derives from the stale row, so both land on one key.
        let entries =
            CacheRefreshHelper::extract_unique_stale_entries(std::slice::from_ref(&stale))
                .expect("stale entries");
        let entry_filters = entries.first().expect("one stale entry").filters.clone();

        let stored = Arc::new(
            data_components::arrow::write::MemTable::try_new(
                Arc::clone(&schema),
                vec![vec![stale]],
            )
            .expect("mem table"),
        ) as Arc<dyn TableProvider>;
        let in_flight: InFlightRevalidations =
            Arc::new(parking_lot::Mutex::new(std::collections::HashMap::new()));

        let refresh = tokio::spawn({
            let origin = Arc::clone(&origin) as Arc<dyn TableProvider>;
            let stored = Arc::clone(&stored);
            let in_flight = Arc::clone(&in_flight);
            async move {
                CacheRefreshHelper::refresh_all_stale_rows(
                    origin,
                    stored,
                    test_session_state(),
                    "test_dataset",
                    Duration::from_secs(1),
                    Arc::new(Mutex::new(())),
                    in_flight,
                    create_cache_write_channel().0,
                )
                .await
            }
        });
        wait_for_scans(&origin, 1).await;

        let accelerator = Arc::new(MockAcceleratorTableProvider::new(
            Arc::clone(&schema),
            vec![],
        ));
        let (batch_write_tx, handle) = spawn_test_cache_write_consumer(&accelerator, &in_flight);
        let served = drain_rows(
            CacheRefreshHelper::handle_cache_miss(
                Arc::clone(&origin) as Arc<dyn TableProvider>,
                &test_session_state(),
                "test_dataset",
                &entry_filters,
                None,
                Arc::clone(&schema),
                true,
                false,
                StaleIfError::Disabled,
                Duration::ZERO,
                None,
                &tokio::runtime::Handle::current(),
                Arc::new(vec![].into()),
                batch_write_tx,
                CacheNamespace::Public,
                Arc::clone(&in_flight),
            )
            .await,
        )
        .await;

        let refreshed = refresh.await.expect("refresh task").expect("refresh");
        assert_eq!(refreshed, 1, "the periodic refresh refreshed its one row");
        assert_eq!(
            origin.scan_count(),
            1,
            "a miss that coalesced onto the periodic refresh must not ask the origin again"
        );
        assert_eq!(served, 1, "the miss is served the refresh's row");

        handle.abort();
    }

    /// A leader that drops before publishing a result (cancelled, failed, or
    /// holding a non-cacheable response) publishes `Failed`, so a follower
    /// waiting on it falls through to its own fetch instead of hanging forever.
    #[tokio::test]
    async fn a_follower_falls_through_when_the_leader_publishes_failed() {
        use futures::StreamExt;

        let origin = Arc::new(MockHttpTableProvider::with_status(200, "fell-through-body"));
        let schema = origin.schema();
        let in_flight: InFlightRevalidations =
            Arc::new(parking_lot::Mutex::new(std::collections::HashMap::new()));

        // A leader holds the claim; a follower clones its receiver before the
        // leader gives up.
        let leader = leader_claim(&in_flight, "k");
        let ClaimOutcome::Follower(in_flight_fetch) =
            CacheKeyClaim::acquire(&in_flight, "k".to_string(), None)
        else {
            panic!("second caller must be a follower");
        };

        // The leader is dropped without publishing a result: `Drop` publishes
        // `Failed`, which the follower's cloned receiver observes.
        drop(leader);

        let filters = [col("content").eq(lit("test"))];
        let session_state = test_session_state();
        let mut stream = CacheRefreshHelper::follow_cache_miss(
            in_flight_fetch.state,
            UncoalescedFetch {
                federated: Arc::clone(&origin) as Arc<dyn TableProvider>,
                session_state: &session_state,
                task_context: session_state.task_ctx(),
                dataset_name: "test_dataset",
                filters: &filters,
                limit: None,
                schema: Arc::clone(&schema),
                stale_if_error: StaleIfError::Disabled,
                max_age: Duration::ZERO,
                expired_batches: None,
            },
        )
        .await;

        let mut served = Vec::new();
        while let Some(b) = stream.next().await {
            served.push(b.expect("stream"));
        }
        let rows: usize = served.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(
            rows, 1,
            "a follower whose leader failed must fetch the origin itself"
        );
        let content = served[0]
            .column_by_name("content")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .expect("content column");
        assert_eq!(content.value(0), "fell-through-body");
    }

    /// A claim that is never handed to a write releases its key when dropped.
    #[tokio::test]
    async fn a_dropped_claim_releases_its_key() {
        // This is what makes a cancelled query safe. The miss path holds the
        // claim across the fetch and across the enqueue, so a client that
        // disconnects, or a send that blocks on a full write channel, drops the
        // future somewhere in the middle. Without release-on-drop that key
        // stays claimed for the life of the process and every later write and
        // revalidation for it is refused.
        let in_flight: InFlightRevalidations =
            Arc::new(parking_lot::Mutex::new(std::collections::HashMap::new()));

        {
            let claim = leader_claim(&in_flight, "k");
            assert_eq!(claim.key(), "k");
            assert!(
                matches!(
                    CacheKeyClaim::acquire(&in_flight, "k".to_string(), None),
                    ClaimOutcome::Follower(_)
                ),
                "a claimed key must make the next caller a follower, not a second leader"
            );
        }

        assert!(
            in_flight.lock().is_empty(),
            "dropping a claim must release its key"
        );
        assert!(
            matches!(
                CacheKeyClaim::acquire(&in_flight, "k".to_string(), None),
                ClaimOutcome::Leader(_)
            ),
            "the key is claimable again"
        );
    }

    /// A claim handed to a queued write is released by that write, not by the
    /// scope that raised it.
    #[tokio::test]
    async fn a_queued_claim_outlives_the_scope_that_raised_it() {
        let in_flight: InFlightRevalidations =
            Arc::new(parking_lot::Mutex::new(std::collections::HashMap::new()));

        {
            let claim = leader_claim(&in_flight, "k");
            claim.into_queued();
        }

        assert!(
            in_flight.lock().contains_key("k"),
            "the flush that writes this key is the one that releases it"
        );
    }

    /// The periodic refresh replaces the entries it refreshes, so it opens the
    /// same delete-then-append gap as every other writer and must hold the key.
    #[tokio::test]
    async fn the_periodic_refresh_skips_an_entry_whose_key_is_already_claimed() {
        // Without the claim, a reader scanning that gap sees no rows, reads a
        // miss, and appends its own copy beside this one — leaving the key
        // holding the response twice.
        let origin = Arc::new(MockHttpTableProvider::with_status(200, "fresh-body"));
        let schema = origin.schema();

        #[expect(clippy::cast_possible_truncation)]
        let fetched_long_ago = (SystemTime::now() - Duration::from_hours(1))
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("clock")
            .as_nanos() as i64;

        let stale = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["/a"])) as ArrayRef,
                Arc::new(StringArray::from(vec![""])) as ArrayRef,
                Arc::new(StringArray::from(vec!["good-body"])) as ArrayRef,
                Arc::new(arrow::array::UInt16Array::from(vec![200_u16])) as ArrayRef,
                Arc::new(TimestampNanosecondArray::from(vec![Some(fetched_long_ago)])) as ArrayRef,
            ],
        )
        .expect("stale row");

        // Claim the key exactly as the refresh will compute it.
        let entries =
            CacheRefreshHelper::extract_unique_stale_entries(std::slice::from_ref(&stale))
                .expect("stale entries");
        let entry = entries.first().expect("one stale entry");
        let namespace_id = entry
            .namespace
            .as_deref()
            .unwrap_or_else(|| CacheNamespace::Public.storage_id());
        let in_flight: InFlightRevalidations =
            Arc::new(parking_lot::Mutex::new(std::collections::HashMap::new()));
        let _held = leader_claim(
            &in_flight,
            &compute_cache_key_from_filters_and_namespace(&entry.filters, namespace_id),
        );

        let accelerator = Arc::new(
            data_components::arrow::write::MemTable::try_new(
                Arc::clone(&schema),
                vec![vec![stale]],
            )
            .expect("mem table"),
        ) as Arc<dyn TableProvider>;

        let refreshed = CacheRefreshHelper::refresh_all_stale_rows(
            Arc::clone(&origin) as Arc<dyn TableProvider>,
            Arc::clone(&accelerator),
            test_session_state(),
            "test_dataset",
            Duration::from_secs(1),
            Arc::new(Mutex::new(())),
            Arc::clone(&in_flight),
            create_cache_write_channel().0,
        )
        .await
        .expect("refresh");

        assert_eq!(
            refreshed, 0,
            "an entry another writer already holds must be left to them"
        );
    }

    /// The claim a miss takes is released by the flush that writes it, so a
    /// response that is never enqueued must never claim.
    #[tokio::test]
    async fn a_failing_origin_does_not_leave_the_key_claimed() {
        use futures::StreamExt;

        // A held claim refuses every later write *and* revalidation for that
        // key, for the life of the process — so one 5xx would make the key
        // permanently uncacheable, which is the opposite of what an operator
        // asking for a cache expects from a transient origin failure.
        let http_source = Arc::new(MockHttpTableProvider::with_status(503, "upstream down"));
        let schema = http_source.schema();
        let accelerator = Arc::new(MockAcceleratorTableProvider::new(
            Arc::clone(&schema),
            vec![],
        ));
        let in_flight: InFlightRevalidations =
            Arc::new(parking_lot::Mutex::new(std::collections::HashMap::new()));
        let (batch_write_tx, handle) = spawn_test_cache_write_consumer(&accelerator, &in_flight);

        let mut stream = CacheRefreshHelper::handle_cache_miss(
            Arc::clone(&http_source) as Arc<dyn TableProvider>,
            &test_session_state(),
            "test_dataset",
            &[col("content").eq(lit("test"))],
            None,
            Arc::clone(&schema),
            false,
            false,
            StaleIfError::Disabled,
            Duration::ZERO,
            None,
            &tokio::runtime::Handle::current(),
            Arc::new(vec![].into()),
            batch_write_tx,
            CacheNamespace::Public,
            Arc::clone(&in_flight),
        )
        .await;

        while let Some(batch) = stream.next().await {
            batch.expect("stream");
        }
        drop(stream);
        // The miss took the only sender with it, so the writer flushes anything
        // enqueued (and releases its claim) before it exits.
        await_writer_exit(handle).await;

        assert!(
            in_flight.lock().is_empty(),
            "a key whose response was never enqueued must not stay claimed"
        );
    }

    /// The periodic refresh replaces the entry it refreshes, so it must not
    /// write a failing origin's error body over the last good response.
    #[tokio::test]
    async fn a_failing_origin_does_not_overwrite_a_stale_entry_with_its_error_body() {
        // A 429 or 5xx arrives as a *successful* fetch whose rows carry that
        // status. Writing it would serve the origin's error body as a cache hit
        // until it expired, and would defeat `caching_stale_if_error` outright.
        let origin = Arc::new(MockHttpTableProvider::with_status(503, "upstream down"));
        let schema = origin.schema();

        #[expect(clippy::cast_possible_truncation)]
        let fetched_long_ago = (SystemTime::now() - Duration::from_hours(1))
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("clock")
            .as_nanos() as i64;

        let stale = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["/a"])) as ArrayRef,
                Arc::new(StringArray::from(vec![""])) as ArrayRef,
                Arc::new(StringArray::from(vec!["good-body"])) as ArrayRef,
                Arc::new(arrow::array::UInt16Array::from(vec![200_u16])) as ArrayRef,
                Arc::new(TimestampNanosecondArray::from(vec![Some(fetched_long_ago)])) as ArrayRef,
            ],
        )
        .expect("stale row");

        let accelerator = Arc::new(
            data_components::arrow::write::MemTable::try_new(
                Arc::clone(&schema),
                vec![vec![stale]],
            )
            .expect("mem table"),
        ) as Arc<dyn TableProvider>;

        let refreshed = CacheRefreshHelper::refresh_all_stale_rows(
            Arc::clone(&origin) as Arc<dyn TableProvider>,
            Arc::clone(&accelerator),
            test_session_state(),
            "test_dataset",
            Duration::from_secs(1),
            Arc::new(Mutex::new(())),
            Arc::new(parking_lot::Mutex::new(std::collections::HashMap::new())),
            create_cache_write_channel().0,
        )
        .await
        .expect("refresh");

        assert_eq!(refreshed, 0, "a failing origin refreshes nothing");

        let ctx = SessionContext::new();
        let batches = ctx
            .read_table(Arc::clone(&accelerator))
            .expect("read")
            .collect()
            .await
            .expect("collect");
        let bodies: Vec<String> = batches
            .iter()
            .flat_map(|batch| {
                let content = batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("utf8");
                (0..batch.num_rows())
                    .map(|r| content.value(r).to_string())
                    .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(
            bodies,
            vec!["good-body".to_string()],
            "the cached response must survive a failing origin"
        );
    }

    /// Test that 404 responses are cached.
    ///
    /// Simulates cache miss flow:
    /// 1. Create mock HTTP source (federated) and empty accelerator
    /// 2. Call `handle_cache_miss` (called when accelerator has no data for query)
    /// 3. Verify user receives 404 response data
    /// 4. Verify accelerator contains the 404 response
    #[tokio::test]
    async fn test_4xx_responses_are_cached() {
        use futures::StreamExt;

        // 1. Create mock HTTP source (returns 404) and empty accelerator
        let http_source = Arc::new(MockHttpTableProvider::with_status(404, "Not Found"));
        let schema = http_source.schema();

        let accelerator = Arc::new(MockAcceleratorTableProvider::new(
            Arc::clone(&schema),
            vec![],
        ));
        let in_flight: InFlightRevalidations =
            Arc::new(parking_lot::Mutex::new(std::collections::HashMap::new()));

        let (batch_write_tx, handle) = spawn_test_cache_write_consumer(&accelerator, &in_flight);

        // 2. Call handle_cache_miss - this is what happens when user queries and cache is empty
        let mut stream = CacheRefreshHelper::handle_cache_miss(
            Arc::clone(&http_source) as Arc<dyn TableProvider>,
            &test_session_state(),
            "test_dataset",
            &[col("content").eq(lit("test"))], // filters
            None,                              // limit
            Arc::clone(&schema),
            false,                  // is_expired
            false,                  // response_filtered
            StaleIfError::Disabled, // stale_if_error
            Duration::ZERO,         // max_age (ignored: no expired batches)
            None,                   // expired_batches
            &tokio::runtime::Handle::current(),
            Arc::new(vec![].into()), // synchronized_children
            batch_write_tx,
            CacheNamespace::Public,
            Arc::clone(&in_flight),
        )
        .await;

        // Collect user-visible results
        let mut user_batches = Vec::new();
        while let Some(result) = stream.next().await {
            user_batches.push(result.expect("stream should not error"));
        }

        // 3. Verify user receives the 404 response data
        assert_eq!(user_batches.len(), 1, "User should receive 1 batch");
        assert_eq!(user_batches[0].num_rows(), 1, "User should receive 1 row");

        let status_col = user_batches[0]
            .column(
                user_batches[0]
                    .schema()
                    .index_of(RESPONSE_STATUS_COLUMN)
                    .expect("column exists"),
            )
            .as_any()
            .downcast_ref::<UInt16Array>()
            .expect("status column");
        assert_eq!(status_col.value(0), 404, "User should see status 404");

        // The miss took the only sender with it, so the writer flushes what it
        // enqueued and exits.
        await_writer_exit(handle).await;

        // 4. Verify accelerator has the 404 response cached
        let cached_data = accelerator.get_data();
        assert!(
            !cached_data.is_empty(),
            "4xx responses SHOULD be in accelerator"
        );

        let cached_rows: usize = cached_data.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(cached_rows, 1, "Should have 1 cached row");

        // Verify cached data has status 404
        let cached_status = cached_data[0]
            .column(
                cached_data[0]
                    .schema()
                    .index_of(RESPONSE_STATUS_COLUMN)
                    .expect("column exists"),
            )
            .as_any()
            .downcast_ref::<UInt16Array>()
            .expect("status column");
        assert_eq!(
            cached_status.value(0),
            404,
            "Cached response should have status 404"
        );
    }

    /// Mock source that records the `DataFusion` session id each `scan()` is planned under, so a
    /// test can tell one shared `SessionState` from a fresh one per fetch.
    #[derive(Debug)]
    struct SessionTrackingTableProvider {
        schema: SchemaRef,
        data: Vec<RecordBatch>,
        session_ids: Arc<RwLock<Vec<String>>>,
    }

    impl SessionTrackingTableProvider {
        fn new(schema: SchemaRef, data: Vec<RecordBatch>) -> Self {
            Self {
                schema,
                data,
                session_ids: Arc::new(RwLock::new(Vec::new())),
            }
        }

        fn recorded_session_ids(&self) -> Vec<String> {
            self.session_ids.read().clone()
        }
    }

    #[async_trait]
    impl TableProvider for SessionTrackingTableProvider {
        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }

        fn table_type(&self) -> TableType {
            TableType::Base
        }

        async fn scan(
            &self,
            state: &dyn Session,
            _projection: Option<&Vec<usize>>,
            _filters: &[Expr],
            _limit: Option<usize>,
        ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
            self.session_ids
                .write()
                .push(state.session_id().to_string());
            Ok(Arc::new(DataSourceExec::new(Arc::new(
                MemorySourceConfig::try_new(
                    std::slice::from_ref(&self.data),
                    Arc::clone(&self.schema),
                    None,
                )?,
            ))))
        }
    }

    /// The columns a cached HTTP response carries, `cache_refreshed_at` included.
    fn http_cache_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("request_path", DataType::Utf8, true),
            Field::new("request_query", DataType::Utf8, true),
            Field::new("content", DataType::Utf8, true),
            Field::new(RESPONSE_STATUS_COLUMN, DataType::UInt16, false),
            Field::new(
                CACHE_REFRESHED_AT_COLUMN,
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
        ]))
    }

    /// One 200 response row whose `cache_refreshed_at` is `refreshed_at` (Unix nanoseconds).
    fn http_row(schema: &SchemaRef, refreshed_at: i64, content: &str) -> RecordBatch {
        RecordBatch::try_new(
            Arc::clone(schema),
            vec![
                Arc::new(StringArray::from(vec!["/api/test"])),
                Arc::new(StringArray::from(vec!["q=test"])),
                Arc::new(StringArray::from(vec![content])),
                Arc::new(UInt16Array::from(vec![200_u16])),
                Arc::new(TimestampNanosecondArray::from(vec![Some(refreshed_at)])),
            ],
        )
        .expect("http response row")
    }

    /// Unix nanoseconds for `ago` before `now_nanos`.
    fn nanos_ago(now_nanos: i64, ago: Duration) -> i64 {
        now_nanos - i64::try_from(ago.as_nanos()).expect("duration fits in i64 nanoseconds")
    }

    #[derive(Debug)]
    struct CountingCacheScan {
        inner: Arc<dyn ExecutionPlan>,
        scans: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl DisplayAs for CountingCacheScan {
        fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
            self.inner.fmt_as(t, f)
        }
    }

    impl ExecutionPlan for CountingCacheScan {
        fn name(&self) -> &'static str {
            "CountingCacheScan"
        }

        fn schema(&self) -> SchemaRef {
            self.inner.schema()
        }

        fn properties(&self) -> &Arc<PlanProperties> {
            self.inner.properties()
        }

        fn apply_expressions(
            &self,
            _f: &mut dyn FnMut(
                &Arc<dyn datafusion::physical_plan::PhysicalExpr>,
            ) -> datafusion::error::Result<
                datafusion::common::tree_node::TreeNodeRecursion,
            >,
        ) -> datafusion::error::Result<datafusion::common::tree_node::TreeNodeRecursion> {
            Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
        }

        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![&self.inner]
        }

        fn with_new_children(
            self: Arc<Self>,
            children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
            Ok(Arc::new(Self {
                inner: Arc::clone(&children[0]),
                scans: Arc::clone(&self.scans),
            }))
        }

        fn execute(
            &self,
            partition: usize,
            context: Arc<TaskContext>,
        ) -> DataFusionResult<SendableRecordBatchStream> {
            self.scans.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.inner.execute(partition, context)
        }
    }

    /// Counts accelerator executions through the actual caching scan plan.
    /// A backend-first success must not execute its child, whereas an origin
    /// failure reads it once, and the disabled and nonzero-TTL paths retain
    /// their normal cache-first scan.
    #[tokio::test]
    async fn zero_ttl_backend_first_only_scans_on_failure() {
        let schema = http_cache_schema();
        let now = i64::try_from(
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .expect("time")
                .as_nanos(),
        )
        .expect("timestamp");
        let old_row = http_row(&schema, nanos_ago(now, Duration::from_secs(2)), "cached");
        let filters = vec![col("request_path").eq(lit("/api/test"))];

        for (ttl, swr, sie, status, expected_scans, expected_content) in [
            (
                Duration::ZERO,
                None,
                StaleIfError::For(Duration::from_mins(1)),
                200,
                0,
                "origin",
            ),
            (
                Duration::ZERO,
                None,
                StaleIfError::For(Duration::from_mins(1)),
                503,
                1,
                "cached",
            ),
            (
                Duration::ZERO,
                None,
                StaleIfError::Disabled,
                503,
                1,
                "error",
            ),
            (
                Duration::from_mins(1),
                None,
                StaleIfError::Enabled,
                200,
                1,
                "cached",
            ),
            (
                Duration::ZERO,
                Some(Duration::from_mins(1)),
                StaleIfError::Enabled,
                200,
                1,
                "cached",
            ),
        ] {
            let source = Arc::new(MockHttpTableProvider::with_status(
                status,
                if status == 200 { "origin" } else { "error" },
            ));
            let accelerator = Arc::new(MockAcceleratorTableProvider::new(
                Arc::clone(&schema),
                vec![old_row.clone()],
            ));
            let inner: Arc<dyn ExecutionPlan> = Arc::new(DataSourceExec::new(Arc::new(
                MemorySourceConfig::try_new(&[vec![old_row.clone()]], Arc::clone(&schema), None)
                    .expect("cache input"),
            )));
            let scans = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let input: Arc<dyn ExecutionPlan> = Arc::new(CountingCacheScan {
                inner,
                scans: Arc::clone(&scans),
            });
            let in_flight: InFlightRevalidations =
                Arc::new(parking_lot::Mutex::new(HashMap::new()));
            let (tx, _consumer) = spawn_test_cache_write_consumer(&accelerator, &in_flight);
            let exec = CachingAccelerationScanExec::new(
                input,
                Some(ttl),
                swr,
                sie,
                source,
                accelerator,
                "http_data".to_string(),
                Handle::current(),
                filters.clone(),
                None,
                None,
                Arc::new(Mutex::new(())),
                in_flight,
                Arc::new(tokio::sync::RwLock::new(Vec::new())),
                tx,
            );
            let batches: Vec<RecordBatch> = exec
                .execute(0, Arc::new(TaskContext::default()))
                .expect("execute")
                .try_collect()
                .await
                .expect("collect");
            let content = batches[0]
                .column_by_name("content")
                .expect("content")
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("string content")
                .value(0);
            assert_eq!(
                content, expected_content,
                "ttl={ttl:?} swr={swr:?} sie={sie:?}"
            );
            assert_eq!(
                scans.load(std::sync::atomic::Ordering::SeqCst),
                expected_scans,
                "ttl={ttl:?} swr={swr:?} sie={sie:?}"
            );
        }
    }

    #[tokio::test]
    async fn follower_reads_deferred_cache_fallback_after_origin_failure() {
        let origin = Arc::new(MockHttpTableProvider::with_status(503, "origin error"));
        let schema = origin.schema();
        let stale = http_row(&schema, 0, "cached");
        let inner: Arc<dyn ExecutionPlan> = Arc::new(DataSourceExec::new(Arc::new(
            MemorySourceConfig::try_new(&[vec![stale]], Arc::clone(&schema), None)
                .expect("cache input"),
        )));
        let scans = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let input: Arc<dyn ExecutionPlan> = Arc::new(CountingCacheScan {
            inner,
            scans: Arc::clone(&scans),
        });
        let in_flight: InFlightRevalidations = Arc::new(parking_lot::Mutex::new(HashMap::new()));
        let leader = leader_claim(&in_flight, "request");
        let ClaimOutcome::Follower(in_flight_fetch) =
            CacheKeyClaim::acquire(&in_flight, "request".to_string(), None)
        else {
            panic!("second caller must be a follower");
        };
        drop(leader);

        let session_state = test_session_state();
        let filters = [col("request_path").eq(lit("/api/test"))];
        let stream = CacheRefreshHelper::follow_cache_miss(
            in_flight_fetch.state,
            UncoalescedFetch {
                federated: Arc::clone(&origin) as Arc<dyn TableProvider>,
                session_state: &session_state,
                dataset_name: "http_data",
                filters: &filters,
                limit: None,
                schema: Arc::clone(&schema),
                stale_if_error: StaleIfError::Enabled,
                max_age: Duration::ZERO,
                expired_batches: Some(CacheFallback::Deferred {
                    input: input.into(),
                    partition: 0,
                    context: Arc::new(TaskContext::default()),
                }),
                task_context: session_state.task_ctx(),
            },
        )
        .await;
        let batches: Vec<RecordBatch> = stream.try_collect().await.expect("collect fallback");

        assert_eq!(batches.len(), 1);
        let content = batches[0]
            .column_by_name("content")
            .expect("content")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string content");
        assert_eq!(content.value(0), "cached");
        assert_eq!(
            scans.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "the deferred accelerator scan runs when the origin fails"
        );
    }

    #[tokio::test]
    async fn follower_does_not_serve_zero_row_deferred_fallback() {
        let origin = Arc::new(MockHttpTableProvider::with_status(503, "origin error"));
        let schema = origin.schema();
        let empty = RecordBatch::new_empty(Arc::clone(&schema));
        let inner: Arc<dyn ExecutionPlan> = Arc::new(DataSourceExec::new(Arc::new(
            MemorySourceConfig::try_new(&[vec![empty]], Arc::clone(&schema), None)
                .expect("empty cache input"),
        )));
        let scans = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let input: Arc<dyn ExecutionPlan> = Arc::new(CountingCacheScan {
            inner,
            scans: Arc::clone(&scans),
        });
        let in_flight: InFlightRevalidations = Arc::new(parking_lot::Mutex::new(HashMap::new()));
        let leader = leader_claim(&in_flight, "request");
        let ClaimOutcome::Follower(in_flight_fetch) =
            CacheKeyClaim::acquire(&in_flight, "request".to_string(), None)
        else {
            panic!("second caller must be a follower");
        };
        drop(leader);

        let session_state = test_session_state();
        let filters = [col("request_path").eq(lit("/api/test"))];
        let stream = CacheRefreshHelper::follow_cache_miss(
            in_flight_fetch.state,
            UncoalescedFetch {
                federated: Arc::clone(&origin) as Arc<dyn TableProvider>,
                session_state: &session_state,
                dataset_name: "http_data",
                filters: &filters,
                limit: None,
                schema: Arc::clone(&schema),
                stale_if_error: StaleIfError::Enabled,
                max_age: Duration::ZERO,
                expired_batches: Some(CacheFallback::Deferred {
                    input: input.into(),
                    partition: 0,
                    context: Arc::new(TaskContext::default()),
                }),
                task_context: session_state.task_ctx(),
            },
        )
        .await;
        let batches: Vec<RecordBatch> = stream.try_collect().await.expect("collect origin failure");

        assert_eq!(
            batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
            1,
            "an empty cache batch cannot suppress the origin's transient response"
        );
        let status = batches[0]
            .column_by_name(RESPONSE_STATUS_COLUMN)
            .expect("response_status")
            .as_any()
            .downcast_ref::<UInt16Array>()
            .expect("UInt16 response_status");
        assert_eq!(status.value(0), 503);
        assert_eq!(
            scans.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "the deferred accelerator scan runs once after origin failure"
        );
    }

    /// Regression guard for the shared `SessionState`.  Every query plans its own
    /// `CachingAccelerationScanExec` (through `scan_plan`, and again through
    /// `with_new_children` on a plan rewrite), and each source fetch that exec issues — a cache
    /// miss, an expired entry re-fetched inline, and a stale-while-revalidate refresh in the
    /// background — must plan under the one process-wide session. A fresh `SessionContext` per
    /// fetch, or a fresh `SessionState` per exec, gives every `scan()` its own session id; this
    /// asserts on the ids rather than on timings, so it holds on a loaded CI runner.
    #[tokio::test]
    async fn source_fetches_across_execs_and_paths_share_one_session_state() {
        let schema = http_cache_schema();
        let now_nanos = i64::try_from(
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .expect("time went backwards")
                .as_nanos(),
        )
        .expect("now fits in i64 nanoseconds");
        let max_age = Duration::from_mins(1);
        let stale_while_revalidate = Duration::from_mins(5);

        let source = Arc::new(SessionTrackingTableProvider::new(
            Arc::clone(&schema),
            vec![http_row(&schema, now_nanos, "from source")],
        ));
        let accelerator = Arc::new(MockAcceleratorTableProvider::new(
            Arc::clone(&schema),
            vec![],
        ));
        let in_flight_revalidations: InFlightRevalidations =
            Arc::new(parking_lot::Mutex::new(HashMap::new()));
        let (batch_write_tx, _consumer_handle) =
            spawn_test_cache_write_consumer(&accelerator, &in_flight_revalidations);

        let build_exec = |input: Arc<dyn ExecutionPlan>, filters: Vec<Expr>| {
            Arc::new(CachingAccelerationScanExec::new(
                input,
                Some(max_age),
                Some(stale_while_revalidate),
                StaleIfError::Disabled,
                Arc::clone(&source) as Arc<dyn TableProvider>,
                Arc::clone(&accelerator) as Arc<dyn TableProvider>,
                "test_dataset".to_string(),
                Handle::current(),
                filters,
                None,
                None,
                Arc::new(Mutex::new(())),
                Arc::clone(&in_flight_revalidations),
                Arc::new(tokio::sync::RwLock::new(Vec::new())),
                batch_write_tx.clone(),
            ))
        };
        let cached_input = |rows: Vec<RecordBatch>| -> Arc<dyn ExecutionPlan> {
            Arc::new(DataSourceExec::new(Arc::new(
                MemorySourceConfig::try_new(&[rows], Arc::clone(&schema), None)
                    .expect("cached rows as a memory source"),
            )))
        };

        // What the accelerator holds for each query: nothing (miss, twice), a row past
        // `max_age + stale_while_revalidate` (expired: re-fetched inline), and a row past
        // `max_age` but inside the window (stale: served, refreshed in the background).
        let expired_at = nanos_ago(
            now_nanos,
            max_age + stale_while_revalidate + Duration::from_mins(1),
        );
        let stale_at = nanos_ago(now_nanos, max_age + Duration::from_mins(1));
        let cases: Vec<(&str, Vec<RecordBatch>)> = vec![
            ("miss", vec![]),
            ("second miss", vec![]),
            ("expired", vec![http_row(&schema, expired_at, "expired")]),
            ("stale", vec![http_row(&schema, stale_at, "stale")]),
        ];

        for (i, (case, cached_rows)) in cases.into_iter().enumerate() {
            // One key per case, so no case is skipped for a write another case still has pending.
            let filters = vec![col("request_path").eq(lit(format!("/api/{i}")))];
            let exec = build_exec(cached_input(cached_rows), filters);
            let rows: Vec<RecordBatch> = exec
                .execute(0, Arc::new(TaskContext::default()))
                .expect("execute")
                .try_collect()
                .await
                .expect("collect");
            assert!(!rows.is_empty(), "{case}: the scan must return rows");
        }

        // A plan rewrite rebuilds the exec through `with_new_children`; that copy fetches too.
        let rewritten = build_exec(
            cached_input(vec![]),
            vec![col("request_path").eq(lit("/api/rewritten"))],
        )
        .replace_children(
            vec![cached_input(vec![])],
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )
        .expect("with_new_children");
        let rows: Vec<RecordBatch> = rewritten
            .execute(0, Arc::new(TaskContext::default()))
            .expect("execute rewritten exec")
            .try_collect()
            .await
            .expect("collect rewritten exec");
        assert!(
            !rows.is_empty(),
            "rewritten exec: the scan must return rows"
        );

        // Four fetches happen inline before their streams end; the stale case's refresh runs on
        // the io runtime, so wait for it — bounded, and naming what was seen if it never lands.
        let expected_fetches = 5;
        let refresh_landed = tokio::time::timeout(Duration::from_secs(10), async {
            while source.recorded_session_ids().len() < expected_fetches {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        assert!(
            refresh_landed.is_ok(),
            "the stale-while-revalidate refresh never reached the source: saw {} of {expected_fetches} fetches",
            source.recorded_session_ids().len()
        );

        let session_ids = source.recorded_session_ids();
        assert_eq!(
            session_ids.len(),
            expected_fetches,
            "one source fetch per case, got {session_ids:?}"
        );
        let distinct: HashSet<&str> = session_ids.iter().map(String::as_str).collect();
        assert_eq!(
            distinct.len(),
            1,
            "every fetch must plan under one shared session state; saw {distinct:?}"
        );
        assert_eq!(
            session_ids[0],
            SHARED_SESSION_STATE.session_id(),
            "fetches must use the process-wide state, not a copy built per exec"
        );
    }

    /// Helper to create a schema with `response_status` column for `filter_5xx` tests.
    /// Carries the HTTP-connector provenance marker so
    /// `filter_transient_error_responses` treats it as a real HTTP-connector
    /// batch rather than passing it through unfiltered (see
    /// `cache::utils::http_fetch_status`).
    fn create_http_response_schema() -> SchemaRef {
        Arc::new(
            Schema::new(vec![
                Field::new("content", DataType::Utf8, false),
                Field::new(RESPONSE_STATUS_COLUMN, DataType::UInt16, false),
            ])
            .with_metadata(std::collections::HashMap::from([(
                HTTP_RESPONSE_STATUS_METADATA_KEY.to_string(),
                "1".to_string(),
            )])),
        )
    }

    #[test]
    fn test_filter_5xx_responses_keeps_2xx() {
        let schema = create_http_response_schema();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["ok1", "ok2", "ok3"])),
                Arc::new(UInt16Array::from(vec![200, 201, 204])),
            ],
        )
        .expect("to create batch");

        let result = cache::filter_transient_error_responses(&[batch]);

        assert_eq!(result.len(), 1, "Should have 1 batch");
        assert_eq!(result[0].num_rows(), 3, "All 2xx rows should be kept");
    }

    #[test]
    fn test_filter_5xx_responses_keeps_4xx() {
        let schema = create_http_response_schema();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec![
                    "not found",
                    "bad request",
                    "forbidden",
                ])),
                Arc::new(UInt16Array::from(vec![404, 400, 403])),
            ],
        )
        .expect("to create batch");

        let result = cache::filter_transient_error_responses(&[batch]);

        assert_eq!(result.len(), 1, "Should have 1 batch");
        assert_eq!(result[0].num_rows(), 3, "All 4xx rows should be kept");
    }

    #[test]
    fn test_filter_5xx_responses_removes_5xx() {
        let schema = create_http_response_schema();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["error1", "error2", "error3"])),
                Arc::new(UInt16Array::from(vec![500, 502, 503])),
            ],
        )
        .expect("to create batch");

        let result = cache::filter_transient_error_responses(&[batch]);

        assert!(result.is_empty(), "All 5xx rows should be filtered out");
    }

    #[test]
    fn test_filter_5xx_responses_mixed_status_codes() {
        let schema = create_http_response_schema();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec![
                    "ok",
                    "not found",
                    "server error",
                    "created",
                ])),
                Arc::new(UInt16Array::from(vec![200, 404, 500, 201])),
            ],
        )
        .expect("to create batch");

        let result = cache::filter_transient_error_responses(&[batch]);

        assert_eq!(result.len(), 1, "Should have 1 batch");
        assert_eq!(result[0].num_rows(), 3, "Should keep 3 non-5xx rows");

        // Verify the content column has the expected values
        let content = result[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("content column");
        assert_eq!(content.value(0), "ok");
        assert_eq!(content.value(1), "not found");
        assert_eq!(content.value(2), "created");
    }

    #[test]
    fn test_filter_5xx_responses_empty_batches() {
        let result = cache::filter_transient_error_responses(&[]);
        assert!(result.is_empty(), "Empty input should return empty output");
    }

    #[test]
    fn test_filter_5xx_responses_multiple_batches() {
        let schema = create_http_response_schema();

        let batch1 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["ok"])),
                Arc::new(UInt16Array::from(vec![200])),
            ],
        )
        .expect("to create batch1");

        let batch2 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["error"])),
                Arc::new(UInt16Array::from(vec![500])),
            ],
        )
        .expect("to create batch2");

        let batch3 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["not found"])),
                Arc::new(UInt16Array::from(vec![404])),
            ],
        )
        .expect("to create batch3");

        let result = cache::filter_transient_error_responses(&[batch1, batch2, batch3]);

        assert_eq!(
            result.len(),
            2,
            "Should have 2 batches (batch2 filtered out entirely)"
        );
        assert_eq!(result[0].num_rows(), 1, "First batch should have 1 row");
        assert_eq!(
            result[1].num_rows(),
            1,
            "Second kept batch should have 1 row"
        );
    }

    #[test]
    fn test_filter_5xx_responses_boundary_status_codes() {
        let schema = create_http_response_schema();
        // Test boundary: 499 (kept), 500 (filtered), 599 (filtered), 600 (kept - not 5xx)
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["499", "500", "599", "600"])),
                Arc::new(UInt16Array::from(vec![499, 500, 599, 600])),
            ],
        )
        .expect("to create batch");

        let result = cache::filter_transient_error_responses(&[batch]);

        assert_eq!(result.len(), 1, "Should have 1 batch");
        assert_eq!(result[0].num_rows(), 2, "Should keep 499 and 600");

        let status = result[0]
            .column(1)
            .as_any()
            .downcast_ref::<UInt16Array>()
            .expect("status column");
        assert_eq!(status.value(0), 499);
        assert_eq!(status.value(1), 600);
    }

    #[test]
    fn test_filter_transient_removes_429() {
        let schema = create_http_response_schema();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["rate limited"])),
                Arc::new(UInt16Array::from(vec![429])),
            ],
        )
        .expect("to create batch");

        let result = cache::filter_transient_error_responses(&[batch]);

        assert!(
            result.is_empty(),
            "429 Too Many Requests should be filtered out"
        );
    }

    #[test]
    fn test_filter_transient_mixed_with_429() {
        let schema = create_http_response_schema();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec![
                    "ok",
                    "rate limited",
                    "server error",
                    "not found",
                ])),
                Arc::new(UInt16Array::from(vec![200, 429, 500, 404])),
            ],
        )
        .expect("to create batch");

        let result = cache::filter_transient_error_responses(&[batch]);

        assert_eq!(result.len(), 1, "Should have 1 batch");
        assert_eq!(
            result[0].num_rows(),
            2,
            "Should keep only 200 and 404, filtering 429 and 500"
        );

        let status = result[0]
            .column(1)
            .as_any()
            .downcast_ref::<UInt16Array>()
            .expect("status column");
        assert_eq!(status.value(0), 200);
        assert_eq!(status.value(1), 404);
    }
}

/// The caching write path must replace a cache entry by telling the engine to
/// delete the superseded rows, not by reading the whole acceleration back,
/// filtering it in memory and overwriting it. These tests watch the calls the
/// accelerator actually receives, because "the right rows ended up stored" is
/// true of both strategies and would not distinguish them.
#[cfg(test)]
mod write_path_tests {
    use super::*;
    use arrow::array::StringArray;
    use arrow::datatypes::{Field, Schema};
    use async_trait::async_trait;
    use datafusion::catalog::{Session, TableProvider};
    use datafusion::common::Constraints;
    use datafusion::datasource::TableType;
    use datafusion::prelude::SessionContext;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Wraps a real accelerator and records which operations it was asked for.
    /// `can_delete` models an engine without `delete_from` — the caller must
    /// fall back rather than fail.
    #[derive(Debug)]
    struct CountingAccelerator {
        inner: Arc<dyn TableProvider>,
        scans: AtomicUsize,
        deletes: AtomicUsize,
        overwrites: AtomicUsize,
        appends: AtomicUsize,
        can_delete: bool,
    }

    impl CountingAccelerator {
        fn new(inner: Arc<dyn TableProvider>, can_delete: bool) -> Self {
            Self {
                inner,
                scans: AtomicUsize::new(0),
                deletes: AtomicUsize::new(0),
                overwrites: AtomicUsize::new(0),
                appends: AtomicUsize::new(0),
                can_delete,
            }
        }
    }

    #[async_trait]
    impl TableProvider for CountingAccelerator {
        fn schema(&self) -> SchemaRef {
            self.inner.schema()
        }

        fn constraints(&self) -> Option<&Constraints> {
            self.inner.constraints()
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
        ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
            self.scans.fetch_add(1, Ordering::Relaxed);
            self.inner.scan(state, projection, filters, limit).await
        }

        async fn insert_into(
            &self,
            state: &dyn Session,
            input: Arc<dyn ExecutionPlan>,
            op: InsertOp,
        ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
            match op {
                InsertOp::Overwrite => self.overwrites.fetch_add(1, Ordering::Relaxed),
                _ => self.appends.fetch_add(1, Ordering::Relaxed),
            };
            self.inner.insert_into(state, input, op).await
        }

        async fn delete_from(
            &self,
            state: &dyn Session,
            filters: Vec<Expr>,
        ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
            if !self.can_delete {
                return Err(DataFusionError::NotImplemented(
                    "Delete not implemented for this table".to_string(),
                ));
            }
            self.deletes.fetch_add(1, Ordering::Relaxed);
            self.inner.delete_from(state, filters).await
        }
    }

    fn cache_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("request_path", DataType::Utf8, false),
            Field::new("content", DataType::Utf8, false),
        ]))
    }

    fn entry(path: &str, content: &str) -> RecordBatch {
        RecordBatch::try_new(
            cache_schema(),
            vec![
                Arc::new(StringArray::from(vec![path])) as ArrayRef,
                Arc::new(StringArray::from(vec![content])) as ArrayRef,
            ],
        )
        .expect("batch")
    }

    fn accelerator_with(
        rows: Vec<RecordBatch>,
        can_delete: bool,
    ) -> (Arc<CountingAccelerator>, Arc<dyn TableProvider>) {
        let inner = data_components::arrow::write::MemTable::try_new(cache_schema(), vec![rows])
            .expect("mem table");
        let counting = Arc::new(CountingAccelerator::new(
            Arc::new(inner) as Arc<dyn TableProvider>,
            can_delete,
        ));
        let as_provider = Arc::clone(&counting) as Arc<dyn TableProvider>;
        (counting, as_provider)
    }

    async fn stored(accelerator: &Arc<dyn TableProvider>) -> Vec<(String, String)> {
        let ctx = SessionContext::new();
        let batches = ctx
            .read_table(Arc::clone(accelerator))
            .expect("read")
            .collect()
            .await
            .expect("collect");
        let mut out = Vec::new();
        for batch in &batches {
            let paths = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("utf8");
            let contents = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("utf8");
            for r in 0..batch.num_rows() {
                out.push((paths.value(r).to_string(), contents.value(r).to_string()));
            }
        }
        out.sort();
        out
    }

    /// Runs the real batched writer over `accelerator` until the sender drops.
    fn spawn_test_writer(
        accelerator: &Arc<dyn TableProvider>,
    ) -> (CacheWriteSender, tokio::task::JoinHandle<()>) {
        let (tx, rx) = create_cache_write_channel();
        let handle = spawn_batched_cache_write_task(
            rx,
            Arc::clone(accelerator),
            TableReference::bare("test_dataset"),
            Arc::new(Mutex::new(())),
            Arc::new(parking_lot::Mutex::new(std::collections::HashMap::new())),
            Arc::new(AtomicI64::new(0)),
            RuntimeStatus::new(),
        );
        (tx, handle)
    }

    #[tokio::test]
    async fn a_fresh_insert_appends_without_deleting() {
        // A key nothing holds yet cannot collide, so the delete would match
        // nothing — and on Cayenne a delete first checkpoints the inline
        // memtable to a file, which collapsed measured ingest from thousands of
        // entries per second to about ten when every write did one.
        let (counting, accelerator) = accelerator_with(vec![], /* can_delete */ true);

        let (tx, handle) = spawn_test_writer(&accelerator);
        tx.send(CacheWriteRequest {
            batches: vec![entry("/new", "first")],
            filters: vec![col("request_path").eq(lit("/new"))],
            cache_key: "key".to_string(),
            namespace_id: "public".into(),
            replaces_existing: false,
        })
        .await
        .expect("send");
        drop(tx);
        handle.await.expect("writer");

        assert_eq!(
            counting.deletes.load(Ordering::Relaxed),
            0,
            "a fresh key must not pay for a delete"
        );
        assert_eq!(counting.appends.load(Ordering::Relaxed), 1);
        assert_eq!(
            stored(&accelerator).await,
            vec![("/new".to_string(), "first".to_string())]
        );
    }

    #[tokio::test]
    async fn two_writes_for_one_key_in_a_flush_store_the_newest_only() {
        // Both are raised as fresh inserts, so neither deletes. Appending both
        // would leave the key holding the response twice, which queries return
        // as duplicated source rows.
        let (_counting, accelerator) = accelerator_with(vec![], /* can_delete */ true);

        let (tx, handle) = spawn_test_writer(&accelerator);
        for content in ["older", "newer"] {
            tx.send(CacheWriteRequest {
                batches: vec![entry("/a", content)],
                filters: vec![col("request_path").eq(lit("/a"))],
                cache_key: "same-key".to_string(),
                namespace_id: "public".into(),
                replaces_existing: false,
            })
            .await
            .expect("send");
        }
        drop(tx);
        handle.await.expect("writer");

        assert_eq!(
            stored(&accelerator).await,
            vec![("/a".to_string(), "newer".to_string())],
            "the key must hold one response, and it must be the newest"
        );
    }

    #[tokio::test]
    async fn replacing_an_entry_deletes_first() {
        let (counting, accelerator) =
            accelerator_with(vec![entry("/a", "old-a"), entry("/b", "b")], true);

        let (tx, handle) = spawn_test_writer(&accelerator);
        tx.send(CacheWriteRequest {
            batches: vec![entry("/a", "new-a")],
            filters: vec![col("request_path").eq(lit("/a"))],
            cache_key: "key".to_string(),
            namespace_id: "public".into(),
            replaces_existing: true,
        })
        .await
        .expect("send");
        drop(tx);
        handle.await.expect("writer");

        assert_eq!(counting.deletes.load(Ordering::Relaxed), 1);
        assert_eq!(
            stored(&accelerator).await,
            vec![
                ("/a".to_string(), "new-a".to_string()),
                ("/b".to_string(), "b".to_string()),
            ],
            "the key holds the new response only"
        );
    }

    #[tokio::test]
    async fn replacing_an_entry_deletes_and_appends_without_reading_the_table() {
        let (counting, accelerator) = accelerator_with(
            vec![entry("/a", "old-a"), entry("/b", "b")],
            /* can_delete */ true,
        );

        CacheRefreshHelper::batched_upsert_into_accelerator(
            &accelerator,
            "test_dataset",
            &[vec![col("request_path").eq(lit("/a"))]],
            vec![entry("/a", "new-a")],
        )
        .await
        .expect("upsert");

        assert_eq!(
            counting.deletes.load(Ordering::Relaxed),
            1,
            "the superseded rows must be removed by the engine"
        );
        assert_eq!(
            counting.appends.load(Ordering::Relaxed),
            1,
            "the replacement rows must be appended"
        );
        assert_eq!(
            counting.overwrites.load(Ordering::Relaxed),
            0,
            "replacing one entry must not rewrite the whole acceleration"
        );
        assert_eq!(
            counting.scans.load(Ordering::Relaxed),
            0,
            "replacing one entry must not read every cached row back first"
        );

        assert_eq!(
            stored(&accelerator).await,
            vec![
                ("/a".to_string(), "new-a".to_string()),
                ("/b".to_string(), "b".to_string()),
            ]
        );
    }

    #[tokio::test]
    async fn an_engine_without_deletes_still_replaces_the_entry_correctly() {
        // Negative control for the test above: an accelerator that cannot
        // delete must fall back to read-filter-write and reach the same stored
        // state, so the assertions above are measuring the strategy and not
        // just the outcome.
        let (counting, accelerator) = accelerator_with(
            vec![entry("/a", "old-a"), entry("/b", "b")],
            /* can_delete */ false,
        );

        CacheRefreshHelper::batched_upsert_into_accelerator(
            &accelerator,
            "test_dataset",
            &[vec![col("request_path").eq(lit("/a"))]],
            vec![entry("/a", "new-a")],
        )
        .await
        .expect("upsert");

        assert_eq!(counting.deletes.load(Ordering::Relaxed), 0);
        assert!(
            counting.scans.load(Ordering::Relaxed) > 0,
            "the fallback path reads the table back"
        );
        assert_eq!(
            counting.overwrites.load(Ordering::Relaxed),
            1,
            "the fallback path rewrites the whole acceleration"
        );

        assert_eq!(
            stored(&accelerator).await,
            vec![
                ("/a".to_string(), "new-a".to_string()),
                ("/b".to_string(), "b".to_string()),
            ]
        );
    }

    #[tokio::test]
    async fn caching_a_new_entry_appends_rather_than_rewriting_the_table() {
        let (counting, accelerator) = accelerator_with(vec![entry("/a", "a")], true);

        CacheRefreshHelper::insert_into_accelerator(
            &accelerator,
            "test_dataset",
            vec![entry("/b", "b")],
        )
        .await
        .expect("insert");

        assert_eq!(counting.appends.load(Ordering::Relaxed), 1);
        assert_eq!(
            counting.overwrites.load(Ordering::Relaxed),
            0,
            "caching one response must not rewrite everything already cached"
        );
        assert_eq!(
            counting.scans.load(Ordering::Relaxed),
            0,
            "caching one response must not read everything already cached"
        );

        assert_eq!(
            stored(&accelerator).await,
            vec![
                ("/a".to_string(), "a".to_string()),
                ("/b".to_string(), "b".to_string()),
            ]
        );
    }

    #[tokio::test]
    async fn an_empty_filter_set_never_becomes_an_unconstrained_delete() {
        // An empty filter set reduces to no predicate, which as a DELETE would
        // remove the entire cache. It must disqualify the delete path instead.
        let (counting, accelerator) = accelerator_with(
            vec![entry("/a", "a"), entry("/b", "b")],
            /* can_delete */ true,
        );

        let replaced = CacheRefreshHelper::delete_and_append(
            &accelerator,
            "test_dataset",
            &[vec![]],
            vec![entry("/c", "c")],
        )
        .await
        .expect("must not error");

        assert!(!replaced, "an unconstrained delete must be refused");
        assert_eq!(counting.deletes.load(Ordering::Relaxed), 0);
        assert_eq!(
            stored(&accelerator).await,
            vec![
                ("/a".to_string(), "a".to_string()),
                ("/b".to_string(), "b".to_string()),
            ]
        );
    }
}
