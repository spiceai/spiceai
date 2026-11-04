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

use super::{
    BindingParametersSnafu, Query, QueryMethod, QueryResult, QueryTracker, ResultsCacheMode,
};
use crate::datafusion::{DataFusion, error::find_datafusion_root, query::error_code::ErrorCode};
use cache::{
    QueryResultsCacheProvider, RevalidationOutcome,
    key::{CacheKey, RawCacheKey},
    result::CacheStatus,
    result::query::CachedQueryResult,
    to_cached_record_batch_stream,
};
use dashmap::DashMap;
use datafusion::{
    common::ParamValues,
    common::TableReference,
    execution::{SendableRecordBatchStream, SessionState},
    logical_expr::LogicalPlan,
    physical_plan::ExecutionPlan,
};
use runtime_request_context::{
    CacheControl, CacheKeyType, CacheNamespace, Protocol, RequestContext,
};
use snafu::ResultExt;
use std::sync::OnceLock;
use std::{collections::HashSet, hash::Hasher, sync::Arc};

/// Returns `Plan` if the result is not cached and needs to be executed, otherwise returns `Cached`
pub(super) enum PlanOrCached {
    Plan(Box<LogicalPlan>, Option<QueryTracker>, RequestCacheManager),
    /// Batches are ready; the tracker is not yet on the stream so the caller
    /// can wrap cancellation inside it (source → cancel → tracker).
    Cached {
        result: QueryResult,
        tracker: Option<QueryTracker>,
    },
}

pub(super) struct RequestCacheManager {
    pub(super) cache_status: CacheStatus,
    pub(super) raw_cache_key: RawCacheKey,
}

impl RequestCacheManager {
    pub(super) fn new(cache_status: CacheStatus, raw_cache_key: RawCacheKey) -> Self {
        Self {
            cache_status,
            raw_cache_key,
        }
    }

    pub(super) fn should_cache_results(&self) -> bool {
        !matches!(self.cache_status, CacheStatus::CacheDisabled)
    }
}

struct CacheResponse {
    result: CacheResult,
    status: CacheStatus,
    tracker: Option<QueryTracker>,
    raw_key: Option<RawCacheKey>,
}

impl CacheResponse {
    fn from(result: CacheResult, status: CacheStatus) -> Self {
        Self {
            result,
            status,
            raw_key: None,
            tracker: None,
        }
    }
    fn with_raw_key(mut self, raw_key: Option<RawCacheKey>) -> Self {
        self.raw_key = raw_key;
        self
    }

    fn with_query_tracker(mut self, tracker: Option<QueryTracker>) -> Self {
        self.tracker = tracker;
        self
    }
}

enum CacheResult {
    Hit(QueryResult),
    MissOrSkipped,
    WrongCacheKeyType,
}

/// Records how a background stale-while-revalidate revalidation ended.
///
/// Reaches the counter directly rather than through `CacheMetrics`, matching
/// the sibling stale-while-revalidate counters incremented from this module:
/// revalidation is specific to the SQL results cache, so there is no generic
/// value type to dispatch on.
fn record_revalidation_outcome(outcome: RevalidationOutcome) {
    cache::metrics::sql_results::SWR_REVALIDATIONS.add(1, &[outcome.key_value()]);
}

/// The outcome a revalidation that failed outright is counted under.
///
/// The HTTP connector refuses a 5xx/429 that outlives its retries instead of returning a
/// row carrying the status, so a revalidation against a failing origin ends here rather
/// than in [`cache::batches_cacheable`]. It kept the previous entry for the same reason a
/// result carrying such a row does, so it is counted as `TransientErrors` too; any other
/// failure keeps `otherwise`.
fn failed_revalidation_outcome(
    error: &(dyn std::error::Error + 'static),
    otherwise: RevalidationOutcome,
) -> RevalidationOutcome {
    if data_components::http::provider::is_transient_origin_failure(error) {
        RevalidationOutcome::TransientErrors
    } else {
        otherwise
    }
}

impl CacheResponse {
    /// Nothing servable was found under `raw_key`.
    fn miss(raw_key: RawCacheKey, tracker: Option<QueryTracker>) -> Self {
        Self::from(CacheResult::MissOrSkipped, CacheStatus::CacheMiss)
            .with_query_tracker(tracker)
            .with_raw_key(Some(raw_key))
    }
}

/// Whether a request looks the results cache up under a given kind of key.
///
/// A request consults the cache under exactly one kind — its plan, its SQL
/// text, or the key its client supplied — chosen by its cache key type.
enum KeyUse {
    LookUp,
    /// A client `no-cache` on the request's kind of key: nothing is looked up,
    /// but the fresh result is stored under this key for later requests.
    Revalidate,
    /// The runtime's own [`CacheControl::NoCache`]: the request does not use
    /// the results cache at all.
    Bypass,
    /// The request's cache key type is looked up under another kind of key.
    WrongKeyType,
}

impl KeyUse {
    fn of(cache_control: CacheControl, key: &CacheKey<'_>) -> Self {
        match (cache_control, key) {
            (
                CacheControl::Cache(CacheKeyType::Default)
                | CacheControl::MaxStale(CacheKeyType::Default, _)
                | CacheControl::MinFresh(CacheKeyType::Default, _)
                | CacheControl::OnlyIfCached(CacheKeyType::Default),
                CacheKey::LogicalPlan(_) | CacheKey::SerializedPlan(_, _),
            )
            | (
                CacheControl::Cache(CacheKeyType::Raw)
                | CacheControl::MaxStale(CacheKeyType::Raw, _)
                | CacheControl::MinFresh(CacheKeyType::Raw, _)
                | CacheControl::OnlyIfCached(CacheKeyType::Raw),
                CacheKey::Query(_, _),
            )
            | (
                CacheControl::Cache(CacheKeyType::ClientSupplied)
                | CacheControl::MaxStale(CacheKeyType::ClientSupplied, _)
                | CacheControl::MinFresh(CacheKeyType::ClientSupplied, _)
                | CacheControl::OnlyIfCached(CacheKeyType::ClientSupplied),
                CacheKey::ClientSupplied(_, _),
            ) => Self::LookUp,
            (CacheControl::Revalidate(CacheKeyType::Default), CacheKey::LogicalPlan(_))
            | (CacheControl::Revalidate(CacheKeyType::Raw), CacheKey::Query(_, _))
            | (
                CacheControl::Revalidate(CacheKeyType::ClientSupplied),
                CacheKey::ClientSupplied(_),
            ) => Self::Revalidate,
            (CacheControl::NoCache, _) => Self::Bypass,
            _ => Self::WrongKeyType,
        }
    }
}

/// An entry found under a request's key that may be served to it.
pub(super) struct ServableEntry {
    cached_result: CachedQueryResult,
    cache_status: CacheStatus,
    /// Whether serving the entry must start a background revalidation.
    revalidate: bool,
}

/// How age is judged when deciding whether a cached result may still be served.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StalePolicy {
    /// Past `item_ttl` is a miss. Used when no stale-while-revalidate window
    /// is configured: the cache backend expires the entry at that age, and a
    /// captured probe hit must not outlive it while waiting on the query runtime.
    FreshOnly,
    /// Past `item_ttl` is served stale and revalidated; past `item_ttl` plus
    /// this window is a miss.
    Window(std::time::Duration),
    /// The request's `max-stale` has no value: only the provider age cap applies.
    AnyAge,
    /// The request's `min-fresh`: the entry must stay within `item_ttl` for
    /// this much longer, and is never served stale.
    FreshFor(std::time::Duration),
}

fn stale_policy(
    cache_control: CacheControl,
    stale_while_revalidate_ttl: Option<std::time::Duration>,
) -> StalePolicy {
    match cache_control {
        CacheControl::MaxStale(_, Some(duration)) => StalePolicy::Window(duration),
        CacheControl::MaxStale(_, None) => StalePolicy::AnyAge,
        CacheControl::MinFresh(_, duration) => StalePolicy::FreshFor(duration),
        _ => match stale_while_revalidate_ttl {
            Some(duration) => StalePolicy::Window(duration),
            None => StalePolicy::FreshOnly,
        },
    }
}

/// The oldest an entry may be and still be served to a request with `policy`:
/// the request's own bound, never more than the cache's `max_servable_age`
/// (`item_ttl + stale_while_revalidate_ttl`). A request's `max-stale` can only
/// narrow what the cache serves.
fn max_age_for(
    policy: StalePolicy,
    ttl: std::time::Duration,
    max_servable_age: std::time::Duration,
) -> std::time::Duration {
    let requested = match policy {
        StalePolicy::FreshOnly => ttl,
        StalePolicy::Window(stale_duration) => ttl.saturating_add(stale_duration),
        StalePolicy::AnyAge => max_servable_age,
        StalePolicy::FreshFor(fresh_for) => ttl.saturating_sub(fresh_for),
    };
    requested.min(max_servable_age)
}

/// Whether TTL / stale-while-revalidate age still allows the entry to be
/// served. Does not mutate; [`apply_age_eligibility`] records revalidation
/// when an entry is past `item_ttl` but still within its bound.
fn age_is_eligible(
    entry: &ServableEntry,
    ttl: std::time::Duration,
    max_servable_age: std::time::Duration,
    policy: StalePolicy,
    now: std::time::Instant,
) -> bool {
    !entry
        .cached_result
        .is_stale(max_age_for(policy, ttl, max_servable_age), now)
}

/// Re-evaluate TTL / stale-while-revalidate age. Returns `false` when the
/// entry must not be served. Not a cache lookup: the caller already holds
/// the captured result.
fn apply_age_eligibility(
    entry: &mut ServableEntry,
    ttl: std::time::Duration,
    max_servable_age: std::time::Duration,
    policy: StalePolicy,
    now: std::time::Instant,
) -> bool {
    if !age_is_eligible(entry, ttl, max_servable_age, policy, now) {
        tracing::debug!("Cache entry is past its eligible age, treating as cache miss");
        return false;
    }
    if matches!(policy, StalePolicy::Window(_) | StalePolicy::AnyAge)
        && entry.cached_result.is_stale(ttl, now)
    {
        tracing::debug!(
            "Cache entry is stale (beyond TTL), triggering background revalidation for stale-while-revalidate"
        );
        entry.cache_status = CacheStatus::CacheStaleWhileRevalidate;
        entry.revalidate = true;
    }
    true
}

/// Serve-time TTL and table-clock checks, without a second counted lookup
/// and without mutating the entry. Used to decide whether a raw hit can
/// stay on the request runtime: an ineligible raw hit falls through to
/// planning, which belongs on the query runtime.
fn entry_still_servable(
    entry: &ServableEntry,
    provider: Option<&QueryResultsCacheProvider>,
    cache_control: CacheControl,
) -> bool {
    let Some(provider) = provider else {
        return true;
    };
    let now = std::time::Instant::now();
    let policy = stale_policy(cache_control, provider.stale_while_revalidate_ttl());
    if !age_is_eligible(
        entry,
        provider.ttl(),
        provider.max_servable_age(),
        policy,
        now,
    ) {
        return false;
    }
    !entry_tables_changed(entry, provider)
}

/// Looks `raw_key` up and decides whether the entry found may be served to a
/// request with `cache_control`.
///
/// An entry older than the request accepts (see [`max_age_for`]) is not
/// servable, and the lookup counts it as a miss. One past its TTL is served
/// stale and marked for revalidation.
async fn find_servable_entry(
    cache_provider: &QueryResultsCacheProvider,
    cache_control: CacheControl,
    raw_key: &RawCacheKey,
) -> super::Result<Option<ServableEntry>> {
    let policy = stale_policy(cache_control, cache_provider.stale_while_revalidate_ttl());
    // The request's own bound goes into the counted lookup, so an entry this
    // request turns away is a miss there rather than a hit followed by a query.
    let max_age = max_age_for(
        policy,
        cache_provider.ttl(),
        cache_provider.max_servable_age(),
    );
    let cached_result = match cache_provider.get_raw_key_within(raw_key, max_age).await {
        Ok(Some(hit)) => hit,
        Ok(None) => return Ok(None),
        Err(e) => return Err(super::Error::FailedToAccessCache { source: e }),
    };
    let mut entry = ServableEntry {
        cached_result,
        cache_status: CacheStatus::CacheHit,
        revalidate: false,
    };
    Ok(apply_age_eligibility(
        &mut entry,
        cache_provider.ttl(),
        cache_provider.max_servable_age(),
        policy,
        std::time::Instant::now(),
    )
    .then_some(entry))
}

/// How serving a servable entry ended.
enum Served {
    /// Batches are ready. The tracker is returned separately so the caller
    /// can wrap cancellation inside it (source → cancel → tracker).
    Hit {
        result: QueryResult,
        tracker: Option<QueryTracker>,
    },
    /// The entry could not be decoded. The tracker is handed back so the query
    /// can still be planned, executed and tracked like a miss.
    Undecodable(Option<QueryTracker>),
}

/// The largest Arrow IPC stream an encoded hit is decoded from where the request
/// arrived, instead of on the query runtime.
///
/// A decode reads the whole stream, so its cost follows the size of the stream,
/// not of the compressed payload the entry holds. In
/// `crates/cache/benches/cache_hit_costs.rs` on an Apple M3 Max a decode costs
/// about 4.5 µs plus 0.4–1.9 µs per KiB of stream across row, column and batch
/// counts and compressibility, while per KiB of payload the same decodes cost
/// 3–850 µs (256 empty batches compress to 194 bytes). At this size the slowest
/// shape measured decodes in about 36 µs, well within the time a task may run
/// without yielding. It matches `FLIGHT_INLINE_ENCODE_MAX_BYTES`, the budget for
/// encoding a response on the same runtime.
const INLINE_DECODE_MAX_BYTES: usize = 16 * 1024;

/// What looking a query up in the results cache found before anything was
/// planned. See [`Query::probe_results_cache`].
pub(super) enum CacheProbe {
    /// No lookup was made, so the planned path makes its own.
    Skipped,
    /// The key was looked up and held nothing servable.
    Missed(RawCacheKey),
    /// An entry the request can be served.
    Hit(Box<ProbedHit>),
}

impl CacheProbe {
    /// Whether the request can be served where it arrived, without the query
    /// runtime.
    ///
    /// Only a hit that is cheap to serve qualifies: one held as batches hands
    /// out the batches the cache already holds, and an encoded one is decoded
    /// here while the stream it decodes to is within [`INLINE_DECODE_MAX_BYTES`].
    /// A larger decode, and a miss, which has a query to plan and execute,
    /// belong on the query runtime.
    pub(super) fn is_servable_in_place(&self) -> bool {
        matches!(self, Self::Hit(hit) if hit.serves_in_place())
    }

    /// A hit servable in place that fails the serve-time TTL / table-clock
    /// checks cannot be handed out in place. Turn it into a miss so the hop
    /// onto the query runtime sees planning work. [`Query::run`] also serves a
    /// remaining in-place hit before that hop, so a later reject still becomes
    /// a miss instead of planning on the request I/O runtime.
    ///
    /// A hit too large to decode in place stays a hit: it hops so it can
    /// decode, and [`Query::serve_probed_hit`] rechecks eligibility after the
    /// wait.
    #[must_use]
    pub(super) fn into_miss_if_in_place_ineligible(
        self,
        cache_control: CacheControl,
        provider: Option<&QueryResultsCacheProvider>,
    ) -> Self {
        match self {
            Self::Hit(hit)
                if hit.serves_in_place()
                    && !entry_still_servable(&hit.entry, provider, cache_control) =>
            {
                Self::Missed(hit.raw_key())
            }
            other => other,
        }
    }
}

/// A results-cache entry found before planning, with what serving it needs.
pub(super) struct ProbedHit {
    raw_key: RawCacheKey,
    entry: ServableEntry,
    sql: Arc<str>,
    /// The plan the key was computed from, kept only when serving the entry
    /// starts a revalidation, which then re-executes the plan rather than
    /// re-parsing the SQL.
    revalidation_plan: Option<LogicalPlan>,
}

impl ProbedHit {
    pub(super) fn raw_key(&self) -> RawCacheKey {
        self.raw_key
    }

    /// Whether serving this hit is cheap enough to do where the request
    /// arrived. See [`CacheProbe::is_servable_in_place`].
    fn serves_in_place(&self) -> bool {
        self.entry
            .cached_result
            .decoded_len()
            .is_none_or(|len| len <= INLINE_DECODE_MAX_BYTES)
    }
}

impl Query {
    /// Returns a `LogicalPlan` if the result is not cached and needs to be executed, otherwise returns a cached `QueryResult`.
    ///
    /// Cache lookups and population are *not* gated on read-only mode. The two
    /// hazards that would have justified gating are handled elsewhere:
    ///
    /// 1. **Cross-principal leakage.** Cache keys are mixed with the originating
    ///    [`runtime_request_context::CacheNamespace`], so a read-only caller
    ///    can only ever observe entries it (or another caller in the same
    ///    namespace) populated.
    /// 2. **Write-capable plans served from cache.**
    ///    [`cache::QueryResultsCacheProvider::cache_is_enabled_for_plan`]
    ///    refuses to cache DDL/DML/Copy/Statement and every
    ///    [`LogicalPlan::Extension`] whose name appears in
    ///    [`cache::WRITE_CAPABLE_EXTENSION_NAMES`] (currently `DdlExtension`
    ///    and `DmlExtension`). New write-capable extension nodes must be
    ///    added there to keep this property.
    ///
    /// `already_looked_up` is the key [`Self::probe_results_cache`] found
    /// nothing servable under, when it made a lookup. That key is not looked up
    /// again: the request has already been counted, and counting it twice would
    /// skew the cache's request and miss metrics.
    #[expect(clippy::too_many_arguments)]
    pub(super) async fn get_plan_or_cached(
        df: &Arc<DataFusion>,
        session: &SessionState,
        request_context: Arc<RequestContext>,
        sql: &str,
        parameters: Option<ParamValues>,
        tracker: Option<QueryTracker>,
        pre_parsed_plan: Option<Box<LogicalPlan>>,
        already_looked_up: Option<RawCacheKey>,
        serialized_plan_cache_key: Option<&str>,
    ) -> super::Result<PlanOrCached> {
        let cache_control = request_context.cache_control();
        let cache_namespace = request_context.cache_namespace();
        let (ns_tag, ns_id) = cache_namespace.hash_inputs();
        let sql_cache_key = CacheKey::Query(sql, parameters.as_ref());
        let scoped_user_cache_key =
            if cache_control.cache_key_type() == Some(CacheKeyType::ClientSupplied) {
                request_context.scoped_client_supplied_cache_key()
            } else {
                None
            };
        // A client key partitions the SQL key; it never replaces it.
        let sql_or_user_cache_key = match scoped_user_cache_key.as_deref() {
            Some(user_key) => CacheKey::ClientSupplied(user_key, &sql_cache_key),
            _ => sql_cache_key,
        };

        // Try to get cached results from SQL or client key.
        let CacheResponse {
            tracker,
            raw_key: sql_or_client_raw_key,
            ..
        } = match Self::try_get_cached_result(
            df,
            &request_context,
            tracker,
            &sql_or_user_cache_key,
            sql,
            already_looked_up,
            parameters.as_ref(),
        )
        .await?
        {
            CacheResponse {
                result: CacheResult::Hit(result),
                tracker,
                ..
            } => {
                return Ok(PlanOrCached::Cached { result, tracker });
            }
            response => response,
        };

        let sql_raw_cache_key =
            sql_cache_key.as_raw_key_in_namespace(Self::plan_hasher(df), ns_tag, ns_id);
        let cached_plan_key = Self::shared_plans_cache_key(df, sql, &request_context);
        let plan: Box<LogicalPlan> = if let Some(plan) = pre_parsed_plan {
            // Reuse the pre-parsed plan to avoid re-parsing. Parameters are
            // already bound from `check_read_only_sql`.
            plan
        } else {
            match Self::get_plan(df, session, sql, cached_plan_key.as_ref(), parameters).await {
                Ok(plan) => Box::new(plan),
                Err(e) => {
                    if let super::Error::UnableToExecuteQuery { source } = e {
                        let code = ErrorCode::from(&source);
                        let snafu_err = super::Error::UnableToExecuteQuery { source };
                        if let Some(t) = tracker {
                            t.finish_with_error(&request_context, snafu_err.to_string(), code);
                        }
                        return Err(snafu_err);
                    }
                    return Err(e);
                }
            }
        };

        // Try to get cached results from plan.
        let CacheResponse {
            mut tracker,
            raw_key: plan_raw_cache_key,
            status,
            ..
        } = match Self::try_get_cached_result(
            df,
            &request_context,
            tracker,
            &CacheKey::for_plan(&plan, serialized_plan_cache_key),
            sql,
            already_looked_up,
            // A `LogicalPlan` key carries the parameter values already bound
            // into it, so a revalidation of a hit on this key re-runs the plan
            // rather than the SQL text and needs no values of its own.
            None,
        )
        .await?
        {
            CacheResponse {
                result: CacheResult::Hit(result),
                tracker,
                ..
            } => {
                return Ok(PlanOrCached::Cached { result, tracker });
            }
            response => response,
        };

        if matches!(cache_control, CacheControl::OnlyIfCached(_)) {
            let error = super::Error::NotCached;
            if let Some(t) = tracker {
                t.finish_with_error(
                    &request_context,
                    error.to_string(),
                    ErrorCode::QueryPlanningError,
                );
            }
            return Err(error);
        }

        let request_raw_cache_key = match request_context.cache_control() {
            CacheControl::Cache(CacheKeyType::Default)
            | CacheControl::MaxStale(CacheKeyType::Default, _)
            | CacheControl::MinFresh(CacheKeyType::Default, _)
            | CacheControl::OnlyIfCached(CacheKeyType::Default)
            | CacheControl::Revalidate(CacheKeyType::Default) => plan_raw_cache_key,
            _ => sql_or_client_raw_key,
        }
        .unwrap_or(sql_raw_cache_key);

        // A `no-cache` request reports `BYPASS` whichever of its two lookups
        // matched its key type; the other reports a wrong-key-type miss.
        let status = match cache_control {
            CacheControl::Revalidate(_) => CacheStatus::CacheBypass,
            _ => status,
        };
        let cache_status = Self::should_cache_results(df, &plan, status);
        tracker = tracker.map(|t| t.results_cache_hit(false));

        Ok(PlanOrCached::Plan(
            plan,
            tracker,
            RequestCacheManager::new(cache_status, request_raw_cache_key),
        ))
    }

    /// Plans a query without consulting or populating the SQL results cache.
    pub(super) async fn get_plan_without_results_cache(
        df: &Arc<DataFusion>,
        session: &SessionState,
        request_context: &RequestContext,
        sql: &str,
        parameters: Option<ParamValues>,
        tracker: Option<QueryTracker>,
        pre_parsed_plan: Option<Box<LogicalPlan>>,
    ) -> super::Result<PlanOrCached> {
        let cache_namespace = request_context.cache_namespace();
        let (ns_tag, ns_id) = cache_namespace.hash_inputs();
        let raw_cache_key = CacheKey::Query(sql, parameters.as_ref()).as_raw_key_in_namespace(
            Self::plan_hasher(df),
            ns_tag,
            ns_id,
        );
        let cached_plan_key = Self::shared_plans_cache_key(df, sql, request_context);
        let plan = if let Some(plan) = pre_parsed_plan {
            plan
        } else {
            match Self::get_plan(df, session, sql, cached_plan_key.as_ref(), parameters).await {
                Ok(plan) => Box::new(plan),
                Err(super::Error::UnableToExecuteQuery { source }) => {
                    let code = ErrorCode::from(&source);
                    let error = super::Error::UnableToExecuteQuery { source };
                    if let Some(tracker) = tracker {
                        tracker.finish_with_error(request_context, error.to_string(), code);
                    }
                    return Err(error);
                }
                Err(error) => return Err(error),
            }
        };

        Ok(PlanOrCached::Plan(
            plan,
            tracker,
            RequestCacheManager::new(CacheStatus::CacheDisabled, raw_cache_key),
        ))
    }

    /// Get the logical plan for the given SQL query, applying parameter values if provided.
    pub(super) async fn get_plan(
        df: &Arc<DataFusion>,
        session: &SessionState,
        sql: &str,
        sql_raw_cache_key: Option<&RawCacheKey>,
        parameters: Option<ParamValues>,
    ) -> super::Result<LogicalPlan> {
        let plan = match df
            .get_or_create_logical_plan(session, sql_raw_cache_key, sql, parameters.as_ref())
            .await
        {
            Ok(plan) => plan,
            Err(e) => {
                return Err(super::Error::UnableToExecuteQuery {
                    source: find_datafusion_root(e),
                });
            }
        };

        // Use the logical plan with parameter values for caching and lookup
        let plan = match parameters {
            Some(param_values) => plan
                .with_param_values(param_values)
                .context(BindingParametersSnafu)?,
            None => plan,
        };
        Ok(plan)
    }

    /// The key a cached [`LogicalPlan`] lives under: the SQL text and the
    /// cache namespace, never the parameter values bound into it.
    ///
    /// Plans keep their placeholders, and [`Self::get_plan`] binds the values
    /// into the plan it hands back, so one SQL text has exactly one plan
    /// whatever is bound into it. (Placeholders in table-function arguments are
    /// substituted before planning instead, and those plans are never cached.)
    /// Mixing the values into this key gives every value tuple its own entry,
    /// which makes a parameterized query, the case the cache exists for, a
    /// guaranteed miss, and evicts unrelated plans while it does so. The values
    /// do key the *results*, which are keyed separately on
    /// `CacheKey::Query(sql, parameters)` or on the bound
    /// [`CacheKey::LogicalPlan`].
    ///
    /// `namespace` is a `CacheNamespace::hash_inputs` pair; `None` leaves the
    /// key shared across principals, which the table-allowlist path relies on.
    pub(super) fn cached_plan_key(
        df: &DataFusion,
        sql: &str,
        namespace: Option<(u8, &[u8])>,
    ) -> RawCacheKey {
        let key = CacheKey::Query(sql, None);
        match namespace {
            Some((tag, id)) => key.as_raw_key_in_namespace(Self::plan_hasher(df), tag, id),
            None => key.as_raw_key(Self::plan_hasher(df)),
        }
    }

    /// Plans cache key shared by `get_schema` and the planned path.
    ///
    /// `None` when the request carries an owned Flight SQL session: those
    /// plans depend on that session's prepared statements and catalog
    /// snapshot, and must not be stored under the principal-only key.
    pub(super) fn shared_plans_cache_key(
        df: &DataFusion,
        sql: &str,
        request_context: &RequestContext,
    ) -> Option<RawCacheKey> {
        if super::owned_flight_session(request_context).is_some() {
            return None;
        }
        let cache_namespace = request_context.cache_namespace();
        let (tag, id) = cache_namespace.hash_inputs();
        Some(Self::cached_plan_key(df, sql, Some((tag, id))))
    }

    /// Return the [`Hasher`] that should be used in caching [`LogicalPlan`]s in [`DataFusion`].
    pub(super) fn plan_hasher(df: &DataFusion) -> Box<dyn Hasher> {
        df.plans_cache_provider().map_or(
            Box::new(std::hash::DefaultHasher::new()) as Box<dyn Hasher>,
            |p| p.hasher(),
        )
    }

    /// `parameters` are the values bound into this request, carried for the
    /// stale-while-revalidate path: a revalidation that has no [`LogicalPlan`]
    /// to re-run rebuilds the query from `sql`, and without the values that SQL
    /// still holds its placeholders and fails to execute.
    async fn try_get_cached_result<'a>(
        df: &Arc<DataFusion>,
        request_context: &Arc<RequestContext>,
        tracker: Option<QueryTracker>,
        key: &'a CacheKey<'a>,
        sql: &str,
        already_looked_up: Option<RawCacheKey>,
        parameters: Option<&ParamValues>,
    ) -> super::Result<CacheResponse> {
        let Some(cache_provider) = df.results_cache_provider() else {
            return Ok(
                CacheResponse::from(CacheResult::MissOrSkipped, CacheStatus::CacheDisabled)
                    .with_query_tracker(tracker),
            );
        };

        let cache_control = request_context.cache_control();

        let raw_key = |provider: &QueryResultsCacheProvider| {
            let ns = request_context.cache_namespace();
            let (ns_tag, ns_id) = ns.hash_inputs();
            key.as_raw_key_in_namespace(provider.hasher(), ns_tag, ns_id)
        };

        // Validate that the provided cache key is the correct type for this request
        match KeyUse::of(cache_control, key) {
            KeyUse::LookUp => {}
            // Skip the read, but hand back the key: the query's fresh result is
            // stored under it, replacing whatever entry was there, so the next
            // request without `no-cache` reads what this one returned.
            KeyUse::Revalidate => {
                return Ok(CacheResponse::from(
                    CacheResult::MissOrSkipped,
                    CacheStatus::CacheBypass,
                )
                .with_query_tracker(tracker)
                .with_raw_key(Some(raw_key(cache_provider.as_ref()))));
            }
            KeyUse::Bypass => {
                return Ok(CacheResponse::from(
                    CacheResult::MissOrSkipped,
                    CacheStatus::CacheBypass,
                )
                .with_query_tracker(tracker));
            }
            KeyUse::WrongKeyType => {
                return Ok(CacheResponse::from(
                    CacheResult::WrongCacheKeyType,
                    CacheStatus::CacheMiss,
                )
                .with_query_tracker(tracker));
            }
        }

        let raw_key = raw_key(cache_provider.as_ref());

        // Looked up before planning, and nothing servable was there.
        if already_looked_up == Some(raw_key) {
            return Ok(CacheResponse::miss(raw_key, tracker));
        }

        let Some(entry) = find_servable_entry(&cache_provider, cache_control, &raw_key).await?
        else {
            return Ok(CacheResponse::miss(raw_key, tracker));
        };

        // Extract plan from cache key if available to avoid re-parsing
        let plan = match key {
            CacheKey::LogicalPlan(p) | CacheKey::SerializedPlan(_, p) => Some(*p),
            _ => None,
        };
        let cache_status = entry.cache_status;
        match Self::serve_entry(
            df,
            request_context,
            tracker,
            sql,
            plan,
            parameters,
            raw_key,
            entry,
        )
        .await
        {
            Served::Hit { result, tracker } => {
                Ok(CacheResponse::from(CacheResult::Hit(result), cache_status)
                    .with_query_tracker(tracker)
                    .with_raw_key(Some(raw_key)))
            }
            Served::Undecodable(tracker) => Ok(CacheResponse::miss(raw_key, tracker)),
        }
    }

    /// Looks the query up in the results cache before anything is planned.
    ///
    /// Makes the one lookup the planned path would make for this request —
    /// under its SQL text, its client-supplied key, or its plan, as its cache
    /// key type selects — so that a hit is served without planning, cloning
    /// session state, or crossing onto the query runtime. A plan-keyed lookup
    /// is only made when the plan is already cached, since planning is the work
    /// this exists to avoid.
    ///
    /// A key looked up here is not looked up again: a miss reports it, for
    /// [`Self::get_plan_or_cached`] to skip.
    pub(super) async fn probe_results_cache(&self, request_context: &RequestContext) -> CacheProbe {
        let QueryMethod::Text {
            sql,
            parameters,
            table_allowlist: None,
            pre_parsed_plan,
        } = &self.sql
        else {
            return CacheProbe::Skipped;
        };
        if self.results_cache_mode != ResultsCacheMode::Default {
            return CacheProbe::Skipped;
        }
        let Some(cache_provider) = self.df.results_cache_provider() else {
            return CacheProbe::Skipped;
        };
        let cache_control = request_context.cache_control();
        // A `no-cache` request is never answered from the cache.
        if matches!(cache_control, CacheControl::Revalidate(_)) {
            return CacheProbe::Skipped;
        }
        let Some(cache_key_type) = cache_control.cache_key_type() else {
            return CacheProbe::Skipped;
        };
        let cache_namespace = request_context.cache_namespace();
        let (ns_tag, ns_id) = cache_namespace.hash_inputs();

        let mut cached_plan = None;
        let raw_key = match cache_key_type {
            CacheKeyType::Raw => CacheKey::Query(sql.as_ref(), parameters.as_ref())
                .as_raw_key_in_namespace(cache_provider.hasher(), ns_tag, ns_id),
            CacheKeyType::ClientSupplied => {
                let Some(user_key) = request_context.scoped_client_supplied_cache_key() else {
                    return CacheProbe::Skipped;
                };
                let sql_key = CacheKey::Query(sql.as_ref(), parameters.as_ref());
                CacheKey::ClientSupplied(&user_key, &sql_key).as_raw_key_in_namespace(
                    cache_provider.hasher(),
                    ns_tag,
                    ns_id,
                )
            }
            CacheKeyType::Default => {
                // A Flight session's plan is not in the shared cache (see
                // `shared_plans_cache_key`). Skip the probe rather than
                // hashing another session's cached plan into this request's
                // results-cache key.
                if super::owned_flight_session(request_context).is_some() {
                    return CacheProbe::Skipped;
                }
                // A pre-parsed plan already has its parameters bound.
                let plan = if let Some(plan) = pre_parsed_plan {
                    plan.as_ref()
                } else {
                    let Some(plan) =
                        Self::cached_plan(&self.df, sql, parameters.as_ref(), (ns_tag, ns_id))
                            .await
                    else {
                        return CacheProbe::Skipped;
                    };
                    cached_plan.insert(plan)
                };
                CacheKey::for_plan(plan, self.serialized_plan_cache_key.as_deref())
                    .as_raw_key_in_namespace(cache_provider.hasher(), ns_tag, ns_id)
            }
        };

        match find_servable_entry(&cache_provider, cache_control, &raw_key).await {
            Ok(Some(entry)) => {
                let revalidation_plan = match cache_key_type {
                    CacheKeyType::Default if entry.revalidate => {
                        cached_plan.or_else(|| pre_parsed_plan.as_deref().cloned())
                    }
                    _ => None,
                };
                CacheProbe::Hit(Box::new(ProbedHit {
                    raw_key,
                    entry,
                    sql: Arc::clone(sql),
                    revalidation_plan,
                }))
            }
            Ok(None) => CacheProbe::Missed(raw_key),
            // Left for the planned path, which reports it against the query.
            Err(_) => CacheProbe::Skipped,
        }
    }

    /// The plan the plans cache holds for `sql` in `namespace`, with
    /// `parameters` bound into it: what [`Self::get_plan`] returns when it does
    /// not have to plan. `None` when it would, or when binding fails.
    async fn cached_plan(
        df: &DataFusion,
        sql: &str,
        parameters: Option<&ParamValues>,
        namespace: (u8, &[u8]),
    ) -> Option<LogicalPlan> {
        let plans_cache = df.plans_cache_provider()?;
        let plan = plans_cache
            .get_raw_key(&Self::cached_plan_key(df, sql, Some(namespace)).as_u64())
            .await?;
        let plan = std::sync::Arc::unwrap_or_clone(plan);
        match parameters {
            Some(parameters) => plan.with_param_values(parameters.clone()).ok(),
            None => Some(plan),
        }
    }
}

/// Checks whether a client write, reload or unload disqualifies this read.
fn entry_tables_changed(entry: &ServableEntry, provider: &QueryResultsCacheProvider) -> bool {
    provider.tables_changed_since(
        &entry.cached_result.input_tables,
        entry.cached_result.read_started_at,
    )
}

/// Cache keys with a stale-while-revalidate background revalidation pending or
/// running, each held by the claim that took it, and the permits that bound how
/// many run at once.
///
/// A claim is taken on the stale hit's request path, so it is a sharded-map entry
/// rather than a write to a cache that runs its own maintenance inline.
struct RevalidationClaims {
    held: DashMap<u64, HeldClaim>,
    time_to_live: std::time::Duration,
    next_generation: std::sync::atomic::AtomicU64,
    running: Arc<tokio::sync::Semaphore>,
}

/// The claim currently holding a key.
#[derive(Clone, Copy)]
struct HeldClaim {
    generation: u64,
    claimed_at: std::time::Instant,
}

impl RevalidationClaims {
    /// `time_to_live` bounds how long a revalidation that never ends can keep its
    /// key from being claimed again. At most `max_running` claims are held at once.
    fn new(time_to_live: std::time::Duration, max_running: usize) -> Self {
        Self {
            held: DashMap::new(),
            time_to_live,
            next_generation: std::sync::atomic::AtomicU64::new(0),
            running: Arc::new(tokio::sync::Semaphore::new(max_running)),
        }
    }

    /// Claims `key` for one revalidation, or returns `None` while another claim
    /// holds it or `max_running` claims are held. A claim older than the
    /// time-to-live no longer holds its key.
    fn claim(&self, key: u64) -> Option<RevalidationClaim<'_>> {
        let generation = self
            .next_generation
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let claimed_at = std::time::Instant::now();
        let claim = HeldClaim {
            generation,
            claimed_at,
        };
        let permit = match self.held.entry(key) {
            dashmap::Entry::Occupied(mut held) => {
                if claimed_at.duration_since(held.get().claimed_at) < self.time_to_live {
                    return None;
                }
                let permit = Arc::clone(&self.running).try_acquire_owned().ok()?;
                held.insert(claim);
                permit
            }
            dashmap::Entry::Vacant(vacant) => {
                let permit = Arc::clone(&self.running).try_acquire_owned().ok()?;
                vacant.insert(claim);
                permit
            }
        };
        Some(RevalidationClaim {
            claims: self,
            key,
            generation,
            expires_at: claimed_at + self.time_to_live,
            _running: permit,
        })
    }
}

/// A cache key claimed for one stale-while-revalidate background revalidation.
/// Dropping it releases the key.
struct RevalidationClaim<'a> {
    claims: &'a RevalidationClaims,
    key: u64,
    generation: u64,
    /// When the claim stops holding its key.
    expires_at: std::time::Instant,
    _running: tokio::sync::OwnedSemaphorePermit,
}

impl Drop for RevalidationClaim<'_> {
    fn drop(&mut self) {
        // Only this claim's own generation is released. A revalidation that outlived
        // the time-to-live may have been replaced by a later claim on the same key,
        // which has to stay held until its own revalidation ends.
        let generation = self.generation;
        self.claims
            .held
            .remove_if(&self.key, |_, held| held.generation == generation);
    }
}

impl Query {
    /// Serves a hit [`Self::probe_results_cache`] found.
    ///
    /// Returns the cached batches and the tracker separately so the caller
    /// can wrap cancellation inside the tracker. `None` when the entry
    /// cannot be decoded, or when the table-change clock has invalidated it
    /// since the probe, with the query's tracker left in place so it can
    /// still be planned, executed and tracked like a miss.
    pub(super) async fn serve_probed_hit(
        &mut self,
        request_context: &Arc<RequestContext>,
        hit: ProbedHit,
    ) -> Option<(QueryResult, Option<QueryTracker>)> {
        let ProbedHit {
            raw_key,
            mut entry,
            sql,
            revalidation_plan,
        } = hit;

        // A hit too large to decode in place hops onto the query runtime after
        // the probe. Recheck TTL/SWR age and the table-change clock at serve
        // time so an entry that waited past `item_ttl` (or past a client write)
        // is not served. In-place hits are sequential on this task;
        // the check is the same and cheap. This is not a second cache lookup.
        if let Some(provider) = self.df.results_cache_provider() {
            let now = std::time::Instant::now();
            let policy = stale_policy(
                request_context.cache_control(),
                provider.stale_while_revalidate_ttl(),
            );
            if !apply_age_eligibility(
                &mut entry,
                provider.ttl(),
                provider.max_servable_age(),
                policy,
                now,
            ) {
                return None;
            }
            if entry_tables_changed(&entry, &provider) {
                return None;
            }
        }

        let parameters = if let QueryMethod::Text { parameters, .. } = &self.sql {
            parameters.as_ref()
        } else {
            None
        };
        match Self::serve_entry(
            &self.df,
            request_context,
            self.tracker.take(),
            &sql,
            revalidation_plan.as_ref(),
            parameters,
            raw_key,
            entry,
        )
        .await
        {
            Served::Hit { result, tracker } => Some((result, tracker)),
            Served::Undecodable(tracker) => {
                self.tracker = tracker;
                None
            }
        }
    }

    /// Streams `entry` to the request, starting the background revalidation it
    /// was marked for. `parameters` are the values bound into the request: a
    /// revalidation with no `plan` rebuilds the query from `sql`, which still
    /// holds its placeholders.
    #[expect(clippy::too_many_arguments)]
    async fn serve_entry(
        df: &Arc<DataFusion>,
        request_context: &Arc<RequestContext>,
        tracker: Option<QueryTracker>,
        sql: &str,
        plan: Option<&LogicalPlan>,
        parameters: Option<&ParamValues>,
        raw_key: RawCacheKey,
        entry: ServableEntry,
    ) -> Served {
        let ServableEntry {
            cached_result,
            cache_status,
            revalidate,
        } = entry;

        if revalidate {
            Self::trigger_background_query_revalidation(
                Arc::clone(df),
                sql,
                plan,
                parameters,
                raw_key,
                request_context.cache_namespace(),
                cached_result.input_tables.arc(),
            );
        }

        let records = if let Some(raw) = cached_result.raw_batches() {
            Ok(raw)
        } else {
            match df.results_cache_provider() {
                Some(provider) => provider.records(&raw_key, &cached_result).await,
                None => cached_result.records().await,
            }
        };
        let records = match records {
            Ok(records) => records,
            Err(e) => {
                tracing::error!("Failed to decode cached query result: {e}");
                return Served::Undecodable(tracker);
            }
        };

        // Duration and returned-output counters finish when this stream is
        // consumed, matching a miss. The batches are already in memory; the
        // client may still disconnect before HTTP or Flight reads them.
        // The tracker is not attached here: the caller wraps cancellation
        // first so a cancel is an error the tracker can finish on.
        let tracker = tracker.map(|t| {
            t.datasets(cached_result.input_tables.arc())
                .results_cache_hit(true)
        });

        Served::Hit {
            result: QueryResult::from_cached_raw(records, cached_result.schema.arc(), cache_status),
            tracker,
        }
    }

    pub(super) fn should_cache_results(
        df: &DataFusion,
        plan: &LogicalPlan,
        cache_status: CacheStatus,
    ) -> CacheStatus {
        match df.results_cache_provider() {
            Some(provider) if provider.cache_is_enabled_for_plan(plan) => cache_status,
            _ => CacheStatus::CacheDisabled,
        }
    }

    /// Trigger background query re-execution for stale-while-revalidate.
    ///
    /// This spawns a background task that re-executes the original query through the full
    /// query pipeline (including cache population), which will:
    /// 1. Use the proper cache control settings to populate the cache
    /// 2. Go through acceleration if available, or the data source
    /// 3. Update the cache with fresh data via the normal `Query::run` flow
    ///
    /// If a `LogicalPlan` is provided, it will be used directly to avoid re-parsing the SQL.
    /// This is more efficient when the plan is already available (e.g., from a plan cache hit).
    ///
    /// Uses lock-free deduplication based on the cache key to ensure only one revalidation
    /// task runs per cache entry. Multiple concurrent requests for the same stale cache entry
    /// will not spawn redundant background tasks.
    ///
    /// The background task will be automatically cancelled if:
    /// - The `DataFusion` context is dropped (runtime shutdown)
    /// - The query execution is interrupted via the session context
    ///
    /// Build the request context that drives a background SWR revalidation
    /// query. We must inherit the originating request's cache namespace so
    /// the refreshed entry lands in the same scope (otherwise a per-user
    /// triggered refresh would write into the System scope and the user
    /// would never see the new data on their next request).
    ///
    /// `NoCache` is intentional: the revalidation flow stores the result
    /// directly under the original cache key via `cache_revalidation_result`,
    /// so going through the normal cache lookup/store path would be
    /// redundant and would also confuse hit/miss accounting.
    fn create_background_context(namespace: CacheNamespace) -> Arc<RequestContext> {
        Arc::new(
            RequestContext::builder(Protocol::Internal)
                .with_cache_control(CacheControl::NoCache)
                .with_cache_namespace(namespace)
                .build(),
        )
    }

    /// Prepares query and input tables for background revalidation.
    ///
    /// `cached_input_tables` is the table set recorded on the entry being
    /// revalidated. It is the fallback when no [`LogicalPlan`] is available —
    /// which is the normal case under
    /// [`spicepod::component::caching::CacheKeyType::Sql`], where the stale hit
    /// is found on the raw-SQL key before a plan exists. The revalidated entry
    /// must carry the same table set as the entry it replaces: the set is what
    /// [`cache::TabledCacheProvider::invalidate_for_table`] matches on, so an
    /// entry stored with an empty set can never be evicted by a client write,
    /// reload or unload, and would be served stale until `item_ttl` expired.
    ///
    /// `parameters` are the values the originating request bound. They matter
    /// only in the no-plan branch, which rebuilds the query from the SQL text:
    /// that text still holds its placeholders, so re-running it without the
    /// values fails with `Placeholder '$1' was not provided a value for
    /// execution` and the stale entry it was meant to replace survives for the
    /// whole `item_ttl + stale_while_revalidate_ttl` window.
    fn prepare_revalidation_query(
        df: &Arc<DataFusion>,
        sql: &str,
        plan: Option<LogicalPlan>,
        parameters: Option<ParamValues>,
        cached_input_tables: Arc<HashSet<TableReference>>,
    ) -> (Query, Arc<HashSet<TableReference>>) {
        if let Some(logical_plan) = plan {
            tracing::debug!("Background revalidation: re-executing query with existing plan");
            let input_tables = Arc::new(cache::get_logical_plan_input_tables(&logical_plan));
            (
                super::Query::from_logical_plan(df, logical_plan),
                input_tables,
            )
        } else {
            tracing::debug!(
                "Background revalidation: re-executing query (will re-parse SQL); sql={}",
                sql
            );
            (
                super::QueryBuilder::new(sql, Arc::clone(df))
                    .parameters(parameters)
                    .build(),
                cached_input_tables,
            )
        }
    }

    /// Handles caching of query results after background revalidation
    ///
    /// Every path that returns without storing leaves the entry this
    /// revalidation was meant to replace in place, to be served stale until it
    /// expires. Since the queries themselves keep succeeding, the counter is
    /// the only thing that surfaces a revalidation that never lands.
    async fn cache_revalidation_result(
        df: &Arc<DataFusion>,
        cache_key: &RawCacheKey,
        batches: Vec<arrow::record_batch::RecordBatch>,
        schema: arrow::datatypes::SchemaRef,
        input_tables: Arc<HashSet<TableReference>>,
        revalidation_started_at: std::time::Instant,
        physical_plan: Option<Arc<dyn ExecutionPlan>>,
    ) {
        let cache_key_u64 = cache_key.as_u64();
        if let Some(cache_provider) = df.results_cache_provider() {
            // Skips encoding a result no lookup could serve; correctness comes
            // from the check every cache hit performs.
            if !cache_provider.is_servable(&input_tables, revalidation_started_at) {
                tracing::debug!(
                    cache_key = cache_key_u64,
                    "Background revalidation result is invalidated or past its TTL and stale window, discarding it"
                );
                record_revalidation_outcome(RevalidationOutcome::InvalidatedMidFlight);
                return;
            }

            // Skip cache writes if the revalidation result contains transient HTTP
            // error responses. Preserve the existing stale cache entry instead of
            // storing a partial result set.
            if !cache::batches_cacheable(&batches) {
                tracing::debug!(
                    cache_key = cache_key_u64,
                    "Background revalidation returned transient HTTP error responses, preserving stale cache"
                );
                record_revalidation_outcome(RevalidationOutcome::TransientErrors);
                return;
            }

            // `batches_cacheable` only sees a retryable status through the
            // `response_status` column or the schema-metadata fallback, both
            // of which a narrow projection (e.g. `SELECT rank FROM ...`) can
            // drop before this admission check ever runs — see
            // `cache::utils::plan_saw_transient_http_failure`.
            if physical_plan
                .as_ref()
                .is_some_and(cache::plan_saw_transient_http_failure)
            {
                tracing::debug!(
                    cache_key = cache_key_u64,
                    "Background revalidation's execution plan recorded a transient HTTP error response (5xx/429) that a projection excluded from the output columns, preserving stale cache"
                );
                record_revalidation_outcome(RevalidationOutcome::TransientErrors);
                return;
            }

            let cached_at = std::time::Instant::now();
            let encoder = cache_provider.encoder();

            // A separate question from the one above: the origin answered fine,
            // but the result may hold a column the copy could not decouple from
            // the memory its producer owns, so an entry over it could not be
            // billed for what it keeps alive. Only a raw entry can: an encoded
            // one keeps the serialized bytes and drops the arrays, so it pins
            // nothing whatever they rested on. Compacting first is what lets
            // `batches_boundable` report what the copy achieved rather than
            // guess from the column types, and it repeats the copy the entry
            // will store, which leaves an already-compact batch untouched.
            let batches = if encoder.is_some() {
                batches
            } else {
                let compacted: Vec<arrow::array::RecordBatch> = batches
                    .iter()
                    .map(arrow_tools::record_batch::compact_retained_buffers)
                    .collect();
                if !cache::batches_boundable(&compacted) {
                    tracing::debug!(
                        cache_key = cache_key_u64,
                        "Background revalidation returned a result the cache cannot bound, preserving stale cache"
                    );
                    record_revalidation_outcome(RevalidationOutcome::Unboundable);
                    return;
                }
                compacted
            };

            // Empty (0-row) revalidation results are cached too. The schema is
            // preserved separately in `CachedQueryResult`, so an empty result
            // refreshes the entry correctly rather than leaving the previous
            // (now stale) value in place.

            match cache::result::query::CachedQueryResult::from_batches(
                batches,
                schema,
                input_tables,
                cached_at,
                revalidation_started_at,
                encoder,
            )
            .await
            {
                Ok(cached_result) => {
                    match cache_provider
                        .store_raw_key(cache_key, cached_result, None)
                        .await
                    {
                        Ok(true) => {
                            tracing::debug!(
                                cache_key = cache_key_u64,
                                "Background revalidation completed successfully and cached"
                            );
                            record_revalidation_outcome(RevalidationOutcome::Stored);
                        }
                        // `store_raw_key` logs the reason.
                        Ok(false) => {
                            record_revalidation_outcome(RevalidationOutcome::InvalidatedMidFlight);
                        }
                        Err(e) => {
                            tracing::debug!(
                                cache_key = cache_key_u64,
                                "Background revalidation failed to cache results: {}",
                                e
                            );
                            record_revalidation_outcome(RevalidationOutcome::PutFailed);
                        }
                    }
                }
                Err(e) => {
                    tracing::debug!(
                        cache_key = cache_key_u64,
                        "Background revalidation failed to encode results: {}",
                        e
                    );
                    record_revalidation_outcome(RevalidationOutcome::EncodeFailed);
                }
            }
        } else {
            tracing::debug!("Background revalidation completed but cache provider unavailable");
        }
    }

    fn trigger_background_query_revalidation(
        df: Arc<DataFusion>,
        sql: &str,
        plan: Option<&LogicalPlan>,
        parameters: Option<&ParamValues>,
        cache_key: RawCacheKey,
        namespace: CacheNamespace,
        cached_input_tables: Arc<HashSet<TableReference>>,
    ) {
        // Cache keys with a background revalidation pending or running. A stale hit
        // claims its key before spawning anything, so every other stale hit on the key
        // finds the claim and skips, however the runtime schedules the tasks. A claim
        // taken inside the spawned task coalesces only the revalidations that overlap
        // in time: a task that starts after the first one finished runs the same query
        // again.
        //
        // Revalidations wait in the same admission queue as client queries, so at most a
        // quarter of the admission limit (the CPU-sized one when admission is unbounded) run
        // at once. A stale hit past that limit is served without one; the entry stays stale
        // and a later hit revalidates it.
        static REVALIDATION_CLAIMS: OnceLock<RevalidationClaims> = OnceLock::new();
        let claims = REVALIDATION_CLAIMS.get_or_init(|| {
            RevalidationClaims::new(
                std::time::Duration::from_mins(5),
                (df.query_admission_limit()
                    .unwrap_or_else(|| cpu_budget::cpu_budget().max_concurrent_queries())
                    / 4)
                .max(1),
            )
        });

        let cache_key_u64 = cache_key.as_u64();
        // Released when the revalidation ends, whether it stores, fails, or is dropped
        // with its runtime, so the next stale hit on this key can start another.
        let Some(claim) = claims.claim(cache_key_u64) else {
            tracing::debug!(
                cache_key = cache_key_u64,
                "Background revalidation already in progress for this cache key, or the revalidation limit is reached; skipped"
            );
            cache::metrics::sql_results::STALE_WHILE_REVALIDATE_SKIPPED.add(1, &[]);
            return;
        };

        // Create a background request context with NoCache to bypass cache lookup
        let background_context = Self::create_background_context(namespace);

        // Clone sql, plan and parameters for the async block
        let sql_owned = sql.to_string();
        let plan_owned = plan.cloned();
        let parameters_owned = parameters.cloned();

        // Get optional dedicated refresh runtime, fall back to current runtime if not configured
        let refresh_runtime = df.refresh_runtime().cloned();
        let deadline = tokio::time::Instant::from_std(claim.expires_at);
        let cancel_token = background_context.cancellation_token().clone();

        // Build the background task
        let background_task = async move {
            let _claim = claim;
            cache::metrics::sql_results::STALE_WHILE_REVALIDATE_BACKGROUND_QUERIES.add(1, &[]);

            tracing::debug!(
                cache_key = cache_key_u64,
                "Starting background revalidation task"
            );

            let (query, input_tables) = Self::prepare_revalidation_query(
                &df,
                &sql_owned,
                plan_owned,
                parameters_owned,
                cached_input_tables,
            );

            // Captured before the query reads anything, so any
            // invalidation of its tables that lands while it executes
            // is ordered after this point and rejects the write.
            let revalidation_started_at = std::time::Instant::now();

            // A revalidation holds one of the limited running permits, so one that hangs is
            // cancelled when its claim expires rather than keeping the permit.
            let revalidation = async {
                let result = background_context
                    .scope(async move { query.run().await })
                    .await;

                match result {
                    Ok(query_result) => {
                        let schema = query_result
                            .cached_schema()
                            .unwrap_or_else(|| query_result.schema());
                        let physical_plan = query_result.physical_plan();
                        tracing::debug!(
                            cache_key = cache_key_u64,
                            "Background query execution succeeded, collecting batches"
                        );
                        match query_result.collect_batches().await {
                            Ok(batches) => {
                                tracing::debug!(
                                    cache_key = cache_key_u64,
                                    num_batches = batches.len(),
                                    "Collected batches, now caching"
                                );
                                Self::cache_revalidation_result(
                                    &df,
                                    &cache_key,
                                    batches,
                                    schema,
                                    input_tables,
                                    revalidation_started_at,
                                    physical_plan,
                                )
                                .await;
                            }
                            Err(e) => {
                                tracing::debug!(
                                    cache_key = cache_key_u64,
                                    "Background revalidation failed during collection: {}",
                                    e
                                );
                                record_revalidation_outcome(failed_revalidation_outcome(
                                    &e,
                                    RevalidationOutcome::CollectFailed,
                                ));
                            }
                        }
                    }
                    Err(e) => {
                        tracing::debug!(
                            cache_key = cache_key_u64,
                            "Background revalidation query failed: {}",
                            e
                        );
                        record_revalidation_outcome(failed_revalidation_outcome(
                            &e,
                            RevalidationOutcome::QueryFailed,
                        ));
                    }
                }
            };
            if tokio::time::timeout_at(deadline, revalidation)
                .await
                .is_err()
            {
                tracing::debug!(
                    cache_key = cache_key_u64,
                    "Background revalidation did not finish before its deadline, cancelled"
                );
                // The query runs in its own task on the query runtime, so dropping the wait
                // for it leaves that task queued for admission or planning; cancelling the
                // context's token ends it too.
                cancel_token.cancel();
                record_revalidation_outcome(RevalidationOutcome::TimedOut);
            }

            tracing::debug!(
                cache_key = cache_key_u64,
                "Background revalidation task completed"
            );
        };

        // Spawn on dedicated refresh runtime if configured, otherwise use current runtime.
        // Using the dedicated refresh runtime isolates SWR background work from user-facing
        // query processing, preventing latency spikes when many cache entries become stale.
        if let Some(runtime) = refresh_runtime {
            runtime.spawn(background_task);
        } else {
            tokio::spawn(background_task);
        }
    }

    /// `read_started_at` is when the query began, and gates the cache write
    /// against any invalidation of `datasets` that lands while it runs — see
    /// [`to_cached_record_batch_stream`].
    pub(super) fn wrap_stream_with_cache(
        df: &DataFusion,
        stream: SendableRecordBatchStream,
        plan_cache_key: RawCacheKey,
        datasets: Arc<HashSet<TableReference>>,
        read_started_at: std::time::Instant,
        physical_plan: Arc<dyn ExecutionPlan>,
    ) -> SendableRecordBatchStream {
        if let Some(cache_provider) = df.results_cache_provider() {
            to_cached_record_batch_stream(
                cache_provider,
                stream,
                plan_cache_key,
                datasets,
                read_started_at,
                Some(physical_plan),
                df.ctx.task_ctx().memory_pool(),
            )
        } else {
            stream
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::{collections::HashSet, sync::Arc, time::Duration};

    use arrow::array::Int64Array;
    use arrow::datatypes::Schema;
    use datafusion::scalar::ScalarValue;

    use cache::{
        Caching, QueryResultsCacheProvider, SimpleCache, key::CacheKey, result::CacheStatus,
    };
    use spicepod::component::caching::SQLResultsCacheConfig;
    use tokio::runtime::Handle;

    use crate::{
        builder::RuntimeBuilder,
        datafusion::{
            DataFusion,
            flight_session_extension::FlightSessionExtension,
            query::{QueryBuilder, ResultsCacheMode},
        },
        status,
    };
    use datafusion::catalog::Session;
    use datafusion::datasource::TableProvider;
    use datafusion::logical_expr::{Expr, TableType};
    use datafusion::prelude::SessionContext;
    use runtime_request_context::{
        CacheControl, CacheKeyType, CacheNamespace, Protocol, RequestContext,
    };

    // Helper function to create a test RequestContext
    fn create_test_request_context(
        cache_control: CacheControl,
        user_cache_key: Option<String>,
    ) -> Arc<RequestContext> {
        Arc::new(
            RequestContext::builder(Protocol::Internal)
                .with_cache_control(cache_control)
                .with_client_supplied_cache_key(user_cache_key)
                .build(),
        )
    }

    #[tokio::test]
    async fn a_flight_session_does_not_use_the_shared_plans_cache() {
        let df = prepare_runtime(None).await;
        let without_session =
            create_test_request_context(CacheControl::Cache(CacheKeyType::Default), None);
        assert!(
            Query::shared_plans_cache_key(&df, "SELECT 1", &without_session).is_some(),
            "HTTP / default context still uses the shared plans cache"
        );

        let ext = FlightSessionExtension::new(Arc::new(SessionContext::new()), None);
        let with_session = Arc::new(
            RequestContext::builder(Protocol::Internal)
                .with_extension(ext)
                .build(),
        );
        assert!(
            Query::shared_plans_cache_key(&df, "SELECT 1", &with_session).is_none(),
            "a Flight session must not read or write the shared plans cache"
        );
    }

    /// Build a `RequestContext` with an explicit cache namespace. Used to
    /// drive cross-principal isolation tests in this module without going
    /// through real auth middleware.
    fn create_test_request_context_in_namespace(
        cache_control: CacheControl,
        namespace: CacheNamespace,
    ) -> Arc<RequestContext> {
        Arc::new(
            RequestContext::builder(Protocol::Internal)
                .with_cache_control(cache_control)
                .with_cache_namespace(namespace)
                .build(),
        )
    }

    fn dummy_servable_entry_cached_at(cached_at: std::time::Instant) -> ServableEntry {
        dummy_servable_entry_for_tables(cached_at, HashSet::new())
    }

    fn dummy_servable_entry_for_tables(
        cached_at: std::time::Instant,
        tables: HashSet<TableReference>,
    ) -> ServableEntry {
        ServableEntry {
            cached_result: cache::result::query::CachedQueryResult::new_raw(
                vec![],
                Arc::new(Schema::empty()),
                Arc::new(tables),
                cached_at,
                cached_at,
            ),
            cache_status: CacheStatus::CacheHit,
            revalidate: false,
        }
    }

    /// An encoded entry whose payload decodes to an Arrow IPC stream of `decoded_len` bytes.
    fn dummy_encoded_entry_cached_at(
        cached_at: std::time::Instant,
        decoded_len: usize,
    ) -> ServableEntry {
        ServableEntry {
            cached_result: cache::result::query::CachedQueryResult::new(
                bytes::Bytes::new(),
                decoded_len,
                Arc::new(Schema::empty()),
                Arc::new(HashSet::new()),
                cached_at,
                cached_at,
                None,
            ),
            cache_status: CacheStatus::CacheHit,
            revalidate: false,
        }
    }

    fn dummy_probe_hit(entry: ServableEntry) -> CacheProbe {
        CacheProbe::Hit(Box::new(ProbedHit {
            raw_key: RawCacheKey::new(1),
            entry,
            sql: Arc::from("SELECT 1"),
            revalidation_plan: None,
        }))
    }

    /// The instant an entry `age` old was cached at, measured back from `now`.
    fn cached_ago(now: std::time::Instant, age: Duration) -> std::time::Instant {
        now.checked_sub(age)
            .expect("the monotonic clock should be past the entry's age")
    }

    #[test]
    fn request_max_stale_cannot_exceed_provider_age_cap() {
        let provider = QueryResultsCacheProvider::try_new(
            &SQLResultsCacheConfig {
                item_ttl: Some("1s".to_string()),
                stale_while_revalidate_ttl: Some("1s".to_string()),
                ..Default::default()
            },
            Box::new([]),
        )
        .expect("create a provider with a two-second age cap");
        let now = std::time::Instant::now();
        for directive in [
            CacheControl::MaxStale(CacheKeyType::Default, Some(Duration::from_secs(5))),
            CacheControl::MaxStale(CacheKeyType::Default, None),
        ] {
            let policy = stale_policy(directive, Some(Duration::from_secs(1)));
            let mut expired =
                dummy_servable_entry_cached_at(cached_ago(now, Duration::from_millis(2_100)));
            assert!(
                !apply_age_eligibility(
                    &mut expired,
                    provider.ttl(),
                    provider.max_servable_age(),
                    policy,
                    now
                ),
                "{directive:?} must not serve beyond the provider's two-second age cap"
            );
            let mut stale =
                dummy_servable_entry_cached_at(cached_ago(now, Duration::from_millis(1_500)));
            assert!(apply_age_eligibility(
                &mut stale,
                provider.ttl(),
                provider.max_servable_age(),
                policy,
                now
            ));
            assert_eq!(stale.cache_status, CacheStatus::CacheStaleWhileRevalidate);
            assert!(stale.revalidate);
        }
    }

    #[test]
    fn a_probed_entry_past_item_ttl_is_not_served() {
        let now = std::time::Instant::now();
        let mut entry =
            dummy_servable_entry_cached_at(cached_ago(now, Duration::from_millis(1_500)));
        assert!(
            !apply_age_eligibility(
                &mut entry,
                Duration::from_secs(1),
                Duration::from_secs(2),
                StalePolicy::FreshOnly,
                now,
            ),
            "an encoded hit that waited past item_ttl must not be served"
        );
    }

    #[test]
    fn a_probed_entry_inside_item_ttl_is_served() {
        let now = std::time::Instant::now();
        let mut entry = dummy_servable_entry_cached_at(cached_ago(now, Duration::from_millis(500)));
        assert!(apply_age_eligibility(
            &mut entry,
            Duration::from_secs(1),
            Duration::from_secs(2),
            StalePolicy::FreshOnly,
            now,
        ));
        assert!(!entry.revalidate);
        assert_eq!(entry.cache_status, CacheStatus::CacheHit);
    }

    #[test]
    fn a_probed_entry_past_ttl_inside_the_stale_window_is_marked_for_revalidation() {
        let now = std::time::Instant::now();
        let mut entry =
            dummy_servable_entry_cached_at(cached_ago(now, Duration::from_millis(1_500)));
        assert!(apply_age_eligibility(
            &mut entry,
            Duration::from_secs(1),
            Duration::from_secs(2),
            StalePolicy::Window(Duration::from_secs(1)),
            now,
        ));
        assert!(entry.revalidate);
        assert_eq!(entry.cache_status, CacheStatus::CacheStaleWhileRevalidate);
    }

    #[test]
    fn a_probed_entry_past_the_stale_window_is_not_served() {
        let now = std::time::Instant::now();
        let mut entry =
            dummy_servable_entry_cached_at(cached_ago(now, Duration::from_millis(2_500)));
        assert!(!apply_age_eligibility(
            &mut entry,
            Duration::from_secs(1),
            Duration::from_secs(2),
            StalePolicy::Window(Duration::from_secs(1)),
            now,
        ));
    }

    #[test]
    fn a_max_stale_without_a_value_serves_an_old_probed_entry() {
        let now = std::time::Instant::now();
        let mut entry =
            dummy_servable_entry_cached_at(cached_ago(now, Duration::from_millis(1_500)));
        assert!(apply_age_eligibility(
            &mut entry,
            Duration::from_secs(1),
            Duration::from_secs(2),
            StalePolicy::AnyAge,
            now,
        ));
    }

    #[test]
    fn a_min_fresh_request_is_served_only_an_entry_fresh_for_that_much_longer() {
        assert_eq!(
            stale_policy(
                CacheControl::MinFresh(CacheKeyType::Default, Duration::from_millis(600)),
                Some(Duration::from_secs(10)),
            ),
            StalePolicy::FreshFor(Duration::from_millis(600))
        );
        let now = std::time::Instant::now();
        let policy = StalePolicy::FreshFor(Duration::from_millis(600));
        for (age_ms, servable) in [(300, true), (500, false), (1_500, false)] {
            let mut entry =
                dummy_servable_entry_cached_at(cached_ago(now, Duration::from_millis(age_ms)));
            assert_eq!(
                apply_age_eligibility(
                    &mut entry,
                    Duration::from_secs(1),
                    Duration::from_secs(11),
                    policy,
                    now,
                ),
                servable,
                "an entry {age_ms} ms old with a 1 s item_ttl and min-fresh of 600 ms"
            );
            assert!(!entry.revalidate);
            assert_eq!(entry.cache_status, CacheStatus::CacheHit);
        }
    }

    /// An `only-if-cached` request is answered from the cache or refused; it never
    /// runs the query.
    #[tokio::test]
    async fn an_only_if_cached_request_fails_on_a_miss_and_is_served_a_hit() {
        for (key_type, cache_key_type) in [
            (
                spicepod::component::caching::CacheKeyType::Plan,
                CacheKeyType::Default,
            ),
            (
                spicepod::component::caching::CacheKeyType::Sql,
                CacheKeyType::Raw,
            ),
        ] {
            let df = prepare_runtime(Some(SQLResultsCacheConfig {
                item_ttl: Some("10m".to_string()),
                cache_key_type: key_type,
                ..Default::default()
            }))
            .await;
            let only_if_cached =
                create_test_request_context(CacheControl::OnlyIfCached(cache_key_type), None);

            let miss = Arc::clone(&only_if_cached)
                .scope(QueryBuilder::new("SELECT 7", Arc::clone(&df)).build().run())
                .await;
            assert!(
                matches!(miss, Err(super::super::Error::NotCached)),
                "{key_type:?}: a miss must be refused, got {:?}",
                miss.map(|result| result.cache_status)
            );

            let (status, value) =
                create_test_request_context(CacheControl::Cache(cache_key_type), None)
                    .scope(run_i64_query(&df, "SELECT 7", ResultsCacheMode::Default))
                    .await;
            assert_eq!((status, value), (CacheStatus::CacheMiss, 7));

            let (status, value) = only_if_cached
                .scope(run_i64_query(&df, "SELECT 7", ResultsCacheMode::Default))
                .await;
            assert_eq!((status, value), (CacheStatus::CacheHit, 7), "{key_type:?}");
        }
    }

    #[test]
    fn stale_policy_matches_lookup_when_the_request_sets_max_stale() {
        assert_eq!(
            stale_policy(CacheControl::Cache(CacheKeyType::Default), None),
            StalePolicy::FreshOnly
        );
        assert_eq!(
            stale_policy(
                CacheControl::Cache(CacheKeyType::Default),
                Some(Duration::from_secs(1)),
            ),
            StalePolicy::Window(Duration::from_secs(1))
        );
        assert_eq!(
            stale_policy(
                CacheControl::MaxStale(CacheKeyType::Default, Some(Duration::from_secs(5))),
                Some(Duration::from_secs(1)),
            ),
            StalePolicy::Window(Duration::from_secs(5))
        );
        assert_eq!(
            stale_policy(
                CacheControl::MaxStale(CacheKeyType::Default, None),
                Some(Duration::from_secs(1)),
            ),
            StalePolicy::AnyAge
        );
    }

    #[tokio::test]
    async fn a_raw_hit_past_item_ttl_is_not_servable_in_place() {
        let df = prepare_runtime(Some(SQLResultsCacheConfig {
            item_ttl: Some("1s".to_string()),
            cache_key_type: spicepod::component::caching::CacheKeyType::Sql,
            ..Default::default()
        }))
        .await;
        let now = std::time::Instant::now();
        let probe = dummy_probe_hit(dummy_servable_entry_cached_at(cached_ago(
            now,
            Duration::from_millis(1_500),
        )));
        assert!(
            probe.is_servable_in_place(),
            "a raw hit still looks in-place before the serve-time recheck"
        );
        let probe = probe.into_miss_if_in_place_ineligible(
            CacheControl::Cache(CacheKeyType::Default),
            df.results_cache_provider().as_deref(),
        );
        assert!(
            !probe.is_servable_in_place(),
            "a raw hit past item_ttl must hop so planning is not on the request runtime"
        );
        assert!(
            matches!(probe, CacheProbe::Missed(_)),
            "the ineligible raw hit is a miss so already_looked_up skips a second counted lookup"
        );
    }

    #[tokio::test]
    async fn a_raw_hit_inside_item_ttl_is_servable_in_place() {
        let df = prepare_runtime(Some(SQLResultsCacheConfig {
            item_ttl: Some("10s".to_string()),
            cache_key_type: spicepod::component::caching::CacheKeyType::Sql,
            ..Default::default()
        }))
        .await;
        let now = std::time::Instant::now();
        let probe = dummy_probe_hit(dummy_servable_entry_cached_at(cached_ago(
            now,
            Duration::from_millis(500),
        )))
        .into_miss_if_in_place_ineligible(
            CacheControl::Cache(CacheKeyType::Default),
            df.results_cache_provider().as_deref(),
        );
        assert!(
            probe.is_servable_in_place(),
            "a fresh raw hit must still be served where the request arrived"
        );
        assert!(matches!(probe, CacheProbe::Hit(_)));
    }

    #[tokio::test]
    async fn an_invalidated_raw_hit_is_not_servable_in_place() {
        let df = prepare_runtime(Some(SQLResultsCacheConfig {
            item_ttl: Some("10m".to_string()),
            cache_key_type: spicepod::component::caching::CacheKeyType::Sql,
            ..Default::default()
        }))
        .await;
        let provider = df
            .results_cache_provider()
            .expect("the test runtime has a results cache");
        // The entry's read must predate the mark: a change recorded
        // before `read_started_at` is not a reason to reject it.
        let probe = dummy_probe_hit(dummy_servable_entry_for_tables(
            cached_ago(std::time::Instant::now(), Duration::from_secs(1)),
            HashSet::from([TableReference::bare("orders")]),
        ));
        provider
            .invalidate_for_table(TableReference::bare("orders"))
            .await
            .expect("the table-change clock should record the invalidation");
        let probe = probe.into_miss_if_in_place_ineligible(
            CacheControl::Cache(CacheKeyType::Default),
            Some(&provider),
        );
        assert!(
            !probe.is_servable_in_place(),
            "a table-change invalidation after the probe must hop to the query runtime"
        );
        assert!(matches!(probe, CacheProbe::Missed(_)));
    }

    #[tokio::test]
    async fn a_raw_hit_inside_the_stale_window_stays_servable_in_place() {
        let df = prepare_runtime(Some(SQLResultsCacheConfig {
            item_ttl: Some("1s".to_string()),
            stale_while_revalidate_ttl: Some("5s".to_string()),
            cache_key_type: spicepod::component::caching::CacheKeyType::Sql,
            ..Default::default()
        }))
        .await;
        let now = std::time::Instant::now();
        let probe = dummy_probe_hit(dummy_servable_entry_cached_at(cached_ago(
            now,
            Duration::from_millis(1_500),
        )))
        .into_miss_if_in_place_ineligible(
            CacheControl::Cache(CacheKeyType::Default),
            df.results_cache_provider().as_deref(),
        );
        assert!(
            probe.is_servable_in_place(),
            "a raw hit past item_ttl but inside the stale window is still served in place"
        );
        assert!(matches!(probe, CacheProbe::Hit(_)));
    }

    #[tokio::test]
    async fn an_encoded_hit_over_the_inline_decode_budget_stays_a_hit_and_is_not_servable_in_place()
    {
        let df = prepare_runtime(Some(SQLResultsCacheConfig {
            item_ttl: Some("1s".to_string()),
            cache_key_type: spicepod::component::caching::CacheKeyType::Sql,
            ..Default::default()
        }))
        .await;
        let now = std::time::Instant::now();
        let probe = dummy_probe_hit(dummy_encoded_entry_cached_at(
            cached_ago(now, Duration::from_millis(1_500)),
            INLINE_DECODE_MAX_BYTES + 1,
        ))
        .into_miss_if_in_place_ineligible(
            CacheControl::Cache(CacheKeyType::Default),
            df.results_cache_provider().as_deref(),
        );
        assert!(
            !probe.is_servable_in_place(),
            "an encoded hit over the inline decode budget hops so it can decode on the query runtime"
        );
        assert!(
            matches!(probe, CacheProbe::Hit(_)),
            "an encoded hit stays a hit so serve_probed_hit can recheck after the hop"
        );
    }

    /// A small encoded hit is decoded where the request arrived, so like a raw hit it has to
    /// pass the serve-time checks before it counts as servable there: past `item_ttl` it is a
    /// miss, and the query hops onto the query runtime to plan.
    #[tokio::test]
    async fn a_small_encoded_hit_past_item_ttl_is_not_servable_in_place() {
        let df = prepare_runtime(Some(SQLResultsCacheConfig {
            item_ttl: Some("1s".to_string()),
            cache_key_type: spicepod::component::caching::CacheKeyType::Sql,
            ..Default::default()
        }))
        .await;
        let now = std::time::Instant::now();
        let probe = dummy_probe_hit(dummy_encoded_entry_cached_at(
            cached_ago(now, Duration::from_millis(1_500)),
            64,
        ));
        assert!(
            probe.is_servable_in_place(),
            "a small encoded hit is decoded where the request arrived"
        );
        let probe = probe.into_miss_if_in_place_ineligible(
            CacheControl::Cache(CacheKeyType::Default),
            df.results_cache_provider().as_deref(),
        );
        assert!(
            !probe.is_servable_in_place(),
            "a small encoded hit past item_ttl must hop so planning is not on the request runtime"
        );
        assert!(
            matches!(probe, CacheProbe::Missed(_)),
            "the ineligible encoded hit is a miss so already_looked_up skips a second counted lookup"
        );
    }

    /// A raw hit that passes `into_miss_if_in_place_ineligible` can still
    /// fail at serve time (table-clock mark between those checks). That
    /// reject must become a miss so `Query::run` hops instead of planning
    /// on the request I/O runtime.
    #[tokio::test]
    async fn a_serve_time_rejected_raw_hit_is_not_servable_in_place() {
        let df = prepare_runtime(Some(SQLResultsCacheConfig {
            item_ttl: Some("10m".to_string()),
            cache_key_type: spicepod::component::caching::CacheKeyType::Sql,
            ..Default::default()
        }))
        .await;
        let provider = df
            .results_cache_provider()
            .expect("the test runtime has a results cache");
        let probe = dummy_probe_hit(dummy_servable_entry_for_tables(
            cached_ago(std::time::Instant::now(), Duration::from_secs(1)),
            HashSet::from([TableReference::bare("orders")]),
        ));
        let probe = probe.into_miss_if_in_place_ineligible(
            CacheControl::Cache(CacheKeyType::Default),
            Some(&provider),
        );
        assert!(
            probe.is_servable_in_place(),
            "the hit is still in-place before the post-classification invalidate"
        );
        provider
            .invalidate_for_table(TableReference::bare("orders"))
            .await
            .expect("the table-change clock should record the invalidation");

        let CacheProbe::Hit(hit) = probe else {
            panic!("into_miss should have kept the fresh raw hit");
        };
        let raw_key = hit.raw_key();
        let request_context =
            create_test_request_context(CacheControl::Cache(CacheKeyType::Default), None);
        let mut query = QueryBuilder::new("SELECT 1", Arc::clone(&df)).build();
        assert!(
            query
                .serve_probed_hit(&request_context, *hit)
                .await
                .is_none(),
            "serve must reject after the table-change clock marks the input"
        );
        let probe = CacheProbe::Missed(raw_key);
        assert!(
            !probe.is_servable_in_place(),
            "a serve-time reject must hop so planning is not on the request runtime"
        );
    }

    async fn prepare_runtime(
        results_cache_config: Option<SQLResultsCacheConfig>,
    ) -> Arc<DataFusion> {
        let plans_cache = Arc::new(SimpleCache::new(
            512,
            Duration::from_hours(1),
            std::hash::BuildHasherDefault::<twox_hash::XxHash3_64>::default(),
        ));
        let results_cache_config = results_cache_config.unwrap_or(SQLResultsCacheConfig {
            item_ttl: Some("10m".to_string()),
            cache_key_type: spicepod::component::caching::CacheKeyType::Plan,
            ..Default::default()
        });

        let cache_provider =
            QueryResultsCacheProvider::try_new(&results_cache_config, Box::new([]))
                .expect("valid cache provider");

        let runtime = RuntimeBuilder::new().build().await;

        Arc::new(
            DataFusion::builder(
                status::RuntimeStatus::new(),
                runtime.accelerator_engine_registry(),
                Handle::current(),
            )
            .with_caching(Arc::new(
                Caching::new()
                    .with_results_cache(Arc::new(cache_provider))
                    .with_plans_cache(plans_cache),
            ))
            .build(),
        )
    }

    #[tokio::test]
    async fn test_request_cache_manager() {
        let cache_status = CacheStatus::CacheHit;
        let raw_cache_key =
            CacheKey::Query("test-key", None).as_raw_key(Box::new(std::hash::DefaultHasher::new()));

        let manager = RequestCacheManager::new(cache_status, raw_cache_key);
        assert!(manager.should_cache_results());
    }

    async fn run_i64_query(
        df: &Arc<DataFusion>,
        sql: &str,
        results_cache_mode: ResultsCacheMode,
    ) -> (CacheStatus, i64) {
        let result = QueryBuilder::new(sql, Arc::clone(df))
            .results_cache_mode(results_cache_mode)
            .build()
            .run()
            .await
            .expect("query should succeed");
        let cache_status = result.cache_status;
        let records = result
            .collect_batches()
            .await
            .expect("query should return records");
        let value = records
            .first()
            .expect("query should return one batch")
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("query should return an Int64 column")
            .value(0);
        (cache_status, value)
    }

    #[tokio::test]
    async fn test_results_cache_bypass_skips_lookup_and_storage() {
        const SQL: &str = "SELECT scan FROM bypass_counted";
        let df = prepare_runtime(Some(SQLResultsCacheConfig {
            item_ttl: Some("10m".to_string()),
            cache_key_type: spicepod::component::caching::CacheKeyType::Sql,
            ..Default::default()
        }))
        .await;
        let scans = register_counted_table(&df, "bypass_counted");
        let request_context = create_test_request_context(
            CacheControl::Cache(CacheKeyType::ClientSupplied),
            Some("bypass-regression".to_string()),
        );

        let (status, value) = Arc::clone(&request_context)
            .scope(run_i64_query(&df, SQL, ResultsCacheMode::Default))
            .await;
        assert_eq!(status, CacheStatus::CacheMiss);
        assert_eq!(value, 1);

        // The same client cache key and SQL are warm, but bypass must execute
        // the query again (the second scan).
        let (status, value) = Arc::clone(&request_context)
            .scope(run_i64_query(&df, SQL, ResultsCacheMode::Bypass))
            .await;
        assert_eq!(status, CacheStatus::CacheDisabled);
        assert_eq!(value, 2);

        // Bypass must not replace the warm entry. A normal request with the same
        // client key and SQL still returns the first scan, not the second.
        let (status, value) = request_context
            .scope(run_i64_query(&df, SQL, ResultsCacheMode::Default))
            .await;
        assert_eq!(status, CacheStatus::CacheHit);
        assert_eq!(value, 1);
        assert_eq!(scans.load(std::sync::atomic::Ordering::SeqCst), 2);
    }

    /// [`run_i64_query`] as a request that keys the results cache the way
    /// `key_type` configures it: by the logical plan, or by the SQL text, whose
    /// hit is answered before the query is planned.
    async fn run_i64_query_keyed(
        df: &Arc<DataFusion>,
        sql: &str,
        key_type: spicepod::component::caching::CacheKeyType,
    ) -> (CacheStatus, i64) {
        let cache_key_type = if matches!(key_type, spicepod::component::caching::CacheKeyType::Sql)
        {
            CacheKeyType::Raw
        } else {
            CacheKeyType::Default
        };
        create_test_request_context(CacheControl::Cache(cache_key_type), None)
            .scope(run_i64_query(df, sql, ResultsCacheMode::Default))
            .await
    }

    /// Plans with volatile functions (`random()`, `uuid()`) or time functions
    /// (`now()`, `current_date`) must never be served from, or stored in, the
    /// results cache, under every cache key type. A deterministic control
    /// query on the same runtime still hits.
    #[tokio::test]
    async fn test_results_cache_skips_non_deterministic_plans() {
        for key_type in [
            spicepod::component::caching::CacheKeyType::Plan,
            spicepod::component::caching::CacheKeyType::Sql,
        ] {
            let df = prepare_runtime(Some(SQLResultsCacheConfig {
                item_ttl: Some("10m".to_string()),
                cache_key_type: key_type,
                ..Default::default()
            }))
            .await;

            for sql in [
                "SELECT CAST(random() * 1000000000000 AS BIGINT)",
                "SELECT CAST(length(uuid()) AS BIGINT) + CAST(random() * 1000000000000 AS BIGINT)",
                "SELECT CAST(date_part('epoch', now()) * 1000000 AS BIGINT)",
                "SELECT CAST(date_part('day', current_date) AS BIGINT)",
            ] {
                for _ in 0..2 {
                    let (status, _) = run_i64_query_keyed(&df, sql, key_type).await;
                    assert_eq!(
                        status,
                        CacheStatus::CacheDisabled,
                        "{key_type:?}: non-deterministic query must not use the results cache: {sql}"
                    );
                }
            }

            let (status, _) = run_i64_query_keyed(&df, "SELECT 42", key_type).await;
            assert_eq!(status, CacheStatus::CacheMiss, "{key_type:?}");
            let (status, value) = run_i64_query_keyed(&df, "SELECT 42", key_type).await;
            assert_eq!(status, CacheStatus::CacheHit, "{key_type:?}");
            assert_eq!(value, 42);
        }
    }

    /// Registers `name()`, a `from: sql` table function whose one `v` row is
    /// drawn from `random()`, parsed from YAML in the shape a spicepod
    /// declares it. `volatility: None` leaves the key out, so the declaration
    /// takes the spicepod default (`volatile`).
    async fn register_random_sql_table_function(
        df: &DataFusion,
        name: &str,
        volatility: Option<&str>,
    ) {
        use runtime_datafusion_udfs::user_functions::{BuiltFunction, build_function};

        let volatility = volatility.map_or_else(String::new, |v| format!("volatility: {v}\n"));
        let decl: spicepod::component::function::Function = yaml::from_str(&format!(
            "
name: {name}
from: sql
kind: table
{volatility}signature:
  returns:
    - {{ name: v, type: int64 }}
body: SELECT CAST(random() * 1000000000000 AS BIGINT) AS v
"
        ))
        .expect("the function declaration should parse");
        let BuiltFunction::Table(udtf) = build_function(&decl)
            .await
            .expect("the function should build")
        else {
            panic!("a `kind: table` declaration should build a table function");
        };
        df.ctx.register_udtf(name, udtf);
    }

    /// A SQL table function's body is planned only inside its provider's
    /// `scan`, so the plan the results cache checks holds a `TableScan` of the
    /// function and never the `random()` in its body. A table function not
    /// declared `immutable` (the default is `volatile`) must never be served
    /// from, or stored in, the results cache, whether the query scans it
    /// directly or in a scalar subquery. The same body declared `immutable`
    /// is still cached, so the declaration is what decides.
    #[tokio::test]
    async fn test_results_cache_skips_non_immutable_sql_table_functions() {
        for key_type in [
            spicepod::component::caching::CacheKeyType::Plan,
            spicepod::component::caching::CacheKeyType::Sql,
        ] {
            let df = prepare_runtime(Some(SQLResultsCacheConfig {
                item_ttl: Some("10m".to_string()),
                cache_key_type: key_type,
                ..Default::default()
            }))
            .await;
            register_random_sql_table_function(&df, "default_rows", None).await;
            register_random_sql_table_function(&df, "stable_rows", Some("stable")).await;
            register_random_sql_table_function(&df, "immutable_rows", Some("immutable")).await;

            for sql in [
                "SELECT v FROM default_rows()",
                "SELECT v FROM stable_rows()",
                "SELECT (SELECT max(v) FROM default_rows())",
            ] {
                let (first_status, first) = run_i64_query_keyed(&df, sql, key_type).await;
                let (second_status, second) = run_i64_query_keyed(&df, sql, key_type).await;
                assert_eq!(
                    (first_status, second_status),
                    (CacheStatus::CacheDisabled, CacheStatus::CacheDisabled),
                    "{key_type:?}: a non-immutable table function must not use the results cache: {sql}"
                );
                // Each run executes the body again. Two independent draws of
                // `random() * 1e12` collide with probability about 1e-12.
                assert_ne!(
                    first, second,
                    "{key_type:?}: the second run must not replay the first run's rows: {sql}"
                );
            }

            let sql = "SELECT v FROM immutable_rows()";
            let (status, first) = run_i64_query_keyed(&df, sql, key_type).await;
            assert_eq!(status, CacheStatus::CacheMiss, "{key_type:?}");
            let (status, second) = run_i64_query_keyed(&df, sql, key_type).await;
            assert_eq!(status, CacheStatus::CacheHit, "{key_type:?}");
            assert_eq!(first, second, "{key_type:?}");
        }
    }

    /// A one-row table whose value is the cache namespace of the request that
    /// planned the scan. It reads the namespace the way the caching accelerator
    /// scopes its rows: from the `RequestContext` that `Query::run_internal`
    /// attaches to the session, since `DataFusion` does not carry the task-local
    /// across `scan`.
    #[derive(Debug)]
    struct NamespaceEchoTable {
        schema: arrow::datatypes::SchemaRef,
    }

    #[async_trait::async_trait]
    impl TableProvider for NamespaceEchoTable {
        fn schema(&self) -> arrow::datatypes::SchemaRef {
            Arc::clone(&self.schema)
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
        ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
            let namespace = state
                .config()
                .get_extension::<RequestContext>()
                .map(|context| context.cache_namespace().storage_id().to_string());
            let batch = arrow::array::RecordBatch::try_new(
                Arc::clone(&self.schema),
                vec![Arc::new(arrow::array::StringArray::from(vec![namespace]))],
            )?;
            datafusion::datasource::MemTable::try_new(Arc::clone(&self.schema), vec![vec![batch]])?
                .scan(state, projection, filters, limit)
                .await
        }
    }

    /// Runs `sql` to completion under `request_context`, draining the stream so
    /// any cache write completes, and returns the cache status with the first
    /// row's string value.
    async fn run_string_query(
        df: &Arc<DataFusion>,
        request_context: &Arc<RequestContext>,
        sql: &'static str,
    ) -> (CacheStatus, Option<String>) {
        use arrow::array::Array;

        let query = QueryBuilder::new(sql, Arc::clone(df)).build();
        Arc::clone(request_context)
            .scope(async move {
                let result = query.run().await.expect("query should succeed");
                let cache_status = result.cache_status;
                let batches = result.collect_batches().await.expect("should drain");
                let column = batches
                    .first()
                    .expect("query should return one batch")
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow::array::StringArray>()
                    .expect("query should return a Utf8 column");
                let value = (!column.is_null(0)).then(|| column.value(0).to_string());
                (cache_status, value)
            })
            .await
    }

    /// SWR background revalidation must run under the originating principal's
    /// namespace, not `System` or `Public`. Whatever the refresh reads that is
    /// scoped per principal, such as the caching accelerator's rows, it must
    /// read as the principal it refreshes for, because the result is stored
    /// under that principal's key.
    ///
    /// The query reads a table that echoes the namespace its scan was planned
    /// under, so each cached result records which principal produced it.
    /// Alice triggers SWR, and once the background refresh lands, her fresh
    /// HIT must still read `apikey:alice`. Bob, running the same SQL, gets a
    /// MISS that runs as him: the refresh did not leak into his scope.
    #[tokio::test]
    async fn test_swr_revalidation_inherits_originating_namespace() {
        let sql = "SELECT ns FROM whoami";
        let df = prepare_runtime(Some(SQLResultsCacheConfig {
            item_ttl: Some("1s".to_string()),
            cache_key_type: spicepod::component::caching::CacheKeyType::Sql,
            stale_while_revalidate_ttl: Some("5s".to_string()),
            ..Default::default()
        }))
        .await;
        let schema = Arc::new(Schema::new(vec![arrow::datatypes::Field::new(
            "ns",
            arrow::datatypes::DataType::Utf8,
            true,
        )]));
        df.ctx
            .register_table(
                TableReference::bare("whoami"),
                Arc::new(NamespaceEchoTable { schema }),
            )
            .expect("should register table");

        let alice = create_test_request_context_in_namespace(
            CacheControl::MaxStale(CacheKeyType::Raw, Some(Duration::from_secs(5))),
            CacheNamespace::Principal("apikey:alice".into()),
        );
        let bob = create_test_request_context_in_namespace(
            CacheControl::Cache(CacheKeyType::Raw),
            CacheNamespace::Principal("apikey:bob".into()),
        );
        let as_alice = Some("apikey:alice".to_string());

        // Alice populates her namespace.
        assert_eq!(
            run_string_query(&df, &alice, sql).await,
            (CacheStatus::CacheMiss, as_alice.clone())
        );

        // Wait past TTL but within stale-while-revalidate window.
        tokio::time::sleep(Duration::from_millis(1500)).await;

        // Alice's stale request is served her entry and triggers background
        // revalidation.
        assert_eq!(
            run_string_query(&df, &alice, sql).await,
            (CacheStatus::CacheStaleWhileRevalidate, as_alice.clone())
        );

        // Poll for the background revalidation instead of sleeping a fixed
        // interval. A fresh HIT proves the refresh was stored under Alice's
        // key; its value proves the refresh ran as Alice.
        let mut last = (CacheStatus::CacheStaleWhileRevalidate, None);
        for _ in 0..100 {
            if let Some(cp) = df.results_cache_provider() {
                cp.run_pending_tasks().await;
            }
            last = run_string_query(&df, &alice, sql).await;
            if last.0 == CacheStatus::CacheHit {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert_eq!(
            last,
            (CacheStatus::CacheHit, as_alice),
            "the background revalidation must refresh alice's entry, running as alice"
        );

        // Bob still sees MISS for the same SQL — SWR did not bleed Alice's
        // entry into a cross-user scope.
        assert_eq!(
            run_string_query(&df, &bob, sql).await,
            (CacheStatus::CacheMiss, Some("apikey:bob".to_string())),
            "SWR refresh must not leak into bob's scope"
        );
    }

    /// Registers an empty in-memory table, so a query over it records a real
    /// input table for the cache entry to be invalidated on.
    fn register_empty_table(df: &Arc<DataFusion>, name: &'static str) {
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
        ]));
        let table = datafusion::datasource::MemTable::try_new(
            Arc::clone(&schema),
            vec![vec![arrow::array::RecordBatch::new_empty(schema)]],
        )
        .expect("valid mem table");
        df.ctx
            .register_table(TableReference::bare(name), Arc::new(table))
            .expect("should register table");
    }

    /// Runs `sql` to completion under `request_context`, draining the stream so
    /// any cache write completes, and returns the observed cache status.
    async fn run_and_drain(
        df: Arc<DataFusion>,
        request_context: Arc<RequestContext>,
        sql: &'static str,
    ) -> CacheStatus {
        let query = QueryBuilder::new(sql, df).build();
        request_context
            .scope(async move {
                let result = query.run().await.expect("query should succeed");
                let cache_status = result.cache_status;
                let _ = result.collect_batches().await.expect("should drain");
                cache_status
            })
            .await
    }

    /// Regression test for #12672: a stale-while-revalidate revalidation must
    /// preserve the input-table set of the entry it replaces.
    ///
    /// Under `cache_key_type: sql` the stale hit is found on the raw-SQL key,
    /// before any `LogicalPlan` exists, so the revalidation path had no plan to
    /// derive input tables from and substituted an empty set. An entry with an
    /// empty set matches no table, so `invalidate_for_table` could never evict
    /// it and it was served stale until `item_ttl` expired — regardless of how
    /// many accelerated refreshes ran in the meantime.
    #[tokio::test]
    async fn test_swr_revalidation_preserves_input_tables() {
        const SQL: &str = "SELECT count(*) FROM swr_table";

        let df = prepare_runtime(Some(SQLResultsCacheConfig {
            item_ttl: Some("1s".to_string()),
            cache_key_type: spicepod::component::caching::CacheKeyType::Sql,
            stale_while_revalidate_ttl: Some("5m".to_string()),
            ..Default::default()
        }))
        .await;

        register_empty_table(&df, "swr_table");

        let request_context =
            create_test_request_context(CacheControl::Cache(CacheKeyType::Raw), None);

        assert_eq!(
            run_and_drain(Arc::clone(&df), Arc::clone(&request_context), SQL).await,
            CacheStatus::CacheMiss
        );

        // Age the entry past its TTL into the stale-while-revalidate window.
        // The sleep is the behavior under test (TTL expiry), not a readiness wait.
        tokio::time::sleep(Duration::from_millis(1_100)).await;

        assert_eq!(
            run_and_drain(Arc::clone(&df), Arc::clone(&request_context), SQL).await,
            CacheStatus::CacheStaleWhileRevalidate
        );

        // Poll for the background revalidation instead of sleeping a fixed
        // interval: a rewritten entry is fresh again, so the status returns to
        // CacheHit once the revalidation has stored its result.
        let mut revalidated = false;
        for _ in 0..100 {
            if run_and_drain(Arc::clone(&df), Arc::clone(&request_context), SQL).await
                == CacheStatus::CacheHit
            {
                revalidated = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(
            revalidated,
            "background revalidation never refreshed the cache entry"
        );

        // A mutation must hard-evict the revalidated entry by its input table.
        df.caching()
            .invalidate_for_table(TableReference::bare("swr_table"))
            .await
            .expect("invalidation should succeed");
        if let Some(cache_provider) = df.results_cache_provider() {
            cache_provider.run_pending_tasks().await;
        }

        assert_eq!(
            run_and_drain(df, request_context, SQL).await,
            CacheStatus::CacheMiss,
            "a mutation must hard-evict a revalidated entry by its input table"
        );
    }

    /// Refresh leaves a recent SQL result fresh until its read-start TTL expires.
    #[tokio::test]
    async fn test_refresh_leaves_results_fresh_with_a_stale_window() {
        const SQL: &str = "SELECT count(*) FROM refreshed_table";

        let df = prepare_runtime(Some(SQLResultsCacheConfig {
            // Long enough that the result remains fresh during the refresh.
            item_ttl: Some("10m".to_string()),
            cache_key_type: spicepod::component::caching::CacheKeyType::Sql,
            stale_while_revalidate_ttl: Some("5m".to_string()),
            ..Default::default()
        }))
        .await;
        register_empty_table(&df, "refreshed_table");

        let request_context =
            create_test_request_context(CacheControl::Cache(CacheKeyType::Raw), None);

        assert_eq!(
            run_and_drain(Arc::clone(&df), Arc::clone(&request_context), SQL).await,
            CacheStatus::CacheMiss
        );
        assert_eq!(
            run_and_drain(Arc::clone(&df), Arc::clone(&request_context), SQL).await,
            CacheStatus::CacheHit
        );

        df.caching()
            .invalidate_for_refresh(TableReference::bare("refreshed_table"))
            .await
            .expect("invalidation should succeed");
        if let Some(cache_provider) = df.results_cache_provider() {
            cache_provider.run_pending_tasks().await;
        }

        assert_eq!(
            run_and_drain(Arc::clone(&df), Arc::clone(&request_context), SQL).await,
            CacheStatus::CacheHit,
            "a refresh must leave the previous result servable rather than flushing it"
        );
    }

    /// A mutation hard-evicts results when no stale window is configured.
    #[tokio::test]
    async fn test_invalidation_stays_hard_without_a_stale_window() {
        const SQL: &str = "SELECT count(*) FROM evicted_table";

        let df = prepare_runtime(Some(SQLResultsCacheConfig {
            item_ttl: Some("10m".to_string()),
            cache_key_type: spicepod::component::caching::CacheKeyType::Sql,
            ..Default::default()
        }))
        .await;
        register_empty_table(&df, "evicted_table");

        let request_context =
            create_test_request_context(CacheControl::Cache(CacheKeyType::Raw), None);

        assert_eq!(
            run_and_drain(Arc::clone(&df), Arc::clone(&request_context), SQL).await,
            CacheStatus::CacheMiss
        );
        assert_eq!(
            run_and_drain(Arc::clone(&df), Arc::clone(&request_context), SQL).await,
            CacheStatus::CacheHit
        );

        df.caching()
            .invalidate_for_table(TableReference::bare("evicted_table"))
            .await
            .expect("invalidation should succeed");
        if let Some(cache_provider) = df.results_cache_provider() {
            cache_provider.run_pending_tasks().await;
        }

        assert_eq!(
            run_and_drain(df, request_context, SQL).await,
            CacheStatus::CacheMiss,
            "a mutation flushes dependent results without a stale window"
        );
    }

    /// A background revalidation must not repopulate an entry whose table was
    /// invalidated *while the revalidation was running*. The revalidation may
    /// have read the pre-invalidation snapshot, and because invalidation can
    /// only remove entries that already exist, storing the result afterwards
    /// resurrects data the mutation had evicted.
    ///
    /// Driving `cache_revalidation_result` directly makes the ordering
    /// deterministic; interleaving a spawned revalidation task with an
    /// invalidation would be timing-dependent.
    #[tokio::test]
    async fn test_swr_revalidation_discards_result_invalidated_mid_flight() {
        let df = prepare_runtime(Some(SQLResultsCacheConfig {
            item_ttl: Some("10m".to_string()),
            cache_key_type: spicepod::component::caching::CacheKeyType::Sql,
            ..Default::default()
        }))
        .await;
        let cache_provider = df
            .results_cache_provider()
            .expect("results cache should be configured");

        let (schema, batch) = one_row_batch();

        // The revalidation begins its read here...
        let revalidation_started_at = std::time::Instant::now();

        // ...and a mutation invalidates the table before the result is stored.
        df.caching()
            .invalidate_for_table(TableReference::bare("revalidated_table"))
            .await
            .expect("invalidation should succeed");

        let invalidated_key = RawCacheKey::new(11);
        Query::cache_revalidation_result(
            &df,
            &invalidated_key,
            vec![batch.clone()],
            Arc::clone(&schema),
            Arc::new(HashSet::from([TableReference::bare("revalidated_table")])),
            revalidation_started_at,
            None,
        )
        .await;
        cache_provider.run_pending_tasks().await;

        assert!(
            cache_provider
                .get_raw_key(&invalidated_key)
                .await
                .expect("cache access should succeed")
                .is_none(),
            "a revalidation whose table was invalidated mid-flight must not repopulate the cache"
        );

        // Control: a revalidation for a table nobody invalidated still stores
        // its result, so the guard is not rejecting every write.
        let untouched_key = RawCacheKey::new(22);
        Query::cache_revalidation_result(
            &df,
            &untouched_key,
            vec![batch],
            schema,
            Arc::new(HashSet::from([TableReference::bare("untouched_table")])),
            revalidation_started_at,
            None,
        )
        .await;
        cache_provider.run_pending_tasks().await;

        assert!(
            cache_provider
                .get_raw_key(&untouched_key)
                .await
                .expect("cache access should succeed")
                .is_some(),
            "an unaffected revalidation must still populate the cache"
        );
    }

    /// A config where nothing expires on `item_ttl` during a test.
    fn sql_cache_config(stale_while_revalidate_ttl: Option<&str>) -> SQLResultsCacheConfig {
        SQLResultsCacheConfig {
            item_ttl: Some("10m".to_string()),
            cache_key_type: spicepod::component::caching::CacheKeyType::Sql,
            stale_while_revalidate_ttl: stale_while_revalidate_ttl.map(str::to_string),
            ..Default::default()
        }
    }

    /// Keeps two instants a test orders distinct: a tie counts as changed.
    async fn tick() {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }

    /// A table hard-invalidated by a client mutation during every scan. Each
    /// scan returns its sequence number.
    #[derive(Debug)]
    struct ChangesDuringEveryScan {
        name: TableReference,
        schema: arrow::datatypes::SchemaRef,
        scans: Arc<std::sync::atomic::AtomicI64>,
        caching: Arc<cache::Caching>,
    }

    #[async_trait::async_trait]
    impl datafusion::catalog::TableProvider for ChangesDuringEveryScan {
        fn schema(&self) -> arrow::datatypes::SchemaRef {
            Arc::clone(&self.schema)
        }

        fn table_type(&self) -> datafusion::datasource::TableType {
            datafusion::datasource::TableType::Base
        }

        async fn scan(
            &self,
            state: &dyn datafusion::catalog::Session,
            projection: Option<&Vec<usize>>,
            filters: &[datafusion::logical_expr::Expr],
            limit: Option<usize>,
        ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
            let scan = self.scans.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
            self.caching
                .invalidate_for_table(self.name.clone())
                .await
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
            let batch = arrow::array::RecordBatch::try_new(
                Arc::clone(&self.schema),
                vec![Arc::new(Int64Array::from(vec![scan]))],
            )?;
            datafusion::datasource::MemTable::try_new(Arc::clone(&self.schema), vec![vec![batch]])?
                .scan(state, projection, filters, limit)
                .await
        }
    }

    fn register_table_changing_during_every_scan(df: &Arc<DataFusion>, name: &'static str) {
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("scan", arrow::datatypes::DataType::Int64, false),
        ]));
        df.ctx
            .register_table(
                TableReference::bare(name),
                Arc::new(ChangesDuringEveryScan {
                    name: TableReference::bare(name),
                    schema,
                    scans: Arc::new(std::sync::atomic::AtomicI64::new(0)),
                    caching: df.caching(),
                }),
            )
            .expect("should register table");
    }

    /// Every hard invalidation rejects the in-flight result, even with a stale window.
    #[tokio::test]
    async fn test_results_overtaken_by_every_change_are_not_cached_with_a_window() {
        const SQL: &str = "SELECT scan FROM changing_table";
        let df = prepare_runtime(Some(sql_cache_config(Some("5m")))).await;
        register_table_changing_during_every_scan(&df, "changing_table");
        let context = create_test_request_context(CacheControl::Cache(CacheKeyType::Raw), None);
        let mut previous = 0;
        for _ in 0..3 {
            let (status, value) = Arc::clone(&context)
                .scope(run_i64_query(&df, SQL, ResultsCacheMode::Default))
                .await;
            assert_eq!(status, CacheStatus::CacheMiss);
            assert!(value > previous);
            previous = value;
        }
    }

    /// Without a stale window, nothing is cached and every request runs.
    #[tokio::test]
    async fn test_results_overtaken_by_every_change_are_not_cached_without_a_window() {
        const SQL: &str = "SELECT scan FROM changing_table_no_window";

        let df = prepare_runtime(Some(sql_cache_config(None))).await;
        register_table_changing_during_every_scan(&df, "changing_table_no_window");
        let request_context =
            create_test_request_context(CacheControl::Cache(CacheKeyType::Raw), None);

        let mut previous = 0;
        for _ in 0..3 {
            let (status, served) = Arc::clone(&request_context)
                .scope(run_i64_query(&df, SQL, ResultsCacheMode::Default))
                .await;
            assert_eq!(status, CacheStatus::CacheMiss);
            assert!(served > previous, "every request must read the table again");
            previous = served;
        }
    }

    fn one_row_batch() -> (arrow::datatypes::SchemaRef, arrow::array::RecordBatch) {
        let schema: arrow::datatypes::SchemaRef = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("n", arrow::datatypes::DataType::Int64, false),
        ]));
        let batch = arrow::array::RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1_i64]))],
        )
        .expect("valid record batch");
        (schema, batch)
    }

    /// Hard invalidations reject revalidation results that began before the change.
    #[tokio::test]
    async fn test_swr_revalidation_rejects_results_under_continuous_changes() {
        let df = prepare_runtime(Some(sql_cache_config(Some("5m")))).await;
        let provider = df
            .results_cache_provider()
            .expect("results cache configured");
        let (schema, batch) = one_row_batch();
        let table = TableReference::bare("cdc_table");
        let tables = Arc::new(HashSet::from([table.clone()]));
        let key = RawCacheKey::new(44);
        for _ in 0..3 {
            let read = std::time::Instant::now();
            df.caching()
                .invalidate_for_table(table.clone())
                .await
                .expect("invalidate table");
            Query::cache_revalidation_result(
                &df,
                &key,
                vec![batch.clone()],
                Arc::clone(&schema),
                Arc::clone(&tables),
                read,
                None,
            )
            .await;
            assert!(
                provider
                    .get_raw_key(&key)
                    .await
                    .expect("lookup result")
                    .is_none()
            );
        }
    }

    /// With no table change, both revalidations are eligible for storage.
    /// A result from an older read must preserve the later resident.
    #[tokio::test]
    async fn test_swr_revalidation_keeps_a_newer_fresh_resident() {
        let df = prepare_runtime(Some(sql_cache_config(Some("5m")))).await;
        let cache_provider = df
            .results_cache_provider()
            .expect("results cache should be configured");
        let (schema, batch) = one_row_batch();
        let tables = Arc::new(HashSet::from([TableReference::bare("fresh_table")]));
        let key = RawCacheKey::new(45);

        let older = std::time::Instant::now();
        tick().await;
        let newer = std::time::Instant::now();

        Query::cache_revalidation_result(
            &df,
            &key,
            vec![batch.clone()],
            Arc::clone(&schema),
            Arc::clone(&tables),
            newer,
            None,
        )
        .await;
        Query::cache_revalidation_result(&df, &key, vec![batch], schema, tables, older, None).await;
        cache_provider.run_pending_tasks().await;

        let entry = cache_provider
            .get_raw_key(&key)
            .await
            .expect("cache access should succeed")
            .expect("the newer result must still be cached");
        assert_eq!(
            entry.read_started_at, newer,
            "a fresh admission must not replace a resident that began reading later"
        );
        assert!(!cache_provider.tables_changed_since(&entry.input_tables, entry.read_started_at));
    }

    /// Reports a fixed [`MetricsSet`], standing in for an `HttpExec` whose
    /// `HTTP_TRANSIENT_FAILURE_METRIC_NAME` counter was incremented by a
    /// fetch a narrow projection then excluded `response_status` from.
    #[derive(Debug)]
    struct MetricsOnlyExecutionPlan {
        metrics: datafusion::physical_plan::metrics::MetricsSet,
        properties: Arc<datafusion::physical_plan::PlanProperties>,
    }

    impl MetricsOnlyExecutionPlan {
        fn with_transient_failure_count(count: usize) -> Self {
            let metrics_set = datafusion::physical_plan::metrics::ExecutionPlanMetricsSet::new();
            datafusion::physical_plan::metrics::MetricBuilder::new(&metrics_set)
                .counter(
                    arrow_tools::metadata_keys::HTTP_TRANSIENT_FAILURE_METRIC_NAME,
                    0,
                )
                .add(count);
            Self {
                metrics: metrics_set.clone_inner(),
                properties: Arc::new(datafusion::physical_plan::PlanProperties::new(
                    datafusion::physical_expr::EquivalenceProperties::new(Arc::new(
                        arrow::datatypes::Schema::empty(),
                    )),
                    datafusion::physical_expr::Partitioning::UnknownPartitioning(1),
                    datafusion::physical_plan::execution_plan::EmissionType::Final,
                    datafusion::physical_plan::execution_plan::Boundedness::Bounded,
                )),
            }
        }
    }

    impl datafusion::physical_plan::DisplayAs for MetricsOnlyExecutionPlan {
        fn fmt_as(
            &self,
            _t: datafusion::physical_plan::DisplayFormatType,
            _f: &mut std::fmt::Formatter,
        ) -> std::fmt::Result {
            unimplemented!("not used in tests")
        }
    }

    impl ExecutionPlan for MetricsOnlyExecutionPlan {
        fn name(&self) -> &'static str {
            "MetricsOnlyExecutionPlan"
        }

        fn properties(&self) -> &Arc<datafusion::physical_plan::PlanProperties> {
            &self.properties
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
            vec![]
        }

        fn with_new_children(
            self: Arc<Self>,
            _children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
            Ok(self)
        }

        fn metrics(&self) -> Option<datafusion::physical_plan::metrics::MetricsSet> {
            Some(self.metrics.clone())
        }

        fn execute(
            &self,
            _partition: usize,
            _context: Arc<datafusion::execution::TaskContext>,
        ) -> datafusion::common::Result<datafusion::execution::SendableRecordBatchStream> {
            unimplemented!("not used in tests")
        }
    }

    /// A revalidation the HTTP connector failed because the origin was down is
    /// counted as `transient_errors`, as a result carrying the origin's 5xx
    /// would be; any other failure keeps the outcome of the arm it failed in.
    #[test]
    fn a_revalidation_failed_by_a_transient_origin_status_counts_as_transient_errors() {
        use datafusion::error::DataFusionError;

        let refused = |status| -> DataFusionError {
            data_components::http::provider::Error::ErrorResponse {
                status,
                endpoint: "https://api.example.com".to_string(),
                dataset: "dataset 'items'".to_string(),
            }
            .into()
        };
        let wrapped = DataFusionError::Context("revalidation".to_string(), Box::new(refused(503)));
        assert_eq!(
            failed_revalidation_outcome(&wrapped, RevalidationOutcome::CollectFailed),
            RevalidationOutcome::TransientErrors
        );
        assert_eq!(
            failed_revalidation_outcome(&refused(404), RevalidationOutcome::CollectFailed),
            RevalidationOutcome::CollectFailed,
            "a 404 is an answer about the resource, not an origin that is down"
        );
        assert_eq!(
            failed_revalidation_outcome(
                &DataFusionError::Execution("connection reset".to_string()),
                RevalidationOutcome::QueryFailed
            ),
            RevalidationOutcome::QueryFailed
        );
    }

    /// The narrow-projection counterpart to
    /// `test_swr_revalidation_discards_result_invalidated_mid_flight`: a
    /// revalidation whose collected batches carry no `response_status`
    /// column or schema-metadata marker (a narrow `SELECT` pruned it, same
    /// as the live-query gap `plan_saw_transient_http_failure` fixes) must
    /// still not overwrite the stale entry when its execution plan recorded
    /// a transient HTTP failure — `batches_cacheable` alone cannot see it.
    #[tokio::test]
    async fn test_swr_revalidation_discards_result_with_plan_reported_transient_failure() {
        let df = prepare_runtime(Some(SQLResultsCacheConfig {
            item_ttl: Some("10m".to_string()),
            cache_key_type: spicepod::component::caching::CacheKeyType::Sql,
            ..Default::default()
        }))
        .await;
        let cache_provider = df
            .results_cache_provider()
            .expect("results cache should be configured");

        let schema: arrow::datatypes::SchemaRef = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("rank", arrow::datatypes::DataType::Int64, false),
        ]));
        let batch = arrow::array::RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1_i64]))],
        )
        .expect("valid record batch");

        let revalidation_started_at = std::time::Instant::now();
        let plan_with_failure: Arc<dyn ExecutionPlan> =
            Arc::new(MetricsOnlyExecutionPlan::with_transient_failure_count(1));

        let key = RawCacheKey::new(33);
        Query::cache_revalidation_result(
            &df,
            &key,
            vec![batch],
            schema,
            Arc::new(HashSet::from([TableReference::bare(
                "narrow_projection_table",
            )])),
            revalidation_started_at,
            Some(plan_with_failure),
        )
        .await;
        cache_provider.run_pending_tasks().await;

        assert!(
            cache_provider
                .get_raw_key(&key)
                .await
                .expect("cache access should succeed")
                .is_none(),
            "a revalidation whose plan recorded a transient HTTP failure must not \
            repopulate the cache, even when the collected batches themselves carry no \
            response_status signal"
        );
    }

    /// Two distinct principals running the same SQL must each see a cache
    /// MISS for the other's first request, and a HIT only on their own
    /// repeat. Cross-principal HITs would be a security regression — the
    /// whole point of `CacheNamespace`.
    #[tokio::test]
    async fn test_results_cache_isolated_per_principal() {
        let df = prepare_runtime(Some(SQLResultsCacheConfig {
            item_ttl: Some("10m".to_string()),
            cache_key_type: spicepod::component::caching::CacheKeyType::Sql,
            ..Default::default()
        }))
        .await;

        let alice = create_test_request_context_in_namespace(
            CacheControl::Cache(CacheKeyType::Raw),
            CacheNamespace::Principal("apikey:alice".into()),
        );
        let bob = create_test_request_context_in_namespace(
            CacheControl::Cache(CacheKeyType::Raw),
            CacheNamespace::Principal("apikey:bob".into()),
        );

        // Alice runs SELECT 1, populates the cache under her namespace.
        let q = QueryBuilder::new("SELECT 1", Arc::clone(&df)).build();
        Arc::clone(&alice)
            .scope(async move {
                let result = q.run().await.expect("query should succeed");
                assert_eq!(result.cache_status, CacheStatus::CacheMiss);
                let _ = result.collect_batches().await.expect("should drain");
            })
            .await;

        // Alice repeats: HIT (own namespace).
        let q = QueryBuilder::new("SELECT 1", Arc::clone(&df)).build();
        Arc::clone(&alice)
            .scope(async move {
                let result = q.run().await.expect("query should succeed");
                assert_eq!(result.cache_status, CacheStatus::CacheHit);
            })
            .await;

        // Bob runs the same SQL: MUST be a MISS, otherwise we leaked
        // Alice's cached result across principals.
        let q = QueryBuilder::new("SELECT 1", Arc::clone(&df)).build();
        Arc::clone(&bob)
            .scope(async move {
                let result = q.run().await.expect("query should succeed");
                assert_eq!(
                    result.cache_status,
                    CacheStatus::CacheMiss,
                    "bob must not see alice's cached entry"
                );
                let _ = result.collect_batches().await.expect("should drain");
            })
            .await;

        // Bob repeats: HIT in his own namespace.
        let q = QueryBuilder::new("SELECT 1", Arc::clone(&df)).build();
        Arc::clone(&bob)
            .scope(async move {
                let result = q.run().await.expect("query should succeed");
                assert_eq!(result.cache_status, CacheStatus::CacheHit);
            })
            .await;

        // And an unauthenticated (Public) caller must also miss — it
        // shares no namespace with either Alice or Bob.
        let public = create_test_request_context_in_namespace(
            CacheControl::Cache(CacheKeyType::Raw),
            CacheNamespace::Public,
        );
        let q = QueryBuilder::new("SELECT 1", Arc::clone(&df)).build();
        Arc::clone(&public)
            .scope(async move {
                let result = q.run().await.expect("query should succeed");
                assert_eq!(result.cache_status, CacheStatus::CacheMiss);
            })
            .await;
    }

    #[tokio::test]
    async fn test_get_plan_or_cached_cache_miss_and_hit() {
        let df = prepare_runtime(Some(SQLResultsCacheConfig {
            item_ttl: Some("10m".to_string()),
            cache_key_type: spicepod::component::caching::CacheKeyType::Sql,
            ..Default::default()
        }))
        .await;

        // Test with SQL cache key
        let request_context =
            create_test_request_context(CacheControl::Cache(CacheKeyType::Raw), None);
        let query_builder = QueryBuilder::new("SELECT 1", Arc::clone(&df));
        let query = query_builder.build();
        Arc::clone(&request_context)
            .scope(async move {
                let result = query.run().await.expect("query should succeed");
                assert_eq!(result.cache_status, CacheStatus::CacheMiss);
                // Need to drain the stream to ensure the cache is populated
                let records = result.collect_batches().await.expect("should collect");
                assert_eq!(records.len(), 1);
                assert_eq!(records[0].num_rows(), 1);
            })
            .await;

        // Repeat the same query to ensure a cache hit
        let query_builder = QueryBuilder::new("SELECT 1", Arc::clone(&df));
        let query = query_builder.build();
        Arc::clone(&request_context)
            .scope(async move {
                let result = query.run().await.expect("query should succeed");
                assert_eq!(result.cache_status, CacheStatus::CacheHit);
            })
            .await;

        // Repeat a similar query, but with different whitespace - this should be a cache miss for the raw SQL cache key
        let query_builder = QueryBuilder::new("SELECT 1 ", Arc::clone(&df));
        let query = query_builder.build();
        Arc::clone(&request_context)
            .scope(async move {
                let result = query.run().await.expect("query should succeed");
                assert_eq!(result.cache_status, CacheStatus::CacheMiss);
            })
            .await;

        // Test with plan cache key
        let request_context =
            create_test_request_context(CacheControl::Cache(CacheKeyType::Default), None);
        let query_builder = QueryBuilder::new("SELECT 1", Arc::clone(&df));
        let query = query_builder.build();
        Arc::clone(&request_context)
            .scope(async move {
                let result = query.run().await.expect("query should succeed");
                // Expect to miss cache because we are using the default cache key type
                assert_eq!(result.cache_status, CacheStatus::CacheMiss);
                // Need to drain the stream to ensure the cache is populated
                let records = result.collect_batches().await.expect("should collect");
                assert_eq!(records.len(), 1);
                assert_eq!(records[0].num_rows(), 1);
            })
            .await;

        // Repeat the same query with the default cache key type - this should be a cache hit
        let query_builder = QueryBuilder::new("SELECT 1", Arc::clone(&df));
        let query = query_builder.build();
        Arc::clone(&request_context)
            .scope(async move {
                let result = query.run().await.expect("query should succeed");
                assert_eq!(result.cache_status, CacheStatus::CacheHit);
            })
            .await;

        // Repeat the same query with the default cache key type, but with different whitespace - this should be a cache hit since the plan is the same
        let query_builder = QueryBuilder::new("SELECT 1 ", Arc::clone(&df));
        let query = query_builder.build();
        Arc::clone(&request_context)
            .scope(async move {
                let result = query.run().await.expect("query should succeed");
                assert_eq!(result.cache_status, CacheStatus::CacheHit);
            })
            .await;

        // Test with user cache key
        let request_context = create_test_request_context(
            CacheControl::Cache(CacheKeyType::ClientSupplied),
            Some("foo".to_string()),
        );
        let query_builder = QueryBuilder::new("SELECT 1", Arc::clone(&df));
        let query = query_builder.build();
        Arc::clone(&request_context)
            .scope(async move {
                let result = query.run().await.expect("query should succeed");
                // Expect to miss cache because it is the first request
                assert_eq!(result.cache_status, CacheStatus::CacheMiss);
                // Need to drain the stream to ensure the cache is populated
                let records = result.collect_batches().await.expect("should collect");
                assert_eq!(records.len(), 1);
                assert_eq!(records[0].num_rows(), 1);
                assert_eq!(
                    records[0]
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .expect("must read i64 array")
                        .value(0),
                    1
                );
            })
            .await;

        // Repeat the same query with the same user key: a hit
        let query_builder = QueryBuilder::new("SELECT 1", Arc::clone(&df));
        let query = query_builder.build();
        Arc::clone(&request_context)
            .scope(async move {
                let result = query.run().await.expect("query should succeed");
                assert_eq!(result.cache_status, CacheStatus::CacheHit);
            })
            .await;

        // A different query with the same user key must not be served the
        // first query's result: the user key partitions the SQL key, it does
        // not replace it.
        let query_builder = QueryBuilder::new("SELECT 2", Arc::clone(&df));
        let query = query_builder.build();
        Arc::clone(&request_context)
            .scope(async move {
                let result = query.run().await.expect("query should succeed");
                assert_eq!(result.cache_status, CacheStatus::CacheMiss);

                let records = result.collect_batches().await.expect("should collect");
                assert_eq!(records.len(), 1);
                assert_eq!(records[0].num_rows(), 1);

                // The query ran
                assert_eq!(
                    records[0]
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .expect("must read i64 array")
                        .value(0),
                    2
                );
            })
            .await;

        // Make a request with the same "SELECT 2" query, but an invalid cache key
        let invalid_user_key_ctx = create_test_request_context(
            CacheControl::Cache(CacheKeyType::ClientSupplied),
            Some("bar$".to_string()),
        );

        let query_builder = QueryBuilder::new("SELECT 2", Arc::clone(&df));
        let query = query_builder.build();
        Arc::clone(&invalid_user_key_ctx)
            .scope(async move {
                let result = query.run().await.expect("query should succeed");

                // An invalid key results in a cache miss
                assert_eq!(result.cache_status, CacheStatus::CacheMiss);

                let records = result.collect_batches().await.expect("should collect");
                assert_eq!(records.len(), 1);
                assert_eq!(records[0].num_rows(), 1);

                // The query was run
                assert_eq!(
                    records[0]
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .expect("must read i64 array")
                        .value(0),
                    2
                );
            })
            .await;

        // Issue the same "SELECT 2" query with the invalid cache key to verify that we fall back
        // on the default behavior if the user sets a cache-control header
        let query_builder = QueryBuilder::new("SELECT 2", Arc::clone(&df));
        let query = query_builder.build();
        Arc::clone(&invalid_user_key_ctx)
            .scope(async move {
                let result = query.run().await.expect("query should succeed");

                // Since cache-control is set, an invalid key with repeated query will fall back
                // to the default plan-key behavior and result in a cache hit
                assert_eq!(result.cache_status, CacheStatus::CacheHit);
            })
            .await;
    }

    /// Regression test for empty (0-row) result sets not being cached.
    ///
    /// `SELECT 1 WHERE 1=0` is optimized to an `EmptyRelation`/`EmptyExec`,
    /// which yields **zero** record batches. Such a result must still be cached
    /// so the second identical request is served from cache instead of being
    /// re-executed against the source.
    #[tokio::test]
    async fn test_empty_result_set_is_cached() {
        let df = prepare_runtime(Some(SQLResultsCacheConfig {
            item_ttl: Some("10m".to_string()),
            cache_key_type: spicepod::component::caching::CacheKeyType::Sql,
            ..Default::default()
        }))
        .await;

        let request_context =
            create_test_request_context(CacheControl::Cache(CacheKeyType::Raw), None);

        // First request: cache miss, populates the cache when drained.
        let query = QueryBuilder::new("SELECT 1 WHERE 1=0", Arc::clone(&df)).build();
        Arc::clone(&request_context)
            .scope(async move {
                let result = query.run().await.expect("query should succeed");
                assert_eq!(result.cache_status, CacheStatus::CacheMiss);
                let records = result.collect_batches().await.expect("should collect");
                let total_rows: usize = records
                    .iter()
                    .map(arrow::array::RecordBatch::num_rows)
                    .sum();
                assert_eq!(total_rows, 0, "result set should be empty");
            })
            .await;

        // Second request: must be a cache hit for the empty result set.
        let query = QueryBuilder::new("SELECT 1 WHERE 1=0", Arc::clone(&df)).build();
        Arc::clone(&request_context)
            .scope(async move {
                let result = query.run().await.expect("query should succeed");
                assert_eq!(
                    result.cache_status,
                    CacheStatus::CacheHit,
                    "empty result sets should be served from cache on repeat requests"
                );
                let records = result.collect_batches().await.expect("should collect");
                let total_rows: usize = records
                    .iter()
                    .map(arrow::array::RecordBatch::num_rows)
                    .sum();
                assert_eq!(total_rows, 0, "cached empty result should still be empty");
            })
            .await;
    }

    #[tokio::test]
    async fn test_get_plan_or_cached_sql_cached_prepared_statements() {
        let df = prepare_runtime(Some(SQLResultsCacheConfig {
            item_ttl: Some("10m".to_string()),
            cache_key_type: spicepod::component::caching::CacheKeyType::Sql,
            ..Default::default()
        }))
        .await;

        let parameters = ParamValues::from(vec![ScalarValue::Int32(Some(1))]);

        let request_context =
            create_test_request_context(CacheControl::Cache(CacheKeyType::Raw), None);
        let query_builder =
            QueryBuilder::new("SELECT $1", Arc::clone(&df)).parameters(Some(parameters));
        let query = query_builder.build();
        Arc::clone(&request_context)
            .scope(async move {
                let result = query.run().await.expect("query should succeed");
                assert_eq!(result.cache_status, CacheStatus::CacheMiss);
                // Need to drain the stream to ensure the cache is populated
                let records = result.collect_batches().await.expect("should collect");
                assert_eq!(records.len(), 1);
                assert_eq!(records[0].num_rows(), 1);
            })
            .await;

        let parameters = ParamValues::from(vec![ScalarValue::Int32(Some(2))]);

        let query_builder =
            QueryBuilder::new("SELECT $1", Arc::clone(&df)).parameters(Some(parameters));
        let query = query_builder.build();
        Arc::clone(&request_context)
            .scope(async move {
                let result = query.run().await.expect("query should succeed");
                assert_eq!(result.cache_status, CacheStatus::CacheMiss);
            })
            .await;
    }

    #[tokio::test]
    async fn test_get_plan_or_cached_plan_cached_prepared_statements() {
        let df = prepare_runtime(Some(SQLResultsCacheConfig {
            item_ttl: Some("10m".to_string()),
            cache_key_type: spicepod::component::caching::CacheKeyType::Plan,
            ..Default::default()
        }))
        .await;

        let parameters = ParamValues::from(vec![ScalarValue::Int32(Some(1))]);

        let request_context =
            create_test_request_context(CacheControl::Cache(CacheKeyType::Default), None);
        let query_builder =
            QueryBuilder::new("SELECT $1", Arc::clone(&df)).parameters(Some(parameters));
        let query = query_builder.build();
        Arc::clone(&request_context)
            .scope(async move {
                let result = query.run().await.expect("query should succeed");
                assert_eq!(result.cache_status, CacheStatus::CacheMiss);
                // Need to drain the stream to ensure the cache is populated
                let records = result.collect_batches().await.expect("should collect");
                assert_eq!(records.len(), 1);
                assert_eq!(records[0].num_rows(), 1);
            })
            .await;

        let parameters = ParamValues::from(vec![ScalarValue::Int32(Some(2))]);

        let query_builder =
            QueryBuilder::new("SELECT $1", Arc::clone(&df)).parameters(Some(parameters));
        let query = query_builder.build();
        Arc::clone(&request_context)
            .scope(async move {
                let result = query.run().await.expect("query should succeed");
                assert_eq!(result.cache_status, CacheStatus::CacheMiss);
                // Need to drain the stream to ensure the cache is populated
                let records = result.collect_batches().await.expect("should collect");
                assert_eq!(records.len(), 1);
                assert_eq!(records[0].num_rows(), 1);
            })
            .await;

        let parameters = ParamValues::from(vec![ScalarValue::Int32(Some(2))]);

        // Repeat the same query to ensure a cache hit
        let query_builder =
            QueryBuilder::new("SELECT $1", Arc::clone(&df)).parameters(Some(parameters));
        let query = query_builder.build();
        Arc::clone(&request_context)
            .scope(async move {
                let result = query.run().await.expect("query should succeed");
                assert_eq!(result.cache_status, CacheStatus::CacheHit);
            })
            .await;
    }

    /// Registers an in-memory table holding one row per id in `ids`, so a
    /// parameterized predicate over it selects a value the test can check.
    fn register_id_table(df: &Arc<DataFusion>, name: &'static str, ids: &[i64]) {
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
        ]));
        let batch = arrow::array::RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(ids.to_vec()))],
        )
        .expect("valid record batch");
        let table =
            datafusion::datasource::MemTable::try_new(Arc::clone(&schema), vec![vec![batch]])
                .expect("valid mem table");
        df.ctx
            .register_table(TableReference::bare(name), Arc::new(table))
            .expect("should register table");
    }

    /// Runs `sql` bound to `value`, returning how the results cache answered and
    /// the `id` column of the result.
    async fn run_id_lookup(
        df: &Arc<DataFusion>,
        request_context: &Arc<RequestContext>,
        sql: &'static str,
        value: i64,
    ) -> (CacheStatus, Vec<i64>) {
        let query = QueryBuilder::new(sql, Arc::clone(df))
            .parameters(Some(ParamValues::from(vec![ScalarValue::Int64(Some(
                value,
            ))])))
            .build();
        Arc::clone(request_context)
            .scope(async move {
                let result = query.run().await.expect("query should succeed");
                let cache_status = result.cache_status;
                let ids = result
                    .collect_batches()
                    .await
                    .expect("should drain")
                    .iter()
                    .flat_map(|batch| {
                        batch
                            .column(0)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .expect("id should be an Int64 column")
                            .iter()
                            .flatten()
                            .collect::<Vec<_>>()
                    })
                    .collect();
                (cache_status, ids)
            })
            .await
    }

    /// One SQL text, many bound values: one cached plan, and a cached result per
    /// value.
    ///
    /// The plan cache exists so a query parses and plans once, and a
    /// parameterized query is the case it should serve best — the SQL text is
    /// fixed and only the bound values move. Keying it on `(sql, parameters)`
    /// gave every distinct value tuple its own entry, so every request
    /// re-parsed and re-planned and the entries accumulated until they evicted
    /// each other.
    ///
    /// The two halves are asserted together because the fix is a split, and
    /// either half alone would be the wrong thing: the values must leave the
    /// *plan* key and stay in the *results* key. A run that reused the plan but
    /// also reused the previous value's rows would be a correctness bug, not an
    /// optimization, so the repeats below check that a second request for a
    /// value is served from the results cache and still returns that value's
    /// own row.
    #[tokio::test]
    async fn a_parameterized_query_caches_one_plan_and_a_result_per_value() {
        const SQL: &str = "SELECT id FROM plan_cache_params WHERE id = $1";

        let df = prepare_runtime(None).await;
        register_id_table(&df, "plan_cache_params", &[1, 2, 3, 4, 5]);
        let plans = df
            .plans_cache_provider()
            .expect("the plans cache is installed unconditionally");
        let request_context =
            create_test_request_context(CacheControl::Cache(CacheKeyType::Default), None);

        for value in [1_i64, 2, 3, 4, 5] {
            let (status, rows) = run_id_lookup(&df, &request_context, SQL, value).await;
            assert_eq!(status, CacheStatus::CacheMiss, "value {value} is new");
            assert_eq!(rows, vec![value], "binding {value} must select its own row");
        }

        // Re-bind values whose plan is now served from the cache. This is where a
        // plan carrying a previous binding, or a results entry shared across
        // values, would surface as the wrong row.
        for value in [1_i64, 3] {
            let (status, rows) = run_id_lookup(&df, &request_context, SQL, value).await;
            assert_eq!(
                status,
                CacheStatus::CacheHit,
                "the results cache must still key on the bound value, so a repeat of {value} is a hit"
            );
            assert_eq!(
                rows,
                vec![value],
                "binding {value} must select its own row; a different row means one results entry \
                 is being shared across parameter values"
            );
        }

        plans.checkpoint().await;
        assert_eq!(
            plans.item_count().await,
            1,
            "one SQL text must cache one plan; more than one means the parameter values reached \
             the plan cache key, so every value tuple re-parses and re-plans"
        );
    }

    /// A stale-while-revalidate revalidation must re-bind the parameter values
    /// of the query it replaces.
    ///
    /// Under `cache_key_type: sql` the stale hit is found on the raw-SQL key,
    /// before any `LogicalPlan` exists, so the revalidation rebuilds the query
    /// from the SQL text. That text still holds its placeholders: rebuilt
    /// without the values it fails with `Placeholder '$1' was not provided a
    /// value for execution`, so the entry is never replaced and every request
    /// inside the window is served a result older than `item_ttl` asked for.
    ///
    /// What proves the revalidation landed is the status returning to
    /// `CacheHit` — only a stored result makes the entry fresh again — and the
    /// row it then serves still being the one the bound value selects.
    #[tokio::test]
    async fn swr_revalidation_of_a_parameterized_query_rebinds_its_values() {
        const SQL: &str = "SELECT id FROM swr_params WHERE id = $1";

        let df = prepare_runtime(Some(SQLResultsCacheConfig {
            item_ttl: Some("1s".to_string()),
            cache_key_type: spicepod::component::caching::CacheKeyType::Sql,
            stale_while_revalidate_ttl: Some("5m".to_string()),
            ..Default::default()
        }))
        .await;
        register_id_table(&df, "swr_params", &[1, 2, 3]);

        let request_context =
            create_test_request_context(CacheControl::Cache(CacheKeyType::Raw), None);

        let (status, rows) = run_id_lookup(&df, &request_context, SQL, 3).await;
        assert_eq!(status, CacheStatus::CacheMiss);
        assert_eq!(rows, vec![3]);

        // Age the entry past its TTL into the stale-while-revalidate window.
        // The sleep is the behavior under test (TTL expiry), not a readiness
        // wait.
        tokio::time::sleep(Duration::from_millis(1_100)).await;

        let (status, rows) = run_id_lookup(&df, &request_context, SQL, 3).await;
        assert_eq!(status, CacheStatus::CacheStaleWhileRevalidate);
        assert_eq!(rows, vec![3]);

        let mut last = (status, rows);
        for _ in 0..100 {
            tokio::time::sleep(Duration::from_millis(50)).await;
            last = run_id_lookup(&df, &request_context, SQL, 3).await;
            if last.0 == CacheStatus::CacheHit {
                break;
            }
        }

        assert_eq!(
            last.0,
            CacheStatus::CacheHit,
            "the background revalidation never replaced the stale entry, so a parameterized query \
             is served a result older than item_ttl for the whole stale window"
        );
        assert_eq!(
            last.1,
            vec![3],
            "the revalidated entry must hold the row the bound value selects"
        );
    }

    #[tokio::test]
    async fn test_client_cache_key_get_after_ttl_expiry() {
        const SQL: &str = "SELECT scan FROM ttl_counted";
        let df = prepare_runtime(Some(SQLResultsCacheConfig {
            item_ttl: Some("5s".to_string()),
            cache_key_type: spicepod::component::caching::CacheKeyType::Sql,
            stale_while_revalidate_ttl: Some("0s".to_string()), // Disable stale-while-revalidate for this test
            ..Default::default()
        }))
        .await;
        register_counted_table(&df, "ttl_counted");

        let request_context = create_test_request_context(
            CacheControl::Cache(CacheKeyType::ClientSupplied),
            Some("foo".to_string()),
        );

        // First request: a miss that executes the query (the first scan).
        assert_eq!(
            Arc::clone(&request_context)
                .scope(run_i64_query(&df, SQL, ResultsCacheMode::Default))
                .await,
            (CacheStatus::CacheMiss, 1)
        );

        // Same client key and SQL: served from the cache, no second scan.
        assert_eq!(
            Arc::clone(&request_context)
                .scope(run_i64_query(&df, SQL, ResultsCacheMode::Default))
                .await,
            (CacheStatus::CacheHit, 1)
        );

        // Run out the TTL
        tokio::time::sleep(Duration::from_secs(5)).await;

        // Force Moka to run pending eviction tasks
        if let Some(cache_provider) = df.results_cache_provider() {
            cache_provider.run_pending_tasks().await;
        }

        // After expiry the query runs again (the second scan).
        assert_eq!(
            Arc::clone(&request_context)
                .scope(run_i64_query(&df, SQL, ResultsCacheMode::Default))
                .await,
            (CacheStatus::CacheMiss, 2)
        );
    }

    /// A client-supplied key is an extra partition of the request's own key,
    /// never the whole key. Two different queries — over two different
    /// tables, or one query with two different bound parameters — sent with
    /// the same `Spice-Cache-Key` must each get their own result, and a
    /// repeat of each is served its own entry.
    #[tokio::test]
    async fn test_client_cache_key_does_not_share_entries_across_queries() {
        for key_type in [
            spicepod::component::caching::CacheKeyType::Plan,
            spicepod::component::caching::CacheKeyType::Sql,
        ] {
            let df = prepare_runtime(Some(SQLResultsCacheConfig {
                item_ttl: Some("10m".to_string()),
                cache_key_type: key_type,
                ..Default::default()
            }))
            .await;
            let scans_a = register_counted_table(&df, "shared_key_table_a");
            let scans_b = register_counted_table(&df, "shared_key_table_b");
            // Table B's scans start at 101 so its rows can never equal table A's.
            scans_b.store(100, std::sync::atomic::Ordering::SeqCst);

            let request_context = create_test_request_context(
                CacheControl::Cache(CacheKeyType::ClientSupplied),
                Some("same-key".to_string()),
            );
            let run = |sql: &'static str| {
                let df = Arc::clone(&df);
                let ctx = Arc::clone(&request_context);
                async move {
                    ctx.scope(run_i64_query(&df, sql, ResultsCacheMode::Default))
                        .await
                }
            };

            assert_eq!(
                run("SELECT scan FROM shared_key_table_a").await,
                (CacheStatus::CacheMiss, 1),
                "{key_type:?}: table A populates its own entry"
            );
            assert_eq!(
                run("SELECT scan FROM shared_key_table_b").await,
                (CacheStatus::CacheMiss, 101),
                "{key_type:?}: table B under the same client key must not be served table A's rows"
            );
            assert_eq!(
                run("SELECT scan FROM shared_key_table_a").await,
                (CacheStatus::CacheHit, 1),
                "{key_type:?}: table A's repeat is served table A's entry"
            );
            assert_eq!(
                run("SELECT scan FROM shared_key_table_b").await,
                (CacheStatus::CacheHit, 101),
                "{key_type:?}: table B's repeat is served table B's entry"
            );
            assert_eq!(scans_a.load(std::sync::atomic::Ordering::SeqCst), 1);
            assert_eq!(scans_b.load(std::sync::atomic::Ordering::SeqCst), 101);

            // Same client key, same SQL, different bound parameter values.
            for (param, expected_status) in [
                (7, CacheStatus::CacheMiss),
                (8, CacheStatus::CacheMiss),
                (7, CacheStatus::CacheHit),
                (8, CacheStatus::CacheHit),
            ] {
                let result = Arc::clone(&request_context)
                    .scope(
                        QueryBuilder::new("SELECT $1", Arc::clone(&df))
                            .parameters(Some(ParamValues::from(vec![ScalarValue::Int64(Some(
                                param,
                            ))])))
                            .build()
                            .run(),
                    )
                    .await
                    .expect("query should succeed");
                let status = result.cache_status;
                let records = result.collect_batches().await.expect("should collect");
                let value = records[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("must read i64 array")
                    .value(0);
                assert_eq!(
                    (status, value),
                    (expected_status, param),
                    "{key_type:?}: parameter {param} under the same client key must get its own row"
                );
            }
        }
    }

    #[tokio::test]
    async fn test_stale_while_revalidate_complete_lifecycle() {
        // This test validates the complete stale-while-revalidate lifecycle:
        // 1. Initial cache population
        // 2. Serving stale data after TTL expiry (within stale window)
        // 3. Background revalidation updating the cache with fresh data
        // 4. Subsequent requests getting the fresh data from cache
        //
        // The queried table returns its scan's sequence number, so the
        // revalidated entry (the second scan) is told apart from the first.
        const SQL: &str = "SELECT scan FROM lifecycle_counted";

        // Use longer timeouts for robustness across different machines and CI environments
        // Configure cache with 3s TTL and 5s max stale-while-revalidate
        let df = prepare_runtime(Some(SQLResultsCacheConfig {
            item_ttl: Some("3s".to_string()),
            cache_key_type: spicepod::component::caching::CacheKeyType::Sql,
            stale_while_revalidate_ttl: Some("5s".to_string()),
            ..Default::default()
        }))
        .await;
        register_counted_table(&df, "lifecycle_counted");

        let request_context = create_test_request_context(
            CacheControl::MaxStale(CacheKeyType::ClientSupplied, Some(Duration::from_secs(5))),
            Some("lifecycle-test-key".to_string()),
        );

        // Step 1: First request populates the cache with the first scan.
        assert_eq!(
            Arc::clone(&request_context)
                .scope(run_i64_query(&df, SQL, ResultsCacheMode::Default))
                .await,
            (CacheStatus::CacheMiss, 1),
            "First query should be a cache miss"
        );

        // Step 2: Wait 3.5s (past TTL but within stale window). The request is
        // served the stale first scan and triggers a background revalidation.
        tokio::time::sleep(Duration::from_millis(3500)).await;
        assert_eq!(
            Arc::clone(&request_context)
                .scope(run_i64_query(&df, SQL, ResultsCacheMode::Default))
                .await,
            (CacheStatus::CacheStaleWhileRevalidate, 1),
            "Should be serving stale data with background revalidation"
        );

        // Step 3: Poll for the background revalidation to store the second scan.
        let mut last = (CacheStatus::CacheStaleWhileRevalidate, 1);
        for _ in 0..50 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            last = Arc::clone(&request_context)
                .scope(run_i64_query(&df, SQL, ResultsCacheMode::Default))
                .await;
            if last.0 == CacheStatus::CacheHit {
                break;
            }
        }
        assert_eq!(
            last,
            (CacheStatus::CacheHit, 2),
            "Background revalidation should have updated the cache with the second scan"
        );

        // Step 4: Subsequent requests continue to get the revalidated value.
        assert_eq!(
            Arc::clone(&request_context)
                .scope(run_i64_query(&df, SQL, ResultsCacheMode::Default))
                .await,
            (CacheStatus::CacheHit, 2),
            "Should continue getting the revalidated value"
        );
    }

    #[tokio::test]
    async fn test_stale_while_revalidate_with_client_supplied_cache_key() {
        // The queried table returns its scan's sequence number, so each
        // revalidation's result is told apart from the entry it replaces.
        const SQL: &str = "SELECT scan FROM swr_client_counted";

        // Configure cache with short TTL and stale-while-revalidate
        let df = prepare_runtime(Some(SQLResultsCacheConfig {
            item_ttl: Some("2s".to_string()),
            stale_while_revalidate_ttl: Some("3s".to_string()),
            cache_key_type: spicepod::component::caching::CacheKeyType::Sql,
            ..Default::default()
        }))
        .await;
        register_counted_table(&df, "swr_client_counted");

        let request_context = create_test_request_context(
            CacheControl::Cache(CacheKeyType::ClientSupplied),
            Some("stale-test-key".to_string()),
        );
        let run = || {
            let df = Arc::clone(&df);
            let ctx = Arc::clone(&request_context);
            async move {
                ctx.scope(run_i64_query(&df, SQL, ResultsCacheMode::Default))
                    .await
            }
        };

        // Step 1: MISS, the first scan.
        assert_eq!(run().await, (CacheStatus::CacheMiss, 1));

        // Step 2: HIT (cached, fresh).
        assert_eq!(run().await, (CacheStatus::CacheHit, 1));

        // Step 3: Wait for TTL to expire (2s) but stay within the
        // stale-while-revalidate window (2s + 3s = 5s total).
        tokio::time::sleep(Duration::from_secs(3)).await;

        // Step 4: STALE, still the first scan; triggers background revalidation.
        assert_eq!(run().await, (CacheStatus::CacheStaleWhileRevalidate, 1));

        // Step 5: Poll for the background revalidation to store the second scan.
        let mut last = (CacheStatus::CacheStaleWhileRevalidate, 1);
        for _ in 0..100 {
            last = run().await;
            if last.0 == CacheStatus::CacheHit {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert_eq!(
            last,
            (CacheStatus::CacheHit, 2),
            "background revalidation never refreshed the entry with the second scan"
        );

        // Step 6: HIT on the revalidated entry.
        assert_eq!(run().await, (CacheStatus::CacheHit, 2));

        // Step 7: Wait for the revalidated entry to become stale (but still within window).
        tokio::time::sleep(Duration::from_secs(3)).await;

        // Step 8: STALE again, serving the revalidated value while a new
        // revalidation runs in the background.
        assert_eq!(run().await, (CacheStatus::CacheStaleWhileRevalidate, 2));
    }

    /// Stale hits on more keys than the limit start no more revalidations than the
    /// limit. A refused key is left unclaimed, so a later stale hit can revalidate it
    /// once a running revalidation ends.
    #[test]
    fn stale_hits_past_the_revalidation_limit_wait_for_a_running_one_to_end() {
        let claims = RevalidationClaims::new(Duration::from_mins(5), 2);
        let first = claims.claim(1).expect("the first key is claimed");
        let _second = claims.claim(2).expect("the second key is claimed");

        assert!(
            claims.claim(3).is_none(),
            "a third revalidation must not start while two run"
        );
        assert!(
            claims.held.get(&3).is_none(),
            "a refused key must stay unclaimed"
        );

        drop(first);
        assert!(
            claims.claim(3).is_some(),
            "the refused key is claimed once a running revalidation ends"
        );
    }

    /// An expired claim refused for want of a permit stays the holder of its key, so
    /// releasing it frees both the key and the permit.
    #[test]
    fn an_expired_claim_refused_at_the_revalidation_limit_keeps_its_key() {
        let claims = RevalidationClaims::new(Duration::ZERO, 1);
        let expired = claims.claim(1).expect("an unheld key is claimed");

        assert!(
            claims.claim(1).is_none(),
            "an expired claim is not replaced while no permit is free"
        );
        assert_eq!(
            claims.held.get(&1).map(|held| held.generation),
            Some(expired.generation),
            "a refused replacement must leave the expired claim holding the key"
        );

        drop(expired);
        assert!(
            claims.claim(1).is_some(),
            "the key and its permit are free once the expired claim is released"
        );
    }

    /// A claim whose revalidation outlives the time-to-live is replaced by a later
    /// claim on the same key. Releasing the expired claim must leave its replacement
    /// held, or a third stale hit would start a revalidation alongside the second.
    #[test]
    fn releasing_an_expired_revalidation_claim_keeps_its_replacement_held() {
        let claims = RevalidationClaims::new(Duration::from_millis(50), 4);
        let expired = claims.claim(7).expect("an unheld key is claimed");
        assert!(claims.claim(7).is_none(), "a held key is not claimed twice");

        // Time itself is under test: the first claim has to outlive its time-to-live.
        std::thread::sleep(Duration::from_millis(150));
        let replacement = claims
            .claim(7)
            .expect("an expired claim no longer holds its key");

        assert_ne!(replacement.generation, expired.generation);

        drop(expired);
        assert_eq!(
            claims.held.get(&7).map(|held| held.generation),
            Some(replacement.generation),
            "releasing the expired claim must not release its replacement"
        );

        drop(replacement);
        assert_eq!(
            claims.held.get(&7).map(|held| held.generation),
            None,
            "the key is free again once its holder releases it"
        );
    }

    #[tokio::test]
    async fn test_single_in_flight_revalidation() {
        // This test validates that concurrent stale-while-revalidate requests
        // for the same cache key only trigger ONE background revalidation,
        // even when multiple requests arrive simultaneously.
        //
        // Expected behavior:
        // 1. Multiple concurrent requests during stale window
        // 2. All get stale data immediately (CacheStaleWhileRevalidate)
        // 3. Only ONE background query executes (single-in-flight semantics):
        //    the queried table counts its scans, and every scan returns its own
        //    sequence number, so the table is scanned exactly twice — the
        //    initial miss and the one revalidation.
        const SQL: &str = "SELECT scan FROM counted_table";

        // Configure cache with 1s TTL and 5s stale window
        let df = prepare_runtime(Some(SQLResultsCacheConfig {
            item_ttl: Some("1s".to_string()),
            stale_while_revalidate_ttl: Some("5s".to_string()),
            cache_key_type: spicepod::component::caching::CacheKeyType::Sql,
            ..Default::default()
        }))
        .await;
        let scans = register_counted_table(&df, "counted_table");

        let request_context = create_test_request_context(
            CacheControl::MaxStale(CacheKeyType::ClientSupplied, Some(Duration::from_secs(5))),
            Some("single-in-flight-test".to_string()),
        );

        // Step 1: Populate cache with initial query (the first scan)
        tracing::info!("Populating cache with initial query");
        assert_eq!(
            Arc::clone(&request_context)
                .scope(run_i64_query(&df, SQL, ResultsCacheMode::Default))
                .await,
            (CacheStatus::CacheMiss, 1)
        );

        // Step 2: Wait for entry to become stale (past TTL but within stale window)
        tracing::info!("Waiting 1.5s for entry to become stale");
        tokio::time::sleep(Duration::from_millis(1500)).await;

        // Step 3: Launch 10 concurrent requests with the SAME cache key
        // All should get stale data, but only ONE should trigger actual background query
        tracing::info!("Launching 10 concurrent stale requests");
        let concurrent_requests = 10;
        let mut handles = Vec::new();

        for i in 0..concurrent_requests {
            let df_clone = Arc::clone(&df);
            let ctx_clone = Arc::clone(&request_context);
            let handle = tokio::spawn(async move {
                // All requests get the STALE value from the first scan.
                let observed = ctx_clone
                    .scope(run_i64_query(&df_clone, SQL, ResultsCacheMode::Default))
                    .await;
                assert_eq!(
                    observed,
                    (CacheStatus::CacheStaleWhileRevalidate, 1),
                    "Request {i} should get the stale value from the first scan"
                );
            });
            handles.push(handle);
        }

        // Wait for all concurrent requests to complete
        for (i, handle) in handles.into_iter().enumerate() {
            handle
                .await
                .unwrap_or_else(|_| panic!("Request {i} should not panic"));
        }

        // Step 4: Poll for the background revalidation instead of sleeping a
        // fixed interval. The poll repeats the same query, so it can only ever
        // read the one revalidation's result.
        tracing::info!("Waiting for background revalidation to complete");
        let mut last = (CacheStatus::CacheStaleWhileRevalidate, 1);
        for _ in 0..100 {
            last = Arc::clone(&request_context)
                .scope(run_i64_query(&df, SQL, ResultsCacheMode::Default))
                .await;
            if last.0 == CacheStatus::CacheHit {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        // Step 5: the entry holds the revalidated value (the second scan), and
        // the table was scanned exactly twice: despite 10 concurrent stale
        // requests, only ONE background query executed.
        assert_eq!(
            last,
            (CacheStatus::CacheHit, 2),
            "the cache should hold the single revalidation's result"
        );
        assert_eq!(
            scans.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "the initial miss and exactly one revalidation should have scanned the table"
        );

        tracing::info!("Single-in-flight test completed successfully");
    }

    /// A one-column table whose every scan returns its own sequence number, so
    /// a test can count how many times a query over it actually executed.
    #[derive(Debug)]
    struct CountingScans {
        schema: arrow::datatypes::SchemaRef,
        scans: Arc<std::sync::atomic::AtomicI64>,
    }

    #[async_trait::async_trait]
    impl datafusion::catalog::TableProvider for CountingScans {
        fn schema(&self) -> arrow::datatypes::SchemaRef {
            Arc::clone(&self.schema)
        }

        fn table_type(&self) -> datafusion::datasource::TableType {
            datafusion::datasource::TableType::Base
        }

        async fn scan(
            &self,
            state: &dyn datafusion::catalog::Session,
            projection: Option<&Vec<usize>>,
            filters: &[datafusion::logical_expr::Expr],
            limit: Option<usize>,
        ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
            let scan = self.scans.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
            let batch = arrow::array::RecordBatch::try_new(
                Arc::clone(&self.schema),
                vec![Arc::new(Int64Array::from(vec![scan]))],
            )?;
            datafusion::datasource::MemTable::try_new(Arc::clone(&self.schema), vec![vec![batch]])?
                .scan(state, projection, filters, limit)
                .await
        }
    }

    /// Registers a [`CountingScans`] table and returns its scan counter.
    fn register_counted_table(
        df: &Arc<DataFusion>,
        name: &'static str,
    ) -> Arc<std::sync::atomic::AtomicI64> {
        let scans = Arc::new(std::sync::atomic::AtomicI64::new(0));
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("scan", arrow::datatypes::DataType::Int64, false),
        ]));
        df.ctx
            .register_table(
                TableReference::bare(name),
                Arc::new(CountingScans {
                    schema,
                    scans: Arc::clone(&scans),
                }),
            )
            .expect("should register table");
        scans
    }

    /// A client's `Cache-Control: no-cache` skips the cached lookup, runs the
    /// query, and stores the fresh result, so the next request without the
    /// header reads it rather than the entry the revalidation replaced. Holds
    /// under every cache key type: plan, SQL text, and a client-supplied key.
    #[tokio::test]
    async fn a_no_cache_request_stores_its_fresh_result_for_later_reads() {
        use spicepod::component::caching::CacheKeyType as ConfiguredCacheKeyType;

        for (configured, key_type, client_key) in [
            (ConfiguredCacheKeyType::Plan, CacheKeyType::Default, None),
            (ConfiguredCacheKeyType::Sql, CacheKeyType::Raw, None),
            (
                ConfiguredCacheKeyType::Plan,
                CacheKeyType::ClientSupplied,
                Some("no-cache-writeback".to_string()),
            ),
        ] {
            const SQL: &str = "SELECT scan FROM no_cache_writeback";
            let df = prepare_runtime(Some(SQLResultsCacheConfig {
                item_ttl: Some("10m".to_string()),
                cache_key_type: configured,
                ..Default::default()
            }))
            .await;
            let scans = register_counted_table(&df, "no_cache_writeback");
            let cached =
                create_test_request_context(CacheControl::Cache(key_type), client_key.clone());
            let no_cache =
                create_test_request_context(CacheControl::Revalidate(key_type), client_key.clone());
            let run = |context: &Arc<RequestContext>| {
                Arc::clone(context).scope(run_i64_query(&df, SQL, ResultsCacheMode::Default))
            };

            assert_eq!(
                run(&cached).await,
                (CacheStatus::CacheMiss, 1),
                "{key_type:?}"
            );
            assert_eq!(
                run(&cached).await,
                (CacheStatus::CacheHit, 1),
                "{key_type:?}"
            );

            // `no-cache` never serves the cached entry: it runs the query.
            assert_eq!(
                run(&no_cache).await,
                (CacheStatus::CacheBypass, 2),
                "{key_type:?}"
            );
            // ...and the next plain read is served what it returned.
            assert_eq!(
                run(&cached).await,
                (CacheStatus::CacheHit, 2),
                "{key_type:?}: the read after no-cache must see the fresh result"
            );

            assert_eq!(
                run(&no_cache).await,
                (CacheStatus::CacheBypass, 3),
                "{key_type:?}"
            );
            assert_eq!(
                run(&cached).await,
                (CacheStatus::CacheHit, 3),
                "{key_type:?}"
            );
            assert_eq!(
                scans.load(std::sync::atomic::Ordering::SeqCst),
                3,
                "{key_type:?}"
            );
        }
    }

    /// The runtime's own [`CacheControl::NoCache`] keeps bypassing the cache
    /// for reads, so a background revalidation never serves itself a hit.
    #[tokio::test]
    async fn the_runtime_no_cache_control_never_serves_a_cached_result() {
        const SQL: &str = "SELECT scan FROM runtime_no_cache";
        let df = prepare_runtime(None).await;
        let scans = register_counted_table(&df, "runtime_no_cache");
        let cached = create_test_request_context(CacheControl::Cache(CacheKeyType::Default), None);
        let bypass = create_test_request_context(CacheControl::NoCache, None);

        assert_eq!(
            Arc::clone(&cached)
                .scope(run_i64_query(&df, SQL, ResultsCacheMode::Default))
                .await,
            (CacheStatus::CacheMiss, 1)
        );
        assert_eq!(
            Arc::clone(&bypass)
                .scope(run_i64_query(&df, SQL, ResultsCacheMode::Default))
                .await,
            (CacheStatus::CacheBypass, 2)
        );
        assert_eq!(scans.load(std::sync::atomic::Ordering::SeqCst), 2);
    }

    /// A hit is served where the request arrived: it has nothing to plan or execute, so it
    /// must not wait on the query runtime. That holds for an entry held as batches and, under
    /// `encoding: zstd`, for a small encoded entry, which is decoded there. Every worker of
    /// that runtime is held while the hit is requested, so a query that still crossed onto it
    /// could not finish.
    #[tokio::test]
    async fn a_hit_is_served_without_the_query_runtime() {
        use spicepod::component::caching::{CacheKeyType as ConfiguredCacheKeyType, Encoding};

        for encoding in [Encoding::None, Encoding::Zstd] {
            for (configured, cache_control, client_key) in [
                (
                    ConfiguredCacheKeyType::Plan,
                    CacheControl::Cache(CacheKeyType::Default),
                    None,
                ),
                (
                    ConfiguredCacheKeyType::Sql,
                    CacheControl::Cache(CacheKeyType::Raw),
                    None,
                ),
                (
                    ConfiguredCacheKeyType::Sql,
                    CacheControl::Cache(CacheKeyType::ClientSupplied),
                    Some("served-in-place".to_string()),
                ),
            ] {
                let df = prepare_runtime(Some(SQLResultsCacheConfig {
                    item_ttl: Some("10m".to_string()),
                    cache_key_type: configured,
                    encoding,
                    ..Default::default()
                }))
                .await;
                assert_eq!(
                    df.results_cache_provider()
                        .expect("the test runtime has a results cache")
                        .encoder()
                        .is_some(),
                    encoding == Encoding::Zstd,
                    "{encoding:?}: results are stored encoded exactly when an encoding is configured"
                );
                let query_runtime = runtime_async::ManagedTokioRuntime::try_new()
                    .expect("the query runtime should start");
                let query_runtime_handle = query_runtime.handle().clone();
                df.set_cpu_runtime(query_runtime);
                let request_context = create_test_request_context(cache_control, client_key);

                let first = Arc::clone(&request_context)
                    .scope(run_i64_query(&df, "SELECT 7", ResultsCacheMode::Default))
                    .await;
                assert_eq!(
                    first,
                    (CacheStatus::CacheMiss, 7),
                    "{encoding:?}, {cache_control:?}: the first run executes on the query runtime and stores the result"
                );

                // Hold every worker of the query runtime until the hit has been served.
                let workers = query_runtime_handle.metrics().num_workers();
                let release = Arc::new(std::sync::Barrier::new(workers + 1));
                let held = Arc::new(std::sync::atomic::AtomicUsize::new(0));
                for _ in 0..workers {
                    let release = Arc::clone(&release);
                    let held = Arc::clone(&held);
                    query_runtime_handle.spawn(async move {
                        held.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        release.wait();
                    });
                }
                let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
                while held.load(std::sync::atomic::Ordering::SeqCst) < workers {
                    assert!(
                        tokio::time::Instant::now() < deadline,
                        "only {} of {workers} query runtime workers were held",
                        held.load(std::sync::atomic::Ordering::SeqCst)
                    );
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }

                let hit = tokio::time::timeout(
                    Duration::from_secs(10),
                    Arc::clone(&request_context).scope(run_i64_query(
                        &df,
                        "SELECT 7",
                        ResultsCacheMode::Default,
                    )),
                )
                .await;
                release.wait();

                let Ok(hit) = hit else {
                    panic!(
                        "{encoding:?}, {cache_control:?}: a cache hit waited on the busy query runtime"
                    );
                };
                assert_eq!(
                    hit,
                    (CacheStatus::CacheHit, 7),
                    "{encoding:?}, {cache_control:?}: the second run is served from the cache"
                );
            }
        }
    }

    /// `runtime.task_history.execution_duration_ms` is the lifetime of the query's `sql_query`
    /// span. A query that waits for a query runtime worker must not count that wait, only the
    /// work it does once it runs there, so the span opens on the query runtime.
    #[tokio::test]
    async fn a_wait_for_a_query_runtime_worker_is_not_in_the_task_history_duration() {
        use std::sync::OnceLock;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::time::Instant;

        use tracing::field::{Field, Visit};
        use tracing::span::{Attributes, Id};
        use tracing_subscriber::layer::{Context, SubscriberExt};
        use tracing_subscriber::registry::LookupSpan;

        const SQL: &str = "SELECT 11 AS query_runtime_wait";
        // How long every query runtime worker is held before the query can run there. The
        // delay is the behavior under test, so it is a fixed sleep.
        const HOLD: Duration = Duration::from_millis(500);

        type Lifetimes = Arc<parking_lot::Mutex<Vec<(String, Duration)>>>;

        /// Records how long each `sql_query` span lived, by the SQL it ran.
        struct SqlQueryLifetimes(Lifetimes);

        struct Opened {
            input: String,
            at: Instant,
        }

        #[derive(Default)]
        struct Input(Option<String>);

        impl Visit for Input {
            fn record_str(&mut self, field: &Field, value: &str) {
                if field.name() == "input" {
                    self.0 = Some(value.to_string());
                }
            }

            fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
                if field.name() == "input" {
                    self.0 = Some(format!("{value:?}"));
                }
            }
        }

        impl<S> tracing_subscriber::Layer<S> for SqlQueryLifetimes
        where
            S: tracing::Subscriber + for<'a> LookupSpan<'a>,
        {
            fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
                if attrs.metadata().name() != "sql_query" {
                    return;
                }
                let mut input = Input::default();
                attrs.record(&mut input);
                if let (Some(input), Some(span)) = (input.0, ctx.span(id)) {
                    span.extensions_mut().insert(Opened {
                        input,
                        at: Instant::now(),
                    });
                }
            }

            fn on_close(&self, id: Id, ctx: Context<'_, S>) {
                if let Some(span) = ctx.span(&id)
                    && let Some(opened) = span.extensions().get::<Opened>()
                {
                    self.0
                        .lock()
                        .push((opened.input.clone(), opened.at.elapsed()));
                }
            }
        }

        // The span can open on a query runtime worker thread, which a thread-local subscriber
        // does not reach, so the recorder is installed for the whole process. Nothing else in
        // this test binary installs a global subscriber.
        static LIFETIMES: OnceLock<Lifetimes> = OnceLock::new();
        let lifetimes = LIFETIMES.get_or_init(|| {
            let lifetimes = Arc::new(parking_lot::Mutex::new(Vec::new()));
            tracing::subscriber::set_global_default(
                tracing_subscriber::registry().with(SqlQueryLifetimes(Arc::clone(&lifetimes))),
            )
            .expect("no other test in this binary installs a global tracing subscriber");
            lifetimes
        });

        let df = prepare_runtime(None).await;
        let query_runtime =
            runtime_async::ManagedTokioRuntime::try_new().expect("the query runtime should start");
        let query_runtime_handle = query_runtime.handle().clone();
        df.set_cpu_runtime(query_runtime);

        let workers = query_runtime_handle.metrics().num_workers();
        let release = Arc::new(std::sync::Barrier::new(workers + 1));
        let held = Arc::new(AtomicUsize::new(0));
        for _ in 0..workers {
            let release = Arc::clone(&release);
            let held = Arc::clone(&held);
            query_runtime_handle.spawn(async move {
                held.fetch_add(1, Ordering::SeqCst);
                release.wait();
            });
        }
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while held.load(Ordering::SeqCst) < workers {
            assert!(
                tokio::time::Instant::now() < deadline,
                "only {} of {workers} query runtime workers were held",
                held.load(Ordering::SeqCst)
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let releaser = std::thread::spawn(move || {
            std::thread::sleep(HOLD);
            release.wait();
        });

        let request_context =
            create_test_request_context(CacheControl::Cache(CacheKeyType::Default), None);
        let result = request_context
            .scope(run_i64_query(&df, SQL, ResultsCacheMode::Default))
            .await;
        releaser
            .join()
            .expect("the thread releasing the workers should finish");
        assert_eq!(
            result,
            (CacheStatus::CacheMiss, 11),
            "the query misses the cache and runs on the query runtime"
        );

        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        let lifetime = loop {
            if let Some((_, lifetime)) = lifetimes.lock().iter().find(|(input, _)| input == SQL) {
                break *lifetime;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "the query's sql_query span never closed"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        eprintln!(
            "the sql_query span lasted {lifetime:?}; every query runtime worker was held for {HOLD:?} before the query could run"
        );
        assert!(
            lifetime < HOLD,
            "the sql_query span lasted {lifetime:?}, which includes the {HOLD:?} the query waited for a query runtime worker"
        );
    }

    /// A cached result served over HTTP keeps the streamed JSON framing. Sending
    /// a small complete hit as a `Content-Length` body is an HTTP contract
    /// change and needs an Enhancement.
    #[tokio::test]
    async fn a_served_hit_is_streamed_over_http() {
        use http_body::Body as _;
        use http_body_util::BodyExt;

        let df = prepare_runtime(None).await;
        register_id_table(&df, "served_over_http", &[1, 2, 3]);
        let request_context =
            create_test_request_context(CacheControl::Cache(CacheKeyType::Default), None);
        let respond = || {
            Arc::clone(&request_context).scope(crate::http::v1::sql_to_http_response(
                Arc::clone(&df),
                Arc::from("SELECT id FROM served_over_http ORDER BY id"),
                None,
                crate::http::v1::ResponseMimeType::Json,
                false,
            ))
        };
        let cache_status = |response: &axum::response::Response| {
            response
                .headers()
                .get("results-cache-status")
                .and_then(|value| value.to_str().ok())
                .map(str::to_string)
        };

        let miss = respond().await;
        assert_eq!(cache_status(&miss).as_deref(), Some("MISS"));
        miss.into_body()
            .collect()
            .await
            .expect("the miss to be read in full, which stores it");

        let hit = respond().await;
        assert_eq!(cache_status(&hit).as_deref(), Some("HIT"));
        assert!(
            hit.body().size_hint().exact().is_none(),
            "a cache hit must keep the streamed JSON framing, not a Content-Length body"
        );
        let body = hit
            .into_body()
            .collect()
            .await
            .expect("the hit to be read in full")
            .to_bytes();
        assert_eq!(body.as_ref(), br#"[{"id":1},{"id":2},{"id":3}]"#);
    }
}
