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

//! Bounding a `refresh_mode: caching` accelerator.
//!
//! Everything here is driven by queries against the accelerator itself rather
//! than by bookkeeping held beside it. The accelerator is the cache, so what it
//! currently holds is a question only it can answer — an in-memory tally would
//! have to be rebuilt on restart, kept in step with every write path, and would
//! disagree with the table the moment a row was evicted by anything else.
//! Asking costs one aggregate per sweep and cannot drift.
//!
//! Three bounds are applied, in order:
//!
//! 1. **Expiry** — rows fetched longer ago than `caching_ttl` plus the
//!    stale-while-revalidate window are deleted. They can no longer be served,
//!    so keeping them only consumes budget.
//! 2. **Item count** — `caching_max_items`.
//! 3. **Byte budget** — `caching_max_size`, measured by summing each row's
//!    payload bytes.
//!
//! Both the deadline and the size are *derived* rather than stored. A row's
//! deadline is `_fetched_at + caching_ttl`, and `caching_ttl` is dataset
//! configuration the sweep already has, so a stored copy would only be a
//! denormalisation that can go stale against the config it came from. A row's
//! size is `octet_length` over its own payload columns, which the engine
//! computes during the same aggregate the sweep already runs, and which stays
//! true if the rows are ever rewritten. Neither needs a storage column, so a
//! caching accelerator's on-disk schema is unchanged.
//!
//! Expiry runs first because it is free capacity: evicting a live entry while
//! an expired one still occupies the budget would be a straight loss.

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use arrow::array::{Array, RecordBatch, StringArray};
use arrow::datatypes::DataType;
use datafusion::catalog::TableProvider;
use datafusion::common::Result as DataFusionResult;
use datafusion::functions::expr_fn::octet_length;
use datafusion::functions_aggregate::expr_fn::{bool_or, count, max, min, sum};
use datafusion::prelude::{Expr, SessionContext, cast, coalesce, col, lit};
use datafusion::scalar::ScalarValue;
use datafusion::sql::TableReference;
use tokio::runtime::Handle;

use super::RetentionPredicate;
use super::caching::{
    CACHE_NAMESPACE_COLUMN, CACHE_REFRESHED_AT_COLUMN, REQUEST_KEY_COLUMNS, effective_max_age,
};
use runtime_datafusion::session_config::get_df_default_config;
use runtime_object_store::registry::default_runtime_env;
use util::expr::combine_exprs_balanced;

/// The most entries one sweep will name in a single `DELETE` predicate.
///
/// The predicate is an `OR` over per-entry equality tests, so it grows with the
/// number of entries evicted; past some width the plan costs more than the
/// eviction saves. Overflow is not dropped — it is evicted by the next sweep,
/// and the shortfall is logged rather than passed over in silence.
///
/// This bounds *predicate width*, which is a hard limit rather than a taste:
/// DataFusion walks an expression tree recursively, and a 50,000-wide `OR`
/// overflowed the stack and aborted the process outright. It is deliberately not
/// the mechanism that keeps a large cache within its budget — see
/// [`bulk_cutoff`], which evicts any number of entries in one small predicate
/// whenever it is provably safe to do so. This cap only bounds the fallback.
const MAX_ENTRIES_PER_SWEEP: usize = 512;

/// The bounds a caching accelerator is held to, beyond its TTLs.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CacheLimits {
    /// `caching_max_size`: total bytes the stored rows may occupy.
    pub max_size_bytes: Option<u64>,
    /// `caching_max_items`: number of rows that may be stored.
    pub max_items: Option<u64>,
    /// `caching_ttl`: how long after it was fetched an entry stays fresh. With
    /// `stale_while_revalidate` it gives the deadline past which an entry can
    /// no longer be served and so no longer needs keeping.
    pub ttl: Option<Duration>,
    /// `caching_stale_while_revalidate_ttl`: how long past its TTL an entry
    /// may still be served, and so how long past it the entry must be kept.
    pub stale_while_revalidate: Option<Duration>,
    /// `caching_stale_if_error`: whether an expired entry may still be served
    /// when the origin is failing.
    ///
    /// This is why expiry alone cannot bound the cache. An entry kept as
    /// error-fallback material is one the expiry sweep must not delete, so with
    /// this enabled the byte and item budgets are the only thing standing
    /// between the cache and unbounded growth.
    pub stale_if_error: bool,
}

impl CacheLimits {
    /// True when a sweep would do something. With `stale_if_error` disabled the
    /// expiry sweep alone is worth running; with it enabled, only a configured
    /// budget can remove anything — so a dataset for which this is false is one
    /// nothing will ever evict from, the configuration #13525 describes.
    #[must_use]
    pub fn is_enforced(&self) -> bool {
        self.max_size_bytes.is_some() || self.max_items.is_some() || !self.stale_if_error
    }
}

/// Shortest interval between eviction sweeps. Sweeping much faster than the
/// entry TTL finds nothing new to expire while still costing an aggregate over
/// the whole table.
const MIN_SWEEP_INTERVAL: Duration = Duration::from_secs(30);

/// Longest interval between eviction sweeps. A budget is only as tight as the
/// sweep that enforces it — between sweeps the cache may overshoot by whatever
/// the workload writes — so the interval is capped regardless of how long the
/// TTL is. Deriving it from `caching_ttl` alone is what let a year-long
/// stale-while-revalidate window schedule effectively one check ever.
const MAX_SWEEP_INTERVAL: Duration = Duration::from_mins(5);

/// How often to sweep, given the dataset's entry TTL.
#[must_use]
pub fn sweep_interval(caching_ttl: Option<Duration>) -> Duration {
    caching_ttl
        .unwrap_or(MIN_SWEEP_INTERVAL)
        .clamp(MIN_SWEEP_INTERVAL, MAX_SWEEP_INTERVAL)
}

/// Bounds a `refresh_mode: caching` accelerator as a retention filter, so the
/// cache is swept by the same loop, lock and index-aware delete every other
/// dataset uses rather than by a second one beside it.
///
/// It resolves the whole decision itself and returns one predicate, because the
/// three things it must apply are not independent: what expiry removes changes
/// whether a budget is exceeded at all, and two filters computed against the
/// same snapshot would each select entries the other was already deleting.
#[derive(Debug)]
pub struct CacheEvictionPredicate {
    dataset_name: TableReference,
    limits: CacheLimits,
    io_runtime: Handle,
}

impl CacheEvictionPredicate {
    #[must_use]
    pub fn new(dataset_name: TableReference, limits: CacheLimits, io_runtime: Handle) -> Self {
        Self {
            dataset_name,
            limits,
            io_runtime,
        }
    }

    /// Warns once, at startup, about configurations this can only partly
    /// enforce — so an operator learns it from the log rather than from
    /// unexplained growth.
    ///
    /// * `accelerator` - the storage the cache is held in, whose schema decides
    ///   which entries can be identified and which payloads can be measured.
    /// * `has_user_retention` - whether the dataset also sets `retention_period`
    ///   or `retention_sql`. Those evict entries as well, so a cache with no
    ///   budget of its own is not necessarily unbounded.
    pub fn warn_about_unenforceable(
        &self,
        accelerator: &Arc<dyn TableProvider>,
        has_user_retention: bool,
    ) {
        let dataset_name = &self.dataset_name;
        let schema = accelerator.schema();

        if self.limits.max_size_bytes.is_some() {
            let unmeasurable = unmeasurable_payload_columns(&schema);
            if !unmeasurable.is_empty() {
                tracing::warn!(
                    "Cache dataset '{dataset_name}' stores column(s) {columns} whose size cannot be measured, so `caching_max_size` counts less than the acceleration actually holds and it may grow past the budget. Store the response as text to have it counted. For details, visit: https://spiceai.org/docs/components/data-accelerators/data-refresh#refresh-modes",
                    columns = unmeasurable
                        .iter()
                        .map(|c| format!("'{c}'"))
                        .collect::<Vec<_>>()
                        .join(", "),
                );
            }
        }

        if (self.limits.max_items.is_some() || self.limits.max_size_bytes.is_some())
            && entry_key_columns(&schema).is_empty()
        {
            tracing::warn!(
                "Cache dataset '{dataset_name}' stores none of the request columns that identify a cache entry ({keys}), so `caching_max_size` and `caching_max_items` cannot be enforced and the acceleration will keep growing. Remove the limits, or use a connector that records the request each row came from.",
                keys = REQUEST_KEY_COLUMNS.join(", "),
            );
        }

        if !self.limits.is_enforced() && !has_user_retention {
            tracing::warn!(
                "Dataset '{dataset_name}' sets `caching_stale_if_error: enabled` with no `caching_max_size` or `caching_max_items`, so no cached entry is ever evicted and the acceleration will grow without bound — expired entries are deliberately kept as fallback for a failing origin. Set a budget to bound it. For details, visit: https://spiceai.org/docs/components/data-accelerators/data-refresh#refresh-modes"
            );
        }
    }
}

#[async_trait::async_trait]
impl RetentionPredicate for CacheEvictionPredicate {
    async fn delete_expr(
        &self,
        accelerator: &Arc<dyn TableProvider>,
        configured: Option<Expr>,
    ) -> DataFusionResult<Option<Expr>> {
        if !self.limits.is_enforced() && configured.is_none() {
            // Nothing would ever be selected, so the whole-table aggregate below
            // would run every tick to return an empty answer. Warned about at
            // load; see `warn_about_unenforceable`.
            return Ok(None);
        }

        let schema = accelerator.schema();
        let key_columns = entry_key_columns(&schema);
        let has_fetched_at = schema.column_with_name(CACHE_REFRESHED_AT_COLUMN).is_some();

        if key_columns.is_empty() {
            // Nothing identifies an entry, so nothing can be evicted as one.
            // Expiry can still run row by row, safely: with no key there is no
            // entry to leave half of. The dataset's own retention predicate is
            // row-level already and passes through unchanged.
            let expired = if self.limits.stale_if_error || !has_fetched_at {
                None
            } else {
                expiry_cutoff(&self.limits).map(row_level_expiry_expr)
            };
            return Ok(match (configured, expired) {
                (Some(a), Some(b)) => Some(a.or(b)),
                (Some(only), None) | (None, Some(only)) => Some(only),
                (None, None) => None,
            });
        }

        let entries = rank_entries(
            accelerator,
            &self.io_runtime,
            &key_columns,
            &schema,
            configured,
            self.limits.max_size_bytes,
        )
        .await?;

        let mut doomed: Vec<&EntryCost> = Vec::new();
        let mut survivors: Vec<&EntryCost> = Vec::new();

        // 1. Entries the dataset's own retention predicate matches, and entries
        //    past their window. Both are judged per entry: a row-level delete
        //    could take part of a multi-row response and leave the rest to be
        //    served as if it were whole.
        let cutoff = (!self.limits.stale_if_error && has_fetched_at)
            .then(|| expiry_cutoff(&self.limits))
            .flatten();
        for entry in &entries {
            let retained_out = entry.matches_configured;
            let expired = cutoff.is_some_and(|cutoff| entry.expired_at(cutoff));
            if retained_out || expired {
                doomed.push(entry);
            } else {
                survivors.push(entry);
            }
        }

        // 2. Budgets, over what step 1 leaves behind — so a budget is not
        //    charged for entries that are going anyway.
        for budget in [
            self.limits.max_items.map(Budget::Items),
            self.limits.max_size_bytes.map(Budget::Bytes),
        ]
        .into_iter()
        .flatten()
        {
            let keep = select_doomed_refs(&survivors, budget);
            if keep == survivors.len() {
                continue;
            }
            tracing::debug!(
                "Cache eviction for dataset '{dataset}' is evicting {evicting} entries to satisfy `{budget_name}`.",
                dataset = self.dataset_name,
                evicting = survivors.len() - keep,
                budget_name = budget.name(),
            );
            // The evicted set is the tail, so the next budget sees exactly what
            // this one leaves behind without any recounting.
            doomed.extend(survivors.drain(keep..));
        }

        if doomed.is_empty() {
            return Ok(None);
        }

        // One range predicate when the doomed and surviving entries do not
        // overlap in fetch time: exact, and not bounded by how many entries it
        // covers.
        if let Some(cutoff) = bulk_cutoff(&doomed, &survivors) {
            return Ok(Some(
                col(CACHE_REFRESHED_AT_COLUMN)
                    .lt_eq(lit(ScalarValue::TimestampNanosecond(Some(cutoff), None))),
            ));
        }

        // Otherwise name each entry, bounded to the rows it held when ranked.
        // Oldest first, since the cap may not reach all of them this sweep.
        let (naming, deferred) = nameable(&doomed);
        if deferred > 0 {
            tracing::info!(
                "Cache eviction for dataset '{dataset}' cannot separate {total} over-budget entries from the ones it keeps by fetch time, so it is naming {naming} individually and leaving {deferred} for the next sweep.",
                dataset = self.dataset_name,
                total = doomed.len(),
                naming = naming.len(),
            );
        }
        let predicates: Vec<Expr> = naming
            .iter()
            .filter_map(|entry| entry_predicate(&entry.key, entry.newest))
            .collect();
        Ok(combine_exprs_balanced(predicates, Expr::or))
    }
}

/// Deletes rows past their window without regard to which entry they belong to.
///
/// Everywhere else in this module a range delete is exactly what must not
/// happen. It is safe *here* only because the caller has established there are
/// no request columns, so there is no entry to leave half of, and a row past its
/// window is unservable on its own terms. A row with no fetch time goes too:
/// there is no deadline to hold it to, and the read path already treats a null
/// timestamp as expired.
fn row_level_expiry_expr(cutoff: i64) -> Expr {
    col(CACHE_REFRESHED_AT_COLUMN).lt_eq(lit(ScalarValue::TimestampNanosecond(Some(cutoff), None)))
}

/// The instant before which an entry can no longer be served: `caching_ttl`
/// plus the stale-while-revalidate grace, ago.
fn expiry_cutoff(limits: &CacheLimits) -> Option<i64> {
    let window = effective_max_age(limits.ttl) + limits.stale_while_revalidate.unwrap_or_default();
    nanos_since_epoch(SystemTime::now().checked_sub(window)?)
}

/// Which budget a trim is enforcing, and what each entry costs against it.
#[derive(Debug, Clone, Copy)]
enum Budget {
    Items(u64),
    Bytes(u64),
}

impl Budget {
    fn allowance(self) -> u64 {
        match self {
            Budget::Items(n) | Budget::Bytes(n) => n,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Budget::Items(_) => "caching_max_items",
            Budget::Bytes(_) => "caching_max_size",
        }
    }
}

/// An expression giving one row's payload size in bytes, or `None` when the
/// schema has no columns this can measure.
///
/// Text is measured exactly with `octet_length` and fixed-width columns
/// contribute their width, which between them covers what a caching
/// accelerator stores: an HTTP response body is text, and a body decomposed
/// into columns is decomposed into text columns. Columns of neither kind are
/// not counted — see [`unmeasurable_payload_columns`], which reports them so
/// the shortfall is stated rather than silent.
///
/// The figure is a payload measure rather than an on-disk one, which is what
/// `caching_max_size` bounds: the engine's own storage adds indexes and
/// compression that nothing here could predict.
///
/// The caching accelerator's reserved columns are excluded, so the budget
/// counts what was cached rather than the bookkeeping around it.
fn payload_bytes_expr(schema: &arrow::datatypes::Schema) -> Option<Expr> {
    schema
        .fields()
        .iter()
        .filter(|field| !super::caching::is_reserved_caching_column(field.name()))
        .filter_map(|field| match field.data_type() {
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                // `octet_length` is Int32 and NULL-propagating; widen it so a
                // large cache cannot overflow the running sum, and give NULL a
                // weight of zero rather than poisoning the row's total.
                Some(coalesce(vec![
                    cast(octet_length(col(field.name())), DataType::Int64),
                    lit(0_i64),
                ]))
            }
            other => other
                .primitive_width()
                .and_then(|width| i64::try_from(width).ok())
                .map(lit),
        })
        .reduce(|acc, next| acc + next)
}

/// Payload columns [`payload_bytes_expr`] cannot weigh, so `caching_max_size`
/// under-counts by whatever they hold.
///
/// `response_headers` is excluded deliberately: it is a `Map` on every HTTP
/// response, it is small beside the body it accompanies, and reporting it would
/// put a warning in the log of every ordinary caching dataset that sets a
/// budget — which is how a warning stops being read. Anything else nested is
/// worth saying out loud, because it could be the payload itself.
fn unmeasurable_payload_columns(schema: &arrow::datatypes::Schema) -> Vec<String> {
    schema
        .fields()
        .iter()
        .filter(|field| !super::caching::is_reserved_caching_column(field.name()))
        .filter(|field| field.name() != "response_headers")
        .filter(|field| {
            !matches!(
                field.data_type(),
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
            ) && field.data_type().primitive_width().is_none()
        })
        .map(|field| field.name().clone())
        .collect()
}

/// One cache entry as the ranking query sees it: its key values and what it
/// costs against each budget.
struct EntryCost {
    key: Vec<(String, Option<String>)>,
    rows: u64,
    bytes: u64,
    /// `min(_fetched_at)` over the entry's rows, or `None` where they carry no
    /// fetch time. An entry is only as fresh as its least fresh row, which is
    /// the same rule `check_cache_freshness` applies on read.
    oldest: Option<i64>,
    /// `max(_fetched_at)` over the entry's rows at the moment it was ranked.
    ///
    /// Carried into the delete predicate so a delete cannot take rows written
    /// after the ranking that chose this entry. See [`entry_predicate`].
    newest: Option<i64>,
    /// True when the dataset's own retention predicate matched any row of this
    /// entry.
    ///
    /// Judged per entry rather than per row: a retention rule that matches one
    /// page of a paginated response must take the whole response, or the pages
    /// it leaves behind would be served as though they were all there was.
    matches_configured: bool,
}

impl EntryCost {
    /// True when this entry can no longer be served, so keeping it only
    /// consumes budget.
    ///
    /// An entry with no fetch time at all is expired: there is no deadline to
    /// hold it to, and the read path treats a null timestamp as expired too.
    fn expired_at(&self, cutoff: i64) -> bool {
        self.oldest.is_none_or(|oldest| oldest <= cutoff)
    }
}

/// The slice of `doomed` one delete may name, and how many that leaves behind.
///
/// Oldest first: `doomed` is ordered newest-first, so the tail is the oldest and
/// the cap defers the *newest* of the doomed. Trimming the other end would keep
/// the least valuable entries and re-doom the same ones every sweep.
fn nameable<'a>(doomed: &'a [&'a EntryCost]) -> (&'a [&'a EntryCost], usize) {
    let deferred = doomed.len().saturating_sub(MAX_ENTRIES_PER_SWEEP);
    (&doomed[deferred..], deferred)
}

/// The `_fetched_at` cutoff that deletes exactly `doomed` and nothing else, when
/// one exists.
///
/// Eviction normally names each entry by key, because a cached response can span
/// rows with different fetch times and a range delete could cut one in half. But
/// when every doomed entry's newest row is older than every survivor's oldest
/// row, no entry straddles the boundary and `_fetched_at <= cutoff` selects the
/// doomed set exactly — one small predicate instead of an `OR` per entry.
///
/// That matters because predicate width is capped by stack depth
/// ([`MAX_ENTRIES_PER_SWEEP`]), so naming entries individually cannot evict
/// faster than a few hundred per sweep — which a cache ingesting hundreds per
/// second outruns forever. A workload that fetches new keys over time separates
/// cleanly in fetch order, so this is the ordinary case and the per-key fallback
/// is for the interleaved one.
///
/// Returns `None` if either side lacks a fetch time, or if the two overlap.
fn bulk_cutoff(doomed: &[&EntryCost], survivors: &[&EntryCost]) -> Option<i64> {
    let newest_doomed = doomed.iter().map(|e| e.newest).max().flatten()?;
    if doomed.iter().any(|e| e.newest.is_none()) {
        return None;
    }
    let oldest_survivor = survivors
        .iter()
        .try_fold(i64::MAX, |acc, e| e.oldest.map(|o| acc.min(o)))?;
    (newest_doomed < oldest_survivor).then_some(newest_doomed)
}

/// Chooses which entries to evict from `entries`, which must be ordered
/// most-recently-fetched first.
///
/// Returns how many of the most recent entries stay; the rest are evicted. An
/// index rather than a second vector, because the evicted set is always a
/// contiguous tail.
///
/// Eviction takes a contiguous *oldest* suffix rather than whatever happens not
/// to fit, so the cache always holds the most recent entries it can afford. If
/// more entries are doomed than one `DELETE` predicate should name, the
/// **oldest** of them go first — trimming the other end would keep the least
/// valuable entries and re-doom the same ones every sweep.
fn select_doomed_refs(entries: &[&EntryCost], budget: Budget) -> usize {
    let allowance = budget.allowance();
    let mut spent: u64 = 0;

    for (index, entry) in entries.iter().enumerate() {
        let cost = match budget {
            Budget::Items(_) => entry.rows,
            Budget::Bytes(_) => entry.bytes,
        };
        if spent.saturating_add(cost) > allowance {
            return index;
        }
        spent = spent.saturating_add(cost);
    }

    entries.len()
}

/// Groups the stored rows into cache entries and orders them
/// most-recently-fetched first, with what each one costs against either budget.
///
/// This is the sweep's only pass over the accelerator, so it collects
/// everything both budgets need at once: a row count, a payload-byte total, and
/// the key to name the entry in a `DELETE`.
async fn rank_entries(
    accelerator: &Arc<dyn TableProvider>,
    io_runtime: &Handle,
    key_columns: &[String],
    schema: &arrow::datatypes::Schema,
    configured: Option<Expr>,
    max_size_bytes: Option<u64>,
) -> DataFusionResult<Vec<EntryCost>> {
    // Only measured when something charges against it. Summing `octet_length`
    // over the payload means reading every cached response body off disk, and
    // for a dataset with a TTL and no byte budget the figure is discarded.
    let size_expr = max_size_bytes.and_then(|_| payload_bytes_expr(schema));
    let has_size = size_expr.is_some();
    let has_fetched_at = schema.column_with_name(CACHE_REFRESHED_AT_COLUMN).is_some();

    let mut aggregates = vec![count(lit(1)).alias("rows")];
    if let Some(size_expr) = size_expr {
        aggregates.push(sum(size_expr).alias("bytes"));
    }
    if has_fetched_at {
        // Rank on — and expire by — the entry's *oldest* page: an entry is only
        // as fresh as the least fresh row a query would be served from it.
        aggregates.push(min(col(CACHE_REFRESHED_AT_COLUMN)).alias("oldest"));
        aggregates.push(max(col(CACHE_REFRESHED_AT_COLUMN)).alias("newest"));
    }
    let has_configured = configured.is_some();
    if let Some(configured) = configured {
        aggregates.push(bool_or(configured).alias("configured"));
    }

    // Built the way every other query against this accelerator is, so the
    // aggregate is planned with the runtime's config and object-store registry
    // and can be pushed down to the engine holding the data. A bare
    // `SessionContext` would pull every payload column of every cached row
    // through DataFusion to produce two numbers.
    let ctx = SessionContext::new_with_config_rt(
        get_df_default_config(),
        default_runtime_env(io_runtime.clone()),
    );
    let mut df = ctx
        .read_table(Arc::clone(accelerator))?
        .aggregate(key_columns.iter().map(col).collect(), aggregates)?;

    if has_fetched_at {
        df = df.sort(vec![col("oldest").sort(false, false)])?;
    }

    let batches = df.collect().await?;

    let mut entries = Vec::new();
    for batch in &batches {
        for row in 0..batch.num_rows() {
            let key = key_columns
                .iter()
                .map(|name| (name.clone(), read_utf8(batch, name, row)))
                .collect();
            entries.push(EntryCost {
                key,
                rows: read_u64_at(batch, "rows", row).unwrap_or(0),
                bytes: if has_size {
                    read_u64_at(batch, "bytes", row).unwrap_or(0)
                } else {
                    0
                },
                oldest: if has_fetched_at {
                    read_timestamp_nanos(batch, "oldest", row)
                } else {
                    None
                },
                newest: if has_fetched_at {
                    read_timestamp_nanos(batch, "newest", row)
                } else {
                    None
                },
                matches_configured: has_configured && read_bool(batch, "configured", row),
            });
        }
    }

    Ok(entries)
}

/// Builds `col = value AND ...` identifying exactly one cache entry, bounded to
/// the rows that entry held when it was ranked.
///
/// An entry with a NULL key component is skipped rather than matched with
/// `IS NULL`, because a delete naming many entries is one statement: an engine
/// that rejects `IS NULL` rejects the whole thing, so emitting it would cost
/// every entry's eviction to save one. HTTP metadata columns hold empty strings
/// rather than NULLs, so this is an edge the shipped connectors do not reach.
///
/// `ranked_newest` is what makes it safe to rank without holding the write
/// lock. A refresh replaces an entry by deleting it and appending rows with a
/// newer `_fetched_at`, so an entry refreshed between the ranking and the delete
/// has no row at or below the timestamp it was ranked at, and the predicate
/// matches nothing — the entry survives whole rather than being deleted out from
/// under the refresh that just repaired it. Without this a sweep could turn a
/// successfully refreshed response into a cache miss, which is precisely what
/// `stale_if_error` exists to avoid during an origin outage.
///
/// Deliberately no `IS NULL` disjunct for a row with no fetch time: an
/// accelerator's delete translator need not accept `IS NULL`, and the `DuckDB`
/// one does not — emitting it failed every sweep with "Expression not
/// supported" and evicted nothing at all. An entry whose rows *all* lack a
/// fetch time is still removed whole, because it ranks with `newest = None`
/// and gets the bare identity predicate. Only an entry mixing timestamped and
/// untimestamped rows keeps the untimestamped ones, which one fetch cannot
/// produce.
fn entry_predicate(key: &[(String, Option<String>)], ranked_newest: Option<i64>) -> Option<Expr> {
    let identity = key
        .iter()
        .map(|(name, value)| value.as_ref().map(|v| col(name).eq(lit(v.as_str()))))
        .collect::<Option<Vec<_>>>()?
        .into_iter()
        .reduce(Expr::and)?;

    let Some(ranked_newest) = ranked_newest else {
        return Some(identity);
    };

    Some(identity.and(
        col(CACHE_REFRESHED_AT_COLUMN).lt_eq(lit(ScalarValue::TimestampNanosecond(
            Some(ranked_newest),
            None,
        ))),
    ))
}

/// The subset of [`REQUEST_KEY_COLUMNS`] this accelerator stores, plus the cache
/// namespace when present.
///
/// The namespace belongs in the key: two principals can hold entries under the
/// same request key, and evicting by request alone would take out a row the
/// budget never charged to that entry.
fn entry_key_columns(schema: &arrow::datatypes::Schema) -> Vec<String> {
    let mut columns: Vec<String> = REQUEST_KEY_COLUMNS
        .iter()
        .filter(|name| is_utf8_column(schema, name))
        .map(|name| (*name).to_string())
        .collect();

    if !columns.is_empty() && is_utf8_column(schema, CACHE_NAMESPACE_COLUMN) {
        columns.push(CACHE_NAMESPACE_COLUMN.to_string());
    }

    columns
}

/// Key columns are read back as strings to rebuild the delete predicate, so a
/// column of another type is not one this eviction can key on.
fn is_utf8_column(schema: &arrow::datatypes::Schema, name: &str) -> bool {
    schema
        .column_with_name(name)
        .is_some_and(|(_, field)| matches!(field.data_type(), DataType::Utf8 | DataType::LargeUtf8))
}

fn nanos_since_epoch(time: SystemTime) -> Option<i64> {
    i64::try_from(time.duration_since(SystemTime::UNIX_EPOCH).ok()?.as_nanos()).ok()
}

/// Reads a non-negative count out of an aggregate result, whatever integer
/// width the engine produced it as.
fn read_u64_at(batch: &RecordBatch, name: &str, row: usize) -> Option<u64> {
    let array = batch.column_by_name(name)?;
    if !array.is_valid(row) {
        return None;
    }
    let scalar = ScalarValue::try_from_array(array, row).ok()?;
    match scalar {
        ScalarValue::UInt64(Some(v)) => Some(v),
        ScalarValue::Int64(Some(v)) => u64::try_from(v).ok(),
        ScalarValue::UInt32(Some(v)) => Some(u64::from(v)),
        ScalarValue::Int32(Some(v)) => u64::try_from(v).ok(),
        ScalarValue::Decimal128(Some(v), _, 0) => u64::try_from(v).ok(),
        _ => None,
    }
}

/// Reads a boolean aggregate, treating NULL (no rows matched) as false.
fn read_bool(batch: &RecordBatch, name: &str, row: usize) -> bool {
    batch
        .column_by_name(name)
        .filter(|array| array.is_valid(row))
        .and_then(|array| ScalarValue::try_from_array(array, row).ok())
        .is_some_and(|scalar| matches!(scalar, ScalarValue::Boolean(Some(true))))
}

/// Reads a timestamp aggregate back as nanoseconds, whatever precision the
/// engine stored it at — Cayenne keeps microseconds.
fn read_timestamp_nanos(batch: &RecordBatch, name: &str, row: usize) -> Option<i64> {
    let array = batch.column_by_name(name)?;
    if !array.is_valid(row) {
        return None;
    }
    match ScalarValue::try_from_array(array, row).ok()? {
        ScalarValue::TimestampNanosecond(v, _) => v,
        ScalarValue::TimestampMicrosecond(v, _) => v.map(|v| v.saturating_mul(1_000)),
        ScalarValue::TimestampMillisecond(v, _) => v.map(|v| v.saturating_mul(1_000_000)),
        ScalarValue::TimestampSecond(v, _) => v.map(|v| v.saturating_mul(1_000_000_000)),
        _ => None,
    }
}

fn read_utf8(batch: &RecordBatch, name: &str, row: usize) -> Option<String> {
    let array = batch.column_by_name(name)?;
    if !array.is_valid(row) {
        return None;
    }
    array
        .as_any()
        .downcast_ref::<StringArray>()
        .map(|a| a.value(row).to_string())
        .or_else(|| {
            ScalarValue::try_from_array(array, row)
                .ok()
                .and_then(|s| match s {
                    ScalarValue::Utf8(v) | ScalarValue::LargeUtf8(v) => v,
                    _ => None,
                })
        })
}

#[expect(clippy::unwrap_used, reason = "test helpers")]
#[cfg(test)]
mod tests {
    use super::super::caching::CacheRefreshHelper;
    use super::super::retention::apply_retention_filters_once;
    use super::*;
    use crate::federated::FederatedTable;
    use arrow::datatypes::{Field, Schema, TimeUnit};

    fn http_cache_schema() -> Schema {
        Schema::new(vec![
            Field::new("request_path", DataType::Utf8, false),
            Field::new("request_query", DataType::Utf8, true),
            Field::new("request_body", DataType::Utf8, true),
            Field::new("content", DataType::Utf8, false),
            Field::new(
                CACHE_REFRESHED_AT_COLUMN,
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
            Field::new(CACHE_NAMESPACE_COLUMN, DataType::Utf8, false),
        ])
    }

    #[test]
    fn entry_key_includes_the_namespace_so_one_principal_does_not_evict_another() {
        let columns = entry_key_columns(&http_cache_schema());
        assert_eq!(
            columns,
            vec![
                "request_path".to_string(),
                "request_query".to_string(),
                "request_body".to_string(),
                CACHE_NAMESPACE_COLUMN.to_string(),
            ]
        );
    }

    #[test]
    fn entry_key_is_empty_without_request_columns() {
        // A non-HTTP caching dataset has no request key, so eviction must fall
        // back rather than key on the namespace alone — which would evict every
        // entry a principal owns at once.
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(CACHE_NAMESPACE_COLUMN, DataType::Utf8, false),
        ]);
        assert!(entry_key_columns(&schema).is_empty());
    }

    fn cost(oldest: Option<i64>, newest: Option<i64>) -> EntryCost {
        EntryCost {
            key: vec![("request_path".to_string(), Some("/x".to_string()))],
            rows: 1,
            bytes: 1,
            oldest,
            newest,
            matches_configured: false,
        }
    }

    #[test]
    fn a_clean_separation_in_fetch_time_yields_one_range_predicate() {
        // The ordinary case: entries fetched over time do not overlap, so the
        // whole doomed set goes in one predicate rather than one per entry.
        let doomed = [cost(Some(10), Some(20)), cost(Some(5), Some(15))];
        let survivors = [cost(Some(30), Some(40)), cost(Some(50), Some(60))];
        let d: Vec<&EntryCost> = doomed.iter().collect();
        let s: Vec<&EntryCost> = survivors.iter().collect();
        assert_eq!(bulk_cutoff(&d, &s), Some(20));
    }

    #[test]
    fn an_overlapping_survivor_refuses_the_range_predicate() {
        // A survivor with a row older than the newest doomed row would be
        // caught by `_fetched_at <= cutoff`, so the range delete is not exact
        // and must not be used.
        let doomed = [cost(Some(10), Some(30))];
        let survivors = [cost(Some(20), Some(40))];
        let d: Vec<&EntryCost> = doomed.iter().collect();
        let s: Vec<&EntryCost> = survivors.iter().collect();
        assert_eq!(bulk_cutoff(&d, &s), None);
    }

    #[test]
    fn a_missing_fetch_time_on_either_side_refuses_the_range_predicate() {
        // Without a timestamp there is no boundary to reason about, and a row
        // that cannot be placed must not be swept up by a range.
        let d1 = [cost(Some(10), None)];
        let s1 = [cost(Some(30), Some(40))];
        assert_eq!(
            bulk_cutoff(
                &d1.iter().collect::<Vec<_>>(),
                &s1.iter().collect::<Vec<_>>()
            ),
            None
        );

        let d2 = [cost(Some(10), Some(20))];
        let s2 = [cost(None, Some(40))];
        assert_eq!(
            bulk_cutoff(
                &d2.iter().collect::<Vec<_>>(),
                &s2.iter().collect::<Vec<_>>()
            ),
            None
        );
    }

    #[test]
    fn evicting_everything_still_yields_a_range_predicate() {
        // No survivors means no boundary to violate.
        let doomed = [cost(Some(1), Some(2)), cost(Some(3), Some(4))];
        let d: Vec<&EntryCost> = doomed.iter().collect();
        assert_eq!(bulk_cutoff(&d, &[]), Some(4));
    }

    #[tokio::test]
    async fn a_budget_evicts_far_more_entries_than_one_predicate_could_name() {
        // The convergence property: naming entries individually is capped at
        // MAX_ENTRIES_PER_SWEEP, so a cache well over budget could never catch
        // up. With a clean fetch-time separation one sweep removes all of it.
        let over = MAX_ENTRIES_PER_SWEEP * 3;
        let rows: Vec<Row> = (0..over + 10)
            .map(|i| Row {
                path: Box::leak(format!("/e{i}").into_boxed_str()),
                query: None,
                // Distinct ages, newest last, so nothing overlaps.
                age: Duration::from_secs((over + 10 - i) as u64),
                content: "body",
            })
            .collect();
        let (accelerator, federated) = cache_table(&rows);

        let deleted = sweep(
            &accelerator,
            &federated,
            CacheLimits {
                max_items: Some(10),
                ..no_expiry()
            },
        )
        .await;

        assert!(
            deleted as usize > MAX_ENTRIES_PER_SWEEP,
            "one sweep must be able to evict past the per-predicate cap, or a \
             cache over budget never converges: deleted={deleted}"
        );
        assert_eq!(remaining(&accelerator).await.len(), 10);
    }

    /// Every predicate this module hands an accelerator must stay inside what a
    /// real delete translator accepts.
    ///
    /// `MemTable`, which the tests above delete through, evaluates arbitrary
    /// DataFusion expressions — so it accepted an `IS NULL` term that the DuckDB
    /// accelerator rejects outright with "Expression not supported". A measured
    /// run failed *every* sweep on that one term and evicted nothing at all,
    /// while the suite was green. The test double being more capable than the
    /// engine is what hid it, so this asserts on the predicate's shape rather
    /// than on what a permissive double does with it.
    #[test]
    fn no_predicate_this_builds_contains_is_null() {
        let schema = http_cache_schema();
        let key: Vec<(String, Option<String>)> = entry_key_columns(&schema)
            .into_iter()
            .map(|name| (name, Some(String::new())))
            .collect();

        let mut rendered = vec![
            entry_predicate(&key, None).expect("identity").to_string(),
            entry_predicate(&key, Some(1_700_000_000))
                .expect("ranked")
                .to_string(),
            row_level_expiry_expr(1_700_000_000).to_string(),
        ];
        if let Some(size) = payload_bytes_expr(&schema) {
            rendered.push(size.to_string());
        }

        for expr in rendered {
            assert!(
                !expr.to_uppercase().contains("IS NULL"),
                "an accelerator's delete translator need not accept IS NULL: {expr}"
            );
        }
    }

    #[test]
    fn entry_predicate_never_emits_is_null() {
        // A delete naming many entries is one statement, and an accelerator's
        // delete translator need not accept `IS NULL` — the DuckDB one does not,
        // and a single such term failed every sweep and evicted nothing at all.
        // An entry with a NULL key component is therefore skipped rather than
        // matched, which costs that entry's eviction instead of everyone's.
        let with_null = vec![
            ("request_path".to_string(), Some("/users".to_string())),
            ("request_query".to_string(), None),
        ];
        assert!(entry_predicate(&with_null, None).is_none());

        // The shape the HTTP connector actually stores: absent query and body
        // are empty strings, so they match by equality.
        let empty = vec![
            ("request_path".to_string(), Some("/users".to_string())),
            ("request_query".to_string(), Some(String::new())),
        ];
        let rendered = entry_predicate(&empty, Some(10)).unwrap().to_string();
        assert!(
            !rendered.contains("IS NULL"),
            "no predicate this builds may contain IS NULL: {rendered}"
        );
    }

    #[test]
    fn entry_predicate_of_an_empty_key_is_none() {
        // Guards the delete path: an unconstrained predicate would delete the
        // whole cache.
        assert!(entry_predicate(&[], None).is_none());
    }

    #[test]
    fn expiry_alone_is_worth_sweeping_for() {
        // Default (`stale_if_error` disabled, no budgets) still expires rows,
        // which is the retention the caching accelerator has always needed.
        assert!(CacheLimits::default().is_enforced());
    }

    #[test]
    fn stale_if_error_without_a_budget_is_the_unbounded_case() {
        // Regression guard for #13525: `stale_if_error` keeps expired entries
        // servable, so with no budget nothing can ever remove one.
        let limits = CacheLimits {
            stale_if_error: true,
            ..Default::default()
        };
        assert!(
            !limits.is_enforced(),
            "nothing would ever evict from this cache"
        );

        // A budget makes it bounded again — and enforced.
        for bounded in [
            CacheLimits {
                stale_if_error: true,
                max_items: Some(10),
                ..Default::default()
            },
            CacheLimits {
                stale_if_error: true,
                max_size_bytes: Some(1024),
                ..Default::default()
            },
        ] {
            assert!(bounded.is_enforced());
        }
    }

    #[test]
    fn sweep_interval_is_clamped_at_both_ends() {
        // A one-second TTL must not schedule a sweep every second...
        assert_eq!(
            sweep_interval(Some(Duration::from_secs(1))),
            MIN_SWEEP_INTERVAL
        );
        // ...and a year-long one must not schedule effectively one sweep ever,
        // which is what deriving the interval straight from the TTL did.
        assert_eq!(
            sweep_interval(Some(Duration::from_hours(365 * 24))),
            MAX_SWEEP_INTERVAL
        );
        assert_eq!(
            sweep_interval(Some(Duration::from_secs(90))),
            Duration::from_secs(90)
        );
        assert_eq!(sweep_interval(None), MIN_SWEEP_INTERVAL);
    }

    /// Entries as `rank_entries` hands them over: most-recently-fetched first.
    fn ranked(costs: &[(u64, u64)]) -> Vec<EntryCost> {
        costs
            .iter()
            .enumerate()
            .map(|(i, (rows, bytes))| EntryCost {
                key: vec![("request_path".to_string(), Some(format!("/{i}")))],
                rows: *rows,
                bytes: *bytes,
                // Newest-first ordering is the caller's contract, so the value
                // only has to be non-null; the budget tests do not expire.
                oldest: Some(i64::MAX),
                newest: Some(i64::MAX),
                matches_configured: false,
            })
            .collect()
    }

    fn doomed_paths(entries: &[EntryCost], budget: Budget) -> Vec<String> {
        let refs: Vec<&EntryCost> = entries.iter().collect();
        let keep = select_doomed_refs(&refs, budget);
        refs[keep..]
            .iter()
            .filter_map(|e| e.key.first().and_then(|(_, v)| v.clone()))
            .collect()
    }

    #[test]
    fn selection_keeps_a_contiguous_most_recent_prefix() {
        // Entries are newest-first, so /0 is newest. A budget of 2 rows keeps
        // /0 and /1 and dooms the rest — not whichever happen to fit.
        let entries = ranked(&[(1, 10), (1, 10), (1, 10), (1, 10)]);
        let doomed = doomed_paths(&entries, Budget::Items(2));
        assert_eq!(doomed, vec!["/2".to_string(), "/3".to_string()]);
    }

    #[test]
    fn selection_charges_bytes_against_a_byte_budget() {
        let entries = ranked(&[(1, 100), (1, 100), (1, 100)]);
        let doomed = doomed_paths(&entries, Budget::Bytes(250));
        assert_eq!(doomed, vec!["/2".to_string()]);
    }

    #[test]
    fn selection_evicts_nothing_when_everything_fits() {
        let entries = ranked(&[(1, 10), (1, 10)]);
        let refs: Vec<&EntryCost> = entries.iter().collect();
        assert_eq!(select_doomed_refs(&refs, Budget::Items(10)), refs.len());
        assert_eq!(select_doomed_refs(&refs, Budget::Bytes(1024)), refs.len());
    }

    #[test]
    fn a_single_entry_larger_than_the_whole_budget_is_evicted() {
        // Otherwise one oversized response would pin the cache over its budget
        // for good.
        let entries = ranked(&[(1, 5_000)]);
        let doomed = doomed_paths(&entries, Budget::Bytes(1_000));
        assert_eq!(doomed, vec!["/0".to_string()]);
    }

    #[test]
    fn selection_dooms_every_over_budget_entry_not_only_what_one_delete_can_name() {
        // The cap belongs to how the delete is written, not to what is over
        // budget. Capping here left over-budget entries among the survivors,
        // where they overlapped the doomed in fetch time and forced the
        // per-entry path forever — so a large cache could never converge.
        let count = MAX_ENTRIES_PER_SWEEP + 10;
        let entries = ranked(&vec![(1_u64, 10_u64); count]);
        let doomed = doomed_paths(&entries, Budget::Items(0));
        assert_eq!(doomed.len(), count, "every over-budget entry is doomed");
    }

    #[test]
    fn a_capped_delete_names_the_oldest_entries_first() {
        // When entries cannot be separated by fetch time the delete names them
        // individually, and the cap must trim the *newest* of the doomed:
        // trimming the other end would keep the least valuable entries and
        // re-doom the same ones every sweep.
        let count = MAX_ENTRIES_PER_SWEEP + 10;
        let entries = ranked(&vec![(1_u64, 10_u64); count]);
        let refs: Vec<&EntryCost> = entries.iter().collect();

        let (naming, deferred) = nameable(&refs);
        assert_eq!(naming.len(), MAX_ENTRIES_PER_SWEEP);
        assert_eq!(deferred, 10);
        let path = |e: &EntryCost| e.key[0].1.clone().unwrap_or_default();
        assert_eq!(path(naming[naming.len() - 1]), format!("/{}", count - 1));
        assert_eq!(
            path(naming[0]),
            format!("/{}", count - MAX_ENTRIES_PER_SWEEP)
        );
    }

    /// One stored row, in the shape a caching HTTP accelerator holds.
    #[derive(Clone, Copy)]
    struct Row {
        path: &'static str,
        query: Option<&'static str>,
        /// How long ago this row was fetched.
        age: Duration,
        /// Payload. Its byte length is what the size budget charges for.
        content: &'static str,
    }

    /// Payload bytes the size expression charges a fixture row, so the budget
    /// tests can state a budget in entries rather than in magic numbers:
    /// `request_path` + `content` (`request_query`/`request_body` are NULL and
    /// cost nothing) + 8 for the `_fetched_at` timestamp.
    fn row_bytes(row: &Row) -> u64 {
        (row.path.len() + row.content.len() + 8) as u64
    }

    fn row(path: &'static str, age: Duration) -> Row {
        Row {
            path,
            query: None,
            age,
            content: "body",
        }
    }

    /// Builds an in-memory accelerator holding `rows`. `MemTable` implements
    /// `delete_from`, so the sweep exercises the same delete path a `DuckDB` or
    /// Cayenne accelerator takes.
    fn cache_table(rows: &[Row]) -> (Arc<dyn TableProvider>, Arc<FederatedTable>) {
        use arrow::array::{StringArray, TimestampNanosecondArray};
        use data_components::arrow::write::MemTable;

        let schema = Arc::new(http_cache_schema());
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(
                    rows.iter().map(|r| r.path).collect::<Vec<_>>(),
                )) as _,
                Arc::new(StringArray::from(
                    rows.iter()
                        .map(|r| r.query.unwrap_or(""))
                        .collect::<Vec<_>>(),
                )) as _,
                Arc::new(StringArray::from(vec![""; rows.len()])) as _,
                Arc::new(StringArray::from(
                    rows.iter().map(|r| r.content).collect::<Vec<_>>(),
                )) as _,
                Arc::new(TimestampNanosecondArray::from(
                    rows.iter()
                        .map(|r| Some(nanos_ago(r.age)))
                        .collect::<Vec<_>>(),
                )) as _,
                Arc::new(StringArray::from(vec!["public"; rows.len()])) as _,
            ],
        )
        .expect("batch");

        let table = MemTable::try_new(schema, vec![vec![batch]]).expect("mem table");
        let accelerator = Arc::new(table) as Arc<dyn TableProvider>;
        let federated = Arc::new(FederatedTable::new_unchecked(Arc::clone(&accelerator)));
        (accelerator, federated)
    }

    /// The `(request_path, request_query)` pairs still stored, sorted.
    async fn remaining(accelerator: &Arc<dyn TableProvider>) -> Vec<(String, Option<String>)> {
        let ctx = SessionContext::new();
        let batches = ctx
            .read_table(Arc::clone(accelerator))
            .expect("read")
            .collect()
            .await
            .expect("collect");

        let mut out = Vec::new();
        for batch in &batches {
            for r in 0..batch.num_rows() {
                out.push((
                    read_utf8(batch, "request_path", r).unwrap_or_default(),
                    // The connector stores an absent query as an empty string;
                    // report it as absent so assertions read as intent.
                    read_utf8(batch, "request_query", r).filter(|q| !q.is_empty()),
                ));
            }
        }
        out.sort();
        out
    }

    /// Runs one tick the way `start_retention_check` does: resolve the
    /// predicate, then delete whatever it named. Returns the rows removed.
    async fn sweep(
        accelerator: &Arc<dyn TableProvider>,
        federated: &Arc<FederatedTable>,
        limits: CacheLimits,
    ) -> u64 {
        sweep_with_configured(accelerator, federated, limits, None).await
    }

    /// As [`sweep`], with a dataset-configured retention predicate in play.
    async fn sweep_with_configured(
        accelerator: &Arc<dyn TableProvider>,
        federated: &Arc<FederatedTable>,
        limits: CacheLimits,
        configured: Option<Expr>,
    ) -> u64 {
        let dataset_name = TableReference::bare("http_cache");
        let predicate =
            CacheEvictionPredicate::new(dataset_name.clone(), limits, Handle::current());
        let Some(expr) = predicate
            .delete_expr(accelerator, configured)
            .await
            .expect("resolve")
        else {
            return 0;
        };
        apply_retention_filters_once(
            &dataset_name,
            accelerator,
            federated,
            expr,
            &Handle::current(),
        )
        .await
        .expect("delete")
    }

    fn nanos_ago(age: Duration) -> i64 {
        let now = nanos_since_epoch(SystemTime::now()).expect("clock");
        now - i64::try_from(age.as_nanos()).expect("age")
    }

    /// A TTL long enough that no fixture row expires during a budget test.
    fn no_expiry() -> CacheLimits {
        CacheLimits {
            ttl: Some(Duration::from_hours(1)),
            ..Default::default()
        }
    }

    /// One row for `path`, fetched just now — what a refresh writes back.
    fn refreshed_row(path: &str) -> RecordBatch {
        use arrow::array::{StringArray, TimestampNanosecondArray};
        RecordBatch::try_new(
            Arc::new(http_cache_schema()),
            vec![
                Arc::new(StringArray::from(vec![path])) as _,
                Arc::new(StringArray::from(vec![""])) as _,
                Arc::new(StringArray::from(vec![""])) as _,
                Arc::new(StringArray::from(vec!["refreshed"])) as _,
                Arc::new(TimestampNanosecondArray::from(vec![Some(nanos_ago(
                    Duration::ZERO,
                ))])) as _,
                Arc::new(StringArray::from(vec!["public"])) as _,
            ],
        )
        .expect("batch")
    }

    #[tokio::test]
    async fn an_entry_refreshed_after_ranking_is_not_evicted() {
        // The sweep ranks without the write lock, so a refresh can replace an
        // entry between the ranking that chose it and the delete that removes
        // it. Deleting by key alone would take the refreshed rows too, turning a
        // response that was just repaired into a cache miss — exactly what
        // `stale_if_error` exists to prevent during an origin outage.
        let (accelerator, federated) = cache_table(&[
            row("/oldest", Duration::from_mins(3)),
            row("/newest", Duration::from_mins(1)),
        ]);

        // Rank the two entries as the sweep would.
        let schema = accelerator.schema();
        let key_columns = entry_key_columns(&schema);
        let dataset_name = TableReference::bare("http_cache");
        let entries = rank_entries(
            &accelerator,
            &Handle::current(),
            &key_columns,
            &schema,
            None,
            None,
        )
        .await
        .expect("rank");
        let doomed: Vec<&EntryCost> = entries
            .iter()
            .filter(|e| e.key.iter().any(|(_, v)| v.as_deref() == Some("/oldest")))
            .collect();
        assert_eq!(doomed.len(), 1, "one entry should be chosen");

        // A refresh lands before the delete: the entry is replaced with a row
        // fetched just now, which is newer than anything the ranking saw.
        CacheRefreshHelper::batched_upsert_into_accelerator(
            &accelerator,
            "http_cache",
            &[vec![col("request_path").eq(lit("/oldest"))]],
            vec![refreshed_row("/oldest")],
        )
        .await
        .expect("refresh");

        // Delete exactly what the ranking chose, as the retention loop would.
        let expr = combine_exprs_balanced(
            doomed
                .iter()
                .filter_map(|entry| entry_predicate(&entry.key, entry.newest))
                .collect(),
            Expr::or,
        )
        .expect("predicate");
        let deleted = apply_retention_filters_once(
            &dataset_name,
            &accelerator,
            &federated,
            expr,
            &Handle::current(),
        )
        .await
        .expect("delete");

        assert_eq!(deleted, 0, "the refreshed entry must not be deleted");
        assert_eq!(
            remaining(&accelerator).await,
            vec![("/newest".to_string(), None), ("/oldest".to_string(), None)],
            "a refresh that landed after the ranking must survive the sweep"
        );
    }

    #[tokio::test]
    async fn rows_past_ttl_plus_swr_are_deleted_and_live_ones_kept() {
        let (accelerator, federated) = cache_table(&[
            row("/live", Duration::from_secs(1)),
            row("/dead", Duration::from_mins(30)),
        ]);

        let deleted = sweep(
            &accelerator,
            &federated,
            CacheLimits {
                ttl: Some(Duration::from_mins(5)),
                ..Default::default()
            },
        )
        .await;

        assert_eq!(deleted, 1);
        assert_eq!(
            remaining(&accelerator).await,
            vec![("/live".to_string(), None)]
        );
    }

    #[tokio::test]
    async fn a_multi_page_entry_is_expired_whole_or_not_at_all() {
        // A paginated response is one cache entry whose pages were fetched
        // separately, so its rows carry different `_fetched_at` values. Expiring
        // by a timestamp range would delete only the pages past the cutoff and
        // leave the rest — and the read path, which judges freshness over the
        // rows that remain, would then find that remainder fresh and serve it as
        // if it were the whole response.
        let (accelerator, federated) = cache_table(&[
            Row {
                path: "/paged",
                query: Some("page=1"),
                age: Duration::from_mins(30),
                content: "page one",
            },
            Row {
                path: "/paged",
                query: Some("page=1"),
                age: Duration::from_secs(1),
                content: "page two",
            },
            row("/fresh", Duration::from_secs(1)),
        ]);

        let deleted = sweep(
            &accelerator,
            &federated,
            CacheLimits {
                ttl: Some(Duration::from_mins(5)),
                ..Default::default()
            },
        )
        .await;

        // The entry is only as fresh as its oldest page, so the whole entry goes.
        assert_eq!(deleted, 2);
        assert_eq!(
            remaining(&accelerator).await,
            vec![("/fresh".to_string(), None)],
            "a multi-page entry must expire whole, never leaving a servable remnant"
        );
    }

    #[tokio::test]
    async fn an_entry_is_kept_while_its_oldest_page_is_still_within_the_window() {
        // The mirror of the test above: judging by the *newest* page instead
        // would keep an entry a query can no longer be served in full.
        let (accelerator, federated) = cache_table(&[Row {
            path: "/paged",
            query: Some("page=1"),
            age: Duration::from_mins(2),
            content: "page one",
        }]);

        let deleted = sweep(
            &accelerator,
            &federated,
            CacheLimits {
                ttl: Some(Duration::from_mins(5)),
                ..Default::default()
            },
        )
        .await;

        assert_eq!(deleted, 0);
        assert_eq!(remaining(&accelerator).await.len(), 1);
    }

    #[tokio::test]
    async fn the_stale_while_revalidate_window_holds_a_row_past_its_ttl() {
        // A row inside its SWR window is still servable, so expiring it would
        // delete data a query is entitled to.
        let (accelerator, federated) = cache_table(&[row("/swr", Duration::from_mins(10))]);

        let deleted = sweep(
            &accelerator,
            &federated,
            CacheLimits {
                ttl: Some(Duration::from_mins(5)),
                stale_while_revalidate: Some(Duration::from_hours(1)),
                ..Default::default()
            },
        )
        .await;

        assert_eq!(deleted, 0);
        assert_eq!(
            remaining(&accelerator).await,
            vec![("/swr".to_string(), None)]
        );
    }

    #[tokio::test]
    async fn an_unset_ttl_uses_the_same_default_the_read_path_does() {
        // The sweep must not evict rows the read path still calls fresh. Both
        // read `DEFAULT_CACHING_TTL`, so they cannot diverge; the ages here are
        // written out rather than derived from it, so this still measures the
        // behaviour instead of restating the constant.
        assert_eq!(
            effective_max_age(None),
            Duration::from_secs(30),
            "the read path's default; the fixture ages below are chosen against it"
        );
        let (accelerator, federated) = cache_table(&[
            row("/fresh", Duration::from_secs(5)),
            row("/stale", Duration::from_secs(90)),
        ]);

        let deleted = sweep(&accelerator, &federated, CacheLimits::default()).await;

        assert_eq!(deleted, 1);
        assert_eq!(
            remaining(&accelerator).await,
            vec![("/fresh".to_string(), None)]
        );
    }

    #[tokio::test]
    async fn the_item_budget_evicts_the_least_recently_fetched_entries() {
        let (accelerator, federated) = cache_table(&[
            row("/oldest", Duration::from_mins(3)),
            row("/middle", Duration::from_mins(2)),
            row("/newest", Duration::from_mins(1)),
        ]);

        let deleted = sweep(
            &accelerator,
            &federated,
            CacheLimits {
                max_items: Some(2),
                ..no_expiry()
            },
        )
        .await;

        assert_eq!(deleted, 1);
        assert_eq!(
            remaining(&accelerator).await,
            vec![("/middle".to_string(), None), ("/newest".to_string(), None)]
        );
    }

    #[tokio::test]
    async fn the_byte_budget_evicts_the_least_recently_fetched_entries() {
        let rows = [
            row("/oldest", Duration::from_mins(3)),
            row("/middle", Duration::from_mins(2)),
            row("/newest", Duration::from_mins(1)),
        ];
        // Every fixture row is the same size, so a budget of two rows' worth
        // must leave exactly the two most recent.
        let each = row_bytes(&rows[0]);
        let (accelerator, federated) = cache_table(&rows);

        let deleted = sweep(
            &accelerator,
            &federated,
            CacheLimits {
                max_size_bytes: Some(each * 2),
                ..no_expiry()
            },
        )
        .await;

        assert_eq!(deleted, 1);
        assert_eq!(
            remaining(&accelerator).await,
            vec![("/middle".to_string(), None), ("/newest".to_string(), None)]
        );
    }

    #[tokio::test]
    async fn the_byte_budget_charges_for_the_payload_each_entry_actually_holds() {
        // A budget measured in rows rather than bytes would evict the same
        // entry whatever it weighed. The big entry must be the one that goes,
        // even though it is not the oldest by much.
        let big = Row {
            path: "/big",
            query: None,
            age: Duration::from_mins(2),
            content: "x".repeat(400).leak(),
        };
        let small = Row {
            path: "/small",
            query: None,
            age: Duration::from_mins(1),
            content: "y",
        };
        let (accelerator, federated) = cache_table(&[big, small]);

        let budget = row_bytes(&big) + row_bytes(&small) - 1;
        let deleted = sweep(
            &accelerator,
            &federated,
            CacheLimits {
                max_size_bytes: Some(budget),
                ..no_expiry()
            },
        )
        .await;

        assert_eq!(deleted, 1);
        assert_eq!(
            remaining(&accelerator).await,
            vec![("/small".to_string(), None)]
        );
    }

    #[test]
    fn a_nested_payload_column_is_reported_as_unweighable() {
        use arrow::datatypes::Fields;

        // `octet_length` takes text only, so a column of another shape is not
        // charged for. That is accurate for what a caching accelerator stores
        // today — an HTTP body is text, and a decomposed body decomposes into
        // text columns — but if one ever is not, the budget under-counts and
        // the operator has to be told rather than left to discover it as
        // unexplained growth.
        let schema = Schema::new(vec![
            Field::new("request_path", DataType::Utf8, false),
            Field::new("content", DataType::Utf8, false),
            Field::new(
                "decoded_body",
                DataType::Struct(Fields::from(vec![Field::new("k", DataType::Utf8, true)])),
                true,
            ),
        ]);

        assert_eq!(
            unmeasurable_payload_columns(&schema),
            vec!["decoded_body".to_string()]
        );
    }

    #[test]
    fn the_header_map_every_response_carries_is_not_reported() {
        // It is a `Map` on every HTTP response and small beside the body, so
        // reporting it would put a warning in the log of every ordinary caching
        // dataset with a budget — which is how a warning stops being read.
        let schema = Schema::new(vec![
            Field::new("content", DataType::Utf8, false),
            Field::new(
                "response_headers",
                DataType::Map(
                    Arc::new(Field::new_struct(
                        "entries",
                        vec![
                            Arc::new(Field::new("keys", DataType::Utf8, false)),
                            Arc::new(Field::new("values", DataType::Utf8, true)),
                        ],
                        false,
                    )),
                    false,
                ),
                true,
            ),
        ]);

        assert!(unmeasurable_payload_columns(&schema).is_empty());
    }

    #[test]
    fn the_caching_accelerators_own_columns_are_not_charged_to_the_budget() {
        // The budget bounds what was cached, not the bookkeeping around it.
        let schema = http_cache_schema();
        let expr = payload_bytes_expr(&schema).expect("measurable").to_string();
        assert!(
            !expr.contains(CACHE_NAMESPACE_COLUMN),
            "reserved columns must not be counted: {expr}"
        );
        assert!(
            expr.contains("content"),
            "the payload must be counted: {expr}"
        );
    }

    #[tokio::test]
    async fn a_multi_page_entry_is_evicted_whole_or_not_at_all() {
        // A paginated response is one cache entry whose pages were fetched
        // separately, so its rows carry different `_fetched_at` values. Trimming
        // by a timestamp boundary would cut it in half and leave the remainder
        // to be served as if it were the whole response — which is why eviction
        // names entries by key rather than deleting a time range.
        let (accelerator, federated) = cache_table(&[
            Row {
                path: "/paged",
                query: Some("page=1"),
                age: Duration::from_mins(3),
                content: "body",
            },
            Row {
                path: "/paged",
                query: Some("page=1"),
                age: Duration::from_secs(30),
                content: "body",
            },
            row("/other", Duration::from_mins(2)),
        ]);

        // Room for two rows only. `/paged` straddles `/other` in fetch time, so
        // any timestamp-range trim would split it.
        let deleted = sweep(
            &accelerator,
            &federated,
            CacheLimits {
                max_items: Some(2),
                ..no_expiry()
            },
        )
        .await;

        assert!(
            deleted > 0,
            "three rows against a budget of two must evict something"
        );

        let left = remaining(&accelerator).await;
        let paged_rows = left.iter().filter(|(p, _)| p == "/paged").count();
        assert!(
            paged_rows == 0 || paged_rows == 2,
            "a multi-page entry must survive or go whole, never in part: {left:?}"
        );
    }

    #[tokio::test]
    async fn stale_if_error_keeps_expired_entries_but_a_budget_still_evicts() {
        // `stale_if_error` means an expired entry is still wanted, so it is not
        // expired away — but a budget bounds the cache regardless, which is what
        // was missing.
        let rows = [
            row("/oldest", Duration::from_mins(30)),
            row("/newest", Duration::from_mins(20)),
        ];
        let (accelerator, federated) = cache_table(&rows);
        let expired = CacheLimits {
            ttl: Some(Duration::from_mins(5)),
            stale_if_error: true,
            ..Default::default()
        };

        let kept = sweep(&accelerator, &federated, expired).await;
        assert_eq!(kept, 0, "expired entries are the fallback");
        assert_eq!(remaining(&accelerator).await.len(), 2);

        let bounded = sweep(
            &accelerator,
            &federated,
            CacheLimits {
                max_items: Some(1),
                ..expired
            },
        )
        .await;
        assert_eq!(bounded, 1, "the budget still evicts one entry");
        assert_eq!(
            remaining(&accelerator).await,
            vec![("/newest".to_string(), None)]
        );
    }

    #[tokio::test]
    async fn a_dataset_retention_rule_takes_whole_entries_not_matching_rows() {
        // A `retention_period`/`retention_sql` the user sets still applies in
        // caching mode, but per entry. Matching one page of a paginated response
        // must take the whole response: leaving the other pages behind would let
        // the read path serve them as though they were all there was.
        let (accelerator, federated) = cache_table(&[
            Row {
                path: "/paged",
                query: Some("page=1"),
                age: Duration::from_secs(1),
                content: "drop-me",
            },
            Row {
                path: "/paged",
                query: Some("page=1"),
                age: Duration::from_secs(1),
                content: "keep-me",
            },
            row("/other", Duration::from_secs(1)),
        ]);

        // Matches exactly one row of the two-row entry.
        let configured = col("content").eq(lit("drop-me"));
        let deleted =
            sweep_with_configured(&accelerator, &federated, no_expiry(), Some(configured)).await;

        assert_eq!(deleted, 2, "both rows of the matched entry must go");
        assert_eq!(
            remaining(&accelerator).await,
            vec![("/other".to_string(), None)],
            "the unmatched entry stays; the matched one goes whole"
        );
    }

    #[tokio::test]
    async fn a_dataset_retention_rule_that_matches_nothing_deletes_nothing() {
        let (accelerator, federated) = cache_table(&[row("/a", Duration::from_secs(1))]);

        let deleted = sweep_with_configured(
            &accelerator,
            &federated,
            no_expiry(),
            Some(col("content").eq(lit("no-such-content"))),
        )
        .await;

        assert_eq!(deleted, 0);
        assert_eq!(remaining(&accelerator).await.len(), 1);
    }

    #[tokio::test]
    async fn a_dataset_retention_rule_applies_alongside_expiry_and_budgets() {
        // All three policies resolve into one predicate, so an entry the user's
        // rule removes is not also charged against the budget.
        let (accelerator, federated) = cache_table(&[
            row("/newest", Duration::from_secs(1)),
            Row {
                path: "/tagged",
                query: None,
                age: Duration::from_secs(2),
                content: "drop-me",
            },
            row("/expired", Duration::from_mins(30)),
        ]);

        let deleted = sweep_with_configured(
            &accelerator,
            &federated,
            CacheLimits {
                ttl: Some(Duration::from_mins(5)),
                max_items: Some(2),
                ..Default::default()
            },
            Some(col("content").eq(lit("drop-me"))),
        )
        .await;

        // `/expired` goes on expiry and `/tagged` on the user's rule; that
        // leaves one entry, which is inside the budget of two — so the budget
        // evicts nothing on top.
        assert_eq!(deleted, 2);
        assert_eq!(
            remaining(&accelerator).await,
            vec![("/newest".to_string(), None)]
        );
    }

    #[tokio::test]
    async fn a_cache_within_its_budgets_is_left_alone() {
        let rows = [
            row("/a", Duration::from_secs(1)),
            row("/b", Duration::from_secs(2)),
        ];
        let total: u64 = rows.iter().map(row_bytes).sum();
        let (accelerator, federated) = cache_table(&rows);

        let deleted = sweep(
            &accelerator,
            &federated,
            CacheLimits {
                max_items: Some(10),
                max_size_bytes: Some(total * 2),
                ..no_expiry()
            },
        )
        .await;

        assert_eq!(deleted, 0);
        assert_eq!(remaining(&accelerator).await.len(), 2);
    }

    #[tokio::test]
    async fn expiry_works_where_the_engine_stores_microseconds() {
        // Cayenne stores timestamps at microsecond precision, so the stored
        // fetch-time column is not the nanosecond type the predicate's literal
        // carries. The comparison must still select the right rows.
        use arrow::array::{StringArray, TimestampMicrosecondArray};
        use data_components::arrow::write::MemTable;

        let schema = Arc::new(Schema::new(vec![
            Field::new("request_path", DataType::Utf8, false),
            Field::new("request_query", DataType::Utf8, true),
            Field::new("request_body", DataType::Utf8, true),
            Field::new("content", DataType::Utf8, false),
            Field::new(
                CACHE_REFRESHED_AT_COLUMN,
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
            Field::new(CACHE_NAMESPACE_COLUMN, DataType::Utf8, false),
        ]));

        let live_us = nanos_ago(Duration::from_secs(1)) / 1_000;
        let dead_us = nanos_ago(Duration::from_mins(30)) / 1_000;

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["/live", "/dead"])) as _,
                Arc::new(StringArray::from(vec!["", ""])) as _,
                Arc::new(StringArray::from(vec!["", ""])) as _,
                Arc::new(StringArray::from(vec!["a", "b"])) as _,
                Arc::new(TimestampMicrosecondArray::from(vec![
                    Some(live_us),
                    Some(dead_us),
                ])) as _,
                Arc::new(StringArray::from(vec!["public", "public"])) as _,
            ],
        )
        .expect("batch");

        let table = MemTable::try_new(schema, vec![vec![batch]]).expect("mem table");
        let accelerator = Arc::new(table) as Arc<dyn TableProvider>;
        let federated = Arc::new(FederatedTable::new_unchecked(Arc::clone(&accelerator)));

        let deleted = sweep(
            &accelerator,
            &federated,
            CacheLimits {
                ttl: Some(Duration::from_mins(5)),
                ..Default::default()
            },
        )
        .await;

        assert_eq!(deleted, 1);
        assert_eq!(
            remaining(&accelerator).await,
            vec![("/live".to_string(), None)]
        );
    }
}
