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

use std::any::Any;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::AtomicI64;
use std::time::{Duration, SystemTime};

use arrow::array::StringArray;
use arrow::array::{Array, ArrayRef, RecordBatch, TimestampNanosecondArray, UInt16Array};
use arrow::compute::{cast, filter_record_batch};
use arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use arrow_tools::format::SchemaDisplay;
use datafusion::common::{DataFusionError, Result as DataFusionResult};
use datafusion::datasource::TableProvider;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Expr, dml::InsertOp, not};
use datafusion::logical_expr::{col, lit};
use datafusion::physical_plan::execution_plan::EmissionType;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, SendableRecordBatchStream,
    stream::RecordBatchStreamAdapter,
};
use datafusion::physical_plan::{Distribution, Partitioning, PlanProperties};
use datafusion::prelude::SessionContext;
use datafusion::scalar::ScalarValue;
use datafusion_expr::expr::ExprListDisplay;
use futures::{StreamExt, TryStreamExt};
use std::collections::HashSet;
use tokio::runtime::Handle;
use tokio::sync::{Mutex, RwLock, mpsc};
use tokio::task::JoinHandle;

use crate::dataupdate::StreamingDataUpdateExecutionPlan;
use runtime_datafusion::execution_plan::schema_cast::SchemaCastScanExec;
use util::expr::combine_exprs_balanced;

/// Type alias for tracking in-flight revalidation requests.
/// The key is a cache key derived from the filter expressions (`request_path`, `request_query`, `request_body`).
/// When a revalidation is in progress for a cache key, other requests for the same key will skip
/// triggering a new revalidation to avoid duplicate upstream requests during the SWR window.
pub type InFlightRevalidations = Arc<Mutex<HashSet<String>>>;

pub const CACHE_REFRESHED_AT_COLUMN: &str = "fetched_at";

/// Maximum number of concurrent refresh requests
const MAX_CONCURRENT_REFRESHES: usize = 10;

/// Channel capacity for batched cache writes. Allows buffering many concurrent requests.
/// This value controls how many cache write requests can be buffered before
/// backpressure is applied to producers.
const CACHE_WRITE_CHANNEL_CAPACITY: usize = 8_192;
/// Flush interval for batched cache writes. Writes are collected and flushed
/// periodically to reduce the overhead of individual write operations.
const CACHE_WRITE_FLUSH_INTERVAL_MS: u64 = 500;

/// Represents a cache write request for batched processing.
///
/// Writes are collected and batched to reduce the O(n²) overhead of the
/// read-combine-overwrite pattern in `DuckDB` accelerator.
#[derive(Debug)]
pub struct CacheWriteRequest {
    /// Batches to write to the accelerator
    pub batches: Vec<RecordBatch>,
    /// Filter expressions to identify the cache key (for upsert operations)
    pub filters: Vec<Expr>,
    /// If true, this is an upsert (expired data exists), otherwise insert (new data)
    pub is_upsert: bool,
    /// Cache key computed from filters, used to track in-flight writes
    pub cache_key: String,
}

/// Sender half of the cache write channel
pub type CacheWriteSender = mpsc::Sender<CacheWriteRequest>;

/// Receiver half of the cache write channel
pub type CacheWriteReceiver = mpsc::Receiver<CacheWriteRequest>;

/// Creates a new cache write channel with the configured capacity.
///
/// Returns the sender (for `CachingAccelerationScanExec` to send writes) and
/// the receiver (for the consumer task to process batched writes).
#[must_use]
pub fn create_cache_write_channel() -> (CacheWriteSender, CacheWriteReceiver) {
    mpsc::channel(CACHE_WRITE_CHANNEL_CAPACITY)
}

/// Spawns a background task that batches cache writes on interval basis.
///
/// Removes cache keys from `in_flight_revalidations` after writes complete.
/// Updates `last_updated_at` after successful writes to support `snapshots_creation_policy: on_change`.
pub fn spawn_batched_cache_write_task(
    mut rx: CacheWriteReceiver,
    accelerator: Arc<dyn TableProvider>,
    dataset_name: String,
    accelerator_write_mutex: Arc<Mutex<()>>,
    in_flight_revalidations: InFlightRevalidations,
    last_updated_at: Arc<AtomicI64>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
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
) {
    if buffer.is_empty() {
        return;
    }

    let flush_start = std::time::Instant::now();

    let request_count = buffer.len();
    let total_rows: usize = buffer
        .iter()
        .flat_map(|r| r.batches.iter())
        .map(RecordBatch::num_rows)
        .sum();

    // Collect cache keys to remove after flushing
    let cache_keys: Vec<String> = buffer.iter().map(|r| r.cache_key.clone()).collect();

    tracing::trace!(
        "Flushing {request_count} cache write requests ({total_rows} total rows) for dataset={dataset_name}"
    );

    // Separate inserts from upserts
    let mut insert_batches: Vec<RecordBatch> = Vec::new();
    let mut upsert_batches: Vec<RecordBatch> = Vec::new();
    let mut upsert_filters: Vec<Vec<Expr>> = Vec::new();

    for req in buffer.drain(..) {
        if req.is_upsert {
            upsert_batches.extend(req.batches);
            upsert_filters.push(req.filters);
        } else {
            insert_batches.extend(req.batches);
        }
    }

    let insert_rows: usize = insert_batches.iter().map(RecordBatch::num_rows).sum();
    let upsert_rows: usize = upsert_batches.iter().map(RecordBatch::num_rows).sum();
    let upsert_count = upsert_filters.len();

    // Combine all batches for writing
    let mut all_batches = insert_batches;
    all_batches.extend(upsert_batches);

    let write_start = std::time::Instant::now();

    // Check if the accelerator has constraints configured (primary key, unique, etc.).
    // If it does, we can use native upsert (append_to_accelerator) which is more efficient
    // than the read-filter-write pattern (batched_upsert_into_accelerator).
    let has_constraints = accelerator.constraints().is_some_and(|c| !c.is_empty());

    // Acquire the mutex once for the entire batch
    let lock_wait_start = std::time::Instant::now();
    let lock_guard = accelerator_write_mutex.lock().await;
    let lock_wait_ms = lock_wait_start.elapsed().as_millis();

    let result = if all_batches.is_empty() {
        Ok(())
    } else if has_constraints {
        // Use native upsert via append - the accelerator's OnConflict::Upsert handles deduplication
        CacheRefreshHelper::append_to_accelerator(accelerator, dataset_name, all_batches).await
    } else if !upsert_filters.is_empty() {
        // No constraints - fall back to read-filter-write pattern for upserts
        CacheRefreshHelper::batched_upsert_into_accelerator(
            accelerator,
            dataset_name,
            &upsert_filters,
            all_batches,
        )
        .await
    } else {
        // No upserts needed - use insert path
        CacheRefreshHelper::insert_into_accelerator(accelerator, dataset_name, all_batches).await
    };

    drop(lock_guard);

    let write_ms = write_start.elapsed().as_millis();
    if let Err(e) = result {
        tracing::warn!("Failed to flush cache updates for dataset {dataset_name}: {e}");
    } else if insert_rows > 0 || upsert_rows > 0 {
        // Update last_updated_at for snapshots_creation_policy: on_change support
        super::AcceleratedTable::set_timestamp_to_now(last_updated_at);

        tracing::trace!(
            "Cache write completed for dataset={dataset_name}: inserts={insert_rows} rows, upserts={upsert_count}, {upsert_rows} rows in {write_ms}ms"
        );
    }

    // Remove cache keys from in-flight tracking now that writes are persisted
    {
        let mut in_flight = in_flight_revalidations.lock().await;
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

/// Compute a cache key from filter expressions.
/// The cache key is a string representation of the filter values for `request_path`, `request_query`, and `request_body`.
fn compute_cache_key_from_filters(filters: &[Expr]) -> String {
    // Sort and join filter expressions to create a consistent cache key
    let mut parts: Vec<String> = filters.iter().map(ToString::to_string).collect();
    parts.sort();
    parts.join("|")
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

const RESPONSE_STATUS_COLUMN: &str = "response_status";

/// Helper functions for cache refresh operations
pub struct CacheRefreshHelper;

impl CacheRefreshHelper {
    /// Refresh ALL stale rows in the cache by querying the accelerator for rows with old `fetched_at` timestamps,
    /// then re-executing the query on the federated source with the original filter parameters.
    /// This is specifically designed for HTTP connector caching mode and is used by the periodic refresh task.
    ///
    /// For single-entry refresh (e.g., SWR pattern), use `refresh_entry` instead.
    pub async fn refresh_all_stale_rows(
        federated: Arc<dyn TableProvider>,
        accelerator: Arc<dyn TableProvider>,
        dataset_name: &str,
        ttl: Duration,
        accelerator_write_mutex: Arc<Mutex<()>>,
    ) -> DataFusionResult<usize> {
        let ctx = SessionContext::new();
        let state = ctx.state();

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

        let plan = accelerator.scan(&state, None, &filters, None).await?;
        let task_ctx = Arc::new(TaskContext::default());

        // Collect all stale rows from accelerator
        let stale_batches = datafusion::physical_plan::collect(plan, task_ctx).await?;

        // Extract unique filter sets from stale rows
        let filter_sets = Self::extract_unique_filter_sets(&stale_batches)?;

        let total_stale_rows: usize = stale_batches.iter().map(RecordBatch::num_rows).sum();
        tracing::debug!(
            "Found {total_stale_rows} stale rows ({} unique filter sets) to refresh for dataset {dataset_name}",
            filter_sets.len()
        );

        if filter_sets.is_empty() {
            return Ok(0);
        }

        // Create futures for all refresh operations and run them with limited concurrency.
        // Each refresh fetches from the source and then upserts into the accelerator,
        // which preserves data for other cache entries (different request paths/queries).
        let refresh_futures = filter_sets.into_iter().map(|row_filters| {
            let federated = Arc::clone(&federated);
            let accelerator = Arc::clone(&accelerator);
            let dataset_name = dataset_name.to_string();
            let accelerator_write_mutex = Arc::clone(&accelerator_write_mutex);

            async move {
                tracing::debug!(
                    "Refreshing stale data for dataset {} with {} filters",
                    dataset_name,
                    row_filters.len()
                );

                let batches =
                    Self::fetch_from_source(&federated, &dataset_name, &row_filters, None).await?;

                if batches.is_empty() {
                    return Ok::<usize, datafusion::error::DataFusionError>(0);
                }

                let refreshed_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();

                // Acquire the mutex to protect accelerator operations
                let lock_guard = accelerator_write_mutex.lock().await;

                // Upsert this specific cache entry - removes rows matching the filters
                // and adds the new data, preserving other cache entries.
                Self::upsert_into_accelerator(&accelerator, &dataset_name, &row_filters, batches)
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
    /// Writes are queued through the batched write channel to reduce accelerator overhead.
    pub async fn refresh_entry(
        federated: Arc<dyn TableProvider>,
        dataset_name: &str,
        filters: &[Expr],
        batch_write_tx: CacheWriteSender,
        in_flight_revalidations: InFlightRevalidations,
    ) -> DataFusionResult<usize> {
        let cache_key = compute_cache_key_from_filters(filters);

        tracing::trace!(
            "Refreshing single cache entry for dataset {dataset_name} with {} filters",
            filters.len()
        );

        // Fetch fresh data for this specific entry
        let batches = Self::fetch_from_source(&federated, dataset_name, filters, None).await?;

        // Filter out 5xx responses - they are transient errors that should not be cached
        let batches = filter_5xx_responses(batches)?;

        if batches.is_empty() {
            tracing::debug!(
                "No cacheable data for dataset={dataset_name} (source returned empty or 5xx)"
            );
            // Remove from in-flight since no data to write
            let mut in_flight = in_flight_revalidations.lock().await;
            in_flight.remove(&cache_key);
            return Ok(0);
        }

        let refreshed_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();

        // Queue write through batched channel
        let request = CacheWriteRequest {
            batches,
            filters: filters.to_vec(),
            is_upsert: true,
            cache_key: cache_key.clone(),
        };

        batch_write_tx
            .send(request)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        tracing::trace!("Queued refresh for dataset={dataset_name}, {refreshed_rows} rows");

        Ok(refreshed_rows)
    }

    /// Extract filter expressions from a row containing `request_path`, `request_query`, `request_body`
    fn extract_filters_from_row(
        batch: &RecordBatch,
        row_idx: usize,
    ) -> DataFusionResult<Vec<Expr>> {
        let schema = batch.schema();
        let mut filters = Vec::new();

        let filter_columns = ["request_path", "request_query", "request_body"];

        for column_name in filter_columns {
            if let Some((idx, _)) = schema.column_with_name(column_name) {
                let array = batch
                    .column(idx)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        datafusion::error::DataFusionError::Execution(format!(
                            "{column_name} column is not a StringArray"
                        ))
                    })?;

                if !array.is_null(row_idx) {
                    let value = array.value(row_idx).to_string();
                    // Only add filter if value is non-empty (empty string means no filter)
                    if !value.is_empty() {
                        tracing::debug!("Extracted {column_name} filter: {value}");
                        filters.push(col(column_name).eq(lit(value)));
                    }
                }
            }
        }

        tracing::debug!(
            "Extracted {} total filters from row (including empty values)",
            filters.len()
        );
        Ok(filters)
    }

    /// Extract unique filter sets from batches, deduplicating rows with identical
    /// `(request_path, request_query, request_body)` values.
    ///
    /// This is needed because HTTP connector JSON array responses are stored as multiple rows
    /// with identical request parameters. Without deduplication, refreshing N rows from the
    /// same JSON array would trigger N identical HTTP requests.
    fn extract_unique_filter_sets(batches: &[RecordBatch]) -> DataFusionResult<Vec<Vec<Expr>>> {
        let mut seen_filter_keys = std::collections::HashSet::new();
        let mut filter_sets: Vec<Vec<Expr>> = Vec::new();

        for batch in batches {
            for row_idx in 0..batch.num_rows() {
                let row_filters = Self::extract_filters_from_row(batch, row_idx)?;
                let cache_key = compute_cache_key_from_filters(&row_filters);
                if seen_filter_keys.insert(cache_key) {
                    filter_sets.push(row_filters);
                }
            }
        }

        Ok(filter_sets)
    }

    /// Overwrite the data in the accelerator with the provided batches
    async fn overwrite_accelerator(
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

        let ctx = SessionContext::new();
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
        let task_ctx = Arc::new(TaskContext::default());
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

        let ctx = SessionContext::new();
        let state = ctx.state();
        let new_rows: usize = new_batches.iter().map(RecordBatch::num_rows).sum();

        tracing::debug!(
            "insert_into_accelerator - reading existing data from accelerator for dataset={}",
            dataset_name
        );

        // Scan all existing data from the accelerator
        let plan = accelerator.scan(&state, None, &[], None).await?;
        let task_ctx = Arc::new(TaskContext::default());
        let existing_batches = datafusion::physical_plan::collect(plan, task_ctx).await?;

        let existing_rows: usize = existing_batches.iter().map(RecordBatch::num_rows).sum();
        tracing::debug!(
            "insert_into_accelerator - found {} existing rows, adding {} new rows for dataset={}",
            existing_rows,
            new_rows,
            dataset_name
        );

        // Combine existing data with new data
        let mut combined_batches = existing_batches;
        combined_batches.extend(new_batches);

        // Overwrite the accelerator with the combined data
        Self::overwrite_accelerator(Arc::clone(accelerator), dataset_name, combined_batches).await
    }

    /// Upsert data into the accelerator by removing rows matching the filters and inserting new data.
    /// This is used when cached data exists but is expired.
    ///
    /// The process:
    /// 1. Scan all data from the accelerator
    /// 2. Filter out rows that match the provided filters (these are the expired rows to replace)
    /// 3. Combine remaining rows with new data
    /// 4. Overwrite the accelerator with the combined data
    async fn upsert_into_accelerator(
        accelerator: &Arc<dyn TableProvider>,
        dataset_name: &str,
        filters: &[Expr],
        new_batches: Vec<RecordBatch>,
    ) -> DataFusionResult<()> {
        if new_batches.is_empty() {
            tracing::debug!(
                "upsert_into_accelerator called with empty batches for dataset={dataset_name}"
            );
            return Ok(());
        }

        let ctx = SessionContext::new();
        let state = ctx.state();

        tracing::debug!(
            "upsert_into_accelerator - reading existing data from accelerator for dataset={}",
            dataset_name
        );

        // Scan all data from the accelerator (no filters to get everything)
        let plan = accelerator.scan(&state, None, &[], None).await?;
        let task_ctx = Arc::new(TaskContext::default());
        let existing_batches = datafusion::physical_plan::collect(plan, task_ctx).await?;

        let existing_rows: usize = existing_batches.iter().map(RecordBatch::num_rows).sum();
        tracing::debug!(
            "upsert_into_accelerator - found {} existing rows in accelerator for dataset={}",
            existing_rows,
            dataset_name
        );

        // If there's no existing data, just insert the new data
        if existing_batches.is_empty() || existing_rows == 0 {
            tracing::debug!(
                "upsert_into_accelerator - no existing data, performing simple insert for dataset={}",
                dataset_name
            );
            return Self::insert_into_accelerator(accelerator, dataset_name, new_batches).await;
        }

        // Build a filter to exclude rows that match the provided filters
        // We need to keep rows that DON'T match the filters
        let exclusion_filter = Self::build_combined_exclusion_filter(&[filters.to_vec()]);

        tracing::debug!(
            "upsert_into_accelerator - filtering out matching rows with {} filters for dataset={}",
            filters.len(),
            dataset_name
        );

        // Filter existing data to keep only non-matching rows
        let df = ctx.read_batches(existing_batches)?;
        let filtered_df = if let Some(filter) = exclusion_filter {
            df.filter(filter)?
        } else {
            // No filters means replace everything
            tracing::debug!(
                "upsert_into_accelerator - no filters provided, will replace all data for dataset={}",
                dataset_name
            );
            // Return early with just the new batches
            return Self::overwrite_accelerator(Arc::clone(accelerator), dataset_name, new_batches)
                .await;
        };

        let kept_batches = filtered_df.collect().await?;
        let kept_rows: usize = kept_batches.iter().map(RecordBatch::num_rows).sum();
        let new_rows: usize = new_batches.iter().map(RecordBatch::num_rows).sum();

        tracing::debug!(
            "upsert_into_accelerator - keeping {} rows, adding {} new rows for dataset={}",
            kept_rows,
            new_rows,
            dataset_name
        );

        // Combine kept rows with new rows
        let mut combined_batches = kept_batches;
        combined_batches.extend(new_batches);

        // Overwrite the accelerator with the combined data
        Self::overwrite_accelerator(Arc::clone(accelerator), dataset_name, combined_batches).await
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
    async fn append_to_accelerator(
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

        let ctx = SessionContext::new();
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

        let task_ctx = Arc::new(TaskContext::default());
        let _ = datafusion::physical_plan::collect(insert_plan, task_ctx).await?;

        tracing::debug!(
            "append_to_accelerator COMPLETED - successfully appended {total_rows} rows to accelerator for dataset={dataset_name}"
        );

        Ok(())
    }

    /// Batched upsert: replace multiple cache entries in a single read-filter-write operation.
    async fn batched_upsert_into_accelerator(
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

        let ctx = SessionContext::new();
        let state = ctx.state();

        tracing::trace!(
            "batched_upsert_into_accelerator - reading existing data from accelerator for dataset={dataset_name}, {} filter sets",
            filter_sets.len()
        );

        // Scan all data from the accelerator (no filters to get everything)
        let plan = accelerator.scan(&state, None, &[], None).await?;
        let task_ctx = Arc::new(TaskContext::default());
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

    /// Propagate cached data to synchronized child accelerators (for localpod caching).
    /// This is called after successfully storing data in the parent accelerator.
    async fn propagate_to_synchronized_children(
        synchronized_children: &SynchronizedChildren,
        dataset_name: &str,
        filters: &[Expr],
        batches: &[RecordBatch],
        is_expired: bool,
    ) {
        let children = synchronized_children.read().await;
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
            let result = if is_expired {
                Self::upsert_into_accelerator(child, dataset_name, filters, batches.to_vec()).await
            } else {
                Self::insert_into_accelerator(child, dataset_name, batches.to_vec()).await
            };

            if let Err(e) = result {
                tracing::warn!(
                    "Failed to propagate cached data to synchronized child {} for dataset {}: {}",
                    idx,
                    dataset_name,
                    e
                );
            } else {
                tracing::debug!(
                    "Successfully propagated cached data to synchronized child {} for dataset={}",
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
    /// * `child_accelerator` - The child's accelerator to initialize
    /// * `dataset_name` - Name of the dataset for logging
    ///
    /// # Returns
    /// Returns the number of rows copied, or an error if the operation fails.
    pub async fn initialize_child_from_parent(
        parent_accelerator: &Arc<dyn TableProvider>,
        child_accelerator: &Arc<dyn TableProvider>,
        dataset_name: &str,
    ) -> DataFusionResult<usize> {
        let ctx = SessionContext::new();
        let state = ctx.state();

        tracing::debug!(
            "Scanning parent accelerator for existing cached data to initialize child for dataset={}",
            dataset_name
        );

        // Scan all existing data from the parent accelerator
        let plan = parent_accelerator.scan(&state, None, &[], None).await?;
        let task_ctx = Arc::new(TaskContext::default());
        let batches = datafusion::physical_plan::collect(plan, task_ctx).await?;

        let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();

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
        Self::overwrite_accelerator(Arc::clone(child_accelerator), dataset_name, batches).await?;

        Ok(total_rows)
    }

    /// Fetch data from federated source for given filters
    async fn fetch_from_source(
        federated: &Arc<dyn TableProvider>,
        dataset_name: &str,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Vec<RecordBatch>> {
        tracing::debug!(
            "Fetching from source for dataset {dataset_name} with {} filters, limit={limit:?}",
            filters.len()
        );
        for (i, filter) in filters.iter().enumerate() {
            tracing::debug!("Source fetch filter {i}: {}", filter.human_display());
        }

        let ctx = SessionContext::new();
        let state = ctx.state();

        // Query source with same filters/limit but all columns
        tracing::debug!("About to scan federated source for dataset={dataset_name}");
        let plan = federated.scan(&state, None, filters, limit).await?;
        tracing::debug!(
            "Federated source SCAN successful for dataset={dataset_name}, plan has {} partitions",
            plan.properties().output_partitioning().partition_count()
        );
        let task_ctx = Arc::new(TaskContext::default());

        // Execute and collect all batches
        let all_batches = datafusion::physical_plan::collect(plan, task_ctx).await?;

        tracing::debug!(
            "Federated source returned {} batches for dataset={}",
            all_batches.len(),
            dataset_name
        );

        Ok(all_batches)
    }

    /// Handle a cache miss by fetching from source and returning a stream.
    /// Returns a `SendableRecordBatchStream` containing the fetched data, empty stream, or error stream.
    ///
    /// # Arguments
    /// * `is_expired` - If `true`, data exists in the cache but is expired, so we use upsert.
    ///   If `false`, no data exists in the cache, so we use insert (append).
    /// * `stale_if_error` - If `true` and `expired_batches` is provided, serve the expired cached data
    ///   when the upstream source returns an error instead of propagating the error.
    /// * `expired_batches` - The expired cached data to serve if `stale_if_error` is enabled and
    ///   the source returns an error.
    /// * `io_runtime` - Tokio runtime handle for spawning background write tasks.
    /// * `synchronized_children` - Child accelerators that should also receive the cached data.
    /// * `batch_write_tx` - Channel sender for batched writes to the caching consumer.
    #[expect(clippy::too_many_arguments)]
    async fn handle_cache_miss(
        federated: Arc<dyn TableProvider>,
        dataset_name: &str,
        filters: &[Expr],
        limit: Option<usize>,
        fallback_schema: SchemaRef,
        is_expired: bool,
        stale_if_error: bool,
        expired_batches: Option<Vec<RecordBatch>>,
        io_runtime: &Handle,
        synchronized_children: SynchronizedChildren,
        batch_write_tx: CacheWriteSender,
    ) -> SendableRecordBatchStream {
        match Self::fetch_from_source(&federated, dataset_name, filters, limit).await {
            Ok(batches) if !batches.is_empty() => {
                let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
                tracing::debug!(
                    "Fetched {} batches ({} total rows) from source for dataset {}",
                    batches.len(),
                    total_rows,
                    dataset_name
                );

                // Use the schema from the fetched batches
                let batch_schema = batches[0].schema();
                tracing::trace!("Fetched batch schema:\n{}", SchemaDisplay(&batch_schema));

                // Filter out 5xx responses for caching - they are transient errors
                // that should not be persisted. User still receives all data.
                let batches_for_cache = match filter_5xx_responses(batches.clone()) {
                    Ok(filtered) => filtered,
                    Err(e) => {
                        tracing::error!(
                            "Failed to filter 5xx responses for caching for dataset={dataset_name}: {e}"
                        );
                        Vec::new() // Skip caching on error, but still return data to user
                    }
                };

                // Clone batches for propagation to children.
                // RecordBatch::clone() is cheap - only clones Arc pointers, not the underlying data.
                let batches_for_propagate = batches_for_cache.clone();
                let filters_clone: Vec<Expr> = filters.to_vec();
                let cache_key = compute_cache_key_from_filters(filters);

                // Only cache if we have non-5xx data to cache
                if batches_for_cache.is_empty() {
                    tracing::debug!(
                        "Fetch returned 5xx response, skipping cache write for dataset={dataset_name}"
                    );
                } else {
                    let cache_rows: usize =
                        batches_for_cache.iter().map(RecordBatch::num_rows).sum();

                    // Send write request to batched consumer
                    let write_request = CacheWriteRequest {
                        batches: batches_for_cache,
                        filters: filters.to_vec(),
                        is_upsert: is_expired,
                        cache_key,
                    };
                    if let Err(e) = batch_write_tx.send(write_request).await {
                        tracing::warn!(
                            "Failed to enqueue cache write for dataset {dataset_name}: {e} (channel closed)"
                        );
                    } else {
                        tracing::trace!(
                            "Enqueued cache write for dataset={dataset_name}, {cache_rows} rows, is_upsert={is_expired}",
                        );
                    }

                    // Propagate filtered data to children (same as parent - excludes 5xx for consistency)
                    let synchronized_children_clone = Arc::clone(&synchronized_children);
                    let dataset_name_clone = dataset_name.to_string();
                    io_runtime.spawn(async move {
                        Self::propagate_to_synchronized_children(
                            &synchronized_children_clone,
                            &dataset_name_clone,
                            &filters_clone,
                            &batches_for_propagate,
                            is_expired,
                        )
                        .await;
                    });

                    tracing::debug!(
                        "Background cache update performed for dataset={dataset_name}, {cache_rows} rows"
                    );
                }

                // Return ALL data to user (including 5xx responses)
                let batch_stream = futures::stream::iter(batches.into_iter().map(Ok));
                let adapter = RecordBatchStreamAdapter::new(batch_schema, batch_stream);
                Box::pin(adapter)
            }
            Ok(_) => {
                // Source returned empty data (no error, just no rows)
                tracing::debug!(
                    "Cache miss - source also has no data for dataset {}",
                    dataset_name
                );
                let empty_stream =
                    RecordBatchStreamAdapter::new(fallback_schema, futures::stream::empty());
                Box::pin(empty_stream)
            }
            Err(e) => {
                // Check if we should serve stale (expired) data on error
                if stale_if_error
                    && let Some(batches) = expired_batches
                    && !batches.is_empty()
                {
                    tracing::warn!(
                        "Cache miss fetch failed for dataset {}, serving stale data due to stale_if_error: {}",
                        dataset_name,
                        e
                    );
                    let batch_schema = batches[0].schema();
                    let batch_stream = futures::stream::iter(batches.into_iter().map(Ok));
                    let adapter = RecordBatchStreamAdapter::new(batch_schema, batch_stream);
                    return Box::pin(adapter);
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

    /// Handle a cache hit by returning cached data and optionally triggering background refresh.
    /// Returns a `SendableRecordBatchStream` containing the cached data.
    ///
    /// Cache behavior based on freshness:
    /// - `Fresh`: Return cached data immediately, no refresh needed
    /// - `Stale`: Return cached data immediately, trigger background refresh (if not already in-flight)
    /// - `Expired`: This should not be called for expired data (handled as cache miss)
    #[expect(clippy::too_many_arguments)]
    async fn handle_cache_hit(
        cached_batches: Vec<RecordBatch>,
        federated: &Arc<dyn TableProvider>,
        dataset_name: &str,
        max_age: Option<Duration>,
        stale_while_revalidate: Option<Duration>,
        io_runtime: &Handle,
        schema: SchemaRef,
        filters: &[Expr],
        in_flight_revalidations: &InFlightRevalidations,
        batch_write_tx: CacheWriteSender,
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
                    // Compute cache key to check for in-flight revalidation
                    let cache_key = compute_cache_key_from_filters(filters);

                    // Try to acquire the revalidation slot for this cache key
                    // Use async lock since we're in an async context
                    let should_revalidate = {
                        let mut in_flight = in_flight_revalidations.lock().await;
                        if in_flight.contains(&cache_key) {
                            tracing::debug!(
                                "Skipping background refresh for dataset={dataset_name}, cache_key={cache_key} - revalidation already in progress"
                            );
                            false
                        } else {
                            in_flight.insert(cache_key.clone());
                            true
                        }
                    };

                    if should_revalidate {
                        tracing::debug!(
                            "Data is stale for dataset={dataset_name}, triggering background refresh"
                        );

                        // Log current fetched_at for debugging
                        if let Some(timestamp) = get_first_fetched_at_timestamp(&cached_batches[0])
                        {
                            tracing::debug!(
                                "Current stale data has {CACHE_REFRESHED_AT_COLUMN} timestamp={timestamp}"
                            );
                        }

                        let federated_clone = Arc::clone(federated);
                        let dataset_name_clone = dataset_name.to_string();
                        let in_flight_clone = Arc::clone(in_flight_revalidations);
                        let filters_for_refresh: Vec<Expr> = filters.to_vec();
                        let batch_write_tx_clone = batch_write_tx.clone();
                        let cache_key_clone = cache_key.clone();

                        io_runtime.spawn(async move {
                            tracing::debug!(
                                "SWR: Background refresh for single entry started for dataset={dataset_name_clone}"
                            );
                            let result = Self::refresh_entry(
                                federated_clone,
                                &dataset_name_clone,
                                &filters_for_refresh,
                                batch_write_tx_clone,
                                Arc::clone(&in_flight_clone),
                            )
                            .await;

                            match result {
                                Ok(rows) => {
                                    tracing::debug!("Background refresh task completed for dataset={dataset_name_clone}, refreshed {rows} rows");
                                }
                                Err(e) => {
                                    tracing::error!(
                                        "Background refresh task failed for dataset={dataset_name_clone}: {e}"
                                    );
                                    // Remove from in-flight only on failure
                                    // On success, cache_key is removed by flush_cache_writes after write completes
                                    let mut in_flight = in_flight_clone.lock().await;
                                    in_flight.remove(&cache_key_clone);
                                }
                            }
                        });
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
pub type SynchronizedChildren = Arc<RwLock<Vec<Arc<dyn TableProvider>>>>;

/// Caching acceleration execution plan that checks staleness and triggers background refresh
pub struct CachingAccelerationScanExec {
    input: Arc<dyn ExecutionPlan>,
    plan_properties: PlanProperties,
    /// Maximum time data is considered "fresh" - can be served without refresh
    max_age: Option<Duration>,
    /// Time window after `max_age` during which stale data can be served while revalidating
    stale_while_revalidate: Option<Duration>,
    /// If true, serve expired cached data when upstream source returns an error
    stale_if_error: bool,
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
}

impl CachingAccelerationScanExec {
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        max_age: Option<Duration>,
        stale_while_revalidate: Option<Duration>,
        stale_if_error: bool,
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
        // Default max_age (TTL) to 30 seconds if not specified
        let max_age = max_age.or(Some(Duration::from_secs(30)));

        let plan_properties = input
            .properties()
            .clone()
            .with_emission_type(EmissionType::Final)
            .with_partitioning(Partitioning::UnknownPartitioning(1));

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
        write!(f, "CachingAccelerationScanExec")
    }
}

impl ExecutionPlan for CachingAccelerationScanExec {
    fn name(&self) -> &'static str {
        "CachingAccelerationScanExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn properties(&self) -> &datafusion::physical_plan::PlanProperties {
        &self.plan_properties
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::SinglePartition; 1]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(
            Arc::clone(&children[0]),
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

        // Execute the accelerator scan
        let accelerator_stream = self.input.execute(partition, Arc::clone(&context))?;

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
        let dataset_name = self.dataset_name.clone();
        let filters = self.filters.clone();
        let limit = self.limit;
        let max_age = self.max_age;
        let stale_while_revalidate = self.stale_while_revalidate;
        let stale_if_error = self.stale_if_error;
        let io_runtime = self.io_runtime.clone();
        let in_flight_revalidations = Arc::clone(&self.in_flight_revalidations);
        let synchronized_children = Arc::clone(&self.synchronized_children);
        let batch_write_tx = self.batch_write_tx.clone();

        tracing::debug!(
            "CacheAccelerationScanExec::execute about to spawn cache check for dataset={}",
            dataset_name
        );

        // Use stream::once pattern to handle cache miss like FallbackOnZeroResultsScanExec
        let cache_miss_or_stale_stream = futures::stream::once(async move {
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
                        // Pass the expired batches for stale_if_error fallback
                        let expired_batches = if stale_if_error {
                            Some(cached_batches)
                        } else {
                            None
                        };
                        return CacheRefreshHelper::handle_cache_miss(
                            federated,
                            &dataset_name,
                            &filters,
                            limit,
                            Arc::clone(&schema_clone),
                            true, // is_expired = true, will upsert
                            stale_if_error,
                            expired_batches,
                            &io_runtime,
                            Arc::clone(&synchronized_children),
                            batch_write_tx.clone(),
                        )
                        .await;
                    }
                }

                // Data is fresh or stale - serve from cache (stale triggers background refresh)
                CacheRefreshHelper::handle_cache_hit(
                    cached_batches,
                    &federated,
                    &dataset_name,
                    max_age,
                    stale_while_revalidate,
                    &io_runtime,
                    Arc::clone(&schema_clone),
                    &filters,
                    &in_flight_revalidations,
                    batch_write_tx.clone(),
                )
                .await
            } else {
                // Cache miss - no data in accelerator - retrieve from source and store in accelerator
                tracing::debug!(
                    "No cached data for dataset={dataset_name}, treating as cache miss (insert)"
                );
                CacheRefreshHelper::handle_cache_miss(
                    federated,
                    &dataset_name,
                    &filters,
                    limit,
                    Arc::clone(&schema_clone),
                    false, // is_expired = false, will insert (append)
                    false, // stale_if_error = false, no expired data to fall back to
                    None,  // no expired batches
                    &io_runtime,
                    synchronized_children,
                    batch_write_tx,
                )
                .await
            }
        })
        .flatten();

        let adapter = RecordBatchStreamAdapter::new(schema, cache_miss_or_stale_stream);
        Ok(Box::pin(adapter))
    }
}

/// Filter out 5xx server error responses from batches before caching.
///
/// 5xx errors are typically transient (e.g., server overload, temporary outage)
/// and should not be persisted in the cache. This ensures that temporary
/// failures don't pollute the cache with error responses that would be
/// served to subsequent requests.
///
/// Returns an error if the `response_status` column is missing or has the wrong type,
/// as this indicates a schema bug in the HTTP connector code path.
fn filter_5xx_responses(batches: Vec<RecordBatch>) -> DataFusionResult<Vec<RecordBatch>> {
    let mut result = Vec::with_capacity(batches.len());

    for batch in batches {
        // Get the response_status column index - must exist for HTTP connector batches
        let col_idx = batch
            .schema()
            .index_of(RESPONSE_STATUS_COLUMN)
            .map_err(|_| {
                DataFusionError::Internal(format!(
                    "Missing required '{RESPONSE_STATUS_COLUMN}' column in HTTP response batch"
                ))
            })?;

        // Get the response_status column as UInt16Array
        let status_array = batch
            .column(col_idx)
            .as_any()
            .downcast_ref::<UInt16Array>()
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "'{RESPONSE_STATUS_COLUMN}' column must be UInt16Array"
                ))
            })?;

        // Create boolean mask: true for non-5xx status codes
        let mask: arrow::array::BooleanArray = status_array
            .iter()
            .map(|status| status.map(|s| !(500..600).contains(&s)))
            .collect();

        // Apply filter
        let filtered = filter_record_batch(&batch, &mask)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

        if filtered.num_rows() > 0 {
            result.push(filtered);
        }
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "duckdb")]
    use crate::dataaccelerator::duckdb::create_table_provider;
    #[cfg(feature = "duckdb")]
    use datafusion::logical_expr::CreateExternalTable;
    #[cfg(feature = "duckdb")]
    use datafusion_table_providers::duckdb::DuckDBTableProviderFactory;
    #[cfg(feature = "duckdb")]
    use duckdb::AccessMode;

    use super::*;
    use arrow::array::{Int32Array, RecordBatch, StringArray, TimestampNanosecondArray};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use async_trait::async_trait;
    use datafusion::catalog::Session;
    use datafusion::common::{Constraint, Constraints, ToDFSchema};
    use datafusion::datasource::TableType;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::sql::TableReference;
    use parking_lot::RwLock;
    use std::any::Any;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::{Duration, SystemTime};

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
        fn as_any(&self) -> &dyn Any {
            self
        }

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
            "test_dataset".to_string(),
            accelerator_write_mutex,
            Arc::clone(in_flight_revalidations),
            last_updated_at,
        );
        (tx, handle)
    }

    #[async_trait]
    impl TableProvider for MockAcceleratorTableProvider {
        fn as_any(&self) -> &dyn Any {
            self
        }

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
    }

    impl MockHttpTableProvider {
        /// Create a mock HTTP provider that returns data with the specified response status code.
        fn with_status(status_code: u16, content: &str) -> Self {
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
            }
        }
    }

    #[async_trait]
    impl TableProvider for MockHttpTableProvider {
        fn as_any(&self) -> &dyn Any {
            self
        }

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
            Ok(Arc::new(DataSourceExec::new(Arc::new(
                MemorySourceConfig::try_new(
                    std::slice::from_ref(&self.data),
                    Arc::clone(&self.schema),
                    None,
                )?,
            ))))
        }
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
        // Only path and body should be extracted (query is null)
        assert_eq!(filters.len(), 2, "Should only extract non-null filters");
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
        // Only query should be extracted (path and body are empty strings)
        assert_eq!(
            filters.len(),
            1,
            "Should not extract filters for empty strings"
        );
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

        let max_age = Duration::from_secs(60);
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

        let max_age = Duration::from_secs(60);
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

        let max_age = Duration::from_secs(60);
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

        let max_age = Duration::from_secs(60);
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

        let max_age = Duration::from_secs(60);
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

        let max_age = Duration::from_secs(60);
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

        let max_age = Duration::from_secs(60);
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

        let max_age = Duration::from_secs(60);
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
        let max_age = Duration::from_secs(60);
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
        let filter_sets = CacheRefreshHelper::extract_unique_filter_sets(&[batch])
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

        let max_age = Duration::from_secs(60);
        let freshness =
            check_cache_freshness(&[batch], max_age, None).expect("Should check freshness");

        assert_eq!(
            freshness,
            CacheFreshness::Expired,
            "Batches without fetched_at column should be expired"
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

        let max_age = Duration::from_secs(60);
        let freshness =
            check_cache_freshness(&[batch], max_age, None).expect("Should check freshness");

        assert_eq!(
            freshness,
            CacheFreshness::Expired,
            "Batches with NULL fetched_at should be expired"
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
        // Includes response_status column required by filter_5xx_responses
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
            let status = UInt16Array::from(vec![200]); // 200 OK - will pass filter_5xx_responses

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
                - Duration::from_secs(120).as_nanos()) as i64;
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

        let max_age = Some(Duration::from_secs(60)); // 60 second TTL
        let stale_while_revalidate = Some(Duration::from_secs(300)); // 5 minute SWR window
        let in_flight_revalidations: InFlightRevalidations =
            Arc::new(Mutex::new(std::collections::HashSet::new()));

        let (batch_write_tx, _consumer_handle) =
            spawn_test_cache_write_consumer(&accelerator, &in_flight_revalidations);

        // Create a tokio runtime handle for the background task
        let io_runtime = tokio::runtime::Handle::current();

        // Call handle_cache_hit - this should:
        // 1. Return the stale data immediately
        // 2. Spawn a background task to refresh ONLY the accessed entry
        let _stream = CacheRefreshHelper::handle_cache_hit(
            vec![stale_cached_data],
            &(Arc::clone(&federated) as Arc<dyn TableProvider>),
            "test_dataset",
            max_age,
            stale_while_revalidate,
            &io_runtime,
            Arc::clone(&schema),
            &access_filters,
            &in_flight_revalidations,
            batch_write_tx,
        )
        .await;

        // Wait for flush interval `CACHE_WRITE_FLUSH_INTERVAL_MS` + buffer 100ms
        tokio::time::sleep(Duration::from_millis(CACHE_WRITE_FLUSH_INTERVAL_MS + 100)).await;

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
        let in_flight = in_flight_revalidations.lock().await;
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
    #[tokio::test]
    async fn test_batched_cache_writer_flushes_multiple_requests() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        let accelerator = Arc::new(MockAcceleratorTableProvider::new(
            Arc::clone(&schema),
            vec![],
        ));
        let in_flight: InFlightRevalidations =
            Arc::new(Mutex::new(std::collections::HashSet::new()));

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
                is_upsert: false,
                cache_key: format!("key_{i}"),
            })
            .await
            .expect("to send write request");
        }

        // Wait for flush interval `CACHE_WRITE_FLUSH_INTERVAL_MS` + buffer 100ms
        tokio::time::sleep(Duration::from_millis(CACHE_WRITE_FLUSH_INTERVAL_MS + 100)).await;

        // Verify accelerator received data
        let data = accelerator.get_data();
        assert!(!data.is_empty(), "Accelerator should have data after flush");

        let total_rows: usize = data.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total_rows, 3, "Should have 3 rows from 3 requests");
    }

    /// Test that 5xx responses are returned to users but NOT written to the cache.
    ///
    /// Simulates cache miss flow:
    /// 1. Create mock HTTP source (federated) and empty accelerator
    /// 2. Call `handle_cache_miss` (called when accelerator has no data for query)
    /// 3. Verify user receives 5xx response data (can see the error)
    /// 4. Verify accelerator remains empty (transient errors not persisted)
    #[tokio::test]
    async fn test_5xx_responses_returned_to_user_but_not_cached() {
        use futures::StreamExt;

        // 1. Create mock HTTP source (returns 500) and empty accelerator
        let http_source = Arc::new(MockHttpTableProvider::with_status(
            500,
            "Internal Server Error",
        ));
        let schema = http_source.schema();

        let accelerator = Arc::new(MockAcceleratorTableProvider::new(
            Arc::clone(&schema),
            vec![],
        ));
        let in_flight: InFlightRevalidations =
            Arc::new(Mutex::new(std::collections::HashSet::new()));

        let (batch_write_tx, _handle) = spawn_test_cache_write_consumer(&accelerator, &in_flight);

        // 2. Call handle_cache_miss - this is what happens when user queries and cache is empty
        let mut stream = CacheRefreshHelper::handle_cache_miss(
            Arc::clone(&http_source) as Arc<dyn TableProvider>,
            "test_dataset",
            &[col("content").eq(lit("test"))], // filters
            None,                              // limit
            Arc::clone(&schema),
            false, // is_expired
            false, // stale_if_error
            None,  // expired_batches
            &tokio::runtime::Handle::current(),
            Arc::new(vec![].into()), // synchronized_children
            batch_write_tx,
        )
        .await;

        // Collect user-visible results
        let mut user_batches = Vec::new();
        while let Some(result) = stream.next().await {
            user_batches.push(result.expect("stream should not error"));
        }

        // 3. Verify user receives the 500 response data
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
        assert_eq!(status_col.value(0), 500, "User should see status 500");

        // Wait for cache write flush
        tokio::time::sleep(Duration::from_millis(CACHE_WRITE_FLUSH_INTERVAL_MS + 100)).await;

        // 4. Verify accelerator is empty (5xx was not cached)
        let cached_data = accelerator.get_data();
        assert!(
            cached_data.is_empty(),
            "5xx responses should NOT be in accelerator"
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
            Arc::new(Mutex::new(std::collections::HashSet::new()));

        let (batch_write_tx, _handle) = spawn_test_cache_write_consumer(&accelerator, &in_flight);

        // 2. Call handle_cache_miss - this is what happens when user queries and cache is empty
        let mut stream = CacheRefreshHelper::handle_cache_miss(
            Arc::clone(&http_source) as Arc<dyn TableProvider>,
            "test_dataset",
            &[col("content").eq(lit("test"))], // filters
            None,                              // limit
            Arc::clone(&schema),
            false, // is_expired
            false, // stale_if_error
            None,  // expired_batches
            &tokio::runtime::Handle::current(),
            Arc::new(vec![].into()), // synchronized_children
            batch_write_tx,
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

        // Wait for cache write flush
        tokio::time::sleep(Duration::from_millis(CACHE_WRITE_FLUSH_INTERVAL_MS + 100)).await;

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

    /// Helper to create a schema with `response_status` column for `filter_5xx` tests
    fn create_http_response_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("content", DataType::Utf8, false),
            Field::new(RESPONSE_STATUS_COLUMN, DataType::UInt16, false),
        ]))
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

        let result = filter_5xx_responses(vec![batch]).expect("filter should succeed");

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

        let result = filter_5xx_responses(vec![batch]).expect("filter should succeed");

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

        let result = filter_5xx_responses(vec![batch]).expect("filter should succeed");

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

        let result = filter_5xx_responses(vec![batch]).expect("filter should succeed");

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
        let result = filter_5xx_responses(vec![]).expect("filter should succeed");
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

        let result =
            filter_5xx_responses(vec![batch1, batch2, batch3]).expect("filter should succeed");

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

        let result = filter_5xx_responses(vec![batch]).expect("filter should succeed");

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

    /// Tests `overwrite_accelerator` with `DuckDB` in-memory accelerator.
    /// This path is used when the accelerator has NO constraints configured.
    #[tokio::test]
    #[cfg(feature = "duckdb")]
    async fn test_overwrite_accelerator_duckdb() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new(
                CACHE_REFRESHED_AT_COLUMN,
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
        ]));

        let df_schema = ToDFSchema::to_dfschema_ref(Arc::clone(&schema))
            .expect("to convert Arrow schema to DataFusion schema");

        // Create an in-memory DuckDB table (no constraints = overwrite path)
        let external_table = CreateExternalTable {
            schema: df_schema,
            name: TableReference::bare("cache_concat_test"),
            location: String::new(),
            file_type: String::new(),
            table_partition_cols: vec![],
            if_not_exists: true,
            or_replace: false,
            definition: None,
            order_exprs: vec![],
            unbounded: false,
            options: HashMap::new(),
            constraints: Constraints::new_unverified(vec![]),
            column_defaults: HashMap::default(),
            temporary: false,
        };

        let duckdb_factory = DuckDBTableProviderFactory::new(AccessMode::ReadWrite);
        let table = create_table_provider(&duckdb_factory, &external_table, None)
            .await
            .expect("table should be created");

        #[expect(clippy::cast_possible_truncation)]
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("Time went backwards")
            .as_nanos() as i64;

        // Create 5 small batches with 2 rows each (10 rows total)
        let batches: Vec<RecordBatch> = (0..5)
            .map(|i| {
                let base_id = i * 2;
                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![
                        Arc::new(Int32Array::from(vec![base_id, base_id + 1])),
                        Arc::new(StringArray::from(vec![
                            format!("name_{base_id}"),
                            format!("name_{}", base_id + 1),
                        ])),
                        Arc::new(TimestampNanosecondArray::from(vec![Some(now); 2])),
                    ],
                )
                .expect("Should create batch")
            })
            .collect();

        // Call overwrite_accelerator with multiple batches
        CacheRefreshHelper::overwrite_accelerator(Arc::clone(&table), "cache_concat_test", batches)
            .await
            .expect("Should overwrite accelerator");

        // Query the DuckDB table to verify all rows were inserted
        let ctx = SessionContext::new();
        ctx.register_table("cache_concat_test", Arc::clone(&table))
            .expect("Should register table");

        let result = ctx
            .sql("SELECT id, name FROM cache_concat_test ORDER BY id")
            .await
            .expect("Should execute query")
            .collect()
            .await
            .expect("Should collect results");

        let pretty =
            arrow::util::pretty::pretty_format_batches(&result).expect("Should format batches");
        insta::assert_snapshot!("duckdb_overwrite_accelerator", pretty);
    }

    /// Tests `append_to_accelerator` with `DuckDB` using primary key and `on_conflict` upsert.
    /// This path is used when the accelerator has constraints configured.
    /// Also tests that there are no issues with multiple upserts for the same key spread across
    /// multiple batches.
    #[tokio::test]
    #[cfg(feature = "duckdb")]
    async fn test_append_to_accelerator_duckdb() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new(
                CACHE_REFRESHED_AT_COLUMN,
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
        ]));

        let df_schema = ToDFSchema::to_dfschema_ref(Arc::clone(&schema))
            .expect("to convert Arrow schema to DataFusion schema");

        // Create primary key constraint on the "id" column (index 0) with upsert behavior
        let mut options = HashMap::new();
        options.insert("on_conflict".to_string(), "upsert:id".to_string());

        let external_table = CreateExternalTable {
            schema: df_schema,
            name: TableReference::bare("cache_upsert_test"),
            location: String::new(),
            file_type: String::new(),
            table_partition_cols: vec![],
            if_not_exists: true,
            or_replace: false,
            definition: None,
            order_exprs: vec![],
            unbounded: false,
            options,
            constraints: Constraints::new_unverified(vec![Constraint::PrimaryKey(vec![0])]),
            column_defaults: HashMap::default(),
            temporary: false,
        };

        let duckdb_factory = DuckDBTableProviderFactory::new(AccessMode::ReadWrite);
        let table = create_table_provider(&duckdb_factory, &external_table, None)
            .await
            .expect("table should be created");

        // Verify that constraints are set (this triggers the append path)
        assert!(
            table.constraints().is_some_and(|c| !c.is_empty()),
            "Table should have constraints configured"
        );

        #[expect(clippy::cast_possible_truncation)]
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("Time went backwards")
            .as_nanos() as i64;

        // Insert initial data: 5 rows with ids 0-4
        let initial_batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![0, 1, 2, 3, 4])),
                Arc::new(StringArray::from(vec![
                    "initial_0",
                    "initial_1",
                    "initial_2",
                    "initial_3",
                    "initial_4",
                ])),
                Arc::new(TimestampNanosecondArray::from(vec![Some(now); 5])),
            ],
        )
        .expect("Should create batch");

        CacheRefreshHelper::append_to_accelerator(&table, "cache_upsert_test", vec![initial_batch])
            .await
            .expect("Should append initial data");

        // Register table with SessionContext for SQL queries
        let ctx = SessionContext::new();
        ctx.register_table("cache_upsert_test", Arc::clone(&table))
            .expect("Should register table");

        // Verify initial insert with snapshot
        let initial_result = ctx
            .sql("SELECT id, name FROM cache_upsert_test ORDER BY id")
            .await
            .expect("Should execute query")
            .collect()
            .await
            .expect("Should collect results");
        let initial_pretty = arrow::util::pretty::pretty_format_batches(&initial_result)
            .expect("Should format batches");
        insta::assert_snapshot!("duckdb_upsert_initial_data", initial_pretty);

        // Create upsert data to test that the same ID can appear across SEPARATE batches
        // within a single append_to_accelerator call. This simulates the scenario where
        // multiple cache refresh responses for the same entry are batched together.
        // Batch 1: id=2 (update), id=3 (update)
        // Batch 2: id=2 (update), id=4 (update), id=5 (new), id=6 (new)
        let upsert_batch_1 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![2, 3])),
                Arc::new(StringArray::from(vec![
                    "updated_2", // Update for id=2
                    "updated_3",
                ])),
                Arc::new(TimestampNanosecondArray::from(vec![
                    Some(
                        now + 1_000_000_000
                    );
                    2
                ])),
            ],
        )
        .expect("Should create batch");

        let upsert_batch_2 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![2, 4, 5, 6])),
                Arc::new(StringArray::from(vec![
                    "updated_2", // Update for id=2 again
                    "updated_4",
                    "new_5",
                    "new_6",
                ])),
                Arc::new(TimestampNanosecondArray::from(vec![
                    Some(
                        now + 1_000_000_000
                    );
                    4
                ])),
            ],
        )
        .expect("Should create batch");

        // Single append with multiple batches containing duplicate id=2 (same data)
        CacheRefreshHelper::append_to_accelerator(
            &table,
            "cache_upsert_test",
            vec![upsert_batch_1, upsert_batch_2],
        )
        .await
        .expect("Should append/upsert batches with duplicate keys (same data)");

        // Verify upsert results with snapshot
        // Expected: 7 rows (ids 0,1,2,3,4,5,6), with id=2 having "updated_2"
        let upsert_result = ctx
            .sql("SELECT id, name FROM cache_upsert_test ORDER BY id")
            .await
            .expect("Should execute query")
            .collect()
            .await
            .expect("Should collect results");
        let upsert_pretty = arrow::util::pretty::pretty_format_batches(&upsert_result)
            .expect("Should format batches");
        insta::assert_snapshot!("duckdb_upsert_second_update", upsert_pretty);
    }
}
