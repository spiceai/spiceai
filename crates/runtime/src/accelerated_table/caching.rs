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
use std::time::{Duration, SystemTime};

use arrow::array::StringArray;
use arrow::array::{Array, RecordBatch, TimestampNanosecondArray};
use arrow::datatypes::SchemaRef;
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
pub fn spawn_batched_cache_write_task(
    mut rx: CacheWriteReceiver,
    accelerator: Arc<dyn TableProvider>,
    dataset_name: String,
    accelerator_write_mutex: Arc<Mutex<()>>,
    in_flight_revalidations: InFlightRevalidations,
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
        let ts_array = array
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(
                    "CACHE_REFRESHED_AT_COLUMN is not TimestampNanosecondArray".to_string(),
                )
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

        if batches.is_empty() {
            tracing::debug!("No data returned from source for dataset={dataset_name}");
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

        tracing::debug!(
            "overwrite_accelerator - inserting {} batches ({} total rows) into accelerator for dataset={}",
            batches.len(),
            total_rows,
            dataset_name
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
        let plan: Arc<dyn ExecutionPlan> =
            Arc::new(StreamingDataUpdateExecutionPlan::new(Box::pin(adapter)));

        // For caching mode, we use InsertOp::Overwrite to replace all existing data
        // because HTTP responses can contain multiple rows with the same filter values
        // (e.g., search results), which would violate primary key constraints if we used
        // InsertOp::Append. This means each query overwrites the cache, which is acceptable
        // for the caching use case.
        let insert_op = InsertOp::Overwrite;

        tracing::debug!(
            "overwrite_accelerator calling accelerator.insert_into with op={:?} for dataset={}",
            insert_op,
            dataset_name
        );
        let insert_plan = accelerator.insert_into(&state, plan, insert_op).await?;

        // Execute the insertion
        tracing::debug!(
            "overwrite_accelerator executing insert plan for dataset={}",
            dataset_name
        );
        let task_ctx = Arc::new(TaskContext::default());
        let _ = datafusion::physical_plan::collect(insert_plan, task_ctx).await?;
        tracing::debug!(
            "overwrite_accelerator COMPLETED - successfully inserted {} rows into accelerator for dataset={}",
            total_rows,
            dataset_name
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

        tracing::trace!(
            "append_to_accelerator - appending {} batches ({total_rows} total rows) to accelerator for dataset={dataset_name}",
            batches.len(),
        );

        let batch_stream = futures::stream::iter(batches.into_iter().map(Ok));
        let adapter = datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
            Arc::clone(&schema),
            batch_stream,
        );

        let plan: Arc<dyn ExecutionPlan> =
            Arc::new(StreamingDataUpdateExecutionPlan::new(Box::pin(adapter)));

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

                // Clone batches for propagation to children.
                // RecordBatch::clone() is cheap - only clones Arc pointers, not the underlying data.
                let batches_for_propagate = batches.clone();
                let filters_clone: Vec<Expr> = filters.to_vec();
                let cache_key = compute_cache_key_from_filters(filters);

                // Send write request to batched consumer (takes ownership of batches clone)
                let write_request = CacheWriteRequest {
                    batches: batches.clone(),
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
                        "Enqueued cache write for dataset={dataset_name}, {total_rows} rows, is_upsert={is_expired}",
                    );
                }

                // Propagate to synchronized children immediately (don't wait for flush interval)
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
                    "Background cache update performed for dataset={dataset_name}, {total_rows} rows"
                );

                // Return data to user immediately (don't wait for background write)
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, RecordBatch, StringArray, TimestampNanosecondArray};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use async_trait::async_trait;
    use datafusion::catalog::Session;
    use datafusion::datasource::TableType;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::physical_plan::ExecutionPlan;
    use parking_lot::RwLock;
    use std::any::Any;
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
        let handle = spawn_batched_cache_write_task(
            rx,
            Arc::clone(accelerator) as Arc<dyn TableProvider>,
            "test_dataset".to_string(),
            accelerator_write_mutex,
            Arc::clone(in_flight_revalidations),
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
        let schema = Arc::new(Schema::new(vec![
            Field::new("request_path", DataType::Utf8, true),
            Field::new("request_query", DataType::Utf8, true),
            Field::new("data", DataType::Utf8, true),
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
}
