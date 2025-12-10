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
use datafusion::common::Result as DataFusionResult;
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
use tokio::sync::Mutex;

use crate::dataupdate::StreamingDataUpdateExecutionPlan;

/// Type alias for tracking in-flight revalidation requests.
/// The key is a cache key derived from the filter expressions (`request_path`, `request_query`, `request_body`).
/// When a revalidation is in progress for a cache key, other requests for the same key will skip
/// triggering a new revalidation to avoid duplicate upstream requests during the SWR window.
pub type InFlightRevalidations = Arc<Mutex<HashSet<String>>>;

pub const CACHE_REFRESHED_AT_COLUMN: &str = "fetched_at";

/// Maximum number of concurrent refresh requests
const MAX_CONCURRENT_REFRESHES: usize = 10;

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
    if batches.is_empty() {
        return Ok(CacheFreshness::Fresh); // No data means nothing to check
    }

    // Check the first batch for schema information
    let schema = batches[0].schema();
    if schema.column_with_name(CACHE_REFRESHED_AT_COLUMN).is_none() {
        // No metadata column means data was never refreshed in cache mode - treat as expired
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
    /// Refresh stale rows in the cache by querying the accelerator for rows with old `fetched_at` timestamps,
    /// then re-executing the query on the federated source with the original filter parameters.
    /// This is specifically designed for HTTP connector caching mode.
    pub async fn refresh_stale_rows(
        federated: Arc<dyn TableProvider>,
        accelerator: Arc<dyn TableProvider>,
        dataset_name: &str,
        ttl: Duration,
        accelerator_mutex: Arc<Mutex<()>>,
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

        // Extract filter sets from all stale rows
        let mut filter_sets: Vec<Vec<Expr>> = Vec::new();
        for batch in &stale_batches {
            for row_idx in 0..batch.num_rows() {
                let row_filters = Self::extract_filters_from_row(batch, row_idx)?;
                filter_sets.push(row_filters);
            }
        }

        tracing::debug!(
            "Found {} stale rows to refresh for dataset {}",
            filter_sets.len(),
            dataset_name
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
            let accelerator_mutex = Arc::clone(&accelerator_mutex);

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
                let lock_guard = accelerator_mutex.lock().await;

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
        let exclusion_filter = Self::build_exclusion_filter(filters);

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

    /// Build an exclusion filter that matches rows NOT matching the provided filters.
    /// Returns `None` if no filters are provided.
    ///
    /// For example, if filters are [path = '/api/users', query = 'page=1'],
    /// this returns: NOT (path = '/api/users' AND query = 'page=1')
    fn build_exclusion_filter(filters: &[Expr]) -> Option<Expr> {
        if filters.is_empty() {
            return None;
        }

        // Combine all filters with AND, then negate
        filters.iter().cloned().reduce(Expr::and).map(not)
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
    /// * `accelerator_mutex` - Mutex to protect concurrent access to the accelerator.
    #[expect(clippy::too_many_arguments)]
    async fn handle_cache_miss(
        federated: Arc<dyn TableProvider>,
        accelerator: Arc<dyn TableProvider>,
        dataset_name: &str,
        filters: &[Expr],
        limit: Option<usize>,
        fallback_schema: SchemaRef,
        is_expired: bool,
        stale_if_error: bool,
        expired_batches: Option<Vec<RecordBatch>>,
        accelerator_mutex: Arc<Mutex<()>>,
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

                // Acquire the mutex to protect accelerator operations
                let lock_guard = accelerator_mutex.lock().await;

                // Store in accelerator for future queries
                let store_result = if is_expired {
                    // Data exists but is expired - upsert (remove matching rows, add new)
                    tracing::debug!("Upserting expired cache entry for dataset={dataset_name}");
                    Self::upsert_into_accelerator(
                        &accelerator,
                        dataset_name,
                        filters,
                        batches.clone(),
                    )
                    .await
                } else {
                    // No data exists - insert (append)
                    tracing::debug!("Inserting new cache entry for dataset={dataset_name}");
                    Self::insert_into_accelerator(&accelerator, dataset_name, batches.clone()).await
                };

                drop(lock_guard); // Release the mutex

                if let Err(e) = store_result {
                    tracing::warn!(
                        "Failed to store fetched data in accelerator for dataset {}: {}",
                        dataset_name,
                        e
                    );
                }

                // Use the schema from the fetched batches, not from the accelerator scan
                let batch_schema = batches[0].schema();
                tracing::debug!("Fetched batch schema:\n{}", SchemaDisplay(&batch_schema));
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
        accelerator: &Arc<dyn TableProvider>,
        dataset_name: &str,
        max_age: Option<Duration>,
        stale_while_revalidate: Option<Duration>,
        io_runtime: &Handle,
        schema: SchemaRef,
        accelerator_mutex: &Arc<Mutex<()>>,
        filters: &[Expr],
        in_flight_revalidations: &InFlightRevalidations,
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
                        let accelerator_clone = Arc::clone(accelerator);
                        let dataset_name_clone = dataset_name.to_string();
                        let accelerator_mutex_clone = Arc::clone(accelerator_mutex);
                        let in_flight_clone = Arc::clone(in_flight_revalidations);

                        io_runtime.spawn(async move {
                            tracing::debug!(
                                "Background refresh task started for dataset={dataset_name_clone}"
                            );
                            let result = Self::refresh_stale_rows(
                                federated_clone,
                                accelerator_clone,
                                &dataset_name_clone,
                                max_age,
                                accelerator_mutex_clone,
                            )
                            .await;

                            // Remove the cache key from in-flight set when done
                            {
                                let mut in_flight = in_flight_clone.lock().await;
                                in_flight.remove(&cache_key);
                            }

                            match result {
                                Ok(rows) => {
                                    tracing::debug!("Background refresh task completed for dataset={dataset_name_clone}, refreshed {rows} rows");
                                }
                                Err(e) => {
                                    tracing::error!("Background refresh task failed for dataset={dataset_name_clone}: {e}");
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
    /// Mutex to protect concurrent access to the accelerator during cache operations
    accelerator_mutex: Arc<Mutex<()>>,
    /// Tracks in-flight revalidation requests to avoid duplicate upstream requests during SWR window
    in_flight_revalidations: InFlightRevalidations,
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
        accelerator_mutex: Arc<Mutex<()>>,
        in_flight_revalidations: InFlightRevalidations,
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
            accelerator_mutex,
            in_flight_revalidations,
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
            Arc::clone(&self.accelerator_mutex),
            Arc::clone(&self.in_flight_revalidations),
        )))
    }

    #[expect(clippy::too_many_lines)]
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
        let schema = accelerator_stream.schema();
        let schema_clone = Arc::clone(&schema);

        let federated = Arc::clone(&self.federated);
        let accelerator = Arc::clone(&self.accelerator);
        let dataset_name = self.dataset_name.clone();
        let filters = self.filters.clone();
        let limit = self.limit;
        let max_age = self.max_age;
        let stale_while_revalidate = self.stale_while_revalidate;
        let stale_if_error = self.stale_if_error;
        let io_runtime = self.io_runtime.clone();
        let accelerator_mutex = Arc::clone(&self.accelerator_mutex);
        let in_flight_revalidations = Arc::clone(&self.in_flight_revalidations);

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
                    let freshness =
                        check_cache_freshness(&cached_batches, max_age, stale_while_revalidate)
                            .unwrap_or(CacheFreshness::Expired);

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
                            accelerator,
                            &dataset_name,
                            &filters,
                            limit,
                            Arc::clone(&schema_clone),
                            true, // is_expired = true, will upsert
                            stale_if_error,
                            expired_batches,
                            Arc::clone(&accelerator_mutex),
                        )
                        .await;
                    }
                }

                // Data is fresh or stale - serve from cache (stale triggers background refresh)
                CacheRefreshHelper::handle_cache_hit(
                    cached_batches,
                    &federated,
                    &accelerator,
                    &dataset_name,
                    max_age,
                    stale_while_revalidate,
                    &io_runtime,
                    Arc::clone(&schema_clone),
                    &accelerator_mutex,
                    &filters,
                    &in_flight_revalidations,
                )
                .await
            } else {
                // Cache miss - no data in accelerator - retrieve from source and store in accelerator
                tracing::debug!(
                    "No cached data for dataset={dataset_name}, treating as cache miss (insert)"
                );
                CacheRefreshHelper::handle_cache_miss(
                    federated,
                    accelerator,
                    &dataset_name,
                    &filters,
                    limit,
                    Arc::clone(&schema_clone),
                    false, // is_expired = false, will insert (append)
                    false, // stale_if_error = false, no expired data to fall back to
                    None,  // no expired batches
                    accelerator_mutex,
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
    use std::sync::Arc;
    use std::time::{Duration, SystemTime};

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
}
