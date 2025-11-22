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

use arrow::array::{Array, RecordBatch, TimestampNanosecondArray};
use arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::common::Result as DataFusionResult;
use datafusion::datasource::TableProvider;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Expr, dml::InsertOp};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, SendableRecordBatchStream,
    stream::RecordBatchStreamAdapter,
};
use datafusion::prelude::SessionContext;
use futures::StreamExt;
use tokio::runtime::Handle;

use crate::dataupdate::StreamingDataUpdateExecutionPlan;

pub const CACHE_REFRESHED_AT_COLUMN: &str = "fetched_at";

/// Check if cached data is stale based on TTL
#[allow(clippy::cast_possible_wrap)] // SystemTime cast to i64 is safe for reasonable timestamps  
fn is_data_stale(batch: &RecordBatch, ttl: Duration) -> DataFusionResult<bool> {
    // Find the refreshed_at column
    let schema = batch.schema();
    let refreshed_at_idx = schema
        .column_with_name(CACHE_REFRESHED_AT_COLUMN)
        .map(|(idx, _)| idx);

    let Some(refreshed_at_idx) = refreshed_at_idx else {
        // No metadata column means data was never refreshed in cache mode
        return Ok(true);
    };

    let refreshed_at_array = batch.column(refreshed_at_idx);
    let refreshed_at_array = refreshed_at_array
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .ok_or_else(|| {
            datafusion::error::DataFusionError::Execution(format!(
                "Expected '{CACHE_REFRESHED_AT_COLUMN}' column to be TimestampNanosecondArray"
            ))
        })?;

    // Check if any row has stale data
    #[allow(clippy::cast_possible_truncation)] // Safe: nanoseconds won't exceed i64::MAX
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?
        .as_nanos() as i64;

    #[allow(clippy::cast_possible_truncation)] // Safe: Duration nanoseconds fit in i64
    let ttl_nanos = ttl.as_nanos() as i64;

    for i in 0..refreshed_at_array.len() {
        if refreshed_at_array.is_null(i) {
            return Ok(true); // Null timestamp means stale
        }
        let refreshed_at = refreshed_at_array.value(i);
        if now - refreshed_at > ttl_nanos {
            return Ok(true); // Data is stale
        }
    }

    Ok(false)
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
    ) -> DataFusionResult<usize> {
        use datafusion::logical_expr::{col, lit};
        use datafusion::scalar::ScalarValue;

        let ctx = SessionContext::new();
        let state = ctx.state();

        // Calculate the staleness threshold
        #[allow(clippy::cast_possible_truncation)] // Safe: nanoseconds won't exceed i64::MAX
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?
            .as_nanos() as i64;
        #[allow(clippy::cast_possible_truncation)] // Safe: Duration nanoseconds fit in i64
        let ttl_nanos = ttl.as_nanos() as i64;
        let stale_threshold = now - ttl_nanos;

        tracing::debug!(
            "Cache: Querying for stale rows in dataset {} with TTL {:?} (threshold: {})",
            dataset_name,
            ttl,
            stale_threshold
        );

        // Scan the accelerator with a filter for stale rows
        // WHERE fetched_at < threshold
        let filters = vec![col(CACHE_REFRESHED_AT_COLUMN).lt(lit(ScalarValue::TimestampNanosecond(Some(stale_threshold), None)))];
        
        let plan = accelerator.scan(&state, None, &filters, None).await?;
        let task_ctx = Arc::new(TaskContext::default());
        let mut total_refreshed = 0;

        // For each stale request combination, re-fetch from the source
        for partition in 0..plan.properties().output_partitioning().partition_count() {
            let mut stream = plan.execute(partition, Arc::clone(&task_ctx))?;
            
            while let Some(batch_result) = stream.next().await {
                let batch = batch_result?;
                
                for row_idx in 0..batch.num_rows() {
                    // Extract the filter parameters for this row
                    let filters = Self::extract_filters_from_row(&batch, row_idx)?;
                    
                    // Re-fetch from the federated source with these filters
                    tracing::debug!(
                        "Cache: Refreshing stale data for dataset {} with {} filters",
                        dataset_name,
                        filters.len()
                    );

                    match Self::fetch_from_source_on_miss(
                        Arc::clone(&federated),
                        Arc::clone(&accelerator),
                        dataset_name,
                        &filters,
                        None,
                    ).await {
                        Ok(batches) => {
                            total_refreshed += batches.iter().map(RecordBatch::num_rows).sum::<usize>();
                        }
                        Err(e) => {
                            tracing::warn!(
                                "Cache: Failed to refresh stale data for dataset {}: {}",
                                dataset_name,
                                e
                            );
                        }
                    }
                }
            }
        }

        tracing::info!(
            "Cache: Refreshed {} stale rows for dataset {}",
            total_refreshed,
            dataset_name
        );

        Ok(total_refreshed)
    }

    /// Extract filter expressions from a row containing `request_path`, `request_query`, `request_body`
    fn extract_filters_from_row(
        batch: &RecordBatch,
        row_idx: usize,
    ) -> DataFusionResult<Vec<Expr>> {
        use arrow::array::StringArray;
        use datafusion::logical_expr::{col, lit};

        let schema = batch.schema();
        let mut filters = Vec::new();

        // Extract request_path
        if let Some((idx, _)) = schema.column_with_name("request_path") {
            let array = batch.column(idx)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| datafusion::error::DataFusionError::Execution(
                    "request_path column is not a StringArray".to_string()
                ))?;
            
            if !array.is_null(row_idx) {
                let value = array.value(row_idx).to_string();
                // Only add filter if value is non-empty (empty string means no path filter)
                if !value.is_empty() {
                    tracing::debug!("Cache: Extracted request_path filter: {}", value);
                    filters.push(col("request_path").eq(lit(value)));
                }
            }
        }

        // Extract request_query  
        if let Some((idx, _)) = schema.column_with_name("request_query") {
            let array = batch.column(idx)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| datafusion::error::DataFusionError::Execution(
                    "request_query column is not a StringArray".to_string()
                ))?;
            
            if !array.is_null(row_idx) {
                let value = array.value(row_idx).to_string();
                // Only add filter if value is non-empty (empty string means no query filter)
                if !value.is_empty() {
                    tracing::debug!("Cache: Extracted request_query filter: {}", value);
                    filters.push(col("request_query").eq(lit(value)));
                }
            }
        }

        // Extract request_body
        if let Some((idx, _)) = schema.column_with_name("request_body") {
            let array = batch.column(idx)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| datafusion::error::DataFusionError::Execution(
                    "request_body column is not a StringArray".to_string()
                ))?;
            
            if !array.is_null(row_idx) {
                let value = array.value(row_idx).to_string();
                // Only add filter if value is non-empty (empty string means no body filter)
                if !value.is_empty() {
                    tracing::debug!("Cache: Extracted request_body filter: {}", value);
                    filters.push(col("request_body").eq(lit(value)));
                }
            }
        }

        tracing::debug!("Cache: Extracted {} total filters from row (including empty values)", filters.len());
        Ok(filters)
    }

    /// Insert batches into the accelerator
    async fn insert_into_accelerator(
        accelerator: Arc<dyn TableProvider>,
        dataset_name: &str,
        batches: Vec<RecordBatch>,
    ) -> DataFusionResult<()> {
        if batches.is_empty() {
            return Ok(());
        }

        let ctx = SessionContext::new();
        let state = ctx.state();
        let schema = batches[0].schema();

        tracing::info!(
            "Cache: Inserting {} batches ({} total rows) into accelerator for dataset {}",
            batches.len(),
            batches
                .iter()
                .map(arrow::array::RecordBatch::num_rows)
                .sum::<usize>(),
            dataset_name
        );

        // Create a stream from the batches
        let batch_stream = futures::stream::iter(batches.into_iter().map(Ok));
        let adapter = datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
            Arc::clone(&schema),
            batch_stream,
        );

        // Create an execution plan that produces this stream
        let plan: Arc<dyn ExecutionPlan> =
            Arc::new(StreamingDataUpdateExecutionPlan::new(Box::pin(adapter)));

        // Insert into accelerator (append in caching mode to support multiple filter combinations)
        let insert_plan = accelerator
            .insert_into(&state, plan, InsertOp::Append)
            .await?;

        // Execute the insertion
        let task_ctx = Arc::new(TaskContext::default());
        datafusion::physical_plan::collect(insert_plan, task_ctx).await?;

        tracing::info!(
            "Cache: Successfully inserted data into accelerator for dataset {}",
            dataset_name
        );
        Ok(())
    }

    /// Fetch from source on cache miss (synchronous - blocks the query)
    async fn fetch_from_source_on_miss(
        federated: Arc<dyn TableProvider>,
        accelerator: Arc<dyn TableProvider>,
        dataset_name: &str,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Vec<RecordBatch>> {
        tracing::debug!(
            "Cache: Fetching from source on cache miss for dataset {} with {} filters, limit={:?}",
            dataset_name,
            filters.len(),
            limit
        );
        for (i, filter) in filters.iter().enumerate() {
            tracing::debug!("Cache: Filter {}: {:?}", i, filter);
        }

        let ctx = SessionContext::new();
        let state = ctx.state();

        // Query source with same filters/limit but all columns
        let plan = federated.scan(&state, None, filters, limit).await?;
        let task_ctx = Arc::new(TaskContext::default());

        // Execute and collect
        let mut all_batches = Vec::new();
        for partition in 0..plan.properties().output_partitioning().partition_count() {
            let mut stream = plan.execute(partition, Arc::clone(&task_ctx))?;
            while let Some(batch) = stream.next().await {
                let batch = batch?;
                if batch.num_rows() > 0 {
                    all_batches.push(batch);
                }
            }
        }

        // Store in accelerator for future queries
        Self::insert_into_accelerator(
            Arc::clone(&accelerator),
            dataset_name,
            all_batches.clone(),
        )
        .await?;

        Ok(all_batches)
    }

    /// Query the source and update the accelerator with fresh data (background refresh)
    async fn refresh_from_source(
        federated: Arc<dyn TableProvider>,
        accelerator: Arc<dyn TableProvider>,
        dataset_name: &str,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<usize> {
        // Create a session to query the source
        let ctx = SessionContext::new();
        let state = ctx.state();

        // Run the same filters/limit but fetch all columns (no projection)
        let plan = federated.scan(&state, None, filters, limit).await?;
        let task_ctx = Arc::new(TaskContext::default());

        // Execute all partitions and collect data
        let mut all_batches = Vec::new();
        for partition in 0..plan.properties().output_partitioning().partition_count() {
            let mut stream = plan.execute(partition, Arc::clone(&task_ctx))?;
            while let Some(batch) = stream.next().await {
                let batch = batch?;
                if batch.num_rows() > 0 {
                    all_batches.push(batch);
                }
            }
        }

        if all_batches.is_empty() {
            tracing::debug!(
                "Cache: No data fetched from source for dataset {}",
                dataset_name
            );
            return Ok(0);
        }

        let total_rows: usize = all_batches
            .iter()
            .map(arrow::array::RecordBatch::num_rows)
            .sum();

        tracing::debug!(
            "Cache: Fetched {} rows from source for dataset {}",
            total_rows,
            dataset_name
        );

        // Insert/replace the batches into the accelerator
        Self::insert_into_accelerator(accelerator, dataset_name, all_batches).await?;

        Ok(total_rows)
    }
}

/// Caching acceleration execution plan that checks staleness and triggers background refresh
pub struct CachingAccelerationScanExec {
    input: Arc<dyn ExecutionPlan>,
    ttl: Option<Duration>,
    federated: Arc<dyn TableProvider>,
    accelerator: Arc<dyn TableProvider>,
    dataset_name: String,
    io_runtime: Handle,
    filters: Vec<Expr>,
    projection: Option<Vec<usize>>,
    limit: Option<usize>,
}

impl CachingAccelerationScanExec {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        ttl: Option<Duration>,
        federated: Arc<dyn TableProvider>,
        accelerator: Arc<dyn TableProvider>,
        dataset_name: String,
        io_runtime: Handle,
        filters: Vec<Expr>,
        projection: Option<Vec<usize>>,
        limit: Option<usize>,
    ) -> Self {
        // Default TTL to 30 seconds if not specified
        let ttl = ttl.or(Some(Duration::from_secs(30)));

        Self {
            input,
            ttl,
            federated,
            accelerator,
            dataset_name,
            io_runtime,
            filters,
            projection,
            limit,
        }
    }

    /// Check if we should trigger a background refresh
    #[allow(dead_code)]
    fn should_refresh(&self, batch: &RecordBatch) -> bool {
        let Some(ttl) = self.ttl else {
            return false; // No TTL configured, never refresh
        };

        is_data_stale(batch, ttl).unwrap_or(false)
    }

    /// Run the user's query on the source (federated table) to fetch fresh data
    #[allow(dead_code)]
    async fn fetch_from_source(
        federated: Arc<dyn TableProvider>,
        dataset_name: &str,
        state: &dyn Session,
        filters: &[Expr],
        projection: Option<&Vec<usize>>,
        limit: Option<usize>,
    ) -> DataFusionResult<Vec<RecordBatch>> {
        tracing::debug!(
            "Cache: Fetching fresh data from source for dataset {}",
            dataset_name
        );

        // Simply run the same query the user requested, but on the source
        let plan = federated.scan(state, projection, filters, limit).await?;
        let _ctx = SessionContext::new(); // TODO: Use for execution context when implementing background refresh
        let task_ctx = Arc::new(TaskContext::default());

        // Execute all partitions
        let mut all_batches = Vec::new();
        for partition in 0..plan.properties().output_partitioning().partition_count() {
            let mut stream = plan.execute(partition, Arc::clone(&task_ctx))?;
            while let Some(batch) = stream.next().await {
                all_batches.push(batch?);
            }
        }

        Ok(all_batches)
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
        self.input.properties()
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
            self.ttl,
            Arc::clone(&self.federated),
            Arc::clone(&self.accelerator),
            self.dataset_name.clone(),
            self.io_runtime.clone(),
            self.filters.clone(),
            self.projection.clone(),
            self.limit,
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        eprintln!("!!! CacheAccelerationScanExec::execute called for partition {}", partition);
        tracing::debug!(
            "CacheAccelerationScanExec::execute dataset={}, partition={}, num_filters={}",
            self.dataset_name,
            partition,
            self.filters.len()
        );
        
        // Execute the accelerator scan
        let mut accelerator_stream = self.input.execute(partition, Arc::clone(&context))?;
        eprintln!("!!! Got accelerator stream");
        let schema = accelerator_stream.schema();
        let schema_clone = Arc::clone(&schema);

        let federated = Arc::clone(&self.federated);
        let accelerator = Arc::clone(&self.accelerator);
        let dataset_name = self.dataset_name.clone();
        let filters = self.filters.clone();
        let limit = self.limit;
        let ttl = self.ttl;
        let io_runtime = self.io_runtime.clone();

        // Use stream::once pattern to handle cache miss like FallbackOnZeroResultsScanExec
        let cache_miss_or_stale_stream = futures::stream::once(async move {
            eprintln!("!!! About to poll accelerator stream");
            // Check if accelerator has data
            if let Some(first_batch) = accelerator_stream.next().await {
                eprintln!("!!! Accelerator stream returned batch");
                match first_batch {
                    Ok(batch) if batch.num_rows() > 0 => {
                        eprintln!("!!! Batch has {} rows - cache hit!", batch.num_rows());
                        tracing::trace!("Cache: Accelerator returned data for dataset {}: {} rows", dataset_name, batch.num_rows());

                        // Check if data is stale and trigger background refresh if needed
                        if let Some(ttl) = ttl
                            && is_data_stale(&batch, ttl).unwrap_or(false) {
                                tracing::debug!("Cache: Data is stale for dataset {}, triggering background refresh", dataset_name);

                                let federated_clone = Arc::clone(&federated);
                                let accelerator_clone = Arc::clone(&accelerator);
                                let dataset_name_clone = dataset_name.clone();
                                let filters_clone = filters.clone();

                                io_runtime.spawn(async move {
                                    if let Err(e) = CacheRefreshHelper::refresh_from_source(
                                        federated_clone,
                                        accelerator_clone,
                                        &dataset_name_clone,
                                        &filters_clone,
                                        limit,
                                    ).await {
                                        tracing::error!("Cache: Background refresh failed for dataset {}: {}", dataset_name_clone, e);
                                    }
                                });
                            }

                        // Return the accelerator data (piece back the stream with first batch)
                        let first_batch_stream = futures::stream::once(async move { Ok(batch) });
                        let adapter = RecordBatchStreamAdapter::new(
                            Arc::clone(&schema_clone),
                            first_batch_stream.chain(accelerator_stream),
                        );
                        Box::pin(adapter) as SendableRecordBatchStream
                    }
                    Ok(batch) => {
                        eprintln!("!!! Batch has 0 rows - cache miss!");
                        // Empty batch (0 rows) - treat as cache miss
                        tracing::info!("Cache: Cache miss for dataset {} - accelerator returned 0 rows, fetching from source", dataset_name);

                        // Fetch from source synchronously
                        match CacheRefreshHelper::fetch_from_source_on_miss(Arc::clone(&federated), Arc::clone(&accelerator), &dataset_name, &filters, limit).await {
                            Ok(batches) if !batches.is_empty() => {
                                let total_rows: usize = batches.iter().map(arrow::array::RecordBatch::num_rows).sum();
                                tracing::info!("Cache: Fetched {} batches ({} total rows) from source for dataset {}",
                                    batches.len(),
                                    total_rows,
                                    dataset_name);

                                let batch_schema = batches[0].schema();
                                let batch_stream = futures::stream::iter(batches.into_iter().map(Ok));
                                let adapter = RecordBatchStreamAdapter::new(batch_schema, batch_stream);
                                Box::pin(adapter) as SendableRecordBatchStream
                            }
                            Ok(_) | Err(_) => {
                                // Source returned no data or error
                                let empty_stream = RecordBatchStreamAdapter::new(
                                    Arc::clone(&schema_clone),
                                    futures::stream::empty(),
                                );
                                Box::pin(empty_stream) as SendableRecordBatchStream
                            }
                        }
                    }
                    Err(e) => {
                        // Error from accelerator - return the error
                        let error_stream = RecordBatchStreamAdapter::new(
                            Arc::clone(&schema_clone),
                            futures::stream::once(async move { Err(e) }),
                        );
                        Box::pin(error_stream) as SendableRecordBatchStream
                    }
                }
            } else {
                eprintln!("!!! Accelerator stream returned None - cache miss!");
                // Cache miss - accelerator returned no data
                tracing::info!("Cache: Cache miss for dataset {} - fetching from source", dataset_name);

                // Fetch from source synchronously
                match CacheRefreshHelper::fetch_from_source_on_miss(federated, Arc::clone(&accelerator), &dataset_name, &filters, limit).await {
                    Ok(batches) if !batches.is_empty() => {
                        let total_rows: usize = batches.iter().map(arrow::array::RecordBatch::num_rows).sum();
                        tracing::info!("Cache: Fetched {} batches ({} total rows) from source for dataset {}",
                            batches.len(),
                            total_rows,
                            dataset_name);
                        
                        // Debug: log the schema and first batch data
                        if let Some(first_batch) = batches.first() {
                            tracing::info!("Cache: Fetched batch schema: {:?}", first_batch.schema());
                            tracing::info!("Cache: First batch data: {:?}", first_batch);
                        }

                        // Use the schema from the fetched batches, not from the accelerator scan
                        let batch_schema = batches[0].schema();
                        let batch_stream = futures::stream::iter(batches.into_iter().map(Ok));
                        let adapter = RecordBatchStreamAdapter::new(batch_schema, batch_stream);
                        Box::pin(adapter) as SendableRecordBatchStream
                    }
                    Ok(_) => {
                        // Source also returned no data
                        tracing::debug!("Cache: Cache miss - source also has no data for dataset {}", dataset_name);
                        let empty_stream = RecordBatchStreamAdapter::new(
                            Arc::clone(&schema_clone),
                            futures::stream::empty(),
                        );
                        Box::pin(empty_stream) as SendableRecordBatchStream
                    }
                    Err(e) => {
                        tracing::error!("Cache: Cache miss fetch failed for dataset {}: {}", dataset_name, e);
                        let error_stream = RecordBatchStreamAdapter::new(
                            Arc::clone(&schema_clone),
                            futures::stream::once(async move { Err(e) }),
                        );
                        Box::pin(error_stream) as SendableRecordBatchStream
                    }
                }
            }
        }).flatten();

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
    fn test_is_data_stale_no_refresh_column() {
        let schema = create_test_schema_without_refresh_timestamp();
        let id_array = Int32Array::from(vec![1, 2, 3]);
        let name_array = StringArray::from(vec!["alice", "bob", "charlie"]);

        let batch =
            RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(id_array), Arc::new(name_array)])
                .expect("Failed to create batch");

        let ttl = Duration::from_secs(60);
        let result = is_data_stale(&batch, ttl).expect("Should successfully check staleness");
        assert!(result, "Data without refresh column should be considered stale");
    }

    #[test]
    fn test_is_data_stale_fresh_data() {
        let schema = create_test_schema_with_refresh_timestamp();
        let id_array = Int32Array::from(vec![1, 2, 3]);
        let name_array = StringArray::from(vec!["alice", "bob", "charlie"]);

        // Create timestamps that are very recent (within TTL)
        #[allow(clippy::cast_possible_truncation)]
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("Time went backwards")
            .as_nanos() as i64;

        let refresh_timestamps = TimestampNanosecondArray::from(vec![
            Some(now - 10_000_000_000),  // 10 seconds ago
            Some(now - 20_000_000_000),  // 20 seconds ago
            Some(now - 5_000_000_000),   // 5 seconds ago
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

        let ttl = Duration::from_secs(60); // 60 second TTL
        let result = is_data_stale(&batch, ttl).expect("Should successfully check staleness");
        assert!(!result, "Data refreshed within TTL should not be stale");
    }

    #[test]
    fn test_is_data_stale_stale_data() {
        let schema = create_test_schema_with_refresh_timestamp();
        let id_array = Int32Array::from(vec![1, 2]);
        let name_array = StringArray::from(vec!["alice", "bob"]);

        #[allow(clippy::cast_possible_truncation)]
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("Time went backwards")
            .as_nanos() as i64;

        let refresh_timestamps = TimestampNanosecondArray::from(vec![
            Some(now - 90_000_000_000),  // 90 seconds ago (stale)
            Some(now - 120_000_000_000), // 120 seconds ago (stale)
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

        let ttl = Duration::from_secs(60); // 60 second TTL
        let result = is_data_stale(&batch, ttl).expect("Should successfully check staleness");
        assert!(result, "Data older than TTL should be stale");
    }

    #[test]
    fn test_is_data_stale_null_timestamps() {
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

        let ttl = Duration::from_secs(60);
        let result = is_data_stale(&batch, ttl).expect("Should successfully check staleness");
        assert!(result, "Data with null timestamps should be considered stale");
    }

    #[test]
    fn test_is_data_stale_mixed_timestamps() {
        let schema = create_test_schema_with_refresh_timestamp();
        let id_array = Int32Array::from(vec![1, 2, 3]);
        let name_array = StringArray::from(vec!["alice", "bob", "charlie"]);

        #[allow(clippy::cast_possible_truncation)]
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("Time went backwards")
            .as_nanos() as i64;

        // Mix of fresh and stale timestamps - if ANY is stale, the whole batch is stale
        let refresh_timestamps = TimestampNanosecondArray::from(vec![
            Some(now - 10_000_000_000),  // 10 seconds ago (fresh)
            Some(now - 90_000_000_000),  // 90 seconds ago (stale)
            Some(now - 5_000_000_000),   // 5 seconds ago (fresh)
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

        let ttl = Duration::from_secs(60);
        let result = is_data_stale(&batch, ttl).expect("Should successfully check staleness");
        assert!(
            result,
            "Data with any stale timestamp should be considered stale"
        );
    }

    #[test]
    fn test_is_data_stale_ttl_boundary() {
        let schema = create_test_schema_with_refresh_timestamp();
        let id_array = Int32Array::from(vec![1, 2]);
        let name_array = StringArray::from(vec!["alice", "bob"]);

        #[allow(clippy::cast_possible_truncation)]
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("Time went backwards")
            .as_nanos() as i64;

        let ttl = Duration::from_secs(60);
        #[allow(clippy::cast_possible_truncation)]
        let ttl_nanos = ttl.as_nanos() as i64;

        // Well within TTL boundary - this should NOT be stale
        let refresh_timestamps_fresh =
            TimestampNanosecondArray::from(vec![Some(now - ttl_nanos + 1_000_000_000), Some(now - ttl_nanos + 2_000_000_000)]);  // 1-2 seconds within boundary

        let batch_fresh = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(id_array.clone()),
                Arc::new(name_array.clone()),
                Arc::new(refresh_timestamps_fresh),
            ],
        )
        .expect("Failed to create batch");

        let result_fresh = is_data_stale(&batch_fresh, ttl).expect("Should successfully check staleness");
        assert!(!result_fresh, "Data well within TTL boundary should not be stale");
        
        // Well past the TTL boundary - this SHOULD be stale
        let refresh_timestamps_stale =
            TimestampNanosecondArray::from(vec![Some(now - ttl_nanos - 1_000_000_000), Some(now - ttl_nanos - 2_000_000_000)]);  // 1-2 seconds past boundary

        let batch_stale = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(id_array),
                Arc::new(name_array),
                Arc::new(refresh_timestamps_stale),
            ],
        )
        .expect("Failed to create batch");

        let result_stale = is_data_stale(&batch_stale, ttl).expect("Should successfully check staleness");
        assert!(result_stale, "Data well past TTL boundary should be stale");
    }

    #[test]
    fn test_is_data_stale_empty_batch() {
        let schema = create_test_schema_with_refresh_timestamp();
        let id_array = Int32Array::from(Vec::<i32>::new());
        let name_array = StringArray::from(Vec::<&str>::new());
        let refresh_timestamps = TimestampNanosecondArray::from(Vec::<Option<i64>>::new());

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(id_array),
                Arc::new(name_array),
                Arc::new(refresh_timestamps),
            ],
        )
        .expect("Failed to create batch");

        let ttl = Duration::from_secs(60);
        let result = is_data_stale(&batch, ttl).expect("Should successfully check staleness");
        assert!(!result, "Empty batch should not be considered stale");
    }

    #[test]
    fn test_extract_filters_from_row_all_columns_present() {
        let schema = create_test_schema_with_request_params();
        let id_array = Int32Array::from(vec![1, 2]);
        let path_array = StringArray::from(vec![Some("/api/users"), Some("/api/posts")]);
        let query_array = StringArray::from(vec![Some("page=1"), Some("limit=10")]);
        let body_array = StringArray::from(vec![Some("{\"id\":1}"), Some("{\"id\":2}")]);

        #[allow(clippy::cast_possible_truncation)]
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

        let filters =
            CacheRefreshHelper::extract_filters_from_row(&batch, 0).expect("Should extract filters");
        assert_eq!(filters.len(), 3, "Should extract 3 filters");
    }

    #[test]
    fn test_extract_filters_from_row_with_nulls() {
        let schema = create_test_schema_with_request_params();
        let id_array = Int32Array::from(vec![1]);
        let path_array = StringArray::from(vec![Some("/api/users")]);
        let query_array = StringArray::from(vec![None::<&str>]); // Null query
        let body_array = StringArray::from(vec![Some("{\"id\":1}")]);

        #[allow(clippy::cast_possible_truncation)]
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

        let filters =
            CacheRefreshHelper::extract_filters_from_row(&batch, 0).expect("Should extract filters");
        // Only path and body should be extracted (query is null)
        assert_eq!(filters.len(), 2, "Should only extract non-null filters");
    }

    #[test]
    fn test_extract_filters_from_row_with_empty_strings() {
        let schema = create_test_schema_with_request_params();
        let id_array = Int32Array::from(vec![1]);
        let path_array = StringArray::from(vec![Some("")]);  // Empty string
        let query_array = StringArray::from(vec![Some("page=1")]);
        let body_array = StringArray::from(vec![Some("")]);  // Empty string

        #[allow(clippy::cast_possible_truncation)]
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

        let filters =
            CacheRefreshHelper::extract_filters_from_row(&batch, 0).expect("Should extract filters");
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

        #[allow(clippy::cast_possible_truncation)]
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

        let filters =
            CacheRefreshHelper::extract_filters_from_row(&batch, 0).expect("Should extract filters");
        assert_eq!(
            filters.len(),
            0,
            "Should extract 0 filters when columns are missing"
        );
    }
}
