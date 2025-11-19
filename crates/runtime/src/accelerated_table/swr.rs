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
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime};

use arrow::array::{ArrayRef, RecordBatch, TimestampSecondArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::Session;
use datafusion::common::Result as DataFusionResult;
use datafusion::datasource::TableProvider;
use datafusion::execution::{SessionState, TaskContext};
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream, stream::RecordBatchStreamAdapter,
};
use datafusion::prelude::{DataFrame, SessionContext};
use futures::{Stream, StreamExt, TryStreamExt};
use tokio::runtime::Handle;

pub const SWR_REFRESHED_AT_COLUMN: &str = "__spice_swr_refreshed_at";

/// Extension to add SWR metadata column to schema
pub fn add_swr_metadata_column(schema: SchemaRef) -> SchemaRef {
    let mut fields: Vec<Field> = schema.fields().iter().cloned().collect();
    fields.push(Field::new(
        SWR_REFRESHED_AT_COLUMN,
        DataType::Timestamp(arrow::datatypes::TimeUnit::Second, None),
        true,
    ));
    Arc::new(Schema::new(fields))
}

/// Check if data in the acceleration is stale based on TTL
pub fn is_data_stale(batch: &RecordBatch, ttl: Duration) -> DataFusionResult<bool> {
    // Find the refreshed_at column
    let schema = batch.schema();
    let refreshed_at_idx = schema
        .column_with_name(SWR_REFRESHED_AT_COLUMN)
        .map(|(idx, _)| idx);

    let Some(refreshed_at_idx) = refreshed_at_idx else {
        // No metadata column means data was never refreshed in SWR mode
        return Ok(true);
    };

    let refreshed_at_array = batch.column(refreshed_at_idx);
    let refreshed_at_array = refreshed_at_array
        .as_any()
        .downcast_ref::<TimestampSecondArray>()
        .ok_or_else(|| {
            datafusion::error::DataFusionError::Execution(format!(
                "Expected {} to be TimestampSecondArray",
                SWR_REFRESHED_AT_COLUMN
            ))
        })?;

    // Check if any row has stale data
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?
        .as_secs() as i64;

    let ttl_secs = ttl.as_secs() as i64;

    for i in 0..refreshed_at_array.len() {
        if refreshed_at_array.is_null(i) {
            return Ok(true); // Null timestamp means stale
        }
        let refreshed_at = refreshed_at_array.value(i);
        if now - refreshed_at > ttl_secs {
            return Ok(true); // Data is stale
        }
    }

    Ok(false)
}

/// Add refreshed_at timestamp to a record batch
pub fn add_refreshed_at_column(batch: RecordBatch) -> DataFusionResult<RecordBatch> {
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?
        .as_secs() as i64;

    let refreshed_at_array: ArrayRef =
        Arc::new(TimestampSecondArray::from(vec![now; batch.num_rows()]));

    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
    columns.push(refreshed_at_array);

    let new_schema = add_swr_metadata_column(batch.schema());
    RecordBatch::try_new(new_schema, columns).map_err(Into::into)
}

/// Helper functions for SWR refresh operations
struct SwrRefreshHelper;

impl SwrRefreshHelper {
    /// Fetch from source on cache miss (synchronous - blocks the query)
    async fn fetch_from_source_on_miss(
        federated: Arc<dyn TableProvider>,
        dataset_name: &str,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Vec<RecordBatch>> {
        tracing::debug!(
            "SWR: Fetching from source on cache miss for dataset {}",
            dataset_name
        );

        let ctx = SessionContext::new();
        let state = ctx.state();

        // Query source with same filters/limit but all columns
        let plan = federated.scan(&state, None, filters, limit).await?;
        let task_ctx = Arc::new(TaskContext::default());

        // Execute and collect
        let mut all_batches = Vec::new();
        for partition in 0..plan.output_partitioning().partition_count() {
            let mut stream = plan.execute(partition, Arc::clone(&task_ctx))?;
            while let Some(batch) = stream.next().await {
                let batch = batch?;
                if batch.num_rows() > 0 {
                    // Add refreshed_at timestamps
                    let timestamped = add_refreshed_at_column(batch)?;
                    all_batches.push(timestamped);
                }
            }
        }

        // TODO: Store all_batches in accelerator for future queries

        Ok(all_batches)
    }

    /// Query the source and update the accelerator with fresh data (background refresh)
    async fn refresh_from_source(
        federated: Arc<dyn TableProvider>,
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
        for partition in 0..plan.output_partitioning().partition_count() {
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
                "SWR: No data fetched from source for dataset {}",
                dataset_name
            );
            return Ok(0);
        }

        // Add refreshed_at timestamps to all batches
        let mut timestamped_batches = Vec::with_capacity(all_batches.len());
        for batch in all_batches {
            let timestamped = add_refreshed_at_column(batch)?;
            timestamped_batches.push(timestamped);
        }

        let total_rows: usize = timestamped_batches.iter().map(|b| b.num_rows()).sum();

        tracing::debug!(
            "SWR: Fetched {} rows from source for dataset {}",
            total_rows,
            dataset_name
        );

        // TODO: Insert/replace the timestamped_batches into the accelerator
        // This requires the accelerator to support write operations
        // For now, we just fetch and add timestamps - the actual storage is deferred

        Ok(total_rows)
    }
}

/// SWR execution plan that checks staleness and triggers background refresh
pub struct SwrScanExec {
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

impl SwrScanExec {
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
    fn should_refresh(&self, batch: &RecordBatch) -> bool {
        let Some(ttl) = self.ttl else {
            return false; // No TTL configured, never refresh
        };

        is_data_stale(batch, ttl).unwrap_or(false)
    }

    /// Run the user's query on the source (federated table) to fetch fresh data
    async fn fetch_from_source(
        federated: Arc<dyn TableProvider>,
        dataset_name: &str,
        state: &dyn Session,
        filters: &[Expr],
        projection: Option<&Vec<usize>>,
        limit: Option<usize>,
    ) -> DataFusionResult<Vec<RecordBatch>> {
        tracing::debug!(
            "SWR: Fetching fresh data from source for dataset {}",
            dataset_name
        );

        // Simply run the same query the user requested, but on the source
        let plan = federated.scan(state, projection, filters, limit).await?;
        let ctx = SessionContext::new();
        let task_ctx = Arc::new(TaskContext::default());

        // Execute all partitions
        let mut all_batches = Vec::new();
        for partition in 0..plan.output_partitioning().partition_count() {
            let mut stream = plan.execute(partition, Arc::clone(&task_ctx))?;
            while let Some(batch) = stream.next().await {
                all_batches.push(batch?);
            }
        }

        Ok(all_batches)
    }
}

impl std::fmt::Debug for SwrScanExec {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "SwrScanExec")
    }
}

impl DisplayAs for SwrScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SwrScanExec")
    }
}

impl ExecutionPlan for SwrScanExec {
    fn name(&self) -> &str {
        "SwrScanExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.user_schema)
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
        // Execute the accelerator scan
        let mut accelerator_stream = self.input.execute(partition, Arc::clone(&context))?;
        let schema = accelerator_stream.schema();

        let federated = Arc::clone(&self.federated);
        let dataset_name = self.dataset_name.clone();
        let filters = self.filters.clone();
        let limit = self.limit;
        let ttl = self.ttl;
        let io_runtime = self.io_runtime.clone();

        // Use stream::once pattern to handle cache miss like FallbackOnZeroResultsScanExec
        let cache_miss_or_stale_stream = futures::stream::once(async move {
            // Check if accelerator has data
            if let Some(first_batch) = accelerator_stream.next().await {
                match first_batch {
                    Ok(batch) => {
                        tracing::trace!("SWR: Accelerator returned data for dataset {}: {} rows", dataset_name, batch.num_rows());

                        // Check if data is stale and trigger background refresh if needed
                        if let Some(ttl) = ttl {
                            if is_data_stale(&batch, ttl).unwrap_or(false) {
                                tracing::debug!("SWR: Data is stale for dataset {}, triggering background refresh", dataset_name);

                                let federated_clone = Arc::clone(&federated);
                                let dataset_name_clone = dataset_name.clone();
                                let filters_clone = filters.clone();

                                io_runtime.spawn(async move {
                                    if let Err(e) = SwrRefreshHelper::refresh_from_source(
                                        federated_clone,
                                        &dataset_name_clone,
                                        &filters_clone,
                                        limit,
                                    ).await {
                                        tracing::error!("SWR: Background refresh failed for dataset {}: {}", dataset_name_clone, e);
                                    }
                                });
                            }
                        }

                        // Return the accelerator data (piece back the stream with first batch)
                        let first_batch_stream = futures::stream::once(async move { Ok(batch) });
                        let adapter = RecordBatchStreamAdapter::new(
                            schema,
                            first_batch_stream.chain(accelerator_stream),
                        );
                        Box::pin(adapter) as SendableRecordBatchStream
                    }
                    Err(e) => {
                        // Error from accelerator - return the error
                        let error_stream = RecordBatchStreamAdapter::new(
                            schema,
                            futures::stream::once(async move { Err(e) }),
                        );
                        Box::pin(error_stream) as SendableRecordBatchStream
                    }
                }
            } else {
                // Cache miss - accelerator returned no data
                tracing::info!("SWR: Cache miss for dataset {} - fetching from source", dataset_name);

                // Fetch from source synchronously
                match SwrRefreshHelper::fetch_from_source_on_miss(federated, &dataset_name, &filters, limit).await {
                    Ok(batches) if !batches.is_empty() => {
                        tracing::info!("SWR: Fetched {} batches ({} total rows) from source for dataset {}",
                            batches.len(),
                            batches.iter().map(|b| b.num_rows()).sum::<usize>(),
                            dataset_name);

                        let batch_stream = futures::stream::iter(batches.into_iter().map(Ok));
                        let adapter = RecordBatchStreamAdapter::new(schema, batch_stream);
                        Box::pin(adapter) as SendableRecordBatchStream
                    }
                    Ok(_) => {
                        // Source also returned no data
                        tracing::debug!("SWR: Cache miss - source also has no data for dataset {}", dataset_name);
                        let empty_stream = RecordBatchStreamAdapter::new(
                            schema,
                            futures::stream::empty(),
                        );
                        Box::pin(empty_stream) as SendableRecordBatchStream
                    }
                    Err(e) => {
                        tracing::error!("SWR: Cache miss fetch failed for dataset {}: {}", dataset_name, e);
                        let error_stream = RecordBatchStreamAdapter::new(
                            schema,
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
