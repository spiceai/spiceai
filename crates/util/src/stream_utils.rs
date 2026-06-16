/*
Copyright 2025 The Spice.ai OSS Authors

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

//! Utilities for working with `DataFusion` record batch streams.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{LexOrdering, OrderingRequirements, PhysicalSortExpr};
use datafusion::physical_plan::execution_plan::{
    Boundedness, CardinalityEffect, EmissionType, InvariantLevel, check_default_invariants,
};
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use parking_lot::Mutex;

/// Sort a record batch stream using `DataFusion`'s `SortExec`.
///
/// This function sorts the incoming stream by the specified columns,
/// which can improve query performance through better data locality
/// and enable more efficient filter pushdown.
///
/// # Features
///
/// Uses `DataFusion`'s `SortExec` which provides:
/// - **Automatic disk spilling**: Handles datasets larger than available memory
/// - **Streaming external merge sort**: Processes data incrementally without loading all into RAM
/// - **SIMD-optimized kernels**: Hardware-accelerated sorting (NEON on arm64, AVX2/AVX-512 on amd64)
/// - **Configurable spill compression**: Supports zstd, `lz4_frame`, or uncompressed spill files
/// - **Memory management**: Integrates with `DataFusion`'s memory pool and reservation system
///
/// # Arguments
///
/// * `stream` - The input record batch stream to sort
/// * `sort_columns` - Sort specifications, in order of precedence. Each entry is
///   either a bare column name (ascending, NULLs last) or
///   `column [ASC|DESC] [NULLS FIRST|LAST]` (case-insensitive). When the NULLS
///   placement is omitted, `DESC` defaults to NULLs first and `ASC` to NULLs
///   last (the SQL/Postgres `ORDER BY` convention).
/// * `context` - Task context for memory management and spill configuration
///
/// # Returns
///
/// A sorted record batch stream, or the original stream if `sort_columns` is empty
/// or contains invalid column names.
///
/// # Errors
///
/// Returns an error if the sort execution fails.
pub fn sort_stream(
    stream: SendableRecordBatchStream,
    sort_columns: &[String],
    context: &Arc<TaskContext>,
) -> Result<SendableRecordBatchStream> {
    if sort_columns.is_empty() {
        return Ok(stream);
    }

    let schema = stream.schema();

    // Build sort expressions from configured sort_columns
    let mut sort_exprs = Vec::with_capacity(sort_columns.len());
    for entry in sort_columns {
        // An entry that matches a column name exactly sorts ascending, NULLs
        // last — the historical behavior, preserved first so a column whose name
        // happens to contain whitespace or a direction keyword keeps working.
        let (col_name, options) = if schema.index_of(entry.trim()).is_ok() {
            (
                entry.trim(),
                arrow::compute::SortOptions {
                    descending: false,
                    nulls_first: false,
                },
            )
        } else if let Some(parsed) = parse_sort_entry(entry) {
            (parsed.0, parsed.1)
        } else {
            tracing::warn!(
                "Invalid sort column specification '{}', expected 'column [ASC|DESC] [NULLS FIRST|LAST]'. Skipping sort.",
                entry
            );
            return Ok(stream);
        };

        // Validate column exists in schema and get its index
        let Ok(column_index) = schema.index_of(col_name) else {
            tracing::warn!(
                "Sort column '{}' not found in schema. Skipping sort.",
                col_name
            );
            return Ok(stream);
        };

        sort_exprs.push(PhysicalSortExpr {
            expr: Arc::new(Column::new(col_name, column_index)),
            options,
        });
    }

    let lex_ordering = LexOrdering::new(sort_exprs).ok_or_else(|| {
        DataFusionError::Execution(
            "Failed to create lex ordering: sort expressions cannot be empty".to_string(),
        )
    })?;

    tracing::debug!(
        "Sorting data stream by columns {:?} using DataFusion SortExec",
        sort_columns
    );

    // Create a streaming execution plan that yields the input stream
    let stream_exec = Arc::new(StreamingExec::new(&schema, stream));

    // Wrap with SortExec for external sorting with disk spilling
    let sort_exec = Arc::new(SortExec::new(lex_ordering, stream_exec));

    // Execute the sort
    let sorted_stream = sort_exec.execute(0, Arc::clone(context))?;

    Ok(sorted_stream)
}

/// Parse one sort specification of the form
/// `column [ASC|DESC] [NULLS FIRST|LAST]` (case-insensitive) into the column
/// name and its Arrow [`SortOptions`](arrow::compute::SortOptions). When the
/// NULLS placement is omitted it follows the SQL/Postgres `ORDER BY` default:
/// NULLs first for `DESC`, NULLs last for `ASC`. Returns `None` for an entry
/// that does not match the grammar (the caller decides how to degrade).
///
/// PUBLIC because any code that advertises a scan `output_ordering` derived from
/// these same sort columns (e.g. Cayenne's sorted-compaction ordering) MUST build
/// its `SortOptions` from THIS parser, so the advertised direction/nulls placement
/// is byte-identical to what `sort_stream` physically wrote. Re-deriving the
/// options separately risks a mismatch (advertise ASC while the file is DESC),
/// which would make sort-elimination / merge-join produce WRONG results.
#[must_use]
pub fn parse_sort_entry(entry: &str) -> Option<(&str, arrow::compute::SortOptions)> {
    let tokens: Vec<&str> = entry.split_whitespace().collect();
    let (&column, modifiers) = tokens.split_first()?;

    let mut descending = false;
    let mut nulls_first: Option<bool> = None;
    let mut rest = modifiers;
    if let Some((&dir, after_dir)) = rest.split_first() {
        if dir.eq_ignore_ascii_case("ASC") {
            rest = after_dir;
        } else if dir.eq_ignore_ascii_case("DESC") {
            descending = true;
            rest = after_dir;
        }
    }
    match rest {
        [] => {}
        [nulls, placement]
            if nulls.eq_ignore_ascii_case("NULLS")
                && (placement.eq_ignore_ascii_case("FIRST")
                    || placement.eq_ignore_ascii_case("LAST")) =>
        {
            nulls_first = Some(placement.eq_ignore_ascii_case("FIRST"));
        }
        _ => return None,
    }

    Some((
        column,
        arrow::compute::SortOptions {
            descending,
            // SQL/Postgres ORDER BY default: DESC puts NULLs first, ASC last.
            nulls_first: nulls_first.unwrap_or(descending),
        },
    ))
}

/// Streaming execution plan that forwards an existing `RecordBatchStream`.
///
/// This is a simple wrapper that allows integrating an existing stream
/// into `DataFusion`'s `ExecutionPlan` framework for operations like sorting.
struct StreamingExec {
    stream: Mutex<Option<SendableRecordBatchStream>>,
    properties: Arc<PlanProperties>,
}

impl StreamingExec {
    fn new(schema: &SchemaRef, stream: SendableRecordBatchStream) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            stream: Mutex::new(Some(stream)),
            properties: Arc::new(properties),
        }
    }
}

impl fmt::Debug for StreamingExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StreamingExec").finish()
    }
}

impl DisplayAs for StreamingExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "StreamingExec")
    }
}

#[deny(clippy::missing_trait_methods)]
impl ExecutionPlan for StreamingExec {
    fn with_preserve_order(&self, _preserve_order: bool) -> Option<Arc<dyn ExecutionPlan>> {
        None
    }

    fn name(&self) -> &'static str {
        "StreamingExec"
    }

    fn static_name() -> &'static str
    where
        Self: Sized,
    {
        "StreamingExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(self.properties().eq_properties.schema())
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn check_invariants(&self, check: InvariantLevel) -> Result<()> {
        check_default_invariants(self, check)
    }

    fn required_input_distribution(&self) -> Vec<datafusion::physical_plan::Distribution> {
        vec![
            datafusion::physical_plan::Distribution::UnspecifiedDistribution;
            self.children().len()
        ]
    }

    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        vec![None; self.children().len()]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![false; self.children().len()]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        self.required_input_distribution()
            .into_iter()
            .map(|dist| {
                !matches!(
                    dist,
                    datafusion::physical_plan::Distribution::SinglePartition
                )
            })
            .collect()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn reset_state(self: Arc<Self>) -> Result<Arc<dyn ExecutionPlan>> {
        let children = self.children().into_iter().cloned().collect();
        self.with_new_children(children)
    }

    fn repartitioned(
        &self,
        _target_partitions: usize,
        _config: &datafusion::config::ConfigOptions,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        Ok(None)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let mut guard = self.stream.try_lock().ok_or_else(|| {
            DataFusionError::Execution("Failed to acquire stream lock".to_string())
        })?;

        let stream = guard
            .take()
            .ok_or_else(|| DataFusionError::Execution("Stream already consumed".to_string()))?;

        Ok(stream)
    }

    fn metrics(&self) -> Option<datafusion::physical_plan::metrics::MetricsSet> {
        None
    }

    fn partition_statistics(
        &self,
        partition: Option<usize>,
    ) -> Result<datafusion::common::Statistics> {
        if let Some(idx) = partition {
            let partition_count = self.properties.output_partitioning().partition_count();
            if idx >= partition_count {
                return Err(DataFusionError::Internal(format!(
                    "Invalid partition index: {idx}, the partition count is {partition_count}"
                )));
            }
        }
        Ok(datafusion::common::Statistics::new_unknown(&self.schema()))
    }

    fn supports_limit_pushdown(&self) -> bool {
        false
    }

    fn with_fetch(&self, _limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        None
    }

    fn fetch(&self) -> Option<usize> {
        None
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::Unknown
    }

    fn try_swapping_with_projection(
        &self,
        _projection: &datafusion::physical_plan::projection::ProjectionExec,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        Ok(None)
    }

    fn gather_filters_for_pushdown(
        &self,
        _phase: datafusion::physical_plan::filter_pushdown::FilterPushdownPhase,
        parent_filters: Vec<Arc<dyn datafusion::physical_expr::PhysicalExpr>>,
        _config: &datafusion::config::ConfigOptions,
    ) -> Result<datafusion::physical_plan::filter_pushdown::FilterDescription> {
        Ok(
            datafusion::physical_plan::filter_pushdown::FilterDescription::all_unsupported(
                &parent_filters,
                &self.children(),
            ),
        )
    }

    fn handle_child_pushdown_result(
        &self,
        _phase: datafusion::physical_plan::filter_pushdown::FilterPushdownPhase,
        child_pushdown_result: datafusion::physical_plan::filter_pushdown::ChildPushdownResult,
        _config: &datafusion::config::ConfigOptions,
    ) -> Result<
        datafusion::physical_plan::filter_pushdown::FilterPushdownPropagation<
            Arc<dyn ExecutionPlan>,
        >,
    > {
        Ok(
            datafusion::physical_plan::filter_pushdown::FilterPushdownPropagation::if_all(
                child_pushdown_result,
            ),
        )
    }

    fn with_new_state(&self, _state: Arc<dyn Any + Send + Sync>) -> Option<Arc<dyn ExecutionPlan>> {
        None
    }

    fn try_pushdown_sort(
        &self,
        _order: &[PhysicalSortExpr],
    ) -> Result<
        datafusion::physical_plan::sort_pushdown::SortOrderPushdownResult<Arc<dyn ExecutionPlan>>,
    > {
        Ok(datafusion::physical_plan::sort_pushdown::SortOrderPushdownResult::Unsupported)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use futures::stream;

    fn create_test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ]))
    }

    fn create_test_batch(ids: Vec<i32>, names: Vec<&str>, values: Vec<i32>) -> RecordBatch {
        let schema = create_test_schema();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(StringArray::from(names)),
                Arc::new(Int32Array::from(values)),
            ],
        )
        .expect("create batch")
    }

    fn create_task_context() -> Arc<TaskContext> {
        Arc::new(TaskContext::default())
    }

    #[tokio::test]
    async fn test_sort_stream_single_column() {
        let schema = create_test_schema();

        // Create unsorted data
        let batch = create_test_batch(
            vec![3, 1, 4, 2],
            vec!["c", "a", "d", "b"],
            vec![30, 10, 40, 20],
        );

        let stream =
            RecordBatchStreamAdapter::new(Arc::clone(&schema), stream::iter(vec![Ok(batch)]));

        let context = create_task_context();
        let sorted = sort_stream(Box::pin(stream), &["id".to_string()], &context)
            .expect("sort should succeed");

        let batches: Vec<RecordBatch> = datafusion::physical_plan::common::collect(sorted)
            .await
            .expect("collect batches");

        assert_eq!(batches.len(), 1);
        let batch = &batches[0];

        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("ids column");

        assert_eq!(ids.values(), &[1, 2, 3, 4]);
    }

    #[tokio::test]
    async fn test_sort_stream_multiple_columns() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("category", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ]));

        // Create data: same category should be sorted by value
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["B", "A", "B", "A"])),
                Arc::new(Int32Array::from(vec![2, 3, 1, 4])),
            ],
        )
        .expect("create batch");

        let stream =
            RecordBatchStreamAdapter::new(Arc::clone(&schema), stream::iter(vec![Ok(batch)]));

        let context = create_task_context();
        let sorted = sort_stream(
            Box::pin(stream),
            &["category".to_string(), "value".to_string()],
            &context,
        )
        .expect("sort should succeed");

        let batches: Vec<RecordBatch> = datafusion::physical_plan::common::collect(sorted)
            .await
            .expect("collect batches");

        assert_eq!(batches.len(), 1);
        let batch = &batches[0];

        let categories = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("category column");
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("value column");

        // Expected: A,3 | A,4 | B,1 | B,2
        assert_eq!(categories.value(0), "A");
        assert_eq!(values.value(0), 3);
        assert_eq!(categories.value(1), "A");
        assert_eq!(values.value(1), 4);
        assert_eq!(categories.value(2), "B");
        assert_eq!(values.value(2), 1);
        assert_eq!(categories.value(3), "B");
        assert_eq!(values.value(3), 2);
    }

    #[tokio::test]
    async fn test_sort_stream_empty_columns_returns_original() {
        let schema = create_test_schema();
        let batch = create_test_batch(vec![3, 1, 2], vec!["c", "a", "b"], vec![30, 10, 20]);

        let stream = RecordBatchStreamAdapter::new(
            Arc::clone(&schema),
            stream::iter(vec![Ok(batch.clone())]),
        );

        let context = create_task_context();
        let result =
            sort_stream(Box::pin(stream), &[], &context).expect("should return original stream");

        let batches: Vec<RecordBatch> = datafusion::physical_plan::common::collect(result)
            .await
            .expect("collect batches");

        assert_eq!(batches.len(), 1);
        // Data should be unchanged
        let ids = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("ids");
        assert_eq!(ids.values(), &[3, 1, 2]);
    }

    #[tokio::test]
    async fn test_sort_stream_invalid_column_returns_original() {
        let schema = create_test_schema();
        let batch = create_test_batch(vec![3, 1, 2], vec!["c", "a", "b"], vec![30, 10, 20]);

        let stream = RecordBatchStreamAdapter::new(
            Arc::clone(&schema),
            stream::iter(vec![Ok(batch.clone())]),
        );

        let context = create_task_context();
        let result = sort_stream(
            Box::pin(stream),
            &["nonexistent_column".to_string()],
            &context,
        )
        .expect("should return original stream");

        let batches: Vec<RecordBatch> = datafusion::physical_plan::common::collect(result)
            .await
            .expect("collect batches");

        assert_eq!(batches.len(), 1);
        // Data should be unchanged
        let ids = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("ids");
        assert_eq!(ids.values(), &[3, 1, 2]);
    }

    #[tokio::test]
    async fn test_sort_stream_multiple_batches() {
        let schema = create_test_schema();

        let batch1 = create_test_batch(vec![3, 1], vec!["c", "a"], vec![30, 10]);
        let batch2 = create_test_batch(vec![4, 2], vec!["d", "b"], vec![40, 20]);

        let stream = RecordBatchStreamAdapter::new(
            Arc::clone(&schema),
            stream::iter(vec![Ok(batch1), Ok(batch2)]),
        );

        let context = create_task_context();
        let sorted = sort_stream(Box::pin(stream), &["id".to_string()], &context)
            .expect("sort should succeed");

        let batches: Vec<RecordBatch> = datafusion::physical_plan::common::collect(sorted)
            .await
            .expect("collect batches");

        // All data from both batches should be sorted together
        let mut all_ids = Vec::new();
        for batch in &batches {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("ids");
            all_ids.extend_from_slice(ids.values());
        }

        assert_eq!(all_ids, vec![1, 2, 3, 4]);
    }

    #[tokio::test]
    async fn test_sort_stream_large_dataset() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Int32, false),
        ]));

        // Create a large dataset (1000 rows) in reverse order
        let size = 1000;
        let ids: Vec<i32> = (0..size).rev().collect();
        let values: Vec<i32> = (0..size).rev().collect();

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(Int32Array::from(values)),
            ],
        )
        .expect("create batch");

        let stream =
            RecordBatchStreamAdapter::new(Arc::clone(&schema), stream::iter(vec![Ok(batch)]));

        let context = create_task_context();
        let sorted = sort_stream(Box::pin(stream), &["id".to_string()], &context)
            .expect("sort should succeed");

        let batches: Vec<RecordBatch> = datafusion::physical_plan::common::collect(sorted)
            .await
            .expect("collect batches");

        let mut all_ids = Vec::new();
        for batch in &batches {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("ids");
            all_ids.extend_from_slice(ids.values());
        }

        // Should be sorted in ascending order
        let expected: Vec<i32> = (0..size).collect();
        assert_eq!(all_ids, expected);
    }

    #[test]
    fn test_parse_sort_entry_forms() {
        use arrow::compute::SortOptions;

        // Bare column: ascending, NULLs last.
        assert_eq!(
            parse_sort_entry("id"),
            Some((
                "id",
                SortOptions {
                    descending: false,
                    nulls_first: false
                }
            ))
        );
        // Direction only: DESC defaults to NULLs first (SQL ORDER BY convention).
        assert_eq!(
            parse_sort_entry("ts DESC"),
            Some((
                "ts",
                SortOptions {
                    descending: true,
                    nulls_first: true
                }
            ))
        );
        assert_eq!(
            parse_sort_entry("ts asc"),
            Some((
                "ts",
                SortOptions {
                    descending: false,
                    nulls_first: false
                }
            ))
        );
        // Explicit NULLS placement, with and without a direction.
        assert_eq!(
            parse_sort_entry("ts DESC NULLS LAST"),
            Some((
                "ts",
                SortOptions {
                    descending: true,
                    nulls_first: false
                }
            ))
        );
        assert_eq!(
            parse_sort_entry("ts nulls first"),
            Some((
                "ts",
                SortOptions {
                    descending: false,
                    nulls_first: true
                }
            ))
        );
        // Invalid trailing tokens are rejected.
        assert_eq!(parse_sort_entry("ts SIDEWAYS"), None);
        assert_eq!(parse_sort_entry("ts DESC NULLS"), None);
        assert_eq!(parse_sort_entry("ts DESC NULLS SOMETIMES"), None);
        assert_eq!(parse_sort_entry(""), None);
    }

    #[tokio::test]
    async fn test_sort_stream_with_direction_entries() {
        let schema = create_test_schema();
        let batch = create_test_batch(
            vec![3, 1, 4, 2],
            vec!["c", "a", "d", "b"],
            vec![30, 10, 40, 20],
        );

        let stream =
            RecordBatchStreamAdapter::new(Arc::clone(&schema), stream::iter(vec![Ok(batch)]));

        let context = create_task_context();
        let sorted = sort_stream(
            Box::pin(stream),
            &["id DESC NULLS LAST".to_string()],
            &context,
        )
        .expect("sort should succeed");

        let batches: Vec<RecordBatch> = datafusion::physical_plan::common::collect(sorted)
            .await
            .expect("collect batches");

        assert_eq!(batches.len(), 1);
        let ids = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("ids column");
        assert_eq!(ids.values(), &[4, 3, 2, 1]);
    }
}
