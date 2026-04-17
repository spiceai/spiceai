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

//! Position-tracking wrappers for MERGE fast-path deletion.
//!
//! Adds `__cayenne_file_path` + `__cayenne_row_idx` metadata columns to scan
//! output so MERGE can delete by pre-computed positions and avoid rebuilding an
//! O(N) filter expression.

use std::any::Any;
use std::borrow::Cow;
use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::{ScanArgs, ScanResult, Session, TableProvider};
use datafusion::config::ConfigOptions;
use datafusion::datasource::source::DataSourceExec;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{Expr, LogicalPlan, TableProviderFilterPushDown, TableType};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{InvariantLevel, check_default_invariants};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SortOrderPushdownResult,
};
use datafusion_datasource::file_scan_config::FileScanConfig;
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_expr::{Distribution, OrderingRequirements, PhysicalExpr};
use datafusion_physical_plan::execution_plan::CardinalityEffect;
use datafusion_physical_plan::expressions::PhysicalSortExpr;
use datafusion_physical_plan::filter_pushdown::{
    ChildPushdownResult, FilterDescription, FilterPushdownPhase, FilterPushdownPropagation,
};
use datafusion_physical_plan::metrics::MetricsSet;
use datafusion_physical_plan::projection::ProjectionExec;
use futures::StreamExt;

/// Metadata column containing source data-file path.
pub(crate) const CAYENNE_FILE_PATH_COLUMN: &str = "__cayenne_file_path";
/// Metadata column containing file-local row index.
pub(crate) const CAYENNE_ROW_IDX_COLUMN: &str = "__cayenne_row_idx";

/// Wraps a table provider and appends position-tracking metadata columns.
#[derive(Debug)]
pub(crate) struct CayennePositionTrackingTable {
    inner: Arc<dyn TableProvider>,
    schema: SchemaRef,
    inner_schema_len: usize,
}

impl CayennePositionTrackingTable {
    /// Create a position-tracking wrapper around `inner`.
    pub(crate) fn try_new(inner: Arc<dyn TableProvider>) -> DFResult<Self> {
        let inner_schema = inner.schema();
        if inner_schema
            .fields()
            .iter()
            .any(|f| f.name() == CAYENNE_FILE_PATH_COLUMN || f.name() == CAYENNE_ROW_IDX_COLUMN)
        {
            return Err(DataFusionError::Internal(
                "Position tracking metadata columns already exist in table schema".to_string(),
            ));
        }

        let schema = extend_schema(&inner_schema);
        Ok(Self {
            inner,
            schema,
            inner_schema_len: inner_schema.fields().len(),
        })
    }

    fn translate_projection(
        &self,
        projection: Option<&Vec<usize>>,
    ) -> DFResult<Option<Vec<usize>>> {
        let Some(projection) = projection else {
            return Ok(None);
        };

        if projection
            .iter()
            .any(|idx| *idx >= self.inner_schema_len + 2)
        {
            return Err(DataFusionError::Internal(format!(
                "Projection index out of bounds for position-tracking schema: max={}, projection={projection:?}",
                self.inner_schema_len + 1
            )));
        }

        let translated: Vec<usize> = projection
            .iter()
            .copied()
            .filter(|idx| *idx < self.inner_schema_len)
            .collect();
        Ok(Some(translated))
    }
}

#[async_trait]
impl TableProvider for CayennePositionTrackingTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn constraints(&self) -> Option<&datafusion_common::Constraints> {
        self.inner.constraints()
    }

    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    fn get_table_definition(&self) -> Option<&str> {
        self.inner.get_table_definition()
    }

    fn get_logical_plan(&self) -> Option<Cow<'_, LogicalPlan>> {
        self.inner.get_logical_plan()
    }

    fn get_column_default(&self, column: &str) -> Option<&Expr> {
        self.inner.get_column_default(column)
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let translated_projection = self.translate_projection(projection)?;
        let inner_plan = self
            .inner
            .scan(state, translated_projection.as_ref(), filters, limit)
            .await?;

        let projected_schema = if let Some(proj) = projection {
            Arc::new(self.schema.project(proj)?)
        } else {
            Arc::clone(&self.schema)
        };

        Ok(Arc::new(CayennePositionTrackingExec::try_new(
            inner_plan,
            projected_schema,
            self.inner_schema_len,
            projection.cloned(),
            translated_projection,
        )?))
    }

    async fn scan_with_args<'a>(
        &self,
        state: &dyn Session,
        args: ScanArgs<'a>,
    ) -> DFResult<ScanResult> {
        let filters = args.filters().unwrap_or(&[]);
        let projection = args.projection().map(|p| p.to_vec());
        let plan = self
            .scan(state, projection.as_ref(), filters, args.limit())
            .await?;
        Ok(plan.into())
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        self.inner.supports_filters_pushdown(filters)
    }

    fn statistics(&self) -> Option<datafusion_common::Statistics> {
        self.inner.statistics()
    }

    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        self.inner.insert_into(state, input, insert_op).await
    }

    async fn delete_from(
        &self,
        state: &dyn Session,
        filters: Vec<Expr>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        self.inner.delete_from(state, filters).await
    }

    async fn update(
        &self,
        state: &dyn Session,
        assignments: Vec<(String, Expr)>,
        filters: Vec<Expr>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        self.inner.update(state, assignments, filters).await
    }
}

#[derive(Debug)]
struct CayennePositionTrackingExec {
    inner: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
    inner_schema_len: usize,
    output_projection: Option<Vec<usize>>,
    translated_inner_projection: Option<Vec<usize>>,
    partition_file_paths: Arc<Vec<String>>,
    properties: PlanProperties,
}

impl CayennePositionTrackingExec {
    fn try_new(
        inner: Arc<dyn ExecutionPlan>,
        schema: SchemaRef,
        inner_schema_len: usize,
        output_projection: Option<Vec<usize>>,
        translated_inner_projection: Option<Vec<usize>>,
    ) -> DFResult<Self> {
        let partition_file_paths = Arc::new(extract_partition_file_paths(&inner)?);
        let partition_count = inner.properties().partitioning.partition_count();
        if partition_file_paths.len() != partition_count {
            return Err(DataFusionError::Internal(format!(
                "Position tracking partition mapping mismatch: expected {partition_count} paths, got {}",
                partition_file_paths.len()
            )));
        }

        let constraints = inner.properties().eq_properties.constraints().clone();
        let properties = inner.properties().clone().with_eq_properties(
            EquivalenceProperties::new(Arc::clone(&schema)).with_constraints(constraints),
        );

        Ok(Self {
            inner,
            schema,
            inner_schema_len,
            output_projection,
            translated_inner_projection,
            partition_file_paths,
            properties,
        })
    }
}

impl DisplayAs for CayennePositionTrackingExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "CayennePositionTrackingExec")
    }
}

impl ExecutionPlan for CayennePositionTrackingExec {
    fn name(&self) -> &'static str {
        "CayennePositionTrackingExec"
    }

    fn static_name() -> &'static str
    where
        Self: Sized,
    {
        "CayennePositionTrackingExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn check_invariants(&self, check: InvariantLevel) -> DFResult<()> {
        check_default_invariants(self, check)
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::UnspecifiedDistribution; self.children().len()]
    }

    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        vec![None; self.children().len()]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true; self.children().len()]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.inner]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(format!(
                "CayennePositionTrackingExec requires exactly one child, got {}",
                children.len()
            )));
        }

        let Some(child) = children.into_iter().next() else {
            unreachable!("validated children length above");
        };

        Ok(Arc::new(Self::try_new(
            child,
            Arc::clone(&self.schema),
            self.inner_schema_len,
            self.output_projection.clone(),
            self.translated_inner_projection.clone(),
        )?))
    }

    fn reset_state(self: Arc<Self>) -> DFResult<Arc<dyn ExecutionPlan>> {
        let children = self.children().into_iter().cloned().collect();
        self.with_new_children(children)
    }

    fn repartitioned(
        &self,
        target_partitions: usize,
        config: &ConfigOptions,
    ) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
        let repartitioned = self.inner.repartitioned(target_partitions, config)?;
        if let Some(plan) = repartitioned {
            return Ok(Some(Arc::new(Self::try_new(
                plan,
                Arc::clone(&self.schema),
                self.inner_schema_len,
                self.output_projection.clone(),
                self.translated_inner_projection.clone(),
            )?)));
        }
        Ok(None)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let file_path = self.partition_file_paths.get(partition).ok_or_else(|| {
            DataFusionError::Internal(format!(
                "Missing source file path for partition {partition} (known: {})",
                self.partition_file_paths.len()
            ))
        })?;

        let inner_stream = self.inner.execute(partition, context)?;
        let schema = Arc::clone(&self.schema);
        let schema_for_projection = Arc::clone(&schema);
        let file_path = file_path.clone();
        let output_projection = self.output_projection.clone();
        let translated_inner_projection = self.translated_inner_projection.clone();
        let inner_schema_len = self.inner_schema_len;

        let mut row_idx_offset: u64 = 0;
        let mapped = inner_stream.map(move |batch_result| {
            let batch = batch_result?;

            let rows = batch.num_rows();
            let rows_u64 = u64::try_from(rows).map_err(|_| {
                DataFusionError::Internal("Batch row count does not fit into u64".to_string())
            })?;

            let path_col: ArrayRef = Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
                file_path.as_str(),
                rows,
            )));

            let end = row_idx_offset.checked_add(rows_u64).ok_or_else(|| {
                DataFusionError::Internal("Row index overflow during position tracking".to_string())
            })?;
            let row_idx_col: ArrayRef =
                Arc::new(UInt64Array::from_iter_values(row_idx_offset..end));
            row_idx_offset = end;

            project_batch(
                &batch,
                Arc::clone(&schema_for_projection),
                output_projection.as_deref(),
                translated_inner_projection.as_deref(),
                inner_schema_len,
                path_col,
                row_idx_col,
            )
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, mapped)))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        self.inner.metrics()
    }

    fn statistics(&self) -> DFResult<datafusion_common::Statistics> {
        #[expect(deprecated)]
        self.inner.statistics()
    }

    fn partition_statistics(
        &self,
        partition: Option<usize>,
    ) -> DFResult<datafusion_common::Statistics> {
        self.inner.partition_statistics(partition)
    }

    fn supports_limit_pushdown(&self) -> bool {
        self.inner.supports_limit_pushdown()
    }

    fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        self.inner.with_fetch(limit).and_then(|plan| {
            Self::try_new(
                plan,
                Arc::clone(&self.schema),
                self.inner_schema_len,
                self.output_projection.clone(),
                self.translated_inner_projection.clone(),
            )
            .ok()
            .map(|wrapped| Arc::new(wrapped) as Arc<dyn ExecutionPlan>)
        })
    }

    fn fetch(&self) -> Option<usize> {
        self.inner.fetch()
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::Equal
    }

    fn try_swapping_with_projection(
        &self,
        _projection: &ProjectionExec,
    ) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
        Ok(None)
    }

    fn gather_filters_for_pushdown(
        &self,
        _phase: FilterPushdownPhase,
        parent_filters: Vec<Arc<dyn PhysicalExpr>>,
        _config: &ConfigOptions,
    ) -> DFResult<FilterDescription> {
        FilterDescription::from_children(parent_filters, &self.children())
    }

    fn handle_child_pushdown_result(
        &self,
        _phase: FilterPushdownPhase,
        child_pushdown_result: ChildPushdownResult,
        _config: &ConfigOptions,
    ) -> DFResult<FilterPushdownPropagation<Arc<dyn ExecutionPlan>>> {
        Ok(FilterPushdownPropagation::if_all(child_pushdown_result))
    }

    fn with_new_state(&self, _state: Arc<dyn Any + Send + Sync>) -> Option<Arc<dyn ExecutionPlan>> {
        None
    }

    fn try_pushdown_sort(
        &self,
        order: &[PhysicalSortExpr],
    ) -> DFResult<SortOrderPushdownResult<Arc<dyn ExecutionPlan>>> {
        let result = self.inner.try_pushdown_sort(order)?;
        result.try_map(|plan| {
            Self::try_new(
                plan,
                Arc::clone(&self.schema),
                self.inner_schema_len,
                self.output_projection.clone(),
                self.translated_inner_projection.clone(),
            )
            .map(|wrapped| Arc::new(wrapped) as Arc<dyn ExecutionPlan>)
        })
    }
}

fn project_batch(
    batch: &RecordBatch,
    schema: SchemaRef,
    output_projection: Option<&[usize]>,
    translated_inner_projection: Option<&[usize]>,
    inner_schema_len: usize,
    file_path_col: ArrayRef,
    row_idx_col: ArrayRef,
) -> DFResult<RecordBatch> {
    let inner_batch_index = |original_inner_idx: usize| -> DFResult<usize> {
        if let Some(projection) = translated_inner_projection {
            projection
                .iter()
                .position(|idx| *idx == original_inner_idx)
                .ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "Inner projection missing requested column index {original_inner_idx}"
                    ))
                })
        } else {
            Ok(original_inner_idx)
        }
    };

    let output_columns = if let Some(projection) = output_projection {
        let mut cols: Vec<ArrayRef> = Vec::with_capacity(projection.len());
        for idx in projection {
            if *idx < inner_schema_len {
                let inner_idx = inner_batch_index(*idx)?;
                cols.push(Arc::clone(batch.column(inner_idx)));
            } else if *idx == inner_schema_len {
                cols.push(Arc::clone(&file_path_col));
            } else if *idx == inner_schema_len + 1 {
                cols.push(Arc::clone(&row_idx_col));
            } else {
                return Err(DataFusionError::Internal(format!(
                    "Projection index out of bounds for position-tracking batch: {idx}"
                )));
            }
        }
        cols
    } else {
        let mut cols = batch.columns().to_vec();
        cols.push(file_path_col);
        cols.push(row_idx_col);
        cols
    };

    RecordBatch::try_new(schema, output_columns).map_err(DataFusionError::from)
}

fn extend_schema(inner_schema: &SchemaRef) -> SchemaRef {
    let mut fields = inner_schema.fields().iter().cloned().collect::<Vec<_>>();
    fields.push(Arc::new(Field::new(
        CAYENNE_FILE_PATH_COLUMN,
        DataType::Utf8,
        false,
    )));
    fields.push(Arc::new(Field::new(
        CAYENNE_ROW_IDX_COLUMN,
        DataType::UInt64,
        false,
    )));
    Arc::new(Schema::new_with_metadata(
        fields,
        inner_schema.metadata().clone(),
    ))
}

fn extract_partition_file_paths(plan: &Arc<dyn ExecutionPlan>) -> DFResult<Vec<String>> {
    if let Some(data_source_exec) = plan.as_any().downcast_ref::<DataSourceExec>()
        && let Some(file_scan_config) = data_source_exec
            .data_source()
            .as_any()
            .downcast_ref::<FileScanConfig>()
    {
        let mut paths = Vec::with_capacity(file_scan_config.file_groups.len());
        for (partition_idx, file_group) in file_scan_config.file_groups.iter().enumerate() {
            if file_group.len() != 1 {
                return Err(DataFusionError::Internal(format!(
                    "Position tracking requires exactly 1 file per partition, found {} in partition {partition_idx}",
                    file_group.len()
                )));
            }
            let Some(file) = file_group.iter().next() else {
                return Err(DataFusionError::Internal(format!(
                    "Position tracking found empty file group for partition {partition_idx}"
                )));
            };
            paths.push(file.path().to_string());
        }
        return Ok(paths);
    }

    let children = plan.children();
    if children.is_empty() {
        return Err(DataFusionError::Internal(format!(
            "Position tracking could not locate FileScanConfig under execution plan '{}'",
            plan.name()
        )));
    }

    if children.len() == 1 {
        return extract_partition_file_paths(children[0]);
    }

    if matches!(plan.name(), "UnionExec" | "PartitionedUnionExec") {
        let mut merged = Vec::new();
        for child in children {
            merged.extend(extract_partition_file_paths(child)?);
        }
        return Ok(merged);
    }

    Err(DataFusionError::Internal(format!(
        "Position tracking does not support multi-input scan node '{}'",
        plan.name()
    )))
}
