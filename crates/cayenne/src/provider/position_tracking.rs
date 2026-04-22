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

//! Position-tracking scan support for `MERGE INTO` on Cayenne `PositionBased` tables.
//!
//! This module wraps Cayenne table scans with two additional columns:
//! - [`POSITION_FILE_PATH_COLUMN`] containing the source file path
//! - [`POSITION_ROW_IDX_COLUMN`] containing the source file's physical row index
//!
//! The row index must come from Vortex's native `row_idx()` expression rather than
//! from DataFusion stream order. Delete vectors target physical file positions, so
//! deriving row ids by counting visible rows after scan/filter/projection is unsafe.

use std::any::Any;
use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch, StringBuilder, StructArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use data_components::delete::DeletionTableProviderAdapter;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::execution_plan::{
    Boundedness, CardinalityEffect, EmissionType, InvariantLevel, check_default_invariants,
};
use datafusion::physical_plan::limit::GlobalLimitExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use datafusion_catalog::Session;
use datafusion_common::{Constraints, Statistics};
use datafusion_datasource::file_groups::FileGroup;
use datafusion_execution::TaskContext;
use datafusion_execution::object_store::ObjectStoreUrl;
use datafusion_expr::{Expr, LogicalPlan, TableProviderFilterPushDown, TableType};
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr::{EquivalenceProperties, Partitioning, PhysicalExpr};
use futures::FutureExt;
use object_store::ObjectStore;
use roaring::{RoaringBitmap, RoaringTreemap};
#[cfg(feature = "partition-table-provider")]
use runtime_table_partition::provider::PartitionTableProvider;
use vortex::VortexSessionDefault;
use vortex::array::arrow::IntoArrowArray;
use vortex::dtype::Nullability::NonNullable;
use vortex::expr::{col, pack};
use vortex::file::OpenOptionsSessionExt;
use vortex::layout::layouts::row_idx::row_idx;
use vortex::session::VortexSession;
use vortex_scan::Selection;

use crate::provider::CayenneTableProvider;

pub(crate) const POSITION_FILE_PATH_COLUMN: &str = "__cayenne_file_path";
pub(crate) const POSITION_ROW_IDX_COLUMN: &str = "__cayenne_row_idx";

#[derive(Debug)]
pub(crate) struct CayennePositionTrackingTable {
    inner: Arc<dyn TableProvider>,
    schema: SchemaRef,
}

impl CayennePositionTrackingTable {
    /// Create a position-tracking wrapper for a Cayenne-backed target provider.
    ///
    /// # Errors
    ///
    /// Returns an error if the target schema already contains one of the reserved
    /// position-tracking column names.
    pub(crate) fn try_new(inner: Arc<dyn TableProvider>) -> DFResult<Self> {
        let inner_schema = inner.schema();
        for reserved_name in [POSITION_FILE_PATH_COLUMN, POSITION_ROW_IDX_COLUMN] {
            if inner_schema.column_with_name(reserved_name).is_some() {
                return Err(DataFusionError::Plan(format!(
                    "Cayenne position tracking column '{reserved_name}' conflicts with an existing target column"
                )));
            }
        }

        let schema = tracked_schema(&inner_schema);
        Ok(Self { inner, schema })
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

    fn constraints(&self) -> Option<&Constraints> {
        self.inner.constraints()
    }

    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion_common::Result<Vec<TableProviderFilterPushDown>> {
        self.inner.supports_filters_pushdown(filters)
    }

    fn statistics(&self) -> Option<Statistics> {
        None
    }

    fn get_table_definition(&self) -> Option<&str> {
        self.inner.get_table_definition()
    }

    fn get_logical_plan(&self) -> Option<Cow<'_, LogicalPlan>> {
        self.inner.get_logical_plan()
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        let plan = position_tracking_scan_for_provider(&self.inner, state, filters, limit).await?;

        let plan = if let Some(projection) = projection {
            apply_projection(plan, &self.schema, projection)?
        } else {
            plan
        };

        Ok(plan)
    }

    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: datafusion::logical_expr::dml::InsertOp,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        self.inner.insert_into(state, input, insert_op).await
    }

    async fn delete_from(
        &self,
        state: &dyn Session,
        filters: Vec<Expr>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        self.inner.delete_from(state, filters).await
    }

    async fn update(
        &self,
        state: &dyn Session,
        assignments: Vec<(String, Expr)>,
        filters: Vec<Expr>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        self.inner.update(state, assignments, filters).await
    }
}

pub(crate) async fn is_position_based_cayenne(provider: &Arc<dyn TableProvider>) -> bool {
    if let Some(cayenne) = unwrap_to_cayenne(provider) {
        return cayenne.is_position_based();
    }

    #[cfg(feature = "partition-table-provider")]
    if let Some(partitioned) = provider.as_any().downcast_ref::<PartitionTableProvider>() {
        let providers: Vec<Arc<dyn TableProvider>> = partitioned.partition_table_providers().await;
        return !providers.is_empty()
            && providers.iter().all(|provider| {
                unwrap_to_cayenne(provider).is_some_and(CayenneTableProvider::is_position_based)
            });
    }

    false
}

pub(crate) fn unwrap_to_cayenne(
    provider: &Arc<dyn TableProvider>,
) -> Option<&CayenneTableProvider> {
    provider
        .as_any()
        .downcast_ref::<CayenneTableProvider>()
        .or_else(|| {
            provider
                .as_any()
                .downcast_ref::<DeletionTableProviderAdapter>()
                .and_then(|adapter| {
                    adapter
                        .source()
                        .as_any()
                        .downcast_ref::<CayenneTableProvider>()
                })
        })
}

fn position_tracking_scan_for_provider<'a>(
    provider: &'a Arc<dyn TableProvider>,
    state: &'a dyn Session,
    filters: &'a [Expr],
    limit: Option<usize>,
) -> futures::future::BoxFuture<'a, datafusion_common::Result<Arc<dyn ExecutionPlan>>> {
    async move {
        if let Some(cayenne) = unwrap_to_cayenne(provider) {
            return cayenne.scan_position_tracking(state, filters, limit).await;
        }

        #[cfg(feature = "partition-table-provider")]
        if let Some(partitioned) = provider.as_any().downcast_ref::<PartitionTableProvider>() {
            let partition_providers: Vec<Arc<dyn TableProvider>> =
                partitioned.partition_table_providers().await;
            let mut plans = Vec::with_capacity(partition_providers.len());
            for partition_provider in partition_providers {
                plans.push(
                    position_tracking_scan_for_provider(&partition_provider, state, filters, None)
                        .await?,
                );
            }

            let mut plan: Arc<dyn ExecutionPlan> = match plans.len() {
                0 => Arc::new(EmptyExec::new(tracked_schema(&partitioned.schema()))),
                1 => plans.pop().ok_or_else(|| {
                    DataFusionError::Execution("expected a partition execution plan".to_string())
                })?,
                _ => UnionExec::try_new(plans)?,
            };

            if let Some(limit) = limit {
                plan = Arc::new(GlobalLimitExec::new(plan, limit, None));
            }

            return Ok(plan);
        }

        Err(DataFusionError::Internal(
            "Position tracking is only supported for Cayenne table providers".to_string(),
        ))
    }
    .boxed()
}

fn apply_projection(
    input: Arc<dyn ExecutionPlan>,
    schema: &SchemaRef,
    projection: &[usize],
) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
    let mut projection_expr: Vec<(Arc<dyn PhysicalExpr>, String)> =
        Vec::with_capacity(projection.len());
    for &idx in projection {
        let field = schema.field(idx);
        let name = field.name().clone();
        projection_expr.push((
            Arc::new(Column::new(&name, idx)) as Arc<dyn PhysicalExpr>,
            name,
        ));
    }

    Ok(Arc::new(ProjectionExec::try_new(projection_expr, input)?))
}

pub(crate) fn tracked_schema(base_schema: &SchemaRef) -> SchemaRef {
    let mut fields: Vec<_> = base_schema.fields().iter().cloned().collect();
    fields.push(Arc::new(Field::new(
        POSITION_FILE_PATH_COLUMN,
        DataType::Utf8,
        false,
    )));
    fields.push(Arc::new(Field::new(
        POSITION_ROW_IDX_COLUMN,
        DataType::UInt64,
        false,
    )));

    Arc::new(Schema::new_with_metadata(
        fields,
        base_schema.metadata().clone(),
    ))
}

fn tracked_scan_schema(base_schema: &SchemaRef) -> SchemaRef {
    let mut fields: Vec<_> = base_schema.fields().iter().cloned().collect();
    fields.push(Arc::new(Field::new(
        POSITION_ROW_IDX_COLUMN,
        DataType::UInt64,
        false,
    )));

    Arc::new(Schema::new_with_metadata(
        fields,
        base_schema.metadata().clone(),
    ))
}

#[derive(Debug)]
pub(crate) struct CayennePositionTrackingExec {
    file_groups: Vec<FileGroup>,
    object_store_url: ObjectStoreUrl,
    base_schema: SchemaRef,
    deletion_snapshot: Arc<HashMap<String, RoaringBitmap>>,
    scan_schema: SchemaRef,
    schema: SchemaRef,
    properties: PlanProperties,
}

impl CayennePositionTrackingExec {
    #[must_use]
    pub(crate) fn new(
        file_groups: Vec<FileGroup>,
        object_store_url: ObjectStoreUrl,
        base_schema: SchemaRef,
        deletion_snapshot: Arc<HashMap<String, RoaringBitmap>>,
    ) -> Self {
        let scan_schema = tracked_scan_schema(&base_schema);
        let schema = tracked_schema(&base_schema);
        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(file_groups.len()),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );

        Self {
            file_groups,
            object_store_url,
            base_schema,
            deletion_snapshot,
            scan_schema,
            schema,
            properties,
        }
    }
}

impl DisplayAs for CayennePositionTrackingExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![]
    }

    fn required_input_distribution(&self) -> Vec<datafusion_physical_expr::Distribution> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(DataFusionError::Internal(format!(
                "CayennePositionTrackingExec requires zero children, got {}",
                children.len()
            )));
        }
        Ok(self)
    }

    fn reset_state(self: Arc<Self>) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion_common::Result<SendableRecordBatchStream> {
        let Some(file_group) = self.file_groups.get(partition).cloned() else {
            return Err(DataFusionError::Execution(format!(
                "CayennePositionTrackingExec partition {partition} out of range ({} partitions)",
                self.file_groups.len()
            )));
        };

        let output_schema = Arc::clone(&self.schema);
        let stream_schema = Arc::clone(&self.schema);
        let scan_schema = Arc::clone(&self.scan_schema);
        let base_schema = Arc::clone(&self.base_schema);
        let deletion_snapshot = Arc::clone(&self.deletion_snapshot);
        let object_store_url = self.object_store_url.clone();

        // Use async_stream here because we need to sequentially open files and then yield
        // multiple record batches per file while preserving backpressure.
        let stream = async_stream::try_stream! {
            let object_store: Arc<dyn ObjectStore> =
                context.runtime_env().object_store(&object_store_url)?;
            let vortex_session = VortexSession::default();
            let projection = build_vortex_projection(&base_schema);

            for file in file_group.into_inner() {
                let file_path = file.path().to_string();
                let vxf = vortex_session
                    .open_options()
                    .open_object_store(&object_store, &file_path)
                    .await
                    .map_err(|e| DataFusionError::Execution(format!(
                        "Failed to open Vortex file '{file_path}' for position tracking: {e}"
                    )))?;

                let mut scan_builder = vxf
                    .scan()
                    .map_err(|e| DataFusionError::Execution(format!(
                        "Failed to build Vortex scan for position tracking on '{file_path}': {e}"
                    )))?
                    .with_projection(projection.clone());

                if let Some(bitmap) = deletion_snapshot.get(&file_path)
                    && !bitmap.is_empty()
                {
                    let excluded_indices: RoaringTreemap = bitmap.iter().map(u64::from).collect();
                    scan_builder = scan_builder.with_selection(Selection::ExcludeRoaring(excluded_indices));
                }

                let mut stream = scan_builder
                    .into_stream()
                    .map_err(|e| DataFusionError::Execution(format!(
                        "Failed to start Vortex stream for position tracking on '{file_path}': {e}"
                    )))?;

                while let Some(chunk_result) = futures::StreamExt::next(&mut stream).await {
                    let chunk = chunk_result.map_err(|e| DataFusionError::Execution(format!(
                        "Failed reading Vortex chunk for position tracking from '{file_path}': {e}"
                    )))?;

                    let arrow_array: arrow::array::ArrayRef = chunk.into_arrow_preferred().map_err(|e| {
                        DataFusionError::Execution(format!(
                            "Failed converting Vortex chunk to Arrow for position tracking from '{file_path}': {e}"
                        ))
                    })?;

                    if arrow_array.is_empty() {
                        continue;
                    }

                    let struct_array = arrow_array
                        .as_any()
                        .downcast_ref::<StructArray>()
                        .ok_or_else(|| {
                            DataFusionError::Internal(format!(
                                "Position-tracking Vortex projection for '{file_path}' did not return a StructArray"
                            ))
                        })?;

                    let batch = RecordBatch::from(struct_array);
                    let batch = arrow_tools::record_batch::try_cast_to(batch, Arc::clone(&scan_schema))
                        .map_err(DataFusionError::from)?;

                    yield append_file_path_column(&batch, &output_schema, &file_path)?;
                }
            }
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            stream_schema,
            stream,
        )))
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::Equal
    }
}

fn build_vortex_projection(base_schema: &SchemaRef) -> vortex::expr::Expression {
    let mut fields: Vec<(String, vortex::expr::Expression)> = base_schema
        .fields()
        .iter()
        .map(|field| (field.name().clone(), col(field.name().clone())))
        .collect();
    fields.push((POSITION_ROW_IDX_COLUMN.to_string(), row_idx()));
    pack(fields, NonNullable)
}

fn append_file_path_column(
    batch: &RecordBatch,
    output_schema: &SchemaRef,
    file_path: &str,
) -> datafusion_common::Result<RecordBatch> {
    let mut file_paths = StringBuilder::new();
    for _ in 0..batch.num_rows() {
        file_paths.append_value(file_path);
    }
    let file_path_column: ArrayRef = Arc::new(file_paths.finish());

    let row_idx_column = batch
        .column_by_name(POSITION_ROW_IDX_COLUMN)
        .ok_or_else(|| {
            DataFusionError::Internal(format!(
                "Position-tracking scan output missing '{POSITION_ROW_IDX_COLUMN}' column"
            ))
        })?
        .clone();

    let mut columns: Vec<ArrayRef> = Vec::with_capacity(output_schema.fields().len());
    for field in output_schema.fields() {
        match field.name().as_str() {
            POSITION_FILE_PATH_COLUMN => columns.push(Arc::clone(&file_path_column)),
            POSITION_ROW_IDX_COLUMN => columns.push(Arc::clone(&row_idx_column)),
            name => {
                let column = batch.column_by_name(name).ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "Position-tracking scan output missing target column '{name}'"
                    ))
                })?;
                columns.push(Arc::clone(column));
            }
        }
    }

    RecordBatch::try_new(Arc::clone(output_schema), columns).map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray, UInt64Array};

    #[test]
    fn tracked_schema_appends_position_columns() {
        let base_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let schema = tracked_schema(&base_schema);

        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.field(1).name(), POSITION_FILE_PATH_COLUMN);
        assert_eq!(schema.field(2).name(), POSITION_ROW_IDX_COLUMN);
    }

    #[test]
    fn append_file_path_column_preserves_target_columns_and_row_idx() {
        let scan_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(POSITION_ROW_IDX_COLUMN, DataType::UInt64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&scan_schema),
            vec![
                Arc::new(Int64Array::from(vec![10, 20, 30])),
                Arc::new(UInt64Array::from(vec![5, 8, 13])),
            ],
        )
        .expect("valid batch");

        let output_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(POSITION_FILE_PATH_COLUMN, DataType::Utf8, false),
            Field::new(POSITION_ROW_IDX_COLUMN, DataType::UInt64, false),
        ]));

        let tracked = append_file_path_column(&batch, &output_schema, "file-a")
            .expect("file path column appended");

        let ids = tracked
            .column_by_name("id")
            .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
            .expect("id column");
        let file_paths = tracked
            .column_by_name(POSITION_FILE_PATH_COLUMN)
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .expect("file path column");
        let row_indices = tracked
            .column_by_name(POSITION_ROW_IDX_COLUMN)
            .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
            .expect("row idx column");

        assert_eq!(ids.values(), &[10, 20, 30]);
        assert_eq!(file_paths.value(0), "file-a");
        assert_eq!(file_paths.value(2), "file-a");
        assert_eq!(row_indices.values(), &[5, 8, 13]);
    }
}
