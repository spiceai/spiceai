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

//! Iceberg delete support.
//!
//! `DELETE FROM` currently fails with a structured `DataFusion` error because
//! Iceberg 0.9.0 no longer exposes the `RowDeltaAction` API needed to commit
//! equality delete files safely.

use std::any::Any;
use std::borrow::Cow;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef};
use async_trait::async_trait;
use datafusion::catalog::{ScanArgs, ScanResult, Session};
use datafusion::common::{
    Constraints, DataFusionError, Result as DFResult, Statistics, ToDFSchema, tree_node::TreeNode,
};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::table::Table;
use iceberg::{Catalog, Error as IcebergError};

fn to_df_error(e: IcebergError) -> DataFusionError {
    DataFusionError::External(Box::new(e))
}

/// Execution plan that scans matching rows, writes equality delete files,
/// and commits them via `RowDeltaAction`.
///
/// Output schema: single column `count` (`UInt64`) with the number of deleted rows.
pub(crate) struct IcebergDeleteExec {
    table: Table,
    catalog: Arc<dyn Catalog>,
    /// The child plan that produces the rows to delete (a scan with filters applied).
    /// The scan is projected to only include columns eligible for equality deletes.
    input: Arc<dyn ExecutionPlan>,
    /// Pre-computed equality delete field IDs (primitive, non-float columns).
    equality_ids: Vec<i32>,
    plan_properties: Arc<PlanProperties>,
}

impl IcebergDeleteExec {
    pub fn new(
        table: Table,
        catalog: Arc<dyn Catalog>,
        input: Arc<dyn ExecutionPlan>,
        equality_ids: Vec<i32>,
    ) -> Self {
        let count_schema = Self::make_count_schema();
        let plan_properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&count_schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        ));

        Self {
            table,
            catalog,
            input,
            equality_ids,
            plan_properties,
        }
    }

    fn make_count_schema() -> ArrowSchemaRef {
        Arc::new(ArrowSchema::new(vec![Field::new(
            "count",
            DataType::UInt64,
            false,
        )]))
    }
}

impl Debug for IcebergDeleteExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IcebergDeleteExec")
            .field("table", &self.table.identifier().to_string())
            .finish_non_exhaustive()
    }
}

impl DisplayAs for IcebergDeleteExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(f, "IcebergDeleteExec: table={}", self.table.identifier())
            }
        }
    }
}

impl ExecutionPlan for IcebergDeleteExec {
    fn name(&self) -> &'static str {
        "IcebergDeleteExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.plan_properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn required_input_distribution(&self) -> Vec<datafusion::physical_plan::Distribution> {
        vec![datafusion::physical_plan::Distribution::SinglePartition]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(format!(
                "IcebergDeleteExec expects exactly one child, got {}",
                children.len()
            )));
        }
        Ok(Arc::new(IcebergDeleteExec::new(
            self.table.clone(),
            Arc::clone(&self.catalog),
            Arc::clone(&children[0]),
            self.equality_ids.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "IcebergDeleteExec only supports partition 0, got {partition}"
            )));
        }

        Err(DataFusionError::NotImplemented(
            "DELETE FROM Iceberg tables is not supported with the current Iceberg dependency because committing equality delete files requires RowDeltaAction, which is unavailable in Iceberg 0.9.0".to_string(),
        ))
    }
}

/// Wrapper that makes an `IcebergTableProvider` support `DELETE FROM`.
///
/// This is registered as the table provider when the Iceberg data connector
/// supports writes, enabling `DELETE FROM` SQL statements.
pub struct IcebergDeletionProvider {
    catalog: Arc<dyn Catalog>,
    table_ident: iceberg::TableIdent,
    inner: Arc<dyn datafusion::datasource::TableProvider>,
}

impl IcebergDeletionProvider {
    /// Create a new deletion-capable wrapper around an `IcebergTableProvider`.
    pub fn new(
        catalog: Arc<dyn Catalog>,
        namespace: iceberg::NamespaceIdent,
        table_name: String,
        inner: Arc<dyn datafusion::datasource::TableProvider>,
    ) -> Self {
        let table_ident = iceberg::TableIdent::new(namespace, table_name);
        Self {
            catalog,
            table_ident,
            inner,
        }
    }
}

impl Debug for IcebergDeletionProvider {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IcebergDeletionProvider")
            .field("table_ident", &self.table_ident.to_string())
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl datafusion::datasource::TableProvider for IcebergDeletionProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        self.inner.schema()
    }

    fn constraints(&self) -> Option<&Constraints> {
        self.inner.constraints()
    }

    fn table_type(&self) -> datafusion::datasource::TableType {
        self.inner.table_type()
    }

    fn get_table_definition(&self) -> Option<&str> {
        self.inner.get_table_definition()
    }

    fn get_logical_plan(&'_ self) -> Option<Cow<'_, LogicalPlan>> {
        self.inner.get_logical_plan()
    }

    fn get_column_default(&self, column: &str) -> Option<&datafusion::logical_expr::Expr> {
        self.inner.get_column_default(column)
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&datafusion::logical_expr::Expr],
    ) -> DFResult<Vec<datafusion::logical_expr::TableProviderFilterPushDown>> {
        self.inner.supports_filters_pushdown(filters)
    }

    fn statistics(&self) -> Option<Statistics> {
        self.inner.statistics()
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[datafusion::logical_expr::Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        self.inner.scan(state, projection, filters, limit).await
    }

    async fn scan_with_args<'a>(
        &self,
        state: &dyn Session,
        args: ScanArgs<'a>,
    ) -> DFResult<ScanResult> {
        self.inner.scan_with_args(state, args).await
    }

    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        overwrite: datafusion::logical_expr::dml::InsertOp,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        self.inner.insert_into(state, input, overwrite).await
    }

    async fn delete_from(
        &self,
        state: &dyn Session,
        filters: Vec<datafusion::logical_expr::Expr>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        self.delete_from_impl(state, &filters).await
    }
}

impl IcebergDeletionProvider {
    async fn delete_from_impl(
        &self,
        state: &dyn Session,
        filters: &[datafusion::logical_expr::Expr],
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        // Load fresh table metadata
        let table = self
            .catalog
            .load_table(&self.table_ident)
            .await
            .map_err(to_df_error)?;

        // Verify format version supports deletes
        if table.metadata().format_version() == iceberg::spec::FormatVersion::V1 {
            return Err(DataFusionError::Plan(
                "DELETE is not supported on Iceberg v1 tables. Upgrade to v2 format.".to_string(),
            ));
        }

        // Derive the equality_ids and scan projection up front. Only primitive
        // non-floating-point columns are eligible for equality deletes per the
        // Iceberg spec. We compute them here so the scan can be projected to
        // only those columns, avoiding reads of float/nested columns that the
        // Iceberg reader cannot resolve by field-id.
        let iceberg_schema = table.metadata().current_schema();
        let arrow_schema = Arc::new(schema_to_arrow_schema(iceberg_schema).map_err(to_df_error)?);

        // Compute (column_index, field_id) pairs for eligible equality columns
        let mut equality_ids: Vec<i32> = Vec::new();
        let mut projection_indices: Vec<usize> = Vec::new();
        for (idx, field) in arrow_schema.fields().iter().enumerate() {
            if field.data_type().is_nested()
                || matches!(
                    field.data_type(),
                    arrow::datatypes::DataType::Float16
                        | arrow::datatypes::DataType::Float32
                        | arrow::datatypes::DataType::Float64
                )
            {
                continue;
            }
            if let Some(field_id) = field
                .metadata()
                .get(parquet::arrow::PARQUET_FIELD_ID_META_KEY)
                .and_then(|v| v.parse::<i32>().ok())
            {
                equality_ids.push(field_id);
                projection_indices.push(idx);
            }
        }

        tracing::debug!(
            table = %table.identifier(),
            schema_field_count = arrow_schema.fields().len(),
            equality_id_count = equality_ids.len(),
            ?equality_ids,
            "Computed equality delete IDs for scan projection"
        );

        // Scan only the equality-eligible columns. This avoids reading
        // float/nested columns that cause field-id resolution errors.
        let scan_plan = self
            .inner
            .scan(state, Some(&projection_indices), filters, None)
            .await?;

        // The Iceberg provider may not push down filters, so add a FilterExec
        // on top of the scan to ensure only matching rows are processed.
        // Filter column references must be unqualified to match the scan
        // output schema (which uses bare column names, not table-qualified).
        let filtered_plan = if filters.is_empty() {
            scan_plan
        } else {
            let scan_schema = scan_plan.schema();
            let df_schema = scan_schema.to_dfschema_ref()?;
            let unqualified_filters: Vec<datafusion::logical_expr::Expr> = filters
                .iter()
                .map(|expr| {
                    expr.clone()
                        .transform(|e| {
                            if let datafusion::logical_expr::Expr::Column(mut col) = e {
                                col.relation = None;
                                Ok(datafusion::common::tree_node::Transformed::yes(
                                    datafusion::logical_expr::Expr::Column(col),
                                ))
                            } else {
                                Ok(datafusion::common::tree_node::Transformed::no(e))
                            }
                        })
                        .map(|t| t.data)
                })
                .collect::<DFResult<Vec<_>>>()?;
            let combined_filter = unqualified_filters
                .into_iter()
                .reduce(datafusion::prelude::Expr::and)
                .ok_or_else(|| {
                    DataFusionError::Internal("Filter list unexpectedly empty".to_string())
                })?;
            let physical_filter = datafusion::physical_expr::create_physical_expr(
                &combined_filter,
                &df_schema,
                state.execution_props(),
            )?;
            Arc::new(datafusion::physical_plan::filter::FilterExec::try_new(
                physical_filter,
                scan_plan,
            )?) as Arc<dyn ExecutionPlan>
        };

        // Coalesce into a single partition for the delete writer
        let coalesced = Arc::new(
            datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec::new(
                filtered_plan,
            ),
        );

        Ok(Arc::new(IcebergDeleteExec::new(
            table,
            Arc::clone(&self.catalog),
            coalesced,
            equality_ids,
        )))
    }
}
