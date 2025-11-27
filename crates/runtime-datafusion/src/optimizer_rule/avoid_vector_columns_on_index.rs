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

use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
    sync::Arc,
};

use arrow::datatypes::Field;
use datafusion::{
    common::tree_node::Transformed,
    datasource::DefaultTableSource,
    error::DataFusionError,
    logical_expr::{Extension, LogicalPlan, Projection, TableScan},
    optimizer::{ApplyOrder, OptimizerRule},
    prelude::{Expr, col},
    scalar::ScalarValue,
    sql::TableReference,
};
use runtime_datafusion_index::{Index, IndexedTableProvider, analyzer::IndexTableScanNode};
use search::index::{VectorScanTableProvider, derived_columns_from_vector_index};

/// An [`OptimizerRule`] that, for any [`LogicalPlan`] with a [`IndexTableScanNode`] extension node, find all
/// [`VectorIndex`] derived columns and remove them from the underlying [`VectorScanTableProvider`] projection.
///
/// This avoids redundant calls to the [`VectorIndex`] during indexing.
///
/// The derived columns are then re-added as NULL literal expressions in a projection on top of the modified [`TableScan`].
pub struct AvoidDerivedVectorColumnOnIndexRule {}

impl Debug for AvoidDerivedVectorColumnOnIndexRule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AvoidDerivedVectorColumnOnIndexRule")
            .finish()
    }
}

impl AvoidDerivedVectorColumnOnIndexRule {
    /// Check if the given [`LogicalPlan`] is an [`IndexTableScanNode`] over a [`VectorScanTableProvider`].
    ///
    /// This will be nested like
    /// ```
    /// Extension(IndexTableScanNode)
    ///   └── TableScan(DefaultTableSource)
    ///       └── IndexedTableProvider
    ///          └── VectorScanTableProvider
    /// ```
    fn is_indexing_with_derived_vector_columns(
        plan: &LogicalPlan,
    ) -> Option<(&IndexedTableProvider, &TableScan)> {
        let LogicalPlan::Extension(ext) = plan else {
            return None;
        };
        let index_scan = ext.node.as_any().downcast_ref::<IndexTableScanNode>()?;

        let LogicalPlan::TableScan(table_scan) = index_scan.input() else {
            return None;
        };

        let default_table_source = table_scan
            .source
            .as_any()
            .downcast_ref::<DefaultTableSource>()?;

        let indexed_table_provider = default_table_source
            .table_provider
            .as_any()
            .downcast_ref::<IndexedTableProvider>()?;

        let _vector_scan_table = indexed_table_provider
            .get_underlying()
            .as_any()
            .downcast_ref::<VectorScanTableProvider>()?;

        Some((indexed_table_provider, table_scan))
    }

    /// For [`Index`] in [`IndexTableScanNode`], find all derived columns of [`VectorIndex`]s.
    fn derived_vector_index_columns(indexes: &[Arc<dyn Index + Send + Sync>]) -> Vec<String> {
        indexes
            .iter()
            .filter_map(derived_columns_from_vector_index)
            .flatten()
            .collect()
    }

    /// Rewrite the given [`TableScan`] to avoid projecting derived vector columns.
    ///
    /// The derived columns are removed from the projection, and then re-added as NULL literal expressions with matching Field types, relations and ordering.
    fn avoid_derived_vector_columns(
        derived: &[&String],
        index_scan: &IndexedTableProvider,
        table_scan: &TableScan,
    ) -> Result<LogicalPlan, DataFusionError> {
        let mut proj = match table_scan.projection.as_ref() {
            None => (0..table_scan.projected_schema.fields().len()).collect(),
            Some(p) => p.clone(),
        };

        // Collect derived column indices and their corresponding fields
        let mut derived_cols: HashMap<usize, (Option<TableReference>, Arc<Field>)> = derived
            .iter()
            .filter_map(|&d| {
                table_scan
                    .projected_schema
                    .index_of_column_by_name(None, d.as_str())
                    .map(|idx| {
                        let (tbl_ref, field) = table_scan.projected_schema.qualified_field(idx);
                        (idx, (tbl_ref.cloned(), Arc::new(field.clone())))
                    })
            })
            .collect();

        // Remove from the projection any of the derived columns
        let derived_indices: HashSet<usize> = derived_cols.keys().copied().collect();
        proj.retain(|p| !derived_indices.contains(p));

        let tbl_scan = TableScan::try_new(
            table_scan.table_name.clone(),
            Arc::clone(&table_scan.source),
            Some(proj),
            table_scan.filters.clone(),
            table_scan.fetch,
        )?;

        // Build projection maintaining original column order. For `derived` columns, add NULL literal expressions.
        // This ensures the output schema matches the original schema, and mimics the VectorIndex having no vectors.
        let scan_schema = Arc::clone(&tbl_scan.projected_schema);
        let mut scan_col_iter = scan_schema.columns().into_iter();
        let mut projections: Vec<Expr> = Vec::new();

        for i in 0..index_scan.get_underlying_ref().schema().fields().len() {
            if let Some((table_ref, field)) = derived_cols.remove(&i) {
                projections.push(
                    Expr::Literal(ScalarValue::try_from(field.data_type())?, None)
                        .alias_qualified(table_ref, field.name()),
                );
            } else if let Some(col_ref) = scan_col_iter.next() {
                projections.push(col(col_ref));
            }
        }

        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(IndexTableScanNode::new(
                LogicalPlan::Projection(Projection::try_new(
                    projections,
                    Arc::new(LogicalPlan::TableScan(tbl_scan)),
                )?),
                index_scan.get_all_indexes(),
            )),
        }))
    }
}

impl OptimizerRule for AvoidDerivedVectorColumnOnIndexRule {
    fn name(&self) -> &'static str {
        "avoid_derived_vector_columns_on_index"
    }
    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn datafusion::optimizer::OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>, DataFusionError> {
        let Some((indexed, table_scan)) = Self::is_indexing_with_derived_vector_columns(&plan)
        else {
            return Ok(Transformed::no(plan));
        };

        let derived_columns = Self::derived_vector_index_columns(&indexed.indexes);
        let projected_derived_columns: Vec<&String> = derived_columns
            .iter()
            .filter(|&c| {
                table_scan
                    .projected_schema
                    .has_column_with_unqualified_name(c)
            })
            .collect();

        if projected_derived_columns.is_empty() {
            return Ok(Transformed::no(plan));
        }

        Ok(Transformed::yes(Self::avoid_derived_vector_columns(
            &projected_derived_columns,
            indexed,
            table_scan,
        )?))
    }
}
