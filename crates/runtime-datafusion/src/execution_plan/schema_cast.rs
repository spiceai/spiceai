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

use arrow::datatypes::{Field, Schema, SchemaRef};
use arrow_tools::record_batch;
use async_stream::stream;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::{
    Statistics,
    tree_node::{Transformed, TreeNode},
};
use datafusion::config::ConfigOptions;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::{ConstExpr, EquivalenceProperties, OrderingRequirements};
use datafusion::physical_plan::execution_plan::{
    CardinalityEffect, InvariantLevel, check_default_invariants,
};
use datafusion::physical_plan::filter_pushdown::{
    ChildPushdownResult, FilterDescription, FilterPushdownPhase, FilterPushdownPropagation,
};
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, ExecutionPlanProperties,
    PhysicalExpr, PlanProperties, SortOrderPushdownResult,
    expressions::{Column, PhysicalSortExpr},
};
use futures::StreamExt;
use std::any::Any;
use std::clone::Clone;
use std::fmt;
use std::sync::Arc;

pub struct SchemaCastScanExec {
    input: Arc<dyn ExecutionPlan>,
    /// The target schema requested by the caller
    target_schema: SchemaRef,
    /// The actual output schema (target schema with nullability adjustments from input)
    output_schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl SchemaCastScanExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, schema: SchemaRef) -> Self {
        // Compute the actual output schema: iterate over target schema fields,
        // but adjust nullability based on input schema to avoid "non-nullable but contains null values" errors.
        let input_schema = input.schema();
        let output_schema = Arc::new(
            Schema::new(
                schema
                    .fields()
                    .iter()
                    .map(|target_field| {
                        if let Ok(input_field) = input_schema.field_with_name(target_field.name()) {
                            // Use target field but make it nullable if input is nullable
                            if input_field.is_nullable() && !target_field.is_nullable() {
                                Field::new(
                                    target_field.name(),
                                    target_field.data_type().clone(),
                                    true, // Make nullable to match input
                                )
                                .with_metadata(target_field.metadata().clone())
                            } else {
                                target_field.as_ref().clone()
                            }
                        } else {
                            target_field.as_ref().clone()
                        }
                    })
                    .collect::<Vec<Field>>(),
            )
            .with_metadata(schema.metadata().clone()),
        );

        let eq_properties =
            Self::output_equivalence_properties(&input, &input_schema, &output_schema);
        let emission_type = input.pipeline_behavior();
        let boundedness = input.boundedness();
        let properties = Arc::new(PlanProperties::new(
            eq_properties,
            input.output_partitioning().clone(),
            emission_type,
            boundedness,
        ));
        Self {
            input,
            target_schema: schema,
            output_schema,
            properties,
        }
    }

    /// The equivalence properties this exec advertises, derived from its input's.
    ///
    /// This exec casts values in place, so it reports `maintains_input_order` and
    /// `CardinalityEffect::Equal`. Both make `EnforceSorting` push an ordering
    /// requirement *through* it to the child and drop the sort once the child
    /// satisfies the requirement — so every property the child used to satisfy it
    /// has to survive into what this exec advertises, not just the child's output
    /// ordering. An equality filter below (`WHERE pk = ?`) satisfies `ORDER BY pk`
    /// by making `pk` constant rather than by any ordering, and a constant that
    /// stops here leaves the `SortPreservingMergeExec` above with an unordered
    /// child, which `SanityCheckPlan` rejects.
    ///
    /// A property is propagated only when every column its expression references
    /// survives into the output schema with the same data type. Column indices are
    /// remapped by name because the output schema may reorder or drop columns
    /// relative to the input. Type casts are not universally monotonic
    /// (`Utf8`→numeric, float NaN handling) and do not preserve a constant's value,
    /// so an expression referencing a cast column is dropped.
    ///
    /// Two classes of the input's properties are still dropped: equivalence classes
    /// (`WHERE a = b` below makes an ordering on `a` satisfy `ORDER BY b`), and every
    /// ordering past the input's first. Dropping a property is safe — it costs a
    /// sort, never correctness — but each is the same trap as the constants above, so
    /// closing one belongs with closing all of them: `EquivalenceProperties::project`
    /// forwards orderings, constants and equivalence classes together, keyed on a
    /// `ProjectionMapping` built from the same-name-same-type guard below. Reach for
    /// that rather than a third enumerated branch.
    /// (`EquivalenceProperties::with_new_schema` is not a substitute: it demands
    /// index-aligned, identically-typed schemas, which is exactly what this exec
    /// exists to break.)
    fn output_equivalence_properties(
        input: &Arc<dyn ExecutionPlan>,
        input_schema: &SchemaRef,
        output_schema: &SchemaRef,
    ) -> EquivalenceProperties {
        // The `Err` is a control-flow signal for "this column does not survive", never
        // surfaced: `remap` turns it into `None` and the property is dropped.
        let unmappable = || DataFusionError::Plan("column does not survive the cast".to_string());
        let remap = |expr: &Arc<dyn PhysicalExpr>| -> Option<Arc<dyn PhysicalExpr>> {
            Arc::clone(expr)
                .transform_up(|expr| {
                    let Some(col) = expr.downcast_ref::<Column>() else {
                        return Ok(Transformed::no(expr));
                    };
                    let input_field = input_schema
                        .fields()
                        .get(col.index())
                        .ok_or_else(unmappable)?;
                    let (output_idx, output_field) = output_schema
                        .column_with_name(col.name())
                        .ok_or_else(unmappable)?;
                    if input_field.data_type() != output_field.data_type() {
                        return Err(unmappable());
                    }
                    if output_idx == col.index() {
                        // Same position: rebuilding the column would allocate a name and
                        // force every ancestor expression node to be rebuilt with it.
                        return Ok(Transformed::no(expr));
                    }
                    Ok(Transformed::yes(
                        Arc::new(Column::new(col.name(), output_idx)) as Arc<dyn PhysicalExpr>,
                    ))
                })
                .ok()
                .map(|transformed| transformed.data)
        };

        let input_properties = input.equivalence_properties();
        let mut eq_properties = EquivalenceProperties::new(Arc::clone(output_schema));
        if let Some(ordering) = input_properties.output_ordering() {
            let remapped: Option<Vec<PhysicalSortExpr>> = ordering
                .iter()
                .map(|sort_expr| {
                    Some(PhysicalSortExpr {
                        expr: remap(&sort_expr.expr)?,
                        options: sort_expr.options,
                    })
                })
                .collect();
            if let Some(new_ordering) = remapped {
                eq_properties.add_orderings([new_ordering]);
            }
        }

        let constants: Vec<ConstExpr> = input_properties
            .constants()
            .into_iter()
            .filter_map(|constant| {
                Some(ConstExpr::new(
                    remap(&constant.expr)?,
                    constant.across_partitions,
                ))
            })
            .collect();
        if !constants.is_empty() && eq_properties.add_constants(constants).is_err() {
            // `add_constants` only fails on an internal invariant, and half-updated
            // properties are not safe to advertise, so advertise none.
            return EquivalenceProperties::new(Arc::clone(output_schema));
        }
        eq_properties
    }
}

impl DisplayAs for SchemaCastScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SchemaCastScanExec")
    }
}

impl fmt::Debug for SchemaCastScanExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SchemaCastScanExec")
            .field("input", &self.input)
            .field("target_schema", &self.target_schema)
            .field("output_schema", &self.output_schema)
            .field("properties", &self.properties)
            .finish()
    }
}

// if new features are added to ExecutionPlan, we want to know
// it's possible we'll just re-implement the default methods - but that requires attention
// for example, the recently added `gather_filters_for_pushdown` defaults to `all_unsupported` but we likely want `from_children`
#[deny(clippy::missing_trait_methods)]
impl ExecutionPlan for SchemaCastScanExec {
    fn downcast_delegate(&self) -> Option<&dyn ExecutionPlan> {
        None
    }

    fn with_preserve_order(&self, _preserve_order: bool) -> Option<Arc<dyn ExecutionPlan>> {
        None
    }

    fn name(&self) -> &'static str {
        "SchemaCastScanExec"
    }

    fn static_name() -> &'static str
    where
        Self: Sized,
    {
        "SchemaCastScanExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.output_schema)
    }

    fn check_invariants(&self, check: InvariantLevel) -> Result<()> {
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
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() == 1 {
            Ok(Arc::new(Self::new(
                Arc::clone(&children[0]),
                Arc::clone(&self.target_schema),
            )))
        } else {
            Err(DataFusionError::Execution(
                "SchemaCastScanExec expects exactly one input".to_string(),
            ))
        }
    }

    fn reset_state(self: Arc<Self>) -> Result<Arc<dyn ExecutionPlan>> {
        let children = self.children().into_iter().cloned().collect();
        self.with_new_children(children)
    }

    fn repartitioned(
        &self,
        _target_partitions: usize,
        _config: &ConfigOptions,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        Ok(None)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let mut stream = self.input.execute(partition, context)?;
        let schema = self.schema();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&schema),
            {
                stream! {
                    while let Some(batch) = stream.next().await {
                        yield record_batch::try_cast_to(batch?, Arc::clone(&schema)).map_err(From::from);
                    }
                }
            },
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        self.input.metrics()
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Arc<Statistics>> {
        self.input.partition_statistics(partition)
    }

    // Allow optimizer to push limits through to inputs
    fn supports_limit_pushdown(&self) -> bool {
        self.input.supports_limit_pushdown()
    }

    fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        let target_schema = Arc::clone(&self.target_schema);
        self.input.with_fetch(limit).map(|plan| {
            Arc::new(SchemaCastScanExec::new(plan, target_schema)) as Arc<dyn ExecutionPlan>
        })
    }

    fn fetch(&self) -> Option<usize> {
        self.input.fetch()
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::Equal
    }

    fn try_swapping_with_projection(
        &self,
        _projection: &ProjectionExec,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        Ok(None)
    }

    fn gather_filters_for_pushdown(
        &self,
        _phase: FilterPushdownPhase,
        parent_filters: Vec<Arc<dyn PhysicalExpr>>,
        _config: &ConfigOptions,
    ) -> Result<FilterDescription> {
        FilterDescription::from_children(parent_filters, &self.children())
    }

    fn handle_child_pushdown_result(
        &self,
        _phase: FilterPushdownPhase,
        child_pushdown_result: ChildPushdownResult,
        _config: &ConfigOptions,
    ) -> Result<FilterPushdownPropagation<Arc<dyn ExecutionPlan>>> {
        Ok(FilterPushdownPropagation::if_all(child_pushdown_result))
    }

    fn with_new_state(&self, _state: Arc<dyn Any + Send + Sync>) -> Option<Arc<dyn ExecutionPlan>> {
        None
    }

    fn try_pushdown_sort(
        &self,
        order: &[PhysicalSortExpr],
    ) -> Result<SortOrderPushdownResult<Arc<dyn ExecutionPlan>>, DataFusionError> {
        let target_schema = Arc::clone(&self.target_schema);
        let result = self.input.try_pushdown_sort(order)?;
        Ok(result.map(|plan| {
            Arc::new(SchemaCastScanExec::new(plan, target_schema)) as Arc<dyn ExecutionPlan>
        }))
    }
}

#[derive(Debug)]
pub struct EnsureSchema {
    input: Arc<dyn TableProvider>,
}

impl EnsureSchema {
    pub fn new(input: Arc<dyn TableProvider>) -> Self {
        Self { input }
    }
}

#[async_trait]
impl TableProvider for EnsureSchema {
    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn table_type(&self) -> TableType {
        self.input.table_type()
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let input = self.input.scan(state, projection, filters, limit).await?;

        // Compute target schema based on projection, not full table schema.
        // When projection is specified, only include those fields.
        let target_schema = match projection {
            Some(indices) => {
                let full_schema = self.schema();
                let projected_fields: Vec<_> = indices
                    .iter()
                    .filter_map(|&i| full_schema.fields().get(i).cloned())
                    .collect();
                Arc::new(Schema::new_with_metadata(
                    projected_fields,
                    full_schema.metadata().clone(),
                ))
            }
            None => self.schema(),
        };

        Ok(Arc::new(SchemaCastScanExec::new(input, target_schema)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::compute::SortOptions;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::logical_expr::TableProviderFilterPushDown;
    use datafusion::physical_plan::displayable;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::expressions::col as physical_col;
    use datafusion::physical_plan::filter::FilterExec;
    use datafusion::physical_plan::sorts::sort::SortExec;
    use datafusion::physical_plan::union::UnionExec;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use datafusion::{
        assert_batches_eq, assert_batches_sorted_eq,
        logical_expr::Operator,
        physical_expr::{
            LexOrdering,
            expressions::{BinaryExpr, Literal},
        },
        scalar::ScalarValue,
    };
    use datafusion_optimizer_rules::common::search_visitor::SearchVisitor;

    fn input_schema_with_extra_column() -> SchemaRef {
        // Input has 3 columns including an internal "fetched_at" column
        Arc::new(Schema::new(vec![
            Field::new("request_path", DataType::Utf8, false),
            Field::new("content", DataType::Utf8, true),
            Field::new("_fetched_at", DataType::Int64, true),
        ]))
    }

    fn expected_output_schema() -> SchemaRef {
        // User expects only 2 columns (no _fetched_at)
        Arc::new(Schema::new(vec![
            Field::new("request_path", DataType::Utf8, false),
            Field::new("content", DataType::Utf8, true),
        ]))
    }

    #[test]
    fn test_schema_returns_expected_schema_not_input_schema() {
        // Simulates the cache HIT scenario from GitHub issue #9019:
        // Input has 3 columns (including internal _fetched_at), but user only requested 2 columns.
        // SchemaCastScanExec should return the expected 2-column schema, not the input's 3-column schema.
        let input = Arc::new(EmptyExec::new(input_schema_with_extra_column()));
        let expected_schema = expected_output_schema();

        let schema_cast = SchemaCastScanExec::new(input, Arc::clone(&expected_schema));

        let actual_schema = schema_cast.schema();
        assert_eq!(
            actual_schema.fields().len(),
            2,
            "Schema should have 2 fields, not 3 (_fetched_at should be stripped)"
        );
        assert_eq!(
            actual_schema.field(0).name(),
            "request_path",
            "First field should be request_path"
        );
        assert_eq!(
            actual_schema.field(1).name(),
            "content",
            "Second field should be content"
        );
        // The schema should exactly match the expected schema
        assert_eq!(
            actual_schema.fields(),
            expected_schema.fields(),
            "Schema should match expected output schema"
        );
    }

    #[test]
    fn test_schema_preserves_when_input_matches_expected() {
        // When input and expected schemas match, SchemaCastScanExec should return that schema.
        let matching_schema = expected_output_schema();
        let input = Arc::new(EmptyExec::new(Arc::clone(&matching_schema)));

        let schema_cast = SchemaCastScanExec::new(input, Arc::clone(&matching_schema));

        let actual_schema = schema_cast.schema();
        assert_eq!(
            actual_schema.fields(),
            matching_schema.fields(),
            "Schema should match when input equals expected"
        );
    }

    #[test]
    fn test_schema_makes_fields_nullable_when_input_is_nullable() {
        // When input schema has nullable fields but target schema has non-nullable,
        // the output should be nullable to avoid "non-nullable but contains null values" errors.
        let input_schema = Arc::new(Schema::new(vec![
            Field::new("request_path", DataType::Utf8, false),
            Field::new("content", DataType::Utf8, true), // nullable in input
        ]));
        let target_schema = Arc::new(Schema::new(vec![
            Field::new("request_path", DataType::Utf8, false),
            Field::new("content", DataType::Utf8, false), // non-nullable in target
        ]));

        let input = Arc::new(EmptyExec::new(input_schema));
        let schema_cast = SchemaCastScanExec::new(input, target_schema);

        let actual_schema = schema_cast.schema();
        assert!(
            actual_schema
                .field_with_name("content")
                .is_ok_and(Field::is_nullable),
            "content field should be nullable because input is nullable"
        );
    }

    #[test]
    fn test_schema_handles_empty_projection() {
        // Test for aggregate queries like `SELECT COUNT(1) FROM table` which have
        // an empty projection (projection=[]) - no columns selected from the table.
        let input_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let empty_schema = Arc::new(Schema::empty());

        let input = Arc::new(EmptyExec::new(input_schema));
        let schema_cast = SchemaCastScanExec::new(input, empty_schema);

        let actual_schema = schema_cast.schema();
        assert_eq!(
            actual_schema.fields().len(),
            0,
            "Schema should have 0 fields for empty projection"
        );
    }

    #[test]
    fn test_ordering_propagated_when_types_match() {
        // When the ordered column has the same type in input and output, ordering
        // should be propagated.
        let input_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let target_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let empty = Arc::new(EmptyExec::new(Arc::clone(&input_schema)));
        let lex_ordering = LexOrdering::new(vec![
            PhysicalSortExpr::new_default(physical_col("id", &input_schema).expect("col id")).asc(),
        ])
        .expect("lex ordering");
        let sorted_input: Arc<dyn ExecutionPlan> = Arc::new(SortExec::new(lex_ordering, empty));

        let schema_cast = SchemaCastScanExec::new(sorted_input, target_schema);

        assert!(
            schema_cast.properties().output_ordering().is_some(),
            "Ordering should be propagated when types match"
        );
    }

    #[test]
    fn test_expression_ordering_propagated_when_referenced_types_match() {
        let input_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("embedding", DataType::Float64, true),
        ]));
        let target_schema = Arc::new(Schema::new(vec![
            Field::new("embedding", DataType::Float64, true),
            Field::new("id", DataType::Int64, false),
        ]));

        let sort_expr: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(Literal::new(ScalarValue::Float64(Some(1.0)))),
            Operator::Minus,
            physical_col("embedding", &input_schema).expect("col embedding"),
        ));
        let lex_ordering = LexOrdering::new(vec![PhysicalSortExpr::new_default(sort_expr).desc()])
            .expect("lex ordering");
        let empty = Arc::new(EmptyExec::new(Arc::clone(&input_schema)));
        let sorted_input: Arc<dyn ExecutionPlan> = Arc::new(SortExec::new(lex_ordering, empty));

        let schema_cast = SchemaCastScanExec::new(sorted_input, target_schema);
        let output_ordering = schema_cast
            .properties()
            .output_ordering()
            .expect("expression ordering should be propagated");
        let binary = output_ordering[0]
            .expr
            .downcast_ref::<BinaryExpr>()
            .expect("sort expression should remain binary");
        let column = binary
            .right()
            .downcast_ref::<Column>()
            .expect("right operand should remain a column");
        assert_eq!(column.name(), "embedding");
        assert_eq!(column.index(), 0, "column index should match output schema");
    }

    #[test]
    fn test_ordering_not_propagated_when_types_differ() {
        // When the ordered column undergoes a type cast, ordering should NOT be
        // propagated since the cast may not be monotonic.
        let input_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let target_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false), // Utf8 -> Int64 (not monotonic)
            Field::new("name", DataType::Utf8, true),
        ]));

        let empty = Arc::new(EmptyExec::new(Arc::clone(&input_schema)));
        let lex_ordering = LexOrdering::new(vec![
            PhysicalSortExpr::new_default(physical_col("id", &input_schema).expect("col id")).asc(),
        ])
        .expect("lex ordering");
        let sorted_input: Arc<dyn ExecutionPlan> = Arc::new(SortExec::new(lex_ordering, empty));

        let schema_cast = SchemaCastScanExec::new(sorted_input, target_schema);

        assert!(
            schema_cast.properties().output_ordering().is_none(),
            "Ordering should NOT be propagated when types differ"
        );
    }

    #[test]
    fn test_ordering_remaps_indices_when_schema_reorders_columns() {
        // When the target schema reorders columns, the ordering column indices
        // should be remapped to the output schema positions.
        let input_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),   // index 0 in input
            Field::new("b", DataType::Utf8, true),     // index 1 in input
            Field::new("c", DataType::Float64, false), // index 2 in input
        ]));
        let target_schema = Arc::new(Schema::new(vec![
            Field::new("c", DataType::Float64, false), // was index 2, now index 0
            Field::new("a", DataType::Int64, false),   // was index 0, now index 1
            Field::new("b", DataType::Utf8, true),     // was index 1, now index 2
        ]));

        let empty = Arc::new(EmptyExec::new(Arc::clone(&input_schema)));
        // Sort on "a" which is at index 0 in input
        let lex_ordering = LexOrdering::new(vec![
            PhysicalSortExpr::new_default(physical_col("a", &input_schema).expect("col a")).asc(),
        ])
        .expect("lex ordering");
        let sorted_input: Arc<dyn ExecutionPlan> = Arc::new(SortExec::new(lex_ordering, empty));

        let schema_cast = SchemaCastScanExec::new(sorted_input, target_schema);

        let output_ordering = schema_cast
            .properties()
            .output_ordering()
            .expect("ordering should be propagated");
        let sort_expr = &output_ordering[0];
        let col = sort_expr
            .expr
            .downcast_ref::<Column>()
            .expect("should be Column expr");
        assert_eq!(
            col.index(),
            1,
            "Column 'a' should be remapped to index 1 in the output schema"
        );
        assert_eq!(col.name(), "a");
    }

    /// A two-column `[id, value]` schema, the shape the new tests below share.
    fn id_value_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
        ]))
    }

    /// The predicate `id = <literal>` over `input`, as a `FilterExec`. An equality
    /// filter is what makes a column constant in `DataFusion`'s equivalence
    /// properties, and a constant column satisfies an `ORDER BY` on it — in either
    /// direction — with no sort.
    fn filter_id_equals(
        input: Arc<dyn ExecutionPlan>,
        literal: ScalarValue,
    ) -> Arc<dyn ExecutionPlan> {
        let schema = input.schema();
        let predicate: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            physical_col("id", &schema).expect("col id"),
            Operator::Eq,
            Arc::new(Literal::new(literal)),
        ));
        Arc::new(FilterExec::try_new(predicate, input).expect("filter exec"))
    }

    fn constant_column_indices(plan: &dyn ExecutionPlan, name: &str) -> Vec<usize> {
        plan.properties()
            .eq_properties
            .constants()
            .iter()
            .filter_map(|constant| constant.expr.downcast_ref::<Column>())
            .filter(|column| column.name() == name)
            .map(Column::index)
            .collect()
    }

    #[test]
    fn a_constant_column_propagates_through_the_schema_cast() {
        let schema = id_value_schema();
        let filtered = filter_id_equals(
            Arc::new(EmptyExec::new(Arc::clone(&schema))),
            ScalarValue::Int64(Some(1)),
        );
        assert_eq!(
            constant_column_indices(filtered.as_ref(), "id"),
            vec![0],
            "precondition: an equality filter makes its column constant"
        );

        let schema_cast = SchemaCastScanExec::new(filtered, Arc::clone(&schema));

        assert_eq!(
            constant_column_indices(&schema_cast, "id"),
            vec![0],
            "a constant column must survive the schema cast"
        );
        for options in [
            SortOptions {
                descending: false,
                nulls_first: false,
            },
            SortOptions {
                descending: true,
                nulls_first: true,
            },
        ] {
            assert!(
                schema_cast
                    .properties()
                    .eq_properties
                    .ordering_satisfy(vec![PhysicalSortExpr::new(
                        physical_col("id", &schema).expect("col id"),
                        options,
                    )])
                    .expect("ordering satisfaction"),
                "a constant column satisfies an ordering on it in either direction ({options:?})"
            );
        }
    }

    #[test]
    fn a_constant_column_is_not_propagated_when_its_type_changes() {
        let input_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("value", DataType::Int64, false),
        ]));
        let filtered = filter_id_equals(
            Arc::new(EmptyExec::new(input_schema)),
            ScalarValue::Utf8(Some("1".to_string())),
        );
        assert_eq!(
            constant_column_indices(filtered.as_ref(), "id"),
            vec![0],
            "precondition: an equality filter makes its column constant"
        );

        let schema_cast = SchemaCastScanExec::new(filtered, id_value_schema());

        assert!(
            constant_column_indices(&schema_cast, "id").is_empty(),
            "a cast does not preserve a constant's value, so the constant must be dropped"
        );
    }

    #[test]
    fn a_constant_column_is_remapped_when_the_schema_reorders_columns() {
        let target_schema = Arc::new(Schema::new(vec![
            Field::new("value", DataType::Int64, false),
            Field::new("id", DataType::Int64, false),
        ]));
        let filtered = filter_id_equals(
            Arc::new(EmptyExec::new(id_value_schema())),
            ScalarValue::Int64(Some(1)),
        );

        let schema_cast = SchemaCastScanExec::new(filtered, target_schema);

        assert_eq!(
            constant_column_indices(&schema_cast, "id"),
            vec![1],
            "the constant's column index must follow the output schema"
        );
    }

    // ── the tiered scan an accelerator serves after a transactional commit ──

    fn id_value_batch(ids: &[i64], values: &[i64]) -> RecordBatch {
        RecordBatch::try_new(
            id_value_schema(),
            vec![
                Arc::new(Int64Array::from(ids.to_vec())),
                Arc::new(Int64Array::from(values.to_vec())),
            ],
        )
        .expect("valid id/value batch")
    }

    /// A scan shaped like an accelerator serving a table that has taken a
    /// transactional commit: one branch per storage tier, unioned together.
    ///
    /// The file branch is empty because the commit's tombstone prunes every file the
    /// query touches, and neither branch absorbs a predicate, so the physical filter
    /// pushdown lands a `FilterExec` on each — which is what makes the union's `id`
    /// constant under `WHERE id = ?`. Registered behind [`EnsureSchema`], so the
    /// tests run the same wrapper composition production does.
    #[derive(Debug)]
    struct TieredScan {
        /// One `Vec` per in-memory partition.
        memory_tier: Vec<Vec<RecordBatch>>,
    }

    #[async_trait]
    impl TableProvider for TieredScan {
        fn schema(&self) -> SchemaRef {
            id_value_schema()
        }

        fn table_type(&self) -> TableType {
            TableType::Base
        }

        fn supports_filters_pushdown(
            &self,
            filters: &[&Expr],
        ) -> Result<Vec<TableProviderFilterPushDown>> {
            Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
        }

        async fn scan(
            &self,
            _state: &dyn Session,
            projection: Option<&Vec<usize>>,
            _filters: &[Expr],
            _limit: Option<usize>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            let memory_tier = MemorySourceConfig::try_new_exec(
                &self.memory_tier,
                self.schema(),
                projection.cloned(),
            )?;
            let file_tier: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(memory_tier.schema()));
            UnionExec::try_new(vec![file_tier, memory_tier])
        }
    }

    /// A session over a [`TieredScan`], with enough partitions for the multi-branch,
    /// repartitioned scan shape to form; a single-partition session never grows it.
    fn tiered_session(memory_tier: Vec<Vec<RecordBatch>>) -> SessionContext {
        let mut config = SessionConfig::new();
        config.options_mut().execution.target_partitions = 4;
        let ctx = SessionContext::new_with_config(config);
        ctx.register_table(
            "tiered",
            Arc::new(EnsureSchema::new(Arc::new(TieredScan { memory_tier }))),
        )
        .expect("table registered");
        ctx
    }

    fn one_row_tier() -> Vec<Vec<RecordBatch>> {
        vec![vec![id_value_batch(&[1], &[7])]]
    }

    async fn collect(ctx: &SessionContext, sql: &str) -> Result<Vec<RecordBatch>> {
        ctx.sql(sql).await?.collect().await
    }

    /// A primary-key point lookup that also orders by the primary key must plan and
    /// return the matching row (regression test for #13554).
    ///
    /// The equality filter makes `id` constant in each union branch, so the scan
    /// satisfies `ORDER BY id` with no sort and `EnforceSorting` leaves a
    /// `SortPreservingMergeExec` reading the ordering back off this exec. `DESC` and a
    /// single-element `IN` reach the same shape.
    #[tokio::test]
    async fn a_pk_point_lookup_ordered_by_the_pk_plans_over_a_tiered_scan() {
        let ctx = tiered_session(one_row_tier());
        for sql in [
            "SELECT id, value FROM tiered WHERE id = 1 ORDER BY id",
            "SELECT id, value FROM tiered WHERE id IN (1) ORDER BY id",
            "SELECT id, value FROM tiered WHERE id = 1 ORDER BY id DESC",
            "SELECT id, value FROM tiered WHERE id = 1 ORDER BY id ASC NULLS FIRST",
        ] {
            let batches = collect(&ctx, sql)
                .await
                .unwrap_or_else(|e| panic!("`{sql}` must plan and execute: {e}"));
            assert_batches_eq!(
                [
                    "+----+-------+",
                    "| id | value |",
                    "+----+-------+",
                    "| 1  | 7     |",
                    "+----+-------+",
                ],
                &batches
            );
        }
    }

    /// The shapes that always planned must keep planning and stay ordered.
    #[tokio::test]
    async fn ordered_shapes_without_a_constant_sort_key_still_plan() {
        let ctx = tiered_session(vec![
            vec![id_value_batch(&[3, 1], &[30, 10])],
            vec![id_value_batch(&[2], &[20])],
        ]);

        for (sql, expected) in [
            (
                "SELECT id, value FROM tiered ORDER BY id",
                vec!["| 1  | 10    |", "| 2  | 20    |", "| 3  | 30    |"],
            ),
            (
                "SELECT id, value FROM tiered WHERE value > 10 ORDER BY id",
                vec!["| 2  | 20    |", "| 3  | 30    |"],
            ),
            (
                "SELECT id, value FROM tiered WHERE id = 3 ORDER BY value",
                vec!["| 3  | 30    |"],
            ),
            (
                "SELECT id, value FROM tiered WHERE id IN (1, 3) ORDER BY id DESC",
                vec!["| 3  | 30    |", "| 1  | 10    |"],
            ),
        ] {
            let batches = collect(&ctx, sql)
                .await
                .unwrap_or_else(|e| panic!("`{sql}` must plan and execute: {e}"));
            let mut want = vec!["+----+-------+", "| id | value |", "+----+-------+"];
            want.extend(expected);
            want.push("+----+-------+");
            assert_batches_eq!(want, &batches);
        }
    }

    /// The point lookup's rows are spread over several in-memory partitions on top of
    /// the empty file branch, so the merge above this exec reads more partitions than
    /// any one branch provides. Every matching row still comes back, once.
    #[tokio::test]
    async fn a_pk_point_lookup_over_several_partitions_returns_every_row_once() {
        let ctx = tiered_session(vec![
            vec![id_value_batch(&[1, 2], &[10, 20])],
            vec![id_value_batch(&[1], &[11])],
            vec![id_value_batch(&[3], &[30])],
        ]);

        let batches = collect(
            &ctx,
            "SELECT id, value FROM tiered WHERE id = 1 ORDER BY id",
        )
        .await
        .expect("point lookup over several partitions must plan and execute");
        assert_batches_sorted_eq!(
            [
                "+----+-------+",
                "| id | value |",
                "+----+-------+",
                "| 1  | 10    |",
                "| 1  | 11    |",
                "+----+-------+",
            ],
            &batches
        );
    }

    /// Non-vacuity guard for the point lookup above: it only exercises the property
    /// this exec advertises while the ordering is genuinely discharged by the
    /// constant. A `SortExec` anywhere in the plan would mean the ordering was sorted
    /// for instead, and the point-lookup test would then pass without ever reading
    /// what this exec advertises.
    #[tokio::test]
    async fn the_pk_point_lookup_plan_discharges_the_ordering_through_this_exec() {
        let ctx = tiered_session(one_row_tier());
        let plan = ctx
            .sql("SELECT id, value FROM tiered WHERE id = 1 ORDER BY id")
            .await
            .expect("logical plan")
            .create_physical_plan()
            .await
            .expect("physical plan");
        let rendered = displayable(plan.as_ref()).indent(true).to_string();

        assert!(
            SearchVisitor::first_concrete_down::<SortExec>(&plan)
                .expect("plan search")
                .is_none(),
            "the constant must discharge the ordering; a SortExec means this plan no \
             longer exercises what this exec advertises:\n{rendered}"
        );
        let schema_cast = SearchVisitor::first_concrete_down::<SchemaCastScanExec>(&plan)
            .expect("plan search")
            .unwrap_or_else(|| panic!("the plan must read through this exec:\n{rendered}"));
        assert_eq!(
            constant_column_indices(schema_cast.as_ref(), "id"),
            vec![0],
            "this exec must advertise the constant the ordering rests on:\n{rendered}"
        );
    }
}
