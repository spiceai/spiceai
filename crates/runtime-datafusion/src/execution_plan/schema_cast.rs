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
use datafusion::common::Statistics;
use datafusion::config::ConfigOptions;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::projection::{ProjectionMapping, ProjectionTargets};
use datafusion::physical_expr::{EquivalenceProperties, OrderingRequirements};
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
    /// `CardinalityEffect::Equal`. Both let `EnforceSorting` push an ordering
    /// requirement *through* it into the child and drop the sort once the child
    /// satisfies the requirement — but the node `SanityCheckPlan` then validates the
    /// surviving `SortPreservingMergeExec` against is *this* one. So whatever the
    /// child used to discharge the requirement has to survive into what this exec
    /// advertises, and a property dropped here does not cost a sort: it rejects the
    /// plan. A constant (`WHERE pk = ?` satisfying `ORDER BY pk`), an equivalence
    /// class (`WHERE a = b` making an ordering on `a` satisfy `ORDER BY b`) and a
    /// secondary ordering all reach that same failure.
    ///
    /// So forward the input's properties wholesale rather than by kind, and put the
    /// conservatism in the column mapping instead: an input column is mapped only
    /// when the output schema still has it, by name, with an unchanged data type.
    /// The output schema may reorder, drop, or retype columns, and a cast is neither
    /// universally monotonic (`Utf8`→numeric, float NaN handling) nor value
    /// preserving, so anything referencing a column that fails that test is absent
    /// from the mapping and [`EquivalenceProperties::project`] drops it.
    fn output_equivalence_properties(
        input: &Arc<dyn ExecutionPlan>,
        input_schema: &SchemaRef,
        output_schema: &SchemaRef,
    ) -> EquivalenceProperties {
        // A name that repeats in either schema is ambiguous, and the ambiguity is not
        // academic: `try_cast_to` re-labels the batch positionally when the schemas
        // already agree, and matches by first name when it has to build columns. Those
        // resolve a repeated name to different inputs, so keying the mapping on the
        // name could advertise one column's properties for another column's values.
        // Map only unambiguous names.
        let occurs_once = |schema: &SchemaRef, name: &str| {
            schema
                .fields()
                .iter()
                .filter(|field| field.name() == name)
                .count()
                == 1
        };

        // Grouped by source column: one input column may be produced more than once,
        // and every target of a source has to travel with it.
        let mut sources: Vec<(usize, ProjectionTargets)> = Vec::new();
        for (output_idx, output_field) in output_schema.fields().iter().enumerate() {
            if !occurs_once(output_schema, output_field.name())
                || !occurs_once(input_schema, output_field.name())
            {
                continue;
            }
            let Some((input_idx, input_field)) = input_schema.column_with_name(output_field.name())
            else {
                continue;
            };
            if input_field.data_type() != output_field.data_type() {
                continue;
            }
            let target: Arc<dyn PhysicalExpr> =
                Arc::new(Column::new(output_field.name(), output_idx));
            match sources.iter_mut().find(|(idx, _)| *idx == input_idx) {
                Some((_, targets)) => targets.push((target, output_idx)),
                None => sources.push((
                    input_idx,
                    ProjectionTargets::from(vec![(target, output_idx)]),
                )),
            }
        }
        let mapping: ProjectionMapping = sources
            .into_iter()
            .map(|(input_idx, targets)| {
                let source: Arc<dyn PhysicalExpr> =
                    Arc::new(Column::new(input_schema.field(input_idx).name(), input_idx));
                (source, targets)
            })
            .collect();
        input
            .equivalence_properties()
            .project(&mapping, Arc::clone(output_schema))
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
    use datafusion::datasource::source::DataSourceExec;
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

    /// A sort requirement in `schema`'s coordinates, ascending, NULLs last.
    fn ascending_on(schema: &SchemaRef, name: &str) -> Vec<PhysicalSortExpr> {
        vec![PhysicalSortExpr::new(
            physical_col(name, schema).expect("column"),
            SortOptions {
                descending: false,
                nulls_first: false,
            },
        )]
    }

    fn a_b_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
        ]))
    }

    /// An equivalence class the child used to satisfy an ordering must survive.
    ///
    /// `WHERE a = b` makes an ordering on `a` satisfy `ORDER BY b`. `EnforceSorting`
    /// pushes the requirement through this exec, the child discharges it, and no sort
    /// is added — so if the class stops here, the merge above has an unordered child
    /// and the plan is rejected, exactly as it was for constants.
    #[test]
    fn an_equivalence_class_propagates_through_the_schema_cast() {
        let schema = a_b_schema();
        let ordering = LexOrdering::new(vec![
            PhysicalSortExpr::new_default(physical_col("a", &schema).expect("col a")).asc(),
        ])
        .expect("lex ordering");
        let sorted: Arc<dyn ExecutionPlan> = Arc::new(SortExec::new(
            ordering,
            Arc::new(EmptyExec::new(Arc::clone(&schema))),
        ));
        let predicate: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            physical_col("a", &schema).expect("col a"),
            Operator::Eq,
            physical_col("b", &schema).expect("col b"),
        ));
        let filtered: Arc<dyn ExecutionPlan> =
            Arc::new(FilterExec::try_new(predicate, sorted).expect("filter exec"));
        assert!(
            filtered
                .equivalence_properties()
                .ordering_satisfy(ascending_on(&schema, "b"))
                .expect("ordering satisfaction"),
            "precondition: the child satisfies ORDER BY b through a = b"
        );

        let schema_cast = SchemaCastScanExec::new(filtered, Arc::clone(&schema));

        assert!(
            schema_cast
                .properties()
                .eq_properties
                .ordering_satisfy(ascending_on(&schema, "b"))
                .expect("ordering satisfaction"),
            "the equivalence class the child discharged the requirement with must survive"
        );
    }

    /// A column name that repeats is ambiguous about which input it came from, and
    /// `try_cast_to` resolves that ambiguity two different ways depending on whether
    /// it can re-label the batch wholesale. Advertising the first column's properties
    /// for all of them could therefore describe values another column holds, so a
    /// repeated name carries nothing.
    #[test]
    fn a_repeated_column_name_propagates_nothing() {
        let duplicated = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Int64, false),
            Field::new("x", DataType::Int64, false),
        ]));
        // Constant on the *first* `x` only; the second holds unrelated values.
        let predicate: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("x", 0)),
            Operator::Eq,
            Arc::new(Literal::new(ScalarValue::Int64(Some(1)))),
        ));
        let filtered: Arc<dyn ExecutionPlan> = Arc::new(
            FilterExec::try_new(predicate, Arc::new(EmptyExec::new(Arc::clone(&duplicated))))
                .expect("filter exec"),
        );
        assert!(
            !constant_column_indices(filtered.as_ref(), "x").is_empty(),
            "precondition: the child holds a constant on one of the two `x` columns"
        );

        let schema_cast = SchemaCastScanExec::new(filtered, Arc::clone(&duplicated));

        assert!(
            constant_column_indices(&schema_cast, "x").is_empty(),
            "an ambiguous name must carry no properties, or one column's constant \
             describes another column's values"
        );
    }

    /// Likewise for an input that advertises more than one ordering: the child
    /// satisfies `ORDER BY b` through its second one, so that one has to survive too.
    /// Forwarding only the input's primary ordering concatenates them into
    /// `[a ASC, b ASC]`, which does not satisfy `[b ASC]` — satisfaction is a prefix
    /// check.
    #[test]
    fn a_secondary_ordering_propagates_through_the_schema_cast() {
        let schema = a_b_schema();
        let orderings: Vec<LexOrdering> = ["a", "b"]
            .into_iter()
            .map(|name| {
                LexOrdering::new(vec![
                    PhysicalSortExpr::new_default(physical_col(name, &schema).expect("column"))
                        .asc(),
                ])
                .expect("lex ordering")
            })
            .collect();
        let source = MemorySourceConfig::try_new(&[vec![]], Arc::clone(&schema), None)
            .expect("memory source")
            .try_with_sort_information(orderings)
            .expect("sort information");
        let ordered: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(source);
        assert!(
            ordered
                .equivalence_properties()
                .ordering_satisfy(ascending_on(&schema, "b"))
                .expect("ordering satisfaction"),
            "precondition: the child satisfies ORDER BY b through its second ordering"
        );

        let schema_cast = SchemaCastScanExec::new(ordered, Arc::clone(&schema));

        assert!(
            schema_cast
                .properties()
                .eq_properties
                .ordering_satisfy(ascending_on(&schema, "b"))
                .expect("ordering satisfaction"),
            "every ordering the child can discharge a requirement with must survive"
        );
    }
}
