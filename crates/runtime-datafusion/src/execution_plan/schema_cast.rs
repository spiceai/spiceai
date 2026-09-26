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
use datafusion::common::stats::Precision;
use datafusion::common::{ColumnStatistics, Constraints, Statistics};
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

    /// For each output column, the input column it is derived from, or `None`
    /// where no input column carries over unchanged.
    ///
    /// A column carries over only when the output schema keeps it unambiguously
    /// by name and with an unchanged data type. This is deliberately
    /// conservative: a repeated name is ambiguous (`try_cast_to` may resolve it
    /// to a different column than the one whose statistics or ordering we would
    /// attribute), and a retyped column is a cast, which is neither value- nor
    /// order-preserving. Both statistics projection and equivalence-property
    /// projection depend on this mapping, so they agree by construction.
    fn output_to_input_columns(
        input_schema: &Schema,
        output_schema: &Schema,
    ) -> Vec<Option<usize>> {
        let occurs_once = |schema: &Schema, name: &str| {
            schema.fields().iter().filter(|f| f.name() == name).count() == 1
        };
        output_schema
            .fields()
            .iter()
            .map(|output_field| {
                let name = output_field.name();
                if !occurs_once(output_schema, name) || !occurs_once(input_schema, name) {
                    return None;
                }
                input_schema
                    .column_with_name(name)
                    .filter(|(_, input_field)| input_field.data_type() == output_field.data_type())
                    .map(|(input_idx, _)| input_idx)
            })
            .collect()
    }

    /// The equivalence properties this exec advertises, derived from its input's.
    ///
    /// This exec casts values in place, so it reports `maintains_input_order` and
    /// `CardinalityEffect::Equal`. Both let `EnforceSorting` push an ordering
    /// requirement *through* it into the child and drop the sort once the child
    /// satisfies the requirement — but `SanityCheckPlan` then validates the surviving
    /// `SortPreservingMergeExec` against *this* node. So whatever the child used to
    /// discharge the requirement has to survive into what this exec
    /// advertises, and a property dropped here does not cost a sort: it rejects the
    /// plan. A constant (`WHERE pk = ?` satisfying `ORDER BY pk`), an equivalence
    /// class (`WHERE a = b` making an ordering on `a` satisfy `ORDER BY b`) and a
    /// secondary ordering all reach that same failure.
    ///
    /// So forward the input's properties wholesale rather than by kind, and put the
    /// conservatism in [`Self::output_to_input_columns`] instead: a property
    /// referencing a column that does not carry over is absent from the mapping,
    /// and [`EquivalenceProperties::project`] drops it.
    fn output_equivalence_properties(
        input: &Arc<dyn ExecutionPlan>,
        input_schema: &SchemaRef,
        output_schema: &SchemaRef,
    ) -> EquivalenceProperties {
        // Grouped by source column: one input column may be produced more than once,
        // and every target of a source has to travel with it.
        let mut sources: Vec<(usize, ProjectionTargets)> = Vec::new();
        for (output_idx, input_idx) in Self::output_to_input_columns(input_schema, output_schema)
            .into_iter()
            .enumerate()
        {
            let Some(input_idx) = input_idx else { continue };
            let target: Arc<dyn PhysicalExpr> = Arc::new(Column::new(
                output_schema.field(output_idx).name(),
                output_idx,
            ));
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
        // `project` also carries the input's `Constraints`, and it carries them
        // wrong for a mapping like this one: `projected_constraints` collects the
        // *target* indices and hands them to `Constraints::project`, which reads them
        // as input projection indices — so a reordered schema keeps `PrimaryKey([0])`
        // while output column 0 is now a different column. A false key claim is worse
        // than none (it feeds `ordering_satisfy_requirement` and the aggregate and
        // join rules), and this exec has no way to state the real one, so drop them.
        input
            .equivalence_properties()
            .project(&mapping, Arc::clone(output_schema))
            .with_constraints(Constraints::default())
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
        // The input's statistics are indexed by its own schema, but this exec
        // advertises `output_schema`, which drops, reorders, or retypes columns
        // (caching mode strips storage-only columns like `_fetched_at`).
        // Forwarding them unchanged reports more `column_statistics` than this
        // node has columns, and a `FilterExec` above then indexes past the end
        // in `AnalysisContext::try_from_statistics` (#14144). Project onto the
        // output schema via the same mapping as the equivalence properties;
        // dropped and retyped columns become unknown.
        let input_stats = self.input.partition_statistics(partition)?;
        let column_map = Self::output_to_input_columns(&self.input.schema(), &self.output_schema);

        let column_statistics: Vec<ColumnStatistics> = column_map
            .into_iter()
            .map(|input_idx| {
                input_idx
                    .and_then(|idx| input_stats.column_statistics.get(idx).cloned())
                    .unwrap_or_else(ColumnStatistics::new_unknown)
            })
            .collect();

        // Sum the retained columns' widths rather than keep the input's total,
        // which still describes the wider child. An unknown column has an absent
        // `byte_size`, which `Precision::add` propagates, so a dropped or retyped
        // column leaves the total absent instead of falsely exact.
        let total_byte_size = column_statistics
            .iter()
            .fold(Precision::Exact(0), |acc, col| acc.add(&col.byte_size));

        Ok(Arc::new(Statistics {
            num_rows: input_stats.num_rows,
            total_byte_size,
            column_statistics,
        }))
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
    use arrow::array::{ArrayRef, Int64Array};
    use arrow::compute::SortOptions;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::common::Constraint;
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
    fn test_partition_statistics_match_output_schema_column_count() {
        // The input carries a storage-only column (`_fetched_at`) that this exec
        // strips, so the statistics must describe the 2-column output, not the
        // 3-column input, or a consumer indexing them against this node's schema
        // goes out of bounds. Regression test for #14144.
        let source = Arc::new(EmptyExec::new(input_schema_with_extra_column()));
        let schema_cast = SchemaCastScanExec::new(source, expected_output_schema());

        let stats = schema_cast
            .partition_statistics(None)
            .expect("partition_statistics should succeed");
        assert_eq!(
            stats.column_statistics.len(),
            2,
            "column statistics count must match the 2-column output schema, not the 3-column input"
        );
    }

    #[test]
    fn test_partition_statistics_do_not_keep_input_total_byte_size() {
        // Dropping a column makes the input's `total_byte_size` describe a wider
        // row than this node emits. It must not survive as an exact statistic:
        // derived from the retained columns, it is either their (smaller) sum or
        // absent, never the input's wider total.
        let input_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
            Field::new("c", DataType::Int64, false),
        ]));
        let output_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&input_schema),
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(Int64Array::from(vec![2])),
                Arc::new(Int64Array::from(vec![3])),
            ],
        )
        .expect("record batch");
        let source =
            MemorySourceConfig::try_new_exec(&[vec![batch]], input_schema, None).expect("source");
        let input_total = source
            .partition_statistics(None)
            .expect("input statistics")
            .total_byte_size;
        assert!(
            matches!(input_total, Precision::Exact(_)),
            "input reports an exact total to guard against"
        );

        let schema_cast = SchemaCastScanExec::new(source, output_schema);
        let total = schema_cast
            .partition_statistics(None)
            .expect("partition_statistics should succeed")
            .total_byte_size;
        assert_ne!(
            total, input_total,
            "total_byte_size must reflect the narrower output, not the input's wider row"
        );
    }

    #[test]
    fn test_numeric_filter_above_schema_cast_analyzes_statistics() {
        // Reproduces #14144: a query filtering on an integer column over a
        // caching-accelerated dataset failed with an `ExprBoundaries` out-of-bounds
        // internal error. The `FilterExec`'s boundary analysis (numeric predicates
        // only, which is why a text-column filter escaped the bug) indexes the
        // child's `column_statistics` against the child's stripped schema, and
        // stale wider statistics ran that index out of bounds.
        let input_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("version", DataType::Int64, true),
            Field::new("_fetched_at", DataType::Int64, true),
            Field::new("__spice_cache_namespace", DataType::Utf8, false),
        ]));
        let output_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("version", DataType::Int64, true),
        ]));

        let source = Arc::new(EmptyExec::new(input_schema));
        let schema_cast: Arc<dyn ExecutionPlan> =
            Arc::new(SchemaCastScanExec::new(source, Arc::clone(&output_schema)));

        let predicate: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            physical_col("id", &output_schema).expect("id column exists"),
            Operator::Eq,
            Arc::new(Literal::new(ScalarValue::Int64(Some(1)))),
        ));
        let filter = FilterExec::try_new(predicate, schema_cast)
            .expect("FilterExec should be constructible");

        // Before the fix this returned the `ExprBoundaries` col_index
        // out-of-bounds internal error instead of `Ok`.
        filter
            .partition_statistics(None)
            .expect("filter statistics analysis must not go out of bounds");
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

    /// The input's constraints are not carried: a key claim is stated by column
    /// index, `project` does not rewrite those indices for a mapping like this one,
    /// and a wrong key is worse than no key — it feeds ordering satisfaction and the
    /// aggregate and join rules.
    #[test]
    fn a_key_constraint_is_not_carried_across_the_schema_cast() {
        let input_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
        ]));
        // The output puts `b` where `a` used to be, so an index-stated key claim that
        // survived unrewritten would name the wrong column.
        let target_schema = Arc::new(Schema::new(vec![
            Field::new("b", DataType::Int64, false),
            Field::new("a", DataType::Int64, false),
        ]));
        let source = MemorySourceConfig::try_new(&[vec![]], Arc::clone(&input_schema), None)
            .expect("memory source");
        let keyed: Arc<dyn ExecutionPlan> =
            Arc::new(DataSourceExec::new(Arc::new(source)).with_constraints(
                Constraints::new_unverified(vec![Constraint::PrimaryKey(vec![0])]),
            ));
        assert!(
            !keyed.equivalence_properties().constraints().is_empty(),
            "precondition: the child declares a primary key on `a`"
        );

        let schema_cast = SchemaCastScanExec::new(keyed, target_schema);

        assert!(
            schema_cast
                .properties()
                .eq_properties
                .constraints()
                .is_empty(),
            "a key claim this exec cannot restate must not be advertised"
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

    // ── projected statistics (#14144) ──

    /// A single-batch memory source whose columns carry `nulls[i]` nulls each,
    /// so each column's exact `null_count` identifies which input column a
    /// projected statistic came from.
    fn source_with_null_counts(
        schema: &SchemaRef,
        nulls: &[usize],
        rows: usize,
    ) -> Arc<dyn ExecutionPlan> {
        let columns = nulls
            .iter()
            .map(|&n| {
                Arc::new(
                    (0..rows)
                        .map(|i| (i >= n).then_some(i64::try_from(i).unwrap_or(0)))
                        .collect::<Int64Array>(),
                ) as ArrayRef
            })
            .collect();
        let batch = RecordBatch::try_new(Arc::clone(schema), columns).expect("valid batch");
        MemorySourceConfig::try_new_exec(&[vec![batch]], Arc::clone(schema), None)
            .expect("memory source")
    }

    /// The child reports one statistic per input column; this exec must report one
    /// per *output* column, taken by name — so dropping and reordering columns
    /// leaves each output entry describing the input column of the matching name.
    #[test]
    fn statistics_are_projected_onto_the_output_schema_by_name() {
        let input_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("value", DataType::Int64, true),
            Field::new("_hidden", DataType::Int64, true),
        ]));
        // id, value, _hidden carry 0, 1, and 2 nulls respectively.
        let source = source_with_null_counts(&input_schema, &[0, 1, 2], 3);

        // Output drops `_hidden` and puts `value` before `id`.
        let target_schema = Arc::new(Schema::new(vec![
            Field::new("value", DataType::Int64, true),
            Field::new("id", DataType::Int64, true),
        ]));
        let stats = SchemaCastScanExec::new(source, target_schema)
            .partition_statistics(None)
            .expect("partition_statistics should succeed");

        assert_eq!(stats.num_rows, Precision::Exact(3));
        assert_eq!(
            stats.column_statistics[0].null_count,
            Precision::Exact(1),
            "output column 0 is `value`, carrying `value`'s statistics"
        );
        assert_eq!(
            stats.column_statistics[1].null_count,
            Precision::Exact(0),
            "output column 1 is `id`, carrying `id`'s statistics"
        );
    }

    /// A retyped column (`Int64` input cast to `Utf8` output) must not carry the
    /// input column's statistics forward: `try_cast_to` converts the values, so an
    /// `Int64` bound would disagree with the `Utf8` field it is now attached to.
    #[test]
    fn retyped_columns_report_unknown_statistics() {
        let input_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let source = source_with_null_counts(&input_schema, &[1], 2);

        let target_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, true)]));
        let stats = SchemaCastScanExec::new(source, target_schema)
            .partition_statistics(None)
            .expect("partition_statistics should succeed");

        assert_eq!(
            stats.column_statistics[0],
            ColumnStatistics::new_unknown(),
            "an Int64 column's statistics must not be advertised for a Utf8 output field"
        );
    }

    /// A `TableProvider` whose scan returns hidden storage columns that
    /// [`SchemaCastScanExec`] strips — the shape a `refresh_mode: caching`
    /// accelerator produces (its `_fetched_at` / `__spice_cache_namespace` columns
    /// live in the accelerator but not in the user-facing schema).
    #[derive(Debug)]
    struct HiddenColumnScan {
        storage: Vec<Vec<RecordBatch>>,
        storage_schema: SchemaRef,
        user_schema: SchemaRef,
    }

    #[async_trait]
    impl TableProvider for HiddenColumnScan {
        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.user_schema)
        }

        fn table_type(&self) -> TableType {
            TableType::Base
        }

        fn supports_filters_pushdown(
            &self,
            filters: &[&Expr],
        ) -> Result<Vec<TableProviderFilterPushDown>> {
            // Inexact, like the caching accelerator, so the optimizer keeps a
            // `FilterExec` above the scan — the node whose boundary analysis reads
            // the statistics this exec advertises.
            Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
        }

        async fn scan(
            &self,
            _state: &dyn Session,
            projection: Option<&Vec<usize>>,
            _filters: &[Expr],
            _limit: Option<usize>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            // The storage scan always carries the hidden column; the user-facing
            // output is the requested projection of the user schema.
            let input = MemorySourceConfig::try_new_exec(
                &self.storage,
                Arc::clone(&self.storage_schema),
                None,
            )?;
            let target_schema = match projection {
                Some(indices) => Arc::new(Schema::new_with_metadata(
                    indices
                        .iter()
                        .filter_map(|&i| {
                            self.user_schema.fields().get(i).map(|f| f.as_ref().clone())
                        })
                        .collect::<Vec<_>>(),
                    self.user_schema.metadata().clone(),
                )),
                None => Arc::clone(&self.user_schema),
            };
            Ok(Arc::new(SchemaCastScanExec::new(input, target_schema)))
        }
    }

    /// An integer-column filter over a scan that hides storage columns must plan
    /// and return the matching rows (regression test for #14144).
    ///
    /// Before the statistics were projected onto the output schema, the scan
    /// advertised a two-column schema while forwarding three column statistics, and
    /// the `id = 1` filter's boundary analysis indexed `col_index = 2` into that
    /// two-field schema — `Internal error: Could not create ExprBoundaries ...
    /// col_index has gone out of bounds`.
    #[tokio::test]
    async fn an_integer_filter_plans_when_the_scan_hides_storage_columns() {
        let storage_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
            Field::new("_hidden", DataType::Int64, true),
        ]));
        let user_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&storage_schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(Int64Array::from(vec![10, 20])),
                Arc::new(Int64Array::from(vec![100, 200])),
            ],
        )
        .expect("valid storage batch");

        let ctx = SessionContext::new();
        ctx.register_table(
            "t",
            Arc::new(HiddenColumnScan {
                storage: vec![vec![batch]],
                storage_schema,
                user_schema,
            }),
        )
        .expect("table registered");

        let batches = collect(&ctx, "SELECT value FROM t WHERE id = 1")
            .await
            .expect("an integer-column filter must plan and execute");
        assert_batches_eq!(
            [
                "+-------+",
                "| value |",
                "+-------+",
                "| 10    |",
                "+-------+",
            ],
            &batches
        );
    }
}
