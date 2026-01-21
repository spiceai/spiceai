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
    PhysicalExpr, PlanProperties,
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
    properties: PlanProperties,
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

        // Create equivalence properties with the actual output schema
        let eq_properties = EquivalenceProperties::new(Arc::clone(&output_schema));
        let emission_type = input.pipeline_behavior();
        let boundedness = input.boundedness();
        let properties = PlanProperties::new(
            eq_properties,
            input.output_partitioning().clone(),
            emission_type,
            boundedness,
        );
        Self {
            input,
            target_schema: schema,
            output_schema,
            properties,
        }
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
    fn name(&self) -> &'static str {
        "SchemaCastScanExec"
    }

    fn static_name() -> &'static str
    where
        Self: Sized,
    {
        "SchemaCastScanExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
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
        vec![false; self.children().len()]
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

    fn statistics(&self) -> Result<Statistics> {
        #[expect(deprecated)]
        self.input.statistics()
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
        self.input.partition_statistics(partition)
    }

    // Allow optimizer to push limits through to inputs
    fn supports_limit_pushdown(&self) -> bool {
        // TODO: https://github.com/spiceai/spiceai/issues/7892
        false
    }

    fn with_fetch(&self, _limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        None
    }

    fn fetch(&self) -> Option<usize> {
        None
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
    fn as_any(&self) -> &dyn Any {
        self
    }

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
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_plan::empty::EmptyExec;

    fn input_schema_with_extra_column() -> SchemaRef {
        // Input has 3 columns including an internal "fetched_at" column
        Arc::new(Schema::new(vec![
            Field::new("request_path", DataType::Utf8, false),
            Field::new("content", DataType::Utf8, true),
            Field::new("fetched_at", DataType::Int64, true),
        ]))
    }

    fn expected_output_schema() -> SchemaRef {
        // User expects only 2 columns (no fetched_at)
        Arc::new(Schema::new(vec![
            Field::new("request_path", DataType::Utf8, false),
            Field::new("content", DataType::Utf8, true),
        ]))
    }

    #[test]
    fn test_schema_returns_expected_schema_not_input_schema() {
        // Simulates the cache HIT scenario from GitHub issue #9019:
        // Input has 3 columns (including internal fetched_at), but user only requested 2 columns.
        // SchemaCastScanExec should return the expected 2-column schema, not the input's 3-column schema.
        let input = Arc::new(EmptyExec::new(input_schema_with_extra_column()));
        let expected_schema = expected_output_schema();

        let schema_cast = SchemaCastScanExec::new(input, Arc::clone(&expected_schema));

        let actual_schema = schema_cast.schema();
        assert_eq!(
            actual_schema.fields().len(),
            2,
            "Schema should have 2 fields, not 3 (fetched_at should be stripped)"
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
}
