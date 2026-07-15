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

//! Plan-level "fall back to a secondary plan on zero results" machinery used by compound
//! indexes in [`CompoundReadMode::FallbackToSecondary`](super::CompoundReadMode::FallbackToSecondary).

use std::{any::Any, fmt, sync::Arc};

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::{
    catalog::Session,
    datasource::{DefaultTableSource, TableProvider, TableType},
    error::{DataFusionError, Result as DataFusionResult},
    execution::{SendableRecordBatchStream, TaskContext},
    logical_expr::{Expr, LogicalPlan, LogicalPlanBuilder},
    physical_expr::EquivalenceProperties,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
        PlanProperties, coalesce_partitions::CoalescePartitionsExec,
        stream::RecordBatchStreamAdapter,
    },
    prelude::cast,
};
use datafusion_expr::ident;
use futures::{StreamExt, TryStreamExt, stream};

/// Build a [`LogicalPlan`] that returns the rows of `primary`, or — if `primary` produces
/// zero rows — the rows of `secondary`, projected and cast onto the primary plan's schema.
///
/// The fallback decision is made at execution time by [`FallbackOnEmptyScanExec`]; the
/// secondary plan is only executed when the primary produced no rows.
///
/// Every column of the primary plan's schema must exist (by unqualified name) in the
/// secondary plan's schema; otherwise a plan error is returned. Columns whose types differ
/// are cast to the primary's type.
pub(super) fn fallback_on_empty_plan(
    primary: Arc<LogicalPlan>,
    secondary: Arc<LogicalPlan>,
) -> Result<LogicalPlan, DataFusionError> {
    let primary_schema = primary.schema();
    let secondary_schema = secondary.schema();

    let projection = primary_schema
        .fields()
        .iter()
        .map(|field| {
            let name = field.name();
            let Ok(secondary_field) = secondary_schema.field_with_unqualified_name(name) else {
                return Err(DataFusionError::Plan(format!(
                    "Cannot fall back from the primary to the secondary index: the secondary index does not provide column '{name}'. Configure the secondary index with the same columns as the primary, or disable fallback."
                )));
            };
            let expr = if secondary_field.data_type() == field.data_type() {
                ident(name)
            } else {
                cast(ident(name), field.data_type().clone())
            };
            Ok(expr.alias(name))
        })
        .collect::<Result<Vec<Expr>, DataFusionError>>()?;

    let schema = Arc::new(primary_schema.as_arrow().clone());
    let secondary_projected = LogicalPlanBuilder::new_from_arc(secondary)
        .project(projection)?
        .build()?;

    let provider = Arc::new(FallbackOnEmptyTableProvider {
        schema,
        primary,
        secondary: Arc::new(secondary_projected),
    });
    LogicalPlanBuilder::scan(
        "compound_index",
        Arc::new(DefaultTableSource::new(provider as Arc<dyn TableProvider>)),
        None,
    )?
    .build()
}

/// A [`TableProvider`] over two [`LogicalPlan`]s with identical schemas: scans return the
/// primary plan's rows, falling back to the secondary plan when the primary is empty.
#[derive(Debug)]
struct FallbackOnEmptyTableProvider {
    schema: SchemaRef,
    primary: Arc<LogicalPlan>,
    /// Already projected onto `schema` by [`fallback_on_empty_plan`].
    secondary: Arc<LogicalPlan>,
}

#[async_trait]
impl TableProvider for FallbackOnEmptyTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        // The projection and limit apply identically to both sides: limiting each side to `n`
        // rows commutes with "primary if non-empty, else secondary" (a non-empty primary stays
        // non-empty under a limit of n >= 1; DataFusion never pushes down a zero limit as a
        // scan, and even then both sides would be empty).
        let apply = |plan: &Arc<LogicalPlan>| -> DataFusionResult<LogicalPlan> {
            let mut builder = LogicalPlanBuilder::new_from_arc(Arc::clone(plan));
            if let Some(indices) = projection {
                builder =
                    builder.project(indices.iter().map(|i| ident(self.schema.field(*i).name())))?;
            }
            if limit.is_some() {
                builder = builder.limit(0, limit)?;
            }
            builder.build()
        };
        let primary = state.create_physical_plan(&apply(&self.primary)?).await?;
        let secondary = state.create_physical_plan(&apply(&self.secondary)?).await?;
        Ok(Arc::new(FallbackOnEmptyScanExec::new(primary, secondary)))
    }
}

/// Executes the primary plan; if it yields zero rows in total, executes the secondary plan.
///
/// Only zero-row batches are buffered (i.e. nothing is): the primary's output streams through
/// unchanged as soon as its first non-empty batch arrives, and the secondary plan is never
/// executed in that case.
struct FallbackOnEmptyScanExec {
    primary: Arc<dyn ExecutionPlan>,
    secondary: Arc<dyn ExecutionPlan>,
    properties: PlanProperties,
}

impl FallbackOnEmptyScanExec {
    fn new(mut primary: Arc<dyn ExecutionPlan>, mut secondary: Arc<dyn ExecutionPlan>) -> Self {
        if primary.output_partitioning().partition_count() != 1 {
            primary = Arc::new(CoalescePartitionsExec::new(primary));
        }
        if secondary.output_partitioning().partition_count() != 1 {
            secondary = Arc::new(CoalescePartitionsExec::new(secondary));
        }
        let properties = PlanProperties::new(
            EquivalenceProperties::new(primary.schema()),
            Partitioning::UnknownPartitioning(1),
            primary.pipeline_behavior(),
            primary.boundedness(),
        );
        Self {
            primary,
            secondary,
            properties,
        }
    }
}

impl fmt::Debug for FallbackOnEmptyScanExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FallbackOnEmptyScanExec")
    }
}

impl DisplayAs for FallbackOnEmptyScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FallbackOnEmptyScanExec")
    }
}

impl ExecutionPlan for FallbackOnEmptyScanExec {
    fn name(&self) -> &'static str {
        "FallbackOnEmptyScanExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.primary, &self.secondary]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let [primary, secondary]: [Arc<dyn ExecutionPlan>; 2] =
            children.try_into().map_err(|_| {
                DataFusionError::Internal(
                    "FallbackOnEmptyScanExec requires exactly two children".to_string(),
                )
            })?;
        Ok(Arc::new(Self::new(primary, secondary)))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "FallbackOnEmptyScanExec has a single output partition, but partition {partition} was requested"
            )));
        }
        let primary = Arc::clone(&self.primary);
        let secondary = Arc::clone(&self.secondary);
        let stream = stream::once(async move {
            let mut primary_stream = primary.execute(0, Arc::clone(&context))?;
            while let Some(batch) = primary_stream.next().await {
                let batch = batch?;
                if batch.num_rows() > 0 {
                    return Ok::<_, DataFusionError>(
                        stream::iter([Ok(batch)]).chain(primary_stream).boxed(),
                    );
                }
            }
            Ok(secondary.execute(0, context)?.boxed())
        })
        .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}
