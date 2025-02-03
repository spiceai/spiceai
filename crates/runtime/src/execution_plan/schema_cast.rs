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
use async_stream::stream;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};
use futures::StreamExt;
use std::any::Any;
use std::clone::Clone;
use std::fmt;
use std::ops::Deref;
use std::sync::Arc;

pub struct SchemaCastScanExec {
    input: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
    properties: PlanProperties,
}

impl SchemaCastScanExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, schema: SchemaRef) -> Self {
        let eq_properties = input.equivalence_properties().clone();
        let execution_mode = input.execution_mode();
        let properties = PlanProperties::new(
            eq_properties,
            input.output_partitioning().clone(),
            execution_mode,
        );
        Self {
            input,
            schema,
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
            .field("schema", &self.schema)
            .field("properties", &self.properties)
            .finish()
    }
}

impl ExecutionPlan for SchemaCastScanExec {
    fn name(&self) -> &'static str {
        "SchemaCastScanExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(
            Schema::new(
                self.input
                    .schema()
                    .fields()
                    .into_iter()
                    .map(|field| {
                        self.schema
                            .field_with_name(field.name())
                            .ok()
                            .map_or(field.deref().clone(), Clone::clone)
                    })
                    .collect::<Vec<Field>>(),
            )
            .with_metadata(self.input.schema().metadata().clone()),
        )
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
                Arc::clone(&self.schema),
            )))
        } else {
            Err(DataFusionError::Execution(
                "SchemaCastScanExec expects exactly one input".to_string(),
            ))
        }
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
                        let batch = arrow_tools::record_batch::try_cast_to(batch?, Arc::clone(&schema));
                        yield batch.map_err(|e| { DataFusionError::External(Box::new(e)) });
                    }
                }
            },
        )))
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
        Ok(Arc::new(SchemaCastScanExec::new(input, self.schema())))
    }
}
