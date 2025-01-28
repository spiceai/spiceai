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

#![allow(clippy::missing_errors_doc)]
use std::{any::Any, borrow::Cow, error::Error, sync::Arc};

use ::arrow::{
    array::{ArrayRef, RecordBatch, UInt64Array},
    datatypes::{DataType, Field, Schema, SchemaRef},
};
use async_trait::async_trait;
use datafusion::{
    catalog::Session,
    common::Constraints,
    datasource::{TableProvider, TableType},
    error::{DataFusionError, Result as DataFusionResult},
    execution::{SendableRecordBatchStream, TaskContext},
    logical_expr::{dml::InsertOp, Expr, LogicalPlan},
    physical_expr::EquivalenceProperties,
    physical_plan::{
        stream::RecordBatchStreamAdapter, DisplayAs, DisplayFormatType, ExecutionMode,
        ExecutionPlan, Partitioning, PlanProperties,
    },
};

use crate::poly::PolyTableProvider;

#[async_trait]
pub trait DeletionTableProvider: TableProvider {
    async fn delete_from(
        &self,
        _state: &dyn Session,
        _filters: &[Expr],
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Plan("Not implemented".to_string()))
    }
}

#[async_trait]
pub trait DeletionSink: Send + Sync {
    async fn delete_from(&self) -> Result<u64, Box<dyn Error + Send + Sync>>;
}

pub struct DeletionExec {
    deletion_sink: Arc<dyn DeletionSink + 'static>,
    properties: PlanProperties,
}

impl DeletionExec {
    pub fn new(deletion_sink: Arc<dyn DeletionSink>, schema: &SchemaRef) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(schema)),
            Partitioning::UnknownPartitioning(1),
            ExecutionMode::Bounded,
        );
        Self {
            deletion_sink,
            properties,
        }
    }
}

impl std::fmt::Debug for DeletionExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeleteExec").finish_non_exhaustive()
    }
}

impl DisplayAs for DeletionExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "DeleteExec")
            }
        }
    }
}

impl ExecutionPlan for DeletionExec {
    fn name(&self) -> &'static str {
        "DeletionExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &datafusion::physical_plan::PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        let count_schema = Arc::new(Schema::new(vec![Field::new(
            "count",
            DataType::UInt64,
            false,
        )]));

        let deletion_sink = Arc::clone(&self.deletion_sink);
        Ok(Box::pin(RecordBatchStreamAdapter::new(count_schema, {
            futures::stream::once(async move {
                let count = deletion_sink
                    .delete_from()
                    .await
                    .map_err(datafusion::error::DataFusionError::from)?;
                let array = Arc::new(UInt64Array::from(vec![count])) as ArrayRef;
                if let Ok(batch) =
                    RecordBatch::try_from_iter_with_nullable(vec![("count", array, false)])
                {
                    Ok(batch)
                } else {
                    Err(DataFusionError::Execution(
                        "failed to create record batch".to_string(),
                    ))
                }
            })
        })))
    }
}

#[derive(Debug)]
pub struct DeletionTableProviderAdapter {
    source: Arc<dyn DeletionTableProvider>,
}

impl DeletionTableProviderAdapter {
    pub fn new(source: Arc<dyn DeletionTableProvider>) -> Self {
        Self { source }
    }
}

#[allow(clippy::needless_pass_by_value)]
pub fn get_deletion_provider(
    from: Arc<dyn TableProvider>,
) -> Option<Arc<dyn DeletionTableProvider>> {
    if let Some(p) = from.as_any().downcast_ref::<PolyTableProvider>() {
        return Some(Arc::new(p.clone()));
    }
    if let Some(p) = from.as_any().downcast_ref::<DeletionTableProviderAdapter>() {
        return Some(Arc::clone(&p.source));
    }

    None
}

#[async_trait]
impl TableProvider for DeletionTableProviderAdapter {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        self.source.schema()
    }
    fn constraints(&self) -> Option<&Constraints> {
        self.source.constraints()
    }
    fn table_type(&self) -> TableType {
        self.source.table_type()
    }
    fn get_logical_plan(&self) -> Option<Cow<LogicalPlan>> {
        self.source.get_logical_plan()
    }
    fn get_column_default(&self, column: &str) -> Option<&Expr> {
        self.source.get_column_default(column)
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        self.source.scan(state, projection, filters, limit).await
    }

    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        overwrite: InsertOp,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        self.source.insert_into(state, input, overwrite).await
    }
}
