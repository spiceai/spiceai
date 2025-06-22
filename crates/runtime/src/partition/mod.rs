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

use std::{any::Any, collections::HashMap, fmt, sync::Arc};

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::{
    catalog::{Session, TableProvider},
    common::Constraints,
    datasource::TableType,
    error::DataFusionError,
    execution::{SendableRecordBatchStream, TaskContext},
    logical_expr::CreateExternalTable,
    physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties},
    prelude::Expr,
};

// [`datafusion::scalar::ScalarValue`] does not necessary traits for Hash key.
type ScalarValueString = String;

#[derive(Debug)]
pub struct PartitionTableProvider {
    schema: SchemaRef,
    _partition_by: Vec<Expr>,
    _partitions: HashMap<ScalarValueString, Arc<dyn TableProvider>>,
    _creator: PartitionCreator,
}

#[derive(Debug)]
struct PartitionCreator {}

impl PartitionTableProvider {
    #[must_use]
    pub fn new(_partition_by: Vec<Expr>, _cmd: &CreateExternalTable) -> Self {
        todo!()
    }
}

#[async_trait]
impl TableProvider for PartitionTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn constraints(&self) -> Option<&Constraints> {
        None
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        todo!()
    }
}

#[derive(Debug)]
pub struct PartitionedTableInsert {
    properties: PlanProperties,
    projected_schema: SchemaRef,
}

impl ExecutionPlan for PartitionedTableInsert {
    fn name(&self) -> &'static str {
        "PartitionedTableInsert"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.projected_schema)
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        todo!()
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        todo!()
    }
}

impl DisplayAs for PartitionedTableInsert {
    fn fmt_as(&self, _t: DisplayFormatType, _f: &mut fmt::Formatter) -> fmt::Result {
        todo!()
    }
}
