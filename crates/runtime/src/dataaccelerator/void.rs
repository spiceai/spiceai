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

//! The void accelerator is an accelerator engine that discards all data sent to it.
//!
//! This is useful for indexes that are injected into the data acceleration read pipeline, but
//! do not need to persist the data.

use std::{any::Any, fmt, sync::Arc};

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::{
    catalog::Session,
    common::project_schema,
    datasource::{TableProvider, TableType},
    error::Result as DataFusionResult,
    execution::{SendableRecordBatchStream, TaskContext},
    logical_expr::{CreateExternalTable, dml::InsertOp},
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, empty::EmptyExec, metrics::MetricsSet,
    },
    prelude::Expr,
};
use datafusion_datasource::sink::{DataSink, DataSinkExec};
use futures::StreamExt;

use super::{AccelerationSource, DataAccelerator};
use crate::parameters::ParameterSpec;

pub struct VoidAccelerator {}

impl VoidAccelerator {
    #[must_use]
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for VoidAccelerator {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl DataAccelerator for VoidAccelerator {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "void"
    }

    async fn create_external_table(
        &self,
        cmd: CreateExternalTable,
        _source: Option<&dyn AccelerationSource>,
        _partition_by: Vec<Expr>,
    ) -> Result<Arc<dyn TableProvider>, Box<dyn std::error::Error + Send + Sync>> {
        Ok(Arc::new(VoidTable::new(Arc::clone(cmd.schema.inner()))))
    }

    fn prefix(&self) -> &'static str {
        "void"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        &[]
    }
}

#[derive(Debug)]
pub struct VoidTable {
    schema: SchemaRef,
}

impl VoidTable {
    #[must_use]
    pub fn new(schema: SchemaRef) -> Self {
        Self { schema }
    }
}

#[async_trait]
impl TableProvider for VoidTable {
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
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let projected_schema = project_schema(&self.schema, projection)?;
        Ok(Arc::new(EmptyExec::new(projected_schema)))
    }

    async fn insert_into(
        &self,
        _state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        _overwrite: InsertOp,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let sink = Arc::new(VoidSink::new(Arc::clone(&self.schema)));
        Ok(Arc::new(DataSinkExec::new(input, sink, None)))
    }
}

#[derive(Debug)]
struct VoidSink {
    schema: SchemaRef,
}

impl VoidSink {
    pub fn new(schema: SchemaRef) -> Self {
        Self { schema }
    }
}

impl DisplayAs for VoidSink {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(f, "VoidSink")
            }
        }
    }
}

#[async_trait]
impl DataSink for VoidSink {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    async fn write_all(
        &self,
        mut data: SendableRecordBatchStream,
        _context: &Arc<TaskContext>,
    ) -> DataFusionResult<u64> {
        let mut row_count = 0;
        while let Some(batch) = data.next().await.transpose()? {
            row_count += batch.num_rows();
        }

        Ok(row_count as u64)
    }
}
