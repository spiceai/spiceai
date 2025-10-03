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

use std::{any::Any, fmt, sync::Arc};

use aws_sdk_s3vectors::types::Index;
use datafusion::{
    arrow::datatypes::SchemaRef,
    common::Result,
    execution::{SendableRecordBatchStream, TaskContext},
    physical_expr::EquivalenceProperties,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
        execution_plan::{Boundedness, EmissionType},
    },
};

use crate::S3Vectors;

static NAME: &str = "ListVectorsExec";

pub struct ListVectorsExec {
    client: Arc<dyn S3Vectors + Send + Sync>,
    index: Index,
    properties: PlanProperties,
    schema: SchemaRef,
}

impl ListVectorsExec {
    pub fn new(client: Arc<dyn S3Vectors + Send + Sync>, index: Index, schema: SchemaRef) -> Self {
        let eq_properties = EquivalenceProperties::new(Arc::clone(&schema));
        let partitioning = Partitioning::UnknownPartitioning(1); // TODO optimize?
        let emission_type = EmissionType::Incremental;
        let boundedness = Boundedness::Bounded;

        let properties =
            PlanProperties::new(eq_properties, partitioning, emission_type, boundedness);

        Self {
            client,
            index,
            properties,
            schema,
        }
    }
}

impl fmt::Debug for ListVectorsExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{NAME} bucket={} index={}",
            self.index.vector_bucket_name(),
            self.index.index_name
        )
    }
}

impl DisplayAs for ListVectorsExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl ExecutionPlan for ListVectorsExec {
    fn name(&self) -> &'static str {
        NAME
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        todo!()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        todo!()
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        todo!()
    }
}
