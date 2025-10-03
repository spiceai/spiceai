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
    common::{Result, Statistics},
    datasource::source::DataSource,
    execution::{SendableRecordBatchStream, TaskContext},
    physical_expr::EquivalenceProperties,
    physical_plan::{
        DisplayFormatType, ExecutionPlan, Partitioning, projection::ProjectionExec,
        stream::RecordBatchStreamAdapter,
    },
};

use crate::S3Vectors;

static NAME: &str = "QueryVectorsSource";

pub struct QueryVectorsSource {
    client: Arc<dyn S3Vectors + Send + Sync>,
    index: Index,
    schema: SchemaRef,
    partitioning: Partitioning,
    eq_properties: EquivalenceProperties,
}

impl fmt::Debug for QueryVectorsSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{NAME} bucket={} index={}",
            self.index.vector_bucket_name(),
            self.index.index_name
        )
    }
}

impl QueryVectorsSource {
    pub fn new(client: Arc<dyn S3Vectors + Send + Sync>, index: Index, schema: SchemaRef) -> Self {
        let eq_properties = EquivalenceProperties::new(Arc::clone(&schema));
        let partitioning = Partitioning::UnknownPartitioning(1);

        Self {
            client,
            index,
            schema,
            partitioning,
            eq_properties,
        }
    }
}

impl DataSource for QueryVectorsSource {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn open(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let schema = Arc::clone(&self.schema);
        let stream = todo!();
        // Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }

    fn statistics(&self) -> Result<Statistics> {
        Ok(Statistics::new_unknown(&self.schema))
    }

    fn output_partitioning(&self) -> Partitioning {
        self.partitioning.clone()
    }

    fn eq_properties(&self) -> EquivalenceProperties {
        self.eq_properties.clone()
    }

    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{self:?}")
    }

    fn with_fetch(&self, _limit: Option<usize>) -> Option<Arc<dyn DataSource>> {
        None
    }

    fn fetch(&self) -> Option<usize> {
        None
    }

    fn try_swapping_with_projection(
        &self,
        _projection: &ProjectionExec,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        Ok(None)
    }
}
