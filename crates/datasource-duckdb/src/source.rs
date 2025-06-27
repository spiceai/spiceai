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

use std::{any::Any, sync::Arc};

use arrow::datatypes::SchemaRef;
use datafusion::{
    common::Statistics,
    datasource::physical_plan::{FileMeta, FileOpenFuture, FileOpener, FileScanConfig, FileSource},
    error::DataFusionError,
    physical_plan::metrics::ExecutionPlanMetricsSet,
};
use duckdb::Connection;
use object_store::ObjectStore;

#[derive(Debug, Default)]
pub struct DuckDBSource {}

impl FileSource for DuckDBSource {
    fn create_file_opener(
        &self,
        _object_store: Arc<dyn ObjectStore>,
        _base_config: &FileScanConfig,
        _partition: usize,
    ) -> Arc<dyn FileOpener> {
        todo!()
    }

    fn as_any(&self) -> &dyn Any {
        todo!()
    }

    fn with_batch_size(&self, _batch_size: usize) -> Arc<dyn FileSource> {
        todo!()
    }

    fn with_schema(&self, _schema: SchemaRef) -> Arc<dyn FileSource> {
        todo!()
    }

    fn with_projection(&self, _config: &FileScanConfig) -> Arc<dyn FileSource> {
        todo!()
    }

    fn with_statistics(&self, _statistics: Statistics) -> Arc<dyn FileSource> {
        todo!()
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        todo!()
    }

    fn statistics(&self) -> Result<Statistics, DataFusionError> {
        todo!()
    }

    fn file_type(&self) -> &str {
        todo!()
    }
}

pub struct DuckDBOpener;

impl FileOpener for DuckDBOpener {
    fn open(&self, file_meta: FileMeta) -> Result<FileOpenFuture, DataFusionError> {
        todo!()
    }
}
