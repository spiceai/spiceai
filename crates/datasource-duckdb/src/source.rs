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

use arrow::{array::RecordBatch, datatypes::SchemaRef, error::ArrowError};
use datafusion::{
    common::Statistics,
    datasource::physical_plan::{FileMeta, FileOpenFuture, FileOpener, FileScanConfig, FileSource},
    error::DataFusionError,
    physical_plan::metrics::ExecutionPlanMetricsSet,
};
use futures::stream::BoxStream;
use object_store::ObjectStore;
use tokio_stream::wrappers::ReceiverStream;

use crate::EXTENSION;

#[derive(Debug, Clone)]
pub struct DuckDbSource {
    schema: Option<SchemaRef>,
    statistics: Statistics,
    metrics: ExecutionPlanMetricsSet,
}

impl Default for DuckDbSource {
    fn default() -> Self {
        Self {
            schema: None,
            statistics: Statistics::default(),
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl FileSource for DuckDbSource {
    fn create_file_opener(
        &self,
        _object_store: Arc<dyn ObjectStore>,
        _base_config: &FileScanConfig,
        _partition: usize,
    ) -> Arc<dyn FileOpener> {
        Arc::new(DuckDbOpener)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn with_batch_size(&self, _batch_size: usize) -> Arc<dyn FileSource> {
        Arc::new(self.clone())
    }

    fn with_schema(&self, schema: SchemaRef) -> Arc<dyn FileSource> {
        let mut new_source = self.clone();
        new_source.schema = Some(schema);
        Arc::new(new_source)
    }

    fn with_projection(&self, _config: &FileScanConfig) -> Arc<dyn FileSource> {
        Arc::new(self.clone())
    }

    fn with_statistics(&self, statistics: Statistics) -> Arc<dyn FileSource> {
        let mut new_source = self.clone();
        new_source.statistics = statistics;
        Arc::new(new_source)
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.metrics
    }

    fn statistics(&self) -> Result<Statistics, DataFusionError> {
        Ok(self.statistics.clone())
    }

    fn file_type(&self) -> &str {
        EXTENSION
    }
}

pub struct DuckDbOpener;

impl FileOpener for DuckDbOpener {
    fn open(&self, file_meta: FileMeta) -> Result<FileOpenFuture, DataFusionError> {
        let (tx, rx) = tokio::sync::mpsc::channel(10);

        // DuckDB connection is not thread safe and so cannot be in async code.
        // We handle the reading in a single thread and send into a channel to
        // be converted into a `Stream`
        std::thread::spawn(move || {
            let table = String::new(); // TODO
            let path = file_meta.location().to_string();
            let conn = duckdb::Connection::open(&path).unwrap();
            let mut stmt = conn.prepare(&format!("SELECT * FROM {table}")).unwrap();
            let mut arrow_stream = stmt.query_arrow([]).unwrap();
            while let Some(record) = arrow_stream.next() {
                tx.blocking_send(Ok(record)).unwrap();
            }
        });

        let record_stream: BoxStream<'static, Result<RecordBatch, ArrowError>> =
            Box::pin(ReceiverStream::new(rx));
        Ok(Box::pin(async move { Ok(record_stream) }))
    }
}
