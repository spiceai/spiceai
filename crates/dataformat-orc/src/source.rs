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

//! [`FileSource`] / [`FileOpener`] that streams ORC stripes as Arrow batches.

use std::sync::Arc;

use arrow::array::new_null_array;
use arrow::datatypes::Schema;
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_scan_config::FileScanConfig;
use datafusion_datasource::file_stream::{FileOpenFuture, FileOpener};
use datafusion_datasource::projection::{ProjectionOpener, SplitProjection};
use datafusion_datasource::{PartitionedFile, TableSchema};
use datafusion_physical_expr::projection::ProjectionExprs;
use futures::StreamExt;
use object_store::ObjectStore;
use orc_rust::arrow_reader::ArrowReaderBuilder;
use orc_rust::projection::ProjectionMask;

use crate::file_format::orc_to_datafusion_error;
use crate::object_store_reader::ObjectStoreReader;

/// Holds the extra configuration [`OrcOpener`] needs for a scan.
#[derive(Clone)]
pub struct OrcSource {
    batch_size: Option<usize>,
    metrics: ExecutionPlanMetricsSet,
    table_schema: TableSchema,
    projection: SplitProjection,
}

impl OrcSource {
    /// Initialize an [`OrcSource`] with the given `table_schema`.
    #[must_use]
    pub fn new(table_schema: TableSchema) -> Self {
        let projection = SplitProjection::unprojected(&table_schema);
        Self {
            batch_size: None,
            metrics: ExecutionPlanMetricsSet::new(),
            table_schema,
            projection,
        }
    }
}

impl FileSource for OrcSource {
    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        base_config: &FileScanConfig,
        _partition: usize,
    ) -> Result<Arc<dyn FileOpener>> {
        let file_schema = self.table_schema.file_schema();
        let projected_schema = Arc::new(file_schema.project(&self.projection.file_indices)?);

        let opener: Arc<dyn FileOpener> = Arc::new(OrcOpener {
            batch_size: self.batch_size.or(base_config.batch_size).unwrap_or(8192),
            projected_schema,
            object_store,
        });

        ProjectionOpener::try_new(self.projection.clone(), opener, file_schema)
    }

    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> {
        let mut conf = self.clone();
        conf.batch_size = Some(batch_size);
        Arc::new(conf)
    }

    fn try_pushdown_projection(
        &self,
        projection: &ProjectionExprs,
    ) -> Result<Option<Arc<dyn FileSource>>> {
        let mut source = self.clone();
        let new_projection = self.projection.source.try_merge(projection)?;
        let split_projection =
            SplitProjection::new(self.table_schema.file_schema(), &new_projection);
        source.projection = split_projection;
        Ok(Some(Arc::new(source)))
    }

    fn projection(&self) -> Option<&ProjectionExprs> {
        Some(&self.projection.source)
    }

    fn table_schema(&self) -> &TableSchema {
        &self.table_schema
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.metrics
    }

    fn file_type(&self) -> &'static str {
        "orc"
    }
}

/// Reorder ORC columns so the batch matches the projected file schema, and
/// backfill projected fields this file does not have with typed NULL arrays.
///
/// `orc-rust` 0.8.0 `ProjectionMask::named_roots` emits columns in file order
/// (not `SELECT` order) and silently omits names absent from the file. A
/// listing table whose schema was merged across files therefore cannot look
/// up every projected field in the batch; missing fields become NULLs.
fn align_orc_batch(batch: &RecordBatch, schema: &Arc<Schema>) -> Result<RecordBatch> {
    let columns: Vec<_> = schema
        .fields()
        .iter()
        .map(|field| match batch.column_by_name(field.name()) {
            Some(column) => Arc::clone(column),
            None => new_null_array(field.data_type(), batch.num_rows()),
        })
        .collect();

    RecordBatch::try_new_with_options(
        Arc::clone(schema),
        columns,
        &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
    )
    .map_err(|err| DataFusionError::ArrowError(Box::new(err), None))
}

/// Opens one ORC object and yields a stream of [`arrow::record_batch::RecordBatch`]es.
pub struct OrcOpener {
    batch_size: usize,
    projected_schema: Arc<Schema>,
    object_store: Arc<dyn ObjectStore>,
}

impl FileOpener for OrcOpener {
    fn open(&self, partitioned_file: PartitionedFile) -> Result<FileOpenFuture> {
        let store = Arc::clone(&self.object_store);
        let batch_size = self.batch_size;
        let projected_schema = Arc::clone(&self.projected_schema);
        let file_range = partitioned_file.range.clone();

        Ok(Box::pin(async move {
            let reader = ObjectStoreReader::new(store, partitioned_file.object_meta.clone());
            let builder = ArrowReaderBuilder::try_new_async(reader)
                .await
                .map_err(orc_to_datafusion_error)?;

            let builder = if projected_schema.fields().is_empty() {
                builder
            } else {
                let names: Vec<String> = projected_schema
                    .fields()
                    .iter()
                    .map(|field| field.name().clone())
                    .collect();
                let projection =
                    ProjectionMask::named_roots(builder.file_metadata().root_data_type(), &names);
                builder.with_projection(projection)
            };

            let builder = builder.with_batch_size(batch_size);
            let builder = match file_range {
                Some(range) => match (usize::try_from(range.start), usize::try_from(range.end)) {
                    (Ok(start), Ok(end)) if end >= start => {
                        builder.with_file_byte_range(start..end)
                    }
                    _ => builder,
                },
                None => builder,
            };

            let stream = builder.build_async();
            Ok(stream
                .map(move |result| {
                    result
                        .map_err(|err| DataFusionError::ArrowError(Box::new(err), None))
                        .and_then(|batch| align_orc_batch(&batch, &projected_schema))
                })
                .boxed())
        }))
    }
}
