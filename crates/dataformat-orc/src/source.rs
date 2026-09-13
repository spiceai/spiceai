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

use arrow::array::{
    Array, ArrayRef, AsArray, FixedSizeListArray, LargeListArray, ListArray, MapArray, StructArray,
    new_null_array,
};
use arrow::datatypes::{DataType, Field, Fields, Schema};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::error::{DataFusionError, Result};
use datafusion::parquet::arrow::async_reader::ObjectVersionType;
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
            object_versioning_type: base_config.object_versioning_type.clone(),
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
///
/// Alignment walks nested `Struct` / list / map types as well. Arrow merges
/// struct children recursively, so a file that only has `payload.id` must
/// still produce a `payload` struct with a typed-NULL `extra` child.
fn align_orc_batch(batch: &RecordBatch, schema: &Arc<Schema>) -> Result<RecordBatch> {
    let columns: Vec<ArrayRef> = schema
        .fields()
        .iter()
        .map(|field| match batch.column_by_name(field.name()) {
            Some(column) => align_array_to_type(column, field.data_type()),
            None => Ok(new_null_array(field.data_type(), batch.num_rows())),
        })
        .collect::<Result<Vec<_>>>()?;

    RecordBatch::try_new_with_options(
        Arc::clone(schema),
        columns,
        &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
    )
    .map_err(|err| DataFusionError::ArrowError(Box::new(err), None))
}

fn type_mismatch(actual: &DataType, expected: &DataType) -> Result<ArrayRef> {
    Err(DataFusionError::ArrowError(
        Box::new(arrow::error::ArrowError::SchemaError(format!(
            "ORC column type {actual} cannot be aligned to merged type {expected}"
        ))),
        None,
    ))
}

fn align_array_to_type(array: &ArrayRef, expected: &DataType) -> Result<ArrayRef> {
    if array.data_type() == expected {
        return Ok(Arc::clone(array));
    }

    match expected {
        DataType::Struct(expected_fields) => align_struct_array(array, expected_fields),
        DataType::List(expected_field) => align_list_array(array, expected_field),
        DataType::LargeList(expected_field) => align_large_list_array(array, expected_field),
        DataType::FixedSizeList(expected_field, size) => {
            align_fixed_size_list_array(array, expected_field, *size)
        }
        DataType::Map(expected_field, sorted) => align_map_array(array, expected_field, *sorted),
        _ => type_mismatch(array.data_type(), expected),
    }
}

fn align_struct_array(array: &ArrayRef, expected_fields: &Fields) -> Result<ArrayRef> {
    let Some(struct_array) = array.as_struct_opt() else {
        return type_mismatch(
            array.data_type(),
            &DataType::Struct(expected_fields.clone()),
        );
    };
    let num_rows = struct_array.len();
    let children = expected_fields
        .iter()
        .map(|field| match struct_array.column_by_name(field.name()) {
            Some(child) => align_array_to_type(child, field.data_type()),
            None => Ok(new_null_array(field.data_type(), num_rows)),
        })
        .collect::<Result<Vec<_>>>()?;

    StructArray::try_new(
        expected_fields.clone(),
        children,
        struct_array.nulls().cloned(),
    )
    .map(|aligned| Arc::new(aligned) as ArrayRef)
    .map_err(|err| DataFusionError::ArrowError(Box::new(err), None))
}

fn align_list_array(array: &ArrayRef, expected_field: &Arc<Field>) -> Result<ArrayRef> {
    let Some(list) = array.as_list_opt::<i32>() else {
        return type_mismatch(
            array.data_type(),
            &DataType::List(Arc::clone(expected_field)),
        );
    };
    let aligned_values = align_array_to_type(list.values(), expected_field.data_type())?;
    ListArray::try_new(
        Arc::clone(expected_field),
        list.offsets().clone(),
        aligned_values,
        list.nulls().cloned(),
    )
    .map(|aligned| Arc::new(aligned) as ArrayRef)
    .map_err(|err| DataFusionError::ArrowError(Box::new(err), None))
}

fn align_large_list_array(array: &ArrayRef, expected_field: &Arc<Field>) -> Result<ArrayRef> {
    let Some(list) = array.as_list_opt::<i64>() else {
        return type_mismatch(
            array.data_type(),
            &DataType::LargeList(Arc::clone(expected_field)),
        );
    };
    let aligned_values = align_array_to_type(list.values(), expected_field.data_type())?;
    LargeListArray::try_new(
        Arc::clone(expected_field),
        list.offsets().clone(),
        aligned_values,
        list.nulls().cloned(),
    )
    .map(|aligned| Arc::new(aligned) as ArrayRef)
    .map_err(|err| DataFusionError::ArrowError(Box::new(err), None))
}

fn align_fixed_size_list_array(
    array: &ArrayRef,
    expected_field: &Arc<Field>,
    size: i32,
) -> Result<ArrayRef> {
    let Some(list) = array.as_fixed_size_list_opt() else {
        return type_mismatch(
            array.data_type(),
            &DataType::FixedSizeList(Arc::clone(expected_field), size),
        );
    };
    let aligned_values = align_array_to_type(list.values(), expected_field.data_type())?;
    FixedSizeListArray::try_new(
        Arc::clone(expected_field),
        size,
        aligned_values,
        list.nulls().cloned(),
    )
    .map(|aligned| Arc::new(aligned) as ArrayRef)
    .map_err(|err| DataFusionError::ArrowError(Box::new(err), None))
}

fn align_map_array(
    array: &ArrayRef,
    expected_field: &Arc<Field>,
    sorted: bool,
) -> Result<ArrayRef> {
    let Some(map) = array.as_map_opt() else {
        return type_mismatch(
            array.data_type(),
            &DataType::Map(Arc::clone(expected_field), sorted),
        );
    };
    let entries = Arc::new(map.entries().clone()) as ArrayRef;
    let aligned_entries = align_array_to_type(&entries, expected_field.data_type())?;
    let Some(aligned_struct) = aligned_entries.as_struct_opt() else {
        return type_mismatch(aligned_entries.data_type(), expected_field.data_type());
    };
    MapArray::try_new(
        Arc::clone(expected_field),
        map.offsets().clone(),
        aligned_struct.clone(),
        map.nulls().cloned(),
        sorted,
    )
    .map(|aligned| Arc::new(aligned) as ArrayRef)
    .map_err(|err| DataFusionError::ArrowError(Box::new(err), None))
}

/// Opens one ORC object and yields a stream of [`arrow::record_batch::RecordBatch`]es.
pub struct OrcOpener {
    batch_size: usize,
    projected_schema: Arc<Schema>,
    object_store: Arc<dyn ObjectStore>,
    object_versioning_type: Option<ObjectVersionType>,
}

impl FileOpener for OrcOpener {
    fn open(&self, partitioned_file: PartitionedFile) -> Result<FileOpenFuture> {
        let store = Arc::clone(&self.object_store);
        let batch_size = self.batch_size;
        let projected_schema = Arc::clone(&self.projected_schema);
        let file_range = partitioned_file.range.clone();
        let object_versioning_type = self.object_versioning_type.clone();

        Ok(Box::pin(async move {
            let reader = ObjectStoreReader::new(
                store,
                partitioned_file.object_meta.clone(),
                object_versioning_type,
            );
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{VersionRecordingStore, write_orc_bytes, write_two_column_batch};
    use arrow::record_batch::RecordBatch;
    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
    use futures::TryStreamExt;
    use object_store::path::Path;
    use object_store::{GetRange, ObjectStoreExt};

    const VERSION: &str = "the-version-the-scan-started-from";

    #[tokio::test]
    async fn create_file_opener_pins_every_request_to_the_listed_object_version() {
        let batch = write_two_column_batch();
        let store = Arc::new(VersionRecordingStore::new(VERSION));
        let location = Path::from("versioned.orc");
        store
            .put(&location, write_orc_bytes(&batch).into())
            .await
            .expect("stores the file");
        let meta = store.head(&location).await.expect("heads the file");
        store.forget_reads();
        let store_handle = Arc::clone(&store);

        let source = OrcSource::new(TableSchema::new(batch.schema(), vec![]));
        let config = FileScanConfigBuilder::new(
            ObjectStoreUrl::parse("memory://").expect("object store url"),
            Arc::new(source.clone()),
        )
        .with_object_versioning_type(Some(ObjectVersionType::Version))
        .build();

        let opener = source
            .create_file_opener(store as Arc<dyn ObjectStore>, &config, 0)
            .expect("creates the ORC opener");
        let stream = opener
            .open(PartitionedFile::from(meta))
            .expect("opens the file")
            .await
            .expect("builds the record-batch stream");
        let rows: usize = stream
            .try_collect::<Vec<_>>()
            .await
            .expect("reads the stripes")
            .iter()
            .map(RecordBatch::num_rows)
            .sum();
        assert_eq!(
            rows, 3,
            "the opener has to reach the stripes, not just the footer"
        );

        let reads = store_handle.reads();
        assert!(
            !reads.is_empty(),
            "the opener issued no request at all, so this asserts nothing"
        );
        for options in &reads {
            assert_eq!(
                options.version.as_deref(),
                Some(VERSION),
                "OrcOpener did not forward FileScanConfig::object_versioning_type, so a \
                 replacement mid-scan is read as a mixture of both versions: {options:?}"
            );
            assert!(
                !matches!(options.range, Some(GetRange::Suffix(_))),
                "a read fell back to a suffix range, which Azure Blob Storage does not serve: \
                 {options:?}"
            );
        }
    }

    #[test]
    fn align_orc_batch_backfills_missing_nested_struct_fields() {
        use arrow::array::{Int64Array, StructArray};

        let file_schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "payload",
                DataType::Struct(Fields::from(vec![Field::new("id", DataType::Int64, true)])),
                true,
            ),
        ]);
        let payload = StructArray::try_new(
            Fields::from(vec![Field::new("id", DataType::Int64, true)]),
            vec![Arc::new(Int64Array::from(vec![Some(10), Some(20)]))],
            None,
        )
        .expect("file payload");
        let file_batch = RecordBatch::try_new(
            Arc::new(file_schema),
            vec![Arc::new(Int64Array::from(vec![1, 2])), Arc::new(payload)],
        )
        .expect("file batch");

        let target = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "payload",
                DataType::Struct(Fields::from(vec![
                    Field::new("id", DataType::Int64, true),
                    Field::new("extra", DataType::Int64, true),
                ])),
                true,
            ),
        ]));

        let aligned = align_orc_batch(&file_batch, &target).expect("nested align");
        let payload = aligned
            .column_by_name("payload")
            .expect("payload")
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("payload is a struct");
        let extra = payload
            .column_by_name("extra")
            .expect("aligned payload includes extra")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("extra is Int64");
        assert_eq!(extra.len(), 2);
        assert!(extra.is_null(0));
        assert!(extra.is_null(1));
        let id = payload
            .column_by_name("id")
            .expect("id")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id is Int64");
        assert_eq!(id.values(), &[10, 20]);
    }
}
