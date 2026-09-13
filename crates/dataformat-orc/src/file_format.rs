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

//! [`OrcFormat`]: Apache ORC [`FileFormat`] for listing tables.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::{Session, memory::DataSourceExec};
use datafusion::common::{Statistics, not_impl_err, stats::Precision};
use datafusion::datasource::file_format::{FileFormat, file_compression_type::FileCompressionType};
use datafusion::datasource::physical_plan::{FileScanConfig, FileSinkConfig, FileSource};
use datafusion::error::{DataFusionError, Result};
use datafusion::parquet::arrow::async_reader::ObjectVersionType;
use datafusion::physical_expr::LexRequirement;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_datasource::TableSchema;
use object_store::{ObjectMeta, ObjectStore};
use orc_rust::reader::metadata::{FileMetadata, read_metadata_async};

use crate::object_store_reader::ObjectStoreReader;
use crate::source::OrcSource;

const ORC_EXTENSION: &str = "orc";

/// Convert an ORC footer into an Arrow schema, including any user metadata.
pub(crate) fn arrow_schema_from_orc_metadata(metadata: &FileMetadata) -> Schema {
    let user_metadata = metadata
        .user_custom_metadata()
        .iter()
        .map(|(key, value)| (key.clone(), String::from_utf8_lossy(value).to_string()))
        .collect::<HashMap<_, _>>();
    metadata
        .root_data_type()
        .create_arrow_schema(&user_metadata)
}

pub(crate) fn orc_to_datafusion_error(err: orc_rust::error::OrcError) -> DataFusionError {
    DataFusionError::External(Box::new(err))
}

/// Schema and stats footer reads use a `Version` pin so a listed generation
/// (or its ETag on an unversioned bucket) cannot be mixed with a replacement.
const SCHEMA_AND_STATS_VERSIONING: Option<ObjectVersionType> = Some(ObjectVersionType::Version);

async fn fetch_schema(
    store: &Arc<dyn ObjectStore>,
    file: &ObjectMeta,
) -> Result<(object_store::path::Path, Schema)> {
    let mut reader =
        ObjectStoreReader::new(Arc::clone(store), file.clone(), SCHEMA_AND_STATS_VERSIONING);
    let metadata = read_metadata_async(&mut reader)
        .await
        .map_err(orc_to_datafusion_error)?;
    Ok((
        file.location.clone(),
        arrow_schema_from_orc_metadata(&metadata),
    ))
}

async fn fetch_row_count(store: &Arc<dyn ObjectStore>, file: &ObjectMeta) -> Result<u64> {
    let mut reader =
        ObjectStoreReader::new(Arc::clone(store), file.clone(), SCHEMA_AND_STATS_VERSIONING);
    let metadata = read_metadata_async(&mut reader)
        .await
        .map_err(orc_to_datafusion_error)?;
    Ok(metadata.number_of_rows())
}

/// Merge per-file ORC schemas for a listing table.
///
/// A field that appears in only some files — including a nested struct
/// child — is filled with typed NULLs at scan time, so it must be nullable
/// in the merged schema even when every file that has it declares it
/// required. [`Schema::try_merge`] recursively unions struct children but
/// keeps the first-seen nullability for a child that is not in every file.
fn merge_orc_file_schemas(schemas: Vec<Schema>) -> Result<Schema> {
    let per_file_fields: Vec<arrow::datatypes::Fields> = schemas
        .iter()
        .map(|schema| schema.fields().clone())
        .collect();
    let merged = Schema::try_merge(schemas)?;
    let fields = merged
        .fields()
        .iter()
        .map(|field| {
            let sources: Vec<Option<&Field>> = per_file_fields
                .iter()
                .map(|fields| field_named(fields, field.name()))
                .collect();
            Arc::new(mark_partial_fields_nullable(field, &sources))
        })
        .collect::<Vec<_>>();

    Ok(Schema::new_with_metadata(fields, merged.metadata().clone()))
}

fn field_named<'a>(fields: &'a arrow::datatypes::Fields, name: &str) -> Option<&'a Field> {
    fields
        .iter()
        .find(|field| field.name() == name)
        .map(Arc::as_ref)
}

fn nested_field_sources<'a>(
    sources: &[Option<&'a Field>],
    child: impl Fn(&'a Field) -> Option<&'a Field>,
) -> Vec<Option<&'a Field>> {
    sources
        .iter()
        .map(|source| source.and_then(|field| child(field)))
        .collect()
}

/// Mark a merged field nullable when any source file lacks it, and do the
/// same for nested children so scan-time NULL backfill is a valid batch.
fn mark_partial_fields_nullable(merged: &Field, sources: &[Option<&Field>]) -> Field {
    let missing_from_some = sources.iter().any(Option::is_none);
    let nullable = merged.is_nullable() || missing_from_some;
    Field::new(
        merged.name(),
        mark_partial_data_type_nullable(merged.data_type(), sources),
        nullable,
    )
    .with_metadata(merged.metadata().clone())
}

fn mark_partial_data_type_nullable(merged: &DataType, sources: &[Option<&Field>]) -> DataType {
    match merged {
        DataType::Struct(children) => {
            let children = children
                .iter()
                .map(|child| {
                    let child_sources =
                        nested_field_sources(sources, |field| match field.data_type() {
                            DataType::Struct(fields) => field_named(fields, child.name()),
                            _ => None,
                        });
                    Arc::new(mark_partial_fields_nullable(child, &child_sources))
                })
                .collect();
            DataType::Struct(children)
        }
        DataType::List(item) => DataType::List(Arc::new(mark_partial_fields_nullable(
            item,
            &nested_field_sources(sources, |field| match field.data_type() {
                DataType::List(item) => Some(item.as_ref()),
                _ => None,
            }),
        ))),
        DataType::LargeList(item) => DataType::LargeList(Arc::new(mark_partial_fields_nullable(
            item,
            &nested_field_sources(sources, |field| match field.data_type() {
                DataType::LargeList(item) => Some(item.as_ref()),
                _ => None,
            }),
        ))),
        DataType::FixedSizeList(item, size) => DataType::FixedSizeList(
            Arc::new(mark_partial_fields_nullable(
                item,
                &nested_field_sources(sources, |field| match field.data_type() {
                    DataType::FixedSizeList(item, _) => Some(item.as_ref()),
                    _ => None,
                }),
            )),
            *size,
        ),
        DataType::Map(entries, sorted) => DataType::Map(
            Arc::new(mark_partial_fields_nullable(
                entries,
                &nested_field_sources(sources, |field| match field.data_type() {
                    DataType::Map(entries, _) => Some(entries.as_ref()),
                    _ => None,
                }),
            )),
            *sorted,
        ),
        other => other.clone(),
    }
}

/// Apache ORC [`FileFormat`] implementation backed by `orc-rust`.
///
/// Read-only: listing connectors scan `.orc` objects. Writing is not implemented.
#[derive(Debug, Default, Clone)]
pub struct OrcFormat;

impl OrcFormat {
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl FileFormat for OrcFormat {
    fn get_ext(&self) -> String {
        ORC_EXTENSION.to_string()
    }

    fn get_ext_with_compression(
        &self,
        _file_compression_type: &FileCompressionType,
    ) -> Result<String> {
        // ORC carries its own stripe compression; the listing path does not
        // wrap `.orc` in a second codec the way it does for CSV/JSON.
        Ok(ORC_EXTENSION.to_string())
    }

    fn compression_type(&self) -> Option<FileCompressionType> {
        None
    }

    async fn infer_schema(
        &self,
        _state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> Result<SchemaRef> {
        let mut schemas = Vec::with_capacity(objects.len());
        // Sort by location so schema field order is deterministic even when
        // the object store lists files in an arbitrary order.
        let mut objects: Vec<&ObjectMeta> = objects.iter().collect();
        objects.sort_by(|a, b| a.location.cmp(&b.location));
        for object in objects {
            let (_path, schema) = fetch_schema(store, object).await?;
            schemas.push(schema);
        }

        let schema = merge_orc_file_schemas(schemas)?;
        Ok(Arc::new(schema))
    }

    async fn infer_stats(
        &self,
        _state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        object: &ObjectMeta,
    ) -> Result<Statistics> {
        let mut statistics = Statistics::new_unknown(&table_schema);
        let rows = fetch_row_count(store, object).await?;
        // Footer `number_of_rows` is exact. The runtime is 64-bit, so this
        // conversion is identity; fail closed rather than truncate on a
        // hypothetical narrower `usize`.
        let rows = usize::try_from(rows).map_err(|_| {
            DataFusionError::Execution(format!(
                "ORC file '{}' row count {rows} does not fit in usize",
                object.location
            ))
        })?;
        statistics.num_rows = Precision::Exact(rows);
        Ok(statistics)
    }

    async fn create_physical_plan(
        &self,
        _state: &dyn Session,
        conf: FileScanConfig,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(DataSourceExec::from_data_source(conf))
    }

    async fn create_writer_physical_plan(
        &self,
        _input: Arc<dyn ExecutionPlan>,
        _state: &dyn Session,
        _conf: FileSinkConfig,
        _order_requirements: Option<LexRequirement>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        not_impl_err!("Inserts are not implemented yet for ORC")
    }

    fn file_source(&self, table_schema: TableSchema) -> Arc<dyn FileSource> {
        Arc::new(OrcSource::new(table_schema))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{put_orc, write_orc_bytes};
    use arrow::array::{Array, Int32Array, StringArray};
    use arrow::datatypes::DataType;
    use arrow::record_batch::RecordBatch;
    use datafusion::execution::context::SessionContext;
    use datafusion::prelude::SessionConfig;
    use object_store::ObjectStore;
    use object_store::memory::InMemory;

    fn sample_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            arrow::datatypes::Field::new("id", DataType::Int32, true),
            arrow::datatypes::Field::new("name", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![Some(1), Some(2), None])),
                Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])),
            ],
        )
        .expect("sample batch")
    }

    #[tokio::test]
    async fn infer_schema_reads_orc_footer() {
        let memory = Arc::new(InMemory::new());
        let bytes = write_orc_bytes(&sample_batch());
        let meta = put_orc(memory.as_ref(), "data/sample.orc", bytes).await;
        let store: Arc<dyn ObjectStore> = memory;

        let ctx = SessionContext::new();
        let format = OrcFormat::new();
        let schema = format
            .infer_schema(&ctx.state(), &store, &[meta])
            .await
            .expect("infer schema");

        assert_eq!(schema.fields().len(), 2);
        schema.field_with_name("id").expect("id field should exist");
        schema
            .field_with_name("name")
            .expect("name field should exist");
    }

    #[tokio::test]
    async fn infer_schema_merges_files_in_location_order() {
        let memory = Arc::new(InMemory::new());

        let first_schema = Arc::new(Schema::new(vec![arrow::datatypes::Field::new(
            "id",
            DataType::Int32,
            true,
        )]));
        let first = RecordBatch::try_new(
            Arc::clone(&first_schema),
            vec![Arc::new(Int32Array::from(vec![1]))],
        )
        .expect("first batch");

        let second_schema = Arc::new(Schema::new(vec![
            arrow::datatypes::Field::new("id", DataType::Int32, true),
            arrow::datatypes::Field::new("extra", DataType::Utf8, true),
        ]));
        let second = RecordBatch::try_new(
            Arc::clone(&second_schema),
            vec![
                Arc::new(Int32Array::from(vec![2])),
                Arc::new(StringArray::from(vec!["x"])),
            ],
        )
        .expect("second batch");

        // Insert in reverse path order so a location sort is load-bearing.
        let z = put_orc(memory.as_ref(), "data/z.orc", write_orc_bytes(&second)).await;
        let a = put_orc(memory.as_ref(), "data/a.orc", write_orc_bytes(&first)).await;
        let store: Arc<dyn ObjectStore> = memory;

        let ctx = SessionContext::new();
        let schema = OrcFormat::new()
            .infer_schema(&ctx.state(), &store, &[z, a])
            .await
            .expect("merged schema");

        assert_eq!(schema.fields().len(), 2);
        schema.field_with_name("id").expect("id field should exist");
        let extra = schema
            .field_with_name("extra")
            .expect("extra field should exist");
        assert!(
            extra.is_nullable(),
            "a column present in only some files must be nullable so scan can backfill NULLs"
        );
    }

    #[test]
    fn merge_orc_file_schemas_marks_partial_fields_nullable() {
        let only_id = Schema::new(vec![arrow::datatypes::Field::new(
            "id",
            DataType::Int32,
            false,
        )]);
        let id_and_extra = Schema::new(vec![
            arrow::datatypes::Field::new("id", DataType::Int32, false),
            arrow::datatypes::Field::new("extra", DataType::Utf8, false),
        ]);

        let merged = merge_orc_file_schemas(vec![only_id, id_and_extra]).expect("merge schemas");

        assert!(
            !merged
                .field_with_name("id")
                .expect("id field")
                .is_nullable(),
            "id is in every file as required and must stay required"
        );
        assert!(
            merged
                .field_with_name("extra")
                .expect("extra field")
                .is_nullable(),
            "extra is missing from some files and must be nullable for NULL backfill"
        );
    }

    #[test]
    fn merge_orc_file_schemas_marks_nested_struct_children_nullable() {
        use arrow::datatypes::Fields;

        let first = Schema::new(vec![Field::new(
            "payload",
            DataType::Struct(Fields::from(vec![Field::new("id", DataType::Int64, true)])),
            true,
        )]);
        let second = Schema::new(vec![Field::new(
            "payload",
            DataType::Struct(Fields::from(vec![
                Field::new("id", DataType::Int64, true),
                Field::new("extra", DataType::Int64, false),
            ])),
            true,
        )]);

        let merged = merge_orc_file_schemas(vec![first, second]).expect("nested merge");
        let payload = merged.field_with_name("payload").expect("payload");
        let DataType::Struct(children) = payload.data_type() else {
            panic!("payload should stay a struct");
        };
        let extra = children
            .iter()
            .find(|field| field.name() == "extra")
            .expect("Arrow Schema::try_merge keeps nested extra");
        assert!(
            extra.is_nullable(),
            "a nested field present in only some files must be nullable so the scan can NULL-fill it"
        );
    }

    #[tokio::test]
    async fn infer_schema_rejects_invalid_bytes() {
        let memory = Arc::new(InMemory::new());
        let meta = put_orc(memory.as_ref(), "data/not.orc", b"this is not orc".to_vec()).await;
        let store: Arc<dyn ObjectStore> = memory;
        let ctx = SessionContext::new();
        OrcFormat::new()
            .infer_schema(&ctx.state(), &store, &[meta])
            .await
            .expect_err("invalid ORC must fail schema inference");
    }

    #[tokio::test]
    async fn infer_stats_reports_exact_row_count() {
        let memory = Arc::new(InMemory::new());
        let batch = sample_batch();
        let meta = put_orc(memory.as_ref(), "data/sample.orc", write_orc_bytes(&batch)).await;
        let store: Arc<dyn ObjectStore> = memory;
        let ctx = SessionContext::new();
        let format = OrcFormat::new();
        let schema = format
            .infer_schema(&ctx.state(), &store, std::slice::from_ref(&meta))
            .await
            .expect("schema");
        let stats = format
            .infer_stats(&ctx.state(), &store, schema, &meta)
            .await
            .expect("stats");
        assert_eq!(stats.num_rows, Precision::Exact(3));
    }

    #[tokio::test]
    async fn listing_scan_returns_written_rows() {
        use datafusion::datasource::listing::{
            ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
        };

        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("sample.orc");
        std::fs::write(&path, write_orc_bytes(&sample_batch())).expect("write fixture");

        let ctx = SessionContext::new_with_config(SessionConfig::new());
        let table_url =
            ListingTableUrl::parse(format!("file://{}", path.display())).expect("listing url");
        let config = ListingTableConfig::new(table_url)
            .with_listing_options(
                ListingOptions::new(Arc::new(OrcFormat::new())).with_file_extension(".orc"),
            )
            .infer_schema(&ctx.state())
            .await
            .expect("infer listing schema");
        let table = ListingTable::try_new(config).expect("listing table");
        ctx.register_table("sample", Arc::new(table))
            .expect("register");

        let df = ctx
            .sql("SELECT id, name FROM sample ORDER BY id NULLS LAST")
            .await
            .expect("sql");
        let batches = df.collect().await.expect("collect");
        let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(rows, 3, "scan must return every written row");

        let projected = ctx
            .sql("SELECT name FROM sample WHERE id = 1")
            .await
            .expect("projected sql");
        let projected_batches = projected.collect().await.expect("collect projected");
        assert_eq!(projected_batches[0].num_rows(), 1);
        let names = projected_batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("name column");
        assert_eq!(names.value(0), "a");

        // `ProjectionMask::named_roots` emits file order; the SELECT list is
        // the opposite of the written schema (id, name).
        let reordered = ctx
            .sql("SELECT name, id FROM sample WHERE id = 2")
            .await
            .expect("reordered sql");
        let reordered_batches = reordered.collect().await.expect("collect reordered");
        assert_eq!(reordered_batches[0].num_rows(), 1);
        assert_eq!(reordered_batches[0].schema().field(0).name(), "name");
        assert_eq!(reordered_batches[0].schema().field(1).name(), "id");
        let reordered_names = reordered_batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("reordered name column");
        assert!(
            reordered_names.is_null(0),
            "id=2 was written with a NULL name"
        );
        let reordered_ids = reordered_batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("reordered id column");
        assert_eq!(reordered_ids.value(0), 2);

        let count = ctx
            .sql("SELECT COUNT(*) AS n FROM sample")
            .await
            .expect("count sql");
        let count_batches = count.collect().await.expect("collect count");
        let counts = count_batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .expect("count column");
        assert_eq!(counts.value(0), 3);
    }

    /// Regression: `orc-rust` 0.8.0 `ProjectionMask::named_roots` omits names
    /// absent from the current file. A listing of files with different
    /// columns must still scan the merged projection, backfilling NULLs.
    #[tokio::test]
    async fn listing_scan_backfills_columns_missing_from_some_files() {
        use datafusion::datasource::listing::{
            ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
        };

        let dir = tempfile::tempdir().expect("tempdir");

        let id_only_schema = Arc::new(Schema::new(vec![arrow::datatypes::Field::new(
            "id",
            DataType::Int32,
            false,
        )]));
        let id_only = RecordBatch::try_new(
            Arc::clone(&id_only_schema),
            vec![Arc::new(Int32Array::from(vec![1]))],
        )
        .expect("id-only batch");

        let both_schema = Arc::new(Schema::new(vec![
            arrow::datatypes::Field::new("id", DataType::Int32, false),
            arrow::datatypes::Field::new("extra", DataType::Utf8, false),
        ]));
        let both = RecordBatch::try_new(
            Arc::clone(&both_schema),
            vec![
                Arc::new(Int32Array::from(vec![2])),
                Arc::new(StringArray::from(vec!["x"])),
            ],
        )
        .expect("id+extra batch");

        std::fs::write(dir.path().join("a.orc"), write_orc_bytes(&id_only)).expect("write a.orc");
        std::fs::write(dir.path().join("b.orc"), write_orc_bytes(&both)).expect("write b.orc");

        let ctx = SessionContext::new_with_config(SessionConfig::new());
        let table_url = ListingTableUrl::parse(format!("file://{}/", dir.path().display()))
            .expect("listing url");
        let config = ListingTableConfig::new(table_url)
            .with_listing_options(
                ListingOptions::new(Arc::new(OrcFormat::new())).with_file_extension(".orc"),
            )
            .infer_schema(&ctx.state())
            .await
            .expect("infer listing schema");
        let table = ListingTable::try_new(config).expect("listing table");
        ctx.register_table("merged", Arc::new(table))
            .expect("register");

        let df = ctx
            .sql("SELECT id, extra FROM merged ORDER BY id")
            .await
            .expect("sql");
        let batches = df.collect().await.expect("collect");
        let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(rows, 2, "scan must return a row from every file");

        let mut pairs = Vec::new();
        for batch in &batches {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("id column");
            let extras = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("extra column");
            for i in 0..batch.num_rows() {
                let extra = if extras.is_null(i) {
                    None
                } else {
                    Some(extras.value(i).to_string())
                };
                pairs.push((ids.value(i), extra));
            }
        }
        assert_eq!(
            pairs,
            vec![(1, None), (2, Some("x".to_string()))],
            "a.orc has no extra column; the merged projection must backfill NULL"
        );

        let extra_only = ctx
            .sql("SELECT extra FROM merged ORDER BY extra NULLS FIRST")
            .await
            .expect("extra-only sql");
        let extra_batches = extra_only.collect().await.expect("collect extra-only");
        let extra_rows: usize = extra_batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(
            extra_rows, 2,
            "projecting only extra must still visit both files"
        );

        let mut extras = Vec::new();
        for batch in &extra_batches {
            let col = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("extra-only column");
            for i in 0..batch.num_rows() {
                extras.push(if col.is_null(i) {
                    None
                } else {
                    Some(col.value(i).to_string())
                });
            }
        }
        assert_eq!(
            extras,
            vec![None, Some("x".to_string())],
            "SELECT extra must backfill NULL for the file that has no extra column"
        );
    }

    /// Arrow 58 `Schema::try_merge` unions struct children recursively. A file
    /// that only has `payload.id` must still scan under a merged type that also
    /// has `payload.extra`, with a typed NULL for the missing child.
    ///
    /// Fixtures are written by PyArrow, not `orc-rust` — the encoder used by
    /// the other listing tests cannot emit structs.
    #[tokio::test]
    async fn listing_scan_backfills_nested_struct_fields_missing_from_one_file() {
        use arrow::array::Int64Array;
        use datafusion::datasource::listing::{
            ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
        };

        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::write(
            dir.path().join("a.orc"),
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/fixtures/orc/payload_id.orc"
            )),
        )
        .expect("write a.orc");
        std::fs::write(
            dir.path().join("b.orc"),
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/fixtures/orc/payload_id_extra.orc"
            )),
        )
        .expect("write b.orc");

        let ctx = SessionContext::new_with_config(SessionConfig::new());
        let listing = ListingOptions::new(Arc::new(OrcFormat::new())).with_file_extension(".orc");
        let table_url = ListingTableUrl::parse(format!("file://{}/", dir.path().display()))
            .expect("listing url");
        let schema = listing
            .infer_schema(&ctx.state(), &table_url)
            .await
            .expect("infer listing schema");
        let extra = match schema
            .field_with_name("payload")
            .expect("payload")
            .data_type()
        {
            DataType::Struct(children) => children
                .iter()
                .find(|field| field.name() == "extra")
                .expect("merged schema includes extra")
                .clone(),
            other => panic!("payload should be a struct, got {other}"),
        };
        assert!(
            extra.is_nullable(),
            "payload.extra is missing from a.orc and must be nullable"
        );

        let config = ListingTableConfig::new(table_url)
            .with_listing_options(listing)
            .with_schema(schema);
        let table = ListingTable::try_new(config).expect("listing table");
        ctx.register_table("merged", Arc::new(table))
            .expect("register");

        let df = ctx
            .sql("SELECT id, payload.extra AS extra FROM merged ORDER BY id")
            .await
            .expect("nested projection");
        let batches = df.collect().await.expect("scan evolving nested structs");
        let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(rows, 2, "scan must return a row from every file");

        let mut pairs = Vec::new();
        for batch in &batches {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("id column");
            let extras = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("extra column");
            for i in 0..batch.num_rows() {
                let extra = if extras.is_null(i) {
                    None
                } else {
                    Some(extras.value(i))
                };
                pairs.push((ids.value(i), extra));
            }
        }
        assert_eq!(
            pairs,
            vec![(1, None), (2, Some(99))],
            "a.orc only has payload.id; the merged projection must NULL-fill payload.extra"
        );
    }

    #[tokio::test]
    async fn infer_schema_pins_every_request_to_the_listed_object_version() {
        use crate::test_support::{VersionRecordingStore, write_orc_bytes, write_two_column_batch};
        use object_store::path::Path;
        use object_store::{GetRange, ObjectStoreExt};

        const VERSION: &str = "the-version-the-scan-started-from";

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

        let ctx = SessionContext::new();
        let schema = OrcFormat::new()
            .infer_schema(&ctx.state(), &(store as Arc<dyn ObjectStore>), &[meta])
            .await
            .expect("infer schema");
        assert!(
            schema.field_with_name("id").is_ok(),
            "schema infer must read the footer, not just pin"
        );

        let reads = store_handle.reads();
        assert!(
            !reads.is_empty(),
            "schema infer issued no request at all, so this asserts nothing"
        );
        for options in &reads {
            assert_eq!(
                options.version.as_deref(),
                Some(VERSION),
                "schema infer did not pin the object version: {options:?}"
            );
            assert!(
                !matches!(options.range, Some(GetRange::Suffix(_))),
                "a read fell back to a suffix range, which Azure Blob Storage does not serve: \
                 {options:?}"
            );
        }
    }

    #[tokio::test]
    async fn infer_stats_reports_zero_rows_for_empty_file() {
        let memory = Arc::new(InMemory::new());
        let schema = Arc::new(Schema::new(vec![arrow::datatypes::Field::new(
            "id",
            DataType::Int32,
            true,
        )]));
        let empty = RecordBatch::new_empty(Arc::clone(&schema));
        let meta = put_orc(memory.as_ref(), "data/empty.orc", write_orc_bytes(&empty)).await;
        let store: Arc<dyn ObjectStore> = memory;
        let ctx = SessionContext::new();
        let format = OrcFormat::new();
        let inferred = format
            .infer_schema(&ctx.state(), &store, std::slice::from_ref(&meta))
            .await
            .expect("schema");
        let stats = format
            .infer_stats(&ctx.state(), &store, inferred, &meta)
            .await
            .expect("stats");
        assert_eq!(stats.num_rows, Precision::Exact(0));
    }
}
