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

//! Per file, a scan reads a JSON key column from the values the file stores for
//! it, or computes it from the file's JSON column when the file stores none.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow::datatypes::{DataType, SchemaRef};
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::common::{Result, ScalarValue};
use datafusion::physical_expr::ScalarFunctionExpr;
use datafusion::physical_expr::expressions::{CastExpr, Column, Literal};
use datafusion::physical_expr_adapter::{
    DefaultPhysicalExprAdapterFactory, PhysicalExprAdapter, PhysicalExprAdapterFactory,
};
use datafusion::physical_plan::PhysicalExpr;

use super::{JsonTopLevelValue, hidden_column_name, is_json_text_type};

/// Adapts a file scan's expressions to each file, as `DataFusion`'s default
/// adapter does, after replacing every `__cayenne_json_top_level_value(<column>,
/// '<key>')` with the column the file stores those values in, when it has one.
///
/// A file stores them under [`hidden_column_name`], a column the table itself does
/// not have; a column of that name that is also the table's is the table's data and
/// is never read in place of the computed values. A file without such a column
/// keeps the computation, which reads the file's JSON column.
#[derive(Debug)]
pub(crate) struct JsonKeyExprAdapterFactory {
    inner: Arc<dyn PhysicalExprAdapterFactory>,
    /// Counts each key column read from a file's stored values.
    stored_reads: Option<Arc<AtomicU64>>,
}

impl JsonKeyExprAdapterFactory {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(DefaultPhysicalExprAdapterFactory),
            stored_reads: None,
        }
    }

    /// The factory, as a file scan configuration takes it.
    #[cfg(test)]
    pub(crate) fn shared() -> Arc<dyn PhysicalExprAdapterFactory> {
        Arc::new(Self::new())
    }

    /// [`Self::shared`], adding one to `stored_reads` for each key column it
    /// reads from a file's stored values.
    pub(crate) fn counting(stored_reads: Arc<AtomicU64>) -> Arc<dyn PhysicalExprAdapterFactory> {
        Arc::new(Self {
            stored_reads: Some(stored_reads),
            ..Self::new()
        })
    }
}

impl PhysicalExprAdapterFactory for JsonKeyExprAdapterFactory {
    fn create(
        &self,
        logical_file_schema: SchemaRef,
        physical_file_schema: SchemaRef,
    ) -> Result<Arc<dyn PhysicalExprAdapter>> {
        Ok(Arc::new(JsonKeyExprAdapter {
            inner: self.inner.create(
                Arc::clone(&logical_file_schema),
                Arc::clone(&physical_file_schema),
            )?,
            logical_file_schema,
            physical_file_schema,
            stored_reads: self.stored_reads.clone(),
        }))
    }
}

#[derive(Debug)]
struct JsonKeyExprAdapter {
    inner: Arc<dyn PhysicalExprAdapter>,
    logical_file_schema: SchemaRef,
    physical_file_schema: SchemaRef,
    stored_reads: Option<Arc<AtomicU64>>,
}

impl PhysicalExprAdapter for JsonKeyExprAdapter {
    fn rewrite(&self, expr: Arc<dyn PhysicalExpr>) -> Result<Arc<dyn PhysicalExpr>> {
        let expr = expr
            .transform_down(|expr| {
                Ok(self
                    .stored_values(&expr)
                    .map_or(Transformed::no(expr), |stored| {
                        Transformed::new(stored, true, TreeNodeRecursion::Jump)
                    }))
            })
            .data()?;
        // The stored column is not in the logical schema; the default adapter
        // resolves such a column against the file's own schema.
        self.inner.rewrite(expr)
    }
}

impl JsonKeyExprAdapter {
    /// The file's stored values for `expr`, when `expr` computes a JSON key column
    /// and the file stores that column.
    fn stored_values(&self, expr: &Arc<dyn PhysicalExpr>) -> Option<Arc<dyn PhysicalExpr>> {
        let call = ScalarFunctionExpr::try_downcast_func::<JsonTopLevelValue>(expr.as_ref())?;
        let [json, key] = call.args() else {
            return None;
        };
        let json = json.downcast_ref::<Column>()?;
        let ScalarValue::Utf8(Some(key)) = key.downcast_ref::<Literal>()?.value() else {
            return None;
        };
        let name = hidden_column_name(json.name(), key);
        if self.logical_file_schema.field_with_name(&name).is_ok() {
            return None;
        }
        let index = self.physical_file_schema.index_of(&name).ok()?;
        let data_type = self.physical_file_schema.field(index).data_type();
        if !is_json_text_type(data_type) {
            return None;
        }
        let stored: Arc<dyn PhysicalExpr> = Arc::new(Column::new(&name, index));
        if let Some(stored_reads) = &self.stored_reads {
            stored_reads.fetch_add(1, Ordering::Relaxed);
        }
        Some(if *data_type == DataType::Utf8View {
            stored
        } else {
            Arc::new(CastExpr::new(stored, DataType::Utf8View, None))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::array::{Array, AsArray, Int64Array, RecordBatch, StringArray};
    use arrow::datatypes::{Field, Schema};
    use datafusion::datasource::file_format::FileFormat;
    use datafusion::datasource::listing::PartitionedFile;
    use datafusion::datasource::physical_plan::FileScanConfigBuilder;
    use datafusion::datasource::table_schema::TableSchema;
    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion::physical_plan::collect;
    use datafusion::prelude::SessionContext;
    use vortex::VortexSessionDefault;
    use vortex_datafusion::{ProjectionPushdown, VortexFormat, VortexTableOptions};

    use crate::json_shred::{JsonKeyColumn, file_projection_with_json_keys};

    const STORED: &str = "__cayenne_json_key:v1:10:attributes:url.path";

    /// Writes `batch` as a Vortex file at `path`, returning its size.
    async fn write_vortex(path: &std::path::Path, batch: &RecordBatch) -> u64 {
        use vortex::array::stream::ArrayStreamAdapter;
        use vortex::arrow::ArrowSessionExt;
        use vortex::buffer::ByteBufferMut;
        use vortex::file::WriteOptionsSessionExt;

        let session = vortex_session::VortexSession::default();
        let schema = batch.schema();
        let arrow = session.arrow();
        let dtype = arrow.from_arrow_schema(&schema).expect("schema converts");
        let chunk = arrow.from_arrow_record_batch(batch.clone(), &schema);
        drop(arrow);
        let stream = ArrayStreamAdapter::new(dtype, futures::stream::iter([chunk]));
        let mut file = ByteBufferMut::empty();
        session
            .write_options()
            .write(&mut file, stream)
            .await
            .expect("write the file");
        let file = file.freeze();
        let bytes: &[u8] = file.as_ref();
        tokio::fs::write(path, bytes).await.expect("store the file");
        u64::try_from(bytes.len()).expect("size fits")
    }

    fn documents() -> StringArray {
        StringArray::from(vec![
            Some(r#"{"url.path": "/a"}"#),
            Some(r#"{"url.path": "/a/longer/than/twelve/bytes"}"#),
            None,
            Some("[1]"),
        ])
    }

    /// `id` and the key column of a scan of both files under `table_schema`.
    async fn scan(
        dir: &std::path::Path,
        files: &[(&str, u64)],
        table_schema: &Arc<Schema>,
        pushdown: ProjectionPushdown,
    ) -> Vec<(i64, Option<String>)> {
        let ctx = SessionContext::new();
        let options = VortexTableOptions {
            projection_pushdown: pushdown,
            ..VortexTableOptions::default()
        };
        let format =
            VortexFormat::new_with_options(vortex_session::VortexSession::default(), options);
        let key = JsonKeyColumn::new(1, "attributes", "url.path").expect("a key");
        let projection =
            file_projection_with_json_keys(table_schema, &[0], &[key], &[]).expect("projection");
        let source = format
            .file_source(TableSchema::from(Arc::clone(table_schema)))
            .try_pushdown_projection(&projection)
            .expect("push the projection")
            .expect("the source takes expressions");
        let mut builder = FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), source)
            .with_expr_adapter(Some(JsonKeyExprAdapterFactory::shared()));
        for (name, size) in files {
            let path = object_store::path::Path::from_filesystem_path(dir.join(name))
                .expect("a filesystem path");
            builder = builder.with_file(PartitionedFile::new(path.to_string(), *size));
        }
        let plan = format
            .create_physical_plan(&ctx.state(), builder.build())
            .await
            .expect("plan the scan");
        let batches = collect(plan, ctx.task_ctx()).await.expect("run the scan");
        let mut rows = Vec::new();
        for batch in &batches {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("ids");
            let values = batch.column(1).as_string_view();
            for row in 0..batch.num_rows() {
                rows.push((
                    ids.value(row),
                    (!values.is_null(row)).then(|| values.value(row).to_string()),
                ));
            }
        }
        rows.sort();
        rows
    }

    /// A file that stores a key's values is read for them; a file without them, a
    /// file that stores them under another version's rules, and any file when the
    /// table itself has a column of that name, is computed from.
    /// The stored values here differ from the computed ones on purpose, so the
    /// test can tell which a row came from.
    #[tokio::test]
    async fn a_file_s_stored_key_values_are_read_in_place_of_the_computed_ones() {
        let dir = tempfile::tempdir().expect("temp dir");
        let stored_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("attributes", DataType::Utf8, true),
            Field::new(STORED, DataType::Utf8, true),
        ]));
        let with_stored = RecordBatch::try_new(
            Arc::clone(&stored_schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
                Arc::new(documents()),
                Arc::new(StringArray::from(vec![
                    Some("\"stored-1\""),
                    Some("\"stored-2-longer-than-twelve\""),
                    None,
                    Some("\"stored-4\""),
                ])),
            ],
        )
        .expect("batch");
        let plain = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("attributes", DataType::Utf8, true),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![11, 12, 13, 14])),
                Arc::new(documents()),
            ],
        )
        .expect("batch");
        // Values stored under other extraction rules, named as those rules named
        // them: the unversioned name and an older version's.
        let old_rules = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("attributes", DataType::Utf8, true),
                Field::new("__cayenne_json_key:10:attributes:url.path", DataType::Utf8, true),
                Field::new("__cayenne_json_key:v0:10:attributes:url.path", DataType::Utf8, true),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![21, 22, 23, 24])),
                Arc::new(documents()),
                Arc::new(StringArray::from(vec![Some("\"old\""); 4])),
                Arc::new(StringArray::from(vec![Some("\"v0\""); 4])),
            ],
        )
        .expect("batch");
        let stored_size = write_vortex(&dir.path().join("stored.vortex"), &with_stored).await;
        let plain_size = write_vortex(&dir.path().join("plain.vortex"), &plain).await;
        let old_size = write_vortex(&dir.path().join("old_rules.vortex"), &old_rules).await;
        let files = [
            ("stored.vortex", stored_size),
            ("plain.vortex", plain_size),
            ("old_rules.vortex", old_size),
        ];

        let table = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("attributes", DataType::Utf8View, true),
        ]));
        let expected = vec![
            (1, Some("\"stored-1\"".to_string())),
            (2, Some("\"stored-2-longer-than-twelve\"".to_string())),
            (3, None),
            (4, Some("\"stored-4\"".to_string())),
            (11, Some("\"/a\"".to_string())),
            (12, Some("\"/a/longer/than/twelve/bytes\"".to_string())),
            (13, None),
            (14, None),
            // Stored under other rules: computed again, never read.
            (21, Some("\"/a\"".to_string())),
            (22, Some("\"/a/longer/than/twelve/bytes\"".to_string())),
            (23, None),
            (24, None),
        ];
        for pushdown in [ProjectionPushdown::Off, ProjectionPushdown::On] {
            assert_eq!(
                scan(dir.path(), &files, &table, pushdown).await,
                expected,
                "{pushdown:?}: a file's stored values answer for its rows"
            );
        }

        // A table column that happens to have the stored column's name is the
        // table's data, never the key's values.
        let named_like_stored = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("attributes", DataType::Utf8View, true),
            Field::new(STORED, DataType::Utf8View, true),
        ]));
        let computed = vec![
            (1, Some("\"/a\"".to_string())),
            (2, Some("\"/a/longer/than/twelve/bytes\"".to_string())),
            (3, None),
            (4, None),
            (11, Some("\"/a\"".to_string())),
            (12, Some("\"/a/longer/than/twelve/bytes\"".to_string())),
            (13, None),
            (14, None),
            (21, Some("\"/a\"".to_string())),
            (22, Some("\"/a/longer/than/twelve/bytes\"".to_string())),
            (23, None),
            (24, None),
        ];
        assert_eq!(
            scan(
                dir.path(),
                &files,
                &named_like_stored,
                ProjectionPushdown::Off
            )
            .await,
            computed
        );
    }
}
