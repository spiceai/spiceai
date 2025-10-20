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

use arrow::{
    array::{ArrayRef, RecordBatch, StringArray},
    datatypes::{DataType, Field, Schema, SchemaRef},
    error::ArrowError,
};
use arrow_array::{TimestampMicrosecondArray, UInt64Array};
use async_stream::stream;
use async_trait::async_trait;
use bytes::Bytes;
use datafusion::{
    catalog::Session,
    common::{Column, Constraint, Constraints, Statistics, project_schema, stats::Precision},
    datasource::{TableProvider, TableType},
    error::{DataFusionError, Result as DataFusionResult},
    execution::{SendableRecordBatchStream, TaskContext},
    logical_expr::{Expr, TableProviderFilterPushDown},
    physical_expr::EquivalenceProperties,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
        execution_plan::{Boundedness, EmissionType},
        stream::RecordBatchStreamAdapter,
    },
};
use datafusion_datasource::metadata::MetadataColumn;
use document_parse::DocumentParser;
use futures::Stream;
use futures::StreamExt;
use object_store::{GetResult, ObjectMeta, ObjectStore, path::Path};
use snafu::ResultExt;
use std::{any::Any, fmt, sync::Arc};

use crate::object::filter::filter_object_meta;

use super::ObjectStoreContext;
use url::Url;

pub struct ObjectStoreTextTable {
    ctx: ObjectStoreContext,

    /// For document tables, provide an optional formatter
    document_formatter: Option<Arc<dyn DocumentParser>>,

    metadata_columns: Vec<MetadataColumn>,
    constraints: Constraints,
}

impl std::fmt::Debug for ObjectStoreTextTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObjectStoreTextTable")
            .field("ctx", &self.ctx)
            .finish_non_exhaustive()
    }
}

impl ObjectStoreTextTable {
    pub fn try_new(
        store: Arc<dyn ObjectStore>,
        url: &Url,
        extension: Option<String>,
        document_formatter: Option<Arc<dyn DocumentParser>>,
        metadata_columns: Option<Vec<MetadataColumn>>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Ok(Self {
            ctx: ObjectStoreContext::try_new(store, url, extension)?,
            document_formatter,
            metadata_columns: metadata_columns.unwrap_or_default(),

            constraints: Constraints::new_unverified(vec![Constraint::PrimaryKey(vec![0])]),
        })
    }

    #[must_use]
    pub fn base_table_schema() -> Schema {
        // Order is important. [`ObjectStoreTextTable`].constraints expects `location` first.
        Schema::new(vec![
            Field::new("location", DataType::Utf8, false),
            Field::new("content", DataType::Utf8, false),
        ])
    }

    fn get_content_value(
        raw: &Bytes,
        formatter: Option<&Arc<dyn DocumentParser>>,
        location: &Path,
    ) -> Result<ArrayRef, ArrowError> {
        let utf8 = match formatter {
            Some(f) => f
                .parse(raw)
                .and_then(|doc| doc.as_flat_utf8())
                .boxed()
                .map_err(|e| format!("Error parsing document {location}: {e}").into()),
            None => std::str::from_utf8(raw).boxed().map(ToString::to_string),
        }
        .map_err(ArrowError::from_external_error)?;

        Ok(Arc::new(StringArray::from(vec![utf8])))
    }

    async fn list_and_filter_objects(
        &self,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Vec<ObjectMeta>> {
        let mut object_stream = self
            .ctx
            .store
            .list(self.ctx.prefix.clone().map(Path::from).as_ref())
            .chunks(128);

        let mut all_metas = Vec::new();

        while let Some(items) = object_stream.next().await {
            if limit.is_some_and(|l| all_metas.len() >= l) {
                break;
            }

            let metas = items
                .into_iter()
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| DataFusionError::Execution(format!("{e}")))?;

            let filtered = filter_object_meta(filters, &metas)?
                .into_iter()
                .filter(|meta| self.ctx.filename_in_scan(meta))
                .collect::<Vec<_>>();
            all_metas.extend(filtered);
        }

        Ok(all_metas)
    }

    fn to_record_batch(
        meta: &ObjectMeta,
        raw: &Bytes,
        formatter: Option<&Arc<dyn DocumentParser>>,
        schema: SchemaRef,
    ) -> Result<RecordBatch, ArrowError> {
        let columns = schema
            .fields()
            .iter()
            .map(|field| match field.name().as_str() {
                "location" => {
                    Ok(Arc::new(StringArray::from(vec![meta.location.to_string()])) as ArrayRef)
                }
                "content" => Self::get_content_value(raw, formatter, &meta.location),
                "last_modified" => Ok(Arc::new(
                    TimestampMicrosecondArray::from(vec![meta.last_modified.timestamp_micros()])
                        .with_timezone("UTC"),
                ) as ArrayRef),
                "size" => Ok(Arc::new(UInt64Array::from(vec![meta.size])) as ArrayRef),
                _ => Err(ArrowError::SchemaError(format!(
                    "Unsupported field name: {}",
                    field.name()
                ))),
            })
            .collect::<Result<Vec<ArrayRef>, ArrowError>>()?;

        RecordBatch::try_new(schema, columns)
    }
}

#[async_trait]
impl TableProvider for ObjectStoreTextTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn constraints(&self) -> Option<&Constraints> {
        Some(&self.constraints)
    }

    fn schema(&self) -> SchemaRef {
        let mut base_field = Self::base_table_schema()
            .fields()
            .iter()
            .cloned()
            .collect::<Vec<_>>();

        base_field.extend(self.metadata_columns.iter().map(|c| Arc::new(c.field())));

        Arc::new(Schema::new(base_field))
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let mut object_metas = self.list_and_filter_objects(filters, limit).await?;
        if let Some(l) = limit {
            object_metas.truncate(l);
        }

        let projected_schema = project_schema(&self.schema(), projection)?;

        Ok(Arc::new(ObjectStoreTextExec::new(
            projected_schema,
            object_metas,
            self.ctx.clone(),
            self.document_formatter.clone(),
        )))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|f| {
                if f.column_refs()
                    .contains(&Column::from_qualified_name("content"))
                {
                    TableProviderFilterPushDown::Unsupported
                } else {
                    TableProviderFilterPushDown::Inexact
                }
            })
            .collect())
    }
}

pub struct ObjectStoreTextExec {
    projected_schema: SchemaRef,
    object_metas: Arc<Vec<ObjectMeta>>,
    properties: PlanProperties,

    ctx: ObjectStoreContext,
    formatter: Option<Arc<dyn DocumentParser>>,
}

impl std::fmt::Debug for ObjectStoreTextExec {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{} prefix={:?}", self.name(), self.ctx.prefix.clone())
    }
}

impl DisplayAs for ObjectStoreTextExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "{} prefix={}",
            self.name(),
            self.ctx.prefix.clone().unwrap_or_default()
        )
    }
}

impl ExecutionPlan for ObjectStoreTextExec {
    fn name(&self) -> &'static str {
        "ObjectStoreTextExec"
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
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn partition_statistics(
        &self,
        _partition: Option<usize>,
    ) -> Result<Statistics, DataFusionError> {
        let size = usize::try_from(
            self.object_metas
                .iter()
                .map(|obj| obj.size)
                .reduce(|a, b| a + b)
                .unwrap_or_default(),
        );

        // Only one partition.
        Ok(Statistics::new_unknown(&self.schema())
            .with_num_rows(Precision::Exact(self.object_metas.len()))
            .with_total_byte_size(match size {
                Ok(s) => Precision::Exact(s),
                Err(_) => Precision::Absent,
            }))
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            to_sendable_stream(
                self.ctx.clone(),
                self.formatter.clone(),
                Arc::clone(&self.object_metas),
                self.schema(),
            ),
        )))
    }
}

impl ObjectStoreTextExec {
    pub(crate) fn new(
        projected_schema: SchemaRef,
        object_metas: Vec<ObjectMeta>,
        ctx: ObjectStoreContext,
        formatter: Option<Arc<dyn DocumentParser>>,
    ) -> Self {
        Self {
            projected_schema: Arc::clone(&projected_schema),
            object_metas: Arc::new(object_metas),
            properties: PlanProperties::new(
                EquivalenceProperties::new(projected_schema),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            ),
            ctx,
            formatter,
        }
    }
}

pub(crate) fn to_sendable_stream(
    ctx: ObjectStoreContext,
    formatter: Option<Arc<dyn DocumentParser>>,
    object_metas: Arc<Vec<ObjectMeta>>,
    schema: SchemaRef,
) -> impl Stream<Item = DataFusionResult<RecordBatch>> + 'static {
    stream! {
        for object_meta in object_metas.iter() {

            // Avoid object-store GET if not in projection
            let bytz = if schema.column_with_name("content").is_some() {
            let result: GetResult = ctx.store.get(&object_meta.location).await.map_err(|e| DataFusionError::Execution(format!("{e}")))?;
              result.bytes().await.map_err(|e| DataFusionError::Execution(format!("{e}")))?
            } else {
                Bytes::new()
            };

            match ObjectStoreTextTable::to_record_batch(object_meta, &bytz, formatter.as_ref(), Arc::clone(&schema)) {
                Ok(batch) => {
                    yield Ok(batch);
                },
                Err(e) => yield Err(DataFusionError::Execution(format!("{e}"))),
            }
        }
    }
}
