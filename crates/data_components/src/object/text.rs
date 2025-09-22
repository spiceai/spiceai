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
use async_stream::stream;
use async_trait::async_trait;
use bytes::Bytes;
use datafusion::{
    catalog::Session,
    common::{Constraints, project_schema},
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
use document_parse::DocumentParser;
use futures::Stream;
use futures::StreamExt;
use object_store::{GetResult, ObjectMeta, ObjectStore, path::Path};
use snafu::ResultExt;
use std::{any::Any, fmt, sync::Arc};

use super::ObjectStoreContext;
use url::Url;

pub struct ObjectStoreTextTable {
    ctx: ObjectStoreContext,

    /// For document tables, provide an optional formatter
    document_formatter: Option<Arc<dyn DocumentParser>>,
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
        formatter: Option<Arc<dyn DocumentParser>>,
    ) -> Result<Arc<Self>, Box<dyn std::error::Error + Send + Sync>> {
        Ok(Arc::new(Self {
            ctx: ObjectStoreContext::try_new(store, url, extension)?,
            document_formatter: formatter,
        }))
    }

    fn table_schema() -> Schema {
        Schema::new(vec![
            Field::new("location", DataType::Utf8, false),
            Field::new("content", DataType::Utf8, false),
        ])
    }

    fn get_location_value(meta_list: &[ObjectMeta]) -> ArrayRef {
        Arc::new(StringArray::from(
            meta_list
                .iter()
                .map(|meta| meta.location.to_string())
                .collect::<Vec<_>>(),
        ))
    }

    fn get_content_value(
        raw: &[Bytes],
        formatter: Option<&Arc<dyn DocumentParser>>,
        meta_list: &[ObjectMeta],
    ) -> Result<ArrayRef, ArrowError> {
        let utf8_strings: Result<Vec<_>, ArrowError> = raw
            .iter()
            .enumerate()
            .map(|(idx, bytes)| {
                let utf8 = match formatter {
                    Some(f) => f
                        .parse(bytes)
                        .and_then(|doc| doc.as_flat_utf8())
                        .boxed()
                        .map_err(|e| {
                            if let Some(meta) = meta_list.get(idx) {
                                format!("Error parsing document {}: {e}", meta.location).into()
                            } else {
                                e
                            }
                        }),
                    None => std::str::from_utf8(bytes).boxed().map(ToString::to_string),
                };
                utf8.map_err(ArrowError::from_external_error)
            })
            .collect();

        Ok(Arc::new(StringArray::from(
            utf8_strings?.into_iter().collect::<Vec<_>>(),
        )))
    }

    fn get_field_value(
        meta_list: &[ObjectMeta],
        raw: &[Bytes],
        field_name: &str,
        formatter: Option<&Arc<dyn DocumentParser>>,
    ) -> Result<ArrayRef, ArrowError> {
        match field_name {
            "location" => Ok(Self::get_location_value(meta_list)),
            "content" => Self::get_content_value(raw, formatter, meta_list),
            _ => Err(ArrowError::SchemaError(format!(
                "Unsupported field name: {field_name}",
            ))),
        }
    }

    fn to_record_batch(
        meta_list: &[ObjectMeta],
        raw: &[Bytes],
        formatter: Option<&Arc<dyn DocumentParser>>,
        schema: SchemaRef,
    ) -> Result<RecordBatch, ArrowError> {
        if meta_list.len() != raw.len() {
            return Err(ArrowError::ParseError("Length mismatch".to_string()));
        }

        let columns = schema
            .fields()
            .iter()
            .map(|field| {
                let field_name = field.name();
                Self::get_field_value(meta_list, raw, field_name, formatter).map_err(|e| {
                    ArrowError::SchemaError(format!("Error getting field {field_name}: {e}"))
                })
            })
            .collect::<Result<Vec<_>, ArrowError>>()?;

        RecordBatch::try_new(schema, columns)
    }
}

#[async_trait]
impl TableProvider for ObjectStoreTextTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Self::table_schema())
    }

    fn constraints(&self) -> Option<&Constraints> {
        None
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
        let projected_schema = project_schema(&self.schema(), projection)?;
        Ok(Arc::new(ObjectStoreTextExec::new(
            projected_schema,
            filters,
            limit,
            self.ctx.clone(),
            self.document_formatter.clone(),
        )))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        Ok(vec![
            TableProviderFilterPushDown::Unsupported;
            filters.len()
        ])
    }
}

pub struct ObjectStoreTextExec {
    projected_schema: SchemaRef,
    _filters: Vec<Expr>,
    limit: Option<usize>,
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
                self.limit,
                self.schema(),
            ),
        )))
    }
}

impl ObjectStoreTextExec {
    pub(crate) fn new(
        projected_schema: SchemaRef,
        filters: &[Expr],
        limit: Option<usize>,
        ctx: ObjectStoreContext,
        formatter: Option<Arc<dyn DocumentParser>>,
    ) -> Self {
        Self {
            projected_schema: Arc::clone(&projected_schema),
            _filters: filters.to_vec(),
            limit,
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
    limit: Option<usize>,
    schema: SchemaRef,
) -> impl Stream<Item = DataFusionResult<RecordBatch>> + 'static {
    stream! {
        let mut object_stream = ctx.store.list(ctx.prefix.clone().map(Path::from).as_ref());
        let mut count = 0;

        while let Some(item) = object_stream.next().await {
            match item {
                Ok(object_meta) => {

                    if !ctx.filename_in_scan(&object_meta) {
                    continue;
                    }

                    let result: GetResult = ctx.store.get(&object_meta.location).await.map_err(|e| DataFusionError::Execution(format!("{e}")))?;
                    let bytz = result.bytes().await.map_err(|e| DataFusionError::Execution(format!("{e}")))?;

                    match ObjectStoreTextTable::to_record_batch(&[object_meta], &[bytz], formatter.as_ref(), Arc::clone(&schema)) {
                        Ok(batch) => {
                            let n = batch.num_rows();
                            yield Ok(batch);
                            count += n;
                        },
                        Err(e) => yield Err(DataFusionError::Execution(format!("{e}"))),
                    }
                },
                Err(e) => yield Err(DataFusionError::Execution(format!("{e}"))),
            }

            // Early exit on LIMIT clause
            if let Some(limit) = limit && count >= limit {
                    break;
                }
        }
    }
}
