/*
Copyright 2024 The Spice.ai OSS Authors

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
#![allow(clippy::module_name_repetitions)]

use arrow::{
    array::{ArrayRef, LargeBinaryArray, RecordBatch, StringArray},
    datatypes::{DataType, Field, Schema, SchemaRef},
    error::ArrowError,
};
use async_stream::stream;
use async_trait::async_trait;
use bytes::Bytes;
use datafusion::{
    common::{project_schema, Constraints},
    datasource::{TableProvider, TableType},
    error::{DataFusionError, Result as DataFusionResult},
    execution::{context::SessionState, SendableRecordBatchStream, TaskContext},
    logical_expr::{Expr, TableProviderFilterPushDown},
    physical_expr::EquivalenceProperties,
    physical_plan::{
        stream::RecordBatchStreamAdapter, DisplayAs, DisplayFormatType, ExecutionMode,
        ExecutionPlan, Partitioning, PlanProperties,
    },
};
use futures::Stream;
use futures::StreamExt;
use object_store::{path::Path, GetResult, ObjectMeta, ObjectStore};
use std::{any::Any, fmt, str::Utf8Error, sync::Arc};

use super::ObjectStoreContext;
use url::Url;

pub struct ObjectStoreRawTable {
    ctx: ObjectStoreContext,
}

impl ObjectStoreRawTable {
    pub fn try_new(
        store: Arc<dyn ObjectStore>,
        url: &Url,
        extension: Option<String>,
    ) -> Result<Arc<Self>, Box<dyn std::error::Error + Send + Sync>> {
        Ok(Arc::new(Self {
            ctx: ObjectStoreContext::try_new(store, url, extension)?,
        }))
    }

    fn table_schema() -> Schema {
        Schema::new(vec![
            Field::new("location", DataType::Utf8, false),
            Field::new("content", DataType::LargeBinary, false),
        ])
    }

    fn to_record_batch(meta_list: &[ObjectMeta], raw: &[Bytes]) -> Result<RecordBatch, ArrowError> {
        if meta_list.len() != raw.len() {
            return Err(ArrowError::ParseError("Length mismatch".to_string()));
        }

        let schema = Self::table_schema();

        let location_array = StringArray::from(
            meta_list
                .iter()
                .map(|meta| meta.location.to_string())
                .collect::<Vec<_>>(),
        );

        let content_array = LargeBinaryArray::from_vec(raw.iter().map(|b| b.as_ref()).collect());

        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(location_array) as ArrayRef,
                Arc::new(content_array) as ArrayRef,
            ],
        )?;

        Ok(batch)
    }
}

#[async_trait]
impl TableProvider for ObjectStoreRawTable {
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
        _state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let projected_schema = project_schema(&self.schema(), projection)?;
        Ok(Arc::new(ObjectStoreRawExec::new(
            projected_schema,
            filters,
            limit,
            self.ctx.clone(),
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

pub struct ObjectStoreRawExec {
    projected_schema: SchemaRef,
    _filters: Vec<Expr>,
    limit: Option<usize>,
    properties: PlanProperties,

    ctx: ObjectStoreContext,
}

impl std::fmt::Debug for ObjectStoreRawExec {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{} prefix={:?}", self.name(), self.ctx.prefix.clone())
    }
}

impl DisplayAs for ObjectStoreRawExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "{} prefix={}",
            self.name(),
            self.ctx.prefix.clone().unwrap_or_default()
        )
    }
}

impl ExecutionPlan for ObjectStoreRawExec {
    fn name(&self) -> &'static str {
        "ObjectStoreRawExec"
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

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
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
            to_sendable_stream(self.ctx.clone(), self.limit), // TODO get prefix from filters
        )))
    }
}

impl ObjectStoreRawExec {
    pub(crate) fn new(
        projected_schema: SchemaRef,
        filters: &[Expr],
        limit: Option<usize>,
        ctx: ObjectStoreContext,
    ) -> Self {
        Self {
            projected_schema: Arc::clone(&projected_schema),
            _filters: filters.to_vec(),
            limit,
            properties: PlanProperties::new(
                EquivalenceProperties::new(projected_schema),
                Partitioning::UnknownPartitioning(1),
                ExecutionMode::Bounded,
            ),
            ctx,
        }
    }
}

pub(crate) fn to_sendable_stream(
    ctx: ObjectStoreContext,
    limit: Option<usize>,
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

                    let result: GetResult = ctx.store.get(&object_meta.location).await?;
                    let bytz = result.bytes().await?;

                    match ObjectStoreRawTable::to_record_batch(&[object_meta], &[bytz]) {
                        Ok(batch) => {yield Ok(batch); count += 1;},
                        Err(e) => yield Err(DataFusionError::Execution(format!("{e}"))),
                    }
                },
                Err(e) => yield Err(DataFusionError::Execution(format!("{e}"))),
            }

            // Early exit on LIMIT clause
            if let Some(limit) = limit {
                if count >= limit {
                    break;
                }
            }
        }
    }
}
