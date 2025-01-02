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
use std::{any::Any, fmt, sync::Arc};

use arrow::{array::RecordBatch, datatypes::SchemaRef};
use async_stream::stream;
use async_trait::async_trait;
use datafusion::{
    catalog::Session,
    common::project_schema,
    datasource::{TableProvider, TableType},
    error::{DataFusionError, Result as DataFusionResult},
    execution::{SendableRecordBatchStream, TaskContext},
    logical_expr::Expr,
    physical_expr::EquivalenceProperties,
    physical_plan::{
        stream::RecordBatchStreamAdapter, DisplayAs, DisplayFormatType, ExecutionMode,
        ExecutionPlan, Partitioning, PlanProperties,
    },
};
use document_parse::DocumentParser;
use futures::{Stream, StreamExt};
use graph_rs_sdk::GraphFailure;
use snafu::ResultExt;

use crate::sharepoint::drive_items::drive_items_to_record_batch;

use super::{
    client::SharepointClient,
    drive_items::{drive_item_table_schema, DRIVE_ITEM_FILE_CONTENT_COLUMN},
    error::Error,
};

pub struct SharepointTableProvider {
    client: SharepointClient,
    formatter: Option<Arc<dyn DocumentParser>>,
    include_file_content: bool,
}

impl std::fmt::Debug for SharepointTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharepointTableProvider")
            .field("include_file_content", &self.include_file_content)
            .finish_non_exhaustive()
    }
}

impl SharepointTableProvider {
    #[must_use]
    pub fn new(
        client: SharepointClient,
        include_file_content: bool,
        formatter: Option<Arc<dyn DocumentParser>>,
    ) -> Self {
        Self {
            client,
            formatter,
            include_file_content,
        }
    }
}

#[async_trait]
impl TableProvider for SharepointTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(drive_item_table_schema(self.include_file_content))
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(SharepointListExec::new(
            self.client.clone(),
            projection,
            &self.schema(),
            limit,
            self.formatter.as_ref(),
        )?))
    }
}

struct SharepointListExec {
    client: SharepointClient,
    schema: SchemaRef,
    properties: PlanProperties,
    projections: Option<Vec<usize>>,
    limit: Option<usize>,
    formatter: Option<Arc<dyn DocumentParser>>,
}

impl SharepointListExec {
    pub fn new(
        client: SharepointClient,
        projections: Option<&Vec<usize>>,
        schema: &SchemaRef,
        limit: Option<usize>,
        formatter: Option<&Arc<dyn DocumentParser>>,
    ) -> DataFusionResult<Self> {
        let projected_schema = project_schema(schema, projections)?;
        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&projected_schema)),
            Partitioning::UnknownPartitioning(1),
            ExecutionMode::Bounded,
        );

        Ok(Self {
            client,
            schema: projected_schema,
            properties,
            limit,
            projections: projections.cloned(),
            formatter: formatter.cloned(),
        })
    }

    /// Returns the drive items, converted into a stream of [`RecordBatch`]s.
    /// If `include_file_content`, the file content for each drive item is downloaded and included
    /// under the `DRIVE_ITEM_FILE_CONTENT_COLUMN` column.
    fn create_record_stream(
        &self,
        include_file_content: bool,
    ) -> DataFusionResult<impl Stream<Item = DataFusionResult<RecordBatch>>> {
        let mut resp_stream = self.client.stream_drive_items(self.limit).map_err(|e| {
            DataFusionError::External(Error::MicrosoftGraphFailure { source: e }.into())
        })?;

        let client = self.client.clone();
        let projection = self.projections.clone();
        let formatter = self.formatter.clone();

        Ok(stream! {

            while let Some(s) = resp_stream.next().await {
                let response = match s {
                    Ok(r) => r,
                    Err(e) => {
                        yield Err(DataFusionError::External(Error::MicrosoftGraphFailure { source: e }.into()));
                        continue;
                    }
                };
                match response.body() {
                    Ok(drive_items) => {
                        let content = if include_file_content {
                            match client.get_file_content(&drive_items.value, formatter.clone()).await.boxed() {
                                Ok(c) => Some(c),
                                Err(e) => {
                                    yield Err(DataFusionError::External(e));
                                    continue;
                                }
                            }
                        } else {
                            None
                        };
                        match drive_items_to_record_batch(&drive_items.value, content) {
                            Ok(record_batch) => {
                                // Ensure that the record batch is projected to the required columns (since `select` on OData from Microsoft Graph isn't used).
                                if let Some(projection) = &projection {
                                    yield record_batch.project(projection).map_err(|e| DataFusionError::ArrowError(e, None))
                                } else {
                                    yield Ok(record_batch)
                                }
                            },
                            Err(e) => yield Err(DataFusionError::ArrowError(e, None)),
                        }
                    },
                    Err(e) => {
                        tracing::debug!("Error fetching drive items. {:#?}", e);
                        yield Err(DataFusionError::External(Error::MicrosoftGraphFailure { source: GraphFailure::ErrorMessage(e.clone()) }.into()));
                    },
                }
            }
        })
    }
}

impl ExecutionPlan for SharepointListExec {
    fn name(&self) -> &'static str {
        "SharepointListExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
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
        // Only retrieve file content if it is in projected schema.
        let include_file_content: bool = self
            .schema()
            .index_of(DRIVE_ITEM_FILE_CONTENT_COLUMN)
            .is_ok();
        let stream_adapter = RecordBatchStreamAdapter::new(
            self.schema(),
            self.create_record_stream(include_file_content)?,
        );

        Ok(Box::pin(stream_adapter))
    }
}

impl std::fmt::Debug for SharepointListExec {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "SharepointListExec client={:?}", self.client)
    }
}

impl DisplayAs for SharepointListExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        write!(f, "SharepointListExec client={:?}", self.client)
    }
}
