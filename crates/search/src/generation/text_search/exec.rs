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
use std::{any::Any, fmt::Formatter, sync::Arc};

use arrow::{datatypes::SchemaRef, error::ArrowError};
use async_stream::stream;
use datafusion::{
    error::{DataFusionError, Result as DataFusionResult},
    execution::SendableRecordBatchStream,
    physical_expr::EquivalenceProperties,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
        execution_plan::{Boundedness, EmissionType},
        stream::RecordBatchStreamAdapter,
    },
    prelude::Expr as LogicalExpr,
};

use futures::StreamExt;

use super::{CandidateGeneration, FullTextSearchFieldIndex};

/// Executes a search on a [`FullTextSearchFieldIndex`] with a given query.
pub struct FullTextSearchExec {
    pub(super) index: FullTextSearchFieldIndex,
    pub(super) query: String,
    filters: Vec<LogicalExpr>,
    limit: usize,
    plan_properties: PlanProperties,
}

impl FullTextSearchExec {
    pub fn try_new(
        index: FullTextSearchFieldIndex,
        query: String,
        schema: SchemaRef,
        projection: Option<&Vec<usize>>,
        filters: Vec<LogicalExpr>,
        limit: usize,
    ) -> Result<Self, ArrowError> {
        let schema = match projection {
            Some(proj) => Arc::new(schema.project(proj.as_slice())?),
            None => schema,
        };

        Ok(Self {
            index,
            query,
            filters,
            limit,
            plan_properties: PlanProperties::new(
                EquivalenceProperties::new(schema),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            ),
        })
    }
}

impl std::fmt::Debug for FullTextSearchExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FullTextSearchTableExec")
            .field("index", &self.index)
            .field("query", &self.query)
            .field("filters", &self.filters)
            .field("limit", &self.limit)
            .finish_non_exhaustive()
    }
}
impl DisplayAs for FullTextSearchExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "FullTextSearchTableExec q={}", self.query)
    }
}

impl ExecutionPlan for FullTextSearchExec {
    fn name(&self) -> &'static str {
        "FullTextSearchTableExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.plan_properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::clone(&self) as Arc<dyn ExecutionPlan>)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let idx = self.index.clone();
        let schema = self.schema();
        let limit = self.limit;
        let query = self.query.clone();
        let s = stream! {
        // TODO: Support filters.
            match idx
                .search(query, &[], &[], limit)
                .await
                .map_err(|e| DataFusionError::Plan(format!("Failed to prepare full text search: {e}"))) {
                Ok(mut stream) => {
                    while let Some(item) = stream.next().await {
                        match item {
                            Err(e) => yield Err(e),
                            Ok(rb) => {
                                // Apply projection, as per `self.schema()`, to record batch from FTS.
                                let proj = rb.schema().fields().iter().enumerate().filter_map(|(i, f)| {
                                    if schema.column_with_name(f.name()).is_some() {
                                        Some(i)
                                    } else {
                                        None
                                    }
                                }).collect::<Vec<_>>();
                                yield rb.project(proj.as_slice()).map_err(DataFusionError::from)
                            }
                        }
                    }
                },
                Err(e) => {
                    yield Err(e);
                    return;
                }
            }
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(self.schema(), s)))
    }
}
