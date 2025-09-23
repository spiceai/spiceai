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

use std::{borrow::Cow, sync::Arc};

use datafusion::{
    catalog::{Session, TableProvider},
    common::{Constraints, Statistics},
    error::Result as DataFusionResult,
    physical_plan::ExecutionPlan,
};
use datafusion_expr::{Expr, LogicalPlan, TableProviderFilterPushDown, TableType, dml::InsertOp};
use opentelemetry::KeyValue;

use crate::request::{AsyncMarker, RequestContext};

/// [`DimensionTrackedTableProvider`] enables arbitrary metric tracking (for a given set of [`RequestContext`] dimensions), for when an arbitrary [`TableProvider`] is scanned (i.e. on [`TableProvider::scan`]).
pub struct DimensionTrackedTableProvider<F: Fn(&[KeyValue]) + Send + Sync> {
    pub inner: Arc<dyn TableProvider>,
    pub track: F,
}

impl<F: Fn(&[KeyValue]) + Send + Sync> std::fmt::Debug for DimensionTrackedTableProvider<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

#[async_trait::async_trait]
impl<F: Fn(&[KeyValue]) + Send + Sync> TableProvider for DimensionTrackedTableProvider<F> {
    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let request_context = RequestContext::current(AsyncMarker::new().await);
        let track_fn = self.track;
        track_fn(&request_context.to_dimensions());

        self.inner.scan(state, projection, filters, limit).await
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self.inner.as_any()
    }

    fn schema(&self) -> datafusion::arrow::datatypes::SchemaRef {
        self.inner.schema()
    }

    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    fn constraints(&self) -> Option<&Constraints> {
        self.inner.constraints()
    }

    fn get_table_definition(&self) -> Option<&str> {
        self.inner.get_table_definition()
    }

    fn get_logical_plan(&self) -> Option<Cow<'_, LogicalPlan>> {
        self.inner.get_logical_plan()
    }

    fn get_column_default(&self, column: &str) -> Option<&Expr> {
        self.inner.get_column_default(column)
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        self.inner.supports_filters_pushdown(filters)
    }

    fn statistics(&self) -> Option<Statistics> {
        self.inner.statistics()
    }

    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        self.inner.insert_into(state, input, insert_op).await
    }
}
