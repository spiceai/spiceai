/*
Copyright 2025 The Spice.ai OSS Authors

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

use std::{any::Any, borrow::Cow, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    arrow::datatypes::SchemaRef,
    catalog::{Session, TableProvider},
    common::{Constraints, Statistics},
    datasource::TableType,
    error::Result as DataFusionResult,
    logical_expr::{LogicalPlan, TableProviderFilterPushDown, dml::InsertOp},
    physical_plan::ExecutionPlan,
    prelude::Expr,
};

use crate::Index;

/// A `TableProvider` that wraps another `TableProvider` and adds indexing capabilities.
#[derive(Debug, Clone)]
pub struct IndexedTableProvider {
    /// The underlying `TableProvider` that provides the data.
    pub underlying: Arc<dyn TableProvider>,

    /// Indexes that are available to make queries more efficient or enable new functionality (i.e. full text search indexes).
    ///
    /// In the future, indexes will be required to implement a trait - but for now all existing
    /// use-cases are supported via UDTFs that downcast indexes to the correct type.
    pub indexes: Vec<Arc<dyn Index + Send + Sync>>,
}

impl IndexedTableProvider {
    pub fn new(underlying: Arc<dyn TableProvider>) -> Self {
        IndexedTableProvider::with_indexes(underlying, vec![])
    }

    pub fn with_indexes(
        underlying: Arc<dyn TableProvider>,
        indexes: Vec<Arc<dyn Index + Send + Sync>>,
    ) -> Self {
        Self {
            underlying,
            indexes,
        }
    }

    #[must_use]
    pub fn add_index(mut self, index: Arc<dyn Index + Send + Sync>) -> Self {
        self.indexes.push(index);
        self
    }

    #[must_use]
    pub fn get_index<T: Index + 'static>(&self) -> Option<&T> {
        self.indexes
            .iter()
            .find_map(|i| i.as_any().downcast_ref::<T>())
    }

    #[must_use]
    pub fn get_indexes<T: Index + 'static>(&self) -> Vec<&T> {
        self.indexes
            .iter()
            .filter_map(|i| i.as_any().downcast_ref::<T>())
            .collect()
    }

    #[must_use]
    pub fn get_underlying(&self) -> Arc<dyn TableProvider> {
        Arc::clone(&self.underlying)
    }

    #[must_use]
    pub fn get_underlying_ref(&self) -> &Arc<dyn TableProvider> {
        &self.underlying
    }
}

#[async_trait]
impl TableProvider for IndexedTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.underlying.schema()
    }

    fn constraints(&self) -> Option<&Constraints> {
        self.underlying.constraints()
    }

    fn table_type(&self) -> TableType {
        self.underlying.table_type()
    }

    fn get_table_definition(&self) -> Option<&str> {
        self.underlying.get_table_definition()
    }

    fn get_logical_plan(&self) -> Option<Cow<LogicalPlan>> {
        self.underlying.get_logical_plan()
    }

    fn get_column_default(&self, column: &str) -> Option<&Expr> {
        self.underlying.get_column_default(column)
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        self.underlying
            .scan(state, projection, filters, limit)
            .await
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        self.underlying.supports_filters_pushdown(filters)
    }

    fn statistics(&self) -> Option<Statistics> {
        self.underlying.statistics()
    }

    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        self.underlying.insert_into(state, input, insert_op).await
    }
}
