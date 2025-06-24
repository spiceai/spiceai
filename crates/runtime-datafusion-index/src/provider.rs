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
    logical_expr::{LogicalPlan, TableProviderFilterPushDown},
    physical_plan::ExecutionPlan,
    prelude::Expr,
};

/// A `TableProvider` that wraps another `TableProvider` and adds indexing capabilities.
pub struct IndexedTableProvider {
    /// The underlying `TableProvider` that provides the data.
    pub underlying: Arc<dyn TableProvider>,

    /// Indexes that are available to make queries more efficient.
    ///
    /// In the future, indexes will be required to implement a trait - but for now all existing
    /// use-cases are supported via UDTFs that downcast indexes to the correct type.
    pub indexes: Vec<Arc<dyn Any>>,
}

impl IndexedTableProvider {
    pub fn new(underlying: Arc<dyn TableProvider>) -> Self {
        Self {
            underlying,
            indexes: Vec::new(),
        }
    }

    pub fn add_index(&mut self, index: Arc<dyn Any>) {
        self.indexes.push(index);
    }
}

#[async_trait]
impl TableProvider for IndexedTableProvider {
    /// Returns the table provider as [`Any`](std::any::Any) so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.underlying.schema()
    }

    fn constraints(&self) -> Option<&Constraints> {
        self.underlying.constraints()
    }

    /// Get the type of this table for metadata/catalog purposes.
    fn table_type(&self) -> TableType {
        self.underlying.table_type()
    }

    /// Get the create statement used to create this table, if available.
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
        let underlying = Arc::clone(&self.underlying);
        underlying.scan(state, projection, filters, limit).await
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

    /// Get statistics for this table, if available
    /// Although not presently used in mainline DataFusion, this allows implementation specific
    /// behavior for downstream repositories, in conjunction with specialized optimizer rules to
    /// perform operations such as re-ordering of joins.
    fn statistics(&self) -> Option<Statistics> {
        self.underlying.statistics()
    }
}
