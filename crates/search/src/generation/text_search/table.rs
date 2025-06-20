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
use std::{any::Any, sync::Arc};

use crate::{
    SEARCH_SCORE_COLUMN_NAME,
    generation::text_search::{
        DEFAULT_BATCH_SIZE, FullTextSearchIndex, exec::FullTextSearchExec, tantivy_to_arrow_type,
    },
};
use arrow::datatypes::{Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::{
    catalog::{Session, TableProvider},
    common::Constraints,
    datasource::TableType,
    error::{DataFusionError, Result as DataFusionResult},
    logical_expr::TableProviderFilterPushDown,
    physical_plan::ExecutionPlan,
    prelude::Expr as LogicalExpr,
};

/// An implementation of [`TableProvider`] based on a given query on a [`FullTextSearch`] index.
///
/// Currently, filter pushdown support is unavailable.
#[derive(Clone, Debug)]
pub struct FullTextSearchTable {
    pub index: FullTextSearchIndex,
    pub query: String,
    pub default_limit: usize,
}

impl FullTextSearchTable {
    #[must_use]
    pub fn new(index: FullTextSearchIndex, query: String) -> Self {
        Self {
            index,
            query,
            default_limit: DEFAULT_BATCH_SIZE,
        }
    }

    #[must_use]
    pub fn with_limit(mut self, limit: usize) -> Self {
        self.default_limit = limit;
        self
    }
}
#[async_trait]
impl TableProvider for FullTextSearchTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        let tantivy_schema = self.index.idx.schema();

        let fields = self
            .index
            .all_columns()
            .iter()
            .filter_map(|field_name| {
                let f = tantivy_schema.get_field(field_name).ok()?;
                let entry = tantivy_schema.get_field_entry(f);
                let data_type = tantivy_to_arrow_type(entry.field_type())?;
                Some(Field::new(field_name, data_type, false))
            })
            .chain([Field::new(
                SEARCH_SCORE_COLUMN_NAME,
                arrow::datatypes::DataType::Float64,
                false,
            )])
            .collect::<Vec<_>>();

        Arc::new(Schema::new(fields))
    }

    fn constraints(&self) -> Option<&Constraints> {
        // TODO primary keys
        None
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[LogicalExpr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(
            FullTextSearchExec::try_new(
                self.clone(),
                projection,
                filters.to_vec(),
                limit.unwrap_or(self.default_limit),
            )
            .map_err(DataFusionError::from)?,
        ))
    }

    fn supports_filters_pushdown(
        &self,
        _filters: &[&LogicalExpr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        Ok(vec![])
    }
}
