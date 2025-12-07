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

use std::{any::Any, cmp::min, sync::Arc};

use crate::{
    SEARCH_SCORE_COLUMN_NAME,
    generation::{
        text_search::{DEFAULT_LIMIT_MAXIMUM, FullTextSearchFieldIndex, exec::FullTextSearchExec},
        util::append_fields,
    },
};
use arrow::datatypes::{Field, Schema};
use async_trait::async_trait;
use datafusion::{
    catalog::{Session, TableProvider},
    datasource::TableType,
    error::DataFusionError,
    physical_plan::ExecutionPlan,
};

/// [`FullTextSearchQuery`] represents a [`TableProvider`] on a full text search index for a given query.
/// [`RecordBatch`] results will be ordered by the relevancy score.
#[derive(Debug)]
pub struct FullTextSearchQuery {
    pub index: Arc<FullTextSearchFieldIndex>,
    pub query: String,

    /// If Some(N), will only retrieve `N` results from the index. If filters are provided that are
    /// unsupported by the index (i.e. via its[`TableProvider::supports_filters_pushdown`] ), then
    ///  `< N` will be returned in the overall SQL query.
    /// If a `limit` is provided such that `limit` < `pre_limit`, `limit` will be used.
    pub pre_limit: Option<usize>,
}

impl FullTextSearchQuery {
    /// Determine whether and how to pick between
    ///   1. The query-provided limit (i.e. passed through in the SQL/Logical plan)
    ///   2. The pre-limit configured in [`FullTextSearchQuery::pre_limit`].
    fn limit_to_use(&self, limit: Option<usize>) -> Option<usize> {
        match (self.pre_limit, limit) {
            (Some(l), None) | (None, Some(l)) => Some(l),
            (None, None) => None,

            // Equivalent to using always using pre_limit, unless `limit` < `pre_limit`.
            (Some(a), Some(b)) => Some(min(a, b)),
        }
    }
}

#[async_trait]
impl TableProvider for FullTextSearchQuery {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Arc<Schema> {
        append_fields(
            &self.index.schema(),
            vec![
                Field::new(
                    SEARCH_SCORE_COLUMN_NAME,
                    arrow::datatypes::DataType::Float64,
                    false,
                )
                .into(),
            ],
        )
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[datafusion::prelude::Expr],
        limit: Option<usize>,
    ) -> std::result::Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Ok(Arc::new(
            FullTextSearchExec::try_new(
                &self.index,
                self.query.clone(),
                self.schema(),
                projection,
                filters.to_vec(),
                self.limit_to_use(limit).unwrap_or(DEFAULT_LIMIT_MAXIMUM),
            )
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?,
        ))
    }
}
