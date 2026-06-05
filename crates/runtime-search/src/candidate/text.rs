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
use std::sync::Arc;

use crate::udtf::{TEXT_SEARCH_UDTF_NAME, TextSearchTableFuncArgs};
use datafusion::catalog::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::sql::TableReference;
use runtime_query_engine::query_engine::QueryEngine;
use search::generation::CandidateGeneration;
use search::generation::text_search::FullTextSearchFieldIndex;
use tonic::async_trait;

pub struct TextSearchCandidate {
    inner: Arc<FullTextSearchFieldIndex>,
    df: Arc<dyn QueryEngine>,
    tbl: TableReference,
}

impl TextSearchCandidate {
    pub fn new(
        inner: Arc<FullTextSearchFieldIndex>,
        df: Arc<dyn QueryEngine>,
        tbl: TableReference,
    ) -> Self {
        Self { inner, df, tbl }
    }
}

#[async_trait]
impl CandidateGeneration for TextSearchCandidate {
    fn search(&self, query: String) -> Result<Arc<dyn TableProvider>, DataFusionError> {
        let udtf_args = TextSearchTableFuncArgs {
            tbl: self.tbl.clone(),
            query,
            column: Some(self.inner.field.clone()),
            limit: None,
            include_score: Some(true),
        }
        .to_expr();

        self.df
            .session_context()
            .table_function(TEXT_SEARCH_UDTF_NAME)?
            .create_table_provider(udtf_args.as_slice())
    }

    /// Returns the name of the column that is used to derive the value in the [`SEARCH_VALUE_COLUMN_NAME`] column.
    fn value_derived_from(&self) -> String {
        self.inner.field.clone()
    }
}
