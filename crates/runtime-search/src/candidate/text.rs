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
#[cfg(feature = "text_search")]
use std::sync::Arc;

#[cfg(feature = "text_search")]
use crate::udtf::{TEXT_SEARCH_UDTF_NAME, TextSearchTableFuncArgs};
#[cfg(feature = "text_search")]
use datafusion::catalog::TableProvider;
#[cfg(feature = "text_search")]
use datafusion::error::DataFusionError;

#[cfg(feature = "text_search")]
use datafusion::sql::TableReference;
#[cfg(feature = "text_search")]
use search::generation::CandidateGeneration;
#[cfg(feature = "text_search")]
use search::generation::text_search::FullTextSearchFieldIndex;
#[cfg(feature = "text_search")]
use tonic::async_trait;

#[cfg(feature = "text_search")]
use runtime_query_engine::query_engine::QueryEngine;

#[cfg(feature = "text_search")]
pub struct TextSearchCandidate {
    inner: Arc<FullTextSearchFieldIndex>,
    df: Arc<dyn QueryEngine>,
    tbl: TableReference,
}

#[cfg(feature = "text_search")]
impl TextSearchCandidate {
    pub fn new(
        inner: Arc<FullTextSearchFieldIndex>,
        df: Arc<dyn QueryEngine>,
        tbl: TableReference,
    ) -> Self {
        Self { inner, df, tbl }
    }
}

#[cfg(feature = "text_search")]
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
