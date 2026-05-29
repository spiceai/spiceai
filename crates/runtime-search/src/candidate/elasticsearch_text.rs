/*
Copyright 2026 The Spice.ai OSS Authors

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

//! [`CandidateGeneration`] for Elasticsearch BM25 full-text search.
//!
//! Delegates to the `text_search()` UDTF, which routes to [`ElasticsearchTextIndex`]
//! via the two-phase index probe added in the UDTF dispatcher.

use std::sync::Arc;

use datafusion::catalog::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::sql::TableReference;
use search::generation::CandidateGeneration;
use tonic::async_trait;

use crate::udtf::{TEXT_SEARCH_UDTF_NAME, TextSearchTableFuncArgs};
use runtime_query_engine::query_engine::QueryEngine;

/// A [`CandidateGeneration`] that issues `text_search()` UDTF calls routed to
/// Elasticsearch BM25 search for a single field on a single table.
pub struct ElasticsearchTextSearchCandidate {
    /// The field (column) to search.
    field: String,
    df: Arc<dyn QueryEngine>,
    tbl: TableReference,
}

impl ElasticsearchTextSearchCandidate {
    pub fn new(field: String, df: Arc<dyn QueryEngine>, tbl: TableReference) -> Self {
        Self { field, df, tbl }
    }
}

#[async_trait]
impl CandidateGeneration for ElasticsearchTextSearchCandidate {
    fn search(&self, query: String) -> Result<Arc<dyn TableProvider>, DataFusionError> {
        let udtf_args = TextSearchTableFuncArgs {
            tbl: self.tbl.clone(),
            query,
            column: Some(self.field.clone()),
            limit: None,
            include_score: Some(true),
        }
        .to_expr();

        self.df
            .session_context()
            .table_function(TEXT_SEARCH_UDTF_NAME)?
            .create_table_provider(udtf_args.as_slice())
    }

    fn value_derived_from(&self) -> String {
        self.field.clone()
    }
}
