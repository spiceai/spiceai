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

use async_trait::async_trait;
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::sqlparser::ast::Expr;
use snafu::Snafu;

#[cfg(feature = "text_search")]
pub mod text_search;

pub mod util;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Error occured during search: {source}"))]
    InternalError {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("A query engine error occured during search: {source}"))]
    QueryError { source: DataFusionError },

    #[cfg(feature = "text_search")]
    #[snafu(display("Error occured performing full text search: {source}"))]
    TextSearchError { source: text_search::Error },
}

impl Error {
    #[must_use]
    pub fn is_user_error(&self) -> bool {
        matches!(
            self,
            Error::TextSearchError { source } if source.is_user_error()
        )
    }

    #[must_use]
    pub fn internal(msg: &str) -> Self {
        Self::InternalError {
            source: Box::from(msg),
        }
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Standard interface to generate search candidates from a given table/dataset/source for subsequent aggregation in a hybrid search system.
#[async_trait]
pub trait CandidateGeneration: Sync + Send {
    /// Generates candidates for a given query term, ordered by decreasing score.
    ///
    /// Any filter within `opt_filters` where [`CandidateGeneration::supports_filters_pushdown`] evaluates to [`true`] is expected to be applied. No assumptions are made on other filters.
    ///
    /// [`RecordBatch`] expects at least two columns:
    ///   1. [`super::SEARCH_SCORE_COLUMN_NAME`] column of type [`arrow::array::Float64Array`].
    ///   2. [`super::SEARCH_VALUE_COLUMN_NAME`] column of type [`arrow::array::StringArray`], [`arrow::array::LargeStringArray`] or [`arrow::array::StringViewArray`].
    ///
    ///  Any column in `addition_projection` that evaluates to true in [`CandidateGeneration::supports_columns`] must also be returned. No assumptions are made on other columns.
    ///
    /// Rows in the [`RecordBatch`] must be ordered by [`super::SEARCH_SCORE_COLUMN_NAME`] descendingly.
    async fn search(
        &self,
        query: String,
        opt_filters: &[&Expr],
        addition_projection: &[&Expr],
        limit: usize,
    ) -> Result<SendableRecordBatchStream>;

    /// Whether candidates can be filtered during generation, i.e. [`CandidateGeneration::search`].
    fn supports_filters_pushdown(&self, filters: &[&Expr]) -> Result<Vec<bool>>;

    /// Whether additional columns of the underlying source can also be retrieved during generation.
    fn supports_columns(&self, projection: &[&Expr]) -> Result<Vec<bool>>;

    /// Returns the name of the column that is used to derive the value in the [`SEARCH_VALUE_COLUMN_NAME`] column.
    fn value_derived_from(&self) -> String;
}
