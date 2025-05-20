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

#![allow(clippy::missing_errors_doc)]
#![allow(clippy::missing_panics_doc)]
#![allow(clippy::doc_markdown)]

use async_trait::async_trait;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::sqlparser::ast::Expr;
use snafu::Snafu;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error occured during search: {source}"))]
    InternalError {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Standard interface to generate search candidates from a given table/dataset/source for subsequent aggregation in a hybrid search system.
/// The candidate generation phase should:
///  - Accept optional filter pushdown.
///  - Emit RecordBatch, or RecordBatch streams
///  - Would be nice if `impl TableProvider for CandidateGeneration` works.
#[async_trait]
pub trait CandidateGeneration: Sync + Send {
    /// Generates candidates for a query term.
    ///
    /// Any filter within `opt_filters` where [`CandidateGeneration::supports_filters_pushdown`] evaluates to [`true`] is expected to be applied. No assumptions are made on other filters.
    ///
    /// [`RecordBatch`] expects at least two columns: a 'score' column, and a column returning the underlying data that matched. Any column in `addition_projection` that evaluates to true in [`CandidateGeneration::supports_columns`] must also be returned. No assumptions are made on other columns.
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
}
