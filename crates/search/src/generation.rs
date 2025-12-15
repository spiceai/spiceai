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

use async_trait::async_trait;
use datafusion::catalog::TableProvider;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
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
        #[cfg(feature = "text_search")]
        {
            matches!(
                self,
                Error::TextSearchError { source } if source.is_user_error()
            )
        }
        #[cfg(not(feature = "text_search"))]
        {
            false
        }
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
    /// [`LogicalPlan`] output [`DFSchema`] expects at least a [`super::SEARCH_SCORE_COLUMN_NAME`] column of type [`arrow::array::Float64Array`].
    ///
    /// Rows in the [`RecordBatch`] must be ordered by [`super::SEARCH_SCORE_COLUMN_NAME`] descendingly.
    fn search(&self, query: String) -> DataFusionResult<Arc<dyn TableProvider>>;

    /// Returns the name of the column that is used to derive the value in the [`SEARCH_VALUE_COLUMN_NAME`] column.
    fn value_derived_from(&self) -> String;

    /// Returns the name of the field in the schema of [`CandidateGeneration::search`] that has the search result match.
    fn value_projection_name(&self) -> String {
        self.value_derived_from()
    }
}
