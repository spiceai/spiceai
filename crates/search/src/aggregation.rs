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

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::execution::SendableRecordBatchStream;
use snafu::Snafu;

pub mod reciprocal_rank;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Generated candidate data missing required column '{col}'."))]
    CandidateMissingRequiredColumn { col: String },

    #[snafu(display("No candidates generated"))]
    NoCandidatesGenerated,

    #[snafu(display(
        "Cannot aggregate candidates from multiple sources (e.g. embedded columns of a dataset) when the dataset has no primary key."
    ))]
    NoPrimaryKey,

    #[snafu(display(
        "Generated candidates have inconsistent columns. From {:?}. And {:?}.",
        s1.fields().iter().map(|f| format!("{}: {}", f.name(), f.data_type())).collect::<Vec<_>>().join(", "),
        s2.fields().iter().map(|f| format!("{}: {}", f.name(), f.data_type())).collect::<Vec<_>>().join(", "),
    ))]
    InconsistentColumns { s1: SchemaRef, s2: SchemaRef },

    #[snafu(display("A database error occurred whilst aggregating search candidates: {source}"))]
    DatafusionError {
        source: datafusion::error::DataFusionError,
    },
}

impl Error {
    #[must_use]
    pub fn is_user_error(&self) -> bool {
        matches!(self, Error::NoPrimaryKey)
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Standard interface for algorithms that decide how to aggregate the results from [`super::generation::CandidateGeneration::search`] into a single-ordered set.
///
/// Candidates (in `candidate_sets`) are expected to have columns as per [`super::generation::CandidateGeneration::search`] documentation. Any additional columns are expected to be common across all [`SendableRecordBatchStream`] in `candidate_sets`.
#[async_trait]
pub trait CandidateAggregation: Sync + Send {
    /// Consumes `candidate_sets` and decides how to order them into a single [`SendableRecordBatchStream`].
    ///
    /// Rows from different [`SendableRecordBatchStream`]s with an equal `primary_key` are considered the same row.
    async fn aggregate(
        &self,
        mut candidate_sets: Vec<SendableRecordBatchStream>,
        primary_key: Vec<String>,
        limit: usize,
    ) -> Result<SendableRecordBatchStream>;
}
