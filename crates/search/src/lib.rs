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

use arrow::array::RecordBatch;
use datafusion::{error::DataFusionError, execution::SendableRecordBatchStream};

use futures::StreamExt;
pub mod aggregation;
pub mod generation;
pub mod index;
pub mod metadata;
pub mod pipeline;
pub mod provider;

pub static SEARCH_SCORE_COLUMN_NAME: &str = "score";
pub static SEARCH_VALUE_COLUMN_NAME: &str = "value";
pub static SEARCH_MATCH_COLUMN_NAME: &str = "match";

pub async fn collect_batches(
    mut stream: SendableRecordBatchStream,
) -> std::result::Result<Vec<RecordBatch>, DataFusionError> {
    let mut batches = Vec::new();
    while let Some(batch) = stream.next().await {
        batches.push(batch?);
    }

    Ok(batches)
}

/// The results of [`CandidateGeneration::search`]'s on a single table.
///
/// Rows from different [`SendableRecordBatchStream`]s with an equal `primary_key` are considered the same row.
pub struct VectorSearchGenerationTableResult {
    pub data: Vec<VectorSearchGenerationResult>,
    pub primary_keys: Vec<String>,
}

/// The results of a single [`CandidateGeneration::search`].
pub struct VectorSearchGenerationResult {
    pub data: SendableRecordBatchStream,
    pub derived_from: String,
}
