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
pub mod candidate;
pub mod full_text;
pub mod request;
pub mod rrf;
pub mod search_engine;
pub mod types;
pub mod util;

use arrow_schema::ArrowError;
use datafusion::sql::TableReference;
use itertools::Itertools;
use search::aggregation;
use snafu::prelude::*;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Data sources [{}] does not exist", data_source.iter().map(TableReference::to_quoted_string).join(", ")))]
    DataSourcesNotFound { data_source: Vec<TableReference> },

    #[snafu(display("Failed to find table '{}'. An internal error occurred during vector search. Report a bug on GitHub: https://github.com/spiceai/spiceai/issues", table.to_quoted_string()))]
    DataSourceNotFound { table: TableReference },

    #[snafu(display(
        "Vector search failed: No tables with embeddings or full text search indexes are available. Ensure embeddings or full text search indexes are configured and try again."
    ))]
    NoTablesWithSearchFound {},

    #[snafu(display("Vector search cannot be run on {}.", data_source.to_quoted_string()))]
    CannotVectorSearchDataset { data_source: TableReference },

    #[snafu(display("Error occurred interacting with datafusion: {source}"))]
    DataFusionError {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("Error occurred in search pipeline: {source}"))]
    SearchPipelineError { source: search::pipeline::Error },

    #[snafu(display("Error occurred generating search candidates: {source}"))]
    SearchGenerationError { source: search::generation::Error },

    #[snafu(display("Error occurred aggregating candidate search results: {source}"))]
    SearchAggregationError { source: aggregation::Error },

    #[snafu(display("Error occurred processing Arrow records: {source}"))]
    RecordProcessingError { source: ArrowError },

    #[snafu(display("Could not format search results: {source}"))]
    FormattingError {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Data source {} does not contain any embedding columns", data_source.to_string()))]
    NoEmbeddingColumns { data_source: TableReference },

    #[snafu(display("Only one embedding column per table currently supported. Table: {} has {num_embeddings} embeddings", data_source.to_string()))]
    IncorrectNumberOfEmbeddingColumns {
        data_source: TableReference,
        num_embeddings: usize,
    },

    #[snafu(display("Embedding model {model_name} not found"))]
    EmbeddingModelNotFound { model_name: String },

    #[snafu(display("Error embedding input text: {source}"))]
    EmbeddingError {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Invalid WHERE condition: {where_cond}"))]
    InvalidWhereCondition { where_cond: String },

    #[snafu(display("An invalid keyword was specified: {keyword}"))]
    InvalidKeyword { keyword: String },

    #[snafu(display("Invalid additional column was specified: {additional_column}"))]
    InvalidAdditionalColumns { additional_column: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
