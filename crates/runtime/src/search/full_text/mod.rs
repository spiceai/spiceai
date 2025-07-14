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
use arrow_schema::DataType;
use snafu::Snafu;
use tantivy::TantivyError;

use datafusion::error::DataFusionError;

pub mod analyzer_rule;
pub mod connector;
pub mod index;
pub mod udtf;
mod util;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Full text search requires a primary key, and the table did not have one.",))]
    NoPrimaryKey,

    #[snafu(display(
        "Primary key column '{column}' used in search index has unsupported data type: '{data_type}'",
    ))]
    PrimaryKeyInvalidType { column: String, data_type: DataType },

    #[snafu(display("Primary key column '{column}' used in search index is not allowed.",))]
    PrimaryKeyInvalidName { column: String },

    #[snafu(display("Primary key column '{column}' not found in table.",))]
    PrimaryKeyNotFound { column: String },

    #[snafu(display("Failed to create a full text search index: {source}.",))]
    IndexCreationError { source: TantivyError },

    #[snafu(display("Failed to insert or update data into a full text search index: {source}.",))]
    IndexInsertionError { source: TantivyError },

    #[snafu(display("Failed to retrieve the data from the full text search index: {source}.",))]
    FailedToRetrieveDataFromIndex { source: TantivyError },

    #[snafu(display("Failed to retrieve the data from the underlying table: {source}.",))]
    FailedToRetrieveDataFromSource { source: DataFusionError },

    #[snafu(display("Failed to insert data into the full text search index: {source}.",))]
    FailedToInsertDataIntoIndex { source: TantivyError },

    #[snafu(display(
        "Failed to create the full text search index. Context: {context}. Error: {source}.",
    ))]
    InvalidIndexingError {
        source: Box<dyn std::error::Error + Send + Sync>,
        context: String,
    },
}
