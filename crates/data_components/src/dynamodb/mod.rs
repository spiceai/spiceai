/*
Copyright 2025 The Spice.ai OSS Authors

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
use datafusion::error::DataFusionError;
use snafu::Snafu;
use std::sync::Arc;

mod arrow;
pub mod provider;
mod request_builder;
mod request_plan;
mod schema;
pub mod stream;
mod table_schema;
mod unnest;
mod utils;

type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Failed to fetch table information. Error: {source} Verify configuration and try again. For details, visit https://spiceai.org/docs/components/data-connectors/dynamodb"
    ))]
    DescribeTableError {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("{source}"))]
    ScanError {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Table does not exist: {table_name}"))]
    TableDoesNotExist { table_name: Arc<str> },

    #[snafu(display("Table status is not active"))]
    TableStatusIsNotActive,

    #[snafu(display("Failed to infer schema: {source}"))]
    SchemaInferenceError { source: ::arrow::error::ArrowError },

    #[snafu(display("Failed to convert DynamoDB items to Arrow: {source}"))]
    ConversionError {
        source: Box<dyn std::error::Error + std::marker::Send + Sync>,
    },

    #[snafu(display("Invalid item access: {message}"))]
    InvalidItemAccess { message: String },

    #[snafu(display("Type {unsupported_type_name} is not supported"))]
    UnsupportedType { unsupported_type_name: String },

    #[snafu(display("DynamoDB returned value of 'Unknown' type"))]
    UnknownType,

    #[snafu(display(
        "Maximum recursion depth of {max_depth} exceeded while processing nested DynamoDB data"
    ))]
    MaxRecursionDepthExceeded { max_depth: usize },

    #[snafu(display("Table has no partition key"))]
    MissingPartitionKey,

    #[snafu(display("Failed to initialize DynamoDB Stream checkpoint: {source}"))]
    FailedToInitializeCheckpoint { source: dynamodb_streams::Error },

    #[snafu(display("Failed to initialize DynamoDB Stream: {source}"))]
    FailedToInitializeStream { source: dynamodb_streams::Error },

    #[snafu(display("Failed to Bootstrap DynamoDB Table: {source}"))]
    FailedToBootstrapTable { source: DataFusionError },
}
