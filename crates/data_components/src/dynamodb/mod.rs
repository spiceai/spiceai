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
use aws_sdk_dynamodb::types::AttributeValue;
use datafusion::error::DataFusionError;
use snafu::Snafu;
use std::collections::HashMap;
use std::sync::Arc;

mod arrow;
mod json_nest;
pub mod provider;
mod request_builder;
mod request_plan;
mod schema;
pub mod stream;
mod table_schema;
mod unnest;
mod utils;

pub use json_nest::JsonNesting;

type DynamoDBRow = HashMap<String, AttributeValue>;

type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Failed to fetch table information. Error: {source} Verify configuration and try again. For details, visit https://spiceai.org/docs/components/data-connectors/dynamodb"
    ))]
    DescribeTableError {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to scan DynamoDB table: {source}"))]
    ScanError {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("DynamoDB table '{table_name}' does not exist"))]
    TableDoesNotExist { table_name: Arc<str> },

    #[snafu(display(
        "DynamoDB table is not in 'ACTIVE' status. Verify the table status and try again."
    ))]
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

    #[snafu(display("DynamoDB table has no partition key defined"))]
    MissingPartitionKey,

    #[snafu(display("Failed to initialize DynamoDB Stream checkpoint: {source}"))]
    FailedToInitializeCheckpoint { source: dynamodb_streams::Error },

    #[snafu(display("Failed to initialize DynamoDB Stream: {source}"))]
    FailedToInitializeStream { source: dynamodb_streams::Error },

    #[snafu(display("Failed to Bootstrap DynamoDB Table: {source}"))]
    FailedToBootstrapTable { source: DataFusionError },

    #[snafu(display("DynamoDB table {table_name} is empty"))]
    EmptyTable { table_name: String },

    #[snafu(display("Failed to serialize data to JSON: {source}"))]
    JsonSerializationError { source: serde_json::Error },

    #[snafu(display(
        "Columns not found in table schema: {field_names}. \
        Ensure configuration is correct, or increase schema_infer_max_records",
    ))]
    ColumnsNotFound { field_names: String },
}
