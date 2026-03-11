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

use snafu::prelude::*;

use crate::Operator;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Invalid filter: {message}"))]
    InvalidFilter { message: String },

    #[snafu(display("Unsupported operator: {not_an_operator}"))]
    UnsupportedOperator { not_an_operator: String },

    #[snafu(display("Invalid value type for operator {}: expected {expected}, got {actual}", operator.as_str()))]
    InvalidValueType {
        operator: Operator,
        expected: String,
        actual: String,
    },

    #[snafu(display("JSON parsing error: {source}"))]
    JsonParsing { source: serde_json::Error },

    #[snafu(display("DataFusion error: {source}"))]
    DataFusion {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("Field not found: {field}"))]
    FieldNotFound { field: String },

    #[snafu(display("Empty array not allowed for {aggregate}"))]
    EmptyArray { aggregate: String },

    #[snafu(display(
        "Maximum recursion depth of {max_depth} exceeded while processing nested S3 Vectors data"
    ))]
    MaxRecursionDepthExceeded { max_depth: usize },
}

pub type Result<T> = std::result::Result<T, Error>;
