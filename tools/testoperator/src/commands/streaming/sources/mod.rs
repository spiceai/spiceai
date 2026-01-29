/*
Copyright 2026 The Spice.ai OSS Authors

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

//! Streaming source implementations for benchmarks.

mod dynamodb;
mod dynamodb_local;

pub use dynamodb::{DynamoDbConfig, DynamoDbStreamsSource};
pub use dynamodb_local::DynamoDbStreamsLocalSource;

use clap::ValueEnum;
use serde::{Deserialize, Serialize};
use test_framework::anyhow::Result;

use super::traits::DynamoDBStreamingSource;

/// Available DynamoDB streaming source types for benchmarks.
#[derive(Debug, Clone, Copy, ValueEnum, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum SourceType {
    /// `DynamoDB` Streams (local Docker container)
    DynamodbStreamsLocal,
    /// AWS `DynamoDB` Streams (actual AWS service)
    DynamodbStreams,
}

impl SourceType {
    /// Create a DynamoDB streaming source instance for this type.
    ///
    /// Configuration is read from environment variables:
    /// - `DynamodbStreamsLocal`: `DYNAMODB_LOCAL_PORT` (optional, default: 8000)
    /// - `DynamodbStreams`: `DYNAMODB_AWS_REGION`, `DYNAMODB_AWS_ACCESS_KEY_ID`,
    ///   `DYNAMODB_AWS_SECRET_ACCESS_KEY` (required), `DYNAMODB_AWS_ENDPOINT_URL` (optional)
    ///
    /// # Errors
    /// Returns an error if required environment variables are not set for `DynamodbStreams`.
    pub fn create_dynamodb(self) -> Result<Box<dyn DynamoDBStreamingSource>> {
        match self {
            Self::DynamodbStreamsLocal => Ok(Box::new(DynamoDbStreamsLocalSource::new())),
            Self::DynamodbStreams => {
                let config = DynamoDbConfig::from_env()?;
                Ok(Box::new(DynamoDbStreamsSource::new(config)))
            }
        }
    }
}

impl std::fmt::Display for SourceType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SourceType::DynamodbStreamsLocal => write!(f, "dynamodb-streams"),
            SourceType::DynamodbStreams => write!(f, "aws-dynamodb-streams"),
        }
    }
}
