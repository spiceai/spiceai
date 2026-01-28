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

mod aws_dynamodb;
mod dynamodb;

pub use aws_dynamodb::{AwsAuthMethod, AwsDynamoDbConfig, AwsDynamoDbStreamsSource};
pub use dynamodb::DynamoDbStreamsSource;

use clap::ValueEnum;
use serde::{Deserialize, Serialize};

use super::traits::StreamingSource;

/// Available streaming source types for benchmarks.
#[derive(Debug, Clone, Copy, ValueEnum, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum SourceType {
    /// `DynamoDB` Streams (local Docker container)
    DynamodbStreams,
    /// AWS `DynamoDB` Streams (actual AWS service)
    AwsDynamodbStreams,
    // Future: Kafka, Debezium, etc.
}

impl SourceType {
    /// Create a streaming source instance for this type.
    ///
    /// For `AwsDynamodbStreams`, this creates a source with default configuration.
    /// Use `create_aws` to configure AWS-specific options.
    pub fn create(&self) -> Box<dyn StreamingSource> {
        match self {
            SourceType::DynamodbStreams => Box::new(DynamoDbStreamsSource::new()),
            SourceType::AwsDynamodbStreams => {
                Box::new(AwsDynamoDbStreamsSource::new(AwsDynamoDbConfig::default()))
            }
        }
    }

    /// Create an AWS `DynamoDB` source with the given configuration.
    ///
    /// This is only valid for `AwsDynamodbStreams` source type.
    pub fn create_aws(&self, config: AwsDynamoDbConfig) -> Box<dyn StreamingSource> {
        match self {
            SourceType::AwsDynamodbStreams => Box::new(AwsDynamoDbStreamsSource::new(config)),
            SourceType::DynamodbStreams => Box::new(DynamoDbStreamsSource::new()),
        }
    }

    /// Check if this source type requires AWS configuration.
    #[must_use]
    pub fn requires_aws_config(&self) -> bool {
        matches!(self, SourceType::AwsDynamodbStreams)
    }
}

impl std::fmt::Display for SourceType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SourceType::DynamodbStreams => write!(f, "dynamodb-streams"),
            SourceType::AwsDynamodbStreams => write!(f, "aws-dynamodb-streams"),
        }
    }
}
