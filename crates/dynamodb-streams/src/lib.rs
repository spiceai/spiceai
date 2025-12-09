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
use aws_sdk_dynamodb::error::ProvideErrorMetadata;
use aws_sdk_dynamodb::operation::describe_table::DescribeTableError;
use aws_sdk_dynamodbstreams::error::SdkError;
use aws_sdk_dynamodbstreams::operation::describe_stream::DescribeStreamError;
use aws_sdk_dynamodbstreams::operation::get_records::GetRecordsError;
use aws_sdk_dynamodbstreams::operation::get_shard_iterator::GetShardIteratorError;
use snafu::Snafu;

pub mod checkpoint;
pub mod client;
mod client_sdk;
mod metrics;
mod stream;
mod stream_state;

pub use checkpoint::Checkpoint;
pub use client::Client;
pub use metrics::{Metrics, MetricsCollector};
pub use stream::{DynamoDBStreamBatch, DynamodbStream};

pub type StreamResult = Result<DynamoDBStreamBatch, Error>;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
pub enum Error {
    // Permanent
    #[snafu(display("Table not found"))]
    TableNotFound,

    #[snafu(display("Stream not found"))]
    StreamNotFound,

    #[snafu(display("Stream description not found: {stream_arn}"))]
    StreamDescriptionNotFound { stream_arn: String },

    #[snafu(display("Shard iterator not found: {shard_id}"))]
    ShardIteratorNotFound { shard_id: String },

    #[snafu(display(
        "Failed to initialize checkpoint due to empty starting_sequence_number in one of the open shards"
    ))]
    FailedToInitializeCheckpoint,

    #[snafu(display("Stream is beyond retention period (more than 24 hours"))]
    StreamBeyondRetention,

    #[snafu(display("Shard not found."))]
    ShardNotFound,

    #[snafu(display("Missing starting_sequence_number"))]
    MissingStaringSequenceNumber,

    #[snafu(display("Inconsistent shard id: {shard_id}"))]
    UnexpectedShardId { shard_id: String },

    #[snafu(display("AWS SDK error: {source}"))]
    SdkError {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    // Retriable - network/transport
    #[snafu(display("Network timeout. Try again later"))]
    Timeout,

    #[snafu(display("Network connection failure. Try again later"))]
    ConnectionFailure,

    #[snafu(display("Request has been throttled. Try again later"))]
    Throttled,

    // Retriable - with special handling.
    // Should never surface to the user.
    #[snafu(display("Iterator expired for shard"))]
    IteratorExpired,
}

/// Checks if an error code indicates a retriable throttling condition.
///
/// AWS SDK may return throttling errors as "unhandled" errors with specific error codes.
/// This function checks for common throttling-related error codes.
fn is_throttling_error_code(code: Option<&str>) -> bool {
    matches!(
        code,
        Some(
            "ThrottlingException"
                | "Throttling"
                | "ProvisionedThroughputExceededException"
                | "RequestLimitExceeded"
        )
    )
}

impl Error {
    #[must_use]
    pub fn is_retriable(&self) -> bool {
        matches!(
            self,
            Error::Timeout | Error::ConnectionFailure | Error::Throttled
        )
    }

    pub fn from_describe_table(err: SdkError<DescribeTableError>) -> Self {
        match err {
            SdkError::TimeoutError(_) => Error::Timeout,
            SdkError::DispatchFailure(_) => Error::ConnectionFailure,

            SdkError::ServiceError(e) => match e.err() {
                DescribeTableError::ResourceNotFoundException(_) => Error::TableNotFound,
                other if is_throttling_error_code(other.code()) => Error::Throttled,
                _ => Error::SdkError {
                    source: Box::new(e.into_err()),
                },
            },

            _ => Error::SdkError {
                source: Box::new(err),
            },
        }
    }

    pub fn from_describe_stream(err: SdkError<DescribeStreamError>) -> Self {
        match err {
            SdkError::TimeoutError(_) => Error::Timeout,
            SdkError::DispatchFailure(_) => Error::ConnectionFailure,

            SdkError::ServiceError(e) => match e.err() {
                DescribeStreamError::ResourceNotFoundException(_) => Error::StreamNotFound,
                other if is_throttling_error_code(other.code()) => Error::Throttled,
                _ => Error::SdkError {
                    source: Box::new(e.into_err()),
                },
            },

            _ => Error::SdkError {
                source: Box::new(err),
            },
        }
    }

    pub fn from_get_records(err: SdkError<GetRecordsError>) -> Self {
        match err {
            SdkError::TimeoutError(_) => Error::Timeout,
            SdkError::DispatchFailure(_) => Error::ConnectionFailure,

            SdkError::ServiceError(e) => match e.err() {
                GetRecordsError::ExpiredIteratorException(_) => Error::IteratorExpired,
                GetRecordsError::LimitExceededException(_) => Error::Throttled,
                GetRecordsError::TrimmedDataAccessException(_) => Error::StreamBeyondRetention,
                other if is_throttling_error_code(other.code()) => Error::Throttled,
                _ => Error::SdkError {
                    source: Box::new(e.into_err()),
                },
            },

            _ => Error::SdkError {
                source: Box::new(err),
            },
        }
    }

    pub fn from_get_shard_iterator(err: SdkError<GetShardIteratorError>) -> Self {
        match err {
            SdkError::TimeoutError(_) => Error::Timeout,
            SdkError::DispatchFailure(_) => Error::ConnectionFailure,

            SdkError::ServiceError(e) => match e.err() {
                GetShardIteratorError::TrimmedDataAccessException(_) => {
                    Error::StreamBeyondRetention
                }
                GetShardIteratorError::ResourceNotFoundException(_) => Error::ShardNotFound,
                other if is_throttling_error_code(other.code()) => Error::Throttled,
                _ => Error::SdkError {
                    source: Box::new(e.into_err()),
                },
            },

            _ => Error::SdkError {
                source: Box::new(err),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod error_is_retriable {
        use super::*;

        #[test]
        fn test_timeout_is_retriable() {
            let err = Error::Timeout;
            assert!(err.is_retriable());
        }

        #[test]
        fn test_connection_failure_is_retriable() {
            let err = Error::ConnectionFailure;
            assert!(err.is_retriable());
        }

        #[test]
        fn test_throttled_is_retriable() {
            let err = Error::Throttled;
            assert!(err.is_retriable());
        }

        #[test]
        fn test_table_not_found_is_not_retriable() {
            let err = Error::TableNotFound;
            assert!(!err.is_retriable());
        }

        #[test]
        fn test_stream_not_found_is_not_retriable() {
            let err = Error::StreamNotFound;
            assert!(!err.is_retriable());
        }

        #[test]
        fn test_stream_description_not_found_is_not_retriable() {
            let err = Error::StreamDescriptionNotFound {
                stream_arn: "arn:aws:dynamodb:region:account:table/name/stream/time".to_string(),
            };
            assert!(!err.is_retriable());
        }

        #[test]
        fn test_shard_iterator_not_found_is_not_retriable() {
            let err = Error::ShardIteratorNotFound {
                shard_id: "shard-123".to_string(),
            };
            assert!(!err.is_retriable());
        }

        #[test]
        fn test_failed_to_initialize_checkpoint_is_not_retriable() {
            let err = Error::FailedToInitializeCheckpoint;
            assert!(!err.is_retriable());
        }

        #[test]
        fn test_stream_beyond_retention_is_not_retriable() {
            let err = Error::StreamBeyondRetention;
            assert!(!err.is_retriable());
        }

        #[test]
        fn test_shard_not_found_is_not_retriable() {
            let err = Error::ShardNotFound;
            assert!(!err.is_retriable());
        }

        #[test]
        fn test_missing_starting_sequence_number_is_not_retriable() {
            let err = Error::MissingStaringSequenceNumber;
            assert!(!err.is_retriable());
        }

        #[test]
        fn test_unexpected_shard_id_is_not_retriable() {
            let err = Error::UnexpectedShardId {
                shard_id: "shard-xyz".to_string(),
            };
            assert!(!err.is_retriable());
        }

        #[test]
        fn test_iterator_expired_is_not_retriable() {
            // Iterator expired is handled specially (reinitialization)
            // but should NOT be in the retriable category
            let err = Error::IteratorExpired;
            assert!(!err.is_retriable());
        }
    }

    mod error_display {
        use super::*;

        #[test]
        fn test_table_not_found_display() {
            let err = Error::TableNotFound;
            assert_eq!(format!("{err}"), "Table not found");
        }

        #[test]
        fn test_stream_not_found_display() {
            let err = Error::StreamNotFound;
            assert_eq!(format!("{err}"), "Stream not found");
        }

        #[test]
        fn test_stream_description_not_found_display() {
            let err = Error::StreamDescriptionNotFound {
                stream_arn: "arn:aws:dynamodb:us-east-1:123456789:table/test/stream/2024"
                    .to_string(),
            };
            let msg = format!("{err}");
            assert!(msg.contains("Stream description not found"));
            assert!(msg.contains("arn:aws:dynamodb:us-east-1:123456789:table/test/stream/2024"));
        }

        #[test]
        fn test_shard_iterator_not_found_display() {
            let err = Error::ShardIteratorNotFound {
                shard_id: "shardId-000000000001".to_string(),
            };
            let msg = format!("{err}");
            assert!(msg.contains("Shard iterator not found"));
            assert!(msg.contains("shardId-000000000001"));
        }

        #[test]
        fn test_unexpected_shard_id_display() {
            let err = Error::UnexpectedShardId {
                shard_id: "unknown-shard".to_string(),
            };
            let msg = format!("{err}");
            assert!(msg.contains("Inconsistent shard id"));
            assert!(msg.contains("unknown-shard"));
        }

        #[test]
        fn test_timeout_display() {
            let err = Error::Timeout;
            assert_eq!(format!("{err}"), "Network timeout. Try again later");
        }

        #[test]
        fn test_connection_failure_display() {
            let err = Error::ConnectionFailure;
            assert_eq!(
                format!("{err}"),
                "Network connection failure. Try again later"
            );
        }

        #[test]
        fn test_throttled_display() {
            let err = Error::Throttled;
            assert_eq!(
                format!("{err}"),
                "Request has been throttled. Try again later"
            );
        }

        #[test]
        fn test_iterator_expired_display() {
            let err = Error::IteratorExpired;
            assert_eq!(format!("{err}"), "Iterator expired for shard");
        }

        #[test]
        fn test_stream_beyond_retention_display() {
            let err = Error::StreamBeyondRetention;
            let msg = format!("{err}");
            assert!(msg.contains("beyond retention period"));
            assert!(msg.contains("24 hours"));
        }
    }
}
