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
use aws_sdk_dynamodb::operation::describe_table::DescribeTableError;
use aws_sdk_dynamodbstreams::error::SdkError;
use aws_sdk_dynamodbstreams::operation::describe_stream::DescribeStreamError;
use aws_sdk_dynamodbstreams::operation::get_records::GetRecordsError;
use aws_sdk_dynamodbstreams::operation::get_shard_iterator::GetShardIteratorError;
use snafu::Snafu;

pub mod checkpoint;
pub mod client;
mod client_sdk;
mod stream;
mod stream_state;

pub use checkpoint::Checkpoint;
pub use client::Client;
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
