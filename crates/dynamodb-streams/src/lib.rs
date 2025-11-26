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
use crate::stream_state::DynamoDBStreamBatch;
use snafu::Snafu;

pub mod checkpoint;
pub mod client;
mod client_sdk;
mod stream;
mod stream_state;

pub type StreamResult = Result<DynamoDBStreamBatch, Error>;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("AWS SDK error: {source}"))]
    SDKError {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Stream not found for table: {table_name}"))]
    StreamNotFound { table_name: String },

    #[snafu(display("Stream description not found: {stream_arn}"))]
    StreamDescriptionNotFound { stream_arn: String },

    #[snafu(display(
        "Failed to initialize checkpoint due to empty starting_sequence_number in one of the open shards"
    ))]
    FailedToInitializeCheckpoint,
}
