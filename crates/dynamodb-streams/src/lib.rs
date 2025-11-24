use crate::stream_state::RecordBatch;
use snafu::Snafu;

pub mod checkpoint;
pub mod client;
mod client_sdk;
mod stream;
mod stream_state;

pub type StreamResult = Result<RecordBatch, Error>;

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
