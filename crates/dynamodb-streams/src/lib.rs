use crate::stream_state::RecordBatch;
use snafu::Snafu;

pub mod client;
mod client_sdk;
pub mod stream;
pub mod types;
mod stream_state;

pub type StreamResult = Result<RecordBatch, Error>;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Control plane error: {source}"))]
    ControlPlane {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Data plane error: {source}"))]
    DataPlane {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Stream not found for table: {table_name}"))]
    StreamNotFound { table_name: String },

    #[snafu(display("Failed to initialize checkpoint"))]
    FailedToInitializeCheckpoint,

    #[snafu(display("Stream description not found: {stream_arn}"))]
    StreamDescriptionNotFound { stream_arn: String },
}
