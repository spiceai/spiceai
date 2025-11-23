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

use snafu::Snafu;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Invalid argument: {message}"))]
    InvalidArgument { message: String },

    #[snafu(display("Authentication failed: {message}"))]
    AuthenticationFailed { message: String },

    #[snafu(display("Connection failed: {message}"))]
    ConnectionFailed { message: String },

    #[snafu(display("Query execution failed: {message}"))]
    QueryFailed { message: String },

    #[snafu(display("Arrow error: {source}"))]
    ArrowError { source: arrow::error::ArrowError },

    #[snafu(display("Connection not initialized"))]
    NotInitialized,

    #[snafu(display("Invalid state: {message}"))]
    InvalidState { message: String },

    #[snafu(display("IO error: {source}"))]
    IoError { source: std::io::Error },

    #[snafu(display("Flight error: {message}"))]
    FlightError { message: String },
}

impl From<arrow::error::ArrowError> for Error {
    fn from(source: arrow::error::ArrowError) -> Self {
        Self::ArrowError { source }
    }
}

impl From<std::io::Error> for Error {
    fn from(source: std::io::Error) -> Self {
        Self::IoError { source }
    }
}
