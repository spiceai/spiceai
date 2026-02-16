/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! Error types for the Spice CLI.

use snafu::Snafu;
use std::path::PathBuf;

/// Result type alias for the Spice CLI.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Error types for the Spice CLI.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    /// Runtime is not installed
    #[snafu(display("The Spice runtime is not installed. Run 'spice install' to install it."))]
    RuntimeNotInstalled,

    /// Runtime is not running
    #[snafu(display("The Spice runtime is unavailable at {endpoint}. Is it running?"))]
    RuntimeUnavailable { endpoint: String },

    /// Failed to connect to the runtime
    #[snafu(display("Failed to connect to runtime at {endpoint}: {source}"))]
    ConnectionFailed {
        endpoint: String,
        source: reqwest::Error,
    },

    /// HTTP request failed
    #[snafu(display("HTTP request failed: {source}"))]
    HttpRequestFailed { source: reqwest::Error },

    /// Invalid HTTP response
    #[snafu(display("Invalid HTTP response: {message}"))]
    InvalidResponse { message: String },

    /// Failed to read/write configuration
    #[snafu(display("Failed to {operation} configuration at {}: {source}", path.display()))]
    ConfigIo {
        operation: &'static str,
        path: PathBuf,
        source: std::io::Error,
    },

    /// Failed to parse configuration
    #[snafu(display("Failed to parse configuration: {message}"))]
    ConfigParse { message: String },

    /// Failed to create directory
    #[snafu(display("Failed to create directory {}: {source}", path.display()))]
    CreateDirectory {
        path: PathBuf,
        source: std::io::Error,
    },

    /// Failed to execute runtime command
    #[snafu(display("Failed to execute runtime: {source}"))]
    RuntimeExecution { source: std::io::Error },

    /// Failed to get runtime version
    #[snafu(display("Failed to get runtime version: {message}"))]
    RuntimeVersion { message: String },

    /// Environment variable error
    #[snafu(display("Environment variable error: {message}"))]
    Environment { message: String },

    /// Invalid argument
    #[snafu(display("Invalid argument: {message}"))]
    InvalidArgument { message: String },

    /// Home directory not found
    #[snafu(display(
        "Could not determine home directory. Set HOME (Unix) or USERPROFILE (Windows) environment variable."
    ))]
    HomeDirectoryNotFound,

    /// REPL error
    #[snafu(display("SQL REPL error: {message}"))]
    Repl { message: String },

    /// Failed to get child process ID
    #[snafu(display("Failed to get child process ID"))]
    ChildProcessId,

    /// Failed to register signal handler
    #[snafu(display("Failed to register signal handler: {source}"))]
    SignalHandler { source: std::io::Error },

    /// Model not found
    #[snafu(display("Model '{model}' not found. Available models: {available}"))]
    ModelNotFound { model: String, available: String },

    /// No models configured
    #[snafu(display("No models found. Please configure a model in your Spicepod."))]
    NoModelsConfigured,
}
