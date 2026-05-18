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

//! Error types for the Spice Cloud API client.

use snafu::Snafu;

/// Result type for Spice Cloud API operations.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Errors that can occur when communicating with the Spice Cloud API.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    /// HTTP transport error.
    #[snafu(display("HTTP request failed: {source}"))]
    HttpRequest { source: reqwest::Error },

    /// The server returned 401 Unauthorized.
    #[snafu(display("Unauthorized: {message}"))]
    Unauthorized { message: String },

    /// The server returned 403 Forbidden.
    #[snafu(display("Forbidden: {message}"))]
    Forbidden { message: String },

    /// The user explicitly denied the browser-based device authorization flow.
    #[snafu(display("Device authorization was denied"))]
    AuthorizationDenied,

    /// The server returned a successful response whose body was invalid for the
    /// requested operation.
    #[snafu(display("Invalid response: {message}"))]
    InvalidResponse { message: String },

    /// The server returned 404 Not Found.
    #[snafu(display("Not found: {message}"))]
    NotFound { message: String },

    /// The server returned 409 Conflict.
    #[snafu(display("Conflict: {message}"))]
    Conflict { message: String },

    /// The server returned an unexpected status code or an unparseable body.
    #[snafu(display("API error ({status}): {message}"))]
    Api { status: u16, message: String },

    /// Failed to parse a JSON response body.
    #[snafu(display("Failed to parse response: {source}"))]
    JsonParse { source: serde_json::Error },
}
