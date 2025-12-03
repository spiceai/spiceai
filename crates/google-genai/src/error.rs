/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("HTTP request failed: {source}"))]
    HttpError { source: reqwest::Error },

    #[snafu(display("JSON serialization/deserialization failed: {source}"))]
    JsonError { source: serde_json::Error },

    #[snafu(display("API error: {message}"))]
    ApiError { message: String, code: Option<i32> },

    #[snafu(display("Invalid API key"))]
    InvalidApiKey,

    #[snafu(display("Model not found: {model}"))]
    ModelNotFound { model: String },

    #[snafu(display("Invalid request: {message}"))]
    InvalidRequest { message: String },

    #[snafu(display("Rate limit exceeded"))]
    RateLimitExceeded,

    #[snafu(display("Streaming error: {message}"))]
    StreamError { message: String },
}

pub type Result<T> = std::result::Result<T, Error>;
