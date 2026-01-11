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

//! Mem0 integration for LLM memory management.
//!
//! This module provides tools for storing, searching, and retrieving memories
//! using the Mem0 platform API (<https://mem0.ai>).

pub mod catalog;
pub mod client;
pub mod factory;
pub mod tools;

use snafu::Snafu;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Invalid mem0 directive 'from: mem0:{id}'"))]
    InvalidMem0Directive { id: String },

    #[snafu(display("Missing required parameter: {param}"))]
    MissingRequiredParameter { param: String },

    #[snafu(display("Failed to make request to mem0 API: {source}"))]
    RequestFailed { source: reqwest::Error },

    #[snafu(display("Failed to parse mem0 API response: {source}"))]
    ResponseParseFailed { source: reqwest::Error },

    #[snafu(display("Mem0 API error: {message}"))]
    ApiError { message: String },

    #[snafu(display("Failed to build HTTP client: {message}"))]
    ClientBuildFailed { message: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
