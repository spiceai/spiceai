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
//!
//! ## Configuration
//!
//! Mem0 is configured as a tool in the `spicepod.yaml`:
//!
//! ```yaml
//! tools:
//!   - name: memory
//!     from: mem0:memory
//!     params:
//!       mem0_api_key: ${secrets:MEM0_API_KEY}
//!       mem0_user_id: default-user
//! ```
//!
//! ## Parameters
//!
//! - `mem0_api_key`: API key for mem0.ai (required)
//! - `mem0_user_id`: User identifier for memory scoping (optional, defaults to "default-user")
//! - `mem0_agent_id`: Agent identifier for memory scoping (optional)
//! - `mem0_app_id`: Application identifier for memory scoping (optional)
//! - `mem0_run_id`: Run identifier for memory scoping (optional)
//! - `mem0_org_id`: Organization identifier (optional)
//! - `mem0_project_id`: Project identifier (optional)
//! - `mem0_base_url`: Custom API base URL (optional)
//! - `mem0_graph_memory`: Enable graph memory extraction ("enabled" to enable, optional)

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

    #[snafu(display(
        "Mem0 API rate limit exceeded after {retries} retries. Consider reducing request frequency. See: https://docs.mem0.ai/platform/quickstart"
    ))]
    RateLimitExceeded { retries: usize },

    #[snafu(display(
        "All {max_retries} retry attempts failed for mem0 API request. Check network connectivity and mem0 service availability."
    ))]
    AllRetriesFailed { max_retries: usize },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
