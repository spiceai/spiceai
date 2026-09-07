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

//! Query API types for the `/v1/queries` endpoints.

use serde::Deserialize;

/// Request body for submitting a new query.
#[derive(Debug, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct SubmitQueryRequest {
    /// The SQL statement to execute.
    pub sql: String,
    /// Optional query parameters (bind variables).
    #[serde(default)]
    pub parameters: Option<serde_json::Value>,
    /// Optional timeout for async jobs.
    /// Jobs running for longer than this will automatically timeout and fail.
    #[serde(default)]
    pub timeout_seconds: Option<u64>,
    /// Optional maximum size of results for async jobs.
    /// Jobs with results larger than this will be failed with an error for exceeding the maximum size.
    #[serde(default)]
    pub maximum_size: Option<u64>,
}
