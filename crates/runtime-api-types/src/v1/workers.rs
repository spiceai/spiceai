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

//! Worker API types for `/v1/workers` endpoint.

use serde::{Deserialize, Serialize};

use super::status::ComponentStatus;

/// Response wrapper for the `/v1/workers` endpoint.
#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "snake_case")]
pub struct WorkerListResponse {
    /// The type of the response (always `list`)
    pub object: String,

    /// The list of workers
    pub data: Vec<WorkerInfo>,
}

/// Worker information returned in the `/v1/workers` response.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "snake_case")]
pub struct WorkerInfo {
    /// The name of the worker
    pub name: String,

    /// A description of what the worker does
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    /// Whether this worker can be used as an LLM model
    pub is_llm: bool,

    /// The status of the worker (e.g., `Ready`, `Initializing`, `Error`)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<ComponentStatus>,
}
