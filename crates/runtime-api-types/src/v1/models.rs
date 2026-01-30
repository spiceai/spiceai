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

//! Model API types for `/v1/models` endpoint.

use serde::{Deserialize, Serialize};

use super::status::ComponentStatus;

/// Response wrapper for the `/v1/models` endpoint (OpenAI-compatible format).
#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "snake_case")]
pub struct ModelListResponse {
    /// The type of the response (always `list`)
    pub object: String,

    /// The list of models
    pub data: Vec<ModelInfo>,
}

/// Model metadata fields that can be optionally requested.
#[derive(Clone, Debug, Serialize, Deserialize, Default)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ModelMetadata {
    /// Whether this model supports the Responses API
    pub supports_responses_api: bool,
}

/// Model information returned in the `/v1/models` response (OpenAI-compatible format).
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "snake_case")]
pub struct ModelInfo {
    /// The name/identifier of the model
    pub id: String,

    /// The type of the object (always `model`)
    pub object: String,

    /// The source from which the model was loaded (e.g., `openai`, `spiceai`)
    pub owned_by: String,

    /// The datasets associated with this model, if any
    #[serde(skip_serializing_if = "Option::is_none")]
    pub datasets: Option<Vec<String>>,

    /// The status of the model (e.g., `Ready`, `Initializing`, `Error`)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<ComponentStatus>,

    /// Optional metadata fields, included when requested via query parameters
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<ModelMetadata>,
}
