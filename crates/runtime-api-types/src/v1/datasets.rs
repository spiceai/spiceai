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

//! Dataset API types for `/v1/datasets` endpoint.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::status::ComponentStatus;

/// Dataset information returned by the `/v1/datasets` endpoint.
#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "lowercase")]
pub struct DatasetInfo {
    /// The source where the dataset is located (e.g., `postgres:syncs`)
    pub from: String,

    /// The name of the dataset
    pub name: String,

    /// Whether replication is enabled for the dataset
    pub replication_enabled: bool,

    /// Whether acceleration is enabled for the dataset
    pub acceleration_enabled: bool,

    /// The current status of the dataset. Only included when `status=true` query parameter is specified.
    /// Possible values: `Initializing`, `Ready`, `Disabled`, `Error`, `Refreshing`, `ShuttingDown`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<ComponentStatus>,

    /// Custom properties for the dataset
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    pub properties: HashMap<String, serde_json::Value>,
}
