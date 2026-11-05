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

//! The models `GET /v1beta/models` lists (<https://ai.google.dev/api/models>).

use serde::{Deserialize, Serialize};

/// A model.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Model {
    /// `models/{id}`.
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_model_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_token_limit: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_token_limit: Option<u32>,
    /// The API methods the model serves, such as `generateContent` and `countTokens`.
    #[serde(default)]
    pub supported_generation_methods: Vec<String>,
}

impl Model {
    /// The model's ID: its `name` without the `models/` prefix.
    #[must_use]
    pub fn id(&self) -> &str {
        self.name.strip_prefix("models/").unwrap_or(&self.name)
    }
}

/// A page of the models `GET /v1beta/models` lists.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ListModelsResponse {
    pub models: Vec<Model>,
    /// The token of the next page; unset on the last.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_page_token: Option<String>,
}
