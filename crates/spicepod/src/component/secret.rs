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

#[cfg(feature = "schemars")]
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::{params::Params, Nameable};

/// The secrets configuration for a Spicepod.
///
/// Example:
/// ```yaml
/// secrets:
///   - from: env
///     name: env
///   - from: kubernetes:my_secret_name
///     name: k8s
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
pub struct Secret {
    pub from: String,

    pub name: String,

    pub description: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params: Option<Params>,
}

impl Nameable for Secret {
    fn name(&self) -> &str {
        &self.name
    }
}
