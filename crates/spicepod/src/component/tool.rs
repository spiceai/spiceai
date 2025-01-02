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

use std::collections::HashMap;

use super::{Nameable, WithDependsOn};
#[cfg(feature = "schemars")]
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
pub struct Tool {
    pub from: String,
    pub name: String,

    pub description: Option<String>,

    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub params: HashMap<String, String>,

    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(rename = "dependsOn", default)]
    pub depends_on: Vec<String>,
}

impl Nameable for Tool {
    fn name(&self) -> &str {
        &self.name
    }
}

impl WithDependsOn<Tool> for Tool {
    fn depends_on(&self, depends_on: &[String]) -> Tool {
        Tool {
            from: self.from.clone(),
            name: self.name.clone(),
            description: self.description.clone(),
            params: self.params.clone(),
            depends_on: depends_on.to_vec(),
        }
    }
}
