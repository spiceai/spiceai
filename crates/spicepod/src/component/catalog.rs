/*
Copyright 2024 The Spice.ai OSS Authors

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

use super::{params::Params, WithDependsOn};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
pub struct Catalog {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub from: String,

    pub name: String,

    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub include: Vec<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params: Option<Params>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dataset_params: Option<Params>,

    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(rename = "dependsOn", default)]
    pub depends_on: Vec<String>,
}

impl Catalog {
    #[must_use]
    pub fn new(from: String, name: String) -> Self {
        Catalog {
            from,
            name,
            include: Vec::default(),
            params: None,
            dataset_params: None,
            depends_on: Vec::default(),
        }
    }
}

impl WithDependsOn<Catalog> for Catalog {
    fn depends_on(&self, depends_on: &[String]) -> Catalog {
        Catalog {
            from: self.from.clone(),
            name: self.name.clone(),
            include: self.include.clone(),
            params: self.params.clone(),
            dataset_params: self.dataset_params.clone(),
            depends_on: depends_on.to_vec(),
        }
    }
}
