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

use super::{Nameable, WithDependsOn};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
pub struct View {
    pub name: String,

    /// Inline SQL that describes a view.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sql: Option<String>,

    /// Reference to a SQL file that describes a view.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sql_ref: Option<String>,

    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(rename = "dependsOn", default)]
    pub depends_on: Vec<String>,
}

impl Nameable for View {
    fn name(&self) -> &str {
        &self.name
    }
}

impl View {
    #[must_use]
    pub fn new(name: String) -> Self {
        Self {
            name,
            sql: None,
            sql_ref: None,
            depends_on: Vec::default(),
        }
    }
}

impl WithDependsOn<View> for View {
    fn depends_on(&self, depends_on: &[String]) -> View {
        Self {
            name: self.name.clone(),
            sql: self.sql.clone(),
            sql_ref: self.sql_ref.clone(),
            depends_on: depends_on.to_vec(),
        }
    }
}
