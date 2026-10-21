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

use crate::component::function::Signature;
use crate::metric::Metrics;

use super::{Nameable, WithDependsOn};
#[cfg(feature = "schemars")]
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct Tool {
    pub from: String,
    pub name: String,

    pub description: Option<String>,

    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub params: HashMap<String, String>,

    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub env: HashMap<String, String>,

    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(rename = "dependsOn", default)]
    pub depends_on: Vec<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metrics: Option<Metrics>,

    /// Whether this tool is also callable as a SQL UDF. Defaults to
    /// `false`; set to `true` to also register a `DataFusion` `ScalarUDF`
    /// that invokes this tool per-row.
    ///
    /// Requires [`Self::signature`] to be set with typed Arrow arg /
    /// return types — a tool's free-form JSON Schema does not uniquely
    /// map to a SQL signature, so the author must pin types explicitly.
    /// Also requires `runtime.functions.enabled: true`.
    #[serde(default)]
    pub as_sql: bool,

    /// Typed signature for SQL invocation when [`Self::as_sql`] is true.
    /// Ignored otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signature: Option<Signature>,
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
            env: self.env.clone(),
            depends_on: depends_on.to_vec(),
            metrics: self.metrics.clone(),
            as_sql: self.as_sql,
            signature: self.signature.clone(),
        }
    }
}
