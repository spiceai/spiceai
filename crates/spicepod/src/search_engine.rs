/*
Copyright 2026 The Spice.ai OSS Authors

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

use serde::{Deserialize, Serialize};

#[cfg(feature = "schemars")]
use schemars::JsonSchema;

use crate::{
    component::{Nameable, WithDependsOn},
    param::Params,
};

/// A named search backend that datasets can reference from `vectors.engine`,
/// `full_text_search.engine`, or column-level search configuration.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct SearchEngine {
    /// Component name referenced by datasets and columns.
    pub name: String,

    /// Backend/provider name, for example `elasticsearch`, `s3_vectors`, or `duckdb`.
    pub from: String,

    /// Search capabilities exposed by this engine.
    pub kind: SearchEngineKindSelection,

    /// Backend connection parameters shared by all capabilities.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params: Option<Params>,

    /// Default engine parameters applied before dataset or column overrides.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub defaults: Option<Params>,

    /// Capability-specific defaults applied after `defaults` and before dataset or column overrides.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capabilities: Option<SearchEngineCapabilities>,

    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(rename = "dependsOn", default)]
    pub depends_on: Vec<String>,
}

impl SearchEngine {
    #[must_use]
    pub fn supports(&self, kind: SearchEngineKind) -> bool {
        self.kind.contains(kind)
    }

    #[must_use]
    pub fn params_for(&self, kind: SearchEngineKind) -> Option<Params> {
        let mut merged = Params::default();
        merge_params(&mut merged, self.params.as_ref());
        merge_params(&mut merged, self.defaults.as_ref());
        merge_params(&mut merged, self.capability_params(kind));

        if merged.data.is_empty() {
            None
        } else {
            Some(merged)
        }
    }

    fn capability_params(&self, kind: SearchEngineKind) -> Option<&Params> {
        let capabilities = self.capabilities.as_ref()?;
        match kind {
            SearchEngineKind::Vector => capabilities.vector.as_ref(),
            SearchEngineKind::Text => capabilities.text.as_ref(),
            SearchEngineKind::Graph => capabilities.graph.as_ref(),
        }
    }
}

impl Nameable for SearchEngine {
    fn name(&self) -> &str {
        &self.name
    }
}

impl WithDependsOn<SearchEngine> for SearchEngine {
    fn depends_on(&self, depends_on: &[String]) -> SearchEngine {
        SearchEngine {
            depends_on: depends_on.to_vec(),
            ..self.clone()
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[serde(untagged)]
pub enum SearchEngineKindSelection {
    Single(SearchEngineKind),
    Multiple(Vec<SearchEngineKind>),
}

impl SearchEngineKindSelection {
    #[must_use]
    pub fn contains(&self, kind: SearchEngineKind) -> bool {
        match self {
            Self::Single(candidate) => *candidate == kind,
            Self::Multiple(kinds) => kinds.contains(&kind),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum SearchEngineKind {
    Vector,
    Text,
    Graph,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct SearchEngineCapabilities {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vector: Option<Params>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text: Option<Params>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub graph: Option<Params>,
}

pub fn merge_params(target: &mut Params, source: Option<&Params>) {
    if let Some(source) = source {
        target.data.extend(source.data.clone());
    }
}
