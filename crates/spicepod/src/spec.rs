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
use serde_json::Value;
use std::fmt::{self, Display, Formatter};
use std::{collections::HashMap, fmt::Debug};

use crate::component::catalog::Catalog;
use crate::component::embeddings::Embeddings;
use crate::component::eval::Eval;
use crate::component::is_default;
use crate::component::runtime::Runtime;
use crate::component::secret::Secret;
use crate::component::tool::Tool;
use crate::component::{
    dataset::Dataset, extension::Extension, model::Model, view::View, ComponentOrReference,
};

#[derive(Clone, PartialEq, Debug, Serialize, Deserialize, Default)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[serde(rename_all = "lowercase")]
pub enum SpicepodVersion {
    #[default]
    V1Beta1,
    V1,
}

impl Display for SpicepodVersion {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

/// # Spicepod Definition
///
/// A Spicepod definition is a YAML file that describes a Spicepod.
#[derive(Debug, Serialize, Deserialize, Default)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
pub struct SpicepodDefinition {
    /// The name of the Spicepod
    pub name: String,

    /// The version of the Spicepod
    pub version: SpicepodVersion,

    /// The kind of Spicepod
    pub kind: SpicepodKind,

    /// Optional runtime configuration
    #[serde(default, skip_serializing_if = "is_default")]
    pub runtime: Runtime,

    /// Optional extensions configuration
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    #[serde(default)]
    pub extensions: HashMap<String, Extension>,

    /// Optional spicepod secrets configuration
    /// Default value is:
    /// ```yaml
    /// secrets:
    ///   - from: env
    ///     name: env
    /// ```
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub secrets: Vec<Secret>,

    #[serde(skip_serializing_if = "HashMap::is_empty")]
    #[serde(default)]
    pub metadata: HashMap<String, Value>,

    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(default)]
    pub catalogs: Vec<ComponentOrReference<Catalog>>,

    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(default)]
    pub datasets: Vec<ComponentOrReference<Dataset>>,

    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(default)]
    pub views: Vec<ComponentOrReference<View>>,

    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(default)]
    pub models: Vec<ComponentOrReference<Model>>,

    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(default)]
    pub embeddings: Vec<ComponentOrReference<Embeddings>>,

    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(default)]
    pub evals: Vec<ComponentOrReference<Eval>>,

    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(default)]
    pub tools: Vec<ComponentOrReference<Tool>>,

    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(default)]
    pub dependencies: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
pub enum SpicepodKind {
    #[default]
    Spicepod,
}
