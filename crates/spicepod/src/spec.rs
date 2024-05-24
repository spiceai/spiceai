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

use serde::{Deserialize, Serialize};
use serde_yaml::{self, Value};
use std::fmt::{self, Display, Formatter};
use std::{collections::HashMap, fmt::Debug};

use crate::component::runtime::Runtime;
use crate::component::secrets::Secrets;
use crate::component::{dataset::Dataset, llms::Llm, model::Model, ComponentOrReference};

#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SpicepodVersion {
    V1Beta1,
}

impl Display for SpicepodVersion {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SpicepodDefinition {
    pub name: String,

    pub version: SpicepodVersion,

    pub kind: SpicepodKind,

    /// Optional runtime configuration
    #[serde(default)]
    pub runtime: Runtime,

    /// Optional spicepod secrets configuration
    /// Default value is `store: file`
    #[serde(default)]
    pub secrets: Secrets,

    #[serde(skip_serializing_if = "HashMap::is_empty")]
    #[serde(default)]
    pub metadata: HashMap<String, Value>,

    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(default)]
    pub datasets: Vec<ComponentOrReference<Dataset>>,

    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(default)]
    pub models: Vec<ComponentOrReference<Model>>,

    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(default)]
    pub dependencies: Vec<String>,

    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(default)]
    pub llms: Vec<ComponentOrReference<Llm>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum SpicepodKind {
    Spicepod,
}
