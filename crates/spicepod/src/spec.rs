use serde::{Deserialize, Serialize};
use serde_yaml::{self, Value};
use std::{collections::HashMap, fmt::Debug};

use crate::component::{dataset::Dataset, ComponentOrReference};

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SpicepodVersion {
    V1Beta1,
    V1Beta2,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SpicepodDefinition {
    pub name: String,

    pub version: SpicepodVersion,

    pub kind: SpicepodKind,

    #[serde(skip_serializing_if = "HashMap::is_empty")]
    #[serde(default)]
    pub metadata: HashMap<String, Value>,

    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(default)]
    pub datasets: Vec<ComponentOrReference<Dataset>>,

    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(default)]
    pub dependencies: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum SpicepodKind {
    Spicepod,
}
