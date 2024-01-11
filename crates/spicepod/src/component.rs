use serde::{Deserialize, Serialize};

pub mod dataset;

pub trait WithDependsOn<T> {
    fn new(&self, depends_on: Vec<String>) -> T;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComponentReference {
    pub from: String,

    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(rename = "dependsOn", default)]
    pub depends_on: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ComponentOrReference<T> {
    Component(T),
    Reference(ComponentReference),
}