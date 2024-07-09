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

use std::fmt::Debug;
use std::path::PathBuf;

#[cfg(feature = "schemars")]
use schemars::JsonSchema;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use snafu::prelude::*;

use crate::reader;
pub mod catalog;
pub mod dataset;
pub mod embeddings;
pub mod extension;
pub mod model;
pub mod params;
pub mod runtime;
pub mod secrets;
pub mod view;

pub trait WithDependsOn<T> {
    fn depends_on(&self, depends_on: &[String]) -> T;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
pub struct ComponentReference {
    pub r#ref: String,

    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(rename = "dependsOn", alias = "datasets", default)]
    pub depends_on: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[serde(untagged)]
pub enum ComponentOrReference<T> {
    Component(T),
    Reference(ComponentReference),
}

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to convert the path into a string"))]
    UnableToConvertPath,
    #[snafu(display("Unable to parse spicepod component {}: {source}", path.display()))]
    UnableToParseSpicepodComponent {
        source: serde_yaml::Error,
        path: PathBuf,
    },
    #[snafu(display("The component referenced by {} does not exist", path.display()))]
    InvalidComponentReference { path: PathBuf },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub fn resolve_component_references<ComponentType, T>(
    fs: &impl reader::ReadableYaml<T>,
    base_path: impl Into<PathBuf>,
    items: &[ComponentOrReference<ComponentType>],
    component_name: &str,
) -> Result<Vec<ComponentType>>
where
    ComponentType: Clone + DeserializeOwned + Debug + WithDependsOn<ComponentType>,
{
    let base_path: PathBuf = base_path.into();
    items
        .iter()
        .map(|item| match item {
            ComponentOrReference::Component(component) => Ok(component.clone()),
            ComponentOrReference::Reference(reference) => {
                let component_base_path = base_path.join(&reference.r#ref);
                let component_base_path_str = component_base_path
                    .to_str()
                    .ok_or(Error::UnableToConvertPath)?;

                let component_rdr = fs
                    .open_yaml(component_base_path_str, component_name)
                    .ok_or_else(|| Error::InvalidComponentReference {
                        path: component_base_path.clone(),
                    })?;

                let component_definition: ComponentType = serde_yaml::from_reader(component_rdr)
                    .context(UnableToParseSpicepodComponentSnafu {
                        path: component_base_path,
                    })?;

                let component = component_definition.depends_on(&reference.depends_on);

                Ok(component)
            }
        })
        .collect()
}
