use std::fmt::Debug;
use std::path::PathBuf;

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use snafu::prelude::*;

use crate::reader;
pub mod dataset;

pub trait WithDependsOn<T> {
    fn depends_on(&self, depends_on: &[String]) -> T;
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

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to convert the path into a string"))]
    UnableToConvertPath,
    #[snafu(display("Unable to parse spicepod component {}", path.display()))]
    UnableToParseSpicepodComponent {
        source: serde_yaml::Error,
        path: PathBuf,
    },
    #[snafu(display("The component referenced by {} does not exist", path.display()))]
    InvalidComponentReference { path: PathBuf },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub fn resolve_component_references<ComponentType>(
    fs: &impl reader::ReadableYaml,
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
                let component_base_path = base_path.join(&reference.from);
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
