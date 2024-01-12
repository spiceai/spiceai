#![allow(clippy::missing_errors_doc)]
#![allow(clippy::module_name_repetitions)]

use snafu::prelude::*;
use std::{
    collections::HashMap,
    fmt::Debug,
    io,
    path::{Path, PathBuf},
};

use component::{dataset::Dataset, ComponentOrReference, WithDependsOn};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_yaml::{self, Value};

pub mod component;
pub mod reader;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to read directory contents from {}", path.display()))]
    UnableToReadDirectoryContents {
        source: std::io::Error,
        path: PathBuf,
    },
    #[snafu(display("Unable to open file {}", path.display()))]
    UnableToOpenFile {
        source: std::io::Error,
        path: PathBuf,
    },
    #[snafu(display("Unable to convert the path into a string"))]
    UnableToConvertPath,
    #[snafu(display("Unable to parse spicepod.yaml"))]
    UnableToParseSpicepod { source: serde_yaml::Error },
    #[snafu(display("Unable to parse spicepod component {}", path.display()))]
    UnableToParseSpicepodComponent {
        source: serde_yaml::Error,
        path: PathBuf,
    },
    #[snafu(display("spicepod.yaml not found in {}", path.display()))]
    SpicepodNotFound { path: PathBuf },
    #[snafu(display("The component referenced by {} does not exist", path.display()))]
    InvalidComponentReference { path: PathBuf },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SpicepodVersion {
    V1Beta1,
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

pub fn load_from(fs: &impl reader::ReadablePath, path: &str) -> Result<SpicepodDefinition> {
    let spicepod_rdr = open_yaml(fs, path, "spicepod")
        .ok_or_else(|| Error::SpicepodNotFound { path: path.into() })?;

    let mut spicepod_definition: SpicepodDefinition =
        serde_yaml::from_reader(spicepod_rdr).context(UnableToParseSpicepodSnafu)?;

    // resolve spicepod component references
    spicepod_definition.datasets =
        resolve_component_references(fs, path, &spicepod_definition.datasets, "dataset")?;

    Ok(spicepod_definition)
}

pub fn load(path: &str) -> Result<SpicepodDefinition> {
    load_from(&reader::StdFileSystem, path)
}

fn resolve_component_references<ComponentType>(
    fs: &impl reader::ReadablePath,
    base_path: impl Into<PathBuf>,
    items: &[ComponentOrReference<ComponentType>],
    component_name: &str,
) -> Result<Vec<ComponentOrReference<ComponentType>>>
where
    ComponentType: Clone + DeserializeOwned + Debug + WithDependsOn<ComponentType>,
{
    let base_path: PathBuf = base_path.into();
    items
        .iter()
        .map(|item| match item {
            ComponentOrReference::Component(component) => {
                Ok(ComponentOrReference::Component(component.clone()))
            }
            ComponentOrReference::Reference(reference) => {
                let component_base_path = base_path.join(&reference.from);
                let component_base_path_str = component_base_path
                    .to_str()
                    .ok_or(Error::UnableToConvertPath)?;

                let component_rdr = open_yaml(fs, component_base_path_str, component_name)
                    .ok_or_else(|| Error::InvalidComponentReference {
                        path: component_base_path.clone(),
                    })?;

                let component_definition: ComponentType = serde_yaml::from_reader(component_rdr)
                    .context(UnableToParseSpicepodComponentSnafu {
                        path: component_base_path,
                    })?;

                let component = ComponentOrReference::Component(
                    component_definition.depends_on(&reference.depends_on),
                );

                Ok(component)
            }
        })
        .collect()
}

fn open_yaml(
    fs: &impl reader::ReadablePath,
    base_path: &str,
    basename: &str,
) -> Option<Box<dyn io::Read>> {
    let yaml_files = vec![format!("{basename}.yaml"), format!("{basename}.yml")];
    let base_path = Path::new(base_path);

    for yaml_file in yaml_files {
        let spicepod_path = base_path.join(yaml_file);
        let spicepod_path = spicepod_path.to_str();
        if spicepod_path.is_none() {
            continue;
        }
        let spicepod_path = spicepod_path?;
        if let Ok(yaml_file) = fs.open(spicepod_path) {
            return Some(yaml_file);
        }
    }

    None
}
