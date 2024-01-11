use std::{
    fs::{File, ReadDir}, collections::HashMap,
    fmt::Debug, path::PathBuf,
};
use snafu::prelude::*;

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_yaml::{self, Value};
use component::{dataset::Dataset, ComponentOrReference, WithDependsOn};

pub mod component;

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
    #[snafu(display("Unable to parse spicepod.yaml"))]
    UnableToParseSpicepod {
        source: serde_yaml::Error,
    },
    #[snafu(display("Unable to parse spicepod component {}", path.display()))]
    UnableToParseSpicepodComponent {
        source: serde_yaml::Error,
        path: PathBuf,
    },
    #[snafu(display("spicepod.yaml not found in {}", path.display()))]
    SpicepodNotFound {
        path: PathBuf,
    },
    #[snafu(display("The component referenced by {} does not exist", path.display()))]
    InvalidComponentReference {
        path: PathBuf,
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SpicepodVersion {
    V1Beta1,
}

#[derive(Debug, Deserialize)]
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

pub fn load(path: impl Into<PathBuf>) -> Result<SpicepodDefinition> {
    let path = path.into();
    let dir: ReadDir = std::fs::read_dir(path).context(UnableToReadDirectoryContentsSnafu{ path })?;

    for entry in dir {
        let entry = entry.context(UnableToReadDirectoryContentsSnafu { path })?;
        let file_name = entry.file_name();
        let entry_path = entry.path();
        if entry_path.is_file() {
            if file_name == "spicepod.yaml" || file_name == "spicepod.yml" {
                let spicepod_definition_yaml = File::open(entry_path).context(UnableToOpenFileSnafu { path: entry_path })?;

                let mut spicepod_definition: SpicepodDefinition =
                    serde_yaml::from_reader(spicepod_definition_yaml).context(UnableToParseSpicepodSnafu)?;

                // expand spicepod components
                spicepod_definition.datasets =
                    expand_spicepod(&path, spicepod_definition.datasets, "dataset")?;

                return Ok(spicepod_definition);
            }
        }
    }

    SpicepodNotFoundSnafu { path }.fail()
}

pub fn expand_spicepod<ComponentType>(
    base_path: impl Into<PathBuf>,
    items: Vec<ComponentOrReference<ComponentType>>,
    manifest_name: &str,
) -> Result<Vec<ComponentOrReference<ComponentType>>>
where
    ComponentType: Clone + DeserializeOwned + Debug + WithDependsOn<ComponentType>,
{
    let base_path: PathBuf = base_path.into();
    let expanded: Result<Vec<ComponentOrReference<ComponentType>>> = items
        .iter()
        .map(|item| match item {
            ComponentOrReference::Component(component) => {
                Ok(ComponentOrReference::Component(component.clone()))
            }
            ComponentOrReference::Reference(reference) => {
                // Get base path from reference.from
                let reference_path = PathBuf::from(&reference.from);
                let reference_base_path = reference_path.parent().ok_or(Error::InvalidComponentReference { path: reference_path })?;
                let component_dir_path = base_path.join(reference_base_path);
                let dir: ReadDir = std::fs::read_dir(component_dir_path).context(UnableToReadDirectoryContentsSnafu{ path: component_dir_path })?;

                for entry in dir {
                    let entry = entry.context(UnableToReadDirectoryContentsSnafu { path: component_dir_path })?;
                    let file_name = entry.file_name();
                    let file_name = match file_name.to_str() {
                        Some(file_name) => file_name,
                        None => continue,
                    };

                    if file_name == manifest_name.to_string() + ".yaml"
                        || file_name == manifest_name.to_string() + ".yml"
                    {
                        let filepath = base_path.join(file_name);
                        let reference_definition = File::open(
                            filepath,
                        )
                        .context(UnableToOpenFileSnafu { path: filepath })?;
                        let component_definition: ComponentType =
                            serde_yaml::from_reader(reference_definition).context(UnableToParseSpicepodComponentSnafu { path: filepath })?;

                        let component = ComponentOrReference::Component(
                            component_definition.new(reference.depends_on.clone()),
                        );

                        return Ok(component);
                    }
                }

                InvalidComponentReferenceSnafu { path }.fail()
            }
        })
        .collect();

    expanded
}
