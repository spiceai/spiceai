#![allow(clippy::missing_errors_doc)]
#![allow(clippy::module_name_repetitions)]

use snafu::prelude::*;
use std::{collections::HashMap, fmt::Debug, fs::File, path::PathBuf};

use component::{dataset::Dataset, ComponentOrReference, WithDependsOn};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_yaml::{self, Value};

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

    if let Some(spicepod_file) = file_open_yaml(&path, "spicepod") {
        let mut spicepod_definition: SpicepodDefinition =
            serde_yaml::from_reader(spicepod_file).context(UnableToParseSpicepodSnafu)?;

        // resolve spicepod component references
        spicepod_definition.datasets =
            resolve_component_references(&path, &spicepod_definition.datasets, "dataset")?;

        return Ok(spicepod_definition);
    }

    SpicepodNotFoundSnafu { path: &path }.fail()
}

fn resolve_component_references<ComponentType>(
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
                let component_dir_path = base_path.join(&reference.from);

                if let Some(component_file) = file_open_yaml(&component_dir_path, component_name) {
                    let component_definition: ComponentType = serde_yaml::from_reader(
                        &component_file,
                    )
                    .context(UnableToParseSpicepodComponentSnafu {
                        path: &component_dir_path,
                    })?;

                    let component = ComponentOrReference::Component(
                        component_definition.depends_on(&reference.depends_on),
                    );

                    return Ok(component);
                }

                InvalidComponentReferenceSnafu {
                    path: &component_dir_path,
                }
                .fail()
            }
        })
        .collect()
}

fn file_open_yaml(base_path: impl Into<PathBuf>, basename: &str) -> Option<File> {
    let yaml_files = vec![format!("{basename}.yaml"), format!("{basename}.yml")];
    let base_path: PathBuf = base_path.into();

    for yaml_file in yaml_files {
        let spicepod_path = base_path.join(yaml_file);
        if let Ok(yaml_file) = File::open(&spicepod_path) {
            return Some(yaml_file);
        }
    }

    None
}
