#![allow(clippy::missing_errors_doc)]
#![allow(clippy::module_name_repetitions)]

use snafu::prelude::*;
use std::fmt::Debug;

use component::dataset::Dataset;
use spec::SpicepodDefinition;

pub mod component;
pub mod reader;
mod spec;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to parse spicepod.yaml"))]
    UnableToParseSpicepod { source: serde_yaml::Error },
    #[snafu(display("Unable to resolve spicepod components {}", path))]
    UnableToResolveSpicepodComponents {
        source: component::Error,
        path: String,
    },
    #[snafu(display("spicepod.yaml not found in {}", path))]
    SpicepodNotFound { path: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub struct Spicepod {
    pub name: String,

    pub datasets: Vec<Dataset>,

    pub dependencies: Vec<String>,
}

impl Spicepod {
    pub fn load(path: &str) -> Result<Self> {
        Self::load_from(&reader::StdFileSystem, path)
    }

    pub fn load_from(fs: &impl reader::ReadableYaml, path: &str) -> Result<Spicepod> {
        let spicepod_rdr =
            fs.open_yaml(path, "spicepod")
                .ok_or_else(|| Error::SpicepodNotFound {
                    path: path.to_string(),
                })?;

        let spicepod_definition: SpicepodDefinition =
            serde_yaml::from_reader(spicepod_rdr).context(UnableToParseSpicepodSnafu)?;

        let resolved_datasets = component::resolve_component_references(
            fs,
            path,
            &spicepod_definition.datasets,
            "dataset",
        )
        .context(UnableToResolveSpicepodComponentsSnafu { path })?;

        Ok(from_definition(spicepod_definition, resolved_datasets))
    }
}

#[must_use]
fn from_definition(spicepod_definition: SpicepodDefinition, datasets: Vec<Dataset>) -> Spicepod {
    Spicepod {
        name: spicepod_definition.name,
        datasets,
        dependencies: spicepod_definition.dependencies,
    }
}
