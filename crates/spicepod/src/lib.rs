#![allow(clippy::missing_errors_doc)]
#![allow(clippy::module_name_repetitions)]

use snafu::prelude::*;
use std::{fmt::Debug, path::PathBuf};

use component::dataset::Dataset;
use component::model::Model;
use spec::SpicepodDefinition;

pub mod component;
pub mod reader;
mod spec;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to parse spicepod.yaml"))]
    UnableToParseSpicepod { source: serde_yaml::Error },
    #[snafu(display("Unable to resolve spicepod components {}", path.display()))]
    UnableToResolveSpicepodComponents {
        source: component::Error,
        path: PathBuf,
    },
    #[snafu(display("spicepod.yaml not found in {}", path.display()))]
    SpicepodNotFound { path: PathBuf },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub struct Spicepod {
    pub name: String,

    pub datasets: Vec<Dataset>,

    pub models: Vec<Model>,

    pub dependencies: Vec<String>,
}

impl Spicepod {
    pub fn load(path: impl Into<PathBuf>) -> Result<Self> {
        Self::load_from(&reader::StdFileSystem, path)
    }

    pub fn load_from<T>(
        fs: &impl reader::ReadableYaml<T>,
        path: impl Into<PathBuf>,
    ) -> Result<Spicepod> {
        let path = path.into();
        let path_str = path.to_string_lossy().to_string();

        let spicepod_rdr = fs
            .open_yaml(&path_str, "spicepod")
            .ok_or_else(|| Error::SpicepodNotFound { path: path.clone() })?;

        let spicepod_definition: SpicepodDefinition =
            serde_yaml::from_reader(spicepod_rdr).context(UnableToParseSpicepodSnafu)?;

        let resolved_datasets = component::resolve_component_references(
            fs,
            &path,
            &spicepod_definition.datasets,
            "dataset",
        )
        .context(UnableToResolveSpicepodComponentsSnafu { path: path.clone() })?;

        let resolved_models = component::resolve_component_references(
            fs,
            &path,
            &spicepod_definition.models,
            "model",
        )
        .context(UnableToResolveSpicepodComponentsSnafu { path: path.clone() })?;

        Ok(from_definition(spicepod_definition, resolved_datasets, resolved_models))
    }
}

#[must_use]
fn from_definition(spicepod_definition: SpicepodDefinition, datasets: Vec<Dataset>, models: Vec<Model>) -> Spicepod {
    Spicepod {
        name: spicepod_definition.name,
        datasets,
        models,
        dependencies: spicepod_definition.dependencies,
    }
}
