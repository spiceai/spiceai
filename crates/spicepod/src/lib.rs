/*
 Copyright 2024 Spice AI, Inc.

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

#![allow(clippy::missing_errors_doc)]
#![allow(clippy::module_name_repetitions)]

use serde::{Deserialize, Serialize};
use snafu::prelude::*;
use std::{fmt::Debug, path::PathBuf};

use component::dataset::Dataset;
use component::model::Model;
use component::secrets::Secrets;
use spec::{SpicepodDefinition, SpicepodVersion};

pub mod component;
pub mod reader;
mod spec;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to parse spicepod.yaml: {source}"))]
    UnableToParseSpicepod { source: serde_yaml::Error },
    #[snafu(display("Unable to resolve spicepod components {}: {source}", path.display()))]
    UnableToResolveSpicepodComponents {
        source: component::Error,
        path: PathBuf,
    },
    #[snafu(display("spicepod.yaml not found in {}, run `spice init <name>` to initialize spicepod.yaml", path.display()))]
    SpicepodNotFound { path: PathBuf },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Spicepod {
    pub version: SpicepodVersion,

    pub name: String,

    pub secrets: Secrets,

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

        Ok(from_definition(
            spicepod_definition,
            resolved_datasets,
            resolved_models,
        ))
    }

    pub fn load_definition(path: impl Into<PathBuf>) -> Result<SpicepodDefinition> {
        Self::load_definition_from(&reader::StdFileSystem, path)
    }

    pub fn load_definition_from<T>(
        fs: &impl reader::ReadableYaml<T>,
        path: impl Into<PathBuf>,
    ) -> Result<SpicepodDefinition> {
        let path = path.into();
        let path_str = path.to_string_lossy().to_string();

        let spicepod_rdr = fs
            .open_yaml(&path_str, "spicepod")
            .ok_or_else(|| Error::SpicepodNotFound { path: path.clone() })?;

        let spicepod_definition: SpicepodDefinition =
            serde_yaml::from_reader(spicepod_rdr).context(UnableToParseSpicepodSnafu)?;

        Ok(spicepod_definition)
    }
}

#[must_use]
fn from_definition(
    spicepod_definition: SpicepodDefinition,
    datasets: Vec<Dataset>,
    models: Vec<Model>,
) -> Spicepod {
    Spicepod {
        name: spicepod_definition.name,
        version: spicepod_definition.version,
        secrets: spicepod_definition.secrets,
        datasets,
        models,
        dependencies: spicepod_definition.dependencies,
    }
}
