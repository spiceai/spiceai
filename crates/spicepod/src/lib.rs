/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use component::tool::Tool;
use reader::ReadableYaml;
use serde::{Deserialize, Serialize};
use snafu::prelude::*;
use std::collections::HashMap;
use std::{fmt::Debug, path::PathBuf};

use component::{
    catalog::Catalog, dataset::Dataset, embeddings::Embeddings, eval::Eval, extension::Extension,
    model::Model, runtime::Runtime, secret::Secret, view::View,
};

use spec::{SpicepodDefinition, SpicepodVersion};

pub mod component;
pub mod reader;
pub mod spec;

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

    #[snafu(display("Unable to load duplicate spicepod {component} component '{name}'"))]
    DuplicateComponent { component: String, name: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Spicepod {
    pub version: SpicepodVersion,

    pub name: String,

    pub extensions: HashMap<String, Extension>,

    pub secrets: Vec<Secret>,

    pub catalogs: Vec<Catalog>,

    pub datasets: Vec<Dataset>,

    pub views: Vec<View>,

    pub models: Vec<Model>,

    pub dependencies: Vec<String>,

    pub embeddings: Vec<Embeddings>,

    pub evals: Vec<Eval>,

    pub tools: Vec<Tool>,

    pub runtime: Runtime,
}

fn detect_duplicate_component_names(
    component_type: &str,
    components: &[impl component::Nameable],
) -> Result<()> {
    let mut component_names = vec![];
    for component in components {
        if component_names.contains(&component.name()) {
            return Err(Error::DuplicateComponent {
                component: component_type.to_string(),
                name: component.name().to_string(),
            });
        }
        component_names.push(component.name());
    }
    Ok(())
}

impl Spicepod {
    pub fn load(path: impl Into<PathBuf>) -> Result<Self> {
        Self::load_from(&reader::StdFileSystem, path)
    }

    fn load_from_rdr<T>(
        fs: &impl reader::ReadableYaml<T>,
        spicepod_rdr: Box<dyn std::io::Read>,
        path: impl Into<PathBuf>,
    ) -> Result<Spicepod> {
        let path = path.into();
        let spicepod_definition: SpicepodDefinition =
            serde_yaml::from_reader(spicepod_rdr).context(UnableToParseSpicepodSnafu)?;

        let resolved_datasets = component::resolve_component_references(
            fs,
            &path,
            &spicepod_definition.datasets,
            "dataset",
        )
        .context(UnableToResolveSpicepodComponentsSnafu { path: path.clone() })?;

        let resolved_catalogs = component::resolve_component_references(
            fs,
            &path,
            &spicepod_definition.catalogs,
            "catalog",
        )
        .context(UnableToResolveSpicepodComponentsSnafu { path: path.clone() })?;

        let resolved_views =
            component::resolve_component_references(fs, &path, &spicepod_definition.views, "view")
                .context(UnableToResolveSpicepodComponentsSnafu { path: path.clone() })?;

        let resolved_models = component::resolve_component_references(
            fs,
            &path,
            &spicepod_definition.models,
            "model",
        )
        .context(UnableToResolveSpicepodComponentsSnafu { path: path.clone() })?;

        let resolved_embeddings = component::resolve_component_references(
            fs,
            &path,
            &spicepod_definition.embeddings,
            "embeddings",
        )
        .context(UnableToResolveSpicepodComponentsSnafu { path: path.clone() })?;

        let resolved_evals =
            component::resolve_component_references(fs, &path, &spicepod_definition.evals, "evals")
                .context(UnableToResolveSpicepodComponentsSnafu { path: path.clone() })?;

        let resolved_tools =
            component::resolve_component_references(fs, &path, &spicepod_definition.tools, "tools")
                .context(UnableToResolveSpicepodComponentsSnafu { path: path.clone() })?;

        detect_duplicate_component_names("secrets", &spicepod_definition.secrets[..])?;
        detect_duplicate_component_names("dataset", &resolved_datasets[..])?;
        detect_duplicate_component_names("view", &resolved_views[..])?;
        detect_duplicate_component_names("model", &resolved_models[..])?;
        detect_duplicate_component_names("embedding", &resolved_embeddings[..])?;
        detect_duplicate_component_names("eval", &resolved_evals[..])?;
        detect_duplicate_component_names("tool", &resolved_tools[..])?;

        Ok(from_definition(
            spicepod_definition,
            resolved_catalogs,
            resolved_datasets,
            resolved_views,
            resolved_embeddings,
            resolved_evals,
            resolved_tools,
            resolved_models,
        ))
    }

    pub fn load_exact(path: impl Into<PathBuf>) -> Result<Spicepod> {
        let fs = reader::StdFileSystem;
        let path = path.into();
        let spicepod_rdr = fs
            .open_exact_yaml(&path)
            .map_err(|_| Error::SpicepodNotFound { path: path.clone() })?;

        Self::load_from_rdr(&fs, spicepod_rdr, path)
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

        Self::load_from_rdr(fs, spicepod_rdr, path)
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

#[allow(clippy::too_many_arguments)]
#[must_use]
fn from_definition(
    spicepod_definition: SpicepodDefinition,
    catalogs: Vec<Catalog>,
    datasets: Vec<Dataset>,
    views: Vec<View>,
    embeddings: Vec<Embeddings>,
    evals: Vec<Eval>,
    tools: Vec<Tool>,
    models: Vec<Model>,
) -> Spicepod {
    Spicepod {
        name: spicepod_definition.name,
        version: spicepod_definition.version,
        extensions: spicepod_definition.extensions,
        secrets: spicepod_definition.secrets,
        catalogs,
        datasets,
        views,
        models,
        embeddings,
        evals,
        tools,
        dependencies: spicepod_definition.dependencies,
        runtime: spicepod_definition.runtime,
    }
}
