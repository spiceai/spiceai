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

#![allow(clippy::missing_errors_doc)]

use std::{collections::HashMap, path::PathBuf};

use snafu::prelude::*;
use spicepod::{
    component::{
        dataset::Dataset,
        extension::Extension,
        llms::Llm,
        model::Model,
        runtime::Runtime,
        secrets::{Secrets, SpiceSecretStore},
    },
    Spicepod,
};

#[derive(Debug, PartialEq)]
pub struct App {
    pub name: String,

    pub secrets: Secrets,

    pub extensions: HashMap<String, Extension>,

    pub datasets: Vec<Dataset>,

    pub models: Vec<Model>,

    pub llms: Vec<Llm>,

    pub spicepods: Vec<Spicepod>,

    pub runtime: Runtime,
}

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to load spicepod {}: {source}", path.display()))]
    UnableToLoadSpicepod {
        source: spicepod::Error,
        path: PathBuf,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub struct AppBuilder {
    name: String,
    secrets: Secrets,
    extensions: HashMap<String, Extension>,
    datasets: Vec<Dataset>,
    models: Vec<Model>,
    llms: Vec<Llm>,
    spicepods: Vec<Spicepod>,
    runtime: Runtime,
}

impl AppBuilder {
    pub fn new(name: impl Into<String>) -> AppBuilder {
        AppBuilder {
            name: name.into(),
            secrets: Secrets::default(),
            extensions: HashMap::new(),
            datasets: vec![],
            models: vec![],
            llms: vec![],
            spicepods: vec![],
            runtime: Runtime::default(),
        }
    }

    #[must_use]
    pub fn with_spicepod(mut self, spicepod: Spicepod) -> AppBuilder {
        self.secrets = spicepod.secrets.clone();
        self.extensions.extend(spicepod.extensions.clone());
        self.datasets.extend(spicepod.datasets.clone());
        self.models.extend(spicepod.models.clone());
        self.llms.extend(spicepod.llms.clone());
        self.spicepods.push(spicepod);
        self
    }

    #[must_use]
    pub fn with_extension(mut self, name: String, extension: Extension) -> AppBuilder {
        self.extensions.insert(name, extension);
        self
    }

    #[must_use]
    pub fn with_secret_store(mut self, secret: SpiceSecretStore) -> AppBuilder {
        self.secrets = Secrets { store: secret };
        self
    }

    #[must_use]
    pub fn with_dataset(mut self, dataset: Dataset) -> AppBuilder {
        self.datasets.push(dataset);
        self
    }

    #[must_use]
    pub fn with_model(mut self, model: Model) -> AppBuilder {
        self.models.push(model);
        self
    }

    #[must_use]
    pub fn with_llm(mut self, llm: Llm) -> AppBuilder {
        self.llms.push(llm);
        self
    }

    #[must_use]
    pub fn build(self) -> App {
        App {
            name: self.name,
            secrets: self.secrets,
            extensions: self.extensions,
            datasets: self.datasets,
            models: self.models,
            llms: self.llms,
            spicepods: self.spicepods,
            runtime: self.runtime,
        }
    }

    pub fn build_from_filesystem_path(path: impl Into<PathBuf>) -> Result<App> {
        let path = path.into();
        let spicepod_root =
            Spicepod::load(&path).context(UnableToLoadSpicepodSnafu { path: path.clone() })?;
        let secrets = spicepod_root.secrets.clone();
        let runtime = spicepod_root.runtime.clone();
        let extensions = spicepod_root.extensions.clone();
        let mut datasets: Vec<Dataset> = vec![];
        let mut models: Vec<Model> = vec![];
        let mut llms: Vec<Llm> = vec![];

        for dataset in &spicepod_root.datasets {
            datasets.push(dataset.clone());
        }

        for model in &spicepod_root.models {
            models.push(model.clone());
        }

        for llm in &spicepod_root.llms {
            llms.push(llm.clone());
        }

        let root_spicepod_name = spicepod_root.name.clone();
        let mut spicepods: Vec<Spicepod> = vec![];

        for dependency in &spicepod_root.dependencies {
            let dependency_path = path.join("spicepods").join(dependency);
            let dependent_spicepod =
                Spicepod::load(&dependency_path).context(UnableToLoadSpicepodSnafu {
                    path: &dependency_path,
                })?;
            for dataset in &dependent_spicepod.datasets {
                datasets.push(dataset.clone());
            }
            for model in &dependent_spicepod.models {
                models.push(model.clone());
            }
            for llm in &dependent_spicepod.llms {
                llms.push(llm.clone());
            }
            spicepods.push(dependent_spicepod);
        }

        spicepods.push(spicepod_root);

        Ok(App {
            name: root_spicepod_name,
            secrets,
            extensions,
            datasets,
            models,
            llms,
            spicepods,
            runtime,
        })
    }
}
