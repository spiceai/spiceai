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
        embeddings::Embeddings,
        extension::Extension,
        llms::Llm,
        model::Model,
        runtime::{ResultsCache, Runtime},
        secrets::{SecretStore, SecretStoreType},
        view::View,
    },
    Spicepod,
};

#[derive(Debug, PartialEq)]
pub struct App {
    pub name: String,

    pub secrets: Vec<SecretStore>,

    pub extensions: HashMap<String, Extension>,

    pub datasets: Vec<Dataset>,

    pub views: Vec<View>,

    pub models: Vec<Model>,

    pub embeddings: Vec<Embeddings>,

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
    secrets: Vec<SecretStore>,
    extensions: HashMap<String, Extension>,
    datasets: Vec<Dataset>,
    views: Vec<View>,
    models: Vec<Model>,
    llms: Vec<Llm>,
    embeddings: Vec<Embeddings>,
    spicepods: Vec<Spicepod>,
    runtime: Runtime,
}

impl AppBuilder {
    pub fn new(name: impl Into<String>) -> AppBuilder {
        AppBuilder {
            name: name.into(),
            secrets: vec![],
            extensions: HashMap::new(),
            datasets: vec![],
            views: vec![],
            models: vec![],
            llms: vec![],
            embeddings: vec![],
            spicepods: vec![],
            runtime: Runtime::default(),
        }
    }

    #[must_use]
    pub fn with_spicepod(mut self, spicepod: Spicepod) -> AppBuilder {
        self.secrets.extend(spicepod.secrets.clone());
        self.extensions.extend(spicepod.extensions.clone());
        self.datasets.extend(spicepod.datasets.clone());
        self.views.extend(spicepod.views.clone());
        self.models.extend(spicepod.models.clone());
        self.llms.extend(spicepod.llms.clone());
        self.embeddings.extend(spicepod.embeddings.clone());
        self.spicepods.push(spicepod);
        self
    }

    #[must_use]
    pub fn with_extension(mut self, name: String, extension: Extension) -> AppBuilder {
        self.extensions.insert(name, extension);
        self
    }

    #[must_use]
    pub fn with_secret_store(mut self, secret: SecretStore) -> AppBuilder {
        self.secrets.push(secret);
        self
    }

    #[must_use]
    pub fn with_dataset(mut self, dataset: Dataset) -> AppBuilder {
        self.datasets.push(dataset);
        self
    }

    #[must_use]
    pub fn with_view(mut self, view: View) -> AppBuilder {
        self.views.push(view);
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
    pub fn with_embedding(mut self, embedding: Embeddings) -> AppBuilder {
        self.embeddings.push(embedding);
        self
    }

    #[must_use]
    pub fn with_results_cache(mut self, results_cache: ResultsCache) -> AppBuilder {
        self.runtime.results_cache = results_cache;
        self
    }

    #[must_use]
    pub fn build(self) -> App {
        App {
            name: self.name,
            secrets: self.secrets,
            extensions: self.extensions,
            datasets: self.datasets,
            views: self.views,
            models: self.models,
            llms: self.llms,
            embeddings: self.embeddings,
            spicepods: self.spicepods,
            runtime: self.runtime,
        }
    }

    pub fn build_from_filesystem_path(path: impl Into<PathBuf>) -> Result<App> {
        let path = path.into();
        let spicepod_root =
            Spicepod::load(&path).context(UnableToLoadSpicepodSnafu { path: path.clone() })?;
        let runtime = spicepod_root.runtime.clone();
        let extensions = spicepod_root.extensions.clone();
        let mut secrets: Vec<SecretStore> = vec![];
        let mut datasets: Vec<Dataset> = vec![];
        let mut views: Vec<View> = vec![];
        let mut models: Vec<Model> = vec![];
        let mut llms: Vec<Llm> = vec![];
        let mut embeddings: Vec<Embeddings> = vec![];

        if spicepod_root.secrets.is_empty() {
            // Default to file and env secrets
            secrets.push({
                SecretStore {
                    store: SecretStoreType::File,
                    params: None,
                }
            });
            secrets.push({
                SecretStore {
                    store: SecretStoreType::Env,
                    params: None,
                }
            });
        } else {
            for secret in &spicepod_root.secrets {
                secrets.push(secret.clone());
            }
        }

        for dataset in &spicepod_root.datasets {
            datasets.push(dataset.clone());
        }

        for view in &spicepod_root.views {
            views.push(view.clone());
        }

        for model in &spicepod_root.models {
            models.push(model.clone());
        }

        for llm in &spicepod_root.llms {
            llms.push(llm.clone());
        }

        for embedding in &spicepod_root.embeddings {
            embeddings.push(embedding.clone());
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
            for view in &dependent_spicepod.views {
                views.push(view.clone());
            }
            for model in &dependent_spicepod.models {
                models.push(model.clone());
            }
            for llm in &dependent_spicepod.llms {
                llms.push(llm.clone());
            }
            for embedding in &dependent_spicepod.embeddings {
                embeddings.push(embedding.clone());
            }
            spicepods.push(dependent_spicepod);
        }

        spicepods.push(spicepod_root);

        Ok(App {
            name: root_spicepod_name,
            secrets,
            extensions,
            datasets,
            views,
            models,
            embeddings,
            llms,
            spicepods,
            runtime,
        })
    }
}
