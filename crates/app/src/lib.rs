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

use std::{collections::HashMap, path::PathBuf};

use snafu::prelude::*;
pub use spicepod;
use spicepod::{
    component::{
        catalog::Catalog,
        dataset::Dataset,
        embeddings::Embeddings,
        eval::Eval,
        extension::Extension,
        model::Model,
        runtime::{CorsConfig, ResultsCache, Runtime, TlsConfig},
        secret::Secret,
        tool::Tool,
        view::View,
    },
    Spicepod,
};

pub mod runtime;

#[derive(Debug, PartialEq, Clone)]
pub struct App {
    pub name: String,

    pub secrets: Vec<Secret>,

    pub extensions: HashMap<String, Extension>,

    pub catalogs: Vec<Catalog>,

    pub datasets: Vec<Dataset>,

    pub views: Vec<View>,

    pub models: Vec<Model>,

    pub embeddings: Vec<Embeddings>,

    pub evals: Vec<Eval>,

    pub tools: Vec<Tool>,

    pub spicepods: Vec<Spicepod>,

    pub runtime: Runtime,
}

impl App {
    /// Retrieve all dataset names that are of a specific connector type.
    #[must_use]
    pub fn datasets_of_connector_type(&self, prefix: &str) -> Vec<String> {
        self.datasets
            .iter()
            .filter(|d| d.from.starts_with(format!("{prefix}:").as_str()))
            .map(|d| d.name.clone())
            .collect()
    }
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
    secrets: Vec<Secret>,
    extensions: HashMap<String, Extension>,
    catalogs: Vec<Catalog>,
    datasets: Vec<Dataset>,
    views: Vec<View>,
    models: Vec<Model>,
    embeddings: Vec<Embeddings>,
    evals: Vec<Eval>,
    tools: Vec<Tool>,
    spicepods: Vec<Spicepod>,
    runtime: Runtime,
}

impl AppBuilder {
    pub fn new(name: impl Into<String>) -> AppBuilder {
        AppBuilder {
            name: name.into(),
            secrets: vec![],
            extensions: HashMap::new(),
            catalogs: vec![],
            datasets: vec![],
            views: vec![],
            models: vec![],
            embeddings: vec![],
            evals: vec![],
            tools: vec![],
            spicepods: vec![],
            runtime: Runtime::default(),
        }
    }

    #[must_use]
    pub fn with_spicepod(mut self, spicepod: Spicepod) -> AppBuilder {
        self.secrets.extend(spicepod.secrets.clone());
        self.extensions.extend(spicepod.extensions.clone());
        self.catalogs.extend(spicepod.catalogs.clone());
        self.datasets.extend(spicepod.datasets.clone());
        self.views.extend(spicepod.views.clone());
        self.models.extend(spicepod.models.clone());
        self.embeddings.extend(spicepod.embeddings.clone());
        self.evals.extend(spicepod.evals.clone());
        self.tools.extend(spicepod.tools.clone());
        self.spicepods.push(spicepod);
        self
    }

    #[must_use]
    pub fn with_extension(mut self, name: String, extension: Extension) -> AppBuilder {
        self.extensions.insert(name, extension);
        self
    }

    #[must_use]
    pub fn with_secret(mut self, secret: Secret) -> AppBuilder {
        self.secrets.push(secret);
        self
    }

    #[must_use]
    pub fn with_catalog(mut self, catalog: Catalog) -> AppBuilder {
        self.catalogs.push(catalog);
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
    pub fn with_eval(mut self, eval: Eval) -> AppBuilder {
        self.evals.push(eval);
        self
    }

    #[must_use]
    pub fn with_embedding(mut self, embedding: Embeddings) -> AppBuilder {
        self.embeddings.push(embedding);
        self
    }

    #[must_use]
    pub fn with_tool(mut self, tool: Tool) -> AppBuilder {
        self.tools.push(tool);
        self
    }

    #[must_use]
    pub fn with_results_cache(mut self, results_cache: ResultsCache) -> AppBuilder {
        self.runtime.results_cache = results_cache;
        self
    }

    #[must_use]
    pub fn with_tls_config(mut self, tls_config: TlsConfig) -> AppBuilder {
        self.runtime.tls = Some(tls_config);
        self
    }

    #[must_use]
    pub fn with_runtime_params(mut self, params: HashMap<String, String>) -> AppBuilder {
        self.runtime.params = params;
        self
    }

    #[must_use]
    pub fn with_cors_config(mut self, cors_config: CorsConfig) -> AppBuilder {
        self.runtime.cors = cors_config;
        self
    }

    #[must_use]
    pub fn with_runtime(mut self, runtime: Runtime) -> AppBuilder {
        self.runtime = runtime;
        self
    }

    #[must_use]
    pub fn build(self) -> App {
        App {
            name: self.name,
            secrets: self.secrets,
            extensions: self.extensions,
            catalogs: self.catalogs,
            datasets: self.datasets,
            views: self.views,
            models: self.models,
            embeddings: self.embeddings,
            evals: self.evals,
            tools: self.tools,
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
        let mut catalogs: Vec<Catalog> = vec![];
        let mut datasets: Vec<Dataset> = vec![];
        let mut views: Vec<View> = vec![];
        let mut models: Vec<Model> = vec![];
        let mut embeddings: Vec<Embeddings> = vec![];
        let mut evals: Vec<Eval> = vec![];
        let mut tools: Vec<Tool> = vec![];

        for catalog in &spicepod_root.catalogs {
            catalogs.push(catalog.clone());
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

        for embedding in &spicepod_root.embeddings {
            embeddings.push(embedding.clone());
        }

        for eval in &spicepod_root.evals {
            evals.push(eval.clone());
        }

        for tool in &spicepod_root.tools {
            tools.push(tool.clone());
        }

        let root_spicepod_name = spicepod_root.name.clone();
        let mut spicepods: Vec<Spicepod> = vec![];

        for dependency in &spicepod_root.dependencies {
            let dependency_path = path.join("spicepods").join(dependency);
            let dependent_spicepod =
                Spicepod::load(&dependency_path).context(UnableToLoadSpicepodSnafu {
                    path: &dependency_path,
                })?;
            for catalog in &dependent_spicepod.catalogs {
                catalogs.push(catalog.clone());
            }
            for dataset in &dependent_spicepod.datasets {
                datasets.push(dataset.clone());
            }
            for view in &dependent_spicepod.views {
                views.push(view.clone());
            }
            for model in &dependent_spicepod.models {
                models.push(model.clone());
            }
            for embedding in &dependent_spicepod.embeddings {
                embeddings.push(embedding.clone());
            }

            for eval in &dependent_spicepod.evals {
                evals.push(eval.clone());
            }

            for tool in &dependent_spicepod.tools {
                tools.push(tool.clone());
            }
            spicepods.push(dependent_spicepod);
        }

        spicepods.push(spicepod_root);

        Ok(App {
            name: root_spicepod_name,
            secrets,
            extensions,
            catalogs,
            datasets,
            views,
            models,
            embeddings,
            evals,
            tools,
            spicepods,
            runtime,
        })
    }
}
