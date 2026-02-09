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

use std::{collections::HashMap, path::PathBuf, sync::Arc};

use serde::{Deserialize, Serialize};
use snafu::prelude::*;
pub use spicepod;
use spicepod::{
    Spicepod,
    component::{
        caching::{CacheConfig, ResultsCache},
        catalog::Catalog,
        dataset::Dataset,
        embeddings::Embeddings,
        eval::Eval,
        management::Management,
        model::Model,
        runtime::{CorsConfig, Runtime, TlsConfig},
        secret::Secret,
        snapshot::Snapshots,
        tool::Tool,
        view::View,
        worker::Worker,
    },
    extension::Extension,
};
use util::in_tracing_context;

pub mod runtime;

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
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

    pub workers: Vec<Worker>,

    pub spicepods: Vec<Spicepod>,

    pub runtime: Runtime,

    pub management: Option<Management>,

    pub snapshots: Option<Arc<Snapshots>>,
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

impl Default for App {
    fn default() -> Self {
        App {
            name: "DEFAULT".to_string(),
            secrets: vec![],
            extensions: HashMap::default(),
            catalogs: vec![],
            datasets: vec![],
            views: vec![],
            models: vec![],
            embeddings: vec![],
            evals: vec![],
            tools: vec![],
            workers: vec![],
            spicepods: vec![],
            runtime: Runtime::default(),
            management: None,
            snapshots: None,
        }
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
    workers: Vec<Worker>,
    spicepods: Vec<Spicepod>,
    runtime: Runtime,
    management: Option<Management>,
    snapshots: Option<Snapshots>,
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
            workers: vec![],
            spicepods: vec![],
            runtime: Runtime::default(),
            management: None,
            snapshots: None,
        }
    }
    #[must_use]
    pub fn with_spicepod(mut self, spicepod: Spicepod) -> AppBuilder {
        self.runtime = spicepod.runtime.clone();
        self.secrets.extend(spicepod.secrets.clone());
        self.extensions.extend(spicepod.extensions.clone());
        if let Some(ref management) = spicepod.management {
            self.management = Some(management.clone());
        }
        if let Some(ref snapshot) = spicepod.snapshots {
            self.snapshots = Some(snapshot.clone());
        }
        self.catalogs.extend(spicepod.catalogs.clone());
        self.datasets.extend(spicepod.datasets.clone());
        self.views.extend(spicepod.views.clone());
        self.models.extend(spicepod.models.clone());
        self.embeddings.extend(spicepod.embeddings.clone());
        self.evals.extend(spicepod.evals.clone());
        self.tools.extend(spicepod.tools.clone());
        self.workers.extend(spicepod.workers.clone());
        self.spicepods.push(spicepod);
        self
    }

    /// Load a spicepod dependency into the app builder.
    ///
    /// As a dependency, `.runtime`, `.management`, and `.snapshots` configurations will be ignored.
    #[must_use]
    pub fn with_spicepod_dependency(mut self, mut spicepod: Spicepod) -> AppBuilder {
        if spicepod.runtime != Runtime::default() {
            in_tracing_context(|| {
                tracing::warn!(
                    "Spicepod dependency has 'runtime' field(s) defined. Runtime configuration must be set in primary spicepod. runtime configuration from dependency will be ignored."
                );
            });
        }
        spicepod.runtime = self.runtime.clone();

        if spicepod.management.is_some() {
            in_tracing_context(|| {
                tracing::warn!(
                    "Spicepod dependency has 'management' field(s) defined. Management configuration must be set in primary spicepod. management configuration from dependency will be ignored."
                );
            });
        }
        spicepod.management = None;
        if spicepod.snapshots.is_some() {
            in_tracing_context(|| {
                tracing::warn!(
                    "Spicepod dependency has 'snapshots' field(s) defined. Snapshot configuration must be set in primary spicepod. snapshots configuration from dependency will be ignored."
                );
            });
        }
        spicepod.snapshots = None;
        self = self.with_spicepod(spicepod);
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
    pub fn with_worker(mut self, worker: Worker) -> AppBuilder {
        self.workers.push(worker);
        self
    }

    #[must_use]
    pub fn with_results_cache(mut self, results_cache: ResultsCache) -> AppBuilder {
        self.runtime.results_cache = Some(results_cache);
        self
    }

    #[must_use]
    pub fn with_search_cache(mut self, search_cache: CacheConfig) -> AppBuilder {
        self.runtime.caching.search_results = Some(search_cache);
        self
    }

    #[must_use]
    pub fn with_embeddings_cache(mut self, embeddings_cache: CacheConfig) -> AppBuilder {
        self.runtime.caching.embeddings = Some(embeddings_cache);
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
    pub fn with_shutdown_timeout(mut self, timeout: impl Into<String>) -> AppBuilder {
        self.runtime.shutdown_timeout = Some(timeout.into());
        self
    }

    #[must_use]
    pub fn with_management(mut self, management: Management) -> AppBuilder {
        self.management = Some(management);
        self
    }

    #[must_use]
    pub fn with_snapshots(mut self, snapshots: Snapshots) -> AppBuilder {
        self.snapshots = Some(snapshots);
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
            workers: self.workers,
            spicepods: self.spicepods,
            runtime: self.runtime,
            management: self.management,
            snapshots: self.snapshots.map(Arc::new),
        }
    }

    pub async fn build_from_path(path: impl Into<PathBuf>) -> Result<App> {
        let path = path.into();
        let spicepod_root = Spicepod::load(&path)
            .await
            .context(UnableToLoadSpicepodSnafu { path: path.clone() })?;
        Self::build_from_spicepod(spicepod_root, Spicepod::base_path(&path)).await
    }

    pub async fn build_from_spicepod(spicepod: Spicepod, path: impl Into<PathBuf>) -> Result<App> {
        let path = path.into();
        let secrets = spicepod.secrets.clone();
        let runtime = spicepod.runtime.clone();
        let extensions = spicepod.extensions.clone();
        let management = spicepod.management.clone();
        let snapshots = spicepod.snapshots.clone();
        let mut catalogs: Vec<Catalog> = vec![];
        let mut datasets: Vec<Dataset> = vec![];
        let mut views: Vec<View> = vec![];
        let mut models: Vec<Model> = vec![];
        let mut embeddings: Vec<Embeddings> = vec![];
        let mut evals: Vec<Eval> = vec![];
        let mut tools: Vec<Tool> = vec![];
        let mut workers: Vec<Worker> = vec![];

        for catalog in &spicepod.catalogs {
            catalogs.push(catalog.clone());
        }

        for dataset in &spicepod.datasets {
            datasets.push(dataset.clone());
        }

        for view in &spicepod.views {
            views.push(view.clone());
        }

        for model in &spicepod.models {
            models.push(model.clone());
        }

        for embedding in &spicepod.embeddings {
            embeddings.push(embedding.clone());
        }

        for eval in &spicepod.evals {
            evals.push(eval.clone());
        }

        for tool in &spicepod.tools {
            tools.push(tool.clone());
        }

        for worker in &spicepod.workers {
            workers.push(worker.clone());
        }

        let root_spicepod_name = spicepod.name.clone();
        let mut spicepods: Vec<Spicepod> = vec![];

        for dependency in &spicepod.dependencies {
            let dependency_path = path.join("spicepods").join(dependency);
            let dependent_spicepod =
                Spicepod::load(&dependency_path)
                    .await
                    .context(UnableToLoadSpicepodSnafu {
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

            for worker in &dependent_spicepod.workers {
                workers.push(worker.clone());
            }

            if dependent_spicepod.runtime != Runtime::default() {
                in_tracing_context(|| {
                    tracing::warn!(
                        "Spicepod dependency '{dependency}' has 'runtime' field(s) defined. Runtime configuration must be set in primary spicepod. '{dependency}' runtime configuration will be ignored."
                    );
                });
            }

            if dependent_spicepod.management.is_some() {
                in_tracing_context(|| {
                    tracing::warn!(
                        "Spicepod dependency '{dependency}' has 'management' field(s) defined. Management configuration must be set in primary spicepod. '{dependency}' management configuration will be ignored."
                    );
                });
            }

            if dependent_spicepod.snapshots.is_some() {
                in_tracing_context(|| {
                    tracing::warn!(
                        "Spicepod dependency '{dependency}' has 'snapshots' field(s) defined. Snapshot configuration must be set in primary spicepod. '{dependency}' snapshots configuration will be ignored."
                    );
                });
            }

            spicepods.push(dependent_spicepod);
        }

        spicepods.push(spicepod);

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
            workers,
            spicepods,
            runtime,
            management,
            snapshots: snapshots.map(Arc::new),
        })
    }
}
