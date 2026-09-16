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
    acceleration::Mode as AccelerationMode,
    component::{
        caching::{CacheConfig, SQLResultsCacheConfig},
        catalog::Catalog,
        dataset::Dataset,
        embeddings::Embeddings,
        function::Function,
        management::Management,
        model::Model,
        rerankers::Reranker,
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

    pub rerankers: Vec<Reranker>,

    pub tools: Vec<Tool>,

    pub workers: Vec<Worker>,

    pub functions: Vec<Function>,

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
            rerankers: vec![],
            tools: vec![],
            workers: vec![],
            functions: vec![],
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

    #[snafu(display(
        "Invalid Cayenne configuration: datasets use different `cayenne_file_path` values without a shared `cayenne_metadata_dir`: {datasets}. Set the same `cayenne_metadata_dir` on every Cayenne dataset. See: https://spiceai.org/docs/components/data-accelerators/cayenne#metastore-location"
    ))]
    InvalidCayenneConfiguration { datasets: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl Error {
    /// Whether the load failed because the Spicepod file is absent, rather than
    /// present but unloadable. See [`spicepod::Error::is_spicepod_missing`].
    #[must_use]
    pub fn is_spicepod_missing(&self) -> bool {
        match self {
            Self::UnableToLoadSpicepod { source, .. } => source.is_spicepod_missing(),
            Self::InvalidCayenneConfiguration { .. } => false,
        }
    }
}

fn cayenne_file_path_conflict(datasets: &[Dataset]) -> Option<String> {
    let cayenne_datasets: Vec<(String, String, Option<String>)> = datasets
        .iter()
        .filter_map(|dataset| {
            let acceleration = dataset.acceleration.as_ref()?;
            if !acceleration.enabled
                || !acceleration.engine.as_deref().is_some_and(|engine| {
                    engine.eq_ignore_ascii_case("cayenne") || engine.eq_ignore_ascii_case("vortex")
                })
                || !matches!(
                    acceleration.mode,
                    AccelerationMode::File
                        | AccelerationMode::FileCreate
                        | AccelerationMode::FileUpdate
                )
            {
                return None;
            }

            let params = acceleration.params.as_ref()?;
            let file_path = params.data.get("cayenne_file_path")?.as_string();
            let metadata_dir = params
                .data
                .get("cayenne_metadata_dir")
                .map(spicepod::param::ParamValue::as_string);
            Some((dataset.name.clone(), file_path, metadata_dir))
        })
        .collect();

    let first_file_path = cayenne_datasets
        .first()
        .map(|(_, file_path, _)| file_path.trim_end_matches('/'));
    if cayenne_datasets
        .iter()
        .all(|(_, file_path, _)| Some(file_path.trim_end_matches('/')) == first_file_path)
    {
        return None;
    }

    let first_metadata_dir = cayenne_datasets.first().and_then(|(_, _, metadata_dir)| {
        metadata_dir
            .as_deref()
            .map(|path| path.trim_end_matches('/'))
    });
    if first_metadata_dir.is_some()
        && cayenne_datasets.iter().all(|(_, _, metadata_dir)| {
            metadata_dir
                .as_deref()
                .map(|path| path.trim_end_matches('/'))
                == first_metadata_dir
        })
    {
        return None;
    }

    Some(
        cayenne_datasets
            .iter()
            .map(|(name, file_path, _)| {
                format!("`{}` (`{}`)", name.escape_debug(), file_path.escape_debug())
            })
            .collect::<Vec<_>>()
            .join(", "),
    )
}

pub struct AppBuilder {
    name: String,
    secrets: Vec<Secret>,
    extensions: HashMap<String, Extension>,
    catalogs: Vec<Catalog>,
    datasets: Vec<Dataset>,
    views: Vec<View>,
    models: Vec<Model>,
    embeddings: Vec<Embeddings>,
    rerankers: Vec<Reranker>,
    tools: Vec<Tool>,
    workers: Vec<Worker>,
    functions: Vec<Function>,
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
            rerankers: vec![],
            tools: vec![],
            workers: vec![],
            functions: vec![],
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
        self.rerankers.extend(spicepod.rerankers.clone());
        self.tools.extend(spicepod.tools.clone());
        self.workers.extend(spicepod.workers.clone());
        self.functions.extend(spicepod.functions.clone());
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
    pub fn with_function(mut self, function: Function) -> AppBuilder {
        self.functions.push(function);
        self
    }

    #[must_use]
    pub fn with_sql_cache(mut self, sql_results: SQLResultsCacheConfig) -> AppBuilder {
        self.runtime.caching.sql_results = Some(sql_results);
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
            rerankers: self.rerankers,
            tools: self.tools,
            workers: self.workers,
            functions: self.functions,
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
        let mut rerankers: Vec<Reranker> = vec![];
        let mut tools: Vec<Tool> = vec![];
        let mut workers: Vec<Worker> = vec![];
        let mut functions: Vec<Function> = vec![];

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

        for reranker in &spicepod.rerankers {
            rerankers.push(reranker.clone());
        }

        for tool in &spicepod.tools {
            tools.push(tool.clone());
        }

        for worker in &spicepod.workers {
            workers.push(worker.clone());
        }

        for function in &spicepod.functions {
            functions.push(function.clone());
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

            for reranker in &dependent_spicepod.rerankers {
                rerankers.push(reranker.clone());
            }

            for tool in &dependent_spicepod.tools {
                tools.push(tool.clone());
            }

            for worker in &dependent_spicepod.workers {
                workers.push(worker.clone());
            }

            for function in &dependent_spicepod.functions {
                functions.push(function.clone());
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
        if let Some(datasets) = cayenne_file_path_conflict(&datasets) {
            return InvalidCayenneConfigurationSnafu { datasets }.fail();
        }

        Ok(App {
            name: root_spicepod_name,
            secrets,
            extensions,
            catalogs,
            datasets,
            views,
            models,
            embeddings,
            rerankers,
            tools,
            workers,
            functions,
            spicepods,
            runtime,
            management,
            snapshots: snapshots.map(Arc::new),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{AccelerationMode, Error, cayenne_file_path_conflict};
    use spicepod::{acceleration::Acceleration, component::dataset::Dataset, param::Params};
    use std::collections::HashMap;

    fn cayenne_dataset(name: &str, file_path: &str, metadata_dir: Option<&str>) -> Dataset {
        let mut params = HashMap::from([("cayenne_file_path".to_string(), file_path.to_string())]);
        if let Some(metadata_dir) = metadata_dir {
            params.insert("cayenne_metadata_dir".to_string(), metadata_dir.to_string());
        }

        let mut dataset = Dataset::new("file:data.parquet", name);
        dataset.acceleration = Some(Acceleration {
            engine: Some("cayenne".to_string()),
            mode: AccelerationMode::File,
            params: Some(Params::from_string_map(params)),
            ..Default::default()
        });
        dataset
    }

    #[test]
    fn cayenne_rejects_multiple_data_roots_without_shared_metadata() {
        let datasets = vec![
            cayenne_dataset("orders", "/mnt/a/cayenne", None),
            cayenne_dataset("customers", "/mnt/b/cayenne", None),
        ];

        let datasets = cayenne_file_path_conflict(&datasets).expect("paths must conflict");
        let error = Error::InvalidCayenneConfiguration { datasets };
        assert_eq!(
            error.to_string(),
            "Invalid Cayenne configuration: datasets use different `cayenne_file_path` values without a shared `cayenne_metadata_dir`: `orders` (`/mnt/a/cayenne`), `customers` (`/mnt/b/cayenne`). Set the same `cayenne_metadata_dir` on every Cayenne dataset. See: https://spiceai.org/docs/components/data-accelerators/cayenne#metastore-location"
        );
    }

    #[test]
    fn cayenne_accepts_multiple_data_roots_with_shared_metadata() {
        let datasets = vec![
            cayenne_dataset("orders", "/mnt/a/cayenne", Some("/data/cayenne/metadata")),
            cayenne_dataset(
                "customers",
                "/mnt/b/cayenne",
                Some("/data/cayenne/metadata"),
            ),
        ];

        assert!(cayenne_file_path_conflict(&datasets).is_none());
    }

    #[test]
    fn cayenne_accepts_one_shared_data_root_without_explicit_metadata() {
        let datasets = vec![
            cayenne_dataset("orders", "/data/cayenne", None),
            cayenne_dataset("customers", "/data/cayenne/", None),
        ];

        assert!(cayenne_file_path_conflict(&datasets).is_none());
    }

    #[test]
    fn cayenne_rejects_conflicts_for_runtime_engine_spellings() {
        let mut datasets = vec![
            cayenne_dataset("orders", "/mnt/a/cayenne", None),
            cayenne_dataset("customers", "/mnt/b/cayenne", None),
        ];
        datasets[0]
            .acceleration
            .as_mut()
            .expect("acceleration")
            .engine = Some("Cayenne".into());
        datasets[1]
            .acceleration
            .as_mut()
            .expect("acceleration")
            .engine = Some("vortex".into());

        assert!(cayenne_file_path_conflict(&datasets).is_some());
    }
}
