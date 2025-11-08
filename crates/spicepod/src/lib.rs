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

use component::management::Management;
use extension::Extension;
use reader::ReadableYaml;
use serde::{Deserialize, Serialize};
use snafu::prelude::*;
use std::collections::HashMap;
use std::path::Path;
use std::{fmt::Debug, path::PathBuf};

#[cfg(feature = "object-store")]
use std::sync::Arc;

use component::{
    catalog::Catalog, dataset::Dataset, embeddings::Embeddings, eval::Eval, model::Model,
    runtime::Runtime, secret::Secret, snapshot::Snapshots, tool::Tool, view::View, worker::Worker,
};

use crate::component::Nameable;
use spec::{SpicepodDefinition, SpicepodVersion};

pub mod acceleration;
pub mod component;
pub mod extension;
mod keywords;
pub mod metric;
pub mod param;
pub mod partitioning;
pub mod reader;
pub mod semantic;
pub mod spec;
pub mod vector;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Failed to parse spicepod.yaml: {source}\n\n\
        The spicepod.yaml file contains invalid YAML or does not match the expected schema.\n\
        Common issues:\n\
        • Check for syntax errors (indentation, colons, quotes)\n\
        • Verify all required fields are present (name, version, kind)\n\
        • Ensure component definitions match the schema\n\n\
        See: https://docs.spiceai.org/reference/spicepod for the complete schema reference."
    ))]
    UnableToParseSpicepod { source: serde_yaml::Error },

    #[snafu(display("Unable to resolve spicepod components {}: {source}", path.display()))]
    UnableToResolveSpicepodComponents {
        source: component::Error,
        path: PathBuf,
    },

    #[snafu(display(
        "spicepod.yaml not found in {}\n\n\
        Cannot start the Spice runtime without a valid spicepod.yaml file.\n\n\
        To fix this:\n\
        • If you're in the wrong directory, navigate to your Spice app directory\n\
        • If you haven't created a Spice app yet, run: spice init <app-name>\n\
        • If spicepod.yaml exists but isn't being detected, check the file name and location\n\n\
        Current working directory: {}\n\
        Expected file: {}/spicepod.yaml or {}/spicepod.yml",
        path.display(),
        std::env::current_dir().ok().and_then(|p| p.to_str().map(String::from)).unwrap_or_else(|| "<unknown>".to_string()),
        path.display(),
        path.display()
    ))]
    SpicepodNotFound { path: PathBuf },

    #[snafu(display("Unable to load duplicate spicepod {component} component '{name}'"))]
    DuplicateComponent { component: String, name: String },

    #[cfg(feature = "object-store")]
    #[snafu(display("Unable to parse URL {}: {source}", path))]
    UnableToParseUrl {
        source: object_store::Error,
        path: String,
    },

    #[snafu(display("Unable to open spicepod {}: {source}", path.display()))]
    UnableToOpenSpicepod {
        source: Box<reader::Error>,
        path: PathBuf,
    },

    #[cfg(feature = "object-store")]
    #[snafu(display("Unable to parse S3 URL {}: {source}", path))]
    UnableToParseS3Url {
        source: aws_sdk_credential_bridge::Error,
        path: String,
    },

    #[snafu(display(
        "The name '{keyword}' is reserved and cannot be used as a name for a dataset. Change the name in the Spicepod and try again."
    ))]
    UseOfReservedKeyword { keyword: String },
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

    pub workers: Vec<Worker>,

    pub runtime: Runtime,

    pub management: Option<Management>,

    pub snapshots: Option<Snapshots>,
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

fn check_for_reserved_keywords(components: &[Dataset]) -> Result<()> {
    for component in components {
        if keywords::is_reserved_keyword(component.name()) {
            return Err(Error::UseOfReservedKeyword {
                keyword: component.name().to_string(),
            });
        }
    }
    Ok(())
}

impl Spicepod {
    pub async fn load(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        #[cfg(feature = "object-store")]
        match url::Url::parse(&path.to_string_lossy()) {
            Ok(url)
                if matches!(
                    url.scheme(),
                    "s3" | "gs" | "azure" | "abfs" | "abfss" | "https"
                ) =>
            {
                Self::load_from_object_store(url).await
            }
            _ => Self::load_from(&reader::StdFileSystem, path).await,
        }

        #[cfg(not(feature = "object-store"))]
        {
            Self::load_from(&reader::StdFileSystem, path).await
        }
    }

    #[cfg(feature = "object-store")]
    pub async fn load_from_object_store(url: url::Url) -> Result<Self> {
        let (store, path) = match (url.scheme(), url.path()) {
            ("s3", path) => {
                let store = aws_sdk_credential_bridge::from_s3_url(&url, None)
                    .await
                    .context(UnableToParseS3UrlSnafu {
                        path: url.to_string(),
                    })?;
                let path = object_store::path::Path::from(path);
                (store, path)
            }
            _ => object_store::parse_url(&url).context(UnableToParseUrlSnafu {
                path: url.to_string(),
            })?,
        };

        let object_fs = reader::ObjectStoreFilesystem::new(Arc::new(store));

        Self::load_from(&object_fs, path.to_string()).await
    }

    async fn load_from_rdr(
        fs: &(impl reader::ReadableYaml + Send + Sync),
        spicepod_rdr: Box<dyn std::io::Read + Send + Sync>,
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
        .await
        .context(UnableToResolveSpicepodComponentsSnafu { path: path.clone() })?;

        let resolved_catalogs = component::resolve_component_references(
            fs,
            &path,
            &spicepod_definition.catalogs,
            "catalog",
        )
        .await
        .context(UnableToResolveSpicepodComponentsSnafu { path: path.clone() })?;

        let resolved_views =
            component::resolve_component_references(fs, &path, &spicepod_definition.views, "view")
                .await
                .context(UnableToResolveSpicepodComponentsSnafu { path: path.clone() })?;

        let resolved_models = component::resolve_component_references(
            fs,
            &path,
            &spicepod_definition.models,
            "model",
        )
        .await
        .context(UnableToResolveSpicepodComponentsSnafu { path: path.clone() })?;

        let resolved_embeddings = component::resolve_component_references(
            fs,
            &path,
            &spicepod_definition.embeddings,
            "embeddings",
        )
        .await
        .context(UnableToResolveSpicepodComponentsSnafu { path: path.clone() })?;

        let resolved_evals =
            component::resolve_component_references(fs, &path, &spicepod_definition.evals, "evals")
                .await
                .context(UnableToResolveSpicepodComponentsSnafu { path: path.clone() })?;

        let resolved_tools =
            component::resolve_component_references(fs, &path, &spicepod_definition.tools, "tools")
                .await
                .context(UnableToResolveSpicepodComponentsSnafu { path: path.clone() })?;

        let resolved_workers = component::resolve_component_references(
            fs,
            &path,
            &spicepod_definition.workers,
            "workers",
        )
        .await
        .context(UnableToResolveSpicepodComponentsSnafu { path: path.clone() })?;

        detect_duplicate_component_names("secrets", &spicepod_definition.secrets[..])?;
        detect_duplicate_component_names("dataset", &resolved_datasets[..])?;
        detect_duplicate_component_names("view", &resolved_views[..])?;
        detect_duplicate_component_names("model", &resolved_models[..])?;
        detect_duplicate_component_names("embedding", &resolved_embeddings[..])?;
        detect_duplicate_component_names("eval", &resolved_evals[..])?;
        detect_duplicate_component_names("tool", &resolved_tools[..])?;
        detect_duplicate_component_names("worker", &resolved_workers[..])?;

        check_for_reserved_keywords(&resolved_datasets[..])?;

        Ok(from_definition(
            spicepod_definition,
            resolved_catalogs,
            resolved_datasets,
            resolved_views,
            resolved_embeddings,
            resolved_evals,
            resolved_tools,
            resolved_models,
            resolved_workers,
        ))
    }

    pub async fn load_exact(path: impl Into<PathBuf>) -> Result<Spicepod> {
        let fs = reader::StdFileSystem;
        let path = path.into();
        let spicepod_rdr = fs
            .open_exact_yaml(path.clone())
            .await
            .map_err(Box::new)
            .context(UnableToOpenSpicepodSnafu { path: path.clone() })?;

        Self::load_from_rdr(&fs, spicepod_rdr, path).await
    }

    pub async fn load_from(
        fs: &(impl reader::ReadableYaml + Send + Sync),
        path: impl Into<PathBuf>,
    ) -> Result<Spicepod> {
        let path = path.into();

        let file_stem = path
            .file_stem()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_default();

        let is_file = path.is_file() || path.extension().is_some();

        let (spicepod_rdr, base_path) = if file_stem == "spicepod" && is_file {
            let spicepod_rdr = fs
                .open_exact_yaml(path.clone())
                .await
                .map_err(Box::new)
                .context(UnableToOpenSpicepodSnafu { path: path.clone() })?;
            (spicepod_rdr, path.parent().unwrap_or(Path::new(".")))
        } else {
            let spicepod_rdr = fs
                .open_yaml(path.clone(), "spicepod")
                .await
                .context(SpicepodNotFoundSnafu { path: path.clone() })?;
            (spicepod_rdr, path.as_ref())
        };

        Self::load_from_rdr(fs, spicepod_rdr, base_path).await
    }

    pub async fn load_definition(path: impl Into<PathBuf>) -> Result<SpicepodDefinition> {
        Self::load_definition_from(&reader::StdFileSystem, path).await
    }

    pub async fn load_definition_from(
        fs: &(impl reader::ReadableYaml + Send + Sync),
        path: impl Into<PathBuf>,
    ) -> Result<SpicepodDefinition> {
        let path = path.into();

        let spicepod_rdr = fs
            .open_yaml(path.clone(), "spicepod")
            .await
            .context(SpicepodNotFoundSnafu { path: path.clone() })?;

        let spicepod_definition: SpicepodDefinition =
            serde_yaml::from_reader(spicepod_rdr).context(UnableToParseSpicepodSnafu)?;

        Ok(spicepod_definition)
    }

    #[must_use]
    pub fn base_path(path: &Path) -> &Path {
        if path.is_file() || path.extension().is_some() {
            path.parent().unwrap_or(Path::new("."))
        } else {
            path
        }
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
    workers: Vec<Worker>,
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
        workers,
        dependencies: spicepod_definition.dependencies,
        runtime: spicepod_definition.runtime,
        management: spicepod_definition.management,
        snapshots: spicepod_definition.snapshots,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_from_spicepods() {
        const SPICEPOD_FILES: [&str; 5] = [
            "./tests/basic_spicepod.yaml",
            "./tests/spicepod_with_caching.yaml",
            "./tests/spicepod_with_only_sql_results.yaml",
            "./tests/spicepod_with_only_search_results.yaml",
            "./tests/spicepod_with_results_cache.yaml",
        ];

        for file in SPICEPOD_FILES {
            let path = PathBuf::from(file);
            Spicepod::load_exact(&path)
                .await
                .expect("Should load spicepod");
        }
    }
}
