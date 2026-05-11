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
    catalog::Catalog, dataset::Dataset, embeddings::Embeddings, function::Function, model::Model,
    rerankers::Reranker, runtime::Runtime, secret::Secret, snapshot::Snapshots, tool::Tool,
    view::View, worker::Worker,
};

use crate::component::Nameable;
use spec::{SpicepodDefinition, SpicepodVersion};

pub mod acceleration;
pub mod component;
pub mod extension;
pub mod fts;
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
    UnableToParseSpicepod { source: yaml::Error },

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

    #[snafu(display(
        "Not a valid Spicepod file: {}\n\n\
        Expected 'kind: Spicepod' but found 'kind: {kind}'.\n\
        Ensure the file is a valid Spicepod YAML file.",
        path.display()
    ))]
    InvalidSpicepodKind { kind: String, path: PathBuf },

    #[snafu(display(
        "Unsupported Spicepod version in {}: '{version}'\n\n\
        Supported versions are: v1, v2 (or full semver: v1.0.0, v2.0.0-rc.1, etc.)",
        path.display()
    ))]
    InvalidSpicepodVersion { version: String, path: PathBuf },

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

    pub rerankers: Vec<Reranker>,

    pub tools: Vec<Tool>,

    pub workers: Vec<Worker>,

    pub functions: Vec<Function>,

    pub runtime: Runtime,

    pub management: Option<Management>,

    pub snapshots: Option<Snapshots>,
}

fn validate_spicepod_header(raw: &yaml::Value, path: &Path) -> Result<()> {
    if let Some(kind) = raw.get("kind").and_then(yaml::Value::as_str)
        && kind != "Spicepod"
    {
        return InvalidSpicepodKindSnafu {
            kind: kind.to_string(),
            path: path.to_path_buf(),
        }
        .fail();
    }

    if let Some(version_val) = raw.get("version")
        && yaml::from_value::<SpicepodVersion>(version_val.clone()).is_err()
    {
        return InvalidSpicepodVersionSnafu {
            version: version_val.as_str().unwrap_or("unknown").to_string(),
            path: path.to_path_buf(),
        }
        .fail();
    }

    Ok(())
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

        let raw: yaml::Value =
            yaml::from_reader(spicepod_rdr).context(UnableToParseSpicepodSnafu)?;

        validate_spicepod_header(&raw, &path)?;

        let spicepod_definition: SpicepodDefinition =
            yaml::from_value(raw).context(UnableToParseSpicepodSnafu)?;

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

        let resolved_rerankers = component::resolve_component_references(
            fs,
            &path,
            &spicepod_definition.rerankers,
            "rerankers",
        )
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

        let resolved_functions = component::resolve_component_references(
            fs,
            &path,
            &spicepod_definition.functions,
            "functions",
        )
        .await
        .context(UnableToResolveSpicepodComponentsSnafu { path: path.clone() })?;

        detect_duplicate_component_names("secrets", &spicepod_definition.secrets[..])?;
        detect_duplicate_component_names("dataset", &resolved_datasets[..])?;
        detect_duplicate_component_names("view", &resolved_views[..])?;
        detect_duplicate_component_names("model", &resolved_models[..])?;
        detect_duplicate_component_names("embedding", &resolved_embeddings[..])?;
        detect_duplicate_component_names("reranker", &resolved_rerankers[..])?;
        detect_duplicate_component_names("tool", &resolved_tools[..])?;
        detect_duplicate_component_names("worker", &resolved_workers[..])?;
        detect_duplicate_component_names("function", &resolved_functions[..])?;

        check_for_reserved_keywords(&resolved_datasets[..])?;

        Ok(from_definition(
            spicepod_definition,
            resolved_catalogs,
            resolved_datasets,
            resolved_views,
            resolved_embeddings,
            resolved_rerankers,
            resolved_tools,
            resolved_models,
            resolved_workers,
            resolved_functions,
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

        let is_file = path.is_file() || path.extension().is_some();

        let (spicepod_rdr, base_path) = if is_file {
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
            yaml::from_reader(spicepod_rdr).context(UnableToParseSpicepodSnafu)?;

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

#[expect(clippy::too_many_arguments)]
#[must_use]
fn from_definition(
    spicepod_definition: SpicepodDefinition,
    catalogs: Vec<Catalog>,
    datasets: Vec<Dataset>,
    views: Vec<View>,
    embeddings: Vec<Embeddings>,
    rerankers: Vec<Reranker>,
    tools: Vec<Tool>,
    models: Vec<Model>,
    workers: Vec<Worker>,
    functions: Vec<Function>,
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
        rerankers,
        tools,
        workers,
        functions,
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

    #[tokio::test]
    async fn test_spicepod_with_functions_loads() {
        let pod = Spicepod::load_exact(&PathBuf::from("./tests/spicepod_with_functions.yaml"))
            .await
            .expect("Should load spicepod with functions");
        assert_eq!(pod.functions.len(), 6);
        let names: Vec<&str> = pod.functions.iter().map(|f| f.name.as_str()).collect();
        assert_eq!(
            names,
            vec![
                "double_it",
                "haversine_km",
                "shout",
                "classify_intent",
                "internal_hash",
                "duplicate_rows"
            ]
        );

        let double_it = &pod.functions[0];
        assert_eq!(double_it.from, "sql");
        assert_eq!(double_it.signature.args.len(), 1);
        assert_eq!(double_it.signature.args[0].name, "x");
        assert_eq!(double_it.signature.args[0].arrow_type, "int64");
        assert_eq!(double_it.signature.scalar_return_type(), Some("int64"));
        assert_eq!(double_it.body.as_deref(), Some("x * 2"));
        // Defaults to being exposed as a tool.
        assert!(double_it.as_tool);

        let classify = &pod.functions[3];
        assert_eq!(classify.from, "http://classifier.internal/v1/classify");
        assert!(classify.body.is_none());
        assert_eq!(classify.params.len(), 2);

        let internal = &pod.functions[4];
        assert_eq!(internal.name, "internal_hash");
        assert!(!internal.as_tool, "internal_hash should be SQL-only");

        let duplicate_rows = &pod.functions[5];
        assert_eq!(duplicate_rows.name, "duplicate_rows");
        assert_eq!(
            duplicate_rows.kind,
            component::function::FunctionKind::Table
        );
        let columns = duplicate_rows
            .signature
            .table_return_columns()
            .expect("table return columns");
        assert_eq!(columns.len(), 2);
        assert_eq!(columns[0].name, "value");
        assert_eq!(columns[1].arrow_type, "int64");

        // Tool with as_sql: true is visible on the resolved Spicepod.
        assert_eq!(pod.tools.len(), 1);
        let geocode = &pod.tools[0];
        assert_eq!(geocode.name, "geocode");
        assert!(geocode.as_sql);
        let sig = geocode.signature.as_ref().expect("signature");
        assert_eq!(sig.args.len(), 1);
        assert_eq!(sig.args[0].arrow_type, "utf8");
    }

    /// Verify that both v1 and v2 spicepod schemas are parsed correctly.
    #[tokio::test]
    async fn test_v1_and_v2_versions() {
        let v1_pod = Spicepod::load_exact(&PathBuf::from("./tests/basic_spicepod.yaml"))
            .await
            .expect("Should load v1 spicepod");
        assert_eq!(v1_pod.version, SpicepodVersion::V1);

        let v2_pod = Spicepod::load_exact(&PathBuf::from("./tests/basic_spicepod_v2.yaml"))
            .await
            .expect("Should load v2 spicepod");
        assert_eq!(v2_pod.version, SpicepodVersion::V2);
    }
}

/// Comprehensive tests for v1 and v2 spicepod schema support.
///
/// v1 corresponds to the `release/1.11` branch schema.
/// v2 corresponds to the current `trunk` branch schema.
///
/// Key differences:
/// - v1 uses `runtime.results_cache`, v2 uses `runtime.caching.sql_results`
/// - v1 uses top-level `runtime.memory_limit`/`runtime.temp_directory`,
///   v2 uses `runtime.query.memory_limit`/`runtime.query.temp_directory`
/// - v2 adds `runtime.ready_state`, `runtime.flight.do_put_rate_limit_enabled`,
///   `runtime.scheduler` partition assignment fields
/// - v2 adds `read_write_create` access mode
/// - v2 adds `stale_while_revalidate_ttl` and `encoding` to `SQLResultsCacheConfig`
#[cfg(test)]
mod version_tests {
    use super::*;
    use crate::component::{
        access::AccessMode,
        caching::{CacheEngine, CacheKeyType, CachingPolicy, Encoding, HashingAlgorithm},
        runtime::RuntimeReadyState,
    };

    // ========================================================================
    // Version parsing
    // ========================================================================

    /// All supported version strings parse to the correct enum variant.
    #[test]
    fn test_version_enum_deserialization() {
        let v1: SpicepodVersion = yaml::from_str("v1").expect("Should parse v1");
        assert_eq!(v1, SpicepodVersion::V1);

        let v2: SpicepodVersion = yaml::from_str("v2").expect("Should parse v2");
        assert_eq!(v2, SpicepodVersion::V2);
    }

    /// v1beta1 is no longer a valid version.
    #[test]
    fn test_v1beta1_rejected() {
        let result: Result<SpicepodVersion, _> = yaml::from_str("v1beta1");
        assert!(result.is_err(), "v1beta1 should no longer be accepted");
    }

    /// Version strings serialize to the expected lowercase YAML values.
    #[test]
    fn test_version_enum_serialization() {
        let v1_str = yaml::to_string(&SpicepodVersion::V1).expect("Should serialize v1");
        assert!(v1_str.trim().contains("v1"), "Expected 'v1', got: {v1_str}");

        let v2_str = yaml::to_string(&SpicepodVersion::V2).expect("Should serialize v2");
        assert!(v2_str.trim().contains("v2"), "Expected 'v2', got: {v2_str}");
    }

    /// Default version is V2.
    #[test]
    fn test_default_version_is_v2() {
        assert_eq!(SpicepodVersion::default(), SpicepodVersion::V2);
    }

    /// `SpicepodDefinition::new()` creates a V2 definition.
    #[test]
    fn test_spicepod_definition_new_is_v2() {
        let def = SpicepodDefinition::new("test_pod");
        assert_eq!(def.version, SpicepodVersion::V2);
        assert_eq!(def.name, "test_pod");
    }

    /// Unknown version strings produce a deserialization error.
    #[test]
    fn test_invalid_version_rejected() {
        let yaml = r"
            version: v3
            kind: Spicepod
            name: invalid
        ";
        let result: Result<SpicepodDefinition, _> = yaml::from_str(yaml);
        assert!(result.is_err(), "Unknown version 'v3' should be rejected");
    }

    // ========================================================================
    // v1 schema support (backward compat with release/1.11)
    // ========================================================================

    /// Minimal v1 spicepod loads correctly.
    #[tokio::test]
    async fn test_v1_minimal_loads() {
        let pod = Spicepod::load_exact(&PathBuf::from("./tests/basic_spicepod.yaml"))
            .await
            .expect("Should load minimal v1 spicepod");
        assert_eq!(pod.version, SpicepodVersion::V1);
        assert_eq!(pod.name, "basic_spicepod");
    }

    /// v1 full-featured spicepod with deprecated `results_cache`, `memory_limit`,
    /// and `temp_directory` loads and migrates correctly.
    #[tokio::test]
    async fn test_v1_full_featured_loads() {
        let pod = Spicepod::load_exact(&PathBuf::from("./tests/v1_full_featured.yaml"))
            .await
            .expect("Should load v1 full-featured spicepod");
        assert_eq!(pod.version, SpicepodVersion::V1);
        assert_eq!(pod.name, "v1_full_featured");

        // Deprecated `results_cache` migrates to `caching.sql_results`
        let sql_results = pod
            .runtime
            .caching
            .sql_results
            .as_ref()
            .expect("sql_results should be migrated from results_cache");
        assert!(sql_results.enabled);
        assert_eq!(sql_results.item_ttl, Some("5s".to_string()));
        assert_eq!(sql_results.max_size, Some("128MiB".to_string()));

        // Deprecated `memory_limit` migrates to `query.memory_limit`
        let query = pod.runtime.query.as_ref().expect("query should be present");
        assert_eq!(query.memory_limit, Some("512MiB".to_string()));

        // Deprecated `temp_directory` migrates to `query.temp_directory`
        assert_eq!(query.temp_directory, Some("/tmp/spice_v1".to_string()));

        // Flight still works without v2-only fields
        let flight = pod
            .runtime
            .flight
            .as_ref()
            .expect("flight should be present");
        assert!(
            flight.do_put_rate_limit_enabled,
            "should default to true for v1"
        );

        // Dataset loaded
        assert_eq!(pod.datasets.len(), 1);
        assert_eq!(pod.datasets[0].name, "my_dataset");
        assert_eq!(pod.datasets[0].access, AccessMode::ReadWrite);

        // v2-only fields get defaults
        assert_eq!(pod.runtime.ready_state, RuntimeReadyState::OnLoad);
    }

    /// v1 `results_cache` with all config options migrates correctly.
    #[tokio::test]
    async fn test_v1_caching_style_loads() {
        let pod = Spicepod::load_exact(&PathBuf::from("./tests/v1_caching_style.yaml"))
            .await
            .expect("Should load v1 caching-style spicepod");
        assert_eq!(pod.version, SpicepodVersion::V1);

        let sql_results = pod
            .runtime
            .caching
            .sql_results
            .as_ref()
            .expect("sql_results should be migrated from results_cache");
        assert!(sql_results.enabled);
        assert_eq!(sql_results.item_ttl, Some("30s".to_string()));
        assert_eq!(sql_results.max_size, Some("64MiB".to_string()));
        assert_eq!(sql_results.caching_policy, CachingPolicy::TinyLfu);
        assert_eq!(sql_results.hashing_algorithm, HashingAlgorithm::Blake3);
        assert_eq!(sql_results.cache_key_type, CacheKeyType::Sql);
        assert_eq!(sql_results.engine, CacheEngine::Pingora);
        // `max_stale_while_revalidate` does NOT map to `stale_while_revalidate_ttl`
        // in the From<ResultsCache> impl — it becomes None
        assert_eq!(sql_results.stale_while_revalidate_ttl, None);
        // Encoding defaults to None when migrating from v1
        assert_eq!(sql_results.encoding, Encoding::None);
    }

    /// v1 `results_cache` combined with explicit `caching.sql_results` —
    /// the explicit v2-style takes priority.
    #[test]
    fn test_v1_results_cache_vs_explicit_sql_results() {
        let yaml = r"
            results_cache:
                enabled: false
                item_ttl: 10s
            caching:
                sql_results:
                    enabled: true
                    item_ttl: 60s
        ";
        let runtime: Runtime = yaml::from_str(yaml).expect("Should parse runtime with both styles");
        let sql_results = runtime
            .caching
            .sql_results
            .expect("sql_results should be present");
        assert!(
            sql_results.enabled,
            "Explicit caching.sql_results should take priority"
        );
        assert_eq!(sql_results.item_ttl, Some("60s".to_string()));
    }

    /// v1 spicepods with `caching:` (v2-style) work even at v1.
    #[tokio::test]
    async fn test_v1_with_v2_caching_style() {
        let pod = Spicepod::load_exact(&PathBuf::from(
            "./tests/spicepod_with_only_sql_results.yaml",
        ))
        .await
        .expect("Should load v1 spicepod with v2-style caching");
        assert_eq!(pod.version, SpicepodVersion::V1);

        let sql_results = pod
            .runtime
            .caching
            .sql_results
            .expect("sql_results should be present");
        assert!(sql_results.enabled);
        assert_eq!(sql_results.item_ttl, Some("1s".to_string()));
    }

    /// v1 deprecated `memory_limit` at top-level is overridden by `query.memory_limit`.
    #[test]
    fn test_v1_memory_limit_query_takes_priority() {
        let yaml = r"
            memory_limit: 100MiB
            query:
                memory_limit: 200MiB
        ";
        let runtime: Runtime = yaml::from_str(yaml).expect("Should parse Runtime");
        let query = runtime.query.expect("query should exist");
        assert_eq!(
            query.memory_limit,
            Some("200MiB".to_string()),
            "query.memory_limit should take priority over deprecated top-level memory_limit"
        );
    }

    /// v1 deprecated `temp_directory` at top-level is overridden by `query.temp_directory`.
    #[test]
    fn test_v1_temp_directory_query_takes_priority() {
        let yaml = r"
            temp_directory: /old
            query:
                temp_directory: /new
        ";
        let runtime: Runtime = yaml::from_str(yaml).expect("Should parse Runtime");
        let query = runtime.query.expect("query should exist");
        assert_eq!(
            query.temp_directory,
            Some("/new".to_string()),
            "query.temp_directory should take priority over deprecated top-level temp_directory"
        );
    }

    // ========================================================================
    // v2 schema support (trunk)
    // ========================================================================

    /// Minimal v2 spicepod loads correctly.
    #[tokio::test]
    async fn test_v2_minimal_loads() {
        let pod = Spicepod::load_exact(&PathBuf::from("./tests/basic_spicepod_v2.yaml"))
            .await
            .expect("Should load minimal v2 spicepod");
        assert_eq!(pod.version, SpicepodVersion::V2);
        assert_eq!(pod.name, "basic_spicepod_v2");
    }

    /// v2 full-featured spicepod with all new features.
    #[tokio::test]
    async fn test_v2_full_featured_loads() {
        let pod = Spicepod::load_exact(&PathBuf::from("./tests/v2_full_featured.yaml"))
            .await
            .expect("Should load v2 full-featured spicepod");
        assert_eq!(pod.version, SpicepodVersion::V2);
        assert_eq!(pod.name, "v2_full_featured");

        // ready_state
        assert_eq!(pod.runtime.ready_state, RuntimeReadyState::OnRegistration);

        // Flight with do_put_rate_limit_enabled
        let flight = pod
            .runtime
            .flight
            .as_ref()
            .expect("flight should be present");
        assert_eq!(flight.max_message_size, Some("32MiB".to_string()));
        assert!(!flight.do_put_rate_limit_enabled);

        // Caching with v2 fields
        let sql_results = pod
            .runtime
            .caching
            .sql_results
            .as_ref()
            .expect("sql_results should be present");
        assert!(sql_results.enabled);
        assert_eq!(sql_results.item_ttl, Some("10s".to_string()));
        assert_eq!(sql_results.max_size, Some("256MiB".to_string()));
        assert_eq!(
            sql_results.stale_while_revalidate_ttl,
            Some("30s".to_string())
        );
        assert_eq!(sql_results.encoding, Encoding::Zstd);

        let search = pod
            .runtime
            .caching
            .search_results
            .as_ref()
            .expect("search_results should be present");
        assert!(search.enabled);
        assert_eq!(search.item_ttl, Some("5s".to_string()));

        let emb = pod
            .runtime
            .caching
            .embeddings
            .as_ref()
            .expect("embeddings cache should be present");
        assert!(!emb.enabled);

        // Query
        let query = pod.runtime.query.as_ref().expect("query should be present");
        assert_eq!(query.memory_limit, Some("1GiB".to_string()));
        assert_eq!(query.temp_directory, Some("/tmp/spice_v2".to_string()));

        // Dataset
        assert_eq!(pod.datasets.len(), 1);
        assert_eq!(pod.datasets[0].name, "my_dataset");

        // Catalog with read_write_create access
        assert_eq!(pod.catalogs.len(), 1);
        assert_eq!(pod.catalogs[0].name, "my_catalog");
        assert_eq!(pod.catalogs[0].access, AccessMode::ReadWriteCreate);
    }

    /// v2 caching with all advanced options.
    #[tokio::test]
    async fn test_v2_caching_style_loads() {
        let pod = Spicepod::load_exact(&PathBuf::from("./tests/v2_caching_style.yaml"))
            .await
            .expect("Should load v2 caching-style spicepod");
        assert_eq!(pod.version, SpicepodVersion::V2);

        let sql_results = pod
            .runtime
            .caching
            .sql_results
            .as_ref()
            .expect("sql_results should be present");
        assert!(sql_results.enabled);
        assert_eq!(sql_results.item_ttl, Some("30s".to_string()));
        assert_eq!(sql_results.max_size, Some("64MiB".to_string()));
        assert_eq!(sql_results.caching_policy, CachingPolicy::TinyLfu);
        assert_eq!(sql_results.hashing_algorithm, HashingAlgorithm::Blake3);
        assert_eq!(sql_results.cache_key_type, CacheKeyType::Sql);
        assert_eq!(sql_results.engine, CacheEngine::Pingora);
        assert_eq!(
            sql_results.stale_while_revalidate_ttl,
            Some("60s".to_string())
        );
        assert_eq!(sql_results.encoding, Encoding::Zstd);
    }

    /// v2 scheduler with partition assignment fields.
    #[tokio::test]
    async fn test_v2_scheduler_with_partition_assignment() {
        let pod = Spicepod::load_exact(&PathBuf::from("./tests/v2_with_scheduler.yaml"))
            .await
            .expect("Should load v2 scheduler spicepod");
        assert_eq!(pod.version, SpicepodVersion::V2);

        let scheduler = pod
            .runtime
            .scheduler
            .as_ref()
            .expect("scheduler should be present");
        assert_eq!(scheduler.state_location, "s3://my-bucket/scheduler-state");
        assert_eq!(scheduler.partition_assignment_interval, "15s");
        assert_eq!(scheduler.max_partition_assignments_per_interval, 50);
        assert_eq!(scheduler.max_partitions_per_executor, 500);
        assert_eq!(scheduler.partition_discovery_timeout, "120s");
    }

    // ========================================================================
    // v2-only features via inline YAML deserialization
    // ========================================================================

    /// `ready_state: on_registration` is a v2-only feature.
    #[test]
    fn test_v2_ready_state_on_registration() {
        let yaml = r"
            ready_state: on_registration
        ";
        let runtime: Runtime = yaml::from_str(yaml).expect("Should parse Runtime");
        assert_eq!(runtime.ready_state, RuntimeReadyState::OnRegistration);
    }

    /// `ready_state` defaults to `on_load`.
    #[test]
    fn test_ready_state_defaults_to_on_load() {
        let yaml = "";
        let runtime: Runtime = yaml::from_str(yaml).expect("Should parse Runtime");
        assert_eq!(runtime.ready_state, RuntimeReadyState::OnLoad);
    }

    /// `do_put_rate_limit_enabled` defaults to true.
    #[test]
    fn test_flight_do_put_rate_limit_defaults_true() {
        let yaml = r"
            max_message_size: 4MiB
        ";
        let flight: component::runtime::Flight = yaml::from_str(yaml).expect("Should parse Flight");
        assert!(flight.do_put_rate_limit_enabled);
    }

    /// `do_put_rate_limit_enabled` can be explicitly disabled.
    #[test]
    fn test_flight_do_put_rate_limit_disabled() {
        let yaml = r"
            do_put_rate_limit_enabled: false
        ";
        let flight: component::runtime::Flight = yaml::from_str(yaml).expect("Should parse Flight");
        assert!(!flight.do_put_rate_limit_enabled);
    }

    /// Scheduler defaults for partition assignment fields.
    #[test]
    fn test_partition_assignment_defaults() {
        let yaml = r"
            state_location: s3://bucket/state
        ";
        let scheduler: component::runtime::Scheduler =
            yaml::from_str(yaml).expect("Should parse Scheduler");
        assert_eq!(scheduler.partition_assignment_interval, "30s");
        assert_eq!(scheduler.max_partition_assignments_per_interval, 100);
        assert_eq!(scheduler.max_partitions_per_executor, 1000);
        assert_eq!(scheduler.partition_discovery_timeout, "60s");
    }

    #[test]
    fn test_runtime_source_rate_control_deserializes() {
        let yaml = r"
            source_rate_control:
              state_location: file:///tmp/spice-source-rate-control
              refresh_interval: 15s
              github_concurrent_connections_limit: 5
              params:
                allow_http: true
        ";
        let runtime: Runtime = yaml::from_str(yaml).expect("Should parse Runtime");
        let source_rate_control = runtime
            .source_rate_control
            .expect("source_rate_control section should exist");
        assert_eq!(
            source_rate_control.state_location.as_deref(),
            Some("file:///tmp/spice-source-rate-control")
        );
        assert_eq!(source_rate_control.refresh_interval, "15s");
        assert_eq!(
            source_rate_control.github_concurrent_connections_limit,
            Some(5)
        );
        assert!(source_rate_control.params.is_some());
    }

    /// `read_write_create` access mode deserializes.
    #[test]
    fn test_access_mode_read_write_create() {
        let mode: AccessMode =
            yaml::from_str("read_write_create").expect("Should parse AccessMode");
        assert_eq!(mode, AccessMode::ReadWriteCreate);
    }

    /// All access modes round-trip correctly.
    #[test]
    fn test_access_mode_roundtrip() {
        for mode in &[
            AccessMode::Read,
            AccessMode::ReadWrite,
            AccessMode::ReadWriteCreate,
        ] {
            let s = yaml::to_string(mode).expect("Should serialize AccessMode");
            let deserialized: AccessMode =
                yaml::from_str(&s).expect("Should deserialize AccessMode");
            assert_eq!(
                *mode, deserialized,
                "AccessMode roundtrip failed for {mode:?}"
            );
        }
    }

    /// `SQLResultsCacheConfig` v2 fields: `stale_while_revalidate_ttl`, `encoding`.
    #[test]
    fn test_sql_results_cache_v2_fields() {
        let yaml = r"
            enabled: true
            item_ttl: 30s
            stale_while_revalidate_ttl: 60s
            encoding: zstd
        ";
        let config: component::caching::SQLResultsCacheConfig =
            yaml::from_str(yaml).expect("Should parse SQLResultsCacheConfig");
        assert!(config.enabled);
        assert_eq!(config.item_ttl, Some("30s".to_string()));
        assert_eq!(config.stale_while_revalidate_ttl, Some("60s".to_string()));
        assert_eq!(config.encoding, Encoding::Zstd);
    }

    /// `Query` struct with `spill_compression`.
    #[test]
    fn test_query_spill_compression() {
        let yaml = r"
            memory_limit: 512MiB
            spill_compression: lz4_frame
        ";
        let query: component::runtime::Query = yaml::from_str(yaml).expect("Should parse Query");
        assert_eq!(query.memory_limit, Some("512MiB".to_string()));
        assert_eq!(
            query.spill_compression,
            Some(component::runtime::SpillCompression::Lz4Frame)
        );
    }

    // ========================================================================
    // Cross-version: v1 features still work at v2 version tag
    // ========================================================================

    /// A v2-tagged spicepod can still use deprecated `results_cache` (backward compat).
    #[test]
    fn test_v2_tag_with_deprecated_results_cache() {
        let yaml = r"
            version: v2
            kind: Spicepod
            name: v2_with_old_cache
            runtime:
              results_cache:
                enabled: true
                item_ttl: 5s
        ";
        let def: SpicepodDefinition =
            yaml::from_str(yaml).expect("Should parse v2 with deprecated results_cache");
        assert_eq!(def.version, SpicepodVersion::V2);

        // The RuntimeDeserializer migrates results_cache → caching.sql_results
        let sql = def.runtime.caching.sql_results.expect("should be migrated");
        assert!(sql.enabled);
        assert_eq!(sql.item_ttl, Some("5s".to_string()));
    }

    /// A v2-tagged spicepod can still use `memory_limit` at the runtime level.
    #[test]
    fn test_v2_tag_with_deprecated_memory_limit() {
        let yaml = r"
            version: v2
            kind: Spicepod
            name: v2_with_old_memory
            runtime:
              memory_limit: 256MiB
        ";
        let def: SpicepodDefinition =
            yaml::from_str(yaml).expect("Should parse v2 with deprecated memory_limit");
        assert_eq!(def.version, SpicepodVersion::V2);

        let query = def.runtime.query.expect("query should be migrated");
        assert_eq!(query.memory_limit, Some("256MiB".to_string()));
    }

    // ========================================================================
    // Version roundtrip through serialization → deserialization
    // ========================================================================

    /// A v1 `SpicepodVersion` roundtrips through YAML.
    #[test]
    fn test_v1_version_roundtrip() {
        let original = SpicepodVersion::V1;
        let yaml_str = yaml::to_string(&original).expect("Should serialize SpicepodVersion::V1");
        let roundtripped: SpicepodVersion =
            yaml::from_str(&yaml_str).expect("Should deserialize back to V1");
        assert_eq!(original, roundtripped);
    }

    /// A v2 `SpicepodVersion` roundtrips through YAML.
    #[test]
    fn test_v2_version_roundtrip() {
        let original = SpicepodVersion::V2;
        let yaml_str = yaml::to_string(&original).expect("Should serialize SpicepodVersion::V2");
        let roundtripped: SpicepodVersion =
            yaml::from_str(&yaml_str).expect("Should deserialize back to V2");
        assert_eq!(original, roundtripped);
    }

    // ========================================================================
    // All v1 test fixtures still load (regression guard)
    // ========================================================================

    /// All v1-versioned test spicepods load successfully, proving backward compat.
    #[tokio::test]
    async fn test_all_v1_fixtures_load() {
        const V1_FILES: [&str; 7] = [
            "./tests/basic_spicepod.yaml",
            "./tests/spicepod_with_caching.yaml",
            "./tests/spicepod_with_only_sql_results.yaml",
            "./tests/spicepod_with_only_search_results.yaml",
            "./tests/spicepod_with_results_cache.yaml",
            "./tests/v1_full_featured.yaml",
            "./tests/v1_caching_style.yaml",
        ];

        for file in V1_FILES {
            let pod = Spicepod::load_exact(&PathBuf::from(file))
                .await
                .unwrap_or_else(|e| panic!("Failed to load v1 fixture {file}: {e}"));
            assert_eq!(pod.version, SpicepodVersion::V1, "Expected V1 for {file}");
        }
    }

    /// `load_from` accepts any YAML file path, not just files named "spicepod".
    #[tokio::test]
    async fn test_load_from_accepts_arbitrary_filename() {
        let pod = Spicepod::load_from(&reader::StdFileSystem, "./tests/basic_spicepod.yaml")
            .await
            .expect("load_from should accept a file path with a non-spicepod stem");
        assert_eq!(pod.name, "basic_spicepod");
    }

    /// `load_from` with a directory still finds spicepod.yaml inside it.
    #[tokio::test]
    async fn test_load_from_directory_finds_spicepod_yaml() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let spicepod_content = "version: v1\nkind: Spicepod\nname: test_pod\n";
        std::fs::write(dir.path().join("spicepod.yaml"), spicepod_content)
            .expect("write spicepod.yaml");

        let pod = Spicepod::load_from(&reader::StdFileSystem, dir.path())
            .await
            .expect("load_from should find spicepod.yaml in a directory");
        assert_eq!(pod.name, "test_pod");
    }

    /// A file with a wrong `kind` (e.g. a Kubernetes YAML) is rejected with a clear error.
    #[tokio::test]
    async fn test_load_from_rejects_wrong_kind() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let path = dir.path().join("deployment.yaml");
        std::fs::write(&path, "kind: Deployment\nname: my-app\nversion: v1\n").expect("write file");

        let err = Spicepod::load_from(&reader::StdFileSystem, &path)
            .await
            .expect_err("should reject wrong kind");
        assert!(
            err.to_string().contains("kind: Deployment"),
            "error should mention the wrong kind: {err}"
        );
    }

    /// A file with an unrecognised version is rejected with a clear error.
    #[tokio::test]
    async fn test_load_from_rejects_unknown_version() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let path = dir.path().join("spicepod.yaml");
        std::fs::write(&path, "kind: Spicepod\nname: my-app\nversion: v99\n").expect("write file");

        let err = Spicepod::load_from(&reader::StdFileSystem, &path)
            .await
            .expect_err("should reject unknown version");
        assert!(
            err.to_string().contains("v99"),
            "error should mention the unsupported version: {err}"
        );
    }

    /// All v2-versioned test spicepods load successfully.
    #[tokio::test]
    async fn test_all_v2_fixtures_load() {
        const V2_FILES: [&str; 4] = [
            "./tests/basic_spicepod_v2.yaml",
            "./tests/v2_full_featured.yaml",
            "./tests/v2_caching_style.yaml",
            "./tests/v2_with_scheduler.yaml",
        ];

        for file in V2_FILES {
            let pod = Spicepod::load_exact(&PathBuf::from(file))
                .await
                .unwrap_or_else(|e| panic!("Failed to load v2 fixture {file}: {e}"));
            assert_eq!(pod.version, SpicepodVersion::V2, "Expected V2 for {file}");
        }
    }

    // ========================================================================
    // Specific v1 → v2 field migrations
    // ========================================================================

    /// `results_cache.cache_max_size` → `caching.sql_results.max_size`
    /// Validates the exact field rename mapping in `From<ResultsCache> for SQLResultsCacheConfig`.
    #[test]
    fn test_results_cache_cache_max_size_maps_to_max_size() {
        let yaml = r"
            results_cache:
                enabled: true
                cache_max_size: 128MiB
        ";
        let runtime: Runtime = yaml::from_str(yaml).expect("Should parse Runtime");
        let sql = runtime
            .caching
            .sql_results
            .expect("sql_results must be migrated");
        assert_eq!(
            sql.max_size,
            Some("128MiB".to_string()),
            "v1 `cache_max_size` should migrate to v2 `max_size`"
        );
    }

    /// `results_cache.max_stale_while_revalidate` does NOT carry over to
    /// `caching.sql_results.stale_while_revalidate_ttl`.
    /// The `From<ResultsCache>` impl explicitly drops this field.
    #[test]
    fn test_results_cache_max_stale_while_revalidate_not_migrated() {
        let yaml = r"
            results_cache:
                enabled: true
                max_stale_while_revalidate: 120s
        ";
        let runtime: Runtime = yaml::from_str(yaml).expect("Should parse Runtime");
        let sql = runtime
            .caching
            .sql_results
            .expect("sql_results must be migrated");
        assert_eq!(
            sql.stale_while_revalidate_ttl, None,
            "v1 `max_stale_while_revalidate` should NOT migrate to `stale_while_revalidate_ttl`"
        );
    }

    /// v1 `ResultsCache` has no `encoding` field; the migration defaults to `Encoding::None`.
    #[test]
    fn test_results_cache_migration_encoding_defaults_to_none() {
        let yaml = r"
            results_cache:
                enabled: true
                item_ttl: 10s
                cache_max_size: 64MiB
        ";
        let runtime: Runtime = yaml::from_str(yaml).expect("Should parse Runtime");
        let sql = runtime
            .caching
            .sql_results
            .expect("sql_results must be migrated");
        assert_eq!(
            sql.encoding,
            Encoding::None,
            "Encoding should default to None when migrating from v1 ResultsCache"
        );
    }

    /// Full field-by-field mapping: every v1 `ResultsCache` field maps correctly
    /// to its v2 `SQLResultsCacheConfig` counterpart.
    #[test]
    fn test_results_cache_full_field_mapping() {
        let yaml = r"
            results_cache:
                enabled: true
                cache_max_size: 256MiB
                item_ttl: 45s
                caching_policy: tiny_lfu
                cache_key_type: sql
                hashing_algorithm: blake3
                engine: pingora
                max_stale_while_revalidate: 120s
        ";
        let runtime: Runtime = yaml::from_str(yaml).expect("Should parse Runtime");
        let sql = runtime
            .caching
            .sql_results
            .expect("sql_results must be migrated");

        // Field-by-field migration assertions
        assert!(sql.enabled, "enabled should be preserved");
        assert_eq!(
            sql.max_size,
            Some("256MiB".to_string()),
            "cache_max_size → max_size"
        );
        assert_eq!(sql.item_ttl, Some("45s".to_string()), "item_ttl preserved");
        assert_eq!(
            sql.caching_policy,
            CachingPolicy::TinyLfu,
            "caching_policy preserved"
        );
        assert_eq!(
            sql.cache_key_type,
            CacheKeyType::Sql,
            "cache_key_type preserved"
        );
        assert_eq!(
            sql.hashing_algorithm,
            HashingAlgorithm::Blake3,
            "hashing_algorithm preserved"
        );
        assert_eq!(sql.engine, CacheEngine::Pingora, "engine preserved");
        assert_eq!(
            sql.stale_while_revalidate_ttl, None,
            "max_stale_while_revalidate NOT migrated to stale_while_revalidate_ttl"
        );
        assert_eq!(
            sql.encoding,
            Encoding::None,
            "encoding defaults to None (not available in v1)"
        );
    }

    /// v1 `runtime.memory_limit` → v2 `runtime.query.memory_limit`
    #[test]
    fn test_memory_limit_migrates_to_query() {
        let yaml = r"
            memory_limit: 1GiB
        ";
        let runtime: Runtime = yaml::from_str(yaml).expect("Should parse Runtime");
        let query = runtime.query.expect("query should be migrated");
        assert_eq!(
            query.memory_limit,
            Some("1GiB".to_string()),
            "Top-level `memory_limit` should migrate to `query.memory_limit`"
        );
    }

    /// v1 `runtime.temp_directory` → v2 `runtime.query.temp_directory`
    #[test]
    fn test_temp_directory_migrates_to_query() {
        let yaml = r"
            temp_directory: /tmp/spice_data
        ";
        let runtime: Runtime = yaml::from_str(yaml).expect("Should parse Runtime");
        let query = runtime.query.expect("query should be migrated");
        assert_eq!(
            query.temp_directory,
            Some("/tmp/spice_data".to_string()),
            "Top-level `temp_directory` should migrate to `query.temp_directory`"
        );
    }

    /// v1 `dataset.invalid_type_action` → v2 `dataset.unsupported_type_action`
    #[test]
    fn test_invalid_type_action_migrates_to_unsupported() {
        use crate::component::dataset::{Dataset, UnsupportedTypeAction};

        // v1 style: invalid_type_action
        let yaml = r"
            name: ds1
            from: test
            invalid_type_action: warn
        ";
        let ds: Dataset = yaml::from_str(yaml).expect("Should parse Dataset");
        assert_eq!(
            ds.unsupported_type_action,
            Some(UnsupportedTypeAction::Warn),
            "v1 `invalid_type_action: warn` should migrate to `unsupported_type_action: warn`"
        );
    }

    /// v2 `dataset.unsupported_type_action` takes precedence over v1 `invalid_type_action`.
    #[test]
    fn test_unsupported_type_action_takes_precedence() {
        use crate::component::dataset::{Dataset, UnsupportedTypeAction};

        let yaml = r"
            name: ds1
            from: test
            invalid_type_action: error
            unsupported_type_action: ignore
        ";
        let ds: Dataset = yaml::from_str(yaml).expect("Should parse Dataset");
        assert_eq!(
            ds.unsupported_type_action,
            Some(UnsupportedTypeAction::Ignore),
            "v2 `unsupported_type_action` should take precedence over v1 `invalid_type_action`"
        );
    }

    /// v2 adds `string` variant to `UnsupportedTypeAction` that v1's `InvalidTypeAction` lacks.
    #[test]
    fn test_unsupported_type_action_string_variant_v2_only() {
        use crate::component::dataset::{Dataset, UnsupportedTypeAction};

        let yaml = r"
            name: ds1
            from: test
            unsupported_type_action: string
        ";
        let ds: Dataset = yaml::from_str(yaml).expect("Should parse Dataset");
        assert_eq!(
            ds.unsupported_type_action,
            Some(UnsupportedTypeAction::String),
            "`string` variant is only available via v2 `unsupported_type_action`"
        );
    }

    /// v1 `Scheduler` works with only `state_location` (partition assignment fields default).
    #[test]
    fn test_v1_scheduler_without_partition_assignment() {
        let yaml = r"
            state_location: s3://bucket/state
        ";
        let scheduler: component::runtime::Scheduler =
            yaml::from_str(yaml).expect("Should parse Scheduler");
        assert_eq!(scheduler.state_location, "s3://bucket/state");
        assert_eq!(
            scheduler.max_partitions_per_executor, 1000,
            "partition assignment fields should default when not specified"
        );
    }

    /// v1 `Flight` struct has no `do_put_rate_limit_enabled` field; it defaults to `true`.
    #[test]
    fn test_v1_flight_without_rate_limit_field() {
        // v1 style: only known flight fields, no do_put_rate_limit_enabled
        let yaml = r"
            max_message_size: 16MiB
        ";
        let flight: component::runtime::Flight = yaml::from_str(yaml).expect("Should parse Flight");
        assert!(
            flight.do_put_rate_limit_enabled,
            "v1 Flight should default `do_put_rate_limit_enabled` to true"
        );
    }

    /// v2 `stale_while_revalidate_ttl` is distinct from v1 `max_stale_while_revalidate`.
    /// v2 uses a response directive on `SQLResultsCacheConfig`; v1 had the field on
    /// `ResultsCache` but it does NOT survive migration.
    #[test]
    fn test_v2_stale_while_revalidate_ttl_is_new() {
        // v2 uses `caching.sql_results.stale_while_revalidate_ttl` directly
        let yaml = r"
            caching:
                sql_results:
                    enabled: true
                    stale_while_revalidate_ttl: 45s
        ";
        let runtime: Runtime = yaml::from_str(yaml).expect("Should parse Runtime");
        let sql = runtime.caching.sql_results.expect("sql_results present");
        assert_eq!(
            sql.stale_while_revalidate_ttl,
            Some("45s".to_string()),
            "v2 `stale_while_revalidate_ttl` should be set directly"
        );

        // v1 `max_stale_while_revalidate` on `results_cache` does NOT migrate
        let yaml_v1 = r"
            results_cache:
                enabled: true
                max_stale_while_revalidate: 45s
        ";
        let runtime_v1: Runtime = yaml::from_str(yaml_v1).expect("Should parse Runtime");
        let sql_v1 = runtime_v1
            .caching
            .sql_results
            .expect("sql_results migrated");
        assert_eq!(
            sql_v1.stale_while_revalidate_ttl, None,
            "v1 `max_stale_while_revalidate` does NOT map to `stale_while_revalidate_ttl`"
        );
    }

    /// v2 `encoding` on `SQLResultsCacheConfig` is new; v1 `ResultsCache` has no encoding.
    #[test]
    fn test_v2_encoding_not_available_in_v1() {
        // v2 can set encoding directly
        let yaml_v2 = r"
            caching:
                sql_results:
                    enabled: true
                    encoding: zstd
        ";
        let runtime_v2: Runtime = yaml::from_str(yaml_v2).expect("Should parse Runtime");
        let sql_v2 = runtime_v2.caching.sql_results.expect("sql_results present");
        assert_eq!(
            sql_v2.encoding,
            Encoding::Zstd,
            "v2 supports encoding field"
        );

        // v1 results_cache migration defaults encoding to None
        let yaml_v1 = r"
            results_cache:
                enabled: true
        ";
        let runtime_v1: Runtime = yaml::from_str(yaml_v1).expect("Should parse Runtime");
        let sql_v1 = runtime_v1
            .caching
            .sql_results
            .expect("sql_results migrated");
        assert_eq!(
            sql_v1.encoding,
            Encoding::None,
            "v1 ResultsCache migration defaults encoding to None"
        );
    }

    /// v2 `spill_compression` on `Query` is new; v1 only had `memory_limit`/`temp_directory`.
    #[test]
    fn test_v2_spill_compression_new_field() {
        // v1 style: only memory_limit/temp_directory via migration
        let yaml_v1 = r"
            memory_limit: 512MiB
        ";
        let runtime_v1: Runtime = yaml::from_str(yaml_v1).expect("Should parse Runtime");
        let query_v1 = runtime_v1.query.expect("query exists");
        assert!(
            query_v1.spill_compression.is_none(),
            "v1 has no spill_compression"
        );

        // v2 style: spill_compression is available
        let yaml_v2 = r"
            query:
                memory_limit: 512MiB
                spill_compression: lz4_frame
        ";
        let runtime_v2: Runtime = yaml::from_str(yaml_v2).expect("Should parse Runtime");
        let query_v2 = runtime_v2.query.expect("query exists");
        assert_eq!(
            query_v2.spill_compression,
            Some(component::runtime::SpillCompression::Lz4Frame),
            "v2 supports spill_compression"
        );
    }

    /// v2 `RuntimeReadyState` is new; v1 defaults to `OnLoad`.
    #[test]
    fn test_v2_ready_state_not_in_v1() {
        // v1 has no ready_state, defaults to OnLoad
        let yaml_v1 = "";
        let runtime_v1: Runtime = yaml::from_str(yaml_v1).expect("Should parse Runtime");
        assert_eq!(
            runtime_v1.ready_state,
            RuntimeReadyState::OnLoad,
            "v1 should default to OnLoad"
        );

        // v2 can set on_registration
        let yaml_v2 = r"
            ready_state: on_registration
        ";
        let runtime_v2: Runtime = yaml::from_str(yaml_v2).expect("Should parse Runtime");
        assert_eq!(
            runtime_v2.ready_state,
            RuntimeReadyState::OnRegistration,
            "v2 supports on_registration"
        );
    }

    /// v2 adds `read_write_create` to `AccessMode`; v1 only has `read` and `read_write`.
    #[test]
    fn test_v2_access_mode_read_write_create_new() {
        use crate::component::dataset::Dataset;

        // v1 modes work
        let yaml_v1 = r"
            name: ds1
            from: test
            access: read_write
        ";
        let ds_v1: Dataset = yaml::from_str(yaml_v1).expect("Should parse Dataset");
        assert_eq!(ds_v1.access, AccessMode::ReadWrite);

        // v2 adds read_write_create
        let yaml_v2 = r"
            name: ds1
            from: test
            access: read_write_create
        ";
        let ds_v2: Dataset = yaml::from_str(yaml_v2).expect("Should parse Dataset");
        assert_eq!(
            ds_v2.access,
            AccessMode::ReadWriteCreate,
            "v2 supports read_write_create access mode"
        );
    }
}
