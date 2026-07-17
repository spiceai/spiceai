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

use app::App;
use globset::{Glob, GlobSet, GlobSetBuilder};
use snafu::prelude::*;
use spicepod::{component::catalog as spicepod_catalog, param::Params};
use std::{collections::HashMap, sync::Arc};

use super::{find_first_delimiter, validate_identifier};
use crate::{Runtime, component::access::AccessMode};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Failed to build catalog '{catalog}': required component '{missing_component}' is missing. An unexpected error occurred. Report a bug to request support: https://github.com/spiceai/spiceai/issues"
    ))]
    UnableToBuildCatalog {
        catalog: String,
        missing_component: String,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

/// Acceleration configuration for an entire [`Catalog`]. See
/// [`spicepod_catalog::CatalogAcceleration`] for the user-facing schema this
/// mirrors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CatalogAcceleration {
    pub engine: CatalogAccelerationEngine,
    pub refresh_mode: CatalogRefreshMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CatalogAccelerationEngine {
    #[default]
    Cayenne,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogRefreshMode {
    Changes,
}

impl From<spicepod_catalog::CatalogAccelerationEngine> for CatalogAccelerationEngine {
    fn from(engine: spicepod_catalog::CatalogAccelerationEngine) -> Self {
        match engine {
            spicepod_catalog::CatalogAccelerationEngine::Cayenne => {
                CatalogAccelerationEngine::Cayenne
            }
        }
    }
}

impl From<spicepod_catalog::CatalogRefreshMode> for CatalogRefreshMode {
    fn from(refresh_mode: spicepod_catalog::CatalogRefreshMode) -> Self {
        match refresh_mode {
            spicepod_catalog::CatalogRefreshMode::Changes => CatalogRefreshMode::Changes,
        }
    }
}

impl From<spicepod_catalog::CatalogAcceleration> for CatalogAcceleration {
    fn from(acceleration: spicepod_catalog::CatalogAcceleration) -> Self {
        CatalogAcceleration {
            engine: acceleration.engine.into(),
            refresh_mode: acceleration.refresh_mode.into(),
        }
    }
}

#[derive(Clone)]
pub struct Catalog {
    pub provider: String,
    pub catalog_id: Option<String>,
    pub from: String,
    pub name: String,
    pub access: AccessMode,
    pub(crate) orig_include: Vec<String>,
    pub include: Option<GlobSet>,
    pub(crate) orig_exclude: Vec<String>,
    pub exclude: Option<GlobSet>,
    pub params: HashMap<String, String>,
    pub dataset_params: HashMap<String, String>,
    pub acceleration: Option<CatalogAcceleration>,
    pub app: Arc<App>,
    pub runtime: Arc<Runtime>,
}

impl std::fmt::Debug for Catalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Catalog")
            .field("provider", &self.provider)
            .field("catalog_id", &self.catalog_id)
            .field("from", &self.from)
            .field("name", &self.name)
            .field("access", &self.access)
            .field("orig_include", &self.orig_include)
            .field("include", &self.include)
            .field("orig_exclude", &self.orig_exclude)
            .field("exclude", &self.exclude)
            .field("params", &self.params)
            .field("dataset_params", &self.dataset_params)
            .field("acceleration", &self.acceleration)
            .field("app", &self.app)
            .finish_non_exhaustive()
    }
}

impl PartialEq for Catalog {
    fn eq(&self, other: &Self) -> bool {
        self.from == other.from
            && self.name == other.name
            && self.access == other.access
            && self.orig_include == other.orig_include
            && self.orig_exclude == other.orig_exclude
            && self.params == other.params
            && self.dataset_params == other.dataset_params
            && self.acceleration == other.acceleration
    }
}

impl Catalog {
    #[must_use]
    pub fn app(&self) -> Arc<App> {
        Arc::clone(&self.app)
    }

    #[must_use]
    pub fn runtime(&self) -> Arc<Runtime> {
        Arc::clone(&self.runtime)
    }

    /// Returns the catalog provider - the first part of the `from` field before the first '://', ':', or '/'.
    ///
    /// # Examples
    ///
    /// ```
    /// use runtime::component::catalog::Catalog;
    ///
    /// let catalog = Catalog::new("foo:bar", "bar");
    ///
    /// assert_eq!(catalog.provider, "foo".to_string());
    /// ```
    ///
    /// ```
    /// use runtime::component::catalog::Catalog;
    ///
    /// let catalog = Catalog::new("foo", "bar");
    ///
    /// assert_eq!(catalog.provider, "foo".to_string());
    /// ```
    #[must_use]
    fn provider(from: &str) -> &str {
        match find_first_delimiter(from) {
            Some((0, _)) | None => from,
            Some((pos, _)) => &from[..pos],
        }
    }

    /// Returns the catalog id - the second part of the `from` field after the first `:`.
    /// This is optional and will return the default catalog from the provider if not set.
    ///
    /// # Examples
    ///
    /// ```
    /// use runtime::component::catalog::Catalog;
    ///
    /// let catalog = Catalog::new("foo:bar", "bar");
    ///
    /// assert_eq!(catalog.catalog_id, Some("bar".to_string()));
    /// ```
    ///
    /// ```
    /// use runtime::component::catalog::Catalog;
    ///
    /// let catalog = Catalog::new("foo", "bar");
    ///
    /// assert_eq!(catalog.catalog_id, None);
    /// ```
    #[must_use]
    fn catalog_id(from: &str) -> Option<&str> {
        match find_first_delimiter(from) {
            Some((pos, len)) => Some(&from[pos + len..]),
            None => None,
        }
    }
}

pub struct CatalogBuilder {
    pub provider: String,
    pub catalog_id: Option<String>,
    pub from: String,
    pub name: String,
    pub access: AccessMode,
    orig_include: Vec<String>,
    pub include: Option<GlobSet>,
    orig_exclude: Vec<String>,
    pub exclude: Option<GlobSet>,
    pub params: HashMap<String, String>,
    pub dataset_params: HashMap<String, String>,
    pub acceleration: Option<CatalogAcceleration>,
    pub app: Option<Arc<App>>,
    pub runtime: Option<Arc<Runtime>>,
}

#[expect(clippy::result_large_err)]
fn compile_globset(patterns: &[String]) -> std::result::Result<Option<GlobSet>, crate::Error> {
    if patterns.is_empty() {
        return Ok(None);
    }

    let mut globset_builder = GlobSetBuilder::new();
    for pattern in patterns {
        let glob = Glob::new(pattern).context(crate::InvalidGlobPatternSnafu { pattern })?;
        globset_builder.add(glob);
    }

    Ok(Some(
        globset_builder
            .build()
            .context(crate::ErrorConvertingGlobSetToRegexSnafu)?,
    ))
}

impl TryFrom<spicepod_catalog::Catalog> for CatalogBuilder {
    type Error = crate::Error;

    fn try_from(catalog: spicepod_catalog::Catalog) -> std::result::Result<Self, Self::Error> {
        let provider = Catalog::provider(&catalog.from);
        let catalog_id = Catalog::catalog_id(&catalog.from).map(String::from);

        let include = compile_globset(&catalog.include)?;
        let exclude = compile_globset(&catalog.exclude)?;

        validate_identifier(&catalog.name).context(crate::ComponentSnafu)?;

        if catalog
            .name
            .eq_ignore_ascii_case(crate::datafusion::SPICE_DEFAULT_CATALOG)
        {
            return Err(crate::Error::ComponentError {
                source: super::Error::ReservedCatalogName {
                    name: catalog.name.clone(),
                },
            });
        }

        // Catalog-level acceleration is only implemented for the `pg`
        // provider (see `catalogconnector::postgres_accelerated`) -- every
        // other provider's connector ignores `catalog.acceleration`
        // entirely, which would otherwise silently no-op a user's config.
        if catalog.acceleration.is_some() && provider != crate::catalogconnector::postgres::PREFIX {
            return Err(crate::Error::ComponentError {
                source: super::Error::CatalogAccelerationUnsupportedProvider {
                    name: catalog.name.clone(),
                    provider: provider.to_string(),
                },
            });
        }

        Ok(CatalogBuilder {
            provider: provider.to_string(),
            catalog_id,
            from: catalog.from.clone(),
            name: catalog.name,
            access: AccessMode::from(catalog.access),
            orig_include: catalog.include.clone(),
            include,
            orig_exclude: catalog.exclude.clone(),
            exclude,
            params: catalog
                .params
                .as_ref()
                .map(Params::as_string_map)
                .unwrap_or_default(),
            dataset_params: catalog
                .dataset_params
                .as_ref()
                .map(Params::as_string_map)
                .unwrap_or_default(),
            acceleration: catalog.acceleration.map(CatalogAcceleration::from),
            app: None,
            runtime: None,
        })
    }
}

impl CatalogBuilder {
    #[expect(clippy::result_large_err)]
    pub fn try_new(from: String, name: &str) -> std::result::Result<Self, crate::Error> {
        validate_identifier(name).context(crate::ComponentSnafu)?;

        if name.eq_ignore_ascii_case(crate::datafusion::SPICE_DEFAULT_CATALOG) {
            return Err(crate::Error::ComponentError {
                source: super::Error::ReservedCatalogName {
                    name: name.to_string(),
                },
            });
        }

        let provider = Catalog::provider(from.as_str());
        let catalog_id = Catalog::catalog_id(from.as_str()).map(String::from);

        Ok(CatalogBuilder {
            provider: provider.to_string(),
            catalog_id,
            from,
            name: name.to_string(),
            access: AccessMode::default(),
            orig_include: Vec::default(),
            include: None,
            orig_exclude: Vec::default(),
            exclude: None,
            params: HashMap::default(),
            dataset_params: HashMap::default(),
            acceleration: None,
            app: None,
            runtime: None,
        })
    }

    #[must_use]
    pub fn with_app(mut self, app: Arc<App>) -> Self {
        self.app = Some(app);
        self
    }

    #[must_use]
    pub fn with_runtime(mut self, runtime: Arc<Runtime>) -> Self {
        self.runtime = Some(runtime);
        self
    }

    pub fn build(self) -> Result<Catalog> {
        let app = self.app.ok_or(Error::UnableToBuildCatalog {
            catalog: self.name.clone(),
            missing_component: "app".to_string(),
        })?;
        let runtime = self.runtime.ok_or(Error::UnableToBuildCatalog {
            catalog: self.name.clone(),
            missing_component: "runtime".to_string(),
        })?;

        let catalog = Catalog {
            provider: self.provider,
            catalog_id: self.catalog_id,
            from: self.from,
            name: self.name,
            access: self.access,
            orig_include: self.orig_include,
            include: self.include,
            orig_exclude: self.orig_exclude,
            exclude: self.exclude,
            params: self.params,
            dataset_params: self.dataset_params,
            acceleration: self.acceleration,
            app,
            runtime,
        };

        Ok(catalog)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spicepod_catalog(
        include: &[&str],
        exclude: &[&str],
        acceleration: Option<spicepod_catalog::CatalogAcceleration>,
    ) -> spicepod_catalog::Catalog {
        spicepod_catalog::Catalog {
            from: "pg".to_string(),
            name: "my_pg".to_string(),
            description: None,
            metadata: HashMap::default(),
            access: spicepod::component::access::AccessMode::default(),
            include: include.iter().map(ToString::to_string).collect(),
            exclude: exclude.iter().map(ToString::to_string).collect(),
            params: None,
            dataset_params: None,
            depends_on: Vec::default(),
            metrics: None,
            acceleration,
        }
    }

    #[test]
    fn test_try_from_without_acceleration() {
        let builder =
            CatalogBuilder::try_from(spicepod_catalog(&[], &[], None)).expect("should build");
        assert_eq!(builder.acceleration, None);
        assert!(builder.include.is_none());
        assert!(builder.exclude.is_none());
    }

    #[test]
    fn test_try_from_compiles_include_and_exclude_globsets() {
        let builder =
            CatalogBuilder::try_from(spicepod_catalog(&["public.*"], &["private.*"], None))
                .expect("should build");

        let include = builder.include.expect("include globset should be built");
        assert!(include.is_match("public.orders"));
        assert!(!include.is_match("private.secrets"));

        let exclude = builder.exclude.expect("exclude globset should be built");
        assert!(exclude.is_match("private.secrets"));
        assert!(!exclude.is_match("public.orders"));
    }

    #[test]
    fn test_try_from_maps_acceleration() {
        let acceleration = spicepod_catalog::CatalogAcceleration {
            engine: spicepod_catalog::CatalogAccelerationEngine::Cayenne,
            refresh_mode: spicepod_catalog::CatalogRefreshMode::Changes,
        };

        let builder = CatalogBuilder::try_from(spicepod_catalog(&[], &[], Some(acceleration)))
            .expect("should build");

        let mapped = builder.acceleration.expect("acceleration should be mapped");
        assert_eq!(mapped.engine, CatalogAccelerationEngine::Cayenne);
        assert_eq!(mapped.refresh_mode, CatalogRefreshMode::Changes);
    }

    #[test]
    fn test_try_from_rejects_acceleration_for_unsupported_provider() {
        let acceleration = spicepod_catalog::CatalogAcceleration {
            engine: spicepod_catalog::CatalogAccelerationEngine::Cayenne,
            refresh_mode: spicepod_catalog::CatalogRefreshMode::Changes,
        };
        let mut catalog = spicepod_catalog(&[], &[], Some(acceleration));
        catalog.from = "mysql".to_string();

        let result = CatalogBuilder::try_from(catalog);
        assert!(
            result.is_err(),
            "acceleration on a provider other than 'pg' should be rejected, not silently ignored"
        );
    }

    #[test]
    fn test_try_from_rejects_invalid_glob_pattern() {
        let result = CatalogBuilder::try_from(spicepod_catalog(&["["], &[], None));
        assert!(result.is_err(), "malformed glob pattern should fail");
    }
}
