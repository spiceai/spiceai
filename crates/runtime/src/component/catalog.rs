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
use data_components::catalog_filter::TableSelector;
use globset::{Glob, GlobSet, GlobSetBuilder};
use snafu::prelude::*;
use spicepod::{component::catalog as spicepod_catalog, param::Params};
use std::ops::{Deref, DerefMut};
use std::{collections::HashMap, sync::Arc};

use crate::Runtime;
use crate::component::access::AccessMode;

// Config-only spec + config types live in `runtime-component`; re-export for
// path compatibility (`crate::component::catalog::CatalogSpec`, etc.).
pub use runtime_component::catalog::{
    CatalogAcceleration, CatalogAccelerationEngine, CatalogRefreshMode, CatalogSpec,
};

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

/// `Arc<Runtime>`-bound wrapper over a [`CatalogSpec`]. Derefs to the spec so
/// `catalog.provider`, `catalog.acceleration`, etc. keep working unchanged.
#[derive(Clone)]
pub struct Catalog {
    pub spec: CatalogSpec,
    pub app: Arc<App>,
    pub runtime: Arc<Runtime>,
}

impl Deref for Catalog {
    type Target = CatalogSpec;

    fn deref(&self) -> &Self::Target {
        &self.spec
    }
}

impl DerefMut for Catalog {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.spec
    }
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
        self.spec == other.spec
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
}

/// Which of the catalog's discovered tables it registers.
///
/// Every catalog connector resolves the configuration through here rather than
/// reading `include` directly, so a connector cannot apply one half of it and
/// silently drop the other -- which is what left `exclude` ignored by all but
/// the `PostgreSQL` connectors (#12636).
///
/// Takes the spec so a `&Catalog` coerces, and so the compiled patterns can be
/// tested without the `app`/`runtime` a built [`Catalog`] also carries.
#[must_use]
pub fn table_selector(catalog: &CatalogSpec) -> TableSelector {
    TableSelector::new(catalog.include.clone(), catalog.exclude.clone())
        .with_include_patterns(&catalog.orig_include)
        .with_exclude_patterns(&catalog.orig_exclude)
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
        let provider = CatalogSpec::provider(&catalog.from);
        let catalog_id = CatalogSpec::catalog_id(&catalog.from).map(String::from);

        let include = compile_globset(&catalog.include)?;
        let exclude = compile_globset(&catalog.exclude)?;

        crate::component::validate_identifier(&catalog.name).context(crate::ComponentSnafu)?;

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
        // Feature-gated: without the `postgres` feature the `pg` catalog
        // connector isn't compiled in at all, so no provider supports catalog
        // acceleration and any `acceleration` config is rejected.
        #[cfg(feature = "postgres")]
        let acceleration_supported = provider == crate::catalogconnector::postgres::PREFIX;
        #[cfg(not(feature = "postgres"))]
        let acceleration_supported = false;

        if catalog.acceleration.is_some() && !acceleration_supported {
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
        crate::component::validate_identifier(name).context(crate::ComponentSnafu)?;

        if name.eq_ignore_ascii_case(crate::datafusion::SPICE_DEFAULT_CATALOG) {
            return Err(crate::Error::ComponentError {
                source: super::Error::ReservedCatalogName {
                    name: name.to_string(),
                },
            });
        }

        let provider = CatalogSpec::provider(from.as_str());
        let catalog_id = CatalogSpec::catalog_id(from.as_str()).map(String::from);

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

    pub fn build(mut self) -> Result<Catalog> {
        let app = self.app.take().ok_or(Error::UnableToBuildCatalog {
            catalog: self.name.clone(),
            missing_component: "app".to_string(),
        })?;
        let runtime = self.runtime.take().ok_or(Error::UnableToBuildCatalog {
            catalog: self.name.clone(),
            missing_component: "runtime".to_string(),
        })?;

        let catalog = Catalog {
            spec: self.into_spec(),
            app,
            runtime,
        };

        Ok(catalog)
    }

    /// The configuration half of the catalog, without the `app`/`runtime` a
    /// fully built [`Catalog`] also carries.
    fn into_spec(self) -> CatalogSpec {
        CatalogSpec {
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
        }
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

    /// The end of the chain the `exclude` field travels: a spicepod `exclude:`
    /// is compiled into a `GlobSet` and must reach the selector every catalog
    /// connector filters through. Before #12636 the compiled set was built and
    /// then read by nobody but the `PostgreSQL` connectors, so an excluded
    /// table was silently registered.
    #[test]
    fn test_table_selector_carries_both_include_and_exclude() {
        let selector = table_selector(
            &CatalogBuilder::try_from(spicepod_catalog(&["public.*"], &["public.audit_log"], None))
                .expect("should build")
                .into_spec(),
        );

        assert!(selector.selects_table("public", "orders"));
        assert!(
            !selector.selects_table("public", "audit_log"),
            "an excluded table must not be selected"
        );
        assert!(!selector.selects_table("reporting", "orders"));

        // The raw patterns travel too, and by a separate path: `selects_table`
        // reads the compiled sets, so dropping either `with_*_patterns` call
        // leaves every assertion above passing while a diagnostic naming the
        // configuration silently omits half of it.
        let described = selector.describe();
        assert!(
            described.contains("include: ['public.*']"),
            "the include patterns should reach the selector verbatim: {described}"
        );
        assert!(
            described.contains("exclude: ['public.audit_log']"),
            "the exclude patterns should reach the selector verbatim: {described}"
        );
    }

    /// An `exclude` with no `include` still withholds: the connectors that only
    /// ever consulted `include` treated this configuration as selecting
    /// everything.
    #[test]
    fn test_table_selector_honors_exclude_without_include() {
        let selector = table_selector(
            &CatalogBuilder::try_from(spicepod_catalog(&[], &["private.*"], None))
                .expect("should build")
                .into_spec(),
        );

        assert!(selector.selects_table("public", "orders"));
        assert!(!selector.selects_table("private", "secrets"));
    }

    #[test]
    fn test_table_selector_selects_everything_when_unconfigured() {
        let selector = table_selector(
            &CatalogBuilder::try_from(spicepod_catalog(&[], &[], None))
                .expect("should build")
                .into_spec(),
        );

        assert!(selector.selects_table("public", "orders"));
        assert!(selector.selects_table("private", "secrets"));
    }

    fn cayenne_file_acceleration() -> spicepod_catalog::CatalogAcceleration {
        spicepod_catalog::CatalogAcceleration {
            engine: spicepod_catalog::CatalogAccelerationEngine::Cayenne,
            refresh_mode: spicepod_catalog::CatalogRefreshMode::Changes,
            mode: spicepod::acceleration::Mode::File,
            params: Some(spicepod::param::Params::from_string_map(
                [("cayenne_file_path".to_string(), "/data".to_string())].into(),
            )),
        }
    }

    // Catalog acceleration is only supported for the `pg` provider, which
    // exists only under the `postgres` feature. Each arm of that conditional
    // gets its own test so the suite passes -- and asserts the behaviour the
    // build actually has -- in both configurations.
    #[cfg(feature = "postgres")]
    #[test]
    fn test_try_from_maps_acceleration() {
        let builder = CatalogBuilder::try_from(spicepod_catalog(
            &[],
            &[],
            Some(cayenne_file_acceleration()),
        ))
        .expect("should build");

        let mapped = builder.acceleration.expect("acceleration should be mapped");
        assert_eq!(mapped.engine, CatalogAccelerationEngine::Cayenne);
        assert_eq!(mapped.refresh_mode, CatalogRefreshMode::Changes);
        // `mode` and `params` must survive the mapping -- dropping either would
        // silently downgrade a durable catalog acceleration to in-memory.
        assert_eq!(
            mapped.mode,
            runtime_component::dataset::acceleration::Mode::File
        );
        assert_eq!(
            mapped.params.get("cayenne_file_path").map(String::as_str),
            Some("/data")
        );
        assert!(mapped.is_durable());
    }

    /// The dataset acceleration a catalog converts into. Two consumers depend on
    /// this being exactly what the accelerated tables are configured with: the
    /// catalog connector, which adds only the per-table key on top of it, and the
    /// runtime builder's Cayenne memory budgets, which classify it (#13013).
    #[test]
    fn test_catalog_acceleration_converts_to_the_dataset_acceleration() {
        let acceleration = CatalogAcceleration::from(cayenne_file_acceleration());

        let converted = acceleration.to_dataset_acceleration();

        assert!(converted.enabled);
        assert_eq!(converted.engine.as_deref(), Some("cayenne"));
        assert_eq!(
            converted.refresh_mode,
            Some(spicepod::acceleration::RefreshMode::Changes)
        );
        assert_eq!(converted.mode, spicepod::acceleration::Mode::File);
        assert_eq!(
            converted
                .params
                .as_ref()
                .map(spicepod::param::Params::as_string_map)
                .unwrap_or_default()
                .get("cayenne_file_path")
                .map(String::as_str),
            Some("/data")
        );
        // The per-table key is the connector's to fill in; the catalog schema has no
        // place to declare one, so leaving a stale value here would key every table
        // on the same columns.
        assert_eq!(converted.primary_key, None);
        assert!(converted.on_conflict.is_empty());

        // Params are omitted entirely rather than serialized as an empty block, so an
        // converted acceleration matches a hand-written one with no `params`.
        let no_params = CatalogAcceleration::from(spicepod_catalog::CatalogAcceleration {
            params: None,
            ..cayenne_file_acceleration()
        });
        assert_eq!(no_params.to_dataset_acceleration().params, None);
    }

    // Without `postgres` there is no `pg` catalog connector compiled in, so no
    // provider supports catalog acceleration and the config is rejected rather
    // than silently no-oping.
    #[cfg(not(feature = "postgres"))]
    #[test]
    fn test_try_from_rejects_acceleration_without_postgres_feature() {
        // `CatalogBuilder` is not `Debug`; drop the success value so the
        // failure message can render the error.
        let result = CatalogBuilder::try_from(spicepod_catalog(
            &[],
            &[],
            Some(cayenne_file_acceleration()),
        ))
        .map(|_| ());

        assert!(
            matches!(
                result,
                Err(crate::Error::ComponentError {
                    source: crate::component::Error::CatalogAccelerationUnsupportedProvider { .. }
                })
            ),
            "a 'pg' catalog acceleration must be rejected when the postgres feature is off, got: {result:?}"
        );
    }

    #[test]
    fn test_try_from_rejects_acceleration_for_unsupported_provider() {
        let mut catalog = spicepod_catalog(&[], &[], Some(cayenne_file_acceleration()));
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
