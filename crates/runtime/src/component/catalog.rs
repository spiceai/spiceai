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
use crate::Runtime;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Failed to build catalog '{catalog}': required component '{missing_component}' is missing.\nAn unexpected error occurred. Report a bug to request support: https://github.com/spiceai/spiceai/issues"
    ))]
    UnableToBuildCatalog {
        catalog: String,
        missing_component: String,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone)]
pub struct Catalog {
    pub provider: String,
    pub catalog_id: Option<String>,
    pub from: String,
    pub name: String,
    pub(crate) orig_include: Vec<String>,
    pub include: Option<GlobSet>,
    pub params: HashMap<String, String>,
    pub dataset_params: HashMap<String, String>,
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
            .field("orig_include", &self.orig_include)
            .field("include", &self.include)
            .field("params", &self.params)
            .field("dataset_params", &self.dataset_params)
            .field("app", &self.app)
            .finish_non_exhaustive()
    }
}

impl PartialEq for Catalog {
    fn eq(&self, other: &Self) -> bool {
        self.from == other.from
            && self.name == other.name
            && self.orig_include == other.orig_include
            && self.params == other.params
            && self.dataset_params == other.dataset_params
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
    orig_include: Vec<String>,
    pub include: Option<GlobSet>,
    pub params: HashMap<String, String>,
    pub dataset_params: HashMap<String, String>,
    pub app: Option<Arc<App>>,
    pub runtime: Option<Arc<Runtime>>,
}

impl TryFrom<spicepod_catalog::Catalog> for CatalogBuilder {
    type Error = crate::Error;

    fn try_from(catalog: spicepod_catalog::Catalog) -> std::result::Result<Self, Self::Error> {
        let provider = Catalog::provider(&catalog.from);
        let catalog_id = Catalog::catalog_id(&catalog.from).map(String::from);

        let mut globset_opt: Option<GlobSet> = None;
        if !catalog.include.is_empty() {
            let mut globset_builder = GlobSetBuilder::new();
            let include_iter = catalog.include.iter().map(|pattern| {
                Glob::new(pattern).context(crate::InvalidGlobPatternSnafu { pattern })
            });
            for glob in include_iter {
                globset_builder.add(glob?);
            }

            globset_opt = Some(
                globset_builder
                    .build()
                    .context(crate::ErrorConvertingGlobSetToRegexSnafu)?,
            );
        }

        validate_identifier(&catalog.name).context(crate::ComponentSnafu)?;

        Ok(CatalogBuilder {
            provider: provider.to_string(),
            catalog_id,
            from: catalog.from.clone(),
            name: catalog.name,
            orig_include: catalog.include.clone(),
            include: globset_opt,
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
            app: None,
            runtime: None,
        })
    }
}

impl CatalogBuilder {
    #[allow(clippy::result_large_err)]
    pub fn try_new(from: String, name: &str) -> std::result::Result<Self, crate::Error> {
        validate_identifier(name).context(crate::ComponentSnafu)?;

        let provider = Catalog::provider(from.as_str());
        let catalog_id = Catalog::catalog_id(from.as_str()).map(String::from);

        Ok(CatalogBuilder {
            provider: provider.to_string(),
            catalog_id,
            from,
            name: name.to_string(),
            orig_include: Vec::default(),
            include: None,
            params: HashMap::default(),
            dataset_params: HashMap::default(),
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
            catalog: self.name.to_string(),
            missing_component: "app".to_string(),
        })?;
        let runtime = self.runtime.ok_or(Error::UnableToBuildCatalog {
            catalog: self.name.to_string(),
            missing_component: "runtime".to_string(),
        })?;

        let catalog = Catalog {
            provider: self.provider,
            catalog_id: self.catalog_id,
            from: self.from,
            name: self.name,
            orig_include: self.orig_include,
            include: self.include,
            params: self.params,
            dataset_params: self.dataset_params,
            app,
            runtime,
        };

        Ok(catalog)
    }
}
