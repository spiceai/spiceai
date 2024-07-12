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

use globset::{Glob, GlobSet, GlobSetBuilder};
use snafu::prelude::*;
use spicepod::component::{catalog as spicepod_catalog, params::Params};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct Catalog {
    pub provider: String,
    pub catalog_id: Option<String>,
    pub name: String,
    orig_include: Vec<String>,
    pub include: Option<GlobSet>,
    pub params: HashMap<String, String>,
    pub dataset_params: HashMap<String, String>,
}

impl PartialEq for Catalog {
    fn eq(&self, other: &Self) -> bool {
        self.provider == other.provider
            && self.catalog_id == other.catalog_id
            && self.name == other.name
            && self.orig_include == other.orig_include
            && self.params == other.params
            && self.dataset_params == other.dataset_params
    }
}

impl TryFrom<spicepod_catalog::Catalog> for Catalog {
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

        Ok(Catalog {
            provider: provider.to_string(),
            catalog_id,
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
        })
    }
}

impl Catalog {
    pub fn new(from: &str, name: &str) -> Self {
        Catalog {
            provider: Catalog::provider(from).to_string(),
            catalog_id: Catalog::catalog_id(from).map(String::from),
            name: name.into(),
            orig_include: Vec::default(),
            include: None,
            params: HashMap::default(),
            dataset_params: HashMap::default(),
        }
    }

    /// Returns the catalog provider - the first part of the `from` field before the first `:`.
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
        from.split(':').next().unwrap_or(from)
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
        match from.find(':') {
            Some(index) => Some(&from[index + 1..]),
            None => None,
        }
    }
}
