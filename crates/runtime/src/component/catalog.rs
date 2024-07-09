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

use datafusion::sql::TableReference;
use spicepod::component::{catalog as spicepod_catalog, params::Params};
use std::collections::HashMap;

use super::dataset::Dataset;

#[derive(Debug, Clone, PartialEq)]
pub struct Catalog {
    pub provider: String,
    pub provider_path: Option<String>,
    pub name: TableReference,
    pub params: HashMap<String, String>,
    pub dataset_params: HashMap<String, String>,
}

impl TryFrom<spicepod_catalog::Catalog> for Catalog {
    type Error = crate::Error;

    fn try_from(catalog: spicepod_catalog::Catalog) -> std::result::Result<Self, Self::Error> {
        let table_reference = Dataset::parse_table_reference(&catalog.name)?;
        let provider = Catalog::provider(&catalog.from);
        let provider_path = Catalog::provider_path(&catalog.from).map(String::from);

        Ok(Catalog {
            provider: provider.to_string(),
            provider_path,
            name: table_reference,
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
    pub fn try_new(from: &str, name: &str) -> std::result::Result<Self, crate::Error> {
        Ok(Catalog {
            provider: Catalog::provider(from).to_string(),
            provider_path: Catalog::provider_path(from).map(String::from),
            name: Dataset::parse_table_reference(name)?,
            params: HashMap::default(),
            dataset_params: HashMap::default(),
        })
    }

    /// Returns the catalog provider - the first part of the `from` field before the first `:`.
    ///
    /// # Examples
    ///
    /// ```
    /// use runtime::component::catalog::Catalog;
    ///
    /// let catalog = Catalog::try_new("foo:bar", "bar").expect("valid catalog");
    ///
    /// assert_eq!(catalog.provider, "foo".to_string());
    /// ```
    ///
    /// ```
    /// use runtime::component::catalog::Catalog;
    ///
    /// let catalog = Catalog::try_new("foo", "bar").expect("valid catalog");
    ///
    /// assert_eq!(catalog.provider, "foo".to_string());
    /// ```
    #[must_use]
    fn provider(from: &str) -> &str {
        from.split(':').next().unwrap_or(from)
    }

    /// Returns the catalog provider path - the second part of the `from` field after the first `:`.
    ///
    /// # Examples
    ///
    /// ```
    /// use runtime::component::catalog::Catalog;
    ///
    /// let catalog = Catalog::try_new("foo:bar", "bar").expect("valid catalog");
    ///
    /// assert_eq!(catalog.provider_path, Some("bar".to_string()));
    /// ```
    ///
    /// ```
    /// use runtime::component::catalog::Catalog;
    ///
    /// let catalog = Catalog::try_new("foo", "bar").expect("valid catalog");
    ///
    /// assert_eq!(catalog.provider_path, None);
    /// ```
    #[must_use]
    fn provider_path(from: &str) -> Option<&str> {
        match from.find(':') {
            Some(index) => Some(&from[index + 1..]),
            None => None,
        }
    }
}
