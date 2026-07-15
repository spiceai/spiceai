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

use std::collections::HashMap;

#[cfg(feature = "schemars")]
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{WithDependsOn, access::AccessMode, is_default};
use crate::{metric::Metrics, param::Params};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct Catalog {
    pub from: String,

    pub name: String,

    pub description: Option<String>,

    #[serde(skip_serializing_if = "HashMap::is_empty")]
    #[serde(default)]
    pub metadata: HashMap<String, Value>,

    #[serde(default, skip_serializing_if = "is_default")]
    pub access: AccessMode,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub include: Vec<String>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub exclude: Vec<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params: Option<Params>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dataset_params: Option<Params>,

    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(rename = "dependsOn", default)]
    pub depends_on: Vec<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metrics: Option<Metrics>,

    /// Automatically bootstrap and accelerate every table discovered by this
    /// catalog, with zero per-table configuration. Only `refresh_mode: changes`
    /// (CDC) is supported today; there is no catalog-level `full` mode.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acceleration: Option<CatalogAcceleration>,
}

impl Catalog {
    #[must_use]
    pub fn new(from: String, name: String) -> Self {
        Catalog {
            from,
            name,
            description: None,
            metadata: HashMap::default(),
            access: AccessMode::default(),
            include: Vec::default(),
            exclude: Vec::default(),
            params: None,
            dataset_params: None,
            depends_on: Vec::default(),
            metrics: None,
            acceleration: None,
        }
    }

    #[must_use]
    pub fn with_access(mut self, access: AccessMode) -> Self {
        self.access = access;
        self
    }
}

impl WithDependsOn<Catalog> for Catalog {
    fn depends_on(&self, depends_on: &[String]) -> Catalog {
        Catalog {
            from: self.from.clone(),
            name: self.name.clone(),
            description: self.description.clone(),
            metadata: self.metadata.clone(),
            access: self.access.clone(),
            include: self.include.clone(),
            exclude: self.exclude.clone(),
            params: self.params.clone(),
            dataset_params: self.dataset_params.clone(),
            depends_on: depends_on.to_vec(),
            metrics: self.metrics.clone(),
            acceleration: self.acceleration.clone(),
        }
    }
}

/// Acceleration configuration for an entire [`Catalog`]: bootstraps and
/// CDC-accelerates every table the catalog connector discovers (subject to
/// `include`/`exclude`), without per-table configuration.
///
/// Every included table must have a primary key — catalog setup fails
/// naming any table that doesn't, rather than silently skipping it or
/// falling back to a heavier access pattern. Use `include`/`exclude` to keep
/// tables without a primary key out of an accelerated catalog's scope.
///
/// Deliberately excludes per-table-only concepts (`primary_key`,
/// `on_conflict`, `indexes`, per-table overrides) — those remain exclusively
/// on a dataset's own `acceleration` block.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct CatalogAcceleration {
    #[serde(default)]
    pub engine: CatalogAccelerationEngine,

    /// Required and explicit: there is no catalog-level default, since the
    /// only supported value today is `changes` (CDC).
    pub refresh_mode: CatalogRefreshMode,
}

/// Accelerator engine used for catalog-wide acceleration. Only `cayenne` is
/// supported today.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum CatalogAccelerationEngine {
    #[default]
    Cayenne,
}

/// Refresh mode for catalog-wide acceleration. Only CDC (`changes`) is
/// supported today; there is no catalog-level `full` mode.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum CatalogRefreshMode {
    Changes,
}

#[cfg(test)]
mod tests {
    use super::*;
    use yaml;

    fn parse(yaml_str: &str) -> Catalog {
        yaml::from_str(yaml_str).expect("Failed to parse Catalog")
    }

    #[test]
    fn test_catalog_without_acceleration_defaults_to_none() {
        let catalog = parse(
            "
                from: pg
                name: my_pg
            ",
        );
        assert_eq!(catalog.acceleration, None);
        assert!(catalog.exclude.is_empty());
    }

    #[test]
    fn test_catalog_acceleration_minimal_config_defaults() {
        let catalog = parse(
            "
                from: pg
                name: my_pg
                acceleration:
                  refresh_mode: changes
            ",
        );
        let acceleration = catalog
            .acceleration
            .expect("acceleration should be present");
        assert_eq!(acceleration.engine, CatalogAccelerationEngine::Cayenne);
        assert_eq!(acceleration.refresh_mode, CatalogRefreshMode::Changes);
    }

    #[test]
    fn test_catalog_acceleration_requires_explicit_refresh_mode() {
        let result = yaml::from_str::<Catalog>(
            "
                from: pg
                name: my_pg
                acceleration: {}
            ",
        );
        assert!(
            result.is_err(),
            "refresh_mode must be required, not defaulted"
        );
    }

    #[test]
    fn test_catalog_acceleration_rejects_unknown_engine() {
        let result = yaml::from_str::<Catalog>(
            "
                from: pg
                name: my_pg
                acceleration:
                  engine: duckdb
                  refresh_mode: changes
            ",
        );
        assert!(result.is_err(), "only `cayenne` is a supported engine");
    }

    #[test]
    fn test_catalog_acceleration_rejects_full_refresh_mode() {
        let result = yaml::from_str::<Catalog>(
            "
                from: pg
                name: my_pg
                acceleration:
                  refresh_mode: full
            ",
        );
        assert!(
            result.is_err(),
            "full refresh is out of scope for catalog-level acceleration"
        );
    }

    #[test]
    fn test_catalog_acceleration_rejects_unknown_field() {
        let result = yaml::from_str::<Catalog>(
            "
                from: pg
                name: my_pg
                acceleration:
                  refresh_mode: changes
                  on_missing_primary_key: skip
            ",
        );
        assert!(
            result.is_err(),
            "on_missing_primary_key was removed; missing a primary key is always an error"
        );
    }

    #[test]
    fn test_catalog_include_and_exclude_parse() {
        let catalog = parse(
            "
                from: pg
                name: my_pg
                include:
                  - 'public.*'
                exclude:
                  - 'private.*'
            ",
        );
        assert_eq!(catalog.include, vec!["public.*".to_string()]);
        assert_eq!(catalog.exclude, vec!["private.*".to_string()]);
    }
}
