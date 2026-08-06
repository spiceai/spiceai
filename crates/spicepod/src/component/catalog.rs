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
use crate::{acceleration::Mode, metric::Metrics, param::Params};

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
/// Each table is accelerated according to its source `REPLICA IDENTITY`:
/// `DEFAULT` (primary key) and `USING INDEX` (a nominated unique index)
/// replicate normally, `FULL` replicates but is heavier (logged as a warning).
/// A table with no usable CDC key (`NOTHING`, or `DEFAULT`/`FULL` without a key)
/// is skipped with a warning and left out of the catalog's namespace, rather
/// than failing the whole catalog. Use `include`/`exclude` to narrow scope and
/// suppress the skip warning for tables you'll handle another way (federation
/// and/or a per-dataset `refresh_mode: full`).
///
/// Deliberately excludes per-table-only concepts (`primary_key`,
/// `on_conflict`, `indexes`, per-table overrides) — those remain exclusively
/// on a dataset's own `acceleration` block.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct CatalogAcceleration {
    #[serde(default)]
    pub engine: CatalogAccelerationEngine,

    /// Required and explicit: there is no catalog-level default, since the
    /// only supported value today is `changes` (CDC).
    pub refresh_mode: CatalogRefreshMode,

    /// Storage mode applied to every table this catalog accelerates, with the
    /// same meaning as a dataset's `acceleration.mode`.
    ///
    /// Defaults to `memory`, which is **not durable**: nothing is written to
    /// disk, so the accelerator starts empty on every restart and each table
    /// re-runs its initial snapshot from the source. Use a file mode (with
    /// `params.cayenne_file_path`) to keep the acceleration across restarts and
    /// resume from the replication slot instead of re-snapshotting.
    ///
    /// Not to be confused with `params.cayenne_cdc_durability: memory`, which
    /// keeps a *file-backed* acceleration and only defers its durable write —
    /// CDC changes buffer in RAM but still drain to disk. `mode` decides whether
    /// the acceleration is persisted at all; `cayenne_cdc_durability` decides
    /// when. For RAM-speed writes that survive a restart, use a file `mode`
    /// together with `cayenne_cdc_durability: memory`.
    #[serde(default)]
    pub mode: Mode,

    /// Engine parameters applied to every table this catalog accelerates (e.g.
    /// `cayenne_file_path`), with the same meaning as a dataset's
    /// `acceleration.params`. Each table gets its own subdirectory under a
    /// configured `cayenne_file_path`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params: Option<Params>,
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
        // Back-compat: an acceleration block that names no mode is in-memory,
        // exactly as before `mode` was accepted here.
        assert_eq!(acceleration.mode, Mode::Memory);
        assert_eq!(acceleration.params, None);
    }

    #[test]
    fn test_catalog_acceleration_parses_mode_and_params() {
        let catalog = parse(
            "
                from: pg
                name: my_pg
                acceleration:
                  engine: cayenne
                  refresh_mode: changes
                  mode: file
                  params:
                    cayenne_file_path: /data
            ",
        );
        let acceleration = catalog
            .acceleration
            .expect("acceleration should be present");
        assert_eq!(acceleration.mode, Mode::File);
        assert_eq!(
            acceleration
                .params
                .expect("params should be present")
                .as_string_map()
                .get("cayenne_file_path")
                .map(String::as_str),
            Some("/data")
        );
    }

    #[test]
    fn test_catalog_acceleration_rejects_unknown_mode() {
        let result = yaml::from_str::<Catalog>(
            "
                from: pg
                name: my_pg
                acceleration:
                  refresh_mode: changes
                  mode: on_disk
            ",
        );
        assert!(
            result.is_err(),
            "an unrecognized mode must fail rather than silently fall back to memory"
        );
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
