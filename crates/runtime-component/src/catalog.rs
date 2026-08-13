/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

use globset::GlobSet;
use spicepod::component::catalog as spicepod_catalog;
use spicepod::param::Params;
use std::collections::HashMap;

use crate::access::AccessMode;
use crate::dataset::acceleration::Mode;
use crate::find_first_delimiter;

/// Acceleration configuration for an entire catalog. See
/// [`spicepod_catalog::CatalogAcceleration`] for the user-facing schema this
/// mirrors.
#[derive(Debug, Clone, PartialEq)]
pub struct CatalogAcceleration {
    pub engine: CatalogAccelerationEngine,
    pub refresh_mode: CatalogRefreshMode,
    /// Storage mode applied to every table the catalog accelerates. `Memory`
    /// (the default) is not durable across restarts.
    pub mode: Mode,
    /// Engine parameters applied to every table the catalog accelerates.
    pub params: HashMap<String, String>,
}

impl CatalogAcceleration {
    /// Whether the acceleration this catalog applies to its tables survives a
    /// process restart. `Memory` is fully in-RAM and `FileCreate` truncates on
    /// startup, so both start empty every boot -- each table then re-runs its
    /// initial snapshot instead of resuming from the replication slot.
    #[must_use]
    pub fn is_durable(&self) -> bool {
        matches!(self.mode, Mode::File | Mode::FileUpdate)
    }

    /// The dataset-level acceleration every table this catalog accelerates is
    /// configured with: the catalog's engine, storage mode and accelerator params,
    /// on the one refresh mode catalog acceleration supports.
    ///
    /// This is the single mapping from a catalog's acceleration onto the per-table
    /// shape it expands into, so the two consumers cannot drift: the catalog
    /// connector fills in the per-table `primary_key`/`on_conflict` on top of it
    /// before building each dataset, and the runtime builder classifies it for the
    /// Cayenne memory budgets — which therefore budget for exactly the tables the
    /// catalog creates.
    #[must_use]
    pub fn to_dataset_acceleration(&self) -> spicepod::acceleration::Acceleration {
        spicepod::acceleration::Acceleration {
            engine: Some(self.engine.engine_name().to_string()),
            refresh_mode: Some(match self.refresh_mode {
                CatalogRefreshMode::Changes => spicepod::acceleration::RefreshMode::Changes,
            }),
            // Under a file mode each table lands in its own directory beneath the
            // configured `cayenne_file_path`, named for the dataset.
            mode: self.mode.into(),
            params: (!self.params.is_empty()).then(|| Params::from_string_map(self.params.clone())),
            ..spicepod::acceleration::Acceleration::default()
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CatalogAccelerationEngine {
    #[default]
    Cayenne,
}

impl CatalogAccelerationEngine {
    /// The dataset-level `acceleration.engine` name this engine maps to.
    #[must_use]
    pub const fn engine_name(self) -> &'static str {
        match self {
            Self::Cayenne => "cayenne",
        }
    }
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
            mode: acceleration.mode.into(),
            params: acceleration
                .params
                .as_ref()
                .map(Params::as_string_map)
                .unwrap_or_default(),
        }
    }
}

/// Config-only core of a catalog — every declared field of a
/// `runtime::component::catalog::Catalog` except the runtime handles
/// (`app`/`runtime`). The runtime wrapper holds `Self` plus those handles and
/// `Deref`s to it.
#[derive(Clone)]
pub struct CatalogSpec {
    pub provider: String,
    pub catalog_id: Option<String>,
    pub from: String,
    pub name: String,
    pub access: AccessMode,
    pub orig_include: Vec<String>,
    pub include: Option<GlobSet>,
    pub orig_exclude: Vec<String>,
    pub exclude: Option<GlobSet>,
    pub params: HashMap<String, String>,
    pub dataset_params: HashMap<String, String>,
    pub acceleration: Option<CatalogAcceleration>,
}

impl std::fmt::Debug for CatalogSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CatalogSpec")
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
            .finish_non_exhaustive()
    }
}

impl PartialEq for CatalogSpec {
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

impl CatalogSpec {
    /// Returns the catalog provider — the first part of the `from` field before
    /// the first `://`, `:`, or `/`. For `from = "foo:bar"` this is `"foo"`; for
    /// `from = "foo"` it is `"foo"`.
    #[must_use]
    pub fn provider(from: &str) -> &str {
        match find_first_delimiter(from) {
            Some((0, _)) | None => from,
            Some((pos, _)) => &from[..pos],
        }
    }

    /// Returns the catalog id — the part of the `from` field after the first
    /// delimiter. Optional: `None` (use the provider's default catalog) when
    /// `from` has no delimiter. For `from = "foo:bar"` this is `Some("bar")`.
    #[must_use]
    pub fn catalog_id(from: &str) -> Option<&str> {
        match find_first_delimiter(from) {
            Some((pos, len)) => Some(&from[pos + len..]),
            None => None,
        }
    }
}
