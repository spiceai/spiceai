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

//! `mode: datalake`: a Cayenne table kept whole in object storage.
//!
//! Every `mode: datalake` dataset in a location shares one catalog, as
//! `mode: file` datasets share one `cayenne.db`, so a transaction across them
//! commits atomically. The location is `<runtime.state.location>/cayenne/`, or
//! a deprecated `cayenne_datalake_location`. Under it:
//!
//! ```text
//! catalog/                         the catalog's authoritative copy
//! <dataset>/<table_id>/            the dataset's data files
//! <dataset>/<table>-<table_id>/    data files the datalake tier compacted
//! ```
//!
//! Local disk holds only a replica of the catalog, rebuilt from `catalog/` when
//! it is missing, so a restart on a new volume finds every acknowledged row.
//! The location's `_spice/format.json` versions this layout: the state
//! location's own, or one beside a deprecated `cayenne_datalake_location`.

use std::path::PathBuf;
use std::sync::Arc;

use cayenne::metastore::durable::CatalogLocation;
use object_store::ObjectStore;
use object_store::path::Path as ObjectPath;
use runtime_acceleration::acceleration::{Mode, RefreshMode};
use runtime_acceleration::acceleration_source::AccelerationSource;
use snafu::Snafu;
use spicepod::component::runtime::{StateLocation, runtime_state_folder};
use url::Url;

const DOCS: &str = "https://spiceai.org/docs/components/data-accelerators/cayenne";

/// The folder of a location's catalog.
const CATALOG_FOLDER: &str = "catalog";

/// The folder of a location's format record, as in a `runtime.state` location.
const FORMAT_FOLDER: &str = "_spice";

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Failed to register dataset '{dataset}' (cayenne): `mode: datalake` stores the table in `runtime.state.location`, which is not set. Set it, or use `mode: file`. See: {DOCS}"
    ))]
    LocationMissing { dataset: String },

    #[snafu(display(
        "Failed to register dataset '{dataset}' (cayenne): `mode: datalake` supports {supported} locations, not '{location}'. Use {usable}. See: {DOCS}",
        supported = if *deprecated { "`s3://` `cayenne_datalake_location`" } else { "`s3://` and `file://` `runtime.state.location`" },
        usable = if *deprecated { "an S3 or S3-compatible location" } else { "an S3, S3-compatible or local location" },
    ))]
    UnsupportedLocation {
        dataset: String,
        location: String,
        deprecated: bool,
    },

    #[snafu(display(
        "Failed to register dataset '{dataset}' (cayenne): the object store for '{location}' could not be built. Check the {credentials} credentials. Cause: {source}",
        credentials = if *deprecated { "`cayenne_datalake_s3_*`" } else { "`runtime.state.params`" },
    ))]
    Store {
        dataset: String,
        location: String,
        deprecated: bool,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to register dataset '{dataset}' (cayenne): {source}"))]
    Check {
        dataset: String,
        source: runtime_state_location::CheckError,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Whether `source` is a `mode: datalake` acceleration.
pub(crate) fn is_datalake(source: &dyn AccelerationSource) -> bool {
    source
        .acceleration()
        .is_some_and(|acceleration| acceleration.enabled && acceleration.mode == Mode::Datalake)
}

/// The folder a dataset's data takes under its location.
fn dataset_folder(source: &dyn AccelerationSource) -> String {
    source.name().to_string().replace(['.', '/'], "_")
}

/// The location a datalake table lives in, shared by every dataset that
/// resolves to it.
#[derive(Debug, Clone)]
pub(crate) struct Location {
    /// The location's URL, ending in `/`.
    url: String,
    /// The `runtime.state` location this is the `cayenne/` folder of, unless
    /// it is a deprecated `cayenne_datalake_location`.
    state: Option<StateLocation>,
}

impl Location {
    /// Resolve the location of `source`'s table.
    ///
    /// # Errors
    ///
    /// Returns an error when no location is configured, or it has a scheme a
    /// datalake table does not support.
    pub(crate) fn resolve(source: &dyn AccelerationSource) -> Result<Self> {
        let dataset = source.name().to_string();
        if let Some(location) = source
            .acceleration()
            .and_then(|acceleration| acceleration.cayenne_datalake_location())
        {
            if !location.starts_with("s3://") {
                return UnsupportedLocationSnafu {
                    dataset,
                    location,
                    deprecated: true,
                }
                .fail();
            }
            return Ok(Self {
                url: format!("{}/", location.trim_end_matches('/')),
                state: None,
            });
        }
        let app = source.app();
        let Some(state) = app.runtime.state.as_ref() else {
            return LocationMissingSnafu { dataset }.fail();
        };
        let location = state.location.as_str();
        if !(location.starts_with("s3://") || location.starts_with("file://")) {
            return UnsupportedLocationSnafu {
                dataset,
                location,
                deprecated: false,
            }
            .fail();
        }
        Ok(Self {
            url: runtime_state_folder(location, runtime_state_location::CAYENNE_STATE_FOLDER),
            state: Some(StateLocation::from(state)),
        })
    }

    /// The location's URL, ending in `/`; it identifies the catalog.
    pub(crate) fn url(&self) -> &str {
        &self.url
    }

    fn local_path(&self) -> Option<PathBuf> {
        let url = Url::parse(&self.url).ok()?;
        (url.scheme() == "file").then(|| url.to_file_path().ok())?
    }

    /// The URL of `source`'s folder, ending in `/`.
    fn dataset_url(&self, source: &dyn AccelerationSource) -> String {
        runtime_state_folder(&self.url, &dataset_folder(source))
    }

    /// `source`'s data directory: a URL in object storage, or a local path
    /// under a `file://` state location.
    pub(crate) fn data_dir(&self, source: &dyn AccelerationSource) -> String {
        match self.local_path() {
            Some(path) => format!("{}/", path.join(dataset_folder(source)).display()),
            None => self.dataset_url(source),
        }
    }

    /// The local replica of the location's catalog, under `metadata_dir`.
    pub(crate) fn replica_path(&self, metadata_dir: &str) -> PathBuf {
        PathBuf::from(metadata_dir)
            .join("datalake")
            .join(format!("{:016x}", fnv1a(self.url.as_bytes())))
            .join("cayenne.db")
    }
}

/// FNV-1a, a stable hash for naming a location's local folder.
fn fnv1a(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x0100_0000_01b3)
    })
}

/// A datalake location's object store, checked and ready to open its catalog.
#[derive(Clone)]
pub(crate) struct Opened {
    pub location: Location,
    /// Where the catalog's authoritative copy lives.
    pub catalog: CatalogLocation,
    /// The store data files are written to, unless they are local.
    store: Option<Arc<dyn ObjectStore>>,
}

impl Opened {
    /// The store `source`'s data files are written to, unless they are local.
    pub(crate) fn data_store(
        &self,
        source: &dyn AccelerationSource,
    ) -> Option<cayenne::metadata::ObjectStoreConfig> {
        let store = self.store.as_ref()?;
        Some(cayenne::metadata::ObjectStoreConfig {
            url: Url::parse(&self.location.dataset_url(source)).ok()?,
            store: Arc::clone(store),
        })
    }

    /// The datalake tier's location for `source`, when the table uses the
    /// tier. The tier compacts what continuous ingestion appends; a full
    /// refresh rewrites the whole table each time, so its files stay where
    /// they are written.
    pub(crate) fn cold_location(&self, source: &dyn AccelerationSource) -> Option<String> {
        self.store.as_ref()?;
        let refresh_mode = source.acceleration().and_then(|a| a.refresh_mode);
        matches!(
            refresh_mode,
            Some(RefreshMode::Changes | RefreshMode::Append)
        )
        .then(|| self.location.dataset_url(source))
    }
}

/// The key prefix of `url` within a store rooted at its bucket.
fn key_prefix(url: &str) -> String {
    Url::parse(url)
        .map(|url| url.path().trim_matches('/').to_string())
        .unwrap_or_default()
}

/// The prefix of `folder` beside `prefix`, its sibling in the same parent.
fn sibling(prefix: &str, folder: &str) -> String {
    match prefix.rsplit_once('/') {
        Some((parent, _)) => format!("{parent}/{folder}"),
        None => folder.to_string(),
    }
}

/// Build the store for `location` and check it: its format version is one
/// this build supports, and it enforces conditional writes.
///
/// # Errors
///
/// Returns an error when the store cannot be built, or fails the check.
pub(crate) async fn open(source: &dyn AccelerationSource, location: Location) -> Result<Opened> {
    let dataset = source.name().to_string();
    let deprecated = location.state.is_none();
    let store_error = |source: Box<dyn std::error::Error + Send + Sync>| Error::Store {
        dataset: dataset.clone(),
        location: location.url.clone(),
        deprecated,
        source,
    };
    // The store is rooted above the location: `prefix` is the location's own
    // key prefix in it, and `format` the prefix of its `_spice/` folder.
    let (store, prefix, format, checked): (Arc<dyn ObjectStore>, String, String, String) =
        match &location.state {
            None => {
                let config = crate::s3::build_datalake_object_store(source, &location.url)
                    .await
                    .map_err(|e| store_error(Box::new(e)))?
                    .ok_or_else(|| store_error("not an s3:// location".into()))?;
                let prefix = key_prefix(&location.url);
                let format = if prefix.is_empty() {
                    FORMAT_FOLDER.to_string()
                } else {
                    format!("{prefix}/{FORMAT_FOLDER}")
                };
                (config.store, prefix, format, location.url.clone())
            }
            Some(state) => {
                let (store, prefix) = runtime_state_location::build_object_store(
                    source.secrets(),
                    tokio::runtime::Handle::current(),
                    state,
                    runtime_state_location::StateFeature::Cayenne,
                )
                .await
                .map_err(|e| store_error(Box::new(e)))?;
                let format = sibling(&prefix, FORMAT_FOLDER);
                (store, prefix, format, state.location.clone())
            }
        };
    runtime_state_location::check_location(
        store.as_ref(),
        &ObjectPath::from(format.as_str()),
        &checked,
    )
    .await
    .map_err(|source| Error::Check {
        dataset: dataset.clone(),
        source,
    })?;

    let catalog = CatalogLocation::new(
        Arc::clone(&store),
        ObjectPath::from(format!("{prefix}/{CATALOG_FOLDER}")),
        runtime_state_folder(&location.url, CATALOG_FOLDER),
    );
    let store = location.local_path().is_none().then_some(store);
    Ok(Opened {
        location,
        catalog,
        store,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_folder_is_beside_the_feature_folder() {
        assert_eq!(sibling("spice/cayenne", FORMAT_FOLDER), "spice/_spice");
        assert_eq!(sibling("cayenne", FORMAT_FOLDER), "_spice");
        assert_eq!(sibling("a/b/cayenne", FORMAT_FOLDER), "a/b/_spice");
    }

    #[test]
    fn replica_folder_is_stable_per_location() {
        let at = |url: &str| {
            Location {
                url: url.to_string(),
                state: None,
            }
            .replica_path("/meta")
        };
        // A changed name would only cost a restore from object storage, but
        // the folder must differ between locations and not between runs.
        assert_eq!(
            at("s3://acme/spice/cayenne/"),
            PathBuf::from("/meta/datalake/0a3c494104ea56dd/cayenne.db")
        );
        assert_ne!(at("s3://acme/a_b/"), at("s3://acme/a/b/"));
    }
}
