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
//! The table lives under one *root*: `<cayenne_datalake_location>/<dataset>/`
//! when the location is set, and `<runtime.state.location>/cayenne/<dataset>/`
//! otherwise. Under it:
//!
//! ```text
//! catalog/                 the catalog's authoritative copy
//! warm/                    data files as they are written
//! <table>-<table_id>/      data files the datalake tier compacted
//! ```
//!
//! Local disk holds only a replica of the catalog, rebuilt from `catalog/` when
//! it is missing, so a restart on a new volume finds every acknowledged row.

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

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Failed to register dataset '{dataset}' (cayenne): `mode: datalake` stores the table in object storage, but neither `cayenne_datalake_location` nor `runtime.state.location` is set. Set one, or use `mode: file`. See: {DOCS}"
    ))]
    LocationMissing { dataset: String },

    #[snafu(display(
        "Failed to register dataset '{dataset}' (cayenne): `mode: datalake` supports `s3://` locations{state}, not '{location}'. Use an S3 or S3-compatible location. See: {DOCS}",
        state = if *from_state { " and a `file://` `runtime.state.location`" } else { "" }
    ))]
    UnsupportedLocation {
        dataset: String,
        location: String,
        from_state: bool,
    },

    #[snafu(display(
        "Failed to register dataset '{dataset}' (cayenne): the object store for '{location}' could not be built. Check the `cayenne_datalake_s3_*` or `runtime.state.params` credentials. Cause: {source}"
    ))]
    Store {
        dataset: String,
        location: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Failed to register dataset '{dataset}' (cayenne): the object store at '{location}' {reason}, so a `mode: datalake` table could lose writes there. Use a store that enforces If-None-Match and If-Match, such as Amazon S3 or MinIO. See: {DOCS}"
    ))]
    ConditionsNotEnforced {
        dataset: String,
        location: String,
        reason: String,
    },

    #[snafu(display(
        "Failed to register dataset '{dataset}' (cayenne): `mode: datalake` cannot open the existing `mode: file` table in '{}' yet, and would start an empty table beside it. Keep `mode: file`, or remove the existing acceleration first. See: {DOCS}",
        path.display()
    ))]
    ExistingFileTable { dataset: String, path: PathBuf },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Whether `source` is a `mode: datalake` acceleration.
pub(crate) fn is_datalake(source: &dyn AccelerationSource) -> bool {
    source
        .acceleration()
        .is_some_and(|acceleration| acceleration.enabled && acceleration.mode == Mode::Datalake)
}

/// The folder a dataset's table takes under its location.
fn table_folder(source: &dyn AccelerationSource) -> String {
    source.name().to_string().replace(['.', '/'], "_")
}

/// Where a datalake table lives.
#[derive(Debug, Clone)]
pub(crate) struct Root {
    /// The table's root URL, ending in `/`.
    url: String,
    /// The `runtime.state` location the root is under, when it is.
    state: Option<StateLocation>,
}

impl Root {
    /// Resolve the root of `source`'s table.
    ///
    /// # Errors
    ///
    /// Returns an error when no location is configured, or it has a scheme a
    /// datalake table does not support.
    pub(crate) fn resolve(source: &dyn AccelerationSource) -> Result<Self> {
        let dataset = source.name().to_string();
        let folder = table_folder(source);
        if let Some(location) = source
            .acceleration()
            .and_then(|acceleration| acceleration.cayenne_datalake_location())
        {
            if !location.starts_with("s3://") {
                return UnsupportedLocationSnafu {
                    dataset,
                    location,
                    from_state: false,
                }
                .fail();
            }
            return Ok(Self {
                url: runtime_state_folder(location, &folder),
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
                from_state: true,
            }
            .fail();
        }
        let tables = runtime_state_folder(location, runtime_state_location::CAYENNE_STATE_FOLDER);
        Ok(Self {
            url: runtime_state_folder(&tables, &folder),
            state: Some(StateLocation::from(state)),
        })
    }

    /// The table's root URL, ending in `/`.
    pub(crate) fn url(&self) -> &str {
        &self.url
    }

    fn local_path(&self) -> Option<PathBuf> {
        let url = Url::parse(&self.url).ok()?;
        (url.scheme() == "file").then(|| url.to_file_path().ok())?
    }

    /// The table's data directory: a URL in object storage, or a local path
    /// under a `file://` state location.
    pub(crate) fn data_dir(&self) -> String {
        match self.local_path() {
            Some(path) => format!("{}/", path.join("warm").display()),
            None => format!("{}warm/", self.url),
        }
    }
}

/// A datalake table's object stores, ready to open it.
pub(crate) struct Opened {
    /// Where the catalog's authoritative copy lives.
    pub catalog: CatalogLocation,
    /// The store data files are written to, unless they are local.
    pub data_store: Option<cayenne::metadata::ObjectStoreConfig>,
    /// The datalake tier's location, when the table uses the tier.
    pub cold_location: Option<String>,
}

/// The key prefix of `url` within a store rooted at its bucket.
fn key_prefix(url: &str) -> String {
    Url::parse(url)
        .map(|url| url.path().trim_matches('/').to_string())
        .unwrap_or_default()
}

/// Build the stores for `root` and check that they enforce conditional writes.
///
/// # Errors
///
/// Returns an error when a store cannot be built or ignores write conditions.
pub(crate) async fn open(source: &dyn AccelerationSource, root: &Root) -> Result<Opened> {
    let dataset = source.name().to_string();
    let store_error = |source: Box<dyn std::error::Error + Send + Sync>| Error::Store {
        dataset: dataset.clone(),
        location: root.url.clone(),
        source,
    };
    let (store, table_prefix): (Arc<dyn ObjectStore>, String) = match &root.state {
        None => {
            let config = crate::s3::build_datalake_object_store(source, &root.url)
                .await
                .map_err(|e| store_error(Box::new(e)))?
                .ok_or_else(|| store_error("not an s3:// location".into()))?;
            (config.store, key_prefix(&root.url))
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
            (store, format!("{prefix}/{}", table_folder(source)))
        }
    };
    let probe = object_store_occ::probe_conditional_writes(
        store.as_ref(),
        &ObjectPath::from(table_prefix.as_str()),
    )
    .await;
    if let Some(reason) = probe.refusal_reason() {
        return ConditionsNotEnforcedSnafu {
            dataset,
            location: root.url.clone(),
            reason,
        }
        .fail();
    }
    if let Some(reason) = probe.inconclusive_reason() {
        return ConditionsNotEnforcedSnafu {
            dataset,
            location: root.url.clone(),
            reason: format!("could not be checked for conditional writes ({reason})"),
        }
        .fail();
    }

    let catalog = CatalogLocation::new(
        Arc::clone(&store),
        ObjectPath::from(format!("{table_prefix}/catalog")),
        format!("{}catalog/", root.url),
    );
    if root.local_path().is_some() {
        return Ok(Opened {
            catalog,
            data_store: None,
            cold_location: None,
        });
    }
    let data_url = Url::parse(&root.data_dir()).map_err(|e| store_error(Box::new(e)))?;
    // The datalake tier compacts what continuous ingestion appends; a full
    // refresh rewrites the whole table each time, so it keeps its files in
    // `warm/` only.
    let refresh_mode = source.acceleration().and_then(|a| a.refresh_mode);
    let cold_location = matches!(
        refresh_mode,
        Some(RefreshMode::Changes | RefreshMode::Append)
    )
    .then(|| root.url.clone());
    Ok(Opened {
        catalog,
        data_store: Some(cayenne::metadata::ObjectStoreConfig {
            url: data_url,
            store,
        }),
        cold_location,
    })
}

/// The local replica of a datalake table's catalog.
pub(crate) fn replica_path(metadata_dir: &str, source: &dyn AccelerationSource) -> PathBuf {
    PathBuf::from(metadata_dir)
        .join("datalake")
        .join(table_folder(source))
        .join("cayenne.db")
}

/// Refuse a datalake table whose dataset already has a `mode: file` table in
/// the shared catalog at `shared`: opening the datalake table would leave that
/// data stranded beside an empty one.
///
/// # Errors
///
/// Returns [`Error::ExistingFileTable`] when the shared catalog holds a table
/// for the dataset.
pub(crate) async fn ensure_no_file_table(
    source: &dyn AccelerationSource,
    shared: PathBuf,
) -> Result<()> {
    let dataset = source.name().to_string();
    let table = dataset.clone();
    let path = shared.clone();
    let exists = tokio::task::spawn_blocking(move || {
        if !path.exists() {
            return false;
        }
        rusqlite::Connection::open_with_flags(&path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .and_then(|conn| {
                conn.query_row(
                    "SELECT EXISTS (SELECT 1 FROM cayenne_table WHERE table_name = ?1)",
                    [table],
                    |row| row.get::<_, bool>(0),
                )
            })
            .unwrap_or(false)
    })
    .await
    .unwrap_or(false);
    if exists {
        return ExistingFileTableSnafu {
            dataset,
            path: shared,
        }
        .fail();
    }
    Ok(())
}
