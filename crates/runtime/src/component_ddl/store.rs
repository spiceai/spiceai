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

//! Durable storage for the datasets and catalogs defined with SQL.
//!
//! The definitions are one JSON document, `sql-definitions/definitions.json`, in the
//! runtime's state location: `runtime.state.location` when it is set, which a
//! cluster shares, and otherwise the instance directory (`.spice`, or
//! `$SPICE_CONFIG_DIR`), which one runtime owns. Every write is conditional on
//! the version that was read, so two writers never overwrite each other.

use std::path::Path;
use std::sync::Arc;

use object_store_occ::{Change, ConflictRetry, ModifyError, ObjectState};
use runtime_state_location::{StateFeature, StateLocationCheck, build_object_store};
use serde::{Deserialize, Serialize};
use snafu::prelude::*;
use spicepod::component::catalog::Catalog;
use spicepod::component::dataset::Dataset;
use spicepod::component::runtime::{RuntimeState, StateLocation, StateSource};
use tokio::runtime::Handle;
use tokio::sync::{Mutex, RwLock};

use crate::secrets::Secrets;

/// The object key of the definitions document.
const DEFINITIONS_KEY: &str = "definitions";

/// The datasets and catalogs defined with SQL, in their Spicepod form.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub(crate) struct SqlDefinitions {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub datasets: Vec<Dataset>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub catalogs: Vec<Catalog>,
}

impl SqlDefinitions {
    pub fn is_empty(&self) -> bool {
        self.datasets.is_empty() && self.catalogs.is_empty()
    }

    /// The definitions as the store reads them back, through JSON. Served and
    /// compared in this form, what a read of the store gives equals what was
    /// written, whatever a field's serialization keeps.
    pub fn as_stored(&self) -> serde_json::Result<Self> {
        serde_json::from_slice(&serde_json::to_vec(self)?)
    }
}

/// A failure of the definitions store, written as the clause a message about
/// the statement or the instance it stops goes on to explain.
#[derive(Debug, Snafu)]
pub(crate) enum Error {
    #[snafu(display(
        "the store of the datasets and catalogs defined with SQL at '{location}' could not be opened. Cause: {}",
        root_cause(source.as_ref())
    ))]
    Open {
        location: String,
        #[snafu(source(from(runtime_state_location::Error, Box::new)))]
        source: Box<runtime_state_location::Error>,
    },

    #[snafu(display(
        "the datasets and catalogs defined with SQL could not be read from '{location}'. Cause: {}",
        root_cause(source.as_ref())
    ))]
    Read {
        location: String,
        #[snafu(source(from(object_store_occ::Error, Box::new)))]
        source: Box<object_store_occ::Error>,
    },

    #[snafu(display(
        "the datasets and catalogs defined with SQL could not be saved to '{location}'; check that Spice can write to it, then run the statement again. Cause: {}",
        root_cause(source.as_ref())
    ))]
    Write {
        location: String,
        source: Box<object_store_occ::Error>,
    },

    #[snafu(display(
        "the datasets and catalogs defined with SQL at '{location}' changed while this statement ran. Run it again."
    ))]
    Conflict { location: String },
}

impl Error {
    /// Where the definitions are kept.
    pub fn location(&self) -> &str {
        match self {
            Self::Open { location, .. }
            | Self::Read { location, .. }
            | Self::Write { location, .. }
            | Self::Conflict { location } => location,
        }
    }

    /// Whether the definitions could not be read.
    pub fn is_unreadable(&self) -> bool {
        matches!(self, Self::Read { .. })
    }

    /// What failed, without the layers that passed it on.
    pub fn cause(&self) -> String {
        match self {
            Self::Open { source, .. } => root_cause(source.as_ref()),
            Self::Read { source, .. } | Self::Write { source, .. } => root_cause(source.as_ref()),
            Self::Conflict { .. } => "another instance changed them".to_string(),
        }
    }
}

/// The innermost error of `error`'s chain: what failed, without the layers
/// that passed it on, such as `Object store operation 'get' failed`. On one
/// line, since messages and log lines embed it, and an object store's error
/// can span several.
fn root_cause(error: &(dyn std::error::Error + 'static)) -> String {
    let mut cause = error;
    while let Some(source) = cause.source() {
        cause = source;
    }
    util::single_line(&cause.to_string()).into_owned()
}

pub(crate) type Result<T, E = Error> = std::result::Result<T, E>;

/// The durable store of [`SqlDefinitions`].
pub(crate) struct DefinitionsStore {
    state: ObjectState<SqlDefinitions>,
    /// Where the document is kept, for messages.
    location: String,
    /// Whether the document is in `runtime.state`, which other instances may
    /// share.
    shared: bool,
    /// Serializes this process's writes, so a write never races another of its
    /// own and conflicts only with a different writer.
    write_lock: Mutex<()>,
}

impl std::fmt::Debug for DefinitionsStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DefinitionsStore")
            .field("location", &self.location)
            .finish_non_exhaustive()
    }
}

impl DefinitionsStore {
    /// Opens the store in `runtime_state` when it is set, and otherwise in
    /// `instance_dir`.
    pub async fn open(
        runtime_state: Option<&RuntimeState>,
        instance_dir: &Path,
        state_location_check: Option<&Arc<StateLocationCheck>>,
        secrets: Arc<RwLock<Secrets>>,
        io_runtime: Handle,
    ) -> Result<Self> {
        let (state, guarded) = match runtime_state {
            Some(state) => (StateLocation::from(state), true),
            None => (
                StateLocation {
                    location: instance_dir_url(instance_dir),
                    params: None,
                    source: StateSource::RuntimeState,
                },
                false,
            ),
        };
        let (store, prefix) =
            build_object_store(secrets, io_runtime, &state, StateFeature::SqlDefinitions)
                .await
                .context(OpenSnafu {
                    location: state.location.clone(),
                })?;
        let store = match state_location_check.filter(|_| guarded) {
            Some(check) => check.guard(StateFeature::SqlDefinitions, store),
            None => store,
        };
        if !guarded {
            restrict_instance_folder(&state.location, &prefix).await;
        }
        let location = format!(
            "{}/{prefix}/{DEFINITIONS_KEY}.json",
            state.location.trim_end_matches('/')
        );
        Ok(Self {
            state: ObjectState::new(store).with_prefix(format!("{prefix}/")),
            location,
            shared: guarded,
            write_lock: Mutex::new(()),
        })
    }

    /// Where the document is kept.
    pub fn location(&self) -> &str {
        &self.location
    }

    /// Whether the document is in `runtime.state`, where another instance that
    /// shares the location may change it.
    pub fn is_shared(&self) -> bool {
        self.shared
    }

    /// The stored definitions, or none when nothing was ever defined.
    pub async fn load(&self) -> Result<SqlDefinitions> {
        Ok(self
            .state
            .get(DEFINITIONS_KEY)
            .await
            .context(ReadSnafu {
                location: self.location.clone(),
            })?
            .unwrap_or_default())
    }

    /// Replace the stored definitions with `next`, if they are still
    /// `expected`: a different writer's change in between is a [`Error::Conflict`],
    /// never overwritten.
    pub async fn replace(&self, expected: &SqlDefinitions, next: &SqlDefinitions) -> Result<()> {
        let _write = self.write_lock.lock().await;
        // A conflict is not retried: the change was computed from `expected`,
        // and applying it to someone else's definitions would be a guess.
        let once = ConflictRetry {
            max_attempts: 1,
            ..ConflictRetry::default()
        };
        let outcome = self
            .state
            .modify(DEFINITIONS_KEY, &once, |stored: Option<SqlDefinitions>| {
                if stored.unwrap_or_default() == *expected {
                    Ok(Change::Write(next.clone(), ()))
                } else {
                    Err(StoredChanged)
                }
            })
            .await;
        match outcome {
            Ok(()) => Ok(()),
            Err(ModifyError::Rejected(StoredChanged) | ModifyError::Contention { .. }) => {
                ConflictSnafu {
                    location: self.location.clone(),
                }
                .fail()
            }
            Err(ModifyError::State(source)) => Err(Error::Write {
                location: self.location.clone(),
                source: Box::new(source),
            }),
        }
    }
}

/// The stored definitions differ from the ones a change was computed from.
#[derive(Debug)]
struct StoredChanged;

impl std::fmt::Display for StoredChanged {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("the stored definitions changed")
    }
}

impl std::error::Error for StoredChanged {}

/// Make the definitions' folder in the instance directory readable by its owner
/// only: the definitions keep their parameters as written, which can be
/// credentials. A `runtime.state` location, which instances may share, keeps
/// the permissions its operator gave it.
#[cfg(unix)]
async fn restrict_instance_folder(location: &str, prefix: &str) {
    use std::os::unix::fs::PermissionsExt;
    let Some(base) = url::Url::parse(location)
        .ok()
        .filter(|url| url.scheme() == "file")
        .and_then(|url| url.to_file_path().ok())
    else {
        return;
    };
    let folder = base.join(prefix);
    let restricted = async {
        tokio::fs::create_dir_all(&folder).await?;
        tokio::fs::set_permissions(&folder, std::fs::Permissions::from_mode(0o700)).await
    };
    if let Err(e) = restricted.await {
        tracing::warn!(
            "Failed to make '{}' readable by its owner only, so other users of this machine may read the datasets and catalogs defined with SQL there, and any credential written in them. Cause: {e}",
            folder.display()
        );
    }
}

#[cfg(not(unix))]
async fn restrict_instance_folder(_location: &str, _prefix: &str) {}

/// A `file://` URL for the instance directory.
fn instance_dir_url(instance_dir: &Path) -> String {
    let absolute = if instance_dir.is_absolute() {
        instance_dir.to_path_buf()
    } else {
        std::env::current_dir()
            .map_or_else(|_| instance_dir.to_path_buf(), |cwd| cwd.join(instance_dir))
    };
    url::Url::from_directory_path(&absolute).map_or_else(
        |()| format!("file://{}", absolute.display()),
        |url| url.to_string(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dataset(name: &str) -> Dataset {
        Dataset::new("file:data.csv", name)
    }

    async fn open(dir: &Path) -> DefinitionsStore {
        DefinitionsStore::open(
            None,
            dir,
            None,
            Arc::new(RwLock::new(Secrets::default())),
            Handle::current(),
        )
        .await
        .expect("store opens")
    }

    /// Definitions written by one store are read back by another over the same
    /// directory, as a restarted runtime reads them, from `sql-definitions/definitions.json`.
    #[tokio::test]
    async fn definitions_survive_reopening_the_store() {
        let dir = tempfile::tempdir().expect("temp dir");
        let store = open(dir.path()).await;
        assert_eq!(store.load().await.expect("load"), SqlDefinitions::default());

        let defined = SqlDefinitions {
            datasets: vec![dataset("orders")],
            catalogs: vec![Catalog::new("iceberg".to_string(), "lake".to_string())],
        };
        store
            .replace(&SqlDefinitions::default(), &defined)
            .await
            .expect("first write");
        assert!(
            dir.path()
                .join("sql-definitions")
                .join("definitions.json")
                .is_file()
        );

        let reopened = open(dir.path()).await;
        assert_eq!(reopened.load().await.expect("load"), defined);
    }

    /// A change computed from definitions another writer has since changed is
    /// refused, and the other writer's change is kept.
    #[tokio::test]
    async fn a_change_from_stale_definitions_is_refused() {
        let dir = tempfile::tempdir().expect("temp dir");
        let first = open(dir.path()).await;
        let second = open(dir.path()).await;

        let orders = SqlDefinitions {
            datasets: vec![dataset("orders")],
            ..SqlDefinitions::default()
        };
        first
            .replace(&SqlDefinitions::default(), &orders)
            .await
            .expect("first writer");

        let customers = SqlDefinitions {
            datasets: vec![dataset("customers")],
            ..SqlDefinitions::default()
        };
        let err = second
            .replace(&SqlDefinitions::default(), &customers)
            .await
            .expect_err("stale writer");
        assert!(matches!(err, Error::Conflict { .. }), "{err}");
        assert_eq!(second.load().await.expect("load"), orders);
    }

    /// A message embeds the innermost error of a chain, without the layers
    /// that passed it on, and on one line, though the store wrote it over two.
    #[test]
    fn a_root_cause_is_the_innermost_error_on_one_line() {
        #[derive(Debug, Snafu)]
        #[snafu(display("Object store operation 'get' failed: {source}"))]
        struct Get {
            source: std::io::Error,
        }
        let error = Get {
            source: std::io::Error::other("Access Denied\nRequest ID: 4442587FB7D0A2F9"),
        };
        assert_eq!(
            root_cause(&error),
            "Access Denied Request ID: 4442587FB7D0A2F9"
        );
    }
}
