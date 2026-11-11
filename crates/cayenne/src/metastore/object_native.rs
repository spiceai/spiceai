// Copyright 2024-2026 The Spice.ai OSS Authors
/*
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

//! SQL-free catalog backed by conditional-write head logs: one for the name
//! registry and one per dataset.

use super::state_reader::DatasetStateReader;
use crate::catalog::{CatalogError, CatalogResult};
use cayenne_metastore_actions::{Action, DatasetState, Kind, Record, proto};
use domain::replay_state;
use object_store_occ::headlog::{HeadLogConfig, Writer};
use slot::StateSlot;

mod catalog;
mod domain;
mod domain_catalog;
mod publish;
mod registry;
mod slot;
mod transactions;
pub use registry::{ObjectNativeCatalog, dataset_log_at};

/// One head log: a serial writer and its last acknowledged state. The catalog
/// keeps one for its name registry and one per dataset. No database is used.
struct Domain {
    inner: StateSlot,
    opened: OpenSummary,
    /// The dataset folder this log is in; empty for the name registry.
    folder: String,
}

/// What opening the catalog found, for the startup log.
#[derive(Clone, Copy, Debug)]
struct OpenSummary {
    replayed: usize,
    lease: u64,
}
struct Inner {
    reader: DatasetStateReader,
    writer: Writer,
    poisoned: bool,
    config: NativeCatalogConfig,
    /// The state with every record up to the last checkpoint this catalog
    /// attempted (or opened from) applied, and the records committed after it,
    /// from which the next checkpoint root is built.
    floor_state: DatasetState,
    since_floor: Vec<(u64, Vec<Record>)>,
    /// The commit of the last checkpoint staged; its floor publishes with the next commit.
    staged_checkpoint: u64,
    /// Changes accepted and visible on this node but not yet committed, one
    /// buffer per set of tables they touch, oldest first. A buffer commits as one
    /// record at an acknowledgement point for one of its tables, or with the
    /// next change to one of them made outside a deferred scope; another table's
    /// commit never publishes it.
    pending: Vec<Pending>,
    /// Commit every change when it is made, even inside a deferred scope.
    immediate: bool,
}

/// Buffered changes for a set of tables. Actions on different tables commute,
/// so buffers can commit in any order and the log still replays to the state
/// this node applied. A record that touches several tables keeps them in one
/// buffer, so it still commits whole.
struct Pending {
    /// Table ids, and `""` for changes no table owns.
    tables: std::collections::BTreeSet<String>,
    actions: Vec<Action>,
}

/// Head-log tuning for a native catalog.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NativeCatalogConfig {
    /// The log's budgets; every writer of one log must use the same values.
    pub log: HeadLogConfig,
    /// Sealed batches listed in the head before the catalog checkpoints at the
    /// newest one.
    pub checkpoint_after_batches: usize,
    /// How long checkpoints keep the evidence that resolves an uncertain commit.
    pub attempt_horizon: std::time::Duration,
    /// How long a transaction's records, a dataset log no name refers to, or a
    /// dropped dataset's log are kept before a catalog that opens may remove
    /// them: anything newer may belong to a process still writing it.
    pub reclaim_after: std::time::Duration,
    /// How long the coordinator of a transaction across datasets tries to
    /// commit it before it aborts it instead, so its client gets an answer and
    /// its datasets accept other changes again.
    pub transaction_deadline: std::time::Duration,
}

impl Default for NativeCatalogConfig {
    fn default() -> Self {
        Self {
            log: HeadLogConfig::default(),
            // Small for the prototype, so short CDC runs checkpoint.
            checkpoint_after_batches: 1,
            attempt_horizon: object_store_occ::headlog::DEFAULT_ATTEMPT_HORIZON,
            reclaim_after: std::time::Duration::from_hours(1),
            transaction_deadline: std::time::Duration::from_secs(60),
        }
    }
}
/// What a sidecar write does to the value under its key.
enum ValueChange {
    Keep,
    Set(Vec<u8>),
    Remove,
}

/// Set (`Some`) or remove (`None`) the value under `(source, partition)`.
fn position(source: &str, partition: &str, value: Option<Vec<u8>>) -> Action {
    Action::new(Kind::AdvanceSourcePosition(proto::AdvanceSourcePosition {
        source: source.to_string(),
        partition: partition.to_string(),
        clear: value.is_none(),
        position: value.unwrap_or_default(),
    }))
}
fn invalid(error: impl std::fmt::Display) -> CatalogError {
    CatalogError::InvalidOperationNoSource {
        message: error.to_string(),
    }
}
fn validate_uuid(name: &str, value: &str) -> CatalogResult<()> {
    uuid::Uuid::parse_str(value)
        .map_err(|_| invalid(format!("{name} is not a valid UUID: {value}")))?;
    Ok(())
}
