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
use object_store::path::Path;
use object_store_occ::headlog::{Collector, HeadLogConfig, Writer};
use slot::StateSlot;
use std::sync::Arc;
mod catalog;
mod domain;
mod domain_catalog;
mod publish;
mod registry;
mod slot;
mod transactions;
pub use registry::{ObjectNativeCatalog, TableDeletionHolds, dataset_log_at, new_dataset_folder};

/// One head log: a serial writer and its last acknowledged state. The catalog
/// keeps one for its name registry and one per dataset. No database is used.
struct Domain {
    inner: Arc<StateSlot>,
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
    /// Why this log accepts no change, when it does not.
    stop: Option<Stop>,
    /// A background task is resolving [`Stop::Unresolved`] or reloading after
    /// [`Stop::Fenced`].
    recovering: bool,
    /// The dataset this log holds, for what it reports; empty for the registry.
    dataset: Arc<str>,
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
    /// Deletes the log's objects below its floor, off the commit path: at
    /// most one collection runs at a time, started after a checkpoint.
    collector: Arc<tokio::sync::Mutex<Collector>>,
}

/// Why a log accepts no change.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Stop {
    /// A commit's outcome is not known after its retries. Its changes are
    /// applied here but may not be in the log, so neither changes nor reads are
    /// served until a background task reopens the log and resumes from what
    /// committed.
    Unresolved,
    /// Another instance took the log over. Writes stop; reads serve the
    /// committed state once it is reloaded (`reloaded`).
    Fenced { reloaded: bool },
    /// The state refused a change this catalog made, which is a bug: the state
    /// no longer matches the log, so the log stops until the process restarts.
    Refused,
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
    /// How long a log's batches, spilled records and checkpoints stay after a
    /// newer checkpoint's floor passes them, so a reader of an older head can
    /// finish; a reader that takes longer opens again from the newer head.
    pub retain_below_floor: std::time::Duration,
    /// How long a transaction's records, a dataset log no name refers to, or a
    /// dropped dataset's log are kept before a catalog that opens may remove
    /// them: anything newer may belong to a process still writing it.
    pub reclaim_after: std::time::Duration,
    /// How long after a dataset is dropped the instance that dropped it waits
    /// before it deletes the dataset's folder, once no restore point or export
    /// holds its tables' deletions. Running scans do not extend this retention.
    pub dropped_grace: std::time::Duration,
    /// How long the coordinator of a transaction across datasets tries to
    /// commit it before it aborts it instead, so its client gets an answer and
    /// its datasets accept other changes again.
    pub transaction_deadline: std::time::Duration,
    /// How many times a commit whose outcome is not known is resolved and
    /// retried before the log reopens in the background instead.
    pub publish_attempts: u32,
    /// The first wait between those retries, and between background reopen
    /// attempts; each later wait doubles, up to 64 times this.
    pub retry_delay: std::time::Duration,
    /// Commit each change to a dataset's log as its own record when it is
    /// staged, even inside a deferred scope, instead of at the scope's
    /// acknowledgement point. Then every staging call is one record, so a test
    /// can count how many calls an operation stages its changes in. `false` in
    /// production: a CDC apply's changes and its source position share a record.
    pub commit_each_change: bool,
}

impl Default for NativeCatalogConfig {
    fn default() -> Self {
        Self {
            log: HeadLogConfig::default(),
            // Small for the prototype, so short CDC runs checkpoint.
            checkpoint_after_batches: 1,
            attempt_horizon: object_store_occ::headlog::DEFAULT_ATTEMPT_HORIZON,
            retain_below_floor: object_store_occ::headlog::DEFAULT_RETENTION,
            reclaim_after: std::time::Duration::from_hours(1),
            dropped_grace: crate::metastore_layout::DATA_FILE_RETENTION,
            transaction_deadline: std::time::Duration::from_secs(60),
            publish_attempts: 8,
            retry_delay: std::time::Duration::from_millis(100),
            commit_each_change: false,
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

/// Bring a stopped log back: an unresolved commit by reopening the log and
/// resuming from what committed, a fenced one by reloading the committed state
/// for its reads. Retries with a doubling wait until it succeeds or the catalog
/// is dropped.
async fn recover_in_background(slot: std::sync::Weak<StateSlot>) {
    let mut attempt: u32 = 0;
    loop {
        let Some(strong) = slot.upgrade() else {
            return;
        };
        let (delay, done) = {
            // Past a transaction's reservation: only the reservation's holder
            // commits while it is held, so a stopped log is its holder's, and the
            // holder may be waiting for this very recovery to learn its outcome.
            let mut lease = strong.take().await;
            let delay = lease.config.retry_delay;
            match lease.recover().await {
                Ok(()) => {
                    lease.recovering = false;
                    (delay, true)
                }
                Err(error) => {
                    let wait = delay.saturating_mul(2_u32.saturating_pow(attempt.min(6)));
                    tracing::warn!(
                        "{}",
                        recovery_retry_message(&lease.dataset, &error.to_string(), wait)
                    );
                    (wait, false)
                }
            }
        };
        drop(strong);
        if done {
            return;
        }
        attempt = attempt.saturating_add(1);
        tokio::time::sleep(delay).await;
    }
}

/// The `transactions/` folder beside the catalog a log at `prefix` belongs to:
/// the registry's log is `<root>/catalog`, a dataset's `<root>/datasets/<folder>`.
fn transactions_beside(prefix: &Path, registry: bool) -> Path {
    let mut parts: Vec<_> = prefix.parts().collect();
    parts.pop();
    if !registry {
        parts.pop();
    }
    Path::from_iter(parts).join("transactions")
}

/// How a log's owner is named in what it reports.
fn subject(dataset: &str) -> String {
    if dataset.is_empty() {
        "The Cayenne object-native metastore".to_string()
    } else {
        format!("Dataset '{dataset}' (cayenne)")
    }
}

/// Logged when a commit's outcome is still unknown after its retries.
pub(crate) fn unresolved_commit_message(dataset: &str, cause: &str) -> String {
    format!(
        "{} could not confirm whether its last change was saved, so it accepts no change, and acknowledges none to its source, until it has read its state back; it retries in the background and resumes on its own once the state location is reachable. Cause: {cause}",
        subject(dataset)
    )
}

/// What the writer whose commit's outcome is unknown is told: not that the
/// change failed, but that it may have been saved.
pub(crate) fn unknown_outcome_message(dataset: &str, cause: &str) -> String {
    let (target, owner) = if dataset.is_empty() {
        (
            "the Cayenne object-native metastore".to_string(),
            "metastore",
        )
    } else {
        (format!("dataset '{dataset}' (cayenne)"), "dataset")
    };
    format!(
        "The outcome of this change to {target} is unknown: Spice could not confirm whether it was saved, so check whether its rows are visible before retrying it. The {owner} accepts no change until it has read its state back, which it retries in the background. Cause: {cause}"
    )
}

/// Logged each time reading the state back fails.
fn recovery_retry_message(dataset: &str, cause: &str, wait: std::time::Duration) -> String {
    format!(
        "{} could not read its state back after an unconfirmed change, so it still accepts no change; it retries in {} ms. Check that the state location is reachable. Cause: {cause}",
        subject(dataset),
        wait.as_millis()
    )
}

/// Logged when the state refused a change: a bug that stops the log.
pub(crate) fn refused_change_message(dataset: &str, cause: &str) -> String {
    format!(
        "{} stopped accepting changes until Spice restarts, because its state refused a change, which is a bug; queries return an error until then. Restart Spice to reload the dataset from its saved state, and report the issue at https://github.com/spiceai/spiceai/issues. Cause: {cause}",
        subject(dataset)
    )
}

/// Logged when another instance took the log over.
pub(crate) fn fenced_message(dataset: &str) -> String {
    format!(
        "{} stopped writing, because another Spice instance now writes its state; this instance keeps answering queries from the state saved so far. Restart this instance to make it the writer again.",
        subject(dataset)
    )
}

/// The refusal [`Inner::open`] returns when another instance holds a newer lease.
const FENCED_ON_REOPEN: &str = "another instance holds the log's newer lease";

fn validate_uuid(name: &str, value: &str) -> CatalogResult<()> {
    uuid::Uuid::parse_str(value)
        .map_err(|_| invalid(format!("{name} is not a valid UUID: {value}")))?;
    Ok(())
}

#[cfg(test)]
mod commit_outcome_message_tests {
    use super::{
        fenced_message, refused_change_message, unknown_outcome_message, unresolved_commit_message,
    };

    #[test]
    fn each_stop_names_the_dataset_what_happens_and_the_fix() {
        assert_eq!(
            unresolved_commit_message("orders", "connection reset"),
            "Dataset 'orders' (cayenne) could not confirm whether its last change was saved, so it accepts no change, and acknowledges none to its source, until it has read its state back; it retries in the background and resumes on its own once the state location is reachable. Cause: connection reset"
        );
        assert_eq!(
            refused_change_message("orders", "guard failed"),
            "Dataset 'orders' (cayenne) stopped accepting changes until Spice restarts, because its state refused a change, which is a bug; queries return an error until then. Restart Spice to reload the dataset from its saved state, and report the issue at https://github.com/spiceai/spiceai/issues. Cause: guard failed"
        );
        assert_eq!(
            fenced_message("orders"),
            "Dataset 'orders' (cayenne) stopped writing, because another Spice instance now writes its state; this instance keeps answering queries from the state saved so far. Restart this instance to make it the writer again."
        );
        assert!(
            fenced_message("").starts_with("The Cayenne object-native metastore stopped writing")
        );
        assert_eq!(
            unknown_outcome_message("orders", "connection reset"),
            "The outcome of this change to dataset 'orders' (cayenne) is unknown: Spice could not confirm whether it was saved, so check whether its rows are visible before retrying it. The dataset accepts no change until it has read its state back, which it retries in the background. Cause: connection reset"
        );
    }
}
