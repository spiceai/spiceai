/*
Copyright 2026 The Spice.ai OSS Authors

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

use std::sync::Arc;

use datafusion::{
    execution::{SessionState, SessionStateBuilder, TaskContext},
    prelude::{SessionConfig, SessionContext},
};

/// A [`SessionConfig`] whose `target_partitions` is sized from the CPU budget
/// (`runtime.cpu.cores`).
///
/// `DataFusion`'s own defaults (`SessionConfig::new()`, `SessionContext::new()`,
/// `TaskContext::default()`, `ConfigOptions::default()`) size `target_partitions`
/// from `available_parallelism()`, which reports the node's cores for a pod with a
/// CPU request and no limit. Start any session that is not derived from the
/// runtime's query session from this instead, so it fans out across the cores
/// the runtime is entitled to rather than the whole node.
///
/// This keeps `DataFusion`'s SQL semantics. A session that must plan SQL the way
/// user queries do (`PostgreSQL` dialect, case-preserving identifiers) starts from
/// `runtime_datafusion::session_config::get_df_default_config()` instead, which
/// applies the same budget.
#[must_use]
pub fn session_config() -> SessionConfig {
    SessionConfig::new().with_target_partitions(cpu_budget::cpu_budget().target_partitions())
}

/// A [`SessionContext`] built from [`session_config`].
#[must_use]
pub fn session_context() -> SessionContext {
    SessionContext::new_with_config(session_config())
}

/// Marks a session as a user's statement writing into an acceleration (an
/// `INSERT`, or a write inside `BEGIN … COMMIT`), as opposed to the
/// accelerator's own writes: refreshes and change streams.
///
/// An accelerator applies a dataset's `on_conflict` to the keys its own writes'
/// incoming data repeats (each record batch one upsert); a user's statement
/// keeps its own semantics. Every other write is the accelerator's own, so a
/// write path that forgets the marker still resolves repeats rather than
/// storing them. [`mark_user_statement`] sets it, and [`is_user_statement`]
/// reads it.
#[derive(Debug, Default)]
pub struct UserStatementWrite;

/// `state` marked as a user's statement; see [`UserStatementWrite`].
#[must_use]
pub fn mark_user_statement(state: &SessionState) -> SessionState {
    let mut state = state.clone();
    state
        .config_mut()
        .set_extension(Arc::new(UserStatementWrite));
    state
}

/// Whether `config` belongs to a user's statement; see [`UserStatementWrite`].
#[must_use]
pub fn is_user_statement(config: &SessionConfig) -> bool {
    config.get_extension::<UserStatementWrite>().is_some()
}

/// Orders the copies of a key a write repeats by the version each row carries,
/// rather than by the order they arrive in: the row with the greatest
/// `(time, content hash)` is kept. Implemented by the writer that knows how to read
/// a row's version (for `on_conflict: upsert_dedup_by_time_column`, its
/// `time_column`); read by an accelerator that resolves repeated keys after
/// writing them.
pub trait RowVersions: Send + Sync + std::fmt::Debug {
    /// Each row of `batch`'s time, as UTC nanoseconds, and content hash. A row
    /// whose time cannot be read fails the write.
    ///
    /// # Errors
    ///
    /// Returns an error if a time is NULL or cannot be read.
    fn versions(
        &self,
        batch: &arrow::array::RecordBatch,
    ) -> datafusion::error::Result<(Vec<i64>, Vec<u64>)>;
}

/// The copies of a repeated key a write did not keep, by why.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct SupersededCounts {
    /// Resolved by arrival order per `on_conflict` (a later copy wins under
    /// `upsert`, the first under `drop`).
    pub repeated: u64,
    /// A copy with an earlier time than the version kept.
    pub older: u64,
    /// A copy with the kept version's time but different content.
    pub equal_time: u64,
    /// A copy identical in time and content to the version kept.
    pub unchanged: u64,
}

impl SupersededCounts {
    /// Add `other` to these counts.
    pub fn add(&mut self, other: &Self) {
        self.repeated += other.repeated;
        self.older += other.older;
        self.equal_time += other.equal_time;
        self.unchanged += other.unchanged;
    }

    /// Whether no copy was superseded.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        *self == Self::default()
    }
}

/// Receives what an accelerator's write superseded, so the writer can record it.
pub trait SupersededReport: Send + Sync + std::fmt::Debug {
    fn superseded(&self, counts: &SupersededCounts);
}

/// What a refresh's write carries to the accelerator: how to order a key's copies,
/// and where to report the ones it does not keep. See [`with_refresh_write`].
#[derive(Debug, Clone, Default)]
pub struct RefreshWrite {
    pub row_versions: Option<Arc<dyn RowVersions>>,
    pub superseded: Option<Arc<dyn SupersededReport>>,
}

tokio::task_local! {
    static REFRESH_WRITE: RefreshWrite;
}

/// Run `write` with `refresh` as the context of the writes it makes; a sink marks
/// its session from it with [`mark_refresh_write`].
pub async fn with_refresh_write<F: std::future::Future>(
    refresh: RefreshWrite,
    write: F,
) -> F::Output {
    REFRESH_WRITE.scope(refresh, write).await
}

/// `state` carrying the current task's [`RefreshWrite`], or `state` unchanged when
/// it has none.
#[must_use]
pub fn mark_refresh_write(state: SessionState) -> SessionState {
    match REFRESH_WRITE.try_with(Clone::clone) {
        Ok(refresh) => {
            let mut state = state;
            state.config_mut().set_extension(Arc::new(refresh));
            state
        }
        Err(_) => state,
    }
}

/// The [`RowVersions`] a write's `config` carries, if any.
#[must_use]
pub fn row_versions(config: &SessionConfig) -> Option<Arc<dyn RowVersions>> {
    config
        .get_extension::<RefreshWrite>()
        .and_then(|refresh| refresh.row_versions.clone())
}

/// Where a write's `config` asks it to report superseded copies, if anywhere.
#[must_use]
pub fn superseded_report(config: &SessionConfig) -> Option<Arc<dyn SupersededReport>> {
    config
        .get_extension::<RefreshWrite>()
        .and_then(|refresh| refresh.superseded.clone())
}

/// A [`TaskContext`] carrying [`session_config`], for executing a plan outside
/// any session.
#[must_use]
pub fn task_context() -> TaskContext {
    TaskContext::default().with_session_config(session_config())
}

/// Returns a [`SessionStateBuilder`] cloned from `existing` while preserving custom rules.
#[must_use]
pub fn builder_from_existing(existing: &SessionState) -> SessionStateBuilder {
    SessionStateBuilder::new_from_existing(existing.clone())
        .with_analyzer_rules(existing.analyzer().rules.iter().map(Arc::clone).collect())
        .with_optimizer_rules(existing.optimizers().iter().map(Arc::clone).collect())
        .with_physical_optimizer_rules(
            existing
                .physical_optimizers()
                .iter()
                .map(Arc::clone)
                .collect(),
        )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn helpers_use_cpu_budget_target_partitions() {
        let cpu_budget::testing::Isolation::Child { cores } = cpu_budget::testing::isolated_budget(
            "session_state::tests::helpers_use_cpu_budget_target_partitions",
        )
        .expect("isolated CPU budget run should pass") else {
            return;
        };

        let expected = cpu_budget::cpu_budget().target_partitions();
        assert_eq!(expected, cores);
        assert_eq!(session_config().target_partitions(), expected);
        assert_eq!(
            session_context().state().config().target_partitions(),
            expected
        );
        assert_eq!(
            task_context().session_config().target_partitions(),
            expected
        );
    }
}
