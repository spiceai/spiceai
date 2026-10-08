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

/// Why a write did not keep a row it received.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SupersededReason {
    /// A version of a key that arrived later (with the same time, when versions
    /// are ordered by time) was kept.
    Arrival,
    /// A version of a key with a later time was kept.
    Older,
}

impl SupersededReason {
    pub const ALL: [Self; 2] = [Self::Arrival, Self::Older];

    /// The `reason` label a metric reports this under.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Arrival => "arrival",
            Self::Older => "older",
        }
    }

    /// Why a copy with time `loser` lost to the copy with time `winner`, each from
    /// [`RowVersions`] (`None` for a NULL time, older than any time): an older time,
    /// or the same time and an earlier arrival.
    #[must_use]
    pub fn of_version(loser: Option<i64>, winner: Option<i64>) -> Self {
        if loser < winner {
            Self::Older
        } else {
            Self::Arrival
        }
    }
}

/// The rows a write received but did not keep, by [`SupersededReason`]. A
/// caller attaches one to the session that runs the write
/// ([`with_superseded_rows`]); the accelerator counts into it.
#[derive(Debug, Default)]
pub struct SupersededRows {
    counts: [std::sync::atomic::AtomicU64; SupersededReason::ALL.len()],
}

impl SupersededRows {
    fn counter(&self, reason: SupersededReason) -> &std::sync::atomic::AtomicU64 {
        &self.counts[reason as usize]
    }

    pub fn add(&self, reason: SupersededReason, rows: u64) {
        if rows > 0 {
            self.counter(reason)
                .fetch_add(rows, std::sync::atomic::Ordering::Relaxed);
        }
    }

    #[must_use]
    pub fn get(&self, reason: SupersededReason) -> u64 {
        self.counter(reason)
            .load(std::sync::atomic::Ordering::Relaxed)
    }
}

/// `state` with `rows` attached, for the accelerator to count the rows the
/// write it runs does not keep; see [`SupersededRows`].
#[must_use]
pub fn with_superseded_rows(state: &SessionState, rows: Arc<SupersededRows>) -> SessionState {
    let mut state = state.clone();
    state.config_mut().set_extension(rows);
    state
}

/// The [`SupersededRows`] attached to `config`, if any.
#[must_use]
pub fn superseded_rows(config: &SessionConfig) -> Option<Arc<SupersededRows>> {
    config.get_extension::<SupersededRows>()
}

/// Orders the copies of a key a write repeats by the time each row carries,
/// rather than only by the order they arrive in: the row with the greatest time is
/// kept, and of rows with the same time the last to arrive. Implemented by the
/// writer that knows how to read a row's time (a refresh, from the dataset's
/// `time_column`); read by an accelerator that resolves repeated keys after
/// writing them.
pub trait RowVersions: Send + Sync + std::fmt::Debug {
    /// Each row of `batch`'s time, as UTC nanoseconds. A NULL time stays NULL and is
    /// older than any time, including [`i64::MIN`].
    ///
    /// # Errors
    ///
    /// Returns an error if a time cannot be read.
    fn versions(
        &self,
        batch: &arrow::array::RecordBatch,
    ) -> datafusion::error::Result<arrow::array::Int64Array>;
}

/// The [`RowVersions`] a write orders a key's copies by; see [`with_row_versions`].
#[derive(Debug)]
struct WriteRowVersions(Arc<dyn RowVersions>);

/// `state` carrying `versions`, so the accelerator's write orders a key's copies
/// by them.
#[must_use]
pub fn with_row_versions(state: &SessionState, versions: Arc<dyn RowVersions>) -> SessionState {
    let mut state = state.clone();
    state
        .config_mut()
        .set_extension(Arc::new(WriteRowVersions(versions)));
    state
}

/// The [`RowVersions`] a write's `config` carries, if any.
#[must_use]
pub fn row_versions(config: &SessionConfig) -> Option<Arc<dyn RowVersions>> {
    config
        .get_extension::<WriteRowVersions>()
        .map(|versions| Arc::clone(&versions.0))
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
