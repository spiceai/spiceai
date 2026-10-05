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
