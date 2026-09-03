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

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use datafusion::logical_expr::LogicalPlan;
use datafusion::sql::TableReference;

use crate::{AsTableRefs, ReadStartedAt};

/// A cached logical plan, stamped with when planning began.
///
/// The stamp is what lets [`crate::TabledCacheProvider::get_raw_key_if_fresh`]
/// reject a plan built from pre-invalidation catalog state. Planning that
/// straddles a refresh, DML write, or schema change stores its plan *after*
/// the invalidation removed existing entries, where a `moka` predicate cannot
/// see it. A stale plan is not just a stale artifact: it pins
/// `Arc<dyn TableSource>` references inside its `TableScan` nodes, so
/// executing it can read the table state it was planned against — and results
/// computed from it re-enter the results cache under a fresh
/// [`ReadStartedAt::read_started_at`], defeating that cache's own staleness
/// check.
#[derive(Clone)]
pub struct CachedLogicalPlan {
    pub plan: LogicalPlan,
    /// When planning began — captured before the planner reads any catalog
    /// state, so an invalidation landing while planning is still running
    /// orders at or after it and rejects the entry.
    pub planned_at: Instant,
}

impl CachedLogicalPlan {
    #[must_use]
    pub fn new(plan: LogicalPlan, planned_at: Instant) -> Self {
        Self { plan, planned_at }
    }
}

impl AsTableRefs for CachedLogicalPlan {
    fn as_table_refs(&self) -> Arc<HashSet<TableReference>> {
        self.plan.as_table_refs()
    }
}

impl ReadStartedAt for CachedLogicalPlan {
    fn read_started_at(&self) -> Instant {
        self.planned_at
    }
}
