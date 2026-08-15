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

//! Distributed full-text search.
//!
//! On a multi-node accelerated table, each executor holds only its partition's
//! Tantivy index, so a BM25 score from one executor is not comparable with
//! another's. This crate rewrites a `text_search` scan on the scheduler into a
//! two-round distributed plan: gather the global collection statistics (a sum of
//! the additive per-partition statistics), then score every executor's partition
//! with those global statistics and merge the comparable results.
//!
//! - [`rewrite::DistributedSearchRewrite`] — the analyzer rule (scheduler only).
//! - [`exec::DistributedSearchExec`] — the physical operator that runs the two
//!   rounds, planned from [`exec::DistributedSearchNode`] by
//!   [`exec::DistributedSearchExtensionPlanner`].
//!
//! Registration (adding the rule and the extension planner to the session) lives
//! in the runtime crate, which owns the wiring and the accelerated-table check.

pub mod exec;
pub mod rewrite;

pub use exec::{
    DistributedExecutor, DistributedSearchExec, DistributedSearchExtensionPlanner,
    DistributedSearchNode, DistributedSearchParams,
};
pub use rewrite::{DistributedSearchRewrite, SearchDistributionGate};
