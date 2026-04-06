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

//! INSERT planning for the unified planner.
//!
//! In distributed (scheduler) mode, produces a [`DistributedCayenneInsertNode`]
//! that forwards the INSERT to executor nodes.
//!
//! In local mode, INSERT is handled by Cayenne's `TableProvider` implementation
//! through `DataFusion`'s standard physical planning — no interception needed.

use std::sync::Arc;

use datafusion::logical_expr::{Extension, LogicalPlan};
use datafusion_expr::DmlStatement;

use crate::datafusion::cayenne_ddl::logical_nodes::DistributedCayenneInsertNode;

/// Wrap a `DataFusion` `DmlStatement` (INSERT) into a distributed Cayenne
/// extension node for forwarding to executor nodes.
///
/// This is only called in distributed (scheduler) mode. In local mode,
/// the standard `DataFusion` plan is returned unchanged by the caller.
pub(super) fn plan_distributed_insert(dml: &DmlStatement) -> LogicalPlan {
    LogicalPlan::Extension(Extension {
        node: Arc::new(DistributedCayenneInsertNode::new(
            dml.table_name.clone(),
            Arc::clone(&dml.input),
            Arc::clone(&dml.output_schema),
        )),
    })
}
