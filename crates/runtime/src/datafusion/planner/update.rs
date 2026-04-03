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

//! UPDATE planning for the unified planner.
//!
//! In distributed (scheduler) mode, produces a [`DistributedCayenneUpdateNode`]
//! that forwards the UPDATE to executor nodes.
//!
//! In local mode, UPDATE is handled by Cayenne's `TableProvider` implementation
//! through `DataFusion`'s standard physical planning — no interception needed.

use std::sync::Arc;

use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{Extension, LogicalPlan};
use datafusion_expr::DmlStatement;

use crate::datafusion::cayenne_ddl::analyzer_rule::{
    extract_filter_sql, extract_update_assignments,
};
use crate::datafusion::cayenne_ddl::logical_nodes::DistributedCayenneUpdateNode;

/// Wrap a `DataFusion` `DmlStatement` (UPDATE) into a distributed Cayenne
/// extension node for forwarding to executor nodes.
///
/// This is only called in distributed (scheduler) mode. In local mode,
/// the standard `DataFusion` plan is returned unchanged by the caller.
pub(super) fn plan_distributed_update(dml: &DmlStatement) -> DFResult<LogicalPlan> {
    let filter_sql = extract_filter_sql(&dml.input)?;
    let assignments_sql = extract_update_assignments(&dml.input, &dml.table_name)?;

    Ok(LogicalPlan::Extension(Extension {
        node: Arc::new(DistributedCayenneUpdateNode::new(
            dml.table_name.clone(),
            Arc::clone(&dml.input),
            Arc::clone(&dml.output_schema),
            filter_sql,
            assignments_sql,
        )),
    }))
}
