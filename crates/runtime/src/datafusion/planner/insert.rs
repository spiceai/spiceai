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
//! In distributed (scheduler) mode, wraps target INSERT statements in a generic
//! [`datafusion_dml::DmlExtensionNode`] with a distributed Cayenne DML handler.
//!
//! In local mode, INSERT is handled by Cayenne's `TableProvider` implementation
//! through `DataFusion`'s standard physical planning — no interception needed.

use std::sync::Arc;

use datafusion::error::DataFusionError;
use datafusion::logical_expr::{Extension, LogicalPlan};
use datafusion_dml::{CatalogDmlHandler, DmlExtensionNode, DmlNodeOp, InsertParams};
use datafusion_expr::{DmlStatement, WriteOp};

/// Wrap a `DataFusion` `DmlStatement` (INSERT) into a generic DML extension
/// node for forwarding to executor nodes.
///
/// This rewrite is only used for scheduler-side distributed overlay paths.
pub(super) fn plan_distributed_insert(
    dml: &DmlStatement,
    handler: Arc<dyn CatalogDmlHandler>,
) -> datafusion::error::Result<LogicalPlan> {
    let WriteOp::Insert(insert_op) = dml.op else {
        return Err(DataFusionError::Internal(format!(
            "Expected WriteOp::Insert for distributed INSERT planning, got {:?}",
            dml.op
        )));
    };

    Ok(LogicalPlan::Extension(Extension {
        node: Arc::new(DmlExtensionNode::new(
            DmlNodeOp::Insert(InsertParams {
                table_name: dml.table_name.clone(),
                insert_op,
            }),
            handler,
            vec![Arc::clone(&dml.input)],
            Arc::clone(&dml.output_schema),
        )),
    }))
}
