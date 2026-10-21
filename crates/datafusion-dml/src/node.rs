/*
Copyright 2026, Spice AI, Inc.

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

//! Logical node definitions for the generic DML extension pipeline.

use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::common::DFSchemaRef;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};

use crate::handler::{CatalogDmlHandler, DeleteParams, InsertParams, MergeParams, UpdateParams};
use crate::helpers::dml_count_output_schema;

/// DML operation kind and its full parameters, stored inside
/// [`DmlExtensionNode`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DmlNodeOp {
    Delete(DeleteParams),
    Update(UpdateParams),
    Insert(InsertParams),
    Merge(Box<MergeParams>),
}

impl DmlNodeOp {
    pub(crate) fn kind_label(&self) -> &'static str {
        match self {
            Self::Delete(_) => "Delete",
            Self::Update(_) => "Update",
            Self::Insert(_) => "Insert",
            Self::Merge(_) => "Merge",
        }
    }

    pub(crate) fn target_table_name(&self) -> String {
        match self {
            Self::Delete(p) => p.table_name.to_string(),
            Self::Update(p) => p.table_name.to_string(),
            Self::Insert(p) => p.table_name.to_string(),
            Self::Merge(p) => p.target_table.to_string(),
        }
    }
}

/// The single logical plan node type for all Spice DML operations.
///
/// Produced by statement/analyzer rewrites and consumed by
/// [`crate::DmlExtensionPlanner`]. The embedded [`CatalogDmlHandler`] carries
/// catalog-specific logic; the planner itself has no catalog knowledge.
pub struct DmlExtensionNode {
    /// DML operation and its full parameters.
    pub op: DmlNodeOp,
    /// Handler that will convert this node to a physical plan.
    pub handler: Arc<dyn CatalogDmlHandler>,
    inputs: Vec<Arc<LogicalPlan>>,
    output_schema: DFSchemaRef,
}

impl DmlExtensionNode {
    /// Construct a node with an explicit output schema.
    #[must_use]
    pub fn new(
        op: DmlNodeOp,
        handler: Arc<dyn CatalogDmlHandler>,
        inputs: Vec<Arc<LogicalPlan>>,
        output_schema: DFSchemaRef,
    ) -> Self {
        Self {
            op,
            handler,
            inputs,
            output_schema,
        }
    }

    /// Construct a node that uses the default DML output schema
    /// (`count: UInt64`).
    #[must_use]
    pub fn new_with_count_output(
        op: DmlNodeOp,
        handler: Arc<dyn CatalogDmlHandler>,
        inputs: Vec<Arc<LogicalPlan>>,
    ) -> Self {
        Self::new(op, handler, inputs, dml_count_output_schema())
    }
}

impl fmt::Debug for DmlExtensionNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DmlExtensionNode")
            .field("op", &self.op)
            .field("handler", &self.handler.name())
            .finish_non_exhaustive()
    }
}

// Hash / PartialEq / Eq / PartialOrd are required by UserDefinedLogicalNodeCore.
// `handler` and `output_schema` are intentionally excluded.
impl Hash for DmlExtensionNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.op.hash(state);
        self.inputs.hash(state);
    }
}

impl PartialEq for DmlExtensionNode {
    fn eq(&self, other: &Self) -> bool {
        self.op == other.op && self.inputs == other.inputs
    }
}

impl Eq for DmlExtensionNode {}

impl PartialOrd for DmlExtensionNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        (self.op.kind_label(), self.op.target_table_name())
            .partial_cmp(&(other.op.kind_label(), other.op.target_table_name()))
    }
}

impl UserDefinedLogicalNodeCore for DmlExtensionNode {
    fn name(&self) -> &'static str {
        "DmlExtension"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        self.inputs.iter().map(AsRef::as_ref).collect()
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.output_schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "DmlExtension({}): {}",
            self.op.kind_label(),
            self.op.target_table_name()
        )
    }

    fn with_exprs_and_inputs(&self, _exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> DFResult<Self> {
        Ok(Self {
            op: self.op.clone(),
            handler: Arc::clone(&self.handler),
            inputs: inputs.into_iter().map(Arc::new).collect(),
            output_schema: DFSchemaRef::clone(&self.output_schema),
        })
    }
}
