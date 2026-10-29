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

//! Statements that define, change, and remove Spicepod datasets and catalogs.
//!
//! The planner turns `CREATE TABLE ... WITH (from = ...)`, `ALTER TABLE`,
//! `DROP TABLE`, and the `CATALOG` statements on Spicepod components into a
//! [`ComponentDdlNode`]. The node carries the operation and the
//! [`ComponentDdlHandler`] that applies it, and [`ComponentDdlPlanner`] plans
//! any such node into a [`ComponentDdlExec`] that runs the handler when the
//! statement executes.

use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::{RecordBatch, StringArray};
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::DFSchemaRef;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion::logical_expr::{
    Expr, LogicalPlan, UserDefinedLogicalNode, UserDefinedLogicalNodeCore,
};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use spicepod::component::catalog::Catalog;
use spicepod::component::dataset::Dataset;

use crate::component_options::{ComponentKind, ComponentOption};
use crate::ddl_output_schema;

/// The name every [`ComponentDdlNode`] reports, which the read-only checks and
/// the results cache match on.
pub const COMPONENT_DDL_NODE_NAME: &str = "ComponentDdl";

/// A change to a component's options.
#[derive(Debug, Clone, PartialEq)]
pub enum OptionsChange {
    /// `SET (key = value, ...)`.
    Set(Vec<ComponentOption>),
    /// `RESET (key, ...)`.
    Reset(Vec<String>),
}

/// A statement that defines, changes, or removes a Spicepod component.
#[derive(Debug, Clone, PartialEq)]
pub enum ComponentDdlOp {
    CreateDataset {
        dataset: Box<Dataset>,
        if_not_exists: bool,
        or_replace: bool,
    },
    AlterDataset {
        name: String,
        change: OptionsChange,
    },
    DropDataset {
        name: String,
        if_exists: bool,
    },
    CreateCatalog {
        catalog: Box<Catalog>,
        if_not_exists: bool,
        or_replace: bool,
    },
    AlterCatalog {
        name: String,
        change: OptionsChange,
    },
    DropCatalog {
        name: String,
        if_exists: bool,
    },
}

impl ComponentDdlOp {
    /// The component the statement names.
    #[must_use]
    pub fn component_name(&self) -> &str {
        match self {
            Self::CreateDataset { dataset, .. } => &dataset.name,
            Self::CreateCatalog { catalog, .. } => &catalog.name,
            Self::AlterDataset { name, .. }
            | Self::DropDataset { name, .. }
            | Self::AlterCatalog { name, .. }
            | Self::DropCatalog { name, .. } => name,
        }
    }

    /// The kind of component the statement names.
    #[must_use]
    pub fn kind(&self) -> ComponentKind {
        match self {
            Self::CreateDataset { .. } | Self::AlterDataset { .. } | Self::DropDataset { .. } => {
                ComponentKind::Dataset
            }
            Self::CreateCatalog { .. } | Self::AlterCatalog { .. } | Self::DropCatalog { .. } => {
                ComponentKind::Catalog
            }
        }
    }

    /// The statement, for plans, logs, and errors: as the user wrote it, so
    /// `CREATE OR REPLACE`, `IF NOT EXISTS`, and `IF EXISTS` are named as
    /// such.
    #[must_use]
    pub fn label(&self) -> &'static str {
        match self {
            Self::CreateDataset {
                or_replace: true, ..
            } => "CREATE OR REPLACE TABLE",
            Self::CreateDataset {
                if_not_exists: true,
                ..
            } => "CREATE TABLE IF NOT EXISTS",
            Self::CreateDataset { .. } => "CREATE TABLE",
            Self::AlterDataset { .. } => "ALTER TABLE",
            Self::DropDataset {
                if_exists: true, ..
            } => "DROP TABLE IF EXISTS",
            Self::DropDataset { .. } => "DROP TABLE",
            Self::CreateCatalog {
                or_replace: true, ..
            } => "CREATE OR REPLACE CATALOG",
            Self::CreateCatalog {
                if_not_exists: true,
                ..
            } => "CREATE CATALOG IF NOT EXISTS",
            Self::CreateCatalog { .. } => "CREATE CATALOG",
            Self::AlterCatalog { .. } => "ALTER CATALOG",
            Self::DropCatalog {
                if_exists: true, ..
            } => "DROP CATALOG IF EXISTS",
            Self::DropCatalog { .. } => "DROP CATALOG",
        }
    }
}

/// Applies component statements: the runtime, which applies a change the way
/// it applies a Spicepod edit.
#[async_trait]
pub trait ComponentDdlHandler: fmt::Debug + Send + Sync {
    /// Refuse a caller that may not run `op`. Called while the statement is
    /// planned, on the caller's request.
    async fn check_access(&self, op: &ComponentDdlOp) -> DFResult<()>;

    /// Apply `op`, returning what the statement reports.
    async fn apply(&self, op: ComponentDdlOp) -> DFResult<String>;

    /// The statement that recreates the component of `kind` named `name`, or
    /// `None` when there is none.
    async fn create_statement(&self, kind: ComponentKind, name: &str) -> DFResult<Option<String>>;
}

// ── Logical node ──────────────────────────────────────────────────────────────

/// The logical plan of a component statement.
pub struct ComponentDdlNode {
    pub op: ComponentDdlOp,
    pub handler: Arc<dyn ComponentDdlHandler>,
    output_schema: DFSchemaRef,
}

impl ComponentDdlNode {
    #[must_use]
    pub fn new(op: ComponentDdlOp, handler: Arc<dyn ComponentDdlHandler>) -> Self {
        Self {
            op,
            handler,
            output_schema: ddl_output_schema(),
        }
    }

    /// The node as a logical plan.
    #[must_use]
    pub fn into_plan(self) -> LogicalPlan {
        LogicalPlan::Extension(datafusion::logical_expr::Extension {
            node: Arc::new(self),
        })
    }
}

impl fmt::Debug for ComponentDdlNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ComponentDdlNode")
            .field("op", &self.op.label())
            .field("name", &self.op.component_name())
            .finish_non_exhaustive()
    }
}

impl Hash for ComponentDdlNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.op.label().hash(state);
        self.op.component_name().hash(state);
    }
}

impl PartialEq for ComponentDdlNode {
    fn eq(&self, other: &Self) -> bool {
        self.op == other.op
    }
}

impl Eq for ComponentDdlNode {}

impl PartialOrd for ComponentDdlNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        (self.op.label(), self.op.component_name())
            .partial_cmp(&(other.op.label(), other.op.component_name()))
    }
}

impl UserDefinedLogicalNodeCore for ComponentDdlNode {
    fn name(&self) -> &'static str {
        COMPONENT_DDL_NODE_NAME
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.output_schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "ComponentDdl: {} {}",
            self.op.label(),
            self.op.component_name()
        )
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        _inputs: Vec<LogicalPlan>,
    ) -> DFResult<Self> {
        Ok(Self {
            op: self.op.clone(),
            handler: Arc::clone(&self.handler),
            output_schema: DFSchemaRef::clone(&self.output_schema),
        })
    }
}

// ── Physical plan ─────────────────────────────────────────────────────────────

/// Runs a component statement's handler when the statement executes, and
/// returns its message as one `result` row.
pub struct ComponentDdlExec {
    op: ComponentDdlOp,
    handler: Arc<dyn ComponentDdlHandler>,
    properties: Arc<PlanProperties>,
}

impl ComponentDdlExec {
    #[must_use]
    pub fn new(op: ComponentDdlOp, handler: Arc<dyn ComponentDdlHandler>) -> Self {
        let schema = result_schema();
        Self {
            op,
            handler,
            properties: Arc::new(PlanProperties::new(
                EquivalenceProperties::new(schema),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Final,
                Boundedness::Bounded,
            )),
        }
    }
}

fn result_schema() -> SchemaRef {
    Arc::new(ddl_output_schema().as_arrow().clone())
}

impl fmt::Debug for ComponentDdlExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ComponentDdlExec")
            .field("op", &self.op.label())
            .field("name", &self.op.component_name())
            .finish_non_exhaustive()
    }
}

impl DisplayAs for ComponentDdlExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "ComponentDdlExec: {} {}",
            self.op.label(),
            self.op.component_name()
        )
    }
}

impl ExecutionPlan for ComponentDdlExec {
    fn name(&self) -> &'static str {
        "ComponentDdlExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(
            &Arc<dyn datafusion::physical_plan::PhysicalExpr>,
        ) -> DFResult<datafusion::common::tree_node::TreeNodeRecursion>,
    ) -> DFResult<datafusion::common::tree_node::TreeNodeRecursion> {
        Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "ComponentDdlExec has one partition, but partition {partition} was requested"
            )));
        }
        let op = self.op.clone();
        let handler = Arc::clone(&self.handler);
        let schema = result_schema();
        let batch_schema = Arc::clone(&schema);
        let stream = futures::stream::once(async move {
            let message = handler.apply(op).await?;
            RecordBatch::try_new(
                batch_schema,
                vec![Arc::new(StringArray::from(vec![message]))],
            )
            .map_err(DataFusionError::from)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

/// Plans every [`ComponentDdlNode`] into a [`ComponentDdlExec`].
#[derive(Debug, Default)]
pub struct ComponentDdlPlanner;

#[async_trait]
impl ExtensionPlanner for ComponentDdlPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        _physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session: &dyn Session,
        _planning_ctx: &PhysicalPlanningContext,
    ) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
        let Some(node) = node.as_any().downcast_ref::<ComponentDdlNode>() else {
            return Ok(None);
        };
        Ok(Some(Arc::new(ComponentDdlExec::new(
            node.op.clone(),
            Arc::clone(&node.handler),
        ))))
    }
}
