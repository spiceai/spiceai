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

//! A query pushed down to a federated SQL source, as an executor runs it.
//!
//! Federation plans a query it pushes down to one source as a
//! `VirtualExecutionPlan`, which holds the connection to that source and
//! cannot be serialized. A distributed task carries the query's logical plan
//! instead, with its tables by reference, and [`FederatedQueryExec`] plans it
//! again on the executor, against the executor's own registration of the same
//! datasets, when the task runs. Planning is asynchronous and plan decoding is
//! not, so the query is planned at `execute`.
//!
//! Planning runs the federation analyzer over a plan it has already rewritten
//! once on the scheduler, so a connector's logical rewrite of a pushed-down
//! plan (`SQLExecutor::logical_optimizer`) must be idempotent, as `SQLite`'s
//! date-operand marking is.

use std::fmt;
use std::sync::Arc;

use arrow_schema::SchemaRef;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{DataFusionError, Result, internal_err};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PhysicalExpr, PlanProperties, execute_stream,
};
use futures::TryStreamExt;

use crate::Runtime;

/// A federated query, planned on the executor when its task runs.
pub struct FederatedQueryExec {
    plan: LogicalPlan,
    runtime: Arc<Runtime>,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl FederatedQueryExec {
    /// `plan` is the query the scheduler pushed down, decoded against this
    /// executor's tables.
    #[must_use]
    pub fn new(plan: LogicalPlan, runtime: Arc<Runtime>) -> Self {
        let schema: SchemaRef = Arc::new(plan.schema().as_arrow().clone());
        // As `VirtualExecutionPlan` reports it: one stream from the source.
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            plan,
            runtime,
            schema,
            properties,
        }
    }

    /// The pushed-down query.
    #[must_use]
    pub fn plan(&self) -> &LogicalPlan {
        &self.plan
    }
}

impl fmt::Debug for FederatedQueryExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FederatedQueryExec")
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

impl DisplayAs for FederatedQueryExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "FederatedQueryExec")
    }
}

impl ExecutionPlan for FederatedQueryExec {
    fn name(&self) -> &'static str {
        "FederatedQueryExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    /// The query's expressions are logical; the plan holds no physical ones.
    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return internal_err!("FederatedQueryExec is a leaf and takes no children");
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return internal_err!(
                "FederatedQueryExec has one partition, but partition {partition} was requested"
            );
        }
        let plan = self.plan.clone();
        let runtime = Arc::clone(&self.runtime);
        let schema = Arc::clone(&self.schema);
        let expected = Arc::clone(&self.schema);
        let stream = async move {
            let physical = runtime.df.ctx.state().create_physical_plan(&plan).await?;
            check_schema(&expected, &physical.schema())?;
            execute_stream(physical, context)
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::once(stream).try_flatten(),
        )))
    }
}

/// The plan the executor built must return the columns the scheduler planned
/// for: the same names and types, in the same order.
fn check_schema(expected: &SchemaRef, actual: &SchemaRef) -> Result<()> {
    let same = expected.fields().len() == actual.fields().len()
        && expected
            .fields()
            .iter()
            .zip(actual.fields())
            .all(|(e, a)| e.name() == a.name() && e.data_type() == a.data_type());
    if same {
        Ok(())
    } else {
        Err(DataFusionError::Execution(format!(
            "The federated query planned on this executor returns {actual:?}, but the scheduler planned it to return {expected:?}. Check that the scheduler and every executor load the same Spicepod."
        )))
    }
}
