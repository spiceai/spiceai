/*
Copyright 2025-2026 The Spice.ai OSS Authors

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

use crate::Runtime;
use crate::execution_plan::UdtfExec;
use crate::metrics::telemetry::track_bytes_processed;
use arrow_schema::Schema;
use ballista_core::serde::BallistaPhysicalExtensionCodec;
#[cfg(not(windows))]
use cayenne::provider::CayenneAccelerationExec;
use datafusion::common::{DataFusionError, Result, exec_err};
use datafusion::execution::{FunctionRegistry, TaskContext};
use datafusion::physical_plan::ExecutionPlan;
#[cfg(not(windows))]
use datafusion::physical_plan::joins::{HashJoinExec, MinMaxLeftAccumulator};
use datafusion_expr::ScalarUDF;
use datafusion_proto::generated::datafusion_common;
#[cfg(not(windows))]
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
#[cfg(not(windows))]
use datafusion_proto::protobuf::PhysicalPlanNode;
use prost::Message;
use runtime_datafusion::execution_plan::schema_cast::SchemaCastScanExec;
use runtime_datafusion::extension::bytes_processed::BytesProcessedExec;
#[cfg(not(windows))]
use runtime_datafusion::join_accumulator::ExactLeftAccumulator;
use runtime_proto::{
    BytesProcessedExecNode, CayenneAccelerationExecNode, SchemaCastScanExecNode,
    SpicePhysicalPlanNode, UdtfExecNode, spice_physical_plan_node,
};
use std::fmt::Debug;
use std::sync::Arc;

use super::spice_logical_codec::SpiceLogicalCodec;

/// Serialization support for custom Spice execution nodes
pub struct SpicePhysicalCodec {
    inner: Arc<dyn PhysicalExtensionCodec>,
    runtime: Option<Arc<Runtime>>,
}

impl Debug for SpicePhysicalCodec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SpicePhysicalCodec")
    }
}

impl SpicePhysicalCodec {
    pub fn new(runtime: Arc<Runtime>) -> Result<Arc<Self>> {
        Ok(Arc::new(Self {
            inner: Arc::new(BallistaPhysicalExtensionCodec::default()),
            runtime: Some(runtime),
        }))
    }

    /// Used during encode and decode
    fn runtime(&self) -> Result<Arc<Runtime>> {
        self.runtime.clone().ok_or(DataFusionError::Execution(
            "SpicePhysicalCodec did not bind a Runtime handle. This is a bug.".to_string(),
        ))
    }
}

impl PhysicalExtensionCodec for SpicePhysicalCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        ctx: &TaskContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if let Ok(plan) = self.inner.try_decode(buf, inputs, ctx) {
            return Ok(plan);
        }

        let wrapper = SpicePhysicalPlanNode::decode(buf)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        match wrapper.node {
            Some(spice_physical_plan_node::Node::SchemaCastScan(node)) => {
                let schema = datafusion_common::Schema::decode(&*node.schema)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                let exec = Arc::new(SchemaCastScanExec::new(
                    Arc::clone(&inputs[0]),
                    Arc::new(Schema::try_from(&schema)?),
                ));

                Ok(exec)
            }
            Some(spice_physical_plan_node::Node::BytesProcessed(_)) => Ok(Arc::new(
                BytesProcessedExec::new(
                    Arc::clone(&inputs[0]),
                    Arc::new(Box::new(track_bytes_processed)),
                )
                .fallback_to_new_context(),
            )),
            Some(spice_physical_plan_node::Node::CayenneAcceleration(_)) => {
                #[cfg(not(windows))]
                {
                    Ok(Arc::new(CayenneAccelerationExec::new(Arc::clone(
                        &inputs[0],
                    ))))
                }
                #[cfg(windows)]
                {
                    exec_err!("CayenneAccelerationExec is not supported on Windows")
                }
            }
            Some(spice_physical_plan_node::Node::Udtf(node)) => {
                // Decode the UdtfExec by re-invoking the UDTF
                let runtime = self.runtime()?;
                let Some(args) = node.args else {
                    return exec_err!("UdtfExecNode missing args");
                };

                // Re-invoke the UDTF to get the TableProvider
                let table_provider = SpiceLogicalCodec::invoke_udtf(args.clone(), &runtime)?;

                // Get the execution plan from the TableProvider using the runtime's session state
                let session_state = runtime.df.ctx.state();
                // NOTE: The codec deserialization API is synchronous, but DataFusion's
                // TableProvider::scan is async. To reconstruct the physical plan we must
                // synchronously wait for the scan to complete. This path is only taken during
                // plan deserialization on executor startup, so the blocking cost is acceptable.
                let inner_plan = tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current().block_on(async {
                        table_provider.scan(&session_state, None, &[], None).await
                    })
                })?;

                Ok(Arc::new(UdtfExec::new(args, inner_plan)))
            }
            None => {
                #[cfg(not(windows))]
                if let Ok(plan) = Self::try_decode_nested_physical_plan(buf, ctx) {
                    return Ok(plan);
                }
                exec_err!("Cannot deserialize unknown execution plan")
            }
        }
    }

    fn try_encode(&self, node: Arc<dyn ExecutionPlan>, buf: &mut Vec<u8>) -> Result<()> {
        let wrapper = if let Some(concrete) = node.as_any().downcast_ref::<SchemaCastScanExec>() {
            let mut schema_buf = vec![];
            let serialized_schema = datafusion_common::Schema::try_from(concrete.schema())?;
            serialized_schema
                .encode(&mut schema_buf)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            SpicePhysicalPlanNode {
                node: Some(spice_physical_plan_node::Node::SchemaCastScan(
                    SchemaCastScanExecNode { schema: schema_buf },
                )),
            }
        } else if node.as_any().downcast_ref::<BytesProcessedExec>().is_some() {
            SpicePhysicalPlanNode {
                node: Some(spice_physical_plan_node::Node::BytesProcessed(
                    BytesProcessedExecNode {},
                )),
            }
        } else if let Some(udtf_exec) = node.as_any().downcast_ref::<UdtfExec>() {
            let mut schema_buf = vec![];
            let serialized_schema = datafusion_common::Schema::try_from(udtf_exec.schema())?;
            serialized_schema
                .encode(&mut schema_buf)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            SpicePhysicalPlanNode {
                node: Some(spice_physical_plan_node::Node::Udtf(UdtfExecNode {
                    args: Some(udtf_exec.args().clone()),
                    schema: schema_buf,
                })),
            }
        } else {
            #[cfg(not(windows))]
            if let Some(hash_join) = node
                .as_any()
                .downcast_ref::<HashJoinExec<ExactLeftAccumulator>>()
            {
                let serializable_join: Arc<dyn ExecutionPlan> =
                    Arc::new(hash_join.recreate_with_accumulator::<MinMaxLeftAccumulator>());
                let physical_node =
                    PhysicalPlanNode::try_from_physical_plan(serializable_join, self)?;
                physical_node
                    .encode(buf)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                return Ok(());
            }

            #[cfg(not(windows))]
            if node
                .as_any()
                .downcast_ref::<CayenneAccelerationExec>()
                .is_some()
            {
                SpicePhysicalPlanNode {
                    node: Some(spice_physical_plan_node::Node::CayenneAcceleration(
                        CayenneAccelerationExecNode {},
                    )),
                }
            } else {
                return self.inner.try_encode(node, buf);
            }
            #[cfg(windows)]
            {
                return self.inner.try_encode(node, buf);
            }
        };

        wrapper
            .encode(buf)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        Ok(())
    }

    fn try_decode_udf(&self, name: &str, _buf: &[u8]) -> Result<Arc<ScalarUDF>> {
        self.runtime()?.df.ctx.udf(name)
    }
}

#[cfg(not(windows))]
impl SpicePhysicalCodec {
    fn try_decode_nested_physical_plan(
        buf: &[u8],
        ctx: &TaskContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let physical_node = PhysicalPlanNode::try_decode(buf)?;
        physical_node.try_into_physical_plan(
            ctx,
            &Self {
                inner: Arc::new(BallistaPhysicalExtensionCodec::default()),
                runtime: None,
            },
        )
    }
}

#[cfg(test)]
#[cfg(not(windows))]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::execution::context::SessionContext;
    use datafusion::physical_expr::expressions::col;
    use datafusion::physical_plan::displayable;
    use datafusion::physical_plan::joins::PartitionMode;
    use datafusion_common::{JoinType, NullEquality};

    fn memory_exec(column_name: &str) -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            column_name,
            DataType::Int32,
            false,
        )]));
        MemorySourceConfig::try_new_exec(&[vec![]], schema, None)
            .expect("memory exec should be valid")
    }

    #[test]
    fn exact_cayenne_hash_join_round_trips_as_serializable_hash_join() {
        let left = memory_exec("left_id");
        let right: Arc<dyn ExecutionPlan> =
            Arc::new(CayenneAccelerationExec::new(memory_exec("right_id")));
        let default_join = HashJoinExec::try_new(
            Arc::clone(&left),
            Arc::clone(&right),
            vec![(
                col("left_id", &left.schema()).expect("left join key should exist"),
                col("right_id", &right.schema()).expect("right join key should exist"),
            )],
            None,
            &JoinType::Inner,
            None,
            PartitionMode::Partitioned,
            NullEquality::NullEqualsNothing,
        )
        .expect("hash join should be valid");
        let exact_join: Arc<dyn ExecutionPlan> =
            Arc::new(default_join.recreate_with_accumulator::<ExactLeftAccumulator>());
        let codec = SpicePhysicalCodec {
            inner: Arc::new(BallistaPhysicalExtensionCodec::default()),
            runtime: None,
        };

        let proto = PhysicalPlanNode::try_from_physical_plan(exact_join, &codec)
            .expect("exact join should serialize through Spice codec");
        let ctx = SessionContext::new();
        let task_ctx = ctx.state().task_ctx();
        let round_tripped = proto
            .try_into_physical_plan(task_ctx.as_ref(), &codec)
            .expect("serialized exact join fallback should decode");
        let plan = displayable(round_tripped.as_ref()).indent(true).to_string();

        assert!(
            plan.contains("accumulator=MinMaxLeftAccumulator"),
            "Distributed fallback should preserve the join with DataFusion's serializable accumulator: {plan}"
        );
        assert!(
            plan.contains("CayenneAccelerationExec"),
            "Cayenne scan marker should survive distributed codec roundtrip: {plan}"
        );
    }
}
