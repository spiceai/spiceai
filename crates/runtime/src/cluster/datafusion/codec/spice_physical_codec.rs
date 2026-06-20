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
use crate::dataconnector::iceberg_cluster::IcebergClusterTableProvider;
use crate::datafusion::planner::peel_table_provider_wrappers;
use crate::execution_plan::{IcebergScanExec, UdtfExec};
use crate::metrics::telemetry::track_bytes_processed;
use arrow_schema::Schema;
use ballista_core::serde::BallistaPhysicalExtensionCodec;
#[cfg(not(windows))]
use cayenne::provider::CayenneAccelerationExec;
use datafusion::common::{DataFusionError, Result, TableReference, exec_err};
use datafusion::execution::{FunctionRegistry, TaskContext};
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_expr::ScalarUDF;
use datafusion_proto::bytes::Serializeable;
use datafusion_proto::generated::datafusion_common;
#[cfg(not(windows))]
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
#[cfg(not(windows))]
use datafusion_proto::protobuf::PhysicalPlanNode;
use prost::Message;
use runtime_datafusion::execution_plan::schema_cast::SchemaCastScanExec;
use runtime_datafusion::extension::bytes_processed::BytesProcessedExec;
use runtime_proto::{
    BytesProcessedExecNode, CayenneAccelerationExecNode, IcebergTableScanExecNode,
    SchemaCastScanExecNode, SpicePhysicalPlanNode, UdtfExecNode, spice_physical_plan_node,
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
            Some(spice_physical_plan_node::Node::IcebergTableScan(node)) => {
                let runtime = self.runtime()?;
                let table_ref = TableReference::from(node.table_ref.as_str());

                // Surface conversion failures as structured errors rather than
                // saturating: a corrupt recipe or scheduler/executor version skew
                // should fail clearly here, not as a later out-of-bounds column or
                // an effectively unbounded limit.
                let projection: Option<Vec<usize>> = if node.has_projection {
                    Some(
                        node.projection
                            .iter()
                            .map(|c| {
                                usize::try_from(*c).map_err(|_| {
                                    DataFusionError::Internal(format!(
                                        "iceberg scan recipe for {table_ref} has projection index \
                                         {c} that does not fit in usize"
                                    ))
                                })
                            })
                            .collect::<Result<Vec<usize>>>()?,
                    )
                } else {
                    None
                };
                let filters = node
                    .filters
                    .iter()
                    .map(|bytes| Expr::from_bytes_with_ctx(bytes, ctx))
                    .collect::<Result<Vec<Expr>>>()?;
                let limit = node
                    .limit
                    .map(|l| {
                        usize::try_from(l).map_err(|_| {
                            DataFusionError::Internal(format!(
                                "iceberg scan recipe for {table_ref} has limit {l} that does not \
                                 fit in usize"
                            ))
                        })
                    })
                    .transpose()?;

                // Re-derive the scan by replaying TableProvider::scan on the
                // already-registered Iceberg provider, reusing its catalog. No
                // secrets cross the wire and no catalog is rebuilt — the executor
                // loaded the same app definition, so re-planning with the same
                // projection/filters reproduces the scheduler's bucketing.
                replan_registered_iceberg_scan(
                    &runtime,
                    ctx,
                    &table_ref,
                    projection.as_ref(),
                    &filters,
                    limit,
                )
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
        let wrapper = if let Some(concrete) = node.downcast_ref::<SchemaCastScanExec>() {
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
        } else if node.downcast_ref::<BytesProcessedExec>().is_some() {
            SpicePhysicalPlanNode {
                node: Some(spice_physical_plan_node::Node::BytesProcessed(
                    BytesProcessedExecNode {},
                )),
            }
        } else if let Some(udtf_exec) = node.downcast_ref::<UdtfExec>() {
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
        } else if let Some(scan_exec) = node.downcast_ref::<IcebergScanExec>() {
            // Serialize the scan recipe (table ref + projection/filters/limit).
            // The executor replays `TableProvider::scan` with these to re-derive
            // an equivalent scan — the iceberg `FileScanTask`s themselves are not
            // serializable (partition / partition_spec fields), so the plan is
            // rebuilt remotely rather than shipped. Conversions that don't fit the
            // wire types fail serialization explicitly rather than silently
            // saturating into a malformed recipe.
            let (has_projection, projection) = match scan_exec.projection() {
                Some(cols) => (
                    true,
                    cols.iter()
                        .map(|c| {
                            u32::try_from(*c).map_err(|_| {
                                DataFusionError::Internal(format!(
                                    "IcebergScanExec projection index {c} does not fit in u32; \
                                     cannot serialize the scan for distributed execution"
                                ))
                            })
                        })
                        .collect::<Result<Vec<u32>>>()?,
                ),
                None => (false, Vec::new()),
            };
            let filters = scan_exec
                .filters()
                .iter()
                .map(|expr| expr.to_bytes().map(|b| b.to_vec()))
                .collect::<Result<Vec<Vec<u8>>>>()?;
            let limit = scan_exec
                .limit()
                .map(|l| {
                    u64::try_from(l).map_err(|_| {
                        DataFusionError::Internal(format!(
                            "IcebergScanExec limit {l} does not fit in u64; cannot serialize the \
                             scan for distributed execution"
                        ))
                    })
                })
                .transpose()?;

            SpicePhysicalPlanNode {
                node: Some(spice_physical_plan_node::Node::IcebergTableScan(
                    IcebergTableScanExecNode {
                        table_ref: scan_exec.table_ref().to_string(),
                        projection,
                        has_projection,
                        filters,
                        limit,
                    },
                )),
            }
        } else {
            #[cfg(not(windows))]
            if node.downcast_ref::<CayenneAccelerationExec>().is_some() {
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

/// Re-derives an Iceberg scan on this executor by replaying
/// [`TableProvider::scan`] on the already-registered provider for `table_ref`.
///
/// The executor loaded the same app definition as the scheduler, so the Iceberg
/// provider (an [`IcebergClusterTableProvider`], possibly behind registration
/// wrappers) is already registered. Resolving it and re-calling `scan` with the
/// same projection/filters/limit rebuilds an equivalent, identically bucketed
/// scan — reusing the provider's catalog (no secrets cross the wire, no catalog
/// rebuilt) and producing a fresh [`IcebergScanExec`].
///
/// The replan uses the runtime's session state but overrides `target_partitions`
/// with the value from the per-job [`TaskContext`]. Iceberg's partition (bucket)
/// count is `target_partitions.min(num_tasks)`, so the replan must use the same
/// `target_partitions` the scheduler planned with — otherwise the executor could
/// rebuild a scan with a different partition count than the stage expects.
fn replan_registered_iceberg_scan(
    runtime: &Arc<Runtime>,
    ctx: &TaskContext,
    table_ref: &TableReference,
    projection: Option<&Vec<usize>>,
    filters: &[Expr],
    limit: Option<usize>,
) -> Result<Arc<dyn ExecutionPlan>> {
    // The codec API is synchronous but resolution/scan are async; this path runs
    // only at plan-decode time on an executor, so blocking is acceptable.
    let provider = tokio::task::block_in_place(|| {
        tokio::runtime::Handle::current().block_on(runtime.df.get_table(table_ref))
    })
    .ok_or_else(|| {
        DataFusionError::Execution(format!(
            "Iceberg table {table_ref} is not registered on this executor; \
             cannot reconstruct the distributed scan"
        ))
    })?;

    // Peel the registration wrappers (FederatedTableProviderAdaptor,
    // MetadataEnrichedTableProvider) down to the IcebergClusterTableProvider that
    // dataset registration leaves in the catalog. The Spice `FederatedTable`
    // holder is already resolved to its inner provider at registration time, so
    // it is never the registered provider here.
    let peeled = peel_table_provider_wrappers(&provider);
    if peeled
        .downcast_ref::<IcebergClusterTableProvider>()
        .is_none()
    {
        return exec_err!(
            "registered provider for {table_ref} is not an IcebergClusterTableProvider; \
             distributed Iceberg scans require the Iceberg data connector"
        );
    }

    // Align target_partitions with the scheduler's per-job config so the rebuilt
    // scan buckets into the same number of partitions as the planned stage.
    let mut session_state = runtime.df.ctx.state();
    session_state
        .config_mut()
        .options_mut()
        .execution
        .target_partitions = ctx.session_config().options().execution.target_partitions;

    tokio::task::block_in_place(|| {
        tokio::runtime::Handle::current().block_on(peeled.scan(
            &session_state,
            projection,
            filters,
            limit,
        ))
    })
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
    use datafusion::common::{JoinType, NullEquality};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::execution::context::SessionContext;
    use datafusion::physical_expr::expressions::col;
    use datafusion::physical_plan::displayable;
    use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};

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
    fn cayenne_hash_join_round_trips_through_nested_physical_plan() {
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
            false,
        )
        .expect("hash join should be valid");
        let join: Arc<dyn ExecutionPlan> = Arc::new(default_join);
        let codec = SpicePhysicalCodec {
            inner: Arc::new(BallistaPhysicalExtensionCodec::default()),
            runtime: None,
        };

        let proto = PhysicalPlanNode::try_from_physical_plan(join, &codec)
            .expect("hash join should serialize through Spice codec");
        let ctx = SessionContext::new();
        let task_ctx = ctx.state().task_ctx();
        let round_tripped = proto
            .try_into_physical_plan(task_ctx.as_ref(), &codec)
            .expect("serialized hash join should decode");
        let plan = displayable(round_tripped.as_ref()).indent(true).to_string();

        assert!(
            plan.contains("HashJoinExec"),
            "Distributed fallback should preserve the hash join: {plan}"
        );
        assert!(
            plan.contains("CayenneAccelerationExec"),
            "Cayenne scan marker should survive distributed codec roundtrip: {plan}"
        );
    }

    #[test]
    fn iceberg_scan_exec_encodes_recipe() {
        use datafusion::logical_expr::{col as logical_col, lit};
        use datafusion::physical_plan::empty::EmptyExec;

        // The encode arm reads only IcebergScanExec's own recipe fields, not the
        // inner plan's type, so an EmptyExec inner is sufficient to exercise it.
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, true),
            Field::new("c", DataType::Float64, true),
        ]));
        let inner: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(schema));
        let scan = IcebergScanExec::new(
            TableReference::bare("trips"),
            inner,
            Some(vec![0, 2]),
            vec![logical_col("a").gt(lit(5_i64))],
            Some(10),
        );

        let codec = SpicePhysicalCodec {
            inner: Arc::new(BallistaPhysicalExtensionCodec::default()),
            runtime: None,
        };
        let mut buf = Vec::new();
        codec
            .try_encode(Arc::new(scan), &mut buf)
            .expect("IcebergScanExec should serialize through the Spice codec");

        let wrapper =
            SpicePhysicalPlanNode::decode(buf.as_slice()).expect("encoded blob should decode");
        match wrapper.node {
            Some(spice_physical_plan_node::Node::IcebergTableScan(node)) => {
                assert_eq!(node.table_ref, "trips");
                assert!(node.has_projection);
                assert_eq!(node.projection, vec![0_u32, 2_u32]);
                assert_eq!(node.limit, Some(10_u64));
                assert_eq!(
                    node.filters.len(),
                    1,
                    "the pushed-down filter should be serialized into the recipe"
                );
            }
            other => panic!("expected an IcebergTableScan recipe node, got {other:?}"),
        }
    }
}
