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
use datafusion::physical_expr::Partitioning;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_plan::{ExecutionPlan, PhysicalExpr};
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
    BytesProcessedExecNode, CayenneAccelerationExecNode, IcebergHashColumn, IcebergPartitioning,
    IcebergTableScanExecNode, SchemaCastScanExecNode, SpicePhysicalPlanNode, UdtfExecNode,
    spice_physical_plan_node,
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
                let partitioning = decode_partitioning(node.partitioning.as_ref())?;

                // Resolve the registered provider synchronously — no catalog I/O,
                // no blocking bridge. The executor loaded the same app definition,
                // so the Iceberg dataset is already registered (in the default
                // catalog, which is sync-accessible). The actual scan is replayed
                // lazily at execute() time; see IcebergScanExec::new_deferred.
                let provider = runtime.df.get_table_sync(&table_ref).ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "Iceberg table {table_ref} is not registered on this executor; \
                         cannot reconstruct the distributed scan"
                    ))
                })?;
                let peeled = peel_table_provider_wrappers(&provider);
                let Some(cluster) = peeled.downcast_ref::<IcebergClusterTableProvider>() else {
                    return exec_err!(
                        "registered provider for {table_ref} is not an IcebergClusterTableProvider; \
                         distributed Iceberg scans require the Iceberg data connector"
                    );
                };
                // The concrete Iceberg provider (not the cluster wrapper), so
                // replaying its scan() yields the bare scan without re-wrapping.
                let inner_provider = Arc::clone(cluster.inner());

                // Output schema = table schema projected by the recipe, taken
                // synchronously from the registered provider.
                let table_schema = inner_provider.schema();
                let output_schema = match &projection {
                    Some(p) => Arc::new(table_schema.project(p)?),
                    None => table_schema,
                };

                Ok(Arc::new(IcebergScanExec::new_deferred(
                    table_ref,
                    inner_provider,
                    projection,
                    filters,
                    limit,
                    output_schema,
                    partitioning,
                )))
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
            let partitioning = encode_partitioning(scan_exec.properties().output_partitioning())?;

            SpicePhysicalPlanNode {
                node: Some(spice_physical_plan_node::Node::IcebergTableScan(
                    IcebergTableScanExecNode {
                        table_ref: scan_exec.table_ref().to_string(),
                        projection,
                        has_projection,
                        filters,
                        limit,
                        partitioning: Some(partitioning),
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

/// Serializes a scan's output [`Partitioning`] into its wire form, so the
/// deferred node on the executor reports the same partition count the scheduler
/// planned (before the lazy scan runs).
///
/// Hash partitioning is reproduced only when every expression is a plain
/// [`Column`]; otherwise the count is preserved as
/// [`Partitioning::UnknownPartitioning`].
fn encode_partitioning(partitioning: &Partitioning) -> Result<IcebergPartitioning> {
    // Counts/indices fail serialization explicitly rather than saturating into a
    // malformed recipe (consistent with how projection/limit are encoded).
    fn to_u64(n: usize) -> Result<u64> {
        u64::try_from(n).map_err(|_| {
            DataFusionError::Internal(format!(
                "IcebergScanExec partitioning value {n} does not fit in u64; cannot serialize \
                 the scan for distributed execution"
            ))
        })
    }
    Ok(match partitioning {
        Partitioning::UnknownPartitioning(n) => IcebergPartitioning {
            kind: 0,
            partition_count: to_u64(*n)?,
            hash_columns: Vec::new(),
        },
        Partitioning::RoundRobinBatch(n) => IcebergPartitioning {
            kind: 2,
            partition_count: to_u64(*n)?,
            hash_columns: Vec::new(),
        },
        Partitioning::Hash(exprs, n) => {
            // Reproduce Hash partitioning only when every expr is a plain Column;
            // otherwise preserve just the count as UnknownPartitioning.
            let mut hash_columns = Vec::with_capacity(exprs.len());
            let mut all_columns = true;
            for expr in exprs {
                if let Some(c) = expr.downcast_ref::<Column>() {
                    hash_columns.push(IcebergHashColumn {
                        name: c.name().to_string(),
                        index: to_u64(c.index())?,
                    });
                } else {
                    all_columns = false;
                    break;
                }
            }
            if all_columns {
                IcebergPartitioning {
                    kind: 1,
                    partition_count: to_u64(*n)?,
                    hash_columns,
                }
            } else {
                IcebergPartitioning {
                    kind: 0,
                    partition_count: to_u64(*n)?,
                    hash_columns: Vec::new(),
                }
            }
        }
    })
}

/// Reconstructs a [`Partitioning`] from its wire form. Values that don't fit
/// `usize` (corrupt recipe / platform skew) fail rather than saturating.
fn decode_partitioning(partitioning: Option<&IcebergPartitioning>) -> Result<Partitioning> {
    fn to_usize(v: u64) -> Result<usize> {
        usize::try_from(v).map_err(|_| {
            DataFusionError::Internal(format!(
                "iceberg scan recipe partitioning value {v} does not fit in usize"
            ))
        })
    }
    let Some(p) = partitioning else {
        return Ok(Partitioning::UnknownPartitioning(1));
    };
    let n = to_usize(p.partition_count)?;
    Ok(match p.kind {
        1 => {
            let exprs: Vec<Arc<dyn PhysicalExpr>> =
                p.hash_columns
                    .iter()
                    .map(|c| {
                        Ok::<_, DataFusionError>(Arc::new(Column::new(&c.name, to_usize(c.index)?))
                            as Arc<dyn PhysicalExpr>)
                    })
                    .collect::<Result<Vec<_>>>()?;
            Partitioning::Hash(exprs, n)
        }
        2 => Partitioning::RoundRobinBatch(n),
        _ => Partitioning::UnknownPartitioning(n),
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
