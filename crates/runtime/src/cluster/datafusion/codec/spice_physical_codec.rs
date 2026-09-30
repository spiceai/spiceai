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
use arrow_schema::Schema;
use ballista_core::serde::BallistaPhysicalExtensionCodec;
#[cfg(not(windows))]
use cayenne::provider::CayenneAccelerationExec;
use data_components::http::provider::{HttpExec, HttpTableProvider};
use datafusion::catalog::TableProvider;
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
use iceberg_datafusion::IcebergTableProvider;
use prost::Message;
use runtime_datafusion::execution_plan::schema_cast::SchemaCastScanExec;
use runtime_datafusion::extension::bytes_processed::BytesProcessedExec;
use runtime_execution_plans::{IcebergScanExec, UdtfExec};
use runtime_metrics::telemetry::track_bytes_processed;
use runtime_proto::{
    BytesProcessedExecNode, CayenneAccelerationExecNode, HttpExecNode, HttpPartition,
    IcebergHashColumn, IcebergPartitioning, IcebergTableScanExecNode, SchemaCastScanExecNode,
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
            Some(spice_physical_plan_node::Node::HttpExec(node)) => {
                if !inputs.is_empty() {
                    return exec_err!("HttpExec must not have input execution plans");
                }

                let runtime = self.runtime()?;
                let table_ref = TableReference::from(node.table_ref.as_str());
                let schema = datafusion_common::Schema::decode(&*node.projected_schema)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                let projected_schema = Arc::new(Schema::try_from(&schema)?);
                let limit = node
                    .limit
                    .map(|limit| {
                        usize::try_from(limit).map_err(|_| {
                            DataFusionError::Internal(format!(
                                "HTTP scan recipe for {table_ref} has limit {limit} that does not fit in usize"
                            ))
                        })
                    })
                    .transpose()?;

                let registered = runtime.df.get_table_sync(&table_ref).ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "HTTP table {table_ref} is not registered on this executor; cannot reconstruct the distributed scan"
                    ))
                })?;
                let Some(provider) = spice_table::find_concrete::<HttpTableProvider>(
                    registered.as_ref(),
                    spice_table::LayerWalk::Read,
                ) else {
                    return exec_err!(
                        "registered provider for {table_ref} is not an HttpTableProvider; distributed HTTP scans require the HTTP data connector"
                    );
                };
                if provider.table_reference() != Some(&table_ref) {
                    return exec_err!(
                        "registered HTTP provider for {table_ref} has a different table identity; cannot reconstruct the distributed scan"
                    );
                }
                validate_http_projected_schema(&table_ref, &projected_schema, &provider.schema())?;

                let partitions = node
                    .partitions
                    .into_iter()
                    .map(|partition| {
                        (
                            partition.path,
                            partition.query,
                            partition.body,
                            partition.request_headers,
                        )
                    })
                    .collect();

                Ok(Arc::new(HttpExec::new(
                    projected_schema,
                    Arc::new(provider.clone()),
                    partitions,
                    limit,
                )))
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
                // Locate the cluster provider through ALL known runtime wrappers
                // (FederatedTableProviderAdaptor, MetadataEnrichedTableProvider,
                // EmbeddingTable, IndexLayer, AcceleratedTable, …), not
                // just the federation/metadata pair — an Iceberg dataset with
                // embeddings or a search index is wrapped further.
                let Some(cluster) = spice_table::find_layer::<IcebergClusterTableProvider>(
                    provider.as_ref(),
                    spice_table::LayerWalk::Read,
                ) else {
                    return exec_err!(
                        "registered provider for {table_ref} is not an IcebergClusterTableProvider; \
                         distributed Iceberg scans require the Iceberg data connector"
                    );
                };
                // Resolve the concrete IcebergTableProvider (the cluster wrapper's
                // inner, peeling the deletion wrapper on the read-write path) and
                // pin it to the snapshot the scheduler chose, so every executor task
                // of this query reads the same snapshot. Scanning this provider
                // directly also yields the bare scan without re-wrapping.
                let Some(iceberg_provider) = concrete_iceberg_provider(cluster.inner()) else {
                    return exec_err!(
                        "IcebergClusterTableProvider for {table_ref} does not wrap an \
                         IcebergTableProvider; cannot reconstruct the distributed scan"
                    );
                };
                let pinned: Arc<dyn TableProvider> =
                    Arc::new(iceberg_provider.clone().with_snapshot_id(node.snapshot_id));

                // Output schema = table schema projected by the recipe, taken
                // synchronously from the registered provider.
                let table_schema = pinned.schema();
                let output_schema = match &projection {
                    Some(p) => Arc::new(table_schema.project(p)?),
                    None => table_schema,
                };

                Ok(Arc::new(IcebergScanExec::new_deferred(
                    table_ref,
                    pinned,
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
        } else if node.is::<BytesProcessedExec>() {
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
        } else if let Some(http_exec) = node.downcast_ref::<HttpExec>() {
            let Some(table_ref) = http_exec.provider().table_reference() else {
                return exec_err!(
                    "HttpExec provider has no registered table reference; cannot serialize the scan for distributed execution"
                );
            };
            let mut schema_buf = Vec::new();
            let serialized_schema =
                datafusion_common::Schema::try_from(Arc::clone(http_exec.projected_schema()))?;
            serialized_schema
                .encode(&mut schema_buf)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            let limit = http_exec
                .limit()
                .map(|limit| {
                    u64::try_from(limit).map_err(|_| {
                        DataFusionError::Internal(format!(
                            "HttpExec limit {limit} does not fit in u64; cannot serialize the scan for distributed execution"
                        ))
                    })
                })
                .transpose()?;

            SpicePhysicalPlanNode {
                node: Some(spice_physical_plan_node::Node::HttpExec(HttpExecNode {
                    table_ref: table_ref.to_quoted_string(),
                    projected_schema: schema_buf,
                    partitions: http_exec
                        .partitions()
                        .iter()
                        .map(|partition| HttpPartition {
                            path: partition.0.clone(),
                            query: partition.1.clone(),
                            body: partition.2.clone(),
                            request_headers: partition.3.clone(),
                        })
                        .collect(),
                    limit,
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
                        // Pin the plan-time snapshot so every executor task reads it.
                        snapshot_id: scan_exec.snapshot_id(),
                    },
                )),
            }
        } else {
            #[cfg(not(windows))]
            if node.is::<CayenneAccelerationExec>() {
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

fn validate_http_projected_schema(
    table_ref: &TableReference,
    projected_schema: &Schema,
    provider_schema: &Schema,
) -> Result<()> {
    for field in projected_schema.fields() {
        let provider_field = provider_schema.field_with_name(field.name()).map_err(|_| {
            DataFusionError::Execution(format!(
                "HTTP scan recipe for {table_ref} projects unknown field '{}'",
                field.name()
            ))
        })?;
        if provider_field.data_type() != field.data_type() {
            return exec_err!(
                "HTTP scan recipe for {table_ref} projects field '{}' as {:?}, but the registered provider uses {:?}",
                field.name(),
                field.data_type(),
                provider_field.data_type()
            );
        }
    }
    Ok(())
}

/// Returns the concrete [`IcebergTableProvider`] behind a cluster wrapper's inner
/// provider — directly (read path) or through the [`IcebergDeletionProvider`] the
/// read-write path inserts. The returned provider is used (cloned + snapshot
/// pinned) to replay the scan on the executor.
fn concrete_iceberg_provider(inner: &Arc<dyn TableProvider>) -> Option<&IcebergTableProvider> {
    if let Some(p) = inner.downcast_ref::<IcebergTableProvider>() {
        return Some(p);
    }
    spice_table::find_concrete::<IcebergTableProvider>(inner.as_ref(), spice_table::LayerWalk::Read)
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
    use datafusion::datasource::MemTable;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::execution::context::SessionContext;
    use datafusion::physical_expr::expressions::col;
    use datafusion::physical_plan::displayable;
    use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
    use reqwest::header::{HeaderMap, HeaderValue};
    use std::collections::HashMap;

    fn memory_exec(column_name: &str) -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            column_name,
            DataType::Int32,
            false,
        )]));
        MemorySourceConfig::try_new_exec(&[vec![]], schema, None)
            .expect("memory exec should be valid")
    }

    fn http_provider(
        table_ref: Option<TableReference>,
        header_value: &'static str,
    ) -> HttpTableProvider {
        let mut headers = HeaderMap::new();
        headers.insert("x-cluster-config", HeaderValue::from_static(header_value));
        let provider = HttpTableProvider::new(
            url::Url::parse("http://127.0.0.1:1/users").expect("test URL should parse"),
            reqwest::Client::new(),
            "json".to_string(),
            false,
        )
        .with_headers(headers);
        match table_ref {
            Some(table_ref) => provider.with_table_reference(table_ref),
            None => provider,
        }
    }

    fn encoded_schema(schema: &Schema) -> Vec<u8> {
        let mut buf = Vec::new();
        datafusion_common::Schema::try_from(schema)
            .expect("test schema should serialize")
            .encode(&mut buf)
            .expect("test schema should encode");
        buf
    }

    fn http_recipe(node: HttpExecNode) -> Vec<u8> {
        SpicePhysicalPlanNode {
            node: Some(spice_physical_plan_node::Node::HttpExec(node)),
        }
        .encode_to_vec()
    }

    #[tokio::test]
    async fn http_exec_round_trips_recipe_and_rebinds_executor_provider() {
        let table_ref = TableReference::bare("users.v1");
        let sender_header = "sender-only-secret";
        let executor_header = "executor-local-secret";
        let sender_provider = Arc::new(http_provider(Some(table_ref.clone()), sender_header));
        let provider_schema = sender_provider.schema();
        let projected_schema = Arc::new(Schema::new_with_metadata(
            vec![
                provider_schema
                    .field_with_name("request_query")
                    .expect("request_query should exist")
                    .clone(),
                provider_schema
                    .field_with_name("content")
                    .expect("content should exist")
                    .clone(),
            ],
            HashMap::from([("test-metadata".to_string(), "retained".to_string())]),
        ));
        let partitions = vec![
            (
                None,
                Some(String::new()),
                Some("sender-body".to_string()),
                None,
            ),
            (
                Some("later".to_string()),
                Some("page=2".to_string()),
                None,
                Some("x-request: executor-visible".to_string()),
            ),
        ];
        let plan: Arc<dyn ExecutionPlan> = Arc::new(HttpExec::new(
            Arc::clone(&projected_schema),
            sender_provider,
            partitions.clone(),
            Some(0),
        ));

        let runtime = Arc::new(Runtime::builder().build().await);
        let executor_provider: Arc<dyn TableProvider> =
            Arc::new(http_provider(Some(table_ref.clone()), executor_header));
        let wrapped_provider = data_components::metadata_enriched_table_provider(
            executor_provider,
            HashMap::from([("description".to_string(), "wrapped provider".to_string())]),
            data_components::FieldMetadata::new(),
        );
        runtime
            .datafusion()
            .ctx
            .register_table(table_ref.clone(), wrapped_provider)
            .expect("executor provider should register");
        let codec = SpicePhysicalCodec::new(Arc::clone(&runtime)).expect("codec should build");

        let proto = PhysicalPlanNode::try_from_physical_plan(plan, codec.as_ref())
            .expect("HTTP scan should serialize through the production physical-plan path");
        let bytes = proto.encode_to_vec();
        assert!(
            !bytes
                .windows(sender_header.len())
                .any(|window| window == sender_header.as_bytes()),
            "scheduler-local headers must not be serialized into the plan"
        );

        let decoded_proto =
            PhysicalPlanNode::decode(bytes.as_slice()).expect("physical plan should decode");
        let task_ctx = runtime.datafusion().ctx.state().task_ctx();
        let decoded = decoded_proto
            .try_into_physical_plan(task_ctx.as_ref(), codec.as_ref())
            .expect("HTTP scan should deserialize through the production physical-plan path");
        let decoded = decoded
            .downcast_ref::<HttpExec>()
            .expect("round-tripped plan should be HttpExec");

        assert_eq!(
            decoded.projected_schema().as_ref(),
            projected_schema.as_ref()
        );
        assert_eq!(decoded.partitions(), partitions.as_slice());
        assert_eq!(decoded.limit(), Some(0));
        assert_eq!(decoded.provider().table_reference(), Some(&table_ref));
        assert_eq!(
            decoded
                .provider()
                .custom_headers()
                .get("x-cluster-config")
                .expect("executor header should be present"),
            HeaderValue::from_static(executor_header)
        );
    }

    #[tokio::test]
    async fn http_exec_rejects_invalid_recipes() {
        let runtime = Arc::new(Runtime::builder().build().await);
        let http_table_ref = TableReference::bare("http_users");
        let registered_http_provider: Arc<dyn TableProvider> = Arc::new(http_provider(
            Some(http_table_ref.clone()),
            "executor-local-secret",
        ));
        runtime
            .datafusion()
            .ctx
            .register_table(http_table_ref.clone(), registered_http_provider)
            .expect("HTTP provider should register");

        let wrong_table_ref = TableReference::bare("memory_users");
        let memory_schema = Arc::new(Schema::new(vec![Field::new(
            "content",
            DataType::Utf8,
            false,
        )]));
        runtime
            .datafusion()
            .ctx
            .register_table(
                wrong_table_ref.clone(),
                Arc::new(
                    MemTable::try_new(Arc::clone(&memory_schema), vec![vec![]])
                        .expect("memory table should build"),
                ),
            )
            .expect("memory provider should register");

        let codec = SpicePhysicalCodec::new(Arc::clone(&runtime)).expect("codec should build");
        let task_ctx = runtime.datafusion().ctx.state().task_ctx();
        let provider_schema = runtime
            .datafusion()
            .get_table_sync(&http_table_ref)
            .expect("HTTP table should resolve")
            .schema();
        let valid_schema = encoded_schema(provider_schema.as_ref());
        let recipe = |table_ref: &TableReference, projected_schema: Vec<u8>| HttpExecNode {
            table_ref: table_ref.to_quoted_string(),
            projected_schema,
            partitions: vec![HttpPartition {
                path: None,
                query: None,
                body: None,
                request_headers: None,
            }],
            limit: None,
        };

        let missing = http_recipe(recipe(
            &TableReference::bare("missing_users"),
            valid_schema.clone(),
        ));
        let err = codec
            .try_decode(&missing, &[], task_ctx.as_ref())
            .expect_err("an unregistered HTTP table should fail");
        assert!(err.to_string().contains("is not registered"), "{err}");

        let wrong_provider = http_recipe(recipe(&wrong_table_ref, valid_schema.clone()));
        let err = codec
            .try_decode(&wrong_provider, &[], task_ctx.as_ref())
            .expect_err("a non-HTTP provider should fail");
        assert!(
            err.to_string().contains("not an HttpTableProvider"),
            "{err}"
        );

        let err = codec
            .try_decode(
                &http_recipe(recipe(&http_table_ref, valid_schema)),
                &[memory_exec("input")],
                task_ctx.as_ref(),
            )
            .expect_err("HttpExec with an input should fail");
        assert!(err.to_string().contains("must not have input"), "{err}");

        let malformed = http_recipe(recipe(&http_table_ref, vec![0xff]));
        codec
            .try_decode(&malformed, &[], task_ctx.as_ref())
            .expect_err("a malformed projected schema should fail");

        let incompatible = http_recipe(recipe(
            &http_table_ref,
            encoded_schema(&Schema::new(vec![Field::new(
                "content",
                DataType::Int64,
                false,
            )])),
        ));
        let err = codec
            .try_decode(&incompatible, &[], task_ctx.as_ref())
            .expect_err("an incompatible projected schema should fail");
        assert!(
            err.to_string().contains("registered provider uses"),
            "{err}"
        );

        let anonymous_provider = Arc::new(http_provider(None, "sender-only-secret"));
        let anonymous = HttpExec::new(
            anonymous_provider.schema(),
            anonymous_provider,
            vec![(None, None, None, None)],
            None,
        );
        let err = codec
            .try_encode(Arc::new(anonymous), &mut Vec::new())
            .expect_err("an anonymous HTTP provider should not serialize");
        assert!(
            err.to_string().contains("no registered table reference"),
            "{err}"
        );
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
