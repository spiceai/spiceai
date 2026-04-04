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
use datafusion_expr::ScalarUDF;
use datafusion_proto::generated::datafusion_common;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use opentelemetry::trace::{SpanId, TraceId};
use prost::Message;
use runtime_datafusion::execution_plan::schema_cast::SchemaCastScanExec;
use runtime_datafusion::extension::bytes_processed::BytesProcessedExec;
use runtime_proto::{
    BytesProcessedExecNode, CayenneAccelerationExecNode, RequestContextProto,
    SchemaCastScanExecNode, UdtfExecNode,
};
use runtime_request_context::{Protocol, RequestContext, RequestContextBuilder, TraceParent};
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

        if let Ok(node) = SchemaCastScanExecNode::decode(buf) {
            let schema = datafusion_common::Schema::decode(&*node.schema)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            let exec = Arc::new(SchemaCastScanExec::new(
                Arc::clone(&inputs[0]),
                Arc::new(Schema::try_from(&schema)?),
            ));

            Ok(exec)
        } else if let Ok(node) = BytesProcessedExecNode::decode(buf) {
            let request_context = request_context_from_proto(node.request_context);
            let mut exec = BytesProcessedExec::new(
                Arc::clone(&inputs[0]),
                Arc::new(Box::new(track_bytes_processed)),
            )
            .fallback_to_new_context();
            if let Some(ctx) = request_context {
                exec = exec.with_request_context(Arc::new(ctx));
            }
            Ok(Arc::new(exec))
        } else if CayenneAccelerationExecNode::decode(buf).is_ok() {
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
        } else if let Ok(node) = UdtfExecNode::decode(buf) {
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
                tokio::runtime::Handle::current()
                    .block_on(async { table_provider.scan(&session_state, None, &[], None).await })
            })?;

            Ok(Arc::new(UdtfExec::new(args, inner_plan)))
        } else {
            exec_err!("Cannot deserialize unknown execution plan")
        }
    }

    fn try_encode(&self, node: Arc<dyn ExecutionPlan>, buf: &mut Vec<u8>) -> Result<()> {
        if let Some(concrete) = node.as_any().downcast_ref::<SchemaCastScanExec>() {
            let mut schema_buf = vec![];
            let serialized_schema = datafusion_common::Schema::try_from(concrete.schema())?;
            serialized_schema
                .encode(&mut schema_buf)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            let node = SchemaCastScanExecNode { schema: schema_buf };
            node.encode(buf)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
        } else if let Some(bytes_exec) = node.as_any().downcast_ref::<BytesProcessedExec>() {
            let request_context = bytes_exec
                .request_context()
                .map(|ctx| request_context_to_proto(ctx));
            let node = BytesProcessedExecNode { request_context };
            node.encode(buf)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
        } else if let Some(udtf_exec) = node.as_any().downcast_ref::<UdtfExec>() {
            // Serialize the UdtfExec with its args and schema
            let mut schema_buf = vec![];
            let serialized_schema = datafusion_common::Schema::try_from(udtf_exec.schema())?;
            serialized_schema
                .encode(&mut schema_buf)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            let node = UdtfExecNode {
                args: Some(udtf_exec.args().clone()),
                schema: schema_buf,
            };
            node.encode(buf)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
        } else {
            #[cfg(not(windows))]
            if node
                .as_any()
                .downcast_ref::<CayenneAccelerationExec>()
                .is_some()
            {
                let node = CayenneAccelerationExecNode {};
                node.encode(buf)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
            } else {
                return self.inner.try_encode(node, buf);
            }
            #[cfg(windows)]
            {
                return self.inner.try_encode(node, buf);
            }
        }

        Ok(())
    }

    fn try_decode_udf(&self, name: &str, _buf: &[u8]) -> Result<Arc<ScalarUDF>> {
        self.runtime()?.df.ctx.udf(name)
    }
}

/// Serialize the essential fields of a [`RequestContext`] into a proto message.
fn request_context_to_proto(ctx: &RequestContext) -> RequestContextProto {
    let (trace_id, span_id) = ctx.trace_parent().as_ref().map_or((None, None), |tp| {
        (Some(tp.trace_id.to_string()), Some(tp.span_id.to_string()))
    });

    RequestContextProto {
        protocol: ctx.protocol() as u32,
        trace_id,
        span_id,
        authorization_header: ctx.authorization_header().map(str::to_string),
    }
}

/// Reconstruct a [`RequestContext`] from its proto representation, if present.
fn request_context_from_proto(proto: Option<RequestContextProto>) -> Option<RequestContext> {
    let proto = proto?;

    let protocol = Protocol::from(proto.protocol as u8);

    let trace_parent = match (proto.trace_id, proto.span_id) {
        (Some(tid), Some(sid)) => {
            let trace_id = TraceId::from_hex(&tid).ok()?;
            let span_id = SpanId::from_hex(&sid).ok()?;
            Some(TraceParent { trace_id, span_id })
        }
        _ => None,
    };

    let ctx = RequestContextBuilder::new(protocol)
        .with_trace_parent(trace_parent)
        .with_authorization_header(proto.authorization_header)
        .build();

    Some(ctx)
}
