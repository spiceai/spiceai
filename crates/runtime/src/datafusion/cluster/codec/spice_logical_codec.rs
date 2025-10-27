use crate::Runtime;
use arrow_schema::SchemaRef;
use ballista_core::serde::BallistaLogicalExtensionCodec;
use datafusion::catalog::TableProvider;
use datafusion::common::{DataFusionError, Result, TableReference, exec_err};
use datafusion::prelude::SessionContext;
use datafusion_expr::registry::FunctionRegistry;
use datafusion_expr::{Extension, LogicalPlan, ScalarUDF};
use datafusion_proto::logical_plan::LogicalExtensionCodec;
use runtime_datafusion::extension::bytes_processed::BytesProcessedNode;
use std::fmt::Debug;
use std::sync::Arc;

/// Serialization support for custom Spice logical nodes
pub struct SpiceLogicalCodec {
    inner: Arc<dyn LogicalExtensionCodec>,
    runtime: Option<Arc<Runtime>>,
}

impl Debug for SpiceLogicalCodec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SpiceLogicalCodec")
    }
}

impl SpiceLogicalCodec {
    #[must_use]
    pub fn new_with_runtime(runtime: Arc<Runtime>) -> Arc<dyn LogicalExtensionCodec> {
        Arc::new(Self {
            inner: Arc::new(BallistaLogicalExtensionCodec::default()),
            runtime: Some(runtime),
        })
    }

    #[must_use]
    pub fn new_codec() -> Arc<dyn LogicalExtensionCodec> {
        Arc::new(Self {
            inner: Arc::new(BallistaLogicalExtensionCodec::default()),
            runtime: None,
        })
    }

    fn runtime(&self) -> Result<Arc<Runtime>> {
        self.runtime.clone().ok_or(DataFusionError::Execution(
            "SpiceLogicalCodec did not bind a Runtime handle. Report this bug on GitHub: https://github.com/spiceai/spiceai/issues".to_string(),
        ))
    }
}

impl LogicalExtensionCodec for SpiceLogicalCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[LogicalPlan],
        ctx: &SessionContext,
    ) -> Result<Extension> {
        if let Ok(ext) = self.inner.try_decode(buf, inputs, ctx) {
            return Ok(ext);
        }

        let name = serde_json::from_slice::<String>(buf)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let node = match name.as_str() {
            "BytesProcessedNode" => Extension {
                node: Arc::new(BytesProcessedNode::new(inputs[0].clone())),
            },
            other => {
                return exec_err!(
                    "SpiceLogicalCodec does not support {other}. Report this bug on GitHub: https://github.com/spiceai/spiceai/issues"
                );
            }
        };

        Ok(node)
    }

    fn try_encode(&self, node: &Extension, buf: &mut Vec<u8>) -> Result<()> {
        if let Ok(()) = self.inner.try_encode(node, buf) {
            return Ok(());
        }

        let node_name = serde_json::to_vec(node.node.name())
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        buf.extend_from_slice(&node_name[..]);
        Ok(())
    }

    // Look up the table ref in the context instead of ser/de
    fn try_decode_table_provider(
        &self,
        _buf: &[u8],
        table_ref: &TableReference,
        _schema: SchemaRef,
        _ctx: &SessionContext,
    ) -> Result<Arc<dyn TableProvider>> {
        if let Some(table_provider) = self.runtime()?.df.get_table_sync(table_ref) {
            Ok(table_provider)
        } else {
            exec_err!(
                "SpiceLogicalCodec could not resolve table reference {}. Report this bug on GitHub: https://github.com/spiceai/spiceai/issues",
                table_ref
            )
        }
    }

    // no-op
    fn try_encode_table_provider(
        &self,
        _table_ref: &TableReference,
        _node: Arc<dyn TableProvider>,
        _buf: &mut Vec<u8>,
    ) -> Result<()> {
        Ok(())
    }

    fn try_decode_udf(&self, name: &str, _buf: &[u8]) -> Result<Arc<ScalarUDF>> {
        self.runtime()?.df.ctx.udf(name)
    }
}
