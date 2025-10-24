use crate::Runtime;
use crate::datafusion::extension::bytes_processed::BytesProcessedExec;
use crate::execution_plan::schema_cast::SchemaCastScanExec;
use arrow_schema::Schema;
use ballista_core::serde::BallistaPhysicalExtensionCodec;
use datafusion::common::{DataFusionError, Result, exec_err};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_datasource::memory::MemorySourceConfig;
use datafusion_datasource::source::DataSourceExec;
use datafusion_expr::ScalarUDF;
use datafusion_expr::registry::FunctionRegistry;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use serde_json::Value;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

macro_rules! deserialize {
    ($map: ident, $key: expr, $t: ty) => {
        $map.get($key)
            .ok_or(DataFusionError::Execution(format!("{} is missing", $key)))
            .and_then(|v| {
                serde_json::from_value::<$t>(v.clone())
                    .map_err(|e| DataFusionError::External(Box::new(e)))
            })
    };
}

const SPICE_EXEC_NAME: &str = "spice.exec.name";

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
        registry: &dyn FunctionRegistry,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if let Ok(plan) = self.inner.try_decode(buf, inputs, registry) {
            return Ok(plan);
        }

        let exec_params = serde_json::from_slice::<HashMap<String, Value>>(buf)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        match exec_params.get(SPICE_EXEC_NAME).and_then(|v| v.as_str()) {
            Some("SchemaCastScanExec") => {
                let schema = deserialize!(exec_params, "spice.exec.schema", Schema)?;
                let exec = Arc::new(SchemaCastScanExec::new(
                    Arc::clone(&inputs[0]),
                    Arc::new(schema),
                ));

                Ok(exec)
            }
            Some("BytesProcessedExec") => {
                // TODO: Make RequestContext serializable
                Ok(Arc::new(BytesProcessedExec::new(Arc::clone(&inputs[0]))))
            }
            _ => exec_err!("Unsupported spice.exec.name"),
        }
    }

    fn try_encode(&self, node: Arc<dyn ExecutionPlan>, buf: &mut Vec<u8>) -> Result<()> {
        let mut map: HashMap<&str, Value> = HashMap::new();
        map.insert(SPICE_EXEC_NAME, node.name().into());

        match node.name() {
            "SchemaCastScanExec" => {
                let Some(concrete) = node.as_any().downcast_ref::<SchemaCastScanExec>() else {
                    return exec_err!("Unable to serialize plan node");
                };

                map.insert(
                    "spice.exec.schema",
                    serde_json::to_value(concrete.schema())
                        .map_err(|e| DataFusionError::Execution(e.to_string()))?,
                );
            }
            "BytesProcessedExec" => { /* no-op */ }
            "DataSourceExec" => {
                let Some(concrete) = node.as_any().downcast_ref::<DataSourceExec>() else {
                    return exec_err!("Unable to serialize plan node");
                };

                let data_source = concrete.data_source();

                // Clearer error message instead of "unsupported plan"
                if data_source
                    .as_any()
                    .downcast_ref::<MemorySourceConfig>()
                    .is_some()
                {
                    return exec_err!(
                        "Memory source scans cannot be distributed across cluster nodes. Use file-based or remote data sources instead."
                    );
                }

                return self.inner.try_encode(node, buf);
            }
            _ => return self.inner.try_encode(node, buf),
        }

        let serialized =
            serde_json::to_vec(&map).map_err(|e| DataFusionError::External(Box::new(e)))?;
        buf.extend_from_slice(&serialized);

        Ok(())
    }

    fn try_decode_udf(&self, name: &str, _buf: &[u8]) -> Result<Arc<ScalarUDF>> {
        self.runtime()?.df.ctx.udf(name)
    }
}
