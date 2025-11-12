use crate::common::plan_node_key::PlanNodeKey;
use crate::common::search_visitor::SearchVisitor;
use crate::concrete;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Result, plan_err};
use datafusion::config::ConfigOptions;
use datafusion::error::DataFusionError;
use datafusion::object_store;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_datasource::file_scan_config::FileScanConfig;
use datafusion_datasource::memory::MemorySourceConfig;
use datafusion_datasource::source::DataSourceExec;
use std::collections::HashMap;
use std::sync::Arc;
use url::Url;

#[derive(Debug, Clone)]
pub struct EnsureSerializableFileScanOptimizer {}

impl EnsureSerializableFileScanOptimizer {
    pub fn new() -> Arc<Self> {
        Arc::new(EnsureSerializableFileScanOptimizer {})
    }

    fn name() -> &'static str {
        "EnsureSerializableFileScanOptimizer"
    }

    fn ensure_and_rewrite(plan: Arc<dyn ExecutionPlan>) -> Result<(PlanNodeKey, DataSourceExec)> {
        let Some(data_source_exec) = concrete!(plan, DataSourceExec) else {
            return plan_err!(
                "{} only operates on DataSourceExec. This is a bug.",
                Self::name()
            );
        };

        if concrete!(data_source_exec.data_source(), MemorySourceConfig).is_some() {
            return plan_err!(
                "{}: DataSourceExec with MemorySourceConfig cannot be distributed. Use file-based or remote data sources instead.",
                Self::name()
            );
        }

        let Some(file_scan_config) = concrete!(data_source_exec.data_source(), FileScanConfig)
        else {
            return plan_err!(
                "{}: does not support {} scans",
                Self::name(),
                std::any::type_name_of_val(data_source_exec.data_source().as_ref())
            );
        };

        let url = Url::parse(file_scan_config.object_store_url.as_str())
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let store =
            object_store::parse_url(&url).map_err(|e| DataFusionError::External(Box::new(e)))?;

        println!("file scan config {:?}", file_scan_config);
        println!("store {:?}", store);

        Ok((PlanNodeKey::from(plan.as_ref()), data_source_exec.clone()))
    }
}

impl PhysicalOptimizerRule for EnsureSerializableFileScanOptimizer {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut rewrites = SearchVisitor::collect_concrete_down::<DataSourceExec>(&plan)?
            .into_iter()
            .map(Self::ensure_and_rewrite)
            .collect::<Result<HashMap<_, _>>>()?;

        if rewrites.is_empty() {
            return Ok(plan);
        }

        let transformed = plan
            .transform_down(|p| {
                let node_key = PlanNodeKey::from(p.as_ref());
                if let Some(new_plan_node) = rewrites.remove(&node_key) {
                    Ok(Transformed::yes(
                        Arc::new(new_plan_node) as Arc<dyn ExecutionPlan>
                    ))
                } else {
                    Ok(Transformed::no(p))
                }
            })
            .map(|t| t.data)?;

        if rewrites.is_empty() {
            Ok(transformed)
        } else {
            plan_err!(
                "{}: failed to rewrite all plan nodes. This is a bug.",
                Self::name()
            )
        }
    }

    fn name(&self) -> &str {
        Self::name()
    }

    fn schema_check(&self) -> bool {
        true
    }
}
