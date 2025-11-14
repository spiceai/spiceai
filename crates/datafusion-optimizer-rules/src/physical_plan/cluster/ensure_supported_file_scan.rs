use crate::common::search_visitor::SearchVisitor;
use crate::concrete;
use datafusion::common::{Result, plan_err};
use datafusion::config::ConfigOptions;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_datasource::file_scan_config::FileScanConfig;
use datafusion_datasource::memory::MemorySourceConfig;
use datafusion_datasource::source::DataSourceExec;
use std::sync::Arc;

/// An optimizer to sanity check `DataSourceExec` encapsulate the kinds of plans
/// we can distribute
#[derive(Debug, Clone)]
pub struct EnsureSupportedFileScan {}

impl EnsureSupportedFileScan {
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(EnsureSupportedFileScan {})
    }

    fn name() -> &'static str {
        "EnsureSerializableFileScanOptimizer"
    }

    fn validate(plan: &Arc<dyn ExecutionPlan>) -> Result<()> {
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

        if concrete!(data_source_exec.data_source(), FileScanConfig).is_none() {
            return plan_err!(
                "{}: does not support {} scans",
                Self::name(),
                std::any::type_name_of_val(data_source_exec.data_source().as_ref())
            );
        }

        Ok(())
    }
}

impl PhysicalOptimizerRule for EnsureSupportedFileScan {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let _ = SearchVisitor::collect_concrete_down::<DataSourceExec>(&plan)?
            .into_iter()
            .map(|p| Self::validate(&p))
            .collect::<Result<Vec<_>>>()?;

        Ok(plan)
    }

    fn name(&self) -> &str {
        Self::name()
    }

    fn schema_check(&self) -> bool {
        true
    }
}
