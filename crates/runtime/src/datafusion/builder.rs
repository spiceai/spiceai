/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use std::{
    collections::{HashMap, HashSet},
    num::NonZeroUsize,
    sync::{Arc, OnceLock, RwLock},
};

use super::{
    DataFusion, SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA, SPICE_METADATA_SCHEMA,
    SPICE_RUNTIME_SCHEMA,
};
#[cfg(feature = "cluster")]
use crate::config::ClusterConfig;
use crate::{dataaccelerator::AcceleratorEngineRegistry, datafusion::SPICE_SCP_SCHEMA};
use crate::{metrics::telemetry::track_bytes_processed, status};
use cache::Caching;
use datafusion::{
    catalog::{CatalogProvider, MemoryCatalogProvider},
    execution::{
        DiskManager, SessionStateBuilder,
        disk_manager::DiskManagerMode,
        memory_pool::{FairSpillPool, MemoryPool, TrackConsumersPool, UnboundedMemoryPool},
        runtime_env::{RuntimeEnv, RuntimeEnvBuilder},
    },
    optimizer::{
        AnalyzerRule,
        analyzer::{
            resolve_grouping_function::ResolveGroupingFunction, type_coercion::TypeCoercion,
        },
    },
    prelude::{SessionConfig, SessionContext},
};
use datafusion::{config::SpillCompression, physical_planner::ExtensionPlanner};
use datafusion_federation::{FederatedPlanner, sql::federation_analyzer_rule};

#[cfg(feature = "duckdb")]
use {
    datafusion_optimizer_rules::logical_plan::duckdb::aggregate_pushdown::DuckDBAggregateLogicalPushdown,
    datafusion_optimizer_rules::logical_plan::duckdb::planner::DuckDBLogicalExtensionPlanner,
    datafusion_optimizer_rules::physical_plan::duckdb::aggregate_pushdown::DuckDBAggregatePushdownRewriter,
    datafusion_optimizer_rules::physical_plan::duckdb::intermediate_index_cte::DuckDBIntermediateIndexMaterializationOptimizer,
};

use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_optimizer::optimizer::PhysicalOptimizer;
use datafusion_optimizer_rules::{
    logical_plan::{
        CacheInvalidationExtensionPlanner, cache_invalidation::CacheInvalidationOptimizerRule,
    },
    physical_plan::EmptyHashJoinExecPhysicalOptimization,
};
use runtime_datafusion::{
    extension::{ExtensionPlanQueryPlanner, bytes_processed::BytesProcessedPhysicalOptimizer},
    schema_provider::SpiceSchemaProvider,
};
use runtime_datafusion_index::analyzer::IndexTableScanExtensionPlanner;
use runtime_object_store::registry::SpiceObjectStoreRegistry;
use spicepod::component::runtime::SpillCompression as SpiceSpillCompression;
use spicepod::metric::Metrics;
use std::sync::LazyLock;
use tokio::{
    runtime::Handle,
    sync::{RwLock as TokioRwLock, Semaphore},
};

pub static DEFAULT_DATAFUSION_CONFIG: LazyLock<RwLock<SessionConfig>> = LazyLock::new(|| {
    let mut df_config = SessionConfig::new();

    // Prevents DataFusion from lowercasing identifiers, i.e. "SELECT MyColumn FROM my_table" would be "SELECT mycolumn FROM mytable" without this.
    // This improves the UX for data sources where column names are case-sensitive, since they no longer need to be quoted.
    df_config
        .options_mut()
        .sql_parser
        .enable_ident_normalization = false;

    df_config.options_mut().optimizer.expand_views_at_output = true;
    df_config.options_mut().sql_parser.dialect = "PostgreSQL".to_string();
    df_config
        .options_mut()
        .execution
        .listing_table_ignore_subdirectory = false;

    // There are some unidentified bugs in DataFusion that cause schema checks to fail for aggregate functions.
    // Spice is affected by this - skip the check until all bugs are fixed.
    // Tracking issue: https://github.com/apache/datafusion/issues/12733
    df_config
        .options_mut()
        .execution
        .skip_physical_aggregate_schema_check = true;

    // Enabling parquet filter pushdown can improve query performance by applying filters while decoding
    // https://docs.rs/datafusion/latest/datafusion/config/struct.ParquetOptions.html#structfield.pushdown_filters
    df_config.options_mut().execution.parquet.pushdown_filters = true;

    RwLock::new(df_config)
});

pub struct DataFusionBuilder {
    config: SessionConfig,
    status: Arc<status::RuntimeStatus>,
    accelerator_engine_registry: Arc<AcceleratorEngineRegistry>,
    memory_limit: Option<u64>,
    temp_directory: Option<String>,
    accelerated_refresh_semaphore: Option<Arc<Semaphore>>,
    task_history_enabled: bool,
    caching: Option<Arc<Caching>>,
    spill_compression: Option<SpillCompression>,
    #[cfg(feature = "cluster")]
    cluster_config: Arc<ClusterConfig>,
    metrics: Option<Metrics>,
    io_runtime: Handle,
    resource_monitor: Option<crate::resource_monitor::ResourceMonitor>,
}

pub(crate) fn get_df_default_config() -> SessionConfig {
    match DEFAULT_DATAFUSION_CONFIG.read() {
        Ok(config) => config.clone(),
        _ => panic!("Failed to read default DataFusion config. This is a bug."),
    }
}

impl DataFusionBuilder {
    /// Creates a new `DataFusionBuilder` with the runtime defaults.
    ///
    /// # Panics
    ///
    /// Panics if a managed Tokio runtime cannot be created. This indicates a bug in the runtime initialization.
    #[must_use]
    pub fn new(
        status: Arc<status::RuntimeStatus>,
        accelerator_engine_registry: Arc<AcceleratorEngineRegistry>,
        io_runtime: Handle,
    ) -> Self {
        let mut df_config = get_df_default_config()
            .with_information_schema(true)
            .with_create_default_catalog_and_schema(false);

        df_config.options_mut().catalog.default_catalog = SPICE_DEFAULT_CATALOG.to_string();
        df_config.options_mut().catalog.default_schema = SPICE_DEFAULT_SCHEMA.to_string();

        Self {
            config: df_config,
            status,
            accelerator_engine_registry,
            memory_limit: None,
            temp_directory: None,
            accelerated_refresh_semaphore: None,
            task_history_enabled: true,
            caching: None,
            spill_compression: None,
            #[cfg(feature = "cluster")]
            cluster_config: Arc::new(ClusterConfig::default()),
            metrics: None,
            io_runtime,
            resource_monitor: None,
        }
    }

    #[must_use]
    pub fn with_task_history(mut self, task_history: bool) -> Self {
        self.task_history_enabled = task_history;
        self
    }

    #[must_use]
    pub fn with_caching(mut self, caching: Arc<Caching>) -> Self {
        self.caching = Some(caching);
        self
    }

    #[cfg(feature = "cluster")]
    #[must_use]
    pub fn with_cluster_config(mut self, config: Arc<ClusterConfig>) -> Self {
        self.cluster_config = config;
        self
    }

    #[must_use]
    pub fn memory_limit(mut self, memory_limit: Option<u64>) -> Self {
        self.memory_limit = memory_limit;
        self
    }

    #[must_use]
    pub fn spill_compression(mut self, spill_compression: Option<SpiceSpillCompression>) -> Self {
        self.spill_compression = match spill_compression {
            Some(SpiceSpillCompression::Zstd) => Some(SpillCompression::Zstd),
            Some(SpiceSpillCompression::Lz4Frame) => Some(SpillCompression::Lz4Frame),
            Some(SpiceSpillCompression::Uncompressed) => Some(SpillCompression::Uncompressed),
            None => None,
        };
        self
    }

    #[must_use]
    pub fn temp_directory(mut self, temp_directory: Option<String>) -> Self {
        self.temp_directory = temp_directory;
        self
    }

    #[must_use]
    pub fn max_parallel_accelerated_refreshes(
        mut self,
        max_parallel_accelerated_refreshes: usize,
    ) -> Self {
        self.accelerated_refresh_semaphore =
            Some(Arc::new(Semaphore::new(max_parallel_accelerated_refreshes)));
        self
    }

    #[must_use]
    pub fn with_metrics(mut self, metrics: Option<Metrics>) -> Self {
        self.metrics = metrics;
        self
    }

    #[must_use]
    pub fn with_resource_monitor(
        mut self,
        monitor: crate::resource_monitor::ResourceMonitor,
    ) -> Self {
        self.resource_monitor = Some(monitor);
        self
    }

    /// Builds the `DataFusion` instance.
    ///
    /// # Panics
    ///
    /// Panics if the `DataFusion` instance cannot be built due to errors in registering functions or schemas.
    #[must_use]
    #[expect(clippy::too_many_lines)]
    pub fn build(self) -> DataFusion {
        let mut config = self.config;

        if let Some(spill_compression) = self.spill_compression {
            config = config.with_spill_compression(spill_compression);
        }

        let mut state = SessionStateBuilder::new()
            .with_config(config)
            .with_default_features()
            .with_query_planner(Arc::new(
                ExtensionPlanQueryPlanner::from_extension_planners(default_extension_planners()),
            ))
            .with_runtime_env(runtime_env(
                self.memory_limit,
                self.temp_directory.clone(),
                self.io_runtime.clone(),
            ))
            .with_analyzer_rules(AnalyzerRulesBuilder::default().build());

        #[cfg(feature = "duckdb")]
        {
            let mut physical_optimizers_with_duckdb: Vec<
                Arc<dyn PhysicalOptimizerRule + Send + Sync>,
            > = vec![
                DuckDBAggregatePushdownRewriter::new(),
                DuckDBIntermediateIndexMaterializationOptimizer::new(),
            ];

            physical_optimizers_with_duckdb.extend(
                state
                    .physical_optimizer_rules()
                    .clone()
                    .unwrap_or_else(|| PhysicalOptimizer::new().rules),
            );

            state = state
                .with_optimizer_rule(DuckDBAggregateLogicalPushdown::new())
                .with_physical_optimizer_rules(physical_optimizers_with_duckdb);
        }

        state = state
            .with_physical_optimizer_rule(Arc::new(EmptyHashJoinExecPhysicalOptimization {}))
            .with_physical_optimizer_rule(Arc::new(BytesProcessedPhysicalOptimizer::new(
                Arc::new(Box::new(track_bytes_processed)),
            )));

        let mut state = state.build();

        if let Err(e) = datafusion_functions_json::register_all(&mut state) {
            panic!("Unable to register JSON functions: {e}");
        }

        if let Err(e) = datafusion_spark::register_all(&mut state) {
            panic!("Unable to register Spark functions: {e}");
        }

        let ctx = SessionContext::new_with_state(state);

        // Add cache invalidation optimizer rule if caching is enabled
        if let Some(caching) = &self.caching {
            ctx.add_optimizer_rule(Arc::new(CacheInvalidationOptimizerRule::new(
                Arc::downgrade(caching),
            )));
        }

        let catalog = MemoryCatalogProvider::new();
        let default_schema = SpiceSchemaProvider::new();
        let runtime_schema = SpiceSchemaProvider::new();

        let metadata_schema = SpiceSchemaProvider::new();

        match catalog.register_schema(SPICE_DEFAULT_SCHEMA, Arc::new(default_schema)) {
            Ok(_) => {}
            Err(e) => {
                panic!("Unable to register default schema: {e}");
            }
        }

        match catalog.register_schema(SPICE_RUNTIME_SCHEMA, Arc::new(runtime_schema)) {
            Ok(_) => {}
            Err(e) => {
                panic!("Unable to register spice runtime schema: {e}");
            }
        }

        if cfg!(feature = "models") {
            use super::SPICE_EVAL_SCHEMA;
            let eval_schema = SpiceSchemaProvider::new();
            match catalog.register_schema(SPICE_EVAL_SCHEMA, Arc::new(eval_schema)) {
                Ok(_) => {}
                Err(e) => {
                    panic!("Unable to register spice eval schema: {e}");
                }
            }
        }

        match catalog.register_schema(SPICE_METADATA_SCHEMA, Arc::new(metadata_schema)) {
            Ok(_) => {}
            Err(e) => {
                panic!("Unable to register spice metadata schema: {e}");
            }
        }

        match catalog.register_schema(SPICE_SCP_SCHEMA, Arc::new(SpiceSchemaProvider::new())) {
            Ok(_) => {}
            Err(e) => {
                panic!("Unable to register spice cloud platform schema: {e}");
            }
        }

        ctx.register_catalog(SPICE_DEFAULT_CATALOG, Arc::new(catalog));

        let caching = self.caching.unwrap_or(Arc::new(Caching::default()));

        DataFusion {
            runtime_status: self.status,
            ctx: Arc::new(ctx),
            data_writers: RwLock::new(HashSet::new()),
            writable_catalogs: RwLock::new(HashSet::new()),
            caching,
            pending_sink_tables: TokioRwLock::new(Vec::new()),
            deferred_tables: TokioRwLock::new(HashMap::new()),
            deferred_catalogs: TokioRwLock::new(HashMap::new()),
            accelerated_tables: TokioRwLock::new(HashSet::new()),
            accelerator_engine_registry: self.accelerator_engine_registry,
            acceleration_refresh_semaphore: self.accelerated_refresh_semaphore,
            task_history_enabled: self.task_history_enabled,
            temp_directory: self.temp_directory.clone(),
            cpu_runtime: OnceLock::new(),
            refresh_runtime: OnceLock::new(),
            io_runtime: self.io_runtime,
            metrics: self.metrics,
            resource_monitor: self.resource_monitor,
            #[cfg(feature = "cluster")]
            cluster_config: self.cluster_config,
            #[cfg(feature = "cluster")]
            scheduler_server: RwLock::new(None),
            #[cfg(feature = "cluster")]
            executor: RwLock::new(None),
        }
    }
}

pub struct AnalyzerRulesBuilder {
    include_federation: bool,
    extra_rules: Vec<Arc<dyn AnalyzerRule + Send + Sync>>,
}

impl AnalyzerRulesBuilder {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn include_federation(mut self, include: bool) -> Self {
        self.include_federation = include;
        self
    }

    #[must_use]
    pub fn with_extra_rules(
        mut self,
        extra_rules: impl IntoIterator<Item = Arc<dyn AnalyzerRule + Send + Sync>>,
    ) -> Self {
        self.extra_rules.extend(extra_rules);
        self
    }

    /// Spice customizes the order of the analyzer rules, since some of them are only relevant when `DataFusion` is executing the query,
    /// as opposed to when underlying federated query engines will execute the query.
    ///
    /// This list should be kept in sync with the default rules in `Analyzer::new()`, but with the federation analyzer rule added first.
    #[must_use]
    pub fn build(self) -> Vec<Arc<dyn AnalyzerRule + Send + Sync>> {
        let mut rules: Vec<Arc<dyn AnalyzerRule + Send + Sync>> = vec![];
        if self.include_federation {
            rules.push(Arc::new(federation_analyzer_rule()));
        }
        // The rest of these rules are run after the federation analyzer since they only affect internal DataFusion execution.
        rules.extend([
            Arc::new(ResolveGroupingFunction::new()) as Arc<dyn AnalyzerRule + Send + Sync>,
            Arc::new(TypeCoercion::new()) as Arc<dyn AnalyzerRule + Send + Sync>,
        ]);
        rules.into_iter().chain(self.extra_rules).collect()
    }
}

impl Default for AnalyzerRulesBuilder {
    fn default() -> Self {
        Self {
            include_federation: true,
            extra_rules: vec![],
        }
    }
}

// This method uses unwrap_or_default, however it should never fail on the initialization. See
// RuntimeEnv::default()
pub(crate) fn runtime_env(
    memory_limit: Option<u64>,
    temp_directory: Option<String>,
    io_runtime: Handle,
) -> Arc<RuntimeEnv> {
    let disk_manager_builder = if let Some(directory) = temp_directory {
        let mode = DiskManagerMode::Directories(vec![directory.into()]);
        DiskManager::builder().with_mode(mode)
    } else {
        DiskManager::builder()
    };

    // If no memory limit is specified, default to 70% of total memory (container-aware)
    let effective_memory_limit = memory_limit.or_else(|| {
        let total_memory = crate::resource_monitor::get_total_memory();
        #[expect(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            clippy::cast_precision_loss
        )]
        let default_limit = (total_memory as f64 * 0.70) as u64;

        tracing::debug!(
            "No memory limit specified, defaulting to 70% of total memory: {}",
            {
                #[expect(clippy::cast_possible_truncation)]
                util::human_readable_bytes(default_limit as usize)
            }
        );
        Some(default_limit)
    });

    let memory_pool: Arc<dyn MemoryPool> = if let Some(limit) = effective_memory_limit {
        let limit = if let Ok(limit) = limit.try_into() {
            limit
        } else {
            tracing::warn!(
                "Memory limit {limit} is too large for the memory pool.\n Defaulting to a maximum sized pool of {}.",
                usize::MAX
            );

            usize::MAX
        };

        let Some(topn) = NonZeroUsize::new(5) else {
            unreachable!("Memory pool TopN must be greater than 0");
        };

        Arc::new(TrackConsumersPool::new(FairSpillPool::new(limit), topn))
    } else {
        let Some(topn) = NonZeroUsize::new(5) else {
            unreachable!("Memory pool TopN must be greater than 0");
        };

        Arc::new(TrackConsumersPool::new(
            UnboundedMemoryPool::default(),
            topn,
        ))
    };

    match RuntimeEnvBuilder::default()
        .with_object_store_registry(Arc::new(SpiceObjectStoreRegistry::new(io_runtime)))
        .with_memory_pool(memory_pool)
        .with_disk_manager_builder(disk_manager_builder)
        .build_arc()
    {
        Ok(runtime_env) => runtime_env,
        Err(e) => {
            unreachable!("Tests ensure this should never fail: {e}");
        }
    }
}

pub(crate) fn default_extension_planners() -> Vec<Arc<dyn ExtensionPlanner + Send + Sync>> {
    vec![
        Arc::new(IndexTableScanExtensionPlanner::new()),
        Arc::new(FederatedPlanner::new()),
        Arc::new(CacheInvalidationExtensionPlanner::new()),
        #[cfg(feature = "duckdb")]
        DuckDBLogicalExtensionPlanner::new(),
    ]
}

#[cfg(test)]
mod tests {
    use datafusion::optimizer::Analyzer;

    /// Verifies that the default analyzer rules are in the expected order.
    ///
    /// If this test fails, `DataFusion` has modified the default analyzer rules and `AnalyzerRulesBuilder::build()` should be updated.
    #[test]
    fn test_verify_default_analyzer_rules() {
        let default_rules = Analyzer::new().rules;
        assert_eq!(
            default_rules.len(),
            2,
            "Default analyzer rules have changed"
        );
        let expected_rule_names = vec!["resolve_grouping_function", "type_coercion"];
        for (rule, expected_name) in default_rules.iter().zip(expected_rule_names.into_iter()) {
            assert_eq!(
                expected_name,
                rule.name(),
                "Default analyzer rule order has changed"
            );
        }
    }
}
