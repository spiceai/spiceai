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
#[cfg(not(windows))]
use crate::accelerated_table::AcceleratedTable;
use crate::cluster::ExecutorRegistry;
use crate::cluster::ResolvedClusterConfig;
#[cfg(not(windows))]
use crate::dataaccelerator::upsert_dedup::UpsertDedupTableProvider;
use crate::{config::ClusterRole, metrics::telemetry::track_bytes_processed, status};
use crate::{dataaccelerator::AcceleratorEngineRegistry, datafusion::SPICE_SCP_SCHEMA};
use cache::Caching;
#[cfg(not(windows))]
use cayenne::optimizer_rules::{
    CayenneAntiJoinSortMergeRewriter, CayenneDynamicFilterSharing, CayenneOptimizerConfig,
};
#[cfg(not(windows))]
use cayenne::{CayenneTableProvider, logical_optimizer::CayennePropagateFilterAcrossEquiJoinKeys};
#[cfg(not(windows))]
use data_components::poly::PolyTableProvider;
#[cfg(not(windows))]
use datafusion::catalog::TableProvider;
#[cfg(not(windows))]
use datafusion::optimizer::{Optimizer, OptimizerRule};
use datafusion::{
    catalog::{CatalogProvider, MemoryCatalogProvider},
    execution::{
        DiskManager, SessionStateBuilder,
        disk_manager::DiskManagerMode,
        memory_pool::{GreedyMemoryPool, TrackConsumersPool},
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
use runtime_datafusion::analyzer_rule::{PartitionedTableScanRewrite, TablePartitionProvider};

#[cfg(feature = "duckdb")]
use {
    datafusion_optimizer_rules::logical_plan::duckdb::aggregate_pushdown::DuckDBAggregateLogicalPushdown,
    datafusion_optimizer_rules::logical_plan::duckdb::planner::DuckDBLogicalExtensionPlanner,
    datafusion_optimizer_rules::physical_plan::duckdb::aggregate_pushdown::DuckDBAggregatePushdownRewriter,
    datafusion_optimizer_rules::physical_plan::duckdb::intermediate_index_cte::DuckDBIntermediateIndexMaterializationOptimizer,
};

use crate::cluster::partition::service::PartitionService;
#[cfg(feature = "duckdb")]
use datafusion::physical_optimizer::PhysicalOptimizerRule;
#[cfg(feature = "duckdb")]
use datafusion::physical_optimizer::optimizer::PhysicalOptimizer;
use datafusion_optimizer_rules::{
    logical_plan::{
        CacheInvalidationExtensionPlanner, cache_invalidation::CacheInvalidationOptimizerRule,
    },
    physical_plan::{
        EmptyHashJoinExecPhysicalOptimization, HttpParamsPushdown,
        flightsql::aggregate_pushdown::FlightSQLPartialAggregatePushdown,
    },
};
#[cfg(not(windows))]
use runtime_datafusion::join_accumulator::clamp_maximum_shared_inlist_memory_bytes;
use runtime_datafusion::{
    extension::{
        ExtensionPlanQueryPlanner, bytes_processed::BytesProcessedPhysicalOptimizer,
        data_source_tree_display::DataSourceTreeDisplayOptimizer,
    },
    schema_provider::SpiceSchemaProvider,
    url_table::{DynamicUrlCatalogList, SpiceUrlTableFactory},
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
    df_config.options_mut().sql_parser.dialect = datafusion::common::config::Dialect::PostgreSQL;
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

const EXACT_JOIN_FILTER_MEMORY_POOL_FRACTION_DENOMINATOR: u64 = 8;

pub struct DataFusionBuilder {
    config: SessionConfig,
    status: Arc<status::RuntimeStatus>,
    accelerator_engine_registry: Arc<AcceleratorEngineRegistry>,
    memory_limit: Option<u64>,
    target_partitions: Option<usize>,
    temp_directory: Option<String>,
    accelerated_refresh_semaphore: Option<Arc<Semaphore>>,
    task_history_enabled: bool,
    caching: Option<Arc<Caching>>,
    spill_compression: Option<SpillCompression>,
    cluster_config: Option<Arc<ResolvedClusterConfig>>,
    metrics: Option<Metrics>,
    io_runtime: Handle,
    resource_monitor: Option<crate::resource_monitor::ResourceMonitor>,
    url_tables_enabled: bool,
    cayenne_sort_merge_min_rows: Option<usize>,
    cayenne_sort_merge_memory_pool_fraction: Option<f64>,
    /// Arbitrary additional analyzer rules.
    additional_analyzer_rules: Vec<Arc<dyn AnalyzerRule + Send + Sync>>,
    executor_registry: Option<Arc<ExecutorRegistry>>,
    partition_service: Option<Arc<PartitionService>>,
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
            target_partitions: None,
            temp_directory: None,
            accelerated_refresh_semaphore: None,
            task_history_enabled: true,
            caching: None,
            spill_compression: None,
            cluster_config: None,
            metrics: None,
            io_runtime,
            resource_monitor: None,
            url_tables_enabled: false,
            cayenne_sort_merge_min_rows: None,
            cayenne_sort_merge_memory_pool_fraction: None,
            additional_analyzer_rules: vec![],
            executor_registry: None,
            partition_service: None,
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

    #[must_use]
    pub fn with_cluster_config(mut self, config: ResolvedClusterConfig) -> Self {
        self.cluster_config = Some(Arc::new(config));
        self
    }

    #[must_use]
    pub fn memory_limit(mut self, memory_limit: Option<u64>) -> Self {
        self.memory_limit = memory_limit;
        self
    }

    #[must_use]
    pub fn target_partitions(mut self, target_partitions: Option<usize>) -> Self {
        self.target_partitions = target_partitions;
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

    /// Enable URL-based table resolution (e.g., `SELECT * FROM 's3://bucket/data.parquet'`).
    ///
    /// When enabled, queries can directly reference object store URLs as table names.
    /// This feature is opt-in and disabled by default.
    ///
    /// Enable via spicepod.yml:
    /// ```yaml
    /// runtime:
    ///   params:
    ///     url_tables: enabled
    /// ```
    #[must_use]
    pub fn with_url_tables(mut self, enabled: bool) -> Self {
        self.url_tables_enabled = enabled;
        self
    }

    #[must_use]
    pub fn cayenne_sort_merge_min_rows(mut self, min_rows: Option<usize>) -> Self {
        self.cayenne_sort_merge_min_rows = min_rows;
        self
    }

    #[must_use]
    pub fn cayenne_sort_merge_memory_pool_fraction(mut self, fraction: Option<f64>) -> Self {
        self.cayenne_sort_merge_memory_pool_fraction = fraction;
        self
    }

    /// Adds additional analyzer rules to the `DataFusion` instance.
    #[must_use]
    pub fn with_analyzer_rules(mut self, rules: Vec<Arc<dyn AnalyzerRule + Send + Sync>>) -> Self {
        self.additional_analyzer_rules = rules;
        self
    }

    /// Sets the executor registry for distributed write forwarding (scheduler mode only).
    #[must_use]
    pub fn with_executor_registry(mut self, registry: Arc<ExecutorRegistry>) -> Self {
        self.executor_registry = Some(registry);
        self
    }

    /// Sets the partition service for discovery and assignment of partitions (scheduler mode only).
    #[must_use]
    pub fn with_partition_service(mut self, service: Arc<PartitionService>) -> Self {
        self.partition_service = Some(service);
        self
    }

    /// Builds the `DataFusion` instance.
    ///
    /// # Panics
    ///
    /// Panics if the `DataFusion` instance cannot be built due to errors in registering functions or schemas.
    #[must_use]
    pub fn build(self) -> DataFusion {
        let mut config = self.config;
        let effective_memory_limit = effective_query_memory_limit(self.memory_limit);

        if let Some(spill_compression) = self.spill_compression {
            config = config.with_spill_compression(spill_compression);
        }

        if let Some(target_partitions) = self.target_partitions {
            if target_partitions > 0 {
                config = config.with_target_partitions(target_partitions);
            } else {
                tracing::warn!(
                    "Ignoring runtime.query.target_partitions=0; value must be greater than 0"
                );
            }
        }

        let exact_join_filter_memory_limit =
            configure_hash_join_memory_limits(&mut config, effective_memory_limit);

        #[cfg(not(windows))]
        {
            config = config.with_option_extension(cayenne_optimizer_config(
                self.cayenne_sort_merge_min_rows,
                self.cayenne_sort_merge_memory_pool_fraction,
                effective_memory_limit,
                exact_join_filter_memory_limit,
            ));
        }

        let datafusion_ref = super::iceberg_ddl::new_shared_datafusion_ref();

        let mut state = SessionStateBuilder::new()
            .with_config(config)
            .with_default_features()
            // Replace the default analyzer rules with an empty set so we can add our own predefined list later (see `AnalyzerRulesBuilder`).
            .with_analyzer_rules(vec![])
            .with_query_planner(Arc::new(
                ExtensionPlanQueryPlanner::from_extension_planners(default_extension_planners(
                    self.executor_registry.clone(),
                    self.io_runtime.clone(),
                )),
            ))
            .with_runtime_env(runtime_env_with_effective_memory_limit(
                effective_memory_limit,
                self.temp_directory.clone(),
                self.io_runtime.clone(),
            ));

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
            .with_physical_optimizer_rule(Arc::new(HttpParamsPushdown))
            .with_physical_optimizer_rule(Arc::new(EmptyHashJoinExecPhysicalOptimization {}));

        #[cfg(not(windows))]
        {
            // Cayenne is not built on Windows, so its exact join-filter rewrite
            // and accumulator budget are only configured for supported targets.
            // Windows keeps DataFusion's standard hash-join dynamic filters.
            clamp_maximum_shared_inlist_memory_bytes(exact_join_filter_memory_limit);
            state = with_cayenne_logical_optimizer(state);
            state = state
                .with_physical_optimizer_rule(Arc::new(CayenneDynamicFilterSharing::new()))
                .with_physical_optimizer_rule(Arc::new(CayenneAntiJoinSortMergeRewriter::new()));
        }
        #[cfg(windows)]
        {
            let _ = exact_join_filter_memory_limit;
        }

        state = state
            .with_physical_optimizer_rule(Arc::new(BytesProcessedPhysicalOptimizer::new(Arc::new(
                Box::new(track_bytes_processed),
            ))))
            .with_physical_optimizer_rule(Arc::new(DataSourceTreeDisplayOptimizer::new()));

        if matches!(
            self.cluster_config.as_ref().and_then(|cfg| cfg.role()),
            Some(ClusterRole::Scheduler)
        ) {
            state = state.with_physical_optimizer_rule(FlightSQLPartialAggregatePushdown::new());
        }

        let mut state = state.build();

        if let Err(e) = datafusion_functions_json::register_all(&mut state) {
            panic!("Unable to register JSON functions: {e}");
        }

        if let Err(e) = datafusion_spark::register_all(&mut state) {
            panic!("Unable to register Spark functions: {e}");
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

        let ctx = SessionContext::new_with_state(state);

        // Add cache invalidation optimizer rule if caching is enabled
        if let Some(caching) = &self.caching {
            ctx.add_optimizer_rule(Arc::new(CacheInvalidationOptimizerRule::new(
                Arc::downgrade(caching),
            )));
        }
        ctx.register_catalog(SPICE_DEFAULT_CATALOG, Arc::new(catalog));

        // Enable URL-based table resolution (e.g., SELECT * FROM 's3://bucket/data.parquet')
        // This is opt-in via `runtime.params.url_tables=enabled`
        if self.url_tables_enabled {
            let url_table_factory = Arc::new(SpiceUrlTableFactory::new());
            let current_catalog_list = Arc::clone(ctx.state().catalog_list());
            let dynamic_catalog_list = Arc::new(DynamicUrlCatalogList::new(
                current_catalog_list,
                Arc::clone(&url_table_factory),
            ));
            ctx.register_catalog_list(dynamic_catalog_list);

            // Register the session state with the factory so it can infer schemas
            url_table_factory.with_state(ctx.state_weak_ref());
        }

        let caching = self.caching.unwrap_or(Arc::new(Caching::default()));

        let ddl_enabled_catalogs = Arc::new(RwLock::new(HashSet::new()));
        let ddl_extension_store =
            datafusion_ddl::new_shared_store(SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA);

        let cayenne_ddl_handler: Option<Arc<dyn datafusion_ddl::CatalogDdlHandler>> =
            // How we handle Cayenne DDL depends if its single node vs distributed.
            if let Some(executor_registry) = &self.executor_registry {
                use crate::cluster::{AcceleratedPartitionProvider, FederatedPartitionProvider};


                // Rules only for distributed query
                // Accelerated tables
                ctx.add_analyzer_rule(Arc::new(PartitionedTableScanRewrite::new(
                    Arc::new(AcceleratedPartitionProvider::from_registry(Arc::clone(executor_registry)))
                        as Arc<dyn TablePartitionProvider>,
                    &ctx,
                )));

                // Federated tables (e.g. Cayenne)
                ctx.add_analyzer_rule(Arc::new(PartitionedTableScanRewrite::new(
                    Arc::new(FederatedPartitionProvider::from_registry(executor_registry))
                        as Arc<dyn TablePartitionProvider>,
                    &ctx,
                )));

                // Distributed Cayenne DDL analyzer rule.
                #[cfg(not(windows))]
                {
                    Some(
                        Arc::new(super::cayenne_ddl::DistributedCayenneDdlHandler::new(
                            Arc::clone(executor_registry),
                        )) as Arc<dyn datafusion_ddl::CatalogDdlHandler>,
                    )
                }
                #[cfg(windows)]
                None
            } else {
                // Single node spice uses default [`cayenne::CayenneDdlHandler`].
                if cfg!(windows) {
                    None
                } else {
                    Some(Arc::new(cayenne::CayenneDdlHandler {})
                        as Arc<dyn datafusion_ddl::CatalogDdlHandler>)
                }
            };

        if let Some(ref cayenne_ddl_handler) = cayenne_ddl_handler {
            ctx.add_analyzer_rule(Arc::new(datafusion_ddl::DdlAnalyzerRule::new(
                ctx.state().catalog_list(),
                &ddl_enabled_catalogs,
                Arc::clone(&ddl_extension_store),
                Arc::clone(cayenne_ddl_handler),
                SPICE_DEFAULT_SCHEMA,
                SPICE_DEFAULT_CATALOG,
            )));
        }

        // Add these analyzer rules after `PartitionedTableScanRewrite` to allow expansion across partitions/executors.
        for rule in AnalyzerRulesBuilder::default().build() {
            ctx.add_analyzer_rule(rule);
        }
        for rule in self.additional_analyzer_rules {
            ctx.add_analyzer_rule(rule);
        }

        // Iceberg DDL analyzer rule.
        ctx.add_analyzer_rule(Arc::new(datafusion_ddl::DdlAnalyzerRule::new(
            ctx.state().catalog_list(),
            &ddl_enabled_catalogs,
            Arc::clone(&ddl_extension_store),
            Arc::new(super::iceberg_ddl::IcebergDdlHandler::new(Arc::clone(
                &datafusion_ref,
            ))),
            SPICE_DEFAULT_SCHEMA,
            SPICE_DEFAULT_CATALOG,
        )));

        DataFusion {
            runtime_status: self.status,
            ctx: Arc::new(ctx),
            data_writers: RwLock::new(HashSet::new()),
            data_update_broadcaster: crate::dataupdate::DataUpdateBroadcaster::new(),
            writable_catalogs: RwLock::new(HashSet::new()),
            ddl_enabled_catalogs,
            ddl_extension_store,
            datafusion_ref,
            caching,
            pending_sink_tables: TokioRwLock::new(Vec::new()),
            deferred_tables: TokioRwLock::new(HashMap::new()),
            deferred_catalogs: TokioRwLock::new(HashMap::new()),
            pending_initializations: TokioRwLock::new(HashMap::new()),
            pending_initializations_count: std::sync::atomic::AtomicUsize::new(0),
            query_cancel_registry: Arc::new(super::query::registry::QueryCancelRegistry::new()),
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
            cluster_config: self.cluster_config.unwrap_or_default(),
            scheduler_server: RwLock::new(None),
            executor: RwLock::new(None),
            executor_stream_registry: RwLock::new(None),
            partition_service: self.partition_service,
            #[cfg(not(windows))]
            cayenne_ddl_handler,
        }
    }
}

#[cfg(not(windows))]
fn with_cayenne_logical_optimizer(mut state: SessionStateBuilder) -> SessionStateBuilder {
    let trailing_rules = state.optimizer_rules().take().unwrap_or_default();
    let mut optimizer_rules = state
        .optimizer()
        .take()
        .map_or_else(|| Optimizer::new().rules, |optimizer| optimizer.rules);

    insert_cayenne_logical_optimizer_rule(&mut optimizer_rules);
    optimizer_rules.extend(trailing_rules);
    state.with_optimizer_rules(optimizer_rules)
}

#[cfg(not(windows))]
fn insert_cayenne_logical_optimizer_rule(rules: &mut Vec<Arc<dyn OptimizerRule + Send + Sync>>) {
    if rules
        .iter()
        .any(|rule| rule.name() == "cayenne_propagate_filter_across_equi_join_keys")
    {
        return;
    }

    let insert_at = rules
        .iter()
        .position(|rule| rule.name() == "decorrelate_predicate_subquery")
        .unwrap_or_else(|| {
            rules
                .iter()
                .position(|rule| rule.name() == "push_down_filter")
                .unwrap_or(rules.len())
        });
    rules.insert(
        insert_at,
        Arc::new(
            CayennePropagateFilterAcrossEquiJoinKeys::new_with_table_provider_predicate(
                is_cayenne_accelerated_table_provider,
            ),
        ),
    );
}

#[cfg(not(windows))]
fn is_cayenne_accelerated_table_provider(provider: &dyn TableProvider) -> bool {
    if is_cayenne_table_provider(provider) {
        return true;
    }

    provider
        .as_any()
        .downcast_ref::<AcceleratedTable>()
        .is_some_and(|table| is_cayenne_table_provider(table.get_accelerator().as_ref()))
}

#[cfg(not(windows))]
fn is_cayenne_table_provider(provider: &dyn TableProvider) -> bool {
    if provider.as_any().is::<CayenneTableProvider>() || has_cayenne_accelerator_metadata(provider)
    {
        return true;
    }

    if let Some(poly) = provider.as_any().downcast_ref::<PolyTableProvider>() {
        return is_cayenne_table_provider(poly.writer().as_ref())
            || is_cayenne_table_provider(poly.get_federated_table_provider().as_ref());
    }

    if let Some(dedup) = provider.as_any().downcast_ref::<UpsertDedupTableProvider>() {
        return is_cayenne_table_provider(dedup.inner().as_ref());
    }

    false
}

#[cfg(not(windows))]
fn has_cayenne_accelerator_metadata(provider: &dyn TableProvider) -> bool {
    provider
        .schema()
        .metadata()
        .get("spice.accelerator")
        .is_some_and(|accelerator| accelerator == "cayenne")
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

fn effective_query_memory_limit(memory_limit: Option<u64>) -> u64 {
    memory_limit.unwrap_or_else(|| {
        let total_memory = crate::resource_monitor::get_total_memory();
        let default_limit = total_memory.saturating_mul(90) / 100;

        tracing::debug!(
            "No memory limit specified, defaulting to 90% of total memory: {}",
            util::human_readable_bytes(default_limit as usize)
        );

        default_limit
    })
}

#[cfg(not(windows))]
fn cayenne_optimizer_config(
    sort_merge_min_rows: Option<usize>,
    sort_merge_memory_pool_fraction: Option<f64>,
    effective_memory_limit: u64,
    exact_join_filter_memory_limit: usize,
) -> CayenneOptimizerConfig {
    let mut config = CayenneOptimizerConfig::default();
    if let Some(sort_merge_min_rows) = sort_merge_min_rows {
        config.sort_merge_min_rows = sort_merge_min_rows;
    }
    if let Some(sort_merge_memory_pool_fraction) = sort_merge_memory_pool_fraction {
        config.sort_merge_memory_pool_fraction = sort_merge_memory_pool_fraction;
    }
    config.sort_merge_memory_pool_bytes = Some(match usize::try_from(effective_memory_limit) {
        Ok(limit) => limit,
        Err(_) => usize::MAX,
    });
    config.exact_join_filter_max_bytes = exact_join_filter_memory_limit;
    config
}

fn exact_join_filter_memory_limit(effective_memory_limit: u64) -> usize {
    let limit = effective_memory_limit / EXACT_JOIN_FILTER_MEMORY_POOL_FRACTION_DENOMINATOR;

    match usize::try_from(limit) {
        Ok(limit) => limit,
        Err(_) => usize::MAX,
    }
}

fn hash_join_inlist_memory_limit_per_partition(
    effective_memory_limit: u64,
    target_partitions: usize,
) -> usize {
    let target_partitions = target_partitions.max(1);
    let target_partitions = u64::try_from(target_partitions).unwrap_or(u64::MAX);

    match usize::try_from(effective_memory_limit / target_partitions) {
        Ok(limit) => limit,
        Err(_) => usize::MAX,
    }
}

fn configure_hash_join_memory_limits(
    config: &mut SessionConfig,
    effective_memory_limit: u64,
) -> usize {
    let runtime_memory_limit_per_partition = hash_join_inlist_memory_limit_per_partition(
        effective_memory_limit,
        config.options().execution.target_partitions,
    );
    let exact_join_filter_memory_limit = exact_join_filter_memory_limit(effective_memory_limit);

    let optimizer = &mut config.options_mut().optimizer;
    optimizer.hash_join_inlist_pushdown_max_size = optimizer
        .hash_join_inlist_pushdown_max_size
        .min(runtime_memory_limit_per_partition);

    exact_join_filter_memory_limit
}

fn runtime_env_with_effective_memory_limit(
    effective_memory_limit: u64,
    temp_directory: Option<String>,
    io_runtime: Handle,
) -> Arc<RuntimeEnv> {
    let disk_manager_builder = if let Some(directory) = temp_directory {
        let mode = DiskManagerMode::Directories(vec![directory.into()]);
        DiskManager::builder().with_mode(mode)
    } else {
        DiskManager::builder()
    };

    let Some(topn) = NonZeroUsize::new(5) else {
        unreachable!("Memory pool TopN must be greater than 0");
    };

    // Runtime is 64-bit minimum; usize is at least 64 bits on all supported targets.
    #[expect(clippy::cast_possible_truncation)]
    let effective_memory_bytes = effective_memory_limit as usize;

    let memory_pool = Arc::new(TrackConsumersPool::new(
        // The runtime supports only 64-bit platforms, so casting u64 to usize
        // will not truncate on supported targets.
        GreedyMemoryPool::new(effective_memory_bytes),
        topn,
    ));

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

pub(crate) fn default_extension_planners(
    _executor_registry: Option<Arc<ExecutorRegistry>>,
    _io_runtime: tokio::runtime::Handle,
) -> Vec<Arc<dyn ExtensionPlanner + Send + Sync>> {
    let planners: Vec<Arc<dyn ExtensionPlanner + Send + Sync>> = vec![
        Arc::new(IndexTableScanExtensionPlanner::new()),
        Arc::new(FederatedPlanner::new()),
        Arc::new(CacheInvalidationExtensionPlanner::new()),
        // One stateless DDL planner handles all DdlExtensionNodes from any handler.
        Arc::new(datafusion_ddl::DdlExtensionPlanner),
        // One stateless DML planner handles all DmlExtensionNodes from any handler.
        Arc::new(datafusion_dml::DmlExtensionPlanner),
        #[cfg(feature = "duckdb")]
        DuckDBLogicalExtensionPlanner::new(),
    ];
    planners
}

#[cfg(test)]
mod tests {
    #[cfg(not(windows))]
    use arrow::datatypes::{DataType, Field, Schema};
    #[cfg(not(windows))]
    use cayenne::optimizer_rules::CayenneOptimizerConfig;
    #[cfg(not(windows))]
    use datafusion::catalog::{MemTable, TableProvider};
    use datafusion::optimizer::Analyzer;

    use super::{
        DataFusionBuilder, configure_hash_join_memory_limits, exact_join_filter_memory_limit,
    };
    use crate::dataaccelerator::AcceleratorEngineRegistry;
    use crate::status;
    #[cfg(not(windows))]
    use data_components::poly::PolyTableProvider;
    use runtime_datafusion::join_accumulator::DEFAULT_MAXIMUM_SHARED_INLIST_MEMORY_BYTES;
    #[cfg(not(windows))]
    use std::collections::HashMap;
    use std::sync::Arc;

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

    #[test]
    fn test_exact_join_filter_memory_limit_respects_runtime_query_memory_limit() {
        assert_eq!(
            128,
            exact_join_filter_memory_limit(1_024),
            "Exact dynamic join filters should use a fraction of the shared runtime query memory budget"
        );

        let high_memory_limit = u64::try_from(DEFAULT_MAXIMUM_SHARED_INLIST_MEMORY_BYTES)
            .expect("default in-list memory limit should fit in u64")
            .saturating_mul(16);
        assert_eq!(
            DEFAULT_MAXIMUM_SHARED_INLIST_MEMORY_BYTES.saturating_mul(2),
            exact_join_filter_memory_limit(high_memory_limit),
            "Exact dynamic join filters should scale above the historical default on larger memory pools"
        );
        assert_eq!(
            0,
            exact_join_filter_memory_limit(1),
            "Very small memory limits should not exceed the configured memory fraction"
        );
    }

    #[test]
    fn test_hash_join_inlist_pushdown_limit_respects_runtime_query_memory_limit() {
        let mut config = datafusion::prelude::SessionConfig::new().with_target_partitions(4);
        config
            .options_mut()
            .optimizer
            .hash_join_inlist_pushdown_max_size = 1_000;

        let exact_join_filter_memory_limit = configure_hash_join_memory_limits(&mut config, 2_048);

        assert_eq!(256, exact_join_filter_memory_limit);
        assert_eq!(
            512,
            config
                .options()
                .optimizer
                .hash_join_inlist_pushdown_max_size,
            "DataFusion's built-in per-partition hash join in-list pushdown should stay within the query memory limit"
        );

        let mut config = datafusion::prelude::SessionConfig::new().with_target_partitions(4);
        config
            .options_mut()
            .optimizer
            .hash_join_inlist_pushdown_max_size = 1_000;

        let exact_join_filter_memory_limit =
            configure_hash_join_memory_limits(&mut config, 1_000_000);

        assert_eq!(
            125_000, exact_join_filter_memory_limit,
            "A larger runtime query memory limit should scale the shared exact join-filter budget"
        );
        assert_eq!(
            1_000,
            config
                .options()
                .optimizer
                .hash_join_inlist_pushdown_max_size,
            "A larger runtime query memory limit should not raise DataFusion's configured hash join in-list cap"
        );
    }

    #[test]
    #[cfg(not(windows))]
    fn test_built_datafusion_registers_cayenne_optimizer_config() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime");
        let handle = rt.handle().clone();

        let df = DataFusionBuilder::new(
            status::RuntimeStatus::new(),
            Arc::new(AcceleratorEngineRegistry::default()),
            handle,
        )
        .memory_limit(Some(1_024))
        .cayenne_sort_merge_min_rows(Some(100_000_000))
        .cayenne_sort_merge_memory_pool_fraction(Some(0.25))
        .build();

        let state = df.ctx.state();
        let config = state
            .config_options()
            .extensions
            .get::<CayenneOptimizerConfig>()
            .expect("Cayenne optimizer config should be registered");

        assert_eq!(config.sort_merge_min_rows, 100_000_000);
        assert!((config.sort_merge_memory_pool_fraction - 0.25).abs() < f64::EPSILON);
        assert_eq!(config.sort_merge_memory_pool_bytes, Some(1_024));
        assert_eq!(config.exact_join_filter_max_bytes, 128);
    }

    #[test]
    #[cfg(not(windows))]
    fn test_cayenne_provider_predicate_detects_poly_accelerator_metadata() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let table =
            Arc::new(MemTable::try_new(Arc::clone(&schema), vec![vec![]]).expect("memtable"));
        let provider = PolyTableProvider::new_with_schema_metadata(
            Arc::clone(&table) as Arc<dyn TableProvider>,
            table,
            HashMap::from([("spice.accelerator".to_string(), "cayenne".to_string())]),
        );

        assert!(super::is_cayenne_accelerated_table_provider(&provider));
    }

    /// Builds a full `DataFusion` instance and verifies the analyzer rules on
    /// the resulting `SessionContext` have the correct ordering.
    ///
    /// Skipped on Windows because the Cayenne DDL analyzer rule is not registered
    /// there, resulting in a different rule list.
    #[test]
    #[cfg(not(windows))]
    fn test_built_datafusion_analyzer_rule_ordering() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime");
        let handle = rt.handle().clone();

        let df = DataFusionBuilder::new(
            status::RuntimeStatus::new(),
            Arc::new(AcceleratorEngineRegistry::default()),
            handle,
        )
        .build();

        let state = df.ctx.state();
        let rule_names: Vec<&str> = state.analyzer().rules.iter().map(|r| r.name()).collect();

        assert_eq!(
            rule_names,
            vec![
                "spice_ddl_rewrite",
                "federation_optimizer_rule",
                "resolve_grouping_function",
                "type_coercion",
                "spice_ddl_rewrite",
            ],
            "Analyzer rule list or ordering has changed"
        );
    }

    #[test]
    #[cfg(not(windows))]
    fn test_built_datafusion_registers_cayenne_logical_rule_before_subquery_decorrelation() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime");
        let handle = rt.handle().clone();

        let df = DataFusionBuilder::new(
            status::RuntimeStatus::new(),
            Arc::new(AcceleratorEngineRegistry::default()),
            handle,
        )
        .build();

        let state = df.ctx.state();
        let rule_names: Vec<&str> = state.optimizers().iter().map(|r| r.name()).collect();
        let cayenne_position = rule_names
            .iter()
            .position(|name| *name == "cayenne_propagate_filter_across_equi_join_keys")
            .expect("Cayenne logical filter propagation rule should be registered");
        let decorrelate_position = rule_names
            .iter()
            .position(|name| *name == "decorrelate_predicate_subquery")
            .expect("DataFusion decorrelate_predicate_subquery rule should be registered");
        let push_down_position = rule_names
            .iter()
            .position(|name| *name == "push_down_filter")
            .expect("DataFusion push_down_filter rule should be registered");

        assert!(
            cayenne_position < decorrelate_position,
            "Cayenne logical filter propagation must run before decorrelate_predicate_subquery so generated InSubquery predicates cannot reach physical planning"
        );
        assert!(
            decorrelate_position < push_down_position,
            "DataFusion decorrelate_predicate_subquery must run before push_down_filter"
        );
        assert_eq!(
            rule_names
                .iter()
                .filter(|name| **name == "cayenne_propagate_filter_across_equi_join_keys")
                .count(),
            1,
            "Cayenne logical filter propagation rule should be registered exactly once"
        );
    }

    #[test]
    #[cfg(not(windows))]
    fn test_built_datafusion_decorrelates_cayenne_propagated_subquery() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime");
        let handle = rt.handle().clone();

        let df = DataFusionBuilder::new(
            status::RuntimeStatus::new(),
            Arc::new(AcceleratorEngineRegistry::default()),
            handle,
        )
        .build();

        rt.block_on(async {
            let nation_schema = Arc::new(Schema::new(vec![
                Field::new("n_nationkey", DataType::Int64, false),
                Field::new("n_name", DataType::Utf8, true),
            ]));
            let supplier_schema = Arc::new(Schema::new(vec![
                Field::new("s_suppkey", DataType::Int64, false),
                Field::new("s_nationkey", DataType::Int64, false),
            ]));

            df.ctx
                .register_table(
                    "nation",
                    Arc::new(
                        MemTable::try_new(Arc::clone(&nation_schema), vec![vec![]])
                            .expect("nation mem table should be valid"),
                    ),
                )
                .expect("nation table should register");
            df.ctx
                .register_table(
                    "supplier",
                    Arc::new(
                        MemTable::try_new(Arc::clone(&supplier_schema), vec![vec![]])
                            .expect("supplier mem table should be valid"),
                    ),
                )
                .expect("supplier table should register");

            let dataframe = df
                .ctx
                .sql(
                    "SELECT s_suppkey FROM supplier, nation \
                     WHERE s_nationkey = n_nationkey AND n_name = 'CHINA'",
                )
                .await
                .expect("q21-shaped query should create a dataframe");
            let optimized_plan = dataframe
                .clone()
                .into_optimized_plan()
                .expect("q21-shaped query should optimize");
            let optimized_plan = optimized_plan.to_string();

            assert!(
                !optimized_plan.contains("InSubquery"),
                "Cayenne propagated subqueries must be decorrelated before physical planning: {optimized_plan}"
            );

            dataframe
                .create_physical_plan()
                .await
                .expect("q21-shaped query should create a physical plan");
        });
    }

    /// Regression test for the post-decorrelation re-propagation bug
    /// (`cayenne::logical_optimizer`): after the rule wraps a Filter with
    /// `InSubquery` and `DataFusion` decorrelates it to `LeftSemi`, the
    /// optimizer iterates the rule pipeline to fixed point. Without the
    /// cycle-detection fix in `analyze_logical_side`, the rule would re-fire
    /// each pass and stack one redundant `LeftSemi` per iteration up to
    /// `max_passes`. This integration test runs the full optimizer pipeline
    /// and asserts the final plan has at most one `LeftSemi` for the q21
    /// shape — proving the cycle guard holds across decorrelation.
    #[test]
    #[cfg(not(windows))]
    fn test_built_datafusion_does_not_stack_redundant_left_semi_after_decorrelation() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime");
        let handle = rt.handle().clone();

        let df = DataFusionBuilder::new(
            status::RuntimeStatus::new(),
            Arc::new(AcceleratorEngineRegistry::default()),
            handle,
        )
        .build();

        rt.block_on(async {
            let nation_schema = Arc::new(Schema::new(vec![
                Field::new("n_nationkey", DataType::Int64, false),
                Field::new("n_name", DataType::Utf8, true),
            ]));
            let supplier_schema = Arc::new(Schema::new(vec![
                Field::new("s_suppkey", DataType::Int64, false),
                Field::new("s_nationkey", DataType::Int64, false),
            ]));

            df.ctx
                .register_table(
                    "nation",
                    Arc::new(
                        MemTable::try_new(Arc::clone(&nation_schema), vec![vec![]])
                            .expect("nation mem table should be valid"),
                    ),
                )
                .expect("nation table should register");
            df.ctx
                .register_table(
                    "supplier",
                    Arc::new(
                        MemTable::try_new(Arc::clone(&supplier_schema), vec![vec![]])
                            .expect("supplier mem table should be valid"),
                    ),
                )
                .expect("supplier table should register");

            let dataframe = df
                .ctx
                .sql(
                    "SELECT s_suppkey FROM supplier, nation \
                     WHERE s_nationkey = n_nationkey AND n_name = 'CHINA'",
                )
                .await
                .expect("q21-shaped query should create a dataframe");
            let optimized_plan = dataframe
                .into_optimized_plan()
                .expect("q21-shaped query should optimize");
            let plan_text = optimized_plan.to_string();

            // The optimizer iterates rules to fixed point. Before the cycle
            // guard, every iteration would add another `LeftSemi Join` on the
            // fact side. With the guard in place we expect exactly one (the
            // single decorrelated propagation).
            let left_semi_count = plan_text.matches("LeftSemi Join").count();
            assert!(
                left_semi_count <= 1,
                "post-decorrelation re-propagation is stacking redundant LeftSemi joins \
                 (count={left_semi_count}); plan was:\n{plan_text}"
            );
        });
    }

    /// Cayenne physical optimizer rules must run after `DataFusion`'s built-in
    /// physical optimizer rules.
    #[test]
    #[cfg(not(windows))]
    fn test_built_datafusion_registers_cayenne_rules_after_datafusion_rules() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime");
        let handle = rt.handle().clone();

        let df = DataFusionBuilder::new(
            status::RuntimeStatus::new(),
            Arc::new(AcceleratorEngineRegistry::default()),
            handle,
        )
        .build();

        let state = df.ctx.state();
        let rule_names: Vec<&str> = state
            .physical_optimizers()
            .iter()
            .map(|r| r.name())
            .collect();
        let sanity_check_position = rule_names
            .iter()
            .position(|name| *name == "SanityCheckPlan")
            .expect("DataFusion sanity check rule should be registered");
        let cayenne_filter_sharing_position = rule_names
            .iter()
            .position(|name| *name == "CayenneDynamicFilterSharing")
            .expect("Cayenne dynamic filter sharing rule should be registered");
        let cayenne_anti_sort_merge_position = rule_names
            .iter()
            .position(|name| *name == "CayenneAntiJoinSortMergeRewriter")
            .expect("Cayenne anti join sort-merge rewriter should be registered");

        assert!(
            sanity_check_position < cayenne_filter_sharing_position,
            "CayenneDynamicFilterSharing must run after DataFusion's built-in physical optimizer rules"
        );
        assert!(
            cayenne_filter_sharing_position < cayenne_anti_sort_merge_position,
            "CayenneDynamicFilterSharing must run before CayenneAntiJoinSortMergeRewriter so same-source joins can receive shared scan filters before any sort-merge rewrite"
        );
    }
}
