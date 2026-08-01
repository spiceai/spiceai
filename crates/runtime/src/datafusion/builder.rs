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
use crate::{config::ClusterRole, status};
use crate::{dataaccelerator::AcceleratorEngineRegistry, datafusion::SPICE_SCP_SCHEMA};
use cache::Caching;
#[cfg(not(windows))]
use cayenne::optimizer_rules::{
    CayenneAntiJoinSortMergeRewriter, CayenneDynamicFilterSharing, CayenneJoinRewriter,
    CayenneMaintainedAggregateRewriter, CayenneOptimizerConfig, CayenneStatsAggregateRewriter,
};
#[cfg(not(windows))]
use cayenne::{
    CayenneTableProvider,
    logical_optimizer::{
        CayenneInListToRangeRewrite, CayennePropagateFilterAcrossEquiJoinKeys,
        CayennePushDownSemiJoin, CayenneReassociateCrossJoin,
    },
};
#[cfg(not(windows))]
use data_components::poly::PolyTableProvider;
#[cfg(not(windows))]
use datafusion::catalog::TableProvider;
#[cfg(not(windows))]
use datafusion::optimizer::{Optimizer, OptimizerRule};
use datafusion::{
    catalog::{CatalogProvider, MemoryCatalogProvider},
    execution::{
        DiskManager, FunctionRegistry, SessionStateBuilder,
        disk_manager::DiskManagerMode,
        memory_pool::{GreedyMemoryPool, TrackConsumersPool},
        object_store::ObjectStoreRegistry,
        runtime_env::{RuntimeEnv, RuntimeEnvBuilder},
    },
    optimizer::AnalyzerRule,
    prelude::{SessionConfig, SessionContext},
};
use datafusion::{config::SpillCompression, physical_planner::ExtensionPlanner};

use datafusion_federation::FederatedPlanner;
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
        CacheInvalidationExtensionPlanner, ReorderJoinRule,
        cache_invalidation::CacheInvalidationOptimizerRule,
    },
    physical_plan::{
        EmptyHashJoinExecPhysicalOptimization, HttpParamsPushdown,
        flightsql::aggregate_pushdown::FlightSQLPartialAggregatePushdown,
        flightsql::broadcast_join::{ExecutorAddressProvider, FlightSQLBroadcastJoinPushdown},
    },
};
#[cfg(not(windows))]
use runtime_datafusion::join_accumulator::clamp_maximum_shared_inlist_memory_bytes;
use runtime_datafusion::{
    extension::{ExtensionPlanQueryPlanner, bytes_processed::BytesProcessedPhysicalOptimizer},
    schema_provider::SpiceSchemaProvider,
    url_table::{DynamicUrlCatalogList, SpiceUrlTableFactory},
};
use runtime_datafusion_index::analyzer::IndexTableScanExtensionPlanner;
use runtime_metrics::telemetry::track_bytes_processed;
use runtime_object_store::registry::SpiceObjectStoreRegistry;
use spicepod::component::runtime::SpillCompression as SpiceSpillCompression;
use spicepod::metric::Metrics;
use tokio::{
    runtime::Handle,
    sync::{RwLock as TokioRwLock, Semaphore},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct CayenneOptimizerRules {
    logical: CayenneLogicalOptimizerRules,
    physical: CayennePhysicalOptimizerRules,
}

// Each field toggles one Cayenne logical optimizer rule; a flag bag is the
// natural shape here, so the >3-bools pedantic lint does not apply.
#[expect(clippy::struct_excessive_bools)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct CayenneLogicalOptimizerRules {
    filter_propagation: bool,
    cross_join_reassociation: bool,
    inlist_to_range: bool,
    semi_join_pushdown: bool,
    join_reorder: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct CayennePhysicalOptimizerRules {
    enabled_rules: u8,
}

impl CayennePhysicalOptimizerRules {
    const DYNAMIC_FILTER_SHARING: u8 = 1 << 0;
    const MAINTAINED_AGGREGATE: u8 = 1 << 1;
    const ANTI_JOIN_SORT_MERGE: u8 = 1 << 2;
    const EXACT_JOIN_FILTER: u8 = 1 << 3;
    const STATS_AGGREGATE: u8 = 1 << 4;

    const fn auto_enabled() -> Self {
        Self {
            enabled_rules: Self::DYNAMIC_FILTER_SHARING
                | Self::MAINTAINED_AGGREGATE
                | Self::ANTI_JOIN_SORT_MERGE
                | Self::STATS_AGGREGATE,
        }
    }

    const fn all_enabled() -> Self {
        Self {
            enabled_rules: Self::DYNAMIC_FILTER_SHARING
                | Self::MAINTAINED_AGGREGATE
                | Self::ANTI_JOIN_SORT_MERGE
                | Self::EXACT_JOIN_FILTER
                | Self::STATS_AGGREGATE,
        }
    }

    const fn none() -> Self {
        Self { enabled_rules: 0 }
    }

    const fn is_enabled(self, rule: u8) -> bool {
        self.enabled_rules & rule != 0
    }

    fn set(&mut self, rule: u8, enabled: bool) {
        if enabled {
            self.enabled_rules |= rule;
        } else {
            self.enabled_rules &= !rule;
        }
    }
}

impl CayenneOptimizerRules {
    #[must_use]
    pub const fn auto_enabled() -> Self {
        Self {
            logical: CayenneLogicalOptimizerRules {
                filter_propagation: false,
                cross_join_reassociation: true,
                inlist_to_range: false,
                semi_join_pushdown: true,
                join_reorder: true,
            },
            physical: CayennePhysicalOptimizerRules::auto_enabled(),
        }
    }

    #[must_use]
    pub const fn all_enabled() -> Self {
        Self {
            logical: CayenneLogicalOptimizerRules {
                filter_propagation: true,
                cross_join_reassociation: true,
                inlist_to_range: true,
                semi_join_pushdown: true,
                join_reorder: true,
            },
            physical: CayennePhysicalOptimizerRules::all_enabled(),
        }
    }

    #[must_use]
    pub const fn none() -> Self {
        Self {
            logical: CayenneLogicalOptimizerRules {
                filter_propagation: false,
                cross_join_reassociation: false,
                inlist_to_range: false,
                semi_join_pushdown: false,
                join_reorder: false,
            },
            physical: CayennePhysicalOptimizerRules::none(),
        }
    }

    #[must_use]
    pub const fn filter_propagation(self) -> bool {
        self.logical.filter_propagation
    }

    pub fn set_filter_propagation(&mut self, enabled: bool) {
        self.logical.filter_propagation = enabled;
    }

    #[must_use]
    pub const fn cross_join_reassociation(self) -> bool {
        self.logical.cross_join_reassociation
    }

    pub fn set_cross_join_reassociation(&mut self, enabled: bool) {
        self.logical.cross_join_reassociation = enabled;
    }

    #[must_use]
    pub const fn inlist_to_range(self) -> bool {
        self.logical.inlist_to_range
    }

    pub fn set_inlist_to_range(&mut self, enabled: bool) {
        self.logical.inlist_to_range = enabled;
    }

    #[must_use]
    pub const fn semi_join_pushdown(self) -> bool {
        self.logical.semi_join_pushdown
    }

    pub fn set_semi_join_pushdown(&mut self, enabled: bool) {
        self.logical.semi_join_pushdown = enabled;
    }

    #[must_use]
    pub const fn join_reorder(self) -> bool {
        self.logical.join_reorder
    }

    pub fn set_join_reorder(&mut self, enabled: bool) {
        self.logical.join_reorder = enabled;
    }

    #[must_use]
    pub const fn dynamic_filter_sharing(self) -> bool {
        self.physical
            .is_enabled(CayennePhysicalOptimizerRules::DYNAMIC_FILTER_SHARING)
    }

    pub fn set_dynamic_filter_sharing(&mut self, enabled: bool) {
        self.physical.set(
            CayennePhysicalOptimizerRules::DYNAMIC_FILTER_SHARING,
            enabled,
        );
    }

    #[must_use]
    pub const fn maintained_aggregate(self) -> bool {
        self.physical
            .is_enabled(CayennePhysicalOptimizerRules::MAINTAINED_AGGREGATE)
    }

    pub fn set_maintained_aggregate(&mut self, enabled: bool) {
        self.physical
            .set(CayennePhysicalOptimizerRules::MAINTAINED_AGGREGATE, enabled);
    }

    #[must_use]
    pub const fn stats_aggregate(self) -> bool {
        self.physical
            .is_enabled(CayennePhysicalOptimizerRules::STATS_AGGREGATE)
    }

    pub fn set_stats_aggregate(&mut self, enabled: bool) {
        self.physical
            .set(CayennePhysicalOptimizerRules::STATS_AGGREGATE, enabled);
    }

    #[must_use]
    pub const fn anti_join_sort_merge(self) -> bool {
        self.physical
            .is_enabled(CayennePhysicalOptimizerRules::ANTI_JOIN_SORT_MERGE)
    }

    pub fn set_anti_join_sort_merge(&mut self, enabled: bool) {
        self.physical
            .set(CayennePhysicalOptimizerRules::ANTI_JOIN_SORT_MERGE, enabled);
    }

    #[must_use]
    pub const fn exact_join_filter(self) -> bool {
        self.physical
            .is_enabled(CayennePhysicalOptimizerRules::EXACT_JOIN_FILTER)
    }

    pub fn set_exact_join_filter(&mut self, enabled: bool) {
        self.physical
            .set(CayennePhysicalOptimizerRules::EXACT_JOIN_FILTER, enabled);
    }
}

impl Default for CayenneOptimizerRules {
    fn default() -> Self {
        Self::auto_enabled()
    }
}

pub struct DataFusionBuilder {
    config: SessionConfig,
    status: Arc<status::RuntimeStatus>,
    accelerator_engine_registry: Arc<AcceleratorEngineRegistry>,
    memory_limit: Option<u64>,
    target_partitions: Option<usize>,
    prefer_hash_join: Option<bool>,
    eager_aggregation: Option<bool>,
    eager_aggregation_min_reduction_factor: Option<usize>,
    eager_aggregation_max_pushed_groups: Option<usize>,
    temp_directory: Option<String>,
    accelerated_refresh_semaphore: Option<Arc<Semaphore>>,
    query_admission_semaphore: Option<Arc<Semaphore>>,
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
    cayenne_footer_cache_mb: Option<usize>,
    /// Fraction of the query memory limit to carve into a dedicated compaction
    /// memory pool. `Some` only when Cayenne acceleration is configured and
    /// dedicated thread pools are enabled (set by the Runtime builder); `None`
    /// leaves the full budget to queries and gives compaction no separate pool.
    compaction_memory_fraction: Option<f64>,
    /// Estimated aggregate bytes the enabled changes-mode Cayenne tables reserve
    /// OUTSIDE the query pool (per-table keyset/segment/coalesce/inline caches),
    /// set by the Runtime builder. When it exceeds the base host/10 headroom, the
    /// query-memory default is reduced by the excess. 0 = none / not Cayenne CDC.
    cayenne_cdc_reservation_bytes: u64,
    /// Coordinated query-pool ceiling (bytes) when `DuckDB` file accelerators are
    /// present, computed by the Runtime builder's cgroup-aware budget so the query
    /// pool + each `DuckDB` instance's own `memory_limit` can't over-commit the
    /// memory available to this process (the cgroup limit in a container).
    /// Applied as a `min`-cap on the DEFAULT query pool only (an explicit
    /// `runtime.query.memory_limit` still wins). `None` = no `DuckDB` coordination.
    duckdb_query_pool_cap: Option<u64>,
    cayenne_optimizer_rules: CayenneOptimizerRules,
    /// Arbitrary additional analyzer rules.
    additional_analyzer_rules: Vec<Arc<dyn AnalyzerRule + Send + Sync>>,
    executor_registry: Option<Arc<ExecutorRegistry>>,
    partition_service: Option<Arc<PartitionService>>,
    partition_load_tracker: Option<Arc<runtime_cluster::PartitionLoadTracker>>,
}

// The default session config and the analyzer-rule list are plain `DataFusion`
// construction with no runtime coupling, so they live in `runtime-datafusion`.
// Re-exported here because `runtime::datafusion::builder::…` is a public path.
pub use runtime_datafusion::analyzer_rule::AnalyzerRulesBuilder;
pub use runtime_datafusion::session_config::{DEFAULT_DATAFUSION_CONFIG, get_df_default_config};

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
            prefer_hash_join: None,
            eager_aggregation: None,
            eager_aggregation_min_reduction_factor: None,
            eager_aggregation_max_pushed_groups: None,
            temp_directory: None,
            accelerated_refresh_semaphore: None,
            query_admission_semaphore: None,
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
            cayenne_footer_cache_mb: None,
            compaction_memory_fraction: None,
            cayenne_cdc_reservation_bytes: 0,
            duckdb_query_pool_cap: None,
            cayenne_optimizer_rules: CayenneOptimizerRules::default(),
            additional_analyzer_rules: vec![],
            executor_registry: None,
            partition_service: None,
            partition_load_tracker: None,
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
    pub fn prefer_hash_join(mut self, prefer_hash_join: Option<bool>) -> Self {
        self.prefer_hash_join = prefer_hash_join;
        self
    }

    #[must_use]
    pub fn eager_aggregation(mut self, eager_aggregation: Option<bool>) -> Self {
        self.eager_aggregation = eager_aggregation;
        self
    }

    #[must_use]
    pub fn eager_aggregation_min_reduction_factor(mut self, factor: Option<usize>) -> Self {
        self.eager_aggregation_min_reduction_factor = factor;
        self
    }

    #[must_use]
    pub fn eager_aggregation_max_pushed_groups(mut self, cap: Option<usize>) -> Self {
        self.eager_aggregation_max_pushed_groups = cap;
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

    /// Bound the number of concurrently-executing query plans — ordinary queries
    /// plus DDL/DML and `EXECUTE` (not lightweight `PREPARE`/`DEALLOCATE`/`SET`) —
    /// i.e. query admission control. `None` leaves the gate unbounded (the prior
    /// behavior); `Some(n)` installs a semaphore of `n` permits (clamped to at
    /// least 1).
    #[must_use]
    pub fn max_concurrent_queries(mut self, max_concurrent_queries: Option<usize>) -> Self {
        self.query_admission_semaphore =
            max_concurrent_queries.map(|n| Arc::new(Semaphore::new(n.max(1))));
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

    #[must_use]
    pub fn cayenne_footer_cache_mb(mut self, footer_cache_mb: Option<usize>) -> Self {
        self.cayenne_footer_cache_mb = footer_cache_mb;
        self
    }

    /// Estimated off-pool per-table Cayenne CDC cache reservation (bytes), summed
    /// over enabled changes-mode Cayenne tables (keyset/segment/coalesce/inline).
    /// Used to reduce the query-memory default when it exceeds the base host/10
    /// headroom. Set by the Runtime builder; `0` disables the reduction.
    #[must_use]
    pub fn cayenne_cdc_reservation_bytes(mut self, bytes: u64) -> Self {
        self.cayenne_cdc_reservation_bytes = bytes;
        self
    }

    /// Coordinated query-pool ceiling (bytes) when `DuckDB` file accelerators are
    /// present. Reduces ONLY the default query pool (via a `min`-cap in
    /// [`effective_query_memory_limit`]) so the query pool + each `DuckDB` instance's
    /// own `memory_limit` can't over-commit the memory available to this process
    /// (the cgroup limit in a container). Set by the Runtime builder;
    /// `None` disables the reduction and an explicit `runtime.query.memory_limit`
    /// always wins.
    #[must_use]
    pub fn duckdb_query_pool_cap(mut self, cap: Option<u64>) -> Self {
        self.duckdb_query_pool_cap = cap;
        self
    }

    /// Carve a dedicated compaction memory pool of `fraction` of the query
    /// memory limit. Set by the Runtime builder only when Cayenne acceleration
    /// is configured and dedicated thread pools are enabled.
    #[must_use]
    pub fn compaction_memory_fraction(mut self, fraction: Option<f64>) -> Self {
        self.compaction_memory_fraction = fraction;
        self
    }

    /// Enables (or disables) Cayenne filter propagation together with its
    /// companion IN-list→range rewrite. The IN-list→range rewrite turns
    /// `col IN (a, b, c)` predicates into range/bound predicates that filter
    /// propagation can then push into Cayenne scans, so the two are toggled
    /// together as a single "filter propagation" capability. For finer-grained
    /// control over individual logical rules, use the
    /// `runtime.params.cayenne_optimizer_rules` config path instead.
    #[must_use]
    pub fn cayenne_filter_propagation_enabled(mut self, enabled: bool) -> Self {
        self.cayenne_optimizer_rules.set_filter_propagation(enabled);
        self.cayenne_optimizer_rules.set_inlist_to_range(enabled);
        self
    }

    #[must_use]
    pub(crate) fn cayenne_optimizer_rules(mut self, rules: CayenneOptimizerRules) -> Self {
        self.cayenne_optimizer_rules = rules;
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

    /// Sets the partition load tracker used to aggregate executor
    /// `PartitionsLoaded` acks (scheduler mode only).
    #[must_use]
    pub fn with_partition_load_tracker(
        mut self,
        tracker: Arc<runtime_cluster::PartitionLoadTracker>,
    ) -> Self {
        self.partition_load_tracker = Some(tracker);
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
        // Request a dedicated compaction memory budget when a fraction is
        // configured (Cayenne acceleration + dedicated thread pools). Its presence
        // is also the "Cayenne in-memory acceleration active" signal that gates the
        // coordinated host-memory partition below: a reduced query-pool default
        // that leaves room for the off-pool Cayenne in-memory CDC tier so
        // query_pool + compaction + tier + headroom ≤ host. The query pool is only
        // shrunk by the compaction carve after the dedicated compaction RuntimeEnv
        // builds successfully; otherwise queries keep the full configured budget.
        let compaction_memory_fraction = self
            .compaction_memory_fraction
            .and_then(validate_compaction_memory_fraction);
        let cayenne_active = compaction_memory_fraction.is_some();
        let effective_memory_limit = effective_query_memory_limit(
            self.memory_limit,
            cayenne_active,
            self.cayenne_cdc_reservation_bytes,
            self.duckdb_query_pool_cap,
        );
        let compaction_memory_bytes = compaction_memory_fraction.map(|fraction| {
            #[expect(
                clippy::cast_precision_loss,
                clippy::cast_possible_truncation,
                clippy::cast_sign_loss
            )]
            let compaction_bytes = (effective_memory_limit as f64 * fraction) as u64;
            compaction_bytes
        });

        let object_store_registry: Arc<dyn ObjectStoreRegistry> =
            Arc::new(SpiceObjectStoreRegistry::new(self.io_runtime.clone()));
        // Build the dedicated compaction environment from the requested carved
        // budget, sharing the query environment's object-store registry so
        // compaction reads/writes the same stores while accounting memory
        // against its own bounded pool.
        let (effective_memory_limit, compaction_runtime_env, compaction_memory_bytes) =
            match compaction_memory_bytes {
                Some(bytes) => match build_compaction_runtime_env(
                    bytes,
                    Arc::clone(&object_store_registry),
                    self.temp_directory.clone(),
                ) {
                    Some(runtime_env) => (
                        effective_memory_limit.saturating_sub(bytes),
                        Some(runtime_env),
                        Some(bytes),
                    ),
                    None => (effective_memory_limit, None, None),
                },
                None => (effective_memory_limit, None, None),
            };

        // After the compaction carve, `effective_memory_limit` is the query memory
        // pool size. Coordinate the off-pool Cayenne in-memory CDC tier budget
        // against it, the carved compaction pool, AND any external accelerator
        // reservation (e.g. co-resident DuckDB instance ceilings) so they never sum
        // past the memory available to this process — get_total_memory() is
        // cgroup-aware, so in a container that is the cgroup limit, not host RAM.
        // `set_compaction_runtime` installs `mem_tier_budget_bytes` instead of the
        // old, isolation-sized `get_total_memory() / 4`.
        let query_memory_pool_bytes = effective_memory_limit;
        let mem_tier_budget_bytes = cayenne_active.then(|| {
            let total_memory = crate::resource_monitor::get_total_memory();
            let external_reservation_bytes =
                crate::accelerator_memory_budget::duckdb_total_reservation_bytes();
            let budget = coordinated_mem_tier_budget(
                total_memory,
                query_memory_pool_bytes,
                compaction_memory_bytes.unwrap_or(0),
                external_reservation_bytes,
            );
            // The tier floor (available/32) can exceed the coordinated remainder when the
            // query pool + compaction + external (DuckDB) reservations leave too
            // little room; the clamp then installs `floor > remainder`, a deliberate
            // small over-commit so a nonzero global cap always exists (memory mode
            // then leans on per-table caps + spill). Warn whenever that binds —
            // whether from an explicit runtime.query.memory_limit OR from a large
            // co-resident DuckDB accelerator reservation (which can now trigger it
            // even when runtime.query.memory_limit is unset).
            if budget <= total_memory / MEM_TIER_FLOOR_FRACTION
                && (self.memory_limit.is_some() || external_reservation_bytes > 0)
            {
                tracing::warn!(
                    query_memory_pool_bytes,
                    total_memory,
                    external_reservation_bytes,
                    mem_tier_budget_bytes = budget,
                    "Cayenne in-memory CDC ingestion has limited memory available: the query pool, compaction pool, and co-resident DuckDB accelerator reservations leave little room for in-memory CDC, so ingestion spills to disk more often and combined memory ceilings may slightly exceed the memory available to this process (the cgroup limit when running in a container). Consider lowering runtime.query.memory_limit or per-dataset duckdb_memory_limit to give in-memory CDC more room."
                );
            }
            budget
        });

        // Cayenne HTAP runs large analytical queries (sorts, hash aggregations,
        // sort-merge joins) that spill to disk via the DiskManager. Its default is
        // the OS temp directory; on hosts that mount the data on a separate volume
        // (e.g. EBS at /media/ebs) the root volume backing OS-tmp is often too
        // small, so a spill fails and the query exhausts the memory pool
        // (ResourceExhausted) instead of spilling — the SF1000 Q10/Q18 symptom.
        // Guide operators to point spill at a roomy volume.
        if cayenne_active && self.temp_directory.is_none() {
            tracing::info!(
                "Cayenne acceleration is active but runtime.query.temp_directory is unset: large analytical queries spill to the OS temp directory. If your data is on a separate volume (e.g. EBS) and the root volume is small, set runtime.query.temp_directory to a path with ample free space so large queries can spill instead of failing."
            );
        }

        if let Some(spill_compression) = self.spill_compression {
            config = config.with_spill_compression(spill_compression);
        }

        if let Some(target_partitions) = self.target_partitions {
            if target_partitions > 0 {
                config = config.with_target_partitions(target_partitions);
                tracing::info!(target_partitions, "Applied runtime.query.target_partitions");
            } else {
                tracing::warn!(
                    "Ignoring runtime.query.target_partitions=0; value must be greater than 0"
                );
            }
        } else {
            tracing::info!(
                effective = config.options().execution.target_partitions,
                "runtime.query.target_partitions not set; using DataFusion default"
            );
        }

        // `HashJoinExec` build sides are not spillable, so very large joins can
        // exhaust the query memory pool outright. Setting this to `false` makes
        // the planner emit spillable sort-merge joins instead. Left unset,
        // DataFusion's default (prefer hash joins) stands; the Cayenne
        // `CayenneAntiJoinSortMergeRewriter` still selectively converts oversized
        // hash joins to sort-merge under the memory gate.
        if let Some(prefer_hash_join) = self.prefer_hash_join {
            config.options_mut().optimizer.prefer_hash_join = prefer_hash_join;
            tracing::info!(prefer_hash_join, "Applied runtime.query.prefer_hash_join");
        }

        // Cost-based eager-aggregation physical optimizer
        // (`datafusion.optimizer.enable_eager_aggregation`): pushes a partial
        // aggregation below a join when a statistics-based cost model predicts a
        // large row reduction, then re-aggregates above the join. Enabled by
        // default in spiced (DataFusion's own default is off); disable via
        // `runtime.query.eager_aggregation: false`. The cost gate can be tuned
        // with `runtime.query.eager_aggregation_min_reduction_factor`
        // (DataFusion default 4) and `..._max_pushed_groups` (default 0 =
        // uncapped); unset leaves the DataFusion default in place.
        let eager_aggregation = self.eager_aggregation.unwrap_or(true);
        config.options_mut().optimizer.enable_eager_aggregation = eager_aggregation;
        if let Some(factor) = self.eager_aggregation_min_reduction_factor {
            if factor > 0 {
                config
                    .options_mut()
                    .optimizer
                    .eager_aggregation_min_reduction_factor = factor;
            } else {
                tracing::warn!(
                    "Ignoring runtime.query.eager_aggregation_min_reduction_factor=0; value must be greater than 0"
                );
            }
        }
        if let Some(cap) = self.eager_aggregation_max_pushed_groups {
            config
                .options_mut()
                .optimizer
                .eager_aggregation_max_pushed_groups = cap;
        }
        if self.eager_aggregation.is_some() {
            tracing::info!(eager_aggregation, "Applied runtime.query.eager_aggregation");
        } else {
            tracing::info!(
                eager_aggregation,
                "runtime.query.eager_aggregation not set; defaulting to spiced default"
            );
        }

        // Sizes DataFusion's *native* hash-join InList dynamic-filter budget
        // (`optimizer.hash_join_inlist_pushdown_max_size`) from the runtime
        // memory limit. The native inner-join dynamic-filter pushdown
        // (min/max bounds + InList/hash-table membership) supersedes the former
        // forked `ExactLeftAccumulator` seam.
        configure_hash_join_memory_limits(&mut config, effective_memory_limit);

        // Per-query budget for the opt-in `CayenneJoinRewriter` exact in-list
        // accumulator. Independent of the default-path
        // `configure_hash_join_memory_limits` cap-raise above; only consumed when
        // the `exact_join_filter` rule is registered below.
        let exact_join_filter_memory_limit = exact_join_filter_memory_limit(effective_memory_limit);

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

        let query_runtime_env = runtime_env_with_effective_memory_limit_and_object_store_registry(
            effective_memory_limit,
            self.temp_directory.clone(),
            object_store_registry,
            self.cayenne_footer_cache_mb
                .map(|size_mb| size_mb.saturating_mul(1024 * 1024)),
        );

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
            .with_runtime_env(Arc::clone(&query_runtime_env));

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
            // Cayenne is not built on Windows, so its physical optimizer rules
            // are only configured for supported targets. By default the ordinary
            // inner-join probe filter is handled by DataFusion 53's native
            // hash-join dynamic-filter pushdown (no Cayenne-specific physical
            // rule); the InList budget for it is sized in
            // `configure_hash_join_memory_limits` above. The opt-in
            // `CayenneJoinRewriter` below (gated on `exact_join_filter`) restores
            // the forked exact in-list accumulator path on top of that default.
            // Windows keeps DataFusion's standard hash-join dynamic filters.
            state = with_cayenne_logical_optimizers(state, self.cayenne_optimizer_rules);
            if self.cayenne_optimizer_rules.dynamic_filter_sharing() {
                state = state
                    .with_physical_optimizer_rule(Arc::new(CayenneDynamicFilterSharing::new()));
            }
            if self.cayenne_optimizer_rules.maintained_aggregate() {
                state = state.with_physical_optimizer_rule(Arc::new(
                    CayenneMaintainedAggregateRewriter::new(),
                ));
            }
            if self.cayenne_optimizer_rules.stats_aggregate() {
                state = state
                    .with_physical_optimizer_rule(Arc::new(CayenneStatsAggregateRewriter::new()));
            }
            if self.cayenne_optimizer_rules.anti_join_sort_merge() {
                state = state.with_physical_optimizer_rule(Arc::new(
                    CayenneAntiJoinSortMergeRewriter::new(),
                ));
            }
            // Opt-in: restores the forked exact in-list join accumulator seam
            // (`ExactLeftAccumulator`). Off by default — the default path uses
            // DataFusion 53's native hash-join dynamic-filter pushdown sized by
            // `configure_hash_join_memory_limits` above. When enabled, clamp the
            // process-wide shared in-list reservation and register the rewriter
            // after the sort-merge rewrite so it only touches remaining
            // `HashJoinExec` nodes.
            if self.cayenne_optimizer_rules.exact_join_filter() {
                clamp_maximum_shared_inlist_memory_bytes(exact_join_filter_memory_limit);
                state = state.with_physical_optimizer_rule(Arc::new(CayenneJoinRewriter::new()));
            } else {
                let _ = exact_join_filter_memory_limit;
            }
        }
        #[cfg(windows)]
        let _ = exact_join_filter_memory_limit;

        state = state.with_physical_optimizer_rule(Arc::new(BytesProcessedPhysicalOptimizer::new(
            Arc::new(Box::new(track_bytes_processed)),
        )));

        if matches!(
            self.cluster_config.as_ref().and_then(|cfg| cfg.role()),
            Some(ClusterRole::Scheduler)
        ) {
            state = state.with_physical_optimizer_rule(FlightSQLPartialAggregatePushdown::new());

            // Distribute small-dimension joins onto executors (broadcast the
            // dim via `executor_table`, join each fact partition locally) so the
            // scheduler stops pulling whole fact tables up to join centrally.
            // Gated on the dim's row-count stats; only fires for genuinely small
            // dimensions. Live executor addresses are read sync at plan time.
            if let Some(registry) = &self.executor_registry {
                let reg = Arc::clone(registry);
                let addresses: ExecutorAddressProvider =
                    Arc::new(move || reg.ready_executor_ids_sync());
                // The primary gate is the scale-invariant cost test
                // (dim_rows × executors < fact_rows); this is the absolute cap
                // on the broadcast dimension's row count to bound per-executor
                // memory regardless of that comparison.
                state = state.with_physical_optimizer_rule(FlightSQLBroadcastJoinPushdown::new(
                    addresses, /* max_broadcast_dim_rows */ 25_000_000,
                ));
            }
        }

        let mut state = state.build();

        if let Err(e) = datafusion_functions_json::register_all(&mut state) {
            panic!("Unable to register JSON functions: {e}");
        }

        // Register Spark-compatible functions, but skip Spark's `trunc` (scalar) and
        // `avg` (aggregate): `register_all` would register them *over* the built-ins
        // of the same name. Spark `trunc` is date-truncation and shadows numeric
        // `trunc(<float>, <int>)` (see spiceai/spiceai#11415). Spark `avg` uses a different
        // partial-aggregate state layout (`[sum, count:Int64]`) than the built-in
        // (`[count:UInt64, sum]`); harmless single-node, but it corrupts DISTRIBUTED
        // plans — the scheduler bakes the shuffle/stage schema from Spark `avg`'s
        // `state_fields` while executors run the built-in `avg`, so the coalescing
        // shuffle reader downcasts the wrong primitive type and panics ("primitive
        // array"). Keep the built-ins; register every other Spark function (mirrors
        // `datafusion_spark::register_all`).
        for udf in datafusion_spark::all_default_scalar_functions() {
            if udf.name() == "trunc" {
                continue;
            }
            let name = udf.name().to_string();
            if let Err(e) = state.register_udf(udf) {
                panic!("Unable to register Spark scalar function `{name}`: {e}");
            }
        }
        for udaf in datafusion_spark::all_default_aggregate_functions() {
            if udaf.name() == "avg" {
                continue;
            }
            let name = udaf.name().to_string();
            if let Err(e) = state.register_udaf(udaf) {
                panic!("Unable to register Spark aggregate function `{name}`: {e}");
            }
        }
        for udwf in datafusion_spark::all_default_window_functions() {
            let name = udwf.name().to_string();
            if let Err(e) = state.register_udwf(udwf) {
                panic!("Unable to register Spark window function `{name}`: {e}");
            }
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
        // Federation runs as the first of these (see `AnalyzerRulesBuilder::include_federation`).
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
            schema_evolve_locks: TokioRwLock::new(HashMap::new()),
            pending_sink_tables: TokioRwLock::new(Vec::new()),
            deferred_tables: TokioRwLock::new(HashMap::new()),
            deferred_catalogs: TokioRwLock::new(HashMap::new()),
            pending_initializations: TokioRwLock::new(HashMap::new()),
            pending_initializations_count: std::sync::atomic::AtomicUsize::new(0),
            query_cancel_registry: Arc::new(super::query::registry::QueryCancelRegistry::new()),
            plan_capture: OnceLock::new(),
            write_stats_notify: tokio::sync::Notify::new(),
            accelerated_tables: TokioRwLock::new(HashSet::new()),
            accelerator_engine_registry: self.accelerator_engine_registry,
            acceleration_refresh_semaphore: self.accelerated_refresh_semaphore,
            query_admission_semaphore: self.query_admission_semaphore,
            task_history_enabled: self.task_history_enabled,
            temp_directory: self.temp_directory.clone(),
            cpu_runtime: OnceLock::new(),
            refresh_runtime: OnceLock::new(),
            cdc_apply_runtime: OnceLock::new(),
            compaction_runtime: OnceLock::new(),
            compaction_runtime_env,
            compaction_memory_bytes,
            query_memory_pool_bytes,
            mem_tier_budget_bytes,
            io_runtime: self.io_runtime,
            metrics: self.metrics,
            resource_monitor: self.resource_monitor,
            cluster_config: self.cluster_config.unwrap_or_default(),
            scheduler_server: RwLock::new(None),
            executor: RwLock::new(None),
            executor_stream_registry: RwLock::new(None),
            partition_service: self.partition_service,
            partition_load_tracker: self.partition_load_tracker,
            #[cfg(not(windows))]
            cayenne_ddl_handler,
        }
    }
}

#[cfg(not(windows))]
fn with_cayenne_logical_optimizers(
    mut state: SessionStateBuilder,
    cayenne_optimizer_rules: CayenneOptimizerRules,
) -> SessionStateBuilder {
    let trailing_rules = state.optimizer_rules().take().unwrap_or_default();
    let mut optimizer_rules = state
        .optimizer()
        .take()
        .map_or_else(|| Optimizer::new().rules, |optimizer| optimizer.rules);

    if cayenne_optimizer_rules.filter_propagation() {
        insert_cayenne_filter_propagation_rule(&mut optimizer_rules);
    }
    if cayenne_optimizer_rules.cross_join_reassociation() {
        insert_cayenne_cross_join_reassociation_rule(&mut optimizer_rules);
    }
    if cayenne_optimizer_rules.inlist_to_range() {
        insert_cayenne_inlist_to_range_rewrite(&mut optimizer_rules);
    }
    if cayenne_optimizer_rules.semi_join_pushdown() {
        insert_cayenne_push_down_semi_join(&mut optimizer_rules);
    }
    if cayenne_optimizer_rules.join_reorder() {
        insert_cayenne_join_reorder_rule(&mut optimizer_rules);
    }
    optimizer_rules.extend(trailing_rules);
    state.with_optimizer_rules(optimizer_rules)
}

#[cfg(not(windows))]
fn insert_cayenne_filter_propagation_rule(rules: &mut Vec<Arc<dyn OptimizerRule + Send + Sync>>) {
    if !rules
        .iter()
        .any(|rule| rule.name() == "cayenne_propagate_filter_across_equi_join_keys")
    {
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
}

#[cfg(not(windows))]
fn insert_cayenne_cross_join_reassociation_rule(
    rules: &mut Vec<Arc<dyn OptimizerRule + Send + Sync>>,
) {
    // Run after DataFusion has extracted join predicates from SQL `FROM`-order
    // cross joins, but before filter pushdown/physical planning consume the
    // left-deep tree shape.
    if !rules
        .iter()
        .any(|rule| rule.name() == "cayenne_reassociate_cross_join")
    {
        let insert_at = rules
            .iter()
            .position(|rule| rule.name() == "eliminate_cross_join")
            .map_or_else(
                || {
                    rules
                        .iter()
                        .position(|rule| rule.name() == "push_down_filter")
                        .unwrap_or(rules.len())
                },
                |position| position + 1,
            );
        rules.insert(
            insert_at,
            Arc::new(
                CayenneReassociateCrossJoin::new_with_table_provider_predicate(
                    is_cayenne_accelerated_table_provider,
                ),
            ),
        );
    }
}

#[cfg(not(windows))]
fn insert_cayenne_inlist_to_range_rewrite(rules: &mut Vec<Arc<dyn OptimizerRule + Send + Sync>>) {
    // Run the IN-list → BETWEEN rewrite ahead of `simplify_expressions` so the
    // downstream simplifier can fold the resulting `Expr::Between` the same way
    // it folds a SQL-parsed BETWEEN.
    if !rules
        .iter()
        .any(|rule| rule.name() == "cayenne_inlist_to_range_rewrite")
    {
        let insert_at = rules
            .iter()
            .position(|rule| rule.name() == "simplify_expressions")
            .unwrap_or(rules.len());
        rules.insert(
            insert_at,
            Arc::new(
                CayenneInListToRangeRewrite::new_with_table_provider_predicate(
                    is_cayenne_accelerated_table_provider,
                ),
            ),
        );
    }
}

#[cfg(not(windows))]
fn insert_cayenne_push_down_semi_join(rules: &mut Vec<Arc<dyn OptimizerRule + Send + Sync>>) {
    // Run after `decorrelate_predicate_subquery` has turned `col IN (subquery)`
    // into the `LeftSemi` join this rule pushes down, and before `push_down_filter`
    // consumes the rewritten tree.
    if !rules
        .iter()
        .any(|rule| rule.name() == "cayenne_push_down_semi_join")
    {
        let insert_at = rules
            .iter()
            .position(|rule| rule.name() == "decorrelate_predicate_subquery")
            .map_or_else(
                || {
                    rules
                        .iter()
                        .position(|rule| rule.name() == "push_down_filter")
                        .unwrap_or(rules.len())
                },
                |position| position + 1,
            );
        rules.insert(
            insert_at,
            Arc::new(CayennePushDownSemiJoin::new_with_table_provider_predicate(
                is_cayenne_accelerated_table_provider,
            )),
        );
    }
}

#[cfg(not(windows))]
fn insert_cayenne_join_reorder_rule(rules: &mut Vec<Arc<dyn OptimizerRule + Send + Sync>>) {
    // Cost-based left-deep join reordering (IK84). It must run *after* the
    // inner-join graph is fully formed AND base-table predicates are pushed to
    // the scans — equi-predicates extracted, cross joins reassociated into a
    // contiguous Inner-join chain, and `push_down_filter` applied so the cost
    // model can credit `TableScan.filters` for scan selectivity — but *before*
    // projection pushdown (`optimize_projections`) inserts intervening
    // Projections that fragment the graph into opaque leaves.
    if !rules.iter().any(|rule| rule.name() == "reorder_join") {
        let insert_at = [
            "push_down_filter",
            "cayenne_reassociate_cross_join",
            "eliminate_cross_join",
        ]
        .iter()
        .filter_map(|name| rules.iter().position(|rule| rule.name() == *name))
        .max()
        .map_or(rules.len(), |position| position + 1);
        rules.insert(insert_at, Arc::new(ReorderJoinRule::default()));
    }
}

#[cfg(not(windows))]
fn is_cayenne_accelerated_table_provider(provider: &dyn TableProvider) -> bool {
    if is_cayenne_table_provider(provider) {
        return true;
    }

    provider
        .downcast_ref::<AcceleratedTable>()
        .is_some_and(|table| is_cayenne_table_provider(table.get_accelerator().as_ref()))
}

#[cfg(not(windows))]
fn is_cayenne_table_provider(provider: &dyn TableProvider) -> bool {
    if provider.is::<CayenneTableProvider>() || has_cayenne_accelerator_metadata(provider) {
        return true;
    }

    if let Some(poly) = provider.downcast_ref::<PolyTableProvider>() {
        return is_cayenne_table_provider(poly.writer().as_ref())
            || is_cayenne_table_provider(poly.get_federated_table_provider().as_ref());
    }

    if let Some(dedup) = provider.downcast_ref::<UpsertDedupTableProvider>() {
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

/// Default fraction of host/container RAM for the query memory pool (before the
/// compaction carve) when the operator sets no explicit `runtime.query.memory_limit`.
const DEFAULT_QUERY_MEMORY_PERCENT: u64 = 90;

/// Reduced BASE default used when Cayenne in-memory acceleration is active. The
/// query pool, the carved compaction pool, AND the off-pool Cayenne in-memory CDC
/// tier (`cdc_durability: memory`) are each derived from total RAM; sized in
/// isolation they sum to >100% of host — the SF1000 process-OOM (RSS 242 GiB on a
/// 256 GiB box, the query pool never reporting exhaustion because the tier is
/// off-pool). Capping the query+compaction block at 70% reserves the remaining 30%
/// for the in-memory tier (up to 20%, host/5, [`coordinated_mem_tier_budget`]) plus
/// a 10% (host/10) headroom covering the off-pool per-table CDC caches / inline
/// memtables / encode buffers / OS overhead — a 70% / 20% / 10% = 100% partition.
/// This is only the BASE: when the estimated per-table CDC cache reservation
/// (keyset/segment/coalesce/inline, summed over changes-mode tables) EXCEEDS the
/// host/10 headroom, the query default is reduced further by the excess in
/// [`effective_query_memory_limit`], down to [`CAYENNE_QUERY_MEMORY_FLOOR_PERCENT`].
const CAYENNE_QUERY_MEMORY_PERCENT: u64 = 70;

/// Floor (% of host) the reservation-aware reduction never pushes the query pool
/// below, so a cache-heavy CDC config (many tables and/or large per-table caches)
/// cannot starve queries. Beyond it, the mem-tier install-time check warns and
/// memory mode leans on the per-table caps + spill/durable backstops.
const CAYENNE_QUERY_MEMORY_FLOOR_PERCENT: u64 = 50;

pub(crate) fn effective_query_memory_limit(
    memory_limit: Option<u64>,
    cayenne_active: bool,
    cdc_reservation_bytes: u64,
    duckdb_query_pool_cap: Option<u64>,
) -> u64 {
    if let Some(limit) = memory_limit {
        // An explicit limit bypasses the reservation-aware derivation below, and
        // with it the only log line that states the projected off-pool cache
        // reservation. Emit the projection here too: operators lowering
        // memory_limit to curb resident memory need to see that the caches do
        // not shrink with it - they are sized from total memory, not the pool.
        if cayenne_active && cdc_reservation_bytes > 0 {
            tracing::info!(
                memory_limit = limit,
                cdc_reservation_bytes,
                "Explicit query memory limit set; the projected per-table Cayenne CDC cache reservation is OFF-pool and unaffected by this limit"
            );
        }
        limit
    } else {
        let total_memory = crate::resource_monitor::get_total_memory();
        let default_limit = if cayenne_active {
            // Cayenne CDC active. Base is CAYENNE_QUERY_MEMORY_PERCENT of host, leaving
            // room for the off-pool in-memory tier (clamped to <= host/5 by
            // `coordinated_mem_tier_budget`) plus a host/10 headroom for the off-pool
            // per-table CDC caches + OS overhead — a 70 / 20 / 10 = 100% partition. The
            // per-table caches (keyset/segment/coalesce/inline) live OUTSIDE the query
            // pool and scale with table count; they are assumed to fit the host/10
            // headroom. When the estimated reservation EXCEEDS that headroom, carve the
            // excess out of the query pool so the freed query bytes cover the excess
            // caches and `query_pool + compaction + tier + caches + headroom` stays
            // within host. Floored at CAYENNE_QUERY_MEMORY_FLOOR_PERCENT so a very
            // cache-heavy config never starves queries (past the floor the tier
            // install-time check warns).
            let base = total_memory.saturating_mul(CAYENNE_QUERY_MEMORY_PERCENT) / 100;
            let base_headroom = total_memory / MEM_TIER_HEADROOM_FRACTION;
            let reservation_excess = cdc_reservation_bytes.saturating_sub(base_headroom);
            let floor = total_memory.saturating_mul(CAYENNE_QUERY_MEMORY_FLOOR_PERCENT) / 100;
            let default_limit = base.saturating_sub(reservation_excess).max(floor);

            // The floor binding is the unfittable-configuration signal: the
            // reservation clawback is capped at (base - floor) percent of host,
            // and when the projected per-table reservation exceeds that, the
            // startup commitment (pools + tier + off-pool caches) exceeds host
            // RAM before a single row arrives. A 121.7 GiB host was OOM-killed
            // at SF-1000 with exactly this signature, and the only trace was
            // this line at debug level.
            if default_limit == floor && reservation_excess > 0 {
                tracing::warn!(
                    cayenne_active,
                    cdc_reservation_bytes,
                    reservation_excess,
                    "Cayenne CDC cache reservation exceeds what the query pool can yield: the pool is floored at {}% of memory and the projected caches do not fit beside it. Expect resident memory above the coordinated budgets; reduce per-table cache parameters or add memory. See the budget arithmetic in this log at startup.",
                    CAYENNE_QUERY_MEMORY_FLOOR_PERCENT
                );
            }
            tracing::debug!(
                cayenne_active,
                cdc_reservation_bytes,
                reservation_excess,
                "No query memory limit specified; Cayenne CDC base {CAYENNE_QUERY_MEMORY_PERCENT}% of total, reduced by the per-table CDC reservation above the host/10 headroom to: {}",
                util::human_readable_bytes(default_limit as usize)
            );

            default_limit
        } else {
            let default_limit = total_memory.saturating_mul(DEFAULT_QUERY_MEMORY_PERCENT) / 100;
            tracing::debug!(
                cayenne_active,
                "No query memory limit specified, defaulting to {DEFAULT_QUERY_MEMORY_PERCENT}% of total memory: {}",
                util::human_readable_bytes(default_limit as usize)
            );
            default_limit
        };

        // Coordinated DuckDB cap: when DuckDB file accelerators are present the
        // Runtime builder computes a reduced query-pool ceiling (see
        // `accelerator_memory_budget`) that leaves room for each DuckDB instance's
        // own `memory_limit`, so the query pool + DuckDB ceilings can't over-commit
        // host RAM. It only ever LOWERS the default (an explicit
        // `runtime.query.memory_limit` short-circuits above and is never reduced).
        match duckdb_query_pool_cap {
            Some(cap) => {
                let capped = default_limit.min(cap);
                if capped < default_limit {
                    tracing::debug!(
                        default_query_memory_bytes = default_limit,
                        coordinated_query_memory_bytes = capped,
                        "Query memory pool reduced below its default by the coordinated DuckDB accelerator budget, leaving room for each DuckDB instance's own memory_limit."
                    );
                }
                capped
            }
            None => default_limit,
        }
    }
}

/// 1/N of host RAM bounding the aggregate off-pool Cayenne in-memory CDC tier (the
/// ceiling, 1/5 = 20%), and the headroom reserve held beyond the pools+tier for the
/// off-pool per-table CDC caches, inline memtables, encode buffers, and OS/allocator
/// overhead (1/10 = 10%). With the 70% query+compaction block the host partitions as
/// 70% / 20% / 10% = 100%.
const MEM_TIER_CEILING_FRACTION: u64 = 5;
const MEM_TIER_HEADROOM_FRACTION: u64 = 10;
/// Raised tier ceiling (1/N of host, > the base `MEM_TIER_CEILING_FRACTION`) the
/// tier may FLOAT up to on a query-light deployment — one where the operator set a
/// low `runtime.query.memory_limit`, leaving RAM the default partition would not
/// otherwise use. The float only consumes room left beyond a DOUBLED headroom
/// reserve and never exceeds the coordinated remainder, so `query_pool +
/// compaction + tier + headroom <= host` (the #11449 invariant) is preserved
/// exactly. 1/4 = 25%, a modest bump above the 20% base ceiling — the fraction must
/// stay SMALLER than `MEM_TIER_CEILING_FRACTION` so the float sits ABOVE the base.
const MEM_TIER_FLOAT_CEILING_FRACTION: u64 = 4;
/// Lower clamp (1/N of host) that keeps a healthy deployment's tier off the ground,
/// and the threshold below which [`coordinated_mem_tier_budget`] stops clamping up
/// and yields to the coordinated remainder instead. When an operator pins an
/// explicit, greedy `runtime.query.memory_limit` that leaves less than this floor,
/// the budget follows the remainder down — but never to 0, because a 0 budget
/// disables the global aggregate cap entirely (per-table caps then sum unbounded
/// across a fleet: the original no-global-cap OOM). Memory mode then leans on the
/// per-table caps + spill/durable backstops, and the caller warns.
pub(crate) const MEM_TIER_FLOOR_FRACTION: u64 = 32;

/// Coordinated aggregate byte budget for the off-pool Cayenne in-memory CDC tier.
///
/// The query pool, carved compaction pool, and this tier are otherwise each
/// derived from total RAM IN ISOLATION (`builder.rs` query pool, compaction carve,
/// and `mod.rs` `get_total_memory()/4`) and sum to >100% of host. Sizing the tier
/// as the host RAM left AFTER the query pool, the compaction pool, any memory
/// reserved outside both by another subsystem (`external_reservation_bytes` — today
/// a co-resident `DuckDB` accelerator's ceiling), and a headroom reserve is the missing
/// cross-subsystem coordination. For the coordinated default inputs — a query pool
/// sized to leave room (see [`effective_query_memory_limit`]) — it yields
/// `query_pool + compaction + external + tier + headroom ≤ host`. While that
/// remainder reaches the `host/32` floor the result is clamped to `[host/32, host/5]`:
/// the `host/5` ceiling keeps the tier ≤ 1/5 of host when the pools are small.
///
/// A greedy explicit `runtime.query.memory_limit`, or a large external reservation,
/// can leave a remainder BELOW the floor. The tier then yields to the remainder rather
/// than clamping up to a floor that would overcommit the host — down to a 1-byte
/// refuse-all gate, since a 0 budget would uninstall the global aggregate cap entirely
/// (see [`MEM_TIER_FLOOR_FRACTION`]). So the inequality above holds exactly, except for
/// that one reserved byte when the remainder is 0; every real append then refuses and
/// CDC spills to the durable backstops. The caller ([`DataFusionBuilder::build`])
/// detects the squeezed budget and warns.
pub(crate) fn coordinated_mem_tier_budget(
    total_memory: u64,
    query_pool_bytes: u64,
    compaction_pool_bytes: u64,
    external_reservation_bytes: u64,
) -> u64 {
    let headroom = total_memory / MEM_TIER_HEADROOM_FRACTION;
    let base_ceiling = total_memory / MEM_TIER_CEILING_FRACTION;
    let floor = (total_memory / MEM_TIER_FLOOR_FRACTION).min(base_ceiling);
    // Memory reserved OUTSIDE the query and compaction pools by other subsystems —
    // today a co-resident DuckDB accelerator's aggregate ceiling (see
    // `accelerator_memory_budget`), and any future external consumer — carved from
    // the same host RAM by the coordinated budget. Subtract it here so the tier,
    // and especially its query-light float below, can't reclaim room already
    // reserved elsewhere. `0` when nothing external is reserved.
    let remainder = total_memory
        .saturating_sub(query_pool_bytes)
        .saturating_sub(compaction_pool_bytes)
        .saturating_sub(external_reservation_bytes)
        .saturating_sub(headroom);
    // Floating ceiling for query-light deployments: when the query + compaction
    // pools are sized well below the default partition (an operator who set a low
    // `runtime.query.memory_limit`), let the tier reclaim part of the freed RAM
    // above the base host/5 cap — up to `host / MEM_TIER_FLOAT_CEILING_FRACTION` —
    // but only the room left beyond a DOUBLED headroom reserve, so the off-pool
    // caches/memtables the single headroom covers keep their slack. `float_room`
    // subtracts the external reservation just like `remainder`, so the float can
    // never reclaim externally-reserved RAM. Raising only the ceiling never lifts
    // the result above `remainder` (the ceiling caps from above, and `remainder` is
    // computed with the single headroom), so the floating ceiling preserves the
    // #11449 no-overcommit invariant `query_pool + compaction + external + tier +
    // headroom <= host` for ANY ceiling.
    //
    // Honesty under a tight explicit `runtime.query.memory_limit`: when the remainder
    // is below the floor, yield to the remainder instead of clamping up to a floor
    // that would make `query + compaction + tier + headroom > host`. A refuse-all
    // budget is the honest envelope there — mem-tier leans on the per-table caps +
    // spill/durable backstops — and it must stay nonzero to keep the global cap
    // installed, so the single reserved byte below is the one deliberate exception
    // to the `<= host` invariant above.
    if remainder < floor {
        // Nonzero refuse-all gate: 1 byte means every real append refuses and
        // CDC spills/falls back, without uninstalling the budget.
        return remainder.max(1);
    }
    let float_room = total_memory
        .saturating_sub(query_pool_bytes)
        .saturating_sub(compaction_pool_bytes)
        .saturating_sub(external_reservation_bytes)
        .saturating_sub(2 * headroom);
    let ceiling = base_ceiling.max(float_room.min(total_memory / MEM_TIER_FLOAT_CEILING_FRACTION));
    remainder.clamp(floor, ceiling)
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

/// Fraction (1/N) of the runtime memory limit budgeted for the opt-in
/// `CayenneJoinRewriter` exact in-list join accumulator.
const EXACT_JOIN_FILTER_MEMORY_POOL_FRACTION_DENOMINATOR: u64 = 8;

/// Per-query byte budget for the opt-in exact in-list join accumulator, derived
/// from the runtime memory limit. Only consumed when the `exact_join_filter`
/// rule is enabled; the default-path native-pushdown cap is sized separately in
/// `configure_hash_join_memory_limits`.
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

/// Sizes `DataFusion`'s native hash-join `InList` dynamic-filter budget
/// (`optimizer.hash_join_inlist_pushdown_max_size`) down to the runtime memory
/// limit divided across `target_partitions`, never raising `DataFusion`'s own
/// default. This bounds the per-partition memory the native inner-join dynamic
/// filter can spend materializing build-side keys as an `InList`; larger build
/// sides automatically fall back to the hash-table membership strategy.
fn configure_hash_join_memory_limits(config: &mut SessionConfig, effective_memory_limit: u64) {
    let runtime_memory_limit_per_partition = hash_join_inlist_memory_limit_per_partition(
        effective_memory_limit,
        config.options().execution.target_partitions,
    );

    let optimizer = &mut config.options_mut().optimizer;
    optimizer.hash_join_inlist_pushdown_max_size = optimizer
        .hash_join_inlist_pushdown_max_size
        .min(runtime_memory_limit_per_partition);
}

fn runtime_env_with_effective_memory_limit_and_object_store_registry(
    effective_memory_limit: u64,
    temp_directory: Option<String>,
    object_store_registry: Arc<dyn ObjectStoreRegistry>,
    metadata_cache_limit_bytes: Option<usize>,
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

    let mut runtime_env_builder = RuntimeEnvBuilder::default()
        .with_object_store_registry(object_store_registry)
        .with_memory_pool(memory_pool)
        .with_disk_manager_builder(disk_manager_builder);

    if let Some(limit) = metadata_cache_limit_bytes {
        runtime_env_builder = runtime_env_builder.with_metadata_cache_limit(limit);
    }

    match runtime_env_builder.build_arc() {
        Ok(runtime_env) => runtime_env,
        Err(e) => {
            unreachable!("Tests ensure this should never fail: {e}");
        }
    }
}

fn validate_compaction_memory_fraction(fraction: f64) -> Option<f64> {
    if fraction.is_finite() && fraction > 0.0 && fraction < 1.0 {
        return Some(fraction);
    }

    tracing::warn!(
        "Ignoring invalid DataFusion compaction_memory_fraction={fraction}; expected a finite value greater than 0 and less than 1"
    );
    None
}

/// Build a dedicated [`RuntimeEnv`] for background Cayenne compaction.
///
/// Sizes a separate [`GreedyMemoryPool`] to `compaction_memory_bytes` (carved
/// from the query memory limit) wrapped in a [`TrackConsumersPool`] for
/// accounting, while sharing the query environment's object-store registry so
/// compaction reads and writes the same stores. The query and compaction pools
/// together never exceed the operator's configured memory limit.
fn build_compaction_runtime_env(
    compaction_memory_bytes: u64,
    object_store_registry: Arc<dyn ObjectStoreRegistry>,
    temp_directory: Option<String>,
) -> Option<Arc<RuntimeEnv>> {
    let disk_manager_builder = if let Some(directory) = temp_directory {
        let mode = DiskManagerMode::Directories(vec![directory.into()]);
        DiskManager::builder().with_mode(mode)
    } else {
        DiskManager::builder()
    };

    let Some(topn) = NonZeroUsize::new(5) else {
        unreachable!("Memory pool TopN must be greater than 0");
    };

    // The runtime supports only 64-bit platforms, so casting u64 to usize will
    // not truncate on supported targets.
    #[expect(clippy::cast_possible_truncation)]
    let compaction_bytes = compaction_memory_bytes as usize;

    let memory_pool = Arc::new(TrackConsumersPool::new(
        GreedyMemoryPool::new(compaction_bytes),
        topn,
    ));

    let runtime_env_builder = RuntimeEnvBuilder::default()
        // Share the query environment's object-store registry so the stores
        // Cayenne registered there are visible to compaction.
        .with_object_store_registry(object_store_registry)
        .with_memory_pool(memory_pool)
        .with_disk_manager_builder(disk_manager_builder);

    match runtime_env_builder.build_arc() {
        Ok(runtime_env) => Some(runtime_env),
        Err(e) => {
            tracing::warn!(
                "Failed to build dedicated Cayenne compaction RuntimeEnv: {e}; disabling dedicated compaction runtime"
            );
            None
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
    use cayenne::logical_optimizer::PROPAGATED_FILTER_ALIAS_PREFIX;
    #[cfg(not(windows))]
    use cayenne::optimizer_rules::CayenneOptimizerConfig;
    #[cfg(not(windows))]
    use datafusion::catalog::{MemTable, TableProvider};
    #[cfg(not(windows))]
    use datafusion::common::ScalarValue;
    #[cfg(not(windows))]
    use datafusion::common::stats::Precision;
    #[cfg(not(windows))]
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    use datafusion::execution::object_store::ObjectStoreRegistry;
    #[cfg(not(windows))]
    use datafusion::logical_expr::Operator;
    use datafusion::optimizer::Analyzer;
    #[cfg(not(windows))]
    use datafusion::prelude::SessionContext;
    #[cfg(not(windows))]
    use datafusion_expr::{Expr, LogicalPlan};

    use super::{
        CAYENNE_QUERY_MEMORY_FLOOR_PERCENT, CAYENNE_QUERY_MEMORY_PERCENT, CayenneOptimizerRules,
        DEFAULT_QUERY_MEMORY_PERCENT, DataFusionBuilder, MEM_TIER_CEILING_FRACTION,
        MEM_TIER_FLOAT_CEILING_FRACTION, MEM_TIER_FLOOR_FRACTION, MEM_TIER_HEADROOM_FRACTION,
        build_compaction_runtime_env, configure_hash_join_memory_limits,
        coordinated_mem_tier_budget, effective_query_memory_limit,
        runtime_env_with_effective_memory_limit_and_object_store_registry,
        validate_compaction_memory_fraction,
    };
    use crate::dataaccelerator::AcceleratorEngineRegistry;
    use crate::status;
    #[cfg(not(windows))]
    use data_components::poly::PolyTableProvider;
    use runtime_object_store::registry::SpiceObjectStoreRegistry;
    #[cfg(not(windows))]
    use std::collections::HashMap;
    use std::sync::Arc;

    /// An explicit `runtime.query.memory_limit` is honored verbatim regardless of
    /// whether Cayenne is active — the coordinated default only applies when unset.
    #[test]
    fn effective_query_memory_limit_honors_explicit_value() {
        assert_eq!(
            effective_query_memory_limit(Some(123 << 30), true, 0, None),
            123 << 30
        );
        assert_eq!(
            effective_query_memory_limit(Some(123 << 30), false, 0, None),
            123 << 30
        );
        // A nonzero CDC reservation never overrides an explicit limit.
        assert_eq!(
            effective_query_memory_limit(Some(7), true, 1 << 30, None),
            7
        );
        // Nor does a DuckDB query-pool cap override an explicit limit.
        assert_eq!(
            effective_query_memory_limit(Some(123 << 30), false, 0, Some(1 << 30)),
            123 << 30
        );
    }

    /// Cayenne active, no explicit limit: a per-table CDC reservation at/under the
    /// base host/10 headroom leaves the default at the base 70%; a reservation ABOVE
    /// the headroom reduces the default by exactly the excess; and a very large
    /// reservation floors at `CAYENNE_QUERY_MEMORY_FLOOR_PERCENT` (never 0). Reads
    /// live host RAM, so it asserts the RELATIONSHIPS rather than absolute bytes.
    #[test]
    fn effective_query_memory_limit_reduces_by_cdc_reservation() {
        let total = crate::resource_monitor::get_total_memory();
        let base = total.saturating_mul(CAYENNE_QUERY_MEMORY_PERCENT) / 100;
        let headroom = total / MEM_TIER_HEADROOM_FRACTION;
        let floor = total.saturating_mul(CAYENNE_QUERY_MEMORY_FLOOR_PERCENT) / 100;

        // Reservation within the base headroom -> no reduction, stays at base 70%.
        assert_eq!(effective_query_memory_limit(None, true, 0, None), base);
        assert_eq!(
            effective_query_memory_limit(None, true, headroom, None),
            base
        );

        // Reservation above the headroom -> reduced by exactly the excess.
        let excess = headroom / 2;
        assert_eq!(
            effective_query_memory_limit(None, true, headroom + excess, None),
            base - excess
        );

        // A reservation larger than the whole host floors the pool, never 0.
        let floored = effective_query_memory_limit(None, true, total.saturating_mul(2), None);
        assert_eq!(floored, floor);
        assert!(floored > 0);

        // The reservation never affects the non-Cayenne default.
        assert_eq!(
            effective_query_memory_limit(None, false, total, None),
            total.saturating_mul(DEFAULT_QUERY_MEMORY_PERCENT) / 100
        );

        // A DuckDB query-pool cap lowers (never raises) the default query pool.
        let non_cayenne_default = total.saturating_mul(DEFAULT_QUERY_MEMORY_PERCENT) / 100;
        let half = non_cayenne_default / 2;
        assert_eq!(
            effective_query_memory_limit(None, false, 0, Some(half)),
            half,
            "a smaller cap reduces the default"
        );
        assert_eq!(
            effective_query_memory_limit(None, false, 0, Some(non_cayenne_default * 2)),
            non_cayenne_default,
            "a larger cap never raises the default"
        );
    }

    // Compile-time invariants on the host-partition constants: the Cayenne
    // query-pool default must be below the non-Cayenne default, and the default
    // partition (70% query+compaction, one-fifth tier ceiling, one-tenth headroom)
    // sums to exactly 100% of host — a 90% allocated block plus a 10% headroom
    // reserve for the off-pool per-table CDC caches and OS overhead. `const`
    // assertions (compile-time) rather than a runtime test asserting constant values
    // (which clippy flags as assertions_on_constants).
    const _: () = assert!(CAYENNE_QUERY_MEMORY_PERCENT < DEFAULT_QUERY_MEMORY_PERCENT);
    const _: () = assert!(CAYENNE_QUERY_MEMORY_PERCENT == 70);
    const _: () = assert!(MEM_TIER_CEILING_FRACTION == 5); // one-fifth = 20%
    const _: () = assert!(MEM_TIER_HEADROOM_FRACTION == 10); // one-tenth = 10%
    // The default partition must not overcommit host RAM: query+compaction (%) +
    // tier ceiling (100/CEIL %) + headroom (100/HEAD %) <= 100. Cross-multiplied to
    // exact integer form (no truncation of fractional percentages). 70/20/10 = 100.
    const _: () = assert!(
        CAYENNE_QUERY_MEMORY_PERCENT * MEM_TIER_CEILING_FRACTION * MEM_TIER_HEADROOM_FRACTION
            + 100 * MEM_TIER_HEADROOM_FRACTION
            + 100 * MEM_TIER_CEILING_FRACTION
            <= 100 * MEM_TIER_CEILING_FRACTION * MEM_TIER_HEADROOM_FRACTION
    );
    // The float ceiling must sit ABOVE the base ceiling (smaller fraction = larger
    // share of host) or the query-light float is inert.
    const _: () = assert!(MEM_TIER_FLOAT_CEILING_FRACTION < MEM_TIER_CEILING_FRACTION);

    /// THE invariant: for the coordinated default partition (Cayenne active, no
    /// explicit limit), `query_pool + compaction + mem_tier + headroom` never
    /// exceeds host RAM — across a wide range of host sizes and compaction
    /// fractions. This is the cross-subsystem coordination that prevents the
    /// SF1000 process OOM (the three budgets summed to 115% of host before).
    #[test]
    fn coordinated_partition_never_overcommits_host() {
        // Mirrors the memory math in `DataFusionBuilder::build` for the default
        // (unset `memory_limit`) path so the asserted invariant tracks the build.
        let totals_gib: [u64; 7] = [8, 16, 32, 64, 128, 256, 1024];
        let compaction_percents: [u64; 5] = [5, 10, 20, 30, 50];
        for gib in totals_gib {
            let total = gib << 30;
            for compaction_pct in compaction_percents {
                let pre_carve = total.saturating_mul(CAYENNE_QUERY_MEMORY_PERCENT) / 100;
                let compaction = pre_carve.saturating_mul(compaction_pct) / 100;
                let query_pool = pre_carve.saturating_sub(compaction);
                let tier = coordinated_mem_tier_budget(total, query_pool, compaction, 0);
                let headroom = total / MEM_TIER_HEADROOM_FRACTION;
                let sum = query_pool + compaction + tier + headroom;
                assert!(
                    sum <= total,
                    "overcommit at {gib} GiB host, compaction {compaction_pct}%: query={query_pool} compaction={compaction} tier={tier} headroom={headroom} sum={sum} > total={total}"
                );
            }
        }
    }

    /// While the coordinated remainder reaches the floor, the tier budget stays inside
    /// `[host/32, host/MEM_TIER_FLOAT_CEILING]` — never above the float ceiling even
    /// when the pools are tiny, and the float (host/4) only engages on a query-light
    /// deployment without breaking the no-overcommit invariant. A greedy pool drives
    /// the remainder under the floor and the budget follows it down, but never to 0 (a
    /// 0 budget disables the global aggregate cap, the original no-global-cap OOM).
    #[test]
    fn coordinated_tier_budget_stays_within_clamp() {
        for gib in [16_u64, 64, 256, 1024] {
            let total = gib << 30;
            let base_ceiling = total / MEM_TIER_CEILING_FRACTION;
            let float_ceiling = total / MEM_TIER_FLOAT_CEILING_FRACTION;
            let floor = (total / MEM_TIER_FLOOR_FRACTION).min(base_ceiling);

            // A tiny query pool (query-light) → the tier floats up to the raised
            // ceiling to use the spare RAM, never above it.
            let big = coordinated_mem_tier_budget(total, total / 100, 0, 0);
            assert_eq!(
                big, float_ceiling,
                "a query-light deployment floats the tier to the raised ceiling"
            );
            // ...and even at the raised ceiling the no-overcommit invariant holds.
            let headroom = total / MEM_TIER_HEADROOM_FRACTION;
            assert!(
                (total / 100) + big + headroom <= total,
                "the float must not overcommit host RAM"
            );

            // A moderate query pool at the default 70% partition stays at/under the
            // BASE ceiling (the float only helps when the pool is sized down) and
            // at/above the floor (remainder still allows the lower clamp).
            let pre_carve = total.saturating_mul(CAYENNE_QUERY_MEMORY_PERCENT) / 100;
            let moderate = coordinated_mem_tier_budget(total, pre_carve, 0, 0);
            assert!(
                moderate <= base_ceiling,
                "the default partition does not float above the base ceiling"
            );
            assert!(
                moderate >= floor,
                "when remainder allows, the tier budget stays at/above the floor"
            );

            // A greedy pool that consumes all of host → tier yields to the
            // remainder (honest, no forced overcommit). Remainder is 0 after
            // headroom, but we still install a 1-byte always-refuse gate so the
            // global cap is never disabled (try_reserve fails → spill). The
            // meaningful claim here is the refuse-all gate, not "no host
            // overcommit" via the floor — the pool already consumes `total`.
            let small = coordinated_mem_tier_budget(total, total, 0, 0);
            assert_eq!(
                small, 1,
                "a greedy pool installs a 1-byte refuse-all gate rather than the host/32 floor"
            );
            assert!(small > 0, "the global aggregate cap must never be disabled");
            assert!(
                small < floor,
                "a greedy pool yields BELOW the floor rather than clamping up to it (floor={floor}, small={small})"
            );
        }
    }

    /// A non-zero external (`DuckDB`) reservation is subtracted from BOTH the tier
    /// remainder and its query-light float, so the tier can only shrink — it can't
    /// reclaim externally-reserved memory — and `query + external + tier + headroom`
    /// never exceeds host RAM.
    #[test]
    fn coordinated_tier_budget_reserves_external_bytes() {
        for gib in [16_u64, 64, 256, 1024] {
            let total = gib << 30;
            let headroom = total / MEM_TIER_HEADROOM_FRACTION;
            let query_pool = total / 10; // query-light: the tier would otherwise float up
            let external = total / 2; // a sizeable co-resident DuckDB reservation

            let with_ext = coordinated_mem_tier_budget(total, query_pool, 0, external);
            let no_ext = coordinated_mem_tier_budget(total, query_pool, 0, 0);

            assert!(
                with_ext <= no_ext,
                "gib={gib}: an external reservation must never grow the tier"
            );
            let sum = query_pool + external + with_ext + headroom;
            assert!(
                sum <= total,
                "gib={gib}: overcommit — query={query_pool} external={external} tier={with_ext} headroom={headroom} sum={sum} > total={total}"
            );
        }
    }

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
        for (rule, expected_name) in default_rules.iter().zip(expected_rule_names) {
            assert_eq!(
                expected_name,
                rule.name(),
                "Default analyzer rule order has changed"
            );
        }
    }

    #[tokio::test]
    async fn test_runtime_env_applies_metadata_cache_limit() {
        let object_store_registry: Arc<dyn ObjectStoreRegistry> = Arc::new(
            SpiceObjectStoreRegistry::new(tokio::runtime::Handle::current()),
        );
        let runtime_env = runtime_env_with_effective_memory_limit_and_object_store_registry(
            1024 * 1024,
            None,
            object_store_registry,
            Some(8 * 1024 * 1024),
        );

        assert_eq!(
            runtime_env.cache_manager.get_metadata_cache_limit(),
            8 * 1024 * 1024
        );
    }

    #[tokio::test]
    async fn compaction_runtime_env_separate_pool_shared_object_store() {
        let object_store_registry: Arc<dyn ObjectStoreRegistry> = Arc::new(
            SpiceObjectStoreRegistry::new(tokio::runtime::Handle::current()),
        );
        let query_env = runtime_env_with_effective_memory_limit_and_object_store_registry(
            1024 * 1024 * 1024,
            None,
            object_store_registry,
            None,
        );
        let compaction_env = build_compaction_runtime_env(
            256 * 1024 * 1024,
            Arc::clone(&query_env.object_store_registry),
            None,
        )
        .expect("compaction RuntimeEnv should build");

        // The carved compaction pool is a DISTINCT pool, so compaction memory is
        // accounted and bounded separately and cannot starve queries.
        assert!(
            !std::sync::Arc::ptr_eq(&query_env.memory_pool, &compaction_env.memory_pool),
            "compaction env must have its own memory pool"
        );
        // ...but it SHARES the query env's object-store registry, so compaction
        // reads and writes the same stores Cayenne registered there.
        assert!(
            std::sync::Arc::ptr_eq(
                &query_env.object_store_registry,
                &compaction_env.object_store_registry
            ),
            "compaction env must share the query object-store registry"
        );
    }

    #[test]
    fn validate_compaction_memory_fraction_accepts_only_strict_fractions() {
        assert_eq!(validate_compaction_memory_fraction(0.5), Some(0.5));
        assert_eq!(validate_compaction_memory_fraction(0.0), None);
        assert_eq!(validate_compaction_memory_fraction(1.0), None);
        assert_eq!(validate_compaction_memory_fraction(f64::NAN), None);
        assert_eq!(validate_compaction_memory_fraction(f64::INFINITY), None);
    }

    #[test]
    fn test_hash_join_inlist_pushdown_limit_respects_runtime_query_memory_limit() {
        let mut config = datafusion::prelude::SessionConfig::new().with_target_partitions(4);
        config
            .options_mut()
            .optimizer
            .hash_join_inlist_pushdown_max_size = 1_000;

        configure_hash_join_memory_limits(&mut config, 2_048);

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

        configure_hash_join_memory_limits(&mut config, 1_000_000);

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
        // memory_limit 1_024 / EXACT_JOIN_FILTER_MEMORY_POOL_FRACTION_DENOMINATOR (8) = 128.
        assert_eq!(config.exact_join_filter_max_bytes, 128);
    }

    #[test]
    fn test_target_partitions_wires_through_to_session_config() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime");
        let handle = rt.handle().clone();

        let df = DataFusionBuilder::new(
            status::RuntimeStatus::new(),
            Arc::new(AcceleratorEngineRegistry::default()),
            handle.clone(),
        )
        .target_partitions(Some(4))
        .build();

        assert_eq!(
            df.ctx
                .state()
                .config()
                .options()
                .execution
                .target_partitions,
            4,
            "target_partitions wired through DataFusionBuilder should be visible on the session config"
        );

        // Sanity check the inverse — None leaves DataFusion's default in place.
        let df_default = DataFusionBuilder::new(
            status::RuntimeStatus::new(),
            Arc::new(AcceleratorEngineRegistry::default()),
            handle,
        )
        .target_partitions(None)
        .build();
        assert_ne!(
            df_default
                .ctx
                .state()
                .config()
                .options()
                .execution
                .target_partitions,
            4,
            "Without an override target_partitions should fall back to DataFusion's default"
        );
    }

    #[test]
    fn test_eager_aggregation_wires_through_to_session_config() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime");
        let handle = rt.handle().clone();

        // Default: spiced enables eager aggregation even though DataFusion's own
        // default is off.
        let df_default = DataFusionBuilder::new(
            status::RuntimeStatus::new(),
            Arc::new(AcceleratorEngineRegistry::default()),
            handle.clone(),
        )
        .build();
        assert!(
            df_default
                .ctx
                .state()
                .config()
                .options()
                .optimizer
                .enable_eager_aggregation,
            "eager aggregation should default to enabled in spiced"
        );

        // An explicit `false` disables the rule.
        let df_off = DataFusionBuilder::new(
            status::RuntimeStatus::new(),
            Arc::new(AcceleratorEngineRegistry::default()),
            handle.clone(),
        )
        .eager_aggregation(Some(false))
        .build();
        assert!(
            !df_off
                .ctx
                .state()
                .config()
                .options()
                .optimizer
                .enable_eager_aggregation,
            "eager_aggregation: false should disable the rule"
        );

        // The two tuning knobs wire through to the optimizer options.
        let df_tuned = DataFusionBuilder::new(
            status::RuntimeStatus::new(),
            Arc::new(AcceleratorEngineRegistry::default()),
            handle,
        )
        .eager_aggregation(Some(true))
        .eager_aggregation_min_reduction_factor(Some(8))
        .eager_aggregation_max_pushed_groups(Some(1024))
        .build();
        assert_eq!(
            df_tuned
                .ctx
                .state()
                .config()
                .options()
                .optimizer
                .eager_aggregation_min_reduction_factor,
            8
        );
        assert_eq!(
            df_tuned
                .ctx
                .state()
                .config()
                .options()
                .optimizer
                .eager_aggregation_max_pushed_groups,
            1024
        );
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
    fn test_built_datafusion_uses_conservative_cayenne_optimizer_defaults() {
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
        let logical_rule_names: Vec<&str> = state
            .optimizers()
            .iter()
            .map(|r| r.name())
            .filter(|name| name.starts_with("cayenne_"))
            .collect();
        let physical_rule_names: Vec<&str> = state
            .physical_optimizers()
            .iter()
            .map(|r| r.name())
            .filter(|name| name.starts_with("Cayenne"))
            .collect();

        assert_eq!(
            logical_rule_names,
            vec![
                "cayenne_push_down_semi_join",
                "cayenne_reassociate_cross_join"
            ],
            "Default Cayenne logical optimizer selection should keep the risky legacy logical rewrites (filter propagation, in-list range) off while enabling the scoped cross-join reassociation and the Q18 semi-join pushdown"
        );
        assert_eq!(
            physical_rule_names,
            vec![
                "CayenneDynamicFilterSharing",
                "CayenneMaintainedAggregateRewriter",
                "CayenneStatsAggregateRewriter",
                "CayenneAntiJoinSortMergeRewriter",
            ],
            "Default Cayenne physical optimizer selection should preserve prior safe defaults (now including the metadata-only stats aggregate fold) without re-enabling the exact join filter"
        );
    }

    #[test]
    #[cfg(not(windows))]
    fn test_built_datafusion_can_disable_cayenne_logical_rule() {
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
        .cayenne_filter_propagation_enabled(false)
        .build();

        let state = df.ctx.state();

        assert!(
            !state
                .optimizers()
                .iter()
                .any(|r| r.name() == "cayenne_propagate_filter_across_equi_join_keys"),
            "Cayenne logical filter propagation should stay disableable"
        );
        assert!(
            state
                .optimizers()
                .iter()
                .any(|r| r.name() == "cayenne_reassociate_cross_join"),
            "Disabling Cayenne filter propagation should not disable the scoped cross-join reassociation default"
        );
    }

    #[test]
    #[cfg(not(windows))]
    fn test_built_datafusion_can_disable_all_cayenne_optimizer_rules() {
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
        .cayenne_optimizer_rules(CayenneOptimizerRules::none())
        .build();

        let state = df.ctx.state();
        assert!(
            !state
                .optimizers()
                .iter()
                .any(|r| r.name().starts_with("cayenne_")),
            "No Cayenne logical optimizer rules should be registered when rule selection is none"
        );
        assert!(
            !state
                .physical_optimizers()
                .iter()
                .any(|r| r.name().starts_with("Cayenne")),
            "No Cayenne physical optimizer rules should be registered when rule selection is none"
        );
    }

    #[test]
    #[cfg(not(windows))]
    fn test_built_datafusion_can_enable_one_cayenne_physical_rule() {
        let mut rules = CayenneOptimizerRules::none();
        rules.set_dynamic_filter_sharing(true);

        let (_, physical_rule_names) = built_datafusion_cayenne_rule_names(rules);

        assert_eq!(physical_rule_names, vec!["CayenneDynamicFilterSharing"]);
    }

    #[test]
    #[cfg(not(windows))]
    fn test_built_datafusion_can_select_each_cayenne_optimizer_rule() {
        let mut filter_propagation = CayenneOptimizerRules::none();
        filter_propagation.set_filter_propagation(true);
        let mut cross_join_reassociation = CayenneOptimizerRules::none();
        cross_join_reassociation.set_cross_join_reassociation(true);
        let mut inlist_to_range = CayenneOptimizerRules::none();
        inlist_to_range.set_inlist_to_range(true);
        let mut semi_join_pushdown = CayenneOptimizerRules::none();
        semi_join_pushdown.set_semi_join_pushdown(true);
        let mut join_reorder = CayenneOptimizerRules::none();
        join_reorder.set_join_reorder(true);
        let mut dynamic_filter_sharing = CayenneOptimizerRules::none();
        dynamic_filter_sharing.set_dynamic_filter_sharing(true);
        let mut maintained_aggregate = CayenneOptimizerRules::none();
        maintained_aggregate.set_maintained_aggregate(true);
        let mut stats_aggregate = CayenneOptimizerRules::none();
        stats_aggregate.set_stats_aggregate(true);
        let mut anti_join_sort_merge = CayenneOptimizerRules::none();
        anti_join_sort_merge.set_anti_join_sort_merge(true);
        let mut exact_join_filter = CayenneOptimizerRules::none();
        exact_join_filter.set_exact_join_filter(true);

        let cases = [
            (
                filter_propagation,
                vec!["cayenne_propagate_filter_across_equi_join_keys"],
                vec![],
            ),
            (
                cross_join_reassociation,
                vec!["cayenne_reassociate_cross_join"],
                vec![],
            ),
            (
                inlist_to_range,
                vec!["cayenne_inlist_to_range_rewrite"],
                vec![],
            ),
            (
                semi_join_pushdown,
                vec!["cayenne_push_down_semi_join"],
                vec![],
            ),
            (join_reorder, vec!["reorder_join"], vec![]),
            (
                dynamic_filter_sharing,
                vec![],
                vec!["CayenneDynamicFilterSharing"],
            ),
            (
                maintained_aggregate,
                vec![],
                vec!["CayenneMaintainedAggregateRewriter"],
            ),
            (
                stats_aggregate,
                vec![],
                vec!["CayenneStatsAggregateRewriter"],
            ),
            (
                anti_join_sort_merge,
                vec![],
                vec!["CayenneAntiJoinSortMergeRewriter"],
            ),
            (exact_join_filter, vec![], vec!["CayenneJoinRewriter"]),
        ];

        for (rules, expected_logical_rules, expected_physical_rules) in cases {
            let (logical_rule_names, physical_rule_names) =
                built_datafusion_cayenne_rule_names(rules);

            assert_eq!(logical_rule_names, expected_logical_rules);
            assert_eq!(physical_rule_names, expected_physical_rules);
        }
    }

    #[test]
    #[cfg(not(windows))]
    fn test_selected_cayenne_inlist_rule_rewrites_only_cayenne_backed_queries() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime");
        let handle = rt.handle().clone();
        let mut rules = CayenneOptimizerRules::none();
        rules.set_inlist_to_range(true);

        let df = DataFusionBuilder::new(
            status::RuntimeStatus::new(),
            Arc::new(AcceleratorEngineRegistry::default()),
            handle,
        )
        .cayenne_optimizer_rules(rules)
        .build();

        rt.block_on(async {
            register_inlist_test_table(&df.ctx, "plain_inlist", false);
            register_inlist_test_table(&df.ctx, "cayenne_inlist", true);

            let plain_plan = optimized_inlist_query_plan(&df.ctx, "plain_inlist").await;
            assert!(
                logical_plan_contains_expr(&plain_plan, |expr| matches!(expr, Expr::InList(_))),
                "selected Cayenne IN-list rewrite must leave non-Cayenne queries untouched; plan was:\n{plain_plan}"
            );
            assert!(
                !logical_plan_has_inlist_range_rewrite(&plain_plan),
                "non-Cayenne query should not be rewritten to a range predicate; plan was:\n{plain_plan}"
            );

            let cayenne_plan = optimized_inlist_query_plan(&df.ctx, "cayenne_inlist").await;
            assert!(
                !logical_plan_contains_expr(&cayenne_plan, |expr| matches!(expr, Expr::InList(_))),
                "Cayenne-backed query should not retain the original IN-list predicate; plan was:\n{cayenne_plan}"
            );
            assert!(
                logical_plan_has_inlist_range_rewrite(&cayenne_plan),
                "Cayenne-backed query should be rewritten to a range predicate; plan was:\n{cayenne_plan}"
            );
        });
    }

    #[test]
    #[cfg(not(windows))]
    fn test_enabled_cayenne_filter_propagation_rewrites_only_selective_large_fact_queries() {
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
        .cayenne_filter_propagation_enabled(true)
        .build();

        rt.block_on(async {
            register_large_fact_dimension_tables(&df.ctx);

            let large_fact_plan = optimized_sql_query_plan(
                &df.ctx,
                "SELECT s_suppkey FROM supplier, nation \
                 WHERE s_nationkey = n_nationkey AND n_name = 'CHINA'",
            )
            .await;
            assert!(
                logical_plan_has_propagated_filter_marker(&large_fact_plan),
                "enabled Cayenne filter propagation should fire for the selective large-fact join; plan was:\n{large_fact_plan}"
            );

            let no_dim_filter_plan = optimized_sql_query_plan(
                &df.ctx,
                "SELECT s_suppkey FROM supplier, nation \
                 WHERE s_nationkey = n_nationkey",
            )
            .await;
            assert!(
                !logical_plan_has_propagated_filter_marker(&no_dim_filter_plan),
                "Cayenne joins without a selective dim-side filter should not receive propagated filters; plan was:\n{no_dim_filter_plan}"
            );

            let small_fact_plan = optimized_sql_query_plan(
                &df.ctx,
                "SELECT s_suppkey FROM small_supplier, nation \
                 WHERE s_nationkey = n_nationkey AND n_name = 'CHINA'",
            )
            .await;
            assert!(
                !logical_plan_has_propagated_filter_marker(&small_fact_plan),
                "Cayenne joins below the fact-cardinality payoff threshold should not receive propagated filters; plan was:\n{small_fact_plan}"
            );
        });
    }

    #[test]
    #[cfg(not(windows))]
    fn test_enabled_cayenne_filter_propagation_handles_mixed_cayenne_and_non_cayenne_joins() {
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
        .cayenne_filter_propagation_enabled(true)
        .build();

        rt.block_on(async {
            register_stat_table(
                &df.ctx,
                "nation_plain",
                vec![
                    Field::new("n_nationkey", DataType::Int64, false),
                    Field::new("n_name", DataType::Utf8, true),
                ],
                25,
                false,
            );
            register_stat_table(
                &df.ctx,
                "supplier_cayenne",
                vec![
                    Field::new("s_suppkey", DataType::Int64, false),
                    Field::new("s_nationkey", DataType::Int64, false),
                ],
                500_000,
                true,
            );
            register_stat_table(
                &df.ctx,
                "nation_cayenne",
                vec![
                    Field::new("n_nationkey", DataType::Int64, false),
                    Field::new("n_name", DataType::Utf8, true),
                ],
                25,
                true,
            );
            register_stat_table(
                &df.ctx,
                "supplier_plain",
                vec![
                    Field::new("s_suppkey", DataType::Int64, false),
                    Field::new("s_nationkey", DataType::Int64, false),
                ],
                500_000,
                false,
            );

            let cayenne_probe_plan = optimized_sql_query_plan(
                &df.ctx,
                "SELECT s_suppkey FROM supplier_cayenne, nation_plain \
                 WHERE s_nationkey = n_nationkey AND n_name = 'CHINA'",
            )
            .await;
            assert!(
                logical_plan_has_propagated_filter_marker(&cayenne_probe_plan),
                "mixed-source join should still propagate onto the Cayenne-backed side when the selective large-fact shape is present; plan was:\n{cayenne_probe_plan}"
            );

            let non_cayenne_probe_plan = optimized_sql_query_plan(
                &df.ctx,
                "SELECT s_suppkey FROM supplier_plain, nation_cayenne \
                 WHERE s_nationkey = n_nationkey AND n_name = 'CHINA'",
            )
            .await;
            assert!(
                !logical_plan_has_propagated_filter_marker(&non_cayenne_probe_plan),
                "mixed-source join should not propagate onto a non-Cayenne probe side; plan was:\n{non_cayenne_probe_plan}"
            );
        });
    }

    #[test]
    #[cfg(not(windows))]
    fn test_built_datafusion_registers_cayenne_logical_rule_when_enabled() {
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
        .cayenne_filter_propagation_enabled(true)
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
        let eliminate_cross_join_position = rule_names
            .iter()
            .position(|name| *name == "eliminate_cross_join")
            .expect("DataFusion eliminate_cross_join rule should be registered");
        let reassociate_position = rule_names
            .iter()
            .position(|name| *name == "cayenne_reassociate_cross_join")
            .expect("Cayenne cross join reassociation rule should be registered");
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
        assert!(
            eliminate_cross_join_position < reassociate_position,
            "Cayenne cross join reassociation must run after DataFusion exposes join predicates from SQL FROM-order cross joins"
        );
        assert!(
            reassociate_position < push_down_position,
            "Cayenne cross join reassociation must run before push_down_filter consumes the join tree shape"
        );
        let reorder_position = rule_names
            .iter()
            .position(|name| *name == "reorder_join")
            .expect("reorder_join rule should be registered (join_reorder is on by default)");
        let optimize_projections_position = rule_names
            .iter()
            .position(|name| *name == "optimize_projections")
            .expect("DataFusion optimize_projections rule should be registered");
        assert!(
            push_down_position < reorder_position,
            "reorder_join must run AFTER push_down_filter so TableScan.filters are populated for cost-based join reordering"
        );
        assert!(
            reorder_position < optimize_projections_position,
            "reorder_join must run BEFORE optimize_projections, which inserts Projections between joins that fragment the reorderable join graph into opaque leaves"
        );
        assert_eq!(
            rule_names
                .iter()
                .filter(|name| **name == "cayenne_propagate_filter_across_equi_join_keys")
                .count(),
            1,
            "Cayenne logical filter propagation rule should be registered exactly once"
        );
        assert_eq!(
            rule_names
                .iter()
                .filter(|name| **name == "cayenne_reassociate_cross_join")
                .count(),
            1,
            "Cayenne cross join reassociation rule should be registered exactly once"
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
        .cayenne_filter_propagation_enabled(true)
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
                .expect("selective large-fact query should create a dataframe");
            let optimized_plan = dataframe
                .clone()
                .into_optimized_plan()
                .expect("selective large-fact query should optimize");
            let optimized_plan = optimized_plan.to_string();

            assert!(
                !optimized_plan.contains("InSubquery"),
                "Cayenne propagated subqueries must be decorrelated before physical planning: {optimized_plan}"
            );

            dataframe
                .create_physical_plan()
                .await
                .expect("selective large-fact query should create a physical plan");
        });
    }

    /// Regression test for the post-decorrelation re-propagation bug
    /// (`cayenne::logical_optimizer`): after the rule wraps a Filter with
    /// `InSubquery` and `DataFusion` decorrelates it to `LeftSemi`, the
    /// optimizer iterates the rule pipeline to fixed point. Without the
    /// cycle-detection fix in `analyze_logical_side`, the rule would re-fire
    /// each pass and stack one redundant `LeftSemi` per iteration up to
    /// `max_passes`. This integration test runs the full optimizer pipeline
    /// and asserts the final plan has at most one `LeftSemi` for the selective
    /// large-fact shape — proving the cycle guard holds across decorrelation.
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
        .cayenne_filter_propagation_enabled(true)
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
                .expect("selective large-fact query should create a dataframe");
            let optimized_plan = dataframe
                .into_optimized_plan()
                .expect("selective large-fact query should optimize");
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
        .cayenne_optimizer_rules(CayenneOptimizerRules::all_enabled())
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
        let cayenne_maintained_aggregate_position = rule_names
            .iter()
            .position(|name| *name == "CayenneMaintainedAggregateRewriter")
            .expect("Cayenne maintained aggregate rewriter should be registered");
        let cayenne_anti_sort_merge_position = rule_names
            .iter()
            .position(|name| *name == "CayenneAntiJoinSortMergeRewriter")
            .expect("Cayenne anti join sort-merge rewriter should be registered");
        let cayenne_join_rewriter_position = rule_names
            .iter()
            .position(|name| *name == "CayenneJoinRewriter")
            .expect("Cayenne join rewriter should be registered when exact_join_filter is on");

        assert!(
            sanity_check_position < cayenne_filter_sharing_position,
            "CayenneDynamicFilterSharing must run after DataFusion's built-in physical optimizer rules"
        );
        assert!(
            cayenne_filter_sharing_position < cayenne_anti_sort_merge_position,
            "CayenneDynamicFilterSharing must run before CayenneAntiJoinSortMergeRewriter so same-source joins can receive shared scan filters before any sort-merge rewrite"
        );
        assert!(
            cayenne_filter_sharing_position < cayenne_maintained_aggregate_position,
            "CayenneMaintainedAggregateRewriter should run with the Cayenne physical rules after DataFusion's built-in physical optimizer rules"
        );
        assert!(
            cayenne_maintained_aggregate_position < cayenne_anti_sort_merge_position,
            "CayenneMaintainedAggregateRewriter should run before Cayenne join rewrites"
        );
        assert!(
            cayenne_anti_sort_merge_position < cayenne_join_rewriter_position,
            "CayenneJoinRewriter must run after same-source sort-merge rewrites so it only touches remaining HashJoinExec nodes"
        );
    }

    #[cfg(not(windows))]
    fn built_datafusion_cayenne_rule_names(
        rules: CayenneOptimizerRules,
    ) -> (Vec<String>, Vec<String>) {
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
        .cayenne_optimizer_rules(rules)
        .build();

        let state = df.ctx.state();
        let logical_rule_names = state
            .optimizers()
            .iter()
            .map(|rule| rule.name().to_string())
            // Cayenne-gated logical rules are `cayenne_*`, plus `reorder_join`
            // (the join-reorder rule, which keeps its DataFusion-style name).
            .filter(|rule_name| rule_name.starts_with("cayenne_") || rule_name == "reorder_join")
            .collect();
        let physical_rule_names = state
            .physical_optimizers()
            .iter()
            .map(|rule| rule.name().to_string())
            .filter(|rule_name| rule_name.starts_with("Cayenne"))
            .collect();

        (logical_rule_names, physical_rule_names)
    }

    #[cfg(not(windows))]
    fn register_inlist_test_table(ctx: &SessionContext, table_name: &str, cayenne_backed: bool) {
        let mut metadata = HashMap::new();
        if cayenne_backed {
            metadata.insert("spice.accelerator".to_string(), "cayenne".to_string());
        }
        let schema = Arc::new(Schema::new_with_metadata(
            vec![Field::new("id", DataType::Int64, false)],
            metadata,
        ));

        ctx.register_table(
            table_name,
            Arc::new(
                MemTable::try_new(Arc::clone(&schema), vec![vec![]])
                    .expect("in-list test table should be valid"),
            ),
        )
        .expect("in-list test table should register");
    }

    #[cfg(not(windows))]
    fn register_large_fact_dimension_tables(ctx: &SessionContext) {
        register_stat_table(
            ctx,
            "nation",
            vec![
                Field::new("n_nationkey", DataType::Int64, false),
                Field::new("n_name", DataType::Utf8, true),
            ],
            25,
            true,
        );
        register_stat_table(
            ctx,
            "supplier",
            vec![
                Field::new("s_suppkey", DataType::Int64, false),
                Field::new("s_nationkey", DataType::Int64, false),
            ],
            500_000,
            true,
        );
        register_stat_table(
            ctx,
            "small_supplier",
            vec![
                Field::new("s_suppkey", DataType::Int64, false),
                Field::new("s_nationkey", DataType::Int64, false),
            ],
            1_000,
            true,
        );
    }

    #[cfg(not(windows))]
    fn register_stat_table(
        ctx: &SessionContext,
        table_name: &str,
        fields: Vec<Field>,
        num_rows: usize,
        cayenne_backed: bool,
    ) {
        let metadata = if cayenne_backed {
            HashMap::from([("spice.accelerator".to_string(), "cayenne".to_string())])
        } else {
            HashMap::new()
        };

        let schema = Arc::new(Schema::new_with_metadata(fields, metadata));

        ctx.register_table(
            table_name,
            Arc::new(
                StatMemTable::try_new(Arc::clone(&schema), vec![vec![]], num_rows)
                    .expect("selective large-fact stat table should be valid"),
            ),
        )
        .expect("selective large-fact stat table should register");
    }

    #[cfg(not(windows))]
    #[derive(Debug)]
    struct StatMemTable {
        inner: MemTable,
        num_rows: usize,
    }

    #[cfg(not(windows))]
    impl StatMemTable {
        fn try_new(
            schema: Arc<Schema>,
            batches: Vec<Vec<arrow::array::RecordBatch>>,
            num_rows: usize,
        ) -> datafusion::error::Result<Self> {
            Ok(Self {
                inner: MemTable::try_new(schema, batches)?,
                num_rows,
            })
        }
    }

    #[cfg(not(windows))]
    #[async_trait::async_trait]
    impl TableProvider for StatMemTable {
        fn schema(&self) -> Arc<Schema> {
            self.inner.schema()
        }

        fn table_type(&self) -> datafusion::datasource::TableType {
            self.inner.table_type()
        }

        async fn scan(
            &self,
            state: &dyn datafusion::catalog::Session,
            projection: Option<&Vec<usize>>,
            filters: &[Expr],
            limit: Option<usize>,
        ) -> datafusion::error::Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
            self.inner.scan(state, projection, filters, limit).await
        }

        fn statistics(&self) -> Option<datafusion::common::Statistics> {
            Some(datafusion::common::Statistics {
                num_rows: Precision::Exact(self.num_rows),
                total_byte_size: Precision::Absent,
                column_statistics: vec![],
            })
        }
    }

    #[cfg(not(windows))]
    async fn optimized_inlist_query_plan(ctx: &SessionContext, table_name: &str) -> LogicalPlan {
        ctx.sql(&format!(
            "SELECT id FROM {table_name} WHERE id IN (5, 6, 7, 8)"
        ))
        .await
        .expect("in-list test query should create a dataframe")
        .into_optimized_plan()
        .expect("in-list test query should optimize")
    }

    #[cfg(not(windows))]
    async fn optimized_sql_query_plan(ctx: &SessionContext, sql: &str) -> LogicalPlan {
        ctx.sql(sql)
            .await
            .expect("test query should create a dataframe")
            .into_optimized_plan()
            .expect("test query should optimize")
    }

    #[cfg(not(windows))]
    fn logical_plan_contains_expr(
        plan: &LogicalPlan,
        matches_expr: impl Fn(&Expr) -> bool,
    ) -> bool {
        let mut found = false;
        let _ = plan.apply(|node| {
            match node {
                LogicalPlan::Filter(filter) => {
                    found = expr_tree_contains(&filter.predicate, &matches_expr);
                }
                LogicalPlan::TableScan(scan) => {
                    found = scan
                        .filters
                        .iter()
                        .any(|filter| expr_tree_contains(filter, &matches_expr));
                }
                _ => {}
            }

            if found {
                Ok(TreeNodeRecursion::Stop)
            } else {
                Ok(TreeNodeRecursion::Continue)
            }
        });
        found
    }

    #[cfg(not(windows))]
    fn logical_plan_has_inlist_range_rewrite(plan: &LogicalPlan) -> bool {
        logical_plan_contains_expr(plan, expr_is_id_between_5_and_8)
            || (logical_plan_contains_expr(plan, |expr| {
                expr_is_id_literal_comparison(expr, Operator::GtEq, 5)
            }) && logical_plan_contains_expr(plan, |expr| {
                expr_is_id_literal_comparison(expr, Operator::LtEq, 8)
            }))
    }

    #[cfg(not(windows))]
    fn logical_plan_has_propagated_filter_marker(plan: &LogicalPlan) -> bool {
        let mut found = false;
        let _ = plan.apply(|node| {
            if let LogicalPlan::SubqueryAlias(alias) = node
                && alias
                    .alias
                    .table()
                    .starts_with(PROPAGATED_FILTER_ALIAS_PREFIX)
            {
                found = true;
                return Ok(TreeNodeRecursion::Stop);
            }
            if let LogicalPlan::Filter(filter) = node
                && expr_tree_contains(&filter.predicate, &expr_has_propagated_filter_marker)
            {
                found = true;
                return Ok(TreeNodeRecursion::Stop);
            }

            Ok(TreeNodeRecursion::Continue)
        });
        found
    }

    #[cfg(not(windows))]
    fn expr_has_propagated_filter_marker(expr: &Expr) -> bool {
        matches!(
            expr,
            Expr::InSubquery(in_subquery)
                if matches!(
                    in_subquery.subquery.subquery.as_ref(),
                    LogicalPlan::SubqueryAlias(alias)
                        if alias
                            .alias
                            .table()
                            .starts_with(PROPAGATED_FILTER_ALIAS_PREFIX)
                )
        )
    }

    #[cfg(not(windows))]
    fn expr_is_id_between_5_and_8(expr: &Expr) -> bool {
        matches!(
            expr,
            Expr::Between(between)
                if !between.negated
                    && expr_is_id_column(&between.expr)
                    && expr_is_int64_literal(&between.low, 5)
                    && expr_is_int64_literal(&between.high, 8)
        )
    }

    #[cfg(not(windows))]
    fn expr_is_id_literal_comparison(expr: &Expr, operator: Operator, literal: i64) -> bool {
        let Expr::BinaryExpr(binary) = expr else {
            return false;
        };

        (binary.op == operator
            && expr_is_id_column(&binary.left)
            && expr_is_int64_literal(&binary.right, literal))
            || (binary.op == reversed_comparison_operator(operator)
                && expr_is_int64_literal(&binary.left, literal)
                && expr_is_id_column(&binary.right))
    }

    #[cfg(not(windows))]
    fn reversed_comparison_operator(operator: Operator) -> Operator {
        match operator {
            Operator::GtEq => Operator::LtEq,
            Operator::LtEq => Operator::GtEq,
            Operator::Gt => Operator::Lt,
            Operator::Lt => Operator::Gt,
            _ => operator,
        }
    }

    #[cfg(not(windows))]
    fn expr_is_id_column(expr: &Expr) -> bool {
        matches!(expr, Expr::Column(column) if column.name == "id")
    }

    #[cfg(not(windows))]
    fn expr_is_int64_literal(expr: &Expr, expected: i64) -> bool {
        matches!(expr, Expr::Literal(ScalarValue::Int64(Some(value)), _) if *value == expected)
    }

    #[cfg(not(windows))]
    fn expr_tree_contains(expr: &Expr, matches_expr: &impl Fn(&Expr) -> bool) -> bool {
        let mut found = false;
        let _ = expr.apply(|expr| {
            if matches_expr(expr) {
                found = true;
                Ok(TreeNodeRecursion::Stop)
            } else {
                Ok(TreeNodeRecursion::Continue)
            }
        });
        found
    }
}
