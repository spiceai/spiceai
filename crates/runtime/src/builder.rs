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

use crate::cluster::ClusterStateStore;
use crate::cluster::DistributedNode;
use crate::cluster::ExecutorRegistry;
use crate::cluster::PartitionStore;
use crate::cluster::ResolvedClusterConfig;
use crate::cluster::SchedulerHeartbeatStore;
use crate::cluster::partition::service::PartitionService;
#[cfg(not(windows))]
use crate::config::ClusterRole;
use crate::config::Config;
#[cfg(not(windows))]
use crate::datafusion::builder::CayenneOptimizerRules;
use crate::datafusion::udf::register_udfs;
use crate::{
    Runtime, catalogconnector,
    dataaccelerator::AcceleratorEngineRegistry,
    dataconnector,
    datafusion::DataFusion,
    datasets_health_monitor::DatasetsHealthMonitor,
    extension::{Extension, ExtensionFactory},
    flight::RateLimits,
    secrets::{self, Secrets},
    status,
};
use app::App;
use runtime_acceleration::acceleration::{RefreshMode, unset_refresh_mode_for_connector};
use runtime_metrics as metrics;
use spicepod::component::runtime::Runtime as SpicepodRuntime;
use spicepod::component::runtime::RuntimeReadyState as SpicepodRuntimeReadyState;
use spicepod::component::runtime::SourceRateControl as SpicepodSourceRateControl;
use spicepod::component::runtime::TelemetryConfig;
use std::{collections::HashMap, net::SocketAddr, str::FromStr, sync::Arc, time::Duration};
use telemetry::metrics_reader::MetricsReader;
use telemetry::timing::TimeMeasurement;
use token_provider::registry::TokenProviderRegistry;
use tokio::runtime::Handle;
use tokio::sync::{Mutex, RwLock};

type DatafusionConfigurationCallback = fn(&mut DataFusion);

const CAYENNE_FOOTER_CACHE_MB_PARAM: &str = "cayenne_footer_cache_mb";
const CAYENNE_SEGMENT_CACHE_MB_PARAM: &str = "cayenne_segment_cache_mb";
const CAYENNE_SORT_MERGE_MIN_ROWS_PARAM: &str = "cayenne_sort_merge_min_rows";
const CAYENNE_SORT_MERGE_MEMORY_POOL_FRACTION_PARAM: &str =
    "cayenne_sort_merge_memory_pool_fraction";
const CAYENNE_FILTER_PROPAGATION_PARAM: &str = "cayenne_filter_propagation";
const CAYENNE_OPTIMIZER_RULES_PARAM: &str = "cayenne_optimizer_rules";

/// Goal-driven adaptive-tuning SLO setpoints, settable GLOBALLY here at
/// `runtime.params` and overridden per-dataset via the matching
/// `acceleration.params` key (see `accelerator_cayenne`). `cayenne_goal_qph`
/// is the exception: QPH is a system-wide metric (a join spans datasets), so it
/// is global-only and a per-dataset value is ignored. Declared here so the keys
/// are part of the recognized `runtime.params` vocabulary and don't false-warn as
/// unknown; the values are resolved (and validated) where the per-dataset Cayenne
/// config is built. NOTE: `cayenne_goal_convergence_window` is deliberately NOT
/// here — it paces HOW the loop chases these SLOs (a control-cadence/benchmarking
/// knob), not a target outcome, so it stays a per-dataset advanced override.
const CAYENNE_GOAL_REPLICATION_LAG_PARAM: &str = "cayenne_goal_replication_lag";
const CAYENNE_GOAL_FRESHNESS_PARAM: &str = "cayenne_goal_freshness";
const CAYENNE_GOAL_QUERY_LATENCY_PARAM: &str = "cayenne_goal_query_latency";
const CAYENNE_GOAL_QPH_PARAM: &str = "cayenne_goal_qph";

/// Process-global `SQLite` metastore pragma tuning keys (cache, mmap, busy
/// timeout, WAL autocheckpoint, `auto_vacuum`). Consumed once at startup in
/// `build_internal`; declared here so they're part of the recognized
/// `runtime.params` vocabulary and don't false-warn as unknown.
const CAYENNE_METASTORE_CACHE_MB_PARAM: &str = "cayenne_metastore_cache_mb";
const CAYENNE_METASTORE_MMAP_MB_PARAM: &str = "cayenne_metastore_mmap_mb";
const CAYENNE_METASTORE_BUSY_TIMEOUT_MS_PARAM: &str = "cayenne_metastore_busy_timeout_ms";
const CAYENNE_METASTORE_WAL_AUTOCHECKPOINT_PAGES_PARAM: &str =
    "cayenne_metastore_wal_autocheckpoint_pages";
const CAYENNE_METASTORE_WAL_TRUNCATE_THRESHOLD_MB_PARAM: &str =
    "cayenne_metastore_wal_truncate_threshold_mb";
const CAYENNE_METASTORE_AUTO_VACUUM_PARAM: &str = "cayenne_metastore_auto_vacuum";
const CAYENNE_METASTORE_INCREMENTAL_VACUUM_PAGES_PARAM: &str =
    "cayenne_metastore_incremental_vacuum_pages";

/// Runtime param: fraction of `runtime.query.memory_limit` carved into a
/// dedicated Cayenne compaction memory pool when Cayenne acceleration is
/// configured on a dataset and dedicated thread pools are enabled.
const CAYENNE_COMPACTION_MEMORY_FRACTION_PARAM: &str = "cayenne_compaction_memory_fraction";
/// Default carve fraction when the param is unset: 20% of the query budget to
/// compaction, 80% retained for queries.
const DEFAULT_COMPACTION_MEMORY_FRACTION: f64 = 0.2;
const MIN_COMPACTION_MEMORY_FRACTION: f64 = 0.05;
const MAX_COMPACTION_MEMORY_FRACTION: f64 = 0.9;

/// `runtime.params` keys with a `cayenne_` prefix that the runtime recognizes.
const KNOWN_CAYENNE_RUNTIME_PARAMS: &[&str] = &[
    CAYENNE_FOOTER_CACHE_MB_PARAM,
    CAYENNE_SEGMENT_CACHE_MB_PARAM,
    CAYENNE_SORT_MERGE_MIN_ROWS_PARAM,
    CAYENNE_SORT_MERGE_MEMORY_POOL_FRACTION_PARAM,
    CAYENNE_FILTER_PROPAGATION_PARAM,
    CAYENNE_OPTIMIZER_RULES_PARAM,
    CAYENNE_COMPACTION_MEMORY_FRACTION_PARAM,
    CAYENNE_METASTORE_CACHE_MB_PARAM,
    CAYENNE_METASTORE_MMAP_MB_PARAM,
    CAYENNE_METASTORE_BUSY_TIMEOUT_MS_PARAM,
    CAYENNE_METASTORE_WAL_AUTOCHECKPOINT_PAGES_PARAM,
    CAYENNE_METASTORE_WAL_TRUNCATE_THRESHOLD_MB_PARAM,
    CAYENNE_METASTORE_AUTO_VACUUM_PARAM,
    CAYENNE_METASTORE_INCREMENTAL_VACUUM_PAGES_PARAM,
    CAYENNE_GOAL_REPLICATION_LAG_PARAM,
    CAYENNE_GOAL_FRESHNESS_PARAM,
    CAYENNE_GOAL_QUERY_LATENCY_PARAM,
    CAYENNE_GOAL_QPH_PARAM,
];

/// Recognized `runtime.params` keys that don't belong to a larger prefix
/// family (the family lists live next to the code that consumes them:
/// `KNOWN_CAYENNE_RUNTIME_PARAMS`, `changes::CDC_RUNTIME_PARAMS`,
/// `http_rate_control::HTTP_RATE_CONTROL_RUNTIME_PARAMS`,
/// `cluster::CLUSTER_GRPC_RUNTIME_PARAMS`).
const MISC_RUNTIME_PARAMS: &[&str] = &[
    "url_tables",
    "geo",
    "parquet_page_index",
    "dedicated_thread_pool",
    "shuffle_location",
    "shuffle_format",
    "github_max_concurrent_connections",
];

/// The complete set of `runtime.params` keys the runtime recognizes, gathered
/// from every consuming subsystem's authoritative list. Used to validate the
/// `runtime.params` section at startup: any key not in this set is a typo or
/// unsupported option and gets a "did you mean" warning scoped to this
/// section's vocabulary. See spiceai/spiceai#10970.
///
/// When adding a new `runtime.params` key, extend the owning family's list
/// (or `MISC_RUNTIME_PARAMS`) so it is recognized here.
fn known_runtime_params() -> Vec<&'static str> {
    let mut known = Vec::with_capacity(
        KNOWN_CAYENNE_RUNTIME_PARAMS.len()
            + crate::accelerated::refresh_task::changes::CDC_RUNTIME_PARAMS.len()
            + dataconnector::http_rate_control::HTTP_RATE_CONTROL_RUNTIME_PARAMS.len()
            + crate::cluster::CLUSTER_GRPC_RUNTIME_PARAMS.len()
            + MISC_RUNTIME_PARAMS.len(),
    );
    known.extend_from_slice(KNOWN_CAYENNE_RUNTIME_PARAMS);
    known.extend_from_slice(crate::accelerated::refresh_task::changes::CDC_RUNTIME_PARAMS);
    known.extend_from_slice(dataconnector::http_rate_control::HTTP_RATE_CONTROL_RUNTIME_PARAMS);
    known.extend_from_slice(crate::cluster::CLUSTER_GRPC_RUNTIME_PARAMS);
    known.extend_from_slice(MISC_RUNTIME_PARAMS);
    known
}

pub struct RuntimeBuilder {
    app: Option<Arc<app::App>>,
    autoload_extensions: HashMap<String, Box<dyn ExtensionFactory>>,
    extensions: Vec<Box<dyn ExtensionFactory>>,
    pods_watcher: Option<podswatcher::PodsWatcher>,
    datasets_health_monitor_enabled: bool,
    metrics_endpoint: Option<SocketAddr>,
    prometheus_registry: Option<prometheus::Registry>,
    metrics_reader: Option<MetricsReader>,
    runtime_status: Arc<status::RuntimeStatus>,
    rate_limits: Option<Arc<RateLimits>>,
    io_runtime: Option<Handle>,
    accelerator_engine_registry: Arc<AcceleratorEngineRegistry>,
    datafusion_configuration_fn: Option<DatafusionConfigurationCallback>,
    token_provider_registry: Arc<TokenProviderRegistry>,
    runtime_config: Arc<Config>,
    resolved_cluster_config: Option<ResolvedClusterConfig>,
    telemetry_config: Option<Arc<tokio::sync::SetOnce<TelemetryConfig>>>,
}

impl RuntimeBuilder {
    #[must_use]
    pub fn new() -> Self {
        RuntimeBuilder {
            app: None,
            extensions: vec![],
            pods_watcher: None,
            datasets_health_monitor_enabled: false,
            metrics_endpoint: None,
            prometheus_registry: None,
            metrics_reader: None,
            autoload_extensions: HashMap::new(),
            runtime_status: status::RuntimeStatus::new(),
            rate_limits: None,
            io_runtime: None,
            accelerator_engine_registry: Arc::new(AcceleratorEngineRegistry::new()),
            datafusion_configuration_fn: None,
            token_provider_registry: Arc::new(TokenProviderRegistry::new()),
            runtime_config: Arc::new(Config::default()),
            resolved_cluster_config: None,
            telemetry_config: None,
        }
    }

    #[must_use]
    pub fn with_app(mut self, app: app::App) -> Self {
        self.app = Some(Arc::new(app));
        self
    }

    #[must_use]
    pub fn with_app_opt(mut self, app: Option<Arc<app::App>>) -> Self {
        self.app = app;
        self
    }

    #[must_use]
    pub fn with_runtime_config(mut self, config: Config) -> Self {
        self.runtime_config = Arc::new(config);
        self
    }

    #[must_use]
    pub fn with_extensions(mut self, extensions: Vec<Box<dyn ExtensionFactory>>) -> Self {
        self.extensions = extensions;
        self
    }

    /// Extensions that will be automatically loaded if a component requests them and the user hasn't explicitly loaded it.
    #[must_use]
    pub fn with_autoload_extensions(
        mut self,
        extensions: HashMap<String, Box<dyn ExtensionFactory>>,
    ) -> Self {
        self.autoload_extensions = extensions;
        self
    }

    #[must_use]
    pub fn with_pods_watcher(mut self, pods_watcher: podswatcher::PodsWatcher) -> Self {
        self.pods_watcher = Some(pods_watcher);
        self
    }

    #[must_use]
    pub fn with_datasets_health_monitor(mut self) -> Self {
        self.datasets_health_monitor_enabled = true;
        self
    }

    #[must_use]
    pub fn with_metrics_server(
        mut self,
        metrics_endpoint: SocketAddr,
        prometheus_registry: prometheus::Registry,
    ) -> Self {
        self.metrics_endpoint = Some(metrics_endpoint);
        self.prometheus_registry = Some(prometheus_registry);
        self
    }

    #[must_use]
    pub fn with_metrics_server_opt(
        mut self,
        metrics_endpoint: Option<SocketAddr>,
        prometheus_registry: Option<prometheus::Registry>,
    ) -> Self {
        self.metrics_endpoint = metrics_endpoint;
        self.prometheus_registry = prometheus_registry;
        self
    }

    #[must_use]
    pub fn with_rate_limits(mut self, rate_limits: RateLimits) -> Self {
        self.rate_limits = Some(Arc::new(rate_limits));
        self
    }

    #[must_use]
    pub fn with_io_runtime(mut self, io_runtime: Handle) -> Self {
        self.io_runtime = Some(io_runtime);
        self
    }

    #[must_use]
    pub fn with_resolved_cluster_config(
        mut self,
        resolved_cluster_config: ResolvedClusterConfig,
    ) -> Self {
        self.resolved_cluster_config = Some(resolved_cluster_config);
        self
    }

    /// Sets a `SetOnce` handle that will be resolved with the spicepod
    /// `TelemetryConfig` once it is available.  For executors, this is set
    /// after the app definition is fetched from the scheduler.
    #[must_use]
    pub fn with_telemetry_config(
        mut self,
        telemetry_config: Arc<tokio::sync::SetOnce<TelemetryConfig>>,
    ) -> Self {
        self.telemetry_config = Some(telemetry_config);
        self
    }

    /// Sets the metrics reader for on-demand OTLP metrics collection in cluster mode.
    ///
    /// This reader is used by:
    /// - `GetMetrics` RPC to return local metrics to peer schedulers
    /// - Executors responding to metrics requests from schedulers via control stream
    #[must_use]
    pub fn with_metrics_reader(mut self, metrics_reader: MetricsReader) -> Self {
        self.metrics_reader = Some(metrics_reader);
        self
    }

    pub async fn build(self) -> Runtime {
        // Initialize DataFusion tracer for span context propagation across async boundaries
        if let Err(e) = telemetry::tracers::init_datafusion_tracer() {
            tracing::warn!(
                "Failed to initialize DataFusion tracer: {e}. Span context may not propagate correctly across async boundaries."
            );
        }

        // Cayenne compaction shutdown state is process-global. Reset it when a
        // fresh Runtime is built so embedded/test runtimes created after a prior
        // shutdown can start maintenance passes again, including when dedicated
        // thread pools are disabled and no compaction runtime handle is injected.
        cayenne::reset_compaction_shutdown();

        // Resolve the effective spicepod runtime config: config override > app > default.
        let spicepod_rt = self.runtime_config.runtime.clone().unwrap_or_else(|| {
            self.app
                .as_ref()
                .map_or(SpicepodRuntime::default(), |app| app.runtime.clone())
        });

        // Resolved before the engines are registered and handed to each constructor: an
        // engine is built by the registration slice, which knows nothing of its type, so
        // this is how a Spicepod setting reaches one. Registering a configured engine
        // afterwards instead would mean naming the concrete accelerator from here.
        let cayenne_footer_cache_mb =
            parse_usize_runtime_param(&spicepod_rt.params, CAYENNE_FOOTER_CACHE_MB_PARAM);
        let accelerator_configs = [data_accelerator_api::AcceleratorRuntimeConfig::Cayenne(
            data_accelerator_api::CayenneRuntimeConfig {
                footer_cache_mb: cayenne_footer_cache_mb,
            },
        )];

        self.accelerator_engine_registry
            .register_all(&accelerator_configs)
            .await;
        dataconnector::register_all().await;
        catalogconnector::register_all().await;
        document_parse::register_all().await;

        let query = spicepod_rt.query.clone().unwrap_or_default();

        let memory_limit = parse_memory_limit(query.memory_limit.clone());
        let target_partitions = query.target_partitions;
        let max_concurrent_queries = query.max_concurrent_queries;

        // The effective timeout is resolved per request by
        // `RequestContextBuilder::build` from the app's `runtime.query.timeout`;
        // validate here so a misconfigured value is warned about once at startup
        if let Err(e) = query.timeout() {
            tracing::warn!("{e} No query timeout will be applied.");
        }

        let metrics = spicepod_rt.metrics.clone();

        let dataset_parallelism = spicepod_rt.dataset_load_parallelism;

        let task_history = spicepod_rt.task_history.enabled;

        let runtime_ready_state = spicepod_rt.ready_state;

        self.runtime_status
            .set_ready_state(match runtime_ready_state {
                SpicepodRuntimeReadyState::OnLoad => status::RuntimeReadyState::OnLoad,
                SpicepodRuntimeReadyState::OnRegistration => {
                    status::RuntimeReadyState::OnRegistration
                }
            });

        // URL tables are opt-in via `runtime.params.url_tables=enabled`
        let url_tables_enabled =
            spicepod_rt.params.get("url_tables").map(String::as_str) == Some("enabled");
        warn_on_unknown_runtime_params(&spicepod_rt.params);
        let cayenne_sort_merge_min_rows =
            parse_usize_runtime_param(&spicepod_rt.params, CAYENNE_SORT_MERGE_MIN_ROWS_PARAM);
        log_applied_cayenne_param(
            CAYENNE_SORT_MERGE_MIN_ROWS_PARAM,
            cayenne_sort_merge_min_rows,
        );
        let cayenne_sort_merge_memory_pool_fraction = parse_f64_runtime_param(
            &spicepod_rt.params,
            CAYENNE_SORT_MERGE_MEMORY_POOL_FRACTION_PARAM,
        );
        log_applied_cayenne_param(
            CAYENNE_SORT_MERGE_MEMORY_POOL_FRACTION_PARAM,
            cayenne_sort_merge_memory_pool_fraction,
        );
        log_applied_cayenne_param(CAYENNE_FOOTER_CACHE_MB_PARAM, cayenne_footer_cache_mb);
        let cayenne_segment_cache_mb =
            parse_usize_runtime_param(&spicepod_rt.params, CAYENNE_SEGMENT_CACHE_MB_PARAM);
        log_applied_cayenne_param(CAYENNE_SEGMENT_CACHE_MB_PARAM, cayenne_segment_cache_mb);
        install_segment_cache(cayenne_segment_cache_mb);
        let cayenne_filter_propagation = parse_cayenne_filter_propagation(&spicepod_rt.params);

        // Process-global SQLite metastore pragma tuning (cache, mmap, busy
        // timeout, WAL autocheckpoint, auto_vacuum). Applied once at startup;
        // connections opened afterward pick up the values. Unset params keep the
        // defaults on `SqliteMetastoreConfig`.
        {
            let mut metastore_cfg = cayenne::SqliteMetastoreConfig::default();
            if let Some(v) =
                parse_usize_runtime_param(&spicepod_rt.params, CAYENNE_METASTORE_CACHE_MB_PARAM)
            {
                metastore_cfg.cache_size_mb = v;
            }
            if let Some(v) =
                parse_usize_runtime_param(&spicepod_rt.params, CAYENNE_METASTORE_MMAP_MB_PARAM)
            {
                metastore_cfg.mmap_size_bytes = i64::try_from(v.saturating_mul(1024 * 1024))
                    .unwrap_or(metastore_cfg.mmap_size_bytes);
            }
            if let Some(v) = parse_usize_runtime_param(
                &spicepod_rt.params,
                CAYENNE_METASTORE_BUSY_TIMEOUT_MS_PARAM,
            ) {
                metastore_cfg.busy_timeout_ms =
                    u64::try_from(v).unwrap_or(metastore_cfg.busy_timeout_ms);
            }
            if let Some(v) = parse_usize_runtime_param(
                &spicepod_rt.params,
                CAYENNE_METASTORE_WAL_AUTOCHECKPOINT_PAGES_PARAM,
            ) {
                metastore_cfg.wal_autocheckpoint_pages =
                    u32::try_from(v).unwrap_or(metastore_cfg.wal_autocheckpoint_pages);
            }
            if let Some(v) = parse_usize_runtime_param(
                &spicepod_rt.params,
                CAYENNE_METASTORE_WAL_TRUNCATE_THRESHOLD_MB_PARAM,
            ) {
                metastore_cfg.wal_truncate_threshold_bytes =
                    u64::try_from(v.saturating_mul(1024 * 1024))
                        .unwrap_or(metastore_cfg.wal_truncate_threshold_bytes);
            }
            if let Some(av) = spicepod_rt.params.get(CAYENNE_METASTORE_AUTO_VACUUM_PARAM) {
                metastore_cfg.auto_vacuum = match av.to_lowercase().as_str() {
                    "none" => cayenne::SqliteAutoVacuum::None,
                    "incremental" => cayenne::SqliteAutoVacuum::Incremental,
                    "full" => cayenne::SqliteAutoVacuum::Full,
                    other => {
                        tracing::warn!(
                            "Invalid cayenne_metastore_auto_vacuum value `{other}`; expected none|incremental|full, using none."
                        );
                        cayenne::SqliteAutoVacuum::None
                    }
                };
            }
            if let Some(v) = parse_usize_runtime_param(
                &spicepod_rt.params,
                CAYENNE_METASTORE_INCREMENTAL_VACUUM_PAGES_PARAM,
            ) {
                metastore_cfg.incremental_vacuum_pages =
                    u32::try_from(v).unwrap_or(metastore_cfg.incremental_vacuum_pages);
            }
            cayenne::set_sqlite_metastore_config(metastore_cfg);
        }

        let cayenne_filter_propagation_enabled =
            cayenne_filter_propagation.is_some_and(CayenneFilterPropagation::is_enabled);
        // Only log "applied" for a value the user validly set; an unset or
        // invalid value yields `None` (and an invalid value already warned).
        log_applied_cayenne_param(
            CAYENNE_FILTER_PROPAGATION_PARAM,
            cayenne_filter_propagation.map(|propagation| {
                if propagation.is_enabled() {
                    "enabled"
                } else {
                    "disabled"
                }
            }),
        );
        let cayenne_optimizer_rules =
            parse_cayenne_optimizer_rules(&spicepod_rt.params, cayenne_filter_propagation_enabled);

        let CayenneMemoryBudgetPlan {
            cayenne_workload,
            compaction_memory_fraction,
            dedicated_thread_pools_enabled,
        } = plan_cayenne_memory_budgets(self.app.as_ref(), &spicepod_rt.params);

        // Estimate the off-pool per-table Cayenne cache reservation, summed over
        // every enabled Cayenne table. The DataFusion builder reduces the
        // query-memory default by it — by the excess over the host/10 headroom the
        // CDC base already reserves, or in full on the standard base, which reserves
        // no such slice — so the query pool + the in-memory tier + the per-table
        // caches stay within host RAM as the table count grows.
        let cayenne_reservation_bytes =
            estimate_cayenne_reservation_bytes(self.app.as_ref(), &spicepod_rt.params);

        // ---- Coordinated cgroup-aware memory budget for DuckDB accelerators ----
        // The DataFusion query pool defaults to 90% of RAM and EACH distinct DuckDB
        // instance defaults to DuckDB's own ~80%-of-RAM `memory_limit`; stacked they
        // over-commit host memory (N datasets on N separate DuckDB files ⇒ N×80%).
        // Compute a cgroup-aware split that fits, publish the per-instance cap for
        // the DuckDB accelerator to apply, and warn with what was applied /
        // recommended. An explicit `runtime.query.memory_limit` / per-dataset
        // `duckdb_memory_limit` always overrides. See `runtime_acceleration::memory_budget`.
        let duckdb_budget_inputs = duckdb_budget_inputs(self.app.as_ref());
        let has_duckdb_instances = duckdb_budget_inputs.num_unset_instances > 0
            || duckdb_budget_inputs.num_explicit_instances > 0;
        let duckdb_query_pool_cap = if has_duckdb_instances {
            // Only a CDC pod pays the reduced query-pool default (it leaves room for
            // the in-memory tier); a bulk-only Cayenne pod keeps the standard default
            // minus its measured cache reservation. Same expression the DataFusion
            // builder applies, so the projected base matches the pool it will build.
            let cayenne_cdc_active =
                dedicated_thread_pools_enabled && cayenne_workload.uses_cdc_tier();
            let total_memory = crate::resource_monitor::get_total_memory();
            // DuckDB's own default memory_limit is ~80% of HOST RAM (not the cgroup
            // limit), so project the un-coordinated ceiling from host memory —
            // otherwise a container (host RAM > cgroup) would under-estimate it and
            // skip coordination exactly where the OOM risk is highest.
            let duckdb_default_per_instance =
                runtime_acceleration::memory_budget::duckdb_default_per_instance_bytes(
                    crate::resource_monitor::get_host_memory(),
                );
            let base_query_budget = crate::datafusion::builder::effective_query_memory_limit(
                None,
                cayenne_cdc_active,
                cayenne_reservation_bytes,
                None,
            );
            let plan = runtime_acceleration::memory_budget::plan(
                total_memory,
                duckdb_default_per_instance,
                base_query_budget,
                memory_limit,
                &duckdb_budget_inputs,
            );
            runtime_acceleration::memory_budget::publish_duckdb_budget(
                plan.per_instance_cap_bytes,
                plan.duckdb_reservation_bytes,
            );
            emit_duckdb_memory_budget_warning(
                &plan,
                total_memory,
                duckdb_default_per_instance,
                &duckdb_budget_inputs,
            );
            plan.query_pool_cap_bytes
        } else {
            // No DuckDB accelerators (or the duckdb feature isn't compiled in): skip
            // the cgroup/host memory probes and the planner entirely — the plan would
            // NoOp anyway. Clear any previously-published budget so a hot-reload that
            // removed all DuckDB accelerators doesn't leave a stale reservation.
            runtime_acceleration::memory_budget::publish_duckdb_budget(0, 0);
            None
        };

        let caching = Runtime::init_caching(Some(&spicepod_rt.caching));
        let io_runtime = self.io_runtime.clone().unwrap_or_else(|| Handle::current());

        // Resolve CDC tunables once at startup so the per-envelope hot path
        // doesn't pay map lookup or spicepod-traversal cost. Reads
        // `cdc_*` knobs from `runtime.params`; missing or rejected values
        // fall back to the matching `SPICE_CDC_*` env var, then to defaults,
        // with a warning for rejected explicit values.
        crate::accelerated::refresh_task::changes::set_cdc_config(
            crate::accelerated::refresh_task::changes::cdc_config_from_params(&spicepod_rt.params),
        );

        // Create resource monitor early so it can be passed to DataFusion
        let resource_monitor = crate::resource_monitor::ResourceMonitor::new();
        let loaded_secrets = Self::load_secrets(self.app.as_ref()).await;

        // The secret preflight runs at the start of `Runtime::load_components`,
        // not here: stores the runtime owns rather than the spicepod — the
        // Cloud Connect delivered-secrets store, restored from its local cache
        // — are registered on the built runtime, between this point and the
        // load that resolves the references.
        let secrets = Arc::new(RwLock::new(loaded_secrets));

        // Read here because `self.resolved_cluster_config` is moved into the
        // DataFusion builder below, and the executor's cluster status is
        // registered once the runtime is constructed.
        let is_cluster_executor = matches!(
            self.resolved_cluster_config
                .as_ref()
                .and_then(ResolvedClusterConfig::effective_role),
            Some(ClusterRole::Executor)
        );

        // Create the shared app reference early so DataFusion, Runtime, and PartitionService share it.
        let shared_app: Arc<RwLock<Option<Arc<App>>>> = Arc::new(RwLock::new(self.app));

        let http_rate_control_registry = build_http_rate_control_registry(
            spicepod_rt.source_rate_control.as_ref(),
            Arc::clone(&secrets),
            io_runtime.clone(),
        )
        .await;

        let scheduler_node_id: Option<Arc<str>> = self
            .resolved_cluster_config
            .as_ref()
            .map(|cfg| Arc::<str>::from(cfg.metrics_node_id()));

        let distributed: Option<DistributedNode> = match self
            .resolved_cluster_config
            .as_ref()
            .and_then(ResolvedClusterConfig::effective_role)
        {
            Some(ClusterRole::Scheduler) => {
                // For a real object store, cluster_state.bootstrap() is called by start_scheduler_registry.
                if let Some(scheduler_config) = shared_app
                    .read()
                    .await
                    .as_ref()
                    .and_then(|app| app.runtime.scheduler.clone())
                {
                    match crate::cluster::scheduler_registry::build_object_store_internal(
                        Arc::clone(&secrets),
                        io_runtime.clone(),
                        &scheduler_config.state_location,
                        &scheduler_config,
                    )
                    .await
                    {
                        Ok((store, base_prefix)) => {
                            let cluster_state =
                                Arc::new(ClusterStateStore::new(Arc::clone(&store), &base_prefix));
                            let heartbeats = Arc::new(SchedulerHeartbeatStore::new(
                                Arc::clone(&store),
                                &base_prefix,
                            ));
                            let accelerations_partitions_store =
                                Arc::new(PartitionStore::accelerations(Arc::clone(&cluster_state)));
                            let catalog_partitions_store =
                                Arc::new(PartitionStore::catalog(Arc::clone(&cluster_state)));
                            let executor_registry = Arc::new(ExecutorRegistry::with_node_id(
                                Arc::clone(&accelerations_partitions_store),
                                Arc::clone(&catalog_partitions_store),
                                scheduler_node_id.clone(),
                            ));
                            let partition_service = Arc::new(PartitionService::new(
                                Arc::clone(&accelerations_partitions_store),
                                Arc::clone(&executor_registry),
                                Arc::clone(&shared_app),
                            ));
                            Some(DistributedNode::Scheduler {
                                peers: Arc::new(RwLock::new(HashMap::new())),
                                job_executor: Arc::new(RwLock::new(None)),
                                executor_registry,
                                cluster_state,
                                heartbeats,
                                accelerations_partitions_store,
                                catalog_partitions_store,
                                partition_service,
                                partition_load_tracker: Arc::new(
                                    runtime_cluster::PartitionLoadTracker::new(),
                                ),
                            })
                        }
                        Err(e) => {
                            tracing::error!(
                                "Failed to initialize cluster state store for scheduler: {e}"
                            );
                            None
                        }
                    }
                } else {
                    tracing::warn!(
                        "'--role scheduler' was specified but no `runtime.scheduler` field was found in spicepod.yaml. Using in-memory cluster state."
                    );
                    let store: Arc<dyn object_store::ObjectStore> =
                        Arc::new(object_store::memory::InMemory::new());
                    let cluster_state = Arc::new(ClusterStateStore::new(Arc::clone(&store), ""));
                    if let Err(err) = cluster_state.bootstrap().await {
                        tracing::warn!(
                            "Failed to bootstrap in-memory cluster state document; will retry: {err}"
                        );
                    }
                    let heartbeats = Arc::new(SchedulerHeartbeatStore::new(Arc::clone(&store), ""));
                    let accelerations_partitions_store =
                        Arc::new(PartitionStore::accelerations(Arc::clone(&cluster_state)));
                    let catalog_partitions_store =
                        Arc::new(PartitionStore::catalog(Arc::clone(&cluster_state)));
                    let executor_registry = Arc::new(ExecutorRegistry::with_node_id(
                        Arc::clone(&accelerations_partitions_store),
                        Arc::clone(&catalog_partitions_store),
                        scheduler_node_id.clone(),
                    ));
                    let partition_service = Arc::new(PartitionService::new(
                        Arc::clone(&accelerations_partitions_store),
                        Arc::clone(&executor_registry),
                        Arc::clone(&shared_app),
                    ));
                    Some(DistributedNode::Scheduler {
                        peers: Arc::new(RwLock::new(HashMap::new())),
                        job_executor: Arc::new(RwLock::new(None)),
                        executor_registry,
                        cluster_state,
                        heartbeats,
                        accelerations_partitions_store,
                        catalog_partitions_store,
                        partition_service,
                        partition_load_tracker: Arc::new(
                            runtime_cluster::PartitionLoadTracker::new(),
                        ),
                    })
                }
            }
            Some(ClusterRole::Executor) => Some(DistributedNode::Executor {
                partition_assignments: Arc::new(RwLock::new(HashMap::new())),
                outbound_broadcaster: crate::cluster::ExecutorOutboundBroadcaster::default(),
            }),
            None => None, // No cluster config means we're running in standalone mode
        };

        let mut df_builder = DataFusion::builder(
            Arc::clone(&self.runtime_status),
            Arc::clone(&self.accelerator_engine_registry),
            io_runtime.clone(),
        )
        .memory_limit(memory_limit)
        .target_partitions(target_partitions)
        .max_concurrent_queries(max_concurrent_queries)
        .prefer_hash_join(query.prefer_hash_join)
        .eager_aggregation(query.eager_aggregation)
        .eager_aggregation_min_reduction_factor(query.eager_aggregation_min_reduction_factor)
        .eager_aggregation_max_pushed_groups(query.eager_aggregation_max_pushed_groups)
        .temp_directory(query.temp_directory)
        .spill_compression(query.spill_compression)
        .with_task_history(task_history)
        .with_caching(caching)
        .with_metrics(metrics)
        .with_resource_monitor(resource_monitor.clone())
        .with_url_tables(url_tables_enabled)
        .cayenne_sort_merge_min_rows(cayenne_sort_merge_min_rows)
        .cayenne_sort_merge_memory_pool_fraction(cayenne_sort_merge_memory_pool_fraction)
        .cayenne_footer_cache_mb(cayenne_footer_cache_mb)
        .compaction_memory_fraction(compaction_memory_fraction)
        .cayenne_workload(cayenne_workload)
        .dedicated_thread_pools_enabled(dedicated_thread_pools_enabled)
        .cayenne_reservation_bytes(cayenne_reservation_bytes)
        .duckdb_query_pool_cap(duckdb_query_pool_cap)
        .cayenne_optimizer_rules(cayenne_optimizer_rules);

        if let Some(DistributedNode::Scheduler {
            executor_registry,
            partition_service,
            partition_load_tracker,
            ..
        }) = distributed.as_ref()
        {
            df_builder = df_builder
                .with_executor_registry(Arc::clone(executor_registry))
                .with_partition_service(Arc::clone(partition_service))
                .with_partition_load_tracker(Arc::clone(partition_load_tracker));
        }

        if let Some(resolved_cluster_config) = self.resolved_cluster_config {
            df_builder = df_builder.with_cluster_config(resolved_cluster_config);
        }

        if let Some(dataset_parallelism) = dataset_parallelism {
            df_builder = df_builder.max_parallel_accelerated_refreshes(dataset_parallelism);
        }

        let mut df = df_builder.build();

        if let Some(callback) = self.datafusion_configuration_fn {
            callback(&mut df);
        }

        let df = Arc::new(df);
        df.set_self_ref();

        let datasets_health_monitor = if self.datasets_health_monitor_enabled {
            let is_task_history_enabled = spicepod_rt.task_history.enabled;
            let datasets_health_monitor = DatasetsHealthMonitor::new(Arc::clone(&df))
                .with_task_history_enabled(is_task_history_enabled);
            datasets_health_monitor.start();
            Some(Arc::new(datasets_health_monitor))
        } else {
            None
        };

        let mut rt = Runtime {
            app: shared_app,
            apply_app_lock: Arc::new(tokio::sync::Mutex::new(())),
            initial_load: Arc::new(crate::InitialLoad::default()),
            df,
            llm_runtime_stores: Arc::new(crate::model::LlmRuntimeStores::default()),
            http_rate_control_registry,
            workers: Arc::new(RwLock::new(HashMap::new())),
            embeds: Arc::new(RwLock::new(HashMap::new())),
            rerankers: Arc::new(RwLock::new(HashMap::new())),
            tools: Arc::new(RwLock::new(HashMap::new())),
            tool_factories: Arc::new(Mutex::new(HashMap::new())),
            pods_watcher: Arc::new(RwLock::new(self.pods_watcher)),
            secrets,
            spaced_tracer: Arc::new(util::tracers::SpacedTracer::new(Duration::from_secs(15))),
            autoload_extensions: Arc::new(self.autoload_extensions),
            extensions: Arc::new(RwLock::new(HashMap::new())),
            datasets_health_monitor,
            metrics_endpoint: self.metrics_endpoint,
            prometheus_registry: self.prometheus_registry,
            metrics_reader: self.metrics_reader,
            rate_limits: self.rate_limits.unwrap_or_default(),
            io_runtime,
            status: self.runtime_status,
            tasks: Arc::new(RwLock::new(HashMap::new())),
            accelerator_engine_registry: self.accelerator_engine_registry,
            token_provider_registry: self.token_provider_registry,
            schedulers: Arc::new(RwLock::new(HashMap::new())),
            distributed,
            resource_monitor,
            config: Arc::clone(&self.runtime_config),
            dataset_load_semaphore: Arc::new(tokio::sync::Semaphore::new(
                dataset_parallelism.unwrap_or(tokio::sync::Semaphore::MAX_PERMITS),
            )),
            telemetry_config: self.telemetry_config,
        };

        // Executors: register cluster status before any concurrent
        // `load_components` / `start_servers` race so readiness cannot pass on
        // dataset-only status while task slots are still closed (#11758 Fix B).
        if is_cluster_executor {
            rt.status
                .update_cluster("executor", status::ComponentStatus::Initializing);
        }

        let mut extensions: HashMap<String, Arc<dyn Extension>> = HashMap::new();
        for factory in self.extensions {
            let mut extension = factory.create();
            let extension_name = extension.name();
            if let Err(err) = extension.initialize(&rt).await {
                tracing::error!(
                    "Failed to initialize extension '{extension_name}', so the features it \
                     provides are unavailable: {err} See: https://spiceai.org/docs"
                );
            } else {
                extensions.insert(extension_name.into(), extension.into());
            }
        }
        rt.extensions = Arc::new(RwLock::new(extensions));

        register_udfs(&rt).await;

        rt
    }

    async fn load_secrets(app: Option<&Arc<App>>) -> Secrets {
        let _guard = TimeMeasurement::new(&metrics::secrets::STORES_LOAD_DURATION_MS, &[]);
        let mut secrets = secrets::Secrets::new();

        if let Some(app) = app
            && let Err(e) = secrets.load_from(&app.secrets).await
        {
            tracing::error!(
                "Failed to load the secret stores declared in the spicepod, so components \
                 resolving a secret reference will not start: {e} \
                 See: https://spiceai.org/docs/components/secret-stores"
            );
        }

        secrets
    }
}

impl Default for RuntimeBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(not(feature = "rate-control"))]
// This build has no persisted rate-control backend, so this stub never awaits.
// It must stay `async` to match the `rate-control` variant's signature: the sole
// caller awaits the result unconditionally. The suppression is inherently
// feature-conditional — it exists only in this `cfg(not(rate-control))` variant,
// exactly the build where the lint fires; under `rate-control` this whole fn is
// compiled out and the real variant awaits.
#[expect(
    clippy::unused_async,
    reason = "signature parity with the rate-control variant; caller awaits unconditionally"
)]
async fn build_http_rate_control_registry(
    source_rate_control: Option<&SpicepodSourceRateControl>,
    secrets: Arc<RwLock<Secrets>>,
    io_runtime: Handle,
) -> Arc<dataconnector::http_rate_control::HttpRateControlRegistry> {
    let _ = (&secrets, &io_runtime);
    if source_rate_control
        .and_then(|config| config.state_location.as_ref())
        .is_some()
    {
        tracing::warn!(
            "Persisted HTTP governor rate-control state requires a Spice.ai Enterprise build. Falling back to in-memory HTTP rate-control state."
        );
    }
    Arc::new(dataconnector::http_rate_control::HttpRateControlRegistry::default())
}

#[cfg(feature = "rate-control")]
async fn build_http_rate_control_registry(
    source_rate_control: Option<&SpicepodSourceRateControl>,
    secrets: Arc<RwLock<Secrets>>,
    io_runtime: Handle,
) -> Arc<dataconnector::http_rate_control::HttpRateControlRegistry> {
    let Some((state_location, params, refresh_interval, config_path)) = source_rate_control
        .and_then(|config| {
            config.state_location.as_deref().map(|state_location| {
                (
                    state_location,
                    config.params.as_ref(),
                    config.refresh_interval.as_str(),
                    "runtime.source_rate_control",
                )
            })
        })
    else {
        return Arc::new(dataconnector::http_rate_control::HttpRateControlRegistry::default());
    };

    let Some(refresh_interval) = parse_rate_control_refresh_interval(refresh_interval, config_path)
    else {
        return Arc::new(dataconnector::http_rate_control::HttpRateControlRegistry::default());
    };

    match crate::object_store_state::build_object_store(
        secrets,
        io_runtime,
        state_location,
        params,
        "rate-control state",
    )
    .await
    {
        Ok((store, base_prefix)) => {
            tracing::info!(
                "Initialized persisted HTTP governor rate-control state with location: {}",
                state_location
            );
            let registry = Arc::new(dataconnector::http_rate_control::HttpRateControlRegistry::with_persisted_governor_state(
                store,
                base_prefix,
                refresh_interval,
            ));
            registry.start_persistence_task();
            registry
        }
        Err(error) => {
            tracing::error!(
                "Failed to initialize persisted HTTP governor rate-control state: {error}"
            );
            Arc::new(dataconnector::http_rate_control::HttpRateControlRegistry::default())
        }
    }
}

#[cfg(feature = "rate-control")]
fn parse_rate_control_refresh_interval(
    refresh_interval: &str,
    config_path: &str,
) -> Option<Duration> {
    match fundu::parse_duration(refresh_interval) {
        Ok(parsed_refresh_interval) if parsed_refresh_interval.is_zero() => {
            tracing::error!(
                "Invalid {config_path}.refresh_interval '{refresh_interval}': value must be greater than 0"
            );
            None
        }
        Ok(parsed_refresh_interval) => Some(parsed_refresh_interval),
        Err(error) => {
            tracing::error!("Invalid {config_path}.refresh_interval '{refresh_interval}': {error}");
            None
        }
    }
}

fn parse_memory_limit(memory_limit: Option<String>) -> Option<u64> {
    let memory_limit = memory_limit?;
    let original_memory_limit = memory_limit.clone();

    #[expect(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let memory_limit = byte_unit::Byte::from_str(&memory_limit)
        .ok()
        // losing the fractional part of a byte is not a problem
        .map(|v| v.get_adjusted_unit(byte_unit::Unit::B).get_value() as u64);

    if memory_limit.is_none() {
        tracing::warn!(
            "An invalid Runtime memory limit was specified: {original_memory_limit} A memory limit must be specified as an integer in GB, MB, or KB size."
        );
    }

    if memory_limit == Some(0) {
        tracing::warn!(
            "A Runtime memory limit of 0 was specified: {original_memory_limit} A memory limit must be greater than 0."
        );
        None
    } else {
        memory_limit
    }
}

/// Emit an INFO log when a `cayenne_*` runtime tunable parsed successfully so
/// operators see at startup which override actually took effect. Only logs
/// when `value` is `Some` (the parser already emits a warning for malformed
/// values). See spiceai/spiceai#10970.
/// Total byte budget for the process-wide Vortex segment cache.
///
/// One cache serves every Cayenne table, so this is a whole-process budget:
/// adding a table divides this pool instead of reserving another cache of its own
/// (spiceai/spiceai#12937). Unset derives ~1/64 of the process's memory
/// entitlement, clamped to [256 MiB, 2 GiB] — a single-table deployment gets
/// somewhat more than the old per-table share, while a fleet of tables can no
/// longer multiply it. `0` disables segment caching.
///
/// The floor matches the old per-table floor on purpose, and is not a regression
/// against it: the old default floored at 256 MiB *per table*, so below 16 GiB —
/// where `RAM/64` is still under the floor — this whole-process budget is at most
/// what a single table used to take, and strictly less for any pod with two or
/// more. A deployment that set an explicit per-table value smaller than the floor
/// is the one case this does not cover, and no floor would: the fix there is to
/// set `runtime.params.cayenne_segment_cache_mb`.
///
/// Note `get_total_memory()` prefers the cgroup limit but falls back to host RAM,
/// so a pod with `requests.memory` and no `limits.memory` still derives from the
/// node's memory; that entitlement gap is tracked separately.
fn segment_cache_budget_bytes(configured_mb: Option<usize>) -> u64 {
    const MIB: u64 = 1024 * 1024;
    const FLOOR_BYTES: u64 = 256 * MIB;
    const CEIL_BYTES: u64 = 2048 * MIB;
    const HOST_FRACTION: u64 = 64;

    match configured_mb {
        Some(mb) => u64::try_from(mb).unwrap_or(u64::MAX).saturating_mul(MIB),
        // Derive in whole MiB: `get_total_memory()` is not page-aligned on every
        // host, so dividing the byte count would give a budget whose value
        // depends on the machine.
        None => (crate::resource_monitor::get_total_memory() / HOST_FRACTION / MIB)
            .clamp(FLOOR_BYTES / MIB, CEIL_BYTES / MIB)
            .saturating_mul(MIB),
    }
}

/// Install the process-wide Vortex segment cache, before any Cayenne table is
/// registered so every format shares one budget.
///
/// Always installs a decision, even for a pod with no Cayenne table today.
/// Datasets can be added after startup through `apply_app`/`apply_dataset_diff`,
/// and a format that finds no decision falls back to a cache of its own — which
/// would rebuild the per-table allocation this replaced, and would ignore a
/// configured `0`. What *is* gated on a Cayenne table is the memory reservation
/// (see `estimate_cayenne_reservation_bytes`): an empty Moka cache allocates
/// nothing until something inserts into it, so installing costs nothing, while
/// reserving against the query pool for a cache no table can read would shrink
/// every other query's budget for nothing.
fn install_segment_cache(configured_mb: Option<usize>) {
    // `runtime.params.cayenne_segment_cache_mb` is the only input. Per-table values
    // sized a per-table cache; there is no conversion from them to a shared budget
    // that is not invented, and a single dataset's setting must not decide the
    // budget every other table reads from. They are reported as ignored where they
    // are read, against the dataset that set them.
    let bytes = segment_cache_budget_bytes(configured_mb);

    if bytes == 0 {
        // Install the decision, not just the absence of one: a format must not
        // substitute a private cache for one the operator switched off.
        vortex_datafusion::install_process_segment_cache(0);
        return;
    }
    if vortex_datafusion::install_process_segment_cache(bytes) {
        tracing::info!(
            "Vortex segment cache installed: {} MB shared across all Cayenne tables",
            bytes / (1024 * 1024)
        );
    } else {
        // A second runtime in one process (tests, embedded hosts) keeps the cache
        // the first one installed; the budget is process-wide by construction.
        tracing::debug!(
            "Vortex segment cache already installed; keeping the existing budget and ignoring {} MB",
            bytes / (1024 * 1024)
        );
    }
}

fn log_applied_cayenne_param<V: std::fmt::Display>(key: &str, value: Option<V>) {
    if let Some(value) = value {
        tracing::info!("Cayenne runtime tunable applied: runtime.params.{key}={value}");
    }
}

/// Warn (with a "did you mean" suggestion when close) on any key in the
/// `runtime.params` section the runtime doesn't recognize, so typos like
/// `cayenne_footer_cach_mb` or `shuffle_locatin` don't silently leave the
/// runtime on defaults. Candidates are scoped to the `runtime.params`
/// section's full vocabulary ([`known_runtime_params`]) so suggestions only
/// ever point at another valid `runtime.params` key. See
/// spiceai/spiceai#10970.
fn warn_on_unknown_runtime_params(params: &HashMap<String, String>) {
    let known = known_runtime_params();
    for key in params.keys() {
        if known.contains(&key.as_str()) {
            continue;
        }
        if let Some(suggestion) = util::levenshtein::closest_match(key, &known) {
            tracing::warn!(
                "runtime.params.{key} is not a recognized runtime parameter; did you mean '{suggestion}'? Ignoring."
            );
        } else {
            tracing::warn!("runtime.params.{key} is not a recognized runtime parameter; ignoring.");
        }
    }
}

fn parse_usize_runtime_param(params: &HashMap<String, String>, key: &str) -> Option<usize> {
    let raw = params.get(key)?;
    if raw.eq_ignore_ascii_case("usize::MAX") || raw.eq_ignore_ascii_case("max") {
        return Some(usize::MAX);
    }

    match raw.parse::<usize>() {
        Ok(value) => Some(value),
        Err(e) => {
            tracing::warn!(
                "runtime.params.{key}={raw:?} is not a valid usize ({e}); using default"
            );
            None
        }
    }
}

/// What the Cayenne accelerations configured in a Spicepod will demand of the host,
/// aggregated over every enabled one. Decides how much memory the runtime reserves
/// outside the query pool and which dedicated thread pools it brings up.
///
/// Both flags are unions, so one CDC table in a pod of full-refresh tables still
/// gets the full CDC-shaped reservation.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CayenneWorkload {
    /// Any enabled Cayenne acceleration at all.
    configured: bool,
    /// Any table on a profile that can hold rows in the off-pool in-memory CDC
    /// tier. Gates the coordinated host-memory partition — the reduced query-pool
    /// default and the global mem-tier byte budget — which exists solely to leave
    /// room for that tier. A pod without one cannot fill it
    /// (`cdc_durability` is forced to `file` off the small-write profile), so
    /// fencing ~20% of host for it would shrink the query pool for nothing.
    ///
    /// Deliberately NOT narrowed to a file acceleration mode, unlike
    /// `needs_compaction`: a `mode: memory` table holds its whole dataset in that
    /// tier permanently, so it is the case that most needs the room reserved.
    uses_cdc_tier: bool,
    /// Any table that accumulates Vortex files for compaction to consolidate — a
    /// file acceleration mode on a profile that is not a whole-table replace (see
    /// `compacts_into_carved_pool`). Gates the dedicated compaction runtime and its
    /// carved memory pool.
    needs_compaction: bool,
}

impl CayenneWorkload {
    #[must_use]
    pub const fn is_configured(self) -> bool {
        self.configured
    }

    #[must_use]
    pub const fn uses_cdc_tier(self) -> bool {
        self.uses_cdc_tier
    }

    #[must_use]
    pub const fn needs_compaction(self) -> bool {
        self.needs_compaction
    }

    /// Whether bringing up the dedicated compaction runtime is worthwhile. True
    /// unless no configured Cayenne acceleration can produce files to compact — a
    /// pod with no Cayenne at all still gets one, because a table created later by
    /// DDL may compact and would otherwise fall back to the ambient runtime.
    #[must_use]
    pub const fn may_compact(self) -> bool {
        !self.configured || self.needs_compaction
    }
}

/// [`unset_refresh_mode_for_connector`] keyed by the raw Spicepod `from:` value rather
/// than a parsed connector name.
///
/// This is the pre-init stand-in for `DataConnector::resolve_refresh_mode`, which
/// cannot be called before connectors are constructed. It parses and delegates to the
/// single mapping table, so the pre-init builder and the post-init accelerator classify
/// a pod through exactly the same rules and cannot disagree about a dataset.
pub(crate) fn connector_unset_refresh_mode(from: &str) -> RefreshMode {
    // `DatasetSpec::source()` is the authoritative `from:` parse — it recognizes
    // `://`, `:` AND `/` as delimiters and maps the empty value to `sink`. Splitting
    // on `:` alone would read `debezium/topic` as the whole string and miss it.
    unset_refresh_mode_for_connector(spicepod_dataset_source(from))
}

/// The connector name in a Spicepod `from:` value, via the same normalization the
/// initialized `Dataset` uses (`runtime_component::DatasetSpec::source`).
fn spicepod_dataset_source(from: &str) -> &str {
    if from == "sink" || from.is_empty() {
        return "sink";
    }
    match runtime_component::find_first_delimiter(from) {
        Some((0, _)) => "",
        Some((pos, _)) => &from[..pos],
        None => "spice.ai",
    }
}

/// The refresh mode a dataset resolves to, accounting for the connector filling in
/// an unset one.
fn resolved_refresh_mode(
    from: &str,
    refresh_mode: Option<&spicepod::acceleration::RefreshMode>,
) -> RefreshMode {
    refresh_mode.map_or_else(
        || connector_unset_refresh_mode(from),
        |mode| RefreshMode::from(mode.clone()),
    )
}

/// Whether any enabled acceleration in `app` streams CDC changes, for ANY engine.
///
/// Gates the dedicated CDC-apply runtime: that pool exists to keep the
/// freshness-critical `refresh_mode: changes` apply loop off the low-priority
/// refresh runtime, so a pod with no changes-mode dataset pays `cores - 1` idle
/// worker threads for a loop that never runs. `DataFusion::cdc_apply_runtime()`
/// falls back to the refresh runtime (and then the CPU runtime), so skipping it is
/// a resource decision, not a behavioral one.
///
/// Datasets and catalogs. A view cannot stream changes — `ViewBuilder::try_from`
/// rejects every view refresh mode except `full` — but an accelerated catalog runs
/// the apply loop for every table it discovers, and `changes` is the only refresh
/// mode its schema admits, so a catalog-accelerated pod needs the runtime as much
/// as an equivalent set of datasets does.
#[must_use]
pub fn streams_cdc_changes(app: Option<&Arc<app::App>>) -> bool {
    app.is_some_and(|app| {
        app.datasets.iter().any(|dataset| {
            dataset.acceleration.as_ref().is_some_and(|accel| {
                accel.enabled
                    && resolved_refresh_mode(&dataset.from, accel.refresh_mode.as_ref())
                        == RefreshMode::Changes
            })
        }) || app.catalogs.iter().any(|catalog| {
            catalog.acceleration.as_ref().is_some_and(|accel| {
                matches!(
                    accel.refresh_mode,
                    spicepod::component::catalog::CatalogRefreshMode::Changes
                )
            })
        })
    })
}

/// Classify the Cayenne accelerations `app` configures (see [`CayenneWorkload`]).
///
/// Covers every component kind that creates a Cayenne table — `app.datasets`,
/// `app.views` and `app.catalogs` — because a kind the classification cannot see
/// runs a Cayenne tier the memory budget does not account for. A view carries its
/// own `acceleration` block and is initialized through the same
/// `DataAccelerator::init` path as a dataset
/// (`init::view::initialize_views_accelerators`, which resolves any engine in the
/// registry); a catalog carries `CatalogAcceleration`, which is converted into the
/// dataset acceleration its tables are configured with (see
/// [`cayenne_accelerations`]).
///
/// Each acceleration is classified by `DataAccelerator::spicepod_write_profile` — the
/// same mapping `accelerator_cayenne` uses to configure the table — so the
/// budget can never disagree with the tables it is budgeting for.
///
/// This enumerates component kinds by hand because it runs *before* initialization,
/// against the Spicepod — the pre-init counterpart of the `AccelerationSource`
/// trait that datasets and views both implement once components exist.
#[cfg(not(windows))]
fn cayenne_workload(app: Option<&Arc<app::App>>) -> CayenneWorkload {
    let Some(app) = app else {
        return CayenneWorkload::default();
    };
    cayenne_accelerations(app).fold(CayenneWorkload::default(), |workload, (accel, profile)| {
        CayenneWorkload {
            configured: true,
            uses_cdc_tier: workload.uses_cdc_tier || profile.uses_cdc_tier,
            needs_compaction: workload.needs_compaction
                || compacts_into_carved_pool(&accel, profile),
        }
    })
}

/// Cayenne is not compiled on Windows (`accelerator-cayenne` is a `cfg(not(windows))`
/// dependency), so no acceleration there can demand anything of the host.
#[cfg(windows)]
fn cayenne_workload(_app: Option<&Arc<app::App>>) -> CayenneWorkload {
    CayenneWorkload::default()
}

/// Whether an enabled Cayenne acceleration can compact into the carved compaction
/// memory pool. Both halves must hold:
///
/// - **A file acceleration mode.** `mode: memory` (the Spicepod default) makes the
///   in-memory tier the table's permanent store: `apply_memory_mode_overrides`
///   zeroes `compaction_background_interval_ms`, and the writer never takes the
///   durable path (`is_memory_resident_mode` in `mutation_writer`), so no Vortex
///   file is ever produced for compaction to consolidate.
/// - **A profile that accumulates files.** A whole-table replace discards what the
///   previous refresh wrote, leaving nothing to consolidate; every other profile
///   builds files up across writes.
#[cfg(not(windows))]
fn compacts_into_carved_pool(
    accel: &spicepod::acceleration::Acceleration,
    profile: data_accelerator_api::SpicepodWriteProfile,
) -> bool {
    use spicepod::acceleration::Mode;

    matches!(accel.mode, Mode::File | Mode::FileCreate | Mode::FileUpdate)
        && profile.needs_compaction
}

/// How many enabled Cayenne accelerations can compact into the carved pool, for the
/// operator log. [`CayenneWorkload::needs_compaction`] is exactly `count > 0`, so
/// the counted set is structurally the same one the gate keys off.
///
/// An accelerated catalog counts once, not once per table: how many tables it
/// discovers is unknowable before the connector connects. The count is a log line
/// about which configuration earned the carve, not a table census.
#[cfg(not(windows))]
fn count_compaction_eligible_accelerations(app: Option<&Arc<app::App>>) -> usize {
    app.map_or(0, |app| {
        cayenne_accelerations(app)
            .filter(|(accel, profile)| compacts_into_carved_pool(accel, *profile))
            .count()
    })
}

#[cfg(windows)]
fn count_compaction_eligible_accelerations(_app: Option<&Arc<app::App>>) -> usize {
    0
}

/// The Cayenne memory decisions the Runtime builder makes at startup.
struct CayenneMemoryBudgetPlan {
    /// What the pod's Cayenne accelerations demand of the host.
    cayenne_workload: CayenneWorkload,
    /// Fraction of the query memory limit to carve for compaction. `None` reserves
    /// no carve.
    compaction_memory_fraction: Option<f64>,
    /// Whether `runtime.params.dedicated_thread_pool` leaves the dedicated pools on.
    /// Gates both budgets, but along different axes, so the `DataFusion` builder
    /// needs it separately from the carve.
    dedicated_thread_pools_enabled: bool,
}

/// Classify what the pod's Cayenne accelerations demand of the host, and decide
/// whether to carve a dedicated compaction memory pool out of the query memory
/// limit.
///
/// The carve is a counter, not an allocation, but it is subtracted straight out of
/// the query memory limit, so reserving it for a deployment that cannot compact
/// into it costs queries real budget. Take it only when at least one enabled
/// Cayenne acceleration can compact AND dedicated thread pools are enabled — the
/// latter because the dedicated compaction runtime is what would draw on the carve.
///
/// Declining the carve leaves nothing worse off: compaction, where it runs at all,
/// accounts against the shared query pool, exactly as it does when Cayenne is
/// absent or dedicated thread pools are disabled.
fn plan_cayenne_memory_budgets(
    app: Option<&Arc<app::App>>,
    params: &HashMap<String, String>,
) -> CayenneMemoryBudgetPlan {
    let cayenne_workload = cayenne_workload(app);
    let dedicated_thread_pools_enabled = !matches!(
        params.get("dedicated_thread_pool").map(String::as_str),
        Some("disabled")
    );
    let compaction_memory_fraction =
        (cayenne_workload.needs_compaction() && dedicated_thread_pools_enabled).then(|| {
            let requested =
                parse_f64_runtime_param(params, CAYENNE_COMPACTION_MEMORY_FRACTION_PARAM)
                    .unwrap_or(DEFAULT_COMPACTION_MEMORY_FRACTION);
            clamp_cayenne_compaction_memory_fraction(requested)
        });

    // Report the decision with the eligible count, so an operator can audit the
    // reserved budget against the spicepod. Silent for a non-Cayenne deployment,
    // and for one that declined the carve only because dedicated thread pools are
    // off — that is reported where the pools themselves are.
    if cayenne_workload.is_configured() && dedicated_thread_pools_enabled {
        if compaction_memory_fraction.is_some() {
            let eligible_accelerations = count_compaction_eligible_accelerations(app);
            tracing::info!(
                eligible_accelerations,
                "Reserving the Cayenne compaction memory pool: {eligible_accelerations} acceleration(s) can compact into it."
            );
        } else {
            tracing::info!(
                "Cayenne compaction memory pool not reserved: no acceleration can compact into it (needs a file acceleration mode, and a refresh_mode other than full — a whole-table replace leaves nothing to consolidate). Compaction, where it runs, accounts against the query pool instead."
            );
        }
    }

    CayenneMemoryBudgetPlan {
        cayenne_workload,
        compaction_memory_fraction,
        dedicated_thread_pools_enabled,
    }
}

/// Whether the pod reads from a `from: cayenne` catalog.
///
/// Such a catalog reads an existing Cayenne store, so it declares no acceleration
/// at all — [`cayenne_accelerations`] converts a catalog's `CatalogAcceleration`
/// and drops the catalogs that have none, which is every catalog of this shape.
/// It is the one way a pod hosts Cayenne tables that no acceleration block names.
#[cfg(not(windows))]
fn reads_from_cayenne_catalog(app: &Arc<app::App>) -> bool {
    app.catalogs.iter().any(|catalog| {
        catalog
            .from
            .split(':')
            .next()
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case("cayenne"))
    })
}

/// Every enabled Cayenne acceleration in `app`, paired with its RESOLVED write
/// profile — the connector default is already applied for an unset `refresh_mode`
/// (see [`connector_unset_refresh_mode`]), so a consumer cannot read the fallback
/// as if it were the answer.
///
/// A view has no `from:` and `ViewBuilder::try_from` rejects every refresh mode
/// except `full`, so its unset default is the whole-table replace.
///
/// A catalog carries `CatalogAcceleration` rather than a dataset's `Acceleration`,
/// so it is CONVERTED into one — through
/// [`runtime_component::catalog::CatalogAcceleration::to_dataset_acceleration`],
/// the same conversion the catalog connector configures every table it accelerates
/// from — and then classified like any other. That is a config-shape conversion
/// only: it reads the Spicepod, discovers nothing, and yields one item per catalog
/// rather than one per table (see `count_compaction_eligible_accelerations` and
/// `estimate_cayenne_reservation_bytes` for what that means for each consumer).
/// Converting rather than classifying `CatalogAcceleration` in place is what keeps
/// the engine the single classifier; the eventual home for this is a
/// trait both acceleration types implement, the pre-init counterpart of
/// `AccelerationSource`.
///
/// The yielded acceleration is therefore owned for a catalog and borrowed for the
/// two component kinds that declare one directly.
#[cfg(not(windows))]
fn cayenne_accelerations(
    app: &Arc<app::App>,
) -> impl Iterator<
    Item = (
        std::borrow::Cow<'_, spicepod::acceleration::Acceleration>,
        data_accelerator_api::SpicepodWriteProfile,
    ),
> {
    use std::borrow::Cow;

    app.datasets
        .iter()
        .map(|dataset| {
            (
                dataset.acceleration.as_ref().map(Cow::Borrowed),
                connector_unset_refresh_mode(&dataset.from),
            )
        })
        .chain(app.views.iter().map(|view| {
            (
                view.acceleration.as_ref().map(Cow::Borrowed),
                RefreshMode::Full,
            )
        }))
        .chain(app.catalogs.iter().map(|catalog| {
            let converted = catalog.acceleration.clone().map(|acceleration| {
                Cow::Owned(
                    runtime_component::catalog::CatalogAcceleration::from(acceleration)
                        .to_dataset_acceleration(),
                )
            });
            // The expansion always names a `refresh_mode` (catalog acceleration
            // requires one), so this fallback is never consulted.
            (converted, RefreshMode::Changes)
        }))
        .filter_map(|(accel, unset)| accel.map(|accel| (accel, unset)))
        .filter(|(accel, _)| {
            accel.enabled
                && accel
                    .engine
                    .as_deref()
                    .is_some_and(|engine| engine.eq_ignore_ascii_case("cayenne"))
        })
        .filter_map(|(accel, unset)| {
            // The engine classifies; this only enumerates. Filtered rather than
            // defaulted: a build with no Cayenne engine linked declares no Cayenne
            // workload, which is exactly what the budget should see.
            cayenne_write_profile(&accel, unset).map(|profile| (accel, profile))
        })
}

/// The Cayenne engine's classification of `acceleration`, or `None` when this build links
/// no Cayenne engine.
///
/// Reached through the registration slice rather than by naming the engine crate, which
/// the runtime must not depend on.
#[cfg(not(windows))]
pub(crate) fn cayenne_write_profile(
    acceleration: &spicepod::acceleration::Acceleration,
    unset_refresh_mode: RefreshMode,
) -> Option<data_accelerator_api::SpicepodWriteProfile> {
    data_accelerator_api::DATA_ACCELERATOR_REGISTRATIONS
        .iter()
        .find(|registration| registration.engine == runtime_acceleration::Engine::Cayenne)
        .and_then(|registration| {
            registration
                .build_with_defaults()?
                .spicepod_write_profile(acceleration, unset_refresh_mode)
        })
}

/// Estimate the aggregate bytes that enabled Cayenne tables reserve OUTSIDE the
/// `DataFusion` query pool. Each uses the explicit per-table param (matching the
/// accelerator's key lists, incl. `cayenne_`-prefixed aliases) when set, else the
/// accelerator's auto-derived cap (mirroring
/// the Cayenne engine's `autotune::HardwareProfile` — keep the fractions in
/// sync).
///
/// Two tiers of consumer, because they scale differently:
///
/// * The **Vortex segment cache** is a SCAN-path cache, and there is exactly one for
///   the process however many tables read through it. It is counted ONCE, outside the
///   per-acceleration fold, at whatever capacity was installed — so adding a table
///   divides that pool rather than reserving another cache. Its presence in this
///   estimate is gated on the pod having something to read through it at all.
/// * The **PK keyset, CDC coalesce buffer, and inline memtable** are write-path
///   state that only a small-write (CDC-profile) table populates, so they are
///   counted only for those.
/// * The **inline-admission buffer plus the serialized entry it produces** are
///   counted for every profile that inlines small writes, which now includes the
///   whole-table replace. One of each per acceleration is exact for the overwrite
///   path: `CayenneContext` hands out a single inline-admission slot, and a
///   partitioned dataset's children share the parent's context
///   (`CayennePartitionCreator::new`), so N concurrent partition overwrites still
///   hold at most one buffer and one blob between them.
///
/// The globally coordinated in-memory tier and the virtual (non-resident) metastore
/// mmap are intentionally excluded: the tier is already capped at host/5 and the
/// mmap is page-cache-backed.
///
/// Datasets, views AND catalogs: a view registers a Cayenne table exactly as a
/// dataset does. `ViewBuilder::try_from` rejects every view refresh mode except
/// `full`, so a view never contributes the write-path tier, but it is classified
/// through the same predicate rather than assumed.
///
/// A catalog contributes ONE table's worth, which is a floor rather than an
/// estimate: it accelerates every table it discovers, and that count is unknowable
/// before the connector connects. Under-counting a catalog leaves the query pool
/// larger than the tables warrant, but counting it as zero — which is what excluding
/// catalogs did — reserves nothing at all for a pod built entirely on one.
#[cfg(not(windows))]
fn estimate_cayenne_reservation_bytes(
    app: Option<&Arc<app::App>>,
    runtime_params: &HashMap<String, String>,
) -> u64 {
    const MIB: u64 = 1024 * 1024;
    const GIB: u64 = 1024 * MIB;
    // Auto-derived per-table cap fractions (mirror of the accelerator's autotune).
    const KEYSET_CACHE_HOST_FRACTION: u64 = 32; // ~1/32 host, clamped [256 MiB, 8 GiB]
    const DEFAULT_COALESCE_BYTES: u64 = 128 * MIB;
    const DEFAULT_INLINE_BYTES: u64 = 8 * MIB;

    // Parse the first matching key as a trimmed u64 (params may carry whitespace,
    // matching the rest of the runtime/dataset param parsing).
    fn parse_u64(map: &HashMap<String, String>, keys: &[&str]) -> Option<u64> {
        keys.iter()
            .find_map(|k| map.get(*k))
            .and_then(|v| v.trim().parse::<u64>().ok())
    }

    let Some(app) = app else {
        return 0;
    };
    let total_memory = crate::resource_monitor::get_total_memory();
    // Global CDC coalesce-buffer size (default 128 MiB); a per-dataset
    // `cdc_max_coalesced_bytes` overlays it per table (see `cdc_config_overlay`).
    let global_coalesce_bytes =
        parse_u64(runtime_params, &["cdc_max_coalesced_bytes"]).unwrap_or(DEFAULT_COALESCE_BYTES);

    let mut total: u64 = 0;
    let mut has_cayenne_acceleration = false;
    for (accel, profile) in cayenne_accelerations(app) {
        has_cayenne_acceleration = true;
        let params = accel
            .params
            .as_ref()
            .map(spicepod::param::Params::as_string_map)
            .unwrap_or_default();
        // Inline-admission state: the bounded Arrow buffer a write is admitted
        // through, plus the Arrow IPC entry it serializes into. Byte-valued
        // params, so no MB conversion; the accelerator's key lists (with the
        // `cayenne_`-prefixed aliases) and its unset defaults are mirrored here.
        if profile.inlines_small_writes {
            let inline_entry =
                parse_u64(&params, &["cayenne_inline_max_bytes", "inline_max_bytes"]).unwrap_or(
                    u64::try_from(cayenne::metadata::DEFAULT_INLINE_MAX_BYTES).unwrap_or(u64::MAX),
                );
            let inline_buffer = parse_u64(
                &params,
                &["cayenne_inline_max_buffer_bytes", "inline_max_buffer_bytes"],
            )
            .unwrap_or(
                u64::try_from(cayenne::metadata::DEFAULT_INLINE_MAX_BUFFER_BYTES)
                    .unwrap_or(u64::MAX),
            );
            total = total
                .saturating_add(inline_entry)
                .saturating_add(inline_buffer);
        }

        if !profile.uses_cdc_tier {
            continue;
        }
        // Write-path state below: only a small-write (CDC-profile) table populates
        // the keyset, the coalesce buffer, or the inline memtable.
        //
        // MB-valued cache params -> bytes; else the accelerator's auto host-fraction cap.
        let keyset = parse_u64(
            &params,
            &["cayenne_pk_keyset_cache_mb", "pk_keyset_cache_mb"],
        )
        .map_or_else(
            || (total_memory / KEYSET_CACHE_HOST_FRACTION).clamp(256 * MIB, 8 * GIB),
            |mb| mb.saturating_mul(MIB),
        );
        // Inline memtable is byte-valued; match the accelerator's key list including
        // the `cayenne_`-prefixed aliases (see the `accelerator-cayenne` crate).
        let inline = parse_u64(
            &params,
            &[
                "cayenne_inline_flush_max_bytes",
                "inline_flush_max_bytes",
                "cayenne_inline_memtable_max_bytes",
                "inline_memtable_max_bytes",
            ],
        )
        .unwrap_or(DEFAULT_INLINE_BYTES);
        // Per-dataset coalesce override wins over the global (mirrors cdc_config_overlay).
        let coalesce =
            parse_u64(&params, &["cdc_max_coalesced_bytes"]).unwrap_or(global_coalesce_bytes);
        total = total
            .saturating_add(keyset)
            .saturating_add(coalesce)
            .saturating_add(inline);
    }

    // Scan-path cache: one process-wide cache shared by every table, so it is
    // counted once rather than per acceleration — and only when something exists
    // to use it, since reserving against the query pool for a cache no table can
    // read would shrink every other query's budget for nothing. Reading the
    // installed capacity rather than recomputing the budget also reports zero
    // when caching is switched off.
    //
    // Catalogs count as much as datasets here: a `from: cayenne` catalog builds
    // its tables through `CayenneTableProviderBuilder` and `CayenneContext`, which
    // read from the same shared cache. A catalog-only pod would otherwise keep the
    // full query pool while the cache filled beside it.
    if has_cayenne_acceleration || reads_from_cayenne_catalog(app) {
        total = total
            .saturating_add(vortex_datafusion::process_segment_cache_capacity_bytes().unwrap_or(0));
    }
    total
}

/// Cayenne is not compiled on Windows (`accelerator-cayenne` is a `cfg(not(windows))`
/// dependency), so nothing there holds an off-pool Cayenne cache.
#[cfg(windows)]
fn estimate_cayenne_reservation_bytes(
    _app: Option<&Arc<app::App>>,
    _runtime_params: &HashMap<String, String>,
) -> u64 {
    0
}

/// Deduped-by-instance summary of the `DuckDB` accelerators in `app`, for the
/// coordinated memory budget ([`runtime_acceleration::memory_budget::plan`]).
///
/// Groups accelerations by `DuckDB` instance identity — one per distinct resolved
/// file path, plus a single shared key for all memory-mode accelerations (mirroring
/// the fork's `DbInstanceKey`) — and classifies each instance as explicit (something
/// on it sets `duckdb_memory_limit`, taking the max) or un-limited. Imperfect path
/// canonicalization only ever OVER-counts instances, yielding smaller, safer caps.
///
/// Covers both `app.datasets` and `app.views`: a view carries its own `acceleration`
/// block and creates a `DuckDB` instance exactly as a dataset does, so an instance
/// the budget cannot see is an instance left at `DuckDB`'s own ~80%-of-RAM default —
/// the over-commit this budget exists to prevent. Catalogs are excluded because they
/// cannot create a `DuckDB` instance at all: `CatalogAcceleration`'s engine enum
/// admits only Cayenne. That is a statement about this engine, not about whether a
/// catalog demands memory — the Cayenne classification counts catalogs (see
/// `cayenne_accelerations`).
///
/// This enumerates component kinds by hand because it runs *before* initialization,
/// against the Spicepod. It is the pre-init mirror of the `AccelerationSource` trait,
/// which datasets and views both implement and which is therefore already
/// kind-agnostic once components exist — the authority after init, but unavailable
/// to the builder here.
#[cfg(feature = "duckdb")]
fn duckdb_budget_inputs(
    app: Option<&Arc<app::App>>,
) -> runtime_acceleration::memory_budget::DuckDbBudgetInputs {
    use runtime_acceleration::memory_budget::DuckDbBudgetInputs;

    // The instance-identity rule is `DuckDB`'s own — it resolves `duckdb_file`,
    // `duckdb_data_dir` and the default filename through the same factory the engine
    // opens databases with — so the engine answers, and the two cannot disagree about
    // which accelerations share an instance. Asked through the registration slice
    // rather than by naming the engine crate, which the runtime must not depend on.
    data_accelerator_api::DATA_ACCELERATOR_REGISTRATIONS
        .iter()
        .find(|registration| registration.engine == runtime_acceleration::Engine::DuckDB)
        .and_then(data_accelerator_api::AcceleratorRegistration::build_with_defaults)
        .map_or_else(DuckDbBudgetInputs::default, |accelerator| {
            accelerator.memory_budget_inputs(app)
        })
}

/// Without the `duckdb` feature no `DuckDB` accelerators can be configured, so the
/// coordinated budget has nothing to do.
#[cfg(not(feature = "duckdb"))]
fn duckdb_budget_inputs(
    _app: Option<&Arc<app::App>>,
) -> runtime_acceleration::memory_budget::DuckDbBudgetInputs {
    runtime_acceleration::memory_budget::DuckDbBudgetInputs::default()
}

/// Emits the "auto-limit with warning" guidance when the coordinated `DuckDB`
/// budget engaged. `NoOp` (no `DuckDB` accelerators, or the naive ceilings already
/// fit) stays silent.
fn emit_duckdb_memory_budget_warning(
    plan: &runtime_acceleration::memory_budget::AcceleratorMemoryPlan,
    total_memory: u64,
    duckdb_default_per_instance: u64,
    inputs: &runtime_acceleration::memory_budget::DuckDbBudgetInputs,
) {
    use runtime_acceleration::memory_budget::PlanOutcome;

    if plan.outcome != PlanOutcome::Applied {
        return;
    }

    let hb = |bytes: u64| util::human_readable_bytes(usize::try_from(bytes).unwrap_or(usize::MAX));
    let n = inputs.num_unset_instances;
    let total_h = hb(total_memory);
    let query_h = hb(plan.effective_query_pool_bytes);
    let per_instance_h = hb(plan.per_instance_cap_bytes);
    // DuckDB's own default is ~80% of HOST RAM (not the cgroup total) — the value the
    // projection/decision used.
    let duckdb_default_h = hb(duckdb_default_per_instance);
    let mixed = if inputs.has_mixed_instance {
        " One or more DuckDB instances have inconsistent duckdb_memory_limit across the datasets that share them (mixed set/unset, or different explicit values); because DuckDB's memory_limit is per-instance the last dataset created wins, so set it consistently on all datasets sharing an instance."
    } else {
        ""
    };

    if n == 0 {
        // Every DuckDB instance set an explicit duckdb_memory_limit — there are no
        // un-limited instances to auto-cap, only the query pool was reduced to fit
        // those explicit ceilings. (Describe just that, not a "0 instances capped".)
        if plan.residual_overcommit {
            tracing::warn!(
                total_memory_bytes = total_memory,
                query_pool_bytes = plan.effective_query_pool_bytes,
                duckdb_explicit_bytes = inputs.sum_explicit_bytes,
                "The explicit DuckDB accelerator memory limits plus the query memory limit exceed the coordinated memory budget and cut into the safety headroom below the {total_h} available to this process; combined ceilings may approach or exceed it and risk an OOM kill under load. Lower the per-dataset duckdb_memory_limit values and/or runtime.query.memory_limit so combined ceilings fit.{mixed} For details, visit: https://spiceai.org/docs/reference/memory"
            );
        } else {
            tracing::warn!(
                total_memory_bytes = total_memory,
                query_pool_bytes = plan.effective_query_pool_bytes,
                duckdb_explicit_bytes = inputs.sum_explicit_bytes,
                "Reduced the DataFusion query memory limit to {query_h} so it plus the explicit DuckDB accelerator memory limits fit the {total_h} available to this process. To customize, set runtime.query.memory_limit.{mixed} For details, visit: https://spiceai.org/docs/reference/memory"
            );
        }
    } else if plan.residual_overcommit {
        tracing::warn!(
            total_memory_bytes = total_memory,
            projected_ceiling_bytes = plan.projected_ceiling_bytes,
            query_pool_bytes = plan.effective_query_pool_bytes,
            duckdb_unset_instances = n,
            recommended_duckdb_memory_limit_bytes = plan.per_instance_cap_bytes,
            recommended_query_memory_limit_bytes = plan.effective_query_pool_bytes,
            "Even after auto-capping, the {n} DuckDB instance(s) without an explicit duckdb_memory_limit plus the query memory limit exceed the coordinated memory budget and cut into the safety headroom below the {total_h} available to this process; combined ceilings may approach or exceed it and risk an OOM kill under load. Reduce the number of distinct DuckDB files, or set runtime.query.memory_limit: \"{query_h}\" and duckdb_memory_limit: \"{per_instance_h}\" on each DuckDB-accelerated dataset so combined ceilings fit.{mixed} For details, visit: https://spiceai.org/docs/reference/memory"
        );
    } else {
        tracing::warn!(
            total_memory_bytes = total_memory,
            query_pool_bytes = plan.effective_query_pool_bytes,
            duckdb_unset_instances = n,
            duckdb_per_instance_bytes = plan.per_instance_cap_bytes,
            duckdb_unset_instance_paths = ?inputs.unset_instance_labels,
            "Detected potential memory over-commit from DuckDB accelerators and automatically capped memory to fit the {total_h} available to this process: {n} DuckDB instance(s) without an explicit duckdb_memory_limit — each would otherwise default to ~80% of host RAM (about {duckdb_default_h} here, or more in a container where DuckDB sees the host's RAM rather than this process's cgroup limit) — are capped at {per_instance_h} each, and the query memory limit at {query_h}. To customize, set runtime.query.memory_limit and/or per-dataset duckdb_memory_limit.{mixed} For details, visit: https://spiceai.org/docs/reference/memory"
        );
    }
}

fn parse_f64_runtime_param(params: &HashMap<String, String>, key: &str) -> Option<f64> {
    let raw = params.get(key)?;
    match raw.parse::<f64>() {
        Ok(value) if value.is_finite() && value >= 0.0 => Some(value),
        Ok(_) => {
            tracing::warn!(
                "runtime.params.{key}={raw:?} must be a finite non-negative number; using default"
            );
            None
        }
        Err(e) => {
            tracing::warn!(
                "runtime.params.{key}={raw:?} is not a valid number ({e}); using default"
            );
            None
        }
    }
}

fn clamp_cayenne_compaction_memory_fraction(value: f64) -> f64 {
    let clamped = value.clamp(
        MIN_COMPACTION_MEMORY_FRACTION,
        MAX_COMPACTION_MEMORY_FRACTION,
    );
    if !(MIN_COMPACTION_MEMORY_FRACTION..=MAX_COMPACTION_MEMORY_FRACTION).contains(&value) {
        tracing::warn!(
            "runtime.params.{CAYENNE_COMPACTION_MEMORY_FRACTION_PARAM}={value} is outside supported range [{MIN_COMPACTION_MEMORY_FRACTION}, {MAX_COMPACTION_MEMORY_FRACTION}]; using {clamped}"
        );
    }
    clamped
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CayenneFilterPropagation {
    Disabled,
    Enabled,
}

impl CayenneFilterPropagation {
    fn is_enabled(self) -> bool {
        matches!(self, Self::Enabled)
    }
}

/// Parse the Cayenne filter-propagation tunable. Returns `None` when the key is
/// unset or holds an invalid value (in which case the effective behavior is
/// `Disabled` and an invalid value warns) — mirroring [`parse_usize_runtime_param`]
/// /[`parse_f64_runtime_param`], so callers only log "applied" for a value the
/// user validly set.
fn parse_cayenne_filter_propagation(
    params: &HashMap<String, String>,
) -> Option<CayenneFilterPropagation> {
    let raw = params.get(CAYENNE_FILTER_PROPAGATION_PARAM)?;

    match raw.trim().to_ascii_lowercase().as_str() {
        "enabled" => Some(CayenneFilterPropagation::Enabled),
        "disabled" => Some(CayenneFilterPropagation::Disabled),
        _ => {
            tracing::warn!(
                "runtime.params.{CAYENNE_FILTER_PROPAGATION_PARAM}={raw:?} must be 'enabled' or 'disabled'; using disabled"
            );
            None
        }
    }
}

fn default_cayenne_optimizer_rules(filter_propagation_enabled: bool) -> CayenneOptimizerRules {
    let mut rules = CayenneOptimizerRules::auto_enabled();
    rules.set_filter_propagation(filter_propagation_enabled);
    rules.set_inlist_to_range(filter_propagation_enabled);
    rules
}

fn parse_cayenne_optimizer_rules(
    params: &HashMap<String, String>,
    filter_propagation_enabled: bool,
) -> CayenneOptimizerRules {
    let default_rules = default_cayenne_optimizer_rules(filter_propagation_enabled);
    let Some(raw) = params.get(CAYENNE_OPTIMIZER_RULES_PARAM) else {
        return default_rules;
    };

    let normalized = raw.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "auto" => return default_rules,
        "all" => return CayenneOptimizerRules::all_enabled(),
        "none" | "disabled" => return CayenneOptimizerRules::none(),
        _ => {}
    }

    let mut rules = CayenneOptimizerRules::none();
    let mut saw_rule = false;
    let mut unknown_rules: Vec<String> = Vec::new();
    for token in normalized
        .split(|character: char| character == ',' || character.is_ascii_whitespace())
        .filter(|token| !token.is_empty())
    {
        let rule_name = token.replace('-', "_");
        match rule_name.as_str() {
            "filter_propagation" | "logical_filter_propagation" | "propagate_filter" => {
                rules.set_filter_propagation(true);
            }
            "cross_join_reassociation" | "reassociate_cross_join" | "join_reassociation" => {
                rules.set_cross_join_reassociation(true);
            }
            "inlist_to_range" | "in_list_to_range" => {
                rules.set_inlist_to_range(true);
            }
            "semi_join_pushdown" | "push_down_semi_join" | "semi_join" => {
                rules.set_semi_join_pushdown(true);
            }
            "join_reorder" | "reorder_join" | "join_ordering" => {
                rules.set_join_reorder(true);
            }
            "dynamic_filter_sharing" | "dynamic_filters" => {
                rules.set_dynamic_filter_sharing(true);
            }
            "maintained_aggregate"
            | "maintained_aggregates"
            | "cdc_aggregate"
            | "cdc_aggregates" => {
                rules.set_maintained_aggregate(true);
            }
            "anti_join_sort_merge" | "anti_sort_merge" => {
                rules.set_anti_join_sort_merge(true);
            }
            // Opt-in: restores the Cayenne `ExactLeftAccumulator` join rewrite
            // (`CayenneJoinRewriter`). Off by default — the default path uses
            // DataFusion 53's native inner-join hash-join dynamic-filter pushdown
            // (min/max bounds + InList/hash-table membership). Naming this rule
            // re-enables the forked exact in-list accumulator alongside it.
            "exact_join_filter" | "join_rewriter" | "exact_accumulator" => {
                rules.set_exact_join_filter(true);
            }
            "stats_aggregate" | "metadata_aggregate" | "aggregate_pushdown" => {
                rules.set_stats_aggregate(true);
            }
            _ => {
                // Don't discard the rest of an explicit list because of one bad
                // token; collect the unknown ones, keep the recognized rules,
                // and warn below.
                unknown_rules.push(token.to_string());
                continue;
            }
        }
        saw_rule = true;
    }

    if saw_rule {
        if !unknown_rules.is_empty() {
            tracing::warn!(
                "runtime.params.{CAYENNE_OPTIMIZER_RULES_PARAM}={raw:?} contains unknown Cayenne optimizer rule(s) {unknown_rules:?}; ignoring them and using the recognized rules"
            );
        }
        rules
    } else {
        tracing::warn!(
            "runtime.params.{CAYENNE_OPTIMIZER_RULES_PARAM}={raw:?} did not include any recognized Cayenne optimizer rules; using auto"
        );
        default_rules
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[cfg(not(windows))]
    fn dataset_with_cayenne(
        name: &str,
        segment_cache_mb: Option<&str>,
    ) -> spicepod::component::dataset::Dataset {
        let mut dataset = spicepod::component::dataset::Dataset::new("postgres:public.t", name);
        let mut acceleration = spicepod::acceleration::Acceleration {
            enabled: true,
            engine: Some("cayenne".to_string()),
            ..Default::default()
        };
        if let Some(mb) = segment_cache_mb {
            acceleration.params = Some(spicepod::param::Params::from_string_map(
                [("cayenne_segment_cache_mb".to_string(), mb.to_string())]
                    .into_iter()
                    .collect(),
            ));
        }
        dataset.acceleration = Some(acceleration);
        dataset
    }

    /// A pod with no Cayenne acceleration must reserve nothing. The reservation is
    /// subtracted from the query memory limit, so counting a cache that is never
    /// installed would shrink every query's budget for memory nothing allocates.
    #[cfg(not(windows))]
    #[test]
    fn no_cayenne_acceleration_reserves_nothing() {
        let app = Arc::new(
            app::AppBuilder::new("test")
                .with_dataset(spicepod::component::dataset::Dataset::new(
                    "postgres:public.t",
                    "plain",
                ))
                .build(),
        );
        assert_eq!(
            estimate_cayenne_reservation_bytes(Some(&app), &HashMap::new()),
            0,
            "a pod with no Cayenne table reserves nothing, segment cache included"
        );
    }

    /// The counterpart to the no-Cayenne case: a pod that does have a Cayenne
    /// table reserves its write-path state, so the gate above is what decides,
    /// not an empty estimator.
    #[cfg(not(windows))]
    #[test]
    fn a_cayenne_acceleration_reserves_its_write_path_state() {
        let app = Arc::new(
            app::AppBuilder::new("test")
                .with_dataset(dataset_with_cayenne("accelerated", None))
                .build(),
        );
        assert!(
            estimate_cayenne_reservation_bytes(Some(&app), &HashMap::new()) > 0,
            "a Cayenne table reserves against the query pool"
        );
    }

    /// A catalog-only pod hosts Cayenne tables with no dataset acceleration block,
    /// so the reservation has to see it or the shared cache grows beside a query
    /// pool that never accounted for it.
    #[cfg(not(windows))]
    #[test]
    fn a_cayenne_catalog_reserves_even_without_a_dataset_acceleration() {
        let catalog =
            spicepod::component::catalog::Catalog::new("cayenne".to_string(), "cat".to_string());
        let app = Arc::new(app::AppBuilder::new("test").with_catalog(catalog).build());
        assert!(
            reads_from_cayenne_catalog(&app),
            "a `from: cayenne` catalog must be recognized"
        );
        assert!(
            estimate_cayenne_reservation_bytes(Some(&app), &HashMap::new()) > 0
                || vortex_datafusion::process_segment_cache_capacity_bytes().is_none(),
            "a catalog-only pod reserves the shared cache when one is installed"
        );
    }

    #[test]
    fn segment_cache_budget_honours_an_explicit_value() {
        const MIB: u64 = 1024 * 1024;
        assert_eq!(segment_cache_budget_bytes(Some(512)), 512 * MIB);
        // Below the derived floor on purpose: an explicit value is the operator's
        // call, not a hint to clamp.
        assert_eq!(segment_cache_budget_bytes(Some(16)), 16 * MIB);
        assert_eq!(
            segment_cache_budget_bytes(Some(0)),
            0,
            "zero disables segment caching"
        );
    }

    #[test]
    fn segment_cache_budget_default_stays_within_its_bounds() {
        const MIB: u64 = 1024 * 1024;
        // Derived from the host, so assert the contract rather than a figure: one
        // shared budget between 256 MiB and 2 GiB, whatever the machine.
        let derived = segment_cache_budget_bytes(None);
        assert!(
            (256 * MIB..=2048 * MIB).contains(&derived),
            "derived budget {derived} out of bounds"
        );
        assert!(
            derived.is_multiple_of(MIB),
            "the budget is a whole number of MiB"
        );
    }

    #[test]
    fn test_parse_memory_limit() {
        let test_cases: Vec<(Option<&str>, Option<u64>)> = vec![
            // bytes
            (Some("1GB"), Some(1_000_000_000)),
            (Some("1G"), Some(1_000_000_000)),
            (Some("1MB"), Some(1_000_000)),
            (Some("1M"), Some(1_000_000)),
            (Some("1KB"), Some(1_000)),
            (Some("1K"), Some(1_000)),
            (Some("1B"), Some(1)),
            // bits
            (Some("1gb"), Some(125_000_000)),
            (Some("1mb"), Some(125_000)),
            (Some("1kb"), Some(125)),
            (Some("1b"), Some(1)),
            // kibi, gibi, mebi
            (Some("1GiB"), Some(1_073_741_824)),
            (Some("1Gi"), Some(1_073_741_824)),
            (Some("1MiB"), Some(1_048_576)),
            (Some("1Mi"), Some(1_048_576)),
            (Some("1KiB"), Some(1024)),
            (Some("1Ki"), Some(1024)),
            // without a b identifier, defaults to bytes
            (Some("1g"), Some(1_000_000_000)),
            (Some("1m"), Some(1_000_000)),
            (Some("1k"), Some(1_000)),
            (Some("1"), Some(1)),
            (Some("0"), None),
            (Some("-1"), None),
            (Some("invalid"), None),
            (None, None),
        ];

        for (input, expected) in test_cases {
            let result = parse_memory_limit(input.map(ToString::to_string));
            assert_eq!(result, expected, "Input: {input:?}");
        }
    }

    /// Builds the `duckdb` acceleration block shared by the budget-adapter tests.
    #[cfg(feature = "duckdb")]
    fn duckdb_acceleration(
        mode: spicepod::acceleration::Mode,
        params: &[(&str, &str)],
    ) -> spicepod::acceleration::Acceleration {
        spicepod::acceleration::Acceleration {
            enabled: true,
            engine: Some("duckdb".to_string()),
            mode,
            params: Some(spicepod::param::Params::from_string_map(
                params
                    .iter()
                    .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                    .collect::<HashMap<String, String>>(),
            )),
            ..spicepod::acceleration::Acceleration::default()
        }
    }

    #[cfg(feature = "duckdb")]
    fn duckdb_ds(
        name: &str,
        mode: spicepod::acceleration::Mode,
        params: &[(&str, &str)],
    ) -> spicepod::component::dataset::Dataset {
        let mut ds = spicepod::component::dataset::Dataset::new("dummy:source", name);
        ds.acceleration = Some(duckdb_acceleration(mode, params));
        ds
    }

    #[cfg(feature = "duckdb")]
    fn duckdb_view(
        name: &str,
        mode: spicepod::acceleration::Mode,
        params: &[(&str, &str)],
    ) -> spicepod::component::view::View {
        duckdb_view_with(name, duckdb_acceleration(mode, params))
    }

    #[cfg(feature = "duckdb")]
    fn duckdb_view_with(
        name: &str,
        acceleration: spicepod::acceleration::Acceleration,
    ) -> spicepod::component::view::View {
        spicepod::component::view::View::new(name.to_string())
            .with_sql("SELECT 1")
            .with_acceleration(acceleration)
    }

    /// Runs the budget adapter over an app built from `datasets` and `views`.
    #[cfg(feature = "duckdb")]
    fn budget_inputs_for(
        datasets: Vec<spicepod::component::dataset::Dataset>,
        views: Vec<spicepod::component::view::View>,
    ) -> runtime_acceleration::memory_budget::DuckDbBudgetInputs {
        let mut builder = app::AppBuilder::new("mem-budget-test");
        for dataset in datasets {
            builder = builder.with_dataset(dataset);
        }
        for view in views {
            builder = builder.with_view(view);
        }
        duckdb_budget_inputs(Some(&std::sync::Arc::new(builder.build())))
    }

    /// The `DuckDB` budget adapter groups datasets by instance identity (distinct
    /// file paths → distinct instances; a shared file or all memory-mode datasets →
    /// one), classifies explicit vs un-limited, and ignores non-DuckDB engines.
    #[cfg(feature = "duckdb")]
    #[test]
    fn test_duckdb_budget_inputs_groups_by_instance() {
        use spicepod::acceleration::{Acceleration, Mode};
        use spicepod::component::dataset::Dataset;

        let inputs_for = |datasets: Vec<Dataset>| budget_inputs_for(datasets, vec![]);

        // Two file-mode datasets on DISTINCT files → two un-limited instances.
        let inputs = inputs_for(vec![
            duckdb_ds("a", Mode::File, &[("duckdb_file", "/tmp/spice-mbt-a.db")]),
            duckdb_ds("b", Mode::File, &[("duckdb_file", "/tmp/spice-mbt-b.db")]),
        ]);
        assert_eq!(inputs.num_unset_instances, 2);
        assert_eq!(inputs.num_explicit_instances, 0);

        // Two file-mode datasets on the SAME file share ONE instance.
        let inputs = inputs_for(vec![
            duckdb_ds(
                "a",
                Mode::File,
                &[("duckdb_file", "/tmp/spice-mbt-shared.db")],
            ),
            duckdb_ds(
                "b",
                Mode::File,
                &[("duckdb_file", "/tmp/spice-mbt-shared.db")],
            ),
        ]);
        assert_eq!(inputs.num_unset_instances, 1);

        // One explicit + one un-limited instance.
        let inputs = inputs_for(vec![
            duckdb_ds(
                "a",
                Mode::File,
                &[
                    ("duckdb_file", "/tmp/spice-mbt-x.db"),
                    ("duckdb_memory_limit", "2GiB"),
                ],
            ),
            duckdb_ds("b", Mode::File, &[("duckdb_file", "/tmp/spice-mbt-y.db")]),
        ]);
        assert_eq!(inputs.num_unset_instances, 1);
        assert_eq!(inputs.num_explicit_instances, 1);
        assert_eq!(inputs.sum_explicit_bytes, 2 * 1024 * 1024 * 1024);

        // All memory-mode datasets collapse to ONE in-memory instance.
        let inputs = inputs_for(vec![
            duckdb_ds("a", Mode::Memory, &[]),
            duckdb_ds("b", Mode::Memory, &[]),
        ]);
        assert_eq!(inputs.num_unset_instances, 1);

        // A non-DuckDB (Arrow) accelerated dataset is ignored.
        let mut arrow_ds = Dataset::new("dummy:source", "arrow");
        arrow_ds.acceleration = Some(Acceleration {
            enabled: true,
            engine: None,
            mode: Mode::Memory,
            ..Acceleration::default()
        });
        let inputs = inputs_for(vec![arrow_ds]);
        assert_eq!(inputs.num_unset_instances, 0);
        assert_eq!(inputs.num_explicit_instances, 0);

        // Two datasets on the SAME file with DIFFERENT explicit limits → one
        // explicit instance flagged as inconsistent (per-instance limit is ambiguous).
        let inputs = inputs_for(vec![
            duckdb_ds(
                "a",
                Mode::File,
                &[
                    ("duckdb_file", "/tmp/spice-mbt-conflict.db"),
                    ("duckdb_memory_limit", "2GiB"),
                ],
            ),
            duckdb_ds(
                "b",
                Mode::File,
                &[
                    ("duckdb_file", "/tmp/spice-mbt-conflict.db"),
                    ("duckdb_memory_limit", "4GiB"),
                ],
            ),
        ]);
        assert_eq!(inputs.num_explicit_instances, 1);
        assert_eq!(inputs.num_unset_instances, 0);
        assert!(inputs.has_mixed_instance, "conflicting per-instance limits");
        // The instance's ceiling uses the max of the conflicting values.
        assert_eq!(inputs.sum_explicit_bytes, 4 * 1024 * 1024 * 1024);
    }

    /// A file-mode Cayenne acceleration. `mode` is set explicitly because the
    /// Spicepod default is `mode: memory`, which never compacts whatever its refresh
    /// mode — that would mask the refresh-mode classification these tests exercise.
    fn cayenne_test_accel(engine: &str, enabled: bool) -> spicepod::acceleration::Acceleration {
        spicepod::acceleration::Acceleration {
            enabled,
            engine: Some(engine.to_string()),
            mode: spicepod::acceleration::Mode::File,
            ..spicepod::acceleration::Acceleration::default()
        }
    }

    fn cayenne_test_dataset(
        name: &str,
        accel: spicepod::acceleration::Acceleration,
    ) -> spicepod::component::dataset::Dataset {
        cayenne_test_dataset_from("dummy:source", name, accel)
    }

    fn cayenne_test_dataset_from(
        from: &str,
        name: &str,
        accel: spicepod::acceleration::Acceleration,
    ) -> spicepod::component::dataset::Dataset {
        let mut ds = spicepod::component::dataset::Dataset::new(from, name);
        ds.acceleration = Some(accel);
        ds
    }

    #[cfg(not(windows))]
    fn cayenne_test_view(
        name: &str,
        accel: spicepod::acceleration::Acceleration,
    ) -> spicepod::component::view::View {
        let mut view = spicepod::component::view::View::new(name.to_string());
        view.sql = Some("SELECT 1".to_string());
        view.acceleration = Some(accel);
        view
    }

    fn cayenne_test_app(
        datasets: Vec<spicepod::component::dataset::Dataset>,
        views: Vec<spicepod::component::view::View>,
    ) -> Arc<app::App> {
        let builder = datasets.into_iter().fold(
            app::AppBuilder::new("cayenne-gate-test"),
            app::AppBuilder::with_dataset,
        );
        Arc::new(
            views
                .into_iter()
                .fold(builder, app::AppBuilder::with_view)
                .build(),
        )
    }

    /// A view carries its own `acceleration` block and reaches the same
    /// `DataAccelerator::init` path as a dataset, so Cayenne on a view must count
    /// toward the compaction-pool gate — otherwise a view-only Cayenne pod carves no
    /// compaction pool and sizes its query pool as if no Cayenne tier existed.
    #[cfg(not(windows))]
    #[test]
    fn cayenne_workload_counts_views_and_datasets() {
        use spicepod::component::dataset::Dataset;
        use spicepod::component::view::View;

        let configured = |datasets: Vec<Dataset>, views: Vec<View>| {
            cayenne_workload(Some(&cayenne_test_app(datasets, views))).is_configured()
        };
        let dataset_with = |engine: &str, enabled: bool| {
            cayenne_test_dataset("ds", cayenne_test_accel(engine, enabled))
        };
        let view_with = |engine: &str, enabled: bool| {
            cayenne_test_view("v", cayenne_test_accel(engine, enabled))
        };

        // Regression: Cayenne declared ONLY on a view must still be seen.
        assert!(
            configured(vec![], vec![view_with("cayenne", true)]),
            "a view-only Cayenne pod must carve a compaction pool"
        );

        // Pre-existing behavior: Cayenne on a dataset is unchanged.
        assert!(configured(vec![dataset_with("cayenne", true)], vec![]));

        // Either side alone is enough; a non-Cayenne sibling doesn't mask it.
        assert!(configured(
            vec![dataset_with("duckdb", true)],
            vec![view_with("cayenne", true)]
        ));

        // Engine matching stays case-insensitive on the view arm too.
        assert!(configured(vec![], vec![view_with("Cayenne", true)]));

        // A disabled Cayenne acceleration is not configured — on either kind.
        assert!(!configured(vec![], vec![view_with("cayenne", false)]));
        assert!(!configured(vec![dataset_with("cayenne", false)], vec![]));

        // No Cayenne anywhere, and the empty / absent-app cases.
        assert!(!configured(
            vec![dataset_with("duckdb", true)],
            vec![view_with("duckdb", true)]
        ));
        assert!(!configured(vec![], vec![]));
        assert!(!cayenne_workload(None).is_configured());
    }

    /// The two host-resource decisions the workload drives must key off the refresh
    /// mode, not merely on Cayenne being present: a full-refresh-only pod has no
    /// reachable in-memory CDC tier to reserve host RAM for, and nothing for
    /// compaction to consolidate.
    #[cfg(not(windows))]
    #[test]
    fn cayenne_workload_separates_cdc_tier_from_compaction() {
        use spicepod::acceleration::RefreshMode;

        let accel_with =
            |mode: RefreshMode, interval: Option<&str>| spicepod::acceleration::Acceleration {
                refresh_mode: Some(mode),
                refresh_check_interval: interval.map(ToString::to_string),
                ..cayenne_test_accel("cayenne", true)
            };
        let workload = |accels: Vec<spicepod::acceleration::Acceleration>| {
            let datasets = accels
                .into_iter()
                .enumerate()
                .map(|(i, a)| cayenne_test_dataset(&format!("ds{i}"), a))
                .collect();
            cayenne_workload(Some(&cayenne_test_app(datasets, vec![])))
        };

        // Full refresh only: neither the CDC tier nor compaction is reachable.
        let full_only = workload(vec![accel_with(RefreshMode::Full, None)]);
        assert!(full_only.is_configured());
        assert!(
            !full_only.uses_cdc_tier(),
            "a full-refresh pod must not fence host RAM for a tier cdc_durability forces to `file`"
        );
        assert!(
            !full_only.needs_compaction(),
            "a whole-table replace leaves nothing to consolidate"
        );

        // An unset refresh_mode defaults to `full` and must classify identically.
        assert_eq!(
            workload(vec![cayenne_test_accel("cayenne", true)]),
            full_only
        );

        // Changes mode needs both.
        let cdc = workload(vec![accel_with(RefreshMode::Changes, None)]);
        assert!(cdc.uses_cdc_tier() && cdc.needs_compaction());

        // Append accumulates files, so it compacts — but only a fast cadence puts it
        // on the small-write profile that can reach the tier.
        let slow_append = workload(vec![accel_with(RefreshMode::Append, Some("1h"))]);
        assert!(!slow_append.uses_cdc_tier() && slow_append.needs_compaction());
        let fast_append = workload(vec![accel_with(RefreshMode::Append, Some("10s"))]);
        assert!(fast_append.uses_cdc_tier() && fast_append.needs_compaction());

        // Snapshot mode is not an overwrite this module can prove, so it keeps
        // compaction on without reaching the tier.
        let snapshot = workload(vec![accel_with(RefreshMode::Snapshot, None)]);
        assert!(!snapshot.uses_cdc_tier() && snapshot.needs_compaction());

        // The two flags also split on the acceleration MODE, not just the refresh
        // mode. `mode: memory` (the Spicepod default) zeroes the compaction interval
        // and never takes the durable write path, so it produces no file to compact —
        // yet it holds its whole dataset in the in-memory tier, which is precisely the
        // case that most needs the host RAM left for it.
        let memory_cdc = workload(vec![spicepod::acceleration::Acceleration {
            mode: spicepod::acceleration::Mode::Memory,
            ..accel_with(RefreshMode::Changes, None)
        }]);
        assert!(
            memory_cdc.uses_cdc_tier(),
            "a memory-mode table lives in the tier, so the room must still be reserved"
        );
        assert!(
            !memory_cdc.needs_compaction(),
            "memory mode never writes a Vortex file, so there is nothing to compact"
        );

        // Both flags are unions: one CDC table in a pod of full-refresh tables still
        // earns the full CDC-shaped reservation.
        let mixed = workload(vec![
            accel_with(RefreshMode::Full, None),
            accel_with(RefreshMode::Changes, None),
        ]);
        assert!(mixed.uses_cdc_tier() && mixed.needs_compaction());
    }

    /// An unset `refresh_mode` is filled in by the CONNECTOR, not by a fixed
    /// default, so the pre-init classifier must apply the same connector defaults.
    /// Assuming `full` for an unannotated `debezium:`/`cdc:` dataset would classify
    /// a genuine CDC pod as a whole-table replace and under-provision its memory.
    #[cfg(not(windows))]
    #[test]
    fn cayenne_workload_honors_connector_unset_refresh_defaults() {
        let workload_for = |from: &str| {
            let ds = cayenne_test_dataset_from(from, "ds", cayenne_test_accel("cayenne", true));
            cayenne_workload(Some(&cayenne_test_app(vec![ds], vec![])))
        };

        // `debezium` and `cdc` resolve an unset mode to `changes`.
        for from in ["debezium:topic", "cdc:stream"] {
            let w = workload_for(from);
            assert!(
                w.uses_cdc_tier() && w.needs_compaction(),
                "{from} with no refresh_mode is a CDC stream and must get the CDC reservation"
            );
        }

        // `sink` resolves to `disabled`: no refresh, but `INSERT INTO` accumulates
        // files, so compaction is still needed — and the CDC tier is not.
        let sink = workload_for("sink");
        assert!(!sink.uses_cdc_tier() && sink.needs_compaction());

        // Everything else takes the trait default of `full`.
        for from in ["postgres:public.t", "s3://bucket/path", "dummy:source"] {
            let w = workload_for(from);
            assert!(
                !w.uses_cdc_tier() && !w.needs_compaction(),
                "{from} with no refresh_mode is a full refresh"
            );
        }

        // An EXPLICIT refresh_mode is authoritative for every connector — each
        // override returns the caller's value verbatim when it is `Some`.
        let mut explicit_full = cayenne_test_accel("cayenne", true);
        explicit_full.refresh_mode = Some(spicepod::acceleration::RefreshMode::Full);
        let ds = cayenne_test_dataset_from("debezium:topic", "ds", explicit_full);
        let w = cayenne_workload(Some(&cayenne_test_app(vec![ds], vec![])));
        assert!(
            !w.uses_cdc_tier() && !w.needs_compaction(),
            "an explicit refresh_mode: full overrides the connector's changes default"
        );
    }

    /// The dedicated CDC-apply pool is `cores - 1` threads for a loop only
    /// `refresh_mode: changes` runs, and it is engine-agnostic.
    #[test]
    fn streams_cdc_changes_detects_any_engine() {
        use spicepod::acceleration::RefreshMode;

        let with_mode = |engine: &str, mode: Option<RefreshMode>, enabled: bool| {
            cayenne_test_dataset(
                "ds",
                spicepod::acceleration::Acceleration {
                    refresh_mode: mode,
                    ..cayenne_test_accel(engine, enabled)
                },
            )
        };
        let from_with_mode = |from: &str, mode: Option<RefreshMode>| {
            cayenne_test_dataset_from(
                from,
                "ds",
                spicepod::acceleration::Acceleration {
                    refresh_mode: mode,
                    ..cayenne_test_accel("cayenne", true)
                },
            )
        };
        let streams = |ds: Vec<spicepod::component::dataset::Dataset>| {
            streams_cdc_changes(Some(&cayenne_test_app(ds, vec![])))
        };

        assert!(streams(vec![with_mode(
            "duckdb",
            Some(RefreshMode::Changes),
            true
        )]));
        assert!(streams(vec![with_mode(
            "cayenne",
            Some(RefreshMode::Changes),
            true
        )]));
        assert!(!streams(vec![with_mode(
            "cayenne",
            Some(RefreshMode::Full),
            true
        )]));
        assert!(!streams(vec![with_mode("cayenne", None, true)]));
        assert!(
            !streams(vec![with_mode(
                "cayenne",
                Some(RefreshMode::Changes),
                false
            )]),
            "a disabled acceleration never runs the apply loop"
        );
        assert!(!streams(vec![]));
        assert!(!streams_cdc_changes(None));

        // An unset refresh_mode on a connector that resolves it to `changes` still
        // runs the apply loop — skipping the pool for it would deprioritize a
        // freshness-critical stream onto the low-priority refresh runtime.
        for from in ["debezium:topic", "cdc:stream"] {
            assert!(
                streams(vec![from_with_mode(from, None)]),
                "{from} with no refresh_mode resolves to changes"
            );
        }
        assert!(!streams(vec![from_with_mode("postgres:public.t", None)]));
        assert!(!streams(vec![from_with_mode("sink", None)]));
        assert!(
            !streams(vec![from_with_mode(
                "debezium:topic",
                Some(RefreshMode::Full)
            )]),
            "an explicit refresh_mode overrides the connector default"
        );

        // An accelerated catalog runs the same apply loop for every table it
        // discovers, so a catalog-only pod needs the dedicated runtime too.
        assert!(streams_cdc_changes(Some(&catalog_test_app(test_catalog(
            Some((spicepod::acceleration::Mode::Memory, &[]))
        )))));
        assert!(!streams_cdc_changes(Some(&catalog_test_app(test_catalog(
            None
        )))));
    }

    /// One accelerated dataset for [`cayenne_budget_app`], as
    /// `(engine, enabled, mode, refresh_mode, refresh_check_interval)`.
    #[cfg(not(windows))]
    type DatasetSpec<'a> = (
        &'a str,
        bool,
        spicepod::acceleration::Mode,
        Option<spicepod::acceleration::RefreshMode>,
        Option<&'a str>,
    );

    /// Build a Spicepod app from `datasets`, plus `cayenne_views` Cayenne-accelerated
    /// file-mode views.
    #[cfg(not(windows))]
    fn cayenne_budget_app(datasets: Vec<DatasetSpec<'_>>, cayenne_views: usize) -> Arc<app::App> {
        use spicepod::acceleration::{Acceleration, Mode};
        use spicepod::component::{dataset::Dataset, view::View};

        let mut builder = app::AppBuilder::new("cayenne-budget-test");
        for (index, (engine, enabled, mode, refresh_mode, refresh_check_interval)) in
            datasets.into_iter().enumerate()
        {
            let mut dataset = Dataset::new("dummy:source", format!("ds_{index}"));
            dataset.acceleration = Some(Acceleration {
                enabled,
                engine: Some(engine.to_string()),
                mode,
                refresh_mode,
                refresh_check_interval: refresh_check_interval.map(ToString::to_string),
                ..Acceleration::default()
            });
            builder = builder.with_dataset(dataset);
        }
        for index in 0..cayenne_views {
            let mut view = View::new(format!("v_{index}"));
            view.sql = Some("SELECT 1".to_string());
            view.acceleration = Some(Acceleration {
                enabled: true,
                engine: Some("cayenne".to_string()),
                mode: Mode::File,
                refresh_mode: Some(spicepod::acceleration::RefreshMode::Full),
                ..Acceleration::default()
            });
            builder = builder.with_view(view);
        }
        Arc::new(builder.build())
    }

    /// A Cayenne acceleration earns the compaction carve only when it is BOTH
    /// file-mode and on a write profile that accumulates files, so only those are
    /// counted. Views never qualify today: `ViewBuilder::try_from` rejects every view
    /// refresh mode except `full`, a whole-table replace.
    #[cfg(not(windows))]
    #[test]
    fn count_compaction_eligible_accelerations_counts_only_eligible_accelerations() {
        use spicepod::acceleration::{Mode, RefreshMode};

        let count = |datasets, views| {
            count_compaction_eligible_accelerations(Some(&cayenne_budget_app(datasets, views)))
        };

        // Every refresh mode that accumulates files, in a file mode. First the
        // small-write profile...
        for refresh_mode in [RefreshMode::Changes, RefreshMode::Caching] {
            assert_eq!(
                count(
                    vec![(
                        "cayenne",
                        true,
                        Mode::File,
                        Some(refresh_mode.clone()),
                        None
                    )],
                    0
                ),
                1,
                "{refresh_mode:?} accumulates files to compact"
            );
        }
        assert_eq!(
            count(
                vec![(
                    "cayenne",
                    true,
                    Mode::File,
                    Some(RefreshMode::Append),
                    Some("5m")
                )],
                0
            ),
            1,
            "append at exactly the threshold is a small-write profile"
        );

        // ...then the bulk-append profiles. Unlike a whole-table replace these still
        // build files up across writes, so they compact — on the conservative cadence
        // rather than the tight one — and the carve is theirs to spend.
        assert_eq!(
            count(
                vec![(
                    "cayenne",
                    true,
                    Mode::File,
                    Some(RefreshMode::Append),
                    Some("6m")
                )],
                0
            ),
            1,
            "a slow append still accumulates files"
        );
        assert_eq!(
            count(
                vec![("cayenne", true, Mode::File, Some(RefreshMode::Append), None)],
                0
            ),
            1
        );
        assert_eq!(
            count(
                vec![(
                    "cayenne",
                    true,
                    Mode::File,
                    Some(RefreshMode::Snapshot),
                    None
                )],
                0
            ),
            1,
            "snapshot mode is not a whole-table replace this module can prove"
        );
        // An unparseable interval is treated as absent rather than classifying off a
        // value the acceleration conversion will reject; `append` without a usable
        // interval is bulk-append, which still compacts.
        assert_eq!(
            count(
                vec![(
                    "cayenne",
                    true,
                    Mode::File,
                    Some(RefreshMode::Append),
                    Some("soon")
                )],
                0
            ),
            1
        );

        // Every file mode counts, not just `file`.
        for mode in [Mode::File, Mode::FileCreate, Mode::FileUpdate] {
            assert_eq!(
                count(
                    vec![(
                        "cayenne",
                        true,
                        mode.clone(),
                        Some(RefreshMode::Changes),
                        None
                    )],
                    0
                ),
                1,
                "{mode:?} is a file acceleration mode"
            );
        }

        // Ineligible: `mode: memory` — the Spicepod DEFAULT. Memory mode zeroes the
        // compaction interval and the writer never takes the durable path, so no
        // Vortex file is ever produced for the carve to be spent on.
        for refresh_mode in [
            RefreshMode::Changes,
            RefreshMode::Caching,
            RefreshMode::Append,
        ] {
            assert_eq!(
                count(
                    vec![(
                        "cayenne",
                        true,
                        Mode::Memory,
                        Some(refresh_mode.clone()),
                        Some("1s")
                    )],
                    0
                ),
                0,
                "mode: memory never compacts ({refresh_mode:?})"
            );
        }

        // Ineligible: the whole-table replace — including the `full` that an unset
        // `refresh_mode` resolves to for this `from:` (`dummy:source`).
        for refresh_mode in [Some(RefreshMode::Full), None] {
            assert_eq!(
                count(
                    vec![("cayenne", true, Mode::File, refresh_mode.clone(), None)],
                    0
                ),
                0,
                "refresh_mode {refresh_mode:?} replaces the whole table"
            );
        }

        // Ineligible: disabled acceleration, or another engine on the same profile.
        assert_eq!(
            count(
                vec![(
                    "cayenne",
                    false,
                    Mode::File,
                    Some(RefreshMode::Changes),
                    None
                )],
                0
            ),
            0
        );
        assert_eq!(
            count(
                vec![("duckdb", true, Mode::File, Some(RefreshMode::Changes), None)],
                0
            ),
            0
        );

        // Engine matching is case-insensitive, eligible accelerations accumulate, and
        // an ineligible sibling does not mask an eligible one.
        assert_eq!(
            count(
                vec![
                    (
                        "Cayenne",
                        true,
                        Mode::File,
                        Some(RefreshMode::Changes),
                        None
                    ),
                    ("cayenne", true, Mode::File, Some(RefreshMode::Full), None),
                    (
                        "cayenne",
                        true,
                        Mode::Memory,
                        Some(RefreshMode::Changes),
                        None
                    ),
                    (
                        "cayenne",
                        true,
                        Mode::File,
                        Some(RefreshMode::Caching),
                        None
                    ),
                ],
                0
            ),
            2
        );

        // Views are counted too, but a view can only be `refresh_mode: full` today, so
        // a view-only pod is Cayenne-configured with nothing eligible.
        let view_only = cayenne_budget_app(vec![], 2);
        assert!(cayenne_workload(Some(&view_only)).is_configured());
        assert_eq!(count_compaction_eligible_accelerations(Some(&view_only)), 0);

        assert_eq!(count_compaction_eligible_accelerations(None), 0);
    }

    /// A Spicepod catalog, accelerated into Cayenne or (with no `acceleration`
    /// block) not accelerated at all. `refresh_mode` has no catalog-level default
    /// because `changes` is the only value the schema admits.
    fn test_catalog(
        acceleration: Option<(spicepod::acceleration::Mode, &[(&str, &str)])>,
    ) -> spicepod::component::catalog::Catalog {
        use spicepod::component::catalog::{
            Catalog, CatalogAcceleration, CatalogAccelerationEngine, CatalogRefreshMode,
        };

        let mut catalog = Catalog::new("postgres:my_catalog".to_string(), "pg".to_string());
        let Some((mode, params)) = acceleration else {
            return catalog;
        };
        catalog.acceleration = Some(CatalogAcceleration {
            engine: CatalogAccelerationEngine::Cayenne,
            refresh_mode: CatalogRefreshMode::Changes,
            mode,
            params: (!params.is_empty()).then(|| {
                spicepod::param::Params::from_string_map(
                    params
                        .iter()
                        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                        .collect(),
                )
            }),
        });
        catalog
    }

    fn catalog_test_app(catalog: spicepod::component::catalog::Catalog) -> Arc<app::App> {
        Arc::new(
            app::AppBuilder::new("catalog-gate-test")
                .with_catalog(catalog)
                .build(),
        )
    }

    /// Regression for #13013: a pod whose only Cayenne acceleration is a catalog must
    /// be budgeted on the same terms as the equivalent set of datasets. Every table a
    /// catalog accelerates is a CDC-accelerated Cayenne table, so the catalog reaches
    /// the in-memory CDC tier (the query-pool reduction that leaves room for that
    /// tier is gated on it) and takes the compaction carve whenever its storage mode
    /// accumulates Vortex files.
    ///
    /// Asserted against an equivalent dataset rather than against literals, so the
    /// two component kinds cannot drift apart as the classification evolves.
    #[cfg(not(windows))]
    #[test]
    fn cayenne_workload_counts_accelerated_catalogs() {
        use spicepod::acceleration::{Acceleration, Mode, RefreshMode};

        let catalog_app = |mode: Mode, params: &[(&str, &str)]| {
            catalog_test_app(test_catalog(Some((mode, params))))
        };
        // The per-table dataset the catalog connector synthesizes for each table it
        // discovers, minus the per-table key.
        let dataset_app = |mode: Mode, params: &[(&str, &str)]| {
            let accel = Acceleration {
                enabled: true,
                engine: Some("cayenne".to_string()),
                refresh_mode: Some(RefreshMode::Changes),
                mode,
                params: (!params.is_empty()).then(|| {
                    spicepod::param::Params::from_string_map(
                        params
                            .iter()
                            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                            .collect(),
                    )
                }),
                ..Acceleration::default()
            };
            cayenne_test_app(
                vec![cayenne_test_dataset_from(
                    "postgres:my_catalog.public.orders",
                    "orders",
                    accel,
                )],
                vec![],
            )
        };

        let params = [("cayenne_file_path", "/data")];
        for mode in [Mode::Memory, Mode::File, Mode::FileCreate, Mode::FileUpdate] {
            let catalog = catalog_app(mode.clone(), &params);
            let dataset = dataset_app(mode.clone(), &params);
            assert_eq!(
                cayenne_workload(Some(&catalog)),
                cayenne_workload(Some(&dataset)),
                "an accelerated catalog must be classified exactly as the tables it creates ({mode:?})"
            );
            assert_eq!(
                count_compaction_eligible_accelerations(Some(&catalog)),
                count_compaction_eligible_accelerations(Some(&dataset)),
                "{mode:?}"
            );
            // The catalog's own accelerator params reach the budget, so a per-table
            // cap the catalog sets is the cap the reservation is sized against.
            assert_eq!(
                estimate_cayenne_reservation_bytes(Some(&catalog), &HashMap::new()),
                estimate_cayenne_reservation_bytes(Some(&dataset), &HashMap::new()),
                "{mode:?}"
            );
        }

        // Spelled out, so the equivalence above can't hold by both sides being wrong:
        // a catalog always streams changes, so it always reaches the CDC tier, and it
        // earns the carve exactly when its storage mode produces files.
        let memory = catalog_app(Mode::Memory, &[]);
        let memory_workload = cayenne_workload(Some(&memory));
        assert!(memory_workload.is_configured());
        assert!(
            memory_workload.uses_cdc_tier(),
            "a catalog's only refresh mode is `changes`, whose tables hold rows in the in-memory CDC tier"
        );
        assert!(
            !memory_workload.needs_compaction(),
            "mode: memory never writes a Vortex file for compaction to consolidate"
        );
        assert!(
            estimate_cayenne_reservation_bytes(Some(&memory), &HashMap::new()) > 0,
            "a catalog-only pod holds off-pool Cayenne caches, so the query pool must be reduced for them"
        );

        let file = catalog_app(Mode::File, &[]);
        assert!(cayenne_workload(Some(&file)).needs_compaction());
        assert_eq!(count_compaction_eligible_accelerations(Some(&file)), 1);
        assert_eq!(
            plan_cayenne_memory_budgets(Some(&file), &HashMap::new()).compaction_memory_fraction,
            Some(DEFAULT_COMPACTION_MEMORY_FRACTION),
            "a file-mode catalog compacts, so it carves the compaction pool"
        );

        // A catalog without an `acceleration` block configures no Cayenne at all.
        let plain = catalog_test_app(test_catalog(None));
        assert!(!cayenne_workload(Some(&plain)).is_configured());
        assert_eq!(
            estimate_cayenne_reservation_bytes(Some(&plain), &HashMap::new()),
            0
        );
    }

    /// Regression for #12320: a Cayenne deployment with no dataset that can compact
    /// must not carve a compaction memory pool. The carve comes straight out of the
    /// query memory limit, so reserving it for a deployment that cannot use it only
    /// shrinks what queries may reserve.
    ///
    /// The off-pool in-memory CDC tier is gated separately, on the tier being
    /// REACHABLE (`uses_cdc_tier`) rather than on the carve: the query-pool default
    /// is itself reduced to leave room for that tier, so a pod that cannot reach it
    /// would otherwise pay that haircut for nothing.
    #[cfg(not(windows))]
    #[test]
    fn cayenne_memory_budgets_are_reserved_only_for_an_eligible_dataset() {
        use spicepod::acceleration::{Mode, RefreshMode};

        let plan = |datasets, views, params: &[(&str, &str)]| {
            let params: HashMap<String, String> = params
                .iter()
                .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                .collect();
            plan_cayenne_memory_budgets(Some(&cayenne_budget_app(datasets, views)), &params)
        };

        // Full-refresh Cayenne: configured (it still runs Cayenne queries), but it
        // carves nothing and reserves no room for a tier it cannot reach.
        let full_refresh = plan(
            vec![("cayenne", true, Mode::File, Some(RefreshMode::Full), None)],
            0,
            &[],
        );
        assert!(
            full_refresh.cayenne_workload.is_configured(),
            "a full-refresh Cayenne pod is still Cayenne-configured"
        );
        assert_eq!(
            full_refresh.compaction_memory_fraction, None,
            "no acceleration can compact into the carve"
        );
        assert!(
            !full_refresh.cayenne_workload.uses_cdc_tier(),
            "cdc_durability is forced to `file` off the small-write profile, so the pod must not pay the reduced query-pool default that exists to leave room for the tier"
        );

        // The Spicepod default `mode: memory` never compacts, so it carves nothing
        // even on a CDC refresh mode. It DOES reach the in-memory tier — that tier is
        // its permanent store — so the room for it is still reserved.
        let memory_mode = plan(
            vec![(
                "cayenne",
                true,
                Mode::Memory,
                Some(RefreshMode::Changes),
                None,
            )],
            0,
            &[],
        );
        assert!(memory_mode.cayenne_workload.is_configured());
        assert_eq!(memory_mode.compaction_memory_fraction, None);
        assert!(
            memory_mode.cayenne_workload.uses_cdc_tier(),
            "a memory-mode table holds its whole dataset in the tier, so it is the case that most needs the room"
        );

        // A view-only Cayenne pod: configured, nothing eligible.
        let view_only = plan(vec![], 1, &[]);
        assert!(view_only.cayenne_workload.is_configured());
        assert_eq!(view_only.compaction_memory_fraction, None);
        assert!(!view_only.cayenne_workload.uses_cdc_tier());

        // One eligible dataset among ineligible siblings restores the carve.
        let mixed = plan(
            vec![
                ("cayenne", true, Mode::File, Some(RefreshMode::Full), None),
                (
                    "cayenne",
                    true,
                    Mode::File,
                    Some(RefreshMode::Changes),
                    None,
                ),
            ],
            0,
            &[],
        );
        assert!(mixed.cayenne_workload.needs_compaction());
        assert!(mixed.cayenne_workload.uses_cdc_tier());
        assert_eq!(
            mixed.compaction_memory_fraction,
            Some(DEFAULT_COMPACTION_MEMORY_FRACTION)
        );

        // An explicit fraction is still honored (and clamped) when eligible.
        let explicit = plan(
            vec![(
                "cayenne",
                true,
                Mode::File,
                Some(RefreshMode::Changes),
                None,
            )],
            0,
            &[(CAYENNE_COMPACTION_MEMORY_FRACTION_PARAM, "0.1")],
        );
        assert_eq!(
            explicit.compaction_memory_fraction,
            Some(clamp_cayenne_compaction_memory_fraction(0.1))
        );

        // Unchanged pre-existing gate: disabling dedicated thread pools drops the
        // carve. The workload keeps reporting what the spicepod configured — it is a
        // property of the pod, not of the pools — and the DataFusion builder keys the
        // tier budget off the carve too, so neither budget is installed either way.
        let no_pools = plan(
            vec![(
                "cayenne",
                true,
                Mode::File,
                Some(RefreshMode::Changes),
                None,
            )],
            0,
            &[("dedicated_thread_pool", "disabled")],
        );
        assert_eq!(no_pools.compaction_memory_fraction, None);
        assert!(
            !no_pools.dedicated_thread_pools_enabled,
            "the DataFusion builder gates the tier partition on this directly, not on the carve"
        );

        let no_cayenne = plan(
            vec![("duckdb", true, Mode::File, Some(RefreshMode::Changes), None)],
            0,
            &[],
        );
        assert!(!no_cayenne.cayenne_workload.is_configured());
        assert_eq!(no_cayenne.compaction_memory_fraction, None);
    }

    /// A `mode: memory` + `refresh_mode: changes` pod reaches the off-pool in-memory
    /// CDC tier but never produces a file to compact, so `spiced` brings up no
    /// dedicated compaction runtime for it ([`CayenneWorkload::may_compact`] is
    /// false). The aggregate tier byte ceiling must be installed anyway: the
    /// query-pool default has already been reduced to leave room for that tier, and
    /// with no ceiling installed every mem-tier reserve succeeds unconditionally —
    /// the coordinated host partition would hold on paper while the tier grew
    /// unbounded, which is the shape of the SF1000 process OOM it exists to prevent.
    #[cfg(not(windows))]
    #[tokio::test]
    async fn memory_mode_cdc_pod_installs_the_mem_tier_budget_without_a_compaction_runtime() {
        use spicepod::acceleration::{Mode, RefreshMode};

        let workload = cayenne_workload(Some(&cayenne_budget_app(
            vec![(
                "cayenne",
                true,
                Mode::Memory,
                Some(RefreshMode::Changes),
                None,
            )],
            0,
        )));
        assert!(
            workload.uses_cdc_tier(),
            "a memory-mode CDC table holds its whole dataset in the tier"
        );
        assert!(
            !workload.may_compact(),
            "memory mode never writes a Vortex file, so no compaction runtime is brought up"
        );

        let df = crate::datafusion::builder::DataFusionBuilder::new(
            status::RuntimeStatus::new(),
            Arc::new(AcceleratorEngineRegistry::default()),
            Handle::current(),
        )
        .cayenne_workload(workload)
        .build();

        df.install_cayenne_global_budgets();

        assert!(
            cayenne::global_mem_tier_total().is_some_and(|bytes| bytes > 0),
            "the aggregate in-memory CDC tier ceiling must be installed for a pod that has no compaction runtime"
        );
    }

    /// A view carrying its own `acceleration` block creates a `DuckDB` instance just
    /// as a dataset does, so the budget must count it: a view-only pod is coordinated
    /// at all, a mixed pod divides by every instance, and a view shares instance
    /// identity (and the explicit/un-limited classification) with a dataset on the
    /// same file.
    #[cfg(feature = "duckdb")]
    #[test]
    fn test_duckdb_budget_inputs_counts_accelerated_views() {
        use spicepod::acceleration::{Acceleration, Mode};
        use spicepod::component::view::View;

        // A pod whose only DuckDB accelerators are on views is still coordinated —
        // before the fix this reported zero instances, so the builder took its
        // early-out and every view instance kept DuckDB's ~80%-of-RAM default.
        let inputs = budget_inputs_for(
            vec![],
            vec![duckdb_view(
                "sales_summary",
                Mode::File,
                &[("duckdb_file", "/tmp/spice-mbt-view-only.db")],
            )],
        );
        assert_eq!(inputs.num_unset_instances, 1);
        assert_eq!(inputs.num_explicit_instances, 0);
        assert_eq!(
            inputs.unset_instance_labels,
            vec!["/tmp/spice-mbt-view-only.db".to_string()]
        );

        // A mixed pod divides the DuckDB pool by every instance, dataset or view.
        let inputs = budget_inputs_for(
            vec![duckdb_ds(
                "orders",
                Mode::File,
                &[("duckdb_file", "/tmp/spice-mbt-view-ds.db")],
            )],
            vec![duckdb_view(
                "orders_summary",
                Mode::File,
                &[("duckdb_file", "/tmp/spice-mbt-view-v.db")],
            )],
        );
        assert_eq!(inputs.num_unset_instances, 2);

        // A view on the SAME file as a dataset is the SAME instance, not a second one.
        let inputs = budget_inputs_for(
            vec![duckdb_ds(
                "orders",
                Mode::File,
                &[("duckdb_file", "/tmp/spice-mbt-view-shared.db")],
            )],
            vec![duckdb_view(
                "orders_summary",
                Mode::File,
                &[("duckdb_file", "/tmp/spice-mbt-view-shared.db")],
            )],
        );
        assert_eq!(inputs.num_unset_instances, 1);
        assert_eq!(inputs.num_explicit_instances, 0);

        // A memory-mode view joins the one shared in-memory instance.
        let inputs = budget_inputs_for(
            vec![duckdb_ds("orders", Mode::Memory, &[])],
            vec![duckdb_view("orders_summary", Mode::Memory, &[])],
        );
        assert_eq!(inputs.num_unset_instances, 1);

        // A view's explicit `duckdb_memory_limit` counts toward the explicit sum.
        let inputs = budget_inputs_for(
            vec![],
            vec![duckdb_view(
                "sales_summary",
                Mode::File,
                &[
                    ("duckdb_file", "/tmp/spice-mbt-view-explicit.db"),
                    ("duckdb_memory_limit", "2GiB"),
                ],
            )],
        );
        assert_eq!(inputs.num_explicit_instances, 1);
        assert_eq!(inputs.num_unset_instances, 0);
        assert_eq!(inputs.sum_explicit_bytes, 2 * 1024 * 1024 * 1024);

        // An un-limited view sharing a dataset's instance makes that instance mixed:
        // DuckDB's `memory_limit` is per-instance, so the effective limit is ambiguous.
        let inputs = budget_inputs_for(
            vec![duckdb_ds(
                "orders",
                Mode::File,
                &[
                    ("duckdb_file", "/tmp/spice-mbt-view-mixed.db"),
                    ("duckdb_memory_limit", "2GiB"),
                ],
            )],
            vec![duckdb_view(
                "orders_summary",
                Mode::File,
                &[("duckdb_file", "/tmp/spice-mbt-view-mixed.db")],
            )],
        );
        assert_eq!(inputs.num_explicit_instances, 1);
        assert_eq!(inputs.num_unset_instances, 0);
        assert!(
            inputs.has_mixed_instance,
            "an un-limited view on an explicitly-limited instance is a mixed instance"
        );

        // A disabled or non-DuckDB view creates no instance.
        let arrow_view = duckdb_view_with(
            "arrow_summary",
            Acceleration {
                enabled: true,
                engine: None,
                mode: Mode::Memory,
                ..Acceleration::default()
            },
        );
        let mut disabled = duckdb_acceleration(
            Mode::File,
            &[("duckdb_file", "/tmp/spice-mbt-view-disabled.db")],
        );
        disabled.enabled = false;
        let disabled_view = duckdb_view_with("disabled_summary", disabled);
        let unaccelerated_view = View::new("plain_summary".to_string()).with_sql("SELECT 1");
        let inputs = budget_inputs_for(vec![], vec![arrow_view, disabled_view, unaccelerated_view]);
        assert_eq!(inputs.num_unset_instances, 0);
        assert_eq!(inputs.num_explicit_instances, 0);
    }

    #[test]
    fn test_parse_usize_runtime_param() {
        let params = HashMap::from([
            (
                "cayenne_sort_merge_min_rows".to_string(),
                "100000000".to_string(),
            ),
            ("disabled".to_string(), "usize::MAX".to_string()),
            ("bad".to_string(), "not-a-number".to_string()),
        ]);

        assert_eq!(
            parse_usize_runtime_param(&params, "cayenne_sort_merge_min_rows"),
            Some(100_000_000)
        );
        assert_eq!(
            parse_usize_runtime_param(&params, "disabled"),
            Some(usize::MAX)
        );
        assert_eq!(parse_usize_runtime_param(&params, "bad"), None);
        assert_eq!(parse_usize_runtime_param(&params, "missing"), None);
    }

    #[test]
    fn test_parse_f64_runtime_param() {
        let params = HashMap::from([
            (
                "cayenne_sort_merge_memory_pool_fraction".to_string(),
                "0.25".to_string(),
            ),
            ("negative".to_string(), "-1.0".to_string()),
            ("nan".to_string(), "NaN".to_string()),
            ("bad".to_string(), "nope".to_string()),
        ]);

        assert_eq!(
            parse_f64_runtime_param(&params, "cayenne_sort_merge_memory_pool_fraction"),
            Some(0.25)
        );
        assert_eq!(parse_f64_runtime_param(&params, "negative"), None);
        assert_eq!(parse_f64_runtime_param(&params, "nan"), None);
        assert_eq!(parse_f64_runtime_param(&params, "bad"), None);
        assert_eq!(parse_f64_runtime_param(&params, "missing"), None);
    }

    #[test]
    fn known_runtime_params_covers_every_family() {
        // The section vocabulary must include *every* key from *every* family
        // that feeds it — not just a representative key — so a future addition
        // to any family list (e.g. a new `cayenne_*` tunable) can't silently
        // fall out of the merged vocabulary and false-warn on a valid key.
        let known = known_runtime_params();
        let family_keys = KNOWN_CAYENNE_RUNTIME_PARAMS
            .iter()
            .chain(crate::accelerated::refresh_task::changes::CDC_RUNTIME_PARAMS)
            .chain(dataconnector::http_rate_control::HTTP_RATE_CONTROL_RUNTIME_PARAMS)
            .chain(MISC_RUNTIME_PARAMS);
        for key in family_keys {
            assert!(
                known.contains(key),
                "known_runtime_params() missing `{key}`; a valid runtime param would false-warn"
            );
        }
        // No accidental duplicates across the merged family lists.
        let mut deduped = known.clone();
        deduped.sort_unstable();
        deduped.dedup();
        assert_eq!(
            deduped.len(),
            known.len(),
            "duplicate keys in known_runtime_params()"
        );
    }

    #[test]
    fn runtime_param_typos_suggest_within_section() {
        // A typo resolves to the intended key, and the suggestion is drawn from
        // the whole `runtime.params` section vocabulary — across families.
        let known = known_runtime_params();
        assert_eq!(
            util::levenshtein::closest_match("cayenne_footer_cach_mb", &known).as_deref(),
            Some("cayenne_footer_cache_mb"),
        );
        assert_eq!(
            util::levenshtein::closest_match("shuffle_locatin", &known).as_deref(),
            Some("shuffle_location"),
        );
        // A wholly unrelated key gets no misleading suggestion.
        assert_eq!(
            util::levenshtein::closest_match("totally_unrelated_key", &known),
            None,
        );
    }

    #[test]
    fn test_clamp_cayenne_compaction_memory_fraction() {
        for (input, expected) in [(0.0, 0.05), (0.2, 0.2), (1.0, 0.9)] {
            let actual = clamp_cayenne_compaction_memory_fraction(input);
            assert!(
                (actual - expected).abs() < f64::EPSILON,
                "expected {input} to clamp to {expected}, got {actual}"
            );
        }
    }

    #[test]
    fn test_parse_cayenne_filter_propagation() {
        let params = HashMap::from([(
            CAYENNE_FILTER_PROPAGATION_PARAM.to_string(),
            "enabled".to_string(),
        )]);

        assert_eq!(
            parse_cayenne_filter_propagation(&params),
            Some(CayenneFilterPropagation::Enabled)
        );
        assert_eq!(
            parse_cayenne_filter_propagation(&HashMap::from([(
                CAYENNE_FILTER_PROPAGATION_PARAM.to_string(),
                "disabled".to_string(),
            )])),
            Some(CayenneFilterPropagation::Disabled)
        );
        // An invalid value warns and yields `None` (effective behavior is
        // disabled, but nothing was validly applied so callers won't log it).
        assert_eq!(
            parse_cayenne_filter_propagation(&HashMap::from([(
                CAYENNE_FILTER_PROPAGATION_PARAM.to_string(),
                "true".to_string(),
            )])),
            None
        );
        // An unset key also yields `None`.
        assert_eq!(parse_cayenne_filter_propagation(&HashMap::new()), None);
    }

    #[test]
    fn test_parse_cayenne_optimizer_rules() {
        let mut legacy_enabled = CayenneOptimizerRules::auto_enabled();
        legacy_enabled.set_filter_propagation(true);
        legacy_enabled.set_inlist_to_range(true);
        assert_eq!(
            parse_cayenne_optimizer_rules(&HashMap::new(), true),
            legacy_enabled
        );

        let legacy_disabled = CayenneOptimizerRules::auto_enabled();
        assert_eq!(
            parse_cayenne_optimizer_rules(&HashMap::new(), false),
            legacy_disabled
        );

        assert_eq!(
            parse_cayenne_optimizer_rules(
                &HashMap::from([(
                    CAYENNE_OPTIMIZER_RULES_PARAM.to_string(),
                    "none".to_string(),
                )]),
                true,
            ),
            CayenneOptimizerRules::none()
        );
        assert_eq!(
            parse_cayenne_optimizer_rules(
                &HashMap::from([(CAYENNE_OPTIMIZER_RULES_PARAM.to_string(), "all".to_string(),)]),
                false,
            ),
            CayenneOptimizerRules::all_enabled()
        );

        // `join_rewriter` is an opt-in alias that enables the Cayenne
        // `ExactLeftAccumulator` rewrite (`exact_join_filter`) alongside any
        // other named rules, and does not trigger the unknown-rule path.
        let mut selected_rules = CayenneOptimizerRules::none();
        selected_rules.set_filter_propagation(true);
        selected_rules.set_cross_join_reassociation(true);
        selected_rules.set_maintained_aggregate(true);
        selected_rules.set_exact_join_filter(true);
        assert_eq!(
            parse_cayenne_optimizer_rules(
                &HashMap::from([(
                    CAYENNE_OPTIMIZER_RULES_PARAM.to_string(),
                    "filter-propagation,cross_join_reassociation,cdc_aggregates,join_rewriter"
                        .to_string(),
                )]),
                true,
            ),
            selected_rules
        );

        // `semi_join_pushdown` is on under both `auto` and `all`, and is also
        // selectable by token (including its aliases) without enabling anything else.
        assert!(CayenneOptimizerRules::auto_enabled().semi_join_pushdown());
        assert!(CayenneOptimizerRules::all_enabled().semi_join_pushdown());
        let mut semi_join_only = CayenneOptimizerRules::none();
        semi_join_only.set_semi_join_pushdown(true);
        assert_eq!(
            parse_cayenne_optimizer_rules(
                &HashMap::from([(
                    CAYENNE_OPTIMIZER_RULES_PARAM.to_string(),
                    "semi-join".to_string(),
                )]),
                false,
            ),
            semi_join_only
        );

        // `stats_aggregate` is on under both `auto` and `all`, and is also
        // selectable by token (including its aliases) without enabling anything else.
        assert!(CayenneOptimizerRules::auto_enabled().stats_aggregate());
        assert!(CayenneOptimizerRules::all_enabled().stats_aggregate());
        let mut stats_aggregate_only = CayenneOptimizerRules::none();
        stats_aggregate_only.set_stats_aggregate(true);
        assert_eq!(
            parse_cayenne_optimizer_rules(
                &HashMap::from([(
                    CAYENNE_OPTIMIZER_RULES_PARAM.to_string(),
                    "metadata_aggregate".to_string(),
                )]),
                false,
            ),
            stats_aggregate_only
        );

        assert_eq!(
            parse_cayenne_optimizer_rules(
                &HashMap::from([(
                    CAYENNE_OPTIMIZER_RULES_PARAM.to_string(),
                    "not_a_rule".to_string(),
                )]),
                false,
            ),
            legacy_disabled
        );

        // A partially-valid explicit list keeps the recognized rules instead of
        // silently reverting to auto when it hits an unknown token.
        let mut partial_rules = CayenneOptimizerRules::none();
        partial_rules.set_filter_propagation(true);
        assert_eq!(
            parse_cayenne_optimizer_rules(
                &HashMap::from([(
                    CAYENNE_OPTIMIZER_RULES_PARAM.to_string(),
                    "filter_propagation,not_a_rule".to_string(),
                )]),
                false,
            ),
            partial_rules
        );
    }
}
