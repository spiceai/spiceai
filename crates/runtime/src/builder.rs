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
use crate::component::dataset::acceleration::Engine;
use crate::config::ClusterRole;
use crate::config::Config;
#[cfg(not(windows))]
use crate::dataaccelerator::cayenne::CayenneAccelerator;
use crate::datafusion::builder::CayenneOptimizerRules;
use crate::datafusion::udf::register_udfs;
use crate::metrics_reader::MetricsReader;
use crate::{
    Runtime, catalogconnector,
    dataaccelerator::AcceleratorEngineRegistry,
    dataconnector,
    datafusion::DataFusion,
    datasets_health_monitor::DatasetsHealthMonitor,
    extension::{Extension, ExtensionFactory},
    flight::RateLimits,
    metrics, podswatcher,
    secrets::{self, Secrets},
    status, tracers,
};
use app::App;
use spicepod::component::runtime::Runtime as SpicepodRuntime;
use spicepod::component::runtime::RuntimeReadyState as SpicepodRuntimeReadyState;
use spicepod::component::runtime::SourceRateControl as SpicepodSourceRateControl;
use spicepod::component::runtime::TelemetryConfig;
use std::{collections::HashMap, net::SocketAddr, str::FromStr, sync::Arc, time::Duration};
use telemetry::timing::TimeMeasurement;
use token_provider::registry::TokenProviderRegistry;
use tokio::runtime::Handle;
use tokio::sync::{Mutex, RwLock};
use util::{in_tracing_context, in_tracing_context_async};

type DatafusionConfigurationCallback = fn(&mut DataFusion);

const CAYENNE_FOOTER_CACHE_MB_PARAM: &str = "cayenne_footer_cache_mb";
/// Runtime param: fraction of `runtime.query.memory_limit` carved into a
/// dedicated Cayenne compaction memory pool when Cayenne acceleration is
/// configured on a dataset and dedicated thread pools are enabled.
const CAYENNE_COMPACTION_MEMORY_FRACTION_PARAM: &str = "cayenne_compaction_memory_fraction";
/// Default carve fraction when the param is unset: 20% of the query budget to
/// compaction, 80% retained for queries.
const DEFAULT_COMPACTION_MEMORY_FRACTION: f64 = 0.2;
const MIN_COMPACTION_MEMORY_FRACTION: f64 = 0.05;
const MAX_COMPACTION_MEMORY_FRACTION: f64 = 0.9;

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

    pub fn with_app(mut self, app: app::App) -> Self {
        self.app = Some(Arc::new(app));
        self
    }

    pub fn with_app_opt(mut self, app: Option<Arc<app::App>>) -> Self {
        self.app = app;
        self
    }

    pub fn with_runtime_config(mut self, config: Config) -> Self {
        self.runtime_config = Arc::new(config);
        self
    }

    pub fn with_extensions(mut self, extensions: Vec<Box<dyn ExtensionFactory>>) -> Self {
        self.extensions = extensions;
        self
    }

    /// Extensions that will be automatically loaded if a component requests them and the user hasn't explicitly loaded it.
    pub fn with_autoload_extensions(
        mut self,
        extensions: HashMap<String, Box<dyn ExtensionFactory>>,
    ) -> Self {
        self.autoload_extensions = extensions;
        self
    }

    pub fn with_pods_watcher(mut self, pods_watcher: podswatcher::PodsWatcher) -> Self {
        self.pods_watcher = Some(pods_watcher);
        self
    }

    pub fn with_datasets_health_monitor(mut self) -> Self {
        self.datasets_health_monitor_enabled = true;
        self
    }

    pub fn with_metrics_server(
        mut self,
        metrics_endpoint: SocketAddr,
        prometheus_registry: prometheus::Registry,
    ) -> Self {
        self.metrics_endpoint = Some(metrics_endpoint);
        self.prometheus_registry = Some(prometheus_registry);
        self
    }

    pub fn with_metrics_server_opt(
        mut self,
        metrics_endpoint: Option<SocketAddr>,
        prometheus_registry: Option<prometheus::Registry>,
    ) -> Self {
        self.metrics_endpoint = metrics_endpoint;
        self.prometheus_registry = prometheus_registry;
        self
    }

    pub fn with_rate_limits(mut self, rate_limits: RateLimits) -> Self {
        self.rate_limits = Some(Arc::new(rate_limits));
        self
    }

    pub fn with_io_runtime(mut self, io_runtime: Handle) -> Self {
        self.io_runtime = Some(io_runtime);
        self
    }

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
    pub fn with_metrics_reader(mut self, metrics_reader: MetricsReader) -> Self {
        self.metrics_reader = Some(metrics_reader);
        self
    }

    pub async fn build(self) -> Runtime {
        // Initialize DataFusion tracer for span context propagation across async boundaries
        if let Err(e) = tracers::init_datafusion_tracer() {
            tracing::warn!(
                "Failed to initialize DataFusion tracer: {e}. Span context may not propagate correctly across async boundaries."
            );
        }

        self.accelerator_engine_registry.register_all().await;
        dataconnector::register_all().await;
        catalogconnector::register_all().await;
        document_parse::register_all().await;

        // Resolve the effective spicepod runtime config: config override > app > default.
        let spicepod_rt = self.runtime_config.runtime.clone().unwrap_or_else(|| {
            self.app
                .as_ref()
                .map_or(SpicepodRuntime::default(), |app| app.runtime.clone())
        });

        let query = spicepod_rt.query.clone().unwrap_or_default();

        let memory_limit = parse_memory_limit(query.memory_limit.clone());
        let target_partitions = query.target_partitions;

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
        let cayenne_sort_merge_min_rows =
            parse_usize_runtime_param(&spicepod_rt.params, "cayenne_sort_merge_min_rows");
        let cayenne_sort_merge_memory_pool_fraction = parse_f64_runtime_param(
            &spicepod_rt.params,
            "cayenne_sort_merge_memory_pool_fraction",
        );
        let cayenne_footer_cache_mb =
            parse_usize_runtime_param(&spicepod_rt.params, CAYENNE_FOOTER_CACHE_MB_PARAM);
        let cayenne_filter_propagation_enabled =
            parse_cayenne_filter_propagation(&spicepod_rt.params).is_enabled();
        let cayenne_optimizer_rules =
            parse_cayenne_optimizer_rules(&spicepod_rt.params, cayenne_filter_propagation_enabled);

        // Carve a dedicated compaction memory pool only when Cayenne acceleration
        // is configured (and enabled) on a dataset AND dedicated thread pools are
        // enabled. This keeps non-Cayenne deployments at full query budget and
        // matches the dedicated compaction runtime's "create only if Cayenne is
        // enabled" lifecycle — the carved env is the signal spiced uses to bring
        // up the compaction worker threads.
        let cayenne_configured = self.app.as_ref().is_some_and(|app| {
            app.datasets.iter().any(|dataset| {
                dataset.acceleration.as_ref().is_some_and(|accel| {
                    accel.enabled
                        && accel
                            .engine
                            .as_deref()
                            .is_some_and(|engine| engine.eq_ignore_ascii_case("cayenne"))
                })
            })
        });
        let dedicated_thread_pools_enabled = !matches!(
            spicepod_rt
                .params
                .get("dedicated_thread_pool")
                .map(String::as_str),
            Some("disabled")
        );
        let compaction_memory_fraction = (cayenne_configured && dedicated_thread_pools_enabled)
            .then(|| {
                let requested = parse_f64_runtime_param(
                    &spicepod_rt.params,
                    CAYENNE_COMPACTION_MEMORY_FRACTION_PARAM,
                )
                .unwrap_or(DEFAULT_COMPACTION_MEMORY_FRACTION);
                clamp_cayenne_compaction_memory_fraction(requested)
            });

        #[cfg(not(windows))]
        if cayenne_footer_cache_mb.is_some() {
            self.accelerator_engine_registry
                .register_accelerator_engine(
                    Engine::Cayenne,
                    Arc::new(CayenneAccelerator::with_footer_cache_mb(
                        cayenne_footer_cache_mb,
                    )),
                )
                .await;
        }

        let caching = Runtime::init_caching(Some(&spicepod_rt.caching));
        let io_runtime = self.io_runtime.clone().unwrap_or_else(|| Handle::current());

        // Resolve CDC tunables once at startup so the per-envelope hot path
        // doesn't pay map lookup or spicepod-traversal cost. Reads
        // `cdc_*` knobs from `runtime.params`; missing or rejected values
        // fall back to the matching `SPICE_CDC_*` env var, then to defaults,
        // with a warning for rejected explicit values.
        crate::accelerated_table::refresh_task::changes::set_cdc_config(
            crate::accelerated_table::refresh_task::changes::cdc_config_from_params(
                &spicepod_rt.params,
            ),
        );

        // Create resource monitor early so it can be passed to DataFusion
        let resource_monitor = crate::resource_monitor::ResourceMonitor::new();
        let secrets = Arc::new(RwLock::new(Self::load_secrets(self.app.as_ref()).await));

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
            df,
            models: Arc::new(RwLock::new(HashMap::new())),
            completion_llms: Arc::new(RwLock::new(HashMap::new())),
            model_rate_controllers: Arc::new(RwLock::new(HashMap::new())),
            http_rate_control_registry,
            responses_llms: Arc::new(RwLock::new(HashMap::new())),
            workers: Arc::new(RwLock::new(HashMap::new())),
            embeds: Arc::new(RwLock::new(HashMap::new())),
            rerankers: Arc::new(RwLock::new(HashMap::new())),
            tools: Arc::new(RwLock::new(HashMap::new())),
            tool_factories: Arc::new(Mutex::new(HashMap::new())),
            pods_watcher: Arc::new(RwLock::new(self.pods_watcher)),
            secrets,
            spaced_tracer: Arc::new(tracers::SpacedTracer::new(Duration::from_secs(15))),
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

        let mut extensions: HashMap<String, Arc<dyn Extension>> = HashMap::new();
        for factory in self.extensions {
            let mut extension = factory.create();
            let extension_name = extension.name();
            if let Err(err) = extension.initialize(&rt).await {
                eprintln!("Failed to initialize extension {extension_name}: {err}");
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

        if let Some(app) = app {
            // `load_secrets` runs before `spiced::init_tracing` installs the
            // global subscriber, so any `tracing::*` events emitted by
            // `Secrets::load_from` and the per-store `init()` paths would
            // otherwise be dropped on the floor. That hides actionable errors
            // like "Vault address unreachable" or "AWS credentials missing"
            // and leaves the operator with only the downstream
            // "undefined store" message at lookup time. Wrap the await in a
            // temporary subscriber so those diagnostics surface.
            if let Err(e) = in_tracing_context_async(secrets.load_from(&app.secrets)).await {
                eprintln!("Error loading secret stores: {e}");
            }
        }

        secrets
    }
}

impl Default for RuntimeBuilder {
    fn default() -> Self {
        Self::new()
    }
}

async fn build_http_rate_control_registry(
    source_rate_control: Option<&SpicepodSourceRateControl>,
    secrets: Arc<RwLock<Secrets>>,
    io_runtime: Handle,
) -> Arc<dataconnector::http_rate_control::HttpRateControlRegistry> {
    #[cfg(not(feature = "rate-control"))]
    {
        let _ = (&secrets, &io_runtime);
        if source_rate_control
            .and_then(|config| config.state_location.as_ref())
            .is_some()
        {
            tracing::warn!(
                "Persisted HTTP governor rate-control state requires a Spice.ai Enterprise build. Falling back to in-memory HTTP rate-control state."
            );
        }
        return Arc::new(dataconnector::http_rate_control::HttpRateControlRegistry::default());
    }

    #[cfg(feature = "rate-control")]
    {
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

        let Some(refresh_interval) =
            parse_rate_control_refresh_interval(refresh_interval, config_path)
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
        in_tracing_context(|| {
            tracing::warn!(
                "An invalid Runtime memory limit was specified: {original_memory_limit} A memory limit must be specified as an integer in GB, MB, or KB size."
            );
        });
    }

    if memory_limit == Some(0) {
        in_tracing_context(|| {
            tracing::warn!(
                "A Runtime memory limit of 0 was specified: {original_memory_limit} A memory limit must be greater than 0."
            );
        });
        None
    } else {
        memory_limit
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

const CAYENNE_FILTER_PROPAGATION_PARAM: &str = "cayenne_filter_propagation";
const CAYENNE_OPTIMIZER_RULES_PARAM: &str = "cayenne_optimizer_rules";

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

fn parse_cayenne_filter_propagation(params: &HashMap<String, String>) -> CayenneFilterPropagation {
    let Some(raw) = params.get(CAYENNE_FILTER_PROPAGATION_PARAM) else {
        return CayenneFilterPropagation::Disabled;
    };

    match raw.trim().to_ascii_lowercase().as_str() {
        "enabled" => CayenneFilterPropagation::Enabled,
        "disabled" => CayenneFilterPropagation::Disabled,
        _ => {
            tracing::warn!(
                "runtime.params.{CAYENNE_FILTER_PROPAGATION_PARAM}={raw:?} must be 'enabled' or 'disabled'; using disabled"
            );
            CayenneFilterPropagation::Disabled
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
            "dynamic_filter_sharing" | "dynamic_filters" => {
                rules.set_dynamic_filter_sharing(true);
            }
            "anti_join_sort_merge" | "anti_sort_merge" => {
                rules.set_anti_join_sort_merge(true);
            }
            "exact_join_filter" | "join_rewriter" | "exact_accumulator" => {
                rules.set_exact_join_filter(true);
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
    fn test_clamp_cayenne_compaction_memory_fraction() {
        assert_eq!(clamp_cayenne_compaction_memory_fraction(0.0), 0.05);
        assert_eq!(clamp_cayenne_compaction_memory_fraction(0.2), 0.2);
        assert_eq!(clamp_cayenne_compaction_memory_fraction(1.0), 0.9);
    }

    #[test]
    fn test_parse_cayenne_filter_propagation() {
        let params = HashMap::from([(
            CAYENNE_FILTER_PROPAGATION_PARAM.to_string(),
            "enabled".to_string(),
        )]);

        assert_eq!(
            parse_cayenne_filter_propagation(&params),
            CayenneFilterPropagation::Enabled
        );
        assert_eq!(
            parse_cayenne_filter_propagation(&HashMap::from([(
                CAYENNE_FILTER_PROPAGATION_PARAM.to_string(),
                "disabled".to_string(),
            )])),
            CayenneFilterPropagation::Disabled
        );
        assert_eq!(
            parse_cayenne_filter_propagation(&HashMap::from([(
                CAYENNE_FILTER_PROPAGATION_PARAM.to_string(),
                "true".to_string(),
            )])),
            CayenneFilterPropagation::Disabled
        );
        assert_eq!(
            parse_cayenne_filter_propagation(&HashMap::new()),
            CayenneFilterPropagation::Disabled
        );
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

        let mut selected_rules = CayenneOptimizerRules::none();
        selected_rules.set_filter_propagation(true);
        selected_rules.set_cross_join_reassociation(true);
        selected_rules.set_exact_join_filter(true);
        assert_eq!(
            parse_cayenne_optimizer_rules(
                &HashMap::from([(
                    CAYENNE_OPTIMIZER_RULES_PARAM.to_string(),
                    "filter-propagation,cross_join_reassociation,join_rewriter".to_string(),
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
