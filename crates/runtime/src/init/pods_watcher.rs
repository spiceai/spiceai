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

use std::sync::Arc;

use app::{App, AppBuilder};
use spicepod::component::{
    caching::{CacheKeyType, Caching},
    runtime::{
        ApiKeyAuth, Auth, Query, Runtime as SpicepodRuntime, TelemetryConfig, UserAgentCollection,
    },
};

use crate::Runtime;

/// What a reload leaves behind when a start-time-only section changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StartTimeScope {
    /// Nothing about the section applies until spiced restarts.
    Process,
    /// The process-wide knobs need a restart, but a component this reload
    /// recreates reads the section from the new app as it is built.
    ProcessAndRecreatedComponents,
}

/// `runtime.*` sections the process consumes once at startup: a reload installs
/// the new value in the app, but the running process keeps what it started
/// with. Returns every such section that differs, with how much of it a restart
/// is needed for.
fn start_time_only_changes(
    current: &SpicepodRuntime,
    new: &SpicepodRuntime,
) -> Vec<(&'static str, StartTimeScope)> {
    use StartTimeScope::{Process, ProcessAndRecreatedComponents};

    // Destructured exhaustively so a new `runtime.*` field does not compile
    // until it is classified as start-time-only or applied on reload.
    let SpicepodRuntime {
        caching,
        dataset_load_parallelism,
        tls,
        tracing: tracing_config,
        telemetry,
        params,
        task_history,
        auth,
        cors,
        flight,
        mcp,
        // Applied on reload: `shutdown_timeout` is read from the current app
        // when the runtime shuts down, and `functions` is reconciled by
        // `crate::datafusion::udf::apply_function_diff`.
        shutdown_timeout: _,
        ready_state,
        output_level,
        query,
        cpu,
        metrics,
        scheduler,
        source_rate_control,
        drasi,
        functions: _,
    } = new;

    [
        (
            "runtime.caching",
            Process,
            start_time_caching(caching) != start_time_caching(&current.caching),
        ),
        (
            "runtime.dataset_load_parallelism",
            Process,
            *dataset_load_parallelism != current.dataset_load_parallelism,
        ),
        ("runtime.tls", Process, *tls != current.tls),
        (
            "runtime.tracing",
            Process,
            *tracing_config != current.tracing,
        ),
        (
            "runtime.telemetry",
            Process,
            start_time_telemetry(telemetry) != start_time_telemetry(&current.telemetry),
        ),
        (
            "runtime.params",
            ProcessAndRecreatedComponents,
            *params != current.params,
        ),
        (
            "runtime.task_history",
            Process,
            *task_history != current.task_history,
        ),
        (
            "runtime.auth",
            Process,
            start_time_auth(auth.as_ref()) != start_time_auth(current.auth.as_ref()),
        ),
        ("runtime.cors", Process, *cors != current.cors),
        (
            "runtime.flight",
            ProcessAndRecreatedComponents,
            !same_start_time_config(flight.as_ref(), current.flight.as_ref()),
        ),
        (
            "runtime.mcp",
            Process,
            !same_start_time_config(mcp.as_ref(), current.mcp.as_ref()),
        ),
        (
            "runtime.ready_state",
            Process,
            *ready_state != current.ready_state,
        ),
        (
            "runtime.output_level",
            Process,
            !same_start_time_config(output_level.as_ref(), current.output_level.as_ref()),
        ),
        (
            "runtime.query",
            Process,
            start_time_query(query.as_ref()) != start_time_query(current.query.as_ref()),
        ),
        (
            "runtime.cpu",
            Process,
            !same_start_time_config(cpu.as_ref(), current.cpu.as_ref()),
        ),
        ("runtime.metrics", Process, *metrics != current.metrics),
        (
            "runtime.scheduler",
            Process,
            *scheduler != current.scheduler,
        ),
        (
            "runtime.source_rate_control",
            ProcessAndRecreatedComponents,
            !same_start_time_config(
                source_rate_control.as_ref(),
                current.source_rate_control.as_ref(),
            ),
        ),
        // The forwarders are installed once into a `OnceLock` during the initial
        // component load, so a reload leaves the running ones in place.
        ("runtime.drasi", Process, *drasi != current.drasi),
    ]
    .into_iter()
    .filter_map(|(section, scope, changed)| changed.then_some((section, scope)))
    .collect()
}

/// True when two optional sections are the same configuration. An absent section
/// and a present-but-default one are equivalent at the sites reading the
/// sections this is applied to; a section whose reader distinguishes the two —
/// `metrics`, which only reaches the accelerated-table builder when it is
/// present — is compared directly instead.
fn same_start_time_config<T: Default + PartialEq>(current: Option<&T>, new: Option<&T>) -> bool {
    let absent = T::default();
    current.unwrap_or(&absent) == new.unwrap_or(&absent)
}

/// The `auth` configuration `EndpointAuth::new` acts on: an absent section, one
/// without an `api_key`, and one whose `api_key` is disabled all leave the
/// endpoints unauthenticated.
fn start_time_auth(auth: Option<&Auth>) -> Option<&ApiKeyAuth> {
    auth.and_then(|auth| auth.api_key.as_ref())
        .filter(|api_key| api_key.enabled)
}

/// `caching` as [`Runtime::init_caching`] reads it — absent sub-sections take
/// their defaults — without `sql_results.cache_key_type`, which
/// `RequestContextBuilder::build` resolves per request.
fn start_time_caching(caching: &Caching) -> Caching {
    let mut sql_results = caching.sql_results.clone().unwrap_or_default();
    sql_results.cache_key_type = CacheKeyType::default();

    Caching {
        sql_results: Some(sql_results),
        search_results: Some(caching.search_results.clone().unwrap_or_default()),
        embeddings: Some(caching.embeddings.clone().unwrap_or_default()),
    }
}

/// `telemetry` without `user_agent_collection`, which
/// `RequestContextBuilder::from_headers` resolves per request.
fn start_time_telemetry(telemetry: &TelemetryConfig) -> TelemetryConfig {
    let mut telemetry = telemetry.clone();
    telemetry.user_agent_collection = UserAgentCollection::default();
    telemetry
}

/// `query` without `timeout`, which `RequestContextBuilder::build` resolves per
/// request. An absent section is normalized to the default so dropping the
/// whole section to unset `timeout` does not read as a start-time change.
fn start_time_query(query: Option<&Query>) -> Query {
    let mut query = query.cloned().unwrap_or_default();
    query.timeout = None;
    query
}

fn warn_on_start_time_only_changes(current: &SpicepodRuntime, new: &SpicepodRuntime) {
    for (section, scope) in start_time_only_changes(current, new) {
        match scope {
            StartTimeScope::Process => tracing::warn!(
                "`{section}` changed, but it is applied when spiced starts: the previous value stays in effect. Restart spiced to apply it."
            ),
            StartTimeScope::ProcessAndRecreatedComponents => tracing::warn!(
                "`{section}` changed, but it is applied when spiced starts: the previous value stays in effect everywhere except the components this reload recreates. Restart spiced to apply it everywhere."
            ),
        }
    }
}

impl Runtime {
    pub(crate) async fn start_pods_watcher(self: Arc<Self>) -> notify::Result<()> {
        let mut pods_watcher = self.pods_watcher.write().await;
        let Some(mut pods_watcher) = pods_watcher.take() else {
            return Ok(());
        };
        let mut rx = pods_watcher.watch().await?;

        while let Some(new_app_path) = rx.recv().await {
            let new_app = match AppBuilder::build_from_path(new_app_path).await {
                Ok(app) => app,
                Err(e) => {
                    tracing::warn!(
                        "Invalid app state detected, unable to load pods information: {e}"
                    );
                    continue;
                }
            };

            Arc::clone(&self).apply_app(Arc::new(new_app)).await;
        }

        Ok(())
    }

    /// Hot-apply a new [`App`] to the running runtime, reconciling catalogs,
    /// datasets, views, models, functions, and (without the `models` feature)
    /// workers against the currently-loaded app.
    ///
    /// This is the diff-based reconcile the pods watcher performs when a
    /// spicepod file changes on disk, and the one a Spice Cloud deployment takes
    /// when what it changes is confined to the sections reconciled here (see
    /// `spiced`'s `cloud_connect` module).
    ///
    /// Returns `true` if `new_app` differed from the current app and was
    /// applied, `false` if it was identical (a no-op). When there is no
    /// current app yet, `new_app` is installed and `true` is returned.
    ///
    /// Diffs are computed while holding only a read lock on the app; the write
    /// lock is taken only for the final swap. The whole method is serialized by
    /// [`Runtime::apply_app_lock`] so two applies cannot diff against the same
    /// old app, interleave their catalog/dataset/view mutations, and overwrite
    /// `self.app` last-writer-wins. We hold the dedicated mutex (rather than the
    /// app write lock) for the duration so the diff phase can still read the app
    /// `RwLock` without deadlocking.
    pub async fn apply_app(self: Arc<Self>, new_app: Arc<App>) -> bool {
        // Serialize the entire diff-and-swap so concurrent callers apply
        // one-at-a-time. Must be the first statement.
        let _serialize = self.apply_app_lock.lock().await;

        // It is safe to operate by read lock until we actually need to update
        // the app state: with applies serialized by `_serialize`, no other path
        // mutates the app during the diff phase, so a write lock is not needed
        // until the final swap.
        let current_app = self.read_app().await;
        Arc::clone(&self)
            .apply_app_diff(current_app.as_ref(), new_app)
            .await
    }

    /// Diff-and-apply behind [`Runtime::apply_app`].
    ///
    /// The caller holds `apply_app_lock`. `current_app` is what to reconcile
    /// *from*, which is not necessarily the installed app; `new_app` is
    /// installed either way.
    async fn apply_app_diff(
        self: Arc<Self>,
        current_app: Option<&Arc<App>>,
        new_app: Arc<App>,
    ) -> bool {
        if let Some(current_app) = current_app {
            if *current_app == new_app {
                return false;
            }

            tracing::debug!("Updated pods information: {new_app:?}");
            tracing::debug!("Previous pods information: {current_app:?}");

            // Most of `runtime.*` sizes or builds something that is already
            // running. Say so rather than silently ignoring the edit.
            warn_on_start_time_only_changes(&current_app.runtime, &new_app.runtime);

            Arc::clone(&self)
                .apply_catalog_diff(current_app, &new_app)
                .await;
            Arc::clone(&self)
                .apply_dataset_diff(current_app, &new_app)
                .await;
            Arc::clone(&self)
                .apply_view_diff(current_app, &new_app)
                .await;
            self.apply_model_diff(current_app, &new_app).await;
            crate::datafusion::udf::apply_function_diff(&self, current_app, &new_app).await;

            if !cfg!(feature = "models") {
                Arc::clone(&self)
                    .apply_worker_diff(current_app, &new_app)
                    .await;
            }
        }

        *self.app.write().await = Some(new_app);

        true
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use parking_lot::Mutex;
    use spicepod::{
        component::{
            caching::{CacheConfig, SQLResultsCacheConfig},
            runtime::{
                ApiKey, ApiKeyAuth, Auth, Cpu, CpuQuantity, Flight, McpConfig, OutputLevel,
                RuntimeReadyState, Scheduler, SourceRateControl, TlsConfig, TracingConfig,
                default_max_partition_assignments_per_interval,
                default_max_partitions_per_executor, default_partition_assignment_interval,
                default_partition_discovery_timeout,
            },
        },
        drasi::{DrasiForwarding, DrasiTransport, RuntimeDrasi},
        metric::{Metric, Metrics},
    };
    use tracing_subscriber::fmt::MakeWriter;

    use super::{
        Arc, CacheKeyType, Query, SpicepodRuntime, StartTimeScope, UserAgentCollection,
        start_time_only_changes, warn_on_start_time_only_changes,
    };

    /// An edit to one setting of a spicepod `runtime:` section.
    type RuntimeEdit = Box<dyn FnOnce(&mut SpicepodRuntime)>;

    fn changes(mutate: impl FnOnce(&mut SpicepodRuntime)) -> Vec<(&'static str, StartTimeScope)> {
        let current = SpicepodRuntime::default();
        let mut new = current.clone();
        mutate(&mut new);
        start_time_only_changes(&current, &new)
    }

    fn changed_sections(mutate: impl FnOnce(&mut SpicepodRuntime)) -> Vec<&'static str> {
        changes(mutate)
            .into_iter()
            .map(|(section, _)| section)
            .collect()
    }

    fn cpu_cores(quantity: &str) -> CpuQuantity {
        serde_json::from_str(&format!("\"{quantity}\"")).expect("valid CPU quantity")
    }

    fn scheduler(state_location: &str) -> Scheduler {
        Scheduler {
            state_location: state_location.to_string(),
            params: None,
            partition_assignment_interval: default_partition_assignment_interval(),
            max_partition_assignments_per_interval: default_max_partition_assignments_per_interval(
            ),
            max_partitions_per_executor: default_max_partitions_per_executor(),
            partition_discovery_timeout: default_partition_discovery_timeout(),
        }
    }

    fn tls(enabled: bool) -> TlsConfig {
        TlsConfig {
            enabled,
            certificate_file: None,
            certificate: None,
            key_file: None,
            key: None,
            client_auth_ca_file: None,
            client_auth_ca: None,
            client_auth_mode: None,
        }
    }

    /// Every section that must be reported, with an edit that changes it.
    fn start_time_only_edits() -> Vec<(&'static str, RuntimeEdit)> {
        vec![
            (
                "runtime.caching",
                Box::new(|rt: &mut SpicepodRuntime| {
                    rt.caching.sql_results = Some(SQLResultsCacheConfig {
                        enabled: false,
                        ..SQLResultsCacheConfig::default()
                    });
                }),
            ),
            (
                "runtime.dataset_load_parallelism",
                Box::new(|rt: &mut SpicepodRuntime| rt.dataset_load_parallelism = Some(4)),
            ),
            (
                "runtime.tls",
                Box::new(|rt: &mut SpicepodRuntime| rt.tls = Some(tls(true))),
            ),
            (
                "runtime.tracing",
                Box::new(|rt: &mut SpicepodRuntime| {
                    rt.tracing = Some(TracingConfig {
                        zipkin_enabled: true,
                        zipkin_endpoint: None,
                    });
                }),
            ),
            (
                "runtime.telemetry",
                Box::new(|rt: &mut SpicepodRuntime| rt.telemetry.enabled = false),
            ),
            (
                "runtime.params",
                Box::new(|rt: &mut SpicepodRuntime| {
                    rt.params
                        .insert("url_tables".to_string(), "enabled".to_string());
                }),
            ),
            (
                "runtime.task_history",
                Box::new(|rt: &mut SpicepodRuntime| rt.task_history.enabled = false),
            ),
            (
                "runtime.auth",
                Box::new(|rt: &mut SpicepodRuntime| {
                    rt.auth = Some(Auth {
                        api_key: Some(ApiKeyAuth {
                            enabled: true,
                            keys: vec![ApiKey::parse_str("secret:rw")],
                        }),
                    });
                }),
            ),
            (
                "runtime.cors",
                Box::new(|rt: &mut SpicepodRuntime| rt.cors.enabled = true),
            ),
            (
                "runtime.flight",
                Box::new(|rt: &mut SpicepodRuntime| {
                    rt.flight = Some(Flight {
                        do_put_rate_limit_enabled: false,
                        ..Flight::default()
                    });
                }),
            ),
            (
                "runtime.mcp",
                Box::new(|rt: &mut SpicepodRuntime| {
                    rt.mcp = Some(McpConfig {
                        allowed_hosts: Some(vec!["*".to_string()]),
                    });
                }),
            ),
            (
                "runtime.ready_state",
                Box::new(|rt: &mut SpicepodRuntime| {
                    rt.ready_state = RuntimeReadyState::OnRegistration;
                }),
            ),
            (
                "runtime.output_level",
                Box::new(|rt: &mut SpicepodRuntime| rt.output_level = Some(OutputLevel::Verbose)),
            ),
            (
                "runtime.query",
                Box::new(|rt: &mut SpicepodRuntime| {
                    rt.query = Some(Query {
                        memory_limit: Some("1GiB".to_string()),
                        ..Query::default()
                    });
                }),
            ),
            (
                "runtime.cpu",
                Box::new(|rt: &mut SpicepodRuntime| {
                    rt.cpu = Some(Cpu {
                        cores: Some(cpu_cores("4")),
                    });
                }),
            ),
            (
                "runtime.metrics",
                Box::new(|rt: &mut SpicepodRuntime| {
                    rt.metrics = Some(Metrics {
                        metrics: vec![Metric {
                            enabled: true,
                            name: "query_duration_ms".to_string(),
                        }],
                    });
                }),
            ),
            (
                "runtime.scheduler",
                Box::new(|rt: &mut SpicepodRuntime| {
                    rt.scheduler = Some(scheduler("s3://bucket/state"));
                }),
            ),
            (
                "runtime.source_rate_control",
                Box::new(|rt: &mut SpicepodRuntime| {
                    rt.source_rate_control = Some(SourceRateControl {
                        github_concurrent_connections_limit: Some(2),
                        ..SourceRateControl::default()
                    });
                }),
            ),
            (
                "runtime.drasi",
                Box::new(|rt: &mut SpicepodRuntime| {
                    rt.drasi = Some(RuntimeDrasi {
                        source_id: "spice-runtime".to_string(),
                        forwarding: DrasiForwarding::default(),
                        tables: Vec::new(),
                        transport: DrasiTransport::default(),
                        params: None,
                    });
                }),
            ),
        ]
    }

    #[test]
    fn each_start_time_only_section_is_reported_by_name() {
        for (section, edit) in start_time_only_edits() {
            assert_eq!(
                changed_sections(edit),
                vec![section],
                "editing {section} must report {section} and nothing else"
            );
        }
    }

    #[test]
    fn an_unchanged_runtime_reports_nothing() {
        assert!(changed_sections(|_| {}).is_empty());
    }

    /// Settings that apply on a reload: reporting them would tell the operator
    /// to restart for a change that already took effect.
    #[test]
    fn settings_applied_on_reload_are_not_reported() {
        let reload_edits: Vec<(&str, RuntimeEdit)> = vec![
            (
                "runtime.shutdown_timeout",
                Box::new(|rt: &mut SpicepodRuntime| {
                    rt.shutdown_timeout = Some("30s".to_string());
                }),
            ),
            (
                "runtime.functions.enabled",
                Box::new(|rt: &mut SpicepodRuntime| rt.functions.enabled = true),
            ),
            (
                "runtime.query.timeout",
                Box::new(|rt: &mut SpicepodRuntime| {
                    rt.query = Some(Query {
                        timeout: Some("30s".to_string()),
                        ..Query::default()
                    });
                }),
            ),
            (
                "runtime.caching.sql_results.cache_key_type",
                Box::new(|rt: &mut SpicepodRuntime| {
                    if let Some(sql_results) = rt.caching.sql_results.as_mut() {
                        sql_results.cache_key_type = CacheKeyType::Sql;
                    }
                }),
            ),
            (
                "runtime.telemetry.user_agent_collection",
                Box::new(|rt: &mut SpicepodRuntime| {
                    rt.telemetry.user_agent_collection = UserAgentCollection::Disabled;
                }),
            ),
        ];

        for (setting, edit) in reload_edits {
            assert_eq!(
                changed_sections(edit),
                Vec::<&str>::new(),
                "{setting} applies on a reload and must not be reported"
            );
        }
    }

    /// Writing a section out with the values it already had by default is not a
    /// configuration change, and reporting it would send the operator to restart
    /// a process that is already running what the file asks for.
    #[test]
    fn spelling_a_section_out_at_its_defaults_is_not_reported() {
        let default_edits: Vec<(&str, RuntimeEdit)> = vec![
            (
                "runtime.flight",
                Box::new(|rt: &mut SpicepodRuntime| rt.flight = Some(Flight::default())),
            ),
            (
                "runtime.mcp",
                Box::new(|rt: &mut SpicepodRuntime| rt.mcp = Some(McpConfig::default())),
            ),
            (
                "runtime.cpu",
                Box::new(|rt: &mut SpicepodRuntime| rt.cpu = Some(Cpu::default())),
            ),
            (
                "runtime.source_rate_control",
                Box::new(|rt: &mut SpicepodRuntime| {
                    rt.source_rate_control = Some(SourceRateControl::default());
                }),
            ),
            (
                "runtime.auth",
                Box::new(|rt: &mut SpicepodRuntime| rt.auth = Some(Auth { api_key: None })),
            ),
            (
                "runtime.auth with its api key disabled",
                Box::new(|rt: &mut SpicepodRuntime| {
                    rt.auth = Some(Auth {
                        api_key: Some(ApiKeyAuth {
                            enabled: false,
                            keys: vec![ApiKey::parse_str("secret:rw")],
                        }),
                    });
                }),
            ),
            (
                "runtime.output_level",
                Box::new(|rt: &mut SpicepodRuntime| {
                    rt.output_level = Some(OutputLevel::default());
                }),
            ),
            (
                "runtime.query",
                Box::new(|rt: &mut SpicepodRuntime| rt.query = Some(Query::default())),
            ),
            (
                "runtime.caching",
                Box::new(|rt: &mut SpicepodRuntime| {
                    rt.caching.sql_results = Some(SQLResultsCacheConfig::default());
                    rt.caching.search_results = Some(CacheConfig::default());
                    rt.caching.embeddings = Some(CacheConfig::default());
                }),
            ),
            (
                "runtime.caching with its sub-sections dropped",
                Box::new(|rt: &mut SpicepodRuntime| {
                    rt.caching.search_results = None;
                    rt.caching.embeddings = None;
                }),
            ),
        ];

        for (setting, edit) in default_edits {
            assert_eq!(
                changed_sections(edit),
                Vec::<&str>::new(),
                "{setting} at its defaults is the configuration already running"
            );
        }
    }

    /// The sections a reload partly applies must not claim the old value is
    /// still in effect everywhere.
    #[test]
    fn sections_recreated_components_re_read_are_reported_as_such() {
        let partly_applied: Vec<(&str, RuntimeEdit)> = vec![
            (
                "runtime.params",
                Box::new(|rt: &mut SpicepodRuntime| {
                    rt.params
                        .insert("url_tables".to_string(), "enabled".to_string());
                }),
            ),
            (
                "runtime.source_rate_control",
                Box::new(|rt: &mut SpicepodRuntime| {
                    rt.source_rate_control = Some(SourceRateControl {
                        github_concurrent_connections_limit: Some(2),
                        ..SourceRateControl::default()
                    });
                }),
            ),
            (
                "runtime.flight",
                Box::new(|rt: &mut SpicepodRuntime| {
                    rt.flight = Some(Flight {
                        max_message_size: Some("100MiB".to_string()),
                        ..Flight::default()
                    });
                }),
            ),
        ];

        for (section, edit) in partly_applied {
            assert_eq!(
                changes(edit),
                vec![(section, StartTimeScope::ProcessAndRecreatedComponents)],
                "a connector recreated by the reload re-reads {section}"
            );
        }

        assert_eq!(
            changes(|rt| rt.cors.enabled = true),
            vec![("runtime.cors", StartTimeScope::Process)]
        );
    }

    /// A section only reads as changed when its start-time settings change, not
    /// when a per-request setting inside it does.
    #[test]
    fn a_section_is_reported_once_its_start_time_settings_change() {
        assert_eq!(
            changed_sections(|rt| {
                rt.query = Some(Query {
                    timeout: Some("30s".to_string()),
                    target_partitions: Some(8),
                    ..Query::default()
                });
            }),
            vec!["runtime.query"]
        );
    }

    #[test]
    fn every_changed_section_is_warned_about_by_name() {
        let current = SpicepodRuntime::default();
        let mut new = current.clone();
        new.cors.enabled = true;
        new.ready_state = RuntimeReadyState::OnRegistration;

        let warnings = capture_warnings(&current, &new);

        assert!(
            warnings.contains("`runtime.cors` changed"),
            "expected a `runtime.cors` warning, got: {warnings}"
        );
        assert!(
            warnings.contains("`runtime.ready_state` changed"),
            "expected a `runtime.ready_state` warning, got: {warnings}"
        );
        assert!(
            warnings.contains("Restart spiced to apply it."),
            "expected the warning to say a restart is required, got: {warnings}"
        );
        assert!(
            !warnings.contains("`runtime.tls`"),
            "unchanged sections must not be warned about, got: {warnings}"
        );
    }

    #[test]
    fn a_partly_applied_section_is_warned_about_without_claiming_nothing_applied() {
        let current = SpicepodRuntime::default();
        let mut new = current.clone();
        new.params
            .insert("url_tables".to_string(), "enabled".to_string());
        new.source_rate_control = Some(SourceRateControl {
            github_concurrent_connections_limit: Some(2),
            ..SourceRateControl::default()
        });

        let warnings = capture_warnings(&current, &new);

        for section in ["runtime.params", "runtime.source_rate_control"] {
            let reported = warnings
                .lines()
                .find(|line| line.contains(&format!("`{section}` changed")))
                .unwrap_or_default();
            assert!(
                reported.contains("except the components this reload recreates"),
                "expected {section} to be reported as only partly applied, got: {warnings}"
            );
        }
    }

    #[test]
    fn an_unchanged_runtime_warns_about_nothing() {
        let current = SpicepodRuntime::default();
        let warnings = capture_warnings(&current, &current.clone());
        assert!(warnings.is_empty(), "expected no warnings, got: {warnings}");
    }

    #[derive(Clone, Default)]
    struct CapturedLogs(Arc<Mutex<Vec<u8>>>);

    impl Write for CapturedLogs {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> MakeWriter<'a> for CapturedLogs {
        type Writer = Self;

        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    fn capture_warnings(current: &SpicepodRuntime, new: &SpicepodRuntime) -> String {
        let logs = CapturedLogs::default();
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .with_writer(logs.clone())
            .finish();
        tracing::subscriber::with_default(subscriber, || {
            warn_on_start_time_only_changes(current, new);
        });
        let captured = logs.0.lock().clone();
        String::from_utf8_lossy(&captured).into_owned()
    }
}
