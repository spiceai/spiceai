/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! `runtime.adaptive_tuning` reaches the tables of a Cayenne catalog.
//!
//! A Cayenne catalog only runs in a cluster, so each test starts a real scheduler and
//! executor, registers the catalog, creates a table through it, and reads whether that
//! table's Cayenne provider runs the closed-loop tuner.

#![cfg(not(windows))]
#![expect(clippy::expect_used, dead_code)]

// Accelerator engines self-register through a linkme slice; the linker drops an unreferenced
// slice static, so a binary exercising Cayenne must name the crate itself.
use accelerator_cayenne as _;

#[path = "cluster/harness.rs"]
mod harness;

use std::collections::HashMap;
use std::io::Write;
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use app::AppBuilder;
use datafusion::common::TableReference;
use harness::ClusterHarness;
use parking_lot::Mutex;
use spicepod::component::access::AccessMode;
use spicepod::component::catalog::Catalog;
use spicepod::component::runtime::AdaptiveTuning;
use spicepod::param::Params;
use tracing_subscriber::fmt::MakeWriter;

/// Everything logged by any thread of this test binary; the catalog is registered on a
/// worker thread, so a thread-local subscriber would miss its warnings.
static LOGS: LazyLock<Arc<Mutex<Vec<u8>>>> = LazyLock::new(|| {
    let logs = Arc::new(Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::fmt()
        .with_ansi(false)
        .with_writer(CapturedLogs(Arc::clone(&logs)))
        .finish();
    tracing::subscriber::set_global_default(subscriber)
        .expect("the log capture must be the only global subscriber");
    logs
});

#[derive(Clone)]
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

fn logged() -> String {
    String::from_utf8_lossy(&LOGS.lock()).into_owned()
}

fn cayenne_catalog(name: &str, dir: &std::path::Path, extra: &[(&str, &str)]) -> Catalog {
    let mut params: HashMap<String, String> = HashMap::from([
        (
            "cayenne_data_dir".to_string(),
            dir.join("data").to_string_lossy().into_owned(),
        ),
        (
            "cayenne_metadata_dir".to_string(),
            dir.join("metadata").to_string_lossy().into_owned(),
        ),
    ]);
    params.extend(
        extra
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string())),
    );
    let mut catalog = Catalog::new("cayenne".to_string(), name.to_string())
        .with_access(AccessMode::ReadWriteCreate);
    catalog.params = Some(Params::from_string_map(params));
    catalog
}

/// The tuning a catalog table resolved.
#[derive(Debug, PartialEq)]
struct Resolved {
    dynamic_tuning: bool,
    replication_lag_secs: Option<f64>,
    freshness_secs: Option<f64>,
    query_latency_ms: Option<f64>,
    convergence_window_secs: Option<f64>,
    qph: Option<f64>,
}

/// Starts a scheduler and executor sharing a Cayenne catalog called `tcat`, creates
/// `tcat.s.t`, and returns whether that table runs the closed-loop tuner.
async fn catalog_table_tuning(
    adaptive_tuning: AdaptiveTuning,
    runtime_params: &[(&str, &str)],
    catalog_params: &[(&str, &str)],
) -> Resolved {
    LazyLock::force(&LOGS);
    let dir = tempfile::tempdir().expect("a temp dir must be created");
    let catalog = cayenne_catalog("tcat", dir.path(), catalog_params);

    let mut scheduler = AppBuilder::new("catalog_tuning_scheduler")
        .with_catalog(catalog.clone())
        .build();
    scheduler.runtime.adaptive_tuning = adaptive_tuning;
    for (key, value) in runtime_params {
        scheduler
            .runtime
            .params
            .insert((*key).to_string(), (*value).to_string());
    }
    let executor = AppBuilder::new("catalog_tuning_executor")
        .with_catalog(catalog)
        .build();

    let harness = ClusterHarness::builder()
        .scheduler(scheduler)
        .executor_with_app(executor)
        .start()
        .await
        .unwrap_or_else(|e| panic!("the cluster must start: {e}\n{}", logged()));
    harness
        .wait_for_executors(Duration::from_secs(60))
        .await
        .expect("the executor must register");

    harness
        .query("CREATE SCHEMA tcat.s")
        .await
        .expect("the schema must be created");
    harness
        .query("CREATE TABLE tcat.s.t (id BIGINT NOT NULL, v VARCHAR, PRIMARY KEY (id)) PARTITION BY id")
        .await
        .expect("the table must be created");

    // Partitions are created by the first write.
    harness
        .query("INSERT INTO tcat.s.t VALUES (1, 'a')")
        .await
        .expect("the row must be inserted");

    // The scheduler routes the write and the executor opens the partition, so the
    // table the tuner runs on lives on the executor once a query has reached it.
    harness
        .query("SELECT COUNT(*) FROM tcat.s.t")
        .await
        .expect("the table must be queryable");
    let mut partitions: Vec<Arc<dyn datafusion::catalog::TableProvider>> = Vec::new();
    for runtime in std::iter::once(&harness.scheduler).chain(&harness.executors) {
        let Some(table) = runtime
            .datafusion()
            .get_table(&TableReference::full("tcat", "s", "t"))
            .await
        else {
            continue;
        };
        if let Some(partitioned) =
            table.downcast_ref::<runtime_table_partition::provider::PartitionTableProvider>()
        {
            partitions.extend(partitioned.partition_table_providers().await);
        } else if table
            .downcast_ref::<cayenne::CayenneTableProvider>()
            .is_some()
        {
            partitions.push(table);
        }
    }
    assert!(!partitions.is_empty(), "the insert must create a partition");
    let resolved = partitions
        .iter()
        .map(|partition| {
            let config = &partition
                .downcast_ref::<cayenne::CayenneTableProvider>()
                .expect("every partition must be a Cayenne table")
                .metadata()
                .vortex_config;
            Resolved {
                dynamic_tuning: config.dynamic_tuning,
                replication_lag_secs: config.goal_replication_lag_secs,
                freshness_secs: config.goal_freshness_secs,
                query_latency_ms: config.goal_query_latency_ms,
                convergence_window_secs: config.goal_convergence_window_secs,
                qph: config.goal_qph,
            }
        })
        .reduce(|first, next| {
            assert_eq!(first, next, "every partition must resolve the same tuning");
            first
        })
        .expect("at least one partition");
    harness.shutdown().await;
    resolved
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(
    not(feature = "spicebench"),
    ignore = "the Cayenne catalog connector requires the spicebench feature"
)]
async fn adaptive_tuning_enabled_turns_adaptive_on_for_catalog_tables() {
    assert!(
        catalog_table_tuning(AdaptiveTuning::Enabled, &[], &[])
            .await
            .dynamic_tuning,
        "`runtime.adaptive_tuning: enabled` must run the closed-loop tuner on catalog tables"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(
    not(feature = "spicebench"),
    ignore = "the Cayenne catalog connector requires the spicebench feature"
)]
async fn catalog_tables_are_static_by_default() {
    assert!(
        !catalog_table_tuning(AdaptiveTuning::Disabled, &[], &[])
            .await
            .dynamic_tuning,
        "without `runtime.adaptive_tuning` catalog tables run static tuning"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(
    not(feature = "spicebench"),
    ignore = "the Cayenne catalog connector requires the spicebench feature"
)]
async fn retired_catalog_tuning_param_warns_once_per_node_and_is_not_applied() {
    let dynamic = catalog_table_tuning(
        AdaptiveTuning::Disabled,
        &[],
        &[
            ("cayenne_tuning", "enabled"),
            ("cayenne_goal_freshness", "5s"),
            ("goal_qph", "100"),
        ],
    )
    .await
    .dynamic_tuning;
    assert!(
        !dynamic,
        "the retired catalog `cayenne_tuning` must not turn the closed-loop tuner on"
    );
    let warning = "Catalog 'tcat' sets `cayenne_tuning`, which is no longer a catalog parameter, so it has no effect. Set `runtime.adaptive_tuning` instead. See: https://spiceai.org/docs/reference/spicepod/runtime";
    let logs = logged();
    // The scheduler and the executor each register the catalog, so each reports it once.
    assert_eq!(
        logs.matches(warning).count(),
        2,
        "the retired catalog param must be reported once per registering node:\n{logs}"
    );
    for warning in [
        "Catalog 'tcat' sets `cayenne_goal_freshness`, which is no longer a catalog parameter, so it has no effect. Set `runtime.params.target_freshness` instead. See: https://spiceai.org/docs/reference/spicepod/runtime",
        "Catalog 'tcat' sets `goal_qph`, which is no longer a catalog parameter, so it has no effect. Set `runtime.params.target_qph` instead. See: https://spiceai.org/docs/reference/spicepod/runtime",
    ] {
        assert_eq!(
            logs.matches(warning).count(),
            2,
            "the retired catalog goal param must be reported once per registering node:\n{logs}"
        );
    }
    for generic in [
        "Ignoring parameter `cayenne_tuning`",
        "Ignoring parameter `cayenne_goal_freshness`",
        "Ignoring parameter `goal_qph`",
    ] {
        assert!(
            !logs.contains(generic),
            "the generic unsupported-parameter warning must not repeat it"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(
    not(feature = "spicebench"),
    ignore = "the Cayenne catalog connector requires the spicebench feature"
)]
async fn runtime_targets_reach_catalog_tables() {
    let resolved = catalog_table_tuning(
        AdaptiveTuning::Enabled,
        &[
            ("target_replication_lag", "10s"),
            ("target_freshness", "5s"),
            ("target_query_latency", "250ms"),
            ("target_convergence_window", "2m"),
            ("target_qph", "5000"),
        ],
        &[],
    )
    .await;
    assert_eq!(
        resolved,
        Resolved {
            dynamic_tuning: true,
            replication_lag_secs: Some(10.0),
            freshness_secs: Some(5.0),
            query_latency_ms: Some(250.0),
            convergence_window_secs: Some(120.0),
            qph: Some(5000.0),
        },
        "the runtime-wide targets must reach catalog tables"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(
    not(feature = "spicebench"),
    ignore = "the Cayenne catalog connector requires the spicebench feature"
)]
async fn targets_without_adaptive_tuning_warn_once_and_leave_the_loop_off() {
    let resolved =
        catalog_table_tuning(AdaptiveTuning::Disabled, &[("target_freshness", "5s")], &[]).await;
    assert!(
        !resolved.dynamic_tuning,
        "a target must not turn the closed-loop tuner on"
    );
    let warning = "`runtime.params.target_*` is set but `runtime.adaptive_tuning` is `disabled`, so catalog 'tcat' ignores the targets.";
    let logs = logged();
    // The scheduler and the executor each register the catalog, so each reports it once.
    assert_eq!(
        logs.matches(warning).count(),
        2,
        "the inert targets must be reported once per registering node:\n{logs}"
    );
}
