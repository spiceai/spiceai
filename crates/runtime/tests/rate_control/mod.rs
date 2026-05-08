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

//! Integration tests for cluster-wide HTTP rate control.
//!
//! The new leased-bucket model treats `requests_per_second_limit` as a
//! **cluster-wide** budget and shares it across replicas via per-window OCC
//! writes to the configured state location. These tests run two replicas
//! against the same `file://` state location and assert that the combined
//! throughput stays within the cluster budget.

use std::{num::NonZeroU32, path::Path, sync::Arc, time::Duration};

use app::{App, AppBuilder};
use runtime::{
    Runtime,
    component::dataset::{Dataset, builder::DatasetBuilder},
    dataconnector::http_rate_control::HttpRateControlConfig,
};
use spicepod::component::runtime::{Runtime as SpicepodRuntime, SourceRateControl};
use url::Url;

const APP_NAME: &str = "rate_control_cluster_lease";
const ORIGIN_URL: &str = "https://rate-control-cluster.example.com/data";

fn app_with_file_rate_control(state_location: &str, refresh_interval: &str) -> App {
    AppBuilder::new(APP_NAME)
        .with_runtime(SpicepodRuntime {
            source_rate_control: Some(SourceRateControl {
                state_location: Some(state_location.to_string()),
                params: None,
                refresh_interval: refresh_interval.to_string(),
                github_concurrent_connections_limit: None,
            }),
            ..Default::default()
        })
        .build()
}

fn dataset_for_runtime(app: &App, runtime: &Arc<Runtime>) -> Dataset {
    DatasetBuilder::try_new(ORIGIN_URL.to_string(), "rate_control_cluster_lease")
        .expect("dataset builder should be valid")
        .with_app(Arc::new(app.clone()))
        .with_runtime(Arc::clone(runtime))
        .build()
        .expect("dataset should build")
}

fn rps_config(rps: u32) -> HttpRateControlConfig {
    HttpRateControlConfig {
        max_concurrent_requests: None,
        requests_per_second: Some(NonZeroU32::new(rps).expect("rps non-zero")),
        requests_per_minute: None,
        jitter_min: Duration::ZERO,
        jitter_max: Duration::ZERO,
    }
}

fn state_url(state_dir: &Path) -> String {
    Url::from_directory_path(state_dir)
        .expect("state dir should convert to file URL")
        .to_string()
}

/// Two saturated replicas sharing one cluster budget should not exceed it.
///
/// This is the regression test for the previous "max-merge" implementation
/// which silently allowed N×budget combined throughput.
#[tokio::test]
async fn cluster_lease_caps_combined_throughput_under_saturation() {
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let state_dir = temp_dir.path().join("rate-control-state");
    let state_location = state_url(&state_dir);

    // Window = 1s (refresh_interval), cluster budget = 10 RPS.
    let refresh_interval = "1s";
    let cluster_rps: u32 = 10;
    let origin_url = Url::parse(ORIGIN_URL).expect("origin URL parse");
    let config = rps_config(cluster_rps);

    let app_a = app_with_file_rate_control(&state_location, refresh_interval);
    let app_b = app_with_file_rate_control(&state_location, refresh_interval);

    let runtime_a = Arc::new(Runtime::builder().with_app(app_a.clone()).build().await);
    let runtime_b = Arc::new(Runtime::builder().with_app(app_b.clone()).build().await);

    let dataset_a = dataset_for_runtime(&app_a, &runtime_a);
    let dataset_b = dataset_for_runtime(&app_b, &runtime_b);

    let shared_a = runtime_a
        .http_rate_control_registry()
        .shared_rate_controller(&origin_url, &config, &dataset_a, "https")
        .await
        .expect("controller a");
    let shared_b = runtime_b
        .http_rate_control_registry()
        .shared_rate_controller(&origin_url, &config, &dataset_b, "https")
        .await
        .expect("controller b");

    let ctrl_a = shared_a.controller.expect("a enabled");
    let ctrl_b = shared_b.controller.expect("b enabled");

    // Drive both replicas as hard as we can for `duration`. Count the total
    // number of permits each acquires.
    let duration = Duration::from_secs(5);
    let started = tokio::time::Instant::now();

    let driver = |ctrl: Arc<runtime_rate_control::RateController>| async move {
        let mut count: u64 = 0;
        while started.elapsed() < duration {
            if ctrl.acquire().await.is_ok() {
                count += 1;
            }
        }
        count
    };

    let (count_a, count_b) = tokio::join!(driver(Arc::clone(&ctrl_a)), driver(Arc::clone(&ctrl_b)));
    let combined = count_a + count_b;
    let elapsed = started.elapsed();
    let observed_rps =
        f64::from(u32::try_from(combined).expect("combined acquisition count should fit in u32"))
            / elapsed.as_secs_f64();

    // Allow up to one extra window of burst above the steady-state cap (10 RPS):
    // worst-case overshoot per window = burst_per_window = 10. With ~5 windows
    // total over 5 seconds, observed rate must stay close to 10 RPS.
    let max_allowed = f64::from(cluster_rps) * 2.0;
    assert!(
        observed_rps <= max_allowed,
        "combined observed {observed_rps:.1} RPS exceeds cap {max_allowed:.1} (a={count_a} b={count_b} elapsed={elapsed:?})"
    );

    // With pre-leasing of the next window, the dead-zone at window boundaries
    // is eliminated and combined throughput should approach the cluster cap.
    // Allow some slack for the first-window startup and timing noise.
    assert!(
        observed_rps >= f64::from(cluster_rps) * 0.7,
        "combined observed {observed_rps:.1} RPS below 70% of cap {cluster_rps} (a={count_a} b={count_b})"
    );
}
