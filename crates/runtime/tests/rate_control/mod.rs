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
use data_connector_api::ConnectorComponent;
use data_http_rate_control::{AdaptiveRateControl, HttpRateControlConfig};
use runtime::{
    Runtime,
    component::dataset::{Dataset, builder::DatasetBuilder},
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
        adaptive: AdaptiveRateControl::default(),
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
        .shared_rate_controller_for_component(
            &origin_url,
            &config,
            dataset_a.app.name.as_str(),
            &ConnectorComponent::from(&dataset_a),
            "https",
        )
        .await
        .expect("controller a");
    let shared_b = runtime_b
        .http_rate_control_registry()
        .shared_rate_controller_for_component(
            &origin_url,
            &config,
            dataset_b.app.name.as_str(),
            &ConnectorComponent::from(&dataset_b),
            "https",
        )
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

/// Shared-origin rules for adaptive HTTP rate control.
///
/// These tests live here rather than in `data-http-rate-control` because they
/// need a `ConnectorComponent`, which requires `runtime-component`; that crate
/// must not carry a dependency on it.
mod adaptive_config_validation {
    use std::num::NonZeroU32;
    use std::sync::Arc;
    use std::time::Duration;

    use data_connector_api::{ConnectorComponent, DataConnectorError};
    use data_http_rate_control::{
        AdaptiveRateControl, HttpRateControlConfig, HttpRateControlRegistry,
    };
    use object_store::memory::InMemory;
    use runtime_component::dataset::DatasetSpec;
    use url::Url;

    const TEST_ORIGIN: &str = "https://origin.example.com/data";

    fn test_component() -> ConnectorComponent {
        ConnectorComponent::Dataset(Arc::new(DatasetSpec::new(
            TEST_ORIGIN,
            "rate_control_test".into(),
        )))
    }

    fn config(
        adaptive: AdaptiveRateControl,
        requests_per_second: Option<u32>,
    ) -> HttpRateControlConfig {
        HttpRateControlConfig {
            max_concurrent_requests: None,
            requests_per_second: requests_per_second
                .map(|rps| NonZeroU32::new(rps).expect("test rps must be non-zero")),
            requests_per_minute: None,
            jitter_min: Duration::ZERO,
            jitter_max: Duration::ZERO,
            adaptive,
        }
    }

    /// With no rate limit there is nothing for adaptive control to scale, so the
    /// configuration is accepted and no controller is built.
    #[tokio::test]
    async fn no_rate_limit_is_accepted_and_builds_no_controller() {
        let registry = Arc::new(HttpRateControlRegistry::default());
        let origin = Url::parse(TEST_ORIGIN).expect("test URL should parse");

        let shared = registry
            .shared_rate_controller_for_component(
                &origin,
                &config(AdaptiveRateControl::default(), None),
                "spicepod",
                &test_component(),
                "https",
            )
            .await
            .expect("no rate limit is a valid configuration");
        assert!(shared.controller.is_none());
    }

    /// Cluster rate control accepts every configuration. The leased cluster
    /// bucket does not adapt yet, so it applies the configured limits unchanged.
    #[tokio::test]
    async fn cluster_rate_control_applies_the_configured_limits() {
        let registry = Arc::new(HttpRateControlRegistry::with_persisted_governor_state(
            Arc::new(InMemory::new()),
            "",
            Duration::from_secs(1),
        ));
        let origin = Url::parse(TEST_ORIGIN).expect("test URL should parse");

        let reservation = Arc::clone(&registry)
            .reserve_shared_rate_controller_for_component(
                &origin,
                &config(AdaptiveRateControl::default(), Some(10)),
                "spicepod",
                &test_component(),
                "https",
            )
            .await
            .expect("cluster rate control accepts the configuration");
        let controller = reservation
            .shared()
            .controller
            .clone()
            .expect("a rate-limited origin has a controller");
        assert_eq!(
            controller.admission_coefficient(),
            None,
            "the leased cluster bucket does not adapt"
        );
        reservation.rollback().await;
    }

    /// Components that share an origin share one controller, so they must agree
    /// on the adaptive tuning as well as on the limits.
    #[tokio::test]
    async fn different_adaptive_tuning_on_one_origin_is_a_config_error() {
        let registry = Arc::new(HttpRateControlRegistry::default());
        let origin = Url::parse(TEST_ORIGIN).expect("test URL should parse");

        registry
            .shared_rate_controller_for_component(
                &origin,
                &config(AdaptiveRateControl::default(), Some(10)),
                "spicepod",
                &test_component(),
                "https",
            )
            .await
            .expect("the first component sets the origin's config");

        let other_tuning = AdaptiveRateControl::new(0.5, Duration::from_secs(10))
            .expect("test control should be valid");
        let error = registry
            .shared_rate_controller_for_component(
                &origin,
                &config(other_tuning, Some(10)),
                "spicepod",
                &test_component(),
                "https",
            )
            .await
            .expect_err("a different failure threshold on the same origin must be rejected");

        match error {
            DataConnectorError::InvalidConfigurationNoSource { message, .. } => {
                assert!(
                    message.contains("different rate-control settings"),
                    "message must name the conflict: {message}"
                );
                assert!(
                    message.contains("rate_control_failure_threshold")
                        && message.contains("rate_control_window"),
                    "message must name the adaptive parameters: {message}"
                );
                assert!(
                    !message.contains("rate_control_mode"),
                    "message must not name a removed parameter: {message}"
                );
            }
            other => panic!("expected an invalid-configuration error, got {other:?}"),
        }
    }
}
