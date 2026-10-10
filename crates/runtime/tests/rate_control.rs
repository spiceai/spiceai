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

//! Integration tests for cluster-wide HTTP rate control.
//!
//! The new leased-bucket model treats `requests_per_second_limit` as a
//! **cluster-wide** budget and shares it across replicas via per-window OCC
//! writes to `runtime.state.location`. These tests run two replicas
//! against the same `file://` state location and assert that the combined
//! throughput stays within the cluster budget.
//!
//! This lives in its own test binary so the sign-off gate (`make nextest`) can
//! run it without loading the `integration` binary, whose debug build on macOS
//! is too large for dyld to map the shared cache beside it.

use std::{num::NonZeroU32, path::Path, sync::Arc, time::Duration};

use app::{App, AppBuilder};
use data_connector_api::ConnectorComponent;
use data_http_rate_control::{AdaptiveRateControl, HttpRateControlConfig};
use runtime::{
    Runtime,
    component::dataset::{Dataset, builder::DatasetBuilder},
};
use spicepod::component::runtime::{Runtime as SpicepodRuntime, RuntimeState, SourceRateControl};
use url::Url;

const APP_NAME: &str = "rate_control_cluster_lease";
const ORIGIN_URL: &str = "https://rate-control-cluster.example.com/data";

fn app_with_file_rate_control(state_location: &str, refresh_interval: &str) -> App {
    AppBuilder::new(APP_NAME)
        .with_runtime(SpicepodRuntime {
            state: Some(RuntimeState {
                location: state_location.to_string(),
                params: None,
            }),
            source_rate_control: Some(SourceRateControl {
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
        acquire_timeout: None,
    }
}

fn state_url(state_dir: &Path) -> String {
    Url::from_directory_path(state_dir)
        .expect("state dir should convert to file URL")
        .to_string()
}

/// What two replicas driven as hard as they can acquired from one cluster
/// budget.
struct SaturatedRun {
    count_a: u64,
    count_b: u64,
    elapsed: Duration,
}

impl SaturatedRun {
    fn observed_rps(&self) -> f64 {
        let combined = u32::try_from(self.count_a + self.count_b)
            .expect("combined acquisition count should fit in u32");
        f64::from(combined) / self.elapsed.as_secs_f64()
    }
}

/// Start two runtimes that share one `file://` state location with a 1s
/// window, give each a controller for the same origin with its own config, and
/// drive both as hard as they can for `warmup` and then `measure`, counting
/// only the permits acquired during `measure`.
async fn drive_two_saturated_replicas(
    config_a: &HttpRateControlConfig,
    config_b: &HttpRateControlConfig,
    warmup: Duration,
    measure: Duration,
) -> SaturatedRun {
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let state_dir = temp_dir.path().join("rate-control-state");
    let state_location = state_url(&state_dir);

    // Window = 1s (refresh_interval).
    let refresh_interval = "1s";
    let origin_url = Url::parse(ORIGIN_URL).expect("origin URL parse");

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
            config_a,
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
            config_b,
            dataset_b.app.name.as_str(),
            &ConnectorComponent::from(&dataset_b),
            "https",
        )
        .await
        .expect("controller b");

    let ctrl_a = shared_a.controller.expect("a enabled");
    let ctrl_b = shared_b.controller.expect("b enabled");

    // Count the permits each replica acquires once the warmup is over.
    let measure_from = tokio::time::Instant::now() + warmup;
    let measure_until = measure_from + measure;
    let driver = |ctrl: Arc<runtime_rate_control::RateController>| async move {
        let mut count: u64 = 0;
        while tokio::time::Instant::now() < measure_until {
            if ctrl.acquire().await.is_ok() && tokio::time::Instant::now() >= measure_from {
                count += 1;
            }
        }
        count
    };

    let (count_a, count_b) = tokio::join!(driver(Arc::clone(&ctrl_a)), driver(Arc::clone(&ctrl_b)));
    SaturatedRun {
        count_a,
        count_b,
        elapsed: measure_from.elapsed(),
    }
}

/// Two saturated replicas sharing one cluster budget should not exceed it.
///
/// This is the regression test for the previous "max-merge" implementation
/// which silently allowed N×budget combined throughput.
#[tokio::test]
async fn cluster_lease_caps_combined_throughput_under_saturation() {
    // Cluster budget = 10 RPS.
    let cluster_rps: u32 = 10;
    let config = rps_config(cluster_rps);
    let run =
        drive_two_saturated_replicas(&config, &config, Duration::ZERO, Duration::from_secs(5))
            .await;
    let observed_rps = run.observed_rps();
    let SaturatedRun {
        count_a,
        count_b,
        elapsed,
    } = run;

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

/// Two saturated replicas that set different `requests_per_second_limit`
/// values for one origin are held together to the lower one.
///
/// Regression test for #14913: each value leased under its own key in the
/// shared state, so the replicas sent the sum of both limits — 30 RPS here.
#[tokio::test]
async fn replicas_with_different_limits_are_held_to_the_lowest() {
    let (lower, higher) = (10_u32, 20_u32);
    // Grants are first-write-wins, so the replica at 20 keeps whatever it
    // leased before it first saw the one at 10: up to three windows when the
    // two replicas' first refreshes straddle a window boundary. The warmup
    // covers them; the measurement is of the steady state.
    let run = drive_two_saturated_replicas(
        &rps_config(lower),
        &rps_config(higher),
        Duration::from_secs(3),
        Duration::from_secs(5),
    )
    .await;
    let observed_rps = run.observed_rps();
    let SaturatedRun {
        count_a,
        count_b,
        elapsed,
    } = run;

    // The measurement can straddle one window more than it spans; half a
    // window of slack above the lower limit allows for that and stays below
    // what the higher limit alone would admit.
    let max_allowed = f64::from(lower) * 1.5;
    assert!(
        observed_rps <= max_allowed,
        "replicas at {lower} and {higher} RPS sent {observed_rps:.1} RPS combined, above {max_allowed:.1} (a={count_a} b={count_b} elapsed={elapsed:?})"
    );
    assert!(
        observed_rps >= f64::from(lower) * 0.7,
        "replicas at {lower} and {higher} RPS sent {observed_rps:.1} RPS combined, below 70% of {lower} (a={count_a} b={count_b})"
    );
}

/// Two replicas sharing one budget through the object store, with adaptive
/// throttling on.
///
/// Every assertion below is on what the replicas actually did — permits taken,
/// budget leased — never on the arithmetic in isolation. The unit tests in
/// `runtime-rate-control` cover the formula; these cover the loop through the
/// shared file.
mod cluster_adaptive {
    use std::{sync::Arc, time::Duration};

    use data_connector_api::ConnectorComponent;
    use data_http_rate_control::{AdaptiveRateControl, HttpRateControlConfig};
    use runtime::Runtime;
    use runtime_rate_control::{RateController, RequestOutcome};

    use super::{
        ORIGIN_URL, app_with_file_rate_control, dataset_for_runtime, rps_config, state_url,
    };
    use url::Url;

    /// Throttle above a 50% error rate (`k = 2`), reacting over one window.
    fn adaptive_rps_config(rps: u32) -> HttpRateControlConfig {
        HttpRateControlConfig {
            adaptive: AdaptiveRateControl::new(0.5, Duration::from_secs(1))
                .expect("a 50% threshold over a 1s window is valid"),
            ..rps_config(rps)
        }
    }

    /// One replica: a runtime, its registry entry, and the controller it shares
    /// with every component on the origin.
    struct Replica {
        controller: Arc<RateController>,
        // Held so the registry and its persistence task outlive the controller.
        _runtime: Arc<Runtime>,
    }

    async fn replica(
        state_location: &str,
        refresh_interval: &str,
        config: &HttpRateControlConfig,
    ) -> Replica {
        let app = app_with_file_rate_control(state_location, refresh_interval);
        let runtime = Arc::new(Runtime::builder().with_app(app.clone()).build().await);
        let dataset = dataset_for_runtime(&app, &runtime);
        let origin = Url::parse(ORIGIN_URL).expect("origin URL parse");

        let shared = runtime
            .http_rate_control_registry()
            .shared_rate_controller_for_component(
                &origin,
                config,
                dataset.app.name.as_str(),
                &ConnectorComponent::from(&dataset),
                "https",
            )
            .await
            .expect("controller builds");

        Replica {
            controller: shared.controller.expect("a rate-limited origin is enabled"),
            _runtime: runtime,
        }
    }

    /// Take permits for `duration`, reporting each one's outcome upstream.
    /// Returns how many were taken.
    async fn drive(
        controller: &Arc<RateController>,
        duration: Duration,
        outcome_of: impl Fn(u64) -> Option<RequestOutcome>,
    ) -> u64 {
        let started = tokio::time::Instant::now();
        let mut taken = 0;
        while started.elapsed() < duration {
            if Arc::clone(controller).acquire().await.is_ok() {
                if let Some(outcome) = outcome_of(taken) {
                    controller.record_outcome(outcome);
                }
                taken += 1;
            }
        }
        taken
    }

    fn temp_state() -> (tempfile::TempDir, String) {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let location = state_url(&temp_dir.path().join("rate-control-state"));
        (temp_dir, location)
    }

    /// A healthy origin is never throttled: the coefficient stays at full
    /// admission and the cluster keeps spending its whole configured budget.
    #[tokio::test]
    async fn a_healthy_origin_is_not_throttled() {
        let (_temp_dir, state_location) = temp_state();
        let config = adaptive_rps_config(10);
        let a = replica(&state_location, "1s", &config).await;
        let b = replica(&state_location, "1s", &config).await;

        let duration = Duration::from_secs(5);
        let (count_a, count_b) = tokio::join!(
            drive(&a.controller, duration, |_| Some(RequestOutcome::Success)),
            drive(&b.controller, duration, |_| Some(RequestOutcome::Success)),
        );

        assert_eq!(
            a.controller.admission_coefficient(),
            Some(1.0),
            "a origin that always succeeds must not be throttled"
        );
        assert_eq!(b.controller.admission_coefficient(), Some(1.0));

        let combined = count_a + count_b;
        assert!(
            combined >= 25,
            "a healthy cluster should spend close to 10 RPS over 5s, took {combined} (a={count_a} b={count_b})"
        );
    }

    /// Both replicas failing: they converge on one smaller budget, and the
    /// permits they take fall with it.
    #[tokio::test]
    async fn a_failing_origin_throttles_every_replica_to_the_same_budget() {
        let (_temp_dir, state_location) = temp_state();
        let config = adaptive_rps_config(20);
        let a = replica(&state_location, "1s", &config).await;
        let b = replica(&state_location, "1s", &config).await;

        // Four of every five requests fail: an 80% error rate, far above the
        // 50% threshold.
        let mostly_failing = |taken: u64| {
            Some(if taken.is_multiple_of(5) {
                RequestOutcome::Success
            } else {
                RequestOutcome::Failure
            })
        };

        let duration = Duration::from_secs(6);
        let (count_a, count_b) = tokio::join!(
            drive(&a.controller, duration, mostly_failing),
            drive(&b.controller, duration, mostly_failing),
        );

        let coefficient_a = a
            .controller
            .admission_coefficient()
            .expect("a cluster controller reports a coefficient");
        let coefficient_b = b
            .controller
            .admission_coefficient()
            .expect("a cluster controller reports a coefficient");

        // Both replicas throttle. Bit-exact agreement across an unsynchronised
        // pair is proved deterministically in the `runtime-rate-control` unit
        // tests, where the refreshes are driven in lockstep; here the two are
        // free-running and may be reporting adjacent windows.
        assert!(
            coefficient_a < 1.0,
            "an 80% error rate must throttle replica a, got {coefficient_a}"
        );
        assert!(
            coefficient_b < 1.0,
            "an 80% error rate must throttle replica b, got {coefficient_b}"
        );

        for bursts in [
            a.controller.cluster_effective_bursts(),
            b.controller.cluster_effective_bursts(),
        ] {
            assert!(
                bursts.iter().all(|(_, burst)| (1..20).contains(burst)),
                "every budget must be throttled below the configured 20 and never below 1: {bursts:?}"
            );
        }

        // The throttle has to show up in permits actually taken, not only in
        // the reported coefficient.
        let combined = count_a + count_b;
        assert!(
            combined < 20 * 6,
            "a throttled cluster must take fewer than the configured 20 RPS over 6s, took {combined} (a={count_a} b={count_b})"
        );
    }

    /// Only one replica sees failures. The coefficient reads the cluster's
    /// combined error rate, so a healthy peer's traffic dilutes it — a single
    /// failing replica cannot throttle the cluster on its own view.
    #[tokio::test]
    async fn the_coefficient_reads_both_replicas_not_either_one() {
        let (_temp_dir, state_location) = temp_state();
        let config = adaptive_rps_config(20);
        let failing = replica(&state_location, "1s", &config).await;
        let healthy = replica(&state_location, "1s", &config).await;

        let duration = Duration::from_secs(6);
        tokio::join!(
            drive(&failing.controller, duration, |_| Some(
                RequestOutcome::Failure
            )),
            drive(&healthy.controller, duration, |_| Some(
                RequestOutcome::Success
            )),
        );

        let combined = failing
            .controller
            .admission_coefficient()
            .expect("a cluster controller reports a coefficient");

        // Both replicas share the budget roughly evenly, so the cluster error
        // rate sits near 50% — the threshold itself. A coefficient derived from
        // the failing replica alone would be about k * 0 = 0.
        assert!(
            combined > 0.5,
            "the failing replica alone must not drive the cluster to zero, got {combined}"
        );
        let seen_by_healthy = healthy
            .controller
            .admission_coefficient()
            .expect("a cluster controller reports a coefficient");
        assert!(
            seen_by_healthy > 0.5,
            "the healthy replica reads the same shared counts, got {seen_by_healthy}"
        );
    }

    /// A request that never reached the origin must not move the coefficient.
    /// Counting an acquire timeout would close a loop: throttle, fewer permits,
    /// more acquire timeouts, deeper throttle.
    #[tokio::test]
    async fn an_acquire_timeout_does_not_move_the_coefficient() {
        let (_temp_dir, state_location) = temp_state();
        // One request per second, with a timeout far shorter than the wait a
        // second caller faces.
        let config = HttpRateControlConfig {
            acquire_timeout: Some(Duration::from_millis(50)),
            ..adaptive_rps_config(1)
        };
        let replica = replica(&state_location, "1s", &config).await;

        let mut timed_out = 0;
        for _ in 0..20 {
            if Arc::clone(&replica.controller).acquire().await.is_err() {
                timed_out += 1;
            }
        }

        assert!(
            timed_out > 0,
            "the test needs at least one acquire timeout to be meaningful"
        );
        assert_eq!(
            replica.controller.admission_coefficient(),
            Some(1.0),
            "acquire timeouts never reach the origin, so they are not upstream failures"
        );
    }
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
            acquire_timeout: None,
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

    /// Cluster rate control adapts: every replica derives the same coefficient
    /// from the shared state and scales the leased budget by it, so the whole
    /// cluster backs off together.
    #[tokio::test]
    async fn cluster_rate_control_adapts() {
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
            .expect("a rate-limited origin builds a controller");
        // The coefficient is fixed per leased window, so lease the first one.
        controller
            .refresh_and_persist_state_snapshot()
            .await
            .expect("lease the first window");
        assert_eq!(
            controller.admission_coefficient(),
            Some(1.0),
            "a cluster controller reports a coefficient, and starts at full admission"
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

/// One origin must resolve to one rate-control configuration, whichever
/// HTTP-based connector reaches it first.
///
/// `rate_control_acquire_timeout` defaults to the connector's client timeout,
/// so every connector in the family must fill an unset value. A connector that
/// did not would leave `None` where its neighbour holds the default, and the
/// shared-origin check would reject two datasets the user configured the same
/// way.
mod shared_origin_acquire_timeout {
    use std::num::NonZeroU32;
    use std::sync::Arc;

    use data_connector_api::{
        ConnectorComponent, DEFAULT_SPICE_CLIENT_TIMEOUT, DataConnectorError,
    };
    use data_http_rate_control::{HttpRateControlConfig, HttpRateControlRegistry};
    use runtime::dataconnector::https::DEFAULT_CLIENT_TIMEOUT as HTTPS_DEFAULT_CLIENT_TIMEOUT;
    use runtime_component::dataset::DatasetSpec;
    use url::Url;

    const SHARED_ORIGIN: &str = "https://shared-origin.example.com/v1";

    fn component(name: &str) -> ConnectorComponent {
        ConnectorComponent::Dataset(Arc::new(DatasetSpec::new(SHARED_ORIGIN, name.into())))
    }

    fn origin_url() -> Url {
        Url::parse(SHARED_ORIGIN).expect("test origin should parse")
    }

    /// A limited config, as a dataset that sets `requests_per_second_limit` and
    /// nothing else resolves to.
    fn limited_config() -> HttpRateControlConfig {
        HttpRateControlConfig {
            requests_per_second: NonZeroU32::new(10),
            ..HttpRateControlConfig::disabled()
        }
    }

    #[tokio::test]
    async fn https_and_graphql_agree_on_the_default_acquire_timeout() {
        let mut https_config = limited_config();
        https_config.apply_default_acquire_timeout(HTTPS_DEFAULT_CLIENT_TIMEOUT);
        let mut graphql_config = limited_config();
        graphql_config.apply_default_acquire_timeout(DEFAULT_SPICE_CLIENT_TIMEOUT);

        assert_eq!(
            https_config, graphql_config,
            "the two connectors must derive the same acquire bound for a shared origin"
        );

        let registry = HttpRateControlRegistry::default();
        let url = origin_url();
        registry
            .shared_rate_controller_for_component(
                &url,
                &https_config,
                "spicepod",
                &component("https_dataset"),
                "https",
            )
            .await
            .expect("the first dataset on the origin defines its rate control");
        registry
            .shared_rate_controller_for_component(
                &url,
                &graphql_config,
                "spicepod",
                &component("graphql_dataset"),
                "graphql",
            )
            .await
            .expect("a GraphQL dataset with the same settings shares that rate control");
    }

    /// The failure a connector that skipped the default would cause, and the
    /// message that must name the parameter to explain it.
    #[tokio::test]
    async fn an_unfilled_acquire_timeout_conflicts_on_a_shared_origin() {
        let mut https_config = limited_config();
        https_config.apply_default_acquire_timeout(HTTPS_DEFAULT_CLIENT_TIMEOUT);

        let registry = HttpRateControlRegistry::default();
        let url = origin_url();
        registry
            .shared_rate_controller_for_component(
                &url,
                &https_config,
                "spicepod",
                &component("https_dataset"),
                "https",
            )
            .await
            .expect("the first dataset on the origin defines its rate control");

        let error = registry
            .shared_rate_controller_for_component(
                &url,
                &limited_config(),
                "spicepod",
                &component("graphql_dataset"),
                "graphql",
            )
            .await
            .expect_err("an unbounded wait conflicts with a bounded one");

        match error {
            DataConnectorError::InvalidConfigurationNoSource { message, .. } => assert!(
                message.contains("rate_control_acquire_timeout"),
                "the conflict message must name the parameter: {message}"
            ),
            other => panic!("expected an invalid-configuration error, got {other:?}"),
        }
    }
}
