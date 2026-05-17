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

use std::fmt::Write;
use std::sync::Arc;

use crate::cluster::ExecutorRegistry;
use crate::status::RuntimeStatus;
use axum::{
    Extension,
    extract::Query,
    http::status,
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};

/// Optional gates layered on top of the default readiness check.
///
/// When neither executor gate is set, `/v1/ready` behaves as before (it only reflects component
/// readiness). When set, the gate is re-evaluated on every probe — failing either gate flips the
/// response back to "not ready" until the threshold is met again.
#[derive(Debug, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::IntoParams))]
pub struct ReadyQuery {
    /// Minimum number of currently-ready executors required for the probe to succeed. "Ready"
    /// means the scheduler has a live `FlightSQL` client for the executor — i.e. it can route
    /// queries to it. A value of `0` is treated as "gate disabled" and never blocks. Requires
    /// scheduler role; supplying a non-zero value outside scheduler role returns `400`.
    #[serde(default)]
    pub min_ready_executors: Option<u32>,

    /// Minimum percentage (0-100) of currently-ready executors relative to the number of
    /// executors currently registered (control stream open). A value of `0` is treated as "gate
    /// disabled" and never blocks. Values above 100 return `400`. Requires scheduler role;
    /// supplying a non-zero value outside scheduler role returns `400`.
    #[serde(default)]
    pub min_ready_executors_percent: Option<u8>,

    /// When `true`, the response body becomes a multi-line diagnostic listing the result of each
    /// gate. The HTTP status code is unchanged. Useful for `kubectl describe` / curl debugging.
    #[serde(default)]
    pub verbose: bool,
}

impl ReadyQuery {
    fn count_gate_active(&self) -> bool {
        matches!(self.min_ready_executors, Some(n) if n > 0)
    }

    fn percent_gate_active(&self) -> bool {
        matches!(self.min_ready_executors_percent, Some(p) if p > 0)
    }

    fn any_executor_gate_active(&self) -> bool {
        self.count_gate_active() || self.percent_gate_active()
    }
}

/// Check Readiness
///
/// Check the runtime status of all the components of the runtime. If the service is ready, it returns an HTTP 200 status with the message "ready". If not, it returns a 503 status with the message "not ready".
///
/// The behavior for when an accelerated dataset is considered ready is configurable via the `ready_state` parameter. See [Data refresh](https://spiceai.org/docs/components/data-accelerators/data-refresh#ready-state) for more details.
///
/// In distributed (scheduler) mode the readiness response can additionally be gated on executor
/// availability via the `min_ready_executors` and `min_ready_executors_percent` query parameters
/// (both optional). Both must pass when supplied. Pass `verbose=true` to get a multi-line
/// diagnostic body explaining each gate.
///
/// ### Readiness Probe
/// In production deployments, the /v1/ready endpoint can be used as a readiness probe for a Spice deployment to ensure traffic is routed to the Spice runtime only after all datasets have finished loading.
///
/// Example Kubernetes readiness probe:
/// ```yaml
/// readinessProbe:
///  httpGet:
///    path: /v1/ready
///    port: 8090
/// ```
///
/// Example with executor gating (scheduler role):
/// ```yaml
/// readinessProbe:
///  httpGet:
///    path: /v1/ready?min_ready_executors=3&min_ready_executors_percent=80
///    port: 8090
/// ```
#[cfg_attr(feature = "openapi", utoipa::path(
    get,
    path = "/v1/ready",
    operation_id = "ready",
    tag = "Ready",
    params(ReadyQuery),
    responses(
        (status = 200, description = "Service is ready", content((String = "text/plain", example = "ready"))),
        (status = 400, description = "Invalid query parameter or executor gate requested outside scheduler role", content((String = "text/plain", example = "min_ready_executors_percent must be between 0 and 100"))),
        (status = 503, description = "Service is not ready", content((String = "text/plain", example = "not ready")))
    )
))]
pub(crate) async fn get(
    Extension(status): Extension<Arc<RuntimeStatus>>,
    Extension(executor_registry): Extension<Option<Arc<ExecutorRegistry>>>,
    Query(query): Query<ReadyQuery>,
) -> Response {
    if let Some(percent) = query.min_ready_executors_percent
        && percent > 100
    {
        return (
            status::StatusCode::BAD_REQUEST,
            "min_ready_executors_percent must be between 0 and 100",
        )
            .into_response();
    }

    // Executor gates are scheduler-only. Supplying them on a non-scheduler runtime is a
    // misconfiguration — return 400 rather than silently passing or failing.
    if query.any_executor_gate_active() && executor_registry.is_none() {
        return (
            status::StatusCode::BAD_REQUEST,
            "executor gates (min_ready_executors, min_ready_executors_percent) require scheduler role; this runtime has no executor registry",
        )
            .into_response();
    }

    let components_ok = status.is_ready();

    // We may not need executor counts; only fetch them when a gate is active. The 400 above
    // ensures we only reach this branch with a registry in hand when a gate is active.
    let executor_state = match (query.any_executor_gate_active(), executor_registry.as_ref()) {
        (true, Some(registry)) => Some(ExecutorState {
            ready: registry.flight_sql_clients_count().await,
            registered: registry.connected_executor_count().await,
        }),
        _ => None,
    };

    // `Some(0)` must match before `(Some(_), None)` so a zero-valued gate is treated as
    // "Skipped" (disabled) consistently whether or not a registry is present.
    let count_pass = match (query.min_ready_executors, executor_state.as_ref()) {
        (Some(0), _) => GateOutcome::Skipped,
        (None, _) | (Some(_), None) => GateOutcome::NotSet,
        (Some(n), Some(state)) => {
            if u64::try_from(state.ready).unwrap_or(u64::MAX) >= u64::from(n) {
                GateOutcome::Pass
            } else {
                GateOutcome::Fail
            }
        }
    };

    let percent_pass = match (query.min_ready_executors_percent, executor_state.as_ref()) {
        (Some(0), _) => GateOutcome::Skipped,
        (None, _) | (Some(_), None) => GateOutcome::NotSet,
        (Some(_), Some(state)) if state.registered == 0 => GateOutcome::Fail,
        (Some(p), Some(state)) => {
            // Saturate on overflow rather than panic / wrap. With realistic executor counts
            // (<< 2^64) the saturation branches are unreachable, but we avoid any chance of an
            // incorrect "ready" result if counts ever grow pathologically large.
            let lhs = u128::try_from(state.ready)
                .unwrap_or(u128::MAX)
                .saturating_mul(100);
            let rhs =
                u128::from(p).saturating_mul(u128::try_from(state.registered).unwrap_or(u128::MAX));
            if lhs >= rhs {
                GateOutcome::Pass
            } else {
                GateOutcome::Fail
            }
        }
    };

    let overall_ok = components_ok
        && !matches!(count_pass, GateOutcome::Fail)
        && !matches!(percent_pass, GateOutcome::Fail);

    let code = if overall_ok {
        status::StatusCode::OK
    } else {
        status::StatusCode::SERVICE_UNAVAILABLE
    };

    let body = if query.verbose {
        render_verbose(
            components_ok,
            count_pass,
            percent_pass,
            executor_state.as_ref(),
            &query,
            overall_ok,
        )
    } else if overall_ok {
        "ready".to_string()
    } else {
        "not ready".to_string()
    };

    (code, body).into_response()
}

#[derive(Debug, Clone, Copy)]
struct ExecutorState {
    ready: usize,
    registered: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GateOutcome {
    /// Param not supplied at all.
    NotSet,
    /// Param supplied with a zero / "disabled" value.
    Skipped,
    Pass,
    Fail,
}

fn render_verbose(
    components_ok: bool,
    count_pass: GateOutcome,
    percent_pass: GateOutcome,
    executor_state: Option<&ExecutorState>,
    query: &ReadyQuery,
    overall_ok: bool,
) -> String {
    let mut out = String::new();
    let _ = writeln!(
        out,
        "{} components {}",
        marker(if components_ok {
            GateOutcome::Pass
        } else {
            GateOutcome::Fail
        }),
        if components_ok { "ok" } else { "not ready" }
    );

    if count_pass != GateOutcome::NotSet || percent_pass != GateOutcome::NotSet {
        let (ready, registered) = executor_state.map_or((0, 0), |s| (s.ready, s.registered));
        let pct: u128 = if registered == 0 {
            0
        } else {
            u128::try_from(ready)
                .unwrap_or(u128::MAX)
                .saturating_mul(100)
                / u128::try_from(registered).unwrap_or(u128::MAX)
        };
        let mut detail =
            format!("executors: {ready}/{registered} ready ({pct}%, registered={registered}");
        if let Some(n) = query.min_ready_executors {
            let _ = write!(detail, ", min={n}");
        }
        if let Some(p) = query.min_ready_executors_percent {
            let _ = write!(detail, ", min_percent={p}%");
        }
        detail.push(')');

        let worst = match (count_pass, percent_pass) {
            (GateOutcome::Fail, _) | (_, GateOutcome::Fail) => GateOutcome::Fail,
            (GateOutcome::Pass, _) | (_, GateOutcome::Pass) => GateOutcome::Pass,
            _ => GateOutcome::Skipped,
        };
        let _ = writeln!(out, "{} {detail}", marker(worst));
    }

    out.push_str(if overall_ok { "ready" } else { "not ready" });
    out
}

fn marker(outcome: GateOutcome) -> &'static str {
    match outcome {
        GateOutcome::Pass => "[+]",
        GateOutcome::Fail => "[-]",
        GateOutcome::Skipped | GateOutcome::NotSet => "[ ]",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;
    use axum::http::StatusCode;
    use runtime_cluster::PartitionStore;
    use runtime_cluster::cluster_state::ClusterStateStore;
    use std::sync::Arc;
    use tokio::sync::mpsc;

    async fn make_registry() -> Arc<ExecutorRegistry> {
        use object_store::ObjectStore;
        use object_store::memory::InMemory;
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let cs = Arc::new(ClusterStateStore::new(store, ""));
        cs.bootstrap().await.expect("bootstrap");
        Arc::new(ExecutorRegistry::new(
            Arc::new(PartitionStore::accelerations(Arc::clone(&cs))),
            Arc::new(PartitionStore::catalog(Arc::clone(&cs))),
            Arc::new(runtime_cluster::OccDdlLog::new(Arc::clone(&cs))),
        ))
    }

    fn dummy_flight_sql_client() -> data_components::flightsql::FlightSqlClient {
        use arrow_flight::flight_service_client::FlightServiceClient;
        use arrow_flight::sql::client::FlightSqlServiceClient;
        use flight_client::cookie::{CookieService, CookieStore};
        use tonic::transport::Endpoint;

        let channel = Endpoint::from_static("http://127.0.0.1:1").connect_lazy();
        let cookie_channel = CookieService::new(channel, Arc::new(CookieStore::new()));
        FlightSqlServiceClient::new_from_inner(FlightServiceClient::new(cookie_channel))
    }

    /// Drives executor lifecycle: registers `connected` control streams, then marks `ready` of
    /// them as fully ready (`FlightSQL` client present). The tests never call `send` on the
    /// stored channel, so we drop the rx ends — the count tracking only cares about `HashMap`
    /// membership.
    async fn drive_executors(registry: &Arc<ExecutorRegistry>, connected: usize, ready: usize) {
        assert!(ready <= connected);
        for i in 0..connected {
            let (tx, _rx) = mpsc::channel(1);
            registry.register(format!("e{i}"), tx).await;
        }
        for i in 0..ready {
            registry
                .insert_flight_sql_client(format!("e{i}"), dummy_flight_sql_client())
                .await;
        }
    }

    fn status_ready() -> Arc<RuntimeStatus> {
        use crate::status::{ComponentStatus, RuntimeReadyState};
        let status = RuntimeStatus::new();
        status.set_ready_state(RuntimeReadyState::OnRegistration);
        status.update_component_status("test:probe", ComponentStatus::Ready);
        status
    }

    fn status_not_ready() -> Arc<RuntimeStatus> {
        RuntimeStatus::new()
    }

    async fn body_string(response: Response) -> String {
        let (_, body) = response.into_parts();
        let bytes = to_bytes(body, 64 * 1024).await.expect("body");
        String::from_utf8(bytes.to_vec()).expect("utf8")
    }

    #[tokio::test]
    async fn no_query_params_returns_ready_when_components_ready() {
        let response = get(
            Extension(status_ready()),
            Extension(None),
            Query(ReadyQuery::default()),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(body_string(response).await, "ready");
    }

    #[tokio::test]
    async fn components_not_ready_returns_503_regardless_of_params() {
        let registry = make_registry().await;
        drive_executors(&registry, 3, 3).await;
        let response = get(
            Extension(status_not_ready()),
            Extension(Some(Arc::clone(&registry))),
            Query(ReadyQuery {
                min_ready_executors: Some(1),
                min_ready_executors_percent: None,
                verbose: false,
            }),
        )
        .await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn count_gate_fails_when_below_threshold() {
        let registry = make_registry().await;
        drive_executors(&registry, 1, 1).await;

        let response = get(
            Extension(status_ready()),
            Extension(Some(Arc::clone(&registry))),
            Query(ReadyQuery {
                min_ready_executors: Some(3),
                min_ready_executors_percent: None,
                verbose: false,
            }),
        )
        .await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body_string(response).await, "not ready");
    }

    #[tokio::test]
    async fn count_gate_passes_at_threshold() {
        let registry = make_registry().await;
        drive_executors(&registry, 3, 3).await;

        let response = get(
            Extension(status_ready()),
            Extension(Some(Arc::clone(&registry))),
            Query(ReadyQuery {
                min_ready_executors: Some(3),
                min_ready_executors_percent: None,
                verbose: false,
            }),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn count_gate_zero_is_disabled() {
        let registry = make_registry().await;
        // Zero ready executors but min_ready_executors=0 is a no-op.
        let response = get(
            Extension(status_ready()),
            Extension(Some(Arc::clone(&registry))),
            Query(ReadyQuery {
                min_ready_executors: Some(0),
                min_ready_executors_percent: None,
                verbose: false,
            }),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn percent_gate_with_no_registered_executors_fails() {
        let registry = make_registry().await;
        let response = get(
            Extension(status_ready()),
            Extension(Some(Arc::clone(&registry))),
            Query(ReadyQuery {
                min_ready_executors: None,
                min_ready_executors_percent: Some(20),
                verbose: false,
            }),
        )
        .await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn percent_gate_uses_registered_as_denominator() {
        let registry = make_registry().await;
        // 10 connected, 2 ready → 20% which meets a 20% threshold.
        drive_executors(&registry, 10, 2).await;
        let response = get(
            Extension(status_ready()),
            Extension(Some(Arc::clone(&registry))),
            Query(ReadyQuery {
                min_ready_executors: None,
                min_ready_executors_percent: Some(20),
                verbose: false,
            }),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);

        // 10 connected, 1 ready → 10% which is below 20%.
        let registry = make_registry().await;
        drive_executors(&registry, 10, 1).await;
        let response = get(
            Extension(status_ready()),
            Extension(Some(Arc::clone(&registry))),
            Query(ReadyQuery {
                min_ready_executors: None,
                min_ready_executors_percent: Some(20),
                verbose: false,
            }),
        )
        .await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn both_gates_must_pass() {
        let registry = make_registry().await;
        // 5 connected, 2 ready → 40% (passes 20%) but count 2 < 3 fails count gate.
        drive_executors(&registry, 5, 2).await;
        let response = get(
            Extension(status_ready()),
            Extension(Some(Arc::clone(&registry))),
            Query(ReadyQuery {
                min_ready_executors: Some(3),
                min_ready_executors_percent: Some(20),
                verbose: false,
            }),
        )
        .await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn invalid_percentage_returns_400() {
        let response = get(
            Extension(status_ready()),
            Extension(None),
            Query(ReadyQuery {
                min_ready_executors: None,
                min_ready_executors_percent: Some(150),
                verbose: false,
            }),
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn executor_gate_without_scheduler_role_returns_400() {
        let response = get(
            Extension(status_ready()),
            Extension(None),
            Query(ReadyQuery {
                min_ready_executors: Some(1),
                min_ready_executors_percent: None,
                verbose: false,
            }),
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(body_string(response).await.contains("scheduler role"));
    }

    #[tokio::test]
    async fn zero_valued_executor_gate_without_scheduler_role_passes() {
        // min_ready_executors=0 is "gate disabled" and should not 400 outside scheduler role.
        let response = get(
            Extension(status_ready()),
            Extension(None),
            Query(ReadyQuery {
                min_ready_executors: Some(0),
                min_ready_executors_percent: Some(0),
                verbose: false,
            }),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn verbose_body_shows_each_gate() {
        let registry = make_registry().await;
        // 5 connected, 2 ready → 40%.
        drive_executors(&registry, 5, 2).await;
        let response = get(
            Extension(status_ready()),
            Extension(Some(Arc::clone(&registry))),
            Query(ReadyQuery {
                min_ready_executors: Some(3),
                min_ready_executors_percent: Some(80),
                verbose: true,
            }),
        )
        .await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = body_string(response).await;
        assert!(body.contains("[+] components ok"), "body: {body}");
        assert!(
            body.contains("2/5 ready (40%, registered=5, min=3, min_percent=80%)"),
            "body: {body}"
        );
        assert!(body.contains("[-]"), "body: {body}");
        assert!(body.trim_end().ends_with("not ready"), "body: {body}");
    }

    #[tokio::test]
    async fn verbose_body_when_ready() {
        let registry = make_registry().await;
        drive_executors(&registry, 3, 3).await;
        let response = get(
            Extension(status_ready()),
            Extension(Some(Arc::clone(&registry))),
            Query(ReadyQuery {
                min_ready_executors: Some(3),
                min_ready_executors_percent: None,
                verbose: true,
            }),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_string(response).await;
        assert!(body.contains("[+] components ok"), "body: {body}");
        assert!(body.contains("[+]"), "body: {body}");
        assert!(body.trim_end().ends_with("ready"), "body: {body}");
        assert!(!body.trim_end().ends_with("not ready"), "body: {body}");
    }
}
