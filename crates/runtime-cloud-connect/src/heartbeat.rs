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

//! Helpers for building [`Heartbeat`] and [`Telemetry`] payloads.
//!
//! The heartbeat / telemetry cadences live on [`crate::CloudConnectConfig`]
//! (see [`crate::config::DEFAULT_HEARTBEAT_INTERVAL`] /
//! [`crate::config::DEFAULT_TELEMETRY_INTERVAL`]) so they can be overridden
//! for tests; the driver reads them from the config.

use std::sync::Arc;

use crate::handlers::{Capability, DeployState, RuntimeHandle, RuntimePhase};
use crate::proto;

/// Build a heartbeat for the current runtime state.
///
/// The counters here are the fixed set the control plane renders per
/// instance. They are deliberately *not* mirrored into
/// [`build_telemetry`]'s open metrics map: one datum, one channel, so the
/// two can never disagree about the same number.
///
/// `reported` is the deploy state last sent on this connection — by the `Hello`
/// that opened it, or by an earlier heartbeat. A `DeployState` rides along only
/// when it differs from that, because each one *replaces* what the control plane
/// holds and a frame carrying none leaves the previous report intact. Repeating
/// an unchanged report every heartbeat would say nothing; omitting a *changed*
/// one would leave a rejected deployment unreported until the next reconnect,
/// which for a validation failure never comes — nothing restarted.
pub(crate) async fn build_heartbeat(
    identifier: &str,
    sequence: u64,
    runtime: &Arc<dyn RuntimeHandle>,
    reported: &mut Option<DeployState>,
) -> proto::Heartbeat {
    let active_datasets = runtime.active_datasets().await;
    let active_models = runtime.active_models().await;

    let deploy_state = match runtime.deploy_state().await {
        Some(state) if reported.as_ref() != Some(&state) => {
            let frame = deploy_state_proto(&state);
            *reported = Some(state);
            Some(frame)
        }
        // Unchanged, or an adapter that does not report deploy versions at all.
        _ => None,
    };

    proto::Heartbeat {
        identifier: identifier.to_string(),
        sequence,
        phase: runtime_phase(runtime).await as i32,
        warnings: Vec::new(),
        active_datasets,
        active_models,
        active_spicepods: 0,
        runtime_versions: std::collections::HashMap::new(),
        deploy_state,
    }
}

/// The wire form of a [`DeployState`].
///
/// Lives here rather than on the handler type so nothing in `handlers` names a
/// generated proto type — the trait stays the crate's stable surface and the
/// client maps it onto the wire.
pub(crate) fn deploy_state_proto(state: &DeployState) -> proto::DeployState {
    proto::DeployState {
        applied_deployment_version: state.applied_deployment_version,
        failed_deployment_version: state.failed_deployment_version,
        failure_message: state.failure_message.clone(),
    }
}

/// The runtime's coarse phase, from the same `status()` call that answers
/// `GetStatus` — so the heartbeat and the status document never report the
/// runtime differently. A handle that cannot report status leaves the phase
/// unspecified rather than guessing at "online".
async fn runtime_phase(runtime: &Arc<dyn RuntimeHandle>) -> proto::RuntimePhase {
    if !runtime.supports(Capability::GetStatus) {
        return proto::RuntimePhase::Unspecified;
    }
    match runtime.status().await {
        Ok(report) => match report.phase {
            RuntimePhase::Unspecified => proto::RuntimePhase::Unspecified,
            RuntimePhase::Ready => proto::RuntimePhase::Ready,
            RuntimePhase::Progressing => proto::RuntimePhase::Progressing,
            RuntimePhase::Failed => proto::RuntimePhase::Failed,
        },
        Err(err) => {
            tracing::debug!(
                "Cloud Connect: could not read runtime status for the heartbeat: {err}"
            );
            proto::RuntimePhase::Unspecified
        }
    }
}

/// Build a telemetry frame for the closed window.
///
/// The metrics map is the open channel for everything *outside* the fixed
/// counter set on [`build_heartbeat`] — a datum belongs to one channel or the
/// other, never both. Nothing populates it yet; the frame is still sent so
/// the control plane can tell "this window carried no metrics" from "this
/// instance stopped reporting".
pub(crate) fn build_telemetry(
    identifier: &str,
    window_start_unix: u64,
    window_end_unix: u64,
) -> proto::Telemetry {
    proto::Telemetry {
        identifier: identifier.to_string(),
        window_start: Some(timestamp(window_start_unix)),
        window_end: Some(timestamp(window_end_unix)),
        metrics: std::collections::HashMap::new(),
    }
}

/// A wire timestamp from Unix seconds. Seconds only — the client has no
/// sub-second sources.
fn timestamp(unix_seconds: u64) -> prost_types::Timestamp {
    prost_types::Timestamp {
        seconds: i64::try_from(unix_seconds).unwrap_or(i64::MAX),
        nanos: 0,
    }
}

/// Current Unix timestamp (seconds), clamped to 0 if the system clock
/// is before the epoch.
#[must_use]
pub(crate) fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handlers::NoopRuntimeHandle;

    #[tokio::test]
    async fn heartbeat_contains_identifier_and_sequence() {
        let runtime: Arc<dyn RuntimeHandle> = Arc::new(NoopRuntimeHandle);
        let mut reported = None;
        let hb = build_heartbeat("inst_test", 42, &runtime, &mut reported).await;
        assert_eq!(hb.identifier, "inst_test");
        assert_eq!(hb.sequence, 42);
        // A handle that cannot report status must not claim a phase.
        assert_eq!(hb.phase, proto::RuntimePhase::Unspecified as i32);
        assert!(
            hb.deploy_state.is_none(),
            "a handle that does not report deploy versions must attach no DeployState"
        );
        assert_eq!(reported, None);
    }

    /// A `DeployState` rides a heartbeat only when it has changed. The control
    /// plane replaces its record with every one it receives, so repeating an
    /// unchanged state says nothing — and a heartbeat with none left the previous
    /// report standing, which is what makes the omission correct rather than lossy.
    #[tokio::test]
    async fn a_deploy_state_rides_a_heartbeat_only_when_it_changes() {
        use async_trait::async_trait;
        use std::sync::Mutex;

        struct Reporting {
            state: Mutex<DeployState>,
        }

        impl Reporting {
            fn set(&self, state: DeployState) {
                *self.state.lock().expect("deploy state lock") = state;
            }
        }

        #[async_trait]
        impl RuntimeHandle for Reporting {
            fn supports(&self, capability: Capability) -> bool {
                capability == Capability::DeployVersions
            }
            async fn deploy_state(&self) -> Option<DeployState> {
                Some(self.state.lock().expect("deploy state lock").clone())
            }
        }

        let handle = Arc::new(Reporting {
            state: Mutex::new(DeployState::applied(7)),
        });
        let runtime: Arc<dyn RuntimeHandle> = Arc::clone(&handle) as Arc<dyn RuntimeHandle>;

        // Nothing reported yet on this connection: the first heartbeat carries it.
        let mut reported = None;
        let first = build_heartbeat("inst_test", 1, &runtime, &mut reported).await;
        let state = first.deploy_state.expect("the first state is news");
        assert_eq!(state.applied_deployment_version, Some(7));

        // Unchanged: nothing to say.
        let second = build_heartbeat("inst_test", 2, &runtime, &mut reported).await;
        assert!(second.deploy_state.is_none());

        // A rejected deployment is news, and this is the only frame that can
        // carry it — nothing restarted, so no new Hello follows.
        handle.set(DeployState::applied(7).with_failure(8, "invalid spicepod"));
        let third = build_heartbeat("inst_test", 3, &runtime, &mut reported).await;
        let state = third.deploy_state.expect("a new failure is news");
        assert_eq!(state.applied_deployment_version, Some(7));
        assert_eq!(state.failed_deployment_version, Some(8));
        assert_eq!(state.failure_message, "invalid spicepod");

        // Superseded: clearing the failure is news too, or a later deployment
        // reusing version 8 would be failed by a stale report.
        handle.set(DeployState::applied(9));
        let fourth = build_heartbeat("inst_test", 4, &runtime, &mut reported).await;
        let state = fourth.deploy_state.expect("clearing a failure is news");
        assert_eq!(state.applied_deployment_version, Some(9));
        assert_eq!(state.failed_deployment_version, None);
        assert!(state.failure_message.is_empty());
    }

    /// A `Hello` seeds what has been reported, so the heartbeat right behind it
    /// does not repeat the state the Hello just sent.
    #[tokio::test]
    async fn a_state_sent_on_the_hello_is_not_repeated_by_the_next_heartbeat() {
        use async_trait::async_trait;

        struct Reporting;

        #[async_trait]
        impl RuntimeHandle for Reporting {
            fn supports(&self, capability: Capability) -> bool {
                capability == Capability::DeployVersions
            }
            async fn deploy_state(&self) -> Option<DeployState> {
                Some(DeployState::applied(3))
            }
        }

        let runtime: Arc<dyn RuntimeHandle> = Arc::new(Reporting);
        let mut reported = runtime.deploy_state().await; // what the Hello carried
        let hb = build_heartbeat("inst_test", 1, &runtime, &mut reported).await;
        assert!(hb.deploy_state.is_none());
    }

    #[tokio::test]
    async fn heartbeat_phase_comes_from_the_status_report() {
        use crate::handlers::{Capability, CommandError, StatusReport};
        use async_trait::async_trait;

        struct ReadyHandle;

        #[async_trait]
        impl RuntimeHandle for ReadyHandle {
            fn supports(&self, capability: Capability) -> bool {
                capability == Capability::GetStatus
            }
            async fn status(&self) -> Result<StatusReport, CommandError> {
                Ok(StatusReport::new(RuntimePhase::Ready, "all ready"))
            }
        }

        let runtime: Arc<dyn RuntimeHandle> = Arc::new(ReadyHandle);
        let mut reported = None;
        let hb = build_heartbeat("inst_test", 1, &runtime, &mut reported).await;
        assert_eq!(hb.phase, proto::RuntimePhase::Ready as i32);
    }

    #[test]
    fn telemetry_carries_the_window_and_no_heartbeat_counters() {
        let t = build_telemetry("inst_test", 1, 2);
        assert_eq!(t.window_start.map(|ts| ts.seconds), Some(1));
        assert_eq!(t.window_end.map(|ts| ts.seconds), Some(2));
        // The dataset/model counters ride on the Heartbeat, and only there.
        assert!(!t.metrics.contains_key("datasets_active"));
        assert!(!t.metrics.contains_key("models_active"));
    }
}
