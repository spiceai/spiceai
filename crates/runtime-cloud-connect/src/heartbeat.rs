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

use crate::handlers::{Capability, RuntimeHandle, RuntimePhase};
use crate::proto;

/// Build a heartbeat for the current runtime state.
///
/// The counters here are the fixed set the control plane renders per
/// instance. They are deliberately *not* mirrored into
/// [`build_telemetry`]'s open metrics map: one datum, one channel, so the
/// two can never disagree about the same number.
pub(crate) async fn build_heartbeat(
    identifier: &str,
    sequence: u64,
    runtime: &Arc<dyn RuntimeHandle>,
) -> proto::Heartbeat {
    let active_datasets = runtime.active_datasets().await;
    let active_models = runtime.active_models().await;

    proto::Heartbeat {
        identifier: identifier.to_string(),
        sequence,
        phase: runtime_phase(runtime).await as i32,
        warnings: Vec::new(),
        active_datasets,
        active_models,
        active_spicepods: 0,
        runtime_versions: std::collections::HashMap::new(),
        // Absent, not empty: this runtime does not report deployment restart
        // state yet, and on the wire "no detail reported" and "nothing needs a
        // restart" are different states. Populate it only from a real
        // restart-required source of truth, never with a placeholder.
        standalone_runtime: None,
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
        let hb = build_heartbeat("inst_test", 42, &runtime).await;
        assert_eq!(hb.identifier, "inst_test");
        assert_eq!(hb.sequence, 42);
        // A handle that cannot report status must not claim a phase.
        assert_eq!(hb.phase, proto::RuntimePhase::Unspecified as i32);
        // A runtime with no restart-state source of truth reports no
        // standalone detail — absent, never a present-but-empty placeholder,
        // since the control plane reads those as different states.
        assert_eq!(hb.standalone_runtime, None);
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
        let hb = build_heartbeat("inst_test", 1, &runtime).await;
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
