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

use std::sync::Arc;
use std::time::Duration;

use crate::handlers::RuntimeHandle;
use crate::proto;

/// Default heartbeat cadence.
pub(crate) const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);

/// Default telemetry cadence.
pub(crate) const TELEMETRY_INTERVAL: Duration = Duration::from_secs(60);

/// Build a heartbeat for the current runtime state.
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
        status: "online".to_string(),
        warnings: Vec::new(),
        active_datasets,
        active_models,
        active_spicepods: 0,
        runtime_versions: std::collections::HashMap::new(),
    }
}

/// Build a stub telemetry frame. The shape is intentionally minimal in
/// v0 — we send the runtime liveness counters and let the server add
/// richer metrics over time.
pub(crate) async fn build_telemetry(
    identifier: &str,
    window_start_unix: u64,
    window_end_unix: u64,
    runtime: &Arc<dyn RuntimeHandle>,
) -> proto::Telemetry {
    let mut metrics = std::collections::HashMap::new();
    metrics.insert(
        "datasets_active".to_string(),
        f64::from(runtime.active_datasets().await),
    );
    metrics.insert(
        "models_active".to_string(),
        f64::from(runtime.active_models().await),
    );
    proto::Telemetry {
        identifier: identifier.to_string(),
        window_start_unix,
        window_end_unix,
        metrics,
    }
}

/// Current Unix timestamp (seconds), clamped to 0 if the system clock
/// is before the epoch.
#[must_use]
pub(crate) fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
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
        assert_eq!(hb.status, "online");
    }

    #[tokio::test]
    async fn telemetry_includes_baseline_metrics() {
        let runtime: Arc<dyn RuntimeHandle> = Arc::new(NoopRuntimeHandle);
        let t = build_telemetry("inst_test", 1, 2, &runtime).await;
        assert!(t.metrics.contains_key("datasets_active"));
        assert!(t.metrics.contains_key("models_active"));
    }
}
