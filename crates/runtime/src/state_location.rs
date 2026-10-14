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

//! Runs the startup check of `runtime.state.location`.
//!
//! The check retries with backoff until it passes or refuses the location. Until
//! it passes the instance is not ready and writes through the stores that
//! [`StateLocationCheck::guard`] wraps wait. A refusal stops startup:
//! [`crate::Runtime::start_servers`] returns it.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use app::App;
use object_store::ObjectStore;
use object_store::path::Path;
use runtime_state_location::{
    CheckError, StateFeature, StateLocationCheck, build_object_store, check_location,
};
use spicepod::acceleration::SnapshotBehavior;
use spicepod::component::runtime::RuntimeState;
use tokio::sync::OnceCell;
use tokio_util::sync::CancellationToken;
use util::fibonacci_backoff::FibonacciBackoffBuilder;

use crate::Runtime;
use crate::status::{ComponentStatus, RuntimeStatus};

/// The status key readiness waits on until the check passes.
pub(crate) const STATUS_KEY: &str = "state_location:runtime.state";

/// Spawns the startup check of `state` on `rt`.
pub(crate) fn spawn_check(rt: &Runtime, state: RuntimeState, check: Arc<StateLocationCheck>) {
    let status = rt.status();
    status.update_component_status(STATUS_KEY, ComponentStatus::Initializing);
    let shutdown = status.shutdown_token();
    let secrets = rt.secrets();
    let io_runtime = rt.tokio_io_runtime();
    let state = Arc::new(state);
    // Built on the first attempt that can build it, and kept for the next ones.
    let store = Arc::new(OnceCell::<(Arc<dyn ObjectStore>, Path)>::new());
    tokio::spawn(async move {
        run_check(&check, &status, &shutdown, || {
            let (secrets, io_runtime) = (Arc::clone(&secrets), io_runtime.clone());
            let (state, store) = (Arc::clone(&state), Arc::clone(&store));
            async move {
                let (store, prefix) = store
                    .get_or_try_init(|| async {
                        build_object_store(
                            secrets,
                            io_runtime,
                            &state.as_ref().into(),
                            StateFeature::StartupCheck,
                        )
                        .await
                        .map(|(store, prefix)| (store, Path::from(prefix)))
                        .map_err(|err| CheckError::unfinished(err.to_string()))
                    })
                    .await?;
                check_location(store.as_ref(), prefix, &state.location).await
            }
        })
        .await;
    });
}

/// Whether `app`'s datasets keep their snapshots in `runtime.state`: snapshots are
/// enabled for one of them and `snapshots.location` does not move them elsewhere.
pub(crate) fn snapshots_use_runtime_state(app: &App) -> bool {
    app.runtime.state.is_some()
        && runtime_acceleration::snapshot::snapshots_enabled()
        && app
            .snapshots
            .as_deref()
            .is_none_or(|snapshots| snapshots.enabled && snapshots.location.is_none())
        && app.datasets.iter().any(|dataset| {
            dataset.acceleration.as_ref().is_some_and(|acceleration| {
                acceleration.enabled && acceleration.snapshots != SnapshotBehavior::Disabled
            })
        })
}

/// Runs `attempt` with backoff until it passes or refuses the location, and
/// reports the outcome to `check` and `status`.
async fn run_check<F, Fut>(
    check: &StateLocationCheck,
    status: &RuntimeStatus,
    shutdown: &CancellationToken,
    mut attempt: F,
) where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<(), CheckError>>,
{
    status.update_component_status(STATUS_KEY, ComponentStatus::Initializing);
    let mut backoff = FibonacciBackoffBuilder::new().max_retries(None).build();
    loop {
        let err = match attempt().await {
            Ok(()) => {
                tracing::info!("{}", passed_message(check));
                status.update_component_status(STATUS_KEY, ComponentStatus::Ready);
                check.pass();
                return;
            }
            Err(err) => err,
        };
        status.update_component_status(STATUS_KEY, ComponentStatus::Error(Some(err.to_string())));
        if !err.is_retryable() {
            check.refuse(err.to_string());
            return;
        }
        let delay = backoff
            .next_duration()
            .unwrap_or_else(|| Duration::from_secs(300));
        tracing::warn!("{}", retry_message(check, &err, delay));
        tokio::select! {
            () = tokio::time::sleep(delay) => {}
            () = shutdown.cancelled() => return,
        }
    }
}

fn passed_message(check: &StateLocationCheck) -> String {
    let location = check.location();
    match feature_list(&check.features()) {
        Some(features) => format!(
            "Shared state location '{location}' enforces conditional writes and holds state for {features}."
        ),
        None => format!(
            "Shared state location '{location}' enforces conditional writes. No enabled feature keeps state in it."
        ),
    }
}

fn retry_message(check: &StateLocationCheck, err: &CheckError, delay: Duration) -> String {
    let location = check.location();
    let waiting = match feature_list(&check.guarded_features()) {
        Some(features) => {
            format!("the instance is not ready, and state writes for {features} wait")
        }
        None => "the instance is not ready".to_string(),
    };
    format!(
        "Could not check shared state location '{location}': {err}. An instance with 'runtime.state' needs read and write access to its location. Until the check passes, {waiting}. Retrying in {}s.",
        delay.as_secs().max(1)
    )
}

/// `a`, `a and b`, `a, b and c`, or `None` for no features.
fn feature_list(features: &[StateFeature]) -> Option<String> {
    let names: Vec<_> = features
        .iter()
        .map(|feature| feature.display_name())
        .collect();
    match names.as_slice() {
        [] => None,
        [only] => Some((*only).to_string()),
        [rest @ .., last] => Some(format!("{} and {last}", rest.join(", "))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::ObjectStoreExt;
    use object_store::memory::InMemory;
    use std::sync::atomic::{AtomicUsize, Ordering};

    const LOCATION: &str = "s3://my-bucket/spice/";

    fn status() -> Arc<RuntimeStatus> {
        RuntimeStatus::new()
    }

    fn component_status(status: &RuntimeStatus) -> Option<ComponentStatus> {
        status.get_all_statuses().get(STATUS_KEY).cloned()
    }

    /// Captures the log lines `run_check` writes, at INFO and above.
    struct Logs(Arc<parking_lot::Mutex<Vec<u8>>>);

    impl std::io::Write for Logs {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    fn capture_logs() -> (
        Arc<parking_lot::Mutex<Vec<u8>>>,
        tracing::subscriber::DefaultGuard,
    ) {
        let sink = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let writer = Arc::clone(&sink);
        let subscriber = tracing_subscriber::fmt()
            .with_writer(move || Logs(Arc::clone(&writer)))
            .with_ansi(false)
            .with_max_level(tracing::Level::INFO)
            .finish();
        (sink, tracing::subscriber::set_default(subscriber))
    }

    fn lines(sink: &parking_lot::Mutex<Vec<u8>>) -> Vec<String> {
        String::from_utf8_lossy(&sink.lock())
            .lines()
            .map(str::to_owned)
            .collect()
    }

    /// Fails the first `failures` attempts with a retryable error, as a store
    /// without write access or a missing bucket does, then checks `store`.
    #[tokio::test(start_paused = true)]
    async fn retries_until_the_location_can_be_checked_then_passes() {
        let (sink, _logs) = capture_logs();
        let store = InMemory::new();
        let check = StateLocationCheck::new(LOCATION);
        let guarded = check.guard(StateFeature::Scheduler, Arc::new(InMemory::new()));
        check.record_feature(StateFeature::Snapshots);
        let status = status();
        let attempts = AtomicUsize::new(0);

        let pending_write = tokio::spawn(async move {
            guarded
                .put(&Path::from("cluster/cluster.json"), "{}".into())
                .await
        });

        let (status_ref, attempts_ref, store_ref) = (&status, &attempts, &store);
        run_check(
            &check,
            &status,
            &CancellationToken::new(),
            move || async move {
                if attempts_ref.fetch_add(1, Ordering::SeqCst) < 2 {
                    let status = status_ref;
                    assert!(
                        !status.is_ready(),
                        "the instance is not ready while it waits"
                    );
                    return Err(CheckError::unfinished(
                        "could not write a probe object: The specified bucket does not exist",
                    ));
                }
                check_location(store_ref, &Path::from("spice/_spice"), LOCATION).await
            },
        )
        .await;

        assert_eq!(attempts.load(Ordering::SeqCst), 3);
        assert_eq!(component_status(&status), Some(ComponentStatus::Ready));
        assert!(status.is_ready());
        assert_eq!(check.finished().await, None);
        pending_write
            .await
            .expect("join")
            .expect("a write that waited lands once the check passes");

        let logged = lines(&sink);
        let retries: Vec<_> = logged
            .iter()
            .filter(|line| line.contains(" WARN "))
            .collect();
        assert_eq!(retries.len(), 2, "each retry logs its cause: {logged:#?}");
        for retry in retries {
            assert!(
                retry.contains(
                    "Could not check shared state location 's3://my-bucket/spice/': could not write a probe object: The specified bucket does not exist. An instance with 'runtime.state' needs read and write access to its location. Until the check passes, the instance is not ready, and state writes for the cluster scheduler wait. Retrying in "
                ),
                "{retry}"
            );
        }
        let infos: Vec<_> = logged
            .iter()
            .filter(|line| line.contains(" INFO "))
            .collect();
        assert_eq!(infos.len(), 1, "one INFO line: {logged:#?}");
        assert!(
            infos[0].ends_with(
                "Shared state location 's3://my-bucket/spice/' enforces conditional writes and holds state for the cluster scheduler and acceleration snapshots."
            ),
            "{}",
            infos[0]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_refusal_stops_retrying_and_fails_waiting_writes() {
        let check = StateLocationCheck::new(LOCATION);
        let guarded = check.guard(StateFeature::RateControl, Arc::new(InMemory::new()));
        let status = status();
        let attempts = AtomicUsize::new(0);

        let attempts_ref = &attempts;
        run_check(
            &check,
            &status,
            &CancellationToken::new(),
            move || async move {
                attempts_ref.fetch_add(1, Ordering::SeqCst);
                Err(CheckError::NewerFormat {
                    location: LOCATION.to_string(),
                    version: 2,
                })
            },
        )
        .await;

        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        let reason = check.finished().await.expect("the location is refused");
        assert!(reason.contains("has format version 2"), "{reason}");
        assert!(matches!(
            component_status(&status),
            Some(ComponentStatus::Error(Some(_)))
        ));
        assert!(!status.is_ready());
        guarded
            .put(&Path::from("rate-control/a.json"), "a".into())
            .await
            .expect_err("a refused location takes no writes");
    }

    #[test]
    fn passed_message_without_features() {
        let check = StateLocationCheck::new(LOCATION);
        assert_eq!(
            passed_message(&check),
            "Shared state location 's3://my-bucket/spice/' enforces conditional writes. No enabled feature keeps state in it."
        );
        check.record_feature(StateFeature::RateControl);
        check.record_feature(StateFeature::Scheduler);
        check.record_feature(StateFeature::ResultsCacheWarmup);
        assert_eq!(
            passed_message(&check),
            "Shared state location 's3://my-bucket/spice/' enforces conditional writes and holds state for the cluster scheduler, source rate control and SQL results cache warmup."
        );
    }

    #[tokio::test]
    async fn spawned_check_passes_on_a_file_location_and_writes_format_json() {
        let dir = tempfile::tempdir().expect("tempdir");
        let state = RuntimeState {
            location: format!("file://{}", dir.path().display()),
            params: None,
        };
        let rt = crate::builder::RuntimeBuilder::new().build().await;
        let check = StateLocationCheck::new(&state.location);
        spawn_check(&rt, state, Arc::clone(&check));

        assert_eq!(check.finished().await, None);
        let format = std::fs::read_to_string(dir.path().join("_spice/format.json"))
            .expect("format.json is created");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&format).expect("JSON"),
            serde_json::json!({ "format-version": runtime_state_location::FORMAT_VERSION })
        );
        assert_eq!(component_status(&rt.status()), Some(ComponentStatus::Ready));
    }
}
