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

//! `runtime.adaptive_tuning` selects the Cayenne tuning mode for every Cayenne-accelerated
//! dataset. These tests load a real Spicepod from disk, through the runtime, and read the
//! mode each resolved Cayenne table actually runs with.
//!
//! The old per-dataset `cayenne_tuning` is no longer read, so a dataset that still sets it
//! stays `disabled` and is told once, and an invalid `runtime.adaptive_tuning` fails the load.

#![cfg(not(windows))]
#![recursion_limit = "256"]
#![expect(clippy::expect_used)]

// Accelerator engines self-register through a linkme slice; the linker drops an unreferenced
// slice static, so a binary exercising Cayenne must name the crate itself.
use accelerator_cayenne as _;

use std::io::Write;
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use app::AppBuilder;
use datafusion::common::TableReference;
use parking_lot::Mutex;
use runtime::Runtime;
use runtime::accelerated::AcceleratedTable;
use tracing_subscriber::fmt::MakeWriter;

/// Everything logged by any thread of this test binary. Dataset loads run on worker
/// threads, so a thread-local subscriber would miss the loader's warnings.
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

/// A Spicepod with one Cayenne-accelerated `file://` dataset called `dataset`.
/// `runtime_params` and `dataset_params` are YAML mapping bodies, each line already
/// indented for its place in the pod.
fn spicepod(
    dataset: &str,
    dir: &std::path::Path,
    runtime_params: &str,
    dataset_params: &str,
) -> String {
    spicepod_with_runtime_fields(dataset, dir, "", runtime_params, dataset_params)
}

/// Like [`spicepod`], with `runtime_fields` (each line indented two spaces) set directly
/// under `runtime:`, beside `params:`.
fn spicepod_with_runtime_fields(
    dataset: &str,
    dir: &std::path::Path,
    runtime_fields: &str,
    runtime_params: &str,
    dataset_params: &str,
) -> String {
    let csv = dir.join("rows.csv");
    std::fs::write(&csv, "id,name\n1,a\n2,b\n3,c\n").expect("the fixture CSV must be written");
    format!(
        "version: v1\n\
         kind: Spicepod\n\
         name: cayenne_runtime_tuning\n\
         runtime:\n{runtime_fields}  params:\n{runtime_params}    cdc_prefetch_buffer: '16'\n\
         datasets:\n\
         \x20 - from: file://{csv}\n\
         \x20   name: {dataset}\n\
         \x20   acceleration:\n\
         \x20     enabled: true\n\
         \x20     engine: cayenne\n\
         \x20     mode: file\n\
         \x20     refresh_mode: full\n\
         \x20     params:\n\
         \x20       cayenne_file_path: {data}\n\
         \x20       cayenne_metadata_dir: {meta}\n\
         {interval}\
         {dataset_params}",
        csv = csv.display(),
        data = dir.join("data").display(),
        meta = dir.join("metadata").display(),
        interval = if dataset_params.contains("cayenne_compaction_background_interval_ms") {
            ""
        } else {
            "        cayenne_compaction_background_interval_ms: '10000'\n"
        },
    )
}

/// Loads `pod` through the runtime and returns whether `dataset`'s Cayenne table runs the
/// closed-loop tuner.
async fn dynamic_tuning_of(dataset: &str, pod: &str, dir: &std::path::Path) -> bool {
    LazyLock::force(&LOGS);
    std::fs::write(dir.join("spicepod.yaml"), pod).expect("the spicepod must be written");
    let app = AppBuilder::build_from_path(dir.to_path_buf())
        .await
        .expect("the spicepod must load");
    let rt = Arc::new(Runtime::builder().with_app(app).build().await);

    tokio::time::timeout(Duration::from_mins(2), Arc::clone(&rt).load_components())
        .await
        .expect("components must load");

    let table = rt
        .datafusion()
        .get_table(&TableReference::bare(dataset))
        .await
        .expect("the dataset must be registered");
    let accelerated =
        spice_table::find_layer::<AcceleratedTable>(table.as_ref(), spice_table::LayerWalk::Read)
            .expect("the dataset must be accelerated");
    let accelerator = accelerated.get_accelerator();
    let cayenne = accelerator
        .downcast_ref::<cayenne::CayenneTableProvider>()
        .or_else(|| {
            spice_table::nodes(accelerator.as_ref(), spice_table::LayerWalk::Read).find_map(
                |node| {
                    node.base_provider()
                        .downcast_ref::<cayenne::CayenneTableProvider>()
                },
            )
        })
        .expect("the accelerator must be a Cayenne table");
    cayenne.metadata().vortex_config.dynamic_tuning
}

#[tokio::test(flavor = "multi_thread")]
async fn runtime_tuning_adaptive_turns_adaptive_on_for_the_dataset() {
    let dir = tempfile::tempdir().expect("temp dir");
    let pod = spicepod_with_runtime_fields(
        "rt_adaptive",
        dir.path(),
        "  adaptive_tuning: enabled\n",
        "",
        "",
    );
    assert!(
        dynamic_tuning_of("rt_adaptive", &pod, dir.path()).await,
        "`runtime.adaptive_tuning: enabled` must run the closed-loop tuner"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn runtime_params_adaptive_tuning_does_not_turn_adaptive_on() {
    let dir = tempfile::tempdir().expect("temp dir");
    let pod = spicepod(
        "rt_params_switch",
        dir.path(),
        "    adaptive_tuning: enabled\n",
        "",
    );
    assert!(
        !dynamic_tuning_of("rt_params_switch", &pod, dir.path()).await,
        "`runtime.params.adaptive_tuning` is not the switch; only `runtime.adaptive_tuning` is"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn runtime_tuning_defaults_to_auto() {
    let dir = tempfile::tempdir().expect("temp dir");
    let pod = spicepod("rt_default", dir.path(), "", "");
    assert!(
        !dynamic_tuning_of("rt_default", &pod, dir.path()).await,
        "without `runtime.adaptive_tuning` the dataset runs static tuning"
    );
}

/// A hot reload that only adds a retired parameter re-resolves the same table with an
/// otherwise identical config, and must still tell the operator the parameter has no effect.
#[tokio::test(flavor = "multi_thread")]
async fn retired_param_added_by_a_reload_warns_once() {
    let dir = tempfile::tempdir().expect("temp dir");
    let before = spicepod("rt_reload", dir.path(), "", "");
    assert!(!dynamic_tuning_of("rt_reload", &before, dir.path()).await);
    let warning = "Dataset 'rt_reload' sets `cayenne_tuning`, which is no longer a dataset parameter, so it has no effect. Set `runtime.adaptive_tuning` instead. See: https://spiceai.org/docs/reference/spicepod/runtime";
    assert_eq!(logged().matches(warning).count(), 0);

    let after = spicepod(
        "rt_reload",
        dir.path(),
        "",
        "        cayenne_tuning: enabled\n",
    );
    assert!(!dynamic_tuning_of("rt_reload", &after, dir.path()).await);
    // The same dataset loading again, as a retry or another reload would, does not repeat it.
    assert!(!dynamic_tuning_of("rt_reload", &after, dir.path()).await);
    let logs = logged();
    assert_eq!(
        logs.matches(warning).count(),
        1,
        "a retired parameter added by a reload must warn once, logs: {logs}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn unprefixed_retired_dataset_params_are_not_applied_and_warn_once() {
    let dir = tempfile::tempdir().expect("temp dir");
    let pod = spicepod(
        "rt_unprefixed",
        dir.path(),
        "",
        "        tuning: enabled\n        goal_freshness: 5s\n        cayenne_goal_qph: \'100\'\n        goal_qph: \'100\'\n",
    );
    assert!(
        !dynamic_tuning_of("rt_unprefixed", &pod, dir.path()).await,
        "the retired unprefixed dataset `tuning` must not turn the closed-loop tuner on"
    );

    let logs = logged();
    for warning in [
        "Dataset 'rt_unprefixed' sets `tuning`, which is no longer a dataset parameter, so it has no effect. Set `runtime.adaptive_tuning` instead. See: https://spiceai.org/docs/reference/spicepod/runtime",
        "Dataset 'rt_unprefixed' sets `goal_freshness`, which is no longer a dataset parameter, so it has no effect. Set `runtime.params.target_freshness` instead. See: https://spiceai.org/docs/reference/spicepod/runtime",
        "Dataset 'rt_unprefixed' sets `cayenne_goal_qph`, which is no longer a dataset parameter, so it has no effect. Set `runtime.params.target_qph` instead. See: https://spiceai.org/docs/reference/spicepod/runtime",
        "Dataset 'rt_unprefixed' sets `goal_qph`, which is no longer a dataset parameter, so it has no effect. Set `runtime.params.target_qph` instead. See: https://spiceai.org/docs/reference/spicepod/runtime",
    ] {
        assert_eq!(
            logs.matches(warning).count(),
            1,
            "expected exactly one retired-parameter warning, logs: {logs}"
        );
    }
    for generic in [
        "Ignoring parameter `tuning`",
        "Ignoring parameter `goal_freshness`",
        "Ignoring parameter `cayenne_goal_qph`",
        "Ignoring parameter `goal_qph`",
    ] {
        assert!(
            !logs.contains(generic),
            "the generic unsupported-parameter warning must not repeat it, logs: {logs}"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn compaction_fallback_with_targets_is_one_warning_and_not_reported_as_disabled() {
    let dir = tempfile::tempdir().expect("temp dir");
    let pod = spicepod_with_runtime_fields(
        "rt_fallback",
        dir.path(),
        "  adaptive_tuning: enabled\n",
        "    target_freshness: 5s\n",
        "        cayenne_compaction_background_interval_ms: '0'\n",
    );
    assert!(
        !dynamic_tuning_of("rt_fallback", &pod, dir.path()).await,
        "background compaction off must fall back to static tuning"
    );

    let logs = logged();
    assert!(
        !logs.contains("`runtime.adaptive_tuning` is `disabled`, so dataset 'rt_fallback'"),
        "targets set with `adaptive_tuning: enabled` must not be reported as ignored by `disabled`, logs: {logs}"
    );
    let fallback = "Dataset 'rt_fallback' cannot use adaptive tuning because";
    assert_eq!(
        logs.matches(fallback).count(),
        1,
        "expected exactly one fallback warning, logs: {logs}"
    );
    assert!(
        logs.contains("The `runtime.params.target_*` targets are ignored too."),
        "the fallback warning must say the targets are ignored too, logs: {logs}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn retired_dataset_tuning_param_is_not_applied_and_warns_once() {
    let dir = tempfile::tempdir().expect("temp dir");
    let pod = spicepod(
        "rt_retired",
        dir.path(),
        "",
        "        cayenne_tuning: adaptive\n",
    );
    assert!(
        !dynamic_tuning_of("rt_retired", &pod, dir.path()).await,
        "the retired dataset `cayenne_tuning` must not turn the closed-loop tuner on"
    );

    let logs = logged();
    let warning = "Dataset 'rt_retired' sets `cayenne_tuning`, which is no longer a dataset parameter, so it has no effect. Set `runtime.adaptive_tuning` instead. See: https://spiceai.org/docs/reference/spicepod/runtime";
    assert_eq!(
        logs.matches(warning).count(),
        1,
        "expected exactly one retired-parameter warning, logs: {logs}"
    );
    assert!(
        !logs.contains("Ignoring parameter `cayenne_tuning`"),
        "the generic not-supported warning must not repeat the retired-parameter one: {logs}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn renamed_runtime_target_param_warns_once_and_is_not_reported_as_unknown() {
    let dir = tempfile::tempdir().expect("temp dir");
    let pod = spicepod(
        "rt_renamed",
        dir.path(),
        "    cayenne_goal_freshness: 5s\n",
        "",
    );
    let _ = dynamic_tuning_of("rt_renamed", &pod, dir.path()).await;

    let logs = logged();
    let warning = "`runtime.params.cayenne_goal_freshness` has been renamed, so it has no effect. Set `runtime.params.target_freshness` instead. See: https://spiceai.org/docs/reference/spicepod/runtime";
    assert_eq!(logs.matches(warning).count(), 1, "logs: {logs}");
    assert!(
        !logs.contains(
            "runtime.params.cayenne_goal_freshness is not a recognized runtime parameter"
        ),
        "a renamed key must not also be reported as unknown: {logs}"
    );
}

#[tokio::test]
async fn invalid_runtime_tuning_fails_the_load() {
    let dir = tempfile::tempdir().expect("temp dir");
    std::fs::write(
        dir.path().join("spicepod.yaml"),
        spicepod_with_runtime_fields(
            "rt_invalid",
            dir.path(),
            "  adaptive_tuning: enablde\n",
            "",
            "",
        ),
    )
    .expect("the spicepod must be written");

    let error = AppBuilder::build_from_path(dir.path().to_path_buf())
        .await
        .expect_err("an invalid `runtime.adaptive_tuning` must fail the load");
    let message = format!("{error:?} {error}");
    assert!(
        message.contains("Invalid `runtime.adaptive_tuning` value 'enablde': expected `enabled` or `disabled`. See: https://spiceai.org/docs/reference/spicepod/runtime"),
        "unexpected error: {message}"
    );
}
