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

//! End-to-end coverage for `runtime.cpu.cores`: spicepod -> `CpuBudget` ->
//! the tokio pools it sizes.
//!
//! Unit tests cover the detection ladder and the derivations in isolation
//! (`crates/cpu-budget`); what only an integration test can catch is the wiring
//! — that the spicepod is loaded before the pools are built, that the value
//! survives `build_app`, and that a dedicated runtime really comes up with the
//! derived worker count rather than inheriting tokio's own default.
//!
//! The process-wide budget is a `OnceLock`, so exactly one test in this binary
//! may install one. The others assert on `build_app` alone, which never touches
//! it.

use std::path::{Path, PathBuf};

use clap::Parser;

/// Write a spicepod containing `runtime_yaml` and return its directory.
fn spicepod_dir(dir: &Path, runtime_yaml: &str) -> PathBuf {
    let path = dir.join("spicepod.yaml");
    std::fs::write(
        &path,
        format!("version: v1\nkind: Spicepod\nname: cpu-budget-test\nruntime:\n{runtime_yaml}"),
    )
    .expect("writes the spicepod");
    dir.to_path_buf()
}

fn build_app(args: &spiced::Args) -> Option<std::sync::Arc<app::App>> {
    let bootstrap = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("builds the bootstrap runtime");
    bootstrap
        .block_on(spiced::build_app(args))
        .expect("loads the spicepod")
        .app
}

fn configured_cores(app: Option<&app::App>) -> Option<String> {
    app.and_then(|app| app.runtime.cpu.as_ref())
        .and_then(|cpu| cpu.cores.as_ref())
        .map(ToString::to_string)
}

/// The acceptance case: `runtime.cpu.cores: 2` must size the main runtime for 2
/// workers and each dedicated runtime for 1, on a host with any number of cores.
#[test]
fn spicepod_cores_size_the_runtime_pools() {
    let dir = tempfile::tempdir().expect("creates a temp dir");
    let args = spiced::Args::parse_from([
        "spiced",
        spicepod_dir(dir.path(), "  cpu:\n    cores: 2\n")
            .to_str()
            .expect("utf-8 path"),
    ]);

    let app = build_app(&args);
    assert_eq!(configured_cores(app.as_deref()).as_deref(), Some("2"));

    spiced::install_cpu_budget(&args, app.as_deref()).expect("installs the budget");

    let budget = cpu_budget::cpu_budget();
    assert_eq!(budget.cores(), 2);
    assert_eq!(budget.source(), cpu_budget::CpuSource::Configured);
    assert_eq!(budget.main_runtime_worker_threads(), 2);
    assert_eq!(budget.dedicated_runtime_worker_threads(), 1);

    // The dedicated pools (cpu, refresh, cdc_apply, compaction) are all built
    // through this one builder, and this is the number `tokio_runtime_workers`
    // reports for each of them.
    let dedicated = runtime_async::ManagedTokioRuntime::try_new().expect("builds a pool");
    assert_eq!(dedicated.handle().metrics().num_workers(), 1);

    // Installing the budget also declares it to Vortex. Like the budget, the
    // declaration resolves once per process, so only the test that installs
    // the budget can assert it.
    #[cfg(not(windows))]
    assert_eq!(
        vortex_utils::parallelism::get_available_parallelism(),
        Some(2)
    );
}

/// `--set-runtime cpu.cores=4` reaches the typed field, so the override surface
/// works for free rather than needing its own plumbing — both when it replaces a
/// configured value and when it has to create the `cpu` section from nothing.
#[test]
fn set_runtime_override_reaches_the_cpu_section() {
    for existing in ["  cpu:\n    cores: 2\n", "  dataset_load_parallelism: 1\n"] {
        let dir = tempfile::tempdir().expect("creates a temp dir");
        let args = spiced::Args::parse_from([
            "spiced",
            "--set-runtime",
            "cpu.cores=4",
            spicepod_dir(dir.path(), existing)
                .to_str()
                .expect("utf-8 path"),
        ]);

        assert_eq!(
            configured_cores(build_app(&args).as_deref()).as_deref(),
            Some("4"),
            "{existing}"
        );
    }
}

/// A cluster executor is the deployment shape most likely to be running under a
/// CPU request, and it starts from a default `App` with only a few fields copied
/// across. `runtime.cpu` has to be one of them.
#[test]
fn cluster_executor_keeps_the_cpu_section_from_its_spicepod() {
    let dir = tempfile::tempdir().expect("creates a temp dir");
    let args = spiced::Args::parse_from([
        "spiced",
        "--scheduler-address",
        "http://127.0.0.1:50051",
        spicepod_dir(dir.path(), "  cpu:\n    cores: 3500m\n")
            .to_str()
            .expect("utf-8 path"),
    ]);

    assert_eq!(
        configured_cores(build_app(&args).as_deref()).as_deref(),
        Some("3500m")
    );
}

/// A value that is not a CPU quantity fails startup with an actionable message,
/// rather than silently clamping to something the operator did not ask for.
#[test]
fn invalid_cores_fails_startup() {
    let dir = tempfile::tempdir().expect("creates a temp dir");
    let args = spiced::Args::parse_from([
        "spiced",
        spicepod_dir(dir.path(), "  cpu:\n    cores: 0\n")
            .to_str()
            .expect("utf-8 path"),
    ]);

    let app = build_app(&args);
    let err = spiced::install_cpu_budget(&args, app.as_deref())
        .expect_err("`cores: 0` must fail startup");
    let message = err.to_string();
    assert!(message.contains("runtime.cpu.cores"), "{message}");
    assert!(message.contains("Invalid value '0'"), "{message}");
}
