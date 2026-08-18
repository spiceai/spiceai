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
#![expect(clippy::expect_used, reason = "integration-test helpers")]

//! End-to-end contract for `spice connect status` and
//! `spice connect service`.
//!
//! The unit tests pin the grammar and the model. These run the real binary,
//! which is the only place three things can be checked: that `--output json`
//! puts JSON and nothing else on stdout, that the human report and the
//! diagnosis land on the streams automation expects, and that the documented
//! exit codes are the ones the process actually returns.

use assert_cmd::{Command, cargo::cargo_bin_cmd};
use tempfile::TempDir;

/// Exit code for a request that cannot be carried out as asked.
#[cfg(target_os = "linux")]
const USAGE_EXIT_CODE: i32 = 2;

fn spice_cmd() -> Command {
    cargo_bin_cmd!("spice")
}

/// An instance directory with no Cloud Connect state and no service.
fn empty_instance() -> TempDir {
    TempDir::new().expect("create tempdir")
}

/// Run `spice connect <args> --dir <instance>` in an isolated directory.
fn connect(instance: &TempDir, args: &[&str]) -> assert_cmd::assert::Assert {
    let mut cmd = spice_cmd();
    cmd.arg("connect")
        .args(args)
        .arg("--dir")
        .arg(instance.path())
        // A stray endpoint override in the environment would send the status
        // probe somewhere real.
        .env_remove("SPICE_CLOUD_ENDPOINT")
        .env_remove("SPICE_CONFIG_DIR");
    cmd.assert()
}

#[test]
fn json_status_writes_only_json_to_stdout() {
    let instance = empty_instance();
    let output = connect(&instance, &["status", "--output", "json"])
        .success()
        .get_output()
        .clone();

    let stdout = String::from_utf8(output.stdout).expect("stdout is UTF-8");
    let report: serde_json::Value =
        serde_json::from_str(&stdout).expect("stdout must be exactly one JSON document");
    assert_eq!(report["schema_version"], 3);
    for section in ["connection", "service", "deployment"] {
        assert!(report[section].is_object(), "missing {section}: {stdout}");
    }
    assert_eq!(report["service"]["state"], "not_installed");
    assert_eq!(report["service"]["installed"], false);
}

#[cfg(target_os = "linux")]
#[test]
fn the_service_status_json_is_the_same_object_the_full_report_nests() {
    let instance = empty_instance();

    let full = connect(&instance, &["status", "--output", "json"])
        .success()
        .get_output()
        .stdout
        .clone();
    let filtered = connect(&instance, &["service", "status", "--output", "json"])
        .success()
        .get_output()
        .stdout
        .clone();

    let full: serde_json::Value = serde_json::from_slice(&full).expect("full report is JSON");
    let filtered: serde_json::Value =
        serde_json::from_slice(&filtered).expect("filtered report is JSON");

    assert_eq!(full["schema_version"], filtered["schema_version"]);
    assert_eq!(
        serde_json::to_string(&full["service"]).expect("serialize"),
        serde_json::to_string(&filtered["service"]).expect("serialize"),
        "automation must never have to reconcile two service schemas"
    );
    // The filtered document carries the service object and nothing else.
    let object = filtered.as_object().expect("an object");
    let mut keys: Vec<&str> = object.keys().map(String::as_str).collect();
    keys.sort_unstable();
    assert_eq!(keys, vec!["schema_version", "service"]);
}

#[cfg(target_os = "linux")]
#[test]
fn the_service_group_with_no_action_prints_help_and_does_nothing() {
    let instance = empty_instance();
    let stdout = connect(&instance, &["service"])
        .success()
        .get_output()
        .stdout
        .clone();
    let stdout = String::from_utf8(stdout).expect("stdout is UTF-8");

    for action in [
        "install",
        "uninstall",
        "start",
        "stop",
        "restart",
        "status",
        "logs",
    ] {
        assert!(
            stdout.contains(&format!("spice connect service {action}")),
            "the no-action help must name `{action}`: {stdout}"
        );
    }
    // The hidden alias is never printed.
    assert!(!stdout.contains("svc"), "{stdout}");
}

#[cfg(target_os = "linux")]
#[test]
fn a_start_with_no_installed_service_exits_two_and_points_at_install() {
    let instance = empty_instance();
    let output = connect(&instance, &["service", "start"])
        .code(USAGE_EXIT_CODE)
        .get_output()
        .clone();

    // The diagnosis is on stderr; stdout stays clean for anything piping it.
    let stderr = String::from_utf8(output.stderr).expect("stderr is UTF-8");
    assert!(
        stderr.contains("spice connect service install"),
        "the refusal must name the command that fixes it: {stderr}"
    );
    assert!(
        String::from_utf8(output.stdout)
            .expect("stdout is UTF-8")
            .trim()
            .is_empty(),
        "a refusal must not write a report to stdout"
    );
}

#[cfg(target_os = "linux")]
#[test]
fn a_restart_with_no_installed_service_never_signals_a_foreground_runtime() {
    let instance = empty_instance();
    let stderr = connect(&instance, &["service", "restart"])
        .code(USAGE_EXIT_CODE)
        .get_output()
        .stderr
        .clone();
    let stderr = String::from_utf8(stderr).expect("stderr is UTF-8");
    assert!(stderr.contains("no supervisor-managed service"), "{stderr}");
    assert!(
        stderr.contains("foreground"),
        "the refusal must say the foreground runtime is left alone: {stderr}"
    );
}

#[cfg(target_os = "linux")]
#[test]
fn stop_logs_and_uninstall_succeed_on_a_directory_with_no_service() {
    let instance = empty_instance();
    for action in [
        vec!["service", "stop"],
        vec!["service", "logs"],
        vec!["service", "uninstall"],
    ] {
        let stdout = connect(&instance, &action)
            .success()
            .get_output()
            .stdout
            .clone();
        let stdout = String::from_utf8(stdout).expect("stdout is UTF-8");
        assert!(
            stdout.to_lowercase().contains("identity"),
            "`{action:?}` must say the Cloud identity is untouched: {stdout}"
        );
    }
}

#[cfg(target_os = "linux")]
#[test]
fn the_hidden_alias_reaches_the_same_command() {
    let instance = empty_instance();
    let via_alias = connect(&instance, &["svc", "status", "--output", "json"])
        .success()
        .get_output()
        .stdout
        .clone();
    let via_name = connect(&instance, &["service", "status", "--output", "json"])
        .success()
        .get_output()
        .stdout
        .clone();
    assert_eq!(via_alias, via_name);
}

#[cfg(target_os = "linux")]
#[test]
fn the_documented_grammar_is_the_only_grammar() {
    let instance = empty_instance();
    // `--install` and `--tail` were the previous spellings; clap must reject
    // both so there is exactly one documented way to say each thing.
    for args in [
        vec!["--install"],
        vec!["service", "logs", "--tail"],
        vec!["service", "logs", "--tail", "50"],
        vec!["service", "logs", "-n", "100001"],
        // Resolution is by instance directory: a supervisor name must not be
        // accepted, because it can name another instance's service.
        vec!["service", "restart", "some.service"],
    ] {
        connect(&instance, &args).failure();
    }
}

#[cfg(target_os = "linux")]
#[test]
fn generated_help_documents_service_and_never_svc() {
    let mut cmd = spice_cmd();
    let stdout = cmd
        .args(["connect", "--help"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let stdout = String::from_utf8(stdout).expect("stdout is UTF-8");
    assert!(stdout.contains("service"), "{stdout}");
    assert!(
        !stdout.contains("svc"),
        "the typing alias must stay out of generated help: {stdout}"
    );
}
