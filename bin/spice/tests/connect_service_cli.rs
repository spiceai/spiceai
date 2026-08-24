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

//! End-to-end grammar and local lifecycle contract for `spice cloud service`.

use assert_cmd::{Command, cargo::cargo_bin_cmd};
use tempfile::TempDir;

#[cfg(target_os = "linux")]
const USAGE_EXIT_CODE: i32 = 2;

fn spice_cmd() -> Command {
    cargo_bin_cmd!("spice")
}

fn empty_instance() -> TempDir {
    TempDir::new().expect("create tempdir")
}

fn cloud(instance: &TempDir, args: &[&str]) -> assert_cmd::assert::Assert {
    let mut cmd = spice_cmd();
    cmd.current_dir(instance.path())
        .arg("cloud")
        .args(args)
        .env_remove("SPICE_CLOUD_ENDPOINT")
        .env_remove("SPICE_CONFIG_DIR");
    cmd.assert()
}

#[test]
fn the_service_group_lists_only_lifecycle_actions() {
    let instance = empty_instance();
    let stdout = cloud(&instance, &["service"])
        .success()
        .get_output()
        .stdout
        .clone();
    let stdout = String::from_utf8(stdout).expect("stdout is UTF-8");

    for action in ["install", "uninstall", "start", "stop", "restart"] {
        assert!(
            stdout.contains(&format!("spice cloud service {action}")),
            "the no-action help must name `{action}`: {stdout}"
        );
    }
    for removed in ["service status", "service logs", "--dir"] {
        assert!(
            !stdout.contains(removed),
            "removed grammar leaked: {stdout}"
        );
    }
}

#[cfg(target_os = "linux")]
#[test]
fn a_start_with_no_installed_service_exits_two_and_points_at_install() {
    let instance = empty_instance();
    let output = cloud(&instance, &["service", "start"])
        .code(USAGE_EXIT_CODE)
        .get_output()
        .clone();
    let stderr = String::from_utf8(output.stderr).expect("stderr is UTF-8");
    assert!(stderr.contains("spice cloud service install"), "{stderr}");
    assert!(
        String::from_utf8(output.stdout)
            .expect("stdout is UTF-8")
            .trim()
            .is_empty(),
        "a refusal must leave stdout clean"
    );
}

#[cfg(target_os = "linux")]
#[test]
fn a_restart_with_no_installed_service_never_signals_a_foreground_runtime() {
    let instance = empty_instance();
    let stderr = cloud(&instance, &["service", "restart"])
        .code(USAGE_EXIT_CODE)
        .get_output()
        .stderr
        .clone();
    let stderr = String::from_utf8(stderr).expect("stderr is UTF-8");
    assert!(stderr.contains("no supervisor-managed service"), "{stderr}");
    assert!(stderr.contains("foreground"), "{stderr}");
}

#[cfg(target_os = "linux")]
#[test]
fn stop_and_uninstall_are_idempotent_without_a_service() {
    let instance = empty_instance();
    for action in ["stop", "uninstall"] {
        let stdout = cloud(&instance, &["service", action])
            .success()
            .get_output()
            .stdout
            .clone();
        let stdout = String::from_utf8(stdout).expect("stdout is UTF-8");
        assert!(
            stdout.to_lowercase().contains("identity"),
            "{action}: {stdout}"
        );
    }
}

#[test]
fn removed_service_grammar_is_rejected() {
    let instance = empty_instance();
    for args in [
        vec!["service", "status"],
        vec!["service", "logs"],
        vec!["service", "start", "--dir", "/tmp/other"],
        vec!["service", "restart", "some.service"],
    ] {
        cloud(&instance, &args).failure();
    }
}

#[test]
fn generated_help_exposes_service_under_cloud_not_connect() {
    let cloud_help = spice_cmd()
        .args(["cloud", "--help"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let cloud_help = String::from_utf8(cloud_help).expect("stdout is UTF-8");
    assert!(cloud_help.contains("service"), "{cloud_help}");

    let connect_help = spice_cmd()
        .args(["connect", "--help"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let connect_help = String::from_utf8(connect_help).expect("stdout is UTF-8");
    assert!(!connect_help.contains("service install"), "{connect_help}");
}
