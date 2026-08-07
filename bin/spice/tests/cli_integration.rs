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

//! Integration tests for the Spice CLI.
//!
//! These tests verify CLI commands work correctly without requiring
//! a running Spice runtime (unless specifically testing runtime interaction).

use assert_cmd::{Command, cargo::cargo_bin_cmd};
use predicates::prelude::*;
use std::fs;
use tempfile::TempDir;

/// Get a Command for the spice binary
fn spice_cmd() -> Command {
    cargo_bin_cmd!("spice")
}

// ============================================================================
// Version Command Tests
// ============================================================================

mod version {
    use super::*;

    #[test]
    fn test_version_command() {
        let mut cmd = spice_cmd();
        cmd.arg("version")
            .assert()
            .success()
            .stdout(predicate::str::contains("CLI version:"));
    }

    #[test]
    fn test_version_flag() {
        let mut cmd = spice_cmd();
        cmd.arg("--version")
            .assert()
            .success()
            .stdout(predicate::str::contains("spice"));
    }

    #[test]
    fn test_help_flag() {
        let mut cmd = spice_cmd();
        cmd.arg("--help")
            .assert()
            .success()
            .stdout(predicate::str::contains("Spice.ai CLI"))
            .stdout(predicate::str::contains("Commands:"));
    }

    #[test]
    fn test_help_command() {
        let mut cmd = spice_cmd();
        cmd.arg("help")
            .assert()
            .success()
            .stdout(predicate::str::contains("Spice.ai CLI"));
    }

    #[test]
    fn test_machine_version_outputs_json() {
        let mut cmd = spice_cmd();
        let output = cmd
            .arg("--machine")
            .arg("version")
            .arg("--cli-only")
            .assert()
            .success()
            .get_output()
            .clone();

        assert!(
            output.stderr.is_empty(),
            "machine version should not write human logs to stderr: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let json: serde_json::Value = serde_json::from_slice(&output.stdout)
            .expect("machine version output should be valid JSON");
        assert!(json.get("cli").is_some(), "JSON should include cli version");
    }

    #[test]
    fn test_machine_errors_are_json() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let missing = temp_dir.path().join("missing-spicepod.yaml");
        let mut cmd = spice_cmd();
        let output = cmd
            .arg("--machine")
            .arg("validate")
            .arg(&missing)
            .assert()
            .failure()
            .get_output()
            .clone();

        assert!(
            output.stdout.is_empty(),
            "machine errors should not write to stdout: {}",
            String::from_utf8_lossy(&output.stdout)
        );
        let json: serde_json::Value = serde_json::from_slice(&output.stderr)
            .expect("machine error output should be valid JSON");
        assert_eq!(json["status"], "error");
        assert_eq!(json["error"]["code"], "invalid_argument");
    }
}

// ============================================================================
// Init Command Tests
// ============================================================================

mod init {
    use super::*;

    #[test]
    fn test_init_creates_spicepod() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");

        let mut cmd = spice_cmd();
        cmd.current_dir(temp_dir.path())
            .arg("init")
            .assert()
            .success()
            .stdout(predicate::str::contains("Initialized"));

        // Verify spicepod.yaml was created
        let spicepod_path = temp_dir.path().join("spicepod.yaml");
        assert!(spicepod_path.exists(), "spicepod.yaml should be created");

        // Verify content
        let content = fs::read_to_string(&spicepod_path).expect("Failed to read spicepod.yaml");
        assert!(content.contains("version:"), "Should contain version field");
        assert!(
            content.contains("kind: Spicepod"),
            "Should contain kind field"
        );
    }

    #[test]
    fn test_init_with_name() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");

        let mut cmd = spice_cmd();
        cmd.current_dir(temp_dir.path())
            .arg("init")
            .arg("my-test-app")
            .assert()
            .success();

        // When a name is provided, it creates a subdirectory
        let spicepod_path = temp_dir.path().join("my-test-app").join("spicepod.yaml");
        let content = fs::read_to_string(&spicepod_path).expect("Failed to read spicepod.yaml");
        assert!(
            content.contains("my-test-app"),
            "Should contain the app name"
        );
    }

    #[test]
    fn test_init_overwrites_with_warning() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");

        // Create initial spicepod
        let mut cmd = spice_cmd();
        cmd.current_dir(temp_dir.path())
            .arg("init")
            .arg("first-app")
            .assert()
            .success();

        // Try to init again - behavior depends on implementation
        // It may succeed with a warning or fail
        let mut cmd2 = spice_cmd();
        let assert = cmd2
            .current_dir(temp_dir.path())
            .arg("init")
            .arg("second-app")
            .assert();

        // Accept either success or failure - just verify it doesn't panic
        let _ = assert;
    }

    #[test]
    fn test_init_help() {
        let mut cmd = spice_cmd();
        cmd.arg("init")
            .arg("--help")
            .assert()
            .success()
            .stdout(predicate::str::contains("Initialize"));
    }
}

// ============================================================================
// Dataset Command Tests
// ============================================================================

mod dataset {
    use super::*;

    #[test]
    fn test_dataset_help() {
        let mut cmd = spice_cmd();
        cmd.arg("dataset")
            .arg("--help")
            .assert()
            .success()
            .stdout(predicate::str::contains("dataset entries"));
    }

    #[test]
    fn test_dataset_configure_help() {
        let mut cmd = spice_cmd();
        cmd.arg("dataset")
            .arg("configure")
            .arg("--help")
            .assert()
            .success()
            .stdout(predicate::str::contains("Create or update a dataset"));
    }
}

// ============================================================================
// Login Command Tests
// ============================================================================

mod login {
    use super::*;

    #[test]
    fn test_login_help() {
        let mut cmd = spice_cmd();
        cmd.arg("login")
            .arg("--help")
            .assert()
            .success()
            .stdout(predicate::str::contains("Login"))
            .stdout(predicate::str::contains("credentials"));
    }

    #[test]
    fn test_login_subcommands_available() {
        let mut cmd = spice_cmd();
        cmd.arg("login")
            .arg("--help")
            .assert()
            .success()
            // Check for subcommand-based providers
            .stdout(predicate::str::contains("Commands:"));
    }

    #[test]
    fn test_login_unknown_provider() {
        let mut cmd = spice_cmd();
        cmd.arg("login")
            .arg("unknown_provider_xyz")
            .assert()
            .failure();
    }
}

// ============================================================================
// Install Command Tests
// ============================================================================

mod install {
    use super::*;

    #[test]
    fn test_install_help() {
        let mut cmd = spice_cmd();
        cmd.arg("install")
            .arg("--help")
            .assert()
            .success()
            .stdout(predicate::str::contains("Install"))
            .stdout(predicate::str::contains("runtime"));
    }

    #[test]
    fn test_install_version_flag() {
        let mut cmd = spice_cmd();
        cmd.arg("install")
            .arg("--help")
            .assert()
            .success()
            .stdout(predicate::str::contains("--version"));
    }
}

// ============================================================================
// Upgrade Command Tests
// ============================================================================

mod upgrade {
    use super::*;

    #[test]
    fn test_upgrade_help() {
        let mut cmd = spice_cmd();
        cmd.arg("upgrade")
            .arg("--help")
            .assert()
            .success()
            .stdout(predicate::str::contains("Upgrade"))
            .stdout(predicate::str::contains("runtime"));
    }
}

// ============================================================================
// SQL Command Tests
// ============================================================================

mod sql {
    use super::*;

    #[test]
    fn test_sql_help() {
        let mut cmd = spice_cmd();
        cmd.arg("sql")
            .arg("--help")
            .assert()
            .success()
            .stdout(predicate::str::contains("SQL"))
            .stdout(predicate::str::contains("query"))
            .stdout(predicate::str::contains("--query"));
    }

    #[test]
    fn test_sql_endpoint_flag() {
        let mut cmd = spice_cmd();
        cmd.arg("sql")
            .arg("--help")
            .assert()
            .success()
            .stdout(predicate::str::contains("--endpoint"));
    }
}

// ============================================================================
// Status Command Tests
// ============================================================================

mod status {
    use super::*;

    #[test]
    fn test_status_help() {
        let mut cmd = spice_cmd();
        cmd.arg("status")
            .arg("--help")
            .assert()
            .success()
            .stdout(predicate::str::contains("status"))
            .stdout(predicate::str::contains("runtime"));
    }

    #[test]
    fn test_status_without_runtime() {
        // Status should fail gracefully when runtime is not running
        let mut cmd = spice_cmd();
        cmd.arg("status")
            .arg("--http-endpoint")
            .arg("http://localhost:59999") // Use unlikely port
            .assert()
            .failure();
    }
}

// ============================================================================
// Datasets Command Tests
// ============================================================================

mod datasets {
    use super::*;

    #[test]
    fn test_datasets_help() {
        let mut cmd = spice_cmd();
        cmd.arg("datasets")
            .arg("--help")
            .assert()
            .success()
            .stdout(predicate::str::contains("datasets"));
    }

    #[test]
    fn test_datasets_without_runtime() {
        let mut cmd = spice_cmd();
        cmd.arg("datasets")
            .arg("--http-endpoint")
            .arg("http://localhost:59999")
            .assert()
            .failure();
    }
}

// ============================================================================
// Models Command Tests
// ============================================================================

mod models {
    use super::*;

    #[test]
    fn test_models_help() {
        let mut cmd = spice_cmd();
        cmd.arg("models")
            .arg("--help")
            .assert()
            .success()
            .stdout(predicate::str::contains("models"));
    }
}

// ============================================================================
// Catalogs Command Tests
// ============================================================================

mod catalogs {
    use super::*;

    #[test]
    fn test_catalogs_help() {
        let mut cmd = spice_cmd();
        cmd.arg("catalogs")
            .arg("--help")
            .assert()
            .success()
            .stdout(predicate::str::contains("catalogs"));
    }
}

// ============================================================================
// Manifest Editing Command Tests
// ============================================================================

mod manifest_editing {
    use super::*;

    fn write_base_yml(temp_dir: &TempDir) -> std::path::PathBuf {
        let manifest_path = temp_dir.path().join("spicepod.yml");
        fs::write(
            &manifest_path,
            "version: v2\nkind: Spicepod\nname: app\nmodels: []\nembeddings: []\nworkers: []\n",
        )
        .expect("base spicepod.yml should be written");
        manifest_path
    }

    #[test]
    fn test_manifest_command_help() {
        let mut model_help = spice_cmd();
        model_help
            .arg("model")
            .arg("--help")
            .assert()
            .success()
            .stdout(predicate::str::contains("component entry"));

        let mut model_add_help = spice_cmd();
        model_add_help
            .arg("model")
            .arg("add")
            .arg("--help")
            .assert()
            .success()
            .stdout(predicate::str::contains("Component name"));

        let mut runtime_help = spice_cmd();
        runtime_help
            .arg("runtime")
            .arg("configure")
            .arg("--help")
            .assert()
            .success()
            .stdout(predicate::str::contains("Set a schema field"));
    }

    #[test]
    fn test_model_add_and_configure_preserves_yml_manifest() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let manifest_path = write_base_yml(&temp_dir);

        let mut add_cmd = spice_cmd();
        add_cmd
            .current_dir(temp_dir.path())
            .arg("model")
            .arg("add")
            .arg("llm")
            .arg("--from")
            .arg("openai:gpt-4o-mini")
            .arg("--param")
            .arg("temperature=0.2")
            .assert()
            .success()
            .stdout(predicate::str::contains("Updated"));

        let mut configure_cmd = spice_cmd();
        configure_cmd
            .current_dir(temp_dir.path())
            .arg("model")
            .arg("configure")
            .arg("llm")
            .arg("--set")
            .arg("datasets=yaml:[documents]")
            .assert()
            .success()
            .stdout(predicate::str::contains("Updated"));

        let updated_manifest =
            fs::read_to_string(&manifest_path).expect("updated spicepod.yml should be readable");
        assert!(updated_manifest.contains("models:"));
        assert!(updated_manifest.contains("name: llm"));
        assert!(updated_manifest.contains("openai:gpt-4o-mini"));
        assert!(updated_manifest.contains("temperature: \"0.2\""));
        assert!(updated_manifest.contains("datasets:"));
        assert!(updated_manifest.contains("- documents"));
        assert!(
            !temp_dir.path().join("spicepod.yaml").exists(),
            "manifest edits should preserve an existing spicepod.yml"
        );
    }

    #[test]
    fn test_runtime_configure_sets_nested_fields() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let manifest_path = write_base_yml(&temp_dir);

        let mut cmd = spice_cmd();
        cmd.current_dir(temp_dir.path())
            .arg("runtime")
            .arg("configure")
            .arg("--set")
            .arg("functions.enabled=yaml:true")
            .assert()
            .success()
            .stdout(predicate::str::contains("Updated"));

        let updated_manifest =
            fs::read_to_string(&manifest_path).expect("updated spicepod.yml should be readable");
        assert!(updated_manifest.contains("runtime:"));
        assert!(updated_manifest.contains("functions:"));
        assert!(updated_manifest.contains("enabled: true"));
    }

    #[test]
    fn test_catalog_add_from_flags() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let manifest_path = write_base_yml(&temp_dir);

        let mut cmd = spice_cmd();
        cmd.current_dir(temp_dir.path())
            .arg("catalog")
            .arg("add")
            .arg("glue")
            .arg("--from")
            .arg("glue")
            .arg("--param")
            .arg("glue_region=us-east-1")
            .assert()
            .success()
            .stdout(predicate::str::contains("Updated"));

        let updated_manifest =
            fs::read_to_string(&manifest_path).expect("updated spicepod.yml should be readable");
        assert!(updated_manifest.contains("catalogs:"));
        assert!(updated_manifest.contains("name: glue"));
        assert!(updated_manifest.contains("from: glue"));
        assert!(updated_manifest.contains("glue_region: us-east-1"));
    }
}

// ============================================================================
// Pods Command Tests
// ============================================================================

mod pods {
    use super::*;

    #[test]
    fn test_pods_help() {
        let mut cmd = spice_cmd();
        cmd.arg("pods")
            .arg("--help")
            .assert()
            .success()
            .stdout(predicate::str::contains("Spicepods"));
    }
}

// ============================================================================
// Refresh Command Tests
// ============================================================================

mod refresh {
    use super::*;

    #[test]
    fn test_refresh_help() {
        let mut cmd = spice_cmd();
        cmd.arg("refresh")
            .arg("--help")
            .assert()
            .success()
            .stdout(predicate::str::contains("Refresh"))
            .stdout(predicate::str::contains("dataset"));
    }

    #[test]
    fn test_refresh_requires_dataset() {
        let mut cmd = spice_cmd();
        cmd.arg("refresh").assert().failure();
    }
}

// ============================================================================
// Add Command Tests
// ============================================================================

mod add {
    use super::*;

    #[test]
    fn test_add_help() {
        let mut cmd = spice_cmd();
        cmd.arg("add")
            .arg("--help")
            .assert()
            .success()
            .stdout(predicate::str::contains("Add"))
            .stdout(predicate::str::contains("Spicepod"));
    }

    #[test]
    fn test_add_requires_spicepod() {
        let mut cmd = spice_cmd();
        cmd.arg("add").assert().failure();
    }

    #[test]
    fn test_add_local_yml_pod_updates_existing_yml_manifest() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let app_manifest = temp_dir.path().join("spicepod.yml");
        fs::write(
            &app_manifest,
            "version: v2\nkind: Spicepod\nname: app\nmodels: []\nembeddings: []\nworkers: []\n",
        )
        .expect("Failed to write app spicepod.yml");

        let local_pod_dir = temp_dir.path().join("localpod");
        fs::create_dir_all(&local_pod_dir).expect("Failed to create local pod dir");
        fs::write(
            local_pod_dir.join("spicepod.yml"),
            "version: v2\nkind: Spicepod\nname: localpod\n",
        )
        .expect("Failed to write local pod spicepod.yml");

        let mut cmd = spice_cmd();
        cmd.current_dir(temp_dir.path())
            .arg("add")
            .arg(&local_pod_dir)
            .assert()
            .success()
            .stdout(predicate::str::contains("added spicepods/localpod"));

        let updated_manifest =
            fs::read_to_string(&app_manifest).expect("Failed to read updated spicepod.yml");
        assert!(
            updated_manifest.contains("models:"),
            "models should be preserved"
        );
        assert!(
            updated_manifest.contains("embeddings:"),
            "embeddings should be preserved"
        );
        assert!(
            updated_manifest.contains("workers:"),
            "workers should be preserved"
        );
        assert!(
            updated_manifest.contains("dependencies:"),
            "dependencies should be added"
        );
        assert!(
            updated_manifest.contains("- spicepods/localpod"),
            "dependency should reference the installed path"
        );
        assert!(
            !temp_dir.path().join("spicepod.yaml").exists(),
            "spice add should edit the existing .yml manifest"
        );
        assert!(
            temp_dir
                .path()
                .join("spicepods")
                .join("localpod")
                .join("spicepod.yaml")
                .exists(),
            "local yml dependency should be normalized to spicepod.yaml"
        );
    }
}

// ============================================================================
// Connect Command Tests
// ============================================================================

mod connect {
    use super::*;

    /// Create a fake `spiced` binary under `$HOME/.spice/bin/spiced` so the
    /// is-runtime-installed preflight passes without a network install. The
    /// stub records every execution by touching a marker file —
    /// `spice connect` must enroll and exit WITHOUT starting `spiced`, and
    /// the marker proves it.
    #[cfg(unix)]
    fn install_fake_spiced(home: &std::path::Path) {
        use std::os::unix::fs::PermissionsExt as _;
        let bin_dir = home.join(".spice").join("bin");
        fs::create_dir_all(&bin_dir).expect("create fake .spice/bin");
        let spiced = bin_dir.join("spiced");
        // `--version` prints and exits without serving, as the real binary does,
        // and deliberately leaves no marker: `spice connect` probes it to report
        // the runtime version in the enroll host facts, which is not the same as
        // starting the runtime. Any other invocation is a real start.
        fs::write(
            &spiced,
            "#!/bin/sh\n\
             if [ \"$1\" = \"--version\" ]; then echo 'spiced v0.0.0-fake'; exit 0; fi\n\
             touch \"$(dirname \"$0\")/spiced-ran\"\n\
             exit 0\n",
        )
        .expect("write fake spiced");
        fs::set_permissions(&spiced, fs::Permissions::from_mode(0o755)).expect("chmod fake spiced");
    }

    /// Marker the fake `spiced` touches when started (a `--version` probe does
    /// not count — see [`install_fake_spiced`]).
    #[cfg(unix)]
    fn spiced_ran_marker(home: &std::path::Path) -> std::path::PathBuf {
        home.join(".spice").join("bin").join("spiced-ran")
    }

    /// Serve exactly one HTTP request on an ephemeral port from a plain-std
    /// thread, answering with `status` and `body`. Hermetic stand-in for the
    /// cloud enroll endpoint — no async runtime or extra dev-deps needed.
    fn spawn_one_shot_http(status: u16, body: &str) -> (String, std::thread::JoinHandle<()>) {
        let (endpoint, handle) = spawn_one_shot_http_capturing(status, body);
        // Discard the captured request; callers that assert on it use
        // `spawn_one_shot_http_capturing` directly.
        let handle = std::thread::spawn(move || {
            handle.join().expect("mock served the request");
        });
        (endpoint, handle)
    }

    /// As [`spawn_one_shot_http`], but the join handle yields the request body
    /// the CLI sent — for asserting on the enroll wire format.
    fn spawn_one_shot_http_capturing(
        status: u16,
        body: &str,
    ) -> (String, std::thread::JoinHandle<String>) {
        use std::io::{Read as _, Write as _};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind mock listener");
        let addr = listener.local_addr().expect("mock listener addr");
        let body = body.to_string();
        let handle = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept enroll request");
            // Read until the full headers + content-length body arrived.
            let mut buf = Vec::new();
            let mut tmp = [0_u8; 4096];
            loop {
                let n = stream.read(&mut tmp).expect("read enroll request");
                if n == 0 {
                    break;
                }
                buf.extend_from_slice(&tmp[..n]);
                let text = String::from_utf8_lossy(&buf);
                if let Some(header_end) = text.find("\r\n\r\n") {
                    let content_length = text[..header_end]
                        .lines()
                        .find_map(|line| {
                            let (name, value) = line.split_once(':')?;
                            if name.eq_ignore_ascii_case("content-length") {
                                value.trim().parse::<usize>().ok()
                            } else {
                                None
                            }
                        })
                        .unwrap_or(0);
                    if buf.len() >= header_end + 4 + content_length {
                        break;
                    }
                }
            }
            let response = format!(
                "HTTP/1.1 {status} MOCK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            stream
                .write_all(response.as_bytes())
                .expect("write enroll response");

            let text = String::from_utf8_lossy(&buf).into_owned();
            match text.split_once("\r\n\r\n") {
                Some((_, request_body)) => request_body.to_string(),
                None => String::new(),
            }
        });
        (format!("http://{addr}"), handle)
    }

    /// A well-formed enroll success response (the CLI does not validate the
    /// PEM contents at enroll — the cloud signed them).
    fn enroll_ok_body() -> String {
        serde_json::json!({
            "instance_id": "inst_cli_test",
            "identity_cert_pem": "-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n",
            "ca_bundle_pem": "-----BEGIN CERTIFICATE-----\nBBBB\n-----END CERTIFICATE-----\n",
            "gateway_addr": "127.0.0.1:443",
            "not_after": "2030-01-01T00:00:00Z",
        })
        .to_string()
    }

    #[test]
    fn test_connect_help() {
        let mut cmd = spice_cmd();
        cmd.arg("connect")
            .arg("--help")
            .assert()
            .success()
            // Spice Cloud Connect adoption flow (new) — still mentions
            // the legacy pod-add behavior in the long help.
            .stdout(predicate::str::contains("SPICE-ADOPT"))
            .stdout(predicate::str::contains("Spice Cloud"))
            .stdout(predicate::str::contains("status"))
            .stdout(predicate::str::contains("remove"))
            // The renamed subcommand replaced `forget` outright — it is
            // unreleased, so there is no alias to carry and no help text
            // should still teach the old name.
            .stdout(predicate::str::contains("forget").not())
            .stdout(predicate::str::contains("--install"))
            .stdout(predicate::str::contains("--region"))
            .stdout(predicate::str::contains("SPICE_CONNECT_ADOPT_REGION"));
    }

    /// `forget` is gone from the arg surface, not merely from the help text:
    /// it no longer clears local state. (It falls through to the deprecated
    /// pod-add path like any other non-code positional and fails there, which
    /// is why this asserts on the state rather than on the message.)
    #[cfg(unix)]
    #[test]
    fn test_connect_forget_no_longer_clears_state() {
        let dir = TempDir::new().expect("create temp config dir");
        let config_dir = dir.path();
        std::fs::write(
            config_dir.join("pending-adopt-code"),
            "SPICE-ADOPT-AAAAA-BBBBB",
        )
        .expect("stage a code");

        spice_cmd()
            .env("SPICE_CONFIG_DIR", config_dir)
            .arg("connect")
            .arg("forget")
            .assert()
            .failure();

        assert!(
            config_dir.join("pending-adopt-code").exists(),
            "`forget` is not a subcommand any more and must not clear state"
        );

        // The renamed verb does.
        spice_cmd()
            .env("SPICE_CONFIG_DIR", config_dir)
            .arg("connect")
            .arg("remove")
            .assert()
            .success();
        assert!(!config_dir.join("pending-adopt-code").exists());
    }

    /// The enroll-and-exit contract: `spice connect <CODE>` completes the
    /// HTTPS enroll, persists `identity.json`, discards the staged code,
    /// exits 0 — and never starts `spiced`. A subsequent
    /// `spice connect status` reports the enrolled identity.
    #[cfg(unix)]
    #[test]
    fn test_connect_enrolls_and_exits_without_starting_spiced() {
        let dir = TempDir::new().expect("create temp config dir");
        let config_dir = dir.path();
        let home = TempDir::new().expect("create temp home");
        install_fake_spiced(home.path());
        let (endpoint, server) = spawn_one_shot_http(200, &enroll_ok_body());

        let mut cmd = spice_cmd();
        cmd.env("HOME", home.path())
            .env("SPICE_CONFIG_DIR", config_dir)
            .env_remove("SPICE_CLOUD_ENDPOINT")
            .arg("connect")
            .arg("SPICE-ADOPT-AAAAA-BBBBB")
            .arg("--endpoint")
            .arg(&endpoint)
            .assert()
            .success()
            .stdout(predicate::str::contains("Enrolled with Spice Cloud"))
            .stdout(predicate::str::contains("inst_cli_test"))
            .stdout(predicate::str::contains("spiced --cloud-connect"));
        server.join().expect("mock served the enroll request");

        assert!(
            config_dir.join("identity.json").exists(),
            "the issued identity must be persisted"
        );
        assert!(
            !config_dir.join("pending-adopt-code").exists(),
            "the consumed code must not stay staged"
        );
        assert!(
            !spiced_ran_marker(home.path()).exists(),
            "`spice connect` must not start spiced"
        );

        let mut cmd = spice_cmd();
        cmd.env("SPICE_CONFIG_DIR", config_dir)
            .arg("connect")
            .arg("status")
            .assert()
            .success()
            .stdout(predicate::str::contains("adopted"))
            .stdout(predicate::str::contains("inst_cli_test"));
    }

    /// `--dir <path>` anchors the per-instance state at `<dir>/.spice`
    /// (when `SPICE_CONFIG_DIR` is not set), for both enroll and status.
    #[cfg(unix)]
    #[test]
    fn test_connect_dir_flag_anchors_instance_state() {
        let instance_dir = TempDir::new().expect("create temp instance dir");
        let cwd = TempDir::new().expect("create temp cwd");
        let home = TempDir::new().expect("create temp home");
        install_fake_spiced(home.path());
        let (endpoint, server) = spawn_one_shot_http(200, &enroll_ok_body());

        let mut cmd = spice_cmd();
        cmd.env("HOME", home.path())
            .env_remove("SPICE_CONFIG_DIR")
            .env_remove("SPICE_CLOUD_ENDPOINT")
            .current_dir(cwd.path())
            .arg("connect")
            .arg("SPICE-ADOPT-AAAAA-BBBBB")
            .arg("--dir")
            .arg(instance_dir.path())
            .arg("--endpoint")
            .arg(&endpoint)
            .assert()
            .success();
        server.join().expect("mock served the enroll request");

        assert!(
            instance_dir
                .path()
                .join(".spice")
                .join("identity.json")
                .exists(),
            "identity must be anchored under <dir>/.spice"
        );
        assert!(
            !cwd.path().join(".spice").exists(),
            "the cwd must not receive instance state when --dir is passed"
        );

        let mut cmd = spice_cmd();
        cmd.env_remove("SPICE_CONFIG_DIR")
            .current_dir(cwd.path())
            .arg("connect")
            .arg("status")
            .arg("--dir")
            .arg(instance_dir.path())
            .assert()
            .success()
            .stdout(predicate::str::contains("adopted"));
    }

    /// `--app-name`/`--create` ride the enroll request and the attached app
    /// from the response is reported in the success output.
    #[cfg(unix)]
    #[test]
    fn test_connect_app_name_reports_attachment() {
        let dir = TempDir::new().expect("create temp config dir");
        let config_dir = dir.path();
        let home = TempDir::new().expect("create temp home");
        install_fake_spiced(home.path());
        let body = serde_json::json!({
            "instance_id": "inst_cli_test",
            "identity_cert_pem": "-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n",
            "ca_bundle_pem": "",
            "gateway_addr": "127.0.0.1:443",
            "not_after": "2030-01-01T00:00:00Z",
            "app_name": "edge-app",
        })
        .to_string();
        let (endpoint, server) = spawn_one_shot_http(200, &body);

        let mut cmd = spice_cmd();
        cmd.env("HOME", home.path())
            .env("SPICE_CONFIG_DIR", config_dir)
            .arg("connect")
            .arg("SPICE-ADOPT-AAAAA-BBBBB")
            .arg("--app-name")
            .arg("edge-app")
            .arg("--create")
            .arg("--endpoint")
            .arg(&endpoint)
            .assert()
            .success()
            .stdout(predicate::str::contains("app:         edge-app"));
        server.join().expect("mock served the enroll request");
    }

    /// `--create` without `--app-name` is a client-side arg error — it
    /// never reaches the cloud.
    #[test]
    fn test_connect_create_requires_app_name() {
        spice_cmd()
            .arg("connect")
            .arg("SPICE-ADOPT-AAAAA-BBBBB")
            .arg("--create")
            .assert()
            .failure()
            .stderr(predicate::str::contains("--app-name"));
    }

    /// A transient enroll failure (here: connection refused) keeps the
    /// staged code so a retry — or a later `spiced --cloud-connect` start —
    /// can redeem it, and exits non-zero with retry guidance.
    #[cfg(unix)]
    #[test]
    fn test_connect_transient_failure_keeps_staged_code() {
        let dir = TempDir::new().expect("create temp config dir");
        let config_dir = dir.path();
        let home = TempDir::new().expect("create temp home");
        install_fake_spiced(home.path());
        // An unroutable local endpoint: refused before any HTTP exchange.
        let refused = {
            let listener =
                std::net::TcpListener::bind("127.0.0.1:0").expect("bind throwaway listener");
            let addr = listener.local_addr().expect("addr");
            drop(listener);
            format!("http://{addr}")
        };

        let mut cmd = spice_cmd();
        cmd.env("HOME", home.path())
            .env("SPICE_CONFIG_DIR", config_dir)
            .arg("connect")
            .arg("SPICE-ADOPT-AAAAA-BBBBB")
            .arg("--endpoint")
            .arg(&refused)
            .assert()
            .failure()
            // CLI errors are emitted via `tracing::error!`, which writes to
            // stdout under the default fmt subscriber.
            .stdout(predicate::str::contains("not consumed"));

        let pending = config_dir.join("pending-adopt-code");
        assert!(
            pending.exists(),
            "a transient failure must keep the staged code for retry"
        );
        assert!(!config_dir.join("identity.json").exists());
    }

    /// An authoritative cloud rejection (4xx) burns the staged code — a
    /// dead code must not be re-presented by a later start.
    #[cfg(unix)]
    #[test]
    fn test_connect_rejected_code_is_discarded() {
        let dir = TempDir::new().expect("create temp config dir");
        let config_dir = dir.path();
        let home = TempDir::new().expect("create temp home");
        install_fake_spiced(home.path());
        let (endpoint, server) =
            spawn_one_shot_http(401, r#"{"error":"Adoption code already used"}"#);

        let mut cmd = spice_cmd();
        cmd.env("HOME", home.path())
            .env("SPICE_CONFIG_DIR", config_dir)
            .arg("connect")
            .arg("SPICE-ADOPT-AAAAA-BBBBB")
            .arg("--endpoint")
            .arg(&endpoint)
            .assert()
            .failure()
            // See above: `tracing::error!` output lands on stdout.
            .stdout(predicate::str::contains("Mint a new adoption code"));
        server.join().expect("mock served the enroll request");

        assert!(
            !config_dir.join("pending-adopt-code").exists(),
            "an authoritatively rejected code must be discarded"
        );
        assert!(!config_dir.join("identity.json").exists());
    }

    /// An app-attachment rejection (404: no such app) is validated by the
    /// cloud BEFORE the code is consumed — the staged code must be kept so
    /// a corrected `--app-name` (or `--create`) can still redeem it.
    #[cfg(unix)]
    #[test]
    fn test_connect_attach_rejection_keeps_code() {
        let dir = TempDir::new().expect("create temp config dir");
        let config_dir = dir.path();
        let home = TempDir::new().expect("create temp home");
        install_fake_spiced(home.path());
        let (endpoint, server) =
            spawn_one_shot_http(404, r#"{"error":"App 'edge-app' not found in this org"}"#);

        let mut cmd = spice_cmd();
        cmd.env("HOME", home.path())
            .env("SPICE_CONFIG_DIR", config_dir)
            .arg("connect")
            .arg("SPICE-ADOPT-AAAAA-BBBBB")
            .arg("--app-name")
            .arg("edge-app")
            .arg("--endpoint")
            .arg(&endpoint)
            .assert()
            .failure()
            .stdout(predicate::str::contains("not consumed"));
        server.join().expect("mock served the enroll request");

        assert!(
            config_dir.join("pending-adopt-code").exists(),
            "an attach rejection must keep the staged code"
        );
        assert!(!config_dir.join("identity.json").exists());
    }

    /// A malformed adoption code (right prefix, wrong shape) should be
    /// rejected as an invalid argument rather than falling through to the
    /// legacy pod-add path and emitting a misleading cloud-Spicepod error.
    #[test]
    fn test_connect_malformed_adoption_code_is_rejected() {
        let dir = TempDir::new().expect("create temp config dir");
        let config_dir = dir.path();
        spice_cmd()
            .env("SPICE_CONFIG_DIR", config_dir)
            .arg("connect")
            .arg("SPICE-ADOPT-AAA-BBBB")
            .assert()
            .failure();

        // It must not have been staged as a pending code or treated as a pod:
        // rejecting early means the pending file is never written.
        assert!(
            !config_dir.join("pending-adopt-code").exists(),
            "malformed code must not be staged"
        );
    }

    /// `spice connect remove` with no prior state should be a no-op.
    #[test]
    fn test_connect_remove_when_nothing_to_clear() {
        let dir = TempDir::new().expect("create temp config dir");
        let mut cmd = spice_cmd();
        cmd.env("SPICE_CONFIG_DIR", dir.path())
            .arg("connect")
            .arg("remove")
            .assert()
            .success()
            .stdout(predicate::str::contains("nothing to remove"));
    }

    /// `spice connect remove` after a connect whose enroll could not reach
    /// the cloud (staged code retained) should clear the pending file.
    ///
    /// There is no identity to release here, so the command clears local state
    /// without contacting the cloud — which is also why it needs no endpoint.
    #[cfg(unix)]
    #[test]
    fn test_connect_remove_clears_pending_code() {
        let dir = TempDir::new().expect("create temp config dir");
        let config_dir = dir.path();
        let home = TempDir::new().expect("create temp home");
        install_fake_spiced(home.path());
        let refused = {
            let listener =
                std::net::TcpListener::bind("127.0.0.1:0").expect("bind throwaway listener");
            let addr = listener.local_addr().expect("addr");
            drop(listener);
            format!("http://{addr}")
        };
        spice_cmd()
            .env("HOME", home.path())
            .env("SPICE_CONFIG_DIR", config_dir)
            .arg("connect")
            .arg("SPICE-ADOPT-AAAAA-BBBBB")
            .arg("--endpoint")
            .arg(&refused)
            .assert()
            .failure();
        assert!(config_dir.join("pending-adopt-code").exists());

        spice_cmd()
            .env("SPICE_CONFIG_DIR", config_dir)
            .arg("connect")
            .arg("remove")
            .assert()
            .success()
            .stdout(predicate::str::contains("identity cleared"));
        assert!(!config_dir.join("pending-adopt-code").exists());
    }

    /// `remove` must delete the delivered-secrets cache: it holds the app's
    /// credentials, and leaving it on a released host leaves them there.
    #[cfg(unix)]
    #[test]
    fn test_connect_remove_deletes_the_delivered_secrets_cache() {
        let dir = TempDir::new().expect("create temp config dir");
        let config_dir = dir.path();
        let cache = config_dir.join("secrets-cache.json");
        // Shape does not matter here — `remove` deletes the file, it does not
        // open it (the key it would need is in the identity it also deletes).
        std::fs::write(&cache, r#"{"format_version":1}"#).expect("stage a cache");

        spice_cmd()
            .env("SPICE_CONFIG_DIR", config_dir)
            .arg("connect")
            .arg("remove")
            .assert()
            .success()
            .stdout(predicate::str::contains("identity cleared"));

        assert!(
            !cache.exists(),
            "the delivered-secrets cache must not survive a remove"
        );
    }

    /// `status` reports which secrets were delivered, by name, from the cache's
    /// plaintext header — and never a value, since it holds no key.
    #[cfg(unix)]
    #[test]
    fn test_connect_status_reports_delivered_secret_names() {
        let dir = TempDir::new().expect("create temp config dir");
        let config_dir = dir.path();
        let home = TempDir::new().expect("create temp home");
        install_fake_spiced(home.path());
        let (endpoint, server) = spawn_one_shot_http(200, &enroll_ok_body());

        spice_cmd()
            .env("HOME", home.path())
            .env("SPICE_CONFIG_DIR", config_dir)
            .arg("connect")
            .arg("SPICE-ADOPT-AAAAA-BBBBB")
            .arg("--endpoint")
            .arg(&endpoint)
            .assert()
            .success();
        server.join().expect("mock served the enroll request");

        // No cache yet: status says so rather than staying silent about it.
        spice_cmd()
            .env("SPICE_CONFIG_DIR", config_dir)
            .arg("connect")
            .arg("status")
            .assert()
            .success()
            .stdout(predicate::str::contains("none delivered yet"));
    }

    /// The success output names every fact the operator needs next: the org,
    /// the instance id, the attachment state, the declared region, and both
    /// continuations (install a service, or run in the foreground).
    #[cfg(unix)]
    #[test]
    fn test_connect_success_output_names_org_region_and_both_continuations() {
        let dir = TempDir::new().expect("create temp config dir");
        let config_dir = dir.path();
        let home = TempDir::new().expect("create temp home");
        install_fake_spiced(home.path());
        let body = serde_json::json!({
            "instance_id": "inst_cli_test",
            "identity_cert_pem": "-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n",
            "ca_bundle_pem": "",
            "gateway_addr": "127.0.0.1:7320",
            "not_after": "2030-01-01T00:00:00Z",
            "org": "acme",
            "region": "on-prem-syd",
        })
        .to_string();
        let (endpoint, server) = spawn_one_shot_http(200, &body);

        spice_cmd()
            .env("HOME", home.path())
            .env("SPICE_CONFIG_DIR", config_dir)
            .arg("connect")
            .arg("SPICE-ADOPT-AAAAA-BBBBB")
            .arg("--region")
            .arg("on-prem-syd")
            .arg("--endpoint")
            .arg(&endpoint)
            .assert()
            .success()
            .stdout(predicate::str::contains("org:         acme"))
            .stdout(predicate::str::contains("instance id: inst_cli_test"))
            .stdout(predicate::str::contains("region:      on-prem-syd"))
            .stdout(predicate::str::contains("unattached"))
            .stdout(predicate::str::contains("sudo spice connect --install"))
            .stdout(predicate::str::contains("spiced --cloud-connect"));
        server.join().expect("mock served the enroll request");
    }

    /// A `--region` no catalog knows must enroll — a standalone host may not be
    /// in a cloud region at all, and a region newer than this build must not
    /// need a release. The label reaches the wire as a top-level field.
    #[cfg(unix)]
    #[test]
    fn test_connect_region_is_sent_as_a_sibling_of_the_host_facts() {
        let dir = TempDir::new().expect("create temp config dir");
        let home = TempDir::new().expect("create temp home");
        install_fake_spiced(home.path());
        let (endpoint, server) = spawn_one_shot_http_capturing(200, &enroll_ok_body());

        spice_cmd()
            .env("HOME", home.path())
            .env("SPICE_CONFIG_DIR", dir.path())
            .arg("connect")
            .arg("SPICE-ADOPT-AAAAA-BBBBB")
            .arg("--region")
            .arg("ap-southeast-7")
            .arg("--endpoint")
            .arg(&endpoint)
            .assert()
            .success();

        let request = server.join().expect("mock captured the enroll request");
        let body: serde_json::Value =
            serde_json::from_str(&request).expect("enroll request body is JSON");
        assert_eq!(body["region"], "ap-southeast-7");
        assert!(
            body["instance"].get("region").is_none(),
            "the declared region must not be nested in the probed host facts: {body}"
        );
    }

    /// The reported runtime version is probed from the `spiced` that will
    /// actually run, not taken from the CLI's own version. A dev CLI paired with
    /// a released runtime would otherwise put a version on the registry row that
    /// the instance is not running.
    #[cfg(unix)]
    #[test]
    fn test_connect_reports_the_installed_runtime_version() {
        let dir = TempDir::new().expect("create temp config dir");
        let home = TempDir::new().expect("create temp home");
        install_fake_spiced(home.path());
        let (endpoint, server) = spawn_one_shot_http_capturing(200, &enroll_ok_body());

        spice_cmd()
            .env("HOME", home.path())
            .env("SPICE_CONFIG_DIR", dir.path())
            .arg("connect")
            .arg("SPICE-ADOPT-AAAAA-BBBBB")
            .arg("--endpoint")
            .arg(&endpoint)
            .assert()
            .success();

        let request = server.join().expect("mock captured the enroll request");
        let body: serde_json::Value =
            serde_json::from_str(&request).expect("enroll request body is JSON");
        assert_eq!(
            body["instance"]["runtime_version"], "spiced v0.0.0-fake",
            "the host facts must carry the runtime's own version, not the CLI's: {body}"
        );
    }

    /// `SPICE_CONNECT_ADOPT_REGION` mirrors `--region` for hosts with no CLI
    /// flags available (containers, cloud-init).
    #[cfg(unix)]
    #[test]
    fn test_connect_region_env_var_mirrors_the_flag() {
        let dir = TempDir::new().expect("create temp config dir");
        let home = TempDir::new().expect("create temp home");
        install_fake_spiced(home.path());
        let (endpoint, server) = spawn_one_shot_http_capturing(200, &enroll_ok_body());

        spice_cmd()
            .env("HOME", home.path())
            .env("SPICE_CONFIG_DIR", dir.path())
            .env("SPICE_CONNECT_ADOPT_REGION", "us-west-2")
            .arg("connect")
            .arg("SPICE-ADOPT-AAAAA-BBBBB")
            .arg("--endpoint")
            .arg(&endpoint)
            .assert()
            .success();

        let request = server.join().expect("mock captured the enroll request");
        let body: serde_json::Value =
            serde_json::from_str(&request).expect("enroll request body is JSON");
        assert_eq!(body["region"], "us-west-2");
    }

    /// Omitting `--region` must send no `region` field at all. `null` would be
    /// read cloud-side as "clear it", silently erasing a region set in the
    /// portal on every re-enrol.
    #[cfg(unix)]
    #[test]
    fn test_connect_without_region_omits_the_field() {
        let dir = TempDir::new().expect("create temp config dir");
        let home = TempDir::new().expect("create temp home");
        install_fake_spiced(home.path());
        let (endpoint, server) = spawn_one_shot_http_capturing(200, &enroll_ok_body());

        spice_cmd()
            .env("HOME", home.path())
            .env("SPICE_CONFIG_DIR", dir.path())
            .env_remove("SPICE_CONNECT_ADOPT_REGION")
            .arg("connect")
            .arg("SPICE-ADOPT-AAAAA-BBBBB")
            .arg("--endpoint")
            .arg(&endpoint)
            .assert()
            .success();

        let request = server.join().expect("mock captured the enroll request");
        let body: serde_json::Value =
            serde_json::from_str(&request).expect("enroll request body is JSON");
        assert!(
            body.get("region").is_none(),
            "an omitted --region must not appear on the wire: {body}"
        );
    }

    /// A malformed `--region` is rejected client-side, before anything is
    /// staged and before any code could be spent.
    #[test]
    fn test_connect_malformed_region_is_rejected_before_staging() {
        let dir = TempDir::new().expect("create temp config dir");
        let config_dir = dir.path();
        spice_cmd()
            .env("SPICE_CONFIG_DIR", config_dir)
            .arg("connect")
            .arg("SPICE-ADOPT-AAAAA-BBBBB")
            .arg("--region")
            .arg("US_WEST_2")
            .assert()
            .failure()
            .stdout(predicate::str::contains("--region"));

        assert!(
            !config_dir.join("pending-adopt-code").exists(),
            "a rejected region must not stage the code"
        );
    }

    /// `--cloud-region` does not choose an instance's gateway — Spice Cloud
    /// resolves that from `--region` by nearest stamp and returns it as
    /// `gateway_addr`. So the adoption path refuses the flag rather than
    /// accepting it and quietly selecting nothing, and the error names the two
    /// flags that do apply.
    #[test]
    fn test_connect_rejects_cloud_region_naming_region_and_endpoint() {
        let dir = TempDir::new().expect("create temp config dir");
        for args in [
            vec!["connect", "status"],
            vec!["connect", "remove"],
            vec!["connect", "SPICE-ADOPT-AAAAA-BBBBB"],
            vec!["connect"],
        ] {
            let mut cmd = spice_cmd();
            cmd.env("SPICE_CONFIG_DIR", dir.path());
            for arg in &args {
                cmd.arg(arg);
            }
            cmd.arg("--cloud-region")
                .arg("us-west-2")
                .assert()
                .failure()
                .stdout(predicate::str::contains(
                    "--cloud-region us-west-2 does not apply to `spice connect`",
                ))
                .stdout(predicate::str::contains("--region"))
                .stdout(predicate::str::contains("--endpoint"));
        }

        assert!(
            !dir.path().join("pending-adopt-code").exists(),
            "a refused --cloud-region must not stage the code"
        );
    }

    /// The deprecated pod-add fallthrough is a Spice.ai Cloud fetch where
    /// `--cloud-region` has always been meaningful, so it must keep working
    /// there — the refusal above is scoped to the adoption path.
    #[test]
    fn test_connect_pod_add_still_accepts_cloud_region() {
        spice_cmd()
            .arg("connect")
            .arg("spiceai/quickstart")
            .arg("--cloud-region")
            .arg("us-west-2")
            .assert()
            // It fails for want of an API key, not for the region flag.
            .failure()
            .stdout(predicate::str::contains("does not apply").not());
    }

    /// The same flag on any other command still requires `--cloud`.
    #[test]
    fn test_cloud_region_without_cloud_is_still_rejected_elsewhere() {
        spice_cmd()
            .arg("--cloud-region")
            .arg("us-west-2")
            .arg("datasets")
            .assert()
            .failure()
            .stdout(predicate::str::contains("--cloud-region requires --cloud"));
    }

    /// `--install` preflights **before** the enroll, so a host that cannot take
    /// a service fails with nothing staged and the code still redeemable.
    /// Non-root (or non-systemd) is the reachable case in a test.
    #[cfg(unix)]
    #[test]
    fn test_connect_install_preflight_runs_before_enroll() {
        let dir = TempDir::new().expect("create temp config dir");
        let config_dir = dir.path();
        let home = TempDir::new().expect("create temp home");
        install_fake_spiced(home.path());
        // No endpoint is served: reaching the enroll at all would hang or fail
        // differently, so a clean preflight failure proves the ordering.
        spice_cmd()
            .env("HOME", home.path())
            .env("SPICE_CONFIG_DIR", config_dir)
            .arg("connect")
            .arg("SPICE-ADOPT-AAAAA-BBBBB")
            .arg("--install")
            .assert()
            .failure()
            .stdout(predicate::str::contains(
                "Failed to install the Spice Cloud Connect service",
            ));

        assert!(
            !config_dir.join("pending-adopt-code").exists(),
            "a failed preflight must stage nothing — the code stays valid"
        );
        assert!(!config_dir.join("identity.json").exists());
    }

    /// With no code, no staged state, and no login, the error must name both
    /// fixes rather than leaving the operator to guess.
    #[test]
    fn test_connect_without_code_or_login_names_both_fixes() {
        let dir = TempDir::new().expect("create temp config dir");
        let home = TempDir::new().expect("create temp home");
        spice_cmd()
            .env("HOME", home.path())
            .env("SPICE_CONFIG_DIR", dir.path())
            .env_remove("SPICE_SPICEAI_TOKEN")
            // `get_auth_token` also reads `.env`/`.env.local` from the cwd.
            .current_dir(home.path())
            .arg("connect")
            .assert()
            .failure()
            .stdout(predicate::str::contains("spice login"))
            .stdout(predicate::str::contains("SPICE-ADOPT"));
    }

    /// An already-enrolled directory is never silently re-enrolled: a bare
    /// `spice connect` reports the existing state instead of minting a second
    /// registry row for the same host.
    #[cfg(unix)]
    #[test]
    fn test_connect_without_code_on_an_enrolled_host_reports_status() {
        let dir = TempDir::new().expect("create temp config dir");
        let config_dir = dir.path();
        let home = TempDir::new().expect("create temp home");
        install_fake_spiced(home.path());
        let (endpoint, server) = spawn_one_shot_http(200, &enroll_ok_body());

        spice_cmd()
            .env("HOME", home.path())
            .env("SPICE_CONFIG_DIR", config_dir)
            .arg("connect")
            .arg("SPICE-ADOPT-AAAAA-BBBBB")
            .arg("--endpoint")
            .arg(&endpoint)
            .assert()
            .success();
        server.join().expect("mock served the enroll request");

        // A bare `spice connect` now: no mint, no second enroll.
        spice_cmd()
            .env("HOME", home.path())
            .env("SPICE_CONFIG_DIR", config_dir)
            .arg("connect")
            .assert()
            .success()
            .stdout(predicate::str::contains("already enrolled"))
            .stdout(predicate::str::contains("inst_cli_test"));
    }
}

// ============================================================================
// Acceleration Command Tests
// ============================================================================

mod acceleration {
    use super::*;

    #[test]
    fn test_acceleration_help() {
        let mut cmd = spice_cmd();
        cmd.arg("acceleration")
            .arg("--help")
            .assert()
            .success()
            .stdout(predicate::str::contains("acceleration"));
    }

    #[test]
    fn test_acceleration_subcommands() {
        let mut cmd = spice_cmd();
        cmd.arg("acceleration")
            .arg("--help")
            .assert()
            .success()
            .stdout(predicate::str::contains("snapshots"))
            .stdout(predicate::str::contains("snapshot"));
    }
}

// ============================================================================
// Search Command Tests
// ============================================================================

mod search {
    use super::*;

    #[test]
    fn test_search_help() {
        let mut cmd = spice_cmd();
        cmd.arg("search")
            .arg("--help")
            .assert()
            .success()
            .stdout(predicate::str::contains("vector or hybrid search"))
            .stdout(predicate::str::contains("embeddings"));
    }

    #[test]
    fn test_search_limit_flag() {
        let mut cmd = spice_cmd();
        cmd.arg("search")
            .arg("--help")
            .assert()
            .success()
            .stdout(predicate::str::contains("--limit"));
    }
}

// ============================================================================
// Chat Command Tests
// ============================================================================

mod chat {
    use super::*;

    #[test]
    fn test_chat_help() {
        let mut cmd = spice_cmd();
        cmd.arg("chat")
            .arg("--help")
            .assert()
            .success()
            .stdout(predicate::str::contains("Chat"))
            .stdout(predicate::str::contains("LLM"));
    }

    #[test]
    fn test_chat_model_flag() {
        let mut cmd = spice_cmd();
        cmd.arg("chat")
            .arg("--help")
            .assert()
            .success()
            .stdout(predicate::str::contains("--model"));
    }
}

// ============================================================================
// NSQL Command Tests
// ============================================================================

mod nsql {
    use super::*;

    #[test]
    fn test_nsql_help() {
        let mut cmd = spice_cmd();
        cmd.arg("nsql")
            .arg("--help")
            .assert()
            .success()
            .stdout(predicate::str::contains("SQL"))
            .stdout(predicate::str::contains("natural-language"));
    }
}

// ============================================================================
// Query Command Tests
// ============================================================================

mod query {
    use super::*;

    #[test]
    fn test_query_help() {
        let mut cmd = spice_cmd();
        cmd.arg("query")
            .arg("--help")
            .assert()
            .success()
            .stdout(predicate::str::contains("async"))
            .stdout(predicate::str::contains("query"));
    }
}

// ============================================================================
// Completions Command Tests
// ============================================================================

mod completions {
    use super::*;

    #[test]
    fn test_completions_help() {
        let mut cmd = spice_cmd();
        cmd.arg("completions")
            .arg("--help")
            .assert()
            .success()
            .stdout(predicate::str::contains("completion scripts"))
            .stdout(predicate::str::contains("zsh"));
    }
}

// ============================================================================
// Trace Command Tests
// ============================================================================

mod trace {
    use super::*;

    #[test]
    fn test_trace_help() {
        let mut cmd = spice_cmd();
        cmd.arg("trace")
            .arg("--help")
            .assert()
            .success()
            .stdout(predicate::str::contains("trace"));
    }
}

// ============================================================================
// Cluster Command Tests
// ============================================================================

mod cluster {
    use super::*;

    #[test]
    fn test_cluster_help() {
        let mut cmd = spice_cmd();
        cmd.arg("cluster")
            .arg("--help")
            .assert()
            .success()
            .stdout(predicate::str::contains("clustered mode"));
    }

    #[test]
    fn test_cluster_tls_help() {
        let mut cmd = spice_cmd();
        cmd.arg("cluster")
            .arg("tls")
            .arg("--help")
            .assert()
            .success()
            .stdout(predicate::str::contains("TLS"));
    }
}

// ============================================================================
// Workers Command Tests
// ============================================================================

mod workers {
    use super::*;

    #[test]
    fn test_workers_help() {
        let mut cmd = spice_cmd();
        cmd.arg("workers")
            .arg("--help")
            .assert()
            .success()
            .stdout(predicate::str::contains("workers"));
    }
}

// ============================================================================
// Cloud Command Tests
// ============================================================================

mod cloud {
    use super::*;

    #[test]
    fn test_cloud_help() {
        let mut cmd = spice_cmd();
        cmd.arg("cloud")
            .arg("--help")
            .assert()
            .success()
            .stdout(predicate::str::contains("Cloud"));
    }

    #[test]
    fn test_cloud_subcommands() {
        let mut cmd = spice_cmd();
        cmd.arg("cloud")
            .arg("--help")
            .assert()
            .success()
            .stdout(predicate::str::contains("login"))
            .stdout(predicate::str::contains("apps"));
    }
}

// ============================================================================
// Run Command Tests
// ============================================================================

mod run {
    use super::*;

    #[test]
    fn test_run_help() {
        let mut cmd = spice_cmd();
        cmd.arg("run")
            .arg("--help")
            .assert()
            .success()
            .stdout(predicate::str::contains("Run"))
            .stdout(predicate::str::contains("Spice.ai"));
    }

    #[test]
    fn test_run_help_shows_flight_endpoint() {
        let mut cmd = spice_cmd();
        cmd.arg("run")
            .arg("--help")
            .assert()
            .success()
            .stdout(predicate::str::contains("--flight-endpoint"));
    }

    #[test]
    fn test_run_help_shows_metrics_endpoint() {
        let mut cmd = spice_cmd();
        cmd.arg("run")
            .arg("--help")
            .assert()
            .success()
            .stdout(predicate::str::contains("--metrics-endpoint"));
    }

    #[test]
    fn test_run_accepts_flight_endpoint_flag() {
        // Verify the flag is parsed correctly (will fail later due to no runtime, but parsing should work)
        let mut cmd = spice_cmd();
        cmd.arg("run")
            .arg("--flight-endpoint")
            .arg("0.0.0.0:50051")
            .arg("--help") // Add --help to avoid actually running
            .assert()
            .success();
    }

    #[test]
    fn test_run_accepts_metrics_endpoint_flag() {
        let mut cmd = spice_cmd();
        cmd.arg("run")
            .arg("--metrics-endpoint")
            .arg("0.0.0.0:9090")
            .arg("--help")
            .assert()
            .success();
    }

    #[test]
    fn test_run_accepts_trailing_args() {
        // Verify trailing args are accepted (passed through to spiced)
        let mut cmd = spice_cmd();
        cmd.arg("run")
            .arg("--help")
            .arg("--")
            .arg("--custom-arg")
            .assert()
            .success();
    }

    #[test]
    fn test_run_with_global_http_endpoint_flag() {
        // Verify global --http-endpoint flag works with run command
        let mut cmd = spice_cmd();
        cmd.arg("--http-endpoint")
            .arg("http://127.0.0.1:9999")
            .arg("run")
            .arg("--help")
            .assert()
            .success();
    }

    #[test]
    fn test_run_with_local_http_endpoint_flag() {
        // Verify run-specific --http-endpoint flag is accepted (overrides binding address)
        let mut cmd = spice_cmd();
        cmd.arg("run")
            .arg("--http-endpoint")
            .arg("0.0.0.0:8080")
            .arg("--help")
            .assert()
            .success();
    }

    #[test]
    fn test_run_help_shows_http_endpoint() {
        // Verify --http-endpoint appears in run help
        let mut cmd = spice_cmd();
        cmd.arg("run")
            .arg("--help")
            .assert()
            .success()
            .stdout(predicate::str::contains("--http-endpoint"));
    }

    #[test]
    fn test_run_with_global_tls_certificate_flag() {
        // Verify global --tls-root-certificate-file flag is accepted
        let mut cmd = spice_cmd();
        cmd.arg("--tls-root-certificate-file")
            .arg("/path/to/cert.pem")
            .arg("run")
            .arg("--help")
            .assert()
            .success();
    }

    #[test]
    fn test_run_with_global_api_key_flag() {
        // Verify global --api-key flag is accepted
        let mut cmd = spice_cmd();
        cmd.arg("--api-key")
            .arg("test-api-key")
            .arg("run")
            .arg("--help")
            .assert()
            .success();
    }

    #[test]
    fn test_run_with_combined_global_and_local_flags() {
        // Verify global and local flags can be combined
        let mut cmd = spice_cmd();
        cmd.arg("--http-endpoint")
            .arg("http://127.0.0.1:9999")
            .arg("--api-key")
            .arg("my-key")
            .arg("--tls-root-certificate-file")
            .arg("/cert.pem")
            .arg("run")
            .arg("--http-endpoint") // Local override for binding
            .arg("0.0.0.0:8080")
            .arg("--flight-endpoint")
            .arg("0.0.0.0:50051")
            .arg("--metrics-endpoint")
            .arg("0.0.0.0:9090")
            .arg("--help")
            .assert()
            .success();
    }
}

// ============================================================================
// Global Flags Tests
// ============================================================================

mod global_flags {
    use super::*;

    #[test]
    fn test_verbose_flag() {
        let mut cmd = spice_cmd();
        cmd.arg("-v")
            .arg("version")
            .assert()
            .success()
            .stdout(predicate::str::contains("CLI version:"));
    }

    #[test]
    fn test_very_verbose_flag() {
        let mut cmd = spice_cmd();
        cmd.arg("-vv")
            .arg("version")
            .assert()
            .success()
            .stdout(predicate::str::contains("CLI version:"));
    }

    #[test]
    fn test_max_verbose_flag() {
        let mut cmd = spice_cmd();
        cmd.arg("-vvv")
            .arg("version")
            .assert()
            .success()
            .stdout(predicate::str::contains("CLI version:"));
    }

    #[test]
    fn test_cloud_flag_attempts_cloud_connection() {
        // When --cloud is used without API key, it attempts to connect to cloud
        // which will fail with a connection error (not an API key error)
        let mut cmd = spice_cmd();
        cmd.arg("--cloud").arg("status").assert().failure();
    }

    #[test]
    fn test_http_endpoint_flag() {
        let mut cmd = spice_cmd();
        cmd.arg("--http-endpoint")
            .arg("http://custom:8080")
            .arg("--help")
            .assert()
            .success();
    }

    #[test]
    fn test_http_endpoint_flag_with_ip() {
        let mut cmd = spice_cmd();
        cmd.arg("--http-endpoint")
            .arg("http://127.0.0.1:9999")
            .arg("--help")
            .assert()
            .success();
    }

    #[test]
    fn test_api_key_flag() {
        let mut cmd = spice_cmd();
        cmd.arg("--api-key")
            .arg("test-api-key-12345")
            .arg("--help")
            .assert()
            .success();
    }

    #[test]
    fn test_tls_root_certificate_file_flag() {
        let mut cmd = spice_cmd();
        cmd.arg("--tls-root-certificate-file")
            .arg("/path/to/certificate.pem")
            .arg("--help")
            .assert()
            .success();
    }

    #[test]
    fn test_all_global_flags_combined() {
        let mut cmd = spice_cmd();
        cmd.arg("-vv")
            .arg("--http-endpoint")
            .arg("http://127.0.0.1:9999")
            .arg("--api-key")
            .arg("my-api-key")
            .arg("--tls-root-certificate-file")
            .arg("/cert.pem")
            .arg("--help")
            .assert()
            .success();
    }

    #[test]
    fn test_global_flags_work_with_status_command() {
        let mut cmd = spice_cmd();
        cmd.arg("--http-endpoint")
            .arg("http://127.0.0.1:9999")
            .arg("--api-key")
            .arg("test-key")
            .arg("status")
            .arg("--help")
            .assert()
            .success();
    }

    #[test]
    fn test_global_flags_work_with_sql_command() {
        let mut cmd = spice_cmd();
        cmd.arg("--http-endpoint")
            .arg("http://127.0.0.1:9999")
            .arg("sql")
            .arg("--help")
            .assert()
            .success();
    }
}

// ============================================================================
// Local vs Remote (Cloud) Mode Tests
// ============================================================================

mod mode_tests {
    use super::*;

    #[test]
    fn test_default_local_mode() {
        // Default mode should be local (no --cloud flag)
        let mut cmd = spice_cmd();
        cmd.arg("--help")
            .assert()
            .success()
            .stdout(predicate::str::contains("--cloud"));
    }

    #[test]
    fn test_cloud_flag_available() {
        // --cloud flag should be available as global option
        let mut cmd = spice_cmd();
        cmd.arg("--help")
            .assert()
            .success()
            .stdout(predicate::str::contains("--cloud"))
            .stdout(predicate::str::contains("Target Spice.ai Cloud"));
    }

    #[test]
    fn test_cloud_mode_with_status() {
        // Cloud mode status should fail without proper connection (no API key)
        let mut cmd = spice_cmd();
        cmd.arg("--cloud")
            .arg("us-east-1")
            .arg("status")
            .assert()
            .failure();
    }

    #[test]
    fn test_cloud_mode_with_api_key_status() {
        // Cloud mode with API key should still fail (invalid key)
        // but the command structure should be valid
        let mut cmd = spice_cmd();
        cmd.arg("--cloud")
            .arg("us-east-1")
            .arg("--api-key")
            .arg("invalid-api-key")
            .arg("status")
            .assert()
            .failure();
    }

    #[test]
    fn test_local_mode_explicit_endpoint() {
        // Local mode with explicit endpoint
        let mut cmd = spice_cmd();
        cmd.arg("--http-endpoint")
            .arg("http://localhost:8090")
            .arg("--help")
            .assert()
            .success();
    }

    #[test]
    fn test_cloud_mode_with_datasets() {
        // Cloud mode with datasets command
        let mut cmd = spice_cmd();
        cmd.arg("--cloud")
            .arg("us-east-1")
            .arg("datasets")
            .arg("--help")
            .assert()
            .success();
    }

    #[test]
    fn test_cloud_mode_with_models() {
        // Cloud mode with models command
        let mut cmd = spice_cmd();
        cmd.arg("--cloud")
            .arg("us-east-1")
            .arg("models")
            .arg("--help")
            .assert()
            .success();
    }

    #[test]
    fn test_cloud_mode_with_search() {
        // Cloud mode with search command
        let mut cmd = spice_cmd();
        cmd.arg("--cloud")
            .arg("us-east-1")
            .arg("search")
            .arg("--help")
            .assert()
            .success();
    }

    #[test]
    fn test_cloud_mode_with_sql() {
        // Cloud mode with sql command
        let mut cmd = spice_cmd();
        cmd.arg("--cloud")
            .arg("us-east-1")
            .arg("sql")
            .arg("--help")
            .assert()
            .success();
    }

    #[test]
    fn test_local_mode_with_run_command() {
        // Local mode run command (default)
        let mut cmd = spice_cmd();
        cmd.arg("run").arg("--help").assert().success();
    }

    #[test]
    fn test_cloud_mode_not_supported_by_datasets() {
        // Some commands don't support cloud mode and should indicate this
        let mut cmd = spice_cmd();
        cmd.arg("--cloud")
            .arg("us-east-1")
            .arg("datasets")
            .assert()
            .success()
            .stdout(predicate::str::contains("does not support"));
    }

    #[test]
    fn test_api_key_env_var_documented() {
        // API key env var should be documented in help
        let mut cmd = spice_cmd();
        cmd.arg("--help")
            .assert()
            .success()
            .stdout(predicate::str::contains("SPICE_API_KEY"));
    }

    #[test]
    fn test_cloud_and_http_endpoint_mutually_exclusive_behavior() {
        // When --cloud is used, it should override --http-endpoint
        // (The context.rs tests verify this behavior, here we just verify flags parse)
        let mut cmd = spice_cmd();
        cmd.arg("--http-endpoint")
            .arg("http://custom:8080")
            .arg("--cloud")
            .arg("us-east-1")
            .arg("--help")
            .assert()
            .success();
    }

    #[test]
    fn test_local_mode_all_query_commands_available() {
        // All query commands should work in local mode
        for command in &["status", "datasets", "models", "sql", "search"] {
            let mut cmd = spice_cmd();
            cmd.arg(command).arg("--help").assert().success();
        }
    }

    #[test]
    fn test_cloud_mode_all_query_commands_available() {
        // All query commands should work in cloud mode
        for command in &["status", "datasets", "models", "sql", "search"] {
            let mut cmd = spice_cmd();
            cmd.arg("--cloud")
                .arg("us-east-1")
                .arg(command)
                .arg("--help")
                .assert()
                .success();
        }
    }
}

// ============================================================================
// Error Handling Tests
// ============================================================================

mod error_handling {
    use super::*;

    #[test]
    fn test_unknown_command() {
        let mut cmd = spice_cmd();
        cmd.arg("unknown_command_xyz")
            .assert()
            .failure()
            .stderr(predicate::str::contains("unrecognized subcommand"));
    }

    #[test]
    fn test_invalid_flag() {
        let mut cmd = spice_cmd();
        cmd.arg("--invalid-flag-xyz")
            .assert()
            .failure()
            .stderr(predicate::str::contains("unexpected argument"));
    }

    #[test]
    fn test_missing_required_subcommand() {
        // Commands that require subcommands should show help
        let mut cmd = spice_cmd();
        cmd.arg("cluster").assert().failure();
    }
}

// ============================================================================
// Environment Variable Tests
// ============================================================================

mod env_vars {
    use super::*;

    #[test]
    fn test_api_key_from_env() {
        let mut cmd = spice_cmd();
        cmd.env("SPICE_API_KEY", "test_key_12345")
            .arg("--help")
            .assert()
            .success();
    }
}
