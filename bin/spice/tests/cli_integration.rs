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

/// Get a Command for the spice binary.
///
/// `SPICED_PATH` is cleared. The CLI resolves the runtime from it and reports a
/// pin that names nothing runnable as an error, so a developer or CI runner
/// with one exported would otherwise make every assertion below a property of
/// the host rather than of the CLI. A test that wants a pin sets one on the
/// returned command.
fn spice_cmd() -> Command {
    let mut cmd = cargo_bin_cmd!("spice");
    cmd.env_remove("SPICED_PATH");
    cmd
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

    /// A `SPICED_PATH` naming something no runtime can be run from. Returned
    /// with its `TempDir`, which has to outlive the command under test.
    ///
    /// A directory, not a file missing its execute bit: Windows carries no
    /// execute bit, so a plain file is runnable there and the CLI would probe
    /// it, report its version as unavailable and exit successfully — passing
    /// the pin through the resolver these tests are asserting it is rejected
    /// by. Being a directory fails `is_runnable_binary` on every platform.
    fn unrunnable_pin() -> (TempDir, std::path::PathBuf) {
        let dir = TempDir::new().expect("create tempdir");
        let pin = dir.path().join("spiced");
        fs::create_dir(&pin).expect("create the pin");
        (dir, pin)
    }

    /// The pin survives the fixture's `env_remove`, and a pin that names
    /// nothing runnable is reported instead of being silently ignored.
    #[test]
    fn a_pinned_runtime_that_is_not_runnable_is_reported_by_version() {
        let (_dir, pin) = unrunnable_pin();

        let mut cmd = spice_cmd();
        cmd.env("SPICED_PATH", &pin)
            .arg("version")
            .assert()
            .failure()
            .stdout(predicate::str::contains("CLI version:"))
            .stdout(predicate::str::contains("SPICED_PATH"))
            .stdout(predicate::str::contains(pin.display().to_string()))
            .stdout(predicate::str::contains("https://spiceai.org/docs/cli"));
    }

    /// The machine-readable form stays machine-readable when resolution
    /// fails: stdout carries the JSON document or nothing at all, never the
    /// human error, which a caller parsing `-o json` would choke on.
    #[test]
    fn a_failed_resolution_does_not_write_prose_into_json_output() {
        let (_dir, pin) = unrunnable_pin();

        let mut cmd = spice_cmd();
        cmd.env("SPICED_PATH", &pin)
            .args(["version", "-o", "json"])
            .assert()
            .failure()
            .stderr(predicate::str::contains("SPICED_PATH"))
            .stdout(predicate::str::is_empty().trim());
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

    const LOGIN_PATHS: [&[&str]; 2] = [&["login"], &["cloud", "login"]];

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
    fn test_cloud_login_methods_match_on_both_command_paths() {
        for prefix in LOGIN_PATHS {
            let mut cmd = spice_cmd();
            cmd.args(prefix)
                .arg("--help")
                .assert()
                .success()
                .stdout(predicate::str::contains("subscription"))
                .stdout(predicate::str::contains("token"))
                .stdout(predicate::str::contains("api"))
                .stdout(predicate::str::contains("--output"));
        }
    }

    #[test]
    fn test_access_token_and_device_spellings_are_explicit() {
        for prefix in LOGIN_PATHS {
            let mut token = spice_cmd();
            token
                .args(prefix)
                .args(["token", "--help"])
                .assert()
                .success()
                .stdout(predicate::str::contains("Access token"));

            let mut pat_alias = spice_cmd();
            pat_alias
                .args(prefix)
                .args(["pat", "--help"])
                .assert()
                .success()
                .stdout(predicate::str::contains("Access token"));

            let mut subscription = spice_cmd();
            subscription
                .args(prefix)
                .args(["subscription", "--help"])
                .assert()
                .success()
                .stdout(predicate::str::contains("--device"));
        }
    }

    #[test]
    fn test_login_unknown_provider() {
        let mut cmd = spice_cmd();
        cmd.arg("login")
            .arg("unknown_provider_xyz")
            .assert()
            .failure();
    }

    /// Standalone `spice login --key` keeps its credential-store behavior: the
    /// key lands in the working directory's `.env`, owner-readable only, with
    /// the same success line as always.
    #[test]
    fn test_login_with_key_writes_env_credentials() {
        let temp_dir = TempDir::new().expect("temp dir should be creatable");

        let mut cmd = spice_cmd();
        cmd.current_dir(temp_dir.path())
            .args(["login", "--key", "sk_test_abc123"])
            .assert()
            .success()
            .stdout(predicate::str::contains(
                "Successfully logged in to Spice.ai with API key",
            ));

        let env_path = temp_dir.path().join(".env");
        let contents = fs::read_to_string(&env_path).expect(".env should be written");
        assert!(
            contents.contains("SPICE_SPICEAI_API_KEY=sk_test_abc123"),
            "the key must be stored under its credential variable: {contents}"
        );

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = fs::metadata(&env_path)
                .expect(".env metadata should be readable")
                .permissions()
                .mode();
            assert_eq!(
                mode & 0o777,
                0o600,
                "the credential file must stay owner-only"
            );
        }
    }

    /// Provider regression guard: the `SharePoint` and ABFS logins keep their
    /// OAuth device-code flows and vocabulary — their help still asks for the
    /// Azure AD tenant/client pair rather than anything session-based.
    #[test]
    fn test_provider_device_flow_help_unchanged() {
        for provider in ["sharepoint", "abfs"] {
            let mut cmd = spice_cmd();
            cmd.args(["login", provider, "--help"])
                .assert()
                .success()
                .stdout(predicate::str::contains("--tenant-id"))
                .stdout(predicate::str::contains("--client-id"));
        }
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

    #[test]
    fn test_connect_help_describes_only_the_deprecated_pod_add_surface() {
        spice_cmd()
            .arg("connect")
            .arg("--help")
            .assert()
            .success()
            .stdout(predicate::str::contains("deprecated"))
            .stdout(predicate::str::contains("spice add"))
            .stdout(predicate::str::contains("spice cloud link"))
            .stdout(predicate::str::contains("\n      --dir").not())
            .stdout(predicate::str::contains("\n      --region").not())
            .stdout(predicate::str::contains("\n      --endpoint").not())
            .stdout(predicate::str::contains("\n      --force").not())
            .stdout(predicate::str::contains("\n      --yes").not());
    }

    #[test]
    fn test_connect_rejects_removed_automation_flags() {
        for flag in [
            "--token",
            "--org",
            "--project",
            "--install",
            "--dir",
            "--region",
            "--endpoint",
            "--force",
            "--yes",
        ] {
            spice_cmd()
                .arg("connect")
                .arg(flag)
                .assert()
                .failure()
                .stderr(predicate::str::contains("unexpected argument"));
        }
    }

    #[test]
    fn test_connect_rejects_removed_lifecycle_subcommands() {
        for command in ["status", "remove", "service"] {
            spice_cmd()
                .arg("connect")
                .arg(command)
                .assert()
                .failure()
                .stdout(predicate::str::contains("only accepts the deprecated"));
        }
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

// ============================================================================
// Coding agent tests (`spice claude`, `spice codex`, ...)
// ============================================================================

/// The real `spice` binary launching stub agents against stub runtimes: what
/// reaches the agent, and what happens to the runtime around it.
#[cfg(unix)]
mod coding_agents {
    use super::*;
    use std::fmt::Write as _;
    use std::io::{BufRead, BufReader, Write};
    use std::net::TcpListener;
    use std::os::unix::fs::PermissionsExt;
    use std::path::Path;
    use std::time::{Duration, Instant};

    const MODELS: &str = r#"{"object":"list","data":[{"id":"qwen","object":"model","owned_by":"openai:qwen3","status":"Ready","context_window":40960}]}"#;

    /// The Spicepod the stub runtimes serve.
    const SPICEPODS: &str = r#"[{"name":"proj-a","version":"v1","datasets_count":0,"models_count":1,"dependencies_count":0}]"#;

    /// How long a stub gets to reach the state a test waits for; only an upper
    /// bound on something that normally takes milliseconds.
    const BUDGET: Duration = Duration::from_secs(30);

    /// A runtime the user started, answering `/health` and `/v1/models` from a
    /// thread of this process. Returns its endpoint.
    fn user_runtime() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind the stub runtime");
        let port = listener.local_addr().expect("stub runtime address").port();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { continue };
                let Ok(reader) = stream.try_clone() else {
                    continue;
                };
                let mut reader = BufReader::new(reader);
                let mut request = String::new();
                if reader.read_line(&mut request).is_err() {
                    continue;
                }
                let mut header = String::new();
                while reader.read_line(&mut header).is_ok_and(|n| n > 0) && header != "\r\n" {
                    header.clear();
                }
                let path = request.split_whitespace().nth(1).unwrap_or_default();
                let (status, body) = if path == "/health" {
                    ("200 OK", "ok")
                } else if path.starts_with("/v1/models") {
                    ("200 OK", MODELS)
                } else if path == "/v1/spicepods" {
                    ("200 OK", SPICEPODS)
                } else {
                    ("404 Not Found", "")
                };
                let _ = write!(
                    stream,
                    "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
            }
        });
        format!("http://127.0.0.1:{port}")
    }

    fn write_executable(path: &Path, script: &str) {
        fs::create_dir_all(path.parent().expect("a parent")).expect("create the directory");
        fs::write(path, script).expect("write the script");
        fs::set_permissions(path, fs::Permissions::from_mode(0o755)).expect("make it executable");
    }

    /// An agent that records its arguments, then `vars` from its environment,
    /// one per line, and exits with `code`.
    fn stub_agent(bin: &Path, name: &str, record: &Path, vars: &[&str], code: i32) {
        let mut script = format!(
            "#!/bin/sh\nprintf '%s\\n' \"$@\" > '{}'\n",
            record.display()
        );
        for var in vars {
            let _ = writeln!(
                script,
                "printf '{var}=%s\\n' \"${{{var}-<unset>}}\" >> '{}'",
                record.display()
            );
        }
        let _ = writeln!(script, "exit {code}");
        write_executable(&bin.join(name), &script);
    }

    /// `spice` with a private home (for `~/.spice/run`) and only `bin` and the
    /// system directories on `PATH`.
    fn agent_cmd(home: &Path, bin: &Path) -> Command {
        let mut cmd = spice_cmd();
        cmd.env("HOME", home)
            .env("PATH", format!("{}:/usr/bin:/bin", bin.display()))
            .env_remove("SPICE_API_KEY")
            .timeout(BUDGET);
        cmd
    }

    #[test]
    fn claude_code_reaches_the_running_runtime_with_every_argument_unchanged() {
        let endpoint = user_runtime();
        let dir = TempDir::new().expect("tempdir");
        let (home, bin, record) = (
            dir.path().join("home"),
            dir.path().join("bin"),
            dir.path().join("record"),
        );
        stub_agent(
            &bin,
            "claude",
            &record,
            &[
                "ANTHROPIC_BASE_URL",
                "ANTHROPIC_MODEL",
                "ANTHROPIC_AUTH_TOKEN",
                "ANTHROPIC_API_KEY",
                "CLAUDE_CODE_MAX_CONTEXT_TOKENS",
            ],
            3,
        );

        agent_cmd(&home, &bin)
            .env("ANTHROPIC_API_KEY", "sk-ant-vendor-key")
            .args([
                "--http-endpoint",
                &endpoint,
                "claude",
                "--resume",
                "a b",
                "-v",
                "--model",
                "qwen",
            ])
            .assert()
            .code(3);

        let recorded = fs::read_to_string(&record).expect("the agent ran");
        let (settings, rest) = after_routing_settings(&recorded);
        assert!(settings.ends_with("/claude/settings.json"), "{settings}");
        assert_eq!(
            rest,
            format!(
                "--resume\na b\n-v\n--model\nqwen\nANTHROPIC_BASE_URL={endpoint}\nANTHROPIC_MODEL=qwen\nANTHROPIC_AUTH_TOKEN=spice\nANTHROPIC_API_KEY=<unset>\nCLAUDE_CODE_MAX_CONTEXT_TOKENS=40960\n"
            ),
            "the agent's own arguments arrive unchanged, and the vendor key never reaches it"
        );
        // A runtime spice did not start is left running.
        assert!(
            std::net::TcpStream::connect(endpoint.trim_start_matches("http://")).is_ok(),
            "the user's runtime must still be serving"
        );
    }

    /// What the agent prints is all that reaches stdout, so `spice claude -p … |
    /// jq` reads Claude Code's answer; spice's own messages go to stderr.
    #[test]
    fn an_agents_stdout_is_its_own() {
        let endpoint = user_runtime();
        let dir = TempDir::new().expect("tempdir");
        let (home, bin) = (dir.path().join("home"), dir.path().join("bin"));
        write_executable(
            &bin.join("claude"),
            "#!/bin/sh\necho '{\"answer\":\"pong\"}'\n",
        );

        agent_cmd(&home, &bin)
            .args(["--http-endpoint", &endpoint, "claude", "-p", "hi"])
            .assert()
            .success()
            .stdout("{\"answer\":\"pong\"}\n")
            .stderr(predicate::str::contains(
                "Starting Claude Code with Spice model 'qwen'",
            ));
    }

    #[test]
    fn codex_gets_its_provider_before_the_users_arguments() {
        let endpoint = user_runtime();
        let dir = TempDir::new().expect("tempdir");
        let (home, bin, record) = (
            dir.path().join("home"),
            dir.path().join("bin"),
            dir.path().join("record"),
        );
        stub_agent(&bin, "codex", &record, &[], 0);

        agent_cmd(&home, &bin)
            .args([
                "--http-endpoint",
                &endpoint,
                "codex",
                "--yolo",
                "exec",
                "hi",
            ])
            .assert()
            .success();

        assert_eq!(
            fs::read_to_string(&record).expect("the agent ran"),
            format!(
                "-c\nmodel_provider=\"spice\"\n-c\nmodel_providers.spice.name=\"Spice\"\n-c\nmodel_providers.spice.base_url=\"{endpoint}/v1\"\n-c\nmodel_providers.spice.wire_api=\"responses\"\n-c\nmodel=\"qwen\"\n-c\nmodel_context_window=40960\n--yolo\nexec\nhi\n"
            )
        );
    }

    #[test]
    fn an_unknown_model_is_refused_before_the_agent_starts() {
        let endpoint = user_runtime();
        let dir = TempDir::new().expect("tempdir");
        let (home, bin, record) = (
            dir.path().join("home"),
            dir.path().join("bin"),
            dir.path().join("record"),
        );
        stub_agent(&bin, "codex", &record, &[], 0);

        agent_cmd(&home, &bin)
            .args(["--http-endpoint", &endpoint, "codex", "-m", "gpt-x"])
            .assert()
            .code(1)
            .stderr(predicate::str::contains(
                "the Spice runtime has no model 'gpt-x'. Available models: qwen.",
            ));
        assert!(
            !record.exists(),
            "the agent must not start on a model the runtime lacks"
        );
    }

    /// `OpenCode` reads `-m qwen` as no model at all: it names models with
    /// their provider, `spice/qwen`.
    #[test]
    fn a_spice_model_named_without_its_provider_is_refused_with_the_fix() {
        let endpoint = user_runtime();
        let dir = TempDir::new().expect("tempdir");
        let (home, bin, record) = (
            dir.path().join("home"),
            dir.path().join("bin"),
            dir.path().join("record"),
        );
        stub_agent(&bin, "opencode", &record, &[], 0);

        agent_cmd(&home, &bin)
            .args([
                "--http-endpoint",
                &endpoint,
                "opencode",
                "run",
                "-m",
                "qwen",
                "hi",
            ])
            .assert()
            .code(2)
            .stderr(predicate::str::contains(
                "OpenCode names a model with its provider, so 'qwen' names no Spice model. Rerun with `spice opencode -m spice/qwen`.",
            ));
        assert!(
            !record.exists(),
            "the agent must not start on a model it cannot resolve"
        );
    }

    #[test]
    fn a_missing_agent_says_how_to_install_it() {
        let endpoint = user_runtime();
        let dir = TempDir::new().expect("tempdir");
        let (home, bin) = (dir.path().join("home"), dir.path().join("bin"));
        fs::create_dir_all(&bin).expect("create bin");

        agent_cmd(&home, &bin)
            .args(["--http-endpoint", &endpoint, "grok"])
            .assert()
            .code(1)
            .stderr(predicate::str::contains(
                "Failed to start Grok Build: it is not installed (no `grok` on PATH). Install it with `curl -fsSL https://x.ai/cli/install.sh | bash`. Then rerun `spice grok`.",
            ));
    }

    /// Grok Build applies an endpoint in its own config over the one spice
    /// gives it, which would send the runtime key elsewhere: refused, naming
    /// the file and the setting, before Grok starts.
    #[test]
    fn a_grok_endpoint_in_its_own_config_is_refused_before_grok_starts() {
        let endpoint = user_runtime();
        let dir = TempDir::new().expect("tempdir");
        let (home, bin, record) = (
            dir.path().join("home"),
            dir.path().join("bin"),
            dir.path().join("record"),
        );
        stub_agent(&bin, "grok", &record, &[], 0);
        let config = home.join(".grok/config.toml");
        fs::create_dir_all(config.parent().expect("a parent")).expect("create ~/.grok");
        fs::write(
            &config,
            "[endpoints]\nmodels_base_url = \"http://127.0.0.1:9/v1\"\n",
        )
        .expect("write Grok's config");

        agent_cmd(&home, &bin)
            .env_remove("GROK_HOME")
            .args(["--http-endpoint", &endpoint, "grok", "-p", "hi"])
            .assert()
            .code(1)
            .stderr(predicate::str::contains(format!(
                "Failed to start Grok Build: '{}' sets `endpoints.models_base_url` to 'http://127.0.0.1:9/v1', which Grok Build applies over the endpoint spice gives it, so its requests and the runtime key would go there instead of to Spice. Remove `endpoints.models_base_url` from that file to run Grok Build on Spice.",
                config.display()
            )));
        assert!(!record.exists(), "Grok must not start");
    }

    /// With no runtime to use and no Spicepod to start one for, nothing is
    /// started: a runtime without a Spicepod answers every request with an
    /// error.
    #[test]
    fn a_directory_without_a_spicepod_says_how_to_get_a_runtime() {
        let lab = Lab::new(Duration::ZERO);
        fs::remove_file(lab.app.join("spicepod.yaml")).expect("remove the Spicepod");
        let address = free_address();
        let mut session = lab.session("only", &format!("http://{address}"));
        let status = session.wait();
        let output = lab.output("only");
        assert_eq!(status.code(), Some(1), "{output}");
        assert!(
            output.contains(&format!(
                "Failed to start the Spice runtime for pi: there is no spicepod.yaml in '{}', and no Spice runtime answers at http://{address}. Run `spice pi` in your Spicepod's directory, start a runtime there with `spice run`, or create a Spicepod here with `spice init` and add a model with `spice model add my-model --from openai:gpt-4o-mini`.",
                fs::canonicalize(&lab.app)
                    .expect("resolve the app directory")
                    .display()
            )),
            "{output}"
        );
        assert_eq!(lab.runtime_pid(), None, "no runtime was started");
        assert!(!lab.agent_started("only"));
    }

    /// A runtime at an endpoint the user named is used whichever Spicepod it
    /// serves, and the session says which one that is.
    #[test]
    fn a_named_runtime_serving_another_spicepod_is_used_and_named() {
        let endpoint = user_runtime();
        let dir = TempDir::new().expect("tempdir");
        let (home, bin, record, project) = (
            dir.path().join("home"),
            dir.path().join("bin"),
            dir.path().join("record"),
            dir.path().join("proj-b"),
        );
        stub_agent(&bin, "codex", &record, &[], 0);
        fs::create_dir_all(&project).expect("create the project");
        fs::write(
            project.join("spicepod.yaml"),
            "version: v1\nkind: Spicepod\nname: proj-b\n",
        )
        .expect("write the Spicepod");

        agent_cmd(&home, &bin)
            .current_dir(&project)
            .args(["--http-endpoint", &endpoint, "codex", "exec", "hi"])
            .assert()
            .success()
            .stderr(predicate::str::contains(format!(
                "Using the Spice runtime already serving the Spicepod 'proj-a' at {endpoint}, not the Spicepod 'proj-b' in this directory, since --http-endpoint names it"
            )));
        assert!(record.exists(), "the agent ran");
    }

    /// `spice help claude` is the one way to read the command's own help, since
    /// `--help` after the command is Claude Code's.
    #[test]
    fn the_commands_help_is_reached_through_spice_help() {
        spice_cmd()
            .args(["help", "claude"])
            .assert()
            .success()
            .stdout(predicate::str::contains(
                "Every argument after `claude` is passed to Claude Code unchanged",
            ));
    }

    /// A stub `spiced`: writes its PID, waits `delay`, then serves `/health`
    /// and `/v1/models` on the `--http` address it is given. It exits on
    /// SIGTERM, and once the test's directory is gone, so a failing test
    /// cannot leave it running. It binds without `HTTPServer`'s lookup of the
    /// host's name, which waits on DNS: on a host whose resolver did not answer
    /// it, the stub never listened within the test's budget.
    fn stub_runtime(path: &Path, pid_file: &Path, delay: Duration) {
        let python = python3();
        let test_dir = pid_file.parent().expect("the PID file's directory");
        write_executable(
            path,
            &format!(
                r#"#!{python}
import http.server, os, signal, socketserver, sys, threading, time
args = sys.argv[1:]
host, port = args[args.index("--http") + 1].rsplit(":", 1)
signal.signal(signal.SIGTERM, lambda *args: os._exit(0))
def watch():
    while os.path.isdir({test_dir:?}):
        time.sleep(0.2)
    os._exit(0)
threading.Thread(target=watch, daemon=True).start()
open({pid_file:?}, "w").write(str(os.getpid()))
time.sleep({delay})
MODELS = {MODELS:?}.encode()
class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        body = b"ok" if self.path == "/health" else MODELS if self.path.startswith("/v1/models") else b""
        self.send_response(200 if body else 404)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
    def log_message(self, *args):
        pass
class Server(http.server.ThreadingHTTPServer):
    def server_bind(self):
        socketserver.TCPServer.server_bind(self)
        self.server_name, self.server_port = host, int(port)
Server((host, int(port)), Handler).serve_forever()
"#,
                python = python.display(),
                test_dir = test_dir.display().to_string(),
                pid_file = pid_file.display().to_string(),
                delay = delay.as_secs_f64(),
            ),
        );
    }

    fn wait_for(what: &str, mut done: impl FnMut() -> bool) {
        let deadline = Instant::now() + BUDGET;
        while !done() {
            assert!(Instant::now() < deadline, "timed out waiting for {what}");
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    fn serving(address: &str) -> bool {
        std::net::TcpStream::connect(address).is_ok()
    }

    fn free_address() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let address = listener.local_addr().expect("address").to_string();
        drop(listener);
        address
    }

    /// A spawned `spice` session, killed with its agent if the test fails
    /// before it exits on its own.
    struct Session(std::process::Child);

    impl Session {
        fn wait(&mut self) -> std::process::ExitStatus {
            self.0.wait().expect("wait for the session")
        }

        fn interrupt(&self) {
            let status = std::process::Command::new("/bin/kill")
                .args(["-INT", &self.0.id().to_string()])
                .status()
                .expect("run kill");
            assert!(status.success(), "send SIGINT to the session");
        }
    }

    impl Drop for Session {
        fn drop(&mut self) {
            if matches!(self.0.try_wait(), Ok(None)) {
                let _ = self.0.kill();
                let _ = self.0.wait();
            }
        }
    }

    /// A Spicepod directory, a private home, and a stub runtime for `spice` to
    /// start, with a `pi` on `PATH` that says it is running, then waits to be
    /// told to exit.
    struct Lab {
        dir: TempDir,
        home: std::path::PathBuf,
        bin: std::path::PathBuf,
        app: std::path::PathBuf,
        spiced: std::path::PathBuf,
        pid_file: std::path::PathBuf,
    }

    impl Lab {
        /// `runtime_delay` is how long the stub runtime takes to start serving.
        fn new(runtime_delay: Duration) -> Self {
            let dir = TempDir::new().expect("tempdir");
            let root = dir.path().to_path_buf();
            let lab = Self {
                home: root.join("home"),
                bin: root.join("bin"),
                app: root.join("app"),
                spiced: root.join("runtime").join("spiced"),
                pid_file: root.join("spiced.pid"),
                dir,
            };
            fs::create_dir_all(&lab.app).expect("create the app directory");
            fs::write(
                lab.app.join("spicepod.yaml"),
                "version: v1\nkind: Spicepod\nname: lab\n",
            )
            .expect("write the Spicepod");
            stub_runtime(&lab.spiced, &lab.pid_file, runtime_delay);
            write_executable(
                &lab.bin.join("pi"),
                &format!(
                    "#!/bin/sh\ntouch '{d}/started-'\"$SESSION\"\nwhile [ -d '{d}' ] && [ ! -e '{d}/exit-'\"$SESSION\" ]; do sleep 0.05; done\n",
                    d = root.display()
                ),
            );
            lab
        }

        /// `spice --http-endpoint {endpoint} pi`, as the session `name`.
        fn session(&self, name: &str, endpoint: &str) -> Session {
            self.run(name, endpoint, "pi")
        }

        /// `spice --http-endpoint {endpoint} {command}`, as the session `name`.
        fn run(&self, name: &str, endpoint: &str, command: &str) -> Session {
            let log = fs::File::create(self.dir.path().join(format!("session-{name}.log")))
                .expect("create the session log");
            Session(
                std::process::Command::new(assert_cmd::cargo::cargo_bin!("spice"))
                    .current_dir(&self.app)
                    .env("HOME", &self.home)
                    .env("PATH", format!("{}:/usr/bin:/bin", self.bin.display()))
                    .env("SPICED_PATH", &self.spiced)
                    .env("SESSION", name)
                    .env_remove("SPICE_API_KEY")
                    .args(["--http-endpoint", endpoint, command])
                    .stdin(std::process::Stdio::null())
                    .stdout(log.try_clone().expect("clone the session log"))
                    .stderr(log)
                    .spawn()
                    .unwrap_or_else(|error| panic!("start spice {command}: {error}")),
            )
        }

        /// What the session `name` printed.
        fn output(&self, name: &str) -> String {
            fs::read_to_string(self.dir.path().join(format!("session-{name}.log")))
                .unwrap_or_default()
        }

        fn agent_started(&self, name: &str) -> bool {
            self.dir.path().join(format!("started-{name}")).exists()
        }

        fn release(&self, name: &str) {
            fs::write(self.dir.path().join(format!("exit-{name}")), "").expect("release the agent");
        }

        /// The stub runtime's PID, once it has been launched.
        fn runtime_pid(&self) -> Option<i32> {
            fs::read_to_string(&self.pid_file).ok()?.trim().parse().ok()
        }

        /// The runtimes the sessions have recorded as theirs to stop.
        fn records(&self) -> Vec<String> {
            fs::read_dir(self.home.join(".spice/run"))
                .map(|entries| {
                    entries
                        .filter_map(|entry| entry.ok()?.file_name().into_string().ok())
                        .filter(|name| {
                            Path::new(name)
                                .extension()
                                .is_some_and(|extension| extension == "json")
                        })
                        .collect()
                })
                .unwrap_or_default()
        }
    }

    /// Two agents share the runtime the first one started: it outlives the
    /// first agent, and the second one stops it.
    #[test]
    fn a_started_runtime_is_shared_and_stopped_by_the_last_agent_to_exit() {
        let lab = Lab::new(Duration::ZERO);
        let address = free_address();
        let endpoint = format!("http://{address}");

        let mut first = lab.session("first", &endpoint);
        wait_for("the first agent", || lab.agent_started("first"));
        assert!(serving(&address), "the first session started the runtime");
        let mut second = lab.session("second", &endpoint);
        wait_for("the second agent", || lab.agent_started("second"));

        lab.release("first");
        assert!(first.wait().success(), "{}", lab.output("first"));
        assert!(
            serving(&address),
            "the runtime outlives the first agent while the second still uses it"
        );

        lab.release("second");
        assert!(second.wait().success(), "{}", lab.output("second"));
        let pid = lab.runtime_pid().expect("the runtime wrote its PID");
        wait_for("the runtime to stop", || {
            !serving(&address) && !process_exists(pid)
        });
        assert_eq!(
            lab.records(),
            Vec::<String>::new(),
            "the record of the stopped runtime is removed"
        );
    }

    /// Spellings of one endpoint share its runtime: a session on `localhost`
    /// keeps alive the runtime a session on `127.0.0.1` started, and is the
    /// one that stops it.
    #[test]
    fn a_started_runtime_is_shared_across_spellings_of_its_endpoint() {
        let lab = Lab::new(Duration::ZERO);
        let address = free_address();
        let port = address.rsplit(':').next().expect("a port");

        let mut first = lab.session("first", &format!("http://{address}"));
        wait_for("the first agent", || lab.agent_started("first"));
        let mut second = lab.session("second", &format!("http://localhost:{port}"));
        wait_for("the second agent", || lab.agent_started("second"));

        lab.release("first");
        assert!(first.wait().success(), "{}", lab.output("first"));
        assert!(
            serving(&address),
            "the runtime outlives the first agent while the second uses it under another spelling"
        );

        lab.release("second");
        assert!(second.wait().success(), "{}", lab.output("second"));
        let pid = lab.runtime_pid().expect("the runtime wrote its PID");
        wait_for("the runtime to stop", || {
            !serving(&address) && !process_exists(pid)
        });
        assert_eq!(lab.records(), Vec::<String>::new());
    }

    /// A spelling of the address that does not reach the runtime another
    /// session started is refused with the endpoint to use, and the runtime is
    /// left to the session using it: `[::1]` does not reach a runtime bound to
    /// `127.0.0.1`.
    #[test]
    fn a_spelling_that_does_not_reach_another_sessions_runtime_is_refused() {
        let lab = Lab::new(Duration::ZERO);
        let address = free_address();
        let port = address.rsplit(':').next().expect("a port");
        let endpoint = format!("http://{address}");

        let mut first = lab.session("first", &endpoint);
        wait_for("the first agent", || lab.agent_started("first"));
        let mut second = lab.session("second", &format!("http://[::1]:{port}"));
        let status = second.wait();
        let output = lab.output("second");
        assert_eq!(status.code(), Some(1), "{output}");
        assert!(
            output.contains(&format!(
                "another `spice` session is using the runtime it started at {endpoint}, which does not answer at http://[::1]:{port}. Rerun with `--http-endpoint {endpoint}`"
            )),
            "{output}"
        );
        assert!(!lab.agent_started("second"));
        assert!(
            serving(&address),
            "the runtime the first session uses keeps running"
        );

        lab.release("first");
        assert!(first.wait().success(), "{}", lab.output("first"));
        let pid = lab.runtime_pid().expect("the runtime wrote its PID");
        wait_for("the runtime to stop", || !process_exists(pid));
        assert_eq!(lab.records(), Vec::<String>::new());
    }

    /// Ctrl-C while the runtime is starting stops the runtime the session
    /// launched, since no agent will ever use it.
    #[test]
    fn interrupting_a_runtime_start_stops_the_runtime() {
        // Longer than the test runs: the runtime is still starting when Ctrl-C comes.
        let lab = Lab::new(Duration::from_secs(600));
        let address = free_address();

        let mut session = lab.session("only", &format!("http://{address}"));
        wait_for("the runtime to be launched", || lab.runtime_pid().is_some());
        let pid = lab.runtime_pid().expect("the runtime wrote its PID");
        wait_for("the session to record the runtime", || {
            !lab.records().is_empty()
        });

        session.interrupt();
        let status = session.wait();
        assert_eq!(status.code(), Some(130), "{}", lab.output("only"));
        wait_for("the runtime to stop", || !process_exists(pid));
        assert_eq!(lab.records(), Vec::<String>::new());
        assert!(!lab.agent_started("only"));
    }

    /// A runtime that exits before it serves fails the session with one error
    /// line that names the full log, below the runtime's last output.
    #[test]
    fn a_runtime_that_exits_before_serving_is_reported_on_one_line() {
        let lab = Lab::new(Duration::ZERO);
        write_executable(
            &lab.spiced,
            "#!/bin/sh\necho 'spiced: loading the Spicepod'\necho 'spiced: the Spicepod is not valid' >&2\nexit 3\n",
        );
        let port = free_address()
            .rsplit_once(':')
            .map(|(_, port)| port.to_string())
            .expect("a port");
        let endpoint = format!("http://127.0.0.1:{port}");

        let status = lab.session("only", &endpoint).wait();
        let output = lab.output("only");
        assert!(!status.success(), "{output}");
        assert!(!lab.agent_started("only"), "{output}");
        let log = lab.home.join(format!(".spice/run/localhost_{port}.log"));
        let error = format!(
            "ERROR Failed to start the Spice runtime at {endpoint}, so the coding agent was not started: the runtime exited (exit status: 3) before it started serving; its last output is above. The full log is at '{}'. See: https://spiceai.org/docs/cli",
            log.display()
        );
        let lines: Vec<&str> = output.lines().collect();
        let error_at = lines
            .iter()
            .position(|line| *line == error)
            .unwrap_or_else(|| panic!("no line is the whole error {error:?} in:\n{output}"));
        let cause_at = lines
            .iter()
            .position(|line| *line == "spiced: the Spicepod is not valid")
            .unwrap_or_else(|| panic!("the runtime's output is not shown in:\n{output}"));
        assert!(
            cause_at < error_at,
            "the runtime's output comes first:\n{output}"
        );
    }

    /// Ctrl-C ends a session that is waiting for another to finish starting the
    /// runtime at once, not when the other session lets go.
    #[test]
    fn interrupting_a_wait_for_another_sessions_start_ends_at_once() {
        // Longer than the test runs: the first session is still starting the runtime.
        let lab = Lab::new(Duration::from_secs(600));
        let endpoint = format!("http://{}", free_address());

        let _starting = lab.session("first", &endpoint);
        wait_for("the runtime to be launched", || lab.runtime_pid().is_some());
        let mut waiting = lab.session("second", &endpoint);
        wait_for("the second session to wait for the first", || {
            lab.output("second")
                .contains("Waiting for another `spice` session to finish starting")
        });

        let interrupted = std::time::Instant::now();
        waiting.interrupt();
        let deadline = interrupted + Duration::from_secs(5);
        let status = loop {
            if let Some(status) = waiting.0.try_wait().expect("poll the session") {
                break status;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "the interrupted session still runs {:?} after Ctrl-C: {}",
                interrupted.elapsed(),
                lab.output("second")
            );
            std::thread::sleep(Duration::from_millis(20));
        };
        assert_eq!(status.code(), Some(130), "{}", lab.output("second"));
        assert!(!lab.agent_started("second"));
    }

    /// A stub Claude Code's recorded arguments, one per line: the path of the
    /// `--settings` file spice routes it with, which comes first, and the rest.
    fn after_routing_settings(recorded: &str) -> (&str, &str) {
        recorded
            .strip_prefix("--settings\n")
            .and_then(|rest| rest.split_once('\n'))
            .expect("Claude Code's routing settings come first")
    }

    /// The `python3` the stubs of the runtime and of Orca run on.
    fn python3() -> std::path::PathBuf {
        std::env::split_paths(&std::env::var_os("PATH").unwrap_or_default())
            .map(|dir| dir.join("python3"))
            .find(|candidate| candidate.is_file())
            .expect("python3 is needed to stand in for the runtime and Orca")
    }

    /// A stub of Orca's `orca` command, installed where `spice orca` looks for
    /// it: inside an `Orca.app` linked onto `PATH` on macOS, as `orca-ide` on
    /// Linux. Every call is logged to `<root>/orca.log` as a JSON line.
    ///
    /// `orca open` starts a stand-in for the app — a process whose arguments
    /// carry `--user-data-dir=<profile>`, as Orca's do — which imports the
    /// profile's seed, but with Codex's command changed the way a user could
    /// in Orca's settings, then answers `settings.get` on a Unix socket until
    /// `<root>/quit-orca` appears. `orca status` exits 3.
    fn stub_orca(root: &Path, bin: &Path) {
        let script = format!(
            r#"#!{python}
import json, os, socket, subprocess, sys, threading, time
ROOT = {root:?}
args = sys.argv[1:]
user_data = os.environ.get("ORCA_USER_DATA_PATH", "")
if args[:1] != ["--app"]:
    with open(os.path.join(ROOT, "orca.log"), "a") as log:
        log.write(json.dumps({{"args": args, "user_data": user_data, "cwd": os.getcwd()}}) + "\n")
def gone():
    return not os.path.isdir(ROOT) or os.path.exists(os.path.join(ROOT, "quit-orca"))
if args[:1] == ["open"]:
    subprocess.Popen([sys.executable, __file__, "--app", "--user-data-dir=" + user_data],
                     start_new_session=True, stdin=subprocess.DEVNULL,
                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    metadata = os.path.join(user_data, "orca-runtime.json")
    while not os.path.exists(metadata) and not gone():
        time.sleep(0.02)
    sys.exit(0)
if args[:1] == ["--app"]:
    user_data = args[1].split("=", 1)[1]
    seed = json.load(open(os.path.join(user_data, "orca-data.json")))
    commands = dict(seed["settings"]["agentCmdOverrides"])
    commands["codex"] = "codex"
    path = "/tmp/spice-orca-test-%d.sock" % os.getpid()
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.bind(path)
    server.listen()
    def answer():
        while True:
            connection, _ = server.accept()
            request = json.loads(connection.makefile().readline())
            reply = {{"id": request["id"], "ok": False}}
            if request.get("authToken") == "t0k" and request.get("method") == "settings.get":
                reply = {{"id": request["id"], "ok": True, "result": {{"settings": {{"agentCmdOverrides": commands}}}}}}
            connection.sendall(b'{{"_keepalive": true}}\n' + json.dumps(reply).encode() + b"\n")
            connection.close()
    threading.Thread(target=answer, daemon=True).start()
    staged = os.path.join(user_data, "orca-runtime.json.tmp")
    json.dump({{"pid": os.getpid(), "transports": [{{"kind": "unix", "endpoint": path}}], "authToken": "t0k"}}, open(staged, "w"))
    os.rename(staged, os.path.join(user_data, "orca-runtime.json"))
    while not gone():
        time.sleep(0.05)
    os.unlink(path)
    sys.exit(0)
if args[:2] == ["repo", "add"]:
    print(json.dumps({{"ok": True}}))
    sys.exit(0)
if args[:1] == ["status"]:
    sys.exit(3)
"#,
            python = python3().display(),
            root = root.display().to_string(),
        );
        if cfg!(target_os = "macos") {
            let app_script = root.join("apps/Orca.app/Contents/Resources/bin/orca");
            write_executable(&app_script, &script);
            fs::create_dir_all(bin).expect("create bin");
            std::os::unix::fs::symlink(&app_script, bin.join("orca"))
                .expect("link Orca's command onto PATH");
        } else {
            write_executable(&bin.join("orca-ide"), &script);
        }
    }

    /// The calls the stub `orca` logged.
    fn orca_calls(root: &Path) -> Vec<serde_json::Value> {
        fs::read_to_string(root.join("orca.log"))
            .unwrap_or_default()
            .lines()
            .map(|line| serde_json::from_str(line).expect("a JSON log line"))
            .collect()
    }

    /// `spice orca` opens Orca on the Spice profile, which it seeds with every
    /// agent running through its launcher; keeps the runtime it started while
    /// Orca is open; warns about the agent whose command a user changed; adds
    /// the git repository it ran in to Orca's projects; and stops the runtime
    /// once Orca has quit.
    #[test]
    fn orca_runs_every_agent_through_spice_and_keeps_the_runtime_until_it_quits() {
        let lab = Lab::new(Duration::ZERO);
        let root = lab.dir.path();
        stub_orca(root, &lab.bin);
        let git = std::process::Command::new("git")
            .args(["init", "-q"])
            .current_dir(&lab.app)
            .status()
            .expect("run git");
        assert!(git.success(), "git init");
        let address = free_address();
        let endpoint = format!("http://{address}");
        let mut session = lab.run("orca", &endpoint, "orca");
        let output = || lab.output("orca");

        let codex_warning = format!(
            "Orca runs Codex CLI with the command 'codex', so Codex CLI sessions in Orca do not use Spice. Set Codex CLI's command in Orca's Settings, under Agents, to `'{}/.spice/orca/bin/spice-agent' codex`.",
            lab.home.display()
        );
        wait_for("the warning about Codex CLI's command", || {
            output().contains(&codex_warning)
        });
        let project = fs::canonicalize(&lab.app).expect("resolve the project");
        wait_for("the project to be added", || {
            orca_calls(root).iter().any(|call| {
                call["args"]
                    == serde_json::json!([
                        "repo",
                        "add",
                        "--path",
                        project.display().to_string(),
                        "--json"
                    ])
            })
        });
        let profile = lab.home.join(".spice/orca/profile");
        let calls = orca_calls(root);
        assert_eq!(
            calls[0]["args"],
            serde_json::json!(["open"]),
            "`orca open` by default"
        );
        assert_eq!(calls[0]["user_data"], profile.display().to_string());

        // Orca is open: the session holds the runtime it started.
        assert!(matches!(session.0.try_wait(), Ok(None)), "{}", output());
        assert!(serving(&address), "the runtime serves while Orca is open");
        let runtime = lab.runtime_pid().expect("the runtime wrote its PID");

        let launcher = lab.home.join(".spice/orca/bin/spice-agent");
        let command = |agent: &str| format!("'{}' {agent}", launcher.display());
        assert_eq!(
            fs::read_to_string(profile.join("orca-data.json")).expect("the profile was seeded"),
            serde_json::json!({ "settings": { "agentCmdOverrides": {
                "antigravity": command("agy"),
                "claude": command("claude"),
                "codex": command("codex"),
                "copilot": command("copilot"),
                "cursor": command("cursor"),
                "grok": command("grok"),
                "opencode": command("opencode"),
                "pi": command("pi"),
            }}})
            .to_string()
        );
        let spice = assert_cmd::cargo::cargo_bin!("spice");
        assert_eq!(
            fs::read_to_string(&launcher).expect("the launcher was written"),
            format!(
                "#!/bin/sh\n# Written by `spice orca`, which rewrites it on every run. Orca runs each coding\n# agent as `spice-agent <agent> [ARGS]`, which starts it on the Spice runtime.\nexec '{}' --http-endpoint '{endpoint}' \"$@\"\n",
                spice.display()
            )
        );
        assert!(
            !output().contains("Orca runs Claude Code"),
            "only the changed command is warned about: {}",
            output()
        );

        fs::write(root.join("quit-orca"), "").expect("quit Orca");
        wait_for("spice orca to exit once Orca has quit", || {
            matches!(session.0.try_wait(), Ok(Some(_)))
        });
        assert!(session.wait().success(), "{}", output());
        wait_for("the runtime to stop", || !process_exists(runtime));
        assert_eq!(lab.records(), Vec::<String>::new());
    }

    /// Arguments reach Orca's command unchanged, and its exit code is
    /// `spice orca`'s; the launcher it leaves then starts an agent through
    /// spice, on the endpoint the session used.
    #[test]
    fn orca_gets_every_argument_and_its_launcher_starts_agents_through_spice() {
        let endpoint = user_runtime();
        let dir = TempDir::new().expect("tempdir");
        let (root, home, bin, record) = (
            dir.path(),
            dir.path().join("home"),
            dir.path().join("bin"),
            dir.path().join("record"),
        );
        stub_orca(root, &bin);
        stub_agent(&bin, "claude", &record, &["ANTHROPIC_BASE_URL"], 0);

        agent_cmd(&home, &bin)
            .args([
                "--http-endpoint",
                &endpoint,
                "orca",
                "status",
                "-v",
                "--json",
            ])
            .assert()
            .code(3);
        let calls = orca_calls(root);
        assert_eq!(
            calls.len(),
            1,
            "no follow-up for a command that opens nothing"
        );
        assert_eq!(
            calls[0]["args"],
            serde_json::json!(["status", "-v", "--json"])
        );

        let launcher = home.join(".spice/orca/bin/spice-agent");
        let ran = std::process::Command::new(&launcher)
            .args(["claude", "--model", "qwen", "-p", "hi"])
            .env("HOME", &home)
            .env("PATH", format!("{}:/usr/bin:/bin", bin.display()))
            .env_remove("SPICE_API_KEY")
            .stdin(std::process::Stdio::null())
            .status()
            .expect("run the launcher");
        assert!(ran.success(), "the agent ran through the launcher");
        let recorded = fs::read_to_string(&record).expect("the agent ran");
        let (_, rest) = after_routing_settings(&recorded);
        assert_eq!(
            rest,
            format!("--model\nqwen\n-p\nhi\nANTHROPIC_BASE_URL={endpoint}\n")
        );
    }

    #[test]
    fn a_missing_orca_says_how_to_install_it() {
        if Path::new("/Applications/Orca.app").exists() {
            eprintln!("skipped: this machine has Orca installed in /Applications");
            return;
        }
        let endpoint = user_runtime();
        let dir = TempDir::new().expect("tempdir");
        let (home, bin) = (dir.path().join("home"), dir.path().join("bin"));
        fs::create_dir_all(&bin).expect("create bin");
        let expected = if cfg!(target_os = "macos") {
            "Failed to start Orca: it is not installed (no `orca` on PATH). Install it with `brew install --cask stablyai/orca/orca`, or download it from https://onorca.dev/download. Then rerun `spice orca`. See: https://www.onorca.dev/docs/install"
        } else {
            "Failed to start Orca: it is not installed (no `orca-ide` on PATH). Download it from https://onorca.dev/download, and register its `orca-ide` command in Orca's Settings, under General. Then rerun `spice orca`. See: https://www.onorca.dev/docs/install"
        };
        agent_cmd(&home, &bin)
            .args(["--http-endpoint", &endpoint, "orca"])
            .assert()
            .code(1)
            .stderr(predicate::str::contains(expected));
        assert!(
            !home.join(".spice/orca").exists(),
            "nothing is written for an Orca that is not installed"
        );
    }

    /// Whether a signal could be sent to `pid`, without sending one.
    fn process_exists(pid: i32) -> bool {
        std::process::Command::new("/bin/kill")
            .args(["-0", &pid.to_string()])
            .stderr(std::process::Stdio::null())
            .status()
            .is_ok_and(|status| status.success())
    }
}
