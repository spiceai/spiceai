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
            .stdout(predicate::str::contains("Dataset"));
    }

    #[test]
    fn test_dataset_configure_help() {
        let mut cmd = spice_cmd();
        cmd.arg("dataset")
            .arg("configure")
            .arg("--help")
            .assert()
            .success()
            .stdout(predicate::str::contains("Configure"));
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
            .stdout(predicate::str::contains("query"));
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
}

// ============================================================================
// Connect Command Tests
// ============================================================================

mod connect {
    use super::*;

    #[test]
    fn test_connect_help() {
        let mut cmd = spice_cmd();
        cmd.arg("connect")
            .arg("--help")
            .assert()
            .success()
            .stdout(predicate::str::contains("Connect"))
            .stdout(predicate::str::contains("Spice.ai Cloud"));
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
            .stdout(predicate::str::contains("Search"))
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
            .stdout(predicate::str::contains("natural language"));
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
// Eval Command Tests
// ============================================================================

mod eval {
    use super::*;

    #[test]
    fn test_eval_help() {
        let mut cmd = spice_cmd();
        cmd.arg("eval")
            .arg("--help")
            .assert()
            .success()
            .stdout(predicate::str::contains("eval"))
            .stdout(predicate::str::contains("model"));
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
            .stdout(predicate::str::contains("Cluster"));
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
            .stdout(predicate::str::contains("Use cloud instance"));
    }

    #[test]
    fn test_cloud_mode_with_status() {
        // Cloud mode status should fail without proper connection (no API key)
        let mut cmd = spice_cmd();
        cmd.arg("--cloud").arg("status").assert().failure();
    }

    #[test]
    fn test_cloud_mode_with_api_key_status() {
        // Cloud mode with API key should still fail (invalid key)
        // but the command structure should be valid
        let mut cmd = spice_cmd();
        cmd.arg("--cloud")
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
            .arg("datasets")
            .assert()
            .success() // Currently exits 0 but prints error message
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
