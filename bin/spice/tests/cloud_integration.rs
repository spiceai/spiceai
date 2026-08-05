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

//! Live CLI integration tests for `spice cloud` subcommands against the Spice
//! Cloud dev API.
//!
//! # Running
//!
//! Tests skip automatically when `SPICE_SPICEAI_TOKEN` is not set.
//!
//! ```bash
//! SPICE_CLOUD_API_URL="https://dev-api.spice.ai" \
//! SPICE_SPICEAI_TOKEN="<your-dev-token>" \
//!   cargo test -p spice --test cloud_integration -- --test-threads=1
//! ```
//!
//! `--test-threads=1` is recommended because tests create/delete apps against
//! the shared dev API and may hit rate limits.

use assert_cmd::Command;
use assert_cmd::cargo::cargo_bin_cmd;
use predicates::prelude::*;

const DEFAULT_DEV_API: &str = "https://dev-api.spice.ai";

/// Build a `Command` for the `spice` binary with auth env vars injected.
/// Returns `None` when `SPICE_SPICEAI_TOKEN` is not set.
fn spice_cloud_cmd() -> Option<Command> {
    let token = std::env::var("SPICE_SPICEAI_TOKEN").ok()?;
    if token.is_empty() {
        return None;
    }
    let base_url =
        std::env::var("SPICE_CLOUD_API_URL").unwrap_or_else(|_| DEFAULT_DEV_API.to_string());

    let mut cmd = cargo_bin_cmd!("spice");
    cmd.env("SPICE_SPICEAI_TOKEN", &token)
        .env("SPICE_CLOUD_API_URL", &base_url);
    Some(cmd)
}

/// Convenience macro: skip the test when credentials are absent.
macro_rules! require_cmd {
    () => {
        match spice_cloud_cmd() {
            Some(c) => c,
            None => {
                eprintln!("SPICE_SPICEAI_TOKEN not set — skipping");
                return;
            }
        }
    };
}

/// Generate a unique app name for test isolation.
fn test_app_name() -> String {
    let short_id = &uuid::Uuid::new_v4().to_string()[..8];
    format!("ci-test-{short_id}")
}

/// Resolve the org name from `spice cloud whoami -o json`.
fn get_org_name() -> Option<String> {
    let output = spice_cloud_cmd()?
        .args(["cloud", "whoami", "-o", "json"])
        .output()
        .ok()?;
    let ctx: serde_json::Value = serde_json::from_slice(&output.stdout).ok()?;
    ctx.get("org_name")?.as_str().map(String::from)
}

/// Helper: create an app and return its org-qualified name (`org/app`).
/// Panics on failure — intended for test setup.
fn create_test_app(name: &str) -> String {
    let org = get_org_name().expect("should resolve org name from whoami");
    let mut cmd = spice_cloud_cmd().expect("credentials required");
    cmd.args(["cloud", "create", "app", name, "-o", "json"])
        .assert()
        .success();
    format!("{org}/{name}")
}

/// Best-effort cleanup: delete an app by org/name, ignoring errors.
fn cleanup_app(org_app: &str) {
    if let Some(mut cmd) = spice_cloud_cmd() {
        let _ = cmd
            .args(["cloud", "delete", "app", org_app, "--yes", "-o", "json"])
            .output();
    }
}

/// Common insta settings: redact dynamic fields so snapshots are stable.
fn insta_settings() -> insta::Settings {
    let mut settings = insta::Settings::clone_current();
    // Numeric IDs (e.g., "id": 123 -> "id": 0)
    settings.add_filter(r#""id": \d+"#, r#""id": 0"#);
    // ISO-8601 timestamps
    settings.add_filter(
        r#""\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[^"]*""#,
        r#""<timestamp>""#,
    );
    // API keys / tokens (20+ alphanum chars)
    settings.add_filter(r#""[A-Za-z0-9_\-]{20,}""#, r#""<redacted>""#);
    // Random UUID suffix in test app names
    settings.add_filter(r#""ci-test-[0-9a-f]{8}""#, r#""ci-test-<uuid>""#);
    // org name
    settings.add_filter(r#""org": "[^"]+""#, r#""org": "<org>""#);
    settings.add_filter(r#""org_name": "[^"]+""#, r#""org_name": "<redacted>""#);
    // username / email
    settings.add_filter(r#""username": "[^"]+""#, r#""username": "<redacted>""#);
    settings.add_filter(r#""email": "[^"]+""#, r#""email": "<redacted>""#);
    // Container image references
    settings.add_filter(r#""image": "[^"]+""#, r#""image": "<image>""#);
    settings.add_filter(r#""image_tag": "[^"]+""#, r#""image_tag": "<image_tag>""#);
    settings
}

// ============================================================================
// Auth — whoami
// ============================================================================

#[test]
fn test_cloud_whoami_json() {
    let mut cmd = require_cmd!();
    let assert = cmd
        .args(["cloud", "whoami", "-o", "json"])
        .assert()
        .success();

    let output = assert.get_output().stdout.clone();
    let ctx: serde_json::Value =
        serde_json::from_slice(&output).expect("whoami should produce valid JSON");

    insta_settings().bind(|| {
        insta::assert_json_snapshot!(ctx, @r#"
        {
          "app_api_key": null,
          "app_name": null,
          "email": "jack@spice.ai",
          "org_name": "spicehq",
          "username": "jeadie-4"
        }
        "#);
    });
}

#[test]
fn test_cloud_whoami_table() {
    let mut cmd = require_cmd!();
    cmd.args(["cloud", "whoami"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Logged in as:"))
        .stdout(predicate::str::contains("Organization:"));
}

#[test]
fn test_cloud_whoami_unauthorized() {
    let base_url =
        std::env::var("SPICE_CLOUD_API_URL").unwrap_or_else(|_| DEFAULT_DEV_API.to_string());
    if std::env::var("SPICE_SPICEAI_TOKEN").is_err() {
        eprintln!("SPICE_SPICEAI_TOKEN not set — skipping");
        return;
    }

    let mut cmd = cargo_bin_cmd!("spice");
    cmd.env("SPICE_SPICEAI_TOKEN", "invalid-token-that-should-not-work")
        .env("SPICE_CLOUD_API_URL", &base_url)
        .args(["cloud", "whoami"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("Unauthorized").or(predicate::str::contains("401")));
}

// ============================================================================
// Apps — full CRUD lifecycle
// ============================================================================

#[test]
fn test_cloud_app_crud_lifecycle() {
    let _ = require_cmd!();
    let name = test_app_name();
    let org = get_org_name().expect("should resolve org name");
    let org_app = format!("{org}/{name}");

    // --- Create ---
    let mut cmd = spice_cloud_cmd().expect("credentials required");
    let create_assert = cmd
        .args([
            "cloud",
            "create",
            "app",
            &name,
            "--description",
            "Integration test app",
            "-o",
            "json",
        ])
        .assert()
        .success();

    let create_output: serde_json::Value =
        serde_json::from_slice(&create_assert.get_output().stdout)
            .expect("create app should produce valid JSON");
    insta_settings().bind(|| {
        insta::assert_json_snapshot!(&create_output, @"");
    });

    // --- Get ---
    let mut cmd = spice_cloud_cmd().expect("credentials required");
    let get_assert = cmd
        .args(["cloud", "get", "app", &org_app, "-o", "json"])
        .assert()
        .success();

    let get_output: serde_json::Value = serde_json::from_slice(&get_assert.get_output().stdout)
        .expect("get app should produce valid JSON");
    insta_settings().bind(|| {
        insta::assert_json_snapshot!(&get_output, @"");
    });

    // --- Apps list (must contain our app) ---
    let mut cmd = spice_cloud_cmd().expect("credentials required");
    let list_assert = cmd.args(["cloud", "apps", "-o", "json"]).assert().success();

    let list_output: serde_json::Value = serde_json::from_slice(&list_assert.get_output().stdout)
        .expect("apps list should produce valid JSON");
    let apps = list_output.as_array().expect("apps should be an array");
    assert!(
        apps.iter()
            .any(|a| a.get("name").and_then(|v| v.as_str()) == Some(name.as_str())),
        "apps list should contain the created app"
    );

    // --- Update ---
    let mut cmd = spice_cloud_cmd().expect("credentials required");
    let update_assert = cmd
        .args([
            "cloud",
            "update",
            "app",
            "--app",
            &org_app,
            "--description",
            "Updated description",
            "-o",
            "json",
        ])
        .assert()
        .success();

    let update_output: serde_json::Value =
        serde_json::from_slice(&update_assert.get_output().stdout)
            .expect("update app should produce valid JSON");
    insta_settings().bind(|| {
        insta::assert_json_snapshot!(&update_output, @"");
    });

    // --- Delete ---
    let mut cmd = spice_cloud_cmd().expect("credentials required");
    cmd.args(["cloud", "delete", "app", &org_app, "--yes", "-o", "json"])
        .assert()
        .success();

    // --- Confirm deleted (get should fail) ---
    let mut cmd = spice_cloud_cmd().expect("credentials required");
    cmd.args(["cloud", "get", "app", &org_app, "-o", "json"])
        .assert()
        .failure();
}

#[test]
fn test_cloud_create_app_duplicate_name_conflict() {
    let _ = require_cmd!();
    let name = test_app_name();
    let org = get_org_name().expect("should resolve org name");
    let org_app = format!("{org}/{name}");

    // First create succeeds
    let mut cmd = spice_cloud_cmd().expect("credentials required");
    cmd.args(["cloud", "create", "app", &name, "-o", "json"])
        .assert()
        .success();

    // Second create with same name should fail
    let mut cmd = spice_cloud_cmd().expect("credentials required");
    cmd.args(["cloud", "create", "app", &name, "-o", "json"])
        .assert()
        .failure();

    cleanup_app(&org_app);
}

#[test]
fn test_cloud_get_app_not_found() {
    let _ = require_cmd!();
    let org = get_org_name().expect("should resolve org name");
    let mut cmd = spice_cloud_cmd().expect("credentials required");
    cmd.args([
        "cloud",
        "get",
        "app",
        &format!("{org}/nonexistent-app-99999"),
        "-o",
        "json",
    ])
    .assert()
    .failure()
    .stderr(predicate::str::contains("not found").or(predicate::str::contains("Not found")));
}

#[test]
fn test_cloud_delete_app_not_found() {
    let _ = require_cmd!();
    let org = get_org_name().expect("should resolve org name");
    let mut cmd = spice_cloud_cmd().expect("credentials required");
    cmd.args([
        "cloud",
        "delete",
        "app",
        &format!("{org}/nonexistent-app-99999"),
        "--yes",
        "-o",
        "json",
    ])
    .assert()
    .failure();
}

// ============================================================================
// Secrets — full CRUD lifecycle
// ============================================================================

#[test]
fn test_cloud_secrets_crud_lifecycle() {
    let _ = require_cmd!();
    let name = test_app_name();
    let org_app = create_test_app(&name);

    // --- Set secret ---
    let mut cmd = spice_cloud_cmd().expect("credentials required");
    cmd.args([
        "cloud",
        "secrets",
        "set",
        "--app",
        &org_app,
        "TEST_SECRET",
        "s3cret_value",
        "-o",
        "json",
    ])
    .assert()
    .success();

    // --- Get secret ---
    let mut cmd = spice_cloud_cmd().expect("credentials required");
    cmd.args([
        "cloud",
        "secrets",
        "get",
        "--app",
        &org_app,
        "TEST_SECRET",
        "-o",
        "json",
    ])
    .assert()
    .success();

    let get_assert = spice_cloud_cmd()
        .expect("credentials required")
        .args([
            "cloud",
            "secrets",
            "get",
            "--app",
            &org_app,
            "TEST_SECRET",
            "-o",
            "json",
        ])
        .assert()
        .success();
    let secret: serde_json::Value = serde_json::from_slice(&get_assert.get_output().stdout)
        .expect("get secret should produce valid JSON");
    insta_settings().bind(|| {
        insta::assert_json_snapshot!(&secret, @"");
    });

    // --- List secrets ---
    let mut cmd = spice_cloud_cmd().expect("credentials required");
    let list_assert = cmd
        .args(["cloud", "secrets", "list", "--app", &org_app, "-o", "json"])
        .assert()
        .success();
    let list_output: serde_json::Value = serde_json::from_slice(&list_assert.get_output().stdout)
        .expect("list secrets should produce valid JSON");
    let listed_entries = list_output
        .as_array()
        .expect("listed entries should be an array");
    assert!(
        listed_entries
            .iter()
            .any(|s| s.get("name").and_then(|v| v.as_str()) == Some("TEST_SECRET")),
        "listed entries should include TEST_SECRET"
    );

    // --- Overwrite secret ---
    let mut cmd = spice_cloud_cmd().expect("credentials required");
    cmd.args([
        "cloud",
        "secrets",
        "set",
        "--app",
        &org_app,
        "TEST_SECRET",
        "new_value",
        "-o",
        "json",
    ])
    .assert()
    .success();

    // --- Delete secret ---
    let mut cmd = spice_cloud_cmd().expect("credentials required");
    cmd.args([
        "cloud",
        "secrets",
        "delete",
        "--app",
        &org_app,
        "TEST_SECRET",
        "-o",
        "json",
    ])
    .assert()
    .success();

    // --- Confirm deleted ---
    let mut cmd = spice_cloud_cmd().expect("credentials required");
    cmd.args([
        "cloud",
        "secrets",
        "get",
        "--app",
        &org_app,
        "TEST_SECRET",
        "-o",
        "json",
    ])
    .assert()
    .failure();

    cleanup_app(&org_app);
}

#[test]
fn test_cloud_get_secret_not_found() {
    let _ = require_cmd!();
    let name = test_app_name();
    let org_app = create_test_app(&name);

    let mut cmd = spice_cloud_cmd().expect("credentials required");
    cmd.args([
        "cloud",
        "secrets",
        "get",
        "--app",
        &org_app,
        "DOES_NOT_EXIST",
        "-o",
        "json",
    ])
    .assert()
    .failure();

    cleanup_app(&org_app);
}

#[test]
fn test_cloud_multiple_secrets() {
    let _ = require_cmd!();
    let name = test_app_name();
    let org_app = create_test_app(&name);

    for i in 0..3 {
        let mut cmd = spice_cloud_cmd().expect("credentials required");
        cmd.args([
            "cloud",
            "secrets",
            "set",
            "--app",
            &org_app,
            &format!("KEY_{i}"),
            &format!("val_{i}"),
            "-o",
            "json",
        ])
        .assert()
        .success();
    }

    let mut cmd = spice_cloud_cmd().expect("credentials required");
    let list_assert = cmd
        .args(["cloud", "secrets", "list", "--app", &org_app, "-o", "json"])
        .assert()
        .success();
    let list_output: serde_json::Value = serde_json::from_slice(&list_assert.get_output().stdout)
        .expect("list secrets should produce valid JSON");
    let entries_count = list_output
        .as_array()
        .expect("listed items should be an array")
        .len();
    assert!(entries_count >= 3, "should have at least 3 entries");

    for i in 0..3 {
        let mut cmd = spice_cloud_cmd().expect("credentials required");
        cmd.args([
            "cloud",
            "secrets",
            "delete",
            "--app",
            &org_app,
            &format!("KEY_{i}"),
            "-o",
            "json",
        ])
        .assert()
        .success();
    }

    let mut cmd = spice_cloud_cmd().expect("credentials required");
    let after_assert = cmd
        .args(["cloud", "secrets", "list", "--app", &org_app, "-o", "json"])
        .assert()
        .success();
    let after_output: serde_json::Value = serde_json::from_slice(&after_assert.get_output().stdout)
        .expect("list secrets should produce valid JSON");
    let remaining_entries = after_output
        .as_array()
        .expect("remaining entries should be an array");
    assert!(
        !remaining_entries.iter().any(|s| s
            .get("name")
            .and_then(|v| v.as_str())
            .is_some_and(|n| n.starts_with("KEY_"))),
        "all KEY_* entries should be deleted"
    );

    cleanup_app(&org_app);
}

// ============================================================================
// API Keys
// ============================================================================

#[test]
fn test_cloud_api_keys_get_and_regenerate() {
    let _ = require_cmd!();
    let name = test_app_name();
    let org_app = create_test_app(&name);
    let settings = insta_settings();

    // --- Get keys ---
    let mut cmd = spice_cloud_cmd().expect("credentials required");
    let keys_assert = cmd
        .args(["cloud", "api-keys", "--app", &org_app, "-o", "json"])
        .assert()
        .success();

    let keys: serde_json::Value = serde_json::from_slice(&keys_assert.get_output().stdout)
        .expect("api-keys should produce valid JSON");
    let original_key1 = keys
        .get("api_key")
        .and_then(|v| v.as_str())
        .map(String::from);

    settings.bind(|| {
        insta::assert_json_snapshot!(&keys, @r#"
        {
          "api_key": "<redacted>",
          "api_key_2": "<redacted>"
        }
        "#);
    });

    // --- Regenerate key 1 ---
    let mut cmd = spice_cloud_cmd().expect("credentials required");
    let regen_assert = cmd
        .args([
            "cloud",
            "api-keys",
            "--app",
            &org_app,
            "--regenerate",
            "1",
            "-o",
            "json",
        ])
        .assert()
        .success();

    let regen: serde_json::Value = serde_json::from_slice(&regen_assert.get_output().stdout)
        .expect("regenerate should produce valid JSON");
    assert_eq!(
        regen
            .get("regenerated_key")
            .and_then(serde_json::Value::as_u64),
        Some(1),
        "regenerated_key should be 1"
    );
    let new_key1 = regen
        .get("api_key")
        .and_then(|v| v.as_str())
        .map(String::from);
    if let (Some(orig), Some(new_k)) = (&original_key1, &new_key1) {
        assert_ne!(orig, new_k, "regenerated key 1 should differ from original");
    }

    settings.bind(|| {
        insta::assert_json_snapshot!(&regen, @r#"
        {
          "api_key": "<redacted>",
          "api_key_2": "<redacted>",
          "regenerated_key": 1
        }
        "#);
    });

    cleanup_app(&org_app);
}

// ============================================================================
// Deployments
// ============================================================================

#[test]
fn test_cloud_deployments_list_empty() {
    let _ = require_cmd!();
    let name = test_app_name();
    let org_app = create_test_app(&name);

    let mut cmd = spice_cloud_cmd().expect("credentials required");
    let list_assert = cmd
        .args(["cloud", "deployments", "--app", &org_app, "-o", "json"])
        .assert()
        .success();

    let list_output: serde_json::Value = serde_json::from_slice(&list_assert.get_output().stdout)
        .expect("list deployments should produce valid JSON");
    let deployments = list_output
        .as_array()
        .expect("deployments should be an array");
    assert!(
        deployments.is_empty(),
        "newly created app should have no deployments"
    );

    cleanup_app(&org_app);
}

#[test]
fn test_cloud_create_deployment() {
    let _ = require_cmd!();
    let name = test_app_name();
    let org_app = create_test_app(&name);
    let settings = insta_settings();

    let mut cmd = spice_cloud_cmd().expect("credentials required");
    let dep_assert = cmd
        .args([
            "cloud",
            "create",
            "deployment",
            "--app",
            &org_app,
            "--replicas",
            "1",
            "-o",
            "json",
        ])
        .assert()
        .success();

    let deployment: serde_json::Value = serde_json::from_slice(&dep_assert.get_output().stdout)
        .expect("create deployment should produce valid JSON");

    settings.bind(|| {
        insta::assert_json_snapshot!(&deployment, @r#"
        {
          "id": "<redacted>",
          "status": "pending",
          "created_at": "<timestamp>",
          "updated_at": "<timestamp>",
          "started_at": null,
          "finished_at": null,
          "image": "<image>",
          "image_tag": "<image_tag>",
          "replicas": 1,
          "branch": null,
          "commit_sha": null,
          "commit_message": null,
          "error_message": null,
          "creation_source": null,
          "created_by": null
        }
        "#);
    });

    // Verify it appears in list
    let mut cmd = spice_cloud_cmd().expect("credentials required");
    let list_assert = cmd
        .args(["cloud", "deployments", "--app", &org_app, "-o", "json"])
        .assert()
        .success();
    let list_output: serde_json::Value = serde_json::from_slice(&list_assert.get_output().stdout)
        .expect("list deployments should produce valid JSON");
    let deps = list_output
        .as_array()
        .expect("deployments should be an array");
    let dep_id = deployment.get("id").and_then(serde_json::Value::as_i64);
    assert!(
        deps.iter()
            .any(|d| d.get("id").and_then(serde_json::Value::as_i64) == dep_id),
        "newly created deployment should appear in list"
    );

    cleanup_app(&org_app);
}

#[test]
fn test_cloud_deployment_logs() {
    let _ = require_cmd!();
    let name = test_app_name();
    let org_app = create_test_app(&name);

    // Create a deployment first
    let mut cmd = spice_cloud_cmd().expect("credentials required");
    let dep_assert = cmd
        .args([
            "cloud",
            "create",
            "deployment",
            "--app",
            &org_app,
            "--replicas",
            "1",
            "-o",
            "json",
        ])
        .assert()
        .success();

    let deployment: serde_json::Value = serde_json::from_slice(&dep_assert.get_output().stdout)
        .expect("create deployment should produce valid JSON");
    let dep_id = deployment
        .get("id")
        .and_then(serde_json::Value::as_i64)
        .expect("deployment should have an id");

    // Fetch logs (may be empty for a fresh deployment, but command must succeed)
    let mut cmd = spice_cloud_cmd().expect("credentials required");
    cmd.args([
        "cloud",
        "logs",
        "--app",
        &org_app,
        "--deployment",
        &dep_id.to_string(),
        "-o",
        "json",
    ])
    .assert()
    .success();

    cleanup_app(&org_app);
}

// ============================================================================
// Regions & Container Images
// ============================================================================

#[test]
fn test_cloud_list_regions() {
    let mut cmd = require_cmd!();
    let assert = cmd
        .args(["cloud", "regions", "-o", "json"])
        .assert()
        .success();

    let output: serde_json::Value = serde_json::from_slice(&assert.get_output().stdout)
        .expect("regions should produce valid JSON");
    let regions = output.as_array().expect("regions should be an array");
    assert!(!regions.is_empty(), "there should be at least one region");

    // At least one region should have a name
    assert!(
        regions.iter().any(|r| r
            .get("name")
            .and_then(|v| v.as_str())
            .is_some_and(|s| !s.is_empty())),
        "at least one region should have a non-empty name"
    );
}

#[test]
fn test_cloud_list_images() {
    let mut cmd = require_cmd!();
    let assert = cmd
        .args(["cloud", "images", "-o", "json"])
        .assert()
        .success();

    let output: serde_json::Value = serde_json::from_slice(&assert.get_output().stdout)
        .expect("images should produce valid JSON");
    // The JSON output for images includes the full ContainerImagesResponse
    let images = output
        .get("images")
        .and_then(|v| v.as_array())
        .expect("should have images array");
    assert!(!images.is_empty(), "should have at least one image");
    assert!(
        output.get("default").is_some(),
        "should have a default image tag"
    );
}

#[test]
fn test_cloud_list_images_with_channel() {
    let mut cmd = require_cmd!();
    cmd.args(["cloud", "images", "--channel", "stable", "-o", "json"])
        .assert()
        .success();
}

// ============================================================================
// Metrics
// ============================================================================

#[test]
fn test_cloud_metrics() {
    let _ = require_cmd!();
    let name = test_app_name();
    let org_app = create_test_app(&name);

    // Metrics for a fresh app (may be empty, but command must succeed)
    let mut cmd = spice_cloud_cmd().expect("credentials required");
    cmd.args(["cloud", "metrics", "--app", &org_app, "-o", "json"])
        .assert()
        .success();

    cleanup_app(&org_app);
}

// ============================================================================
// Inspect
// ============================================================================

#[test]
fn test_cloud_inspect() {
    let _ = require_cmd!();
    let name = test_app_name();
    let org_app = create_test_app(&name);

    let mut cmd = spice_cloud_cmd().expect("credentials required");
    let assert = cmd
        .args(["cloud", "inspect", "--app", &org_app, "-o", "json"])
        .assert()
        .success();

    let output: serde_json::Value = serde_json::from_slice(&assert.get_output().stdout)
        .expect("inspect should produce valid JSON");
    assert!(output.get("app").is_some(), "inspect should include app");

    cleanup_app(&org_app);
}

// ============================================================================
// Link / Unlink (local config, no API needed)
// ============================================================================

#[test]
fn test_cloud_link_and_unlink() {
    let _ = require_cmd!();
    let name = test_app_name();
    let org_app = create_test_app(&name);

    let temp_dir = tempfile::TempDir::new().expect("should create temp dir");

    // Link
    let mut cmd = spice_cloud_cmd().expect("credentials required");
    cmd.current_dir(temp_dir.path())
        .args(["cloud", "link", &org_app])
        .assert()
        .success()
        .stdout(predicate::str::contains("Linked to app"));

    // Verify .spice/cloud.json was created
    let config_path = temp_dir.path().join(".spice").join("cloud.json");
    assert!(config_path.exists(), ".spice/cloud.json should be created");

    // Unlink
    let mut cmd = spice_cloud_cmd().expect("credentials required");
    cmd.current_dir(temp_dir.path())
        .args(["cloud", "unlink"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Unlinked"));

    assert!(
        !config_path.exists(),
        ".spice/cloud.json should be removed after unlink"
    );

    cleanup_app(&org_app);
}

// ============================================================================
// Help output — verify subcommands are wired correctly
// ============================================================================

#[test]
fn test_cloud_help_lists_all_subcommands() {
    let mut cmd = cargo_bin_cmd!("spice");
    cmd.args(["cloud", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("login"))
        .stdout(predicate::str::contains("logout"))
        .stdout(predicate::str::contains("whoami"))
        .stdout(predicate::str::contains("orgs"))
        .stdout(predicate::str::contains("org"))
        .stdout(predicate::str::contains("link"))
        .stdout(predicate::str::contains("unlink"))
        .stdout(predicate::str::contains("projects"))
        .stdout(predicate::str::contains("deployments"))
        .stdout(predicate::str::contains("regions"))
        .stdout(predicate::str::contains("images"))
        .stdout(predicate::str::contains("secrets"))
        .stdout(predicate::str::contains("logs"))
        .stdout(predicate::str::contains("project"))
        .stdout(predicate::str::contains("deploy"))
        .stdout(predicate::str::contains("status"))
        .stdout(predicate::str::contains("datasets"))
        // `rollback` is intentionally absent from the current CloudCommands enum.
        .stdout(predicate::str::contains("api-keys"))
        .stdout(predicate::str::contains("metrics"));
}

// ============================================================================
// Multi-org surface — no credentials needed
// ============================================================================

#[test]
fn test_cloud_org_flag_is_available_on_every_subcommand() {
    // `--org` is global, so it must reach subcommands whose own args do not
    // declare it. Without this, selecting an org per-invocation would only work
    // on whichever commands happened to be updated.
    for subcommand in [
        "apps",
        "deployments",
        "logs",
        "deploy",
        "inspect",
        "metrics",
    ] {
        let mut cmd = cargo_bin_cmd!("spice");
        cmd.args(["cloud", subcommand, "--help"])
            .assert()
            .success()
            .stdout(predicate::str::contains("--org"));
    }
}

#[test]
fn test_cloud_superseded_spellings_still_parse() {
    // Scripts written against the previous surface must keep working for a
    // release. These are hidden from help but still accepted.
    for argv in [
        vec!["cloud", "create", "project", "--help"],
        vec!["cloud", "get", "project", "--help"],
        vec!["cloud", "update", "project", "--help"],
        vec!["cloud", "delete", "project", "--help"],
        vec!["cloud", "inspect", "--help"],
        vec!["cloud", "instance", "status", "--help"],
        vec!["cloud", "apps", "--help"],
    ] {
        let mut cmd = cargo_bin_cmd!("spice");
        cmd.args(&argv).assert().success();
    }
}

#[test]
fn test_cloud_superseded_spellings_are_hidden_from_help() {
    // Accepted, but not advertised — help should teach only the current shape.
    let mut cmd = cargo_bin_cmd!("spice");
    let assert = cmd.args(["cloud", "--help"]).assert().success();
    let stdout = String::from_utf8_lossy(&assert.get_output().stdout).to_string();

    let commands = stdout
        .split("Commands:")
        .nth(1)
        .unwrap_or_default()
        .split("Options:")
        .next()
        .unwrap_or_default()
        .to_string();

    for hidden in ["inspect", "instance"] {
        assert!(
            !commands.contains(&format!("  {hidden} ")),
            "'{hidden}' should be hidden from the command list, got:\n{commands}"
        );
    }
    assert!(commands.contains("status"), "status should be listed");
    assert!(commands.contains("project"), "project should be listed");
}

#[test]
fn test_cloud_org_subcommands_are_wired() {
    let mut cmd = cargo_bin_cmd!("spice");
    cmd.args(["cloud", "org", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("use"))
        .stdout(predicate::str::contains("current"))
        .stdout(predicate::str::contains("clear"));
}

#[test]
fn test_cloud_deploy_help_documents_wait_and_timeout() {
    let mut cmd = cargo_bin_cmd!("spice");
    cmd.args(["cloud", "deploy", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("--wait"))
        .stdout(predicate::str::contains("--timeout"));
}

#[test]
fn test_cloud_logs_help_documents_filters() {
    let mut cmd = cargo_bin_cmd!("spice");
    cmd.args(["cloud", "logs", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("--level"))
        .stdout(predicate::str::contains("--since"))
        .stdout(predicate::str::contains("--deployment"));
}

#[test]
fn test_cloud_invalid_org_fails_before_contacting_the_api() {
    // An unusable org name must be rejected locally, without a token and
    // without a request that could leak it into a URL or header.
    // Human-readable diagnostics go to the tracing subscriber, which writes to
    // stdout; `--machine` mode is what emits structured errors on stderr.
    let mut cmd = cargo_bin_cmd!("spice");
    cmd.env_remove("SPICE_SPICEAI_TOKEN")
        .env_remove("SPICE_CLOUD_ORG")
        .args(["cloud", "apps", "--org", "not a valid org"])
        .assert()
        .failure()
        .stdout(predicate::str::contains("Invalid organization name"));
}

#[test]
fn test_cloud_machine_errors_carry_a_stable_code() {
    // Agents branch on `error.code`; unauthenticated must be distinguishable
    // from every other failure without parsing prose.
    let temp_dir = tempfile::TempDir::new().expect("should create temp dir");
    let mut cmd = cargo_bin_cmd!("spice");
    let assert = cmd
        .current_dir(temp_dir.path())
        .env_remove("SPICE_SPICEAI_TOKEN")
        .env_remove("SPICE_CLOUD_ORG")
        .env("HOME", temp_dir.path())
        .args(["--machine", "cloud", "apps"])
        .assert()
        .failure();

    let stderr = String::from_utf8_lossy(&assert.get_output().stderr).to_string();
    let body: serde_json::Value = serde_json::from_str(stderr.trim())
        .unwrap_or_else(|e| panic!("machine-mode errors must be JSON: {e}; got: {stderr}"));
    assert_eq!(
        body["error"]["code"].as_str(),
        Some("not_authenticated"),
        "unexpected machine error: {body}"
    );
    assert!(
        body["error"]["hint"]
            .as_str()
            .is_some_and(|hint| hint.contains("spice cloud login")),
        "the error should carry an actionable hint: {body}"
    );
}

#[test]
fn test_cloud_named_org_without_a_credential_fails_closed() {
    // Regression: the CLI used to fall back to the default credential, running
    // the command against that credential's org while reporting the requested
    // one. A named org must require a credential bound to it.
    let temp_dir = tempfile::TempDir::new().expect("should create temp dir");
    let mut cmd = cargo_bin_cmd!("spice");
    let assert = cmd
        .current_dir(temp_dir.path())
        .env("HOME", temp_dir.path())
        .env("SPICE_SPICEAI_TOKEN", "personal-org-token")
        .env_remove("SPICE_CLOUD_ORG")
        .args(["--machine", "cloud", "apps", "--org", "spicehq"])
        .assert()
        .failure();

    let stderr = String::from_utf8_lossy(&assert.get_output().stderr).to_string();
    let body: serde_json::Value =
        serde_json::from_str(stderr.trim()).expect("machine errors must be JSON");
    assert_eq!(
        body["error"]["code"].as_str(),
        Some("org_credential_missing"),
        "unexpected error: {body}"
    );
}

#[test]
fn test_cloud_auth_failures_use_a_dedicated_exit_code() {
    // Automation branches on the exit code to re-authenticate without parsing
    // prose, matching the convention `gh` uses.
    let temp_dir = tempfile::TempDir::new().expect("should create temp dir");
    let mut cmd = cargo_bin_cmd!("spice");
    cmd.current_dir(temp_dir.path())
        .env("HOME", temp_dir.path())
        .env_remove("SPICE_SPICEAI_TOKEN")
        .env_remove("SPICE_CLOUD_ORG")
        .args(["cloud", "apps"])
        .assert()
        .code(4);
}

#[test]
fn test_cloud_conflicting_explicit_orgs_are_refused() {
    // Regression: an explicit --org used to lose silently to the app argument
    // with only a warning, which `--machine` output never shows.
    let temp_dir = tempfile::TempDir::new().expect("should create temp dir");
    let mut cmd = cargo_bin_cmd!("spice");
    let assert = cmd
        .current_dir(temp_dir.path())
        .env("HOME", temp_dir.path())
        .env("SPICE_SPICEAI_TOKEN", "token")
        .env_remove("SPICE_CLOUD_ORG")
        .args([
            "--machine",
            "cloud",
            "deploy",
            "--app",
            "spicehq/team-app",
            "--org",
            "lukekim",
        ])
        .assert()
        .failure();

    let stderr = String::from_utf8_lossy(&assert.get_output().stderr).to_string();
    let body: serde_json::Value =
        serde_json::from_str(stderr.trim()).expect("machine errors must be JSON");
    assert_eq!(body["error"]["code"].as_str(), Some("org_conflict"));
    let message = body["error"]["message"].as_str().unwrap_or_default();
    assert!(
        message.contains("spicehq") && message.contains("lukekim"),
        "the error must name both organizations: {message}"
    );
}

#[test]
fn test_cloud_app_required_error_names_both_ways_to_supply_one() {
    let temp_dir = tempfile::TempDir::new().expect("should create temp dir");
    let mut cmd = cargo_bin_cmd!("spice");
    cmd.current_dir(temp_dir.path())
        .env("SPICE_SPICEAI_TOKEN", "placeholder-token")
        .env_remove("SPICE_CLOUD_ORG")
        .env("HOME", temp_dir.path())
        .args(["cloud", "deployments"])
        .assert()
        .failure()
        .stdout(predicate::str::contains("--project"))
        .stdout(predicate::str::contains("spice cloud link"));
}

#[test]
fn test_cloud_secrets_help() {
    let mut cmd = cargo_bin_cmd!("spice");
    cmd.args(["cloud", "secrets", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("list"))
        .stdout(predicate::str::contains("set"))
        .stdout(predicate::str::contains("get"))
        .stdout(predicate::str::contains("delete"));
}

#[test]
fn test_cloud_create_help() {
    let mut cmd = cargo_bin_cmd!("spice");
    cmd.args(["cloud", "create", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("project"))
        .stdout(predicate::str::contains("deployment"));
}
