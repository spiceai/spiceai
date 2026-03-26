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

//! Live integration tests for [`CloudClient`] against the Spice Cloud dev API.
//!
//! # Running
//!
//! These tests skip automatically when `SPICE_SPICEAI_TOKEN` is not set.
//! To run them locally:
//!
//! ```bash
//! SPICE_CLOUD_API_URL="https://dev-api.spice.ai" \
//! SPICE_SPICEAI_TOKEN="<your-dev-token>" \
//!   cargo test -p spice-cloud-client --test integration -- --test-threads=1
//! ```
//!
//! `--test-threads=1` is recommended because several tests create/delete apps
//! against the shared dev API and may hit rate limits.

use spice_cloud_client::CloudClient;
use spice_cloud_client::error::Error;
use spice_cloud_client::types::{CreateAppRequest, CreateDeploymentRequest, UpdateAppRequest};

const DEFAULT_DEV_API: &str = "https://dev-api.spice.ai";

/// Build an authenticated [`CloudClient`] pointing at the dev API, or return
/// `None` when `SPICE_SPICEAI_TOKEN` is not set (causing tests to skip).
fn try_dev_client() -> Option<CloudClient> {
    let token = std::env::var("SPICE_SPICEAI_TOKEN").ok()?;
    if token.is_empty() {
        return None;
    }
    let base_url =
        std::env::var("SPICE_CLOUD_API_URL").unwrap_or_else(|_| DEFAULT_DEV_API.to_string());
    Some(
        CloudClient::new(&base_url)
            .expect("should build CloudClient")
            .with_token(token),
    )
}

/// Convenience wrapper: skip the test when credentials are absent.
macro_rules! require_client {
    () => {
        match try_dev_client() {
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

/// Helper to build a [`CreateAppRequest`] with sensible defaults for tests.
fn new_create_app_request(name: &str, description: Option<&str>) -> CreateAppRequest {
    CreateAppRequest {
        name: name.to_string(),
        description: description.map(String::from),
        visibility: "private".to_string(),
        cname: None,
        tags: None,
        replicas: None,
        resources: None,
        executor: None,
    }
}

/// Best-effort cleanup: delete an app by ID, ignoring errors.
async fn cleanup_app(client: &CloudClient, app_id: i64) {
    let _ = client.delete_app(app_id).await;
}

/// Common insta settings: redact all dynamic fields produced by the API so
/// that snapshots are deterministic across runs and environments.
fn insta_settings() -> insta::Settings {
    let mut settings = insta::Settings::clone_current();
    // Numeric IDs
    settings.add_filter(r#""id": \d+"#, r#""id": "<redacted>""#);
    // ISO-8601 timestamps  (e.g. "2026-03-25T12:00:00Z" or with fractional seconds / offset)
    settings.add_filter(
        r#""\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[^"]*""#,
        r#""<timestamp>""#,
    );
    // API keys / tokens (long alphanumeric strings, 20+ chars)
    settings.add_filter(r#""[A-Za-z0-9_\-]{20,}""#, r#""<redacted>""#);
    // The test-generated app name contains a random UUID prefix
    settings.add_filter(r#""ci-test-[0-9a-f]{8}""#, r#""ci-test-<uuid>""#);
    // org name (varies by account)
    settings.add_filter(r#""org": "[^"]+""#, r#""org": "<org>""#);
    // username / email in auth context
    settings.add_filter(r#""username": "[^"]+""#, r#""username": "<redacted>""#);
    settings.add_filter(r#""email": "[^"]+""#, r#""email": "<redacted>""#);
    settings.add_filter(r#""org_name": "[^"]+""#, r#""org_name": "<redacted>""#);
    // Container image references (vary by environment)
    settings.add_filter(r#""image": "[^"]+""#, r#""image": "<image>""#);
    settings.add_filter(r#""image_tag": "[^"]+""#, r#""image_tag": "<image_tag>""#);
    settings
}

// ============================================================================
// Auth
// ============================================================================

#[tokio::test]
async fn test_auth_context() {
    let client = require_client!();
    let ctx = client
        .get_auth_context()
        .await
        .expect("get_auth_context should succeed");

    insta_settings().bind(|| {
        insta::assert_json_snapshot!(ctx, @r#"
        {
          "username": "<redacted>",
          "email": "<redacted>",
          "org_name": "<redacted>",
          "app_name": null,
          "app_api_key": null
        }
        "#);
    });
}

#[tokio::test]
async fn test_auth_context_unauthorized() {
    let base_url =
        std::env::var("SPICE_CLOUD_API_URL").unwrap_or_else(|_| DEFAULT_DEV_API.to_string());
    // Still need the env var to be set so we know the test is opted-in.
    if std::env::var("SPICE_SPICEAI_TOKEN").is_err() {
        eprintln!("SPICE_SPICEAI_TOKEN not set — skipping");
        return;
    }
    let client = CloudClient::new(&base_url)
        .expect("should build CloudClient")
        .with_token("invalid-token-that-should-not-work");

    let err = client
        .get_auth_context()
        .await
        .expect_err("should fail with invalid token");

    assert!(
        matches!(err, Error::Unauthorized { .. }),
        "expected Unauthorized, got: {err:?}"
    );
}

// ============================================================================
// Apps — full CRUD lifecycle
// ============================================================================

#[tokio::test]
async fn test_app_crud_lifecycle() {
    let client = require_client!();
    let name = test_app_name();
    let settings = insta_settings();

    // --- Create ---
    let create_req = new_create_app_request(&name, Some("Integration test app"));
    let app = client
        .create_app(&create_req)
        .await
        .expect("create_app should succeed");
    let app_id = app.id;

    settings.bind(|| {
        insta::assert_json_snapshot!(&app, @r#"
        {
          "id": "<redacted>",
          "name": "ci-test-<uuid>",
          "org": "<org>",
          "description": "Integration test app",
          "visibility": "private",
          "created_at": "<timestamp>",
          "region": null,
          "production_branch": null,
          "config": null
        }
        "#);
    });

    // --- Get by ID ---
    let fetched = client
        .get_app_by_id(app_id)
        .await
        .expect("get_app_by_id should succeed");
    assert_eq!(fetched.id, app_id);
    assert_eq!(fetched.name, name);

    // --- List (must contain our app) ---
    let apps = client.list_apps().await.expect("list_apps should succeed");
    assert!(
        apps.iter().any(|a| a.id == app_id),
        "list_apps should contain the created app"
    );

    // --- Update ---
    let update_req = UpdateAppRequest {
        description: Some("Updated description".to_string()),
        ..UpdateAppRequest::default()
    };
    let updated = client
        .update_app(app_id, &update_req)
        .await
        .expect("update_app should succeed");
    assert_eq!(
        updated.description.as_deref(),
        Some("Updated description"),
        "description should be updated"
    );

    // --- Delete ---
    client
        .delete_app(app_id)
        .await
        .expect("delete_app should succeed");

    // --- Confirm deleted (expect 404) ---
    let err = client
        .get_app_by_id(app_id)
        .await
        .expect_err("get_app_by_id after delete should fail");
    assert!(
        matches!(err, Error::NotFound { .. }),
        "expected NotFound after deletion, got: {err:?}"
    );
}

#[tokio::test]
async fn test_create_app_duplicate_name_conflict() {
    let client = require_client!();
    let name = test_app_name();

    let req = new_create_app_request(&name, None);

    let app = client
        .create_app(&req)
        .await
        .expect("first create should succeed");
    let app_id = app.id;

    // Second create with same name should conflict.
    let result = client.create_app(&req).await;
    assert!(
        result.is_err(),
        "duplicate create should fail, got: {result:?}"
    );
    if let Err(ref e) = result {
        assert!(
            matches!(e, Error::Conflict { .. } | Error::Api { .. }),
            "expected Conflict or Api error for duplicate name, got: {e:?}"
        );
    }

    cleanup_app(&client, app_id).await;
}

#[tokio::test]
async fn test_get_app_not_found() {
    let client = require_client!();
    let err = client
        .get_app_by_id(i64::MAX)
        .await
        .expect_err("non-existent app ID should fail");
    assert!(
        matches!(err, Error::NotFound { .. }),
        "expected NotFound, got: {err:?}"
    );
}

#[tokio::test]
async fn test_delete_app_not_found() {
    let client = require_client!();
    let err = client
        .delete_app(i64::MAX)
        .await
        .expect_err("deleting non-existent app should fail");
    assert!(
        matches!(err, Error::NotFound { .. }),
        "expected NotFound, got: {err:?}"
    );
}

// ============================================================================
// Secrets — full CRUD lifecycle
// ============================================================================

#[tokio::test]
async fn test_secrets_crud_lifecycle() {
    let client = require_client!();
    let name = test_app_name();
    let settings = insta_settings();

    let app = client
        .create_app(&new_create_app_request(&name, Some("Secrets test")))
        .await
        .expect("create_app for secrets test should succeed");
    let app_id = app.id;

    // --- Set secret ---
    let secret = client
        .set_secret(app_id, "TEST_SECRET", "s3cret_value")
        .await
        .expect("set_secret should succeed");

    settings.bind(|| {
        insta::assert_json_snapshot!(&secret, @r#"
        {
          "id": "<redacted>",
          "name": "TEST_SECRET",
          "value": null,
          "created_at": "<timestamp>",
          "updated_at": "<timestamp>"
        }
        "#);
    });

    // --- Get secret ---
    let fetched = client
        .get_secret(app_id, "TEST_SECRET")
        .await
        .expect("get_secret should succeed");
    assert_eq!(fetched.name, "TEST_SECRET");
    if let Some(ref val) = fetched.value {
        assert_eq!(val, "s3cret_value", "secret value should round-trip");
    }

    // --- List secrets (must contain ours) ---
    let secrets = client
        .list_secrets(app_id)
        .await
        .expect("list_secrets should succeed");
    assert!(
        secrets.iter().any(|s| s.name == "TEST_SECRET"),
        "list_secrets should include TEST_SECRET"
    );

    // --- Overwrite secret ---
    let updated = client
        .set_secret(app_id, "TEST_SECRET", "new_value")
        .await
        .expect("overwriting secret should succeed");
    assert_eq!(updated.name, "TEST_SECRET");

    // --- Delete secret ---
    client
        .delete_secret(app_id, "TEST_SECRET")
        .await
        .expect("delete_secret should succeed");

    // --- Confirm deleted ---
    let err = client
        .get_secret(app_id, "TEST_SECRET")
        .await
        .expect_err("get_secret after delete should fail");
    assert!(
        matches!(err, Error::NotFound { .. }),
        "expected NotFound for deleted secret, got: {err:?}"
    );

    cleanup_app(&client, app_id).await;
}

#[tokio::test]
async fn test_get_secret_not_found() {
    let client = require_client!();
    let name = test_app_name();

    let app = client
        .create_app(&new_create_app_request(&name, None))
        .await
        .expect("create_app should succeed");
    let app_id = app.id;

    let err = client
        .get_secret(app_id, "DOES_NOT_EXIST")
        .await
        .expect_err("non-existent secret should fail");
    assert!(
        matches!(err, Error::NotFound { .. }),
        "expected NotFound, got: {err:?}"
    );

    cleanup_app(&client, app_id).await;
}

#[tokio::test]
async fn test_multiple_secrets() {
    let client = require_client!();
    let name = test_app_name();

    let app = client
        .create_app(&new_create_app_request(&name, None))
        .await
        .expect("create_app should succeed");
    let app_id = app.id;

    for i in 0..3 {
        client
            .set_secret(app_id, &format!("KEY_{i}"), &format!("val_{i}"))
            .await
            .unwrap_or_else(|e| panic!("set_secret KEY_{i} should succeed: {e:?}"));
    }

    let secrets = client
        .list_secrets(app_id)
        .await
        .expect("list_secrets should succeed");
    assert!(
        secrets.len() >= 3,
        "should have at least 3 secrets, got {}",
        secrets.len()
    );

    for i in 0..3 {
        client
            .delete_secret(app_id, &format!("KEY_{i}"))
            .await
            .unwrap_or_else(|e| panic!("delete_secret KEY_{i} should succeed: {e:?}"));
    }

    let after = client
        .list_secrets(app_id)
        .await
        .expect("list_secrets after cleanup should succeed");
    assert!(
        !after.iter().any(|s| s.name.starts_with("KEY_")),
        "all KEY_* secrets should be deleted"
    );

    cleanup_app(&client, app_id).await;
}

// ============================================================================
// API Keys
// ============================================================================

#[tokio::test]
async fn test_api_keys_get_and_regenerate() {
    let client = require_client!();
    let name = test_app_name();
    let settings = insta_settings();

    let app = client
        .create_app(&new_create_app_request(&name, None))
        .await
        .expect("create_app should succeed");
    let app_id = app.id;

    let keys = client
        .get_api_keys(app_id)
        .await
        .expect("get_api_keys should succeed");
    let original_key1 = keys.api_key.clone();

    settings.bind(|| {
        insta::assert_json_snapshot!(&keys, @r#"
        {
          "api_key": "<redacted>",
          "api_key_2": "<redacted>"
        }
        "#);
    });

    let regen = client
        .regenerate_api_key(app_id, 1)
        .await
        .expect("regenerate_api_key should succeed");
    assert_eq!(
        regen.regenerated_key,
        Some(1),
        "regenerated_key should be 1"
    );
    if let (Some(orig), Some(new_key)) = (&original_key1, &regen.api_key) {
        assert_ne!(
            orig, new_key,
            "regenerated key 1 should differ from original"
        );
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

    cleanup_app(&client, app_id).await;
}

// ============================================================================
// Deployments
// ============================================================================

#[tokio::test]
async fn test_deployments_list_empty() {
    let client = require_client!();
    let name = test_app_name();

    let app = client
        .create_app(&new_create_app_request(&name, None))
        .await
        .expect("create_app should succeed");
    let app_id = app.id;

    let deployments = client
        .list_deployments(app_id, 10, None)
        .await
        .expect("list_deployments should succeed");
    for d in &deployments {
        assert!(d.id > 0, "deployment id should be positive");
        assert!(
            !d.status.is_empty(),
            "deployment status should not be empty"
        );
    }

    cleanup_app(&client, app_id).await;
}

#[tokio::test]
async fn test_deployments_list_with_status_filter() {
    let client = require_client!();
    let name = test_app_name();

    let app = client
        .create_app(&new_create_app_request(&name, None))
        .await
        .expect("create_app should succeed");
    let app_id = app.id;

    let deployments = client
        .list_deployments(app_id, 5, Some("running"))
        .await
        .expect("list_deployments with status filter should succeed");
    for d in &deployments {
        assert_eq!(
            d.status, "running",
            "filtered deployments should all be 'running'"
        );
    }

    cleanup_app(&client, app_id).await;
}

#[tokio::test]
async fn test_create_deployment() {
    let client = require_client!();
    let name = test_app_name();
    let settings = insta_settings();

    let app = client
        .create_app(&new_create_app_request(&name, None))
        .await
        .expect("create_app should succeed");
    let app_id = app.id;

    let req = CreateDeploymentRequest {
        image: None,
        image_tag: None,
        replicas: Some(1),
        branch: None,
        commit_sha: None,
        commit_message: Some("integration test deployment".to_string()),
        channel: None,
        debug: false,
    };
    let deployment = client
        .create_deployment(app_id, &req)
        .await
        .expect("create_deployment should succeed");

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
          "commit_message": "integration test deployment",
          "error_message": null,
          "creation_source": null,
          "created_by": null
        }
        "#);
    });

    let deployments = client
        .list_deployments(app_id, 10, None)
        .await
        .expect("list_deployments should succeed");
    assert!(
        deployments.iter().any(|d| d.id == deployment.id),
        "newly created deployment should appear in list"
    );

    cleanup_app(&client, app_id).await;
}

#[tokio::test]
async fn test_deployment_logs() {
    let client = require_client!();
    let name = test_app_name();

    let app = client
        .create_app(&new_create_app_request(&name, None))
        .await
        .expect("create_app should succeed");
    let app_id = app.id;

    let deployment = client
        .create_deployment(
            app_id,
            &CreateDeploymentRequest {
                image: None,
                image_tag: None,
                replicas: Some(1),
                branch: None,
                commit_sha: None,
                commit_message: Some("logs test".to_string()),
                channel: None,
                debug: false,
            },
        )
        .await
        .expect("create_deployment should succeed");

    let logs = client
        .get_deployment_logs(app_id, deployment.id, 50, None)
        .await
        .expect("get_deployment_logs should succeed");
    // Logs may be empty for a freshly created deployment, but the call must succeed.
    let _ = logs.logs.len();

    cleanup_app(&client, app_id).await;
}

#[tokio::test]
async fn test_rollback() {
    let client = require_client!();
    let name = test_app_name();

    let app = client
        .create_app(&new_create_app_request(&name, None))
        .await
        .expect("create_app should succeed");
    let app_id = app.id;

    let dep_req = CreateDeploymentRequest {
        image: None,
        image_tag: None,
        replicas: Some(1),
        branch: None,
        commit_sha: None,
        commit_message: Some("rollback test v1".to_string()),
        channel: None,
        debug: false,
    };
    let dep1 = client
        .create_deployment(app_id, &dep_req)
        .await
        .expect("create first deployment should succeed");

    let dep_req2 = CreateDeploymentRequest {
        image: None,
        image_tag: None,
        replicas: Some(1),
        branch: None,
        commit_sha: None,
        commit_message: Some("rollback test v2".to_string()),
        channel: None,
        debug: false,
    };
    let _dep2 = client
        .create_deployment(app_id, &dep_req2)
        .await
        .expect("create second deployment should succeed");

    let rollback_dep = client
        .rollback(app_id, dep1.id)
        .await
        .expect("rollback should succeed");
    assert!(
        rollback_dep.id > 0,
        "rollback should create a new deployment"
    );

    cleanup_app(&client, app_id).await;
}

// ============================================================================
// Regions & Container Images
// ============================================================================

#[tokio::test]
async fn test_list_regions() {
    let client = require_client!();
    let resp = client
        .list_regions(None)
        .await
        .expect("list_regions should succeed");
    assert!(
        !resp.regions.is_empty(),
        "there should be at least one region"
    );

    let has_default = resp.regions.iter().any(|r| r.is_default);
    assert!(has_default, "at least one region should be the default");

    for r in &resp.regions {
        assert!(!r.name.is_empty(), "region name should not be empty");
        assert!(!r.region.is_empty(), "region code should not be empty");
        assert!(
            !r.provider.is_empty(),
            "region provider should not be empty"
        );
    }
}

#[tokio::test]
async fn test_list_container_images() {
    let client = require_client!();
    let resp = client
        .list_container_images(None)
        .await
        .expect("list_container_images should succeed");
    assert!(
        !resp.images.is_empty(),
        "there should be at least one container image"
    );
    assert!(
        resp.default.is_some(),
        "there should be a default image tag"
    );

    for img in &resp.images {
        assert!(!img.tag.is_empty(), "image tag should not be empty");
    }
}

#[tokio::test]
async fn test_list_container_images_with_channel_filter() {
    let client = require_client!();
    let resp = client
        .list_container_images(Some("stable"))
        .await
        .expect("list_container_images with channel filter should succeed");
    for img in &resp.images {
        if let Some(ref ch) = img.channel {
            assert_eq!(
                ch, "stable",
                "filtered images should be in 'stable' channel"
            );
        }
    }
}

// ============================================================================
// Metrics
// ============================================================================

#[tokio::test]
async fn test_get_app_metrics() {
    let client = require_client!();
    let name = test_app_name();

    let app = client
        .create_app(&new_create_app_request(&name, None))
        .await
        .expect("create_app should succeed");
    let app_id = app.id;

    let metrics = client
        .get_app_metrics(app_id, None)
        .await
        .expect("get_app_metrics should succeed");
    let _ = metrics.metrics.len();

    let metrics_windowed = client
        .get_app_metrics(app_id, Some("5m"))
        .await
        .expect("get_app_metrics with window should succeed");
    let _ = metrics_windowed.metrics.len();

    cleanup_app(&client, app_id).await;
}

// ============================================================================
// Client construction
// ============================================================================

#[tokio::test]
async fn test_default_url_client() {
    // No credentials needed — just tests client construction.
    let client = CloudClient::default_url().expect("default_url should build");
    assert_eq!(client.base_url(), "https://api.spice.ai");
}

#[tokio::test]
async fn test_with_timeout() {
    let base_url =
        std::env::var("SPICE_CLOUD_API_URL").unwrap_or_else(|_| DEFAULT_DEV_API.to_string());
    let Some(token) = std::env::var("SPICE_SPICEAI_TOKEN").ok() else {
        eprintln!("SPICE_SPICEAI_TOKEN not set — skipping");
        return;
    };

    let client = CloudClient::new(&base_url)
        .expect("should build client")
        .with_token(token)
        .with_timeout(std::time::Duration::from_nanos(1))
        .expect("with_timeout should succeed");

    let err = client
        .get_auth_context()
        .await
        .expect_err("extremely short timeout should cause failure");
    assert!(
        matches!(err, Error::HttpRequest { .. }),
        "expected HttpRequest (timeout), got: {err:?}"
    );
}

#[tokio::test]
async fn test_base_url_trailing_slash_trimmed() {
    let _ = require_client!();
    let base_url =
        std::env::var("SPICE_CLOUD_API_URL").unwrap_or_else(|_| DEFAULT_DEV_API.to_string());
    let token =
        std::env::var("SPICE_SPICEAI_TOKEN").expect("token must be set (require_client! passed)");

    let url_with_slash = format!("{base_url}/");
    let client = CloudClient::new(&url_with_slash)
        .expect("should build client")
        .with_token(token);

    let ctx = client
        .get_auth_context()
        .await
        .expect("trailing-slash URL should still work");
    assert!(!ctx.username.is_empty(), "username must not be empty");
}
