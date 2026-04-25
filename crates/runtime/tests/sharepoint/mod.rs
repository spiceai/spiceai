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

//! End-to-end SharePoint integration tests.
//!
//! These tests exercise the real Microsoft Graph API and write to a real
//! SharePoint drive. They skip (and print a one-line skip reason to stderr)
//! when the required env vars are not set, so the full
//! `cargo test -p runtime --test integration` run stays green for
//! contributors without a SharePoint tenant.
//!
//! To run them:
//!
//! ```sh
//! export SHAREPOINT_TEST_TENANT_ID=...
//! export SHAREPOINT_TEST_CLIENT_ID=...
//! export SHAREPOINT_TEST_CLIENT_SECRET=...
//! # Optional — defaults to `sharepoint://me`.
//! export SHAREPOINT_TEST_DRIVE=sharepoint://sites/{site-id}
//! # Optional — defaults to `spice-integration-test`.
//! export SHAREPOINT_TEST_PREFIX=Documents/spice-tests
//! cargo test -p runtime --test integration sharepoint:: -- --include-ignored
//! ```
//!
//! The tests write files under `{SHAREPOINT_TEST_DRIVE}/{SHAREPOINT_TEST_PREFIX}/`
//! and attempt to clean up after themselves on the normal path. Cleanup is
//! best-effort: a test that exits early on a timeout/INSERT/SELECT error may
//! leave its file behind, so each file name is unique-per-run to avoid
//! collisions if that happens.

#![expect(
    clippy::doc_markdown,
    reason = "prose-frequent identifiers (SharePoint, DataFusion) are clearer without backticks"
)]

use std::{sync::Arc, time::Duration};

use app::AppBuilder;
use arrow::array::{Int64Array, RecordBatch, UInt64Array};
use data_components::sharepoint::auth::{DEFAULT_SCOPE, SharepointAuth};
use futures::StreamExt;
use object_store::ObjectStore;
use runtime::Runtime;
use secrecy::SecretString;
use spicepod::{component::dataset::Dataset, param::Params};

use crate::{configure_test_datafusion, init_tracing, utils::test_request_context};

/// Required env vars for live tests. If any are missing, tests skip.
struct LiveConfig {
    tenant_id: String,
    client_id: String,
    client_secret: String,
    drive_url_prefix: String,
    path_prefix: String,
}

impl LiveConfig {
    fn from_env() -> Option<Self> {
        let tenant_id = std::env::var("SHAREPOINT_TEST_TENANT_ID").ok()?;
        let client_id = std::env::var("SHAREPOINT_TEST_CLIENT_ID").ok()?;
        let client_secret = std::env::var("SHAREPOINT_TEST_CLIENT_SECRET").ok()?;
        if tenant_id.is_empty() || client_id.is_empty() || client_secret.is_empty() {
            return None;
        }
        let drive_url_prefix = std::env::var("SHAREPOINT_TEST_DRIVE")
            .unwrap_or_else(|_| "sharepoint://me".to_string());
        let path_prefix = std::env::var("SHAREPOINT_TEST_PREFIX")
            .unwrap_or_else(|_| "spice-integration-test".to_string());
        Some(Self {
            tenant_id,
            client_id,
            client_secret,
            drive_url_prefix,
            path_prefix,
        })
    }

    fn dataset_uri(&self, file: &str) -> String {
        format!(
            "{}/{}/{}",
            self.drive_url_prefix.trim_end_matches('/'),
            self.path_prefix.trim_matches('/'),
            file
        )
    }

    fn to_params(&self, file_format: &str) -> Params {
        Params::from_string_map(
            [
                ("sharepoint_tenant_id".to_string(), self.tenant_id.clone()),
                ("sharepoint_client_id".to_string(), self.client_id.clone()),
                (
                    "sharepoint_client_secret".to_string(),
                    self.client_secret.clone(),
                ),
                ("file_format".to_string(), file_format.to_string()),
            ]
            .into_iter()
            .collect(),
        )
    }
}

fn unique_filename(ext: &str) -> String {
    // Combine ms timestamp (human-readable in cleanup logs) with a v4
    // UUID suffix so two tests starting in the same millisecond — or two
    // parallel test processes — can't ever collide and trample each
    // other's drive items.
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    let nonce = uuid::Uuid::new_v4().simple();
    format!("spice-test-{ts}-{nonce}.{ext}")
}

/// Build a `SharepointObjectStore` matching the `kind` DataFusion would
/// derive from the test URI, plus the path the test should pass into
/// `delete()`/etc. — for `me` URIs that's the in-drive path; for other
/// kinds it's `{drive-id}/{in-drive}` so the store's `resolve()` can
/// recover the drive ID from the first path segment.
fn store_and_delete_path(
    client: std::sync::Arc<data_components::sharepoint::GraphClient>,
    uri: &str,
) -> Result<
    (
        data_components::sharepoint::object_store::SharepointObjectStore,
        object_store::path::Path,
    ),
    anyhow::Error,
> {
    use data_components::sharepoint::object_store::{
        DriveKind, SharepointObjectStore, SharepointObjectStoreConfig,
    };
    use data_components::sharepoint::url::{DriveRef, SharepointUrl};

    let parsed = SharepointUrl::parse(uri).map_err(|e| anyhow::anyhow!("parse test uri: {e}"))?;
    let (kind, path) = match &parsed.drive {
        DriveRef::Me => (None, parsed.item_path.clone()),
        DriveRef::Drive(id) => (Some(DriveKind::Drives), prefix_with(id, &parsed.item_path)),
        DriveRef::Site(id) => (Some(DriveKind::Sites), prefix_with(id, &parsed.item_path)),
        DriveRef::User(id) => (Some(DriveKind::Users), prefix_with(id, &parsed.item_path)),
        DriveRef::Group(id) => (Some(DriveKind::Groups), prefix_with(id, &parsed.item_path)),
    };
    let store = SharepointObjectStore::new(client, kind, SharepointObjectStoreConfig::default());
    Ok((store, path))
}

/// Extract the scalar value produced by `SELECT COUNT(*) AS n FROM ...`.
///
/// `assert!(!batches.is_empty())` is useless against `COUNT(*)` because it
/// always returns one row, so we have to actually inspect the cell.
fn count_from_batches(batches: &[RecordBatch]) -> Result<i64, anyhow::Error> {
    let batch = batches
        .first()
        .ok_or_else(|| anyhow::anyhow!("no batches returned"))?;
    let col = batch.column(0);
    if let Some(array) = col.as_any().downcast_ref::<UInt64Array>() {
        if array.is_empty() {
            return Err(anyhow::anyhow!("count batch had zero rows"));
        }
        return i64::try_from(array.value(0))
            .map_err(|_| anyhow::anyhow!("count value overflowed i64"));
    }
    if let Some(array) = col.as_any().downcast_ref::<Int64Array>() {
        if array.is_empty() {
            return Err(anyhow::anyhow!("count batch had zero rows"));
        }
        return Ok(array.value(0));
    }
    Err(anyhow::anyhow!(
        "expected UInt64 or Int64 count column, got {:?}",
        col.data_type()
    ))
}

fn prefix_with(id: &str, p: &object_store::path::Path) -> object_store::path::Path {
    let mut segments = vec![id.to_string()];
    for part in p.parts() {
        segments.push(part.as_ref().to_string());
    }
    segments.iter().map(String::as_str).collect()
}

/// End-to-end round-trip using `sharepoint://` URL: DDL create external table,
/// `INSERT INTO`, `SELECT`, `DELETE`. Skips silently when credentials aren't
/// in the environment.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires live SharePoint tenant credentials — see module docs"]
async fn sharepoint_csv_round_trip() -> Result<(), anyhow::Error> {
    let Some(cfg) = LiveConfig::from_env() else {
        eprintln!("Skipping sharepoint_csv_round_trip: env vars not set");
        return Ok(());
    };

    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async move {
            let file = unique_filename("csv");
            let uri = cfg.dataset_uri(&file);
            let mut ds = Dataset::new(uri.clone(), "sp_test");
            ds.params = Some(cfg.to_params("csv"));

            let app = AppBuilder::new("sharepoint_csv_round_trip")
                .with_dataset(ds)
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;
            let cloned = Arc::new(rt.clone());
            tokio::select! {
                () = tokio::time::sleep(Duration::from_secs(60)) => {
                    return Err(anyhow::anyhow!("timed out loading SharePoint dataset"));
                }
                () = cloned.load_components() => {}
            }

            // INSERT: write a single row via DataFusion.
            let _ = rt
                .datafusion()
                .query_builder("INSERT INTO sp_test VALUES ('hello', 1)")
                .build()
                .run()
                .await
                .map_err(|e| anyhow::anyhow!("INSERT failed: {e}"))?;

            // SELECT: read it back.
            let mut q = rt
                .datafusion()
                .query_builder("SELECT COUNT(*) AS n FROM sp_test")
                .build()
                .run()
                .await
                .map_err(|e| anyhow::anyhow!("SELECT failed: {e}"))?;
            let mut batches = Vec::new();
            while let Some(b) = q.data.next().await {
                batches.push(b?);
            }
            let count = count_from_batches(&batches)?;
            assert_eq!(count, 1, "INSERT should make one row visible on read-back");

            // Clean up — DELETE the file we wrote.
            let auth = SharepointAuth::ClientCredentials {
                tenant_id: cfg.tenant_id.clone(),
                client_id: cfg.client_id.clone(),
                client_secret: SecretString::new(cfg.client_secret.clone().into()),
                scope: Some(DEFAULT_SCOPE.to_string()),
            };
            let client = auth
                .build_graph_client()
                .await
                .map_err(|e| anyhow::anyhow!("build_graph_client: {e}"))?;
            let (store, delete_path) = store_and_delete_path(client, &uri)?;
            store
                .delete(&delete_path)
                .await
                .map_err(|e| anyhow::anyhow!("cleanup delete: {e}"))?;
            Ok(())
        })
        .await
}

/// Exercise `COPY TO` writing a small Parquet file, then read it back.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires live SharePoint tenant credentials — see module docs"]
async fn sharepoint_parquet_copy_to() -> Result<(), anyhow::Error> {
    let Some(cfg) = LiveConfig::from_env() else {
        eprintln!("Skipping sharepoint_parquet_copy_to: env vars not set");
        return Ok(());
    };

    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async move {
            let file = unique_filename("parquet");
            let uri = cfg.dataset_uri(&file);

            // `COPY (SELECT ...) TO '<sharepoint://...>' (FORMAT parquet)`
            // writes via the registered SharepointObjectStore; the target
            // dataset lets us read it back after the COPY completes.
            let mut target = Dataset::new(uri.clone(), "sp_target");
            target.params = Some(cfg.to_params("parquet"));

            let app = AppBuilder::new("sharepoint_parquet_copy_to")
                .with_dataset(target)
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;
            let cloned = Arc::new(rt.clone());
            tokio::select! {
                () = tokio::time::sleep(Duration::from_secs(60)) => {
                    return Err(anyhow::anyhow!("timed out loading dataset"));
                }
                () = cloned.load_components() => {}
            }

            let copy_sql =
                format!("COPY (SELECT 'Q2' AS quarter, 42 AS n) TO '{uri}' (FORMAT parquet)");
            let _ = rt
                .datafusion()
                .query_builder(&copy_sql)
                .build()
                .run()
                .await
                .map_err(|e| anyhow::anyhow!("COPY TO failed: {e}"))?;

            let mut q = rt
                .datafusion()
                .query_builder("SELECT COUNT(*) AS n FROM sp_target")
                .build()
                .run()
                .await
                .map_err(|e| anyhow::anyhow!("SELECT failed: {e}"))?;
            let mut batches = Vec::new();
            while let Some(b) = q.data.next().await {
                batches.push(b?);
            }
            let count = count_from_batches(&batches)?;
            assert_eq!(
                count, 1,
                "COPY TO should produce exactly one row visible on read-back"
            );

            // Clean up.
            let auth = SharepointAuth::ClientCredentials {
                tenant_id: cfg.tenant_id.clone(),
                client_id: cfg.client_id.clone(),
                client_secret: SecretString::new(cfg.client_secret.clone().into()),
                scope: None,
            };
            let client = auth
                .build_graph_client()
                .await
                .map_err(|e| anyhow::anyhow!("build_graph_client: {e}"))?;
            let (store, delete_path) = store_and_delete_path(client, &uri)?;
            if let Err(e) = store.delete(&delete_path).await {
                // Surface cleanup failures so we don't silently leak
                // artifacts in the test tenant, but don't fail the test —
                // the COPY TO round-trip has already succeeded.
                eprintln!("sharepoint_parquet_copy_to: cleanup delete failed: {e}");
            }
            Ok(())
        })
        .await
}

/// Verify the legacy `sharepoint:me/root` metadata-listing path still routes
/// to `SharepointTableProvider` — this doesn't mutate state, so it's safer
/// to run routinely when creds are configured.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires live SharePoint tenant credentials — see module docs"]
async fn sharepoint_legacy_metadata_listing() -> Result<(), anyhow::Error> {
    let Some(cfg) = LiveConfig::from_env() else {
        eprintln!("Skipping sharepoint_legacy_metadata_listing: env vars not set");
        return Ok(());
    };

    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async move {
            let mut ds = Dataset::new("sharepoint:me/root", "sp_legacy");
            // Legacy path uses the sharepoint-prefixed params but without file_format.
            ds.params = Some(Params::from_string_map(
                [
                    ("sharepoint_tenant_id".to_string(), cfg.tenant_id.clone()),
                    ("sharepoint_client_id".to_string(), cfg.client_id.clone()),
                    (
                        "sharepoint_client_secret".to_string(),
                        cfg.client_secret.clone(),
                    ),
                ]
                .into_iter()
                .collect(),
            ));

            let app = AppBuilder::new("sharepoint_legacy_metadata_listing")
                .with_dataset(ds)
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;
            let cloned = Arc::new(rt.clone());
            tokio::select! {
                () = tokio::time::sleep(Duration::from_secs(60)) => {
                    return Err(anyhow::anyhow!("timed out loading legacy dataset"));
                }
                () = cloned.load_components() => {}
            }

            let mut q = rt
                .datafusion()
                .query_builder("SELECT name FROM sp_legacy LIMIT 5")
                .build()
                .run()
                .await
                .map_err(|e| anyhow::anyhow!("legacy listing SELECT failed: {e}"))?;
            let mut batches = Vec::new();
            while let Some(b) = q.data.next().await {
                batches.push(b?);
            }
            // Users with an empty drive will still get a valid response (0 rows).
            Ok(())
        })
        .await
}
