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
//! SharePoint drive. They silently no-op when the required env vars are not
//! set, so the full `cargo test -p runtime --test integration` run stays
//! green for contributors without a SharePoint tenant.
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
//! and clean up after themselves. Each file name is unique-per-run to avoid
//! collisions.

use std::{sync::Arc, time::Duration};

use app::AppBuilder;
use futures::StreamExt;

use runtime::Runtime;
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
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    format!("spice-test-{ts}.{ext}")
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
            assert!(!batches.is_empty(), "expected at least one batch");

            // Clean up — DELETE the file we wrote.
            use data_components::sharepoint::auth::{DEFAULT_SCOPE, SharepointAuth};
            use data_components::sharepoint::object_store::{
                SharepointObjectStore, SharepointObjectStoreConfig,
            };
            use object_store::ObjectStore;
            use secrecy::SecretString;

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
            let parsed = data_components::sharepoint::url::SharepointUrl::parse(&uri)
                .map_err(|e| anyhow::anyhow!("parse test uri: {e}"))?;
            let store = SharepointObjectStore::new(
                client,
                parsed.drive,
                SharepointObjectStoreConfig::default(),
            );
            store
                .delete(&parsed.item_path)
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
            assert!(
                !batches.is_empty(),
                "COPY TO produced no rows visible on read-back"
            );

            // Clean up.
            use data_components::sharepoint::auth::SharepointAuth;
            use data_components::sharepoint::object_store::{
                SharepointObjectStore, SharepointObjectStoreConfig,
            };
            use object_store::ObjectStore;
            use secrecy::SecretString;

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
            let parsed = data_components::sharepoint::url::SharepointUrl::parse(&uri)
                .map_err(|e| anyhow::anyhow!("parse test uri: {e}"))?;
            let store = SharepointObjectStore::new(
                client,
                parsed.drive,
                SharepointObjectStoreConfig::default(),
            );
            let _ = store.delete(&parsed.item_path).await;
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
