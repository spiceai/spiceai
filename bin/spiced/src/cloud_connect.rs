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

//! Bootstrap glue for Spice Cloud Connect inside `spiced`.
//!
//! Wires the spice-cloud-connect client into the runtime so that a
//! standalone `spiced` can be discovered, adopted, and managed by a
//! Spice Cloud control plane.
//!
//! ## Opt-in semantics
//!
//! `CloudConnect` is **disabled by default**. It activates only if one of
//! the following is true at boot:
//!
//! 1. `$SPICE_CONFIG_DIR/identity.json` exists.
//! 2. `SPICE_ADOPT_CODE` env var is set.
//! 3. `$SPICE_CONFIG_DIR/pending-adopt-code` file exists.
//!
//! If none of the above is true, this module never opens a connection.

use std::path::Path;
use std::sync::Arc;

use app::{App, AppBuilder};
use async_trait::async_trait;
use runtime::Runtime;
use runtime_cloud_connect::config::{CLOUD_MANAGED_SPICEPOD_FILE, CloudConnectConfig};
use runtime_cloud_connect::handlers::{QueryResult, RuntimeHandle};
use runtime_cloud_connect::{CloudConnect, identity::IdentityStore};

/// Read the optional `cloud-endpoint` override file written by
/// `spice connect <code> --endpoint <url>`.
fn read_endpoint_override(config_dir: &Path) -> Option<String> {
    let path = config_dir.join("cloud-endpoint");
    std::fs::read_to_string(path)
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

/// Build a [`CloudConnectConfig`] from env + on-disk state.
fn build_config(runtime_version: &str) -> CloudConnectConfig {
    let mut config = CloudConnectConfig::from_env(runtime_version);
    if std::env::var_os("SPICE_CLOUD_ENDPOINT").is_none()
        && let Some(override_endpoint) = read_endpoint_override(&config.config_dir)
    {
        config.endpoint = override_endpoint;
    }
    config
}

/// Start the Cloud Connect client if any of the opt-in conditions are
/// met. The returned `Option<CloudConnect>` is `None` when `CloudConnect`
/// is disabled — which is the default for vanilla OSS installs.
pub async fn maybe_start(runtime_version: &str, runtime: Arc<Runtime>) -> Option<CloudConnect> {
    let config = build_config(runtime_version);

    // Quick sanity probe — if no identity AND no adoption code, skip.
    // Surface a load/parse error (corrupt or unreadable identity.json)
    // rather than silently treating it as "not adopted", so a broken
    // identity file is visible to the operator instead of quietly
    // disabling Cloud Connect.
    let has_identity = match IdentityStore::load_optional(&config.identity_path) {
        Ok(opt) => opt.is_some(),
        Err(err) => {
            tracing::warn!(
                "Spice Cloud Connect: could not read identity at {}: {err}; \
                 treating as not-adopted — fix or remove the file to re-adopt",
                config.identity_path.display()
            );
            false
        }
    };
    if !has_identity && config.adoption_code.is_none() {
        tracing::debug!(
            "Spice Cloud Connect: disabled (no identity at {} and no adoption code)",
            config.identity_path.display()
        );
        return None;
    }

    tracing::info!(
        "Spice Cloud Connect: enabled, endpoint={} mode={}",
        config.endpoint,
        if has_identity { "identity" } else { "adopt" }
    );

    let handle: Arc<dyn RuntimeHandle> = Arc::new(SpicedRuntimeHandle::new(runtime));

    match CloudConnect::start(config, handle).await {
        Ok(Some(client)) => Some(client),
        Ok(None) => None,
        Err(err) => {
            tracing::warn!(
                "Spice Cloud Connect: failed to start (continuing without cloud management): {err}"
            );
            None
        }
    }
}

/// Thin adapter so the cloud-connect client can call into the runtime
/// without taking a hard dep on the `runtime` crate.
struct SpicedRuntimeHandle {
    runtime: Arc<Runtime>,
}

impl SpicedRuntimeHandle {
    fn new(runtime: Arc<Runtime>) -> Self {
        Self { runtime }
    }
}

#[async_trait]
impl RuntimeHandle for SpicedRuntimeHandle {
    async fn active_datasets(&self) -> u32 {
        match self.runtime.read_app().await {
            Some(app) => u32::try_from(app.datasets.len()).unwrap_or(u32::MAX),
            None => 0,
        }
    }

    async fn active_models(&self) -> u32 {
        match self.runtime.read_app().await {
            Some(app) => u32::try_from(app.models.len()).unwrap_or(u32::MAX),
            None => 0,
        }
    }

    async fn runtime_info_json(&self) -> serde_json::Value {
        let app = self.runtime.read_app().await;
        let (name, datasets, models, catalogs, views) = match &app {
            Some(app) => (
                Some(app.name.clone()),
                app.datasets.len(),
                app.models.len(),
                app.catalogs.len(),
                app.views.len(),
            ),
            None => (None, 0, 0, 0, 0),
        };
        serde_json::json!({
            "name": name,
            "datasets": datasets,
            "models": models,
            "catalogs": catalogs,
            "views": views,
        })
    }

    /// Execute a SQL statement and return the results as a native Arrow IPC
    /// stream (schema + record batches) plus row-count / truncation metadata.
    /// The runtime never flattens rows to JSON — the cloud control plane
    /// decodes the Arrow directly and renders JSON only at its REST edge.
    ///
    /// Caps:
    /// - `max_rows == 0` → default cap of [`DEFAULT_RUN_QUERY_ROW_CAP`].
    /// - Hard ceiling of [`RUN_QUERY_HARD_ROW_CAP`] rows regardless.
    /// - Byte budget of [`RUN_QUERY_BYTE_BUDGET`] on the encoded IPC stream
    ///   — exceeding either cap sets `truncated: true`.
    async fn execute_sql(&self, sql: &str, max_rows: u32) -> Result<QueryResult, String> {
        use futures::StreamExt as _;

        let cap = resolve_run_query_cap(max_rows);

        let df = self.runtime.datafusion();
        // Cloud-originated RunQuery is a remote management surface; we
        // never want an adopted control plane to be able to mutate the
        // local runtime via DDL/DML. Force read-only at the query layer
        // regardless of principal — there is no signed-in user here.
        let query = df.query_builder(sql).read_only(true).build();
        let result = query.run().await.map_err(|e| e.to_string())?;
        let mut stream = result.data;
        // Capture the stream schema BEFORE consuming. A query that returns
        // zero batches (empty result set) still has a real schema, so
        // without this snapshot the envelope would advertise empty columns.
        let stream_schema = stream.schema();

        // Collect up to `cap` rows worth of batches. We stop streaming
        // once we have enough rows so we don't pull the rest of a huge
        // result set into memory just to throw it away.
        let mut batches: Vec<arrow::record_batch::RecordBatch> = Vec::new();
        let mut collected_rows: usize = 0;
        let mut source_truncated = false;
        while let Some(batch) = stream.next().await {
            let batch = batch.map_err(|e| e.to_string())?;
            let take_rows = cap.saturating_sub(collected_rows);
            if take_rows == 0 {
                // We've hit the cap. Only flag truncation if this next
                // batch actually carries rows — an empty trailing batch
                // means the result ended exactly at the cap, not beyond it.
                if batch.num_rows() > 0 {
                    source_truncated = true;
                }
                break;
            }
            if batch.num_rows() > take_rows {
                source_truncated = true;
                batches.push(batch.slice(0, take_rows));
                break;
            }
            collected_rows += batch.num_rows();
            batches.push(batch);
        }

        // Encode the collected batches to an Arrow IPC stream, enforcing
        // the byte budget. If encoding would exceed the budget we stop
        // writing further batches and mark the result truncated.
        let (arrow_ipc, encoded_rows, byte_truncated) =
            encode_ipc_bounded(stream_schema.as_ref(), &batches, RUN_QUERY_BYTE_BUDGET)?;
        Ok(QueryResult {
            arrow_ipc,
            row_count: encoded_rows,
            truncated: source_truncated || byte_truncated,
        })
    }

    /// Apply a cloud-managed spicepod and hot-reload it into the running
    /// runtime — no restart required.
    ///
    /// 1. The YAML is validated by building an [`App`] from it on a sibling
    ///    temp file, so a malformed control-plane push is rejected with a
    ///    clear error and the previous good `spicepod-cloud-managed.yml` is
    ///    left untouched.
    /// 2. The validated file is promoted to the canonical path so a later
    ///    process restart also picks it up.
    /// 3. The new app is hot-applied via [`Runtime::apply_app`] — the same
    ///    catalog/dataset/view/model/function/worker diff-reconcile the pods
    ///    watcher performs on a file change — so the configuration takes
    ///    effect immediately.
    async fn apply_spicepod(
        &self,
        config_dir: &Path,
        spicepod_yaml: &str,
    ) -> Result<serde_json::Value, String> {
        let (new_app, path) = stage_cloud_managed_spicepod(config_dir, spicepod_yaml).await?;

        let datasets = new_app.datasets.len();
        let models = new_app.models.len();
        let catalogs = new_app.catalogs.len();
        let views = new_app.views.len();

        let changed = Arc::clone(&self.runtime).apply_app(Arc::new(new_app)).await;

        Ok(serde_json::json!({
            "path": path.display().to_string(),
            "applied": true,
            "reload": if changed { "hot" } else { "unchanged" },
            "datasets": datasets,
            "models": models,
            "catalogs": catalogs,
            "views": views,
        }))
    }
}

/// Validate a cloud-managed spicepod and persist it to disk.
///
/// Writes `spicepod_yaml` to a sibling `*.incoming.yml` temp file, builds an
/// [`App`] from it to validate (parse + resolve), and only on success
/// atomically promotes the temp file to the canonical
/// [`CLOUD_MANAGED_SPICEPOD_FILE`] path. On any failure the canonical file is
/// left untouched and the temp file is cleaned up. Returns the built `App`
/// (ready to hot-apply) and the canonical path it was written to.
///
/// Factored out of [`SpicedRuntimeHandle::apply_spicepod`] so the
/// file-staging + validation can be unit-tested without a running runtime.
async fn stage_cloud_managed_spicepod(
    config_dir: &Path,
    spicepod_yaml: &str,
) -> Result<(App, std::path::PathBuf), String> {
    let path = config_dir.join(CLOUD_MANAGED_SPICEPOD_FILE);
    tokio::fs::create_dir_all(config_dir)
        .await
        .map_err(|e| format!("create config dir: {e}"))?;

    // Validate on a temp file first so a bad push never clobbers the last
    // known-good spicepod on disk.
    let incoming = config_dir.join("spicepod-cloud-managed.incoming.yml");
    tokio::fs::write(&incoming, spicepod_yaml)
        .await
        .map_err(|e| format!("write spicepod: {e}"))?;

    match AppBuilder::build_from_path(incoming.clone()).await {
        Ok(app) => {
            tokio::fs::rename(&incoming, &path)
                .await
                .map_err(|e| format!("persist spicepod: {e}"))?;
            Ok((app, path))
        }
        Err(e) => {
            // Best-effort cleanup; ignore failure (temp file is inert).
            let _ = tokio::fs::remove_file(&incoming).await;
            Err(format!("invalid spicepod: {e}"))
        }
    }
}

/// Byte budget for an encoded `RunQuery` Arrow IPC stream. A coarse secondary
/// guard on top of the row cap so a few very wide rows can't blow past the
/// gRPC message size; the last batch written may push modestly over before
/// we stop.
pub const RUN_QUERY_BYTE_BUDGET: usize = 5 * 1024 * 1024;

/// Encode `batches` into an Arrow IPC stream (`schema` first), stopping once
/// the buffer exceeds `budget`. Returns the bytes, the number of rows
/// actually written, and whether the budget cut the stream short.
fn encode_ipc_bounded(
    schema: &arrow::datatypes::Schema,
    batches: &[arrow::record_batch::RecordBatch],
    budget: usize,
) -> Result<(Vec<u8>, u64, bool), String> {
    use arrow::ipc::writer::StreamWriter;
    let mut buf: Vec<u8> = Vec::new();
    let mut writer = StreamWriter::try_new(&mut buf, schema)
        .map_err(|e| format!("arrow ipc init failed: {e}"))?;
    let mut rows: u64 = 0;
    let mut truncated = false;
    for batch in batches {
        writer
            .write(batch)
            .map_err(|e| format!("arrow ipc write failed: {e}"))?;
        rows += batch.num_rows() as u64;
        if writer.get_ref().len() > budget {
            truncated = true;
            break;
        }
    }
    writer
        .finish()
        .map_err(|e| format!("arrow ipc finish failed: {e}"))?;
    drop(writer);
    Ok((buf, rows, truncated))
}

/// Default row cap when the cloud control plane sets `max_rows = 0`.
pub const DEFAULT_RUN_QUERY_ROW_CAP: usize = 1_000;
/// Hard row ceiling that the runtime always enforces, even when the
/// control plane requests more.
pub const RUN_QUERY_HARD_ROW_CAP: usize = 10_000;

/// Resolve the effective row cap for a `RunQuery`:
/// - `max_rows == 0` → [`DEFAULT_RUN_QUERY_ROW_CAP`]
/// - else → `min(max_rows, RUN_QUERY_HARD_ROW_CAP)`
#[must_use]
pub fn resolve_run_query_cap(max_rows: u32) -> usize {
    if max_rows == 0 {
        DEFAULT_RUN_QUERY_ROW_CAP
    } else {
        (max_rows as usize).min(RUN_QUERY_HARD_ROW_CAP)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_query_cap_defaults_when_unset() {
        assert_eq!(resolve_run_query_cap(0), DEFAULT_RUN_QUERY_ROW_CAP);
    }

    #[test]
    fn run_query_cap_honors_caller_below_hard_cap() {
        assert_eq!(resolve_run_query_cap(50), 50);
        assert_eq!(resolve_run_query_cap(9_999), 9_999);
        assert_eq!(resolve_run_query_cap(10_000), 10_000);
    }

    #[test]
    fn run_query_cap_clamps_to_hard_cap() {
        // The hard cap is 10_000 — even when the caller asks for 99_999
        // we never serialize more than 10_000 rows.
        assert_eq!(resolve_run_query_cap(99_999), RUN_QUERY_HARD_ROW_CAP);
        assert_eq!(resolve_run_query_cap(u32::MAX), RUN_QUERY_HARD_ROW_CAP);
    }

    /// Minimal valid spicepod (no components — an empty app is valid).
    const VALID_SPICEPOD: &str = "version: v2\nkind: Spicepod\nname: cloud-managed-test\n";
    /// Invalid YAML (unclosed flow sequence) — guaranteed to fail parsing.
    const INVALID_SPICEPOD: &str = "name: [unclosed";

    /// Create (and clean) a unique temp dir for a test without pulling in a
    /// temp-file crate. Names are per-test + per-process so parallel tests
    /// don't collide.
    fn scratch_dir(tag: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("spice-cc-{}-{tag}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create scratch dir");
        dir
    }

    #[tokio::test]
    async fn stage_valid_spicepod_writes_canonical_and_returns_app() {
        let dir = scratch_dir("valid");
        let (app, path) = stage_cloud_managed_spicepod(&dir, VALID_SPICEPOD)
            .await
            .expect("valid spicepod stages");

        assert_eq!(app.name, "cloud-managed-test");
        assert_eq!(path, dir.join(CLOUD_MANAGED_SPICEPOD_FILE));
        // Canonical file written with the supplied content.
        let on_disk = std::fs::read_to_string(&path).expect("canonical file exists");
        assert_eq!(on_disk, VALID_SPICEPOD);
        // The temp .incoming file was renamed away, not left behind.
        assert!(!dir.join("spicepod-cloud-managed.incoming.yml").exists());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn stage_invalid_spicepod_preserves_previous_good_file() {
        let dir = scratch_dir("invalid-preserves");
        // Land a known-good canonical file first.
        stage_cloud_managed_spicepod(&dir, VALID_SPICEPOD)
            .await
            .expect("first valid stage");
        let canonical = dir.join(CLOUD_MANAGED_SPICEPOD_FILE);

        // A subsequent invalid push must be rejected and must NOT clobber it.
        let err = stage_cloud_managed_spicepod(&dir, INVALID_SPICEPOD)
            .await
            .expect_err("invalid spicepod rejected");
        assert!(err.contains("invalid spicepod"), "unexpected error: {err}");

        let on_disk = std::fs::read_to_string(&canonical).expect("canonical still present");
        assert_eq!(on_disk, VALID_SPICEPOD, "previous good config preserved");
        assert!(!dir.join("spicepod-cloud-managed.incoming.yml").exists());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn stage_invalid_spicepod_when_none_exists_leaves_no_canonical() {
        let dir = scratch_dir("invalid-fresh");
        let err = stage_cloud_managed_spicepod(&dir, INVALID_SPICEPOD)
            .await
            .expect_err("invalid spicepod rejected");
        assert!(err.contains("invalid spicepod"), "unexpected error: {err}");
        // Nothing should have been promoted to the canonical path.
        assert!(!dir.join(CLOUD_MANAGED_SPICEPOD_FILE).exists());
        assert!(!dir.join("spicepod-cloud-managed.incoming.yml").exists());

        let _ = std::fs::remove_dir_all(&dir);
    }
}
