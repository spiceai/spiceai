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
//! CloudConnect is **disabled by default**. It activates only if one of
//! the following is true at boot:
//!
//! 1. `$SPICE_CONFIG_DIR/identity.json` exists.
//! 2. `SPICE_ADOPT_CODE` env var is set.
//! 3. `$SPICE_CONFIG_DIR/pending-adopt-code` file exists.
//!
//! If none of the above is true, this module never opens a connection.

use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use runtime::Runtime;
use runtime_cloud_connect::config::CloudConnectConfig;
use runtime_cloud_connect::handlers::RuntimeHandle;
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
/// met. The returned `Option<CloudConnect>` is `None` when CloudConnect
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

    /// Execute a SQL statement and return the rows plus column metadata
    /// in the wire shape consumed by the Spice Cloud portal's Query tab:
    ///
    /// ```json
    /// {
    ///   "columns": [{ "name": "...", "data_type": "..." }],
    ///   "rows": [[v1, v2, ...], ...],
    ///   "row_count": N,
    ///   "truncated": bool
    /// }
    /// ```
    ///
    /// Caps:
    /// - `max_rows == 0` → default cap of [`DEFAULT_RUN_QUERY_ROW_CAP`].
    /// - Hard ceiling of [`RUN_QUERY_HARD_ROW_CAP`] rows regardless.
    /// - Payload byte budget of
    ///   [`runtime_cloud_connect::arrow_json::PAYLOAD_SIZE_BUDGET_BYTES`]
    ///   — exceeding either cap sets `truncated: true`.
    async fn execute_sql(&self, sql: &str, max_rows: u32) -> Result<serde_json::Value, String> {
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
        use futures::StreamExt as _;

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
                source_truncated = true;
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

        let mut envelope = runtime_cloud_connect::arrow_json::encode_record_batches_with_schema(
            &batches,
            Some(stream_schema.as_ref()),
            cap,
        );
        // Propagate the source-side truncation flag if we cut off the
        // upstream stream before exhausting it.
        if source_truncated && let Some(obj) = envelope.as_object_mut() {
            obj.insert("truncated".to_string(), serde_json::Value::Bool(true));
        }
        Ok(envelope)
    }
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
}
