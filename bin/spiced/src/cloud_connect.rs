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
pub async fn maybe_start(
    runtime_version: &str,
    runtime: Arc<Runtime>,
) -> Option<CloudConnect> {
    let config = build_config(runtime_version);

    // Quick sanity probe — if no identity AND no adoption code, skip.
    let has_identity = IdentityStore::load_optional(&config.identity_path)
        .ok()
        .flatten()
        .is_some();
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

    async fn execute_sql(
        &self,
        sql: &str,
        max_rows: u32,
    ) -> Result<serde_json::Value, String> {
        // Cap defensively. The control plane is trusted to set a
        // sensible limit, but we still box it.
        let cap = max_rows.clamp(1, 10_000) as usize;
        let df = self.runtime.datafusion();
        let query = df.query_builder(sql).build();
        let result = query.run().await.map_err(|e| e.to_string())?;
        let mut stream = result.data;
        use futures::StreamExt as _;
        let mut total_rows: usize = 0;
        let mut total_batches: usize = 0;
        let mut schema_json: Option<serde_json::Value> = None;
        while let Some(batch) = stream.next().await {
            let batch = batch.map_err(|e| e.to_string())?;
            if schema_json.is_none() {
                let schema = batch.schema();
                schema_json = Some(serde_json::json!(
                    schema
                        .fields()
                        .iter()
                        .map(|f| {
                            serde_json::json!({
                                "name": f.name(),
                                "type": f.data_type().to_string(),
                                "nullable": f.is_nullable(),
                            })
                        })
                        .collect::<Vec<_>>()
                ));
            }
            total_batches += 1;
            total_rows = total_rows.saturating_add(batch.num_rows());
            if total_rows >= cap {
                break;
            }
        }
        Ok(serde_json::json!({
            "rows": total_rows,
            "batches": total_batches,
            "schema": schema_json.unwrap_or(serde_json::Value::Null),
            "note": "RunQuery returns row counts and schema only in v0; row payloads are not streamed to the cloud."
        }))
    }
}
