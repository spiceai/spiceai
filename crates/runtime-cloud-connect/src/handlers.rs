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

//! Handlers for cloud-originated `ControlMessage`s.
//!
//! These translate proto-level commands into calls on a
//! [`RuntimeHandle`] which is implemented by the spiced runtime (or, in
//! tests, by a mock). Keeping the trait small here means this crate
//! never has to compile against the runtime crate — avoiding a cycle
//! and keeping spice-cloud-connect easy to unit-test.

use std::path::Path;

use async_trait::async_trait;

/// Surface area the cloud-connect client needs from the runtime.
///
/// All methods are best-effort. Implementations should return a
/// JSON-serializable summary of what they did via the
/// `CommandResult.payload_json` field; on error, they should return
/// `Err(string)` rather than panicking.
#[async_trait]
pub trait RuntimeHandle: Send + Sync + 'static {
    /// Number of active datasets currently loaded.
    async fn active_datasets(&self) -> u32 {
        0
    }

    /// Number of active models currently loaded.
    async fn active_models(&self) -> u32 {
        0
    }

    /// Construct the `runtime_info` payload returned for the
    /// `GetRuntimeInfo` command. Implementations should include version,
    /// dataset / model summaries, etc.
    async fn runtime_info_json(&self) -> serde_json::Value {
        serde_json::json!({
            "datasets": self.active_datasets().await,
            "models": self.active_models().await,
        })
    }

    /// Execute a SQL query and serialize results to JSON. Returns
    /// `Err(message)` if the query fails. Implementations should cap
    /// the result size to `max_rows`.
    ///
    /// Default implementation returns a stub error so out-of-the-box
    /// CloudConnect doesn't accidentally execute SQL in test harnesses.
    async fn execute_sql(&self, _sql: &str, _max_rows: u32) -> Result<serde_json::Value, String> {
        Err("RunQuery is not implemented in this build".to_string())
    }

    /// Apply a cloud-managed spicepod to disk and trigger a reload.
    ///
    /// The default implementation writes the YAML to
    /// `config_dir/spicepod-cloud-managed.yml`. Implementations that
    /// want to actually merge it into the running runtime should
    /// override this.
    async fn apply_spicepod(
        &self,
        config_dir: &Path,
        spicepod_yaml: &str,
    ) -> Result<serde_json::Value, String> {
        let path = config_dir.join(crate::config::CLOUD_MANAGED_SPICEPOD_FILE);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("create config dir: {e}"))?;
        }
        std::fs::write(&path, spicepod_yaml).map_err(|e| format!("write spicepod: {e}"))?;
        Ok(serde_json::json!({
            "path": path.display().to_string(),
            "reload": "deferred",
        }))
    }

    /// Restart the runtime. `graceful` indicates whether the runtime
    /// should drain in-flight requests before exiting. The default
    /// implementation is a no-op so that test mocks don't accidentally
    /// kill themselves.
    async fn restart(&self, _graceful: bool) -> Result<serde_json::Value, String> {
        Ok(serde_json::json!({ "status": "restart_not_implemented" }))
    }

    /// Attempt an in-place runtime upgrade. v0: always returns an
    /// "unsupported" payload.
    async fn upgrade_runtime(
        &self,
        target_version: &str,
    ) -> Result<serde_json::Value, String> {
        Ok(serde_json::json!({
            "status": "unsupported",
            "requested_version": target_version,
            "note": "UpgradeRuntime is not implemented in v0",
        }))
    }
}

/// Minimal no-op runtime handle, useful for unit tests and as a stand-in
/// when CloudConnect is exercised outside the real spiced binary.
#[derive(Debug, Default, Clone)]
pub struct NoopRuntimeHandle;

#[async_trait]
impl RuntimeHandle for NoopRuntimeHandle {}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn noop_runtime_handle_defaults() {
        let h = NoopRuntimeHandle;
        assert_eq!(h.active_datasets().await, 0);
        assert_eq!(h.active_models().await, 0);
        let info = h.runtime_info_json().await;
        assert_eq!(info["datasets"], 0);
        assert_eq!(info["models"], 0);
        let err = h.execute_sql("select 1", 10).await;
        assert!(err.is_err());
        let up = h.upgrade_runtime("v9.9.9").await.unwrap();
        assert_eq!(up["status"], "unsupported");
    }
}
