// Copyright 2026 Spice AI, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;
use system_adapter_protocol::{
    AdbcDriver, DatasetConfig, Handler, QueryMethodResponse, Server, SetupResponse,
    TeardownResponse,
};
use test_framework::anyhow;
use uuid::Uuid;

use crate::args::StdioArgs;
use crate::commands;

/// State for an active benchmark run provisioned via `setup`.
struct RunState {
    /// Spice Cloud app ID.
    app_id: i64,
    /// API key for the app (used for Flight SQL authentication).
    api_key: String,
    /// Flight SQL endpoint URL derived from the cname.
    flight_url: String,
    /// Spice Cloud API base URL (for rebuilding client on teardown).
    base_url: String,
    /// Spice Cloud API token (for rebuilding client on teardown).
    token: String,
}

/// System adapter handler that provisions Spice Cloud apps.
struct SpidapterHandler {
    /// Active runs keyed by run ID.
    runs: HashMap<Uuid, RunState>,
    /// Spice Cloud API URL override (from CLI args).
    api_url_override: Option<String>,
    /// Timeout in seconds for deployment readiness.
    ready_wait: u64,
}

impl SpidapterHandler {
    fn new(args: &StdioArgs) -> Self {
        Self {
            runs: HashMap::new(),
            api_url_override: args.spice_cloud_api_url.clone(),
            ready_wait: args.ready_wait,
        }
    }
}

#[async_trait]
impl Handler for SpidapterHandler {
    async fn setup(
        &mut self,
        run_id: Uuid,
        datasets: HashMap<String, DatasetConfig>,
    ) -> Result<SetupResponse, String> {
        eprintln!(
            "[stdio] setup: run_id={run_id}, dataset_keys={:?}",
            datasets.keys().collect::<Vec<_>>()
        );

        let state =
            provision_spice_cloud_app(run_id, self.api_url_override.as_deref(), self.ready_wait)
                .await
                .map_err(|e| format!("Setup failed: {e}"))?;

        self.runs.insert(run_id, state);
        Ok(SetupResponse { ok: true })
    }

    async fn query_method(
        &mut self,
        run_id: Uuid,
    ) -> Result<QueryMethodResponse, String> {
        eprintln!("[stdio] query_method: run_id={run_id}");

        let state = self
            .runs
            .get(&run_id)
            .ok_or_else(|| format!("Unknown run_id: {run_id}"))?;

        Ok(QueryMethodResponse {
            driver: AdbcDriver::Flightsql,
            db_kwargs: HashMap::from([
                (
                    "uri".to_string(),
                    serde_json::Value::String(state.flight_url.clone()),
                ),
                (
                    "username".to_string(),
                    serde_json::Value::String(String::new()),
                ),
                (
                    "password".to_string(),
                    serde_json::Value::String(state.api_key.clone()),
                ),
            ]),
        })
    }

    async fn teardown(&mut self, run_id: Uuid) -> Result<TeardownResponse, String> {
        eprintln!("[stdio] teardown: run_id={run_id}");

        let Some(state) = self.runs.remove(&run_id) else {
            eprintln!("[stdio] teardown: run_id={run_id} not found (already torn down?)");
            return Ok(TeardownResponse { ok: true });
        };

        eprintln!(
            "[stdio] teardown: deleting app {} at {}",
            state.app_id, state.base_url
        );

        let cloud_client = commands::build_cloud_client(&state.base_url, &state.token)
            .map_err(|e| format!("Failed to create cloud client: {e}"))?;

        commands::delete_app(&cloud_client, state.app_id)
            .await
            .map_err(|e| format!("Failed to delete app {}: {e}", state.app_id))?;

        eprintln!("[stdio] teardown: app {} deleted", state.app_id);
        Ok(TeardownResponse { ok: true })
    }
}

pub async fn run_stdio_server(args: &StdioArgs) -> anyhow::Result<()> {
    let handler = SpidapterHandler::new(args);
    let mut server = Server::new(handler);
    server
        .run_stdio()
        .await
        .map_err(|e| anyhow::anyhow!("Stdio server error: {e}"))
}

// ── Spice Cloud provisioning ─────────────────────────────────────────

/// Provision a Spice Cloud app from the setup request.
///
/// Follows the same flow as `SpiceCloudSpicedStarter::start()`:
/// 1. Resolve the default cname / region
/// 2. Create or find the SCP app
/// 3. Generate and upload the spicepod from the dataset configs
/// 4. Set secrets (RUNNER + any env-based secrets)
/// 5. Create a deployment
/// 6. Wait for the deployment to become ready
async fn provision_spice_cloud_app(
    run_id: Uuid,
    api_url_override: Option<&str>,
    ready_wait: u64,
) -> anyhow::Result<RunState> {
    let base_url = commands::spice_cloud_base_url(api_url_override);
    let token = commands::spice_cloud_token()?;

    let cloud_client = commands::build_cloud_client(&base_url, &token)?
        .with_timeout(Duration::from_secs(600))?;

    let cname = commands::resolve_default_cname(&cloud_client).await?;
    let flight_url = commands::flight_url_from_cname(&cname);
    let run_id_str = run_id.to_string();
    let short_id = run_id_str.split('-').next().unwrap_or_default();
    let app_name = commands::sanitize_app_name(&format!("spidapter-{short_id}"));

    eprintln!("[stdio] Spice Cloud API: {base_url}");
    eprintln!("[stdio] Region cname: {cname}");
    eprintln!("[stdio] Flight endpoint: {flight_url}");
    eprintln!("[stdio] App name: {app_name}");

    let (app_id, app_api_key) =
        commands::ensure_spice_cloud_app(&cloud_client, &app_name).await?;

    let api_key = app_api_key.ok_or_else(|| {
        anyhow::anyhow!("Spice Cloud did not return an API key for app '{app_name}'")
    })?;

    eprintln!("[stdio] App ID: {app_id}");

    // Generate initial spicepod YAML (tables created later via create_tables)
    let spicepod_yaml = generate_initial_spicepod(&run_id);
    eprintln!("[stdio] Generated spicepod:\n{spicepod_yaml}");

    eprintln!("[stdio] Uploading spicepod to app...");
    commands::apply_spicepod_to_app(&cloud_client, app_id, &spicepod_yaml).await?;
    eprintln!("[stdio] Spicepod uploaded");

    // Set secrets from environment for any secret references in the spicepod
    eprintln!("[stdio] Setting secrets from spicepod...");
    commands::secrets::set_spicepod_secrets(&cloud_client, app_id, &spicepod_yaml)
        .await?;
    eprintln!("[stdio] Spicepod secrets set");

    eprintln!("[stdio] Setting RUNNER secret...");
    commands::secrets::set_secret(&cloud_client, app_id, "RUNNER", "spidapter")
        .await?;
    eprintln!("[stdio] RUNNER secret set");

    eprintln!("[stdio] Creating deployment...");
    commands::create_deployment(&cloud_client, app_id).await?;

    let http_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(600))
        .build()?;
    commands::wait_for_deployment_ready(&http_client, &cname, &api_key, Duration::from_secs(ready_wait))
        .await?;

    eprintln!("[stdio] Spice Cloud deployment ready for app '{app_name}' at {flight_url}");

    Ok(RunState {
        app_id,
        api_key,
        flight_url,
        base_url,
        token,
    })
}

/// Generate a minimal initial spicepod YAML with no datasets.
fn generate_initial_spicepod(run_id: &Uuid) -> String {
    let run_id_str = run_id.to_string();
    let short_id = run_id_str.split('-').next().unwrap_or_default();
    format!("version: v1beta1\nkind: Spicepod\nname: spidapter-{short_id}\n")
}
